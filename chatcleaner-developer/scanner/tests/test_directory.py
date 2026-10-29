import asyncio
import csv
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from directory import Directory, SearchSpec, ExportSpec, community, eligible, export_rows
from engine import Store
from telethon import types, errors, functions
from pydantic import ValidationError


def channel(cid=100, **kwargs):
    return types.Channel(id=cid, title=kwargs.pop('title', 'Дубай туристы'), username='dubai'+str(cid),
                         photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
                         megagroup=kwargs.pop('megagroup', True), left=kwargs.pop('left', True), **kwargs)


class FilterTests(unittest.TestCase):
    def test_normalization_and_invalid_input(self):
        self.assertEqual(SearchSpec(queries=[' Dubai ', 'DUBAI']).queries, ['dubai'])
        for payload in [dict(queries=[]),dict(queries=['ab']),dict(queries=['abc']*21),dict(queries=['x'*101]),
                        dict(queries=['dubai'],min_members=200,max_members=100),dict(queries=['dubai'],limit=201),
                        dict(queries=['dubai'],active_days=-1),dict(queries=['dubai'],kind='people')]:
            with self.assertRaises(ValidationError): SearchSpec(**payload)

    def test_external_type_words_members_and_membership(self):
        row = community(channel(participants_count=500))
        self.assertTrue(eligible(row, SearchSpec(queries=['dubai'],include='ДУБАЙ',exclude='крипто',min_members=100,max_members=1000,membership='not_joined')))
        for spec in [dict(kind='channel'),dict(exclude='туристы'),dict(include='сафари'),dict(min_members=501),dict(max_members=499),dict(membership='joined')]:
            self.assertFalse(eligible(row, SearchSpec(queries=['dubai'],**spec)))
        row['members']=None
        self.assertTrue(eligible(row, SearchSpec(queries=['dubai'])))
        self.assertFalse(eligible(row, SearchSpec(queries=['dubai'],max_members=100)))
        self.assertFalse(eligible(row, SearchSpec(queries=['dubai'],min_members=1)))
        self.assertIsNone(community(channel(creator=True)))
        self.assertIsNone(community(channel(admin_rights=types.ChatAdminRights(delete_messages=True))))
        self.assertIsNone(community(types.User(id=123,username='person')))

    def test_csv_formula_and_multiline_roundtrip(self):
        for title in ['=HYPERLINK("x")','  +SUM(1,2)','@danger','-1+2','\tformula','Яхты; "Дубай"\nчат']:
            row=community(channel(title=title));row['queries']=['=bad','Dubai']
            csv_text=export_rows([row], 'csv')
            self.assertTrue(csv_text.startswith('\ufeff'))
            cells=list(csv.reader(io.StringIO(csv_text.lstrip('\ufeff')),delimiter=';'))
            self.assertEqual(len(cells),2)
            self.assertEqual(len(cells[1]),8)
            self.assertTrue(cells[1][0].startswith("'") if title[0]!='Я' else cells[1][0]==title)
            self.assertEqual(export_rows([row],'txt'), 'https://t.me/dubai100\n')


class DirectoryJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'test.sqlite3')
        self.responses={'dubai':[channel(participants_count=500)],'туристы':[channel(participants_count=500)]}
        self.calls=[];owner=self
        class Telegram:
            def is_connected(self):return True
            async def __call__(self,request):
                assert isinstance(request,functions.contacts.SearchRequest), 'Must not join or send messages'
                owner.calls.append(request.q)
                result=owner.responses[request.q]
                if isinstance(result,Exception):raise result
                return SimpleNamespace(chats=result)
            async def get_messages(self,entity,limit):
                if entity.id==102:raise errors.ChannelPrivateError(request=None)
                return [SimpleNamespace(date=datetime.now(timezone.utc)-timedelta(days=60 if entity.id==101 else 0))]
        self.runtime=SimpleNamespace(client=Telegram(),me=SimpleNamespace(id=42))
        self.directory=Directory(lambda:self.store,lambda:self.runtime);self.directory.pause=0

    async def asyncTearDown(self):
        if self.directory.task and not self.directory.task.done():await self.directory.cancel(self.directory.active_id)
        self.store.db.close();self.tmp.cleanup()

    async def search(self,**kwargs):
        result=await self.directory.start(SearchSpec(**kwargs));await self.directory.task
        return await self.directory.detail(result['id'])

    async def test_multiple_queries_merge_and_persist_with_exports(self):
        result=await self.search(queries=['Dubai','Туристы'])
        self.assertEqual(result['state'],'completed');self.assertEqual(result['done'],2);self.assertEqual(result['count'],1)
        self.assertEqual(result['results'][0]['queries'],['dubai','туристы'])
        # A fresh service sees saved data; no Telegram session is needed for export.
        restored=Directory(lambda:self.store,lambda:None)
        self.assertEqual((await restored.detail(result['id']))['results'],result['results'])
        exported=await restored.export(result['id'],ExportSpec(format='txt',ids=[result['results'][0]['id']]))
        self.assertEqual(exported.body.decode(),'https://t.me/dubai100\n')
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):await restored.export(result['id'],ExportSpec(format='txt',ids=[-42]))

    async def test_activity_unknown_and_inaccessible_filtered(self):
        self.responses['dubai']=[channel(100),channel(101),channel(102)]
        result=await self.search(queries=['dubai'],active_days=7)
        self.assertEqual(result['count'],1)
        self.assertEqual(result['stats']['inactive'],1);self.assertEqual(result['stats']['unavailable'],1)
        self.assertIsNotNone(result['results'][0]['last_message_at'])

    async def test_limit_owned_exclusion_and_cap(self):
        self.responses['dubai']=[channel(99,creator=True),channel(100),channel(101)]
        result=await self.search(queries=['dubai','туристы'],limit=1)
        self.assertEqual(result['count'],1);self.assertTrue(result['limited']);self.assertEqual(self.calls,['dubai'])
        for _ in range(11):await self.search(queries=['dubai'],limit=1)
        self.assertEqual(len((await self.directory.history())['runs']),10)
        self.assertEqual(len(self.store.rows('SELECT * FROM parser_results')),10)

    async def test_flood_wait_preserves_partial_results_and_prevents_immediate_retry(self):
        self.responses['туристы']=errors.FloodWaitError(request=None,capture=30)
        result=await self.search(queries=['dubai','туристы'])
        self.assertEqual(result['state'],'partial');self.assertEqual(result['count'],1)
        self.assertIn('30',result['error'])
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:await self.directory.start(SearchSpec(queries=['dubai']))
        self.assertEqual(ctx.exception.status_code,429)

    async def test_immediate_cancel_before_worker_starts(self):
        result=await self.directory.start(SearchSpec(queries=['dubai']))
        result=await self.directory.cancel(result['id'])
        self.assertEqual(result['state'],'cancelled')
        self.assertEqual(self.calls,[])

    async def test_cancel_exclusion_and_restart_recovery(self):
        self.runtime.discovery_next=time.time()+4
        result=await self.directory.start(SearchSpec(queries=['dubai']))
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:await self.directory.start(SearchSpec(queries=['dubai']))
        self.assertEqual(ctx.exception.status_code,409)
        await asyncio.sleep(0)
        result=await self.directory.cancel(result['id'])
        self.assertEqual(result['state'],'cancelled');self.assertEqual(self.calls,[])
        self.directory.update(result['id'],state='running');self.directory.recover()
        self.assertEqual((await self.directory.detail(result['id']))['state'],'interrupted')


class DirectoryHTTPTests(unittest.TestCase):
    def test_auth_csrf_validation_and_export(self):
        with tempfile.TemporaryDirectory() as folder:
            os.environ.setdefault('SCANNER_DATA',folder)
            import app
            from fastapi.testclient import TestClient
            db=Store(Path(folder)/'http.sqlite3')
            with patch.object(app,'store',db),TestClient(app.app,raise_server_exceptions=False) as client:
                # No Telegram API credentials in this temporary DB.
                headers={'X-Panel-User':'owner','Origin':app.ORIGIN,'X-Scanner-Request':'1'}
                self.assertEqual(client.get('/api/scanner/parser/runs').status_code,401)
                self.assertEqual(client.post('/api/scanner/parser/runs',json={'queries':['dubai']},headers={'X-Panel-User':'owner'}).status_code,403)
                self.assertEqual(client.post('/api/scanner/parser/runs',json={'queries':['a']},headers=headers).status_code,422)
                self.assertEqual(client.post('/api/scanner/parser/runs',json={'queries':['dubai']},headers=headers).status_code,409)
                self.assertEqual(client.get('/api/scanner/parser/runs',headers=headers).json(),{'runs':[]})
                self.assertEqual(client.get('/api/scanner/parser/runs/123',headers=headers).status_code,404)
