import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store, explain, matches
from history import HistoryRunner, HistorySpec
from telethon import errors

class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'db')
        self.store.put('running',True);self.store.put('destination',99)
        with self.store.db:
            self.store.db.execute("INSERT INTO sources(id,title,username,cursor) VALUES(-100001,'External','external',100)")
            self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Yacht',json.dumps(dict(sources=[-100001],keywords=['яхту'],required=['ищу'],excluded=['предлагаю'],substring=False))))
        owner=self
        class Client:
            def is_connected(self):return True
            async def get_entity(self,id):return SimpleNamespace(id=id,creator=False,admin_rights=None)
            async def iter_messages(self,entity,**kwargs):
                for m in owner.messages:yield m
        self.runtime=SimpleNamespace(client=Client(),me=SimpleNamespace(id=99))
        self.runner=HistoryRunner(lambda:self.store,lambda:self.runtime);self.runner.pause=0
        def msg(mid,text,out=False,days=0):return SimpleNamespace(id=mid,message=text,out=out,date=datetime.now(timezone.utc)-timedelta(days=days),sender_id=123,get_sender=AsyncMock(return_value=SimpleNamespace(username='buyer')))
        self.messages=[msg(100,'Ищу яхту на завтра'),msg(99,'яхту предлагаем'),msg(98,'Ищу яхту, предлагаю услуги'),msg(97,'Привет'),msg(96,'Ищу яхту исходящее',out=True),msg(95,'Ищу яхту давно',days=8)]
    async def asyncTearDown(self):
        await self.runner.stop();self.store.db.close();self.tmp.cleanup()
    async def run_job(self,**kwargs):
        await self.runner.start(HistorySpec(**kwargs));await self.runner.task;return await self.runner.detail()
    async def test_preview_explains_absence_without_leads_or_cursor_changes(self):
        job=await self.run_job(days=3)
        self.assertEqual(job['state'],'completed')
        for name,expected in dict(read=5,text=4,matched=1,no_keywords=1,no_intent=1,excluded=1,skipped=1).items():self.assertEqual(job[name],expected,name)
        self.assertEqual(self.store.rows('SELECT * FROM leads'),[])
        self.assertEqual(self.store.rows('SELECT cursor FROM sources')[0]['cursor'],100)
    async def test_http_preview_preserves_data_without_recursive_encoder(self):
        from fastapi.responses import JSONResponse
        job=await self.run_job(days=3)
        response=await self.runner.detail_http()
        self.assertIsInstance(response,JSONResponse)
        self.assertEqual(json.loads(response.body),job)
    async def test_small_cached_batch_yields_to_other_tasks(self):
        self.messages=self.messages[:4]
        progress=[]
        async def other_work():
            progress.append(self.runner.job['read'])
        tasks=[]
        async def cached_batch(entity,**kwargs):
            tasks.append(asyncio.create_task(other_work()))
            for message in self.messages:yield message
        self.runtime.client.iter_messages=cached_batch
        await self.run_job(days=3)
        await tasks[0]
        self.assertLess(progress[0],4)
        self.assertEqual(self.runner.job['read'],4)
        self.assertEqual(self.store.rows('SELECT cursor FROM sources')[0]['cursor'],100)
    async def test_import_is_real_historical_and_quiet_even_when_live_paused(self):
        self.store.put('running',False)
        job=await self.run_job(mode='import')
        self.assertEqual(job['imported'],1)
        lead=self.store.rows('SELECT * FROM leads')[0]
        self.assertEqual(lead['origin'],'history');self.assertEqual(lead['message_id'],100)
        self.assertEqual(lead['created'],self.messages[0].date.isoformat())
        self.assertEqual(self.store.rows('SELECT * FROM outbox'),[])
        job=await self.run_job(mode='import');self.assertEqual(job['imported'],0);self.assertEqual(job['duplicate'],1)
        self.store.put('running',True)
        self.assertIsNone(self.store.ingest(-100001,100,self.messages[0].message,sender_id=123))
    async def test_no_disabled_foreign_or_managed_sources(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):await self.runner.start(HistorySpec(sources=[-100009]))
        self.runtime.client.get_entity=AsyncMock(return_value=SimpleNamespace(creator=True))
        job=await self.run_job();self.assertEqual(job['state'],'partial');self.assertEqual(job['read'],0)
        self.assertIn('Административная',job['sources'][0]['error'])
    async def test_cancel_before_start_and_recover(self):
        await self.runner.start(HistorySpec());await self.runner.stop()
        self.assertEqual(self.runner.summary()['state'],'cancelled')
        self.runner.job['state']='running';self.runner.save()
        other=HistoryRunner(lambda:self.store,lambda:self.runtime);other.recover()
        self.assertEqual(other.summary()['state'],'interrupted')
    async def test_flood_wait_stops_and_blocks_immediate_retry(self):
        self.runtime.client.get_entity=AsyncMock(side_effect=errors.FloodWaitError(request=None,capture=30))
        job=await self.run_job();self.assertEqual(job['state'],'partial');self.assertIn('30',job['error'])
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:await self.runner.start(HistorySpec())
        self.assertEqual(ctx.exception.status_code,429)
    async def test_disconnected_and_no_rules(self):
        self.runtime.me=None
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):await self.runner.start(HistorySpec())
        self.runtime.me=SimpleNamespace(id=99)
        with self.store.db:self.store.db.execute('UPDATE rules SET enabled=0')
        job=await self.run_job();self.assertEqual(job['read'],0);self.assertEqual(job['state'],'partial')

class ExplanationTests(unittest.TestCase):
    def test_word_boundaries_and_case_are_unchanged(self):
        rule=dict(keywords=['яхту'],required=['ищу'],excluded=['сдаем'])
        for text,reason in [('ИЩУ яхту!','matched'),('Ищу яхту, сдаём','excluded'),('Есть яхту','no_intent'),('Ищу яхтуподобное','no_keywords')]:
            result=explain(text,rule);self.assertEqual(result['reason'],reason)
            self.assertEqual(bool(matches(text,rule)),reason=='matched')
