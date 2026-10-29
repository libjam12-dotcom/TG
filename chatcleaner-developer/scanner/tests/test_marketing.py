import asyncio
from datetime import datetime,timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from marketing import Marketing, Source, Assignment, Enabled, Rule, LeadStatus, Preview
from marketing_topics import seed_marketing
from engine import Store, matches
from directory import SearchSpec, ExportSpec
from history import HistorySpec
from telethon import types,functions,errors
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient

class MarketingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.base=Store(Path(self.tmp.name)/'client.db')
        self.entity=types.Channel(id=700,title='Внешний бизнес-чат',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),username='external_dubai',megagroup=True,left=True)
        owner=self
        class Client:
            messages=[];calls=0;failure=None
            def is_connected(self):return True
            async def get_entity(self,ref):return owner.entity
            async def get_messages(self,*a,**k):
                self.calls+=1
                if self.failure:raise self.failure
                return self.messages[-1:]
            async def iter_messages(self,*a,**k):
                for m in self.messages:
                    if m.id>k.get('min_id',0):yield m
            async def __call__(self,req):
                self.calls+=1
                return SimpleNamespace(chats=[owner.entity])
        self.client=Client();self.rt=SimpleNamespace(client=self.client,me=types.User(id=42,first_name='Connected'),discovery_next=0,report_error=lambda e:None)
        self.m=Marketing(Path(self.tmp.name)/'owner.db',lambda:self.rt,lambda:self.base)
        seed_marketing(self.m.store);self.m.directory.pause=0;self.m.history.pause=0;self.m.scan_pause=0
        self.base.put('api',{'id':999,'hash':'client-secret-do-not-copy'});self.base.put('destination',123);self.base.put('running',True)
        self.before=self.base.db.iterdump();self.baseline=list(self.before)
    async def asyncTearDown(self):
        await self.m.shutdown();self.base.db.close();self.tmp.cleanup()
    async def configured(self):
        result=await self.m.add_source(Source(reference='https://t.me/external_dubai'))
        await self.m.assign(Assignment(sources=[result['id']],rules=[r['id'] for r in self.m.store.rules()]))
        await self.m.running(Enabled(enabled=True));return result['id']
    def message(self,id,text):
        return SimpleNamespace(id=id,message=text,out=False,date=datetime.now(timezone.utc),sender_id=77,get_sender=AsyncMock(return_value=types.User(id=77,first_name='Запрос',username='requester')))
    async def test_nine_topics_and_no_client_credentials(self):
        self.assertEqual(len(self.m.store.rules()),9)
        self.assertIsNone(self.m.store.get('api'));self.assertIsNone(self.m.store.get('destination'))
        self.assertEqual(list(self.base.db.iterdump()),self.baseline)
        self.assertTrue(all(not r['enabled'] for r in self.m.store.rules()))
    async def test_live_demand_isolated_and_no_outbound(self):
        await self.configured();self.client.messages=[self.message(5,'Нужен сайт для бизнеса в Дубае. Кто может помочь?'),self.message(6,'Предлагаем услуги: сделаем сайт, пишите нам.')]
        await self.m.scan();await self.m.scan()
        leads=self.m.store.rows('SELECT * FROM leads');self.assertEqual(len(leads),1)
        self.assertEqual(leads[0]['message_id'],5);self.assertEqual(self.m.store.rows('SELECT * FROM outbox'),[])
        self.assertEqual(list(self.base.db.iterdump()),self.baseline)
        await self.m.lead_status(leads[0]['id'],LeadStatus(status='working'))
        state=await self.m.state('working',0,0);self.assertEqual(state['filtered_total'],1)
    async def test_history_import_does_not_move_live_cursor_or_notify(self):
        cid=await self.configured();self.client.messages=[self.message(8,'Ищем маркетолога для запуска нового магазина')]
        await self.m.history.start(HistorySpec(sources=[cid],days=3,limit=10,mode='import'));await self.m.history.task
        self.assertEqual(self.m.history.job['state'],'completed');self.assertEqual(self.m.history.job['imported'],1)
        self.assertEqual(self.m.store.db.execute('SELECT cursor FROM sources').fetchone()[0],0)
        self.assertEqual(self.m.store.rows('SELECT * FROM outbox'),[]);self.assertEqual(list(self.base.db.iterdump()),self.baseline)
    async def test_owner_guard_every_route_including_exports(self):
        app=FastAPI();app.include_router(self.m.router)
        with TestClient(app) as c:
            for route in self.m.router.routes:
                path=route.path.replace('{source_id}','1').replace('{rule_id}','1').replace('{lead_id}','1').replace('{run_id}','1')
                for role in ('admin',''):
                    with self.subTest(path=path,role=role):
                        response=c.request(next(iter(route.methods)),path,json={},headers={'X-Panel-User':'owner','X-Panel-Role':role})
                        self.assertEqual(response.status_code,403)
            self.assertEqual(c.get('/api/scanner/marketing/state',headers={'X-Panel-Role':'owner'}).status_code,403)
            response=c.get('/api/scanner/marketing/state',headers={'X-Panel-User':'owner','X-Panel-Role':'owner'})
            self.assertEqual(response.status_code,200);self.assertEqual(len(response.json()['rules']),9)
    async def test_account_switch_stops_reads(self):
        await self.configured();self.rt.me=types.User(id=99,first_name='Other')
        self.client.messages=[self.message(5,'Нужен сайт')];await self.m.scan()
        self.assertEqual(self.m.store.rows('SELECT * FROM leads'),[])
        with self.assertRaises(HTTPException):await self.m.history.start(HistorySpec())
        self.assertEqual((await self.m.state('all',0,0))['connection'],'not_connected')
    async def test_read_transport_rejects_send_and_join(self):
        for request in (functions.messages.SendMessageRequest(peer='me',message='never',random_id=123),functions.channels.JoinChannelRequest(channel='external_dubai')):
            with self.assertRaises(HTTPException) as ctx:await self.m.client(request)
            self.assertEqual(ctx.exception.status_code,403)
        self.assertEqual(self.client.calls,0)
    async def test_admin_or_private_sources_rejected(self):
        self.entity.creator=True
        with self.assertRaises(HTTPException):await self.m.add_source(Source(reference='external_dubai'))
        self.entity.creator=False;self.entity.username=None
        with self.assertRaises(HTTPException):await self.m.add_source(Source(reference='external_dubai'))
        self.assertEqual(self.m.store.rows('SELECT * FROM sources'),[])
    async def test_new_admin_source_disabled_before_read(self):
        await self.configured();self.entity.creator=True;self.client.messages=[self.message(5,'Нужен сайт')]
        await self.m.scan();self.assertEqual(self.m.store.rows('SELECT * FROM leads'),[])
        self.assertEqual(self.m.store.db.execute('SELECT enabled FROM sources').fetchone()[0],0)
    async def test_flood_wait_shared_but_regular_search_delay_allowed(self):
        self.rt.discovery_next=time.time()+5
        self.assertIs(self.m.client.current(),self.client)
        self.client.failure=errors.FloodWaitError(request=None,capture=20)
        with self.assertRaises(errors.FloodWaitError):await self.m.client.get_messages(self.entity,limit=1)
        self.assertGreater(self.base.get('scan_not_before'),time.time()+19)
        with self.assertRaises(HTTPException) as ctx:await self.m.client.get_messages(self.entity,limit=1)
        self.assertEqual(ctx.exception.status_code,429)
    async def test_parser_exports_only_marketing_results(self):
        await self.m.directory.start(SearchSpec(queries=['Dubai business'],kind='group',limit=50))
        await self.m.directory.task
        report=await self.m.directory.detail(self.m.directory.active_id)
        self.assertEqual(report['count'],1)
        txt=await self.m.directory.export(report['id'],ExportSpec(format='txt'))
        self.assertIn(b'https://t.me/external_dubai',txt.body)
        self.assertEqual(list(self.base.db.iterdump()),self.baseline)
    async def test_remove_only_personal_assignment(self):
        cid=await self.configured();await self.m.source_remove(cid)
        self.assertTrue(all(not r['sources'] and not r['enabled'] for r in self.m.store.rules()))
        self.assertEqual(list(self.base.db.iterdump()),self.baseline)
    async def test_topic_examples(self):
        examples={'websites':'Нужен сайт для компании','ecommerce':'Ищем разработчика на битрикс','bots':'Нужен телеграм бот для записи','automation':'Нужна автоматизация заявок','crm':'Кто может настроить CRM для салона','marketing':'Нужен маркетолог в Дубае','seo':'Нужен SEO для сайта','smm':'Ищу smm специалиста для проекта','ads':'Посоветуйте таргетолога'}
        for rule in self.m.store.rules():
            self.assertTrue(matches(examples[rule['topic']],rule),rule['name'])
            self.assertFalse(matches('Предлагаю услуги: '+examples[rule['topic']],rule),rule['name'])
    async def test_csv_formula_escaped(self):
        await self.configured();self.client.messages=[self.message(4,'= Нужен сайт')];await self.m.scan()
        content=(await self.m.export()).body.decode();self.assertIn("'= Нужен сайт",content)
    async def test_pause_prevents_ingestion(self):
        await self.configured();await self.m.running(Enabled(enabled=False));self.client.messages=[self.message(4,'Нужен сайт')]
        await self.m.scan();self.assertEqual(self.m.store.rows('SELECT * FROM leads'),[])

    async def test_fifty_lead_pages_keep_all_results_and_filters(self):
        cid=await self.configured()
        for i in range(63):self.m.store.ingest(cid,i+1,f'Нужен сайт для проекта {i}')
        first=await self.m.state('new',0,0)
        self.assertEqual(len(first['leads']),50);self.assertEqual(first['next_offset'],50)
        last=await self.m.state('new',0,50)
        self.assertEqual(len(last['leads']),13);self.assertIsNone(last['next_offset'])
        self.assertEqual(len({r['id'] for r in first['leads']+last['leads']}),63)
        empty=await self.m.state('working',0,50)
        self.assertEqual(empty['offset'],0);self.assertEqual(empty['leads'],[])

    async def test_cached_backlog_yields_before_all_messages_are_processed(self):
        await self.configured()
        self.client.messages=[self.message(i+1,'Просто разговор без запроса') for i in range(80)]
        counts=[]
        async def heartbeat():
            await asyncio.sleep(0)
            counts.append(self.m.store.db.execute('SELECT messages_checked FROM sources').fetchone()[0])
        task=asyncio.create_task(heartbeat())
        await self.m.scan();await task
        self.assertLess(counts[0],80)

    async def test_cached_match_is_rechecked_after_rule_edit(self):
        cid=await self.configured();self.m.store.ingest(cid,88,'Нужен сайт для кофейни')
        first=await self.m.state('new',0,0)
        self.assertTrue(first['leads'][0]['matches_current_rules'])
        rule=next(r for r in self.m.store.rules() if r['topic']=='websites')
        await self.m.rule_save(Rule(id=rule['id'],name=rule['name'],sources=rule['sources'],
            keywords='нужен сайт',required='нужен',excluded='кофейни'))
        updated=await self.m.state('new',0,0)
        self.assertFalse(updated['leads'][0]['matches_current_rules'])

    async def test_flood_wait_resumes_source_and_keeps_pause_visible(self):
        cid=await self.configured();seen=[];blocked=[True]
        with self.m.store.db:
            self.m.store.db.execute("INSERT INTO sources(id,title,username) VALUES(?, 'Second','external_second')",(cid+1,))
        await self.m.assign(Assignment(sources=[cid+1],rules=[1]))
        async def messages(entity,**kwargs):
            source=self.m.store.get('scan_resume_source')
            # Entity resolution below gives a unique ID for each source.
            seen.append(entity.id)
            if entity.id==cid+1 and blocked[0]:
                blocked[0]=False;raise errors.FloodWaitError(request=None,capture=5)
            if False:yield None
        self.client.get_entity=AsyncMock(side_effect=lambda i:SimpleNamespace(id=i,creator=False,admin_rights=None))
        self.client.iter_messages=messages
        await self.m.scan()
        self.assertEqual(seen,[cid,cid+1]);self.assertEqual(self.m.store.get('scan_resume_source'),cid+1)
        self.assertEqual(self.m.store.get('scan_health')['cycle']['state'],'telegram_pause')
        error=self.m.store.get('error');await self.m.scan();self.assertEqual(self.m.store.get('error'),error)
        self.m.store.put('scan_not_before',0);self.base.put('scan_not_before',0);self.rt.discovery_next=0
        seen.clear();await self.m.scan();self.assertEqual(seen,[cid+1,cid])
        self.assertIsNone(self.m.store.get('error'))

    async def test_scan_counts_exclusions_checkpoints_and_keeps_last_success(self):
        cid=await self.configured()
        self.client.messages=[self.message(1,'Нужен сайт для проекта'),self.message(2,'Предлагаю услуги: нужен сайт?'),self.message(3,'Доброе утро')]
        await self.m.scan();health=self.m.store.get('scan_health')['cycle']
        self.assertEqual({k:health[k] for k in ('checked','matched','saved','no_keywords','excluded')},
                         dict(checked=3,matched=1,saved=1,no_keywords=1,excluded=1))
        self.assertEqual(sum(health['details'].values()),1)
        source=self.m.store.rows('SELECT * FROM sources WHERE id=?',(cid,))[0]
        self.assertEqual((source['cursor'],source['messages_checked']),(3,3))
        await self.m.scan();self.assertEqual(self.m.store.get('scan_health')['cycle']['checked'],0)
        self.assertEqual(len(self.m.store.rows('SELECT * FROM leads')),1)

    async def test_failed_batch_flushes_only_completed_messages_before_retry(self):
        cid=await self.configured()
        async def messages(entity,**kwargs):
            yield self.message(1,'Обычный разговор')
            raise TimeoutError('read failed')
        self.client.iter_messages=messages
        await self.m.scan()
        source=self.m.store.rows('SELECT * FROM sources WHERE id=?',(cid,))[0]
        self.assertEqual((source['cursor'],source['messages_checked']),(1,1))
        self.assertIsNotNone(source['error']);self.assertEqual(self.m.store.get('scan_health')['cycle']['state'],'partial')

if __name__=='__main__':unittest.main()
