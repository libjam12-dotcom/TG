import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store, now, matches
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from marketing import Marketing, DubaiProfile, Rule
from marketing_topics import seed_marketing
from signm_pipeline import OWNER_ID, PipelineSettings, Prepare, Stage, EditDraft, Confirm, AIDraft, DeliveryTest, DeliveryReconcile
from signm_pipeline import _assessed_text
from signm_intent import assess_request
from telethon import types


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.client_store = Store(Path(self.temp.name) / 'client.db')
        self.client_store.put('private_client_data', 'not-owner-data')
        self.baseline = list(self.client_store.db.iterdump())
        runtime = SimpleNamespace(client=None, me=None, discovery_next=0)
        self.m = Marketing(Path(self.temp.name) / 'owner.db', lambda: runtime, lambda: self.client_store)
        seed_marketing(self.m.store)
        self.m.inbox.initialize()
        self.p = self.m.pipeline
        with self.m.store.db:
            self.m.store.db.execute("INSERT INTO sources(id,title,username) VALUES(-42,'Бизнес в Дубае','dubai_business_test')")
        self.lid = self.insert('Ищу разработчика: нужен сайт для салона в Дубае. Есть ТЗ.')

    async def asyncTearDown(self):
        await self.m.shutdown()
        self.client_store.db.close()
        self.temp.cleanup()

    def insert(self, text, age=0, author='@buyer_test', sender=123, source=-42):
        index = self.m.store.db.execute('SELECT COUNT(*) FROM leads').fetchone()[0] + 1
        stamp = (datetime.now(timezone.utc) - timedelta(days=age)).isoformat()
        with self.m.store.db:
            return self.m.store.db.execute('INSERT INTO leads(chat_id,message_id,text,author,sender_id,link,created,found,fingerprint,matches) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (source,index,text,author,sender,f'https://t.me/dubai_business_test/{index}',stamp,now(),str(index),'[]')).lastrowid

    async def test_prepare_only_fresh_direct_personal_requests_and_never_sends(self):
        for text, kwargs in [
            ('Нужен сайт для вашего бизнеса? Создаю сайты, пишите мне!', {}),
            ('Ищу 2–3 проекта по автоматизации. Я разработчик.', {}),
            ('Нужен SEO специалист в штат, оклад 100000.', {}),
            ('Ищу разработчика сайта в Дубае.', {'age':8}),
            ('Ищу разработчика сайта в Дубае.', {'author':'@job_feed','sender':-123}),
        ]:
            self.insert(text, **kwargs)
        result = await self.p.prepare(Prepare(limit=20))
        self.assertEqual(result['prepared'], [self.lid])
        self.assertEqual((result['external_calls'], result['messages_sent']), (0,0))
        item = await self.p.detail(self.lid)
        self.assertTrue(item['eligible'])
        self.assertIn('Я Разработчикислав, SignM', item['draft'])
        self.assertEqual(item['draft_kind'], 'template')
        self.assertEqual((await self.p.prepare(Prepare()))['count'], 0)
        self.assertEqual(self.m.store.rows('SELECT * FROM outbox'), [])
        self.assertEqual(self.m.store.rows('SELECT * FROM conversation_messages'), [])
        self.assertEqual(list(self.client_store.db.iterdump()), self.baseline)

    async def test_draft_invalidated_when_source_message_changes(self):
        await self.p.prepare(Prepare())
        with self.m.store.db:self.m.store.db.execute('UPDATE leads SET text=? WHERE id=?', ('Предлагаю разработку сайтов.', self.lid))
        item = await self.p.detail(self.lid)
        self.assertFalse(item['eligible'])
        self.assertFalse(item['draft_current'])
        self.assertEqual(item['draft'], '')
        with self.assertRaises(HTTPException):
            await self.p.edit_draft(self.lid, EditDraft(text='Anything'))

    async def test_no_synthetic_contacted_or_replied_stage(self):
        for stage in ('contacted', 'replied'):
            with self.assertRaises(HTTPException) as ctx:
                await self.p.set_stage(self.lid, Stage(stage=stage))
            self.assertEqual(ctx.exception.status_code, 409)
        t = now()
        with self.m.store.db:
            cid = self.m.store.db.execute('INSERT INTO conversations(account_id,peer_id,access_hash,title,username,lead_id,rule_id,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',
                (OWNER_ID,123,'unused','Buyer','buyer_test',self.lid,1,t,t)).lastrowid
            self.m.store.db.execute('INSERT INTO conversation_leads VALUES(?,?)',(cid,self.lid))
            self.m.store.db.execute("INSERT INTO conversation_messages(cid,direction,kind,state,text,created) VALUES(?,'outgoing','manual','sent','Actual sent reply',?)",(cid,t))
        self.assertEqual((await self.p.set_stage(self.lid, Stage(stage='contacted')))['stage'], 'contacted')
        with self.m.store.db:self.m.store.db.execute("INSERT INTO conversation_messages(cid,direction,kind,state,text,created) VALUES(?,'incoming','customer','received','Actual response',?)",(cid,now()))
        self.assertEqual((await self.p.set_stage(self.lid, Stage(stage='replied')))['stage'], 'replied')

    async def test_payment_requires_real_details_and_explicit_owner_confirmation(self):
        with self.assertRaises(HTTPException):
            await self.p.set_stage(self.lid, Stage(stage='paid', amount='300', payment_reference='bank-ref-123', confirmed=True))
        await self.p.set_stage(self.lid, Stage(stage='proposal', amount='300', currency='USD'))
        with self.assertRaises(HTTPException):
            await self.p.set_stage(self.lid, Stage(stage='payment_pending', amount='300'))
        await self.p.settings_save(PipelineSettings(payment_instructions='Счёт выставляет Разработчик после согласования; реквизиты отдельно.'))
        await self.p.set_stage(self.lid, Stage(stage='payment_pending', amount='300'))
        for spec in [Stage(stage='paid', amount='300', confirmed=True),Stage(stage='paid', amount='300',payment_reference='bank-ref-123'),Stage(stage='paid',amount='NaN',payment_reference='bank-ref-123',confirmed=True)]:
            with self.assertRaises(HTTPException):await self.p.set_stage(self.lid,spec)
        result = await self.p.set_stage(self.lid,Stage(stage='paid',amount='300',payment_reference='bank-ref-123',confirmed=True))
        self.assertEqual(result['payment_verification'], 'owner_confirmation')
        self.assertEqual((await self.p.state())['counts']['paid'],1)

    async def test_zero_budget_blocks_paid_io_and_unknown_keeps_reservation(self):
        cfg=self.m.inbox.config();cfg.update(consent=True,provider_verified=True,model='test-model')
        self.m.store.put('conversation_config',cfg);self.m.store.put('conversation_api_key','test-only-not-a-real-key')
        with patch('owner_ai.structured',new_callable=AsyncMock) as call:
            with self.assertRaises(HTTPException):await self.p.ai_draft(self.lid,Confirm(confirmed=True))
            call.assert_not_awaited()
        await self.p.settings_save(PipelineSettings(ai_budget_usd=1,input_usd_per_million=1,output_usd_per_million=2))
        with patch('owner_ai.structured',new_callable=AsyncMock,side_effect=RuntimeError('uncertain network outcome')):
            with self.assertRaises(HTTPException):await self.p.ai_draft(self.lid,Confirm(confirmed=True))
        self.assertGreater(self.p.budget()['reserved_usd'],0)
        self.assertEqual((await self.p.detail(self.lid))['draft'],'')

    async def test_ai_draft_quotes_original_and_is_cached_without_send(self):
        cfg=self.m.inbox.config();cfg.update(consent=True,provider_verified=True,model='test-model')
        self.m.store.put('conversation_config',cfg);self.m.store.put('conversation_api_key','test-only-not-a-real-key')
        await self.p.settings_save(PipelineSettings(ai_budget_usd=1,input_usd_per_million=1,output_usd_per_million=2))
        decision=AIDraft(buyer_request=True,evidence='нужен сайт для салона в Дубае',text='Здравствуйте! Задача по сайту ещё актуальна?')
        with patch('owner_ai.structured',new_callable=AsyncMock,return_value=(decision,{'input_tokens':100,'output_tokens':20})) as call:
            a=await self.p.ai_draft(self.lid,Confirm(confirmed=True))
            b=await self.p.ai_draft(self.lid,Confirm(confirmed=True))
            self.assertEqual(a['draft'],b['draft']);self.assertEqual(call.await_count,1)
        self.assertEqual(self.m.store.rows('SELECT * FROM conversation_messages'),[])

    async def test_unknown_ai_attempt_cannot_be_silently_repeated(self):
        cfg=self.m.inbox.config();cfg.update(consent=True,provider_verified=True,model='test-model')
        self.m.store.put('conversation_config',cfg);self.m.store.put('conversation_api_key','test-key')
        await self.p.settings_save(PipelineSettings(ai_budget_usd=1,input_usd_per_million=1,output_usd_per_million=2))
        with patch('owner_ai.structured',new_callable=AsyncMock,side_effect=RuntimeError()) as call:
            for _ in range(2):
                with self.assertRaises(HTTPException):await self.p.ai_draft(self.lid,Confirm(confirmed=True))
            self.assertEqual(call.await_count,1)

    async def test_currency_survives_stage_only_change(self):
        await self.p.set_stage(self.lid,Stage(stage='proposal',amount='500',currency='AED'))
        await self.p.settings_save(PipelineSettings(payment_instructions='Actual public payment instructions'))
        result=await self.p.set_stage(self.lid,Stage(stage='payment_pending'))
        self.assertEqual((result['amount'],result['currency']),('500','AED'))

    async def test_explicit_other_geo_not_in_dubai_queue(self):
        other=self.insert('Ищу разработчика сайта для бизнеса в Москве.')
        item=await self.p.detail(other)
        self.assertEqual(item['source_geo'],'dubai')
        self.assertEqual(item['request_geo'],'other')
        self.assertFalse(item['eligible'])

    async def test_paid_response_after_consent_change_is_accounted_but_not_saved(self):
        cfg=self.m.inbox.config();cfg.update(consent=True,provider_verified=True,model='test-model')
        self.m.store.put('conversation_config',cfg);self.m.store.put('conversation_api_key','test-key')
        await self.p.settings_save(PipelineSettings(ai_budget_usd=1,input_usd_per_million=1,output_usd_per_million=2))
        async def changed(*args):
            current=self.m.inbox.config();current['consent']=False;self.m.store.put('conversation_config',current)
            return AIDraft(buyer_request=True,evidence='нужен сайт для салона в Дубае',text='Здравствуйте!'),{'input_tokens':100,'output_tokens':20}
        with patch('owner_ai.structured',side_effect=changed):
            with self.assertRaises(HTTPException):await self.p.ai_draft(self.lid,Confirm(confirmed=True))
        self.assertGreater(self.p.budget()['spent_usd'],0)
        self.assertEqual((await self.p.detail(self.lid))['draft'],'')

    async def test_explicit_profile_preserves_client_history_and_rule_edits(self):
        ids=[r['id'] for r in self.m.store.rules()]
        with self.assertRaises(HTTPException):await self.m.dubai_profile(DubaiProfile(sources=[-42]))
        await self.m.dubai_profile(DubaiProfile(sources=[-42],confirmed=True))
        rule=self.m.store.rules()[0]
        self.assertEqual([r['id'] for r in self.m.store.rules()],ids)
        self.assertEqual(rule['lead_profile'],'signm_dubai_v1')
        await self.m.rule_save(Rule(id=rule['id'],name=rule['name'],sources=[-42],keywords='\n'.join(rule['keywords'])))
        self.assertEqual(self.m.store.rules()[0]['lead_profile'],'signm_dubai_v1')
        self.assertEqual(list(self.client_store.db.iterdump()),self.baseline)

    async def test_dubai_profile_uses_semantic_intent_and_preserves_exact_exclusions(self):
        with self.m.store.db:
            self.m.store.db.executemany('INSERT INTO sources(id,title,username) VALUES(?,?,?)',[
                (-44,'Второй бизнес-чат','dubai_second'),(-43,'Третий бизнес-чат','dubai_third')])
            for rule in self.m.store.rules():
                value={k:v for k,v in rule.items() if k not in ('id','name','enabled')}
                value['excluded']+=['наша компания занимается','личное исключение']
                self.m.store.db.execute('UPDATE rules SET data=? WHERE id=?',(json.dumps(value,ensure_ascii=False),rule['id']))
        before=self.m.store.rules()
        leads_before=self.m.store.rows('SELECT * FROM leads')
        await self.m.dubai_profile(DubaiProfile(sources=[-44,-43,-42,-44],confirmed=True))
        after=self.m.store.rules()
        by_topic={r['topic']:r for r in after}
        cases=[
            ('smm','Компания ищет SMM для проекта в Дубае.'),
            ('smm','Наша компания ищет SMM для продвижения салона в Дубае.'),
            ('seo','Требуются услуги SEO для сайта клиники в Дубае.'),
            ('smm','Кто занимается Instagram в Дубае?'),
        ]
        for topic,text in cases:
            with self.subTest(buyer=text):
                self.assertTrue(matches(text,by_topic[topic]))
        for text in ['Наша компания предлагает SMM для бизнеса в Дубае.',
                     'Ищем клиентов на SMM в Дубае, пишите!',
                     'Наша компания ищет SMM в штат, оклад 6000 AED.']:
            with self.subTest(supplier_or_employment=text):
                self.assertFalse(matches(text,by_topic['smm']))
        self.assertEqual([r['id'] for r in after],[r['id'] for r in before])
        for old,new in zip(before,after):
            self.assertEqual(new['sources'],[-44,-43,-42])
            self.assertEqual(new['required'],[])
            self.assertEqual(new['excluded'],[p for p in old['excluded'] if p!='наша компания']+['наша компания предлагает'])
        self.assertIn('instagram',by_topic['smm']['keywords'])
        self.assertEqual(self.m.store.get('signm_dubai_source_ids'),[-44,-43,-42])
        self.assertEqual(self.m.store.rows('SELECT * FROM leads'),leads_before)
        self.assertEqual(list(self.client_store.db.iterdump()),self.baseline)
        await self.m.dubai_profile(DubaiProfile(sources=[-44,-43,-42],confirmed=True))
        self.assertEqual(self.m.store.rules(),after)

    async def test_pipeline_routes_remain_owner_only(self):
        app=FastAPI();app.include_router(self.m.router)
        with TestClient(app) as c:
            for path,method in [('/pipeline','GET'),('/pipeline/settings','POST'),('/pipeline/prepare','POST'),('/pipeline/1/draft','POST'),('/pipeline/1/stage','POST')]:
                r=c.request(method,'/api/scanner/marketing'+path,headers={'X-Panel-User':'client','X-Panel-Role':'admin'},json={})
                self.assertEqual(r.status_code,403)
            r=c.get('/api/scanner/marketing/pipeline',headers={'X-Panel-User':'owner','X-Panel-Role':'owner'})
            self.assertEqual(r.status_code,200);self.assertFalse(r.json()['sends_automatically'])

    async def test_state_query_budget_does_not_grow_with_500_leads(self):
        async def measured():
            queries=[]
            self.m.store.db.set_trace_callback(lambda sql: queries.append(sql) if sql.lstrip().upper().startswith('SELECT') else None)
            try:
                result=await self.p.state()
            finally:
                self.m.store.db.set_trace_callback(None)
            return result,len(queries)
        _,small=await measured()
        for index in range(500):
            self.insert(f'Ищу разработчика: нужен сайт для бизнеса в Дубае, проект {index}.')
        result,large=await measured()
        self.assertEqual(result['counts']['scanned'],501)
        self.assertEqual(result['counts']['eligible'],501)
        self.assertEqual(len(result['items']),100)
        self.assertLessEqual(large,small+2)
        self.assertLessEqual(large,20)

    async def test_other_http_request_runs_before_large_state_finishes(self):
        for index in range(500):
            self.insert(f'Ищу разработчика: нужен сайт в Дубае, проект {index}.')
        app=FastAPI();app.include_router(self.m.router)
        processed=0
        first=asyncio.Event()
        original=self.p.item
        def counted(*args,**kwargs):
            nonlocal processed
            processed+=1
            first.set()
            return original(*args,**kwargs)
        @app.get('/probe')
        async def probe():
            return {'processed':processed}
        headers={'X-Panel-User':'owner','X-Panel-Role':'owner'}
        with patch.object(self.p,'item',side_effect=counted):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                request=asyncio.create_task(client.get('/api/scanner/marketing/pipeline',headers=headers))
                await asyncio.wait_for(first.wait(),1)
                during=await client.get('/probe')
                self.assertGreater(during.json()['processed'],0)
                self.assertLess(during.json()['processed'],501)
                response=await request
                self.assertEqual(response.status_code,200)
                self.assertEqual(response.json()['counts']['scanned'],501)

    async def test_bulk_detail_consistency_and_source_edit_invalidates_saved_draft(self):
        await self.p.prepare(Prepare())
        t=now()
        with self.m.store.db:
            cid=self.m.store.db.execute('INSERT INTO conversations(account_id,peer_id,access_hash,title,username,lead_id,rule_id,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',
                (OWNER_ID,123,'unused','Buyer','buyer_test',self.lid,1,t,t)).lastrowid
            self.m.store.db.execute('INSERT INTO conversation_leads VALUES(?,?)',(cid,self.lid))
        detail=await self.p.detail(self.lid)
        listing=next(item for item in (await self.p.state())['items'] if item['lead_id']==self.lid)
        self.assertEqual(listing,{k:v for k,v in detail.items() if k!='events'})
        self.assertTrue(detail['draft_current'])
        self.assertEqual(detail['conversation_id'],cid)
        with self.m.store.db:
            self.m.store.db.execute("UPDATE sources SET title='Общий чат',username='general_chat' WHERE id=-42")
        self.m.store.put('signm_dubai_source_ids',[-42])
        changed=await self.p.detail(self.lid)
        listing=next(item for item in (await self.p.state())['items'] if item['lead_id']==self.lid)
        self.assertEqual(listing,{k:v for k,v in changed.items() if k!='events'})
        self.assertEqual(changed['source_geo'],'dubai')
        self.assertFalse(changed['draft_current'])
        self.assertEqual(changed['draft'],'')

    async def test_assessment_cache_is_bounded_but_edits_and_freshness_remain_live(self):
        _assessed_text.cache_clear()
        with patch('signm_pipeline.assess_request',wraps=assess_request) as assess:
            first=await self.p.detail(self.lid)
            self.assertTrue(first['eligible'])
            first['qualification']['services'].clear()
            cached=await self.p.detail(self.lid)
            self.assertIn('websites',cached['qualification']['services'])
            self.assertEqual(assess.call_count,1)
            old=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
            with self.m.store.db:
                self.m.store.db.execute('UPDATE leads SET created=? WHERE id=?',(old,self.lid))
            await self.p.settings_save(PipelineSettings(days=1))
            self.assertFalse((await self.p.detail(self.lid))['fresh'])
            await self.p.settings_save(PipelineSettings(days=3))
            self.assertTrue((await self.p.detail(self.lid))['eligible'])
            self.assertEqual(assess.call_count,1)
            with self.m.store.db:
                self.m.store.db.execute('UPDATE leads SET text=? WHERE id=?',('Предлагаю разработку сайтов в Дубае.',self.lid))
            changed=await self.p.detail(self.lid)
            self.assertFalse(changed['eligible'])
            self.assertEqual(changed['qualification']['label'],'supplier_offer')
            self.assertEqual(assess.call_count,2)
        self.assertEqual(_assessed_text.cache_info().maxsize,1024)

    async def test_prepare_no_eligible_501_has_fixed_query_budget_and_full_quota_skips_leads(self):
        with self.m.store.db:
            self.m.store.db.execute('UPDATE leads SET text=? WHERE id=?',('Предлагаю разработку сайтов в Дубае.',self.lid))
        async def measured():
            queries=[]
            self.m.store.db.set_trace_callback(lambda sql:queries.append(sql) if sql.lstrip().upper().startswith('SELECT') else None)
            try:
                result=await self.p.prepare(Prepare(limit=20))
            finally:
                self.m.store.db.set_trace_callback(None)
            return result,queries
        _,small=await measured()
        for index in range(500):
            self.insert(f'Предлагаю разработку сайтов в Дубае, предложение {index}.')
        result,large=await measured()
        self.assertEqual(result['count'],0)
        self.assertLessEqual(len(large),len(small)+2)
        self.assertLessEqual(len(large),10)
        with self.m.store.db:
            self.m.store.db.executemany("INSERT INTO owner_draft_usage(lead_id,kind,state,created) VALUES(?,'template','completed',?)",[(self.lid,now()) for _ in range(20)])
        result,spent=await measured()
        self.assertEqual(result['count'],0)
        self.assertFalse(any('FROM LEADS ' in sql.upper() for sql in spent))
        self.assertLessEqual(len(spent),2)

    async def test_prepare_yield_does_not_overwrite_new_manual_draft(self):
        for index in range(25):
            self.insert(f'Предлагаю разработку сайтов в Дубае, предложение {index}.')
        task=asyncio.create_task(self.p.prepare(Prepare(limit=20)))
        await asyncio.sleep(0)
        manual='Здравствуйте! Это мой персональный проверенный отклик.'
        await self.p.edit_draft(self.lid,EditDraft(text=manual))
        result=await task
        self.assertEqual(result['count'],0)
        self.assertEqual((await self.p.detail(self.lid))['draft'],manual)

    async def test_delivery_test_is_self_only_durable_and_never_a_lead(self):
        class Client:
            calls=0
            def is_connected(self):return True
            async def get_me(self):return SimpleNamespace(id=OWNER_ID,username='demo_identity_3')
            async def __call__(self,request):
                self.calls+=1
                self.request=request
                return types.UpdateShortSentMessage(id=77,pts=1,pts_count=1,date=datetime.now(timezone.utc),out=True)
            async def get_messages(self,peer,ids):
                assert isinstance(peer,types.InputPeerSelf)
                return types.Message(id=ids,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=datetime.now(timezone.utc),message=self.request.message,out=True)
        client=Client();self.m.sender.client=client;self.m.sender.me=SimpleNamespace(id=OWNER_ID,username='demo_identity_3')
        spec=DeliveryTest(request_key='test-signm-saved-123',text='SignM: контроль доставки в собственное Избранное.',confirmed=True)
        first=await self.p.test_delivery(spec);second=await self.p.test_delivery(spec)
        self.assertEqual((first['state'],first['verified']),('sent',1))
        self.assertEqual(first['telegram_message_id'],second['telegram_message_id'])
        self.assertEqual(client.calls,1)
        self.assertIsInstance(client.request.peer,types.InputPeerSelf)
        self.assertEqual(self.m.store.rows('SELECT * FROM owner_deals'),[])
        with self.assertRaises(HTTPException):await self.p.test_delivery(spec.model_copy(update={'request_key':'another-key-12345'}))
        self.m.sender.client=None;self.m.sender.me=None

    def delivery_fixture(self, response=None):
        class Client:
            def __init__(self):
                self.calls=0;self.reads=[];self.messages=None
                self.identity=SimpleNamespace(id=OWNER_ID,username='demo_identity_3')
                self.after_read=None
            def is_connected(self):return True
            async def get_me(self):return self.identity
            async def __call__(self,request):
                self.calls+=1;self.request=request
                return response(request)
            async def get_messages(self,peer,**kwargs):
                assert isinstance(peer,types.InputPeerSelf)
                self.reads.append(kwargs)
                if self.after_read:self.after_read()
                values=self.messages
                if values is None:
                    values=[types.Message(id=77,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=datetime.now(timezone.utc),message=self.request.message,out=True)]
                return values[0] if 'ids' in kwargs and values else None if 'ids' in kwargs else values
            async def disconnect(self):pass
        client=Client();self.m.sender.client=client;self.m.sender.me=SimpleNamespace(id=OWNER_ID,username='demo_identity_3')
        return client

    async def test_delivery_handles_real_telethon_update_variants_and_random_id_mapping(self):
        def message(request):
            return types.Message(id=77,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=datetime.now(timezone.utc),message=request.message,out=True)
        variants={
            'short_sent':lambda r:types.UpdateShortSentMessage(id=77,pts=1,pts_count=1,date=datetime.now(timezone.utc),out=True),
            'short_sent_without_optional_flag':lambda r:types.UpdateShortSentMessage(id=77,pts=1,pts_count=1,date=datetime.now(timezone.utc)),
            'short_mapping':lambda r:types.UpdateShort(types.UpdateMessageID(id=77,random_id=r.random_id),datetime.now(timezone.utc)),
            'short_new_message':lambda r:types.UpdateShort(types.UpdateNewMessage(message(r),pts=1,pts_count=1),datetime.now(timezone.utc)),
            'updates_mapping':lambda r:types.Updates(updates=[types.UpdateMessageID(id=88,random_id=r.random_id+1),types.UpdateMessageID(id=77,random_id=r.random_id)],users=[],chats=[],date=datetime.now(timezone.utc),seq=1),
            'combined_mapping':lambda r:types.UpdatesCombined(updates=[types.UpdateMessageID(id=77,random_id=r.random_id)],users=[],chats=[],date=datetime.now(timezone.utc),seq_start=1,seq=1),
            'updates_message':lambda r:types.Updates(updates=[types.UpdateNewMessage(message(r),pts=1,pts_count=1)],users=[],chats=[],date=datetime.now(timezone.utc),seq=1),
            'short_message':lambda r:types.UpdateShortMessage(id=77,user_id=OWNER_ID,message=r.message,pts=1,pts_count=1,date=datetime.now(timezone.utc),out=True),
        }
        for name,response in variants.items():
            with self.subTest(response=name):
                with self.m.store.db:self.m.store.db.execute('DELETE FROM owner_pipeline_delivery_tests')
                client=self.delivery_fixture(response)
                result=await self.p.test_delivery(DeliveryTest(request_key='test-response-'+name,text='SignM: offline response shape verification.',confirmed=True))
                self.assertEqual((result['state'],result['verified'],result['telegram_message_id']),('sent',1,77))
                self.assertEqual(client.calls,1)
                self.assertEqual(client.reads,[{'ids':77}])

    def uncertain_delivery(self, **changes):
        created=datetime.now(timezone.utc)-timedelta(minutes=2)
        row=dict(request_key='test-reconcile-uncertain',account_id=OWNER_ID,text='SignM: one previously attempted Saved Messages check.',random_id=123,state='uncertain',telegram_message_id=None,verified=0,created=created.isoformat(),updated=created.isoformat(),error='Original uncertain dispatch.')
        row.update(changes)
        with self.m.store.db:
            self.m.store.db.execute('DELETE FROM owner_pipeline_delivery_tests')
            self.m.store.db.execute('INSERT INTO owner_pipeline_delivery_tests('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',tuple(row.values()))
        message=types.Message(id=77,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=datetime.fromisoformat(row['created'])+timedelta(seconds=1),message=row['text'],out=True)
        return row,message

    async def test_reconcile_reads_existing_uncertain_test_without_sending_or_making_lead(self):
        row,message=self.uncertain_delivery()
        client=self.delivery_fixture();client.messages=[message]
        result=await self.p.reconcile_delivery(DeliveryReconcile(request_key=row['request_key'],confirmed=True))
        self.assertEqual((result['state'],result['verified'],result['telegram_message_id']),('sent',1,77))
        self.assertEqual(client.calls,0);self.assertEqual(client.reads,[{'limit':20}])
        cached=await self.p.reconcile_delivery(DeliveryReconcile(request_key=row['request_key'],confirmed=True))
        self.assertEqual(cached['telegram_message_id'],77)
        self.assertEqual(client.reads,[{'limit':20}])
        again=await self.p.test_delivery(DeliveryTest(request_key=row['request_key'],text=row['text'],confirmed=True))
        self.assertEqual(again['telegram_message_id'],77);self.assertEqual(client.calls,0)
        self.assertEqual(self.m.store.rows('SELECT * FROM owner_deals'),[])
        self.assertEqual(list(self.client_store.db.iterdump()),self.baseline)

    async def test_reconcile_known_id_is_read_by_id_and_requires_confirmation(self):
        row,message=self.uncertain_delivery(telegram_message_id=77)
        client=self.delivery_fixture();client.messages=[message]
        with self.assertRaises(HTTPException):
            await self.p.reconcile_delivery(DeliveryReconcile(request_key=row['request_key']))
        self.assertEqual(client.reads,[])
        result=await self.p.reconcile_delivery(DeliveryReconcile(request_key=row['request_key'],confirmed=True))
        self.assertEqual(result['state'],'sent');self.assertEqual(client.reads,[{'ids':77}]);self.assertEqual(client.calls,0)

    async def test_reconcile_rejects_wrong_account_text_peer_duplicates_expired_and_invalid_ids(self):
        cases=['account','username','row_account','text','peer','from','date','duplicates','expired','invalid_id','wrong_id','client_changed','forwarded','incoming','bad_saved_id','future_message','naive_message_date']
        for case in cases:
            with self.subTest(case=case):
                changes={'account_id':42} if case=='row_account' else {'created':(datetime.now(timezone.utc)-timedelta(hours=25)).isoformat()} if case=='expired' else {'telegram_message_id':0} if case=='invalid_id' else {'telegram_message_id':78} if case=='wrong_id' else {}
                row,message=self.uncertain_delivery(**changes)
                client=self.delivery_fixture();client.messages=[message]
                if case=='account':client.identity.id=42
                if case=='username':client.identity.username='different_sender'
                if case=='text':message.message='Another text'
                if case=='peer':message.peer_id=types.PeerUser(42)
                if case=='from':message.from_id=types.PeerUser(42)
                if case=='date':message.date-=timedelta(hours=1)
                if case=='incoming':message.out=False
                if case=='bad_saved_id':message.id=0
                if case=='future_message':message.date+=timedelta(minutes=3)
                if case=='naive_message_date':message.date=message.date.replace(tzinfo=None)
                if case=='duplicates':
                    client.messages.append(types.Message(id=78,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=message.date,message=row['text'],out=True))
                if case=='client_changed':client.after_read=lambda:setattr(self.m.sender,'client',None)
                if case=='forwarded':message.fwd_from=types.MessageFwdHeader(date=message.date,from_id=types.PeerUser(42))
                result=await self.p.reconcile_delivery(DeliveryReconcile(request_key=row['request_key'],confirmed=True))
                self.assertEqual((result['state'],result['verified']),('uncertain',0));self.assertEqual(client.calls,0)

    async def test_invalid_or_ambiguous_response_id_remains_uncertain_without_read_or_repeat(self):
        for case in ['bad_id','bad_random_id','ambiguous']:
            with self.subTest(case=case):
                with self.m.store.db:self.m.store.db.execute('DELETE FROM owner_pipeline_delivery_tests')
                def response(request):
                    updates=[types.UpdateMessageID(id=0 if case=='bad_id' else 77,random_id=request.random_id+1 if case=='bad_random_id' else request.random_id)]
                    if case=='bad_random_id':updates.append(types.UpdateNewMessage(types.Message(id=77,peer_id=types.PeerUser(OWNER_ID),from_id=types.PeerUser(OWNER_ID),date=datetime.now(timezone.utc),message=request.message,out=True),pts=1,pts_count=1))
                    if case=='ambiguous':updates.append(types.UpdateMessageID(id=78,random_id=request.random_id))
                    return types.Updates(updates=updates,users=[],chats=[],date=datetime.now(timezone.utc),seq=1)
                client=self.delivery_fixture(response)
                spec=DeliveryTest(request_key='test-response-bad-'+case,text='SignM: offline unknown delivery response.',confirmed=True)
                result=await self.p.test_delivery(spec);await self.p.test_delivery(spec)
                self.assertEqual((result['state'],result['verified']),('uncertain',0));self.assertEqual(client.calls,1);self.assertEqual(client.reads,[])


if __name__=='__main__':unittest.main()
