import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store, words, matches

class MatchingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'test.db')
        self.store.put('running',True)
        self.store.put('destination',-999)
        with self.store.db:
            self.store.db.execute("INSERT INTO sources(id,title) VALUES(-1001,'Test')")
            self.store.db.execute("INSERT INTO sources(id,title) VALUES(-1002,'Other')")
        self.rule={'sources':[-1001], 'keywords':words('ЯХТУ\n\nяхту\nкатер'), 'required':words('ищу\nнужна'), 'excluded':words('\nсдаём\n'), 'substring':False}
        with self.store.db:self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Яхты',json.dumps(self.rule)))
    def tearDown(self):
        self.store.db.close();self.tmp.cleanup()
    def ingest(self,chat=-1001,msg=1,text='ИЩУ яхту!',sender=123):
        return self.store.ingest(chat,msg,text,sender_id=sender)
    def test_normalization_and_exclusions(self):
        self.assertEqual(self.rule['keywords'],['яхту','катер'])
        self.assertTrue(matches('ИЩУ ЯХТУ',self.rule))
        self.assertFalse(matches('ищу яхту, СДАЕМ яхты',self.rule))
        self.assertFalse(matches('ищу катерину',self.rule))
        self.assertFalse(matches('продаю яхту',self.rule))
    def test_multiple_rules_one_lead_one_outbox(self):
        with self.store.db:self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Второй',json.dumps(self.rule)))
        lead=self.ingest()
        self.assertIsNotNone(lead)
        self.assertEqual(len(json.loads(self.store.rows('SELECT matches FROM leads')[0]['matches'])),2)
        self.assertEqual(len(self.store.rows('SELECT * FROM outbox')),1)
        self.assertIsNone(self.ingest())
    def test_source_allowlist_pause_and_destination(self):
        self.assertIsNone(self.ingest(chat=-1002))
        self.assertIsNone(self.ingest(chat=-2000))
        self.store.put('destination',-1001)
        self.assertIsNone(self.ingest())
        self.store.put('destination',-999);self.store.put('running',False)
        self.assertIsNone(self.ingest())
    def test_restart_restores_settings_and_prevents_duplicates(self):
        self.ingest();self.store.db.close()
        self.store=Store(Path(self.tmp.name)/'test.db')
        self.assertTrue(self.store.get('running'))
        self.assertEqual(len(self.store.rules()),1)
        self.assertIsNone(self.ingest())
    def test_same_author_repost_but_different_author_is_new(self):
        self.assertIsNotNone(self.ingest())
        self.assertIsNone(self.ingest(msg=2))
        self.assertIsNotNone(self.ingest(msg=3,sender=456))
    def test_edits_apply_immediately(self):
        self.assertIsNotNone(self.ingest())
        self.rule['keywords']=['сайт']
        with self.store.db:self.store.db.execute('UPDATE rules SET data=?',(json.dumps(self.rule),))
        self.assertIsNone(self.ingest(msg=2,text='нужна яхту завтра'))
        self.assertIsNotNone(self.ingest(msg=3,text='ищу сайт'))
    def test_disabled_source_and_rule(self):
        with self.store.db:self.store.db.execute('UPDATE sources SET enabled=0')
        self.assertIsNone(self.ingest())
        with self.store.db:
            self.store.db.execute('UPDATE sources SET enabled=1');self.store.db.execute('UPDATE rules SET enabled=0')
        self.assertIsNone(self.ingest())
    def test_substring_is_opt_in(self):
        self.rule['keywords']=['яхт']
        self.assertFalse(matches('ищу яхту',self.rule))
        self.rule['substring']=True
        self.assertTrue(matches('ищу яхту',self.rule))

class APITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();os.environ['SCANNER_DATA']=cls.tmp.name
        import app
        cls.mod=app
        # This HTTP fixture deliberately does not run lifespan. Do not reuse callbacks
        # from an earlier TestClient lifespan whose Operations database is now closed.
        cls.previous_runtime=app.runtime;app.runtime=app.Runtime()
        from fastapi.testclient import TestClient
        cls.client=TestClient(app.app,raise_server_exceptions=False)
        cls.headers={'X-Panel-User':'tester','Origin':app.ORIGIN,'X-Scanner-Request':'1'}
    @classmethod
    def tearDownClass(cls):
        cls.client.close();cls.mod.runtime=cls.previous_runtime;cls.mod.store.db.close();cls.tmp.cleanup()
    def test_file_import_auth_csrf_limits_and_secret_errors(self):
        url='/api/scanner/operations/accounts/import-file'
        self.assertEqual(self.client.post(url,json={}).status_code,401)
        self.assertEqual(self.client.post(url,headers={**self.headers,'X-Panel-Role':'manager'},json={}).status_code,403)
        self.assertEqual(self.client.post(url,headers={'X-Panel-User':'tester','X-Panel-Role':'admin'},json={}).status_code,403)
        r=self.client.post(url,headers={**self.headers,'X-Panel-Role':'admin'},json={'passcode':'private-secret'})
        self.assertEqual(r.status_code,422);self.assertNotIn('private-secret',r.text)
    def test_bot_notification_auth_csrf_and_secret_validation(self):
        url='/api/scanner/bot-notifications'
        self.assertEqual(self.client.get(url).status_code,401)
        self.assertEqual(self.client.get(url,headers={**self.headers,'X-Panel-Role':'manager'}).status_code,403)
        self.assertEqual(self.client.post(url+'/config',headers={'X-Panel-User':'tester','X-Panel-Role':'owner'},json={}).status_code,403)
        r=self.client.post(url+'/config',headers={**self.headers,'X-Panel-Role':'owner'},json={'token':'secret-do-not-echo'})
        self.assertEqual(r.status_code,422);self.assertNotIn('secret-do-not-echo',r.text)
        r=self.client.get(url,headers={**self.headers,'X-Panel-Role':'owner'})
        self.assertEqual(r.status_code,200);self.assertNotIn('token',r.json().keys())
    def test_client_rules_and_assign_accept_70_sources(self):
        from app import Rule,AttachSources
        ids=list(range(-100,-170,-1))
        self.assertEqual(len(Rule(name='Test',sources=ids,keywords='DJ').sources),70)
        self.assertEqual(len(AttachSources(sources=ids,rules=[1]).sources),70)
    def test_operations_require_authenticated_admin(self):
        self.assertEqual(self.client.get('/api/scanner/operations').status_code,401)
        self.assertEqual(self.client.get('/api/scanner/operations',headers={'X-Panel-User':'tester','X-Panel-Role':'manager'}).status_code,403)
    def test_operations_csrf_and_secret_validation(self):
        url='/api/scanner/operations/accounts/import'
        self.assertEqual(self.client.post(url,headers={'X-Panel-User':'tester','X-Panel-Role':'owner'},json={}).status_code,403)
        headers={**self.headers,'X-Panel-Role':'owner'}
        result=self.client.post(url,headers=headers,json={'payload':'private-session-secret','authorized':True})
        self.assertEqual(result.status_code,422);self.assertNotIn('private-session-secret',result.text)
        result=self.client.post(url,headers=headers,json={'payload':{'excess':'x'*66000},'authorized':True})
        self.assertEqual(result.status_code,413)
    def test_no_proxy_auth(self):
        self.assertEqual(self.client.get('/api/scanner/state').status_code,401)
    def test_csrf_required(self):
        self.assertEqual(self.client.post('/api/scanner/running',headers={'X-Panel-User':'tester'},json={'enabled':False}).status_code,403)
        bad={**self.headers,'Origin':'https://evil.invalid'}
        self.assertEqual(self.client.post('/api/scanner/running',headers=bad,json={'enabled':False}).status_code,403)
    def test_cannot_start_without_account(self):
        self.assertEqual(self.client.post('/api/scanner/running',headers=self.headers,json={'enabled':True}).status_code,409)
    def test_validation_does_not_echo_secrets(self):
        result=self.client.post('/api/scanner/credentials',headers=self.headers,json={'api_id':1,'api_hash':'sensitive-secret'})
        self.assertEqual(result.status_code,422);self.assertNotIn('sensitive-secret',result.text)
    def test_save_rule_preview_and_edit(self):
        with self.mod.store.db:self.mod.store.db.execute("INSERT OR IGNORE INTO sources(id,title) VALUES(-1003,'Test')")
        payload={'name':'Тест','sources':[-1003],'keywords':'Яхту\n','required':'ищу','excluded':''}
        self.assertEqual(self.client.post('/api/scanner/rules',headers=self.headers,json=payload).status_code,200)
        state=self.client.get('/api/scanner/state',headers=self.headers).json()
        rid=state['rules'][-1]['id']
        result=self.client.post('/api/scanner/preview',headers=self.headers,json={'rule_id':rid,'text':'Ищу яхту'}).json()
        self.assertTrue(result['matched'])
        self.assertEqual(self.mod.store.rows('SELECT * FROM leads'),[])
        self.client.post('/api/scanner/rules',headers=self.headers,json={**payload,'id':rid,'keywords':'катер'})
        result=self.client.post('/api/scanner/preview',headers=self.headers,json={'rule_id':rid,'text':'Ищу яхту'}).json()
        self.assertFalse(result['matched'])
    def test_state_never_exposes_api_credentials(self):
        self.mod.store.put('api',{'id':123,'hash':'secret-should-not-appear'})
        response=self.client.get('/api/scanner/state',headers=self.headers)
        self.assertNotIn('secret-should-not-appear',response.text)
        self.mod.store.put('api',None)
    def test_outgoing_messages_are_ignored(self):
        asyncio.run(self.mod.runtime.consume(-1003,SimpleNamespace(out=True,message='ищу яхту')))
        self.assertEqual(self.mod.store.rows('SELECT * FROM leads'),[])

if __name__=='__main__':unittest.main()

class DeliveryTests(unittest.TestCase):
    def test_retry_keeps_telegram_id_and_sends_single_outbox_record(self):
        from unittest.mock import patch, AsyncMock
        from telethon import errors
        import app
        with tempfile.TemporaryDirectory() as folder:
            db=Store(Path(folder)/'delivery.db')
            db.put('running',True);db.put('destination',-999)
            rule={'sources':[-1001],'keywords':['яхту'],'required':[],'excluded':[],'substring':False}
            with db.db:
                db.db.execute("INSERT INTO sources(id,title) VALUES(-1001,'Источник')")
                db.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Яхты',json.dumps(rule)))
            db.ingest(-1001,1,'Ищу яхту',sender_id=123)
            sent=[]
            class FakeClient:
                def is_connected(self):return True
                async def get_input_entity(self,target):return None
                async def __call__(self,request):
                    sent.append(request)
                    if len(sent)==1:raise errors.FloodWaitError(request=None,capture=3)
            runtime=app.Runtime();runtime.client=FakeClient();runtime.me=SimpleNamespace(id=1)
            async def once():
                with patch.object(app.asyncio,'sleep',new=AsyncMock(side_effect=asyncio.CancelledError)):
                    try:await runtime.outbox_loop()
                    except asyncio.CancelledError:pass
            with patch.object(app,'store',db):
                asyncio.run(once())
                self.assertEqual(db.rows('SELECT * FROM outbox')[0]['state'],'pending')
                with db.db:db.db.execute('UPDATE outbox SET next_at=0')
                asyncio.run(once())
                self.assertEqual(db.rows('SELECT * FROM outbox')[0]['state'],'sent')
                self.assertEqual(sent[0].random_id,sent[1].random_id)
                self.assertEqual(len(db.rows('SELECT * FROM outbox')),1)
            db.db.close()
