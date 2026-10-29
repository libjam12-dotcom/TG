"""HTTP-to-worker acceptance checks; Telegram transport is deliberately simulated."""
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store
from telethon import errors, types, utils


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ.setdefault('SCANNER_DATA', self.tmp.name)
        import app
        self.app = app
        self.db = Store(Path(self.tmp.name) / 'workflow.db')
        self.runtime = app.Runtime()
        self.group = types.Channel(id=12345, title='Тестовый источник', photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), megagroup=True, username='testsource', left=False)
        self.source_id = utils.get_peer_id(self.group)
        self.sent = []
        owner = self

        class FakeTelegram:
            authorized = False
            online = False
            two_factor = True
            backlog = []
            def __init__(self, *args, **kwargs): pass
            def add_event_handler(self, *args): pass
            async def connect(self): self.online = True
            async def disconnect(self): self.online = False
            def is_connected(self): return self.online
            async def is_user_authorized(self): return self.authorized
            async def get_me(self): return types.User(id=42, first_name='Тест', username='testowner', is_self=True)
            async def send_code_request(self, phone): return SimpleNamespace(phone_code_hash='test-hash')
            async def sign_in(self, **kwargs):
                if 'password' in kwargs:
                    if kwargs['password'] != 'test-password': raise errors.PasswordHashInvalidError(request=None)
                else:
                    if kwargs['code'] != '12345': raise errors.PhoneCodeInvalidError(request=None)
                    if self.two_factor: raise errors.SessionPasswordNeededError(request=None)
                self.authorized = True
            async def iter_dialogs(self, **kwargs):
                yield SimpleNamespace(id=owner.source_id, name=owner.group.title, entity=owner.group, is_group=True, is_channel=True)
            async def get_entity(self, reference): return owner.group
            async def get_messages(self, entity, **kwargs): return [SimpleNamespace(id=10)]
            async def iter_messages(self, chat_id, **kwargs):
                for message in self.backlog:
                    if message.id > kwargs['min_id']: yield message
            async def get_input_entity(self, destination): return types.InputPeerSelf()
            async def send_message(self, destination, text, **kwargs): owner.sent.append(text)
            async def __call__(self, request): owner.sent.append(request.message)
            async def log_out(self): self.authorized = False; self.online = False

        self.fake_type = FakeTelegram
        self.patches = [patch.object(app, 'store', self.db), patch.object(app, 'runtime', self.runtime), patch.object(app, 'TelegramClient', FakeTelegram)]
        for item in self.patches: item.start()
        app.bot_notifications.initialize()
        from fastapi.testclient import TestClient
        self.client = TestClient(app.app, raise_server_exceptions=False)
        self.headers = {'X-Panel-User':'tester', 'Origin':app.ORIGIN, 'X-Scanner-Request':'1'}

    def tearDown(self):
        self.client.close()
        for item in reversed(self.patches): item.stop()
        self.db.db.close()
        self.tmp.cleanup()

    def post(self, route, payload, status=200):
        response = self.client.post('/api/scanner/' + route, json=payload, headers=self.headers)
        self.assertEqual(response.status_code, status, response.text)
        return response.json()

    def state(self): return self.client.get('/api/scanner/state', headers=self.headers).json()

    def test_more_than_ten_rules_preserves_existing_and_assignment(self):
        with self.db.db:
            self.db.db.execute('INSERT INTO sources(id,title,username) VALUES(?,?,?)',(self.source_id,'Test','testsource'))
        spec={'name':'Direction','sources':[self.source_id],'keywords':'звук','required':'нужен','enabled':True}
        for i in range(25):
            self.post('rules',{**spec,'name':f'Direction {i}'})
        before=self.db.rules()
        self.post('rules',spec,400)
        self.assertEqual(before,self.db.rules())
        self.post('assign',{'sources':[self.source_id],'rules':[r['id'] for r in before]})
        self.post('rules',{**spec,'id':before[0]['id'],'name':'Edited'})
        self.assertEqual(len(self.db.rules()),25)

    def login(self, two_factor=True):
        self.post('credentials', {'api_id':12345, 'api_hash':'0'*32})
        self.runtime.client.two_factor = two_factor
        self.post('auth/code', {'phone':'+79990000000'})
        self.assertEqual(self.state()['connection'], 'code_required')
        self.post('auth/confirm', {'code':'99999'}, 400)
        result = self.post('auth/confirm', {'code':'12345'})
        if two_factor:
            self.assertEqual(result['step'], 'password')
            self.post('auth/password', {'password':'wrong'}, 400)
            self.post('auth/password', {'password':'test-password'})
        self.assertEqual(self.state()['connection'], 'connected')
        self.assertIsNone(self.runtime.code_hash)

    def configure(self):
        self.post('sources', {'reference':'https://t.me/testsource'})
        self.post('rules', {'name':'Яхты', 'sources':[self.source_id], 'keywords':'яхту\nкатер', 'required':'ищу\nнужна', 'excluded':'предлагаем'})
        self.post('destination', {'reference':'me'})
        self.post('running', {'enabled':True})

    def message(self, number, text, outgoing=False):
        return SimpleNamespace(id=number, message=text, out=outgoing, sender_id=100, date=datetime.now(timezone.utc), get_sender=AsyncMock(return_value=types.User(id=100, username='potentialclient')))

    def deliver_once(self):
        async def run():
            with patch.object(self.app.asyncio, 'sleep', new=AsyncMock(side_effect=asyncio.CancelledError)):
                try: await self.runtime.outbox_loop()
                except asyncio.CancelledError: pass
        asyncio.run(run())

    def test_full_login_sources_rules_live_event_delivery_status_restart_disconnect(self):
        self.login()
        dialogs = self.client.get('/api/scanner/dialogs', headers=self.headers).json()['dialogs']
        self.assertEqual(dialogs[0]['id'], self.source_id)
        self.configure()
        self.post('notification-test', {})
        self.assertEqual(len(self.sent), 1)
        self.post('destination', {'reference':str(self.source_id)}, 400)
        for number, text, outgoing in [(11,'Ищу яхту на завтра',False), (12,'Ищу яхту, предлагаем аренду',False), (13,'Ищу яхту на завтра',False), (14,'Нужна яхту на субботу',True)]:
            asyncio.run(self.runtime.consume(self.source_id, self.message(number,text,outgoing)))
        state = self.state()
        self.assertEqual(state['total'], 1)
        self.assertEqual(state['pending'], 1)
        self.assertEqual(state['leads'][0]['link'], 'https://t.me/testsource/11')
        self.deliver_once()
        self.assertEqual(len(self.sent), 2)
        self.assertIn('Ищу яхту на завтра', self.sent[-1])
        self.assertEqual(self.state()['leads'][0]['delivery'], 'sent')
        lead_id = state['leads'][0]['id']
        self.post(f'leads/{lead_id}', {'status':'working'})
        self.assertEqual(self.state()['leads'][0]['status'], 'working')
        reopened = Store(Path(self.tmp.name) / 'workflow.db')
        self.assertTrue(reopened.get('running'))
        self.assertEqual(len(reopened.rows('SELECT * FROM leads')), 1)
        reopened.db.close()
        self.post('auth/disconnect', {})
        state = self.state()
        self.assertFalse(state['running'])
        self.assertFalse(state['sources'][0]['enabled'])
        self.assertEqual(state['total'], 1)
        self.assertEqual(state['connection'], 'not_connected')

    def test_login_without_2fa_and_polling_catches_missed_events(self):
        self.login(two_factor=False)
        self.configure()
        self.runtime.client.backlog = [self.message(9,'Ищу яхту старое'), self.message(11,'Ищу катер в пятницу')]
        asyncio.run(self.runtime.scan())
        asyncio.run(self.runtime.scan())
        self.assertEqual(self.state()['total'], 1)
        self.assertEqual(self.state()['sources'][0]['cursor'], 11)
        self.post('running', {'enabled':False})
        self.runtime.client.backlog.append(self.message(12,'Нужна яхту в воскресенье'))
        asyncio.run(self.runtime.scan())
        self.post('running', {'enabled':True})
        asyncio.run(self.runtime.scan())
        self.assertEqual(self.state()['total'], 1)
        self.assertEqual(self.state()['sources'][0]['cursor'], 12)

    def test_destination_change_cancels_pending_and_source_removal_disables_rule(self):
        self.login()
        self.configure()
        asyncio.run(self.runtime.consume(self.source_id, self.message(11,'Ищу яхту')))
        self.post('destination', {'reference':''})
        self.deliver_once()
        self.assertEqual(self.sent, [])
        self.assertEqual(self.state()['leads'][0]['delivery'], 'cancelled')
        self.post(f'sources/{self.source_id}/remove', {})
        self.assertFalse(self.state()['rules'][0]['enabled'])
        self.assertEqual(self.state()['rules'][0]['sources'], [])
        self.post('running', {'enabled':True}, 400)

    def test_failed_notification_can_retry_with_same_dedup_key(self):
        self.login();self.configure()
        asyncio.run(self.runtime.consume(self.source_id,self.message(31,'Ищу яхту')))
        row=self.db.rows('SELECT * FROM outbox')[0]
        self.post(f'leads/{row["lead_id"]}/retry',{},400)
        with self.db.db:self.db.db.execute("UPDATE outbox SET state='failed',attempts=8,error='temporary'")
        self.post(f'leads/{row["lead_id"]}/retry',{})
        current=self.db.rows('SELECT * FROM outbox')[0]
        self.assertEqual(current['random_id'],row['random_id']);self.assertEqual(current['attempts'],0)
        self.deliver_once()
        self.assertEqual(self.state()['leads'][0]['delivery'],'sent')

    def test_notification_retry_rejects_changed_recipient_and_rejected_lead(self):
        self.login();self.configure()
        asyncio.run(self.runtime.consume(self.source_id,self.message(31,'Ищу яхту')))
        row=self.db.rows('SELECT * FROM outbox')[0]
        with self.db.db:self.db.db.execute("UPDATE outbox SET state='failed'")
        self.post(f'leads/{row["lead_id"]}',{'status':'rejected'})
        self.post(f'leads/{row["lead_id"]}/retry',{},400)
        self.post(f'leads/{row["lead_id"]}',{'status':'new'})
        self.db.put('destination',999)
        self.post(f'leads/{row["lead_id"]}/retry',{},400)

    def test_demand_filter_roundtrip_preserves_history_and_old_client_edits(self):
        self.login(); self.configure()
        text='Ищу обмен валюты. На завтра запланировали яхту.'
        asyncio.run(self.runtime.consume(self.source_id,self.message(81,text)))
        before=self.state(); self.assertEqual(before['total'],1)
        rule=before['rules'][0]
        spec={'id':rule['id'],'name':rule['name'],'sources':rule['sources'],
              'keywords':'\n'.join(rule['keywords']),'required':'\n'.join(rule['required']),
              'excluded':'\n'.join(rule['excluded'])}
        self.post('rules',{**spec,'demand_only':True})
        preview=self.post('preview',{'rule_id':rule['id'],'text':text})
        self.assertFalse(preview['matched']); self.assertEqual(preview['reason'],'unrelated_intent')
        after=self.state()
        self.assertEqual(after['total'],before['total'])
        self.assertEqual(after['leads'][0]['status'],before['leads'][0]['status'])
        self.assertFalse(after['leads'][0]['matches_current_rules'])
        self.post('rules',spec)
        self.assertTrue(self.state()['rules'][0]['demand_only'])
        self.post('rules',{**spec,'demand_only':False})
        self.assertTrue(self.state()['leads'][0]['matches_current_rules'])

    def test_revoked_session_stops_search_and_changes_connection_status(self):
        self.login();self.configure()
        async def broken(*args,**kwargs):
            raise errors.SessionRevokedError(request=None)
            yield
        self.runtime.client.iter_messages=broken
        asyncio.run(self.runtime.scan())
        self.assertEqual(self.state()['connection'],'not_connected')
        self.assertFalse(self.state()['running'])
        self.assertIsNotNone(self.state()['error'])

    def test_flood_wait_backoff_survives_restart_without_sleeping_worker(self):
        self.login();self.configure()
        async def flooded(*args,**kwargs):
            raise errors.FloodWaitError(request=None,capture=7200)
            yield
        self.runtime.client.iter_messages=flooded
        asyncio.run(self.runtime.scan())
        import time
        self.assertGreater(self.db.get('scan_not_before'),time.time()+7190)
        reopened=Store(Path(self.tmp.name)/'workflow.db')
        self.assertGreater(reopened.get('scan_not_before'),time.time()+7190)
        reopened.db.close()

    def test_nonexistent_resource_and_promoted_source_are_rejected(self):
        self.login();self.configure()
        self.post('sources/-999',{'enabled':True},404)
        self.post('leads/999',{'status':'working'},404)
        self.group.creator=True
        self.post(f'sources/{self.source_id}',{'enabled':True},400)

    def test_all_leads_remain_accessible_with_status_topic_and_pagination(self):
        self.login();self.configure()
        for i in range(205):
            self.db.ingest(self.source_id,100+i,f'Ищу яхту заявка {i}',sender_id=100+i)
        first=self.client.get('/api/scanner/state?status=new&topic=1',headers=self.headers).json()
        self.assertEqual(first['filtered_total'],205);self.assertEqual(len(first['leads']),50)
        pages=[first];offset=first['next_offset']
        while offset is not None:
            page=self.client.get(f'/api/scanner/state?status=new&topic=1&offset={offset}',headers=self.headers).json()
            pages.append(page);offset=page['next_offset']
        self.assertEqual([len(p['leads']) for p in pages],[50,50,50,50,5])
        self.assertEqual(len({x['id'] for p in pages for x in p['leads']}),205)
        past_end=self.client.get('/api/scanner/state?offset=400',headers=self.headers).json()
        self.assertEqual(past_end['offset'],200)
        self.assertEqual(len(past_end['leads']),5)
        self.post(f'leads/{first["leads"][0]["id"]}',{'status':'rejected'})
        rejected=self.client.get('/api/scanner/state?status=rejected',headers=self.headers).json()
        self.assertEqual(rejected['filtered_total'],1)
        empty=self.client.get('/api/scanner/state?topic=999',headers=self.headers).json()
        self.assertEqual(empty['leads'],[])
        self.assertEqual(empty['offset'],0)
        self.assertEqual(self.client.get('/api/scanner/state?offset=-1',headers=self.headers).status_code,422)


    def test_flood_wait_resumes_at_blocked_source_instead_of_starving_tail(self):
        self.login(two_factor=False)
        with self.db.db:
            for source_id in (-100003,-100002,-100001):
                self.db.db.execute('INSERT INTO sources(id,title) VALUES(?,?)',(source_id,str(source_id)))
        self.runtime.scan_pause=0;seen=[];blocked=[True]
        async def messages(source_id,**kwargs):
            seen.append(source_id)
            if source_id==-100002 and blocked[0]:
                blocked[0]=False
                raise errors.FloodWaitError(request=None,capture=10)
            if False:yield None
        self.runtime.client.iter_messages=messages
        asyncio.run(self.runtime.scan())
        self.assertEqual(seen,[-100003,-100002])
        self.assertEqual(self.db.get('scan_resume_source'),-100002)
        asyncio.run(self.runtime.scan());self.assertEqual(seen,[-100003,-100002])
        self.db.put('scan_not_before',0);seen.clear();asyncio.run(self.runtime.scan())
        self.assertEqual(seen,[-100002,-100001,-100003]);self.assertIsNone(self.db.get('scan_resume_source'));self.assertIsNone(self.runtime.error)

    def test_removed_resume_source_does_not_break_scan(self):
        self.login(two_factor=False);self.runtime.scan_pause=0;self.db.put('scan_resume_source',-100009)
        with self.db.db:self.db.db.execute("INSERT INTO sources(id,title) VALUES(-100001,'Test')")
        seen=[]
        async def messages(source_id,**kwargs):
            seen.append(source_id)
            if False:yield None
        self.runtime.client.iter_messages=messages
        asyncio.run(self.runtime.scan());self.assertEqual(seen,[-100001]);self.assertIsNone(self.db.get('scan_resume_source'))

    def test_idle_sources_commit_in_batches_without_losing_success_times(self):
        self.login(two_factor=False);self.runtime.scan_pause=0
        with self.db.db:
            self.db.db.executemany('INSERT INTO sources(id,title) VALUES(?,?)',[(i,str(i)) for i in range(-70,0)])
        statements=[];self.db.db.set_trace_callback(statements.append)
        asyncio.run(self.runtime.scan())
        self.assertLessEqual(sum(s=='COMMIT' for s in statements),8)
        self.assertEqual(self.db.db.execute('SELECT COUNT(*) FROM sources WHERE last_scan IS NOT NULL').fetchone()[0],70)
        self.assertIsNone(self.db.get('scan_resume_source'))

    def test_partial_read_checkpoints_only_processed_messages_without_author_lookups(self):
        self.login(two_factor=False);self.configure();self.runtime.scan_pause=0
        sender=AsyncMock(side_effect=AssertionError('Non-matching message must not resolve sender'))
        async def messages(chat_id,**kwargs):
            for i in (11,12):
                yield SimpleNamespace(id=i,message='Обычная беседа',out=False,date=datetime.now(timezone.utc),get_sender=sender)
            raise TimeoutError('read interrupted')
        self.runtime.client.iter_messages=messages;asyncio.run(self.runtime.scan())
        row=self.db.rows('SELECT * FROM sources WHERE id=?',(self.source_id,))[0]
        self.assertEqual((row['cursor'],row['messages_checked']),(12,2));self.assertIsNotNone(row['error'])
        self.assertEqual(self.db.rows('SELECT * FROM leads'),[]);sender.assert_not_awaited()

if __name__ == '__main__': unittest.main()
