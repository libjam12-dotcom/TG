"""CLIENT primary outbound guard across queues; all transport and stores are offline."""
import asyncio,json,sys,unittest,os
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from pydantic import ValidationError
from telethon import types,errors
from operations import Operations,JobSpec,Control,ResumeOutbound
from publisher import Publisher,PostSpec
import test_conversations as _inbox_fixtures

class SharedOutboundTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.inbox=_inbox_fixtures.InboxTests();await self.inbox.asyncSetUp()
        self.store=self.inbox.store;self.runtime=self.inbox.runtime;self.client=self.inbox.client
        self.client.get_me=AsyncMock(return_value=self.runtime.me)
        self.group=types.Channel(id=10,title='Managed',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,creator=True,access_hash=111)
        original=self.client.get_entity
        async def entity(ref):return self.group if isinstance(ref,int) and ref<0 else await original(ref)
        self.client.get_entity=entity
        self.ops=Operations(self.inbox.tmp.name,lambda:{'id':1,'hash':'fixture'},lambda:self.runtime,lambda:self.store);self.ops.initialize()
        with patch.dict(os.environ,{'SCANNER_DATA':str(Path(self.inbox.tmp.name)/'app-data')}):
            import app
        self.app=app;app.bind_outbound_controls(self.runtime,self.ops)
        self.publisher=Publisher(lambda:self.store,lambda:self.runtime,self.inbox.tmp.name);self.publisher.initialize()
        self.job=await self.ops.create(JobSpec(request_key='guarded-primary-job-001',account='primary',kind='post',title='Pending operation',destinations=[-1000000000010],text='Operation'))
        await self.ops.control(self.job['id'],Control(action='start',confirmed=True))
    async def asyncTearDown(self):
        await self.publisher.shutdown();await self.ops.shutdown();await self.inbox.asyncTearDown()
    async def post(self):
        p=await self.publisher.create(PostSpec(title='Post',text='Content',destinations=[-1000000000010]))
        await self.publisher.publish(p['id']);return p
    def scanner(self):
        runtime=self.app.Runtime();runtime.client=self.client;runtime.me=self.runtime.me;runtime.scan_pause=0
        self.app.bind_outbound_controls(runtime,self.ops);return runtime
    def notifications(self):
        self.store.put('destination',-999)
        for n in [100,101]:self.store.ingest(-1001,n,'Нужна яхта, запрос '+str(n),'Client',n,'https://t.me/external/'+str(n))
        return self.store.rows("SELECT * FROM outbox WHERE state='pending' ORDER BY lead_id")
    async def outbox_once(self,runtime):
        with patch.object(self.app,'store',self.store),patch.object(self.app.asyncio,'sleep',AsyncMock(side_effect=asyncio.CancelledError)):
            try:await runtime.outbox_loop()
            except asyncio.CancelledError:pass
    def resume(self,**kw):
        values=dict(confirmed=True,telegram_id=42,reason='Owner reviewed the Telegram restriction and pending queues')
        values.update(kw);return ResumeOutbound(**values)
    def assert_paused(self):
        account=self.ops.account_row('primary');self.assertEqual(account['enabled'],0);self.assertIn('Telegram',account['error'])
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_jobs WHERE id=?',(self.job['id'],))[0]['state'],'paused')
        with self.assertRaises(HTTPException) as raised:self.ops.online('primary')
        self.assertIn('приостановлены',raised.exception.detail)
    async def assert_reads_continue(self):
        runtime=self.scanner();self.client.messages=[SimpleNamespace(id=500,out=False,message='Обычное сообщение',date=datetime.now(timezone.utc))]
        with self.store.db:
            for n in range(69):self.store.db.execute('INSERT INTO sources(id,title) VALUES(?,?)',(-2000-n,'Source '+str(n)))
        with patch.object(self.app,'store',self.store):await runtime.scan()
        sources=self.store.rows('SELECT cursor FROM sources WHERE enabled=1')
        self.assertEqual(len(sources),70);self.assertTrue(all(source['cursor']==500 for source in sources))
        self.assertTrue(self.store.get('running'));self.assertIsNotNone(runtime.me)
    async def test_conversation_peer_flood_stops_manual_content_operations_and_notifications_but_not_reads(self):
        first=await self.inbox.send();second=await self.inbox.send('second_manual_guard_key');p=await self.post();self.notifications();runtime=self.scanner()
        calls=[]
        async def refused(client,request):calls.append(request);raise errors.PeerFloodError(None)
        with patch.object(type(self.client),'__call__',refused):
            await self.inbox.crm.deliver_one();await self.inbox.crm.deliver_one();await self.publisher.tick();await self.ops.tick();await self.outbox_once(runtime)
        self.assertEqual(len(calls),1);self.assert_paused()
        self.assertEqual(self.inbox.row(first['id'])['state'],'failed');self.assertEqual(self.inbox.row(second['id'])['state'],'pending')
        self.assertEqual(self.publisher.post(p['id'])['delivery'][0]['state'],'pending')
        self.assertTrue(all(row['state']=='pending' for row in self.store.rows('SELECT state FROM outbox')))
        await self.assert_reads_continue()
    async def test_content_peer_flood_stops_all_other_outgoing_queues(self):
        first=await self.post();second=await self.post();manual=await self.inbox.send();self.notifications();runtime=self.scanner()
        self.publisher.send=AsyncMock(side_effect=errors.PeerFloodError(None))
        await self.publisher.tick();await self.publisher.tick();await self.inbox.crm.deliver_one();await self.ops.tick();await self.outbox_once(runtime)
        self.publisher.send.assert_awaited_once();self.assert_paused();self.assertFalse(self.client.sent)
        self.assertEqual(self.publisher.post(first['id'])['delivery'][0]['state'],'failed')
        self.assertEqual(self.publisher.post(second['id'])['delivery'][0]['state'],'pending');self.assertEqual(self.inbox.row(manual['id'])['state'],'pending')
        await self.assert_reads_continue()
    async def test_account_notification_peer_flood_stops_other_queues_without_retrying_failed_delivery(self):
        rows=self.notifications();runtime=self.scanner();manual=await self.inbox.send();p=await self.post();calls=[]
        async def peer(_):return types.InputPeerUser(99,999)
        self.client.get_input_entity=peer
        async def refused(client,request):calls.append(request);raise errors.PeerFloodError(None)
        with patch.object(type(self.client),'__call__',refused):
            await self.outbox_once(runtime);await self.outbox_once(runtime);await self.inbox.crm.deliver_one();await self.publisher.tick()
        self.assertEqual(len(calls),1);self.assert_paused()
        actual=self.store.rows('SELECT state FROM outbox ORDER BY lead_id');self.assertEqual([r['state'] for r in actual],['failed','pending'])
        self.assertEqual(self.inbox.row(manual['id'])['state'],'pending');self.assertEqual(self.publisher.post(p['id'])['delivery'][0]['state'],'pending')
        await self.assert_reads_continue()
    async def test_guard_rechecked_after_conversation_entity_await_keeps_pending(self):
        message=await self.inbox.send();original=self.client.get_entity
        async def stop(ref):
            entity=await original(ref);self.ops.pause_outbound('primary',errors.PeerFloodError(None));return entity
        self.client.get_entity=stop;await self.inbox.crm.deliver_one()
        self.assertEqual(self.inbox.row(message['id'])['state'],'pending');self.assertFalse(self.client.sent)
    async def test_guard_rechecked_after_publisher_input_peer_await_keeps_pending(self):
        p=await self.post()
        async def stop(_):
            self.ops.pause_outbound('primary',errors.PeerFloodError(None));return types.InputPeerChannel(10,111)
        self.client.get_input_entity=stop;await self.publisher.tick()
        self.assertEqual(self.publisher.post(p['id'])['delivery'][0]['state'],'pending');self.assertFalse(self.client.sent)
    async def test_guard_rechecked_after_notification_input_peer_await_keeps_pending(self):
        self.notifications();runtime=self.scanner()
        async def stop(_):
            self.ops.pause_outbound('primary',errors.PeerFloodError(None));return types.InputPeerUser(99,999)
        self.client.get_input_entity=stop;await self.outbox_once(runtime)
        self.assertTrue(all(r['state']=='pending' and r['attempts']==0 for r in self.store.rows('SELECT state,attempts FROM outbox')))
        self.assertFalse(self.client.sent)
    async def test_explicit_primary_resume_only_changes_account_not_jobs_or_failed_unknown_rows(self):
        first=await self.inbox.send();second=await self.inbox.send('pending_manual_guard_key')
        with self.store.db:self.store.db.execute("UPDATE conversation_messages SET state='uncertain' WHERE id=?",(first['id'],))
        self.ops.pause_outbound('primary',errors.PeerFloodError(None));self.assert_paused()
        before_jobs=self.ops.store.rows('SELECT * FROM op_jobs');before_items=self.ops.store.rows('SELECT * FROM op_items')
        before_messages=self.store.rows('SELECT * FROM conversation_messages')
        result=await self.ops.resume_primary_outbound(self.resume())
        self.assertTrue(result['pending_outgoing_may_continue']);self.assertFalse(result['jobs_resumed'])
        self.assertEqual(self.ops.account_row('primary')['enabled'],1);self.assertIsNone(self.ops.account_row('primary')['error'])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs'),before_jobs);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before_items)
        self.assertEqual(self.store.rows('SELECT * FROM conversation_messages'),before_messages)
        await self.inbox.crm.deliver_one();self.assertEqual(self.inbox.row(first['id'])['state'],'uncertain')
        self.assertEqual(self.inbox.row(second['id'])['state'],'sent');self.assertEqual(len(self.client.sent),1)
    async def test_no_autoenable_on_state_restart_or_late_bind(self):
        self.ops.pause_outbound('primary',errors.PeerFloodError(None));await self.ops.state();self.ops.initialize()
        self.app.bind_outbound_controls(self.runtime,self.ops);await self.ops.state();self.assert_paused()
    async def test_primary_resume_rejects_missing_confirmation_wrong_identity_cooldown_and_new_error(self):
        for values in [dict(confirmed=False),dict(reason='short')]:
            with self.assertRaises(ValidationError):self.resume(**values)
        self.ops.pause_outbound('primary',errors.PeerFloodError(None))
        with self.assertRaises(HTTPException):await self.ops.resume_primary_outbound(self.resume(telegram_id=99))
        with self.ops.store.db:self.ops.store.db.execute("UPDATE op_accounts SET not_before=? WHERE id='primary'",(datetime.now(timezone.utc).timestamp()+60,))
        with self.assertRaises(HTTPException) as cooldown:await self.ops.resume_primary_outbound(self.resume())
        self.assertEqual(cooldown.exception.status_code,429)
        with self.ops.store.db:self.ops.store.db.execute("UPDATE op_accounts SET not_before=0 WHERE id='primary'")
        self.client.get_me=AsyncMock(return_value=types.User(id=99))
        with self.assertRaises(HTTPException):await self.ops.resume_primary_outbound(self.resume())
        async def changed():
            with self.ops.store.db:self.ops.store.db.execute("UPDATE op_accounts SET error='New Telegram restriction' WHERE id='primary'")
            return self.runtime.me
        self.client.get_me=changed
        with self.assertRaises(HTTPException):await self.ops.resume_primary_outbound(self.resume())
        self.assertEqual(self.ops.account_row('primary')['enabled'],0)
    async def test_same_peer_flood_arriving_during_resume_rejects_the_stale_confirmation(self):
        self.ops.pause_outbound('primary',errors.PeerFloodError(None))
        async def new_restriction():
            self.ops.pause_outbound('primary',errors.PeerFloodError(None));return self.runtime.me
        self.client.get_me=new_restriction
        with self.assertRaises(HTTPException):await self.ops.resume_primary_outbound(self.resume())
        self.assert_paused()
    async def test_resume_get_me_flood_wait_is_persisted_and_does_not_enable_account(self):
        self.ops.pause_outbound('primary',errors.PeerFloodError(None));self.client.get_me=AsyncMock(side_effect=errors.FloodWaitError(None,capture=60))
        with self.assertRaises(HTTPException) as raised:await self.ops.resume_primary_outbound(self.resume())
        self.assertEqual(raised.exception.status_code,429);self.assertEqual(self.ops.account_row('primary')['enabled'],0)
        self.assertGreater(self.ops.account_row('primary')['not_before'],datetime.now(timezone.utc).timestamp()+55)
    async def test_manual_notification_test_obeys_shared_pause_and_exposes_the_reason(self):
        runtime=self.scanner();self.store.put('destination',-999);self.client.send_message=AsyncMock()
        self.ops.pause_outbound('primary',errors.PeerFloodError(None))
        with patch.object(self.app,'store',self.store),patch.object(self.app,'runtime',runtime):
            with self.assertRaises(HTTPException):await self.app.notification_test()
        self.client.send_message.assert_not_awaited();status=runtime.outbound_status()
        self.assertFalse(status['allowed']);self.assertIn('приостановлены',status['reason'])
    async def test_manual_bot_connect_rechecks_shared_pause_before_each_mutating_rpc(self):
        bot=types.User(id=77,first_name='Bot',bot=True,access_hash=777);calls=[]
        async def entity(ref):return bot if isinstance(ref,str) else self.group
        self.client.get_entity=entity
        async def check_then_pause(client,request):
            from telethon import functions
            calls.append(request)
            if isinstance(request,functions.channels.GetParticipantRequest):
                self.ops.pause_outbound('primary',errors.PeerFloodError(None));raise errors.UserNotParticipantError(None)
        with patch.object(type(self.client),'__call__',check_then_pause):
            with self.assertRaises(HTTPException):await self.publisher.connect_bot(-1000000000010)
        self.assertEqual(len(calls),1)
        from telethon import functions
        self.assertIsInstance(calls[0],functions.channels.GetParticipantRequest)
