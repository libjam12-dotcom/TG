import asyncio,json,sys,tempfile,time,unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store
from marketing_topics import seed_marketing
from owner_sender import OwnerSender,Phone,Code,Password
from owner_ai import OwnerConversations,BuyerDecision,WatchContact,structured,ENDPOINT,MANUAL_OFFER,request_owner_decision
from qualification import ProviderError,Decision,Fact
from conversations import Settings,Topic
from telethon import types,errors
from fastapi import HTTPException
import httpx

class OwnerAssistantTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'owner.db');seed_marketing(self.store)
  self.sender=OwnerSender(Path(self.tmp.name)/'owner-telegram',lambda:{'id':123,'hash':'synthetic-app-hash'},self.store)
  self.inbox=OwnerConversations(lambda:self.store,lambda:self.sender);self.inbox.initialize()
  self.client=SimpleNamespace(is_connected=lambda:True,connect=AsyncMock(),is_user_authorized=AsyncMock(return_value=False),sign_in=AsyncMock(),get_me=AsyncMock(return_value=types.User(id=42,username='demo_identity_3')),send_code_request=AsyncMock(return_value=SimpleNamespace(phone_code_hash='synthetic-code-hash')),log_out=AsyncMock(),get_entity=AsyncMock(),get_messages=AsyncMock(),get_input_entity=AsyncMock(),disconnect=AsyncMock())
  self.sender.client=self.client
 async def asyncTearDown(self):
  await self.sender.cancel_qr()
  self.store.db.close();self.tmp.cleanup()
 async def test_partial_connected_authorized_session_recovers_without_new_client_or_code(self):
  self.client.is_user_authorized.return_value=True
  self.sender.error='Telegram: TimeoutError.'
  with patch('owner_sender.TelegramClient') as factory:
   await self.sender.connect()
  self.assertEqual(self.sender.me.id,42);self.assertEqual(self.sender.status,'connected');self.assertIsNone(self.sender.error)
  self.client.is_user_authorized.assert_awaited_once();self.client.get_me.assert_awaited_once()
  factory.assert_not_called();self.client.log_out.assert_not_called();self.client.send_code_request.assert_not_called()
  self.assertTrue(self.inbox.config()['paused']);self.assertFalse(self.inbox.config()['auto_first']);self.assertFalse(self.inbox.config()['auto_reply'])
 async def test_partial_authorization_and_identity_timeouts_are_bounded_then_recover_same_client(self):
  for phase in ['authorization','identity']:
   with self.subTest(phase=phase):
    self.sender.me=None;self.sender.status='not_connected'
    self.client.is_user_authorized=AsyncMock(return_value=True)
    self.client.get_me=AsyncMock(return_value=types.User(id=42,username='demo_identity_3'))
    calls=0
    async def slow():
     nonlocal calls
     calls+=1
     if calls==1:await asyncio.Event().wait()
     return True if phase=='authorization' else types.User(id=42,username='demo_identity_3')
    target=self.client.is_user_authorized if phase=='authorization' else self.client.get_me
    target.side_effect=slow
    with patch('owner_sender.CONNECT_TIMEOUT',.02,create=True):
     with self.assertRaises(asyncio.TimeoutError):await self.sender.connect()
     self.assertIsNone(self.sender.me)
     await self.sender.connect()
    self.assertIs(self.sender.client,self.client);self.assertEqual(self.sender.me.id,42)
    self.assertEqual(calls,2);self.client.log_out.assert_not_called();self.client.send_code_request.assert_not_called()
 async def test_concurrent_connect_creates_one_client_and_handlers_and_authorizes_once(self):
  self.sender.client=None
  connected=False
  async def connect():
   nonlocal connected
   await asyncio.sleep(0);connected=True
  async def authorized():
   await asyncio.sleep(0);return True
  from unittest.mock import Mock
  client=SimpleNamespace(is_connected=lambda:connected,connect=AsyncMock(side_effect=connect),is_user_authorized=AsyncMock(side_effect=authorized),get_me=AsyncMock(return_value=types.User(id=42,username='demo_identity_3')),add_event_handler=Mock(),log_out=AsyncMock(),disconnect=AsyncMock())
  with patch('owner_sender.TelegramClient',return_value=client) as factory:
   await asyncio.gather(self.sender.connect(),self.sender.connect())
  factory.assert_called_once();self.assertEqual(client.add_event_handler.call_count,2)
  client.connect.assert_awaited_once();client.is_user_authorized.assert_awaited_once();client.get_me.assert_awaited_once();client.log_out.assert_not_called()
 async def test_timeout_closes_partial_transport_and_reuses_session_and_challenge(self):
  from unittest.mock import Mock
  for phase in ['connect','authorization']:
   with self.subTest(phase=phase):
    connected=False;attempts=0
    async def connect():
     nonlocal connected,attempts
     connected=True;attempts+=1
     if phase=='connect' and attempts==1:await asyncio.Event().wait()
    auth_attempts=0
    async def authorized():
     nonlocal auth_attempts
     auth_attempts+=1
     if phase=='authorization' and auth_attempts==1:await asyncio.Event().wait()
     return True
    async def disconnect():
     nonlocal connected
     connected=False
    client=SimpleNamespace(is_connected=lambda:connected,connect=AsyncMock(side_effect=connect),is_user_authorized=AsyncMock(side_effect=authorized),get_me=AsyncMock(return_value=types.User(id=42,username='demo_identity_3')),disconnect=AsyncMock(side_effect=disconnect),log_out=AsyncMock(),add_event_handler=Mock(),session=SimpleNamespace(delete=Mock()))
    self.sender.client=None;self.sender.me=None;self.sender.status='code_required';self.sender.auth_until=time.time()+600
    self.sender.phone='+70000000000';self.sender.code_hash='keep-code'
    with patch('owner_sender.TelegramClient',return_value=client) as factory,patch('owner_sender.CONNECT_TIMEOUT',.02):
     with self.assertRaises(asyncio.TimeoutError):await self.sender.connect()
     self.assertFalse(client.is_connected());self.assertIs(self.sender.client,client);self.assertIsNone(self.sender.me)
     self.assertEqual(self.sender.status,'code_required');self.assertEqual(self.sender.code_hash,'keep-code');self.assertEqual(self.sender.phone,'+70000000000')
     client.disconnect.assert_awaited_once();client.log_out.assert_not_called();client.session.delete.assert_not_called()
     await self.sender.connect()
    self.assertEqual(self.sender.me.id,42);self.assertEqual(self.sender.status,'connected')
    self.assertEqual(client.connect.await_count,2);factory.assert_called_once();self.assertEqual(client.add_event_handler.call_count,2)
    client.log_out.assert_not_called();client.session.delete.assert_not_called()
 async def test_background_connect_preserves_active_code_password_and_qr_challenges(self):
  self.store.put('owner_sender_started',True)
  for status in ['code_required','password_required','qr_required']:
   with self.subTest(status=status):
    self.sender.status=status;self.sender.auth_until=time.time()+600;self.sender.qr_expires=time.time()+600
    self.sender.phone='+70000000000';self.sender.code_hash='keep-code';self.sender.qr=object()
    qr=self.sender.qr;paused=asyncio.Event()
    async def sleep(delay):
     paused.set();await asyncio.Event().wait()
    with patch.object(self.sender,'connect',new_callable=AsyncMock) as connect,patch('owner_sender.asyncio.sleep',side_effect=sleep):
     task=asyncio.create_task(self.sender.loop())
     try:
      await asyncio.wait_for(paused.wait(),.3)
      connect.assert_not_awaited();self.assertEqual(self.sender.status,status);self.assertEqual(self.sender.code_hash,'keep-code');self.assertIs(self.sender.qr,qr)
     finally:
      task.cancel()
      try:await task
      except asyncio.CancelledError:pass
 async def test_background_connect_does_not_interleave_with_locked_auth_flow(self):
  self.store.put('owner_sender_started',True);paused=asyncio.Event()
  async def sleep(delay):paused.set();await asyncio.Event().wait()
  async with self.sender.lock:
   with patch.object(self.sender,'connect',new_callable=AsyncMock) as connect,patch('owner_sender.asyncio.sleep',side_effect=sleep):
    task=asyncio.create_task(self.sender.loop())
    try:
     await asyncio.wait_for(paused.wait(),.3);connect.assert_not_awaited()
    finally:
     task.cancel()
     try:await task
     except asyncio.CancelledError:pass
 async def test_shutdown_cancels_pending_authorization_without_logout(self):
  self.store.put('owner_sender_started',True);started=asyncio.Event()
  async def hang():started.set();await asyncio.Event().wait()
  self.client.is_user_authorized.side_effect=hang
  self.sender.task=asyncio.create_task(self.sender.loop())
  try:
   await asyncio.wait_for(started.wait(),.3)
   await asyncio.wait_for(self.sender.shutdown(),.3)
   self.assertTrue(self.sender.task.done());self.assertFalse(self.sender.lock.locked())
   self.client.disconnect.assert_awaited_once();self.client.log_out.assert_not_called();self.client.send_code_request.assert_not_called()
  finally:
   self.sender.task.cancel()
   try:await self.sender.task
   except asyncio.CancelledError:pass
 async def test_background_loop_retries_partial_timeout_and_restores_identity(self):
  self.store.put('owner_sender_started',True)
  calls=0;first_pause=asyncio.Event();retry=asyncio.Event();recovered=asyncio.Event()
  async def authorized():
   nonlocal calls
   calls+=1
   if calls==1:await asyncio.Event().wait()
   return True
  pauses=0
  async def sleep(delay):
   nonlocal pauses
   pauses+=1
   if pauses==1:
    first_pause.set();await retry.wait()
   else:
    recovered.set();await asyncio.Event().wait()
  self.client.is_user_authorized.side_effect=authorized
  with patch('owner_sender.CONNECT_TIMEOUT',.02),patch('owner_sender.asyncio.sleep',side_effect=sleep):
   self.sender.task=asyncio.create_task(self.sender.loop())
   try:
    await asyncio.wait_for(first_pause.wait(),.3)
    self.assertIsNone(self.sender.me);self.assertIn('TimeoutError',self.sender.error)
    retry.set();await asyncio.wait_for(recovered.wait(),.3)
    self.assertEqual(self.sender.me.id,42);self.assertIsNone(self.sender.error);self.assertEqual(calls,2)
    self.client.send_code_request.assert_not_called();self.client.log_out.assert_not_called()
   finally:await self.sender.shutdown()
 async def test_failed_code_request_does_not_claim_to_wait_for_code(self):
  self.sender.status='code_required';self.sender.phone='+70000000000';self.sender.auth_until=time.time()+600
  self.client.send_code_request.side_effect=errors.SendCodeUnavailableError(None)
  with self.assertRaises(HTTPException) as e:await self.sender.code(Phone(phone='+70000000000'))
  self.assertIn('QR',e.exception.detail)
  state=await self.sender.state();self.assertEqual(state['connection'],'not_connected')
  self.assertIsNone(self.sender.phone);self.assertIsNone(self.sender.code_hash)
 async def test_delivery_method_is_sanitized_and_expired_stage_is_cleared(self):
  self.client.send_code_request.return_value=types.auth.SentCode(type=types.auth.SentCodeTypeApp(length=5),phone_code_hash='synthetic-secret-hash',timeout=60)
  await self.sender.code(Phone(phone='+70000000000'))
  state=await self.sender.state();self.assertEqual(state['delivery_method'],'SentCodeTypeApp')
  self.assertNotIn('synthetic-secret-hash',json.dumps(state));self.assertNotIn('+70000000000',json.dumps(state))
  self.sender.auth_until=0
  self.assertEqual((await self.sender.state())['connection'],'not_connected')
 async def test_qr_login_renders_locally_and_does_not_enable_outreach(self):
  qr=SimpleNamespace(url='tg://login?token=synthetic-login-token',expires=datetime.now(timezone.utc)+timedelta(seconds=30),wait=AsyncMock())
  self.client.qr_login=AsyncMock(return_value=qr)
  self.assertEqual(await self.sender.start_qr(),{'step':'qr'})
  image=await self.sender.get_qr_image();self.assertEqual(image.media_type,'image/svg+xml')
  self.assertIn(b'<svg',image.body);self.assertEqual(image.headers['cache-control'],'no-store')
  self.assertNotIn('synthetic-login-token',json.dumps(await self.sender.state()))
  await self.sender.qr_task
  self.assertEqual((await self.sender.state())['connection'],'connected')
  self.client.send_code_request.assert_not_called()
  self.assertTrue(self.inbox.config()['paused']);self.assertFalse(self.inbox.config()['auto_first'])
  self.assertNotIn('synthetic-login-token','\n'.join(self.store.db.iterdump()))
 async def test_qr_login_supports_2fa(self):
  qr=SimpleNamespace(url='tg://login?token=synthetic-login-token',expires=datetime.now(timezone.utc)+timedelta(seconds=30),wait=AsyncMock(side_effect=errors.SessionPasswordNeededError(None)))
  self.client.qr_login=AsyncMock(return_value=qr)
  await self.sender.start_qr();await self.sender.qr_task
  self.assertEqual((await self.sender.state())['connection'],'password_required')
  with self.assertRaises(HTTPException):await self.sender.get_qr_image()
  self.assertEqual(await self.sender.password(Password(password='synthetic-password')),{'step':'connected'})
 async def test_qr_rejects_other_profile(self):
  qr=SimpleNamespace(url='tg://login?token=synthetic-login-token',expires=datetime.now(timezone.utc)+timedelta(seconds=30),wait=AsyncMock())
  self.client.qr_login=AsyncMock(return_value=qr)
  self.client.get_me.return_value=types.User(id=99,username='other_profile')
  await self.sender.start_qr();await self.sender.qr_task
  self.client.log_out.assert_awaited_once();self.assertIsNone(self.sender.me)
  self.assertEqual((await self.sender.state())['connection'],'not_connected')
 async def test_expired_qr_cannot_be_retrieved_or_reused(self):
  qr=SimpleNamespace(url='tg://login?token=synthetic-login-token',expires=datetime.now(timezone.utc)-timedelta(seconds=1),wait=AsyncMock(side_effect=asyncio.TimeoutError()))
  self.client.qr_login=AsyncMock(return_value=qr)
  await self.sender.start_qr()
  with self.assertRaises(HTTPException) as e:await self.sender.get_qr_image()
  self.assertEqual(e.exception.status_code,410)
  await self.sender.qr_task
  self.assertEqual((await self.sender.state())['connection'],'not_connected')
 async def test_full_auth_with_2fa_does_not_persist_secrets(self):
  await self.sender.code(Phone(phone='+70000000000'))
  self.client.sign_in.side_effect=errors.SessionPasswordNeededError(None)
  self.assertEqual(await self.sender.confirm(Code(code='12345')),{'step':'password'})
  self.client.sign_in.side_effect=None
  self.assertEqual(await self.sender.password(Password(password='synthetic-password')),{'step':'connected'})
  state=await self.sender.state();self.assertEqual(state['username'],'demo_identity_3')
  dump='\n'.join(self.store.db.iterdump())
  for secret in ('synthetic-password','12345','synthetic-code-hash','synthetic-app-hash','+70000000000'):self.assertNotIn(secret,dump)
  self.assertIsNone(self.store.get('api'))
 async def test_wrong_profile_is_logged_out_and_pauses(self):
  self.client.get_me.return_value=types.User(id=43,username='other_profile')
  with self.assertRaises(HTTPException):await self.sender.ready()
  self.client.log_out.assert_awaited_once();self.assertIsNone(self.sender.me);self.assertTrue(self.inbox.config()['paused'])
 async def test_expected_profile_accepts_username_case_variations(self):
  for username in ('demo_identity_3','demo_identity_3','demo_identity_3'):
   self.client.get_me.return_value=types.User(id=42,username=username)
   await self.sender.ready()
   state=await self.sender.state()
   self.assertEqual(state['connection'],'connected');self.assertEqual(state['expected_username'],'demo_identity_3')
   self.assertEqual(state['account_id'],42)
  self.client.log_out.assert_not_called()
 async def test_misspelled_username_with_underscore_is_rejected_after_code(self):
  self.client.get_me.return_value=types.User(id=99,username='murk_tenet')
  await self.sender.code(Phone(phone='+70000000000'))
  with self.assertRaises(HTTPException) as exc:await self.sender.confirm(Code(code='12345'))
  self.assertEqual(exc.exception.status_code,409);self.assertIn('@demo_identity_3',exc.exception.detail)
  self.client.log_out.assert_awaited_once();self.assertIsNone(self.sender.me)
  self.assertTrue(self.inbox.config()['paused'])
 async def test_bot_with_expected_username_is_not_an_owner_account(self):
  self.client.get_me.return_value=types.User(id=42,username='demo_identity_3',bot=True)
  with self.assertRaises(HTTPException):await self.sender.ready()
  self.client.log_out.assert_awaited_once();self.assertIsNone(self.sender.me)
 async def test_wrong_password_retains_password_step_and_can_retry(self):
  self.sender.status='password_required';self.sender.auth_until=time.time()+600
  self.client.sign_in.side_effect=errors.PasswordHashInvalidError(None)
  with self.assertRaises(HTTPException) as e:await self.sender.password(Password(password='wrong'))
  self.assertIn('облачный пароль',e.exception.detail);self.assertEqual(self.sender.status,'password_required')
  self.client.sign_in.side_effect=None;await self.sender.password(Password(password='correct'))
  self.assertEqual(self.sender.me.id,42)
 async def test_code_cooldown_and_expiry(self):
  await self.sender.code(Phone(phone='+70000000000'))
  with self.assertRaises(HTTPException) as e:await self.sender.code(Phone(phone='+70000000000'))
  self.assertEqual(e.exception.status_code,429)
  self.sender.auth_until=0
  with self.assertRaises(HTTPException):await self.sender.confirm(Code(code='12345'))
  self.client.sign_in.assert_not_called()
 async def test_disconnect_clears_owner_identity(self):
  await self.sender.ready();await self.sender.disconnect()
  self.assertIsNone(self.sender.me);self.assertIsNone(self.sender.client);self.assertFalse(self.store.get('owner_sender_started'))
 async def test_nine_it_scenarios_paused_idempotent(self):
  settings=await self.inbox.settings();self.assertEqual(len(settings['topics']),9)
  self.assertTrue(settings['config']['paused']);self.assertFalse(settings['config']['provider_verified']);self.assertFalse(settings['key_configured'])
  for topic in settings['topics']:
   self.assertIn('ИИ-помощник',topic['first_text']);self.assertNotIn('гостей',topic['questions'])
  self.store.put('conversation_topic_1',{'company':'Edited'});self.inbox.initialize()
  self.assertEqual(self.inbox.topic(1)['company'],'Edited')
 async def test_buyer_gate_rejects_vendor_and_unproven_claim(self):
  lead={'id':1,'text':'Нужен сайт для магазина'};rule={'name':'Сайты'}
  with patch('owner_ai.structured',new=AsyncMock(return_value=(BuyerDecision(buyer_request=False,evidence='',reason='vendor'),{}))) as provider:
   self.assertFalse(await self.inbox.first_contact_allowed(lead,rule));self.assertFalse(await self.inbox.first_contact_allowed(lead,rule));self.assertEqual(provider.await_count,1)
  lead['text']='Другой текст'
  with patch('owner_ai.structured',new=AsyncMock(return_value=(BuyerDecision(buyer_request=True,evidence='Нужен сайт для магазина',reason='unproven'),{}))):
   self.assertFalse(await self.inbox.first_contact_allowed(lead,rule))
 async def test_buyer_gate_accepts_grounded_demand(self):
  lead={'id':2,'text':'Нужен сайт для магазина'}
  with patch('owner_ai.structured',new=AsyncMock(return_value=(BuyerDecision(buyer_request=True,evidence=lead['text'],reason='direct'),{'input_tokens':20}))):
   self.assertTrue(await self.inbox.first_contact_allowed(lead,{'name':'Сайты'}))
  self.assertEqual(self.store.db.execute('SELECT calls,input_tokens FROM conversation_usage').fetchone()[0],1)
 async def test_public_source_resolved_in_own_session(self):
  with self.store.db:self.store.db.execute("INSERT INTO sources(id,title,username) VALUES(-1001,'Public','public_source')")
  self.sender.me=types.User(id=42,username='demo_identity_3')
  author=types.User(id=77,first_name='Buyer',access_hash=777)
  source=SimpleNamespace(id=-1001)
  self.client.get_entity.side_effect=[ValueError('not cached'),source]
  self.client.get_messages.return_value=SimpleNamespace(get_sender=AsyncMock(return_value=author))
  self.client.get_input_entity.return_value=types.InputPeerUser(77,777)
  result=await self.inbox.resolve({'id':77,'sender_id':77,'chat_id':-1001,'message_id':5,'author':'@personal_author','text':'Нужен сайт','origin':'history'})
  self.assertEqual(result[0].id,77);self.client.get_messages.assert_awaited_once_with(source,ids=5)
 async def test_roundup_has_no_personal_author(self):
  with self.assertRaises(HTTPException):await self.inbox.resolve({'sender_id':-1001})
  self.client.get_entity.assert_not_called()
 async def test_no_sender_no_auto_contact_or_provider_calls(self):
  self.sender.client=None
  with patch('owner_ai.structured',new=AsyncMock()) as provider:
   await self.inbox.initiate_one();provider.assert_not_called()
 async def test_gateway_request_and_quota_errors_sanitized(self):
  response=httpx.Response(200,json={'stop_reason':'tool_use','content':[{'type':'tool_use','name':'decision','input':{'buyer_request':True,'evidence':'Нужен сайт','reason':'direct'}}],'usage':{}})
  mock=AsyncMock();mock.__aenter__.return_value=mock;mock.post.return_value=response
  with patch('owner_ai.httpx.AsyncClient',return_value=mock) as constructor:
   decision,_=await structured('synthetic-key','claude-sonnet-4-6','system',{'text':'Нужен сайт'},BuyerDecision)
   self.assertTrue(decision.buyer_request);self.assertEqual(mock.post.call_args.args[0],ENDPOINT);self.assertFalse(constructor.call_args.kwargs['follow_redirects'])
   mock.post.return_value=httpx.Response(402,text='secret synthetic-key')
   with self.assertRaises(ProviderError) as e:await structured('synthetic-key','model','system',{},BuyerDecision)
   self.assertIn('квота',str(e.exception));self.assertNotIn('synthetic-key',str(e.exception))
 async def test_gateway_invalid_structure_never_accepted(self):
  mock=AsyncMock();mock.__aenter__.return_value=mock;mock.post.return_value=httpx.Response(200,json={'stop_reason':'max_tokens','content':[]})
  with patch('owner_ai.httpx.AsyncClient',return_value=mock),self.assertRaises(ProviderError):await structured('synthetic','model','system',{},BuyerDecision)

 async def watched_dialogue(self):
  await self.sender.ready()
  self.client.get_entity.return_value=types.User(id=77,username='BuyerOne',first_name='Buyer')
  self.client.get_input_entity.return_value=types.InputPeerUser(77,777)
  self.client.get_messages.return_value=[types.Message(id=11,peer_id=types.PeerUser(77),from_id=types.PeerUser(77),date=datetime.now(timezone.utc),message='Нужен сайт',out=False),types.Message(id=10,peer_id=types.PeerUser(77),from_id=types.PeerUser(42),date=datetime.now(timezone.utc),message='Buyer, здравствуйте! Я Разработчикислав, SignM.',out=True)]
  spec=WatchContact(account_id=42,peer_id=77,username='BuyerOne',rule_id=1,outgoing_prefix='Buyer, здравствуйте!',confirmed=True)
  return spec,await self.inbox.watch_contact(spec)
 async def test_watch_contact_imports_history_without_sending_or_replaying_old_incoming(self):
  spec,result=await self.watched_dialogue();c=result['conversation']
  self.assertEqual(c['mode'],'manual');self.assertEqual(len(result['messages']),2)
  self.assertEqual(c['ai_cursor'],result['messages'][-1]['id']);self.assertIsNotNone(c['last_sync'])
  self.assertEqual(self.store.db.execute('SELECT origin FROM leads WHERE id=?',(c['lead_id'],)).fetchone()[0],'manual_dialogue')
  self.assertEqual(self.store.db.execute("SELECT count(*) FROM conversation_messages WHERE state='pending'").fetchone()[0],0)
  self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0],0)
  second=await self.inbox.watch_contact(spec);self.assertEqual(second['conversation']['id'],c['id'])
  self.assertEqual(self.store.db.execute('SELECT count(*) FROM conversations').fetchone()[0],1)
 async def test_watch_contact_rejects_wrong_recipient_before_history(self):
  await self.sender.ready();self.client.get_entity.return_value=types.User(id=78,username='BuyerOne')
  with self.assertRaises(HTTPException):await self.inbox.watch_contact(WatchContact(account_id=42,peer_id=77,username='BuyerOne',rule_id=1,outgoing_prefix='Buyer, здравствуйте!',confirmed=True))
  self.client.get_messages.assert_not_called()
 async def test_watch_contact_requires_actual_outgoing_offer(self):
  spec,result=await self.watched_dialogue()
  self.client.get_entity.return_value=types.User(id=78,username='BuyerTwo');self.client.get_input_entity.return_value=types.InputPeerUser(78,778)
  self.client.get_messages.return_value=[]
  with self.assertRaises(HTTPException):await self.inbox.watch_contact(spec.model_copy(update={'peer_id':78,'username':'BuyerTwo'}))
  self.assertEqual(self.store.db.execute('SELECT count(*) FROM conversations').fetchone()[0],1)
 async def test_owner_ai_cannot_enqueue_a_first_contact(self):
  _,result=await self.watched_dialogue()
  with self.assertRaises(HTTPException):self.inbox.enqueue(result['conversation'],'Здравствуйте!','new_first_contact','ai')
 async def test_personal_reply_policy_survives_stale_first_contact_configuration(self):
  await self.sender.ready()
  self.store.put('conversation_config',{**self.inbox.config(),'paused':False,'auto_first':True})
  with patch.object(self.inbox,'first_contact_allowed',new=AsyncMock()) as gate:
   await self.inbox.initiate_one();gate.assert_not_called()
  with self.assertRaises(HTTPException):await self.inbox.save_settings(Settings(auto_first=True))
  with self.assertRaises(HTTPException):await self.inbox.save_topic(1,Topic(auto_first=True))
 async def test_personal_limits_are_enforced_on_api_not_only_ui(self):
  defaults={'daily_messages':10,'daily_ai_calls':20,'max_ai_replies':3,'delay_seconds':60}
  for field,value in [('daily_messages',11),('daily_ai_calls',21),('max_ai_replies',4),('delay_seconds',59)]:
   with self.assertRaises(HTTPException):await self.inbox.save_settings(Settings(**{**defaults,field:value}))
  await self.inbox.save_settings(Settings(**defaults))
  self.assertTrue(self.inbox.config()['paused'])
  with self.assertRaises(HTTPException):await self.inbox.save_topic(1,Topic(max_replies=4))
 async def test_reply_policy_migration_preserves_facts_and_lower_limits(self):
  self.store.put('owner_assistant_reply_only_v1',False)
  cfg=self.inbox.config();cfg.update(auto_first=True,daily_messages=5,delay_seconds=90)
  self.store.put('conversation_config',cfg)
  t=self.inbox.topic(1);t.update(facts='Согласованные сведения.',auto_first=True,max_replies=6)
  self.store.put('conversation_topic_1',t);self.inbox.initialize()
  self.assertFalse(self.inbox.config()['auto_first']);self.assertEqual(self.inbox.config()['daily_messages'],5)
  self.assertEqual(self.inbox.config()['delay_seconds'],90)
  self.assertEqual(self.inbox.topic(1)['facts'],'Согласованные сведения.')
  self.assertFalse(self.inbox.topic(1)['auto_first']);self.assertEqual(self.inbox.topic(1)['max_replies'],3)
 async def test_owner_ai_rejects_duplicate_long_and_link_replies(self):
  _,result=await self.watched_dialogue();c=self.inbox.get(result['conversation']['id'])
  trigger=result['messages'][-1]['id']
  for reply in ['Buyer, здравствуйте! Я Разработчикислав, SignM.','x'*701,'Вот сайт: https://example.com']:
   with self.assertRaises(HTTPException):self.inbox.enqueue(c,reply,'test_reply_rejection','ai',trigger)
  self.assertEqual(self.store.db.execute("SELECT count(*) FROM conversation_messages WHERE state='pending'").fetchone()[0],0)
 async def test_owner_ai_reply_is_bound_to_latest_incoming(self):
  _,result=await self.watched_dialogue();c=self.inbox.get(result['conversation']['id']);trigger=result['messages'][-1]['id']
  self.inbox.enqueue(c,'Я ИИ-помощник Разработчикислава. Какой результат нужен от сайта?','valid_business_reply','ai',trigger)
  with self.assertRaises(HTTPException):self.inbox.enqueue(c,'Какой у вас бюджет?','reply_to_stale_message','ai',trigger)
 async def test_manual_owner_message_cancels_pending_ai_and_pauses(self):
  _,result=await self.watched_dialogue();cid=result['conversation']['id'];c=self.inbox.get(cid)
  with self.store.db:self.store.db.execute("UPDATE conversations SET mode='ai' WHERE id=?",(cid,))
  self.inbox.enqueue(c,'Я ИИ-помощник Разработчикислава. Что должен делать сайт?','pending_before_human','ai',result['messages'][-1]['id'])
  message=types.Message(id=12,peer_id=types.PeerUser(77),from_id=types.PeerUser(42),date=datetime.now(timezone.utc),message='Это Разработчикислав, продолжу сам.',out=True)
  await self.inbox.ingest(self.inbox.get(cid),message)
  self.assertEqual(self.inbox.get(cid)['mode'],'manual')
  self.assertEqual(self.store.db.execute("SELECT state FROM conversation_messages WHERE request_key='pending_before_human'").fetchone()[0],'cancelled')
 async def test_sent_offer_does_not_establish_customer_budget_or_need(self):
  decision=Decision(action='reply',reply='Я ИИ-помощник Разработчикислава. Есть описание задачи?',facts=[Fact(name='budget',value='35000',evidence='Предлагаю сайт за 35000'),Fact(name='need',value='Сайт с каталогом',evidence='Нужен каталог')],interested=False,interest_quote='',contact_ready=False,contact_quote='',next_action='')
  payload={'lead':MANUAL_OFFER+'Предлагаю сайт за 35000','dialogue':[{'direction':'outgoing','text':'Предлагаю сайт за 35000'},{'direction':'incoming','text':'Нужен каталог'}]}
  with patch('owner_ai.structured',new=AsyncMock(return_value=(decision,{}))):
   result,_=await request_owner_decision('synthetic','model',payload)
  self.assertEqual([f.name for f in result.facts],['need'])
 async def test_first_ai_reply_must_disclose_assistant(self):
  _,result=await self.watched_dialogue();c=self.inbox.get(result['conversation']['id']);trigger=result['messages'][-1]['id']
  with self.assertRaises(HTTPException):self.inbox.enqueue(c,'Какой сайт нужен?','undisclosed_ai_reply','ai',trigger)
  mid=self.inbox.enqueue(c,'Я ИИ-помощник Разработчикислава. Есть описание задачи?','disclosed_ai_reply','ai',trigger)
  self.assertTrue(mid)

if __name__=='__main__':unittest.main()
