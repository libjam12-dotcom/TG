"""Offline SDK transport and current-session diagnostics; no Telegram connection."""
import asyncio,json,os,sys,time,unittest
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from telethon import TelegramClient,errors,functions,types
from telethon.sessions import MemorySession
from operations import Operations,invite_rpc_diagnosis
import test_operations as _fixtures

class Sender:
 def __init__(self,failure):self.failure=failure;self.requests=[]
 def send(self,request,ordered=False):
  self.requests.append(type(request).__name__);future=asyncio.get_running_loop().create_future()
  if len(self.requests)==1:future.set_exception(self.failure(request))
  else:future.set_result(types.messages.InvitedUsers(updates=types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0),missing_invitees=[]))
  return future

class RPCDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.fixture=_fixtures.OperationsTests();await self.fixture.asyncSetUp()
  self.ops,self.a,self.c=self.fixture.ops,self.fixture.a,self.fixture.c
 async def asyncTearDown(self):await self.fixture.asyncTearDown()
 async def client_variants(self):
  pool=Operations.make_client(MemorySession(),{'id':123,'hash':'0'*32})
  with patch.dict(os.environ,{'SCANNER_DATA':str(Path(self.fixture.tmp.name)/'app')}):import app
  self.ops.store.put('api',{'id':123,'hash':'0'*32});clients=[]
  client_type=app.TelegramClient
  def factory(session,aid,api_hash,**kwargs):
   c=client_type(MemorySession(),aid,api_hash,**kwargs)
   c.connect=AsyncMock();c.is_user_authorized=AsyncMock(return_value=False);clients.append(c);return c
  with patch.object(app,'store',self.ops.store),patch.object(app,'TelegramClient',factory):
   runtime=app.Runtime();await runtime.connect()
  return [('pool',pool),('primary',clients[0])]
 def auth_sender(self,client,failure,*,repeat=False,switch=True,authorized=False):
  calls=[]
  def send(req,ordered=False):
   calls.append(type(req).__name__);future=asyncio.get_running_loop().create_future()
   if len(calls)==1 or repeat:future.set_exception(failure(req,len(calls)))
   else:future.set_result(types.auth.SentCode(types.auth.SentCodeTypeApp(length=5),'fixture-code-hash'))
   return future
  client._sender=SimpleNamespace(send=send);client.session.set_dc(2,'149.154.167.51',443)
  async def change_dc(dc):
   if switch:client.session.set_dc(dc,'149.154.167.91',443)
  client._switch_dc=AsyncMock(side_effect=change_dc);client.is_user_authorized=AsyncMock(return_value=authorized)
  return calls
 async def test_public_send_and_resend_code_keep_one_explicit_dc_redirect_without_write_retries(self):
  reference=TelegramClient(MemorySession(),123,'0'*32)
  calls=self.auth_sender(reference,lambda req,n:errors.PhoneMigrateError(req,capture=4))
  await reference.send_code_request('15550000001');self.assertEqual(calls,['SendCodeRequest','SendCodeRequest'])
  for name,client in await self.client_variants():
   for resend in [False,True]:
    with self.subTest(client=name,resend=resend):
     phone='15550000001';client._phone_code_hash.clear()
     if resend:client._phone_code_hash[phone]='previous-fixture-code-hash'
     calls=self.auth_sender(client,lambda req,n:errors.NetworkMigrateError(req,capture=4) if resend else errors.PhoneMigrateError(req,capture=4))
     result=await client.send_code_request(phone)
     kind='ResendCodeRequest' if resend else 'SendCodeRequest'
     self.assertEqual(calls,[kind,kind]);self.assertEqual(result.phone_code_hash,'fixture-code-hash')
     self.assertEqual(client._phone_code_hash[phone],'fixture-code-hash');self.assertEqual(client._request_retries,0)
 async def test_auth_redirect_does_not_retry_server_timeout_peer_or_flood_failures(self):
  for name,client in await self.client_variants():
   for kind in ['server','timeout','peer','flood']:
    with self.subTest(client=name,error=kind):
     errors_by_name={'server':lambda r:errors.ServerError(r,'synthetic_server_error',code=500),'timeout':lambda r:errors.TimedOutError(r,'TIMEOUT',code=503),'peer':errors.PeerFloodError,'flood':lambda r:errors.FloodWaitError(r,capture=30)}
     client._phone_code_hash.clear();calls=self.auth_sender(client,lambda req,n:errors_by_name[kind](req))
     with patch('telethon.client.users.asyncio.sleep',new=AsyncMock()):
      with self.assertRaises(errors.RPCError):await client.send_code_request('15550000001')
     self.assertEqual(calls,['SendCodeRequest']);client._switch_dc.assert_not_awaited()
 async def test_auth_redirect_is_bounded_and_requires_real_dc_switch_and_not_authorized(self):
  for name,client in await self.client_variants():
   for scenario in ['second_redirect','no_switch','already_authorized']:
    with self.subTest(client=name,scenario=scenario):
     client._phone_code_hash.clear();calls=self.auth_sender(client,lambda req,n:errors.PhoneMigrateError(req,capture=3+n),repeat=True,switch=scenario!='no_switch',authorized=scenario=='already_authorized')
     with self.assertRaises(errors.PhoneMigrateError):await client.send_code_request('15550000001')
     self.assertEqual(len(calls),2 if scenario=='second_redirect' else 1);self.assertEqual(client._request_retries,0)
 async def test_invite_migration_is_not_repeated_by_auth_redirect_wrapper(self):
  for name,client in await self.client_variants():
   with self.subTest(client=name):
    calls=self.auth_sender(client,lambda req,n:errors.PhoneMigrateError(req,capture=4))
    req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
    with self.assertRaises(errors.PhoneMigrateError):await client(req)
    self.assertEqual(calls,['InviteToChannelRequest']);self.assertEqual(client._request_retries,0)
 async def test_real_sdk_constructors_never_reissue_invite_after_retryable_server_error(self):
  for name,client in await self.client_variants():
   with self.subTest(client=name):
    sender=Sender(lambda req:errors.ServerError(req,'synthetic_server_error',code=500));client._sender=sender
    req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
    with patch('telethon.client.users.asyncio.sleep',new=AsyncMock()):
     try:await client(req)
     except errors.ServerError:pass
    self.assertEqual(sender.requests,['InviteToChannelRequest'])
    self.assertEqual(client._request_retries,0);self.assertTrue(client._raise_last_call_error)
 async def test_real_sdk_peer_flood_is_raw_and_never_automatically_retried(self):
  for name,client in await self.client_variants():
   with self.subTest(client=name):
    sender=Sender(errors.PeerFloodError);client._sender=sender
    req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
    with self.assertRaises(errors.PeerFloodError):await client(req)
    self.assertEqual(sender.requests,['InviteToChannelRequest'])
 def test_rpc_diagnosis_records_only_safe_constructor_code_and_known_enum(self):
  req=functions.channels.InviteToChannelRequest(types.InputChannel(10,99123456789),[types.InputUser(200,88123456789)])
  metadata=invite_rpc_diagnosis(errors.PeerFloodError(req))['rpc']
  self.assertEqual(metadata,{'request_constructor':'InviteToChannelRequest','exception_type':'PeerFloodError','code':400,'message':'PEER_FLOOD'})
  for token in ['99123456789','88123456789','access_hash','users','channel']:
   self.assertNotIn(token,json.dumps(metadata))
  from operations import safe_rpc_metadata
  unknown=safe_rpc_metadata(errors.RPCError(req,'PRIVATE_SECRET_1234',code=400))
  self.assertIsNone(unknown['message']);self.assertNotIn('PRIVATE_SECRET_1234',json.dumps(unknown))
  untrusted=safe_rpc_metadata(errors.RPCError(SimpleNamespace(phone='PRIVATE_PHONE'),'PHONE_NUMBER_INVALID',code=400))
  self.assertIsNone(untrusted['request_constructor']);self.assertEqual(untrusted['message'],'PHONE_NUMBER_INVALID')
 async def test_new_invite_error_retains_rpc_evidence_without_rewriting_older_items(self):
  legacy=await self.fixture.draft('invite',request_key='legacy-error-without-rpc-metadata')
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='failed',diagnosis=? WHERE job=?",(json.dumps({'code':'LegacyRecordedError','preserved':'original evidence'}),legacy['id']))
  old=self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(legacy['id'],))
  j=await self.fixture.draft('invite')
  req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
  self.c.failure=errors.PeerFloodError(req);await self.fixture.start(j);await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertTrue(item['diagnosis']['invitation_requested']);self.assertEqual(item['diagnosis']['rpc']['request_constructor'],'InviteToChannelRequest')
  self.assertEqual(item['diagnosis']['rpc']['message'],'PEER_FLOOD');self.assertEqual(item['state'],'blocked')
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(legacy['id'],)),old)
 async def assert_ambiguous_invite(self,error):
  j=await self.fixture.draft('invite');self.c.failure=error;await self.fixture.start(j);await self.ops.tick()
  result=await self.ops.detail(j['id']);item=result['items'][0]
  self.assertEqual(item['state'],'uncertain');self.assertEqual(result['state'],'paused')
  self.assertTrue(item['diagnosis']['invitation_requested']);self.assertEqual(item['diagnosis']['verification_error']['rpc']['exception_type'],type(error).__name__)
  self.assertEqual(len(self.c.calls),1);self.fixture.clear_wait();self.c.failure=None;await self.ops.tick();self.assertEqual(len(self.c.calls),1)
 async def test_dispatched_invite_server_error_is_unknown_and_never_retried(self):
  req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
  await self.assert_ambiguous_invite(errors.ServerError(req,'synthetic_server_error',code=500))
 async def test_dispatched_invite_sdk_timeout_is_unknown_and_never_retried(self):
  req=functions.channels.InviteToChannelRequest(types.InputChannel(10,11),[types.InputUser(200,22)])
  await self.assert_ambiguous_invite(errors.TimedOutError(req,'TIMEOUT',code=503))
 async def test_preflight_server_error_stays_failed_without_invite_marker(self):
  j=await self.fixture.draft('invite');await self.fixture.start(j)
  self.c.get_entity=AsyncMock(side_effect=errors.ServerError(None,'synthetic_server_error',code=500));await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertFalse(item['diagnosis']['invitation_requested']);self.assertFalse(self.c.calls)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_invited'),[])
 async def test_conversation_raw_ambiguous_rpc_is_uncertain_and_keeps_valid_receipt_guard(self):
  import test_conversations as inbox_fixtures
  for error in [errors.ServerError(None,'synthetic_server_error',code=500),errors.TimedOutError(None,'TIMEOUT',code=503)]:
   fixture=inbox_fixtures.InboxTests();await fixture.asyncSetUp()
   try:
    message=await fixture.send();fixture.client.failure=error;await fixture.crm.deliver_one()
    self.assertEqual(fixture.row(message['id'])['state'],'uncertain');fixture.client.failure=None
    await fixture.crm.deliver_one();self.assertFalse(fixture.client.sent)
   finally:await fixture.asyncTearDown()
 async def test_publisher_raw_ambiguous_rpc_is_uncertain_and_not_retryable(self):
  import test_publisher as publisher_fixtures
  for error in [errors.ServerError(None,'synthetic_server_error',code=500),errors.TimedOutError(None,'TIMEOUT',code=503)]:
   fixture=publisher_fixtures.PublisherTests();await fixture.asyncSetUp()
   try:
    post=await fixture.draft();await fixture.pub.publish(post['id']);fixture.pub.send.side_effect=error
    await fixture.pub.tick();self.assertEqual(fixture.pub.post(post['id'])['delivery'][0]['state'],'uncertain')
    with self.assertRaises(HTTPException):await fixture.pub.retry(post['id'])
    await fixture.pub.tick();fixture.pub.send.assert_awaited_once()
   finally:await fixture.asyncTearDown()
 def authorization(self,**changes):
  at=datetime.now(timezone.utc);values=dict(hash=77123456789,device_model='Fixture PC',platform='Linux',system_version='6.8',api_id=123,app_name='Fixture app',app_version='1.0',date_created=at,date_active=at,ip='PRIVATE_IP',country='PRIVATE_COUNTRY',region='PRIVATE_REGION',current=True,password_pending=False,unconfirmed=False)
  values.update(changes);return types.Authorization(**values)
 async def with_authorizations(self,records,callback=None):
  self.c.api_id=123;calls=[]
  async def transport(client,req):
   calls.append(type(req).__name__)
   self.assertIsInstance(req,functions.account.GetAuthorizationsRequest)
   if callback:callback()
   return types.account.Authorizations(authorization_ttl_days=180,authorizations=records)
  with patch.object(type(self.c),'__call__',transport):result=await self.ops.account_diagnostics(self.a.aid)
  return result,calls
 async def test_diagnostics_reads_only_current_authorization_while_paused_and_redacts_private_fields(self):
  self.a.pause()
  before={t:self.ops.store.rows('SELECT * FROM '+t) for t in ['op_accounts','op_jobs','op_items','op_invited','settings']}
  result,calls=await self.with_authorizations([self.authorization(current=False,app_name='OTHER_PRIVATE_SESSION'),self.authorization()])
  self.assertEqual(calls,['GetAuthorizationsRequest']);self.assertTrue(result['connected']);self.assertTrue(result['outbound_paused'])
  self.assertEqual((result['expected_sender_id'],result['sender_id']),(100,100));self.assertTrue(result['session_current'])
  self.assertFalse(result['unconfirmed']);self.assertFalse(result['password_pending']);self.assertTrue(result['client_config_api_id_match'])
  self.assertEqual(result['current_auth_api_id'],123);self.assertEqual(result['app_name'],'Fixture app')
  for private in ['PRIVATE_IP','PRIVATE_COUNTRY','PRIVATE_REGION','77123456789','OTHER_PRIVATE_SESSION','api_hash','auth_key']:
   self.assertNotIn(private,json.dumps(result))
  for table,rows in before.items():self.assertEqual(self.ops.store.rows('SELECT * FROM '+table),rows)
  with self.assertRaises(HTTPException):self.ops.online(self.a.aid)
 async def test_diagnostics_missing_current_auth_or_api_mismatch_is_honest_not_a_new_session(self):
  result,_=await self.with_authorizations([self.authorization(current=False)])
  self.assertEqual(result['authorization_status'],'unconfirmed');self.assertIsNone(result['session_current']);self.assertIsNone(result['app_name']);self.assertIsNone(result['client_config_api_id_match'])
  result,_=await self.with_authorizations([self.authorization(api_id=999,password_pending=True,unconfirmed=True)])
  self.assertFalse(result['client_config_api_id_match']);self.assertTrue(result['password_pending']);self.assertTrue(result['unconfirmed'])
 async def test_diagnostics_rejects_wrong_fresh_identity_changed_client_and_cooldown(self):
  self.c.get_me=AsyncMock(return_value=types.User(id=999))
  with self.assertRaises(HTTPException) as mismatch:await self.ops.account_diagnostics(self.a.aid)
  self.assertEqual(mismatch.exception.status_code,409)
  self.c.get_me=AsyncMock(return_value=types.User(id=100,username='owned_account'))
  def replace():self.a.client=_fixtures.Client(self.fixture.group)
  with self.assertRaises(HTTPException) as changed:await self.with_authorizations([self.authorization()],replace)
  self.assertEqual(changed.exception.status_code,409);self.a.client=self.c
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=? WHERE id=?',(time.time()+60,self.a.aid))
  self.c.get_me.reset_mock()
  with self.assertRaises(HTTPException) as wait:await self.ops.account_diagnostics(self.a.aid)
  self.assertEqual(wait.exception.status_code,429);self.c.get_me.assert_not_awaited()
 async def test_diagnostics_flood_wait_preserves_max_cooldown_and_does_not_enable(self):
  self.a.pause()
  async def limited():
   with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=? WHERE id=?',(time.time()+120,self.a.aid))
   raise errors.FloodWaitError(None,capture=30)
  self.c.get_me=limited
  with self.assertRaises(HTTPException) as wait:await self.ops.account_diagnostics(self.a.aid)
  self.assertEqual(wait.exception.status_code,429);self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],time.time()+115);self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
 async def test_diagnostics_peer_flood_or_revoked_session_keeps_shared_protective_stop(self):
  for error in [errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError]:
   with self.subTest(error=error.__name__):
    self.a.client=self.c;self.a.me=types.User(id=100,username='owned_account')
    with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET enabled=1 WHERE id=?',(self.a.aid,))
    self.c.get_me=AsyncMock(side_effect=error(None));before=self.ops.store.rows('SELECT * FROM op_items')
    with self.assertRaises(HTTPException) as blocked:await self.ops.account_diagnostics(self.a.aid)
    self.assertEqual(blocked.exception.status_code,409);self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before)
