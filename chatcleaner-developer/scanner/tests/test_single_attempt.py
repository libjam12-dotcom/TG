"""Offline one-item controls: no Telegram connection or outbound account enable."""
import asyncio,json,time,unittest
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
from fastapi import HTTPException
from telethon import errors,functions,types
import operations
import test_operations as _fixtures

class ControlClient(_fixtures.CohortClient):
 def __init__(self,group):
  super().__init__(group);self.contacts=set();self.mutuals=set();self.failures={}
 async def get_entity(self,ref):
  user=await super().get_entity(ref)
  if isinstance(user,types.User):user.contact=user.id in self.contacts;user.mutual_contact=user.id in self.mutuals
  return user
 async def __call__(self,request):
  name=type(request).__name__
  if name in self.failures:self.calls.append(request);raise self.failures[name](request)
  if isinstance(request,functions.contacts.AddContactRequest):
   self.calls.append(request);self.contacts.add(request.id.id)
   return types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0)
  if isinstance(request,functions.users.GetUsersRequest):
   self.calls.append(request);return [await self.get_entity(types.InputPeerUser(user.id,user.access_hash)) for user in request.id]
  return await super().__call__(request)

class SingleAttemptTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.fixture=_fixtures.OperationsTests();await self.fixture.asyncSetUp()
  self.ops=self.fixture.ops;self.a=self.fixture.a
 async def asyncTearDown(self):await self.fixture.asyncTearDown()
 async def job(self,count=3,**kw):
  j=await self.fixture.pilot_draft(count,**kw);self.c=ControlClient(self.fixture.group);self.a.client=self.c;self.fixture.c=self.c
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?",(j['id'],))
  return await self.ops.detail(j['id'])
 def spec(self,j,item,key='single-control-request-key-001'):
  return operations.SingleAttempt(confirmed=True,request_key=key,reason='Explicit owner approved exactly one pending recipient',telegram_id=j['telegram_id'],chat_id=item['chat_id'],user_id=item['user_id'],cohort_hash=self.ops.pilot_cohort(j)[1])
 async def arm(self,j,index=1,key='single-control-request-key-001'):
  item=j['items'][index];self.c.mutuals.add(item['user_id'])
  return await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item,key))
 def invite_calls(self):return [r for r in self.c.calls if isinstance(r,functions.channels.InviteToChannelRequest)]
 async def test_persisted_gate_filters_the_selected_item_instead_of_first_sibling(self):
  j=await self.job();selected=j['items'][1];self.c.mutuals.add(selected['user_id'])
  record={'version':1,'state':'armed','job_id':j['id'],'item_id':selected['id'],'account':j['account'],'telegram_id':j['telegram_id'],'chat_id':selected['chat_id'],'user_id':selected['user_id'],'cohort_hash':self.ops.pilot_cohort(j)[1],'request_key':'persisted-single-control-key','digest':'fixture','requested_at':datetime.now(timezone.utc).isoformat(),'reason':'One selected recipient only','max_attempts':1,'requires_mutual_contact':True}
  record.update(utc_day=datetime.now(timezone.utc).date().isoformat(),expires_at=(int(time.time())//86400+1)*86400)
  self.ops.store.put('single_attempt_job_'+str(j['id']),record)
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET state='queued' WHERE id=?",(j['id'],))
  await self.ops.tick()
  self.assertEqual([r.users[0].id for r in self.invite_calls()],[selected['user_id']])
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['items'][0]['state'],'pending')
 async def test_preview_is_read_only_while_outbound_paused_and_reports_real_budget(self):
  j=await self.job(add_contacts_before_invite=True);self.a.pause();before=self.ops.store.db.iterdump();snapshot=list(before)
  preview=await self.ops.single_preview(j['id'],j['items'][1]['id'])
  self.assertFalse(preview['can_start']);self.assertTrue(preview['outbound_paused']);self.assertTrue(preview['receiver_action_pending']);self.assertTrue(preview['contact_preparation_planned'])
  self.assertEqual(preview['account_count_today'],0);self.assertEqual(preview['effective_daily_limit'],20)
  self.assertFalse(preview['invitation_requested']);self.assertEqual(list(self.ops.store.db.iterdump()),snapshot);self.assertFalse(self.c.calls)
 async def test_one_selected_success_always_pauses_and_key_replay_never_rearms(self):
  j=await self.job();item=j['items'][1];original_spec=j['spec'];await self.arm(j)
  await self.ops.tick();self.fixture.clear_wait();await self.ops.tick()
  fresh=await self.ops.detail(j['id']);self.assertEqual(len(self.invite_calls()),1);self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['spec'],original_spec)
  self.assertEqual([i['state'] for i in fresh['items']],['pending','invited','pending']);self.assertEqual(fresh['single_attempt']['state'],'finished')
  replay=await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item))
  self.assertEqual(replay['single_attempt']['state'],'finished');await self.ops.tick();self.assertEqual(len(self.invite_calls()),1)
  for action in ('start','resume','continue_pending'):
   with self.assertRaises(HTTPException):await self.ops.control(j['id'],operations.Control(action=action,confirmed=True))
 async def test_contact_prep_without_fresh_mutual_stops_before_invite_and_is_known_failure(self):
  j=await self.job(add_contacts_before_invite=True);item=j['items'][1]
  await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item));await self.ops.tick()
  fresh=await self.ops.detail(j['id']);result=fresh['items'][1]
  self.assertEqual(sum(isinstance(r,functions.contacts.AddContactRequest) for r in self.c.calls),1);self.assertFalse(self.invite_calls())
  self.assertEqual(result['state'],'failed');self.assertFalse(result['diagnosis']['invitation_requested']);self.assertEqual(fresh['state'],'paused')
  self.assertEqual(self.ops.account_daily_count(j['account'],datetime.now(timezone.utc).date().isoformat()),1)
 async def test_fresh_mutual_after_one_contact_prep_allows_exactly_one_invite(self):
  j=await self.job(add_contacts_before_invite=True);item=j['items'][1]
  original=self.c.__class__.__call__
  async def call(client,request):
   result=await original(client,request)
   if isinstance(request,functions.contacts.AddContactRequest):client.mutuals.add(request.id.id)
   return result
  with patch.object(ControlClient,'__call__',call):
   await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item));await self.ops.tick()
  self.assertEqual(len(self.invite_calls()),1);self.assertEqual((await self.ops.detail(j['id']))['state'],'paused')
 async def test_pending_flood_result_consumes_one_step_and_cannot_reclaim_same_item(self):
  j=await self.job(add_contacts_before_invite=True);item=j['items'][1]
  self.c.failures['AddContactRequest']=lambda req:errors.FloodWaitError(req,capture=30)
  await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item));await self.ops.tick();self.fixture.clear_wait()
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['items'][1]['state'],'pending');self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['single_attempt']['result_state'],'pending')
  with self.assertRaises(HTTPException):await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item,'single-control-request-key-002'))
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],operations.Control(action='continue_after_single',confirmed=True,ack_single_request_key=fresh['single_attempt']['request_key']))
  await self.ops.tick();self.assertEqual(len(self.c.calls),1);self.assertFalse(self.invite_calls())
 async def restart_case(self,claimed):
  j=await self.job();await self.arm(j)
  if claimed:
   async def crash(*_):raise asyncio.CancelledError()
   with patch.object(self.ops,'execute',crash):
    with self.assertRaises(asyncio.CancelledError):await self.ops.tick()
  self.ops.initialize();self.fixture.clear_wait();await self.ops.tick()
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['single_attempt']['state'],'interrupted');self.assertFalse(self.invite_calls())
 async def test_crash_before_claim_is_fail_closed_on_initialize(self):await self.restart_case(False)
 async def test_crash_after_claim_is_fail_closed_on_initialize(self):await self.restart_case(True)
 async def test_explicit_release_requires_latest_ack_known_result_and_preserves_pilot(self):
  j=await self.job();await self.arm(j);await self.ops.tick();self.fixture.clear_wait();fresh=await self.ops.detail(j['id'])
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],operations.Control(action='continue_after_single',confirmed=True,ack_single_request_key='wrong-single-control-key'))
  result=await self.ops.control(j['id'],operations.Control(action='continue_after_single',confirmed=True,ack_single_request_key=fresh['single_attempt']['request_key']))
  self.assertEqual(result['state'],'queued');self.assertEqual(result['single_attempt']['state'],'released');self.assertEqual(result['spec'],j['spec']);self.assertIsNone(result['pilot'])
 async def test_tampered_bindings_paused_account_and_normal_quota_never_arm(self):
  j=await self.job();item=j['items'][1];self.c.mutuals.add(item['user_id']);spec=self.spec(j,item)
  for key,value in [('telegram_id',101),('chat_id',-10099),('user_id',999),('cohort_hash','f'*64)]:
   with self.subTest(key=key):
    with self.assertRaises(HTTPException):await self.ops.start_single_attempt(j['id'],item['id'],spec.model_copy(update={key:value}))
  self.a.pause()
  with self.assertRaises(HTTPException):await self.ops.start_single_attempt(j['id'],item['id'],spec)
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET enabled=1 WHERE id=?',(j['account'],))
  await self.fixture.seed_daily_actions()
  preview=await self.ops.single_preview(j['id'],item['id']);self.assertFalse(preview['quota_available'])
  with self.assertRaises(HTTPException):await self.ops.start_single_attempt(j['id'],item['id'],spec)
  self.assertIsNone(self.ops.store.get('single_attempt_job_'+str(j['id'])));self.assertFalse(self.c.calls)
 async def test_active_pilot_debits_only_existing_budget_once_and_preserves_other_jobs(self):
  j=await self.job();base=await self.fixture.seed_daily_actions();await self.ops.start_pilot(j['id'],self.fixture.pilot_spec(additional_attempts=2))
  await self.ops.control(j['id'],operations.Control(action='pause'));before=await self.ops.detail(j['id'])
  old=self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(base['id'],));await self.arm(before);await self.ops.tick();self.fixture.clear_wait();await self.ops.tick()
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['pilot']['attempts_used'],1);self.assertEqual(fresh['pilot']['account_count_today'],21);self.assertEqual(fresh['pilot']['account_daily_ceiling'],22)
  self.assertEqual(fresh['spec'],j['spec']);self.assertEqual(self.ops.pilot_cohort(fresh)[1],self.ops.pilot_cohort(j)[1]);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(base['id'],)),old)
  self.assertEqual(len(self.invite_calls()),1);self.assertEqual(fresh['state'],'paused')
 async def test_mutual_lost_on_final_read_cannot_dispatch_invite(self):
  j=await self.job();await self.arm(j);self.c.mutuals.clear();await self.ops.tick()
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['items'][1]['state'],'failed');self.assertFalse(fresh['items'][1]['diagnosis']['invitation_requested']);self.assertFalse(self.invite_calls())
 async def test_peer_flood_is_shared_hardstop_and_unknown_server_result_never_releases(self):
  j=await self.job();await self.arm(j);self.c.failures['InviteToChannelRequest']=errors.PeerFloodError
  await self.ops.tick();self.fixture.clear_wait();await self.ops.tick();fresh=await self.ops.detail(j['id'])
  self.assertEqual(fresh['items'][1]['state'],'blocked');self.assertFalse(self.ops.account_row(j['account'])['enabled']);self.assertEqual(fresh['state'],'paused');self.assertEqual(len(self.invite_calls()),1)
 async def test_raw_server_failure_keeps_uncertain_and_requires_manual_review(self):
  j=await self.job();await self.arm(j);self.c.failures['InviteToChannelRequest']=lambda r:errors.ServerError(r,'SERVER_ERROR',code=500)
  await self.ops.tick();self.fixture.clear_wait();fresh=await self.ops.detail(j['id'])
  self.assertEqual(fresh['items'][1]['state'],'uncertain');self.assertFalse(fresh['single_attempt']['can_continue'])
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],operations.Control(action='continue_after_single',confirmed=True,ack_single_request_key=fresh['single_attempt']['request_key']))
  with self.assertRaises(HTTPException):await self.ops.start_single_attempt(j['id'],j['items'][2]['id'],self.spec(j,j['items'][2],'second-single-after-unknown'))
  await self.ops.tick();self.assertEqual(len(self.invite_calls()),1)
 async def test_spec_change_after_claim_blocks_all_mutating_requests(self):
  j=await self.job(add_contacts_before_invite=True);await self.arm(j);original=self.c.get_entity
  async def changed(ref):
   entity=await original(ref)
   if isinstance(ref,int) and ref<0:
    payload={**j['spec'],'delay_seconds':61}
    with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=?',(json.dumps(payload),j['id']))
   return entity
  self.c.get_entity=changed;await self.ops.tick();fresh=await self.ops.detail(j['id'])
  self.assertFalse(self.c.calls);self.assertFalse(fresh['items'][1]['diagnosis']['invitation_requested']);self.assertEqual(fresh['state'],'paused')
 async def test_no_gate_restart_does_not_seed_settings_or_change_history(self):
  j=await self.job();before=list(self.ops.store.db.iterdump());self.ops.initialize()
  self.assertEqual(list(self.ops.store.db.iterdump()),before)
 async def test_pilot_start_cannot_bypass_unreleased_single_gate_or_create_allowance(self):
  j=await self.job();await self.arm(j)
  self.assertFalse((await self.ops.pilot_preview(j['id']))['eligible'])
  with self.assertRaises(HTTPException):await self.ops.start_pilot(j['id'],self.fixture.pilot_spec())
  self.assertIsNone(self.ops.store.get('invite_pilot_'+str(j['id'])));self.assertFalse(self.c.calls)
 async def test_public_preview_and_start_contract_require_explicit_confirmation_and_exact_binding(self):
  from fastapi import FastAPI
  from httpx import AsyncClient,ASGITransport
  j=await self.job();item=j['items'][1];self.c.mutuals.add(item['user_id']);app=FastAPI();app.include_router(self.ops.router)
  base=f"/api/scanner/operations/jobs/{j['id']}/items/{item['id']}"
  async with AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
   response=await client.get(base+'/single-preview');self.assertEqual(response.status_code,200);preview=response.json();self.assertTrue(preview['can_start'])
   payload=self.spec(j,item).model_dump()
   for bad in ({**payload,'confirmed':False},{**payload,'pilot_allowance':99}):self.assertEqual((await client.post(base+'/single-attempt',json=bad)).status_code,422)
   receipt=(await client.post(base+'/single-attempt',json=payload)).json()
   self.assertEqual(receipt['single_attempt']['sender_id'],j['telegram_id']);self.assertEqual(receipt['single_attempt']['target'],item['chat_id']);self.assertEqual(receipt['single_attempt']['state'],'armed')
   self.assertEqual(receipt['job']['single_attempt']['request_key'],payload['request_key']);self.assertFalse(self.c.calls)
   self.assertNotIn('user_access_hash',json.dumps(receipt));self.assertNotIn('pilot_binding',json.dumps(receipt))
 async def test_next_explicit_single_uses_new_item_but_replay_of_old_key_cannot_release_new_gate(self):
  j=await self.job();await self.arm(j);await self.ops.tick();self.fixture.clear_wait()
  await self.arm(await self.ops.detail(j['id']),index=2,key='second-pending-control-key')
  replay=await self.ops.start_single_attempt(j['id'],j['items'][1]['id'],self.spec(j,j['items'][1]))
  self.assertFalse(replay['single_attempt']['can_continue']);self.assertEqual(replay['job']['single_attempt']['request_key'],'second-pending-control-key')
  await self.ops.tick();self.assertEqual([r.users[0].id for r in self.invite_calls()],[201,202])
  self.assertEqual(self.ops.account_daily_count(j['account'],datetime.now(timezone.utc).date().isoformat()),2)
 async def test_budget_lost_before_claim_pauses_instead_of_scheduling_old_step_tomorrow(self):
  j=await self.job();await self.arm(j);await self.fixture.seed_daily_actions()
  await self.ops.tick();fresh=await self.ops.detail(j['id'])
  self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['single_attempt']['state'],'interrupted');self.assertFalse(self.c.calls)
  tomorrow=(int(time.time())//86400+1)*86400+1
  with patch('operations.time.time',return_value=tomorrow),patch('operations.now',return_value=datetime.fromtimestamp(tomorrow,timezone.utc).isoformat()):await self.ops.tick()
  self.assertFalse(self.c.calls);self.assertIsNone(self.ops.store.get(f"single_attempt_item_{j['id']}_{j['items'][1]['id']}"))
 async def test_armed_step_expiring_before_claim_never_auto_starts_next_day(self):
  j=await self.job();await self.arm(j);tomorrow=(int(time.time())//86400+1)*86400+1
  with patch('operations.time.time',return_value=tomorrow),patch('operations.now',return_value=datetime.fromtimestamp(tomorrow,timezone.utc).isoformat()):await self.ops.tick()
  fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['state'],'paused');self.assertEqual(fresh['single_attempt']['state'],'interrupted');self.assertFalse(self.c.calls)
 async def test_single_window_expiring_on_final_read_never_dispatches_invite(self):
  j=await self.job();await self.arm(j);original=self.c.__class__.__call__;clock=[time.time()]
  async def expired(client,request):
   result=await original(client,request)
   if isinstance(request,functions.users.GetUsersRequest):clock[0]=(int(clock[0])//86400+1)*86400+1
   return result
  with patch('operations.time.time',side_effect=lambda:clock[0]),patch.object(ControlClient,'__call__',expired):await self.ops.tick()
  self.assertFalse(self.invite_calls());fresh=await self.ops.detail(j['id']);self.assertEqual(fresh['state'],'paused');self.assertFalse(fresh['items'][1]['diagnosis']['invitation_requested'])
 async def test_last_normal_slot_is_valid_after_its_own_claim(self):
  j=await self.job();await self.fixture.seed_daily_actions(19);await self.arm(j);await self.ops.tick()
  self.assertEqual(len(self.invite_calls()),1);self.assertEqual(self.ops.account_daily_count(j['account'],datetime.now(timezone.utc).date().isoformat()),20)
  self.assertEqual((await self.ops.detail(j['id']))['state'],'paused')
 async def test_expired_pilot_new_utc_day_uses_normal_quota_without_regrant(self):
  j=await self.job();await self.fixture.seed_daily_actions();await self.ops.start_pilot(j['id'],self.fixture.pilot_spec(additional_attempts=2));await self.ops.control(j['id'],operations.Control(action='pause'))
  original=self.ops.store.get('invite_pilot_'+str(j['id']));tomorrow=original['expires_at']+1;item=j['items'][1];self.c.mutuals.add(item['user_id'])
  with patch('operations.time.time',return_value=tomorrow),patch('operations.now',return_value=datetime.fromtimestamp(tomorrow,timezone.utc).isoformat()):
   preview=await self.ops.single_preview(j['id'],item['id']);self.assertTrue(preview['can_start']);self.assertEqual(preview['pilot']['state'],'expired');self.assertEqual(preview['effective_daily_limit'],20);self.assertEqual(preview['account_count_today'],0)
   await self.ops.start_single_attempt(j['id'],item['id'],self.spec(j,item));await self.ops.tick()
  self.assertEqual(len(self.invite_calls()),1);self.assertEqual(self.ops.store.get('invite_pilot_'+str(j['id'])),original)
