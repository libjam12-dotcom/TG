import asyncio
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import sys,tempfile,unittest,time
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from operations import Operations,AccountSpec,JobSpec,Control,PoolAccount,ImportSpec,parse_import,ProxySpec,RetryInvite,AddContact,PilotStart,ResumeOutbound
from owner_sender import Phone,Code,Password
from fastapi import HTTPException
from telethon import types,functions,errors
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from telethon.sessions import StringSession
from telethon.crypto import AuthKey

class Client:
 def __init__(self,group):
  self.group=group;self.calls=[];self.failure=None;self.member=False;self.missing=False;self.missing_flags={};self.contact=False;self.mutual=False
  self.get_me=AsyncMock(return_value=types.User(id=100,username='owned_account'))
  self.sign_in=AsyncMock();self.send_code_request=AsyncMock(return_value=SimpleNamespace(phone_code_hash='private-code-hash'))
  self.disconnect=AsyncMock();self.log_out=AsyncMock()
 def is_connected(self):return True
 async def get_entity(self,ref):return self.group if isinstance(ref,int) and ref<0 else types.User(id=200,first_name='Consenting',username='agreed_user',access_hash=1,contact=self.contact,mutual_contact=self.mutual)
 async def get_input_entity(self,entity):return entity
 async def get_permissions(self,entity,user):
  if user=='me' or self.member:return ParticipantPermissions(types.ChannelParticipant(user_id=100 if user=='me' else user.id,date=datetime.now(timezone.utc)),False)
  raise errors.UserNotParticipantError(None)
 async def __call__(self,req):
  self.calls.append(req)
  if self.failure:raise self.failure
  if isinstance(req,functions.contacts.AddContactRequest):
   self.contact=True
   return types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0)
  if isinstance(req,functions.users.GetUsersRequest):
   return [types.User(id=req.id[0].id,first_name='Consenting',username='agreed_user',access_hash=1,contact=self.contact,mutual_contact=self.mutual)]
  if isinstance(req,functions.messages.SendMessageRequest):return types.UpdateShortSentMessage(id=700,pts=1,pts_count=1,date=datetime.now(timezone.utc))
  if isinstance(req,functions.messages.ExportChatInviteRequest):return types.ChatInviteExported(link='https://t.me/+DEVELOPMENT_ONLY',admin_id=100,date=datetime.now(timezone.utc))
  self.member=not self.missing
  return types.messages.InvitedUsers(updates=types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0),missing_invitees=[types.MissingInvitee(user_id=200,**self.missing_flags)] if self.missing else [])

class CohortClient(Client):
 def __init__(self,group):super().__init__(group);self.members=set()
 async def get_entity(self,ref):
  if isinstance(ref,int) and ref<0:return self.group
  uid=ref.user_id if isinstance(ref,types.InputPeerUser) else int(ref.rsplit('_',1)[-1])+200 if isinstance(ref,str) and ref.startswith('pilot_user_') else ref
  return types.User(id=uid,first_name='Recipient',username='pilot_user_'+str(uid-200),access_hash=uid+100,contact=False,mutual_contact=False)
 async def get_permissions(self,entity,user):
  if user.id in self.members:return ParticipantPermissions(types.ChannelParticipant(user_id=user.id,date=datetime.now(timezone.utc)),False)
  raise errors.UserNotParticipantError(None)
 async def __call__(self,request):
  result=await super().__call__(request)
  if isinstance(request,functions.channels.InviteToChannelRequest) and not self.missing:self.members.update(user.id for user in request.users)
  return result

class OperationsTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.ops=Operations(self.tmp.name,lambda:{'id':123,'hash':'synthetic-app-hash'});self.ops.initialize()
  self.group=types.Channel(id=10,title='Managed group',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,creator=True,username='managed_group')
  self.acc=await self.ops.add_account(AccountSpec(label='Authorized',authorized=True));self.a=self.ops.accounts[self.acc['id']]
  self.c=Client(self.group);self.a.client=self.c;await self.a.ready()
 async def asyncTearDown(self):await self.ops.shutdown();self.tmp.cleanup()
 async def draft(self,kind='post',**kw):
  values=dict(request_key='test-unique-key-0001',account=self.a.aid,kind=kind,title='Test',destinations=[-1000000000010],text='Publication' if kind=='post' else '')
  if kind=='invite':values.update(users=['@agreed_user'],consent_note='Consent obtained during event registration')
  values.update(kw);return await self.ops.create(JobSpec(**values))
 async def start(self,j):return await self.ops.control(j['id'],Control(action='start',confirmed=True))
 def clear_wait(self):
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=0')
 async def pilot_draft(self,count=12,**kw):
  self.c=CohortClient(self.group);self.a.client=self.c
  return await self.draft('invite',users=['pilot_user_'+str(i) for i in range(count)],**kw)
 def pilot_spec(self,**kw):
  values=dict(confirmed=True,request_key='one-time-pilot-key-0001',reason='Owner approved a bounded pilot for today',additional_attempts=50,start_now=True)
  values.update(kw);return PilotStart(**values)
 async def seed_daily_actions(self,count=20):
  base=await self.draft(request_key='baseline-actions-key-0001')
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_items SET state='sent',attempted_at=?,finished_at=? WHERE job=?",(datetime.now(timezone.utc).isoformat(),datetime.now(timezone.utc).isoformat(),base['id']))
   self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?",(base['id'],))
   for _ in range(count-1):
    self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,attempted_at) SELECT job,chat_id,chat_title,state,random_ids,attempted_at FROM op_items WHERE job=? LIMIT 1',(base['id'],))
  return base
 async def test_pilot_preview_is_read_only_and_shows_additional_budget_honestly(self):
  j=await self.pilot_draft();await self.seed_daily_actions()
  before=self.ops.store.rows('SELECT * FROM op_jobs ORDER BY id')
  preview=await self.ops.pilot_preview(j['id'])
  self.assertEqual(preview['account_count_today'],20);self.assertEqual(preview['proposed_account_daily_ceiling'],70)
  self.assertEqual(preview['normal_daily_limit'],20);self.assertTrue(preview['eligible'])
  self.assertIsNone(preview['pilot']);self.assertFalse(self.c.calls)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs ORDER BY id'),before)
  self.assertFalse(self.ops.store.rows("SELECT key FROM settings WHERE key LIKE 'invite_pilot_%'"))
 async def test_pilot_confirmation_is_required_bounded_and_cannot_be_forged_in_jobs(self):
  from pydantic import ValidationError
  for values in [dict(confirmed=False),dict(start_now=False),dict(additional_attempts=51),dict(goal_added=11),dict(unknown_override=True)]:
   with self.subTest(values=values):
    with self.assertRaises(ValidationError):self.pilot_spec(**values)
  for key in ['pilot','pilot_allowance','pilot_budget']:
   with self.assertRaises(ValidationError):await self.draft('invite',**{key:{'account_daily_ceiling':100}})
 async def test_pilot_start_is_idempotent_and_preserves_other_jobs_and_counts(self):
  j=await self.pilot_draft(scheduled_at=datetime.now(timezone.utc)+timedelta(hours=2));base=await self.seed_daily_actions()
  other=await self.draft(request_key='unrelated-draft-key-001')
  jobs=self.ops.store.rows('SELECT * FROM op_jobs WHERE id!=? ORDER BY id',(j['id'],));items=self.ops.store.rows('SELECT * FROM op_items ORDER BY id')
  started=await self.ops.start_pilot(j['id'],self.pilot_spec())
  self.assertEqual(started['state'],'queued');self.assertEqual(started['not_before'],0)
  self.assertEqual(started['pilot']['account_daily_ceiling'],70);self.assertEqual(started['pilot']['remaining_attempts'],50)
  self.assertEqual(started['spec']['daily_limit'],20);self.assertFalse(self.c.calls)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs WHERE id!=? ORDER BY id',(j['id'],)),jobs)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_items ORDER BY id'),items)
  await self.ops.tick();self.clear_wait();again=await self.ops.start_pilot(j['id'],self.pilot_spec())
  self.assertEqual(again['pilot']['attempts_used'],1);self.assertEqual(again['pilot']['account_count_today'],21)
  self.assertEqual(again['pilot']['remaining_attempts'],49)
  with self.assertRaises(HTTPException):await self.ops.start_pilot(j['id'],self.pilot_spec(request_key='second-grant-key-0001'))
  with self.assertRaises(HTTPException):await self.ops.start_pilot(other['id'],self.pilot_spec())
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs WHERE id=?',(base['id'],))[0]['state'],'completed')
 async def test_only_selected_pilot_can_exceed_normal_quota_and_shared_count_still_caps_it(self):
  j=await self.pilot_draft();base=await self.seed_daily_actions()
  await self.ops.start_pilot(j['id'],self.pilot_spec(additional_attempts=2));await self.ops.tick();self.clear_wait()
  self.assertEqual(len(self.c.calls),1)
  with self.ops.store.db:
   self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,attempted_at) SELECT job,chat_id,chat_title,state,random_ids,attempted_at FROM op_items WHERE job=? LIMIT 1',(base['id'],))
  await self.ops.tick();detail=await self.ops.detail(j['id'])
  self.assertEqual(len(self.c.calls),1);self.assertEqual(detail['state'],'paused')
  self.assertEqual(detail['pilot']['account_count_today'],22);self.assertEqual(detail['pilot']['attempts_used'],1)
  self.assertEqual(detail['pilot']['state'],'budget_exhausted');self.assertEqual(detail['pilot']['remaining_attempts'],0)
  other=await self.draft(request_key='other-default-quota-key');await self.start(other);await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertEqual((await self.ops.detail(other['id']))['items'][0]['state'],'pending')
 async def test_pilot_goal_stops_before_eleventh_invite_and_cannot_resume_or_regrant(self):
  j=await self.pilot_draft();await self.seed_daily_actions()
  await self.ops.start_pilot(j['id'],self.pilot_spec())
  for _ in range(10):
   await self.ops.tick();self.clear_wait()
  detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['state'],'paused');self.assertEqual(detail['pilot']['state'],'goal_met')
  self.assertEqual(detail['pilot']['verified_added'],10);self.assertEqual(detail['pilot']['attempts_used'],10)
  self.assertEqual([i['state'] for i in detail['items']].count('pending'),2);self.assertEqual(len(self.c.calls),10)
  await self.ops.tick();self.assertEqual(len(self.c.calls),10)
  for action in ['resume','continue_pending']:
   with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action=action,confirmed=True))
  with self.assertRaises(HTTPException):await self.ops.start_pilot(j['id'],self.pilot_spec(request_key='goal-cannot-regrant-key'))
  again=await self.ops.start_pilot(j['id'],self.pilot_spec());self.assertEqual(again['state'],'paused')
 async def test_expired_pilot_uses_normal_quota_without_rearming(self):
  j=await self.pilot_draft();await self.seed_daily_actions();await self.ops.start_pilot(j['id'],self.pilot_spec())
  record=self.ops.store.get('invite_pilot_'+str(j['id']));record['expires_at']=0;self.ops.store.put('invite_pilot_'+str(j['id']),record)
  await self.ops.tick();detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['pilot']['state'],'expired');self.assertFalse(self.c.calls)
  self.assertGreater(detail['not_before'],datetime.now(timezone.utc).timestamp())
  with self.assertRaises(HTTPException):await self.ops.start_pilot(j['id'],self.pilot_spec(request_key='expired-new-grant-key'))
 async def test_pilot_sender_cohort_or_spec_tamper_never_raises_normal_quota(self):
  for change in ['sender','cohort','spec','allowance']:
   with self.subTest(change=change):
    # Each case starts from an independent SQLite fixture, without deleting any history.
    test=OperationsTests();await test.asyncSetUp()
    try:
     j=await test.pilot_draft();await test.seed_daily_actions();await test.ops.start_pilot(j['id'],test.pilot_spec())
     with test.ops.store.db:
      if change=='sender':
       test.a.me=types.User(id=101,username='changed_sender')
       test.ops.store.db.execute('UPDATE op_accounts SET telegram_id=101 WHERE id=?',(test.a.aid,))
       test.ops.store.db.execute('UPDATE op_jobs SET telegram_id=101 WHERE id=?',(j['id'],))
      elif change=='cohort':test.ops.store.db.execute('UPDATE op_items SET user_id=999 WHERE id=?',(j['items'][0]['id'],))
      elif change=='spec':
       payload=j['spec'];payload['delay_seconds']=30;test.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=?',(json.dumps(payload),j['id']))
      else:
       record=test.ops.store.get('invite_pilot_'+str(j['id']));record['account_daily_ceiling']=999;test.ops.save_pilot(j['id'],record)
     await test.ops.tick();detail=await test.ops.detail(j['id'])
     self.assertEqual(detail['pilot']['state'],'invalid');self.assertFalse(test.c.calls)
     self.assertGreater(detail['not_before'],datetime.now(timezone.utc).timestamp())
    finally:await test.asyncTearDown()
 async def test_pilot_flood_wait_pauses_without_rearming_or_ignoring_account_cooldown(self):
  j=await self.pilot_draft();await self.seed_daily_actions();await self.ops.start_pilot(j['id'],self.pilot_spec())
  self.c.failure=errors.FloodWaitError(None,capture=60);await self.ops.tick();await self.ops.tick()
  detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['state'],'paused');self.assertEqual(detail['pilot']['attempts_used'],1);self.assertEqual(len(self.c.calls),1)
  self.assertEqual(detail['items'][0]['state'],'pending')
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='resume',confirmed=True))
  again=await self.ops.start_pilot(j['id'],self.pilot_spec());self.assertEqual(again['state'],'paused')
  self.assertEqual(again['pilot']['attempts_used'],1);self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+55)
 async def test_claimed_pilot_expiring_during_user_read_cannot_write_to_telegram(self):
  import time
  j=await self.pilot_draft();await self.seed_daily_actions()
  clock=[(int(time.time())//86400+1)*86400-5]
  with patch('operations.time.time',side_effect=lambda:clock[0]):
   await self.ops.start_pilot(j['id'],self.pilot_spec())
   original=self.c.get_entity
   async def delayed(ref):
    user=await original(ref)
    if isinstance(ref,int) and ref<0:clock[0]+=10
    return user
   self.c.get_entity=delayed;await self.ops.tick()
   detail=await self.ops.detail(j['id']);item=detail['items'][0]
   self.assertEqual(item['state'],'failed');self.assertFalse(item['diagnosis']['invitation_requested'])
   self.assertEqual(item['diagnosis']['code'],'invite_preflight_failed');self.assertFalse(self.c.calls)
   self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'));self.assertEqual(detail['pilot']['attempts_used'],1)
 async def test_claimed_pilot_expiring_in_last_contact_context_await_cannot_add_contact(self):
  import time
  j=await self.pilot_draft(add_contacts_before_invite=True);await self.seed_daily_actions()
  clock=[(int(time.time())//86400+1)*86400-5]
  with patch('operations.time.time',side_effect=lambda:clock[0]):
   await self.ops.start_pilot(j['id'],self.pilot_spec())
   original=self.ops.rights;checks=0
   async def delayed(*args,**kw):
    nonlocal checks
    result=await original(*args,**kw);checks+=1
    if checks==2:clock[0]+=10
    return result
   self.ops.rights=delayed;await self.ops.tick()
   detail=await self.ops.detail(j['id']);item=detail['items'][0]
   self.assertEqual(checks,2);self.assertEqual(item['state'],'failed')
   self.assertFalse(self.c.calls);self.assertFalse(item['diagnosis']['invitation_requested'])
   self.assertFalse(item['diagnosis']['contact_preparation']['request_sent'])
   self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'));self.assertEqual(detail['pilot']['attempts_used'],1)
 async def test_last_claimed_slot_remains_valid_after_its_budget_debit(self):
  j=await self.pilot_draft();await self.seed_daily_actions()
  await self.ops.start_pilot(j['id'],self.pilot_spec(additional_attempts=1));await self.ops.tick()
  detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['items'][0]['state'],'invited');self.assertEqual(len(self.c.calls),1)
  self.assertEqual(detail['pilot']['attempts_used'],1);self.assertEqual(detail['pilot']['account_count_today'],21)
  self.assertEqual(detail['pilot']['state'],'budget_exhausted')
 async def test_cohort_changed_after_pilot_claim_cannot_send_an_invite(self):
  j=await self.pilot_draft();await self.seed_daily_actions();await self.ops.start_pilot(j['id'],self.pilot_spec())
  original=self.c.get_entity
  async def changed(ref):
   user=await original(ref)
   if isinstance(ref,int) and ref<0:
    with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET user_name=? WHERE id=?',('changed_username',j['items'][1]['id']))
   return user
  self.c.get_entity=changed;await self.ops.tick();item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertFalse(item['diagnosis']['invitation_requested'])
  self.assertFalse(self.c.calls);self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_pilot_rejects_unknown_results_duplicate_cohort_and_wrong_sender(self):
  j=await self.pilot_draft()
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='uncertain' WHERE id=?",(j['items'][0]['id'],))
  self.assertFalse((await self.ops.pilot_preview(j['id']))['eligible'])
  with self.assertRaises(HTTPException):await self.ops.start_pilot(j['id'],self.pilot_spec())
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_items SET state='pending',user_id=200 WHERE job=?",(j['id'],))
  with self.assertRaises(HTTPException):await self.ops.pilot_preview(j['id'])
  self.assertFalse(self.c.calls);self.assertIsNone(self.ops.store.get('invite_pilot_'+str(j['id'])))
 async def test_personal_message_draft_is_deduplicated_and_sends_only_after_confirmation(self):
  self.group.creator=False
  j=await self.draft('message',destinations=[],users=['@agreed_user','AGREED_USER','@alias_user'],text='Requested details',consent_note='The recipient requested these details')
  self.assertEqual(j['state'],'draft');self.assertEqual(len(j['items']),1)
  self.assertEqual(j['items'][0]['chat_id'],200);self.assertEqual(j['items'][0]['user_id'],200)
  await self.ops.state();self.assertFalse(self.c.calls)
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='start'))
  await self.start(j);await self.ops.tick()
  detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['items'][0]['state'],'sent')
  self.assertEqual(detail['items'][0]['message_ids'],[700])
  self.assertEqual(detail['items'][0]['links'],['https://t.me/agreed_user'])
  self.assertEqual(len(self.c.calls),1);self.assertIsInstance(self.c.calls[0],functions.messages.SendMessageRequest)
  self.assertEqual(self.c.calls[0].message,'Requested details')
 async def test_personal_message_rejects_wrong_shape_and_missing_consent(self):
  values=dict(destinations=[],users=['@agreed_user'],text='Requested details',consent_note='Recipient asked for details')
  for changes in [dict(consent_note=''),dict(users=[]),dict(destinations=[-1000000000010]),dict(invite_url='https://t.me/managed_group'),dict(text=''),dict(media=['not-allowed']),dict(text='😀'*2050),dict(candidate_ids=[1],audience_run_id=1)]:
   with self.subTest(changes=changes):
    with self.assertRaises(HTTPException):await self.draft('message',**{**values,**changes})
  self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'));self.assertFalse(self.c.calls)
 async def test_personal_message_recipient_is_rechecked_before_send(self):
  j=await self.draft('message',destinations=[],users=['@agreed_user'],text='Requested details',consent_note='Recipient asked for details')
  await self.start(j)
  self.c.get_entity=AsyncMock(return_value=types.User(id=201,first_name='Different recipient',username='different'))
  await self.ops.tick()
  self.assertFalse(self.c.calls);self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'failed')
 async def test_manual_user_list_rejects_numeric_phone_and_id_before_resolution(self):
  self.c.get_entity=AsyncMock()
  for kind in ['invite','invite_message','message']:
   with self.subTest(kind=kind):
    extra=dict(text='Requested details',invite_url='https://t.me/managed_group') if kind=='invite_message' else dict(text='Requested details',destinations=[]) if kind=='message' else {}
    with self.assertRaises(HTTPException):await self.draft(kind,users=['1234567890'],consent_note='Recipient requested this action',**extra)
  self.c.get_entity.assert_not_awaited();self.assertFalse(self.c.calls)
 async def test_personal_message_pause_keeps_pending_without_sending(self):
  j=await self.draft('message',destinations=[],users=['@agreed_user'],text='Requested details',consent_note='Recipient asked for details')
  await self.start(j);await self.ops.control(j['id'],Control(action='pause'));await self.ops.tick()
  self.assertFalse(self.c.calls);self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'pending')
 async def test_nonmessage_still_requires_a_destination(self):
  with self.assertRaises(HTTPException):await self.draft(destinations=[])
  self.assertFalse(self.c.calls)
 async def test_membership_preflight_reads_without_inviting(self):
  r=await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertFalse(r['member']);self.assertFalse(self.c.calls)
  self.c.member=True;r=await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertTrue(r['member']);self.assertFalse(self.c.calls)
  self.assertEqual(r['user_id'],200);self.assertEqual(r['sender_id'],100)
 async def test_reconcile_paused_account_can_read_membership_without_enabling_writes(self):
  self.a.pause();before=self.ops.store.rows('SELECT * FROM op_accounts')
  result=await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertFalse(result['member']);self.assertTrue(result['outbound_paused'])
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_accounts'),before);self.assertFalse(self.c.calls)
  with self.assertRaises(HTTPException):self.ops.online(self.a.aid)
 async def test_reconcile_saved_item_uses_exact_original_peer_and_preserves_history(self):
  j=await self.draft('invite');item=j['items'][0];self.a.pause();self.c.member=True
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='uncertain',error='Preserved historical outcome' WHERE id=?",(item['id'],))
  original=self.c.get_entity;refs=[]
  async def entity(ref):
   refs.append(ref)
   if isinstance(ref,str):return types.User(id=999,username='agreed_user',access_hash=999)
   return await original(ref)
  self.c.get_entity=entity
  before={table:self.ops.store.rows('SELECT * FROM '+table) for table in ['op_accounts','op_jobs','op_items','op_invited','settings']}
  result=await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual((result['sender_id'],result['target'],result['user_id']),(100,-1000000000010,200))
  self.assertTrue(result['member']);self.assertEqual(result['history_state'],'uncertain');self.assertFalse(result['uncertain'])
  self.assertEqual(result['attribution'],'current_membership_only');self.assertFalse(result['history_changed']);self.assertFalse(result['invitation_requested'])
  self.assertTrue(any(isinstance(ref,types.InputPeerUser) and ref.user_id==200 and ref.access_hash==1 for ref in refs))
  self.assertFalse(any(isinstance(ref,str) for ref in refs));self.assertFalse(self.c.calls)
  for table,rows in before.items():self.assertEqual(self.ops.store.rows('SELECT * FROM '+table),rows)
  self.assertFalse(set(result)&{'user_access_hash','access_hash','phone','text','spec'})
 async def test_reconcile_unknown_or_departed_membership_does_not_reclassify_historical_invite(self):
  j=await self.draft('invite');item=j['items'][0];self.a.pause()
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='invited' WHERE id=?",(item['id'],))
  self.c.get_permissions=AsyncMock(return_value=SimpleNamespace(participant=object()))
  result=await self.ops.item_membership(j['id'],item['id'])
  self.assertIsNone(result['member']);self.assertTrue(result['uncertain']);self.assertTrue(result['uncertainty']);self.assertEqual(result['history_state'],'invited')
  self.c.get_permissions=AsyncMock(side_effect=errors.UserNotParticipantError(None))
  result=await self.ops.item_membership(j['id'],item['id'])
  self.assertFalse(result['member']);self.assertFalse(result['uncertain']);self.assertEqual(result['history_state'],'invited')
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'invited');self.assertFalse(self.c.calls)
 async def test_reconcile_read_rejects_cooldown_rights_changed_sender_or_wrong_item_without_write(self):
  j=await self.draft('invite');item=j['items'][0];self.a.pause()
  with self.assertRaises(HTTPException) as missing:await self.ops.item_membership(j['id']+1,item['id'])
  self.assertEqual(missing.exception.status_code,404)
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=? WHERE id=?',(time.time()+60,self.a.aid))
  with self.assertRaises(HTTPException) as cooldown:await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(cooldown.exception.status_code,429);self.clear_wait();self.group.creator=False
  with self.assertRaises(HTTPException) as rights:await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(rights.exception.status_code,403);self.group.creator=True
  original=self.c.get_permissions
  async def replaced(entity,user):
   value=await original(entity,user)
   self.a.client=Client(self.group)
   return value
  self.c.member=True;self.c.get_permissions=replaced
  with self.assertRaises(HTTPException) as changed:await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(changed.exception.status_code,409);self.assertFalse(self.c.calls);self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
 async def test_reconcile_read_flood_wait_persists_maximum_cooldown_without_history_or_enable(self):
  j=await self.draft('invite');item=j['items'][0];self.a.pause();before=self.ops.store.rows('SELECT * FROM op_items')
  async def limited(entity,user):
   with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=? WHERE id=?',(time.time()+120,self.a.aid))
   raise errors.FloodWaitError(None,capture=30)
  self.c.get_permissions=limited
  with self.assertRaises(HTTPException) as flood:await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(flood.exception.status_code,429);self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],time.time()+115)
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before);self.assertFalse(self.c.calls)
 async def test_reconcile_new_read_peer_flood_stops_enabled_sender_and_preserves_item_history(self):
  j=await self.draft('invite');item=j['items'][0]
  post=await self.draft('post',request_key='queued-post-before-read-peer-flood');await self.start(post)
  before=self.ops.store.rows('SELECT * FROM op_items');self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],1)
  self.c.get_permissions=AsyncMock(side_effect=errors.PeerFloodError(None))
  with self.assertRaises(HTTPException) as blocked:await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(blocked.exception.status_code,409);self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
  self.assertEqual((await self.ops.detail(post['id']))['state'],'paused');self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before)
  await self.ops.tick();self.assertFalse(self.c.calls)
 async def test_reconcile_revoked_read_session_pauses_sender_and_returns_human_error(self):
  for exception in [errors.SessionRevokedError,errors.AuthKeyUnregisteredError]:
   with self.subTest(error=exception.__name__):
    self.a.client=self.c;await self.a.ready()
    # Reconnection preserves the first subcase's protective stop. Reset only
    # this synthetic branch through the explicit owner-confirmed restore API.
    if not self.ops.account_row(self.a.aid)['enabled']:
     await self.ops.resume_secondary_outbound(self.a.aid,ResumeOutbound(confirmed=True,telegram_id=100,reason='Explicit reset between synthetic revoked-session branches'))
    j=await self.draft('invite',request_key='revoked-read-key-'+exception.__name__);item=j['items'][0]
    before=self.ops.store.rows('SELECT * FROM op_items');self.c.get_permissions=AsyncMock(side_effect=exception(None))
    with self.assertRaises(HTTPException) as blocked:await self.ops.item_membership(j['id'],item['id'])
    self.assertEqual(blocked.exception.status_code,409);self.assertIn('сесси',blocked.exception.detail.lower())
    self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before);self.assertFalse(self.c.calls)
 async def test_reconcile_read_peer_flood_is_not_a_success_or_automatic_resume(self):
  j=await self.draft('invite');item=j['items'][0];self.a.pause();before=self.ops.store.rows('SELECT * FROM op_items')
  self.c.get_permissions=AsyncMock(side_effect=errors.PeerFloodError(None))
  with self.assertRaises(HTTPException):await self.ops.item_membership(j['id'],item['id'])
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0);self.assertEqual(self.ops.store.rows('SELECT * FROM op_items'),before);self.assertFalse(self.c.calls)
 async def test_membership_checks_actual_participant_types(self):
  at=datetime.now(timezone.utc)
  cases=[
   (types.ChannelParticipant(user_id=200,date=at),True),
   (types.ChannelParticipantSelf(user_id=200,inviter_id=100,date=at),True),
   (types.ChannelParticipantAdmin(user_id=200,promoted_by=100,date=at,admin_rights=types.ChatAdminRights()),True),
   (types.ChannelParticipantCreator(user_id=200,admin_rights=types.ChatAdminRights()),True),
   (types.ChatParticipant(user_id=200,inviter_id=100,date=at),True),
   (types.ChatParticipantAdmin(user_id=200,inviter_id=100,date=at),True),
   (types.ChatParticipantCreator(user_id=200),True),
   (types.ChannelParticipantLeft(peer=types.PeerUser(200)),False),
   (types.ChannelParticipantBanned(peer=types.PeerUser(200),kicked_by=100,date=at,banned_rights=types.ChatBannedRights(until_date=at),left=True),False),
   (types.ChannelParticipantBanned(peer=types.PeerUser(200),kicked_by=100,date=at,banned_rights=types.ChatBannedRights(until_date=at,view_messages=True)),False),
   (types.ChannelParticipantBanned(peer=types.PeerUser(200),kicked_by=100,date=at,banned_rights=types.ChatBannedRights(until_date=at,send_messages=True)),True),
  ]
  for participant,expected in cases:
   with self.subTest(participant=type(participant).__name__,expected=expected):
    self.c.get_permissions=AsyncMock(return_value=ParticipantPermissions(participant,False))
    result=await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
    self.assertIs(result['member'],expected)
  self.assertFalse(self.c.calls)
 async def test_invite_unconfirmed_membership_has_three_bounded_read_checks_and_one_invite(self):
  j=await self.draft('invite');await self.start(j)
  self.c.get_permissions=AsyncMock(side_effect=errors.UserNotParticipantError(None))
  with patch('operations.asyncio.sleep',new_callable=AsyncMock) as sleep:
   await self.ops.tick()
  detail=await self.ops.detail(j['id']);item=detail['items'][0]
  self.assertEqual(item['state'],'uncertain');self.assertEqual(detail['state'],'paused')
  self.assertEqual(self.c.get_permissions.await_count,4)
  self.assertEqual(sleep.await_count,2)
  self.assertEqual(len(self.c.calls),1);self.assertIsInstance(self.c.calls[0],functions.channels.InviteToChannelRequest)
  self.assertEqual(len(item['diagnosis']['verification']),3)
  self.assertFalse(item['diagnosis']['retry_after_change'])
  self.assertEqual(item['message_ids'],[]);self.assertIsNone(item['invite_link'])
 async def test_manual_invite_keeps_input_peer_across_a_cold_entity_cache(self):
  j=await self.draft('invite',scheduled_at=datetime.now(timezone.utc)+timedelta(minutes=5))
  self.assertEqual(self.ops.store.rows('SELECT user_access_hash FROM op_items')[0]['user_access_hash'],1)
  original=self.c.get_entity;resolved=[]
  async def cold(ref):
   resolved.append(ref)
   if isinstance(ref,int) and ref>0:raise ValueError('No cached input peer')
   return await original(ref)
  self.c.get_entity=cold
  await self.start(j)
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET not_before=0 WHERE id=?',(j['id'],))
  await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'invited');self.assertEqual(len(self.c.calls),1)
  self.assertTrue(any(isinstance(ref,types.InputPeerUser) and ref.user_id==200 and ref.access_hash==1 for ref in resolved))
 async def test_legacy_invite_resolves_saved_username_only_for_the_same_numeric_user(self):
  j=await self.draft('invite')
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET user_access_hash=NULL WHERE job=?',(j['id'],))
  original=self.c.get_entity;resolved=[]
  async def cold(ref):
   resolved.append(ref)
   if ref==200:raise ValueError('No cached input peer')
   return await original(ref)
  self.c.get_entity=cold;await self.start(j);await self.ops.tick()
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'invited')
  self.assertIn('agreed_user',resolved);self.assertEqual(len(self.c.calls),1)
 async def test_legacy_reassigned_username_is_preflight_failure_without_invite_marker(self):
  j=await self.draft('invite')
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET user_access_hash=NULL WHERE job=?',(j['id'],))
  original=self.c.get_entity
  async def reassigned(ref):
   if ref==200:raise ValueError('No cached input peer')
   if ref=='agreed_user':return types.User(id=201,username='agreed_user',access_hash=2)
   return await original(ref)
  self.c.get_entity=reassigned;await self.start(j);await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertEqual(item['diagnosis']['code'],'invite_preflight_failed')
  self.assertFalse(item['diagnosis']['invitation_requested']);self.assertFalse(self.c.calls)
  self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_invite_preflight_errors_never_become_an_attempted_unknown_pair(self):
  for number,failure in enumerate([ValueError('No cached input peer'),TimeoutError(),errors.UserPrivacyRestrictedError(None)]):
   with self.subTest(failure=type(failure).__name__):
    self.clear_wait()
    j=await self.draft('invite',request_key='preflight-failure-key-'+str(number));await self.start(j)
    self.c.get_permissions=AsyncMock(side_effect=failure);await self.ops.tick()
    item=(await self.ops.detail(j['id']))['items'][0]
    self.assertEqual(item['state'],'failed');self.assertEqual(item['diagnosis']['source'],'invite_preflight')
    self.assertFalse(item['diagnosis']['invitation_requested']);self.assertFalse(self.c.calls)
    self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'));self.clear_wait()
 async def test_timeout_after_invite_request_stays_uncertain_and_cannot_repeat(self):
  j=await self.draft('invite');await self.start(j);self.c.failure=TimeoutError();await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'uncertain');self.assertTrue(item['diagnosis']['invitation_requested'])
  self.assertEqual(len(self.c.calls),1);self.assertEqual(self.ops.store.rows('SELECT state FROM op_invited'),[{'state':'uncertain'}])
  self.clear_wait();await self.ops.tick();self.assertEqual(len(self.c.calls),1)
  with self.assertRaises(HTTPException):await self.start(j)
 async def test_unrecognised_membership_before_invite_does_not_poison_the_pair(self):
  j=await self.draft('invite');await self.start(j)
  self.c.get_permissions=AsyncMock(return_value=ParticipantPermissions(SimpleNamespace(),False))
  await self.ops.tick();item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertEqual(item['diagnosis']['code'],'invite_preflight_failed')
  self.assertFalse(item['diagnosis']['invitation_requested']);self.assertFalse(self.c.calls)
  self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_membership_preflight_rechecks_rights_and_rejects_numeric_user(self):
  self.c.get_entity=AsyncMock()
  with self.assertRaises(HTTPException):await self.ops.membership(self.a.aid,-1000000000010,'12345')
  self.c.get_entity.assert_not_awaited()
  self.c.get_entity=Client(self.group).get_entity;self.group.creator=False
  with self.assertRaises(HTTPException):await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertFalse(self.c.calls)
 async def test_membership_unknown_type_is_not_reported_as_member(self):
  self.c.get_permissions=AsyncMock(return_value=ParticipantPermissions(SimpleNamespace(),False))
  with self.assertRaises(HTTPException) as raised:await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertEqual(raised.exception.status_code,502);self.assertFalse(self.c.calls)
 async def test_membership_contact_flags_are_read_only_optional_diagnostics(self):
  original=self.c.get_entity
  for contact,mutual in [(None,None),(False,False),(True,False),(True,True)]:
   with self.subTest(contact=contact,mutual=mutual):
    async def resolve(ref):
     user=await original(ref)
     if isinstance(user,types.User):user.contact=contact;user.mutual_contact=mutual
     return user
    self.c.get_entity=resolve
    result=await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
    self.assertIs(result['sender_has_contact'],contact);self.assertIs(result['mutual_contact'],mutual)
  self.assertFalse(self.c.calls)
 async def test_membership_response_for_other_user_is_not_proof(self):
  self.c.get_permissions=AsyncMock(return_value=ParticipantPermissions(types.ChannelParticipant(user_id=999,date=datetime.now(timezone.utc)),False))
  with self.assertRaises(HTTPException) as raised:await self.ops.membership(self.a.aid,-1000000000010,'@agreed_user')
  self.assertEqual(raised.exception.status_code,502);self.assertFalse(self.c.calls)
 async def test_invite_left_participant_does_not_skip_actual_invite(self):
  j=await self.draft('invite');await self.start(j)
  self.c.get_permissions=AsyncMock(side_effect=[ParticipantPermissions(types.ChannelParticipantLeft(types.PeerUser(200)),False),ParticipantPermissions(types.ChannelParticipant(user_id=200,date=datetime.now(timezone.utc)),False)])
  await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'invited');self.assertEqual(len(self.c.calls),1)
  self.assertEqual(item['diagnosis']['verification'][0]['participant_type'],'ChannelParticipant')
 async def test_invite_eventual_membership_uses_one_invite_and_bounded_gets_with_safe_evidence(self):
  j=await self.draft('invite');await self.start(j)
  member=ParticipantPermissions(types.ChannelParticipant(user_id=200,date=datetime.now(timezone.utc)),False)
  self.c.get_permissions=AsyncMock(side_effect=[errors.UserNotParticipantError(None)]*3+[member])
  wait_for=asyncio.wait_for
  with patch('operations.asyncio.sleep',new_callable=AsyncMock),patch('operations.asyncio.wait_for',wraps=wait_for) as waits:
   await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0];diagnosis=item['diagnosis']
  self.assertEqual(item['state'],'invited');self.assertEqual(len(self.c.calls),1)
  self.assertIsInstance(self.c.calls[0],functions.channels.InviteToChannelRequest)
  self.assertEqual(self.c.get_permissions.await_count,4)
  self.assertEqual([call.kwargs['timeout'] for call in waits.call_args_list],[90,10,10,10])
  self.assertEqual([v['member'] for v in diagnosis['verification']],[False,False,True])
  self.assertEqual(diagnosis['response']['response_type'],'InvitedUsers')
  self.assertEqual(diagnosis['response']['updates_type'],'Updates')
  self.assertEqual(diagnosis['response']['missing_invitees'],[])
  self.assertNotIn('access_hash',json.dumps(diagnosis));self.assertNotIn('session',json.dumps(diagnosis))
 async def test_invite_privacy_response_stays_restricted_without_post_checks_or_message(self):
  self.c.missing=True;self.c.missing_flags={'premium_required_for_pm':True}
  j=await self.draft('invite');await self.start(j)
  permissions=self.c.get_permissions;self.c.get_permissions=AsyncMock(side_effect=permissions)
  await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'restricted');self.assertEqual(item['diagnosis']['code'],'privacy_restricted')
  self.assertEqual(self.c.get_permissions.await_count,1);self.assertEqual(len(self.c.calls),1)
  self.assertIsInstance(self.c.calls[0],functions.channels.InviteToChannelRequest)
  self.assertEqual(item['diagnosis']['verification'],[])
  self.assertEqual(item['diagnosis']['response']['missing_invitees'],[{'user_id':200,'premium_would_allow_invite':False,'premium_required_for_pm':True}])
 async def test_invite_post_check_flood_wait_never_requeues_already_attempted_invite(self):
  j=await self.draft('invite');await self.start(j)
  self.c.get_permissions=AsyncMock(side_effect=[errors.UserNotParticipantError(None),errors.FloodWaitError(None,capture=30)])
  await self.ops.tick()
  detail=await self.ops.detail(j['id']);item=detail['items'][0]
  self.assertEqual(item['state'],'uncertain');self.assertEqual(detail['state'],'paused')
  self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+25)
  self.assertEqual(item['diagnosis']['verification_error']['code'],'FloodWaitError')
  self.assertEqual(item['diagnosis']['response']['response_type'],'InvitedUsers')
  self.clear_wait();await self.ops.tick();self.assertEqual(len(self.c.calls),1)
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
 async def test_basic_group_invite_uses_one_add_request_and_three_reads_without_dm(self):
  self.group=types.Chat(id=10,title='Managed small group',photo=types.ChatPhotoEmpty(),participants_count=1,date=datetime.now(timezone.utc),version=1,creator=True)
  self.c.group=self.group
  j=await self.draft('invite',destinations=[-10]);await self.start(j)
  self.c.get_permissions=AsyncMock(side_effect=errors.UserNotParticipantError(None))
  with patch('operations.asyncio.sleep',new_callable=AsyncMock):await self.ops.tick()
  detail=await self.ops.detail(j['id'])
  self.assertEqual(detail['items'][0]['state'],'uncertain');self.assertEqual(detail['state'],'paused')
  self.assertEqual(self.c.get_permissions.await_count,4);self.assertEqual(len(self.c.calls),1)
  self.assertIsInstance(self.c.calls[0],functions.messages.AddChatUserRequest);self.assertEqual(self.c.calls[0].fwd_limit,0)
 async def test_invite_evidence_excludes_full_entities_and_messages(self):
  from operations import invite_response_evidence
  response=types.messages.InvitedUsers(
   updates=types.Updates(updates=[types.UpdateNewMessage(message=types.Message(id=1,peer_id=types.PeerUser(200),message='private-message-fixture'),pts=1,pts_count=1)],users=[types.User(id=200,access_hash=9999912345,phone='79000000000',first_name='private-user-fixture')],chats=[],date=datetime.now(timezone.utc),seq=0),missing_invitees=[])
  evidence=invite_response_evidence(response)
  self.assertEqual(evidence['update_types'],['UpdateNewMessage'])
  encoded=json.dumps(evidence)
  for private in ['private-message-fixture','private-user-fixture','9999912345','79000000000','access_hash']:
   self.assertNotIn(private,encoded)
 async def test_invite_sender_change_during_post_check_keeps_uncertain(self):
  j=await self.draft('invite');await self.start(j)
  async def checked(entity,user):
   if not self.c.calls:raise errors.UserNotParticipantError(None)
   self.a.client=Client(self.group)
   return ParticipantPermissions(types.ChannelParticipant(user_id=200,date=datetime.now(timezone.utc)),False)
  self.c.get_permissions=checked;await self.ops.tick()
  detail=await self.ops.detail(j['id']);self.assertEqual(detail['items'][0]['state'],'uncertain')
  self.assertEqual(detail['state'],'paused');self.assertEqual(len(self.c.calls),1)
 async def paused_mixed_invite(self):
  original=self.c.get_entity
  async def resolve(ref):
   ids={'agreed_two':201,'agreed_three':202}
   uid=ids.get(ref,ref if ref in (201,202) else None)
   return types.User(id=uid,username='agreed_two' if uid==201 else 'agreed_three',first_name='Consenting') if uid else await original(ref)
  self.c.get_entity=resolve
  j=await self.draft('invite',users=['agreed_user','agreed_two','agreed_three'])
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?",(j['id'],))
   for item,state in zip(j['items'],['uncertain','restricted','pending']):
    if state=='pending':continue
    self.ops.store.db.execute("UPDATE op_items SET state=?,attempted_at=?,finished_at=?,error=?,diagnosis=? WHERE id=?",(state,'2026-10-15T01:00:00+00:00','2026-10-15T01:00:01+00:00',state,json.dumps({'original':state}),item['id']))
    self.ops.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?)',(item['chat_id'],item['user_id'],state,j['id']))
  return await self.ops.detail(j['id'])
 async def test_continue_pending_keeps_terminal_evidence_and_only_invites_pending(self):
  j=await self.paused_mixed_invite()
  account_before=self.ops.account_row(self.a.aid);markers_before=self.ops.store.rows('SELECT * FROM op_invited ORDER BY user_id')
  items_before=self.ops.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(j['id'],))
  terminal_before=self.ops.store.rows("SELECT * FROM op_items WHERE state IN ('uncertain','restricted') ORDER BY id")
  for action in ['start','resume']:
   with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action=action,confirmed=True))
  continued=await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  self.assertEqual(continued['state'],'queued');self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(j['id'],)),items_before)
  self.assertEqual(self.ops.account_row(self.a.aid),account_before)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_invited ORDER BY user_id'),markers_before)
  self.assertEqual(continued['spec'],j['spec']);self.assertFalse(self.c.calls)
  await self.ops.tick();final=await self.ops.detail(j['id'])
  self.assertEqual([i['state'] for i in final['items']],['uncertain','restricted','invited'])
  self.assertEqual(self.ops.store.rows("SELECT * FROM op_items WHERE state IN ('uncertain','restricted') ORDER BY id"),terminal_before);self.assertEqual(len(self.c.calls),1)
  self.assertEqual(self.c.calls[0].users[0].id,202)
 async def test_continue_pending_requires_confirmation_paused_invite_and_no_sending(self):
  j=await self.paused_mixed_invite()
  with self.assertRaises(HTTPException) as raised:await self.ops.control(j['id'],Control(action='continue_pending'))
  self.assertEqual(raised.exception.status_code,400)
  for state,kind in [('draft','invite'),('queued','invite'),('completed','invite'),('paused','post'),('paused','invite_message')]:
   with self.subTest(state=state,kind=kind):
    with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET state=?,kind=? WHERE id=?',(state,kind,j['id']))
    with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_jobs SET state='paused',kind='invite' WHERE id=?",(j['id'],))
   self.ops.store.db.execute("UPDATE op_items SET state='sending' WHERE id=?",(j['items'][1]['id'],))
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_items SET state='restricted' WHERE id=?",(j['items'][1]['id'],))
   self.ops.store.db.execute("UPDATE op_items SET state='cancelled' WHERE state='pending'")
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  self.assertFalse(self.c.calls)
 async def test_continue_pending_respects_cooldown_and_account_identity(self):
  j=await self.paused_mixed_invite();until=datetime.now(timezone.utc).timestamp()+300
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=?',(until,))
  with self.assertRaises(HTTPException) as raised:await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  self.assertEqual(raised.exception.status_code,429);self.assertEqual(self.ops.account_row(self.a.aid)['not_before'],until)
  self.clear_wait();self.a.me=types.User(id=999,username='different_sender')
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  self.assertEqual((await self.ops.detail(j['id']))['state'],'paused');self.assertFalse(self.c.calls)
 async def test_continue_pending_preserves_scheduled_time_and_limits(self):
  j=await self.paused_mixed_invite();until=datetime.now(timezone.utc).timestamp()+300
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET not_before=? WHERE id=?',(until,j['id']))
  result=await self.ops.control(j['id'],Control(action='continue_pending',confirmed=True))
  self.assertEqual(result['state'],'scheduled');self.assertEqual(result['not_before'],until)
  self.assertEqual(result['spec'],j['spec']);await self.ops.tick();self.assertFalse(self.c.calls)
 async def test_contact_option_defaults_off_and_is_only_for_direct_invites(self):
  from pydantic import ValidationError
  for kind in ['post','message','invite_message','link']:
   with self.subTest(kind=kind):
    with self.assertRaises(ValidationError):await self.draft(kind,add_contacts_before_invite=True)
  j=await self.draft('invite');self.assertNotIn('add_contacts_before_invite',j['spec'])
  await self.start(j);await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertIsInstance(self.c.calls[0],functions.channels.InviteToChannelRequest)
 async def test_opt_in_contact_is_added_once_verified_fresh_then_invited_without_pm(self):
  j=await self.draft('invite',add_contacts_before_invite=True);self.assertFalse(self.c.calls)
  await self.start(j);await self.ops.tick()
  self.assertEqual([type(r) for r in self.c.calls],[functions.contacts.AddContactRequest,functions.users.GetUsersRequest,functions.channels.InviteToChannelRequest])
  request=self.c.calls[0];self.assertEqual(request.id.id,200);self.assertEqual(request.phone,'');self.assertIs(request.add_phone_privacy_exception,False)
  item=(await self.ops.detail(j['id']))['items'][0];contact=item['diagnosis']['contact_preparation']
  self.assertEqual(item['state'],'invited');self.assertEqual(contact['status'],'added_verified')
  self.assertIs(contact['sender_has_contact'],True);self.assertIs(contact['mutual_contact'],False)
  self.assertIs(contact['phone_privacy_exception_added'],False);self.assertTrue(item['diagnosis']['invitation_requested'])
  self.assertNotIn('access_hash',json.dumps(contact));self.assertEqual(item['message_ids'],[])
 async def test_already_saved_contact_does_not_create_another_contact_write(self):
  self.c.contact=True;j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j);await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertIsInstance(self.c.calls[0],functions.channels.InviteToChannelRequest)
  contact=(await self.ops.detail(j['id']))['items'][0]['diagnosis']['contact_preparation']
  self.assertEqual(contact['status'],'already_contact');self.assertFalse(contact['request_sent'])
 async def test_contact_endpoint_only_saves_one_confirmed_contact_and_no_job(self):
  result=await self.ops.add_contact(self.a.aid,AddContact(username='@agreed_user',confirmed=True))
  self.assertEqual([type(r) for r in self.c.calls],[functions.contacts.AddContactRequest,functions.users.GetUsersRequest])
  self.assertTrue(result['sender_has_contact']);self.assertFalse(result['mutual_contact'])
  self.assertFalse(result['invitation_requested']);self.assertFalse(result['message_sent']);self.assertFalse(result['phone_privacy_exception_added'])
  self.assertEqual(result['contact_preparation']['status'],'added_verified');self.assertFalse(self.ops.store.rows('SELECT * FROM op_jobs'))
  self.assertEqual(self.ops.store.get('last_contact_preparation_'+self.a.aid)['contact_preparation'],result['contact_preparation'])
  route=next(route for route in self.ops.router.routes if route.path.endswith('/accounts/{aid}/contacts/add'))
  self.assertEqual(route.methods,{'POST'})
 async def test_contact_endpoint_requires_confirmation_one_username_and_idle_sender(self):
  from pydantic import ValidationError
  for values in [dict(username='agreed_user'),dict(username='agreed_user',confirmed=False),dict(username='agreed_user',confirmed=True,phone='79000000000')]:
   with self.assertRaises(ValidationError):AddContact(**values)
  for name in ['123456789','agreed_user another_user','https://t.me/agreed_user']:
   with self.assertRaises(HTTPException):await self.ops.add_contact(self.a.aid,AddContact(username=name,confirmed=True))
  j=await self.draft('invite')
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='sending' WHERE job=?",(j['id'],))
  with self.assertRaises(HTTPException):await self.ops.add_contact(self.a.aid,AddContact(username='agreed_user',confirmed=True))
  self.assertFalse(self.c.calls)
 async def test_contact_endpoint_preserves_existing_cooldown(self):
  until=datetime.now(timezone.utc).timestamp()+300
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=?',(until,))
  with self.assertRaises(HTTPException) as raised:await self.ops.add_contact(self.a.aid,AddContact(username='agreed_user',confirmed=True))
  self.assertEqual(raised.exception.status_code,429);self.assertEqual(self.ops.account_row(self.a.aid)['not_before'],until);self.assertFalse(self.c.calls)
 async def test_optional_contact_write_failure_still_allows_one_direct_invite(self):
  original=Client.__call__
  async def refused(client,request):
   if isinstance(request,functions.contacts.AddContactRequest):
    client.calls.append(request);raise errors.UserPrivacyRestrictedError(None)
   return await original(client,request)
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  with patch.object(Client,'__call__',refused):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'invited');self.assertEqual(item['diagnosis']['contact_preparation']['status'],'not_verified')
  self.assertTrue(item['diagnosis']['invitation_requested']);self.assertIsNone(item['diagnosis']['contact_preparation']['sender_has_contact'])
  self.assertEqual([type(r) for r in self.c.calls],[functions.contacts.AddContactRequest,functions.channels.InviteToChannelRequest])
 async def test_contact_flood_wait_uses_cooldown_without_an_invite_attempt(self):
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  self.c.failure=errors.FloodWaitError(None,capture=30);await self.ops.tick()
  detail=await self.ops.detail(j['id']);item=detail['items'][0]
  self.assertEqual(detail['state'],'paused');self.assertEqual(item['state'],'pending')
  self.assertEqual(item['diagnosis']['code'],'contact_flood_wait');self.assertFalse(item['diagnosis']['invitation_requested'])
  self.assertEqual(item['diagnosis']['contact_preparation']['status'],'rate_limited')
  self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+25)
  self.assertEqual(len(self.c.calls),1);self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_contact_endpoint_flood_wait_records_no_invite_and_rate_limit(self):
  self.c.failure=errors.FloodWaitError(None,capture=30)
  with self.assertRaises(HTTPException) as raised:await self.ops.add_contact(self.a.aid,AddContact(username='agreed_user',confirmed=True))
  self.assertEqual(raised.exception.status_code,429);self.assertEqual(len(self.c.calls),1)
  self.assertEqual(self.ops.store.get('last_contact_preparation_'+self.a.aid)['contact_preparation']['status'],'rate_limited')
  self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+25)
 async def test_optional_contact_peer_flood_is_an_account_hard_stop(self):
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  self.c.failure=errors.PeerFloodError(None);await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'blocked');self.assertFalse(item['diagnosis']['invitation_requested'])
  self.assertFalse(self.ops.account_row(self.a.aid)['enabled']);self.assertEqual(len(self.c.calls),1)
  self.assertIsInstance(self.c.calls[0],functions.contacts.AddContactRequest)
  self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_unverified_optional_contact_does_not_block_direct_invite(self):
  original=Client.__call__
  async def unknown(client,request):
   if isinstance(request,functions.users.GetUsersRequest):
    client.calls.append(request);return [types.User(id=200,min=True,contact=True,mutual_contact=True)]
   return await original(client,request)
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  with patch.object(Client,'__call__',unknown):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'invited');self.assertEqual(len(self.c.calls),3)
  self.assertIsNone(item['diagnosis']['contact_preparation']['sender_has_contact']);self.assertIsNone(item['diagnosis']['contact_preparation']['mutual_contact'])
  self.assertTrue(item['diagnosis']['invitation_requested']);self.assertEqual(item['diagnosis']['contact_preparation']['status'],'not_verified')
 async def test_optional_contact_read_error_rechecks_rights_before_direct_invite(self):
  original=Client.__call__
  async def refused(client,request):
   if isinstance(request,functions.users.GetUsersRequest):
    client.calls.append(request);self.group.creator=False;raise ValueError('Contact read failed')
   return await original(client,request)
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  with patch.object(Client,'__call__',refused):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertFalse(item['diagnosis']['invitation_requested'])
  self.assertEqual(len(self.c.calls),2);self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_optional_contact_read_error_allows_one_direct_invite(self):
  original=Client.__call__
  async def refused(client,request):
   if isinstance(request,functions.users.GetUsersRequest):
    client.calls.append(request);raise ValueError('Contact read failed')
   return await original(client,request)
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  with patch.object(Client,'__call__',refused):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'invited');self.assertEqual(item['diagnosis']['contact_preparation']['status'],'not_verified')
  self.assertTrue(item['diagnosis']['invitation_requested'])
  self.assertEqual([type(r) for r in self.c.calls],[functions.contacts.AddContactRequest,functions.users.GetUsersRequest,functions.channels.InviteToChannelRequest])
 async def test_contact_save_does_not_claim_mutuality_or_override_invite_rpc_refusal(self):
  original=Client.__call__
  async def nonmutual(client,request):
   if isinstance(request,functions.channels.InviteToChannelRequest):
    client.calls.append(request);raise errors.UserNotMutualContactError(None)
   return await original(client,request)
  j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  with patch.object(Client,'__call__',nonmutual):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'restricted');self.assertEqual(item['diagnosis']['code'],'UserNotMutualContactError')
  self.assertIn('этого получателя',item['error'])
  self.assertEqual(item['diagnosis']['contact_preparation']['status'],'added_verified');self.assertFalse(item['diagnosis']['contact_preparation']['mutual_contact'])
  self.assertEqual(len(self.c.calls),3);self.assertTrue(item['diagnosis']['invitation_requested'])
 async def test_pause_after_contact_save_prevents_the_following_invite(self):
  original=Client.__call__;j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  async def paused(client,request):
   result=await original(client,request)
   if isinstance(request,functions.users.GetUsersRequest):await self.ops.control(j['id'],Control(action='pause'))
   return result
  with patch.object(Client,'__call__',paused):await self.ops.tick()
  detail=await self.ops.detail(j['id']);self.assertEqual(detail['state'],'paused');self.assertEqual(detail['items'][0]['state'],'pending')
  self.assertEqual(detail['items'][0]['diagnosis']['contact_preparation']['status'],'added_verified')
  self.assertFalse(detail['items'][0]['diagnosis']['invitation_requested']);self.assertEqual(len(self.c.calls),2)
 async def test_sender_change_after_contact_save_prevents_fresh_read_and_invite(self):
  original=Client.__call__;j=await self.draft('invite',add_contacts_before_invite=True);await self.start(j)
  async def changed(client,request):
   result=await original(client,request)
   if isinstance(request,functions.contacts.AddContactRequest):self.a.client=Client(self.group)
   return result
  with patch.object(Client,'__call__',changed):await self.ops.tick()
  item=(await self.ops.detail(j['id']))['items'][0]
  self.assertEqual(item['state'],'failed');self.assertFalse(item['diagnosis']['invitation_requested']);self.assertEqual(len(self.c.calls),1)
  self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))
 async def test_selected_restricted_retry_from_paused_parent_preserves_other_items(self):
  j=await self.paused_mixed_invite();restricted=j['items'][1]
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET diagnosis=? WHERE id=?',(json.dumps({'source':'rpc_error','code':'UserNotMutualContactError','retry_after_change':True}),restricted['id']))
  before=self.ops.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(j['id'],))
  retry=await self.ops.retry_invite(j['id'],self.retry_spec(item_ids=[restricted['id']]))
  self.assertEqual(retry['state'],'draft');self.assertEqual(len(retry['items']),1);self.assertEqual(retry['items'][0]['user_id'],201)
  self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(j['id'],)),before)
  self.assertEqual((await self.ops.detail(j['id']))['state'],'paused');self.assertFalse(self.c.calls)
 async def test_restricted_retry_from_queued_or_sending_parent_is_blocked(self):
  j=await self.paused_mixed_invite();restricted=j['items'][1]
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET diagnosis=? WHERE id=?',(json.dumps({'retry_after_change':True}),restricted['id']))
  for state,sending in [('queued',False),('scheduled',False),('paused',True)]:
   with self.subTest(state=state,sending=sending):
    with self.ops.store.db:
     self.ops.store.db.execute('UPDATE op_jobs SET state=? WHERE id=?',(state,j['id']))
     self.ops.store.db.execute("UPDATE op_items SET state=? WHERE id=?",('sending' if sending else 'pending',j['items'][2]['id']))
    with self.assertRaises(HTTPException):await self.ops.retry_invite(j['id'],self.retry_spec(item_ids=[restricted['id']]))
  self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_jobs')),1);self.assertFalse(self.c.calls)
 async def test_proxy_is_account_bound_masked_and_never_falls_back(self):
  proxy=ProxySpec(mode='socks5',address='8.8.8.8',port=1080,username='synthetic',password='secret-proxy-password')
  b=await self.ops.add_account(AccountSpec(label='Proxy fixture',authorized=True,proxy=proxy))
  self.assertEqual(self.ops.proxy_connection(b['id'])['proxy_type'],'socks5')
  self.assertIsNone(self.ops.proxy_connection(self.a.aid))
  self.assertNotIn('secret-proxy-password',json.dumps(await self.ops.state()))
  client=SimpleNamespace(connect=AsyncMock(side_effect=TimeoutError()),disconnect=AsyncMock(),is_connected=lambda:False)
  with patch('operations.ClientTelegramClient',return_value=client) as factory:
   with self.assertRaises(HTTPException):await self.ops.reconnect(b['id'])
  self.assertEqual(factory.call_args.kwargs['proxy']['addr'],'8.8.8.8')
  self.assertEqual(factory.call_count,1)
  self.assertIsNone(self.ops.accounts[b['id']].client)
 async def test_proxy_rejects_private_endpoints_and_active_queue_changes(self):
  from pydantic import ValidationError
  for host in ['127.0.0.1','169.254.169.254','10.0.0.1','::1','proxy.example.com']:
   with self.assertRaises(ValidationError):ProxySpec(mode='http',address=host)
  j=await self.draft();await self.start(j)
  with self.assertRaises(HTTPException):await self.ops.set_proxy(self.a.aid,ProxySpec())
  self.assertFalse(self.c.disconnect.await_count)
 async def test_reconnect_bounds_authorization_and_identity_checks_without_sending(self):
  real_timeout=asyncio.timeout
  async def stalled():await asyncio.Event().wait()
  for stalled_check in ('is_user_authorized','get_me'):
   with self.subTest(stalled_check=stalled_check):
    client=SimpleNamespace(connect=AsyncMock(),disconnect=AsyncMock(),is_connected=lambda:False,
                           is_user_authorized=AsyncMock(return_value=True),get_me=AsyncMock(return_value=types.User(id=100)))
    getattr(client,stalled_check).side_effect=stalled
    self.a.client=client;self.a.me=None;self.a.status='not_connected'
    with patch('operations.asyncio.timeout',side_effect=lambda seconds:real_timeout(min(seconds,0.01))):
     with self.assertRaises(HTTPException) as caught:
      await asyncio.wait_for(self.ops.reconnect(self.a.aid),0.2)
    self.assertEqual(caught.exception.status_code,409)
    client.disconnect.assert_awaited_once()
    self.assertIsNone(self.a.client);self.assertIsNone(self.a.me)
    self.assertFalse(self.ops.account_row(self.a.aid)['enabled'])
    self.assertFalse(self.c.calls)
 async def test_creation_and_read_never_send_idempotent(self):
  j=await self.draft();same=await self.draft();self.assertEqual(j['id'],same['id']);await self.ops.state();self.assertFalse(self.c.calls)
  with self.assertRaises(HTTPException):await self.draft(text='Different')
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='start'))
  await self.start(j);await self.ops.tick();self.clear_wait();await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertEqual((await self.ops.detail(j['id']))['items'][0]['links'],['https://t.me/managed_group/700'])
 async def test_account_auth_code_2fa_no_secret_persistence(self):
  b=await self.ops.add_account(AccountSpec(label='Second',authorized=True));a=self.ops.accounts[b['id']];a.client=Client(self.group)
  a.client.get_me.return_value=types.User(id=101,username='owned_second')
  a.client.is_user_authorized=AsyncMock(return_value=False)
  await a.code(Phone(phone='+70000000000'))
  a.client.sign_in.side_effect=errors.SessionPasswordNeededError(None)
  self.assertEqual(await a.confirm(Code(code='12345')),{'step':'password'})
  a.client.sign_in.side_effect=None;await a.password(Password(password='private-2fa'))
  dump='\n'.join(self.ops.store.db.iterdump())
  for secret in ('12345','private-2fa','private-code-hash','synthetic-app-hash','+70000000000'):self.assertNotIn(secret,dump)
  self.assertEqual(self.ops.account_row(a.aid)['telegram_id'],101)
 async def test_duplicate_and_changed_account_rejected(self):
  b=await self.ops.add_account(AccountSpec(label='Dup',authorized=True));a=self.ops.accounts[b['id']];a.client=Client(self.group)
  with self.assertRaises(HTTPException):await a.ready()
  self.c.get_me.return_value=types.User(id=999,username='different')
  with self.assertRaises(HTTPException):await self.a.ready()
  self.assertFalse(self.ops.account_row(self.a.aid)['enabled'])
 async def test_multiple_accounts_stay_bound(self):
  b=await self.ops.add_account(AccountSpec(label='Second',authorized=True));a=self.ops.accounts[b['id']];c=Client(self.group);c.get_me.return_value=types.User(id=101,username='second');a.client=c;await a.ready()
  j=await self.draft(account=a.aid);await self.start(j);await self.ops.tick();self.assertEqual(len(c.calls),1);self.assertEqual(self.c.calls,[])
 async def test_schedule_pause_resume_cancel(self):
  j=await self.draft(scheduled_at=datetime.now(timezone.utc)+timedelta(hours=1));await self.start(j);await self.ops.tick();self.assertFalse(self.c.calls)
  await self.ops.control(j['id'],Control(action='pause'));await self.ops.tick();self.assertFalse(self.c.calls)
  await self.ops.control(j['id'],Control(action='resume',confirmed=True));await self.ops.control(j['id'],Control(action='cancel'));await self.ops.tick()
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'cancelled')
 async def test_managed_and_external_permission_validation(self):
  self.group.creator=False
  with self.assertRaises(HTTPException):await self.draft()
  j=await self.draft(permission_note='Approved by the group administrator')
  await self.start(j);await self.ops.tick();self.assertEqual(len(self.c.calls),1)
 async def test_destination_picker_uses_dialog_flags_then_job_rechecks_live_rights(self):
  self.group.creator=False
  async def dialogs(**kwargs):
   yield SimpleNamespace(entity=types.User(id=200,first_name='Private'))
   yield SimpleNamespace(entity=self.group)
  self.c.iter_dialogs=dialogs;self.c.get_permissions=AsyncMock(side_effect=errors.UserNotParticipantError(None))
  result=await self.ops.destinations(self.a.aid)
  self.assertEqual((result['checked'],result['skipped'],len(result['destinations'])),(2,1,1))
  self.c.get_permissions.assert_not_awaited()
  with self.assertRaises(HTTPException):await self.draft(permission_note='Approved by group administrator')
  self.c.get_permissions.assert_awaited_once();self.assertFalse(self.c.calls)
 async def test_rights_rechecked_before_send(self):
  j=await self.draft();await self.start(j);self.group.creator=False;await self.ops.tick()
  self.assertFalse(self.c.calls);self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'failed')
 async def test_no_private_recipient_or_no_consent(self):
  with self.assertRaises(HTTPException):await self.draft(destinations=[200])
  with self.assertRaises(HTTPException):await self.draft('invite',consent_note='')
  self.group.creator=False
  with self.assertRaises(HTTPException):await self.draft('invite')
  self.assertFalse(self.c.calls)
 async def test_invite_verification_and_cross_account_dedup(self):
  j=await self.draft('invite');await self.start(j);await self.ops.tick();self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'invited')
  self.clear_wait();self.c.member=False
  k=await self.draft('invite',request_key='second-unique-key-01');await self.start(k);await self.ops.tick();self.assertEqual(len(self.c.calls),1)
  self.assertEqual((await self.ops.detail(k['id']))['items'][0]['state'],'duplicate_skipped')
 async def test_already_member_does_not_invite(self):
  self.c.member=True;j=await self.draft('invite');await self.start(j);await self.ops.tick();self.assertFalse(self.c.calls)
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'already_member')
 async def test_new_consented_job_after_already_member_checks_membership_again(self):
  self.c.member=True;j=await self.draft('invite');await self.start(j);await self.ops.tick()
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'already_member')
  self.clear_wait();self.c.member=False
  k=await self.draft('invite',request_key='new-consent-after-leaving');await self.start(k);await self.ops.tick()
  self.assertEqual((await self.ops.detail(k['id']))['items'][0]['state'],'invited')
  self.assertEqual(len(self.c.calls),1)
  self.clear_wait();self.c.member=False
  m=await self.draft('invite',request_key='must-not-invite-again');await self.start(m);await self.ops.tick()
  self.assertEqual((await self.ops.detail(m['id']))['items'][0]['state'],'duplicate_skipped')
  self.assertEqual(len(self.c.calls),1)
 async def test_missing_invitee_not_reported_success(self):
  self.c.missing=True;j=await self.draft('invite');await self.start(j);await self.ops.tick()
  result=await self.ops.detail(j['id'])
  self.assertEqual(result['items'][0]['state'],'restricted')
  self.assertEqual(result['items'][0]['diagnosis']['code'],'privacy_restricted')
  self.assertTrue(result['retryable'])
 async def failed_invite(self):
  self.c.missing=True;j=await self.draft('invite');await self.start(j);await self.ops.tick();self.clear_wait()
  return await self.ops.detail(j['id'])
 def retry_spec(self,**kwargs):
  return RetryInvite(confirmed=True,request_key='manual-retry-key-0001',reason='Recipient changed group invitation permissions',**kwargs)
 async def test_manual_invite_retry_is_new_draft_idempotent_and_keeps_history(self):
  old=await self.failed_invite();before=self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(old['id'],))
  retry=await self.ops.retry_invite(old['id'],self.retry_spec(item_ids=[old['items'][0]['id']]))
  repeated=await self.ops.retry_invite(old['id'],self.retry_spec(item_ids=[old['items'][0]['id']]))
  self.assertEqual(retry['id'],repeated['id']);self.assertNotEqual(retry['id'],old['id'])
  self.assertEqual(retry['state'],'draft');self.assertEqual(retry['retry_of_job'],old['id'])
  self.assertEqual(retry['account'],old['account']);self.assertEqual(retry['telegram_id'],old['telegram_id'])
  self.assertEqual(retry['items'][0]['user_id'],old['items'][0]['user_id'])
  self.assertEqual(retry['items'][0]['chat_id'],old['items'][0]['chat_id'])
  self.assertEqual(retry['spec']['consent_note'],old['spec']['consent_note'])
  self.assertEqual(before,self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(old['id'],)))
  self.assertEqual(len(self.c.calls),1)
  with self.assertRaises(HTTPException):await self.ops.control(retry['id'],Control(action='start'))
  self.c.missing=False;await self.start(retry);await self.ops.tick()
  self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'invited')
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'invited','job':retry['id']}])
  self.assertEqual(len(self.c.calls),2);self.assertEqual(before,self.ops.store.rows('SELECT * FROM op_items WHERE job=?',(old['id'],)))
  self.assertEqual((await self.ops.detail(old['id']))['retry_jobs'],[retry['id']])
  self.assertFalse((await self.ops.detail(old['id']))['retryable'])
  self.clear_wait();self.c.member=False
  ordinary=await self.draft('invite',request_key='ordinary-after-retry-success');await self.start(ordinary);await self.ops.tick()
  self.assertEqual((await self.ops.detail(ordinary['id']))['items'][0]['state'],'duplicate_skipped')
  self.assertEqual(len(self.c.calls),2)
 async def test_legacy_restricted_retry_recovers_the_same_user_after_cache_loss(self):
  old=await self.failed_invite()
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET user_access_hash=NULL WHERE job=?',(old['id'],))
  original=self.c.get_entity
  async def cold(ref):
   if ref==200:raise ValueError('No cached input peer')
   return await original(ref)
  self.c.get_entity=cold
  retry=await self.ops.retry_invite(old['id'],self.retry_spec());self.c.missing=False
  await self.start(retry);await self.ops.tick()
  self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'invited')
  self.assertEqual(len(self.c.calls),2)
 async def test_old_invite_unknown_diagnosis_is_not_invented_and_can_be_manually_retested(self):
  old=await self.failed_invite()
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET diagnosis='{}' WHERE job=?",(old['id'],))
  detail=await self.ops.detail(old['id']);self.assertEqual(detail['items'][0]['diagnosis'],{})
  self.assertIn('не была сохранена',detail['items'][0]['retry_reason'])
  retry=await self.ops.retry_invite(old['id'],self.retry_spec());self.assertEqual(retry['state'],'draft')
  self.assertEqual((await self.ops.detail(old['id']))['items'][0]['diagnosis'],{})
  self.assertEqual(len(self.c.calls),1)
 async def test_manual_retry_unchanged_privacy_records_another_refusal_without_auto_retry(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  await self.start(retry);await self.ops.tick();self.clear_wait();await self.ops.tick()
  detail=await self.ops.detail(retry['id']);self.assertEqual(detail['items'][0]['state'],'restricted')
  self.assertTrue(detail['retryable']);self.assertFalse((await self.ops.detail(old['id']))['retryable'])
  self.assertEqual(len(self.c.calls),2)
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'restricted','job':retry['id']}])
 async def test_retry_rejects_concurrent_duplicate_and_request_key_reuse_for_other_body(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],RetryInvite(confirmed=True,request_key='another-manual-retry-key',reason='Changed permissions again'))
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],RetryInvite(confirmed=True,request_key='manual-retry-key-0001',reason='Different reason for this request'))
  self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_jobs')),2)
  await self.ops.control(retry['id'],Control(action='cancel'))
  second=await self.ops.retry_invite(old['id'],RetryInvite(confirmed=True,request_key='after-cancel-retry-key',reason='Recipient confirmed changed permission'))
  self.assertEqual(second['state'],'draft');self.assertEqual(len(self.c.calls),1)
 async def test_retry_rejects_unsupported_results_and_nonselected_records(self):
  old=await self.failed_invite();iid=old['items'][0]['id']
  for state in ['uncertain','sending','sent','invited','already_member','blocked','failed','duplicate_skipped','pending']:
   with self.subTest(state=state):
    with self.ops.store.db:
     self.ops.store.db.execute('UPDATE op_items SET state=? WHERE id=?',(state,iid))
     self.ops.store.db.execute('UPDATE op_invited SET state=?',(state,))
    with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec())
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_items SET state='restricted' WHERE id=?",(iid,))
   self.ops.store.db.execute("UPDATE op_invited SET state='restricted'")
  for ids in [[iid+100],[iid,iid]]:
   with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec(item_ids=ids))
  self.assertEqual(len(self.c.calls),1);self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_jobs')),1)
 async def test_retry_rejects_sender_or_numeric_recipient_identity_change(self):
  old=await self.failed_invite();original=self.c.get_entity
  async def changed(ref):
   if ref==200 or isinstance(ref,types.InputPeerUser) and ref.user_id==200:return types.User(id=201,username='different_person')
   return await original(ref)
  self.c.get_entity=changed
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec())
  self.c.get_entity=original;self.a.me=types.User(id=101,username='changed_sender')
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET telegram_id=101 WHERE id=?',(self.a.aid,))
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec())
  self.assertEqual(len(self.c.calls),1);self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_jobs')),1)
 async def test_retry_audience_candidates_and_session_remain_bound(self):
  from audience import session_binding
  self.c.session=SimpleNamespace(auth_key=SimpleNamespace(key=b'synthetic-session-key'))
  old=await self.failed_invite();payload=old['spec'];payload.update(audience_run_id=1,candidate_ids=[1],users=[])
  with self.ops.store.db:
   self.ops.store.db.execute("INSERT INTO audience_runs(id,account,telegram_id,binding,target,target_title,spec,created,updated) VALUES(1,?,?,?,?,?,?,?,?)",(self.a.aid,100,session_binding(self.a),old['items'][0]['chat_id'],'Managed group','{}','before','before'))
   self.ops.store.db.execute("INSERT INTO audience_candidates(id,run_id,user_id,username,display_name,access_hash,eligible,reason,evidence) VALUES(1,1,200,'agreed_user','Consenting',1,1,'Approved','[]')")
   self.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=?',(json.dumps(payload),old['id']))
   self.ops.store.db.execute('UPDATE op_items SET user_access_hash=1 WHERE job=?',(old['id'],))
  self.assertTrue((await self.ops.detail(old['id']))['retryable'])
  with self.ops.store.db:self.ops.store.db.execute('UPDATE audience_candidates SET access_hash=2 WHERE id=1')
  blocked=await self.ops.detail(old['id']);self.assertFalse(blocked['retryable'])
  self.assertIn('подбора изменились',blocked['items'][0]['retry_reason'])
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec())
  with self.ops.store.db:self.ops.store.db.execute('UPDATE audience_candidates SET access_hash=1 WHERE id=1')
  retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  self.assertEqual(retry['spec']['audience_run_id'],1);self.assertEqual(retry['spec']['candidate_ids'],[1]);self.assertEqual(retry['spec']['users'],[])
  self.c.session.auth_key.key=b'another-session-key'
  with self.assertRaises(HTTPException):await self.start(retry)
  self.assertEqual(len(self.c.calls),1)
 async def test_two_concurrent_retry_requests_create_only_one_draft(self):
  old=await self.failed_invite()
  requests=[RetryInvite(confirmed=True,request_key='parallel-retry-key-'+str(n),reason='Recipient changed group permissions') for n in range(2)]
  results=await asyncio.gather(*(self.ops.retry_invite(old['id'],request) for request in requests),return_exceptions=True)
  self.assertEqual(sum(isinstance(result,dict) for result in results),1)
  self.assertEqual(sum(isinstance(result,HTTPException) for result in results),1)
  self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_jobs')),2);self.assertEqual(len(self.c.calls),1)
 async def test_retry_stale_marker_refuses_start_and_never_overwrites_newer_result(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_invited SET state='uncertain',job=999")
  with self.assertRaises(HTTPException):await self.start(retry)
  self.assertEqual((await self.ops.detail(retry['id']))['state'],'draft')
  with self.ops.store.db:
   self.ops.store.db.execute("UPDATE op_invited SET state='restricted',job=?",(old['id'],))
  await self.start(retry)
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_invited SET state='uncertain',job=999")
  await self.ops.tick()
  self.assertEqual(len(self.c.calls),1)
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'uncertain','job':999}])
  self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'failed')
 async def test_retry_invalidated_during_resolution_keeps_old_marker_and_sends_nothing(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec());await self.start(retry)
  original=self.c.get_entity
  async def invalidate(ref):
   if ref==200 or isinstance(ref,types.InputPeerUser) and ref.user_id==200:
    with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='uncertain' WHERE job=?",(old['id'],))
   return await original(ref)
  self.c.get_entity=invalidate;await self.ops.tick()
  self.assertEqual(len(self.c.calls),1)
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'restricted','job':old['id']}])
  self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'failed')
 async def test_retry_checks_membership_and_permissions_again_without_inviting_existing_member(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  self.c.member=True;await self.start(retry);await self.ops.tick()
  self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'already_member')
  self.assertEqual(len(self.c.calls),1)
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'already_member','job':retry['id']}])
 async def test_retry_current_group_rights_are_required_to_create_and_execute(self):
  old=await self.failed_invite();self.group.creator=False
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],self.retry_spec())
  self.group.creator=True;retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  await self.start(retry);self.group.creator=False;await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertEqual((await self.ops.detail(retry['id']))['items'][0]['state'],'failed')
 async def test_missing_invitee_premium_flags_distinguish_invite_from_private_message(self):
  for flags,code in [(dict(premium_would_allow_invite=True,premium_required_for_pm=False),'premium_required_for_invite'),(dict(premium_would_allow_invite=False,premium_required_for_pm=True),'privacy_restricted')]:
   with self.subTest(flags=flags):
    self.c.missing=True;self.c.missing_flags=flags
    with self.ops.store.db:self.ops.store.db.execute('DELETE FROM op_invited')
    self.clear_wait();j=await self.draft('invite',request_key='flags-'+code+'-0001');await self.start(j);await self.ops.tick()
    diagnosis=(await self.ops.detail(j['id']))['items'][0]['diagnosis']
    self.assertEqual(diagnosis['code'],code)
    self.assertEqual(diagnosis['premium_would_allow_invite'],flags['premium_would_allow_invite'])
    self.assertEqual(diagnosis['premium_required_for_pm'],flags['premium_required_for_pm'])
    if flags['premium_required_for_pm']:self.assertIn('не разрешает прямое добавление',diagnosis['next_action'])
 async def test_invite_rpc_privacy_is_retryable_but_account_block_is_not(self):
  self.c.failure=errors.UserPrivacyRestrictedError(None);j=await self.draft('invite');await self.start(j);await self.ops.tick()
  detail=await self.ops.detail(j['id']);self.assertTrue(detail['retryable'])
  self.assertEqual(detail['items'][0]['diagnosis']['source'],'rpc_error')
  self.assertEqual(detail['items'][0]['diagnosis']['code'],'UserPrivacyRestrictedError')
  self.assertEqual(detail['items'][0]['state'],'restricted')
  self.assertTrue(detail['items'][0]['diagnosis']['invitation_requested'])
  self.assertNotIn('request',detail['items'][0]['diagnosis'])
  self.clear_wait();self.c.failure=errors.PeerFloodError(None)
  retry=await self.ops.retry_invite(j['id'],self.retry_spec());await self.start(retry);await self.ops.tick()
  detail=await self.ops.detail(retry['id']);self.assertFalse(detail['retryable'])
  self.assertEqual(detail['items'][0]['state'],'blocked');self.assertFalse(self.ops.account_row(self.a.aid)['enabled'])
 async def test_retry_that_becomes_uncertain_is_never_repeated_or_rotated(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  self.c.failure=TimeoutError();await self.start(retry);await self.ops.tick();self.clear_wait();await self.ops.tick()
  detail=await self.ops.detail(retry['id']);self.assertEqual(detail['items'][0]['state'],'uncertain');self.assertEqual(detail['state'],'paused')
  self.assertFalse(detail['retryable']);self.assertFalse((await self.ops.detail(old['id']))['retryable'])
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'uncertain','job':retry['id']}])
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],RetryInvite(confirmed=True,request_key='uncertain-manual-retry-key',reason='Recipient changed privacy again'))
  with self.assertRaises(HTTPException):await self.ops.control(retry['id'],Control(action='resume',confirmed=True))
  self.assertEqual(len(self.c.calls),2)
 async def test_context_failure_after_retry_request_keeps_unknown_marker_not_old_refusal(self):
  old=await self.failed_invite();retry=await self.ops.retry_invite(old['id'],self.retry_spec())
  self.c.failure=HTTPException(409,'Context changed after dispatch');await self.start(retry);await self.ops.tick()
  item=(await self.ops.detail(retry['id']))['items'][0]
  self.assertEqual(item['state'],'uncertain');self.assertTrue(item['diagnosis']['invitation_requested'])
  self.assertEqual(self.ops.store.rows('SELECT state,job FROM op_invited'),[{'state':'uncertain','job':retry['id']}])
  with self.assertRaises(HTTPException):await self.ops.retry_invite(old['id'],RetryInvite(confirmed=True,request_key='context-unknown-retry-key',reason='Recipient changed privacy again'))
  self.assertEqual(len(self.c.calls),2)
 async def test_api_cannot_forge_retry_proof_or_change_retry_sender(self):
  from pydantic import ValidationError
  for key in ['retry_invite','retry_of','retry_of_job']:
   with self.subTest(key=key):
    with self.assertRaises(ValidationError):await self.draft('invite',**{key:{'job_id':1,'item_ids':[1]}})
  with self.assertRaises(ValidationError):RetryInvite(confirmed=False,request_key='manual-retry-key-0001',reason='Changed permission')
  with self.assertRaises(ValidationError):RetryInvite(confirmed=True,request_key='manual-retry-key-0001',reason='Changed permission',account='another-account')
  self.assertFalse(self.c.calls)
 async def test_personal_invitation_draft_confirm_send_and_dedup(self):
  options=dict(users=['@agreed_user'],consent_note='Asked for event invitations',text='Your requested invitation',invite_url='https://t.me/managed_group')
  j=await self.draft('invite_message',**options);self.assertFalse(self.c.calls)
  await self.start(j);await self.ops.tick()
  result=await self.ops.detail(j['id']);self.assertEqual(result['items'][0]['state'],'sent')
  self.assertEqual(result['items'][0]['message_ids'],[700])
  request=self.c.calls[0];self.assertIsInstance(request,functions.messages.SendMessageRequest)
  self.assertEqual(request.peer.id,200);self.assertIn(options['invite_url'],request.message)
  self.assertFalse(any(isinstance(x,functions.channels.InviteToChannelRequest) for x in self.c.calls))
  self.clear_wait();k=await self.draft('invite_message',request_key='different-campaign-same-person',**options);await self.start(k);await self.ops.tick()
  self.assertEqual((await self.ops.detail(k['id']))['items'][0]['state'],'duplicate_skipped');self.assertEqual(len(self.c.calls),1)
 async def test_personal_invitation_rejects_wrong_group_link_and_missing_consent(self):
  with self.assertRaises(HTTPException):await self.draft('invite_message',users=['@agreed_user'],text='Hello',invite_url='https://t.me/managed_group')
  with self.assertRaises(HTTPException):await self.draft('invite_message',users=['@agreed_user'],consent_note='Asked for invitations',text='Hello',invite_url='https://t.me/other_group')
  self.assertFalse(self.c.calls)
 async def test_link_is_persisted_not_sent(self):
  j=await self.draft('link');await self.start(j);await self.ops.tick();d=await self.ops.detail(j['id'])
  self.assertEqual(d['items'][0]['invite_link'],'https://t.me/+DEVELOPMENT_ONLY');self.assertEqual(len(self.c.calls),1)
  self.assertIsInstance(self.c.calls[0],functions.messages.ExportChatInviteRequest);self.assertTrue(self.c.calls[0].request_needed)
 async def test_floodwait_pauses_account_queue_and_keeps_random_id(self):
  j=await self.draft();await self.start(j);ids=self.ops.store.rows('SELECT random_ids FROM op_items')[0]['random_ids']
  self.c.failure=errors.FloodWaitError(None,capture=120);await self.ops.tick();await self.ops.tick()
  self.assertEqual(len(self.c.calls),1);self.assertEqual((await self.ops.detail(j['id']))['state'],'paused')
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='resume',confirmed=True))
  self.assertEqual(self.ops.store.rows('SELECT random_ids FROM op_items')[0]['random_ids'],ids)
 async def test_peer_flood_disables_account_no_rotation(self):
  j=await self.draft();await self.start(j);self.c.failure=errors.PeerFloodError(None);await self.ops.tick();await self.ops.tick()
  self.assertFalse(self.ops.account_row(self.a.aid)['enabled']);self.assertEqual(len(self.c.calls),1)
 async def test_uncertain_and_restart_never_resend(self):
  j=await self.draft();await self.start(j);self.c.failure=TimeoutError();await self.ops.tick();self.clear_wait();await self.ops.tick()
  self.assertEqual(len(self.c.calls),1)
  with self.assertRaises(HTTPException):await self.ops.control(j['id'],Control(action='resume',confirmed=True))
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='sending'")
  self.ops.initialize();self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'uncertain')
 async def test_daily_limit_holds_remaining(self):
  j=await self.draft(daily_limit=1);await self.start(j);await self.ops.tick();self.clear_wait()
  k=await self.draft(request_key='next-daily-key-0001');await self.start(k);await self.ops.tick();self.assertEqual(len(self.c.calls),1)
 async def test_pause_during_resolution_prevents_send(self):
  j=await self.draft();await self.start(j);original=self.c.get_entity
  async def read(ref):
   await self.ops.control(j['id'],Control(action='pause'));return await original(ref)
  self.c.get_entity=read;await self.ops.tick();self.assertEqual(self.c.calls,[]);self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'pending')
 async def test_csv_formula_escaped(self):
  j=await self.draft(title='=danger');r=await self.ops.report(j['id']);self.assertIn("'=danger",r.body.decode())

 async def test_import_metadata_never_claims_connected_and_can_remove(self):
  r=await self.ops.import_account(ImportSpec(payload={'phone':'+70000000000','app_id':1234,'app_hash':'a'*32,'session_file':'/etc/passwd','password':'private-unused-password'},authorized=True))
  self.assertEqual(r['connection'],'not_connected');self.assertTrue(r['needs_auth'])
  dump=json.dumps(await self.ops.state());self.assertNotIn('a'*32,dump);self.assertNotIn('private-unused-password',dump)
  aid=r['account']['id'];self.assertEqual(self.ops.accounts[aid].get_api()['id'],1234)
  await self.ops.remove_account(aid);self.assertNotIn(aid,self.ops.accounts);self.assertIsNone(self.ops.store.get('account_api_'+aid))
  with self.assertRaises(HTTPException):await self.ops.remove_account(self.a.aid)
 def session_payload(self,host='149.154.167.51'):
  s=StringSession();s.set_dc(2,host,443);s.auth_key=AuthKey(bytes([42])*256)
  return {'format':'chatcleaner.telethon.v1','api_id':1234,'api_hash':'a'*32,'session_string':s.save()}
 async def test_import_validation_no_private_endpoint_or_secret_echo(self):
  for payload in [self.session_payload('127.0.0.1'),self.session_payload('8.8.8.8'),{'session_string':'private-invalid-session'}, {'api_id':1}, {'user_id':0},{'format':'unknown'}]:
   with self.assertRaises(HTTPException) as caught:await self.ops.import_account(ImportSpec(payload=payload,authorized=True))
   self.assertNotIn('private-invalid-session',str(caught.exception.detail))
  self.assertEqual(len(self.ops.accounts),1);self.assertFalse(self.c.calls)
 async def test_valid_string_session_import_requires_telegram_identity(self):
  c=Client(self.group);c.connect=AsyncMock();c.is_user_authorized=AsyncMock(return_value=True)
  c.get_me.return_value=types.User(id=201,username='imported',phone='70000000001')
  with patch('operations.ClientTelegramClient',return_value=c):
   r=await self.ops.import_account(ImportSpec(payload={**self.session_payload(),'user_id':201,'phone':'+70000000001'},authorized=True))
  self.assertEqual(r['connection'],'connected');self.assertEqual(r['account']['telegram_id'],201)
  self.assertTrue(Path(self.ops.accounts[r['account']['id']].session_path).exists());c.log_out.assert_not_awaited()
  self.assertNotIn(self.session_payload()['session_string'],json.dumps(await self.ops.state()))
 async def test_duplicate_import_and_unauthed_import_do_not_revoke_source(self):
  c=Client(self.group);c.connect=AsyncMock();c.is_user_authorized=AsyncMock(return_value=True)
  with patch('operations.ClientTelegramClient',return_value=c):r=await self.ops.import_account(ImportSpec(payload=self.session_payload(),authorized=True))
  self.assertEqual(r['connection'],'not_connected');c.log_out.assert_not_awaited();self.assertIn('уже подключён',r['error'])
  c.is_user_authorized.return_value=False
  with patch('operations.ClientTelegramClient',return_value=c):r=await self.ops.import_account(ImportSpec(payload=self.session_payload(),authorized=True))
  self.assertTrue(r['needs_auth']);self.assertFalse(r['account']['enabled']);c.log_out.assert_not_awaited()
 async def test_primary_uses_existing_client_and_cannot_auth_or_disconnect(self):
  # Different independent runtime identity from the existing additional account.
  runtime=SimpleNamespace(client=self.c,me=types.User(id=500,username='main_account'))
  self.ops.get_primary=lambda:runtime;self.ops.sync_primary()
  self.assertIs(self.ops.online('primary').client,self.c)
  with self.assertRaises(HTTPException):await self.ops.disconnect('primary')
  with self.assertRaises(HTTPException):await self.ops.code('primary',Phone(phone='+70000000000'))
  j=await self.draft(account='primary');await self.start(j);await self.ops.tick();self.assertEqual(len(self.c.calls),1)
  self.assertFalse((self.ops.data/'primary.session').exists())
  self.clear_wait();runtime.me=types.User(id=501,username='replacement');self.ops.sync_primary()
  self.assertFalse(self.ops.account_row('primary')['enabled'])
 async def test_removal_targets_only_persisted_message_ids_and_verifies(self):
  self.c.delete_messages=AsyncMock();self.c.get_messages=AsyncMock(return_value=[None])
  j=await self.draft();await self.start(j);await self.ops.tick();self.clear_wait()
  r=await self.ops.remove_messages(j['id']);self.assertEqual(r['items'][0]['state'],'removed')
  self.c.delete_messages.assert_awaited_once_with(self.group,[700],revoke=True)
  await self.ops.remove_messages(j['id']);self.c.delete_messages.assert_awaited_once()
 async def test_removal_not_reported_until_absent(self):
  self.c.delete_messages=AsyncMock();self.c.get_messages=AsyncMock(return_value=[types.Message(id=700,peer_id=types.PeerChannel(10),message='still present')])
  j=await self.draft();await self.start(j);await self.ops.tick();self.clear_wait()
  with self.assertRaises(HTTPException):await self.ops.remove_messages(j['id'])
  self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'sent')
 async def test_disconnected_account_does_not_block_other_queue(self):
  j=await self.draft();await self.start(j)
  b=await self.ops.add_account(AccountSpec(label='Second',authorized=True));a=self.ops.accounts[b['id']];c=Client(self.group);c.get_me.return_value=types.User(id=101);a.client=c;await a.ready()
  k=await self.draft(account=a.aid,request_key='second-independent-key');await self.start(k)
  self.c.is_connected=lambda:False;await self.ops.tick();self.assertEqual(len(c.calls),1);self.assertFalse(self.c.calls)

if __name__=='__main__':unittest.main()
