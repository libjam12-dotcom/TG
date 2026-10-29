"""Native temp SQLite + fake SDK; exact evidence-bound factual clarification."""
from datetime import datetime,timezone
import importlib.util
import json
from pathlib import Path
import sys
import unittest

STAGE=Path(__file__).resolve().parents[1]
OLD=STAGE
sys.path.insert(0,str(STAGE))
import operations,spambot_check
from spambot_check import SpamBotRequest,SpamBotClarification,PREFIX,BOT,SCOPE,CLARIFICATION_SCOPE,digest
from fastapi import HTTPException,FastAPI
from pydantic import ValidationError
from telethon import errors,types,functions
import httpx
spec=importlib.util.spec_from_file_location('old_spambot_regression',OLD/'tests/test_spambot_check.py')
old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
SOURCE_KEY='spambot-followup-20261029-test-1'

class ClarificationTests(old.SpamBotTests):
 def seed(self):
  source=dict(scope=SCOPE,account=self.a.aid,telegram_id=100,bot_id=BOT,request_key=SOURCE_KEY,state='reply_received',send_claims=1,
   reply_read_attempts=1,outgoing_message_id=564,attempted_at='2026-10-29T10:47:32.121941+00:00',
   replies=[{'id':565,'date':'2026-10-29T10:47:32+00:00','text':'Good news, no limits are currently applied to your account. You’re free as a bird!'}],
   digest=digest({'scope':SCOPE,'account':self.a.aid,'telegram_id':100,'bot_id':BOT,'text':'/start'}))
  self.ops.store.put(PREFIX+SOURCE_KEY,source)
  cfg=dict(account=self.a.aid,telegram_id=100,consent_confirmed=True,consent_note='Owner recorded approved recipient consent')
  payload={'manual_invite_campaign':{'scope':'manual-invite-campaign-v1','config':cfg}}
  diag={'invitation_requested':True,'verification_error':{'code':'PeerFloodError','rpc':{'exception_type':'PeerFloodError','message':'PEER_FLOOD','request_constructor':'InviteToChannelRequest'}}}
  with self.ops.store.db:
   self.ops.store.db.execute('INSERT INTO op_jobs(id,request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
    (189,'source-owned-control','source',self.a.aid,100,'invite','Synthetic source',json.dumps(payload),'paused','old','old'))
   self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids,attempted_at,diagnosis) VALUES(?,?,?,?,?,?,?,?,?)',
    (189,-1004292653659,'Synthetic',710000010,'approved_person','uncertain','[]','2026-10-29T12:19:28.486424+00:00',json.dumps(diag)))
  return source
 def clarification_request(self,key='factual-clarification-request-1',**changes):
  return SpamBotClarification(**dict(confirmed=True,telegram_id=100,request_key=key,source_check_key=SOURCE_KEY,source_job_id=189,**changes))
 async def test_fixed_clarification_one_send_bound_evidence_preserves_old_start_record(self):
  source=self.seed();before=self.snapshot()
  r=await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(r['scope'],CLARIFICATION_SCOPE);self.assertEqual(r['source_reply_id'],565)
  text=self.sends()[0].message
  self.assertIn('2026-10-29 10:47:32 UTC',text);self.assertIn('2026-10-29 12:19:28 UTC',text)
  self.assertIn('PEER_FLOOD',text);self.assertNotIn('/start',text);self.assertNotIn('710000010',text)
  self.assertEqual(r['factual_text'],text);self.assertEqual(len(self.sends()),1);self.assertEqual(len(self.reads()),1)
  self.assertEqual(self.ops.store.get(PREFIX+SOURCE_KEY),source);self.assertEqual(self.snapshot(),before)
  cached_start=await self.check.check(self.a.aid,SpamBotRequest(confirmed=True,telegram_id=100,request_key=SOURCE_KEY))
  self.assertEqual(cached_start['scope'],SCOPE);self.assertEqual(len(self.sends()),1)
 async def test_cross_account_or_uid_source_check_rejected_before_any_sdk_write(self):
  source=self.seed()
  for field,value in [('account','other-account'),('telegram_id',101),('bot_id',123)]:
   changed=dict(source);changed[field]=value;self.ops.store.put(PREFIX+SOURCE_KEY,changed)
   with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(self.client.calls,[]);self.client.get_me.assert_not_awaited()
 async def test_wrong_job_identity_kind_or_no_consent_rejected(self):
  self.seed()
  row=self.ops.store.rows('SELECT * FROM op_jobs WHERE id=189')[0]
  with self.ops.store.db:self.ops.store.db.execute('INSERT INTO op_accounts(id,label,created) VALUES(?,?,?)',('other','Other synthetic','old'))
  for column,value in [('account','other'),('telegram_id',101),('kind','post')]:
   with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET '+column+'=? WHERE id=189',(value,))
   with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
   with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET '+column+'=? WHERE id=189',(row[column],))
  payload=json.loads(row['spec']);payload['manual_invite_campaign']['config']['consent_confirmed']=False
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=189',(json.dumps(payload),))
  with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(self.client.calls,[])
 async def test_no_peerflood_or_failure_before_clean_reply_is_not_a_valid_discrepancy(self):
  self.seed();item=self.ops.store.rows('SELECT * FROM op_items WHERE job=189')[0]
  diag=json.loads(item['diagnosis']);diag['verification_error']['code']='UserNotMutualContactError'
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET diagnosis=? WHERE id=?',(json.dumps(diag),item['id']))
  with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_items SET diagnosis=?,attempted_at=? WHERE id=?',(item['diagnosis'],'2026-10-29T09:00:00+00:00',item['id']))
  with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(self.client.calls,[])
 async def test_source_check_race_after_snapshot_blocks_before_send(self):
  source=self.seed()
  async def changed():
   mutated=dict(source);mutated['replies']=[];self.ops.store.put(PREFIX+SOURCE_KEY,mutated)
   return self.client.me
  self.client.get_me.side_effect=changed
  with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(self.client.calls,[])
 async def test_native_source_job_race_after_snapshot_blocks_before_send(self):
  self.seed()
  async def changed():
   with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET title='Changed after snapshot' WHERE id=189")
   return self.client.me
  self.client.get_me.side_effect=changed
  with self.assertRaises(HTTPException):await self.check.clarification(self.a.aid,self.clarification_request())
  self.assertEqual(self.client.calls,[])

 async def test_cached_clarification_get_and_repost_do_not_resend_or_modify_sources(self):
  self.seed();request=self.clarification_request()
  first=await self.check.clarification(self.a.aid,request)
  second=await self.check.clarification(self.a.aid,request)
  cached=await self.check.status(self.a.aid,request.request_key)
  self.assertEqual(first,second);self.assertEqual(first,cached);self.assertEqual(len(self.sends()),1)
  with self.assertRaises(HTTPException):await self.check.check(self.a.aid,SpamBotRequest(confirmed=True,telegram_id=100,request_key=request.request_key))
 async def test_lost_ack_on_clarification_consumes_one_claim_no_auto_repeat(self):
  self.seed();self.client.fail=TimeoutError()
  request=self.clarification_request();result=await self.check.clarification(self.a.aid,request)
  self.assertEqual(result['state'],'unknown')
  await self.check.clarification(self.a.aid,request)
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])
 async def test_clarification_owner_only_extra_text_forbidden_empty_body_zero_rpc(self):
  self.seed()
  with self.assertRaises(ValidationError):SpamBotClarification(**dict(self.clarification_request().model_dump(),text='arbitrary appeal'))
  app=FastAPI();app.include_router(self.ops.router)
  url='/api/scanner/operations/accounts/'+self.a.aid+'/spambot-clarification'
  async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
   owner={'x-panel-user':'owner-fixture','x-panel-role':'owner'};admin={'x-panel-user':'admin-fixture','x-panel-role':'admin'}
   denied=await client.post(url,json=self.clarification_request().model_dump(),headers=admin)
   self.assertEqual(denied.status_code,403)
   empty=await client.post(url,json={},headers=owner);self.assertEqual(empty.status_code,422)
   self.assertEqual(self.client.calls,[])
   sent=await client.post(url,json=self.clarification_request().model_dump(),headers=owner);self.assertEqual(sent.status_code,200)
   got=await client.get(url+'/'+self.clarification_request().request_key,headers=owner);self.assertEqual(got.status_code,200)
   denied_get=await client.get(url+'/'+self.clarification_request().request_key,headers=admin);self.assertEqual(denied_get.status_code,403)

if __name__=='__main__':unittest.main()
