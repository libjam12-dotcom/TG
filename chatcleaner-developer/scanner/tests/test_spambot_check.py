"""Dedicated existing fake client; native temp SQLite; no real session or network."""
import asyncio
from datetime import datetime,timezone,timedelta
import importlib.util
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import AsyncMock

STAGE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(STAGE))
import operations,spambot_check
from spambot_check import SpamBotRequest,BOT,PREFIX,SENDER_PREFIX
from fastapi import FastAPI,HTTPException
from pydantic import ValidationError
from telethon import errors,types,functions
import httpx
spec=importlib.util.spec_from_file_location('spambot_operation_fixture',STAGE/'tests/test_operations.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)

class BotClient:
 _request_retries=0
 flood_sleep_threshold=0
 def __init__(self):
  self.me=types.User(id=100,username='selected_test')
  self.bot=types.User(id=BOT,username='SpamBot',bot=True,verified=True,access_hash=121)
  self.calls=[];self.fail=None;self.hook=None;self.reply=True;self.reply_peer=BOT
  self.get_me=AsyncMock(return_value=self.me)
  self.get_entity=AsyncMock(return_value=self.bot)
  self.connect=AsyncMock(side_effect=AssertionError('No reconnect'))
  self.disconnect=AsyncMock(side_effect=AssertionError('No disconnect'))
 def is_connected(self):return True
 async def __call__(self,req):
  self.calls.append(req)
  if self.hook:await self.hook(req)
  if isinstance(req,functions.messages.SendMessageRequest):
   if self.fail:raise self.fail
   return types.UpdateShortSentMessage(id=501,pts=1,pts_count=1,date=datetime.now(timezone.utc))
  if isinstance(req,functions.messages.GetHistoryRequest):
   fresh=types.Message(id=502,peer_id=types.PeerUser(self.reply_peer),from_id=types.PeerUser(BOT),date=datetime.now(timezone.utc),message='Fresh official reply')
   old=types.Message(id=499,peer_id=types.PeerUser(BOT),from_id=types.PeerUser(BOT),date=datetime.now(timezone.utc)-timedelta(hours=1),message='Old clean reply')
   wrong=types.Message(id=503,peer_id=types.PeerUser(BOT+1),from_id=types.PeerUser(BOT+1),date=datetime.now(timezone.utc),message='Wrong bot')
   return types.messages.Messages(messages=([fresh] if self.reply else [])+[old,wrong],chats=[],users=[])
  raise AssertionError('Other RPC forbidden')

class SpamBotTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixture.OperationsTests.asyncSetUp
 async def asyncSetUp(self):
  await fixture.OperationsTests.asyncSetUp(self)
  self.client=BotClient();self.a.client=self.client;self.a.me=self.client.me
  self.a.pause();self.a.error='Saved PeerFlood guard'
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',('Saved PeerFlood guard',self.a.aid))
  self.ops.store.put('outbound_pause_revision_'+self.a.aid,8)
  self.before=self.snapshot();self.check=self.ops.spambot_check
  self.old_wait=spambot_check.WAIT_REPLY;spambot_check.WAIT_REPLY=0
 async def asyncTearDown(self):
  self.a.client=self.c  # native fixture teardown must not disconnect live/fake diagnostic client
  await fixture.OperationsTests.asyncTearDown(self)
  spambot_check.WAIT_REPLY=self.old_wait
  self.client.connect.assert_not_called();self.client.disconnect.assert_not_called()
 def snapshot(self):
  return {name:self.ops.store.rows('SELECT * FROM '+name+' ORDER BY rowid') for name in ['op_accounts','op_jobs','op_items','op_invited']}
 def request(self,key='official-start-request-0001'):
  return SpamBotRequest(confirmed=True,telegram_id=100,request_key=key)
 def sends(self):return [r for r in self.client.calls if isinstance(r,functions.messages.SendMessageRequest)]
 def reads(self):return [r for r in self.client.calls if isinstance(r,functions.messages.GetHistoryRequest)]
 async def test_one_start_claim_before_rpc_one_history_and_fresh_official_reply_only(self):
  async def assert_claim(req):
   if isinstance(req,functions.messages.SendMessageRequest):
    record=self.ops.store.get(PREFIX+self.request().request_key)
    self.assertEqual(record['state'],'claimed');self.assertEqual(record['send_claims'],1)
  self.client.hook=assert_claim
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'reply_received')
  self.assertEqual([r['text'] for r in result['replies']],['Fresh official reply'])
  self.assertEqual(len(self.sends()),1);self.assertEqual(len(self.reads()),1)
  self.assertEqual((self.sends()[0].peer.user_id,self.sends()[0].message),(BOT,'/start'))
  self.assertEqual((self.reads()[0].min_id,self.reads()[0].limit),(501,5))
  self.assertEqual(self.snapshot(),self.before)
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),8)
 async def test_raw_incoming_peer_may_be_exact_own_uid_but_other_peer_rejected(self):
  self.client.reply_peer=100
  first=await self.check.check(self.a.aid,self.request())
  self.assertEqual(first['state'],'reply_received')
  self.client.reply_peer=101
  second=await self.check.check(self.a.aid,self.request('different-explicit-request2'))
  self.assertEqual(second['state'],'reply_pending');self.assertEqual(second['replies'],[])
  self.assertEqual(len(self.sends()),2);self.assertEqual(len(self.reads()),2)

 async def test_double_click_same_key_and_cached_get_do_not_repeat_any_rpc(self):
  one,two=await asyncio.gather(self.check.check(self.a.aid,self.request()),self.check.check(self.a.aid,self.request()))
  self.assertEqual(one,two)
  self.client.get_me.side_effect=AssertionError('Cached GET must not call Telegram')
  got=await self.check.status(self.a.aid,self.request().request_key)
  self.assertEqual(got,one);self.assertEqual(len(self.sends()),1);self.assertEqual(len(self.reads()),1)
 async def test_lost_ack_unknown_is_durable_and_new_key_cannot_retry_unknown(self):
  self.client.fail=asyncio.TimeoutError()
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'unknown')
  await self.check.check(self.a.aid,self.request())
  with self.assertRaises(HTTPException):await self.check.check(self.a.aid,self.request('another-explicit-key-0002'))
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])
  self.assertEqual(self.snapshot(),self.before)
 async def test_wrong_bot_id_or_unverified_bot_never_sends(self):
  for changed in ('id','verified','username'):
   original=getattr(self.client.bot,changed)
   setattr(self.client.bot,changed,BOT+1 if changed=='id' else False if changed=='verified' else 'fake_spambot')
   with self.assertRaises(HTTPException):await self.check.check(self.a.aid,self.request())
   setattr(self.client.bot,changed,original)
  self.assertEqual(self.client.calls,[]);self.assertEqual(self.snapshot(),self.before)
 async def test_blocked_bot_no_unblock_appeal_resume_or_retry(self):
  self.client.fail=errors.YouBlockedUserError(None)
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'blocked')
  await self.check.check(self.a.aid,self.request())
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])
  self.assertEqual(self.snapshot(),self.before)
 async def test_revoked_stops_without_touching_jobs(self):
  self.client.fail=errors.SessionRevokedError(None)
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'stopped');self.assertEqual(result['error_type'],'SessionRevokedError')
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])
  self.assertEqual(self.snapshot(),self.before)
 async def test_peerflood_stops_without_history_or_changing_existing_pause(self):
  self.client.fail=errors.PeerFloodError(None)
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'stopped');self.assertEqual(result['error_type'],'PeerFloodError')
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])
  self.assertEqual(self.snapshot(),self.before)
 async def test_preflight_revoked_is_explicit_refusal_zero_messages(self):
  self.client.get_me.side_effect=errors.SessionRevokedError(None)
  with self.assertRaises(HTTPException) as caught:await self.check.check(self.a.aid,self.request())
  self.assertEqual(caught.exception.status_code,409)
  self.assertEqual(self.client.calls,[]);self.assertEqual(self.snapshot(),self.before)
 async def test_http_request_cancel_after_claim_durable_unknown_no_resend(self):
  entered,release=asyncio.Event(),asyncio.Event()
  async def block_send(req):
   if isinstance(req,functions.messages.SendMessageRequest):entered.set();await release.wait()
  self.client.hook=block_send
  task=asyncio.create_task(self.check.check(self.a.aid,self.request()))
  await entered.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  got=await self.check.status(self.a.aid,self.request().request_key)
  self.assertEqual(got['state'],'unknown');self.assertEqual(got['error_type'],'RequestInterrupted')
  await self.check.check(self.a.aid,self.request())
  self.assertEqual(len(self.sends()),1);self.assertEqual(self.reads(),[])

 async def test_floodwait_never_shortens_cooldown_or_enables_account(self):
  self.client.fail=errors.FloodWaitError(None,capture=300)
  result=await self.check.check(self.a.aid,self.request())
  row=self.ops.account_row(self.a.aid)
  self.assertEqual(result['state'],'stopped');self.assertEqual(result['wait_seconds'],300)
  self.assertEqual(row['enabled'],0);self.assertGreater(row['not_before'],time.time()+290)
  self.assertEqual(row['error'],'Saved PeerFlood guard')
  for name in ('op_jobs','op_items','op_invited'):self.assertEqual(self.snapshot()[name],self.before[name])
 async def test_sender_revision_race_before_claim_never_sends(self):
  async def changed():
   self.ops.store.put('outbound_pause_revision_'+self.a.aid,9)
   return self.client.me
  self.client.get_me.side_effect=changed
  with self.assertRaises(HTTPException):await self.check.check(self.a.aid,self.request())
  self.assertEqual(self.client.calls,[])
 async def test_delayed_reply_inconclusive_one_read_no_auto_start_again(self):
  self.client.reply=False
  result=await self.check.check(self.a.aid,self.request())
  self.assertEqual(result['state'],'reply_pending');self.assertEqual(result['replies'],[])
  await self.check.check(self.a.aid,self.request())
  self.assertEqual(len(self.sends()),1);self.assertEqual(len(self.reads()),1)
 async def test_restart_claim_interrupted_without_rpc_or_retry(self):
  request=self.request();record=dict(scope=spambot_check.SCOPE,account=self.a.aid,telegram_id=100,request_key=request.request_key,state='claimed',send_claims=1)
  with self.ops.store.db:self.check.save(record)
  self.check.initialize()
  got=await self.check.status(self.a.aid,request.request_key)
  self.assertEqual(got['state'],'interrupted');self.assertEqual(self.client.calls,[])
 async def test_endpoint_strict_body_and_owner_guard(self):
  for data in [dict(confirmed=False),dict(confirmed='true'),dict(confirmed=True,message='other text')]:
   with self.assertRaises(ValidationError):SpamBotRequest(telegram_id=100,request_key='official-start-request-0001',**data)
  app=FastAPI();app.include_router(self.ops.router)
  async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
   url='/api/scanner/operations/accounts/'+self.a.aid+'/spambot-check'
   response=await client.post(url,json=self.request().model_dump())
   self.assertEqual(response.status_code,403)
   admin={'x-panel-user':'admin-fixture','x-panel-role':'admin'}
   response=await client.post(url,json=self.request().model_dump(),headers=admin)
   self.assertEqual(response.status_code,403);self.assertEqual(self.client.calls,[])
   owner={'x-panel-user':'owner-fixture','x-panel-role':'owner'}
   empty=await client.post(url,json={},headers=owner)
   self.assertEqual(empty.status_code,422);self.assertEqual(self.client.calls,[])
   allowed=await client.post(url,json=self.request().model_dump(),headers=owner)
   self.assertEqual(allowed.status_code,200)
   denied=await client.get(url+'/'+self.request().request_key,headers=admin)
   self.assertEqual(denied.status_code,403)
   cached=await client.get(url+'/'+self.request().request_key,headers=owner)
   self.assertEqual(cached.status_code,200);self.assertEqual(len(self.sends()),1)

if __name__=='__main__':unittest.main()
