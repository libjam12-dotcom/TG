"""Explicit secondary restore and manual write gates, synthetic temp SQLite only."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import AsyncMock

SOURCE=Path(__file__).resolve().parents[1]
BACKEND=SOURCE
sys.path.insert(0,str(SOURCE))
import operations
import manual_invite_campaign
from operations import ResumeOutbound
from manual_invite_campaign import ManualConfig,QuickDraftRequest,QuickStartRequest,DraftRequest,StartRequest
from fastapi import HTTPException
from pydantic import ValidationError
from telethon import errors,types
spec=importlib.util.spec_from_file_location('restore_operation_fixture',BACKEND/'tests/test_operations.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)

class RestoreTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixture.OperationsTests.asyncSetUp
 asyncTearDown=fixture.OperationsTests.asyncTearDown
 draft=fixture.OperationsTests.draft

 def request(self,**extra):
  return ResumeOutbound(**dict(confirmed=True,telegram_id=100,reason='Owner reviewed protective stop',**extra))

 def protected(self,error='PeerFlood guard',revision=4):
  self.a.pause();self.a.error=error
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET error=?,not_before=0 WHERE id=?',(error,self.a.aid))
  self.ops.store.put('outbound_pause_revision_'+self.a.aid,revision)
  self.c.get_me.reset_mock()

 def native(self):
  return {'jobs':self.ops.store.rows('SELECT * FROM op_jobs ORDER BY id'),'items':self.ops.store.rows('SELECT * FROM op_items ORDER BY id')}

 async def test_explicit_restore_exact_identity_preserves_revision_cooldown_and_all_job_states(self):
  await self.draft();self.protected();before=self.native()
  result=await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertTrue(result['ok']);self.assertFalse(result['jobs_resumed'])
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],1)
  self.assertIsNone(self.ops.account_row(self.a.aid)['error']);self.assertIsNone(self.a.error)
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),4)
  self.assertEqual(self.ops.account_row(self.a.aid)['not_before'],0)
  self.assertEqual(self.native(),before)
  self.assertEqual(self.ops.store.get('last_outbound_resume_'+self.a.aid)['telegram_id'],100)
  self.c.get_me.assert_awaited_once()

 async def test_reconnect_endpoint_itself_preserves_existing_protection(self):
  await self.draft();self.protected();before=self.native()
  self.a.me=None
  self.c.is_user_authorized=AsyncMock(return_value=True)
  result=await self.ops.reconnect(self.a.aid)
  self.assertTrue(result['authorized'])
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
  self.assertEqual(self.ops.account_row(self.a.aid)['error'],'PeerFlood guard')
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),4)
  self.assertEqual(self.native(),before)

 async def test_known_uid_connected_confirmation_and_reason_required(self):
  self.protected()
  with self.assertRaises(ValidationError):ResumeOutbound(confirmed=False,telegram_id=100,reason='Owner reviewed protective stop')
  with self.assertRaises(HTTPException):
   await self.ops.resume_secondary_outbound(self.a.aid,ResumeOutbound(confirmed=True,telegram_id=101,reason='Owner reviewed protective stop'))
  with self.assertRaises(HTTPException):
   await self.ops.resume_secondary_outbound(self.a.aid,ResumeOutbound(confirmed=True,telegram_id=100,reason='        '))
  self.a.me=None
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.c.get_me.assert_not_awaited()
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)

 async def test_cooldown_blocks_restore_without_rpcs_or_reset(self):
  self.protected();deadline=time.time()+600
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=? WHERE id=?',(deadline,self.a.aid))
  with self.assertRaises(HTTPException) as caught:await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(caught.exception.status_code,429)
  self.c.get_me.assert_not_awaited()
  self.assertEqual(self.ops.account_row(self.a.aid)['not_before'],deadline)

 async def test_pause_revision_change_during_get_me_refuses_restore(self):
  self.protected()
  async def changed():
   self.ops.store.put('outbound_pause_revision_'+self.a.aid,5)
   return types.User(id=100,username='owned_account')
  self.c.get_me.side_effect=changed
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
  self.assertEqual(self.ops.account_row(self.a.aid)['error'],'PeerFlood guard')

 async def test_disconnected_client_after_get_me_cannot_grant_permission(self):
  self.protected()
  async def lost_connection():
   self.c.is_connected=lambda:False
   return types.User(id=100,username='owned_account')
  self.c.get_me.side_effect=lost_connection
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)

 async def test_runtime_only_error_drift_during_get_me_rejects_restore(self):
  self.protected();before=self.ops.account_row(self.a.aid);revision=self.ops.store.get('outbound_pause_revision_'+self.a.aid)
  async def changed_runtime_only():
   self.a.error='NEW runtime Telegram protection'
   return types.User(id=100,username='owned_account')
  self.c.get_me.side_effect=changed_runtime_only
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid),before)
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),revision)
  self.assertEqual(self.a.error,'NEW runtime Telegram protection')
  self.assertIsNone(self.ops.store.get('last_outbound_resume_'+self.a.aid))

 async def test_get_me_wrong_identity_refuses_restore(self):
  self.protected();self.c.get_me.return_value=types.User(id=101)
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)

 async def test_active_manual_scope_blocks_restore_before_get_me(self):
  self.protected()
  self.ops.store.put('manual_invite_campaign_job_999',{'state':'waiting','config':{'telegram_id':100}})
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.c.get_me.assert_not_awaited()
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)

 async def test_queued_or_scheduled_job_blocks_restore_without_awakening_old_queue(self):
  job=await self.draft();self.protected()
  for state in ('queued','scheduled'):
   with self.ops.store.db:self.ops.store.db.execute('UPDATE op_jobs SET state=? WHERE id=?',(state,job['id']))
   before=self.native()
   with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
   self.c.get_me.assert_not_awaited()
   self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
   self.assertEqual(self.native(),before)
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?",(job['id'],))

 async def test_current_sending_action_blocks_restore(self):
  job=await self.draft();self.protected()
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='sending' WHERE job=?",(job['id'],))
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.c.get_me.assert_not_awaited()
  with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='pending' WHERE job=?",(job['id'],))

 async def test_fresh_peerflood_persists_guard_revision_without_job_changes(self):
  await self.draft();self.protected(error=None);before=self.native()
  self.c.get_me.side_effect=errors.PeerFloodError(None)
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  row=self.ops.account_row(self.a.aid)
  self.assertEqual(row['enabled'],0);self.assertIn('Telegram',row['error'])
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),5)
  self.assertEqual(self.native(),before)

 async def test_fresh_revoked_session_persists_guard_without_job_changes(self):
  await self.draft();self.protected(error=None);before=self.native()
  self.c.get_me.side_effect=errors.SessionRevokedError(None)
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
  self.assertIsNotNone(self.ops.account_row(self.a.aid)['error'])
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),5)
  self.assertEqual(self.native(),before)

 async def test_fresh_floodwait_increases_not_before_and_never_enables(self):
  await self.draft();self.protected(error=None);before=self.native()
  self.c.get_me.side_effect=errors.FloodWaitError(None,capture=600)
  with self.assertRaises(HTTPException):await self.ops.resume_secondary_outbound(self.a.aid,self.request())
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)
  self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],time.time()+590)
  self.assertEqual(self.ops.store.get('outbound_pause_revision_'+self.a.aid),4)
  self.assertEqual(self.native(),before)

class ManualGateTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixture.OperationsTests.asyncSetUp
 asyncTearDown=fixture.OperationsTests.asyncTearDown

 def config(self):
  self.c._request_retries=0;self.c.flood_sleep_threshold=0
  return ManualConfig(account=self.a.aid,telegram_id=100,target_id=-1000000000010,target_username='managed_group',users=['agreed_user'],
   consent_note='Approved single synthetic recipient',consent_confirmed=True,max_attempts=1,account_day_attempt_cap=5,success_goal=1,interval_seconds=1)

 def protect(self):
  self.a.pause();self.a.error='PeerFlood guard'
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',('PeerFlood guard',self.a.aid))
  self.ops.store.put('outbound_pause_revision_'+self.a.aid,4)

 async def test_saved_error_must_block_start_before_worker_is_created(self):
  cfg=self.config();self.protect();manual=self.ops.manual_invite_campaign
  draft=await manual.quick_draft(QuickDraftRequest(config=cfg,request_key='error-gate-proof-draft1'))
  with self.assertRaises(HTTPException):
   await manual.quick_start(QuickStartRequest(job_id=draft['job_id'],config_digest=draft['config_digest'],request_key='error-gate-proof-start1',confirmed=True))
  self.assertIsNone(manual.task);self.assertEqual(self.c.calls,[])

 async def test_protected_quick_draft_readable_but_start_rejected_before_recipient_rpc(self):
  cfg=self.config();self.protect();manual=self.ops.manual_invite_campaign
  draft=await manual.quick_draft(QuickDraftRequest(config=cfg,request_key='guarded-quick-draft1'))
  self.assertEqual(draft['sender_error'],'PeerFlood guard')
  self.assertEqual(self.c.calls,[])
  with self.assertRaises(HTTPException):await manual.quick_start(QuickStartRequest(job_id=draft['job_id'],config_digest=draft['config_digest'],request_key='guarded-quick-start1',confirmed=True))
  self.assertEqual(self.c.calls,[])
  self.assertIsNone(manual.task)
  record=manual.record(draft['job_id']);record['state']='paused'
  with self.ops.store.db:manual.save(record)
  self.assertFalse((await manual.status(draft['job_id']))['can_resume'])
  self.assertEqual((await manual.status(draft['job_id']))['sender_error'],'PeerFlood guard')

 async def test_protected_full_preview_and_draft_readable_but_valid_token_cannot_start(self):
  cfg=self.config();self.protect();manual=self.ops.manual_invite_campaign
  progress=await manual.preview(cfg)
  await asyncio.gather(*tuple(manual.preview_tasks))
  preview=await manual.preview_status(progress['preview_id']);self.assertEqual(preview['state'],'ready')
  draft=await manual.draft(DraftRequest(config=cfg,candidate_token=preview['candidate_token'],request_key='guarded-full-draft1'))
  with self.assertRaises(HTTPException):await manual.start(StartRequest(job_id=draft['job_id'],candidate_token=preview['candidate_token'],request_key='guarded-full-start1',confirmed=True))
  self.assertEqual(self.c.calls,[]);self.assertIsNone(manual.task)

 async def test_error_free_manual_pause_allows_explicit_quick_start(self):
  cfg=self.config();self.a.pause();manual=self.ops.manual_invite_campaign
  self.assertIsNone(self.ops.account_row(self.a.aid)['error'])
  draft=await manual.quick_draft(QuickDraftRequest(config=cfg,request_key='ordinary-pause-draft1'))
  self.assertIsNone(draft['sender_error'])
  await manual.quick_start(QuickStartRequest(job_id=draft['job_id'],config_digest=draft['config_digest'],request_key='ordinary-pause-start1',confirmed=True))
  await manual.task
  result=await manual.status(draft['job_id'])
  self.assertEqual(result['counts']['added'],1,result)
  self.assertEqual(self.ops.account_row(self.a.aid)['enabled'],0)

 async def test_saved_error_still_blocks_even_when_enabled_and_runtime_only_error_blocks(self):
  cfg=self.config();manual=self.ops.manual_invite_campaign
  draft=await manual.quick_draft(QuickDraftRequest(config=cfg,request_key='saved-error-draft1'))
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',('Stored Telegram error',self.a.aid))
  request=QuickStartRequest(job_id=draft['job_id'],config_digest=draft['config_digest'],request_key='saved-error-start1',confirmed=True)
  with self.assertRaises(HTTPException):await manual.quick_start(request)
  with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET error=NULL WHERE id=?',(self.a.aid,))
  self.a.error='Current Telegram error'
  with self.assertRaises(HTTPException):await manual.quick_start(request)
  self.assertEqual(self.c.calls,[])

 async def test_explicit_restore_clears_public_error_then_separate_start_is_needed(self):
  cfg=self.config();self.protect();manual=self.ops.manual_invite_campaign
  draft=await manual.quick_draft(QuickDraftRequest(config=cfg,request_key='restore-draft-0001'))
  record=manual.record(draft['job_id']);record['state']='paused'
  with self.ops.store.db:manual.save(record)
  await self.ops.resume_secondary_outbound(self.a.aid,ResumeOutbound(confirmed=True,telegram_id=100,reason='Owner separately reviewed stop'))
  result=await manual.status(draft['job_id'])
  self.assertIsNone(result['sender_error']);self.assertTrue(result['can_resume'])
  self.assertIsNone(manual.task);self.assertEqual(self.c.calls,[])

if __name__=='__main__':unittest.main()
