"""Offline reproduction of reconnect erasing an existing outbound pause."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import time
import unittest

SOURCE=Path(__file__).resolve().parents[1]
BACKEND=SOURCE
sys.path.insert(0,str(SOURCE))
import operations
import manual_invite_campaign
# Fixtures use an in-memory DB, fake synthetic session bytes and AsyncMock client.
spec=importlib.util.spec_from_file_location('pool_connect_fixture',BACKEND/'tests/test_pool_account_connect.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)

class PauseProtection(fixture.PoolConnectTests):
 def protect(self,error='PeerFlood guard',revision=8):
  with self.store.db:
   self.store.db.execute('ALTER TABLE op_accounts ADD COLUMN not_before REAL NOT NULL DEFAULT 0')
   self.store.db.execute('UPDATE op_accounts SET enabled=0,error=?,not_before=? WHERE id=?',(error,time.time()+900,'pool'))
  self.store.config['outbound_pause_revision_pool']=revision
  self.a.error=error
  return self.ops.account_row('pool')

 async def test_reconnect_existing_paused_identity_preserves_error_cooldown_revision_and_queue(self):
  before=self.protect()
  config=json.dumps(self.store.config,sort_keys=True)
  await self.a.connect()
  after=self.ops.account_row('pool')
  self.assertEqual(after['enabled'],0,'Readiness must not grant outbound permission')
  self.assertEqual(after['error'],before['error'])
  self.assertEqual(self.a.error,before['error'])
  self.assertEqual(after['not_before'],before['not_before'])
  self.assertEqual(json.dumps(self.store.config,sort_keys=True),config)
  self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)
  self.assertEqual(self.store.rows('SELECT * FROM op_items ORDER BY id'),self.items)
  self.assertEqual(Path(self.a.session_path).read_bytes(),self.session_bytes)
  self.assertEqual(self.a.me.id,100)
  self.assertEqual(self.a.status,'connected')

 async def test_pause_arriving_while_get_me_awaits_is_not_erased(self):
  entered,release=asyncio.Event(),asyncio.Event()
  async def delayed():
   entered.set();await release.wait();return fixture.types.User(id=100,username='synthetic')
  self.client.get_me.side_effect=delayed
  task=asyncio.create_task(self.a.connect())
  await entered.wait()
  before=self.protect(error='SessionRevokedError preserved',revision=9)
  release.set();await task
  after=self.ops.account_row('pool')
  self.assertEqual(after['enabled'],0)
  self.assertEqual(after['error'],before['error'])
  self.assertEqual(after['not_before'],before['not_before'])
  self.assertEqual(self.store.config['outbound_pause_revision_pool'],9)
  self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)

 async def test_existing_enabled_account_keeps_floodwait_error_and_deadline(self):
  self.protect(error='FloodWait saved',revision=8)
  with self.store.db:self.store.db.execute('UPDATE op_accounts SET enabled=1 WHERE id=?',('pool',))
  before=self.ops.account_row('pool')
  await self.a.connect()
  after=self.ops.account_row('pool')
  # Existing known protective errors remain sticky even after a successful read.
  self.assertEqual(after['enabled'],0)
  self.assertEqual(after['error'],before['error'])
  self.assertEqual(after['not_before'],before['not_before'])
  self.assertEqual(self.store.config['outbound_pause_revision_pool'],8)

 async def test_runtime_only_protection_is_sticky_and_persisted_for_known_identity(self):
  self.protect(error=None,revision=8)
  with self.store.db:self.store.db.execute('UPDATE op_accounts SET enabled=1,error=NULL WHERE id=?',('pool',))
  self.a.error='Telegram: PeerFloodError.'
  before=self.ops.account_row('pool');config=json.dumps(self.store.config,sort_keys=True)
  await self.a.connect()
  after=self.ops.account_row('pool')
  self.assertEqual(after['enabled'],0)
  self.assertEqual(after['error'],'Telegram: PeerFloodError.')
  self.assertEqual(self.a.error,'Telegram: PeerFloodError.')
  self.assertEqual(after['not_before'],before['not_before'])
  self.assertEqual(json.dumps(self.store.config,sort_keys=True),config)
  self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)
  self.assertEqual(self.store.rows('SELECT * FROM op_items ORDER BY id'),self.items)
  self.assertEqual(Path(self.a.session_path).read_bytes(),self.session_bytes)
  self.assertEqual(self.a.me.id,100);self.assertEqual(self.a.status,'connected')

 async def test_first_authorized_identity_still_becomes_ready_and_enabled(self):
  with self.store.db:self.store.db.execute('UPDATE op_accounts SET telegram_id=NULL,enabled=0,error=NULL WHERE id=?',('pool',))
  await self.a.connect()
  row=self.ops.account_row('pool')
  self.assertEqual(row['telegram_id'],100)
  self.assertEqual(row['enabled'],1)
  self.assertIsNone(row['error'])
  self.assertEqual(self.a.status,'connected')

 async def test_existing_manual_pause_without_error_stays_paused(self):
  before=self.protect(error=None)
  await self.a.connect()
  row=self.ops.account_row('pool')
  self.assertEqual(row['enabled'],0)
  self.assertIsNone(row['error'])
  self.assertEqual(row['not_before'],before['not_before'])

if __name__=='__main__':unittest.main()
