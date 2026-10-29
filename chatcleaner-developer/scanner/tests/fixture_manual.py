import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
"""Native SQLite + fake Telegram behavioral regression tests; no network."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import ValidationError
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from operations import Operations, Control, SingleAttempt, RetryInvite
from manual_invite_campaign import ManualConfig, DraftRequest, StartRequest, PauseRequest, PREFIX, CLAIM_PREFIX
SENDER = 710001001
TARGET = -1000710001002

class Client:
    _request_retries = 0
    flood_sleep_threshold = 0

    def __init__(self):
        self.me = types.User(id=SENDER, username='demo_sender')
        self.group = types.Channel(id=710001002, title='Chosen managed Dubai group', username='demo_target', megagroup=True, photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), admin_rights=types.ChatAdminRights(invite_users=True))
        self.users = [types.User(id=2234567800 + i, username='person_' + str(i), first_name='PRIVATE', access_hash=9911 + i, contact=False, mutual_contact=False) for i in range(30)]
        self.members = set()
        self.calls = []
        self.failures = {}
        self.invite_hook = None
        self.read_hook = None
        self.ghost = False
        self.connect = AsyncMock(side_effect=AssertionError('no connect'))
        self.disconnect = AsyncMock(side_effect=AssertionError('no disconnect'))
        self.send_message = AsyncMock(side_effect=AssertionError('no messages'))

    def is_connected(self):
        return True

    async def get_me(self):
        return self.me

    async def iter_dialogs(self, limit):
        yield SimpleNamespace(entity=self.group)

    async def get_entity(self, target):
        if self.read_hook:
            await self.read_hook()
        if target == TARGET:
            return self.group
        if isinstance(target, types.InputPeerUser):
            return next((u for u in self.users if u.id == target.user_id))
        return next((u for u in self.users if u.username == target))

    async def get_permissions(self, group, user):
        if user.id not in self.members:
            raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChannelParticipant(user_id=user.id, date=datetime.now(timezone.utc)), False)

    async def __call__(self, request):
        self.calls.append(request)
        if not isinstance(request, functions.channels.InviteToChannelRequest):
            raise AssertionError('Only explicit invite RPC; contacts and messages forbidden')
        user = request.users[0]
        if self.invite_hook:
            await self.invite_hook()
        if user.id in self.failures:
            raise self.failures[user.id]
        if not self.ghost:
            self.members.add(user.id)
        return types.messages.InvitedUsers(types.Updates([], [], [], datetime.now(timezone.utc), 0), [])

class ManualFixture(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.wall = time.time()
        self.clock = patch('time.time', side_effect=lambda: self.wall)
        self.clock.start()
        self.client = Client()
        self.runtime = SimpleNamespace(client=self.client, me=self.client.me)
        self.ops = Operations(self.tmp.name, lambda: None, lambda: self.runtime)
        self.ops.initialize()
        self.manual = self.ops.manual_invite_campaign
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0,error='Historical protection preserved' WHERE id='primary'")
            for n, state in enumerate(('paused', 'cancelled')):
                self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)', ('old-' + str(n), 'old', 'primary', SENDER, 'invite', 'Old never resume', '{"daily_limit":20}', state, '2026-01-01', '2026-01-01'))
            self.ops.store.db.execute('INSERT INTO settings VALUES(?,?)', ('isolated_invite_day_old', '{"state":"completed","preserve":true}'))
        self.old = self.manual.projection_for(SENDER)
        self.app = FastAPI()
        self.app.include_router(self.ops.router)
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test', headers={'x-panel-user': 'owner', 'x-panel-role': 'owner'})

    async def asyncTearDown(self):
        await self.ops.shutdown()
        await self.http.aclose()
        self.clock.stop()
        self.client.connect.assert_not_awaited()
        self.client.disconnect.assert_not_awaited()
        self.client.send_message.assert_not_awaited()
        self.tmp.cleanup()

    def config(self, count=25, **extra):
        return ManualConfig(account='primary', telegram_id=SENDER, target_id=TARGET, target_username='demo_target', users=['@' + u.username for u in self.client.users[:count]], consent_note='Первые 25 человек одобрили добавление в выбранную группу.', consent_confirmed=True, **dict(max_attempts=count, account_day_attempt_cap=count, interval_seconds=1, **extra))

    async def ready(self, cfg=None, jid=None):
        result = await self.manual.preview(cfg or self.config()) if jid is None else await self.manual.preview_job(jid)
        self.assertEqual(result['state'], 'checking')
        while self.manual.preview_tasks:
            await asyncio.gather(*tuple(self.manual.preview_tasks))
        result = await self.manual.preview_status(result['preview_id'])
        self.assertEqual(result['state'], 'ready', result)
        return result

    async def saved(self, cfg=None):
        cfg = cfg or self.config()
        preview = await self.ready(cfg)
        saved = await self.manual.draft(DraftRequest(config=cfg, candidate_token=preview['candidate_token'], request_key='draft-request-0001'))
        return (preview, saved)

    async def run_worker(self):
        for _ in range(300):
            if self.manual.task.done():
                await self.manual.task
                return
            await asyncio.sleep(0.001)
            if self.manual.active:
                self.wall = max(self.wall, self.manual.active['record']['next_at'] + 0.02)
                self.manual.wake.set()
        self.fail('worker did not stop')

    async def start(self, preview, saved, key='start-request-0001'):
        spec = StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key=key, confirmed=True)
        await self.manual.start(spec)
        await self.run_worker()
        return (await self.manual.status(saved['job_id']), spec)

    def isolated(self, jid):
        self.assertEqual(self.manual.projection_for(SENDER, jid), self.old)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertEqual(self.ops.account_row('primary')['error'], 'Historical protection preserved')
        self.assertEqual(self.ops.store.get('isolated_invite_day_old'), {'state': 'completed', 'preserve': True})