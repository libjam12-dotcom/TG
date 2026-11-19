"""Native SQLite + fake Telegram behavioral regression tests; no network."""
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

CANDIDATE = Path(__file__).resolve().parents[2]
ROOT = CANDIDATE.parents[1]
sys.path.insert(0, os.environ.get('MANUAL_TEST_SCANNER', str(CANDIDATE / 'scanner')))
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import ValidationError
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from operations import Operations, Control, SingleAttempt, RetryInvite
from manual_invite_campaign import ManualConfig, DraftRequest, StartRequest, PauseRequest, PREFIX, CLAIM_PREFIX

SENDER = 5550001001
TARGET = -1001234501001


class Client:
    _request_retries = 0
    flood_sleep_threshold = 0

    def __init__(self):
        self.me = types.User(id=SENDER, username='sender_test_account')
        self.group = types.Channel(id=1234501001, title='Chosen managed Dubai group', username='target_test_group', megagroup=True,
            photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), admin_rights=types.ChatAdminRights(invite_users=True))
        self.users = [types.User(id=2234567800+i, username='person_'+str(i), first_name='PRIVATE', access_hash=9911+i,
            contact=False, mutual_contact=False) for i in range(30)]
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
            return next(u for u in self.users if u.id == target.user_id)
        return next(u for u in self.users if u.username == target)

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


class ManualTests(unittest.IsolatedAsyncioTestCase):
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
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0,error=NULL WHERE id='primary'")
            for n, state in enumerate(('paused', 'cancelled')):
                self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('old-'+str(n), 'old', 'primary', SENDER, 'invite', 'Old never resume', '{"daily_limit":20}', state, '2026-01-01', '2026-01-01'))
            self.ops.store.db.execute('INSERT INTO settings VALUES(?,?)', ('isolated_invite_day_old', '{"state":"completed","preserve":true}'))
        self.old = self.manual.projection_for(SENDER)
        self.app = FastAPI()
        self.app.include_router(self.ops.router)
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test',
            headers={'x-panel-user':'owner', 'x-panel-role':'owner'})

    async def asyncTearDown(self):
        await self.ops.shutdown()
        await self.http.aclose()
        self.clock.stop()
        self.client.connect.assert_not_awaited()
        self.client.disconnect.assert_not_awaited()
        self.client.send_message.assert_not_awaited()
        self.tmp.cleanup()

    def config(self, count=25, **extra):
        return ManualConfig(account='primary', telegram_id=SENDER, target_id=TARGET, target_username='target_test_group',
            users=['@'+u.username for u in self.client.users[:count]], consent_note='Первые 25 человек одобрили добавление в выбранную группу.',
            consent_confirmed=True, **dict(max_attempts=count, account_day_attempt_cap=count, interval_seconds=1, **extra))

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
        return preview, saved

    async def run_worker(self):
        # Advance only this synthetic clock; wake the finite worker after each step.
        for _ in range(300):
            if self.manual.task.done():
                await self.manual.task
                return
            await asyncio.sleep(.001)
            if self.manual.active:
                self.wall = max(self.wall, self.manual.active['record']['next_at'] + .02)
                self.manual.wake.set()
        self.fail('worker did not stop')

    async def start(self, preview, saved, key='start-request-0001'):
        spec = StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key=key, confirmed=True)
        await self.manual.start(spec)
        await self.run_worker()
        return await self.manual.status(saved['job_id']), spec

    def isolated(self, jid):
        self.assertEqual(self.manual.projection_for(SENDER, jid), self.old)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertIsNone(self.ops.account_row('primary')['error'])
        self.assertEqual(self.ops.store.get('isolated_invite_day_old'), {'state':'completed','preserve':True})

    async def test_manual25_noncontacts_draft_and_verified_native_history(self):
        preview, saved = await self.saved()
        self.assertEqual(len(preview['selected']), 25)
        self.assertTrue(all(not u['sender_has_contact'] and not u['mutual_contact'] for u in preview['selected']))
        self.assertEqual(self.client.calls, [])
        self.assertEqual(saved['state'], 'draft')
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 25, result)
        self.assertEqual(result['counts']['attempts'], 25)
        self.assertEqual(len(self.client.calls), 25)
        self.assertTrue(all(r.channel is self.client.group for r in self.client.calls))
        self.assertEqual(len(self.ops.store.rows('SELECT key FROM settings WHERE key LIKE ?', (CLAIM_PREFIX+'%',))), 25)
        self.assertTrue(all(i['diagnosis']['member_before']['member'] is False and i['diagnosis']['member_after']['member'] is True for i in result['items']))
        native = await self.ops.detail(saved['job_id'])
        self.assertEqual(len(native['items']), 25)
        self.assertFalse(native['retryable'])
        self.isolated(saved['job_id'])

    async def test_changed_cap_consent_or_group_invalidates_preview_and_no_write(self):
        cfg = self.config(2)
        preview = await self.ready(cfg)
        for change in ({'max_attempts':1}, {'interval_seconds':2}, {'consent_note':'Другое подтверждение согласия'}, {'target_id':-1001234502002}):
            with self.assertRaises(HTTPException):
                await self.manual.draft(DraftRequest(config=ManualConfig(**dict(cfg.model_dump(), **change)), candidate_token=preview['candidate_token'], request_key='changed-request-0001'))
        self.assertEqual(self.client.calls, [])

    async def test_already_member_does_not_count_goal(self):
        self.client.members.add(self.client.users[0].id)
        cfg = self.config(3, success_goal=1)
        preview, saved = await self.saved(cfg)
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['already_member'], 1)
        self.assertEqual(result['counts']['pending'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(result['stop_reason'], 'verified_goal_reached')

    async def test_privacy_skips_to_next_candidate_and_claims_once(self):
        self.client.failures[self.client.users[0].id] = errors.UserPrivacyRestrictedError(None)
        preview, saved = await self.saved(self.config(3))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['restricted'], 1)
        self.assertEqual(result['counts']['added'], 2)
        self.assertEqual(result['counts']['attempts'], 3)
        self.assertEqual(len(self.client.calls), 3)

    async def test_unknown_after_rpc_stops_and_no_replay_on_resume(self):
        self.client.failures[self.client.users[0].id] = RuntimeError('unknown transport result')
        preview, saved = await self.saved(self.config(3))
        result, spec = await self.start(preview, saved)
        self.assertEqual(result['counts']['uncertain'], 1)
        self.assertEqual(result['counts']['pending'], 2)
        self.assertEqual(result['state'], 'stopped')
        await self.manual.start(spec)
        self.assertEqual(len(self.client.calls), 1)
        self.wall = result['next_at'] + .02
        preview = await self.ready(jid=saved['job_id'])
        result, _ = await self.start(preview, saved, 'resume-request-0002')
        self.assertEqual(result['counts']['added'], 2)
        self.assertEqual(len(self.client.calls), 3)
        self.assertEqual(sum(r.users[0].id == self.client.users[0].id for r in self.client.calls), 1)

    async def test_peerflood_stops_and_preserves_protection(self):
        self.client.failures[self.client.users[0].id] = errors.PeerFloodError(None)
        preview, saved = await self.saved(self.config(3))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(result['counts']['pending'], 2)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary'), 1)

    async def test_floodwait_stops_and_retains_server_delay(self):
        self.client.failures[self.client.users[0].id] = errors.FloodWaitError(None, capture=600)
        preview, saved = await self.saved(self.config(3))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(len(self.client.calls), 1)
        self.assertGreaterEqual(self.ops.account_row('primary')['not_before'], self.wall + 590)

    async def test_revoked_session_stops_no_next_candidate(self):
        self.client.failures[self.client.users[0].id] = errors.SessionRevokedError(None)
        preview, saved = await self.saved(self.config(3))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary'), 1)

    async def test_preview_progress_get_draft_status_never_invite(self):
        r = await self.http.get('/api/scanner/operations/manual-invite-campaign/destinations', params={'account':'primary','telegram_id':SENDER})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()['destinations'][0]['id'], TARGET)
        self.assertTrue(r.json()['outbound_paused'])
        preview, saved = await self.saved(self.config(2))
        self.client.get_entity = AsyncMock(side_effect=AssertionError('GET must be cached only'))
        for path in ('/previews/'+preview['preview_id'], '/status?job_id='+str(saved['job_id'])):
            r = await self.http.get('/api/scanner/operations/manual-invite-campaign'+path)
            self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.client.calls, [])
        denied = await self.http.get('/api/scanner/operations/manual-invite-campaign/status?job_id='+str(saved['job_id']), headers={'x-panel-role':'viewer'})
        self.assertEqual(denied.status_code, 403)
        self.isolated(saved['job_id'])

    async def test_async_preview_returns_before_remote_reads_and_reports_error(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow_read():
            entered.set()
            await release.wait()
        self.client.read_hook = slow_read
        p = await self.manual.preview(self.config(2))
        self.assertEqual(p['checked'], 0)
        await entered.wait()
        r = await self.manual.preview_status(p['preview_id'])
        self.assertEqual(r['state'], 'checking')
        self.assertEqual(self.client.calls, [])
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=1 WHERE id='primary'")
        release.set()
        await asyncio.gather(*tuple(self.manual.preview_tasks))
        r = await self.manual.preview_status(p['preview_id'])
        self.assertEqual(r['state'], 'failed')
        self.assertEqual(self.client.calls, [])

    async def test_sender_change_pause_revision_or_old_job_change_invalidates_start(self):
        preview, saved = await self.saved(self.config(2))
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE request_key='old-1'")
        with self.assertRaises(HTTPException):
            await self.manual.start(StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key='stale-start-00001', confirmed=True))
        self.assertEqual(self.client.calls, [])

    async def test_frozen_cohort_tamper_prevents_new_preview(self):
        _, saved = await self.saved(self.config(2))
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_items SET user_id=? WHERE job=?', (self.client.users[5].id, saved['job_id']))
        p = await self.manual.preview_job(saved['job_id'])
        await asyncio.gather(*tuple(self.manual.preview_tasks))
        r = await self.manual.preview_status(p['preview_id'])
        self.assertEqual(r['state'], 'failed')
        self.assertEqual(self.client.calls, [])

    async def test_pause_resume_only_pending_never_autostart(self):
        preview, saved = await self.saved(self.config(3))
        spec = StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key='pause-start-00001', confirmed=True)
        await self.manual.start(spec)
        for _ in range(100):
            await asyncio.sleep(.001)
            if self.client.calls:
                break
        result = await self.manual.pause(PauseRequest(job_id=saved['job_id']))
        self.manual.wake.set()
        await self.manual.task
        self.assertTrue(result['pause_requested'])
        self.assertEqual((await self.manual.status(saved['job_id']))['state'], 'paused')
        self.assertEqual(len(self.client.calls), 1)
        self.wall = result['next_at'] + .02
        p = await self.ready(jid=saved['job_id'])
        result, _ = await self.start(p, saved, 'pause-resume-0002')
        self.assertEqual(result['counts']['added'], 3)
        self.assertEqual(len(self.client.calls), 3)
        self.isolated(saved['job_id'])

    async def test_pause_immediate_ack_during_rpc_preserves_current_result_and_blocks_next(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow_rpc():
            entered.set()
            await release.wait()
        self.client.invite_hook = slow_rpc
        preview, saved = await self.saved(self.config(3))
        await self.manual.start(StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key='fast-pause-00001', confirmed=True))
        await entered.wait()
        paused = await asyncio.wait_for(self.manual.pause(PauseRequest(job_id=saved['job_id'])), .1)
        self.assertTrue(paused['pause_requested'])
        self.assertEqual(paused['state'], 'running')
        self.assertEqual(paused['counts']['sending'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.assertTrue(self.manual.record(saved['job_id'])['pause_requested'])
        release.set()
        await self.manual.task
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['pending'], 2)
        self.assertEqual(result['counts'].get('uncertain', 0), 0)
        self.assertEqual(len(self.client.calls), 1)
        self.assertFalse(result['items'][0]['diagnosis']['request_outcome_unknown'])
        self.isolated(saved['job_id'])

    async def test_pause_during_preflight_keeps_pending_item_and_sends_nothing(self):
        preview, saved = await self.saved(self.config(2))
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow_read():
            entered.set()
            await release.wait()
        self.client.read_hook = slow_read
        await self.manual.start(StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key='preflight-pause1', confirmed=True))
        await entered.wait()
        paused = await asyncio.wait_for(self.manual.pause(PauseRequest(job_id=saved['job_id'])), .1)
        self.assertTrue(paused['pause_requested'])
        release.set()
        await self.manual.task
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['counts']['pending'], 2)
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(self.client.calls, [])
        self.isolated(saved['job_id'])

    async def test_startup_interrupts_manual_record_without_worker_and_old_jobs_unchanged(self):
        _, saved = await self.saved(self.config(2))
        record = self.manual.record(saved['job_id'])
        record['state'] = 'running'
        record['pause_requested'] = True
        with self.ops.store.db:
            self.manual.save(record)
        self.manual.initialize()
        self.assertEqual(self.manual.record(saved['job_id'])['state'], 'interrupted')
        self.assertTrue(self.manual.record(saved['job_id'])['pause_requested'])
        self.assertIsNone(self.manual.task)
        self.assertEqual(self.client.calls, [])
        self.isolated(saved['job_id'])

    async def test_account_day_counts_all_actions_same_uid_and_not_just_this_job(self):
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,state,random_ids,attempted_at) VALUES(?,?,?,?,?,?,?)',
                (1, TARGET, 'old', 999, 'restricted', '[]', datetime.now(timezone.utc).isoformat()))
            self.ops.store.db.execute("INSERT INTO op_accounts(id,label,created) VALUES('alias','other old identity record','old')")
            row = self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                ('same-uid-job', 'old', 'alias', SENDER, 'message', 'other action', '{"daily_limit":20}', 'paused', 'old', 'old'))
            self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,state,random_ids,attempted_at) VALUES(?,?,?,?,?,?,?)',
                (row.lastrowid, 123, 'old', 999, 'sent', '[]', datetime.now(timezone.utc).isoformat()))
        self.old = self.manual.projection_for(SENDER)
        preview, saved = await self.saved(self.config(3))
        self.assertEqual(preview['account_attempts_today'], 2)
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['stop_reason'], 'account_day_attempt_cap_reached')
        self.assertEqual(len(self.client.calls), 1)
        self.isolated(saved['job_id'])

    async def test_generic_control_single_pilot_retry_and_execute_reject_manual_job(self):
        _, saved = await self.saved(self.config(2))
        job = await self.ops.detail(saved['job_id'])
        with self.assertRaises(HTTPException):
            await self.ops.control(saved['job_id'], Control(action='start', confirmed=True))
        with self.assertRaises(HTTPException):
            await self.ops.pilot_preview(saved['job_id'])
        with self.assertRaises(HTTPException):
            await self.ops.single_preview(saved['job_id'], saved['items'][0]['id'])
        with self.assertRaises(HTTPException):
            await self.ops.execute(self.ops.accounts['primary'], job, saved['items'][0])
        self.assertEqual(self.client.calls, [])
        self.isolated(saved['job_id'])

    async def test_explicit_secondary_selection_is_respected_and_primary_untouched(self):
        second = Client()
        second.me = types.User(id=5550002001, username='secondary_chosen')
        second_runtime = SimpleNamespace(client=second, me=second.me, error=None)
        self.ops.accounts['selected-secondary'] = second_runtime
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_accounts(id,label,telegram_id,username,enabled,created) VALUES(?,?,?,?,?,?)',
                ('selected-secondary', 'Chosen secondary', second.me.id, second.me.username, 1, 'old'))
        cfg = ManualConfig(**dict(self.config(2).model_dump(), account='selected-secondary', telegram_id=second.me.id))
        preview, saved = await self.saved(cfg)
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 2)
        self.assertEqual(result['account'], 'selected-secondary')
        self.assertEqual(result['telegram_id'], second.me.id)
        self.assertEqual(len(second.calls), 2)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.manual.projection_for(SENDER), self.old)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertEqual(self.ops.account_row('selected-secondary')['enabled'], 1)
        self.ops.accounts.pop('selected-secondary')

    async def test_pause_revision_change_between_reads_prevents_rpc(self):
        preview, saved = await self.saved(self.config(2))
        changed = False
        async def change_revision():
            nonlocal changed
            if not changed:
                changed = True
                self.ops.store.put('outbound_pause_revision_primary', 1)
        self.client.read_hook = change_revision
        result, _ = await self.start(preview, saved)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(result['state'], 'stopped')

    async def test_persistent_claim_exists_before_rpc_and_cancel_never_retries(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def blocked_rpc():
            item = self.ops.store.rows("SELECT * FROM op_items WHERE state='sending'")[0]
            claim = self.ops.store.get(CLAIM_PREFIX + str(item['id']))
            self.assertEqual(claim['user_id'], item['user_id'])
            self.assertEqual(claim['telegram_id'], SENDER)
            self.assertIsNotNone(item['attempted_at'])
            entered.set()
            await release.wait()
        self.client.invite_hook = blocked_rpc
        preview, saved = await self.saved(self.config(2))
        await self.manual.start(StartRequest(job_id=saved['job_id'], candidate_token=preview['candidate_token'], request_key='cancel-start-001', confirmed=True))
        await entered.wait()
        await self.manual.shutdown()
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['counts']['uncertain'], 1)
        self.assertEqual(result['state'], 'interrupted')
        self.assertEqual(len(self.client.calls), 1)
        self.assertIsNotNone(self.ops.store.get(CLAIM_PREFIX + str(result['items'][0]['id'])))
        self.manual.initialize()
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)

    async def test_generic_tick_quarantines_injected_manual_queue_state(self):
        _, saved = await self.saved(self.config(2))
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=1 WHERE id='primary'")
            self.ops.store.db.execute("UPDATE op_jobs SET state='queued' WHERE id=?", (saved['job_id'],))
        await self.ops.tick()
        self.assertEqual(self.manual.job(saved['job_id'])['state'], 'paused')
        self.assertEqual(self.client.calls, [])

    async def test_username_resolution_must_match_approved_name(self):
        original = self.client.get_entity
        async def swapped(target):
            if target == 'person_0':
                return self.client.users[1]
            return await original(target)
        self.client.get_entity = swapped
        progress = await self.manual.preview(self.config(1))
        await asyncio.gather(*tuple(self.manual.preview_tasks))
        result = await self.manual.preview_status(progress['preview_id'])
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(self.client.calls, [])

    async def test_full_configured_interval_and_long_title_save_without_changing_old_policy(self):
        self.client.group.title = 'Very long managed group title ' * 8
        cfg = ManualConfig(**dict(self.config(2).model_dump(), interval_seconds=86400, account_day_attempt_cap=1000, max_attempts=1000))
        preview, saved = await self.saved(cfg)
        self.assertEqual(saved['config']['interval_seconds'], 86400)
        job = self.manual.job(saved['job_id'])
        self.assertEqual(job['spec']['delay_seconds'], 86400)
        self.assertEqual(job['spec']['daily_limit'], 20)
        self.assertLessEqual(len(job['title']), 100)
        self.assertEqual(self.client.calls, [])
        self.isolated(saved['job_id'])

    async def test_validation_bounds_explicit_consent_and_duplicate_list(self):
        cfg = self.config(2).model_dump()
        for extra in ({'max_attempts':0}, {'interval_seconds':0}, {'account_day_attempt_cap':1001}, {'success_goal':3},
            {'consent_confirmed':'true'}, {'retry_after_change_confirmed':'true'}, {'users':['person_1','PERSON_1']}, {'users':['12345']}):
            with self.assertRaises(ValidationError):
                ManualConfig(**dict(cfg, **extra))
        valid = ManualConfig(**dict(cfg, interval_seconds=86400, max_attempts=1000, account_day_attempt_cap=1000))
        self.assertEqual(valid.interval_seconds, 86400)


    def history_item(self, state='already_member', diagnosis=None, kind='invite', marker=True):
        uid = self.client.users[0].id
        jid = self.ops.store.rows('SELECT id FROM op_jobs ORDER BY id')[0]['id']
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET kind=? WHERE id=?', (kind, jid))
            self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids,diagnosis,attempted_at) VALUES(?,?,?,?,?,?,?,?,?)',
                (jid, TARGET, 'Group', uid, self.client.users[0].username, state, '[]', json.dumps(diagnosis or {}),
                 '2026-01-01T00:00:00+00:00' if state not in ('already_member', 'duplicate_skipped') else None))
            if marker:
                self.ops.store.db.execute('INSERT OR REPLACE INTO op_invited VALUES(?,?,?,?)', (TARGET, uid, state, jid))
        return jid

    async def test_history_current_member_is_classified_from_fresh_membership(self):
        self.history_item()
        self.client.members.add(self.client.users[0].id)
        preview, saved = await self.saved(self.config(1))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['items'][0]['state'], 'already_member')
        self.assertTrue(result['items'][0]['diagnosis']['member_before']['member'])
        self.assertEqual(self.client.calls, [])

    async def test_absent_historical_member_requires_separate_retry_confirmation(self):
        self.history_item()
        preview, saved = await self.saved(self.config(1))
        result, _ = await self.start(preview, saved)
        d = result['items'][0]['diagnosis']
        self.assertEqual(d['code'], 'target_history_exists')
        self.assertFalse(d['member_before']['member'])
        self.assertTrue(d['retry_after_change'])
        self.assertIn('подтвердите', d['next_action'])
        self.assertEqual(self.client.calls, [])

    async def test_confirmed_retry_after_leave_preserves_history_and_claims_once(self):
        old = self.history_item('invited')
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.store.rows('SELECT job,state FROM op_invited')[0], {'job':old, 'state':'invited'})
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_items WHERE job=?', (old,))[0]['state'], 'invited')

    async def test_reconfirmed_retry_with_known_member_privacy_success_and_unattempted_skips(self):
        self.history_item('already_member')
        self.history_item('restricted', {'code':'UserNotMutualContactError', 'retry_after_change':True})
        self.history_item('invited', {'code':'invite_member_verified', 'retry_after_change':False})
        self.history_item('duplicate_skipped', marker=False)
        self.history_item('duplicate_skipped', marker=False)
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_invited')[0]['state'], 'invited')

    async def test_confirmed_retry_after_privacy_change(self):
        self.history_item('restricted', {'code':'UserPrivacyRestrictedError', 'retry_after_change':True})
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(len(self.client.calls), 1)

    async def test_confirmation_never_unlocks_uncertain_or_inflight_history(self):
        for state in ('uncertain', 'sending', 'failed'):
            with self.subTest(state=state):
                self.history_item(state)
                self.assertEqual(self.manual.history_retry_blocker(TARGET, self.client.users[0].id), 'target_history_unresolved')
        # Switching sender cannot bypass ambiguous target history.
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET telegram_id=? WHERE request_key=?', (SENDER+1, 'old-0'))
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['items'][0]['diagnosis']['code'], 'target_history_unresolved')
        self.assertFalse(result['items'][0]['diagnosis']['member_before']['member'])
        self.assertFalse(result['items'][0]['diagnosis']['retry_after_change'])
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(self.client.calls, [])

    async def test_confirmation_never_unlocks_marker_only_unknown_restriction(self):
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?)', (TARGET, self.client.users[0].id, 'restricted', 999))
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['items'][0]['diagnosis']['code'], 'target_history_unresolved')
        self.assertEqual(self.client.calls, [])

    async def test_preflight_rechecks_membership_after_confirmed_preview(self):
        self.history_item()
        preview, saved = await self.saved(self.config(1, retry_after_change_confirmed=True))
        self.client.members.add(self.client.users[0].id)
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['already_member'], 1)
        self.assertEqual(self.client.calls, [])

    async def test_other_operation_history_does_not_block_invite(self):
        self.history_item('uncertain', kind='message', marker=False)
        preview, saved = await self.saved(self.config(1))
        result, _ = await self.start(preview, saved)
        self.assertEqual(result['counts']['added'], 1)

if __name__ == '__main__':
    unittest.main()
