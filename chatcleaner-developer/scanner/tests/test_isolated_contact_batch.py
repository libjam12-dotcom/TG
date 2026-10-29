"""Finite batch behaviour with synthetic existing contacts; no network."""
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

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(HERE))
from fastapi import FastAPI, HTTPException
import httpx
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from operations import Operations, Control
from isolated_invite import DAY_PREFIX as SINGLE_DAY, utc_day
from isolated_contact_batch import (IsolatedContactBatch, BatchStart, BatchPause, SCOPE,
    DAY_PREFIX, REQUEST_PREFIX, ITEM_PREFIX, ACCOUNT, SENDER, TARGET)


FIRST = 2234567800


def user(uid):
    return types.User(id=uid, username='contact_' + str(uid), first_name='PRIVATE_NAME',
        phone='PRIVATE_PHONE', access_hash=998877665544, contact=True, mutual_contact=True)


class Client:
    _request_retries = 0
    flood_sleep_threshold = 0

    def __init__(self, count):
        self.me = types.User(id=SENDER, username='demo_identity_1')
        self.group = types.Channel(id=720000000, title='ТЕСТОВОЕ СООБЩЕСТВО', megagroup=True,
            photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), username='demo_identity_6',
            admin_rights=types.ChatAdminRights(invite_users=True))
        self.users = [user(FIRST + i) for i in range(count)]
        self.saved = {u.id for u in self.users}
        self.members = set()
        self.calls = []
        self.invite_times = []
        self.failure = None
        self.invite_hook = None
        self.identity_hook = None
        self.ghost = False
        self.service_event = False
        self.member_reads = []
        self.gets = 0
        self.connect = AsyncMock(side_effect=AssertionError('No connect'))
        self.disconnect = AsyncMock(side_effect=AssertionError('No disconnect'))
        self.send_message = AsyncMock(side_effect=AssertionError('No messages'))

    def is_connected(self):
        return True

    async def get_me(self):
        if self.identity_hook:
            await self.identity_hook()
        return self.me

    async def get_entity(self, target):
        if target != TARGET:
            raise AssertionError('Fixed target only')
        return self.group

    async def get_permissions(self, group, person):
        self.member_reads.append(person.id)
        if person.id not in self.members:
            raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChannelParticipant(user_id=person.id, date=datetime.now(timezone.utc)), False)

    async def __call__(self, request):
        self.calls.append(request)
        if isinstance(request, functions.contacts.GetContactsRequest):
            self.gets += 1
            return types.contacts.Contacts([types.Contact(uid, True) for uid in self.saved], len(self.saved), self.users)
        if isinstance(request, functions.users.GetUsersRequest):
            return [u for u in self.users if u.id == request.id[0].user_id]
        if not isinstance(request, functions.channels.InviteToChannelRequest):
            raise AssertionError('Other RPC prohibited')
        self.invite_times.append(time.time())
        if self.invite_hook:
            await self.invite_hook()
        if self.failure:
            raise self.failure
        recipient = request.users[0]
        if not self.ghost:
            self.members.add(recipient.id)
        updates = []
        if self.service_event:
            message = types.MessageService(id=777, peer_id=types.PeerChannel(720000000),
                date=datetime.now(timezone.utc), action=types.MessageActionChatAddUser([recipient.id]),
                from_id=types.PeerUser(SENDER))
            updates = [types.UpdateNewChannelMessage(message, 1, 1)]
        return types.messages.InvitedUsers(types.Updates(updates, [], [], datetime.now(timezone.utc), 0), [])


class BatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.wall = time.time()
        self.clock_patch = patch('time.time', side_effect=lambda: self.wall)
        self.clock_patch.start()
        self.client = Client(10)
        self.runtime = SimpleNamespace(client=self.client, me=self.client.me)
        self.ops = Operations(self.tmp.name, lambda: None, lambda: self.runtime)
        self.ops.initialize()
        self.batch = self.ops.isolated_contact_batch
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0,error='Historical PeerFlood preserved' WHERE id='primary'")
            spec = json.dumps({'daily_limit': 20, 'delay_seconds': 60})
            row = self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                ('earlier-single', 'a', ACCOUNT, SENDER, 'invite', 'Earlier verified single', spec, 'paused', utc_day(), utc_day()))
            self.single_job = row.lastrowid
            self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids,attempted_at,finished_at,diagnosis) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (self.single_job, TARGET, 'Existing target', 217678350, 'ALEXRLG', 'invited', '[]',
                 datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(), '{"code":"invite_member_verified"}'))
            self.ops.store.db.execute('INSERT INTO settings VALUES(?,?)', (SINGLE_DAY + utc_day(), '{"permanent_single_claim":"preserve"}'))
        self.initial_projection = self.batch.projection()
        self.single_claim = self.ops.store.get(SINGLE_DAY + utc_day())
        self.old_error = self.ops.account_row(ACCOUNT)['error']
        self.batch.wait_until_due = self.fast_wait

    async def asyncTearDown(self):
        await self.ops.shutdown()
        self.clock_patch.stop()
        self.client.connect.assert_not_awaited()
        self.client.disconnect.assert_not_awaited()
        self.client.send_message.assert_not_awaited()
        self.tmp.cleanup()

    async def fast_wait(self, ctx):
        self.wall = max(self.wall, ctx['record']['next_at'] + .02)
        await asyncio.sleep(0)

    def invites(self):
        return [r for r in self.client.calls if isinstance(r, functions.channels.InviteToChannelRequest)]

    async def prepared(self, key='batch-control-request-key-0001'):
        preview = await self.batch.preview(SCOPE)
        spec = BatchStart(scope=SCOPE, confirmed=True, reason='Owner explicitly requested ten additional existing mutual contacts',
            request_key=key, candidate_token=preview['candidate_token'])
        return preview, spec

    async def run_batch(self, spec):
        started = await self.batch.start(spec)
        await self.batch.task
        return started, await self.batch.status(SCOPE)

    def isolated(self, jid):
        self.assertEqual(self.batch.projection(jid), self.initial_projection)
        self.assertEqual(self.ops.account_row(ACCOUNT)['enabled'], 0)
        self.assertEqual(self.ops.account_row(ACCOUNT)['error'], self.old_error)
        self.assertEqual(self.ops.store.get(SINGLE_DAY + utc_day()), self.single_claim)

    async def test_ten_additional_contacts_shared_budget_includes_existing_single(self):
        with self.assertRaises(HTTPException):
            self.ops.online(ACCOUNT)
        preview, spec = await self.prepared()
        self.assertEqual(len(preview['selected']), 10)
        started, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'completed')
        self.assertEqual(status['counts']['added'], 10)
        self.assertEqual(status['attempts_claimed'], 10)
        self.assertEqual(self.ops.account_daily_count(ACCOUNT, utc_day()), 11)
        self.assertEqual(len(self.invites()), 10)
        self.assertEqual(len({r.users[0].id for r in self.invites()}), 10)
        self.assertTrue(all(b - a >= 60 for a, b in zip(self.client.invite_times, self.client.invite_times[1:])))
        self.isolated(status['job_id'])
        normal = await self.ops.detail(status['job_id'])
        self.assertEqual(normal['state'], 'paused')
        self.assertEqual(len(normal['items']), 10)
        self.assertEqual([i['state'] for i in normal['items']], ['invited'] * 10)

    async def test_preview_readonly_private_filter_history_and_maximum_thirty_checks(self):
        self.client.users = [user(217678350), user(710000010), user(FIRST), user(FIRST + 1)]
        self.client.users[2].username = None
        self.client.saved = {u.id for u in self.client.users}
        before = '\n'.join(self.ops.store.db.iterdump())
        preview = await self.batch.preview(SCOPE)
        self.assertEqual(len(preview['selected']), 1)
        self.assertEqual(preview['selected'][0]['user_id'], FIRST + 1)
        self.assertEqual(self.client.gets, 1)
        self.assertEqual(before, '\n'.join(self.ops.store.db.iterdump()))
        for value in ('PRIVATE_NAME', 'PRIVATE_PHONE', '998877665544', 'access_hash', 'contact_217678350'):
            self.assertNotIn(value, json.dumps(preview))
        self.batch.next_preview = 0
        self.client.users = [user(FIRST + i) for i in range(40)]
        self.client.saved = {u.id for u in self.client.users}
        self.client.members = self.client.saved.copy()
        self.client.member_reads.clear()
        preview = await self.batch.preview(SCOPE)
        self.assertEqual(preview['selected'], [])
        self.assertEqual(preview['membership_checks'], 30)

    async def test_preview_skips_explicit_exclusions_and_duplicate_contact_rows(self):
        from operation_workbench import ExclusionSpec
        await self.ops.workbench.add_exclusion(ExclusionSpec(request_key='batch-exclusion-request-0001',
            subject='user', identifier=str(FIRST), reason='Explicit exclusion'))
        self.client.users = [user(FIRST), user(FIRST + 1), user(FIRST + 1)]
        self.client.saved = {FIRST, FIRST + 1}
        preview = await self.batch.preview(SCOPE)
        self.assertEqual([r['user_id'] for r in preview['selected']], [FIRST + 1])
        self.assertEqual(preview['membership_checks'], 1)

    async def test_start_concurrency_and_replay_keep_one_job_one_worker_and_day_claim(self):
        _, spec = await self.prepared()
        first, replay = await asyncio.gather(self.batch.start(spec), self.batch.start(spec))
        self.assertEqual(first['job_id'], replay['job_id'])
        await self.batch.task
        again = await self.batch.start(spec)
        self.assertEqual(again['job_id'], first['job_id'])
        self.assertEqual(len(self.invites()), 10)
        with self.assertRaises(HTTPException):
            await self.batch.start(spec.model_copy(update={'request_key': 'another-batch-key-0002'}))
        self.assertEqual(self.ops.store.rows('SELECT COUNT(*) AS n FROM op_jobs')[0]['n'], 2)

    async def test_normal_control_cannot_queue_batch_and_old_tick_rejects_forced_queue(self):
        _, spec = await self.prepared()
        original_worker = self.batch.worker
        self.batch.worker = AsyncMock()
        started = await self.batch.start(spec)
        await self.batch.task
        for action in ('start', 'resume', 'continue_pending', 'continue_after_single'):
            with self.assertRaises(HTTPException):
                await self.ops.control(started['job_id'], Control(action=action, confirmed=True))
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=1 WHERE id='primary'")
            self.ops.store.db.execute("UPDATE op_jobs SET state='queued' WHERE id=?", (started['job_id'],))
        await self.ops.tick()
        self.assertEqual(len(self.invites()), 0)
        self.assertEqual(self.ops.account_daily_count(ACCOUNT, utc_day()), 1)
        self.assertEqual((await self.ops.detail(started['job_id']))['state'], 'paused')
        self.batch.worker = original_worker

    async def test_pause_during_wait_keeps_remaining_pending_and_no_resume(self):
        _, spec = await self.prepared()
        waiting = asyncio.Event()
        async def block(ctx):
            waiting.set()
            await self.batch.wake.wait()
        self.batch.wait_until_due = block
        started = await self.batch.start(spec)
        await asyncio.wait_for(waiting.wait(), 3)
        result = await self.batch.pause(BatchPause(scope=SCOPE, request_key=spec.request_key, job_id=started['job_id']))
        await self.batch.task
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['pending'], 9)
        self.assertFalse(result['can_resume'])
        self.assertEqual(len(self.invites()), 1)
        replay = await self.batch.start(spec)
        self.assertEqual(replay['state'], 'paused')
        self.assertEqual(len(self.invites()), 1)

    async def test_peerflood_stops_first_attempt_without_account_rotation(self):
        _, spec = await self.prepared()
        self.client.failure = errors.PeerFloodError(None)
        _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        self.assertEqual(len(self.invites()), 1)
        self.assertEqual(self.ops.account_row(ACCOUNT)['enabled'], 0)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary'), 1)
        await self.batch.start(spec)
        self.assertEqual(len(self.invites()), 1)

    async def test_floodwait_stops_and_never_sleeps_then_resumes_the_batch(self):
        _, spec = await self.prepared()
        self.client.failure = errors.FloodWaitError(None, capture=500)
        _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertGreaterEqual(self.ops.account_row(ACCOUNT)['not_before'], self.wall + 500)
        self.assertEqual(len(self.invites()), 1)

    async def test_service_event_without_member_stops_and_is_not_counted_added(self):
        _, spec = await self.prepared()
        self.client.ghost = True
        self.client.service_event = True
        with patch('isolated_contact_batch.asyncio.sleep', new=AsyncMock()):
            _, status = await self.run_batch(spec)
        self.assertEqual(status['counts']['added'], 0)
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        self.assertEqual(status['items'][0]['diagnosis_code'], 'invite_service_membership_conflict')
        self.assertEqual(len(self.invites()), 1)

    async def test_live_status_returns_sending_without_waiting_worker_lock(self):
        _, spec = await self.prepared()
        entered, release = asyncio.Event(), asyncio.Event()
        async def block():
            entered.set()
            await release.wait()
        self.client.invite_hook = block
        started = await self.batch.start(spec)
        await asyncio.wait_for(entered.wait(), 3)
        status = await asyncio.wait_for(self.batch.status(SCOPE), .1)
        self.assertEqual(status['counts']['sending'], 1)
        self.assertEqual(status['attempts_claimed'], 1)
        release.set()
        await self.batch.task

    async def test_worker_cancellation_keeps_sending_unknown_and_pending_paused(self):
        _, spec = await self.prepared()
        entered = asyncio.Event()
        async def block():
            entered.set()
            await asyncio.Event().wait()
        self.client.invite_hook = block
        started = await self.batch.start(spec)
        await asyncio.wait_for(entered.wait(), 3)
        self.batch.task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.batch.task
        status = await self.batch.status(SCOPE)
        self.assertEqual(status['state'], 'interrupted')
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        self.assertEqual((await self.ops.detail(started['job_id']))['state'], 'paused')
        replay = await self.batch.start(spec)
        self.assertEqual(replay['state'], 'interrupted')
        self.assertEqual(len(self.invites()), 1)

    async def test_restart_recovers_claimed_item_without_creating_task(self):
        _, spec = await self.prepared()
        self.batch.worker = AsyncMock()
        started = await self.batch.start(spec)
        await self.batch.task
        ctx = self.batch.active
        self.batch.claim_item(ctx, ctx['items'][0])
        self.ops.initialize()
        status = await self.batch.status(SCOPE)
        self.assertEqual(status['state'], 'interrupted')
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        await self.ops.tick()
        self.assertEqual(len(self.invites()), 0)
        self.assertIsNotNone(self.ops.store.get(ITEM_PREFIX + utc_day() + '_' + str(FIRST)))

    async def test_budget_is_not_raised_and_token_caps_selected_remaining_slots(self):
        real = self.ops.account_daily_count
        with patch.object(self.ops, 'account_daily_count', side_effect=lambda aid, day: 18 + real(aid, day)):
            preview, spec = await self.prepared()
            self.assertEqual(len(preview['selected']), 1)
            _, status = await self.run_batch(spec)
        self.assertEqual(status['counts']['added'], 1)
        self.assertEqual(len(self.invites()), 1)
        self.assertEqual(self.ops.store.get(SINGLE_DAY + utc_day()), self.single_claim)

    async def test_fresh_contact_loss_skips_recipient_without_invite_or_budget_debit(self):
        _, spec = await self.prepared()
        self.client.users[0].mutual_contact = False
        _, status = await self.run_batch(spec)
        self.assertEqual(status['counts']['failed'], 1)
        self.assertEqual(status['counts']['added'], 9)
        self.assertEqual(status['attempts_claimed'], 9)
        self.assertEqual(self.ops.account_daily_count(ACCOUNT, utc_day()), 10)

    async def test_existing_member_skips_without_new_addition(self):
        _, spec = await self.prepared()
        self.client.members.add(FIRST)
        _, status = await self.run_batch(spec)
        self.assertEqual(status['counts']['already_member'], 1)
        self.assertEqual(status['counts']['added'], 9)
        self.assertEqual(status['attempts_claimed'], 9)

    async def test_mutated_per_recipient_claim_blocks_before_the_sole_write(self):
        _, spec = await self.prepared()
        original = self.batch.claim_item
        def mutate(ctx, item):
            original(ctx, item)
            with self.ops.store.db:
                self.ops.store.db.execute('DELETE FROM settings WHERE key=?', (ctx['current_claim']['key'],))
        with patch.object(self.batch, 'claim_item', side_effect=mutate):
            _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(len(self.invites()), 0)

    async def test_replaced_identity_or_pause_revision_prevents_next_rpc(self):
        _, spec = await self.prepared()
        async def change():
            self.ops.store.put('outbound_pause_revision_primary', 1)
        self.client.identity_hook = change
        _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(len(self.invites()), 0)
        self.assertEqual(status['attempts_claimed'], 0)

    async def test_normal_pause_and_cancel_routes_are_scoped_and_no_new_queue(self):
        _, spec = await self.prepared()
        self.batch.worker = AsyncMock()
        started = await self.batch.start(spec)
        await self.batch.task
        normal = await self.ops.control(started['job_id'], Control(action='pause'))
        self.assertEqual(normal['id'], started['job_id'])
        self.assertEqual((await self.batch.status(SCOPE))['state'], 'paused')
        normal = await self.ops.control(started['job_id'], Control(action='cancel'))
        self.assertEqual(normal['state'], 'cancelled')
        self.assertTrue(all(i['state'] == 'cancelled' for i in normal['items']))

    async def test_monotonic_run_deadline_is_not_extended_by_wall_clock_rollback(self):
        _, spec = await self.prepared()
        started = await self.batch.start(spec)
        self.batch.active['run_deadline'] = time.monotonic() - .01
        self.wall -= 3600
        await self.batch.task
        status = await self.batch.status(SCOPE)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(status['counts']['pending'], 10)
        self.assertEqual(len(self.invites()), 0)

    async def test_revoked_session_stops_the_worker_after_one_claim(self):
        _, spec = await self.prepared()
        self.client.failure = errors.SessionRevokedError(None)
        _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(status['attempts_claimed'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        self.assertEqual(self.ops.account_row(ACCOUNT)['enabled'], 0)
        self.assertEqual(len(self.invites()), 1)

    async def test_unknown_timeout_stops_without_retry_and_redacts_error_body(self):
        _, spec = await self.prepared()
        self.client.failure = TimeoutError('PRIVATE_ERROR_CONTENT')
        _, status = await self.run_batch(spec)
        self.assertEqual(status['state'], 'stopped')
        self.assertEqual(status['counts']['uncertain'], 1)
        self.assertEqual(status['counts']['pending'], 9)
        self.assertNotIn('PRIVATE_ERROR_CONTENT', json.dumps(status))
        await self.batch.start(spec)
        self.assertEqual(len(self.invites()), 1)

    async def test_http_auth_scope_schema_and_missing_scope_health_are_zero_rpc(self):
        app = FastAPI()
        app.include_router(self.ops.router)
        base = '/api/scanner/operations/isolated-invite-batch'
        headers = {'x-panel-user': 'owner', 'x-panel-role': 'owner'}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            self.assertEqual((await client.get(base + '/contacts-preview')).status_code, 403)
            self.assertEqual((await client.get(base + '/contacts-preview', headers=headers)).status_code, 422)
            self.assertEqual((await client.get(base + '/contacts-preview?scope=wrong', headers=headers)).status_code, 422)
            self.assertEqual((await client.post(base + '/start', json={}, headers=headers)).status_code, 422)
            status = await client.get(base + '/status?scope=' + SCOPE, headers=headers)
            self.assertEqual(status.status_code, 200)
            self.assertTrue(status.json()['can_start'])
            self.assertEqual(status.headers.get('cache-control'), 'no-store')
        self.assertEqual(self.client.calls, [])


if __name__ == '__main__':
    unittest.main()
