"""Offline behavioural acceptance for an isolated CLIENT contact control."""
import asyncio
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

CANDIDATE = Path(__file__).resolve().parents[1]
CANONICAL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CANONICAL))
sys.path.insert(0, str(CANDIDATE))
from fastapi import FastAPI, HTTPException
import httpx
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from operations import Operations
from isolated_invite import (ACCOUNT, SENDER, TARGET, SCOPE, DAY_PREFIX, REQUEST_PREFIX,
                             IsolatedAttempt, IsolatedInvite, utc_day)


UID = 1234567890


def person(uid=UID, **kwargs):
    fields = dict(id=uid, username='contact_' + str(uid), first_name='PRIVATE_CONTACT_NAME',
                  phone='PRIVATE_PHONE', access_hash=776655443322, contact=True, mutual_contact=True)
    fields.update(kwargs)
    return types.User(**fields)


class Client:
    _request_retries = 0
    flood_sleep_threshold = 0

    def __init__(self):
        self.me = types.User(id=SENDER, username='demo_identity_1')
        self.group = types.Channel(id=720000000, title='ТЕСТОВОЕ СООБЩЕСТВО', megagroup=True,
            photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
            username='demo_identity_6', admin_rights=types.ChatAdminRights(invite_users=True))
        self.users = [person()]
        self.saved_ids = {UID}
        self.members = set()
        self.calls = []
        self.reads = []
        self.invite_failure = None
        self.ghost = False
        self.service_event = False
        self.missing = False
        self.identity_hook = None
        self.user_hook = None
        self.invite_hook = None
        self.membership_hook = None
        self.connect = AsyncMock(side_effect=AssertionError('No connect'))
        self.disconnect = AsyncMock(side_effect=AssertionError('No disconnect'))
        self.sign_in = AsyncMock(side_effect=AssertionError('No auth'))
        self.send_message = AsyncMock(side_effect=AssertionError('No messages'))

    def is_connected(self):
        return True

    async def get_me(self):
        self.reads.append('identity')
        if self.identity_hook:
            await self.identity_hook()
        return self.me

    async def get_entity(self, target):
        self.reads.append('group')
        if target != TARGET:
            raise AssertionError('Fixed group only')
        return self.group

    async def get_permissions(self, group, user):
        self.reads.append(('membership', user.id))
        if self.membership_hook:
            await self.membership_hook(user)
        if user.id not in self.members:
            raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChannelParticipant(user_id=user.id, date=datetime.now(timezone.utc)), False)

    async def __call__(self, request):
        self.calls.append(request)
        if isinstance(request, functions.contacts.GetContactsRequest):
            return types.contacts.Contacts(contacts=[types.Contact(uid, True) for uid in self.saved_ids],
                saved_count=len(self.saved_ids), users=self.users)
        if isinstance(request, functions.users.GetUsersRequest):
            if self.user_hook:
                await self.user_hook()
            return [u for u in self.users if u.id == request.id[0].user_id]
        if not isinstance(request, functions.channels.InviteToChannelRequest):
            raise AssertionError('No other Telegram writes/reads')
        if self.invite_hook:
            await self.invite_hook()
        if self.invite_failure:
            raise self.invite_failure
        user = request.users[0]
        if not self.ghost and not self.missing:
            self.members.add(user.id)
        updates = []
        if self.service_event:
            message = types.MessageService(id=777, peer_id=types.PeerChannel(720000000),
                date=datetime.now(timezone.utc), action=types.MessageActionChatAddUser([user.id]),
                from_id=types.PeerUser(SENDER))
            updates = [types.UpdateNewChannelMessage(message, 1, 1)]
        return types.messages.InvitedUsers(updates=types.Updates(updates, [], [], datetime.now(timezone.utc), 0),
            missing_invitees=[types.MissingInvitee(user.id)] if self.missing else [])


class IsolatedInviteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.client = Client()
        self.runtime = SimpleNamespace(client=self.client, me=self.client.me)
        self.ops = Operations(self.tmp.name, lambda: None, lambda: self.runtime)
        self.ops.initialize()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0,error='historical PeerFlood preserved' WHERE id='primary'")
        self.module = self.ops.isolated_invite
        self.before_account = self.ops.account_row(ACCOUNT).copy()
        self.seed_other()
        self.old_projection = self.module.projection()

    async def asyncTearDown(self):
        self.client.connect.assert_not_awaited()
        self.client.disconnect.assert_not_awaited()
        self.client.sign_in.assert_not_awaited()
        self.client.send_message.assert_not_awaited()
        await self.ops.shutdown()
        self.tmp.cleanup()

    def seed_other(self):
        spec = json.dumps({'daily_limit': 20, 'delay_seconds': 60})
        with self.ops.store.db:
            row = self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) '
                'VALUES(?,?,?,?,?,?,?,?,?,?)', ('unrelated-primary', 'a', ACCOUNT, SENDER, 'invite', 'Old paused job', spec, 'paused',
                '2026-10-01T00:00:00+00:00', '2026-10-01T00:00:00+00:00'))
            self.old_job = row.lastrowid
            self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids) VALUES(?,?,?,?,?,?,?)',
                (self.old_job, TARGET, 'Old group', 222333444, 'old_user', 'pending', '[]'))

    def writes(self):
        return [r for r in self.client.calls if isinstance(r, functions.channels.InviteToChannelRequest)]

    async def prepared(self, key='isolated-control-key-0001'):
        preview = await self.module.preview(SCOPE)
        self.assertEqual(preview['selected']['user_id'], UID)
        return preview, IsolatedAttempt(scope=SCOPE, confirmed=True, reason='Owner requested one isolated contact control',
            request_key=key, candidate_token=preview['candidate_token'])

    def db_snapshot(self):
        return '\n'.join(self.ops.store.db.iterdump())

    def assert_isolation(self, job_id):
        self.assertEqual(self.module.projection(job_id), self.old_projection)
        account = self.ops.account_row(ACCOUNT)
        self.assertEqual(account['enabled'], 0)
        self.assertEqual(account['error'], self.before_account['error'])
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_jobs WHERE id=?', (self.old_job,))[0]['state'], 'paused')

    async def test_original_online_blocks_but_isolated_verified_control_does_not_enable(self):
        with self.assertRaises(HTTPException):
            self.ops.online(ACCOUNT)
        _, spec = await self.prepared()
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'invited')
        self.assertEqual(len(self.writes()), 1)
        self.assert_isolation(result['job_id'])
        job = await self.ops.detail(result['job_id'])
        self.assertEqual(job['state'], 'paused')
        self.assertEqual(len(job['items']), 1)
        diagnosis = job['items'][0]['diagnosis']
        self.assertEqual(diagnosis['code'], 'invite_member_verified')
        self.assertTrue(diagnosis['contact_evidence']['mutual_contact'])
        self.assertEqual(diagnosis['isolated_control']['primary_projection_before'], diagnosis['isolated_control']['primary_projection_after'])
        self.assertEqual(self.ops.account_daily_count(ACCOUNT, utc_day()), 1)
        await self.ops.tick()
        self.assertEqual(len(self.writes()), 1)

    async def test_preview_readonly_filters_all_history_and_private_contact_fields(self):
        old = person(222333444)
        hidden = person(777, username=None)
        stranger = person(888, mutual_contact=False)
        owner = person(710000010)
        not_saved = person(999)
        self.client.users = [old, hidden, stranger, owner, not_saved, person()]
        self.client.saved_ids = {u.id for u in self.client.users if u.id != 999}
        before = self.db_snapshot()
        data = await self.module.preview(SCOPE)
        self.assertEqual(self.db_snapshot(), before)
        self.assertFalse(self.writes())
        self.assertEqual(data['membership_checks'], 1)
        self.assertEqual(sum(isinstance(r, functions.contacts.GetContactsRequest) for r in self.client.calls), 1)
        self.assertGreater(data['token_ttl_seconds'], 120)
        self.assertIn('expires_at', data)
        for secret in ('PRIVATE_CONTACT_NAME', 'PRIVATE_PHONE', '776655443322', 'access_hash', 'contact_222333444'):
            self.assertNotIn(secret, json.dumps(data))

    async def test_preview_checks_at_most_ten_members_and_never_returns_full_list(self):
        self.client.users = [person(UID + i) for i in range(12)]
        self.client.saved_ids = {u.id for u in self.client.users}
        self.client.members = set(self.client.saved_ids)
        data = await self.module.preview(SCOPE)
        self.assertIsNone(data['selected'])
        self.assertEqual(data['membership_checks'], 10)
        self.assertEqual(len([r for r in self.client.reads if isinstance(r, tuple)]), 10)
        self.assertNotIn('users', data)

    async def test_pending_history_uncertain_and_accepted_are_all_excluded(self):
        self.client.users = [person(222333444), person(710000003), person()]
        self.client.saved_ids = {u.id for u in self.client.users}
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?)', (TARGET, UID, 'uncertain', self.old_job))
        data = await self.module.preview(SCOPE)
        self.assertIsNone(data['selected'])
        self.assertEqual(data['membership_checks'], 0)

    async def test_same_key_concurrency_and_replay_never_repeat_write(self):
        _, spec = await self.prepared()
        first, second = await asyncio.gather(self.module.attempt(spec), self.module.attempt(spec))
        self.assertEqual(first, second)
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual((await self.module.attempt(spec))['job_id'], first['job_id'])
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec.model_copy(update={'request_key': 'different-control-key-0002'}))
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec.model_copy(update={'reason': 'A different bound reason'}))
        self.assertEqual(len(self.writes()), 1)

    async def test_expired_previous_day_or_forged_token_blocks_before_rpc(self):
        _, spec = await self.prepared()
        before = len(self.client.calls)
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec.model_copy(update={'candidate_token': 'x' * 43}))
        self.module.candidate['day'] = '2000-01-01'
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.module.candidate['day'] = utc_day()
        self.module.candidate['expires'] = 0
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertEqual(len(self.client.calls), before)
        self.assertFalse(self.writes())

    async def test_fresh_contact_loss_or_membership_change_prevents_claim_and_write(self):
        _, spec = await self.prepared()
        self.client.users[0].mutual_contact = False
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.client.users[0].mutual_contact = True
        self.client.members.add(UID)
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(DAY_PREFIX + utc_day()))

    async def test_replaced_client_or_changed_pause_revision_blocks(self):
        _, spec = await self.prepared()
        original = self.runtime.client
        self.runtime.client = Client()
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.runtime.client = original
        self.ops.store.put('outbound_pause_revision_primary', 1)
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertFalse(self.writes())

    async def test_revision_change_during_identity_prevents_next_rpc(self):
        _, spec = await self.prepared()
        async def change():
            self.ops.store.put('outbound_pause_revision_primary', 1)
        self.client.identity_hook = change
        before = len(self.client.calls)
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertEqual(len(self.client.calls), before)
        self.assertFalse(self.writes())

    async def test_cooldown_budget_and_automatic_retry_settings_never_bypassed(self):
        _, spec = await self.prepared()
        import time
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET not_before=? WHERE id='primary'", (time.time() + 100,))
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET not_before=0 WHERE id='primary'")
        with patch.object(self.ops, 'account_daily_count', return_value=20):
            with self.assertRaises(HTTPException):
                await self.module.attempt(spec)
        self.client._request_retries = 1
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertFalse(self.writes())

    async def test_last_budget_slot_is_available_after_own_claim(self):
        _, spec = await self.prepared()
        real = self.ops.account_daily_count
        with patch.object(self.ops, 'account_daily_count', side_effect=lambda aid, day: 19 + real(aid, day)):
            data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'invited')
        self.assertEqual(len(self.writes()), 1)

    async def test_service_add_event_without_member_stays_uncertain_and_paused(self):
        _, spec = await self.prepared()
        self.client.ghost = True
        self.client.service_event = True
        with patch('isolated_invite.asyncio.sleep', new=AsyncMock()):
            data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'uncertain')
        job = await self.ops.detail(data['job_id'])
        self.assertEqual(job['items'][0]['diagnosis']['code'], 'invite_service_membership_conflict')
        self.assertEqual(len(job['items'][0]['diagnosis']['verification']), 3)
        self.assertEqual(job['state'], 'paused')
        self.assert_isolation(data['job_id'])
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_peerflood_keeps_disabled_and_consumes_permanent_day_claim(self):
        _, spec = await self.prepared()
        self.client.invite_failure = errors.PeerFloodError(None)
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'uncertain')
        self.assertEqual(self.ops.account_row(ACCOUNT)['enabled'], 0)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary', 0), 1)
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_floodwait_preserves_protective_cooldown_and_never_retries(self):
        _, spec = await self.prepared()
        self.client.invite_failure = errors.FloodWaitError(None, capture=333)
        import time
        before = time.time()
        data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'uncertain')
        self.assertGreaterEqual(self.ops.account_row(ACCOUNT)['not_before'], before + 333)
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_cancellation_after_claim_persists_uncertain_with_history(self):
        _, spec = await self.prepared()
        async def cancel():
            raise asyncio.CancelledError()
        self.client.invite_hook = cancel
        with self.assertRaises(asyncio.CancelledError):
            await self.module.attempt(spec)
        status = await self.module.status(SCOPE)
        self.assertEqual(status['result_state'], 'uncertain')
        self.assertTrue(status['invitation_requested'])
        self.assertEqual((await self.ops.detail(status['job_id']))['items'][0]['state'], 'uncertain')
        self.assertEqual((await self.module.attempt(spec))['result_state'], 'uncertain')
        self.assertEqual(len(self.writes()), 1)

    async def test_unknown_timeout_consumes_claim_without_clearing_error(self):
        _, spec = await self.prepared()
        self.client.invite_failure = TimeoutError('PRIVATE_ERROR_CONTENT')
        data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'uncertain')
        job = await self.ops.detail(data['job_id'])
        self.assertNotIn('PRIVATE_ERROR_CONTENT', json.dumps(job))
        self.assert_isolation(data['job_id'])

    async def test_missing_invitee_is_restricted_never_false_success(self):
        _, spec = await self.prepared()
        self.client.missing = True
        data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'restricted')
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual((await self.ops.detail(data['job_id']))['items'][0]['diagnosis']['code'], 'privacy_restricted')

    async def test_claim_is_durable_before_invite_and_new_job_never_queued(self):
        _, spec = await self.prepared()
        async def inspect():
            record = self.ops.store.get(DAY_PREFIX + utc_day())
            self.assertEqual(record['state'], 'claimed')
            self.assertTrue(record['invitation_requested'])
            self.assertEqual(self.ops.store.get(REQUEST_PREFIX + spec.request_key), record)
            job = await self.ops.detail(record['job_id'])
            self.assertEqual((job['state'], job['items'][0]['state']), ('paused', 'sending'))
            self.assertTrue(job['items'][0]['diagnosis']['request_outcome_unknown'])
            self.assertEqual(self.ops.account_row(ACCOUNT)['enabled'], 0)
        self.client.invite_hook = inspect
        data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'invited')

    async def test_mutation_or_cancellation_after_dispatch_cannot_be_reported_success(self):
        _, spec = await self.prepared()
        async def mutate():
            record = self.ops.store.get(DAY_PREFIX + utc_day())
            with self.ops.store.db:
                self.ops.store.db.execute("UPDATE op_jobs SET state='cancelled' WHERE id=?", (record['job_id'],))
        self.client.invite_hook = mutate
        data = await self.module.attempt(spec)
        self.assertEqual(data['result_state'], 'uncertain')
        self.assertEqual((await self.ops.detail(data['job_id']))['state'], 'cancelled')

    async def test_slow_sql_deadline_does_not_start_following_rpc(self):
        import time
        original = self.module.projection
        def slow(*args):
            value = original(*args)
            time.sleep(.02)
            return value
        with patch('isolated_invite.PREVIEW_TIMEOUT', .005), patch.object(self.module, 'projection', side_effect=slow):
            with self.assertRaises(HTTPException) as raised:
                await self.module.preview(SCOPE)
        self.assertEqual(raised.exception.status_code, 504)
        self.assertFalse(self.client.calls)
        self.assertEqual(self.client.reads, [])

    async def test_lock_contention_counts_against_deadline_and_starts_no_rpc(self):
        _, spec = await self.prepared()
        before = len(self.client.calls)
        await self.ops.lock.acquire()
        try:
            with patch('isolated_invite.PREVIEW_TIMEOUT', .01), patch('isolated_invite.ATTEMPT_TIMEOUT', .01):
                for call in (lambda: self.module.preview(SCOPE), lambda: self.module.attempt(spec)):
                    with self.assertRaises(HTTPException) as raised:
                        await call()
                    self.assertEqual(raised.exception.status_code, 504)
        finally:
            self.ops.lock.release()
        self.assertEqual(len(self.client.calls), before)
        self.assertFalse(self.writes())

    async def test_preview_skips_workbench_exclusion_before_membership(self):
        from operation_workbench import ExclusionSpec
        await self.ops.workbench.add_exclusion(ExclusionSpec(request_key='exclude-user-key-0001', subject='user',
            identifier=str(UID), reason='Explicit do not contact'))
        next_user = person(UID + 1)
        self.client.users.append(next_user)
        self.client.saved_ids.add(next_user.id)
        before = self.db_snapshot()
        data = await self.module.preview(SCOPE)
        self.assertEqual(data['selected']['user_id'], next_user.id)
        self.assertNotIn(('membership', UID), self.client.reads)
        self.assertEqual(self.db_snapshot(), before)

    async def test_startup_recovery_keeps_claim_and_marks_surviving_sending_unknown(self):
        _, spec = await self.prepared()
        ctx = self.module.context(__import__('time').monotonic() + 20)
        await self.module.identity(ctx)
        record = self.module.claim(ctx, spec, self.client.group, self.client.users[0], 'saved-request-digest')
        self.ops.initialize()
        item = self.ops.store.rows('SELECT state FROM op_items WHERE id=?', (record['item_id'],))[0]
        self.assertEqual(item['state'], 'uncertain')
        self.assertEqual(self.ops.store.get(DAY_PREFIX + utc_day())['request_key'], spec.request_key)
        self.assertEqual((await self.ops.detail(record['job_id']))['state'], 'paused')
        await self.ops.tick()
        self.assertFalse(self.writes())

    async def test_tampered_saved_recipient_after_dispatch_stays_unknown(self):
        _, spec = await self.prepared()
        async def mutate():
            record = self.ops.store.get(DAY_PREFIX + utc_day())
            with self.ops.store.db:
                self.ops.store.db.execute('UPDATE op_items SET user_id=? WHERE id=?', (99999888, record['item_id']))
        self.client.invite_hook = mutate
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'uncertain')
        self.assertEqual(len(self.writes()), 1)

    async def test_explicit_privacy_rpc_rejection_is_preserved_and_never_retried(self):
        _, spec = await self.prepared()
        self.client.invite_failure = errors.UserPrivacyRestrictedError(None)
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'restricted')
        diagnosis = (await self.ops.detail(result['job_id']))['items'][0]['diagnosis']
        self.assertEqual(diagnosis['code'], 'UserPrivacyRestrictedError')
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_http_auth_scope_schema_and_empty_health_are_zero_rpc(self):
        app = FastAPI()
        app.include_router(self.ops.router)
        base = '/api/scanner/operations/isolated-invite'
        headers = {'x-panel-user': 'owner', 'x-panel-role': 'owner'}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            self.assertEqual((await client.get(base + '/contacts-preview')).status_code, 403)
            self.assertEqual((await client.get(base + '/contacts-preview', headers=headers)).status_code, 422)
            self.assertEqual((await client.get(base + '/contacts-preview?scope=wrong', headers=headers)).status_code, 422)
            self.assertEqual((await client.post(base + '/attempt', headers=headers, json={})).status_code, 422)
            for key, value in (('confirmed', False), ('confirmed', 1), ('account', 'other')):
                payload = dict(scope=SCOPE, confirmed=True, reason='An explicit control request',
                               request_key='http-single-key-0001', candidate_token='x' * 43)
                payload[key] = value
                self.assertEqual((await client.post(base + '/attempt', headers=headers, json=payload)).status_code, 422)
        self.assertFalse(self.client.calls)
        self.assertEqual(self.client.reads, [])


if __name__ == '__main__':
    unittest.main()
