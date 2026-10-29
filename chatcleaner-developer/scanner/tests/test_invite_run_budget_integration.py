"""Offline tests through Operations: a finite grant never replaces Telegram guards.

The client below has no socket/session/connect method. Only its recorded TL
requests stand in for Telegram writes; membership changes only after that fake
invite returns. Every database belongs to a TemporaryDirectory.
"""
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import operations
from fastapi import HTTPException
from invite_run_budget import CLAIM_PREFIX, GRANT_PREFIX, SENDER_ID, TARGET
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions


MIDNIGHT_HOLD = 'Дневной предел достигнут. Продолжение со следующего дня UTC.'


class OfflineInviteClient:
    def __init__(self, group):
        self.group = group
        self.calls = []
        self.members = set()
        self.contacts = set()
        self.membership_reads = []
        self.failure = None
        self.before_invite = None
        self.after_contact = None
        self.on_entity = None
        self.disconnect = AsyncMock()

    def is_connected(self):
        return True

    def user(self, uid):
        return types.User(id=uid, first_name='Agreed recipient',
                          username='run_user_' + str(uid - 10000),
                          access_hash=uid + 100, contact=uid in self.contacts,
                          mutual_contact=False)

    async def get_entity(self, ref):
        if isinstance(ref, int) and ref < 0:
            return self.group
        uid = (ref.user_id if isinstance(ref, types.InputPeerUser) else
               int(ref.rsplit('_', 1)[-1]) + 10000 if isinstance(ref, str) else ref)
        if self.on_entity is not None:
            self.on_entity(ref)
        return self.user(uid)

    async def get_input_entity(self, entity):
        return entity

    async def get_permissions(self, entity, user):
        self.membership_reads.append(user.id)
        if user.id not in self.members:
            raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChannelParticipant(
            user_id=user.id, date=datetime.now(timezone.utc)), False)

    async def __call__(self, request):
        self.calls.append(request)
        if self.failure is not None:
            raise self.failure
        if isinstance(request, functions.contacts.AddContactRequest):
            self.contacts.add(request.id.id)
            if self.after_contact is not None:
                self.after_contact()
            return types.Updates(updates=[], users=[], chats=[],
                                 date=datetime.now(timezone.utc), seq=0)
        if isinstance(request, functions.users.GetUsersRequest):
            return [self.user(user.id) for user in request.id]
        if isinstance(request, functions.channels.InviteToChannelRequest):
            if self.before_invite is not None:
                self.before_invite(request)
            self.members.update(user.id for user in request.users)
            return types.messages.InvitedUsers(updates=types.Updates(
                updates=[], users=[], chats=[], date=datetime.now(timezone.utc), seq=0),
                missing_invitees=[])
        raise AssertionError('Unexpected offline request: ' + type(request).__name__)


class InviteRunBudgetIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = [datetime.now(timezone.utc).replace(
            hour=12, minute=0, second=0, microsecond=0).timestamp()]
        self.time_patch = patch('operations.time.time', side_effect=lambda: self.clock[0])
        self.now_patch = patch('operations.now', side_effect=self.timestamp)
        self.time_patch.start()
        self.now_patch.start()
        self.addCleanup(self.time_patch.stop)
        self.addCleanup(self.now_patch.stop)
        self.group = types.Channel(id=720000000, title='Authorised group',
            photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
            megagroup=True, creator=True, username='offline_group')
        self.client = OfflineInviteClient(self.group)
        self.primary = SimpleNamespace(client=self.client,
            me=types.User(id=SENDER_ID, first_name='Authorised sender',
                          username='offline_primary'))
        self.ops = self.new_operations()
        self.ops.initialize()
        if hasattr(self.ops, 'invite_runs'):
            self.ops.invite_runs.clock = lambda: self.clock[0]
        self.jobs = {}
        for jid in range(1, 14):
            invite = jid in (11, 13)
            spec = dict(request_key='run-integration-job-' + str(jid).zfill(4),
                        account='primary', kind='invite' if invite else 'post',
                        title='Saved job ' + str(jid), destinations=[TARGET],
                        text='' if invite else 'Offline baseline', daily_limit=20)
            if invite:
                spec.update(users=['run_user_' + str(jid)],
                            consent_note='Explicit permission for this offline test')
            job = await self.ops.create(operations.JobSpec(**spec))
            self.assertEqual(job['id'], jid)
            self.jobs[jid] = job
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='cancelled' WHERE id NOT IN (11,13)")
            self.ops.store.db.execute("UPDATE op_items SET state='cancelled' WHERE job NOT IN (11,13)")
            self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id IN (11,13)")
            self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=1")
            self.ops.store.db.execute("UPDATE op_items SET state='sent',attempted_at=?,finished_at=? WHERE job=1",
                                      (self.timestamp(), self.timestamp()))
            for _ in range(19):
                self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,attempted_at,finished_at) '
                    'SELECT job,chat_id,chat_title,state,random_ids,attempted_at,finished_at FROM op_items WHERE job=1 LIMIT 1')
        self.key = 'finite-run-integration-0001'
        self.expiry = self.clock[0] + 3600
        self.addAsyncCleanup(self.close_operations)

    def new_operations(self):
        return operations.Operations(self.tmp.name,
            lambda: {'id': 123, 'hash': 'offline-api-hash'}, lambda: self.primary)

    async def close_operations(self):
        await self.ops.shutdown()

    def timestamp(self):
        return datetime.fromtimestamp(self.clock[0], timezone.utc).isoformat()

    def count(self, aid='primary'):
        return self.ops.account_daily_count(aid, self.timestamp()[:10])

    def sql(self, sql, args=()):
        with self.ops.store.db:
            self.ops.store.db.execute(sql, args)

    async def grant(self):
        preview = await self.ops.invite_run_preview()
        self.assertEqual({item['job_id'] for item in preview['items']}, {11, 13})
        self.assertEqual(preview['account_count_today'], 20)
        self.grant_spec = operations.InviteRunGrant(confirmed=True, request_key=self.key,
            reason='Owner explicitly approved only this saved finite cohort',
            expires_at=self.expiry, cohort_hash=preview['cohort_hash'], items=preview['items'])
        return await self.ops.grant_invite_run_budget(self.grant_spec)

    async def status(self):
        return await self.ops.invite_run_status(self.key)

    async def continue_job(self, jid):
        return await self.ops.control(jid, operations.Control(
            action='continue_pending', confirmed=True))

    async def run_tick(self):
        await self.ops.tick()
        self.clock[0] += 61

    def invites(self):
        return [request for request in self.client.calls
                if isinstance(request, functions.channels.InviteToChannelRequest)]

    def snapshot(self):
        return {table: self.ops.store.rows('SELECT * FROM ' + table + ' ORDER BY rowid')
                for table in ('op_accounts', 'op_jobs', 'op_items', 'op_invited')}

    async def test_preview_and_grant_save_only_without_start_or_counter_reset(self):
        before = self.snapshot()
        grant = await self.grant()
        self.assertEqual(self.snapshot(), before)
        self.assertEqual((grant['initial_count'], grant['max_claims'],
                          grant['account_daily_ceiling']), (20, 2, 22))
        await self.run_tick()
        self.assertFalse(self.client.calls)
        self.assertEqual(self.count(), 20)
        self.assertEqual((await self.status())['claims_used'], 0)
        for jid in (11, 13):
            self.assertEqual((await self.ops.detail(jid))['state'], 'paused')

    async def test_exact_grant_executes_two_invites_with_verified_membership_and_last_slot(self):
        await self.grant()
        for jid in (11, 13):
            await self.continue_job(jid)
        seen_counts = []
        def inspect_claim(request):
            uid = request.users[0].id
            row = self.ops.store.rows("SELECT * FROM op_items WHERE user_id=? AND state='sending'", (uid,))
            self.assertEqual(len(row), 1)
            self.assertIsNotNone(row[0]['attempted_at'])
            claim = self.ops.store.get(CLAIM_PREFIX + str(row[0]['id']))
            self.assertEqual(claim['user_id'], uid)
            self.assertIsNotNone(claim['invite_write_at'])
            seen_counts.append(self.count())
        self.client.before_invite = inspect_claim
        for _ in range(4):
            await self.run_tick()
        self.assertEqual(seen_counts, [21, 22])
        self.assertEqual([r.users[0].id for r in self.invites()], [10011, 10013])
        self.assertEqual(self.client.members, {10011, 10013})
        self.assertEqual(self.count(), 22)
        for jid in (11, 13):
            item = (await self.ops.detail(jid))['items'][0]
            self.assertEqual(item['state'], 'invited')
            self.assertTrue(item['diagnosis']['invitation_requested'])
            self.assertEqual(item['diagnosis']['verification'][-1]['member'], True)
        self.assertEqual((await self.status())['claims_used'], 2)
        self.assertEqual((await self.status())['state'], 'complete')

    async def test_without_grant_normal_twenty_is_a_real_hold_not_an_invite(self):
        await self.continue_job(11)
        await self.run_tick()
        item = (await self.ops.detail(11))['items'][0]
        self.assertEqual(item['state'], 'pending')
        self.assertEqual(item['error'], MIDNIGHT_HOLD)
        self.assertFalse(self.client.calls)
        self.assertEqual(self.count(), 20)

    async def test_only_exact_internal_midnight_hold_is_cleared_by_confirmed_continue(self):
        tomorrow = (int(self.clock[0]) // 86400 + 1) * 86400
        self.sql('UPDATE op_jobs SET not_before=? WHERE id=11', (tomorrow,))
        self.sql('UPDATE op_items SET error=? WHERE job=11', (MIDNIGHT_HOLD,))
        await self.grant()
        with self.assertRaises(HTTPException):
            await self.ops.control(11, operations.Control(action='continue_pending'))
        held = await self.ops.detail(11)
        self.assertEqual((held['state'], held['not_before']), ('paused', tomorrow))
        self.assertEqual(held['items'][0]['error'], MIDNIGHT_HOLD)
        continued = await self.continue_job(11)
        self.assertEqual(continued['state'], 'queued')
        self.assertLessEqual(continued['not_before'], self.clock[0])
        await self.run_tick()
        self.assertEqual(len(self.invites()), 1)

    async def test_real_account_wait_and_unrecognised_job_hold_are_never_cleared(self):
        await self.grant()
        wait = self.clock[0] + 900
        self.sql("UPDATE op_accounts SET not_before=?,error='Telegram real wait' WHERE id='primary'", (wait,))
        with self.assertRaises(HTTPException):
            await self.continue_job(11)
        self.assertEqual(self.ops.account_row('primary')['not_before'], wait)
        await self.run_tick()
        self.assertFalse(self.client.calls)
        self.assertEqual(self.count(), 20)

    async def test_unrelated_schedule_is_preserved_even_with_a_finite_grant(self):
        wait = self.clock[0] + 900
        self.sql('UPDATE op_jobs SET not_before=? WHERE id=11', (wait,))
        self.sql("UPDATE op_items SET error='Telegram просит подождать.' WHERE job=11")
        await self.grant()
        try:
            await self.continue_job(11)
        except HTTPException:
            pass
        self.assertEqual((await self.ops.detail(11))['not_before'], wait)
        await self.run_tick()
        self.assertFalse(self.client.calls)

    async def test_other_primary_job_never_borrows_the_grant(self):
        other = await self.ops.create(operations.JobSpec(
            request_key='out-of-scope-primary-job-0001', account='primary', kind='invite',
            title='Outside finite cohort', destinations=[TARGET], users=['run_user_99'],
            consent_note='Explicit offline permission', daily_limit=20))
        await self.grant()
        await self.ops.control(other['id'], operations.Control(action='start', confirmed=True))
        await self.run_tick()
        self.assertFalse(self.client.calls)
        self.assertEqual((await self.ops.detail(other['id']))['items'][0]['state'], 'pending')
        self.assertEqual((await self.status())['claims_used'], 0)
        self.assertEqual(self.count(), 20)

    async def test_other_account_keeps_its_normal_budget_and_cannot_claim_this_receipt(self):
        account = await self.ops.add_account(operations.AccountSpec(label='Other offline account', authorized=True))
        aid = account['id']
        other_client = OfflineInviteClient(self.group)
        other = self.ops.accounts[aid]
        other.client = other_client
        other.me = types.User(id=999001, first_name='Other offline sender')
        other.status = 'connected'
        self.sql('UPDATE op_accounts SET telegram_id=?,enabled=1 WHERE id=?', (other.me.id, aid))
        outside = await self.ops.create(operations.JobSpec(request_key='outside-account-job-0001',
            account=aid, kind='invite', title='Other account job', destinations=[TARGET],
            users=['run_user_98'], consent_note='Explicit offline permission', daily_limit=20))
        for _ in range(20):
            self.sql("INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,attempted_at) VALUES(?,?,?,'sent','[]',?)",
                     (outside['id'], TARGET, 'Baseline count', self.timestamp()))
        await self.grant()
        await self.ops.control(outside['id'], operations.Control(action='start', confirmed=True))
        await self.run_tick()
        self.assertFalse(other_client.calls)
        self.assertFalse(self.client.calls)
        self.assertEqual((await self.status())['claims_used'], 0)
        self.assertEqual((self.count(), self.count(aid)), (20, 20))

    async def test_expiry_is_enforced_before_rows_even_when_job_waits_until_tomorrow(self):
        await self.grant()
        for jid in (11, 13):
            await self.continue_job(jid)
        self.sql('UPDATE op_jobs SET not_before=? WHERE id IN (11,13)', (self.clock[0] + 86400,))
        self.clock[0] = self.expiry + 1
        await self.run_tick()
        self.assertEqual((await self.status())['state'], 'expired')
        self.assertEqual([(await self.ops.detail(j))['state'] for j in (11, 13)], ['paused', 'paused'])
        self.assertFalse(self.client.calls)
        self.assertEqual((await self.status())['claims_used'], 0)
        with self.assertRaises(HTTPException):
            await self.continue_job(11)

    async def test_restart_never_rebinds_or_falls_back_to_ordinary_budget(self):
        await self.grant()
        await self.continue_job(11)
        await self.ops.shutdown()
        self.ops = self.new_operations()
        self.ops.initialize()
        self.ops.invite_runs.clock = lambda: self.clock[0]
        await self.run_tick()
        status = await self.status()
        self.assertEqual(status['state'], 'stopped')
        self.assertFalse(status['client_bound'])
        self.assertFalse(self.client.calls)
        self.assertEqual(self.count(), 20)
        self.assertEqual((await self.ops.detail(11))['state'], 'paused')
        replay = await self.ops.grant_invite_run_budget(self.grant_spec)
        self.assertFalse(replay['client_bound'])
        await self.run_tick()
        self.assertFalse(self.client.calls)

    async def test_expiry_during_peer_resolution_burns_no_external_write(self):
        await self.grant()
        await self.continue_job(11)
        def expire(ref):
            if isinstance(ref, types.InputPeerUser):
                self.clock[0] = self.expiry + 1
        self.client.on_entity = expire
        await self.run_tick()
        self.assertFalse(self.client.calls)
        self.assertIn((await self.status())['state'], ('expired', 'stopped'))
        self.assertEqual((await self.status())['claims_used'], 1)
        item = (await self.ops.detail(11))['items'][0]
        self.assertFalse(item['diagnosis']['invitation_requested'])
        self.assertEqual(self.count(), 21)

    async def test_claim_and_sending_transition_roll_back_in_one_worker_transaction(self):
        await self.grant()
        await self.continue_job(11)
        self.sql("CREATE TRIGGER reject_test_sending BEFORE UPDATE OF state ON op_items "
                 "WHEN NEW.job=11 AND NEW.state='sending' BEGIN "
                 "SELECT RAISE(ABORT, 'offline transaction failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            await self.ops.tick()
        item = (await self.ops.detail(11))['items'][0]
        self.assertEqual(item['state'], 'pending')
        self.assertIsNone(item['attempted_at'])
        self.assertIsNone(self.ops.store.get(CLAIM_PREFIX + str(item['id'])))
        self.assertEqual((await self.status())['claims_used'], 0)
        self.assertEqual(self.count(), 20)
        self.assertFalse(self.client.calls)

    async def test_expiry_in_last_contact_context_await_prevents_add_contact(self):
        spec = json.loads(self.ops.store.rows('SELECT spec FROM op_jobs WHERE id=13')[0]['spec'])
        spec['add_contacts_before_invite'] = True
        self.sql('UPDATE op_jobs SET spec=? WHERE id=13', (json.dumps(spec),))
        await self.grant()
        await self.continue_job(13)
        original_rights = self.ops.rights
        checks = 0
        async def expire_after_rights(*args, **kwargs):
            nonlocal checks
            result = await original_rights(*args, **kwargs)
            checks += 1
            if checks == 2:
                self.clock[0] = self.expiry + 1
            return result
        self.ops.rights = expire_after_rights
        await self.run_tick()
        await self.run_tick()
        self.assertEqual(checks, 2)
        self.assertFalse(self.client.calls)
        self.assertIn((await self.status())['state'], ('expired', 'stopped'))
        self.assertEqual((await self.status())['claims_used'], 1)
        item = (await self.ops.detail(13))['items'][0]
        self.assertFalse(item['diagnosis']['invitation_requested'])
        claim = self.ops.store.get(CLAIM_PREFIX + str(item['id']))
        self.assertIsNone(claim['contact_write_at'])
        self.assertIsNone(claim['invite_write_at'])

    async def test_manual_pause_after_contact_prevents_the_following_invite(self):
        spec = json.loads(self.ops.store.rows('SELECT spec FROM op_jobs WHERE id=13')[0]['spec'])
        spec['add_contacts_before_invite'] = True
        self.sql('UPDATE op_jobs SET spec=? WHERE id=13', (json.dumps(spec),))
        await self.grant()
        await self.continue_job(13)
        self.client.after_contact = lambda: self.sql("UPDATE op_jobs SET state='paused' WHERE id=13")
        await self.run_tick()
        await self.run_tick()
        self.assertEqual([type(r) for r in self.client.calls], [functions.contacts.AddContactRequest,
            functions.users.GetUsersRequest])
        self.assertFalse(self.invites())
        self.assertEqual((await self.ops.detail(13))['state'], 'paused')
        self.assertEqual((await self.status())['claims_used'], 1)

    async def test_contact_context_records_one_contact_and_one_invite_not_a_double_guard(self):
        spec = json.loads(self.ops.store.rows('SELECT spec FROM op_jobs WHERE id=13')[0]['spec'])
        spec['add_contacts_before_invite'] = True
        self.sql('UPDATE op_jobs SET spec=? WHERE id=13', (json.dumps(spec),))
        await self.grant()
        await self.continue_job(13)
        await self.run_tick()
        await self.run_tick()
        self.assertEqual([type(r) for r in self.client.calls], [functions.contacts.AddContactRequest,
            functions.users.GetUsersRequest, functions.channels.InviteToChannelRequest])
        item = (await self.ops.detail(13))['items'][0]
        self.assertEqual(item['state'], 'invited')
        claim = self.ops.store.get(CLAIM_PREFIX + str(item['id']))
        self.assertIsNotNone(claim['contact_write_at'])
        self.assertIsNotNone(claim['invite_write_at'])
        self.assertEqual(self.count(), 21)

    async def test_peer_flood_stops_the_whole_grant_without_next_call_or_account_rotation(self):
        await self.grant()
        for jid in (11, 13):
            await self.continue_job(jid)
        self.client.failure = errors.PeerFloodError(None)
        for _ in range(3):
            await self.run_tick()
        self.assertEqual(len(self.invites()), 1)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertEqual((await self.status())['state'], 'stopped')
        self.assertEqual((await self.status())['claims_used'], 1)
        self.assertEqual((await self.ops.detail(13))['items'][0]['state'], 'pending')
        self.assertEqual(self.count(), 21)

    async def test_existing_terminal_evidence_is_not_repeated_or_included_in_the_grant(self):
        terminal = []
        for state, uid in [('invited', 10071), ('restricted', 10072), ('uncertain', 10073)]:
            with self.ops.store.db:
                cursor = self.ops.store.db.execute("INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids,diagnosis) VALUES(11,?,?,?,?,?,'[]',?)",
                    (TARGET, 'Original group', uid, 'run_user_' + str(uid - 10000), state,
                     json.dumps({'source': 'saved_evidence', 'invitation_requested': True})))
                terminal.append(cursor.lastrowid)
        before = self.ops.store.rows('SELECT * FROM op_items WHERE id IN (?,?,?) ORDER BY id', terminal)
        grant = await self.grant()
        self.assertEqual(grant['max_claims'], 2)
        await self.continue_job(11)
        await self.run_tick()
        await self.run_tick()
        after = self.ops.store.rows('SELECT * FROM op_items WHERE id IN (?,?,?) ORDER BY id', terminal)
        self.assertEqual(after, before)
        self.assertEqual([r.users[0].id for r in self.invites()], [10011])
        self.assertEqual((await self.ops.detail(11))['items'][0]['state'], 'invited')


if __name__ == '__main__':
    unittest.main()
