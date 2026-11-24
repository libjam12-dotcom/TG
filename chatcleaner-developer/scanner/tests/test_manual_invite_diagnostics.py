"""Offline acceptance for queue policy, restrictions and membership evidence."""
import asyncio
import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException
from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from fixture_manual import ManualFixture, SENDER, TARGET
from manual_invite_campaign import QuickDraftRequest, QuickStartRequest, PauseRequest
from operations import classify_invite_failure, missing_invitee_diagnosis, participant_membership, ResumeOutbound


def request():
    return functions.channels.InviteToChannelRequest(types.InputChannel(1, 123456789), [types.InputUser(2, 987654321)])


class ClassificationTests(unittest.TestCase):
    def test_only_known_recipient_errors_at_the_write_can_advance(self):
        for code in ('CHAT_MEMBER_ADD_FAILED', 'USER_NOT_MUTUAL_CONTACT', 'USER_PRIVACY_RESTRICTED', 'USER_KICKED'):
            for phase in ('target_preflight', 'resolve_recipient', 'membership_before', 'invite_rpc', 'membership_after'):
                with self.subTest(code=code, phase=phase):
                    result = classify_invite_failure(errors.BadRequestError(request(), code, 400), phase)
                    self.assertEqual(result['queue_action'], 'next_recipient' if phase == 'invite_rpc' else 'stop')
        for code in ('CHAT_INVALID', 'CHANNEL_INVALID', 'CHANNEL_MONOFORUM_UNSUPPORTED', 'CHAT_ADMIN_REQUIRED',
                     'CHANNEL_PRIVATE', 'USER_BANNED_IN_CHANNEL', 'PRIVATE_SECRET'):
            self.assertEqual(classify_invite_failure(errors.BadRequestError(request(), code, 400), 'invite_rpc')['queue_action'], 'stop')
        wrong_method = functions.users.GetUsersRequest([])
        self.assertEqual(classify_invite_failure(errors.BadRequestError(wrong_method, 'USER_KICKED', 400), 'invite_rpc')['queue_action'], 'stop')

    def test_unknown_diagnostics_do_not_serialize_text_or_request_values(self):
        for exc in (errors.BadRequestError(request(), 'PRIVATE_SECRET', 400),
                    RuntimeError('PRIVATE_SECRET session=PRIVATE_SESSION'), HTTPException(403, 'PRIVATE_SECRET')):
            result = classify_invite_failure(exc, 'invite_rpc')
            self.assertEqual(result['queue_action'], 'stop')
            for secret in ('PRIVATE_SECRET', 'PRIVATE_SESSION', '123456789', '987654321', 'access_hash'):
                self.assertNotIn(secret, json.dumps(result))

    def test_missing_invitees_all_flag_combinations(self):
        for premium in (False, True):
            for pm in (False, True):
                result = missing_invitee_diagnosis(SimpleNamespace(premium_would_allow_invite=premium, premium_required_for_pm=pm))
                self.assertEqual(result['code'], 'premium_required_for_invite' if premium else 'privacy_restricted')
                self.assertEqual(result['premium_required_for_pm'], pm)


class DiagnosticsTests(ManualFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET error=NULL WHERE id='primary'")

    async def launch(self, count=2, cfg=None):
        self.saved_job = await self.manual.quick_draft(QuickDraftRequest(config=cfg or self.config(count), request_key='diagnostics-draft-001'))
        self.start_spec = QuickStartRequest(confirmed=True, job_id=self.saved_job['job_id'],
            config_digest=self.saved_job['config_digest'], request_key='diagnostics-start-001')
        await self.manual.quick_start(self.start_spec)
        await self.run_worker()
        return await self.manual.status(self.saved_job['job_id'])

    def failure(self, exc):
        self.client.failures[self.client.users[0].id] = exc

    async def test_peer_flood_survives_spambot_and_general_resume(self):
        self.failure(errors.PeerFloodError(request()))
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'PEER_FLOOD')
        self.assertEqual(result['stop_diagnosis']['phase'], 'invite_rpc')
        self.assertEqual(result['counts']['added'], 0)
        self.assertEqual(result['items'][1]['state'], 'pending')
        self.assertIsNone(result['direct_invite_restriction']['wait_until'])
        from spambot_check import PREFIX, SENDER_PREFIX, SCOPE
        self.ops.store.put(SENDER_PREFIX + str(SENDER), 'saved-spambot-001')
        self.ops.store.put(PREFIX + 'saved-spambot-001', dict(scope=SCOPE, account='primary', telegram_id=SENDER,
            state='reply_received', checked_at='2026-11-24T00:00:00Z', replies=[dict(text='No limits', date='2026-11-24T00:00:01Z')]))
        await self.ops.resume_primary_outbound(ResumeOutbound(confirmed=True, telegram_id=SENDER, reason='Separate general outbound review'))
        result = await self.manual.status(result['job_id'])
        self.assertEqual(result['spambot']['state'], 'reply_received')
        self.assertFalse(result['can_resume'])
        from operation_workbench import WorkbenchStopped
        with self.assertRaises(WorkbenchStopped):
            self.ops.workbench.guard(self.ops.accounts['primary'],
                dict(id=result['job_id'], account='primary', telegram_id=SENDER, kind='invite', spec={}), result['items'][1])
        self.manual.initialize()
        with self.assertRaises(HTTPException):
            await self.manual.quick_start(self.start_spec.model_copy(update={'request_key': 'diagnostics-resume-001'}))
        self.assertEqual(len(self.client.calls), 1)

    async def test_flood_wait_persists_separately_from_interval(self):
        self.failure(errors.FloodWaitError(request(), capture=120))
        result = await self.launch()
        wait = self.ops.account_row('primary')['not_before']
        self.assertEqual(result['stop_reason'], 'FLOOD_WAIT')
        self.assertGreaterEqual(wait, self.wall + 120)
        self.assertEqual(result['telegram_wait_until'], wait)
        self.manual.initialize()
        self.assertEqual(self.manual.record(result['job_id'])['telegram_wait_until'], wait)
        self.assertEqual(self.ops.account_row('primary')['not_before'], wait)
        self.assertFalse(result['can_resume'])

    async def test_interval_does_not_create_account_flood_wait(self):
        result = await self.launch()
        self.assertEqual(self.ops.account_row('primary')['not_before'], 0)
        self.assertEqual(result['counts']['added'], 2)
        self.assertGreater(result['next_at'], 0)
        self.assertIsNone(result['direct_invite_restriction'])
        self.assertEqual(result['next_at'], result['queue_not_before'])

    async def test_explicit_server_wait_does_not_inherit_an_unattributed_account_timer(self):
        cfg = self.config().model_dump()
        account = self.ops.accounts['primary']
        ctx = dict(config=cfg, account=account, client=self.client, phase='invite_rpc')
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET not_before=? WHERE id='primary'", (self.wall + 1000,))
        self.manual.protect(ctx, errors.FloodWaitError(request(), capture=120))
        restriction = self.manual.invite_restriction(cfg)
        self.assertEqual(restriction['wait_until'], self.wall + 121)
        self.assertEqual(self.ops.account_row('primary')['not_before'], self.wall + 1000)
        self.manual.protect(ctx, errors.FloodWaitError(request(), capture=30))
        self.assertEqual(self.manual.invite_restriction(cfg)['wait_until'], self.wall + 121)

    async def test_unknown_rpc_stops_with_safe_metadata_and_no_repeat(self):
        self.failure(errors.BadRequestError(request(), 'PRIVATE_SECRET', 400))
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'unknown_rpc_error')
        self.assertEqual(result['items'][0]['state'], 'blocked')
        self.assertFalse(result['items'][0]['diagnosis']['request_outcome_unknown'])
        self.assertIsNone(result['stop_diagnosis']['rpc']['message'])
        self.assertEqual(result['items'][1]['state'], 'pending')
        await self.manual.quick_start(self.start_spec)
        self.assertEqual(len(self.client.calls), 1)
        self.assertNotIn('PRIVATE_SECRET', json.dumps(result))

    async def test_target_error_stops_after_one_rpc(self):
        self.failure(errors.ChannelInvalidError(request()))
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'CHANNEL_INVALID')
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(result['items'][1]['state'], 'pending')

    async def test_revoked_session_stops_and_protects_sender(self):
        self.failure(errors.SessionRevokedError(request()))
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'SESSION_REVOKED')
        self.assertFalse(result['can_resume'])
        self.assertEqual(len(self.client.calls), 1)

    async def test_timeout_after_dispatch_stops_next_request(self):
        self.failure(TimeoutError('PRIVATE_SECRET'))
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'transport_timeout')
        self.assertEqual(result['items'][0]['state'], 'uncertain')
        self.assertEqual(len(self.client.calls), 1)

    async def test_recipient_error_in_verification_stops(self):
        original = self.client.get_permissions
        async def permissions(group, user):
            if self.client.calls:
                raise errors.UserNotMutualContactError(request())
            return await original(group, user)
        self.client.get_permissions = permissions
        result = await self.launch()
        self.assertEqual(result['stop_reason'], 'USER_NOT_MUTUAL_CONTACT')
        self.assertEqual(result['stop_diagnosis']['phase'], 'membership_after')
        self.assertEqual(result['items'][0]['state'], 'uncertain')
        self.assertEqual(len(self.client.calls), 1)

    async def test_empty_missing_without_membership_is_not_success(self):
        self.client.ghost = True
        original_sleep = asyncio.sleep
        async def yield_without_delay(delay):
            await original_sleep(0)
        with patch('manual_invite_campaign.asyncio.sleep', new=yield_without_delay):
            result = await self.launch()
        self.assertEqual(result['counts']['added'], 0)
        self.assertEqual(result['stop_reason'], 'invite_membership_unconfirmed')
        self.assertEqual(len(self.client.calls), 1)

    async def test_latest_run_remains_distinct_after_resume(self):
        async def pause_once():
            if len(self.client.calls) == 1:
                await self.manual.pause(PauseRequest(job_id=self.saved_job['job_id']))
        self.client.invite_hook = pause_once
        result = await self.launch(3)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['last_run']['added'], 1)
        for user in self.client.users[1:3]:
            self.client.failures[user.id] = errors.UserNotMutualContactError(request())
        await self.manual.quick_start(self.start_spec.model_copy(update={'request_key': 'diagnostics-resume-001'}))
        await self.run_worker()
        result = await self.manual.status(result['job_id'])
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['last_run']['added'], 0)
        self.assertEqual(result['last_run']['attempts'], 2)
        self.assertEqual(result['counts']['rejected'], 2)

    async def test_active_ban_skips_rpc_and_preserves_membership_flags(self):
        uid = self.client.users[0].id
        participant = types.ChannelParticipantBanned(peer=types.PeerUser(uid), kicked_by=SENDER,
            date=datetime.now(timezone.utc), left=True,
            banned_rights=types.ChatBannedRights(until_date=0, view_messages=True))
        original = self.client.get_permissions
        async def permissions(group, user):
            return ParticipantPermissions(participant, False) if user.id == uid else await original(group, user)
        self.client.get_permissions = permissions
        result = await self.launch()
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['counts']['skipped_without_request'], 1)
        before = result['items'][0]['diagnosis']['member_before']
        self.assertEqual((before['target_id'], before['user_id']), (TARGET, uid))
        self.assertEqual((before['left'], before['view_messages'], before['active_ban']), (True, True, True))
        self.assertEqual(result['items'][0]['diagnosis']['code'], 'recipient_banned')
        self.assertEqual(len(self.client.calls), 1)

    async def test_participant_flags_and_missing_fields_are_not_guessed(self):
        uid = self.client.users[0].id
        for left, view, member in ((False, False, True), (True, False, False), (False, True, False), (True, True, False)):
            participant = types.ChannelParticipantBanned(peer=types.PeerUser(uid), kicked_by=SENDER,
                date=datetime.now(timezone.utc), left=left,
                banned_rights=types.ChatBannedRights(until_date=0, view_messages=view))
            self.client.get_permissions = AsyncMock(return_value=ParticipantPermissions(participant, False))
            result = await self.ops.read_membership(self.client, self.client.group, self.client.users[0])
            self.assertEqual(result['member'], member)
            self.assertEqual(result['active_ban'], view)
        del participant.left
        del participant.banned_rights.view_messages
        self.assertIsNone(participant_membership(SimpleNamespace(participant=participant)))
        result = await self.ops.read_membership(self.client, self.client.group, self.client.users[0])
        self.assertIsNone(result['active_ban'])
        self.assertIsNone(result['member'])

    async def test_current_readback_does_not_rewrite_history(self):
        self.failure(TimeoutError())
        result = await self.launch()
        item = result['items'][0]
        before = self.ops.store.rows('SELECT * FROM op_items WHERE id=?', (item['id'],))
        self.client.members.add(item['user_id'])
        readback = await self.ops.item_membership(result['job_id'], item['id'])
        self.assertTrue(readback['member'])
        self.assertFalse(readback['history_changed'])
        refreshed = await self.manual.status(result['job_id'])
        self.assertTrue(refreshed['items'][0]['current_membership']['member'])
        self.assertEqual(refreshed['items'][0]['state'], 'uncertain')
        self.assertEqual(refreshed['counts']['added'], 0)
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE id=?', (item['id'],)), before)
        self.assertEqual(len(self.client.calls), 1)

    async def test_missing_invitees_each_flag_combination_advances_without_additions(self):
        async def invoke(client, rpc):
            client.calls.append(rpc)
            index = next(i for i, user in enumerate(client.users) if user.id == rpc.users[0].id)
            return types.messages.InvitedUsers(types.Updates([], [], [], datetime.now(timezone.utc), 0),
                [types.MissingInvitee(user_id=rpc.users[0].id, premium_would_allow_invite=bool(index & 1), premium_required_for_pm=bool(index & 2))])
        with patch.object(type(self.client), '__call__', invoke):
            result = await self.launch(4)
        self.assertEqual(result['counts']['attempts'], 4)
        self.assertEqual(result['counts']['added'], 0)
        self.assertEqual(result['counts']['rejected'], 4)
        self.assertEqual(len(self.client.calls), 4)
        self.assertEqual([i['state'] for i in result['items']], ['restricted'] * 4)

    async def test_list_attempt_cap_and_skipped_rows_are_distinct(self):
        old_job = self.ops.store.rows('SELECT id FROM op_jobs LIMIT 1')[0]['id']
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?)', (TARGET, self.client.users[0].id, 'uncertain', old_job))
        cfg = self.config(30).model_copy(update={'max_attempts': 25, 'account_day_attempt_cap': 25})
        result = await self.launch(cfg=cfg)
        self.assertEqual(result['counts']['total'], 30)
        self.assertEqual(result['counts']['attempts'], 25)
        self.assertEqual(result['counts']['skipped_without_request'], 1)
        self.assertEqual(result['counts']['pending'], 4)
        self.assertEqual(result['stop_reason'], 'campaign_attempt_cap_reached')
        self.assertEqual(len(self.client.calls), 25)

    async def test_restart_and_repeated_start_do_not_repeat_an_inflight_claim(self):
        entered = asyncio.Event()
        async def blocked():
            entered.set()
            await asyncio.Event().wait()
        self.client.invite_hook = blocked
        saved = await self.manual.quick_draft(QuickDraftRequest(config=self.config(2), request_key='restart-draft-001'))
        start = QuickStartRequest(confirmed=True, job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='restart-start-001')
        await self.manual.quick_start(start)
        await asyncio.wait_for(entered.wait(), 5)
        self.manual.task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.manual.task
        self.manual.initialize()
        self.client.invite_hook = None
        await self.manual.quick_start(start)  # old key is cached, never a second write
        self.assertEqual(len(self.client.calls), 1)
        await self.manual.quick_start(start.model_copy(update={'request_key': 'restart-resume-001'}))
        await self.run_worker()
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['items'][0]['state'], 'uncertain')
        self.assertEqual(result['items'][1]['state'], 'invited')
        self.assertEqual(len(self.client.calls), 2)
        self.assertNotEqual(self.client.calls[0].users[0].id, self.client.calls[1].users[0].id)
