"""Explicit local recovery must never enable global outbound or dispatch a write."""
from unittest.mock import AsyncMock
from fastapi import HTTPException
from pydantic import ValidationError
from telethon import errors, functions, types
from fixture_manual import ManualFixture, SENDER
from manual_invite_campaign import QuickDraftRequest, QuickStartRequest, ReviewRestriction, REVIEW_PREFIX, RESTRICTION_PREFIX


class RecoveryTests(ManualFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET error=NULL WHERE id='primary'")
        self.client.failures[self.client.users[0].id] = errors.PeerFloodError(functions.channels.InviteToChannelRequest(self.client.group, [self.client.users[0]]))
        saved = await self.manual.quick_draft(QuickDraftRequest(config=self.config(2), request_key='recovery-draft-001'))
        self.start_spec = QuickStartRequest(confirmed=True, job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='recovery-start-001')
        await self.manual.quick_start(self.start_spec)
        await self.run_worker()
        self.stopped = await self.manual.status(saved['job_id'])
        self.review = ReviewRestriction(job_id=saved['job_id'], confirmed=True,
            restriction_digest=self.stopped['restriction_review_digest'], request_key='recovery-review-001',
            reason='Administrator reviewed Telegram before a separate controlled start')

    async def test_review_preserves_global_pause_history_and_requires_separate_start(self):
        history = {table: self.ops.store.rows('SELECT * FROM ' + table) for table in ('op_jobs', 'op_items', 'op_invited')}
        result = await self.manual.review_restriction(self.review)
        self.assertTrue(result['can_resume'])
        self.assertIsNone(result['direct_invite_restriction'])
        self.assertIsNone(result['sender_error'])
        self.assertEqual(result['stop_reason'], 'PEER_FLOOD')
        self.assertTrue(result['outbound_paused'])
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.assertEqual(history, {table: self.ops.store.rows('SELECT * FROM ' + table) for table in history})
        audit = self.ops.store.get(REVIEW_PREFIX + self.review.request_key)
        self.assertFalse(audit['method_permission_verified'])
        self.assertFalse(audit['general_outbound_enabled'])
        self.assertFalse(audit['jobs_resumed'])
        self.assertEqual(audit['restriction']['code'], 'PEER_FLOOD')
        self.assertEqual(len(self.client.calls), 1)
        await self.manual.quick_start(self.start_spec.model_copy(update={'request_key': 'recovery-start-002'}))
        await self.run_worker()
        self.assertEqual(len(self.client.calls), 2)
        self.assertNotEqual(self.client.calls[0].users[0].id, self.client.calls[1].users[0].id)
        self.assertEqual((await self.manual.status(result['job_id']))['counts']['added'], 1)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)

    async def test_replay_cannot_clear_a_newer_failure_and_key_is_payload_bound(self):
        self.client.get_me = AsyncMock(return_value=self.client.me)
        await self.manual.review_restriction(self.review)
        self.client.get_me.assert_awaited_once()
        cfg = self.manual.record(self.review.job_id)['config']
        self.manual.protect(dict(config=cfg, account=self.ops.accounts['primary'], client=self.client, phase='invite_rpc'), errors.PeerFloodError(None))
        new_failure = self.manual.invite_restriction(cfg)
        result = await self.manual.review_restriction(self.review)
        self.assertFalse(result['can_resume'])
        self.assertEqual(self.manual.invite_restriction(cfg), new_failure)
        self.client.get_me.assert_awaited_once()
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review.model_copy(update={'reason': 'A different review reason'}))

    async def test_stale_digest_never_reads_or_clears(self):
        self.client.get_me = AsyncMock(return_value=self.client.me)
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review.model_copy(update={'restriction_digest': '0' * 64}))
        self.client.get_me.assert_not_awaited()
        self.assertIsNotNone(self.manual.invite_restriction(self.manual.record(self.review.job_id)['config']))

    async def test_flood_wait_is_not_shortened_or_cleared(self):
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET not_before=? WHERE id='primary'", (self.wall + 600,))
        self.client.get_me = AsyncMock(return_value=self.client.me)
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        self.assertEqual(self.ops.account_row('primary')['not_before'], self.wall + 600)
        self.client.get_me.assert_not_awaited()

    async def test_enabled_global_queue_and_unrelated_session_errors_refuse_review(self):
        self.client.get_me = AsyncMock(return_value=self.client.me)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=1 WHERE id='primary'")
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0,error='SessionRevokedError' WHERE id='primary'")
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        self.client.get_me.assert_not_awaited()

    async def test_new_pause_during_identity_read_wins_over_review(self):
        cfg = self.manual.record(self.review.job_id)['config']
        async def fresh_failure():
            self.manual.protect(dict(config=cfg, account=self.ops.accounts['primary'], client=self.client, phase='invite_rpc'), errors.PeerFloodError(None))
            return self.client.me
        self.client.get_me = AsyncMock(side_effect=fresh_failure)
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        self.assertIsNotNone(self.manual.invite_restriction(cfg))
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)

    async def test_changed_identity_does_not_clear_pause(self):
        self.client.get_me = AsyncMock(return_value=types.User(id=SENDER + 10))
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        self.assertIsNotNone(self.ops.account_row('primary')['error'])

    async def test_confirmation_and_authorization_are_required(self):
        with self.assertRaises(ValidationError):
            ReviewRestriction(**dict(self.review.model_dump(), confirmed=False))
        response = await self.http.post('/api/scanner/operations/manual-invite-campaign/review-restriction',
            json=self.review.model_dump(), headers={'x-panel-role': 'client'})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(len(self.client.calls), 1)

    async def test_own_sending_item_blocks_review(self):
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_items SET state='sending' WHERE id=?", (self.stopped['items'][0]['id'],))
        self.client.get_me = AsyncMock(return_value=self.client.me)
        with self.assertRaises(HTTPException):
            await self.manual.review_restriction(self.review)
        self.client.get_me.assert_not_awaited()

    async def test_exact_legacy_method_error_is_reviewable_without_rewriting_history(self):
        with self.ops.store.db:
            self.ops.store.db.execute('DELETE FROM settings WHERE key=?', (RESTRICTION_PREFIX + str(SENDER),))
        before = self.ops.store.rows('SELECT * FROM op_items WHERE job=?', (self.review.job_id,))
        legacy = await self.manual.status(self.review.job_id)
        self.assertEqual(legacy['direct_invite_restriction']['source'], 'legacy_rpc_history')
        self.assertTrue(legacy['can_review_restriction'])
        result = await self.manual.review_restriction(self.review.model_copy(update={'restriction_digest': legacy['restriction_review_digest']}))
        self.assertTrue(result['can_resume'])
        self.manual.initialize()
        result = await self.manual.status(self.review.job_id)
        self.assertIsNone(result['direct_invite_restriction'])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=?', (self.review.job_id,)), before)
        self.assertEqual(len(self.client.calls), 1)
