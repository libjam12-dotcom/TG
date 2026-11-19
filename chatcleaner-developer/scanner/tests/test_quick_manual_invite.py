"""Quick manual mode: native SQLite history with fake Telegram only."""
import asyncio
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

CANDIDATE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(CANDIDATE/'scanner')]
import manual_invite_campaign
import operations
from fastapi import HTTPException
from telethon import errors, types
from manual_invite_campaign import QuickDraftRequest, QuickStartRequest, DraftRequest, StartRequest, PauseRequest, RESOLVED_PREFIX

spec = importlib.util.spec_from_file_location('legacy_manual_behavior', CANDIDATE/'scanner/tests/test_manual_invite_campaign.py')
legacy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(legacy)


class QuickTests(legacy.ManualTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.entities = []
        original = self.client.get_entity
        async def tracked(ref):
            self.entities.append(ref)
            return await original(ref)
        self.client.get_entity = tracked

    def recipient_reads(self):
        return [ref for ref in self.entities if isinstance(ref, (str, types.InputPeerUser))]

    async def quick_saved(self, count=20, cfg=None, key='quick-draft-00001'):
        return await self.manual.quick_draft(QuickDraftRequest(config=cfg or self.config(count), request_key=key))

    async def quick_run(self, saved, key='quick-start-00001'):
        req = QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key=key, confirmed=True)
        result = await self.manual.quick_start(req)
        await self.run_worker()
        return await self.manual.status(saved['job_id']), req

    async def test_empty_response_reports_received_response_without_claiming_privacy(self):
        self.client.ghost = True
        saved = await self.quick_saved(count=1)
        await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='empty-response-001', confirmed=True))
        await self.manual.task
        result = await self.manual.status(saved['job_id'])
        item = result['items'][0]
        self.assertEqual(item['state'], 'uncertain')
        self.assertIn('Telegram вернул ответ', item['diagnosis']['reason'])
        self.assertEqual(len(item['diagnosis']['verification']), 3)
        self.assertFalse(item['diagnosis']['retry_after_change'])
        self.assertEqual(len(self.client.calls), 1)

    async def test_quick_historical_retry_requires_explicit_flag_and_preserves_history(self):
        self.history_item('restricted', {'code':'UserPrivacyRestrictedError', 'retry_after_change':True})
        saved = await self.quick_saved(cfg=self.config(1, retry_after_change_confirmed=True))
        result, req = await self.quick_run(saved)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(len(self.client.calls), 1)
        await self.manual.quick_start(req)
        self.assertEqual(len(self.client.calls), 1)

    async def test_quick_uncertain_history_stays_blocked_despite_confirmation(self):
        self.history_item('uncertain')
        saved = await self.quick_saved(cfg=self.config(1, retry_after_change_confirmed=True))
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['items'][0]['diagnosis']['code'], 'target_history_unresolved')
        self.assertFalse(result['items'][0]['diagnosis']['member_before']['member'])
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(self.client.calls, [])

    async def test_confirmed_retry_does_not_bypass_sender_error_or_unknown_membership(self):
        self.history_item('already_member')
        saved = await self.quick_saved(cfg=self.config(1, retry_after_change_confirmed=True))
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET error='PeerFloodError' WHERE id='primary'")
        with self.assertRaises(HTTPException):
            await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='sender-block-00001', confirmed=True))
        self.assertEqual(self.client.calls, [])
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET error=NULL WHERE id='primary'")
        self.ops.read_membership = AsyncMock(return_value={'member':None, 'checked_at':'2026-11-19'})
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(self.client.calls, [])

    async def test_quick_draft_and_start_no_upfront_user_reads_first_invite_precedes_next_resolve(self):
        saved = await self.quick_saved()
        self.assertEqual(saved['resolution'], 'on_dispatch')
        self.assertEqual(self.recipient_reads(), [])
        self.assertEqual(self.client.calls, [])
        self.assertTrue(all(i['user_id'] is None for i in saved['items']))
        self.assertTrue(all(i['user_access_hash'] is None for i in self.manual.items(saved['job_id'])))
        async def before_rpc():
            n = len(self.client.calls)
            self.assertEqual(self.recipient_reads(), ['person_'+str(i) for i in range(n)])
            current = self.manual.items(saved['job_id'])[n-1]
            seal = self.ops.store.get(RESOLVED_PREFIX+str(current['id']))
            self.assertEqual(seal['user_id'], current['user_id'])
            self.assertEqual(seal['user_access_hash'], current['user_access_hash'])
        self.client.invite_hook = before_rpc
        req = QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='quick-start-00001', confirmed=True)
        started = await self.manual.quick_start(req)
        self.assertEqual(self.recipient_reads(), [])
        self.assertEqual(started['counts']['attempts'], 0)
        await self.run_worker()
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['counts']['added'], 20, result)
        self.assertEqual(result['counts']['attempts'], 20)
        self.assertEqual(len(self.client.calls), 20)
        self.assertEqual(len(self.ops.store.rows('SELECT key FROM settings WHERE key LIKE ?', (RESOLVED_PREFIX+'%',))), 20)
        self.isolated(saved['job_id'])

    async def test_cached_draft_request_recovery_never_contacts_telegram(self):
        saved = await self.quick_saved(2)
        self.client.get_entity = AsyncMock(side_effect=AssertionError('cached GET only'))
        self.client.get_me = AsyncMock(side_effect=AssertionError('cached GET only'))
        response = await self.http.get('/api/scanner/operations/manual-invite-campaign/draft-requests/quick-draft-00001')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['job_id'], saved['job_id'])
        missing = await self.http.get('/api/scanner/operations/manual-invite-campaign/draft-requests/missing-request')
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(self.client.calls, [])

    async def test_quick_draft_key_idempotent_and_changed_config_rejected(self):
        saved = await self.quick_saved(2)
        self.client.get_entity = AsyncMock(side_effect=AssertionError('duplicate save must be cached'))
        same = await self.quick_saved(2)
        self.assertEqual(saved['job_id'], same['job_id'])
        with self.assertRaises(HTTPException):
            await self.quick_saved(3)
        self.assertEqual(self.recipient_reads(), [])
        self.assertEqual(self.client.calls, [])

    async def test_cross_mode_start_and_preview_rejected(self):
        quick = await self.quick_saved(2)
        with self.assertRaises(HTTPException):
            await self.manual.preview_job(quick['job_id'])
        with self.assertRaises(HTTPException):
            await self.manual.start(StartRequest(job_id=quick['job_id'], candidate_token='a'*43, request_key='wrong-full-start1', confirmed=True))
        full_preview, full = await self.saved(self.config(2))
        with self.assertRaises(HTTPException):
            await self.manual.quick_start(QuickStartRequest(job_id=full['job_id'], config_digest=full['config_digest'], request_key='wrong-quick-start', confirmed=True))
        with self.assertRaises(HTTPException):
            await self.manual.quick_draft(QuickDraftRequest(config=self.config(2), request_key='draft-request-0001'))
        with self.assertRaises(HTTPException):
            await self.manual.draft(DraftRequest(config=self.config(2), candidate_token=full_preview['candidate_token'], request_key='quick-draft-00001'))
        self.assertEqual(self.client.calls, [])

    async def test_quick_unresolved_uid_or_name_tamper_rejected(self):
        saved = await self.quick_saved(2)
        first = self.manual.items(saved['job_id'])[0]
        for sql, params in [
            ('UPDATE op_items SET user_id=? WHERE id=?', (123, first['id'])),
            ('UPDATE op_items SET user_name=? WHERE id=?', ('changed_name', first['id']))]:
            with self.ops.store.db:
                self.ops.store.db.execute(sql, params)
            with self.assertRaises(HTTPException):
                await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='tampered-start-01', confirmed=True))
            with self.ops.store.db:
                self.ops.store.db.execute('UPDATE op_items SET user_id=NULL,user_access_hash=NULL,user_name=? WHERE id=?', ('person_0',first['id']))
        self.assertEqual(self.recipient_reads(), [])
        self.assertEqual(self.client.calls, [])

    async def test_quick_duplicate_alias_never_dispatches_twice(self):
        saved = await self.quick_saved(2)
        original = self.client.get_entity
        async def alias(ref):
            if ref == 'person_1':
                return types.User(id=self.client.users[0].id, username='person_1', access_hash=100, first_name='alias')
            return await original(ref)
        self.client.get_entity = alias
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['added'], 1, result)
        self.assertEqual(result['counts']['duplicate_skipped'], 1)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.manual.job(saved['job_id'])

    async def test_quick_canonical_username_mismatch_stops_before_claim(self):
        saved = await self.quick_saved(2)
        self.client.users[0].username = 'different_person'
        original = self.client.get_entity
        async def stale(ref):
            if ref == 'person_0':
                return self.client.users[0]
            return await original(ref)
        self.client.get_entity = stale
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(result['counts']['attempts'], 0)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(len(self.recipient_reads()), 0)  # override intercepted stale name
        self.assertTrue(all(i['user_id'] is None for i in result['items']))

    async def test_quick_sealed_uid_and_hash_tamper_rejected_before_resume(self):
        saved = await self.quick_saved(2)
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.ops.read_membership
        async def gate(client, group, user):
            entered.set()
            await release.wait()
            return await original(client, group, user)
        self.ops.read_membership = gate
        await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='seal-pause-start1', confirmed=True))
        await entered.wait()
        await self.manual.pause(PauseRequest(job_id=saved['job_id']))
        release.set()
        await self.manual.task
        first = self.manual.items(saved['job_id'])[0]
        self.assertIsNotNone(first['user_id'])
        self.assertIsNotNone(self.ops.store.get(RESOLVED_PREFIX+str(first['id'])))
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_items SET user_access_hash=user_access_hash+1 WHERE id=?', (first['id'],))
        with self.assertRaises(HTTPException):
            await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='seal-resume-0001', confirmed=True))
        self.assertEqual(self.client.calls, [])

    async def test_quick_privacy_advances_unknown_stops_and_idempotence_never_retries(self):
        saved = await self.quick_saved(4)
        self.client.failures[self.client.users[0].id] = errors.UserPrivacyRestrictedError(None)
        self.client.failures[self.client.users[1].id] = RuntimeError('unknown')
        result, req = await self.quick_run(saved)
        self.assertEqual(result['counts']['restricted'], 1)
        self.assertEqual(result['counts']['uncertain'], 1)
        self.assertEqual(result['counts']['pending'], 2)
        self.assertEqual(len(self.client.calls), 2)
        await self.manual.quick_start(req)
        self.assertEqual(len(self.client.calls), 2)
        self.wall = result['next_at']+.02
        result, _ = await self.quick_run(saved, 'quick-resume-0001')
        self.assertEqual(result['counts']['added'], 2)
        self.assertEqual(len(self.client.calls), 4)
        self.assertEqual(len(set(r.users[0].id for r in self.client.calls)), 4)
        self.isolated(saved['job_id'])

    async def test_quick_known_recipient_privacy_before_seal_advances_without_attempt(self):
        saved = await self.quick_saved(2)
        original = self.client.get_entity
        async def restricted(ref):
            if ref == 'person_0':
                raise errors.UserPrivacyRestrictedError(None)
            return await original(ref)
        self.client.get_entity = restricted
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['restricted'], 1)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['items'][0]['user_id'], None)
        self.assertFalse(result['items'][0]['diagnosis']['invitation_requested'])
        self.assertEqual(len(self.client.calls), 1)
        self.manual.job(saved['job_id'])

    async def test_quick_secondary20_exact_identity_target_and_verified_goal(self):
        secondary = legacy.Client()
        secondary.me = types.User(id=8492521342, username='holoptic_Fidejussion')
        self.ops.accounts['selected-secondary'] = SimpleNamespace(client=secondary, me=secondary.me, error=None)
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_accounts(id,label,telegram_id,username,enabled,created) VALUES(?,?,?,?,?,?)',
                ('selected-secondary', 'Selected secondary', secondary.me.id, secondary.me.username, 1, 'old'))
        cfg = manual_invite_campaign.ManualConfig(**dict(self.config(20).model_dump(), account='selected-secondary', telegram_id=secondary.me.id, success_goal=20))
        saved = await self.quick_saved(cfg=cfg)
        self.assertEqual(saved['target']['id'], -1001234501001)
        self.assertEqual(saved['telegram_id'], 8492521342)
        self.assertEqual(secondary.calls, [])
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['added'], 20, result)
        self.assertEqual(result['counts']['attempts'], 20)
        self.assertEqual(len(secondary.calls), 20)
        self.assertEqual(self.client.calls, [])
        self.assertTrue(all(r.channel is secondary.group for r in secondary.calls))
        self.assertEqual(self.manual.projection_for(legacy.SENDER), self.old)
        self.assertEqual(self.ops.account_row('primary')['enabled'], 0)
        self.ops.accounts.pop('selected-secondary')

    async def test_quick_already_member_never_counts_goal(self):
        self.client.members.add(self.client.users[0].id)
        saved = await self.quick_saved(cfg=self.config(3, success_goal=1))
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['already_member'], 1)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['counts']['pending'], 1)
        self.assertEqual(self.recipient_reads(), ['person_0', 'person_1'])

    async def test_quick_peerflood_stops_before_next_resolution(self):
        saved = await self.quick_saved(3)
        self.client.failures[self.client.users[0].id] = errors.PeerFloodError(None)
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(self.recipient_reads(), ['person_0'])
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary'), 1)

    async def test_quick_floodwait_retains_server_delay_and_stops(self):
        saved = await self.quick_saved(3)
        self.client.failures[self.client.users[0].id] = errors.FloodWaitError(None, capture=600)
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(self.recipient_reads(), ['person_0'])
        self.assertEqual(len(self.client.calls), 1)
        self.assertGreaterEqual(self.ops.account_row('primary')['not_before'], self.wall+590)

    async def test_quick_revoked_session_stops_before_next_resolution(self):
        saved = await self.quick_saved(3)
        self.client.failures[self.client.users[0].id] = errors.SessionRevokedError(None)
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(self.recipient_reads(), ['person_0'])
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.ops.store.get('outbound_pause_revision_primary'), 1)

    async def test_quick_daycap_stops_before_reading_next_user(self):
        saved = await self.quick_saved(3, cfg=manual_invite_campaign.ManualConfig(**dict(self.config(3).model_dump(), account_day_attempt_cap=1)))
        result, _ = await self.quick_run(saved)
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(self.recipient_reads(), ['person_0'])
        self.assertEqual(len(self.client.calls), 1)
        with self.assertRaises(HTTPException):
            await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='cap-resume-00001', confirmed=True))
        self.assertEqual(self.recipient_reads(), ['person_0'])

    async def test_quick_pause_during_rpc_preserves_result_no_next_resolve_and_explicit_resume(self):
        saved = await self.quick_saved(3)
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow():
            entered.set()
            await release.wait()
        self.client.invite_hook = slow
        await self.manual.quick_start(QuickStartRequest(job_id=saved['job_id'], config_digest=saved['config_digest'], request_key='quick-pause-0001', confirmed=True))
        await entered.wait()
        r = await asyncio.wait_for(self.manual.pause(PauseRequest(job_id=saved['job_id'])), .1)
        self.assertTrue(r['pause_requested'])
        release.set()
        await self.manual.task
        result = await self.manual.status(saved['job_id'])
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(self.recipient_reads(), ['person_0'])
        self.wall = result['next_at']+.02
        self.client.invite_hook = None
        result, _ = await self.quick_run(saved, 'quick-pause-resume')
        self.assertEqual(result['counts']['added'], 3)
        self.assertFalse(result['pause_requested'])
        self.assertEqual(len(self.client.calls), 3)
        self.isolated(saved['job_id'])


if __name__ == '__main__':
    unittest.main()
