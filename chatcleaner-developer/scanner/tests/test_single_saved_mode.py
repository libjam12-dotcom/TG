"""Explicit single saved-contact mode: one claim/write, real flags, legacy parity."""
import asyncio
import copy
import json
import unittest
from pathlib import Path
import sys
SCANNER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCANNER))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from unittest.mock import patch
from fastapi import FastAPI, HTTPException
import httpx
from telethon import errors, functions
import isolated_invite as single
from isolated_contact_batch import BatchStart
from isolated_invite_campaign import CampaignStart
import test_isolated_invite as base


class SingleSavedModeTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = base.IsolatedInviteTests.asyncSetUp
    asyncTearDown = base.IsolatedInviteTests.asyncTearDown
    seed_other = base.IsolatedInviteTests.seed_other
    writes = base.IsolatedInviteTests.writes
    db_snapshot = base.IsolatedInviteTests.db_snapshot
    assert_isolation = base.IsolatedInviteTests.assert_isolation

    async def prepared_saved(self, key='saved-single-control-key-0001'):
        next(user for user in self.client.users if user.id == base.UID).mutual_contact = False
        preview = await self.module.preview(single.SCOPE, mode='saved_contacts')
        self.assertEqual(preview['selected']['user_id'], base.UID)
        self.assertIs(preview['selected']['mutual_contact'], False)
        spec = single.SingleContactAttempt(scope=single.SCOPE, mode='saved_contacts', confirmed=True,
            reason='Explicit one saved client contact control', request_key=key,
            candidate_token=preview['candidate_token'])
        return preview, spec

    async def test_saved_nonmutual_success_has_one_rpc_real_flags_and_paused_history(self):
        _, spec = await self.prepared_saved()
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'invited')
        self.assertEqual(result['mode'], 'saved_contacts')
        self.assertIs(result['sender_has_contact'], True)
        self.assertIs(result['mutual_contact'], False)
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(len(self.writes()[0].users), 1)
        self.assert_isolation(result['job_id'])
        job = await self.ops.detail(result['job_id'])
        diagnosis = job['items'][0]['diagnosis']
        self.assertEqual(diagnosis['code'], 'invite_member_verified')
        self.assertIs(diagnosis['contact_evidence']['mutual_contact'], False)
        self.assertEqual(diagnosis['isolated_control']['mode'], 'saved_contacts')
        record = self.ops.store.get(single.DAY_PREFIX+single.utc_day())
        self.assertEqual(record['version'], 2)
        self.assertEqual(record['mode'], 'saved_contacts')
        self.assertIs(record['contact_evidence']['mutual_contact'], False)
        self.assertEqual(self.ops.account_daily_count(single.ACCOUNT, single.utc_day()), 1)
        self.assertEqual((await self.module.attempt(spec))['job_id'], result['job_id'])
        await self.ops.tick()
        self.assertEqual(len(self.writes()), 1)
        self.assertFalse(any(isinstance(r, functions.contacts.AddContactRequest) for r in self.client.calls))

    async def test_saved_nonmutual_actual_rpc_failure_preserves_constructor_and_no_retry(self):
        _, spec = await self.prepared_saved()
        request = functions.channels.InviteToChannelRequest(self.client.group, [self.client.users[0]])
        self.client.invite_failure = errors.BadRequestError(request, 'CHAT_MEMBER_ADD_FAILED', 400)
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'failed')
        item = (await self.ops.detail(result['job_id']))['items'][0]
        self.assertEqual(item['diagnosis']['rpc'], {'request_constructor': 'InviteToChannelRequest',
            'exception_type': 'BadRequestError', 'code': 400, 'message': 'CHAT_MEMBER_ADD_FAILED'})
        self.assertIs(item['diagnosis']['contact_evidence']['mutual_contact'], False)
        self.assertIs(item['diagnosis']['invitation_requested'], True)
        self.assert_isolation(result['job_id'])
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_saved_token_cannot_cross_mode_or_account_target_identity(self):
        _, spec = await self.prepared_saved()
        for key, value in (('mode', 'mutual'), ('account', 'other'), ('sender_id', 123), ('target_id', -999), ('member_before', True)):
            with self.subTest(binding=key):
                original = self.module.candidate[key]
                if key == 'mode':
                    changed_spec = spec.model_copy(update={'mode': value})
                else:
                    self.module.candidate[key] = value
                    changed_spec = spec
                calls = len(self.client.calls)
                with self.assertRaises(HTTPException):
                    await self.module.attempt(changed_spec)
                self.assertEqual(len(self.client.calls), calls)
                self.module.candidate[key] = original
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(single.DAY_PREFIX+single.utc_day()))

    async def test_default_mutual_stays_strict_and_inherited_models_are_unchanged(self):
        self.client.users[0].mutual_contact = False
        data = await self.module.preview(single.SCOPE)
        self.assertIsNone(data['selected'])
        self.assertEqual(data['mode'], 'mutual')
        self.assertFalse(self.module.suitable(self.client.users[0]))
        self.assertNotIn('mode', single.IsolatedAttempt.model_fields)
        self.assertNotIn('mode', BatchStart.model_fields)
        self.assertEqual(CampaignStart.model_fields['mode'].default, 'mutual')
        self.assertFalse(self.writes())

    async def test_default_mode_replays_legacy_digest_without_rpc_or_new_claim(self):
        legacy = single.IsolatedAttempt(scope=single.SCOPE, confirmed=True, reason='Previously confirmed mutual control',
            request_key='legacy-isolated-key-0001', candidate_token='x'*43)
        old = {'version': 1, 'state': 'finished', 'scope': single.SCOPE, 'request_key': legacy.request_key,
            'digest': single.digest(legacy.model_dump()), 'utc_day': single.utc_day(), 'job_id': 998, 'item_id': 999,
            'user_id': base.UID, 'result_state': 'invited', 'invitation_requested': True}
        self.ops.store.put(single.REQUEST_PREFIX+legacy.request_key, old)
        spec = single.SingleContactAttempt(**legacy.model_dump())
        result = await self.module.attempt(spec)
        self.assertEqual(result['job_id'], 998)
        self.assertEqual(result['mode'], 'mutual')
        self.assertFalse(self.client.calls)
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec.model_copy(update={'mode': 'saved_contacts'}))
        self.assertFalse(self.client.calls)

    async def test_modes_share_one_day_claim_and_all_prior_pending_history_is_excluded(self):
        old_pending = base.person(222333444, mutual_contact=False)
        self.client.users = [old_pending, self.client.users[0]]
        self.client.saved_ids.add(old_pending.id)
        preview, spec = await self.prepared_saved()
        self.assertEqual(preview['membership_checks'], 1)
        result = await self.module.attempt(spec)
        with self.assertRaises(HTTPException):
            await self.module.preview(single.SCOPE)
        with self.assertRaises(HTTPException):
            await self.module.preview(single.SCOPE, 'saved_contacts')
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec.model_copy(update={'mode': 'mutual', 'request_key': 'other-mutual-key-0001'}))
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.ops.store.rows('SELECT count(*) AS count FROM op_items WHERE job=?', (result['job_id'],))[0]['count'], 1)

    async def test_saved_fresh_contact_loss_blocks_before_claim_or_rpc(self):
        _, spec = await self.prepared_saved()
        self.client.users[0].contact = False
        with self.assertRaises(HTTPException):
            await self.module.attempt(spec)
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(single.DAY_PREFIX+single.utc_day()))

    async def test_saved_normal_budget_is_never_campaign_100(self):
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=?',
                (json.dumps({'daily_limit': 1, 'delay_seconds': 60}), self.old_job))
            self.ops.store.db.execute('UPDATE op_items SET attempted_at=? WHERE job=?',
                (single.utc_day()+'T00:00:00+00:00', self.old_job))
        self.client.users[0].mutual_contact = False
        with self.assertRaises(HTTPException):
            await self.module.preview(single.SCOPE, 'saved_contacts')
        self.assertFalse(self.writes())

    async def test_saved_unknown_keeps_one_consumed_claim_and_real_flags(self):
        _, spec = await self.prepared_saved()
        self.client.invite_failure = TimeoutError('PRIVATE_TIMEOUT')
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'uncertain')
        item = (await self.ops.detail(result['job_id']))['items'][0]
        self.assertIs(item['diagnosis']['request_outcome_unknown'], True)
        self.assertIs(item['diagnosis']['contact_evidence']['mutual_contact'], False)
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_saved_durable_mode_and_flag_mutation_is_rejected_by_guard(self):
        _, spec = await self.prepared_saved()
        deadline = __import__('time').monotonic()+80
        ctx = self.module.context(deadline)
        record = self.module.claim(ctx, spec, self.client.group, self.client.users[0], 'test-digest')
        for key, value in (('mode', 'mutual'), ('contact_evidence', {'sender_has_contact': True, 'mutual_contact': True, 'checked_at': record['claimed_at']})):
            changed = copy.deepcopy(record); changed[key] = value
            for prefix in (single.DAY_PREFIX+single.utc_day(), single.REQUEST_PREFIX+spec.request_key):
                self.ops.store.put(prefix, changed)
            with self.assertRaises(HTTPException): self.module.guard(ctx)
            for prefix in (single.DAY_PREFIX+single.utc_day(), single.REQUEST_PREFIX+spec.request_key):
                self.ops.store.put(prefix, record)
        self.assertFalse(self.writes())

    async def test_http_explicit_saved_mode_and_unknown_mode_schema(self):
        self.client.users[0].mutual_contact = False
        app = FastAPI(); app.include_router(self.ops.router)
        headers = {'x-panel-user': 'owner', 'x-panel-role': 'owner'}
        prefix = '/api/scanner/operations/isolated-invite'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture') as client:
            bad = await client.get(prefix+'/contacts-preview?scope='+single.SCOPE+'&mode=all', headers=headers)
            self.assertEqual(bad.status_code, 422)
            response = await client.get(prefix+'/contacts-preview?scope='+single.SCOPE+'&mode=saved_contacts', headers=headers)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            payload = {'scope': single.SCOPE, 'mode': 'saved_contacts', 'confirmed': True,
                'reason': 'Explicit one saved contact request', 'request_key': 'http-saved-key-0001',
                'candidate_token': data['candidate_token']}
            response = await client.post(prefix+'/attempt', headers=headers, json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertIs(response.json()['mutual_contact'], False)
        self.assertEqual(len(self.writes()), 1)


if __name__ == '__main__':
    unittest.main()
