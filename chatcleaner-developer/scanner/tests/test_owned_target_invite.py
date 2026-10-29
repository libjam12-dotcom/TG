"""Offline exact-target/owned-recipient controls; no real API or Telegram calls."""
import copy
import json
import time
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
import httpx
from telethon import errors, functions

import isolated_invite as single
import owned_target_invite as owned
import test_isolated_invite as base


class OwnedClient(base.Client):
    def __init__(self):
        super().__init__()
        self.group.id = 720000001
        self.group.username = owned.TARGET_USERNAME
        self.group.title = 'ОАЭ ЧАТ | ДУБАЙ ЧАТИК'
        self.users = [base.person(owned.RECIPIENT, username='demo_identity_2', contact=False, mutual_contact=False)]
        self.saved_ids = set()

    async def get_entity(self, target):
        self.reads.append(('entity', target))
        if target == owned.TARGET:
            return self.group
        if target == owned.RECIPIENT_USERNAME:
            return self.users[0]
        raise AssertionError('Only the exact new group and owned technical recipient may be resolved')


class OwnedTargetTests(unittest.IsolatedAsyncioTestCase):
    seed_other = base.IsolatedInviteTests.seed_other
    writes = base.IsolatedInviteTests.writes
    db_snapshot = base.IsolatedInviteTests.db_snapshot
    assert_isolation = base.IsolatedInviteTests.assert_isolation
    asyncTearDown = base.IsolatedInviteTests.asyncTearDown

    async def asyncSetUp(self):
        self.addCleanup(patch.stopall)
        patch.object(single, 'utc_day', return_value=owned.VALID_UTC_DAY).start()
        patch.object(owned, 'utc_day', return_value=owned.VALID_UTC_DAY).start()
        patch.object(single, 'now', return_value=owned.VALID_UTC_DAY+'T12:00:00+00:00').start()
        patch.object(owned, 'now', return_value=owned.VALID_UTC_DAY+'T12:00:00+00:00').start()
        await base.IsolatedInviteTests.asyncSetUp(self)
        self.client = OwnedClient()
        self.runtime.client = self.client
        self.runtime.me = self.client.me
        self.module = self.ops.isolated_invite.owned_target

    async def prepared(self, key='owned-target-control-key-0001'):
        preview = await self.module.preview(owned.SCOPE)
        spec = owned.OwnedTargetAttempt(scope=owned.SCOPE, confirmed=True,
            reason='Owner requested exactly one owned technical account in the new group',
            request_key=key, candidate_token=preview['candidate_token'])
        return preview, spec

    async def test_preview_only_fixed_entities_no_contacts_or_invite_real_flags(self):
        preview, _ = await self.prepared()
        self.assertEqual(preview['scope'], owned.SCOPE)
        self.assertEqual(preview['target_id'], owned.TARGET)
        self.assertEqual(preview['mode'], owned.MODE)
        self.assertEqual(preview['selected']['user_id'], owned.RECIPIENT)
        self.assertIs(preview['selected']['sender_has_contact'], False)
        self.assertIs(preview['selected']['mutual_contact'], False)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.client.reads.count(('entity', owned.RECIPIENT_USERNAME)), 1)
        self.assertEqual(self.module.projection(), self.old_projection)

    async def test_success_one_rpc_new_target_and_paused_history_no_contact_write(self):
        _, spec = await self.prepared()
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'invited')
        self.assertEqual(result['scope'], owned.SCOPE)
        self.assertEqual(result['target_id'], owned.TARGET)
        self.assertEqual(result['mode'], owned.MODE)
        self.assertIs(result['sender_has_contact'], False)
        self.assertIs(result['mutual_contact'], False)
        self.assertEqual(len(self.writes()), 1)
        request = self.writes()[0]
        self.assertEqual(request.channel.id, 720000001)
        self.assertEqual([u.id for u in request.users], [owned.RECIPIENT])
        self.assert_isolation(result['job_id'])
        job = await self.ops.detail(result['job_id'])
        self.assertEqual(job['state'], 'paused')
        self.assertEqual(job['items'][0]['chat_id'], owned.TARGET)
        self.assertEqual(job['items'][0]['user_id'], owned.RECIPIENT)
        self.assertEqual(job['spec']['destinations'], [owned.TARGET])
        self.assertEqual(job['items'][0]['diagnosis']['isolated_control']['scope'], owned.SCOPE)
        await self.module.attempt(spec)
        await self.ops.tick()
        self.assertEqual(len(self.writes()), 1)
        self.assertFalse(any(isinstance(r, functions.contacts.AddContactRequest) for r in self.client.calls))

    async def test_wrong_uid_or_username_rejected_before_claim(self):
        for attr, value in (('id', 7777777), ('username', 'wrong_account')):
            with self.subTest(attr=attr):
                original = getattr(self.client.users[0], attr)
                setattr(self.client.users[0], attr, value)
                with self.assertRaises(HTTPException):
                    await self.module.preview(owned.SCOPE)
                self.module.next_preview = 0
                setattr(self.client.users[0], attr, original)
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(single.DAY_PREFIX+single.utc_day()))

    async def test_wrong_group_and_missing_rights_fail_before_owned_resolution(self):
        original = self.client.group.id
        self.client.group.id = 720000000
        with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
        self.client.group.id = original
        self.module.next_preview = 0
        self.client.group.admin_rights.invite_users = False
        with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
        self.assertNotIn(('entity', owned.RECIPIENT_USERNAME), self.client.reads)
        self.assertFalse(self.writes())

    async def test_token_scope_target_uid_and_day_cannot_be_substituted(self):
        _, spec = await self.prepared()
        for key, value in (('scope', single.SCOPE), ('target_id', single.TARGET),
                           ('user_id', 123123), ('day', '2026-10-27')):
            original = self.module.candidate[key]
            self.module.candidate[key] = value
            with self.subTest(binding=key), self.assertRaises(HTTPException):
                await self.module.attempt(spec)
            self.module.candidate[key] = original
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(single.DAY_PREFIX+single.utc_day()))

    async def test_fresh_user_change_after_preview_blocks_before_claim(self):
        _, spec = await self.prepared()
        self.client.users[0].contact = True
        with self.assertRaises(HTTPException): await self.module.attempt(spec)
        self.assertFalse(self.writes())
        self.assertIsNone(self.ops.store.get(single.DAY_PREFIX+single.utc_day()))

    async def test_already_member_or_history_blocks_new_invite(self):
        self.client.members.add(owned.RECIPIENT)
        with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
        self.client.members.clear()
        self.module.next_preview = 0
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?)',
                (owned.TARGET, owned.RECIPIENT, 'uncertain', self.old_job))
        with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
        self.assertFalse(self.writes())

    async def test_scope_expiry_fails_closed_before_rpc_and_status_stays_readable(self):
        _, spec = await self.prepared()
        with patch.object(owned, 'utc_day', return_value='2026-10-29'):
            with self.assertRaises(HTTPException): await self.module.attempt(spec)
            self.module.next_preview = 0
            with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
            state = await self.module.status(owned.SCOPE)
            self.assertEqual(state['state'], 'unused')
        self.assertFalse(self.writes())

    async def test_empty_response_is_unknown_not_success_and_never_retried(self):
        _, spec = await self.prepared()
        self.client.ghost = True
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'uncertain')
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)
        self.assertNotIn(owned.RECIPIENT, self.client.members)
        self.assert_isolation(result['job_id'])

    async def test_revoked_session_keeps_one_claim_and_protective_pause(self):
        _, spec = await self.prepared()
        request = functions.channels.InviteToChannelRequest(self.client.group, [self.client.users[0]])
        self.client.invite_failure = errors.SessionRevokedError(request)
        result = await self.module.attempt(spec)
        self.assertEqual(result['result_state'], 'uncertain')
        self.assertEqual(self.ops.account_row(single.ACCOUNT)['enabled'], 0)
        self.assertEqual(len(self.writes()), 1)
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_floodwait_is_preserved_not_shortened_or_retried(self):
        _, spec = await self.prepared()
        self.client.invite_failure = errors.FloodWaitError(None, capture=600)
        before = time.time()
        result = await self.module.attempt(spec)
        self.assertGreaterEqual(self.ops.account_row(single.ACCOUNT)['not_before'], before+600)
        self.assertEqual(result['result_state'], 'uncertain')
        await self.module.attempt(spec)
        self.assertEqual(len(self.writes()), 1)

    async def test_shared_daily_claim_legacy_status_preserves_new_scope_and_target(self):
        _, spec = await self.prepared()
        result = await self.module.attempt(spec)
        with self.assertRaises(HTTPException):
            await self.ops.isolated_invite.preview(single.SCOPE)
        status = await self.ops.isolated_invite.status(single.SCOPE)
        self.assertEqual(status['scope'], owned.SCOPE)
        self.assertEqual(status['target_id'], owned.TARGET)
        self.assertEqual(status['job_id'], result['job_id'])
        self.assertEqual(len(self.writes()), 1)

    async def test_legacy_claim_consumes_new_scope_and_legacy_status_never_claims_new_target(self):
        record = {'version': 1, 'scope': single.SCOPE, 'utc_day': single.utc_day(),
            'state': 'finished', 'request_key': 'legacy-key-owned-test', 'result_state': 'invited'}
        self.ops.store.put(single.DAY_PREFIX+single.utc_day(), record)
        with self.assertRaises(HTTPException): await self.module.preview(owned.SCOPE)
        status = await self.module.status(owned.SCOPE)
        self.assertEqual(status['scope'], single.SCOPE)
        self.assertEqual(status['target_id'], single.TARGET)
        self.assertFalse(self.client.calls)

    async def test_durable_claim_target_mutation_is_rejected(self):
        _, spec = await self.prepared()
        ctx = self.module.context(time.monotonic()+80)
        record = self.module.claim(ctx, spec, self.client.group, self.client.users[0], 'test-digest')
        changed = copy.deepcopy(record)
        changed['target_id'] = single.TARGET
        for prefix in (single.DAY_PREFIX+single.utc_day(), single.REQUEST_PREFIX+spec.request_key):
            self.ops.store.put(prefix, changed)
        with self.assertRaises(HTTPException): self.module.guard(ctx)
        self.assertFalse(self.writes())

    async def test_routes_registered_once_and_http_rejects_scope_target_uid_mode_and_false_confirmation(self):
        paths = [r.path for r in self.ops.router.routes]
        for suffix in ('preview', 'attempt', 'status'):
            self.assertEqual(paths.count('/api/scanner/operations/owned-target-invite/'+suffix), 1)
        app = FastAPI(); app.include_router(self.ops.router)
        headers = {'x-panel-user': 'owner', 'x-panel-role': 'owner'}
        prefix = '/api/scanner/operations/owned-target-invite'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture') as client:
            unauthorized = await client.get(prefix+'/preview?scope='+owned.SCOPE)
            self.assertEqual(unauthorized.status_code, 403)
            wrong_mode = await client.get(prefix+'/preview?scope='+owned.SCOPE+'&mode=saved_contacts', headers=headers)
            self.assertEqual(wrong_mode.status_code, 422)
            preview = await client.get(prefix+'/preview?scope='+owned.SCOPE, headers=headers)
            self.assertEqual(preview.status_code, 200)
            payload = dict(scope=owned.SCOPE, mode=owned.MODE, confirmed=True,
                reason='One owned technical control only', request_key='http-owned-key-0001',
                candidate_token=preview.json()['candidate_token'])
            for key, value in (('scope', single.SCOPE), ('target_id', single.TARGET), ('user_id', 888),
                               ('mode', 'saved_contacts'), ('confirmed', 1)):
                with self.subTest(key=key):
                    bad = {**payload, key: value}
                    response = await client.post(prefix+'/attempt', json=bad, headers=headers)
                    self.assertEqual(response.status_code, 422)
            response = await client.post(prefix+'/attempt', json=payload, headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['mode'], owned.MODE)
        self.assertEqual(len(self.writes()), 1)


if __name__ == '__main__':
    unittest.main()
