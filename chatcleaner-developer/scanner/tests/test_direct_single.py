"""Offline regression: one durable direct invitation without a mutual-contact prerequisite."""
import asyncio
import copy
from datetime import datetime, timezone
import json
import time
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from httpx import AsyncClient, ASGITransport
from telethon import errors, functions, types
import operations
import test_single_attempt as _single


class DirectSingleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.f = _single.SingleAttemptTests()
        await self.f.asyncSetUp()
        self.ops, self.a = self.f.ops, self.f.a

    async def asyncTearDown(self):
        await self.f.asyncTearDown()

    async def job(self, **changes):
        return await self.f.job(**changes)

    def spec(self, j, item, key='explicit-direct-single-key-001', mode='direct'):
        payload = self.f.spec(j, item, key).model_dump()
        payload['mode'] = mode
        return operations.SingleAttempt(**payload)

    async def arm(self, j, index=1, **changes):
        item = j['items'][index]
        return await self.ops.start_single_attempt(j['id'], item['id'], self.spec(j, item, **changes))

    def invites(self):
        return self.f.invite_calls()

    async def test_preview_direct_is_readonly_and_does_not_require_unset_contacts(self):
        j = await self.job(); item = j['items'][1]
        before = list(self.ops.store.db.iterdump())
        legacy = await self.ops.single_preview(j['id'], item['id'])
        direct = await self.ops.single_preview(j['id'], item['id'], mode='direct')
        self.assertFalse(legacy['can_start'])
        self.assertEqual(legacy['mode'], 'mutual_contact_control')
        self.assertTrue(legacy['requires_mutual_contact'])
        self.assertTrue(direct['can_start'])
        self.assertEqual(direct['mode'], 'direct')
        self.assertFalse(direct['requires_mutual_contact'])
        self.assertFalse(direct['invite_requires_mutual_contact'])
        self.assertFalse(direct['receiver_action_pending'])
        self.assertFalse(direct['contact_preparation_planned'])
        self.assertFalse(direct['mutual_contact'])
        self.assertEqual(list(self.ops.store.db.iterdump()), before)
        self.assertFalse(self.f.c.calls)

    async def test_direct_dispatches_selected_nonmutual_once_then_server_pauses_without_callback(self):
        j = await self.job(); selected = j['items'][1]; original = copy.deepcopy(j['spec'])
        receipt = await self.arm(j)
        self.assertEqual(receipt['single_attempt']['mode'], 'direct')
        self.assertFalse(receipt['single_attempt']['requires_mutual_contact'])
        # The HTTP caller does nothing after receiving (or losing) the receipt.
        await self.ops.tick(); self.f.fixture.clear_wait(); await self.ops.tick()
        fresh = await self.ops.detail(j['id'])
        self.assertEqual([r.users[0].id for r in self.invites()], [selected['user_id']])
        self.assertEqual([i['state'] for i in fresh['items']], ['pending', 'invited', 'pending'])
        self.assertEqual(fresh['state'], 'paused')
        self.assertEqual(fresh['spec'], original)
        self.assertEqual(fresh['single_attempt']['state'], 'finished')
        self.assertTrue(all(isinstance(r, functions.channels.InviteToChannelRequest) for r in self.f.c.calls))
        diagnosis = fresh['items'][1]['diagnosis']['single_attempt']
        self.assertEqual(diagnosis['mode'], 'direct')
        self.assertFalse(diagnosis['requires_mutual_contact'])
        self.assertIsNone(diagnosis['mutual_checked_at'])
        self.assertEqual(self.ops.account_daily_count(j['account'], datetime.now(timezone.utc).date().isoformat()), 1)

    async def test_legacy_default_still_rejects_nonmutual_after_optional_contact(self):
        j = await self.job(add_contacts_before_invite=True); item = j['items'][1]
        await self.ops.start_single_attempt(j['id'], item['id'], self.f.spec(j, item))
        await self.ops.tick()
        fresh = await self.ops.detail(j['id'])
        self.assertFalse(self.invites())
        self.assertEqual(fresh['items'][1]['state'], 'failed')
        self.assertTrue(fresh['single_attempt']['requires_mutual_contact'])
        self.assertEqual(fresh['single_attempt']['mode'], 'mutual_contact_control')

    async def test_direct_optional_contact_policy_remains_opt_in_and_does_not_require_mutual(self):
        j = await self.job(add_contacts_before_invite=True)
        await self.arm(j); await self.ops.tick()
        self.assertEqual(len(self.invites()), 1)
        self.assertEqual(sum(isinstance(r, functions.contacts.AddContactRequest) for r in self.f.c.calls), 1)
        fresh = await self.ops.detail(j['id'])
        self.assertEqual(fresh['state'], 'paused')
        self.assertFalse(fresh['items'][1]['diagnosis']['contact_preparation']['mutual_contact'])

    async def test_direct_replay_never_rearms_and_same_key_mode_change_is_conflict(self):
        j = await self.job(); item = j['items'][1]
        await self.arm(j); await self.ops.tick(); self.f.fixture.clear_wait()
        replay = await self.arm(j)
        self.assertEqual(replay['single_attempt']['state'], 'finished')
        with self.assertRaises(HTTPException) as conflict:
            await self.arm(j, mode='mutual_contact_control')
        self.assertEqual(conflict.exception.status_code, 409)
        for action in ('start', 'resume', 'continue_pending'):
            with self.assertRaises(HTTPException):
                await self.ops.control(j['id'], operations.Control(action=action, confirmed=True))
        await self.ops.tick()
        self.assertEqual(len(self.invites()), 1)
        with self.assertRaises(HTTPException):
            await self.arm(j, key='another-key-for-same-direct-item')

    async def test_old_legacy_key_without_mode_remains_idempotently_replayable(self):
        j = await self.job(); item = j['items'][1]; self.f.c.mutuals.add(item['user_id'])
        spec = self.f.spec(j, item)
        await self.ops.start_single_attempt(j['id'], item['id'], spec)
        record = self.ops.store.get('single_attempt_job_'+str(j['id']))
        record.pop('mode', None)
        with self.ops.store.db: self.ops.save_single_attempt(record)
        replay = await self.ops.start_single_attempt(j['id'], item['id'], spec)
        self.assertEqual(replay['single_attempt']['mode'], 'mutual_contact_control')
        self.assertTrue(replay['single_attempt']['requires_mutual_contact'])
        await self.ops.tick()
        self.assertEqual(len(self.invites()), 1)

    async def test_mode_tamper_before_claim_is_rejected_by_saved_request_binding(self):
        j = await self.job(); await self.arm(j)
        key = 'single_attempt_job_'+str(j['id'])
        record = self.ops.store.get(key)
        record.update(mode='mutual_contact_control', requires_mutual_contact=True)
        self.ops.store.put(key, record)  # Request-key record remains the original direct policy.
        await self.ops.tick()
        self.assertFalse(self.f.c.calls)
        fresh = await self.ops.detail(j['id'])
        self.assertEqual(fresh['state'], 'paused')
        self.assertTrue(all(i['attempted_at'] is None for i in fresh['items']))

    async def test_mode_change_after_claim_cannot_change_policy_or_dispatch(self):
        j = await self.job(); await self.arm(j)
        original = self.f.c.get_entity
        async def changed(ref):
            value = await original(ref)
            if isinstance(ref, int) and ref < 0:
                record = self.ops.store.get('single_attempt_job_'+str(j['id']))
                record.update(mode='mutual_contact_control', requires_mutual_contact=True)
                with self.ops.store.db: self.ops.save_single_attempt(record)
            return value
        self.f.c.get_entity = changed
        await self.ops.tick()
        self.assertFalse(self.invites())
        fresh = await self.ops.detail(j['id'])
        self.assertFalse(fresh['items'][1]['diagnosis']['invitation_requested'])
        self.assertEqual(fresh['state'], 'paused')

    async def test_context_change_after_preflight_cannot_send_direct(self):
        j = await self.job(); await self.arm(j)
        original = self.f.c.get_permissions
        async def changed(*args):
            try: return await original(*args)
            finally: self.a.client = type(self.f.c)(self.f.fixture.group)
        self.f.c.get_permissions = changed
        await self.ops.tick()
        self.assertFalse(self.invites())
        self.assertEqual((await self.ops.detail(j['id']))['state'], 'paused')

    async def test_direct_peer_flood_stops_account_and_never_releases_other_pending(self):
        j = await self.job(); await self.arm(j)
        self.f.c.failures['InviteToChannelRequest'] = errors.PeerFloodError
        await self.ops.tick(); self.f.fixture.clear_wait(); await self.ops.tick()
        fresh = await self.ops.detail(j['id'])
        self.assertEqual(fresh['items'][1]['state'], 'blocked')
        self.assertFalse(self.ops.account_row(j['account'])['enabled'])
        self.assertEqual(fresh['state'], 'paused')
        self.assertEqual(len(self.invites()), 1)

    async def test_direct_unknown_server_result_keeps_history_and_is_not_retried(self):
        j = await self.job(); await self.arm(j)
        self.f.c.failures['InviteToChannelRequest'] = lambda r: errors.ServerError(r, 'SERVER_ERROR', code=500)
        await self.ops.tick(); self.f.fixture.clear_wait()
        before = self.ops.store.rows('SELECT * FROM op_items WHERE job=?', (j['id'],))
        await self.ops.tick(); await self.arm(j)
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE job=?', (j['id'],)), before)
        fresh = await self.ops.detail(j['id'])
        self.assertEqual(fresh['items'][1]['state'], 'uncertain')
        self.assertFalse(fresh['single_attempt']['can_continue'])
        with self.assertRaises(HTTPException):
            await self.arm(j, index=2, key='new-direct-after-unknown-result')
        self.assertEqual(len(self.invites()), 1)

    async def test_direct_restart_before_claim_and_during_rpc_cannot_release_sibling(self):
        for dispatched in (False, True):
            with self.subTest(dispatched=dispatched):
                if dispatched:
                    await self.f.asyncTearDown(); await self.f.asyncSetUp()
                    self.ops, self.a = self.f.ops, self.f.a
                j = await self.job(); await self.arm(j)
                if dispatched:
                    self.f.c.failures['InviteToChannelRequest'] = lambda r: asyncio.CancelledError()
                    with self.assertRaises(asyncio.CancelledError): await self.ops.tick()
                self.ops.initialize(); self.f.fixture.clear_wait(); await self.ops.tick()
                fresh = await self.ops.detail(j['id'])
                self.assertEqual(fresh['state'], 'paused')
                self.assertEqual(fresh['single_attempt']['state'], 'interrupted')
                self.assertEqual(len(self.invites()), int(dispatched))
                self.assertEqual(fresh['items'][0]['state'], 'pending')
                self.assertEqual(fresh['items'][2]['state'], 'pending')

    async def test_direct_daily_cap_cannot_be_bypassed(self):
        j = await self.job()
        await self.f.fixture.seed_daily_actions()
        with self.assertRaises(HTTPException): await self.arm(j)
        self.assertFalse(self.f.c.calls)
        self.assertIsNone(self.ops.store.get('single_attempt_job_'+str(j['id'])))

    async def test_direct_cannot_arm_after_the_real_existing_pilot_goal_is_met(self):
        j = await self.job(count=12)
        await self.f.fixture.seed_daily_actions()
        await self.ops.start_pilot(j['id'], self.f.fixture.pilot_spec())
        for _ in range(10):
            await self.ops.tick(); self.f.fixture.clear_wait()
        fresh = await self.ops.detail(j['id'])
        self.assertTrue(fresh['pilot']['goal_met'])
        self.assertEqual(len(self.invites()), 10)
        item = fresh['items'][10]
        preview = await self.ops.single_preview(j['id'], item['id'], mode='direct')
        self.assertFalse(preview['can_start'])
        with self.assertRaises(HTTPException): await self.arm(fresh, index=10)
        self.assertEqual(len(self.invites()), 10)
        self.assertIsNone(self.ops.store.get('single_attempt_job_'+str(j['id'])))

    async def test_direct_expires_after_120_seconds_and_legacy_keeps_utc_window(self):
        timestamp = (int(time.time())//86400)*86400+3600
        with patch('operations.time.time', return_value=timestamp), patch('operations.now', return_value=datetime.fromtimestamp(timestamp,timezone.utc).isoformat()):
            j = await self.job(); await self.arm(j)
            direct = self.ops.store.get('single_attempt_job_'+str(j['id']))
            self.assertEqual(direct['expires_at'], timestamp+120)
        expired = timestamp+121
        with patch('operations.time.time', return_value=expired), patch('operations.now', return_value=datetime.fromtimestamp(expired,timezone.utc).isoformat()):
            await self.ops.tick()
        self.assertFalse(self.invites())
        fresh = await self.ops.detail(j['id'])
        self.assertEqual(fresh['state'], 'paused')
        self.assertEqual(fresh['single_attempt']['state'], 'interrupted')
        with patch('operations.time.time', return_value=timestamp), patch('operations.now', return_value=datetime.fromtimestamp(timestamp,timezone.utc).isoformat()):
            legacy = await self.job(request_key='separate-legacy-expiration-job')
            await self.f.arm(legacy, key='legacy-window-is-still-utc-midnight')
            record = self.ops.store.get('single_attempt_job_'+str(legacy['id']))
            self.assertEqual(record['expires_at'], (timestamp//86400+1)*86400)

    async def test_public_api_requires_explicit_valid_mode_and_retains_old_default(self):
        j = await self.job(); item = j['items'][1]
        app = FastAPI(); app.include_router(self.ops.router)
        base = f"/api/scanner/operations/jobs/{j['id']}/items/{item['id']}"
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://offline') as client:
            legacy = await client.get(base+'/single-preview')
            self.assertEqual(legacy.status_code, 200)
            self.assertTrue(legacy.json()['requires_mutual_contact'])
            preview = await client.get(base+'/single-preview?mode=direct')
            self.assertTrue(preview.json()['can_start'])
            self.assertEqual((await client.get(base+'/single-preview?mode=unknown')).status_code, 422)
            payload = self.spec(j, item).model_dump()
            self.assertEqual((await client.post(base+'/single-attempt', json={**payload, 'mode':'unknown'})).status_code, 422)
            response = await client.post(base+'/single-attempt', json=payload)
            self.assertEqual(response.status_code, 200)
            receipt = response.json()
            self.assertEqual(receipt['single_attempt']['mode'], 'direct')
            self.assertFalse(receipt['single_attempt']['requires_mutual_contact'])
            self.assertEqual(receipt['single_attempt']['max_attempts'], 1)
            self.assertTrue(receipt['single_attempt']['pause_after_attempt'])
            for private in ('user_access_hash', 'pilot_binding'):
                self.assertNotIn(private, json.dumps(receipt))
