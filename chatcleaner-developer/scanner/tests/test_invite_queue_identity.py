"""Offline regressions for stale job identities; all Telegram clients are fixtures."""
import json
import unittest

from fastapi import HTTPException
from telethon import types

from operations import AccountSpec, Control, SingleAttempt
import test_operations as _fixtures


class InviteQueueIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = _fixtures.OperationsTests()
        await self.fixture.asyncSetUp()
        self.ops = self.fixture.ops

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_stale_job_pauses_without_blocking_another_accounts_invite(self):
        stale = await self.fixture.pilot_draft(3)
        await self.fixture.start(stale)
        terminal_ids = [item['id'] for item in stale['items'][1:]]
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET telegram_id=999 WHERE id=?', (stale['id'],))
            for iid, state in zip(terminal_ids, ['uncertain', 'invited']):
                self.ops.store.db.execute(
                    'UPDATE op_items SET state=?,error=?,diagnosis=?,attempted_at=?,finished_at=? WHERE id=?',
                    (state, 'Original outcome', json.dumps({'reason': 'Original evidence'}),
                     '2026-10-18T10:00:00+00:00', '2026-10-18T10:00:01+00:00', iid))
        terminal_before = self.ops.store.rows('SELECT * FROM op_items WHERE id IN (?,?) ORDER BY id', terminal_ids)

        second = await self.ops.add_account(AccountSpec(label='Independent account fixture', authorized=True))
        account = self.ops.accounts[second['id']]
        client = _fixtures.Client(self.fixture.group)
        client.get_me.return_value = types.User(id=101, username='second_fixture')
        account.client = client
        await account.ready()
        other = await self.fixture.draft('invite', account=account.aid, request_key='independent-invite-queue-key')
        await self.fixture.start(other)
        accounts_before = self.ops.store.rows('SELECT * FROM op_accounts ORDER BY id')

        await self.ops.tick()

        paused = await self.ops.detail(stale['id'])
        self.assertEqual(paused['state'], 'paused')
        self.assertEqual(paused['items'][0]['state'], 'pending')
        self.assertIn('отправител', paused['items'][0]['error'].lower())
        self.assertIsNone(paused['items'][0]['attempted_at'])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_items WHERE id IN (?,?) ORDER BY id', terminal_ids), terminal_before)
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_accounts ORDER BY id'), accounts_before)
        self.assertFalse(self.fixture.c.calls)
        self.assertFalse(client.calls)
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_invited'))

        await self.ops.tick()

        self.assertEqual((await self.ops.detail(other['id']))['items'][0]['state'], 'invited')
        self.assertEqual(len(client.calls), 1)
        self.assertFalse(self.fixture.c.calls)
        self.assertEqual((await self.ops.detail(stale['id']))['state'], 'paused')

    async def test_stale_identity_interrupts_single_grant_without_rearming(self):
        job = await self.fixture.pilot_draft(1)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?", (job['id'],))
        item = job['items'][0]
        spec = SingleAttempt(
            confirmed=True, request_key='stale-single-identity-key', mode='direct',
            reason='An explicitly bounded offline identity regression',
            telegram_id=job['telegram_id'], chat_id=item['chat_id'], user_id=item['user_id'],
            cohort_hash=self.ops.pilot_cohort(job)[1])
        await self.ops.start_single_attempt(job['id'], item['id'], spec)
        before = self.ops.store.get('single_attempt_job_' + str(job['id']))
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET telegram_id=999 WHERE id=?', (job['id'],))

        await self.ops.tick()

        record = self.ops.store.get('single_attempt_job_' + str(job['id']))
        self.assertEqual(record['state'], 'interrupted')
        self.assertEqual(record['request_key'], before['request_key'])
        self.assertEqual(record['expires_at'], before['expires_at'])
        self.assertFalse(record['invitation_requested'])
        self.assertIsNone(record['claimed_at'])
        self.assertIsNone(self.ops.store.get(f"single_attempt_item_{job['id']}_{item['id']}"))
        self.assertEqual((await self.ops.detail(job['id']))['state'], 'paused')
        self.assertFalse(self.fixture.c.calls)

        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET telegram_id=? WHERE id=?', (job['telegram_id'], job['id']))
        repeated = await self.ops.start_single_attempt(job['id'], item['id'], spec)
        self.assertEqual(repeated['single_attempt']['state'], 'interrupted')
        with self.assertRaises(HTTPException):
            await self.ops.control(job['id'], Control(action='resume', confirmed=True))
        await self.ops.tick()
        self.assertFalse(self.fixture.c.calls)
        self.assertEqual((await self.ops.detail(job['id']))['state'], 'paused')


if __name__ == '__main__':
    unittest.main()
