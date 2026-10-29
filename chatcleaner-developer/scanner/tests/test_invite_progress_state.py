"""State/API integration without Telegram or writes during the read."""
import json
import unittest
from datetime import datetime, timezone
import test_operations as fixture


class InviteProgressStateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.f = fixture.OperationsTests()
        await self.f.asyncSetUp()

    async def asyncTearDown(self):
        await self.f.asyncTearDown()

    async def test_all_history_survives_latest_hundred_job_window_and_is_read_only(self):
        ops = self.f.ops
        invite = await self.f.draft('invite')
        post = await self.f.draft(request_key='separate-post-for-summary')
        timestamp = datetime.now(timezone.utc).isoformat()
        proof = json.dumps({'code': 'invite_member_verified', 'invitation_requested': True})
        with ops.store.db:
            ops.store.db.execute("UPDATE op_items SET state='invited',diagnosis=?,attempted_at=?,finished_at=? WHERE job=?", (proof, timestamp, timestamp, invite['id']))
            ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?", (invite['id'],))
            # Many newer publications must not hide the old invitation result.
            for n in range(105):
                ops.store.db.execute("INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) SELECT ?,digest,account,telegram_id,kind,title,spec,state,created,updated FROM op_jobs WHERE id=?", ('post-window-' + str(n), post['id']))
            # A similarly labelled publication is never an invitation success.
            ops.store.db.execute("UPDATE op_items SET state='invited',diagnosis=?,attempted_at=?,finished_at=? WHERE job=?", (proof, timestamp, timestamp, post['id']))
        before = {t: ops.store.rows('SELECT * FROM ' + t) for t in ('op_accounts', 'op_jobs', 'op_items', 'op_invited', 'settings')}
        state = await ops.state()
        self.assertEqual(len(state['jobs']), 100)
        self.assertNotIn(invite['id'], [j['id'] for j in state['jobs']])
        self.assertEqual(state['invite_summary']['scope'], 'all_invite_jobs')
        self.assertEqual(state['invite_summary']['recorded_request_rows_today'], 1)
        group, = state['invite_summary']['groups']
        self.assertEqual(group['verified_added_total'], 1)
        self.assertEqual(group['verified_added_today'], 1)
        self.assertEqual(before, {t: ops.store.rows('SELECT * FROM ' + t) for t in before})
        self.assertEqual(self.f.c.calls, [])

    async def test_paused_pending_are_not_described_as_active(self):
        invite = await self.f.draft('invite')
        with self.f.ops.store.db:
            self.f.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?", (invite['id'],))
        state = await self.f.ops.state()
        group, = state['invite_summary']['groups']
        self.assertEqual((group['pending_total'], group['pending_paused'], group['pending_active']), (1, 1, 0))
        self.assertEqual(self.f.c.calls, [])


if __name__ == '__main__':
    unittest.main()
