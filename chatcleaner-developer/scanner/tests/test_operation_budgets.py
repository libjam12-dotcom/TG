import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from pydantic import ValidationError
from operation_budgets import DailyBudgetSpec, OperationBudgets
from operations import Operations


DAY = '2026-10-18'
STAMP = DAY + 'T12:00:00+00:00'


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Operations(self.tmp.name, lambda: None)
        self.ops.initialize()
        self.budgets = OperationBudgets(self.ops)
        self.budgets.initialize()
        with self.ops.store.db:
            for aid, tid in [('new', 100), ('other', 200), ('primary', 300)]:
                self.ops.store.db.execute('INSERT INTO op_accounts(id,label,telegram_id,enabled,created) VALUES(?,?,?,1,?)', (aid, aid, tid, STAMP))
        self.clock = patch('operation_budgets.now', return_value=STAMP)
        self.clock.start()

    async def asyncTearDown(self):
        self.clock.stop()
        await self.ops.shutdown()
        self.tmp.cleanup()

    def spec(self, **values):
        args = dict(confirmed=True, telegram_id=100, utc_day=DAY, daily_limit=50,
                    reason='Согласованы площадки кампании', request_key='budget-request-key-0001')
        args.update(values)
        return DailyBudgetSpec(**args)

    def job(self, aid='new', tid=100, cap=5, state='completed', attempts=0):
        with self.ops.store.db:
            cur = self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                ('job-key-' + str(len(self.ops.store.rows('SELECT id FROM op_jobs'))), 'digest', aid, tid, 'post', 'Test', json.dumps({'daily_limit': cap}), state, STAMP, STAMP))
            jid = cur.lastrowid
            for n in range(attempts):
                self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,attempted_at) VALUES(?,?,?,?,?,?)', (jid, -100-n, 'Fixture', 'sent', '[]', STAMP))
        return dict(id=jid, account=aid, telegram_id=tid, spec={'daily_limit': cap})

    async def test_override_preserves_history_used_count_and_current_job_cap(self):
        self.job(attempts=3)
        other = self.job(aid='other', tid=200, attempts=1)
        current = self.job(cap=50, state='draft')
        old_tables = {t: self.ops.store.rows('SELECT * FROM ' + t) for t in ['op_jobs', 'op_items', 'op_accounts', 'settings']}
        before = await self.budgets.get_budget('new')
        self.assertEqual((before['effective_daily_limit'], before['remaining_today']), (5, 2))
        saved = await self.budgets.save_budget('new', self.spec())
        state = saved['budget']
        self.assertEqual((state['job_baseline_daily_limit'], state['effective_daily_limit'], state['account_count_today'], state['remaining_today']), (5, 50, 3, 47))
        self.assertEqual(self.budgets.effective_limit(current, 5), 50)
        self.assertEqual(self.budgets.effective_limit({**current, 'spec': {'daily_limit': 20}}, 5), 20)
        self.assertEqual(self.budgets.effective_limit(other, 5), 5)
        self.assertEqual(old_tables, {t: self.ops.store.rows('SELECT * FROM ' + t) for t in old_tables})

    async def test_same_key_is_idempotent_and_conflicting_replay_rejected(self):
        first, second = await asyncio.gather(self.budgets.save_budget('new', self.spec()), self.budgets.save_budget('new', self.spec()))
        self.assertEqual(first['receipt'], second['receipt'])
        self.assertEqual(len(self.ops.store.rows('SELECT * FROM op_daily_budgets')), 1)
        with self.assertRaises(HTTPException) as changed:
            await self.budgets.save_budget('new', self.spec(daily_limit=40))
        self.assertEqual(changed.exception.status_code, 409)

    async def test_new_confirmation_appends_audit_without_overwriting_prior_record(self):
        first = (await self.budgets.save_budget('new', self.spec()))['receipt']
        second = (await self.budgets.save_budget('new', self.spec(daily_limit=25, request_key='budget-request-key-0002')))['receipt']
        self.assertGreater(second['id'], first['id'])
        self.assertEqual(self.ops.store.rows('SELECT daily_limit FROM op_daily_budgets ORDER BY id'), [{'daily_limit': 50}, {'daily_limit': 25}])
        self.assertEqual((await self.budgets.get_budget('new'))['override'], second)

    async def test_identity_change_rejects_stale_write_and_does_not_inherit_override(self):
        old = self.job(cap=50)
        await self.budgets.save_budget('new', self.spec())
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET telegram_id=777 WHERE id='new'")
        self.assertIsNone((await self.budgets.get_budget('new'))['override'])
        self.assertEqual(self.budgets.effective_limit(old, 5), 5)
        with self.assertRaises(HTTPException) as changed:
            await self.budgets.save_budget('new', self.spec())
        self.assertEqual(changed.exception.status_code, 409)

    async def test_primary_is_read_only_and_other_identity_cannot_reuse_key(self):
        self.job(aid='primary', tid=300, attempts=1)
        self.assertFalse((await self.budgets.get_budget('primary'))['editable'])
        with self.assertRaises(HTTPException) as primary:
            await self.budgets.save_budget('primary', self.spec(telegram_id=300))
        self.assertEqual(primary.exception.status_code, 403)
        await self.budgets.save_budget('new', self.spec())
        with self.assertRaises(HTTPException) as reused:
            await self.budgets.save_budget('other', self.spec(telegram_id=200))
        self.assertEqual(reused.exception.status_code, 409)
        self.assertIsNone((await self.budgets.get_budget('other'))['override'])

    async def test_utc_rollover_expires_cap_and_replay_cannot_reactivate_it(self):
        current = self.job(cap=50, attempts=3)
        receipt = (await self.budgets.save_budget('new', self.spec()))['receipt']
        with patch('operation_budgets.now', return_value='2026-10-19T00:00:01+00:00'):
            self.assertEqual(self.budgets.effective_limit(current, 5), 5)
            state = await self.budgets.get_budget('new')
            self.assertIsNone(state['override'])
            self.assertEqual(state['account_count_today'], 0)
            replay = await self.budgets.save_budget('new', self.spec())
            self.assertEqual(replay['receipt'], receipt)
            self.assertIsNone(replay['budget']['override'])
            with self.assertRaises(HTTPException):
                await self.budgets.save_budget('new', self.spec(request_key='budget-stale-day-0002'))
        self.assertEqual(len(self.ops.store.rows('SELECT * FROM op_items')), 3)

    async def test_explicit_future_grants_are_bounded_inactive_until_their_utc_day(self):
        current=self.job(cap=50,attempts=3)
        tables={t:self.ops.store.rows('SELECT * FROM '+t) for t in ['op_jobs','op_items','op_accounts','settings']}
        tomorrow=self.spec(utc_day='2026-10-19',request_key='future-budget-grant-0001')
        receipt=(await self.budgets.save_budget('new',tomorrow))['receipt']
        self.assertEqual(receipt['utc_day'],'2026-10-19')
        self.assertIsNone(self.budgets.override('new',100))
        state=await self.budgets.get_budget('new')
        self.assertEqual(state['future_grants'],[receipt]);self.assertEqual(state['max_advance_days'],7)
        again=(await self.budgets.save_budget('new',tomorrow))['receipt']
        self.assertEqual(again,receipt)
        await self.budgets.save_budget('new',self.spec(utc_day='2026-10-25',request_key='future-budget-grant-day7'))
        self.assertEqual([r['utc_day'] for r in (await self.budgets.get_budget('new'))['future_grants']],['2026-10-19','2026-10-25'])
        with patch('operation_budgets.now',return_value='2026-10-19T00:00:01+00:00'):
            self.assertEqual(self.budgets.effective_limit(current,5),50)
            self.assertEqual(self.budgets.override('new',100)['id'],receipt['id'])
        with patch('operation_budgets.now',return_value='2026-10-20T00:00:01+00:00'):
            self.assertIsNone(self.budgets.override('new',100))
            self.assertEqual(self.budgets.effective_limit(current,5),5)
        self.assertEqual(tables,{t:self.ops.store.rows('SELECT * FROM '+t) for t in tables})

    async def test_future_grant_rejects_past_far_day_primary_identity_and_conflicting_key(self):
        for day in ['2026-10-17','2026-10-26']:
            with self.subTest(day=day),self.assertRaises(HTTPException) as invalid:
                await self.budgets.save_budget('new',self.spec(utc_day=day,request_key='invalid-budget-'+day))
            self.assertEqual(invalid.exception.status_code,409)
        with self.assertRaises(HTTPException):
            await self.budgets.save_budget('primary',self.spec(telegram_id=300,utc_day='2026-10-19'))
        with self.assertRaises(HTTPException):
            await self.budgets.save_budget('new',self.spec(telegram_id=999,utc_day='2026-10-19'))
        await self.budgets.save_budget('new',self.spec(utc_day='2026-10-19'))
        with self.assertRaises(HTTPException) as conflict:
            await self.budgets.save_budget('new',self.spec(utc_day='2026-10-20'))
        self.assertEqual(conflict.exception.status_code,409)
        self.assertEqual(len(self.ops.store.rows('SELECT * FROM op_daily_budgets')),1)

    async def test_schema_requires_explicit_cap_identity_day_and_reason(self):
        for values in [dict(daily_limit=0), dict(daily_limit=101), dict(daily_limit=True),
                       dict(daily_limit=5.5), dict(telegram_id=True), dict(confirmed=False),
                       dict(reason='        '), dict(utc_day='2026-02-30'), dict(reason='bad\ud800text'),
                       dict(reset_counters=True)]:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self.spec(**values)


if __name__ == '__main__':
    unittest.main()
