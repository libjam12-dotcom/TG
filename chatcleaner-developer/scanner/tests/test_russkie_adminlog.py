"""Read original-group events only for the saved twenty-person job126."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import APIRouter, FastAPI, HTTPException
import httpx
from telethon import functions, types
import test_qa_invite_adminlog as fixtures
from qa_invite_adminlog import (QaInviteAdminLog, RUSSKIE_ACCOUNT, RUSSKIE_SENDER,
    RUSSKIE_TARGET, RUSSKIE_JOB, RUSSKIE_SCOPE, RUSSKIE_ITEM_COUNT)


START = datetime(2026, 10, 21, 5, 54, 51, tzinfo=timezone.utc)
AFTER = START + timedelta(seconds=20)


class SelectOnlyStore:
    def __init__(self, job, items):
        self.job, self.items = job, items
        self.calls = []
        self.put = Mock(side_effect=AssertionError('No settings writes'))
        self.db = Mock()
        self.db.execute = Mock(side_effect=AssertionError('No DB writes'))
        self.db.commit = Mock(side_effect=AssertionError('No commits'))

    def rows(self, query, params):
        self.calls.append((query, params))
        if not query.startswith('SELECT ') or params != (126,):
            raise AssertionError('Only fixed source SELECTs are permitted')
        if 'FROM op_jobs' in query:
            return [] if self.job is None else [copy.deepcopy(self.job)]
        if 'FROM op_items' in query:
            if 'LIMIT 21' not in query:
                raise AssertionError('Source scan must remain bounded')
            return copy.deepcopy(self.items[:21])
        raise AssertionError('Unexpected table')


class RusskieAdminLogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = fixtures.ReadClient()
        self.client.get_me.return_value = types.User(id=RUSSKIE_SENDER, username='demo_identity_1')
        self.client.channel = types.Channel(id=720000000, title='private-original-group-title',
            photo=types.ChatPhotoEmpty(), date=START, megagroup=True,
            admin_rights=types.ChatAdminRights(invite_users=True))
        self.client.get_entity.return_value = self.client.channel
        self.account = SimpleNamespace(client=self.client, me=types.User(id=RUSSKIE_SENDER))
        self.row = {'telegram_id': RUSSKIE_SENDER, 'enabled': 0, 'not_before': 0, 'error': 'PeerFlood preserved'}
        self.job = {'id': 126, 'account': 'primary', 'telegram_id': RUSSKIE_SENDER, 'kind': 'invite',
            'state': 'paused', 'created': '2026-10-21T05:40:43.920556+00:00',
            'updated': '2026-10-21T05:54:00+00:00',
            'spec': '{"private_note":"private-spec-secret"}'}
        self.items = [dict(id=350+i, job=126, chat_id=RUSSKIE_TARGET, user_id=200+i,
            state='restricted' if i == 0 else 'blocked' if i == 1 else 'pending',
            attempted_at=START.isoformat() if i == 0 else (START+timedelta(seconds=60)).isoformat() if i == 1 else None,
            finished_at=None) for i in range(RUSSKIE_ITEM_COUNT)]
        self.store = SelectOnlyStore(self.job, self.items)
        self.ops = SimpleNamespace(accounts={'primary': self.account}, store=self.store,
            account_row=Mock(side_effect=lambda aid: self.row if aid == 'primary' else (_ for _ in ()).throw(AssertionError('Wrong account'))),
            auth=Mock(side_effect=AssertionError('No auth')), online=Mock(side_effect=AssertionError('No outbound')),
            readable_account=Mock(side_effect=AssertionError('No lazy runtime')), make_client=Mock(side_effect=AssertionError('No new session')))
        self.diag = QaInviteAdminLog(self.ops)
        self.row_before = copy.deepcopy(self.row)

    async def asyncTearDown(self):
        self.assertEqual(self.row, self.row_before)
        for method in ('connect', 'disconnect', 'log_out', 'sign_in', 'send_code_request', 'send_message'):
            getattr(self.client, method).assert_not_awaited()
        for method in ('auth', 'online', 'readable_account', 'make_client'):
            getattr(self.ops, method).assert_not_called()
        self.store.put.assert_not_called(); self.store.db.execute.assert_not_called(); self.store.db.commit.assert_not_called()

    async def test_paused_primary_reads_original_group_and_only_saved_subjects(self):
        self.client.result = types.channels.AdminLogResults(events=[
            fixtures.event(1, types.ChannelAdminLogEventActionParticipantInvite(fixtures.member(200)), actor=98765, date=AFTER),
            fixtures.event(2, types.ChannelAdminLogEventActionParticipantLeave(), actor=201, date=AFTER),
            fixtures.event(3, types.ChannelAdminLogEventActionParticipantInvite(fixtures.member(999)), actor=200, date=AFTER),
            fixtures.event(4, types.ChannelAdminLogEventActionParticipantJoin(), actor=200, date=START-timedelta(seconds=1)),
            fixtures.event(5, types.ChannelAdminLogEventActionChangeTitle('private-title-before','private-title-after'), actor=200,date=AFTER)],
            chats=[self.client.channel], users=[types.User(id=999, first_name='private-person-secret', phone='private-phone-secret', access_hash=9988776655)])
        before = copy.deepcopy((self.job, self.items))
        data = json.loads((await self.diag.read_russkie(RUSSKIE_SCOPE)).body)
        self.assertEqual((data['account'], data['sender_id'], data['target_id'], data['source_job_id']),
                         ('primary', RUSSKIE_SENDER, RUSSKIE_TARGET, 126))
        self.assertEqual(data['cohort_count'],20); self.assertEqual(data['subject_count'],20)
        self.assertEqual(data['since'],START.isoformat()); self.assertEqual(data['since_source'],'earliest_saved_attempt_at')
        self.assertEqual([r['event_id'] for r in data['events']],[1,2])
        self.assertEqual([r['recipient_id'] for r in data['events']],[200,201])
        self.assertEqual((self.job,self.items),before)
        self.assertTrue(data['read_only']); self.assertFalse(data['invitation_requested'])
        self.assertFalse(data['history_changed']); self.assertFalse(data['membership_evaluated'])
        self.client.get_entity.assert_awaited_once_with(RUSSKIE_TARGET)
        self.assertEqual(len(self.client.calls),1)
        req=self.client.calls[0]; self.assertIsInstance(req,functions.channels.GetAdminLogRequest)
        self.assertEqual(req.limit,50); self.assertIsNone(req.admins)
        for secret in ('private-spec-secret','private-title-before','private-title-after','private-original-group-title',
                       'private-person-secret','private-phone-secret','9988776655','access_hash'):
            self.assertNotIn(secret,json.dumps(data))

    async def test_invalid_source_job_or_any_wrong_target_blocks_before_telegram(self):
        mutations=[lambda:self.job.update(account='other'),lambda:self.job.update(telegram_id=42),
            lambda:self.job.update(kind='post'),lambda:self.items[0].update(chat_id=-42),
            lambda:self.items[0].update(user_id=0),lambda:self.items.pop(),
            lambda:self.items.append(copy.deepcopy(self.items[0])),lambda:self.items[1].update(user_id=self.items[0]['user_id'])]
        original_job,original_items=copy.deepcopy((self.job,self.items))
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                self.job.clear();self.job.update(copy.deepcopy(original_job));self.items[:]=copy.deepcopy(original_items)
                self.diag.next_read=0; mutate()
                with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
                self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_not_awaited();self.client.get_entity.assert_not_awaited();self.assertEqual(self.client.calls,[])

    async def test_missing_or_invalid_attempt_time_never_reads_telegram(self):
        for date in (None,'bad-time','2026-10-21T05:54:51','2026-01-01T00:00:00+00:00'):
            with self.subTest(date=date):
                for item in self.items:item['attempted_at']=None
                self.items[0]['attempted_at']=date
                with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
                self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_not_awaited();self.assertEqual(self.client.calls,[])

    async def test_source_job_change_during_identity_read_prevents_log_request(self):
        async def change():
            self.job['spec']='{"changed":"private-changed-spec"}'
            return types.User(id=RUSSKIE_SENDER)
        self.client.get_me.side_effect=change
        with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,409)
        self.client.get_entity.assert_not_awaited();self.assertEqual(self.client.calls,[])

    async def test_source_database_error_is_redacted_before_any_telegram_read(self):
        self.store.rows=Mock(side_effect=RuntimeError('private-database-error-secret'))
        with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,502)
        self.assertNotIn('private-database-error-secret',raised.exception.detail)
        self.client.get_me.assert_not_awaited();self.assertEqual(self.client.calls,[])

    async def test_initial_source_sql_delay_exhausts_shared_deadline_before_any_telegram(self):
        original=self.store.rows
        def slow(query, params):
            result=original(query,params)
            time.sleep(.03)
            return result
        self.store.rows=slow
        with patch('qa_invite_adminlog.READ_TIMEOUT',.01):
            with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,504)
        self.assertEqual(len(self.store.calls),1)
        self.client.get_me.assert_not_awaited();self.client.get_entity.assert_not_awaited()
        self.assertEqual(self.client.calls,[])

    async def test_repeated_source_guard_delay_prevents_the_following_adminlog_rpc(self):
        original=self.store.rows
        def slow(query, params):
            result=original(query,params)
            # Initial source + pre-identity guard + post-identity guard each
            # consumed two SELECTs. Delay the post-group guard's first SELECT.
            if len(self.store.calls)==7:time.sleep(.03)
            return result
        self.store.rows=slow
        with patch('qa_invite_adminlog.READ_TIMEOUT',.01):
            with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,504)
        self.assertEqual(len(self.store.calls),7)
        self.client.get_me.assert_awaited_once();self.client.get_entity.assert_awaited_once_with(RUSSKIE_TARGET)
        self.assertEqual(self.client.calls,[])

    async def test_source_recipient_change_during_group_read_prevents_log_request(self):
        async def change(target):
            self.items[0]['user_id']=4444
            return self.client.channel
        self.client.get_entity.side_effect=change
        with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,409);self.assertEqual(self.client.calls,[])

    async def test_source_change_after_response_discards_all_result(self):
        async def change():self.job['state']='cancelled'
        self.client.hook=change
        with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
        self.assertEqual(raised.exception.status_code,409);self.assertEqual(len(self.client.calls),1)

    async def test_primary_runtime_absence_or_identity_mismatch_does_not_connect(self):
        for accounts in ({},{'primary':SimpleNamespace(client=self.client,me=types.User(id=42))}):
            with self.subTest(accounts=accounts):
                self.ops.accounts=accounts
                with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
                self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_not_awaited();self.assertEqual(self.client.calls,[])

    async def test_stored_account_not_before_is_respected_without_business_write(self):
        original=self.row['not_before'];self.row['not_before']=datetime.now(timezone.utc).timestamp()+120
        try:
            with self.assertRaises(HTTPException) as raised:await self.diag.read_russkie(RUSSKIE_SCOPE)
            self.assertEqual(raised.exception.status_code,429)
            self.client.get_me.assert_not_awaited();self.assertEqual(self.client.calls,[])
        finally:self.row['not_before']=original

    async def test_new_route_has_owner_admin_guard_and_zero_rpc_422(self):
        app=FastAPI();outer=APIRouter(prefix='/api/scanner/operations');outer.include_router(self.diag.router);app.include_router(outer)
        url='/api/scanner/operations/qa/russkie-adminlog'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            for headers in ({},{'x-panel-user':'viewer','x-panel-role':'viewer'}):
                self.assertEqual((await client.get(url,params={'scope':RUSSKIE_SCOPE},headers=headers)).status_code,403)
            for role in ('owner','admin'):
                headers={'x-panel-user':'tester','x-panel-role':role}
                self.assertEqual((await client.get(url,headers=headers)).status_code,422)
                self.assertEqual((await client.get(url,params={'scope':'qa130-murk'},headers=headers)).status_code,422)
            self.client.get_me.assert_not_awaited();self.assertEqual(self.client.calls,[]);self.assertEqual(self.store.calls,[])
            response=await client.get(url,params={'scope':RUSSKIE_SCOPE,'job_id':13,'target_id':-42,'recipient_ids':'999'},headers={'x-panel-user':'owner','x-panel-role':'owner'})
            self.assertEqual(response.status_code,200);self.assertEqual(response.json()['source_job_id'],126)
            self.assertEqual(response.json()['target_id'],RUSSKIE_TARGET)


if __name__=='__main__':unittest.main()
