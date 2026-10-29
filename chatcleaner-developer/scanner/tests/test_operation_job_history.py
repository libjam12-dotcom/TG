"""Saved job history must remain complete, filterable and read-only."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import httpx
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operations import AccountSpec, Operations


STAMP = '2026-10-21T12:00:00+00:00'
GROUP_A = -1000000000010
GROUP_B = -1000000000020


class JobHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Operations(self.tmp.name, lambda: None)
        self.ops.initialize()
        self.account = (await self.ops.add_account(AccountSpec(label='Offline fixture', authorized=True)))['id']
        self.app = FastAPI()
        self.app.include_router(self.ops.router)
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test')

    async def asyncTearDown(self):
        await self.http.aclose()
        await self.ops.shutdown()
        self.tmp.cleanup()

    def job(self, kind='invite', items=None, state='paused'):
        if items is None: items = [(GROUP_A, 'pending')]
        key = 'history-' + str(self.ops.store.db.execute('SELECT COUNT(*) FROM op_jobs').fetchone()[0])
        spec = {'daily_limit': 20, 'destinations': list(dict.fromkeys(chat for chat, _ in items)),
                'delay_seconds': 60, 'text': 'Fixture', 'media': [], 'permission_note': 'Approved fixture',
                'consent_note': 'Approved fixture', 'users': []}
        with self.ops.store.db:
            cur = self.ops.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (key, 'private-digest', self.account, 100, kind, 'Saved '+kind, json.dumps(spec), state, STAMP, STAMP))
            jid = cur.lastrowid
            for number, (chat, item_state) in enumerate(items):
                self.ops.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids) VALUES(?,?,?,?,?,?,?)',
                    (jid, chat, 'Fixture group', 200+number, 'fixture_user_'+str(number), item_state, '[123]'))
        return jid

    async def history(self, **params):
        response = await self.http.get('/api/scanner/operations/jobs', params=params)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    async def test_kind_filter_finds_old_invites_before_latest_hundred_posts(self):
        first = self.job()
        second = self.job(items=[(GROUP_B, 'pending')])
        for _ in range(115): self.job(kind='post', state='completed', items=[(GROUP_A, 'sent')])
        legacy = await self.ops.state()
        self.assertEqual(len(legacy['jobs']), 100)
        self.assertFalse(any(job['kind'] == 'invite' for job in legacy['jobs']))
        result = await self.history(kind='invite')
        self.assertEqual([job['id'] for job in result['jobs']], [second, first])
        self.assertFalse(result['has_more'])
        self.assertIsNone(result['next_before_id'])
        self.assertEqual(result['filters'], {'kind': 'invite', 'chat_id': None, 'pending_only': False})

    async def test_keyset_pages_have_no_gaps_or_duplicates_when_new_jobs_arrive(self):
        expected = [self.job() for _ in range(67)][::-1]
        first = await self.history(kind='invite', limit=7)
        self.assertTrue(first['has_more'])
        self.assertEqual(first['next_before_id'], first['jobs'][-1]['id'])
        new_job = self.job()
        seen = [row['id'] for row in first['jobs']]
        cursor = first['next_before_id']
        while cursor is not None:
            result = await self.history(kind='invite', limit=7, before_id=cursor)
            ids = [row['id'] for row in result['jobs']]
            self.assertTrue(all(jid < cursor for jid in ids))
            seen.extend(ids)
            cursor = result['next_before_id']
            self.assertEqual(result['has_more'], cursor is not None)
        self.assertEqual(seen, expected)
        self.assertEqual(len(seen), len(set(seen)))
        self.assertNotIn(new_job, seen)
        self.assertEqual((await self.history(kind='invite', limit=7))['jobs'][0]['id'], new_job)

    async def test_chat_and_pending_filters_must_match_the_same_item(self):
        matching = self.job(items=[(GROUP_A, 'pending'), (GROUP_A, 'pending'), (GROUP_B, 'sent')])
        cross_item = self.job(items=[(GROUP_A, 'sent'), (GROUP_B, 'pending')])
        sending = self.job(items=[(GROUP_A, 'sending')])
        self.job(items=[(GROUP_B, 'pending')])
        for _ in range(60): self.job(kind='post', state='completed', items=[(GROUP_A, 'sent')])
        result = await self.history(kind='invite', chat_id=GROUP_A, pending_only=True, limit=1)
        self.assertEqual([job['id'] for job in result['jobs']], [matching])
        self.assertFalse(result['has_more'])
        self.assertEqual(len(result['jobs'][0]['items']), 3)  # Detail retains the whole saved job.
        self.assertEqual(result['filters'], {'kind': 'invite', 'chat_id': GROUP_A, 'pending_only': True})
        self.assertEqual([row['id'] for row in (await self.history(kind='invite', chat_id=GROUP_A))['jobs']],
                         [sending, cross_item, matching])
        self.assertIn(cross_item, [row['id'] for row in (await self.history(pending_only=True))['jobs']])

    async def test_optional_kind_and_chat_filters_cover_all_existing_kinds(self):
        kinds = ['post', 'invite', 'link', 'invite_message', 'message']
        jobs = {kind: self.job(kind=kind) for kind in kinds}
        result = await self.history()
        self.assertEqual(result['limit'], 25)
        self.assertEqual([row['id'] for row in result['jobs']], list(jobs.values())[::-1])
        for kind, jid in jobs.items():
            self.assertEqual([row['id'] for row in (await self.history(kind=kind, chat_id=GROUP_A))['jobs']], [jid])
        self.assertEqual((await self.history(chat_id=GROUP_B))['jobs'], [])

    async def test_empty_and_exact_size_pages_have_no_false_continuation(self):
        empty = await self.history()
        self.assertEqual(empty['jobs'], [])
        self.assertFalse(empty['has_more'])
        self.assertIsNone(empty['next_before_id'])
        ids = [self.job() for _ in range(2)]
        exact = await self.history(limit=2)
        self.assertEqual([row['id'] for row in exact['jobs']], ids[::-1])
        self.assertFalse(exact['has_more'])
        self.assertIsNone(exact['next_before_id'])
        self.assertEqual((await self.history(before_id=ids[0]))['jobs'], [])

    async def test_query_validation_rejects_unbounded_or_invalid_inputs(self):
        invalid = [{'limit': 0}, {'limit': 51}, {'limit': -1}, {'limit': 'many'},
                   {'before_id': 0}, {'before_id': -1}, {'before_id': 'last'}, {'before_id': 2**63},
                   {'kind': 'all'}, {'kind': "invite' OR 1=1 --"},
                   {'chat_id': 'group'}, {'chat_id': 2**63}, {'chat_id': -(2**63)-1}, {'pending_only': 'perhaps'}]
        for params in invalid:
            with self.subTest(params=params):
                response = await self.http.get('/api/scanner/operations/jobs', params=params)
                self.assertEqual(response.status_code, 422, response.text)
        for _ in range(52): self.job()
        self.assertEqual(len((await self.history(limit=50))['jobs']), 50)

    async def test_history_is_read_only_without_sync_or_telegram_rpc(self):
        self.job()
        self.job(kind='post', state='completed', items=[(GROUP_B, 'sent')])
        self.ops.store.put('preserved-setting', {'enabled': False, 'limit': 17})
        snapshot = {table: self.ops.store.rows('SELECT * FROM '+table) for table in ['op_jobs', 'op_items', 'op_accounts', 'settings']}
        changes = self.ops.store.db.total_changes
        client = Mock()
        client.side_effect = AssertionError('History must not call Telegram')
        client.get_entity.side_effect = AssertionError('History must not resolve peers')
        self.ops.accounts[self.account].client = client
        writes = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                  sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_ALTER_TABLE}
        self.ops.store.db.set_authorizer(lambda action, *_: sqlite3.SQLITE_DENY if action in writes else sqlite3.SQLITE_OK)
        try:
            with patch.object(self.ops, 'sync_primary', side_effect=AssertionError('History must not sync accounts')):
                result = await self.history(kind='invite', chat_id=GROUP_A, pending_only=True)
                self.assertEqual(len(result['jobs']), 1)
                self.assertNotIn('digest', result['jobs'][0])
                self.assertNotIn('request_key', result['jobs'][0])
        finally:
            self.ops.store.db.set_authorizer(None)
            self.ops.accounts[self.account].client = None
        self.assertEqual(changes, self.ops.store.db.total_changes)
        self.assertEqual(snapshot, {table: self.ops.store.rows('SELECT * FROM '+table) for table in snapshot})
        client.assert_not_called()
        client.get_entity.assert_not_called()


if __name__ == '__main__': unittest.main()
