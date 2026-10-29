import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from telethon import errors, functions, types
from engine import Store, now
from operation_tracking import OperationTracking, TrackingInbox, TrackingReply


CHAT_A, CHAT_B = -1000000000010, -1000000000020


def timestamp_for_test(value):
    return datetime.fromisoformat(value).timestamp()


class Client:
    def __init__(self):
        self.handlers, self.calls, self.reads, self.resolves = [], [], [], []
        self.failure, self.read_failure, self.receipt_missing = None, None, False
        self.connected, self.available = True, {700}

    def is_connected(self):
        return self.connected

    def add_event_handler(self, handler, builder):
        self.handlers.append((handler, builder))

    def remove_event_handler(self, handler):
        self.handlers = [(h, b) for h, b in self.handlers if h is not handler]

    async def get_input_entity(self, chat):
        self.resolves.append(chat)
        if chat < -10**12:
            return types.InputPeerChannel(-chat - 10**12, 123)
        if chat < 0:
            return types.InputPeerChat(-chat)
        return types.InputPeerUser(chat, 123)

    async def get_messages(self, chat, **kwargs):
        self.reads.append((chat, kwargs))
        if self.read_failure:
            raise self.read_failure
        return [SimpleNamespace(id=n) if n in self.available else types.MessageEmpty(n, types.PeerChannel(10)) for n in kwargs['ids']]

    async def __call__(self, req):
        self.calls.append(req)
        if self.failure:
            raise self.failure
        if self.receipt_missing:
            return types.Updates(updates=[], users=[], chats=[], date=datetime.now(timezone.utc), seq=0)
        return types.UpdateShortSentMessage(id=900, pts=1, pts_count=1, date=datetime.now(timezone.utc))


class Ops:
    def __init__(self, path):
        self.store = Store(path)
        self.lock = asyncio.Lock()
        self.accounts, self.pauses = {}, []
        self.store.db.executescript('''
          CREATE TABLE op_accounts(id TEXT PRIMARY KEY,label TEXT,telegram_id INTEGER,username TEXT,enabled INTEGER,not_before REAL,error TEXT);
          CREATE TABLE op_jobs(id INTEGER PRIMARY KEY,account TEXT,telegram_id INTEGER,kind TEXT,created TEXT);
          CREATE TABLE op_items(id INTEGER PRIMARY KEY,job INTEGER,chat_id INTEGER,chat_title TEXT,chat_username TEXT,state TEXT,message_ids TEXT,attempted_at TEXT,finished_at TEXT,error TEXT);
        ''')
        for aid, tid in [('a', 100), ('b', 200)]:
            self.store.db.execute('INSERT INTO op_accounts VALUES(?,?,?,?,1,0,NULL)', (aid, aid, tid, 'account_' + aid))
            self.accounts[aid] = SimpleNamespace(client=Client(), me=SimpleNamespace(id=tid))
        self.store.db.commit()

    def account_row(self, aid):
        rows = self.store.rows('SELECT * FROM op_accounts WHERE id=?', (aid,))
        if not rows:
            raise HTTPException(404, 'AccountMissing')
        return rows[0]

    def readable_account(self, aid):
        row, account = self.account_row(aid), self.accounts.get(aid)
        if not account or not account.client.is_connected() or account.me.id != row['telegram_id']:
            raise HTTPException(409, 'IdentityChanged')
        if row['not_before'] > time.time():
            raise HTTPException(429, 'Cooldown')
        return account

    def online(self, aid):
        account, row = self.readable_account(aid), self.account_row(aid)
        if not row['enabled'] or row['not_before'] > datetime.now(timezone.utc).timestamp():
            raise HTTPException(409, 'OutboundPaused')
        return account

    def pause_outbound(self, aid, exc):
        self.pauses.append((aid, type(exc).__name__))
        with self.store.db:
            self.store.db.execute('UPDATE op_accounts SET enabled=0 WHERE id=?', (aid,))


class TrackingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Ops(Path(self.tmp.name) / 'tracking.sqlite3')
        self.tracking = OperationTracking(self.ops)
        self.tracking.initialize()
        self.c = self.ops.accounts['a'].client
        self.seed()

    async def asyncTearDown(self):
        self.tracking.unbind_clients()
        self.ops.store.db.close()
        self.tmp.cleanup()

    def seed(self, jid=1, iid=1, account='a', chat=CHAT_A, state='sent', mids=None, kind='post'):
        tid = self.ops.account_row(account)['telegram_id']
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT OR IGNORE INTO op_jobs VALUES(?,?,?,?,?)', (jid, account, tid, kind, now()))
            self.ops.store.db.execute('INSERT INTO op_items VALUES(?,?,?,?,?,?,?,?,?,?)',
                (iid, jid, chat, 'Group ' + str(chat), None, state, json.dumps([700] if mids is None else mids), now(), now(), None))

    def incoming(self, mid=701, chat=CHAT_A, reply=700, private=False, text='Question', date=None, peer=None):
        header = SimpleNamespace(reply_to_msg_id=reply, reply_to_peer_id=peer)
        msg = SimpleNamespace(id=mid, out=False, message=text, sender_id=555,
                              reply_to=header, date=date or datetime.now(timezone.utc))
        return SimpleNamespace(chat_id=chat, is_private=private, message=msg)

    async def ingest(self, **kwargs):
        await self.tracking.on_new_message('a', 100, self.c, self.incoming(**kwargs))

    def events(self, kind=None):
        return self.ops.store.rows('SELECT * FROM op_tracking_events' + (' WHERE kind=?' if kind else '') + ' ORDER BY id', (kind,) if kind else ())

    async def reply(self, event_id=None, key='reply-unique-request-0001', text='Answer'):
        if event_id is None:
            rows = self.events('group_reply')
            if not rows:
                await self.ingest()
                rows = self.events('group_reply')
            event_id = rows[0]['id']
        return await self.tracking.reply(1, TrackingReply(event_id=event_id, request_key=key, text=text, confirmed=True))

    async def test_sync_is_idempotent_post_only_and_preserves_send_state(self):
        self.seed(jid=2, iid=2, kind='invite')
        before = self.ops.store.rows('SELECT * FROM op_items')
        self.tracking.sync_jobs(); self.tracking.sync_jobs()
        self.assertEqual([r['kind'] for r in self.events()], ['send_requested', 'sent_confirmed'])
        self.assertEqual(before, self.ops.store.rows('SELECT * FROM op_items'))
        self.assertEqual(len(self.ops.store.rows('SELECT * FROM op_tracking_publications')), 1)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_items SET state='removed' WHERE id=1")
        payload = await self.tracking.job(1)
        self.assertEqual(payload['publications'][0]['presence'], 'removed_by_panel')
        self.assertEqual(payload['publications'][0]['send_state'], 'removed')
        self.assertEqual(payload['summary']['sent_confirmed'], 1)
        self.assertEqual(payload['summary']['removed_by_panel'], 1)
        self.assertFalse(self.c.calls)

    async def test_unknown_and_failed_publications_are_distinct(self):
        self.seed(jid=2, iid=2, state='uncertain', mids=[])
        self.seed(jid=3, iid=3, state='failed', mids=[])
        self.tracking.sync_jobs()
        self.assertEqual(self.events('send_unknown')[0]['job_id'], 2)
        self.assertEqual(self.events('send_failed')[0]['job_id'], 3)
        self.assertFalse(self.events('deletion_observed'))

    async def test_binding_is_per_client_identity_no_duplicates_or_owner_sender(self):
        owner = Client()
        self.ops.owner = SimpleNamespace(client=owner)
        self.tracking.bind_clients(); self.tracking.bind_clients()
        self.assertEqual(len(self.c.handlers), 2)
        self.assertEqual(len(self.ops.accounts['b'].client.handlers), 2)
        self.assertFalse(owner.handlers)
        replacement = Client()
        self.ops.accounts['a'].client = replacement
        self.tracking.bind_clients()
        self.assertFalse(self.c.handlers)
        self.assertEqual(len(replacement.handlers), 2)
        self.ops.accounts['a'].me.id = 999
        self.tracking.bind_clients()
        self.assertFalse(replacement.handlers)
        self.assertFalse(self.tracking.observer('a', 100)['identity_matches'])

    async def test_replies_require_exact_account_chat_and_saved_ancestry(self):
        self.seed(jid=2, iid=2, account='b', chat=CHAT_B)
        await self.ingest(chat=CHAT_B)
        await self.tracking.on_new_message('b', 100, self.ops.accounts['b'].client, self.incoming())
        await self.ingest(reply=666)
        await self.ingest(peer=types.PeerChannel(20))
        self.assertFalse(self.events('group_reply'))
        await self.ingest(); await self.ingest()
        first = self.events('group_reply')[0]
        await self.ingest(mid=702, reply=701)
        rows = self.events('group_reply')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]['parent_event_id'], first['id'])
        self.assertEqual(rows[1]['item_id'], 1)
        await self.ingest(mid=703, chat=CHAT_B, reply=701)
        self.assertEqual(len(self.events('group_reply')), 2)

    async def test_inbox_is_opt_in_new_only_and_never_attributed_to_job(self):
        old = datetime.now(timezone.utc) - timedelta(minutes=1)
        await self.ingest(mid=90, chat=555, private=True)
        self.assertFalse(self.events('incoming_dm'))
        enabled = await self.tracking.set_inbox('a', TrackingInbox(enabled=True, telegram_id=100))
        self.assertTrue(enabled['enabled'])
        await self.ingest(mid=90, chat=555, private=True, date=old)
        await self.ingest(mid=91, chat=555, private=True)
        await self.ingest(mid=91, chat=555, private=True)
        rows = self.events('incoming_dm')
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]['job_id']); self.assertEqual(rows[0]['attribution'], 'unknown')
        self.assertFalse((await self.tracking.job(1))['summary'].get('incoming_dm'))
        self.assertEqual(len((await self.tracking.inbox('a'))['events']), 1)
        self.assertFalse(self.c.reads)
        await self.tracking.set_inbox('a', TrackingInbox(enabled=False, telegram_id=100))
        await self.ingest(mid=92, chat=555, private=True)
        self.assertEqual(len(self.events('incoming_dm')), 1)
        with self.assertRaises(HTTPException):
            await self.tracking.set_inbox('a', TrackingInbox(enabled=True, telegram_id=999))

    async def test_deletion_observation_never_changes_send_state_and_scopes_ids(self):
        self.seed(jid=2, iid=2, chat=CHAT_B)
        self.seed(jid=3, iid=3, chat=-10)
        await self.tracking.on_deleted('a', 100, self.c, SimpleNamespace(chat_id=None, deleted_ids=[700]))
        self.assertEqual([e['chat_id'] for e in self.events('deletion_observed')], [-10])
        await self.tracking.on_deleted('a', 100, self.c, SimpleNamespace(chat_id=CHAT_A, deleted_ids=[700]))
        self.assertEqual({e['chat_id'] for e in self.events('deletion_observed')}, {-10, CHAT_A})
        self.assertEqual({r['state'] for r in self.ops.store.rows('SELECT state FROM op_items')}, {'sent'})
        self.assertEqual((await self.tracking.job(2))['publications'][0]['presence'], 'not_checked')

    async def test_deletion_update_does_not_truncate_after_ten_ids(self):
        await self.tracking.on_deleted('a', 100, self.c,
            SimpleNamespace(chat_id=CHAT_A, deleted_ids=list(range(1, 50)) + [700]))
        self.assertEqual(len(self.events('deletion_observed')), 1)
        self.assertEqual(self.events('deletion_observed')[0]['message_id'], 700)

    async def test_newer_live_delete_wins_over_inflight_presence_snapshot(self):
        original = self.c.get_messages
        async def snapshot_after_delete(chat, **kwargs):
            snapshot = await original(chat, **kwargs)
            await self.tracking.on_deleted('a', 100, self.c,
                SimpleNamespace(chat_id=CHAT_A, deleted_ids=[700]))
            return snapshot
        self.c.get_messages = snapshot_after_delete
        data = await self.tracking.check(1)
        self.assertEqual(data['publications'][0]['presence'], 'deletion_observed')
        self.assertTrue(self.events('check_superseded'))
        self.assertFalse(self.events('present'))
        self.c.get_messages = original
        data = await self.tracking.check(1)
        self.assertEqual(data['publications'][0]['presence'], 'present')

    async def test_check_uses_only_saved_ids_errors_are_not_deletions(self):
        first = await self.tracking.check(1)
        self.assertEqual(first['publications'][0]['presence'], 'present')
        self.assertEqual(self.c.reads, [(CHAT_A, {'ids': [700]})])
        self.c.read_failure = errors.ChannelPrivateError(None)
        failed = await self.tracking.check(1)
        self.assertEqual(failed['publications'][0]['presence'], 'present')
        self.assertEqual(failed['publications'][0]['check_error_code'], 'ChannelPrivateError')
        self.assertEqual(self.events('check_failed')[-1]['rpc_code'], 400)
        self.c.read_failure = RuntimeError('SESSION_SECRET must never be exposed')
        safe = await self.tracking.check(1)
        self.assertNotIn('SESSION_SECRET', json.dumps(safe))
        self.c.read_failure = None; self.c.available.clear()
        missing = await self.tracking.check(1)
        self.assertEqual(missing['publications'][0]['presence'], 'missing_on_check')
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_items')[0]['state'], 'sent')
        self.assertFalse(self.events('deletion_observed')); self.assertFalse(self.c.calls)

    async def test_check_is_bounded_to_thirty_items(self):
        for iid in range(2, 33):
            self.seed(jid=1, iid=iid, chat=CHAT_A-iid)
        data = await self.tracking.check(1)
        self.assertEqual(len(self.c.reads), 30)
        self.assertEqual(len(data['publications']), 30)

    async def test_monitor_skips_legacy_removed_unknown_and_old_publications(self):
        self.seed(jid=2, iid=2, state='removed')
        self.seed(jid=3, iid=3, state='uncertain')
        self.seed(jid=4, iid=4)
        old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET created=? WHERE id=1', (old,))
            self.ops.store.db.execute('UPDATE op_items SET attempted_at=?,finished_at=? WHERE id=4', (old, old))
        self.tracking.sync_jobs()
        self.assertFalse(await self.tracking.monitor_once())
        self.assertFalse(self.c.reads)
        self.assertTrue(all(row['next_check_at'] is None for row in self.ops.store.rows('SELECT next_check_at FROM op_tracking_publications')))
        installed = self.ops.store.get('operation_tracking_installed_at')
        self.tracking.initialize()
        self.assertEqual(self.ops.store.get('operation_tracking_installed_at'), installed)

    async def test_monitor_reads_one_publication_per_iteration_with_bounded_intervals(self):
        self.seed(jid=2, iid=2, chat=CHAT_B)
        self.tracking.sync_jobs()
        pubs = self.ops.store.rows('SELECT * FROM op_tracking_publications ORDER BY item_id')
        first_due = max(row['next_check_at'] for row in pubs)
        self.assertAlmostEqual(pubs[0]['next_check_at'] - timestamp_for_test(self.ops.store.rows('SELECT finished_at FROM op_items WHERE id=1')[0]['finished_at']), 60, places=3)
        with patch('operation_tracking.time.time', return_value=first_due - 1):
            self.assertFalse(await self.tracking.monitor_once())
        with patch('operation_tracking.time.time', return_value=first_due):
            self.assertTrue(await self.tracking.monitor_once())
            self.assertEqual(len(self.c.reads), 1)
        first = self.ops.store.rows('SELECT * FROM op_tracking_publications WHERE item_id=1')[0]
        self.assertEqual(first['check_count'], 1)
        self.assertEqual(first['next_check_at'], first_due + 300)
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_tracking_publications SET next_check_at=0 WHERE item_id=2')
        with patch('operation_tracking.time.time', return_value=first_due + 300):
            self.assertTrue(await self.tracking.monitor_once())
        first = self.ops.store.rows('SELECT * FROM op_tracking_publications WHERE item_id=1')[0]
        self.assertEqual(first['check_count'], 2)
        self.assertEqual(first['next_check_at'], first_due + 300 + 1800)
        self.assertEqual(len(self.c.reads), 2)
        await self.tracking.check_lock.acquire()
        try:
            self.assertFalse(await self.tracking.monitor_once())
        finally:
            self.tracking.check_lock.release()
        with patch('operation_tracking.time.time', return_value=first_due + 86401):
            self.assertFalse(await self.tracking.monitor_once())
        self.assertEqual(len(self.c.reads), 2)
        self.assertEqual(self.ops.store.rows('SELECT next_check_at FROM op_tracking_publications WHERE item_id=1')[0]['next_check_at'], 0)

    async def test_monitor_error_is_not_missing_and_flood_cooldown_is_respected(self):
        self.tracking.sync_jobs()
        due = self.ops.store.rows('SELECT next_check_at FROM op_tracking_publications')[0]['next_check_at']
        self.c.read_failure = errors.FloodWaitError(None, capture=500)
        with patch('operation_tracking.time.time', return_value=due):
            self.assertTrue(await self.tracking.monitor_once())
        pub = self.ops.store.rows('SELECT * FROM op_tracking_publications')[0]
        self.assertEqual(pub['presence'], 'not_checked')
        self.assertEqual(pub['check_error_code'], 'FloodWaitError')
        self.assertEqual(pub['next_check_at'], due + 501)
        self.assertEqual(self.events('check_failed')[0]['source'], 'automatic_presence_check')
        self.assertFalse(self.events('deletion_observed')); self.assertFalse(self.events('missing_on_check'))
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_tracking_publications SET next_check_at=?', (due,))
        with patch('operation_tracking.time.time', return_value=due + 1):
            self.assertFalse(await self.tracking.monitor_once())
        self.assertEqual(len(self.c.reads), 1)
        self.assertFalse(self.c.calls)

    async def test_manual_reply_exact_peer_plain_text_and_idempotent_receipt(self):
        result = await self.reply(text='Literal **text** <b>text</b>')
        again = await self.reply(text='Literal **text** <b>text</b>')
        self.assertEqual(result['state'], 'sent'); self.assertEqual(again['message_id'], 900)
        self.assertEqual(len(self.c.calls), 1)
        req = self.c.calls[0]
        self.assertIsInstance(req, functions.messages.SendMessageRequest)
        self.assertEqual(req.peer.channel_id, 10)
        self.assertEqual(req.reply_to.reply_to_msg_id, 701)
        self.assertEqual(req.message, 'Literal **text** <b>text</b>')
        self.assertIsNone(req.entities)
        self.assertFalse(result['tracking']['events'][-1]['can_reply'])
        with self.assertRaises(HTTPException):
            await self.reply(key='reply-different-request-0002')
        with self.assertRaises(HTTPException):
            await self.reply(text='Changed body')
        await self.ingest(mid=702, reply=900)
        last = self.events('group_reply')[-1]
        self.assertEqual(last['parent_event_id'], self.events('reply_sent')[0]['id'])

    async def test_timeout_receipt_missing_and_restart_never_repeat(self):
        self.c.failure = asyncio.TimeoutError('SECRET timeout details')
        result = await self.reply()
        self.assertEqual(result['state'], 'unknown')
        self.assertNotIn('SECRET', json.dumps(result))
        self.tracking.initialize()
        again = await self.reply()
        self.assertEqual(again['state'], 'unknown'); self.assertEqual(len(self.c.calls), 1)
        with self.assertRaises(HTTPException):
            await self.reply(key='another-unknown-key-0001')
        self.c.failure = None; self.c.receipt_missing = True
        await self.ingest(mid=702)
        second = await self.reply(event_id=self.events('group_reply')[-1]['id'], key='receipt-missing-key-0001')
        self.assertEqual(second['state'], 'unknown')
        self.assertEqual(second['error_code'], 'ReceiptMissing')
        self.assertEqual(len(self.c.calls), 2)

    async def test_interrupted_claim_is_unknown_and_cannot_requeue(self):
        await self.reply()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_tracking_replies SET state='sending',message_id=NULL")
        self.tracking.initialize()
        result = await self.reply()
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(len(self.c.calls), 1)

    async def test_reply_refuses_wrong_job_account_identity_and_paused_outbound(self):
        await self.ingest()
        event_id = self.events('group_reply')[0]['id']
        self.seed(jid=2, iid=2)
        with self.assertRaises(HTTPException):
            await self.tracking.reply(2, TrackingReply(event_id=event_id, request_key='wrong-job-request-0001', text='No', confirmed=True))
        self.ops.accounts['a'].me.id = 101
        with self.assertRaises(HTTPException):
            await self.reply()
        self.ops.accounts['a'].me.id = 100
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0 WHERE id='a'")
        self.assertFalse((await self.tracking.job(1))['events'][-1]['can_reply'])
        with self.assertRaises(HTTPException):
            await self.reply()
        self.assertFalse(self.c.calls)

    async def test_flood_pauses_outbound_and_never_retries(self):
        self.c.failure = errors.FloodWaitError(None, capture=60)
        result = await self.reply()
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(self.ops.pauses, [('a', 'FloodWaitError')])
        self.assertGreater(self.ops.account_row('a')['not_before'], datetime.now(timezone.utc).timestamp() + 55)
        await self.reply()
        self.assertEqual(len(self.c.calls), 1)

    async def test_dm_manual_reply_is_account_bound_and_remains_unknown_attribution(self):
        await self.tracking.set_inbox('a', TrackingInbox(enabled=True, telegram_id=100))
        await self.ingest(mid=91, chat=555, private=True)
        event = self.events('incoming_dm')[0]
        spec = TrackingReply(event_id=event['id'], text='Reply', request_key='dm-only-manual-key-0001', confirmed=True)
        with self.assertRaises(HTTPException):
            await self.tracking.reply(1, spec)
        result = await self.tracking.inbox_reply('a', spec)
        self.assertEqual(result['state'], 'sent')
        self.assertIsNone(result['event']['job_id'])
        self.assertEqual(result['event']['attribution'], 'unknown')
        self.assertEqual(self.c.calls[0].peer.user_id, 555)
        self.assertEqual(self.c.calls[0].reply_to.reply_to_msg_id, 91)

    async def test_event_and_text_output_are_bounded(self):
        for mid in range(701, 1005):
            await self.ingest(mid=mid, text='X' * 5000)
        data = await self.tracking.job(1)
        self.assertEqual(len(data['events']), 300)
        self.assertTrue(all(len(e['text']) <= 2000 for e in data['events']))
        self.assertEqual(data['summary']['group_reply'], 304)
        self.assertNotIn('request_key', json.dumps(data['account']))

    async def test_api_contract_and_confirmation_validation(self):
        await self.ingest()
        event_id = self.events('group_reply')[0]['id']
        app = FastAPI(); app.include_router(self.tracking.router, prefix='/api/scanner/operations')
        with TestClient(app) as client:
            response = client.get('/api/scanner/operations/tracking/jobs/1')
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data['telegram_id'], 100)
            self.assertEqual(set(data), {'job_id', 'account', 'telegram_id', 'observer', 'summary', 'publications', 'events'})
            self.assertEqual(data['publications'][0]['links'], ['https://t.me/c/10/700'])
            self.assertTrue(data['events'][-1]['can_reply'])
            response = client.post('/api/scanner/operations/tracking/jobs/1/reply', json=dict(event_id=event_id, text='test', request_key='http-request-key-0001', confirmed=False))
            self.assertEqual(response.status_code, 422)
            response = client.post('/api/scanner/operations/tracking/accounts/a/inbox', json=dict(enabled=True, telegram_id=999))
            self.assertEqual(response.status_code, 409)
            response = client.post('/api/scanner/operations/tracking/jobs/1/check', json={})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['publications'][0]['presence'], 'present')
            response = client.get('/api/scanner/operations/tracking/jobs/999')
            self.assertEqual(response.status_code, 404)
        self.assertFalse(self.c.calls)


if __name__ == '__main__':
    unittest.main()
