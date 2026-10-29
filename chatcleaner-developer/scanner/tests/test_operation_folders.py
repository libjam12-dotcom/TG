import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from telethon import errors, functions, types
from engine import Store
from operation_folders import OperationFolders, FolderPreview, FolderJoin, advertisement_review

SENDER = 700000111
URL = 'https://t.me/addlist/Fixture_folder1'
GROUP = -1000000000101
CHANNEL = -1000000000102
OWN = -1000000000103


def group(number, **values):
    return types.Channel(id=number, title='Fixture ' + str(number), photo=types.ChatPhotoEmpty(),
                         date=datetime.now(timezone.utc), access_hash=number * 3,
                         megagroup=True, left=True, **values)


class Client:
    def __init__(self):
        self.calls = []
        self.join_error = None
        self.check_hook = None
        self.check_error = None
        self.chats = [group(101), types.Channel(id=102, title='Channel', photo=types.ChatPhotoEmpty(),
                      date=datetime.now(timezone.utc), access_hash=306, broadcast=True, left=True), group(103, creator=True)]
        self.chats[-1].left = False
        self.members = {103}
        self.rules = ''
        self.pinned = ''
        self.imported = False
        self.join_without_membership = False

    def is_connected(self):
        return True

    async def __call__(self, request):
        self.calls.append(request)
        if isinstance(request, functions.chatlists.CheckChatlistInviteRequest):
            if self.check_hook:
                self.check_hook()
            if self.check_error:
                raise self.check_error
            peers = [types.PeerChannel(chat.id) for chat in self.chats]
            if self.imported:
                return types.chatlists.ChatlistInviteAlready(filter_id=9, missing_peers=peers,
                    already_peers=[], chats=self.chats, users=[])
            return types.chatlists.ChatlistInvite(title=types.TextWithEntities('Folder', []),
                peers=peers, chats=self.chats, users=[])
        if isinstance(request, (functions.chatlists.JoinChatlistInviteRequest, functions.chatlists.JoinChatlistUpdatesRequest)):
            if self.join_error:
                raise self.join_error
            if not self.join_without_membership:
                self.members.update(peer.channel_id for peer in request.peers)
                for chat in self.chats:
                    if chat.id in self.members:
                        chat.left = False
            return types.Updates(updates=[], users=[], chats=[], date=datetime.now(timezone.utc), seq=1)
        if isinstance(request, functions.channels.GetParticipantRequest):
            if request.channel.channel_id not in self.members:
                raise errors.UserNotParticipantError(request)
            return SimpleNamespace(participant=types.ChannelParticipant(user_id=SENDER, date=datetime.now(timezone.utc)))
        if isinstance(request, functions.channels.GetFullChannelRequest):
            chat = next(chat for chat in self.chats if chat.id == request.channel.channel_id)
            return SimpleNamespace(chats=[chat], full_chat=SimpleNamespace(id=chat.id, about=self.rules,
                pinned_msg_id=77 if self.pinned else None))
        if isinstance(request, functions.channels.GetMessagesRequest):
            return SimpleNamespace(messages=[types.Message(id=77, peer_id=types.PeerChannel(request.channel.channel_id),
                message=self.pinned, date=datetime.now(timezone.utc))])
        raise AssertionError('Unexpected RPC: ' + type(request).__name__)


class Ops:
    def __init__(self, directory):
        self.store = Store(Path(directory) / 'test.sqlite3')
        self.lock = asyncio.Lock()
        self.destination_cache = {}
        self.store.db.executescript('''
          CREATE TABLE op_accounts(id TEXT PRIMARY KEY,telegram_id INTEGER,enabled INTEGER,not_before REAL,error TEXT);
          INSERT INTO op_accounts VALUES('extra',700000111,1,0,NULL);
          CREATE TABLE op_jobs(id INTEGER PRIMARY KEY,account TEXT,state TEXT);
          CREATE TABLE op_items(id INTEGER PRIMARY KEY,job INTEGER,state TEXT);
        ''')
        self.client = Client()
        self.sender = SimpleNamespace(me=types.User(id=SENDER), client=self.client)
        self.pauses = []

    def account_row(self, aid):
        values = self.store.rows('SELECT * FROM op_accounts WHERE id=?', (aid,))
        if not values:
            raise HTTPException(404, 'missing')
        return values[0]

    def readable_account(self, aid, *, check_cooldown=True):
        import time
        row = self.account_row(aid)
        if row['telegram_id'] != self.sender.me.id:
            raise HTTPException(409, 'identity')
        if check_cooldown and row['not_before'] > time.time():
            raise HTTPException(429, 'cooldown')
        return self.sender

    def online(self, aid):
        sender = self.readable_account(aid)
        if not self.account_row(aid)['enabled']:
            raise HTTPException(409, 'paused')
        return sender

    def pause_outbound(self, aid, exc):
        self.pauses.append(type(exc).__name__)
        with self.store.db:
            self.store.db.execute('UPDATE op_accounts SET enabled=0 WHERE id=?', (aid,))


class FolderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.ops = Ops(self.temp.name)
        self.module = OperationFolders(self.ops)
        self.module.initialize()

    async def asyncTearDown(self):
        self.ops.store.db.close()
        self.temp.cleanup()

    async def preview(self):
        return await self.module.preview('extra', FolderPreview(url=URL, telegram_id=SENDER))

    def spec(self, preview, ids=None, key='fixture-folder-join-0001'):
        return FolderJoin(preview_id=preview['preview_id'], telegram_id=SENDER,
                          chat_ids=ids or [GROUP], request_key=key, confirmed=True)

    def writes(self):
        return [request for request in self.ops.client.calls if isinstance(request,
            (functions.chatlists.JoinChatlistInviteRequest, functions.chatlists.JoinChatlistUpdatesRequest))]

    async def test_preview_exact_account_folder_no_join_and_no_private_hash_output(self):
        response = await self.preview()
        self.assertEqual(response['telegram_id'], SENDER)
        self.assertEqual(response['slug'], 'Fixture_folder1')
        self.assertEqual(len(response['sources']), 3)
        self.assertEqual(response['sources'][0]['advertising_permission'], 'unknown')
        self.assertFalse(response['sources'][1]['join_supported'])
        self.assertTrue(response['sources'][2]['managed'])
        self.assertNotIn('_access_hash', json.dumps(response))
        self.assertEqual(self.writes(), [])
        self.assertEqual(type(self.ops.client.calls[0]), functions.chatlists.CheckChatlistInviteRequest)

    async def test_preview_identity_change_discards_response(self):
        self.ops.client.check_hook = lambda: setattr(self.ops.sender.me, 'id', SENDER + 1)
        with self.assertRaises(HTTPException):
            await self.preview()
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_folder_previews'), [])

    async def test_primary_join_is_forbidden_without_network_or_claim(self):
        with self.ops.store.db:
            self.ops.store.db.execute("INSERT INTO op_accounts VALUES('primary',?,1,0,NULL)", (SENDER,))
        response = await self.module.preview('primary', FolderPreview(url=URL, telegram_id=SENDER))
        before = len(self.ops.client.calls)
        with self.assertRaises(HTTPException) as caught:
            await self.module.join('primary', self.spec(response))
        self.assertEqual(caught.exception.status_code, 403)
        self.assertEqual(len(self.ops.client.calls), before)
        self.assertEqual(self.writes(), [])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_folder_join_requests'), [])

    async def test_batch_join_is_one_write_exact_peers_and_independent_membership(self):
        response = await self.preview()
        result = await self.module.join('extra', self.spec(response, [GROUP, OWN]))
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(result['summary'], {'joined': 1, 'already_member': 1})
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual([peer.channel_id for peer in self.writes()[0].peers], [101])
        self.assertTrue(all(item['member_after'] for item in result['items']))
        calls = len(self.ops.client.calls)
        replay = await self.module.join('extra', self.spec(response, [GROUP, OWN]))
        self.assertEqual(replay, result)
        self.assertEqual(len(self.ops.client.calls), calls, 'Replay must read only saved receipt, even during cooldown')

    async def test_imported_folder_uses_exact_filter_and_subset(self):
        self.ops.client.imported = True
        response = await self.preview()
        await self.module.join('extra', self.spec(response))
        request = self.writes()[0]
        self.assertIsInstance(request, functions.chatlists.JoinChatlistUpdatesRequest)
        self.assertEqual(request.chatlist.filter_id, 9)
        self.assertEqual([peer.channel_id for peer in request.peers], [101])

    async def test_new_key_cannot_retry_unknown_join_even_after_new_preview(self):
        self.ops.client.join_error = OSError('synthetic-secret-must-not-leak')
        response = await self.preview()
        result = await self.module.join('extra', self.spec(response))
        self.assertEqual(result['state'], 'unknown')
        self.assertNotIn('synthetic-secret', json.dumps(result))
        self.ops.client.join_error = None
        fresh = await self.preview()
        with self.assertRaises(HTTPException):
            await self.module.join('extra', self.spec(fresh, key='fixture-folder-join-0002'))
        self.assertEqual(len(self.writes()), 1)

    async def test_success_response_without_membership_is_unknown_not_joined(self):
        self.ops.client.join_without_membership = True
        result = await self.module.join('extra', self.spec(await self.preview()))
        self.assertEqual(result['state'], 'partial')
        self.assertEqual(result['items'][0]['state'], 'unknown')
        self.assertIs(result['items'][0]['member_after'], False)

    async def test_channels_and_outside_snapshot_rejected_before_write(self):
        response = await self.preview()
        for cid in (CHANNEL, -1000000099999):
            with self.assertRaises(HTTPException):
                await self.module.join('extra', self.spec(response, [cid]))
        self.assertEqual(self.writes(), [])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_folder_join_requests'), [])

    async def test_folder_changed_since_review_rejected_before_claim(self):
        response = await self.preview()
        self.ops.client.chats = self.ops.client.chats[1:]
        with self.assertRaises(HTTPException):
            await self.module.join('extra', self.spec(response))
        self.assertEqual(self.writes(), [])
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_folder_join_requests'), [])

    async def test_only_latest_preview_can_join(self):
        old = await self.preview()
        await self.preview()
        with self.assertRaises(HTTPException):
            await self.module.join('extra', self.spec(old))
        self.assertEqual(self.writes(), [])

    async def test_flood_on_join_pauses_and_stops_without_membership_spam(self):
        response = await self.preview()
        self.ops.client.join_error = errors.FloodWaitError(request=None, capture=45)
        result = await self.module.join('extra', self.spec(response))
        self.assertEqual(result['state'], 'unknown')
        self.assertFalse(self.ops.account_row('extra')['enabled'])
        self.assertGreater(self.ops.account_row('extra')['not_before'], 0)
        self.assertEqual(self.ops.pauses, ['FloodWaitError'])
        self.assertFalse(any(isinstance(request, functions.channels.GetParticipantRequest) for request in self.ops.client.calls))

    async def test_restart_marks_inflight_unknown_and_same_key_never_writes(self):
        response = await self.preview()
        self.ops.client.join_error = TimeoutError()
        spec = self.spec(response)
        result = await self.module.join('extra', spec)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_folder_join_requests SET state='joining'")
            self.ops.store.db.execute("UPDATE op_folder_join_items SET state='requested'")
        fresh = OperationFolders(self.ops)
        fresh.initialize()
        calls = len(self.ops.client.calls)
        replay = await fresh.join('extra', spec)
        self.assertEqual(replay['state'], 'unknown')
        self.assertEqual(replay['items'][0]['error_code'], 'ProcessInterrupted')
        self.assertEqual(len(self.ops.client.calls), calls)

    async def test_audit_reads_about_exact_pin_and_never_auto_permits(self):
        response = await self.preview()
        self.ops.client.rules = 'Добро пожаловать. Реклама только по согласованию с администратором.'
        self.ops.client.pinned = 'Правила чата. Будьте вежливы.'
        result = await self.module.audit('extra', response['preview_id'], GROUP, SENDER)
        self.assertEqual(result['advertising_permission'], 'unknown')
        self.assertTrue(result['manual_review_required'])
        reads = [request for request in self.ops.client.calls if isinstance(request, functions.channels.GetMessagesRequest)]
        self.assertEqual(len(reads), 1)
        self.assertEqual([item.id for item in reads[0].id], [77])
        self.assertEqual(self.writes(), [])

    async def test_explicit_advertising_ban_is_evidence_and_blocks_campaign_join(self):
        response = await self.preview()
        self.ops.client.rules = 'Реклама запрещена.'
        result = await self.module.audit('extra', response['preview_id'], GROUP, SENDER)
        self.assertEqual(result['advertising_permission'], 'forbidden')
        with self.assertRaises(HTTPException):
            await self.module.join('extra', self.spec(response))
        self.assertEqual(self.writes(), [])

    async def test_wrong_sender_and_account_reuse_cannot_read_or_join_saved_snapshot(self):
        response = await self.preview()
        with self.assertRaises(HTTPException):
            await self.module.audit('extra', response['preview_id'], GROUP, SENDER + 1)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET telegram_id=? WHERE id='extra'", (SENDER + 1,))
        with self.assertRaises(HTTPException):
            await self.module.preview_record('extra', response['preview_id'])
        with self.assertRaises(HTTPException):
            await self.module.join('extra', self.spec(response))
        self.assertEqual(self.writes(), [])


class RulesTests(unittest.TestCase):
    def test_unknown_never_becomes_permission_even_for_positive_or_empty_text(self):
        for text in ('', 'Реклама разрешена по согласованию.', 'Мы продаём рекламу.', 'No advertisements', 'Реклама запрещена'):
            result = advertisement_review(text, '')
            self.assertIn(result['advertising_permission'], ('unknown', 'forbidden'))
        self.assertEqual(advertisement_review('No advertisements', '')['advertising_permission'], 'forbidden')


if __name__ == '__main__':
    unittest.main()
