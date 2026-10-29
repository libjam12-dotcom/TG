"""External sources must work without admin rights or implicit joining."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store
from telethon import errors, functions, types, utils


class ExternalSourceTests(unittest.TestCase):
    def setUp(self):
        import app
        from fastapi.testclient import TestClient
        self.app = app
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'external.sqlite3')
        self.group = types.Channel(id=712345, title='External Dubai', username='externaldubai',
                                   photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
                                   megagroup=True, left=True)
        self.cid = utils.get_peer_id(self.group)
        self.message = SimpleNamespace(id=11, message='Ищу яхту на выходные', out=False,
                                       sender_id=555, date=datetime.now(timezone.utc),
                                       get_sender=AsyncMock(return_value=types.User(id=555, username='requester')))
        owner = self

        class Client:
            get_entity = AsyncMock(side_effect=lambda ref: owner.group)
            get_messages = AsyncMock(return_value=[SimpleNamespace(id=10, date=datetime.now(timezone.utc))])
            def is_connected(self): return True
            async def iter_messages(self, chat_id, **kwargs):
                if owner.message.id > kwargs['min_id']: yield owner.message
            async def __call__(self, request):
                if not isinstance(request, functions.contacts.SearchRequest):
                    raise AssertionError('External source setup must not join or send messages')
                return SimpleNamespace(chats=owner.discovery)

        self.telegram = Client()
        self.runtime = app.Runtime()
        self.runtime.client = self.telegram
        self.runtime.me = SimpleNamespace(id=42)
        self.runtime.status = 'connected'
        self.patches = [patch.object(app, 'store', self.store), patch.object(app, 'runtime', self.runtime)]
        for p in self.patches: p.start()
        app.bot_notifications.initialize()
        self.client = TestClient(app.app, raise_server_exceptions=False)
        self.headers = {'X-Panel-User': 'tester', 'Origin': app.ORIGIN, 'X-Scanner-Request': '1'}

    def tearDown(self):
        self.client.close()
        for p in reversed(self.patches): p.stop()
        self.store.db.close()
        self.tmp.cleanup()

    def post(self, path, body):
        return self.client.post('/api/scanner/' + path, json=body, headers=self.headers)

    def test_nonmember_public_source_reads_and_polls_without_joining(self):
        result = self.post('sources', {'reference': '@externaldubai'})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['access_mode'], 'public')
        self.assertEqual(self.store.rows('SELECT * FROM sources')[0]['cursor'], 10)
        result = self.post('rules', {'name': 'Yachts', 'sources': [self.cid], 'keywords': 'яхту', 'required': 'ищу'})
        self.assertEqual(result.status_code, 200)
        self.store.put('running', True)
        asyncio.run(self.runtime.scan())
        asyncio.run(self.runtime.scan())
        self.assertEqual(len(self.store.rows('SELECT * FROM leads')), 1)
        source = self.store.rows('SELECT * FROM sources')[0]
        self.assertEqual(source['cursor'], 11)
        self.assertIsNotNone(source['last_message_at'])
        self.assertIsNone(source['error'])

    def test_owned_admin_and_creator_sources_are_rejected(self):
        for creator in (False, True):
            self.group.creator = creator
            self.group.admin_rights = None if creator else types.ChatAdminRights(delete_messages=True)
            result = self.post('sources', {'reference': '@externaldubai'})
            self.assertEqual(result.status_code, 400)
            self.assertIn('административная', result.json()['detail'])
        self.telegram.get_messages.assert_not_awaited()
        self.assertEqual(self.store.rows('SELECT * FROM sources'), [])

    def test_private_inaccessible_source_and_telegram_denial_are_not_added(self):
        self.group.username = None
        self.assertEqual(self.post('sources', {'reference': str(self.cid)}).status_code, 400)
        self.telegram.get_messages.assert_not_awaited()
        self.group.username = 'externaldubai'
        self.telegram.get_messages.side_effect = errors.ChannelPrivateError(request=None)
        result = self.post('sources', {'reference': '@externaldubai'})
        self.assertEqual(result.status_code, 400)
        self.assertIn('не разрешает читать', result.json()['detail'])
        self.assertEqual(self.store.rows('SELECT * FROM sources'), [])

    def test_discovery_excludes_managed_sources_but_keeps_external_memberships(self):
        owned = types.Channel(id=777777, title='Owned', username='owneddubai',
                              photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
                              megagroup=True, creator=True)
        member = types.Channel(id=888888, title='External member', username='externalmember',
                               photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
                               megagroup=True, left=False)
        self.discovery = [self.group, owned, member]
        result = self.post('discover', {'query': 'Dubai', 'kind': 'group'})
        self.assertEqual(result.status_code, 200)
        self.assertEqual({r['username'] for r in result.json()['results']}, {'externaldubai', 'externalmember'})


class SourceMigrationTests(unittest.TestCase):
    def test_existing_database_keeps_cursor_and_sources_after_upgrade(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'old.sqlite3'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE sources(id INTEGER PRIMARY KEY,title TEXT NOT NULL,username TEXT,enabled INTEGER NOT NULL DEFAULT 1,cursor INTEGER NOT NULL DEFAULT 0,last_scan TEXT,error TEXT)')
            db.execute("INSERT INTO sources(id,title,cursor) VALUES(-1001,'Existing',4321)")
            db.commit(); db.close()
            for _ in range(2):
                store = Store(path)
                source = store.rows('SELECT * FROM sources')[0]
                self.assertEqual(source['cursor'], 4321)
                self.assertEqual(source['access_mode'], 'member')
                self.assertIsNone(source['last_message_at'])
                store.db.close()
