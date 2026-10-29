import asyncio
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store
from fastapi import HTTPException
from operation_test_chat import OperationTestChat, TestChatSpec
from telethon import functions, types


class TestChatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'test.sqlite3')
        self.calls = []
        self.failure = None
        self.account = SimpleNamespace(me=SimpleNamespace(id=123), client=AsyncMock(side_effect=self.create_rpc))
        self.ops = SimpleNamespace(store=self.store, lock=asyncio.Lock(),
                                   online=lambda aid: self.account, destination_cache={'a': 'old'})
        self.route = OperationTestChat(self.ops)
        self.spec = TestChatSpec(request_key='test-chat-unique-key-0001', telegram_id=123, confirmed=True)

    async def create_rpc(self, request):
        self.calls.append(request)
        if self.failure:
            raise self.failure
        return SimpleNamespace(chats=[types.Channel(
            id=42, title='Chatcleaner · тест маршрута UFC 333', photo=types.ChatPhotoEmpty(),
            date=datetime.now(timezone.utc), megagroup=True, creator=True)])

    async def asyncTearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    async def test_replayed_request_creates_one_empty_private_group(self):
        first = await self.route.create('a', self.spec)
        second = await self.route.create('a', self.spec)
        self.assertEqual(first, second)
        self.assertEqual(first['chat_id'], -1000000000042)
        self.assertEqual(len(self.calls), 1)
        self.assertIsInstance(self.calls[0], functions.channels.CreateChannelRequest)
        self.assertTrue(self.calls[0].megagroup)
        self.assertNotIn('a', self.ops.destination_cache)

    async def test_unknown_creation_is_never_retried(self):
        self.failure = TimeoutError('sensitive data must not leave the API')
        first = await self.route.create('a', self.spec)
        second = await self.route.create('a', self.spec)
        self.assertEqual(first['state'], 'unknown')
        self.assertEqual(first, second)
        self.assertNotIn('sensitive', str(second))
        self.assertEqual(len(self.calls), 1)

    async def test_identity_mismatch_has_no_rpc_and_key_cannot_move_accounts(self):
        wrong = self.spec.model_copy(update={'telegram_id': 456})
        with self.assertRaises(HTTPException):
            await self.route.create('a', wrong)
        self.assertFalse(self.calls)
        await self.route.create('a', self.spec)
        with self.assertRaises(HTTPException):
            await self.route.create('b', self.spec)
        self.assertEqual(len(self.calls), 1)

    async def test_restart_recovers_inflight_creation_as_unknown_without_rpc(self):
        key = 'test_chat_' + hashlib.sha256(self.spec.request_key.encode()).hexdigest()
        self.store.put(key, dict(account='a', telegram_id=123, state='creating'))
        self.route.initialize()
        result = await self.route.create('a', self.spec)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(result['error_code'], 'ProcessInterrupted')
        self.assertFalse(self.calls)
