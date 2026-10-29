"""HTTP availability and client-session recovery, entirely offline."""
import asyncio
import contextlib
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store


class FakeClient:
    def __init__(self):
        self.connected = False
        self.handlers = []
        self.hang_connect = False
        self.hang_authorized = False
        self.hang_identity = False
        self.connect_started = asyncio.Event()
        self.connect_cancelled = asyncio.Event()
        self.connect_calls = 0
        self.authorization_calls = 0
        self.me_calls = 0
        self.authorized = True
        self.identity = SimpleNamespace(id=42, username='client_account')

    def add_event_handler(self, callback, event):
        self.handlers.append((callback, event))

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connect_calls += 1
        self.connect_started.set()
        if self.hang_connect:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.connect_cancelled.set()
                raise
        self.connected = True

    async def is_user_authorized(self):
        self.authorization_calls += 1
        if self.hang_authorized:
            await asyncio.Event().wait()
        return self.authorized

    async def get_me(self):
        self.me_calls += 1
        if self.hang_identity:
            await asyncio.Event().wait()
        return self.identity

    async def disconnect(self):
        self.connected = False


class IdleService:
    def __init__(self):
        self.task = None

    def initialize(self):
        pass

    def recover(self):
        pass

    async def loop(self):
        await asyncio.Event().wait()

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task


class RuntimeStartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with patch.dict(os.environ, {'SCANNER_DATA': self.tmp.name}):
            import app
        self.app = app
        self.store = Store(Path(self.tmp.name) / 'test.sqlite3')
        self.store.put('api', {'id': 123, 'hash': '0' * 32})
        self.runtime = app.Runtime()
        self.client = FakeClient()
        self.patches = [patch.object(app, 'store', self.store),
                        patch.object(app, 'runtime', self.runtime),
                        patch.object(app, 'TelegramClient', return_value=self.client)]
        self.factory = self.patches[-1].start()
        for item in self.patches[:-1]:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.store.db.close()
        self.tmp.cleanup()

    async def test_reconnect_reuses_client_and_restores_authorization_and_identity(self):
        await self.runtime.connect()
        self.runtime.me = None
        self.runtime.status = 'not_connected'
        self.client.connected = False
        await self.runtime.connect()
        self.assertIs(self.runtime.client, self.client)
        self.assertIs(self.runtime.me, self.client.identity)
        self.assertEqual(self.runtime.status, 'connected')
        self.assertEqual(self.client.connect_calls, 2)
        self.assertEqual(self.client.authorization_calls, 2)
        self.assertEqual(self.client.me_calls, 2)
        self.assertEqual(len(self.client.handlers), 3)
        self.factory.assert_called_once()

    async def test_authorization_failure_clears_stale_identity(self):
        await self.runtime.connect()
        self.client.authorized = False
        await self.runtime.connect()
        self.assertIsNone(self.runtime.me)
        self.assertEqual(self.runtime.status, 'not_connected')

    async def assert_scan_retry_recovers(self, phase):
        setattr(self.client, phase, True)
        first_pause = asyncio.Event()
        next_attempt = asyncio.Event()
        recovered = asyncio.Event()
        original_sleep = asyncio.sleep

        async def pause(delay):
            if delay == 30:
                first_pause.set()
                await next_attempt.wait()
            else:
                await original_sleep(delay)

        async def scan():
            if self.runtime.me:
                recovered.set()
                await asyncio.Event().wait()

        with patch.object(self.app, 'CONNECT_TIMEOUT', .02), \
                patch.object(self.app.asyncio, 'sleep', side_effect=pause), \
                patch.object(self.runtime, 'scan', side_effect=scan):
            task = asyncio.create_task(self.runtime.scan_loop())
            try:
                await asyncio.wait_for(first_pause.wait(), .3)
                self.assertIsNone(self.runtime.me)
                self.assertIsNotNone(self.runtime.error)
                setattr(self.client, phase, False)
                next_attempt.set()
                await asyncio.wait_for(recovered.wait(), .3)
                self.assertIs(self.runtime.me, self.client.identity)
                self.assertIsNone(self.runtime.error)
                self.assertEqual(self.client.connect_calls, 2 if phase == 'hang_connect' else 1)
                self.assertEqual(self.client.authorization_calls, 1 if phase == 'hang_connect' else 2)
                self.assertEqual(len(self.client.handlers), 3)
                self.factory.assert_called_once()
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_scan_retry_recovers_after_transport_timeout(self):
        await self.assert_scan_retry_recovers('hang_connect')

    async def test_scan_retry_recovers_same_partial_client_after_authorization_timeout(self):
        await self.assert_scan_retry_recovers('hang_authorized')

    async def test_scan_retry_recovers_identity_after_get_me_timeout(self):
        await self.assert_scan_retry_recovers('hang_identity')

    async def test_background_retry_does_not_interleave_with_login_or_replace_challenge(self):
        self.runtime.phone = '+15550000000'
        self.runtime.code_hash = 'existing-challenge'
        self.runtime.status = 'password_required'
        self.runtime.auth_until = time.time() + 600
        paused = asyncio.Event()

        async def pause(delay):
            paused.set()
            await asyncio.Event().wait()

        async with self.runtime.auth_lock:
            with patch.object(self.app.asyncio, 'sleep', side_effect=pause):
                task = asyncio.create_task(self.runtime.scan_loop())
                try:
                    await asyncio.wait_for(paused.wait(), .3)
                    self.factory.assert_not_called()
                    self.assertEqual(self.runtime.code_hash, 'existing-challenge')
                    self.assertEqual(self.runtime.status, 'password_required')
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def test_background_retry_leaves_unlocked_active_login_challenge_untouched(self):
        self.runtime.status = 'code_required'
        self.runtime.phone = '+15550000000'
        self.runtime.code_hash = 'existing-challenge'
        self.runtime.auth_until = time.time() + 600
        paused = asyncio.Event()

        async def pause(delay):
            paused.set()
            await asyncio.Event().wait()

        with patch.object(self.app.asyncio, 'sleep', side_effect=pause):
            task = asyncio.create_task(self.runtime.scan_loop())
            try:
                await asyncio.wait_for(paused.wait(), .3)
                self.factory.assert_not_called()
                self.assertEqual(self.runtime.code_hash, 'existing-challenge')
                self.assertEqual(self.runtime.status, 'code_required')
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_http_lifespan_opens_while_connect_hangs_and_shutdown_cancels_it(self):
        self.client.hang_connect = True
        services = {name: IdleService() for name in (
            'bot_notifications', 'operations', 'marketing', 'conversations',
            'publisher', 'directory', 'history')}
        probe = FastAPI(lifespan=self.app.lifespan)

        @probe.get('/ready')
        async def ready():
            return {'http': True}

        with contextlib.ExitStack() as stack:
            for name, service in services.items():
                stack.enter_context(patch.object(self.app, name, service))
            stack.enter_context(patch.object(self.app, 'seed_topics'))
            stack.enter_context(patch.object(self.app, 'bind_outbound_controls'))
            async with asyncio.timeout(.5):
                async with self.app.lifespan(probe):
                    await self.client.connect_started.wait()
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=probe), base_url='http://test') as client:
                        response = await client.get('/ready')
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json(), {'http': True})
                    self.assertIsNone(self.runtime.me)
                self.assertTrue(self.client.connect_cancelled.is_set())
                self.assertFalse(self.client.is_connected())
                self.assertTrue(all(task.done() for task in self.runtime.tasks))


if __name__ == '__main__':
    unittest.main()
