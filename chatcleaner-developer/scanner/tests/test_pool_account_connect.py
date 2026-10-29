"""Pool-account readiness recovery; no network, real sessions, or outbound work."""
import asyncio
import ast
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import operations
from fastapi import HTTPException
from telethon import types


class Store:
    def __init__(self):
        self.db=sqlite3.connect(':memory:');self.db.row_factory=sqlite3.Row
        self.db.executescript('CREATE TABLE op_accounts(id TEXT,telegram_id INTEGER,username TEXT,enabled INTEGER,error TEXT);CREATE TABLE op_jobs(id INTEGER,account TEXT,state TEXT,updated TEXT);CREATE TABLE op_items(id INTEGER,job INTEGER,state TEXT)')
        self.db.execute("INSERT INTO op_accounts VALUES ('pool',100,'synthetic',1,NULL)")
        self.db.executemany("INSERT INTO op_jobs VALUES (?,'pool','scheduled',NULL)",[(i,) for i in range(38)])
        self.db.executemany("INSERT INTO op_items VALUES (?,0,'pending')",[(i,) for i in range(120)])
        self.config={'account_api_pool':{'id':2040,'hash':'synthetic'},'account_profile_pool':'tdesktop'}
    def get(self,key,default=None):return self.config.get(key,default)
    def rows(self,sql,args=()):return [dict(r) for r in self.db.execute(sql,args)]


class Client:
    def __init__(self,connected=True):
        self.connected=connected;self.handlers=('keep-existing-handlers',)
        async def connect():self.connected=True
        async def disconnect():self.connected=False
        self.connect=AsyncMock(side_effect=connect);self.disconnect=AsyncMock(side_effect=disconnect)
        self.is_user_authorized=AsyncMock(return_value=True)
        self.get_me=AsyncMock(return_value=types.User(id=100,username='synthetic'))
        self.log_out=AsyncMock(side_effect=AssertionError('Logout is forbidden'))
    def is_connected(self):return self.connected


class PoolConnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store();self.addCleanup(self.store.db.close)
        self.proxy=dict(proxy_type='socks5',addr='synthetic-proxy',port=13620)
        self.client=Client()
        self.ops=SimpleNamespace(data=Path(self.tmp.name),store=self.store,get_api=lambda:None,
            make_client=Mock(return_value=self.client),proxy_connection=Mock(return_value=self.proxy),
            account_row=lambda aid:self.store.rows('SELECT * FROM op_accounts WHERE id=?',(aid,))[0])
        self.a=operations.PoolAccount(self.ops,'pool')
        self.jobs=self.store.rows('SELECT * FROM op_jobs ORDER BY id')
        self.items=self.store.rows('SELECT * FROM op_items ORDER BY id')
        self.session_bytes=b'synthetic-session-must-stay'
        Path(self.a.session_path).write_bytes(self.session_bytes)
        self.original_wait_for=asyncio.wait_for

    def bounded(self):
        calls=[]
        async def wait_for(coro,timeout):
            calls.append(timeout)
            return await self.original_wait_for(coro,0.01 if timeout==20 else 0.05)
        return calls,patch.object(operations.asyncio,'wait_for',wait_for)

    async def test_connected_transport_without_identity_finishes_readiness(self):
        self.a.client=self.client
        await self.a.connect()
        self.assertEqual(self.a.me.id,100);self.assertEqual(self.a.status,'connected')
        self.client.connect.assert_not_called();self.client.is_user_authorized.assert_awaited_once();self.client.get_me.assert_awaited_once()
        self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)

    async def test_already_ready_does_not_make_rpc_or_client(self):
        self.a.client=self.client;self.a.me=types.User(id=100);self.a.status='connected'
        await self.a.connect()
        self.ops.make_client.assert_not_called();self.client.is_user_authorized.assert_not_called();self.client.get_me.assert_not_called()

    async def test_timeout_closes_only_transport_preserves_challenge_and_retry_recovers(self):
        self.a.client=self.client;self.a.status='password_required';self.a.phone='+10000000000';self.a.code_hash='synthetic-challenge';self.a.auth_until=time.time()+600
        async def stuck():await asyncio.Event().wait()
        self.client.is_user_authorized.side_effect=stuck
        calls,bound=self.bounded()
        with bound:
            with self.assertRaises(asyncio.TimeoutError):await self.a.connect()
        self.assertEqual(calls,[20,5]);self.assertIs(self.a.client,self.client);self.assertIsNone(self.a.me)
        self.assertEqual(self.a.status,'password_required');self.assertEqual(self.a.code_hash,'synthetic-challenge')
        self.assertEqual(self.client.handlers,('keep-existing-handlers',));self.client.disconnect.assert_awaited_once();self.client.log_out.assert_not_called()
        self.assertEqual(Path(self.a.session_path).read_bytes(),self.session_bytes);self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)
        self.assertEqual(len(self.items),120);self.assertEqual(self.store.rows('SELECT * FROM op_items ORDER BY id'),self.items)
        self.client.is_user_authorized.side_effect=None
        await self.a.connect()
        self.assertEqual(self.a.me.id,100);self.client.connect.assert_awaited_once();self.ops.make_client.assert_not_called()

    async def test_get_me_is_inside_whole_twenty_second_deadline(self):
        self.a.client=self.client
        async def stuck():await asyncio.Event().wait()
        self.client.get_me.side_effect=stuck
        calls,bound=self.bounded()
        with bound:
            with self.assertRaises(asyncio.TimeoutError):await self.a.connect()
        self.assertEqual(calls,[20,5]);self.assertIsNone(self.a.me);self.assertEqual(self.a.status,'not_connected')
        self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)

    async def test_partially_connected_transport_timeout_is_closed_and_session_kept(self):
        self.a.client=self.client;self.client.connected=False
        async def stuck_connect():
            self.client.connected=True
            await asyncio.Event().wait()
        self.client.connect.side_effect=stuck_connect
        calls,bound=self.bounded()
        with bound:
            with self.assertRaises(asyncio.TimeoutError):await self.a.connect()
        self.assertEqual(calls,[20,5]);self.client.disconnect.assert_awaited_once()
        self.assertFalse(self.client.is_connected());self.assertIs(self.a.client,self.client)
        self.assertEqual(Path(self.a.session_path).read_bytes(),self.session_bytes)
        self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)
        self.assertEqual(self.store.rows('SELECT * FROM op_items ORDER BY id'),self.items)

    async def test_factory_keeps_saved_profile_proxy_and_concurrent_calls_use_one_client(self):
        self.client.connected=False
        await asyncio.gather(self.a.connect(),self.a.connect())
        self.ops.make_client.assert_called_once_with(self.a.session_path,self.store.config['account_api_pool'],'tdesktop',self.proxy)
        self.ops.proxy_connection.assert_called_once_with('pool');self.client.connect.assert_awaited_once()
        self.client.is_user_authorized.assert_awaited_once();self.assertEqual(self.a.me.id,100)

    async def test_failed_identity_still_rejects_and_uses_existing_ready_guard(self):
        self.a.client=self.client;self.client.get_me.return_value=types.User(id=101,username='wrong')
        with self.assertRaises(HTTPException):await self.a.connect()
        self.assertIsNone(self.a.client);self.assertIsNone(self.a.me)
        self.assertTrue(all(j['state']=='paused' for j in self.store.rows('SELECT * FROM op_jobs')))
        self.client.log_out.assert_not_called()

    async def test_unauthorized_connect_does_not_pause_jobs_or_delete_session(self):
        self.a.client=self.client;self.client.is_user_authorized.return_value=False
        await self.a.connect()
        self.assertIsNone(self.a.me);self.assertIs(self.a.client,self.client)
        self.assertEqual(self.store.rows('SELECT * FROM op_jobs ORDER BY id'),self.jobs)
        self.client.disconnect.assert_not_called();self.client.log_out.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
