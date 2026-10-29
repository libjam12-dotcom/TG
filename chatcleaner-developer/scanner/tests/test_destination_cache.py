import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
import test_operations as fixtures
from fastapi import HTTPException


class DestinationCacheTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.OperationsTests.asyncSetUp
    asyncTearDown = fixtures.OperationsTests.asyncTearDown

    async def load(self):
        self.ops.load_destinations = AsyncMock(return_value={'destinations':[{'id':10,'title':'Group'}],'sender':'test'})
        return await self.ops.destinations(self.a.aid)

    async def test_repeat_is_cached_and_response_mutation_cannot_pollute_cache(self):
        first=await self.load();first['destinations'][0]['title']='changed'
        second=await self.ops.destinations(self.a.aid)
        self.assertFalse(first['cached']);self.assertTrue(second['cached'])
        self.assertEqual(second['destinations'][0]['title'],'Group')
        self.ops.load_destinations.assert_awaited_once()

    async def test_concurrent_picker_requests_share_one_telegram_read(self):
        async def slow(aid):
            await asyncio.sleep(.03)
            return {'destinations':[]}
        self.ops.load_destinations=AsyncMock(side_effect=slow)
        results=await asyncio.gather(*(self.ops.destinations(self.a.aid) for _ in range(20)))
        self.ops.load_destinations.assert_awaited_once()
        self.assertEqual(sum(not r['cached'] for r in results),1)

    async def test_expiry_and_explicit_refresh(self):
        await self.load()
        self.ops.destination_cache[self.a.aid]['at']-=301
        self.assertFalse((await self.ops.destinations(self.a.aid))['cached'])
        self.assertFalse((await self.ops.destinations(self.a.aid,refresh=True))['cached'])
        self.assertEqual(self.ops.load_destinations.await_count,3)

    async def test_disabled_account_cannot_read_cached_list(self):
        await self.load()
        with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET enabled=0 WHERE id=?',(self.a.aid,))
        with self.assertRaises(HTTPException):await self.ops.destinations(self.a.aid)

    async def test_new_client_cannot_reuse_old_list(self):
        await self.load();self.a.client=fixtures.Client(self.group)
        self.assertFalse((await self.ops.destinations(self.a.aid))['cached'])
        self.assertEqual(self.ops.load_destinations.await_count,2)

    async def test_failed_refresh_invalidates_cache(self):
        await self.load();self.ops.load_destinations.side_effect=TimeoutError
        with self.assertRaises(HTTPException) as caught:await self.ops.destinations(self.a.aid,refresh=True)
        self.assertEqual(caught.exception.status_code,504)
        self.assertNotIn(self.a.aid,self.ops.destination_cache)

    async def test_account_change_during_read_rejected(self):
        async def changed(aid):
            self.a.client=fixtures.Client(self.group)
            return {'destinations':[]}
        self.ops.load_destinations=AsyncMock(side_effect=changed)
        with self.assertRaises(HTTPException):await self.ops.destinations(self.a.aid)
        self.assertNotIn(self.a.aid,self.ops.destination_cache)

    async def test_primary_sync_does_not_write_unchanged_identity(self):
        self.ops.get_primary=lambda:None
        self.ops.accounts['primary']=SimpleNamespace(me=SimpleNamespace(id=500,username='primary'),pause=lambda:None)
        with self.ops.store.db:self.ops.store.db.execute("INSERT INTO op_accounts(id,label,created,telegram_id,username) VALUES('primary','Primary','now',500,'primary')")
        statements=[];self.ops.store.db.set_trace_callback(statements.append)
        for _ in range(10):self.ops.sync_primary()
        self.assertFalse(any(s.startswith('UPDATE') for s in statements))
        self.ops.accounts['primary'].me.username='renamed';self.ops.sync_primary()
        self.assertEqual(self.ops.account_row('primary')['username'],'renamed')
        self.ops.store.db.set_trace_callback(None);del self.ops.accounts['primary']
