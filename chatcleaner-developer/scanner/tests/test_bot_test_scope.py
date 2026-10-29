"""Synthetic temporary-DB regression: a test receipt must stay bound to its destination."""
import importlib.util
import os
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scanner'))
import bot_notifications
spec=importlib.util.spec_from_file_location('acceptance_legacy_bot_tests',ROOT/'scanner/tests/test_bot_notifications.py')
legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy)
from bot_notifications import Test
from fastapi import HTTPException

class TestReceiptScope(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=legacy.NotificationTests.asyncSetUp
    asyncTearDown=legacy.NotificationTests.asyncTearDown
    configure=legacy.NotificationTests.configure

    async def check_changed_scope(self,change):
        self.forum=True
        await self.configure(thread_id=44)
        key='bound-test-receipt-key'
        await self.bot.test(Test(request_key=key,confirmed=True))
        old=dict(self.store.rows('SELECT * FROM bot_deliveries')[0])
        if change=='chat':self.chat_id=-100998
        await self.configure(thread_id=55 if change=='topic' else 44)
        cfg=self.bot.config()
        with self.assertRaises(HTTPException) as rejected:
            await self.bot.test(Test(request_key=key,confirmed=True,chat_id=cfg['chat_id'],revision=cfg['revision']))
        self.assertEqual(rejected.exception.status_code,409)
        self.assertEqual(sum(method=='sendMessage' for method,_ in self.calls),1)
        self.assertEqual(dict(self.store.rows('SELECT * FROM bot_deliveries')[0]),old)

    async def test_same_key_after_chat_change_rejects_old_receipt(self):
        await self.check_changed_scope('chat')

    async def test_same_key_after_topic_change_rejects_old_receipt(self):
        await self.check_changed_scope('topic')

if __name__=='__main__':unittest.main()
