import json
from types import SimpleNamespace
from unittest.mock import Mock
from fastapi import HTTPException
from telethon import types
from test_qa_invite_adminlog import QaInviteAdminLogTests, QA_ACCOUNT, QA_SENDER, QA_TARGET, QA_RECIPIENT, STAMP, event, member

class SavedItemTests(QaInviteAdminLogTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.binding=dict(id=12,job=8,chat_id=QA_TARGET,user_id=QA_RECIPIENT,attempted_at=STAMP.isoformat(),account=QA_ACCOUNT,telegram_id=QA_SENDER,kind='invite')
        self.ops.store=SimpleNamespace(rows=Mock(side_effect=lambda sql,params:[dict(self.binding)] if params==(8,12) else []))
    async def test_saved_item_returns_only_bound_subject_without_writes(self):
        self.client.result=types.channels.AdminLogResults(events=[event(111,types.ChannelAdminLogEventActionParticipantInvite(member()))],users=[],chats=[])
        r=await self.diag.read_item(8,12);d=json.loads(r.body)
        self.assertEqual((d['source_job_id'],d['source_item_id'],d['recipient_id']),(8,12,QA_RECIPIENT))
        self.assertNotIn('qa_job_id',d);self.assertEqual(len(d['events']),1)
        self.assertFalse(d['invitation_requested']);self.assertFalse(d['history_changed']);self.assertTrue(d['limited_window'])
        self.assertEqual(len(self.client.calls),1)
    async def test_missing_item_and_noattempt_rejected_before_telegram(self):
        for changed in ({'kind':'post'},{'attempted_at':None},{'user_id':None}):
            original=dict(self.binding);self.binding.update(changed)
            with self.assertRaises(HTTPException):await self.diag.read_item(8,12)
            self.binding=original
        with self.assertRaises(HTTPException):await self.diag.read_item(8,99)
        self.assertEqual(self.client.calls,[]);self.client.get_me.assert_not_awaited()
    async def test_binding_change_during_read_discards_response(self):
        async def change():self.binding['user_id']+=1
        self.client.hook=change
        with self.assertRaises(HTTPException) as e:await self.diag.read_item(8,12)
        self.assertEqual(e.exception.status_code,409)
    async def test_wrong_saved_sender_rejects_before_read(self):
        self.binding['telegram_id']+=1
        with self.assertRaises(HTTPException):await self.diag.read_item(8,12)
        self.assertEqual(self.client.calls,[])
