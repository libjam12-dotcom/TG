"""Owner-only invite-privacy reads: no Telegram connection or outgoing actions."""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi import APIRouter, Depends, FastAPI, HTTPException
import httpx
from telethon import errors, functions, types
from engine import Store
from marketing import owner_only
from owner_sender import OwnerSender

OWNER=710000010
INVITER=710000016


class ReadClient:
    def __init__(self,privacy):
        self.connected=True
        self.get_me=AsyncMock(return_value=types.User(id=OWNER,username='demo_identity_3'))
        self.read_privacy=AsyncMock(return_value=privacy)
        self.connect=AsyncMock();self.log_out=AsyncMock();self.disconnect=AsyncMock()
        self.send_code_request=AsyncMock();self.sign_in=AsyncMock();self.send_message=AsyncMock()
    def is_connected(self):return self.connected
    async def __call__(self,request):return await self.read_privacy(request)


class OwnerInvitePrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'owner.sqlite3')
        self.store.put('conversation_config',{'paused':False,'auto_first':False,'auto_reply':False})
        self.sender=OwnerSender(Path(self.tmp.name)/'existing-owner',lambda:None,self.store)
        self.sender.status='connected';self.sender.me=types.User(id=OWNER,username='demo_identity_3')
        self.privacy=types.account.PrivacyRules(rules=[
            types.PrivacyValueDisallowUsers(users=[INVITER,1234567]),
            types.PrivacyValueAllowAll(),
            types.PrivacyValueAllowChatParticipants(chats=[7777777]),
            types.PrivacyValueAllowUsers(users=[1234567])],
            chats=[],users=[types.User(id=1234567,first_name='private-name-secret',phone='private-phone-secret',access_hash=9988776655)])
        self.client=ReadClient(self.privacy);self.sender.client=self.client
        self.auth_next=self.sender.auth_next
        self.settings=self.store.rows('SELECT * FROM settings ORDER BY key')

    async def asyncTearDown(self):
        self.assertEqual(self.sender.auth_next,self.auth_next)
        self.assertEqual(self.store.rows('SELECT * FROM settings ORDER BY key'),self.settings)
        for fn in ('connect','log_out','disconnect','send_code_request','sign_in','send_message'):
            getattr(self.client,fn).assert_not_awaited()
        self.store.db.close();self.tmp.cleanup()

    async def test_rules_keep_order_and_only_explicit_user_exception_matches(self):
        response=await self.sender.invite_privacy(INVITER)
        self.assertEqual(response.headers['cache-control'],'no-store')
        data=json.loads(response.body)
        self.assertEqual(data['account_id'],OWNER)
        self.assertEqual(data['match_scope'],'explicit_user_exceptions_only')
        self.assertFalse(data['eligibility_evaluated']);self.assertFalse(data['invitation_requested'])
        self.assertTrue(data['read_only'])
        self.assertEqual(data['rules'],[
            {'type':'PrivacyValueDisallowUsers','user_exception_count':2,'chat_exception_count':0,'explicit_user_exception_match':True},
            {'type':'PrivacyValueAllowAll','user_exception_count':0,'chat_exception_count':0,'explicit_user_exception_match':False},
            {'type':'PrivacyValueAllowChatParticipants','user_exception_count':0,'chat_exception_count':1,'explicit_user_exception_match':False},
            {'type':'PrivacyValueAllowUsers','user_exception_count':1,'chat_exception_count':0,'explicit_user_exception_match':False}])
        self.client.get_me.assert_awaited_once()
        self.client.read_privacy.assert_awaited_once()
        request=self.client.read_privacy.await_args.args[0]
        self.assertIsInstance(request,functions.account.GetPrivacyRequest)
        self.assertIsInstance(request.key,types.InputPrivacyKeyChatInvite)
        for secret in ('1234567','7777777','9988776655','private-name-secret','private-phone-secret','access_hash'):
            self.assertNotIn(secret,response.body.decode())

    async def test_second_read_obeys_transient_cooldown_without_another_rpc(self):
        await self.sender.invite_privacy(INVITER)
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,429)
        self.assertGreaterEqual(int(raised.exception.headers['Retry-After']),59)
        self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_awaited_once()

    async def test_wrong_current_identity_never_reads_or_logs_out(self):
        self.sender.me=types.User(id=42,username='demo_identity_3')
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_not_awaited();self.client.read_privacy.assert_not_awaited()

    async def test_wrong_fresh_identity_never_reads_privacy_or_logs_out(self):
        self.client.get_me.return_value=types.User(id=42,username='demo_identity_3')
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_not_awaited()

    async def test_wrong_fresh_username_never_reads_privacy(self):
        self.client.get_me.return_value=types.User(id=OWNER,username='another_profile')
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        self.client.read_privacy.assert_not_awaited()

    async def test_client_change_during_identity_read_stops_before_privacy(self):
        async def change():
            self.sender.client=ReadClient(self.privacy)
            return types.User(id=OWNER,username='demo_identity_3')
        self.client.get_me.side_effect=change
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        self.client.read_privacy.assert_not_awaited()

    async def test_client_change_during_privacy_read_discards_result(self):
        async def change(request):
            self.sender.client=ReadClient(self.privacy)
            return self.privacy
        self.client.read_privacy.side_effect=change
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_awaited_once()

    async def test_not_connected_does_not_create_or_connect_session(self):
        self.client.connected=False
        with patch('owner_sender.TelegramClient') as factory:
            with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,409)
        factory.assert_not_called()
        self.client.get_me.assert_not_awaited();self.client.read_privacy.assert_not_awaited()

    async def test_flood_wait_extends_diagnostic_cooldown_without_business_state_changes(self):
        self.client.read_privacy.side_effect=errors.FloodWaitError(None,capture=120)
        with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,429)
        self.assertGreaterEqual(int(raised.exception.headers['Retry-After']),120)
        with self.assertRaises(HTTPException):await self.sender.invite_privacy(INVITER)
        self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_awaited_once()
        self.assertEqual(self.sender.status,'connected')

    async def test_peer_flood_and_revoked_session_are_reported_without_retries_or_logout(self):
        for error in (errors.PeerFloodError(None),errors.SessionRevokedError(None)):
            with self.subTest(error=type(error).__name__):
                self.sender.invite_privacy_next=0
                self.client.get_me.reset_mock();self.client.read_privacy.reset_mock()
                self.client.read_privacy.side_effect=error
                with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
                self.assertEqual(raised.exception.status_code,409)
                self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_awaited_once()
                self.assertEqual(self.sender.status,'connected');self.assertEqual(self.sender.me.id,OWNER)

    async def test_total_timeout_bounds_identity_read_and_prevents_privacy_call(self):
        async def stuck():await asyncio.Event().wait()
        self.client.get_me.side_effect=stuck
        with patch('owner_sender.INVITE_PRIVACY_TIMEOUT',.02):
            with self.assertRaises(HTTPException) as raised:await self.sender.invite_privacy(INVITER)
        self.assertEqual(raised.exception.status_code,504)
        self.client.get_me.assert_awaited_once();self.client.read_privacy.assert_not_awaited()

    async def test_owner_only_and_positive_inviter_validation_are_inherited_by_router(self):
        app=FastAPI()
        router=APIRouter(prefix='/api/scanner/marketing',dependencies=[Depends(owner_only)])
        router.include_router(self.sender.router);app.include_router(router)
        url='/api/scanner/marketing/sender/invite-privacy'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            for headers in ({},{'x-panel-user':'demo','x-panel-role':'admin'}):
                result=await client.get(url,params={'inviter_id':INVITER},headers=headers)
                self.assertEqual(result.status_code,403)
            headers={'x-panel-user':'owner','x-panel-role':'owner'}
            result=await client.get(url,params={'inviter_id':0},headers=headers)
            self.assertEqual(result.status_code,422)
            self.client.get_me.assert_not_awaited()
            result=await client.get(url,params={'inviter_id':INVITER},headers=headers)
            self.assertEqual(result.status_code,200)
            self.assertFalse(result.json()['eligibility_evaluated'])
