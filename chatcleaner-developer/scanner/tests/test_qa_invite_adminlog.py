"""Offline QA130 log reads: narrow scope, metadata redaction, no writes/auth."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import APIRouter, FastAPI, HTTPException
import httpx
from telethon import errors, functions, types
from qa_invite_adminlog import (QaInviteAdminLog, QA_ACCOUNT, QA_SENDER, QA_TARGET,
                               QA_RECIPIENT, QA_SCOPE, QA_SINCE, recipient_event)


STAMP = datetime(2026, 10, 21, 8, 0, tzinfo=timezone.utc)


def member(uid=QA_RECIPIENT):
    return types.ChannelParticipant(user_id=uid, date=STAMP)


def event(eid, action, actor=QA_SENDER, date=STAMP):
    return types.ChannelAdminLogEvent(id=eid, date=date, user_id=actor, action=action)


class ReadClient:
    def __init__(self):
        self.connected = True
        self.get_me = AsyncMock(return_value=types.User(id=QA_SENDER, username='demo_identity_2'))
        self.channel = types.Channel(id=720000002, title='private-channel-title',
            photo=types.ChatPhotoEmpty(), date=STAMP, megagroup=True, creator=True)
        self.get_entity = AsyncMock(return_value=self.channel)
        self.calls = []
        self.result = types.channels.AdminLogResults(events=[], chats=[], users=[])
        self.hook = None
        self.connect = AsyncMock(); self.disconnect = AsyncMock(); self.log_out = AsyncMock()
        self.sign_in = AsyncMock(); self.send_code_request = AsyncMock(); self.send_message = AsyncMock()

    def is_connected(self):
        return self.connected

    async def __call__(self, request):
        self.calls.append(request)
        if not isinstance(request, functions.channels.GetAdminLogRequest):
            raise AssertionError('Only GetAdminLog is permitted')
        if self.hook:
            await self.hook()
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class QaInviteAdminLogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = ReadClient()
        self.account = SimpleNamespace(client=self.client, me=types.User(id=QA_SENDER, username='demo_identity_2'))
        self.row = {'telegram_id': QA_SENDER, 'enabled': 0, 'not_before': 0, 'error': 'preserved pause'}
        self.business = {'jobs': {130: 'paused'}, 'primary_enabled': 0, 'second_enabled': 0,
                         'attempts': 19, 'pending_count': 112}
        self.before = copy.deepcopy((self.row, self.business))
        self.ops = SimpleNamespace(accounts={QA_ACCOUNT: self.account},
            account_row=Mock(side_effect=lambda aid: self.row if aid == QA_ACCOUNT else (_ for _ in ()).throw(AssertionError('Wrong account'))),
            auth=Mock(side_effect=AssertionError('No lazy auth')), online=Mock(side_effect=AssertionError('No outbound guard')),
            readable_account=Mock(side_effect=AssertionError('No lazy reader')), make_client=Mock(side_effect=AssertionError('No client creation')))
        self.diag = QaInviteAdminLog(self.ops)

    async def asyncTearDown(self):
        self.assertEqual((self.row, self.business), self.before)
        for method in ('connect', 'disconnect', 'log_out', 'sign_in', 'send_code_request', 'send_message'):
            getattr(self.client, method).assert_not_awaited()
        for method in ('auth', 'online', 'readable_account', 'make_client'):
            getattr(self.ops, method).assert_not_called()

    async def test_fixed_scope_single_read_filters_subject_not_actor_and_redacts_entities(self):
        invite = types.ChatInviteExported(link='https://t.me/+DEVELOPMENT_ONLY', admin_id=1234, date=STAMP)
        self.client.result = types.channels.AdminLogResults(events=[
            event(1, types.ChannelAdminLogEventActionParticipantInvite(member()), actor=987654321),
            event(2, types.ChannelAdminLogEventActionParticipantInvite(member(1234567)), actor=QA_RECIPIENT),
            event(3, types.ChannelAdminLogEventActionParticipantLeave(), actor=QA_RECIPIENT),
            event(4, types.ChannelAdminLogEventActionParticipantJoinByInvite(invite), actor=QA_RECIPIENT),
            event(5, types.ChannelAdminLogEventActionChangeTitle('private-old-title', 'private-new-title'), actor=QA_RECIPIENT),
            event(6, types.ChannelAdminLogEventActionParticipantJoin(), actor=QA_RECIPIENT, date=QA_SINCE-timedelta(seconds=1))],
            chats=[self.client.channel], users=[types.User(id=1234567, first_name='private-name-secret', phone='private-phone-secret', access_hash=9988776655)])
        response = await self.diag.read(QA_SCOPE)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        data = json.loads(response.body)
        self.assertEqual([e['event_id'] for e in data['events']], [1, 3, 4])
        self.assertEqual(data['events'][0]['actor_id'], 987654321)
        self.assertEqual(data['sender_id'], QA_SENDER); self.assertEqual(data['target_id'], QA_TARGET)
        self.assertEqual(data['recipient_id'], QA_RECIPIENT)
        self.assertFalse(data['membership_evaluated']); self.assertFalse(data['eligibility_evaluated'])
        self.assertFalse(data['history_changed']); self.assertFalse(data['invitation_requested'])
        self.assertTrue(data['limited_window']); self.assertTrue(data['read_only'])
        self.client.get_me.assert_awaited_once(); self.client.get_entity.assert_awaited_once_with(QA_TARGET)
        self.assertEqual(len(self.client.calls), 1)
        req = self.client.calls[0]
        self.assertIsInstance(req, functions.channels.GetAdminLogRequest)
        self.assertEqual((req.limit, req.q, req.max_id, req.min_id), (50, '', 0, 0))
        self.assertIsNone(req.admins)
        for flag in ('invite', 'join', 'leave', 'kick', 'unkick', 'ban', 'unban'):
            self.assertIs(getattr(req.events_filter, flag), True)
        for flag in ('edit', 'delete', 'info', 'settings', 'send'):
            self.assertIsNone(getattr(req.events_filter, flag))
        encoded = response.body.decode()
        for secret in ('1234567', 'private-link-secret', 'private-name-secret', 'private-phone-secret',
                       'private-channel-title', 'private-old-title', 'private-new-title', '9988776655', 'access_hash'):
            self.assertNotIn(secret, encoded)

    def test_ban_subject_is_matched_by_participant_and_minimal_state_only(self):
        banned = types.ChannelParticipantBanned(peer=types.PeerUser(QA_RECIPIENT), kicked_by=1234567,
            date=STAMP, banned_rights=types.ChatBannedRights(until_date=STAMP, view_messages=True), left=True)
        row = recipient_event(event(7, types.ChannelAdminLogEventActionParticipantToggleBan(member(), banned), actor=7654321))
        self.assertEqual(row['recipient_id'], QA_RECIPIENT); self.assertTrue(row['left_after'])
        self.assertNotIn('1234567', json.dumps(row)); self.assertNotIn('banned_rights', json.dumps(row))
        bad = types.ChannelAdminLogEventActionParticipantToggleBan(member(999), banned)
        self.assertIsNone(recipient_event(event(8, bad)))

    async def test_response_processing_is_limited_to_first_fifty_events(self):
        self.client.result = types.channels.AdminLogResults(events=[
            event(i, types.ChannelAdminLogEventActionParticipantLeave(), actor=QA_RECIPIENT) for i in range(1, 61)], chats=[], users=[])
        data = json.loads((await self.diag.read(QA_SCOPE)).body)
        self.assertEqual(data['scanned_event_count'], 50); self.assertEqual(len(data['events']), 50)

    async def test_missing_or_disconnected_runtime_never_connects_or_reads(self):
        for state in ('missing', 'disconnected'):
            with self.subTest(state=state):
                self.diag.next_read = 0
                self.ops.accounts = {} if state == 'missing' else {QA_ACCOUNT: self.account}
                self.client.connected = state != 'disconnected'
                with self.assertRaises(HTTPException) as raised:
                    await self.diag.read(QA_SCOPE)
                self.assertEqual(raised.exception.status_code, 409)
        self.client.get_me.assert_not_awaited(); self.client.get_entity.assert_not_awaited()
        self.assertEqual(self.client.calls, [])

    async def test_cached_and_fresh_identity_are_both_pinned(self):
        self.account.me = types.User(id=42, username='demo_identity_2')
        with self.assertRaises(HTTPException): await self.diag.read(QA_SCOPE)
        self.client.get_me.assert_not_awaited()
        self.account.me = types.User(id=QA_SENDER)
        self.client.get_me.return_value = types.User(id=42)
        with self.assertRaises(HTTPException): await self.diag.read(QA_SCOPE)
        self.client.get_entity.assert_not_awaited(); self.assertEqual(self.client.calls, [])

    async def test_runtime_replacement_during_get_me_prevents_adminlog(self):
        async def replace():
            self.ops.accounts = {}
            return types.User(id=QA_SENDER)
        self.client.get_me.side_effect = replace
        with self.assertRaises(HTTPException): await self.diag.read(QA_SCOPE)
        self.client.get_entity.assert_not_awaited(); self.assertEqual(self.client.calls, [])

    async def test_client_change_after_adminlog_discards_result(self):
        async def replace(): self.account.client = ReadClient()
        self.client.hook = replace
        with self.assertRaises(HTTPException): await self.diag.read(QA_SCOPE)
        self.assertEqual(len(self.client.calls), 1)

    async def test_wrong_group_or_missing_admin_rights_never_reads_log(self):
        for channel in [types.Channel(id=42, title='wrong', photo=types.ChatPhotoEmpty(), date=STAMP, megagroup=True, creator=True),
                        types.Channel(id=720000002, title='no rights', photo=types.ChatPhotoEmpty(), date=STAMP, megagroup=True)]:
            with self.subTest(channel=channel.id):
                self.diag.next_read = 0; self.client.get_entity.return_value = channel
                with self.assertRaises(HTTPException): await self.diag.read(QA_SCOPE)
        self.assertEqual(self.client.calls, [])

    async def test_cooldown_does_not_repeat_rpc_or_change_account_controls(self):
        await self.diag.read(QA_SCOPE)
        with self.assertRaises(HTTPException) as raised: await self.diag.read(QA_SCOPE)
        self.assertEqual(raised.exception.status_code, 429)
        self.assertGreaterEqual(int(raised.exception.headers['Retry-After']), 59)
        self.assertEqual(len(self.client.calls), 1); self.client.get_me.assert_awaited_once()

    async def test_errors_never_retry_auth_or_change_business_state(self):
        for err, status in [(errors.FloodWaitError(None, capture=120), 429),
                            (errors.PeerFloodError(None), 409), (errors.SessionRevokedError(None), 409),
                            (errors.ChatAdminRequiredError(None), 403), (RuntimeError('private-error-secret'), 502)]:
            with self.subTest(error=type(err).__name__):
                self.diag.next_read = 0; self.client.calls = []; self.client.result = err
                with self.assertRaises(HTTPException) as raised: await self.diag.read(QA_SCOPE)
                self.assertEqual(raised.exception.status_code, status)
                self.assertEqual(raised.exception.headers['Cache-Control'], 'no-store')
                self.assertNotIn('private-error-secret', raised.exception.detail)
                self.assertEqual(len(self.client.calls), 1)

    async def test_timeout_bounds_the_complete_read(self):
        async def stuck(): await asyncio.Event().wait()
        self.client.get_me.side_effect = stuck
        with patch('qa_invite_adminlog.READ_TIMEOUT', .02):
            with self.assertRaises(HTTPException) as raised: await self.diag.read(QA_SCOPE)
        self.assertEqual(raised.exception.status_code, 504)
        self.assertEqual(self.client.calls, []); self.client.get_entity.assert_not_awaited()

    async def test_router_guard_required_scope_health_and_fixed_ids(self):
        app = FastAPI(); outer = APIRouter(prefix='/api/scanner/operations')
        outer.include_router(self.diag.router); app.include_router(outer)
        url = '/api/scanner/operations/qa/invite-adminlog'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            for headers in ({}, {'x-panel-user': 'viewer', 'x-panel-role': 'viewer'}):
                r = await client.get(url, params={'scope': QA_SCOPE}, headers=headers)
                self.assertEqual(r.status_code, 403)
            for role in ('owner', 'admin'):
                headers = {'x-panel-user': 'tester', 'x-panel-role': role}
                r = await client.get(url, headers=headers)
                self.assertEqual(r.status_code, 422)
                r = await client.get(url, params={'scope': 'wrong'}, headers=headers)
                self.assertEqual(r.status_code, 422)
            self.client.get_me.assert_not_awaited(); self.assertEqual(self.client.calls, [])
            r = await client.get(url, params={'scope': QA_SCOPE, 'account': 'primary', 'target_id': -42, 'recipient_id': 42},
                                 headers={'x-panel-user': 'owner', 'x-panel-role': 'owner'})
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()['target_id'], QA_TARGET); self.assertEqual(r.json()['recipient_id'], QA_RECIPIENT)
            self.assertEqual(len(self.client.calls), 1)


if __name__ == '__main__':
    unittest.main()
