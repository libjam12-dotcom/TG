"""Replay the observed AddUser/member-absent contradiction without Telegram access."""
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
import unittest

from telethon import errors, functions, types
from telethon.tl.custom.participantpermissions import ParticipantPermissions

import test_operations as fixtures
from operations import invite_response_evidence


STAMP = datetime(2026, 10, 21, 7, 54, 57, tzinfo=timezone.utc)
TARGET = -1000000000010


def service_update(*, peer=None, users=None, actor_id=100):
    return types.UpdateNewChannelMessage(
        message=types.MessageService(
            id=4, peer_id=peer or types.PeerChannel(10), date=STAMP,
            action=types.MessageActionChatAddUser(users=users or [200]),
            from_id=types.PeerUser(actor_id)), pts=1, pts_count=1)


def response(update=None):
    return types.messages.InvitedUsers(
        updates=types.Updates(updates=[update or service_update()], users=[], chats=[], date=STAMP, seq=0),
        missing_invitees=[])


class InviteServiceMembershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.f = fixtures.OperationsTests()
        await self.f.asyncSetUp()

    async def asyncTearDown(self):
        await self.f.asyncTearDown()

    async def replay(self, permissions):
        job = await self.f.draft('invite')
        await self.f.start(job)
        client = self.f.c
        client.get_permissions = AsyncMock(side_effect=permissions)
        original = type(client).__call__

        async def invoke(current, request):
            if isinstance(request, functions.channels.InviteToChannelRequest):
                current.calls.append(request)
                return response()
            return await original(current, request)

        with patch.object(type(client), '__call__', new=invoke), patch('operations.asyncio.sleep', new_callable=AsyncMock):
            await self.f.ops.tick()
        return await self.f.ops.detail(job['id'])

    async def test_add_user_event_with_three_absent_reads_is_conflict_not_success_or_retry(self):
        job = await self.replay(errors.UserNotParticipantError(None))
        item = job['items'][0]
        self.assertEqual(item['state'], 'uncertain')
        self.assertEqual(job['state'], 'paused')
        self.assertEqual(item['diagnosis']['code'], 'invite_service_membership_conflict')
        self.assertFalse(item['diagnosis']['retry_after_change'])
        self.assertEqual([v['member'] for v in item['diagnosis']['verification']], [False] * 3)
        self.assertEqual(item['diagnosis']['response']['add_user_events'], [{
            'message_id': 4, 'chat_id': TARGET, 'user_id': 200,
            'actor_id': 100, 'date': STAMP.isoformat()}])
        self.assertEqual(len(self.f.c.calls), 1)
        self.assertEqual(self.f.c.get_permissions.await_count, 4)
        self.f.clear_wait()
        await self.f.ops.tick()
        self.assertEqual(len(self.f.c.calls), 1)
        marker = self.f.ops.store.rows('SELECT state FROM op_invited WHERE chat_id=? AND user_id=?', (TARGET, 200))
        self.assertEqual(marker, [{'state': 'uncertain'}])

    async def test_add_user_event_only_becomes_invited_when_membership_is_confirmed(self):
        member = ParticipantPermissions(types.ChannelParticipant(user_id=200, date=STAMP), False)
        job = await self.replay([errors.UserNotParticipantError(None), member])
        item = job['items'][0]
        self.assertEqual(item['state'], 'invited')
        self.assertEqual(item['diagnosis']['code'], 'invite_member_verified')
        self.assertEqual(item['diagnosis']['response']['add_user_events'][0]['user_id'], 200)
        self.assertEqual(len(self.f.c.calls), 1)

    async def test_service_event_with_unknown_check_keeps_generic_unconfirmed(self):
        job = await self.replay([errors.UserNotParticipantError(None), TimeoutError(),
                                 errors.UserNotParticipantError(None), errors.UserNotParticipantError(None)])
        item = job['items'][0]
        self.assertEqual(item['state'], 'uncertain')
        self.assertEqual(item['diagnosis']['code'], 'invite_membership_unconfirmed')
        self.assertEqual([v['member'] for v in item['diagnosis']['verification']], [None, False, False])
        self.assertEqual(job['state'], 'paused')
        self.assertEqual(len(self.f.c.calls), 1)

    def test_service_evidence_is_scoped_to_expected_user_and_group(self):
        for update in (service_update(peer=types.PeerChannel(11)), service_update(users=[201]), service_update(actor_id=101)):
            with self.subTest(update=update):
                evidence = invite_response_evidence(response(update), target_id=TARGET, user_id=200, sender_id=100)
                self.assertEqual(evidence['add_user_events'], [])
        self.assertEqual(invite_response_evidence(response())['add_user_events'], [])

    def test_direct_updates_and_short_updates_keep_service_metadata(self):
        containers = [
            types.Updates(updates=[service_update()], users=[], chats=[], date=STAMP, seq=0),
            types.UpdatesCombined(updates=[service_update()], users=[], chats=[], date=STAMP, seq_start=0, seq=0),
            types.UpdateShort(update=service_update(), date=STAMP)]
        for container in containers:
            with self.subTest(container=type(container).__name__):
                evidence = invite_response_evidence(container, target_id=TARGET, user_id=200)
                self.assertEqual(evidence['updates_type'], type(container).__name__)
                self.assertEqual(evidence['add_user_events'][0]['user_id'], 200)

    def test_service_evidence_excludes_message_text_entities_and_auth_data(self):
        result = response()
        result.updates.updates.append(types.UpdateNewMessage(
            message=types.Message(id=5, peer_id=types.PeerUser(200), message='private-text-secret'), pts=2, pts_count=1))
        result.updates.users.append(types.User(id=200, first_name='private-name-secret', phone='private-phone-secret', access_hash=991234567))
        encoded = json.dumps(invite_response_evidence(result, target_id=TARGET, user_id=200))
        for secret in ('private-text-secret', 'private-name-secret', 'private-phone-secret', '991234567', 'access_hash'):
            self.assertNotIn(secret, encoded)
