"""Offline regressions for the method-scoped, closed RPC enum allowlist."""
import json
import unittest

from telethon import errors, functions, types
from operations import safe_rpc_metadata
import test_operations as fixtures


class InviteRPCEnumAllowlistTests(unittest.TestCase):
    def request(self):
        return functions.channels.InviteToChannelRequest(
            types.InputChannel(10, 99123456789), [types.InputUser(200, 88123456789)])

    def test_official_unmapped_invite_bad_request_enum_is_retained(self):
        self.assertNotIn('CHAT_MEMBER_ADD_FAILED', errors.rpc_errors_dict)
        result = safe_rpc_metadata(errors.BadRequestError(self.request(), 'CHAT_MEMBER_ADD_FAILED', code=400))
        self.assertEqual(result, {
            'request_constructor': 'InviteToChannelRequest',
            'exception_type': 'BadRequestError', 'code': 400, 'message': 'CHAT_MEMBER_ADD_FAILED'})

    def test_official_invite_monoforum_enum_is_retained(self):
        result = safe_rpc_metadata(errors.BadRequestError(self.request(), 'CHANNEL_MONOFORUM_UNSUPPORTED', code=400))
        self.assertEqual(result['message'], 'CHANNEL_MONOFORUM_UNSUPPORTED')

    def test_added_enums_require_the_exact_invite_constructor_and_400_code(self):
        other = functions.users.GetUsersRequest([types.InputUser(200, 88123456789)])
        for enum in ('CHAT_MEMBER_ADD_FAILED', 'CHANNEL_MONOFORUM_UNSUPPORTED'):
            with self.subTest(enum=enum):
                # Both values are absent from pinned Telethon's generic enum map.
                self.assertNotIn(enum, errors.rpc_errors_dict)
                for request, code in ((other, 400), (self.request(), 500), (None, 400)):
                    result = safe_rpc_metadata(errors.RPCError(request, enum, code=code))
                    self.assertIsNone(result['message'])

    def test_private_unknown_strings_and_request_fields_remain_redacted(self):
        for message in ('PRIVATE_SECRET_1234', 'CHAT_MEMBER_ADD_FAILED PRIVATE_SECRET_1234',
                        'CHAT_MEMBER_ADD_FAILED\nPRIVATE_SECRET_1234', 'chat_member_add_failed'):
            with self.subTest(message=message):
                result = safe_rpc_metadata(errors.BadRequestError(self.request(), message, code=400))
                self.assertIsNone(result['message'])
                encoded = json.dumps(result)
                for private in ('PRIVATE_SECRET_1234', '99123456789', '88123456789', 'access_hash', 'users', 'channel'):
                    self.assertNotIn(private, encoded)


class InviteRPCEnumHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.OperationsTests()
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_new_official_enum_is_recorded_without_reclassifying_existing_unknown_history(self):
        ops = self.fixture.ops
        legacy = await self.fixture.draft('invite', request_key='preserved-unknown-enum-history')
        original = {'code': 'BadRequestError', 'rpc': {
            'request_constructor': 'InviteToChannelRequest', 'exception_type': 'BadRequestError',
            'code': 400, 'message': None}, 'preserved': 'original evidence'}
        with ops.store.db:
            ops.store.db.execute("UPDATE op_items SET state='failed',diagnosis=? WHERE job=?",
                                 (json.dumps(original), legacy['id']))
        old_rows = ops.store.rows('SELECT * FROM op_items WHERE job=?', (legacy['id'],))
        job = await self.fixture.draft('invite')
        request = InviteRPCEnumAllowlistTests().request()
        self.fixture.c.failure = errors.BadRequestError(request, 'CHAT_MEMBER_ADD_FAILED', code=400)
        await self.fixture.start(job)
        await ops.tick()
        item = (await ops.detail(job['id']))['items'][0]
        self.assertEqual(item['state'], 'failed')
        self.assertEqual(item['diagnosis']['code'], 'BadRequestError')
        self.assertTrue(item['diagnosis']['invitation_requested'])
        self.assertEqual(item['diagnosis']['rpc']['message'], 'CHAT_MEMBER_ADD_FAILED')
        self.assertEqual(ops.store.rows('SELECT * FROM op_items WHERE job=?', (legacy['id'],)), old_rows)
        self.assertEqual(len(self.fixture.c.calls), 1)
        self.assertEqual(ops.store.rows("SELECT * FROM op_invited WHERE state='invited'"), [])
        self.assertEqual(ops.store.rows('SELECT state FROM op_invited WHERE job=?', (job['id'],)), [{'state': 'failed'}])
