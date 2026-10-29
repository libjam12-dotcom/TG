from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

import httpx
from fastapi import FastAPI
from telethon import errors, functions, types, utils

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store
from source_folders import SourceFolders


def channel(cid, username=None, **kwargs):
    return types.Channel(id=cid, title='Fixture group '+str(cid), username=username,
                         photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), **kwargs)


class FolderAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name)/'test.sqlite3')
        self.calls, self.response, self.failure, self.after_read = [], None, None, None
        owner = self
        class Telegram:
            connected = True
            def is_connected(self): return self.connected
            async def __call__(self, request):
                # Reject any mutation or message request, including automatic joining.
                if not isinstance(request, functions.chatlists.CheckChatlistInviteRequest):
                    raise AssertionError('Folder preview must only inspect the invite')
                owner.calls.append(request.slug)
                if owner.after_read: owner.after_read()
                if owner.failure: raise owner.failure
                return owner.response
        self.telegram = Telegram()
        self.runtime = SimpleNamespace(client=self.telegram, me=SimpleNamespace(id=42))
        self.folders = SourceFolders(lambda:self.runtime, lambda:self.store)
        self.app = FastAPI()
        self.app.include_router(self.folders.router)
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://fixture.test')

    async def asyncTearDown(self):
        await self.http.aclose()
        self.store.db.close()
        self.tmp.cleanup()

    async def preview(self, url='https://t.me/addlist/Fixture_slug-1'):
        return await self.http.post('/api/scanner/source-folders/preview', json={'url':url})

    def invite(self, chats, peers=None):
        return types.chatlists.ChatlistInvite(title=types.TextWithEntities('Fixture folder', []),
                                             peers=peers or [types.PeerChannel(c.id) for c in chats], chats=chats, users=[])

    async def test_preview_lists_folder_and_existing_sources_without_writing_or_joining(self):
        public = channel(101, 'fixture_public', left=True, megagroup=True)
        private = channel(102, left=False, megagroup=True)
        self.response = self.invite([public, private])
        public_id = utils.get_peer_id(public)
        with self.store.db:
            self.store.db.execute('INSERT INTO sources(id,title,username,enabled) VALUES(?,?,?,0)', (public_id,'Existing source','fixture_public'))
        before = '\n'.join(self.store.db.iterdump())
        response = await self.preview()
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual((data['slug'], data['title']), ('Fixture_slug-1','Fixture folder'))
        self.assertEqual({r['id'] for r in data['sources']}, {public_id,utils.get_peer_id(private)})
        public_row = next(r for r in data['sources'] if r['id']==public_id)
        self.assertTrue(public_row['already_configured'])
        self.assertTrue(all(r['readable_candidate'] for r in data['sources']))
        self.assertTrue(all('проверяется отдельно' in r['access_note'] for r in data['sources']))
        self.assertEqual(before, '\n'.join(self.store.db.iterdump()))
        self.assertEqual(self.calls, ['Fixture_slug-1'])

    async def test_preview_exposes_actual_group_channel_and_unknown_types_without_changing_eligibility(self):
        group = channel(101, 'fixture_group', megagroup=True)
        broadcast = channel(102, 'fixture_channel', broadcast=True)
        private = types.Chat(id=103, title='Basic group', photo=types.ChatPhotoEmpty(), participants_count=5, date=None, version=1)
        forbidden = types.ChannelForbidden(id=104, access_hash=5, title='Unavailable')
        self.response = self.invite([group, broadcast, private, forbidden], [types.PeerChannel(101), types.PeerChannel(102), types.PeerChat(103), types.PeerChannel(104)])
        before = '\n'.join(self.store.db.iterdump())
        response = await self.preview()
        self.assertEqual(response.status_code, 200)
        rows = {row['id']: row for row in response.json()['sources']}
        self.assertEqual(rows[utils.get_peer_id(group)]['kind'], 'group')
        self.assertEqual(rows[utils.get_peer_id(broadcast)]['kind'], 'channel')
        self.assertEqual(rows[utils.get_peer_id(private)]['kind'], 'group')
        self.assertEqual(rows[utils.get_peer_id(forbidden)]['kind'], 'unknown')
        self.assertTrue(rows[utils.get_peer_id(broadcast)]['readable_candidate'])
        self.assertFalse(rows[utils.get_peer_id(forbidden)]['readable_candidate'])
        self.assertEqual(before, '\n'.join(self.store.db.iterdump()))
        self.assertEqual(self.calls, ['Fixture_slug-1'])

    async def test_admin_creator_private_and_forbidden_are_not_external_candidates(self):
        chats = [channel(1,'managed_admin',admin_rights=types.ChatAdminRights(delete_messages=True)),
                 channel(2,'managed_creator',creator=True), channel(3,left=True),
                 types.ChannelForbidden(id=4,access_hash=5,title='Unavailable'),
                 types.Chat(id=5,title='Old group',photo=types.ChatPhotoEmpty(),participants_count=5,date=None,version=1,deactivated=True)]
        self.response = self.invite(chats,[types.PeerChannel(c.id) for c in chats[:4]]+[types.PeerChat(5)])
        response = await self.preview()
        self.assertEqual(response.status_code, 200)
        rows = response.json()['sources']
        self.assertEqual(len(rows),5)
        self.assertTrue(all(not r['readable_candidate'] for r in rows))
        self.assertTrue(rows[0]['managed'])
        self.assertTrue(rows[1]['managed'])
        self.assertTrue(all(r['access_note'] for r in rows))

    async def test_imported_folder_combines_missing_and_existing_peers_without_users_or_duplicates(self):
        a, b = channel(11,'first'), channel(12,'second')
        self.response = types.chatlists.ChatlistInviteAlready(filter_id=7,
            already_peers=[types.PeerChannel(11)],
            missing_peers=[types.PeerChannel(12),types.PeerChannel(11),types.PeerChannel(13),types.PeerUser(90)],
            chats=[a,b,channel(99,'unrelated')],users=[types.User(id=90)])
        response = await self.preview()
        self.assertEqual(response.status_code,200)
        rows = response.json()['sources']
        self.assertEqual({r['id'] for r in rows},{utils.get_peer_id(types.PeerChannel(i)) for i in (11,12,13)})
        unknown = next(r for r in rows if r['id']==utils.get_peer_id(types.PeerChannel(13)))
        self.assertFalse(unknown['readable_candidate'])
        self.assertTrue(response.json()['title'])

    async def test_invalid_links_do_not_contact_telegram(self):
        for url in ['http://t.me/addlist/slug','https://example.test/addlist/slug','https://t.me.evil.test/addlist/slug',
                    'https://user@t.me/addlist/slug','https://t.me:443/addlist/slug','https://t.me/addlist/',
                    'https://t.me/addlist/slug/extra','https://t.me/addlist/slug?x=1','https://t.me/addlist/slug#x',
                    'https://t.me/addlist/%2fsecret']:
            with self.subTest(url=url):
                response = await self.preview(url)
                self.assertEqual(response.status_code,400)
        self.assertFalse(self.calls)

    async def test_disconnected_account_returns_actionable_conflict(self):
        self.telegram.connected = False
        response = await self.preview()
        self.assertEqual(response.status_code,409)
        self.assertIn('подключите',response.json()['detail'])
        self.assertFalse(self.calls)

    async def test_expired_and_invalid_invites_return_not_found_without_internal_error(self):
        for failure in [errors.InviteHashExpiredError(None),errors.InviteHashInvalidError(None),
                        errors.RPCError(None,'INVITE_SLUG_EXPIRED',400)]:
            with self.subTest(error=type(failure).__name__):
                self.failure = failure
                response = await self.preview()
                self.assertEqual(response.status_code,404)
                self.assertIn('новую ссылку',response.json()['detail'])
                self.assertNotIn('INVITE_',response.text)

    async def test_temporary_telegram_errors_return_conflict_without_leaking_details(self):
        for failure in [errors.FloodWaitError(None,capture=45),errors.RPCError(None,'INTERNAL_PRIVATE_VALUE',400),
                        OSError('INTERNAL_PRIVATE_VALUE'),TimeoutError('INTERNAL_PRIVATE_VALUE')]:
            with self.subTest(error=type(failure).__name__):
                self.failure = failure
                response = await self.preview()
                self.assertEqual(response.status_code,409)
                self.assertNotIn('INTERNAL_PRIVATE_VALUE',response.text)

    async def test_identity_change_during_check_does_not_return_stale_folder(self):
        self.response = self.invite([channel(9,'fixture')])
        self.after_read = lambda:setattr(self.runtime,'me',SimpleNamespace(id=77))
        response = await self.preview()
        self.assertEqual(response.status_code,409)
        self.assertIn('изменился',response.json()['detail'])
