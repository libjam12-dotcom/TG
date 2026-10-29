import asyncio
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store
from publisher import Publisher,PostSpec,ScheduleSpec,writable
from fastapi import HTTPException
from telethon import types,errors,functions

class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'db')
        self.entity=types.Channel(id=123,title='Managed',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,creator=True,username='managed')
        self.client=SimpleNamespace(is_connected=lambda:True,get_entity=AsyncMock(return_value=self.entity))
        self.runtime=SimpleNamespace(client=self.client,me=SimpleNamespace(id=5))
        self.pub=Publisher(lambda:self.store,lambda:self.runtime,self.tmp.name);self.pub.initialize()
        self.pub.send=AsyncMock(return_value=types.UpdateShortSentMessage(id=123,pts=1,pts_count=1,date=datetime.now(timezone.utc)))
    async def asyncTearDown(self):
        await self.pub.shutdown();self.store.db.close();self.tmp.cleanup()
    async def draft(self,**kwargs):return await self.pub.create(PostSpec(title='Test',text='Hello',destinations=[-1000000000123],**kwargs))
    async def test_managed_only_and_channel_post_permission(self):
        self.entity.creator=False
        with self.assertRaises(HTTPException):await self.draft()
        self.entity.broadcast=True;self.entity.admin_rights=types.ChatAdminRights(delete_messages=True)
        self.assertFalse(writable(self.entity))
        self.entity.admin_rights.post_messages=True;self.assertTrue(writable(self.entity))
    async def test_connect_bot_requires_management_and_grants_only_moderation(self):
        requests=[];bot=types.User(id=77,first_name='Bot',bot=True)
        owner=self
        class Client:
            def is_connected(self):return True
            async def get_entity(self,ref):return bot if isinstance(ref,str) else owner.entity
            async def __call__(self,request):
                requests.append(request)
                if isinstance(request,functions.channels.GetParticipantRequest):raise errors.UserNotParticipantError(None)
        self.runtime.client=Client()
        self.entity.creator=False
        with self.assertRaises(HTTPException):await self.pub.connect_bot(-1000000000123)
        self.assertEqual(requests,[])
        self.entity.admin_rights=types.ChatAdminRights(delete_messages=True)
        with self.assertRaises(HTTPException):await self.pub.connect_bot(-1000000000123)
        self.assertFalse(any(isinstance(r,functions.channels.EditAdminRequest) for r in requests))
        self.entity.creator=True;requests.clear();await self.pub.connect_bot(-1000000000123)
        rights=requests[-1].admin_rights;self.assertTrue(rights.delete_messages);self.assertTrue(rights.ban_users);self.assertFalse(rights.add_admins);self.assertFalse(rights.post_messages)
    async def test_publish_is_idempotent_and_no_read_side_effect(self):
        p=await self.draft();await self.pub.posts();self.pub.send.assert_not_called()
        await self.pub.publish(p['id']);await self.pub.publish(p['id']);await self.pub.tick();await self.pub.publish(p['id']);await self.pub.tick()
        self.pub.send.assert_awaited_once();p=self.pub.post(p['id']);self.assertEqual(p['status'],'published');self.assertEqual(p['delivery'][0]['links'],['https://t.me/managed/123'])
    async def test_remove_only_own_recorded_sent_messages_once(self):
        p=await self.draft();await self.pub.publish(p['id'])
        with self.assertRaises(HTTPException):await self.pub.remove_messages(p['id'])
        await self.pub.tick();self.client.delete_messages=AsyncMock()
        await self.pub.remove_messages(p['id']);await self.pub.remove_messages(p['id'])
        self.client.delete_messages.assert_awaited_once_with(self.entity,[123],revoke=True)
        self.assertEqual(self.pub.post(p['id'])['status'],'removed')
    async def test_schedule_cancel_and_edit(self):
        p=await self.draft()
        with self.assertRaises(HTTPException):await self.pub.schedule(p['id'],ScheduleSpec(at=datetime.now()))
        await self.pub.schedule(p['id'],ScheduleSpec(at=datetime.now(timezone.utc)+timedelta(hours=1)))
        await self.pub.tick();self.pub.send.assert_not_called()
        await self.pub.edit(p['id'],PostSpec(title='Changed',text='yes',destinations=[-1000000000123]))
        self.assertEqual(self.pub.post(p['id'])['status'],'draft')
        await self.pub.publish(p['id']);await self.pub.cancel(p['id']);await self.pub.tick();self.pub.send.assert_not_called()
    async def test_uncertain_and_restart_never_resend(self):
        p=await self.draft();await self.pub.publish(p['id']);self.pub.send.side_effect=TimeoutError()
        await self.pub.tick();self.assertEqual(self.pub.post(p['id'])['delivery'][0]['state'],'uncertain')
        with self.assertRaises(HTTPException):await self.pub.retry(p['id'])
        with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='sending'");self.store.db.execute("UPDATE content_posts SET status='publishing'")
        self.pub.initialize();await self.pub.tick();self.assertEqual(self.pub.send.await_count,1)
    async def test_retry_only_failed_destinations_with_same_random_ids(self):
        p=await self.draft();await self.pub.publish(p['id']);ids=self.store.rows('SELECT random_ids FROM content_delivery')[0]['random_ids']
        self.pub.send.side_effect=errors.ChatWriteForbiddenError(None);await self.pub.tick();self.assertEqual(self.pub.post(p['id'])['delivery'][0]['state'],'failed')
        self.pub.send.side_effect=None;await self.pub.retry(p['id']);await self.pub.tick();self.assertEqual(self.pub.post(p['id'])['status'],'published')
        self.assertEqual(self.store.rows('SELECT random_ids FROM content_delivery')[0]['random_ids'],ids)
    async def test_floodwait_respects_delay_and_loss_of_permissions_fails(self):
        p=await self.draft();await self.pub.publish(p['id']);self.pub.send.side_effect=errors.FloodWaitError(None,capture=15)
        await self.pub.tick();await self.pub.tick();self.assertEqual(self.pub.send.await_count,1)
        self.entity.creator=False
        with self.store.db:self.store.db.execute('UPDATE content_delivery SET next_at=0')
        await self.pub.tick();self.assertEqual(self.pub.post(p['id'])['delivery'][0]['state'],'failed');self.assertEqual(self.pub.send.await_count,1)
    async def test_caption_validation_and_media_upload(self):
        class Request:
            async def stream(self):yield b'not a photograph'
        with self.assertRaises(HTTPException):await self.pub.upload(Request())
        with self.assertRaises(HTTPException):await self.pub.create(PostSpec(title='x',text='🙂'*600,media=['missing'],destinations=[-1000000000123]))
        with self.assertRaises(HTTPException):await self.pub.create(PostSpec(title='  ',text='yes',destinations=[-1000000000123]))
    async def test_album_uses_uploaded_media_and_persistent_random_ids(self):
        for i in ['one','two']:
            (self.pub.data/(i+'.jpg')).write_bytes(b'image')
            with self.store.db:self.store.db.execute('INSERT INTO content_media VALUES(?,?,?,?,?)',(i,i+'.jpg','image/jpeg',5,'now'))
        requests=[]
        class Client:
            get_input_entity=AsyncMock(return_value=types.InputPeerSelf())
            upload_file=AsyncMock(return_value=types.InputFile(id=1,parts=1,name='test.jpg',md5_checksum=''))
            async def __call__(self,request):
                requests.append(request)
                if isinstance(request,functions.messages.UploadMediaRequest):return types.MessageMediaPhoto(photo=types.Photo(id=1,access_hash=2,file_reference=b'x',date=datetime.now(timezone.utc),sizes=[],dc_id=2))
                return SimpleNamespace(updates=[])
        self.runtime.client=Client()
        await Publisher.send(self.pub,{'media':['one','two'],'text':'caption','silent':True},{'random_ids':json.dumps([111,222])},self.entity)
        self.assertEqual(len(requests),3);album=requests[-1];self.assertIsInstance(album,functions.messages.SendMultiMediaRequest)
        self.assertEqual([m.random_id for m in album.multi_media],[111,222]);self.assertEqual([m.message for m in album.multi_media],['caption',''])
        self.assertTrue(all(isinstance(m.media,types.InputMediaPhoto) for m in album.multi_media))
