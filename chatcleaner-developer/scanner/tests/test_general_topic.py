"""Explicit General route stays saved; its transport never replies to message1."""
import json
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from fastapi import HTTPException
from pydantic import ValidationError
from telethon import functions,types
import test_operation_topics as fixture
from operation_topics import DestinationTopic
from operation_workbench import PlanSpec
from operation_campaigns import CampaignSpec
from publisher import Publisher

CID=fixture.CID
WRITES=(functions.messages.SendMessageRequest,functions.messages.SendMediaRequest,functions.messages.SendMultiMediaRequest)


class GeneralTopicTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixture.OperationTopicTests.asyncSetUp
    asyncTearDown=fixture.OperationTopicTests.asyncTearDown
    start=fixture.OperationTopicTests.start
    clear_wait=fixture.OperationTopicTests.clear_wait
    policy=fixture.OperationTopicTests.policy
    draft=fixture.OperationTopicTests.draft
    forum=fixture.OperationTopicTests.forum
    mapping=fixture.OperationTopicTests.mapping
    add_media=fixture.OperationTopicTests.add_media

    def general(self):
        self.forum();self.c.topic=fixture.topic(topic_id=1,title='General')

    async def test_general_is_strict_explicit_integer_route(self):
        self.assertEqual(DestinationTopic(chat_id=CID,topic_id=1).topic_id,1)
        for tid in (0,-1,True,'1',1.0,2**31):
            with self.subTest(topic_id=tid),self.assertRaises(ValidationError):DestinationTopic(chat_id=CID,topic_id=tid)
        self.assertEqual(DestinationTopic(chat_id=CID,topic_id=140).topic_id,140)

    async def test_general_metadata_reads_exact1_member_and_preserves_identity(self):
        self.general();before=self.ops.store.db.total_changes
        result=await self.ops.topics.metadata(self.a.aid,CID,1)
        self.assertEqual((result['chat_id'],result['topic_id'],result['title']),(CID,1,'General'))
        self.assertTrue(result['writable']);self.assertEqual(result['telegram_id'],100)
        self.assertEqual(self.ops.store.db.total_changes,before)
        self.assertEqual([type(r) for r in self.c.calls],[functions.channels.GetParticipantRequest,functions.channels.GetForumTopicsByIDRequest])
        self.assertEqual(self.c.calls[-1].topics,[1]);self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_draft_snapshot_preview_item_and_worker_keep_route1(self):
        self.general();job=await self.draft(destination_topics=self.mapping(1))
        self.assertEqual(job['spec']['destination_topics'],self.mapping(1))
        self.assertEqual(job['spec']['topic_snapshots'][0]['topic_id'],1)
        self.assertEqual((job['items'][0]['topic_id'],job['items'][0]['topic_title']),(1,'General'))
        self.assertEqual(job['spec']['posting_policy']['destinations'][0]['chat_id'],CID)
        random_id=json.loads(self.ops.store.rows('SELECT random_ids FROM op_items WHERE job=?',(job['id'],))[0]['random_ids'])[0]
        self.c.calls.clear();await self.start(job);await self.ops.tick()
        writes=[r for r in self.c.calls if isinstance(r,WRITES)]
        self.assertEqual(len(writes),1);self.assertIsNone(writes[0].reply_to);self.assertEqual(writes[0].random_id,random_id)
        saved=await self.ops.detail(job['id']);self.assertEqual(saved['items'][0]['state'],'sent')
        self.assertEqual(saved['items'][0]['topic_id'],1)
        exact=[r for r in self.c.calls if isinstance(r,functions.channels.GetForumTopicsByIDRequest)]
        self.assertTrue(exact);self.assertTrue(all(r.topics==[1] for r in exact))

    async def test_text_transport_general_vs_thread_and_plain(self):
        pub=Publisher(lambda:self.ops.store,lambda:self.a,self.ops.data)
        for tid in (None,1,140):
            self.c.calls.clear();await pub.send(dict(media=[],text='hello',silent=False),dict(random_ids='[12345]'),self.group,topic_id=tid)
            request=self.c.calls[0];self.assertIsInstance(request,functions.messages.SendMessageRequest)
            if tid==140:self.assertEqual(request.reply_to.reply_to_msg_id,140)
            else:self.assertIsNone(request.reply_to)
            self.assertEqual(request.random_id,12345)
        self.c.calls.clear()
        with self.assertRaises(HTTPException):await pub.send(dict(media=[],text='hello',silent=False),dict(random_ids='[12345]'),self.group,topic_id=0)
        self.assertFalse(self.c.calls)

    async def test_general_photo_and_album_omit_reply_to_keep_ids(self):
        self.general()
        for mid in ('a'*32,'b'*32):self.add_media(mid)
        original=fixture.TopicClient.__call__;requests=[]
        class MediaClient(fixture.TopicClient):
            async def __call__(inner,request):
                requests.append(request)
                if isinstance(request,functions.messages.UploadMediaRequest):
                    return types.MessageMediaPhoto(photo=types.Photo(id=1,access_hash=2,file_reference=b'x',date=datetime.now(timezone.utc),sizes=[],dc_id=2))
                return await original(inner,request)
        client=MediaClient(self.group);client.upload_file=AsyncMock(return_value=types.InputFile(id=1,parts=1,name='test.jpg',md5_checksum=''));self.a.client=client
        pub=Publisher(lambda:self.ops.store,lambda:self.a,self.ops.data)
        for media,ids in [(['a'*32],[111]),(['a'*32,'b'*32],[222,333])]:
            await pub.send(dict(media=media,text='caption',silent=False),dict(random_ids=json.dumps(ids)),self.group,topic_id=1)
        writes=[r for r in requests if isinstance(r,WRITES)]
        self.assertEqual(len(writes),2);self.assertTrue(all(r.reply_to is None for r in writes))
        self.assertEqual(writes[0].random_id,111);self.assertEqual([r.random_id for r in writes[1].multi_media],[222,333])

    async def test_general_no_route_is_never_automatic_fallback(self):
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping(1))
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.general()
        with self.assertRaises(HTTPException):await self.draft()
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertFalse(self.c.calls)
        self.group.forum=False;plain=await self.draft();self.group.forum=True
        await self.start(plain);await self.ops.tick()
        self.assertEqual((await self.ops.detail(plain['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_closed_hidden_short_missing_wrong_id_rejected(self):
        self.general()
        for bad in (fixture.topic(1,closed=True),fixture.topic(1,hidden=True),fixture.topic(1,short=True),
            types.ForumTopicDeleted(id=1),None,fixture.topic(140)):
            self.c.topic=bad
            with self.subTest(topic=bad),self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping(1))
            self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_wrong_peer_member_or_ban_still_rejected(self):
        self.general()
        other=types.Channel(id=20,title='Wrong',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,forum=True)
        with self.assertRaises(HTTPException):await self.ops.topics.check(self.a,other,CID,1)
        self.assertFalse(self.c.calls)
        self.c.participant=types.ChannelParticipant(user_id=101,date=datetime.now(timezone.utc))
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping(1))
        self.c.participant=types.ChannelParticipant(user_id=100,date=datetime.now(timezone.utc))
        self.group.default_banned_rights=types.ChatBannedRights(until_date=None,send_plain=True)
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping(1))
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_renamed_after_draft_blocks_before_send(self):
        self.general();job=await self.draft(destination_topics=self.mapping(1));self.c.topic.title='Changed'
        self.c.calls.clear();await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_closes_during_upload_final_recheck_blocks(self):
        self.general();self.add_media('a'*32);job=await self.draft(media=['a'*32],destination_topics=self.mapping(1))
        async def upload(_):
            self.c.topic.closed=True;return types.InputFile(id=1,parts=1,name='test.jpg',md5_checksum='')
        self.c.upload_file=upload;self.c.calls.clear();await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

    async def test_general_keeps_existing_cid_removed_hold_across_topics(self):
        self.forum();old=await self.draft(destination_topics=self.mapping(140))
        with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?",(old['id'],))
        self.ops.tracking._event('removed-old-topic-for-general','deletion_observed',job_id=old['id'],item_id=old['items'][0]['id'],
            account=self.a.aid,telegram_id=100,chat_id=CID,message_id=700,attribution='saved_publication')
        self.c.topic=fixture.topic(1,title='General');job=await self.draft(request_key='general-hold-does-not-bypass',destination_topics=self.mapping(1))
        self.c.calls.clear();await self.start(job);await self.ops.tick();item=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'publication_removed');self.assertIsNone(item['attempted_at']);self.assertFalse(self.c.calls)

    async def test_general_finite_preview_saved_plan_report_preserve_explicit1(self):
        self.general();profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='General',text='Publication')))['campaign']
        day=(datetime.now(timezone.utc)+timedelta(days=1)).date()
        spec=PlanSpec(request_key='general-explicit-finite-plan',account=self.a.aid,telegram_id=100,kind='post',
            title='Explicit General',destinations=[CID],destination_topics=self.mapping(1),text='Publication',
            campaign_id=profile['id'],campaign_revision=profile['revision'],start_date=day,end_date=day,times=['12:00'])
        before=await self.ops.workbench.preview(spec);self.assertEqual(before['destination_topics'],self.mapping(1))
        saved=await self.ops.workbench.save_plan(spec);job=await self.ops.detail(saved['jobs'][0]['id'])
        self.assertEqual(job['spec']['destination_topics'],self.mapping(1));self.assertEqual(job['spec']['topic_snapshots'][0]['topic_id'],1)
        self.assertEqual(saved['items'][0]['topic_id'],1);self.assertEqual(saved['topic_snapshots'][0]['topic_id'],1)
        report=(await self.ops.workbench.report(saved['id'])).body.decode('utf-8');self.assertIn(';1;General;',report)
        self.assertFalse(any(isinstance(r,WRITES) for r in self.c.calls))

if __name__=='__main__':unittest.main()
