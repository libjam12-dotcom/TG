"""Exact forum destinations must never fall back to General."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from fastapi import HTTPException
from pydantic import ValidationError
from telethon import functions, types

import test_operations as fixture
from operations import JobSpec
from operation_workbench import PlanSpec, ExclusionSpec
from operation_campaigns import CampaignSpec
from publisher import Publisher


CID = -1000000000010


def topic(topic_id=140, **changes):
    values = dict(id=topic_id, date=datetime.now(timezone.utc), title='Free ads', icon_color=0,
                  top_message=topic_id, read_inbox_max_id=0, read_outbox_max_id=0,
                  unread_count=0, unread_mentions_count=0, unread_reactions_count=0,
                  from_id=types.PeerUser(100), notify_settings=types.PeerNotifySettings())
    values.update(changes)
    return types.ForumTopic(**values)


class TopicClient(fixture.Client):
    def __init__(self, group):
        super().__init__(group)
        self.topic = topic()
        self.participant = types.ChannelParticipant(user_id=100, date=datetime.now(timezone.utc))
        self.topic_hook = None

    async def __call__(self, request):
        if isinstance(request, functions.channels.GetParticipantRequest):
            self.calls.append(request)
            return SimpleNamespace(participant=self.participant)
        if isinstance(request, functions.channels.GetForumTopicsByIDRequest):
            self.calls.append(request)
            if self.topic_hook:
                await self.topic_hook()
            return SimpleNamespace(topics=[] if self.topic is None else [self.topic])
        return await super().__call__(request)


class OperationTopicTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixture.OperationsTests.asyncSetUp
    asyncTearDown = fixture.OperationsTests.asyncTearDown
    start = fixture.OperationsTests.start
    clear_wait = fixture.OperationsTests.clear_wait

    def policy(self, **changes):
        end=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
        result=dict(window_expires_at=end,campaign_expires_at=end,timezone='UTC',
                    destinations=[dict(chat_id=CID,max_per_day=1,min_interval_seconds=14400)])
        result.update(changes)
        return result

    async def draft(self, kind='post', **values):
        if values.get('destination_topics'):
            if not hasattr(self,'profile'):
                self.profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='Topic test',text='Publication')))['campaign']
            values.setdefault('campaign_id',self.profile['id'])
            values.setdefault('campaign_revision',self.profile['revision'])
            values.setdefault('posting_policy',self.policy())
        return await fixture.OperationsTests.draft(self,kind,**values)

    def forum(self):
        self.group.forum = True
        self.c = TopicClient(self.group)
        self.a.client = self.c

    def mapping(self, topic_id=140):
        return [dict(chat_id=CID, topic_id=topic_id)]

    async def test_nonempty_topic_change_conflicts_with_same_request_key(self):
        self.forum()
        profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='Digest topic test',text='Publication')))['campaign']
        # The same policy timestamps and every other field stay fixed; only the
        # selected topic changes. Reusing the key must not adopt a new destination.
        values=dict(request_key='topic-digest-key-0001',account=self.a.aid,kind='post',title='Topic digest',
            text='Publication',destinations=[CID],campaign_id=profile['id'],campaign_revision=profile['revision'],
            posting_policy=self.policy())
        original=await self.ops.create(JobSpec(**values,destination_topics=self.mapping(140)))
        before=self.ops.store.rows('SELECT digest,spec FROM op_jobs WHERE id=?',(original['id'],))[0]
        with self.assertRaises(HTTPException) as conflict:
            await self.ops.create(JobSpec(**values,destination_topics=self.mapping(141)))
        self.assertEqual(conflict.exception.status_code,409)
        self.assertEqual(self.ops.store.rows('SELECT digest,spec FROM op_jobs WHERE id=?',(original['id'],))[0],before)
        self.assertEqual(json.loads(before['spec'])['destination_topics'],self.mapping(140))
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_general_requires_bound_policy_and_client_snapshots_are_rejected(self):
        values = dict(request_key='topic-validation-test-01', account=self.a.aid, kind='post',
                      title='Topic', destinations=[CID], text='hello')
        with self.assertRaises(ValidationError):
            JobSpec(**values, destination_topics=self.mapping(1))
        general = JobSpec(**values, destination_topics=self.mapping(1),
                          campaign_id=1, campaign_revision=1, posting_policy=self.policy())
        self.assertEqual(general.destination_topics[0].topic_id, 1)
        with self.assertRaises(ValidationError):
            JobSpec(**values, topic_snapshots=[])

    async def test_topic_mapping_requires_saved_campaign_and_finite_cid_policy(self):
        values=dict(request_key='topic-policy-required-01',account=self.a.aid,kind='post',
                    title='Topic',destinations=[CID],text='hello',destination_topics=self.mapping())
        for extra in ({},{'campaign_id':1,'campaign_revision':1},{'posting_policy':self.policy()}):
            with self.subTest(extra=extra),self.assertRaises(ValidationError):JobSpec(**values,**extra)
        JobSpec(**values,campaign_id=1,campaign_revision=1,posting_policy=self.policy())

    async def test_forum_draft_preserves_exact_server_snapshot_without_send(self):
        self.forum()
        job = await self.draft(destination_topics=self.mapping())
        self.assertEqual(job['spec']['destination_topics'], self.mapping())
        snap = job['spec']['topic_snapshots'][0]
        self.assertEqual((snap['chat_id'], snap['topic_id'], snap['title']), (CID, 140, 'Free ads'))
        self.assertEqual(snap['telegram_id'], 100)
        self.assertFalse(any(isinstance(r, functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_publisher_text_uses_exact_topic_and_persistent_random_id(self):
        self.forum()
        pub = Publisher(lambda: self.ops.store, lambda: self.a, self.ops.data)
        await pub.send(dict(media=[], text='hello', silent=False), dict(random_ids='[12345]'),
                       self.group, topic_id=140)
        request = next(r for r in self.c.calls if isinstance(r, functions.messages.SendMessageRequest))
        self.assertEqual((request.reply_to.reply_to_msg_id, request.random_id), (140, 12345))
        self.assertIsNone(request.reply_to.top_msg_id)

    async def test_mapping_is_strict_scoped_unique_and_post_only(self):
        values = dict(request_key='topic-validation-test-02', account=self.a.aid, kind='post',
                      title='Topic', destinations=[CID], text='hello')
        for routes in ([dict(chat_id=CID,topic_id=True)], [dict(chat_id=CID,topic_id='140')],
                       [dict(chat_id=CID,topic_id=2**31)], [dict(chat_id=CID-1,topic_id=140)],
                       self.mapping()+self.mapping(), [dict(chat_id=CID,topic_id=140,title='forged')]):
            with self.subTest(routes=routes), self.assertRaises(ValidationError):
                JobSpec(**values,destination_topics=routes)
        values['kind']='invite'
        with self.assertRaises(ValidationError):JobSpec(**values,destination_topics=self.mapping())

    async def test_forum_without_selection_or_topic_for_nonforum_never_creates_job(self):
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping())
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.forum()
        with self.assertRaises(HTTPException):await self.draft()
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertFalse(self.c.calls)

    async def test_plain_job_spec_and_transport_are_unchanged_without_topic_rpc(self):
        job=await self.draft()
        self.assertNotIn('destination_topics',job['spec'])
        self.assertNotIn('topic_snapshots',job['spec'])
        self.assertNotIn('topic_id',job['items'][0])
        self.assertFalse(self.c.calls)
        await self.start(job);await self.ops.tick()
        self.assertEqual(len(self.c.calls),1)
        self.assertIsInstance(self.c.calls[0],functions.messages.SendMessageRequest)
        self.assertIsNone(self.c.calls[0].reply_to)

    async def test_closed_hidden_short_deleted_missing_and_wrong_topic_fail_closed(self):
        self.forum()
        for bad in (topic(closed=True),topic(hidden=True),topic(short=True),
                    types.ForumTopicDeleted(id=140),None,topic(topic_id=141)):
            self.c.topic=bad
            with self.subTest(topic=bad),self.assertRaises(HTTPException):
                await self.draft(destination_topics=self.mapping())
            self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_metadata_is_read_only_identity_bound_and_drops_message_payloads(self):
        self.forum();before=self.ops.store.db.total_changes
        result=await self.ops.topics.metadata(self.a.aid,CID,140)
        self.assertEqual((result['account'],result['telegram_id'],result['chat_id'],result['topic_id']),
                         (self.a.aid,100,CID,140))
        self.assertEqual(result['title'],'Free ads')
        self.assertNotIn('messages',result);self.assertNotIn('users',result)
        self.assertEqual(self.ops.store.db.total_changes,before)
        self.assertEqual([type(r) for r in self.c.calls],
                         [functions.channels.GetParticipantRequest,functions.channels.GetForumTopicsByIDRequest])
        self.assertEqual(self.c.calls[-1].topics,[140])

    async def test_wrong_peer_or_unverified_participant_or_send_ban_stops(self):
        self.forum()
        other=types.Channel(id=20,title='Other',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,forum=True)
        with self.assertRaises(HTTPException):await self.ops.topics.check(self.a,other,CID,140)
        self.assertFalse(self.c.calls)
        self.c.participant=types.ChannelParticipant(user_id=101,date=datetime.now(timezone.utc))
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping())
        self.c.participant=types.ChannelParticipant(user_id=100,date=datetime.now(timezone.utc))
        self.group.default_banned_rights=types.ChatBannedRights(until_date=None,send_plain=True)
        with self.assertRaises(HTTPException):await self.draft(destination_topics=self.mapping())
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_closed_or_renamed_after_draft_stops_without_general_send(self):
        self.forum();job=await self.draft(destination_topics=self.mapping())
        self.c.topic.closed=True
        await self.start(job);await self.ops.tick()
        detail=await self.ops.detail(job['id'])
        self.assertEqual(detail['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))
        self.clear_wait();self.c.topic=topic()
        policy=self.policy();policy['destinations'][0].update(max_per_day=3,min_interval_seconds=0)
        job=await self.draft(request_key='topic-renamed-test-01',destination_topics=self.mapping(),posting_policy=policy)
        self.c.topic.title='Different category'
        await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_pause_or_sender_switch_during_final_topic_read_stops(self):
        self.forum();job=await self.draft(destination_topics=self.mapping())
        async def pause():
            with self.ops.store.db:self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?",(job['id'],))
        self.c.topic_hook=pause
        await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'blocked')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))
        self.clear_wait();self.c.topic_hook=None
        policy=self.policy();policy['destinations'][0].update(max_per_day=3,min_interval_seconds=0)
        job=await self.draft(request_key='topic-client-switch-01',destination_topics=self.mapping(),posting_policy=policy)
        old=self.c
        async def change_client():self.a.client=TopicClient(self.group)
        old.topic_hook=change_client
        await self.start(job);await self.ops.tick()
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in old.calls))
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')

    async def test_changed_forum_mode_never_falls_back(self):
        job=await self.draft()
        self.forum()
        await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    def add_media(self,mid):
        path=self.ops.media_store.data/(mid+'.jpg');path.write_bytes(b'mock photo')
        with self.ops.store.db:self.ops.store.db.execute('INSERT INTO content_media VALUES(?,?,?,?,?)',
            (mid,path.name,'image/jpeg',10,'test'))

    async def test_photo_and_album_share_exact_topic_and_saved_random_ids(self):
        self.forum()
        for mid in ('a'*32,'b'*32):self.add_media(mid)
        self.c.upload_file=AsyncMock(return_value=types.InputFile(id=1,parts=1,name='test.jpg',md5_checksum=''))
        original=self.c.__class__.__call__
        requests=[]
        class MediaClient(TopicClient):
            async def __call__(inner,request):
                requests.append(request)
                if isinstance(request,functions.messages.UploadMediaRequest):
                    return types.MessageMediaPhoto(photo=types.Photo(id=1,access_hash=2,file_reference=b'x',
                        date=datetime.now(timezone.utc),sizes=[],dc_id=2))
                return await original(inner,request)
        client=MediaClient(self.group);client.upload_file=self.c.upload_file;self.a.client=client
        pub=Publisher(lambda:self.ops.store,lambda:self.a,self.ops.data)
        for media,ids in [(['a'*32],[111]),(['a'*32,'b'*32],[222,333])]:
            await pub.send(dict(media=media,text='caption',silent=False),dict(random_ids=json.dumps(ids)),
                           self.group,topic_id=140)
        writes=[r for r in requests if isinstance(r,(functions.messages.SendMediaRequest,functions.messages.SendMultiMediaRequest))]
        self.assertEqual(len(writes),2)
        self.assertTrue(all(r.reply_to.reply_to_msg_id==140 for r in writes))
        self.assertEqual(writes[0].random_id,111)
        self.assertEqual([m.random_id for m in writes[1].multi_media],[222,333])

    async def test_closes_during_photo_upload_stops_final_rpc(self):
        self.forum();self.add_media('a'*32)
        job=await self.draft(media=['a'*32],destination_topics=self.mapping())
        async def upload(_):
            self.c.topic.closed=True
            return types.InputFile(id=1,parts=1,name='test.jpg',md5_checksum='')
        self.c.upload_file=upload
        await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,(functions.messages.SendMessageRequest,
            functions.messages.SendMediaRequest,functions.messages.SendMultiMediaRequest)) for r in self.c.calls))

    async def test_group_exclusion_during_topic_read_still_wins(self):
        self.forum();job=await self.draft(destination_topics=self.mapping())
        async def exclude():
            await self.ops.workbench.add_exclusion(ExclusionSpec(request_key='late-topic-exclusion-01',
                subject='chat',identifier=str(CID),reason='Excluded during topic read'))
        self.c.topic_hook=exclude
        await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'excluded')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_finite_plan_preserves_topic_snapshots_and_old_job_rows(self):
        self.forum()
        old=await self.draft(destination_topics=self.mapping())
        oldrow=self.ops.store.rows('SELECT * FROM op_jobs WHERE id=?',(old['id'],))
        profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='Topic profile',text='Publication')))['campaign']
        day=(datetime.now(timezone.utc)+timedelta(days=1)).date()
        spec=PlanSpec(request_key='forum-finite-plan-001',account=self.a.aid,telegram_id=100,kind='post',
            title='Finite topics',destinations=[CID],destination_topics=self.mapping(),text='Publication',
            campaign_id=profile['id'],campaign_revision=profile['revision'],start_date=day,end_date=day,times=['12:00'])
        self.c.calls.clear()
        preview=await self.ops.workbench.preview(spec)
        self.assertEqual(preview['destination_topics'],self.mapping());self.assertFalse(self.c.calls)
        saved=await self.ops.workbench.save_plan(spec)
        planned=await self.ops.detail(saved['jobs'][0]['id'])
        self.assertEqual(planned['spec']['destination_topics'],self.mapping())
        self.assertEqual(planned['spec']['topic_snapshots'][0]['topic_id'],140)
        self.assertEqual(planned['items'][0]['topic_title'],'Free ads')
        self.assertEqual((saved['items'][0]['topic_id'],saved['items'][0]['topic_title']),(140,'Free ads'))
        self.assertEqual(saved['topic_snapshots'][0],planned['spec']['topic_snapshots'][0])
        before=self.ops.store.db.total_changes
        report=(await self.ops.workbench.report(saved['id'])).body.decode('utf-8')
        self.assertIn('ID темы;Тема',report);self.assertIn(';140;Free ads;',report)
        self.assertEqual(self.ops.store.db.total_changes,before)
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs WHERE id=?',(old['id'],)),oldrow)
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_other_topic_cannot_bypass_removed_forbidden_or_unknown_cid_hold(self):
        self.forum()
        old=await self.draft(destination_topics=self.mapping())
        olditem=old['items'][0]['id']
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?",(old['id'],))
        cases=[('deletion_observed','sent',None,'publication_removed'),
               (None,'failed','Telegram: ForbiddenError.','previous_forbidden'),
               (None,'uncertain',None,'previous_unknown')]
        for index,(event,state,error,expected) in enumerate(cases):
            with self.subTest(hold=expected):
                with self.ops.store.db:
                    self.ops.store.db.execute('DELETE FROM op_tracking_events WHERE item_id=?',(olditem,))
                    self.ops.store.db.execute('UPDATE op_items SET state=?,error=? WHERE id=?',(state,error,olditem))
                if event:
                    self.ops.tracking._event('topic-hold-event-01',event,job_id=old['id'],item_id=olditem,
                        account=self.a.aid,telegram_id=100,chat_id=CID,message_id=700,attribution='saved_publication')
                self.c.topic=topic(topic_id=141)
                job=await self.draft(request_key='topic-hold-new-key-'+str(index),destination_topics=self.mapping(141))
                self.c.calls.clear()
                await self.start(job);await self.ops.tick()
                item=(await self.ops.detail(job['id']))['items'][0]
                self.assertEqual(item['diagnosis']['code'],expected)
                self.assertIsNone(item['attempted_at']);self.assertFalse(self.c.calls)

    async def test_other_topic_shares_daily_frequency_and_interval(self):
        self.forum();old=await self.draft(destination_topics=self.mapping())
        stamp=datetime.now(timezone.utc).isoformat()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?",(old['id'],))
            self.ops.store.db.execute("UPDATE op_items SET state='sent',attempted_at=?,finished_at=? WHERE job=?",(stamp,stamp,old['id']))
        for index,(maximum,expected) in enumerate([(1,'daily_frequency_reached'),(3,'interval_not_elapsed')]):
            policy=self.policy();policy['destinations'][0]['max_per_day']=maximum
            self.c.topic=topic(topic_id=141)
            job=await self.draft(request_key='topic-frequency-key-'+str(index),destination_topics=self.mapping(141),posting_policy=policy)
            self.c.calls.clear();await self.start(job);await self.ops.tick()
            item=(await self.ops.detail(job['id']))['items'][0]
            self.assertEqual(item['diagnosis']['code'],expected)
            self.assertIsNone(item['attempted_at']);self.assertFalse(self.c.calls)

    async def test_deletion_arriving_during_final_topic_read_stops_send(self):
        self.forum();old=await self.draft(destination_topics=self.mapping())
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_jobs SET state='completed' WHERE id=?",(old['id'],))
            self.ops.store.db.execute("UPDATE op_items SET state='sent' WHERE job=?",(old['id'],))
        self.c.topic=topic(topic_id=141)
        job=await self.draft(request_key='late-topic-deletion-01',destination_topics=self.mapping(141))
        async def deletion():
            self.ops.tracking._event('late-topic-removal-event','deletion_observed',job_id=old['id'],
                item_id=old['items'][0]['id'],account=self.a.aid,telegram_id=100,chat_id=CID,
                message_id=700,attribution='saved_publication')
        self.c.topic_hook=deletion;self.c.calls.clear()
        await self.start(job);await self.ops.tick()
        item=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'publication_removed')
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_corrupt_saved_topic_or_missing_policy_never_falls_back(self):
        self.forum();job=await self.draft(destination_topics=self.mapping())
        for change in ('topic_id','telegram_id','posting_policy','campaign_snapshot'):
            payload=json.loads(json.dumps(job['spec']))
            if change in ('topic_id','telegram_id'):payload['topic_snapshots'][0][change]+=1
            else:payload.pop(change)
            broken=dict(job,spec=payload)
            with self.subTest(change=change),self.assertRaises(HTTPException):
                self.ops.topics.saved(broken,job['items'][0])
        self.assertFalse(any(isinstance(r,functions.messages.SendMessageRequest) for r in self.c.calls))

    async def test_policy_removed_from_persisted_routed_job_stops_worker_before_send(self):
        self.forum();job=await self.draft(destination_topics=self.mapping())
        await self.start(job)
        payload=dict(job['spec']);payload.pop('posting_policy')
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_jobs SET spec=? WHERE id=?',(json.dumps(payload),job['id']))
        self.c.calls.clear()
        with self.assertRaises(HTTPException) as stopped:await self.ops.tick()
        self.assertEqual(stopped.exception.status_code,409)
        self.assertFalse(self.c.calls)

    async def test_plain_plan_report_keeps_legacy_csv_columns(self):
        profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='Plain',text='Publication')))['campaign']
        day=(datetime.now(timezone.utc)+timedelta(days=1)).date()
        spec=PlanSpec(request_key='plain-report-columns-01',account=self.a.aid,telegram_id=100,kind='post',
            title='Plain',destinations=[CID],text='Publication',campaign_id=profile['id'],
            campaign_revision=profile['revision'],start_date=day,end_date=day,times=['12:00'])
        plan=await self.ops.workbench.save_plan(spec)
        self.assertNotIn('topic_snapshots',plan)
        self.assertNotIn('destination_topics',plan['spec'])
        report=(await self.ops.workbench.report(plan['id'])).body.decode('utf-8')
        self.assertEqual(report.splitlines()[0],'\ufeffКампания;Задание;Площадка;Пользователь;Статус;Ошибка;ID сообщений;Завершено')
