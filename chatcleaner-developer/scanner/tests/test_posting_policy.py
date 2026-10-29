import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from pydantic import ValidationError
from telethon import errors, types
from operations import Operations, AccountSpec, JobSpec, Control
from operation_campaigns import CampaignSpec
from posting_policy import PostingPolicy, expiry_decision, publication_decision
from test_operations import Client


CHAT = -1000000000010
OTHER = -1000000000020
AT = datetime.now(timezone.utc).replace(microsecond=0)


def policy(**values):
    result = dict(window_expires_at=(AT + timedelta(hours=1)).isoformat(),
                  campaign_expires_at=(AT + timedelta(days=2)).isoformat(),
                  timezone='Asia/Dubai',
                  destinations=[dict(chat_id=CHAT, max_per_day=3, min_interval_seconds=0)])
    result.update(values)
    return result


class PolicySchemaTests(unittest.TestCase):
    def spec(self, **values):
        data = dict(request_key='finite-policy-test-key', account='owned', kind='post', title='Test',
                    destinations=[CHAT], text='Test', campaign_id=5, campaign_revision=1, posting_policy=policy())
        data.update(values)
        return JobSpec(**data)

    def test_typed_snapshot_and_exact_destinations(self):
        snapshot = self.spec().model_dump(mode='json')['posting_policy']
        self.assertEqual(snapshot['destinations'][0]['max_per_day'], 3)
        for values in [dict(kind='message'), dict(campaign_id=None, campaign_revision=None),
                       dict(destinations=[OTHER]), dict(destinations=[CHAT, CHAT]),
                       dict(posting_policy=policy(unknown_flag=True)),
                       dict(posting_policy=policy(window_expires_at=AT.replace(tzinfo=None).isoformat())),
                       dict(posting_policy=policy(campaign_expires_at=AT.isoformat())),
                       dict(posting_policy=policy(timezone='Invalid/Nowhere'))]:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self.spec(**values)

    def test_strict_group_constraints_and_evidence(self):
        invalid = [dict(max_per_day=4), dict(max_per_day=True), dict(min_interval_seconds=-1),
                   dict(chat_id=True), dict(chat_id=10), dict(allowed_weekdays=[]),
                   dict(allowed_weekdays=[0, 0]), dict(allowed_weekdays=[True]),
                   dict(rule_url='file:///private/session'), dict(rule_url='https://user:secret@example.com/rule'),
                   dict(rule_sha256='wrong'), dict(rule_checked_at=AT.replace(tzinfo=None).isoformat())]
        for update in invalid:
            data = policy()
            data['destinations'][0].update(update)
            with self.subTest(update=update), self.assertRaises(ValidationError):PostingPolicy(**data)
        good = policy()
        good['destinations'][0].update(rule_url='https://t.me/dubai_chat/1', rule_sha256='a' * 64,
                                      rule_checked_at=AT.isoformat(), allowed_weekdays=[0, 3])
        self.assertEqual(PostingPolicy(**good).destinations[0].allowed_weekdays, [0, 3])

    def test_scheduled_window_is_positive_and_at_most_two_hours(self):
        for minutes in [-1, 0, 121]:
            with self.subTest(minutes=minutes), self.assertRaises(ValidationError):
                self.spec(scheduled_at=AT, posting_policy=policy(window_expires_at=(AT + timedelta(minutes=minutes)).isoformat()))
        self.spec(scheduled_at=AT, posting_policy=policy(window_expires_at=(AT + timedelta(hours=2)).isoformat()))


class PolicyQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Operations(self.tmp.name, lambda: {'id':123,'hash':'synthetic-app-hash'})
        self.ops.initialize()
        self.group = types.Channel(id=10,title='Test group',photo=types.ChatPhotoEmpty(),date=AT,
                                   megagroup=True,creator=True,username='dubai_group')
        row = await self.ops.add_account(AccountSpec(label='Owned test',authorized=True))
        self.a = self.ops.accounts[row['id']]
        self.client = Client(self.group)
        self.a.client = self.client
        await self.a.ready()
        self.profile = (await self.ops.campaigns.save_campaign(CampaignSpec(name='Finite',text='Original',
                       delay_seconds=90,daily_limit=50)))['campaign']
        self.seq = 0

    async def asyncTearDown(self):
        await self.ops.shutdown()
        self.tmp.cleanup()

    async def draft(self, **values):
        self.seq += 1
        data = dict(request_key='finite-policy-test-key-' + str(self.seq), account=self.a.aid,kind='post',
                    title='Finite',destinations=[CHAT],text='Original',delay_seconds=90,daily_limit=50,
                    campaign_id=self.profile['id'],campaign_revision=self.profile['revision'],posting_policy=policy())
        data.update(values)
        return await self.ops.create(JobSpec(**data))

    async def start(self, job):
        return await self.ops.control(job['id'], Control(action='start',confirmed=True))

    def clear_wait(self):
        with self.ops.store.db:self.ops.store.db.execute('UPDATE op_accounts SET not_before=0')

    def old(self, *, state='sent', chat=CHAT, tid=100, account=None, error=None, diagnosis=None, attempted=None):
        aid = account or self.a.aid
        with self.ops.store.db:
            cur = self.ops.store.db.execute('''INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated)
              VALUES(?,?,?,?,?,?,?,?,?,?)''', ('old-key-'+str(self.seq)+'-'+str(len(self.ops.store.rows('SELECT id FROM op_jobs'))),
              'digest',aid,tid,'post','Old',json.dumps(dict(daily_limit=50)), 'completed',AT.isoformat(),AT.isoformat()))
            jid = cur.lastrowid
            cur = self.ops.store.db.execute('''INSERT INTO op_items(job,chat_id,chat_title,state,random_ids,error,diagnosis,attempted_at,finished_at)
              VALUES(?,?,?,?,?,?,?,?,?)''', (jid,chat,'Old',state,'[1]',error,json.dumps(diagnosis or {}),
              attempted if attempted is not None else AT.isoformat(), AT.isoformat()))
        return jid, cur.lastrowid

    def event(self, job, item, kind='deletion_observed', *, chat=CHAT, tid=100, account=None):
        return self.ops.tracking._event('fixture-event-'+str(item)+'-'+kind,kind,job_id=job,item_id=item,
              account=account or self.a.aid,telegram_id=tid,chat_id=chat,message_id=700,
              attribution='saved_publication')

    async def test_future_job_then_expired_even_while_disabled_and_on_budget_cooldown(self):
        job = await self.draft(scheduled_at=AT + timedelta(minutes=10))
        await self.start(job)
        await self.ops.tick()
        self.assertFalse(self.client.calls)
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_accounts SET enabled=0,not_before=? WHERE id=?',(AT.timestamp()+86400,self.a.aid))
            self.ops.store.db.execute('UPDATE op_jobs SET not_before=? WHERE id=?',(AT.timestamp()+86400,job['id']))
        with patch('posting_policy.time.time',return_value=AT.timestamp()+3601):await self.ops.tick()
        result = await self.ops.detail(job['id'])
        self.assertEqual((result['state'],result['items'][0]['state']),('cancelled','cancelled'))
        self.assertIsNone(result['items'][0]['attempted_at'])
        self.assertEqual(result['items'][0]['diagnosis']['code'],'window_expired')
        self.assertFalse(self.client.calls)

    async def test_policy_and_campaign_snapshot_are_immutable_and_idempotent(self):
        job = await self.draft(request_key='repeat-exact-policy-key')
        same = await self.draft(request_key='repeat-exact-policy-key')
        self.assertEqual(job['id'],same['id'])
        changed = policy();changed['destinations'][0]['max_per_day']=1
        with self.assertRaises(HTTPException) as conflict:
            await self.draft(request_key='repeat-exact-policy-key',posting_policy=changed)
        self.assertEqual(conflict.exception.status_code,409)
        await self.ops.campaigns.save_campaign(CampaignSpec(id=self.profile['id'],revision=self.profile['revision'],name='Changed',text='Changed'))
        result = await self.ops.detail(job['id'])
        self.assertEqual(result['spec']['campaign_snapshot']['text'],'Original')
        self.assertEqual(result['spec']['posting_policy']['destinations'][0]['max_per_day'],3)

    async def test_deletion_and_missing_block_before_budget_or_rpc(self):
        for kind in ['deletion_observed','missing_on_check','partially_missing']:
            with self.subTest(kind=kind):
                oldjob, olditem = self.old()
                event = self.event(oldjob,olditem,kind)
                job = await self.draft()
                await self.start(job);await self.ops.tick()
                result = (await self.ops.detail(job['id']))['items'][0]
                self.assertEqual(result['state'],'blocked')
                self.assertIsNone(result['attempted_at'])
                self.assertEqual(result['diagnosis']['evidence_event_id'],event['id'])
                self.assertFalse(self.client.calls)
                with self.ops.store.db:self.ops.store.db.execute('DELETE FROM op_tracking_events WHERE id=?',(event['id'],))

    async def test_other_identity_chat_and_panel_removal_do_not_block(self):
        for chat,tid in [(OTHER,100),(CHAT,999)]:
            oldjob,olditem=self.old(chat=chat,tid=tid)
            self.event(oldjob,olditem,chat=chat,tid=tid)
        oldjob,olditem=self.old(state='removed',attempted=(AT-timedelta(days=2)).isoformat())
        self.event(oldjob,olditem,kind='removed_by_panel')
        job=await self.draft();await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'sent')
        self.assertEqual(len(self.client.calls),1)

    async def test_foreign_account_history_with_same_old_telegram_id_does_not_apply(self):
        foreign=await self.ops.add_account(AccountSpec(label='Other owned account',authorized=True))
        oldjob,olditem=self.old(account=foreign['id'],tid=100,state='failed',error='Telegram: ForbiddenError.')
        self.event(oldjob,olditem,account=foreign['id'],tid=100)
        job=await self.draft();await self.start(job);await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'sent')
        self.assertEqual(len(self.client.calls),1)

    async def test_historical_forbidden_and_uncertain_stop_new_job_without_retries(self):
        oldjob,olditem=self.old(state='failed',error='Telegram: ForbiddenError.')
        job=await self.draft();await self.start(job);await self.ops.tick()
        item=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'previous_forbidden')
        self.assertIsNone(item['attempted_at']);self.assertFalse(self.client.calls)
        with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET error=NULL,state='uncertain' WHERE id=?",(olditem,))
        new=await self.draft();await self.start(new);await self.ops.tick()
        item=(await self.ops.detail(new['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'previous_unknown')
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_items WHERE id=?',(olditem,))[0]['state'],'uncertain')
        self.assertFalse(self.client.calls)

    async def test_daily_frequency_uses_group_timezone_across_jobs_and_preserves_history(self):
        # 21 UTC belongs to the next Dubai calendar day; same local day counts.
        stamp=datetime(2026,10,19,1,0,tzinfo=timezone.utc)
        oldjob,olditem=self.old(attempted='2026-10-18T21:00:00+00:00')
        item=dict(id=999,chat_id=CHAT)
        j=dict(account=self.a.aid,telegram_id=100,spec={'posting_policy':policy(
               window_expires_at=(stamp+timedelta(hours=1)).isoformat(),
               campaign_expires_at=(stamp+timedelta(days=2)).isoformat())})
        j['spec']['posting_policy']['destinations'][0]['max_per_day']=1
        decision=publication_decision(self.ops.store,j,item,stamp.timestamp())
        self.assertEqual(decision['diagnosis']['code'],'daily_frequency_reached')
        self.assertEqual(decision['diagnosis']['count_today'],1)
        self.assertEqual(self.ops.store.rows('SELECT state FROM op_items WHERE id=?',(olditem,))[0]['state'],'sent')

    async def test_minimum_interval_and_weekday_constraints(self):
        self.old(attempted=(AT-timedelta(seconds=60)).isoformat())
        job=await self.draft()
        p=job['spec']['posting_policy']['destinations'][0]
        p['min_interval_seconds']=3600
        self.assertEqual(publication_decision(self.ops.store,job,job['items'][0],AT.timestamp())['diagnosis']['code'],'interval_not_elapsed')
        from zoneinfo import ZoneInfo
        p['allowed_weekdays']=[(AT.astimezone(ZoneInfo('Asia/Dubai')).weekday()+1)%7]
        self.assertEqual(publication_decision(self.ops.store,job,job['items'][0],AT.timestamp())['diagnosis']['code'],'weekday_not_allowed')

    async def test_deletion_during_peer_resolution_stops_last_write_guard(self):
        oldjob,olditem=self.old(attempted=(AT-timedelta(days=2)).isoformat())
        job=await self.draft();await self.start(job)
        original=self.client.get_input_entity
        async def resolve(entity):
            self.event(oldjob,olditem)
            return await original(entity)
        self.client.get_input_entity=resolve
        await self.ops.tick()
        result=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(result['state'],'blocked')
        self.assertEqual(result['diagnosis']['code'],'publication_removed')
        self.assertFalse(self.client.calls)

    async def test_expiry_during_peer_resolution_stops_last_write_guard(self):
        job=await self.draft();await self.start(job)
        original=self.client.get_input_entity
        # Advance the clock between claim and final guard, without changing the immutable spec.
        async def resolve_with_clock(entity):
            self.clock=patch('posting_policy.time.time',return_value=AT.timestamp()+3601)
            self.clock.start()
            return await original(entity)
        self.client.get_input_entity=resolve_with_clock
        try:await self.ops.tick()
        finally:self.clock.stop()
        result=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(result['diagnosis']['code'],'window_expired');self.assertFalse(self.client.calls)

    async def test_pause_and_identity_replacement_during_peer_resolution_never_write(self):
        job=await self.draft();await self.start(job)
        original=self.client.get_input_entity
        async def resolve(entity):
            await self.ops.control(job['id'],Control(action='pause'))
            return await original(entity)
        self.client.get_input_entity=resolve
        await self.ops.tick()
        item=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'operation_stopped');self.assertFalse(self.client.calls)
        self.clear_wait()
        follow=await self.draft();await self.start(follow)
        async def replace(entity):
            self.a.me=types.User(id=999,first_name='Changed')
            return await original(entity)
        self.client.get_input_entity=replace
        await self.ops.tick()
        item=(await self.ops.detail(follow['id']))['items'][0]
        # The runtime guard may reject first; either path is a known refusal, no write.
        self.assertIn(item['state'],['failed','blocked']);self.assertFalse(self.client.calls)

    async def test_aggregate_fifty_cap_and_ninety_second_spacing_are_preserved(self):
        for _ in range(49):self.old(chat=OTHER)
        first=await self.draft();follow=await self.draft()
        await self.start(first);await self.start(follow);await self.ops.tick()
        self.assertEqual(len(self.client.calls),1)
        self.assertGreaterEqual(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+88)
        await self.ops.tick()
        self.assertEqual(len(self.client.calls),1)
        self.clear_wait();await self.ops.tick()
        pending=(await self.ops.detail(follow['id']))['items'][0]
        self.assertEqual(pending['state'],'pending');self.assertIsNone(pending['attempted_at'])
        self.assertEqual(self.ops.account_daily_count(self.a.aid,AT.isoformat()[:10]),50)
        with patch('posting_policy.time.time',return_value=AT.timestamp()+7201):await self.ops.tick()
        self.assertEqual((await self.ops.detail(follow['id']))['items'][0]['state'],'cancelled')
        self.assertEqual(len(self.client.calls),1)

    async def test_floodwait_pauses_all_future_windows_without_auto_resume(self):
        first=await self.draft();await self.start(first)
        future=await self.draft(scheduled_at=AT+timedelta(minutes=10));await self.start(future)
        self.client.failure=errors.FloodWaitError(None,capture=120)
        await self.ops.tick();await self.ops.tick()
        self.assertEqual((await self.ops.detail(first['id']))['state'],'paused')
        self.assertEqual((await self.ops.detail(future['id']))['state'],'paused')
        self.assertEqual(len(self.client.calls),1)
        self.assertGreater(self.ops.account_row(self.a.aid)['not_before'],datetime.now(timezone.utc).timestamp()+118)

    async def test_forbidden_rpc_metadata_stops_a_later_window(self):
        job=await self.draft();await self.start(job)
        self.client.failure=errors.ForbiddenError(None,'FORBIDDEN',code=403)
        await self.ops.tick();self.clear_wait()
        first=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(first['diagnosis']['rpc']['code'],403)
        self.client.failure=None
        follow=await self.draft();await self.start(follow);await self.ops.tick()
        last=(await self.ops.detail(follow['id']))['items'][0]
        self.assertEqual(last['diagnosis']['code'],'previous_forbidden')
        self.assertEqual(len(self.client.calls),1)

    async def test_live_rights_forbidden_is_saved_and_stops_a_later_window(self):
        job=await self.draft(permission_note='Owner reviewed permitted advertisements')
        await self.start(job)
        self.group.creator=False
        self.group.default_banned_rights=types.ChatBannedRights(until_date=AT+timedelta(days=2),send_messages=True)
        await self.ops.tick();self.clear_wait()
        first=(await self.ops.detail(job['id']))['items'][0]
        self.assertEqual(first['diagnosis']['http_status'],403)
        self.group.default_banned_rights=None
        follow=await self.draft(permission_note='Owner reviewed permitted advertisements')
        await self.start(follow);await self.ops.tick()
        item=(await self.ops.detail(follow['id']))['items'][0]
        self.assertEqual(item['diagnosis']['code'],'previous_forbidden');self.assertFalse(self.client.calls)

    async def test_expiry_never_changes_sending_or_uncertain_history(self):
        job=await self.draft();await self.start(job)
        with self.ops.store.db:self.ops.store.db.execute("UPDATE op_items SET state='uncertain',attempted_at=? WHERE job=?",(AT.isoformat(),job['id']))
        with patch('posting_policy.time.time',return_value=AT.timestamp()+3601):await self.ops.tick()
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'],'uncertain')
        self.assertFalse(self.client.calls)

    async def test_legacy_post_remains_unchanged(self):
        oldjob,olditem=self.old();self.event(oldjob,olditem)
        job=await self.draft(posting_policy=None);await self.start(job);await self.ops.tick()
        result=await self.ops.detail(job['id'])
        self.assertNotIn('posting_policy',result['spec'])
        self.assertEqual(result['items'][0]['state'],'sent')


if __name__=='__main__':unittest.main()
