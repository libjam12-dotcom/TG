import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from telethon import types
from engine import now
from operation_campaigns import (ArchiveSpec, CampaignSpec, OperationCampaigns,
                                ReplyTemplateArchive, ReplyTemplateSpec, classify_reply)
from operations import JobSpec, Operations


CHAT = -1000000000010


class Client:
    def __init__(self):
        self.handlers, self.writes, self.reads = [], [], []

    def is_connected(self):
        return True

    async def get_entity(self, chat):
        self.reads.append(chat)
        return types.Channel(id=10, title='Private test group', photo=types.ChatPhotoEmpty(),
                             date=datetime.now(timezone.utc), megagroup=True, creator=True)

    def add_event_handler(self, fn, builder):
        self.handlers.append(fn)

    def remove_event_handler(self, fn):
        self.handlers = [handler for handler in self.handlers if handler != fn]

    async def disconnect(self):
        return None

    async def __call__(self, request):
        self.writes.append(request)
        raise AssertionError('Settings and draft creation must never write to Telegram')


class CampaignTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Operations(self.tmp.name, lambda: None)
        self.ops.initialize()
        self.campaigns = self.ops.campaigns
        for aid, tid in [('a', 100), ('b', 200)]:
            with self.ops.store.db:
                self.ops.store.db.execute('INSERT INTO op_accounts(id,label,telegram_id,enabled,created) VALUES(?,?,?,1,?)', (aid, aid, tid, now()))
            self.ops.accounts[aid] = SimpleNamespace(aid=aid, client=Client(), me=SimpleNamespace(id=tid))

    async def asyncTearDown(self):
        await self.ops.shutdown()
        self.tmp.cleanup()

    async def profile(self, **values):
        args = dict(name='UFC 333', text='Tickets from $750, contact @seller',
                    keywords=[' БИЛЕТ ', 'цена', 'Билет'], stop_words=[' Реклама '], contact='@seller')
        args.update(values)
        return (await self.campaigns.save_campaign(CampaignSpec(**args)))['campaign']

    async def template(self, aid='a', telegram_id=100, **values):
        args = dict(telegram_id=telegram_id, name='Стоимость', text='Категории от $750. Что вас интересует?')
        args.update(values)
        return (await self.campaigns.save_template(aid, ReplyTemplateSpec(**args)))['template']

    def job_spec(self, **values):
        args = dict(request_key='campaign-job-unique-key-0001', account='a', kind='post',
                    title='Edited offer', destinations=[CHAT], text='Edited draft @seller',
                    delay_seconds=45, daily_limit=3)
        args.update(values)
        return JobSpec(**args)

    async def test_profile_save_is_persistent_and_has_no_business_side_effects(self):
        before = {table: self.ops.store.rows('SELECT * FROM ' + table) for table in ['op_accounts', 'op_jobs', 'op_items', 'settings']}
        profile = await self.profile()
        self.assertEqual(profile['revision'], 1)
        self.assertEqual(profile['keywords'], ['билет', 'цена'])
        self.assertEqual(profile['stop_words'], ['реклама'])
        self.assertNotIn('account', profile)
        self.assertNotIn('destinations', profile)
        restarted = OperationCampaigns(self.ops)
        restarted.initialize()
        self.assertEqual((await restarted.list_campaigns())['campaigns'], [profile])
        self.assertEqual(before, {table: self.ops.store.rows('SELECT * FROM ' + table) for table in before})
        self.assertTrue(all(not a.client.writes and not a.client.reads for a in self.ops.accounts.values()))

    async def test_records_and_job_snapshot_survive_database_close_and_reopen(self):
        profile, template = await self.profile(), await self.template()
        job = await self.ops.create(self.job_spec(campaign_id=profile['id'], campaign_revision=1))
        await self.ops.shutdown()
        self.ops = Operations(self.tmp.name, lambda: None)
        self.ops.initialize()
        self.campaigns = self.ops.campaigns
        self.assertEqual((await self.campaigns.list_campaigns())['campaigns'], [profile])
        self.assertEqual((await self.campaigns.list_templates('a'))['templates'], [template])
        self.assertEqual((await self.ops.detail(job['id']))['spec']['campaign_snapshot'], profile)
        updated = await self.profile(id=profile['id'], revision=1, name='After restart')
        self.assertEqual(updated['revision'], 2)
        self.assertEqual((await self.ops.detail(job['id']))['spec']['campaign_snapshot'], profile)

    async def test_revision_cas_and_archive_restore_are_explicit(self):
        profile = await self.profile()
        updated = await self.profile(id=profile['id'], revision=1, name='Revised')
        self.assertEqual(updated['revision'], 2)
        with self.assertRaises(HTTPException) as stale:
            await self.profile(id=profile['id'], revision=1, text='Lost tab')
        self.assertEqual(stale.exception.status_code, 409)
        archived = (await self.campaigns.archive_campaign(profile['id'], ArchiveSpec(revision=2, archived=True)))['campaign']
        self.assertTrue(archived['archived'])
        self.assertEqual((await self.campaigns.list_campaigns())['campaigns'], [])
        self.assertEqual(len((await self.campaigns.list_campaigns(True))['campaigns']), 1)
        with self.assertRaises(HTTPException):
            await self.profile(id=profile['id'], revision=3)
        with self.assertRaises(HTTPException):
            await self.campaigns.archive_campaign(profile['id'], ArchiveSpec(revision=2, archived=False))
        restored = (await self.campaigns.archive_campaign(profile['id'], ArchiveSpec(revision=3, archived=False)))['campaign']
        self.assertEqual(restored['revision'], 4)
        self.assertFalse(restored['archived'])

    async def test_concurrent_revision_updates_cannot_overwrite_each_other(self):
        profile = await self.profile()
        results = await asyncio.gather(self.profile(id=profile['id'], revision=1, name='First'),
                                       self.profile(id=profile['id'], revision=1, name='Second'), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, dict) for result in results), 1)
        self.assertEqual(sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results), 1)

    async def test_content_validation_uses_telegram_units_and_uploaded_media(self):
        mid = 'a' * 32
        self.ops.media_store.data.joinpath(mid + '.png').write_bytes(b'fake fixture photo')
        with self.ops.store.db:
            self.ops.store.db.execute('INSERT INTO content_media VALUES(?,?,?,?,?)', (mid, mid + '.png', 'image/png', 18, now()))
        photo_profile = await self.profile(text='😀' * 512, media=[mid])
        self.assertEqual(photo_profile['media'], [mid])
        for values in [dict(text=''), dict(name=' '), dict(text='😀' * 2049), dict(text='😀' * 513, media=[mid]),
                       dict(media=[mid, mid]), dict(media=['../escape']), dict(media=['b' * 32]),
                       dict(keywords=[' ']), dict(stop_words=['x' * 81])]:
            with self.subTest(values=values):
                with self.assertRaises(HTTPException):
                    await self.profile(**values)
        self.ops.media_store.data.joinpath(mid + '.png').unlink()
        with self.assertRaises(HTTPException) as missing:
            await self.profile(media=[mid])
        self.assertEqual(missing.exception.status_code, 400)
        self.assertEqual(len((await self.campaigns.list_campaigns())['campaigns']), 1)

    async def test_untrusted_fields_pairs_and_bounds_rejected_by_schema(self):
        for values in [dict(account='a'), dict(destinations=[CHAT]), dict(auto_reply=True), dict(id=1),
                       dict(revision=1), dict(daily_limit=101), dict(delay_seconds=29), dict(keywords=['a'] * 51),
                       dict(contact='\ud800'), dict(keywords=['\udfff'])]:
            with self.subTest(values=values):
                with self.assertRaises(ValidationError):
                    CampaignSpec(name='Test', text='Offer', **values)
        for values in [dict(campaign_id=1), dict(campaign_revision=1), dict(campaign_snapshot={}),
                       dict(campaign_id=1, campaign_revision=1, kind='message')]:
            with self.subTest(values=values):
                with self.assertRaises(ValidationError):
                    self.job_spec(**values)
        with self.assertRaises(ValidationError):
            ReplyTemplateSpec(telegram_id=100, name='Invalid unicode', text='\ud800')

    async def test_job_copies_profile_but_preserves_explicit_draft_override(self):
        profile = await self.profile()
        spec = self.job_spec(campaign_id=profile['id'], campaign_revision=1)
        job = await self.ops.create(spec)
        self.assertEqual(job['state'], 'draft')
        self.assertEqual(job['spec']['text'], 'Edited draft @seller')
        self.assertEqual(job['spec']['daily_limit'], 3)
        self.assertEqual(job['spec']['campaign_snapshot'], profile)
        self.assertNotEqual(job['spec']['text'], job['spec']['campaign_snapshot']['text'])
        await self.profile(id=profile['id'], revision=1, keywords=['изменено'], contact='@another')
        await self.campaigns.archive_campaign(profile['id'], ArchiveSpec(revision=2, archived=True))
        replay = await self.ops.create(spec)
        self.assertEqual(replay['id'], job['id'])
        self.assertEqual(replay['spec']['campaign_snapshot'], profile)
        self.assertEqual(len(self.ops.store.rows('SELECT id FROM op_items')), 1)
        self.assertTrue(all(not a.client.writes for a in self.ops.accounts.values()))
        self.campaigns.initialize()
        self.assertEqual((await self.ops.detail(job['id']))['spec']['campaign_snapshot'], profile)

    async def test_stale_missing_archived_profile_rejected_before_recipient_access(self):
        profile = await self.profile()
        await self.profile(id=profile['id'], revision=1, name='Changed')
        for cid, revision in [(profile['id'], 1), (999, 1)]:
            with self.assertRaises(HTTPException):
                await self.ops.create(self.job_spec(campaign_id=cid, campaign_revision=revision))
        await self.campaigns.archive_campaign(profile['id'], ArchiveSpec(revision=2, archived=True))
        with self.assertRaises(HTTPException):
            await self.ops.create(self.job_spec(campaign_id=profile['id'], campaign_revision=3))
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertFalse(self.ops.accounts['a'].client.reads)

    async def test_legacy_digest_and_conflicting_key_behavior_are_preserved(self):
        spec = self.job_spec()
        # Fixed pre-topic API payload: do not derive this compatibility fixture
        # from today's model defaults, which gain new optional fields over time.
        legacy = dict(request_key='campaign-job-unique-key-0001', account='a', kind='post',
            title='Edited offer', destinations=[CHAT], text='Edited draft @seller', media=[], users=[],
            audience_run_id=None, candidate_ids=[], permission_note='', consent_note='', delay_seconds=45,
            daily_limit=3, scheduled_at=None, link_hours=24, request_needed=True, invite_url='')
        expected = '41acbfc97d2df9e0f139ba97fad57effcab287fd4e4274167b3d86067a5f809f'
        self.assertEqual(hashlib.sha256(json.dumps(legacy, sort_keys=True).encode()).hexdigest(), expected)
        job = await self.ops.create(spec)
        saved = self.ops.store.rows('SELECT digest,spec FROM op_jobs WHERE id=?', (job['id'],))[0]
        self.assertEqual(saved['digest'], expected)
        self.assertNotIn('campaign_id', json.loads(saved['spec']))
        self.assertNotIn('posting_policy', json.loads(saved['spec']))
        self.assertNotIn('destination_topics', json.loads(saved['spec']))
        self.assertEqual((await self.ops.create(spec))['id'], job['id'])
        profile = await self.profile()
        with self.assertRaises(HTTPException) as conflict:
            await self.ops.create(self.job_spec(campaign_id=profile['id'], campaign_revision=1))
        self.assertEqual(conflict.exception.status_code, 409)

    async def test_templates_are_scoped_to_account_and_current_telegram_identity(self):
        template = await self.template()
        other = await self.template(aid='b', telegram_id=200, text='Only account B')
        self.assertEqual((await self.campaigns.list_templates('a'))['templates'], [template])
        self.assertEqual((await self.campaigns.list_templates('b'))['templates'], [other])
        with self.assertRaises(HTTPException) as wrong_account:
            await self.template(aid='b', telegram_id=200, id=template['id'], revision=1)
        self.assertEqual(wrong_account.exception.status_code, 404)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET telegram_id=300 WHERE id='a'")
        self.assertEqual((await self.campaigns.list_templates('a'))['templates'], [])
        with self.assertRaises(HTTPException) as replaced:
            await self.template()
        self.assertEqual(replaced.exception.status_code, 409)
        with self.assertRaises(HTTPException):
            await self.template(telegram_id=300, id=template['id'], revision=1)
        new_template = await self.template(telegram_id=300, text='Only new identity')
        self.campaigns.initialize()
        self.assertEqual((await self.campaigns.list_templates('a'))['templates'], [new_template])

    async def test_template_cas_archive_and_utf16_validation(self):
        template = await self.template()
        updated = await self.template(id=template['id'], revision=1, text='Updated manual reply')
        self.assertEqual(updated['revision'], 2)
        with self.assertRaises(HTTPException):
            await self.template(id=template['id'], revision=1)
        for values in [dict(text='😀' * 2049), dict(text=' '), dict(name=' ')]:
            with self.assertRaises(HTTPException):
                await self.template(**values)
        archived = await self.campaigns.archive_template('a', template['id'], ReplyTemplateArchive(telegram_id=100, revision=2, archived=True))
        self.assertTrue(archived['template']['archived'])
        self.assertFalse((await self.campaigns.list_templates('a'))['templates'])
        self.assertEqual(len((await self.campaigns.list_templates('a', True))['templates']), 1)
        self.assertTrue(all(not a.client.writes for a in self.ops.accounts.values()))

    async def test_unconnected_account_can_edit_its_saved_identity_templates_without_rpc(self):
        self.ops.accounts.pop('a')
        template = await self.template()
        self.assertEqual((await self.campaigns.list_templates('a'))['templates'], [template])
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET telegram_id=NULL WHERE id='a'")
        self.assertIsNone((await self.campaigns.list_templates('a'))['telegram_id'])
        with self.assertRaises(HTTPException):
            await self.template()

    async def test_exact_reply_labels_use_immutable_profile_never_discard_events(self):
        profile = await self.profile()
        job = await self.ops.create(self.job_spec(campaign_id=profile['id'], campaign_revision=1))
        await self.profile(id=profile['id'], revision=1, keywords=['different'], stop_words=[])
        item = job['items'][0]
        for mid, text in [(1, 'Есть БИЛЕТ?'), (2, 'цена? РЕКЛАМА'), (3, 'Спасибо'), (4, 'билетик')]:
            self.ops.tracking._event('fixture:' + str(mid), 'group_reply', job_id=job['id'], item_id=item['id'],
                account='a', telegram_id=100, chat_id=CHAT, message_id=mid, text=text)
        # Deliberately include an unknown-source DM in this journal fixture: it
        # must not inherit campaign attribution or a keyword label.
        self.ops.tracking._event('fixture:dm', 'incoming_dm', job_id=job['id'], account='a', telegram_id=100,
                                 chat_id=555, message_id=10, text='билет', attribution='unknown')
        result = await self.ops.tracking.job(job['id'])
        self.assertEqual(result['campaign']['revision'], 1)
        replies = [event for event in result['events'] if event['kind'] == 'group_reply']
        self.assertEqual([e['classification']['status'] for e in replies], ['keyword_match', 'stop_word', 'no_keyword_match', 'no_keyword_match'])
        self.assertEqual(result['classification_summary'], dict(scope='visible_events', counts=dict(keyword_match=1, stop_word=1, no_keyword_match=2)))
        self.assertNotIn('classification', next(event for event in result['events'] if event['kind'] == 'incoming_dm'))
        self.assertEqual(len(result['events']), 5)
        self.assertEqual(result['summary']['group_reply'], 4)
        self.assertEqual((await self.ops.detail(job['id']))['items'][0]['state'], 'pending')

    async def test_routes_expose_validation_and_cas_without_sending(self):
        app = FastAPI()
        app.include_router(self.ops.router)
        with TestClient(app) as client:
            first = client.post('/api/scanner/operations/campaigns', json={'name': 'Route', 'text': 'Offer'})
            self.assertEqual(first.status_code, 200)
            profile = first.json()['campaign']
            self.assertEqual(client.get('/api/scanner/operations/campaigns').json()['campaigns'][0]['id'], profile['id'])
            self.assertEqual(client.post('/api/scanner/operations/campaigns', json={'name': 'Bad', 'text': 'Offer', 'destinations': [CHAT]}).status_code, 422)
            payload = dict(id=profile['id'], revision=profile['revision'], name='Route2', text='Changed')
            self.assertEqual(client.post('/api/scanner/operations/campaigns', json=payload).status_code, 200)
            self.assertEqual(client.post('/api/scanner/operations/campaigns', json=payload).status_code, 409)
            self.assertEqual(client.post('/api/scanner/operations/accounts/a/reply-templates', json=dict(telegram_id=200, name='Wrong', text='Draft')).status_code, 409)
            saved = client.post('/api/scanner/operations/accounts/a/reply-templates', json=dict(telegram_id=100, name='Manual', text='Draft'))
            self.assertEqual(saved.status_code, 200)
            self.assertEqual(client.get('/api/scanner/operations/accounts/b/reply-templates').json()['templates'], [])
        self.assertTrue(all(not a.client.writes for a in self.ops.accounts.values()))


class ClassificationTests(unittest.TestCase):
    def test_matching_reuses_unicode_literal_phrase_rules_and_stopword_priority(self):
        profile = dict(keywords=['билет', 'ufc 333', 'a+b'], stop_words=['не интересно'])
        self.assertEqual(classify_reply('БИЛЕТ на UFC\n333?', profile)['matched_keywords'], ['билет', 'ufc 333'])
        self.assertEqual(classify_reply('Не интересно, билет', profile)['status'], 'stop_word')
        self.assertEqual(classify_reply('A+B', profile)['status'], 'keyword_match')
        self.assertEqual(classify_reply('AXXB', profile)['status'], 'no_keyword_match')
        self.assertEqual(classify_reply('билетики', profile)['status'], 'no_keyword_match')
        self.assertEqual(classify_reply('Thank you', dict(keywords=[], stop_words=[]))['status'], 'unfiltered')


if __name__ == '__main__':
    unittest.main()
