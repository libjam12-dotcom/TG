import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from telethon import errors, functions, types, utils
from telethon.tl.custom.participantpermissions import ParticipantPermissions
from telethon.crypto import AuthKey
from operations import Operations, AccountSpec, JobSpec, Control
from audience import AudienceSpec, AudienceSelection, AudienceControl, AudienceDraft, ParseSpec
from engine import Store


def group(cid, title, **kw):
    return types.Channel(id=cid, title=title, photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc), megagroup=True, **kw)


class Client:
    def __init__(self):
        self.session = SimpleNamespace(auth_key=AuthKey(b'1'*256))
        self.target = group(10, 'Our group', creator=True, username='our_group')
        self.source = group(20, 'Дубай маркетинг', username='dubai_marketing', participants_count=100)
        self.other = group(21, 'Москва маркетинг', username='msk_marketing')
        self.admin = group(22, 'Дубай own', username='own_dubai', creator=True)
        self.users = [types.User(id=200, first_name='Alice', username='alice_test', access_hash=123), types.User(id=201, first_name='No handle', access_hash=124), types.User(id=202, first_name='Bot', bot=True), types.User(id=203, deleted=True), types.User(id=100, first_name='Self', access_hash=125), types.User(id=204, first_name='Existing', access_hash=126)]
        self.members = {204}; self.calls = []; self.hidden = False; self.flood = False
        self.get_me = AsyncMock(return_value=types.User(id=100, username='our_account'))
        self.disconnect = AsyncMock()
    def is_connected(self): return True
    async def get_entity(self, ref):
        if isinstance(ref, types.InputPeerUser):
            return next(u for u in self.users if u.id == ref.user_id and u.access_hash == ref.access_hash)
        if ref == utils.get_peer_id(self.target): return self.target
        if ref == self.source.username: return self.source
        return next(u for u in self.users if u.username == ref or u.id == ref)
    async def get_permissions(self, entity, user):
        if user.id not in self.members: raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChannelParticipant(user_id=user.id,date=datetime.now(timezone.utc)),False)
    async def get_messages(self, entity, limit, offset_id):
        if offset_id: return []
        result=[]
        for i, user in enumerate(self.users):
            result.append(SimpleNamespace(id=100-i, date=datetime.now(timezone.utc), message='Нужен маркетинг', get_sender=AsyncMock(return_value=user)))
        result.append(SimpleNamespace(id=90, date=datetime.now(timezone.utc), message='Нужен маркетинг', get_sender=AsyncMock(return_value=self.source)))
        return result
    async def __call__(self, request):
        self.calls.append(request)
        if self.flood: raise errors.FloodWaitError(None, capture=10)
        if isinstance(request, functions.contacts.SearchRequest): return SimpleNamespace(chats=[self.source, self.other, self.admin, self.target])
        if isinstance(request, functions.channels.GetParticipantsRequest):
            if self.hidden: raise errors.ChatAdminRequiredError(None)
            return SimpleNamespace(users=self.users, participants=[SimpleNamespace(user_id=u.id) for u in self.users])
        if isinstance(request, (functions.channels.InviteToChannelRequest, functions.messages.AddChatUserRequest)):
            for user in request.users: self.members.add(user.id)
            return SimpleNamespace(missing_invitees=[])
        raise AssertionError(type(request))


class AudienceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.ops=Operations(self.tmp.name, lambda:{'id':1,'hash':'test'}); self.ops.initialize()
        row=await self.ops.add_account(AccountSpec(label='Test', authorized=True)); self.a=self.ops.accounts[row['id']]; self.client=Client(); self.a.client=self.client; await self.a.ready()
        self.aud=self.ops.audience; self.aud.pace=AsyncMock()
    async def asyncTearDown(self): await self.ops.shutdown(); self.tmp.cleanup()
    async def make_run(self, **kw):
        spec=dict(account=self.a.aid, target=utils.get_peer_id(self.client.target), title='Маркетинг', topic='Маркетинг', geography='Дубай', queries=['маркетинг дубай'], keywords='маркетинг', per_source=10)
        spec.update(kw); return await self.aud.create(AudienceSpec(**spec))
    async def search(self, row):
        await self.aud.control(row['id'], AudienceControl(action='search')); await self.aud.task
        result=await self.aud.detail(row['id']); await self.aud.select(row['id'], AudienceSelection(source_ids=[self.client.source.id*-1-1000000000000])); return result
    async def collect(self, row):
        await self.search(row); await self.aud.control(row['id'], AudienceControl(action='collect')); await self.aud.task
        return await self.aud.detail(row['id'])
    async def test_selection_preview_draft_and_explicit_direct_add(self):
        row=await self.make_run(); result=await self.collect(row)
        self.assertEqual(len(result['sources']),1); self.assertEqual(result['stats']['eligible'],2); self.assertEqual(result['stats']['excluded'],4)
        self.assertNotIn('binding',result); self.assertNotIn('access_hash',json.dumps(result))
        self.assertFalse(any(isinstance(r,functions.channels.InviteToChannelRequest) for r in self.client.calls))
        ids=[p['id'] for p in result['candidates'] if p['eligible']]; await self.aud.select(row['id'],AudienceSelection(candidate_ids=ids))
        draft=await self.aud.draft(row['id'],AudienceDraft(request_key='audience-test-key-001',consent_note='Synthetic consenting registrants'))
        self.assertEqual(draft['state'],'draft'); self.assertEqual({p['user_id'] for p in draft['items']},{200,201})
        self.assertNotIn('access_hash',json.dumps(draft)); self.assertNotIn('binding',json.dumps(draft))
        self.assertEqual((await self.aud.draft(row['id'],AudienceDraft(request_key='audience-test-key-001',consent_note='Synthetic consenting registrants')))['id'],draft['id'])
        with self.assertRaises(HTTPException): await self.aud.draft(row['id'],AudienceDraft(request_key='audience-test-key-002',consent_note='Synthetic consenting registrants'))
        await self.ops.control(draft['id'],Control(action='start',confirmed=True)); await self.ops.tick()
        self.assertEqual((await self.ops.detail(draft['id']))['items'][0]['state'],'invited')
        calls=[r for r in self.client.calls if isinstance(r,functions.channels.InviteToChannelRequest)]; self.assertEqual(len(calls),1)
        with self.ops.store.db: self.ops.store.db.execute('UPDATE op_accounts SET not_before=0')
        await self.ops.tick(); self.assertIn(201,self.client.members)
    async def test_hidden_members_are_reported_without_fallback(self):
        row=await self.make_run(method='members'); self.client.hidden=True; result=await self.collect(row)
        self.assertEqual(result['sources'][0]['state'],'unavailable'); self.assertEqual(result['stats']['available'],0)
        self.assertEqual(result['phase'],'review_people')
    async def test_members_evidence_is_not_demand_or_consent(self):
        row=await self.make_run(method='members'); result=await self.collect(row)
        self.assertEqual(result['candidates'][0]['evidence'][0]['method'],'members')
        self.assertIn('не подтверждены',result['candidates'][0]['reason'])
    async def test_cross_session_and_target_cannot_reuse_people(self):
        row=await self.make_run(); result=await self.collect(row); ids=[p['id'] for p in result['candidates'] if p['eligible']]; await self.aud.select(row['id'],AudienceSelection(candidate_ids=ids))
        with self.assertRaises(HTTPException): await self.aud.resolve_selected(row['id'],ids,self.a.aid,-123)
        self.client.session.auth_key=AuthKey(b'2'*256)
        with self.assertRaises(HTTPException): await self.aud.resolve_selected(row['id'],ids,self.a.aid,row['target'])
        self.assertFalse(any(isinstance(r,functions.channels.InviteToChannelRequest) for r in self.client.calls))
    async def test_draft_rechecks_session_at_delivery(self):
        row=await self.make_run(); result=await self.collect(row); await self.aud.select(row['id'],AudienceSelection(candidate_ids=[result['candidates'][0]['id']]))
        draft=await self.aud.draft(row['id'],AudienceDraft(request_key='audience-test-key-003',consent_note='Synthetic consenting registrants'))
        await self.ops.control(draft['id'],Control(action='start',confirmed=True)); self.client.session.auth_key=AuthKey(b'3'*256); await self.ops.tick()
        self.assertEqual((await self.ops.detail(draft['id']))['items'][0]['state'],'failed')
        self.assertFalse(any(isinstance(r,functions.channels.InviteToChannelRequest) for r in self.client.calls))
    async def test_restricted_target_and_ineligible_selection(self):
        self.client.target.creator=False
        with self.assertRaises(HTTPException): await self.make_run()
        self.client.target.creator=True; row=await self.make_run(); result=await self.collect(row)
        bad=[p['id'] for p in result['candidates'] if not p['eligible']]
        with self.assertRaises(HTTPException): await self.aud.select(row['id'],AudienceSelection(candidate_ids=bad))
        with self.assertRaises(HTTPException): await self.aud.select(row['id'],AudienceSelection(source_ids=[123]))
    async def test_flood_wait_pause_and_no_rotation(self):
        row=await self.make_run(); self.client.flood=True; await self.aud.control(row['id'],AudienceControl(action='search')); await self.aud.task
        result=await self.aud.detail(row['id']); self.assertEqual(result['state'],'paused'); self.assertGreater(result['next_at'],0)
        with self.assertRaises(HTTPException): await self.aud.control(row['id'],AudienceControl(action='resume'))
        self.assertEqual(len(self.client.calls),1)
    async def test_pause_cancel_and_restart_preserve_choices(self):
        row=await self.make_run(); blocker=asyncio.Event()
        async def pace(_): await blocker.wait()
        self.aud.pace=pace; await self.aud.control(row['id'],AudienceControl(action='search')); await asyncio.sleep(0)
        await self.aud.control(row['id'],AudienceControl(action='pause')); self.assertEqual((await self.aud.detail(row['id']))['state'],'paused')
        self.aud.pace=AsyncMock(); await self.aud.control(row['id'],AudienceControl(action='resume')); await self.aud.task
        await self.aud.select(row['id'],AudienceSelection(source_ids=[utils.get_peer_id(self.client.source)]))
        self.aud.update(row['id'],state='running',phase='collect'); self.aud.initialize(); result=await self.aud.detail(row['id'])
        self.assertEqual(result['state'],'paused'); self.assertTrue(result['sources'][0]['selected'])
        await self.aud.control(row['id'],AudienceControl(action='cancel')); self.assertEqual((await self.aud.detail(row['id']))['state'],'cancelled')
    async def test_repeat_author_is_deduplicated_and_csv_has_no_private_hash(self):
        row=await self.make_run(); result=await self.collect(row)
        proof=result['candidates'][0]['evidence'][0]
        await self.aud.candidate(self.aud.row(row['id']),self.a,self.client.target,self.client.users[0],proof)
        self.assertEqual((await self.aud.detail(row['id']))['stats']['available'],6)
        csv=await self.aud.export(row['id']); self.assertNotIn('access_hash',csv.body.decode()); self.assertIn('https://t.me/dubai_marketing/100',csv.body.decode())
    async def test_limit_is_honest_partial_selection(self):
        row=await self.make_run(max_people=1); result=await self.collect(row)
        self.assertEqual(result['stats']['available'],1)
        self.assertEqual(result['phase'],'review_people')
        self.assertIn('частичная',result['error'])
    async def test_selection_cannot_race_draft_or_create_a_second_job(self):
        row=await self.make_run(); result=await self.collect(row)
        ids=[p['id'] for p in result['candidates'] if p['eligible']]; await self.aud.select(row['id'],AudienceSelection(candidate_ids=ids))
        entered=asyncio.Event(); unblock=asyncio.Event(); original=self.client.get_entity
        async def slow(ref):
            if isinstance(ref,types.InputPeerUser): entered.set(); await unblock.wait()
            return await original(ref)
        self.client.get_entity=slow
        first=asyncio.create_task(self.aud.draft(row['id'],AudienceDraft(request_key='race-first-request-01',consent_note='Synthetic consenting registrants')))
        await entered.wait()
        selection=asyncio.create_task(self.aud.select(row['id'],AudienceSelection(candidate_ids=ids[:1])))
        second=asyncio.create_task(self.aud.draft(row['id'],AudienceDraft(request_key='race-second-request-01',consent_note='Synthetic consenting registrants')))
        await asyncio.sleep(0); self.assertFalse(selection.done()); unblock.set(); draft=await first
        with self.assertRaises(HTTPException): await selection
        with self.assertRaises(HTTPException): await second
        self.assertEqual((await self.aud.detail(row['id']))['job_id'],draft['id'])
        self.assertEqual(self.ops.store.db.execute('SELECT COUNT(*) FROM op_jobs').fetchone()[0],1)
        self.assertEqual({i['user_id'] for i in draft['items']},{200,201})
    async def test_primary_flood_wait_is_shared_with_scanner(self):
        shared=Store(Path(self.tmp.name)/'client.sqlite3'); runtime=SimpleNamespace(client=self.client,me=types.User(id=100,username='primary'),discovery_next=0)
        extra=Operations(Path(self.tmp.name)/'extra',lambda:{'id':1,'hash':'test'},lambda:runtime,lambda:shared); extra.initialize(); extra.audience.pace=AsyncMock()
        try:
            row=await extra.audience.create(AudienceSpec(account='primary',target=utils.get_peer_id(self.client.target),title='Primary test',topic='Маркетинг',queries=['маркетинг'],keywords='маркетинг'))
            self.client.flood=True;await extra.audience.control(row['id'],AudienceControl(action='search'));await extra.audience.task
            result=await extra.audience.detail(row['id']);self.assertEqual(result['state'],'paused')
            self.assertEqual(shared.get('scan_not_before'),result['next_at']);self.assertEqual(runtime.discovery_next,result['next_at'])
        finally:await extra.shutdown();shared.db.close()
    async def test_http_routes_plan_read_and_invalid_selection_never_send(self):
        from fastapi import FastAPI
        import httpx
        app=FastAPI();app.include_router(self.ops.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
            spec=AudienceSpec(account=self.a.aid,target=utils.get_peer_id(self.client.target),title='HTTP plan',topic='Маркетинг',queries=['маркетинг'],keywords='маркетинг')
            response=await c.post('/api/scanner/operations/audience/runs',json=spec.model_dump());self.assertEqual(response.status_code,200);rid=response.json()['id']
            self.assertEqual((await c.get('/api/scanner/operations/audience/runs')).json()['runs'][0]['id'],rid)
            self.assertEqual((await c.post(f'/api/scanner/operations/audience/runs/{rid}/selection',json={'candidate_ids':[555]})).status_code,400)
            self.assertEqual((await c.post(f'/api/scanner/operations/audience/runs/{rid}/draft',json={'request_key':'test-http-draft-key','consent_note':'Test registrant consent'})).status_code,400)
            self.assertEqual(self.client.calls,[])


class DirectParsingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = AudienceTests.asyncSetUp
    asyncTearDown = AudienceTests.asyncTearDown

    async def parse(self, **values):
        spec = dict(account=self.a.aid, source='https://t.me/dubai_marketing', per_source=10)
        spec.update(values)
        row = await self.aud.parse(ParseSpec(**spec))
        await self.aud.task
        return await self.aud.detail(row['id'])

    async def test_one_action_collects_without_target_setup_or_writes_to_telegram(self):
        self.ops.rights = AsyncMock(side_effect=AssertionError('Parsing does not check invitation rights'))
        self.client.get_permissions = AsyncMock(side_effect=AssertionError('No target membership in standalone parsing'))
        result = await self.parse()
        self.assertEqual((result['mode'], result['target'], result['state'], result['phase']), ('parse', 0, 'ready', 'review_people'))
        self.assertTrue(result['spec']['standalone'])
        self.assertEqual(result['spec']['source'], 'dubai_marketing')
        self.assertEqual(result['stats']['eligible'], 3)
        self.assertEqual(result['stats']['excluded'], 3)
        self.assertEqual(result['stats']['copyable'], 1)
        self.assertEqual(result['sources'][0]['queries'], [])
        self.assertTrue(result['sources'][0]['selected'])
        self.assertTrue(all(isinstance(r, functions.channels.GetParticipantsRequest) for r in self.client.calls))
        self.assertNotIn('access_hash', json.dumps(result))
        self.assertNotIn('binding', result)

    async def test_authors_can_collect_without_keywords_and_repeats_are_deduplicated(self):
        original = self.client.get_messages
        async def messages(*args, **kwargs):
            rows = await original(*args, **kwargs)
            return rows + rows[:1]
        self.client.get_messages = messages
        result = await self.parse(method='authors')
        self.assertEqual(result['stats']['available'], 6)
        self.assertEqual(result['stats']['eligible'], 3)
        self.assertEqual(len([p for p in result['candidates'] if p['user_id'] == 200]), 1)
        filtered = await self.parse(method='authors', keywords='диджей')
        self.assertEqual(filtered['stats']['available'], 0)

    async def test_private_peer_id_keeps_links_valid_and_is_resolved_by_same_account(self):
        self.client.source.username = None
        original = self.client.get_entity
        async def resolve(ref):
            if ref == utils.get_peer_id(self.client.source): return self.client.source
            return await original(ref)
        self.client.get_entity = resolve
        result = await self.parse(source=utils.get_peer_id(self.client.source), method='authors')
        self.assertEqual(result['state'], 'ready')
        self.assertIsNone(result['sources'][0]['username'])
        proof = result['candidates'][0]['evidence'][0]
        self.assertEqual(proof['source_url'], 'https://t.me/c/20')
        self.assertEqual(proof['message_url'], 'https://t.me/c/20/100')
        self.assertNotIn('None', (await self.aud.export(result['id'])).body.decode())

    async def test_basic_group_collection_is_bounded_and_has_no_fake_public_links(self):
        basic = types.Chat(id=30, title='Private basic group', photo=types.ChatPhotoEmpty(), participants_count=6,
                           date=datetime.now(timezone.utc), version=1)
        self.client.get_entity = AsyncMock(return_value=basic)
        self.client.get_participants = AsyncMock(return_value=self.client.users)
        result = await self.parse(source=-30)
        self.assertEqual(result['stats']['eligible'], 3)
        self.client.get_participants.assert_awaited_once_with(basic, limit=10)
        self.assertIsNone(result['candidates'][0]['evidence'][0]['source_url'])
        self.assertNotIn('https://t.me/None', (await self.aud.export(result['id'])).body.decode())

    async def test_limit_returns_partial_result_not_all_participants(self):
        result = await self.parse(max_people=1)
        self.assertEqual(result['stats']['available'], 1)
        self.assertIn('частичная', result['error'])
        self.assertLessEqual(len(self.client.calls), 1)

    async def test_hidden_members_are_saved_as_unavailable_without_fallback(self):
        self.client.hidden = True
        result = await self.parse()
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(result['sources'][0]['state'], 'unavailable')
        self.assertIn('недоступен', result['sources'][0]['error'])
        self.assertEqual(result['stats']['available'], 0)
        self.assertEqual(len(self.client.calls), 1)

    async def test_changed_session_mid_read_is_saved_as_paused(self):
        original = self.client.get_entity
        async def replace_session(ref):
            result = await original(ref)
            self.client.session.auth_key = AuthKey(b'2'*256)
            return result
        self.client.get_entity = replace_session
        result = await self.parse()
        self.assertEqual(result['state'], 'paused')
        self.assertIn('сессия изменились', result['error'])
        self.assertEqual(result['stats']['available'], 0)
        self.assertEqual(self.client.calls, [])

    async def test_initial_resolution_failure_and_flood_wait_preserve_run(self):
        self.client.get_entity = AsyncMock(side_effect=errors.FloodWaitError(None, capture=12))
        result = await self.parse()
        self.assertEqual(result['state'], 'paused')
        self.assertGreater(result['next_at'], 0)
        self.assertEqual(self.ops.account_row(self.a.aid)['not_before'], result['next_at'])
        with self.assertRaises(HTTPException): await self.aud.control(result['id'], AudienceControl(action='resume'))
        self.assertEqual(self.client.calls, [])

    async def test_unavailable_source_is_saved_with_actionable_error(self):
        self.client.get_entity = AsyncMock(side_effect=ValueError('Cannot find any entity'))
        result = await self.parse()
        self.assertEqual(result['state'], 'paused')
        self.assertIn('недоступен выбранному аккаунту', result['error'])
        self.assertEqual(result['sources'], [])
        self.assertEqual(self.client.calls, [])

    async def test_pause_resume_preserves_direct_source_and_result(self):
        blocker = asyncio.Event()
        async def pace(_): await blocker.wait()
        self.aud.pace = pace
        row = await self.aud.parse(ParseSpec(account=self.a.aid, source='dubai_marketing', per_source=10))
        await asyncio.sleep(0)
        with self.assertRaises(HTTPException): await self.aud.parse(ParseSpec(account=self.a.aid, source='dubai_marketing'))
        self.assertEqual(self.ops.store.db.execute('SELECT COUNT(*) FROM audience_runs').fetchone()[0], 1)
        await self.aud.control(row['id'], AudienceControl(action='pause'))
        self.assertEqual((await self.aud.detail(row['id']))['state'], 'paused')
        self.aud.pace = AsyncMock()
        await self.aud.control(row['id'], AudienceControl(action='resume'))
        await self.aud.task
        result = await self.aud.detail(row['id'])
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(len(result['sources']), 1)

    async def test_standalone_cannot_bypass_target_bound_draft(self):
        result = await self.parse()
        selected = [p['id'] for p in result['candidates'] if p['eligible']][:1]
        await self.aud.select(result['id'], AudienceSelection(candidate_ids=selected))
        with self.assertRaises(HTTPException): await self.aud.resolve_selected(result['id'], selected, self.a.aid, 0)
        with self.assertRaises(HTTPException): await self.aud.draft(result['id'], AudienceDraft(request_key='standalone-test-no-draft', consent_note='Only parsing was requested'))
        forged = AudienceSpec.model_validate(result['spec'])
        with self.assertRaises(HTTPException): await self.aud.create(forged)
        self.assertEqual(self.ops.store.db.execute('SELECT COUNT(*) FROM op_jobs').fetchone()[0], 0)

    async def test_parse_http_contract_bounds_and_source_formats(self):
        from fastapi import FastAPI
        import httpx
        app = FastAPI(); app.include_router(self.ops.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            for source in [123, 'https://other.example/group', 'https://t.me/+DEVELOPMENT_ONLY', 'https://t.me/handle?join=true']:
                response = await client.post('/api/scanner/operations/audience/parse', json={'account': self.a.aid, 'source': source})
                self.assertEqual(response.status_code, 422)
            response = await client.post('/api/scanner/operations/audience/parse', json={'account': self.a.aid, 'source': '@dubai_marketing', 'max_people': 201})
            self.assertEqual(response.status_code, 422)
            response = await client.post('/api/scanner/operations/audience/parse', json={'account': self.a.aid, 'source': '@dubai_marketing', 'per_source': 10})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['mode'], 'parse')
            self.assertEqual(response.json()['state'], 'running')
            await self.aud.task
            history = (await client.get('/api/scanner/operations/audience/runs')).json()['runs']
            self.assertEqual(history[0]['mode'], 'parse')
        self.assertEqual(ParseSpec(account='primary', source='https://t.me/c/20/100').source, utils.get_peer_id(self.client.source))


class MultipleSourceParsingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = AudienceTests.asyncSetUp
    asyncTearDown = AudienceTests.asyncTearDown

    def configure_sources(self, members=None):
        self.groups = [self.client.source, group(21, 'Русские в Дубае', username='russian_dubai')]
        shared = types.User(id=301, first_name='Shared', username='shared_user', access_hash=301)
        self.people = members or {20: [shared, types.User(id=302, first_name='First', username='first_user', access_hash=302)],
                                  21: [shared, types.User(id=303, first_name='Second', username='second_user', access_hash=303)]}
        self.resolution_calls = []
        async def entity(reference):
            self.resolution_calls.append(reference)
            for source in self.groups:
                if reference in (source.username, utils.get_peer_id(source)): return source
            raise ValueError('Unavailable source')
        async def participants(request):
            self.client.calls.append(request)
            self.assertIsInstance(request, functions.channels.GetParticipantsRequest)
            users = self.people[request.channel.id][request.offset:request.offset+request.limit]
            return SimpleNamespace(users=users, participants=[SimpleNamespace(user_id=user.id) for user in users])
        self.client.get_entity = entity
        class SourceClient(Client):
            async def __call__(client, request): return await participants(request)
        self.client.__class__ = SourceClient

    async def parse(self, sources, **values):
        request = ParseSpec(account=self.a.aid, sources=sources, per_source=10, **values)
        row = await self.aud.parse(request)
        await self.aud.task
        return await self.aud.detail(row['id'])

    async def test_canonical_and_telegram_id_dedupe_share_one_candidate_list(self):
        self.configure_sources()
        self.ops.rights = AsyncMock(side_effect=AssertionError('No invitation rights while parsing'))
        self.client.get_permissions = AsyncMock(side_effect=AssertionError('No target membership while parsing'))
        references = ['@DUBAI_MARKETING', 'https://t.me/dubai_marketing', utils.get_peer_id(self.groups[0]), '@russian_dubai']
        result = await self.parse(references)
        self.assertEqual(result['spec']['sources'], ['dubai_marketing', utils.get_peer_id(self.groups[0]), 'russian_dubai'])
        self.assertIsNone(result['spec']['source'])
        self.assertEqual(result['source_stats'], {'requested': 3, 'resolved': 2, 'complete': 2, 'pending': 0, 'limited': 0, 'unavailable': 0, 'resolution_errors': 0})
        self.assertEqual(result['stats']['available'], 3)
        shared = next(person for person in result['candidates'] if person['user_id'] == 301)
        self.assertEqual({proof['source_id'] for proof in shared['evidence']}, {utils.get_peer_id(source) for source in self.groups})
        self.assertEqual(result['target'], 0)
        self.assertFalse(self.ops.store.rows('SELECT id FROM op_jobs'))
        self.assertTrue(all(isinstance(request, functions.channels.GetParticipantsRequest) for request in self.client.calls))
        self.assertNotIn('access_hash', json.dumps(result))

    async def test_unavailable_reference_does_not_erase_valid_sources_or_results(self):
        self.configure_sources()
        result = await self.parse(['dubai_marketing', 'missing_group', 'russian_dubai'])
        self.assertEqual((result['state'], result['phase']), ('ready', 'review_people'))
        self.assertEqual(result['stats']['available'], 3)
        self.assertEqual(result['source_stats']['resolved'], 2)
        self.assertEqual(result['source_stats']['resolution_errors'], 1)
        self.assertEqual(result['source_resolution_errors'][0]['source'], 'missing_group')
        self.assertIn('недоступен выбранному аккаунту', result['source_resolution_errors'][0]['reason'])
        self.assertIn('Недоступны 1', result['error'])
        self.assertEqual(len(result['spec']['source_resolution']), 3)

    async def test_total_cap_preserves_unread_groups_and_unconsumed_page_positions(self):
        self.configure_sources()
        result = await self.parse(['dubai_marketing', 'russian_dubai'], max_people=1)
        self.assertEqual(result['stats']['available'], 1)
        self.assertEqual(result['source_stats']['pending'], 2)
        self.assertEqual(result['source_stats']['complete'], 0)
        self.assertEqual(sorted(source['scanned'] for source in result['sources']), [0, 1])
        self.assertEqual(sorted(source['cursor'] for source in result['sources']), [0, 1])
        self.assertIn('частичная', result['error'])
        self.assertIn('Ещё не прочитаны полностью 2', result['error'])
        self.assertEqual(len(self.client.calls), 1)

    async def test_hidden_members_do_not_stop_other_selected_sources(self):
        self.configure_sources()
        original = self.client.__class__.__call__
        async def call(client, request):
            if request.channel.id == 21: raise errors.ChatAdminRequiredError(None)
            return await original(client, request)
        self.client.__class__.__call__ = call
        result = await self.parse(['dubai_marketing', 'russian_dubai'])
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(result['source_stats']['unavailable'], 1)
        self.assertEqual(result['source_stats']['complete'], 1)
        self.assertEqual(result['stats']['available'], 2)
        unavailable = next(source for source in result['sources'] if source['state'] == 'unavailable')
        self.assertIn('Скрытые списки не обходятся', unavailable['error'])
        self.assertIn('Недоступны 1', result['error'])

    async def test_per_source_cap_is_limited_not_completed(self):
        users = [types.User(id=400+i, first_name=str(i), username='user_'+str(i), access_hash=400+i) for i in range(20)]
        self.configure_sources({20: users, 21: []})
        result = await self.parse(['dubai_marketing', 'russian_dubai'])
        self.assertEqual(result['stats']['available'], 10)
        self.assertEqual(result['source_stats']['limited'], 1)
        self.assertEqual(result['source_stats']['complete'], 1)
        self.assertIn('частичная', result['error'])

    async def test_pause_during_resolution_resumes_without_losing_or_duplicating_sources(self):
        self.configure_sources()
        entered = asyncio.Event(); release = asyncio.Event(); calls = 0
        async def pace(_):
            nonlocal calls
            calls += 1
            if calls == 2: entered.set(); await release.wait()
        self.aud.pace = pace
        row = await self.aud.parse(ParseSpec(account=self.a.aid, sources=['dubai_marketing', 'russian_dubai'], per_source=10))
        await entered.wait()
        await self.aud.control(row['id'], AudienceControl(action='pause'))
        paused = await self.aud.detail(row['id'])
        self.assertEqual(len(paused['sources']), 1)
        self.assertEqual(len(paused['spec']['source_resolution']), 1)
        self.aud.pace = AsyncMock()
        await self.aud.control(row['id'], AudienceControl(action='resume')); await self.aud.task
        result = await self.aud.detail(row['id'])
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(len(result['sources']), 2)
        self.assertEqual(len(result['spec']['source_resolution']), 2)
        self.assertEqual(result['stats']['available'], 3)

    async def test_pause_between_sources_resumes_existing_results_and_merges_evidence(self):
        self.configure_sources()
        entered = asyncio.Event(); release = asyncio.Event(); calls = 0
        async def pace(_):
            nonlocal calls
            calls += 1
            if calls == 4: entered.set(); await release.wait()
        self.aud.pace = pace
        row = await self.aud.parse(ParseSpec(account=self.a.aid, sources=['dubai_marketing', 'russian_dubai'], per_source=10))
        await entered.wait()
        await self.aud.control(row['id'], AudienceControl(action='pause'))
        paused = await self.aud.detail(row['id'])
        self.assertEqual(paused['stats']['available'], 2)
        self.assertEqual(paused['source_stats']['complete'], 1)
        self.aud.pace = AsyncMock()
        await self.aud.control(row['id'], AudienceControl(action='resume')); await self.aud.task
        result = await self.aud.detail(row['id'])
        self.assertEqual(result['stats']['available'], 3)
        shared = next(person for person in result['candidates'] if person['user_id'] == 301)
        self.assertEqual(len(shared['evidence']), 2)
        self.assertEqual(result['source_stats']['complete'], 2)

    async def test_authors_share_dedupe_and_cap_does_not_skip_unprocessed_messages(self):
        self.configure_sources()
        async def messages(source, limit, offset_id):
            if offset_id: return []
            return [SimpleNamespace(id=100-i, date=datetime.now(timezone.utc), message='Дубай', get_sender=AsyncMock(return_value=user)) for i, user in enumerate(self.people[source.id])]
        self.client.get_messages = messages
        result = await self.parse(['dubai_marketing', 'russian_dubai'], method='authors')
        self.assertEqual(result['stats']['available'], 3)
        shared = next(person for person in result['candidates'] if person['user_id'] == 301)
        self.assertEqual(len(shared['evidence']), 2)
        limited = await self.parse(['dubai_marketing', 'russian_dubai'], method='authors', max_people=1)
        self.assertEqual(limited['source_stats']['pending'], 2)
        read = next(source for source in limited['sources'] if source['scanned'])
        self.assertEqual((read['scanned'], read['cursor']), (1, 100))

    async def test_session_replacement_during_second_resolution_pauses_before_accepting_it(self):
        self.configure_sources()
        original = self.client.get_entity
        async def entity(reference):
            result = await original(reference)
            if reference == 'russian_dubai': self.client.session.auth_key = AuthKey(b'2'*256)
            return result
        self.client.get_entity = entity
        result = await self.parse(['dubai_marketing', 'russian_dubai'])
        self.assertEqual(result['state'], 'paused')
        self.assertIn('сессия изменились', result['error'])
        self.assertEqual(len(result['sources']), 1)
        self.assertEqual(result['stats']['available'], 0)
        self.assertEqual(self.client.calls, [])
        with self.assertRaises(HTTPException): await self.aud.control(result['id'], AudienceControl(action='resume'))

    async def test_same_key_new_client_during_read_is_not_accepted(self):
        self.configure_sources()
        original = self.client.get_entity
        async def entity(reference):
            result = await original(reference)
            self.a.client = Client()
            return result
        self.client.get_entity = entity
        result = await self.parse(['dubai_marketing', 'russian_dubai'])
        self.assertEqual(result['state'], 'paused')
        self.assertIn('Подключение изменилось', result['error'])
        self.assertEqual(result['sources'], [])
        self.assertEqual(self.client.calls, [])

    async def test_resolution_error_with_changed_session_cannot_be_hidden_as_unavailable(self):
        self.configure_sources()
        async def changed(_):
            self.client.session.auth_key = AuthKey(b'2'*256)
            raise ValueError('Unavailable peer')
        self.client.get_entity = changed
        result = await self.parse(['dubai_marketing', 'russian_dubai'])
        self.assertEqual(result['state'], 'paused')
        self.assertIn('сессия изменились', result['error'])
        self.assertEqual(result['source_resolution_errors'], [])

    async def test_http_contract_requires_one_reference_shape_and_bounds_each_reference(self):
        from fastapi import FastAPI
        import httpx
        app = FastAPI(); app.include_router(self.ops.router)
        invalid = [{}, {'sources': []}, {'sources': ['dubai_marketing']*101}, {'source': 'dubai_marketing', 'sources': ['russian_dubai']},
                   {'sources': ['https://t.me/+DEVELOPMENT_ONLY']}, {'sources': [123]}, {'sources': ['https://t.me/addlist/Synthetic_folder']}]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            for body in invalid:
                response = await client.post('/api/scanner/operations/audience/parse', json={'account': self.a.aid, **body})
                self.assertEqual(response.status_code, 422, body)
        self.assertEqual(self.ops.store.rows('SELECT id FROM audience_runs'), [])
        self.assertEqual(self.client.calls, [])


if __name__=='__main__': unittest.main()
