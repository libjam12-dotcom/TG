"""Phase one must prepare drafts without altering existing work or sending."""
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import test_operations as fixture
from operation_workbench import PlanSpec, ExclusionSpec, DisableExclusion
from operations import Control
from fastapi import HTTPException


class WorkbenchTests(fixture.OperationsTests):
    # Reuse the isolated account fixture, not the parent test suite.
    async def asyncSetUp(self):
        await super().asyncSetUp()
        from operation_campaigns import CampaignSpec
        self.profile=(await self.ops.campaigns.save_campaign(CampaignSpec(name='Test profile',text='Approved publication')))['campaign']

    def plan(self, **changes):
        start = datetime.now(timezone.utc) + timedelta(days=1)
        values = dict(request_key='phase-one-plan-key-001', account=self.a.aid,
                      telegram_id=100, kind='post', title='Finite test',
                      destinations=[-1000000000010], text='Approved publication',
                      campaign_id=self.profile['id'],campaign_revision=self.profile['revision'],
                      permission_note='Publication agreed with owner',
                      timezone='UTC', start_date=start.date().isoformat(),
                      end_date=(start+timedelta(days=1)).date().isoformat(), times=['12:00'])
        values.update(changes)
        return PlanSpec(**values)

    async def test_preview_is_read_only_and_save_creates_only_drafts(self):
        legacy=await self.draft()
        before=self.ops.store.rows('SELECT * FROM op_jobs')
        self.ops.store.put('preserved-setting', {'value':7})
        p=self.plan()
        preview=await self.ops.workbench.preview(p)
        self.assertEqual(len(preview['slots']),2)
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs'),before)
        self.assertFalse(self.c.calls)
        saved=await self.ops.workbench.save_plan(p)
        self.assertEqual(len(saved['jobs']),2)
        self.assertTrue(all(j['state']=='draft' for j in saved['jobs']))
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs WHERE id=?',(legacy['id'],)),before)
        self.assertEqual(self.ops.store.get('preserved-setting'),{'value':7})
        self.assertFalse(self.c.calls)
        await self.ops.tick()
        self.assertFalse(self.c.calls)
        again=await self.ops.workbench.save_plan(p)
        self.assertEqual(again['id'],saved['id'])
        with self.assertRaises(HTTPException):
            await self.ops.workbench.save_plan(self.plan(text='Different text'))

    async def test_exclusion_added_after_draft_stops_worker_without_send(self):
        job=await self.draft()
        await self.ops.workbench.add_exclusion(ExclusionSpec(
            request_key='phase-one-exclusion-001',subject='chat',identifier='-1000000000010',
            reason='Do not post in this chat'))
        await self.start(job)
        await self.ops.tick()
        detail=await self.ops.detail(job['id'])
        self.assertEqual(detail['items'][0]['state'],'excluded')
        self.assertFalse(self.c.calls)

    async def test_exclusions_are_normalized_scoped_and_revision_checked(self):
        spec=ExclusionSpec(request_key='phase-one-exclusion-002',subject='user',
                           identifier='@Agreed_User',reason='Recipient opted out',
                           account=self.a.aid,telegram_id=100)
        row=await self.ops.workbench.add_exclusion(spec)
        self.assertEqual(row['identifier'],'agreed_user')
        self.assertEqual((await self.ops.workbench.add_exclusion(spec))['id'],row['id'])
        self.assertIsNotNone(self.ops.workbench.match(self.a.aid,100,0,None,200,'AGREED_USER'))
        self.assertIsNone(self.ops.workbench.match('other',100,0,None,200,'agreed_user'))
        with self.assertRaises(HTTPException):
            await self.ops.workbench.disable_exclusion(row['id'],DisableExclusion(revision=9,confirmed=True))
        await self.ops.workbench.disable_exclusion(row['id'],DisableExclusion(revision=1,confirmed=True))
        self.assertIsNone(self.ops.workbench.match(self.a.aid,100,0,None,200,'agreed_user'))

    async def test_report_is_read_only_and_unknown_is_not_success(self):
        plan=await self.ops.workbench.save_plan(self.plan())
        jobs=[j['id'] for j in plan['jobs']]
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_items SET state='uncertain' WHERE job=?",(jobs[0],))
        before=self.ops.store.db.total_changes
        report=await self.ops.workbench.plan_detail(plan['id'])
        self.assertEqual(report['counts']['uncertain'],1)
        self.assertEqual(report['counts'].get('sent',0),0)
        self.assertEqual(before,self.ops.store.db.total_changes)
        self.assertFalse(self.c.calls)

    async def test_wrong_account_identity_never_creates_plan(self):
        with self.assertRaises(HTTPException):
            await self.ops.workbench.save_plan(self.plan(telegram_id=999))
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_plans'))

    async def test_readiness_is_read_only_and_never_exposes_proxy_secret(self):
        self.ops.store.put('account_proxy_'+self.a.aid,dict(mode='socks5',address='8.8.8.8',port=1080,username='user',password='secret'))
        before=self.ops.store.db.total_changes
        result=await self.ops.workbench.readiness(self.a.aid)
        self.assertEqual(result['telegram_id'],100)
        self.assertNotIn('secret',json.dumps(result))
        self.assertEqual(before,self.ops.store.db.total_changes)
        self.assertFalse(self.c.calls)

    async def test_atomic_rollback_preserves_old_jobs(self):
        legacy=await self.draft()
        before=self.ops.store.rows('SELECT * FROM op_jobs')
        self.ops.store.db.execute("CREATE TRIGGER fail_second BEFORE INSERT ON op_plan_jobs WHEN NEW.slot=1 BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(Exception):await self.ops.workbench.save_plan(self.plan())
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_plans'))
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_plan_jobs'))
        self.assertEqual(self.ops.store.rows('SELECT * FROM op_jobs'),before)

    async def test_personal_cohort_is_resolved_deduplicated_and_distributed_once(self):
        from test_operations import CohortClient
        self.c=CohortClient(self.group);self.a.client=self.c
        p=self.plan(kind='message',destinations=[],users=['pilot_user_1','@pilot_user_2','PILOT_USER_1','pilot_user_3'],
                    campaign_id=None,campaign_revision=None,consent_note='Confirmed opt in at registration')
        report=await self.ops.workbench.save_plan(p)
        self.assertEqual(len(report['items']),3)
        self.assertEqual(len({i['user_id'] for i in report['items']}),3)
        self.assertTrue(all(j['state']=='draft' for j in report['jobs']))
        self.assertFalse(self.c.calls)

    async def test_exclusion_race_during_peer_resolution_stops_the_send(self):
        j=await self.draft()
        original=self.c.get_input_entity
        async def excluded_peer(entity):
            await self.ops.workbench.add_exclusion(ExclusionSpec(request_key='late-exclusion-key-001',
                subject='chat',identifier=str(-1000000000010),reason='Added during preflight'))
            return await original(entity)
        self.c.get_input_entity=excluded_peer
        await self.start(j);await self.ops.tick()
        self.assertEqual((await self.ops.detail(j['id']))['items'][0]['state'],'excluded')
        self.assertFalse(self.c.calls)

    async def test_profile_revision_race_never_commits_drafts(self):
        original=self.c.get_entity
        async def changed_profile(ref):
            with self.ops.store.db:self.ops.store.db.execute('UPDATE op_campaigns SET revision=revision+1 WHERE id=?',(self.profile['id'],))
            return await original(ref)
        self.c.get_entity=changed_profile
        with self.assertRaises(HTTPException):await self.ops.workbench.save_plan(self.plan())
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_plans'))
        self.assertFalse(self.ops.store.rows('SELECT * FROM op_jobs'))

    async def test_unknown_calendar_and_unbounded_plans_are_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):self.plan(timezone='Unknown/Place')
        with self.assertRaises(ValidationError):self.plan(times=['12:00','12:00'])
        with self.assertRaises(ValidationError):self.plan(end_date=(datetime.now(timezone.utc)+timedelta(days=40)).date().isoformat())
        p=self.plan(times=['12:00','13:00'])
        with self.assertRaises(HTTPException):await self.ops.workbench.preview(p)

    async def test_http_routes_validate_drafts_and_export_report_without_running(self):
        import httpx
        from fastapi import FastAPI
        app=FastAPI();app.include_router(self.ops.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
            body=self.plan().model_dump(mode='json')
            preview=await client.post('/api/scanner/operations/plans/preview',json=body)
            self.assertEqual(preview.status_code,200)
            self.assertFalse(self.ops.store.rows('SELECT * FROM op_jobs'))
            saved=await client.post('/api/scanner/operations/plans',json=body)
            self.assertEqual(saved.status_code,200,saved.text)
            pid=saved.json()['id']
            before=self.ops.store.db.total_changes
            self.assertEqual((await client.get(f'/api/scanner/operations/plans/{pid}')).status_code,200)
            csv=await client.get(f'/api/scanner/operations/plans/{pid}/report')
            self.assertIn('text/csv',csv.headers['content-type'])
            self.assertEqual(before,self.ops.store.db.total_changes)
            invalid=await client.post('/api/scanner/operations/plans',json={**body,'start_now':True})
            self.assertEqual(invalid.status_code,422)
        self.assertFalse(self.c.calls)

    async def test_expired_personal_campaign_window_never_writes(self):
        from unittest.mock import patch
        report=await self.ops.workbench.save_plan(self.plan(kind='message',destinations=[],users=['agreed_user'],
            campaign_id=None,campaign_revision=None,consent_note='Confirmed opt in at registration',end_date=self.plan().start_date.isoformat()))
        j=await self.ops.detail(report['jobs'][0]['id']);await self.start(j)
        with patch('operation_workbench.time.time',return_value=datetime.fromisoformat(j['spec']['scheduled_at']).timestamp()+7201):
            await self.ops.tick()
        item=(await self.ops.detail(j['id']))['items'][0]
        self.assertEqual(item['state'],'cancelled')
        self.assertFalse(self.c.calls)


# Avoid collecting inherited test cases a second time.
for _name in list(vars(fixture.OperationsTests)):
    if _name.startswith('test_') and _name not in WorkbenchTests.__dict__:
        setattr(WorkbenchTests,_name,None)
