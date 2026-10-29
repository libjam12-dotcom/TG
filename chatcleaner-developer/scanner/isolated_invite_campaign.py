"""Finite, explicitly authorized contact campaign; normal outbound stays disabled.

Only this in-memory worker may touch its paused job. Persistent claims prevent
replays; restart leaves pending items paused and never recreates the worker.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
import json
import math
import re
import secrets
import time
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from telethon import errors, functions, types

from engine import now
from isolated_invite import (IsolatedInvite, IsolatedAttempt, isolated_admin_only,
    ACCOUNT, SENDER, TARGET, EXCLUDED, REVOKED, digest, utc_day, fail)
from operation_workbench import WorkbenchStopped


SCOPE = 'client-russkie-contact-campaign-v2'
DAY_PREFIX = 'isolated_invite_campaign_day_'
REQUEST_PREFIX = 'isolated_invite_campaign_request_'
ITEM_PREFIX = 'isolated_invite_campaign_item_'
MAX_ITEMS = 100
ACCOUNT_DAY_ATTEMPT_CEILING = 100
GOAL_ADDED = 100
MAX_CHECKS = 150
DELAY = 60
TOKEN_TTL = 300
RUN_TTL = 10800
PREVIEW_TIMEOUT = 180
STEP_TIMEOUT = 80
ACTIVE = ('running', 'waiting')
Mode = Literal['mutual', 'saved_contacts']
SAVED_PILOT_MAX = 10
COUNT_KEYS = ('total', 'added', 'already_member', 'restricted', 'failed', 'blocked',
              'uncertain', 'pending', 'sending', 'cancelled', 'duplicate_skipped', 'other')


class CampaignStart(IsolatedAttempt):
    scope: Literal[SCOPE]
    policy_digest: str = Field(pattern=r'^[a-f0-9]{64}$')
    mode: Mode = 'mutual'


class CampaignPause(BaseModel):
    model_config = {'extra': 'forbid'}
    scope: Literal[SCOPE]
    mode: Mode = 'mutual'
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    job_id: int = Field(gt=0, strict=True)


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


class IsolatedInviteCampaign(IsolatedInvite):
    def __init__(self, ops):
        super().__init__(ops)
        self.task = None
        self.active = None
        self.wake = asyncio.Event()
        self.router = APIRouter(dependencies=[Depends(isolated_admin_only)])
        self.router.add_api_route('/isolated-invite-campaign/contacts-preview', self.preview, methods=['GET'])
        self.router.add_api_route('/isolated-invite-campaign/start', self.start, methods=['POST'])
        self.router.add_api_route('/isolated-invite-campaign/status', self.status, methods=['GET'])
        self.router.add_api_route('/isolated-invite-campaign/pause', self.pause, methods=['POST'])

    @staticmethod
    def require_scope(scope):
        if scope != SCOPE:
            raise fail(422, 'Неизвестная область контрольного пакета.')

    @staticmethod
    def require_mode(mode):
        if mode not in ('mutual', 'saved_contacts'):
            raise fail(422, 'Неизвестный режим контактов.')

    @staticmethod
    def record_mode(record):
        mode = record.get('mode', 'mutual') if isinstance(record, dict) else 'mutual'
        IsolatedInviteCampaign.require_mode(mode)
        return mode

    @staticmethod
    def day_key(day, mode='mutual'):
        IsolatedInviteCampaign.require_mode(mode)
        return DAY_PREFIX + day + ('_saved_contacts' if mode == 'saved_contacts' else '')

    @staticmethod
    def max_items(mode):
        return SAVED_PILOT_MAX if mode == 'saved_contacts' else MAX_ITEMS

    def eligible(self, user, mode):
        if mode == 'mutual':
            return self.suitable(user)
        return (isinstance(user, types.User) and not user.min and not user.bot and not user.deleted and
            user.id not in EXCLUDED and user.contact is True and isinstance(user.username, str) and
            re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{3,31}', user.username) and type(user.access_hash) is int)

    @asynccontextmanager
    async def locked(self, deadline):
        # Compute the timeout before constructing a coroutine at an expired lease.
        timeout = self.remaining(deadline)
        try:
            await asyncio.wait_for(self.ops.lock.acquire(), timeout)
        except TimeoutError:
            raise fail(504, 'Время ожидания контрольного пакета истекло.') from None
        try:
            self.remaining(deadline)
            yield
        finally:
            self.ops.lock.release()

    async def read(self, ctx, call):
        self.guard(ctx)
        timeout = self.remaining(ctx['deadline'])
        result = await asyncio.wait_for(call(), timeout)
        self.guard(ctx)
        return result

    def initialize(self):
        # Operations already recovers sending->uncertain before this hook.
        with self.store.db:
            for row in self.store.rows("SELECT value FROM settings WHERE key LIKE 'isolated_invite_campaign_day_%'"):
                record = json.loads(row['value'])
                if isinstance(record, dict) and record.get('state') in ACTIVE:
                    record.update(state='interrupted', stop_reason='process_restart', stopped_at=now())
                    self.save_batch(record)
                    self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'", (record['job_id'],))

    async def shutdown(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    @staticmethod
    def belongs(job):
        marker = job.get('spec', {}).get('isolated_invite_campaign')
        return isinstance(marker, dict) and marker.get('scope') == SCOPE

    def save_batch(self, record):
        for key in (self.day_key(record['utc_day'], self.record_mode(record)), REQUEST_PREFIX + record['request_key']):
            self.store.db.execute('UPDATE settings SET value=? WHERE key=?', (json.dumps(record, ensure_ascii=False), key))

    def history(self, exclude_job=None):
        rows = self.store.rows('SELECT user_id FROM op_items WHERE chat_id=? AND user_id IS NOT NULL AND job!=? '
            'UNION SELECT user_id FROM op_invited WHERE chat_id=?', (TARGET, exclude_job or -1, TARGET))
        return EXCLUDED | frozenset(row['user_id'] for row in rows)

    def budget(self):
        # This explicit campaign allowance never changes the ordinary scheduler's
        # policy. Count ALL account attempts, including today's R5/R6 controls.
        normal = super().budget()
        return {**normal, 'account_day_attempt_ceiling': ACCOUNT_DAY_ATTEMPT_CEILING,
            'remaining_today': max(0, ACCOUNT_DAY_ATTEMPT_CEILING - normal['account_count_today'])}

    def verified_today(self):
        """Distinct, attributed new additions, never service events or old members."""
        day, users = utc_day(), set()
        rows = self.store.rows('SELECT i.user_id,i.finished_at,i.diagnosis FROM op_items i '
            'JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND j.telegram_id=? '
            "AND j.kind='invite' AND i.chat_id=? AND i.state='invited'",
            (ACCOUNT, SENDER, TARGET))
        for row in rows:
            try:
                finished = datetime.fromisoformat(row['finished_at'].replace('Z', '+00:00'))
                diagnosis = json.loads(row['diagnosis'] or '{}')
                if (finished.tzinfo is not None and finished.astimezone(timezone.utc).date().isoformat() == day and
                    type(row['user_id']) is int and row['user_id'] not in EXCLUDED and
                    isinstance(diagnosis, dict) and diagnosis.get('code') == 'invite_member_verified' and
                    diagnosis.get('invitation_requested') is True):
                    users.add(row['user_id'])
            except (AttributeError, TypeError, ValueError):
                continue
        return len(users)

    def policy(self, mode='mutual'):
        self.require_mode(mode)
        policy = {'scope': SCOPE, 'version': 2, 'utc_day': utc_day(), 'account': ACCOUNT,
            'sender_id': SENDER, 'target_id': TARGET, 'account_day_attempt_ceiling': ACCOUNT_DAY_ATTEMPT_CEILING,
            'normal_daily_attempt_limit': super().budget()['normal_daily_limit'], 'goal_added': GOAL_ADDED,
            'max_cohort': MAX_ITEMS, 'minimum_interval_seconds': DELAY, 'max_membership_checks': MAX_CHECKS,
            'no_auto_resume': True, 'no_account_rotation': True}
        # The default v2 policy stays byte-for-byte canonical compatible with R7.
        if mode == 'saved_contacts':
            policy.update(version=3, mode=mode, max_cohort=SAVED_PILOT_MAX, stop_first_not_mutual=True)
        return policy

    def capacity(self):
        budget = self.budget()
        verified = self.verified_today()
        remaining = budget['remaining_today']
        reset = (datetime.fromisoformat(utc_day()).replace(tzinfo=timezone.utc) + timedelta(days=1)).isoformat()
        return {'utc_day': utc_day(), 'goal_added': GOAL_ADDED,
            'budget_reset_at': reset,
            'account_day_attempt_ceiling': ACCOUNT_DAY_ATTEMPT_CEILING,
            'normal_daily_attempt_limit': budget['normal_daily_limit'],
            'account_attempts_today': budget['account_count_today'], 'verified_added_today': verified,
            'remaining_attempts_today': remaining, 'goal_remaining': max(0, GOAL_ADDED - verified),
            'maximum_possible_added_today': verified + remaining,
            'goal_possible_with_current_budget': verified + remaining >= GOAL_ADDED,
            'budget': {'utc_day': utc_day(), 'account_day_attempt_ceiling': ACCOUNT_DAY_ATTEMPT_CEILING,
                'current_day_attempts': budget['account_count_today'], 'current_day_verified': verified,
                'remaining_today': remaining, 'maximum_possible_new_today': verified + remaining,
                'budget_reset_at': reset}}

    def other_control_active(self, exclude_request=None):
        for row in self.store.rows("SELECT key,value FROM settings WHERE key LIKE 'isolated_contact_batch_day_%' OR key LIKE 'isolated_invite_day_%' OR key LIKE 'isolated_invite_campaign_day_%'"):
            record = json.loads(row['value'])
            if (isinstance(record, dict) and record.get('state') in (*ACTIVE, 'claimed') and
                (exclude_request is None or record.get('request_key') != exclude_request)):
                return True
        return False

    def context(self, deadline, mode='mutual'):
        self.require_mode(mode)
        self.remaining(deadline)
        account = self.ops.readable_account(ACCOUNT)
        row = self.ops.account_row(ACCOUNT)
        if (account.me.id != SENDER or row['telegram_id'] != SENDER or row['enabled'] != 0 or
            getattr(account.client, '_request_retries', None) != 0 or
            getattr(account.client, 'flood_sleep_threshold', None) != 0 or self.other_control_active()):
            raise fail(409, 'Нужен прежний подключённый клиентский аккаунт и остановленные исходящие.')
        if self.store.rows('SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job '
            "WHERE j.account=? AND i.state='sending' LIMIT 1", (ACCOUNT,)):
            raise fail(409, 'У аккаунта уже есть незавершённое действие.')
        budget = self.budget()
        if budget['remaining_today'] < 1 or self.verified_today() >= GOAL_ADDED:
            raise fail(409, 'Дневной бюджет исчерпан или цель уже подтверждена.')
        ctx = {'account': account, 'client': account.client, 'day': utc_day(), 'row_error': row['error'],
            'runtime_error': account.error, 'revision': self.store.get('outbound_pause_revision_primary', 0),
            'projection': self.projection(), 'used': budget['account_count_today'],
            'limit': ACCOUNT_DAY_ATTEMPT_CEILING, 'normal_limit': budget['normal_daily_limit'],
            'deadline': deadline, 'job_id': None, 'item_id': None, 'record': None, 'mode': mode}
        self.guard(ctx)
        return ctx

    def guard(self, ctx, *, allow_cooldown=False):
        if not ctx.get('batch'):
            self.remaining(ctx['deadline'])
            a = self.ops.readable_account(ACCOUNT)
            row = self.ops.account_row(ACCOUNT)
            budget = self.budget()
            if (a is not ctx['account'] or a.client is not ctx['client'] or a.me.id != SENDER or
                row['telegram_id'] != SENDER or row['enabled'] != 0 or row['error'] != ctx['row_error'] or
                a.error != ctx['runtime_error'] or utc_day() != ctx['day'] or self.other_control_active() or
                getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0 or
                self.store.get('outbound_pause_revision_primary', 0) != ctx['revision'] or
                self.projection() != ctx['projection'] or budget['account_count_today'] != ctx['used'] or
                budget['normal_daily_limit'] != ctx['normal_limit'] or ctx['used'] >= ctx['limit']):
                raise fail(409, 'Отправитель, дневной бюджет, защитная пауза или история изменились.')
            self.remaining(ctx['deadline'])
            return
        self.remaining(ctx['deadline'])
        a = self.ops.readable_account(ACCOUNT, check_cooldown=not allow_cooldown)
        row = self.ops.account_row(ACCOUNT)
        record = self.store.get(REQUEST_PREFIX + ctx['record']['request_key'])
        if (a is not ctx['account'] or a.client is not ctx['client'] or a.me.id != SENDER or
            row['telegram_id'] != SENDER or row['enabled'] != 0 or row['error'] != ctx['row_error'] or
            a.error != ctx['runtime_error'] or utc_day() != ctx['day'] or time.time() >= ctx['record']['expires_at'] or
            getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0 or
            self.store.get('outbound_pause_revision_primary', 0) != ctx['revision'] or
            self.projection(ctx['job_id']) != ctx['projection'] or not isinstance(record, dict) or
            record.get('state') not in ACTIVE or any(record.get(k) != ctx['record'].get(k) for k in
                ('request_key', 'digest', 'job_id', 'utc_day', 'scope', 'cohort_hash', 'attempts_claimed', 'expires_at', 'policy_digest', 'policy')) or
            self.record_mode(record) != ctx['mode'] or
            self.store.get(self.day_key(ctx['day'], ctx['mode'])) != record):
            raise fail(409, 'Контекст пакета или защитная пауза изменились.')
        current = self.store.rows('SELECT state,account,telegram_id,kind,spec,digest,request_key FROM op_jobs WHERE id=?', (ctx['job_id'],))
        if not current or current[0]['state'] != 'paused' or any(current[0].get(k) != v for k, v in ctx['job_binding'].items()):
            raise fail(409, 'Сохранённое контрольное задание изменилось.')
        items = self.store.rows('SELECT id,chat_id,user_id,user_name,user_access_hash,state FROM op_items WHERE job=? ORDER BY id', (ctx['job_id'],))
        if items != ctx['items']:
            raise fail(409, 'Состав или состояние участников пакета изменилось.')
        if any(item['state'] == 'sending' for item in items):
            proof = ctx.get('current_claim')
            claim = self.store.get(proof['key']) if isinstance(proof, dict) else None
            if not isinstance(claim, dict) or claim.get('scope') != SCOPE or any(
                claim.get(key) != proof.get(key) for key in ('job_id', 'item_id', 'user_id', 'request_key', 'policy_digest', 'utc_day', 'mode')):
                raise fail(409, 'Постоянный допуск текущего участника изменился.')
        other = self.store.rows('SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job '
            "WHERE j.account=? AND j.id!=? AND i.state='sending' LIMIT 1", (ACCOUNT, ctx['job_id']))
        budget = self.budget()
        expected = ctx['used'] + record['attempts_claimed']
        if (other or expected > ctx['limit'] or record['attempts_claimed'] > self.max_items(ctx['mode']) or
            budget['account_day_attempt_ceiling'] != ctx['limit'] or budget['normal_daily_limit'] != ctx['normal_limit'] or
            budget['account_count_today'] != expected or record.get('policy') != self.policy(ctx['mode']) or
            record.get('policy_digest') != digest(self.policy(ctx['mode'])) or self.other_control_active(record['request_key'])):
            raise fail(409, 'Общий дневной бюджет или другое действие аккаунта изменились.')
        if row['not_before'] > ctx['record']['next_at'] + .01:
            raise fail(409, 'Появилось новое ограничение ожидания аккаунта.')
        self.remaining(ctx['deadline'])

    def selected_item(self, ctx, iid):
        return next(item for item in ctx['items'] if item['id'] == iid)

    def stop(self, record, state, reason):
        record.update(state=state, stop_reason=reason, stopped_at=now())
        with self.store.db:
            self.save_batch(record)
            self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND state!='cancelled'", (now(), record['job_id']))
        self.wake.set()

    def public(self, record, mode=None):
        mode = self.record_mode(record) if mode is None else mode
        self.require_mode(mode)
        if record is not None and self.record_mode(record) != mode:
            raise fail(409, 'Режим сохранённого пакета изменился.')
        account = self.ops.account_row(ACCOUNT)
        runtime = self.ops.accounts.get(ACCOUNT)
        capacity = self.capacity()
        policy = self.policy(mode)
        can_start = (self.store.get(self.day_key(utc_day(), mode)) is None and
            not (self.task and not self.task.done()) and account['enabled'] == 0 and
            account['telegram_id'] == SENDER and account['not_before'] <= time.time() and
            runtime is not None and runtime.me is not None and runtime.me.id == SENDER and
            runtime.client is not None and runtime.client.is_connected() and not self.other_control_active() and
            capacity['remaining_attempts_today'] > 0 and capacity['goal_remaining'] > 0)
        base = {'scope': SCOPE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
            'mode': mode, 'max_attempts': self.max_items(mode), 'delay_seconds': DELAY, 'can_resume': False,
            'can_start': bool(can_start), 'can_pause': bool(record and record.get('state') in ACTIVE),
            'outbound_paused': account['enabled'] == 0, **capacity,
            'policy': policy, 'policy_digest': digest(policy)}
        if record is None:
            return {**base, 'state': 'unused', 'job_id': None, 'request_key': None, 'items': [],
                'counts': {k: 0 for k in COUNT_KEYS},
                'attempts_claimed': 0, 'next_attempt_not_before': None, 'expires_at': None, 'wait_seconds': 0, 'stop_reason': None}
        items = self.store.rows('SELECT id,user_id,user_name,state,attempted_at,finished_at,diagnosis FROM op_items WHERE job=? ORDER BY id', (record['job_id'],))
        counts = {k: 0 for k in COUNT_KEYS}
        out = []
        for item in items:
            diagnosis = json.loads(item['diagnosis'] or '{}')
            state = item['state']
            counts['total'] += 1
            if state == 'invited' and diagnosis.get('code') == 'invite_member_verified' and diagnosis.get('invitation_requested') is True:
                counts['added'] += 1
            elif state == 'invited':
                counts['uncertain'] += 1
            elif state in counts and state not in ('total', 'added'):
                counts[state] += 1
            else:
                counts['other'] += 1
            out.append({'item_id': item['id'], 'user_id': item['user_id'], 'username': item['user_name'],
                'state': state, 'diagnosis_code': diagnosis.get('code'),
                'invitation_requested': diagnosis.get('invitation_requested') is True,
                'attempted_at': item['attempted_at'], 'finished_at': item['finished_at']})
        wait = max(0, math.ceil(record['next_at'] - time.time())) if record['state'] in ACTIVE else 0
        return {**base, 'state': 'waiting' if wait else record['state'], 'job_id': record['job_id'],
            'request_key': record['request_key'], 'campaign_utc_day': record['utc_day'], 'counts': counts, 'items': out,
            'attempts_claimed': record['attempts_claimed'], 'next_attempt_not_before': iso(record['next_at']),
            'expires_at': iso(record['expires_at']), 'wait_seconds': wait, 'stop_reason': record.get('stop_reason'),
            'campaign_policy_digest': record['policy_digest'], 'audience': record.get('audience')}

    def latest(self, mode='mutual'):
        current = self.store.get(self.day_key(utc_day(), mode))
        if current is not None:
            return current
        rows = self.store.rows("SELECT value FROM settings WHERE key LIKE 'isolated_invite_campaign_day_%' ORDER BY key DESC")
        return next((record for row in rows if self.record_mode(record := json.loads(row['value'])) == mode), None)

    async def status(self, scope: Literal[SCOPE] = Query(...), mode: Mode = 'mutual'):
        self.require_scope(scope)
        self.require_mode(mode)
        # No lock/await: polling can show sending while the worker awaits Telegram.
        return self.public(self.latest(mode), mode)

    async def preview(self, scope: Literal[SCOPE] = Query(...), mode: Mode = 'mutual'):
        self.require_scope(scope)
        self.require_mode(mode)
        deadline = time.monotonic() + PREVIEW_TIMEOUT
        async with self.locked(deadline):
            if self.store.get(self.day_key(utc_day(), mode)) is not None or self.task and not self.task.done():
                raise fail(409, 'Контрольный пакет сегодня уже создан; нового допуска нет.')
            if time.monotonic() < self.next_preview:
                raise fail(429, 'Подождите минуту перед следующим предпросмотром.')
            ctx = self.context(deadline, mode)
            self.next_preview = time.monotonic() + 60
            self.candidate = None
            try:
                await self.identity(ctx)
                group = await self.group(ctx)
                contacts = await self.read(ctx, lambda: ctx['client'](functions.contacts.GetContactsRequest(hash=0)))
                if not isinstance(contacts, types.contacts.Contacts):
                    raise fail(409, 'Telegram не вернул свежие контакты.')
                saved = {c.user_id for c in contacts.contacts if isinstance(c, types.Contact)}
                excluded = self.history()
                selected, private, checked = [], [], 0
                eligible_seen = 0
                exhausted = True
                limit = min(self.max_items(mode), ctx['limit'] - ctx['used'])
                # First classify every saved UID locally. Only the membership
                # stage is bounded; its unread count never hides static reasons.
                entities = {}
                for user in contacts.users:
                    uid = getattr(user, 'id', None)
                    if uid in saved:
                        entities.setdefault(uid, user)
                funnel = {key: 0 for key in ('missing_entity', 'protected_identity', 'invalid_profile',
                    'no_username', 'not_saved_contact', 'non_mutual', 'prior_target_history',
                    'explicit_exclusion', 'already_member', 'unknown_membership', 'selected', 'unread_due_limit')}
                funnel['missing_entity'] = len(saved) - len(entities)
                eligible = []
                synthetic = {'id': -1, 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
                for user in entities.values():
                    self.remaining(deadline)
                    if user.id in EXCLUDED:
                        funnel['protected_identity'] += 1
                        continue
                    if (not isinstance(user, types.User) or user.min or user.bot or user.deleted or
                        type(user.access_hash) is not int):
                        funnel['invalid_profile'] += 1
                        continue
                    if not isinstance(user.username, str) or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{3,31}', user.username):
                        funnel['no_username'] += 1
                        continue
                    if user.contact is not True:
                        funnel['not_saved_contact'] += 1
                        continue
                    if mode == 'mutual' and user.mutual_contact is not True:
                        funnel['non_mutual'] += 1
                        continue
                    if user.id in excluded:
                        funnel['prior_target_history'] += 1
                        continue
                    item = {'chat_id': TARGET, 'user_id': user.id, 'user_name': user.username}
                    try:
                        self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                    except WorkbenchStopped:
                        funnel['explicit_exclusion'] += 1
                        continue
                    eligible.append(user)
                eligible_seen = len(eligible)
                self.guard(ctx)
                for user in eligible:
                    self.remaining(deadline)
                    member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                    checked += 1
                    if member['member'] is None:
                        funnel['unknown_membership'] += 1
                        raise fail(409, 'Неизвестное членство остановило подбор контактов.')
                    if member['member'] is False:
                        funnel['selected'] += 1
                        selected.append({'user_id': user.id, 'username': user.username, 'member': False,
                            'sender_has_contact': user.contact is True, 'mutual_contact': user.mutual_contact is True,
                            'checked_at': member['checked_at']})
                        private.append({'user_id': user.id, 'username': user.username, 'access_hash': user.access_hash})
                    else:
                        funnel['already_member'] += 1
                    if len(selected) >= limit or checked >= MAX_CHECKS:
                        exhausted = False
                        break
                funnel['unread_due_limit'] = len(eligible) - checked
                exhausted = checked == len(eligible)
                self.guard(ctx)
                policy = self.policy(mode)
                audience = {'saved_contact_count': len(saved), 'suitable_contacts_seen': eligible_seen,
                    'membership_checks': checked, 'nonmembers_selected': len(selected), 'selection_limit': limit,
                    'contact_response_exhausted': exhausted,
                    'partial': not exhausted, 'funnel': funnel,
                    'selected_mutual': sum(user['mutual_contact'] is True for user in selected),
                    'selected_non_mutual': sum(user['mutual_contact'] is False for user in selected),
                    'stop_reason': 'response_exhausted' if exhausted else
                        ('selection_limit' if len(selected) >= limit else 'membership_check_limit')}
                result = {'scope': SCOPE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
                    'mode': mode, 'selected': selected, 'membership_checks': checked, 'max_selected': self.max_items(mode),
                    'max_membership_checks': MAX_CHECKS, 'outbound_paused': True,
                    'history_changed': False, 'invitation_requested': False, 'candidate_token': None,
                    'token_ttl_seconds': 0, 'expires_at': None, **self.capacity(),
                    'policy': policy, 'policy_digest': digest(policy), 'audience': audience}
                if selected:
                    wall = time.time()
                    ttl = min(TOKEN_TTL, (int(wall) // 86400 + 1) * 86400 - wall)
                    token = secrets.token_urlsafe(32)
                    self.candidate = {'token': token, 'client': ctx['client'], 'revision': ctx['revision'],
                        'day': ctx['day'], 'expires': time.monotonic() + ttl, 'selected': private,
                        'policy_digest': digest(policy), 'policy': policy, 'used': ctx['used'],
                        'projection': ctx['projection'], 'audience': audience, 'mode': mode}
                    result.update(candidate_token=token, token_ttl_seconds=round(ttl, 3), expires_at=iso(wall + ttl))
                return result
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                raise fail(429 if isinstance(exc, errors.FloodWaitError) else 409,
                    'Telegram остановил подбор (' + type(exc).__name__ + '); приглашений нет.') from None
            except Exception as exc:
                from fastapi import HTTPException
                if isinstance(exc, HTTPException):
                    raise
                raise fail(504 if isinstance(exc, TimeoutError) else 502,
                    'Подбор не завершён (' + type(exc).__name__ + '); приглашений нет.') from None

    async def start(self, spec: CampaignStart):
        self.require_scope(spec.scope)
        self.require_mode(spec.mode)
        request = spec.model_dump()
        if spec.mode == 'mutual':
            request.pop('mode')  # existing R7 request digests remain compatible
        request_digest = digest(request)
        deadline = time.monotonic() + 15
        async with self.locked(deadline):
            existing = self.store.get(REQUEST_PREFIX + spec.request_key)
            if existing is not None:
                if not isinstance(existing, dict) or existing.get('digest') != request_digest:
                    raise fail(409, 'Ключ уже использован для другого пакета.')
                if self.record_mode(existing) != spec.mode:
                    raise fail(409, 'Ключ уже использован в другом режиме.')
                return self.public(existing)
            if self.store.get(self.day_key(utc_day(), spec.mode)) is not None or self.task and not self.task.done():
                raise fail(409, 'Сегодня пакет уже создан; повторного запуска нет.')
            proof = self.candidate
            if (not proof or not secrets.compare_digest(proof['token'], spec.candidate_token) or
                proof['day'] != utc_day() or time.monotonic() >= proof['expires'] or
                proof['mode'] != spec.mode or proof['policy_digest'] != spec.policy_digest or
                proof['policy'] != self.policy(spec.mode)):
                raise fail(409, 'Предпросмотр отсутствует или истёк.')
            ctx = self.context(min(deadline, proof['expires']), spec.mode)
            if (proof['client'] is not ctx['client'] or proof['revision'] != ctx['revision'] or
                proof['used'] != ctx['used'] or proof['projection'] != ctx['projection']):
                raise fail(409, 'Подключение или защитная пауза изменились.')
            candidates = proof['selected']
            if not 1 <= len(candidates) <= min(self.max_items(spec.mode), ctx['limit'] - ctx['used']):
                raise fail(409, 'Состав превышает оставшийся дневной бюджет.')
            if len({u['user_id'] for u in candidates}) != len(candidates) or any(u['user_id'] in self.history() for u in candidates):
                raise fail(409, 'Кто-то из выбранных уже появился в истории; обновите предпросмотр.')
            from operations import JobSpec
            timestamp = now()
            wall = time.time()
            expires = min(wall + RUN_TTL, (int(wall) // 86400 + 1) * 86400)
            # The cohort is authoritative server-owned op_items. Never widen the
            # ordinary JobSpec.users<=50 contract or expose internal access hashes.
            job_spec = JobSpec(request_key=spec.request_key, account=ACCOUNT, kind='invite', title='Кампания контактов · цель 100',
                destinations=[TARGET], users=[], consent_note=spec.reason,
                delay_seconds=DELAY, daily_limit=20).model_dump(mode='json')
            job_spec['isolated_invite_campaign'] = {'scope': SCOPE, 'request_key': spec.request_key,
                'max_attempts': self.max_items(spec.mode), 'policy_digest': spec.policy_digest,
                'server_owned_cohort': True, 'cohort_size': len(candidates)}
            if spec.mode == 'saved_contacts':
                job_spec['isolated_invite_campaign']['mode'] = spec.mode
            encoded_spec = json.dumps(job_spec, ensure_ascii=False)
            record = {'version': 2, 'scope': SCOPE, 'request_key': spec.request_key, 'digest': request_digest,
                'utc_day': ctx['day'], 'state': 'running', 'attempts_claimed': 0, 'requested_at': timestamp,
                'expires_at': expires, 'next_at': wall, 'cohort_hash': digest(candidates), 'stop_reason': None,
                'policy': proof['policy'], 'policy_digest': spec.policy_digest, 'audience': proof['audience']}
            if spec.mode == 'saved_contacts':
                record.update(version=3, mode=spec.mode)
            self.guard(ctx)
            with self.store.db:
                cur = self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('isolated-campaign:' + spec.request_key, request_digest, ACCOUNT, SENDER, 'invite', 'Кампания контактов · цель 100', encoded_spec, 'paused', timestamp, timestamp))
                record['job_id'] = cur.lastrowid
                for user in candidates:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,state,random_ids,user_access_hash) VALUES(?,?,?,?,?,?,?,?,?)',
                        (record['job_id'], TARGET, 'ТЕСТОВОЕ СООБЩЕСТВО', 'demo_identity_6', user['user_id'], user['username'], 'pending', '[]', user['access_hash']))
                for key in (self.day_key(ctx['day'], spec.mode), REQUEST_PREFIX + spec.request_key):
                    self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)', (key, json.dumps(record, ensure_ascii=False)))
            run_deadline = time.monotonic() + expires - wall
            ctx.update(batch=True, job_id=record['job_id'], record=record, run_deadline=run_deadline,
                deadline=min(time.monotonic() + STEP_TIMEOUT, run_deadline),
                job_binding={'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': encoded_spec,
                             'digest': request_digest, 'request_key': 'isolated-campaign:' + spec.request_key})
            ctx['items'] = self.store.rows('SELECT id,chat_id,user_id,user_name,user_access_hash,state FROM op_items WHERE job=? ORDER BY id', (record['job_id'],))
            self.active = ctx
            self.candidate = None
            self.wake.clear()
            self.task = asyncio.create_task(self.worker(ctx))
            return self.public(record)

    async def pause(self, spec: CampaignPause):
        self.require_scope(spec.scope)
        self.require_mode(spec.mode)
        async with self.locked(time.monotonic() + STEP_TIMEOUT + 5):
            record = self.store.get(REQUEST_PREFIX + spec.request_key)
            if (not isinstance(record, dict) or record.get('job_id') != spec.job_id or
                self.record_mode(record) != spec.mode):
                raise fail(409, 'Пакет не найден или изменился.')
            if record['state'] in ACTIVE:
                self.stop(record, 'paused', 'operator_pause')
            return self.public(record)

    def control_job(self, job, spec):
        # Called by normal Operations.control while it already owns ops.lock.
        if spec.action not in ('pause', 'cancel'):
            raise fail(409, 'Контрольный пакет не запускается общей очередью и не возобновляется.')
        marker = job['spec']['isolated_invite_campaign']
        record = self.store.get(REQUEST_PREFIX + marker['request_key'])
        if not isinstance(record, dict) or record.get('job_id') != job['id']:
            raise fail(409, 'Привязка контрольного пакета изменилась.')
        if record['state'] in ACTIVE:
            self.stop(record, 'paused', 'operator_pause' if spec.action == 'pause' else 'operator_cancel')
        if spec.action == 'cancel':
            with self.store.db:
                self.store.db.execute("UPDATE op_jobs SET state='cancelled',updated=? WHERE id=?", (now(), job['id']))
                self.store.db.execute("UPDATE op_items SET state='cancelled',finished_at=? WHERE job=? AND state='pending'", (now(), job['id']))
        return self.public(record)

    def reject_queue_job(self, job):
        """An unexpected queued marker must never be claimed by the old worker."""
        marker = job['spec']['isolated_invite_campaign']
        record = self.store.get(REQUEST_PREFIX + marker['request_key'])
        if isinstance(record, dict) and record.get('job_id') == job['id'] and record.get('state') in ACTIVE:
            self.stop(record, 'stopped', 'generic_queue_rejected')
        with self.store.db:
            self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'", (job['id'],))

    def persist_item(self, ctx, item, result, requested):
        record = ctx['record']
        diagnosis = result.setdefault('diagnosis', {})
        diagnosis.update(invitation_requested=requested, request_outcome_unknown=result['state'] == 'uncertain',
            isolated_invite_campaign={'scope': SCOPE, 'request_key': record['request_key'], 'max_attempts': MAX_ITEMS,
                'policy_digest': record['policy_digest'], 'account_day_attempt_ceiling': ACCOUNT_DAY_ATTEMPT_CEILING,
                'enabled_before': 0, 'enabled_after': self.ops.account_row(ACCOUNT)['enabled'],
                'primary_projection_before': ctx['projection'], 'primary_projection_after': self.projection(ctx['job_id']),
                'pause_revision_before': ctx['revision'], 'pause_revision_after': self.store.get('outbound_pause_revision_primary', 0)})
        diagnosis['isolated_invite_campaign']['max_attempts'] = self.max_items(ctx['mode'])
        if ctx['mode'] == 'saved_contacts':
            diagnosis['isolated_invite_campaign']['mode'] = ctx['mode']
        timestamp = now()
        with self.store.db:
            self.store.db.execute('UPDATE op_items SET state=?,error=?,diagnosis=?,finished_at=? WHERE id=? AND job=?',
                (result['state'], result.get('error'), json.dumps(diagnosis, ensure_ascii=False), timestamp, item['id'], ctx['job_id']))
            if requested or result['state'] == 'already_member':
                self.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?) ON CONFLICT(chat_id,user_id) DO NOTHING',
                    (TARGET, item['user_id'], result['state'], ctx['job_id']))
            self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=? AND telegram_id=?',
                (time.time() + DELAY, ACCOUNT, SENDER))
            record['next_at'] = self.ops.account_row(ACCOUNT)['not_before']
            self.save_batch(record)
        item['state'] = result['state']

    def claim_item(self, ctx, item):
        self.guard(ctx)
        record = ctx['record']
        key = ITEM_PREFIX + ctx['day'] + '_' + str(item['user_id'])
        if item['state'] != 'pending' or record['attempts_claimed'] >= self.max_items(ctx['mode']) or item['user_id'] in self.history(ctx['job_id']):
            raise fail(409, 'Получатель уже обработан или лимит пакета исчерпан.')
        if self.budget()['remaining_today'] < 1 or self.verified_today() >= GOAL_ADDED:
            raise fail(409, 'Дневной бюджет исчерпан или цель подтверждена; нового запроса нет.')
        timestamp = now()
        with self.store.db:
            self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)', (key, json.dumps({
                'scope': SCOPE, 'job_id': ctx['job_id'], 'item_id': item['id'], 'user_id': item['user_id'],
                'request_key': record['request_key'], 'policy_digest': record['policy_digest'],
                'utc_day': ctx['day'], 'mode': ctx['mode'], 'claimed_at': timestamp})))
            self.store.db.execute("UPDATE op_items SET state='sending',attempted_at=?,diagnosis=? WHERE id=? AND state='pending'",
                (timestamp, json.dumps({'source': 'isolated_invite_campaign', 'code': 'batch_dispatch_unknown',
                    'invitation_requested': True, 'request_outcome_unknown': True}), item['id']))
            record['attempts_claimed'] += 1
            self.save_batch(record)
        item['state'] = 'sending'
        ctx['current_claim'] = {'key': key, 'job_id': ctx['job_id'], 'item_id': item['id'],
            'user_id': item['user_id'], 'request_key': record['request_key'],
            'policy_digest': record['policy_digest'], 'utc_day': ctx['day'], 'mode': ctx['mode']}

    async def step(self, ctx, item):
        from operations import (invite_response_evidence, missing_invitee_diagnosis, invite_rpc_diagnosis,
                                unconfirmed_invite, invite_preflight_failure)
        requested, evidence, verification = False, None, []
        try:
            await self.identity(ctx)
            group = await self.group(ctx)
            users = await self.read(ctx, lambda: ctx['client'](functions.users.GetUsersRequest(
                [types.InputUser(item['user_id'], item['user_access_hash'])])))
            if len(users) != 1 or users[0].id != item['user_id'] or not self.eligible(users[0], ctx['mode']):
                return {'state': 'failed', 'diagnosis': {'source': 'contact_preflight', 'code': 'contact_changed', 'invitation_requested': False}}, False, False
            user = users[0]
            if item['user_id'] in self.history(ctx['job_id']):
                return {'state': 'duplicate_skipped', 'diagnosis': {'code': 'target_history_changed'}}, False, False
            member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
            if member['member'] is True:
                return {'state': 'already_member', 'diagnosis': {'source': 'membership_check', 'code': 'member_before_invite'}}, False, False
            if member['member'] is None:
                return invite_preflight_failure(fail(502, 'Неизвестное членство.')), False, True
            await self.read(ctx, lambda: self.ops.rights(ctx['account'], group, 'invite'))
            synthetic = {'id': ctx['job_id'], 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
            self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
            self.claim_item(ctx, item)
            self.guard(ctx)
            self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
            async def write_once():
                nonlocal requested
                self.guard(ctx)
                requested = True
                return await ctx['client'](functions.channels.InviteToChannelRequest(group, [user]))
            response = await asyncio.wait_for(write_once(), self.remaining(ctx['deadline']))
            evidence = invite_response_evidence(response, target_id=TARGET, user_id=user.id, sender_id=SENDER)
            self.guard(ctx)
            missing = next((m for m in getattr(response, 'missing_invitees', []) if m.user_id == user.id), None)
            if missing:
                diagnosis = missing_invitee_diagnosis(missing)
                diagnosis.update(response=evidence, verification=[])
                return {'state': 'restricted', 'error': diagnosis['reason'], 'diagnosis': diagnosis}, True, False
            for attempt, delay in enumerate((0, 1, 2), 1):
                if delay:
                    await self.read(ctx, lambda: asyncio.sleep(delay))
                checked = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                verification.append({'attempt': attempt, **checked})
                if checked['member'] is True:
                    return {'state': 'invited', 'diagnosis': {'source': 'membership_check', 'code': 'invite_member_verified',
                        'reason': 'После запроса Telegram подтвердил членство участника в группе.',
                        'next_action': 'Результат сохранён в истории панели.',
                        'response': evidence, 'verification': verification, 'retry_after_change': False,
                        'contact_evidence': {'sender_has_contact': user.contact is True,
                            'mutual_contact': user.mutual_contact is True, 'checked_at': now()}}}, True, False
            result = unconfirmed_invite('После запроса членство не подтверждено; пакет остановлен.', evidence, verification)
            if evidence['add_user_events'] and all(v.get('member') is False for v in verification):
                result['diagnosis']['code'] = 'invite_service_membership_conflict'
            return result, True, True
        except asyncio.CancelledError:
            if item['state'] == 'sending':
                self.persist_item(ctx, item, unconfirmed_invite('Проверка прервана; повтор отключён.', evidence, verification), True)
            raise
        except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
            self.protect(ctx, exc)
            result = (unconfirmed_invite('Telegram остановил пакет; результат требует проверки.', evidence, verification)
                if item['state'] == 'sending' else {'state': 'blocked', 'diagnosis': {}})
            result['diagnosis']['verification_error'] = invite_rpc_diagnosis(exc)
            return result, item['state'] == 'sending', True
        except Exception as exc:
            if item['state'] == 'sending':
                result = unconfirmed_invite('Результат неизвестен (' + type(exc).__name__ + '); пакет остановлен.', evidence, verification)
                if isinstance(exc, errors.RPCError) and evidence is None and not isinstance(exc, (errors.ServerError, errors.TimedOutError)):
                    diagnosis = invite_rpc_diagnosis(exc)
                    return {'state': 'restricted' if diagnosis.get('retry_after_change') else 'failed',
                        'error': diagnosis['reason'], 'diagnosis': diagnosis}, requested, (
                            not diagnosis.get('retry_after_change', False) or
                            ctx['mode'] == 'saved_contacts' and isinstance(exc, errors.UserNotMutualContactError))
                return result, True, True
            if isinstance(exc, WorkbenchStopped):
                return exc.result, False, False
            return invite_preflight_failure(exc), False, True

    async def wait_until_due(self, ctx):
        self.wake.clear()
        delay = min(60, max(0, ctx['record']['next_at'] - time.time()))
        if delay:
            try:
                await asyncio.wait_for(self.wake.wait(), delay)
            except TimeoutError:
                pass

    async def worker(self, ctx):
        record = ctx['record']
        try:
            while True:
                ctx['deadline'] = min(time.monotonic() + STEP_TIMEOUT, ctx['run_deadline'])
                async with self.locked(ctx['deadline']):
                    current = self.store.get(REQUEST_PREFIX + record['request_key'])
                    if not current or current.get('state') not in ACTIVE:
                        return
                    self.guard(ctx, allow_cooldown=True)
                    if self.verified_today() >= GOAL_ADDED:
                        self.stop(record, 'completed', 'daily_verified_goal_reached')
                        return
                    if self.budget()['remaining_today'] < 1:
                        self.stop(record, 'completed', 'account_day_attempt_budget_exhausted')
                        return
                    pending = next((item for item in ctx['items'] if item['state'] == 'pending'), None)
                    if not pending:
                        self.stop(record, 'completed', 'all_selected_processed')
                        return
                    if time.time() >= record['next_at']:
                        result, requested, stop = await self.step(ctx, pending)
                        self.persist_item(ctx, pending, result, requested)
                        if stop:
                            self.stop(record, 'stopped', result.get('diagnosis', {}).get('code') or 'telegram_or_context_stop')
                            return
                        continue
                await self.wait_until_due(ctx)
        except asyncio.CancelledError:
            self.stop(record, 'interrupted', 'worker_cancelled')
            raise
        except Exception as exc:
            self.stop(record, 'stopped', 'context_stop_' + type(exc).__name__)
        finally:
            self.active = None
