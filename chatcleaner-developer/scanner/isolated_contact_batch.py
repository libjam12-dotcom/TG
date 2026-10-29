"""Finite manually authorized contact batch; normal outbound remains disabled.

Only this in-memory worker may touch its paused job. Persistent claims prevent
replays; restart leaves pending items paused and never recreates the worker.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import math
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


SCOPE = 'client-russkie-contact-batch-v1'
DAY_PREFIX = 'isolated_contact_batch_day_'
REQUEST_PREFIX = 'isolated_contact_batch_request_'
ITEM_PREFIX = 'isolated_contact_batch_item_'
MAX_ITEMS = 10
MAX_CHECKS = 30
DELAY = 60
TOKEN_TTL = 300
RUN_TTL = 900
PREVIEW_TIMEOUT = 90
STEP_TIMEOUT = 80
ACTIVE = ('running', 'waiting')


class BatchStart(IsolatedAttempt):
    scope: Literal[SCOPE]


class BatchPause(BaseModel):
    model_config = {'extra': 'forbid'}
    scope: Literal[SCOPE]
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    job_id: int = Field(gt=0, strict=True)


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


class IsolatedContactBatch(IsolatedInvite):
    def __init__(self, ops):
        super().__init__(ops)
        self.task = None
        self.active = None
        self.wake = asyncio.Event()
        self.router = APIRouter(dependencies=[Depends(isolated_admin_only)])
        self.router.add_api_route('/isolated-invite-batch/contacts-preview', self.preview, methods=['GET'])
        self.router.add_api_route('/isolated-invite-batch/start', self.start, methods=['POST'])
        self.router.add_api_route('/isolated-invite-batch/status', self.status, methods=['GET'])
        self.router.add_api_route('/isolated-invite-batch/pause', self.pause, methods=['POST'])

    @staticmethod
    def require_scope(scope):
        if scope != SCOPE:
            raise fail(422, 'Неизвестная область контрольного пакета.')

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
            for row in self.store.rows("SELECT value FROM settings WHERE key LIKE 'isolated_contact_batch_day_%'"):
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
        marker = job.get('spec', {}).get('isolated_contact_batch')
        return isinstance(marker, dict) and marker.get('scope') == SCOPE

    def save_batch(self, record):
        for key in (DAY_PREFIX + record['utc_day'], REQUEST_PREFIX + record['request_key']):
            self.store.db.execute('UPDATE settings SET value=? WHERE key=?', (json.dumps(record, ensure_ascii=False), key))

    def history(self, exclude_job=None):
        rows = self.store.rows('SELECT user_id FROM op_items WHERE chat_id=? AND user_id IS NOT NULL AND job!=? '
            'UNION SELECT user_id FROM op_invited WHERE chat_id=?', (TARGET, exclude_job or -1, TARGET))
        return EXCLUDED | frozenset(row['user_id'] for row in rows)

    def guard(self, ctx, *, allow_cooldown=False):
        if not ctx.get('batch'):
            return super().guard(ctx)
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
                ('request_key', 'digest', 'job_id', 'utc_day', 'scope', 'cohort_hash', 'attempts_claimed', 'expires_at')) or
            self.store.get(DAY_PREFIX + ctx['day']) != record):
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
                claim.get(key) != proof.get(key) for key in ('job_id', 'item_id', 'user_id', 'request_key')):
                raise fail(409, 'Постоянный допуск текущего участника изменился.')
        other = self.store.rows('SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job '
            "WHERE j.account=? AND j.id!=? AND i.state='sending' LIMIT 1", (ACCOUNT, ctx['job_id']))
        budget = self.budget()
        expected = ctx['used'] + record['attempts_claimed']
        if (other or expected > ctx['limit'] or record['attempts_claimed'] > MAX_ITEMS or
            budget['normal_daily_limit'] != ctx['limit'] or budget['account_count_today'] != expected):
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

    def public(self, record):
        account = self.ops.account_row(ACCOUNT)
        runtime = self.ops.accounts.get(ACCOUNT)
        can_start = (self.store.get(DAY_PREFIX + utc_day()) is None and
            not (self.task and not self.task.done()) and account['enabled'] == 0 and
            account['telegram_id'] == SENDER and account['not_before'] <= time.time() and
            runtime is not None and runtime.me is not None and runtime.me.id == SENDER and
            runtime.client is not None and runtime.client.is_connected() and self.budget()['remaining_today'] > 0)
        base = {'scope': SCOPE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
            'max_attempts': MAX_ITEMS, 'delay_seconds': DELAY, 'can_resume': False,
            'can_start': bool(can_start), 'can_pause': bool(record and record.get('state') in ACTIVE),
            'outbound_paused': account['enabled'] == 0}
        if record is None:
            return {**base, 'state': 'unused', 'job_id': None, 'request_key': None, 'items': [],
                'counts': {k: 0 for k in ('total', 'added', 'already_member', 'restricted', 'failed', 'blocked', 'uncertain', 'pending', 'sending')},
                'attempts_claimed': 0, 'next_attempt_not_before': None, 'expires_at': None, 'wait_seconds': 0, 'stop_reason': None}
        items = self.store.rows('SELECT id,user_id,user_name,state,attempted_at,finished_at,diagnosis FROM op_items WHERE job=? ORDER BY id', (record['job_id'],))
        counts = {k: 0 for k in ('total', 'added', 'already_member', 'restricted', 'failed', 'blocked', 'uncertain', 'pending', 'sending')}
        out = []
        for item in items:
            diagnosis = json.loads(item['diagnosis'] or '{}')
            state = item['state']
            counts['total'] += 1
            if state == 'invited' and diagnosis.get('code') == 'invite_member_verified':
                counts['added'] += 1
            elif state in counts:
                counts[state] += 1
            out.append({'item_id': item['id'], 'user_id': item['user_id'], 'username': item['user_name'],
                'state': state, 'diagnosis_code': diagnosis.get('code'),
                'attempted_at': item['attempted_at'], 'finished_at': item['finished_at']})
        wait = max(0, math.ceil(record['next_at'] - time.time())) if record['state'] in ACTIVE else 0
        return {**base, 'state': 'waiting' if wait else record['state'], 'job_id': record['job_id'],
            'request_key': record['request_key'], 'utc_day': record['utc_day'], 'counts': counts, 'items': out,
            'attempts_claimed': record['attempts_claimed'], 'next_attempt_not_before': iso(record['next_at']),
            'expires_at': iso(record['expires_at']), 'wait_seconds': wait, 'stop_reason': record.get('stop_reason')}

    def latest(self):
        current = self.store.get(DAY_PREFIX + utc_day())
        if current is not None:
            return current
        rows = self.store.rows("SELECT value FROM settings WHERE key LIKE 'isolated_contact_batch_day_%' ORDER BY key DESC LIMIT 1")
        return json.loads(rows[0]['value']) if rows else None

    async def status(self, scope: Literal[SCOPE] = Query(...)):
        self.require_scope(scope)
        # No lock/await: polling can show sending while the worker awaits Telegram.
        return self.public(self.latest())

    async def preview(self, scope: Literal[SCOPE] = Query(...)):
        self.require_scope(scope)
        deadline = time.monotonic() + PREVIEW_TIMEOUT
        async with self.locked(deadline):
            if self.store.get(DAY_PREFIX + utc_day()) is not None or self.task and not self.task.done():
                raise fail(409, 'Контрольный пакет сегодня уже создан; нового допуска нет.')
            if time.monotonic() < self.next_preview:
                raise fail(429, 'Подождите минуту перед следующим предпросмотром.')
            ctx = self.context(deadline)
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
                selected, private, checked, seen = [], [], 0, set()
                limit = min(MAX_ITEMS, ctx['limit'] - ctx['used'])
                synthetic = {'id': -1, 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
                for user in contacts.users:
                    self.remaining(deadline)
                    if not self.suitable(user) or user.id not in saved or user.id in excluded or user.id in seen:
                        continue
                    seen.add(user.id)
                    item = {'chat_id': TARGET, 'user_id': user.id, 'user_name': user.username}
                    try:
                        self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                    except WorkbenchStopped:
                        continue
                    if user.id in self.history():
                        continue
                    member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                    checked += 1
                    if member['member'] is None:
                        raise fail(409, 'Неизвестное членство остановило подбор контактов.')
                    if member['member'] is False:
                        selected.append({'user_id': user.id, 'username': user.username, 'member': False,
                            'sender_has_contact': True, 'mutual_contact': True, 'checked_at': member['checked_at']})
                        private.append({'user_id': user.id, 'username': user.username, 'access_hash': user.access_hash})
                    if len(selected) >= limit or checked >= MAX_CHECKS:
                        break
                result = {'scope': SCOPE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
                    'selected': selected, 'membership_checks': checked, 'max_selected': MAX_ITEMS,
                    'max_membership_checks': MAX_CHECKS, 'outbound_paused': True,
                    'history_changed': False, 'invitation_requested': False, 'candidate_token': None,
                    'token_ttl_seconds': 0, 'expires_at': None}
                if selected:
                    wall = time.time()
                    ttl = min(TOKEN_TTL, (int(wall) // 86400 + 1) * 86400 - wall)
                    token = secrets.token_urlsafe(32)
                    self.candidate = {'token': token, 'client': ctx['client'], 'revision': ctx['revision'],
                        'day': ctx['day'], 'expires': time.monotonic() + ttl, 'selected': private}
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

    async def start(self, spec: BatchStart):
        self.require_scope(spec.scope)
        request_digest = digest(spec.model_dump())
        deadline = time.monotonic() + 15
        async with self.locked(deadline):
            existing = self.store.get(REQUEST_PREFIX + spec.request_key)
            if existing is not None:
                if not isinstance(existing, dict) or existing.get('digest') != request_digest:
                    raise fail(409, 'Ключ уже использован для другого пакета.')
                return self.public(existing)
            if self.store.get(DAY_PREFIX + utc_day()) is not None or self.task and not self.task.done():
                raise fail(409, 'Сегодня пакет уже создан; повторного запуска нет.')
            proof = self.candidate
            if (not proof or not secrets.compare_digest(proof['token'], spec.candidate_token) or
                proof['day'] != utc_day() or time.monotonic() >= proof['expires']):
                raise fail(409, 'Предпросмотр отсутствует или истёк.')
            ctx = self.context(min(deadline, proof['expires']))
            if proof['client'] is not ctx['client'] or proof['revision'] != ctx['revision']:
                raise fail(409, 'Подключение или защитная пауза изменились.')
            candidates = proof['selected']
            if not 1 <= len(candidates) <= min(MAX_ITEMS, ctx['limit'] - ctx['used']):
                raise fail(409, 'Состав превышает оставшийся дневной бюджет.')
            if len({u['user_id'] for u in candidates}) != len(candidates) or any(u['user_id'] in self.history() for u in candidates):
                raise fail(409, 'Кто-то из выбранных уже появился в истории; обновите предпросмотр.')
            from operations import JobSpec
            timestamp = now()
            wall = time.time()
            expires = min(wall + RUN_TTL, (int(wall) // 86400 + 1) * 86400)
            job_spec = JobSpec(request_key=spec.request_key, account=ACCOUNT, kind='invite', title='Контрольный пакет контактов',
                destinations=[TARGET], users=['@' + u['username'] for u in candidates], consent_note=spec.reason,
                delay_seconds=DELAY, daily_limit=20).model_dump(mode='json')
            job_spec['isolated_contact_batch'] = {'scope': SCOPE, 'request_key': spec.request_key, 'max_attempts': MAX_ITEMS}
            encoded_spec = json.dumps(job_spec, ensure_ascii=False)
            record = {'version': 1, 'scope': SCOPE, 'request_key': spec.request_key, 'digest': request_digest,
                'utc_day': ctx['day'], 'state': 'running', 'attempts_claimed': 0, 'requested_at': timestamp,
                'expires_at': expires, 'next_at': wall, 'cohort_hash': digest(candidates), 'stop_reason': None}
            self.guard(ctx)
            with self.store.db:
                cur = self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('isolated-batch:' + spec.request_key, request_digest, ACCOUNT, SENDER, 'invite', 'Контрольный пакет контактов', encoded_spec, 'paused', timestamp, timestamp))
                record['job_id'] = cur.lastrowid
                for user in candidates:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,state,random_ids,user_access_hash) VALUES(?,?,?,?,?,?,?,?,?)',
                        (record['job_id'], TARGET, 'ТЕСТОВОЕ СООБЩЕСТВО', 'demo_identity_6', user['user_id'], user['username'], 'pending', '[]', user['access_hash']))
                for key in (DAY_PREFIX + ctx['day'], REQUEST_PREFIX + spec.request_key):
                    self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)', (key, json.dumps(record, ensure_ascii=False)))
            run_deadline = time.monotonic() + expires - wall
            ctx.update(batch=True, job_id=record['job_id'], record=record, run_deadline=run_deadline,
                deadline=min(time.monotonic() + STEP_TIMEOUT, run_deadline),
                job_binding={'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': encoded_spec,
                             'digest': request_digest, 'request_key': 'isolated-batch:' + spec.request_key})
            ctx['items'] = self.store.rows('SELECT id,chat_id,user_id,user_name,user_access_hash,state FROM op_items WHERE job=? ORDER BY id', (record['job_id'],))
            self.active = ctx
            self.candidate = None
            self.wake.clear()
            self.task = asyncio.create_task(self.worker(ctx))
            return self.public(record)

    async def pause(self, spec: BatchPause):
        self.require_scope(spec.scope)
        async with self.locked(time.monotonic() + STEP_TIMEOUT + 5):
            record = self.store.get(REQUEST_PREFIX + spec.request_key)
            if not isinstance(record, dict) or record.get('job_id') != spec.job_id:
                raise fail(409, 'Пакет не найден или изменился.')
            if record['state'] in ACTIVE:
                self.stop(record, 'paused', 'operator_pause')
            return self.public(record)

    def control_job(self, job, spec):
        # Called by normal Operations.control while it already owns ops.lock.
        if spec.action not in ('pause', 'cancel'):
            raise fail(409, 'Контрольный пакет не запускается общей очередью и не возобновляется.')
        marker = job['spec']['isolated_contact_batch']
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
        marker = job['spec']['isolated_contact_batch']
        record = self.store.get(REQUEST_PREFIX + marker['request_key'])
        if isinstance(record, dict) and record.get('job_id') == job['id'] and record.get('state') in ACTIVE:
            self.stop(record, 'stopped', 'generic_queue_rejected')
        with self.store.db:
            self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'", (job['id'],))

    def persist_item(self, ctx, item, result, requested):
        record = ctx['record']
        diagnosis = result.setdefault('diagnosis', {})
        diagnosis.update(invitation_requested=requested, request_outcome_unknown=result['state'] == 'uncertain',
            isolated_contact_batch={'scope': SCOPE, 'request_key': record['request_key'], 'max_attempts': MAX_ITEMS,
                'enabled_before': 0, 'enabled_after': self.ops.account_row(ACCOUNT)['enabled'],
                'primary_projection_before': ctx['projection'], 'primary_projection_after': self.projection(ctx['job_id']),
                'pause_revision_before': ctx['revision'], 'pause_revision_after': self.store.get('outbound_pause_revision_primary', 0)})
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
        if item['state'] != 'pending' or record['attempts_claimed'] >= MAX_ITEMS or item['user_id'] in self.history(ctx['job_id']):
            raise fail(409, 'Получатель уже обработан или лимит пакета исчерпан.')
        timestamp = now()
        with self.store.db:
            self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)', (key, json.dumps({
                'scope': SCOPE, 'job_id': ctx['job_id'], 'item_id': item['id'], 'user_id': item['user_id'],
                'request_key': record['request_key'], 'claimed_at': timestamp})))
            self.store.db.execute("UPDATE op_items SET state='sending',attempted_at=?,diagnosis=? WHERE id=? AND state='pending'",
                (timestamp, json.dumps({'source': 'isolated_contact_batch', 'code': 'batch_dispatch_unknown',
                    'invitation_requested': True, 'request_outcome_unknown': True}), item['id']))
            record['attempts_claimed'] += 1
            self.save_batch(record)
        item['state'] = 'sending'
        ctx['current_claim'] = {'key': key, 'job_id': ctx['job_id'], 'item_id': item['id'],
            'user_id': item['user_id'], 'request_key': record['request_key']}

    async def step(self, ctx, item):
        from operations import (invite_response_evidence, missing_invitee_diagnosis, invite_rpc_diagnosis,
                                unconfirmed_invite, invite_preflight_failure)
        requested, evidence, verification = False, None, []
        try:
            await self.identity(ctx)
            group = await self.group(ctx)
            users = await self.read(ctx, lambda: ctx['client'](functions.users.GetUsersRequest(
                [types.InputUser(item['user_id'], item['user_access_hash'])])))
            if len(users) != 1 or users[0].id != item['user_id'] or not self.suitable(users[0]):
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
                        'contact_evidence': {'sender_has_contact': True, 'mutual_contact': True, 'checked_at': now()}}}, True, False
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
                        'error': diagnosis['reason'], 'diagnosis': diagnosis}, requested, not diagnosis.get('retry_after_change', False)
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
