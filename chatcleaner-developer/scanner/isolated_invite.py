"""One explicitly confirmed CLIENT contact control while general outbound stays off.

This never enables an account, queues a job, imports contacts, or retries a write.
The durable UTC-day claim is consumed even if the request result is unknown.
"""
import asyncio
import hashlib
import json
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator
from telethon import errors, functions, types, utils

from engine import now


SCOPE = 'client-russkie-contact-v1'
ACCOUNT = 'primary'
SENDER = 710000015
TARGET = -1000720000000
EXCLUDED = frozenset((SENDER, 710000010, 710000016, 710000012, 710000011,
    710000013, 710000005, 710000007, 710000008, 710000009, 710000014,
    710000003, 710000004))
TOKEN_TTL = 300
PREVIEW_TIMEOUT = 45
ATTEMPT_TIMEOUT = 80
MAX_MEMBERSHIP_READS = 10
DAY_PREFIX = 'isolated_invite_day_'
REQUEST_PREFIX = 'isolated_invite_request_'
Mode = Literal['mutual', 'saved_contacts']
REVOKED = (errors.SessionRevokedError, errors.AuthKeyUnregisteredError,
           errors.UserDeactivatedError, errors.UserDeactivatedBanError)


def fail(code, text):
    return HTTPException(code, text, headers={'Cache-Control': 'no-store'})


async def isolated_admin_only(request: Request, response: Response):
    # The loopback API trusts identity headers overwritten by the panel gateway.
    if not request.headers.get('x-panel-user') or request.headers.get('x-panel-role') not in ('owner', 'admin'):
        raise fail(403, 'Нужен доступ владельца или администратора панели.')
    response.headers['Cache-Control'] = 'no-store'


class IsolatedAttempt(BaseModel):
    model_config = {'extra': 'forbid'}
    scope: Literal[SCOPE]
    confirmed: Literal[True]
    reason: str = Field(min_length=8, max_length=500)
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    candidate_token: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')

    @field_validator('confirmed', mode='before')
    @classmethod
    def explicit_confirmation(cls, value):
        if value is not True:
            raise ValueError('Нужно явное подтверждение.')
        return value

    @field_validator('reason')
    @classmethod
    def meaningful_reason(cls, value):
        value = value.strip()
        if len(value) < 8:
            raise ValueError('Укажите основание проверки.')
        value.encode('utf-8')
        return value


class SingleContactAttempt(IsolatedAttempt):
    # R6/R7 inherit IsolatedAttempt. Keep their schemas and request digests intact.
    mode: Mode = 'mutual'


def utc_day():
    return datetime.now(timezone.utc).date().isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class IsolatedInvite:
    scope = SCOPE
    target_id = TARGET
    control_title = 'Одиночный контроль контакта'

    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.candidate = None
        self.next_preview = 0
        self.router = APIRouter(dependencies=[Depends(isolated_admin_only)])
        self.router.add_api_route('/isolated-invite/contacts-preview', self.preview, methods=['GET'])
        self.router.add_api_route('/isolated-invite/attempt', self.attempt, methods=['POST'])
        self.router.add_api_route('/isolated-invite/status', self.status, methods=['GET'])
        # A separate, fixed owned-account diagnostic shares this class's safety gates.
        if type(self) is IsolatedInvite:
            from owned_target_invite import OwnedTargetInvite
            self.owned_target = OwnedTargetInvite(ops)
            self.router.include_router(self.owned_target.router)

    @classmethod
    def require_scope(cls, scope):
        if scope != cls.scope:
            raise fail(422, 'Неизвестная область одиночного контроля.')

    @staticmethod
    def remaining(deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise fail(504, 'Время одиночной проверки истекло; автоматического повтора нет.')
        return remaining

    @asynccontextmanager
    async def locked(self, deadline):
        try:
            await asyncio.wait_for(self.ops.lock.acquire(), self.remaining(deadline))
        except TimeoutError:
            raise fail(504, 'Другое действие заняло время контроля; новый запрос не запускался.') from None
        try:
            self.remaining(deadline)
            yield
        finally:
            self.ops.lock.release()

    def projection(self, exclude_job=None):
        jobs = self.store.rows('SELECT id,state FROM op_jobs WHERE account=? AND id!=? ORDER BY id',
                               (ACCOUNT, exclude_job or -1))
        items = self.store.rows('SELECT i.id,i.state,i.attempted_at FROM op_items i JOIN op_jobs j ON j.id=i.job '
                                'WHERE j.account=? AND j.id!=? ORDER BY i.id', (ACCOUNT, exclude_job or -1))
        return digest({'jobs': jobs, 'items': items})

    def history(self):
        rows = self.store.rows('SELECT user_id FROM op_items WHERE chat_id=? AND user_id IS NOT NULL '
                               'UNION SELECT user_id FROM op_invited WHERE chat_id=?', (self.target_id, self.target_id))
        return EXCLUDED | frozenset(row['user_id'] for row in rows)

    def budget(self):
        job = {'account': ACCOUNT, 'telegram_id': SENDER, 'spec': {'daily_limit': 20}}
        limit = self.ops.normal_daily_limit(job)
        used = self.ops.account_daily_count(ACCOUNT, utc_day())
        return {'normal_daily_limit': limit, 'account_count_today': used,
                'remaining_today': max(0, limit - used)}

    def context(self, deadline):
        self.remaining(deadline)
        a = self.ops.readable_account(ACCOUNT)
        row = self.ops.account_row(ACCOUNT)
        if a.me.id != SENDER or row['telegram_id'] != SENDER or row['enabled'] != 0:
            raise fail(409, 'Нужен прежний подключённый клиентский аккаунт с остановленными исходящими.')
        if getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0:
            raise fail(409, 'Подключённый клиент не подтверждает отключение автоматических повторов RPC.')
        if self.store.rows('SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job '
                           "WHERE j.account=? AND i.state='sending' LIMIT 1", (ACCOUNT,)):
            raise fail(409, 'У аккаунта уже есть незавершённое действие.')
        budget = self.budget()
        if budget['remaining_today'] < 1:
            raise fail(409, 'Дневной бюджет исчерпан; одиночный контроль его не увеличивает.')
        result = {'account': a, 'client': a.client, 'day': utc_day(), 'row_error': row['error'],
                  'runtime_error': a.error, 'revision': self.store.get('outbound_pause_revision_primary', 0),
                  'projection': self.projection(), 'used': budget['account_count_today'], 'limit': budget['normal_daily_limit'],
                  'deadline': deadline, 'job_id': None, 'item_id': None, 'record': None,
                  'control_scope': self.scope, 'target_id': self.target_id}
        self.guard(result)
        return result

    def guard(self, ctx):
        self.remaining(ctx['deadline'])
        a = self.ops.readable_account(ACCOUNT)
        row = self.ops.account_row(ACCOUNT)
        if (ctx['control_scope'] != self.scope or ctx['target_id'] != self.target_id or
            a is not ctx['account'] or a.client is not ctx['client'] or a.me.id != SENDER or
            row['telegram_id'] != SENDER or row['enabled'] != 0 or row['error'] != ctx['row_error'] or
            a.error != ctx['runtime_error'] or utc_day() != ctx['day'] or
            getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0 or
            self.store.get('outbound_pause_revision_primary', 0) != ctx['revision'] or
            self.projection(ctx['job_id']) != ctx['projection']):
            raise fail(409, 'Отправитель, защитная пауза или сохранённые действия изменились.')
        rows = self.store.rows('SELECT i.id FROM op_items i JOIN op_jobs j ON j.id=i.job '
                               "WHERE j.account=? AND i.state='sending' AND i.id!=? LIMIT 1", (ACCOUNT, ctx['item_id'] or -1))
        if rows:
            raise fail(409, 'Появилось другое незавершённое действие аккаунта.')
        budget = self.budget()
        expected = ctx['used'] + (1 if ctx['job_id'] else 0)
        if budget['normal_daily_limit'] != ctx['limit'] or budget['account_count_today'] != expected or expected > ctx['limit']:
            raise fail(409, 'Дневной бюджет изменился; запуск остановлен.')
        if ctx['job_id']:
            jobs = self.store.rows('SELECT state,account,telegram_id,kind,digest,request_key,spec FROM op_jobs WHERE id=?', (ctx['job_id'],))
            items = self.store.rows('SELECT state,chat_id,user_id,user_access_hash FROM op_items WHERE id=? AND job=?', (ctx['item_id'], ctx['job_id']))
            record = self.store.get(DAY_PREFIX + ctx['day'])
            if (not jobs or jobs[0]['state'] != 'paused' or not items or items[0]['state'] != 'sending' or
                any(jobs[0][key] != value for key, value in ctx['job_binding'].items()) or
                any(items[0][key] != value for key, value in ctx['item_binding'].items()) or
                not isinstance(record, dict) or record.get('state') != 'claimed' or
                any(record.get(key) != ctx['record'].get(key) for key in ('request_key', 'digest', 'scope', 'utc_day', 'job_id', 'item_id', 'user_id', 'target_id')) or
                record.get('mode') != ctx['single_binding']['mode'] or
                record.get('contact_evidence') != ctx['single_binding']['contact_evidence'] or
                self.store.get(REQUEST_PREFIX + record['request_key']) != record):
                raise fail(409, 'Однократный допуск или запись истории изменились.')
        self.remaining(ctx['deadline'])

    async def read(self, ctx, call):
        self.guard(ctx)
        result = await asyncio.wait_for(call(), self.remaining(ctx['deadline']))
        self.guard(ctx)
        return result

    async def identity(self, ctx):
        me = await self.read(ctx, ctx['client'].get_me)
        if not isinstance(me, types.User) or me.id != SENDER or me.bot or me.deleted:
            raise fail(409, 'Telegram не подтвердил личность клиентского аккаунта.')
        ctx['identity_checked_at'] = now()

    async def group(self, ctx):
        group = await self.read(ctx, lambda: ctx['client'].get_entity(self.target_id))
        if not isinstance(group, types.Channel) or not group.megagroup or utils.get_peer_id(group) != self.target_id:
            raise fail(409, 'Telegram не подтвердил исходную целевую группу.')
        await self.read(ctx, lambda: self.ops.rights(ctx['account'], group, 'invite'))
        return group

    @staticmethod
    def suitable(user):
        return (isinstance(user, types.User) and not user.min and not user.bot and not user.deleted and
                user.id not in EXCLUDED and user.contact is True and user.mutual_contact is True and
                isinstance(user.username, str) and re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{3,31}', user.username) and
                type(user.access_hash) is int)

    @staticmethod
    def single_mode(mode):
        if mode not in ('mutual', 'saved_contacts'):
            raise fail(422, 'Неизвестный режим одиночного контакта.')

    def single_eligible(self, user, mode):
        self.single_mode(mode)
        if mode == 'mutual':
            return self.suitable(user)
        return (isinstance(user, types.User) and not user.min and not user.bot and not user.deleted and
                user.id not in EXCLUDED and user.contact is True and
                isinstance(user.username, str) and re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{3,31}', user.username) and
                type(user.access_hash) is int and type(user.mutual_contact) is bool)

    @staticmethod
    def single_contact_evidence(user, checked_at):
        return {'sender_has_contact': user.contact is True,
                'mutual_contact': user.mutual_contact is True, 'checked_at': checked_at}

    def protect(self, ctx, exc):
        # Protect only the exact existing identity; never alter a replacement client.
        a = self.ops.accounts.get(ACCOUNT)
        if a is not ctx['account'] or a.client is not ctx['client'] or not a.me or a.me.id != SENDER:
            return
        if isinstance(exc, errors.FloodWaitError):
            with self.store.db:
                self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=? AND telegram_id=?',
                                      (time.time() + exc.seconds + 1, ACCOUNT, SENDER))
        elif isinstance(exc, (errors.PeerFloodError, *REVOKED)):
            self.ops.pause_outbound(ACCOUNT, exc)

    async def preview(self, scope: Literal[SCOPE] = Query(...), mode: Mode = 'mutual'):
        self.require_scope(scope)
        self.single_mode(mode)
        deadline = time.monotonic() + PREVIEW_TIMEOUT
        async with self.locked(deadline):
            if self.store.get(DAY_PREFIX + utc_day()) is not None:
                raise fail(409, 'Сегодня одиночный контроль уже использован; нового допуска нет.')
            if time.monotonic() < self.next_preview:
                raise fail(429, 'Повторный предпросмотр доступен через минуту.')
            ctx = self.context(deadline)
            self.candidate = None
            self.next_preview = time.monotonic() + 60
            try:
                await self.identity(ctx)
                group = await self.group(ctx)
                contacts = await self.read(ctx, lambda: ctx['client'](functions.contacts.GetContactsRequest(hash=0)))
                if not isinstance(contacts, types.contacts.Contacts):
                    raise fail(409, 'Telegram не вернул свежий список контактов.')
                saved_ids = {contact.user_id for contact in contacts.contacts if isinstance(contact, types.Contact)}
                excluded = self.history()
                self.guard(ctx)
                checked = 0
                for user in contacts.users:
                    self.remaining(deadline)
                    if not self.single_eligible(user, mode) or user.id not in saved_ids or user.id in excluded:
                        continue
                    synthetic = {'id': -1, 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
                    item = {'chat_id': self.target_id, 'user_id': user.id, 'user_name': user.username}
                    from operation_workbench import WorkbenchStopped
                    try:
                        self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                    except WorkbenchStopped:
                        continue
                    if user.id in self.history():
                        continue
                    checked += 1
                    member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                    if user.id in self.history():
                        raise fail(409, 'Получатель появился в истории во время проверки.')
                    if member['member'] is None:
                        raise fail(409, 'Членство не определено; дальнейший подбор остановлен.')
                    if member['member'] is False:
                        token = secrets.token_urlsafe(32)
                        wall = time.time()
                        ttl = min(TOKEN_TTL, (int(wall) // 86400 + 1) * 86400 - wall)
                        self.candidate = {'token': token, 'client': ctx['client'], 'revision': ctx['revision'],
                            'scope': self.scope, 'mode': mode, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': self.target_id,
                            'member_before': False,
                            'day': ctx['day'], 'expires': time.monotonic() + ttl,
                            'user_id': user.id, 'username': user.username, 'access_hash': user.access_hash,
                            'checked_at': member['checked_at']}
                        contact = self.single_contact_evidence(user, member['checked_at'])
                        return {'scope': self.scope, 'mode': mode, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': self.target_id,
                            'candidate_token': token, 'token_ttl_seconds': round(ttl, 3),
                            'expires_at': datetime.fromtimestamp(wall + ttl, timezone.utc).isoformat(),
                            'selected': {'user_id': user.id, 'username': user.username, 'member': False,
                                         **contact},
                            'membership_checks': checked, 'history_changed': False, 'invitation_requested': False,
                            'outbound_paused': True, 'checked_at': now()}
                    if checked >= MAX_MEMBERSHIP_READS:
                        break
                return {'scope': self.scope, 'mode': mode, 'selected': None, 'membership_checks': checked,
                        'history_changed': False, 'invitation_requested': False, 'outbound_paused': True,
                        'reason': ('В пределах проверки новый взаимный контакт не найден.' if mode == 'mutual'
                                   else 'В пределах проверки новый сохранённый контакт не найден.'), 'checked_at': now()}
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                raise fail(429 if isinstance(exc, errors.FloodWaitError) else 409,
                           'Telegram остановил проверку (' + type(exc).__name__ + '); повтор не запускался.') from None
            except HTTPException:
                raise
            except Exception as exc:
                raise fail(504 if isinstance(exc, TimeoutError) else 502,
                           'Предпросмотр не завершён (' + type(exc).__name__ + '); приглашение не запускалось.') from None

    def save_record(self, record):
        for key in (DAY_PREFIX + record['utc_day'], REQUEST_PREFIX + record['request_key']):
            self.store.db.execute('UPDATE settings SET value=? WHERE key=?',
                                  (json.dumps(record, ensure_ascii=False), key))

    @staticmethod
    def public(record):
        keys = ('state', 'request_key', 'utc_day', 'job_id', 'item_id', 'user_id', 'username',
                'claimed_at', 'finished_at', 'result_state', 'invitation_requested', 'request_started_at')
        contact = record.get('contact_evidence', {'sender_has_contact': True, 'mutual_contact': True})
        return {'scope': record.get('scope', SCOPE), 'mode': record.get('mode', 'mutual'),
                'account': ACCOUNT, 'sender_id': SENDER, 'target_id': record.get('target_id', TARGET),
                'sender_has_contact': contact['sender_has_contact'], 'mutual_contact': contact['mutual_contact'],
                **{key: record.get(key) for key in keys}, 'outbound_paused': True, 'automatic_retry': False}

    async def status(self, scope: Literal[SCOPE] = Query(...)):
        self.require_scope(scope)
        async with self.locked(time.monotonic() + 10):
            record = self.store.get(DAY_PREFIX + utc_day())
            return self.public(record) if isinstance(record, dict) else {'scope': self.scope, 'state': 'unused', 'utc_day': utc_day()}

    def claim(self, ctx, spec, group, user, request_digest):
        self.guard(ctx)
        if user.id in self.history():
            raise fail(409, 'Получатель уже присутствует в истории целевой группы; повтора нет.')
        timestamp = now()
        # No task is ever queued. Existing startup recovery treats sending as unknown.
        from operations import JobSpec
        job_spec = JobSpec(request_key=spec.request_key, account=ACCOUNT, kind='invite',
            title=self.control_title, destinations=[self.target_id], users=['@' + user.username],
            consent_note=spec.reason, delay_seconds=60, daily_limit=20).model_dump(mode='json')
        mode = getattr(spec, 'mode', 'mutual')
        self.single_mode(mode)
        contact = self.single_contact_evidence(user, timestamp)
        record = {'version': 2, 'scope': self.scope, 'mode': mode, 'contact_evidence': contact,
                  'state': 'claimed', 'utc_day': ctx['day'],
                  'request_key': spec.request_key, 'digest': request_digest,
                  'user_id': user.id, 'username': user.username, 'target_id': self.target_id, 'claimed_at': timestamp,
                  'invitation_requested': False, 'request_started_at': None}
        initial = {'source': 'isolated_contact_control', 'code': 'isolated_control_claimed',
                   'invitation_requested': False, 'request_outcome_unknown': True,
                   'contact_evidence': contact}
        with self.store.db:
            if (self.store.get(DAY_PREFIX + ctx['day']) is not None or
                self.store.get(REQUEST_PREFIX + spec.request_key) is not None):
                raise fail(409, 'Однократный допуск уже использован.')
            cur = self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) '
                'VALUES(?,?,?,?,?,?,?,?,?,?)', ('isolated:' + spec.request_key, request_digest, ACCOUNT, SENDER, 'invite',
                self.control_title, json.dumps(job_spec, ensure_ascii=False), 'paused', timestamp, timestamp))
            record['job_id'] = cur.lastrowid
            cur = self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,state,random_ids,attempted_at,user_access_hash,diagnosis) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?)', (record['job_id'], self.target_id, group.title, group.username, user.id, user.username,
                'sending', '[]', timestamp, user.access_hash, json.dumps(initial, ensure_ascii=False)))
            record['item_id'] = cur.lastrowid
            for key in (DAY_PREFIX + ctx['day'], REQUEST_PREFIX + spec.request_key):
                self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)', (key, json.dumps(record, ensure_ascii=False)))
        ctx.update(job_id=record['job_id'], item_id=record['item_id'], record=record,
            single_binding={'mode': mode, 'contact_evidence': dict(contact)},
            job_binding={'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'digest': request_digest,
                         'request_key': 'isolated:' + spec.request_key, 'spec': json.dumps(job_spec, ensure_ascii=False)},
            item_binding={'chat_id': self.target_id, 'user_id': user.id, 'user_access_hash': user.access_hash})
        return record

    def finish(self, ctx, result, requested):
        record = ctx['record']
        diagnosis = result.setdefault('diagnosis', {})
        diagnosis.update(invitation_requested=requested, request_outcome_unknown=result['state'] == 'uncertain',
            contact_evidence=record['contact_evidence'],
            isolated_control={'scope': self.scope, 'request_key': record['request_key'], 'max_attempts': 1,
                'mode': record['mode'],
                'identity_checked_at': ctx.get('identity_checked_at'), 'member_before': False,
                'member_checked_at': ctx.get('member_checked_at'),
                'enabled_before': 0, 'enabled_after': self.ops.account_row(ACCOUNT)['enabled'],
                'primary_projection_before': ctx['projection'], 'primary_projection_after': self.projection(ctx['job_id']),
                'pause_revision_before': ctx['revision'],
                'pause_revision_after': self.store.get('outbound_pause_revision_primary', 0),
                'claimed_at': record['claimed_at'], 'request_started_at': record.get('request_started_at')})
        timestamp = now()
        with self.store.db:
            self.store.db.execute('UPDATE op_items SET state=?,error=?,diagnosis=?,finished_at=? WHERE id=? AND job=?',
                (result['state'], result.get('error'), json.dumps(diagnosis, ensure_ascii=False), timestamp, ctx['item_id'], ctx['job_id']))
            self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND state!='cancelled'", (timestamp, ctx['job_id']))
            if requested or result['state'] == 'already_member':
                self.store.db.execute('INSERT INTO op_invited(chat_id,user_id,state,job) VALUES(?,?,?,?) ON CONFLICT(chat_id,user_id) DO NOTHING',
                                     (self.target_id, record['user_id'], result['state'], ctx['job_id']))
            self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=? AND telegram_id=?',
                                  (time.time() + 60, ACCOUNT, SENDER))
            record.update(state='finished', finished_at=timestamp, result_state=result['state'], invitation_requested=requested)
            self.save_record(record)
        return self.public(record)

    async def attempt(self, spec: SingleContactAttempt):
        self.require_scope(spec.scope)
        mode = getattr(spec, 'mode', 'mutual')
        self.single_mode(mode)
        request = spec.model_dump()
        if mode == 'mutual':
            request.pop('mode', None)  # Preserve pre-mode R5 request digests.
        request_digest = digest(request)
        deadline = time.monotonic() + ATTEMPT_TIMEOUT
        async with self.locked(deadline):
            existing = self.store.get(REQUEST_PREFIX + spec.request_key)
            if existing is not None:
                if not isinstance(existing, dict) or existing.get('digest') != request_digest:
                    raise fail(409, 'Ключ уже использован для другого действия.')
                return self.public(existing)
            if self.store.get(DAY_PREFIX + utc_day()) is not None:
                raise fail(409, 'Сегодня одиночный контроль уже использован.')
            proof = self.candidate
            if (not proof or not secrets.compare_digest(proof['token'], spec.candidate_token) or
                proof['day'] != utc_day() or time.monotonic() >= proof['expires'] or
                proof.get('mode') != mode or proof.get('account') != ACCOUNT or
                proof.get('sender_id') != SENDER or proof.get('target_id') != self.target_id or proof.get('scope') != self.scope or
                proof.get('member_before') is not False):
                raise fail(409, 'Обновите предпросмотр: допуск отсутствует или истёк.')
            ctx = self.context(min(deadline, proof['expires']))
            if proof['client'] is not ctx['client'] or proof['revision'] != ctx['revision']:
                raise fail(409, 'Подключение или защитная пауза изменились после предпросмотра.')
            requested = False
            evidence, verification = None, []
            from operations import (invite_response_evidence, missing_invitee_diagnosis,
                                    invite_rpc_diagnosis, unconfirmed_invite, invite_preflight_failure)
            try:
                await self.identity(ctx)
                group = await self.group(ctx)
                users = await self.read(ctx, lambda: ctx['client'](functions.users.GetUsersRequest(
                    [types.InputUser(proof['user_id'], proof['access_hash'])])))
                if len(users) != 1 or not self.single_eligible(users[0], mode) or users[0].id != proof['user_id']:
                    raise fail(409, 'Свежий контакт выбранного режима не подтверждён; приглашение не запускалось.')
                user = users[0]
                if user.id in self.history():
                    raise fail(409, 'Получатель уже присутствует в истории; повтор отключён.')
                member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                if member['member'] is not False:
                    raise fail(409, 'Нужно подтверждённое отсутствие пользователя в группе.')
                ctx['member_checked_at'] = member['checked_at']
                await self.read(ctx, lambda: self.ops.rights(ctx['account'], group, 'invite'))
                synthetic = {'id': -1, 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
                item = {'chat_id': self.target_id, 'user_id': user.id, 'user_name': user.username}
                self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                record = self.claim(ctx, spec, group, user, request_digest)
                self.candidate = None
                self.guard(ctx)
                self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                # Persist possible dispatch BEFORE the sole write await. A lost response is unknown.
                record.update(invitation_requested=True, request_started_at=now())
                with self.store.db:
                    self.save_record(record)
                    self.store.db.execute('UPDATE op_items SET diagnosis=? WHERE id=?',
                        (json.dumps({'source': 'isolated_contact_control', 'code': 'isolated_control_dispatch_unknown',
                                     'invitation_requested': True, 'request_outcome_unknown': True,
                                     'request_started_at': record['request_started_at']}), ctx['item_id']))
                self.guard(ctx)
                requested = True
                response = await asyncio.wait_for(ctx['client'](functions.channels.InviteToChannelRequest(group, [user])),
                                                  self.remaining(ctx['deadline']))
                evidence = invite_response_evidence(response, target_id=self.target_id, user_id=user.id, sender_id=SENDER)
                self.guard(ctx)
                missing = next((m for m in getattr(response, 'missing_invitees', []) if m.user_id == user.id), None)
                if missing:
                    diagnosis = missing_invitee_diagnosis(missing)
                    diagnosis.update(response=evidence, verification=[])
                    result = {'state': 'restricted', 'error': diagnosis['reason'], 'diagnosis': diagnosis}
                else:
                    result = None
                    for attempt, delay in enumerate((0, 1, 2), 1):
                        if delay:
                            await self.read(ctx, lambda: asyncio.sleep(delay))
                        checked = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                        verification.append({'attempt': attempt, **checked})
                        if checked['member'] is True:
                            result = {'state': 'invited', 'diagnosis': {'source': 'membership_check',
                                'code': 'invite_member_verified', 'reason': 'Telegram подтвердил новое членство после запроса.',
                                'next_action': 'Результат сохранён в истории панели.', 'retry_after_change': False,
                                'response': evidence, 'verification': verification}}
                            break
                    if result is None:
                        result = unconfirmed_invite('После запроса членство не подтверждено; повтор отключён.', evidence, verification)
                        if evidence['add_user_events'] and all(v.get('member') is False for v in verification):
                            result['diagnosis']['code'] = 'invite_service_membership_conflict'
            except asyncio.CancelledError:
                if ctx['record']:
                    self.finish(ctx, unconfirmed_invite('Одиночная проверка прервана; результат требует проверки.', evidence, verification),
                                requested or bool(ctx['record'].get('invitation_requested')))
                raise
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                if not ctx['record']:
                    raise fail(429 if isinstance(exc, errors.FloodWaitError) else 409,
                               'Telegram остановил предпроверку (' + type(exc).__name__ + '); приглашение не запускалось.') from None
                result = (unconfirmed_invite('Telegram остановил контроль; членство не подтверждено.', evidence, verification)
                          if requested else {'state': 'blocked', 'error': 'Telegram остановил одиночный контроль.'})
                result.setdefault('diagnosis', {})['verification_error'] = invite_rpc_diagnosis(exc)
            except Exception as exc:
                if not ctx['record']:
                    if isinstance(exc, HTTPException):
                        raise
                    raise fail(504 if isinstance(exc, TimeoutError) else 502,
                               'Предпроверка не завершена (' + type(exc).__name__ + '); приглашение не запускалось.') from None
                result = (unconfirmed_invite('Результат одиночного контроля не подтверждён (' + type(exc).__name__ + ').', evidence, verification)
                          if requested or ctx['record'].get('invitation_requested') else invite_preflight_failure(exc))
                if isinstance(exc, errors.RPCError):
                    diagnosis = invite_rpc_diagnosis(exc)
                    if requested and evidence is None and not isinstance(exc, (errors.ServerError, errors.TimedOutError)):
                        result = {'state': 'restricted' if diagnosis.get('retry_after_change') else 'failed',
                                  'error': diagnosis['reason'], 'diagnosis': diagnosis}
                    else:
                        result.setdefault('diagnosis', {})['verification_error'] = diagnosis
            return self.finish(ctx, result, requested or bool(ctx['record'].get('invitation_requested')))
