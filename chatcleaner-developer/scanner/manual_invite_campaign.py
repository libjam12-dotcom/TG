"""Explicit finite manual invitations on native operation history; no queue enablement.

No contact enumeration/import, account rotation, write retry, or startup resume.
Only this controller can dispatch a persisted manual campaign, one durable claim
per recipient, with fresh sender/rights/user/membership evidence before the RPC.
"""
import asyncio
import json
import re
import secrets
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from telethon import errors, functions, types, utils

from engine import now
from isolated_invite import IsolatedInvite, REVOKED, digest, fail, isolated_admin_only, utc_day
from operation_workbench import WorkbenchStopped

SCOPE = 'manual-invite-campaign-v1'
PREFIX = 'manual_invite_campaign_job_'
DRAFT_PREFIX = 'manual_invite_campaign_draft_'
CLAIM_PREFIX = 'manual_invite_campaign_claim_'
START_PREFIX = 'manual_invite_campaign_start_'
QUICK_DRAFT_PREFIX = 'manual_invite_campaign_quick_draft_'
RESOLVED_PREFIX = 'manual_invite_campaign_resolved_'
ON_DISPATCH = 'on_dispatch'
TOKEN_TTL = 300
READ_TIMEOUT = 3600
STEP_TIMEOUT = 80
ACTIVE = ('running', 'waiting')
ITEM_FIELDS = 'id,chat_id,chat_title,chat_username,user_id,user_name,user_access_hash,state'


class ManualPauseRequested(Exception):
    pass


class ResolvedDuplicate(Exception):
    pass


class Strict(BaseModel):
    model_config = {'extra': 'forbid'}


class ManualConfig(Strict):
    account: str = Field(min_length=1, max_length=100)
    telegram_id: int = Field(gt=0, strict=True)
    target_id: int = Field(lt=0, strict=True)
    target_username: str | None = Field(default=None, max_length=32)
    users: list[str] = Field(min_length=1, max_length=1000)
    consent_note: str = Field(min_length=8, max_length=500)
    consent_confirmed: bool
    max_attempts: int = Field(default=25, ge=1, le=1000, strict=True)
    account_day_attempt_cap: int = Field(default=25, ge=1, le=1000, strict=True)
    success_goal: int | None = Field(default=None, ge=1, le=1000, strict=True)
    interval_seconds: int = Field(default=60, ge=1, le=86400, strict=True)

    @field_validator('consent_confirmed', mode='before')
    @classmethod
    def consent(cls, value):
        if value is not True:
            raise ValueError('Подтвердите согласие выбранных людей.')
        return True

    @field_validator('consent_note')
    @classmethod
    def reason(cls, value):
        value = value.strip()
        if len(value) < 8:
            raise ValueError('Опишите основание согласия.')
        return value

    @field_validator('users')
    @classmethod
    def usernames(cls, values):
        result = [v.strip().lstrip('@').lower() for v in values]
        if any(not re.fullmatch(r'[a-z][a-z0-9_]{3,31}', v) for v in result):
            raise ValueError('Укажите только @username, по одному на строку.')
        if len(set(result)) != len(result):
            raise ValueError('Уберите повторяющиеся @username.')
        return result

    @field_validator('target_username')
    @classmethod
    def group_name(cls, value):
        value = value.strip().lstrip('@').lower() if value else None
        if value and not re.fullmatch(r'[a-z][a-z0-9_]{3,31}', value):
            raise ValueError('Неверное имя группы.')
        return value

    @model_validator(mode='after')
    def bounds(self):
        if self.success_goal and self.success_goal > min(self.max_attempts, len(self.users)):
            raise ValueError('Цель добавлений превышает список или число попыток.')
        return self


class DraftRequest(Strict):
    config: ManualConfig
    candidate_token: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')


class StartRequest(Strict):
    job_id: int = Field(gt=0, strict=True)
    candidate_token: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    confirmed: bool

    @field_validator('confirmed', mode='before')
    @classmethod
    def confirmation(cls, value):
        if value is not True:
            raise ValueError('Отдельно подтвердите запуск.')
        return True


class QuickDraftRequest(Strict):
    config: ManualConfig
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')


class QuickStartRequest(Strict):
    job_id: int = Field(gt=0, strict=True)
    config_digest: str = Field(pattern=r'^[a-f0-9]{64}$')
    request_key: str = Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    confirmed: bool

    @field_validator('confirmed', mode='before')
    @classmethod
    def confirmation(cls, value):
        if value is not True:
            raise ValueError('Отдельно подтвердите запуск.')
        return True


class PauseRequest(Strict):
    job_id: int = Field(gt=0, strict=True)


class ManualInviteCampaign(IsolatedInvite):
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.proofs = {}
        self.previews = {}
        self.preview_tasks = set()
        self.task = None
        self.active = None
        self.wake = asyncio.Event()
        self.router = APIRouter(prefix='/manual-invite-campaign', dependencies=[Depends(isolated_admin_only)])
        for path, fn, methods in [('/destinations', self.destinations, ['GET']),
            ('/preview', self.preview, ['POST']), ('/previews/{preview_id}', self.preview_status, ['GET']), ('/draft', self.draft, ['POST']),
            ('/jobs/{jid}/preview', self.preview_job, ['POST']), ('/start', self.start, ['POST']),
            ('/pause', self.pause, ['POST']), ('/status', self.status, ['GET']),
            ('/quick-draft', self.quick_draft, ['POST']), ('/quick-start', self.quick_start, ['POST']),
            ('/draft-requests/{request_key}', self.draft_request, ['GET'])]:
            self.router.add_api_route(path, fn, methods=methods)

    @staticmethod
    def belongs(job):
        marker = job.get('spec', {}).get('manual_invite_campaign')
        return isinstance(marker, dict) and marker.get('scope') == SCOPE

    def initialize(self):
        with self.store.db:
            for row in self.store.rows('SELECT key,value FROM settings WHERE key LIKE ?', (PREFIX + '%',)):
                record = json.loads(row['value'])
                if record.get('state') in ACTIVE:
                    record.update(state='interrupted', stop_reason='process_restart', stopped_at=now())
                    self.save(record)
                    self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?", (record['job_id'],))

    async def shutdown(self):
        for task in tuple(self.preview_tasks):
            task.cancel()
        if self.preview_tasks:
            await asyncio.gather(*self.preview_tasks, return_exceptions=True)
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def save(self, record):
        self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (PREFIX + str(record['job_id']), json.dumps(record, ensure_ascii=False)))

    def record(self, jid):
        record = self.store.get(PREFIX + str(jid))
        if not isinstance(record, dict) or record.get('scope') != SCOPE:
            raise fail(404, 'Ручная кампания не найдена.')
        return record

    def job(self, jid):
        rows = self.store.rows('SELECT * FROM op_jobs WHERE id=?', (jid,))
        if not rows:
            raise fail(404, 'Задание не найдено.')
        job = rows[0]
        job['spec'] = json.loads(job['spec'])
        if not self.belongs(job):
            raise fail(409, 'Это задание управляется в другой карточке.')
        marker = job['spec']['manual_invite_campaign']
        if (job['account'] != marker['config']['account'] or job['telegram_id'] != marker['config']['telegram_id'] or
            job['kind'] != 'invite' or digest(marker['config']) != marker['config_digest'] or
            self.cohort_hash(job['id'], marker) != marker['cohort_hash']):
            raise fail(409, 'Привязка или состав сохранённой кампании изменились.')
        return job

    def cohort_hash(self, jid, marker=None):
        items = self.items(jid)
        if marker and marker.get('resolution') == ON_DISPATCH:
            cfg = marker['config']
            if len(items) != len(cfg['users']):
                raise fail(409, 'Состав ручного списка изменился.')
            for item, approved_name in zip(items, cfg['users']):
                if (item['user_name'] != approved_name or item['chat_id'] != cfg['target_id'] or
                    item['chat_username'] != cfg['target_username']):
                    raise fail(409, 'Имя получателя или выбранная группа изменились.')
                seal = self.store.get(RESOLVED_PREFIX + str(item['id']))
                if item['user_id'] is None and item['user_access_hash'] is None:
                    if seal is not None or item['state'] in ('sending', 'invited', 'already_member', 'uncertain'):
                        raise fail(409, 'Привязка ещё не разрешённого получателя изменена.')
                else:
                    expected = self.resolved_binding(jid, item, cfg)
                    if not isinstance(seal, dict) or any(seal.get(k) != v for k, v in expected.items()):
                        raise fail(409, 'Постоянная привязка Telegram ID получателя изменена.')
            return digest([dict(username=i['user_name'], target_id=i['chat_id'], target_username=i['chat_username']) for i in items])
        if marker and marker.get('resolution') is not None:
            raise fail(409, 'Неизвестный режим разрешения получателей.')
        return digest([dict(user_id=i['user_id'], username=i['user_name'], access_hash=i['user_access_hash'],
            target_id=i['chat_id'], target_username=i['chat_username']) for i in items])

    @staticmethod
    def resolved_binding(jid, item, cfg):
        if type(item['user_id']) is not int or item['user_id'] <= 0 or type(item['user_access_hash']) is not int:
            raise fail(409, 'Сохранённый Telegram ID или peer получателя недействителен.')
        return dict(scope=SCOPE, job_id=jid, item_id=item['id'], user_id=item['user_id'],
            user_access_hash=item['user_access_hash'], username=item['user_name'], account=cfg['account'],
            telegram_id=cfg['telegram_id'], target_id=cfg['target_id'], target_username=cfg['target_username'], config_digest=digest(cfg))

    def items(self, jid):
        return self.store.rows('SELECT ' + ITEM_FIELDS + ' FROM op_items WHERE job=? ORDER BY id', (jid,))

    def attempts_today(self, uid):
        return self.store.rows('SELECT count(*) AS n FROM op_items i JOIN op_jobs j ON j.id=i.job '
            'WHERE j.telegram_id=? AND substr(i.attempted_at,1,10)=?', (uid, utc_day()))[0]['n']

    def projection_for(self, uid, jid=None):
        return digest({'jobs': self.store.rows('SELECT * FROM op_jobs WHERE telegram_id=? AND id!=? ORDER BY id', (uid, jid or -1)),
            'items': self.store.rows('SELECT i.* FROM op_items i JOIN op_jobs j ON j.id=i.job '
                'WHERE j.telegram_id=? AND j.id!=? ORDER BY i.id', (uid, jid or -1))})

    def historical(self, target, uid, jid=None):
        return bool(self.store.rows('SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job '
            "WHERE i.chat_id=? AND i.user_id=? AND i.job!=? AND (i.attempted_at IS NOT NULL OR i.state IN ('already_member','invited','uncertain','sending')) "
            'UNION SELECT 1 FROM op_invited WHERE chat_id=? AND user_id=? LIMIT 1', (target, uid, jid or -1, target, uid)))

    def competing(self, config, jid=None):
        if self.store.rows('SELECT 1 FROM op_jobs j LEFT JOIN op_items i ON i.job=j.id '
            "WHERE j.telegram_id=? AND j.id!=? AND (j.state IN ('queued','scheduled') OR i.state='sending') LIMIT 1",
            (config['telegram_id'], jid or -1)):
            return True
        if config['account'] == 'primary':
            for row in self.store.rows("SELECT value FROM settings WHERE key LIKE 'isolated_contact_batch_day_%' OR key LIKE 'isolated_invite_day_%' OR key LIKE 'isolated_invite_campaign_day_%'"):
                if json.loads(row['value']).get('state') in (*ACTIVE, 'claimed'):
                    return True
        for row in self.store.rows('SELECT value FROM settings WHERE key LIKE ?', (PREFIX + '%',)):
            other = json.loads(row['value'])
            if other.get('job_id') != jid and other.get('config', {}).get('telegram_id') == config['telegram_id'] and other.get('state') in ACTIVE:
                return True
        return False

    def context(self, config, deadline, jid=None):
        a = self.ops.readable_account(config['account'])
        row = self.ops.account_row(config['account'])
        if a.me.id != config['telegram_id'] or row['telegram_id'] != config['telegram_id']:
            raise fail(409, 'Личность выбранного отправителя изменилась.')
        if getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0:
            raise fail(409, 'У клиента должны быть отключены автоматические повторы RPC и ожидание FloodWait.')
        if self.competing(config, jid):
            raise fail(409, 'Сначала остановите другие исходящие задания этого отправителя.')
        ctx = dict(config=config, account=a, client=a.client, row=row, day=utc_day(), job_id=jid,
            deadline=deadline, revision=self.store.get('outbound_pause_revision_' + config['account'], 0),
            projection=self.projection_for(config['telegram_id'], jid), used=self.attempts_today(config['telegram_id']), claimed=0, runtime_error=a.error)
        self.guard(ctx)
        return ctx

    def guard(self, ctx, *, allow_cooldown=False):
        self.remaining(ctx['deadline'])
        cfg = ctx['config']
        a = self.ops.readable_account(cfg['account'], check_cooldown=not allow_cooldown)
        row = self.ops.account_row(cfg['account'])
        if (a is not ctx['account'] or a.client is not ctx['client'] or a.me.id != cfg['telegram_id'] or
            row['telegram_id'] != cfg['telegram_id'] or any(row[k] != ctx['row'][k] for k in ('enabled', 'error')) or
            a.error != ctx['runtime_error'] or utc_day() != ctx['day'] or
            self.store.get('outbound_pause_revision_' + cfg['account'], 0) != ctx['revision'] or
            getattr(a.client, '_request_retries', None) != 0 or getattr(a.client, 'flood_sleep_threshold', None) != 0 or
            self.projection_for(cfg['telegram_id'], ctx['job_id']) != ctx['projection'] or self.competing(cfg, ctx['job_id']) or
            self.attempts_today(cfg['telegram_id']) != ctx['used'] + ctx['claimed']):
            raise fail(409, 'Отправитель, защитная пауза, бюджет или другие задания изменились.')
        if ctx.get('record'):
            record = self.record(ctx['job_id'])
            job = self.job(ctx['job_id'])
            if (record != ctx['record'] or record['state'] not in ACTIVE or job['state'] != 'paused' or
                job['spec'] != ctx['job_spec'] or self.items(ctx['job_id']) != ctx['items'] or
                time.time() >= record['expires_at']):
                raise fail(409, 'Сохранённая кампания изменена, остановлена или истекла.')
            if row['not_before'] > record['next_at'] + .01:
                raise fail(409, 'Появилось новое ограничение ожидания Telegram.')
            for item in ctx['items']:
                if item['state'] == 'sending':
                    claim = self.store.get(CLAIM_PREFIX + str(item['id']))
                    if claim != ctx.get('claim'):
                        raise fail(409, 'Постоянный допуск получателя изменился.')

    async def read(self, ctx, call):
        self.guard(ctx)
        value = await asyncio.wait_for(call(), min(30, self.remaining(ctx['deadline'])))
        self.guard(ctx)
        return value

    def protect(self, ctx, exc):
        cfg = ctx['config']
        if self.ops.accounts.get(cfg['account']) is not ctx['account'] or ctx['account'].client is not ctx['client']:
            return
        if isinstance(exc, errors.FloodWaitError):
            with self.store.db:
                self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=? AND telegram_id=?',
                    (time.time() + exc.seconds + 1, cfg['account'], cfg['telegram_id']))
        elif isinstance(exc, (errors.PeerFloodError, *REVOKED)):
            self.ops.pause_outbound(cfg['account'], exc)

    async def identity(self, ctx):
        user = await self.read(ctx, ctx['client'].get_me)
        if not isinstance(user, types.User) or user.id != ctx['config']['telegram_id'] or user.bot or user.deleted:
            raise fail(409, 'Telegram не подтвердил выбранного отправителя.')

    async def group(self, ctx):
        cfg = ctx['config']
        group = await self.read(ctx, lambda: ctx['client'].get_entity(cfg['target_id']))
        if (not isinstance(group, types.Channel) or not group.megagroup or utils.get_peer_id(group) != cfg['target_id'] or
            (cfg['target_username'] or '').lower() != (getattr(group, 'username', None) or '').lower()):
            raise fail(409, 'Выбранная группа или её имя изменились. Загрузите группы заново.')
        await self.read(ctx, lambda: self.ops.rights(ctx['account'], group, 'invite'))
        return group

    @staticmethod
    def eligible(user, sender):
        return (isinstance(user, types.User) and not user.min and not user.bot and not user.deleted and
            user.id != sender and type(user.access_hash) is int)

    async def destinations(self, account: str, telegram_id: int):
        async with self.locked(time.monotonic() + 60):
            a = self.ops.readable_account(account)
            row = self.ops.account_row(account)
            if a.me.id != telegram_id or row['telegram_id'] != telegram_id:
                raise fail(409, 'Отправитель изменился.')
            client, groups = a.client, []
            revision = self.store.get('outbound_pause_revision_' + account, 0)
            try:
                async def collect():
                    async for dialog in client.iter_dialogs(limit=500):
                        entity = dialog.entity
                        if not isinstance(entity, types.Channel) or not entity.megagroup:
                            continue
                        rights = getattr(entity, 'admin_rights', None)
                        if entity.creator or rights and rights.invite_users:
                            groups.append(await self.ops.rights(a, entity, 'invite'))
                await asyncio.wait_for(collect(), 55)
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(dict(config={'account': account, 'telegram_id': telegram_id}, account=a, client=client), exc)
                raise fail(409, 'Telegram остановил чтение групп: ' + type(exc).__name__) from None
            if (self.ops.readable_account(account).client is not client or a.me.id != telegram_id or
                self.store.get('outbound_pause_revision_' + account, 0) != revision):
                raise fail(409, 'Отправитель изменился при чтении групп.')
            return dict(account=account, telegram_id=telegram_id, username=a.me.username,
                outbound_paused=not bool(row['enabled']), destinations=groups, invitation_requested=False)

    async def inspect(self, config, jid=None, progress=None):
        ctx = self.context(config, time.monotonic() + READ_TIMEOUT, jid)
        ctx['runtime_error'] = ctx['account'].error
        try:
            await self.identity(ctx)
            group = await self.group(ctx)
            selected = []
            if jid:
                job = self.job(jid)
                if job['spec']['manual_invite_campaign']['config'] != config:
                    raise fail(409, 'Настройки сохранённой кампании изменились.')
            existing = self.items(jid) if jid else None
            source = [i for i in existing if i['state'] == 'pending'] if jid else config['users']
            if progress is not None:
                progress['total'] = len(source)
            for candidate in source:
                user = await self.read(ctx, lambda c=candidate: ctx['client'].get_entity(
                    types.InputPeerUser(c['user_id'], c['user_access_hash']) if existing is not None else c))
                if (not self.eligible(user, config['telegram_id']) or
                    (existing is None and (user.username or '').lower() != candidate) or
                    (existing is not None and (user.id != candidate['user_id'] or user.username != candidate['user_name']))):
                    raise fail(409, 'Один из выбранных получателей изменился или недоступен.')
                if user.id in {i['user_id'] for i in selected}:
                    raise fail(409, 'Несколько имён указывают на одного человека.')
                item = dict(chat_id=config['target_id'], chat_username=config['target_username'], user_id=user.id, user_name=user.username)
                self.ops.workbench.guard(ctx['account'], dict(id=jid or -1, account=config['account'], telegram_id=config['telegram_id'], spec={}), item, group, user)
                member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                if member['member'] is None:
                    raise fail(409, 'Членство одного из получателей не подтверждено. Повторного запроса нет.')
                selected.append(dict(user_id=user.id, username=user.username, access_hash=user.access_hash,
                    member=member['member'], checked_at=member['checked_at'], history=self.historical(config['target_id'], user.id, jid),
                    sender_has_contact=user.contact is True, mutual_contact=user.mutual_contact is True))
                if progress is not None:
                    progress['checked'] = len(selected)
                    progress['selected'] = [{k: v for k, v in i.items() if k != 'access_hash'} for i in selected]
            if not source:
                raise fail(409, 'В кампании нет ожидающих получателей.')
            token = secrets.token_urlsafe(32)
            self.proofs = {k: v for k, v in self.proofs.items() if v['expires'] > time.monotonic()}
            # Bound short-lived server proofs; no persistent alternate queue.
            if len(self.proofs) >= 20:
                self.proofs.pop(next(iter(self.proofs)))
            ctx.update(token=token, expires=time.monotonic() + TOKEN_TTL, selected=selected,
                config_digest=digest(config), frozen_items=existing, target={'id': config['target_id'], 'title': group.title, 'username': group.username})
            self.proofs[token] = ctx
            return self.preview_public(ctx)
        except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
            self.protect(ctx, exc)
            raise fail(409, 'Telegram остановил предпроверку: ' + type(exc).__name__) from None

    def preview_public(self, proof):
        return dict(scope=SCOPE, job_id=proof['job_id'], config=proof['config'], config_digest=proof['config_digest'],
            account=proof['config']['account'], telegram_id=proof['config']['telegram_id'], username=proof['account'].me.username,
            target=proof['target'], candidate_token=proof['token'], token_ttl_seconds=TOKEN_TTL,
            selected=[{k: v for k, v in i.items() if k != 'access_hash'} for i in proof['selected']],
            account_attempts_today=proof['used'], remaining_attempts_today=max(0, proof['config']['account_day_attempt_cap'] - proof['used']),
            outbound_paused=not bool(proof['row']['enabled']), invitation_requested=False)

    def launch_preview(self, config, jid=None):
        if sum(not task.done() for task in self.preview_tasks) >= 2:
            raise fail(429, 'Предыдущая проверка ещё выполняется. Дождитесь результата.')
        preview_id = secrets.token_urlsafe(24)
        progress = dict(preview_id=preview_id, state='checking', checked=0, total=len(config['users']),
            config=config, account=config['account'], telegram_id=config['telegram_id'],
            target={'id': config['target_id'], 'username': config['target_username']},
            invitation_requested=False, created_at=time.time())
        self.previews = {key: value for key, value in self.previews.items() if time.time() - value['created_at'] < READ_TIMEOUT + TOKEN_TTL}
        while len(self.previews) >= 20:
            oldest = next((key for key, value in self.previews.items() if value['state'] != 'checking'), None)
            if oldest is None:
                raise fail(429, 'Слишком много активных проверок.')
            self.previews.pop(oldest)
        self.previews[preview_id] = progress
        async def perform():
            try:
                result = await self.inspect(config, jid, progress)
                progress.update(result, state='ready')
            except asyncio.CancelledError:
                progress.update(state='failed', error='Проверка прервана, приглашения не запускались.')
                raise
            except Exception as exc:
                progress.update(state='failed', error=exc.detail if isinstance(exc, HTTPException) else
                    'Проверка не завершена (' + type(exc).__name__ + '). Приглашения не запускались.')
        task = asyncio.create_task(perform())
        self.preview_tasks.add(task)
        task.add_done_callback(self.preview_tasks.discard)
        return dict(progress)

    async def preview(self, config: ManualConfig):
        # The POST schedules read-only work; polling GET never contacts Telegram.
        return self.launch_preview(config.model_dump())

    async def preview_status(self, preview_id: str):
        progress = self.previews.get(preview_id)
        if not progress:
            raise fail(404, 'Предпросмотр не найден. Выполните новую проверку.')
        return dict(progress)

    async def preview_job(self, jid: int):
        record = self.record(jid)
        if record.get('resolution') == ON_DISPATCH:
            raise fail(409, 'Этот ручной список проверяет людей по одному при отдельном запуске.')
        if record['state'] not in ('draft', 'paused', 'interrupted', 'stopped'):
            raise fail(409, 'Эта кампания не готова к отдельному запуску.')
        if self.task and not self.task.done():
            raise fail(409, 'Дождитесь остановки текущей кампании.')
        return self.launch_preview(record['config'], jid)

    def proof(self, token, config=None, jid=None):
        proof = self.proofs.get(token)
        if (not proof or proof['expires'] <= time.monotonic() or proof['day'] != utc_day() or
            proof['job_id'] != jid or config is not None and proof['config_digest'] != digest(config)):
            raise fail(409, 'Настройки или предпросмотр изменились. Выполните новую проверку.')
        proof['deadline'] = min(proof['expires'], time.monotonic() + 15)
        self.guard(proof)
        if jid and self.items(jid) != proof['frozen_items']:
            raise fail(409, 'Состав сохранённого задания изменился.')
        return proof

    async def draft(self, spec: DraftRequest):
        async with self.locked(time.monotonic() + 15):
            cfg = spec.config.model_dump()
            request_digest = digest(cfg)
            if self.store.get(QUICK_DRAFT_PREFIX + spec.request_key):
                raise fail(409, 'Ключ принадлежит быстрому ручному списку.')
            old = self.store.get(DRAFT_PREFIX + spec.request_key)
            if old:
                if old['digest'] != request_digest:
                    raise fail(409, 'Ключ черновика уже использован с другими настройками.')
                return self.public(self.record(old['job_id']))
            proof = self.proof(spec.candidate_token, cfg)
            timestamp = now()
            # Ordinary queue policy remains compatible; this marker is rejected
            # by every generic start/retry/execute path.
            from operations import JobSpec
            job_spec = JobSpec(request_key=spec.request_key, account=cfg['account'], kind='invite',
                title=('Ручное приглашение · ' + proof['target']['title'])[:100], destinations=[cfg['target_id']], users=[],
                consent_note=cfg['consent_note'], delay_seconds=60, daily_limit=20).model_dump(mode='json')
            job_spec['delay_seconds'] = cfg['interval_seconds']
            job_spec['manual_invite_campaign'] = dict(scope=SCOPE, config=cfg, config_digest=request_digest, cohort_hash=digest([dict(user_id=i['user_id'], username=i['username'], access_hash=i['access_hash'], target_id=cfg['target_id'], target_username=cfg['target_username']) for i in proof['selected']]))
            record = dict(scope=SCOPE, config=cfg, config_digest=request_digest, target=proof['target'], state='draft',
                attempts_claimed=0, next_at=0, expires_at=0, stop_reason=None, pause_requested=False, created_at=timestamp)
            with self.store.db:
                row = self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('manual-invite:' + spec.request_key, request_digest, cfg['account'], cfg['telegram_id'], 'invite', job_spec['title'], json.dumps(job_spec, ensure_ascii=False), 'draft', timestamp, timestamp))
                record['job_id'] = row.lastrowid
                for user in proof['selected']:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,user_access_hash,state,random_ids) VALUES(?,?,?,?,?,?,?,?,?)',
                        (record['job_id'], cfg['target_id'], proof['target']['title'], cfg['target_username'], user['user_id'], user['username'], user['access_hash'], 'pending', '[]'))
                self.save(record)
                self.store.db.execute('INSERT INTO settings VALUES(?,?)', (DRAFT_PREFIX + spec.request_key, json.dumps(dict(digest=request_digest, job_id=record['job_id']))))
            proof.update(job_id=record['job_id'], frozen_items=self.items(record['job_id']))
            return self.public(record)

    def public(self, record):
        items = self.store.rows('SELECT id,user_id,user_name,state,attempted_at,finished_at,diagnosis,error FROM op_items WHERE job=? ORDER BY id', (record['job_id'],))
        counts = dict(total=len(items), attempts=sum(bool(i['attempted_at']) for i in items), added=0)
        for item in items:
            item['diagnosis'] = json.loads(item['diagnosis'] or '{}')
            counts[item['state']] = counts.get(item['state'], 0) + 1
            d = item['diagnosis']
            if item['state'] == 'invited' and d.get('member_before', {}).get('member') is False and d.get('member_after', {}).get('member') is True and d.get('invitation_requested') is True:
                counts['added'] += 1
        row = self.ops.account_row(record['config']['account'])
        sender_error=self.sender_error(record['config'])
        return dict(**record, counts=counts, items=items, account=record['config']['account'], telegram_id=record['config']['telegram_id'],
            account_attempts_today=self.attempts_today(record['config']['telegram_id']), outbound_paused=not bool(row['enabled']), sender_error=sender_error,
            can_pause=record['state'] in ACTIVE, can_resume=not bool(sender_error) and record['state'] in ('paused', 'interrupted', 'stopped') and any(i['state'] == 'pending' for i in items),
            invitation_requested=bool(counts['attempts']))

    async def status(self, job_id: int):
        return self.public(self.record(job_id))

    def sender_error(self, config):
        row=self.ops.account_row(config['account'])
        account=self.ops.accounts.get(config['account'])
        return row.get('error') or getattr(account,'error',None) or None

    def require_unrestricted_outbound(self, config):
        if self.sender_error(config):
            raise fail(409, 'Исходящие остановлены сохранённым ограничением Telegram. Проверьте аккаунт и отдельно подтвердите восстановление исходящих; повторного приглашения нет.')

    async def start(self, spec: StartRequest):
        async with self.locked(time.monotonic() + 15):
            old = self.store.get(START_PREFIX + spec.request_key)
            start_digest = digest(spec.model_dump())
            if old:
                if old['digest'] != start_digest:
                    raise fail(409, 'Ключ запуска уже использован с другими параметрами.')
                return self.public(self.record(old['job_id']))
            record = self.record(spec.job_id)
            if record.get('resolution') == ON_DISPATCH:
                raise fail(409, 'Этот список запускается только через отдельный быстрый запуск.')
            if record['state'] not in ('draft', 'paused', 'interrupted', 'stopped') or self.task and not self.task.done():
                raise fail(409, 'Эту кампанию сейчас нельзя запустить.')
            self.require_unrestricted_outbound(record['config'])
            ctx = self.proof(spec.candidate_token, record['config'], spec.job_id)
            cfg = record['config']
            if record['attempts_claimed'] >= cfg['max_attempts'] or ctx['used'] >= cfg['account_day_attempt_cap']:
                raise fail(409, 'Лимит попыток кампании или отправителя за сутки исчерпан.')
            if cfg['success_goal'] and self.public(record)['counts']['added'] >= cfg['success_goal']:
                raise fail(409, 'Цель подтверждённых добавлений достигнута.')
            job = self.job(spec.job_id)
            expected_marker = job['spec']['manual_invite_campaign']
            if expected_marker['config_digest'] != digest(cfg) or expected_marker['config'] != cfg:
                raise fail(409, 'Настройки сохранённого задания изменились.')
            wall = time.time()
            record.update(state='running', stop_reason=None, pause_requested=False, utc_day=utc_day(), next_at=max(wall, self.ops.account_row(cfg['account'])['not_before']),
                expires_at=(int(wall) // 86400 + 1) * 86400, started_at=now())
            with self.store.db:
                self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=?", (now(), spec.job_id))
                self.save(record)
                self.store.db.execute('INSERT INTO settings VALUES(?,?)', (START_PREFIX + spec.request_key, json.dumps(dict(digest=start_digest, job_id=spec.job_id))))
            ctx.update(record=record, items=self.items(spec.job_id), job_spec=job['spec'], claimed=0)
            self.proofs.pop(spec.candidate_token, None)
            self.active = ctx
            self.wake.clear()
            self.task = asyncio.create_task(self.worker(ctx))
            return self.public(record)

    def quick_job(self, jid):
        record, job = self.record(jid), self.job(jid)
        marker = job['spec']['manual_invite_campaign']
        if (record.get('resolution') != ON_DISPATCH or marker.get('resolution') != ON_DISPATCH or
            record['config'] != marker['config'] or record['config_digest'] != marker['config_digest']):
            raise fail(409, 'Для этого задания требуется прежний полный предпросмотр.')
        return record, job

    async def draft_request(self, request_key: str):
        # Recovery of a lost POST response is cached-only, never Telegram reads.
        saved = self.store.get(QUICK_DRAFT_PREFIX + request_key)
        if not isinstance(saved, dict):
            raise fail(404, 'Быстрый черновик с этим ключом не найден.')
        record, _ = self.quick_job(saved['job_id'])
        if saved.get('digest') != record['config_digest']:
            raise fail(409, 'Привязка ключа черновика изменилась.')
        return self.public(record)

    async def quick_draft(self, spec: QuickDraftRequest):
        async with self.locked(time.monotonic() + 25):
            cfg = spec.config.model_dump()
            config_digest = digest(cfg)
            old = self.store.get(QUICK_DRAFT_PREFIX + spec.request_key)
            if old:
                if old.get('digest') != config_digest:
                    raise fail(409, 'Ключ черновика уже использован с другими настройками.')
                return await self.draft_request(spec.request_key)
            if self.store.get(DRAFT_PREFIX + spec.request_key):
                raise fail(409, 'Ключ принадлежит прежнему режиму полного предпросмотра.')
            ctx = self.context(cfg, time.monotonic() + 25)
            try:
                await self.identity(ctx)
                group = await self.group(ctx)
                self.guard(ctx)
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                raise fail(409, 'Telegram остановил проверку отправителя или группы: ' + type(exc).__name__) from None
            from operations import JobSpec
            target = dict(id=cfg['target_id'], title=group.title, username=group.username)
            job_spec = JobSpec(request_key=spec.request_key, account=cfg['account'], kind='invite',
                title=('Ручное приглашение · ' + group.title)[:100], destinations=[cfg['target_id']], users=[],
                consent_note=cfg['consent_note'], delay_seconds=60, daily_limit=20).model_dump(mode='json')
            job_spec['delay_seconds'] = cfg['interval_seconds']
            job_spec['manual_invite_campaign'] = dict(scope=SCOPE, resolution=ON_DISPATCH, config=cfg,
                config_digest=config_digest, cohort_hash=digest([dict(username=name, target_id=cfg['target_id'],
                    target_username=cfg['target_username']) for name in cfg['users']]))
            timestamp = now()
            record = dict(scope=SCOPE, resolution=ON_DISPATCH, config=cfg, config_digest=config_digest,
                target=target, state='draft', attempts_claimed=0, next_at=0, expires_at=0, stop_reason=None,
                pause_requested=False, created_at=timestamp)
            with self.store.db:
                row = self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('manual-quick:' + spec.request_key, config_digest, cfg['account'], cfg['telegram_id'], 'invite',
                     job_spec['title'], json.dumps(job_spec, ensure_ascii=False), 'draft', timestamp, timestamp))
                record['job_id'] = row.lastrowid
                for username in cfg['users']:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,user_access_hash,state,random_ids) VALUES(?,?,?,?,?,?,?,?,?)',
                        (record['job_id'], cfg['target_id'], group.title, cfg['target_username'], None, username, None, 'pending', '[]'))
                self.save(record)
                self.store.db.execute('INSERT INTO settings VALUES(?,?)', (QUICK_DRAFT_PREFIX + spec.request_key,
                    json.dumps(dict(digest=config_digest, job_id=record['job_id']))))
            return self.public(record)

    async def quick_start(self, spec: QuickStartRequest):
        async with self.locked(time.monotonic() + 25):
            record, job = self.quick_job(spec.job_id)
            if spec.config_digest != record['config_digest']:
                raise fail(409, 'Настройки быстрого запуска изменились.')
            start_digest = digest(spec.model_dump())
            old = self.store.get(START_PREFIX + spec.request_key)
            if old:
                if old.get('digest') != start_digest or old.get('job_id') != spec.job_id:
                    raise fail(409, 'Ключ запуска уже использован с другими параметрами.')
                return self.public(record)
            if record['state'] not in ('draft', 'paused', 'interrupted', 'stopped') or self.task and not self.task.done():
                raise fail(409, 'Эту кампанию сейчас нельзя запустить.')
            cfg = record['config']
            self.require_unrestricted_outbound(cfg)
            ctx = self.context(cfg, time.monotonic() + 25, spec.job_id)
            if record['attempts_claimed'] >= cfg['max_attempts'] or ctx['used'] >= cfg['account_day_attempt_cap']:
                raise fail(409, 'Лимит попыток кампании или отправителя за сутки исчерпан.')
            if cfg['success_goal'] and self.public(record)['counts']['added'] >= cfg['success_goal']:
                raise fail(409, 'Цель подтверждённых добавлений достигнута.')
            if not any(i['state'] == 'pending' for i in self.items(spec.job_id)):
                raise fail(409, 'В кампании нет ожидающих получателей.')
            try:
                await self.identity(ctx)
                await self.group(ctx)
                self.guard(ctx)
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                raise fail(409, 'Telegram остановил проверку отправителя или группы: ' + type(exc).__name__) from None
            current, current_job = self.quick_job(spec.job_id)
            if current != record or current_job != job:
                raise fail(409, 'Сохранённая кампания изменилась во время проверки.')
            wall = time.time()
            record.update(state='running', stop_reason=None, pause_requested=False, utc_day=utc_day(),
                next_at=max(wall, self.ops.account_row(cfg['account'])['not_before']),
                expires_at=(int(wall) // 86400 + 1) * 86400, started_at=now())
            with self.store.db:
                self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=?", (now(), spec.job_id))
                self.save(record)
                self.store.db.execute('INSERT INTO settings VALUES(?,?)', (START_PREFIX + spec.request_key,
                    json.dumps(dict(digest=start_digest, job_id=spec.job_id))))
            ctx.update(record=record, items=self.items(spec.job_id), job_spec=job['spec'], claimed=0)
            self.active = ctx
            self.wake.clear()
            self.task = asyncio.create_task(self.worker(ctx))
            return self.public(record)

    def seal_recipient(self, ctx, item, user):
        self.guard(ctx)
        if ctx['record'].get('pause_requested'):
            raise ManualPauseRequested()
        if item['user_id'] is not None or item['user_access_hash'] is not None:
            raise fail(409, 'Личность получателя уже привязана.')
        if any(other['id'] != item['id'] and other['user_id'] == user.id for other in ctx['items']):
            raise ResolvedDuplicate()
        resolved = dict(item, user_id=user.id, user_access_hash=user.access_hash)
        seal = self.resolved_binding(ctx['job_id'], resolved, ctx['config'])
        with self.store.db:
            self.store.db.execute('INSERT INTO settings VALUES(?,?)',
                (RESOLVED_PREFIX + str(item['id']), json.dumps(dict(seal, resolved_at=now()))))
            updated = self.store.db.execute('UPDATE op_items SET user_id=?,user_access_hash=? WHERE id=? AND job=? '
                "AND user_id IS NULL AND user_access_hash IS NULL AND state='pending'",
                (user.id, user.access_hash, item['id'], ctx['job_id']))
            if updated.rowcount != 1:
                raise fail(409, 'Постоянная привязка получателя уже использована.')
        item.update(user_id=user.id, user_access_hash=user.access_hash)
        self.guard(ctx)

    def stop(self, record, state, reason):
        record.update(state=state, stop_reason=reason, stopped_at=now())
        with self.store.db:
            self.save(record)
            self.store.db.execute("UPDATE op_jobs SET state=?,updated=? WHERE id=?", ('completed' if state == 'completed' else 'paused', now(), record['job_id']))
        self.wake.set()

    async def pause(self, spec: PauseRequest):
        # Persist intent immediately, even while an RPC holds the step lock.
        # Keep the active record object in sync so its current read/verification
        # can finish under the existing guard; never cancel an in-flight write.
        record = self.record(spec.job_id)
        if record['state'] in ACTIVE:
            if self.active and self.active['job_id'] == spec.job_id:
                record = self.active['record']
            record['pause_requested'] = True
            record['pause_requested_at'] = now()
            with self.store.db:
                self.save(record)
            self.wake.set()
        return self.public(record)

    def reject_queue_job(self, job):
        with self.store.db:
            self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?", (job['id'],))

    def control_job(self, job, spec):
        raise fail(409, 'Ручная кампания запускается и приостанавливается только в настройках приглашений.')

    def claim(self, ctx, item):
        self.guard(ctx)
        cfg, record = ctx['config'], ctx['record']
        if record.get('pause_requested'):
            raise ManualPauseRequested()
        if (item['state'] != 'pending' or record['attempts_claimed'] >= cfg['max_attempts'] or
            self.attempts_today(cfg['telegram_id']) >= cfg['account_day_attempt_cap']):
            raise fail(409, 'Лимит попыток исчерпан или получатель уже обработан.')
        claim = dict(scope=SCOPE, job_id=ctx['job_id'], item_id=item['id'], user_id=item['user_id'],
            account=cfg['account'], telegram_id=cfg['telegram_id'], target_id=cfg['target_id'], config_digest=digest(cfg), claimed_at=now())
        with self.store.db:
            self.store.db.execute('INSERT INTO settings VALUES(?,?)', (CLAIM_PREFIX + str(item['id']), json.dumps(claim)))
            result = self.store.db.execute("UPDATE op_items SET state='sending',attempted_at=?,diagnosis=? WHERE id=? AND state='pending'",
                (now(), json.dumps(dict(source=SCOPE, code='dispatch_unknown', invitation_requested=True, request_outcome_unknown=True)), item['id']))
            if result.rowcount != 1:
                raise fail(409, 'Допуск участника уже использован.')
            record['attempts_claimed'] += 1
            self.save(record)
        item['state'] = 'sending'
        ctx['claimed'] += 1
        ctx['claim'] = claim

    def finish(self, ctx, item, result, requested):
        cfg, record = ctx['config'], ctx['record']
        diagnosis = result.setdefault('diagnosis', {})
        diagnosis.update(source=SCOPE, invitation_requested=requested, request_outcome_unknown=result['state'] == 'uncertain',
            config_digest=digest(cfg), account=cfg['account'], telegram_id=cfg['telegram_id'], target_id=cfg['target_id'])
        with self.store.db:
            self.store.db.execute('UPDATE op_items SET state=?,error=?,diagnosis=?,finished_at=? WHERE id=? AND job=?',
                (result['state'], result.get('error'), json.dumps(diagnosis, ensure_ascii=False), now(), item['id'], ctx['job_id']))
            if requested or result['state'] == 'already_member':
                self.store.db.execute('INSERT INTO op_invited VALUES(?,?,?,?) ON CONFLICT(chat_id,user_id) DO NOTHING',
                    (cfg['target_id'], item['user_id'], result['state'], ctx['job_id']))
            if requested:
                self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=? AND telegram_id=?',
                    (time.time() + cfg['interval_seconds'], cfg['account'], cfg['telegram_id']))
            record['next_at'] = max(time.time(), self.ops.account_row(cfg['account'])['not_before'])
            self.save(record)
        item['state'] = result['state']

    async def step(self, ctx, item):
        from operations import invite_response_evidence, missing_invitee_diagnosis, invite_rpc_diagnosis, unconfirmed_invite, invite_preflight_failure
        requested, before, evidence, after = False, None, None, None
        cfg = ctx['config']
        try:
            await self.identity(ctx)
            group = await self.group(ctx)
            quick = ctx['record'].get('resolution') == ON_DISPATCH
            ref = item['user_name'] if quick and item['user_id'] is None else types.InputPeerUser(item['user_id'], item['user_access_hash'])
            user = await self.read(ctx, lambda: ctx['client'].get_entity(ref))
            if (not self.eligible(user, cfg['telegram_id']) or
                (quick and (user.username or '').lower() != item['user_name']) or
                (not quick and user.username != item['user_name']) or
                (item['user_id'] is not None and user.id != item['user_id'])):
                raise fail(409, 'Личность или имя получателя изменились.')
            if quick and item['user_id'] is None:
                self.seal_recipient(ctx, item, user)
            if self.historical(cfg['target_id'], user.id, ctx['job_id']):
                return dict(state='duplicate_skipped', diagnosis={'code': 'target_history_exists'}), False, False
            before = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
            if before['member'] is True:
                return dict(state='already_member', diagnosis={'code': 'member_before_invite', 'member_before': before}), False, False
            if before['member'] is not False:
                raise fail(409, 'Отсутствие участника в группе не подтверждено.')
            job = dict(id=ctx['job_id'], account=cfg['account'], telegram_id=cfg['telegram_id'], kind='invite', spec={})
            self.ops.workbench.guard(ctx['account'], job, item, group, user)
            self.claim(ctx, item)
            self.guard(ctx)
            self.ops.workbench.guard(ctx['account'], job, item, group, user)
            requested = True
            response = await asyncio.wait_for(ctx['client'](functions.channels.InviteToChannelRequest(group, [user])), self.remaining(ctx['deadline']))
            evidence = invite_response_evidence(response, target_id=cfg['target_id'], user_id=user.id, sender_id=cfg['telegram_id'])
            self.guard(ctx)
            missing = next((m for m in getattr(response, 'missing_invitees', []) if m.user_id == user.id), None)
            if missing:
                d = missing_invitee_diagnosis(missing)
                d.update(response=evidence, member_before=before)
                return dict(state='restricted', diagnosis=d), True, False
            verification = []
            for attempt, delay in enumerate((0, 1, 2), 1):
                if delay:
                    await self.read(ctx, lambda: asyncio.sleep(delay))
                after = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                verification.append(dict(attempt=attempt, **after))
                if after['member'] is True:
                    return dict(state='invited', diagnosis=dict(code='invite_member_verified', member_before=before, member_after=after,
                        response=evidence, verification=verification, retry_after_change=False)), True, False
            result = unconfirmed_invite('Членство после запроса не подтверждено. Кампания остановлена.', evidence, verification)
            result['diagnosis'].update(member_before=before, member_after=after)
            return result, True, True
        except ResolvedDuplicate:
            return dict(state='duplicate_skipped', diagnosis={'code': 'same_uid_in_manual_cohort'}), False, False
        except ManualPauseRequested:
            return None, False, True
        except asyncio.CancelledError:
            if item['state'] == 'sending':
                self.finish(ctx, item, unconfirmed_invite('Процесс прерван, повтор отключён.', evidence, []), True)
            raise
        except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
            self.protect(ctx, exc)
            result = unconfirmed_invite('Telegram остановил кампанию; автоматического повтора нет.', evidence, []) if item['state'] == 'sending' else dict(state='blocked', diagnosis={})
            result['diagnosis']['verification_error'] = invite_rpc_diagnosis(exc)
            return result, requested, True
        except Exception as exc:
            if item['state'] == 'sending':
                # Only known recipient privacy restrictions permit advancing.
                if isinstance(exc, (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError, errors.UserChannelsTooMuchError, errors.UserKickedError)):
                    d = invite_rpc_diagnosis(exc)
                    d.update(member_before=before)
                    return dict(state='restricted', error=d['reason'], diagnosis=d), requested, False
                result = unconfirmed_invite('Неизвестный результат: ' + type(exc).__name__ + '. Повтор отключён.', evidence, [])
                # Preserve safe RPC enums and method, never raw exception/request text.
                result['diagnosis']['verification_error'] = invite_rpc_diagnosis(exc)
                result['diagnosis']['member_before'] = before
                return result, requested, True
            if (ctx['record'].get('resolution') == ON_DISPATCH and
                isinstance(exc, (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError,
                    errors.UserChannelsTooMuchError, errors.UserKickedError))):
                d = invite_rpc_diagnosis(exc)
                d['member_before'] = before
                return dict(state='restricted', error=d['reason'], diagnosis=d), False, False
            if isinstance(exc, WorkbenchStopped):
                return exc.result, False, False
            return invite_preflight_failure(exc), False, True

    async def worker(self, ctx):
        record, cfg = ctx['record'], ctx['config']
        try:
            while True:
                ctx['deadline'] = time.monotonic() + STEP_TIMEOUT
                async with self.locked(ctx['deadline']):
                    if self.record(ctx['job_id'])['state'] not in ACTIVE:
                        return
                    self.guard(ctx, allow_cooldown=True)
                    if record.get('pause_requested'):
                        self.stop(record, 'paused', 'owner_pause')
                        return
                    counts = self.public(record)['counts']
                    reason = ('campaign_attempt_cap_reached' if record['attempts_claimed'] >= cfg['max_attempts'] else
                        'account_day_attempt_cap_reached' if self.attempts_today(cfg['telegram_id']) >= cfg['account_day_attempt_cap'] else
                        'verified_goal_reached' if cfg['success_goal'] and counts['added'] >= cfg['success_goal'] else None)
                    pending = next((i for i in ctx['items'] if i['state'] == 'pending'), None)
                    if reason or pending is None:
                        self.stop(record, 'paused' if reason == 'account_day_attempt_cap_reached' else 'completed', reason or 'all_selected_processed')
                        return
                    if time.time() >= record['next_at']:
                        result, requested, stop = await self.step(ctx, pending)
                        if result is None:
                            self.stop(record, 'paused', 'owner_pause')
                            return
                        self.finish(ctx, pending, result, requested)
                        if stop:
                            self.stop(record, 'stopped', result.get('diagnosis', {}).get('code', 'telegram_or_context_stop'))
                            return
                        continue
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), min(60, max(.01, record['next_at'] - time.time())))
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            self.stop(record, 'interrupted', 'worker_cancelled')
            raise
        except Exception as exc:
            self.stop(record, 'stopped', 'context_stop_' + type(exc).__name__)
        finally:
            self.active = None
