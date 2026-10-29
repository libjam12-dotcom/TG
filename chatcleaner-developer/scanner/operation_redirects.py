"""Explicit, identity-bound canned redirects for *new* private enquiries.

This module never reads private history, initiates conversations, changes the
primary account, or resumes paused accounts. A persisted claim consumes a sender
slot even when its delivery is unknown; process recovery never replays a write.
"""
import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator
from telethon import errors, functions, types

from engine import now
from operation_tracking import TrackingReply, safe_tracking_error, timestamp


REVOKED = (errors.SessionRevokedError, errors.AuthKeyUnregisteredError,
           errors.UserDeactivatedError, errors.UserDeactivatedBanError)


class RedirectSettings(BaseModel):
    model_config = {'extra': 'forbid'}
    enabled: bool
    telegram_id: int = Field(gt=0)
    text: str = Field(min_length=1, max_length=1000)
    daily_limit: int = Field(default=20, ge=1, le=20)

    @model_validator(mode='after')
    def valid_text(self):
        self.text = self.text.strip()
        if not self.text:
            raise ValueError('Введите текст ответа.')
        try:
            self.text.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('Текст содержит неподдерживаемый символ.') from None
        return self


class BioUpdate(BaseModel):
    model_config = {'extra': 'forbid'}
    telegram_id: int = Field(gt=0)
    about: str = Field(max_length=70)
    request_key: str = Field(min_length=16, max_length=80)
    confirmed: Literal[True]

    @model_validator(mode='after')
    def valid_bio(self):
        try:
            length = len(self.about.encode('utf-16-le')) // 2
        except UnicodeEncodeError:
            raise ValueError('Описание содержит неподдерживаемый символ.') from None
        if length > 70:
            raise ValueError('Описание должно содержать не более 70 символов Telegram.')
        return self


class OperationRedirects:
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.lock = asyncio.Lock()
        self.profile_lock = asyncio.Lock()
        self.router = APIRouter()
        for path, fn, methods in [
            ('/accounts/{aid}/dm-redirect', self.settings, ['GET']),
            ('/accounts/{aid}/dm-redirect', self.set_settings, ['POST']),
            ('/accounts/{aid}/profile', self.profile, ['GET']),
            ('/accounts/{aid}/profile/bio', self.update_bio, ['POST']),
        ]:
            self.router.add_api_route(path, fn, methods=methods)

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_dm_redirect_settings(
          account TEXT PRIMARY KEY,telegram_id INTEGER NOT NULL,enabled INTEGER NOT NULL,
          text TEXT NOT NULL,daily_limit INTEGER NOT NULL,enabled_at TEXT,updated_at TEXT NOT NULL,
          pause_reason TEXT);
        CREATE TABLE IF NOT EXISTS op_dm_redirect_claims(
          id INTEGER PRIMARY KEY,event_id INTEGER UNIQUE NOT NULL,account TEXT NOT NULL,
          telegram_id INTEGER NOT NULL,sender_id INTEGER NOT NULL,request_key TEXT UNIQUE NOT NULL,
          state TEXT NOT NULL,error_code TEXT,created_at REAL NOT NULL,finished_at TEXT);
        CREATE INDEX IF NOT EXISTS op_dm_redirect_sender
          ON op_dm_redirect_claims(account,telegram_id,sender_id,created_at);
        CREATE TABLE IF NOT EXISTS op_profile_updates(
          id INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,
          account TEXT NOT NULL,telegram_id INTEGER NOT NULL,about TEXT NOT NULL,state TEXT NOT NULL,
          error_code TEXT,rpc_code INTEGER,created_at TEXT NOT NULL,finished_at TEXT);
        ''')
        # Unknown writes are never retried, even with the same request key.
        with self.store.db:
            self.store.db.execute("UPDATE op_dm_redirect_settings SET enabled=0,pause_reason='ProcessInterrupted',updated_at=? WHERE EXISTS (SELECT 1 FROM op_dm_redirect_claims c WHERE c.account=op_dm_redirect_settings.account AND c.telegram_id=op_dm_redirect_settings.telegram_id AND c.state='claimed')", (now(),))
            self.store.db.execute("UPDATE op_dm_redirect_claims SET state='unknown',error_code='ProcessInterrupted',finished_at=? WHERE state='claimed'", (now(),))
            self.store.db.execute("UPDATE op_profile_updates SET state='unknown',error_code='ProcessInterrupted',finished_at=? WHERE state='sending'", (now(),))
        self._mark_source()

    def _bound(self, aid, tid=None):
        row = self.ops.account_row(aid)
        if aid == 'primary':
            raise HTTPException(409, 'Этот раздел предназначен для дополнительного аккаунта кампании.')
        if tid is not None and row['telegram_id'] != tid:
            raise HTTPException(409, 'Личность аккаунта изменилась. Обновите панель.')
        return row

    async def settings(self, aid: str):
        account = self._bound(aid)
        rows = self.store.rows('SELECT * FROM op_dm_redirect_settings WHERE account=? AND telegram_id=?', (aid, account['telegram_id']))
        cfg = rows[0] if rows else dict(enabled=0, text='', daily_limit=20, enabled_at=None, updated_at=None, pause_reason=None)
        count = self.store.rows('SELECT COUNT(*) AS n FROM op_dm_redirect_claims WHERE account=? AND telegram_id=? AND created_at>=?', (aid, account['telegram_id'], time.time() - 86400))[0]['n']
        return dict(account=aid, telegram_id=account['telegram_id'], enabled=bool(cfg['enabled']),
                    text=cfg['text'], daily_limit=cfg['daily_limit'], enabled_at=cfg['enabled_at'],
                    updated_at=cfg['updated_at'], pause_reason=cfg['pause_reason'],
                    attempts_last_24h=count, per_sender_hours=24,
                    inbox_enabled=self.ops.tracking._inbox_enabled(aid, account['telegram_id']))

    async def set_settings(self, aid: str, spec: RedirectSettings):
        async with self.lock:
            self._bound(aid, spec.telegram_id)
            if spec.enabled:
                self.ops.online(aid)
                if not self.ops.tracking._inbox_enabled(aid, spec.telegram_id):
                    raise HTTPException(409, 'Сначала включите наблюдение новых личных сообщений этого аккаунта.')
            rows = self.store.rows('SELECT * FROM op_dm_redirect_settings WHERE account=?', (aid,))
            old = rows[0] if rows else None
            stamp = old['enabled_at'] if old and old['enabled'] and old['telegram_id'] == spec.telegram_id else now()
            with self.store.db:
                self.store.db.execute('''INSERT INTO op_dm_redirect_settings VALUES(?,?,?,?,?,?,?,NULL)
                  ON CONFLICT(account) DO UPDATE SET telegram_id=excluded.telegram_id,enabled=excluded.enabled,
                  text=excluded.text,daily_limit=excluded.daily_limit,enabled_at=excluded.enabled_at,
                  updated_at=excluded.updated_at,pause_reason=NULL''',
                  (aid, spec.telegram_id, int(spec.enabled), spec.text, spec.daily_limit, stamp if spec.enabled else None, now()))
        return await self.settings(aid)

    def _pause_redirect(self, aid, tid, reason):
        with self.store.db:
            self.store.db.execute('UPDATE op_dm_redirect_settings SET enabled=0,pause_reason=?,updated_at=? WHERE account=? AND telegram_id=?', (reason, now(), aid, tid))

    def _mark_source(self, key=None):
        where = ' AND r.request_key=?' if key else ''
        with self.store.db:
            self.store.db.execute("UPDATE op_tracking_events SET source='dm_redirect' WHERE EXISTS (SELECT 1 FROM op_tracking_replies r JOIN op_dm_redirect_claims c ON c.request_key=r.request_key WHERE op_tracking_events.event_key LIKE 'reply:'||r.id||':%'" + where + ')', (key,) if key else ())

    def _pause_account_if_needed(self, aid, exc):
        if isinstance(exc, (errors.FloodWaitError, errors.PeerFloodError, *REVOKED)):
            self.ops.pause_outbound(aid, exc)
            if isinstance(exc, errors.FloodWaitError):
                with self.store.db:
                    self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?', (time.time() + max(0, exc.seconds) + 1, aid))

    async def on_new_dm(self, aid, tid, client, event, saved):
        """Called only during live NewMessage ingestion, immediately after save.

        No queue/history worker calls this function. Both the event and its saved
        immutable identity must match; eligibility is checked again after awaits.
        """
        async with self.lock:
            if aid == 'primary' or not self.ops.tracking._identity(aid, tid, client):
                return
            rows = self.store.rows('SELECT * FROM op_dm_redirect_settings WHERE account=? AND telegram_id=? AND enabled=1', (aid, tid))
            if not rows or not self.ops.tracking._inbox_enabled(aid, tid):
                return
            cfg = rows[0]
            msg = getattr(event, 'message', None)
            chat, sender_id = getattr(event, 'chat_id', None), getattr(msg, 'sender_id', None)
            date = getattr(msg, 'date', None)
            if (not msg or not getattr(event, 'is_private', False) or getattr(msg, 'out', True)
                    or getattr(msg, 'action', None) or getattr(msg, 'via_bot_id', None)
                    or type(chat) is not int or chat <= 0 or chat != sender_id or chat in (tid, 777000)
                    or not isinstance(date, datetime)):
                return
            date = date if date.tzinfo else date.replace(tzinfo=timezone.utc)
            if date.timestamp() < timestamp(cfg['enabled_at']) or date.timestamp() > time.time() + 300:
                return
            # Do not permit a caller to attach the wrong journal row to an event.
            recorded = self.store.rows('SELECT * FROM op_tracking_events WHERE id=?', (saved['id'],))
            if not recorded:
                return
            saved = recorded[0]
            if (saved['kind'] != 'incoming_dm' or saved['job_id'] is not None
                    or saved['account'] != aid or saved['telegram_id'] != tid
                    or saved['chat_id'] != chat or saved['sender_id'] != sender_id
                    or saved['message_id'] != getattr(msg, 'id', None)):
                return
            sender = getattr(event, 'sender', None)
            if sender is None:
                try:
                    sender = await asyncio.wait_for(event.get_sender(), 10)
                except Exception:
                    return
            if (not isinstance(sender, types.User) or sender.id != sender_id
                    or sender.bot or sender.deleted or sender.support or getattr(sender, 'is_self', False)):
                return
            try:
                a = self.ops.online(aid)
            except HTTPException:
                return
            if a.client is not client or a.me.id != tid or not self.ops.tracking._inbox_enabled(aid, tid):
                return
            cutoff = time.time() - 86400
            if self.store.rows('SELECT 1 FROM op_dm_redirect_claims WHERE event_id=? OR (account=? AND telegram_id=? AND sender_id=? AND created_at>=?)', (saved['id'], aid, tid, sender_id, cutoff)):
                return
            count = self.store.rows('SELECT COUNT(*) AS n FROM op_dm_redirect_claims WHERE account=? AND telegram_id=? AND created_at>=?', (aid, tid, cutoff))[0]['n']
            if count >= cfg['daily_limit']:
                return
            # Manual answer claims take precedence. No auto retry of that event.
            if self.store.rows('SELECT 1 FROM op_tracking_replies WHERE event_id=?', (saved['id'],)):
                return
            key = 'dm-redirect:' + hashlib.sha256(f'{aid}:{tid}:{saved["id"]}'.encode()).hexdigest()
            with self.store.db:
                cur = self.store.db.execute('''INSERT INTO op_dm_redirect_claims
                  (event_id,account,telegram_id,sender_id,request_key,state,created_at)
                  VALUES(?,?,?,?,?,'claimed',?)''', (saved['id'], aid, tid, sender_id, key, time.time()))
            state, code = 'unknown', None
            try:
                receipt = await self.ops.tracking._send_reply(TrackingReply(
                    event_id=saved['id'], text=cfg['text'], request_key=key, confirmed=True), jid=None, aid=aid)
                state, code = receipt['state'], receipt['error_code']
            except asyncio.CancelledError:
                code = 'CancelledError'
                raise
            except HTTPException:
                state, code = 'skipped', 'ReplyGuardRejected'
            except Exception as exc:
                code, _ = safe_tracking_error(exc)
                self._pause_account_if_needed(aid, exc)
            finally:
                with self.store.db:
                    self.store.db.execute('UPDATE op_dm_redirect_claims SET state=?,error_code=?,finished_at=? WHERE id=?', (state, code, now(), cur.lastrowid))
                self._mark_source(key)
                if state == 'unknown' or code in ('FloodWaitError', 'PeerFloodError', *(e.__name__ for e in REVOKED)):
                    self._pause_redirect(aid, tid, code or 'DeliveryUnknown')

    async def _read_profile(self, aid, tid=None):
        row = self._bound(aid, tid)
        a = self.ops.readable_account(aid)
        client, expected = a.client, row['telegram_id']
        result = await asyncio.wait_for(client(functions.users.GetFullUserRequest(types.InputUserSelf())), 20)
        fresh = self.ops.readable_account(aid)
        full = getattr(result, 'full_user', None)
        if (fresh.client is not client or fresh.me.id != expected or getattr(full, 'id', None) != expected):
            raise HTTPException(409, 'Личность аккаунта изменилась при чтении профиля.')
        return dict(account=aid, telegram_id=expected, username=getattr(fresh.me, 'username', None),
                    first_name=getattr(fresh.me, 'first_name', None), last_name=getattr(fresh.me, 'last_name', None),
                    about=getattr(full, 'about', '') or '')

    async def profile(self, aid: str):
        return await self._read_profile(aid)

    @staticmethod
    def _bio_receipt(row):
        return {k: row[k] for k in ('request_key', 'account', 'telegram_id', 'about', 'state',
                                    'error_code', 'rpc_code', 'created_at', 'finished_at')}

    async def update_bio(self, aid: str, spec: BioUpdate):
        digest = hashlib.sha256(json.dumps([aid, spec.telegram_id, spec.about], ensure_ascii=False).encode()).hexdigest()
        async with self.profile_lock:
            self._bound(aid, spec.telegram_id)
            existing = self.store.rows('SELECT * FROM op_profile_updates WHERE request_key=?', (spec.request_key,))
            if existing:
                if existing[0]['digest'] != digest:
                    raise HTTPException(409, 'Ключ уже относится к другому изменению профиля.')
                return self._bio_receipt(existing[0])
            a = self.ops.online(aid)
            if a.me.id != spec.telegram_id:
                raise HTTPException(409, 'Личность аккаунта изменилась.')
            client = a.client
            with self.store.db:
                cur = self.store.db.execute('''INSERT INTO op_profile_updates
                  (request_key,digest,account,telegram_id,about,state,created_at)
                  VALUES(?,?,?,?,?,'sending',?)''', (spec.request_key, digest, aid, spec.telegram_id, spec.about, now()))
            state, code, rpc, write_returned = 'unknown', None, None, False
            try:
                response = await asyncio.wait_for(client(functions.account.UpdateProfileRequest(about=spec.about)), 30)
                write_returned = True
                if getattr(response, 'id', None) != spec.telegram_id:
                    code = 'IdentityReceiptMismatch'
                else:
                    actual = await self._read_profile(aid, spec.telegram_id)
                    state, code = ('confirmed', None) if actual['about'] == spec.about else ('unknown', 'BioNotVerified')
            except asyncio.CancelledError:
                code = 'CancelledError'
                raise
            except Exception as exc:
                code, rpc = safe_tracking_error(exc)
                self._pause_account_if_needed(aid, exc)
                state = 'failed' if not write_returned and isinstance(exc, errors.RPCError) and not isinstance(exc, (errors.ServerError, errors.TimedOutError)) else 'unknown'
            finally:
                with self.store.db:
                    self.store.db.execute('UPDATE op_profile_updates SET state=?,error_code=?,rpc_code=?,finished_at=? WHERE id=?', (state, code, rpc, now(), cur.lastrowid))
            return self._bio_receipt(self.store.rows('SELECT * FROM op_profile_updates WHERE id=?', (cur.lastrowid,))[0])
