"""One owner-requested diagnostic: primary -> one fixed group -> one owned account.

This scoped adapter reuses R5's single-write claim/guard/finish/protection path.
It never reads the address book, enables general outbound, or prepares contacts.
It deliberately expires after 28 October 2026 UTC and shares R5's daily claim.
"""
import secrets
import time
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from telethon import errors, types

from engine import now
from isolated_invite import (ACCOUNT, SENDER, DAY_PREFIX, TOKEN_TTL,
    PREVIEW_TIMEOUT, REVOKED, IsolatedAttempt, IsolatedInvite, fail,
    isolated_admin_only, utc_day)


SCOPE = 'client-dubai-one-owned-20261028-v1'
TARGET = -1000720000001
TARGET_USERNAME = 'demo_identity_5'
RECIPIENT = 710000016
RECIPIENT_USERNAME = 'demo_identity_2'
VALID_UTC_DAY = '2026-10-28'
MODE = 'owned_technical'


class OwnedTargetAttempt(IsolatedAttempt):
    scope: Literal[SCOPE]
    mode: Literal[MODE] = MODE


class OwnedTargetInvite(IsolatedInvite):
    scope = SCOPE
    target_id = TARGET
    control_title = 'Один технический инвайт в Дубай чатик'

    def __init__(self, ops):
        # Avoid the parent's legacy routes and recursive adapter registration.
        self.ops, self.store = ops, ops.store
        self.candidate = None
        self.next_preview = 0
        self.router = APIRouter(dependencies=[Depends(isolated_admin_only)])
        self.router.add_api_route('/owned-target-invite/preview', self.preview, methods=['GET'])
        self.router.add_api_route('/owned-target-invite/attempt', self.attempt, methods=['POST'])
        self.router.add_api_route('/owned-target-invite/status', self.status, methods=['GET'])

    def live_scope(self):
        if self.scope != SCOPE or self.target_id != TARGET or utc_day() != VALID_UTC_DAY:
            raise fail(409, 'Разовый контроль этой пары аккаунтов и группы закрыт или изменён.')

    def guard(self, ctx):
        self.live_scope()
        return super().guard(ctx)

    def history(self):
        rows = self.store.rows('SELECT user_id FROM op_items WHERE chat_id=? AND user_id IS NOT NULL '
            'UNION SELECT user_id FROM op_invited WHERE chat_id=?', (TARGET, TARGET))
        return frozenset((SENDER,)) | frozenset(row['user_id'] for row in rows)

    @staticmethod
    def single_mode(mode):
        if mode != MODE:
            raise fail(422, 'Этот контроль поддерживает только одного собственного технического участника.')

    def single_eligible(self, user, mode):
        self.single_mode(mode)
        # Telegram's absent contact flags are false; retain their real bool evidence.
        return (isinstance(user, types.User) and not user.min and not user.bot and not user.deleted and
            user.id == RECIPIENT and isinstance(user.username, str) and
            user.username.lower() == RECIPIENT_USERNAME and type(user.access_hash) is int and
            user.contact in (None, False) and user.mutual_contact in (None, False))

    async def group(self, ctx):
        group = await super().group(ctx)
        if str(getattr(group, 'username', '')).lower() != TARGET_USERNAME:
            raise fail(409, 'Публичное имя целевой группы изменилось; приглашение не запускалось.')
        return group

    async def preview(self, scope: Literal[SCOPE] = Query(...), mode: Literal[MODE] = MODE):
        self.require_scope(scope)
        self.single_mode(mode)
        self.live_scope()
        deadline = time.monotonic() + PREVIEW_TIMEOUT
        async with self.locked(deadline):
            if self.store.get(DAY_PREFIX + utc_day()) is not None:
                raise fail(409, 'Общий одиночный допуск R5 сегодня уже использован.')
            if time.monotonic() < self.next_preview:
                raise fail(429, 'Повторный предпросмотр доступен через минуту.')
            self.candidate = None
            self.next_preview = time.monotonic() + 60
            ctx = self.context(deadline)
            try:
                await self.identity(ctx)
                group = await self.group(ctx)
                user = await self.read(ctx, lambda: ctx['client'].get_entity(RECIPIENT_USERNAME))
                if not self.single_eligible(user, MODE):
                    raise fail(409, 'Собственный невзаимный технический аккаунт не подтверждён.')
                if user.id in self.history():
                    raise fail(409, 'Этот участник уже имеет историю в новой группе; повтора нет.')
                synthetic = {'id': -1, 'account': ACCOUNT, 'telegram_id': SENDER, 'kind': 'invite', 'spec': {}}
                item = {'chat_id': TARGET, 'user_id': RECIPIENT, 'user_name': user.username}
                self.ops.workbench.guard(ctx['account'], synthetic, item, group, user)
                member = await self.read(ctx, lambda: self.ops.read_membership(ctx['client'], group, user))
                if member.get('member') is not False:
                    raise fail(409, 'Нужно подтверждённое отсутствие технического участника в новой группе.')
                if user.id in self.history():
                    raise fail(409, 'История участника изменилась во время проверки; приглашение не запускалось.')
                wall = time.time()
                ttl = min(TOKEN_TTL, (int(wall) // 86400 + 1) * 86400 - wall)
                token = secrets.token_urlsafe(32)
                self.candidate = {'token': token, 'client': ctx['client'], 'revision': ctx['revision'],
                    'scope': SCOPE, 'mode': MODE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
                    'member_before': False, 'day': ctx['day'], 'expires': time.monotonic() + ttl,
                    'user_id': RECIPIENT, 'username': user.username, 'access_hash': user.access_hash,
                    'checked_at': member['checked_at']}
                return {'scope': SCOPE, 'mode': MODE, 'account': ACCOUNT, 'sender_id': SENDER, 'target_id': TARGET,
                    'candidate_token': token, 'token_ttl_seconds': round(ttl, 3),
                    'expires_at': datetime.fromtimestamp(wall + ttl, timezone.utc).isoformat(),
                    'selected': {'user_id': RECIPIENT, 'username': user.username, 'member': False,
                                 **self.single_contact_evidence(user, member['checked_at'])},
                    'membership_checks': 1, 'history_changed': False, 'invitation_requested': False,
                    'outbound_paused': True, 'checked_at': now()}
            except (errors.FloodWaitError, errors.PeerFloodError, *REVOKED) as exc:
                self.protect(ctx, exc)
                raise fail(429 if isinstance(exc, errors.FloodWaitError) else 409,
                    'Telegram остановил техническую предпроверку (' + type(exc).__name__ + '); повтора нет.') from None
            except HTTPException:
                raise
            except Exception as exc:
                raise fail(504 if isinstance(exc, TimeoutError) else 502,
                    'Техническая предпроверка не завершена (' + type(exc).__name__ + '); приглашение не запускалось.') from None

    async def attempt(self, spec: OwnedTargetAttempt):
        self.live_scope()
        return await super().attempt(spec)

    async def status(self, scope: Literal[SCOPE] = Query(...)):
        # Status remains readable after the date gate closes; it never sends.
        return await super().status(scope)
