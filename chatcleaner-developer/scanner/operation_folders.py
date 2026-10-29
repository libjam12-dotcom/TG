"""Account-bound folder inspection and explicitly selected group joins; no posting.

Telegram folder semantics: https://core.telegram.org/api/folders
One official folder RPC joins only the reviewed selected peers. Durable claims
precede that RPC; neither a repeated HTTP request nor restart retries a write.
"""
import asyncio
from collections import Counter
import hashlib
import json
import re
import time
from typing import Literal
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, StrictInt, model_validator
from telethon import errors, functions, types, utils

from engine import now
from source_folders import folder_slug

SAFETY = (errors.FloodWaitError, errors.PeerFloodError, errors.SessionRevokedError,
          errors.AuthKeyUnregisteredError, errors.UserDeactivatedError,
          errors.UserDeactivatedBanError)
ACTIVE_MEMBERS = (types.ChannelParticipant, types.ChannelParticipantSelf,
                  types.ChannelParticipantAdmin, types.ChannelParticipantCreator)
FORBIDDEN_ADS = tuple(re.compile(pattern, re.I) for pattern in (
    r'\b(?:реклама|рекламные\s+(?:посты|публикации|объявления))\s+(?:строго\s+)?(?:запрещен[аоы]?|не\s+допуска(?:ется|ются))\b',
    r'\bзапрещ(?:ена|ено|ены|ается|аются)\s+(?:(?:любая|вся|любые|коммерческая)\s+)?(?:реклама|рекламные\s+(?:посты|публикации|объявления))\b',
    r'\bno\s+(?:advertising|advertisements|ads|commercial\s+posts)\b',
    r'\b(?:advertising|advertisements|ads)\s+(?:is\s+|are\s+)?(?:prohibited|forbidden|not\s+allowed)\b',
))


class FolderPreview(BaseModel):
    model_config = {'extra': 'forbid'}
    url: str = Field(min_length=1, max_length=300)
    telegram_id: int = Field(gt=0)


class FolderJoin(BaseModel):
    model_config = {'extra': 'forbid'}
    preview_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    telegram_id: int = Field(gt=0)
    chat_ids: list[StrictInt] = Field(min_length=1, max_length=50)
    request_key: str = Field(min_length=16, max_length=80, pattern=r'^[A-Za-z0-9_-]+$')
    confirmed: Literal[True]

    @model_validator(mode='after')
    def selected_groups(self):
        if len(set(self.chat_ids)) != len(self.chat_ids) or any(cid >= -10**12 for cid in self.chat_ids):
            raise ValueError('Выберите разные группы из проверенной папки.')
        self.chat_ids.sort()
        return self


def advertisement_review(about, pinned_text):
    matches = [match.group(0)[:200] for text in (about, pinned_text)
               for pattern in FORBIDDEN_ADS for match in pattern.finditer(text)]
    return {'advertising_permission': 'forbidden' if matches else 'unknown',
            'prohibition_matches': list(dict.fromkeys(matches))[:10],
            'manual_review_required': True,
            'note': 'Размещение рекламы не разрешается автоматически. Проверьте правила и договорённость с администрацией.'}


class OperationFolders:
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.lock = asyncio.Lock()
        self.router = APIRouter(prefix='/accounts/{aid}/folder')
        for path, endpoint, methods in (
            ('/preview', self.preview, ['POST']),
            ('/previews/{preview_id}', self.preview_record, ['GET']),
            ('/previews/{preview_id}/groups/{chat_id}/audit', self.audit, ['GET']),
            ('/join', self.join, ['POST']),
            ('/joins/{request_key}', self.join_record, ['GET']),
        ):
            self.router.add_api_route(path, endpoint, methods=methods)

    def initialize(self):
        self.store.db.executescript('''
          CREATE TABLE IF NOT EXISTS op_folder_previews(
            id TEXT PRIMARY KEY,account TEXT NOT NULL,telegram_id INTEGER NOT NULL,
            slug TEXT NOT NULL,title TEXT NOT NULL,created_at TEXT NOT NULL,created_epoch REAL NOT NULL,peers TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS op_folder_preview_identity ON op_folder_previews(account,telegram_id,slug,created_at);
          CREATE TABLE IF NOT EXISTS op_folder_audits(
            preview_id TEXT NOT NULL,chat_id INTEGER NOT NULL,checked_at TEXT NOT NULL,evidence TEXT NOT NULL,
            PRIMARY KEY(preview_id,chat_id));
          CREATE TABLE IF NOT EXISTS op_folder_join_requests(
            request_key TEXT PRIMARY KEY,digest TEXT NOT NULL,account TEXT NOT NULL,telegram_id INTEGER NOT NULL,
            preview_id TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,finished_at TEXT,error_code TEXT);
          CREATE TABLE IF NOT EXISTS op_folder_join_items(
            request_key TEXT NOT NULL,chat_id INTEGER NOT NULL,state TEXT NOT NULL,
            requested INTEGER NOT NULL DEFAULT 0,member_before INTEGER,member_after INTEGER,error_code TEXT,checked_at TEXT,
            PRIMARY KEY(request_key,chat_id));
        ''')
        with self.store.db:
            self.store.db.execute("UPDATE op_folder_join_items SET state='unknown',error_code='ProcessInterrupted' WHERE request_key IN (SELECT request_key FROM op_folder_join_requests WHERE state='joining') AND state IN ('requested','pending')")
            self.store.db.execute("UPDATE op_folder_join_requests SET state='unknown',error_code='ProcessInterrupted',finished_at=? WHERE state='joining'", (now(),))

    def account(self, aid, tid, *, client=None, write=False, cooldown=True):
        account = self.ops.online(aid) if write else self.ops.readable_account(aid, check_cooldown=cooldown)
        if account.me.id != tid or (client is not None and account.client is not client):
            raise HTTPException(409, 'Аккаунт изменился. Проверьте папку для текущего отправителя.')
        return account

    def safety_stop(self, aid, exc):
        self.ops.pause_outbound(aid, exc)
        if isinstance(exc, errors.FloodWaitError):
            with self.store.db:
                self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?', (time.time() + max(0, exc.seconds) + 1, aid))

    async def read(self, aid, tid, client, request, *, cooldown=True):
        self.account(aid, tid, client=client, cooldown=cooldown)
        try:
            result = await asyncio.wait_for(client(request), 15)
        except SAFETY as exc:
            self.safety_stop(aid, exc)
            raise
        self.account(aid, tid, client=client, cooldown=cooldown)
        return result

    @staticmethod
    def unpack(result):
        if isinstance(result, types.chatlists.ChatlistInvite):
            title, peers, filter_id = result.title.text, result.peers, None
        elif isinstance(result, types.chatlists.ChatlistInviteAlready):
            title, peers, filter_id = 'Папка Telegram уже подключена', [*result.already_peers, *result.missing_peers], result.filter_id
        else:
            raise HTTPException(502, 'Telegram не подтвердил состав папки.')
        if len(peers) > 500:
            raise HTTPException(400, 'В папке более 500 площадок. Используйте меньшую папку.')
        chats = {utils.get_peer_id(chat): chat for chat in result.chats}
        rows = []
        seen = set()
        for peer in peers:
            if not isinstance(peer, (types.PeerChat, types.PeerChannel)):
                continue
            cid = utils.get_peer_id(peer)
            if cid in seen:
                continue
            seen.add(cid)
            chat = chats.get(cid)
            available = isinstance(chat, (types.Channel, types.Chat))
            channel = isinstance(chat, types.Channel)
            group = isinstance(chat, types.Chat) or channel and bool(chat.megagroup) and not chat.broadcast
            inactive = bool(getattr(chat, 'deactivated', False) or getattr(chat, 'migrated_to', None))
            blocked = bool(getattr(getattr(chat, 'banned_rights', None), 'view_messages', False))
            access_hash = getattr(chat, 'access_hash', None)
            supported = bool(channel and group and not inactive and not blocked and not chat.min and type(access_hash) is int and access_hash != 0)
            left = bool(getattr(chat, 'left', False)) if available else None
            rows.append({'id': cid, 'title': getattr(chat, 'title', None) or 'Недоступная площадка',
                         'username': getattr(chat, 'username', None), 'kind': 'group' if group else 'channel' if available else 'unknown',
                         'available': available, 'managed': bool(getattr(chat, 'creator', False) or getattr(chat, 'admin_rights', None)),
                         'left': left, 'membership': 'not_joined' if left is True else 'reported_member' if left is False else 'unknown',
                         'membership_verified': False, 'join_supported': supported, 'inactive': inactive,
                         'advertising_permission': 'unknown', '_access_hash': access_hash if supported else None})
        return str(title)[:200], rows, filter_id

    def stored_preview(self, aid, preview_id, tid=None, *, latest=False):
        rows = self.store.rows('SELECT * FROM op_folder_previews WHERE id=? AND account=?', (preview_id, aid))
        if not rows:
            raise HTTPException(404, 'Проверка папки не найдена.')
        row = rows[0]
        actual = self.ops.account_row(aid)['telegram_id']
        if row['telegram_id'] != actual or tid is not None and row['telegram_id'] != tid:
            raise HTTPException(409, 'Проверка папки относится к другому Telegram-аккаунту.')
        if latest:
            current = self.store.rows('SELECT id FROM op_folder_previews WHERE account=? AND telegram_id=? AND slug=? ORDER BY created_at DESC,id DESC LIMIT 1', (aid, actual, row['slug']))[0]
            if current['id'] != preview_id or time.time() - row['created_epoch'] > 3600:
                raise HTTPException(409, 'Обновите состав папки и выберите группы заново.')
        return {**row, 'peers': json.loads(row['peers'])}

    def public_preview(self, row):
        audits = {audit['chat_id']: json.loads(audit['evidence']) for audit in self.store.rows('SELECT * FROM op_folder_audits WHERE preview_id=?', (row['id'],))}
        return {'preview_id': row['id'], 'account': row['account'], 'telegram_id': row['telegram_id'],
                'slug': row['slug'], 'title': row['title'], 'created_at': row['created_at'],
                'sources': [{**{key: value for key, value in peer.items() if not key.startswith('_')},
                             **({'audit': audits[peer['id']]} if peer['id'] in audits else {})} for peer in row['peers']],
                'joined_by_preview': False, 'posts_sent': 0}

    async def preview(self, aid: str, spec: FolderPreview):
        slug = folder_slug(spec.url)
        async with self.lock:
            account = self.account(aid, spec.telegram_id)
            try:
                result = await self.read(aid, spec.telegram_id, account.client, functions.chatlists.CheckChatlistInviteRequest(slug))
                title, peers, _ = self.unpack(result)
            except SAFETY as exc:
                raise HTTPException(429 if isinstance(exc, errors.FloodWaitError) else 409, 'Telegram остановил проверку папки (' + type(exc).__name__ + ').') from None
            except (errors.RPCError, TimeoutError, OSError) as exc:
                raise HTTPException(409, 'Не удалось проверить папку (' + type(exc).__name__ + ').') from None
            if len(self.store.rows('SELECT id FROM op_folder_previews WHERE account=? AND telegram_id=?', (aid, spec.telegram_id))) >= 200:
                raise HTTPException(400, 'Достигнут предел 200 сохранённых проверок папок для аккаунта.')
            row = {'id': uuid.uuid4().hex, 'account': aid, 'telegram_id': spec.telegram_id,
                   'slug': slug, 'title': title, 'created_at': now(), 'created_epoch': time.time(), 'peers': peers}
            with self.store.db:
                self.store.db.execute('INSERT INTO op_folder_previews VALUES(?,?,?,?,?,?,?,?)',
                    (row['id'], aid, spec.telegram_id, slug, title, row['created_at'], row['created_epoch'], json.dumps(peers, ensure_ascii=False)))
            return self.public_preview(row)

    async def preview_record(self, aid: str, preview_id: str):
        return self.public_preview(self.stored_preview(aid, preview_id))

    async def audit(self, aid: str, preview_id: str, chat_id: int, telegram_id: int):
        row = self.stored_preview(aid, preview_id, telegram_id)
        peer = next((peer for peer in row['peers'] if peer['id'] == chat_id), None)
        if not peer or not peer['join_supported']:
            raise HTTPException(400, 'Выберите доступную группу из этой проверки папки.')
        account = self.account(aid, telegram_id)
        channel = types.InputChannel(utils.resolve_id(chat_id)[0], peer['_access_hash'])
        evidence = {'account': aid, 'telegram_id': telegram_id, 'preview_id': preview_id, 'chat_id': chat_id,
                    'checked_at': now(), 'about': '', 'pinned_message_id': None, 'pinned_text': '',
                    'rules_complete': False, 'error_code': None, 'membership_verified': False, 'member': None}
        try:
            async with asyncio.timeout(40):
                full = await self.read(aid, telegram_id, account.client, functions.channels.GetFullChannelRequest(channel))
                chats = [chat for chat in full.chats if isinstance(chat, types.Channel) and utils.get_peer_id(chat) == chat_id]
                if len(chats) != 1 or not chats[0].megagroup or chats[0].broadcast or full.full_chat.id != channel.channel_id:
                    raise HTTPException(409, 'Telegram вернул другую площадку.')
                evidence['about'] = str(full.full_chat.about or '')[:12000]
                pinned = getattr(full.full_chat, 'pinned_msg_id', None)
                if type(pinned) is int and pinned > 0:
                    evidence['pinned_message_id'] = pinned
                    messages = await self.read(aid, telegram_id, account.client, functions.channels.GetMessagesRequest(channel, [types.InputMessageID(pinned)]))
                    found = [message for message in messages.messages if isinstance(message, types.Message)
                             and message.id == pinned and utils.get_peer_id(message.peer_id) == chat_id]
                    if len(found) == 1:
                        evidence['pinned_text'] = str(found[0].message or '')[:12000]
                evidence['member'] = await self.member(aid, telegram_id, account.client, channel)
                evidence['membership_verified'] = evidence['member'] is not None
        except SAFETY as exc:
            evidence['error_code'] = type(exc).__name__
        except (errors.RPCError, TimeoutError, OSError) as exc:
            evidence['error_code'] = type(exc).__name__
        evidence.update(advertisement_review(evidence['about'], evidence['pinned_text']))
        with self.store.db:
            self.store.db.execute('INSERT INTO op_folder_audits VALUES(?,?,?,?) ON CONFLICT(preview_id,chat_id) DO UPDATE SET checked_at=excluded.checked_at,evidence=excluded.evidence',
                                  (preview_id, chat_id, evidence['checked_at'], json.dumps(evidence, ensure_ascii=False)))
        return evidence

    async def member(self, aid, tid, client, channel, *, cooldown=True):
        try:
            result = await self.read(aid, tid, client, functions.channels.GetParticipantRequest(channel, types.InputPeerSelf()), cooldown=cooldown)
        except errors.UserNotParticipantError:
            return False
        participant = result.participant
        if isinstance(participant, ACTIVE_MEMBERS) and participant.user_id == tid:
            return True
        if isinstance(participant, (types.ChannelParticipantLeft, types.ChannelParticipantBanned)):
            return False if isinstance(participant, types.ChannelParticipantLeft) or participant.left or participant.banned_rights.view_messages else None
        return None

    def receipt(self, aid, key):
        rows = self.store.rows('SELECT * FROM op_folder_join_requests WHERE request_key=? AND account=?', (key, aid))
        if not rows:
            raise HTTPException(404, 'Результат вступления не найден.')
        row = rows[0]
        if self.ops.account_row(aid)['telegram_id'] != row['telegram_id']:
            raise HTTPException(409, 'Результат относится к другой личности аккаунта.')
        items = self.store.rows('SELECT chat_id,state,requested,member_before,member_after,error_code,checked_at FROM op_folder_join_items WHERE request_key=? ORDER BY chat_id', (key,))
        for item in items:
            item['requested'] = bool(item['requested'])
            for field in ('member_before', 'member_after'):
                if item[field] is not None:
                    item[field] = bool(item[field])
        return {key: value for key, value in row.items() if key != 'digest'} | {'items': items, 'summary': dict(Counter(item['state'] for item in items)), 'posts_sent': 0, 'automatic_retry': False}

    async def join_record(self, aid: str, request_key: str):
        return self.receipt(aid, request_key)

    def finish_item(self, key, cid, state, *, member=None, error=None):
        with self.store.db:
            self.store.db.execute('UPDATE op_folder_join_items SET state=?,member_after=?,error_code=?,checked_at=? WHERE request_key=? AND chat_id=?', (state, member, error, now(), key, cid))

    async def join(self, aid: str, spec: FolderJoin):
        if aid == 'primary':
            raise HTTPException(403, 'Вступление через папку доступно только отдельному аккаунту рассылки. Основной аккаунт не изменяется.')
        digest = hashlib.sha256(json.dumps(spec.model_dump(), sort_keys=True).encode()).hexdigest()
        async with self.lock, self.ops.lock:
            existing = self.store.rows('SELECT digest,account,telegram_id FROM op_folder_join_requests WHERE request_key=?', (spec.request_key,))
            if existing:
                if (existing[0]['digest'], existing[0]['account'], existing[0]['telegram_id']) != (digest, aid, spec.telegram_id):
                    raise HTTPException(409, 'Ключ уже привязан к другому вступлению.')
                return self.receipt(aid, spec.request_key)
            row = self.stored_preview(aid, spec.preview_id, spec.telegram_id, latest=True)
            account = self.account(aid, spec.telegram_id, write=True)
            client = account.client
            if self.store.rows("SELECT 1 FROM op_jobs WHERE account=? AND state IN ('queued','scheduled') LIMIT 1", (aid,)) or self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1", (aid,)):
                raise HTTPException(409, 'Сначала приостановите задания выбранного аккаунта и дождитесь текущего действия.')
            reviewed = {peer['id']: peer for peer in row['peers']}
            if any(cid not in reviewed or not reviewed[cid]['join_supported'] for cid in spec.chat_ids):
                raise HTTPException(400, 'Вступление разрешено только в выбранные группы проверенной папки; каналы исключены.')
            forbidden = self.store.rows("SELECT chat_id FROM op_folder_audits WHERE preview_id=? AND json_extract(evidence,'$.advertising_permission')='forbidden'", (spec.preview_id,))
            if set(spec.chat_ids) & {item['chat_id'] for item in forbidden}:
                raise HTTPException(409, 'В правилах выбранной группы обнаружен запрет рекламы. Исключите её из рекламной кампании.')
            try:
                fresh = await self.read(aid, spec.telegram_id, client, functions.chatlists.CheckChatlistInviteRequest(row['slug']))
                _, fresh_peers, filter_id = self.unpack(fresh)
            except SAFETY as exc:
                raise HTTPException(429 if isinstance(exc, errors.FloodWaitError) else 409, 'Telegram остановил проверку перед вступлением (' + type(exc).__name__ + ').') from None
            except (errors.RPCError, TimeoutError, OSError) as exc:
                raise HTTPException(409, 'Состав папки не подтверждён (' + type(exc).__name__ + '). Вступление не запускалось.') from None
            selected = {peer['id']: peer for peer in fresh_peers if peer['id'] in spec.chat_ids}
            if len(selected) != len(spec.chat_ids) or any(not peer['join_supported'] for peer in selected.values()):
                raise HTTPException(409, 'Состав папки или доступ изменился. Проверьте её заново.')
            to_join = [peer for peer in selected.values() if peer['left'] is True]
            for peer in to_join:
                old = self.store.rows("SELECT 1 FROM op_folder_join_items i JOIN op_folder_join_requests r ON r.request_key=i.request_key WHERE r.account=? AND r.telegram_id=? AND i.chat_id=? AND i.requested=1 AND i.state IN ('requested','unknown','join_requested') LIMIT 1", (aid, spec.telegram_id, peer['id']))
                if old:
                    raise HTTPException(409, 'Для выбранной группы уже есть неподтверждённое вступление. Повтор запрещён; проверьте членство вручную.')
            self.account(aid, spec.telegram_id, client=client, write=True)
            with self.store.db:
                self.store.db.execute('INSERT INTO op_folder_join_requests VALUES(?,?,?,?,?,?,?,?,?)', (spec.request_key, digest, aid, spec.telegram_id, spec.preview_id, 'joining', now(), None, None))
                for cid, peer in selected.items():
                    requested = peer['left'] is True
                    self.store.db.execute('INSERT INTO op_folder_join_items(request_key,chat_id,state,requested,member_before) VALUES(?,?,?,?,?)', (spec.request_key, cid, 'requested' if requested else 'pending', requested, not requested))
            try:
                if to_join:
                    peers = [types.InputPeerChannel(utils.resolve_id(peer['id'])[0], peer['_access_hash']) for peer in to_join]
                    request = functions.chatlists.JoinChatlistInviteRequest(row['slug'], peers) if filter_id is None else functions.chatlists.JoinChatlistUpdatesRequest(types.InputChatlistDialogFilter(filter_id), peers)
                    self.account(aid, spec.telegram_id, client=client, write=True)
                    await asyncio.wait_for(client(request), 30)
                    with self.store.db:
                        self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?', (time.time() + 30, aid))
                    self.ops.destination_cache.pop(aid, None)
                async with asyncio.timeout(90):
                    for cid, peer in selected.items():
                        channel = types.InputChannel(utils.resolve_id(cid)[0], peer['_access_hash'])
                        try:
                            member = await self.member(aid, spec.telegram_id, client, channel, cooldown=False)
                            state = ('joined' if peer['left'] else 'already_member') if member is True else 'unknown'
                            self.finish_item(spec.request_key, cid, state, member=member)
                        except SAFETY:
                            raise
                        except (errors.RPCError, TimeoutError, OSError) as exc:
                            self.finish_item(spec.request_key, cid, 'unknown', error=type(exc).__name__)
                with self.store.db:
                    unknown = self.store.rows("SELECT 1 FROM op_folder_join_items WHERE request_key=? AND state='unknown' LIMIT 1", (spec.request_key,))
                    self.store.db.execute('UPDATE op_folder_join_requests SET state=?,finished_at=? WHERE request_key=?', ('partial' if unknown else 'completed', now(), spec.request_key))
            except BaseException as exc:
                if isinstance(exc, SAFETY):
                    self.safety_stop(aid, exc)
                with self.store.db:
                    self.store.db.execute("UPDATE op_folder_join_items SET state='unknown',error_code=? WHERE request_key=? AND state IN ('requested','pending')", (type(exc).__name__, spec.request_key))
                    self.store.db.execute("UPDATE op_folder_join_requests SET state='unknown',error_code=?,finished_at=? WHERE request_key=?", (type(exc).__name__, now(), spec.request_key))
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    raise
            return self.receipt(aid, spec.request_key)
