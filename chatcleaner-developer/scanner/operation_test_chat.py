"""Explicit, idempotent creation of an empty private test group for a panel owner."""
import asyncio
import hashlib
import json
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from telethon import errors, functions, types, utils

from engine import now


class TestChatSpec(BaseModel):
    model_config = {'extra': 'forbid'}
    request_key: str = Field(min_length=16, max_length=80)
    telegram_id: int = Field(gt=0)
    confirmed: Literal[True]


class OperationTestChat:
    def __init__(self, ops):
        self.ops = ops
        self.router = APIRouter(prefix='/tracking')
        self.router.add_api_route('/accounts/{aid}/test-chat', self.create, methods=['POST'])

    def initialize(self):
        for row in self.ops.store.rows("SELECT key,value FROM settings WHERE key LIKE 'test_chat_%'"):
            record = json.loads(row['value'])
            if isinstance(record, dict) and record.get('state') == 'creating':
                record.update(state='unknown', error_code='ProcessInterrupted', finished_at=now())
                self.ops.store.put(row['key'], record)

    async def create(self, aid: str, spec: TestChatSpec):
        # The persisted claim precedes the RPC: a lost response must never create
        # another group. This endpoint does not invite anyone or send messages.
        async with self.ops.lock:
            account = self.ops.online(aid)
            if account.me.id != spec.telegram_id:
                raise HTTPException(409, 'Аккаунт изменился. Проверьте отправителя.')
            key = 'test_chat_' + hashlib.sha256(spec.request_key.encode()).hexdigest()
            record = self.ops.store.get(key)
            if record:
                if record['account'] != aid or record['telegram_id'] != spec.telegram_id:
                    raise HTTPException(409, 'Ключ проверки уже привязан к другому аккаунту.')
                return record
            record = dict(account=aid, telegram_id=spec.telegram_id, state='creating',
                          requested_at=now(), title='Chatcleaner · тест маршрута UFC 333')
            self.ops.store.put(key, record)
            try:
                result = await asyncio.wait_for(account.client(functions.channels.CreateChannelRequest(
                    title=record['title'],
                    about='Закрытая техническая группа для проверки одной публикации и удаления.',
                    megagroup=True)), 30)
                chats = [c for c in result.chats if isinstance(c, types.Channel) and c.creator
                         and c.megagroup and not c.username and c.title == record['title']]
                if len(chats) != 1:
                    raise ValueError('UnexpectedCreateGroupResponse')
                chat = chats[0]
                record.update(state='created', chat_id=utils.get_peer_id(chat), finished_at=now())
                self.ops.destination_cache.pop(aid, None)
            except (errors.FloodWaitError, errors.PeerFloodError) as exc:
                self.ops.pause_outbound(aid, exc)
                record.update(state='unknown', error_code=type(exc).__name__, finished_at=now())
            except BaseException as exc:
                record.update(state='unknown', error_code=type(exc).__name__, finished_at=now())
                self.ops.store.put(key, record)
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    raise
            self.ops.store.put(key, record)
            return record
