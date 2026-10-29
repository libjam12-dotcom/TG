"""Explicit forum routes for the existing post queue; metadata reads only.

No topic creation, history loading, membership changes or General fallback.
https://core.telegram.org/api/forum#interacting-within-topics
"""
import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, ValidationError
from telethon import errors, functions, types, utils

from engine import now
from posting_policy import saved_policy


class DestinationTopic(BaseModel):
    model_config = {'extra': 'forbid'}
    chat_id: int = Field(strict=True, lt=0, gt=-(2**53))
    topic_id: int = Field(strict=True, ge=1, le=2**31-1)


def validate_topic_routes(routes, destinations, kind):
    if routes and kind != 'post':
        raise ValueError('Раздел форума доступен только для публикаций в группы.')
    ids = [r.chat_id for r in routes]
    if len(set(ids)) != len(ids) or not set(ids).issubset(destinations):
        raise ValueError('Выберите одну тему для каждой площадки из списка получателей.')


class OperationTopics:
    def __init__(self, ops):
        self.ops = ops
        self.router = APIRouter()
        self.router.add_api_route('/accounts/{aid}/destinations/{chat_id}/topics/{topic_id}',
                                  self.metadata, methods=['GET'])

    def identity(self, aid, client, tid):
        current = self.ops.readable_account(aid)
        if current.client is not client or current.me.id != tid:
            raise HTTPException(409, 'Отправитель изменился при проверке темы.')

    async def check(self, a, entity, chat_id, topic_id, *, media=False):
        route = DestinationTopic(chat_id=chat_id, topic_id=topic_id)
        client, tid = a.client, a.me.id
        self.identity(a.aid, client, tid)
        if (not isinstance(entity, types.Channel) or utils.get_peer_id(entity) != route.chat_id
                or not entity.megagroup or not entity.forum or entity.broadcast
                or entity.left or getattr(entity, 'deactivated', False)):
            raise HTTPException(400, 'Выбранная площадка не является доступным форумом.')
        try:
            result = await asyncio.wait_for(client(functions.channels.GetParticipantRequest(
                channel=entity, participant=types.InputPeerSelf())), 20)
        except errors.UserNotParticipantError:
            raise HTTPException(403, 'Аккаунт не участник форума. Автоматического вступления нет.') from None
        self.identity(a.aid, client, tid)
        participant = getattr(result, 'participant', None)
        known = (types.ChannelParticipant, types.ChannelParticipantSelf,
                 types.ChannelParticipantAdmin, types.ChannelParticipantCreator)
        if isinstance(participant, types.ChannelParticipantBanned):
            member_id = utils.get_peer_id(participant.peer)
            if participant.left:
                raise HTTPException(403, 'Аккаунт не участник форума.')
        elif isinstance(participant, known):
            member_id = participant.user_id
        else:
            raise HTTPException(409, 'Telegram не подтвердил членство отправителя в форуме.')
        if member_id != tid:
            raise HTTPException(409, 'Членство относится к другому отправителю.')
        bans = [getattr(entity, 'banned_rights', None), getattr(entity, 'default_banned_rights', None),
                getattr(participant, 'banned_rights', None)]
        denied = ('view_messages', 'send_messages', 'send_plain') + (('send_media', 'send_photos') if media else ())
        if any(getattr(rights, flag, False) for rights in bans if rights for flag in denied):
            raise HTTPException(403, 'В форуме запрещён выбранный вид публикации.')
        result = await asyncio.wait_for(client(functions.channels.GetForumTopicsByIDRequest(
            channel=entity, topics=[route.topic_id])), 20)
        self.identity(a.aid, client, tid)
        topics = getattr(result, 'topics', None)
        if (not isinstance(topics, list) or len(topics) != 1
                or not isinstance(topics[0], types.ForumTopic) or topics[0].id != route.topic_id):
            raise HTTPException(409, 'Тема удалена или её точный ID не подтверждён.')
        topic = topics[0]
        if topic.closed or topic.hidden or topic.short or not isinstance(topic.title, str) or not topic.title.strip():
            raise HTTPException(409, 'Тема закрыта, скрыта или недоступна для публикации.')
        return dict(account=a.aid, telegram_id=tid, chat_id=route.chat_id, chat_title=entity.title,
                    topic_id=route.topic_id, title=topic.title, forum=True, closed=False,
                    hidden=False, writable=True, checked_at=now())

    async def metadata(self, aid: str, chat_id: int, topic_id: int):
        try:
            DestinationTopic(chat_id=chat_id, topic_id=topic_id)
        except ValidationError:
            raise HTTPException(400, 'Укажите отрицательный ID группы и ID темы от 1.') from None
        a = self.ops.readable_account(aid)
        client, tid = a.client, a.me.id
        try:
            entity = await asyncio.wait_for(client.get_entity(chat_id), 20)
            self.identity(aid, client, tid)
            return await self.check(a, entity, chat_id, topic_id)
        except HTTPException:
            raise
        except errors.FloodWaitError:
            raise HTTPException(429, 'Telegram ограничил проверку темы. Повторите после ожидания.') from None
        except (errors.RPCError, TimeoutError):
            raise HTTPException(502, 'Не удалось подтвердить тему в Telegram. Отправка не запускалась.') from None

    @staticmethod
    def saved(job, item):
        """Parse immutable routes without RPC, including when the live forum changed."""
        spec = job['spec']
        try:
            routes = [DestinationTopic.model_validate(r) for r in spec.get('destination_topics', [])]
            validate_topic_routes(routes, spec.get('destinations', []), job['kind'])
            if not routes:
                if spec.get('topic_snapshots'):
                    raise ValueError('Unexpected topic snapshot')
                return None
            policy = saved_policy(job)
            if (policy is None or not spec.get('campaign_id') or not spec.get('campaign_revision')
                    or not isinstance(spec.get('campaign_snapshot'), dict)
                    or set(d.chat_id for d in policy.destinations) != set(spec.get('destinations', []))):
                raise ValueError('Topic routes require the saved campaign and CID posting policy')
            snapshots = spec.get('topic_snapshots')
            if not isinstance(snapshots, list) or len(snapshots) != len(routes):
                raise ValueError('Missing topic snapshot')
            actual = {}
            for row in snapshots:
                route = DestinationTopic.model_validate({k: row.get(k) for k in ('chat_id', 'topic_id')})
                if (route.chat_id in actual or row.get('account') != job['account']
                        or row.get('telegram_id') != job['telegram_id']
                        or row.get('forum') is not True or row.get('writable') is not True
                        or row.get('closed') is not False or row.get('hidden') is not False
                        or not isinstance(row.get('title'), str) or not row['title'].strip()):
                    raise ValueError('Invalid topic snapshot')
                at = datetime.fromisoformat(row['checked_at'])
                if at.tzinfo is None or at.utcoffset() is None:
                    raise ValueError('Unbound topic timestamp')
                actual[route.chat_id] = row
            if {cid: row['topic_id'] for cid, row in actual.items()} != {r.chat_id: r.topic_id for r in routes}:
                raise ValueError('Topic snapshot route mismatch')
            return actual.get(item['chat_id'])
        except (ValueError, TypeError, KeyError, AttributeError):
            raise HTTPException(409, 'Сохранённая тема или личность отправителя изменились. Отправка остановлена.') from None
