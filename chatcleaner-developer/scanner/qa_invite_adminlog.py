"""One fixed QA130 admin-log read through an already connected runtime.

No account construction, auth, contacts, messages, invitations or state writes.
This endpoint reports historical metadata, never invitation eligibility/success.
"""
import asyncio
import copy
from collections.abc import Mapping
from datetime import datetime, timezone
import math
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from telethon import errors, functions, types, utils


QA_ACCOUNT = 'dc3a101405dae3282db74119f2500f22'
QA_SENDER = 710000016
QA_TARGET = -1000720000002
QA_RECIPIENT = 710000010
QA_SCOPE = 'qa130-murk'
QA_SINCE = datetime(2026, 10, 21, 7, 54, 50, tzinfo=timezone.utc)
RUSSKIE_ACCOUNT = 'primary'
RUSSKIE_SENDER = 710000015
RUSSKIE_TARGET = -1000720000000
RUSSKIE_JOB = 126
RUSSKIE_SCOPE = 'pilot126'
RUSSKIE_ITEM_COUNT = 20
READ_LIMIT = 50
READ_TIMEOUT = 30


def fail(status, text, wait=None):
    headers = {'Cache-Control': 'no-store'}
    if wait is not None:
        headers['Retry-After'] = str(wait)
    return HTTPException(status, text, headers=headers)


async def qa_admin_only(request: Request):
    # The loopback API trusts only the identity headers overwritten by nginx.
    if not request.headers.get('x-panel-user') or request.headers.get('x-panel-role') not in ('owner', 'admin'):
        raise fail(403, 'Для диагностики нужен доступ владельца или администратора панели.')


def participant_id(participant):
    if isinstance(participant, (types.ChannelParticipant, types.ChannelParticipantSelf,
                                types.ChannelParticipantCreator, types.ChannelParticipantAdmin)):
        value = participant.user_id
    elif isinstance(participant, (types.ChannelParticipantLeft, types.ChannelParticipantBanned)):
        peer = participant.peer
        value = peer.user_id if isinstance(peer, types.PeerUser) else None
    else:
        return None
    return value if type(value) is int and value > 0 else None


def recipient_event(event, *, recipient_ids=None, since=QA_SINCE):
    """Internal parser: whitelisted metadata for fixed/saved subjects only."""
    subjects = frozenset((QA_RECIPIENT,)) if recipient_ids is None else frozenset(recipient_ids)
    if not subjects or any(type(uid) is not int or uid <= 0 for uid in subjects):
        return None
    if not isinstance(since, datetime) or since.tzinfo is None:
        return None
    if not isinstance(event, types.ChannelAdminLogEvent):
        return None
    if type(event.id) is not int or event.id <= 0 or type(event.user_id) is not int or event.user_id <= 0:
        return None
    stamp = event.date
    if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp < since:
        return None
    action = event.action
    if isinstance(action, types.ChannelAdminLogEventActionParticipantInvite):
        subject = participant_id(action.participant)
    elif isinstance(action, (types.ChannelAdminLogEventActionParticipantJoin,
                             types.ChannelAdminLogEventActionParticipantJoinByInvite,
                             types.ChannelAdminLogEventActionParticipantJoinByRequest,
                             types.ChannelAdminLogEventActionParticipantLeave)):
        subject = event.user_id
    elif isinstance(action, types.ChannelAdminLogEventActionParticipantToggleBan):
        subject = participant_id(action.new_participant)
        if participant_id(action.prev_participant) != subject:
            return None
    else:
        return None
    if subject not in subjects:
        return None
    result = {'event_id': event.id, 'date': stamp.astimezone(timezone.utc).isoformat(),
              'actor_id': event.user_id, 'recipient_id': subject,
              'action_type': type(action).__name__}
    if isinstance(action, types.ChannelAdminLogEventActionParticipantToggleBan):
        new = action.new_participant
        result['participant_type_after'] = type(new).__name__
        result['left_after'] = (True if isinstance(new, types.ChannelParticipantLeft) else
                                bool(new.left or new.banned_rights.view_messages)
                                if isinstance(new, types.ChannelParticipantBanned) else False)
    return result


class QaInviteAdminLog:
    def __init__(self, ops):
        self.ops = ops
        self.lock = asyncio.Lock()
        self.next_read = 0
        self.router = APIRouter(dependencies=[Depends(qa_admin_only)])
        self.router.add_api_route('/qa/invite-adminlog', self.read, methods=['GET'])
        self.router.add_api_route('/qa/russkie-adminlog', self.read_russkie, methods=['GET'])

    @staticmethod
    def remaining(deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise fail(504, 'Бюджет времени диагностики исчерпан. Следующее чтение Telegram не запускалось.')
        return remaining

    def existing(self, *, check_cooldown=True, account_id=QA_ACCOUNT, sender_id=QA_SENDER):
        # Never call auth/readable_account/connect/ready or create a runtime.
        runtimes = getattr(self.ops, 'accounts', None)
        account = runtimes.get(account_id) if isinstance(runtimes, Mapping) else None
        if account is None:
            raise fail(409, 'Нужно существующее подключение тестового аккаунта. Вход автоматически не запускается.')
        row = self.ops.account_row(account_id)  # Existing implementation is a SELECT only.
        client = getattr(account, 'client', None)
        me = getattr(account, 'me', None)
        if (client is None or not client.is_connected() or not isinstance(me, types.User)
                or me.id != sender_id or me.bot or row.get('telegram_id') != sender_id):
            raise fail(409, 'Личность или подключение тестового аккаунта не подтверждены. Повторный вход не запускается.')
        if check_cooldown and row.get('not_before', 0) > time.time():
            wait = max(1, math.ceil(row['not_before'] - time.time()))
            raise fail(429, 'Сначала дождитесь завершения текущего ограничения аккаунта.', wait)
        return account

    def guard(self, account, client, identity, *, account_id=QA_ACCOUNT, sender_id=QA_SENDER, binding=None, deadline=None):
        if deadline is not None:
            self.remaining(deadline)
        current = self.existing(account_id=account_id, sender_id=sender_id)
        if deadline is not None:
            self.remaining(deadline)
        if current is not account or current.client is not client or current.me is not identity:
            raise fail(409, 'Подключение или личность изменились. Результат чтения отброшен.')
        if binding is not None and self.source126(deadline=deadline) != binding:
            raise fail(409, 'Привязка сохранённого задания126 изменилась. Результат чтения отброшен.')
        if deadline is not None:
            self.remaining(deadline)

    def source126(self, *, deadline=None):
        """Bounded SELECTs; check elapsed time after each, without a SQL wall bound."""
        if deadline is not None:
            self.remaining(deadline)
        jobs = self.ops.store.rows(
            'SELECT id,account,telegram_id,kind,state,created,updated,spec FROM op_jobs WHERE id=?',
            (RUSSKIE_JOB,))
        if deadline is not None:
            self.remaining(deadline)
        if len(jobs) != 1:
            raise fail(409, 'Сохранённое исходное задание126 не найдено. Журнал не читался.')
        job = jobs[0]
        if (job.get('id') != RUSSKIE_JOB or job.get('account') != RUSSKIE_ACCOUNT
                or job.get('telegram_id') != RUSSKIE_SENDER or job.get('kind') != 'invite'):
            raise fail(409, 'Отправитель или тип исходного задания126 изменились. Журнал не читался.')
        items = self.ops.store.rows(
            'SELECT id,job,chat_id,user_id,state,attempted_at,finished_at FROM op_items WHERE job=? ORDER BY id LIMIT 21',
            (RUSSKIE_JOB,))
        if deadline is not None:
            self.remaining(deadline)
        if (len(items) != RUSSKIE_ITEM_COUNT or any(
                type(row.get('id')) is not int or row['id'] <= 0 or row.get('job') != RUSSKIE_JOB
                or type(row.get('chat_id')) is not int or row['chat_id'] != RUSSKIE_TARGET
                or type(row.get('user_id')) is not int or row['user_id'] <= 0 for row in items)):
            raise fail(409, 'Группа или получатели исходного задания126 не подтверждены. Журнал не читался.')
        if len({row['user_id'] for row in items}) != RUSSKIE_ITEM_COUNT:
            raise fail(409, 'Исходный состав получателей задания126 изменился. Журнал не читался.')
        try:
            created = datetime.fromisoformat(job['created'])
            attempts = [datetime.fromisoformat(row['attempted_at']) for row in items if row.get('attempted_at') is not None]
            if not created.tzinfo or not attempts or any(not date.tzinfo or date < created for date in attempts):
                raise ValueError()
            since = min(attempts).astimezone(timezone.utc)
        except (KeyError, TypeError, ValueError):
            raise fail(409, 'Время начала сохранённого задания126 не подтверждено. Журнал не читался.') from None
        binding = {'job': copy.deepcopy(job), 'items': copy.deepcopy(items), 'since': since,
                   'subjects': frozenset(row['user_id'] for row in items)}
        if deadline is not None:
            self.remaining(deadline)
        return binding

    async def read(self, scope: Literal['qa130-murk'] = Query(...)):
        if scope != QA_SCOPE:
            raise fail(422, 'Диагностика доступна только для сохранённого контроля QA130.')
        return await self.read_context(account_id=QA_ACCOUNT, sender_id=QA_SENDER, target_id=QA_TARGET,
            recipient_ids=frozenset((QA_RECIPIENT,)), since=QA_SINCE, scope=QA_SCOPE)

    async def read_russkie(self, scope: Literal['pilot126'] = Query(...)):
        deadline = time.monotonic() + READ_TIMEOUT
        if scope != RUSSKIE_SCOPE:
            raise fail(422, 'Диагностика доступна только для сохранённого задания126.')
        try:
            binding = self.source126(deadline=deadline)
            self.remaining(deadline)
        except HTTPException:
            raise
        except Exception:
            raise fail(502, 'Не удалось подтвердить сохранённое задание126. Журнал не читался.') from None
        return await self.read_context(account_id=RUSSKIE_ACCOUNT, sender_id=RUSSKIE_SENDER,
            target_id=RUSSKIE_TARGET, recipient_ids=binding['subjects'], since=binding['since'],
            scope=RUSSKIE_SCOPE, binding=binding, deadline=deadline)

    async def read_context(self, *, account_id, sender_id, target_id, recipient_ids, since, scope, binding=None, deadline=None):
        if deadline is None:
            deadline = time.monotonic() + READ_TIMEOUT
        try:
            async with asyncio.timeout(self.remaining(deadline)):
                async with self.lock:
                    self.remaining(deadline)
                    wait = max(0, math.ceil(self.next_read - time.monotonic()))
                    if wait:
                        raise fail(429, 'Повторное диагностическое чтение пока недоступно.', wait)
                    account = self.existing(account_id=account_id, sender_id=sender_id)
                    self.remaining(deadline)
                    client, identity = account.client, account.me
                    def same_reader():
                        self.guard(account, client, identity, account_id=account_id, sender_id=sender_id,
                                   binding=binding, deadline=deadline)
                    same_reader()
                    self.next_read = time.monotonic() + 60
                    self.remaining(deadline)
                    fresh = await client.get_me()
                    same_reader()
                    if not isinstance(fresh, types.User) or fresh.id != sender_id or fresh.bot:
                        raise fail(409, 'Telegram не подтвердил исходный тестовый аккаунт. Журнал не читался.')
                    self.remaining(deadline)
                    entity = await client.get_entity(target_id)
                    same_reader()
                    if (not isinstance(entity, types.Channel) or utils.get_peer_id(entity) != target_id
                            or not entity.megagroup or entity.broadcast or entity.left
                            or getattr(entity, 'deactivated', False)):
                        raise fail(409, 'Telegram не подтвердил исходную техническую группу. Журнал не читался.')
                    if not (entity.creator or getattr(entity, 'admin_rights', None)):
                        raise fail(403, 'Не подтверждены права читать журнал технической группы.')
                    request = functions.channels.GetAdminLogRequest(
                        channel=entity, q='', max_id=0, min_id=0, limit=READ_LIMIT,
                        events_filter=types.ChannelAdminLogEventsFilter(
                            join=True, leave=True, invite=True, ban=True, unban=True,
                            kick=True, unkick=True),
                        admins=None)  # Recipient filtering is local; admins filters actors.
                    self.remaining(deadline)
                    result = await client(request)
                    same_reader()
                    if not isinstance(result, types.channels.AdminLogResults) or not isinstance(result.events, list):
                        raise fail(502, 'Telegram вернул неизвестный ответ журнала. Вывод о приглашении не сделан.')
                    raw = result.events[:READ_LIMIT]
                    events = [row for event in raw if (row := recipient_event(event, recipient_ids=recipient_ids, since=since)) is not None]
                    response = {
                        'scope': scope, 'account': account_id,
                        'sender_id': sender_id, 'target_id': target_id,
                        'checked_at': datetime.now(timezone.utc).isoformat(),
                        'since': since.isoformat(), 'limit': READ_LIMIT,
                        'scanned_event_count': len(raw), 'events': events,
                        'limited_window': True, 'read_only': True,
                        'invitation_requested': False, 'history_changed': False,
                        'eligibility_evaluated': False, 'membership_evaluated': False,
                        'notice': 'Это ограниченный исторический журнал выбранного сохранённого контроля. Отсутствие события не устанавливает причину и не доказывает текущее членство. Приглашения, настройки, очередь и лимиты не менялись.'}
                    if binding is None:
                        response.update(qa_job_id=130, recipient_id=QA_RECIPIENT)
                    else:
                        response.update(source_job_id=RUSSKIE_JOB, cohort_count=RUSSKIE_ITEM_COUNT,
                                        subject_count=len(recipient_ids),
                                        since_source='earliest_saved_attempt_at')
                    return JSONResponse(response,
                        headers={'Cache-Control': 'no-store'})
        except errors.FloodWaitError as exc:
            self.next_read = max(self.next_read, time.monotonic() + max(60, exc.seconds + 1))
            wait = max(1, math.ceil(self.next_read - time.monotonic()))
            raise fail(429, 'Telegram ограничил диагностическое чтение. Автоматического повтора нет.', wait) from None
        except (errors.PeerFloodError, errors.SessionRevokedError, errors.AuthKeyUnregisteredError,
                errors.UserDeactivatedError, errors.UserDeactivatedBanError):
            raise fail(409, 'Telegram отклонил диагностическое чтение или сессию. Новых действий и повторного входа не выполнялось.') from None
        except errors.ChatAdminRequiredError:
            raise fail(403, 'Telegram не разрешил чтение журнала этой группы.') from None
        except TimeoutError:
            raise fail(504, 'Чтение журнала не завершено за отведённое время. Автоматического повтора нет.') from None
        except HTTPException:
            raise
        except Exception:
            raise fail(502, 'Не удалось прочитать журнал. Техническая причина не установлена; повтор не выполнялся.') from None
