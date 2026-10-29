"""Immutable, finite publication windows; eligibility is rechecked before writing.

This is a gate for the existing operation queue, never another scheduler. Rules
and geography are reviewed by the owner before a snapshot is supplied. A snapshot
cannot override a saved deletion, an unknown write or a Telegram restriction.
"""
import json
import time
from datetime import datetime
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Annotated

from fastapi import HTTPException
from pydantic import BaseModel, Field, model_validator


Weekday = Annotated[int, Field(strict=True, ge=0, le=6)]


def aware(value):
    return value.tzinfo is not None and value.utcoffset() is not None


class DestinationPostingPolicy(BaseModel):
    model_config = {'extra': 'forbid'}
    chat_id: int = Field(strict=True, lt=0)
    max_per_day: int = Field(strict=True, ge=1, le=3)
    min_interval_seconds: int = Field(strict=True, ge=0)
    allowed_weekdays: list[Weekday] | None = Field(default=None, min_length=1, max_length=7)
    rule_url: str | None = Field(default=None, max_length=512)
    rule_sha256: str | None = Field(default=None, pattern=r'^[a-f0-9]{64}$')
    rule_checked_at: datetime | None = None

    @model_validator(mode='after')
    def evidence(self):
        if self.allowed_weekdays is not None and len(set(self.allowed_weekdays)) != len(self.allowed_weekdays):
            raise ValueError('Дни недели в ограничении площадки не должны повторяться.')
        if self.rule_url is not None:
            url = urlsplit(self.rule_url)
            if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password:
                raise ValueError('Источник правила должен быть публичной HTTP(S) ссылкой без пароля.')
        if self.rule_checked_at is not None and not aware(self.rule_checked_at):
            raise ValueError('Время проверки правила должно содержать часовой пояс.')
        return self


class PostingPolicy(BaseModel):
    model_config = {'extra': 'forbid'}
    window_expires_at: datetime
    campaign_expires_at: datetime
    timezone: str = Field(default='Asia/Dubai', min_length=1, max_length=80)
    destinations: list[DestinationPostingPolicy] = Field(min_length=1, max_length=30)

    @model_validator(mode='after')
    def finite_snapshot(self):
        if not aware(self.window_expires_at) or not aware(self.campaign_expires_at):
            raise ValueError('Конец окна и кампании должны содержать часовой пояс.')
        if self.window_expires_at > self.campaign_expires_at:
            raise ValueError('Окно отправки должно завершаться не позже кампании.')
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError('Укажите действительный часовой пояс площадок.') from None
        ids = [d.chat_id for d in self.destinations]
        if len(set(ids)) != len(ids):
            raise ValueError('Укажите каждую площадку в политике только один раз.')
        return self


REASONS = {
    'window_expired': 'Окно публикации завершилось. Устаревшее сообщение не отправлено.',
    'campaign_expired': 'Кампания завершилась. Новая публикация не отправлена.',
    'weekday_not_allowed': 'Правило площадки не допускает публикацию в этот день недели.',
    'daily_frequency_reached': 'Дневная частота публикаций в этой площадке уже достигнута.',
    'interval_not_elapsed': 'До следующей публикации в этой площадке ещё не прошёл заданный интервал.',
    'publication_removed': 'В этой площадке наблюдалось удаление прежней публикации. Следующие размещения остановлены.',
    'publication_missing': 'Прежняя публикация в этой площадке не найдена. Следующие размещения остановлены до проверки.',
    'previous_forbidden': 'Telegram ранее запретил публикацию этого аккаунта в этой площадке. Следующие размещения остановлены.',
    'previous_unknown': 'Есть непроверенный результат прежней публикации в этой площадке. Автоматического повтора нет.',
    'invalid_saved_policy': 'Сохранённое ограничение публикации не прошло проверку. Отправка остановлена.',
    'sender_changed': 'Личность аккаунта изменилась. Публикация остановлена.',
    'operation_stopped': 'Задание или исходящие аккаунта приостановлены. Публикация не отправлена.',
}
EXPIRED = {'window_expired', 'campaign_expired'}


def decision(code, **evidence):
    terminal = code in EXPIRED | {'weekday_not_allowed', 'daily_frequency_reached', 'interval_not_elapsed'}
    return dict(state='cancelled' if terminal else 'blocked',
                error=REASONS[code], diagnosis=dict(source='posting_policy', code=code,
                reason=REASONS[code], next_action='Проверьте журнал, правила и состояние площадки перед новым ручным решением.',
                retry_after_change=False, **evidence))


def saved_policy(job):
    value = job.get('spec', {}).get('posting_policy')
    return PostingPolicy.model_validate(value) if value is not None else None


def expiry_decision(job, at=None):
    """No network/SQL writes; also usable when account/job cooldown is still active."""
    try:
        policy = saved_policy(job)
    except (TypeError, ValueError):
        return decision('invalid_saved_policy')
    if policy is None:
        return None
    stamp = time.time() if at is None else at
    if stamp >= policy.campaign_expires_at.timestamp():
        return decision('campaign_expired')
    if stamp >= policy.window_expires_at.timestamp():
        return decision('window_expired')
    return None


def timestamp(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.timestamp() if aware(parsed) else None
    except (TypeError, ValueError, OverflowError):
        return None


def publication_decision(store, job, item, at=None):
    """Fail closed for this exact sender/chat across all saved post jobs.

    Public rule metadata is a review receipt, not an automatic grant of consent.
    Current item is excluded so its already-claimed sending state cannot block itself.
    """
    expired = expiry_decision(job, at)
    if expired:
        return expired
    try:
        policy = saved_policy(job)
    except (TypeError, ValueError):
        return decision('invalid_saved_policy')
    if policy is None:
        return None
    matches = [d for d in policy.destinations if d.chat_id == item['chat_id']]
    if len(matches) != 1:
        return decision('invalid_saved_policy')
    dest = matches[0]
    stamp = time.time() if at is None else at
    local = datetime.fromtimestamp(stamp, ZoneInfo(policy.timezone))
    if dest.allowed_weekdays is not None and local.weekday() not in dest.allowed_weekdays:
        return decision('weekday_not_allowed')
    identity = (job['account'], job['telegram_id'], item['chat_id'])
    events = store.rows("""SELECT id,kind,item_id FROM op_tracking_events
      WHERE account=? AND telegram_id=? AND chat_id=?
        AND kind IN ('deletion_observed','missing_on_check','partially_missing') ORDER BY id LIMIT 1""", identity)
    if events:
        code = 'publication_removed' if events[0]['kind'] == 'deletion_observed' else 'publication_missing'
        return decision(code, previous_item_id=events[0]['item_id'], evidence_event_id=events[0]['id'])
    publications = store.rows("""SELECT item_id,presence FROM op_tracking_publications
      WHERE account=? AND telegram_id=? AND chat_id=?
        AND presence IN ('deletion_observed','missing_on_check','partially_missing') ORDER BY item_id LIMIT 1""", identity)
    if publications:
        code = 'publication_removed' if publications[0]['presence'] == 'deletion_observed' else 'publication_missing'
        return decision(code, previous_item_id=publications[0]['item_id'])
    history = store.rows("""SELECT i.id,i.state,i.error,i.diagnosis,i.attempted_at,i.finished_at
      FROM op_items i JOIN op_jobs j ON j.id=i.job
      WHERE j.kind='post' AND j.account=? AND j.telegram_id=? AND i.chat_id=? AND i.id!=?
      ORDER BY i.id""", (*identity, item['id']))
    attempts = []
    forbidden = {'ForbiddenError', 'ChatWriteForbiddenError', 'UserBannedInChannelError', 'ChannelPrivateError', 'ChatAdminRequiredError'}
    legacy_forbidden = {'Telegram: ForbiddenError.', 'В эту площадку нельзя отправлять сообщения.',
                        'Аккаунт ограничен в этой площадке.', 'Площадка недоступна аккаунту.', 'Не хватает прав администратора.',
                        'В группе запрещена отправка сообщений.', 'Аккаунт не участник группы. Автоматического вступления нет.'}
    for old in history:
        try:
            diagnosis = json.loads(old['diagnosis'])
        except (TypeError, ValueError):
            diagnosis = {}
        if not isinstance(diagnosis, dict):diagnosis = {}
        rpc = diagnosis.get('rpc') if isinstance(diagnosis.get('rpc'), dict) else {}
        if diagnosis.get('code') in forbidden or diagnosis.get('http_status') == 403 or rpc.get('exception_type') in forbidden or rpc.get('code') == 403 or old['error'] in legacy_forbidden:
            return decision('previous_forbidden', previous_item_id=old['id'])
        if old['state'] in ('uncertain', 'sending'):
            return decision('previous_unknown', previous_item_id=old['id'])
        attempted = timestamp(old['attempted_at'])
        if attempted is not None:
            attempts.append(attempted)
    # Count attempted publications conservatively, never just successful sends.
    today = [t for t in attempts if datetime.fromtimestamp(t, ZoneInfo(policy.timezone)).date() == local.date()]
    if len(today) >= dest.max_per_day:
        return decision('daily_frequency_reached', count_today=len(today), max_per_day=dest.max_per_day)
    if attempts and stamp < max(attempts) + dest.min_interval_seconds:
        return decision('interval_not_elapsed', next_allowed_at=max(attempts) + dest.min_interval_seconds)
    return None


class PostingPolicyStopped(HTTPException):
    """A known pre-write stop; never classify it as an ambiguous Telegram write."""
    def __init__(self, result):
        self.result = result
        super().__init__(409, result['error'])
