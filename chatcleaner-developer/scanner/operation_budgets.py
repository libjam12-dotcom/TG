"""Explicit UTC-day internal caps for additional accounts; never reset history.

This changes only the panel's own cap. Telegram pauses, identity checks and the
current job's immutable cap remain authoritative. Saving never resumes a job.
"""
import json
from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator

from engine import now


class DailyBudgetSpec(BaseModel):
    model_config = {'extra': 'forbid'}
    confirmed: Literal[True]
    telegram_id: int = Field(gt=0, strict=True)
    utc_day: str = Field(pattern=r'^\d{4}-\d{2}-\d{2}$')
    daily_limit: int = Field(ge=1, le=100, strict=True)
    reason: str = Field(min_length=8, max_length=500)
    request_key: str = Field(min_length=16, max_length=80)

    @model_validator(mode='after')
    def valid_fields(self):
        self.reason = self.reason.strip()
        if len(self.reason) < 8:
            raise ValueError('Укажите причину изменения дневного предела, от 8 символов.')
        date.fromisoformat(self.utc_day)
        try:
            self.reason.encode('utf-8')
            self.request_key.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('Текст содержит неподдерживаемый символ.') from None
        return self


class OperationBudgets:
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.router = APIRouter()
        self.router.add_api_route('/accounts/{aid}/daily-budget', self.get_budget, methods=['GET'])
        self.router.add_api_route('/accounts/{aid}/daily-budget', self.save_budget, methods=['POST'])

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_daily_budgets(
          id INTEGER PRIMARY KEY AUTOINCREMENT,request_key TEXT UNIQUE NOT NULL,
          account TEXT NOT NULL,telegram_id INTEGER NOT NULL,utc_day TEXT NOT NULL,
          daily_limit INTEGER NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS op_daily_budgets_identity_day
          ON op_daily_budgets(account,telegram_id,utc_day,id);
        ''')

    @staticmethod
    def _receipt(row):
        return {**row, 'confirmed': True, 'scope': 'panel_internal_utc_day'}

    def _identity(self, aid, expected=None):
        identity = self.ops.account_row(aid)['telegram_id']
        if not isinstance(identity, int) or identity <= 0:
            raise HTTPException(409, 'Сначала подключите и проверьте личность Telegram аккаунта.')
        if expected is not None and identity != expected:
            raise HTTPException(409, 'Личность аккаунта изменилась. Обновите панель.')
        return identity

    def override(self, aid, identity, day=None):
        # No override can affect the primary, even if a database record is stale.
        if aid == 'primary':
            return None
        rows = self.store.rows('SELECT * FROM op_daily_budgets WHERE account=? AND telegram_id=? AND utc_day=? ORDER BY id DESC LIMIT 1',
                               (aid, identity, day or now()[:10]))
        return self._receipt(rows[0]) if rows else None

    def effective_limit(self, job, baseline):
        row = self.ops.account_row(job['account'])
        if row['telegram_id'] != job['telegram_id']:
            return baseline
        override = self.override(job['account'], job['telegram_id'])
        return min(job['spec']['daily_limit'], override['daily_limit']) if override else baseline

    def snapshot(self, aid):
        identity, day = self._identity(aid), now()[:10]
        # Same fallback as normal_daily_limit with a neutral candidate cap of 100.
        rows = self.store.rows("SELECT spec FROM op_jobs WHERE account=? AND state NOT IN ('draft','cancelled') AND (substr(created,1,10)=? OR state IN ('queued','scheduled','paused'))", (aid, day))
        baseline = min([100] + [json.loads(row['spec'])['daily_limit'] for row in rows])
        override = self.override(aid, identity, day)
        cap = override['daily_limit'] if override else baseline
        used = self.ops.account_daily_count(aid, day)
        through = (date.fromisoformat(day) + timedelta(days=7)).isoformat()
        grants = self.store.rows('''SELECT b.* FROM op_daily_budgets b
          WHERE account=? AND telegram_id=? AND utc_day>? AND utc_day<=?
            AND id=(SELECT MAX(x.id) FROM op_daily_budgets x WHERE x.account=b.account
              AND x.telegram_id=b.telegram_id AND x.utc_day=b.utc_day) ORDER BY utc_day''',
          (aid, identity, day, through)) if aid != 'primary' else []
        return dict(account=aid, telegram_id=identity, utc_day=day,
                    job_baseline_daily_limit=baseline, effective_daily_limit=cap,
                    account_count_today=used, remaining_today=max(0, cap-used),
                    override=override, editable=aid != 'primary',
                    scope='panel_internal_utc_day', checked_at=now(),
                    future_grants=[self._receipt(row) for row in grants], max_advance_days=7)

    async def get_budget(self, aid: str):
        async with self.ops.lock:
            return self.snapshot(aid)

    async def save_budget(self, aid: str, spec: DailyBudgetSpec):
        async with self.ops.lock:
            if aid == 'primary':
                raise HTTPException(403, 'Дневной предел основного аккаунта этим действием не изменяется.')
            self._identity(aid, spec.telegram_id)
            existing = self.store.rows('SELECT * FROM op_daily_budgets WHERE request_key=?', (spec.request_key,))
            values = dict(account=aid, **spec.model_dump(exclude={'confirmed'}))
            if existing:
                record = existing[0]
                if any(record[key] != value for key, value in values.items()):
                    raise HTTPException(409, 'Ключ подтверждения уже использован для другого изменения.')
            else:
                today = date.fromisoformat(now()[:10])
                if not today <= date.fromisoformat(spec.utc_day) <= today + timedelta(days=7):
                    raise HTTPException(409, 'Подтвердить дневной предел можно на сегодня и ближайшие семь дней UTC.')
                with self.store.db:
                    cur = self.store.db.execute('INSERT INTO op_daily_budgets(request_key,account,telegram_id,utc_day,daily_limit,reason,created_at) VALUES(?,?,?,?,?,?,?)',
                        (spec.request_key, aid, spec.telegram_id, spec.utc_day, spec.daily_limit, spec.reason, now()))
                record = self.store.rows('SELECT * FROM op_daily_budgets WHERE id=?', (cur.lastrowid,))[0]
            return dict(receipt=self._receipt(record), budget=self.snapshot(aid))
