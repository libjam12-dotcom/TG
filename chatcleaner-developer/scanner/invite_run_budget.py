"""Finite, opt-in panel budget for the already authorised jobs 11 and 13.

This module is NOT registered with Operations and performs no Telegram calls.
Integration must hold ``ops.lock`` and own the SQLite transaction for every
mutation. Call ``enforce_stops`` before selecting work (even if not_before is
tomorrow) and after saving a result. For a scoped job, never fall back to the
ordinary budget after a rejection/expiry: use ``for_job`` to find its receipt.
``claim`` belongs in the same transaction as pending -> sending/attempted_at;
``before_write`` belongs immediately before the external write, with no await
between its commit and that write. Existing identity, rights, membership,
deduplication, pilot/single gates and Telegram error handling remain mandatory.

Grant persistence never starts, resumes or enables anything. A new instance
cannot inherit a previous instance's client binding, including after restart.
"""
import hashlib
import json
import math
import time
from datetime import datetime, timezone

from fastapi import HTTPException


ACCOUNT = 'primary'
SENDER_ID = 710000015
TARGET = -1000720000000
JOB_IDS = (11, 13)
MAX_ITEMS = 57
GRANT_PREFIX = 'invite_run_grant_'
CLAIM_PREFIX = 'invite_run_claim_item_'


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


class InviteRunBudget:
    def __init__(self, ops, *, clock=time.time):
        self.ops, self.store, self.clock = ops, ops.store, clock
        self._clients = {}

    def initialize(self):
        """Check existing storage; there is no schema migration or commit."""
        self.store.db.execute('SELECT key,value FROM settings LIMIT 0')

    def _records(self, prefix):
        # LIKE would treat underscores in the prefix as wildcards.
        return [json.loads(row['value']) for row in self.store.rows(
            'SELECT value FROM settings WHERE substr(key,1,?)=? ORDER BY rowid',
            (len(prefix), prefix))]

    def _claim(self, item_id):
        return self.store.get(CLAIM_PREFIX + str(item_id))

    def _day(self):
        return datetime.fromtimestamp(self.clock(), timezone.utc).date().isoformat()

    def _count(self):
        return self.ops.account_daily_count(ACCOUNT, self._day())

    def _save(self, record):
        self.store.db.execute('UPDATE settings SET value=? WHERE key=?',
                              (json.dumps(record, ensure_ascii=False), GRANT_PREFIX + record['request_key']))

    def _record(self, key):
        record = self.store.get(GRANT_PREFIX + key)
        if record is None:
            raise HTTPException(404, 'Конечный допуск не найден.')
        return record

    @staticmethod
    def _job_binding(job):
        return {key: job[key] for key in ('id', 'account', 'telegram_id', 'kind', 'spec')}

    @staticmethod
    def _item_binding(item):
        return dict(item_id=item['id'], job_id=item['job'], user_id=item['user_id'],
                    binding=_hash({k: item.get(k) for k in
                                   ('id', 'job', 'chat_id', 'user_id', 'user_name', 'user_access_hash')}))

    def _cohort_matches(self, record, rows):
        selected = {i['item_id'] for i in record['envelope']['items']}
        return ([self._item_binding(i) for i in rows if i['id'] in selected] ==
                record['envelope']['items'])

    def _jobs(self):
        jobs = self.store.rows('SELECT * FROM op_jobs WHERE id IN (?,?) ORDER BY id', JOB_IDS)
        if len(jobs) != 2 or any(j['account'] != ACCOUNT or j['telegram_id'] != SENDER_ID or j['kind'] != 'invite' for j in jobs):
            raise HTTPException(409, 'Допуск относится только к сохранённым заданиям 11/13 клиентского аккаунта.')
        rows = self.store.rows('SELECT * FROM op_items WHERE job IN (?,?) ORDER BY id', JOB_IDS)
        if not rows or any(i['chat_id'] != TARGET for i in rows):
            raise HTTPException(409, 'Исходная группа задания изменилась.')
        return jobs, rows

    def preview(self):
        """Read-only snapshot; item identities are public, peer hashes stay private."""
        jobs, rows = self._jobs()
        pending = [i for i in rows if i['state'] == 'pending']
        if not 1 <= len(pending) <= MAX_ITEMS:
            raise HTTPException(409, 'Нужны от 1 до 57 ещё ожидающих получателей.')
        for item in pending:
            diagnosis = json.loads(item.get('diagnosis') or '{}')
            if item.get('attempted_at') is not None or diagnosis.get('invitation_requested') is True:
                raise HTTPException(409, 'Ожидающий получатель уже имел попытку; этот допуск не создаёт повтор.')
        items = [self._item_binding(i) for i in pending]
        if len({i['user_id'] for i in items}) != len(items) or any(type(i['user_id']) is not int or i['user_id'] <= 0 or i['user_id'] == SENDER_ID for i in items):
            raise HTTPException(409, 'Нужен точный список уникальных исходных получателей.')
        envelope = dict(jobs=[self._job_binding(j) for j in jobs], items=items)
        midnight = (int(self.clock()) // 86400 + 1) * 86400
        return dict(account=ACCOUNT, telegram_id=SENDER_ID, target=TARGET, job_ids=list(JOB_IDS),
                    utc_day=self._day(), cohort_hash=_hash(envelope),
                    items=[{k: i[k] for k in ('item_id', 'job_id', 'user_id')} for i in items],
                    remaining_claims=len(items), account_count_today=self._count(),
                    expires_at_max=min(self.clock() + 7200, midnight), _envelope=envelope)

    def _runtime(self, client, *, cooldown=True):
        try:
            row = self.ops.account_row(ACCOUNT)
        except HTTPException:
            return 'sender_changed'
        account = self.ops.accounts.get(ACCOUNT)
        if row['telegram_id'] != SENDER_ID or account is None or getattr(getattr(account, 'me', None), 'id', None) != SENDER_ID:
            return 'sender_changed'
        if not row['enabled'] or row.get('error') or getattr(account, 'error', None):
            return 'account_stopped'
        if account.client is not client or client is None or not client.is_connected():
            return 'client_changed'
        if cooldown and row['not_before'] > self.clock():
            return 'account_cooldown'
        return None

    def _gates(self):
        for jid in JOB_IDS:
            gate = self.store.get('single_attempt_job_' + str(jid))
            if gate is not None and (not isinstance(gate, dict) or gate.get('state') != 'released'):
                return 'single_gate_unreleased'
            pilot = self.store.get('invite_pilot_' + str(jid))
            if pilot is not None:
                if (not isinstance(pilot, dict) or pilot.get('goal_reached_at') or
                        type(pilot.get('expires_at')) not in (int, float) or not math.isfinite(pilot['expires_at'])):
                    return 'pilot_stopped'
                if pilot.get('utc_day') == self._day() and pilot.get('expires_at', 0) > self.clock():
                    return 'pilot_already_active'
        return None

    def grant(self, *, request_key, confirmed, reason, expires_at, items, cohort_hash, client):
        """Save once, under the caller's lock/transaction. Never resume a job."""
        if confirmed is not True or not isinstance(request_key, str) or not 16 <= len(request_key) <= 80 or not isinstance(reason, str) or not 8 <= len(reason.strip()) <= 500:
            raise HTTPException(400, 'Нужны отдельное подтверждение, новый ключ и причина конечного допуска.')
        request = dict(request_key=request_key, confirmed=True, reason=reason.strip(),
                       expires_at=expires_at, items=items, cohort_hash=cohort_hash)
        try:
            digest = _hash(request)
        except (TypeError, ValueError, UnicodeEncodeError):
            raise HTTPException(400, 'Некорректные параметры допуска.') from None
        old = self.store.get(GRANT_PREFIX + request_key)
        if old is not None:
            if old['digest'] != digest:
                raise HTTPException(409, 'Этот ключ уже использован с другими параметрами.')
            return self.status(request_key)  # A replay never binds a new client or rearms expiry.
        preview = self.preview()
        if (type(expires_at) not in (int, float) or not math.isfinite(expires_at) or
                not self.clock() < expires_at <= preview['expires_at_max']):
            raise HTTPException(409, 'Срок допуска: не дольше двух часов и не позже ближайшей полуночи UTC.')
        if items != preview['items'] or cohort_hash != preview['cohort_hash']:
            raise HTTPException(409, 'Список или исходные задания изменились. Обновите предпросмотр.')
        reason_code = self._runtime(client) or self._gates()
        if reason_code:
            raise HTTPException(409, 'Допуск не сохранён: ' + reason_code)
        if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1", (ACCOUNT,)):
            raise HTTPException(409, 'Дождитесь завершения текущего действия аккаунта.')
        if any(j['state'] not in ('draft', 'paused', 'queued', 'scheduled') for j in self._jobs()[0]):
            raise HTTPException(409, 'Исходное задание уже завершено.')
        for row in self._records(GRANT_PREFIX):
            if row['state'] == 'active':
                raise HTTPException(409, 'У этой выборки уже есть конечный допуск; повторная выдача отключена.')
        if any(self._claim(i['item_id']) is not None for i in items):
            raise HTTPException(409, 'Один из получателей уже использовал разовую попытку.')
        record = dict(request_key=request_key, digest=digest, state='active', account=ACCOUNT, telegram_id=SENDER_ID,
                      target=TARGET, job_ids=list(JOB_IDS), utc_day=preview['utc_day'],
                      created_at=self.clock(), expires_at=expires_at, reason=reason.strip(),
                      cohort_hash=cohort_hash, envelope=preview['_envelope'],
                      initial_count=preview['account_count_today'], max_claims=len(items),
                      account_daily_ceiling=preview['account_count_today'] + len(items))
        self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)',
                              (GRANT_PREFIX + request_key, json.dumps(record, ensure_ascii=False)))
        self._clients[request_key] = client
        return self.status(request_key)

    def status(self, key):
        record = self._record(key)
        used = sum(c['request_key'] == key for c in self._records(CLAIM_PREFIX))
        effective = record['state']
        if effective == 'active' and (self.clock() >= record['expires_at'] or self._day() != record['utc_day']):
            effective = 'expired'
        return {k: v for k, v in record.items() if k not in ('envelope', 'digest')} | dict(
            state=effective, claims_used=used, remaining_claims=max(0, record['max_claims'] - used),
            client_bound=key in self._clients)

    def for_job(self, jid):
        """Includes stopped/expired receipts: integration must not fall back silently."""
        if jid not in JOB_IDS:
            return None
        rows = self._records(GRANT_PREFIX)
        return rows[-1]['request_key'] if rows else None

    def _context(self, record, client):
        if record['state'] != 'active' or self.clock() >= record['expires_at'] or self._day() != record['utc_day']:
            raise HTTPException(409, 'Конечный допуск завершён или истёк.')
        if self._clients.get(record['request_key']) is not client:
            raise HTTPException(409, 'Соединение конечного допуска изменилось; автоматического восстановления нет.')
        reason = self._runtime(client) or self._gates()
        if reason:
            raise HTTPException(409, 'Действие остановлено: ' + reason)
        jobs, rows = self._jobs()
        if [self._job_binding(j) for j in jobs] != record['envelope']['jobs']:
            raise HTTPException(409, 'Сохранённые параметры задания изменились.')
        if not self._cohort_matches(record, rows):
            raise HTTPException(409, 'Исходная конечная выборка изменилась. Все её действия остановлены.')
        return jobs, rows

    def validate_continue(self, key, job_id, *, client):
        """Read-only guard for a separate, explicitly confirmed continuation.

        This does not clear any hold or change job/account state. The caller may
        only clear a positively identified internal daily-cap hold; Telegram
        errors, account cooldowns and unrelated schedules stay in force.
        """
        record = self._record(key)
        jobs, rows = self._context(record, client)
        if job_id not in JOB_IDS:
            raise HTTPException(409, 'Задание не входит в конечный допуск.')
        job = next(j for j in jobs if j['id'] == job_id)
        if job['state'] not in ('draft', 'paused', 'queued', 'scheduled'):
            raise HTTPException(409, 'Исходное задание уже завершено.')
        expected = {i['item_id']: i for i in record['envelope']['items']}
        pending = [i for i in rows if i['job'] == job_id and i['state'] == 'pending']
        if not pending or any(i['id'] not in expected or self._item_binding(i) != expected[i['id']]
                              or i.get('attempted_at') is not None or self._claim(i['id']) is not None
                              or json.loads(i.get('diagnosis') or '{}').get('invitation_requested') is True for i in pending):
            raise HTTPException(409, 'Есть получатели вне исходной ещё не использованной выборки.')
        count = self._count()
        if count < record['initial_count'] or count >= record['account_daily_ceiling'] or self.status(key)['remaining_claims'] == 0:
            raise HTTPException(409, 'Внутренний конечный бюджет недоступен.')
        return dict(request_key=key, job_id=job_id, item_ids=[i['id'] for i in pending],
                    account_daily_ceiling=record['account_daily_ceiling'], expires_at=record['expires_at'])

    def _check(self, record, client, *, item_id, writing):
        jobs, rows = self._context(record, client)
        expected = next((i for i in record['envelope']['items'] if i['item_id'] == item_id), None)
        current = next((i for i in rows if i['id'] == item_id), None)
        if not expected or not current or self._item_binding(current) != expected:
            raise HTTPException(409, 'Получатель не входит в исходную конечную выборку или изменился.')
        job = next(j for j in jobs if j['id'] == current['job'])
        if job['state'] not in ('queued', 'scheduled') or job['not_before'] > self.clock():
            raise HTTPException(409, 'Задание остановлено или время запуска ещё не наступило.')
        if current['state'] != ('sending' if writing else 'pending'):
            raise HTTPException(409, 'Повтор завершённого или уже выполняющегося получателя запрещён.')
        if not writing and (current.get('attempted_at') is not None or
                            json.loads(current.get('diagnosis') or '{}').get('invitation_requested') is True):
            raise HTTPException(409, 'У получателя уже сохранена попытка.')
        if writing and (not current.get('attempted_at') or current['attempted_at'][:10] != record['utc_day']):
            raise HTTPException(409, 'Разовая попытка не сохранена в текущем дне UTC.')
        count = self._count()
        if count < record['initial_count'] or count > record['account_daily_ceiling'] or (not writing and count == record['account_daily_ceiling']):
            raise HTTPException(409, 'Внутренний дневной бюджет недоступен или счётчик изменился.')
        return current, job

    def claim(self, key, item_id, *, client):
        record = self._record(key)
        item, _ = self._check(record, client, item_id=item_id, writing=False)
        if self._claim(item_id) is not None:
            raise HTTPException(409, 'Разовая попытка этого получателя уже учтена.')
        if self.status(key)['claims_used'] >= record['max_claims']:
            raise HTTPException(409, 'Разовый бюджет исчерпан.')
        if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1", (ACCOUNT,)):
            raise HTTPException(409, 'Другое действие аккаунта ещё выполняется.')
        claim = dict(item_id=item_id, request_key=key, job_id=item['job'], user_id=item['user_id'],
                     claimed_at=self.clock(), contact_write_at=None, invite_write_at=None)
        self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?)',
                              (CLAIM_PREFIX + str(item_id), json.dumps(claim, ensure_ascii=False)))
        return dict(request_key=key, item_id=item_id, job_id=item['job'], user_id=item['user_id'],
                    account_daily_ceiling=record['account_daily_ceiling'])

    def before_write(self, key, item_id, *, client, phase='invite'):
        """One durable intent per external write; second use is rejected."""
        record = self._record(key)
        item, job = self._check(record, client, item_id=item_id, writing=True)
        if phase not in ('contact', 'invite') or (phase == 'contact' and not json.loads(job['spec']).get('add_contacts_before_invite', False)):
            raise HTTPException(409, 'Такое внешнее действие не входит в сохранённое задание.')
        claim = self._claim(item_id)
        if not claim or (claim['request_key'], claim['job_id'], claim['user_id']) != (key, item['job'], item['user_id']):
            raise HTTPException(409, 'Исходная разовая попытка не найдена.')
        column = 'contact_write_at' if phase == 'contact' else 'invite_write_at'
        if claim[column] is not None or (phase == 'contact' and claim['invite_write_at'] is not None):
            raise HTTPException(409, 'Повтор внешнего действия этой попытки запрещён.')
        claim[column] = self.clock()
        self.store.db.execute('UPDATE settings SET value=? WHERE key=?',
                              (json.dumps(claim, ensure_ascii=False), CLAIM_PREFIX + str(item_id)))

    def _finish(self, record, reason):
        record.update(state=reason if reason in ('expired', 'budget_exhausted', 'complete') else 'stopped',
                      stop_reason=reason, stopped_at=self.clock())
        self._save(record)
        for jid in JOB_IDS:
            self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND account=? AND telegram_id=? AND kind='invite' AND state IN ('queued','scheduled') AND EXISTS (SELECT 1 FROM op_items WHERE job=? AND state='pending')",
                                  (datetime.fromtimestamp(self.clock(), timezone.utc).isoformat(), jid, ACCOUNT, SENDER_ID, jid))

    def stop(self, key, reason):
        """Persist a rejected grant and pause its pending jobs; caller commits.

        Intended for claim/write guard failures outside the failed transaction.
        An already stopped receipt is immutable and cannot be reactivated here.
        """
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise HTTPException(400, 'Нужна причина остановки конечного допуска.')
        record = self._record(key)
        if record['state'] == 'active':
            self._finish(record, reason.strip())
        else:
            # A legacy control can queue a job again after this receipt ended.
            # Reapply the scope's pause without rewriting terminal evidence.
            for jid in JOB_IDS:
                self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND account=? AND telegram_id=? AND kind='invite' AND state IN ('queued','scheduled') AND EXISTS (SELECT 1 FROM op_items WHERE job=? AND state='pending')",
                                      (datetime.fromtimestamp(self.clock(), timezone.utc).isoformat(), jid, ACCOUNT, SENDER_ID, jid))
        return self.status(key)

    def enforce_stops(self):
        """Caller commits this maintenance separately from a rejected claim.

        A cooldown alone is a wait, not a stop; an error/disabled account is a
        stop. Stops affect only queued/scheduled grant jobs with pending rows;
        they never alter item results, account state, spec or old pilot data.
        """
        stopped = []
        for record in self._records(GRANT_PREFIX):
            if record['state'] != 'active':
                continue
            key = record['request_key']
            reason = None
            if self.clock() >= record['expires_at'] or self._day() != record['utc_day']:
                reason = 'expired'
            elif key not in self._clients:
                reason = 'client_binding_lost'
            else:
                reason = self._runtime(self._clients[key], cooldown=False) or self._gates()
            try:
                jobs, items = self._jobs()
                if not reason and [self._job_binding(j) for j in jobs] != record['envelope']['jobs']:
                    reason = 'job_changed'
            except HTTPException:
                items = []
                reason = reason or 'job_changed'
            selected = {i['item_id'] for i in record['envelope']['items']}
            scoped = [i for i in items if i['id'] in selected]
            if not reason and not self._cohort_matches(record, items):
                reason = 'cohort_changed'
            sending = any(i['state'] == 'sending' for i in scoped)
            if not reason and not sending:
                if scoped and all(i['state'] not in ('pending', 'sending') for i in scoped):
                    reason = 'complete'
                elif self.status(key)['claims_used'] >= record['max_claims'] or self._count() >= record['account_daily_ceiling']:
                    reason = 'budget_exhausted'
            if not reason:
                continue
            self._finish(record, reason)
            stopped.append(dict(request_key=key, reason=reason))
        return stopped
