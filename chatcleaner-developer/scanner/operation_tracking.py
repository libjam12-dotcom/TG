"""Account-bound publication observations; no history crawl or automatic replies.

The journal is deliberately separate from op_items: an observation about presence
must never change a publication's send result. Telegram does not provide read
receipts for group publications, and a missing message does not name its remover.
"""
import asyncio
import hashlib
import json
import secrets
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from telethon import errors, events, functions, types, utils

from engine import now


class TrackingReply(BaseModel):
    model_config = {'extra': 'forbid'}
    event_id: int = Field(gt=0)
    text: str = Field(min_length=1, max_length=4096)
    request_key: str = Field(min_length=16, max_length=80)
    confirmed: Literal[True]


class TrackingInbox(BaseModel):
    model_config = {'extra': 'forbid'}
    enabled: bool
    telegram_id: int = Field(gt=0)


def safe_tracking_error(exc):
    """Never expose exception strings, request payloads, sessions or proxy data."""
    code = getattr(exc, 'code', None)
    return type(exc).__name__[:80], code if type(code) is int and -1000 <= code <= 1000 else None


def message_ids(value):
    try:
        rows = json.loads(value) if isinstance(value, str) else value
        return list(dict.fromkeys(n for n in rows if type(n) is int and n > 0))[:10]
    except (TypeError, ValueError):
        return []


def timestamp(value):
    try:
        date = datetime.fromisoformat(value)
        return (date if date.tzinfo else date.replace(tzinfo=timezone.utc)).timestamp()
    except (TypeError, ValueError):
        return 0


class OperationTracking:
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.bound = {}
        self.reply_lock = asyncio.Lock()
        self.check_lock = asyncio.Lock()
        self.router = APIRouter(prefix='/tracking')
        for path, fn, methods in [
            ('/jobs/{jid}', self.job, ['GET']),
            ('/jobs/{jid}/check', self.check, ['POST']),
            ('/jobs/{jid}/reply', self.reply, ['POST']),
            ('/accounts/{aid}/inbox', self.inbox, ['GET']),
            ('/accounts/{aid}/inbox', self.set_inbox, ['POST']),
            ('/accounts/{aid}/inbox/reply', self.inbox_reply, ['POST']),
        ]:
            self.router.add_api_route(path, fn, methods=methods)

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_tracking_observers(
          account TEXT PRIMARY KEY,telegram_id INTEGER NOT NULL,started_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS op_tracking_inboxes(
          account TEXT PRIMARY KEY,telegram_id INTEGER NOT NULL,enabled INTEGER NOT NULL DEFAULT 0,enabled_at TEXT);
        CREATE TABLE IF NOT EXISTS op_tracking_publications(
          item_id INTEGER PRIMARY KEY,job_id INTEGER NOT NULL,account TEXT NOT NULL,
          telegram_id INTEGER NOT NULL,chat_id INTEGER NOT NULL,message_ids TEXT NOT NULL,
          presence TEXT NOT NULL DEFAULT 'not_checked',last_checked_at TEXT,check_error_code TEXT,
          next_check_at REAL,check_count INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS op_tracking_publication_identity
          ON op_tracking_publications(account,telegram_id,chat_id);
        CREATE TABLE IF NOT EXISTS op_tracking_events(
          id INTEGER PRIMARY KEY,event_key TEXT UNIQUE NOT NULL,job_id INTEGER,item_id INTEGER,
          account TEXT NOT NULL,telegram_id INTEGER NOT NULL,chat_id INTEGER,message_id INTEGER,
          reply_to_msg_id INTEGER,sender_id INTEGER,parent_event_id INTEGER,kind TEXT NOT NULL,
          text TEXT NOT NULL DEFAULT '',source TEXT NOT NULL,error_code TEXT,rpc_code INTEGER,
          attribution TEXT NOT NULL,observed_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS op_tracking_events_job ON op_tracking_events(job_id,id);
        CREATE INDEX IF NOT EXISTS op_tracking_events_identity
          ON op_tracking_events(account,telegram_id,chat_id,message_id);
        CREATE TABLE IF NOT EXISTS op_tracking_replies(
          id INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,event_id INTEGER UNIQUE NOT NULL,
          digest TEXT NOT NULL,account TEXT NOT NULL,telegram_id INTEGER NOT NULL,chat_id INTEGER NOT NULL,
          text TEXT NOT NULL,random_id INTEGER NOT NULL,state TEXT NOT NULL,message_id INTEGER,
          error_code TEXT,rpc_code INTEGER,created_at TEXT NOT NULL,finished_at TEXT);
        ''')
        columns = {r['name'] for r in self.store.rows('PRAGMA table_info(op_tracking_publications)')}
        for name, definition in [('next_check_at', 'REAL'), ('check_count', 'INTEGER NOT NULL DEFAULT 0')]:
            if name not in columns:
                with self.store.db:
                    self.store.db.execute(f'ALTER TABLE op_tracking_publications ADD COLUMN {name} {definition}')
        if not self.store.get('operation_tracking_installed_at'):
            self.store.put('operation_tracking_installed_at', now())
        # A process restart cannot prove that an in-flight write did not arrive.
        for row in self.store.rows("SELECT * FROM op_tracking_replies WHERE state='sending'"):
            with self.store.db:
                self.store.db.execute("UPDATE op_tracking_replies SET state='unknown',error_code='ProcessInterrupted',finished_at=? WHERE id=?", (now(), row['id']))
            self._reply_event(row, 'unknown', error_code='ProcessInterrupted')
        self.sync_jobs()

    def _event(self, key, kind, *, job_id=None, item_id=None, account, telegram_id,
               chat_id=None, message_id=None, reply_to_msg_id=None, sender_id=None,
               parent_event_id=None, text='', source='live_update', error_code=None,
               rpc_code=None, attribution='exact_reply', observed_at=None):
        with self.store.db:
            self.store.db.execute('''INSERT OR IGNORE INTO op_tracking_events
              (event_key,job_id,item_id,account,telegram_id,chat_id,message_id,reply_to_msg_id,
               sender_id,parent_event_id,kind,text,source,error_code,rpc_code,attribution,observed_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (key, job_id, item_id, account, telegram_id, chat_id, message_id, reply_to_msg_id,
               sender_id, parent_event_id, kind, str(text or '')[:4096], source, error_code,
               rpc_code, attribution, observed_at or now()))
        return self.store.rows('SELECT * FROM op_tracking_events WHERE event_key=?', (key,))[0]

    def _job(self, jid):
        rows = self.store.rows('SELECT * FROM op_jobs WHERE id=?', (jid,))
        if not rows:
            raise HTTPException(404, 'Задание не найдено.')
        if rows[0]['kind'] != 'post':
            raise HTTPException(409, 'Наблюдение публикаций доступно для заданий отправки в группы.')
        return rows[0]

    def sync_jobs(self, jid=None):
        sql = "SELECT i.*,j.account,j.telegram_id,j.created AS job_created FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.kind='post'"
        rows = self.store.rows(sql + (' AND j.id=?' if jid is not None else ''), (jid,) if jid is not None else ())
        for item in rows:
            mids = message_ids(item['message_ids'])
            with self.store.db:
                self.store.db.execute('''INSERT INTO op_tracking_publications
                  (item_id,job_id,account,telegram_id,chat_id,message_ids) VALUES(?,?,?,?,?,?)
                  ON CONFLICT(item_id) DO UPDATE SET message_ids=excluded.message_ids
                  WHERE excluded.message_ids!='[]' AND account=excluded.account
                    AND telegram_id=excluded.telegram_id AND chat_id=excluded.chat_id''',
                  (item['id'], item['job'], item['account'], item['telegram_id'], item['chat_id'], json.dumps(mids)))
            base = dict(job_id=item['job'], item_id=item['id'], account=item['account'],
                        telegram_id=item['telegram_id'], chat_id=item['chat_id'],
                        source='existing_operation', attribution='saved_publication')
            prefix = 'item:' + str(item['id']) + ':'
            if item['attempted_at']:
                self._event(prefix + 'requested:' + item['attempted_at'], 'send_requested',
                            observed_at=item['attempted_at'], **base)
            if mids and item['state'] in ('sent', 'removed'):
                for mid in mids:
                    self._event(prefix + 'sent:' + str(mid), 'sent_confirmed', message_id=mid,
                                # Historical removed items overwrote finished_at with
                                # deletion time. Never label that as send time.
                                observed_at=(item['attempted_at'] if item['state'] == 'removed'
                                             else item['finished_at'] or item['attempted_at']),
                                text=('Восстановлено по сохранённым ID. Время приблизительное: начало попытки отправки.'
                                      if item['state'] == 'removed' else ''), **base)
            elif item['state'] == 'uncertain':
                self._event(prefix + 'unknown:' + str(item['attempted_at']), 'send_unknown',
                            observed_at=item['finished_at'] or item['attempted_at'], **base)
            elif item['attempted_at'] and (item['state'] in ('failed', 'blocked', 'restricted') or
                                          item['state'] == 'pending' and item['error']):
                self._event(prefix + 'failed:' + str(item['attempted_at']), 'send_failed',
                            error_code='OperationRejected', observed_at=item['finished_at'] or item['attempted_at'], **base)
            if item['state'] == 'removed':
                self._event(prefix + 'removed', 'removed_by_panel', observed_at=item['finished_at'], **base)
                with self.store.db:
                    self.store.db.execute("UPDATE op_tracking_publications SET presence='removed_by_panel' WHERE item_id=?", (item['id'],))
            elif mids and item['state'] == 'sent' and self._monitor_eligible(item):
                sent_at = timestamp(item['finished_at'] or item['attempted_at'])
                if sent_at:
                    with self.store.db:
                        self.store.db.execute('''UPDATE op_tracking_publications SET next_check_at=?
                          WHERE item_id=? AND next_check_at IS NULL AND check_count=0''',
                          (sent_at + 60, item['id']))

    def _monitor_eligible(self, item):
        if item.get('state', item.get('send_state')) != 'sent':
            return False
        installed = timestamp(self.store.get('operation_tracking_installed_at'))
        created = timestamp(item.get('job_created'))
        sent = timestamp(item.get('finished_at') or item.get('attempted_at'))
        return bool(installed and created >= installed and sent and 0 <= time.time() - sent < 86400)

    def _identity(self, aid, telegram_id=None, client=None):
        try:
            row = self.ops.account_row(aid)
        except HTTPException:
            return None
        account = self.ops.accounts.get(aid)
        if (not account or not account.me or not account.client or not account.client.is_connected()
                or account.me.id != row['telegram_id']
                or telegram_id is not None and account.me.id != telegram_id
                or client is not None and account.client is not client):
            return None
        return account

    def bind_clients(self):
        """Synchronously bind only Operations clients; never consult the owner sender."""
        for aid, (client, tid, incoming, deleted) in list(self.bound.items()):
            if not self._identity(aid, tid, client):
                client.remove_event_handler(incoming)
                client.remove_event_handler(deleted)
                del self.bound[aid]
        for aid in self.ops.accounts:
            a = self._identity(aid)
            if not a or aid in self.bound:
                continue
            client, tid = a.client, a.me.id
            async def incoming(event, account=aid, identity=tid, bound_client=client):
                await self.on_new_message(account, identity, bound_client, event)
            async def deleted(event, account=aid, identity=tid, bound_client=client):
                await self.on_deleted(account, identity, bound_client, event)
            client.add_event_handler(incoming, events.NewMessage(incoming=True))
            client.add_event_handler(deleted, events.MessageDeleted())
            self.bound[aid] = (client, tid, incoming, deleted)
            with self.store.db:
                self.store.db.execute('''INSERT INTO op_tracking_observers VALUES(?,?,?)
                  ON CONFLICT(account) DO UPDATE SET telegram_id=excluded.telegram_id,started_at=excluded.started_at''', (aid, tid, now()))

    def unbind_clients(self):
        for client, _, incoming, deleted in self.bound.values():
            client.remove_event_handler(incoming)
            client.remove_event_handler(deleted)
        self.bound.clear()

    def observer(self, aid, telegram_id=None):
        row = self.ops.account_row(aid)
        a = self._identity(aid, telegram_id)
        bound = self.bound.get(aid)
        started = self.store.rows('SELECT * FROM op_tracking_observers WHERE account=?', (aid,))
        inbox = self.store.rows('SELECT * FROM op_tracking_inboxes WHERE account=?', (aid,))
        identity_matches = row['telegram_id'] == telegram_id if telegram_id is not None else bool(a)
        return dict(connected=bool(a), identity_matches=bool(identity_matches and a),
                    observing=bool(a and bound and bound[0] is a.client and bound[1] == a.me.id),
                    observation_started_at=started[0]['started_at'] if started and started[0]['telegram_id'] == row['telegram_id'] else None,
                    inbox_enabled=bool(inbox and inbox[0]['enabled'] and inbox[0]['telegram_id'] == row['telegram_id']))

    def _account_json(self, aid):
        row = self.ops.account_row(aid)
        return {k: row.get(k) for k in ('id', 'label', 'telegram_id', 'username')}

    def _inbox_enabled(self, aid, tid):
        return bool(self.store.rows('SELECT 1 FROM op_tracking_inboxes WHERE account=? AND telegram_id=? AND enabled=1', (aid, tid)))

    async def on_new_message(self, aid, tid, client, event):
        if not self._identity(aid, tid, client):
            return
        msg = getattr(event, 'message', None)
        mid, chat = getattr(msg, 'id', None), getattr(event, 'chat_id', None)
        if not msg or getattr(msg, 'out', False) or type(mid) is not int or mid <= 0 or type(chat) is not int:
            return
        # Do not import queued/history private messages from before explicit opt-in.
        if getattr(event, 'is_private', False):
            if not self._inbox_enabled(aid, tid) or chat <= 0:
                return
            cfg = self.store.rows('SELECT enabled_at FROM op_tracking_inboxes WHERE account=?', (aid,))[0]
            date = getattr(msg, 'date', None)
            if not isinstance(date, datetime) or not cfg['enabled_at']:
                return
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            if date < datetime.fromisoformat(cfg['enabled_at']):
                return
            saved = self._event(f'incoming:{aid}:{tid}:{chat}:{mid}', 'incoming_dm', account=aid,
                        telegram_id=tid, chat_id=chat, message_id=mid, sender_id=getattr(msg, 'sender_id', None),
                        text=getattr(msg, 'message', ''), attribution='unknown')
            if hasattr(self.ops, 'redirects'):
                await self.ops.redirects.on_new_dm(aid, tid, client, event, saved)
            return
        if chat >= 0:
            return
        header = getattr(msg, 'reply_to', None)
        reply_to = getattr(msg, 'reply_to_msg_id', None) or getattr(header, 'reply_to_msg_id', None)
        # A cross-peer reply header must not borrow a same-numbered local message.
        peer = getattr(header, 'reply_to_peer_id', None)
        if peer is not None and utils.get_peer_id(peer) != chat:
            return
        if type(reply_to) is not int or reply_to <= 0:
            return
        self.sync_jobs()
        candidates = self.store.rows('SELECT * FROM op_tracking_publications WHERE account=? AND telegram_id=? AND chat_id=?', (aid, tid, chat))
        roots = [r for r in candidates if reply_to in message_ids(r['message_ids'])]
        parent = None
        if not roots:
            parents = self.store.rows("SELECT * FROM op_tracking_events WHERE account=? AND telegram_id=? AND chat_id=? AND message_id=? AND kind IN ('group_reply','reply_sent') AND job_id IS NOT NULL ORDER BY id LIMIT 1", (aid, tid, chat, reply_to))
            if not parents:
                return
            parent = parents[0]
            roots = self.store.rows('SELECT * FROM op_tracking_publications WHERE item_id=? AND account=? AND telegram_id=? AND chat_id=?', (parent['item_id'], aid, tid, chat))
        if not roots:
            return
        root = roots[0]
        self._event(f'incoming:{aid}:{tid}:{chat}:{mid}', 'group_reply', job_id=root['job_id'],
                    item_id=root['item_id'], account=aid, telegram_id=tid, chat_id=chat,
                    message_id=mid, reply_to_msg_id=reply_to, sender_id=getattr(msg, 'sender_id', None),
                    parent_event_id=parent['id'] if parent else None, text=getattr(msg, 'message', ''))

    async def on_deleted(self, aid, tid, client, event):
        if not self._identity(aid, tid, client):
            return
        # A deletion update can cover many messages; the album limit applies to
        # saved publication IDs, never to this independent update's ID list.
        deleted = getattr(event, 'deleted_ids', [])
        ids = {mid for mid in deleted[:100000] if type(mid) is int and mid > 0} if isinstance(deleted, (list, tuple)) else set()
        chat = getattr(event, 'chat_id', None)
        self.sync_jobs()
        rows = self.store.rows('SELECT * FROM op_tracking_publications WHERE account=? AND telegram_id=?', (aid, tid))
        for pub in rows:
            # Telegram omits peers for private/basic deletions, never for channels.
            if chat is not None and chat != pub['chat_id']:
                continue
            if chat is None and not (-10**12 < pub['chat_id'] < 0):
                continue
            found = set(ids).intersection(message_ids(pub['message_ids']))
            for mid in found:
                self._event(f'deleted:{aid}:{tid}:{pub["chat_id"]}:{mid}', 'deletion_observed',
                            job_id=pub['job_id'], item_id=pub['item_id'], account=aid, telegram_id=tid,
                            chat_id=pub['chat_id'], message_id=mid, attribution='saved_publication')
            if found:
                with self.store.db:
                    self.store.db.execute("UPDATE op_tracking_publications SET presence='deletion_observed' WHERE item_id=? AND presence!='removed_by_panel'", (pub['item_id'],))

    def _can_reply(self, event):
        if event['kind'] not in ('group_reply', 'incoming_dm'):
            return False
        if self.store.rows('SELECT 1 FROM op_tracking_replies WHERE event_id=?', (event['id'],)):
            return False
        if event['kind'] == 'incoming_dm' and not self._inbox_enabled(event['account'], event['telegram_id']):
            return False
        if event['job_id'] is not None:
            jobs = self.store.rows('SELECT account,telegram_id,kind FROM op_jobs WHERE id=?', (event['job_id'],))
            if not jobs or jobs[0] != dict(account=event['account'], telegram_id=event['telegram_id'], kind='post'):
                return False
        try:
            a = self.ops.online(event['account'])
            return a.me.id == event['telegram_id']
        except HTTPException:
            return False

    def _event_json(self, row):
        keys = ('id', 'job_id', 'item_id', 'kind', 'observed_at', 'chat_id', 'message_id',
                'reply_to_msg_id', 'sender_id', 'parent_event_id', 'text', 'source',
                'error_code', 'rpc_code', 'attribution')
        return {**{k: (row[k][:2000] if k == 'text' else row[k]) for k in keys},
                'can_reply': self._can_reply(row)}

    async def job(self, jid: int):
        job = self._job(jid)
        self.sync_jobs(jid)
        self.bind_clients()
        pubs = self.store.rows('''SELECT p.*,i.chat_title,i.chat_username,i.state AS send_state
          FROM op_tracking_publications p JOIN op_items i ON i.id=p.item_id WHERE p.job_id=? ORDER BY p.item_id LIMIT 30''', (jid,))
        publications = []
        for pub in pubs:
            mids, chat = message_ids(pub['message_ids']), pub['chat_id']
            username = pub['chat_username']
            links = [f'https://t.me/{username}/{mid}' if username else
                     f'https://t.me/c/{-chat-10**12}/{mid}' for mid in mids] if username or chat < -10**12 else []
            publications.append({k: pub[k] for k in ('item_id', 'chat_id', 'chat_title', 'send_state', 'presence', 'last_checked_at', 'check_error_code', 'check_count', 'next_check_at')} | {'message_ids': mids, 'links': links})
        rows = self.store.rows('SELECT * FROM op_tracking_events WHERE job_id=? ORDER BY id DESC LIMIT 300', (jid,))
        counts = dict(Counter(r['kind'] for r in self.store.rows('SELECT kind FROM op_tracking_events WHERE job_id=?', (jid,))))
        events = [self._event_json(r) for r in reversed(rows)]
        campaign_data = self.ops.campaigns.tracking_data(job, rows, events) if hasattr(self.ops, 'campaigns') else {}
        return dict(job_id=jid, account=self._account_json(job['account']), telegram_id=job['telegram_id'],
                    observer=self.observer(job['account'], job['telegram_id']), summary=counts,
                    publications=publications, events=events, **campaign_data)

    async def check(self, jid: int):
        job = self._job(jid)
        self.sync_jobs(jid)
        if self.check_lock.locked():
            raise HTTPException(409, 'Дождитесь завершения текущей проверки.')
        async with self.check_lock:
            a = self.ops.readable_account(job['account'])
            if a.me.id != job['telegram_id']:
                raise HTTPException(409, 'Личность аккаунта задания изменилась.')
            rows = self.store.rows('''SELECT p.*,j.created AS job_created,i.attempted_at,i.finished_at,i.state AS send_state
              FROM op_tracking_publications p JOIN op_jobs j ON j.id=p.job_id
              JOIN op_items i ON i.id=p.item_id WHERE p.job_id=? ORDER BY p.item_id LIMIT 30''', (jid,))
            for pub in rows:
                if not message_ids(pub['message_ids']):
                    continue
                try:
                    a = self.ops.readable_account(job['account'])
                except HTTPException:
                    break
                await self._check_publication(pub, a.client, 'message_id_check')
        return await self.job(jid)

    async def _check_publication(self, pub, client, source):
        mids = message_ids(pub['message_ids'])
        stamp, presence, code, rpc, cooldown = now(), None, None, None, 0
        try:
            if not self._identity(pub['account'], pub['telegram_id'], client):
                raise HTTPException(409, 'AccountIdentityChanged')
            result = await asyncio.wait_for(client.get_messages(pub['chat_id'], ids=mids), 20)
            if not isinstance(result, (list, tuple)):
                raise ValueError('InvalidMessageList')
            found = {m.id for m in result if m and not isinstance(m, types.MessageEmpty)
                     and getattr(m, 'id', None) in mids}
            presence = 'present' if len(found) == len(mids) else 'partially_missing' if found else 'missing_on_check'
        except Exception as exc:
            code, rpc = safe_tracking_error(exc)
            if isinstance(exc, errors.FloodWaitError):
                cooldown = time.time() + max(0, exc.seconds) + 1
                with self.store.db:
                    self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?', (cooldown, pub['account']))
        superseded = not code and bool(self.store.rows('''SELECT 1 FROM op_tracking_events
          WHERE item_id=? AND kind='deletion_observed' AND observed_at>=? LIMIT 1''', (pub['item_id'], stamp)))
        if superseded:
            # Newer passive evidence wins over a snapshot requested before it.
            presence = None
        count = pub['check_count'] + 1
        next_check = max(time.time() + (300 if count == 1 else 1800), cooldown) if self._monitor_eligible(pub) else 0
        with self.store.db:
            self.store.db.execute('''UPDATE op_tracking_publications SET last_checked_at=?,check_error_code=?,
              check_count=?,next_check_at=?,presence=CASE WHEN ? IS NULL OR presence='removed_by_panel'
              THEN presence ELSE ? END WHERE item_id=?''',
              (stamp, code, count, next_check, presence, presence, pub['item_id']))
        self._event(f'check:{pub["item_id"]}:{stamp}', 'check_failed' if code else 'check_superseded' if superseded else presence,
                    job_id=pub['job_id'], item_id=pub['item_id'], account=pub['account'], telegram_id=pub['telegram_id'],
                    chat_id=pub['chat_id'], source=source, error_code=code, rpc_code=rpc,
                    attribution='saved_publication', observed_at=stamp,
                    text='Во время проверки получено более новое событие удаления; предыдущий снимок не заменяет его.' if superseded else '')

    async def monitor_once(self):
        """One bounded, read-only presence probe; the existing worker owns cadence."""
        if self.check_lock.locked():
            return False
        async with self.check_lock:
            rows = self.store.rows('''SELECT p.*,j.created AS job_created,i.attempted_at,i.finished_at,i.state AS send_state
              FROM op_tracking_publications p JOIN op_jobs j ON j.id=p.job_id JOIN op_items i ON i.id=p.item_id
              WHERE i.state='sent' AND j.kind='post' AND p.account=j.account AND p.telegram_id=j.telegram_id
                AND p.next_check_at>0 AND p.next_check_at<=? ORDER BY p.next_check_at,p.item_id''', (time.time(),))
            for pub in rows:
                if not self._monitor_eligible(pub):
                    with self.store.db:
                        self.store.db.execute('UPDATE op_tracking_publications SET next_check_at=0 WHERE item_id=?', (pub['item_id'],))
                    continue
                try:
                    a = self.ops.readable_account(pub['account'])
                except HTTPException:
                    continue
                if a.me.id != pub['telegram_id']:
                    continue
                await self._check_publication(pub, a.client, 'automatic_presence_check')
                return True
        return False

    async def inbox(self, aid: str):
        self.ops.account_row(aid)
        self.bind_clients()
        account = self._account_json(aid)
        tid = account['telegram_id']
        rows = self.store.rows("SELECT * FROM op_tracking_events WHERE account=? AND telegram_id=? AND attribution='unknown' ORDER BY id DESC LIMIT 300", (aid, tid))
        return dict(account=account, telegram_id=tid, enabled=self._inbox_enabled(aid, tid),
                    observer=self.observer(aid, tid), events=[self._event_json(r) for r in reversed(rows)])

    async def set_inbox(self, aid: str, spec: TrackingInbox):
        row = self.ops.account_row(aid)
        if row['telegram_id'] != spec.telegram_id:
            raise HTTPException(409, 'Личность аккаунта изменилась. Обновите панель.')
        if spec.enabled and not self._identity(aid, spec.telegram_id):
            raise HTTPException(409, 'Подключите выбранный аккаунт для наблюдения входящих.')
        old = self.store.rows('SELECT * FROM op_tracking_inboxes WHERE account=?', (aid,))
        stamp = old[0]['enabled_at'] if old and old[0]['enabled'] and old[0]['telegram_id'] == spec.telegram_id else now()
        with self.store.db:
            self.store.db.execute('''INSERT INTO op_tracking_inboxes VALUES(?,?,?,?) ON CONFLICT(account)
              DO UPDATE SET telegram_id=excluded.telegram_id,enabled=excluded.enabled,enabled_at=excluded.enabled_at''',
              (aid, spec.telegram_id, int(spec.enabled), stamp if spec.enabled else None))
        return await self.inbox(aid)

    def _reply_event(self, row, state, message_id=None, error_code=None, rpc_code=None):
        event = self.store.rows('SELECT * FROM op_tracking_events WHERE id=?', (row['event_id'],))[0]
        return self._event('reply:' + str(row['id']) + ':' + state, 'reply_' + state,
                           job_id=event['job_id'], item_id=event['item_id'], account=row['account'],
                           telegram_id=row['telegram_id'], chat_id=row['chat_id'], message_id=message_id,
                           reply_to_msg_id=event['message_id'], parent_event_id=event['id'],
                           text=row['text'], source='manual_reply', error_code=error_code,
                           rpc_code=rpc_code, attribution=event['attribution'])

    async def reply(self, jid: int, spec: TrackingReply):
        job = self._job(jid)
        result = await self._send_reply(spec, jid=jid, aid=job['account'])
        return {**result, 'tracking': await self.job(jid)}

    async def inbox_reply(self, aid: str, spec: TrackingReply):
        result = await self._send_reply(spec, jid=None, aid=aid)
        return {**result, 'inbox': await self.inbox(aid)}

    async def _send_reply(self, spec, *, jid, aid):
        text = spec.text.strip()
        if not text:
            raise HTTPException(400, 'Введите текст ответа.')
        digest = hashlib.sha256(json.dumps([spec.event_id, text, aid, jid], ensure_ascii=False).encode()).hexdigest()
        async with self.reply_lock:
            existing = self.store.rows('SELECT * FROM op_tracking_replies WHERE request_key=?', (spec.request_key,))
            if existing:
                row = existing[0]
                if row['digest'] != digest:
                    raise HTTPException(409, 'Этот ключ уже относится к другому ответу.')
                return self._reply_receipt(row)
            rows = self.store.rows('SELECT * FROM op_tracking_events WHERE id=? AND account=?', (spec.event_id, aid))
            if not rows:
                raise HTTPException(404, 'Входящее сообщение не найдено.')
            event = rows[0]
            if (event['job_id'] != jid or event['kind'] != ('incoming_dm' if jid is None else 'group_reply')
                    or not event['message_id']):
                raise HTTPException(409, 'Ответ разрешён только на сохранённое входящее сообщение этого раздела.')
            if not self._can_reply(event):
                raise HTTPException(409, 'Ответ уже обработан либо исходящие или личность аккаунта недоступны.')
            a = self.ops.online(aid)
            if a.me.id != event['telegram_id']:
                raise HTTPException(409, 'Личность отправителя изменилась.')
            client = a.client
            # Entity resolution is read-only and occurs before claiming the write.
            peer = await asyncio.wait_for(client.get_input_entity(event['chat_id']), 20)
            if utils.get_peer_id(peer) != event['chat_id']:
                raise HTTPException(409, 'Telegram вернул другую площадку.')
            a = self.ops.online(aid)
            if a.client is not client or a.me.id != event['telegram_id']:
                raise HTTPException(409, 'Аккаунт изменился перед ответом.')
            if event['kind'] == 'incoming_dm' and not self._inbox_enabled(aid, event['telegram_id']):
                raise HTTPException(409, 'Наблюдение личных сообщений отключено.')
            with self.store.db:
                cur = self.store.db.execute('''INSERT INTO op_tracking_replies
                  (request_key,event_id,digest,account,telegram_id,chat_id,text,random_id,state,created_at)
                  VALUES(?,?,?,?,?,?,?,?,?,?)''', (spec.request_key, event['id'], digest, aid,
                  event['telegram_id'], event['chat_id'], text, secrets.randbits(63), 'sending', now()))
            row = self.store.rows('SELECT * FROM op_tracking_replies WHERE id=?', (cur.lastrowid,))[0]
            self._reply_event(row, 'requested')
            state, mid, code, rpc = 'unknown', None, None, None
            try:
                response = await asyncio.wait_for(client(functions.messages.SendMessageRequest(
                    peer=peer, message=text, random_id=row['random_id'],
                    reply_to=types.InputReplyToMessage(reply_to_msg_id=event['message_id']),
                    no_webpage=True)), 30)
                if isinstance(response, types.UpdateShortSentMessage):
                    mid = response.id
                else:
                    updates = getattr(response, 'updates', [])
                    for update in updates:
                        if isinstance(update, types.UpdateMessageID) and update.random_id == row['random_id']:
                            mid = update.id
                            break
                if type(mid) is int and mid > 0:
                    state = 'sent'
                else:
                    code = 'ReceiptMissing'
            except asyncio.CancelledError:
                with self.store.db:
                    self.store.db.execute("UPDATE op_tracking_replies SET state='unknown',error_code='CancelledError',finished_at=? WHERE id=?", (now(), row['id']))
                self._reply_event(row, 'unknown', error_code='CancelledError')
                raise
            except Exception as exc:
                code, rpc = safe_tracking_error(exc)
                if isinstance(exc, (errors.FloodWaitError, errors.PeerFloodError,
                                    errors.SessionRevokedError, errors.AuthKeyUnregisteredError,
                                    errors.UserDeactivatedError, errors.UserDeactivatedBanError)):
                    self.ops.pause_outbound(aid, exc)
                    if isinstance(exc, errors.FloodWaitError):
                        with self.store.db:
                            self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?', (time.time() + max(0, exc.seconds) + 1, aid))
                state = 'unknown' if not isinstance(exc, errors.RPCError) or isinstance(exc, (errors.ServerError, errors.TimedOutError)) else 'failed'
            with self.store.db:
                self.store.db.execute('''UPDATE op_tracking_replies SET state=?,message_id=?,error_code=?,rpc_code=?,finished_at=? WHERE id=?''', (state, mid, code, rpc, now(), row['id']))
            self._reply_event(row, state, mid, code, rpc)
            return self._reply_receipt(self.store.rows('SELECT * FROM op_tracking_replies WHERE id=?', (row['id'],))[0])

    def _reply_receipt(self, row):
        suffix = 'unknown' if row['state'] == 'sending' else row['state']
        result = self.store.rows('SELECT * FROM op_tracking_events WHERE event_key=?', ('reply:' + str(row['id']) + ':' + suffix,))
        return dict(state=suffix, request_key=row['request_key'], message_id=row['message_id'],
                    error_code=row['error_code'], event=self._event_json(result[0]) if result else None)
