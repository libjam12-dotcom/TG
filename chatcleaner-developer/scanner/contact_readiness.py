"""Owner-only contact evidence. Never promote a username in a repost body."""
import hashlib
import json
import re
from urllib.parse import urlsplit
from fastapi import HTTPException
from engine import now

VERSION = 1
AGGREGATORS = {'synthetic_job_feed'}
MARKETPLACES = {'kwork.ru', 'kwork.com', 'fl.ru', 'freelance.ru', 'freelancehunt.com', 'upwork.com'}


def classify(lead):
    if lead.get('origin') == 'manual_dialogue':
        return {'kind': 'contacted_dialogue', 'reason': 'Ранее отправленное владельцем предложение. Это запись переписки, а не новая заявка.', 'contact_username': None, 'task_url': None}
    text = lead.get('text', '')
    urls = re.findall(r'https?://[^\s<>]+', text)
    task = None
    for url in urls:
        parsed = urlsplit(url.rstrip('.,);'))
        host = (parsed.hostname or '').lower().removeprefix('www.')
        if host in MARKETPLACES and re.search(r'/(?:projects?|jobs?|orders?)/[^/?#]+', parsed.path, re.I):
            task = url.rstrip('.,);'); break
    source = (lead.get('source_username') or '').casefold()
    marketplace = task or re.search(r'кворк|\bkwork\b|безопасн\w* сделк\w*|заказ\w* (?:с|из) бирж', text, re.I)
    author = lead.get('author', '').strip()
    uid = lead.get('sender_id')
    if task:
        return {'kind': 'marketplace_task', 'reason': 'Есть ссылка на заказ биржи. Отклик оформляется на бирже; Telegram-контакт заказчика не подтверждён.', 'contact_username': None, 'task_url': task}
    if source in AGGREGATORS or marketplace or not isinstance(uid, int) or uid <= 0:
        return {'kind': 'missing_contact', 'reason': 'Публикация канала/агрегатора или репост биржи. Личный контакт заказчика не подтверждён. Username канала и контакты в тексте не считаются автором заявки.', 'contact_username': None, 'task_url': None}
    if not re.fullmatch(r'@[A-Za-z][A-Za-z0-9_]{3,31}', author) or author.casefold().endswith('bot') or author[1:].casefold() == source:
        return {'kind': 'missing_contact', 'reason': 'Нет отдельного личного username автора. Контакт из текста требует ручной проверки; подставлять его автоматически нельзя.', 'contact_username': None, 'task_url': None}
    return {'kind': 'telegram_author', 'reason': 'Telegram сохранил положительный ID и личный username автора сообщения. Актуальность запроса и роль заказчика нужно уточнить.', 'contact_username': author[1:], 'task_url': None}


class ContactReadiness:
    def __init__(self, store): self.store = store

    def initialize(self):
        first = not self.store.db.execute("SELECT 1 FROM sqlite_master WHERE name='owner_contact_dirty'").fetchone()
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS owner_contact_readiness(lead_id INTEGER PRIMARY KEY,kind TEXT NOT NULL,reason TEXT NOT NULL,contact_username TEXT,task_url TEXT,fingerprint TEXT NOT NULL,version INTEGER NOT NULL,assessed TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS owner_contact_review_history(id INTEGER PRIMARY KEY,lead_id INTEGER NOT NULL,assessment TEXT NOT NULL,created TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS owner_contact_kind ON owner_contact_readiness(kind,lead_id);
        CREATE INDEX IF NOT EXISTS owner_contact_history_lead ON owner_contact_review_history(lead_id,id DESC);
        CREATE TABLE IF NOT EXISTS owner_contact_dirty(lead_id INTEGER PRIMARY KEY);
        CREATE TRIGGER IF NOT EXISTS owner_contact_lead_insert AFTER INSERT ON leads BEGIN
          INSERT OR IGNORE INTO owner_contact_dirty VALUES(NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS owner_contact_lead_update AFTER UPDATE OF text,author,sender_id,origin,chat_id ON leads
        WHEN OLD.text IS NOT NEW.text OR OLD.author IS NOT NEW.author OR OLD.sender_id IS NOT NEW.sender_id OR OLD.origin IS NOT NEW.origin OR OLD.chat_id IS NOT NEW.chat_id BEGIN
          INSERT OR IGNORE INTO owner_contact_dirty VALUES(NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS owner_contact_lead_delete AFTER DELETE ON leads BEGIN
          DELETE FROM owner_contact_dirty WHERE lead_id=OLD.id;
          DELETE FROM owner_contact_readiness WHERE lead_id=OLD.id;
        END;
        CREATE TRIGGER IF NOT EXISTS owner_contact_source_insert AFTER INSERT ON sources BEGIN
          INSERT OR IGNORE INTO owner_contact_dirty SELECT id FROM leads WHERE chat_id=NEW.id;
        END;
        CREATE TRIGGER IF NOT EXISTS owner_contact_source_update AFTER UPDATE OF username ON sources
        WHEN OLD.username IS NOT NEW.username BEGIN
          INSERT OR IGNORE INTO owner_contact_dirty SELECT id FROM leads WHERE chat_id=NEW.id;
        END;
        CREATE TRIGGER IF NOT EXISTS owner_contact_source_delete AFTER DELETE ON sources BEGIN
          INSERT OR IGNORE INTO owner_contact_dirty SELECT id FROM leads WHERE chat_id=OLD.id;
        END;
        ''')
        with self.store.db:
            if first:
                self.store.db.execute('INSERT OR IGNORE INTO owner_contact_dirty SELECT id FROM leads')
            else:
                self.store.db.execute('INSERT OR IGNORE INTO owner_contact_dirty SELECT l.id FROM leads l LEFT JOIN owner_contact_readiness r ON r.lead_id=l.id WHERE r.lead_id IS NULL OR r.version!=?', (VERSION,))
        self.backfill()

    def assess(self, lead):
        with self.store.db:
            return self._assess(lead)

    def _assess(self, lead):
        fingerprint = hashlib.sha256(json.dumps({key: lead.get(key) for key in ('text', 'author', 'sender_id', 'source_username', 'origin')}, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        old = self.store.rows('SELECT * FROM owner_contact_readiness WHERE lead_id=?', (lead['id'],))
        if old and old[0]['fingerprint'] == fingerprint and old[0]['version'] == VERSION: return old[0]
        data = classify(lead)
        data.update(lead_id=lead['id'], fingerprint=fingerprint, version=VERSION, assessed=now())
        self.store.db.execute('INSERT INTO owner_contact_review_history(lead_id,assessment,created) VALUES(?,?,?)', (lead['id'], json.dumps(data, ensure_ascii=False), now()))
        self.store.db.execute('INSERT INTO owner_contact_readiness(lead_id,kind,reason,contact_username,task_url,fingerprint,version,assessed) VALUES(:lead_id,:kind,:reason,:contact_username,:task_url,:fingerprint,:version,:assessed) ON CONFLICT(lead_id) DO UPDATE SET kind=excluded.kind,reason=excluded.reason,contact_username=excluded.contact_username,task_url=excluded.task_url,fingerprint=excluded.fingerprint,version=excluded.version,assessed=excluded.assessed', data)
        return data

    def backfill(self):
        if not self.store.db.execute('SELECT 1 FROM owner_contact_dirty LIMIT 1').fetchone(): return
        # Hold the write transaction before reading dirty rows: a concurrent
        # update cannot be lost between classification and clearing the queue.
        own_transaction = not self.store.db.in_transaction
        if own_transaction: self.store.db.execute('BEGIN IMMEDIATE')
        try:
            leads = self.store.rows('SELECT l.*,s.username AS source_username FROM owner_contact_dirty d JOIN leads l ON l.id=d.lead_id LEFT JOIN sources s ON s.id=l.chat_id')
            for lead in leads: self._assess(lead)
            self.store.db.executemany('DELETE FROM owner_contact_dirty WHERE lead_id=?', [(lead['id'],) for lead in leads])
            if own_transaction: self.store.db.commit()
        except Exception:
            if own_transaction: self.store.db.rollback()
            raise

    def saved(self, leads):
        if not leads: return {}
        return {r['lead_id']: r for r in self.store.rows('SELECT * FROM owner_contact_readiness WHERE lead_id IN ('+','.join('?' for _ in leads)+')', tuple(l['id'] for l in leads))}

    async def history(self, lead_id: int):
        return {'history': [json.loads(r['assessment']) for r in self.store.rows('SELECT assessment FROM owner_contact_review_history WHERE lead_id=? ORDER BY id DESC', (lead_id,))]}

    def require_personal_author(self, lead):
        rows = self.store.rows('SELECT username FROM sources WHERE id=?', (lead['chat_id'],))
        assessment = self.assess({**lead, 'source_username': rows[0]['username'] if rows else None})
        if assessment['kind'] not in ('telegram_author', 'contacted_dialogue'):
            raise HTTPException(409, assessment['reason']+' Личный диалог автоматически не открывается.')
