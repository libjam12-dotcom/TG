"""Persistent, literal matching and deduplication. No network side effects."""
import hashlib
import json
import re
import sqlite3
import time
from collections import deque
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from functools import lru_cache


def now():
    return datetime.now(timezone.utc).isoformat()


def normalize(value):
    return ' '.join(unicodedata.normalize('NFKC', value).casefold().replace('ё', 'е').split())


def words(value):
    rows = value.splitlines() if isinstance(value, str) else value
    return list(dict.fromkeys(normalize(s) for s in rows if normalize(s)))


@lru_cache(maxsize=2048)
def phrase_pattern(word):
    return re.compile(r'(?<!\w)' + re.escape(word) + r'(?!\w)')


def explain(text, rule):
    original = text
    text = normalize(text)
    def has(w):
        return w in text if rule.get('substring') else phrase_pattern(w).search(text) is not None
    hits = [w for w in rule['keywords'] if has(w)]
    required = [w for w in rule.get('required', []) if has(w)] if hits else []
    excluded = [w for w in rule.get('excluded', []) if has(w)] if hits else []
    reason = 'no_keywords' if not hits else 'excluded' if excluded else 'no_intent' if rule.get('required') and not required else 'matched'
    detail = None
    if reason == 'matched':
        if rule.get('lead_profile') == 'signm_dubai_v1':
            # Only the explicitly selected personal SignM profile changes its
            # intent policy. Existing client and owner rules keep their filters.
            from signm_intent import assess_request
            assessment = assess_request(original, rule)
            if assessment['label'] != 'direct_request':
                detail = 'unrelated_intent' if assessment['label'] == 'unclear' else assessment['label']
        else:
            detail = scope_rejection(original, rule)
            if not detail and rule.get('demand_only'):
                detail = demand_rejection(original, rule)
        if detail:
            reason = 'no_intent' if detail == 'unrelated_intent' else 'excluded'
    return dict(reason=reason, keywords=hits, required=required, excluded=excluded, detail=detail)


def scope_rejection(text, rule):
    """Optional owner filters; client rules without these flags are unchanged."""
    text = normalize(text)
    if rule.get('language') == 'ru':
        russian = len(re.findall(r'[а-я]', text))
        latin = len(re.findall(r'[a-z]', text))
        if russian < 4 or russian < latin * 0.4 or re.search(r'[іїєґў]',text):
            return 'other_language'
    if rule.get('contractor_only'):
        if re.search(r'(?<!\w)(?:помогу|ищу проекты|ищу проект|ищу новый проект|ищем проекты|беру проект|беру 1 проект|'
                     r'я авитолог|создаю цифровые продукты|я действующий разработчик|'
                     r'занимаюсь разработкой сайтов|занимаюсь веб-дизайном|разрабатываю сайты|разрабатываю веб-приложения|'
                     r'разрабатываю\s*:\s*сайты|сделаю под ключ|вам нужен|вам нужна)(?!\w)',text):
            return 'supplier_offer'
        if any(phrase in text for phrase in ('устали искать заказы','активировать подписку','активировать бота',
                                             'ботработ','мониторит более 10 000','бот следит за открытыми')):
            return 'supplier_offer'
        hard = (r'в штат|штатн\w*|на постоянную работу|постоянн\w* сотрудник\w*|'
                r'полный рабочий день|полная занятость|заработная плата|зарплат\w*|оклад\w*|'
                r'трудоустройство|трудов\w* договор\w*|по тк рф|испытательный срок|'
                r'соцпакет|карьерный рост|full[ -]?time|salary|5\s*[/×хx]\s*2|2\s*[/×хx]\s*2|'
                r'[48]0 часов в неделю|сотрудник\w* в команд\w*|личного помощника|специалист\w* для общения с клиентами')
        if re.search(r'(?<!\w)(?:'+hard+r')(?!\w)', text):
            return 'employment'
        vacancy = re.search(r'(?<!\w)(?:ваканси\w*|hiring|job opening|обязанности|график работы|сотрудник\w*)(?!\w)', text)
        project = re.search(r'(?<!\w)(?:проектн\w*|разов\w*|подряд\w*|фриланс\w*|на проект|для проекта|на запуск|аутсорс\w*)(?!\w)', text)
        if vacancy and not project:
            return 'employment'
    return None


def demand_rejection(text, rule):
    """Conservative, explainable filters. This is not semantic AI classification."""
    normalized = normalize(text)
    topic = rule.get('topic')
    offers = r'(?<!\w)(?:предлагаю|предлагаем|продаю|продаем|сдаю|сдаем|оказываю|оказываем|наши услуги|мои услуги|we offer|our services|book with us)(?!\w)'
    for offer in re.finditer(offers, normalized):
        if not re.search(r'(?:\bне|\bnot)\s*$', normalized[:offer.start()]):
            return 'supplier_offer'
    if re.search(r'(?<!\w)(?:ищу работу|ищем работу|ищу подработку|ищу заказы|ищу заказчиков|looking for work|looking for gigs|looking for a job|мое резюме|есть опыт работы)(?!\w)', normalized):
        return 'job_seeker'
    if not rule.get('contractor_only') and topic in ('cars','transfers','hotels','visas','emirates_id','smm') and re.search(r'(?<!\w)(?:вакансия|вакансии|зарплата|оклад|в штат|на постоянную работу|резюме|job vacancy|salary|full time|full-time)(?!\w)', normalized):
        return 'employment'
    if topic in ('visas','emirates_id') and re.search(r'(?<!\w)(?:виза предоставляется|визу предоставляем|visa provided|visa is provided|со своей визой|с собственной визой|есть виза|есть emirates id)(?!\w)', normalized):
        return 'existing_document'
    if topic == 'safari' and re.search(r'(?<!\w)(?:кения|кении|кению|танзания|танзании|танзанию|zanzibar|занзибар|африка|африке|африку|kenya|tanzania|zanzibar|south africa)(?!\w)', normalized) and not re.search(r'(?<!\w)(?:дубай|дубае|дубая|оаэ|dubai|uae)(?!\w)', normalized):
        return 'other_geography'
    required = rule.get('required', [])
    if required:
        def has(part, phrase):
            return phrase in part if rule.get('substring') else phrase_pattern(phrase).search(part) is not None
        # Sentence boundaries prevent currency questions at a hotel from becoming
        # hotel leads. Line breaks alone are preserved as part of one sentence.
        clauses = [normalize(part) for part in re.split(r'[.!?;。！？]+', text)]
        if not any(any(has(part, word) for word in rule['keywords']) and
                   any(has(part, intent) for intent in required) for part in clauses):
            return 'unrelated_intent'
    return None


def matches(text, rule):
    result = explain(text, rule)
    return result['keywords'] if result['reason'] == 'matched' else []


class MeasuredConnection(sqlite3.Connection):
    """Bounded in-memory commit timings; no statement text or user data."""
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs);self.commit_times=deque(maxlen=128)

    def __exit__(self,*args):
        started=time.perf_counter()
        try:return super().__exit__(*args)
        finally:self.commit_times.append(time.perf_counter()-started)


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.notification_hook = None
        self.db = sqlite3.connect(path, check_same_thread=False,factory=MeasuredConnection)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY,title TEXT NOT NULL,username TEXT,enabled INTEGER NOT NULL DEFAULT 1,cursor INTEGER NOT NULL DEFAULT 0,last_scan TEXT,error TEXT);
        CREATE TABLE IF NOT EXISTS rules(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,data TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS leads(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,text TEXT NOT NULL,author TEXT NOT NULL,sender_id INTEGER,link TEXT,created TEXT NOT NULL,found TEXT NOT NULL,fingerprint TEXT NOT NULL,matches TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'new',UNIQUE(chat_id,message_id));
        CREATE INDEX IF NOT EXISTS leads_fingerprint ON leads(fingerprint,found);
        CREATE INDEX IF NOT EXISTS leads_created_id ON leads(created DESC,id DESC);
        CREATE INDEX IF NOT EXISTS leads_status_created ON leads(status,created DESC,id DESC);
        CREATE TABLE IF NOT EXISTS outbox(lead_id INTEGER PRIMARY KEY REFERENCES leads(id),destination INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,error TEXT,random_id INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS parser_runs(id INTEGER PRIMARY KEY AUTOINCREMENT,spec TEXT NOT NULL,state TEXT NOT NULL,done INTEGER NOT NULL DEFAULT 0,total INTEGER NOT NULL,created TEXT NOT NULL,updated TEXT NOT NULL,error TEXT,limited INTEGER NOT NULL DEFAULT 0,stats TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS parser_results(run_id INTEGER NOT NULL REFERENCES parser_runs(id) ON DELETE CASCADE,chat_id INTEGER NOT NULL,data TEXT NOT NULL,PRIMARY KEY(run_id,chat_id));
        ''')
        columns = {row['name'] for row in self.db.execute('PRAGMA table_info(sources)')}
        if 'access_mode' not in columns:
            self.db.execute("ALTER TABLE sources ADD COLUMN access_mode TEXT NOT NULL DEFAULT 'member'")
        if 'last_message_at' not in columns:
            self.db.execute('ALTER TABLE sources ADD COLUMN last_message_at TEXT')
        for name, definition in [('messages_checked', 'INTEGER NOT NULL DEFAULT 0'), ('stats_since', 'TEXT')]:
            if name not in columns: self.db.execute(f'ALTER TABLE sources ADD COLUMN {name} {definition}')
        if 'origin' not in {row['name'] for row in self.db.execute('PRAGMA table_info(leads)')}:
            self.db.execute("ALTER TABLE leads ADD COLUMN origin TEXT NOT NULL DEFAULT 'live'")
        self.db.commit()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def storage_health(self):
        values=list(self.db.commit_times)
        return {'sample':len(values),'recent_commit_max_ms':round(max(values,default=0)*1000,2),
                'recent_commit_total_ms':round(sum(values)*1000,2)}

    def put(self, key, value):
        with self.db:
            self.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE settings.value!=excluded.value', (key, json.dumps(value)))

    def rows(self, sql, args=()):
        return [dict(row) for row in self.db.execute(sql, args)]

    def rules(self):
        return [dict(id=r['id'], name=r['name'], enabled=bool(r['enabled']), **json.loads(r['data'])) for r in self.rows('SELECT * FROM rules ORDER BY id')]

    def ingest(self, chat_id, message_id, text, author='', sender_id=None, link=None, created=None, *, origin='live'):
        source = self.db.execute('SELECT enabled FROM sources WHERE id=?', (chat_id,)).fetchone()
        if not source or not source[0] or not text or (origin == 'live' and not self.get('running', False)):
            return None
        # Destination must never become an input, including if it was added earlier.
        if chat_id == self.get('destination') or chat_id == self.get('notification_bot_config',{}).get('chat_id'):
            return None
        found = []
        for rule in self.rules():
            if rule['enabled'] and chat_id in rule['sources']:
                hits = matches(text, rule)
                if hits:
                    found.append({'id': rule['id'], 'name': rule['name'], 'keywords': hits})
        if not found:
            return None
        fingerprint = hashlib.sha256(f'{sender_id if sender_id else chat_id}:{normalize(text)}'.encode()).hexdigest()
        if self.db.execute("SELECT 1 FROM leads WHERE fingerprint=? AND julianday(found)>julianday('now','-1 day')", (fingerprint,)).fetchone():
            return None
        with self.db:
            cur = self.db.execute('INSERT OR IGNORE INTO leads(chat_id,message_id,text,author,sender_id,link,created,found,fingerprint,matches,origin) VALUES(?,?,?,?,?,?,?,?,?,?,?)', (chat_id,message_id,text,author,sender_id,link,created or now(),now(),fingerprint,json.dumps(found,ensure_ascii=False),origin))
            if not cur.rowcount:
                return None
            lead_id = cur.lastrowid
            destination = self.get('destination')
            if destination and origin == 'live' and self.get('notification_mode','account')=='account':
                import secrets
                self.db.execute('INSERT INTO outbox(lead_id,destination,random_id) VALUES(?,?,?)', (lead_id,destination,secrets.randbits(63)))
            if self.notification_hook:self.notification_hook(lead_id,origin)
        return lead_id
