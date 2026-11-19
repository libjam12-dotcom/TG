import json
from pathlib import Path
import secrets
import sqlite3
import time
from typing import Any

from antispam.domain.models import MessageContext, Decision
from antispam.moderation.normalize import content_hash


class Store:
    def __init__(self, path: Path | str):
        if str(path) != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path))
        self.db.row_factory = sqlite3.Row
        self.db.executescript(Path(__file__).with_name('schema.sql').read_text())
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS runtime_settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions(user_id INTEGER PRIMARY KEY, state_json TEXT NOT NULL, expires_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS confirmations(id TEXT PRIMARY KEY, actor_id INTEGER NOT NULL, payload_json TEXT NOT NULL, expires_at INTEGER NOT NULL, consumed INTEGER NOT NULL DEFAULT 0);
        ''')
        self.db.execute('INSERT OR IGNORE INTO schema_versions VALUES(1,?)', (int(time.time()),))
        self.db.execute("UPDATE action_jobs SET status='pending' WHERE status='running'")
        self.db.commit()

    def rows(self, sql: str, params=()) -> list[dict]:
        return [dict(x) for x in self.db.execute(sql, params).fetchall()]

    def one(self, sql: str, params=()) -> dict | None:
        row = self.db.execute(sql, params).fetchone()
        return dict(row) if row else None

    def write(self, sql: str, params=()) -> int:
        with self.db:
            cursor = self.db.execute(sql, params)
        return cursor.lastrowid or 0

    def register_chat(self, chat_id: int, title: str) -> None:
        self.write('INSERT INTO chats(chat_id,title) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title WHERE chats.title!=excluded.title', (chat_id, title))

    def chats(self) -> list[dict]:
        return self.rows('SELECT * FROM chats ORDER BY title')

    def chat(self, chat_id: int) -> dict | None:
        return self.one('SELECT * FROM chats WHERE chat_id=?', (chat_id,))

    def setting(self, key: str, default: Any = None) -> Any:
        row = self.one('SELECT value FROM runtime_settings WHERE key=?', (key,))
        return json.loads(row['value']) if row else default

    def set_setting(self, key: str, value: Any) -> None:
        self.write('INSERT INTO runtime_settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE runtime_settings.value!=excluded.value', (key, json.dumps(value)))

    def bump_policy(self, chat_id: int) -> None:
        self.write('UPDATE chats SET policy_version=policy_version+1 WHERE chat_id=? OR ?=0', (chat_id, chat_id))

    def audit(self, actor: int, chat_id: int, operation: str, details: dict) -> None:
        self.write('INSERT INTO admin_audit(actor_id,chat_id,operation,details_json,created_at) VALUES(?,?,?,?,?)',
                   (actor, chat_id or None, operation, json.dumps(details, ensure_ascii=False), int(time.time())))

    def rules(self, chat_id: int) -> list[dict]:
        return self.rows('SELECT * FROM rules WHERE enabled=1 AND (chat_id IS NULL OR chat_id=?) ORDER BY id', (chat_id,))

    def user_policy(self, chat_id: int, user_id: int | None, kind: str) -> bool:
        if user_id is None:
            return False
        return bool(self.one('SELECT id FROM user_policies WHERE user_id=? AND kind=? AND (chat_id IS NULL OR chat_id=?)', (user_id, kind, chat_id)))

    def set_user_policy(self, chat_id: int, user_id: int, kind: str, actor: int, remove=False) -> None:
        with self.db:
            self.db.execute('DELETE FROM user_policies WHERE COALESCE(chat_id,0)=? AND user_id=? AND kind=?', (chat_id, user_id, kind))
            if not remove:
                self.db.execute('INSERT INTO user_policies(chat_id,user_id,kind,created_by,created_at) VALUES(?,?,?,?,?)',
                                (chat_id or None, user_id, kind, actor, int(time.time())))
        self.bump_policy(chat_id)

    def exceptions(self, chat_id: int) -> list[dict]:
        return self.rows('SELECT * FROM rule_exceptions WHERE chat_id IS NULL OR chat_id=?', (chat_id,))

    def joined(self, chat_id: int, user_id: int, at: int) -> None:
        current = self.one('SELECT joined_at FROM member_state WHERE chat_id=? AND user_id=?', (chat_id, user_id))
        if current and current['joined_at'] is not None and abs(current['joined_at']-at) < 5:
            return
        self.write('INSERT INTO member_state(chat_id,user_id,joined_at,first_seen_at,message_count) VALUES(?,?,?,?,0) '
                   'ON CONFLICT(chat_id,user_id) DO UPDATE SET joined_at=excluded.joined_at,message_count=0', (chat_id, user_id, at, at))

    def member(self, chat_id: int, user_id: int | None) -> dict:
        return self.one('SELECT * FROM member_state WHERE chat_id=? AND user_id=?', (chat_id, user_id)) or {}

    def recent(self, ctx: MessageContext, since: int) -> list[dict]:
        if ctx.user_id is None:
            return []
        return self.rows('SELECT * FROM message_state WHERE user_id=? AND observed_at>=? AND NOT(chat_id=? AND message_id=?)',
                         (ctx.user_id, since, ctx.chat_id, ctx.message_id))

    def record(self, ctx: MessageContext, decision: Decision, actual: str, action_payload: dict | None = None) -> tuple[int | None, bool]:
        digest = content_hash(ctx.text + '\n' + '\n'.join(ctx.links))
        previous = self.one('SELECT content_hash FROM message_state WHERE chat_id=? AND message_id=?', (ctx.chat_id, ctx.message_id))
        if previous and previous['content_hash'] == digest:
            return None, False
        with self.db:
            if not previous and ctx.user_id is not None:
                self.db.execute('INSERT INTO member_state(chat_id,user_id,first_seen_at,message_count) VALUES(?,?,?,1) '
                                'ON CONFLICT(chat_id,user_id) DO UPDATE SET message_count=message_count+1', (ctx.chat_id,ctx.user_id,ctx.observed_at))
            self.db.execute('INSERT INTO message_state(chat_id,message_id,user_id,sender_chat_id,thread_id,content_hash,observed_at,edited_at) '
                            'VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(chat_id,message_id) DO UPDATE SET content_hash=excluded.content_hash,edited_at=excluded.edited_at',
                            (ctx.chat_id,ctx.message_id,ctx.user_id,ctx.sender_chat_id,ctx.thread_id,digest,ctx.observed_at,ctx.observed_at if ctx.is_edit else None))
            cur=self.db.execute('INSERT INTO moderation_events(chat_id,message_id,user_id,content_hash,excerpt,excerpt_expires_at,intended_action,reasons_json,policy_version,actual_result,created_at) '
                               'VALUES(?,?,?,?,?,?,?,?,?,?,?)', (ctx.chat_id,ctx.message_id,ctx.user_id,digest,ctx.text[:500],ctx.observed_at+7*86400,
                               decision.action.value,json.dumps([s.code for s in decision.signals],ensure_ascii=False),decision.policy_version,actual,ctx.observed_at))
            event_id=cur.lastrowid
            # Persist the action with the event before yielding to notifications or Telegram.
            # A restart between record() and Executor.run() must not lose moderation.
            if action_payload is not None:
                self.db.execute('INSERT INTO action_jobs(event_id,operation_key,chat_id,action,payload_json) VALUES(?,?,?,?,?)',
                    (event_id,f'event:{event_id}:automatic',ctx.chat_id,action_payload['action'],json.dumps(action_payload)))
            if decision.needs_admin or (actual=='observed' and decision.action.value!='allow'):
                self.db.execute('INSERT INTO review_queue(event_id) VALUES(?)',(event_id,))
        return event_id, True

    def set_session(self, user_id: int, state: dict) -> None:
        self.write('INSERT INTO sessions VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET state_json=excluded.state_json,expires_at=excluded.expires_at',
                   (user_id,json.dumps(state),int(time.time())+600))

    def session(self, user_id: int) -> dict | None:
        row=self.one('SELECT * FROM sessions WHERE user_id=? AND expires_at>?',(user_id,int(time.time())))
        return json.loads(row['state_json']) if row else None

    def clear_session(self, user_id: int) -> None:
        self.write('DELETE FROM sessions WHERE user_id=?',(user_id,))

    def confirmation(self, actor: int, payload: dict) -> str:
        key=secrets.token_hex(8)
        self.write('INSERT INTO confirmations(id,actor_id,payload_json,expires_at) VALUES(?,?,?,?)',(key,actor,json.dumps(payload),int(time.time())+300))
        return key

    def consume_confirmation(self, key: str, actor: int) -> dict | None:
        with self.db:
            row=self.db.execute('SELECT * FROM confirmations WHERE id=? AND actor_id=? AND consumed=0 AND expires_at>?',(key,actor,int(time.time()))).fetchone()
            if not row: return None
            self.db.execute('UPDATE confirmations SET consumed=1 WHERE id=?',(key,))
        return json.loads(row['payload_json'])

    def cleanup(self) -> None:
        now=int(time.time())
        with self.db:
            self.db.execute('UPDATE moderation_events SET excerpt=NULL WHERE excerpt_expires_at<? AND excerpt IS NOT NULL',(now,))
            # Short-lived counters; preserve event history separately.
            self.db.execute('DELETE FROM message_state WHERE observed_at<?',(now-86400,))
            self.db.execute('DELETE FROM sessions WHERE expires_at<?',(now,))
            self.db.execute('DELETE FROM confirmations WHERE expires_at<?',(now,))
            self.db.execute('DELETE FROM ai_cache WHERE expires_at<?',(now,))
            self.db.execute('DELETE FROM processed_updates WHERE processed_at<?',(now-2*86400,))
            cutoff=now-30*86400
            old='SELECT id FROM moderation_events WHERE created_at<?'
            # Retain events while an action is still pending; expire old unresolved reviews.
            self.db.execute("DELETE FROM review_queue WHERE event_id IN ("+old+")",(cutoff,))
            self.db.execute("DELETE FROM action_jobs WHERE status IN ('done','failed') AND event_id IN ("+old+")",(cutoff,))
            self.db.execute('DELETE FROM moderation_events WHERE created_at<? AND id NOT IN (SELECT event_id FROM action_jobs WHERE event_id IS NOT NULL)',(cutoff,))
            self.db.execute('DELETE FROM admin_audit WHERE created_at<?',(cutoff,))

    def backup(self, destination: Path) -> None:
        destination.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(destination) as target:
            self.db.backup(target)

    def export_config(self) -> dict:
        # Explicit allowlist: sessions, credentials, message excerpts and notification
        # recipients never belong in the portable moderation configuration.
        prefixes=('autoban:','quarantine_auto:','behavior_auto:','behavior:','ad_enabled:','ad_contact:',
                  'advertising:','ad_rule_ids:','obfuscated_words:','learned_words:',
                  'contextual_slang:','auto_ban_patterns:','learned_critical_rules:',
                  'reporting_context:','fuzzy_review_targets:','review_spelling_patterns:')
        settings={r['key']:json.loads(r['value']) for r in self.rows('SELECT * FROM runtime_settings') if r['key'].startswith(prefixes)}
        return {'format':'chatcleaner-antispam-config-v1','exported_at':int(time.time()),
            'chats':self.chats(),'rules':self.rows('SELECT * FROM rules ORDER BY id'),
            'exceptions':self.rows('SELECT * FROM rule_exceptions ORDER BY id'),
            'users':self.rows('SELECT * FROM user_policies ORDER BY id'),'settings':settings}

    def close(self) -> None:
        self.db.close()
