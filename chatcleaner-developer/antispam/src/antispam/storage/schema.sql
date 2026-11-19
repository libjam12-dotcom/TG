-- Draft schema v1. No production database exists yet.
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)),
    mode TEXT NOT NULL DEFAULT 'observe' CHECK(mode IN ('observe','active')),
    policy_version INTEGER NOT NULL DEFAULT 1,
    overrides_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER REFERENCES chats(chat_id),
    kind TEXT NOT NULL CHECK(kind IN ('word','phrase','domain','url')),
    pattern TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('review','delete','restrict','ban')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_by INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS user_policies (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER REFERENCES chats(chat_id),
    user_id INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('trusted','blocked')),
    scope_json TEXT NOT NULL DEFAULT '{}',
    created_by INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS user_policy_unique ON user_policies(COALESCE(chat_id,0), user_id, kind);
CREATE TABLE IF NOT EXISTS rule_exceptions (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER REFERENCES chats(chat_id),
    rule_id INTEGER REFERENCES rules(id),
    kind TEXT NOT NULL CHECK(kind IN ('user','phrase','domain','category')),
    value TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    created_by INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS member_state (
    chat_id INTEGER NOT NULL REFERENCES chats(chat_id),
    user_id INTEGER NOT NULL,
    joined_at INTEGER,
    first_seen_at INTEGER NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(chat_id,user_id)
);
CREATE TABLE IF NOT EXISTS message_state (
    chat_id INTEGER NOT NULL REFERENCES chats(chat_id),
    message_id INTEGER NOT NULL,
    user_id INTEGER,
    sender_chat_id INTEGER,
    thread_id INTEGER,
    content_hash TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    edited_at INTEGER,
    PRIMARY KEY(chat_id,message_id)
);
CREATE INDEX IF NOT EXISTS recent_author_messages ON message_state(user_id,observed_at);
CREATE INDEX IF NOT EXISTS message_state_observed_at ON message_state(observed_at);
CREATE TABLE IF NOT EXISTS processed_updates (
    update_id INTEGER PRIMARY KEY, processed_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS moderation_events (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL REFERENCES chats(chat_id),
    message_id INTEGER,
    user_id INTEGER,
    content_hash TEXT,
    excerpt TEXT,
    excerpt_expires_at INTEGER,
    intended_action TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    actual_result TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS moderation_events_created_at ON moderation_events(created_at);
CREATE INDEX IF NOT EXISTS moderation_events_excerpt_expiry ON moderation_events(excerpt_expires_at) WHERE excerpt IS NOT NULL;
CREATE TABLE IF NOT EXISTS review_queue (
    event_id INTEGER PRIMARY KEY REFERENCES moderation_events(id),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','resolved','expired')),
    resolved_by INTEGER,
    resolved_at INTEGER
);
CREATE TABLE IF NOT EXISTS action_jobs (
    id INTEGER PRIMARY KEY,
    event_id INTEGER REFERENCES moderation_events(id),
    operation_key TEXT NOT NULL UNIQUE,
    chat_id INTEGER NOT NULL REFERENCES chats(chat_id),
    action TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','done','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER,
    error_code TEXT
);
CREATE TABLE IF NOT EXISTS admin_audit (
    id INTEGER PRIMARY KEY,
    actor_id INTEGER NOT NULL,
    chat_id INTEGER,
    operation TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS admin_audit_created_at ON admin_audit(created_at);
CREATE TABLE IF NOT EXISTS ai_cache (
    cache_key TEXT PRIMARY KEY,
    verdict_json TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ai_budget (
    day TEXT PRIMARY KEY,
    reserved_requests INTEGER NOT NULL DEFAULT 0 CHECK(reserved_requests>=0),
    completed_requests INTEGER NOT NULL DEFAULT 0 CHECK(completed_requests>=0)
);
