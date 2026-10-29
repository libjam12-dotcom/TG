"""Pure summaries of saved direct-invite rows; no database or Telegram access.

The caller selects only invite jobs. ``accounts`` maps operation-account keys to
``{enabled: bool | 0 | 1, connected: bool, telegram_id: int}`` snapshots. Queue
activity describes that snapshot, not a promise that a pending invite will work.

Verified additions count unique users per group. Other state totals and the
three request counters count journal rows, never transport/RPC attempts: the
transport can retry internally. A dated legacy row without a boolean request
marker remains unknown instead of being treated as a sent request.
"""
from collections.abc import Mapping
from datetime import date, datetime, timezone
import json
import re


def _utc_day(value):
    """Only an explicitly zoned timestamp can establish a UTC day."""
    try:
        timestamp = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            return None
        return timestamp.astimezone(timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def _diagnosis(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, Mapping) else {}


def _identity(value):
    return type(value) is int and value > 0


def summarize_invites(rows, day_utc, accounts):
    """Return a JSON-safe summary of all saved invite jobs for one UTC day.

``groups`` is ordered by numeric ``chat_id``. Each group exposes
``verified_added_total``, ``verified_added_today``, ``already_member_total``,
``uncertain_total``, ``pending_total``, ``pending_active``, ``pending_paused``,
``pending_draft``, ``pending_other`` and ``pending_binding_mismatch``.
The four pending buckets are disjoint; binding mismatch is an extra subset.
Top-level ``*_rows_today`` counters use attempted_at; verified today uses
finished_at. Missing, malformed or timezone-free timestamps are never today.
"""
    if (not isinstance(day_utc, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day_utc)):
        raise ValueError('day_utc must be a YYYY-MM-DD UTC date')
    try:
        date.fromisoformat(day_utc)
    except ValueError:
        raise ValueError('day_utc must be a YYYY-MM-DD UTC date') from None
    if not isinstance(accounts, Mapping):
        raise ValueError('accounts must map account keys to current snapshots')

    result = {
        'day_utc': day_utc,
        'scope': 'all_invite_jobs',
        'groups': [],
        'recorded_request_rows_today': 0,
        'legacy_unknown_request_rows_today': 0,
        'preflights_today': 0,
    }
    groups = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        diagnosis = _diagnosis(row.get('diagnosis'))
        if _utc_day(row.get('attempted_at')) == day_utc:
            requested = diagnosis.get('invitation_requested')
            counter = ('recorded_request_rows_today' if requested is True else
                       'preflights_today' if requested is False else
                       'legacy_unknown_request_rows_today')
            result[counter] += 1

        chat_id = row.get('chat_id')
        if type(chat_id) is not int or chat_id == 0:
            continue
        if chat_id not in groups:
            groups[chat_id] = {
                'chat_id': chat_id, 'chat_title': '',
                'verified_added_total': 0, 'verified_added_today': 0,
                'already_member_total': 0, 'uncertain_total': 0,
                'pending_total': 0, 'pending_active': 0,
                'pending_paused': 0, 'pending_draft': 0, 'pending_other': 0,
                'pending_binding_mismatch': 0,
                '_verified_users': set(), '_verified_users_today': set(),
            }
        group = groups[chat_id]
        if not group['chat_title'] and isinstance(row.get('chat_title'), str):
            group['chat_title'] = row['chat_title'].strip()
        state = row.get('state')
        user_id = row.get('user_id')
        if state == 'invited' and diagnosis.get('code') == 'invite_member_verified' and _identity(user_id):
            group['_verified_users'].add(user_id)
            if _utc_day(row.get('finished_at')) == day_utc:
                group['_verified_users_today'].add(user_id)
        elif state == 'already_member':
            group['already_member_total'] += 1
        elif state == 'uncertain':
            group['uncertain_total'] += 1
        elif state == 'pending':
            group['pending_total'] += 1
            account = accounts.get(row.get('account'))
            account = account if isinstance(account, Mapping) else {}
            bound = (_identity(row.get('telegram_id')) and
                     _identity(account.get('telegram_id')) and
                     row['telegram_id'] == account['telegram_id'])
            if not bound:
                group['pending_binding_mismatch'] += 1
            enabled = account.get('enabled')
            ready = (bound and (enabled is True or type(enabled) is int and enabled == 1)
                     and account.get('connected') is True)
            parent = row.get('job_state')
            bucket = ('pending_paused' if parent == 'paused' else
                      'pending_draft' if parent == 'draft' else
                      'pending_active' if parent in ('queued', 'scheduled') and ready else
                      'pending_other')
            group[bucket] += 1

    for chat_id in sorted(groups):
        group = groups[chat_id]
        group['verified_added_total'] = len(group.pop('_verified_users'))
        group['verified_added_today'] = len(group.pop('_verified_users_today'))
        result['groups'].append(group)
    return result
