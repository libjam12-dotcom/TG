"""Pure saved-journal fixtures. No database, client, network or scheduler."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from invite_progress import summarize_invites


DAY = '2026-10-21'
AT = DAY + 'T04:00:00+00:00'
ACCOUNTS = {'a': {'enabled': 1, 'connected': True, 'telegram_id': 100},
            'b': {'enabled': True, 'connected': True, 'telegram_id': 200}}


def saved(**values):
    row = dict(chat_id=-1001, chat_title='Group', user_id=1, state='invited',
               attempted_at=AT, finished_at=AT,
               diagnosis={'code': 'invite_member_verified', 'invitation_requested': True},
               account='a', telegram_id=100, job_state='completed')
    row.update(values)
    return row


class InviteProgressTests(unittest.TestCase):
    def summary(self, rows, accounts=ACCOUNTS):
        return summarize_invites(rows, DAY, accounts)

    def test_unique_verified_users_deduplicate_retries_and_accounts_per_group(self):
        rows = [saved(), saved(account='b', telegram_id=200), saved(user_id=2),
                saved(chat_id=-1002), saved(chat_id=-1002, user_id=2)]
        result = self.summary(rows)
        self.assertEqual([g['chat_id'] for g in result['groups']], [-1002, -1001])
        self.assertEqual([g['verified_added_total'] for g in result['groups']], [2, 2])
        self.assertEqual(result['recorded_request_rows_today'], 5, 'journal rows are not unique-user or RPC counts')

    def test_already_member_uncertain_and_unproved_legacy_are_not_new_additions(self):
        rows = [saved(state='already_member'), saved(state='uncertain'),
                saved(diagnosis={}), saved(diagnosis='malformed'),
                saved(diagnosis={'code': 'other'}), saved(user_id=None)]
        group = self.summary(rows)['groups'][0]
        self.assertEqual(group['verified_added_total'], 0)
        self.assertEqual(group['already_member_total'], 1)
        self.assertEqual(group['uncertain_total'], 1)

    def test_today_uses_finished_utc_day_not_attempted_or_created(self):
        rows = [saved(user_id=1, attempted_at='2026-10-20T12:00:00Z', finished_at='2026-10-21T00:30:00+02:00'),
                saved(user_id=2, attempted_at='2026-10-20T12:00:00Z', finished_at='2026-10-20T23:30:00-02:00'),
                saved(user_id=3, finished_at=None, created=AT),
                saved(user_id=4, finished_at='2026-10-21T04:00:00'),
                saved(user_id=5, finished_at='broken')]
        group = self.summary(rows)['groups'][0]
        self.assertEqual(group['verified_added_total'], 5)
        self.assertEqual(group['verified_added_today'], 1)

    def test_duplicate_with_one_valid_finished_date_is_one_today(self):
        group = self.summary([saved(finished_at=None), saved(), saved()])['groups'][0]
        self.assertEqual((group['verified_added_total'], group['verified_added_today']), (1, 1))

    def test_request_markers_are_boolean_and_count_attempted_rows_in_utc(self):
        rows = [saved(diagnosis=json.dumps({'invitation_requested': True})),
                saved(diagnosis={'invitation_requested': False}),
                saved(diagnosis={}), saved(diagnosis={'invitation_requested': 'true'}),
                saved(diagnosis={'invitation_requested': 1}), saved(diagnosis='[]'),
                saved(attempted_at='2026-10-21T00:30:00+02:00'),
                saved(attempted_at='2026-10-20T23:30:00-02:00'),
                saved(attempted_at='2026-10-21T01:00:00'), saved(attempted_at=None)]
        result = self.summary(rows)
        self.assertEqual(result['recorded_request_rows_today'], 2)
        self.assertEqual(result['preflights_today'], 1)
        self.assertEqual(result['legacy_unknown_request_rows_today'], 4)

    def test_only_live_bound_enabled_connected_queued_or_scheduled_is_active(self):
        rows = [saved(state='pending', job_state='queued'), saved(state='pending', job_state='scheduled'),
                saved(state='pending', job_state='paused'), saved(state='pending', job_state='draft'),
                saved(state='pending', job_state='queued', account='missing'),
                saved(state='pending', job_state='queued', account='b', telegram_id=100),
                saved(state='pending', job_state='cancelled')]
        group = self.summary(rows)['groups'][0]
        self.assertEqual(group['pending_total'], 7)
        self.assertEqual(group['pending_active'], 2)
        self.assertEqual(group['pending_paused'], 1)
        self.assertEqual(group['pending_draft'], 1)
        self.assertEqual(group['pending_other'], 3)
        self.assertEqual(group['pending_binding_mismatch'], 2)
        self.assertEqual(sum(group[k] for k in ('pending_active', 'pending_paused', 'pending_draft', 'pending_other')), 7)

    def test_disabled_disconnected_or_string_truthiness_is_not_active(self):
        for change in ({'enabled': 0}, {'connected': False}, {'enabled': '1'}, {'connected': 'true'},
                       {'telegram_id': '100'}, {'telegram_id': True}):
            with self.subTest(change=change):
                accounts = copy.deepcopy(ACCOUNTS);accounts['a'].update(change)
                group = self.summary([saved(state='pending', job_state='queued')], accounts)['groups'][0]
                self.assertEqual(group['pending_active'], 0)
                self.assertEqual(group['pending_other'], 1)

    def test_paused_binding_mismatch_stays_paused_and_separately_marked(self):
        group = self.summary([saved(state='pending', job_state='paused', telegram_id=999)])['groups'][0]
        self.assertEqual(group['pending_paused'], 1)
        self.assertEqual(group['pending_binding_mismatch'], 1)
        self.assertEqual(group['pending_active'], 0)

    def test_empty_and_json_serializable_output_and_no_input_mutation(self):
        self.assertEqual(self.summary([])['groups'], [])
        rows = [saved(diagnosis={'code': 'invite_member_verified', 'invitation_requested': True})]
        before = copy.deepcopy((rows, ACCOUNTS))
        result = self.summary(rows)
        self.assertEqual((rows, ACCOUNTS), before)
        self.assertEqual(json.loads(json.dumps(result)), result)
        self.assertEqual(result['scope'], 'all_invite_jobs')
        self.assertEqual(result['day_utc'], DAY)

    def test_no_success_for_noninteger_ids_or_nested_proof(self):
        rows = [saved(user_id=True), saved(user_id='1'), saved(user_id=0),
                saved(diagnosis={'verification': {'code': 'invite_member_verified'}})]
        self.assertEqual(self.summary(rows)['groups'][0]['verified_added_total'], 0)

    def test_invalid_day_or_account_snapshot_shape_is_rejected(self):
        for day in ('2026-2-1', '2026-02-30', AT, None):
            with self.subTest(day=day), self.assertRaises(ValueError):
                summarize_invites([], day, ACCOUNTS)
        with self.assertRaises(ValueError):
            summarize_invites([], DAY, [])


if __name__ == '__main__':
    unittest.main()
