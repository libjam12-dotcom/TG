import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import HTTPException
from telethon import errors, functions, types
from operation_tracking import OperationTracking, TrackingInbox
from operation_redirects import OperationRedirects, RedirectSettings, BioUpdate
from test_operation_tracking import Ops, Client


TEXT = 'По всем вопросам о билетах на UFC 333 пишите @demo_identity_4.'


class RedirectClient(Client):
    def __init__(self, tid):
        super().__init__()
        self.tid, self.about = tid, ''
        self.bio_writes, self.profile_reads, self.profile_failure = [], 0, None

    async def __call__(self, req):
        if isinstance(req, functions.account.UpdateProfileRequest):
            self.bio_writes.append(req)
            if self.failure:
                raise self.failure
            self.about = req.about
            return types.User(id=self.tid)
        if isinstance(req, functions.users.GetFullUserRequest):
            self.profile_reads += 1
            if self.profile_failure:
                raise self.profile_failure
            return SimpleNamespace(full_user=SimpleNamespace(id=self.tid, about=self.about))
        return await super().__call__(req)


class RedirectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ops = Ops(Path(self.tmp.name) / 'redirect.sqlite3')
        self.c = self.ops.accounts['a'].client = RedirectClient(100)
        self.ops.tracking = OperationTracking(self.ops)
        self.ops.tracking.initialize()
        self.redirects = OperationRedirects(self.ops)
        self.redirects.initialize()
        await self.ops.tracking.set_inbox('a', TrackingInbox(telegram_id=100, enabled=True))

    async def asyncTearDown(self):
        self.ops.tracking.unbind_clients()
        self.ops.store.db.close()
        self.tmp.cleanup()

    async def enable(self, **kwargs):
        return await self.redirects.set_settings('a', RedirectSettings(enabled=True, telegram_id=100, text=TEXT, **kwargs))

    def event(self, mid=1, sender=555, **kwargs):
        msg = SimpleNamespace(id=mid, sender_id=sender, out=False, message='Билеты?',
                              date=datetime.now(timezone.utc), action=None, via_bot_id=None)
        return SimpleNamespace(chat_id=sender, is_private=True, message=msg,
                               sender=types.User(id=sender), **kwargs)

    async def incoming(self, event=None, aid='a', tid=100, client=None):
        event = event or self.event()
        client = client or self.c
        await self.ops.tracking.on_new_message(aid, tid, client, event)
        saved = self.ops.store.rows('SELECT * FROM op_tracking_events WHERE event_key=?',
                                   (f'incoming:{aid}:{tid}:{event.chat_id}:{event.message.id}',))
        if saved:
            await self.redirects.on_new_dm(aid, tid, client, event, saved[0])
        return saved[0] if saved else None

    def claims(self):
        return self.ops.store.rows('SELECT * FROM op_dm_redirect_claims ORDER BY id')

    def bio(self, **kwargs):
        return BioUpdate(telegram_id=100, about='Билеты UFC 333: @demo_identity_4',
                         request_key='bio-update-unique-0001', confirmed=True, **kwargs)

    async def test_off_by_default_and_no_old_history_replay_after_enable(self):
        event = self.event()
        await self.incoming(event)
        await self.enable()
        await self.incoming(event)
        self.assertEqual([], self.c.calls)
        self.assertEqual([], self.claims())

    async def test_new_dm_sends_one_canned_reply_and_journal_is_explicit(self):
        await self.enable()
        saved = await self.incoming()
        self.assertEqual(1, len(self.c.calls))
        self.assertEqual(TEXT, self.c.calls[0].message)
        self.assertEqual(1, self.c.calls[0].reply_to.reply_to_msg_id)
        self.assertEqual('sent', self.claims()[0]['state'])
        rows = self.ops.store.rows("SELECT * FROM op_tracking_events WHERE kind LIKE 'reply_%'")
        self.assertEqual(['reply_requested', 'reply_sent'], [r['kind'] for r in rows])
        self.assertTrue(all(r['source'] == 'dm_redirect' and r['parent_event_id'] == saved['id'] for r in rows))

    async def test_duplicate_update_concurrent_and_new_message_same_sender_send_once(self):
        await self.enable()
        event = self.event()
        await asyncio.gather(self.incoming(event), self.incoming(event), self.incoming(self.event(mid=2)))
        self.assertEqual(1, len(self.c.calls))
        self.assertEqual(1, len(self.claims()))

    async def test_sender_slot_expires_after_24h_but_same_event_never_replays(self):
        await self.enable()
        event = self.event()
        await self.incoming(event)
        with self.ops.store.db:
            self.ops.store.db.execute('UPDATE op_dm_redirect_claims SET created_at=?', (time.time() - 86401,))
        await self.incoming(event)
        self.assertEqual(1, len(self.c.calls))
        await self.incoming(self.event(mid=2))
        self.assertEqual(2, len(self.c.calls))

    async def test_cap_counts_attempts_in_rolling_24h(self):
        await self.enable(daily_limit=1)
        await self.incoming()
        await self.incoming(self.event(mid=2, sender=556))
        self.assertEqual(1, len(self.c.calls))
        self.assertEqual(1, (await self.redirects.settings('a'))['attempts_last_24h'])

    async def test_bots_service_self_outgoing_and_nonprivate_are_ignored(self):
        await self.enable()
        events = [self.event(mid=i, sender=600+i) for i in range(1, 7)]
        events[0].sender.bot = True
        events[1].sender.support = True
        events[2].message.action = object()
        events[3].message.out = True
        events[4].is_private = False
        events[5].message.via_bot_id = 123
        for event in events + [self.event(mid=99, sender=100), self.event(mid=98, sender=777000)]:
            await self.incoming(event)
        self.assertEqual([], self.c.calls)

    async def test_fails_closed_when_sender_cannot_be_verified(self):
        await self.enable()
        event = self.event()
        event.sender = SimpleNamespace(id=555, bot=False)
        await self.incoming(event)
        self.assertEqual([], self.c.calls)

    async def test_identity_switch_or_other_account_cannot_inherit_setting(self):
        await self.enable()
        await self.ops.tracking.set_inbox('b', TrackingInbox(telegram_id=200, enabled=True))
        await self.incoming(aid='b', tid=200, client=self.ops.accounts['b'].client)
        self.ops.accounts['a'].me.id = 101
        await self.incoming(self.event(mid=2))
        with self.assertRaises(HTTPException):
            await self.redirects.set_settings('a', RedirectSettings(enabled=True, telegram_id=101, text=TEXT))
        self.assertEqual([], self.c.calls)
        self.assertEqual([], self.ops.accounts['b'].client.calls)

    async def test_disable_inbox_paused_account_and_cooldown_do_not_send(self):
        await self.enable()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0 WHERE id='a'")
        await self.incoming()
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=1,not_before=? WHERE id='a'", (time.time()+60,))
        await self.incoming(self.event(mid=2))
        await self.ops.tracking.set_inbox('a', TrackingInbox(telegram_id=100, enabled=False))
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET not_before=0 WHERE id='a'")
        await self.incoming(self.event(mid=3))
        self.assertEqual([], self.c.calls)

    async def test_enable_requires_explicit_inbox_and_does_not_enable_it(self):
        await self.ops.tracking.set_inbox('a', TrackingInbox(telegram_id=100, enabled=False))
        with self.assertRaises(HTTPException):
            await self.enable()
        self.assertFalse(self.ops.tracking._inbox_enabled('a', 100))

    async def test_unknown_send_pauses_redirect_and_is_never_replayed(self):
        await self.enable()
        self.c.receipt_missing = True
        event = self.event()
        await self.incoming(event)
        self.assertEqual('unknown', self.claims()[0]['state'])
        self.assertFalse((await self.redirects.settings('a'))['enabled'])
        self.c.receipt_missing = False
        await self.enable()
        await self.incoming(event)
        await self.incoming(self.event(mid=2))
        self.assertEqual(1, len(self.c.calls))

    async def test_flood_pauses_only_selected_account_and_redirect(self):
        await self.enable()
        self.c.failure = errors.FloodWaitError(None, 15)
        await self.incoming()
        self.assertEqual([('a', 'FloodWaitError')], self.ops.pauses)
        self.assertFalse((await self.redirects.settings('a'))['enabled'])
        self.assertTrue(self.ops.account_row('b')['enabled'])
        self.assertGreater(self.ops.account_row('a')['not_before'], time.time())

    async def test_flood_while_resolving_before_reply_claim_still_pauses_account(self):
        await self.enable()
        async def blocked(chat):
            raise errors.FloodWaitError(None, 12)
        self.c.get_input_entity = blocked
        await self.incoming()
        self.assertEqual([], self.c.calls)
        self.assertEqual([('a', 'FloodWaitError')], self.ops.pauses)
        self.assertEqual('unknown', self.claims()[0]['state'])
        self.assertFalse((await self.redirects.settings('a'))['enabled'])

    async def test_forged_saved_row_identity_cannot_select_another_event(self):
        await self.enable()
        event = self.event()
        await self.ops.tracking.on_new_message('a', 100, self.c, event)
        saved = self.ops.store.rows("SELECT * FROM op_tracking_events WHERE kind='incoming_dm'")[0]
        event.message.id += 1
        await self.redirects.on_new_dm('a', 100, self.c, event, saved)
        self.assertEqual([], self.c.calls)
        self.assertEqual([], self.claims())

    async def test_claim_interrupted_at_restart_never_replays(self):
        await self.enable()
        event = self.event()
        saved = await self.incoming(event)
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_dm_redirect_claims SET state='claimed'")
        self.redirects.initialize()
        self.assertEqual('unknown', self.claims()[0]['state'])
        self.assertFalse((await self.redirects.settings('a'))['enabled'])
        await self.enable()
        await self.redirects.on_new_dm('a', 100, self.c, event, saved)
        self.assertEqual(1, len(self.c.calls))

    async def test_manual_reply_prevents_automatic_second_answer(self):
        await self.enable()
        event = self.event()
        await self.ops.tracking.on_new_message('a', 100, self.c, event)
        saved = self.ops.store.rows("SELECT * FROM op_tracking_events WHERE kind='incoming_dm'")[0]
        from operation_tracking import TrackingReply
        await self.ops.tracking._send_reply(TrackingReply(event_id=saved['id'], text='Ручной ответ', request_key='manual-reply-unique-0001', confirmed=True), jid=None, aid='a')
        await self.redirects.on_new_dm('a', 100, self.c, event, saved)
        self.assertEqual(1, len(self.c.calls))
        self.assertEqual([], self.claims())

    async def test_bio_exact_identity_confirmed_and_idempotent(self):
        result = await self.redirects.update_bio('a', self.bio())
        self.assertEqual('confirmed', result['state'])
        self.assertEqual(result, await self.redirects.update_bio('a', self.bio()))
        self.assertEqual(1, len(self.c.bio_writes))
        self.assertEqual('Билеты UFC 333: @demo_identity_4', (await self.redirects.profile('a'))['about'])
        self.assertEqual('', self.ops.accounts['b'].client.__dict__.get('about', ''))

    async def test_bio_rejects_wrong_identity_primary_or_key_reuse(self):
        spec = self.bio()
        spec.telegram_id = 999
        with self.assertRaises(HTTPException):
            await self.redirects.update_bio('a', spec)
        await self.redirects.update_bio('a', self.bio())
        spec = self.bio()
        spec.about = 'Другой текст'
        with self.assertRaises(HTTPException):
            await self.redirects.update_bio('a', spec)
        with self.ops.store.db:
            self.ops.store.db.execute("INSERT INTO op_accounts VALUES('primary','p',999,'p',1,0,NULL)")
        with self.assertRaises(HTTPException):
            await self.redirects.update_bio('primary', BioUpdate(telegram_id=999, about='text', request_key='primary-guard-key-0001', confirmed=True))

    async def test_bio_update_unknown_verification_failure_never_replays(self):
        self.c.profile_failure = errors.FloodWaitError(None, 10)
        result = await self.redirects.update_bio('a', self.bio())
        self.assertEqual('unknown', result['state'])
        self.c.profile_failure = None
        self.assertEqual(result, await self.redirects.update_bio('a', self.bio()))
        self.assertEqual(1, len(self.c.bio_writes))

    async def test_bio_process_recovery_unknown_is_not_retried(self):
        await self.redirects.update_bio('a', self.bio())
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_profile_updates SET state='sending'")
        self.redirects.initialize()
        result = await self.redirects.update_bio('a', self.bio())
        self.assertEqual('unknown', result['state'])
        self.assertEqual(1, len(self.c.bio_writes))

    def test_unicode_limits_and_empty_text_are_validated(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            RedirectSettings(enabled=True, telegram_id=100, text='   ')
        with self.assertRaises(ValidationError):
            BioUpdate(telegram_id=100, about='😀'*36, request_key='bio-utf16-limit-key', confirmed=True)
        with self.assertRaises(ValidationError):
            RedirectSettings(enabled=True, telegram_id=100, text='ok', daily_limit=21)


if __name__ == '__main__':
    unittest.main()
