from fixture_manual import ManualFixture, SENDER, TARGET
from manual_invite_campaign import QuickDraftRequest, QuickStartRequest


class ManualInviteHistoryTests(ManualFixture):
    async def test_previous_link_does_not_block_direct_invite_but_previous_direct_attempts_do(self):
        with self.ops.store.db as db:
            db.execute("UPDATE op_accounts SET error=NULL WHERE id='primary'")
            history = [('invite_message', 'sent'), ('invite', 'restricted'), ('invite', 'uncertain')]
            timestamp = '2020-01-01T00:00:00+00:00'
            for user, (kind, state) in zip(self.client.users, history):
                job_id = db.execute(
                    'INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,created,updated) '
                    'VALUES(?,?,?,?,?,?,?,?,?,?)',
                    ('history-' + user.username, 'history', 'primary', SENDER, kind, 'Prior operation', '{}',
                     'completed', timestamp, timestamp),
                ).lastrowid
                db.execute(
                    'INSERT INTO op_items(job,chat_id,chat_title,user_id,user_name,state,random_ids,attempted_at) '
                    'VALUES(?,?,?,?,?,?,?,?)',
                    (job_id, TARGET, self.client.group.title, user.id, user.username, state, '[]', timestamp),
                )
                if state == 'uncertain':
                    db.execute('INSERT INTO op_invited VALUES(?,?,?,?)', (TARGET, user.id, state, job_id))

        saved = await self.manual.quick_draft(
            QuickDraftRequest(config=self.config(len(history)), request_key='history-draft-001'))
        await self.manual.quick_start(QuickStartRequest(
            confirmed=True, job_id=saved['job_id'], config_digest=saved['config_digest'],
            request_key='history-start-001'))
        await self.run_worker()

        result = await self.manual.status(saved['job_id'])
        self.assertEqual([item['state'] for item in result['items']],
                         ['invited', 'duplicate_skipped', 'duplicate_skipped'])
        self.assertEqual(result['counts']['attempts'], 1)
        self.assertEqual(result['counts']['added'], 1)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.client.calls[0].users[0].id, self.client.users[0].id)
        for item in result['items'][1:]:
            self.assertEqual(item['diagnosis']['code'], 'target_history_exists')
            self.assertFalse(item['diagnosis']['invitation_requested'])
