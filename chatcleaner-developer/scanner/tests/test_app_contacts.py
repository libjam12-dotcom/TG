import json, os, sys, tempfile, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import Store, now

class ClientContactStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        os.environ.setdefault('SCANNER_DATA', self.tmp.name)
        import app
        self.app=app
        self.store=Store(Path(self.tmp.name)/'client.sqlite3')
        self.runtime=app.Runtime()
        self.runtime.me=SimpleNamespace(id=42,username='client_account')
        self.runtime.client=SimpleNamespace(is_connected=lambda: True)
        self.runtime.status='connected'
        self.patch_store=patch.object(app,'store',self.store);self.patch_store.start()
        self.patch_runtime=patch.object(app,'runtime',self.runtime);self.patch_runtime.start()
        self.store.put('running',True)
        with self.store.db:
            self.store.db.execute("INSERT INTO sources(id,title) VALUES(-1001,'Источник')")
            self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('DJ',json.dumps({'sources':[-1001],'keywords':['диджея'],'required':['ищу']})))
        self.app.conversations.initialize()
        self.app.bot_notifications.initialize()
    def tearDown(self):
        self.patch_runtime.stop();self.patch_store.stop();self.store.db.close();self.tmp.cleanup()
    def lead(self, sender=123, text='Ищу диджея на субботу'):
        return self.store.ingest(-1001,1,text,'Автор',sender)
    async def snapshot(self):
        return await self.app.state('all',0,0,0)
    async def test_channel_candidate_has_no_personal_contact(self):
        self.lead(-100200)
        lead=(await self.snapshot())['leads'][0]
        self.assertEqual(lead['client_contact']['status'],'no_contact')
        self.assertFalse(lead['client_contact']['ready'])
    async def test_personal_id_does_not_become_ready_lead(self):
        self.lead()
        contact=(await self.snapshot())['leads'][0]['client_contact']
        self.assertEqual(contact['status'],'needs_review')
        self.assertFalse(contact['author_verified'])
        self.assertFalse(contact['customer_verified'])
    async def test_telegram_forward_metadata_is_preserved_and_reported(self):
        lid=self.lead()
        self.runtime.capture_contact_evidence(lid,SimpleNamespace(fwd_from=object()),SimpleNamespace(bot=False,deleted=False))
        contact=(await self.snapshot())['leads'][0]['client_contact']
        self.assertIn('forwarded_message',contact['reason_codes'])
        self.assertEqual(len(self.store.rows('SELECT * FROM leads')),1)
    async def test_only_matching_client_conversation_confirms_author(self):
        lid=self.lead()
        with self.store.db:
            cursor=self.store.db.execute('INSERT INTO conversations(account_id,peer_id,access_hash,title,lead_id,rule_id,last_sync,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',(42,123,'1','Автор',lid,1,now(),now(),now()))
            self.store.db.execute('INSERT INTO conversation_leads VALUES(?,?)',(cursor.lastrowid,lid))
        contact=(await self.snapshot())['leads'][0]['client_contact']
        self.assertEqual(contact['status'],'confirmed_author')
        self.assertFalse(contact['customer_verified'])
        self.runtime.me.id=99
        self.assertEqual((await self.snapshot())['leads'][0]['client_contact']['status'],'needs_review')

if __name__=='__main__': unittest.main()
