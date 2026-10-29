from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from contact_readiness import classify,ContactReadiness
from engine import Store,now


class ContactTests(unittest.TestCase):
    def lead(self, **kw):
        return dict(text='Нужен сайт, пишите @footer_person',sender_id=123,author='@real_author',source_username='external_chat',origin='history',**kw)
    def test_channel_aggregator_username_not_customer(self):
        lead=self.lead(); lead.update(sender_id=-1001489437229,author='@synthetic_job_feed',source_username='synthetic_job_feed')
        self.assertEqual(classify(lead)['kind'],'missing_contact'); self.assertIsNone(classify(lead)['contact_username'])
    def test_body_username_never_promoted(self):
        lead=self.lead(); lead.update(author='Автор не указан')
        self.assertEqual(classify(lead)['kind'],'missing_contact')
    def test_marketplace_task_and_footer_are_different(self):
        lead=self.lead(); lead['text']='Нужен сайт, безопасная сделка Kwork: https://kwork.ru/projects/1234/view. @publisher'
        result=classify(lead); self.assertEqual(result['kind'],'marketplace_task'); self.assertEqual(result['task_url'],'https://kwork.ru/projects/1234/view'); self.assertIsNone(result['contact_username'])
        lead['text']='Работа на Kwork https://kwork.ru/ @publisher'; self.assertEqual(classify(lead)['kind'],'missing_contact')
    def test_real_author_and_manual_dialogue(self):
        self.assertEqual(classify(self.lead())['contact_username'],'real_author')
        lead=self.lead(); lead['origin']='manual_dialogue'; self.assertEqual(classify(lead)['kind'],'contacted_dialogue')
    def test_backfill_preserves_original_and_has_idempotent_history(self):
        with tempfile.TemporaryDirectory() as d:
            owner=Store(Path(d)/'owner.sqlite3'); client=Store(Path(d)/'client.sqlite3')
            for store in (owner,client):
                with store.db:
                    store.db.execute('INSERT INTO sources(id,title,username) VALUES(-1001,?,?)',('Aggregator','synthetic_job_feed'))
                    store.db.execute('INSERT INTO leads(chat_id,message_id,text,author,sender_id,link,created,found,fingerprint,matches,status,origin) VALUES(-1001,1,?,?,?,?,?,?,?,?,?,?)',('Нужен сайт','@synthetic_job_feed',-1001,'https://t.me/synthetic_job_feed/1',now(),now(),'unchanged','[]','working','history'))
            before=owner.rows('SELECT * FROM leads'); client_before='\n'.join(client.db.iterdump())
            review=ContactReadiness(owner); review.initialize(); review.backfill()
            self.assertEqual(owner.rows('SELECT * FROM leads'),before); self.assertEqual('\n'.join(client.db.iterdump()),client_before)
            self.assertEqual(owner.rows('SELECT * FROM owner_contact_readiness')[0]['kind'],'missing_contact')
            self.assertEqual(len(owner.rows('SELECT * FROM owner_contact_review_history')),1)
            with owner.db: owner.db.execute("UPDATE leads SET text='Заказ https://kwork.ru/projects/123/view'")
            review.backfill(); self.assertEqual(len(owner.rows('SELECT * FROM owner_contact_review_history')),2)
            self.assertEqual(owner.rows('SELECT kind FROM owner_contact_readiness')[0]['kind'],'marketplace_task')
            owner.db.close();client.db.close()

    def test_dirty_queue_skips_full_reclassification_and_invalidates_changes(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(Path(d)/'owner.db')
            with store.db:
                store.db.execute("INSERT INTO sources(id,title,username) VALUES(-1,'External','external_chat')")
                store.db.executemany('INSERT INTO leads(chat_id,message_id,text,author,sender_id,created,found,fingerprint,matches) VALUES(-1,?,?,?,?,?,?,?,?)',
                    [(i,'Нужен сайт','@real_author',123,now(),now(),str(i),'[]') for i in range(1000)])
            review=ContactReadiness(store);review.initialize();sql=[]
            store.db.set_trace_callback(sql.append)
            with patch('contact_readiness.classify',wraps=classify) as classifier:
                review.backfill();self.assertEqual(classifier.call_count,0)
                self.assertEqual(len(sql),1,'Unchanged state does not scan all leads')
                with store.db:store.db.execute("UPDATE leads SET text='Работа https://kwork.ru/projects/1/view' WHERE id=1")
                review.backfill();self.assertEqual(classifier.call_count,1)
                self.assertEqual(review.saved([{'id':1}])[1]['kind'],'marketplace_task')
                with store.db:store.db.execute("UPDATE sources SET username='synthetic_job_feed' WHERE id=-1")
                review.backfill();self.assertEqual(classifier.call_count,1001)
                self.assertEqual(review.saved([{'id':2}])[2]['kind'],'missing_contact')
            with store.db:store.db.execute('DELETE FROM leads WHERE id=1')
            self.assertEqual(review.saved([{'id':1}]),{})
            self.assertFalse(store.db.execute('SELECT 1 FROM owner_contact_dirty').fetchone())
            store.db.close()

    def test_failed_backfill_keeps_dirty_work_and_rolls_back_audit(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(Path(d)/'owner.db');review=ContactReadiness(store);review.initialize()
            with store.db:
                store.db.executemany('INSERT INTO leads(chat_id,message_id,text,author,sender_id,created,found,fingerprint,matches) VALUES(-1,?,?,?,?,?,?,?,?)',
                    [(i,'Нужен сайт','@real_author',123,now(),now(),str(i),'[]') for i in (1,2)])
            with patch('contact_readiness.classify',side_effect=[classify(self.lead()),RuntimeError('failure')]):
                with self.assertRaises(RuntimeError):review.backfill()
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM owner_contact_dirty').fetchone()[0],2)
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM owner_contact_review_history').fetchone()[0],0)
            review.backfill();self.assertEqual(store.db.execute('SELECT COUNT(*) FROM owner_contact_readiness').fetchone()[0],2)
            store.db.close()

if __name__=='__main__':unittest.main()
