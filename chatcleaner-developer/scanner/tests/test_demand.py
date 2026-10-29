from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store, explain, matches
from topics import seed_topics

class DemandTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'test.db')
        seed_topics(self.store)
        self.rules={r['topic']:{**r,'demand_only':True} for r in self.store.rules()}

    def tearDown(self):
        self.store.db.close(); self.tmp.cleanup()

    def test_audit_false_positives(self):
        cases=[
          ('hotels','Кто может обменять 100 долларов? Нахожусь у Ramee dream hotel.','unrelated_intent'),
          ('smm','Предлагаю SMM, кому нужно продвижение?','supplier_offer'),
          ('tickets','Продаю билеты, кому нужны?','supplier_offer'),
          ('visas','Ищу сотрудников. Виза предоставляется.','existing_document'),
          ('hotels','Ищу управляющего в отель. Зарплата 5000.','employment'),
          ('transfers','Ищу работу водителя, свой автомобиль','job_seeker'),
          ('emirates_id','Ищу работу. Есть Emirates ID.','job_seeker'),
          ('safari','Ищу сафари в Кении','other_geography'),
          ('smm','Вакансия: нужен SMM в штат','employment'),
        ]
        for topic,text,reason in cases:
            with self.subTest(text=text):
                result=explain(text,self.rules[topic]); self.assertNotEqual(result['reason'],'matched');self.assertEqual(result['detail'],reason)

    def test_valid_requests_are_preserved(self):
        for topic,text in [
            ('hotels','Посоветуйте отель со скидкой'),('hotels','Кто может забронировать отель на завтра?'),
            ('smm','Ищу SMM специалиста для бизнеса'),('yacht','Я не предлагаю услуги, я ищу яхту на завтра'),
            ('tickets','Куплю билеты на концерт'),('cars','Нужно арендовать машину на три дня'),
            ('transfers','Нужен водитель на один день'),('visas','Кто поможет с визой в ОАЭ'),
            ('emirates_id','Нужно оформить Emirates ID'),('safari','Ищу сафари в Дубае'),
            ('yacht','Ищу\nяхту на день рождения'),('safari','Были в Кении, теперь ищу сафари в Дубае')]:
            with self.subTest(text=text): self.assertTrue(matches(text,self.rules[topic]))

    def test_custom_rules_keep_old_behavior_until_opted_in(self):
        rule={**self.rules['hotels'],'demand_only':False}
        self.assertTrue(matches('Кто может обменять деньги? Я у hotel.',rule))
