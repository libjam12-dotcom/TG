import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store,matches,explain
from marketing_topics import seed_marketing

class OwnerProjectScopeTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'owner.db')
  seed_marketing(self.store);self.rules={r['topic']:r for r in self.store.rules()}
 def tearDown(self):self.store.db.close();self.tmp.cleanup()
 def test_any_geography_russian_and_technical_terms(self):
  for text in ['Нужен сайт для бизнеса в Москве','Нужен сайт для компании в Берлине','Ищу подрядчика: нужен сайт для США','Нужен сайт в Дубае']:
   self.assertTrue(matches(text,self.rules['websites']),text)
  self.assertTrue(matches('Нужен SEO',self.rules['seo']))
  self.assertTrue(matches('Кто может настроить CRM и интеграции Shopify?',self.rules['crm']))
 def test_english_and_staff_are_excluded(self):
  for text in ['Need a website developer for a project','Ищем веб-разработчика в штат, удалённо','Вакансия: нужен маркетолог, зарплата 100000','Нужен SMM на проект, оклад 80000 и график 5/2','Нужен SEO. Обязанности: рост трафика. График работы 9–18']:
   self.assertFalse(any(matches(text,r) for r in self.rules.values()),text)
 def test_ads_and_non_russian_cyrillic_are_excluded(self):
  for text in ['Привіт! Хочу запропонувати сайт на розбір, потрібна автоматизація',
               '#помогу Нужен full-stack разработчик под запуск сайта? Я действующий разработчик',
               'Ищу проекты на разработку. Создаю сайты и Telegram-боты',
               'Устали искать заказы? Нужен бот для поиска заказов? Активировать подписку',
               'Нужен сайт или Telegram-бот для бизнеса? Сделаю под ключ.',
               'Разрабатываю: сайты и лендинги, CRM. Нужен сайт? Пишите']:
   self.assertFalse(any(matches(text,r) for r in self.rules.values()),text)
 def test_project_vacancy_and_direct_requests_are_kept(self):
  for text,topic in [('Проектная вакансия: нужен SMM для запуска магазина','smm'),('Ищем маркетолога для запуска нового магазина','marketing'),('Нужен лендинг, разовая работа, срок неделя','websites'),('Посоветуйте таргетолога для проекта','ads')]:
   self.assertTrue(matches(text,self.rules[topic]),text)
 def test_scope_applies_even_when_demand_filter_disabled(self):
  rule=dict(self.rules['websites'],demand_only=False,required=[],excluded=[])
  self.assertEqual(explain('Need a website',rule)['detail'],'other_language')
  self.assertEqual(explain('Нужен сайт, нанимаем в штат',rule)['detail'],'employment')
 def test_client_rules_not_changed(self):
  client=dict(self.rules['websites'],excluded=[]);client.pop('language');client.pop('contractor_only')
  self.assertTrue(matches('Нужен сайт, нанимаем в штат',client))
 def test_migration_preserves_user_edits_and_is_idempotent(self):
  rule=self.rules['websites'];data={k:v for k,v in rule.items() if k not in ('id','name','enabled')}
  data.update(keywords=['особый сайт'],sources=[123])
  with self.store.db:self.store.db.execute('UPDATE rules SET data=? WHERE id=?',(json.dumps(data),rule['id']))
  seed_marketing(self.store);r=self.store.rules()[0]
  self.assertEqual(r['keywords'],['особый сайт']);self.assertEqual(r['sources'],[123])
  self.assertTrue(all('Дубай' not in q and 'Dubai' not in q for r in self.store.rules() for q in r['discovery_queries']))
if __name__=='__main__':unittest.main()
