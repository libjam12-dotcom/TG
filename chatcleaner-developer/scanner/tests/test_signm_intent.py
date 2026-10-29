"""Owner Dubai profile regressions, using synthetic public-request examples."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import explain, matches
from signm_intent import assess_request


class SignMDubaiIntentTests(unittest.TestCase):
    def test_real_ru_and_en_service_requests_have_exact_evidence(self):
        cases = [
            ('websites', 'Друзья, нужен сайт для нашей клиники в Дубае.'),
            ('websites', 'Can anyone recommend a website developer in Dubai?'),
            ('smm', 'Посоветуйте специалиста по ведению Instagram для нашего салона.'),
            ('smm', 'Looking for a social media manager for our Dubai clinic.'),
            ('ads', 'Кто настраивает директ для недвижимости?'),
            ('ads', 'Need a Google Ads specialist for a Dubai restaurant.'),
            ('seo', 'Ищу SEO специалиста для сайта магазина, проектная работа.'),
            ('seo', 'We need an SEO agency for our Dubai online store.'),
            ('bots', 'Нужен Telegram-бот для записи на консультации.'),
            ('bots', 'Looking for a developer to build a Telegram bot for bookings.'),
            ('crm', 'Нужно настроить amoCRM и передачу заявок, разовая задача.'),
            ('crm', 'Can someone integrate our CRM with WhatsApp?'),
            ('automation', 'Хотим автоматизировать передачу заявок в CRM.'),
            ('automation', 'We need business automation with n8n for lead routing.'),
            ('smm', 'Ищу SMM на проект: нужны кейсы. Оплата 3000 AED в месяц.'),
            ('websites', 'Я занимаюсь недвижимостью, нужен сайт для агентства.'),
            ('websites', 'I build houses and need a website for my Dubai business.'),
            ('smm', 'Нужен SMM, не в штат, разовый проект.'),
        ]
        for topic, text in cases:
            with self.subTest(topic=topic, text=text):
                result = assess_request(text, {'topic': topic})
                self.assertEqual(result['label'], 'direct_request', result)
                self.assertTrue(result['evidence'])
                self.assertIn(result['evidence'], text)
                self.assertIn(topic, result['services'])

    def test_provider_rhetorical_pitches_are_not_buyer_requests(self):
        cases = [
            'Кому нужен сайт? Я веб-разработчик, есть свободное окно.',
            'Занимаюсь разработкой под разные задачи. Если нужен сайт, бот или автоматизация — пишите в ЛС.',
            'Я SMM специалист, ищу новые проекты в Дубае. Нужен SMM? Пишите!',
            'Need a website? I build websites for Dubai businesses. DM me.',
            'We offer SEO and Google Ads. Looking for new clients.',
            'Научу SEO: записывайтесь на курс. Нужен сайт? Сделаем на занятиях.',
            'Ищу 2–3 проекта по автоматизации. Я разработчик.',
            'Ищу 2-3 небольших проекта по автоматизации для портфолио.',
            'Ищу один новый проект: нужна CRM или автоматизация?',
            'Looking for 2-3 small projects in SEO and automation.',
        ]
        for text in cases:
            with self.subTest(text=text):
                self.assertEqual(assess_request(text)['label'], 'supplier_offer')

    def test_staff_aggregator_and_unrelated_tool_mentions_are_not_direct(self):
        cases = [
            ('Ищем SMM в штат. Зарплата 8000 AED, полный рабочий день.', 'employment'),
            ('Hiring a full-time SEO specialist in Dubai. Salary 8000 AED.', 'employment'),
            ('Подборка заказов: нужен сайт для салона. Контакт @BuyerOne. Нужен SMM для магазина. @BuyerTwo', 'aggregator'),
            ('Нужны графические дизайнеры. Требования: Figma, Canva, нейросети. Отклик через анкету.', 'unclear'),
            ('В команду интернет-магазина нужен операционный помощник. Таблицы, остатки, статусы заказов.', 'employment'),
            ('Ищу квартиру в Дубае. Также читаю про SEO.', 'unclear'),
            ('Нам больше не нужен сайт, подрядчик уже найден.', 'unclear'),
            ('Подскажите, как самому настроить CRM?', 'unclear'),
            ('How do I set up Google Ads for my business?', 'unclear'),
        ]
        for text, label in cases:
            with self.subTest(text=text):
                self.assertEqual(assess_request(text)['label'], label)

    def test_explicit_availability_offers_and_company_offers_are_not_buyers(self):
        cases = [
            "IN NEED OF A CYBERSECURITY EXPERT❓\nI'm available for all cybersecurity services\nWebsite/Domain Access & Cloning\nSend a DM for more info.",
            "Need a website? I am available for freelance work and SEO projects.",
            "Need an SEO specialist? We're available for SEO services.",
            "Need social media support? I’m available for new SMM projects.",
            'Наша компания предлагает: нужен сайт? Пишите нам.',
        ]
        for text in cases:
            with self.subTest(text=text):
                self.assertEqual(assess_request(text)['label'], 'supplier_offer')

    def test_buyer_availability_to_discuss_and_company_seeking_are_preserved(self):
        cases = [
            "I'm available tomorrow to discuss our project. We need a website developer in Dubai.",
            "I'm available for a meeting to discuss the services we need. Looking for an SEO agency.",
            'Наша компания ищет разработчика: нужен сайт для салона в Дубае.',
        ]
        for text in cases:
            with self.subTest(text=text):
                result = assess_request(text)
                self.assertEqual(result['label'], 'direct_request')
                self.assertIn(result['evidence'], text)

    def test_exact_service_requested_not_service_in_other_clause(self):
        text = 'Ищу SMM для клиники. SEO уже занимается другой подрядчик.'
        self.assertEqual(assess_request(text, {'topic': 'smm'})['label'], 'direct_request')
        self.assertEqual(assess_request(text, {'topic': 'seo'})['label'], 'unclear')

    def test_profile_changes_only_opted_in_rules(self):
        legacy = dict(keywords=['нужен сайт'], required=['нужен'], excluded=[],
                      demand_only=True, topic='websites', language='ru', contractor_only=True)
        text = 'Кому нужен сайт? Я веб-разработчик, есть свободное окно.'
        self.assertTrue(matches(text, legacy))  # Reproduces the previous false positive.
        scoped = {**legacy, 'lead_profile': 'signm_dubai_v1', 'language': 'ru_en'}
        result = explain(text, scoped)
        self.assertEqual(result['reason'], 'excluded')
        self.assertEqual(result['detail'], 'supplier_offer')
        client = dict(keywords=['smm'], required=['нужен'], excluded=[], demand_only=False)
        self.assertTrue(matches('Нужен SMM в штат', client))

    def test_en_profile_buyer_and_ru_profile_keeps_old_language_policy(self):
        rule = dict(keywords=['website developer'], required=['recommend'], excluded=[],
                    topic='websites', demand_only=True, contractor_only=True, language='ru')
        text = 'Can anyone recommend a website developer in Dubai?'
        self.assertEqual(explain(text, rule)['detail'], 'other_language')
        self.assertTrue(matches(text, {**rule, 'lead_profile': 'signm_dubai_v1', 'language': 'ru_en'}))


if __name__ == '__main__':
    unittest.main()
