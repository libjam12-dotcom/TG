from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sound_topic import sound_rule
from engine import matches

POSITIVE=[
 'Нужна аппаратура на мероприятие в Дубае',
 'Ищу колонки на вечеринку',
 'Нужен звук на яхту в субботу',
 'Подскажите, где арендовать звуковое оборудование на свадьбу',
 'Ищу аренду колонок и микрофона на день рождения',
 'Нужны колонки, два микрофона и микшерный пульт',
 'У кого есть аппаратура для концерта?',
 'Посоветуйте прокат акустических систем в Дубае',
 'Хочу арендовать сабвуфер и колонки для мероприятия',
 'Нужна озвучка корпоративного вечера',
 'Сколько стоит звук для свадьбы на 50 гостей?',
 'Ищу DJ оборудование в аренду для вечеринки',
 'Ищу пульт и аппаратуру для диджея',
 'Нужно взять напрокат микрофоны на конференцию',
 'Looking for sound equipment for a Dubai yacht party',
 'Need speakers for a birthday party',
 'Looking to rent a PA system for our event',
 'Can anyone recommend sound system rental in Dubai?',
 'Need two microphones and an audio mixer tomorrow',
 'Looking for DJ equipment rental',
 'Where can I hire speakers for a wedding?',
 'PA system needed for our corporate event',
]
NEGATIVE=[
 'Сдаем колонки и аппаратуру, пишите в личку',
 'Нужен звук? Предлагаем аренду аппаратуры',
 'Колонки в аренду, доставка по Дубаю',
 'Сдам звук на мероприятие, есть свободные даты',
 'Sound equipment available for rent, contact us',
 'Need speakers? We offer sound system rental',
 'Looking for work, experienced sound equipment technician',
 'Ищу работу, настраиваю звук и колонки',
 'Требуется техник по звуковому оборудованию в штат',
 'Вакансия: нужен специалист, обслуживать звук',
 'Ищу газовую колонку для квартиры',
 'Нужна медицинская аппаратура для больницы',
 'Подскажите, как включить звук на ноутбуке',
 'Нужна помощь, звук не работает на телефоне',
 'Нужен ремонт колонок',
 'Looking for computer speakers',
 'Need car speakers',
 'Looking for keynote speakers for a conference',
 'Вчера был отличный звук, колонки впечатлили',
 'Ищу фотографа. На площадке уже есть звук и колонки.',
 'Нужен диджей на свадьбу',
 'Sound system for sale, DM me',
]

class SoundTests(unittest.TestCase):
    def test_customer_requests(self):
        for text in POSITIVE:
            with self.subTest(text=text):self.assertTrue(matches(text,sound_rule()))
    def test_offers_jobs_unrelated_context(self):
        for text in NEGATIVE:
            with self.subTest(text=text):self.assertFalse(matches(text,sound_rule()))
    def test_sources_unique(self):
        self.assertEqual(sound_rule([-1,-1,-2])['sources'],[-1,-2])
