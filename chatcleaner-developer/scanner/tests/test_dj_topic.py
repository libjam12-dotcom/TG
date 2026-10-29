from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dj_topic import dj_rule
from engine import matches

POSITIVE = [
 'Ищу диджея на яхту в Дубае на субботу',
 'Ищу хорошего диджея для вечеринки',
 'Нужен DJ на день рождения, 20 гостей',
 'Посоветуйте диджея на свадьбу',
 'Порекомендуйте русскоговорящего диджея в Дубае',
 'Подскажите контакт DJ на корпоратив',
 'Кто знает диджея для мероприятия?',
 'Нужна диджейка на девичник',
 'Ищем диджей-девушку на яхту',
 'Ищу ди-джея на праздник',
 'Нужны услуги диджея для свадьбы',
 'Сколько стоит пригласить диджея на 4 часа?',
 'Есть ли свободные диджеи на завтра?',
 'У кого есть контакты диджеев?',
 'Требуется диджей на разовое мероприятие',
 'Looking for a DJ for a yacht party in Dubai',
 'Need a female DJ for a birthday party',
 'Can anyone recommend a wedding DJ?',
 'Any DJ available this Saturday?',
 'Looking to hire a deejay for our corporate event',
 'DJ needed for a party in Dubai Marina',
 'Does anyone have recommendations for a wedding DJ?',
 'Поделитесь контактами диджея',
]
NEGATIVE = [
 'Я диджей, ищу заказы в Дубае',
 'Нужен диджей? Предлагаю услуги для праздников',
 'Ищу работу диджеем в Дубае',
 'Я профессиональный DJ. Looking for gigs',
 'DJ available for bookings. Book me for your next party',
 'Looking for venues, I am a DJ',
 'Ищу DJ контроллер Pioneer',
 'Нужен пульт диджея на вечер',
 'Looking for DJ equipment rental',
 'Нужны курсы диджеинга',
 'Хочу научиться играть как DJ',
 'Can anyone recommend DJ lessons?',
 'Какой трек вчера играл диджей?',
 'Подскажите плейлист DJ с вечеринки',
 'Вчера на яхте был отличный диджей',
 'Диджей выступит на вечеринке в субботу',
 'Посоветуйте фотографа на яхту',
]

class DJTests(unittest.TestCase):
    def test_customer_demand(self):
        for text in POSITIVE:
            with self.subTest(text=text): self.assertTrue(matches(text, dj_rule()))

    def test_offers_equipment_training_and_chatter(self):
        for text in NEGATIVE:
            with self.subTest(text=text): self.assertFalse(matches(text, dj_rule()))

    def test_whole_words_and_sources(self):
        rule = dj_rule([-1,-1,-2])
        self.assertEqual(rule['sources'],[-1,-2])
        self.assertFalse(matches('Need a djinn',rule))
