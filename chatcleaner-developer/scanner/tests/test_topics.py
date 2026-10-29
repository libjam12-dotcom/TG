import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store,matches
from topics import seed_topics
from telethon import types

class TopicTests(unittest.TestCase):
    def test_booking_a_yacht_or_safari_does_not_match_hotels(self):
        with tempfile.TemporaryDirectory() as folder:
            s=Store(Path(folder)/'topics.db');seed_topics(s)
            rules={r['topic']:r for r in s.rules()}
            for topic,text in [('yacht','Хочу забронировать яхту на субботу'),('safari','Нужно забронировать сафари на двоих')]:
                self.assertTrue(matches(text,rules[topic]))
                self.assertFalse(matches(text,rules['hotels']))
            self.assertTrue(matches('Хочу забронировать гостиницу на неделю',rules['hotels']))
            s.db.close()
    def test_nine_drafts_are_persistent_and_match_requests_not_offers(self):
        with tempfile.TemporaryDirectory() as folder:
            s=Store(Path(folder)/'topics.db');seed_topics(s);seed_topics(s)
            self.assertEqual(len(s.rules()),9)
            samples=['Куплю билеты на UFC','Ищу яхту на день рождения','Нужно сафари в Дубае','Нужна машина? Ищу авто в аренду','Нужен трансфер из аэропорта','Посоветуйте отель со скидкой','Кто поможет с визой','Нужно оформить Emirates ID','Ищу smm специалиста']
            for rule,text in zip(s.rules(),samples):
                self.assertFalse(rule['enabled']);self.assertEqual(rule['sources'],[])
                self.assertTrue(matches(text,rule),(rule['name'],text))
                self.assertFalse(matches('Предлагаем наши услуги. '+text,rule))
            s.db.close()
    def test_assignment_and_discovery_filters(self):
        import app
        with tempfile.TemporaryDirectory() as folder:
            s=Store(Path(folder)/'topics.db');seed_topics(s)
            with s.db:s.db.execute("INSERT INTO sources(id,title) VALUES(-100123,'Test')")
            async def run():
                await app.assign(app.AttachSources(sources=[-100123],rules=[1,2]))
                result=await app.discover(app.Discovery(query='Dubai',kind='group',min_members=100))
                self.assertEqual(len(result['results']),1)
                self.assertEqual(result['results'][0]['title'],'Dubai group')
            class Client:
                def is_connected(self):return True
                async def __call__(self,request):
                    common=dict(photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),left=True)
                    return SimpleNamespace(chats=[types.Channel(id=1,title='Dubai group',username='dubaigroup',megagroup=True,participants_count=200,**common),types.Channel(id=2,title='Dubai channel',username='dubaichannel',broadcast=True,participants_count=1000,**common)])
            rt=app.Runtime();rt.client=Client();rt.me=SimpleNamespace(id=1)
            with patch.object(app,'store',s),patch.object(app,'runtime',rt):asyncio.run(run())
            self.assertTrue(s.rules()[0]['enabled']);self.assertEqual(s.rules()[1]['sources'],[-100123])
            self.assertFalse(s.rules()[2]['enabled']);s.db.close()
