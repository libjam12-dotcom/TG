import json
import sys
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi import HTTPException
from telethon import types
from engine import Store
from marketing_topics import seed_marketing
from owner_ai import OwnerConversations
from owner_prospects import ReviewedRequest


class ReviewedProspectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'owner.db')
        seed_marketing(self.store)
        self.source=types.Channel(id=123,access_hash=321,title='Requests',photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),username='public_requests',broadcast=True)
        self.quote='Для наружной рекламы нужно сделать многостраничный сайт. В отклике стоимость и портфолио.\n📝 @BuyerOne'
        self.post=types.Message(id=5,peer_id=types.PeerChannel(123),from_id=types.PeerChannel(123),date=datetime.now(timezone.utc),message='Подборка\n'+self.quote+'\nДругие задания')
        self.buyer=types.User(id=77,access_hash=777,first_name='Buyer',username='BuyerOne')
        self.client=SimpleNamespace(is_connected=lambda:True,get_entity=AsyncMock(side_effect=[self.source,self.buyer]),get_messages=AsyncMock(side_effect=[self.post,[],[]]),get_input_entity=AsyncMock(return_value=types.InputPeerUser(77,777)))
        self.runtime=SimpleNamespace(client=self.client,me=types.User(id=42,username='demo_identity_3'))
        self.inbox=OwnerConversations(lambda:self.store,lambda:self.runtime)
        self.inbox.initialize()
        with self.store.db:
            self.store.db.execute("INSERT INTO sources(id,title,username,enabled) VALUES(-1000000000123,'Requests','public_requests',1)")
            rule=self.inbox.rule(1)
            data={k:v for k,v in {**rule,'sources':[-1000000000123],'keywords':['сайт'],'required':['нужен','нужно'],'demand_only':True,'language':'ru','contractor_only':True}.items() if k not in ('id','name','enabled')}
            self.store.db.execute('UPDATE rules SET enabled=1,data=? WHERE id=1',(json.dumps(data),))
        self.spec=ReviewedRequest(account_id=42,source_id=-1000000000123,message_id=5,username='BuyerOne',quote=self.quote,rule_id=1,confirmed=True)
    async def asyncTearDown(self):
        self.store.db.close()
        self.tmp.cleanup()
    async def test_verified_named_contact_opens_manual_without_sending_or_forging_author(self):
        result=await self.inbox.open_prospect(self.spec)
        self.assertEqual(result['conversation']['peer_id'],77)
        self.assertEqual(result['conversation']['mode'],'manual')
        self.assertFalse(result['reviewed_request']['previous_outgoing_found'])
        lead=self.store.rows('SELECT * FROM leads')[0]
        self.assertEqual(lead['sender_id'],-1000000000123)
        self.assertEqual(lead['author'],'@public_requests')
        self.assertEqual(lead['origin'],'manual_prospect')
        self.assertFalse(self.store.rows('SELECT * FROM conversation_messages'))
        self.assertTrue(self.inbox.config()['paused'])
        self.assertFalse(self.inbox.config()['auto_first'])
        self.client.get_messages.assert_any_await(types.InputPeerUser(77,777),limit=1,from_user='me')
    async def test_old_outgoing_blocks_even_when_recent_history_empty(self):
        self.client.get_messages.side_effect=[self.post,[SimpleNamespace(id=1,out=True)],[]]
        with self.assertRaises(HTTPException) as exc:await self.inbox.open_prospect(self.spec)
        self.assertIn('уже писал',exc.exception.detail)
        self.assertFalse(self.store.rows('SELECT * FROM conversations'))
    async def test_delivery_options_only_reads_selected_peer_and_hides_profile_data(self):
        result=await self.inbox.open_prospect(self.spec)
        rpc=AsyncMock(return_value=SimpleNamespace(full_user=SimpleNamespace(id=77,blocked=False,contact_require_premium=False,send_paid_messages_stars=50,about='private description')))
        self.runtime.client=rpc
        rpc.is_connected=lambda:True
        value=await self.inbox.delivery_options(result['conversation']['id'])
        self.assertEqual(value['send_paid_messages_stars'],50)
        self.assertNotIn('about',value)
        self.assertFalse(value['sent'])
        self.assertFalse(self.store.rows('SELECT * FROM conversation_messages'))
        self.assertEqual(type(rpc.await_args.args[0]).__name__,'GetFullUserRequest')
        self.runtime.me=types.User(id=99,username='Other')
        with self.assertRaises(HTTPException):await self.inbox.delivery_options(result['conversation']['id'])
        self.assertEqual(rpc.await_count,1)
    async def test_changed_source_post_and_wrong_contact_do_not_open(self):
        self.client.get_messages.side_effect=[SimpleNamespace(message='Нужно что-то другое')]
        with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec)
        self.assertFalse(self.store.rows('SELECT * FROM conversations'))
    async def test_source_id_mismatch_rejected_before_private_history(self):
        self.source.id=124
        with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec)
        self.client.get_messages.assert_not_awaited()
    async def test_unconfirmed_account_or_unmentioned_contact_never_reads_private_history(self):
        for update in ({'confirmed':False},{'account_id':99},{'username':'OtherBuyer'}):
            with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec.model_copy(update=update))
        self.client.get_messages.assert_not_awaited()
    async def test_supplier_employment_stale_and_bot_not_eligible(self):
        for quote in ['Предлагаю услуги: сделаю сайт. @BuyerOne','Нужен разработчик сайта в штат 5/2. @BuyerOne']:
            with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec.model_copy(update={'quote':quote}))
        self.post.date=datetime.now(timezone.utc)-timedelta(days=8)
        self.client.get_entity.side_effect=[self.source]
        self.client.get_messages.side_effect=[self.post]
        with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec)
        self.post.date=datetime.now(timezone.utc)
        self.buyer.bot=True
        self.client.get_entity.side_effect=[self.source,self.buyer]
        self.client.get_messages.side_effect=[self.post]
        with self.assertRaises(HTTPException):await self.inbox.open_prospect(self.spec)
        self.assertFalse(self.store.rows('SELECT * FROM conversations'))


if __name__=='__main__':unittest.main()
