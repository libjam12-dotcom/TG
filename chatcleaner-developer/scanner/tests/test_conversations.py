import asyncio
from datetime import datetime,timezone
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store,now
from conversations import Conversations,OpenLead,Send,Control,Read,Retry,Settings,Topic
from qualification import Decision,Fact,validate_decision,request_decision,ProviderError
from telethon import types,errors
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient

class InboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'db')
        self.peer=types.User(id=77,first_name='Клиент',username=None,access_hash=777)
        owner=self
        class Client:
            sent=[];failure=None;messages=[];entity=owner.peer
            def is_connected(self):return True
            async def get_entity(self,p):return self.entity
            async def get_input_entity(self,p):return types.InputPeerUser(p.id,p.access_hash)
            async def get_messages(self,p,**kw):
                if 'ids' in kw:return SimpleNamespace(get_sender=AsyncMock(return_value=self.entity))
                return self.messages[::-1]
            async def iter_messages(self,p,**kw):
                for m in self.messages:
                    if m.id>kw.get('min_id',0):yield m
            async def __call__(self,req):
                if self.failure:raise self.failure
                self.sent.append(req);return SimpleNamespace(id=1000+len(self.sent))
        self.client=Client();self.runtime=SimpleNamespace(me=types.User(id=42,first_name='Владелец'),client=self.client)
        self.crm=Conversations(lambda:self.store,lambda:self.runtime);self.crm.initialize()
        with self.store.db:
            self.store.db.execute('INSERT INTO sources(id,title,username) VALUES(-1001,?,?)',('Внешний чат','external'))
            spec={'keywords':['яхта'],'required':[],'excluded':[],'sources':[-1001]}
            self.store.db.execute('INSERT INTO rules(id,name,data) VALUES(1,?,?)',('Яхты',json.dumps(spec)))
        self.store.put('running',True)
        self.lead=self.store.ingest(-1001,1,'Нужна яхта в Дубае','Автор',77,'https://t.me/external/1')
        self.opened=await self.crm.open_lead(OpenLead(lead_id=self.lead));self.cid=self.opened['conversation']['id']
    async def asyncTearDown(self):self.store.db.close();self.tmp.cleanup()
    def msg(self,n,text,out=False):return SimpleNamespace(id=n,message=text,out=out,peer_id=types.PeerUser(77),sender_id=42 if out else 77,date=datetime.now(timezone.utc))
    async def incoming(self,text='Нужна яхта',n=20):await self.crm.ingest(self.crm.get(self.cid),self.msg(n,text))
    async def send(self,key='request_abcdefgh1',text='Здравствуйте!'):
        return await self.crm.send(self.cid,Send(text=text,request_key=key,account_id=42,peer_id=77,confirmed=True))
    def row(self,mid):return self.store.rows('SELECT * FROM conversation_messages WHERE id=?',(mid,))[0]
    def decision(self,**kwargs):return Decision(**{'action':'reply','reply':'На какую дату нужна яхта?','facts':[],'interested':False,'interest_quote':'','contact_ready':False,'contact_quote':'','next_action':'',**kwargs})
    async def ai(self,decision=None):
        self.store.put('conversation_api_key','fixture-key')
        cfg={**self.crm.config(),'paused':False,'auto_reply':True,'account_id':42,'model':'fixture-model','consent':True,'provider_verified':True}
        self.store.put('conversation_config',cfg)
        self.store.put('conversation_topic_1',{**Topic().model_dump(),'mode':'ai','company':'Тестовая компания','facts':'Подбираем яхты в Дубае. Цены и наличие уточняет менеджер.','reviewed':True})
        await self.crm.control(self.cid,Control(mode='ai'))
        with self.store.db:self.store.db.execute('UPDATE conversations SET ai_after=0 WHERE id=?',(self.cid,))
        self.crm.provider=AsyncMock(return_value=(decision or self.decision(),{'input_tokens':50,'output_tokens':20}))
    async def test_verified_private_recipient_without_username_and_group_rejected(self):
        self.assertEqual(self.opened['conversation']['peer_id'],77)
        self.assertNotIn('access_hash',self.opened['conversation'])
        for sender in (-1001,None):
            lead={**self.store.rows('SELECT * FROM leads')[0],'sender_id':sender}
            with self.assertRaises(HTTPException):await self.crm.resolve(lead)
        self.client.entity=types.User(id=999,first_name='Другой',access_hash=1)
        with self.assertRaises(HTTPException):await self.crm.resolve(self.store.rows('SELECT * FROM leads')[0])
    async def test_manual_delivery_and_idempotency(self):
        a=await self.send();b=await self.send();self.assertEqual(a['id'],b['id'])
        await self.crm.deliver_one();await self.crm.deliver_one()
        self.assertEqual(len(self.client.sent),1);req=self.client.sent[0]
        self.assertIsInstance(req.peer,types.InputPeerUser);self.assertEqual(req.peer.user_id,77)
        self.assertEqual(self.row(a['id'])['state'],'sent')
        self.assertEqual(self.row(a['id'])['tg_id'],1001)
        with self.assertRaises(HTTPException):await self.send(text='Другой текст')
    async def test_explicit_confirmation_account_and_peer_guards(self):
        for kwargs in ({'confirmed':False},{'account_id':43},{'peer_id':88}):
            data=dict(text='Hello',request_key='fixture_request1',account_id=42,peer_id=77,confirmed=True);data.update(kwargs)
            with self.assertRaises(HTTPException):await self.crm.send(self.cid,Send(**data))
        self.runtime.me=types.User(id=43,first_name='Другой аккаунт')
        with self.assertRaises(HTTPException):await self.crm.detail(self.cid)
    async def test_incoming_dedup_unread_and_scoped_read(self):
        await self.incoming(n=20);await self.incoming(n=20)
        first=(await self.crm.detail(self.cid))['messages'][0]['id']
        await self.incoming('Ещё вопрос',21)
        await self.crm.read(self.cid,Read(through_id=first))
        d=await self.crm.detail(self.cid);self.assertEqual(len(d['messages']),2);self.assertEqual(d['conversation']['unread'],1)
        event=SimpleNamespace(is_private=False,chat_id=-1001,message=self.msg(30,'В группе'))
        await self.crm.on_event(event);self.assertEqual(len((await self.crm.detail(self.cid))['messages']),2)
    async def test_refusal_cancels_pending_and_prevents_future_sends(self):
        item=await self.send();await self.incoming('Не пишите мне больше')
        self.assertEqual(self.row(item['id'])['state'],'cancelled');self.assertTrue(self.crm.get(self.cid)['stopped'])
        with self.assertRaises(HTTPException):await self.send('new_request_abcd')
        await self.crm.deliver_one();self.assertFalse(self.client.sent)
    async def test_unknown_send_is_not_retried_after_restart(self):
        item=await self.send();self.client.failure=asyncio.TimeoutError()
        await self.crm.deliver_one();self.assertEqual(self.row(item['id'])['state'],'uncertain')
        self.crm.initialize();await self.crm.deliver_one()
        with self.assertRaises(HTTPException):await self.crm.retry(self.cid,item['id'],Retry(confirmed=True))
        with self.assertRaises(HTTPException):await self.send('another_request1')
    async def test_empty_telegram_receipt_is_uncertain_and_never_blindly_retried(self):
        item=await self.send();client_type=type(self.client)
        async def empty(client,request):
            client.sent.append(request)
            return types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0)
        with patch.object(client_type,'__call__',empty):
            await self.crm.deliver_one();self.crm.initialize();await self.crm.deliver_one()
        self.assertEqual(self.row(item['id'])['state'],'uncertain');self.assertIsNone(self.row(item['id'])['tg_id'])
        self.assertEqual(len(self.client.sent),1)
        with self.assertRaises(HTTPException):await self.crm.retry(self.cid,item['id'],Retry(confirmed=True))
    async def test_outgoing_event_receipt_is_preserved_when_rpc_response_has_no_id(self):
        item=await self.send(text='Event before RPC response');client_type=type(self.client)
        async def event_first(client,request):
            client.sent.append(request)
            await self.crm.ingest(self.crm.get(self.cid),self.msg(700,'Event before RPC response',True))
            return types.Updates(updates=[],users=[],chats=[],date=datetime.now(timezone.utc),seq=0)
        with patch.object(client_type,'__call__',event_first):await self.crm.deliver_one()
        self.assertEqual(self.row(item['id'])['state'],'sent');self.assertEqual(self.row(item['id'])['tg_id'],700)
        self.assertEqual(len(self.store.rows('SELECT * FROM conversation_messages')),1)
    async def test_crashed_sending_recovers_as_uncertain(self):
        item=await self.send()
        with self.store.db:self.store.db.execute("UPDATE conversation_messages SET state='sending' WHERE id=?",(item['id'],))
        self.crm.initialize();self.assertEqual(self.row(item['id'])['state'],'uncertain')
    async def test_known_telegram_refusal_retry_uses_same_random_id(self):
        item=await self.send();rid=self.row(item['id'])['random_id'];self.client.failure=errors.ChatWriteForbiddenError(None)
        await self.crm.deliver_one();self.assertEqual(self.row(item['id'])['state'],'failed')
        self.client.failure=None;await self.crm.retry(self.cid,item['id'],Retry(confirmed=True));await self.crm.deliver_one()
        self.assertEqual(self.client.sent[0].random_id,rid)
    async def test_flood_wait_obeyed_without_immediate_retry(self):
        item=await self.send();self.client.failure=errors.FloodWaitError(None,60)
        await self.crm.deliver_one();self.assertEqual(self.row(item['id'])['state'],'pending');self.assertGreater(self.store.get('conversation_not_before'),0)
        self.client.failure=None;await self.crm.deliver_one();self.assertFalse(self.client.sent)
    async def test_sync_catches_missed_messages_after_restart(self):
        await self.incoming(n=10);self.client.messages=[self.msg(10,'Нужна яхта'),self.msg(11,'На пятницу')]
        self.crm.initialize();await self.crm.sync_one(self.crm.get(self.cid));await self.crm.sync_one(self.crm.get(self.cid))
        self.assertEqual(len((await self.crm.detail(self.cid))['messages']),2);self.assertEqual(self.crm.get(self.cid)['cursor'],11)
    async def test_ai_reply_once_and_no_repeated_known_questions_in_payload(self):
        await self.incoming('Нужна яхта в Дубае на пятницу');await self.ai()
        await self.crm.qualify_one();await self.crm.qualify_one()
        self.assertEqual(self.crm.provider.await_count,1)
        self.assertIn('на пятницу',self.crm.provider.call_args.args[2]['dialogue'][0]['text'])
        await self.crm.deliver_one();self.assertEqual(len(self.client.sent),1)
    async def test_manager_takeover_during_provider_call_discards_reply(self):
        await self.incoming();await self.ai()
        async def provider(*args):
            await self.crm.control(self.cid,Control(mode='manual'))
            return self.decision(),{}
        self.crm.provider=provider;await self.crm.qualify_one()
        self.assertEqual(self.crm.get(self.cid)['mode'],'manual')
        self.assertFalse(self.store.rows("SELECT * FROM conversation_messages WHERE kind='ai'"))
    async def test_refusal_arrives_during_provider_call(self):
        await self.incoming();await self.ai()
        async def provider(*args):
            await self.incoming('Не пишите больше',21)
            return self.decision(),{}
        self.crm.provider=provider;await self.crm.qualify_one();await self.crm.deliver_one()
        self.assertFalse(self.client.sent);self.assertTrue(self.crm.get(self.cid)['stopped'])
    async def test_external_manager_message_pauses_ai(self):
        await self.incoming();await self.ai();await self.crm.ingest(self.crm.get(self.cid),self.msg(21,'Я менеджер, помогу',True))
        self.assertEqual(self.crm.get(self.cid)['mode'],'manual')
    async def test_hot_requires_both_exact_incoming_quotes(self):
        text='Мне нужна ваша яхта. Позвоните мне завтра, хочу обсудить с менеджером.'
        await self.incoming(text)
        d=self.decision(action='handoff',reply='',interested=True,interest_quote='Мне нужна ваша яхта',contact_ready=True,contact_quote='Позвоните мне завтра',next_action='Связаться завтра в этом диалоге, уточнить телефон.')
        await self.ai(d);await self.crm.qualify_one()
        c=self.crm.get(self.cid);self.assertEqual(c['status'],'hot');self.assertEqual(c['mode'],'manual');self.assertTrue(json.loads(c['handoff'])['hot']);self.assertFalse(self.client.sent)
    async def test_thanks_is_not_hot_even_if_model_claims_it(self):
        await self.incoming('Спасибо')
        await self.ai(self.decision(action='handoff',reply='',interested=True,interest_quote='Спасибо',contact_ready=True,contact_quote='Спасибо'))
        await self.crm.qualify_one();self.assertNotEqual(self.crm.get(self.cid)['status'],'hot')
    async def test_ungrounded_quotes_and_numbers_rejected(self):
        d=self.decision(reply='Стоимость 9999 долларов')
        with self.assertRaises(ProviderError):validate_decision(d,'Нужна яхта',['Здравствуйте'],{},'Цены уточняет менеджер')
        d=self.decision(facts=[Fact(name='budget',value='100',evidence='Несуществующая цитата')])
        facts,hot,reply=validate_decision(d,'Нужна яхта',['Здравствуйте'],{},'Цены уточняет менеджер')
        self.assertEqual(facts,{})
    async def test_provider_error_pauses_without_fake_reply(self):
        await self.incoming();await self.ai();self.crm.provider=AsyncMock(side_effect=ProviderError('Лимит провайдера'))
        await self.crm.qualify_one();self.assertEqual(self.crm.get(self.cid)['mode'],'manual');self.assertFalse(self.client.sent)
    async def test_daily_call_and_reply_limits(self):
        await self.incoming();await self.ai()
        with self.store.db:self.store.db.execute('INSERT INTO conversation_usage(day,calls) VALUES(?,40)',(now()[:10],))
        await self.crm.qualify_one();self.crm.provider.assert_not_called();self.assertEqual(self.crm.get(self.cid)['mode'],'manual')
    async def test_saving_new_key_invalidates_verification_without_leaking(self):
        await self.ai();before=self.store.get('conversation_api_key')
        with self.assertRaises(HTTPException):await self.crm.save_settings(Settings(paused=False,model='fixture-model',api_key='changed-key',consent=True,account_id=42))
        self.assertEqual(self.store.get('conversation_api_key'),before)
        response=await self.crm.save_settings(Settings(model='fixture-model',api_key='changed-key'))
        self.assertFalse(response['config']['provider_verified']);self.assertNotIn('changed-key',json.dumps(response))
    async def test_close_cancels_manual_queue(self):
        m=await self.send();await self.crm.control(self.cid,Control(status='closed'));await self.crm.deliver_one();self.assertEqual(self.row(m['id'])['state'],'cancelled')
    async def test_http_routing_and_validation(self):
        app=FastAPI();app.include_router(self.crm.router)
        with TestClient(app) as client:
            self.assertEqual(client.get('/api/scanner/conversations/settings').status_code,200)
            self.assertEqual(client.post('/api/scanner/conversations/'+str(self.cid)+'/send',json={'text':'Hello'}).status_code,422)
            response=client.get('/api/scanner/conversations/'+str(self.cid));self.assertNotIn('access_hash',response.text)
    async def test_auto_first_disabled_and_old_leads_skipped(self):
        await self.ai();await self.crm.initiate_one();self.assertFalse(self.client.sent)
        cfg=self.crm.config();cfg.update(auto_first=True,first_since=now());self.store.put('conversation_config',cfg)
        await self.crm.initiate_one();self.assertEqual(len(self.store.rows('SELECT * FROM conversations')),1)

    async def prepare_new_lead(self):
        await self.ai()
        cfg=self.crm.config();cfg.update(auto_first=True,first_since='2000-01-01T00:00:00+00:00');self.store.put('conversation_config',cfg)
        self.store.put('conversation_topic_1',{**self.crm.topic(1),'auto_first':True})
        self.client.entity=types.User(id=88,first_name='Новый клиент',access_hash=888)
        return self.store.ingest(-1001,2,'Нужна яхта на завтра в Дубае','Новый клиент',88,'https://t.me/external/2')

    async def test_enabled_first_contact_is_scoped_and_only_once(self):
        await self.prepare_new_lead();await self.crm.initiate_one();await self.crm.initiate_one()
        rows=self.store.rows("SELECT * FROM conversation_messages WHERE kind='ai'")
        self.assertEqual(len(rows),1);self.assertIn('Тестовая компания',rows[0]['text'])
        await self.crm.deliver_one();self.assertEqual(len(self.client.sent),1)
        self.assertEqual(self.client.sent[0].peer.user_id,88)

    async def test_first_contact_skips_unavailable_or_existing_history(self):
        await self.prepare_new_lead()
        self.client.get_messages=AsyncMock(side_effect=asyncio.TimeoutError())
        await self.crm.initiate_one();self.assertFalse(self.store.rows("SELECT * FROM conversation_messages WHERE kind='ai'"))
        self.assertIn('истории',self.store.rows('SELECT * FROM conversations WHERE peer_id=88')[0]['error'])

    async def test_first_contact_skips_existing_private_messages(self):
        await self.prepare_new_lead()
        self.client.messages=[SimpleNamespace(id=10,message='Уже общались',out=False,peer_id=types.PeerUser(88),sender_id=88,date=datetime.now(timezone.utc))]
        await self.crm.initiate_one();self.assertFalse(self.store.rows("SELECT * FROM conversation_messages WHERE kind='ai'"))

    async def test_new_event_does_not_advance_poll_cursor_past_missed_message(self):
        await self.incoming('Последнее событие',n=12)
        self.assertEqual(self.crm.get(self.cid)['cursor'],0)
        self.client.messages=[self.msg(11,'Пропущено при разрыве'),self.msg(12,'Последнее событие')]
        await self.crm.sync_one(self.crm.get(self.cid))
        self.assertEqual({r['tg_id'] for r in (await self.crm.detail(self.cid))['messages']},{11,12})

    async def test_edited_refusal_cancels_queued_reply(self):
        await self.incoming('Пока интересно',n=20);item=await self.send()
        await self.incoming('Не пишите мне',n=20)
        self.assertTrue(self.crm.get(self.cid)['stopped']);self.assertEqual(self.row(item['id'])['state'],'cancelled')

    async def test_unfilled_company_template_is_rejected(self):
        with self.assertRaises(HTTPException):await self.send(text='Я помощник [название компании]')
        self.assertFalse(self.store.rows('SELECT * FROM conversation_messages'))

class ProviderAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_adapter_request_shape_and_parsing(self):
        decision={'action':'stop','reply':'','facts':[],'interested':False,'interest_quote':'','contact_ready':False,'contact_quote':'','next_action':''}
        response=SimpleNamespace(status_code=200,json=lambda:{'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(decision)}]}],'usage':{'input_tokens':1}})
        client=AsyncMock();client.post.return_value=response
        with patch('qualification.httpx.AsyncClient') as cls:
            cls.return_value.__aenter__.return_value=client
            result,usage=await request_decision('fake-key','fixture-model',{'lead':'test'})
        call=client.post.call_args;self.assertEqual(call.args[0],'https://api.openai.com/v1/responses');self.assertFalse(call.kwargs['json']['store']);self.assertNotIn('tools',call.kwargs['json']);self.assertEqual(result.action,'stop')

if __name__=='__main__':unittest.main()
