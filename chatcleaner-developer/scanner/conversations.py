"""Account-scoped direct conversations, durable delivery and opt-in AI qualification."""
import asyncio
import contextlib
from datetime import datetime, timezone
import json
import re
import secrets
import time
from typing import Literal
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from telethon import types, functions, errors
from engine import now, matches
from qualification import request_decision, validate_decision, ProviderError

DEFAULTS={'paused':True,'auto_reply':False,'auto_first':False,'account_id':None,'model':'',
          'provider_verified':False,'consent':False,'daily_messages':20,'daily_ai_calls':40,'max_ai_replies':6,'delay_seconds':30}
FIELDS={'need':'Потребность','city':'Город','date':'Дата','budget':'Бюджет','details':'Детали услуги','contact':'Канал / контакт','contact_time':'Время связи'}
STOP=re.compile(r'\b(?:не пишите|не пиши|не надо|не нужно|не интересно|неинтересно|не актуально|неактуально|отстань|отказ|ошиблись|stop|unsubscribe|not interested|do not contact|don.t message|wrong person)\b',re.I)

class Settings(BaseModel):
    paused:bool=True
    auto_reply:bool=False
    auto_first:bool=False
    account_id:int|None=None
    model:str=Field(default='',max_length=100,pattern=r'^[a-zA-Z0-9._:-]*$')
    api_key:str=Field(default='',max_length=512)
    clear_key:bool=False
    consent:bool=False
    daily_messages:int=Field(default=20,ge=1,le=100)
    daily_ai_calls:int=Field(default=40,ge=1,le=200)
    max_ai_replies:int=Field(default=6,ge=1,le=20)
    delay_seconds:int=Field(default=30,ge=15,le=3600)

class Topic(BaseModel):
    mode:Literal['manual','ai']='manual'
    max_replies:int=Field(default=6,ge=1,le=20)
    company:str=Field(default='',max_length=120)
    facts:str=Field(default='',max_length=8000)
    questions:str=Field(default='Услуга и задача\nГород и место\nДата\nБюджет\nКоличество гостей / детали\nУдобный канал и время связи',max_length=3000)
    handoff:str=Field(default='Подтверждённый интерес и готовность обсудить с менеджером. По просьбе человека передать менеджеру раньше.',max_length=2000)
    first_text:str=Field(default='Здравствуйте! Я помощник {company}. Увидел ваш запрос в «{source}» по теме «{topic}». Подскажите, он ещё актуален?',max_length=1500)
    reviewed:bool=False
    auto_first:bool=False

class OpenLead(BaseModel):
    lead_id:int=Field(gt=0)
    rule_id:int|None=None

class Send(BaseModel):
    text:str=Field(min_length=1,max_length=4096)
    request_key:str=Field(min_length=16,max_length=80,pattern=r'^[a-zA-Z0-9_-]+$')
    account_id:int=Field(gt=0)
    peer_id:int=Field(gt=0)
    confirmed:bool=False

class Control(BaseModel):
    mode:Literal['manual','ai']='manual'
    status:Literal['new','working','hot','closed']='working'
    rule_id:int|None=None

class Read(BaseModel):
    through_id:int=Field(ge=0)

class Retry(BaseModel):
    confirmed:bool=False


def error_text(exc):
    if isinstance(exc,errors.FloodWaitError):return f'Telegram просит подождать {exc.seconds} секунд.'
    return {'UserPrivacyRestrictedError':'Пользователь ограничил личные сообщения.',
            'UserIsBlockedError':'Пользователь заблокирован.', 'YouBlockedUserError':'Сначала снимите блокировку в Telegram.',
            'PeerFloodError':'Telegram ограничил исходящие сообщения. Автоматика остановлена.',
            'ChatWriteForbiddenError':'Telegram запретил отправку этому получателю.',
            'InputUserDeactivatedError':'Аккаунт получателя удалён.',
            'UserDeactivatedError':'Аккаунт недоступен.', 'UserDeactivatedBanError':'Аккаунт заблокирован.',
            'AuthKeyUnregisteredError':'Подключите Telegram заново.', 'SessionRevokedError':'Сессия Telegram отозвана.',
            'ChatForwardsRestrictedError':'Отправка ограничена Telegram.',
            'AllowPaymentRequiredError':'Telegram требует оплату сообщения. Автоматическая оплата не поддерживается.'}.get(type(exc).__name__,f'Действие не выполнено ({type(exc).__name__}).')

class Conversations:
    def __init__(self,get_store,get_runtime,prefix='/api/scanner/conversations'):
        self.get_store,self.get_runtime=get_store,get_runtime
        self.task=None;self.lock=asyncio.Lock();self.poll_position=0;self.last_poll=0
        self.provider=request_decision
        self.router=APIRouter(prefix=prefix)
        for path,fn,methods in [('/settings',self.settings,['GET']),('/settings',self.save_settings,['POST']),('/provider-test',self.provider_test,['POST']),('/topics/{rule_id}',self.save_topic,['POST']),('/open',self.open_lead,['POST']),('',self.listing,['GET']),('/{cid}',self.detail,['GET']),('/{cid}/send',self.send,['POST']),('/{cid}/control',self.control,['POST']),('/{cid}/read',self.read,['POST']),('/{cid}/sync',self.sync_endpoint,['POST']),('/{cid}/messages/{mid}/retry',self.retry,['POST']),('/{cid}/messages/{mid}/telegram-evidence',self.telegram_evidence,['GET'])]:
            self.router.add_api_route(path,fn,methods=methods)

    @property
    def store(self):return self.get_store()
    @property
    def runtime(self):return self.get_runtime()
    @property
    def db(self):return self.store.db

    def outbound_allowed(self):
        guard=getattr(self.runtime,'outbound_guard',None)
        if guard:
            try:guard()
            except HTTPException:return False
        return True

    def stop_outbound(self,exc):
        stop=getattr(self.runtime,'outbound_pause',None)
        if stop:stop(exc)

    def initialize(self):
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS conversations(id INTEGER PRIMARY KEY,account_id INTEGER NOT NULL,peer_id INTEGER NOT NULL,access_hash TEXT NOT NULL,title TEXT NOT NULL,username TEXT,lead_id INTEGER NOT NULL REFERENCES leads(id),rule_id INTEGER NOT NULL,mode TEXT NOT NULL DEFAULT 'manual',status TEXT NOT NULL DEFAULT 'new',stopped INTEGER NOT NULL DEFAULT 0,version INTEGER NOT NULL DEFAULT 0,cursor INTEGER NOT NULL DEFAULT 0,ai_cursor INTEGER NOT NULL DEFAULT 0,ai_after REAL NOT NULL DEFAULT 0,facts TEXT NOT NULL DEFAULT '{}',handoff TEXT NOT NULL DEFAULT '{}',error TEXT,last_sync TEXT,created TEXT NOT NULL,updated TEXT NOT NULL,UNIQUE(account_id,peer_id));
        CREATE TABLE IF NOT EXISTS conversation_leads(cid INTEGER NOT NULL REFERENCES conversations(id),lead_id INTEGER NOT NULL REFERENCES leads(id),PRIMARY KEY(cid,lead_id));
        CREATE TABLE IF NOT EXISTS conversation_messages(id INTEGER PRIMARY KEY,cid INTEGER NOT NULL REFERENCES conversations(id),tg_id INTEGER,random_id INTEGER,request_key TEXT,direction TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,text TEXT NOT NULL,created TEXT NOT NULL,attempted_at TEXT,unread INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,trigger_id INTEGER,error TEXT,UNIQUE(cid,tg_id),UNIQUE(cid,request_key));
        CREATE INDEX IF NOT EXISTS conversation_pending ON conversation_messages(state,next_at);
        CREATE TABLE IF NOT EXISTS conversation_usage(day TEXT PRIMARY KEY,calls INTEGER NOT NULL DEFAULT 0,input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS conversation_open_errors(account_id INTEGER NOT NULL,lead_id INTEGER NOT NULL,error TEXT NOT NULL,PRIMARY KEY(account_id,lead_id));
        ''')
        with self.db:
            self.db.execute("UPDATE conversation_messages SET state='uncertain',error='Сервис перезапустился во время отправки. Проверьте личную переписку в Telegram; автоматический повтор отключён.' WHERE state='sending'")
            self.db.execute("UPDATE conversations SET mode='manual',error='Нужно проверить незавершённую отправку.' WHERE id IN (SELECT cid FROM conversation_messages WHERE state='uncertain')")

    def config(self):return {**DEFAULTS,**self.store.get('conversation_config',{})}
    def topic(self,rule_id):return {**Topic().model_dump(),**self.store.get('conversation_topic_'+str(rule_id),{})}
    def account(self,required=True):
        me=self.runtime.me
        if me and self.runtime.client and self.runtime.client.is_connected():return me.id
        if required:raise HTTPException(409,'Подключите Telegram. Переписка сохранена; отправка пока недоступна.')
        return self.store.get('conversation_last_account')
    def get(self,cid,online=False):
        rows=self.store.rows('SELECT * FROM conversations WHERE id=? AND account_id=?',(cid,self.account(online)))
        if not rows:raise HTTPException(404,'Диалог не найден для текущего Telegram-аккаунта.')
        return rows[0]
    def rule(self,rule_id):
        rule=next((r for r in self.store.rules() if r['id']==rule_id),None)
        if not rule:raise HTTPException(404,'Тема не найдена.')
        return rule
    def pause(self,cid,error=None,status=None):
        with self.db:
            self.db.execute("UPDATE conversations SET mode='manual',version=version+1,error=?,status=COALESCE(?,status),updated=? WHERE id=?",(error,status,now(),cid))
            self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Автоматика остановлена менеджером или новым событием.' WHERE cid=? AND kind='ai' AND state='pending'",(cid,))
    def ready(self,cfg,topic,account):
        return bool(not cfg['paused'] and cfg['consent'] and cfg['provider_verified'] and cfg['model'] and self.store.get('conversation_api_key') and cfg['account_id']==account and topic['mode']=='ai' and topic['reviewed'] and topic['company'].strip() and topic['facts'].strip())

    def stop(self,cid):
        self.pause(cid,'Собеседник отказался от дальнейших сообщений.','closed')
        with self.db:
            self.db.execute('UPDATE conversations SET stopped=1 WHERE id=?',(cid,))
            self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Получатель отказался от сообщений.' WHERE cid=? AND state='pending'",(cid,))

    async def settings(self):
        cfg=self.config()
        return {'config':cfg,'key_configured':bool(self.store.get('conversation_api_key')),'account_id':self.account(False),'account_name':getattr(self.runtime.me,'username',None) or getattr(self.runtime.me,'first_name',None),
                'topics':[{'id':r['id'],'name':r['name'],**self.topic(r['id'])} for r in self.store.rules()],
                'usage':self.store.rows('SELECT * FROM conversation_usage WHERE day=?',(now()[:10],)),
                'error':self.store.get('conversation_worker_error')}

    async def save_settings(self,spec:Settings):
        async with self.lock:
            previous=self.config();cfg=spec.model_dump(exclude={'api_key','clear_key'})
            cfg['provider_verified']=previous['provider_verified'] and cfg['model']==previous['model'] and not spec.api_key and not spec.clear_key
            if not cfg['paused']:
                if cfg['account_id']!=self.account() or not cfg['provider_verified'] or not cfg['consent']:
                    raise HTTPException(400,'Сначала сохраните и проверьте ключ/модель, подтвердите передачу текстов OpenAI и выберите подключённый аккаунт.')
            if spec.api_key.strip():self.store.put('conversation_api_key',spec.api_key.strip())
            elif spec.clear_key:self.store.put('conversation_api_key','')
            cfg['first_since']=previous.get('first_since',now()) if previous['auto_first'] and cfg['auto_first'] and not previous['paused'] and not cfg['paused'] else now()
            self.store.put('conversation_config',cfg)
            self.store.put('conversation_config_version',self.store.get('conversation_config_version',0)+1)
            with self.db:self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Настройки автоматики изменены.' WHERE kind='ai' AND state='pending'")
        return await self.settings()

    async def save_topic(self,rule_id:int,spec:Topic):
        self.rule(rule_id)
        if spec.reviewed and (not spec.company.strip() or not spec.facts.strip()):raise HTTPException(400,'Укажите компанию и проверенные сведения об услуге.')
        if spec.reviewed and not re.search(r'помощник|assistant',spec.first_text,re.I):raise HTTPException(400,'Первое сообщение должно представлять отправителя как помощника.')
        if any(x not in ('company','source','topic') for x in re.findall(r'\{([^{}]+)\}',spec.first_text)):raise HTTPException(400,'В шаблоне доступны {company}, {source}, {topic}.')
        async with self.lock:
            self.store.put('conversation_topic_'+str(rule_id),spec.model_dump())
            for c in self.store.rows('SELECT id FROM conversations WHERE rule_id=?',(rule_id,)):self.pause(c['id'],'Сценарий изменён. Просмотрите его и снова включите ИИ для диалога.')
            self.store.put('conversation_config_version',self.store.get('conversation_config_version',0)+1)
        return {'ok':True}

    def reserve_call(self):
        day=now()[:10];limit=self.config()['daily_ai_calls']
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO conversation_usage(day) VALUES(?)',(day,))
            if not self.db.execute('UPDATE conversation_usage SET calls=calls+1 WHERE day=? AND calls<?',(day,limit)).rowcount:raise ProviderError('Дневной лимит запросов ИИ исчерпан.')
    async def provider_test(self):
        cfg=self.config();key=self.store.get('conversation_api_key')
        if not key or not cfg['model']:raise HTTPException(400,'Сохраните ключ OpenAI и название модели.')
        version=self.store.get('conversation_config_version',0)
        try:
            self.reserve_call()
            _,usage=await self.provider(key,cfg['model'],{'operator_configuration':{'company':'Тест','facts':'Это синтетическая проверка соединения, сообщений клиентам не отправлять.'},'lead':'Нужна яхта','incoming_messages':['Не пишите мне'],'previous_facts':{}})
            with self.db:self.db.execute('UPDATE conversation_usage SET input_tokens=input_tokens+?,output_tokens=output_tokens+? WHERE day=?',(int(usage.get('input_tokens',0)),int(usage.get('output_tokens',0)),now()[:10]))
        except ProviderError as exc:raise HTTPException(400,str(exc)) from None
        if version!=self.store.get('conversation_config_version',0):raise HTTPException(409,'Настройки изменились во время проверки. Проверьте ещё раз.')
        cfg['provider_verified']=True;self.store.put('conversation_config',cfg)
        return {'ok':True,'note':'OpenAI вернул структурированный ответ на синтетический пример. Сообщения в Telegram не отправлялись.'}

    async def resolve(self,lead):
        expected=lead.get('sender_id')
        if not expected or expected<=0:raise HTTPException(409,'У сообщения нет подтверждённого личного автора. Отправка в исходную группу не выполняется.')
        client=self.runtime.client;entity=None
        try:entity=await client.get_entity(expected)
        except (ValueError,errors.PeerIdInvalidError):pass
        if entity is None:
            msg=await client.get_messages(lead['chat_id'],ids=lead['message_id'])
            if msg:entity=await msg.get_sender()
        if not isinstance(entity,types.User) or entity.id!=expected or entity.id==self.account() or entity.bot or entity.deleted:
            raise HTTPException(409,'Не удалось подтвердить личного адресата. Нельзя писать каналу, боту, себе или другому пользователю.')
        peer=await client.get_input_entity(entity)
        if not isinstance(peer,types.InputPeerUser):raise HTTPException(409,'Личный адресат недоступен.')
        return entity,peer

    async def open_lead(self,spec:OpenLead):
        async with self.lock:
            account=self.account();rows=self.store.rows('SELECT l.*,s.title source_title FROM leads l LEFT JOIN sources s ON s.id=l.chat_id WHERE l.id=?',(spec.lead_id,))
            if not rows:raise HTTPException(404,'Лид не найден.')
            lead=rows[0];matches=json.loads(lead['matches']);rule_id=spec.rule_id or matches[0]['id']
            if rule_id not in [m['id'] for m in matches]:raise HTTPException(400,'Тема не принадлежит этому лиду.')
            self.rule(rule_id)
            try:entity,peer=await asyncio.wait_for(self.resolve(lead),20)
            except HTTPException:raise
            except Exception as exc:raise HTTPException(409,error_text(exc)) from None
            self.store.put('conversation_last_account',account)
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO conversations(account_id,peer_id,access_hash,title,username,lead_id,rule_id,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',
                                (account,entity.id,str(peer.access_hash),entity.first_name or entity.username or str(entity.id),entity.username,lead['id'],rule_id,now(),now()))
                cid=self.db.execute('SELECT id FROM conversations WHERE account_id=? AND peer_id=?',(account,entity.id)).fetchone()[0]
                self.db.execute('INSERT OR IGNORE INTO conversation_leads VALUES(?,?)',(cid,lead['id']))
            c=self.get(cid)
            if c['last_sync'] is None:
                try:await self.sync_one(c,initial=True)
                except Exception as exc:
                    with self.db:self.db.execute('UPDATE conversations SET error=? WHERE id=?',('Не удалось загрузить историю: '+error_text(exc),cid))
            return await self.detail(cid)

    def public(self,c):
        c={k:v for k,v in c.items() if k!='access_hash'}
        c['facts']=json.loads(c['facts']);c['handoff']=json.loads(c['handoff'])
        c['unread']=self.db.execute('SELECT COUNT(*) FROM conversation_messages WHERE cid=? AND unread=1',(c['id'],)).fetchone()[0]
        return c
    async def listing(self,offset:int=0):
        if offset<0:raise HTTPException(400,'Некорректная страница.')
        account=self.account(False)
        rows=self.store.rows('SELECT * FROM conversations WHERE account_id=? ORDER BY updated DESC,id DESC LIMIT 100 OFFSET ?',(account,offset))
        return {'conversations':[self.public(c) for c in rows],'total':self.db.execute('SELECT COUNT(*) FROM conversations WHERE account_id=?',(account,)).fetchone()[0],
                'unread':self.db.execute('SELECT COUNT(*) FROM conversation_messages m JOIN conversations c ON c.id=m.cid WHERE c.account_id=? AND m.unread=1',(account,)).fetchone()[0],
                'open_errors':self.store.rows('SELECT lead_id,error FROM conversation_open_errors WHERE account_id=? LIMIT 100',(account,)),'account_id':account,'paused':self.config()['paused'],'error':self.store.get('conversation_worker_error')}
    async def detail(self,cid:int,before:int=0):
        c=self.get(cid);lead=self.store.rows('SELECT l.*,s.title source_title FROM leads l LEFT JOIN sources s ON s.id=l.chat_id WHERE l.id=?',(c['lead_id'],))[0]
        messages=self.store.rows('SELECT id,tg_id,direction,kind,state,text,created,unread,error FROM conversation_messages WHERE cid=? AND (?=0 OR id<?) ORDER BY id DESC LIMIT 100',(cid,before,before))
        topic=self.topic(c['rule_id']);rule=self.rule(c['rule_id'])
        draft=topic['first_text'].replace('{company}',topic['company'] or '[название компании]').replace('{source}',lead['source_title'] or 'Telegram').replace('{topic}',rule['name'])
        return {'conversation':self.public(c),'messages':messages[::-1],'older_before':messages[-1]['id'] if len(messages)==100 else None,'lead':{k:lead[k] for k in ('id','text','author','link','source_title')},'linked_leads':[x['lead_id'] for x in self.store.rows('SELECT lead_id FROM conversation_leads WHERE cid=?',(cid,))],
                'sender':getattr(self.runtime.me,'username',None) or getattr(self.runtime.me,'first_name',None),'connected':self.runtime.me is not None,'draft':draft,'topic':rule['name'],'ai_ready':self.ready(self.config(),topic,c['account_id'])}

    def enqueue(self,c,text,key,kind,trigger=None):
        if c['stopped']:raise HTTPException(409,'Собеседник отказался от сообщений. Отправка остановлена.')
        row=self.store.rows('SELECT * FROM conversation_messages WHERE cid=? AND request_key=?',(c['id'],key))
        if row:
            if row[0]['text']!=text:raise HTTPException(409,'Этот идентификатор уже использован для другого текста.')
            return row[0]['id']
        if self.db.execute("SELECT 1 FROM conversation_messages WHERE cid=? AND state IN ('sending','uncertain')",(c['id'],)).fetchone():raise HTTPException(409,'Сначала проверьте незавершённую отправку в Telegram.')
        with self.db:
            cur=self.db.execute("INSERT INTO conversation_messages(cid,random_id,request_key,direction,kind,state,text,created,trigger_id) VALUES(?,?,?,'outgoing',?,'pending',?,?,?)",(c['id'],secrets.randbits(63),key,kind,text,now(),trigger))
            self.db.execute('UPDATE conversations SET updated=? WHERE id=?',(now(),c['id']))
        return cur.lastrowid
    async def send(self,cid:int,spec:Send):
        async with self.lock:
            c=self.get(cid,True)
            if not spec.confirmed or spec.account_id!=c['account_id'] or spec.peer_id!=c['peer_id']:raise HTTPException(409,'Подтвердите показанные аккаунт и личного получателя.')
            text=spec.text.strip()
            if not text:raise HTTPException(400,'Введите текст сообщения.')
            if '[название компании]' in text or re.search(r'\{(?:company|source|topic)\}',text):raise HTTPException(400,'Заполните название компании и подстановки в тексте перед отправкой.')
            self.pause(cid,None,'working')
            mid=self.enqueue(c,text,spec.request_key,'manual')
        return {'id':mid,'queued':True}
    async def control(self,cid:int,spec:Control):
        async with self.lock:
            c=self.get(cid,True);rule_id=spec.rule_id or c['rule_id'];self.rule(rule_id)
            if spec.mode=='ai' and (c['stopped'] or spec.status in ('hot','closed') or not self.ready(self.config(),self.topic(rule_id),c['account_id'])):raise HTTPException(400,'Для ИИ нужны проверенные ключ, модель, сведения об услуге и включённая автоматика. Отказавшимся и переданным менеджеру ИИ не пишет.')
            if spec.mode=='ai' and self.db.execute("SELECT 1 FROM conversation_messages WHERE cid=? AND state IN ('pending','sending','uncertain')",(cid,)).fetchone():raise HTTPException(409,'Дождитесь отправки или проверьте её неопределённый результат.')
            if spec.status=='hot' and c['status']!='hot':raise HTTPException(400,'Статус заинтересованного формируется из подтверждённого интереса и готовности к связи; сейчас используйте «В работе».')
            self.pause(cid,None,spec.status)
            with self.db:self.db.execute('UPDATE conversations SET mode=?,rule_id=?,ai_after=? WHERE id=?',(spec.mode,rule_id,time.time()+self.config()['delay_seconds'],cid))
            if spec.status=='closed':
                with self.db:self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Диалог закрыт менеджером.' WHERE cid=? AND state='pending'",(cid,))
        return await self.detail(cid)
    async def read(self,cid:int,spec:Read):
        self.get(cid)
        with self.db:self.db.execute('UPDATE conversation_messages SET unread=0 WHERE cid=? AND id<=?',(cid,spec.through_id))
        return {'ok':True}
    async def sync_endpoint(self,cid:int):
        async with self.lock:
            c=self.get(cid,True)
            try:await self.sync_one(c)
            except Exception as exc:raise HTTPException(409,error_text(exc)) from None
        return await self.detail(cid)

    async def telegram_evidence(self,cid:int,mid:int):
        """Read constructor metadata for one already stored Telegram message.

        No history ingest, mark-read, retry, content or credentials are exposed.
        The local message ID fixes the Telegram ID and conversation recipient.
        """
        c=self.get(cid,True);account=c['account_id'];client=self.runtime.client
        row=self.store.rows('SELECT tg_id FROM conversation_messages WHERE cid=? AND id=?',(cid,mid))
        if not row:raise HTTPException(404,'Сообщение не найдено в этом диалоге.')
        tg_id=row[0]['tg_id']
        if type(tg_id) is not int or tg_id<=0:raise HTTPException(409,'У записи ещё нет подтверждённого ID Telegram.')
        if self.runtime.client is not client or self.account()!=account:raise HTTPException(409,'Telegram-аккаунт изменился.')
        peer=types.InputPeerUser(c['peer_id'],int(c['access_hash']))
        try:message=await asyncio.wait_for(client.get_messages(peer,ids=tg_id),20)
        except asyncio.TimeoutError:raise HTTPException(504,'Telegram не ответил на проверку записи. Автоматического повтора нет.') from None
        except Exception as exc:raise HTTPException(409,error_text(exc)) from None
        if self.runtime.client is not client or self.account()!=account:raise HTTPException(409,'Telegram-аккаунт изменился во время проверки.')
        if message is None:raise HTTPException(404,'Telegram не вернул эту запись; она могла быть удалена.')
        if not isinstance(message,(types.Message,types.MessageService,types.MessageEmpty)) or message.id!=tg_id:
            raise HTTPException(409,'Telegram вернул другую запись. Результат проверки не принят.')
        message_peer=getattr(message,'peer_id',None)
        is_empty=isinstance(message,types.MessageEmpty)
        if not (is_empty and message_peer is None) and (not isinstance(message_peer,types.PeerUser) or message_peer.user_id!=c['peer_id']):
            raise HTTPException(409,'Адресат записи Telegram не совпадает с диалогом.')
        sender=getattr(message,'from_id',None);text=getattr(message,'message',None)
        action=getattr(message,'action',None);media=getattr(message,'media',None);date=getattr(message,'date',None)
        return {'constructor':type(message).__name__,'action_constructor':type(action).__name__ if action is not None else None,
                'id':message.id,'out':getattr(message,'out',None),'peer_type':type(message_peer).__name__ if message_peer is not None else None,
                'peer_id':getattr(message_peer,'user_id',None),'from_type':type(sender).__name__ if sender is not None else None,
                'from_id':next((getattr(sender,k) for k in ('user_id','chat_id','channel_id') if hasattr(sender,k)),None),
                'date':date.isoformat() if date is not None else None,'media_constructor':type(media).__name__ if media is not None else None,
                'has_text':bool(text),'text_len':len(text) if isinstance(text,str) else 0,
                'is_service':isinstance(message,types.MessageService),'is_empty':is_empty}

    async def retry(self,cid:int,mid:int,spec:Retry):
        async with self.lock:
            c=self.get(cid,True)
            if not spec.confirmed or c['stopped']:raise HTTPException(400,'Повтор не подтверждён или получатель отказался.')
            row=self.store.rows('SELECT * FROM conversation_messages WHERE id=? AND cid=?',(mid,cid))
            if not row or row[0]['state']!='failed':raise HTTPException(409,'Повтор возможен только после подтверждённого отказа Telegram. Неопределённые отправки не повторяются.')
            self.pause(cid,None,'working')
            with self.db:self.db.execute("UPDATE conversation_messages SET state='pending',kind='manual',error=NULL,next_at=0 WHERE id=?",(mid,))
        return {'ok':True}

    async def ingest(self,c,message,initial=False):
        # Service actions are Telegram events, not customer/manager replies.
        # Empty history entries also have no user content (or even a peer).
        # sync_one still advances its cursor after these entries are ignored.
        if isinstance(message,(types.MessageService,types.MessageEmpty)):return
        if not isinstance(message.peer_id,types.PeerUser) or message.peer_id.user_id!=c['peer_id']:return
        incoming=not message.out
        if incoming and message.sender_id!=c['peer_id']:return
        text=message.message or '[Медиа без текста — требуется менеджер]';cid=c['id']
        existing=self.store.rows('SELECT * FROM conversation_messages WHERE cid=? AND tg_id=?',(cid,message.id))
        if existing:
            if existing[0]['text']!=text:
                with self.db:self.db.execute('UPDATE conversation_messages SET text=? WHERE id=?',(text,existing[0]['id']))
                if not initial:
                    if incoming and STOP.search(text):self.stop(cid)
                    else:self.pause(cid,'Сообщение изменено. Проверьте диалог перед продолжением ИИ.')
            return
        # An outgoing Telegram update can arrive before SendMessageRequest returns.
        pending=self.store.rows("SELECT * FROM conversation_messages WHERE cid=? AND state='sending' AND text=?",(cid,text)) if not incoming else []
        if len(pending)==1:
            with self.db:
                self.db.execute("UPDATE conversation_messages SET tg_id=?,state='sent',error=NULL WHERE id=?",(message.id,pending[0]['id']))
            return
        with self.db:
            cur=self.db.execute('INSERT OR IGNORE INTO conversation_messages(cid,tg_id,direction,kind,state,text,created,unread) VALUES(?,?,?,?,?,?,?,?)',(cid,message.id,'incoming' if incoming else 'outgoing','customer' if incoming else 'external','received' if incoming else 'sent',text,message.date.isoformat(),int(incoming and not initial)))
            if not cur.rowcount:return
            local_id=cur.lastrowid
            self.db.execute('UPDATE conversations SET updated=?,version=version+1,ai_after=? WHERE id=?',(now(),time.time()+self.config()['delay_seconds'],cid))
            if initial:self.db.execute('UPDATE conversations SET ai_cursor=MAX(ai_cursor,?) WHERE id=?',(local_id,cid))
            else:self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Получено новое сообщение; предыдущий автоответ отменён.' WHERE cid=? AND kind='ai' AND state='pending'",(cid,))
        if not initial and incoming and STOP.search(text):
            self.stop(cid)
        elif not initial and (not incoming or not message.message):self.pause(cid,'Ответ отправлен вне панели.' if not incoming else 'Получено медиа без текста. Ответьте вручную.','working')

    async def on_event(self,event):
        if not getattr(event,'is_private',False) or not self.runtime.me:return
        for c in self.store.rows('SELECT * FROM conversations WHERE account_id=? AND peer_id=?',(self.runtime.me.id,event.chat_id)):
            await self.ingest(c,event.message)

    async def sync_one(self,c,initial=False):
        client=self.runtime.client
        if c['account_id']!=self.account():raise HTTPException(409,'Telegram-аккаунт изменился.')
        peer=types.InputPeerUser(c['peer_id'],int(c['access_hash']))
        async def accept(message):
            if self.runtime.client is not client or c['account_id']!=self.account():raise HTTPException(409,'Telegram-аккаунт изменился.')
            await self.ingest(c,message,initial=initial)
            with self.db:self.db.execute('UPDATE conversations SET cursor=MAX(cursor,?) WHERE id=?',(message.id,c['id']))
        if initial:
            messages=await client.get_messages(peer,limit=30)
            for message in reversed(messages):await accept(message)
        else:
            async for message in client.iter_messages(peer,min_id=c['cursor'],reverse=True,limit=200):await accept(message)
        with self.db:self.db.execute('UPDATE conversations SET last_sync=? WHERE id=?',(now(),c['id']))

    def sent_today(self,account):
        return self.db.execute("SELECT COUNT(*) FROM conversation_messages m JOIN conversations c ON c.id=m.cid WHERE c.account_id=? AND m.direction='outgoing' AND m.kind!='external' AND m.state IN ('sending','sent','uncertain') AND substr(COALESCE(m.attempted_at,m.created),1,10)=?",(account,now()[:10])).fetchone()[0]
    async def deliver_one(self):
        async with self.lock:
            account=self.account(False)
            if not self.runtime.me or not self.outbound_allowed() or time.time()<self.store.get('conversation_not_before',0):return
            rows=self.store.rows("SELECT m.* FROM conversation_messages m JOIN conversations c ON c.id=m.cid WHERE c.account_id=? AND m.state='pending' AND m.next_at<=? ORDER BY m.id LIMIT 1",(account,time.time()))
            if not rows:return
            m=rows[0];c=self.get(m['cid'],True);cfg=self.config()
            if c['stopped'] or (m['kind']=='ai' and (c['mode']!='ai' or not self.ready(cfg,self.topic(c['rule_id']),account))):
                with self.db:self.db.execute("UPDATE conversation_messages SET state='cancelled',error='Автоматика или диалог остановлены.' WHERE id=?",(m['id'],))
                return
            if self.sent_today(account)>=cfg['daily_messages']:
                with self.db:self.db.execute("UPDATE conversation_messages SET state='failed',error='Дневной лимит исходящих сообщений исчерпан; проверьте очередь перед повтором.' WHERE id=?",(m['id'],))
                return
            try:
                peer=types.InputPeerUser(c['peer_id'],int(c['access_hash']))
                entity=await asyncio.wait_for(self.runtime.client.get_entity(peer),15)
                if not isinstance(entity,types.User) or entity.id!=c['peer_id'] or entity.bot or entity.deleted:raise ValueError('recipient not verified')
                # Recheck after network await: incoming refusal can cancel the pending row.
                current=self.get(c['id'],True)
                if current['stopped'] or not self.db.execute("SELECT 1 FROM conversation_messages WHERE id=? AND state='pending'",(m['id'],)).fetchone():return
                if not self.outbound_allowed():return
                with self.db:self.db.execute("UPDATE conversation_messages SET state='sending',attempted_at=? WHERE id=?",(now(),m['id']))
                result=await asyncio.wait_for(self.runtime.client(functions.messages.SendMessageRequest(peer=peer,message=m['text'],random_id=m['random_id'],no_webpage=True)),25)
                tg_id=getattr(result,'id',None)
                for u in getattr(result,'updates',[]):
                    if isinstance(u,types.UpdateMessageID) and u.random_id==m['random_id']:tg_id=u.id
                saved=self.store.rows('SELECT state,tg_id FROM conversation_messages WHERE id=?',(m['id'],))[0]
                # The outgoing event can arrive before the RPC response and already provide the receipt.
                if saved['state']=='sent' and type(saved['tg_id']) is int and saved['tg_id']>0:return
                if type(tg_id) is int and tg_id>0:
                    with self.db:self.db.execute("UPDATE conversation_messages SET state='sent',tg_id=COALESCE(tg_id,?),error=NULL WHERE id=?",(tg_id,m['id']))
                else:
                    with self.db:self.db.execute("UPDATE conversation_messages SET state='uncertain',error='Telegram не вернул ID сообщения. Проверьте личный диалог; автоматического повтора нет.' WHERE id=?",(m['id'],))
                    self.pause(c['id'],'Telegram не подтвердил ID отправленного сообщения. Проверьте переписку вручную.')
            except errors.FloodWaitError as exc:
                self.store.put('conversation_not_before',time.time()+exc.seconds+1)
                with self.db:self.db.execute("UPDATE conversation_messages SET state='pending',next_at=?,error=? WHERE id=?",(time.time()+exc.seconds+1,error_text(exc),m['id']))
            except Exception as exc:
                if isinstance(exc,(errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError)):
                    self.stop_outbound(exc)
                # Server failures/timeouts can arrive after Telegram applied the write.
                row=self.store.rows('SELECT state FROM conversation_messages WHERE id=?',(m['id'],))[0]
                if row['state']=='sent':return
                known_rejection=isinstance(exc,errors.RPCError) and not isinstance(exc,(errors.ServerError,errors.TimedOutError))
                state='failed' if known_rejection or row['state']=='pending' else 'uncertain'
                with self.db:self.db.execute('UPDATE conversation_messages SET state=?,error=? WHERE id=?',(state,error_text(exc) if state=='failed' else 'Результат отправки неизвестен. Проверьте личный диалог в Telegram. Автоповтора нет.',m['id']))
                self.pause(c['id'],error_text(exc))
                if isinstance(exc,errors.PeerFloodError):cfg['paused']=True;self.store.put('conversation_config',cfg)

    async def qualify_one(self):
        cfg=self.config();account=self.account(False)
        if not self.runtime.me or not self.outbound_allowed() or cfg['paused'] or not cfg['auto_reply']:return
        candidates=self.store.rows("SELECT * FROM conversations WHERE account_id=? AND mode='ai' AND stopped=0 AND status NOT IN ('hot','closed') AND ai_after<=? ORDER BY updated LIMIT 100",(account,time.time()))
        for c in candidates:
            topic=self.topic(c['rule_id'])
            if not self.ready(cfg,topic,account):continue
            if self.db.execute("SELECT 1 FROM conversation_messages WHERE cid=? AND state IN ('pending','sending','uncertain')",(c['id'],)).fetchone():continue
            incoming=self.store.rows("SELECT * FROM conversation_messages WHERE cid=? AND direction='incoming' ORDER BY id DESC LIMIT 30",(c['id'],))
            if not incoming or incoming[0]['id']<=c['ai_cursor']:continue
            trigger=incoming[0]['id'];version=c['version'];config_version=self.store.get('conversation_config_version',0)
            if self.db.execute("SELECT COUNT(*) FROM conversation_messages WHERE cid=? AND kind='ai' AND state='sent'",(c['id'],)).fetchone()[0]>=min(cfg['max_ai_replies'],topic['max_replies']):
                self.pause(c['id'],'Лимит ответов ИИ достигнут. Продолжите вручную.','working');continue
            lead=self.store.rows('SELECT text FROM leads WHERE id=?',(c['lead_id'],))[0]['text']
            messages=self.store.rows('SELECT direction,text FROM conversation_messages WHERE cid=? AND state IN (\'sent\',\'received\') ORDER BY julianday(created) DESC,id DESC LIMIT 30',(c['id'],))[::-1]
            try:
                self.reserve_call()
                bounded=[{**m,'text':m['text'][:2000]} for m in messages[-20:]]
                decision,usage=await self.provider(self.store.get('conversation_api_key'),cfg['model'],{'operator_configuration':topic,'lead':lead[:5000],'dialogue':bounded,'previous_facts':json.loads(c['facts'])})
                facts,hot,reply=validate_decision(decision,lead,[x['text'] for x in incoming],json.loads(c['facts']),topic['facts'])
                async with self.lock:
                    current=self.get(c['id'],True)
                    if current['version']!=version or current['mode']!='ai' or config_version!=self.store.get('conversation_config_version',0):return
                    handoff={}
                    if decision.action=='handoff':
                        handoff={'hot':hot,'interest_quote':decision.interest_quote if hot else '', 'contact_quote':decision.contact_quote if hot else '',
                                 'summary':'\n'.join(FIELDS[k]+': '+v['value'] for k,v in facts.items()),'unknown':[label for key,label in FIELDS.items() if key not in facts],
                                 'next_action':decision.next_action[:1000] or 'Менеджеру продолжить в этом диалоге.', 'created':now()}
                    with self.db:
                        self.db.execute('UPDATE conversations SET facts=?,handoff=?,ai_cursor=?,error=NULL,updated=? WHERE id=?',(json.dumps(facts,ensure_ascii=False),json.dumps(handoff,ensure_ascii=False),trigger,now(),c['id']))
                        self.db.execute('UPDATE conversation_usage SET input_tokens=input_tokens+?,output_tokens=output_tokens+? WHERE day=?',(int(usage.get('input_tokens',0)),int(usage.get('output_tokens',0)),now()[:10]))
                    if decision.action=='stop':
                        self.pause(c['id'],'ИИ распознал отказ. Автоматические сообщения прекращены.','closed')
                        with self.db:self.db.execute('UPDATE conversations SET stopped=1 WHERE id=?',(c['id'],))
                    elif decision.action=='handoff':self.pause(c['id'],None,'hot' if hot else 'working')
                    elif reply:self.enqueue(current,reply,'ai_'+str(trigger),'ai',trigger)
                    else:self.pause(c['id'],'ИИ не предложил ответ. Требуется менеджер.','working')
            except (ProviderError,HTTPException) as exc:self.pause(c['id'],str(exc) if isinstance(exc,ProviderError) else 'Аккаунт или настройки изменились.','working')
            except Exception:self.pause(c['id'],'Ошибка обработки ответа ИИ. Продолжите вручную.','working')
            return

    async def first_contact_allowed(self,lead,rule):
        return True

    async def initiate_one(self):
        cfg=self.config();account=self.account(False)
        if not self.outbound_allowed():return
        if not self.runtime.me or cfg['paused'] or not cfg['auto_first'] or self.sent_today(account)>=cfg['daily_messages']:return
        for lead in self.store.rows("SELECT * FROM leads WHERE origin='live' AND status='new' AND found>? AND julianday(created)>julianday('now','-1 day') ORDER BY id DESC LIMIT 100",(cfg.get('first_since',now()),)):
            if not lead['sender_id'] or lead['sender_id']<=0:continue
            if self.db.execute('SELECT 1 FROM conversations WHERE account_id=? AND peer_id=?',(account,lead['sender_id'])).fetchone():continue
            if self.db.execute('SELECT 1 FROM conversation_open_errors WHERE account_id=? AND lead_id=?',(account,lead['id'])).fetchone():continue
            for match in json.loads(lead['matches']):
                topic=self.topic(match['id'])
                rule=self.rule(match['id'])
                if not rule['enabled'] or not matches(lead['text'],rule):continue
                if not topic['auto_first'] or not self.ready(cfg,topic,account):continue
                try:
                    if not await self.first_contact_allowed(lead,rule):continue
                    detail=await self.open_lead(OpenLead(lead_id=lead['id'],rule_id=match['id']));c=self.get(detail['conversation']['id'],True)
                    if not c['last_sync'] or c['error']:
                        self.pause(c['id'],'Сначала проверьте доступность личной истории. Первый контакт не отправлен.');return
                    # Existing private history must be reviewed by a human before outreach.
                    if detail['messages']:
                        self.pause(c['id'],'Уже есть личная переписка. Проверьте её перед обращением.');return
                    async with self.lock:
                        if not self.ready(self.config(),self.topic(match['id']),account) or not self.config()['auto_first']:return
                        with self.db:self.db.execute("UPDATE conversations SET mode='ai',status='working' WHERE id=?",(c['id'],))
                        self.enqueue(c,detail['draft'],'auto_first_'+str(lead['id']),'ai')
                except Exception as exc:
                    with self.db:self.db.execute('INSERT OR REPLACE INTO conversation_open_errors VALUES(?,?,?)',(account,lead['id'],exc.detail if isinstance(exc,HTTPException) else error_text(exc)))
                return

    async def loop(self):
        while True:
            try:
                if self.runtime.me and self.runtime.client.is_connected():
                    if time.time()-self.last_poll>=30:
                        self.last_poll=time.time();cs=self.store.rows('SELECT * FROM conversations WHERE account_id=? ORDER BY id',(self.account(),))
                        for i in range(min(5,len(cs))):
                            c=cs[(self.poll_position+i)%len(cs)]
                            try:await self.sync_one(c)
                            except errors.FloodWaitError as exc:
                                self.last_poll=time.time()+exc.seconds;break
                            except Exception as exc:
                                with self.db:self.db.execute('UPDATE conversations SET error=? WHERE id=?',(error_text(exc),c['id']))
                        self.poll_position+=5
                    await self.deliver_one();await self.qualify_one();await self.initiate_one()
                if self.store.get('conversation_worker_error') is not None:self.store.put('conversation_worker_error',None)
            except asyncio.CancelledError:raise
            except Exception as exc:self.store.put('conversation_worker_error',error_text(exc))
            await asyncio.sleep(3)
    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
