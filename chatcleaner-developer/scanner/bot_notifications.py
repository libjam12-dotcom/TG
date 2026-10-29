"""Client lead cards via Bot API. No webhook/getUpdates and no automatic double delivery."""
import asyncio,contextlib,json,re,time,logging
from typing import Literal
from urllib.parse import urlsplit
import httpx
from fastapi import APIRouter,HTTPException
from pydantic import BaseModel,Field
from engine import now

# httpx normally logs full request URLs, which contain Bot API credentials.
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
ORIGIN=__import__('os').environ.get('PANEL_ORIGIN','http://127.0.0.1:8080')
def clip_utf16(text,limit):
    return str(text).encode('utf-16-le')[:limit*2].decode('utf-16-le',errors='ignore')
class BotConfig(BaseModel):
    token:str=Field(default='',max_length=160)
    reference:str=Field(min_length=1,max_length=64)
    thread_id:int|None=Field(default=None,ge=1,le=2147483647)
    rules:list[int]=Field(default_factory=list,max_length=25)
    sources:list[int]=Field(default_factory=list,max_length=100)
class Mode(BaseModel):
    mode:Literal['account','bot','panel']
    chat_id:int|None=Field(default=None,le=-1)
    revision:int|None=Field(default=None,ge=1)
class Test(BaseModel):
    chat_id:int|None=Field(default=None,le=-1)
    revision:int|None=Field(default=None,ge=1)
    request_key:str=Field(min_length=16,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')
    confirmed:Literal[True]
class Retry(BaseModel):
    confirmed:Literal[True]
class APIError(Exception):
    def __init__(self,code=0,retry_after=0,uncertain=False):
        self.code,self.retry_after,self.uncertain=code,retry_after,uncertain
    @property
    def message(self):
        if self.uncertain:return 'Ответ Telegram не получен. Отправка могла состояться: проверьте чат, автоматического повтора нет.'
        if self.code==429:return 'Telegram ограничил частоту. Следующая попытка после указанного времени.'
        if self.code==401:return 'Токен бота недействителен или отозван.'
        if self.code==403:return 'Бот удалён из группы или ему запрещена отправка. Проверьте его права.'
        if self.code==400:return 'Telegram отклонил запрос. Проверьте ID группы, ID темы и доступ бота.'
        return 'Не удалось подключиться к Telegram Bot API.'

class BotNotifications:
    def __init__(self,store,lock=None,transport=None):
        self._store=store;self.lock=lock or asyncio.Lock();self.transport=transport;self.task=None
        self.router=APIRouter(prefix='/api/scanner/bot-notifications')
        for path,fn,methods in [('',self.state,['GET']),('/config',self.configure,['POST']),('/mode',self.mode,['POST']),('/test',self.test,['POST']),('/{delivery_id}/retry',self.retry,['POST'])]:self.router.add_api_route(path,fn,methods=methods)
    @property
    def store(self):return self._store() if callable(self._store) else self._store
    def initialize(self):
        self.store.db.executescript('''CREATE TABLE IF NOT EXISTS bot_deliveries(
          id TEXT PRIMARY KEY,lead_id INTEGER UNIQUE REFERENCES leads(id),chat_id INTEGER NOT NULL,thread_id INTEGER,
          revision INTEGER NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
          next_at REAL NOT NULL DEFAULT 0,error TEXT,message_id INTEGER,created TEXT NOT NULL,updated TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS bot_delivery_queue ON bot_deliveries(state,next_at);''')
        with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='uncertain',error='Перезапуск во время отправки: проверьте чат, автоматического повтора нет.',updated=? WHERE state='sending'",(now(),))
        self.store.notification_hook=self.enqueue
    async def call(self,token,method,payload):
        # Bot API requires a credential in the HTTPS path. This path is never
        # returned to the panel, stored in a lead/link, or included in errors/logs.
        assert method in ('getMe','getChat','getChatMember','sendMessage')
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20,connect=8),transport=self.transport,follow_redirects=False) as c:
                response=await c.post('https://api.telegram.org/bot'+token+'/'+method,json=payload)
            try:data=response.json()
            except (ValueError,TypeError):raise APIError(uncertain=method=='sendMessage') from None
            if response.status_code>=500:raise APIError(uncertain=method=='sendMessage')
            if not isinstance(data,dict) or not data.get('ok'):
                code=data.get('error_code',response.status_code) if isinstance(data,dict) else 0
                delay=(data.get('parameters') or {}).get('retry_after',0) if isinstance(data,dict) else 0
                raise APIError(code,int(delay) if isinstance(delay,(int,float)) else 0)
            return data['result']
        except APIError:raise
        except (httpx.ConnectTimeout,httpx.ConnectError):raise APIError() from None
        except Exception:raise APIError(uncertain=method=='sendMessage') from None
    def config(self):return self.store.get('notification_bot_config') or {}
    def delivery_mode(self):return self.store.get('notification_mode','account')
    async def state(self):
        cfg=self.config();rows=self.store.rows('SELECT id,lead_id,state,chat_id,thread_id,attempts,next_at,error,message_id,created,updated FROM bot_deliveries ORDER BY created DESC LIMIT 50')
        return {'mode':self.delivery_mode(),'configured':bool(self.store.get('notification_bot_token') and cfg.get('chat_id')),
          'token_saved':bool(self.store.get('notification_bot_token')),'bot_username':cfg.get('bot_username'),
          'revision':cfg.get('revision',0),'chat_id':cfg.get('chat_id'),'chat_title':cfg.get('chat_title'),'thread_id':cfg.get('thread_id'),
          'rules':cfg.get('rules',[]),'sources':cfg.get('sources',[]),'checked_at':cfg.get('checked_at'),
          'topic_check_note':'Тема форума проверяется тестовой отправкой.' if cfg.get('thread_id') else None,
          'pending':self.store.db.execute("SELECT COUNT(*) FROM bot_deliveries WHERE state='pending'").fetchone()[0],
          'deliveries':rows,'error':self.store.get('notification_bot_error')}
    async def configure(self,spec:BotConfig):
        token=spec.token.strip() or self.store.get('notification_bot_token')
        if not token or not re.fullmatch(r'\d{5,16}:[A-Za-z0-9_-]{20,120}',token):raise HTTPException(400,'Введите токен отдельного бота из BotFather.')
        ref=spec.reference.strip()
        if re.fullmatch(r'-[1-9]\d{0,15}',ref):ref=int(ref)
        elif not re.fullmatch(r'@[A-Za-z][A-Za-z0-9_]{3,31}',ref):raise HTTPException(400,'Выберите группу аккаунта, укажите её отрицательный ID или @username.')
        if not set(spec.rules)<={r['id'] for r in self.store.rules()} or not set(spec.sources)<={r['id'] for r in self.store.rows('SELECT id FROM sources')}:raise HTTPException(400,'Выберите существующие тематики и источники.')
        async with self.lock:
            try:
                me=await self.call(token,'getMe',{})
                if not me.get('is_bot'):raise HTTPException(400,'Токен не относится к боту.')
                if me.get('username','').lower()=='developmentdemo_bot':raise HTTPException(400,'Создайте отдельного бота уведомлений, чтобы настройки антиспам-бота оставались отдельными.')
                chat=await self.call(token,'getChat',{'chat_id':ref})
                if chat.get('type') not in ('group','supergroup') or type(chat.get('id')) is not int or chat['id']>=0:raise HTTPException(400,'Получателем должна быть группа или супергруппа.')
                if self.store.db.execute('SELECT 1 FROM sources WHERE id=? AND enabled=1',(chat['id'],)).fetchone():raise HTTPException(400,'Группа уведомлений не должна быть активным источником лидсканера.')
                member=await self.call(token,'getChatMember',{'chat_id':chat['id'],'user_id':me['id']})
                status=member.get('status')
                if status in ('left','kicked') or status not in ('creator','administrator','member','restricted'):raise HTTPException(400,'Добавьте бота в выбранную группу.')
                if status=='restricted' and (not member.get('is_member') or not member.get('can_send_messages')):raise HTTPException(400,'Разрешите боту отправлять сообщения в группе.')
                if status=='member' and chat.get('permissions',{}).get('can_send_messages') is False:raise HTTPException(400,'В группе запрещена отправка обычным участникам. Разрешите её боту.')
                if spec.thread_id and not chat.get('is_forum'):raise HTTPException(400,'ID темы применяется только к группе с форумом.')
            except APIError as exc:raise HTTPException(400,exc.message) from None
            old=self.config();cfg={'bot_id':me['id'],'bot_username':me.get('username'),'chat_id':chat['id'],
                'chat_title':str(chat.get('title','Группа'))[:200],'thread_id':spec.thread_id,'rules':list(dict.fromkeys(spec.rules)),
                'sources':list(dict.fromkeys(spec.sources)),'checked_at':now(),'revision':old.get('revision',0)+1}
            with self.store.db:
                self.store.put('notification_bot_token',token);self.store.put('notification_bot_config',cfg);self.store.put('notification_bot_error',None)
                # New connection/settings are staged. User explicitly enables delivery.
                if self.delivery_mode()=='bot':self.store.put('notification_mode','panel')
                self.store.db.execute("UPDATE bot_deliveries SET state='cancelled',updated=? WHERE state='pending'",(now(),))
        return {'ok':True,'note':'Бот и группа проверены. Отправки не было. Проверьте тест и отдельно включите доставку ботом.',**(await self.state())}
    def change_mode(self,value):
        if value=='bot' and not (self.store.get('notification_bot_token') and self.config().get('chat_id')):raise HTTPException(409,'Сначала сохраните и проверьте бота и группу.')
        with self.store.db:
            self.store.put('notification_mode',value)
            if value!='account':self.store.db.execute("UPDATE outbox SET state='cancelled' WHERE state='pending'")
            if value!='bot':self.store.db.execute("UPDATE bot_deliveries SET state='cancelled',updated=? WHERE state='pending' AND lead_id IS NOT NULL",(now(),))
    def check_review(self,spec):
        cfg=self.config()
        if (spec.chat_id is not None or spec.revision is not None) and (spec.chat_id!=cfg.get('chat_id') or spec.revision!=cfg.get('revision')):
            raise HTTPException(409,'Настройки получателя изменились. Обновите панель и подтвердите новую группу заново.')
    async def mode(self,spec:Mode):
        async with self.lock:
            if spec.mode=='bot':self.check_review(spec)
            self.change_mode(spec.mode)
        return await self.state()
    def card(self,lead):
        matches=json.loads(lead['matches']);titles=', '.join(m['name'] for m in matches)[:300]
        source=self.store.rows('SELECT title FROM sources WHERE id=?',(lead['chat_id'],));source=source[0]['title'] if source else str(lead['chat_id'])
        keys=', '.join(dict.fromkeys(k for m in matches for k in m.get('keywords',[])))[:350]
        text=f"Новый запрос · {titles}\nИсточник: {source[:200]}\nАвтор: {lead['author'][:150]}\nКлючи: {keys}\nОпубликовано: {lead['created']}\n\n"+clip_utf16(lead['text'],2300)
        if len(lead['text'])>2300:text+='\n\nПолный текст — в панели.'
        buttons=[];link=lead.get('link')
        if link and urlsplit(link).scheme=='https' and urlsplit(link).hostname=='t.me':buttons.append({'text':'Исходный запрос','url':link})
        buttons.append({'text':'Открыть находку в панели','url':ORIGIN+'/?lead='+str(lead['id'])+'#scanner'})
        return {'text':clip_utf16(text,3900),'link_preview_options':{'is_disabled':True},'reply_markup':{'inline_keyboard':[buttons]}}
    def enqueue(self,lead_id,origin):
        if origin!='live' or self.delivery_mode()!='bot':return
        cfg=self.config()
        if not cfg.get('chat_id'):return
        lead=self.store.rows('SELECT * FROM leads WHERE id=?',(lead_id,))[0]
        if cfg.get('sources') and lead['chat_id'] not in cfg['sources']:return
        if cfg.get('rules') and not set(cfg['rules']) & {m['id'] for m in json.loads(lead['matches'])}:return
        self.insert('lead-'+str(lead_id),lead_id,self.card(lead),cfg)
    def insert(self,id,lead_id,payload,cfg):
        self.store.db.execute('INSERT OR IGNORE INTO bot_deliveries(id,lead_id,chat_id,thread_id,revision,payload,created,updated) VALUES(?,?,?,?,?,?,?,?)',
            (id,lead_id,cfg['chat_id'],cfg.get('thread_id'),cfg['revision'],json.dumps(payload,ensure_ascii=False),now(),now()))
    async def test(self,spec:Test):
        async with self.lock:
            self.check_review(spec)
            cfg=self.config()
            if not self.store.get('notification_bot_token') or not cfg.get('chat_id'):raise HTTPException(409,'Сначала настройте бота и группу.')
            id='test-'+spec.request_key
            existing=self.store.rows('SELECT revision,chat_id,thread_id FROM bot_deliveries WHERE id=?',(id,))
            if existing and any(existing[0][key]!=cfg.get(key) for key in ('revision','chat_id','thread_id')):
                raise HTTPException(409,'Этот ключ теста относится к прежнему получателю. Обновите настройки и подтвердите новый тест с новым ключом.')
            with self.store.db:self.insert(id,None,{'text':'Проверка Chatcleaner: бот уведомлений подключён. Лиды будут приходить после включения доставки в панели.'},cfg)
            await self.send(id,explicit=True)
            return self.store.rows('SELECT id,state,message_id,error,next_at FROM bot_deliveries WHERE id=?',(id,))[0]
    async def retry(self,delivery_id:str,spec:Retry):
        async with self.lock:
            rows=self.store.rows('SELECT * FROM bot_deliveries WHERE id=?',(delivery_id,))
            if not rows:raise HTTPException(404,'Уведомление не найдено.')
            row=rows[0];cfg=self.config()
            if row['state']!='failed':raise HTTPException(409,'Повтор разрешён только после подтверждённого отказа Telegram. При неопределённом результате проверьте чат.')
            if row['revision']!=cfg.get('revision'):raise HTTPException(409,'Настройки получателя изменились. Старое уведомление не отправляется в новый чат.')
            with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='pending',error=NULL,next_at=0,attempts=0,updated=? WHERE id=?",(now(),delivery_id))
        return {'ok':True}
    async def send(self,id,explicit=False):
        row=self.store.rows('SELECT * FROM bot_deliveries WHERE id=?',(id,))[0];cfg=self.config()
        if row['state']!='pending' or row['next_at']>time.time() or self.store.get('notification_bot_not_before',0)>time.time():return
        if row['lead_id'] is not None and (self.delivery_mode()!='bot' or not self.store.get('running',False)):return
        if row['revision']!=cfg.get('revision') or row['chat_id']!=cfg.get('chat_id'):
            with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='cancelled',updated=? WHERE id=?",(now(),id))
            return
        lead=self.store.rows('SELECT status FROM leads WHERE id=?',(row['lead_id'],)) if row['lead_id'] else []
        if lead and lead[0]['status']=='rejected':
            with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='cancelled',updated=? WHERE id=?",(now(),id))
            return
        payload=json.loads(row['payload']);payload['chat_id']=row['chat_id']
        if row['thread_id']:payload['message_thread_id']=row['thread_id']
        attempts=row['attempts']+1
        with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='sending',attempts=?,updated=? WHERE id=?",(attempts,now(),id))
        try:
            result=await self.call(self.store.get('notification_bot_token'),'sendMessage',payload)
            if type(result.get('message_id')) is not int:raise APIError(uncertain=True)
            with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='sent',message_id=?,error=NULL,updated=? WHERE id=?",(result['message_id'],now(),id))
            self.store.put('notification_bot_error',None)
        except APIError as exc:
            state='uncertain' if exc.uncertain else 'pending' if (exc.code==429 or exc.code==0) and attempts<8 else 'failed'
            delay=max(1,exc.retry_after or 30*2**min(attempts,7))
            with self.store.db:
                self.store.db.execute('UPDATE bot_deliveries SET state=?,next_at=?,error=?,updated=? WHERE id=?',(state,time.time()+delay,exc.message,now(),id))
                self.store.put('notification_bot_error',exc.message)
                if exc.code==429:self.store.put('notification_bot_not_before',time.time()+delay)
    async def tick(self):
        async with self.lock:
            if self.store.get('notification_bot_not_before',0)>time.time():return
            where="" if self.delivery_mode()=='bot' and self.store.get('running',False) else ' AND lead_id IS NULL'
            row=self.store.rows("SELECT id FROM bot_deliveries WHERE state='pending' AND next_at<=?"+where+" ORDER BY created LIMIT 1",(time.time(),))
            if row:await self.send(row[0]['id'])
    async def loop(self):
        while True:
            try:await self.tick()
            except asyncio.CancelledError:raise
            except Exception:self.store.put('notification_bot_error','Сбой очереди уведомлений. Проверьте журнал доставки.')
            await asyncio.sleep(4)
    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
