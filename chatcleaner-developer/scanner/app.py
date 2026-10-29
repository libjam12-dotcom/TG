import asyncio
import contextlib
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
from functools import lru_cache

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from telethon import events, utils, errors, types, functions
from engine import Store, matches, explain, words, now
from topics import seed_topics
from directory import Directory
from history import HistoryRunner
from publisher import Publisher, MAX_PHOTO
from account_import import MAX_REQUEST
from conversations import Conversations
from client_contacts import annotate_client_contact
from source_folders import SourceFolders
from marketing import Marketing
from operations import Operations,ClientTelegramClient as TelegramClient
from bot_notifications import BotNotifications,Retry as BotRetry

os.umask(0o077)
DATA = Path(os.environ.get('SCANNER_DATA', '/var/lib/chatcleaner-scanner'))
DATA.mkdir(parents=True, exist_ok=True)
store = Store(DATA / 'scanner.sqlite3')
store.db.execute('CREATE TABLE IF NOT EXISTS client_lead_evidence(lead_id INTEGER PRIMARY KEY REFERENCES leads(id),forwarded INTEGER NOT NULL,sender_is_bot INTEGER NOT NULL,sender_deleted INTEGER NOT NULL)')
store.db.commit()
_client_contact_store = store
def ensure_client_contacts():
    global _client_contact_store
    if _client_contact_store is store:
        return
    store.db.execute('CREATE TABLE IF NOT EXISTS client_lead_evidence(lead_id INTEGER PRIMARY KEY REFERENCES leads(id),forwarded INTEGER NOT NULL,sender_is_bot INTEGER NOT NULL,sender_deleted INTEGER NOT NULL)')
    store.db.commit()
    _client_contact_store = store
ORIGIN = os.environ.get('PANEL_ORIGIN', 'http://127.0.0.1:8080')
CONNECT_TIMEOUT = 20
log = logging.getLogger('scanner')
logging.getLogger('telethon').setLevel(logging.WARNING)


def fail(text, status=400):
    raise HTTPException(status, text)


def managed_chat(entity):
    return bool(getattr(entity, 'creator', False) or getattr(entity, 'admin_rights', None))


def error_text(exc):
    if isinstance(exc, errors.FloodWaitError):
        return f'Telegram просит подождать {exc.seconds} секунд.'
    mapping = {'PhoneCodeInvalidError':'Неверный код входа.', 'PhoneCodeExpiredError':'Код истёк. Запросите новый.', 'PasswordHashInvalidError':'Неверный пароль двухэтапной проверки.', 'ApiIdInvalidError':'Проверьте API ID и API hash.', 'PhoneNumberInvalidError':'Проверьте номер телефона.', 'SessionRevokedError':'Сессия отозвана. Подключите аккаунт заново.', 'AuthKeyUnregisteredError':'Подключите аккаунт заново.', 'ChatWriteForbiddenError':'Аккаунт не может писать в выбранный чат.', 'UserBannedInChannelError':'Нет права отправлять сообщения в выбранный чат.', 'ChannelPrivateError':'Нет доступа к источнику.', 'PeerIdInvalidError':'Чат не найден среди доступных аккаунту.'}
    return mapping.get(type(exc).__name__, f'Не удалось выполнить действие ({type(exc).__name__}).')


class Runtime:
    def __init__(self):
        self.client = None
        self.me = None
        self.phone = None
        self.code_hash = None
        self.auth_until = 0
        self.auth_next = 0
        self.auth_lock = asyncio.Lock()
        self.scan_pause = 1
        self.scan_lock = asyncio.Lock()
        self.delivery_lock = asyncio.Lock()
        self.status = 'not_connected'
        self.error = None
        self.last_event = None
        self.tasks = []
        self.outbound_guard = None
        self.outbound_pause = None

    def outbound_allowed(self):
        return self.outbound_status()['allowed']

    def outbound_status(self):
        if self.outbound_guard:
            try:self.outbound_guard()
            except HTTPException as exc:return {'allowed':False,'reason':exc.detail}
        return {'allowed':True,'reason':None}

    def stop_outbound(self,exc):
        if self.outbound_pause:self.outbound_pause(exc)

    def report_error(self, exc):
        self.error = error_text(exc)
        if isinstance(exc, (errors.SessionRevokedError, errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.UserDeactivatedBanError)):
            self.me = None
            self.status = 'not_connected'
            store.put('running', False)

    async def connect(self):
        config = store.get('api')
        if not config:
            return
        if not self.client:
            self.client = TelegramClient(str(DATA / 'telegram'), config['id'], config['hash'], device_model='Chatcleaner Lead Scanner', app_version='1.0', flood_sleep_threshold=0, request_retries=0, raise_last_call_error=True)
            self.client.add_event_handler(self.on_message, events.NewMessage(incoming=True))
            connection_client=self.client
            async def private_event(event):
                if self.client is connection_client:
                    try:await conversations.on_event(event)
                    except Exception as exc:store.put('conversation_worker_error',error_text(exc))
            self.client.add_event_handler(private_event, events.NewMessage())
            self.client.add_event_handler(private_event, events.MessageEdited())
        if not self.client.is_connected():
            await self.client.connect()
        # A timed-out connection can retain a transport without an identity.
        # Recheck authorization on the same client before allowing scanner work.
        if await self.client.is_user_authorized():
            await self.ready()
        else:
            self.me = None
            self.status = 'not_connected'

    async def ready(self):
        self.me = await self.client.get_me()
        self.status = 'connected'
        self.error = None
        self.phone = self.code_hash = None
        self.auth_until = 0

    async def on_message(self, event):
        try:
            await self.consume(event.chat_id, event.message)
        except Exception as exc:
            self.report_error(exc)

    def capture_contact_evidence(self, lead_id, message, sender):
        if not lead_id:
            return
        ensure_client_contacts()
        with store.db:
            store.db.execute('INSERT OR REPLACE INTO client_lead_evidence VALUES(?,?,?,?)',
                             (lead_id, bool(getattr(message, 'fwd_from', None)),
                              bool(getattr(sender, 'bot', False)), bool(getattr(sender, 'deleted', False))))

    async def consume(self, chat_id, message, *, update_activity=True):
        if message.out or not message.message:
            return
        source = store.db.execute('SELECT * FROM sources WHERE id=? AND enabled=1', (chat_id,)).fetchone()
        if not source or message.id <= source['cursor']:
            return
        self.last_event = now()
        if update_activity:
            with store.db:
                store.db.execute('UPDATE sources SET last_message_at=MAX(COALESCE(last_message_at,?),?) WHERE id=?',
                                 (message.date.isoformat(),message.date.isoformat(),chat_id))
        # Resolve a sender only for demand that can actually become a lead.
        # Non-matching messages are still counted and checkpointed by scan().
        if not store.get('running',False) or not any(r['enabled'] and chat_id in r['sources'] and matches(message.message,r) for r in store.rules()):
            return
        sender = await message.get_sender()
        username = getattr(sender, 'username', None)
        author = '@' + username if username else ' '.join(filter(None, [getattr(sender, 'first_name', ''),getattr(sender, 'last_name', '')])) or 'Автор не указан'
        link = f'https://t.me/{source["username"]}/{message.id}' if source['username'] else f'https://t.me/c/{utils.resolve_id(chat_id)[0]}/{message.id}' if chat_id < -1000000000000 else None
        lead_id = store.ingest(chat_id, message.id, message.message, author, message.sender_id, link, message.date.isoformat())
        self.capture_contact_evidence(lead_id, message, sender)

    async def scan(self):
        if not self.client or not self.me or not self.client.is_connected():
            return
        if time.time() < store.get('scan_not_before', 0):
            return
        succeeded = True
        async with self.scan_lock:
            sources=store.rows('SELECT * FROM sources WHERE enabled=1 ORDER BY id')
            resume=store.get('scan_resume_source')
            start=next((i for i,s in enumerate(sources) if s['id']==resume),0)
            sources=sources[start:]+sources[:start]
            if self.error and self.error.startswith('Telegram просит подождать'):self.error=None
            updates={};completed={};resume_source=resume;pending_count=0
            def flush():
                nonlocal pending_count
                if not updates and not completed:return
                with store.db:
                    for sid,row in updates.items():
                        store.db.execute('UPDATE sources SET cursor=MAX(cursor,?),messages_checked=messages_checked+?,stats_since=COALESCE(stats_since,?),last_message_at=MAX(COALESCE(last_message_at,?),?) WHERE id=?',
                                         (row['id'],row['count'],now(),row['date'],row['date'],sid))
                    for sid,stamp in completed.items():store.db.execute('UPDATE sources SET last_scan=?,error=NULL WHERE id=?',(stamp,sid))
                    store.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('scan_resume_source',json.dumps(resume_source)))
                updates.clear();completed.clear();pending_count=0
            try:
                for index,source in enumerate(sources):
                    resume_source=source['id']
                    try:
                        async for message in self.client.iter_messages(source['id'], min_id=source['cursor'], reverse=True, limit=200):
                            await self.consume(source['id'], message,update_activity=False)
                            row=updates.setdefault(source['id'],{'count':0})
                            row.update(id=message.id,date=message.date.isoformat());row['count']+=1;pending_count+=1
                            if pending_count>=20:flush()
                            await asyncio.sleep(0)
                        completed[source['id']]=now()
                    except errors.FloodWaitError as exc:
                        flush();self.report_error(exc)
                        store.put('scan_not_before', time.time()+exc.seconds+1)
                        store.put('scan_resume_source',source['id'])
                        return
                    except Exception as exc:
                        flush();succeeded=False;self.report_error(exc)
                        with store.db:store.db.execute('UPDATE sources SET error=? WHERE id=?',(error_text(exc),source['id']))
                        if self.me is None:return
                    resume_source=sources[index+1]['id'] if index+1<len(sources) else None
                    if len(completed)>=10:flush()
                    if self.scan_pause:await asyncio.sleep(self.scan_pause)
            finally:flush()
            if succeeded:
                self.error = None

    async def scan_loop(self):
        while True:
            try:
                # HTTP starts independently of Telegram. Serialize background
                # recovery with explicit login; never replace an active challenge.
                if not self.auth_lock.locked() and store.get('api'):
                    async with self.auth_lock:
                        challenge = self.status in ('code_required','password_required') and time.time() < self.auth_until
                        if not challenge and (not self.client or not self.me or not self.client.is_connected()):
                            self.me = None
                            self.status = 'not_connected'
                            await asyncio.wait_for(self.connect(), CONNECT_TIMEOUT)
                await self.scan()
            except Exception as exc:
                self.report_error(exc)
            await asyncio.sleep(30)

    async def outbox_loop(self):
        while True:
            try:
                async with self.delivery_lock:
                    if self.client and self.me and self.client.is_connected() and self.outbound_allowed() and store.get('running',False) and store.get('notification_mode','account')=='account':
                        rows = store.rows("SELECT o.*,l.text,l.author,l.link,l.matches,s.title FROM outbox o JOIN leads l ON l.id=o.lead_id LEFT JOIN sources s ON s.id=l.chat_id WHERE o.state='pending' AND o.next_at<=? ORDER BY o.lead_id LIMIT 1", (time.time(),))
                        for row in rows:
                            # Changing destination cancels old pending deliveries instead of leaking to an old chat.
                            if row['destination'] != store.get('destination'):
                                with store.db: store.db.execute("UPDATE outbox SET state='cancelled' WHERE lead_id=?",(row['lead_id'],))
                                continue
                            text = 'Новый запрос · ' + ', '.join(x['name'] for x in json.loads(row['matches'])) + '\n' + (row['title'] or '') + '\n' + row['author'] + '\n\n' + row['text'][:2300] + '\n\n' + (row['link'] or 'Ссылка недоступна')
                            try:
                                peer = await self.client.get_input_entity(row['destination'])
                                if not self.outbound_allowed():break
                                await self.client(functions.messages.SendMessageRequest(peer=peer,message=text,random_id=row['random_id'],no_webpage=True))
                                with store.db: store.db.execute("UPDATE outbox SET state='sent',error=NULL WHERE lead_id=?",(row['lead_id'],))
                            except Exception as exc:
                                if isinstance(exc,(errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError)):
                                    self.stop_outbound(exc)
                                if isinstance(exc, (errors.SessionRevokedError, errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.UserDeactivatedBanError)):
                                    self.report_error(exc)
                                attempts = row['attempts']+1
                                delay = exc.seconds + 1 if isinstance(exc,errors.FloodWaitError) else min(3600,30*2**min(attempts,7))
                                stopped=isinstance(exc,(errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError))
                                with store.db: store.db.execute('UPDATE outbox SET attempts=?,next_at=?,error=?,state=? WHERE lead_id=?',(attempts,time.time()+delay,error_text(exc),'failed' if stopped or attempts>=8 else 'pending',row['lead_id']))
            except Exception as exc:
                self.report_error(exc)
            await asyncio.sleep(4)

runtime = Runtime()
bot_notifications = BotNotifications(lambda:store,runtime.delivery_lock)
bot_notifications.initialize()
directory = Directory(lambda: store, lambda: runtime)
history = HistoryRunner(lambda: store, lambda: runtime)
publisher = Publisher(lambda: store, lambda: runtime, DATA)
conversations = Conversations(lambda: store, lambda: runtime)
marketing = Marketing(DATA / "owner-marketing.sqlite3", lambda: runtime, lambda: store)
operations = Operations(DATA, lambda: store.get("api"), lambda: runtime, lambda: store)
source_folders = SourceFolders(lambda:runtime,lambda:store)

def bind_outbound_controls(target,ops):
    # Late-bound to the initialized CLIENT Operations account; reading sources remains independent.
    target.outbound_guard=lambda:ops.online('primary')
    target.outbound_pause=lambda exc:ops.pause_outbound('primary',exc)

@asynccontextmanager
async def lifespan(app):
    bot_notifications.task = asyncio.create_task(bot_notifications.loop())
    operations.initialize()
    bind_outbound_controls(runtime,operations)
    operations.task = asyncio.create_task(operations.loop())
    marketing.initialize()
    conversations.initialize()
    conversations.task = asyncio.create_task(conversations.loop())
    publisher.initialize()
    publisher.task = asyncio.create_task(publisher.loop())
    seed_topics(store)
    directory.recover()
    history.recover()
    runtime.tasks = [asyncio.create_task(runtime.scan_loop()),asyncio.create_task(runtime.outbox_loop())]
    yield
    # Stop reconnect/scan tasks before their stores and dependent workers close.
    for task in runtime.tasks: task.cancel()
    for task in runtime.tasks:
        with contextlib.suppress(asyncio.CancelledError): await task
    await bot_notifications.shutdown()
    await operations.shutdown()
    await marketing.shutdown()
    await directory.shutdown()
    await history.shutdown()
    await publisher.shutdown()
    await conversations.shutdown()
    if runtime.client: await runtime.client.disconnect()
    store.db.close()

app = FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None,openapi_url=None)
app.include_router(marketing.router)
app.include_router(operations.router)
app.include_router(bot_notifications.router)
app.include_router(directory.router)
app.include_router(history.router)
app.include_router(publisher.router)
app.include_router(conversations.router)
app.include_router(source_folders.router)

@app.middleware('http')
async def protect(request: Request, call_next):
    # Nginx overwrites identity headers after session authentication; app binds to loopback only.
    if not request.headers.get('x-panel-user'):
        return JSONResponse({'detail':'Требуется вход в панель.'},status_code=401)
    if request.url.path.startswith(('/api/scanner/operations','/api/scanner/bot-notifications')) and request.headers.get('x-panel-role') not in ('owner','admin'):
        return JSONResponse({'detail':'Нужен доступ администратора панели.'},status_code=403)
    if request.method not in ('GET','HEAD'):
        if request.headers.get('origin') != ORIGIN or request.headers.get('x-scanner-request') != '1':
            return JSONResponse({'detail':'Запрос отклонён: откройте панель заново.'},status_code=403)
        body_limit = MAX_PHOTO if request.url.path in ('/api/scanner/content/media','/api/scanner/operations/media') else MAX_REQUEST if request.url.path=='/api/scanner/operations/accounts/import-file' else 65536
        if int(request.headers.get('content-length','0') or '0')>body_limit:
            return JSONResponse({'detail':'Слишком большой запрос.'},status_code=413)
    response = await call_next(request)
    response.headers['Cache-Control']='no-store'
    return response

@app.exception_handler(RequestValidationError)
async def invalid_request(request, exc):
    return JSONResponse({'detail':'Проверьте формат и заполнение полей.'},status_code=422)

@app.exception_handler(Exception)
async def generic_error(request, exc):
    if not request.url.path.startswith('/api/scanner/operations') and isinstance(exc, (errors.SessionRevokedError, errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.UserDeactivatedBanError)):
        runtime.report_error(exc)
    return JSONResponse({'detail':error_text(exc)},status_code=400)


def connected():
    if not runtime.client or not runtime.me or not runtime.client.is_connected(): fail('Сначала подключите Telegram-аккаунт.',409)

@lru_cache(maxsize=1024)
def saved_message_matches(text, rule_json):
    # Private to the client scanner process; rule edits change the cache key.
    return bool(matches(text, json.loads(rule_json)))

async def state(status:Literal['all','new','working','rejected']='all',topic:int=Query(default=0,ge=0),offset:int=Query(default=0,ge=0),lead_id:int=Query(default=0,ge=0)):
    ensure_client_contacts()
    where="WHERE (?='all' OR l.status=?) AND (?=0 OR EXISTS (SELECT 1 FROM json_each(l.matches) m WHERE json_extract(m.value,'$.id')=?))"
    where+=" AND (?=0 OR l.id=?)"
    params=(status,status,topic,topic,lead_id,lead_id)
    filtered=store.db.execute('SELECT COUNT(*) FROM leads l '+where,params).fetchone()[0]
    page_size=50
    offset=min(offset,((filtered-1)//page_size)*page_size if filtered else 0)
    leads = store.rows('SELECT l.*,s.title AS source_title,COALESCE(b.state,o.state) AS delivery,COALESCE(b.error,o.error) AS delivery_error FROM leads l LEFT JOIN sources s ON s.id=l.chat_id LEFT JOIN outbox o ON o.lead_id=l.id LEFT JOIN bot_deliveries b ON b.lead_id=l.id '+where+' ORDER BY l.created DESC,l.id DESC LIMIT ? OFFSET ?',params+(page_size,offset))
    current_rules={rule['id']:rule for rule in store.rules()}
    rule_json={key:json.dumps(rule,sort_keys=True,ensure_ascii=False) for key,rule in current_rules.items()}
    sources=store.rows('SELECT * FROM sources ORDER BY title')
    sources_by_id={source['id']:source for source in sources}
    account_id=getattr(runtime.me,'id',None)
    contact_evidence={}
    contact_conversations={}
    if leads:
        ids=[lead['id'] for lead in leads]
        placeholders=','.join('?' for _ in ids)
        contact_evidence={row['lead_id']:row for row in store.rows('SELECT * FROM client_lead_evidence WHERE lead_id IN ('+placeholders+')',tuple(ids))}
        if account_id and store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conversation_leads'").fetchone():
            rows=store.rows('SELECT c.account_id,c.peer_id,c.lead_id,c.last_sync,cl.lead_id AS linked_id FROM conversations c JOIN conversation_leads cl ON cl.cid=c.id WHERE c.account_id=? AND cl.lead_id IN ('+placeholders+')',(account_id,*ids))
            contact_conversations={row['linked_id']:{**row,'linked_lead_ids':[row['linked_id']]} for row in rows}
    for lead in leads:
        lead['matches']=json.loads(lead['matches']);lead.pop('fingerprint',None)
        lead['matches_current_rules']=any(saved_message_matches(lead['text'],rule_json[m['id']])
                                          for m in lead['matches'] if m['id'] in current_rules)
        evidence=contact_evidence.get(lead['id'],{})
        annotated=annotate_client_contact({**lead,**{key:bool(evidence[key]) for key in ('forwarded','sender_is_bot','sender_deleted') if key in evidence}},sources_by_id.get(lead['chat_id']),conversation=contact_conversations.get(lead['id']),account_id=account_id)
        lead['client_contact']=annotated['client_contact']
    return {'outbound':runtime.outbound_status(),'source_limit':100,'storage_health':store.storage_health(),'connection':runtime.status if runtime.client and runtime.client.is_connected() else 'not_connected','account':getattr(runtime.me,'username',None) or getattr(runtime.me,'first_name',None),'api_configured':bool(store.get('api')),'running':store.get('running',False),'destination':store.get('destination'),'destination_title':store.get('destination_title'),'error':runtime.error,'last_event':runtime.last_event,'sources':store.rows('SELECT * FROM sources ORDER BY title'),'rules':store.rules(),'leads':leads,'total':store.db.execute('SELECT COUNT(*) FROM leads').fetchone()[0],'filtered_total':filtered,'offset':offset,'page_size':page_size,'next_offset':offset+page_size if offset+len(leads)<filtered else None,'history':history.summary(),'notification_mode':store.get('notification_mode','account'),'pending':store.db.execute("SELECT COUNT(*) FROM outbox WHERE state='pending'").fetchone()[0]+store.db.execute("SELECT COUNT(*) FROM bot_deliveries WHERE state='pending'").fetchone()[0]}

@app.get('/api/scanner/state')
async def json_state(status:Literal['all','new','working','rejected']='all',topic:int=Query(default=0,ge=0),offset:int=Query(default=0,ge=0),lead_id:int=Query(default=0,ge=0)):
    return JSONResponse(await state(status,topic,offset,lead_id))

class Credentials(BaseModel):
    api_id: int = Field(gt=0)
    api_hash: str = Field(pattern=r'^[a-fA-F0-9]{32}$')

@app.post('/api/scanner/credentials')
async def credentials(data: Credentials):
    async with runtime.auth_lock:
        if runtime.me: fail('Сначала отключите текущий аккаунт.')
        if runtime.client: await runtime.client.disconnect();runtime.client=None
        store.put('api',{'id':data.api_id,'hash':data.api_hash})
        await runtime.connect()
    return {'ok':True}

class Phone(BaseModel):
    phone: str = Field(pattern=r'^\+[0-9]{10,15}$')

@app.post('/api/scanner/auth/code')
async def auth_code(data: Phone):
    async with runtime.auth_lock:
        if runtime.me: fail('Аккаунт уже подключён.')
        if time.time()<runtime.auth_next: fail('Подождите минуту перед повторным запросом кода.',429)
        if not runtime.client: fail('Сначала сохраните API ID и API hash.')
        runtime.auth_next=time.time()+60
        sent=await runtime.client.send_code_request(data.phone)
        runtime.phone=data.phone;runtime.code_hash=sent.phone_code_hash;runtime.auth_until=time.time()+600
        runtime.status='code_required'
    return {'step':'code'}

class Code(BaseModel):
    code: str = Field(pattern=r'^[0-9]{4,8}$')

@app.post('/api/scanner/auth/confirm')
async def auth_confirm(data: Code):
    async with runtime.auth_lock:
        if not runtime.phone or time.time()>runtime.auth_until: fail('Запросите новый код входа.')
        try:
            await runtime.client.sign_in(phone=runtime.phone,code=data.code,phone_code_hash=runtime.code_hash)
        except errors.SessionPasswordNeededError:
            runtime.status='password_required'
            return {'step':'password'}
        await runtime.ready()
    return {'step':'connected'}

class Password(BaseModel):
    password: str = Field(min_length=1,max_length=256)

@app.post('/api/scanner/auth/password')
async def auth_password(data: Password):
    async with runtime.auth_lock:
        if runtime.status!='password_required' or time.time()>runtime.auth_until: fail('Сначала запросите код входа.')
        await runtime.client.sign_in(password=data.password)
        await runtime.ready()
    return {'step':'connected'}

@app.post('/api/scanner/auth/disconnect')
async def disconnect():
    await history.stop()
    if directory.task and not directory.task.done():
        await directory.cancel(directory.active_id)
    store.put('running',False)
    async with runtime.auth_lock:
        if runtime.client:
            await runtime.client.log_out()
        runtime.client=None;runtime.me=None;runtime.status='not_connected'
        runtime.phone=runtime.code_hash=None
        store.put('destination',None);store.put('destination_title',None)
        with store.db:
            store.db.execute('UPDATE sources SET enabled=0')
            store.db.execute("UPDATE outbox SET state='cancelled' WHERE state='pending'")
    return {'ok':True}

@app.get('/api/scanner/dialogs')
async def dialogs():
    connected()
    result=[]
    async for dialog in runtime.client.iter_dialogs(limit=300):
        if dialog.is_group or dialog.is_channel:
            result.append({'id':dialog.id,'title':dialog.name,'username':getattr(dialog.entity,'username',None),'kind':'Группа' if dialog.is_group else 'Канал','managed':managed_chat(dialog.entity)})
    return {'dialogs':result,'limit':300}

class Source(BaseModel):
    reference: str = Field(min_length=1,max_length=200)

@app.post('/api/scanner/sources')
async def add_source(data: Source):
    connected()
    ref=data.reference.strip()
    if ref.startswith('https://t.me/'): ref=ref[len('https://t.me/'):].rstrip('/')
    if not re.fullmatch(r'@?[A-Za-z][A-Za-z0-9_]{3,31}|-\d+',ref): fail('Укажите @username, https://t.me/username или ID доступной группы. Для закрытой группы выберите её из списка чатов.')
    entity=await runtime.client.get_entity(int(ref) if ref.startswith('-') else ref)
    if not isinstance(entity,(types.Channel,types.Chat)): fail('Источником может быть только группа или канал.')
    if managed_chat(entity): fail('Это ваша административная площадка. Она управляется в разделе «Группы и каналы»; для лидсканера выберите внешнее сообщество.')
    chat_id=utils.get_peer_id(entity)
    if chat_id==store.get('destination') or chat_id==bot_notifications.config().get('chat_id'): fail('Чат уведомлений нельзя использовать как источник.')
    if getattr(entity,'deactivated',False): fail('Эта группа больше не активна.')
    if getattr(entity,'left',False) and not getattr(entity,'username',None): fail('Для закрытого источника сначала получите доступ и вступите через Telegram.')
    existing=store.db.execute('SELECT id FROM sources WHERE id=?',(chat_id,)).fetchone()
    if not existing and store.db.execute('SELECT COUNT(*) FROM sources').fetchone()[0]>=100: fail('Доступно до 100 источников.')
    # Membership is not required when Telegram itself permits reading a public peer.
    # Do not join automatically or bypass a private/forbidden response.
    try:
        latest=await runtime.client.get_messages(entity,limit=1)
    except errors.ChannelPrivateError:
        fail('Telegram не разрешает читать этот источник. Получите доступ или вступите в группу через Telegram, затем повторите проверку.')
    mode='public' if getattr(entity,'left',False) else 'member'
    date=getattr(latest[0],'date',None) if latest else None
    with store.db:
        store.db.execute('INSERT INTO sources(id,title,username,cursor,access_mode,last_message_at,last_scan) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET title=excluded.title,username=excluded.username,enabled=1,error=NULL,access_mode=excluded.access_mode,last_message_at=excluded.last_message_at,last_scan=excluded.last_scan', (chat_id,entity.title,getattr(entity,'username',None),latest[0].id if latest else 0,mode,date.isoformat() if date else None,now()))
    return {'ok':True,'id':chat_id,'access_mode':mode}

class Enabled(BaseModel):
    enabled: bool

@app.post('/api/scanner/sources/{source_id}')
async def source_toggle(source_id:int, data:Enabled):
    source=store.db.execute('SELECT * FROM sources WHERE id=?',(source_id,)).fetchone()
    if not source: fail('Источник не найден.',404)
    if data.enabled and source_id in (store.get('destination'),bot_notifications.config().get('chat_id')): fail('Чат уведомлений нельзя использовать как источник.')
    if data.enabled:
        connected()
        entity=await runtime.client.get_entity(source['id'])
        if managed_chat(entity): fail('Площадка стала административной. Для лидсканера выберите внешнее сообщество.')
        await runtime.client.get_messages(entity,limit=1)
    with store.db: store.db.execute('UPDATE sources SET enabled=? WHERE id=?',(data.enabled,source_id))
    return {'ok':True}

class Rule(BaseModel):
    id: int | None = None
    name: str = Field(min_length=1,max_length=80)
    sources: list[int] = Field(default_factory=list,max_length=100)
    keywords: str = Field(min_length=1,max_length=8000)
    required: str = Field(default='',max_length=8000)
    excluded: str = Field(default='',max_length=8000)
    substring: bool = False
    demand_only: bool | None = None
    enabled: bool = True

@app.post('/api/scanner/rules')
async def rule_save(data:Rule):
    valid={r['id'] for r in store.rows('SELECT id FROM sources')}
    if not set(data.sources)<=valid: fail('Выберите существующие источники.')
    if data.enabled and not data.sources: fail('Выберите хотя бы один источник или сохраните поиск выключенным.')
    rule={'sources':data.sources,'keywords':words(data.keywords),'required':words(data.required),'excluded':words(data.excluded),'substring':data.substring}
    if not rule['keywords']: fail('Укажите хотя бы одно ключевое слово.')
    if any(len(w)>200 for key in ('keywords','required','excluded') for w in rule[key]): fail('Фраза не должна превышать 200 символов.')
    if not data.name.strip(): fail('Укажите название поиска.')
    with store.db:
        if data.id:
            if not store.db.execute('SELECT 1 FROM rules WHERE id=?',(data.id,)).fetchone(): fail('Поиск не найден.',404)
            previous=next((r for r in store.rules() if r['id']==data.id),{})
            for key in ('topic','discovery_queries'):
                if key in previous: rule[key]=previous[key]
            rule['demand_only'] = data.demand_only if data.demand_only is not None else previous.get('demand_only', False)
            store.db.execute('UPDATE rules SET name=?,data=?,enabled=? WHERE id=?',(data.name.strip(),json.dumps(rule),data.enabled,data.id))
        else:
            rule['demand_only'] = bool(data.demand_only)
            if store.db.execute('SELECT COUNT(*) FROM rules').fetchone()[0]>=25: fail('Доступно до 25 поисков. Измените существующий.')
            store.db.execute('INSERT INTO rules(name,data,enabled) VALUES(?,?,?)',(data.name.strip(),json.dumps(rule),data.enabled))
    return {'ok':True}

@app.post('/api/scanner/rules/{rule_id}')
async def rule_toggle(rule_id:int,data:Enabled):
    rule=next((r for r in store.rules() if r['id']==rule_id),None)
    if not rule: fail('Поиск не найден.',404)
    if data.enabled and not rule['sources']: fail('Сначала выберите источники внутри поиска.')
    with store.db: store.db.execute('UPDATE rules SET enabled=? WHERE id=?',(data.enabled,rule_id))
    return {'ok':True}

class Preview(BaseModel):
    rule_id:int
    text:str=Field(min_length=1,max_length=10000)

@app.post('/api/scanner/preview')
async def preview(data:Preview):
    rule=next((r for r in store.rules() if r['id']==data.rule_id),None)
    if not rule: fail('Поиск не найден.',404)
    result=explain(data.text,rule)
    reasons={'no_keywords':'Нет ключевых слов выбранного направления.', 'excluded':'Сработало исключение.',
             'no_intent':'Не найдено заданных слов спроса.', 'matched':'Ключи и условия поиска совпали.',
             'supplier_offer':'Сообщение похоже на предложение продавца.', 'job_seeker':'Сообщение похоже на поиск работы или заказчиков.',
             'employment':'Сообщение похоже на вакансию, а не запрос услуги.', 'existing_document':'Упоминаются имеющиеся документы или их предоставление работодателем.',
             'other_geography':'Сафари относится к другой стране.', 'unrelated_intent':'Ключ и слово спроса находятся в разных предложениях.'}
    matched=result['reason']=='matched'
    return {'matched':matched,'keywords':result['keywords'] if matched else [],'reason':result['detail'] or result['reason'],
            'note':reasons[result['detail'] or result['reason']]+' Только проверка: сообщение не сохранено и не отправлено.'}

class Destination(BaseModel):
    reference:str=Field(default='',max_length=200)

@app.post('/api/scanner/destination')
async def destination(data:Destination):
    async with runtime.delivery_lock:
        connected()
        ref=data.reference.strip()
        if not ref:
            store.put('destination',None);store.put('destination_title',None)
        else:
            if ref=='me': entity=await runtime.client.get_me()
            elif re.fullmatch(r'-\d+|@?[A-Za-z][A-Za-z0-9_]{3,31}',ref): entity=await runtime.client.get_entity(int(ref) if ref.startswith('-') else ref)
            else: fail('Выберите чат из списка или «Избранное».')
            if isinstance(entity,types.User) and entity.id!=runtime.me.id: fail('Выберите группу или своё «Избранное», а не личную переписку.')
            peer_id=utils.get_peer_id(entity)
            if store.db.execute('SELECT 1 FROM sources WHERE id=? AND enabled=1',(peer_id,)).fetchone(): fail('Сначала отключите этот чат в источниках.')
            store.put('destination',peer_id);store.put('destination_title',getattr(entity,'title','Избранное'))
        with store.db: store.db.execute("UPDATE outbox SET state='cancelled' WHERE state='pending' AND destination!=?",(store.get('destination',0) or 0,))
        bot_notifications.change_mode('account' if data.reference.strip() else 'panel')
    return {'ok':True}

@app.post('/api/scanner/notification-test')
async def notification_test():
    async with runtime.delivery_lock:
        if bot_notifications.delivery_mode()!='account':fail('Для выбранного режима используйте тест бота уведомлений или включите доставку аккаунтом.')
        connected()
        target=store.get('destination')
        if not target: fail('Сначала выберите чат уведомлений.')
        if runtime.outbound_guard:runtime.outbound_guard()
        try:await runtime.client.send_message(target,'Проверка Chatcleaner: уведомления лидсканера подключены.',link_preview=False)
        except (errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError) as exc:
            runtime.stop_outbound(exc);fail(error_text(exc))
    return {'ok':True}

@app.post('/api/scanner/running')
async def running(data:Enabled):
    if data.enabled:
        connected()
        if not any(r['enabled'] and any(s['enabled'] and s['id'] in r['sources'] for s in store.rows('SELECT * FROM sources')) for r in store.rules()): fail('Добавьте активный поиск и хотя бы один включённый источник.')
    store.put('running',data.enabled)
    return {'ok':True}

class LeadStatus(BaseModel):
    status:str=Field(pattern=r'^(new|working|rejected)$')

@app.post('/api/scanner/leads/{lead_id}')
async def lead_status(lead_id:int,data:LeadStatus):
    if not store.db.execute('SELECT 1 FROM leads WHERE id=?',(lead_id,)).fetchone(): fail('Находка не найдена.',404)
    with store.db: store.db.execute('UPDATE leads SET status=? WHERE id=?',(data.status,lead_id))
    return {'ok':True}

@app.post('/api/scanner/leads/{lead_id}/retry')
async def retry_notification(lead_id:int):
    if store.db.execute('SELECT 1 FROM bot_deliveries WHERE lead_id=?',(lead_id,)).fetchone():
        return await bot_notifications.retry('lead-'+str(lead_id),BotRetry(confirmed=True))
    if bot_notifications.delivery_mode()!='account':fail('Сначала включите доставку аккаунтом.')
    connected()
    row=store.db.execute('SELECT o.*,l.status FROM outbox o JOIN leads l ON l.id=o.lead_id WHERE lead_id=?',(lead_id,)).fetchone()
    if not row: fail('У находки нет уведомления для повторной отправки.',404)
    if row['state']!='failed': fail('Повтор доступен только для уведомления с ошибкой.')
    if row['destination']!=store.get('destination'): fail('Получатель изменился. Старое уведомление повторно не отправляется.')
    if row['status']=='rejected': fail('Находка отклонена. Сначала измените её статус.')
    # Keep the Telegram random_id so a lost acknowledgement cannot create a duplicate.
    with store.db:
        store.db.execute("UPDATE outbox SET state='pending',attempts=0,next_at=0,error=NULL WHERE lead_id=?",(lead_id,))
    return {'ok':True}

@app.post('/api/scanner/sources/{source_id}/remove')
async def source_remove(source_id:int):
    with store.db:
        for rule in store.rules():
            if source_id in rule['sources']:
                rule['sources']=[s for s in rule['sources'] if s!=source_id]
                data={key:value for key,value in rule.items() if key not in ('id','name','enabled')}
                store.db.execute('UPDATE rules SET data=?,enabled=? WHERE id=?',(json.dumps(data),rule['enabled'] and bool(rule['sources']),rule['id']))
        store.db.execute('DELETE FROM sources WHERE id=?',(source_id,))
    return {'ok':True}


class Discovery(BaseModel):
    query:str=Field(min_length=3,max_length=100)
    kind:str=Field(default='all',pattern=r'^(all|group|channel)$')
    min_members:int=Field(default=0,ge=0,le=100000000)

@app.post('/api/scanner/discover')
async def discover(data:Discovery):
    connected()
    if directory.task and not directory.task.done(): fail('Дождитесь завершения парсера площадок.',429)
    if time.time()<getattr(runtime,'discovery_next',0): fail('Подождите несколько секунд перед следующим поиском.',429)
    runtime.discovery_next=time.time()+5
    found=await runtime.client(functions.contacts.SearchRequest(q=data.query.strip(),limit=50))
    result=[]
    for entity in found.chats:
        if not isinstance(entity,(types.Chat,types.Channel)) or not getattr(entity,'username',None):continue
        if managed_chat(entity):continue
        kind='group' if isinstance(entity,types.Chat) or getattr(entity,'megagroup',False) else 'channel'
        count=getattr(entity,'participants_count',None)
        if data.kind!='all' and kind!=data.kind:continue
        if data.min_members and (count is None or count<data.min_members):continue
        result.append({'id':utils.get_peer_id(entity),'title':entity.title,'username':entity.username,'kind':kind,'members':count,'joined':not bool(getattr(entity,'left',False))})
    return {'results':result,'note':'Внешние публичные площадки по названиям и username. Ваши административные группы исключены. Перед добавлением проверяется доступ к сообщениям; выдача ограничена Telegram.'}

class AttachSources(BaseModel):
    sources:list[int]=Field(min_length=1,max_length=100)
    rules:list[int]=Field(min_length=1,max_length=25)

@app.post('/api/scanner/assign')
async def assign(data:AttachSources):
    valid={r['id'] for r in store.rows('SELECT id FROM sources WHERE enabled=1')}
    if not set(data.sources)<=valid:fail('Сначала добавьте и включите выбранные источники.')
    rules={r['id']:r for r in store.rules()}
    if not set(data.rules)<=set(rules):fail('Поиск не найден.',404)
    with store.db:
        for rid in data.rules:
            r=rules[rid];r['sources']=list(dict.fromkeys(r['sources']+data.sources))
            if len(r['sources'])>100:fail('В одном поиске не больше 100 источников.')
            value={k:v for k,v in r.items() if k not in ('id','name','enabled')}
            store.db.execute('UPDATE rules SET data=?,enabled=1 WHERE id=?',(json.dumps(value,ensure_ascii=False),rid))
    return {'ok':True}
