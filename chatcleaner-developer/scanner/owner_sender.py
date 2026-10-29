"""Separate owner MTProto session; reuses the service app identity, never the client session."""
import asyncio
import contextlib
import io
import math
import time
from datetime import datetime,timezone
from fastapi import APIRouter,HTTPException,Query
from fastapi.responses import Response,JSONResponse
from pydantic import BaseModel,Field
from telethon import TelegramClient,events,errors,functions,types

CONNECT_TIMEOUT=20
INVITE_PRIVACY_TIMEOUT=20
OWNER_INVITE_PRIVACY_UID=710000010

class Phone(BaseModel):phone:str=Field(pattern=r'^\+[0-9]{10,15}$')
class Code(BaseModel):code:str=Field(pattern=r'^[0-9]{4,8}$')
class Password(BaseModel):password:str=Field(min_length=1,max_length=256)

class OwnerSender:
    def __init__(self,session_path,get_api,store):
        self.session_path=str(session_path);self.get_api=get_api;self.store=store
        self.client=None;self.me=None;self.status='not_connected';self.error=None
        self.phone=None;self.code_hash=None;self.auth_until=0;self.auth_next=0
        self.delivery_method=None;self.requested_at=None
        self.invite_privacy_next=0
        self.qr=None;self.qr_task=None;self.qr_image=None;self.qr_expires=0
        self.lock=asyncio.Lock();self.connect_lock=asyncio.Lock();self.task=None;self.on_event=None
        self.expected_username='demo_identity_3'
        self.router=APIRouter(prefix='/sender')
        for path,fn,methods in [('',self.state,['GET']),('/invite-privacy',self.invite_privacy,['GET']),('/code',self.code,['POST']),('/qr',self.start_qr,['POST']),('/qr-image',self.get_qr_image,['GET']),('/confirm',self.confirm,['POST']),('/password',self.password,['POST']),('/disconnect',self.disconnect,['POST'])]:
            self.router.add_api_route(path,fn,methods=methods)
    def pause(self):
        cfg=self.store.get('conversation_config',{});cfg.update(paused=True,auto_first=False,auto_reply=False)
        self.store.put('conversation_config',cfg)
        if self.store.db.execute("SELECT 1 FROM sqlite_master WHERE name='conversation_messages'").fetchone():
            with self.store.db:self.store.db.execute("UPDATE conversation_messages SET state='cancelled',error='Подключение отправителя изменилось.' WHERE state='pending'")
    def report_error(self,exc):
        self.error={'PasswordHashInvalidError':'Неверный облачный пароль Telegram. Введите пароль двухэтапной проверки, а не код входа.',
                    'PhoneCodeInvalidError':'Неверный код Telegram. Введите последний полученный код.',
                    'PhoneCodeExpiredError':'Код истёк. Запросите новый.',
                    'SendCodeUnavailableError':'Telegram не смог отправить новый код: доступные способы доставки для текущего запроса исчерпаны. Используйте вход по QR через Telegram на телефоне.',
                    'ApiIdInvalidError':'API-приложение сервиса недоступно.',
                    'PhoneNumberInvalidError':'Проверьте номер телефона.'}.get(type(exc).__name__,f'Telegram: {type(exc).__name__}.')
        if isinstance(exc,errors.FloodWaitError):self.error=f'Telegram просит подождать {exc.seconds} секунд.';self.auth_next=time.time()+exc.seconds+1
        if isinstance(exc,(errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError)):
            self.me=None;self.status='not_connected';self.pause()
    def challenge_active(self):
        return (self.status in ('code_required','password_required') and time.time()<self.auth_until
                or self.status=='qr_required' and self.qr is not None and time.time()<self.qr_expires)
    async def connect(self):
        # Separate from the auth lock: explicit login already holds self.lock.
        async with self.connect_lock:
            api=self.get_api()
            if not api:return
            if self.client and self.client.is_connected() and self.me:return
            client=self.client
            if not client:
                client=TelegramClient(self.session_path,api['id'],api['hash'],device_model='SignM Personal Assistant',app_version='1.0',flood_sleep_threshold=0)
                self.client=client
                async def event(e):
                    if self.client is client and self.me and self.on_event:
                        try:await self.on_event(e)
                        except Exception as exc:self.store.put('conversation_worker_error',f'Ошибка входящего события ({type(exc).__name__}).')
                client.add_event_handler(event,events.NewMessage());client.add_event_handler(event,events.MessageEdited())
            self.me=None
            if not self.challenge_active():self.status='not_connected'
            async def finish():
                if not client.is_connected():await client.connect()
                # A previous timeout may leave a connected transport but no me.
                # Complete authorization on that same session instead of returning.
                if await client.is_user_authorized():await self.ready()
                elif not self.challenge_active():self.status='not_connected';self.pause()
            try:
                await asyncio.wait_for(finish(),CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                # A cancelled Telethon connect can report connected before its
                # transport loops are usable. Keep the session/client/handlers,
                # but close this transport before the next normal reconnect.
                if self.client is client:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(client.disconnect(),min(CONNECT_TIMEOUT,5))
                raise
    async def ready(self):
        me=await asyncio.wait_for(self.client.get_me(),CONNECT_TIMEOUT)
        if not me or (me.username or '').casefold()!=self.expected_username.casefold() or getattr(me,'bot',False):
            await self.client.log_out();self.client=None;self.me=None;self.status='not_connected';self.pause()
            self.error=f'Подключён другой профиль. Для отправки нужен @{self.expected_username}.'
            raise HTTPException(409,self.error)
        self.me=me;self.status='connected';self.error=None
        self.store.put('owner_sender_started',True)
        self.phone=self.code_hash=None;self.auth_until=0
        self.qr=None;self.qr_image=None;self.qr_expires=0
        self.delivery_method=None;self.requested_at=None
    async def state(self):
        if self.status in ('code_required','password_required') and time.time()>self.auth_until:
            self.status='not_connected';self.phone=self.code_hash=None;self.delivery_method=None
            self.error='Запрос входа истёк. Начните вход заново или используйте QR.'
        return {'connection':self.status if self.client and self.client.is_connected() else 'not_connected',
                'username':getattr(self.me,'username',None),'account_id':getattr(self.me,'id',None),
                'api_available':bool(self.get_api()),'expected_username':self.expected_username,'error':self.error,
                'delivery_method':self.delivery_method,'requested_at':self.requested_at,
                'retry_after':max(0,math.ceil(self.auth_next-time.time())),
                'qr_expires_at':self.qr_expires if self.status=='qr_required' else None}
    async def invite_privacy(self,inviter_id:int=Query(gt=0,le=2**63-1)):
        """Read the connected owner's invite rules; never evaluate invite eligibility."""
        if type(inviter_id) is not int or not 0<inviter_id<2**63:
            raise HTTPException(400,'Укажите положительный Telegram ID проверяемого отправителя.')
        def fail(status,message,wait=None):
            headers={'Cache-Control':'no-store'}
            if wait is not None:headers['Retry-After']=str(wait)
            return HTTPException(status,message,headers=headers)
        try:
            async with asyncio.timeout(INVITE_PRIVACY_TIMEOUT):
                async with self.lock:
                    client=self.client;identity=self.me
                    def guard():
                        if (client is None or self.client is not client or self.me is not identity or
                            not client.is_connected() or self.status!='connected' or
                            not isinstance(identity,types.User) or identity.id!=OWNER_INVITE_PRIVACY_UID or
                            (identity.username or '').casefold()!='demo_identity_3'):
                            raise fail(409,'Для проверки нужен уже подключённый личный @demo_identity_3. Подключение или профиль изменились; чтение не выполнено.')
                    guard()
                    wait=max(0,math.ceil(self.invite_privacy_next-time.monotonic()))
                    if wait:raise fail(429,f'Повторить чтение настроек можно через {wait} секунд.',wait)
                    self.invite_privacy_next=time.monotonic()+60
                    guard()
                    fresh=await client.get_me()
                    guard()
                    if (not isinstance(fresh,types.User) or fresh.id!=OWNER_INVITE_PRIVACY_UID or
                        fresh.bot or (fresh.username or '').casefold()!='demo_identity_3'):
                        raise fail(409,'Telegram не подтвердил личный @demo_identity_3. Настройки не прочитаны; выход из аккаунта не выполнялся.')
                    guard()
                    privacy=await client(functions.account.GetPrivacyRequest(types.InputPrivacyKeyChatInvite()))
                    guard()
                    if not isinstance(privacy,types.account.PrivacyRules):
                        raise fail(502,'Telegram вернул неизвестный ответ настроек. Возможность приглашения не оценивалась.')
                    rules=[]
                    for rule in privacy.rules:
                        users=getattr(rule,'users',[]);chats=getattr(rule,'chats',[])
                        rules.append({'type':type(rule).__name__,
                                      'user_exception_count':len(users),'chat_exception_count':len(chats),
                                      'explicit_user_exception_match':inviter_id in users})
                    return JSONResponse({'account_id':fresh.id,'username':fresh.username,
                        'checked_at':datetime.now(timezone.utc).isoformat(),'rules':rules,
                        'match_scope':'explicit_user_exceptions_only','eligibility_evaluated':False,
                        'read_only':True,'invitation_requested':False,
                        'notice':'Совпадение относится только к явным исключениям по пользователям. Правила контактов, участников чатов и общие правила здесь не оцениваются; настройки не гарантируют добавление в группу.'},
                        headers={'Cache-Control':'no-store'})
        except errors.FloodWaitError as exc:
            self.invite_privacy_next=max(self.invite_privacy_next,time.monotonic()+max(60,exc.seconds+1))
            wait=max(0,math.ceil(self.invite_privacy_next-time.monotonic()))
            raise fail(429,f'Telegram просит подождать {wait} секунд. Чтение настроек не завершено; автоматического повтора нет.',wait) from None
        except errors.PeerFloodError:
            raise fail(409,'Telegram ограничил чтение настроек (PeerFlood). Проверка остановлена; повтор не выполнялся.') from None
        except (errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError):
            raise fail(409,'Telegram не подтвердил сессию личного аккаунта. Проверка остановлена; повторный вход автоматически не запускался.') from None
        except TimeoutError:
            raise fail(504,'Telegram долго отвечает на чтение настроек. Проверка не завершена; автоматического повтора нет.') from None
        except HTTPException:raise
        except Exception:
            raise fail(502,'Не удалось прочитать настройки приглашений. Возможность добавления не оценивалась; автоматического повтора нет.') from None
    async def cancel_qr(self):
        task=self.qr_task;self.qr_task=None
        self.qr=None;self.qr_image=None;self.qr_expires=0
        if task and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await task
    async def start_qr(self):
        async with self.lock:
            if self.me:raise HTTPException(409,'Отправитель уже подключён.')
            if self.status=='qr_required' and self.qr_image and time.time()<self.qr_expires:
                return {'step':'qr'}
            if time.time()<self.auth_next:raise HTTPException(429,f'Подождите {math.ceil(self.auth_next-time.time())} секунд перед следующим входом.')
            self.auth_next=time.time()+3
            await self.cancel_qr()
            try:
                import qrcode
                from qrcode.image.svg import SvgPathImage
                await self.connect()
                if self.me:return {'step':'connected'}
                if not self.client:raise HTTPException(409,'В сервисе не настроено API-приложение.')
                self.pause()
                qr=await asyncio.wait_for(self.client.qr_login(),25)
                stream=io.BytesIO()
                qrcode.make(qr.url,image_factory=SvgPathImage,border=4).save(stream)
                self.qr=qr;self.qr_image=stream.getvalue();self.qr_expires=qr.expires.timestamp()
                self.phone=self.code_hash=None;self.delivery_method=None;self.auth_until=0
                self.status='qr_required';self.error=None
                self.qr_task=asyncio.create_task(self.wait_qr(qr))
            except HTTPException:raise
            except Exception as exc:
                self.status='not_connected';self.report_error(exc);raise HTTPException(400,self.error) from None
        return {'step':'qr'}
    async def get_qr_image(self):
        if self.status!='qr_required' or not self.qr_image or time.time()>=self.qr_expires:
            raise HTTPException(410,'QR истёк. Создайте новый в панели.')
        return Response(self.qr_image,media_type='image/svg+xml',headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff'})
    async def wait_qr(self,qr):
        try:
            await qr.wait()
            async with self.lock:
                if self.qr is qr:await self.ready()
        except errors.SessionPasswordNeededError:
            async with self.lock:
                if self.qr is qr:
                    self.qr=None;self.qr_image=None;self.qr_expires=0
                    self.status='password_required';self.auth_until=time.time()+600;self.error=None
        except asyncio.TimeoutError:
            async with self.lock:
                if self.qr is qr:
                    self.qr=None;self.qr_image=None;self.qr_expires=0
                    self.status='not_connected';self.error='QR истёк. Нажмите «Войти по QR» ещё раз.'
        except asyncio.CancelledError:raise
        except Exception as exc:
            async with self.lock:
                # ready() may already have rejected and logged out a different profile.
                if self.qr is qr or isinstance(exc,HTTPException):
                    self.qr=None;self.qr_image=None;self.qr_expires=0;self.status='not_connected'
                    if not isinstance(exc,HTTPException):self.report_error(exc)
    async def code(self,spec:Phone):
        async with self.lock:
            if self.me:raise HTTPException(409,'Отправитель уже подключён.')
            if time.time()<self.auth_next:raise HTTPException(429,'Дождитесь минуты перед следующим запросом кода.')
            self.auth_next=time.time()+60
            await self.cancel_qr()
            try:
                await self.connect()
                if self.me:raise HTTPException(409,'Отправитель уже подключён.')
                if not self.client:raise HTTPException(409,'В сервисе не настроено API-приложение.')
                sent=await self.client.send_code_request(spec.phone)
                self.phone=spec.phone;self.code_hash=sent.phone_code_hash;self.auth_until=time.time()+600
                self.delivery_method=type(getattr(sent,'type',None)).__name__
                self.requested_at=time.time()
                self.status='code_required';self.error=None
            except HTTPException:raise
            except Exception as exc:
                self.status='not_connected';self.phone=self.code_hash=None;self.auth_until=0;self.delivery_method=None
                self.report_error(exc);raise HTTPException(400,self.error) from None
        return {'step':'code'}
    async def confirm(self,spec:Code):
        async with self.lock:
            if not self.client or self.status!='code_required' or not self.phone or time.time()>self.auth_until:raise HTTPException(400,'Запросите новый код.')
            try:await self.client.sign_in(phone=self.phone,code=spec.code,phone_code_hash=self.code_hash);await self.ready()
            except errors.SessionPasswordNeededError:self.status='password_required';self.error=None;return {'step':'password'}
            except HTTPException:raise
            except Exception as exc:self.report_error(exc);raise HTTPException(400,self.error) from None
        return {'step':'connected'}
    async def password(self,spec:Password):
        async with self.lock:
            if not self.client or self.status!='password_required' or time.time()>self.auth_until:raise HTTPException(400,'Сначала подтвердите вход по коду или QR.')
            try:await self.client.sign_in(password=spec.password);await self.ready()
            except HTTPException:raise
            except Exception as exc:self.report_error(exc);raise HTTPException(400,self.error) from None
        return {'step':'connected'}
    async def disconnect(self):
        async with self.lock:
            await self.cancel_qr()
            self.pause();self.store.put('owner_sender_started',False)
            client=self.client
            self.client=None;self.me=None;self.status='not_connected';self.phone=self.code_hash=None
            self.error=None;self.auth_until=0
            if client:
                try:await asyncio.wait_for(client.log_out(),20)
                except Exception:
                    await client.disconnect()
                    client.session.delete()
        return {'ok':True}
    async def loop(self):
        while True:
            try:
                # Only resume an existing authorized owner session. No unsolicited code requests.
                if self.store.get('owner_sender_started') and not self.lock.locked():
                    async with self.lock:
                        if not self.challenge_active():await self.connect()
            except Exception as exc:self.report_error(exc)
            await asyncio.sleep(30)
    async def shutdown(self):
        await self.cancel_qr()
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
        if self.client:await self.client.disconnect()
