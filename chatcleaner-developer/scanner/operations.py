"""Explicit account-bound publications and consented invitations. No account rotation."""
import asyncio
import base64
import binascii
import contextlib
import copy
import csv
import hashlib
import io
import ipaddress
import json
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field, model_validator
from telethon import TelegramClient, errors, functions, types, utils
from telethon.sessions import StringSession, SQLiteSession
from telethon.tl import TLRequest
from engine import Store, now
from owner_sender import OwnerSender, Phone, Code, Password
from publisher import Publisher
from account_import import decode_file, ImportError as FileImportError, MAX_REQUEST
from audience import Audience
from operation_tracking import OperationTracking
from operation_test_chat import OperationTestChat
from operation_campaigns import OperationCampaigns
from operation_folders import OperationFolders
from operation_redirects import OperationRedirects
from operation_budgets import OperationBudgets
from invite_run_budget import InviteRunBudget
from operation_workbench import OperationWorkbench, WorkbenchStopped
from invite_progress import summarize_invites
from operation_topics import DestinationTopic, OperationTopics, validate_topic_routes
from qa_invite_adminlog import QaInviteAdminLog
from isolated_invite import IsolatedInvite
from isolated_contact_batch import IsolatedContactBatch
from isolated_invite_campaign import IsolatedInviteCampaign
from manual_invite_campaign import ManualInviteCampaign
from posting_policy import PostingPolicy, PostingPolicyStopped, decision as posting_decision, expiry_decision, publication_decision

class ProxySpec(BaseModel):
    mode: Literal['direct','socks5','http'] = 'direct'
    address: str = Field(default='',max_length=64)
    port: int = Field(default=1080,ge=1,le=65535)
    username: str = Field(default='',max_length=128)
    password: str = Field(default='',max_length=256,repr=False)

    @model_validator(mode='after')
    def public_endpoint(self):
        if self.mode!='direct':
            try: address=ipaddress.ip_address(self.address.strip())
            except ValueError: raise ValueError('Укажите публичный IP-адрес прокси.') from None
            if not address.is_global:raise ValueError('Прокси должен иметь публичный IP-адрес.')
            self.address=str(address)
        return self

    def connection(self):
        if self.mode=='direct':return None
        return dict(proxy_type=self.mode,addr=self.address,port=self.port,
                    username=self.username or None,password=self.password or None,rdns=True)

class AccountSpec(BaseModel):
    label: str = Field(min_length=1, max_length=80)
    authorized: Literal[True]
    proxy: ProxySpec | None = None

class ImportSpec(BaseModel):
    payload: dict
    authorized: Literal[True]
    proxy: ProxySpec | None = None

class FileImportSpec(BaseModel):
    kind: Literal['telethon','tdata']
    label: str = Field(min_length=1, max_length=80)
    data: str = Field(min_length=1, max_length=12*1024*1024)
    passcode: str = Field(default='', max_length=256)
    account_index: int = Field(default=0, ge=0, le=9)
    api_id: int | None = None
    api_hash: str | None = Field(default=None, max_length=32)
    authorized: Literal[True]
    proxy: ProxySpec | None = None

class JobSpec(BaseModel):
    request_key: str = Field(min_length=16, max_length=80)
    account: str
    kind: Literal['post','invite','link','invite_message','message']
    title: str = Field(min_length=1, max_length=100)
    destinations: list[int] = Field(default_factory=list, max_length=30)
    destination_topics: list[DestinationTopic] = Field(default_factory=list, max_length=30)
    text: str = Field(default='', max_length=4096)
    media: list[str] = Field(default_factory=list, max_length=10)
    users: list[str] = Field(default_factory=list, max_length=50)
    audience_run_id: int | None = None
    candidate_ids: list[int] = Field(default_factory=list, max_length=50)
    permission_note: str = Field(default='', max_length=500)
    consent_note: str = Field(default='', max_length=500)
    delay_seconds: int = Field(default=60, ge=30, le=3600)
    daily_limit: int = Field(default=20, ge=1, le=100)
    scheduled_at: datetime | None = None
    link_hours: int = Field(default=24, ge=1, le=168)
    request_needed: bool = True
    invite_url: str = Field(default='',max_length=512)
    add_contacts_before_invite: bool = False
    campaign_id: int | None = Field(default=None, gt=0)
    campaign_revision: int | None = Field(default=None, gt=0)
    posting_policy: PostingPolicy | None = None

    @model_validator(mode='after')
    def contact_option_kind(self):
        validate_topic_routes(self.destination_topics,self.destinations,self.kind)
        if self.add_contacts_before_invite and self.kind!='invite':
            raise ValueError('Добавление в контакты доступно только перед прямым добавлением участников.')
        if (self.campaign_id is None)!=(self.campaign_revision is None):
            raise ValueError('Выберите профиль кампании вместе с его версией.')
        if self.campaign_id is not None and self.kind!='post':
            raise ValueError('Профиль кампании применяется только к публикациям в группах.')
        if self.destination_topics and (self.campaign_id is None or self.posting_policy is None):
            raise ValueError('Для публикации в тему выберите сохранённый профиль кампании и конечные ограничения частоты.')
        if self.posting_policy is not None:
            if self.kind!='post' or self.campaign_id is None:
                raise ValueError('Ограничение повторных публикаций требует публикацию в группы и сохранённую версию кампании.')
            ids=[d.chat_id for d in self.posting_policy.destinations]
            if len(self.destinations)!=len(set(self.destinations)) or set(ids)!=set(self.destinations):
                raise ValueError('Площадки задания и ограничения публикаций должны точно совпадать.')
            if self.scheduled_at is not None:
                if self.scheduled_at.tzinfo is None or self.scheduled_at.utcoffset() is None:
                    raise ValueError('Время начала окна должно содержать часовой пояс.')
                duration=self.posting_policy.window_expires_at.timestamp()-self.scheduled_at.timestamp()
                if not 0<duration<=7200:
                    raise ValueError('Окно отправки должно завершаться в течение двух часов после начала.')
        return self

    @model_validator(mode='before')
    @classmethod
    def server_owned_retry(cls,values):
        if isinstance(values,dict) and 'campaign_snapshot' in values:
            raise ValueError('Снимок профиля кампании сохраняет только сервер.')
        if isinstance(values,dict) and 'topic_snapshots' in values:
            raise ValueError('Снимок темы форума сохраняет только сервер.')
        if isinstance(values,dict) and any(key in values for key in ('pilot','pilot_allowance','pilot_budget')):
            raise ValueError('Разовый пилот подтверждается отдельно для сохранённого задания; его допуск нельзя передать при создании.')
        if isinstance(values,dict) and any(key in values for key in ('retry_invite','retry_of','retry_of_job')):
            raise ValueError('Повтор приглашения создаётся только из сохранённого результата через retry-invite.')
        return values

class PilotStart(BaseModel):
    model_config = {'extra':'forbid'}
    confirmed: Literal[True]
    request_key: str = Field(min_length=16,max_length=80)
    reason: str = Field(min_length=8,max_length=500)
    additional_attempts: int = Field(ge=1,le=50)
    goal_added: Literal[10] = 10
    start_now: Literal[True]

class RetryInvite(BaseModel):
    model_config = {'extra':'forbid'}
    confirmed: Literal[True]
    request_key: str = Field(min_length=16,max_length=80)
    reason: str = Field(min_length=8,max_length=500)
    item_ids: list[int] = Field(default_factory=list,max_length=50)

class Control(BaseModel):
    action: Literal['start','pause','resume','cancel','continue_pending','continue_after_single']
    confirmed: bool = False
    ack_single_request_key: str | None = Field(default=None,min_length=16,max_length=80)

class InviteRunMember(BaseModel):
    model_config = {'extra':'forbid'}
    item_id: int = Field(gt=0,strict=True)
    job_id: int = Field(gt=0,strict=True)
    user_id: int = Field(gt=0,strict=True)

class InviteRunGrant(BaseModel):
    model_config = {'extra':'forbid'}
    confirmed: Literal[True]
    request_key: str = Field(min_length=16,max_length=80)
    reason: str = Field(min_length=8,max_length=500)
    expires_at: float
    cohort_hash: str = Field(pattern=r'^[a-f0-9]{64}$')
    items: list[InviteRunMember] = Field(min_length=1,max_length=57)

class SingleAttempt(BaseModel):
    model_config = {'extra':'forbid'}
    confirmed: Literal[True]
    mode: Literal['mutual_contact_control','direct'] = 'mutual_contact_control'
    request_key: str = Field(min_length=16,max_length=80)
    reason: str = Field(min_length=8,max_length=500)
    telegram_id: int = Field(gt=0)
    chat_id: int
    user_id: int = Field(gt=0)
    cohort_hash: str = Field(pattern=r'^[a-f0-9]{64}$')

class Resolve(BaseModel):
    reference: str = Field(min_length=1, max_length=150)

class AddContact(BaseModel):
    model_config = {'extra':'forbid'}
    username: str = Field(min_length=4,max_length=33)
    confirmed: Literal[True]

class ResumeOutbound(BaseModel):
    model_config = {'extra':'forbid'}
    confirmed: Literal[True]
    telegram_id: int = Field(gt=0)
    reason: str = Field(min_length=8,max_length=500)

REVOKED=(errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError)
SINGLE_KNOWN_RESULTS=('invited','already_member','restricted','failed','blocked','duplicate_skipped')
# Official method errors absent from pinned Telethon 1.40.0's enum map.
# https://core.telegram.org/method/channels.inviteToChannel
INVITE_TO_CHANNEL_EXTRA_RPC_ENUMS=frozenset({'CHAT_MEMBER_ADD_FAILED','CHANNEL_MONOFORUM_UNSUPPORTED'})

def safe_error(exc):
    names={'UserPrivacyRestrictedError':'Настройки приватности пользователя запрещают приглашение.',
           'UserNotMutualContactError':'Telegram отклонил добавление этого получателя: требуется взаимный контакт с отправителем.',
           'ChatAdminRequiredError':'Не хватает прав администратора.',
           'ChatWriteForbiddenError':'В эту площадку нельзя отправлять сообщения.',
           'UserBannedInChannelError':'Аккаунт ограничен в этой площадке.',
           'PeerFloodError':'Telegram ограничил действия аккаунта. Задания остановлены; смена аккаунта автоматически не выполняется.',
           'UserChannelsTooMuchError':'У пользователя слишком много групп/каналов.',
           'ChannelPrivateError':'Площадка недоступна аккаунту.',
           'PhoneNumberBannedError':'Telegram заблокировал номер.'}
    return exc.detail if isinstance(exc,HTTPException) else names.get(type(exc).__name__,'Telegram: '+type(exc).__name__+'.')

def safe_rpc_metadata(exc):
    """Keep only SDK enum/constructor metadata, never exception text or request fields."""
    request=getattr(exc,'request',None)
    constructor=type(request).__name__ if isinstance(request,TLRequest) and type(request).__module__.startswith('telethon.tl.functions.') else None
    enums={cls:name for name,cls in errors.rpc_errors_dict.items() if re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',name)}
    for pattern,cls in errors.rpc_errors_re:
        prefix=re.match(r'[A-Z_]+',pattern)
        if prefix:enums.setdefault(cls,prefix.group().rstrip('_'))
    message=enums.get(type(exc))
    if message is None:
        candidate=getattr(exc,'message',None)
        allowed=set(enums.values())|{'BAD_REQUEST','UNAUTHORIZED','FORBIDDEN','NOT_FOUND','FLOOD','AUTH_KEY','SERVER_ERROR','TIMEOUT'}
        if type(request) is functions.channels.InviteToChannelRequest and getattr(exc,'code',None)==400:
            allowed|=INVITE_TO_CHANNEL_EXTRA_RPC_ENUMS
        if candidate in allowed:
            message=candidate
    code=getattr(exc,'code',None)
    return {'request_constructor':constructor,'exception_type':type(exc).__name__,
            'code':code if type(code) is int and -1000<=code<=1000 else None,'message':message}

def invite_rpc_diagnosis(exc):
    code=type(exc).__name__
    actions={'UserPrivacyRestrictedError':'Получатель может изменить разрешение на добавление в группы. После этого создайте отдельный подтверждённый повтор.',
             'UserNotMutualContactError':'Проверьте взаимные контакты отправителя и получателя. После изменения создайте отдельный подтверждённый повтор.',
             'ChatAdminRequiredError':'Проверьте права приглашать участников в выбранной группе.',
             'UserChannelsTooMuchError':'Получателю нужно освободить место в списке групп и каналов.',
             'UserBannedInChannelError':'Проверьте ограничения отправителя в группе.',
             'PeerFloodError':'Проверьте ограничения аккаунта в Telegram. Повтор и автоматическая замена аккаунта отключены.'}
    return {'source':'rpc_error','code':code,'reason':safe_error(exc),
            'next_action':actions.get(code,'Проверьте результат и ограничения в Telegram. Автоматического повтора нет.'),
            'retry_after_change':code in ('UserPrivacyRestrictedError','UserNotMutualContactError'),
            'rpc':safe_rpc_metadata(exc)}

def missing_invitee_diagnosis(invitee):
    premium=bool(getattr(invitee,'premium_would_allow_invite',False))
    premium_pm=bool(getattr(invitee,'premium_required_for_pm',False))
    if premium:
        code='premium_required_for_invite'
        reason='Telegram сообщил: для добавления этого участника отправителю требуется Telegram Premium.'
        action='Проверьте статус Premium отправителя. Панель ничего не покупает; повтор возможен только после изменения и отдельного подтверждения.'
    else:
        code='privacy_restricted'
        reason='Telegram сообщил: настройки приватности получателя запрещают прямое добавление в группу.'
        action='Получатель может разрешить добавление в группы. После изменения создайте отдельный подтверждённый повтор.'
        if premium_pm:
            action+=' Telegram также требует Premium отправителя для личного сообщения со ссылкой; это не разрешает прямое добавление. Сообщение автоматически не отправляется.'
    return {'source':'missing_invitees','code':code,'reason':reason,'next_action':action,
            'premium_would_allow_invite':premium,'premium_required_for_pm':premium_pm,'retry_after_change':True}

def participant_membership(permissions):
    """A permissions response can describe someone who has left or was kicked."""
    participant=getattr(permissions,'participant',None)
    if isinstance(participant,types.ChannelParticipantLeft):return False
    if isinstance(participant,types.ChannelParticipantBanned):
        return not bool(participant.left or participant.banned_rights.view_messages)
    if isinstance(participant,(types.ChannelParticipant,types.ChannelParticipantSelf,
                              types.ChannelParticipantAdmin,types.ChannelParticipantCreator,
                              types.ChatParticipant,types.ChatParticipantAdmin,types.ChatParticipantCreator)):
        return True
    return None  # An unrecognised response is not proof of membership.

def invite_response_evidence(result,*,target_id=None,user_id=None,sender_id=None):
    """Only types and invitation flags; never serialise Telegram entities/messages."""
    missing=getattr(result,'missing_invitees',[])
    updates=result if isinstance(result,(types.Updates,types.UpdatesCombined,types.UpdateShort)) else getattr(result,'updates',None)
    nested=getattr(updates,'updates',[])
    if not isinstance(nested,(list,tuple)):nested=[]
    if getattr(updates,'update',None) is not None:nested=[*nested,updates.update]
    events=[]
    if type(target_id) is int and type(user_id) is int:
        for update in nested:
            if not isinstance(update,(types.UpdateNewChannelMessage,types.UpdateNewMessage)):continue
            message=update.message
            if not isinstance(message,types.MessageService) or not isinstance(message.action,types.MessageActionChatAddUser):continue
            if not isinstance(message.peer_id,(types.PeerChannel,types.PeerChat)):continue
            if utils.get_peer_id(message.peer_id)!=target_id or user_id not in message.action.users:continue
            actor=getattr(message,'from_id',None)
            if sender_id is not None and (not isinstance(actor,types.PeerUser) or actor.user_id!=sender_id):continue
            events.append({'message_id':message.id,'chat_id':target_id,'user_id':user_id,
                           'actor_id':utils.get_peer_id(actor) if isinstance(actor,(types.PeerUser,types.PeerChannel,types.PeerChat)) else None,
                           'date':message.date.astimezone(timezone.utc).isoformat() if isinstance(message.date,datetime) and message.date.tzinfo else None})
            if len(events)>=10:break
    return {'response_type':type(result).__name__,'missing_invitees_present':hasattr(result,'missing_invitees'),
            'missing_invitees_count':len(missing),
            'missing_invitees':[{'user_id':m.user_id,
                                'premium_would_allow_invite':bool(getattr(m,'premium_would_allow_invite',False)),
                                'premium_required_for_pm':bool(getattr(m,'premium_required_for_pm',False))} for m in missing[:50]],
            'updates_type':type(updates).__name__ if updates is not None else None,
            'update_types':sorted({type(u).__name__ for u in nested})[:50],
            'add_user_events':events}

def unconfirmed_invite(reason,evidence=None,verification=None):
    return {'state':'uncertain','error':reason,'diagnosis':{
        'source':'membership_check','code':'invite_membership_unconfirmed','reason':reason,
        'next_action':'Проверьте членство в Telegram. Непроверенное приглашение не повторяется; оставшихся можно продолжить отдельным действием.',
        'retry_after_change':False,'response':evidence or {},'verification':verification or []}}

def invite_preflight_failure(exc):
    reason='Проверка перед приглашением не завершена. Запрос добавления не отправлен. '+safe_error(exc)
    return {'state':'failed','error':reason,'diagnosis':{
        'source':'invite_preflight','code':'invite_preflight_failed','reason':reason,
        'error_type':type(exc).__name__,'invitation_requested':False,'retry_after_change':False,
        'next_action':'Проверьте доступность получателя, подключение аккаунта и права в группе. Приглашение не запускалось.'}}

def contact_flags(user):
    known=isinstance(user,types.User) and not user.min
    return {'sender_has_contact':user.contact if known and isinstance(user.contact,bool) else None,
            'mutual_contact':user.mutual_contact if known and isinstance(user.mutual_contact,bool) else None}

PHONE_VISIBILITY_NOTICE='Видимость номера определяется текущими настройками Telegram; отдельное исключение для контакта не добавляется.'

# Telegram's published CIDRs; imported session endpoints never target local/private hosts.
TG_NETWORKS=[ipaddress.ip_network(x) for x in ['91.108.56.0/22','91.108.4.0/22','91.108.8.0/22','91.108.16.0/22','91.108.12.0/22','149.154.160.0/20','91.105.192.0/23','91.108.20.0/22','185.76.151.0/24','2001:b28:f23d::/48','2001:b28:f23f::/48','2001:67c:4e8::/48','2001:b28:f23c::/48','2a0a:f280::/32']]

def parse_import(payload):
    if len(payload)>40:raise HTTPException(400,'Слишком много полей в JSON.')
    if payload.get('format','chatcleaner.telethon.v1')!='chatcleaner.telethon.v1':raise HTTPException(400,'Поддерживается формат chatcleaner.telethon.v1.')
    label=payload.get('label') or payload.get('username') or 'Аккаунт из JSON'
    if not isinstance(label,str) or not 1<=len(label.strip())<=80:raise HTTPException(400,'Неверное название аккаунта.')
    aid=payload.get('api_id',payload.get('app_id'));hash_=payload.get('api_hash',payload.get('app_hash'))
    if (aid is None)!=(hash_ is None):raise HTTPException(400,'API ID и API hash должны быть указаны вместе.')
    api=None
    if aid is not None:
        if isinstance(aid,bool) or not str(aid).isdigit() or not 0<int(aid)<2147483647 or not isinstance(hash_,str) or len(hash_)!=32 or any(c not in '0123456789abcdefABCDEF' for c in hash_):raise HTTPException(400,'Неверный формат API ID / API hash.')
        api={'id':int(aid),'hash':hash_}
    session=payload.get('session_string',payload.get('string_session'))
    parsed=None
    if session is not None:
        if not isinstance(session,str) or len(session)>2048:raise HTTPException(400,'Ожидается строка Telethon StringSession.')
        try:
            parsed=StringSession(session)
            addr=ipaddress.ip_address(parsed.server_address)
            if not parsed.auth_key or not 1<=parsed.dc_id<=5 or parsed.port not in (80,443,5222) or not any(addr in net for net in TG_NETWORKS):raise ValueError()
        except Exception:raise HTTPException(400,'Не удалось проверить Telethon StringSession или адрес дата-центра Telegram. Для .session и tdata ZIP используйте отдельную форму импорта файла.') from None
    phone=payload.get('phone')
    if phone is not None:
        if not isinstance(phone,(int,str)):raise HTTPException(400,'Неверный номер в JSON.')
        phone='+'+str(phone).lstrip('+')
        try:Phone(phone=phone)
        except Exception:raise HTTPException(400,'Неверный номер в JSON.') from None
    uid=payload.get('user_id')
    if uid is not None and (isinstance(uid,bool) or not str(uid).isdigit() or not 0<int(uid)<2**63):raise HTTPException(400,'Неверный user_id.')
    return {'label':label.strip(),'api':api,'session':parsed,'phone':phone,'user_id':int(uid) if uid else None,
            'ignored_fields':[k for k in payload if k not in {'format','label','username','phone','user_id','api_id','api_hash','app_id','app_hash','session_string','string_session'}]}

class PrimaryAccount:
    def __init__(self,ops):self.ops=ops;self.aid='primary';self.error=None
    @property
    def client(self):return self.ops.get_primary().client
    @property
    def me(self):return self.ops.get_primary().me
    @property
    def status(self):return 'connected' if self.me and self.client and self.client.is_connected() else 'not_connected'
    def pause(self):
        with self.ops.store.db:
            self.ops.store.db.execute("UPDATE op_accounts SET enabled=0 WHERE id='primary'")
            self.ops.store.db.execute("UPDATE op_jobs SET state='paused' WHERE account='primary' AND state IN ('queued','scheduled')")
    def report_error(self,exc):self.error=safe_error(exc)
    def outbound_guard(self):return self.ops.online(self.aid)
    async def connect(self):pass

class PoolAccount(OwnerSender):
    def __init__(self,ops,aid):
        self.ops,self.aid=ops,aid
        super().__init__(ops.data/(aid+'.session'),lambda:ops.store.get('account_api_'+aid) or ops.get_api(),ops.store)
    def pause(self):
        with self.store.db:
            self.store.db.execute("UPDATE op_accounts SET enabled=0 WHERE id=?",(self.aid,))
            self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE account=? AND state IN ('queued','scheduled')",(now(),self.aid))
    def outbound_guard(self):return self.ops.online(self.aid)
    async def connect(self):
        async with self.connect_lock:
            api=self.get_api()
            if not api:return
            if self.client and self.client.is_connected() and self.me:return
            client=self.client
            if not client:
                client=self.ops.make_client(self.session_path,api,self.store.get('account_profile_'+self.aid),self.ops.proxy_connection(self.aid))
                self.client=client
            self.me=None
            if not self.challenge_active():self.status='not_connected'
            async def finish():
                if not client.is_connected():await client.connect()
                if await client.is_user_authorized():await self.ready()
            try:
                await asyncio.wait_for(finish(),20)
            except asyncio.TimeoutError:
                # Keep the imported session and client; only reset a partial transport.
                if self.client is client:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(client.disconnect(),5)
                raise
    async def ready(self):
        me=await self.client.get_me();row=self.ops.account_row(self.aid)
        duplicate=self.store.rows('SELECT id FROM op_accounts WHERE telegram_id=? AND id!=?',(me.id,self.aid)) if me else []
        if not me or me.bot or (row['telegram_id'] and row['telegram_id']!=me.id) or duplicate:
            await self.client.disconnect();self.client=None;self.me=None;self.status='not_connected';self.pause()
            raise HTTPException(409,'Аккаунт уже добавлен или отличается от ранее подключённого. Используйте его существующую запись.')
        # Connection readiness does not grant a new outbound permission to an
        # existing identity. The fresh row above also preserves a pause applied
        # while get_me was awaiting Telegram; only first authorization enables.
        existing=row['telegram_id'] is not None
        enabled=row['enabled'] if existing else 1
        runtime_error=self.error if existing else None
        error=(row['error'] or runtime_error) if existing else None
        if error:enabled=0
        self.me=me;self.status='connected'
        self.error=runtime_error or error
        self.phone=self.code_hash=None;self.auth_until=0
        with self.store.db:self.store.db.execute('UPDATE op_accounts SET telegram_id=?,username=?,enabled=?,error=? WHERE id=?',(me.id,me.username,enabled,error,self.aid))
    async def disconnect(self):
        if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending'",(self.aid,)):
            raise HTTPException(409,'Дождитесь результата текущей операции. Остальные задания можно поставить на паузу.')
        async with self.lock:
            self.pause()
            if self.client:await self.client.disconnect()
            self.client=None;self.me=None;self.status='not_connected';self.phone=self.code_hash=None
            self.error=None;self.auth_until=0
            for suffix in ('','-journal','-wal','-shm'):Path(str(self.session_path)+suffix).unlink(missing_ok=True)
        return {'ok':True}

class ClientTelegramClient(TelegramClient):
    """No ambiguous RPC retries; retain one explicitly rejected unauthorised auth redirect."""
    async def _call(self,sender,request,ordered=False,flood_sleep_threshold=None):
        auth_code=isinstance(request,(functions.auth.SendCodeRequest,functions.auth.ResendCodeRequest))
        previous_dc=self.session.dc_id if auth_code else None
        try:return await super()._call(sender,request,ordered=ordered,flood_sleep_threshold=flood_sleep_threshold)
        except (errors.PhoneMigrateError,errors.NetworkMigrateError) as exc:
            if (not auth_code or exc.code!=303 or previous_dc==exc.new_dc or
                self.session.dc_id!=exc.new_dc or await self.is_user_authorized()):
                raise
            # The SDK already switched DC after the server rejected the first auth request.
            # Calling the base implementation once bounds redirects and never retries invites.
            return await super()._call(self._sender,request,ordered=ordered,flood_sleep_threshold=flood_sleep_threshold)

class Operations:
    def __init__(self,data,get_api,get_primary=None,get_client_store=None):
        self.data=Path(data)/'operations';self.data.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.store=Store(self.data/'operations.sqlite3');self.get_api=get_api;self.get_primary=get_primary
        self.get_client_store=get_client_store
        self.accounts={};self.lock=asyncio.Lock();self.import_lock=asyncio.Lock();self.task=None;self.last_connect=0
        self.destination_cache={};self.destination_locks={}
        self.media_store=Publisher(lambda:self.store,lambda:None,self.data)
        self.router=APIRouter(prefix='/api/scanner/operations')
        routes=[('',self.state,['GET']),('/accounts',self.add_account,['POST']),('/accounts/import',self.import_account,['POST']),('/accounts/{aid}/code',self.code,['POST']),('/accounts/{aid}/confirm',self.confirm,['POST']),('/accounts/{aid}/password',self.password,['POST']),('/accounts/{aid}/disconnect',self.disconnect,['POST']),('/accounts/{aid}/remove',self.remove_account,['POST']),('/accounts/{aid}/destinations',self.destinations,['GET']),('/accounts/{aid}/resolve',self.resolve,['POST']),('/media',self.media_store.upload,['POST']),('/media/{media_id}',self.media_store.media,['GET']),('/jobs',self.create,['POST']),('/jobs/{jid}',self.detail,['GET']),('/jobs/{jid}/control',self.control,['POST']),('/jobs/{jid}/report',self.report,['GET']),('/jobs/{jid}/remove-messages',self.remove_messages,['POST'])]
        for path,fn,methods in routes:self.router.add_api_route(path,fn,methods=methods)
        self.router.add_api_route('/jobs',self.job_history,methods=['GET'])
        self.router.add_api_route('/accounts/import-file',self.import_file,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/proxy',self.set_proxy,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/reconnect',self.reconnect,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/membership',self.membership,methods=['GET'])
        self.router.add_api_route('/accounts/{aid}/diagnostics',self.account_diagnostics,methods=['GET'])
        self.router.add_api_route('/jobs/{jid}/items/{iid}/membership',self.item_membership,methods=['GET'])
        self.router.add_api_route('/jobs/{jid}/items/{iid}/single-preview',self.single_preview,methods=['GET'])
        self.router.add_api_route('/jobs/{jid}/items/{iid}/single-attempt',self.start_single_attempt,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/contacts/add',self.add_contact,methods=['POST'])
        self.router.add_api_route('/accounts/primary/resume-outbound',self.resume_primary_outbound,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/resume-outbound',self.resume_secondary_outbound,methods=['POST'])
        self.router.add_api_route('/jobs/{jid}/retry-invite',self.retry_invite,methods=['POST'])
        self.router.add_api_route('/jobs/{jid}/pilot-preview',self.pilot_preview,methods=['GET'])
        self.router.add_api_route('/jobs/{jid}/pilot',self.start_pilot,methods=['POST'])
        self.audience=Audience(self);self.router.include_router(self.audience.router)
        self.tracking=OperationTracking(self);self.router.include_router(self.tracking.router)
        self.test_chat=OperationTestChat(self);self.router.include_router(self.test_chat.router)
        self.campaigns=OperationCampaigns(self);self.router.include_router(self.campaigns.router)
        self.folders=OperationFolders(self);self.router.include_router(self.folders.router)
        self.redirects=OperationRedirects(self);self.router.include_router(self.redirects.router)
        self.budgets=OperationBudgets(self);self.router.include_router(self.budgets.router)
        self.invite_runs=InviteRunBudget(self)
        self.router.add_api_route('/invite-run-budgets/preview',self.invite_run_preview,methods=['GET'])
        self.router.add_api_route('/invite-run-budgets/{request_key}',self.invite_run_status,methods=['GET'])
        self.router.add_api_route('/invite-run-budgets',self.grant_invite_run_budget,methods=['POST'])
        self.workbench=OperationWorkbench(self);self.router.include_router(self.workbench.router)
        self.topics=OperationTopics(self);self.router.include_router(self.topics.router)
        self.qa_invite_adminlog=QaInviteAdminLog(self);self.router.include_router(self.qa_invite_adminlog.router)
        self.isolated_invite=IsolatedInvite(self);self.router.include_router(self.isolated_invite.router)
        self.isolated_contact_batch=IsolatedContactBatch(self);self.router.include_router(self.isolated_contact_batch.router)
        self.isolated_invite_campaign=IsolatedInviteCampaign(self);self.router.include_router(self.isolated_invite_campaign.router)
        self.manual_invite_campaign=ManualInviteCampaign(self);self.router.include_router(self.manual_invite_campaign.router)
        from spambot_check import SpamBotCheck
        self.spambot_check=SpamBotCheck(self);self.router.include_router(self.spambot_check.router)
    @staticmethod
    def make_client(session,api,profile=None,proxy=None):
        client=ClientTelegramClient(session,api['id'],api['hash'],device_model='Chatcleaner Operations',app_version='1.2',flood_sleep_threshold=0,proxy=proxy,request_retries=0,raise_last_call_error=True)
        if profile=='tdesktop':client._init_request.lang_pack='tdesktop'
        return client
    async def import_file(self,request:Request):
        # Bound chunked bodies as well as Content-Length; no uploaded filename is used.
        if self.import_lock.locked():raise HTTPException(409,'Дождитесь завершения текущего импорта.')
        async with self.import_lock:
            body=bytearray()
            async for chunk in request.stream():
                if len(body)+len(chunk)>MAX_REQUEST:raise HTTPException(413,'Файл слишком большой. Максимум 8 МБ.')
                body.extend(chunk)
            try:spec=FileImportSpec.model_validate_json(body)
            except Exception:raise HTTPException(422,'Проверьте файл, формат и подтверждение права использовать аккаунт.') from None
            finally:body.clear()
            try:
                blob=base64.b64decode(spec.data,validate=True);spec.data=''
                payload=await asyncio.to_thread(decode_file,spec.kind,blob,spec.passcode,spec.account_index)
                if spec.kind=='telethon':
                    if spec.api_id is not None or spec.api_hash is not None:
                        payload.update(api_id=spec.api_id,api_hash=spec.api_hash)
                payload['label']=spec.label
                parsed=parse_import(payload)
            except FileImportError as exc:raise HTTPException(400,str(exc)) from None
            except (binascii.Error,ValueError,TypeError,OverflowError):raise HTTPException(400,'Не удалось прочитать файл сессии.') from None
            finally:spec.passcode='';spec.data='';blob=None
            return await self.import_parsed(parsed,'tdesktop' if spec.kind=='tdata' else None,spec.proxy)
    def initialize(self):
        self.media_store.initialize()
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_accounts(id TEXT PRIMARY KEY,label TEXT NOT NULL,telegram_id INTEGER UNIQUE,username TEXT,enabled INTEGER NOT NULL DEFAULT 0,not_before REAL NOT NULL DEFAULT 0,error TEXT,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS op_jobs(id INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,account TEXT NOT NULL REFERENCES op_accounts(id),telegram_id INTEGER NOT NULL,kind TEXT NOT NULL,title TEXT NOT NULL,spec TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'draft',not_before REAL NOT NULL DEFAULT 0,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS op_items(id INTEGER PRIMARY KEY,job INTEGER NOT NULL REFERENCES op_jobs(id),chat_id INTEGER NOT NULL,chat_title TEXT NOT NULL,chat_username TEXT,user_id INTEGER,user_name TEXT,state TEXT NOT NULL DEFAULT 'pending',random_ids TEXT NOT NULL,message_ids TEXT NOT NULL DEFAULT '[]',invite_link TEXT,error TEXT,attempted_at TEXT,finished_at TEXT);
        CREATE TABLE IF NOT EXISTS op_invited(chat_id INTEGER NOT NULL,user_id INTEGER NOT NULL,state TEXT NOT NULL,job INTEGER NOT NULL,PRIMARY KEY(chat_id,user_id));
        ''')
        item_columns={r['name'] for r in self.store.rows('PRAGMA table_info(op_items)')}
        if 'user_access_hash' not in item_columns:
            with self.store.db:self.store.db.execute('ALTER TABLE op_items ADD COLUMN user_access_hash INTEGER')
        if 'diagnosis' not in item_columns:
            with self.store.db:self.store.db.execute("ALTER TABLE op_items ADD COLUMN diagnosis TEXT NOT NULL DEFAULT '{}'")
        self.audience.initialize()
        with self.store.db:
            self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id IN (SELECT job FROM op_items WHERE state='sending')")
            self.store.db.execute("UPDATE op_items SET state='uncertain',error='Перезапуск во время операции. Результат нужно проверить в Telegram; автоматического повтора нет.' WHERE state='sending'")
            self.store.db.execute("INSERT OR IGNORE INTO op_invited SELECT i.chat_id,i.user_id,'uncertain',i.job FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.kind='invite' AND i.state='uncertain'")
            for row in self.store.rows("SELECT value FROM settings WHERE key LIKE 'single_attempt_job_%'"):
                record=json.loads(row['value'])
                if isinstance(record,dict) and record.get('state') in ('armed','claimed'):
                    record.update(state='interrupted',finished_at=now())
                    self.save_single_attempt(record)
                    self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'",(record['job_id'],))
        for a in self.store.rows("SELECT id FROM op_accounts WHERE id!='primary'"):self.accounts[a['id']]=PoolAccount(self,a['id'])
        self.sync_primary()
        self.campaigns.initialize()
        self.tracking.initialize()
        self.test_chat.initialize()
        self.folders.initialize()
        self.redirects.initialize()
        self.budgets.initialize()
        self.invite_runs.initialize()
        self.workbench.initialize()
        self.isolated_contact_batch.initialize()
        self.isolated_invite_campaign.initialize()
        self.manual_invite_campaign.initialize()
        self.spambot_check.initialize()

    async def invite_run_preview(self):
        async with self.lock:
            return {k:v for k,v in self.invite_runs.preview().items() if k!='_envelope'}

    async def invite_run_status(self,request_key:str):
        async with self.lock:return self.invite_runs.status(request_key)

    async def grant_invite_run_budget(self,spec:InviteRunGrant):
        async with self.lock:
            account=self.accounts.get('primary')
            with self.store.db:
                return self.invite_runs.grant(**spec.model_dump(),client=getattr(account,'client',None))
    def sync_primary(self):
        if not self.get_primary:return
        if 'primary' not in self.accounts:
            self.accounts['primary']=PrimaryAccount(self)
            with self.store.db:self.store.db.execute("INSERT OR IGNORE INTO op_accounts(id,label,enabled,created) VALUES('primary','Основной аккаунт лидсканера',1,?)",(now(),))
        a=self.accounts['primary'];old=self.account_row('primary')
        if a.me:
            if old['telegram_id'] and old['telegram_id']!=a.me.id:a.pause()
            if old['telegram_id']!=a.me.id or old['username']!=a.me.username:
                with self.store.db:self.store.db.execute("UPDATE op_accounts SET telegram_id=?,username=? WHERE id='primary'",(a.me.id,a.me.username))
    def account_row(self,aid):
        rows=self.store.rows('SELECT * FROM op_accounts WHERE id=?',(aid,))
        if not rows:raise HTTPException(404,'Аккаунт не найден.')
        return rows[0]
    def online(self,aid):
        a=self.accounts.get(aid);row=self.account_row(aid)
        if a and not row['enabled'] and a.me and a.client and a.client.is_connected():
            raise HTTPException(409,'Исходящие действия аккаунта приостановлены. '+(row['error'] or a.error or 'Требуется проверка ограничений Telegram.')+' Откройте «Аккаунты и журнал» и отдельно подтвердите восстановление исходящих.')
        if not a or not row['enabled'] or not a.me or not a.client or not a.client.is_connected():raise HTTPException(409,'Подключите выбранный Telegram-аккаунт.')
        if a.me.id!=row['telegram_id']:raise HTTPException(409,'Изменилась личность отправителя.')
        if row['not_before']>time.time():raise HTTPException(429,'Telegram ограничил аккаунт до '+datetime.fromtimestamp(row['not_before'],timezone.utc).isoformat())
        return a

    def readable_account(self,aid,*,check_cooldown=True):
        """A connected account may inspect membership while outbound writes stay paused."""
        a=self.accounts.get(aid);row=self.account_row(aid)
        if not a or not a.me or not a.client or not a.client.is_connected():
            raise HTTPException(409,'Подключите выбранный Telegram-аккаунт для проверки членства.')
        if a.me.id!=row['telegram_id']:raise HTTPException(409,'Изменилась личность отправителя.')
        if check_cooldown and row['not_before']>time.time():
            raise HTTPException(429,'Telegram ограничил аккаунт до '+datetime.fromtimestamp(row['not_before'],timezone.utc).isoformat())
        return a

    def pause_outbound(self,aid,exc):
        a=self.accounts.get(aid)
        if not a:return
        a.pause();a.report_error(exc)
        with self.store.db:
            self.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',(safe_error(exc),aid))
            key='outbound_pause_revision_'+aid
            self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                  (key,json.dumps(self.store.get(key,0)+1)))

    async def resume_primary_outbound(self,spec:ResumeOutbound):
        if len(spec.reason.strip())<8:raise HTTPException(400,'Опишите причину восстановления исходящих.')
        async with self.lock:
            a=self.accounts.get('primary');row=self.account_row('primary')
            if not a or not a.me or not a.client or not a.client.is_connected():raise HTTPException(409,'Сначала подключите основной аккаунт в лидсканере.')
            if spec.telegram_id!=row['telegram_id'] or a.me.id!=spec.telegram_id:raise HTTPException(409,'Отправитель изменился. Обновите панель и проверьте основной аккаунт.')
            if row['not_before']>time.time():raise HTTPException(429,'Срок ожидания Telegram ещё не завершён.')
            if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account='primary' AND i.state='sending' LIMIT 1"):
                raise HTTPException(409,'Дождитесь завершения текущего действия основного аккаунта.')
            client=a.client
            pause_revision=self.store.get('outbound_pause_revision_primary',0)
            try:me=await asyncio.wait_for(client.get_me(),15)
            except errors.FloodWaitError as exc:
                if a.client is client and a.me and a.me.id==spec.telegram_id:
                    with self.store.db:self.store.db.execute("UPDATE op_accounts SET not_before=MAX(not_before,?),error=? WHERE id='primary'",(time.time()+exc.seconds+1,f'Telegram просит подождать {exc.seconds} секунд.'))
                raise HTTPException(429,'Telegram просит подождать '+str(exc.seconds)+' секунд. Исходящие не включены.') from None
            except Exception as exc:raise HTTPException(409,'Telegram не подтвердил подключение ('+type(exc).__name__+'). Исходящие не включены.') from None
            current=self.account_row('primary')
            if (not isinstance(me,types.User) or me.bot or me.id!=spec.telegram_id or a.client is not client or
                not a.me or a.me.id!=spec.telegram_id or current['telegram_id']!=spec.telegram_id or
                current['not_before']>time.time() or current['error']!=row['error'] or
                self.store.get('outbound_pause_revision_primary',0)!=pause_revision):
                raise HTTPException(409,'Аккаунт или его ограничения изменились во время проверки. Исходящие не включены.')
            with self.store.db:
                self.store.db.execute("UPDATE op_accounts SET enabled=1,error=NULL WHERE id='primary'")
                self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                      ('last_outbound_resume_primary',json.dumps({'telegram_id':me.id,'reason':spec.reason.strip(),'at':now()})))
            a.error=None
            return {'ok':True,'account':'primary','telegram_id':me.id,'jobs_resumed':False,'pending_outgoing_may_continue':True,
                    'note':'Исходящие разрешены после проверки. Ожидающие ручные сообщения, уведомления аккаунтом и публикации могут продолжиться. Задания инвайтинга и рассылок остаются на паузе; failed и unknown не повторяются.'}
    async def resume_secondary_outbound(self,aid:str,spec:ResumeOutbound):
        if aid=='primary':raise HTTPException(403,'Используйте отдельное подтверждение основного аккаунта.')
        if len(spec.reason.strip())<8:raise HTTPException(400,'Опишите причину восстановления исходящих.')
        async with self.lock:
            a=self.auth(aid);row=self.account_row(aid)
            if (not row['telegram_id'] or spec.telegram_id!=row['telegram_id'] or not a.me or
                a.me.id!=spec.telegram_id or not a.client or not a.client.is_connected()):
                raise HTTPException(409,'Подключите и проверьте выбранный Telegram-аккаунт.')
            if row['not_before']>time.time():raise HTTPException(429,'Срок ожидания Telegram ещё не завершён.')
            if self.store.rows("SELECT 1 FROM op_jobs WHERE telegram_id=? AND state IN ('queued','scheduled') LIMIT 1",(spec.telegram_id,)):
                raise HTTPException(409,'Сначала приостановите сохранённые задания отправителя; восстановление не запускает старую очередь.')
            if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.telegram_id=? AND i.state='sending' LIMIT 1",(spec.telegram_id,)):
                raise HTTPException(409,'Дождитесь завершения текущего действия этого отправителя.')
            for saved in self.store.rows("SELECT value FROM settings WHERE key LIKE 'manual_invite_campaign_job_%'"):
                record=json.loads(saved['value'])
                if record.get('state') in ('running','waiting') and record.get('config',{}).get('telegram_id')==spec.telegram_id:
                    raise HTTPException(409,'Сначала приостановите текущую ручную кампанию отправителя.')
            for controller in (self.isolated_invite_campaign,self.isolated_contact_batch):
                active=getattr(controller,'active',None)
                if isinstance(active,dict) and active.get('account') is a:
                    raise HTTPException(409,'Сначала приостановите текущий изолированный контроль отправителя.')
            client=a.client;runtime_error=a.error;pause_revision=self.store.get('outbound_pause_revision_'+aid,0)
            try:me=await asyncio.wait_for(client.get_me(),15)
            except (errors.FloodWaitError,errors.PeerFloodError,*REVOKED) as exc:
                current=self.account_row(aid)
                if self.accounts.get(aid) is a and a.client is client and current['telegram_id']==spec.telegram_id:
                    with self.store.db:
                        if isinstance(exc,errors.FloodWaitError):
                            self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?),error=? WHERE id=?',
                                (time.time()+exc.seconds+1,f'Telegram просит подождать {exc.seconds} секунд.',aid))
                        else:
                            self.store.db.execute('UPDATE op_accounts SET enabled=0,error=? WHERE id=?',(safe_error(exc),aid))
                            key='outbound_pause_revision_'+aid
                            self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                (key,json.dumps(self.store.get(key,0)+1)))
                    a.error=safe_error(exc)
                raise HTTPException(429 if isinstance(exc,errors.FloodWaitError) else 409,
                    'Telegram остановил проверку ('+type(exc).__name__+'). Исходящие не включены.') from None
            except Exception as exc:
                raise HTTPException(409,'Telegram не подтвердил подключение ('+type(exc).__name__+'). Исходящие не включены.') from None
            current=self.account_row(aid)
            if (not isinstance(me,types.User) or me.bot or me.deleted or me.id!=spec.telegram_id or self.accounts.get(aid) is not a or
                a.client is not client or not client.is_connected() or a.error!=runtime_error or not a.me or a.me.id!=spec.telegram_id or
                any(current[k]!=row[k] for k in ('telegram_id','enabled','error','not_before')) or
                current['not_before']>time.time() or self.store.get('outbound_pause_revision_'+aid,0)!=pause_revision):
                raise HTTPException(409,'Аккаунт или его ограничения изменились во время проверки. Исходящие не включены.')
            with self.store.db:
                self.store.db.execute('UPDATE op_accounts SET enabled=1,error=NULL WHERE id=?',(aid,))
                self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                    ('last_outbound_resume_'+aid,json.dumps({'telegram_id':me.id,'reason':spec.reason.strip(),'at':now()})))
            a.error=None
            return {'ok':True,'account':aid,'telegram_id':me.id,'jobs_resumed':False,'pending_outgoing_may_continue':True,
                'note':'Исходящие разрешены отдельно. Сохранённые задания остаются на паузе; failed и unknown не повторяются. Ожидающие ручные ответы могут продолжиться.'}

    async def state(self):
        self.sync_primary()
        accounts=[]
        for row in self.store.rows('SELECT * FROM op_accounts ORDER BY created'):
            a=self.accounts[row['id']]
            row['connection']=a.status if a.client and a.client.is_connected() else 'not_connected'
            row['proxy']=self.proxy_public(row['id']);row['error']=a.error or row['error'];row['primary']=row['id']=='primary';accounts.append(row)
        invite_rows=self.store.rows("SELECT i.chat_id,i.chat_title,i.user_id,i.state,i.attempted_at,i.finished_at,i.diagnosis,j.account,j.telegram_id,j.state AS job_state FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.kind='invite' ORDER BY i.id")
        bindings={a['id']:{'enabled':a['enabled'],'connected':a['connection']=='connected','telegram_id':a['telegram_id']} for a in accounts}
        return {'accounts':accounts,'api_available':bool(self.get_api()),'jobs':[await self.detail(j['id']) for j in self.store.rows('SELECT id FROM op_jobs ORDER BY id DESC LIMIT 100')],
                'invite_summary':summarize_invites(invite_rows,now()[:10],bindings),'worker_error':self.store.get('worker_error')}

    async def job_history(self,kind:Literal['post','invite','link','invite_message','message']|None=None,
                          chat_id:Annotated[int|None,Query(ge=-(2**63),lt=2**63)]=None,pending_only:bool=False,
                          before_id:Annotated[int|None,Query(gt=0,lt=2**63)]=None,
                          limit:Annotated[int,Query(ge=1,le=50)]=25):
        """Read saved jobs without syncing accounts or contacting Telegram."""
        where=[];values=[]
        if kind is not None:where.append('j.kind=?');values.append(kind)
        if before_id is not None:where.append('j.id<?');values.append(before_id)
        if chat_id is not None or pending_only:
            item_where=['i.job=j.id']
            if chat_id is not None:item_where.append('i.chat_id=?');values.append(chat_id)
            if pending_only:item_where.append("i.state='pending'")
            where.append('EXISTS (SELECT 1 FROM op_items i WHERE '+' AND '.join(item_where)+')')
        sql='SELECT j.id FROM op_jobs j'+(' WHERE '+' AND '.join(where) if where else '')+' ORDER BY j.id DESC LIMIT ?'
        rows=self.store.rows(sql,(*values,limit+1));has_more=len(rows)>limit
        page=rows[:limit]
        return {'jobs':[await self.detail(row['id']) for row in page],'has_more':has_more,
                'next_before_id':page[-1]['id'] if has_more else None,
                'filters':{'kind':kind,'chat_id':chat_id,'pending_only':pending_only},'limit':limit}

    def proxy_connection(self,aid):
        config=self.store.get('account_proxy_'+aid)
        return ProxySpec.model_validate(config).connection() if config else None
    def proxy_public(self,aid):
        config=self.store.get('account_proxy_'+aid) or {'mode':'direct'}
        return {k:v for k,v in config.items() if k!='password'} | {'password_saved':bool(config.get('password'))}
    async def set_proxy(self,aid:str,spec:ProxySpec):
        async with self.lock:
            a=self.auth(aid)
            if self.store.rows("SELECT 1 FROM op_jobs WHERE account=? AND state IN ('queued','scheduled')",(aid,)) or self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending'",(aid,)):
                raise HTTPException(409,'Сначала приостановите задания аккаунта и дождитесь завершения текущего действия.')
            async with a.lock:
                a.pause()
                if a.client:await a.client.disconnect()
                a.client=None;a.me=None;a.status='not_connected';a.phone=a.code_hash=None;a.auth_until=0
                self.store.put('account_proxy_'+aid,spec.model_dump())
                self.destination_cache.pop(aid,None)
        return {'proxy':self.proxy_public(aid),'note':'Настройки сохранены. Проверьте соединение. Задания остаются на паузе.'}
    async def reconnect(self,aid:str):
        a=self.auth(aid)
        async with a.lock:
            try:
                async with asyncio.timeout(20):
                    await a.connect()
                    if a.client:await a.client.get_me()
            except Exception as exc:
                a.pause();a.report_error(exc)
                if a.client:await a.client.disconnect()
                a.client=None;a.me=None;a.status='not_connected'
                raise HTTPException(409,'Соединение не установлено ('+type(exc).__name__+'). Проверьте прокси и доступность Telegram. Прямое подключение вместо прокси не используется.') from None
        return {'connection':a.status,'authorized':bool(a.me),'proxy':self.proxy_public(aid)}
    async def add_account(self,spec:AccountSpec):
        async with self.lock:
            if len([x for x in self.accounts if x!='primary'])>=5:raise HTTPException(400,'В пилоте доступны пять отдельных аккаунтов.')
            if not spec.label.strip():raise HTTPException(400,'Укажите название аккаунта.')
            aid=uuid.uuid4().hex
            with self.store.db:self.store.db.execute('INSERT INTO op_accounts(id,label,created) VALUES(?,?,?)',(aid,spec.label.strip(),now()))
            if spec.proxy:self.store.put('account_proxy_'+aid,spec.proxy.model_dump())
            self.accounts[aid]=PoolAccount(self,aid)
            return self.account_row(aid)
    def auth(self,aid):
        if aid=='primary':raise HTTPException(403,'Подключение основного аккаунта управляется в «Лидсканер → Подключение».')
        self.account_row(aid);return self.accounts[aid]
    async def code(self,aid:str,spec:Phone):return await self.auth(aid).code(spec)
    async def confirm(self,aid:str,spec:Code):return await self.auth(aid).confirm(spec)
    async def password(self,aid:str,spec:Password):return await self.auth(aid).password(spec)
    async def disconnect(self,aid:str):return await self.auth(aid).disconnect()
    async def remove_account(self,aid:str):
        async with self.lock:
            a=self.auth(aid)
            if a.me or self.store.rows('SELECT 1 FROM op_jobs WHERE account=?',(aid,)):raise HTTPException(409,'Можно убрать только неподключённую запись без заданий.')
            if a.client:await a.client.disconnect()
            with self.store.db:
                self.store.db.execute('DELETE FROM op_accounts WHERE id=?',(aid,))
                self.store.db.execute('DELETE FROM settings WHERE key=?',('account_api_'+aid,))
                self.store.db.execute('DELETE FROM settings WHERE key=?',('account_profile_'+aid,))
                self.store.db.execute('DELETE FROM settings WHERE key=?',('account_proxy_'+aid,))
            for suffix in ('','-journal','-wal','-shm'):Path(str(a.session_path)+suffix).unlink(missing_ok=True)
            del self.accounts[aid]
            return {'removed':True}
    async def import_account(self,spec:ImportSpec):
        parsed=parse_import(spec.payload)
        if self.import_lock.locked():raise HTTPException(409,'Дождитесь завершения текущего импорта.')
        async with self.import_lock:return await self.import_parsed(parsed,proxy=spec.proxy)
    async def import_parsed(self,parsed,profile=None,proxy=None):
        self.sync_primary()
        # No actions are executed from JSON. Only whitelisted fields are interpreted.
        record=await self.add_account(AccountSpec(label=parsed['label'],authorized=True,proxy=proxy));aid=record['id'];a=self.accounts[aid]
        if parsed['api']:self.store.put('account_api_'+aid,parsed['api'])
        if profile:self.store.put('account_profile_'+aid,profile)
        if not parsed['session']:
            return {'account':self.account_row(aid),'connection':'not_connected','needs_auth':True,'ignored_fields':parsed['ignored_fields'],'note':'JSON содержит настройки. Для входа введите номер, код и облачный пароль, если Telegram запросит его. Путь session_file не является авторизацией.'}
        api=a.get_api()
        if not api:
            a.error='В JSON нет API ID/hash и приложение сервиса не настроено.'
            return {'account':self.account_row(aid),'connection':'not_connected','needs_auth':True,'error':a.error}
        client=self.make_client(parsed['session'],api,profile,self.proxy_connection(aid))
        try:
            await asyncio.wait_for(client.connect(),20)
            if not await asyncio.wait_for(client.is_user_authorized(),15):raise HTTPException(400,'Сессия не авторизована или отозвана. Подключитесь по номеру и коду.')
            me=await asyncio.wait_for(client.get_me(),15)
            if not me or me.bot or (parsed['user_id'] and me.id!=parsed['user_id']) or (parsed['phone'] and str(me.phone or '').lstrip('+')!=parsed['phone'].lstrip('+')):raise HTTPException(400,'Личность сессии не совпадает с данными JSON.')
            if self.store.rows('SELECT 1 FROM op_accounts WHERE telegram_id=?',(me.id,)):raise HTTPException(409,'Этот Telegram-аккаунт уже подключён. Используйте существующую запись.')
            await client.disconnect()
            session=SQLiteSession(a.session_path);session.set_dc(parsed['session'].dc_id,parsed['session'].server_address,parsed['session'].port);session.auth_key=parsed['session'].auth_key;session.save();session.close()
            Path(a.session_path).chmod(0o600)
            await a.connect()
            if not a.me:raise HTTPException(400,'Telegram не подтвердил авторизацию импортированной сессии.')
            return {'account':self.account_row(aid),'connection':'connected','needs_auth':False,'ignored_fields':parsed['ignored_fields']}
        except Exception as exc:
            # Do not log out an imported key: that could revoke the original owner's session.
            if a.client:await a.client.disconnect()
            a.client=None;a.me=None;a.status='not_connected'
            a.error=safe_error(exc);a.pause()
            for suffix in ('','-journal','-wal','-shm'):Path(str(a.session_path)+suffix).unlink(missing_ok=True)
            with self.store.db:self.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',(a.error,aid))
            return {'account':self.account_row(aid),'connection':'not_connected','needs_auth':True,'error':a.error}
        finally:
            if client.is_connected():await client.disconnect()
    async def rights(self,a,entity,kind,note='',*,check_membership=True):
        if not isinstance(entity,(types.Channel,types.Chat)) or getattr(entity,'left',False) or getattr(entity,'deactivated',False):raise HTTPException(400,'Выберите доступную группу или канал.')
        rights=getattr(entity,'admin_rights',None);creator=bool(getattr(entity,'creator',False));managed=creator or bool(rights)
        if kind in ('invite','link','invite_message'):
            if not (creator or (rights and rights.invite_users)):raise HTTPException(403,'Для приглашений нужны права добавления участников в управляемой группе.')
        elif getattr(entity,'broadcast',False):
            if not (creator or (rights and rights.post_messages)):raise HTTPException(403,'Нет права публикации в канале.')
        elif not managed:
            if len(note.strip())<8:raise HTTPException(400,'Для внешней группы укажите основание разрешения на публикацию.')
            if check_membership:
                try:await a.client.get_permissions(entity,'me')
                except errors.UserNotParticipantError:raise HTTPException(403,'Аккаунт не участник группы. Автоматического вступления нет.') from None
            banned=[getattr(entity,'banned_rights',None),getattr(entity,'default_banned_rights',None)]
            if any(r and r.send_messages for r in banned):raise HTTPException(403,'В группе запрещена отправка сообщений.')
        return {'id':utils.get_peer_id(entity),'title':entity.title,'username':getattr(entity,'username',None),'managed':managed,'can_invite':creator or bool(rights and rights.invite_users),'forum':bool(getattr(entity,'forum',False)),'kind':'Канал' if getattr(entity,'broadcast',False) else 'Группа'}
    async def read_membership(self,client,entity,user):
        try:permissions=await client.get_permissions(entity,user)
        except errors.UserNotParticipantError:
            return {'member':False,'participant_type':None,'error_type':'UserNotParticipantError','checked_at':now()}
        participant=getattr(permissions,'participant',None)
        peer=getattr(participant,'peer',None)
        participant_id=getattr(participant,'user_id',getattr(peer,'user_id',None))
        member=participant_membership(permissions) if participant_id==user.id else None
        return {'member':member,
                'participant_type':type(participant).__name__ if participant is not None else None,'checked_at':now()}
    async def resolve_invite_user(self,client,item):
        """Retain the account-scoped peer; legacy usernames may only recover the same ID."""
        uid=item['user_id'];access_hash=item.get('user_access_hash')
        if access_hash is not None:
            user=await client.get_entity(types.InputPeerUser(uid,access_hash))
        else:
            try:user=await client.get_entity(uid)
            except ValueError:
                ref=(item.get('user_name') or '').strip().lstrip('@').lower()
                if not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',ref):raise
                user=await client.get_entity(ref)
        if not isinstance(user,types.User) or user.id!=uid or user.bot or user.deleted:
            raise HTTPException(409,'Исходный получатель больше недоступен или username принадлежит другому человеку; приглашение не отправлено.')
        return user
    async def membership(self,aid:str,target:int,username:str):
        ref=username.strip().lstrip('@').lower()
        if not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',ref):raise HTTPException(400,'Укажите @username пользователя.')
        result=await self.membership_read(aid,target,ref=ref)
        if result['member'] is None:raise HTTPException(502,'Telegram вернул неизвестный тип участника. Членство не подтверждено.')
        return result

    async def account_diagnostics(self,aid:str):
        client=None;identity=None
        def same_reader(*,check_cooldown=True):
            current=self.readable_account(aid,check_cooldown=check_cooldown)
            if current.client is not client or current.me.id!=identity:
                raise HTTPException(409,'Аккаунт изменился во время диагностики.')
        try:
            async with asyncio.timeout(45):
                a=self.readable_account(aid);client=a.client;identity=a.me.id
                me=await client.get_me();same_reader()
                if not isinstance(me,types.User) or me.bot or me.id!=identity:
                    raise HTTPException(409,'Telegram не подтвердил сохранённую личность аккаунта.')
                result=await client(functions.account.GetAuthorizationsRequest());same_reader()
                records=[r for r in getattr(result,'authorizations',[]) if isinstance(r,types.Authorization) and r.current is True]
                current=records[0] if len(records)==1 else None
                def label(name):
                    value=getattr(current,name,None)
                    return value[:200] if isinstance(value,str) else None
                def flag(name):
                    value=getattr(current,name,None)
                    return value if type(value) is bool else None
                api_id=getattr(current,'api_id',None)
                if type(api_id) is not int or api_id<=0:api_id=None
                configured=getattr(client,'api_id',None)
                return {'account':aid,'expected_sender_id':identity,'sender_id':me.id,'username':me.username,
                        'connected':True,'outbound_paused':not bool(self.account_row(aid)['enabled']),'checked_at':now(),
                        'authorization_status':'confirmed' if current else 'unconfirmed','session_current':True if current else None,
                        'unconfirmed':flag('unconfirmed'),'password_pending':flag('password_pending'),
                        'current_auth_api_id':api_id,'client_config_api_id_match':api_id==configured if api_id is not None and type(configured) is int else None,
                        **{name:label(name) for name in ('app_name','app_version','device_model','platform','system_version')}}
        except errors.FloodWaitError as exc:
            same_reader(check_cooldown=False)
            with self.store.db:self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?',(time.time()+exc.seconds+1,aid))
            raise HTTPException(429,f'Telegram просит подождать {exc.seconds} секунд. Диагностика не завершена; исходящие не включались.') from None
        except (errors.PeerFloodError,*REVOKED) as exc:
            same_reader(check_cooldown=False);self.pause_outbound(aid,exc)
            reason='Telegram больше не подтверждает сессию аккаунта. Подключите аккаунт заново.' if isinstance(exc,REVOKED) else 'Telegram отклонил диагностику (PeerFloodError).'
            raise HTTPException(409,reason+' Исходящие приостановлены; автоматического повтора нет.') from None
        except TimeoutError:raise HTTPException(504,'Telegram долго отвечает на диагностику. Исходящие не включались.') from None

    async def item_membership(self,jid:int,iid:int):
        rows=self.store.rows('SELECT i.*,j.account,j.telegram_id,j.kind FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.id=? AND i.id=?',(jid,iid))
        if not rows:raise HTTPException(404,'Получатель этого задания не найден.')
        item=rows[0]
        if item['kind']!='invite' or not item['user_id']:raise HTTPException(400,'Проверка доступна для сохранённого прямого приглашения.')
        return await self.membership_read(item['account'],item['chat_id'],item=item)

    async def membership_read(self,aid,target,*,ref=None,item=None):
        client=None;sender_id=None
        def same_reader(*,check_cooldown=True):
            current=self.readable_account(aid,check_cooldown=check_cooldown)
            if current.client is not client or current.me.id!=sender_id:
                raise HTTPException(409,'Аккаунт изменился во время проверки.')
            return current
        try:
            async with asyncio.timeout(45):
                a=self.readable_account(aid);client=a.client;sender_id=a.me.id
                if item and item['telegram_id']!=sender_id:raise HTTPException(409,'Отправитель отличается от сохранённого приглашения.')
                entity=await client.get_entity(target)
                same_reader()
                group=await self.rights(a,entity,'invite')
                same_reader()
                if group['id']!=target:raise HTTPException(409,'Группа отличается от сохранённой цели проверки.')
                user=await self.resolve_invite_user(client,item) if item else await client.get_entity(ref)
                same_reader()
                if not isinstance(user,types.User) or user.bot or user.deleted:raise HTTPException(400,'Получатель должен быть действующим личным аккаунтом.')
                checked=await self.read_membership(client,entity,user)
                same_reader()
                history_state=None
                if item:
                    current=self.store.rows('SELECT i.*,j.account,j.telegram_id,j.kind FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.id=? AND i.id=?',(item['job'],item['id']))
                    if not current or any(current[0][key]!=item[key] for key in ('job','account','telegram_id','kind','chat_id','user_id','user_access_hash')):
                        raise HTTPException(409,'Сохранённая привязка приглашения изменилась во время проверки.')
                    history_state=current[0]['state']
                return {'account':aid,'sender':a.me.username,'sender_id':sender_id,'target':group['id'],'username':user.username,'user_id':user.id,
                        **contact_flags(user),**checked,'history_state':history_state,
                        'outbound_paused':not bool(self.account_row(aid)['enabled']),
                        'uncertain':checked['member'] is None,
                        'uncertainty':'Telegram вернул неизвестный тип участника; текущее членство не подтверждено.' if checked['member'] is None else None,
                        'attribution':'current_membership_only','invitation_requested':False,'history_changed':False}
        except errors.FloodWaitError as exc:
            same_reader(check_cooldown=False)
            with self.store.db:self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?',(time.time()+exc.seconds+1,aid))
            raise HTTPException(429,f'Telegram просит подождать {exc.seconds} секунд. Проверка членства не завершена; приглашение не запускалось.') from None
        except (errors.PeerFloodError,*REVOKED) as exc:
            same_reader(check_cooldown=False)
            self.pause_outbound(aid,exc)
            reason='Telegram больше не подтверждает сессию аккаунта. Подключите аккаунт заново.' if isinstance(exc,REVOKED) else 'Telegram отклонил проверку членства (PeerFloodError).'
            raise HTTPException(409,reason+' Исходящие приостановлены. Результат не подтверждён; приглашение не запускалось.') from None
        except TimeoutError:raise HTTPException(504,'Telegram долго проверяет членство. Приглашение не запускалось.') from None
    async def prepare_contact(self,a,user,record,before_add=None,write_guard=None):
        """One optional contact write, followed by a fresh read; never messages/invites."""
        client=a.client;identity=a.me.id
        record.update(status='checking',request_sent=False,phone_privacy_exception_added=False,
                      phone_visibility_notice=PHONE_VISIBILITY_NOTICE,**contact_flags(user))
        def same_sender():
            if self.online(a.aid).client is not client or a.me.id!=identity:
                raise HTTPException(409,'Аккаунт изменился при подготовке контакта.')
        same_sender()
        if record['sender_has_contact'] is True:
            record.update(status='already_contact',checked_at=now());return
        if before_add:await before_add()
        same_sender()
        if write_guard:await write_guard()
        record.update(status='requested',request_sent=True)
        try:
            result=await asyncio.wait_for(client(functions.contacts.AddContactRequest(
                id=user,first_name=user.first_name or user.username or 'Контакт',last_name=user.last_name or '',
                phone='',add_phone_privacy_exception=False)),timeout=15)
            record['response_type']=type(result).__name__
            same_sender()
            fresh=await asyncio.wait_for(client(functions.users.GetUsersRequest([user])),timeout=10)
            same_sender()
            if len(fresh)!=1 or not isinstance(fresh[0],types.User) or fresh[0].id!=user.id:
                record.update(status='not_verified',sender_has_contact=None,mutual_contact=None,checked_at=now());return
            record.update(**contact_flags(fresh[0]),checked_at=now())
            record['status']='added_verified' if record['sender_has_contact'] is True else 'not_verified'
        except Exception as exc:
            record.update(status='rate_limited' if isinstance(exc,errors.FloodWaitError) else 'not_verified',
                          sender_has_contact=None,mutual_contact=None,error_type=type(exc).__name__,checked_at=now())
            if isinstance(exc,errors.RPCError):record['rpc']=safe_rpc_metadata(exc)
            raise
    async def add_contact(self,aid:str,spec:AddContact):
        async with self.lock:
            ref=spec.username.strip().lstrip('@').lower()
            if not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',ref):raise HTTPException(400,'Укажите @username одного пользователя.')
            a=self.online(aid);client=a.client;identity=a.me.id
            if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1",(aid,)):
                raise HTTPException(409,'Дождитесь завершения текущего действия аккаунта.')
            user=await asyncio.wait_for(client.get_entity(ref),timeout=15)
            if not isinstance(user,types.User) or user.bot or user.deleted or user.id==identity:
                raise HTTPException(400,'Выберите действующий личный аккаунт другого пользователя.')
            if self.online(aid).client is not client or a.me.id!=identity:raise HTTPException(409,'Отправитель изменился.')
            record={}
            try:await self.prepare_contact(a,user,record)
            except errors.FloodWaitError as exc:
                until=time.time()+exc.seconds+1
                with self.store.db:
                    self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?),error=? WHERE id=?',(until,f'Telegram просит подождать {exc.seconds} секунд.',aid))
                    self.store.db.execute("UPDATE op_jobs SET state='paused',not_before=MAX(not_before,?),updated=? WHERE account=? AND state IN ('queued','scheduled')",(until,now(),aid))
                raise HTTPException(429,f'Telegram просит подождать {exc.seconds} секунд. Приглашение не запускалось.') from None
            except (errors.PeerFloodError,*REVOKED) as exc:
                a.pause();a.report_error(exc)
                with self.store.db:self.store.db.execute('UPDATE op_accounts SET error=? WHERE id=?',(safe_error(exc),aid))
                raise HTTPException(400,safe_error(exc)) from None
            except HTTPException:raise
            except Exception as exc:record.update(status='not_verified',error_type=type(exc).__name__)
            finally:
                self.store.put('last_contact_preparation_'+aid,{'account':aid,'username':ref,'contact_preparation':record,'checked_at':now()})
            return {'account':aid,'sender':a.me.username,'username':user.username or ref,
                    'sender_has_contact':record.get('sender_has_contact'),'mutual_contact':record.get('mutual_contact'),
                    'invitation_requested':False,'message_sent':False,'phone_privacy_exception_added':False,'contact_preparation':record}
    async def destinations(self,aid:str,refresh:bool=False):
        # Cache only picker metadata, never permission to send. Creation and
        # delivery still resolve the live entity and check rights independently.
        a=self.online(aid);requested=time.monotonic()
        async with self.destination_locks.setdefault(aid,asyncio.Lock()):
            a=self.online(aid);client=a.client;identity=a.me.id
            cached=self.destination_cache.get(aid)
            if cached and cached['client'] is client and cached['identity']==identity:
                age=time.monotonic()-cached['at']
                if age<300 and (not refresh or cached['at']>=requested):
                    return {**copy.deepcopy(cached['data']),'cached':True,'cache_age_seconds':round(age,1)}
            try:result=await self.load_destinations(aid)
            except TimeoutError:
                self.destination_cache.pop(aid,None)
                raise HTTPException(504,'Telegram долго отдаёт список групп. Повторите загрузку через минуту; заполненные поля сохранены.') from None
            except BaseException:
                self.destination_cache.pop(aid,None)
                raise
            current=self.online(aid)
            if current.client is not client or current.me.id!=identity:
                self.destination_cache.pop(aid,None)
                raise HTTPException(409,'Аккаунт изменился во время загрузки. Повторите выбор площадок.')
            self.destination_cache[aid]={'client':client,'identity':identity,'at':time.monotonic(),'data':copy.deepcopy(result)}
            return {**result,'cached':False,'cache_age_seconds':0}
    async def load_destinations(self,aid:str):
        a=self.online(aid);result=[];checked=0;skipped=0
        async with asyncio.timeout(25):
            async for d in a.client.iter_dialogs(limit=300):
                checked+=1
                # Dialog entities already describe membership and posting flags.
                # Detailed live permissions are still checked when creating a
                # job and again by its worker, not for every row in this picker.
                try:result.append(await self.rights(a,d.entity,'post','Проверить разрешение перед отправкой',check_membership=False))
                except (HTTPException,errors.RPCError):skipped+=1
        return {'destinations':result,'limit':300,'checked':checked,'skipped':skipped,'sender':a.me.username or a.me.first_name}
    async def resolve(self,aid:str,spec:Resolve):
        a=self.online(aid);ref=spec.reference.strip().removeprefix('https://t.me/').removeprefix('@')
        if not ref or '/' in ref or '+' in ref:raise HTTPException(400,'Введите публичный @username группы.')
        entity=await asyncio.wait_for(a.client.get_entity(ref),20)
        return await self.rights(a,entity,'post','Разрешение будет подтверждено в задании')
    async def detail(self,jid:int):
        rows=self.store.rows('SELECT * FROM op_jobs WHERE id=?',(jid,))
        if not rows:raise HTTPException(404,'Задание не найдено.')
        j=rows[0];j['spec']=json.loads(j['spec']);j.pop('digest');j.pop('request_key')
        j['items']=self.store.rows('SELECT id,chat_id,chat_title,chat_username,user_id,user_name,state,message_ids,invite_link,error,attempted_at,finished_at,diagnosis FROM op_items WHERE job=? ORDER BY id',(jid,))
        for item in j['items']:
            item['message_ids']=json.loads(item['message_ids'])
            item['diagnosis']=json.loads(item['diagnosis'])
            item['links']=([f'https://t.me/{item["user_name"]}'] if item['user_name'] and item['message_ids'] else []) if j['kind'] in ('invite_message','message') else [f'https://t.me/{item["chat_username"]}/{mid}' if item['chat_username'] else f'https://t.me/c/{utils.resolve_id(item["chat_id"])[0]}/{mid}' for mid in item['message_ids']]
            item['retryable'],item['retry_reason']=self.invite_retry_eligibility(j,item)
            if j['kind']=='post' and j['spec'].get('destination_topics'):
                route=self.topics.saved(j,item)
                if route:item.update(topic_id=route['topic_id'],topic_title=route['title'])
        j['retryable_item_ids']=[item['id'] for item in j['items'] if item['retryable']]
        j['retryable']=bool(j['retryable_item_ids'])
        j['retry_of_job']=j['spec'].get('retry_invite',{}).get('job_id')
        j['retry_jobs']=[row['id'] for row in self.store.rows("SELECT id,spec FROM op_jobs WHERE kind='invite' ORDER BY id") if json.loads(row['spec']).get('retry_invite',{}).get('job_id')==jid]
        j['pilot']=self.pilot_status(j)
        j['single_attempt']=self.single_attempt_status(j)
        return j

    def save_single_attempt(self,record):
        for key in ('single_attempt_job_'+str(record['job_id']),'single_attempt_request_'+record['request_key']):
            self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                  (key,json.dumps(record,ensure_ascii=False)))

    def single_budget(self,j):
        pilot=self.pilot_status(j);count=self.account_daily_count(j['account'],now()[:10])
        normal=self.normal_daily_limit(j)
        limit=pilot['account_daily_ceiling'] if pilot and pilot['active'] else normal
        available=count<limit and not (pilot and (pilot['goal_met'] or pilot['state']=='budget_exhausted'))
        remaining=max(0,limit-count)
        if pilot and pilot['active']:remaining=min(remaining,pilot['remaining_attempts'])
        return {'normal_daily_limit':normal,'effective_daily_limit':limit,'account_count_today':count,
                'remaining_today':remaining,'quota_available':available,'pilot':pilot}

    def single_pilot_binding(self,jid):
        record=self.store.get('invite_pilot_'+str(jid))
        if record is None:return None
        if not isinstance(record,dict):return 'invalid'
        immutable={key:value for key,value in record.items() if key not in ('attempts_used','goal_reached_at')}
        return hashlib.sha256(json.dumps(immutable,sort_keys=True).encode()).hexdigest()

    @staticmethod
    def single_mode(record):
        mode=record.get('mode','mutual_contact_control')
        return mode if mode in ('mutual_contact_control','direct') else None

    def single_binding_valid(self,j,record):
        try:
            rows,fingerprint=self.pilot_cohort(j)
            mode=self.single_mode(record);requires_mutual=mode=='mutual_contact_control'
            policy_valid=mode is not None and record.get('requires_mutual_contact') is requires_mutual
            if 'mode' in record:
                saved=self.store.get('single_attempt_request_'+record['request_key'])
                policy_valid=policy_valid and isinstance(saved,dict) and self.single_mode(saved)==mode and saved.get('requires_mutual_contact') is requires_mutual and saved.get('digest')==record.get('digest')
            return (record.get('version')==1 and record.get('max_attempts')==1 and policy_valid and
                    record.get('job_id')==j['id'] and record.get('account')==j['account'] and
                    record.get('telegram_id')==j['telegram_id']==self.account_row(j['account'])['telegram_id'] and
                    record.get('cohort_hash')==fingerprint and record.get('pilot_binding')==self.single_pilot_binding(j['id']) and any(
                        r['id']==record.get('item_id') and r['chat_id']==record.get('chat_id') and r['user_id']==record.get('user_id') for r in rows))
        except (HTTPException,KeyError,TypeError,ValueError):return False

    def single_attempt_status(self,j,record=None):
        record=self.store.get('single_attempt_job_'+str(j['id'])) if record is None else record
        if not isinstance(record,dict):return None
        result={key:record.get(key) for key in ('job_id','item_id','account','user_id','cohort_hash','request_key','state',
                'requested_at','claimed_at','finished_at','released_at','result_state','invitation_requested','utc_day','expires_at')}
        mode=self.single_mode(record)
        result.update(sender_id=record.get('telegram_id'),target=record.get('chat_id'),mode=mode,requires_mutual_contact=mode=='mutual_contact_control',
                      pause_after_attempt=True,max_attempts=1,can_continue=False,blocked_reason=None)
        if record.get('state')=='released':return result
        reason=None
        latest=self.store.get('single_attempt_job_'+str(j['id']))
        if not isinstance(latest,dict) or latest.get('request_key')!=record.get('request_key'):reason='Для задания уже подготовлена другая одиночная проверка.'
        elif not self.single_binding_valid(j,record):reason='Привязка одиночной проверки изменилась.'
        elif record.get('state')!='finished' or record.get('result_state') not in SINGLE_KNOWN_RESULTS:
            reason='Сначала нужен завершённый известный результат одиночной проверки; неизвестные и ожидающие не продолжаются.'
        elif j['state']!='paused' or not any(i['state']=='pending' for i in j['items']) or any(i['state']=='sending' for i in j['items']):
            reason='Продолжение доступно только для ожидающих участников приостановленного задания.'
        elif j['not_before']>time.time() or not self.single_budget(j)['quota_available']:reason='Дождитесь доступного окна и дневного бюджета.'
        else:
            try:self.online(j['account'])
            except HTTPException as exc:reason=str(exc.detail)
        result.update(can_continue=reason is None,blocked_reason=reason)
        return result

    async def single_preview(self,jid:int,iid:int,mode:Literal['mutual_contact_control','direct']='mutual_contact_control'):
        if mode not in ('mutual_contact_control','direct'):raise HTTPException(400,'Неизвестный режим одиночной проверки.')
        requires_mutual=mode=='mutual_contact_control'
        j=await self.detail(jid)
        item=next((i for i in j['items'] if i['id']==iid),None)
        if j['kind']!='invite' or not item:raise HTTPException(404,'Участник прямого приглашения не найден.')
        fingerprint=self.pilot_cohort(j)[1]
        check=await self.item_membership(jid,iid)
        fresh=await self.detail(jid)
        if self.pilot_cohort(fresh)[1]!=fingerprint:raise HTTPException(409,'Задание изменилось во время предпросмотра.')
        item=next(i for i in fresh['items'] if i['id']==iid);row=self.account_row(j['account']);budget=self.single_budget(fresh)
        blocked=[];gate=fresh['single_attempt'];prep=bool(fresh['spec'].get('add_contacts_before_invite'))
        if fresh['state']!='paused' or item['state']!='pending':blocked.append('Выберите ожидающего участника приостановленного задания.')
        if not row['enabled']:blocked.append('Исходящие аккаунта приостановлены; предпросмотр не включает их.')
        if max(row['not_before'],fresh['not_before'])>time.time():blocked.append('Окно ожидания ещё не завершилось.')
        if not budget['quota_available']:blocked.append('Нет доступного дневного бюджета; одиночная проверка не увеличивает лимиты.')
        if gate and gate['state']!='released' and (gate['state']!='finished' or gate['result_state'] not in SINGLE_KNOWN_RESULTS):
            blocked.append('Предыдущая одиночная проверка ещё не завершена известным результатом.')
        if self.store.get(f'single_attempt_item_{jid}_{iid}') is not None:blocked.append('Этот участник уже использовал одиночную попытку; повтор тем же или новым ключом отключён.')
        if check['member'] is not False:blocked.append('Для нового добавления нужно подтверждённое отсутствие участника в группе.')
        if requires_mutual and check['mutual_contact'] is not True and not prep:blocked.append('Взаимный контакт не подтверждён; подготовка контакта в этом задании выключена.')
        if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1",(j['account'],)):
            blocked.append('Дождитесь завершения текущего действия аккаунта.')
        old=self.store.rows('SELECT state FROM op_invited WHERE chat_id=? AND user_id=?',(item['chat_id'],item['user_id']))
        if old and old[0]['state']!='already_member':blocked.append('Получатель уже обработан для этой группы; повтор одиночной проверкой не разрешён.')
        return {'job_id':jid,'item_id':iid,'account':j['account'],'sender_id':j['telegram_id'],'target':item['chat_id'],
                'user_id':item['user_id'],'user_name':item['user_name'],'cohort_hash':fingerprint,'checked_at':check['checked_at'],
                'job_state':fresh['state'],'item_state':item['state'],'member':check['member'],
                'sender_has_contact':check['sender_has_contact'],'mutual_contact':check['mutual_contact'],
                'outbound_paused':not bool(row['enabled']),'not_before':max(row['not_before'],fresh['not_before']),
                **budget,'can_start':not blocked,'blocked_reasons':blocked,'single_attempt':gate,
                'mode':mode,'requires_mutual_contact':requires_mutual,
                'invite_requires_mutual_contact':requires_mutual,'contact_preparation_planned':prep,
                'receiver_action_pending':requires_mutual and check['mutual_contact'] is not True,'may_stop_without_invite':True,
                'history_changed':False,'invitation_requested':False,'attribution':'preview_only'}

    async def start_single_attempt(self,jid:int,iid:int,spec:SingleAttempt):
        request={'job_id':jid,'item_id':iid,**spec.model_dump()};request['reason']=spec.reason.strip()
        # Preserve canonical digests for existing legacy request keys; direct is explicit and distinct.
        if spec.mode=='mutual_contact_control':request.pop('mode',None)
        if len(request['reason'])<8:raise HTTPException(400,'Опишите основание одиночной проверки.')
        digest=hashlib.sha256(json.dumps(request,sort_keys=True).encode()).hexdigest()
        async with self.lock:
            existing=self.store.get('single_attempt_request_'+spec.request_key)
            if existing is not None:
                if not isinstance(existing,dict) or existing.get('digest')!=digest:raise HTTPException(409,'Ключ уже использован для другого действия.')
                j=await self.detail(jid);return {'job':j,'single_attempt':self.single_attempt_status(j,existing)}
            preview=await self.single_preview(jid,iid,mode=spec.mode)
            if (preview['sender_id'],preview['target'],preview['user_id'],preview['cohort_hash'])!=(spec.telegram_id,spec.chat_id,spec.user_id,spec.cohort_hash):
                raise HTTPException(409,'Отправитель, группа, получатель или состав задания изменились. Обновите предпросмотр.')
            if not preview['can_start']:raise HTTPException(409,' '.join(preview['blocked_reasons']))
            j=await self.detail(jid);a=self.online(j['account'])
            if a.me.id!=spec.telegram_id or self.pilot_cohort(j)[1]!=spec.cohort_hash:raise HTTPException(409,'Привязка проверки изменилась.')
            if j['spec'].get('audience_run_id'):self.audience.bound(self.audience.row(j['spec']['audience_run_id']))
            timestamp=time.time();midnight=(int(timestamp)//86400+1)*86400
            record={'version':1,'state':'armed','job_id':jid,'item_id':iid,'account':j['account'],'telegram_id':spec.telegram_id,
                    'chat_id':spec.chat_id,'user_id':spec.user_id,'cohort_hash':spec.cohort_hash,'request_key':spec.request_key,
                    'digest':digest,'reason':request['reason'],'requested_at':now(),'max_attempts':1,
                    'mode':spec.mode,'requires_mutual_contact':spec.mode=='mutual_contact_control',
                    'pilot_binding':self.single_pilot_binding(jid),
                    'utc_day':datetime.fromtimestamp(timestamp,timezone.utc).date().isoformat(),
                    'expires_at':min(timestamp+120,midnight) if spec.mode=='direct' else midnight,
                    'invitation_requested':False,'claimed_at':None,'finished_at':None,'result_state':None}
            with self.store.db:
                self.save_single_attempt(record)
                self.store.db.execute("UPDATE op_jobs SET state='queued',updated=? WHERE id=?",(now(),jid))
            return {'job':await self.detail(jid),'single_attempt':self.single_attempt_status(j,record)}

    def validate_single_claim(self,j,item):
        proof=item.get('_single_claim')
        if proof is None:return
        rows=self.store.rows('SELECT * FROM op_jobs WHERE id=?',(j['id'],))
        if not rows:raise HTTPException(409,'Задание одиночной проверки больше недоступно.')
        current=rows[0];current['spec']=json.loads(current['spec'])
        record=self.store.get('single_attempt_job_'+str(j['id']))
        if (not isinstance(record,dict) or record.get('state')!='claimed' or record.get('request_key')!=proof['request_key'] or
            self.single_mode(record)!=self.single_mode(proof) or record.get('requires_mutual_contact') is not proof.get('requires_mutual_contact') or record.get('digest')!=proof.get('digest') or
            record.get('item_id')!=item['id'] or current['state'] not in ('queued','scheduled') or not self.single_window_valid(record) or
            not self.single_binding_valid(current,record) or
            self.store.get(f"single_attempt_item_{j['id']}_{item['id']}")!=proof['request_key']):
            raise HTTPException(409,'Одиночный допуск изменился или уже завершён; запрос не отправлен.')
        a=self.online(j['account'])
        if a.client is not item.get('_single_claim_client') or a.me.id!=record['telegram_id']:
            raise HTTPException(409,'Отправитель одиночной проверки изменился.')

    def single_window_valid(self,record):
        timestamp=time.time()
        return (record.get('utc_day')==datetime.fromtimestamp(timestamp,timezone.utc).date().isoformat() and
                isinstance(record.get('expires_at'),(int,float)) and timestamp<record['expires_at'])

    def interrupt_single_attempt(self,j,record):
        with self.store.db:
            record.update(state='interrupted',finished_at=now());self.save_single_attempt(record)
            self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'",(j['id'],))

    def account_daily_count(self,aid,day):
        return self.store.db.execute("SELECT count(*) FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND substr(i.attempted_at,1,10)=?",(aid,day)).fetchone()[0]

    def normal_daily_limit(self,j):
        limits=self.store.rows("SELECT spec FROM op_jobs WHERE account=? AND state NOT IN ('draft','cancelled') AND (substr(created,1,10)=? OR state IN ('queued','scheduled','paused'))",(j['account'],now()[:10]))
        baseline=min([j['spec']['daily_limit']]+[json.loads(r['spec'])['daily_limit'] for r in limits])
        return self.budgets.effective_limit(j,baseline)

    def pilot_cohort(self,j):
        if IsolatedInviteCampaign.belongs(j) or ManualInviteCampaign.belongs(j):
            raise HTTPException(409,'Изолированная кампания запускается только из собственной карточки; общий пилот и одиночный повтор отключены.')
        rows=self.store.rows('SELECT id,chat_id,user_id,user_name,user_access_hash FROM op_items WHERE job=? ORDER BY id',(j['id'],))
        if (j['kind']!='invite' or not 1<=len(rows)<=50 or len({r['chat_id'] for r in rows})!=1 or
            any(not isinstance(r['user_id'],int) or r['user_id']<=0 or r['user_id']==j['telegram_id'] for r in rows) or
            len({r['user_id'] for r in rows})!=len(rows)):
            raise HTTPException(409,'Пилот требует одну группу и до 50 уникальных получателей сохранённого задания добавления участников.')
        body={'job':j['id'],'account':j['account'],'telegram_id':j['telegram_id'],'kind':j['kind'],'spec':j['spec'],'cohort':rows}
        return rows,hashlib.sha256(json.dumps(body,sort_keys=True).encode()).hexdigest()

    def save_pilot(self,jid,record):
        # Caller owns the SQLite transaction; claims and their budget debit commit together.
        self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                              ('invite_pilot_'+str(jid),json.dumps(record)))

    def pilot_status(self,j):
        record=self.store.get('invite_pilot_'+str(j['id']))
        if not isinstance(record,dict):return None
        timestamp=time.time();day=datetime.fromtimestamp(timestamp,timezone.utc).date().isoformat()
        valid=False;verified=0
        try:
            rows,fingerprint=self.pilot_cohort(j)
            sender=self.account_row(j['account'])
            valid=(record.get('version')==1 and record.get('confirmed') is True and record.get('job_id')==j['id'] and
                   record.get('account')==j['account'] and record.get('telegram_id')==j['telegram_id']==sender['telegram_id'] and
                   record.get('chat_id')==rows[0]['chat_id'] and record.get('cohort_hash')==fingerprint and
                   type(record.get('additional_attempts')) is int and 1<=record['additional_attempts']<=50 and
                   type(record.get('attempts_used')) is int and 0<=record['attempts_used']<=record['additional_attempts'] and
                   type(record.get('account_count_at_confirmation')) is int and record['account_count_at_confirmation']>=0 and
                   type(record.get('account_daily_ceiling')) is int and record.get('goal_added')==10 and
                   record['account_daily_ceiling']==record.get('account_count_at_confirmation',-1000)+record['additional_attempts'])
            verified=len({(i['chat_id'],i['user_id']) for i in j['items'] if i['state']=='invited' and
                          i['diagnosis'].get('code')=='invite_member_verified'})
        except (HTTPException,KeyError,TypeError,ValueError):valid=False
        goal_met=bool(record.get('goal_reached_at')) or (valid and verified>=record.get('goal_added',10))
        expired=not isinstance(record.get('expires_at'),(int,float)) or timestamp>=record['expires_at'] or record.get('utc_day')!=day
        count=self.account_daily_count(j['account'],day)
        active=valid and not expired and not goal_met and record['attempts_used']<record['additional_attempts'] and count<record['account_daily_ceiling']
        state='goal_met' if goal_met else 'invalid' if not valid else 'expired' if expired else 'active' if active else 'budget_exhausted'
        reasons={'goal_met':'Цель пилота достигнута: 10 новых добавлений подтверждены. Оставшиеся не запускаются.',
                 'invalid':'Привязка пилота изменилась. Разовый допуск не действует; применяется обычный предел.',
                 'expired':'Разовый допуск истёк. Применяется обычный дневной предел.',
                 'active':'Разовый допуск действует только для этого задания сегодня UTC.',
                 'budget_exhausted':'Разовый бюджет пилота исчерпан. Оставшиеся не запускаются.'}
        return {'confirmed':True,'state':state,'active':active,'reason':reasons[state],'utc_day':record.get('utc_day'),
                'expires_at':record.get('expires_at'),'additional_attempts':record.get('additional_attempts'),
                'attempts_used':record.get('attempts_used'),'account_count_at_confirmation':record.get('account_count_at_confirmation'),
                'account_count_today':count,'account_daily_ceiling':record.get('account_daily_ceiling'),
                'remaining_attempts':max(0,min(record.get('additional_attempts',0)-record.get('attempts_used',0),record.get('account_daily_ceiling',0)-count)) if active else 0,
                'normal_daily_limit':self.normal_daily_limit(j),'goal_added':10,'verified_added':verified,
                'cohort_size':len(j['items']),'goal_met':goal_met,'requested_at':record.get('requested_at')}

    async def pilot_preview(self,jid:int,additional_attempts:int=50):
        if not 1<=additional_attempts<=50:raise HTTPException(400,'Разовый бюджет: от 1 до 50 дополнительных попыток.')
        j=await self.detail(jid);rows,fingerprint=self.pilot_cohort(j)
        day=datetime.fromtimestamp(time.time(),timezone.utc).date().isoformat();count=self.account_daily_count(j['account'],day)
        blocked=[]
        if j['state'] not in ('draft','scheduled','queued','paused'):blocked.append('Задание завершено.')
        if not any(i['state']=='pending' for i in j['items']):blocked.append('Нет ожидающих участников.')
        if any(i['state'] in ('uncertain','sending') for i in j['items']):blocked.append('Есть непроверенный или выполняющийся результат.')
        gate=self.store.get('single_attempt_job_'+str(jid))
        if gate and (not isinstance(gate,dict) or gate.get('state')!='released'):
            blocked.append('Сначала отдельно завершите одиночную проверку; разовый пилот не снимает её ограничение.')
        if self.store.get('invite_pilot_'+str(jid)) is not None:blocked.append('Разовый допуск для этого задания уже выдавался; повторная выдача отключена.')
        if self.account_row(j['account'])['telegram_id']!=j['telegram_id']:blocked.append('Отправитель сохранённого задания изменился.')
        return {'job_id':jid,'account':j['account'],'telegram_id':j['telegram_id'],'chat_id':rows[0]['chat_id'],
                'cohort_size':len(rows),'cohort_hash':fingerprint,'utc_day':day,'account_count_today':count,
                'normal_daily_limit':self.normal_daily_limit(j),'additional_attempts':additional_attempts,
                'proposed_account_daily_ceiling':count+additional_attempts,'goal_added':10,
                'eligible':not blocked,'blocked_reasons':blocked,'pilot':j['pilot'],
                'message':'Разовый бюджет добавляется к уже учтённым действиям. История и обычные лимиты остальных заданий сохраняются.'}

    async def start_pilot(self,jid:int,spec:PilotStart):
        request={'job_id':jid,**spec.model_dump()};request['reason']=spec.reason.strip()
        if len(request['reason'])<8:raise HTTPException(400,'Укажите причину разового пилота.')
        digest=hashlib.sha256(json.dumps(request,sort_keys=True).encode()).hexdigest()
        async with self.lock:
            existing=self.store.get('invite_pilot_request_'+spec.request_key)
            if existing:
                if existing.get('digest')!=digest:raise HTTPException(409,'Ключ уже использован для другого разового пилота.')
                return await self.detail(jid)
            preview=await self.pilot_preview(jid,spec.additional_attempts)
            if not preview['eligible']:raise HTTPException(409,' '.join(preview['blocked_reasons']))
            j=await self.detail(jid);a=self.online(j['account']);client=a.client;identity=a.me.id
            if identity!=j['telegram_id']:raise HTTPException(409,'Отправитель изменился.')
            if self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.account=? AND i.state='sending' LIMIT 1",(j['account'],)):
                raise HTTPException(409,'Дождитесь завершения текущего действия аккаунта.')
            entity=await asyncio.wait_for(client.get_entity(preview['chat_id']),20)
            await self.rights(a,entity,'invite',j['spec']['permission_note'])
            if self.online(j['account']).client is not client or a.me.id!=identity:raise HTTPException(409,'Отправитель изменился при подготовке пилота.')
            if j['spec'].get('audience_run_id'):self.audience.bound(self.audience.row(j['spec']['audience_run_id']))
            if j['spec'].get('retry_invite'):
                for item in j['items']:
                    if item['state']=='pending':self.validate_invite_retry(j,item)
            fresh=await self.detail(jid)
            if self.pilot_cohort(fresh)[1]!=preview['cohort_hash'] or fresh['state']!=j['state']:
                raise HTTPException(409,'Задание или список участников изменились при подготовке пилота.')
            timestamp=time.time();day=datetime.fromtimestamp(timestamp,timezone.utc).date().isoformat()
            if day!=preview['utc_day']:raise HTTPException(409,'Наступил новый день UTC. Обновите предпросмотр пилота.')
            record={'version':1,'confirmed':True,'job_id':jid,'account':j['account'],'telegram_id':identity,
                    'chat_id':preview['chat_id'],'cohort_hash':preview['cohort_hash'],'utc_day':day,
                    'expires_at':(int(timestamp)//86400+1)*86400,'additional_attempts':spec.additional_attempts,
                    'account_count_at_confirmation':preview['account_count_today'],
                    'account_daily_ceiling':preview['proposed_account_daily_ceiling'],'attempts_used':0,
                    'goal_added':10,'requested_at':now(),'reason':request['reason']}
            with self.store.db:
                self.save_pilot(jid,record)
                self.store.db.execute('INSERT INTO settings VALUES(?,?)',('invite_pilot_request_'+spec.request_key,json.dumps({'digest':digest,'job_id':jid})))
                self.store.db.execute("UPDATE op_jobs SET state='queued',not_before=0,updated=? WHERE id=?",(now(),jid))
            return await self.detail(jid)

    def stop_pilot_goal(self,j):
        pilot=self.pilot_status(j)
        if not pilot or not pilot['goal_met']:return False
        record=self.store.get('invite_pilot_'+str(j['id']))
        with self.store.db:
            record.setdefault('goal_reached_at',now());self.save_pilot(j['id'],record)
            self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND state!='cancelled'",(now(),j['id']))
        return True

    def validate_pilot_claim(self,j,item):
        """Recheck the original claim immediately before a Telegram write, without rearming it."""
        proof=item.get('_pilot_claim')
        if not proof:return
        rows=self.store.rows('SELECT * FROM op_jobs WHERE id=?',(j['id'],))
        if not rows:raise HTTPException(409,'Исходное задание пилота больше недоступно.')
        current=rows[0];current['spec']=json.loads(current['spec'])
        current['items']=self.store.rows('SELECT chat_id,user_id,state,diagnosis FROM op_items WHERE job=?',(j['id'],))
        for row in current['items']:row['diagnosis']=json.loads(row['diagnosis'])
        record=self.store.get('invite_pilot_'+str(j['id']));status=self.pilot_status(current)
        keys=('job_id','account','telegram_id','chat_id','cohort_hash','utc_day','expires_at',
              'account_daily_ceiling','additional_attempts','requested_at')
        if (current['state'] not in ('queued','scheduled') or not status or
            status['state'] not in ('active','budget_exhausted') or
            not isinstance(record,dict) or any(record.get(key)!=proof.get(key) for key in keys) or
            record.get('attempts_used',0)<proof['claim_number'] or
            status['account_count_today']>proof['account_daily_ceiling']):
            raise HTTPException(409,'Разовый допуск пилота истёк, исчерпан или его привязка изменилась перед действием. Запрос добавления не отправлен.')
        a=self.online(proof['account'])
        if a.me.id!=proof['telegram_id'] or a.client is not item.get('_pilot_claim_client'):
            raise HTTPException(409,'Отправитель изменился после учёта попытки пилота.')
        if time.time()>=proof['expires_at']:
            raise HTTPException(409,'Разовый допуск пилота истёк непосредственно перед действием. Запрос добавления не отправлен.')

    def active_invite_retry(self,chat_id,user_id,exclude_job=None):
        rows=self.store.rows("SELECT j.id,j.spec FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.kind='invite' AND i.chat_id=? AND i.user_id=? AND i.state IN ('pending','sending') AND j.state IN ('draft','queued','scheduled','paused')",(chat_id,user_id))
        return next((row['id'] for row in rows if row['id']!=exclude_job and json.loads(row['spec']).get('retry_invite')),None)

    def invite_retry_eligibility(self,j,item,exclude_job=None):
        if IsolatedInviteCampaign.belongs(j) or ManualInviteCampaign.belongs(j):
            return False,'Изолированная кампания не создаёт автоматических или общих повторов.'
        if j['kind']!='invite' or j['state'] not in ('failed','partial','paused') or item['state']!='restricted':
            return False,'Повтор предусмотрен только после подтверждённого отказа прямого приглашения.'
        if self.store.rows("SELECT 1 FROM op_items WHERE job=? AND state='sending' LIMIT 1",(j['id'],)):
            return False,'Дождитесь завершения текущего действия исходного задания.'
        diagnosis=item.get('diagnosis') or {}
        if isinstance(diagnosis,str):diagnosis=json.loads(diagnosis)
        if diagnosis and not diagnosis.get('retry_after_change'):
            return False,'Это ограничение не допускает повтор после изменения приватности.'
        payload=j['spec'] if isinstance(j['spec'],dict) else json.loads(j['spec'])
        if payload.get('audience_run_id'):
            try:
                run=self.audience.row(payload['audience_run_id']);self.audience.bound(run)
            except HTTPException as exc:return False,str(exc.detail)
            stored=self.store.rows('SELECT user_access_hash FROM op_items WHERE id=?',(item['id'],))
            candidate=self.store.rows('SELECT id,access_hash FROM audience_candidates WHERE run_id=? AND user_id=?',(run['id'],item['user_id']))
            if (run['account']!=j['account'] or run['telegram_id']!=j['telegram_id'] or run['target']!=item['chat_id'] or
                not stored or len(candidate)!=1 or candidate[0]['id'] not in payload.get('candidate_ids',[]) or candidate[0]['access_hash']!=stored[0]['user_access_hash']):
                return False,'Сохранённые участники или привязка подбора изменились; повтор отключён.'
        marker=self.store.rows('SELECT state,job FROM op_invited WHERE chat_id=? AND user_id=?',(item['chat_id'],item['user_id']))
        if not marker or marker[0]['state']!='restricted' or marker[0]['job']!=j['id']:
            return False,'Есть более новый или непроверенный результат для этой пары. Повтор отключён.'
        if self.active_invite_retry(item['chat_id'],item['user_id'],exclude_job):
            return False,'Подтверждённый повтор уже подготовлен. Откройте его черновик в журнале.'
        return True,('Точная причина старого отказа не была сохранена. Повтор допустим только после изменения разрешений и отдельного подтверждения.' if not diagnosis else 'Повтор допустим после изменения указанного ограничения и отдельного подтверждения.')

    def validate_invite_retry(self,j,item):
        proof=j['spec'].get('retry_invite')
        if not isinstance(proof,dict):raise HTTPException(409,'Не найдена сохранённая связь с исходным приглашением.')
        source=self.store.rows('SELECT * FROM op_jobs WHERE id=?',(proof.get('job_id'),))
        if not source:raise HTTPException(409,'Исходное задание не найдено; повтор отключён.')
        original=source[0];original['spec']=json.loads(original['spec'])
        if original['account']!=j['account'] or original['telegram_id']!=j['telegram_id'] or original['kind']!='invite':
            raise HTTPException(409,'Отправитель или вид исходного задания изменился; повтор отключён.')
        originals=self.store.rows('SELECT * FROM op_items WHERE job=? AND chat_id=? AND user_id=?',(original['id'],item['chat_id'],item['user_id']))
        chosen=[row for row in originals if row['id'] in proof.get('item_ids',[])]
        if len(chosen)!=1:raise HTTPException(409,'Получатель или группа не совпадают с исходной попыткой.')
        allowed,reason=self.invite_retry_eligibility(original,chosen[0],exclude_job=j['id'])
        if not allowed:raise HTTPException(409,reason)
        return chosen[0]

    async def retry_invite(self,jid:int,spec:RetryInvite):
        try:
            async with asyncio.timeout(45):return await self.prepare_invite_retry(jid,spec)
        except TimeoutError:raise HTTPException(504,'Telegram долго проверяет повтор. Черновик не создан; отправка не запускалась.') from None

    async def prepare_invite_retry(self,jid,spec):
        reason=spec.reason.strip()
        if len(reason)<8 or len(set(spec.item_ids))!=len(spec.item_ids):raise HTTPException(400,'Опишите изменение разрешений и выберите участников без дублей.')
        request={'job_id':jid,'reason':reason,'item_ids':sorted(spec.item_ids)}
        digest=hashlib.sha256(json.dumps(request,sort_keys=True).encode()).hexdigest()
        async with self.lock,self.audience.lock:
            existing=self.store.rows('SELECT id,digest,spec FROM op_jobs WHERE request_key=?',(spec.request_key,))
            if existing:
                if existing[0]['digest']!=digest or json.loads(existing[0]['spec']).get('retry_invite',{}).get('job_id')!=jid:
                    raise HTTPException(409,'Этот ключ уже принадлежит другому заданию.')
                return await self.detail(existing[0]['id'])
            original=await self.detail(jid)
            if IsolatedInviteCampaign.belongs(original) or ManualInviteCampaign.belongs(original):
                raise HTTPException(409,'Повтор изолированной кампании через общий журнал отключён.')
            selected=spec.item_ids or original['retryable_item_ids']
            if not selected or not set(selected)<=set(original['retryable_item_ids']):raise HTTPException(409,'Выберите только доступные restricted-результаты исходного задания. Успехи и непроверенные операции не повторяются.')
            a=self.online(original['account']);client=a.client;identity=a.me.id
            if identity!=original['telegram_id']:raise HTTPException(409,'Отправитель отличается от исходного приглашения.')
            rows=self.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(jid,))
            rows=[row for row in rows if row['id'] in selected]
            if len({(row['chat_id'],row['user_id']) for row in rows})!=len(rows):raise HTTPException(409,'В исходном результате есть дубли; повтор отключён.')
            payload=copy.deepcopy(original['spec'])
            payload.update(request_key=spec.request_key,title=('Повтор: '+original['title'])[:100],scheduled_at=None)
            payload['users']=[row['user_name'] for row in rows if row['user_name']]
            payload['destinations']=list(dict.fromkeys(row['chat_id'] for row in rows))
            payload['retry_invite']={**request,'item_ids':sorted(selected),'requested_at':now()}
            if len(payload.get('consent_note','').strip())<8:raise HTTPException(409,'Не сохранено согласие участников; повтор отключён.')
            if payload.get('audience_run_id'):
                run=self.audience.row(payload['audience_run_id']);self.audience.bound(run)
                if run['account']!=original['account'] or run['target']!=payload['destinations'][0]:raise HTTPException(409,'Подбор относится к другому аккаунту или группе.')
                candidates=self.store.rows('SELECT id,user_id,access_hash FROM audience_candidates WHERE run_id=?',(run['id'],))
                chosen=[p for p in candidates if p['id'] in payload['candidate_ids'] and any(p['user_id']==r['user_id'] and p['access_hash']==r['user_access_hash'] for r in rows)]
                if len(chosen)!=len(rows):raise HTTPException(409,'Сохранённые участники подбора изменились; повтор отключён.')
                payload['candidate_ids']=[p['id'] for p in chosen];payload['users']=[]
            for row in rows:
                entity=await asyncio.wait_for(client.get_entity(row['chat_id']),20)
                dest=await self.rights(a,entity,'invite',payload['permission_note'])
                if dest['id']!=row['chat_id']:raise HTTPException(409,'Выбранная группа изменилась.')
                user=await asyncio.wait_for(self.resolve_invite_user(client,row),20)
                if not isinstance(user,types.User) or user.id!=row['user_id'] or user.bot or user.deleted or user.id==identity:raise HTTPException(409,'Исходный получатель больше недоступен; повтор отключён.')
            if self.online(original['account']).client is not client or a.me.id!=identity:raise HTTPException(409,'Отправитель изменился при подготовке повтора.')
            if payload.get('audience_run_id'):self.audience.bound(self.audience.row(payload['audience_run_id']))
            for row in rows:
                allowed,message=self.invite_retry_eligibility(original,row)
                if not allowed:raise HTTPException(409,message)
            with self.store.db:
                cur=self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',(spec.request_key,digest,original['account'],identity,'invite',payload['title'],json.dumps(payload,ensure_ascii=False),now(),now()))
                retry_id=cur.lastrowid
                for row in rows:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,random_ids,user_access_hash) VALUES(?,?,?,?,?,?,?,?)',(retry_id,row['chat_id'],row['chat_title'],row['chat_username'],row['user_id'],row['user_name'],json.dumps([secrets.randbits(63)]),row['user_access_hash']))
            return await self.detail(retry_id)
    async def create(self,spec:JobSpec):
        try:
            async with asyncio.timeout(45):return await self.prepare_job(spec)
        except TimeoutError:raise HTTPException(504,'Telegram долго проверяет получателей. Черновик не создан; сократите список и повторите. Отправка не запускалась.') from None
    async def prepare_job(self,spec:JobSpec,*,prepare_only=False):
        async with self.lock, self.audience.lock:
            payload=spec.model_dump(mode='json')
            if not spec.destination_topics:payload.pop('destination_topics',None)
            if not spec.add_contacts_before_invite:payload.pop('add_contacts_before_invite',None)
            if spec.campaign_id is None:
                payload.pop('campaign_id',None);payload.pop('campaign_revision',None)
            if spec.posting_policy is None:payload.pop('posting_policy',None)
            digest=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
            existing=self.store.rows('SELECT id,digest FROM op_jobs WHERE request_key=?',(spec.request_key,))
            if existing:
                if prepare_only:raise HTTPException(409,'Ключ проверки уже используется заданием.')
                if existing[0]['digest']!=digest:raise HTTPException(409,'Этот ключ уже принадлежит другому заданию.')
                return await self.detail(existing[0]['id'])
            if spec.campaign_id is not None:
                payload['campaign_snapshot']=self.campaigns.snapshot(spec.campaign_id,spec.campaign_revision)
            a=self.online(spec.account)
            original_client=a.client;original_id=a.me.id
            if not spec.title.strip():raise HTTPException(400,'Укажите название.')
            if spec.scheduled_at and (spec.scheduled_at.tzinfo is None or not time.time()+5<spec.scheduled_at.timestamp()<time.time()+365*86400):raise HTTPException(400,'Укажите будущее время с часовым поясом, в пределах года.')
            if spec.posting_policy is not None and spec.scheduled_at is None:
                remaining=spec.posting_policy.window_expires_at.timestamp()-time.time()
                if not 0<remaining<=7200:raise HTTPException(400,'Текущее окно публикации должно завершаться в течение двух часов.')
            if spec.kind!='message' and not spec.destinations:raise HTTPException(400,'Выберите площадку задания.')
            if spec.kind=='message':
                if spec.destinations or spec.audience_run_id or spec.candidate_ids or spec.invite_url:raise HTTPException(400,'Для личной рассылки укажите только список @username, без группы или ссылки-приглашения.')
                if not spec.users or len(spec.consent_note.strip())<8:raise HTTPException(400,'Укажите получателей и основание согласия на личные сообщения.')
                if spec.media or not spec.text.strip():raise HTTPException(400,'Введите текст личной рассылки без фото.')
                if len(spec.text.encode('utf-16-le'))//2>4096:raise HTTPException(400,'Текст личного сообщения должен быть короче 4096 символов.')
            elif spec.kind=='post':
                if not spec.text.strip() and not spec.media:raise HTTPException(400,'Добавьте текст или фото.')
                if len(spec.text.encode('utf-16-le'))//2>(1024 if spec.media else 4096):raise HTTPException(400,'Слишком длинный текст: до 4096, подпись к фото до 1024 символов.')
                if len(set(spec.media))!=len(spec.media):raise HTTPException(400,'Повторяющиеся фото.')
                for mid in spec.media:self.media_store.media_row(mid)
            elif spec.kind=='invite_message':
                if spec.media or not spec.text.strip():raise HTTPException(400,'Введите текст личного приглашения без фото.')
                if not re.fullmatch(r'https://t\.me/(?:[A-Za-z][A-Za-z0-9_]{3,31}|\+[A-Za-z0-9_-]+)',spec.invite_url):raise HTTPException(400,'Укажите ссылку t.me на выбранную группу или канал.')
                if len((spec.text+'\n\n'+spec.invite_url).encode('utf-16-le'))//2>4096:raise HTTPException(400,'Текст вместе со ссылкой должен быть короче 4096 символов.')
            elif spec.media or spec.text:raise HTTPException(400,'В приглашениях и ссылках не используется текст публикации или фото.')
            if spec.kind in ('invite','invite_message') and (len(spec.destinations)!=1 or not (spec.users or spec.candidate_ids) or len(spec.consent_note.strip())<8):raise HTTPException(400,'Укажите одну группу, согласованных пользователей и основание их согласия.')
            if spec.kind not in ('invite','invite_message','message') and (spec.users or spec.candidate_ids or spec.audience_run_id):raise HTTPException(400,'Список пользователей нужен только для приглашений или личной рассылки.')
            if spec.kind=='invite_message' and (spec.audience_run_id or spec.candidate_ids):raise HTTPException(400,'Для личных приглашений укажите согласованный список @username.')
            if bool(spec.audience_run_id)!=bool(spec.candidate_ids) or (spec.users and spec.candidate_ids):raise HTTPException(400,'Выберите участников из подбора либо укажите ручной список.')
            users=[]
            if spec.audience_run_id:
                users=await self.audience.resolve_selected(spec.audience_run_id,spec.candidate_ids,spec.account,spec.destinations[0])
            elif spec.kind in ('invite','invite_message','message'):
                for ref in dict.fromkeys(u.strip().lstrip('@').lower() for u in spec.users):
                    if not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',ref):raise HTTPException(400,'Укажите @username пользователя, не телефон или числовой ID.')
                    user=await asyncio.wait_for(a.client.get_entity(ref),20)
                    if not isinstance(user,types.User) or user.bot or user.deleted or user.id==a.me.id:raise HTTPException(400,'Получатель должен быть действующим личным аккаунтом.')
                    users.append(user)
            resolved=[]
            if spec.kind=='message':
                seen=set()
                for user in users:
                    if user.id in seen:continue
                    seen.add(user.id)
                    resolved.append(({'id':user.id,'title':user.first_name or user.username or str(user.id),'username':user.username},user))
            for cid in dict.fromkeys(spec.destinations):
                entity=await asyncio.wait_for(a.client.get_entity(cid),20)
                d=await self.rights(a,entity,spec.kind,spec.permission_note)
                route=next((r for r in spec.destination_topics if r.chat_id==cid),None)
                if spec.kind=='post' and (route or getattr(entity,'forum',False)):
                    if route is None:raise HTTPException(400,'Для форума явно выберите тему. Публикация в General не подставляется.')
                    snapshot=await self.topics.check(a,entity,cid,route.topic_id,media=bool(spec.media))
                    payload.setdefault('topic_snapshots',[]).append(snapshot)
                if spec.kind=='invite_message':await self.check_invite_url(a,entity,spec.invite_url)
                for user in users or [None]:resolved.append((d,user))
            if self.online(spec.account).client is not original_client or a.me.id!=original_id:raise HTTPException(409,'Отправитель изменился при подготовке.')
            if spec.audience_run_id:self.audience.bound(self.audience.row(spec.audience_run_id))
            if prepare_only:return dict(payload=payload,digest=digest,resolved=resolved,client=original_client,telegram_id=original_id)
            with self.store.db:
                cur=self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,not_before,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',(spec.request_key,digest,spec.account,a.me.id,spec.kind,spec.title.strip(),json.dumps(payload,ensure_ascii=False),spec.scheduled_at.timestamp() if spec.scheduled_at else 0,now(),now()));jid=cur.lastrowid
                for d,u in resolved:
                    self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,random_ids,user_access_hash) VALUES(?,?,?,?,?,?,?,?)',(jid,d['id'],d['title'],d['username'],u.id if u else None,u.username if u else None,json.dumps([secrets.randbits(63) for _ in range(max(1,len(spec.media)))]),u.access_hash if u else None))
                if spec.audience_run_id:self.store.db.execute('UPDATE audience_runs SET job_id=?,updated=? WHERE id=?',(jid,now(),spec.audience_run_id))
            return await self.detail(jid)
    async def check_invite_url(self,a,entity,url):
        slug=url.removeprefix('https://t.me/')
        if not slug.startswith('+'):
            if slug.casefold()!=(getattr(entity,'username',None) or '').casefold():
                raise HTTPException(400,'Ссылка должна вести в выбранную группу или канал.')
        else:
            checked=await asyncio.wait_for(a.client(functions.messages.CheckChatInviteRequest(slug[1:])),20)
            if not isinstance(checked,types.ChatInviteAlready) or utils.get_peer_id(checked.chat)!=utils.get_peer_id(entity):
                raise HTTPException(400,'Не удалось подтвердить, что ссылка относится к выбранной площадке. Создайте ссылку в её настройках или в панели.')
    async def control(self,jid:int,spec:Control):
        async with self.lock:
            j=await self.detail(jid)
            if self.manual_invite_campaign.belongs(j):
                self.manual_invite_campaign.control_job(j,spec)
                return await self.detail(jid)
            if self.isolated_invite_campaign.belongs(j):
                self.isolated_invite_campaign.control_job(j,spec)
                return await self.detail(jid)
            if self.isolated_contact_batch.belongs(j):
                self.isolated_contact_batch.control_job(j,spec)
                return await self.detail(jid)
            release_cap_hold=False
            gate=self.store.get('single_attempt_job_'+str(jid))
            if spec.action in ('start','resume','continue_pending') and isinstance(gate,dict) and gate.get('state')!='released':
                raise HTTPException(409,'Одиночная проверка требует отдельного подтверждения «Продолжить остальных» после известного результата.')
            if spec.action in ('start','resume','continue_pending','continue_after_single') and j.get('pilot') and j['pilot']['goal_met']:
                raise HTTPException(409,'Цель разового пилота достигнута. Оставшиеся участники не запускаются повторно.')
            if spec.action=='continue_after_single':
                if not spec.confirmed or not isinstance(gate,dict) or spec.ack_single_request_key!=gate.get('request_key'):
                    raise HTTPException(409,'Отдельно подтвердите результат последней одиночной проверки.')
                status=self.single_attempt_status(j,gate)
                if not status or not status['can_continue']:raise HTTPException(409,status.get('blocked_reason') if status else 'Одиночная проверка не найдена.')
                a=self.online(j['account'])
                if a.me.id!=j['telegram_id']:raise HTTPException(409,'Отправитель изменился.')
                run_key=self.invite_runs.for_job(jid)
                if run_key:self.invite_runs.validate_continue(run_key,jid,client=a.client)
                if j['spec'].get('retry_invite'):
                    for item in j['items']:
                        if item['state']=='pending':self.validate_invite_retry(j,item)
                state='queued'
            elif spec.action=='continue_pending':
                if not spec.confirmed:raise HTTPException(400,'Подтвердите продолжение только ожидающих участников.')
                if j['kind']!='invite' or j['state']!='paused':raise HTTPException(409,'Продолжить ожидающих можно только в приостановленном задании добавления участников.')
                if any(i['state']=='sending' for i in j['items']):raise HTTPException(409,'Дождитесь завершения текущего действия.')
                pending=[i for i in j['items'] if i['state']=='pending']
                if not pending:raise HTTPException(409,'В задании нет ожидающих участников.')
                a=self.online(j['account'])
                if a.me.id!=j['telegram_id']:raise HTTPException(409,'Отправитель изменился.')
                run_key=self.invite_runs.for_job(jid)
                if run_key:
                    self.invite_runs.validate_continue(run_key,jid,client=a.client)
                    midnight=(int(time.time())//86400+1)*86400
                    # Only the panel's positively identified daily hold is released.
                    # Account cooldowns and every unrelated schedule remain intact.
                    if j['not_before']==midnight and pending[0].get('error')=='Дневной предел достигнут. Продолжение со следующего дня UTC.':
                        release_cap_hold=True
                        j['not_before']=0
                if j['spec'].get('retry_invite'):
                    for item in pending:self.validate_invite_retry(j,item)
                state='scheduled' if j['not_before']>time.time() else 'queued'
            elif spec.action in ('start','resume'):
                if not spec.confirmed:raise HTTPException(400,'Проверьте предпросмотр и подтвердите действие.')
                if j['state'] not in ('draft','paused'):raise HTTPException(409,'Задание уже запущено или завершено.')
                a=self.online(j['account'])
                if a.me.id!=j['telegram_id']:raise HTTPException(409,'Отправитель изменился.')
                run_key=self.invite_runs.for_job(jid)
                if run_key:self.invite_runs.validate_continue(run_key,jid,client=a.client)
                if any(i['state'] in ('uncertain','sending') for i in j['items']):raise HTTPException(409,'Есть непроверенный результат. Не повторяйте операцию до проверки Telegram.')
                if j['spec'].get('retry_invite'):
                    for item in j['items']:
                        if item['state']=='pending':self.validate_invite_retry(j,item)
                state='scheduled' if j['not_before']>time.time() else 'queued'
            elif spec.action=='pause':
                if j['state'] not in ('queued','scheduled'):raise HTTPException(409,'Можно приостановить активное задание.')
                state='paused'
            else:
                if j['state'] in ('completed','partial','cancelled','failed'):raise HTTPException(409,'Задание завершено.')
                state='cancelled'
            with self.store.db:
                if release_cap_hold:self.store.db.execute('UPDATE op_jobs SET not_before=0 WHERE id=?',(jid,))
                if spec.action=='continue_after_single':
                    gate.update(state='released',released_at=now());self.save_single_attempt(gate)
                self.store.db.execute('UPDATE op_jobs SET state=?,updated=? WHERE id=?',(state,now(),jid))
                if state=='cancelled':self.store.db.execute("UPDATE op_items SET state='cancelled' WHERE job=? AND state='pending'",(jid,))
            return await self.detail(jid)
    async def remove_messages(self,jid:int):
        async with self.lock:
            j=await self.detail(jid);a=self.online(j['account'])
            if j['kind']!='post' or j['state'] in ('queued','scheduled') or any(i['state']=='sending' for i in j['items']):raise HTTPException(409,'Сначала завершите или отмените отправку.')
            if a.me.id!=j['telegram_id']:raise HTTPException(409,'Отправитель изменился.')
            for item in j['items']:
                if item['state']!='sent':continue
                entity=await a.client.get_entity(item['chat_id'])
                await self.rights(a,entity,'post',j['spec']['permission_note'])
                await a.client.delete_messages(entity,item['message_ids'],revoke=True)
                remaining=await a.client.get_messages(entity,ids=item['message_ids'])
                if any(m and not isinstance(m,types.MessageEmpty) for m in remaining):raise HTTPException(409,'Telegram ещё не подтвердил удаление. Проверьте публикацию.')
                with self.store.db:self.store.db.execute("UPDATE op_items SET state='removed',finished_at=? WHERE id=?",(now(),item['id']))
                self.tracking.sync_jobs(jid)
            return await self.detail(jid)
    async def report(self,jid:int):
        j=await self.detail(jid);s=io.StringIO();out=csv.writer(s,delimiter=';')
        has_topics=bool(j['spec'].get('destination_topics'))
        out.writerow(['Задание','Аккаунт','Площадка','Пользователь','Статус','Ошибка','Ссылки','Время']+(['Тема ID','Тема'] if has_topics else []))
        for i in j['items']:
            values=[j['title'],j['account'],i['chat_title'],i['user_name'] or '',i['state'],i['error'] or '',i['invite_link'] or ' '.join(i['links']),i['finished_at'] or '']
            if has_topics:values.extend([i.get('topic_id',''),i.get('topic_title','')])
            out.writerow(["'"+str(v) if str(v).startswith(('=','+','-','@','\t','\r')) else v for v in values])
        return Response('\ufeff'+s.getvalue(),media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="chatcleaner-job-{jid}.csv"'})
    def finish(self,jid):
        states=[r['state'] for r in self.store.rows('SELECT state FROM op_items WHERE job=?',(jid,))]
        if any(s in ('pending','sending') for s in states):return
        good={'sent','invited','already_member','link_created','duplicate_skipped'}
        state='completed' if all(s in good for s in states) else 'partial' if any(s in good for s in states) else 'failed'
        if states and all(s=='cancelled' for s in states):
            row=self.store.rows('SELECT spec FROM op_jobs WHERE id=?',(jid,))
            if row and json.loads(row[0]['spec']).get('posting_policy') is not None:state='cancelled'
        with self.store.db:self.store.db.execute("UPDATE op_jobs SET state=?,updated=? WHERE id=? AND state NOT IN ('cancelled','paused')",(state,now(),jid))
    def expire_posting_windows(self):
        # Expiry is independent of account/job cooldown: stale windows never
        # accumulate behind a budget hold or wake after the event has ended.
        for row in self.store.rows("SELECT id,spec FROM op_jobs j WHERE kind='post' AND state IN ('queued','scheduled','paused') AND EXISTS (SELECT 1 FROM op_items i WHERE i.job=j.id AND i.state='pending')"):
            job={'spec':json.loads(row['spec'])}
            result=expiry_decision(job)
            if not result or result['diagnosis']['code'] not in ('window_expired','campaign_expired'):continue
            with self.store.db:
                self.store.db.execute("UPDATE op_items SET state=?,error=?,diagnosis=?,finished_at=? WHERE job=? AND state='pending'",
                    (result['state'],result['error'],json.dumps(result['diagnosis'],ensure_ascii=False),now(),row['id']))
                states=[i['state'] for i in self.store.rows('SELECT state FROM op_items WHERE job=?',(row['id'],))]
                if states and all(state=='cancelled' for state in states):
                    self.store.db.execute("UPDATE op_jobs SET state='cancelled',updated=? WHERE id=?",(now(),row['id']))
            self.finish(row['id'])
    def posting_before_write(self,a,j,item,client):
        # Publisher calls this synchronously after peer resolution and directly
        # before the write, so a deletion/pause received during an await wins.
        fresh=self.store.rows('SELECT state,account,telegram_id FROM op_jobs WHERE id=?',(j['id'],))
        if not fresh or fresh[0]['account']!=j['account'] or fresh[0]['telegram_id']!=j['telegram_id'] or getattr(a.me,'id',None)!=j['telegram_id'] or a.client is not client:
            raise PostingPolicyStopped(posting_decision('sender_changed'))
        if fresh[0]['state'] not in ('queued','scheduled') or not self.account_row(a.aid)['enabled']:
            raise PostingPolicyStopped(posting_decision('operation_stopped'))
        self.online(j['account'])
        result=publication_decision(self.store,j,item)
        if result:raise PostingPolicyStopped(result)

    async def invite_run_before_write(self,j,item,client,phase='invite'):
        key=self.invite_runs.for_job(j['id'])
        if not key:return
        async with self.lock:
            try:
                with self.store.db:
                    claim=item.get('_run_claim')
                    if not claim or claim['request_key']!=key:
                        raise HTTPException(409,'Разовая попытка конечного допуска не заявлена.')
                    self.invite_runs.before_write(key,item['id'],client=client,phase=phase)
            except HTTPException:
                with self.store.db:self.invite_runs.stop(key,'write_rejected')
                raise
    async def execute(self,a,j,item):
        if IsolatedInviteCampaign.belongs(j) or ManualInviteCampaign.belongs(j):
            raise HTTPException(409,'Общий исполнитель не запускает изолированную кампанию.')
        client=a.client
        self.workbench.guard(a,j,item)
        spec=j['spec'];entity=await a.client.get_entity(item['chat_id'])
        if j['kind']=='message':
            if not isinstance(entity,types.User) or entity.id!=item['user_id'] or entity.bot or entity.deleted or entity.id==a.me.id:raise HTTPException(400,'Получатель больше недоступен.')
        else:await self.rights(a,entity,j['kind'],spec['permission_note'])
        user=None
        if j['kind']=='invite_message':
            old=self.store.rows("SELECT 1 FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.kind='invite_message' AND i.chat_id=? AND i.user_id=? AND i.id!=? AND (i.state IN ('sent','uncertain','sending','blocked','restricted') OR i.attempted_at IS NOT NULL) LIMIT 1",(item['chat_id'],item['user_id'],item['id']))
            if old:return {'state':'duplicate_skipped','error':'Личное приглашение этому человеку уже обрабатывалось. Повтор не отправлен.'}
            user=await a.client.get_entity(item['user_id'])
            if not isinstance(user,types.User) or user.id!=item['user_id'] or user.bot or user.deleted:raise HTTPException(400,'Получатель больше недоступен.')
            await self.check_invite_url(a,entity,spec['invite_url'])
        if j['kind']=='invite':
            if spec.get('retry_invite'):
                source_item=self.validate_invite_retry(j,item)
                item['_retry_source_job']=source_item['job']
            else:
                old=self.store.rows('SELECT state FROM op_invited WHERE chat_id=? AND user_id=?',(item['chat_id'],item['user_id']))
                if old and old[0]['state']!='already_member':return {'state':'duplicate_skipped','error':'Этот пользователь уже обработан для группы. Автоматического повтора с другого аккаунта нет.'}
            if spec.get('audience_run_id'):self.audience.bound(self.audience.row(spec['audience_run_id']))
            user=await self.resolve_invite_user(client,item)
            if not isinstance(user,types.User) or user.id!=item['user_id'] or user.bot or user.deleted:raise HTTPException(400,'Получатель больше недоступен.')
            checked=await self.read_membership(client,entity,user)
            if checked['member'] is True:
                return {'state':'already_member','diagnosis':{'source':'membership_check','code':'member_before_invite','reason':'Участник уже состоял в группе при проверке. Запрос добавления не отправлен.','next_action':'Нового добавления нет.','retry_after_change':False}}
            if checked['member'] is None:
                result=invite_preflight_failure(HTTPException(502,'Неизвестный тип участника.'))
                result['diagnosis']['verification']=[checked]
                return result
        fresh=await self.detail(j['id'])
        if self.online(j['account']).client is not client or a.me.id!=j['telegram_id']:raise HTTPException(409,'Аккаунт изменился перед выполнением.')
        if spec.get('audience_run_id'):self.audience.bound(self.audience.row(spec['audience_run_id']))
        if fresh['state'] not in ('queued','scheduled') or not self.account_row(a.aid)['enabled']:return {'state':'cancelled' if fresh['state']=='cancelled' else 'pending'}
        if j['kind'] in ('post','invite_message','message'):
            text=spec['text']
            if j['kind']=='invite_message':
                entity=user
                if spec['invite_url'] not in text:text+='\n\n'+spec['invite_url']
            pub=Publisher(lambda:self.store,lambda:a,self.data)
            def before_write():
                self.workbench.guard(a,j,item,entity,user if user else entity if j['kind']=='message' else None)
                if spec.get('posting_policy') is not None or route is not None:self.posting_before_write(a,j,item,client)
            route=self.topics.saved(j,item) if j['kind']=='post' else None
            if j['kind']=='post' and getattr(entity,'forum',False) and route is None:
                raise HTTPException(409,'Форум требует явно сохранённую тему. Публикация в General остановлена.')
            if route:
                async def before_send():
                    latest_entity=await asyncio.wait_for(client.get_entity(item['chat_id']),20)
                    self.topics.identity(a.aid,client,j['telegram_id'])
                    current_topic=await self.topics.check(a,latest_entity,item['chat_id'],route['topic_id'],media=bool(spec['media']))
                    if current_topic['title']!=route['title']:
                        raise HTTPException(409,'Название выбранной темы изменилось. Проверьте новое размещение перед отправкой.')
                result=await pub.send({'media':spec['media'],'text':text,'silent':False},item,entity,
                    before_write=before_write,topic_id=route['topic_id'],before_send=before_send)
            else:
                result=await pub.send({'media':spec['media'],'text':text,'silent':False},item,entity,before_write=before_write)
            ids=[result.id] if isinstance(result,types.UpdateShortSentMessage) else []
            for update in getattr(result,'updates',[]):
                if isinstance(update,types.UpdateMessageID):ids.append(update.id)
                elif isinstance(update,(types.UpdateNewChannelMessage,types.UpdateNewMessage)):ids.append(update.message.id)
            ids=list(dict.fromkeys(ids))
            return {'state':'sent','message_ids':json.dumps(ids)} if ids else {'state':'uncertain','error':'Telegram не вернул ID. Проверьте результат; повтор отключён.'}
        if j['kind']=='link':
            self.workbench.guard(a,j,item,entity,user)
            result=await a.client(functions.messages.ExportChatInviteRequest(peer=entity,expire_date=datetime.fromtimestamp(time.time()+spec['link_hours']*3600,timezone.utc),request_needed=spec['request_needed'],title=j['title'][:32]))
            return {'state':'link_created','invite_link':result.link} if isinstance(result,types.ChatInviteExported) else {'state':'uncertain','error':'Не получена ссылка от Telegram.'}
        if spec.get('add_contacts_before_invite',False):
            contact={};item['_contact_preparation']=contact
            async def contact_context():
                self.workbench.guard(a,j,item,entity,user)
                current=await self.detail(j['id'])
                if self.online(j['account']).client is not client or a.me.id!=j['telegram_id']:
                    raise HTTPException(409,'Отправитель изменился перед добавлением контакта.')
                if current['state'] not in ('queued','scheduled'):raise HTTPException(409,'Задание остановлено.')
                await self.rights(a,entity,'invite',spec['permission_note'])
                if spec.get('audience_run_id'):self.audience.bound(self.audience.row(spec['audience_run_id']))
                self.validate_pilot_claim(j,item)
                self.validate_single_claim(j,item)
                self.workbench.guard(a,j,item,entity,user)
            try:
                await self.prepare_contact(a,user,contact,contact_context,
                    lambda:self.invite_run_before_write(j,item,client,'contact'))
            except (errors.FloodWaitError,errors.PeerFloodError,*REVOKED):raise
            except HTTPException:raise
            except Exception as exc:
                if not contact.get('request_sent'):contact['status']='not_requested'
                contact.setdefault('error_type',type(exc).__name__)
            current=await self.detail(j['id'])
            if current['state'] not in ('queued','scheduled'):
                return {'state':'cancelled' if current['state']=='cancelled' else 'pending'}
            # Contact preparation is optional. Telegram decides whether the direct invite is allowed.
            await contact_context()
        if item.get('_single_claim') is not None and self.single_mode(item['_single_claim'])=='mutual_contact_control':
            # An explicit control step must prove reciprocity on a fresh read; one-way AddContact is insufficient.
            users=await asyncio.wait_for(client(functions.users.GetUsersRequest([user])),timeout=10)
            self.validate_single_claim(j,item)
            if len(users)!=1 or not isinstance(users[0],types.User) or users[0].id!=item['user_id'] or users[0].bot or users[0].deleted:
                raise HTTPException(409,'Telegram не подтвердил исходного получателя после подготовки контакта.')
            user=users[0];item['_single_mutual_checked_at']=now()
            item['_single_mutual_confirmed']=contact_flags(user)['mutual_contact'] is True
            if not item['_single_mutual_confirmed']:
                raise HTTPException(409,'Одиночная проверка завершена без приглашения: взаимный контакт не подтверждён. Получателю нужно сохранить аккаунт отправителя; одностороннее добавление этого не заменяет.')
        if spec.get('retry_invite'):
            item.pop('_retry_source_job',None)
            item['_retry_source_job']=self.validate_invite_retry(j,item)['job']
        self.validate_pilot_claim(j,item)
        self.validate_single_claim(j,item)
        self.workbench.guard(a,j,item,entity,user)
        await self.invite_run_before_write(j,item,client)
        item['_invite_request_started']=True
        result=await a.client(functions.channels.InviteToChannelRequest(entity,[user]) if isinstance(entity,types.Channel) else functions.messages.AddChatUserRequest(entity.id,user,fwd_limit=0))
        evidence=invite_response_evidence(result,target_id=item['chat_id'],user_id=user.id,sender_id=j['telegram_id'])
        item['_invite_response']=evidence
        missing=next((x for x in getattr(result,'missing_invitees',[]) if x.user_id==user.id),None)
        if missing:
            diagnosis=missing_invitee_diagnosis(missing)
            diagnosis.update(response=evidence,verification=[])
            return {'state':'restricted','error':diagnosis['reason'],'diagnosis':diagnosis}
        verification=[]
        item['_invite_verification']=verification
        for attempt,delay in enumerate((0,1,2),1):
            if delay:await asyncio.sleep(delay)
            try:
                if self.online(j['account']).client is not client or a.me.id!=j['telegram_id']:raise HTTPException(409,'Аккаунт изменился во время проверки.')
                checked=await asyncio.wait_for(self.read_membership(client,entity,user),timeout=10)
                verification.append({'attempt':attempt,**checked})
                if self.online(j['account']).client is not client or a.me.id!=j['telegram_id']:raise HTTPException(409,'Аккаунт изменился во время проверки.')
                if spec.get('audience_run_id'):self.audience.bound(self.audience.row(spec['audience_run_id']))
            except (errors.FloodWaitError,errors.PeerFloodError,*REVOKED):raise
            except HTTPException:
                return unconfirmed_invite('Аккаунт или привязка изменились после запроса. Проверьте членство вручную.',evidence,verification)
            except Exception as exc:
                verification.append({'attempt':attempt,'member':None,'error_type':type(exc).__name__,'checked_at':now()})
                continue
            if checked['member'] is True:
                return {'state':'invited','diagnosis':{'source':'membership_check','code':'invite_member_verified','reason':'После запроса добавления Telegram подтвердил членство участника в группе.','next_action':'Проверьте результат в группе и сохранённом журнале панели.','retry_after_change':False,'response':evidence,'verification':verification}}
        if evidence['add_user_events'] and len(verification)==3 and all(v.get('member') is False for v in verification):
            result=unconfirmed_invite('Telegram вернул служебную запись добавления, но три проверки не нашли пользователя среди участников. Добавление не подтверждено; служебная запись не засчитана как успех.',evidence,verification)
            result['diagnosis'].update(code='invite_service_membership_conflict',
                next_action='Сохранены служебное событие и результаты проверок для технического разбора. Причина расхождения не установлена. Автоматического повтора нет.')
            return result
        return unconfirmed_invite('Telegram не подтвердил членство после трёх проверок. Проверьте группу вручную.',evidence,verification)
    async def tick(self):
        self.sync_primary()
        async with self.lock:
            with self.store.db:self.invite_runs.enforce_stops()
            self.expire_posting_windows()
            rows=self.store.rows("SELECT i.*,j.account FROM op_items i JOIN op_jobs j ON j.id=i.job JOIN op_accounts a ON a.id=j.account WHERE i.state='pending' AND j.state IN ('queued','scheduled') AND j.not_before<=? AND a.enabled=1 AND a.not_before<=? ORDER BY i.id",(time.time(),time.time()))
            rows=[r for r in rows if (a:=self.accounts.get(r['account'])) and a.me and a.client and a.client.is_connected()]
            rows=[r for r in rows if (gate:=self.store.get('single_attempt_job_'+str(r['job']))) is None or
                  isinstance(gate,dict) and (gate.get('state')=='released' or gate.get('state')=='armed' and gate.get('item_id')==r['id'])]
            if not rows:return
            item=rows[0];j=await self.detail(item['job'])
            if self.manual_invite_campaign.belongs(j):
                self.manual_invite_campaign.reject_queue_job(j)
                return
            if self.isolated_invite_campaign.belongs(j):
                self.isolated_invite_campaign.reject_queue_job(j)
                return
            if self.isolated_contact_batch.belongs(j):
                self.isolated_contact_batch.reject_queue_job(j)
                return
            try:a=self.online(j['account'])
            except HTTPException:return
            if a.me.id!=j['telegram_id']:
                # A stale saved sender must not stay at the head of every tick.
                # Preserve pending/terminal results and never transfer this job
                # or its one-item grant to the currently connected identity.
                reason='Сохранённый отправитель задания отличается от подключённого аккаунта. Задание поставлено на паузу; новое действие не запускалось.'
                with self.store.db:
                    gate=self.store.get('single_attempt_job_'+str(j['id']))
                    if isinstance(gate,dict) and gate.get('state') in ('armed','claimed'):
                        gate.update(state='interrupted',finished_at=now());self.save_single_attempt(gate)
                    self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=? AND state IN ('queued','scheduled')",(now(),j['id']))
                    self.store.db.execute("UPDATE op_items SET error=CASE WHEN error IS NULL OR error='' THEN ? ELSE error||char(10)||? END WHERE job=? AND state='pending'",(reason,reason,j['id']))
                return
            if j['kind']=='post':
                stopped=publication_decision(self.store,j,item)
                if stopped:
                    with self.store.db:self.store.db.execute('UPDATE op_items SET state=?,error=?,diagnosis=?,finished_at=? WHERE id=? AND state=\'pending\'',
                        (stopped['state'],stopped['error'],json.dumps(stopped['diagnosis'],ensure_ascii=False),now(),item['id']))
                    self.finish(j['id']);return
            if self.stop_pilot_goal(j):return
            pilot=j.get('pilot')
            single=self.store.get('single_attempt_job_'+str(j['id']))
            if isinstance(single,dict) and single.get('state')=='armed' and not self.single_window_valid(single):
                self.interrupt_single_attempt(j,single);return
            if pilot and pilot['state']=='budget_exhausted':
                if isinstance(single,dict) and single.get('state')!='released':
                    self.interrupt_single_attempt(j,single);return
                with self.store.db:self.store.db.execute("UPDATE op_jobs SET state='paused',updated=? WHERE id=?",(now(),j['id']))
                return
            count=self.account_daily_count(j['account'],now()[:10])
            effective_limit=pilot['account_daily_ceiling'] if pilot and pilot['active'] else self.normal_daily_limit(j)
            run_key=self.invite_runs.for_job(j['id'])
            if run_key:effective_limit=self.invite_runs.status(run_key)['account_daily_ceiling']
            if count>=effective_limit:
                if run_key:
                    with self.store.db:self.invite_runs.stop(run_key,'budget_exhausted')
                    return
                if isinstance(single,dict) and single.get('state')!='released':
                    self.interrupt_single_attempt(j,single);return
                tomorrow=(int(time.time())//86400+1)*86400
                with self.store.db:
                    self.store.db.execute('UPDATE op_jobs SET not_before=? WHERE id=?',(tomorrow,j['id']))
                    self.store.db.execute("UPDATE op_items SET error='Дневной предел достигнут. Продолжение со следующего дня UTC.' WHERE id=?",(item['id'],))
                return
            if isinstance(single,dict) and single.get('state')!='released':
                if (single.get('state')!='armed' or single.get('item_id')!=item['id'] or not self.single_binding_valid(j,single) or
                    self.store.get(f"single_attempt_item_{j['id']}_{item['id']}") is not None):
                    with self.store.db:self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=?",(j['id'],))
                    return
            try:
                with self.store.db:
                    if run_key:item['_run_claim']=self.invite_runs.claim(run_key,item['id'],client=a.client)
                    if isinstance(single,dict) and single.get('state')=='armed':
                        single.update(state='claimed',claimed_at=now());self.save_single_attempt(single)
                        self.store.db.execute('INSERT INTO settings VALUES(?,?)',(f"single_attempt_item_{j['id']}_{item['id']}",json.dumps(single['request_key'])))
                        item['_single_claim']=copy.deepcopy(single);item['_single_claim_client']=a.client
                    if pilot and pilot['active']:
                        record=self.store.get('invite_pilot_'+str(j['id']));record['attempts_used']+=1
                        self.save_pilot(j['id'],record)
                        item['_pilot_claim']={key:record[key] for key in ('job_id','account','telegram_id','chat_id','cohort_hash','utc_day',
                                                                        'expires_at','account_daily_ceiling','additional_attempts','requested_at')}
                        item['_pilot_claim']['claim_number']=record['attempts_used']
                        item['_pilot_claim_client']=a.client
                    self.store.db.execute("UPDATE op_items SET state='sending',attempted_at=? WHERE id=?",(now(),item['id']))
            except HTTPException:
                if not run_key:raise
                # Persist the stop outside the rolled-back claim transaction.
                with self.store.db:self.invite_runs.stop(run_key,'claim_rejected')
                return
        self.tracking.sync_jobs(j['id'])
        try:result=await asyncio.wait_for(self.execute(a,j,item),timeout=90)
        except (PostingPolicyStopped,WorkbenchStopped) as exc:result=exc.result
        except errors.FloodWaitError as exc:
            until=time.time()+exc.seconds+1
            with self.store.db:
                self.store.db.execute('UPDATE op_accounts SET not_before=?,error=? WHERE id=?',(until,f'Telegram просит подождать {exc.seconds} секунд.',a.aid))
                self.store.db.execute("UPDATE op_jobs SET state='paused',not_before=?,updated=? WHERE account=? AND state IN ('queued','scheduled')",(until,now(),a.aid))
            if '_invite_response' in item:
                result=unconfirmed_invite(f'После запроса добавления Telegram ограничил проверку на {exc.seconds} сек. Результат не подтверждён.',item['_invite_response'],item.get('_invite_verification'))
                result['diagnosis']['verification_error']=invite_rpc_diagnosis(exc)
            elif '_contact_preparation' in item and not item.get('_invite_request_started'):
                result={'state':'pending','error':f'Добавление контакта ограничено на {exc.seconds} сек. Приглашение не запускалось.',
                        'diagnosis':{'source':'contact_preparation','code':'contact_flood_wait','reason':'Telegram ограничил подготовку контакта. Приглашение не запускалось.','retry_after_change':False}}
            else:result={'state':'pending','error':f'FloodWait: {exc.seconds} сек. Возобновление вручную после ожидания.','diagnosis':invite_rpc_diagnosis(exc) if j['kind']=='invite' else {}}
        except (errors.PeerFloodError,*REVOKED) as exc:
            self.pause_outbound(a.aid,exc)
            if '_invite_response' in item:
                result=unconfirmed_invite('После запроса добавления Telegram остановил аккаунт. Проверьте членство вручную.',item['_invite_response'],item.get('_invite_verification'))
                result['diagnosis']['verification_error']=invite_rpc_diagnosis(exc)
            else:result={'state':'blocked','error':safe_error(exc),'diagnosis':invite_rpc_diagnosis(exc) if j['kind']=='invite' else {}}
        except errors.RPCError as exc:
            diagnosis=invite_rpc_diagnosis(exc) if j['kind']=='invite' else {'source':'rpc_error','code':type(exc).__name__,'rpc':safe_rpc_metadata(exc)}
            if isinstance(exc,(errors.ServerError,errors.TimedOutError)):
                if j['kind']=='invite':
                    result=unconfirmed_invite('Telegram не подтвердил результат запроса ('+type(exc).__name__+'). Проверьте членство вручную.',item.get('_invite_response'),item.get('_invite_verification')) if item.get('_invite_request_started') else invite_preflight_failure(exc)
                    result['diagnosis']['verification_error']=diagnosis
                else:result={'state':'uncertain','error':'Telegram не подтвердил результат ('+type(exc).__name__+'). Проверьте площадку; автоматического повтора нет.','diagnosis':diagnosis}
            else:result=invite_preflight_failure(exc) if j['kind']=='invite' and not item.get('_invite_request_started') else {'state':'restricted' if diagnosis.get('retry_after_change') else 'failed','error':safe_error(exc),'diagnosis':diagnosis}
        except HTTPException as exc:
            if not item.get('_invite_request_started'):item.pop('_retry_source_job',None)
            if j['kind']=='invite':
                result=unconfirmed_invite('Проверка контекста не завершена после запроса добавления. Проверьте членство вручную.',item.get('_invite_response'),item.get('_invite_verification')) if item.get('_invite_request_started') else invite_preflight_failure(exc)
            else:
                result={'state':'failed','error':safe_error(exc)}
                if j['kind']=='post':result['diagnosis']={'source':'preflight','code':'HTTP'+str(exc.status_code),'http_status':exc.status_code}
        except Exception as exc:
            reason=f'Результат неизвестен ({type(exc).__name__}). Проверьте Telegram; автоматический повтор отключён.'
            if j['kind']=='invite':
                result=unconfirmed_invite(reason,item.get('_invite_response'),item.get('_invite_verification')) if item.get('_invite_request_started') else invite_preflight_failure(exc)
            else:result={'state':'uncertain','error':reason}
        if j['kind']=='invite':result.setdefault('diagnosis',{})['invitation_requested']=bool(item.get('_invite_request_started'))
        if '_run_claim' in item:result.setdefault('diagnosis',{})['run_budget']=item['_run_claim']
        if '_contact_preparation' in item:
            result.setdefault('diagnosis',{})['contact_preparation']=item['_contact_preparation']
            result['diagnosis']['invitation_requested']=bool(item.get('_invite_request_started'))
        if '_single_claim' in item:
            result.setdefault('diagnosis',{})['single_attempt']={'request_key':item['_single_claim']['request_key'],
                'mode':self.single_mode(item['_single_claim']),
                'requires_mutual_contact':self.single_mode(item['_single_claim'])=='mutual_contact_control','mutual_contact_confirmed':item.get('_single_mutual_confirmed',False),
                'mutual_checked_at':item.get('_single_mutual_checked_at'),'pause_after_attempt':True}
        with self.store.db:
            self.store.db.execute('UPDATE op_items SET state=?,message_ids=?,invite_link=?,error=?,finished_at=?,diagnosis=? WHERE id=?',(result['state'],result.get('message_ids','[]'),result.get('invite_link'),result.get('error'),None if result['state']=='pending' else now(),json.dumps(result.get('diagnosis',{}),ensure_ascii=False),item['id']))
            self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?) WHERE id=?',(time.time()+j['spec']['delay_seconds'],a.aid))
            if j['kind']=='invite' and result['state'] not in ('pending','cancelled') and (item.get('_invite_request_started') or result['state']=='already_member'):
                self.store.db.execute("INSERT INTO op_invited VALUES(?,?,?,?) ON CONFLICT(chat_id,user_id) DO UPDATE SET state=excluded.state,job=excluded.job WHERE op_invited.state='already_member' OR (op_invited.state='restricted' AND op_invited.job=?)",(item['chat_id'],item['user_id'],result['state'],j['id'],item.get('_retry_source_job',-1)))
            if result['state']=='uncertain':self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'",(j['id'],))
            if '_single_claim' in item:
                single=item['_single_claim'];single.update(state='finished',finished_at=now(),result_state=result['state'],
                    invitation_requested=bool(item.get('_invite_request_started')))
                self.save_single_attempt(single)
                self.store.db.execute("UPDATE op_jobs SET state='paused' WHERE id=? AND state!='cancelled'",(j['id'],))
        async with self.lock:
            with self.store.db:self.invite_runs.enforce_stops()
        if not self.stop_pilot_goal(await self.detail(j['id'])):self.finish(j['id'])
    async def loop(self):
        while True:
            try:
                if time.time()-self.last_connect>30:
                    self.last_connect=time.time()
                    for row in self.store.rows('SELECT id FROM op_accounts WHERE enabled=1'):
                        a=self.accounts[row['id']]
                        try:await a.connect()
                        except Exception as exc:a.report_error(exc)
                self.tracking.bind_clients()
                self.tracking.sync_jobs()
                await self.tick()
                self.tracking.sync_jobs()
                await self.tracking.monitor_once()
                self.store.put('worker_error',None)
            except Exception as exc:self.store.put('worker_error','Ошибка очереди: '+type(exc).__name__)
            await asyncio.sleep(2)
    async def shutdown(self):
        await self.manual_invite_campaign.shutdown()
        await self.isolated_invite_campaign.shutdown()
        await self.isolated_contact_batch.shutdown()
        await self.audience.shutdown()
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
        self.tracking.unbind_clients()
        for a in self.accounts.values():
            if a.aid!='primary' and a.client:await a.client.disconnect()
        self.store.db.close()
