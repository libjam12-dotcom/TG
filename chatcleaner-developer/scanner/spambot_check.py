"""One explicit /start to verified official SpamBot on the existing paused client.

Native settings claims survive lost ACK/restart. No reconnect, unblock, appeal,
invitation, global resume, generic-message input, or automatic read/write retry.
"""
import asyncio
import json
import secrets
import time
from datetime import datetime,timezone
from contextlib import asynccontextmanager

from fastapi import APIRouter,Depends,Request,Response
from pydantic import BaseModel,Field,field_validator
from telethon import types,functions,errors
from engine import now
from isolated_invite import fail,isolated_admin_only,digest,REVOKED
from operations import safe_error,safe_rpc_metadata

BOT=178220800
PREFIX='official_spambot_check_request_'
SENDER_PREFIX='official_spambot_check_sender_'
SCOPE='official-spambot-one-start-v1'
CLARIFICATION_SCOPE='official-spambot-factual-clarification-v1'
SCOPES=(SCOPE,CLARIFICATION_SCOPE)
TIMEOUT=35
WAIT_REPLY=3
TERMINAL=('reply_received','reply_pending','blocked','stopped','unknown','interrupted')

async def spambot_owner_only(request:Request,response:Response):
    await isolated_admin_only(request,response)
    if request.headers.get('x-panel-role')!='owner':raise fail(403,'Проверка SpamBot доступна только владельцу панели.')


class SpamBotRequest(BaseModel):
    model_config={'extra':'forbid'}
    confirmed:bool
    telegram_id:int=Field(gt=0,strict=True)
    request_key:str=Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    @field_validator('confirmed',mode='before')
    @classmethod
    def explicit(cls,value):
        if value is not True:raise ValueError('Отдельно подтвердите один /start официальному SpamBot.')
        return True

class SpamBotClarification(SpamBotRequest):
    source_check_key:str=Field(pattern=r'^[A-Za-z0-9._:-]{16,80}$')
    source_job_id:int=Field(gt=0,strict=True)

class SpamBotCheck:
    def __init__(self,ops):
        self.ops,self.store=ops,ops.store
        self.router=APIRouter(dependencies=[Depends(spambot_owner_only)])
        self.router.add_api_route('/accounts/{aid}/spambot-check',self.check,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/spambot-check/{request_key}',self.status,methods=['GET'])
        self.router.add_api_route('/accounts/{aid}/spambot-clarification',self.clarification,methods=['POST'])
        self.router.add_api_route('/accounts/{aid}/spambot-clarification/{request_key}',self.status,methods=['GET'])

    def initialize(self):
        with self.store.db:
            for row in self.store.rows('SELECT value FROM settings WHERE key LIKE ?',(PREFIX+'%',)):
                record=json.loads(row['value'])
                if record.get('scope') in SCOPES and record.get('state') not in TERMINAL:
                    record.update(state='interrupted',error_type='ProcessRestart',finished_at=now(),automatic_retry=False)
                    self.save(record)

    def save(self,record):
        self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (PREFIX+record['request_key'],json.dumps(record,ensure_ascii=False)))

    def public(self,record):
        return {k:v for k,v in record.items() if k not in ('digest','random_id','source_check_digest','input_digest')}

    async def status(self,aid:str,request_key:str):
        record=self.store.get(PREFIX+request_key)
        if not isinstance(record,dict) or record.get('scope') not in SCOPES or record.get('account')!=aid:
            raise fail(404,'Сохранённая проверка SpamBot не найдена.')
        return self.public(record)  # cached-only; never touches Telegram

    def projection(self,uid):
        return digest({'jobs':self.store.rows('SELECT * FROM op_jobs WHERE telegram_id=? ORDER BY id',(uid,)),
            'items':self.store.rows('SELECT i.* FROM op_items i JOIN op_jobs j ON j.id=i.job WHERE j.telegram_id=? ORDER BY i.id',(uid,)),
            'invited':self.store.rows('SELECT * FROM op_invited ORDER BY chat_id,user_id')})

    def signature(self,aid):
        return digest({key:self.store.get(key) for key in ('account_proxy_'+aid,'account_profile_'+aid,'account_api_'+aid)})

    def guard(self,ctx):
        if time.monotonic()>=ctx['deadline']:raise fail(504,'Время проверки SpamBot истекло; автоматического повтора нет.')
        a=self.ops.readable_account(ctx['aid'])
        row=self.ops.account_row(ctx['aid'])
        if (a is not ctx['account'] or a.client is not ctx['client'] or a.me.id!=ctx['uid'] or
            row!=ctx['row'] or a.error!=ctx['runtime_error'] or
            self.store.get('outbound_pause_revision_'+ctx['aid'],0)!=ctx['revision'] or
            self.signature(ctx['aid'])!=ctx['signature'] or self.projection(ctx['uid'])!=ctx['projection'] or
            getattr(a.client,'_request_retries',None)!=0 or getattr(a.client,'flood_sleep_threshold',None)!=0):
            raise fail(409,'Отправитель, защита, прокси или сохранённые задания изменились; нового запроса нет.')
        if ctx.get('source_check_key') and digest(self.store.get(PREFIX+ctx['source_check_key']))!=ctx['source_check_digest']:
            raise fail(409,'Исходный ответ SpamBot изменился; новое сообщение не отправляется.')

    async def read(self,ctx,call,timeout=8):
        self.guard(ctx)
        result=await asyncio.wait_for(call(),min(timeout,ctx['deadline']-time.monotonic()))
        self.guard(ctx)
        return result

    def protect(self,ctx,exc):
        # Error handling may strengthen protection, never clear it or touch jobs.
        a=self.ops.accounts.get(ctx['aid'])
        if a is not ctx['account'] or a.client is not ctx['client']:return
        row=self.ops.account_row(ctx['aid'])
        if row['telegram_id']!=ctx['uid']:return
        with self.store.db:
            if isinstance(exc,errors.FloodWaitError):
                self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?),error=COALESCE(error,?) WHERE id=?',
                    (time.time()+exc.seconds+1,safe_error(exc),ctx['aid']))
            elif isinstance(exc,(errors.PeerFloodError,*REVOKED)):
                error=row['error'] or safe_error(exc)
                self.store.db.execute('UPDATE op_accounts SET enabled=0,error=? WHERE id=?',(error,ctx['aid']))
                if row['enabled'] or not row['error']:
                    key='outbound_pause_revision_'+ctx['aid']
                    self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                        (key,json.dumps(self.store.get(key,0)+1)))
                a.error=a.error or error

    @staticmethod
    def sent_id(response,random_id):
        if isinstance(response,types.UpdateShortSentMessage):return response.id
        for update in getattr(response,'updates',[]):
            if isinstance(update,types.UpdateMessageID) and update.random_id==random_id:return update.id
        return None

    @asynccontextmanager
    async def locked(self,deadline):
        try:await asyncio.wait_for(self.ops.lock.acquire(),min(5,max(.01,deadline-time.monotonic())))
        except TimeoutError:raise fail(409,'Другое действие ещё выполняется. /start не отправлялся.') from None
        try:yield
        finally:self.ops.lock.release()

    def clarification_evidence(self,aid,spec):
        source=self.store.get(PREFIX+spec.source_check_key)
        if (not isinstance(source,dict) or source.get('scope')!=SCOPE or source.get('account')!=aid or
            source.get('telegram_id')!=spec.telegram_id or source.get('bot_id')!=BOT or
            source.get('state')!='reply_received' or source.get('send_claims')!=1 or source.get('reply_read_attempts')!=1):
            raise fail(409,'Нужен сохранённый подтверждённый ответ официального SpamBot этого же аккаунта.')
        clean=[]
        for reply in source.get('replies',[]):
            if (type(reply.get('id')) is int and reply['id']>source.get('outgoing_message_id',0) and
                'no limits are currently applied to your account' in str(reply.get('text','')).lower()):
                when=datetime.fromisoformat(reply['date'].replace('Z','+00:00'))
                if when.tzinfo:clean.append((when,reply['id']))
        if not clean:raise fail(409,'В исходной проверке нет подтверждённого ответа об отсутствии ограничений.')
        clean_at,reply_id=max(clean)
        jobs=self.store.rows('SELECT * FROM op_jobs WHERE id=?',(spec.source_job_id,))
        if not jobs or jobs[0]['account']!=aid or jobs[0]['telegram_id']!=spec.telegram_id or jobs[0]['kind']!='invite':
            raise fail(409,'Исходное приглашение принадлежит другому аккаунту или виду задания.')
        job=jobs[0];payload=json.loads(job['spec']);cfg=payload.get('manual_invite_campaign',{}).get('config',{})
        if (cfg.get('account')!=aid or cfg.get('telegram_id')!=spec.telegram_id or cfg.get('consent_confirmed') is not True or
            len(str(cfg.get('consent_note','')).strip())<8):
            raise fail(409,'В исходном задании нет сохранённого подтверждения согласия.')
        items=self.store.rows('SELECT * FROM op_items WHERE job=? ORDER BY id',(spec.source_job_id,))
        failures=[]
        for item in items:
            diagnosis=json.loads(item.get('diagnosis') or '{}');error=diagnosis.get('verification_error',diagnosis);rpc=error.get('rpc',{})
            if (diagnosis.get('invitation_requested') is True and error.get('code')=='PeerFloodError' and
                rpc.get('exception_type')=='PeerFloodError' and rpc.get('message')=='PEER_FLOOD' and
                rpc.get('request_constructor')=='InviteToChannelRequest' and item.get('attempted_at')):
                when=datetime.fromisoformat(item['attempted_at'].replace('Z','+00:00'))
                if when.tzinfo and when>clean_at:failures.append((when,item['id']))
        if not failures:raise fail(409,'Нет последующего подтверждённого PEER_FLOOD от InviteToChannelRequest.')
        failed_at,item_id=min(failures)
        stamp=lambda value:value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        text=('Здравствуйте. Прошу уточнить статус ограничений этого аккаунта. '
            +stamp(clean_at)+' официальный @SpamBot сообщил: «No limits are currently applied to your account». '
            +stamp(failed_at)+' из этого же аккаунта метод channels.inviteToChannel вернул PEER_FLOOD. '
            'В сохранённом задании панели пользователь подтвердил наличие согласия на приглашение. '
            'Уточните, пожалуйста, причину и срок действия этого ограничения и как объясняется расхождение с ответом SpamBot.')
        evidence=dict(source_check_key=spec.source_check_key,source_job_id=spec.source_job_id,source_reply_id=reply_id,
            source_item_id=item_id,clean_reply_at=clean_at.isoformat(),peer_flood_at=failed_at.isoformat(),consent_confirmed=True,
            evidence_digest=digest({'check':source,'job':job,'items':items}),source_check_digest=digest(source),factual_text=text)
        return evidence

    async def check(self,aid:str,spec:SpamBotRequest):
        return await self._send(aid,spec)

    async def clarification(self,aid:str,spec:SpamBotClarification):
        return await self._send(aid,spec,clarify=True)

    async def _send(self,aid,spec,clarify=False):
        deadline=time.monotonic()+TIMEOUT
        async with self.locked(deadline):
            scope=CLARIFICATION_SCOPE if clarify else SCOPE
            binding=dict(scope=scope,account=aid,telegram_id=spec.telegram_id,bot_id=BOT)
            if clarify:binding.update(source_check_key=spec.source_check_key,source_job_id=spec.source_job_id)
            old=self.store.get(PREFIX+spec.request_key)
            if clarify and old:
                if old.get('scope')!=scope or old.get('input_digest')!=digest(binding):raise fail(409,'Ключ уже принадлежит другому сообщению.')
                return self.public(old)
            evidence=self.clarification_evidence(aid,spec) if clarify else None
            text=evidence['factual_text'] if evidence else '/start'
            request_digest=digest({'scope':scope,'account':aid,'telegram_id':spec.telegram_id,'bot_id':BOT,'text':text,
                **({'evidence_digest':evidence['evidence_digest']} if evidence else {})})
            if old:
                if old.get('digest')!=request_digest:raise fail(409,'Ключ уже принадлежит другой проверке.')
                return self.public(old)
            if aid=='primary':raise fail(403,'Эта разовая диагностика требует отдельный подключённый аккаунт панели.')
            a=self.ops.auth(aid);self.ops.readable_account(aid);row=self.ops.account_row(aid)
            if a.me.id!=spec.telegram_id or row['telegram_id']!=spec.telegram_id or row['enabled']!=0:
                raise fail(409,'Нужен показанный отдельный аккаунт с приостановленными исходящими.')
            if self.store.rows("SELECT 1 FROM op_jobs j LEFT JOIN op_items i ON i.job=j.id WHERE j.telegram_id=? AND (j.state IN ('queued','scheduled') OR i.state='sending') LIMIT 1",(spec.telegram_id,)):
                raise fail(409,'Сначала остановите другие исходящие задания отправителя.')
            for saved in self.store.rows("SELECT value FROM settings WHERE key LIKE 'manual_invite_campaign_job_%'"):
                record=json.loads(saved['value'])
                if record.get('state') in ('running','waiting') and record.get('config',{}).get('telegram_id')==spec.telegram_id:
                    raise fail(409,'У отправителя есть активная ручная кампания.')
            previous=self.store.get(SENDER_PREFIX+str(spec.telegram_id))
            previous=self.store.get(PREFIX+previous) if isinstance(previous,str) else None
            if isinstance(previous,dict) and previous.get('state') in ('claimed','reading','unknown','interrupted'):
                raise fail(409,'Предыдущая попытка имеет неизвестный результат; новый /start не отправляется.')
            ctx=dict(aid=aid,uid=spec.telegram_id,account=a,client=a.client,row=row,runtime_error=a.error,
                revision=self.store.get('outbound_pause_revision_'+aid,0),signature=self.signature(aid),
                projection=self.projection(spec.telegram_id),deadline=deadline)
            if evidence:ctx.update(source_check_key=spec.source_check_key,source_check_digest=evidence['source_check_digest'])
            record=None
            try:
                self.guard(ctx)
                me=await self.read(ctx,a.client.get_me)
                if not isinstance(me,types.User) or me.id!=spec.telegram_id or me.bot or me.deleted:
                    raise fail(409,'Telegram не подтвердил выбранного отправителя.')
                bot=await self.read(ctx,lambda:a.client.get_entity('SpamBot'))
                if (not isinstance(bot,types.User) or bot.id!=BOT or not bot.bot or not bot.verified or bot.deleted or bot.min or
                    (bot.username or '').lower()!='spambot' or type(bot.access_hash) is not int or not bot.access_hash):
                    raise fail(409,'Официальный SpamBot не подтверждён; сообщение не отправлялось.')
                peer=types.InputPeerUser(BOT,bot.access_hash)
                self.guard(ctx)
                random_id=secrets.randbits(63) or 1
                record=dict(scope=scope,account=aid,telegram_id=spec.telegram_id,bot_id=BOT,request_key=spec.request_key,
                    digest=request_digest,random_id=random_id,state='claimed',send_claims=1,reply_read_attempts=0,
                    automatic_retry=False,invitation_calls=0,appeal_calls=0,unblock_calls=0,checked_at=now(),
                    attempted_at=now(),outbound_paused=True,replies=[])
                if evidence:record.update(input_digest=digest(binding),**evidence)
                with self.store.db:
                    self.store.db.execute('INSERT INTO settings VALUES(?,?)',(PREFIX+spec.request_key,json.dumps(record)))
                    self.store.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                        (SENDER_PREFIX+str(spec.telegram_id),json.dumps(spec.request_key)))
                self.guard(ctx)
                response=await asyncio.wait_for(a.client(functions.messages.SendMessageRequest(peer=peer,message=text,random_id=random_id,no_webpage=True)),8)
                self.guard(ctx)
                outgoing_id=self.sent_id(response,random_id)
                if type(outgoing_id) is not int or outgoing_id<=0:
                    record.update(state='unknown',error_type='MissingSendReceipt',finished_at=now())
                else:
                    record.update(state='reading',outgoing_message_id=outgoing_id)
                    with self.store.db:self.save(record)
                    await self.read(ctx,lambda:asyncio.sleep(WAIT_REPLY),timeout=WAIT_REPLY+1)
                    record['reply_read_attempts']=1
                    with self.store.db:self.save(record)
                    response=await self.read(ctx,lambda:a.client(functions.messages.GetHistoryRequest(peer=peer,offset_id=0,offset_date=None,
                        add_offset=0,limit=5,max_id=0,min_id=outgoing_id,hash=0)))
                    for message in list(getattr(response,'messages',[]))[:5]:
                        date=getattr(message,'date',None);target=getattr(message,'peer_id',None)
                        if (isinstance(message,types.Message) and message.id>outgoing_id and not message.out and
                            getattr(message,'sender_id',None)==BOT and isinstance(target,types.PeerUser) and target.user_id in (BOT,ctx['uid']) and
                            isinstance(date,datetime) and date.tzinfo and date.timestamp()>=datetime.fromisoformat(record['attempted_at']).timestamp()-1):
                            record['replies'].append({'id':message.id,'date':date.astimezone(timezone.utc).isoformat(),'text':(message.message or '')[:4096]})
                    record.update(state='reply_received' if record['replies'] else 'reply_pending',finished_at=now())
                with self.store.db:self.save(record)
                return self.public(record)
            except asyncio.CancelledError:
                if record:
                    record.update(state='unknown',error_type='RequestInterrupted',finished_at=now())
                    with self.store.db:self.save(record)
                raise
            except Exception as exc:
                if isinstance(exc,(errors.FloodWaitError,errors.PeerFloodError,*REVOKED)):self.protect(ctx,exc)
                if not record:
                    if isinstance(exc,(errors.FloodWaitError,errors.PeerFloodError,*REVOKED)):
                        raise fail(429 if isinstance(exc,errors.FloodWaitError) else 409,
                            'Telegram остановил предпроверку ('+type(exc).__name__+'). /start не отправлялся; автоматического повтора нет.') from None
                    raise
                record.update(state='blocked' if isinstance(exc,(errors.UserIsBlockedError,errors.YouBlockedUserError)) else 'stopped'
                    if isinstance(exc,(errors.FloodWaitError,errors.PeerFloodError,*REVOKED)) else 'unknown',
                    error_type=type(exc).__name__,finished_at=now())
                if isinstance(exc,errors.RPCError):record['rpc']=safe_rpc_metadata(exc)
                if isinstance(exc,errors.FloodWaitError):record['wait_seconds']=exc.seconds
                with self.store.db:self.save(record)
                return self.public(record)
