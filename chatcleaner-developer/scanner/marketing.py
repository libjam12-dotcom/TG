"""Owner-only demand workspace. Separate store, shared read-only Telegram transport.
Never copies API credentials, Telegram sessions, client rules, leads or destinations.
"""
import asyncio
import contextlib
import csv
import io
import json
import re
import time
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel, Field
from telethon import errors, functions, types, utils
from engine import Store, explain, matches, now, words
from contact_readiness import ContactReadiness
from directory import Directory
from history import HistoryRunner
from marketing_topics import seed_marketing
from functools import lru_cache
from owner_sender import OwnerSender
from owner_hub import OwnerHub
from owner_ai import OwnerConversations
from signm_pipeline import SignmPipeline, Prepare


@lru_cache(maxsize=512)
def saved_message_matches(text, rule_json):
    # Rules are part of the key: edits invalidate a result without a stale TTL.
    return bool(matches(text, json.loads(rule_json)))

PREFIX = '/api/scanner/marketing'
SOURCE_LIMIT = 100

async def owner_only(request: Request):
    # Both headers must be overwritten by nginx from the authenticated session.
    if not request.headers.get('x-panel-user') or request.headers.get('x-panel-role') != 'owner':
        raise HTTPException(403, 'Раздел «Маркетинг» доступен только владельцу.')

class Enabled(BaseModel):
    enabled: bool

class Source(BaseModel):
    reference: str = Field(min_length=1, max_length=200)

class Rule(BaseModel):
    id: int | None = None
    name: str = Field(min_length=1,max_length=80)
    sources: list[int] = Field(default_factory=list,max_length=SOURCE_LIMIT)
    keywords: str = Field(min_length=1,max_length=8000)
    required: str = Field(default='',max_length=8000)
    excluded: str = Field(default='',max_length=8000)
    substring: bool = False
    demand_only: bool = True
    enabled: bool = True
    language: Literal['ru','ru_en'] | None = None
    lead_profile: Literal['signm_dubai_v1'] | None = None

class Assignment(BaseModel):
    sources: list[int] = Field(min_length=1,max_length=SOURCE_LIMIT)
    rules: list[int] = Field(min_length=1,max_length=25)

class Preview(BaseModel):
    rule_id: int
    text: str = Field(min_length=1,max_length=10000)

class LeadStatus(BaseModel):
    status: Literal['new','working','rejected']

class DubaiProfile(BaseModel):
    confirmed: bool = False
    sources: list[int] = Field(min_length=1,max_length=SOURCE_LIMIT)

class ReadTransport:
    """Deliberately narrow interface: no sends, joins, private dialogs or auth."""
    def __init__(self, workspace):
        self.workspace=workspace
        self.lock=asyncio.Lock()

    def is_connected(self):
        rt=self.workspace.runtime
        return bool(rt.client and rt.me and rt.client.is_connected() and self.workspace.account_matches())

    def current(self):
        if not self.is_connected():
            raise HTTPException(409,'Общее подключение недоступно или аккаунт изменился. Проверьте «Лидсканер → Подключение».')
        if max(self.workspace.store.get('scan_not_before',0),self.workspace.get_client_store().get('scan_not_before',0),self.workspace.get_client_store().get('history_retry_at',0)) > time.time():
            raise HTTPException(429,'Telegram ограничил частоту запросов. Дождитесь завершения паузы.')
        return self.workspace.runtime.client

    async def call(self, name, *args, **kwargs):
        async with self.lock:
            client=self.current()
            try:
                result=await asyncio.wait_for(getattr(client,name)(*args,**kwargs),timeout=40)
                if self.workspace.runtime.client is not client or not self.workspace.account_matches():
                    raise HTTPException(409,'Подключённый аккаунт изменился. Результат не сохранён.')
                return result
            except errors.FloodWaitError as exc:
                self.workspace.defer(exc.seconds)
                raise
            except (errors.SessionRevokedError, errors.AuthKeyUnregisteredError) as exc:
                self.workspace.runtime.report_error(exc)
                raise

    async def get_entity(self,*a,**k): return await self.call('get_entity',*a,**k)
    async def get_messages(self,*a,**k): return await self.call('get_messages',*a,**k)
    async def __call__(self,request):
        if not isinstance(request,functions.contacts.SearchRequest):
            raise HTTPException(403,'Маркетинг поддерживает только чтение внешних сообществ.')
        return await self.call('__call__',request)

    async def iter_messages(self,*a,**k):
        client=self.current()
        iterator=client.iter_messages(*a,**k).__aiter__()
        while True:
            async with self.lock:
                if self.current() is not client:
                    raise HTTPException(409,'Аккаунт изменился. Анализ остановлен.')
                try:
                    item=await asyncio.wait_for(anext(iterator),timeout=40)
                except StopAsyncIteration:
                    break
                except errors.FloodWaitError as exc:
                    self.workspace.defer(exc.seconds)
                    raise
            if self.workspace.runtime.client is not client or not self.workspace.account_matches():
                raise HTTPException(409,'Аккаунт изменился. Анализ остановлен.')
            yield item

class Marketing:
    def __init__(self, path, get_runtime, get_client_store):
        self.store=Store(path)
        self.contact_review=ContactReadiness(self.store)
        self.contact_review.initialize()
        self.get_runtime=get_runtime
        self.get_client_store=get_client_store
        self.transport=ReadTransport(self)
        self.sender=OwnerSender(Path(path).parent/'owner-telegram',lambda:self.get_client_store().get('api'),self.store)
        self.inbox=OwnerConversations(lambda:self.store,lambda:self.sender)
        self.pipeline=SignmPipeline(self)
        self.sender.on_event=self.inbox.on_event
        self.task=None
        self.scan_pause=1
        if 'scan_stats' not in {row['name'] for row in self.store.db.execute('PRAGMA table_info(sources)')}:
            self.store.db.execute("ALTER TABLE sources ADD COLUMN scan_stats TEXT NOT NULL DEFAULT '{}'")
            self.store.db.commit()
        self.operation_lock=asyncio.Lock()
        self.router=APIRouter(prefix=PREFIX,dependencies=[Depends(owner_only)])
        self.directory=Directory(lambda:self.store,lambda:self,prefix='/parser',before_start=self.before_analysis)
        self.history=HistoryRunner(lambda:self.store,lambda:self,prefix='/history',before_start=self.before_analysis)
        self.router.include_router(self.sender.router)
        self.hub=OwnerHub(self.sender,Path(path).parent/"owner-projects"/"singularity.json")
        self.router.include_router(self.hub.router)
        self.router.include_router(self.inbox.router)
        self.router.include_router(self.pipeline.router)
        self.router.add_api_route('/pipeline/profile',self.dubai_profile,methods=['POST'])
        self.router.include_router(self.directory.router)
        self.router.include_router(self.history.router)
        self.router.add_api_route('/contact-review/{lead_id}/history',self.contact_review.history,methods=['GET'])
        for path,func,methods in [
            ('/state',self.json_state,['GET']),('/sources',self.add_source,['POST']),
            ('/sources/{source_id}',self.source_toggle,['POST']),
            ('/sources/{source_id}/remove',self.source_remove,['POST']),
            ('/rules',self.rule_save,['POST']),('/rules/{rule_id}',self.rule_toggle,['POST']),
            ('/assign',self.assign,['POST']),('/preview',self.preview,['POST']),
            ('/running',self.running,['POST']),('/leads/{lead_id}',self.lead_status,['POST']),
            ('/export',self.export,['GET']),
        ]: self.router.add_api_route(path,func,methods=methods)

    @property
    def discovery_next(self): return getattr(self.runtime,'discovery_next',0)
    @discovery_next.setter
    def discovery_next(self,value): self.runtime.discovery_next=value
    def before_analysis(self):
        self.bind_account()
        if any(worker.task and not worker.task.done() for worker in (self.directory,self.history)):
            raise HTTPException(409,'Дождитесь завершения текущего парсинга или анализа истории.')
    @property
    def runtime(self): return self.get_runtime()
    @property
    def client(self): return self.transport
    @property
    def me(self): return self.runtime.me if self.account_matches() else None
    def account_matches(self):
        return self.store.get('account_id') in (None,getattr(self.runtime.me,'id',None))
    def bind_account(self):
        self.transport.current()
        if self.store.get('account_id') is None:
            self.store.put('account_id',self.runtime.me.id)
    def retry_at(self):
        return max(self.store.get('scan_not_before',0),self.get_client_store().get('scan_not_before',0),
                   self.store.get('history_retry_at',0),self.get_client_store().get('history_retry_at',0),getattr(self.runtime,'discovery_next',0))
    def defer(self,seconds):
        until=time.time()+seconds+1
        self.store.put('scan_not_before',until)
        self.get_client_store().put('scan_not_before',max(until,self.get_client_store().get('scan_not_before',0)))
        self.runtime.discovery_next=max(until,getattr(self.runtime,'discovery_next',0))
        self.discovery_next=max(until,self.discovery_next)
    def initialize(self):
        seed_marketing(self.store)
        self.contact_review.initialize()
        self.inbox.initialize()
        self.inbox.task=asyncio.create_task(self.inbox.loop())
        self.sender.task=asyncio.create_task(self.sender.loop())
        # Private findings are deliberately panel-only, with no notification worker.
        self.store.put('destination',None)
        self.directory.recover();self.history.recover()
        self.task=asyncio.create_task(self.loop())
    async def shutdown(self):
        await self.directory.shutdown();await self.history.shutdown()
        await self.inbox.shutdown();await self.sender.shutdown()
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
        self.store.db.close()

    async def json_state(self,status:Literal['all','new','working','rejected']='all',topic:int=Query(default=0,ge=0),offset:int=Query(default=0,ge=0),contact:Literal['all','telegram_author','marketplace_task','missing_contact','contacted_dialogue']='all'):
        # The state already contains JSON primitives. Avoid FastAPI's repeated
        # recursive conversion of every keyword, source and message field.
        return JSONResponse(await self.state(status,topic,offset,contact))

    async def state(self,status:Literal['all','new','working','rejected']='all',topic:int=Query(default=0,ge=0),offset:int=Query(default=0,ge=0),contact:Literal['all','telegram_author','marketplace_task','missing_contact','contacted_dialogue']='all'):
        self.contact_review.backfill()
        where="WHERE (?='all' OR l.status=?) AND (?=0 OR EXISTS(SELECT 1 FROM json_each(l.matches) m WHERE json_extract(m.value,'$.id')=?)) AND (?='all' OR l.id IN (SELECT lead_id FROM owner_contact_readiness WHERE kind=?))"
        params=(status,status,topic,topic,contact,contact)
        count=self.store.db.execute('SELECT COUNT(*) FROM leads l '+where,params).fetchone()[0]
        page_size=50
        offset=min(offset,((count-1)//page_size)*page_size if count else 0)
        leads=self.store.rows('SELECT l.*,s.title AS source_title,s.username AS source_username FROM leads l LEFT JOIN sources s ON s.id=l.chat_id '+where+' ORDER BY l.created DESC,l.id DESC LIMIT ? OFFSET ?',params+(page_size,offset))
        rules={r['id']:r for r in self.store.rules()}
        rule_json={key:json.dumps(rule,sort_keys=True,ensure_ascii=False) for key,rule in rules.items()}
        assessments=self.contact_review.saved(leads)
        for lead in leads:
            lead['contact_readiness']=assessments[lead['id']]
            lead['matches']=json.loads(lead['matches']);lead.pop('fingerprint',None)
            lead['matches_current_rules']=any(saved_message_matches(lead['text'],rule_json[m['id']]) for m in lead['matches'] if m['id'] in rules)
        ready=self.transport.is_connected()
        sources=self.store.rows('SELECT * FROM sources ORDER BY title')
        health=self.store.get('scan_health',{})
        eligible=[s for s in sources if s['enabled'] and any(r['enabled'] and s['id'] in r['sources'] for r in rules.values())]
        health.update(task_alive=bool(self.task and not self.task.done()),eligible_sources=len(eligible),
                      latest_source_success=max((s['last_scan'] or '' for s in eligible),default='') or None,
                      oldest_source_success=min((s['last_scan'] or '' for s in eligible),default='') or None,
                      messages_checked=sum(s['messages_checked'] for s in sources),
                      sources_with_errors=sum(bool(s['error']) for s in eligible))
        return dict(storage_health=self.store.storage_health(),shared_storage_health=self.get_client_store().storage_health(),connection='connected' if ready else 'not_connected',shared_connection=True,
                    running=self.store.get('running',False) and ready,requested_running=self.store.get('running',False),
                    error=self.store.get('error') if self.account_matches() else 'Общий Telegram-аккаунт изменился. Поиск приостановлен.',
                    retry_at=self.retry_at(),source_limit=SOURCE_LIMIT,sources=sources,scan_health=health,
                    rules=list(rules.values()),leads=leads,total=self.store.db.execute('SELECT COUNT(*) FROM leads').fetchone()[0],
                    filtered_total=count,offset=offset,page_size=page_size,next_offset=offset+page_size if offset+len(leads)<count else None,
                    history=self.history.summary(),pending=0,contact_counts={r['kind']:r['count'] for r in self.store.rows('SELECT kind,COUNT(*) AS count FROM owner_contact_readiness GROUP BY kind')})

    async def add_source(self,data:Source):
        async with self.operation_lock:
            self.bind_account()
            ref=data.reference.strip()
            if ref.startswith('https://t.me/'):ref=ref[13:].rstrip('/')
            if not re.fullmatch(r'@?[A-Za-z][A-Za-z0-9_]{3,31}',ref):
                raise HTTPException(400,'Укажите публичный @username или https://t.me/username.')
            entity=await self.client.get_entity(ref)
            if not isinstance(entity,types.Channel) or not getattr(entity,'username',None):
                raise HTTPException(400,'Выберите публичную группу или канал.')
            if getattr(entity,'creator',False) or getattr(entity,'admin_rights',None):
                raise HTTPException(400,'Административные площадки исключены. Выберите внешнее сообщество.')
            cid=utils.get_peer_id(entity)
            if cid==self.get_client_store().get('destination'):
                raise HTTPException(400,'Чат уведомлений нельзя использовать как источник.')
            exists=self.store.db.execute('SELECT 1 FROM sources WHERE id=?',(cid,)).fetchone()
            if not exists and self.store.db.execute('SELECT COUNT(*) FROM sources').fetchone()[0]>=SOURCE_LIMIT:
                raise HTTPException(400,f'В личном разделе доступно до {SOURCE_LIMIT} источников.')
            latest=await self.client.get_messages(entity,limit=1)
            mode='public' if getattr(entity,'left',False) else 'member'
            with self.store.db:
                self.store.db.execute('INSERT INTO sources(id,title,username,cursor,access_mode,last_scan) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET title=excluded.title,username=excluded.username,enabled=1,error=NULL,access_mode=excluded.access_mode',
                    (cid,entity.title,entity.username,latest[0].id if latest else 0,mode,now()))
            return dict(ok=True,id=cid,access_mode=mode)

    async def source_toggle(self,source_id:int,data:Enabled):
        source=self.store.db.execute('SELECT * FROM sources WHERE id=?',(source_id,)).fetchone()
        if not source:raise HTTPException(404,'Источник не найден.')
        if data.enabled:await self.add_source(Source(reference=source['username']))
        else:
            with self.store.db:self.store.db.execute('UPDATE sources SET enabled=0 WHERE id=?',(source_id,))
        return {'ok':True}

    async def source_remove(self,source_id:int):
        with self.store.db:
            for r in self.store.rules():
                if source_id in r['sources']:
                    r['sources'].remove(source_id)
                    enabled=r['enabled'] and bool(r['sources'])
                    self.store.db.execute('UPDATE rules SET data=?,enabled=? WHERE id=?',
                        (json.dumps({k:v for k,v in r.items() if k not in ('id','name','enabled')}),enabled,r['id']))
            self.store.db.execute('DELETE FROM sources WHERE id=?',(source_id,))
        return {'ok':True}

    async def rule_save(self,data:Rule):
        valid={s['id'] for s in self.store.rows('SELECT id FROM sources')}
        if not set(data.sources)<=valid:raise HTTPException(400,'Выберите существующие источники.')
        if data.enabled and not data.sources:raise HTTPException(400,'Выберите источники или сохраните поиск выключенным.')
        if not data.name.strip():raise HTTPException(400,'Введите название поиска.')
        old=next((r for r in self.store.rules() if r['id']==data.id),None) if data.id else None
        value=dict(sources=list(dict.fromkeys(data.sources)),substring=data.substring,demand_only=data.demand_only,
                   language=data.language or (old or {}).get('language','ru'),contractor_only=True)
        profile=data.lead_profile or (old or {}).get('lead_profile')
        if profile:value['lead_profile']=profile
        for field in ('keywords','required','excluded'):
            value[field]=words(getattr(data,field))
            if any(len(w)>200 for w in value[field]):raise HTTPException(400,'Фраза не должна превышать 200 символов.')
        if not value['keywords']:raise HTTPException(400,'Укажите ключевые слова.')
        with self.store.db:
            if data.id:
                old=next((r for r in self.store.rules() if r['id']==data.id),None)
                if not old:raise HTTPException(404,'Поиск не найден.')
                for key in ('topic','discovery_queries'):
                    if key in old:value[key]=old[key]
                self.store.db.execute('UPDATE rules SET name=?,data=?,enabled=? WHERE id=?',(data.name.strip(),json.dumps(value),data.enabled,data.id))
            else:
                if len(self.store.rules())>=25:raise HTTPException(400,'Доступно до 25 поисков.')
                self.store.db.execute('INSERT INTO rules(name,data,enabled) VALUES(?,?,?)',(data.name.strip(),json.dumps(value),data.enabled))
        return {'ok':True}

    async def dubai_profile(self,data:DubaiProfile):
        """Explicit owner-only opt-in. Keep existing IDs, leads and old sources."""
        async with self.operation_lock:
            if not data.confirmed:
                raise HTTPException(409,'Подтвердите источники нового личного поиска SignM.')
            ids=list(dict.fromkeys(data.sources))
            valid={s['id'] for s in self.store.rows('SELECT id FROM sources')}
            if not set(ids)<=valid:
                raise HTTPException(400,'Сначала проверьте и добавьте выбранные публичные источники.')
            extra={
                'websites':'сайт\nведение сайта\nweb designer\nwebsite\nweb development',
                'smm':'ведение instagram\nвести instagram\ninstagram\nsocial media\ncontent manager',
                'ads':'директ\nтаргет\nнастраивает рекламу\npaid ads\nppc',
                'automation':'ai automation\nintegration\nинтеграция',
            }
            with self.store.db:
                for rule in self.store.rules():
                    value={k:v for k,v in rule.items() if k not in ('id','name','enabled')}
                    value.update(sources=ids,language='ru_en',lead_profile='signm_dubai_v1',contractor_only=True,demand_only=True,
                        required=[],discovery_queries=['Дубай бизнес чат','Дубай предприниматели','Dubai business community'])
                    value['keywords']=words(rule['keywords']+extra.get(rule.get('topic'),'').splitlines())
                    value['excluded']=[phrase for phrase in rule.get('excluded',[]) if phrase!='наша компания']
                    if 'наша компания предлагает' not in value['excluded']:
                        value['excluded'].append('наша компания предлагает')
                    self.store.db.execute('UPDATE rules SET data=?,enabled=1 WHERE id=?',(json.dumps(value,ensure_ascii=False),rule['id']))
                self.store.db.execute('UPDATE sources SET enabled=0')
                self.store.db.executemany('UPDATE sources SET enabled=1 WHERE id=?',[(sid,) for sid in ids])
                self.store.db.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                    ('signm_dubai_source_ids',json.dumps(ids)))
            return {'ok':True,'sources':ids,'profile':'signm_dubai_v1','other_sources_preserved':True,'messages_sent':0}

    async def rule_toggle(self,rule_id:int,data:Enabled):
        rule=next((r for r in self.store.rules() if r['id']==rule_id),None)
        if not rule:raise HTTPException(404,'Поиск не найден.')
        if data.enabled and not rule['sources']:raise HTTPException(400,'Сначала выберите источники.')
        with self.store.db:self.store.db.execute('UPDATE rules SET enabled=? WHERE id=?',(data.enabled,rule_id))
        return {'ok':True}

    async def assign(self,data:Assignment):
        valid={s['id'] for s in self.store.rows('SELECT id FROM sources WHERE enabled=1')}
        rules={r['id']:r for r in self.store.rules()}
        if not set(data.sources)<=valid or not set(data.rules)<=rules.keys():
            raise HTTPException(400,'Выберите включённые источники и существующие поиски.')
        with self.store.db:
            for rid in data.rules:
                r=rules[rid];r['sources']=list(dict.fromkeys(r['sources']+data.sources))
                self.store.db.execute('UPDATE rules SET data=?,enabled=1 WHERE id=?',
                    (json.dumps({k:v for k,v in r.items() if k not in ('id','name','enabled')}),rid))
        return {'ok':True}

    async def preview(self,data:Preview):
        rule=next((r for r in self.store.rules() if r['id']==data.rule_id),None)
        if not rule:raise HTTPException(404,'Поиск не найден.')
        result=explain(data.text,rule);matched=result['reason']=='matched'
        return dict(matched=matched,keywords=result['keywords'] if matched else [],reason=result['reason'],
                    note='Проверка правил: '+str(result.get('detail') or result['reason'])+'. Сообщение не сохранено и не отправлено.')

    async def running(self,data:Enabled):
        if data.enabled:
            self.bind_account()
            sources={s['id'] for s in self.store.rows('SELECT id FROM sources WHERE enabled=1')}
            if not any(r['enabled'] and sources.intersection(r['sources']) for r in self.store.rules()):
                raise HTTPException(400,'Добавьте источники и включите хотя бы один поиск.')
        self.store.put('running',data.enabled)
        return {'ok':True}

    async def lead_status(self,lead_id:int,data:LeadStatus):
        with self.store.db:
            cur=self.store.db.execute('UPDATE leads SET status=? WHERE id=?',(data.status,lead_id))
            if not cur.rowcount:raise HTTPException(404,'Находка не найдена.')
        return {'ok':True}

    async def export(self):
        stream=io.StringIO(newline='');writer=csv.writer(stream,delimiter=';')
        writer.writerow(['Дата','Источник','Автор','Сообщение','Ссылка','Статус','Тематики'])
        def cell(v):
            s=str(v or '')
            return "'"+s if s.lstrip().startswith(('=','+','-','@')) else s
        for r in self.store.rows('SELECT l.*,s.title FROM leads l LEFT JOIN sources s ON s.id=l.chat_id ORDER BY l.id DESC LIMIT 10000'):
            writer.writerow([cell(r[k]) for k in ('created','title','author','text','link','status')]+[cell(', '.join(m['name'] for m in json.loads(r['matches'])))])
        return Response('\ufeff'+stream.getvalue(),media_type='text/csv',headers={'Content-Disposition':'attachment; filename="signm-marketing-leads.csv"'})

    async def scan(self):
        if not self.store.get('running',False) or not self.transport.is_connected() or self.retry_at()>time.time():return
        if self.history.task and not self.history.task.done():return
        self.bind_account()
        sources=[s for s in self.store.rows('SELECT * FROM sources WHERE enabled=1 ORDER BY id')
                 if any(r['enabled'] and s['id'] in r['sources'] for r in self.store.rules())]
        resume=self.store.get('scan_resume_source')
        start=next((i for i,s in enumerate(sources) if s['id']==resume),0)
        sources=sources[start:]+sources[:start]
        health=self.store.get('scan_health',{})
        cycle=dict(started=now(),finished=None,state='reading',sources_done=0,sources_total=len(sources),
                   checked=0,matched=0,saved=0,duplicate=0,no_keywords=0,no_intent=0,excluded=0,skipped=0,details={})
        health['cycle']=cycle;health['current_source']=None
        self.store.put('scan_health',health)
        for index,source in enumerate(sources):
            if not self.store.get('running',False):
                cycle['state']='stopped';break
            health['current_source']=source['id']
            stats=dict(checked=0,matched=0,saved=0,duplicate=0,no_keywords=0,no_intent=0,excluded=0,skipped=0,details={})
            pending=0;last_id=source['cursor'];last_date=None
            def flush():
                nonlocal pending
                if not pending:return
                with self.store.db:self.store.db.execute('UPDATE sources SET cursor=MAX(cursor,?),messages_checked=messages_checked+?,stats_since=COALESCE(stats_since,?),last_message_at=? WHERE id=?',(last_id,pending,now(),last_date,source['id']))
                pending=0
            try:
                entity=await self.client.get_entity(source['id'])
                if getattr(entity,'creator',False) or getattr(entity,'admin_rights',None) or source['id']==self.get_client_store().get('destination'):
                    with self.store.db:self.store.db.execute('UPDATE sources SET enabled=0,error=? WHERE id=?',('Площадка стала административной или получателем уведомлений.',source['id']))
                    continue
                async for msg in self.client.iter_messages(entity,min_id=source['cursor'],reverse=True,limit=200):
                    if not self.store.get('running',False):
                        cycle['state']='stopped';break
                    if msg.message and not msg.out:
                        # Only resolve authors of matching messages; never enumerate members.
                        relevant=[explain(msg.message,r) for r in self.store.rules() if r['enabled'] and source['id'] in r['sources']]
                        hits=[r for r in relevant if r['keywords']]
                        reason='matched' if any(r['reason']=='matched' for r in hits) else 'no_intent' if any(r['reason']=='no_intent' for r in hits) else 'excluded' if hits else 'no_keywords'
                        if reason=='matched':
                            sender=await msg.get_sender()
                            if not self.account_matches():
                                cycle['state']='account_changed';break
                            author='@'+sender.username if getattr(sender,'username',None) else ' '.join(filter(None,[getattr(sender,'first_name',''),getattr(sender,'last_name','')])) or 'Автор не указан'
                            saved=self.store.ingest(source['id'],msg.id,msg.message,author,msg.sender_id,f'https://t.me/{source["username"]}/{msg.id}',msg.date.isoformat())
                            stats['saved' if saved else 'duplicate']+=1
                        elif hits:
                            detail=next((r.get('detail') for r in hits if r.get('detail')),reason)
                            stats['details'][detail]=stats['details'].get(detail,0)+1
                        stats[reason]+=1
                    else:stats['skipped']+=1
                    stats['checked']+=1;pending+=1;last_id=msg.id;last_date=msg.date.isoformat()
                    # Checkpoint every 20 completed messages. A crash replays at
                    # most one batch; unique message IDs prevent duplicate leads.
                    if pending>=20:flush()
                    # Telethon can yield an entire cached batch without awaiting I/O.
                    # Give HTTP requests a turn while processing a busy source.
                    await asyncio.sleep(0)
                flush()
                if cycle['state']=='reading':
                    with self.store.db:self.store.db.execute('UPDATE sources SET last_scan=?,error=NULL WHERE id=?',(now(),source['id']))
                    cycle['sources_done']+=1
            except errors.FloodWaitError as exc:
                self.defer(exc.seconds)
                self.store.put('scan_resume_source',source['id'])
                cycle['state']='telegram_pause'
                self.store.put('error',f'Telegram просит подождать {exc.seconds} секунд. Поиск продолжится с текущего источника.')
            except Exception as exc:
                detail=exc.detail if isinstance(exc,HTTPException) else f'Не удалось прочитать источник ({type(exc).__name__}).'
                with self.store.db:self.store.db.execute('UPDATE sources SET error=? WHERE id=?',(detail,source['id']))
                if isinstance(exc,HTTPException) and exc.status_code==429:
                    cycle['state']='telegram_pause';self.store.put('scan_resume_source',source['id'])
            finally:
                flush()
                previous=json.loads(source['scan_stats']);totals=previous.get('totals',{})
                for key,value in stats.items():
                    if key=='details':continue
                    totals[key]=totals.get(key,0)+value;cycle[key]+=value
                for key,value in stats['details'].items():cycle['details'][key]=cycle['details'].get(key,0)+value
                with self.store.db:self.store.db.execute('UPDATE sources SET scan_stats=? WHERE id=?',
                    (json.dumps(dict(since=previous.get('since') or now(),checked_at=now(),last=stats,totals=totals,capped=stats['checked']>=200)),source['id']))
                self.store.put('scan_health',health)
            if cycle['state']!='reading':break
            self.store.put('scan_resume_source',sources[index+1]['id'] if index+1<len(sources) else None)
            await asyncio.sleep(self.scan_pause)
        health['current_source']=None;cycle['finished']=now()
        if cycle['state']=='reading':
            cycle['state']='partial' if any(s['error'] for s in self.store.rows('SELECT error FROM sources WHERE enabled=1')) else 'completed'
            if cycle['state']=='completed':health['last_cycle_completed']=cycle['finished'];self.store.put('error',None)
        self.store.put('scan_health',health)

    async def loop(self):
        while True:
            try:
                await self.scan()
                if self.pipeline.settings()['auto_templates']:
                    await self.pipeline.prepare(Prepare(limit=10))
            except Exception as exc:self.store.put('error',f'Поиск временно недоступен ({type(exc).__name__}).')
            await asyncio.sleep(90)
