"""Finite draft campaigns and explicit exclusions, using the existing queue.

No timer, account connection, import or outbound action is started by this module.
The wizard does read-only recipient preflight only after an explicit save.
"""
import asyncio
import csv
import hashlib
import io
import json
import re
import secrets
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, model_validator

from engine import now
from operation_topics import DestinationTopic, validate_topic_routes


class StrictSpec(BaseModel):
    model_config = {'extra': 'forbid'}

    @model_validator(mode='after')
    def utf8(self):
        try:
            json.dumps(self.model_dump(mode='json'),ensure_ascii=False).encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('Текст содержит неподдерживаемый символ.') from None
        return self


class ExclusionSpec(StrictSpec):
    request_key: str = Field(min_length=16,max_length=80)
    subject: Literal['user','chat']
    identifier: str = Field(min_length=1,max_length=80)
    reason: str = Field(min_length=3,max_length=500)
    account: str | None = Field(default=None,max_length=80)
    telegram_id: int | None = Field(default=None,gt=0,strict=True)

    @model_validator(mode='after')
    def normalize(self):
        self.identifier=self.identifier.strip().lstrip('@').lower()
        self.reason=self.reason.strip()
        if len(self.reason)<3:raise ValueError('Укажите причину исключения.')
        if re.fullmatch(r'-?\d+',self.identifier):
            number=int(self.identifier)
            if not -(2**53)<number<2**53 or number==0 or (self.subject=='user' and number<0) or (self.subject=='chat' and number>0):
                raise ValueError('ID пользователя положительный, ID группы или канала отрицательный.')
            self.identifier=str(number)
        elif not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',self.identifier):
            raise ValueError('Укажите числовой ID или @username, без ссылки и телефона.')
        if (self.account is None)!=(self.telegram_id is None):
            raise ValueError('Для области аккаунта нужны аккаунт и его Telegram ID.')
        return self


class DisableExclusion(StrictSpec):
    revision: int = Field(gt=0,strict=True)
    confirmed: Literal[True]


class PlanSpec(StrictSpec):
    request_key: str = Field(min_length=16,max_length=80)
    account: str = Field(min_length=1,max_length=80)
    telegram_id: int = Field(gt=0,strict=True)
    kind: Literal['post','message','invite']
    title: str = Field(min_length=1,max_length=100)
    destinations: list[int] = Field(default_factory=list,max_length=30)
    destination_topics: list[DestinationTopic] = Field(default_factory=list,max_length=30)
    users: list[str] = Field(default_factory=list,max_length=50)
    text: str = Field(default='',max_length=4096)
    media: list[str] = Field(default_factory=list,max_length=10)
    permission_note: str = Field(default='',max_length=500)
    consent_note: str = Field(default='',max_length=500)
    delay_seconds: int = Field(default=60,ge=30,le=3600,strict=True)
    daily_limit: int = Field(default=20,ge=1,le=100,strict=True)
    campaign_id: int | None = Field(default=None,gt=0,strict=True)
    campaign_revision: int | None = Field(default=None,gt=0,strict=True)
    timezone: str = Field(default='Asia/Dubai',max_length=80)
    start_date: date
    end_date: date
    times: list[str] = Field(min_length=1,max_length=3)

    @model_validator(mode='after')
    def valid(self):
        validate_topic_routes(self.destination_topics,self.destinations,self.kind)
        self.title=self.title.strip()
        if not self.title:raise ValueError('Укажите название кампании.')
        try:ZoneInfo(self.timezone)
        except (KeyError,ValueError):raise ValueError('Выберите действующий часовой пояс IANA.') from None
        if not 0<=(self.end_date-self.start_date).days<14:raise ValueError('Кампания длится от 1 до 14 дней.')
        if len(set(self.times))!=len(self.times) or any(not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',v) for v in self.times):
            raise ValueError('Время указывается без повторов в формате ЧЧ:ММ.')
        self.times=sorted(self.times)
        if len(set(self.destinations))!=len(self.destinations) or any(type(i) is not int or not -(2**53)<i<0 for i in self.destinations):
            raise ValueError('Укажите уникальные отрицательные ID площадок.')
        self.users=list(dict.fromkeys(u.strip().lstrip('@').lower() for u in self.users))
        if any(not re.fullmatch(r'[a-z][a-z0-9_]{3,31}',u) for u in self.users):raise ValueError('Список получателей: только @username.')
        if (self.campaign_id is None)!=(self.campaign_revision is None):raise ValueError('Нужны ID и версия профиля.')
        if self.campaign_id is not None and self.kind!='post':raise ValueError('Профиль публикации используется только для публикаций.')
        if self.kind=='message' and self.destinations:raise ValueError('Личная рассылка не требует площадки.')
        if self.kind!='message' and not self.destinations:raise ValueError('Выберите площадку.')
        if self.kind=='invite' and len(self.destinations)!=1:raise ValueError('Для приглашений выберите одну группу.')
        if self.kind=='post' and self.campaign_id is None:raise ValueError('Выберите сохранённый профиль публикации.')
        if self.kind=='post' and self.users:raise ValueError('Для публикации не нужен список пользователей.')
        if self.kind!='post' and (not self.users or len(self.consent_note.strip())<8):raise ValueError('Укажите получателей и основание согласия.')
        if self.kind=='invite' and (self.text or self.media):raise ValueError('Приглашение не содержит публикацию.')
        if self.kind=='message' and (self.media or not self.text.strip()):raise ValueError('Личная рассылка содержит только текст.')
        if self.kind=='post' and not (self.text.strip() or self.media):raise ValueError('Добавьте текст или фото публикации.')
        if len(self.text.encode('utf-16-le'))//2>(1024 if self.media else 4096):raise ValueError('Слишком длинный текст.')
        return self


class WorkbenchStopped(Exception):
    def __init__(self,result):self.result=result


class ExclusionStopped(WorkbenchStopped):
    def __init__(self,row):
        self.result=dict(state='excluded',error='Исключение #%s: %s'%(row['id'],row['reason']),
                         diagnosis=dict(source='exclusion',exclusion_id=row['id'],code='explicit_exclusion'))


class OperationWorkbench:
    def __init__(self,ops):
        self.ops,self.store=ops,ops.store
        self.router=APIRouter()
        for path,fn,methods in [('/exclusions',self.exclusions,['GET']),('/exclusions',self.add_exclusion,['POST']),
            ('/exclusions/{eid}/disable',self.disable_exclusion,['POST']),('/plans',self.plans,['GET']),
            ('/plans/preview',self.preview,['POST']),('/plans',self.save_plan,['POST']),
            ('/plans/{pid}',self.plan_detail,['GET']),('/plans/{pid}/report',self.report,['GET']),
            ('/accounts/{aid}/readiness',self.readiness,['GET'])]:
            self.router.add_api_route(path,fn,methods=methods)

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_exclusions(id INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,identifier TEXT NOT NULL,reason TEXT NOT NULL,account TEXT,telegram_id INTEGER,
          active INTEGER NOT NULL DEFAULT 1,revision INTEGER NOT NULL DEFAULT 1,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS op_exclusions_lookup ON op_exclusions(active,subject,identifier);
        CREATE TABLE IF NOT EXISTS op_plans(id INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,
          account TEXT NOT NULL,telegram_id INTEGER NOT NULL,title TEXT NOT NULL,spec TEXT NOT NULL,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS op_plan_jobs(plan_id INTEGER NOT NULL REFERENCES op_plans(id),
          job_id INTEGER UNIQUE NOT NULL REFERENCES op_jobs(id),slot INTEGER NOT NULL,PRIMARY KEY(plan_id,slot));
        ''')

    async def exclusions(self):
        return {'items':self.store.rows('SELECT * FROM op_exclusions ORDER BY active DESC,id DESC LIMIT 1000')}

    def identity(self,aid,tid):
        if self.ops.account_row(aid)['telegram_id']!=tid:raise HTTPException(409,'Личность аккаунта изменилась. Обновите панель.')

    async def add_exclusion(self,spec:ExclusionSpec):
        async with self.ops.lock:
            if spec.account:self.identity(spec.account,spec.telegram_id)
            values=spec.model_dump()
            rows=self.store.rows('SELECT * FROM op_exclusions WHERE request_key=?',(spec.request_key,))
            if rows:
                if any(rows[0][k]!=v for k,v in values.items()):raise HTTPException(409,'Ключ уже использован для другого исключения.')
                return rows[0]
            with self.store.db:
                cur=self.store.db.execute('INSERT INTO op_exclusions(request_key,subject,identifier,reason,account,telegram_id,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                    (spec.request_key,spec.subject,spec.identifier,spec.reason,spec.account,spec.telegram_id,now(),now()))
            return self.store.rows('SELECT * FROM op_exclusions WHERE id=?',(cur.lastrowid,))[0]

    async def disable_exclusion(self,eid:int,spec:DisableExclusion):
        async with self.ops.lock:
            rows=self.store.rows('SELECT * FROM op_exclusions WHERE id=?',(eid,))
            if not rows:raise HTTPException(404,'Исключение не найдено.')
            row=rows[0]
            if not row['active'] and row['revision']==spec.revision+1:return row
            if row['revision']!=spec.revision:raise HTTPException(409,'Исключение изменилось. Обновите список.')
            with self.store.db:self.store.db.execute('UPDATE op_exclusions SET active=0,revision=revision+1,updated=? WHERE id=?',(now(),eid))
            return self.store.rows('SELECT * FROM op_exclusions WHERE id=?',(eid,))[0]

    def match(self,aid,tid,chat_id,chat_username,user_id=None,user_name=None):
        candidates=[]
        if chat_id and chat_id<0:candidates.append(('chat',str(chat_id)))
        if chat_id and chat_id<0 and chat_username:candidates.append(('chat',str(chat_username).lstrip('@').lower()))
        if user_id:candidates.append(('user',str(user_id)))
        if user_name:candidates.append(('user',str(user_name).lstrip('@').lower()))
        for subject,identifier in candidates:
            rows=self.store.rows('SELECT * FROM op_exclusions WHERE active=1 AND subject=? AND identifier=? AND (account IS NULL OR (account=? AND telegram_id=?)) ORDER BY id LIMIT 1',
                                  (subject,identifier,aid,tid))
            if rows:return rows[0]
        return None

    def guard(self,a,j,item,entity=None,user=None):
        window=self.window_stop(j)
        if window:raise WorkbenchStopped(window)
        # Check saved identifiers AND current usernames so renaming does not bypass an ID exclusion.
        for cid,cname,uid,uname in [(item['chat_id'],item.get('chat_username'),item.get('user_id'),item.get('user_name')),
            (item['chat_id'],getattr(entity,'username',None),item.get('user_id'),getattr(user,'username',None))]:
            row=self.match(j['account'],j['telegram_id'],cid,cname,uid,uname)
            if row:raise ExclusionStopped(row)

    def window_stop(self,job):
        # Only jobs created by this wizard have this finite two-hour deadline.
        # The original scheduled_at remains immutable if a daily budget defers not_before.
        if not self.store.rows('SELECT 1 FROM op_plan_jobs WHERE job_id=?',(job['id'],)):return None
        start=datetime.fromisoformat(job['spec']['scheduled_at'])
        if time.time()>=start.timestamp()+7200:
            return dict(state='cancelled',error='Окно кампании завершилось. Действие не отправлено.',
                        diagnosis=dict(source='campaign_window',code='window_expired'))
        return None

    def slots(self,spec):
        zone=ZoneInfo(spec.timezone);slots=[]
        for offset in range((spec.end_date-spec.start_date).days+1):
            day=spec.start_date+timedelta(days=offset)
            for clock in spec.times:
                local=datetime.fromisoformat(day.isoformat()+'T'+clock)
                aware=local.replace(tzinfo=zone)
                utc=aware.astimezone(timezone.utc)
                if utc.astimezone(zone).replace(tzinfo=None)!=local or aware.utcoffset()!=local.replace(tzinfo=zone,fold=1).utcoffset():
                    raise HTTPException(400,'Время неоднозначно или отсутствует при переводе часов. Выберите другой час.')
                if not time.time()+60<utc.timestamp()<time.time()+365*86400:raise HTTPException(400,'Все окна должны быть в будущем, в пределах года.')
                slots.append(utc)
        if len(slots)>30:raise HTTPException(400,'В кампании максимум 30 окон.')
        if spec.kind=='post' and any((b-a).total_seconds()<14400 for a,b in zip(slots,slots[1:])):
            raise HTTPException(400,'Между окнами публикации требуется не менее четырёх часов.')
        if spec.kind!='post' and len(slots)>len(spec.users):raise HTTPException(400,'Окон больше, чем уникальных получателей. Сократите календарь.')
        return slots

    async def preview(self,spec:PlanSpec):
        self.identity(spec.account,spec.telegram_id)
        slots=self.slots(spec)
        known=[]
        for cid in spec.destinations:
            row=self.match(spec.account,spec.telegram_id,cid,None)
            if row:known.append(dict(subject='chat',identifier=str(cid),reason=row['reason']))
        for username in spec.users:
            row=self.match(spec.account,spec.telegram_id,0,None,None,username)
            if row:known.append(dict(subject='user',identifier=username,reason=row['reason']))
        counts=[len(spec.destinations)]*len(slots) if spec.kind=='post' else [len(spec.users[i::len(slots)]) for i in range(len(slots))]
        return dict(**(dict(destination_topics=[r.model_dump() for r in spec.destination_topics]) if spec.destination_topics else {}),slots=[dict(at=s.isoformat(),local=s.astimezone(ZoneInfo(spec.timezone)).isoformat(),items=counts[i]) for i,s in enumerate(slots)],
                    total_items=sum(counts),known_exclusions=known,save_state='draft',
                    note='Предпросмотр не обращается к Telegram. При сохранении проверяются получатели и права; отправка не запускается. Получатели личных сообщений и приглашений распределяются по окнам однократно.')

    @staticmethod
    def digest(spec):return hashlib.sha256(json.dumps(spec,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

    async def save_plan(self,spec:PlanSpec):
        # Local import avoids a module cycle; use precisely the queue's validation contract.
        from operations import JobSpec
        raw=spec.model_dump(mode='json')
        if not spec.destination_topics:raw.pop('destination_topics',None)
        digest=self.digest(raw)
        existing=self.store.rows('SELECT id,digest FROM op_plans WHERE request_key=?',(spec.request_key,))
        if existing:
            if existing[0]['digest']!=digest:raise HTTPException(409,'Ключ уже принадлежит другой кампании.')
            return await self.plan_detail(existing[0]['id'])
        self.identity(spec.account,spec.telegram_id)
        slots=self.slots(spec)
        base={k:raw[k] for k in ('account','kind','title','destinations','users','text','media','permission_note','consent_note','delay_seconds','daily_limit','campaign_id','campaign_revision')}
        if spec.destination_topics:base['destination_topics']=raw['destination_topics']
        base.update(request_key='plan-preflight-'+hashlib.sha256(spec.request_key.encode()).hexdigest()[:40],scheduled_at=slots[0])
        if spec.destination_topics:
            base['posting_policy']=dict(window_expires_at=(slots[0]+timedelta(hours=2)).isoformat(),campaign_expires_at=(slots[-1]+timedelta(hours=2)).isoformat(),timezone=spec.timezone,
                destinations=[dict(chat_id=cid,max_per_day=len(spec.times),min_interval_seconds=14400) for cid in spec.destinations])
        try:
            async with asyncio.timeout(45):prepared=await self.ops.prepare_job(JobSpec(**base),prepare_only=True)
        except TimeoutError:raise HTTPException(504,'Telegram не завершил проверку. Кампания не создана, отправки не было.') from None
        async with self.ops.lock:
            self.identity(spec.account,spec.telegram_id)
            current=self.ops.online(spec.account)
            if current.client is not prepared['client'] or current.me.id!=spec.telegram_id:raise HTTPException(409,'Отправитель изменился при проверке.')
            existing=self.store.rows('SELECT id,digest FROM op_plans WHERE request_key=?',(spec.request_key,))
            if existing:
                if existing[0]['digest']!=digest:raise HTTPException(409,'Ключ уже использован.')
                return await self.plan_detail(existing[0]['id'])
            if spec.campaign_id is not None:self.ops.campaigns.snapshot(spec.campaign_id,spec.campaign_revision)
            resolved=prepared['resolved']
            if spec.kind!='post':
                seen=set();resolved=[pair for pair in resolved if not (pair[1].id in seen or seen.add(pair[1].id))]
                if len(slots)>len(resolved):raise HTTPException(400,'После проверки уникальных получателей меньше, чем окон.')
            stamp=now()
            with self.store.db:
                cur=self.store.db.execute('INSERT INTO op_plans(request_key,digest,account,telegram_id,title,spec,created) VALUES(?,?,?,?,?,?,?)',
                    (spec.request_key,digest,spec.account,spec.telegram_id,spec.title,json.dumps(raw,ensure_ascii=False),stamp));pid=cur.lastrowid
                for index,slot in enumerate(slots):
                    pairs=resolved if spec.kind=='post' else resolved[index::len(slots)]
                    payload=dict(prepared['payload']);payload.update(request_key='plan-job-'+hashlib.sha256((spec.request_key+':'+str(index)).encode()).hexdigest(),scheduled_at=slot.isoformat())
                    if spec.kind!='post':payload['users']=[u.username for _,u in pairs]
                    if spec.kind=='post':
                        payload['posting_policy']=dict(window_expires_at=(slot+timedelta(hours=2)).isoformat(),campaign_expires_at=(slots[-1]+timedelta(hours=2)).isoformat(),timezone=spec.timezone,
                            destinations=[dict(chat_id=cid,max_per_day=len(spec.times),min_interval_seconds=14400) for cid in spec.destinations])
                    # Validate the complete immutable snapshot before any row can be committed.
                    JobSpec(**{k:v for k,v in payload.items() if k not in ('campaign_snapshot','topic_snapshots')})
                    cur=self.store.db.execute('INSERT INTO op_jobs(request_key,digest,account,telegram_id,kind,title,spec,state,not_before,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                        (payload['request_key'],self.digest(payload),spec.account,spec.telegram_id,spec.kind,spec.title,json.dumps(payload,ensure_ascii=False),'draft',slot.timestamp(),stamp,stamp));jid=cur.lastrowid
                    self.store.db.execute('INSERT INTO op_plan_jobs VALUES(?,?,?)',(pid,jid,index))
                    for d,u in pairs:
                        exclusion=self.match(spec.account,spec.telegram_id,d['id'],d.get('username'),u.id if u else None,u.username if u else None)
                        self.store.db.execute('INSERT INTO op_items(job,chat_id,chat_title,chat_username,user_id,user_name,state,random_ids,user_access_hash,error) VALUES(?,?,?,?,?,?,?,?,?,?)',
                            (jid,d['id'],d['title'],d.get('username'),u.id if u else None,u.username if u else None,'excluded' if exclusion else 'pending',json.dumps([secrets.randbits(63) for _ in range(max(1,len(spec.media)))]),u.access_hash if u else None,'Исключение #%s: %s'%(exclusion['id'],exclusion['reason']) if exclusion else None))
            return await self.plan_detail(pid)

    async def plans(self):
        return dict(items=self.store.rows('SELECT id,title,account,telegram_id,created FROM op_plans ORDER BY id DESC LIMIT 100'))

    async def plan_detail(self,pid:int):
        rows=self.store.rows('SELECT * FROM op_plans WHERE id=?',(pid,))
        if not rows:raise HTTPException(404,'Кампания не найдена.')
        result=rows[0];result['spec']=json.loads(result['spec'])
        jobs=self.store.rows('SELECT j.id,j.state,j.kind,j.not_before,j.title FROM op_jobs j JOIN op_plan_jobs p ON p.job_id=j.id WHERE p.plan_id=? ORDER BY p.slot',(pid,))
        items=self.store.rows('SELECT i.*,j.kind FROM op_items i JOIN op_plan_jobs p ON p.job_id=i.job JOIN op_jobs j ON j.id=i.job WHERE p.plan_id=? ORDER BY i.id',(pid,))
        items=[{k:v for k,v in item.items() if k not in ('random_ids','user_access_hash')} for item in items]
        if result['spec'].get('destination_topics'):
            snapshots={}
            for job in self.store.rows('SELECT j.* FROM op_jobs j JOIN op_plan_jobs p ON p.job_id=j.id WHERE p.plan_id=? ORDER BY p.slot',(pid,)):
                job['spec']=json.loads(job['spec'])
                for item in (i for i in items if i['job']==job['id']):
                    route=self.ops.topics.saved(job,item)
                    if route:
                        item.update(topic_id=route['topic_id'],topic_title=route['title'])
                        snapshots.setdefault((route['chat_id'],route['topic_id']),route)
            result['topic_snapshots']=list(snapshots.values())
        presence=self.store.rows('SELECT t.presence,COUNT(*) count FROM op_tracking_publications t JOIN op_plan_jobs p ON p.job_id=t.job_id WHERE p.plan_id=? GROUP BY t.presence',(pid,))
        replies=self.store.rows("SELECT COUNT(*) count FROM op_tracking_events e JOIN op_plan_jobs p ON p.job_id=e.job_id WHERE p.plan_id=? AND e.kind='group_reply'",(pid,))[0]['count']
        return dict(**result,jobs=jobs,counts=dict(Counter(i['state'] for i in items)),items=items,presence={r['presence']:r['count'] for r in presence},group_replies=replies,
                    note='Отправлено не означает прочитано или заявка. Уже состоявшие участники не считаются новыми. Личные ответы не приписываются кампании без подтверждённой связи. Все задания создаются черновиками; запуск каждого подтверждается отдельно в журнале.')

    async def report(self,pid:int):
        plan=await self.plan_detail(pid);out=io.StringIO();writer=csv.writer(out,delimiter=';')
        has_topics=bool(plan['spec'].get('destination_topics'))
        writer.writerow(['Кампания','Задание','Площадка']+(['ID темы','Тема'] if has_topics else [])+['Пользователь','Статус','Ошибка','ID сообщений','Завершено'])
        def safe(value):
            value=str(value or '')
            return "'"+value if value.lstrip().startswith(('=','+','-','@')) else value
        for item in plan['items']:
            values=[plan['title'],item['job'],item['chat_id']]+([item.get('topic_id'),item.get('topic_title')] if has_topics else [])+[item['user_name'] or item['user_id'],item['state'],item['error'],item['message_ids'],item['finished_at']]
            writer.writerow([safe(v) for v in values])
        return Response('\ufeff'+out.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':f'attachment; filename="campaign-{pid}.csv"'})

    async def readiness(self,aid:str):
        row=self.ops.account_row(aid);a=self.ops.accounts.get(aid)
        connected=bool(a and a.client and a.client.is_connected())
        identity_ok=bool(connected and a.me and a.me.id==row['telegram_id'])
        cooldown=max(0,int(row['not_before']-time.time()))
        counts=self.store.rows('SELECT state,COUNT(*) count FROM op_jobs WHERE account=? GROUP BY state',(aid,))
        proxy=self.ops.proxy_public(aid)
        return dict(account=aid,telegram_id=row['telegram_id'],connected=connected,identity_confirmed=identity_ok,
                    outbound_enabled=bool(row['enabled']),cooldown_seconds=cooldown,
                    readiness='ready' if identity_ok and row['enabled'] and not cooldown else 'blocked',
                    proxy={k:proxy[k] for k in ('mode','address','port','password_saved') if k in proxy},
                    jobs={r['state']:r['count'] for r in counts},
                    budget=self.ops.budgets.snapshot(aid) if row['telegram_id'] else None,checked_at=now(),
                    note='Состояние панели по сохранённым данным. Это не гарантия разрешения Telegram на рассылку или приглашение. Ничего не запускает и не переподключает.')
