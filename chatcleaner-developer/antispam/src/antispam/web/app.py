"""Single-process panel API and Telegram polling, sharing the existing moderation engine."""
import asyncio
import contextlib
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import time
import traceback
from contextlib import asynccontextmanager
from typing import Literal

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter, TelegramConflictError
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from antispam.config import Settings
from antispam.domain.models import MessageContext
from antispam.storage.repositories import Store
from antispam.telegram.handlers import Controller, env_file

os.umask(0o077)
ORIGIN=os.environ.get('PANEL_ORIGIN','http://127.0.0.1:8080')
ctrl=None
health={'connected':False,'username':None,'last_poll':None,'error':None}
log=logging.getLogger(__name__)

def log_poll_failure(exc):
    # Log code locations only: exception values can contain tokens or message text.
    frames=' > '.join(f'{Path(f.filename).name}:{f.lineno}:{f.name}' for f in traceback.extract_tb(exc.__traceback__))
    log.error('Telegram update loop failed: %s [%s]',type(exc).__name__,frames)

def fail(text,code=400): raise HTTPException(code,text)
def require_chat(cid):
    chat=ctrl.store.chat(cid)
    if not chat: fail('Площадка не найдена.',404)
    return chat

def actor():
    ids=ctrl.owners()
    if not ids: fail('Не настроен владелец бота.',409)
    return min(ids)

def audit(request,cid,operation,details):
    ctrl.store.audit(actor(),cid,'panel:'+operation,{**details,'panel_user':request.headers.get('x-panel-user')})

async def process_updates(dp,updates):
    for update in updates:
        await dp.feed_update(ctrl.bot,update)
        ctrl.store.set_setting('panel_update_offset',update.update_id+1)
        # Handlers can complete synchronously (cached rights + SQLite). Yield
        # between updates so a full Telegram batch cannot starve HTTP requests.
        await asyncio.sleep(0)

async def polling(dp):
    while True:
        try:
            me=await ctrl.bot.get_me()
            info=await ctrl.bot.get_webhook_info()
            if info.url:
                health.update(connected=False,error='У бота настроен webhook другой установки. Остановите её перед подключением.')
                await asyncio.sleep(30);continue
            health['username']=me.username
            updates=await ctrl.bot.get_updates(offset=ctrl.store.setting('panel_update_offset',0),timeout=20,
                allowed_updates=['message','edited_message','channel_post','edited_channel_post','my_chat_member','chat_member','callback_query'])
            health.update(connected=True,last_poll=int(time.time()),error=None)
            await process_updates(dp,updates)
        except TelegramConflictError:
            health.update(connected=False,error='Другой экземпляр бота забирает сообщения. Остановите его.')
            await asyncio.sleep(15)
        except TelegramRetryAfter as exc:
            health.update(connected=False,error=f'Telegram просит подождать {exc.retry_after} секунд.')
            await asyncio.sleep(exc.retry_after+1)
        except asyncio.CancelledError: raise
        except Exception as exc:
            log_poll_failure(exc)
            health.update(connected=False,error='Ошибка соединения или обработки: '+type(exc).__name__)
            await asyncio.sleep(5)

async def maintenance():
    while True:
        ctrl.store.cleanup()
        day=time.strftime('%Y-%m-%d',time.gmtime())
        if ctrl.store.setting('panel_backup_day')!=day:
            folder=Path(os.environ.get('BACKUP_DIR','data/backups'));folder.mkdir(parents=True,exist_ok=True)
            ctrl.store.backup(folder/f'antispam-{day}.sqlite3')
            for old in sorted(folder.glob('antispam-*.sqlite3'))[:-7]:old.unlink()
            ctrl.store.set_setting('panel_backup_day',day)
        await asyncio.sleep(300)

@asynccontextmanager
async def lifespan(app):
    global ctrl
    settings=Settings.from_env({**env_file(),**os.environ})
    settings.database_path.parent.mkdir(parents=True,exist_ok=True)
    lock=open(settings.database_path.with_suffix('.lock'),'a+')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    policy_path=settings.policy_path if settings.policy_path.exists() else Path('config/policy.example.json')
    store=Store(settings.database_path)
    bot=Bot(settings.bot_token,default=DefaultBotProperties(parse_mode='HTML'))
    ctrl=Controller(bot,store,settings,json.loads(policy_path.read_text()))
    dp=Dispatcher();dp.include_router(ctrl.router)
    tasks=[asyncio.create_task(polling(dp)),asyncio.create_task(ctrl.executor.retry_loop()),asyncio.create_task(maintenance())]
    try: yield
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await bot.session.close();store.close();lock.close()

app=FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None,openapi_url=None)

@app.middleware('http')
async def protect(request:Request,call_next):
    if not request.headers.get('x-panel-user'):return JSONResponse({'detail':'Требуется вход в панель.'},status_code=401)
    if request.method not in ('GET','HEAD') and (request.headers.get('origin')!=ORIGIN or request.headers.get('x-panel-request')!='1'):
        return JSONResponse({'detail':'Откройте панель заново.'},status_code=403)
    response=await call_next(request);response.headers['Cache-Control']='no-store';return response

@app.exception_handler(RequestValidationError)
async def invalid(request,exc):return JSONResponse({'detail':'Проверьте формат и заполнение полей.'},status_code=422)
@app.exception_handler(TelegramAPIError)
async def telegram_error(request,exc):
    return JSONResponse({'detail':'Telegram отклонил запрос. Проверьте ID площадки и административные права бота. Тип: '+type(exc).__name__},status_code=400)
@app.exception_handler(Exception)
async def internal_error(request,exc):return JSONResponse({'detail':'Действие не выполнено: '+type(exc).__name__},status_code=500)

async def inspect_chat(reference):
    info=await ctrl.bot.get_chat(reference)
    if info.type not in ('group','supergroup','channel'):fail('Добавьте группу или канал, а не личную переписку.')
    me=await ctrl.bot.get_me();rights=await ctrl.bot.get_chat_member(info.id,me.id)
    admin=rights.status in ('administrator','creator')
    result={'type':str(info.type),'username':info.username,'linked_chat_id':getattr(info,'linked_chat_id',None),
        'admin':admin,'delete':admin and bool(getattr(rights,'can_delete_messages',False) or rights.status=='creator'),
        'restrict':admin and bool(getattr(rights,'can_restrict_members',False) or rights.status=='creator'), 'checked_at':int(time.time())}
    ctrl.store.register_chat(info.id,info.title or str(info.id));ctrl.store.set_setting(f'rights:{info.id}',result)
    return info.id,result

@app.get('/api/antispam/state')
async def state():
    s=ctrl.store;chats=[]
    for c in s.chats():
        c.update(rights=s.setting(f'rights:{c["chat_id"]}',{}),autoban=s.setting(f'autoban:{c["chat_id"]}',False),
            quarantine_auto=s.setting(f'quarantine_auto:{c["chat_id"]}',False),
            advertising=s.setting(f'ad_enabled:{c["chat_id"]}',s.setting('ad_enabled:0',s.setting('advertising:0',{}).get('enabled',False))),behavior_auto=s.setting(f'behavior_auto:{c["chat_id"]}',False),
            last_message=s.setting(f'last_message:{c["chat_id"]}'),contact=s.setting(f'ad_contact:{c["chat_id"]}',s.setting('ad_contact:0','')),behavior=s.setting(f'behavior:{c["chat_id"]}',s.setting('behavior:0',ctrl.policy['behavior'])))
        chats.append(c)
    counts={r['actual_result']:r['n'] for r in s.rows('SELECT actual_result,COUNT(*) n FROM moderation_events GROUP BY actual_result')}
    stats={'checked':sum(counts.values()),'allowed':counts.get('allowed',0)+counts.get('kept',0),
        'deleted':counts.get('delete',0),'restricted':counts.get('restrict',0),'banned':counts.get('ban',0),
        'observed':counts.get('observed',0),'review':counts.get('review',0),
        'failed':sum(v for k,v in counts.items() if k.startswith('failed:')),
        'reviewed':s.one("SELECT COUNT(*) n FROM review_queue WHERE status='resolved'")['n']}
    return {'bot':health,'chats':chats,'stats':stats,'rule_count':s.one('SELECT COUNT(*) n FROM rules WHERE enabled=1')['n'],
        'pending':s.one("SELECT COUNT(*) n FROM review_queue WHERE status='pending'")['n'],
        'pending_jobs':s.one("SELECT COUNT(*) n FROM action_jobs WHERE status IN ('pending','running')")['n']}

@app.get('/api/antispam/config/export')
async def export_config():
    return JSONResponse(ctrl.store.export_config(),headers={'Content-Disposition':'attachment; filename="chatcleaner-antispam-config.json"'})

class Source(BaseModel):reference:str=Field(min_length=1,max_length=150)
@app.post('/api/antispam/chats')
async def add_chat(data:Source,request:Request):
    ref=data.reference.strip().removeprefix('https://t.me/').rstrip('/')
    if not re.fullmatch(r'-\d+|@?[A-Za-z][A-Za-z0-9_]{3,31}',ref):fail('Укажите @username, ссылку t.me/username или отрицательный ID чата.')
    cid,rights=await inspect_chat(int(ref) if ref.startswith('-') else '@'+ref.lstrip('@'))
    audit(request,cid,'chat_added',{});return {'id':cid,'rights':rights}
@app.post('/api/antispam/chats/{cid}/check')
async def check_chat(cid:int):require_chat(cid);return {'rights':(await inspect_chat(cid))[1]}

class ChatSettings(BaseModel):
    enabled:bool
    mode:Literal['observe','active']
    autoban:bool=False
    # `None` distinguishes a legacy client that omits this new field from an
    # explicit opt-out; omitted values preserve the current stored setting.
    quarantine_auto:bool|None=None
    advertising:bool=False
    behavior_auto:bool=False
    contact:str=Field(default='',max_length=200)
    review_at_messages:int=Field(default=5,ge=2,le=100)
    review_at_duplicates:int=Field(default=3,ge=2,le=30)
    review_at_distinct_chats:int=Field(default=3,ge=2,le=30)

@app.post('/api/antispam/chats/{cid}/settings')
async def settings(cid:int,data:ChatSettings,request:Request):
    require_chat(cid)
    if data.enabled:
        _,rights=await inspect_chat(cid)
        if not rights['admin']:fail('Назначьте бота администратором площадки.')
        if data.mode=='active' and not rights['delete']:fail('Выдайте боту право удаления сообщений.')
        if data.mode=='active' and data.autoban and not rights['restrict']:fail('Для автобана нужно право блокировки участников.')
        quarantine_auto=(data.quarantine_auto if data.quarantine_auto is not None
                         else ctrl.store.setting(f'quarantine_auto:{cid}',False))
        if data.mode=='active' and quarantine_auto and not rights['restrict']:fail('Для карантина нужно право ограничивать участников.')
        if rights['type']=='channel' and (data.autoban or data.behavior_auto):fail('Для публикаций канала доступны фильтрация и удаление; автобан и флуд настраиваются в группе обсуждений.')
    s=ctrl.store
    async with ctrl.executor.lock:
        with s.db:
            s.db.execute('UPDATE chats SET enabled=?,mode=?,policy_version=policy_version+1 WHERE chat_id=?',(data.enabled,data.mode,cid))
            values={f'autoban:{cid}':data.autoban,f'quarantine_auto:{cid}':quarantine_auto,f'ad_enabled:{cid}':data.advertising,f'behavior_auto:{cid}':data.behavior_auto,f'ad_contact:{cid}':data.contact.strip()}
            behavior={**ctrl.policy['behavior'],**s.setting(f'behavior:{cid}',{}),**{k:getattr(data,k) for k in ('review_at_messages','review_at_duplicates','review_at_distinct_chats')}}
            values[f'behavior:{cid}']=behavior
            for k,v in values.items():s.db.execute('INSERT OR REPLACE INTO runtime_settings VALUES(?,?)',(k,json.dumps(v)))
        audit(request,cid,'settings',data.model_dump())
    return {'ok':True}

@app.get('/api/antispam/rules')
async def rules(cid:int=0):
    return {'rules':ctrl.store.rows('SELECT * FROM rules WHERE chat_id IS NULL OR chat_id=? ORDER BY id DESC',(cid,)),
        'exceptions':ctrl.store.exceptions(cid),'users':ctrl.store.rows('SELECT * FROM user_policies WHERE chat_id IS NULL OR chat_id=? ORDER BY id DESC',(cid,))}

class Rule(BaseModel):
    chat_id:int=0
    id:int|None=None
    kind:Literal['word','phrase','domain','url']='word'
    pattern:str=Field(min_length=1,max_length=300)
    action:Literal['review','delete','restrict','ban']='delete'
    enabled:bool=True
@app.post('/api/antispam/rules')
async def rule(data:Rule,request:Request):
    if data.chat_id:require_chat(data.chat_id)
    if not data.pattern.strip():fail('Введите слово или фразу.')
    s=ctrl.store
    if data.id:
        old=s.one('SELECT * FROM rules WHERE id=?',(data.id,))
        if not old:fail('Правило не найдено.',404)
        if (old['chat_id'] or 0)!=data.chat_id:fail('Область правила нельзя менять при редактировании.')
        s.write('UPDATE rules SET kind=?,pattern=?,action=?,enabled=? WHERE id=?',(data.kind,data.pattern.strip(),data.action,data.enabled,data.id))
    else:s.write('INSERT INTO rules(chat_id,kind,pattern,action,enabled,created_by,created_at) VALUES(?,?,?,?,?,?,?)',(data.chat_id or None,data.kind,data.pattern.strip(),data.action,data.enabled,actor(),int(time.time())))
    s.bump_policy(data.chat_id);audit(request,data.chat_id,'rule_saved',data.model_dump());return {'ok':True}

class ExceptionRule(BaseModel):
    chat_id:int=0
    value:str=Field(min_length=1,max_length=500)
    category:Literal['word','phrase','domain','advertising','behavior']='word'
@app.post('/api/antispam/exceptions')
async def exception(data:ExceptionRule,request:Request):
    if data.chat_id:require_chat(data.chat_id)
    if not data.value.strip():fail('Введите текст исключения.')
    scope={'categories':[data.category],'match_mode':'whole_message'}
    ctrl.store.write('INSERT INTO rule_exceptions(chat_id,kind,value,scope_json,created_by,created_at) VALUES(?,?,?,?,?,?)',(data.chat_id or None,'phrase',data.value.strip(),json.dumps(scope),actor(),int(time.time())))
    ctrl.store.bump_policy(data.chat_id);audit(request,data.chat_id,'exception_added',data.model_dump());return {'ok':True}
@app.post('/api/antispam/exceptions/{eid}/remove')
async def remove_exception(eid:int,request:Request):
    row=ctrl.store.one('SELECT * FROM rule_exceptions WHERE id=?',(eid,))
    if not row:fail('Исключение не найдено.',404)
    ctrl.store.write('DELETE FROM rule_exceptions WHERE id=?',(eid,));ctrl.store.bump_policy(row['chat_id'] or 0)
    audit(request,row['chat_id'],'exception_removed',{'id':eid});return {'ok':True}

class UserPolicy(BaseModel):
    chat_id:int
    user_id:int=Field(gt=0)
    kind:Literal['trusted','blocked']
    remove:bool=False
@app.post('/api/antispam/users')
async def user_policy(data:UserPolicy,request:Request):
    require_chat(data.chat_id)
    if data.kind=='blocked' and not data.remove and await ctrl.is_protected(data.chat_id,data.user_id):fail('Разработчикельцев и администраторов блокировать нельзя.')
    ctrl.store.set_user_policy(data.chat_id,data.user_id,data.kind,actor(),data.remove)
    audit(request,data.chat_id,'user_policy',data.model_dump());return {'ok':True}

class Preview(BaseModel):
    chat_id:int
    text:str=Field(min_length=1,max_length=10000)
@app.post('/api/antispam/preview')
async def preview(data:Preview):
    require_chat(data.chat_id)
    d=ctrl.engine.evaluate(MessageContext(data.chat_id,0,None,data.text,int(time.time()),is_manual_check=True))
    return {'action':d.action.value,'reasons':[s.code for s in d.signals],'note':'Текст проверен без удаления, блокировки и отправки сообщений.'}

@app.get('/api/antispam/events')
async def events(cid:int=0,pending:bool=False,before:int=0):
    rows=ctrl.store.rows("SELECT e.*,c.title,q.status AS review_status FROM moderation_events e JOIN chats c ON c.chat_id=e.chat_id LEFT JOIN review_queue q ON q.event_id=e.id WHERE (?=0 OR e.chat_id=?) AND (?=0 OR q.status='pending') AND (?=0 OR e.id<?) ORDER BY e.id DESC LIMIT 100",(cid,cid,pending,before,before))
    for row in rows:
        row['reasons']=json.loads(row.pop('reasons_json'))
        row['link']=f'https://t.me/c/{str(row["chat_id"])[4:]}/{row["message_id"]}' if str(row['chat_id']).startswith('-100') and row['message_id'] else None
    return {'events':rows,'next_before':rows[-1]['id'] if len(rows)==100 else None}

class EventAction(BaseModel):action:Literal['keep','delete','ban','restrict','unban','unrestrict']
@app.post('/api/antispam/events/{eid}/action')
async def event_action(eid:int,data:EventAction,request:Request):
    s=ctrl.store;e=s.one('SELECT * FROM moderation_events WHERE id=?',(eid,))
    if not e:fail('Событие не найдено.',404)
    if data.action=='keep':
        async with ctrl.executor.lock:
            if s.one("SELECT 1 FROM action_jobs WHERE event_id=? AND status='done'",(eid,)):fail('Действие уже выполнено. Удалённое сообщение восстановить нельзя.')
            s.write("UPDATE action_jobs SET status='failed',error_code='cancelled_by_owner' WHERE event_id=? AND status='pending'",(eid,))
            s.write("UPDATE moderation_events SET actual_result='kept' WHERE id=?",(eid,))
        result='success'
    else:
        if data.action in ('ban','restrict','unban','unrestrict') and not e['user_id']:fail('У этого сообщения нет доступного ID пользователя. Можно удалить сообщение.')
        payload={'chat_id':e['chat_id'],'message_id':e['message_id'],'user_id':e['user_id'],'action':data.action,'actor_id':actor(),'remove_message':data.action=='ban'}
        result=await ctrl.executor.run(payload,f'panel:event:{eid}:{data.action}',eid)
    if result in ('success','already_done'):
        s.write("UPDATE review_queue SET status='resolved',resolved_by=?,resolved_at=? WHERE event_id=?",(actor(),int(time.time()),eid))
    audit(request,e['chat_id'],'event_action',{'event_id':eid,'action':data.action,'result':result})
    return {'result':result}

@app.get('/api/antispam/audit')
async def audit_log():
    return {'audit':ctrl.store.rows('SELECT * FROM admin_audit ORDER BY id DESC LIMIT 100'),
        'jobs':ctrl.store.rows('SELECT id,event_id,chat_id,action,status,attempts,error_code FROM action_jobs ORDER BY id DESC LIMIT 100')}
