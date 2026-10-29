"""Owner-controlled text/photo publishing to managed Telegram communities."""
import asyncio
import contextlib
import json
import logging
from pathlib import Path
import secrets
import time
from datetime import datetime, timezone
from typing import Literal
import uuid
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from telethon import errors, functions, types, utils
from engine import now

MAX_PHOTO = 10 * 1024 * 1024

class PostSpec(BaseModel):
    title: str = Field(min_length=1, max_length=100)
    text: str = Field(default='', max_length=4096)
    media: list[str] = Field(default_factory=list, max_length=10)
    destinations: list[int] = Field(min_length=1, max_length=30)
    silent: bool = False

class ScheduleSpec(BaseModel):
    at: datetime


def writable(entity):
    if not isinstance(entity,(types.Chat,types.Channel)) or getattr(entity,'deactivated',False): return False
    rights=getattr(entity,'admin_rights',None)
    managed=bool(getattr(entity,'creator',False) or rights)
    return managed and (not getattr(entity,'broadcast',False) or getattr(entity,'creator',False) or bool(getattr(rights,'post_messages',False)))


class PublicationCancelled(Exception):
    pass


class Publisher:
    def __init__(self,get_store,get_runtime,data):
        self.get_store,self.get_runtime=get_store,get_runtime
        self.data=Path(data)/'content-media'
        self.task=None
        self.lock=asyncio.Lock()
        self.router=APIRouter(prefix='/api/scanner/content')
        for path,fn,methods in [('/destinations',self.destinations,['GET']),('/media',self.upload,['POST']),('/media/{media_id}',self.media,['GET']),('/posts',self.posts,['GET']),('/posts',self.create,['POST']),('/posts/{post_id}',self.edit,['POST']),('/posts/{post_id}/publish',self.publish,['POST']),('/posts/{post_id}/schedule',self.schedule,['POST']),('/posts/{post_id}/cancel',self.cancel,['POST']),('/posts/{post_id}/review',self.review,['POST']),('/posts/{post_id}/retry',self.retry,['POST'])]:
            self.router.add_api_route(path,fn,methods=methods)
        self.router.add_api_route('/destinations/{chat_id}/bot',self.connect_bot,methods=['POST'])
        self.router.add_api_route('/posts/{post_id}/remove-messages',self.remove_messages,methods=['POST'])

    @property
    def store(self):return self.get_store()
    @property
    def runtime(self):return self.get_runtime()

    def guard_outbound(self):
        guard=getattr(self.runtime,'outbound_guard',None)
        if guard:guard()

    def outbound_allowed(self):
        try:self.guard_outbound()
        except HTTPException:return False
        return True

    def stop_outbound(self,exc):
        stop=getattr(self.runtime,'outbound_pause',None)
        if stop:stop(exc)

    async def protected_write(self,client,request):
        self.guard_outbound()
        try:return await client(request)
        except (errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError) as exc:
            self.stop_outbound(exc);raise

    def initialize(self):
        self.data.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS content_media(id TEXT PRIMARY KEY,path TEXT NOT NULL,mime TEXT NOT NULL,size INTEGER NOT NULL,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS content_posts(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,text TEXT NOT NULL,media TEXT NOT NULL,destinations TEXT NOT NULL,silent INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'draft',scheduled_at REAL,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS content_delivery(post_id INTEGER NOT NULL REFERENCES content_posts(id),chat_id INTEGER NOT NULL,title TEXT NOT NULL,username TEXT,state TEXT NOT NULL DEFAULT 'pending',next_at REAL NOT NULL DEFAULT 0,error TEXT,random_ids TEXT NOT NULL,message_ids TEXT NOT NULL DEFAULT '[]',sent_at TEXT,PRIMARY KEY(post_id,chat_id));
        ''')
        # A crash may happen after Telegram accepted a send: never resend blindly.
        with self.store.db:
            self.store.db.execute("UPDATE content_delivery SET state='uncertain',error='Отправка прервана. Проверьте Telegram перед повтором.' WHERE state='sending'")
        for p in self.store.rows("SELECT id FROM content_posts WHERE status IN ('queued','publishing')"):
            self.finish(p['id'])

    def connected(self):
        if not self.runtime.client or not self.runtime.me or not self.runtime.client.is_connected():raise HTTPException(409,'Подключите Telegram-аккаунт во вкладке «Лидсканер → Подключение».')

    async def destinations(self):
        self.connected();result=[]
        async for d in self.runtime.client.iter_dialogs(limit=1000):
            if writable(d.entity):result.append(dict(id=d.id,title=d.name,username=getattr(d.entity,'username',None),kind='Канал' if getattr(d.entity,'broadcast',False) else 'Группа'))
        return {'destinations':result,'sender':getattr(self.runtime.me,'username',None) or getattr(self.runtime.me,'first_name','Telegram'),'limit':1000}

    async def connect_bot(self,chat_id:int):
        self.connected()
        async with self.lock:
            self.guard_outbound()
            client=self.runtime.client;entity=await client.get_entity(chat_id)
            if not writable(entity):raise HTTPException(400,'Подключать бота можно только к своим управляемым площадкам.')
            if not isinstance(entity,types.Channel):raise HTTPException(400,'Для обычной группы назначьте бота администратором в Telegram вручную.')
            bot=await client.get_entity('@DevelopmentDemo_bot')
            if not getattr(bot,'bot',False):raise HTTPException(409,'Не удалось проверить аккаунт антиспам-бота.')
            current=None
            try:current=(await client(functions.channels.GetParticipantRequest(entity,bot))).participant
            except errors.UserNotParticipantError:pass
            rights=getattr(current,'admin_rights',None)
            if rights and rights.delete_messages and (entity.broadcast or rights.ban_users):return {'ok':True,'already_admin':True}
            if not entity.creator and not getattr(entity.admin_rights,'add_admins',False):raise HTTPException(400,'У аккаунта нет права назначать администраторов. Владелец группы должен выдать боту удаление сообщений и блокировку участников в Telegram.')
            if current is None:await self.protected_write(client,functions.channels.InviteToChannelRequest(entity,[bot]))
            rights=rights or types.ChatAdminRights()
            rights.delete_messages=True
            if not entity.broadcast:rights.ban_users=True
            await self.protected_write(client,functions.channels.EditAdminRequest(channel=entity,user_id=bot,admin_rights=rights,rank='Антиспам'))
            self.store.put('last_bot_connection',dict(chat_id=chat_id,at=now()))
            return {'ok':True,'already_admin':False}

    async def upload(self,request:Request):
        total=self.store.db.execute('SELECT COALESCE(SUM(size),0) FROM content_media').fetchone()[0]
        if total>=250*1024*1024:raise HTTPException(400,'Лимит хранилища фото 250 МБ. Удалите ненужные вложения через администратора сервера.')
        content=bytearray()
        async for chunk in request.stream():
            content.extend(chunk)
            if len(content)>MAX_PHOTO:raise HTTPException(413,'Фото не больше 10 МБ.')
        if content.startswith(b'\xff\xd8\xff') and content.endswith(b'\xff\xd9'):ext,mime='jpg','image/jpeg'
        elif content.startswith(b'\x89PNG\r\n\x1a\n') and b'IEND' in content[-16:]:ext,mime='png','image/png'
        else:raise HTTPException(400,'Выберите фото JPEG или PNG. Другие форматы пока не поддерживаются.')
        if total+len(content)>250*1024*1024:raise HTTPException(400,'Недостаточно места в хранилище фото.')
        media_id=uuid.uuid4().hex;filename=media_id+'.'+ext
        (self.data/filename).write_bytes(content)
        with self.store.db:self.store.db.execute('INSERT INTO content_media VALUES(?,?,?,?,?)',(media_id,filename,mime,len(content),now()))
        return dict(id=media_id,url='/api/scanner/content/media/'+media_id,mime=mime,size=len(content))

    def media_row(self,media_id):
        rows=self.store.rows('SELECT * FROM content_media WHERE id=?',(media_id,))
        if not rows:raise HTTPException(404,'Фото не найдено.')
        return rows[0]

    async def media(self,media_id:str):
        row=self.media_row(media_id)
        return FileResponse(self.data/row['path'],media_type=row['mime'],headers={'X-Content-Type-Options':'nosniff'})

    def post(self,post_id):
        rows=self.store.rows('SELECT * FROM content_posts WHERE id=?',(post_id,))
        if not rows:raise HTTPException(404,'Публикация не найдена.')
        row=rows[0];row['media']=json.loads(row['media']);row['destinations']=json.loads(row['destinations'])
        row['delivery']=self.store.rows('SELECT chat_id,title,username,state,error,message_ids,sent_at,next_at FROM content_delivery WHERE post_id=?',(post_id,))
        for d in row['delivery']:
            d['message_ids']=json.loads(d['message_ids'])
            d['links']=[f'https://t.me/{d["username"]}/{mid}' if d['username'] else f'https://t.me/c/{utils.resolve_id(d["chat_id"])[0]}/{mid}' for mid in d['message_ids']]
        return row

    async def posts(self):return {'posts':[self.post(r['id']) for r in self.store.rows('SELECT id FROM content_posts ORDER BY id DESC LIMIT 100')]}

    async def validate(self,spec):
        self.connected()
        if not spec.title.strip():raise HTTPException(400,'Укажите название публикации.')
        if not spec.text.strip() and not spec.media:raise HTTPException(400,'Добавьте текст или фото.')
        if len(spec.media)!=len(set(spec.media)):raise HTTPException(400,'Уберите повторяющиеся фото.')
        if spec.media and len(spec.text.encode('utf-16-le'))//2>1024:raise HTTPException(400,'Подпись к фото — до 1024 символов; сократите текст.')
        if not spec.media and len(spec.text.encode('utf-16-le'))//2>4096:raise HTTPException(400,'Текст — до 4096 символов.')
        for mid in spec.media:self.media_row(mid)
        result=[]
        for cid in dict.fromkeys(spec.destinations):
            entity=await self.runtime.client.get_entity(cid)
            if not writable(entity):raise HTTPException(400,'Можно публиковать только в управляемые группы и каналы с правом публикации.')
            result.append(dict(id=cid,title=entity.title,username=getattr(entity,'username',None)))
        return result

    async def create(self,spec:PostSpec):
        async with self.lock:
            destinations=await self.validate(spec)
            with self.store.db:
                c=self.store.db.execute('INSERT INTO content_posts(title,text,media,destinations,silent,created,updated) VALUES(?,?,?,?,?,?,?)',(spec.title.strip(),spec.text,json.dumps(spec.media),json.dumps(destinations,ensure_ascii=False),spec.silent,now(),now()))
            return self.post(c.lastrowid)

    async def edit(self,post_id:int,spec:PostSpec):
        async with self.lock:
            if self.post(post_id)['status'] not in ('draft','review','scheduled'):raise HTTPException(409,'Создайте новый черновик: эта публикация уже передана на отправку.')
            destinations=await self.validate(spec)
            with self.store.db:self.store.db.execute("UPDATE content_posts SET title=?,text=?,media=?,destinations=?,silent=?,status='draft',scheduled_at=NULL,updated=? WHERE id=?",(spec.title.strip(),spec.text,json.dumps(spec.media),json.dumps(destinations,ensure_ascii=False),spec.silent,now(),post_id))
            return self.post(post_id)

    def enqueue(self,post):
        with self.store.db:
            for d in post['destinations']:
                random_ids=[secrets.randbits(63) for _ in range(max(1,len(post['media'])))]
                self.store.db.execute('INSERT OR IGNORE INTO content_delivery(post_id,chat_id,title,username,random_ids) VALUES(?,?,?,?,?)',(post['id'],d['id'],d['title'],d['username'],json.dumps(random_ids)))
            self.store.db.execute("UPDATE content_posts SET status='queued',updated=? WHERE id=?",(now(),post['id']))

    async def publish(self,post_id:int):
        self.connected()
        async with self.lock:
            post=self.post(post_id)
            if post['status'] in ('draft','review','scheduled'):self.enqueue(post)
            return self.post(post_id)

    async def schedule(self,post_id:int,spec:ScheduleSpec):
        if spec.at.tzinfo is None:raise HTTPException(400,'Укажите время с часовым поясом.')
        at=spec.at.timestamp()
        if not time.time()+5<at<time.time()+365*86400:raise HTTPException(400,'Выберите будущее время в пределах года.')
        async with self.lock:
            if self.post(post_id)['status'] not in ('draft','review','scheduled'):raise HTTPException(409,'Эта публикация уже передана на отправку.')
            with self.store.db:self.store.db.execute("UPDATE content_posts SET status='scheduled',scheduled_at=?,updated=? WHERE id=?",(at,now(),post_id))
            return self.post(post_id)

    async def review(self,post_id:int):
        async with self.lock:
            if self.post(post_id)['status']!='draft':raise HTTPException(409,'На согласование можно отправить черновик.')
            with self.store.db:self.store.db.execute("UPDATE content_posts SET status='review',updated=? WHERE id=?",(now(),post_id))
            return self.post(post_id)

    async def cancel(self,post_id:int):
        async with self.lock:
            if self.post(post_id)['status'] not in ('draft','review','scheduled','queued','publishing'):raise HTTPException(409,'Эта публикация уже завершена.')
            with self.store.db:
                self.store.db.execute("UPDATE content_delivery SET state='cancelled' WHERE post_id=? AND state='pending'",(post_id,))
                self.store.db.execute("UPDATE content_posts SET status='cancelled',updated=? WHERE id=?",(now(),post_id))
            return self.post(post_id)

    async def retry(self,post_id:int):
        self.connected()
        async with self.lock:
            self.post(post_id)
            with self.store.db:
                # Ambiguous transport results require checking Telegram, not a blind replay.
                c=self.store.db.execute("UPDATE content_delivery SET state='pending',error=NULL,next_at=0 WHERE post_id=? AND state='failed'",(post_id,))
                if not c.rowcount:raise HTTPException(400,'Нет подтверждённых неудачных отправок для повтора.')
                self.store.db.execute("UPDATE content_posts SET status='queued',updated=? WHERE id=?",(now(),post_id))
            return self.post(post_id)

    async def remove_messages(self,post_id:int):
        self.connected()
        async with self.lock:
            post=self.post(post_id)
            if any(d['state'] in ('pending','sending') for d in post['delivery']):raise HTTPException(409,'Сначала отмените отправку и дождитесь завершения текущей задачи.')
            for d in post['delivery']:
                if d['state']!='sent':continue
                entity=await self.runtime.client.get_entity(d['chat_id'])
                if not writable(entity):raise HTTPException(400,'Нет прав управления одной из площадок. Уже выполненные удаления сохранены.')
                await self.runtime.client.delete_messages(entity,d['message_ids'],revoke=True)
                with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='removed' WHERE post_id=? AND chat_id=?",(post_id,d['chat_id']))
            if any(d['state']=='removed' for d in self.post(post_id)['delivery']):
                with self.store.db:self.store.db.execute("UPDATE content_posts SET status='removed',updated=? WHERE id=?",(now(),post_id))
            return self.post(post_id)

    def finish(self,post_id):
        # An in-flight delivery may finish after cancellation; preserve both the
        # user's cancelled post and the actual per-destination result/IDs.
        if self.post(post_id)['status']=='cancelled':
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='cancelled' WHERE post_id=? AND state='pending'",(post_id,))
            return
        rows=self.store.rows('SELECT state FROM content_delivery WHERE post_id=?',(post_id,))
        states=[r['state'] for r in rows]
        if not states:return
        status='publishing' if any(s in ('pending','sending') for s in states) else 'published' if all(s=='sent' for s in states) else 'partial' if 'sent' in states else 'cancelled' if all(s=='cancelled' for s in states) else 'failed'
        with self.store.db:self.store.db.execute('UPDATE content_posts SET status=?,updated=? WHERE id=?',(status,now(),post_id))

    async def tick(self):
        if not self.runtime.client or not self.runtime.me or not self.runtime.client.is_connected() or not self.outbound_allowed():return
        async with self.lock:
            for post in self.store.rows("SELECT id FROM content_posts WHERE status='scheduled' AND scheduled_at<=?",(time.time(),)):self.enqueue(self.post(post['id']))
            rows=self.store.rows("SELECT d.* FROM content_delivery d JOIN content_posts p ON p.id=d.post_id WHERE d.state='pending' AND d.next_at<=? AND p.status IN ('queued','publishing') ORDER BY d.post_id,d.chat_id LIMIT 1",(time.time(),))
            if not rows:return
            row=rows[0];post=self.post(row['post_id'])
            with self.store.db:
                self.store.db.execute("UPDATE content_delivery SET state='sending' WHERE post_id=? AND chat_id=?",(post['id'],row['chat_id']))
                self.store.db.execute("UPDATE content_posts SET status='publishing' WHERE id=?",(post['id'],))
        try:
            entity=await self.runtime.client.get_entity(row['chat_id'])
            if not writable(entity):raise HTTPException(400,'Нет прав публикации в этой площадке.')
            if not self.outbound_allowed():
                with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='pending' WHERE post_id=? AND chat_id=?",(post['id'],row['chat_id']))
                return
            def still_active():
                saved=self.post(post['id'])
                delivery=next((d for d in saved['delivery'] if d['chat_id']==row['chat_id']),None)
                if saved['status']=='cancelled' or not delivery or delivery['state']!='sending':
                    raise PublicationCancelled()
            result=await asyncio.wait_for(self.send(post,row,entity,before_write=still_active),timeout=180)
            ids=[]
            if isinstance(result,types.UpdateShortSentMessage):ids=[result.id]
            for update in getattr(result,'updates',[]):
                if isinstance(update,types.UpdateMessageID):ids.append(update.id)
                elif isinstance(update,(types.UpdateNewChannelMessage,types.UpdateNewMessage)):ids.append(update.message.id)
            ids=list(dict.fromkeys(ids))
            state='sent' if ids else 'uncertain'
            error=None if ids else 'Telegram вернул ответ без ID сообщений. Проверьте площадку; автоматического повтора нет.'
            with self.store.db:self.store.db.execute('UPDATE content_delivery SET state=?,message_ids=?,error=?,sent_at=? WHERE post_id=? AND chat_id=?',(state,json.dumps(ids),error,now(),post['id'],row['chat_id']))
        except PublicationCancelled:
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='cancelled' WHERE post_id=? AND chat_id=? AND state='sending'",(post['id'],row['chat_id']))
        except errors.FloodWaitError as exc:
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='pending',next_at=?,error=? WHERE post_id=? AND chat_id=?",(time.time()+exc.seconds+1,f'Telegram просит подождать {exc.seconds} секунд.',post['id'],row['chat_id']))
        except (errors.PeerFloodError,errors.SessionRevokedError,errors.AuthKeyUnregisteredError,errors.UserDeactivatedError,errors.UserDeactivatedBanError) as exc:
            self.stop_outbound(exc)
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='failed',error=? WHERE post_id=? AND chat_id=?",('Исходящие остановлены Telegram ('+type(exc).__name__+'). Проверьте аккаунт перед восстановлением.',post['id'],row['chat_id']))
        except (errors.ServerError,errors.TimedOutError) as exc:
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='uncertain',error=? WHERE post_id=? AND chat_id=?",('Telegram не подтвердил результат ('+type(exc).__name__+'). Проверьте площадку; автоматического повтора нет.',post['id'],row['chat_id']))
        except (errors.RPCError,HTTPException) as exc:
            message=exc.detail if isinstance(exc,HTTPException) else f'Telegram отклонил публикацию ({type(exc).__name__}). Проверьте права и размер вложения.'
            state='pending' if isinstance(exc,HTTPException) and not self.outbound_allowed() else 'failed'
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state=?,error=? WHERE post_id=? AND chat_id=?",(state,message,post['id'],row['chat_id']))
        except Exception as exc:
            with self.store.db:self.store.db.execute("UPDATE content_delivery SET state='uncertain',error=? WHERE post_id=? AND chat_id=?",(f'Результат отправки неизвестен ({type(exc).__name__}). Проверьте Telegram, прежде чем создавать повторный пост.',post['id'],row['chat_id']))
        self.finish(post['id'])

    async def send(self,post,row,entity,*,before_write=None,topic_id=None,before_send=None):
        client=self.runtime.client;peer=await client.get_input_entity(entity);ids=json.loads(row['random_ids'])
        if topic_id is not None and (type(topic_id) is not int or not 1<=topic_id<=2**31-1):
            raise HTTPException(400,'Укажите явно выбранную тему форума с ID от 1.')
        # General is an explicit saved route, but its transport is a plain send.
        routing={'reply_to':types.InputReplyToMessage(reply_to_msg_id=topic_id)} if topic_id is not None and topic_id!=1 else {}
        def guard():
            self.guard_outbound()
            if before_write:before_write()
        async def final_guard():
            if before_send:await before_send()
            guard()
        if not post['media']:
            await final_guard()
            return await client(functions.messages.SendMessageRequest(peer=peer,message=post['text'],random_id=ids[0],silent=bool(post['silent']),no_webpage=True,**routing))
        medias=[]
        for i,mid in enumerate(post['media']):
            media=self.media_row(mid);guard();uploaded=await client.upload_file(self.data/media['path'])
            media_input=types.InputMediaUploadedPhoto(file=uploaded)
            if len(post['media'])==1:
                await final_guard()
                return await client(functions.messages.SendMediaRequest(peer=peer,media=media_input,message=post['text'],random_id=ids[0],silent=bool(post['silent']),**routing))
            guard()
            remote=await client(functions.messages.UploadMediaRequest(peer=peer,media=media_input))
            medias.append(types.InputSingleMedia(media=utils.get_input_media(remote),random_id=ids[i],message=post['text'] if i==0 else ''))
        await final_guard()
        return await client(functions.messages.SendMultiMediaRequest(peer=peer,multi_media=medias,silent=bool(post['silent']),**routing))

    async def loop(self):
        while True:
            try:await self.tick()
            except Exception as exc:logging.getLogger(__name__).error('Publisher loop failed: %s',type(exc).__name__)
            await asyncio.sleep(2)

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
