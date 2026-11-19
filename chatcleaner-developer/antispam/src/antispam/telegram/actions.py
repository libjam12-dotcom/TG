import asyncio
import json
import time
import html
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter, TelegramNetworkError, TelegramBadRequest
from aiogram.types import ChatPermissions
from antispam.storage.repositories import Store


class Executor:
    def __init__(self, bot: Bot, store: Store, owners):
        self.bot, self.store, self.owners = bot, store, owners
        self.lock = asyncio.Lock()

    async def run(self, payload: dict, operation_key: str, event_id: int | None = None) -> str:
        async with self.lock:
            return await self._run(payload, operation_key, event_id)

    async def _run(self, payload: dict, operation_key: str, event_id: int | None = None) -> str:
        chat_id=payload['chat_id']; action=payload['action']
        existing=self.store.one('SELECT * FROM action_jobs WHERE operation_key=?',(operation_key,))
        if existing and existing['status']=='done':return 'already_done'
        if existing and existing['status']=='failed' and existing['error_code']=='cancelled_by_owner':return 'cancelled_by_owner'
        if not existing:
            self.store.write('INSERT INTO action_jobs(event_id,operation_key,chat_id,action,payload_json) VALUES(?,?,?,?,?)',
                             (event_id,operation_key,chat_id,action,json.dumps(payload)))
        job=self.store.one('SELECT * FROM action_jobs WHERE operation_key=?',(operation_key,))
        payload=dict(payload)
        saved_payload=json.loads(job['payload_json'])
        if saved_payload.get('message_removed'):payload['message_removed']=True
        if saved_payload.get('ad_notice_result'):payload['ad_notice_result']=saved_payload['ad_notice_result']
        if payload.get('automatic'):
            chat=self.store.chat(chat_id)
            if not chat or not chat['enabled'] or chat['mode']!='active' or chat['policy_version']!=payload.get('policy_version'):
                return self.finish(job,'failed','policy_changed')
        self.store.write("UPDATE action_jobs SET status='running',attempts=attempts+1 WHERE id=?",(job['id'],))
        try:
            me=await self.bot.get_me()
            rights=await self.bot.get_chat_member(chat_id,me.id)
            if rights.status not in ('administrator','creator'):
                return self.finish(job,'failed','bot_not_admin')
            if action=='delete' and payload.get('user_id'):
                member=await self.bot.get_chat_member(chat_id,payload['user_id'])
                if payload['user_id'] in self.owners() or member.status in ('administrator','creator'):
                    return self.finish(job,'failed','protected_user')
            if action in ('ban','restrict','unban','unrestrict'):
                uid=payload.get('user_id')
                if not uid:return self.finish(job,'failed','unknown_user')
                member=await self.bot.get_chat_member(chat_id,uid)
                if uid in self.owners() or member.status in ('administrator','creator'):
                    return self.finish(job,'failed','protected_user')
                if not getattr(rights,'can_restrict_members',False):return self.finish(job,'failed','missing_restrict_right')
            if action=='delete' and not getattr(rights,'can_delete_messages',False):
                return self.finish(job,'failed','missing_delete_right')
            if action in ('ban','restrict') and payload.get('remove_message'):
                if not getattr(rights,'can_delete_messages',False):
                    return self.finish(job,'failed','missing_delete_right')
                if not payload.get('message_id'):return self.finish(job,'failed','missing_message_id')
                if not payload.get('message_removed'):
                    try:await self.bot.delete_message(chat_id,payload['message_id'])
                    except TelegramBadRequest as exc:
                        if 'message to delete not found' not in str(exc).casefold():raise
                    payload['message_removed']=True
                    self.store.write('UPDATE action_jobs SET payload_json=? WHERE id=?',(json.dumps(payload),job['id']))
            if action=='delete':
                if not payload.get('message_removed'):
                    try:await self.bot.delete_message(chat_id,payload['message_id'])
                    except TelegramBadRequest as exc:
                        if 'message to delete not found' not in str(exc).casefold():raise
                    payload['message_removed']=True
                    self.store.write('UPDATE action_jobs SET payload_json=? WHERE id=?',(json.dumps(payload),job['id']))
                if payload.get('ad_notice') and not payload.get('ad_notice_result'):
                    payload['ad_notice_result']=await self.ad_notice(payload)
                    self.store.write('UPDATE action_jobs SET payload_json=? WHERE id=?',(json.dumps(payload),job['id']))
            elif action=='ban':await self.bot.ban_chat_member(chat_id,payload['user_id'],revoke_messages=False)
            elif action=='unban':await self.bot.unban_chat_member(chat_id,payload['user_id'],only_if_banned=True)
            elif action=='restrict':
                await self.bot.restrict_chat_member(chat_id,payload['user_id'],permissions=ChatPermissions(can_send_messages=False),
                                                   until_date=int(time.time())+600,use_independent_chat_permissions=True)
            elif action=='unrestrict':
                chat=await self.bot.get_chat(chat_id)
                await self.bot.restrict_chat_member(chat_id,payload['user_id'],permissions=chat.permissions or ChatPermissions(can_send_messages=True),use_independent_chat_permissions=True)
            else:return self.finish(job,'failed','unknown_action')
            return self.finish(job,'done','success')
        except TelegramRetryAfter as exc:
            delay=max(1,int(exc.retry_after)+1)
            self.store.write("UPDATE action_jobs SET status='pending',next_attempt_at=?,error_code='rate_limit' WHERE id=?",(int(time.time())+delay,job['id']))
            return 'queued_retry'
        except TelegramNetworkError:
            self.store.write("UPDATE action_jobs SET status='pending',next_attempt_at=?,error_code='network' WHERE id=?",(int(time.time())+10,job['id']))
            return 'queued_retry'
        except TelegramAPIError:
            return self.finish(job,'failed','telegram_rejected')

    def finish(self, job: dict, status: str, result: str) -> str:
        self.store.write('UPDATE action_jobs SET status=?,error_code=?,next_attempt_at=NULL WHERE id=?',(status,result,job['id']))
        if job['event_id']:
            value=job['action'] if status=='done' else 'failed:'+result
            self.store.write('UPDATE moderation_events SET actual_result=? WHERE id=?',(value,job['event_id']))
            if status=='done':
                self.store.write("UPDATE review_queue SET status='resolved',resolved_at=? WHERE event_id=?",(int(time.time()),job['event_id']))
        return result

    async def ad_notice(self,payload:dict) -> str:
        """A notification failure never turns a successful deletion into a failed moderation."""
        uid=payload.get('user_id');cid=payload['chat_id'];notice=payload['ad_notice'];now=int(time.time())
        cooldown_key=f'ad_notice:{cid}:{notice.get("author_key",uid or 0)}'
        if self.store.setting(cooldown_key,0)>now:return 'cooldown'
        self.store.set_setting(cooldown_key,now+max(60,min(int(notice.get('cooldown_seconds',600)),3600)))
        title=html.escape(notice.get('group_title','группе'))
        contact=html.escape(notice.get('contact','администраторам группы'))
        text=f'Ваше рекламное сообщение в «{title}» удалено.\nРазмещение рекламы нужно согласовать с администратором.\nПо вопросам рекламы: {contact}.\nВаш аккаунт за это объявление не заблокирован.'
        if uid:
            try:
                await self.bot.send_message(uid,text)
                return 'private'
            except TelegramNetworkError:return 'delivery_unknown'
            except TelegramRetryAfter:return 'rate_limited'
            except TelegramAPIError:pass
        name=html.escape(notice.get('author_name') or 'Автор объявления')
        mention=f'<a href="tg://user?id={uid}">{name}</a>' if uid else name
        kwargs={'message_thread_id':notice['thread_id']} if notice.get('thread_id') else {}
        try:
            await self.bot.send_message(cid,mention+'\n'+text,**kwargs)
            return 'group'
        except TelegramAPIError:return 'unavailable'

    async def retry_loop(self):
        while True:
            pending=self.store.rows("SELECT * FROM action_jobs WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY id LIMIT 20",(int(time.time()),))
            for job in pending:
                if job['attempts']>=5:
                    self.finish(job,'failed','retry_limit');continue
                payload=json.loads(job['payload_json'])
                # Manual retries keep the explicit actor; owner access can be revoked.
                if not payload.get('automatic') and payload.get('actor_id') not in self.owners():
                    self.finish(job,'failed','actor_access_revoked');continue
                await self.run(payload,job['operation_key'],job['event_id'])
            await asyncio.sleep(3)
