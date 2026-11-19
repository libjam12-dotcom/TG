import asyncio
from dataclasses import replace
from datetime import datetime
import html
import json
import logging
import re
from pathlib import Path
import time

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, FSInputFile, BufferedInputFile

from antispam.config import Settings
from antispam.domain.models import Action, MessageContext
from antispam.moderation.engine import Engine
from antispam.moderation.normalize import domain, normalize, word_spans
from antispam.storage.repositories import Store
from antispam.telegram.actions import Executor
from antispam.telegram.menus import keyboard, home, chat_menu

log=logging.getLogger(__name__)
LABELS={'allow':'Пропустить','review':'На разбор','delete':'Удалить сообщение','restrict':'Ограничить на 10 минут','ban':'Заблокировать'}


def telegram_time(value: datetime | int) -> int:
    # aiogram may expose edit_date as Unix seconds while date is a datetime.
    return int(value.timestamp()) if isinstance(value,datetime) else int(value)


def env_file(path=Path('.env')) -> dict:
    if not path.exists():return {}
    values={}
    for line in path.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key,value=line.split('=',1);values[key.strip()]=value.strip()
    return values


class Controller:
    def __init__(self,bot: Bot,store: Store,settings: Settings,policy: dict):
        self.bot,self.store,self.settings,self.policy=bot,store,settings,policy
        self.engine=Engine(store,policy)
        self.executor=Executor(bot,store,self.owners)
        self.router=Router()
        self.admin_cache={}
        self.last_message_saved_at={}
        self.router.channel_post.register(self.group)
        self.router.edited_channel_post.register(self.edited)
        self.router.my_chat_member.register(self.bot_membership)
        self.router.chat_member.register(self.member_join)
        self.router.callback_query.register(self.callback)
        self.router.message.register(self.identify,Command('id'))
        self.router.message.register(self.start,Command('start','menu','help'))
        self.router.message.register(self.check_command,Command('check'))
        self.router.message.register(self.cancel,Command('cancel'))
        self.router.message.register(self.private,F.chat.type=='private')
        self.router.message.register(self.group,F.chat.type.in_({'group','supergroup'}))
        self.router.edited_message.register(self.edited,F.chat.type.in_({'group','supergroup'}))

    def owners(self) -> set[int]:
        # Local owner list may be updated without restarting the polling process.
        raw=env_file().get('BOT_OWNER_IDS',','.join(map(str,self.settings.owner_ids)))
        try:return {int(x.strip()) for x in raw.split(',') if x.strip() and int(x.strip())>0}
        except ValueError:return set()

    async def permitted(self,uid:int,message:Message) -> bool:
        if uid in self.owners():return True
        await message.answer(f'Управление ещё не разрешено для вашего аккаунта. Ваш Telegram ID: <code>{uid}</code>. Передайте его исполнителю для настройки доступа.')
        return False

    async def identify(self,m:Message):
        if m.from_user:await m.answer(f'Ваш Telegram ID: <code>{m.from_user.id}</code>\nID чата: <code>{m.chat.id}</code>')

    async def start(self,m:Message):
        if not m.from_user:return
        if m.chat.type!='private':
            self.store.register_chat(m.chat.id,m.chat.title or str(m.chat.id))
            await m.answer('Группа добавлена в список. Назначьте бота администратором с правами удаления сообщений и ограничения участников. Затем в личном чате @ExampleModerationBot: /menu → Группы → эта группа → Режим наблюдения. Если доступа к меню ещё нет, отправьте боту /id и передайте исполнителю.');return
        if m.from_user.id not in self.owners():
            contact=self.store.setting('ad_contact:0','администраторам группы')
            await m.answer('Я помогаю поддерживать порядок в группах. Несогласованные рекламные объявления удаляются.\nПо вопросам размещения рекламы: '+html.escape(contact)+'.\nУправление доступно администраторам бота. /id — узнать свой ID.');return
        self.store.set_setting(f'notify:{m.from_user.id}',True)
        await m.answer('Chatcleaner Dubai\n\nВыберите действие. Новые группы начинают с выключенными проверками. Для пилота включите «Режим наблюдения»: бот покажет причины, сохраняя сообщения. AI не используется.',reply_markup=home())

    async def cancel(self,m:Message):
        if m.from_user:self.store.clear_session(m.from_user.id)
        await m.answer('Ввод отменён. /menu — главное меню.')

    async def is_protected(self,cid:int,uid:int|None) -> bool:
        if uid in self.owners():return True
        if uid is None:return False
        cached=self.admin_cache.get(cid)
        if not cached or cached[0]<time.time():
            try:
                admins=await self.bot.get_chat_administrators(cid)
                cached=(time.time()+60,{a.user.id for a in admins})
                self.admin_cache[cid]=cached
            except TelegramAPIError:
                # Do not punish when the list of protected users cannot be verified.
                return True
        return uid in cached[1]

    def context(self,m:Message,manual=False) -> MessageContext:
        text=m.text or m.caption or ''
        links=[]
        for entity in m.entities or m.caption_entities or []:
            if entity.type=='text_link' and entity.url:links.append(entity.url)
            elif entity.type=='url':links.append(entity.extract_from(text))
        member=self.store.member(m.chat.id,m.from_user.id if m.from_user else None)
        return MessageContext(chat_id=m.chat.id,message_id=m.message_id,
            user_id=m.from_user.id if m.from_user and not m.sender_chat else None,
            text=text,observed_at=telegram_time(m.edit_date or m.date),thread_id=m.message_thread_id,
            sender_chat_id=m.sender_chat.id if m.sender_chat else None,
            joined_at=member.get('joined_at'),first_messages_seen=member.get('message_count',0),
            is_edit=bool(m.edit_date),is_manual_check=manual,links=tuple(links),
            protected=bool(m.chat.type!='channel' and m.sender_chat and (m.sender_chat.id==m.chat.id or m.is_automatic_forward)))

    async def check_command(self,m:Message):
        if not m.from_user or not await self.permitted(m.from_user.id,m):return
        if m.reply_to_message:
            ctx=self.context(m.reply_to_message,True)
        else:
            text=(m.text or '').partition(' ')[2]
            if not text:await m.answer('Ответьте на сообщение командой /check или напишите /check текст.');return
            ctx=MessageContext(m.chat.id,0,None,text,int(time.time()),is_manual_check=True)
        d=self.engine.evaluate(ctx)
        await m.answer(self.verdict(d)+'\n\nЭто проверка. Наказание не применяется.')

    def verdict(self,d):
        reasons='\n'.join('• '+html.escape(s.code) for s in d.signals) or 'Совпадений с правилами и подозрительных признаков нет.'
        return f'<b>{LABELS[d.action.value]}</b>\n{reasons}'

    async def bot_membership(self,u:ChatMemberUpdated):
        if u.chat.type not in ('group','supergroup','channel'):return
        self.store.register_chat(u.chat.id,u.chat.title or str(u.chat.id))
        if u.new_chat_member.status not in ('administrator','creator'):
            self.store.write('UPDATE chats SET enabled=0 WHERE chat_id=?',(u.chat.id,))
            await self.notify(f'Бот не имеет административных прав в «{html.escape(u.chat.title or str(u.chat.id))}». Проверки остановлены.')
        else:
            await self.notify(f'Подключена группа «{html.escape(u.chat.title or str(u.chat.id))}». Проверки пока выключены; включите наблюдение через меню.',keyboard([[('Открыть группу',f'chat:{u.chat.id}')]]))

    async def member_join(self,u:ChatMemberUpdated):
        self.store.register_chat(u.chat.id,u.chat.title or str(u.chat.id))
        old=u.old_chat_member;new=u.new_chat_member
        old_present=old.status in ('member','administrator','creator') or (old.status=='restricted' and getattr(old,'is_member',False))
        new_present=new.status in ('member','administrator','creator') or (new.status=='restricted' and getattr(new,'is_member',False))
        if not old_present and new_present:self.store.joined(u.chat.id,new.user.id,telegram_time(u.date))

    async def group(self,m:Message):
        title=m.chat.title or str(m.chat.id)
        chat=self.store.chat(m.chat.id)
        if not chat or chat['title']!=title:
            self.store.register_chat(m.chat.id,title)
            chat=self.store.chat(m.chat.id)
        for member in m.new_chat_members or []:self.store.joined(m.chat.id,member.id,telegram_time(m.date))
        if m.new_chat_members or m.left_chat_member or not (m.text or m.caption or m.entities or m.caption_entities):return
        if m.from_user and m.from_user.is_bot and not m.sender_chat:return
        if not chat['enabled']:return
        now=int(time.time())
        if now-self.last_message_saved_at.get(m.chat.id,0)>=30:
            self.store.set_setting(f'last_message:{m.chat.id}',now)
            self.last_message_saved_at[m.chat.id]=now
        ctx=self.context(m)
        ctx=replace(ctx,protected=ctx.protected or await self.is_protected(ctx.chat_id,ctx.user_id))
        decision=self.engine.evaluate(ctx)
        actual='allowed' if decision.action==Action.ALLOW else 'observed' if chat['mode']=='observe' else 'pending' if decision.action!=Action.REVIEW else 'review'
        payload=None
        if chat['mode']=='active' and decision.action in (Action.DELETE,Action.RESTRICT,Action.BAN):
            payload={'chat_id':ctx.chat_id,'message_id':ctx.message_id,'user_id':ctx.user_id,'action':decision.action.value,
                     'automatic':True,'policy_version':decision.policy_version,
                     'remove_message':decision.action in (Action.RESTRICT,Action.BAN)}
            if decision.category=='advertisement' and decision.action==Action.DELETE and m.chat.type!='channel':
                payload['ad_notice']=self.advertising_notice(chat,ctx,m.from_user.full_name if m.from_user else m.sender_chat.title if m.sender_chat else '')
        eid,new=self.store.record(ctx,decision,actual,payload)
        if not new:return
        result=None
        if payload is not None:
            result=await self.executor.run(payload,f'event:{eid}:automatic',eid)
        if decision.action!=Action.ALLOW:
            await self.notify(f'<b>{html.escape(chat["title"])}</b> • #{eid}\n'+self.verdict(decision)+'\n\n'+html.escape(ctx.text[:500])+f'\n\nРежим: {"наблюдение" if chat["mode"]=="observe" else "активный"}',
                              keyboard([[('Разобрать',f'event:{eid}')]]))
        if result and result not in ('success','already_done'):await self.notify(f'Действие по событию #{eid}: {html.escape(result)}')

    async def edited(self,m:Message):await self.group(m)

    def advertising_notice(self,chat,ctx,name=''):
        return {'group_title':chat['title'],'author_name':name,'thread_id':ctx.thread_id,
                'author_key':ctx.user_id or ctx.sender_chat_id or 0,
                'cooldown_seconds':self.store.setting('advertising:0',{}).get('notice_cooldown_seconds',600),
                'contact':self.store.setting(f'ad_contact:{ctx.chat_id}',self.store.setting('ad_contact:0','администраторам группы'))}

    async def notify(self,text:str,markup=None):
        for uid in self.owners():
            if not self.store.setting(f'notify:{uid}',False):continue
            try:await self.bot.send_message(uid,text,reply_markup=markup)
            except TelegramAPIError:log.warning('Notification unavailable for configured owner')

    async def show(self,c:CallbackQuery,text:str,markup=None):
        await c.message.answer(text,reply_markup=markup)

    async def confirm(self,c:CallbackQuery,payload:dict,text:str):
        key=self.store.confirmation(c.from_user.id,payload)
        await self.show(c,text,keyboard([[('Подтвердить',f'confirm:{key}'),('Отмена','cancel')]]))

    async def callback(self,c:CallbackQuery):
        if not c.message:return
        if c.from_user.id not in self.owners():await c.answer('Нет доступа',show_alert=True);return
        await c.answer()
        parts=(c.data or '').split(':');cmd=parts[0]
        try:
            await self.route_callback(c,cmd,parts)
        except (ValueError,KeyError,TypeError):
            await self.show(c,'Некорректные данные кнопки. Откройте /menu заново.')
        except TelegramAPIError:
            await self.show(c,'Telegram отклонил действие. Проверьте права бота и доступность группы.')

    async def route_callback(self,c:CallbackQuery,cmd:str,p:list[str]):
        actor=c.from_user.id
        if cmd=='home':await self.show(c,'Главное меню',home())
        elif cmd=='export':
            content=json.dumps(self.store.export_config(),ensure_ascii=False,indent=2).encode()
            await c.message.answer_document(BufferedInputFile(content,filename='chatcleaner-antispam-config.json'),caption='Настройки модерации без токенов и переписки. Для полного восстановления используется серверная резервная копия базы.')
        elif cmd=='guide':
            path=Path('docs/client-testing.txt')
            await c.message.answer_document(FSInputFile(path),caption='Инструкция для теста и действующие правила. Сначала режим наблюдения, затем удаление в отдельной тестовой группе.')
        elif cmd=='dictionary':
            await c.message.answer_document(FSInputFile(Path('docs/filter-dictionary.txt')),caption='Словарь запретов и точечные исключения. Текущие настройки каждой группы — в её меню.')
        elif cmd=='cancel':self.store.clear_session(actor);await self.show(c,'Ввод отменён.',home())
        elif cmd=='groups':
            chats=self.store.chats()
            await self.show(c,'Выберите группу.' if chats else 'Добавьте бота администратором в тестовую группу. Она появится здесь.',
                keyboard([[(chat['title'][:50],f'chat:{chat["chat_id"]}')] for chat in chats]+[[('Главное меню','home')]]))
        elif cmd=='chat':
            chat=self.store.chat(int(p[1]))
            if not chat:await self.show(c,'Группа не подключена.');return
            await self.show(c,f'<b>{html.escape(chat["title"])}</b>\nID: <code>{chat["chat_id"]}</code>\nРежим: {chat["mode"]}; проверки: {"включены" if chat["enabled"] else "выключены"}',chat_menu(chat))
        elif cmd=='rights':
            cid=int(p[1]); me=await self.bot.get_me(); r=await self.bot.get_chat_member(cid,me.id)
            await self.show(c,f'Статус: {r.status}\nУдаление: {bool(getattr(r,"can_delete_messages",False))}\nОграничения: {bool(getattr(r,"can_restrict_members",False))}')
        elif cmd=='mode':
            cid=int(p[1]);mode=p[2]
            if not self.store.chat(cid):return
            if mode=='active':await self.confirm(c,{'op':'mode','chat_id':cid,'mode':'active'},'Включить автоматическое удаление по точным правилам? Поведенческие признаки продолжат отправляться на разбор.');return
            self.store.write('UPDATE chats SET enabled=?,mode=? WHERE chat_id=?',(int(mode!='off'),'observe',cid))
            self.store.audit(actor,cid,'mode',{'mode':mode})
            await self.show(c,'Настройка сохранена.',chat_menu(self.store.chat(cid)))
        elif cmd=='prompt':
            kind=p[1];cid=int(p[2])
            self.store.set_session(actor,{'kind':kind,'chat_id':cid})
            prompts={
                'check':'Пришлите текст или перешлите сообщение. Автор пересылки не блокируется. /cancel — отмена.',
                'rule':'Формат: слово | удалить | фраза\nТипы: слово, фраза, домен, ссылка. Действия: удалить, разбор.\nНапример: домен | удалить | spam.example\nПравило будет показано перед сохранением.',
                'exception':'Формат: слово | разрешённая фраза\nКатегории: слово, фраза, домен, поведение. Исключение относится только к выбранной категории.',
                'trusted':'Пришлите числовой Telegram ID пользователя для списка доверенных.',
                'blocked':'Пришлите числовой Telegram ID для списка блокировки. Добавление во всю сеть будет показано отдельно перед подтверждением.',
                'notify':'Уведомления приходят владельцам, которые нажали /start. /cancel — отмена.'}
            prompts['adcontact']='Пришлите контакт для рекламы: @username или номер телефона с кодом страны. Область: '+('вся сеть' if cid==0 else 'эта группа')+'.'
            await self.show(c,prompts.get(kind,'Введите значение.'))
        elif cmd=='rules':
            cid=int(p[1]);rules=self.store.rules(cid)
            size=15;pages=max(1,(len(rules)+size-1)//size)
            page=max(0,min(int(p[2]) if len(p)>2 else 0,pages-1));visible=rules[page*size:(page+1)*size]
            text=f'Правила: {len(rules)}. Страница {page+1}/{pages}\n'+ ('\n'.join(f'#{r["id"]} {r["kind"]}: {html.escape(r["pattern"])} → {LABELS[r["action"]]}' for r in visible) or 'Пока пусто.')
            rows=[[('Добавить правило',f'prompt:rule:{cid}')]]
            rows += [[(f'Изменить #{r["id"]}',f'ruleedit:{r["id"]}:{cid}'),(f'Выключить #{r["id"]}',f'ruledel:{r["id"]}:{cid}') ] for r in visible]
            navigation=[]
            if page: navigation.append(('← Назад',f'rules:{cid}:{page-1}'))
            if page+1<pages: navigation.append(('Далее →',f'rules:{cid}:{page+1}'))
            if navigation: rows.append(navigation)
            rows.append([('Главное меню','home')]);await self.show(c,text,keyboard(rows))
        elif cmd=='ruleedit':
            rid=int(p[1]);r=self.store.one('SELECT * FROM rules WHERE id=?',(rid,))
            if not r:return
            self.store.set_session(actor,{'kind':'rule','chat_id':r['chat_id'] or 0,'rule_id':rid})
            await self.show(c,f'Изменение правила #{rid}: {html.escape(r["pattern"])}.\nПришлите новый вариант: тип | действие | шаблон.\nТипы: слово, фраза, домен, ссылка; действия: удалить, разбор. /cancel — отмена.')
        elif cmd=='ruledel':
            rid=int(p[1]);cid=int(p[2]);r=self.store.one('SELECT * FROM rules WHERE id=?',(rid,))
            if not r:return
            await self.confirm(c,{'op':'ruledel','chat_id':cid,'rule_id':rid},f'Выключить правило #{rid}: {html.escape(r["pattern"])}?')
        elif cmd=='exceptions':
            cid=int(p[1]);items=self.store.exceptions(cid)
            size=15;pages=max(1,(len(items)+size-1)//size)
            page=max(0,min(int(p[2]) if len(p)>2 else 0,pages-1));visible=items[page*size:(page+1)*size]
            labels={'word':'слова','phrase':'фразы','domain':'домены','behavior':'поведение'}
            lines=[]
            for e in visible:
                scope=json.loads(e['scope_json']);r=self.store.one('SELECT pattern FROM rules WHERE id=?',(e['rule_id'],)) if e['rule_id'] else None
                target='только правило «'+r['pattern']+'»' if r else ', '.join(labels.get(k,k) for k in scope.get('categories',[]))
                value=e['value'][:160]+('…' if len(e['value'])>160 else '')
                lines.append(f'#{e["id"]}: «{html.escape(value)}» → {html.escape(target)}')
            text=f'Исключения: {len(items)}. Страница {page+1}/{pages}\n'+ ('\n'.join(lines) or 'Пока пусто.')
            rows=[[('Добавить исключение',f'prompt:exception:{cid}')]]+ [[(f'Удалить #{e["id"]}',f'exdel:{e["id"]}:{cid}')] for e in visible]
            navigation=[]
            if page:navigation.append(('← Назад',f'exceptions:{cid}:{page-1}'))
            if page+1<pages:navigation.append(('Далее →',f'exceptions:{cid}:{page+1}'))
            if navigation:rows.append(navigation)
            rows.append([('Главное меню','home')])
            await self.show(c,text,keyboard(rows))
        elif cmd=='exdel':await self.confirm(c,{'op':'exdel','chat_id':int(p[2]),'exception_id':int(p[1])},'Удалить это исключение?')
        elif cmd=='users':
            kind=p[1];cid=int(p[2]);items=self.store.rows('SELECT * FROM user_policies WHERE kind=? AND (chat_id IS NULL OR chat_id=?)',(kind,cid))
            title='Доверенные' if kind=='trusted' else 'Блокировки'
            text=title+':\n'+ ('\n'.join(f'<code>{i["user_id"]}</code> ({"вся сеть" if i["chat_id"] is None else "группа"})' for i in items[-20:]) or 'Список пуст.')
            rows=[[('Добавить',f'prompt:{kind}:{cid}')]]+ [[(f'Убрать {i["user_id"]}',f'userdel:{i["id"]}:{cid}')] for i in items[-15:]] + [[('Главное меню','home')]]
            await self.show(c,text,keyboard(rows))
        elif cmd=='userdel':
            item=self.store.one('SELECT * FROM user_policies WHERE id=?',(int(p[1]),))
            if item:await self.confirm(c,{'op':'userdel','chat_id':int(p[2]),'id':item['id']},'Удалить пользователя из списка? Уже применённый бан снимается отдельно в карточке события.')
        elif cmd in ('review','log'):
            cid=int(p[1])
            if cmd=='review':
                items=self.store.rows("SELECT e.* FROM moderation_events e JOIN review_queue q ON q.event_id=e.id WHERE q.status='pending' AND (e.chat_id=? OR ?=0) ORDER BY e.id DESC LIMIT 15",(cid,cid))
            else:items=self.store.rows('SELECT * FROM moderation_events WHERE chat_id=? OR ?=0 ORDER BY id DESC LIMIT 15',(cid,cid))
            text=('На разбор:' if cmd=='review' else 'Последние события:')+'\n'+ ('\n'.join(f'#{i["id"]}: {LABELS[i["intended_action"]]} / {html.escape(i["actual_result"])}' for i in items) or 'Пока пусто.')
            await self.show(c,text,keyboard([[(f'Открыть #{i["id"]}',f'event:{i["id"]}')] for i in items]+[[('Главное меню','home')]]))
        elif cmd=='event':
            event=self.store.one('SELECT * FROM moderation_events WHERE id=?',(int(p[1]),))
            if not event:return
            text=f'Событие #{event["id"]}\nГруппа: <code>{event["chat_id"]}</code>\nПользователь: <code>{event["user_id"]}</code>\nРешение: {LABELS[event["intended_action"]]}\nРезультат: {html.escape(event["actual_result"])}\n'+html.escape(event['reasons_json'])+'\n\n'+html.escape(event['excerpt'] or 'Фрагмент уже удалён по сроку хранения.')
            rows=[[('Оставить',f'resolve:{event["id"]}'),('Удалить',f'eventaction:{event["id"]}:delete')]]
            if event['user_id']:
                rows += [[('Ограничить 10 мин',f'eventaction:{event["id"]}:restrict'),('Бан',f'eventaction:{event["id"]}:ban')],
                         [('Разбанить',f'eventaction:{event["id"]}:unban'),('Снять ограничение',f'eventaction:{event["id"]}:unrestrict')],
                         [('Доверенный',f'eventtrust:{event["id"]}'),('Отметить ошибку',f'error:{event["id"]}')]]
            rows += [[('Пополнить словарь',f'learn:rule:{event["id"]}'),('Добавить исключение',f'learn:exception:{event["id"]}')],
                     [('Это реклама',f'learn:ad:{event["id"]}'),('Разрешить этот текст',f'learn:adallow:{event["id"]}')]]
            job=self.store.one('SELECT payload_json FROM action_jobs WHERE event_id=? AND status=\'done\' ORDER BY id DESC LIMIT 1',(event['id'],))
            if job:
                status=json.loads(job['payload_json']).get('ad_notice_result')
                if status:
                    label={'private':'личное сообщение','group':'в группе','cooldown':'недавно уже уведомляли','delivery_unknown':'доставка не подтверждена','rate_limited':'ограничение отправки Telegram','unavailable':'отправить не удалось'}.get(status,status)
                    text+='\n\nУведомление автору: '+html.escape(label)
            rows.append([('Главное меню','home')]);await self.show(c,text,keyboard(rows))
        elif cmd=='learn':
            kind=p[1];eid=int(p[2]);event=self.store.one('SELECT * FROM moderation_events WHERE id=?',(eid,))
            if not event or kind not in ('rule','exception','ad','adallow'):return
            cid=0 if kind=='rule' else event['chat_id']
            self.store.set_session(actor,{'kind':'learn_'+kind,'chat_id':cid,'event_id':eid})
            prompts={'rule':'Введите новый запрет: слово | меф или фраза | запрещённое предложение. Правило действует во всех группах. Для запрета используйте однозначное слово; фен/соль отдельно не запрещайте.',
                     'exception':'Пришлите точную допустимую фразу. Она освободит только совпавшие словесные правила в этой группе; другое запрещённое содержание сохранит приоритет.',
                     'ad':'Пришлите характерную фразу объявления. Она будет удаляться в этой группе с уведомлением автору о согласовании рекламы; без бана.',
                     'adallow':'Пришлите полный разрешённый текст объявления. Исключение будет действовать только для этого текста в этой группе; запрещённые темы оно не разрешает.'}
            await self.show(c,prompts[kind]+'\n\nСначала покажу изменение для подтверждения. /cancel — отмена.')
        elif cmd in ('resolve','error'):
            eid=int(p[1]);self.store.write("UPDATE review_queue SET status='resolved',resolved_by=?,resolved_at=? WHERE event_id=?",(actor,int(time.time()),eid))
            self.store.audit(actor,0,cmd,{'event_id':eid})
            rows=[[('Добавить исключение',f'learn:exception:{eid}'),('Разрешить рекламный текст',f'learn:adallow:{eid}')]] if cmd=='error' else [[('Пополнить словарь',f'learn:rule:{eid}')]]
            await self.show(c,'Отмечено. Чтобы изменить дальнейшую фильтрацию, сохраните правило или исключение.',keyboard(rows))
        elif cmd=='eventtrust':
            e=self.store.one('SELECT * FROM moderation_events WHERE id=?',(int(p[1]),))
            if e and e['user_id']:await self.confirm(c,{'op':'user','chat_id':e['chat_id'],'user_id':e['user_id'],'kind':'trusted'},'Добавить автора в доверенные для этой группы?')
        elif cmd=='eventaction':
            e=self.store.one('SELECT * FROM moderation_events WHERE id=?',(int(p[1]),));action=p[2]
            if not e or action not in ('delete','restrict','ban','unban','unrestrict'):return
            await self.confirm(c,{'op':'action','chat_id':e['chat_id'],'message_id':e['message_id'],'user_id':e['user_id'],'action':action,'event_id':e['id']},f'Подтвердить действие «{html.escape(action)}» для события #{e["id"]}?')
        elif cmd=='stats':
            cid=int(p[1]);counts=self.store.rows('SELECT actual_result,COUNT(*) AS n FROM moderation_events WHERE chat_id=? OR ?=0 GROUP BY actual_result',(cid,cid))
            await self.show(c,'Статистика за срок хранения:\n'+('\n'.join(f'{html.escape(i["actual_result"])}: {i["n"]}' for i in counts) or 'Нет событий.'))
        elif cmd=='settings':
            cid=int(p[1]);b=self.store.setting(f'behavior:{cid}',self.store.setting('behavior:0',self.policy['behavior']))
            auto=self.store.setting(f'behavior_auto:{cid}',False)
            rows=[[('Строже: 4/60, 2 повтора',f'preset:{cid}:strict'),('Мягче: 8/60, 4 повтора',f'preset:{cid}:gentle')]]
            if cid:rows.append([('Удалять по совокупности: '+('ВКЛ' if auto else 'ВЫКЛ'),f'behaviorauto:{cid}:{0 if auto else 1}')])
            ban=self.store.setting(f'autoban:{cid}',False)
            if cid:rows.append([('Автобан явной рекламы: '+('ВКЛ' if ban else 'ВЫКЛ'),f'autoban:{cid}:{0 if ban else 1}')])
            ads=self.store.setting(f'ad_enabled:{cid}',self.store.setting('ad_enabled:0',self.store.setting('advertising:0',{}).get('enabled',False)))
            rows.append([('Удалять несогласованную рекламу: '+('ВКЛ' if ads else 'ВЫКЛ'),f'ads:{cid}:{0 if ads else 1}')])
            rows.append([('Контакт для рекламы',f'prompt:adcontact:{cid}')])
            rows.append([('Главное меню','home')])
            contact=self.store.setting(f'ad_contact:{cid}',self.store.setting('ad_contact:0','администраторам группы'))
            await self.show(c,f'Поведение: частота {b["review_at_messages"]}/{b["rate_window_seconds"]} сек; повторы {b["review_at_duplicates"]}/{b["duplicate_window_seconds"]} сек.\nAI: выключен.\nАвтоматический бан по поведению: выключен.\nАвтобан явной запрещённой рекламы: {"включён" if ban else "выключен"}.\nУдаление по нескольким сильным признакам: {"включено" if auto else "выключено"}.\nНесогласованная реклама: {"удалять без бана" if ads else "автофильтр выключен"}.\nКонтакт: {html.escape(contact)}.\nНовая ссылка сама по себе отправляется на разбор.',keyboard(rows))
        elif cmd=='ads':
            cid=int(p[1]);enabled=bool(int(p[2]))
            await self.confirm(c,{'op':'ads','chat_id':cid,'enabled':enabled},'Изменить удаление несогласованной рекламы? В активном режиме объявление удаляется без бана, автору отправляется контакт администратора. В наблюдении наказаний нет.')
        elif cmd=='autoban':
            cid=int(p[1]);enabled=bool(int(p[2]))
            if not cid or not self.store.chat(cid):return
            await self.confirm(c,{'op':'autoban','chat_id':cid,'enabled':enabled},'Изменить автобан за явные запрещённые предложения? В активном режиме сообщение удаляется и автор блокируется. Одно слово без предложения — только удаление. Новости и предупреждения — на разбор.')
        elif cmd=='behaviorauto':
            cid=int(p[1]);enabled=bool(int(p[2]))
            await self.confirm(c,{'op':'behaviorauto','chat_id':cid,'enabled':enabled},'Изменить автоматическое удаление по совокупности признаков? Например, частые сообщения вместе с повторяющимся текстом. Применяется только в активном режиме; баны не назначаются.')
        elif cmd=='preset':
            cid=int(p[1]);strict=p[2]=='strict';b=dict(self.policy['behavior']);b.update(review_at_messages=4 if strict else 8,review_at_duplicates=2 if strict else 4)
            self.store.set_setting(f'behavior:{cid}',b);self.store.bump_policy(cid);self.store.audit(actor,cid,'behavior',b);await self.show(c,'Пороги сохранены. По поведению сообщения идут на разбор.')
        elif cmd=='status':
            me=await self.bot.get_me();chats=self.store.chats();pending=self.store.one("SELECT COUNT(*) n FROM action_jobs WHERE status='pending'")
            await self.show(c,f'@{me.username}\nГрупп: {len(chats)}\nПроверяют сообщения: {sum(x["enabled"] for x in chats)}\nОжидают исполнения: {pending["n"]}\nAI: выключен\nРазработчикельцев меню: {len(self.owners())}',home())
        elif cmd=='confirm':
            payload=self.store.consume_confirmation(p[1],actor)
            if not payload:await self.show(c,'Подтверждение устарело или уже использовано.');return
            await self.apply(c,payload,p[1])

    async def private(self,m:Message):
        if not m.from_user or not await self.permitted(m.from_user.id,m):return
        actor=m.from_user.id;state=self.store.session(actor)
        if not state:await m.answer('Выберите действие в меню.',reply_markup=home());return
        cid=state['chat_id'];kind=state['kind'];text=m.text or m.caption or ''
        if kind=='check':
            ctx=replace(self.context(m,True),chat_id=cid,user_id=None,joined_at=None,first_messages_seen=0,protected=False)
            await m.answer(self.verdict(self.engine.evaluate(ctx))+'\n\nЭто проверка без удаления и блокировки.')
            self.store.clear_session(actor);return
        try:
            if kind=='adcontact':
                value=text.strip()
                if not re.fullmatch(r'@[A-Za-z][A-Za-z0-9_]{4,31}|\+[\d ()-]{7,35}',value):raise ValueError
                payload={'op':'adcontact','chat_id':cid,'value':value}
                explanation='Контакт для рекламы: '+html.escape(value)+'. Сохранить?'
            elif kind in ('learn_rule','learn_ad'):
                if kind=='learn_rule':
                    fields=[x.strip() for x in text.split('|',1)]
                    if len(fields)!=2 or fields[0].casefold() not in ('слово','фраза'):raise ValueError
                    rule_type='word' if fields[0].casefold()=='слово' else 'phrase';pattern=fields[1]
                else:rule_type='phrase';pattern=text.strip()
                if not pattern or len(pattern)>300:raise ValueError
                if rule_type=='word' and any(c.isspace() for c in pattern):raise ValueError
                payload={'op':'learn_rule','chat_id':cid,'kind':rule_type,'pattern':normalize(pattern),
                         'action':'delete','advertising':kind=='learn_ad','event_id':state['event_id']}
                explanation=f'Добавить {"рекламный шаблон" if kind=="learn_ad" else "запрет"} «{html.escape(pattern)}»?\nОбласть: {"вся сеть" if cid==0 else "эта группа"}.\nДействие: удалить без бана'+(' и сообщить контакт администратора.' if kind=='learn_ad' else '. Маскировки нового слова тоже будут проверяться.')
                if kind=='learn_rule':explanation+='\nКритическое правило: явное предложение продажи/доставки/заказа также ведёт к бану при включённом автобане. Обычную рекламу добавляйте через «Это реклама».'
            elif kind in ('learn_exception','learn_adallow'):
                value=normalize(text)
                if not value or len(value)>2000:raise ValueError
                targets=[]
                if kind=='learn_exception':
                    if len(value.split())<2:
                        await m.answer('Исключение из одного слова слишком широкое. Пришлите точную допустимую фразу с контекстом.');return
                    targets=[r['id'] for r in self.store.rules(cid) if r['kind']=='word' and word_spans(value,r['pattern'],True)]
                    if not targets:
                        await m.answer('В этой фразе не найдено словесного правила. Для разрешения обычного объявления выберите «Разрешить этот текст»; для согласованного рекламодателя — «Доверенный».');return
                payload={'op':'learn_exception','chat_id':cid,'value':value,'rule_ids':targets,
                         'advertising':kind=='learn_adallow','event_id':state['event_id']}
                explanation='Добавить исключение для этой группы?\n'+html.escape(value[:350])+'\n'+('Разрешается только полный совпадающий рекламный текст. Запрещённые темы сохраняют приоритет.' if kind=='learn_adallow' else 'Только найденные словесные правила: '+', '.join(map(str,targets))+'. Другая запрещённая часть сообщения не разрешается.')
            elif kind in ('trusted','blocked'):
                uid=int(text.strip())
                if uid<=0:raise ValueError
                payload={'op':'user','chat_id':cid,'user_id':uid,'kind':kind}
                title='доверенные' if kind=='trusted' else 'блокировки'
                scope='вся подключённая сеть' if cid==0 else str(cid)
                explanation=f'Добавить ID {uid} в {title}? Область: {scope}.'
                if kind=='blocked':explanation+=' После подтверждения будет применён бан в выбранных группах.'
            elif kind=='rule':
                fields=[x.strip() for x in text.split('|',2)]
                if len(fields)!=3:raise ValueError
                types={'слово':'word','фраза':'phrase','домен':'domain','ссылка':'url'};actions={'удалить':'delete','разбор':'review'}
                rule_type=types[fields[0].casefold()];action=actions[fields[1].casefold()];pattern=fields[2]
                if not pattern or len(pattern)>300:raise ValueError
                if rule_type=='domain' and (not domain(pattern.lstrip('*.')) or '/' in pattern or ' ' in pattern):raise ValueError
                payload={'op':'rule','chat_id':cid,'kind':rule_type,'pattern':pattern,'action':action}
                if state.get('rule_id'):payload['rule_id']=state['rule_id']
                explanation=f'Сохранить правило: {html.escape(pattern)} → {LABELS[action]}?\nОбласть: {"все группы" if cid==0 else cid}. Сначала проверьте его на допустимых рекламных обращениях.'
            elif kind=='exception':
                fields=[x.strip() for x in text.split('|',1)]
                if len(fields)!=2 or not fields[1] or len(fields[1])>300:raise ValueError
                categories={'слово':'word','фраза':'phrase','домен':'domain','поведение':'behavior'}
                category=categories[fields[0].casefold()]
                payload={'op':'exception','chat_id':cid,'category':category,'value':fields[1]}
                explanation=f'Разрешённая фраза: {html.escape(fields[1])}. Исключение действует только для категории {category}. Сохранить?'
            else:raise ValueError
        except (ValueError,KeyError):
            await m.answer('Не удалось разобрать ввод. Проверьте формат либо /cancel.');return
        key=self.store.confirmation(actor,payload);self.store.clear_session(actor)
        await m.answer(explanation,reply_markup=keyboard([[('Подтвердить',f'confirm:{key}'),('Отмена','cancel')]]))

    async def apply(self,c:CallbackQuery,payload:dict,key:str):
        actor=c.from_user.id;cid=payload['chat_id'];op=payload['op']
        if cid and not self.store.chat(cid):await self.show(c,'Группа не подключена.');return
        if op=='mode':
            self.store.write("UPDATE chats SET enabled=1,mode='active' WHERE chat_id=?",(cid,))
            await self.show(c,'Активный режим включён. По поведению — ручной разбор; по точным правилам — заданное действие.')
        elif op=='adcontact':
            self.store.set_setting(f'ad_contact:{cid}',payload['value'])
            await self.show(c,'Контакт для рекламы сохранён.')
        elif op=='ads':
            self.store.set_setting(f'ad_enabled:{cid}',payload['enabled']);self.store.bump_policy(cid)
            await self.show(c,'Удаление несогласованной рекламы '+('включено' if payload['enabled'] else 'выключено')+'.')
        elif op=='behaviorauto':
            self.store.set_setting(f'behavior_auto:{cid}',payload['enabled']);self.store.bump_policy(cid)
            await self.show(c,'Настройка сохранена. Она действует только в активном режиме.')
        elif op=='autoban':
            self.store.set_setting(f'autoban:{cid}',payload['enabled']);self.store.bump_policy(cid)
            await self.show(c,'Автобан запрещённых предложений '+('включён' if payload['enabled'] else 'выключен')+'. Работает только в активном режиме.')
        elif op in ('rule','learn_rule'):
            if payload.get('rule_id'):
                rid=payload['rule_id']
                self.store.write('UPDATE rules SET kind=?,pattern=?,action=?,enabled=1 WHERE id=?',
                                 (payload['kind'],payload['pattern'],payload['action'],rid))
            else:
                rid=self.store.write('INSERT INTO rules(chat_id,kind,pattern,action,created_by,created_at) VALUES(?,?,?,?,?,?)',
                                    (cid or None,payload['kind'],payload['pattern'],payload['action'],actor,int(time.time())))
            self.store.bump_policy(cid);await self.show(c,f'Правило #{rid} сохранено.')
            if op=='learn_rule':
                if payload['advertising']:
                    ids=self.store.setting('ad_rule_ids:0',[])
                    self.store.set_setting('ad_rule_ids:0',list(dict.fromkeys([*ids,rid])))
                elif payload['kind']=='word':
                    words=self.store.setting('learned_words:0',[])
                    self.store.set_setting('learned_words:0',list(dict.fromkeys([*words,normalize(payload['pattern'])])))
                if not payload['advertising']:
                    ids=self.store.setting('learned_critical_rules:0',[])
                    self.store.set_setting('learned_critical_rules:0',list(dict.fromkeys([*ids,rid])))
                if payload['advertising']:
                    event=self.store.one('SELECT * FROM moderation_events WHERE id=?',(payload['event_id'],))
                    if event:
                        ctx=MessageContext(cid,event['message_id'],event['user_id'],event['excerpt'] or '',int(time.time()))
                        data={'chat_id':cid,'message_id':event['message_id'],'user_id':event['user_id'],'action':'delete',
                              'actor_id':actor,'ad_notice':self.advertising_notice(self.store.chat(cid),ctx)}
                        result=await self.executor.run(data,f'confirm:{key}',event['id'])
                        await self.show(c,'Обработка текущего объявления: '+result+'.')
        elif op=='learn_exception':
            targets=[None] if payload['advertising'] else payload['rule_ids']
            for rid in targets:
                scope={'categories':['advertising'],'match_mode':'whole_message'} if payload['advertising'] else {'categories':[],'match_mode':'covered_occurrences'}
                self.store.write('INSERT INTO rule_exceptions(chat_id,rule_id,kind,value,scope_json,created_by,created_at) VALUES(?,?,?,?,?,?,?)',
                                 (cid or None,rid,'phrase',payload['value'],json.dumps(scope),actor,int(time.time())))
            self.store.bump_policy(cid)
            await self.show(c,'Исключение сохранено. Удалённое сообщение Telegram автоматически не восстанавливает.')
        elif op=='ruledel':
            self.store.write('UPDATE rules SET enabled=0 WHERE id=?',(payload['rule_id'],));self.store.bump_policy(cid);await self.show(c,'Правило выключено.')
        elif op=='exception':
            self.store.write('INSERT INTO rule_exceptions(chat_id,kind,value,scope_json,created_by,created_at) VALUES(?,?,?,?,?,?)',
                             (cid or None,'phrase',payload['value'],json.dumps({'categories':[payload['category']]}),actor,int(time.time())))
            self.store.bump_policy(cid);await self.show(c,'Исключение сохранено.')
        elif op=='exdel':
            self.store.write('DELETE FROM rule_exceptions WHERE id=?',(payload['exception_id'],));self.store.bump_policy(cid);await self.show(c,'Исключение удалено.')
        elif op=='user':
            uid=payload['user_id']
            if uid in self.owners() and payload['kind']=='blocked':await self.show(c,'Владельца бота блокировать нельзя.');return
            chats=self.store.chats() if not cid else [self.store.chat(cid)]
            if payload['kind']=='blocked':
                results=[]
                # Protect administrators before persisting a network block.
                protected=[chat for chat in chats if await self.is_protected(chat['chat_id'],uid)]
                if protected:await self.show(c,'Пользователь — администратор либо права не удалось проверить. Блокировка не добавлена.');return
                self.store.set_user_policy(cid,uid,'blocked',actor)
                for chat in chats:
                    result=await self.executor.run({'chat_id':chat['chat_id'],'user_id':uid,'action':'ban','actor_id':actor},f'confirm:{key}:{chat["chat_id"]}')
                    results.append(f'{html.escape(chat["title"])}: {result}')
                await self.show(c,'Пользователь в списке блокировки.\n'+'\n'.join(results))
            else:self.store.set_user_policy(cid,uid,'trusted',actor);await self.show(c,'Пользователь добавлен в доверенные. Существующие баны это не снимает.')
        elif op=='userdel':
            row=self.store.one('SELECT * FROM user_policies WHERE id=?',(payload['id'],))
            if row:self.store.write('DELETE FROM user_policies WHERE id=?',(row['id'],));self.store.bump_policy(row['chat_id'] or 0)
            await self.show(c,'Запись удалена. Снятие ранее применённого бана — отдельное действие.')
        elif op=='action':
            data={**payload,'actor_id':actor}
            result=await self.executor.run(data,f'confirm:{key}',payload['event_id'])
            if result in ('success','already_done'):
                self.store.write("UPDATE review_queue SET status='resolved',resolved_by=?,resolved_at=? WHERE event_id=?",(actor,int(time.time()),payload['event_id']))
            await self.show(c,'Результат: '+result)
        self.store.audit(actor,cid,op,{k:v for k,v in payload.items() if k!='text'})
