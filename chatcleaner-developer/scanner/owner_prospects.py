"""Manual review of a named recipient in an original public request.

This route only opens a conversation. It never sends, enables AI, joins a
community, or treats a channel/aggregator as the customer's personal account.
"""
import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException
from pydantic import BaseModel, Field
from telethon import types, utils, functions

from engine import now, matches
from conversations import error_text, STOP


class ReviewedRequest(BaseModel):
    account_id: int = Field(gt=0)
    source_id: int
    message_id: int = Field(gt=0)
    username: str = Field(pattern=r'^@?[A-Za-z][A-Za-z0-9_]{3,31}$')
    quote: str = Field(min_length=30, max_length=4000)
    rule_id: int = Field(gt=0)
    confirmed: bool = False


def initialize(inbox):
    inbox.db.execute('''CREATE TABLE IF NOT EXISTS owner_prospect_reviews(
        id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL,
        username TEXT NOT NULL, source_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        quote TEXT NOT NULL, conversation_id INTEGER NOT NULL, checked TEXT NOT NULL,
        UNIQUE(account_id,peer_id,source_id,message_id))''')
    inbox.db.commit()


async def delivery_options(inbox, cid):
    """Read the selected peer's delivery restrictions, without paying or sending."""
    account = inbox.account()
    conversation = inbox.get(cid, True)
    client = inbox.runtime.client
    peer = types.InputPeerUser(conversation['peer_id'], int(conversation['access_hash']))
    try:
        result = await asyncio.wait_for(client(functions.users.GetFullUserRequest(peer)), 20)
    except Exception as exc:
        raise HTTPException(409, 'Проверка ограничений не завершена: ' + error_text(exc)) from None
    if inbox.runtime.client is not client or inbox.account() != account or result.full_user.id != conversation['peer_id']:
        raise HTTPException(409, 'Аккаунт или адресат изменился во время проверки.')
    full = result.full_user
    return {'account_id': account, 'peer_id': conversation['peer_id'], 'username': conversation['username'],
            'blocked': bool(full.blocked), 'contact_require_premium': bool(getattr(full, 'contact_require_premium', False)),
            'send_paid_messages_stars': getattr(full, 'send_paid_messages_stars', None),
            'sender_premium': bool(getattr(inbox.runtime.me, 'premium', False)), 'checked_at': now(),
            'sent': False, 'paid': False}


async def open_reviewed(inbox, spec: ReviewedRequest):
    async with inbox.lock:
        account=inbox.account()
        if not spec.confirmed or spec.account_id!=account:
            raise HTTPException(409,'Подтвердите свой аккаунт и контакт из исходного запроса.')
        rule=inbox.rule(spec.rule_id)
        sources=inbox.store.rows('SELECT * FROM sources WHERE id=? AND enabled=1',(spec.source_id,))
        if not sources or not sources[0]['username'] or spec.source_id not in rule['sources'] or not rule['enabled']:
            raise HTTPException(409,'Выберите публичный источник с включённой тематикой.')
        source=sources[0]
        username=spec.username.lstrip('@')
        quote=spec.quote.strip()
        if not re.search(r'(?<![\w])(?:@|https?://t\.me/)'+re.escape(username)+r'(?![\w])',quote,re.I):
            raise HTTPException(409,'В выбранном фрагменте нет этого точного контакта.')
        if not matches(quote,rule):
            raise HTTPException(409,'Фрагмент не соответствует запросу проектных услуг выбранной тематики.')
        client=inbox.runtime.client
        try:
            entity_source=await asyncio.wait_for(client.get_entity(source['username']),20)
            if not isinstance(entity_source,(types.Channel,types.Chat)) or utils.get_peer_id(entity_source)!=spec.source_id or getattr(entity_source,'creator',False) or getattr(entity_source,'admin_rights',None):
                raise HTTPException(409,'Исходный публичный источник не подтверждён.')
            post=await asyncio.wait_for(client.get_messages(entity_source,ids=spec.message_id),20)
            if not post or quote not in (post.message or ''):
                raise HTTPException(409,'Фрагмент не найден в текущем исходном сообщении. Повторно проверьте пост.')
            if post.date<datetime.now(timezone.utc)-timedelta(days=7):
                raise HTTPException(409,'Запрос старше семи дней. Сначала подтвердите актуальность.')
            recipient=await asyncio.wait_for(client.get_entity(username),20)
            if not isinstance(recipient,types.User) or (recipient.username or '').casefold()!=username.casefold() or recipient.id==account or recipient.bot or recipient.deleted:
                raise HTTPException(409,'Контакт не является подтверждённым личным профилем заказчика.')
            peer=await client.get_input_entity(recipient)
            if not isinstance(peer,types.InputPeerUser):
                raise HTTPException(409,'Личный контакт недоступен.')
            # A server-side Telegram search checks older outgoing messages too,
            # rather than concluding "new contact" from the last 30 messages.
            outgoing=await asyncio.wait_for(client.get_messages(peer,limit=1,from_user='me'),20)
            if outgoing:
                raise HTTPException(409,'Ты уже писал этому человеку. Повторное предложение не подготовлено.')
            history=await asyncio.wait_for(client.get_messages(peer,limit=30),20)
            if any(m.out for m in history):
                raise HTTPException(409,'В переписке уже есть твоё исходящее сообщение. Контакт исключён.')
            if any(STOP.search(m.message or '') for m in history if not m.out):
                raise HTTPException(409,'В переписке есть отказ от сообщений. Контакт исключён.')
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(409,'Проверка контакта не завершена: '+error_text(exc)) from None
        if inbox.runtime.client is not client or inbox.account()!=account:
            raise HTTPException(409,'Отправитель изменился во время проверки.')
        existing=inbox.store.rows('SELECT id,stopped FROM conversations WHERE account_id=? AND peer_id=?',(account,recipient.id))
        if existing:
            cid=existing[0]['id']
            if existing[0]['stopped'] or inbox.db.execute("SELECT 1 FROM conversation_messages WHERE cid=? AND direction='outgoing' AND state IN ('pending','sending','sent','uncertain')",(cid,)).fetchone():
                raise HTTPException(409,'Для контакта уже подготовлено предложение или получен отказ. Повтор исключён.')
        else:
            inbox.store.put('conversation_last_account',account)
            fingerprint=hashlib.sha256(f'reviewed:{spec.source_id}:{spec.message_id}:{quote}'.encode()).hexdigest()
            with inbox.db:
                inbox.db.execute('INSERT OR IGNORE INTO leads(chat_id,message_id,text,author,sender_id,link,created,found,fingerprint,matches,status,origin) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                    (spec.source_id,spec.message_id,quote,'@'+source['username'],post.sender_id,'https://t.me/'+source['username']+'/'+str(spec.message_id),post.date.isoformat(),now(),fingerprint,json.dumps([{'id':rule['id'],'name':rule['name']}],ensure_ascii=False),'working','manual_prospect'))
                lid=inbox.db.execute('SELECT id FROM leads WHERE chat_id=? AND message_id=?',(spec.source_id,spec.message_id)).fetchone()[0]
                cur=inbox.db.execute('INSERT INTO conversations(account_id,peer_id,access_hash,title,username,lead_id,rule_id,status,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (account,recipient.id,str(peer.access_hash),recipient.first_name or recipient.username,recipient.username,lid,rule['id'],'new',now(),now()))
                cid=cur.lastrowid
                inbox.db.execute('INSERT INTO conversation_leads VALUES(?,?)',(cid,lid))
            conversation=inbox.get(cid)
            for message in reversed(history):
                await inbox.ingest(conversation,message,initial=True)
                with inbox.db:inbox.db.execute('UPDATE conversations SET cursor=MAX(cursor,?) WHERE id=?',(message.id,cid))
            with inbox.db:inbox.db.execute('UPDATE conversations SET last_sync=? WHERE id=?',(now(),cid))
        with inbox.db:
            inbox.db.execute('INSERT OR IGNORE INTO owner_prospect_reviews(account_id,peer_id,username,source_id,message_id,quote,conversation_id,checked) VALUES(?,?,?,?,?,?,?,?)',
                (account,recipient.id,recipient.username,spec.source_id,spec.message_id,quote,cid,now()))
        result=await inbox.detail(cid)
        result['reviewed_request']={'source_url':'https://t.me/'+source['username']+'/'+str(spec.message_id),'quote':quote,'contact_username':recipient.username,'previous_outgoing_found':False,'manual_only':True}
        return result
