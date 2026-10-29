"""Inspect a shared Telegram folder without joining or changing scanner sources."""
import asyncio
import re
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from telethon import errors, functions, types, utils


class FolderPreview(BaseModel):
    url: str = Field(min_length=1, max_length=300)


def folder_slug(url):
    try:
        parsed = urlsplit(url.strip())
    except ValueError:
        parsed = None
    if (not parsed or parsed.scheme != 'https' or parsed.netloc != 't.me'
            or parsed.query or parsed.fragment
            or not re.fullmatch(r'/addlist/[A-Za-z0-9_-]{1,128}/?', parsed.path)):
        raise HTTPException(400, 'Укажите ссылку папки вида https://t.me/addlist/…')
    return parsed.path.rstrip('/').rsplit('/', 1)[-1]


class SourceFolders:
    def __init__(self, get_runtime, get_store):
        self.get_runtime, self.get_store = get_runtime, get_store
        self.router = APIRouter(prefix='/api/scanner/source-folders')
        self.router.add_api_route('/preview', self.preview, methods=['POST'])

    async def preview(self, spec: FolderPreview):
        slug = folder_slug(spec.url)
        runtime = self.get_runtime()
        client = getattr(runtime, 'client', None)
        me = getattr(runtime, 'me', None)
        if not client or not me or not client.is_connected():
            raise HTTPException(409, 'Сначала подключите Telegram-аккаунт в лидсканере.')
        account_id = me.id
        try:
            result = await asyncio.wait_for(client(functions.chatlists.CheckChatlistInviteRequest(slug)), 30)
        except (errors.InviteHashEmptyError, errors.InviteHashExpiredError, errors.InviteHashInvalidError):
            raise HTTPException(404, 'Папка не найдена или ссылка больше не действует. Попросите новую ссылку.') from None
        except errors.FloodWaitError as exc:
            raise HTTPException(409, f'Telegram просит подождать {exc.seconds} секунд перед проверкой папки.') from None
        except errors.RPCError as exc:
            # This Telethon version does not have generated INVITE_SLUG_* error classes.
            if getattr(exc, 'message', '') in ('INVITE_SLUG_EMPTY', 'INVITE_SLUG_EXPIRED', 'INVITE_SLUG_INVALID'):
                raise HTTPException(404, 'Папка не найдена или ссылка больше не действует. Попросите новую ссылку.') from None
            raise HTTPException(409, 'Telegram не разрешил проверить папку. Проверьте доступ аккаунта и повторите позже.') from None
        except (TimeoutError, OSError):
            raise HTTPException(409, 'Telegram не ответил при проверке папки. Повторите позже.') from None
        current = self.get_runtime()
        if (getattr(current, 'client', None) is not client
                or getattr(getattr(current, 'me', None), 'id', None) != account_id
                or not client.is_connected()):
            raise HTTPException(409, 'Telegram-аккаунт изменился во время проверки. Проверьте папку заново.')
        if isinstance(result, types.chatlists.ChatlistInvite):
            title = result.title.text
            peers = result.peers
        elif isinstance(result, types.chatlists.ChatlistInviteAlready):
            # CheckChatlistInvite does not return the title of an already imported folder.
            title = 'Папка Telegram уже подключена'
            peers = [*result.already_peers, *result.missing_peers]
        else:
            raise HTTPException(409, 'Telegram вернул неполные данные папки. Повторите проверку позже.')
        configured = {row['id'] for row in self.get_store().rows('SELECT id FROM sources')}
        chats = {utils.get_peer_id(chat): chat for chat in result.chats}
        sources, seen = [], set()
        for peer in peers:
            if not isinstance(peer, (types.PeerChat, types.PeerChannel)):
                continue
            chat_id = utils.get_peer_id(peer)
            if chat_id in seen:
                continue
            seen.add(chat_id)
            chat = chats.get(chat_id)
            managed = bool(getattr(chat, 'creator', False) or getattr(chat, 'admin_rights', None))
            username = getattr(chat, 'username', None)
            available = isinstance(chat, (types.Chat, types.Channel))
            inactive = bool(getattr(chat, 'deactivated', False) or getattr(chat, 'migrated_to', None))
            joined = available and not bool(getattr(chat, 'left', False))
            candidate = bool(available and not inactive and not managed and (username or joined))
            if managed:
                note = 'Управляемая аккаунтом площадка: исключена из внешних источников лидсканера.'
            elif not available:
                note = 'Telegram не предоставил доступные данные группы. Чтение сообщений не подтверждено.'
            elif inactive:
                note = 'Группа закрыта или перенесена; выберите действующий источник.'
            elif not candidate:
                note = 'Закрытая группа: сначала получите доступ через Telegram. Панель не вступает автоматически.'
            else:
                note = 'Кандидат в источник. Чтение сообщений проверяется отдельно при подключении; папка не добавлена.'
            sources.append(dict(id=chat_id, title=getattr(chat, 'title', None) or 'Недоступная группа',
                                username=username, already_configured=chat_id in configured,
                                kind=('group' if isinstance(chat, types.Chat) or bool(getattr(chat, 'megagroup', False)) else 'channel') if available else 'unknown',
                                managed=managed, readable_candidate=candidate, access_note=note))
        return dict(slug=slug, title=title, sources=sources)
