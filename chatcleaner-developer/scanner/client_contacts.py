"""Conservative, client-only contact evidence for preserved scanner candidates.

This module has no storage, Telegram calls, owner state, or sending side effects.
Pass a server-read conversation and the CURRENT connected account_id to recognize
an author previously resolved and synchronized by Conversations.open_lead. A name,
username, positive sender ID, or contact in the message body is not that proof.
Even a confirmed author is not a confirmed customer or permission to contact them.
"""
from collections.abc import Mapping
import re


_FORWARD_FIELDS = ('forwarded', 'is_forwarded', 'fwd_from', 'forward_origin',
                   'forward_from', 'forward_from_chat')
_REPOST_HINT = re.compile(
    r'^\s*(?:репост(?:\s|[:.!]|$)|переслан(?:о|ное сообщение)\s+(?:из|от)\b|'
    r'forwarded\s+from\b)|\b(?:заявка|заказ|публикация)\s+с\s+биржи\b',
    re.I | re.M,
)
_AGGREGATOR_TITLE_HINT = re.compile(
    r'\b(?:биржа\s+(?:заказов|фриланса|фрилансеров)|'
    r'агрегатор\s+(?:заявок|заказов|вакансий)|'
    r'freelance\s+(?:job\s+aggregator|exchange))\b', re.I,
)
_MARKETPLACE_LINK = re.compile(
    r'(?<![\w.-])(?:https?://)?(?:[a-z0-9-]+\.)*'
    r'(?:kwork\.ru|kwork\.com|fl\.ru|freelance\.ru|upwork\.com|freelancer\.com)'
    r'(?![\w.-])', re.I,
)
_BODY_USERNAME = re.compile(r'(?<![\w@])@[A-Za-z][A-Za-z0-9_]{3,31}(?![\w@])')

_REASONS = {
    'missing_personal_author': 'Telegram не сохранил подтверждаемый личный ID автора. Контакт заказчика недоступен.',
    'own_account': 'Автором указан подключённый аккаунт. Это не контакт потенциального заказчика.',
    'bot_or_deleted_author': 'Автор отмечен как бот или удалённый аккаунт. Личный контакт заказчика не подтверждён.',
    'personal_author_unverified': 'Есть личный ID автора сообщения, но адресат ещё не проверен через переписку. Автор может оказаться посредником или автором репоста.',
    'conversation_author_verified': 'Личный автор ранее проверен и история синхронизирована для подключённого аккаунта. Это подтверждает адресата, но не его роль заказчика.',
    'forwarded_message': 'У сообщения есть метаданные пересылки. Отправитель репоста не считается подтверждённым заказчиком.',
    'repost_text_hint': 'В тексте есть явная пометка о репосте или заказе с биржи. Это признак для ручной проверки, а не определение роли автора.',
    'aggregator_source': 'Источник отмечен как агрегатор или биржа. Личный контакт заказчика требует отдельной проверки.',
    'aggregator_title_hint': 'Название источника содержит признак биржи или агрегатора. Это текстовая подсказка для проверки, а не точная классификация источника.',
    'marketplace_link': 'В тексте есть ссылка на биржу. Она не подтверждает Telegram-контакт заказчика.',
    'body_contact_unverified': 'Username в тексте сообщения не считается автором или подтверждённым контактом заказчика.',
    'customer_not_verified': 'Актуальность запроса и принадлежность заявки заказчику ещё нужно подтвердить. Находка сохраняется как кандидат.',
}
_REVIEW_CODES = {'forwarded_message', 'repost_text_hint', 'aggregator_source',
                 'aggregator_title_hint', 'marketplace_link'}
_LABELS = {'no_contact': 'Нет подтверждённого контакта',
           'needs_review': 'Нужна проверка контакта заказчика',
           'confirmed_author': 'Личный автор подтверждён'}


def _personal_id(value):
    # bool is an int in Python, but is not a Telegram user ID.
    return type(value) is int and 0 < value < 2**63


def _conversation_matches(lead, conversation, account_id):
    """Only a matching server-read conversation is evidence; ignore lead flags."""
    if not isinstance(conversation, Mapping) or not _personal_id(account_id):
        return False
    if (not _personal_id(conversation.get('account_id')) or
            not _personal_id(conversation.get('peer_id')) or
            conversation['account_id'] != account_id or
            conversation['peer_id'] != lead.get('sender_id')):
        return False
    synced = conversation.get('last_sync')
    if not isinstance(synced, str) or not synced.strip():
        return False
    lead_id = lead.get('id')
    if not _personal_id(lead_id):
        return False
    original = conversation.get('lead_id')
    if _personal_id(original) and original == lead_id:
        return True
    linked = conversation.get('linked_lead_ids', ())
    return (isinstance(linked, (list, tuple, set, frozenset)) and
            any(_personal_id(value) and value == lead_id for value in linked))


def annotate_client_contact(lead, source=None, *, conversation=None, account_id=None):
    """Return a shallow lead copy with a new ``client_contact`` evidence mapping.

    ``source`` may supply title, kind=aggregator/marketplace, or is_aggregator=True.
    Optional forward metadata on ``lead`` improves review hints; its absence never
    proves that a message is original. ``conversation`` must come from the client
    database, not a browser payload, and provide account_id, peer_id, lead_id (or
    linked_lead_ids), and last_sync. account_id is the current runtime account.
    All statuses retain customer_verified=False and ready=False: this is contact
    evidence, not customer qualification, permission, or an automation switch.
    """
    source = source if isinstance(source, Mapping) else {}
    text = lead.get('text') if isinstance(lead.get('text'), str) else ''
    codes = []
    sender_id = lead.get('sender_id')
    personal = _personal_id(sender_id)
    if not personal:
        codes.append('missing_personal_author')
    elif _personal_id(account_id) and sender_id == account_id:
        personal = False
        codes.append('own_account')
    elif lead.get('sender_is_bot') is True or lead.get('sender_deleted') is True:
        personal = False
        codes.append('bot_or_deleted_author')

    verified = personal and _conversation_matches(lead, conversation, account_id)
    if personal:
        codes.append('conversation_author_verified' if verified else 'personal_author_unverified')
    if any(lead.get(field) for field in _FORWARD_FIELDS):
        codes.append('forwarded_message')
    if _REPOST_HINT.search(text):
        codes.append('repost_text_hint')
    kind = source.get('kind')
    if source.get('is_aggregator') is True or (isinstance(kind, str) and
                                             kind.casefold() in ('aggregator', 'marketplace')):
        codes.append('aggregator_source')
    title = source.get('title')
    if isinstance(title, str) and _AGGREGATOR_TITLE_HINT.search(title):
        codes.append('aggregator_title_hint')
    if _MARKETPLACE_LINK.search(text):
        codes.append('marketplace_link')
    if _BODY_USERNAME.search(text):
        codes.append('body_contact_unverified')
    codes.append('customer_not_verified')
    status = ('no_contact' if not personal else
              'confirmed_author' if verified and not _REVIEW_CODES.intersection(codes) else
              'needs_review')
    contact = {'status': status, 'label': _LABELS[status], 'reason_codes': codes,
               'reasons': [_REASONS[code] for code in codes],
               'author_verified': bool(verified), 'customer_verified': False, 'ready': False}
    return {**lead, 'client_contact': contact}
