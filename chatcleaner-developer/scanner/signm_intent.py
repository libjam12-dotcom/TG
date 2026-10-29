"""Explainable, local buyer-intent filter for the opted-in personal SignM profile.

This only evaluates message text. Identity, geography, freshness and permission to
contact remain separate evidence; a direct request is never proof of payment.
"""
import re


PROFILE = 'signm_dubai_v1'

SERVICE_PATTERNS = {
    'websites': r'сайт\w*|лендинг\w*|тильд\w*|джумл\w*|website\w*|web[ -]?(?:developer|development|design)\w*|landing(?: page)?|wordpress|joomla|tilda',
    'ecommerce': r'интернет[ -]магазин\w*|битрикс(?!\s*24)|bitrix(?!\s*24)|shopify|woocommerce|e[ -]?commerce|online store',
    'smm': r'smm|смм\w*|соцсет\w*|социальн\w*\s+сет\w*|инстаграм\w*|instagram|social media(?:\s+\w+){0,2}|контент[ -](?:менеджер|план)\w*|рилсмейкер\w*',
    'ads': r'авитолог(?:а|у|ом|е|и|ов|ам|ами|ах)?|директолог\w*|таргетолог\w*|директ|таргет|контекстн\w*\s+реклам\w*|(?:google|meta|facebook|instagram|telegram|yandex|tiktok)\s+(?:ads|direct)|ppc|paid (?:ads|advertising)|media buyer',
    'seo': r'seo|сео|сеошник\w*|аудит\w*\s+сайт\w*|продвижени\w*\s+сайт\w*|продвинуть\s+сайт\w*|search engine optim(?:ization|isation)|website audit|site audit',
    'bots': r'(?:telegram|телеграм|тг|чат|chat)[ -]?бот\w*|telegram[ -]?bot\w*|chatbot\w*|chat bot\w*|бот(?:а|ы|ов|у|ом)?',
    'crm': r'crm|срм|amocrm|amo\s*crm|амосрм|амо|битрикс\s*24|bitrix\s*24|воронк\w*\s+продаж\w*|lead management',
    'automation': r'автоматизац\w*|автоматизир\w*|интеграц\w*|интегрир\w*|integrat\w*|n8n|zapier|make\.com|ai[ -](?:agent|assistant|automation)\w*|ии[ -]?(?:агент|ассистент|бот)\w*|нейросет\w*|business automation|workflow automation',
    'marketing': r'маркетолог\w*|маркетингов\w*\s+(?:агентств\w*|стратег\w*)|лидогенерац\w*|привлечени\w*\s+клиент\w*|digital marketing|marketing (?:specialist|agency)|lead generation',
}


def _pattern(value):
    return re.compile(r'(?<!\w)(?:' + value + r')(?!\w)', re.I)


SERVICES = {key: _pattern(value) for key, value in SERVICE_PATTERNS.items()}
REQUEST = _pattern(
    r'ищу|ищем|ищет|ишу|нужен|нужна|нужно|нужны|требуется|требуются|хочу|хотим|'
    r'посоветуйте|порекомендуйте|подскажите|закажу|заказать|сколько стоит|'
    r'кто\s+(?:может|занимается|поможет|сможет|настраивает|делает|разрабатывает)|'
    r'есть\s+ли\s+(?:(?:тут|здесь)\s+)?специалист\w*|'
    r'looking\s+for|searching\s+for|seeking|need(?:ed)?|require|recommend(?:ations)?|'
    r'can\s+(?:anyone|someone)|who\s+(?:can|does)')
OFFER = _pattern(
    r'предлагаю|предлагаем|наши услуги|мои услуги|беру\s+(?:новые\s+)?проекты|'
    r'(?:наша\s+компания|наше\s+агентство|наша\s+команда)\s+предлагает|'
    r'ищ(?:у|ем)\s+(?:(?:\d+(?:\s*[–—-]\s*\d+)?|один|два|три|несколько|'
    r'новые|небольшие|новый|небольшой|новых|небольших)\s+){0,3}'
    r'(?:проекты|проект|проекта|проектов|клиентов|заказы|заказов|заказчиков|работу)|'
    # An avitolog asking for their own work/clients is a provider even without
    # a CTA. Do not match a buyer's "нужен авитолог/сайт" procurement clause.
    r'(?:я\s+(?:(?:опытный|профессиональный|частный)\s+)?авитолог|работаю\s+авитологом)'
    r'\b[^.!?;\n]{0,120}\b(?:нужен\s+(?:новый\s+)?(?:проект|клиент|заказ)|'
    r'нужны\s+(?:новые\s+)?(?:проекты|клиенты|заказы)|нужна\s+(?:новая\s+)?работа)(?![-\w])|'
    r'we offer|our services|my services|looking for\s+(?:(?:new|small|\d+(?:[–—-]\d+)?)\s+){0,3}(?:projects|clients|work|gigs)|'
    r'научу|обучаю|записывайтесь на (?:курс|обучение)|набор на курс')
AVAILABILITY_OFFER = _pattern(
    r'(?:i(?:\s+am|[’\x27]m)|we(?:\s+are|[’\x27]re))\s+(?:currently\s+)?available\s+for\s+'
    # A provider's availability for services/work differs from a buyer being
    # available for a meeting or to discuss a project. Do not bridge that verb.
    r'(?:(?!(?:meeting|call|discussion|discuss|talk|chat)\b)[a-z][a-z-]*[ \t]+){0,6}'
    r'(?:services?|work|projects?)')
PROVIDER_ACTION = _pattern(
    r'занимаюсь|занимаемся|разрабатываю|разрабатываем|создаю|создаем|создаём|делаю|делаем|'
    r'настраиваю|настраиваем|веду|ведем|ведём|i build|i develop|we build|we develop')
PROVIDER_IDENTITY = _pattern(r'я\s+(?:авитолог(?:а|у|ом|е|и|ов|ам|ами|ах)?|веб[ -]?разработчик|smm[ -]?специалист|смм[ -]?специалист|маркетолог|таргетолог|директолог|seo[ -]?специалист)|i(?: am|[’\x27]m)\s+(?:a\s+)?(?:web developer|smm specialist|seo specialist)')
CTA = _pattern(r'пишите|напишите|обращайтесь|dm me|contact me|message me|contact us|пиши|напиши')
RHETORICAL = _pattern(r'кому\s+(?:нужен|нужна|нужно|нужны)|если\s+(?:(?:вам|тебе)\s+)?(?:нужен|нужна|нужно|нужны)')
# A help-offer tag needs an explicit seller delivery promise and CTA; the
# hashtag alone never decides who is buying a service.
HELP_OFFER = re.compile(r'(?<!\w)#помогу(?!\w)', re.I)
DELIVERY_OFFER = _pattern(r'отправьте\b[\s\S]{0,160}\bполучите')
BUYER_NEED = _pattern(r'(?:нам|мне)\s+(?:нужен|нужна|нужно|нужны)')
BARE_BUYER_NEED = re.compile(r'^\s*(?:нужен|нужна|нужно|нужны)\b', re.I)
BUYER_ACCOMMODATION = re.compile(r'^\s+(?:(?:дополнительн\w+|тестов\w+)\s+)?(?:доступ\w*|тз|техническ\w+\s+задани\w*|исходник\w*)(?!\w)', re.I)
OWN_WORK = re.compile(r'\s+(?:(?:нов\w+|небольш\w+)\s+)?(?:клиент\w*|заказ\w*|проект\w*|работ\w*)(?!\w)', re.I)
# A seller invites readers to request the seller's prices/catalogue. This is
# different from a buyer asking providers to send their quotes or portfolio.
SALES_CONTACT = _pattern(
    r'(?:dm|message|contact|reach out)[ \t]*[:–—-]?[ \t]+'
    r'(?:(?:me|us|@[A-Za-z][A-Za-z0-9_]{3,31})[ \t]+)?'
    r'(?:for|to (?:get|receive))[ \t]+(?:our[ \t]+)?'
    r'(?:pricing|prices?|rates|packages?|availability|(?:a[ \t]+)?quote|price list|catalog(?:ue)?)|'
    r'(?:пишите|напишите|обращайтесь)[ \t]*[:–—-]?[ \t]+'
    r'(?:(?:мне|нам|@[A-Za-z][A-Za-z0-9_]{3,31})[ \t]+)?'
    r'(?:за|для получения)[ \t]+(?:прайс\w*|цен\w*|тариф\w*|каталог\w*)')
PROFESSIONAL = _pattern(
    r'подрядчик\w*|исполнител\w*|специалист\w*|агентств\w*|разработчик\w*|'
    r'contractor|freelancer|agency|developer|specialist|manager')
PERSONAL_REQUEST = _pattern(
    r'ищу|ищем|нам\s+(?:нужен|нужна|нужно|нужны)|'
    r'(?:i|we)\s+(?:need|require|(?:am|are)\s+looking\s+for)')
EMPLOYMENT = _pattern(
    r'в штат|штатн\w*|полный рабочий день|полная занятость|зарплат\w*|оклад\w*|'
    r'трудоустройство|трудов\w* договор\w*|по тк рф|испытательный срок|соцпакет|'
    r'full[ -]?time|salary|5\s*[/×хx]\s*2|2\s*[/×хx]\s*2|'
    r'операционн\w*\s+помощник\w*|ответственн\w*\s+помощник\w*|личн\w*\s+помощник\w*')
VACANCY = _pattern(r'ваканси\w*|hiring|job opening|график работы|обязанности')
PROJECT = _pattern(r'проектн\w*|разов\w*|подряд\w*|на проект|для проекта|аутсорс\w*|contractor|contract work|freelance|agency')
AGGREGATOR = _pattern(
    r'подборка\s+(?:заказов|заданий|вакансий)|дайджест\s+(?:заказов|вакансий)|'
    r'переслано|репост|заказчик пишет|заказ\w*\s+(?:с|из)\s+бирж\w*|'
    r'вакансия\s+(?:с|из)\s+бирж\w*|job roundup|forwarded from|copied job')
CLOSED = _pattern(r'больше\s+не\s+нуж\w*|подрядчик\s+уже\s+найден|уже\s+нашли|задача\s+закрыта|неактуально|не актуально|no longer (?:need|needed)|position filled')
OTHER_ROLE = _pattern(r'(?:нуж\w*|ищу|ищем|требуется)\s+(?:графическ\w*\s+дизайнер\w*|водител\w*|бухгалтер\w*|монтажер\w*|монтажёр\w*)')
SELF_SERVICE = _pattern(r'как\s+(?:мне\s+)?(?:самому|самостоятельно)|своими руками|подскажите[,]?\s+как|how do i|how can i|tutorial')
NEGATED = re.compile(r'(?:\bне|\bnot|don[’\x27]t|do not)\s*$', re.I)


def _positive_matches(pattern, text):
    return [m for m in pattern.finditer(text) if not NEGATED.search(text[max(0, m.start()-12):m.start()])]


def _near(first, second, distance=160):
    return max(first.start(), second.start()) - min(first.end(), second.end()) <= distance


def _procurement_clause(text, patterns):
    # A business may describe what it sells and separately hire a contractor.
    # Only a distinct service + professional + request clause can override an
    # offer elsewhere; bare "Need X?" advertising questions never qualify.
    for clause in re.finditer(r'[^.!?;,。！？]+[.!?;,。！？]*', text):
        part = clause.group(0)
        if not PROFESSIONAL.search(part) or _positive_matches(OFFER, part) or RHETORICAL.search(part) or SALES_CONTACT.search(part):
            continue
        if '?' in part or '？' in part or '❓' in part:
            if not PERSONAL_REQUEST.search(part):
                continue
        requests = _positive_matches(REQUEST, part)
        local = [hit for pattern in patterns.values() for hit in pattern.finditer(part)]
        if any(_near(request, service) for request in requests for service in local):
            return part.strip()
    return ''


def _buyer_need_clause(text, patterns):
    # A buyer may offer access to their own systems in a conditional sentence.
    # Do not turn that accommodation into an advertising pitch.
    clauses = list(re.finditer(r'[^.!?;。！？]+[.!?;。！？]*', text))
    for clause in clauses:
        part = clause.group(0)
        conditional = RHETORICAL.search(part)
        # A separate offer of the actual service stays a seller pitch, even if
        # a previous bare sentence says 'Нужен сайт.' or 'Мне нужен сайт?'.
        if conditional and any(pattern.search(part) for pattern in SERVICES.values()):
            if not BUYER_ACCOMMODATION.match(part[conditional.end():]):
                return False
    for clause in clauses:
        part = clause.group(0)
        if RHETORICAL.search(part) or _positive_matches(OFFER, part):
            continue
        needs = _positive_matches(BUYER_NEED, part)
        # A declarative opening such as 'Нужен сайт.' is also a buyer need.
        # A bare advertising question ('Нужен сайт?') is not enough evidence.
        if not any(mark in part for mark in ('?', '？', '❓')):
            needs += _positive_matches(BARE_BUYER_NEED, part)
        needs = [m for m in needs if not OWN_WORK.match(part[m.end():])]
        local = [hit for pattern in patterns.values() for hit in pattern.finditer(part)]
        if any(_near(need, service) for need in needs for service in local):
            return True
    return False


def assess_request(text, rule=None):
    """Return label, exact evidence, stable reason and explicitly mentioned services.

    A rule's topic optionally narrows evidence to that requested service. This is
    a conservative heuristic, not semantic AI or verification of the author.
    """
    if not isinstance(text, str) or not text.strip():
        return dict(label='unclear', evidence='', reason='empty_text', services=[])
    topic = (rule or {}).get('topic')
    patterns = {topic: SERVICES[topic]} if topic in SERVICES else SERVICES
    service_hits = {key: list(pattern.finditer(text)) for key, pattern in patterns.items()}
    services = [key for key, hits in service_hits.items() if hits]

    def result(label, reason, evidence=''):
        return dict(label=label, evidence=evidence, reason=reason, services=services)

    if not services:
        return result('unclear', 'no_relevant_service')
    if CLOSED.search(text):
        return result('unclear', 'request_closed')
    if AGGREGATOR.search(text):
        return result('aggregator', 'third_party_roundup')
    # A seller's price/catalogue CTA has priority even when a separate bare
    # request names a professional. Mixed sales/procurement posts stay review-only.
    if SALES_CONTACT.search(text):
        return result('supplier_offer', 'provider_sales_contact')
    procurement = _procurement_clause(text, patterns)
    buyer_need = _buyer_need_clause(text, patterns)
    if not procurement:
        if _positive_matches(OFFER, text):
            return result('supplier_offer', 'provider_or_training_offer')
        if _positive_matches(AVAILABILITY_OFFER, text):
            return result('supplier_offer', 'provider_available_for_services')
        actions = _positive_matches(PROVIDER_ACTION, text)
        service_spans = [hit for hits in service_hits.values() for hit in hits]
        if any(_near(action, service, 90) and not _positive_matches(
                REQUEST, text[min(action.end(), service.end()):max(action.start(), service.start())])
               for action in actions for service in service_spans):
            return result('supplier_offer', 'provider_self_description')
        if PROVIDER_IDENTITY.search(text) and (CTA.search(text) or RHETORICAL.search(text)):
            return result('supplier_offer', 'provider_rhetorical_pitch')
        if HELP_OFFER.search(text) and DELIVERY_OFFER.search(text) and CTA.search(text) and not buyer_need:
            return result('supplier_offer', 'provider_delivery_pitch')
        if RHETORICAL.search(text) and CTA.search(text) and not buyer_need:
            return result('supplier_offer', 'rhetorical_contact_pitch')
        # English landing-page-style pitches commonly start with "Need a website?"
        # and carry the offer in the next sentence.
        if re.search(r'^\s*need\b[^.!?]{0,160}\?', text, re.I) and CTA.search(text):
            return result('supplier_offer', 'rhetorical_contact_pitch')
    if _positive_matches(EMPLOYMENT, text) or (VACANCY.search(text) and not PROJECT.search(text)):
        return result('employment', 'staff_employment')
    if OTHER_ROLE.search(text):
        return result('unclear', 'service_only_a_tool_or_background')
    if SELF_SERVICE.search(text):
        return result('unclear', 'self_service_question')

    if procurement:
        return result('direct_request', 'explicit_service_request', procurement)

    # Keep line breaks inside a sentence for formatted requests, but never link
    # an unrelated question in one sentence to a service mentioned in another.
    for clause in re.finditer(r'[^.!?;。！？]+[.!?;。！？]*', text):
        part = clause.group(0)
        requests = _positive_matches(REQUEST, part)
        if not requests:
            continue
        local = [hit for pattern in patterns.values() for hit in pattern.finditer(part)]
        if any(_near(request, service) for request in requests for service in local):
            evidence = part.strip()
            return result('direct_request', 'explicit_service_request', evidence)
    return result('unclear', 'no_local_service_request')
