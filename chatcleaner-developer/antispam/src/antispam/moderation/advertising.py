"""Deterministic offer patterns; questions and single contact links are not ads."""
import re
from antispam.moderation.normalize import normalize


def is_request(text: str) -> bool:
    text=normalize(text)
    if re.search(r'\b(?:продам|продаю|сдаю|предлагаю услуги|предлагаем услуги)\b',text):return False
    if re.search(r'^(?:всем привет[!,. ]*|привет[!,. ]*)?(?:куплю|покупаю|заказываю|хочу|хотим|нужно|нужна).{0,45}реклам',text):return True
    if re.search(r'ищу (?:клиентов|работу)',text):return False
    return bool(re.search(r'^(?:всем привет[!,. ]*|привет[!,. ]*)?(?:ищу|нужен|нужна|нужно|подскажите|где|как|кто знает|можно ли|есть ли)\b',text))


def ad_signals(text: str, config: dict) -> list[str]:
    if not config.get('enabled'):return []
    text=normalize(text)
    if re.search(r'^(?:осторожно|не ведитесь|предупреждаю|полиция|спамеры|warning|beware)\b',text):return []
    if re.search(r'мошенники предлагают|спамеры пишут|рассылают [«"\']|цитата:|запустил набор персонала',text):return []
    if is_request(text):return []
    return [pattern['name'] for pattern in config.get('patterns',[])
            if all(re.search(p,text) for p in pattern['all'])]
