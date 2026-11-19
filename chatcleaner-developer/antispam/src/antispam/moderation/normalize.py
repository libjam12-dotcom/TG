import hashlib
from functools import lru_cache
import re
import unicodedata
from urllib.parse import urlsplit

URL = re.compile(r"(?:https?://|www\.)[^\s<>]+|(?<![\w@])(?:t\.me|telegram\.me)/[^\s<>]+", re.I)


def normalize(text: str) -> str:
    text = unicodedata.normalize('NFKC', text).casefold().replace('ё', 'е')
    # Strip invisibles and decorative strike/underline overlays; preserve linguistic accents.
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Cf' and ch not in '\u0332\u0333\u0334\u0335\u0336\u0337\u0338')
    return ' '.join(text.split())


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize(text).encode()).hexdigest()


def urls(text: str, links: tuple[str, ...] = ()) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*links, *(m.group(0).rstrip('.,;!?)') for m in URL.finditer(text)))))


def domain(value: str) -> str:
    value = value.strip()
    try:
        parsed = urlsplit(value if '://' in value else 'https://' + value)
        host = parsed.hostname or ''
        return host.rstrip('.').encode('idna').decode().casefold()
    except (ValueError, UnicodeError):
        return ''


def domain_matches(host: str, pattern: str) -> bool:
    # Subdomains are allowed ONLY through an explicit *.example.com rule.
    if pattern.startswith('*.'):
        root = domain(pattern[2:])
        return bool(root) and (host == root or host.endswith('.' + root))
    return bool(host) and host == domain(pattern)


def word_matches(text: str, pattern: str) -> bool:
    return bool(re.search(r'(?<!\w)' + re.escape(normalize(pattern)) + r'(?!\w)', normalize(text)))


@lru_cache(maxsize=512)
def keyword_regex(pattern: str, obfuscated: bool = False):
    pattern=normalize(pattern)
    if not obfuscated:return re.compile(r'(?<!\w)'+re.escape(pattern)+r'(?!\w)')
    pairs=['aа@4','eе3€','oо0','pр','cс','yу','xх','kк','mм','tт7','bв','hн','iі1!|']
    aliases={c:'['+pair+']' for pair in pairs for c in pair}
    aliases.update({'s':'[s$5]','с':'[сcs$5]','н':'[нhn]','р':'[рpr]',
                    'и':'[иiu1!|]','л':'[лl]','д':'[дd]','г':'[гg]','з':'[зz]','ф':'[фf]'})
    letters=[aliases.get(c,re.escape(c)) for c in pattern]
    # Target known forbidden terms only; never compact every word in a message.
    return re.compile(r'(?<!\w)'+r'[ ._*·/$\-]{0,3}'.join(letters)+r'(?!\w)')


def word_spans(text: str, pattern: str, obfuscated: bool = False):
    return [m.span() for m in keyword_regex(pattern,obfuscated).finditer(normalize(text))]


def covered_by_phrases(text: str, spans: list, phrases: list[str]) -> bool:
    normalized=normalize(text)
    allowed=[]
    for phrase in phrases:
        value=normalize(phrase)
        if value:allowed.extend(m.span() for m in re.finditer(re.escape(value),normalized))
    return bool(spans) and all(any(a<=start and end<=b for a,b in allowed) for start,end in spans)


def canonicalize_keywords(text: str, matches: list) -> str:
    """Replace proven keyword spans only; keep unrelated text and word boundaries."""
    selected=[]
    for start,end,keyword in sorted(matches,key=lambda m:(-(m[1]-m[0]),m[0])):
        if not any(start<b and a<end for a,b,_ in selected):selected.append((start,end,keyword))
    for start,end,keyword in sorted(selected,reverse=True):text=text[:start]+keyword+text[end:]
    return text


def latin_skeleton(value: str) -> str:
    translit=dict(zip('абвгдезийклмнопрстуфхцы','abvgdeziyklmnoprstufhcy'))
    translit.update({'ж':'zh','ч':'ch','ш':'sh','щ':'sch','ь':'','ъ':'','э':'e','ю':'yu','я':'ya',
                     '$':'s','5':'s','3':'e','€':'e','0':'o','@':'a','4':'a','1':'i','!':'i','|':'i','7':'t'})
    return ''.join(translit.get(c,c) for c in normalize(value))


@lru_cache(maxsize=2048)
def near_keyword(token: str, targets: tuple[str,...]) -> str | None:
    token=latin_skeleton(token)
    # Short words and medical stems are intentionally excluded from approximate matching.
    if not 7<=len(token)<=15 or token.startswith(('narkolog','narkoz')):return None
    if token in targets:return None
    for target in targets:
        if abs(len(token)-len(target))>2 or token[:3]!=target[:3] or token==target:continue
        row=list(range(len(target)+1))
        for i,left in enumerate(token,1):
            new=[i]
            for j,right in enumerate(target,1):new.append(min(new[-1]+1,row[j]+1,row[j-1]+(left!=right)))
            row=new
        if row[-1]<=2:return target
    return None


def fuzzy_keywords(text: str, targets: list[str]) -> list[tuple[str,str]]:
    result=[];references=tuple(targets)
    for token in dict.fromkeys(re.findall(r'[a-zа-яії0-9$@!|]+',normalize(text))):
        match=near_keyword(token,references)
        if match:result.append((token,match))
    return result
