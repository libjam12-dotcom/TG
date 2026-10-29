"""Account-bound, resumable audience research. Collection never adds a person."""
import asyncio
import contextlib
import csv
import hashlib
import io
import json
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Literal
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, model_validator
from telethon import errors, functions, types, utils
from directory import SearchSpec, community, eligible
from engine import normalize, now, words


class AudienceSpec(BaseModel):
    account: str
    target: int
    title: str = Field(min_length=1, max_length=100)
    topic: str = Field(min_length=2, max_length=100)
    geography: str = Field(default='', max_length=100)
    queries: list[str] = Field(min_length=1, max_length=20)
    keywords: str = Field(default='', max_length=2000)
    exclude: str = Field(default='', max_length=1000)
    method: Literal['authors', 'members'] = 'authors'
    history_days: int = Field(default=14, ge=1, le=90)
    per_source: int = Field(default=100, ge=10, le=500)
    max_people: int = Field(default=100, ge=1, le=200)
    source_kind: Literal['group', 'channel', 'all'] = 'group'
    min_members: int = Field(default=0, ge=0, le=100000000)
    max_members: int = Field(default=0, ge=0, le=100000000)
    standalone: bool = False
    source: str | int | None = None
    sources: list[str | int] | None = Field(default=None, min_length=1, max_length=100)

    @model_validator(mode='after')
    def validate_search(self):
        search = SearchSpec(queries=self.queries, min_members=self.min_members, max_members=self.max_members)
        self.queries = search.queries
        if not self.title.strip() or not self.topic.strip(): raise ValueError('Укажите название и тематику.')
        if self.standalone:
            if self.target != 0: raise ValueError('Прямой сбор не привязан к целевой группе.')
            self.source, self.sources = parse_references(self.source, self.sources)
        if self.method == 'authors' and not self.standalone and not words(self.keywords): raise ValueError('Для авторов сообщений нужны тематические слова.')
        return self


def source_reference(value):
    """Only resolve a peer already accessible to the account; never join a link."""
    if isinstance(value, int):
        if value >= 0: raise ValueError('ID группы должен быть отрицательным Telegram ID.')
        return value
    value = value.strip()
    if re.fullmatch(r'-\d{1,20}', value): return source_reference(int(value))
    if value.startswith('https://'):
        parsed = urlparse(value)
        if parsed.netloc != 't.me' or parsed.query or parsed.fragment: raise ValueError('Укажите @username, ссылку https://t.me/username или Telegram ID группы.')
        match = re.fullmatch(r'/c/(\d{1,20})(?:/\d+)?/?', parsed.path)
        if match: return -1000000000000-int(match.group(1))
        value = parsed.path.strip('/')
    value = value.removeprefix('@')
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{3,31}', value): raise ValueError('Укажите @username, ссылку https://t.me/username или Telegram ID группы. Ссылки-приглашения не подключают аккаунт к группе.')
    return value


def parse_references(source, sources):
    if (source is None) == (sources is None): raise ValueError('Укажите один источник или список источников, но не оба поля сразу.')
    references = [source] if sources is None else sources
    if not references: raise ValueError('Выберите хотя бы один источник.')
    normalized = list(dict.fromkeys(ref.lower() if isinstance(ref, str) else ref for ref in (source_reference(value) for value in references)))
    return (normalized[0], None) if sources is None else (None, normalized)


class ParseSpec(BaseModel):
    account: str = Field(min_length=1, max_length=80)
    source: str | int | None = None
    sources: list[str | int] | None = Field(default=None, min_length=1, max_length=100)
    method: Literal['authors', 'members'] = 'members'
    history_days: int = Field(default=14, ge=1, le=90)
    per_source: int = Field(default=100, ge=10, le=500)
    max_people: int = Field(default=100, ge=1, le=200)
    keywords: str = Field(default='', max_length=2000)
    exclude: str = Field(default='', max_length=1000)

    @model_validator(mode='after')
    def validate_source(self):
        self.source, self.sources = parse_references(self.source, self.sources)
        return self


class AudienceSelection(BaseModel):
    source_ids: list[int] | None = Field(default=None, max_length=10)
    candidate_ids: list[int] | None = Field(default=None, max_length=50)


class AudienceControl(BaseModel):
    action: Literal['search', 'collect', 'pause', 'resume', 'cancel']


class AudienceDraft(BaseModel):
    request_key: str = Field(min_length=16, max_length=80)
    consent_note: str = Field(min_length=8, max_length=500)
    delay_seconds: int = Field(default=60, ge=30, le=3600)
    daily_limit: int = Field(default=20, ge=1, le=100)


def session_binding(account):
    """Private digest, never exported. A replacement session must recollect peers."""
    session = getattr(account.client, 'session', None)
    key = getattr(getattr(session, 'auth_key', None), 'key', None)
    if not key: raise HTTPException(409, 'Сессия не подтверждена. Завершите подключение аккаунта.')
    return hashlib.sha256(str(account.me.id).encode() + b':' + key).hexdigest()


class Audience:
    def __init__(self, ops):
        self.ops = ops
        self.task = None
        self.active_id = None
        self.lock = asyncio.Lock()
        self.router = APIRouter(prefix='/audience')
        for path, fn, methods in [('/options', self.options, ['GET']), ('/parse', self.parse, ['POST']), ('/runs', self.history, ['GET']),
            ('/runs', self.create, ['POST']), ('/runs/{rid}', self.detail, ['GET']),
            ('/runs/{rid}/selection', self.select, ['POST']), ('/runs/{rid}/control', self.control, ['POST']),
            ('/runs/{rid}/draft', self.draft, ['POST']), ('/runs/{rid}/export', self.export, ['GET'])]:
            self.router.add_api_route(path, fn, methods=methods)

    @property
    def store(self): return self.ops.store

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS audience_runs(id INTEGER PRIMARY KEY,account TEXT NOT NULL,telegram_id INTEGER NOT NULL,binding TEXT NOT NULL,target INTEGER NOT NULL,target_title TEXT NOT NULL,spec TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'draft',phase TEXT NOT NULL DEFAULT 'search',search_index INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,error TEXT,job_id INTEGER,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audience_sources(run_id INTEGER NOT NULL,id INTEGER NOT NULL,data TEXT NOT NULL,selected INTEGER NOT NULL DEFAULT 0,cursor INTEGER NOT NULL DEFAULT 0,scanned INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'pending',error TEXT,PRIMARY KEY(run_id,id));
        CREATE TABLE IF NOT EXISTS audience_candidates(id INTEGER PRIMARY KEY,run_id INTEGER NOT NULL,user_id INTEGER NOT NULL,username TEXT,display_name TEXT NOT NULL,access_hash INTEGER,eligible INTEGER NOT NULL,reason TEXT NOT NULL,selected INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL,UNIQUE(run_id,user_id));
        ''')
        with self.store.db:
            self.store.db.execute("UPDATE audience_runs SET state='paused',error='Сервер перезапущен. Выбор сохранён; продолжение вручную.',updated=? WHERE state='running'", (now(),))

    def row(self, rid):
        rows = self.store.rows('SELECT * FROM audience_runs WHERE id=?', (rid,))
        if not rows: raise HTTPException(404, 'Подбор не найден.')
        row = rows[0]; row['spec'] = json.loads(row['spec'])
        return row

    def update(self, rid, **values):
        values['updated'] = now()
        with self.store.db:
            self.store.db.execute('UPDATE audience_runs SET ' + ','.join(k+'=?' for k in values) + ' WHERE id=?', (*values.values(), rid))

    def bound(self, row):
        a = self.ops.online(row['account'])
        if a.me.id != row['telegram_id'] or session_binding(a) != row['binding']:
            raise HTTPException(409, 'Аккаунт или сессия изменились. Создайте новый подбор: данные участников нельзя переносить между сессиями.')
        return a

    async def read(self, row, a, awaitable):
        client = a.client
        try: result = await asyncio.wait_for(awaitable, 40)
        except BaseException: raise
        if self.bound(row).client is not client: raise HTTPException(409, 'Подключение изменилось во время чтения.')
        return result

    async def options(self):
        source = self.ops.get_client_store() if self.ops.get_client_store else None
        return {'topics': [{'name': r['name'], 'keywords': r.get('keywords', '')} for r in source.rules()] if source else [],
                'methods': ['authors', 'members'], 'max_selected': 50}

    async def history(self):
        return {'runs': [await self.detail(r['id']) for r in self.store.rows('SELECT id FROM audience_runs ORDER BY id DESC LIMIT 20')]}

    async def detail(self, rid: int):
        row = self.row(rid); row.pop('binding')
        row['mode'] = 'parse' if row['spec'].get('standalone') else 'audience'
        row['sources'] = []
        for source in self.store.rows('SELECT * FROM audience_sources WHERE run_id=? ORDER BY id', (rid,)):
            data = json.loads(source.pop('data')); row['sources'].append({**data, **source})
        row['candidates'] = self.store.rows('SELECT id,user_id,username,display_name,eligible,reason,selected,evidence FROM audience_candidates WHERE run_id=? ORDER BY eligible DESC,id', (rid,))
        for person in row['candidates']: person['evidence'] = json.loads(person['evidence'])
        row['stats'] = {'available': len(row['candidates']), 'eligible': sum(p['eligible'] for p in row['candidates']),
                        'selected': sum(p['selected'] for p in row['candidates']), 'excluded': sum(not p['eligible'] for p in row['candidates']),
                        'copyable': sum(bool(p['eligible'] and p['username']) for p in row['candidates'])}
        if row['spec'].get('standalone'):
            row['source_resolution_errors'] = [{'source': result['source'], 'reason': result['error']} for result in row['spec'].get('source_resolution', []) if result.get('error')]
            row['source_stats'] = {'requested': len(row['spec'].get('sources') or [row['spec'].get('source')]), 'resolved': len(row['sources']),
                **{state: sum(source['state'] == state for source in row['sources']) for state in ('complete', 'pending', 'unavailable', 'limited')},
                'resolution_errors': len(row['source_resolution_errors'])}
        return row

    async def create(self, spec: AudienceSpec):
        if spec.standalone: raise HTTPException(400, 'Для прямого сбора используйте вкладку «Парсинг».')
        async with self.lock:
            a = self.ops.online(spec.account); binding = session_binding(a)
            entity = await asyncio.wait_for(a.client.get_entity(spec.target), 20)
            dest = await self.ops.rights(a, entity, 'invite')
            if dest['id'] != spec.target or getattr(entity, 'broadcast', False): raise HTTPException(400, 'Выберите свою управляемую группу с правом приглашения.')
            if self.ops.online(spec.account).client is not a.client or session_binding(a) != binding: raise HTTPException(409, 'Сессия изменилась.')
            with self.store.db:
                cur = self.store.db.execute('INSERT INTO audience_runs(account,telegram_id,binding,target,target_title,spec,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                    (spec.account, a.me.id, binding, spec.target, dest['title'], spec.model_dump_json(), now(), now()))
            return await self.detail(cur.lastrowid)

    async def parse(self, request: ParseSpec):
        async with self.lock:
            if self.task and not self.task.done(): raise HTTPException(409, 'Другой сбор ещё выполняется. Остановите его или дождитесь результата.')
            a = self.ops.online(request.account)
            title = 'Сбор из '+str(request.source) if request.sources is None else 'Сбор из выбранных групп ('+str(len(request.sources))+')'
            spec = AudienceSpec(**request.model_dump(), standalone=True, target=0, title=title, topic='Прямой сбор', queries=['прямой сбор'])
            with self.store.db:
                cur = self.store.db.execute("INSERT INTO audience_runs(account,telegram_id,binding,target,target_title,spec,state,phase,created,updated) VALUES(?,?,?,?,?,?,'running','collect',?,?)",
                    (request.account, a.me.id, session_binding(a), 0, '', spec.model_dump_json(), now(), now()))
            self.active_id = cur.lastrowid
            self.task = asyncio.create_task(self.work(self.active_id))
            return await self.detail(self.active_id)

    async def select(self, rid: int, spec: AudienceSelection):
        async with self.lock:
            row = self.row(rid)
            if row['state'] == 'running' or row['job_id']: raise HTTPException(409, 'Остановите подбор перед изменением выбора. Выбор созданного задания закреплён.')
            for values, table, where in [(spec.source_ids, 'audience_sources', ''), (spec.candidate_ids, 'audience_candidates', ' AND eligible=1')]:
                if values is None: continue
                valid = {r['id'] for r in self.store.rows('SELECT id FROM '+table+' WHERE run_id=?'+where, (rid,))}
                if len(set(values)) != len(values) or not set(values) <= valid: raise HTTPException(400, 'В выборе есть недоступные записи.')
            with self.store.db:
                if spec.source_ids is not None:
                    old = {s['id'] for s in self.store.rows('SELECT id FROM audience_sources WHERE run_id=? AND selected=1', (rid,))}
                    if old != set(spec.source_ids) and self.store.rows('SELECT 1 FROM audience_candidates WHERE run_id=?', (rid,)):
                        raise HTTPException(409, 'Участники уже собраны из этих источников. Для другого набора сохраните новый подбор.')
                    self.store.db.execute('UPDATE audience_sources SET selected=0 WHERE run_id=?', (rid,))
                    for sid in spec.source_ids: self.store.db.execute('UPDATE audience_sources SET selected=1 WHERE run_id=? AND id=?', (rid, sid))
                if spec.candidate_ids is not None:
                    self.store.db.execute('UPDATE audience_candidates SET selected=0 WHERE run_id=?', (rid,))
                    for pid in spec.candidate_ids: self.store.db.execute('UPDATE audience_candidates SET selected=1 WHERE run_id=? AND id=?', (rid, pid))
            self.update(rid)
            return await self.detail(rid)

    async def stop_task(self, rid):
        if self.active_id == rid and self.task and not self.task.done():
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await self.task

    async def control(self, rid: int, spec: AudienceControl):
        async with self.lock:
            row = self.row(rid)
            if row['job_id']: raise HTTPException(409, 'Черновик приглашений уже создан. Управляйте им в журнале.')
            if spec.action in ('pause', 'cancel'):
                await self.stop_task(rid)
                self.update(rid, state='paused' if spec.action == 'pause' else 'cancelled')
                return await self.detail(rid)
            if self.task and not self.task.done(): raise HTTPException(409, 'Другой подбор ещё выполняется. Остановите его или дождитесь результата.')
            self.bound(row)
            if row['state'] == 'cancelled': raise HTTPException(409, 'Подбор отменён. Сохраните новый.')
            if spec.action == 'search':
                if row['spec'].get('standalone'): raise HTTPException(409, 'Источник уже указан. Для прямого сбора используйте «Продолжить» или сохраните новый сбор.')
                if row['state'] != 'draft': raise HTTPException(409, 'Поиск уже начат. Используйте «Продолжить».')
                phase = 'search'
            elif spec.action == 'collect':
                if row['phase'] != 'review_sources' or row['state'] != 'ready': raise HTTPException(409, 'Сначала завершите поиск и выберите источники.')
                if not self.store.rows('SELECT 1 FROM audience_sources WHERE run_id=? AND selected=1', (rid,)): raise HTTPException(400, 'Выберите хотя бы один источник.')
                phase = 'collect'
            else:
                if row['state'] != 'paused' or row['phase'] not in ('search', 'collect'): raise HTTPException(409, 'Нет приостановленного чтения для продолжения.')
                phase = row['phase']
            if row['next_at'] > time.time(): raise HTTPException(429, 'Продолжить можно после '+datetime.fromtimestamp(row['next_at'], timezone.utc).isoformat())
            self.update(rid, state='running', phase=phase, error=None)
            self.active_id = rid; self.task = asyncio.create_task(self.work(rid))
            return await self.detail(rid)

    async def pace(self, row):
        until=max(self.row(row['id'])['next_at'],self.ops.account_row(row['account'])['not_before'])
        if row['account']=='primary' and self.ops.get_client_store:
            client_store=self.ops.get_client_store()
            until=max(until,client_store.get('scan_not_before',0),client_store.get('history_retry_at',0),getattr(self.ops.get_primary(),'discovery_next',0))
        await asyncio.sleep(max(0,until-time.time()))
        self.bound(row)
        next_at=time.time()+5
        self.update(row['id'],next_at=next_at)
        if row['account']=='primary' and self.ops.get_client_store:
            shared=max(self.ops.get_client_store().get('scan_not_before',0),self.ops.get_client_store().get('history_retry_at',0),getattr(self.ops.get_primary(),'discovery_next',0))
            if shared>time.time()+5: raise HTTPException(429,'Общий аккаунт получил новое ограничение Telegram. Продолжите после ожидания.')
            self.ops.get_client_store().put('scan_not_before',max(shared,next_at))
            self.ops.get_primary().discovery_next=max(shared,next_at)

    async def search(self, row, a, spec):
        filters = SearchSpec(queries=spec.queries, kind=spec.source_kind, min_members=spec.min_members, max_members=spec.max_members, exclude=spec.exclude)
        aliases = {'дубай': ['дубай', 'dubai', 'оаэ', 'uae'], 'оаэ': ['дубай', 'dubai', 'оаэ', 'uae']}
        geo = aliases.get(normalize(spec.geography), [normalize(spec.geography)]) if spec.geography.strip() else []
        for idx in range(row['search_index'], len(spec.queries)):
            await self.pace(row)
            found = await self.read(row, a, a.client(functions.contacts.SearchRequest(q=spec.queries[idx], limit=50)))
            for entity in found.chats:
                source = community(entity)
                if not source or source['id'] == row['target'] or not eligible(source, filters): continue
                if geo and not any(g in normalize(source['title']+' '+source['username']) for g in geo): continue
                old = self.store.rows('SELECT data FROM audience_sources WHERE run_id=? AND id=?', (row['id'], source['id']))
                if old: source['queries'] = json.loads(old[0]['data'])['queries']
                source['queries'] = list(dict.fromkeys(source['queries']+[spec.queries[idx]]))
                if not old and self.store.db.execute('SELECT COUNT(*) FROM audience_sources WHERE run_id=?', (row['id'],)).fetchone()[0] >= 100: continue
                with self.store.db:
                    self.store.db.execute('INSERT INTO audience_sources(run_id,id,data) VALUES(?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET data=excluded.data', (row['id'], source['id'], json.dumps(source, ensure_ascii=False)))
            self.update(row['id'], search_index=idx+1)
        self.update(row['id'], state='ready', phase='review_sources')

    async def candidate(self, row, a, target, user, proof):
        if not isinstance(user, types.User): return
        old = self.store.rows('SELECT * FROM audience_candidates WHERE run_id=? AND user_id=?', (row['id'], user.id))
        if old:
            evidence = json.loads(old[0]['evidence'])
            if proof not in evidence and len(evidence) < 10:
                with self.store.db: self.store.db.execute('UPDATE audience_candidates SET evidence=? WHERE id=?', (json.dumps(evidence+[proof], ensure_ascii=False), old[0]['id']))
            return
        standalone = row['spec'].get('standalone', False)
        if standalone:
            reason = 'Доступный участник источника; интерес и согласие на приглашение или сообщение не подтверждены.' if proof['method'] == 'members' else 'Автор сообщения; согласие на приглашение или сообщение не подтверждено.'
        else:
            reason = 'Доступный участник тематического источника; интерес и согласие не подтверждены.' if proof['method'] == 'members' else 'Автор тематического сообщения; согласие на приглашение не подтверждено.'
        valid = True
        if user.bot: valid, reason = False, 'Бот.'
        elif user.deleted: valid, reason = False, 'Удалённый аккаунт.'
        elif user.id == a.me.id: valid, reason = False, 'Сам выбранный аккаунт.'
        elif getattr(user, 'min', False) or not user.access_hash: valid, reason = False, 'Telegram не предоставил полную запись пользователя для этой сессии.'
        elif not standalone and self.store.rows('SELECT 1 FROM op_invited WHERE chat_id=? AND user_id=?', (row['target'], user.id)): valid, reason = False, 'Уже обработан для этой группы; повтор исключён.'
        if valid and not standalone:
            await self.pace(row)
            try:
                await self.read(row, a, a.client.get_permissions(target, user)); valid, reason = False, 'Уже участник выбранной группы.'
            except errors.UserNotParticipantError: pass
            except (errors.FloodWaitError, errors.PeerFloodError): raise
            except errors.RPCError: valid, reason = False, 'Членство в целевой группе не удалось проверить. Запись исключена до проверки.'
        with self.store.db:
            self.store.db.execute('INSERT INTO audience_candidates(run_id,user_id,username,display_name,access_hash,eligible,reason,evidence) VALUES(?,?,?,?,?,?,?,?)',
                (row['id'], user.id, user.username, ' '.join(x for x in [user.first_name, user.last_name] if x) or str(user.id), user.access_hash, int(valid), reason, json.dumps([proof], ensure_ascii=False)))

    async def direct_source(self, row, a, spec):
        multiple = spec.sources is not None
        if not multiple and self.store.rows('SELECT 1 FROM audience_sources WHERE run_id=?', (row['id'],)): return
        references = spec.sources if multiple else [spec.source]
        current_spec = self.row(row['id'])['spec']
        resolved = current_spec.get('source_resolution', [])
        for reference in references:
            if any(result['source'] == reference for result in resolved): continue
            await self.pace(row)
            try:
                source = await self.read(row, a, a.client.get_entity(reference))
                if not isinstance(source, (types.Chat, types.Channel)) or getattr(source, 'deactivated', False):
                    raise HTTPException(400, 'Источник должен быть доступной группой или каналом выбранного аккаунта.')
            except (ValueError, errors.UsernameInvalidError, errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.PeerIdInvalidError, HTTPException) as exc:
                self.bound(row)
                if isinstance(exc, HTTPException) and exc.status_code != 400: raise
                reason = exc.detail if isinstance(exc, HTTPException) else 'Источник не найден или недоступен выбранному аккаунту. Проверьте username или Telegram ID; для приватной группы аккаунт должен уже иметь доступ.'
                if not multiple: raise HTTPException(400, reason)
                resolved.append({'source': reference, 'error': reason})
            else:
                data = dict(id=utils.get_peer_id(source), title=source.title, username=getattr(source, 'username', None),
                            kind='group' if isinstance(source, types.Chat) or getattr(source, 'megagroup', False) else 'channel',
                            members=getattr(source, 'participants_count', None), joined=not bool(getattr(source, 'left', False)), queries=[])
                with self.store.db:
                    self.store.db.execute('INSERT INTO audience_sources(run_id,id,data,selected) VALUES(?,?,?,1) ON CONFLICT(run_id,id) DO NOTHING', (row['id'], data['id'], json.dumps(data, ensure_ascii=False)))
                resolved.append({'source': reference, 'id': data['id']})
                if not multiple: self.update(row['id'], target_title=source.title)
            current_spec['source_resolution'] = resolved
            self.update(row['id'], spec=json.dumps(current_spec, ensure_ascii=False))

    def collection_result(self, row, spec):
        sources = self.store.rows('SELECT state FROM audience_sources WHERE run_id=? AND selected=1', (row['id'],))
        capped = self.store.db.execute('SELECT COUNT(*) FROM audience_candidates WHERE run_id=?', (row['id'],)).fetchone()[0] >= spec.max_people
        pending = sum(source['state'] == 'pending' for source in sources)
        limited = sum(source['state'] == 'limited' for source in sources)
        unavailable = sum(source['state'] == 'unavailable' for source in sources)
        unavailable += sum(bool(result.get('error')) for result in self.row(row['id'])['spec'].get('source_resolution', []))
        messages = []
        if capped: messages.append('Достигнут выбранный предел людей. Это частичная выборка.')
        elif limited: messages.append('Достигнут предел чтения в '+str(limited)+' источниках. Это частичная выборка.')
        if pending: messages.append('Ещё не прочитаны полностью '+str(pending)+' источников.')
        if unavailable: messages.append('Недоступны '+str(unavailable)+' источников. Доступные результаты сохранены.')
        self.update(row['id'], state='ready', phase='review_people', error=' '.join(messages) or None)

    async def collect(self, row, a, spec):
        target = None
        if spec.standalone:
            if row['target'] != 0: raise HTTPException(409, 'Прямой сбор не может использовать целевую группу другого подбора.')
            await self.direct_source(row, a, spec)
        else:
            target = await self.read(row, a, a.client.get_entity(row['target']))
            await self.ops.rights(a, target, 'invite')
        cutoff = datetime.now(timezone.utc)-timedelta(days=spec.history_days)
        for saved in self.store.rows("SELECT * FROM audience_sources WHERE run_id=? AND selected=1 AND state='pending' ORDER BY id", (row['id'],)):
            if self.store.db.execute('SELECT COUNT(*) FROM audience_candidates WHERE run_id=?', (row['id'],)).fetchone()[0] >= spec.max_people:
                self.collection_result(row, spec); return
            data = json.loads(saved['data'])
            try:
                source = await self.read(row, a, a.client.get_entity(data['username'] or data['id']))
                source_valid = isinstance(source, (types.Chat, types.Channel)) and not getattr(source, 'deactivated', False) if spec.standalone else bool(community(source))
                if not source_valid or utils.get_peer_id(source) != saved['id']: raise HTTPException(409, 'Источник изменился или больше недоступен выбранному аккаунту.')
                source_url = 'https://t.me/'+data['username'] if data['username'] else (f'https://t.me/c/{source.id}' if isinstance(source, types.Channel) else None)
                done = False
                while not done and saved['scanned'] < spec.per_source:
                    if self.store.db.execute('SELECT COUNT(*) FROM audience_candidates WHERE run_id=?', (row['id'],)).fetchone()[0] >= spec.max_people:
                        self.collection_result(row, spec); return
                    await self.pace(row)
                    limit = min(25, spec.per_source-saved['scanned'])
                    if spec.method == 'members':
                        if isinstance(source, types.Chat):
                            all_people = await self.read(row, a, a.client.get_participants(source, limit=spec.per_source))
                            entries = all_people[saved['cursor']:saved['cursor']+limit]
                            count = len(entries)
                        else:
                            result = await self.read(row, a, a.client(functions.channels.GetParticipantsRequest(channel=source, filter=types.ChannelParticipantsRecent(), offset=saved['cursor'], limit=limit, hash=0)))
                            entries, count = result.users, len(result.participants)
                        consumed = 0
                        for user in entries:
                            if self.store.db.execute('SELECT COUNT(*) FROM audience_candidates WHERE run_id=?', (row['id'],)).fetchone()[0] >= spec.max_people: break
                            await self.candidate(row, a, target, user, {'source_id': saved['id'], 'source_title': data['title'], 'source_url': source_url, 'method': 'members', 'quote': 'Видимый список участников', 'message_url': None})
                            consumed += 1
                        consumed_all = consumed == len(entries)
                        advanced = count if consumed_all else consumed
                        saved['cursor'] += advanced; saved['scanned'] += advanced
                        done = consumed_all and count < limit
                    else:
                        entries = await self.read(row, a, a.client.get_messages(source, limit=limit, offset_id=saved['cursor']))
                        consumed = []
                        for msg in entries:
                            if self.store.db.execute('SELECT COUNT(*) FROM audience_candidates WHERE run_id=?', (row['id'],)).fetchone()[0] >= spec.max_people: break
                            consumed.append(msg)
                            if msg.date and msg.date < cutoff: done = True; break
                            text = getattr(msg, 'message', '') or ''
                            if (words(spec.keywords) and not any(w in normalize(text) for w in words(spec.keywords))) or any(w in normalize(text) for w in words(spec.exclude)): continue
                            user = await self.read(row, a, msg.get_sender())
                            await self.candidate(row, a, target, user, {'source_id': saved['id'], 'source_title': data['title'], 'source_url': source_url, 'method': 'authors', 'quote': text[:400], 'message_url': f'{source_url}/{msg.id}' if source_url else None, 'date': msg.date.isoformat() if msg.date else None})
                        saved['scanned'] += len(consumed)
                        if consumed: saved['cursor'] = consumed[-1].id
                        done = done or not entries or (len(consumed) == len(entries) and len(entries) < limit)
                    source_state = 'complete' if done else ('limited' if saved['scanned'] >= spec.per_source else 'pending')
                    with self.store.db:
                        self.store.db.execute('UPDATE audience_sources SET cursor=?,scanned=?,state=? WHERE run_id=? AND id=?', (saved['cursor'], saved['scanned'], source_state, row['id'], saved['id']))
            except (ValueError, errors.UsernameInvalidError, errors.UsernameNotOccupiedError, errors.PeerIdInvalidError, errors.ChannelPrivateError, errors.ChatAdminRequiredError, HTTPException) as exc:
                self.bound(row)
                if isinstance(exc, HTTPException) and exc.status_code != 400: raise
                with self.store.db: self.store.db.execute("UPDATE audience_sources SET state='unavailable',error=? WHERE run_id=? AND id=?", ('Источник или список участников недоступен выбранному аккаунту. Скрытые списки не обходятся.', row['id'], saved['id']))
        self.collection_result(row, spec)

    async def work(self, rid):
        try:
            row = self.row(rid); a = self.bound(row); spec = AudienceSpec.model_validate(row['spec'])
            if row['phase'] == 'search': await self.search(row, a, spec)
            else: await self.collect(row, a, spec)
        except asyncio.CancelledError: raise
        except errors.FloodWaitError as exc:
            until = time.time()+exc.seconds+1
            with self.store.db: self.store.db.execute('UPDATE op_accounts SET not_before=MAX(not_before,?),error=? WHERE id=?', (until, 'Telegram просит подождать.', row['account']))
            if row['account'] == 'primary' and self.ops.get_client_store:
                client_store = self.ops.get_client_store(); client_store.put('scan_not_before', max(until, client_store.get('scan_not_before', 0)))
                self.ops.get_primary().discovery_next = max(until, getattr(self.ops.get_primary(), 'discovery_next', 0))
            self.update(rid, state='paused', next_at=until, error=f'Telegram просит подождать {exc.seconds} секунд. Продолжение вручную после ожидания.')
        except errors.PeerFloodError:
            a.pause(); self.update(rid, state='paused', error='Telegram ограничил аккаунт. Автоматической замены аккаунта нет.')
        except Exception as exc:
            self.update(rid, state='paused', error=exc.detail if isinstance(exc, HTTPException) else 'Чтение прервано: '+type(exc).__name__+'. Результаты сохранены.')

    async def resolve_selected(self, rid, ids, account, target):
        row = self.row(rid)
        if row['spec'].get('standalone'): raise HTTPException(409, 'Скопируйте доступные @username из парсинга и вставьте в отдельное задание. Права группы и участники проверяются перед запуском.')
        if row['job_id']: raise HTTPException(409, 'Для этого выбора уже создан черновик приглашений. Откройте журнал.')
        if row['account'] != account or row['target'] != target: raise HTTPException(409, 'Подбор принадлежит другому аккаунту или группе.')
        if row['state'] != 'ready' or row['phase'] != 'review_people': raise HTTPException(409, 'Сначала завершите сбор и проверьте людей.')
        a = self.bound(row)
        candidates = self.store.rows('SELECT * FROM audience_candidates WHERE run_id=? AND eligible=1 AND selected=1', (rid,))
        if not ids or len(set(ids)) != len(ids) or set(ids) != {p['id'] for p in candidates}: raise HTTPException(400, 'Сохранённый выбор участников изменился. Обновите предпросмотр.')
        users = []
        for person in candidates:
            user = await self.read(row, a, a.client.get_entity(types.InputPeerUser(person['user_id'], person['access_hash'])))
            if not isinstance(user, types.User) or user.id != person['user_id'] or user.bot or user.deleted or user.id == a.me.id: raise HTTPException(409, 'Один из выбранных участников больше недоступен.')
            users.append(user)
        return users

    async def draft(self, rid: int, spec: AudienceDraft):
        # Operations.create owns the delivery lock; never hold the audience lock while taking it.
        from operations import JobSpec
        row = self.row(rid)
        if row['spec'].get('standalone'): raise HTTPException(409, 'Скопируйте доступные @username из парсинга и вставьте в инвайтинг. Для ID без username нужен подбор с целевой группой.')
        ids = [p['id'] for p in self.store.rows('SELECT id FROM audience_candidates WHERE run_id=? AND selected=1', (rid,))]
        result = await self.ops.create(JobSpec(request_key=spec.request_key, account=row['account'], kind='invite', title=row['spec']['title'], destinations=[row['target']],
            audience_run_id=rid, candidate_ids=ids, consent_note=spec.consent_note, delay_seconds=spec.delay_seconds, daily_limit=spec.daily_limit))
        self.update(rid, job_id=result['id'])
        return result

    async def export(self, rid: int):
        row = await self.detail(rid); stream = io.StringIO(); writer = csv.writer(stream, delimiter=';')
        writer.writerow(['Telegram ID', 'Username', 'Имя', 'Доступен для выбора', 'Причина', 'Выбран', 'Источники', 'Сообщения'])
        for p in row['candidates']:
            cells = [p['user_id'], p['username'] or '', p['display_name'], bool(p['eligible']), p['reason'], bool(p['selected']), ' | '.join(dict.fromkeys(e['source_url'] for e in p['evidence'] if e.get('source_url'))), ' | '.join(e['message_url'] for e in p['evidence'] if e.get('message_url'))]
            writer.writerow(["'"+str(v) if str(v).lstrip().startswith(('=', '+', '-', '@')) else v for v in cells])
        return Response('\ufeff'+stream.getvalue(), media_type='text/csv', headers={'Content-Disposition': f'attachment; filename="audience-{rid}.csv"'})

    async def shutdown(self):
        if self.active_id:
            await self.stop_task(self.active_id)
            if self.row(self.active_id)['state'] == 'running': self.update(self.active_id, state='paused', error='Сервис остановлен. Продолжение вручную.')
