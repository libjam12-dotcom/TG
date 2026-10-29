"""Bounded, persistent discovery of external public Telegram communities."""
import asyncio
import contextlib
import csv
import io
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, model_validator
from telethon import errors, functions, types, utils
from engine import normalize, now, words


class SearchSpec(BaseModel):
    queries: list[str] = Field(min_length=1, max_length=20)
    kind: Literal['all', 'group', 'channel'] = 'group'
    min_members: int = Field(default=0, ge=0, le=100000000)
    max_members: int = Field(default=0, ge=0, le=100000000)
    include: str = Field(default='', max_length=2000)
    exclude: str = Field(default='', max_length=2000)
    membership: Literal['all', 'joined', 'not_joined'] = 'all'
    active_days: int = Field(default=0, ge=0, le=365)
    limit: int = Field(default=100, ge=1, le=200)

    @model_validator(mode='after')
    def validate_filters(self):
        self.queries = list(dict.fromkeys(normalize(q) for q in self.queries))
        if any(not 3 <= len(q) <= 100 for q in self.queries):
            raise ValueError('Each query needs 3–100 characters')
        if self.max_members and self.min_members > self.max_members:
            raise ValueError('Invalid member range')
        return self


class ExportSpec(BaseModel):
    format: Literal['csv', 'txt']
    ids: list[int] | None = Field(default=None, min_length=1, max_length=200)


def community(entity):
    if not isinstance(entity, (types.Chat, types.Channel)) or not getattr(entity, 'username', None):
        return None
    if getattr(entity, 'creator', False) or getattr(entity, 'admin_rights', None) or getattr(entity, 'deactivated', False):
        return None
    return dict(id=utils.get_peer_id(entity), title=entity.title, username=entity.username,
                kind='group' if isinstance(entity, types.Chat) or getattr(entity, 'megagroup', False) else 'channel',
                members=getattr(entity, 'participants_count', None), joined=not bool(getattr(entity, 'left', False)),
                last_message_at=None, queries=[])


def eligible(row, spec):
    if spec.kind != 'all' and row['kind'] != spec.kind: return False
    count = row['members']
    if (spec.min_members or spec.max_members) and count is None: return False
    if spec.min_members and count < spec.min_members: return False
    if spec.max_members and count > spec.max_members: return False
    if spec.membership != 'all' and row['joined'] != (spec.membership == 'joined'): return False
    title = normalize(row['title'] + ' ' + row['username'])
    include, exclude = words(spec.include), words(spec.exclude)
    return (not include or any(w in title for w in include)) and not any(w in title for w in exclude)


def export_rows(rows, format):
    if format == 'txt':
        return '\n'.join('https://t.me/' + r['username'] for r in rows) + '\n'
    # Spreadsheet apps may otherwise execute untrusted titles as formulas.
    def cell(value):
        text = str(value) if value is not None else ''
        return "'" + text if text.lstrip().startswith(('=', '+', '-', '@')) or text.startswith(('\t', '\r', '\n')) else text
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, delimiter=';')
    writer.writerow(['Название', 'Ссылка', 'Тип', 'Участников', 'Аккаунт состоит', 'Последнее сообщение UTC', 'Поисковые запросы', 'Telegram ID'])
    for r in rows:
        writer.writerow([cell(r['title']), 'https://t.me/' + r['username'], 'Группа' if r['kind'] == 'group' else 'Канал',
                         r['members'], 'Да' if r['joined'] else 'Нет', r.get('last_message_at') or '', cell(' | '.join(r['queries'])), cell(r['id'])])
    return '\ufeff' + stream.getvalue()


class Directory:
    def __init__(self, get_store, get_runtime, prefix='/api/scanner/parser', before_start=None):
        self.get_store, self.get_runtime = get_store, get_runtime
        self.before_start = before_start
        self.task = None
        self.active_id = None
        self.pause = 5
        self.router = APIRouter(prefix=prefix)
        self.router.add_api_route('/runs', self.start, methods=['POST'])
        self.router.add_api_route('/runs', self.history, methods=['GET'])
        self.router.add_api_route('/runs/{run_id}', self.detail, methods=['GET'])
        self.router.add_api_route('/runs/{run_id}/cancel', self.cancel, methods=['POST'])
        self.router.add_api_route('/runs/{run_id}/export', self.export, methods=['POST'])

    @property
    def store(self): return self.get_store()
    @property
    def runtime(self): return self.get_runtime()

    def update(self, run_id, **values):
        values['updated'] = now()
        with self.store.db:
            self.store.db.execute('UPDATE parser_runs SET ' + ','.join(k + '=?' for k in values) + ' WHERE id=?', (*values.values(), run_id))

    def recover(self):
        with self.store.db:
            self.store.db.execute("UPDATE parser_runs SET state='interrupted',error='Поиск прерван перезапуском сервера. Результаты сохранены; запустите поиск повторно.',updated=? WHERE state='running'", (now(),))

    async def shutdown(self):
        if self.task and not self.task.done():
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await self.task
            self.update(self.active_id, state='interrupted', error='Поиск прерван перезапуском сервера. Результаты сохранены.')

    def run_row(self, run_id):
        rows = self.store.rows('SELECT * FROM parser_runs WHERE id=?', (run_id,))
        if not rows: raise HTTPException(404, 'Сохранённый поиск не найден.')
        row = rows[0]
        row['spec'], row['stats'] = json.loads(row['spec']), json.loads(row['stats'])
        row['count'] = self.store.db.execute('SELECT COUNT(*) FROM parser_results WHERE run_id=?', (run_id,)).fetchone()[0]
        return row

    async def history(self):
        return {'runs': [self.run_row(r['id']) for r in self.store.rows('SELECT id FROM parser_runs ORDER BY id DESC LIMIT 10')]}

    async def detail(self, run_id: int):
        run = self.run_row(run_id)
        run['results'] = [json.loads(r['data']) for r in self.store.rows('SELECT data FROM parser_results WHERE run_id=?', (run_id,))]
        run['results'].sort(key=lambda r: (-(r['members'] or 0), normalize(r['title'])))
        return run

    async def start(self, spec: SearchSpec):
        if self.before_start: self.before_start()
        runtime = self.runtime
        if not runtime.client or not runtime.me or not runtime.client.is_connected():
            raise HTTPException(409, 'Сначала подключите Telegram-аккаунт.')
        if self.task and not self.task.done(): raise HTTPException(409, 'Поиск уже идёт. Дождитесь завершения или остановите его.')
        remaining = int(getattr(runtime, 'discovery_next', 0) - time.time())
        if remaining > 5: raise HTTPException(429, f'Telegram просит подождать {remaining + 1} секунд перед следующим поиском.')
        with self.store.db:
            cursor = self.store.db.execute('INSERT INTO parser_runs(spec,state,total,created,updated) VALUES(?,?,?,?,?)',
                                           (spec.model_dump_json(), 'running', len(spec.queries), now(), now()))
            run_id = cursor.lastrowid
            self.store.db.execute('DELETE FROM parser_runs WHERE id NOT IN (SELECT id FROM parser_runs ORDER BY id DESC LIMIT 10)')
        self.active_id = run_id
        self.task = asyncio.create_task(self.work(run_id, spec))
        return self.run_row(run_id)

    async def cancel(self, run_id: int):
        row = self.run_row(run_id)
        if row['state'] == 'running' and self.active_id == run_id and self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await self.task
            self.update(run_id, state='cancelled')
        return await self.detail(run_id)

    async def export(self, run_id: int, spec: ExportSpec):
        rows = (await self.detail(run_id))['results']
        if spec.ids is not None:
            if not set(spec.ids) <= {r['id'] for r in rows}: raise HTTPException(400, 'Некоторые выбранные площадки отсутствуют в этом поиске.')
            rows = [r for r in rows if r['id'] in spec.ids]
        if not rows: raise HTTPException(400, 'Нет результатов для выгрузки.')
        return Response(export_rows(rows, spec.format), media_type='text/csv' if spec.format == 'csv' else 'text/plain',
                        headers={'Content-Disposition': f'attachment; filename="telegram-communities-{run_id}.{spec.format}"'})

    async def work(self, run_id, spec):
        seen, found = set(), {}
        stats = dict(seen=0, filtered=0, unavailable=0, inactive=0)
        try:
            for number, query in enumerate(spec.queries):
                await asyncio.sleep(max(0, getattr(self.runtime, 'discovery_next', 0) - time.time()))
                self.runtime.discovery_next = time.time() + self.pause
                response = await asyncio.wait_for(self.runtime.client(functions.contacts.SearchRequest(q=query, limit=50)), timeout=40)
                for entity in response.chats:
                    row = community(entity)
                    if not row: continue
                    cid = row['id']
                    if cid in found:
                        if query not in found[cid]['queries']:
                            found[cid]['queries'].append(query)
                            self.save(run_id, found[cid])
                        continue
                    if cid in seen: continue
                    seen.add(cid); stats['seen'] += 1
                    if not eligible(row, spec): stats['filtered'] += 1; continue
                    if spec.active_days:
                        try:
                            messages = await asyncio.wait_for(self.runtime.client.get_messages(entity, limit=1), timeout=15)
                            date = getattr(messages[0], 'date', None) if messages else None
                            if not date or date < datetime.now(timezone.utc) - timedelta(days=spec.active_days):
                                stats['inactive'] += 1; continue
                            row['last_message_at'] = date.isoformat()
                        except errors.FloodWaitError: raise
                        except Exception:
                            stats['unavailable'] += 1; continue
                        finally:
                            await asyncio.sleep(0.3)
                    row['queries'] = [query]
                    found[cid] = row
                    self.save(run_id, row)
                    if len(found) >= spec.limit: break
                self.update(run_id, done=number + 1, stats=json.dumps(stats))
                if len(found) >= spec.limit:
                    self.update(run_id, limited=1)
                    break
            self.update(run_id, state='completed')
        except asyncio.CancelledError:
            self.update(run_id, state='cancelled', stats=json.dumps(stats))
            raise
        except errors.FloodWaitError as exc:
            self.runtime.discovery_next = time.time() + exc.seconds + 1
            self.update(run_id, state='partial', error=f'Telegram просит подождать {exc.seconds} секунд. Найденные площадки сохранены. Повторите поиск позднее.', stats=json.dumps(stats))
        except Exception as exc:
            # Never return raw exceptions containing account or connection information.
            self.update(run_id, state='partial', error=f'Поиск прерван ({type(exc).__name__}). Найденные площадки сохранены. Проверьте подключение и повторите поиск.', stats=json.dumps(stats))

    def save(self, run_id, row):
        with self.store.db:
            self.store.db.execute('INSERT OR REPLACE INTO parser_results VALUES(?,?,?)', (run_id, row['id'], json.dumps(row, ensure_ascii=False)))
