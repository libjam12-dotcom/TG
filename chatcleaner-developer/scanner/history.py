"""Explicit bounded history analysis, separate from the live polling cursor."""
import asyncio
import contextlib
import time
from datetime import datetime, timezone, timedelta
from typing import Literal
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from telethon import errors, utils
from engine import explain, now, normalize


class HistorySpec(BaseModel):
    sources: list[int] = Field(default_factory=list, max_length=100)
    days: int = Field(default=3, ge=1, le=7)
    limit: int = Field(default=500, ge=10, le=2000)
    mode: Literal['preview', 'import'] = 'preview'


class HistoryRunner:
    def __init__(self, get_store, get_runtime, prefix='/api/scanner/history', before_start=None):
        self.get_store, self.get_runtime = get_store, get_runtime
        self.before_start = before_start
        self.task = None
        self.job = None
        self.pause = 0.5
        self.router = APIRouter(prefix=prefix)
        self.router.add_api_route('', self.start, methods=['POST'])
        self.router.add_api_route('', self.detail_http, methods=['GET'])
        self.router.add_api_route('/stop', self.stop, methods=['POST'])

    @property
    def store(self): return self.get_store()
    @property
    def runtime(self): return self.get_runtime()

    def save(self):
        self.job['updated'] = now()
        self.store.put('history_job', self.job)

    def summary(self):
        job = self.job or self.store.get('history_job')
        return {k:v for k,v in job.items() if k != 'samples'} if job else None

    async def detail(self): return self.job or self.store.get('history_job')

    async def detail_http(self):
        # History contains only JSON values. Avoid recursively reconstructing the
        # whole preview in FastAPI for every status poll on the shared small VM.
        return JSONResponse(await self.detail())

    def recover(self):
        self.job = self.store.get('history_job')
        if self.job and self.job['state'] == 'running':
            self.job.update(state='interrupted', error='Проверка прервана перезапуском. Уже сохранённые находки остались в панели.')
            self.save()

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await self.task
            self.job['state'] = 'cancelled'
            self.save()
        return self.summary()

    async def shutdown(self):
        if self.task and not self.task.done():
            await self.stop()
            self.job['state'] = 'interrupted'
            self.save()

    async def start(self, spec: HistorySpec):
        if self.before_start: self.before_start()
        if not self.runtime.client or not self.runtime.me or not self.runtime.client.is_connected():
            raise HTTPException(409, 'Сначала подключите Telegram-аккаунт.')
        if self.task and not self.task.done(): raise HTTPException(409, 'Проверка истории уже идёт.')
        wait = self.store.get('history_retry_at', 0) - time.time()
        if wait > 0: raise HTTPException(429, f'Telegram просит подождать {int(wait)+1} секунд.')
        available = {s['id']:s for s in self.store.rows('SELECT * FROM sources WHERE enabled=1') if s['id'] != self.store.get('destination')}
        ids = list(dict.fromkeys(spec.sources or list(available)))
        if not ids or not set(ids) <= set(available): raise HTTPException(400, 'Выберите включённые источники.')
        self.job = dict(state='running', mode=spec.mode, days=spec.days, limit=spec.limit, started=now(), updated=now(),
                        done=0, total=len(ids), read=0, text=0, topic_hits=0, matched=0, imported=0, duplicate=0,
                        no_keywords=0, no_intent=0, excluded=0, skipped=0, sources=[], samples=[], error=None)
        self.save()
        self.task = asyncio.create_task(self.work(spec, [available[i] for i in ids]))
        return self.summary()

    async def work(self, spec, sources):
        since = datetime.now(timezone.utc) - timedelta(days=spec.days)
        try:
            for source in sources:
                row = dict(id=source['id'], title=source['title'], username=source['username'], read=0, text=0,
                           topic_hits=0, matched=0, imported=0, duplicate=0, no_keywords=0, no_intent=0, excluded=0,
                           skipped=0, oldest=None, newest=None, capped=False, error=None)
                self.job['sources'].append(row)
                try:
                    await asyncio.wait_for(self.check_source(spec, source, row, since), timeout=180)
                except errors.FloodWaitError: raise
                except Exception as exc:
                    row['error'] = f'Не удалось прочитать источник ({type(exc).__name__}).'
                self.job['done'] += 1
                self.save()
                await asyncio.sleep(self.pause)
            self.job['state'] = 'partial' if any(r['error'] for r in self.job['sources']) else 'completed'
        except asyncio.CancelledError:
            self.job['state'] = 'cancelled'
            raise
        except errors.FloodWaitError as exc:
            self.store.put('history_retry_at', time.time()+exc.seconds+1)
            self.job.update(state='partial', error=f'Telegram просит подождать {exc.seconds} секунд. Прочитанные сообщения учтены, сохранённые находки остались в панели.')
        except Exception as exc:
            self.job.update(state='partial', error=f'Проверка прервана ({type(exc).__name__}). Результаты сохранены.')
        finally:
            self.save()

    async def check_source(self, spec, source, row, since):
        # Read newest first. Never modify the cursor used by the live scanner.
        rules = [r for r in self.store.rules() if r['enabled'] and source['id'] in r['sources']]
        if not rules:
            row['error'] = 'Нет включённых тематик для этого источника.'
            return
        entity = await self.runtime.client.get_entity(source['id'])
        if getattr(entity, 'creator', False) or getattr(entity, 'admin_rights', None):
            row['error'] = 'Административная площадка исключена из анализа внешних источников.'
            return
        def count(key):
            row[key] += 1
            self.job[key] += 1
        async for message in self.runtime.client.iter_messages(entity, limit=spec.limit, wait_time=1):
            # Cached Telegram batches can otherwise monopolize the event loop.
            # Yield even for outgoing, empty and too-old messages.
            await asyncio.sleep(0)
            if message.date < since: break
            count('read')
            row['oldest'] = message.date.isoformat()
            row['newest'] = row['newest'] or row['oldest']
            if message.out or not message.message:
                count('skipped'); continue
            count('text')
            decisions = [dict(id=r['id'], name=r['name'], **explain(message.message, r)) for r in rules]
            relevant = [r for r in decisions if r['keywords']]
            accepted = [r for r in relevant if r['reason'] == 'matched']
            if relevant: count('topic_hits')
            reason = 'matched' if accepted else 'no_intent' if any(r['reason']=='no_intent' for r in relevant) else 'excluded' if relevant else 'no_keywords'
            count(reason)
            link = f'https://t.me/{source["username"]}/{message.id}' if source['username'] else f'https://t.me/c/{utils.resolve_id(source["id"])[0]}/{message.id}'
            if relevant and sum(s['reason']==reason for s in self.job['samples']) < 30 and not any(normalize(s['text']) == normalize(message.message[:4000]) for s in self.job['samples']):
                self.job['samples'].append(dict(source=source['username'], message_id=message.id, date=message.date.isoformat(), text=message.message[:4000], reason=reason, rules=relevant, link=link))
            if accepted and spec.mode == 'import':
                sender = await message.get_sender()
                username = getattr(sender, 'username', None)
                author = '@'+username if username else ' '.join(filter(None,[getattr(sender,'first_name',''),getattr(sender,'last_name','')])) or 'Автор не указан'
                lead_id = self.store.ingest(source['id'], message.id, message.message, author, message.sender_id, link, message.date.isoformat(), origin='history')
                capture=getattr(self.runtime,'capture_contact_evidence',None)
                if capture: capture(lead_id,message,sender)
                count('imported' if lead_id else 'duplicate')
            if row['read'] % 50 == 0:
                self.save()
                await asyncio.sleep(0)
        row['capped'] = row['read'] >= spec.limit
