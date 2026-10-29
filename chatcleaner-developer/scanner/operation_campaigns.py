"""Reusable publication settings and identity-bound manual reply snippets.

Saving a profile never selects recipients, schedules an operation, edits a Telegram
profile or sends a message. Jobs retain their own copy of the profile they used.
"""
import json
import re
from collections import Counter

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator

from engine import normalize, now, phrase_pattern


class SettingsSpec(BaseModel):
    model_config = {'extra': 'forbid'}

    @model_validator(mode='after')
    def valid_text(self):
        for value in self.model_dump().values():
            for text in value if isinstance(value, list) else [value]:
                if isinstance(text, str):
                    try:
                        text.encode('utf-8')
                    except UnicodeEncodeError:
                        raise ValueError('Текст содержит неподдерживаемый символ. Вставьте его заново.') from None
        return self


class CampaignSpec(SettingsSpec):
    id: int | None = Field(default=None, gt=0)
    revision: int | None = Field(default=None, gt=0)
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(default='', max_length=1000)
    text: str = Field(default='', max_length=4096)
    media: list[str] = Field(default_factory=list, max_length=10)
    delay_seconds: int = Field(default=60, ge=30, le=3600)
    daily_limit: int = Field(default=20, ge=1, le=100)
    keywords: list[str] = Field(default_factory=list, max_length=50)
    stop_words: list[str] = Field(default_factory=list, max_length=50)
    contact: str = Field(default='', max_length=160)

    @model_validator(mode='after')
    def identifiers(self):
        if (self.id is None) != (self.revision is None):
            raise ValueError('Для изменения профиля нужны его ID и версия.')
        return self


class ArchiveSpec(SettingsSpec):
    revision: int = Field(gt=0)
    archived: bool


class ReplyTemplateSpec(SettingsSpec):
    telegram_id: int = Field(gt=0)
    id: int | None = Field(default=None, gt=0)
    revision: int | None = Field(default=None, gt=0)
    name: str = Field(min_length=1, max_length=100)
    text: str = Field(min_length=1, max_length=4096)

    @model_validator(mode='after')
    def identifiers(self):
        if (self.id is None) != (self.revision is None):
            raise ValueError('Для изменения шаблона нужны его ID и версия.')
        return self


class ReplyTemplateArchive(ArchiveSpec):
    telegram_id: int = Field(gt=0)


def _terms(values):
    result = []
    for value in values:
        value = normalize(value)
        if not value or len(value) > 80:
            raise HTTPException(400, 'Ключевые слова и стоп-слова: непустые фразы до 80 символов.')
        if value not in result:
            result.append(value)
    return result


def classify_reply(text, snapshot):
    """A literal label for an already attributed group reply, never a send rule."""
    text = normalize(text)
    keywords = [term for term in snapshot.get('keywords', []) if phrase_pattern(term).search(text)]
    stopped = [term for term in snapshot.get('stop_words', []) if phrase_pattern(term).search(text)]
    status = ('stop_word' if stopped else 'keyword_match' if keywords else
              'no_keyword_match' if snapshot.get('keywords') else 'unfiltered')
    return dict(status=status, matched_keywords=keywords, matched_stop_words=stopped)


class OperationCampaigns:
    def __init__(self, ops):
        self.ops, self.store = ops, ops.store
        self.router = APIRouter()
        for path, fn, methods in [
            ('/campaigns', self.list_campaigns, ['GET']),
            ('/campaigns', self.save_campaign, ['POST']),
            ('/campaigns/{cid}/archive', self.archive_campaign, ['POST']),
            ('/accounts/{aid}/reply-templates', self.list_templates, ['GET']),
            ('/accounts/{aid}/reply-templates', self.save_template, ['POST']),
            ('/accounts/{aid}/reply-templates/{tid}/archive', self.archive_template, ['POST']),
        ]:
            self.router.add_api_route(path, fn, methods=methods)

    def initialize(self):
        self.store.db.executescript('''
        CREATE TABLE IF NOT EXISTS op_campaigns(
          id INTEGER PRIMARY KEY AUTOINCREMENT,revision INTEGER NOT NULL,name TEXT NOT NULL,
          data TEXT NOT NULL,archived INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS op_reply_templates(
          id INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,telegram_id INTEGER NOT NULL,
          revision INTEGER NOT NULL,name TEXT NOT NULL,text TEXT NOT NULL,archived INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS op_reply_templates_identity
          ON op_reply_templates(account,telegram_id,archived,id);
        ''')

    def campaign_row(self, cid):
        rows = self.store.rows('SELECT * FROM op_campaigns WHERE id=?', (cid,))
        if not rows:
            raise HTTPException(404, 'Профиль кампании не найден.')
        row = rows[0]
        return {**json.loads(row['data']), **{k: row[k] for k in
                ('id', 'revision', 'name', 'created_at', 'updated_at')}, 'archived': bool(row['archived'])}

    async def list_campaigns(self, include_archived: bool = False):
        rows = self.store.rows('SELECT id FROM op_campaigns' + ('' if include_archived else ' WHERE archived=0') + ' ORDER BY updated_at DESC,id DESC')
        return dict(campaigns=[self.campaign_row(row['id']) for row in rows])

    def _content(self, spec):
        if not spec.name.strip():
            raise HTTPException(400, 'Укажите название профиля кампании.')
        if not spec.text.strip() and not spec.media:
            raise HTTPException(400, 'Добавьте текст или фото профиля кампании.')
        if len(spec.text.encode('utf-16-le')) // 2 > (1024 if spec.media else 4096):
            raise HTTPException(400, 'Слишком длинный текст: до 4096, подпись к фото до 1024 символов.')
        if len(set(spec.media)) != len(spec.media):
            raise HTTPException(400, 'Повторяющиеся фото.')
        for mid in spec.media:
            if not re.fullmatch(r'[a-f0-9]{32}', mid):
                raise HTTPException(400, 'Выберите загруженное в панель фото.')
            row = self.ops.media_store.media_row(mid)
            root = self.ops.media_store.data.resolve()
            path = (root / row['path']).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise HTTPException(400, 'Фото недоступно. Загрузите его заново в панель.')
        return dict(description=spec.description.strip(), text=spec.text, media=spec.media,
                    delay_seconds=spec.delay_seconds, daily_limit=spec.daily_limit,
                    keywords=_terms(spec.keywords), stop_words=_terms(spec.stop_words),
                    contact=spec.contact.strip())

    async def save_campaign(self, spec: CampaignSpec):
        async with self.ops.lock:
            data, stamp = self._content(spec), now()
            with self.store.db:
                if spec.id is None:
                    if self.store.rows('SELECT COUNT(*) AS n FROM op_campaigns')[0]['n'] >= 200:
                        raise HTTPException(400, 'Достигнут предел 200 профилей кампаний, включая архив.')
                    cur = self.store.db.execute('INSERT INTO op_campaigns(revision,name,data,created_at,updated_at) VALUES(1,?,?,?,?)',
                                               (spec.name.strip(), json.dumps(data, ensure_ascii=False), stamp, stamp))
                    cid = cur.lastrowid
                else:
                    old = self.campaign_row(spec.id)
                    if old['revision'] != spec.revision:
                        raise HTTPException(409, 'Профиль изменён в другой вкладке. Обновите список перед сохранением.')
                    if old['archived']:
                        raise HTTPException(409, 'Профиль в архиве. Сначала восстановите его.')
                    cur = self.store.db.execute('UPDATE op_campaigns SET revision=revision+1,name=?,data=?,updated_at=? WHERE id=? AND revision=? AND archived=0',
                        (spec.name.strip(), json.dumps(data, ensure_ascii=False), stamp, spec.id, spec.revision))
                    if cur.rowcount != 1:
                        raise HTTPException(409, 'Версия профиля изменилась. Обновите список.')
                    cid = spec.id
            return dict(campaign=self.campaign_row(cid))

    async def archive_campaign(self, cid: int, spec: ArchiveSpec):
        async with self.ops.lock:
            self.campaign_row(cid)
            with self.store.db:
                cur = self.store.db.execute('UPDATE op_campaigns SET revision=revision+1,archived=?,updated_at=? WHERE id=? AND revision=?',
                                           (int(spec.archived), now(), cid, spec.revision))
                if cur.rowcount != 1:
                    raise HTTPException(409, 'Версия профиля изменилась. Обновите список.')
            return dict(campaign=self.campaign_row(cid))

    def snapshot(self, cid, revision):
        profile = self.campaign_row(cid)
        if profile['revision'] != revision:
            raise HTTPException(409, 'Профиль кампании изменился. Обновите его и проверьте черновик.')
        if profile['archived']:
            raise HTTPException(409, 'Профиль кампании в архиве. Выберите действующий профиль.')
        return profile

    def _identity(self, aid, telegram_id=None):
        identity = self.ops.account_row(aid)['telegram_id']
        if telegram_id is not None and identity != telegram_id:
            raise HTTPException(409, 'Личность аккаунта изменилась. Обновите панель перед сохранением шаблона.')
        return identity

    def _template(self, aid, identity, tid):
        rows = self.store.rows('SELECT * FROM op_reply_templates WHERE id=? AND account=? AND telegram_id=?', (tid, aid, identity))
        if not rows:
            raise HTTPException(404, 'Шаблон ответа этого аккаунта не найден.')
        return self._template_json(rows[0])

    @staticmethod
    def _template_json(row):
        return {**{k: row[k] for k in ('id', 'revision', 'name', 'text', 'created_at', 'updated_at')},
                'archived': bool(row['archived'])}

    async def list_templates(self, aid: str, include_archived: bool = False):
        identity = self._identity(aid)
        rows = self.store.rows('SELECT * FROM op_reply_templates WHERE account=? AND telegram_id=?' +
                               ('' if include_archived else ' AND archived=0') + ' ORDER BY updated_at DESC,id DESC', (aid, identity))
        return dict(account=aid, telegram_id=identity, templates=[self._template_json(row) for row in rows])

    async def save_template(self, aid: str, spec: ReplyTemplateSpec):
        async with self.ops.lock:
            identity, stamp = self._identity(aid, spec.telegram_id), now()
            if not spec.name.strip() or not spec.text.strip():
                raise HTTPException(400, 'Укажите название и текст шаблона ответа.')
            if len(spec.text.encode('utf-16-le')) // 2 > 4096:
                raise HTTPException(400, 'Ответ должен быть короче 4096 символов Telegram.')
            with self.store.db:
                if spec.id is None:
                    if self.store.rows('SELECT COUNT(*) AS n FROM op_reply_templates WHERE account=? AND telegram_id=?', (aid, identity))[0]['n'] >= 200:
                        raise HTTPException(400, 'Достигнут предел 200 шаблонов аккаунта, включая архив.')
                    cur = self.store.db.execute('INSERT INTO op_reply_templates(account,telegram_id,revision,name,text,created_at,updated_at) VALUES(?,?,1,?,?,?,?)',
                        (aid, identity, spec.name.strip(), spec.text, stamp, stamp))
                    tid = cur.lastrowid
                else:
                    old = self._template(aid, identity, spec.id)
                    if old['archived']:
                        raise HTTPException(409, 'Шаблон в архиве. Сначала восстановите его.')
                    cur = self.store.db.execute('UPDATE op_reply_templates SET revision=revision+1,name=?,text=?,updated_at=? WHERE id=? AND account=? AND telegram_id=? AND revision=? AND archived=0',
                        (spec.name.strip(), spec.text, stamp, spec.id, aid, identity, spec.revision))
                    if cur.rowcount != 1:
                        raise HTTPException(409, 'Шаблон изменён в другой вкладке. Обновите список перед сохранением.')
                    tid = spec.id
            return dict(account=aid, telegram_id=identity, template=self._template(aid, identity, tid))

    async def archive_template(self, aid: str, tid: int, spec: ReplyTemplateArchive):
        async with self.ops.lock:
            identity = self._identity(aid, spec.telegram_id)
            self._template(aid, identity, tid)
            with self.store.db:
                cur = self.store.db.execute('UPDATE op_reply_templates SET revision=revision+1,archived=?,updated_at=? WHERE id=? AND account=? AND telegram_id=? AND revision=?',
                    (int(spec.archived), now(), tid, aid, identity, spec.revision))
                if cur.rowcount != 1:
                    raise HTTPException(409, 'Версия шаблона изменилась. Обновите список.')
            return dict(account=aid, telegram_id=identity, template=self._template(aid, identity, tid))

    def tracking_data(self, job, rows, events):
        spec = job.get('spec')
        if isinstance(spec, str):
            spec = json.loads(spec)
        snapshot = (spec or {}).get('campaign_snapshot')
        if not snapshot:
            return {}
        counts = Counter()
        for row, event in zip(reversed(rows), events):
            if (row['kind'] == 'group_reply' and row['attribution'] == 'exact_reply'
                    and row['account'] == job['account'] and row['telegram_id'] == job['telegram_id']
                    and row['job_id'] == job['id']):
                event['classification'] = classify_reply(row['text'], snapshot)
                counts[event['classification']['status']] += 1
        return dict(campaign={key: snapshot[key] for key in ('id', 'revision', 'name', 'keywords', 'stop_words', 'contact')},
                    classification_summary=dict(scope='visible_events', counts=dict(counts)))
