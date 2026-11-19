"""Loopback-only session auth for independently authenticated panel hosts.
No Telegram access, password plaintext storage, or credential logging.
"""
from contextlib import closing, suppress
from hashlib import pbkdf2_hmac, sha256
from http.cookies import SimpleCookie, CookieError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time

COOKIE = '__Host-panel_session'
ITERATIONS = 600_000
SESSION_TTL = 12 * 60 * 60
REMEMBER_TTL = 7 * 24 * 60 * 60
FAIL_WINDOW = 15 * 60
FAIL_LIMIT = 6


def password_hash(password, salt, iterations=ITERATIONS):
    return pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(salt), iterations).hex()


def make_credentials(username, password, origin):
    salt = secrets.token_hex(24)
    return dict(username=username, origin=origin, salt=salt,
                iterations=ITERATIONS, password_hash=password_hash(password, salt))


def cookie_token(header):
    try:
        cookies = SimpleCookie()
        cookies.load(header or '')
        value = cookies[COOKIE].value if COOKIE in cookies else ''
        return value if re.fullmatch(r'[A-Za-z0-9_-]{43}', value) else None
    except CookieError:
        return None


def cookie_header(token='', remember=False, delete=False):
    value = f'{COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Lax'
    if delete:
        value += '; Max-Age=0'
    elif remember:
        value += f'; Max-Age={REMEMBER_TTL}'
    return value


class AuthStore:
    def __init__(self, path, sites, clock=time.time):
        self.path, self.sites, self.clock = str(path), sites, clock
        self.lock = threading.RLock()
        # Keep session checks responsive while expensive password hashes run.
        self.login_slots = threading.BoundedSemaphore(4)
        self.path_parent = Path(path).parent
        self.path_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        for site in sites.values():
            names = set()
            for user in [site, *site.get('additional_users',[])]:
                if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,64}', user['username']) or user['username'] in names:
                    raise ValueError('Invalid or duplicate configured username')
                if user.get('role','admin') not in ('owner','admin'):
                    raise ValueError('Invalid configured role')
                names.add(user['username'])
            if not site['origin'].startswith('https://'):
                raise ValueError('HTTPS origin required')
        with closing(self.db()) as db:
            db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY,site TEXT NOT NULL,username TEXT NOT NULL,version TEXT NOT NULL,expires REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires);
            CREATE TABLE IF NOT EXISTS attempts(site TEXT NOT NULL,ip TEXT NOT NULL,started REAL NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(site,ip));
            ''')

    def db(self):
        return sqlite3.connect(self.path, timeout=10)

    def user_config(self, site, username):
        config = self.sites.get(site)
        if not config:
            return None
        return next((u for u in [config, *config.get('additional_users',[])] if u['username'] == username), None)

    def identity(self, site, user):
        return dict(username=user['username'], role=user.get('role','owner' if site == 'personal' else 'admin'))

    def reserve_attempt(self, site, ip):
        current = self.clock()
        with self.lock, closing(self.db()) as db, db:
            db.execute('DELETE FROM attempts WHERE started<=?', (current - FAIL_WINDOW,))
            db.execute('DELETE FROM sessions WHERE expires<=?', (current,))
            row = db.execute('SELECT started,count FROM attempts WHERE site=? AND ip=?', (site,ip)).fetchone()
            if row and row[1] >= FAIL_LIMIT:
                return max(1, int(row[0] + FAIL_WINDOW - current) + 1)
            db.execute('INSERT INTO attempts VALUES(?,?,?,1) ON CONFLICT(site,ip) DO UPDATE SET count=count+1', (site,ip,current))
        return 0

    def authenticate(self, site, ip, username, password, remember=False, old_token=None):
        if not self.login_slots.acquire(blocking=False):
            return dict(status=503, detail='Вход сейчас занят. Повторите через несколько секунд.', retry_after=3)
        try:
            return self._authenticate(site, ip, username, password, remember, old_token)
        finally:
            self.login_slots.release()

    def _authenticate(self, site, ip, username, password, remember=False, old_token=None):
        config = self.sites.get(site)
        if not config:
            return dict(status=401, detail='Неверный логин или пароль.')
        wait = self.reserve_attempt(site, ip)
        if wait:
            return dict(status=429, detail='Слишком много попыток входа. Подождите 15 минут и повторите.', retry_after=wait)
        user = self.user_config(site, username)
        # Unknown usernames still perform the same expensive hash operation.
        candidate = user or config
        actual = password_hash(password, candidate['salt'], candidate['iterations'])
        valid_password = hmac.compare_digest(actual, candidate['password_hash'])
        if not (valid_password and user):
            return dict(status=401, detail='Неверный логин или пароль.')
        token = secrets.token_urlsafe(32)
        expires = self.clock() + (REMEMBER_TTL if remember else SESSION_TTL)
        with self.lock, closing(self.db()) as db, db:
            if old_token:
                db.execute('DELETE FROM sessions WHERE site=? AND token_hash=?', (site,sha256(old_token.encode()).hexdigest()))
            db.execute('DELETE FROM attempts WHERE site=? AND ip=?', (site,ip))
            db.execute('INSERT INTO sessions VALUES(?,?,?,?,?)', (sha256(token.encode()).hexdigest(),site,user['username'],user['password_hash'],expires))
        return dict(status=200, **self.identity(site,user), token=token, expires=expires)

    def verify(self, site, token):
        if site not in self.sites or not token:
            return None
        with closing(self.db()) as db:
            row = db.execute('SELECT username,version,expires FROM sessions WHERE site=? AND token_hash=?', (site,sha256(token.encode()).hexdigest())).fetchone()
        user = self.user_config(site, row[0]) if row else None
        if not user or row[2] <= self.clock() or not hmac.compare_digest(row[1], user['password_hash']):
            return None
        return dict(**self.identity(site,user), expires=row[2])

    def logout(self, site, token):
        if token:
            with self.lock, closing(self.db()) as db, db:
                db.execute('DELETE FROM sessions WHERE site=? AND token_hash=?', (site,sha256(token.encode()).hexdigest()))


def handler_for(store):
    class Handler(BaseHTTPRequestHandler):
        # Avoid ever logging user-provided headers, request bodies or tokens.
        def log_message(self, *args):
            pass

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def response(self, status, body=None, headers=None):
            encoded = json.dumps(body or {}, ensure_ascii=False).encode() if status != 204 else b''
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(encoded)))
            for key,value in (headers or {}).items():
                self.send_header(key,value)
            self.end_headers()
            if encoded and self.command != 'HEAD':
                with suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(encoded)

        def do_GET(self):
            site = self.headers.get('X-Auth-Site','')
            if self.path not in ('/verify','/auth/me'):
                return self.response(404)
            user = store.verify(site,cookie_token(self.headers.get('Cookie')))
            if not user:
                return self.response(401, {'detail':'Войдите в панель.'})
            if self.path == '/verify':
                return self.response(204, headers={'X-Auth-User':user['username'],'X-Auth-Role':user['role']})
            return self.response(200, user)

        def do_HEAD(self):
            self.do_GET()

        def do_POST(self):
            site = self.headers.get('X-Auth-Site','')
            config = store.sites.get(site)
            if self.path not in ('/auth/login','/auth/logout'):
                return self.response(404)
            if not config or self.headers.get('Origin') != config['origin'] or self.headers.get('X-Panel-Request') != '1':
                return self.response(403, {'detail':'Откройте страницу входа заново.'})
            try:
                length = int(self.headers.get('Content-Length','0'))
                if not 0 < length <= 4096:
                    return self.response(413, {'detail':'Проверьте размер запроса.'})
                if self.headers.get('Content-Type','').split(';')[0].strip() != 'application/json':
                    return self.response(415, {'detail':'Откройте страницу входа заново.'})
                data = json.loads(self.rfile.read(length))
                if not isinstance(data,dict): raise ValueError()
            except (ValueError,UnicodeError):
                return self.response(400, {'detail':'Проверьте заполнение полей.'})
            token = cookie_token(self.headers.get('Cookie'))
            if self.path == '/auth/logout':
                store.logout(site, token)
                return self.response(200, {'ok':True}, {'Set-Cookie':cookie_header(delete=True)})
            username, password = data.get('username'), data.get('password')
            if not isinstance(username,str) or not isinstance(password,str) or not 1 <= len(username) <= 128 or not 1 <= len(password) <= 512 or not isinstance(data.get('remember',False),bool):
                return self.response(400, {'detail':'Введите логин и пароль.'})
            result = store.authenticate(site,self.headers.get('X-Auth-Client-IP','unknown'),username.strip(),password,data.get('remember',False),token)
            if result['status'] != 200:
                extra = {'Retry-After':str(result['retry_after'])} if 'retry_after' in result else {}
                return self.response(result['status'], {'detail':result['detail']}, extra)
            self.response(200, {'ok':True,'username':result['username'],'role':result['role']}, {'Set-Cookie':cookie_header(result['token'],data.get('remember',False))})
    return Handler


class LimitedServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128
    def __init__(self, *args, **kwargs):
        self.slots = threading.BoundedSemaphore(32)
        super().__init__(*args, **kwargs)
    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            # A finite retryable response is preferable to an unexplained reset.
            with suppress(OSError):
                request.settimeout(0.1)
                request.sendall(b'HTTP/1.0 503 Service Unavailable\r\nRetry-After: 3\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            request.close()
            return
        try:
            super().process_request(request,address)
        except BaseException:
            self.slots.release()
            raise
    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request,address)
        finally:
            self.slots.release()


if __name__ == '__main__':
    os.umask(0o077)
    sites = json.loads(Path(os.environ['PANEL_AUTH_CONFIG']).read_text())
    state = AuthStore(Path(os.environ.get('PANEL_AUTH_DATA','/var/lib/panel-auth'))/'sessions.sqlite3',sites)
    LimitedServer(('127.0.0.1',8790),handler_for(state)).serve_forever()
