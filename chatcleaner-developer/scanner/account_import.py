"""Bounded, offline readers for owner-supplied Telethon and Telegram Desktop files.

No archive extraction or application imports; no network calls. Only an auth key and
its DC are copied into a NEW Telethon session. The uploaded database is never used
as the application's live database. Format references: Telegram Desktop local
storage / AuthKey and Telethon SQLiteSession. OpenTele is used only by fixture tests.
"""
import hashlib
import hmac
import io
import re
import sqlite3
import stat
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from telethon.crypto import AES, AuthKey
from telethon.sessions import StringSession

MAX_FILE = 8 * 1024 * 1024
MAX_REQUEST = 12 * 1024 * 1024
MAX_UNPACKED = 32 * 1024 * 1024
DESKTOP_API = {'id': 2040, 'hash': 'd821608a7f555f559cbe2ef2879fe6e1'}
DC_ADDRESS = {1:'149.154.175.53',2:'149.154.167.51',3:'149.154.175.100',4:'149.154.167.91',5:'91.108.56.130'}

class ImportError(ValueError):
    """Only fixed, non-secret messages may leave this module."""

def session_value(dc, address, port, key):
    if not isinstance(key, bytes) or len(key) != 256 or not any(key):
        raise ImportError('В файле нет действительного ключа сессии. Войдите по номеру и коду.')
    s = StringSession(); s.set_dc(dc, address, port); s.auth_key = AuthKey(key)
    return s.save()

def read_sqlite(blob):
    if not blob.startswith(b'SQLite format 3\0'):
        raise ImportError('Ожидается SQLite .session из Telethon. Для StringSession используйте импорт JSON.')
    with tempfile.TemporaryDirectory(prefix='chatcleaner-import-') as directory:
        path = Path(directory)/'input.session'; path.write_bytes(blob); path.chmod(0o600)
        db = sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1', uri=True)
        try:
            db.execute('PRAGMA trusted_schema=OFF'); db.execute('PRAGMA query_only=ON')
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024*1024)
            db.set_progress_handler(lambda: 1, 100000)
            tables = {r[0]:r[1] for r in db.execute('SELECT name,type FROM sqlite_master')}
            if tables.get('sessions') != 'table': raise ImportError('В файле нет таблицы сессии Telethon.')
            columns = {r[1] for r in db.execute('PRAGMA table_info(sessions)')}
            if 'api_id' in columns or 'is_bot' in columns:
                raise ImportError('Это сессия Pyrogram. Она пока не поддерживается; используйте вход по номеру или Telethon .session.')
            if not {'dc_id','server_address','port','auth_key'} <= columns:
                raise ImportError('Неизвестный формат .session. Поддерживается SQLite Telethon.')
            rows = db.execute('SELECT dc_id,server_address,port,auth_key FROM sessions LIMIT 2').fetchall()
            if len(rows)!=1: raise ImportError('Ожидается ровно одна сессия в файле.')
            dc, address, port, key = rows[0]
            if type(dc) is not int or type(port) is not int or not isinstance(address,str):
                raise ImportError('Повреждены параметры дата-центра в .session.')
            return {'session_string':session_value(dc,address,port,key)}
        except sqlite3.Error:
            raise ImportError('Не удалось прочитать .session. Закройте исходный клиент и сделайте целую копию файла.') from None
        finally: db.close()

class Reader:
    def __init__(self, data): self.data, self.pos = data, 0
    def take(self, n):
        if n<0 or self.pos+n>len(self.data): raise ImportError('Неполные данные tdata.')
        value=self.data[self.pos:self.pos+n]; self.pos+=n; return value
    def number(self, n=4, signed=False): return int.from_bytes(self.take(n),'big',signed=signed)
    def array(self):
        n=self.number()
        if n==0xffffffff:return b''
        if n>1024*1024: raise ImportError('Слишком большое поле в tdata.')
        return self.take(n)

def desktop_file(files, base):
    # Telegram's safe-file order: current file, then backups.
    for suffix in ('s','1','0'):
        blob=files.get(base+suffix)
        if not blob or len(blob)<24 or blob[:4]!=b'TDF$':continue
        body=blob[8:-16]
        checksum=hashlib.md5(body+len(body).to_bytes(4,'little')+blob[4:8]+b'TDF$').digest()
        if hmac.compare_digest(checksum,blob[-16:]):return Reader(body)
    raise ImportError('Отсутствует или повреждён файл авторизации tdata. Нужна полная копия после закрытия Telegram Desktop.')

def decrypt_local(blob,key):
    if len(key)!=256 or len(blob)<32 or len(blob)%16:raise ImportError('Повреждены зашифрованные данные tdata.')
    msg=blob[:16]; sha=lambda b:hashlib.sha1(b).digest()
    a=sha(msg+key[8:40]);b=sha(key[40:56]+msg+key[56:72]);c=sha(key[72:104]+msg);d=sha(msg+key[104:136])
    plain=AES.decrypt_ige(blob[16:],a[:8]+b[8:20]+c[4:16],a[8:20]+b[:8]+c[16:20]+d[:8])
    if not hmac.compare_digest(sha(plain)[:16],msg):raise ImportError('Неверный локальный код Telegram Desktop или повреждён tdata. Это не облачный пароль 2FA.')
    n=int.from_bytes(plain[:4],'little')
    if not 4<=n<=len(plain) or n<=len(plain)-16:raise ImportError('Повреждена длина данных tdata.')
    return Reader(plain[4:n])

def read_tdata(blob, passcode='', account_index=0):
    # Do not extract ZIP paths to disk. Validate ALL entries, then read only small
    # auth files. Media/cache is neither interpreted nor retained.
    files={}; total=0; seen=set()
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            entries=archive.infolist()
            if len(entries)>1024:raise ImportError('В ZIP слишком много файлов. Уберите кэш и медиа из копии tdata.')
            for info in entries:
                name=info.filename; p=PurePosixPath(name); mode=info.external_attr>>16
                if '\\' in name or '\0' in name or ':' in name or p.is_absolute() or '..' in p.parts or len(name)>250 or len(p.parts)>8:
                    raise ImportError('ZIP содержит небезопасный путь.')
                if name in seen:raise ImportError('ZIP содержит повторяющиеся имена файлов.')
                seen.add(name)
                if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode))):
                    raise ImportError('Ссылки и специальные файлы в ZIP не поддерживаются.')
                if info.flag_bits & 1:raise ImportError('Уберите пароль самого ZIP. Локальный код tdata вводится отдельно.')
                total+=info.file_size
                if total>MAX_UNPACKED or info.file_size>MAX_FILE or info.file_size>max(1024*1024,info.compress_size*200):
                    raise ImportError('Архив слишком большой после распаковки. Уберите кэш и медиа из копии tdata.')
                if re.fullmatch(r'(key_data|[A-F0-9]{16})[s01]',p.name) and not info.is_dir():
                    if info.file_size>65536:raise ImportError('Слишком большой файл авторизации tdata.')
                    files[name]=archive.read(info)
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError):
        raise ImportError('Нужен исправный ZIP с папкой tdata; RAR и 7z не поддерживаются.') from None
    roots={str(PurePosixPath(name).parent) for name in files if re.fullmatch(r'key_data[s01]',PurePosixPath(name).name)}
    if len(roots)!=1:raise ImportError('В ZIP должна быть одна папка tdata с key_datas (или key_data0/key_data1).')
    root=next(iter(roots)); prefix='' if root=='.' else root+'/'
    stream=desktop_file(files,prefix+'key_data');salt=stream.array();wrapped=stream.array();info=stream.array()
    if len(salt)!=32:raise ImportError('Неподдерживаемый формат соли tdata.')
    password=passcode.encode('utf-8')
    key=hashlib.pbkdf2_hmac('sha512',hashlib.sha512(salt+password+salt).digest(),salt,100000 if password else 1,256)
    local=decrypt_local(wrapped,key).take(256)
    account_info=decrypt_local(info,local);count=account_info.number()
    if not 1<=count<=10:raise ImportError('Неподдерживаемое число аккаунтов в tdata.')
    indices=[account_info.number() for _ in range(count)]
    if account_index not in indices:raise ImportError('Такого индекса аккаунта нет в tdata. Доступные индексы: '+', '.join(map(str,indices)))
    data_name='data' if account_index==0 else 'data#'+str(account_index+1)
    # Local-storage filename: 64 low MD5 bits, least-significant nibble first.
    filename=''.join(f'{b:02X}'[::-1] for b in hashlib.md5(data_name.encode()).digest()[:8])
    auth=decrypt_local(desktop_file(files,prefix+filename).array(),local)
    if auth.number()!=0x4b:raise ImportError('Этот вариант tdata не поддерживается. Используйте вход по номеру.')
    stream=Reader(auth.array());uid=stream.number();dc=stream.number()
    if uid==dc==0xffffffff:uid=stream.number(8);dc=stream.number()
    if not 0<uid<2**63 or dc not in DC_ADDRESS:raise ImportError('Неподдерживаемый аккаунт или дата-центр tdata.')
    n=stream.number();keys={}
    if not 1<=n<=20:raise ImportError('Повреждён список ключей tdata.')
    for _ in range(n):
        key_dc=stream.number();keys[key_dc]=stream.take(256)
    if dc not in keys:raise ImportError('В tdata нет ключа основного дата-центра.')
    return {'session_string':session_value(dc,DC_ADDRESS[dc],443,keys[dc]),'user_id':uid,
            'api_id':DESKTOP_API['id'],'api_hash':DESKTOP_API['hash']}

def decode_file(kind, blob, passcode='', account_index=0):
    if not blob or len(blob)>MAX_FILE:raise ImportError('Размер файла должен быть от 1 байта до 8 МБ.')
    if kind=='telethon':return read_sqlite(blob)
    if kind=='tdata':return read_tdata(blob,passcode,account_index)
    raise ImportError('Выберите Telethon .session или Telegram Desktop tdata ZIP.')
