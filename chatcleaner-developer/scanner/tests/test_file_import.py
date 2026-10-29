"""Offline synthetic sessions only. No calls to Telegram from this test module."""
import base64,io,json,sqlite3,stat,sys,tempfile,unittest,zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI
from httpx import AsyncClient,ASGITransport
from telethon.sessions import SQLiteSession,StringSession
from telethon.crypto import AuthKey
from account_import import decode_file,ImportError,MAX_FILE,MAX_REQUEST
from operations import Operations,parse_import

FIXTURES=Path(__file__).parent/'fixtures'
def session_blob(host='149.154.167.51'):
 with tempfile.TemporaryDirectory() as d:
  s=SQLiteSession(str(Path(d)/'synthetic'));s.set_dc(2,host,443);s.auth_key=AuthKey(bytes([42])*256);s.save();s.close()
  return (Path(d)/'synthetic.session').read_bytes()
def zip_bytes(entries):
 b=io.BytesIO()
 with zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED) as z:
  for name,value in entries:z.writestr(name,value)
 return b.getvalue()

class ReaderTests(unittest.TestCase):
 def test_telethon_created_file_matches_auth_key_and_dc(self):
  r=decode_file('telethon',session_blob());s=StringSession(r['session_string'])
  self.assertEqual(s.auth_key.key,bytes([42])*256);self.assertEqual(s.dc_id,2)
 def test_session_ssrf_is_rejected_by_shared_validation(self):
  from fastapi import HTTPException
  with self.assertRaises(HTTPException):parse_import(decode_file('telethon',session_blob('127.0.0.1')))
 def test_pyrogram_is_explicitly_rejected(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'p';db=sqlite3.connect(p);db.execute('CREATE TABLE sessions(dc_id INTEGER,api_id INTEGER,auth_key BLOB,is_bot INTEGER)');db.commit();db.close()
   with self.assertRaisesRegex(ImportError,'Pyrogram'):decode_file('telethon',p.read_bytes())
 def test_views_are_not_session_tables(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'v';db=sqlite3.connect(p);db.execute("CREATE VIEW sessions AS SELECT 2 AS dc_id");db.close()
   with self.assertRaises(ImportError):decode_file('telethon',p.read_bytes())
 def test_corrupt_session_and_oversized_file(self):
  for data in [b'bad',b'',b'0'*(MAX_FILE+1)]:
   with self.assertRaises(ImportError):decode_file('telethon',data)
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 def test_opentele_generated_open_and_passcoded_archives(self):
  for name,pw in [('tdesktop-open.zip',''),('tdesktop-locked.zip','local-test-only')]:
   for index in [0,1]:
    r=decode_file('tdata',(FIXTURES/name).read_bytes(),pw,index)
    self.assertEqual(r['user_id'],700000001+index);self.assertEqual(r['api_id'],2040)
    self.assertEqual(StringSession(r['session_string']).auth_key.key,bytes([42+index])*256)
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 def test_wrong_passcode_does_not_echo_it(self):
  with self.assertRaises(ImportError) as c:decode_file('tdata',(FIXTURES/'tdesktop-locked.zip').read_bytes(),'SECRET-test')
  self.assertNotIn('SECRET-test',str(c.exception));self.assertIn('локальный',str(c.exception))
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 def test_missing_account_index_explicit(self):
  with self.assertRaisesRegex(ImportError,'индекс'):decode_file('tdata',(FIXTURES/'tdesktop-open.zip').read_bytes(),account_index=5)
 def test_no_extraction_and_rejects_traversal_absolute_windows_symlink(self):
  cases=['../escape','/tmp/escape','tdata/../../escape','C:/evil','tdata\\evil']
  for name in cases:
   with self.assertRaises(ImportError):decode_file('tdata',zip_bytes([(name,b'data')]))
  symlink=zipfile.ZipInfo('link');symlink.create_system=3;symlink.external_attr=(stat.S_IFLNK|0o777)<<16
  with self.assertRaises(ImportError):decode_file('tdata',zip_bytes([(symlink,b'/etc/passwd')]))
 def test_zip_bomb_and_many_entries(self):
  for blob in [zip_bytes([('cache',b'0'*(MAX_FILE+1))]),zip_bytes([(str(i),b'x') for i in range(1025)])]:
   with self.assertRaises(ImportError):decode_file('tdata',blob)
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 def test_corrupt_tdata_integrity_rejected(self):
  with zipfile.ZipFile(FIXTURES/'tdesktop-open.zip') as z:entries=[(i.filename,z.read(i)) for i in z.infolist()]
  entries=[(n,b[:-1]+bytes([b[-1]^1]) if n.endswith('key_datas') else b) for n,b in entries]
  with self.assertRaises(ImportError):decode_file('tdata',zip_bytes(entries))
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 def test_multiple_tdata_roots_rejected(self):
  with zipfile.ZipFile(FIXTURES/'tdesktop-open.zip') as z:entries=[(i.filename,z.read(i)) for i in z.infolist()]
  with self.assertRaises(ImportError):decode_file('tdata',zip_bytes(entries+[(n.replace('tdata/','other/'),b) for n,b in entries]))

class FileAPITests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.ops=Operations(self.tmp.name,lambda:{'id':123,'hash':'a'*32});self.ops.initialize()
  self.app=FastAPI();self.app.include_router(self.ops.router)
  self.http=AsyncClient(transport=ASGITransport(app=self.app),base_url='http://test')
  self.fake=SimpleNamespace(connect=AsyncMock(),disconnect=AsyncMock(),is_user_authorized=AsyncMock(return_value=True),is_connected=lambda:True,get_me=AsyncMock(return_value=SimpleNamespace(id=700000001,username='synthetic',phone='70000000000',bot=False)),_init_request=SimpleNamespace())
 def payload(self,kind='telethon'):
  return {'kind':kind,'label':'Synthetic owned account','data':base64.b64encode(session_blob() if kind=='telethon' else (FIXTURES/'tdesktop-locked.zip').read_bytes()).decode(),'authorized':True,'passcode':'local-test-only'}
 async def asyncTearDown(self):
  await self.http.aclose();self.ops.store.db.close();self.tmp.cleanup()
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 async def test_success_file_checks_identity_no_jobs_secrets_or_password_retention(self):
  for kind in ['telethon','tdata']:
   self.fake.get_me.return_value.id+=int(kind=='tdata')
   payload=self.payload(kind)
   if kind=='tdata':payload['account_index']=1
   with patch('operations.ClientTelegramClient',return_value=self.fake):response=await self.http.post('/api/scanner/operations/accounts/import-file',json=payload)
   self.assertEqual(response.status_code,200,response.text);self.assertEqual(response.json()['connection'],'connected')
   state=await self.ops.state();self.assertEqual(state['jobs'],[])
   dump=json.dumps(state);self.assertNotIn('local-test-only',dump);self.assertNotIn(payload['data'],dump)
   settings=str(self.ops.store.rows('SELECT * FROM settings'));self.assertNotIn('local-test-only',settings)
 async def test_revoked_file_does_not_leave_session_or_claim_connected(self):
  self.fake.is_user_authorized.return_value=False
  with patch('operations.ClientTelegramClient',return_value=self.fake):r=await self.http.post('/api/scanner/operations/accounts/import-file',json=self.payload())
  self.assertEqual(r.json()['connection'],'not_connected');self.assertFalse(list(self.ops.data.glob('*.session')))
 async def test_duplicate_file_identity_rejected_and_original_preserved(self):
  with patch('operations.ClientTelegramClient',return_value=self.fake):
   first=await self.http.post('/api/scanner/operations/accounts/import-file',json=self.payload())
   second=await self.http.post('/api/scanner/operations/accounts/import-file',json=self.payload())
  self.assertEqual(first.json()['connection'],'connected');self.assertEqual(second.json()['connection'],'not_connected')
  self.assertEqual(len(list(self.ops.data.glob('*.session'))),1)
 async def test_disconnect_erases_panel_copy_without_revoking_original(self):
  self.fake.log_out=AsyncMock()
  with patch('operations.ClientTelegramClient',return_value=self.fake):r=await self.http.post('/api/scanner/operations/accounts/import-file',json=self.payload())
  aid=r.json()['account']['id'];await self.ops.disconnect(aid)
  self.fake.log_out.assert_not_awaited();self.assertFalse(list(self.ops.data.glob('*.session')))
  self.assertEqual((await self.ops.state())['accounts'][0]['connection'],'not_connected')
 @unittest.skip('Session/tdata archives intentionally not distributed; regenerate synthetic fixtures separately')
 async def test_bad_input_never_creates_account_or_echoes_secrets(self):
  for body in [{**self.payload(),'authorized':False},{**self.payload(),'data':'not-base64'},self.payload('tdata')|{'passcode':'SECRET-WRONG'}]:
   r=await self.http.post('/api/scanner/operations/accounts/import-file',json=body)
   self.assertIn(r.status_code,[400,422]);self.assertNotIn('SECRET-WRONG',r.text)
  self.assertEqual(self.ops.accounts,{})
 async def test_body_size_without_content_length_is_bounded(self):
  async def data():
   for _ in range(13):yield b'x'*(1024*1024)
  r=await self.http.post('/api/scanner/operations/accounts/import-file',content=data())
  self.assertEqual(r.status_code,413);self.assertEqual(self.ops.accounts,{})
 async def test_concurrent_import_rejected_without_creation(self):
  async with self.ops.import_lock:r=await self.http.post('/api/scanner/operations/accounts/import-file',json=self.payload())
  self.assertEqual(r.status_code,409);self.assertFalse(self.ops.accounts)
