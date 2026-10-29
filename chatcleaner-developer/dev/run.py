"""Local-only UI+API development adapter. No production credentials or auth server."""
from pathlib import Path
import os,sys,subprocess,threading,http.client,json,time
from http.server import ThreadingHTTPServer,SimpleHTTPRequestHandler
ROOT=Path(__file__).resolve().parents[1]
ORIGIN='http://127.0.0.1:8080'
class Handler(SimpleHTTPRequestHandler):
 def __init__(self,*args,**kw):super().__init__(*args,directory=str(ROOT/'dist'),**kw)
 def log_message(self,*args):pass
 def run_request(self):
  if self.headers.get('Host') not in ('127.0.0.1:8080','localhost:8080'):self.send_error(403);return
  if self.command not in ('GET','HEAD') and self.headers.get('Origin')!=ORIGIN:self.send_error(403);return
  if self.path in ('/auth/me','/auth/logout'):
   body=json.dumps({'username':'local-developer','role':'owner'}).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
  if self.path.startswith('/api/'):
   try:
    size=int(self.headers.get('Content-Length','0'))
    if not 0<=size<=25*1024*1024:self.send_error(413);return
    headers={k:v for k,v in self.headers.items() if k.lower() in ('content-type','x-scanner-request','x-panel-request')}
    headers.update({'X-Panel-User':'local-developer','X-Panel-Role':'owner','Origin':ORIGIN})
    c=http.client.HTTPConnection('127.0.0.1',8787,timeout=60);c.request(self.command,self.path,body=self.rfile.read(size) if size else None,headers=headers)
    r=c.getresponse();body=r.read();self.send_response(r.status)
    self.send_header('Content-Type',r.getheader('Content-Type','application/json'));self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);c.close()
   except Exception:self.send_error(502,'Local backend unavailable')
   return
  if self.command not in ('GET','HEAD'):self.send_error(405);return
  return super().do_GET() if self.command=='GET' else super().do_HEAD()
 do_GET=run_request;do_POST=run_request;do_PUT=run_request;do_DELETE=run_request;do_PATCH=run_request;do_HEAD=run_request
if __name__=='__main__':
 os.umask(0o077)
 data=ROOT/'.local-data';data.mkdir(mode=0o700,exist_ok=True)
 env=dict(os.environ,SCANNER_DATA=str(data),PANEL_ORIGIN=ORIGIN,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(ROOT/'dev')+os.pathsep+str(ROOT/'scanner'))
 backend=subprocess.Popen([sys.executable,'-m','uvicorn','app:app','--host','127.0.0.1','--port','8787','--no-access-log'],cwd=ROOT/'scanner',env=env)
 server=None
 try:
  server=ThreadingHTTPServer(('127.0.0.1',8080),Handler)
  print('Local development only: '+ORIGIN+'/#invite-settings ; Telegram disconnected; external network blocked by default',flush=True)
  server.serve_forever()
 except KeyboardInterrupt:pass
 finally:
  if server:server.server_close()
  backend.terminate()
  try:backend.wait(timeout=8)
  except subprocess.TimeoutExpired:backend.kill();backend.wait()
