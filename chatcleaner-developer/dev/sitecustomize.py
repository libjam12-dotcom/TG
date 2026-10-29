"""Development-only guard: external socket connections denied by default.
Not a security sandbox: developers must also isolate their VM/container.
"""
import os,socket,ipaddress
if os.environ.get('HANDOVER_ALLOW_EXTERNAL')!='1':
 _connect=socket.socket.connect;_connect_ex=socket.socket.connect_ex;_resolve=socket.getaddrinfo
 def allowed(address):
  if not isinstance(address,tuple):return True # local Unix sockets
  host=str(address[0])
  if host=='localhost':return True
  try:return ipaddress.ip_address(host).is_loopback
  except ValueError:return False
 def connect(self,address):
  if not allowed(address):raise PermissionError('Handover offline mode: external network disabled')
  return _connect(self,address)
 def connect_ex(self,address):
  if not allowed(address):raise PermissionError('Handover offline mode: external network disabled')
  return _connect_ex(self,address)
 def resolve(host,*args,**kwargs):
  if host is not None and not allowed((host,0)):raise PermissionError('Handover offline mode: external DNS disabled')
  return _resolve(host,*args,**kwargs)
 socket.socket.connect=connect;socket.socket.connect_ex=connect_ex;socket.getaddrinfo=resolve
