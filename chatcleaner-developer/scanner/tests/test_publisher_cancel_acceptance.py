"""Offline reproduction: cancellation while a legacy Content photo uploads."""
import asyncio
from datetime import datetime,timezone
from pathlib import Path
import os,sys,tempfile,unittest
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store
from publisher import Publisher,PostSpec
from telethon import types

class CancelRace(unittest.IsolatedAsyncioTestCase):
 async def test_cancel_before_sendmedia_rpc_prevents_outbound_and_preserves_cancelled(self):
  with tempfile.TemporaryDirectory() as data:
   store=Store(Path(data)/'isolated.sqlite3')
   entered,release=asyncio.Event(),asyncio.Event()
   calls=[]
   group=types.Channel(id=123,title='Synthetic',access_hash=1,photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,creator=True)
   class Client:
    def is_connected(self):return True
    async def get_entity(self,ref):return group
    async def get_input_entity(self,entity):return types.InputPeerChannel(group.id,group.access_hash)
    async def upload_file(self,file):
     entered.set();await release.wait();return types.InputFile(id=1,parts=1,name='synthetic.jpg',md5_checksum='0'*32)
    async def __call__(self,request):
     calls.append(type(request).__name__)
     return types.UpdateShortSentMessage(id=99,pts=1,pts_count=1,date=datetime.now(timezone.utc))
   runtime=SimpleNamespace(client=Client(),me=SimpleNamespace(id=5))
   pub=Publisher(lambda:store,lambda:runtime,data);pub.initialize()
   media='a'*32;physical=pub.data/'synthetic.jpg';physical.write_bytes(b'synthetic-not-uploaded')
   with store.db:store.db.execute('INSERT INTO content_media VALUES(?,?,?,?,?)',(media,'synthetic.jpg','image/jpeg',physical.stat().st_size,datetime.now(timezone.utc).isoformat()))
   post=await pub.create(PostSpec(title='Test cancel photo',text='Synthetic',media=[media],destinations=[-1000000000123]))
   await pub.publish(post['id'])
   task=asyncio.create_task(pub.tick())
   await asyncio.wait_for(entered.wait(),1)
   cancelled=await pub.cancel(post['id'])
   self.assertEqual(cancelled['status'],'cancelled')
   release.set();await task
   final=pub.post(post['id'])
   await pub.shutdown();store.db.close()
   self.assertEqual(calls,[],f"post state after cancellation={final['status']}; outbound RPCs={calls}")
   self.assertEqual(final['status'],'cancelled')

 async def test_cancel_after_rpc_started_keeps_actual_message_id_and_cancelled_post(self):
  with tempfile.TemporaryDirectory() as data:
   store=Store(Path(data)/'isolated.sqlite3')
   entered,release=asyncio.Event(),asyncio.Event()
   calls=[]
   group=types.Channel(id=123,title='Synthetic',access_hash=1,photo=types.ChatPhotoEmpty(),date=datetime.now(timezone.utc),megagroup=True,creator=True)
   class Client:
    def is_connected(self):return True
    async def get_entity(self,ref):return group
    async def get_input_entity(self,entity):return types.InputPeerChannel(group.id,group.access_hash)
    async def __call__(self,request):
     calls.append(type(request).__name__);entered.set();await release.wait()
     return types.UpdateShortSentMessage(id=99,pts=1,pts_count=1,date=datetime.now(timezone.utc))
   runtime=SimpleNamespace(client=Client(),me=SimpleNamespace(id=5))
   pub=Publisher(lambda:store,lambda:runtime,data);pub.initialize()
   post=await pub.create(PostSpec(title='Test in-flight cancel',text='Synthetic',destinations=[-1000000000123]))
   await pub.publish(post['id']);task=asyncio.create_task(pub.tick())
   await asyncio.wait_for(entered.wait(),1)
   await pub.cancel(post['id']);release.set();await task
   final=pub.post(post['id'])
   await pub.shutdown();store.db.close()
   self.assertEqual(calls,['SendMessageRequest'])
   self.assertEqual(final['status'],'cancelled')
   self.assertEqual(final['delivery'][0]['state'],'sent')
   self.assertEqual(final['delivery'][0]['message_ids'],[99])

if __name__=='__main__':unittest.main()
