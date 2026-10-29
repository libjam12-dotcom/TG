import asyncio,json,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
import httpx
from fastapi import HTTPException
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Store
from bot_notifications import BotNotifications,BotConfig,Mode,Test,Retry,APIError
TOKEN='50000:'+('a'*30)
class NotificationTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'db');self.store.put('running',True);self.store.put('destination',99)
  with self.store.db:
   self.store.db.execute("INSERT INTO sources(id,title,username) VALUES(-100001,'Внешняя группа','external')")
   self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Яхты',json.dumps(dict(sources=[-100001],keywords=['яхту'],required=['ищу'],excluded=[],substring=False))))
  self.calls=[];self.send_error=None;self.status='member';self.chat_type='supergroup';self.forum=False;self.chat_id=-100999;self.bot_name='TestLeadNotifyBot'
  async def handler(request):
   method=request.url.path.rsplit('/',1)[-1];payload=json.loads(request.content);self.calls.append((method,payload))
   if method=='getMe':result={'id':50000,'username':self.bot_name,'is_bot':True}
   elif method=='getChat':result={'id':self.chat_id,'title':'Группа лидов','type':self.chat_type,'is_forum':self.forum,'permissions':{'can_send_messages':True}}
   elif method=='getChatMember':result={'status':self.status,'is_member':self.status!='left','can_send_messages':self.status!='restricted'}
   elif method=='sendMessage':
    if isinstance(self.send_error,Exception):raise self.send_error
    if self.send_error:return httpx.Response(self.send_error.get('error_code',400),json=self.send_error)
    result={'message_id':321}
   else:raise AssertionError(method)
   return httpx.Response(200,json={'ok':True,'result':result})
  self.bot=BotNotifications(self.store,transport=httpx.MockTransport(handler));self.bot.initialize()
 async def asyncTearDown(self):await self.bot.shutdown();self.store.db.close();self.tmp.cleanup()
 async def configure(self,**kwargs):return await self.bot.configure(BotConfig(token=TOKEN,reference='-100999',**kwargs))
 async def activate(self,**kwargs):await self.configure(**kwargs);await self.bot.mode(Mode(mode='bot'))
 def lead(self,id=1,origin='live',sender=1,text=None):return self.store.ingest(-100001,id,text or 'Ищу яхту '+str(id),author='@buyer',sender_id=sender,link='https://t.me/external/'+str(id),origin=origin)
 async def test_validation_without_send_preserves_old_account_receiver(self):
  result=await self.configure();self.assertTrue(result['configured']);self.assertEqual(result['mode'],'account');self.assertEqual(self.store.get('destination'),99)
  self.assertEqual([x[0] for x in self.calls],['getMe','getChat','getChatMember']);self.assertNotIn(TOKEN,json.dumps(result))
 async def test_no_double_delivery_and_exact_receipt(self):
  self.lead();self.assertEqual(len(self.store.rows('SELECT * FROM outbox')),1)
  await self.activate();id=self.lead(2);self.assertEqual(self.store.rows('SELECT state FROM outbox')[0]['state'],'cancelled');self.assertEqual(len(self.store.rows('SELECT * FROM bot_deliveries')),1)
  await self.bot.tick();await self.bot.tick();row=self.store.rows('SELECT * FROM bot_deliveries')[0]
  self.assertEqual((row['state'],row['message_id']),('sent',321));self.assertEqual(sum(x[0]=='sendMessage' for x in self.calls),1)
  p=self.calls[-1][1];self.assertEqual(p['chat_id'],-100999);self.assertIn('Источник: Внешняя группа',p['text']);self.assertIn('@buyer',p['text']);self.assertTrue(p['reply_markup']['inline_keyboard'][0][1]['url'].endswith('?lead='+str(id)+'#scanner'))
 async def test_history_and_duplicate_never_enqueue(self):
  await self.activate();self.lead(origin='history');self.lead(2);self.assertIsNone(self.lead(2));self.assertEqual(len(self.store.rows('SELECT * FROM bot_deliveries')),1)
 async def test_filters_apply_only_to_new_cards(self):
  await self.activate(sources=[-100001],rules=[1]);self.lead();self.assertEqual(len(self.store.rows('SELECT * FROM bot_deliveries')),1)
  await self.configure();self.assertEqual(self.bot.delivery_mode(),'panel');self.assertEqual(self.store.rows('SELECT state FROM bot_deliveries')[0]['state'],'cancelled')
  self.assertEqual(len(self.store.rows('SELECT * FROM leads')),1)
 async def test_wrong_or_private_recipient_and_separate_bot(self):
  self.chat_type='private'
  with self.assertRaises(HTTPException):await self.configure()
  self.chat_type='supergroup';self.bot_name='DevelopmentDemo_bot'
  with self.assertRaises(HTTPException):await self.configure()
  self.assertFalse((await self.bot.state())['configured'])
 async def test_removed_or_restricted_member(self):
  for status in ('left','kicked','restricted'):
   self.status=status
   with self.assertRaises(HTTPException):await self.configure()
 async def test_active_source_cannot_receive(self):
  self.chat_id=-100001
  with self.assertRaises(HTTPException):await self.configure()
  self.assertEqual(len(self.calls),2)
 async def test_forum_explicit_test_once_even_with_duplicate_request(self):
  self.forum=True;await self.configure(thread_id=44)
  spec=Test(request_key='synthetic-test-key',confirmed=True)
  a=await self.bot.test(spec);b=await self.bot.test(spec)
  self.assertEqual(a['message_id'],321);self.assertEqual(a,b);self.assertEqual(self.calls[-1][1]['message_thread_id'],44);self.assertEqual(sum(x[0]=='sendMessage' for x in self.calls),1)
 async def test_thread_rejected_on_non_forum(self):
  with self.assertRaises(HTTPException):await self.configure(thread_id=44)
 async def test_429_honors_global_retry_after_and_does_not_block_forever(self):
  await self.activate();self.lead();self.lead(2,sender=2);self.send_error={'ok':False,'error_code':429,'parameters':{'retry_after':90}}
  before=time.time();await self.bot.tick();await self.bot.tick();self.assertEqual(sum(x[0]=='sendMessage' for x in self.calls),1)
  self.assertGreaterEqual(self.store.get('notification_bot_not_before'),before+90)
  self.send_error=None;self.store.put('notification_bot_not_before',0)
  with self.store.db:self.store.db.execute('UPDATE bot_deliveries SET next_at=0')
  await self.bot.tick();self.assertEqual(self.store.rows('SELECT state FROM bot_deliveries ORDER BY id')[0]['state'],'sent')
 async def test_read_timeout_uncertain_never_blind_retry(self):
  await self.activate();self.lead();self.send_error=httpx.ReadTimeout('private-url-must-not-leak')
  await self.bot.tick();await self.bot.tick();row=self.store.rows('SELECT * FROM bot_deliveries')[0];self.assertEqual(row['state'],'uncertain')
  with self.assertRaises(HTTPException):await self.bot.retry(row['id'],Retry(confirmed=True))
  self.assertNotIn('private-url',json.dumps(await self.bot.state()));self.assertEqual(sum(x[0]=='sendMessage' for x in self.calls),1)
 async def test_reject_retry_when_destination_revision_changed(self):
  await self.activate();self.lead();self.send_error={'ok':False,'error_code':403};await self.bot.tick();row=self.store.rows('SELECT * FROM bot_deliveries')[0]
  self.assertEqual(row['state'],'failed');await self.configure()
  with self.assertRaises(HTTPException):await self.bot.retry(row['id'],Retry(confirmed=True))
 async def test_confirmed_refusal_can_retry_same_recipient(self):
  await self.activate();self.lead();self.send_error={'ok':False,'error_code':403};await self.bot.tick();self.send_error=None
  await self.bot.retry('lead-1',Retry(confirmed=True));await self.bot.tick();self.assertEqual(self.store.rows('SELECT state FROM bot_deliveries')[0]['state'],'sent')
 async def test_restart_during_send_is_uncertain(self):
  await self.activate();self.lead()
  with self.store.db:self.store.db.execute("UPDATE bot_deliveries SET state='sending'")
  self.bot.initialize();await self.bot.tick();self.assertEqual(self.store.rows('SELECT state FROM bot_deliveries')[0]['state'],'uncertain');self.assertFalse(any(x[0]=='sendMessage' for x in self.calls))
 async def test_scanner_pause_does_not_starve_explicit_test_retry(self):
  await self.activate();self.lead();self.store.put('running',False)
  self.send_error=httpx.ConnectError('network');r=await self.bot.test(Test(request_key='test-key-long-enough',confirmed=True));self.assertEqual(r['state'],'pending')
  self.send_error=None
  with self.store.db:self.store.db.execute('UPDATE bot_deliveries SET next_at=0')
  await self.bot.tick();self.assertEqual(self.store.rows("SELECT state FROM bot_deliveries WHERE lead_id IS NULL")[0]['state'],'sent')
 async def test_card_handles_untrusted_text_and_astral_unicode(self):
  await self.activate();id=self.lead(text='Ищу яхту <b>подмена</b> '+('🚢'*4000));row=self.store.rows('SELECT * FROM bot_deliveries')[0];p=json.loads(row['payload'])
  self.assertNotIn('parse_mode',p);self.assertIn('<b>',p['text']);self.assertLessEqual(len(p['text'].encode('utf-16-le'))//2,3900)
 async def test_recipient_source_is_ignored_even_after_manual_toggle(self):
  await self.activate();self.store.put('notification_bot_config',{**self.bot.config(),'chat_id':-100001});self.assertIsNone(self.lead())
 async def test_mode_change_serializes_with_active_send(self):
  await self.activate();self.lead();started=asyncio.Event();finish=asyncio.Event();real=self.bot.call
  async def blocking(token,method,payload):
   if method=='sendMessage':started.set();await finish.wait()
   return await real(token,method,payload)
  self.bot.call=blocking;worker=asyncio.create_task(self.bot.tick());await started.wait();mode=asyncio.create_task(self.bot.mode(Mode(mode='account')));await asyncio.sleep(0)
  self.assertFalse(mode.done());finish.set();await worker;await mode;self.assertEqual(self.bot.delivery_mode(),'account');self.assertEqual(self.store.rows('SELECT state FROM bot_deliveries')[0]['state'],'sent')

 async def test_delivery_filters_skip_unselected_source_and_rule(self):
  with self.store.db:
   self.store.db.execute("INSERT INTO sources(id,title) VALUES(-100002,'Другая группа')")
   self.store.db.execute('INSERT INTO rules(name,data) VALUES(?,?)',('Сайты',json.dumps(dict(sources=[-100001],keywords=['сайт'],required=['ищу'],excluded=[]))))
  await self.activate(sources=[-100002]);self.lead();self.assertEqual(self.store.rows('SELECT * FROM bot_deliveries'),[])
  await self.configure(rules=[2]);await self.bot.mode(Mode(mode='bot'));self.lead(2,sender=2);self.assertEqual(self.store.rows('SELECT * FROM bot_deliveries'),[])
  self.lead(3,sender=3,text='Ищу сайт');self.assertEqual(len(self.store.rows('SELECT * FROM bot_deliveries')),1)

 async def test_stale_review_does_not_send_or_enable_a_changed_recipient(self):
  await self.configure();revision=self.bot.config()['revision'];await self.configure()
  with self.assertRaises(HTTPException):await self.bot.test(Test(request_key='stale-review-test',confirmed=True,chat_id=-100999,revision=revision))
  with self.assertRaises(HTTPException):await self.bot.mode(Mode(mode='bot',chat_id=-100999,revision=revision))
  self.assertEqual(self.store.rows('SELECT * FROM bot_deliveries'),[]);self.assertEqual(self.bot.delivery_mode(),'account');self.assertFalse(any(x[0]=='sendMessage' for x in self.calls))
