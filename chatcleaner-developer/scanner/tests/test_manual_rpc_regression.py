import sys,importlib.util,unittest
from pathlib import Path
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import operations,manual_invite_campaign
from telethon import errors,functions
sys.path.insert(0,str(Path(__file__).resolve().parent))
import fixture_manual as legacy
class Diagnosis(legacy.ManualFixture):
 async def test_badrequest_keeps_rpc_enum_stops_no_retry(self):
  self.ops.store.db.execute("UPDATE op_accounts SET error=NULL WHERE id='primary'");self.ops.store.db.commit()
  user=self.client.users[0]
  self.client.failures[user.id]=errors.BadRequestError(functions.channels.InviteToChannelRequest(self.client.group,[user]),'CHAT_MEMBER_ADD_FAILED',400)
  saved=await self.manual.quick_draft(manual_invite_campaign.QuickDraftRequest(config=self.config(2),request_key='diagnosis-draft-001'))
  await self.manual.quick_start(manual_invite_campaign.QuickStartRequest(confirmed=True,job_id=saved['job_id'],config_digest=saved['config_digest'],request_key='diagnosis-start-001'))
  await self.run_worker();out=await self.manual.status(saved['job_id'])
  self.assertEqual(out['counts']['attempts'],1);self.assertEqual(out['counts']['added'],0);self.assertEqual(out['state'],'stopped')
  rpc=out['items'][0]['diagnosis']['verification_error']['rpc']
  self.assertEqual(rpc['message'],'CHAT_MEMBER_ADD_FAILED');self.assertEqual(rpc['request_constructor'],'InviteToChannelRequest')
  self.assertEqual(out['items'][1]['state'],'pending')
if __name__=='__main__':unittest.main(defaultTest='Diagnosis.test_badrequest_keeps_rpc_enum_stops_no_retry')
