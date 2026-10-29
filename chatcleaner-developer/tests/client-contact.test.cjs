const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
async function render(contact){
  const root={innerHTML:''},window={};
  const document={activeElement:null,querySelector:s=>s==='#lead-scanner'?root:null,addEventListener(){}};
  const state={sources:[],rules:[],total:1,pending:0,connection:'connected',running:true,
    leads:[{id:1,status:'new',text:'Ищу диджея',matches:[],client_contact:contact}]};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../scanner.js'),'utf8'),{
    window,document,location:{hash:''},sessionStorage:{getItem(){return null;}},URLSearchParams,AbortSignal,
    fetch:async()=>({ok:true,json:async()=>state}),setInterval(){}});
  await window.LeadScanner.mount();return root.innerHTML;
}
test('A channel candidate explains the missing contact and offers no conversation action',async()=>{
  const html=await render({status:'no_contact',label:'Нет подтверждённого контакта',reasons:['Автором указан канал.'],ready:false});
  assert.match(html,/Нет подтверждённого контакта/);assert.match(html,/Автором указан канал/);
  assert.doesNotMatch(html,/data-l-action="conversation"/);
});
test('An unverified personal author requires review and is not advertised as a ready customer',async()=>{
  const html=await render({status:'needs_review',label:'Нужна проверка контакта заказчика',reasons:['Автор может быть посредником.'],author_verified:false,ready:false});
  assert.match(html,/Проверить автора/);assert.match(html,/Автор может быть посредником/);
  assert.doesNotMatch(html,/>Написать</);
});
