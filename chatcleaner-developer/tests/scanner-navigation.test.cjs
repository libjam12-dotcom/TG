const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');

for(const [file,name,selector] of [['scanner.js','LeadScanner','#lead-scanner']]){
  function fixture(){
    let root=null,poll;const pending=[];
    const document={activeElement:null,querySelector:s=>s===selector?root:null,addEventListener(){}};
    const window={};
    vm.runInNewContext(fs.readFileSync(path.join(__dirname,'..',file),'utf8'),{
      window,document,location:{hash:''},sessionStorage:{getItem(){return null;}},URLSearchParams,
      AbortSignal:{timeout:ms=>({timeout:ms})},
      fetch:(url,options)=>new Promise((resolve,reject)=>pending.push({url,options,resolve,reject})),
      setInterval:fn=>{poll=fn;}
    });
    const ui=window[name];const state={sources:[],rules:[],leads:[],total:0,pending:0,connection:'connected',running:true};
    return {ui,pending,poll:()=>poll(),root:()=>root,document,state,
      enter(){root={innerHTML:ui.view()};return ui.mount();},leave(){root=null;},
      async respond(status=200,data=state){pending.shift().resolve({ok:status===200,status,json:async()=>data});await new Promise(setImmediate);}
    };
  }
  test(file+': returning during unchanged background refresh never leaves a spinner',async()=>{
    const f=fixture();const first=f.enter();await f.respond();await first;
    f.poll();assert.equal(f.pending.length,1);f.leave();await f.enter();
    assert.match(f.root().innerHTML,/Найденные запросы/);
    assert.equal(f.pending.length,1,'navigation reuses the in-flight request');
    await f.respond();assert.doesNotMatch(f.root().innerHTML,/Загружаем лидсканер/);
  });
  test(file+': revisiting shows last data before network; an unchanged response preserves the form',async()=>{
    const f=fixture();const first=f.enter();await f.respond();await first;
    f.leave();const remount=f.enter();assert.match(f.root().innerHTML,/Найденные запросы/);
    f.root().innerHTML+='unsaved form content';await f.respond();await remount;
    assert.match(f.root().innerHTML,/unsaved form content/);
  });
  test(file+': failed refresh recovers, then revoked access clears cached leads',async()=>{
    const f=fixture();const first=f.enter();await f.respond();await first;
    f.poll();await f.respond(503,{detail:'Temporarily unavailable'});
    assert.match(f.root().innerHTML,/Temporarily unavailable/);
    f.poll();await f.respond();assert.doesNotMatch(f.root().innerHTML,/Temporarily unavailable/);
    f.poll();await f.respond(403,{detail:'Access denied'});
    assert.match(f.root().innerHTML,/Access denied/);assert.doesNotMatch(f.root().innerHTML,/Найденные запросы/);
  });
}
