const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
function fixture(identity){const scripts=[],events={};const window={panelIdentity:identity,addEventListener:(name,fn)=>events[name]=fn};const document={createElement:()=>({}),head:{append:s=>scripts.push(s)}};vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../community-link-loader.js'),'utf8'),{window,document});return {scripts,window,events};}
test('Client identity never loads the personal community script',()=>{const f=fixture({role:'admin'});f.events['panel-identity']();assert.equal(f.scripts.length,0);});
test('An unresolved identity waits for the server identity event',()=>{const f=fixture();assert.equal(f.scripts.length,0);f.window.panelIdentity={role:'admin'};f.events['panel-identity']();assert.equal(f.scripts.length,0);});
test('Owner identity retains the existing script once across repeated events',()=>{const f=fixture({role:'owner'});f.events['panel-identity']();f.events['panel-identity']();assert.equal(f.scripts.length,1);assert.equal(f.scripts[0].src,'/community-link.js');});
test('An asynchronously resolved owner gets the original personal link script',()=>{const f=fixture();f.window.panelIdentity={role:'owner'};f.events['panel-identity']();assert.equal(f.scripts.length,1);});
