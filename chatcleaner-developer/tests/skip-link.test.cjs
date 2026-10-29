const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const root=path.resolve(__dirname,'..');
const candidate=fs.readFileSync(path.join(root,'app.js'),'utf8');
function exercise(source,skip=true){
 const events={},clicks=[];let prevented=false,focused=0,scrolled=0,renders=0;const location={hash:'#scanner'};
 const main={focus(){focused++;},scrollIntoView(){scrolled++;}},side={classList:{remove(){}}},menu={setAttribute(){}};
 const context={location,window:{addEventListener:(name,handler)=>events[name]=handler,scrollTo(){}},document:{addEventListener:(name,handler)=>{if(name==='click')clicks.push(handler);}},$:selector=>selector==='#main'?main:selector==='#sidebar'?side:menu,render(){renders++;}};
 vm.createContext(context);vm.runInContext("let route='scanner',search='unsaved form';const defaultRoute='home';const sections=[['home'],['scanner']];const can=()=>true;",context);
 const router=source.split('\n').find(line=>line.startsWith("window.addEventListener('hashchange'"));assert.ok(router);vm.runInContext(router,context);
 const handler=source.split('\n').find(line=>line.startsWith('function skipToContent('));if(handler)vm.runInContext(handler+"\ndocument.addEventListener('click',skipToContent);",context);
 const event={target:{closest:selector=>skip&&selector==='a.skip-link[href="#main"]'?{}:null},preventDefault(){prevented=true;}};
 for(const handler of clicks)handler(event);
 if(!prevented){location.hash=skip?'#main':'#home';events.hashchange();}
 return {prevented,focused,scrolled,renders,hash:location.hash,route:vm.runInContext('route',context),draft:vm.runInContext('search',context)};
}
test('candidate skip-to-content keeps route/form and focuses main without changing hash or remounting',()=>{const value=exercise(candidate);assert.equal(value.prevented,true);assert.equal(value.focused,1);assert.equal(value.scrolled,1);assert.equal(value.route,'scanner');assert.equal(value.hash,'#scanner');assert.equal(value.renders,0);assert.equal(value.draft,'unsaved form');});
test('candidate leaves ordinary route links to existing router',()=>{const value=exercise(candidate,false);assert.equal(value.prevented,false);assert.equal(value.focused,0);assert.equal(value.route,'home');assert.equal(value.renders,1);});
