'use strict';
// One fixed contact batch. Mount/poll only read status; writes need their own click.
window.IsolatedContactBatch=(()=>{
 const base='/api/scanner/operations/isolated-invite-batch',scope='client-russkie-contact-batch-v1';
 const sender=710000015,target=-1000720000000;
 const h=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const labels={unused:'Пакет ещё не запускался',running:'Выполняется',waiting:'Пауза между приглашениями',paused:'Остановлен вручную',stopped:'Пакет остановлен',completed:'Завершён',interrupted:'Выполнение прервано'};
 const itemLabels={pending:'Ожидает',sending:'Выполняется',invited:'Добавлен',already_member:'Уже в группе',restricted:'Ограничение Telegram',failed:'Не добавлен',blocked:'Действие ограничено',uncertain:'Результат не подтверждён'};
 const countKeys=['total','added','already_member','restricted','failed','blocked','uncertain','pending','sending'];
 let mounted=null,current=null,preview=null,confirmed=false,busy='',error='',info='',requestKey='',unknownWrite=false;
 let poll=null,expiry=null,readSequence=0,revision=0;
 const root=()=>document.querySelector('#isolated-contact-batch');
 const validId=n=>Number.isSafeInteger(n)&&n>0;
 const validUser=s=>typeof s==='string'&&/^[A-Za-z][A-Za-z0-9_]{3,31}$/.test(s);
 const iso=s=>typeof s==='string'&&Number.isFinite(Date.parse(s));
 const stamp=s=>iso(s)?new Date(s).toLocaleString('ru-RU'):'—';
 const people=(n,object=false)=>n===1?(object?'1 человека':'1 человек'):n>=2&&n<=4?n+' человека':n+' человек';
 function require(value,message){if(!value)throw Error(message);}
 function binding(r){require(r&&r.scope===scope&&r.account==='primary'&&r.sender_id===sender&&r.target_id===target&&r.outbound_paused===true,'Сервер не подтвердил нужный аккаунт и группу. Обновите состояние.');}
 function checkStatus(r){
  binding(r);
  require(Object.hasOwn(labels,r.state)&&r.can_resume===false&&r.max_attempts===10&&typeof r.can_start==='boolean','Состояние пакета не подтверждено. Обновите состояние.');
  require(Array.isArray(r.items)&&r.items.length<=10&&r.counts&&typeof r.counts==='object','Список результатов не подтверждён.');
  for(const k of countKeys)require(Number.isInteger(r.counts[k])&&r.counts[k]>=0&&r.counts[k]<=10,'Счётчики пакета не подтверждены.');
  require(r.counts.total===r.items.length,'Счётчики и список получателей не совпали.');
  const seen=new Set(),derived=Object.fromEntries(countKeys.map(k=>[k,0]));derived.total=r.items.length;
  for(const item of r.items){
   require(validId(item.item_id)&&validId(item.user_id)&&validUser(item.username)&&!seen.has(item.user_id)&&Object.hasOwn(itemLabels,item.state),'Сервер вернул неподтверждённого получателя.');
   seen.add(item.user_id);derived[item.state==='invited'?'added':item.state]++;
  }
  for(const k of countKeys)require(derived[k]===r.counts[k],'Счётчик добавлений не совпал с результатами получателей.');
  require(Number.isInteger(r.attempts_claimed)&&r.attempts_claimed>=0&&r.attempts_claimed<=10,'Число попыток не подтверждено.');
  require((r.next_attempt_not_before==null||iso(r.next_attempt_not_before))&&(r.expires_at==null||iso(r.expires_at)),'Время следующего действия не подтверждено.');
  if(r.state==='unused')require(r.job_id===null&&r.items.length===0,'Пустой пакет содержит задание.');
  else require(validId(r.job_id)&&typeof r.request_key==='string'&&r.request_key.length<=100,'Запись пакета в журнале не подтверждена.');
  return r;
 }
 function checkPreview(r){
  binding(r);
  require(r.invitation_requested===false&&r.history_changed===false&&Array.isArray(r.selected)&&r.selected.length<=10,'Проверка списка не подтверждена. Приглашения не запускались.');
  require(Number.isInteger(r.membership_checks)&&r.membership_checks>=0&&r.membership_checks<=30&&iso(r.expires_at),'Срок проверки списка не подтверждён.');
  const seen=new Set();
  for(const user of r.selected){
   require(validId(user.user_id)&&validUser(user.username)&&!seen.has(user.user_id)&&user.member===false&&user.sender_has_contact===true&&user.mutual_contact===true&&iso(user.checked_at),'В списке есть неподтверждённый контакт. Проверьте список заново.');
   seen.add(user.user_id);
  }
  if(r.selected.length)require(typeof r.candidate_token==='string'&&/^[A-Za-z0-9_-]{43}$/.test(r.candidate_token),'Список не подтверждён сервером.');
  return r;
 }
 async function api(path,body){
  const options={method:body===undefined?'GET':'POST',credentials:'same-origin',headers:{'X-Scanner-Request':'1'},signal:AbortSignal.timeout(path.startsWith('/status?')?20000:90000)};
  if(body!==undefined){options.headers['Content-Type']='application/json';options.body=JSON.stringify(body);}
  const response=await fetch(base+path,options);
  let value;try{value=await response.json();}catch{throw Error('Сервер не подтвердил результат. Обновите состояние; повторный запуск не выполняется.');}
  if(!response.ok){const err=Error(typeof value?.detail==='string'?value.detail.slice(0,400):'Действие не подтверждено сервером. Обновите состояние.');err.status=response.status;throw err;}
  return value;
 }
 function clearPreview(){preview=null;confirmed=false;if(expiry){clearTimeout(expiry);expiry=null;}}
 function canPreview(){return !busy&&!unknownWrite&&current?.can_start===true;}
 function canStart(){return canPreview()&&confirmed&&preview?.selected.length>0&&Date.parse(preview.expires_at)>Date.now();}
 function button(text,action,disabled=false,secondary=true){return `<button type="button" class="button${secondary?' secondary':''}" data-icb-action="${action}"${disabled?' disabled':''}>${h(text)}</button>`;}
 function counters(r){const c=r.counts,limited=c.restricted+c.failed+c.blocked;return `<div class="icb-counts" role="status" aria-live="polite"><strong>Добавлено: ${c.added} из ${c.total}</strong><span>Не добавлены: ${limited}</span><span>Не подтверждено: ${c.uncertain}</span><span>Ожидают: ${c.pending}</span>${c.sending?`<span>Выполняется: ${c.sending}</span>`:''}</div>${c.already_member?`<p class="small">Уже в группе: ${c.already_member}. Это не новые добавления.</p>`:''}`;}
 function resultView(){
  if(!current?.job_id)return '';
  const active=['running','waiting'].includes(current.state);
  return `<section class="icb-results" aria-labelledby="icb-result-heading"><h3 id="icb-result-heading">${h(labels[current.state])} · задание №${current.job_id}</h3>${counters(current)}
   ${current.next_attempt_not_before&&active?`<p class="small">Следующая попытка не ранее ${h(stamp(current.next_attempt_not_before))}.</p>`:''}
   ${current.stop_reason?`<p class="notice">${h(current.stop_reason)}</p>`:''}
   <ul class="icb-people">${current.items.map(i=>`<li><strong>@${h(i.username)}</strong><span class="badge">${h(itemLabels[i.state])}</span>${i.state==='uncertain'?'<small>Повторного запроса нет. Проверьте членство в журнале.</small>':''}</li>`).join('')}</ul>
   <div class="ls-actions">${active?button('Остановить этот пакет','pause',!!busy):''}${button('Обновить состояние','status',!!busy)}${button('Открыть журнал панели','history')}</div>
   ${active?'<p class="small muted">Остановка действует только на этот пакет. Уже отправленный запрос Telegram может завершиться.</p>':'<p class="small muted">История сохранена в обычном журнале. Автоматического возобновления нет.</p>'}
  </section>`;
 }
 function previewView(){
  if(!preview)return '';
  if(!preview.selected.length)return '<p class="notice" role="status">В этой проверке подходящих взаимных контактов, которых ещё нет в группе, не найдено. Приглашения не запускались.</p>';
  return `<section class="icb-preview" aria-labelledby="icb-list-heading"><h3 id="icb-list-heading">Проверьте список: ${people(preview.selected.length)}</h3>
   <p class="small">Это взаимные контакты клиентки; при проверке они не состояли в группе. Список действует до ${h(stamp(preview.expires_at))}.</p>
   <ol class="icb-people">${preview.selected.map(u=>`<li><strong>@${h(u.username)}</strong><span class="small">Взаимный контакт · ещё не в группе</span></li>`).join('')}</ol>
   <label class="ls-check"><input type="checkbox" data-icb-confirm${confirmed?' checked':''}${busy?' disabled':''}><span>Проверил этот список и группу «ТЕСТОВОЕ СООБЩЕСТВО». Подтверждаю запуск одного пакета приглашений.</span></label>
   <div class="ls-actions">${button(busy==='start'?'Запускаем…':`Пригласить ${people(preview.selected.length,true)}`,'start',!canStart(),false)}${button('Проверить список заново','preview',!canPreview())}</div>
  </section>`;
 }
 function draw(){
  const el=root();if(!el)return;
  el.innerHTML=`<style>#isolated-contact-batch{margin-bottom:22px;min-width:0}#isolated-contact-batch p{max-width:78ch;overflow-wrap:anywhere}#isolated-contact-batch .icb-people{padding-left:0;list-style:none;margin:14px 0;border-top:1px solid var(--line,#e5ebef)}#isolated-contact-batch .icb-people li{display:flex;align-items:center;justify-content:space-between;gap:9px 16px;flex-wrap:wrap;padding:12px 0;border-bottom:1px solid var(--line,#e5ebef);overflow-wrap:anywhere;min-width:0}#isolated-contact-batch .icb-people small{flex-basis:100%}#isolated-contact-batch .icb-counts{display:flex;gap:12px 22px;flex-wrap:wrap;margin:14px 0}#isolated-contact-batch .button{min-height:44px;white-space:normal;max-width:100%;overflow-wrap:anywhere}#isolated-contact-batch h3{margin-top:20px}#isolated-contact-batch .icb-preview,#isolated-contact-batch .icb-results{padding-top:4px}#isolated-contact-batch input[type=checkbox]{width:18px;height:18px;flex-shrink:0}@media(max-width:600px){#isolated-contact-batch .ls-actions{align-items:stretch}#isolated-contact-batch .ls-actions>.button{flex:1 1 210px}#isolated-contact-batch .icb-people li{align-items:flex-start}}</style>
   <h2 id="icb-heading">Пригласить из контактов клиентки</h2><p>Аккаунт: <strong>@demo_identity_1</strong>. Группа: <strong>ТЕСТОВОЕ СООБЩЕСТВО</strong>.</p>
   <p class="small">Один пакет до 10 взаимных контактов, пауза между попытками не меньше 60 секунд. Личные сообщения и ссылки не отправляются.</p>
   <p class="small muted">Основные исходящие аккаунта остаются на паузе. Работает только отдельно подтверждённый пакет; прежние задания не запускаются.</p>
   ${error?`<p class="ls-error" role="alert">${h(error)}</p>`:''}${info?`<p class="notice" role="status">${h(info)}</p>`:''}
   ${!current?'<p class="small" role="status">Проверяем состояние пакета…</p>':''}
   ${unknownWrite?'<p class="notice" role="status">Ответ на запуск не получен. Проверяем сохранённое состояние без повторной отправки.</p>':''}
   ${current?.can_start&&!preview?`<div class="ls-actions">${button(busy==='preview'?'Проверяем контакты…':'Проверить до 10 контактов','preview',!canPreview())}</div><p class="small muted">Проверка только подготавливает список. Приглашения начнутся после отдельного подтверждения.</p>`:''}
   ${!current?`<div class="ls-actions">${button('Обновить состояние','status',!!busy)}</div>`:''}
   ${previewView()}${resultView()}`;
 }
 async function readStatus(){
  const sequence=++readSequence,seenRevision=revision;
  try{
   const value=checkStatus(await api('/status?scope='+encodeURIComponent(scope)));
   if(sequence!==readSequence||seenRevision!==revision)return;
   current=value;
   if(value.job_id){if(value.request_key===requestKey||value.can_start===false)unknownWrite=false;clearPreview();}
   if(!busy)error='';
  }catch(e){if(sequence===readSequence&&seenRevision===revision)error=e.message||'Состояние пакета не загружено.';}
  draw();
 }
 async function readPreview(){
  require(canPreview(),'Сначала обновите состояние пакета. Нового запуска не было.');
  busy='preview';error='';info='';clearPreview();draw();
  try{
   const value=checkPreview(await api('/contacts-preview?scope='+encodeURIComponent(scope)));
   preview=value;confirmed=false;
   const remaining=Date.parse(value.expires_at)-Date.now();
   if(remaining<=0){clearPreview();throw Error('Срок проверки списка истёк. Проверьте контакты заново.');}
   expiry=setTimeout(()=>{clearPreview();info='Срок проверки списка истёк. Проверьте контакты заново.';draw();},Math.min(remaining,2147483647));
  }catch(e){error=e.message||'Проверка списка не завершена. Приглашения не запускались.';}
  finally{busy='';draw();}
 }
 async function start(){
  require(canStart(),'Подтвердите действующий список получателей. Приглашения не запускались.');
  require(typeof crypto?.randomUUID==='function','Обновите браузер, чтобы подтвердить запуск.');
  const token=preview.candidate_token,key=crypto.randomUUID();requestKey=key;busy='start';revision++;error='';info='';draw();
  // Once a POST is attempted the preview is discarded, even if its response is lost.
  clearPreview();unknownWrite=true;
  try{
   const value=checkStatus(await api('/start',{scope,confirmed:true,reason:'Ручное подтверждение проверенного списка контактов для группы «ТЕСТОВОЕ СООБЩЕСТВО» в панели.',request_key:key,candidate_token:token}));
   require(value.request_key===key&&validId(value.job_id),'Сервер не подтвердил задание этого запуска. Обновите состояние.');
   current=value;unknownWrite=false;info='Пакет запущен. Показаны фактические результаты каждого получателя.';
  }catch(e){error=(e.message||'Ответ на запуск не получен.')+' Повторная отправка не выполняется.';}
  finally{busy='';draw();if(unknownWrite)await readStatus();}
 }
 async function pause(){
  require(!busy&&current&&['running','waiting'].includes(current.state)&&validId(current.job_id),'Этот пакет сейчас не выполняется.');
  const key=current.request_key,job=current.job_id;busy='pause';revision++;error='';draw();
  try{
   const value=checkStatus(await api('/pause',{scope,request_key:key,job_id:job}));
   require(value.job_id===job&&value.request_key===key,'Ответ относится к другому пакету. Обновите состояние.');
   current=value;info='Остановка запрошена только для этого пакета. Завершающийся запрос Telegram может изменить результат.';
  }catch(e){error=(e.message||'Остановка не подтверждена.')+' Проверьте состояние; другие задания не менялись.';}
  finally{busy='';draw();await readStatus();}
 }
 function view(){return '<section id="isolated-contact-batch" class="panel ls-section" aria-labelledby="icb-heading"></section>';}
 function unmount(){mounted=null;readSequence++;clearPreview();if(poll){clearInterval(poll);poll=null;}}
 function mount(){
  const el=root();if(!el){unmount();return;}
  if(mounted===el)return;
  mounted=el;draw();readStatus();
  if(!poll)poll=setInterval(()=>{if(!root()){unmount();return;}if(document.visibilityState==='hidden'||busy)return;if(unknownWrite||current&&['running','waiting'].includes(current.state))readStatus();},5000);
 }
 document.addEventListener('change',event=>{const t=event.target;if(!t?.hasAttribute?.('data-icb-confirm')||!t.closest('#isolated-contact-batch')||busy)return;confirmed=t.checked;const b=root()?.querySelector?.('[data-icb-action="start"]');if(b)b.disabled=!canStart();});
 document.addEventListener('click',event=>{
  const b=event.target.closest?.('[data-icb-action]');if(!b||!b.closest('#isolated-contact-batch')||b.disabled)return;
  event.preventDefault();event.stopPropagation();
  const action=b.dataset.icbAction;
  if(action==='history'){window.TelegramOperations?.openTab?.('jobs');location.hash='#operations';return;}
  if(busy)return;
  const work=action==='status'?readStatus:action==='preview'?readPreview:action==='start'?start:action==='pause'?pause:null;
  if(work)Promise.resolve().then(work).catch(e=>{error=e.message||'Действие не выполнено.';draw();});
 });
 return {view,mount,unmount};
})();
