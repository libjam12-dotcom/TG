'use strict';
// One finite campaign. Mount/poll read status only; every Telegram preview/write needs its own click.
window.IsolatedInviteCampaign=(()=>{
 const base='/api/scanner/operations/isolated-invite-campaign',scope='client-russkie-contact-campaign-v2';
 const sender=710000015,target=-1000720000000;
 const modes={mutual:{title:'Взаимные контакты',limit:100},saved_contacts:{title:'Существующие контакты: проверка до 10',limit:10}};
 const funnelLabels={missing_entity:'Нет данных профиля в ответе',protected_identity:'Пропущены служебные профили',invalid_profile:'Не подходит профиль',no_username:'Без username: фильтр панели',not_saved_contact:'Статус контакта не подтверждён',non_mutual:'Пропущены невзаимные контакты',prior_target_history:'Уже в истории или других заданиях',explicit_exclusion:'В списке исключений',already_member:'Уже в группе',unknown_membership:'Членство не подтверждено',selected:'Выбраны для приглашения',unread_due_limit:'Ещё не проверены из-за предела выборки'};
 let mode='mutual';
 const modeLimit=()=>modes[mode].limit;
 const h=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const labels={unused:'Кампания ещё не запускалась',running:'Выполняется',waiting:'Пауза между приглашениями',paused:'Остановлена вручную',stopped:'Кампания остановлена',completed:'Кампания завершена',interrupted:'Выполнение прервано'};
 const itemLabels={pending:'Ожидает',sending:'Выполняется',invited:'Добавлен',already_member:'Уже в группе',restricted:'Отказ Telegram',failed:'Не добавлен',blocked:'Действие ограничено',uncertain:'Результат не подтверждён',duplicate_skipped:'Повтор пропущен',cancelled:'Отменён',excluded:'Исключён'};
 const countKeys=['total','added','already_member','restricted','failed','blocked','uncertain','pending','sending'];
 let mounted=null,current=null,preview=null,confirmed=false,busy='',error='',info='',requestKey='',unknownWrite=false;
 let poll=null,expiry=null,readSequence=0,revision=0,lifecycle=0,statusReady=false;
 const root=()=>document.querySelector('#isolated-invite-campaign');
 const validId=n=>Number.isSafeInteger(n)&&n>0;
 const validUser=s=>typeof s==='string'&&/^[A-Za-z][A-Za-z0-9_]{3,31}$/.test(s);
 const iso=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}T/.test(s)&&Number.isFinite(Date.parse(s));
 const validKey=s=>typeof s==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s);
 const alive=v=>v===lifecycle&&mounted&&mounted===root();
 function rememberPending(key){requestKey=key;unknownWrite=!!key;try{if(key)sessionStorage.setItem(scope+':pending-start',JSON.stringify({mode,key}));else sessionStorage.removeItem(scope+':pending-start');}catch{}}
 function restorePending(){try{const stored=sessionStorage.getItem(scope+':pending-start');if(validKey(stored)){mode='mutual';requestKey=stored;unknownWrite=true;return;}const pending=JSON.parse(stored);if(pending&&Object.hasOwn(modes,pending.mode)&&validKey(pending.key)){mode=pending.mode;requestKey=pending.key;unknownWrite=true;}}catch{}}
 const stamp=s=>iso(s)?new Date(s).toLocaleString('ru-RU'):'—';
 const people=(n,object=false)=>{const k=n%100,u=n%10;return n+' '+(object?(u===1&&k!==11?'человека':'человек'):u===1&&k!==11?'человек':u>=2&&u<=4&&(k<12||k>14)?'человека':'человек');};
 function require(value,message){if(!value)throw Error(message);}
 function binding(r){require(r&&r.scope===scope&&r.account==='primary'&&r.sender_id===sender&&r.target_id===target&&r.outbound_paused===true&&(r.mode===mode||r.mode===undefined&&mode==='mutual'),'Сервер не подтвердил нужный режим, аккаунт и группу. Обновите состояние.');}
 function capacity(r){
  require(r.goal_added===100&&r.account_day_attempt_ceiling===100&&r.normal_daily_attempt_limit===20,'Бюджет кампании не подтверждён.');
  require(typeof r.utc_day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(r.utc_day),'День бюджета не подтверждён.');
  require(iso(r.budget_reset_at)&&Date.parse(r.budget_reset_at)===Date.parse(r.utc_day+'T00:00:00Z')+86400000,'Время сброса бюджета не подтверждено.');
  for(const k of ['account_attempts_today','verified_added_today','remaining_attempts_today','goal_remaining','maximum_possible_added_today'])require(Number.isSafeInteger(r[k])&&r[k]>=0,'Дневные счётчики не подтверждены.');
  require(r.verified_added_today<=r.account_attempts_today&&r.remaining_attempts_today===Math.max(0,100-r.account_attempts_today)&&r.goal_remaining===Math.max(0,100-r.verified_added_today)&&r.maximum_possible_added_today===r.verified_added_today+r.remaining_attempts_today&&r.goal_possible_with_current_budget===(r.maximum_possible_added_today>=100),'Дневной бюджет не совпал с результатами.');
 }
 function checkAudience(a,selectedLength){
  if(!a?.funnel)return;
  require(Number.isSafeInteger(a.saved_contact_count)&&a.saved_contact_count>=0&&a.funnel&&typeof a.funnel==='object','Воронка подбора не подтверждена.');
  let total=0;for(const key of Object.keys(funnelLabels)){require(Number.isSafeInteger(a.funnel[key])&&a.funnel[key]>=0,'Воронка подбора не подтверждена.');total+=a.funnel[key];}
  require(total===a.saved_contact_count&&a.funnel.selected===selectedLength,'Число выбранных людей не совпало с воронкой подбора.');
  require(Number.isSafeInteger(a.selected_mutual)&&a.selected_mutual>=0&&Number.isSafeInteger(a.selected_non_mutual)&&a.selected_non_mutual>=0&&a.selected_mutual+a.selected_non_mutual===selectedLength,'Взаимность выбранных контактов не подтверждена.');
 }
 function checkStatus(r){
  binding(r);capacity(r);
  require(Object.hasOwn(labels,r.state)&&r.can_resume===false&&r.max_attempts===modeLimit()&&typeof r.can_start==='boolean'&&typeof r.can_pause==='boolean','Состояние кампании не подтверждено. Обновите состояние.');
  require(Array.isArray(r.items)&&r.items.length<=modeLimit()&&r.counts&&typeof r.counts==='object','Список результатов не подтверждён.');
  const keys=[...countKeys,...['duplicate_skipped','cancelled','other'].filter(k=>Object.hasOwn(r.counts,k))];
  for(const k of keys)require(Number.isInteger(r.counts[k])&&r.counts[k]>=0&&r.counts[k]<=modeLimit(),'Счётчики кампании не подтверждены.');
  require(r.counts.total===r.items.length,'Счётчики и список получателей не совпали.');
  checkAudience(r.audience,r.items.length);
  const seen=new Set(),itemIds=new Set(),derived=Object.fromEntries(keys.map(k=>[k,0]));derived.total=r.items.length;
  for(const item of r.items){
   require(validId(item.item_id)&&validId(item.user_id)&&validUser(item.username)&&!seen.has(item.user_id)&&!itemIds.has(item.item_id)&&typeof item.state==='string'&&/^[a-z_]{1,40}$/.test(item.state),'Сервер вернул неподтверждённого получателя.');
   const key=item.state==='invited'?(item.diagnosis_code==='invite_member_verified'&&item.invitation_requested===true?'added':'uncertain'):item.state==='excluded'?'other':Object.hasOwn(itemLabels,item.state)?item.state:'other';require(Object.hasOwn(derived,key),'Исход получателя отсутствует в счётчиках.');
   seen.add(item.user_id);itemIds.add(item.item_id);derived[key]++;
  }
  for(const k of keys)require(derived[k]===r.counts[k],'Счётчик добавлений не совпал с результатами получателей.');
  require(Number.isInteger(r.attempts_claimed)&&r.attempts_claimed>=0&&r.attempts_claimed<=r.items.length,'Число попыток не подтверждено.');
  require((r.next_attempt_not_before==null||iso(r.next_attempt_not_before))&&(r.expires_at==null||iso(r.expires_at)),'Время следующего действия не подтверждено.');
  if(r.state==='unused')require(r.job_id===null&&r.items.length===0&&r.attempts_claimed===0,'Пустая кампания содержит задание.');
  else require(validId(r.job_id)&&validKey(r.request_key),'Запись кампании в журнале не подтверждена.');
  return r;
 }
 function checkPreview(r){
  binding(r);capacity(r);
  require(r.invitation_requested===false&&r.history_changed===false&&Array.isArray(r.selected)&&r.selected.length<=Math.min(modeLimit(),r.remaining_attempts_today),'Проверка списка не подтверждена. Приглашения не запускались.');
  require(Number.isInteger(r.membership_checks)&&r.membership_checks>=r.selected.length&&r.membership_checks<=150&&(r.selected.length?iso(r.expires_at):r.expires_at===null),'Срок проверки списка не подтверждён.');
  require(typeof r.policy_digest==='string'&&/^[a-f0-9]{64}$/.test(r.policy_digest),'Условия запуска не подтверждены сервером.');
  require(mode==='mutual'||r.audience?.funnel,'Воронка проверки существующих контактов не подтверждена.');
  checkAudience(r.audience,r.selected.length);
  const seen=new Set();
  for(const user of r.selected){
   require(validId(user.user_id)&&validUser(user.username)&&!seen.has(user.user_id)&&user.member===false&&user.sender_has_contact===true&&typeof user.mutual_contact==='boolean'&&(mode==='saved_contacts'||user.mutual_contact===true)&&iso(user.checked_at),'В списке есть неподтверждённый контакт. Проверьте список заново.');
   seen.add(user.user_id);
  }
  if(r.audience?.funnel)require(r.audience.selected_mutual===r.selected.filter(u=>u.mutual_contact).length,'Взаимность выбранных контактов не совпала с воронкой.');
  if(r.selected.length)require(typeof r.candidate_token==='string'&&/^[A-Za-z0-9_-]{43}$/.test(r.candidate_token),'Список не подтверждён сервером.');
  else require(r.candidate_token===null,'Пустой список содержит допуск запуска.');
  return r;
 }
 async function api(path,body){
  const options={method:body===undefined?'GET':'POST',credentials:'same-origin',cache:'no-store',headers:{'X-Scanner-Request':'1'},signal:AbortSignal.timeout(path.startsWith('/status?')?20000:180000)};
  if(body!==undefined){options.headers['Content-Type']='application/json';options.body=JSON.stringify(body);}
  let response;try{response=await fetch(base+path,options);}catch{throw Error('Ответ сервера не получен. Проверьте сохранённое состояние; повторного запуска нет.');}
  let value;try{value=await response.json();}catch{throw Error('Сервер не подтвердил результат. Обновите состояние; повторный запуск не выполняется.');}
  if(!response.ok){
   let detail=typeof value?.detail==='string'?value.detail.slice(0,600):'';
   for(const secret of [body?.candidate_token,preview?.candidate_token])if(typeof secret==='string'&&secret)detail=detail.split(secret).join('[скрыто]');
   detail=detail.replace(/\b(?:[A-Za-z0-9_-]{43}|[a-f0-9]{64})\b/g,'[скрыто]');
   const err=Error(response.status===401||response.status===403?'Нет доступа к кампании. Проверьте вход и роль в панели.':response.status===429?'Требуется пауза. Обновите сохранённое состояние.':[400,409,422].includes(response.status)&&detail?detail:'Действие не подтверждено сервером. Обновите сохранённое состояние.');err.status=response.status;throw err;
  }
  return value;
 }
 function clearPreview(){preview=null;confirmed=false;if(expiry){clearTimeout(expiry);expiry=null;}}
 function canPreview(){return !!mounted&&!busy&&!unknownWrite&&statusReady&&current?.can_start===true;}
 function canStart(){return canPreview()&&confirmed&&preview?.selected.length>0&&preview.utc_day===current.utc_day&&(preview.mode||'mutual')===mode&&Date.parse(preview.expires_at)>Date.now();}
 function button(text,action,disabled=false,secondary=true){return `<button type="button" class="button${secondary?' secondary':''}" data-iic-action="${action}"${disabled?' disabled':''}>${h(text)}</button>`;}
 function capacityView(r){
  if(!r)return '';
  return `<section aria-labelledby="iic-capacity-heading"><h3 id="iic-capacity-heading">Сегодня: ${r.verified_added_today} подтверждённых новых из цели 100</h3><progress class="iic-progress" max="100" value="${r.verified_added_today}" aria-label="Подтверждённые новые участники сегодня: ${r.verified_added_today} из цели 100"></progress>
   <p class="small">Цель — 100 новых участников за сутки. Результат зависит от ответов Telegram и настроек получателей.</p>
   <dl class="iic-ledger"><div><dt>Попытки аккаунта сегодня</dt><dd>${r.account_attempts_today} из ${r.account_day_attempt_ceiling}</dd></div><div><dt>Осталось попыток</dt><dd>${r.remaining_attempts_today}</dd></div><div><dt>До цели новых участников</dt><dd>${r.goal_remaining}</dd></div><div><dt>Максимум новых сегодня при успехе всех оставшихся попыток</dt><dd>${r.maximum_possible_added_today}</dd></div></dl>
   ${!r.goal_possible_with_current_budget?'<p class="notice">Сегодняшнего остатка попыток уже недостаточно для 100 новых участников. Можно выполнить доступную часть кампании; это не обещание результата.</p>':''}
   <p class="small muted">Расчётный день: ${h(r.utc_day)} (UTC). Следующий сброс: ${h(new Intl.DateTimeFormat('ru-RU',{dateStyle:'short',timeStyle:'short',timeZone:'Asia/Yekaterinburg'}).format(new Date(r.budget_reset_at)))} по Екатеринбургу, в 00:00 UTC.</p>
   <p class="small muted">Общий дневной бюджет инвайтинга — до 100 попыток аккаунта. В выбранном режиме один список содержит не больше ${modeLimit()} человек; обычный лимит остальных заданий остаётся ${r.normal_daily_attempt_limit}. Прежние пакеты учтены в дневных счётчиках.</p></section>`;
 }
 function modeView(){return `<fieldset class="iic-modes"><legend>Откуда подбирать участников</legend>${Object.entries(modes).map(([value,item])=>`<label><input type="radio" name="iic-mode" value="${value}" data-iic-mode${mode===value?' checked':''}${busy||unknownWrite?' disabled':''}><span>${h(item.title)}</span></label>`).join('')}<p class="small muted">${mode==='mutual'?'Прежний подбор: только существующие взаимные контакты с username.':'Небольшая проверка существующих контактов с username без требования взаимности. Telegram может отказать в добавлении; при требовании взаимного контакта проверка остановится.'}</p></fieldset>`;}
 function counters(r){const c=r.counts;return `<dl class="iic-ledger" aria-label="Результаты этой кампании"><div><dt>Новые в этой кампании</dt><dd>${c.added} из ${c.total} выбранных</dd></div><div><dt>Попытки кампании</dt><dd>${r.attempts_claimed}</dd></div><div><dt>Отказы Telegram</dt><dd>${c.restricted}</dd></div><div><dt>Результат не подтверждён</dt><dd>${c.uncertain}</dd></div><div><dt>Ожидают</dt><dd>${c.pending}</dd></div><div><dt>Выполняется</dt><dd>${c.sending}</dd></div></dl><p class="small">Уже в группе: ${c.already_member}. Не добавлены по другой причине: ${c.failed+c.blocked}. Пропущены повторы: ${c.duplicate_skipped||0}. Отменены: ${c.cancelled||0}. Другие состояния: ${c.other||0}. Это не новые добавления.</p>`;}
 function itemLabel(i){return i.state==='invited'&&!(i.diagnosis_code==='invite_member_verified'&&i.invitation_requested===true)?'Результат не подтверждён':itemLabels[i.state]||'Другое состояние; проверьте журнал';}
 function audienceView(a){
  if(!a||!['saved_contact_count','suitable_contacts_seen','membership_checks','nonmembers_selected'].every(k=>Number.isSafeInteger(a[k])&&a[k]>=0))return '';
  return `<section aria-label="Подбор получателей"><p class="small">Сохранённых контактов: ${a.saved_contact_count}. Подошли под фильтры подбора: ${a.suitable_contacts_seen}. Проверено членство: ${a.membership_checks}. Выбрано неучастников: ${a.nonmembers_selected}.${a.partial===true?' Членство проверено не для всех подходящих контактов.':''}</p>
   ${a.funnel?`<details><summary>Почему выбраны именно эти люди</summary><dl class="iic-ledger">${Object.entries(funnelLabels).map(([key,label])=>`<div><dt>${h(label)}</dt><dd>${a.funnel[key]}</dd></div>`).join('')}</dl><p class="small">В выбранном списке: взаимных контактов ${a.selected_mutual}, без подтверждённой взаимности ${a.selected_non_mutual}. Один человек учтён только в одной причине отбора или пропуска. ${mode==='mutual'?'Фильтр только взаимных контактов выбран в панели.':'В этом режиме отсутствие взаимности не исключает контакт из списка.'} Telegram отдельно проверяет возможность добавления.</p></details>`:''}</section>`;
 }
 function resultView(){
  if(!current?.job_id)return '';
  const active=['running','waiting'].includes(current.state);
  return `<section class="iic-results" aria-labelledby="iic-result-heading"><h3 id="iic-result-heading">${h(labels[current.state])} · задание №${current.job_id}</h3>${counters(current)}
   ${typeof current.campaign_utc_day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(current.campaign_utc_day)?`<p class="small">Эта запись относится к расчётному дню ${h(current.campaign_utc_day)} (UTC). Дневные счётчики выше показаны за ${h(current.utc_day)} (UTC).</p>`:''}
   ${audienceView(current.audience)}
   ${current.next_attempt_not_before&&active?`<p class="small">Следующая попытка не ранее ${h(stamp(current.next_attempt_not_before))}.</p>`:''}
   ${current.stop_reason==='UserNotMutualContactError'?'<p class="notice">В этой проверке Telegram потребовал взаимный контакт. Проверка остановлена; оставшимся людям запросы автоматически не отправляются.</p>':['stopped','interrupted'].includes(current.state)?'<p class="notice">Эта запись остановлена. Причина сохранена в её журнале; это не новая проверка состояния всего аккаунта. Автоматического возобновления нет.</p>':''}
   <details><summary>Получатели и результаты (${current.items.length})</summary><ul class="iic-people" tabindex="0" aria-label="Получатели кампании">${current.items.map(i=>`<li><strong>@${h(i.username)}</strong><span class="badge">${h(itemLabel(i))}</span>${i.state==='uncertain'?'<small>Повторного запроса нет. Проверьте членство в журнале.</small>':''}${i.diagnosis_code==='privacy_restricted'?'<small>Настройки приватности помешали добавлению.</small>':''}</li>`).join('')}</ul></details>
   <div class="ls-actions">${active&&current.can_pause?button('Остановить эту кампанию','pause',!!busy):''}${button('Обновить состояние','status',!!busy)}${button('Открыть журнал панели','history')}</div>
   ${active?'<p class="small muted">Остановка действует только на эту кампанию. Уже отправленный запрос Telegram может завершиться.</p>':'<p class="small muted">История сохранена в обычном журнале. Автоматического возобновления нет.</p>'}
  </section>`;
 }
 function previewView(){
  if(!preview)return '';
  if(!preview.selected.length)return '<p class="notice" role="status">В этой проверке подходящих '+(mode==='mutual'?'взаимных контактов':'существующих контактов')+', которых ещё нет в группе, не найдено. Приглашения не запускались.</p>'+audienceView(preview.audience);
  return `<section class="iic-preview" aria-labelledby="iic-list-heading"><h3 id="iic-list-heading">Проверьте список: ${people(preview.selected.length)}</h3>
   <p class="small">Это существующие ${mode==='mutual'?'взаимные ':''}контакты клиентки; при проверке они не состояли в группе. Список действует до ${h(stamp(preview.expires_at))}.</p>
   ${audienceView(preview.audience)}
   <details open><summary>Состав кампании (${preview.selected.length})</summary><ol class="iic-people" tabindex="0" aria-label="Проверенный состав кампании">${preview.selected.map(u=>`<li><strong>@${h(u.username)}</strong><span class="small">${u.mutual_contact?'Взаимный контакт':'Контакт клиентки; взаимность не подтверждена'}; ещё не в группе</span></li>`).join('')}</ol></details>
   <label class="ls-check"><input type="checkbox" data-iic-confirm${confirmed?' checked':''}${busy?' disabled':''}><span>Проверил список, режим «${h(modes[mode].title)}», аккаунт клиентки и группу «ТЕСТОВОЕ СООБЩЕСТВО». Подтверждаю одну конечную кампанию с показанным дневным бюджетом.</span></label>
   <div class="ls-actions">${button(busy==='start'?'Запускаем…':`Запустить для ${people(preview.selected.length,true)}`,'start',!canStart(),false)}${button('Проверить список заново','preview',!canPreview())}</div>
  </section>`;
 }
 function draw(){
  const el=root();if(!el)return;
  el.innerHTML=`<style>#isolated-invite-campaign .iic-modes{border:0;padding:0;margin:18px 0}#isolated-invite-campaign .iic-modes legend{font-weight:650;padding:0}#isolated-invite-campaign .iic-modes label{display:flex;align-items:center;gap:12px;min-height:44px;padding:8px 0}#isolated-invite-campaign .iic-modes input{width:18px;height:18px;flex-shrink:0}#isolated-invite-campaign .iic-progress{width:100%;height:12px;accent-color:var(--primary,#0b788b)}#isolated-invite-campaign .iic-ledger{display:grid;grid-template-columns:1fr 1fr;gap:0 24px;margin:16px 0}#isolated-invite-campaign .iic-ledger>div{display:flex;align-items:baseline;justify-content:space-between;gap:12px;border-bottom:1px solid var(--line,#dbe5e9);padding:10px 0}#isolated-invite-campaign dt{max-width:35ch}#isolated-invite-campaign dd{margin:0;font-weight:700;white-space:nowrap;font-variant-numeric:tabular-nums}#isolated-invite-campaign .iic-people{max-height:360px;overflow-y:auto}#isolated-invite-campaign summary{cursor:pointer;padding:12px 0;min-height:44px}#isolated-invite-campaign :focus-visible{outline:3px solid var(--primary,#0b788b);outline-offset:3px}@media(max-width:600px){#isolated-invite-campaign .iic-ledger{grid-template-columns:1fr}}#isolated-invite-campaign{margin-bottom:22px;min-width:0}#isolated-invite-campaign p{max-width:78ch;overflow-wrap:anywhere}#isolated-invite-campaign .iic-people{padding-left:0;list-style:none;margin:14px 0;border-top:1px solid var(--line,#e5ebef)}#isolated-invite-campaign .iic-people li{display:flex;align-items:center;justify-content:space-between;gap:9px 16px;flex-wrap:wrap;padding:12px 0;border-bottom:1px solid var(--line,#e5ebef);overflow-wrap:anywhere;min-width:0}#isolated-invite-campaign .iic-people small{flex-basis:100%}#isolated-invite-campaign .iic-counts{display:flex;gap:12px 22px;flex-wrap:wrap;margin:14px 0}#isolated-invite-campaign .button{min-height:44px;white-space:normal;max-width:100%;overflow-wrap:anywhere}#isolated-invite-campaign h3{margin-top:20px}#isolated-invite-campaign .iic-preview,#isolated-invite-campaign .iic-results{padding-top:4px}#isolated-invite-campaign input[type=checkbox]{width:18px;height:18px;flex-shrink:0}@media(max-width:600px){#isolated-invite-campaign .ls-actions{align-items:stretch}#isolated-invite-campaign .ls-actions>.button{flex:1 1 210px}#isolated-invite-campaign .iic-people li{align-items:flex-start}}</style>
   <p><a class="button secondary" href="#invite-settings">Новая кампания: настройки инвайтинга</a></p><h2 id="iic-heading">Прежняя кампания из контактов · история и управление</h2><p>Аккаунт: <strong>@demo_identity_1</strong>. Группа: <strong>ТЕСТОВОЕ СООБЩЕСТВО</strong>.</p>
   <p class="small">Прямое добавление из уже существующих контактов, с паузой не меньше 60 секунд. Контакты не создаются; личные сообщения и ссылки не отправляются.</p>
   <p class="small muted">Основные исходящие остаются на паузе. Работает только отдельно подтверждённая кампания; прежние задания не запускаются.</p>
   ${modeView()}
   ${error?`<p class="ls-error" role="alert">${h(error)}</p>`:''}${info?`<p class="notice" role="status">${h(info)}</p>`:''}
   ${!current&&!error?'<p class="small" role="status">Проверяем сохранённое состояние…</p>':''}
   ${unknownWrite?'<p class="notice" role="status">Ответ на запуск не получен. Проверяем сохранённое состояние без повторной отправки.</p>':''}
   ${capacityView(preview||current)}
   ${current?.can_start&&!preview?`<div class="ls-actions">${button(busy==='preview'?'Проверяем контакты…':mode==='mutual'?'Подготовить список получателей':'Проверить до 10 существующих контактов','preview',!canPreview())}</div><p class="small muted">Проверка подготавливает список до ${modeLimit()} человек в пределах остатка попыток. Запуск потребует отдельного подтверждения.</p>`:''}
   ${!current?.job_id?`<div class="ls-actions">${button('Обновить состояние','status',!!busy)}</div>`:''}
   ${previewView()}${resultView()}`;
 }
 async function readStatus(){
  const sequence=++readSequence,seenRevision=revision,version=lifecycle;
  try{
   const value=checkStatus(await api('/status?scope='+encodeURIComponent(scope)+'&mode='+encodeURIComponent(mode)));
   if(!alive(version)||sequence!==readSequence||seenRevision!==revision)return;
   if(preview&&(preview.utc_day!==value.utc_day||(preview.mode||'mutual')!==mode||preview.account_attempts_today!==value.account_attempts_today||!value.can_start))clearPreview();
   current=value;statusReady=true;
   if(value.job_id){if(value.request_key===requestKey)rememberPending('');clearPreview();}
   if(!busy&&!unknownWrite)error='';
  }catch(e){if(alive(version)&&sequence===readSequence&&seenRevision===revision){current=null;statusReady=false;clearPreview();error=e.message||'Состояние кампании не загружено.';}}
  if(alive(version))draw();
 }
 async function readPreview(){
  require(canPreview(),'Сначала обновите состояние кампании. Нового запуска не было.');
  const version=lifecycle,seenRevision=++revision;readSequence++;busy='preview';error='';info='';clearPreview();draw();
  try{
   const value=checkPreview(await api('/contacts-preview?scope='+encodeURIComponent(scope)+'&mode='+encodeURIComponent(mode)));
   if(!alive(version)||seenRevision!==revision)return;
   preview=value;confirmed=false;
   if(!value.selected.length)return;
   const remaining=Date.parse(value.expires_at)-Date.now();
   if(remaining<=0){clearPreview();throw Error('Срок проверки списка истёк. Проверьте контакты заново.');}
   expiry=setTimeout(()=>{if(!alive(version))return;clearPreview();info='Срок проверки списка истёк. Проверьте контакты заново.';draw();},Math.min(remaining,2147483647));
  }catch(e){if(alive(version)&&seenRevision===revision){clearPreview();error=e.message||'Проверка списка не завершена. Приглашения не запускались.';}}
  finally{if(alive(version)&&seenRevision===revision){busy='';draw();}}
 }
 async function start(){
  require(canStart(),'Подтвердите действующий список получателей. Приглашения не запускались.');
  require(typeof crypto?.randomUUID==='function','Обновите браузер, чтобы подтвердить запуск.');
  const token=preview.candidate_token,policy=preview.policy_digest,key=crypto.randomUUID(),version=lifecycle;rememberPending(key);busy='start';revision++;readSequence++;error='';info='';draw();
  // Once a POST is attempted the preview is discarded, even if its response is lost.
  clearPreview();unknownWrite=true;
  try{
   const value=checkStatus(await api('/start',{scope,mode,confirmed:true,reason:'Ручное подтверждение списка существующих контактов в режиме «'+modes[mode].title+'» для группы «ТЕСТОВОЕ СООБЩЕСТВО» и показанного дневного бюджета кампании в панели.',request_key:key,candidate_token:token,policy_digest:policy}));
   if(!alive(version))return;
   require(value.request_key===key&&validId(value.job_id),'Сервер не подтвердил задание этого запуска. Обновите состояние.');
   current=value;statusReady=true;rememberPending('');info='Кампания запущена. Показаны сохранённые результаты каждого получателя.';
  }catch(e){if(alive(version)){statusReady=false;error=(e.message||'Ответ на запуск не получен.')+' Повторная отправка не выполняется.';}}
  finally{if(alive(version)){busy='';draw();if(unknownWrite)await readStatus();}}
 }
 async function pause(){
  require(!busy&&current&&['running','waiting'].includes(current.state)&&current.can_pause&&validId(current.job_id),'Эта кампания сейчас не выполняется.');
  const key=current.request_key,job=current.job_id,version=lifecycle;busy='pause';revision++;readSequence++;error='';draw();
  try{
   const value=checkStatus(await api('/pause',{scope,mode,request_key:key,job_id:job}));
   if(!alive(version))return;
   require(value.job_id===job&&value.request_key===key,'Ответ относится к другой кампании. Обновите состояние.');
   current=value;info='Остановка запрошена только для этой кампании. Уже отправленный запрос Telegram может завершиться.';
  }catch(e){if(alive(version))error=(e.message||'Остановка не подтверждена.')+' Проверьте сохранённое состояние.';}
  finally{if(alive(version)){busy='';draw();await readStatus();}}
 }
 function view(){return '<section id="isolated-invite-campaign" class="panel ls-section" aria-labelledby="iic-heading"></section>';}
 function switchMode(next){
  require(Object.hasOwn(modes,next),'Выберите доступный режим подбора.');
  if(next===mode)return;
  require(!busy&&!unknownWrite,'Сначала дождитесь сохранённого результата текущего действия.');
  mode=next;unmount();rememberPending('');error='';info='';mount();
 }
 function unmount(){mounted=null;lifecycle++;readSequence++;revision++;current=null;statusReady=false;busy='';clearPreview();if(poll){clearInterval(poll);poll=null;}}
 function mount(){
  const el=root();if(!el){unmount();return;}
  if(mounted===el)return;
  unmount();mounted=el;restorePending();draw();readStatus();
  if(!poll)poll=setInterval(()=>{if(!root()){unmount();return;}if(document.visibilityState==='hidden'||busy)return;if(unknownWrite||current&&['running','waiting'].includes(current.state))readStatus();},5000);
 }
 document.addEventListener('change',event=>{
  const t=event.target;if(!t?.closest?.('#isolated-invite-campaign'))return;
  if(t.hasAttribute?.('data-iic-mode')){if(busy||unknownWrite){draw();return;}try{switchMode(t.value);}catch(e){error=e.message;draw();}return;}
  if(!t.hasAttribute?.('data-iic-confirm')||busy)return;confirmed=t.checked;const b=root()?.querySelector?.('[data-iic-action="start"]');if(b)b.disabled=!canStart();
 });
 document.addEventListener('click',event=>{
  const b=event.target.closest?.('[data-iic-action]');if(!b||!b.closest('#isolated-invite-campaign')||b.disabled)return;
  event.preventDefault();event.stopPropagation();
  const action=b.dataset.iicAction;
  if(action==='history'){window.TelegramOperations?.openTab?.('jobs');location.hash='#operations';return;}
  if(busy)return;
  const work=action==='status'?readStatus:action==='preview'?readPreview:action==='start'?start:action==='pause'?pause:null;
  if(work)Promise.resolve().then(work).catch(e=>{error=e.message||'Действие не выполнено.';draw();});
 });
 return {view,mount,unmount};
})();
