'use strict';
window.OperationPreview=(()=>{
 const h=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function topicLabel(title,id){return window.OperationTopics?.label?.(title,id)||(id===1?'Общий раздел (General), ID 1':String(title??'')+' · #'+String(id??''));}
 function outgoing(d){const text=String(d.text||''),url=String(d.invite_url||'');return d.kind==='invite_message'&&!text.includes(url)?text+'\n\n'+url:text;}
 function links(text){
  const found=new Map();
  for(const token of text.match(/https?:\/\/[^\s<>"']+|@[a-zA-Z][a-zA-Z0-9_]{3,31}\b/g)||[]){
   const value=token.replace(/[.,;!?)]*$/,'');
   try{const url=new URL(value.startsWith('@')?'https://t.me/'+value.slice(1):value);if(['http:','https:'].includes(url.protocol))found.set(url.href,{text:value,url:url.href});}catch{}
  }
  return [...found.values()].slice(0,30);
 }
 function policyTime(value){
  const date=value?new Date(value):null;
  if(!date||!Number.isFinite(date.getTime()))return 'Не указано';
  return new Intl.DateTimeFormat('ru-RU',{timeZone:'Asia/Dubai',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(date);
 }
 function interval(seconds){
  if(!Number.isSafeInteger(seconds)||seconds<0)return 'Не указан';
  if(!seconds)return 'Не задан';
  const hours=Math.floor(seconds/3600),minutes=Math.floor(seconds%3600/60),rest=seconds%60;
  return [hours?hours+' ч':'',minutes?minutes+' мин':'',rest?rest+' сек':''].filter(Boolean).join(' ');
 }
 function postingPolicyView(d,targets=[]){
  const policy=d?.posting_policy;
  if(d?.kind!=='post'||!policy||typeof policy!=='object'||!Array.isArray(policy.destinations))return '';
  const weekdays=['пн','вт','ср','чт','пт','сб','вс'];
  return `<section class="op-posting-policy" data-op-posting-policy><h4>Расписание публикаций</h4><dl class="op-preview-settings"><div><dt>Окно отправки · Дубай (UTC+4)</dt><dd>${h(policyTime(d.scheduled_at))} — ${h(policyTime(policy.window_expires_at))}</dd></div><div><dt>Кампания до · Дубай (UTC+4)</dt><dd>${h(policyTime(policy.campaign_expires_at))}</dd></div><div><dt>Часовой пояс частоты</dt><dd>${h(policy.timezone||'Не указан')}</dd></div></dl><ul class="op-policy-destinations">${policy.destinations.map(item=>{
   const target=targets.find(t=>String(t.id??t.chat_id)===String(item.chat_id)),title=target?.title||target?.chat_title||String(item.chat_id),days=Array.isArray(item.allowed_weekdays)?[...new Set(item.allowed_weekdays.filter(n=>Number.isInteger(n)&&n>=0&&n<7))].sort():null;
   const rule=links(String(item.rule_url||'')).find(l=>l.url===item.rule_url);
   return `<li><strong>${h(title)}</strong><span>Не более ${h(item.max_per_day)} в сутки · интервал: ${h(interval(item.min_interval_seconds))}</span>${days?`<span>${days.length===7?'Ежедневно':days.length?'Дни: '+days.map(n=>weekdays[n]).join(', '):'Дни размещения не выбраны'}</span>`:''}${rule?`<a href="${h(rule.url)}" target="_blank" rel="noopener noreferrer">Условия размещения</a>`:''}</li>`;
  }).join('')}</ul><p class="small muted">Если публикацию удалят, следующие размещения в этой группе остановятся.</p></section>`;
 }
 function topicView(rows=[]){return rows.length?`<section class="op-topic-picker"><h4>Разделы публикации</h4><ul>${rows.map(t=>`<li><strong>${h(t.chat_title||t.chat_id)}</strong><span class="op-topic-label">Раздел: ${h(topicLabel(t.title,t.topic_id))}</span></li>`).join('')}</ul></section>`:'';}
 function jobView(j){return postingPolicyView({...j?.spec,kind:j?.kind},(j?.items||[]).map(i=>({id:i.chat_id,title:i.chat_title})))+(j?.kind==='post'?topicView(j.spec?.topic_snapshots||[]):'');}
 function model(d){
  const text=outgoing(d),media=d.kind==='post'?[...(d.media||[])]:[],limit=media.length?1024:4096,errors=[];
  if(['post','message','invite_message'].includes(d.kind)&&!text.trim()&&!media.length)errors.push('Добавьте текст или фото.');
  if(text.length>limit)errors.push(media.length?'Подпись к фото превышает 1024 символа.':'Текст превышает 4096 символов.');
  if(media.length>10)errors.push('В публикации может быть до 10 фото.');
  if(d.kind==='invite_message'&&!/^https:\/\/t\.me\/(?:[A-Za-z][A-Za-z0-9_]{3,31}|\+[A-Za-z0-9_-]+)$/.test(d.invite_url||''))errors.push('Укажите ссылку t.me на выбранную группу или канал.');
  if(!d.account)errors.push('Выберите аккаунт отправителя.');
  else if(d.account.connection!=='connected'||!d.account.enabled)errors.push('Исходящие выбранного аккаунта недоступны.');
  if(d.kind==='post')for(const t of d.targets||[])if(t.forum===true&&(!Number.isSafeInteger(t.topic_id)||t.topic_id<1||!t.topic_title))errors.push('Выберите и проверьте раздел для «'+(t.title||'группа')+'».');
  if(!(d.targets||[]).length)errors.push(d.kind==='post'?'Выберите группы для публикации.':'Укажите получателей.');
  return {text,media,limit,errors,links:links(text),length:text.length};
 }
 function view(){return '<section id="op-text-preview" class="panel ls-section op-expanded-preview"><h3>Предпросмотр публикации</h3><p class="small muted">Текст и порядок фото из текущего черновика. Оформление приложения Telegram может отличаться.</p><div id="op-expanded-preview"><p id="op-preview">Введите текст сообщения.</p></div></section>';}
 function update(d){
  const el=document.querySelector('#op-expanded-preview');if(!el)return;
  const m=model(d),a=d.account,targets=d.targets||[],kind=d.kind==='post'?'Группы':'Получатели',sender=a?(a.username?'@'+a.username:a.label||a.id):'Не выбран';
  const schedule=d.scheduled_at&&Number.isFinite(new Date(d.scheduled_at).getTime())?new Date(d.scheduled_at).toLocaleString('ru-RU'):'После отдельного подтверждения запуска';
  el.innerHTML=`<div class="op-preview-envelope"><p class="small"><strong>Отправитель:</strong> ${h(sender)}${a?.telegram_id?' · ID '+h(a.telegram_id):''}</p><p class="small"><strong>${kind} (${targets.length}):</strong> ${targets.length?targets.slice(0,50).map(t=>h(t.title||t)+(t.forum===true?' · '+h(t.topic_title?(t.topic_id===1?topicLabel(t.topic_title,t.topic_id):'раздел «'+t.topic_title+'» · #'+t.topic_id):'раздел не выбран'):'')).join(', '):'Не выбраны'}</p><div class="op-preview-message">${m.media.length?`<div class="op-preview-gallery">${m.media.map((id,i)=>`<figure><img src="/api/scanner/operations/media/${encodeURIComponent(String(id))}" alt="Фото ${i+1} из ${m.media.length}"><figcaption>Фото ${i+1}</figcaption></figure>`).join('')}</div>`:''}<p id="op-preview">${h(m.text|| (m.media.length?'':'Введите текст сообщения.'))}</p></div><p class="small" data-op-preview-length>${m.length} / ${m.limit} ${m.media.length?'· подпись к фото':'· текст'}${m.media.length>1?' · альбом, подпись у первого фото':''}</p></div>${m.links.length?`<details data-op-preview-links><summary>Контакты и ссылки в тексте (${m.links.length})</summary><ul>${m.links.map(l=>`<li><a href="${h(l.url)}" target="_blank" rel="noopener noreferrer">${h(l.text)}</a></li>`).join('')}</ul></details>`:''}<dl class="op-preview-settings"><div><dt>Запуск</dt><dd>${h(schedule)}</dd></div><div><dt>Пауза аккаунта</dt><dd>${h(d.delay_seconds)} сек.</dd></div><div><dt>Дневной предел аккаунта · UTC</dt><dd>${h(d.daily_limit)}</dd></div></dl>${postingPolicyView(d,targets)}${m.errors.length?`<div class="ls-error" role="status"><strong>Перед сохранением</strong><ul>${m.errors.map(e=>`<li>${h(e)}</li>`).join('')}</ul></div>`:'<p class="small" role="status">Черновик заполнен. Права площадок и получатели проверяются сервером при сохранении.</p>'}`;
 }
 return {view,update,model,jobView};
})();
