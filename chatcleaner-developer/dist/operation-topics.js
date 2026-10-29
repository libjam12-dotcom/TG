'use strict';
// Explicit forum-section selection shared by the existing composers. No writes.
window.OperationTopics=(()=>{
 const h=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function label(title,id){return id===1?'Общий раздел (General), ID 1':String(title??'')+' · #'+String(id??'');}
 function topicId(value,target){
  const raw=String(value||'').trim();let token=raw;
  if(!/^\d+$/.test(raw)){
   let url;try{url=new URL(raw);}catch{throw Error('Вставьте ссылку на раздел Telegram или его номер.');}
   if(url.protocol!=='https:'||url.hostname!=='t.me')throw Error('Нужна ссылка на раздел на t.me.');
   const parts=url.pathname.split('/').filter(Boolean),username=String(target.username||target.chat_username||'').replace(/^@/,'').toLowerCase();
   if(parts[0]==='c'){
    if(!/^\d+$/.test(parts[1]||'')||-1000000000000-Number(parts[1])!==target.id)throw Error('Ссылка относится к другой группе.');
    token=parts[2];if(parts.length<3||parts.length>4)throw Error('Скопируйте ссылку именно на нужный раздел.');
   }else{
    if(!username||parts[0]?.toLowerCase()!==username)throw Error('Ссылка не совпадает с выбранной группой. Можно указать номер раздела.');
    token=parts[1];if(parts.length<2||parts.length>3)throw Error('Скопируйте ссылку именно на нужный раздел.');
   }
  }
  const id=Number(token);if(!/^\d+$/.test(String(token||''))||!Number.isSafeInteger(id)||id<1||id>2147483647)throw Error('Укажите номер раздела от 1 до 2147483647.');
  return id;
 }
 function create(){
  let identity='',targets=[],entries=new Map();
  function sync(account,list=[],kind='post'){
   const key=account?.id&&Number.isSafeInteger(account.telegram_id)?account.id+':'+account.telegram_id:'';
   if(identity!==key){identity=key;entries=new Map();}
   targets=kind==='post'?list.filter(t=>t.forum===true):[];
   const ids=new Set(targets.map(t=>t.id));for(const id of entries.keys())if(!ids.has(id))entries.delete(id);
   for(const t of targets)if(!entries.has(t.id))entries.set(t.id,{value:'',topic:null,error:'',loading:false,revision:0});
  }
  function input(cid,value){const row=entries.get(Number(cid));if(!row)return;row.value=value;row.topic=null;row.error='';row.loading=false;row.revision++;}
  async function check(cid,api,account){
   cid=Number(cid);const row=entries.get(cid),target=targets.find(t=>t.id===cid),key=identity;if(!row||!target||!account||key!==account.id+':'+account.telegram_id)return false;
   const revision=++row.revision;row.topic=null;row.error='';row.loading=true;
   try{
    const id=topicId(row.value,target),r=await api('/accounts/'+encodeURIComponent(account.id)+'/destinations/'+cid+'/topics/'+id);
    if(entries.get(cid)!==row||identity!==key||row.revision!==revision)return false;
    if(r?.account!==account.id||r.telegram_id!==account.telegram_id||r.chat_id!==cid||r.topic_id!==id||r.forum!==true||r.closed!==false||r.hidden!==false||r.writable!==true||typeof r.title!=='string'||!r.title.trim()||!Number.isFinite(Date.parse(r.checked_at)))throw Error('Сервер не подтвердил доступный раздел этой группы. Проверьте его заново.');
    row.topic={...r};return true;
   }catch(e){if(entries.get(cid)===row&&identity===key&&row.revision===revision)row.error=e.message||'Раздел не удалось проверить.';return false;}
   finally{if(entries.get(cid)===row&&identity===key&&row.revision===revision)row.loading=false;}
  }
  function mappings(){return targets.map(t=>{const row=entries.get(t.id);if(!row?.topic||row.loading)throw Error('Выберите и проверьте раздел для «'+t.title+'».');return {chat_id:t.id,topic_id:row.topic.topic_id};});}
  function preview(list){return list.map(t=>{const row=entries.get(t.id);return t.forum===true?{...t,topic_id:row?.topic?.topic_id,topic_title:row?.topic?.title,topic_required:!row?.topic}:t;});}
  function view(){return targets.length?`<section class="op-topic-picker"><h3>Разделы выбранных групп</h3><p class="small muted">Скопируйте ссылку на нужный раздел из Telegram или укажите его номер. Для каждой группы выберите раздел явно. Для общего раздела (General) укажите 1.</p><p class="small">Для публикации в разделе нужен сохранённый профиль. Не более одной публикации в группе за сутки, с интервалом от 4 часов. Окно разовой отправки — 2 часа. Смена раздела не снимает ограничения группы после удаления или запрета.</p>${targets.map(t=>{const row=entries.get(t.id);return `<div class="op-topic-card"><label class="ls-field">Раздел: ${h(t.title)}<input type="text" data-op-topic-input="${t.id}" value="${h(row.value)}" placeholder="Ссылка на раздел или его номер" autocomplete="off"></label><button type="button" class="button secondary cc-button" data-op-topic-check="${t.id}" ${row.loading?'disabled':''}>${row.loading?'Проверяем раздел…':'Проверить раздел'}</button><p class="${row.error?'ls-error':'small'}" role="status">${row.error?h(row.error):row.topic?'Выбран раздел: '+h(label(row.topic.title,row.topic.topic_id)):'Раздел пока не выбран.'}</p></div>`;}).join('')}</section>`:'';}
  return {sync,input,check,mappings,preview,view};
 }
 function singlePolicy(ids,scheduledAt,now=Date.now()){const start=scheduledAt?Date.parse(scheduledAt):now;if(!Number.isFinite(start))throw Error('Проверьте время публикации.');const end=new Date(start+2*3600000).toISOString();return {window_expires_at:end,campaign_expires_at:end,timezone:'Asia/Dubai',destinations:ids.map(chat_id=>({chat_id,max_per_day:1,min_interval_seconds:14400}))};}
 return {create,topicId,singlePolicy,label};
})();
