'use strict';
window.OwnerSenderPanel=(()=>{
 let state=null,error='',busy=false,poll=null;
 const h=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const root=()=>document.querySelector('#owner-sender');
 const field=(label,name,type,attrs='')=>`<label class="ls-field">${h(label)}<input name="${name}" type="${type}" required ${attrs}></label>`;
 const form=(name,body,label)=>`<form data-sender-form="${name}">${body}<button class="button" ${busy?'disabled':''}>${label}</button></form>`;
 const phoneForm=()=>form('code',field('Номер аккаунта @demo_identity_3','phone','tel','placeholder="+79068128175" pattern="\\+[0-9]{10,15}" autocomplete="tel"'),'Получить код Telegram');
 const qrButton=()=>`<button class="button secondary" data-sender-qr ${busy?'disabled':''}>Войти по QR без кода</button>`;
 async function api(path='',body){const r=await fetch('/api/scanner/marketing/sender'+path,{method:body===undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-Scanner-Request':'1'},signal:AbortSignal.timeout(45000),...(body===undefined?{}:{body:JSON.stringify(body)})});let data;try{data=await r.json();}catch{throw Error('Сервис не ответил. Повторите проверку подключения.');}if(!r.ok)throw Error(data.detail||'Не удалось выполнить действие.');return data;}
 function view(){return '<section id="owner-sender" class="panel ls-section"><h2>Отправитель · @demo_identity_3</h2><p>Проверяем личное подключение…</p></section>';}
 function delivery(){return ({SentCodeTypeApp:'Telegram выбрал доставку в служебный чат «Telegram» в уже подключённом приложении. Открой на телефоне именно аккаунт с этим номером и проверь подключение к сети.',SentCodeTypeSms:'Telegram выбрал SMS на указанный номер. Проверь сообщения на телефоне.',SentCodeTypeCall:'Telegram выбрал телефонный звонок. Дождись звонка на указанный номер.',SentCodeTypeEmailCode:'Telegram выбрал доставку на почту для входа. Если этот способ не подходит, используй QR.',SentCodeTypeFirebaseSms:'Telegram выбрал SMS с проверкой официального приложения. Используй вход по QR.'})[state?.delivery_method]||'Запрос принят Telegram. Если свежий код не появился, используй вход по QR.';}
 function schedule(){clearTimeout(poll);if(state?.connection==='qr_required'&&root())poll=setTimeout(async()=>{if(root()&&!busy)await mount();else if(root())schedule();},2000);}
 async function mount(){try{state=await api();error='';}catch(e){error=e.message;}draw();schedule();}
 function draw(){
  const el=root();if(!el)return;
  let body='';
  if(!state)body='<button class="button" data-sender-refresh>Проверить ещё раз</button>';
  else if(state.connection==='connected')body=`<div class="notice">Подключён @${h(state.username)} · ID ${h(state.account_id)}. Теперь открой «Переписка → Настройки ИИ».</div><button class="button secondary" data-sender-disconnect>Отключить личного отправителя</button>`;
  else if(state.connection==='qr_required')body=`<p>На телефоне открой Telegram под @demo_identity_3 → Настройки → Устройства → Подключить устройство. Отсканируй QR и подтверди вход.</p><img src="/api/scanner/marketing/sender/qr-image?v=${h(state.qr_expires_at)}" alt="QR для подключения твоего Telegram" width="260" height="260" style="display:block;background:white;max-width:100%;margin:16px 0"><p class="muted">QR действует до ${h(new Date(state.qr_expires_at*1000).toLocaleTimeString('ru-RU'))}. После подтверждения панель сама перейдёт к следующему шагу. Если потребуется облачный пароль, введи его здесь.</p><button class="button secondary" data-sender-refresh>Проверить подтверждение</button>`;
  else if(state.connection==='code_required')body=form('confirm',`<p>${h(delivery())}</p>`+field('Свежий код входа','code','text','inputmode="numeric" pattern="[0-9]{4,8}" autocomplete="one-time-code"'),'Подтвердить код')+'<p>Нет свежего кода?</p>'+qrButton();
  else if(state.connection==='password_required')body=form('password','<p>Telegram подтвердил первый шаг входа. Теперь нужен пароль двухэтапной проверки, заданный в настройках Telegram.</p>'+field('Облачный пароль','password','password','autocomplete="off"'),'Подключить @demo_identity_3');
  else if(state.api_available)body=qrButton()+'<details style="margin-top:16px"><summary>Войти по номеру и коду</summary>'+phoneForm()+'</details>';
  else body='<p>В сервисе пока недоступно API-приложение.</p>';
  const retry=state?.retry_after>0?`<p class="muted">До следующего запроса входа: ${h(state.retry_after)} сек.</p>`:'';
  el.innerHTML=`<h2>Твой Telegram · @demo_identity_3</h2><p>Здесь подключается аккаунт, от которого помощник будет вести личные диалоги. API-приложение уже настроено в сервисе: заново создавать его на my.telegram.org не нужно.</p>${error||state?.error?`<div class="ls-error" role="alert">${h(error||state.error)}</div>`:''}${body}${retry}${state?.connection==='code_required'?`<details style="margin-top:16px"><summary>Запросить новый код</summary>${phoneForm()}</details>`:''}<p class="muted small">Вводи код и пароль только в этой форме. QR предназначен только для твоего входа. Сервис проверит, что подключён именно @demo_identity_3. После входа выбери сценарии и включи помощника в настройках ИИ.</p>`;
 }
 function showError(message){error=message;const old=root()?.querySelector('.ls-error');if(old)old.remove();const el=document.createElement('div');el.className='ls-error';el.setAttribute('role','alert');el.textContent=error;root()?.prepend(el);}
 document.addEventListener('submit',async e=>{const f=e.target;if(!f.matches('[data-sender-form]'))return;e.preventDefault();if(busy)return;busy=true;const b=f.querySelector('button');b.disabled=true;error='';try{await api('/'+f.dataset.senderForm,Object.fromEntries(new FormData(f)));f.reset();busy=false;await mount();}catch(err){if(f.dataset.senderForm==='code'){try{state=await api();}catch{}draw();}showError(err.message);}finally{busy=false;b.disabled=false;schedule();}});
 document.addEventListener('click',async e=>{
  if(e.target.closest('[data-sender-refresh]')&&!busy)await mount();
  if(e.target.closest('[data-sender-qr]')&&!busy){busy=true;error='';e.target.closest('button').disabled=true;try{await api('/qr',{});busy=false;await mount();}catch(err){showError(err.message);}finally{busy=false;root()?.querySelector('[data-sender-qr]')?.removeAttribute('disabled');schedule();}}
  if(e.target.closest('[data-sender-disconnect]')&&!busy){if(!confirm('Отключить только личного отправителя и остановить его сообщения?'))return;try{await api('/disconnect',{});await mount();}catch(err){showError(err.message);}}
 });
 return {view,mount};
})();
