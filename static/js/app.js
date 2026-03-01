let C={},taTimer=null,taRes=[],taIdx=-1,pendingNport=null;
const COLORS=['#5b8def','#8b6cf6','#ec4899','#f59e0b','#34d399','#f87171','#22d3ee','#84cc16','#f97316','#6366f1','#14b8a6','#e879f9','#a78bfa','#a3e635'];
let displayTopN=10;
let _xlinkTickers=new Set();

const STORY_SECTIONS=[
  {id:'highlights',type:'analysis',title:'Key Highlights'},
  {id:'overview',type:'analysis',title:'Portfolio Overview'},
  {id:'holdings',type:'holdings',title:'Holdings'},
  {id:'sectors',type:'analysis',title:'Sector Positioning'},
  {id:'sectorBar',type:'chart',title:'Sector Allocation'},
  {id:'industryBar',type:'chart',title:'Industry Allocation'},
  {id:'geography',type:'analysis',title:'Geographic Exposure'},
  {id:'geoBar',type:'chart',title:'Geographic Allocation'},
  {id:'valuation',type:'analysis',title:'Valuation & Growth'},
  {id:'bubble',type:'chart',title:'Valuation vs Growth'},
  {id:'earnings',type:'analysis',title:'Earnings Surprises'},
  {id:'risks',type:'analysis',title:'Key Risks'}
];

/* ── Quarter helpers ───────────────────────── */
function qLabel(dateStr){
  if(!dateStr)return '';
  const p=dateStr.split('-');if(p.length<2)return '';
  const yr=p[0].slice(-2),m=parseInt(p[1]),q=Math.ceil(m/3);
  return q+'Q'+yr}
function priorQEnd(dateStr){
  if(!dateStr)return '';
  const p=dateStr.split('-');const yr=parseInt(p[0]),m=parseInt(p[1]),q=Math.ceil(m/3);
  if(q===1)return(yr-1)+'-12-31';if(q===2)return yr+'-03-31';
  if(q===3)return yr+'-06-30';return yr+'-09-30'}

/* ── Quarter Select ────────────────────────── */
function populateQuarterSelect(){
  const sel=document.getElementById('sDate');
  const now=new Date();
  const qEndDates=[[3,31],[6,30],[9,30],[12,31]];
  // Find most recent completed quarter end
  let startYr=now.getFullYear(),startQi=-1;
  for(let qi=3;qi>=0;qi--){
    const candidate=new Date(startYr,qEndDates[qi][0]-1,qEndDates[qi][1]);
    if(candidate<=now){startQi=qi;break}}
  if(startQi<0){startYr--;startQi=3}
  // Walk back 8 quarters
  let options=[],yr=startYr,qi=startQi;
  for(let i=0;i<8;i++){
    const m=qEndDates[qi][0],d=qEndDates[qi][1];
    const ds=yr+'-'+String(m).padStart(2,'0')+'-'+String(d).padStart(2,'0');
    const q=qi+1,yy=String(yr).slice(-2);
    options.push({value:ds,label:q+'Q'+yy+' ('+ds+')'});
    qi--;if(qi<0){qi=3;yr--}}
  sel.innerHTML=options.map(o=>`<option value="${o.value}">${o.label}</option>`).join('');
  const cfgVal=C.max_date||'';
  if(cfgVal&&options.some(o=>o.value===cfgVal)){sel.value=cfgVal}
  else if(options.length){sel.value=options[0].value}}

/* ── Config ─────────────────────────────────── */
async function load(){const r=await fetch('/api/config');C=await r.json();renderMgrs();renderCfg();loadPresets()}
function renderMgrs(){
  const g=document.getElementById('mGrid');let h='',n=0;
  const entries=Object.entries(C.managers_13f||{});
  const wts=C.manager_weights||{};
  for(const[name,cik]of entries){n++;
    const clr=COLORS[n%COLORS.length];const w=wts[name]||'';
    h+=`<div class="mgr-card stagger" style="border-left-color:${clr};animation-delay:${n*30}ms">
      <div><div class="mgr-name">${E(name)} <span style="font-size:10px;color:var(--muted);font-weight:400">(13F)</span></div><div class="mgr-cik">CIK: ${E(cik)}</div></div>
      <div class="flex items-center gap-2">
        <input class="inp" style="width:60px;padding:3px 6px;font-size:12px;text-align:center" placeholder="Wt%" value="${w}" onchange="setWt('${A(name)}',this.value)" title="Portfolio weight %">
        <button class="btn-red" onclick="delMgr('${A(name)}')">&#10005;</button>
      </div></div>`}
  const nportEntries=Object.entries(C.managers_nport||{});
  for(const[name,info]of nportEntries){n++;
    const clr='#8b5cf6';const w=wts[name]||'';
    h+=`<div class="mgr-card stagger" style="border-left-color:${clr};animation-delay:${n*30}ms">
      <div><div class="mgr-name">${E(name)} <span style="font-size:10px;color:#8b5cf6;font-weight:400">(Fund)</span></div>
        <div class="mgr-cik">CIK: ${E(info.cik)} | Series: ${E(info.series_keyword)}</div></div>
      <div class="flex items-center gap-2">
        <input class="inp" style="width:60px;padding:3px 6px;font-size:12px;text-align:center" placeholder="Wt%" value="${w}" onchange="setWt('${A(name)}',this.value)" title="Portfolio weight %">
        <button class="btn-red" onclick="delNportMgr('${A(name)}')">&#10005;</button>
      </div></div>`}
  if(n===0){h=`<div style="grid-column:1/-1;text-align:center;padding:64px var(--sp-7)">
    <div style="font-size:48px;margin-bottom:var(--sp-5);opacity:0.25">&#128200;</div>
    <div style="font-size:var(--text-lg);font-weight:var(--weight-semibold);color:#fff;margin-bottom:var(--sp-2)">No managers added yet</div>
    <div style="font-size:var(--text-sm);color:var(--muted);max-width:400px;margin:0 auto;line-height:1.55">Search for investment managers or mutual funds above to start building your portfolio view. Try loading a preset to get started quickly.</div>
  </div>`}
  g.innerHTML=h;document.getElementById('mCnt').textContent=n+' managers';
  updateWeightBar()}
function updateWeightBar(){
  const wts=C.manager_weights||{};
  const allMgrs=[...Object.keys(C.managers_13f||{}),...Object.keys(C.managers_nport||{})];
  if(!allMgrs.length){document.getElementById('wtBarArea').classList.add('hidden');return}
  let total=0;for(const m of allMgrs){total+=parseFloat(wts[m])||0}
  const pct=Math.min(total,100);
  document.getElementById('wtBarArea').classList.remove('hidden');
  document.getElementById('wtFill').style.width=pct+'%';
  document.getElementById('wtFill').style.background=total>100?'linear-gradient(90deg,#ef4444,#dc2626)':total===100?'linear-gradient(90deg,#34d399,#10b981)':'linear-gradient(90deg,#5b8def,#8b6cf6)';
  document.getElementById('wtBarText').textContent=total.toFixed(1)+'% allocated';
  const remain=Math.max(0,100-total);
  document.getElementById('wtLabel').textContent=total>=100?'Fully allocated':remain.toFixed(1)+'% remaining';
  document.getElementById('wtLabel').style.color=total>100?'#ef4444':total===100?'#4ade80':'#94a3b8'}
function renderCfg(){
  document.getElementById('sTopN').value=C.top_n||20;
  populateQuarterSelect();
  document.getElementById('sIdent').value=C.identity||'';
  document.getElementById('sEnrich').checked=C.enrich_financial!==false}
async function saveCfg(){
  C.top_n=parseInt(document.getElementById('sTopN').value)||20;
  C.max_date=document.getElementById('sDate').value;
  C.identity=document.getElementById('sIdent').value;
  C.enrich_financial=document.getElementById('sEnrich').checked;
  await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(C)});
  toast('Settings saved')}

/* ── Weights ──────────────────────────────────── */
async function setWt(name,val){
  C.manager_weights=C.manager_weights||{};
  const v=parseFloat(val);
  if(isNaN(v)||v<=0)delete C.manager_weights[name];
  else C.manager_weights[name]=v;
  updateWeightBar();
  await fetch('/api/manager-weights',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({weights:C.manager_weights})})}
function clearMgrs(){
  showConfirm('Remove all managers and start from scratch?',async()=>{
    await fetch('/api/managers/clear',{method:'POST'});
    await load();toast('All managers cleared','success')
  },{title:'Clear All Managers',destructive:true,confirmText:'Clear All'})}
async function doStop(){await fetch('/api/stop',{method:'POST'});document.getElementById('stopBtn').classList.add('hidden')}

/* ── Presets ─────────────────────────────────── */
async function loadPresets(){
  const r=await fetch('/api/presets');const d=await r.json();
  const sel=document.getElementById('presetSel');
  sel.innerHTML='<option value="">— Presets —</option>';
  for(const p of d.presets||[]){
    sel.innerHTML+=`<option value="${E(p)}">${E(p)}</option>`}
  document.getElementById('delPresetBtn').classList.add('hidden')}
function savePreset(){
  const mgrCount=Object.keys(C.managers_13f||{}).length+Object.keys(C.managers_nport||{}).length;
  showPromptModal('Save Preset','e.g. Tech Giants Q4',async(name)=>{
    await fetch('/api/presets',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name})});
    toast('Preset "'+name+'" saved ('+mgrCount+' managers)','success');loadPresets()})}
async function loadPreset(){
  const sel=document.getElementById('presetSel');const name=sel.value;
  if(!name){document.getElementById('delPresetBtn').classList.add('hidden');return}
  document.getElementById('delPresetBtn').classList.remove('hidden');
  await fetch('/api/presets/'+encodeURIComponent(name)+'/load',{method:'POST'});
  await load();toast('Preset loaded: '+name)}
function deletePreset(){
  const sel=document.getElementById('presetSel');const name=sel.value;
  if(!name)return;
  showConfirm('Delete preset "'+name+'"? This cannot be undone.',async()=>{
    await fetch('/api/presets/'+encodeURIComponent(name),{method:'DELETE'});
    toast('Preset "'+name+'" deleted','danger');loadPresets()
  },{title:'Delete Preset',destructive:true,confirmText:'Delete'})}

/* ── Unified Typeahead ──────────────────────── */
const _taCache={};
const _TA_CACHE_TTL=900000;
function taInput(){
  const q=document.getElementById('aName').value.trim();
  if(q.length<2){taHide();return}
  const ql=q.toLowerCase(),c=_taCache[ql];
  if(c){taRes=c.results;taIdx=-1;taRender();
    if((Date.now()-c.ts)<_TA_CACHE_TTL)return;
    taFetch(q,true);return}
  clearTimeout(taTimer);taTimer=setTimeout(()=>taFetch(q,false),80)}
async function taFetch(q,background){
  try{const r=await fetch('/api/search-unified?q='+encodeURIComponent(q));
    const data=await r.json();
    _taCache[q.toLowerCase()]={results:data,ts:Date.now()};
    if(!background){taRes=data;taIdx=-1;taRender()}
    else{const cur=document.getElementById('aName').value.trim().toLowerCase();
      if(cur===q.toLowerCase()){taRes=data;taIdx=-1;taRender()}}
  }catch(e){if(!background)taHide()}}
function taRender(){
  const d=document.getElementById('taDd');
  if(!taRes.length){taHide();return}
  d.innerHTML=taRes.map((r,i)=>{
    const badge=r.type==='NPORT'?'<span style="color:#8b5cf6;font-size:10px;margin-left:6px">(Fund)</span>':'<span style="color:var(--muted);font-size:10px;margin-left:6px">(13F)</span>';
    const tkr=r.ticker?`<span style="color:#22d3ee;font-size:11px;margin-left:6px;font-weight:600">${E(r.ticker)}</span>`:'';
    const sub=r.ticker?`${E(r.ticker)} | CIK: ${E(r.cik)}`:`CIK: ${E(r.cik)}`;
    return `<div class="ta-item${i===taIdx?' sel':''}" onmousedown="taSel(${i})">
      <div><div class="n">${E(r.name)}${tkr}${badge}</div><div class="m">${sub}</div></div>
      <button class="btn-green btn-sm" onmousedown="event.stopPropagation();taAdd(${i})">+ Add</button>
    </div>`}).join('');
  d.classList.add('open')}
function taHide(){document.getElementById('taDd').classList.remove('open');taRes=[];taIdx=-1}
let pendingType='13F';
function taSel(i){const r=taRes[i];if(!r)return;
  document.getElementById('aName').value=r.name;
  document.getElementById('aCik').value=r.cik;
  pendingType=r.type||'13F';
  if(r.type==='NPORT'&&!r.series_keyword){showNportSeries(r.name,r.cik)}
  const ab=document.getElementById('addMgrBtn');ab.classList.remove('pulse');void ab.offsetWidth;ab.classList.add('pulse');
  taHide();window._lastSelResult=r}
async function taAdd(i){const r=taRes[i];if(!r)return;
  if(r.type==='NPORT'&&r.series_keyword){
    const displayName=r.ticker?(r.name+' ('+r.ticker+')'):r.name;
    await fetch('/api/managers-nport',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name:displayName,cik:r.cik,series_keyword:r.series_keyword})});
    taHide();document.getElementById('aName').value='';document.getElementById('aCik').value='';
    await load();toast('Fund added: '+displayName);return}
  if(r.type==='NPORT'){showNportSeries(r.name,r.cik);taHide();return}
  await fetch('/api/managers',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name:r.name,cik:r.cik})});
  taHide();document.getElementById('aName').value='';document.getElementById('aCik').value='';
  await load();toast('Added: '+r.name)}
function taKey(e){const d=document.getElementById('taDd');
  if(!d.classList.contains('open'))return;
  if(e.key==='ArrowDown'){e.preventDefault();taIdx=Math.min(taIdx+1,taRes.length-1);taRender()}
  else if(e.key==='ArrowUp'){e.preventDefault();taIdx=Math.max(taIdx-1,0);taRender()}
  else if(e.key==='Enter'&&taIdx>=0){e.preventDefault();taSel(taIdx)}
  else if(e.key==='Escape')taHide()}
document.addEventListener('click',e=>{
  if(!document.getElementById('taWrap').contains(e.target))taHide()});

/* ── N-PORT Series (inline) ──────────────────── */
function showNportSeries(name,cik){
  pendingNport={name,cik};
  document.getElementById('aName').value=name;
  document.getElementById('aCik').value=cik;
  document.getElementById('nportSeriesRow').classList.remove('hidden');
  loadNportSeries(cik)}
function cancelNport(){
  pendingNport=null;
  document.getElementById('nportSeriesRow').classList.add('hidden');
  document.getElementById('aName').value='';document.getElementById('aCik').value='';
  document.getElementById('nSeries').innerHTML='<option value="">— Select series —</option>';
  document.getElementById('nSeriesManual').value=''}
async function loadNportSeries(cik){
  const sel=document.getElementById('nSeries');
  sel.innerHTML='<option value="">Loading series...</option>';
  try{const r=await fetch('/api/nport-series/'+encodeURIComponent(cik));const d=await r.json();
    sel.innerHTML='<option value="">— Select series —</option>';
    for(const s of d.series||[]){sel.innerHTML+=`<option value="${E(s)}">${E(s)}</option>`}
    if(!(d.series||[]).length)sel.innerHTML='<option value="">No series found</option>'}
  catch(e){sel.innerHTML='<option value="">Error loading</option>'}}
async function addNportFromUnified(){
  if(!pendingNport)return;
  let kw=document.getElementById('nSeries').value;
  if(!kw)kw=document.getElementById('nSeriesManual').value.trim();
  if(!kw){showAlert('Missing Series','Select a series from the dropdown or enter a keyword to identify the fund.');return}
  await fetch('/api/managers-nport',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name:pendingNport.name,cik:pendingNport.cik,series_keyword:kw})});
  cancelNport();await load();toast('Fund added')}
async function delNportMgr(name){
  await fetch('/api/managers-nport',{method:'DELETE',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name})});await load();toast('Fund removed')}

/* ── CRUD ───────────────────────────────────── */
async function addMgr(){
  const name=document.getElementById('aName').value.trim(),cik=document.getElementById('aCik').value.trim();
  if(!name||!cik){showAlert('Required Fields','Both manager name and CIK number are required. Use the search box to find managers.');return}
  const lr=window._lastSelResult;
  if(pendingType==='NPORT'&&lr&&lr.series_keyword&&lr.cik===cik){
    const displayName=lr.ticker?(lr.name+' ('+lr.ticker+')'):lr.name;
    await fetch('/api/managers-nport',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name:displayName,cik:lr.cik,series_keyword:lr.series_keyword})});
    document.getElementById('aName').value='';document.getElementById('aCik').value='';
    pendingType='13F';window._lastSelResult=null;
    await load();toast('Fund added: '+displayName);return}
  if(pendingType==='NPORT'){showNportSeries(name,cik);return}
  await fetch('/api/managers',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name,cik})});
  document.getElementById('aName').value='';document.getElementById('aCik').value='';
  pendingType='13F';
  await load();toast('Manager added')}
async function delMgr(name){
  await fetch('/api/managers',{method:'DELETE',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name})});await load();toast('Manager removed')}

/* ── Run ────────────────────────────────────── */
async function doRun(){
  await saveCfg();const btn=document.getElementById('runBtn');
  btn.disabled=true;btn.innerHTML='&#9203; Running...';
  document.getElementById('stopBtn').classList.remove('hidden');
  document.getElementById('sBadge').textContent='Fetching...';
  document.getElementById('progressRow').style.display='';
  document.getElementById('pFill').style.width='0%';
  document.getElementById('pText').textContent='';
  document.getElementById('logBox').textContent='';
  document.getElementById('mgrStatus').innerHTML='';
  document.getElementById('resArea').classList.add('hidden');
  document.getElementById('dlBtn').classList.add('hidden');
  let r;
  try{r=await fetch('/api/run',{method:'POST'})}catch(e){
    btn.disabled=false;btn.innerHTML='&#9654; Fetch Holdings';
    document.getElementById('stopBtn').classList.add('hidden');
    document.getElementById('sBadge').textContent='Error';showAlert('Network Error','Could not reach the server: '+e.message);return}
  if(!r.ok){try{const e=await r.json();showAlert('Server Error',e.error)}catch(e){showAlert('Server Error','Server returned status '+r.status)}
    btn.disabled=false;btn.innerHTML='&#9654; Fetch Holdings';
    document.getElementById('stopBtn').classList.add('hidden');return}
  let run_id;try{const d=await r.json();run_id=d.run_id}catch(e){
    btn.disabled=false;btn.innerHTML='&#9654; Fetch Holdings';
    document.getElementById('stopBtn').classList.add('hidden');alert('Invalid server response');return}
  const es=new EventSource('/api/stream/'+run_id),log=document.getElementById('logBox');
  let sseErrorCount=0;
  es.onmessage=e=>{sseErrorCount=0;const m=JSON.parse(e.data);
    if(m.type==='log'){log.textContent+=m.message+'\n';log.scrollTop=log.scrollHeight}
    else if(m.type==='progress'){const p=Math.round(m.done/m.total*100);
      document.getElementById('pFill').style.width=p+'%';
      document.getElementById('pText').textContent=`${m.done}/${m.total} managers`;
      document.getElementById('sBadge').textContent=m.done+'/'+m.total}
    else if(m.type==='manager_start'){addMgrChip(m.name,'running')}
    else if(m.type==='manager_done'){
      updateMgrChip(m.name,m.status==='success'?'ok':'err',m.status==='success'?m.positions+' pos':m.error)}
    else if(m.type==='enrich_start'){
      document.getElementById('pText').textContent='Enriching 0/'+m.total+' tickers...';
      document.getElementById('pFill').style.width='0%'}
    else if(m.type==='enrich_progress'){
      const p=Math.round(m.done/m.total*100);
      document.getElementById('pFill').style.width=p+'%';
      document.getElementById('pText').textContent=`Enriching ${m.done}/${m.total} tickers...`}
    else if(m.type==='complete'){es.close();btn.disabled=false;btn.innerHTML='&#9654; Fetch Holdings';
      document.getElementById('stopBtn').classList.add('hidden');
      const aborted=m.aborted;
      document.getElementById('sBadge').textContent=aborted?'Stopped':'Complete';
      document.getElementById('pFill').style.width='100%';
      document.getElementById('pText').textContent=aborted?'Stopped by user':'Complete';
      if(m.results&&m.results.managers&&m.results.managers.length){showRes(m.results);loadAllData()}}};
  es.onerror=()=>{sseErrorCount++;
    if(sseErrorCount>=3){es.close();btn.disabled=false;btn.innerHTML='&#9654; Fetch Holdings';
      document.getElementById('stopBtn').classList.add('hidden');
      document.getElementById('sBadge').textContent='Error'}}}

function addMgrChip(name,cls){
  const s=document.getElementById('mgrStatus');
  const id='chip-'+name.replace(/[^a-zA-Z0-9]/g,'_');
  s.innerHTML+=`<div class="mgr-chip ${cls}" id="${id}">${cls==='running'?'<span class="spinner"></span>':''}${E(name)}</div>`}
function updateMgrChip(name,cls,info){
  const id='chip-'+name.replace(/[^a-zA-Z0-9]/g,'_');
  const el=document.getElementById(id);
  if(el){el.className='mgr-chip '+cls;
    el.innerHTML=(cls==='ok'?'&#10003; ':'&#10007; ')+E(name)+(info?' ('+E(String(info))+')':'')}}

/* ── Results ────────────────────────────────── */
function showRes(r){
  document.getElementById('resArea').classList.remove('hidden');
  const ok=r.managers.filter(m=>m.status==='success').length,
        bad=r.managers.filter(m=>m.status==='error').length;
  document.getElementById('resSummary').innerHTML=
    `<span class="text-green-400 font-semibold">${ok} succeeded</span>`+
    (bad?` <span class="text-red-400 font-semibold ml-2">${bad} failed</span>`:'');
  document.getElementById('csvLinks').innerHTML=(r.files||[]).map(f=>
    `<a href="/files/${f}" download class="text-blue-400 hover:text-blue-300 text-sm underline">&#128196; ${f}</a>`).join('');
  if((r.files||[]).length){document.getElementById('dlBtn').classList.remove('hidden')}
  document.getElementById('resetBtn').classList.remove('hidden')}
function dlAll(){window.location.href='/api/download-all'}
function doReset(){
  showConfirm('Clear all results and start over?',async()=>{
    await fetch('/api/reset',{method:'POST'});
    // Purge Plotly charts before clearing DOM
    document.querySelectorAll('#storyArea .js-plotly-plot').forEach(el=>{try{Plotly.purge(el)}catch(e){}});
    document.getElementById('resArea').classList.add('hidden');
    document.getElementById('dlBtn').classList.add('hidden');
    document.getElementById('resetBtn').classList.add('hidden');
    document.getElementById('progressRow').style.display='none';
    document.getElementById('pFill').style.width='0%';
    document.getElementById('pText').textContent='';
    document.getElementById('logBox').textContent='Waiting to start...';
    document.getElementById('mgrStatus').innerHTML='';
    document.getElementById('sBadge').textContent='Ready';
    document.getElementById('csvLinks').innerHTML='';
    // Clear story area and posture card
    const story=document.getElementById('storyArea');
    if(story)story.innerHTML='';
    const posture=document.getElementById('postureCard');
    if(posture){posture.style.display='none';posture.classList.remove('visible')}
    _xlinkTickers=new Set();
    pendingNport=null;pendingType='13F';
    document.getElementById('nportSeriesRow').classList.add('hidden');
    toast('Reset complete')
  },{title:'Reset Results',destructive:true,confirmText:'Reset'})}

/* ── Story Area (interleaved layout) ─────── */
function initStoryArea(){
  const area=document.getElementById('storyArea');
  area.innerHTML='';
  for(const sec of STORY_SECTIONS){
    const div=document.createElement('div');
    div.id='story-'+sec.id;
    div.className='story-section mb-5';
    if(sec.type==='chart'){
      const chartId=sec.id==='bubble'?'bubbleChart':sec.id;
      let inner=`<div class="card p-5">`;
      if(sec.id==='bubble'){
        inner+=`<div class="flex items-center justify-between mb-3">
          <h2 class="text-lg font-semibold text-white">${E(sec.title)}</h2>
          <div id="bubbleMgrBtns" class="flex gap-2 flex-wrap"></div>
        </div>
        <div id="bubbleChart" style="height:500px"></div>`}
      else{
        inner+=`<div id="${chartId}" style="height:400px"></div>`}
      inner+=`</div>`;
      div.innerHTML=inner}
    else if(sec.type==='holdings'){
      div.innerHTML=`<div class="card p-5">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-semibold text-white">Holdings</h2>
          <div class="flex gap-2">
            <button id="top10Btn" class="btn btn-sm btn-blue" onclick="setTopN(10)">Top 10</button>
            <button id="top20Btn" class="btn btn-sm btn-gray" onclick="setTopN(20)">Top 20</button>
          </div>
        </div>
        <div id="tablesArea"><div class="build-message"><span class="build-spinner"></span>Building your investment story...</div></div>
      </div>`}
    else{
      div.innerHTML=`<div class="card p-6">
        <div id="${sec.id}-content"><div class="build-message"><span class="build-spinner"></span>Building your investment story...</div></div>
      </div>`}
    area.appendChild(div)}}

/* ── Portfolio Posture Card ────────────────── */
async function loadPostureCard(){
  try{
    const r=await fetch('/api/summary-data');if(!r.ok)return;
    const d=await r.json();if(d.error)return;
    const fwdPe=d.weighted_forward_pe;
    const epsGr=d.weighted_eps_growth;
    const qtdRet=d.weighted_return?.qtd_weighted_return;
    const beatRate=d.eps_beat_rate;
    // Determine tilt
    let tilt='Balanced';
    if(fwdPe!=null&&epsGr!=null){
      if(epsGr>15&&fwdPe>25)tilt='Growth-Tilted';
      else if(epsGr>12&&fwdPe<=25)tilt='GARP';
      else if(fwdPe<18&&epsGr<10)tilt='Value-Oriented';
      else if(fwdPe>22&&epsGr<10)tilt='Quality / Momentum'}
    // Sentiment score 0-3
    let sentScore=0;
    if(qtdRet!=null&&qtdRet>0)sentScore++;
    if(epsGr!=null&&epsGr>10)sentScore++;
    if(beatRate!=null&&beatRate>=65)sentScore++;
    let sentLabel,sentCls;
    if(sentScore>=2){sentLabel='Constructive';sentCls='constructive'}
    else if(sentScore===1){sentLabel='Neutral';sentCls='neutral'}
    else{sentLabel='Cautious';sentCls='cautious'}
    // Render
    document.getElementById('postureTilt').textContent=tilt;
    document.getElementById('postureSentiment').className='posture-sentiment '+sentCls;
    document.getElementById('postureSentiment').textContent=sentLabel;
    const retColor=qtdRet!=null?(qtdRet>=0?'var(--success)':'var(--danger)'):'#fff';
    document.getElementById('postureStats').innerHTML=`
      <div class="posture-stat"><div class="posture-stat-label">Fwd P/E</div><div class="posture-stat-value">${fwdPe!=null?fwdPe.toFixed(1)+'x':'—'}</div></div>
      <div class="posture-stat"><div class="posture-stat-label">EPS Growth</div><div class="posture-stat-value">${epsGr!=null?(epsGr>0?'+':'')+epsGr.toFixed(1)+'%':'—'}</div></div>
      <div class="posture-stat"><div class="posture-stat-label">QTD Return</div><div class="posture-stat-value" style="color:${retColor}">${qtdRet!=null?(qtdRet>0?'+':'')+qtdRet.toFixed(1)+'%':'—'}</div></div>
      <div class="posture-stat"><div class="posture-stat-label">EPS Beat Rate</div><div class="posture-stat-value">${beatRate!=null?beatRate.toFixed(0)+'%':'—'}</div></div>`;
    const card=document.getElementById('postureCard');
    card.style.display='';
    requestAnimationFrame(()=>card.classList.add('visible'));
  }catch(e){}}

/* ── IntersectionObserver for story sections ── */
function initIntersectionObserver(){
  const sections=document.querySelectorAll('.story-section');
  if(!sections.length)return;
  const obs=new IntersectionObserver((entries)=>{
    for(const entry of entries){
      if(entry.isIntersecting){
        entry.target.classList.add('visible');
        obs.unobserve(entry.target)}}
  },{threshold:0.08});
  sections.forEach(s=>obs.observe(s))}

/* ── Cross-link helpers ────────────────────── */
function applyXlinks(text,meta){
  if(!meta)return text;
  let out=text;
  // Wrap tickers from meta
  if(meta.tickers&&meta.tickers.length){
    for(const tk of meta.tickers){
      if(!tk)continue;
      const re=new RegExp('\\b'+tk.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'\\b','g');
      out=out.replace(re,`<span class="xlink" data-type="ticker" data-value="${E(tk)}">${E(tk)}</span>`)}}
  // Wrap known portfolio tickers via regex (skip inside HTML tags)
  out=out.replace(/(<[^>]*>)|\b([A-Z]{1,5})\b/g,(match,tag,ticker)=>{
    if(tag)return tag;
    if(ticker&&_xlinkTickers.has(ticker)&&!out.includes('data-value="'+ticker+'"'))
      return `<span class="xlink" data-type="ticker" data-value="${E(ticker)}">${E(ticker)}</span>`;
    return match});
  // Wrap sectors from meta
  if(meta.sectors&&meta.sectors.length){
    for(const sec of meta.sectors){
      if(!sec)continue;
      const re=new RegExp(sec.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'),'g');
      out=out.replace(re,`<span class="xlink" data-type="sector" data-value="${E(sec)}">${E(sec)}</span>`)}}
  return out}

function handleXlinkClick(el){
  const type=el.dataset.type,value=el.dataset.value;
  if(type==='ticker'){
    const rows=document.querySelectorAll('.holdings-tbl tbody tr');
    for(const row of rows){
      const tickerCell=row.querySelector('.ticker');
      if(tickerCell&&tickerCell.textContent.trim()===value){
        row.scrollIntoView({behavior:'smooth',block:'center'});
        row.classList.add('xlink-highlight');
        setTimeout(()=>row.classList.remove('xlink-highlight'),1800);
        return}}}
  if(type==='sector'){
    const chartEl=document.getElementById('story-sectorBar');
    if(chartEl)chartEl.scrollIntoView({behavior:'smooth',block:'center'});
    highlightBarChartItem('sectorBar',value)}}

function highlightBarChartItem(elId,itemName){
  const el=document.getElementById(elId);
  if(!el||!el.data||!el.data[0])return;
  const labels=el.data[0].y;
  const idx=labels.indexOf(itemName);
  if(idx<0)return;
  const origColors=[...el.data[0].marker.color];
  const highlight=[...origColors];
  highlight[idx]='#fbbf24';
  Plotly.restyle(elId,{'marker.color':[highlight]},0);
  setTimeout(()=>Plotly.restyle(elId,{'marker.color':[origColors]},0),1500)}

/* ── Load All Data (after fetch or history load) ── */
async function loadAllData(){
  initStoryArea();
  loadPostureCard();
  // Fetch analysis + tables in parallel for cross-linking
  const [analysisRes,tableRes]=await Promise.allSettled([
    fetch('/api/written-analysis').then(r=>r.ok?r.json():null),
    fetch('/api/portfolio-table?top_n='+displayTopN).then(r=>r.ok?r.json():null)
  ]);
  const analysisData=analysisRes.status==='fulfilled'?analysisRes.value:null;
  const tableData=tableRes.status==='fulfilled'?tableRes.value:null;
  // Build xlink ticker set from table data
  _xlinkTickers=new Set();
  if(tableData){
    const allRows=[...(tableData.weighted?.rows||[])];
    if(tableData.managers){for(const data of Object.values(tableData.managers)){allRows.push(...(data.rows||[]))}}
    for(const r of allRows){if(r.ticker&&r.ticker!=='—')_xlinkTickers.add(r.ticker)}}
  // Render analysis with xlinks
  renderAnalysisStory(analysisData);
  // Render tables
  renderTablesStory(tableData);
  // Load charts independently
  loadBubbleChart();
  loadCharts();
  setTimeout(initIntersectionObserver,150)}

/* ── Render Analysis into Story Sections ──── */
function renderAnalysisStory(d){
  if(!d||d.error){
    const el=document.getElementById('highlights-content');
    if(el)el.innerHTML='<p class="text-slate-400">'+(d&&d.error?E(d.error):'Analysis unavailable.')+'</p>';
    return}
  // Key Highlights with xlinks
  const hlEl=document.getElementById('highlights-content');
  if(hlEl&&d.highlights&&d.highlights.length){
    let h='<h3 class="analysis-title" style="margin-bottom:var(--sp-3)">Key Highlights</h3><div class="highlights-wrap">';
    for(const hl of d.highlights){
      const body=applyXlinks(E(hl.body),hl.meta||{});
      h+=`<div class="highlight-card">
        <h4 class="highlight-title">${E(hl.title)}</h4>
        <p class="highlight-body">${body}</p>
      </div>`}
    h+='</div>';
    hlEl.innerHTML=h}
  // Individual analysis sections
  const sectionKeys=['overview','geography','sectors','valuation','earnings','risks'];
  for(const s of sectionKeys){
    const el=document.getElementById(s+'-content');
    if(!el)continue;
    if(!d[s]){el.innerHTML='';continue}
    const titles={'overview':'Portfolio Overview','sectors':'Sector Positioning','geography':'Geographic Exposure',
      'valuation':'Valuation & Growth','earnings':'Earnings Surprises','risks':'Key Risks'};
    // Use empty meta for written sections (xlinks rely on portfolio tickers)
    const text=applyXlinks(E(d[s]),{tickers:[],sectors:[]});
    el.innerHTML=`<h3 class="analysis-title">${titles[s]||s}</h3><p class="analysis-text">${text}</p>`}}

/* ── Render Tables into Story Section ─────── */
function renderTablesStory(d){
  const el=document.getElementById('tablesArea');
  if(!el)return;
  if(!d||d.error){el.innerHTML='<p class="text-slate-400">'+(d&&d.error?E(d.error):'Table data unavailable.')+'</p>';return}
  let h='';
  if(d.weighted){h+=renderTable('Weighted Portfolio',d.weighted.rows,d.weighted.totals)}
  if(d.managers){
    for(const[mgr,data]of Object.entries(d.managers)){
      h+=renderTable(mgr,data.rows,data.totals)}}
  h+='<div class="table-footnote">'
    +'<strong>Methodology:</strong> '
    +'Fwd EPS Growth: Analyst consensus next-fiscal-year EPS growth (source: Yahoo Finance growth_estimates). '
    +'Portfolio-level growth rates winsorized at \u00b150% per stock before weighting. '
    +'Fwd P/E: Forward price/earnings (source: Yahoo Finance forwardPE = currentPrice / forwardEps). '
    +'Portfolio P/E is weighted harmonic mean; stocks with negative values are excluded. '
    +'QTD Return: Weighted arithmetic mean. Monthly returns computed from yfinance historical prices. '
    +'All totals computed from full holdings, not just displayed rows.'
    +'</div>';
  el.innerHTML=h||'<p class="text-slate-400">No table data.</p>'}

async function loadTables(topN){
  const r=await fetch('/api/portfolio-table?top_n='+topN);
  if(!r.ok)return;
  const d=await r.json();
  // Rebuild xlink tickers
  _xlinkTickers=new Set();
  const allRows=[...(d.weighted?.rows||[])];
  if(d.managers){for(const data of Object.values(d.managers)){allRows.push(...(data.rows||[]))}}
  for(const row of allRows){if(row.ticker&&row.ticker!=='—')_xlinkTickers.add(row.ticker)}
  renderTablesStory(d)}

function renderTable(title,rows,totals){
  // Detect monthly return columns from first row
  const monthCols=(rows.length&&rows[0].monthly_returns)?rows[0].monthly_returns.map(m=>m.month):[];
  let h=`<div class="holdings-table-wrap">
    <h3 class="holdings-table-title">${E(title)}</h3>
    <div class="table-scroll"><table class="holdings-tbl"><thead><tr>
      <th>% of Port</th><th>Stock</th><th>Ticker</th><th>Sector</th><th>Industry</th>
      <th>Qtr End Price</th><th>Current Price</th><th>QTD Return</th>`;
  for(const mc of monthCols) h+=`<th>${E(mc)}</th>`;
  h+=`<th>Fwd P/E</th><th>Fwd EPS Gr</th><th>Rpt EPS</th><th>EPS Beat</th>
    </tr></thead><tbody>`;
  for(const r of rows){
    const retCls=r.qtd_return!=null?(r.qtd_return>=0?'ret-pos':'ret-neg'):'';
    const beatInfo=formatBeat(r.eps_beat_dollars,r.eps_beat_pct);
    h+=`<tr>
      <td class="mono">${r.pct!=null?r.pct.toFixed(2)+'%':'—'}</td>
      <td class="stock-name">${E(r.name||'—')}</td>
      <td class="ticker">${E(r.ticker||'—')}</td>
      <td class="sector-col">${E(r.sector||'—')}</td>
      <td class="industry-col">${E(r.industry||'—')}</td>
      <td class="mono">${fmtPrice(r.filing_price)}</td>
      <td class="mono">${fmtPrice(r.current_price)}</td>
      <td class="mono ${retCls}">${fmtRet(r.qtd_return)}</td>`;
    if(r.monthly_returns){for(const m of r.monthly_returns){
      const mc=m.return_pct!=null?(m.return_pct>=0?'ret-pos':'ret-neg'):'';
      h+=`<td class="mono ${mc}">${fmtRet(m.return_pct)}</td>`}}
    else{for(let i=0;i<monthCols.length;i++) h+='<td class="mono">—</td>'}
    h+=`<td class="mono">${r.forward_pe!=null?r.forward_pe.toFixed(1)+'x':'—'}</td>
      <td class="mono ${r.forward_eps_growth!=null?(r.forward_eps_growth>=0?'ret-pos':'ret-neg'):''}">${r.forward_eps_growth!=null?(r.forward_eps_growth>0?'+':'')+r.forward_eps_growth.toFixed(1)+'%':'—'}</td>
      <td class="mono">${r.filing_reported_eps!=null?'$'+r.filing_reported_eps.toFixed(2):'—'}</td>
      <td class="mono">${beatInfo}</td>
    </tr>`}
  // Totals row
  if(totals){
    h+=`<tr class="totals-row">
      <td class="mono">100.0%</td><td colspan="6" style="font-weight:600;color:#fff">Portfolio Totals (all holdings)</td>
      <td class="mono ${totals.qtd_return!=null?(totals.qtd_return>=0?'ret-pos':'ret-neg'):''}">${fmtRet(totals.qtd_return)}</td>`;
    if(totals.monthly_returns){for(const m of totals.monthly_returns){
      const mc=m.return_pct!=null?(m.return_pct>=0?'ret-pos':'ret-neg'):'';
      h+=`<td class="mono ${mc}">${fmtRet(m.return_pct)}</td>`}}
    else{for(let i=0;i<monthCols.length;i++) h+='<td class="mono">—</td>'}
    h+=`<td class="mono">${totals.forward_pe!=null?totals.forward_pe.toFixed(1)+'x':'—'}</td>
      <td class="mono">${totals.forward_eps_growth!=null?(totals.forward_eps_growth>0?'+':'')+totals.forward_eps_growth.toFixed(1)+'%':'—'}</td>
      <td colspan="2"></td>
    </tr>`}
  h+='</tbody></table></div></div>';
  return h}

function formatBeat(dollars,pct){
  if(dollars==null&&pct==null)return '—';
  if(pct==null)return '—';
  const absPct=Math.abs(pct);
  if(pct>1)return `<span class="beat-pos">&#9650; +${absPct.toFixed(1)}%</span>`;
  if(pct<-1)return `<span class="beat-neg">&#9660; -${absPct.toFixed(1)}%</span>`;
  return `<span class="beat-met">&#9654; ${absPct.toFixed(1)}%</span>`}
function fmtPrice(v){return v!=null?'$'+v.toFixed(2):'—'}
function fmtRet(v){return v!=null?(v>0?'+':'')+v.toFixed(1)+'%':'—'}

/* ── Top N Toggle ──────────────────────────── */
function setTopN(n){
  displayTopN=n;
  document.getElementById('top10Btn').className='btn btn-sm '+(n===10?'btn-blue':'btn-gray');
  document.getElementById('top20Btn').className='btn btn-sm '+(n===20?'btn-blue':'btn-gray');
  loadTables(n)}

/* ── Bubble Chart ──────────────────────────── */
let _bubbleActiveMgr='';
async function loadBubbleChart(manager){
  _bubbleActiveMgr=manager||'';
  const el=document.getElementById('bubbleChart');
  try{
    const url='/api/bubble-data'+(manager?'?manager='+encodeURIComponent(manager):'');
    const r=await fetch(url);if(!r.ok)return;
    const d=await r.json();
    if(!d.stocks||!d.stocks.length){Plotly.purge(el);return}
    // Manager toggle buttons
    const btnEl=document.getElementById('bubbleMgrBtns');
    const mgrs=d.managers||[];
    let bh='<button class="btn btn-sm '+(manager?'btn-gray':'btn-blue')+'" onclick="loadBubbleChart()">Weighted Portfolio</button>';
    for(const m of mgrs){
      bh+=`<button class="btn btn-sm ${manager===m?'btn-blue':'btn-gray'}" onclick="loadBubbleChart('${A(m)}')">${E(m)}</button>`}
    btnEl.innerHTML=bh;
    // Build scatter
    const stocks=d.stocks;
    const x=stocks.map(s=>s.forward_eps_growth);
    const y=stocks.map(s=>s.forward_pe);
    const sz=stocks.map(s=>Math.max(s.pct*4,8));
    const txt=stocks.map(s=>s.ticker||s.short_name);
    const hoverTxt=stocks.map(s=>`<b>${s.ticker}</b><br>${s.short_name}<br>Fwd P/E: ${s.forward_pe.toFixed(1)}x<br>EPS Gr: ${s.forward_eps_growth>0?'+':''}${s.forward_eps_growth.toFixed(1)}%<br>Weight: ${s.pct.toFixed(2)}%`);
    const traces=[{
      x,y,mode:'markers+text',type:'scatter',
      marker:{size:sz,color:stocks.map((_,i)=>COLORS[i%COLORS.length]),opacity:0.8,
              line:{width:1,color:'rgba(255,255,255,0.3)'}},
      text:txt,textposition:'top center',textfont:{size:9,color:'#e2e8f0'},
      hovertext:hoverTxt,hoverinfo:'text',
    }];
    // Portfolio average diamond
    const avg=d.portfolio_avg;
    if(avg&&avg.forward_pe!=null&&avg.eps_growth!=null){
      traces.push({
        x:[avg.eps_growth],y:[avg.forward_pe],mode:'markers+text',type:'scatter',
        marker:{size:16,color:'#fbbf24',symbol:'diamond',line:{width:2,color:'#fff'}},
        text:['Portfolio'],textposition:'bottom center',textfont:{size:10,color:'#fbbf24',weight:'bold'},
        hovertext:[`<b>Portfolio Avg</b><br>Fwd P/E: ${avg.forward_pe.toFixed(1)}x<br>EPS Gr: ${avg.eps_growth>0?'+':''}${avg.eps_growth.toFixed(1)}%`],
        hoverinfo:'text',showlegend:false,
      })}
    Plotly.newPlot(el,traces,{
      title:{text:'Forward P/E vs EPS Growth'+(manager?' — '+manager:''),font:{color:'#fff',size:16},y:0.97},
      paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
      xaxis:{title:{text:'Fwd EPS Growth (%)',font:{color:'#94a3b8',size:12}},
             color:'#94a3b8',tickfont:{size:10},gridcolor:'rgba(51,65,85,.4)',zeroline:true,zerolinecolor:'rgba(100,116,139,.5)'},
      yaxis:{title:{text:'Forward P/E (x)',font:{color:'#94a3b8',size:12}},
             color:'#94a3b8',tickfont:{size:10},gridcolor:'rgba(51,65,85,.4)'},
      margin:{t:50,b:50,l:60,r:20},showlegend:false,
      hovermode:'closest',
    },{responsive:true});
  }catch(e){}}

/* ── Bar Charts ────────────────────────────── */
async function loadCharts(){
  const [sectorRes,geoRes]=await Promise.allSettled([
    fetch('/api/sector-data').then(r=>r.json()),
    fetch('/api/geo-data').then(r=>r.json())
  ]);
  const sd=sectorRes.status==='fulfilled'?sectorRes.value:null;
  const gd=geoRes.status==='fulfilled'?geoRes.value:null;
  if(sd&&sd.sectors&&sd.sectors.length)renderSimpleBar('sectorBar',sd.sectors.slice(0,10),'Sector Allocation');
  if(gd&&gd.countries&&gd.countries.length)renderSimpleBar('geoBar',gd.countries.slice(0,10),'Geographic Allocation');
  if(sd&&sd.industries&&sd.industries.length)renderSimpleBar('industryBar',sd.industries.slice(0,12),'Industry Allocation')}

function renderSimpleBar(elId,items,title){
  const labels=items.map(s=>s.name).reverse();
  const vals=items.map(s=>s.pct).reverse();
  const maxVal=Math.max(...vals,1);
  const barColors=items.map((_,i)=>COLORS[i%COLORS.length]).reverse();
  const customdata=items.map(s=>s.stocks_detail||[]).reverse();
  // Build hover text with stock detail
  const hoverText=items.map(s=>{
    let h=`<b>${s.name}</b>: ${s.pct.toFixed(1)}%`;
    if(s.stocks_detail&&s.stocks_detail.length){
      h+='<br><br>Top holdings:';
      for(const sd of s.stocks_detail.slice(0,5)){
        h+=`<br>  ${sd.name}: ${sd.pct.toFixed(1)}%`}}
    return h}).reverse();
  Plotly.newPlot(elId,[{
    y:labels,x:vals,type:'bar',orientation:'h',
    marker:{color:barColors},
    text:vals.map(v=>v.toFixed(1)+'%'),textposition:'outside',
    textfont:{color:'#e2e8f0',size:10},
    customdata:customdata,
    hovertext:hoverText,hoverinfo:'text',
  }],{
    title:{text:title,font:{color:'#fff',size:18},y:0.96,pad:{b:15}},
    paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
    xaxis:{color:'#94a3b8',tickfont:{size:10},gridcolor:'rgba(51,65,85,.4)',
      title:{text:'Weight (%)',font:{color:'#94a3b8',size:11}},range:[0,maxVal*1.2]},
    yaxis:{color:'#e2e8f0',tickfont:{size:10},automargin:true},
    margin:{t:75,b:40,l:145,r:30},bargap:0.2
  },{responsive:true});
  // Click-to-pin popup
  const plotEl=document.getElementById(elId);
  plotEl.on('plotly_click',function(data){
    dismissBarPopup();
    const pt=data.points[0];if(!pt)return;
    const detail=pt.customdata;
    if(!detail||!detail.length)return;
    const catName=pt.y;
    const catPct=pt.x;
    let ph=`<div class="bar-popup-title">${E(catName)} (${catPct.toFixed(1)}%)</div><div class="bar-popup-list">`;
    for(const s of detail){
      const mgrs=s.managers&&s.managers.length?s.managers.join(', '):'';
      ph+=`<div class="bar-popup-stock"><span class="bar-popup-name">${E(s.name)}</span><span class="bar-popup-pct">${s.pct.toFixed(1)}%</span></div>`;
      if(mgrs) ph+=`<div class="bar-popup-mgrs">${E(mgrs)}</div>`}
    ph+='</div>';
    const popup=document.createElement('div');
    popup.className='bar-popup';
    popup.innerHTML=ph;
    // Position near click
    const rect=plotEl.getBoundingClientRect();
    const px=data.event.clientX-rect.left+10;
    const py=data.event.clientY-rect.top-20;
    popup.style.left=Math.min(px,rect.width-260)+'px';
    popup.style.top=Math.max(py,10)+'px';
    plotEl.style.position='relative';
    plotEl.appendChild(popup);
    popup._barPopupActive=true})}

/* ── Bar Popup Dismiss ─────────────────────── */
function dismissBarPopup(){
  document.querySelectorAll('.bar-popup').forEach(p=>p.remove())}
document.addEventListener('click',e=>{
  if(!e.target.closest('.bar-popup')&&!e.target.closest('.js-plotly-plot')){dismissBarPopup()}});

/* ── Modal System ──────────────────────────── */
function showModal(opts){
  return new Promise(resolve=>{
    // Remove any existing modal
    document.querySelectorAll('.modal-overlay').forEach(m=>m.remove());
    const overlay=document.createElement('div');
    overlay.className='modal-overlay';
    const panel=document.createElement('div');
    panel.className='modal-panel';
    let h='';
    if(opts.icon)h+=`<div class="modal-icon">${opts.icon}</div>`;
    if(opts.title)h+=`<div class="modal-title">${E(opts.title)}</div>`;
    if(opts.desc)h+=`<div class="modal-desc">${E(opts.desc)}</div>`;
    if(opts.type==='prompt')h+=`<input class="inp modal-input" placeholder="${E(opts.placeholder||'')}" autofocus>`;
    h+='<div class="modal-actions">';
    if(opts.type==='alert'){
      h+=`<button class="btn btn-blue modal-ok">OK</button>`}
    else{
      h+=`<button class="btn btn-gray modal-cancel">${E(opts.cancelText||'Cancel')}</button>`;
      const confirmCls=opts.destructive?'btn-red':'btn-blue';
      h+=`<button class="btn ${confirmCls} modal-confirm">${E(opts.confirmText||'Confirm')}</button>`}
    h+='</div>';
    panel.innerHTML=h;
    overlay.appendChild(panel);
    document.body.appendChild(overlay);
    const close=(val)=>{
      overlay.style.transition='opacity .2s';overlay.style.opacity='0';
      setTimeout(()=>overlay.remove(),200);resolve(val)};
    // Focus management
    const inp=panel.querySelector('.modal-input');
    const confirmBtn=panel.querySelector('.modal-confirm')||panel.querySelector('.modal-ok');
    if(inp)setTimeout(()=>inp.focus(),50);
    else if(confirmBtn)setTimeout(()=>confirmBtn.focus(),50);
    // Event handlers
    overlay.addEventListener('click',e=>{if(e.target===overlay)close(opts.type==='prompt'?null:false)});
    panel.addEventListener('keydown',e=>{
      if(e.key==='Escape')close(opts.type==='prompt'?null:false);
      if(e.key==='Enter'&&opts.type==='prompt'){close(inp.value)}
      if(e.key==='Enter'&&opts.type==='confirm'){close(true)}});
    const okBtn=panel.querySelector('.modal-ok');
    if(okBtn)okBtn.onclick=()=>close(true);
    const cancelBtn=panel.querySelector('.modal-cancel');
    if(cancelBtn)cancelBtn.onclick=()=>close(opts.type==='prompt'?null:false);
    if(panel.querySelector('.modal-confirm'))
      panel.querySelector('.modal-confirm').onclick=()=>close(opts.type==='prompt'?inp.value:true)})}

function showConfirm(desc,onConfirm,opts={}){
  showModal({type:'confirm',title:opts.title||'Confirm',desc,icon:opts.destructive?'&#9888;&#65039;':'&#10067;',
    destructive:opts.destructive,confirmText:opts.confirmText||(opts.destructive?'Delete':'Confirm'),
    cancelText:opts.cancelText||'Cancel'}).then(ok=>{if(ok)onConfirm()})}

function showPromptModal(title,placeholder,onConfirm){
  showModal({type:'prompt',title,placeholder,icon:'&#9999;&#65039;'}).then(val=>{if(val!=null&&val.trim())onConfirm(val.trim())})}

function showAlert(title,message){
  showModal({type:'alert',title,desc:message,icon:'&#9432;'})}

/* ── Toast (class-based) ──────────────────── */
function toast(m,type){
  document.querySelectorAll('.toast-wrap').forEach(t=>t.remove());
  const t=document.createElement('div');
  t.className='toast-wrap'+(type==='success'?' toast-success':type==='danger'?' toast-danger':'');
  const icon=type==='success'?'&#10003; ':type==='danger'?'&#9888; ':'';
  t.innerHTML=icon+E(m);
  document.body.appendChild(t);
  setTimeout(()=>{t.style.transition='all .3s cubic-bezier(0.4,0,0.2,1)';t.style.opacity='0';t.style.transform='translateY(8px)';setTimeout(()=>t.remove(),300)},2500)}
function E(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;')}
function A(s){return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'")}


/* ── Init ── */
load();
window.addEventListener('resize',()=>{document.querySelectorAll('.js-plotly-plot').forEach(el=>{Plotly.Plots.resize(el)})});
// Xlink event delegation
document.addEventListener('click',e=>{
  const xl=e.target.closest('.xlink');
  if(xl){e.preventDefault();handleXlinkClick(xl)}});
