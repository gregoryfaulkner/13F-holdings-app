let C={},taTimer=null,taRes=[],taIdx=-1,pendingNport=null;
const COLORS=['#5b8def','#8b6cf6','#ec4899','#f59e0b','#34d399','#f87171','#22d3ee','#84cc16','#f97316','#6366f1','#14b8a6','#e879f9','#a78bfa','#a3e635'];

/* ── Shared Chart Helpers ────────────────────── */
function qrColor(qr,fallback){
  if(qr==null)return fallback;
  if(qr===0)return'hsl(220,10%,35%)';
  return qr>0?`hsl(${Math.min(120,qr*6)},70%,35%)`:`hsl(0,${Math.min(80,Math.abs(qr)*4)}%,35%)`}

function plotTreemap(elId,d,opts){
  const o=opts||{};
  const trace={type:'treemap',ids:d.ids,labels:d.labels,parents:d.parents,values:d.values,
    marker:{colors:d.colors},hoverinfo:'text',hovertext:d.hovers,
    textinfo:'label',textfont:{color:'#fff',size:o.textSize||11},
    branchvalues:'total',maxdepth:3,tiling:{packing:'squarify'}};
  if(o.textTemplate)trace.texttemplate=o.textTemplate;
  Plotly.newPlot(elId,[trace],
    {paper_bgcolor:'#0c1220',margin:{t:10,b:10,l:10,r:10},font:{color:'#e2e8f0'}},{responsive:true});
  document.getElementById(elId).on('plotly_click',function(ev){
    if(ev.points&&ev.points[0]){const m=(ev.points[0].label||'').match(/\(([A-Z.\-]{1,6})\)$/);if(m)openStockPanel(m[1])}})}

function plotCompBar(elId,cfg){
  const {labels,portVals,acwiVals,hasAcwi,title,titleNoAcwi,precision,leftMargin,diffThreshold,onClick}=cfg;
  const p=precision||1;
  const traces=[];
  if(hasAcwi){traces.push({y:labels,x:acwiVals,type:'bar',orientation:'h',name:'ACWI',
    marker:{color:'#F59E0B'},text:acwiVals.map(v=>v>0?v.toFixed(p)+'%':''),textposition:'outside',
    textfont:{color:'#FCD34D',size:10},hovertemplate:'%{y}: %{x:.'+p+'f}%<extra>ACWI</extra>'})}
  traces.push({y:labels,x:portVals,type:'bar',orientation:'h',name:'Portfolio',
    marker:{color:'#3B82F6'},text:portVals.map(v=>v.toFixed(p)+'%'),textposition:'outside',
    textfont:{color:'#93C5FD',size:10},hovertemplate:'%{y}: %{x:.'+p+'f}%<extra>Portfolio</extra>'});
  const maxVal=Math.max(...portVals,...(hasAcwi?acwiVals:[]),1);
  const annotations=[];
  if(hasAcwi){labels.forEach((l,i)=>{
    const diff=portVals[i]-(acwiVals[i]||0);
    if(Math.abs(diff)>=(diffThreshold||0.3)){
      annotations.push({x:maxVal*1.22,y:l,text:(diff>0?'+':'')+diff.toFixed(p),
        showarrow:false,font:{color:diff>0?'#4ADE80':'#F87171',size:9},xanchor:'left'})}})}
  Plotly.newPlot(elId,traces,{
    title:{text:hasAcwi?title:(titleNoAcwi||title),font:{color:'#fff',size:18},y:0.96,pad:{b:15}},
    barmode:'group',paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
    xaxis:{color:'#94a3b8',tickfont:{size:10},gridcolor:'rgba(51,65,85,.4)',title:{text:'Weight (%)',font:{color:'#94a3b8',size:11}},
      range:[0,maxVal*(hasAcwi?1.35:1.2)]},
    yaxis:{color:'#e2e8f0',tickfont:{size:10},automargin:true},
    legend:{font:{color:'#94a3b8',size:11},bgcolor:'transparent',x:1,xanchor:'right',y:1.02,orientation:'h'},
    margin:{t:75,b:40,l:leftMargin||145,r:hasAcwi?55:20},annotations,
    bargap:0.15,bargroupgap:0.08},{responsive:true});
  if(onClick)document.getElementById(elId).on('plotly_click',onClick)}

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
  /* Most recent completed quarter end */
  const qEnds=[[3,31],[6,30],[9,30],[12,31]];
  let options=[];
  for(let offset=0;offset<8;offset++){
    /* Walk backwards from current quarter */
    const d=new Date(now.getFullYear(),now.getMonth()-offset*3,1);
    /* Find the quarter end <= d */
    for(let qi=3;qi>=0;qi--){
      const qm=qEnds[qi][0]-1;/* JS month 0-based */
      const qd=qEnds[qi][1];
      const yr=d.getFullYear()-(qm>d.getMonth()?1:0);
      const candidate=new Date(yr,qm,qd);
      if(candidate<=now&&!options.some(o=>o.value===candidate.toISOString().slice(0,10))){
        const q=qi+1;const yy=String(yr).slice(-2);
        options.push({value:candidate.toISOString().slice(0,10),label:q+'Q'+yy+' ('+candidate.toISOString().slice(0,10)+')'});
        break}}}
  /* Deduplicate and sort descending */
  const seen=new Set();
  options=options.filter(o=>{if(seen.has(o.value))return false;seen.add(o.value);return true});
  options.sort((a,b)=>b.value.localeCompare(a.value));
  /* Take 8 most recent */
  options=options.slice(0,8);
  sel.innerHTML=options.map(o=>`<option value="${o.value}">${o.label}</option>`).join('');
  /* Set value from config or default to first (most recent completed quarter) */
  const cfgVal=C.max_date||'';
  if(cfgVal&&options.some(o=>o.value===cfgVal)){sel.value=cfgVal}
  else if(options.length){sel.value=options[0].value}}

/* ── Config ─────────────────────────────────── */
async function load(){const r=await fetch('/api/config');C=await r.json();renderMgrs();renderCfg();loadPresets();updateDiffDates()}
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
  const remain=Math.max(0,100-total);
  document.getElementById('wtBarArea').classList.remove('hidden');
  document.getElementById('wtFill').style.width=pct+'%';
  document.getElementById('wtFill').style.background=total>100?'linear-gradient(90deg,#ef4444,#dc2626)':total===100?'linear-gradient(90deg,#34d399,#10b981)':'linear-gradient(90deg,#5b8def,#8b6cf6)';
  document.getElementById('wtBarText').textContent=total.toFixed(1)+'% allocated';
  document.getElementById('wtLabel').textContent=total>=100?'Fully allocated':remain.toFixed(1)+'% remaining';
  document.getElementById('wtLabel').style.color=total>100?'#ef4444':total===100?'#4ade80':'#94a3b8'}
function renderCfg(){
  document.getElementById('sClient').value=C.client_name||'';
  document.getElementById('sReport').value=C.report_name||'';
  document.getElementById('sTopN').value=C.top_n||20;
  populateQuarterSelect();
  document.getElementById('sIdent').value=C.identity||'';
  document.getElementById('sEnrich').checked=C.enrich_financial!==false}
async function saveCfg(){
  C.client_name=document.getElementById('sClient').value;
  C.report_name=document.getElementById('sReport').value;
  C.top_n=parseInt(document.getElementById('sTopN').value)||20;
  C.max_date=document.getElementById('sDate').value;
  C.identity=document.getElementById('sIdent').value;
  C.enrich_financial=document.getElementById('sEnrich').checked;
  await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(C)});
  updateDiffDates();toast('Settings saved')}

/* ── Weights, Clear, PDF ──────────────────────── */
async function setWt(name,val){
  C.manager_weights=C.manager_weights||{};
  const v=parseFloat(val);
  if(isNaN(v)||v<=0)delete C.manager_weights[name];
  else C.manager_weights[name]=v;
  updateWeightBar();
  await fetch('/api/manager-weights',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({weights:C.manager_weights})})}
async function clearMgrs(){
  if(!confirm('Remove all managers and start from scratch?'))return;
  await fetch('/api/managers/clear',{method:'POST'});
  await load();toast('All managers cleared')}
function updateDiffDates(){
  const maxDate=C.max_date||'2025-12-31';
  const priorDate=priorQEnd(maxDate);
  document.getElementById('diffDate2').value=maxDate;
  document.getElementById('diffDate1').value=priorDate;
  document.getElementById('diffLabel1').textContent='Period 1 — '+qLabel(priorDate);
  document.getElementById('diffLabel2').textContent='Period 2 — '+qLabel(maxDate)+' (Filing Date)'}
function dlPdf(){window.location.href='/api/download-pdf'}
async function doStop(){await fetch('/api/stop',{method:'POST'});document.getElementById('stopBtn').classList.add('hidden')}

/* ── Presets ─────────────────────────────────── */
async function loadPresets(){
  const r=await fetch('/api/presets');const d=await r.json();
  const sel=document.getElementById('presetSel');
  sel.innerHTML='<option value="">— Presets —</option>';
  for(const p of d.presets||[]){
    sel.innerHTML+=`<option value="${E(p)}">${E(p)}</option>`}}
async function savePreset(){
  const name=prompt('Preset name:');if(!name)return;
  await fetch('/api/presets',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name})});
  toast('Preset saved');loadPresets()}
async function loadPreset(){
  const sel=document.getElementById('presetSel');const name=sel.value;
  if(!name)return;
  await fetch('/api/presets/'+encodeURIComponent(name)+'/load',{method:'POST'});
  await load();toast('Preset loaded: '+name)}

/* ── Unified Typeahead ──────────────────────── */
const _taCache={};  /* Client-side search cache: query→{results,ts} */
const _TA_CACHE_TTL=900000; /* 15 minutes */
function taInput(){
  const q=document.getElementById('aName').value.trim();
  if(q.length<2){taHide();return}
  const ql=q.toLowerCase(),c=_taCache[ql];
  /* Stale-while-revalidate: show cached results instantly, refresh in background */
  if(c){taRes=c.results;taIdx=-1;taRender();
    if((Date.now()-c.ts)<_TA_CACHE_TTL)return;
    /* Cache is stale — revalidate in background (don't block UI) */
    taFetch(q,true);return}
  clearTimeout(taTimer);taTimer=setTimeout(()=>taFetch(q,false),80)}
async function taFetch(q,background){
  try{const r=await fetch('/api/search-unified?q='+encodeURIComponent(q));
    const data=await r.json();
    _taCache[q.toLowerCase()]={results:data,ts:Date.now()};
    if(!background){taRes=data;taIdx=-1;taRender()}
    else{/* Update cache silently; if user still on same query, refresh */
      const cur=document.getElementById('aName').value.trim().toLowerCase();
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
  taHide();
  /* Store the full result for direct-add */
  window._lastSelResult=r}
async function taAdd(i){const r=taRes[i];if(!r)return;
  if(r.type==='NPORT'&&r.series_keyword){
    /* Fund already identified — add directly, skip series picker */
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
  if(!kw){alert('Select a series or enter a keyword');return}
  await fetch('/api/managers-nport',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name:pendingNport.name,cik:pendingNport.cik,series_keyword:kw})});
  cancelNport();await load();toast('Fund added')}
async function delNportMgr(name){
  await fetch('/api/managers-nport',{method:'DELETE',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name})});await load();toast('Fund removed')}

/* ── CRUD ───────────────────────────────────── */
async function addMgr(){
  const name=document.getElementById('aName').value.trim(),cik=document.getElementById('aCik').value.trim();
  if(!name||!cik){alert('Name and CIK are required');return}
  /* If we have a pre-identified fund from search, add directly */
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
    document.getElementById('sBadge').textContent='Error';alert('Network error: '+e.message);return}
  if(!r.ok){try{const e=await r.json();alert(e.error)}catch(e){alert('Server error ('+r.status+')')}
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
    else if(m.type==='manager_start'){
      addMgrChip(m.name,'running')}
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
      if(m.results&&m.results.managers&&m.results.managers.length){showRes(m.results);buildHeroInsight();loadTM();loadHeatmap();loadSummary();loadOverlap();loadSectors();loadSectorTreemap('sector');loadGeoTreemap();loadHistory()}}};
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
  if((r.files||[]).length){document.getElementById('dlBtn').classList.remove('hidden');document.getElementById('pdfBtn').classList.remove('hidden')}
  document.getElementById('resetBtn').classList.remove('hidden')}
function dlAll(){window.location.href='/api/download-all'}
async function doReset(){
  if(!confirm('Clear all results and start over?'))return;
  await fetch('/api/reset',{method:'POST'});
  document.getElementById('resArea').classList.add('hidden');
  document.getElementById('summaryArea').classList.add('hidden');
  document.getElementById('heroInsight').classList.add('hidden');
  document.getElementById('dlBtn').classList.add('hidden');
  document.getElementById('pdfBtn').classList.add('hidden');
  document.getElementById('resetBtn').classList.add('hidden');
  document.getElementById('acwiSource').classList.add('hidden');
  document.getElementById('progressRow').style.display='none';
  document.getElementById('pFill').style.width='0%';
  document.getElementById('pText').textContent='';
  document.getElementById('logBox').textContent='Waiting to start...';
  document.getElementById('mgrStatus').innerHTML='';
  document.getElementById('sBadge').textContent='Ready';
  document.getElementById('tmBox').innerHTML='';
  document.getElementById('tmTitle').innerHTML='';
  document.getElementById('overlapBox').innerHTML='';
  document.getElementById('sectorTreemapBox').innerHTML='';
  document.getElementById('geoTreemapBox').innerHTML='';
  document.getElementById('diffBox').innerHTML='';
  document.getElementById('csvLinks').innerHTML='';
  try{Plotly.purge('sectorCompBar');Plotly.purge('geoCompBar');Plotly.purge('topHoldingsCompBar');Plotly.purge('industryBar');Plotly.purge('sectorSunburst');Plotly.purge('sectorByMgr');Plotly.purge('valuationBubble');Plotly.purge('overlapBubbleMatrix')}catch(e){}
  closeCategoryDetail();
  document.querySelectorAll('.chart-insight,.summary-findings').forEach(el=>{el.classList.add('hidden');el.innerHTML=''});
  document.getElementById('heatmapBox').innerHTML='';
  _spotData=null;closeStockPanel();
  last_db_run_id=null;pendingNport=null;pendingType='13F';
  document.getElementById('nportSeriesRow').classList.add('hidden');
  toast('Reset complete')}

/* ── Portfolio Story Hero ──────────────────── */
async function buildHeroInsight(){
  try{
    const r=await fetch('/api/summary-data');const d=await r.json();
    if(!d.total_holdings)return;
    const hero=document.getElementById('heroInsight');hero.classList.remove('hidden');
    // Build narrative headline
    let topSector='diversified sectors',topSectorPct='';
    try{const sr=await fetch('/api/sector-data');const sd=await sr.json();if(sd.sectors&&sd.sectors.length){topSector=sd.sectors[0].name;topSectorPct=sd.sectors[0].pct.toFixed(0)}}catch(e){}
    const mgrCount=d.unique_managers||0;
    document.getElementById('heroHeadline').textContent=mgrCount+' managers, '+d.unique_stocks+' unique positions, led by '+topSector+(topSectorPct?' at '+topSectorPct+'%':'');
    // Subtext
    let sub=[];
    if(d.most_common_stock)sub.push(d.most_common_stock.ticker+' is the most widely held name ('+d.most_common_stock.manager_count+' managers)');
    if(d.weighted_return&&d.weighted_return.filing_qtr_weighted_return!=null){const ret=d.weighted_return.filing_qtr_weighted_return;sub.push('Weighted portfolio returned '+(ret>0?'+':'')+ret.toFixed(1)+'% last quarter')}
    if(d.weighted_return&&d.weighted_return.qtd_weighted_return!=null){const qret=d.weighted_return.qtd_weighted_return;sub.push('Since filing, portfolio returned '+(qret>0?'+':'')+qret.toFixed(1)+'% QTD')}
    document.getElementById('heroSubtext').textContent=sub.join('. ')+(sub.length?'.':'');
    // Metrics
    let mh='';
    const mt=(v,l)=>`<div style="min-width:120px"><div style="font-size:var(--text-2xl);font-weight:var(--weight-bold);color:#fff">${v}</div><div style="font-size:var(--text-xs);color:var(--muted);text-transform:uppercase;letter-spacing:var(--tracking-wide);margin-top:4px">${l}</div></div>`;
    mh+=mt(d.total_holdings.toLocaleString(),'Total Holdings');
    mh+=mt(d.unique_stocks||'-','Unique Stocks');
    if(d.avg_quarter_return!=null)mh+=mt((d.avg_quarter_return>0?'+':'')+d.avg_quarter_return.toFixed(1)+'%','Avg Return');
    if(d.eps_beat_rate!=null)mh+=mt(d.eps_beat_rate.toFixed(0)+'%','EPS Beat Rate');
    document.getElementById('heroMetrics').innerHTML=mh;
  }catch(e){}}

/* ── Summary Dashboard ──────────────────────── */
async function loadSummary(){
  try{
    document.getElementById('summaryArea').classList.remove('hidden');
    if(window._resetScrollReveal) window._resetScrollReveal();
    const sg=document.getElementById('statGrid');
    sg.innerHTML=Array(12).fill(0).map(()=>'<div class="stat-box"><div class="skeleton skel-bar" style="width:60%;margin:8px auto"></div><div class="skeleton skel-bar" style="width:80%;height:10px;margin:4px auto"></div></div>').join('');
    showSkeletonBox('sectorCompBar',380);showSkeletonBox('geoCompBar',380);showSkeletonBox('topHoldingsCompBar',380);showSkeletonBox('industryBar',340);showSkeletonBox('valuationBubble',460);
    /* Parallel fetch: summary + sectors + ACWI + geo + valuation */
    const [summaryRes,sectorRes,acwiRes,geoRes,valRes]=await Promise.allSettled([
      fetch('/api/summary-data').then(r=>r.json()),
      fetch('/api/sector-data').then(r=>r.json()),
      fetch('/api/acwi-benchmark').then(r=>r.json()),
      fetch('/api/geo-data').then(r=>r.json()),
      fetch('/api/valuation-scatter').then(r=>r.json())
    ]);
    const d=summaryRes.status==='fulfilled'?summaryRes.value:null;
    const sd=sectorRes.status==='fulfilled'?sectorRes.value:null;
    const acwi=acwiRes.status==='fulfilled'&&!acwiRes.value.error?acwiRes.value:null;
    const gd=geoRes.status==='fulfilled'&&!geoRes.value.error?geoRes.value:null;
    const vd=valRes.status==='fulfilled'&&!valRes.value.error?valRes.value:null;
    if(!d||!d.total_holdings){document.getElementById('summaryArea').classList.add('hidden');return}
    const wr=d.weighted_return||{};
    const fQ=d.filing_quarter||'Filing';const pQ=d.prior_quarter||'Prior';
    sg.innerHTML=`
      <div class="stat-box fade"><div class="stat-val" id="sv-total">0</div><div class="stat-label">Total Holdings</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-unique">0</div><div class="stat-label">Unique Stocks</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-mgrs">0</div><div class="stat-label">Managers</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-avgret" data-pct="1">${d.avg_quarter_return!=null?'0%':'N/A'}</div><div class="stat-label">Avg ${fQ} Return</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-wretf" data-pct="1" data-signed="1">${wr.filing_qtr_weighted_return!=null?'0%':'N/A'}</div><div class="stat-label">Wtd ${fQ} Return</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-wretp" data-pct="1" data-signed="1">${wr.prior_qtr_weighted_return!=null?'0%':'N/A'}</div><div class="stat-label">Wtd ${pQ} Return</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-wretq" data-pct="1" data-signed="1">${wr.qtd_weighted_return!=null?'0%':'N/A'}</div><div class="stat-label">Wtd QTD Return</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-beat" data-pct="1">${d.eps_beat_rate!=null?'0%':'N/A'}</div><div class="stat-label">EPS Beat Rate</div></div>
      <div class="stat-box fade"><div class="stat-val">${d.most_common_stock?E(d.most_common_stock.ticker):'—'}</div>
        <div class="stat-label">Most Held (${d.most_common_stock?d.most_common_stock.manager_count:0} mgrs)</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-fpe" data-suffix="x">${d.weighted_forward_pe!=null?'0':'N/A'}</div><div class="stat-label">Fwd P/E (Wtd)</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-epsg" data-pct="1">${d.weighted_eps_growth!=null?'0%':'N/A'}</div><div class="stat-label">Fwd EPS Growth</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-divy" data-pct="1">${d.weighted_div_yield!=null?'0%':'N/A'}</div><div class="stat-label">Dividend Yield</div></div>
      <div class="stat-box fade"><div class="stat-val" id="sv-expret" data-pct="1">${d.expected_return!=null?'0%':'N/A'}</div><div class="stat-label">Expected Return</div></div>`;
    animateValue(document.getElementById('sv-total'),d.total_holdings,600);
    animateValue(document.getElementById('sv-unique'),d.unique_stocks,600);
    animateValue(document.getElementById('sv-mgrs'),d.unique_managers,400);
    if(d.avg_quarter_return!=null)animateValue(document.getElementById('sv-avgret'),d.avg_quarter_return,800);
    if(wr.filing_qtr_weighted_return!=null)animateValue(document.getElementById('sv-wretf'),wr.filing_qtr_weighted_return,800);
    if(wr.prior_qtr_weighted_return!=null)animateValue(document.getElementById('sv-wretp'),wr.prior_qtr_weighted_return,800);
    if(wr.qtd_weighted_return!=null)animateValue(document.getElementById('sv-wretq'),wr.qtd_weighted_return,800);
    if(d.eps_beat_rate!=null)animateValue(document.getElementById('sv-beat'),d.eps_beat_rate,800);
    if(d.weighted_forward_pe!=null)animateValue(document.getElementById('sv-fpe'),d.weighted_forward_pe,800);
    if(d.weighted_eps_growth!=null)animateValue(document.getElementById('sv-epsg'),d.weighted_eps_growth,800);
    if(d.weighted_div_yield!=null)animateValue(document.getElementById('sv-divy'),d.weighted_div_yield,800);
    if(d.expected_return!=null)animateValue(document.getElementById('sv-expret'),d.expected_return,800);
    /* Render comparison charts */
    renderCompBar('sector',sd,acwi);
    renderCompBar('geo',gd,acwi);
    renderCompBar('holdings',d,acwi);
    renderIndustryBar(sd);
    renderValuationBubble(vd);
    /* Key takeaway insights above each chart */
    generateSectorInsight(sd,acwi);
    generateGeoInsight(gd,acwi);
    generateHoldingsInsight(d);
    generateIndustryInsight(sd);
    generateValuationInsight(vd);
    generateSummaryFindings(d,sd,gd,acwi,vd);
    if(acwi&&acwi.source){
      document.getElementById('acwiSourceLabel').textContent=acwi.source;
      document.getElementById('acwiSource').classList.remove('hidden');
    }
    /* Trigger scroll reveal after all summary content rendered */
    if(window._initScrollReveal) setTimeout(window._initScrollReveal,100);
  }catch(e){console.error('loadSummary:',e)}}

/* ── Comparison Bar Configs & Unified Renderer ─ */
const COMP_BAR_CONFIGS={
  sector:{elId:'sectorCompBar',dataKey:'normalized_sectors',acwiKey:'sectors',nameKey:'name',
    title:'Sector Allocation — Portfolio vs ACWI',titleNoAcwi:'Sector Allocation',
    precision:1,leftMargin:145,diffThreshold:0.3,catType:'sector',topN:10},
  holdings:{elId:'topHoldingsCompBar',acwiKey:'top_holdings',
    title:'Top Holdings — Portfolio vs ACWI',titleNoAcwi:'Top 10 Holdings by Portfolio %',
    precision:2,leftMargin:160,diffThreshold:0.05,topN:10},
  geo:{elId:'geoCompBar',dataKey:'normalized_countries',acwiKey:'countries',nameKey:'name',
    title:'Geographic Allocation — Portfolio vs ACWI',titleNoAcwi:'Geographic Allocation',
    precision:1,leftMargin:130,diffThreshold:0.3,catType:'country',topN:10}
};
function renderCompBar(key,data,acwi){
  const c=COMP_BAR_CONFIGS[key];if(!c)return;
  if(key==='holdings'){
    const tsp=data.top_stocks_by_pct||[];if(!tsp.length)return;
    const items=tsp.slice(0,c.topN);
    const labels=items.map(s=>(s.short_name||s.name)+' ('+s.ticker+')').reverse();
    const portVals=items.map(s=>s.pct).reverse();
    const hasAcwi=acwi&&acwi.top_holdings;
    let acwiLookup={};
    if(hasAcwi)acwi.top_holdings.forEach(h=>{acwiLookup[h.ticker]=h.weight});
    plotCompBar(c.elId,{labels,portVals,hasAcwi,
      acwiVals:hasAcwi?items.map(s=>acwiLookup[s.ticker]||0).reverse():[],
      title:c.title,titleNoAcwi:c.titleNoAcwi,precision:c.precision,leftMargin:c.leftMargin,diffThreshold:c.diffThreshold,
      onClick:function(d){if(d.points&&d.points[0]){const m=(d.points[0].y||'').match(/\(([A-Z.\-]{1,6})\)$/);if(m)openStockPanel(m[1])}}});
    return}
  const arr=data&&data[c.dataKey];if(!arr||arr.length<1)return;
  const items=arr.slice(0,c.topN);
  const labels=items.map(s=>s[c.nameKey]).reverse();
  const portVals=items.map(s=>s.pct).reverse();
  const hasAcwi=acwi&&acwi[c.acwiKey];
  plotCompBar(c.elId,{labels,portVals,hasAcwi,
    acwiVals:hasAcwi?labels.map(l=>acwi[c.acwiKey][l]||0):[],
    title:c.title,titleNoAcwi:c.titleNoAcwi,precision:c.precision,leftMargin:c.leftMargin,diffThreshold:c.diffThreshold,
    onClick:function(d){if(d.points&&d.points[0])showCategoryDetail(c.elId,c.catType,d.points[0].y)}})}

/* ── Industry Bar (portfolio-only, no ACWI data) ─ */
function renderIndustryBar(sd){
  if(!sd||!sd.industries||sd.industries.length<1)return;
  const items=sd.industries.slice(0,12);
  const fullNames=items.map(s=>s.name).reverse();
  const labels=items.map(s=>s.name.length>30?s.name.slice(0,28)+'…':s.name).reverse();
  const vals=items.map(s=>s.pct).reverse();
  const maxVal=Math.max(...vals,1);
  Plotly.newPlot('industryBar',[{y:labels,x:vals,type:'bar',orientation:'h',
    customdata:fullNames,
    marker:{color:items.map((_,i)=>COLORS[i%COLORS.length]).reverse()},
    text:vals.map(v=>v.toFixed(1)+'%'),textposition:'outside',
    textfont:{color:'#e2e8f0',size:10},hovertemplate:'%{customdata}: %{x:.1f}%<extra></extra>'}],{
    title:{text:'Industry Allocation',font:{color:'#fff',size:18},y:0.96,pad:{b:15}},
    paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
    xaxis:{color:'#94a3b8',tickfont:{size:10},gridcolor:'rgba(51,65,85,.4)',title:{text:'Weight (%)',font:{color:'#94a3b8',size:11}},
      range:[0,maxVal*1.2]},
    yaxis:{color:'#e2e8f0',tickfont:{size:10},automargin:true},
    margin:{t:75,b:40,l:180,r:30},bargap:0.2},{responsive:true});
  document.getElementById('industryBar').on('plotly_click',function(data){
    if(data.points&&data.points[0]){const fn=data.points[0].customdata||data.points[0].y;showCategoryDetail('industryBar','industry',fn)}})}

/* ── Valuation Bubble Scatter (Fwd P/E vs EPS Growth) ─ */
function renderValuationBubble(vd){
  if(!vd||!vd.stocks||vd.stocks.length<3){document.getElementById('valuationBubble').innerHTML='';return}
  const stocks=vd.stocks;const avg=vd.portfolio_avg||{};
  const SECTOR_COLORS={'Technology':'#3B82F6','Communication Services':'#8B5CF6','Healthcare':'#10B981',
    'Financial Services':'#F59E0B','Consumer Cyclical':'#EF4444','Industrials':'#6366F1',
    'Consumer Defensive':'#14B8A6','Energy':'#F97316','Basic Materials':'#A855F7',
    'Real Estate':'#EC4899','Utilities':'#06B6D4'};
  /* Build traces by sector for legend */
  const bySector={};
  stocks.forEach(s=>{const sec=s.sector||'Other';if(!bySector[sec])bySector[sec]=[];bySector[sec].push(s)});
  const traces=[];
  const maxPct=Math.max(...stocks.map(s=>s.pct));
  const sizeRef=2.0*maxPct/(40*40);
  Object.keys(bySector).forEach(sec=>{
    const arr=bySector[sec];
    traces.push({x:arr.map(s=>s.forward_pe),y:arr.map(s=>s.forward_eps_growth),
      text:arr.map(s=>s.ticker),
      customdata:arr.map(s=>[s.name,s.pct,s.dividend_yield,s.sector]),
      mode:'markers+text',type:'scatter',name:sec,
      marker:{size:arr.map(s=>s.pct),sizemode:'area',sizeref:sizeRef,sizemin:6,
        color:SECTOR_COLORS[sec]||'#64748B',opacity:0.8,
        line:{width:1,color:'rgba(255,255,255,0.3)'}},
      textposition:'top center',textfont:{color:'#94a3b8',size:9},
      hovertemplate:'<b>%{text}</b><br>%{customdata[0]}<br>Fwd P/E: %{x:.1f}x<br>EPS Growth: %{y:.1f}%<br>Weight: %{customdata[1]:.2f}%<br>Div Yield: %{customdata[2]}%<br>Sector: %{customdata[3]}<extra></extra>'})});
  /* OLS regression line */
  const xArr=stocks.map(s=>s.forward_pe),yArr=stocks.map(s=>s.forward_eps_growth);
  const n=xArr.length;
  const xMean=xArr.reduce((a,b)=>a+b,0)/n,yMean=yArr.reduce((a,b)=>a+b,0)/n;
  let num=0,den=0;
  for(let i=0;i<n;i++){num+=(xArr[i]-xMean)*(yArr[i]-yMean);den+=(xArr[i]-xMean)*(xArr[i]-xMean)}
  const slope=den>0?num/den:0;const intercept=yMean-slope*xMean;
  const xMin=Math.min(...xArr),xMax=Math.max(...xArr);
  const xPad=(xMax-xMin)*0.05;
  traces.push({x:[xMin-xPad,xMax+xPad],y:[slope*(xMin-xPad)+intercept,slope*(xMax+xPad)+intercept],
    mode:'lines',type:'scatter',name:'OLS Fit',
    line:{color:'rgba(148,163,184,0.5)',width:2,dash:'dot'},
    hoverinfo:'skip',showlegend:true});
  /* Portfolio average marker (green square) */
  if(avg.forward_pe!=null&&avg.eps_growth!=null){
    traces.push({x:[avg.forward_pe],y:[avg.eps_growth],mode:'markers+text',type:'scatter',
      name:'Portfolio Avg',text:['Portfolio'],textposition:'bottom center',
      textfont:{color:'#22C55E',size:11,weight:'bold'},
      marker:{symbol:'square',size:16,color:'#22C55E',
        line:{width:2,color:'#fff'}},
      hovertemplate:'<b>Portfolio Average</b><br>Fwd P/E: '+avg.forward_pe.toFixed(1)+'x<br>EPS Growth: '+avg.eps_growth.toFixed(1)+'%<br>Div Yield: '+(avg.div_yield!=null?avg.div_yield.toFixed(1)+'%':'N/A')+'<br>Expected Return: '+(avg.expected_return!=null?avg.expected_return.toFixed(1)+'%':'N/A')+'<extra></extra>'})}
  Plotly.newPlot('valuationBubble',traces,{
    title:{text:'Valuation vs Growth \u2014 Top '+stocks.length+' Holdings',font:{color:'#fff',size:18},y:0.96,pad:{b:15}},
    paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
    xaxis:{title:{text:'Forward P/E Ratio',font:{color:'#94a3b8',size:12}},color:'#94a3b8',tickfont:{size:10},
      gridcolor:'rgba(51,65,85,.4)',zeroline:false},
    yaxis:{title:{text:'Forward EPS Growth (%)',font:{color:'#94a3b8',size:12}},color:'#94a3b8',tickfont:{size:10},
      gridcolor:'rgba(51,65,85,.4)',zeroline:true,zerolinecolor:'rgba(51,65,85,.6)'},
    legend:{font:{color:'#94a3b8',size:10},bgcolor:'transparent',x:1,xanchor:'right',y:1,orientation:'v'},
    margin:{t:75,b:60,l:60,r:20},hovermode:'closest'},{responsive:true});
  document.getElementById('valuationBubble').on('plotly_click',function(data){
    if(data.points&&data.points[0]&&data.points[0].text){openStockPanel(data.points[0].text)}})}

/* ── Key Takeaway Insight Generators ─────── */
function _setInsight(id,html){
  const el=document.getElementById(id);
  if(!el)return;if(!html){el.classList.add('hidden');el.innerHTML='';return}
  el.innerHTML=html;el.classList.remove('hidden')}

function _buildInsight(id,data,guard,buildFn){
  if(!guard(data)){_setInsight(id,'');return}
  const items=buildFn(data);
  _setInsight(id,items.slice(0,3).join(''))}

function generateSectorInsight(sd,acwi){
  _buildInsight('sectorCompBarInsight',sd,d=>d&&d.normalized_sectors&&d.normalized_sectors.length,function(d){
    const sectors=d.normalized_sectors.slice(0,10);const items=[];const top=sectors[0];
    items.push('<div class="ci-item"><strong>'+E(top.name)+' Dominant:</strong> Largest allocation at '+top.pct.toFixed(1)+'% of portfolio.</div>');
    if(acwi&&acwi.sectors){
      let maxOW=null,maxOWdiff=0,maxUW=null,maxUWdiff=0;
      sectors.forEach(s=>{const bm=acwi.sectors[s.name]||0;const diff=s.pct-bm;
        if(diff>maxOWdiff){maxOWdiff=diff;maxOW=s}
        if(diff<maxUWdiff){maxUWdiff=diff;maxUW=s}});
      if(maxOW&&maxOWdiff>=1)items.push('<div class="ci-item"><strong>Overweight '+E(maxOW.name)+':</strong> +'+maxOWdiff.toFixed(1)+'% vs ACWI benchmark ('+maxOW.pct.toFixed(1)+'% vs '+(acwi.sectors[maxOW.name]||0).toFixed(1)+'%).</div>');
      if(maxUW&&Math.abs(maxUWdiff)>=1)items.push('<div class="ci-item"><strong>Underweight '+E(maxUW.name)+':</strong> '+maxUWdiff.toFixed(1)+'% vs ACWI benchmark.</div>')}
    return items})}

function generateGeoInsight(gd,acwi){
  _buildInsight('geoCompBarInsight',gd,d=>d&&d.normalized_countries&&d.normalized_countries.length,function(d){
    const countries=d.normalized_countries.slice(0,10);const items=[];
    const us=countries.find(c=>c.name==='United States');
    if(us){
      let usNote='<strong>US Allocation:</strong> '+us.pct.toFixed(1)+'% domestic exposure';
      if(acwi&&acwi.countries){const bmUS=acwi.countries['United States']||0;if(bmUS>0)usNote+=' vs '+bmUS.toFixed(1)+'% ACWI'}
      items.push('<div class="ci-item">'+usNote+'.</div>')}
    const nonUS=countries.filter(c=>c.name!=='United States');
    if(nonUS.length){const intlPct=nonUS.reduce((s,c)=>s+c.pct,0);
      items.push('<div class="ci-item"><strong>International:</strong> '+intlPct.toFixed(1)+'% across '+nonUS.length+' countries, led by '+E(nonUS[0].name)+' ('+nonUS[0].pct.toFixed(1)+'%).</div>')}
    return items})}

function generateHoldingsInsight(d){
  _buildInsight('topHoldingsInsight',d,d=>d&&d.top_stocks_by_pct&&d.top_stocks_by_pct.length,function(d){
    const stocks=d.top_stocks_by_pct;const items=[];
    const top5pct=stocks.slice(0,5).reduce((s,st)=>s+st.pct,0);
    items.push('<div class="ci-item"><strong>Top 5 Concentration:</strong> '+top5pct.toFixed(1)+'% of portfolio in just 5 names.</div>');
    if(d.most_common_stock)items.push('<div class="ci-item"><strong>Consensus Pick:</strong> '+E(d.most_common_stock.ticker)+' held by '+d.most_common_stock.manager_count+' managers.</div>');
    const topStock=stocks[0];
    if(topStock)items.push('<div class="ci-item"><strong>Largest Position:</strong> '+E(topStock.ticker)+' at '+topStock.pct.toFixed(2)+'% weighted allocation.</div>');
    return items})}

function generateIndustryInsight(sd){
  _buildInsight('industryBarInsight',sd,d=>d&&d.industries&&d.industries.length,function(d){
    const ind=d.industries;const items=[];
    items.push('<div class="ci-item"><strong>'+E(ind[0].name)+' Leads:</strong> Top industry at '+ind[0].pct.toFixed(1)+'% of portfolio.</div>');
    items.push('<div class="ci-item"><strong>Breadth:</strong> '+ind.length+' industries represented across the combined portfolio.</div>');
    if(ind.length>=3){const top3=ind.slice(0,3).reduce((s,i)=>s+i.pct,0);
      items.push('<div class="ci-item"><strong>Top 3 Industries:</strong> '+top3.toFixed(1)+'% combined — '+ind.slice(0,3).map(i=>E(i.name)).join(', ')+'.</div>')}
    return items})}

function generateValuationInsight(vd){
  _buildInsight('valuationInsight',vd,d=>d&&d.portfolio_avg,function(d){
    const avg=d.portfolio_avg;const items=[];
    if(avg.forward_pe!=null&&avg.eps_growth!=null){
      const tilt=avg.forward_pe>25?'Growth':(avg.forward_pe>18?'Blend':'Value');
      items.push('<div class="ci-item"><strong>'+tilt+' Tilt:</strong> Portfolio forward P/E of '+avg.forward_pe.toFixed(1)+'x with '+avg.eps_growth.toFixed(1)+'% expected EPS growth.</div>')}
    if(avg.expected_return!=null)items.push('<div class="ci-item"><strong>Expected Return:</strong> '+avg.expected_return.toFixed(1)+'% implied return (EPS growth + dividend yield).</div>');
    if(avg.div_yield!=null)items.push('<div class="ci-item"><strong>Income Component:</strong> '+avg.div_yield.toFixed(1)+'% weighted dividend yield contributes to total return.</div>');
    return items})}

function generateSummaryFindings(d,sd,gd,acwi,vd){
  const el=document.getElementById('summaryFindings');if(!el||!d)return;
  const parts=[];
  /* Portfolio composition */
  parts.push('This portfolio combines <strong>'+d.unique_stocks+' unique stocks</strong> across <strong>'+d.unique_managers+' managers</strong> with '+d.total_holdings+' total positions.');
  /* Top sector */
  if(sd&&sd.normalized_sectors&&sd.normalized_sectors.length){const ts=sd.normalized_sectors[0];
    parts.push('The largest sector bet is <strong>'+E(ts.name)+'</strong> at '+ts.pct.toFixed(1)+'%'+(acwi&&acwi.sectors&&acwi.sectors[ts.name]?' (vs '+acwi.sectors[ts.name].toFixed(1)+'% ACWI)':'')+'.')}
  /* Geography */
  if(gd&&gd.normalized_countries&&gd.normalized_countries.length){const us=gd.normalized_countries.find(c=>c.name==='United States');
    if(us)parts.push('Geographic exposure is '+us.pct.toFixed(0)+'% US-based.')}
  /* Returns */
  const wr=d.weighted_return||{};
  if(wr.filing_qtr_weighted_return!=null)parts.push('The weighted portfolio returned <strong>'+(wr.filing_qtr_weighted_return>0?'+':'')+wr.filing_qtr_weighted_return.toFixed(1)+'%</strong> in the filing quarter.');
  if(wr.qtd_weighted_return!=null)parts.push('Since the filing quarter end, the portfolio has returned <strong>'+(wr.qtd_weighted_return>0?'+':'')+wr.qtd_weighted_return.toFixed(1)+'%</strong> QTD.');
  /* Valuation */
  if(vd&&vd.portfolio_avg&&vd.portfolio_avg.forward_pe!=null)parts.push('Valuation metrics show a <strong>'+vd.portfolio_avg.forward_pe.toFixed(1)+'x forward P/E</strong>'+(vd.portfolio_avg.expected_return!=null?' with an implied expected return of '+vd.portfolio_avg.expected_return.toFixed(1)+'%.':'.'));
  /* Most held */
  if(d.most_common_stock)parts.push('<strong>'+E(d.most_common_stock.ticker)+'</strong> is the most widely held stock across '+d.most_common_stock.manager_count+' managers.');
  el.innerHTML=parts.join(' ');el.classList.remove('hidden')}

/* ── Category Drill-Down Detail Card ─ */
let _activeDetail=null;
async function showCategoryDetail(chartId,catType,catName){
  const detailId=chartId+'Detail';
  const detailEl=document.getElementById(detailId);
  if(!detailEl)return;
  /* Toggle: click same category closes it */
  if(_activeDetail&&_activeDetail.chartId===chartId&&_activeDetail.catName===catName){
    closeCategoryDetail();return}
  /* Close any other open detail card */
  document.querySelectorAll('.chart-detail-card').forEach(el=>el.classList.add('hidden'));
  _activeDetail={chartId,catName};
  detailEl.classList.remove('hidden');
  detailEl.innerHTML='<div style="text-align:center;padding:18px;color:var(--muted)"><span class="skeleton skel-bar" style="display:inline-block;width:120px;height:14px"></span></div>';
  try{
    const r=await fetch('/api/category-stocks?type='+encodeURIComponent(catType)+'&name='+encodeURIComponent(catName));
    const d=await r.json();
    if(d.error||!d.stocks||!d.stocks.length){detailEl.innerHTML='<div style="padding:12px;color:var(--muted);font-size:var(--text-sm)">No stock data for this category.</div>';return}
    let html='<button class="cdc-close" onclick="closeCategoryDetail()">&times;</button>';
    html+='<div class="cdc-title">'+E(catName)+' — Top Stocks</div>';
    html+='<table><thead><tr><th>Ticker</th><th>Name</th><th style="text-align:right">Port %</th><th>Managers</th></tr></thead><tbody>';
    d.stocks.forEach(s=>{
      const pills=s.managers.map(m=>{
        const short=m.name.split(/[\s&]/)[0];
        return '<span class="cdc-mgr-pill" title="'+E(m.name)+' ('+m.pct.toFixed(2)+'%)">'+E(short)+'</span>'}).join('');
      html+='<tr onclick="openStockPanel(\''+E(s.ticker)+'\')">';
      html+='<td class="cdc-ticker">'+E(s.ticker)+'</td>';
      html+='<td>'+E(s.short_name||s.name)+'</td>';
      html+='<td class="cdc-pct" style="text-align:right">'+s.pct.toFixed(2)+'%</td>';
      html+='<td><div class="cdc-mgr-pills">'+pills+'</div></td>';
      html+='</tr>'});
    html+='</tbody></table>';
    detailEl.innerHTML=html;
  }catch(e){detailEl.innerHTML='<div style="padding:12px;color:var(--danger);font-size:var(--text-sm)">Error loading stocks.</div>'}
}
function closeCategoryDetail(){
  document.querySelectorAll('.chart-detail-card').forEach(el=>{el.classList.add('hidden');el.innerHTML=''});
  _activeDetail=null}

/* ── Treemap (Plotly.js) ────────────────────── */
async function loadTM(){
  const parts=[];if(C.client_name)parts.push(C.client_name);if(C.report_name)parts.push(C.report_name);
  const titleEl=document.getElementById('tmTitle');
  if(parts.length)titleEl.innerHTML=`<div style="font-size:20px;font-weight:700;color:#fff">${E(parts.join(' — '))}</div>`;
  else titleEl.innerHTML='';
  showSkeletonBox('tmBox',500);
  try{const r=await fetch('/api/treemap-data'),d=await r.json();renderTM(d.managers||[])}
  catch(e){document.getElementById('tmBox').innerHTML='<p class="text-slate-400">No data.</p>'}}
function renderTM(mgrs){
  if(!mgrs.length){document.getElementById('tmBox').innerHTML='<p class="text-slate-400 p-4">No data yet.</p>';return}
  const ids=[],labels=[],parents=[],values=[],colors=[],hovers=[];
  const hasWt=mgrs.some(m=>m.weight>0);
  const totalWt=hasWt?mgrs.reduce((s,m)=>s+(m.weight||0),0):(100);
  ids.push('All');labels.push('All Managers');parents.push('');values.push(totalWt);colors.push('#0f172a');hovers.push('');
  mgrs.forEach((mg,mi)=>{
    const mId='M_'+mi;const clr=COLORS[mi%COLORS.length];
    const mw=hasWt?(mg.weight||0):(100/mgrs.length);
    ids.push(mId);labels.push(mg.manager);parents.push('All');values.push(mw);colors.push(clr);
    hovers.push(`<b>${mg.manager}</b><br>Weight: ${mw.toFixed(1)}%`);
    const totPct=mg.stocks.reduce((s,st)=>s+(st.pct||0),0)||1;
    mg.stocks.forEach((st,si)=>{
      const sId=mId+'_'+si;
      const sw=(st.pct/totPct)*mw;
      const lbl=st.display_label||(st.ticker!=='N/A'?st.name+' ('+st.ticker+')':st.name);
      ids.push(sId);labels.push(lbl);parents.push(mId);values.push(sw);
      const qr=st.filing_quarter_return;
      colors.push(qrColor(qr,clr));
      let hv=`<b>${E(st.name)}</b> (${st.ticker})<br>${st.pct}% of fund`;
      if(st.prior_quarter_return!=null)hv+=`<br>Prior Qtr: ${st.prior_quarter_return>0?'+':''}${st.prior_quarter_return.toFixed(1)}%`;
      if(qr!=null)hv+=`<br>Filing Qtr: ${qr>0?'+':''}${qr.toFixed(1)}%`;
      if(st.qtd_return!=null)hv+=`<br>QTD: ${st.qtd_return>0?'+':''}${st.qtd_return.toFixed(1)}%`;
      if(st.forward_pe!=null)hv+=`<br>Fwd P/E: ${st.forward_pe.toFixed(1)}x`;
      if(st.sector)hv+=`<br>Sector: ${st.sector}`;
      hovers.push(hv)})});
  plotTreemap('tmBox',{ids,labels,parents,values,colors,hovers},{textSize:10,textTemplate:'%{label}'})}

/* ── Overlap ────────────────────────────────── */
async function loadOverlap(){
  showSkeleton('overlapBox',6);showSkeletonBox('overlapBubbleMatrix',420);
  try{
    const r=await fetch('/api/overlap-data');const d=await r.json();
    const ov=d.overlap||[];
    if(!ov.length){
      document.getElementById('overlapBox').innerHTML=`<div style="text-align:center;padding:var(--sp-6)">
        <div style="font-size:2.5rem;margin-bottom:var(--sp-3)">🔍</div>
        <p class="text-slate-300" style="font-size:var(--text-base);font-weight:600;margin-bottom:var(--sp-2)">No Overlap Detected</p>
        <p class="text-slate-400" style="font-size:var(--text-sm);max-width:420px;margin:0 auto;line-height:1.5">
          Overlap analysis requires <strong style="color:#fff">two or more managers</strong> holding the same stock.
          Try loading multiple 13F filings to see which stocks are shared across managers.</p></div>`;
      document.getElementById('overlapBubbleMatrix').innerHTML='';return}
    let h='<table class="overlap-tbl"><thead><tr><th>Stock</th><th># Managers</th><th>Avg %</th><th>Sector</th><th>Held By</th></tr></thead><tbody>';
    for(const o of ov.slice(0,50)){
      const pills=o.managers.map(m=>`<span class="mgr-pill">${E(m)}</span>`).join('');
      const lbl=o.display_label||(o.name+' ('+o.ticker+')');
      h+=`<tr style="cursor:pointer" onclick="openStockPanel('${E(o.ticker||'')}')"><td class="font-semibold text-white">${E(lbl)}</td>
        <td class="text-center"><span class="text-blue-400 font-bold">${o.manager_count}</span></td>
        <td>${o.avg_pct}%</td><td class="text-slate-400">${E(o.sector||'—')}</td>
        <td><div class="mgr-pills">${pills}</div></td></tr>`}
    h+='</tbody></table>';
    document.getElementById('overlapBox').innerHTML=h;
    renderOverlapBubbleMatrix(ov);
  }catch(e){console.error('loadOverlap error:',e);
    document.getElementById('overlapBox').innerHTML='<p class="text-red-400 p-4">Error loading overlap data. Check console for details.</p>';
    document.getElementById('overlapBubbleMatrix').innerHTML=''}}

/* ── Overlap Bubble Matrix (Manager × Stock) ── */
function renderOverlapBubbleMatrix(ov){
  if(!ov||ov.length<2){document.getElementById('overlapBubbleMatrix').innerHTML='';return}
  const SECTOR_COLORS={'Technology':'#3B82F6','Communication Services':'#8B5CF6','Healthcare':'#10B981',
    'Financial Services':'#F59E0B','Consumer Cyclical':'#EF4444','Industrials':'#6366F1',
    'Consumer Defensive':'#14B8A6','Energy':'#F97316','Basic Materials':'#A855F7',
    'Real Estate':'#EC4899','Utilities':'#06B6D4'};
  /* Build unique tickers (x) and managers (y) */
  const top=ov.slice(0,25);
  const tickerList=top.map(o=>o.ticker);
  const mgrSet=new Set();top.forEach(o=>o.managers.forEach(m=>mgrSet.add(m)));
  const mgrList=[...mgrSet];
  /* Build data points */
  const xVals=[],yVals=[],sizes=[],colors=[],texts=[],hovers=[];
  top.forEach(o=>{
    const bubbleSize=o.avg_pct;
    const clr=SECTOR_COLORS[o.sector]||'#64748B';
    o.managers.forEach(m=>{
      xVals.push(o.ticker);yVals.push(m);
      sizes.push(bubbleSize);colors.push(clr);
      texts.push('');
      hovers.push('<b>'+E(o.ticker)+'</b> — '+E(m)+'<br>Avg weight: '+o.avg_pct.toFixed(2)+'%<br>Sector: '+E(o.sector||'N/A')+'<br>Held by '+o.manager_count+' managers')})});
  const maxSize=Math.max(...sizes,0.1);
  const sizeRef=2.0*maxSize/(35*35);
  Plotly.newPlot('overlapBubbleMatrix',[{
    x:xVals,y:yVals,mode:'markers',type:'scatter',
    marker:{size:sizes,sizemode:'area',sizeref:sizeRef,sizemin:6,color:colors,opacity:0.8,
      line:{width:1,color:'rgba(255,255,255,0.25)'}},
    text:texts,hovertext:hovers,hoverinfo:'text'
  }],{
    title:{text:'Manager × Stock Overlap Matrix',font:{color:'#fff',size:18},y:0.96,pad:{b:15}},
    paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
    xaxis:{color:'#e2e8f0',tickfont:{size:10},tickangle:-45,categoryorder:'array',categoryarray:tickerList},
    yaxis:{color:'#e2e8f0',tickfont:{size:10},automargin:true,categoryorder:'array',categoryarray:mgrList},
    margin:{t:75,b:80,l:180,r:20},hovermode:'closest',
    showlegend:false
  },{responsive:true});
  document.getElementById('overlapBubbleMatrix').on('plotly_click',function(data){
    if(data.points&&data.points[0]&&data.points[0].x){openStockPanel(data.points[0].x)}})}

/* ── Sector/Industry Treemap ────────────────── */
async function loadSectorTreemap(mode){
  document.getElementById('sectorTmBtn').className='btn btn-sm '+(mode==='sector'?'btn-blue':'btn-gray');
  document.getElementById('industryTmBtn').className='btn btn-sm '+(mode==='industry'?'btn-blue':'btn-gray');
  showSkeletonBox('sectorTreemapBox',500);
  try{
    const r=await fetch('/api/sector-treemap-data');const d=await r.json();
    const items=mode==='industry'?d.industries:d.sectors;
    if(!items||!items.length){document.getElementById('sectorTreemapBox').innerHTML='<p class="text-slate-400 p-4">No data.</p>';return}
    const ids=[],labels=[],parents=[],values=[],colors=[],hovers=[];
    ids.push('All');labels.push(mode==='industry'?'All Industries':'All Sectors');parents.push('');values.push(0);colors.push('#0f172a');hovers.push('');
    let totalPct=0;
    items.forEach((sec,si)=>{
      const secId='sec_'+si;const clr=COLORS[si%COLORS.length];
      ids.push(secId);labels.push(sec.name);parents.push('All');
      const secPct=sec.pct||0;values.push(secPct);colors.push(clr);totalPct+=secPct;
      let hv='<b>'+E(sec.name)+'</b><br>'+secPct.toFixed(1)+'% of portfolio';
      if(sec.manager_shares&&sec.manager_shares.length){
        hv+='<br><br>Manager breakdown:';
        sec.manager_shares.slice(0,5).forEach(ms=>{
          hv+='<br>'+E(ms.manager)+': '+ms.pct_of_sector.toFixed(0)+'%'
        });
        if(sec.manager_shares.length>5)hv+='<br>...+'+(sec.manager_shares.length-5)+' more'}
      hovers.push(hv);
      const stockPctSum=sec.top_stocks.reduce((s,st)=>s+st.weight_in_combined,0)||1;
      sec.top_stocks.forEach((st,sti)=>{
        const stId=secId+'_'+sti;
        const sw=(st.weight_in_combined/stockPctSum)*secPct;
        ids.push(stId);labels.push(st.display_label||st.name);parents.push(secId);values.push(sw);
        const qr=st.filing_quarter_return;
        colors.push(qrColor(qr,clr));
        let shv='<b>'+E(st.name)+'</b> ('+E(st.ticker)+')<br>Manager: '+E(st.manager)+'<br>Weight: '+st.weight_in_combined.toFixed(2)+'%';
        if(qr!=null)shv+='<br>Filing Qtr: '+(qr>0?'+':'')+qr.toFixed(1)+'%';
        hovers.push(shv)})});
    values[0]=totalPct;
    plotTreemap('sectorTreemapBox',{ids,labels,parents,values,colors,hovers})
  }catch(e){document.getElementById('sectorTreemapBox').innerHTML='<p class="text-red-400 p-4">Error loading treemap</p>'}}

/* ── Geographic Treemap ────────────────────── */
async function loadGeoTreemap(){
  showSkeletonBox('geoTreemapBox',500);
  try{
    const r=await fetch('/api/geo-treemap-data');const d=await r.json();
    const items=d.countries||[];
    if(!items.length){document.getElementById('geoTreemapBox').innerHTML='<p class="text-slate-400 p-4">No geographic data.</p>';return}
    const ids=[],labels=[],parents=[],values=[],colors=[],hovers=[];
    ids.push('All');labels.push('All Countries');parents.push('');values.push(0);colors.push('#0f172a');hovers.push('');
    let totalPct=0;
    items.forEach((co,ci)=>{
      const coId='co_'+ci;const clr=COLORS[ci%COLORS.length];
      ids.push(coId);labels.push(co.name);parents.push('All');
      const coPct=co.pct||0;values.push(coPct);colors.push(clr);totalPct+=coPct;
      let hv='<b>'+E(co.name)+'</b><br>'+coPct.toFixed(1)+'% of portfolio<br>'+co.count+' holdings';
      if(co.manager_shares&&co.manager_shares.length){
        hv+='<br><br>Manager breakdown:';
        co.manager_shares.slice(0,5).forEach(ms=>{
          hv+='<br>'+E(ms.manager)+': '+ms.pct_of_country.toFixed(0)+'%'
        });
        if(co.manager_shares.length>5)hv+='<br>...+'+(co.manager_shares.length-5)+' more'}
      hovers.push(hv);
      const stockPctSum=co.top_stocks.reduce((s,st)=>s+st.weight_in_combined,0)||1;
      co.top_stocks.forEach((st,sti)=>{
        const stId=coId+'_'+sti;
        const sw=(st.weight_in_combined/stockPctSum)*coPct;
        ids.push(stId);labels.push(st.display_label||st.name);parents.push(coId);values.push(sw);
        const qr=st.filing_quarter_return;
        colors.push(qrColor(qr,clr));
        let shv='<b>'+E(st.name)+'</b> ('+E(st.ticker)+')<br>Manager: '+E(st.manager)+'<br>Weight: '+st.weight_in_combined.toFixed(2)+'%';
        if(qr!=null)shv+='<br>Filing Qtr: '+(qr>0?'+':'')+qr.toFixed(1)+'%';
        hovers.push(shv)})});
    values[0]=totalPct;
    plotTreemap('geoTreemapBox',{ids,labels,parents,values,colors,hovers})
  }catch(e){document.getElementById('geoTreemapBox').innerHTML='<p class="text-red-400 p-4">Error loading geo treemap</p>'}}

/* ── Sectors ────────────────────────────────── */
async function loadSectors(){
  try{
    const r=await fetch('/api/sector-data');const d=await r.json();
    if(!d.sectors||d.sectors.length<2)return;
    // Sunburst
    const ids2=[],labels2=[],parents2=[],values2=[];
    ids2.push('root');labels2.push('All');parents2.push('');values2.push(0);
    const sectorSet=new Set();
    for(const s of d.sectors){
      if(!sectorSet.has(s.name)){sectorSet.add(s.name);
        ids2.push('s_'+s.name);labels2.push(s.name);parents2.push('root');values2.push(s.total_value)}}
    for(const ind of (d.industries||[]).slice(0,30)){
      ids2.push('i_'+ind.name);labels2.push(ind.name);parents2.push('s_'+ind.sector);values2.push(ind.total_value)}
    Plotly.newPlot('sectorSunburst',[{type:'sunburst',ids:ids2,labels:labels2,parents:parents2,values:values2,
      branchvalues:'total',marker:{colors:COLORS},textfont:{color:'#fff',size:10},
      hoverinfo:'label+value+percent root'}],
      {title:{text:'Sector / Industry Breakdown',font:{color:'#fff',size:18},pad:{b:15}},
       paper_bgcolor:'#0c1220',margin:{t:55,b:10,l:10,r:10}},{responsive:true});
    // Sector by manager stacked bar
    if(d.by_manager){
      const mgrNames=Object.keys(d.by_manager).slice(0,15);
      const allSectors=[...new Set(mgrNames.flatMap(m=>d.by_manager[m].map(s=>s.sector)))].slice(0,10);
      const traces=allSectors.map((sec,si)=>({
        name:sec,x:mgrNames,y:mgrNames.map(m=>{const f=d.by_manager[m].find(s=>s.sector===sec);return f?f.pct:0}),
        type:'bar',marker:{color:COLORS[si%COLORS.length]}}));
      Plotly.newPlot('sectorByMgr',traces,
        {barmode:'stack',title:{text:'Sector Allocation by Manager',font:{color:'#fff',size:18},pad:{b:15}},
         paper_bgcolor:'#0c1220',plot_bgcolor:'#0c1220',
         xaxis:{color:'#94a3b8',tickangle:-45,tickfont:{size:9}},
         yaxis:{color:'#94a3b8',title:'%'},legend:{font:{color:'#94a3b8',size:9}},
         margin:{t:55,b:100,l:40,r:10}},{responsive:true})
    }
  }catch(e){}}

/* ── QoQ Diff ───────────────────────────────── */
async function runDiff(){
  const d1=document.getElementById('diffDate1').value,d2=document.getElementById('diffDate2').value;
  if(!d1||!d2){alert('Select both dates');return}
  document.getElementById('diffBox').innerHTML='<p class="text-slate-400">Running QoQ comparison...</p>';
  const r=await fetch('/api/run-diff',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({date1:d1,date2:d2})});
  if(!r.ok){const e=await r.json();alert(e.error);return}
  const{run_id}=await r.json(),es=new EventSource('/api/stream/'+run_id);
  es.onmessage=e=>{const m=JSON.parse(e.data);
    if(m.type==='complete'){es.close();showDiff(m.results.diff)}};
  es.onerror=()=>{es.close();document.getElementById('diffBox').innerHTML='<p class="text-red-400">Error running diff</p>'}}
function showDiff(diff){
  if(!diff||!Object.keys(diff).length){document.getElementById('diffBox').innerHTML='<p class="text-slate-400">No diff data.</p>';return}
  let h='';
  for(const[mgr,d]of Object.entries(diff)){
    h+=`<div class="card p-4 mb-3"><h3 class="font-semibold text-white mb-2">${E(mgr)}</h3>`;
    if(d.new_positions.length){h+=`<div class="mb-2"><span class="diff-new font-semibold">New Positions (${d.new_positions.length})</span><div class="text-sm mt-1">`;
      for(const p of d.new_positions){const nm=p.name?E(p.name)+' ('+E(p.ticker)+')':E(p.ticker);h+=`<span class="diff-new">${nm} ${p.pct}%</span> `}h+='</div></div>'}
    if(d.exited_positions.length){h+=`<div class="mb-2"><span class="diff-exit font-semibold">Exited (${d.exited_positions.length})</span><div class="text-sm mt-1">`;
      for(const p of d.exited_positions){const nm=p.name?E(p.name)+' ('+E(p.ticker)+')':E(p.ticker);h+=`<span class="diff-exit">${nm} ${p.pct}%</span> `}h+='</div></div>'}
    if(d.changed_positions.length){h+=`<div class="mb-2"><span class="diff-change font-semibold">Weight Changes (${d.changed_positions.length})</span><div class="text-sm mt-1">`;
      for(const p of d.changed_positions){const arrow=p.change_pct>0?'&#9650;':'&#9660;';const nm=p.name?E(p.name)+' ('+E(p.ticker)+')':E(p.ticker);
        h+=`<span class="diff-change">${nm} ${arrow}${Math.abs(p.change_pct).toFixed(1)}pp</span> `}h+='</div></div>'}
    if(d.unchanged_count)h+=`<div class="text-xs text-slate-500">${d.unchanged_count} positions unchanged</div>`;
    h+='</div>'}
  document.getElementById('diffBox').innerHTML=h}

/* ── Tabs ───────────────────────────────────── */
function switchTab(id){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));
  document.querySelector(`.tab-content#tab-${id}`).classList.add('active');
  /* Activate the tab button — works for both click events and programmatic calls */
  document.querySelectorAll('.tab').forEach(t=>{if(t.textContent.trim().toLowerCase().replace(/\s+/g,'').includes(id))t.classList.add('active')})}

/* ── Util ───────────────────────────────────── */
function toast(m){const t=document.createElement('div');t.textContent=m;
  t.style.cssText='position:fixed;bottom:24px;right:24px;background:rgba(26,35,50,.92);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);color:#fff;padding:14px 28px;border-radius:var(--radius-md);font-weight:var(--weight-medium);font-size:var(--text-sm);z-index:var(--z-toast);animation:fadeInUp .4s cubic-bezier(0.34,1.56,0.64,1);box-shadow:var(--shadow-lg);border:1px solid var(--border);letter-spacing:-0.01em';
  document.body.appendChild(t);setTimeout(()=>{t.style.transition='all .3s cubic-bezier(0.4,0,0.2,1)';t.style.opacity='0';t.style.transform='translateY(8px)';setTimeout(()=>t.remove(),300)},2500)}
function E(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;')}
function A(s){return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'")}
/* Skeleton + animation helpers */
function showSkeleton(elId,rows){const el=document.getElementById(elId);if(!el)return;
  let h='';for(let i=0;i<(rows||3);i++){const w=60+Math.random()*35;h+=`<div class="skeleton skel-bar" style="width:${w}%;opacity:${.5+Math.random()*.3}"></div>`}
  el.innerHTML=h}
function showSkeletonBox(elId,h){const el=document.getElementById(elId);if(!el)return;
  el.innerHTML=`<div class="skeleton skel-box" style="width:100%;height:${h||300}px"></div>`}
function animateValue(el,end,dur){
  if(!el||isNaN(end))return;const isFloat=String(end).includes('.');const hasPct=el.textContent&&el.textContent.includes('%');
  const hasPlus=end>0&&el.dataset.signed;
  let start=0;const startTime=performance.now();
  function step(now){const p=Math.min((now-startTime)/dur,1);const eased=1-Math.pow(1-p,3);
    const v=start+eased*(end-start);
    let txt=isFloat?v.toFixed(1):Math.round(v);
    if(hasPlus&&v>0)txt='+'+txt;if(hasPct||el.dataset.pct)txt+='%';if(el.dataset.suffix)txt+=el.dataset.suffix;
    el.textContent=txt;if(p<1)requestAnimationFrame(step)}
  requestAnimationFrame(step)}

/* ── History (SQLite) ─────────────────────────── */
let histRuns=[];
async function loadHistory(){
  try{const r=await fetch('/api/history');const d=await r.json();
    histRuns=d.runs||[];renderHistory()}catch(e){}}
function renderHistory(){
  const el=document.getElementById('historyList');
  if(!histRuns.length){el.innerHTML='<div class="text-xs text-slate-500">No saved runs yet. Fetch holdings to create one.</div>';
    document.getElementById('histCompare').classList.add('hidden');return}
  const activeId=(typeof last_db_run_id!=='undefined')?last_db_run_id:null;
  el.innerHTML=histRuns.map(r=>{
    const d=r.created_at?new Date(r.created_at).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'2-digit',hour:'numeric',minute:'2-digit'}):'';
    const lbl=r.label||'';
    const isActive=r.id===activeId;
    return `<div class="hist-item${isActive?' active':''}" title="Run #${r.id} — ${r.holding_count} holdings, ${r.manager_count} managers">
      <div style="flex:1;min-width:0" onclick="histLoad(${r.id})">
        <div style="font-size:12px;font-weight:600;color:${isActive?'#60a5fa':'#e2e8f0'}">${lbl||'Run #'+r.id}${isActive?' &#9679;':''}</div>
        <div style="font-size:10px;color:var(--muted)">${d} &middot; ${r.holding_count} pos &middot; ${r.manager_count} mgrs</div>
        <div style="font-size:10px;color:var(--muted)">Max date: ${r.max_date||'?'} &middot; Top ${r.top_n||'?'}</div>
      </div>
      <div class="hist-actions">
        <input class="hist-label-inp" value="${E(lbl)}" placeholder="label" onclick="event.stopPropagation()"
               onchange="histLabel(${r.id},this.value)" title="Set label">
        <button class="btn-red" style="font-size:10px;padding:2px 6px" onclick="event.stopPropagation();histDel(${r.id})" title="Delete">&#10005;</button>
      </div></div>`}).join('');
  /* Populate compare dropdowns */
  if(histRuns.length>=2){
    document.getElementById('histCompare').classList.remove('hidden');
    const opts=histRuns.map(r=>`<option value="${r.id}">${r.label||'Run #'+r.id} (${r.max_date||r.run_date})</option>`).join('');
    document.getElementById('histRun1').innerHTML=opts;
    document.getElementById('histRun2').innerHTML=opts;
    /* Default: second-newest vs newest */
    if(histRuns.length>=2){
      document.getElementById('histRun1').value=histRuns[1].id;
      document.getElementById('histRun2').value=histRuns[0].id}
  }else{document.getElementById('histCompare').classList.add('hidden')}}
let last_db_run_id=null;
async function histLoad(id){
  toast('Loading run #'+id+'...');
  const r=await fetch('/api/history/'+id+'/load',{method:'POST'});
  if(!r.ok){toast('Failed to load');return}
  const d=await r.json();last_db_run_id=id;
  document.getElementById('sBadge').textContent='Loaded run #'+id;
  document.getElementById('resArea').classList.remove('hidden');
  document.getElementById('dlBtn').classList.remove('hidden');
  document.getElementById('pdfBtn').classList.remove('hidden');
  document.getElementById('resetBtn').classList.remove('hidden');
  document.getElementById('resSummary').innerHTML=
    '<span class="text-blue-400 font-semibold">Loaded '+d.holdings+' holdings (run #'+id+')</span>';
  document.getElementById('csvLinks').innerHTML='';
  buildHeroInsight();loadTM();loadHeatmap();loadSummary();loadOverlap();loadSectors();loadSectorTreemap('sector');loadGeoTreemap();
  renderHistory();toast('Run #'+id+' loaded')}
async function histDel(id){
  if(!confirm('Delete run #'+id+'? This cannot be undone.'))return;
  await fetch('/api/history/'+id,{method:'DELETE'});
  histRuns=histRuns.filter(r=>r.id!==id);renderHistory();toast('Run deleted')}
async function histLabel(id,label){
  await fetch('/api/history/'+id+'/label',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label})});
  const r=histRuns.find(x=>x.id===id);if(r)r.label=label;renderHistory()}
async function histCompare(){
  const id1=parseInt(document.getElementById('histRun1').value);
  const id2=parseInt(document.getElementById('histRun2').value);
  if(id1===id2){toast('Select two different runs');return}
  toast('Comparing...');
  const r=await fetch('/api/history/compare',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({run_id_1:id1,run_id_2:id2})});
  if(!r.ok){toast('Compare failed');return}
  const d=await r.json();
  document.getElementById('resArea').classList.remove('hidden');
  switchTab('diff');showDiff(d.diff);
  const r1=histRuns.find(x=>x.id===id1),r2=histRuns.find(x=>x.id===id2);
  const l1=r1?(r1.label||'Run #'+id1):'?',l2=r2?(r2.label||'Run #'+id2):'?';
  toast('Compared: '+l1+' vs '+l2)}

load();loadHistory();
/* ── Plotly resize on layout shift ── */
window.addEventListener('resize',()=>{document.querySelectorAll('.js-plotly-plot').forEach(el=>{Plotly.Plots.resize(el)})});

/* ── Heatmap ───────────────────────────────── */
function hmColor(ret){
  if(ret==null)return'#334155';
  const clamped=Math.max(-30,Math.min(30,ret));
  if(clamped>=0){const t=clamped/30;return`hsl(${140},${50+t*30}%,${22+t*18}%)`}
  else{const t=Math.abs(clamped)/30;return`hsl(${0},${40+t*40}%,${25+t*12}%)`}}
async function loadHeatmap(){
  showSkeletonBox('heatmapBox',400);
  try{const r=await fetch('/api/treemap-data');const d=await r.json();
    const mgrs=d.managers||[];if(!mgrs.length){document.getElementById('heatmapBox').innerHTML='<p class="text-slate-400 p-4">No data yet. Run a fetch first.</p>';return}
    let h='<div class="hm-wrap">';
    mgrs.forEach((mg,mi)=>{
      const clr=COLORS[mi%COLORS.length];
      const sorted=[...mg.stocks].sort((a,b)=>b.pct-a.pct);
      const totalPct=sorted.reduce((s,st)=>s+st.pct,0)||1;
      const TOTAL_AREA=Math.max(sorted.length*2800,40000);
      h+=`<div class="hm-section fade" style="animation-delay:${mi*50}ms">`;
      h+=`<div class="hm-header"><div class="hm-header-dot" style="background:${clr}"></div>${E(mg.manager)}<span style="color:var(--muted);font-weight:400;font-size:11px">${sorted.length} holdings</span></div>`;
      h+='<div class="hm-grid">';
      sorted.forEach(st=>{
        const frac=st.pct/totalPct;
        const area=Math.max(frac*TOTAL_AREA,1200);
        const side=Math.round(Math.sqrt(area));
        const w=Math.max(side,44);const hh=Math.max(Math.round(area/w),36);
        const bg=hmColor(st.filing_quarter_return);
        const ticker=st.ticker&&st.ticker!=='N/A'?st.ticker:st.name.substring(0,6);
        const retStr=st.filing_quarter_return!=null?((st.filing_quarter_return>0?'+':'')+st.filing_quarter_return.toFixed(1)+'%'):'';
        const tip=E(st.name)+(st.ticker&&st.ticker!=='N/A'?' ('+st.ticker+')':'')+' — '+st.pct+'%'+(retStr?' | Qtr: '+retStr:'')+(st.sector?' | '+st.sector:'');
        h+=`<div class="hm-cell" style="width:${w}px;height:${hh}px;background:${bg};cursor:pointer" title="${tip}" onclick="openStockPanel('${st.ticker&&st.ticker!=='N/A'?E(st.ticker):''}')">`;
        h+=`<span class="hm-ticker">${E(ticker)}</span>`;
        h+=`<span class="hm-pct">${st.pct}%</span>`;
        if(retStr)h+=`<span class="hm-ret">${retStr}</span>`;
        h+='</div>'});
      h+='</div></div>'});
    h+='</div>';
    document.getElementById('heatmapBox').innerHTML=h}
  catch(e){document.getElementById('heatmapBox').innerHTML='<p class="text-red-400 p-4">Error loading heatmap</p>'}}

/* ── Stock Spotlight (Ctrl+K) ──────────────── */
let _spotData=null;
function openSpotlight(){
  document.getElementById('spotlight').style.display='';
  const inp=document.getElementById('spotInput');inp.value='';inp.focus();
  document.getElementById('spotResults').innerHTML='<div class="spot-empty">Type a ticker or stock name to search across all managers</div>';
  /* Cache data */
  if(!_spotData){fetch('/api/treemap-data').then(r=>r.json()).then(d=>{_spotData=d.managers||[]}).catch(()=>{})}}
function closeSpotlight(){document.getElementById('spotlight').style.display='none'}
function spotSearch(q){
  const box=document.getElementById('spotResults');
  if(!q||q.length<1){box.innerHTML='<div class="spot-empty">Type a ticker or stock name to search across all managers</div>';return}
  if(!_spotData||!_spotData.length){box.innerHTML='<div class="spot-empty">No holdings data loaded. Fetch holdings first.</div>';return}
  const ql=q.toLowerCase();
  /* Build grouped results: {ticker: {name, ticker, sector, return, managers:[{name,pct}]}} */
  const grouped={};
  _spotData.forEach(mg=>{mg.stocks.forEach(st=>{
    const tk=st.ticker&&st.ticker!=='N/A'?st.ticker:'__'+st.name;
    const nameMatch=st.name.toLowerCase().includes(ql);
    const tickerMatch=st.ticker&&st.ticker.toLowerCase().startsWith(ql);
    if(!nameMatch&&!tickerMatch)return;
    if(!grouped[tk])grouped[tk]={name:st.name,ticker:st.ticker,sector:st.sector||'',ret:st.filing_quarter_return,managers:[]};
    grouped[tk].managers.push({name:mg.manager,pct:st.pct})})});
  const results=Object.values(grouped).sort((a,b)=>b.managers.length-a.managers.length||b.managers.reduce((s,m)=>s+m.pct,0)-a.managers.reduce((s,m)=>s+m.pct,0)).slice(0,20);
  if(!results.length){box.innerHTML='<div class="spot-empty">No matches for "'+E(q)+'"</div>';return}
  box.innerHTML=results.map(r=>{
    const retStr=r.ret!=null?((r.ret>0?'+':'')+r.ret.toFixed(1)+'%'):'';
    const retCls=r.ret!=null?(r.ret>=0?'pos':'neg'):'';
    const mgrHtml=r.managers.map(m=>`<span class="spot-mgr">${E(m.name)}<span class="spot-mgr-pct">${m.pct}%</span></span>`).join('');
    return `<div class="spot-item" style="cursor:pointer" onclick="openStockPanel('${E(r.ticker&&r.ticker!=='N/A'?r.ticker:'')}');closeSpotlight()"><div class="spot-stock-row">
      <span class="spot-ticker">${E(r.ticker&&r.ticker!=='N/A'?r.ticker:'—')}</span>
      <span class="spot-name">${E(r.name)}</span>
      ${retStr?'<span class="spot-return '+retCls+'">'+retStr+'</span>':''}
      <span class="spot-sector">${E(r.sector)}</span>
    </div><div class="spot-mgrs">${mgrHtml}</div></div>`}).join('')}
document.addEventListener('keydown',e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==='k'){e.preventDefault();
    if(document.getElementById('spotlight').style.display==='none')openSpotlight();else closeSpotlight()}
  if(e.key==='Escape'){
    if(_sdpOpen){closeStockPanel();return}
    if(document.getElementById('spotlight').style.display!=='none')closeSpotlight()}});

/* ── Stock Drill-Down Panel ───────────── */
let _sdpOpen=false;
function openStockPanel(ticker){
  if(!ticker||ticker==='N/A')return;
  ticker=ticker.toUpperCase();
  _sdpOpen=true;
  document.getElementById('sdpOverlay').classList.add('open');
  document.getElementById('sdpPanel').classList.add('open');
  document.getElementById('sdpBreadcrumbTicker').textContent=ticker;
  document.getElementById('sdpBody').innerHTML=
    '<div class="skeleton skel-bar" style="width:60%"></div>'+
    '<div class="skeleton skel-bar" style="width:80%"></div>'+
    '<div class="skeleton skel-box" style="height:80px;margin:12px 0"></div>'+
    '<div class="skeleton skel-bar" style="width:50%"></div>'+
    '<div class="skeleton skel-bar" style="width:70%"></div>'+
    '<div class="skeleton skel-box" style="height:120px;margin:12px 0"></div>';
  loadStockDetail(ticker)}
function closeStockPanel(){
  _sdpOpen=false;
  document.getElementById('sdpOverlay').classList.remove('open');
  document.getElementById('sdpPanel').classList.remove('open')}
async function loadStockDetail(ticker){
  try{
    const r=await fetch('/api/stock-detail/'+encodeURIComponent(ticker));
    if(!r.ok){document.getElementById('sdpBody').innerHTML='<p class="text-red-400 p-4">Ticker not found in current holdings.</p>';return}
    const d=await r.json();renderStockPanel(d)
  }catch(e){document.getElementById('sdpBody').innerHTML='<p class="text-red-400 p-4">Error loading stock details.</p>'}}
function renderStockPanel(d){
  const body=document.getElementById('sdpBody');
  const fmtMcap=v=>{if(v==null)return'N/A';if(v>=1e12)return'$'+(v/1e12).toFixed(2)+'T';if(v>=1e9)return'$'+(v/1e9).toFixed(1)+'B';if(v>=1e6)return'$'+(v/1e6).toFixed(0)+'M';return'$'+v.toLocaleString()};
  const fmtPct=v=>v!=null?(v>0?'+':'')+v.toFixed(1)+'%':'N/A';
  const fmtVal=v=>{if(v==null)return'N/A';if(v>=1e9)return'$'+(v/1e9).toFixed(2)+'B';if(v>=1e6)return'$'+(v/1e6).toFixed(1)+'M';if(v>=1e3)return'$'+(v/1e3).toFixed(0)+'K';return'$'+v.toLocaleString()};
  const retPill=v=>{if(v==null)return'<span style="color:var(--muted)">N/A</span>';const cls=v>=0?'pos':'neg';return'<span class="sdp-return-pill '+cls+'">'+fmtPct(v)+'</span>'};
  let h='<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">';
  h+='<span class="sdp-ticker-badge">'+E(d.ticker)+'</span>';
  h+='<span class="sdp-title">'+E(d.name)+'</span></div>';
  h+='<div class="sdp-subtitle">'+E(d.sector||'')+(d.industry?' / '+E(d.industry):'')+(d.country?' &middot; '+E(d.country):'')+'</div>';
  // Sparkline
  h+='<div class="sdp-section"><div class="sdp-section-title">Price (6 Months)</div>';
  h+='<div id="sdpSparkline" class="sdp-sparkline"></div></div>';
  // Key stats
  h+='<div class="sdp-section"><div class="sdp-section-title">Key Stats</div>';
  h+='<div class="sdp-stats">';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+(d.forward_pe!=null?d.forward_pe.toFixed(1)+'x':'N/A')+'</div><div class="sdp-stat-label">Forward P/E</div></div>';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+(d.trailing_eps!=null?'$'+d.trailing_eps.toFixed(2):'N/A')+'</div><div class="sdp-stat-label">Trail 4Q EPS</div></div>';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+(d.forward_eps!=null?'$'+d.forward_eps.toFixed(2):'N/A')+'</div><div class="sdp-stat-label">Fwd 12M EPS</div></div>';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+fmtMcap(d.market_cap)+'</div><div class="sdp-stat-label">Market Cap</div></div>';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+(d.dividend_yield!=null?d.dividend_yield.toFixed(2)+'%':'N/A')+'</div><div class="sdp-stat-label">Dividend Yield</div></div>';
  h+='<div class="sdp-stat"><div class="sdp-stat-val">'+(d.forward_eps_growth!=null?fmtPct(d.forward_eps_growth):'N/A')+'</div><div class="sdp-stat-label">Fwd EPS Growth</div></div>';
  h+='</div></div>';
  // Quarterly returns
  h+='<div class="sdp-section"><div class="sdp-section-title">Quarterly Returns</div>';
  h+='<div style="display:flex;gap:8px">';
  h+='<div class="sdp-stat" style="flex:1"><div class="sdp-stat-val">'+retPill(d.filing_quarter_return_pct)+'</div><div class="sdp-stat-label">Filing Quarter</div></div>';
  h+='<div class="sdp-stat" style="flex:1"><div class="sdp-stat-val">'+retPill(d.prior_quarter_return_pct)+'</div><div class="sdp-stat-label">Prior Quarter</div></div>';
  h+='<div class="sdp-stat" style="flex:1"><div class="sdp-stat-val">'+retPill(d.qtd_return_pct)+'</div><div class="sdp-stat-label">QTD Return</div></div>';
  if(d.filing_eps_beat_pct!=null){h+='<div class="sdp-stat" style="flex:1"><div class="sdp-stat-val">'+retPill(d.filing_eps_beat_pct)+'</div><div class="sdp-stat-label">EPS Beat</div></div>'}
  h+='</div></div>';
  // Manager table
  h+='<div class="sdp-section"><div class="sdp-section-title">Manager Holdings ('+d.managers.length+')</div>';
  h+='<table class="sdp-mgr-tbl"><thead><tr><th>Manager</th><th>Weight</th><th>Value</th><th>Rank</th></tr></thead><tbody>';
  for(const m of d.managers){
    h+='<tr><td class="mgr-name-cell">'+E(m.manager)+'</td>';
    h+='<td class="pct-cell">'+m.pct+'%</td>';
    h+='<td class="val-cell">'+fmtVal(m.value)+'</td>';
    h+='<td class="val-cell">#'+(m.rank||'?')+'</td></tr>'}
  h+='</tbody></table></div>';
  // Weight history
  h+='<div class="sdp-section"><div class="sdp-section-title">Weight History Across Runs</div>';
  h+='<div id="sdpHistoryChart" class="sdp-history-chart"></div></div>';
  body.innerHTML=h;
  // Render sparkline
  if(d.sparkline&&d.sparkline.length>0){
    const up=d.sparkline[d.sparkline.length-1]>=d.sparkline[0];
    const clr=up?'#4ade80':'#f87171';
    const fill=up?'rgba(34,197,94,.08)':'rgba(239,68,68,.08)';
    Plotly.newPlot('sdpSparkline',[{x:d.sparkline_dates,y:d.sparkline,type:'scatter',mode:'lines',
      line:{color:clr,width:2},fill:'tozeroy',fillcolor:fill,
      hovertemplate:'%{x}<br>$%{y:.2f}<extra></extra>'}],
      {paper_bgcolor:'transparent',plot_bgcolor:'transparent',
       margin:{t:5,b:20,l:45,r:10},
       xaxis:{showgrid:false,color:'#64748b',tickfont:{size:9},nticks:4,tickformat:'%b %d'},
       yaxis:{showgrid:true,gridcolor:'rgba(51,65,85,.3)',color:'#64748b',tickfont:{size:9},tickprefix:'$'},
       font:{color:'#94a3b8'}},{responsive:true,displayModeBar:false})
  }else{document.getElementById('sdpSparkline').innerHTML='<div style="padding:24px;text-align:center;color:var(--muted);font-size:12px">No price data available</div>'}
  // Render weight history
  renderWeightHistory(d.history,d.ticker)}
function renderWeightHistory(history,ticker){
  const el=document.getElementById('sdpHistoryChart');
  if(!history||!history.length){el.innerHTML='<div style="padding:24px;text-align:center;color:var(--muted);font-size:12px">No history yet — run multiple fetches to see trends</div>';return}
  const labels=history.map(h=>h.max_date||h.run_date);
  const avgW=history.map(h=>{const s=h.managers.reduce((a,m)=>a+m.pct,0);return+(s/h.managers.length).toFixed(2)});
  const mgrCt=history.map(h=>h.managers.length);
  Plotly.newPlot(el,[
    {x:labels,y:avgW,type:'scatter',mode:'lines+markers',name:'Avg Weight %',
     line:{color:'#3b82f6',width:2},marker:{size:6,color:'#3b82f6'},
     hovertemplate:'%{x}<br>Avg: %{y:.2f}%<extra></extra>'},
    {x:labels,y:mgrCt,type:'bar',name:'# Managers',
     marker:{color:'rgba(59,130,246,.2)'},yaxis:'y2',
     hovertemplate:'%{x}<br>%{y} managers<extra></extra>'}],
    {paper_bgcolor:'transparent',plot_bgcolor:'transparent',
     margin:{t:5,b:30,l:40,r:40},
     xaxis:{showgrid:false,color:'#64748b',tickfont:{size:9},tickangle:-30},
     yaxis:{showgrid:true,gridcolor:'rgba(51,65,85,.3)',color:'#64748b',tickfont:{size:9},title:{text:'Avg %',font:{size:9,color:'#64748b'}}},
     yaxis2:{overlaying:'y',side:'right',showgrid:false,color:'#64748b',tickfont:{size:9},title:{text:'# Mgrs',font:{size:9,color:'#64748b'}}},
     legend:{font:{color:'#94a3b8',size:9},orientation:'h',y:-0.3},
     font:{color:'#94a3b8'},showlegend:true,barmode:'overlay'},
    {responsive:true,displayModeBar:false})}

/* ── Apple-style Scroll Reveal (IntersectionObserver) ── */
(function(){
  let srObserver=null;
  function getScrollRoot(){return document.querySelector('.flex-1.overflow-y-auto')}

  function createObserver(){
    if(srObserver) srObserver.disconnect();
    const root=getScrollRoot()||null;
    srObserver=new IntersectionObserver((entries)=>{
      entries.forEach(entry=>{
        if(entry.isIntersecting){
          entry.target.classList.add('visible');
          const children=entry.target.querySelectorAll('.sr-child');
          children.forEach((ch,i)=>{ch.style.transitionDelay=(i*0.08)+'s';ch.classList.add('visible')});
        }
      })
    },{root:root,threshold:0.08,rootMargin:'0px 0px -60px 0px'});
    return srObserver;
  }

  function initScrollReveal(){
    if(!srObserver) createObserver();
    /* Double-rAF ensures browser paints opacity:0 before we observe and trigger .visible */
    requestAnimationFrame(()=>{requestAnimationFrame(()=>{
      document.querySelectorAll('.scroll-reveal:not(.sr-observed)').forEach(el=>{
        el.classList.add('sr-observed');
        srObserver.observe(el);
      });
    })});
  }

  /* Reset: remove .visible and .sr-observed so elements can re-animate */
  function resetScrollReveal(){
    if(srObserver) srObserver.disconnect();
    document.querySelectorAll('.scroll-reveal').forEach(el=>{
      el.classList.remove('visible','sr-observed');
      el.querySelectorAll('.sr-child').forEach(ch=>{ch.classList.remove('visible');ch.style.transitionDelay=''});
    });
    createObserver();
  }

  const mo=new MutationObserver(()=>requestAnimationFrame(initScrollReveal));
  mo.observe(document.body,{childList:true,subtree:true});
  document.addEventListener('DOMContentLoaded',()=>{createObserver();initScrollReveal()});
  const origSwitchTab=window.switchTab;
  if(origSwitchTab){window.switchTab=function(t){
    origSwitchTab(t);
    /* Reset and re-trigger for the newly visible tab content */
    setTimeout(()=>{resetScrollReveal();initScrollReveal()},80);
  }}
  window._initScrollReveal=initScrollReveal;
  window._resetScrollReveal=resetScrollReveal;
})();
