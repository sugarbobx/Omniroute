// ══════════════════════════════════════════════════════════════════════
// ANIMATED BACKGROUND: Stars + Chart Patterns + Constellations
// ══════════════════════════════════════════════════════════════════════
(function(){
const canvas = document.getElementById('bg-canvas');
const ctx    = canvas.getContext('2d');
let W,H,stars=[],patterns=[],constellations=[];
const BLUE = '#0070f3', BLUE2 = '#3b9eff';

function resize(){
  W = canvas.width  = window.innerWidth;
  H = canvas.height = window.innerHeight;
}
window.addEventListener('resize', resize);
resize();

// ── Stars ──────────────────────────────────────────────────────────
function mkStars(){
  stars = Array.from({length:200}, () => ({
    x: Math.random()*W, y: Math.random()*H,
    r: Math.random()*1.4 + 0.3,
    a: Math.random(), da: (Math.random()-.5)*.008,
    speed: Math.random()*.15 + .05,
  }));
}

function drawStars(){
  stars.forEach(s => {
    s.a = Math.max(.05, Math.min(1, s.a + s.da));
    if(s.a <= .05 || s.a >= 1) s.da *= -1;
    s.x -= s.speed;
    if(s.x < -2) s.x = W + 2;
    ctx.beginPath();
    ctx.arc(s.x, s.y, s.r, 0, Math.PI*2);
    ctx.fillStyle = `rgba(59,158,255,${s.a * .7})`;
    ctx.fill();
  });
}

// ── Constellation cluster ──────────────────────────────────────────
class Constellation {
  constructor(){this.reset(true);}
  reset(init=false){
    this.cx = Math.random()*W;
    this.cy = Math.random()*H;
    this.pts = Array.from({length:Math.floor(Math.random()*5+4)}, () => ({
      ox: (Math.random()-.5)*160,
      oy: (Math.random()-.5)*160,
    }));
    // Build edges (spanning-tree style for clarity)
    this.edges=[];
    for(let i=1;i<this.pts.length;i++){
      this.edges.push([i-1, i]);
      if(i>2 && Math.random()>.5) this.edges.push([Math.floor(Math.random()*i), i]);
    }
    this.angle  = Math.random()*Math.PI*2;
    this.dAngle = (Math.random()-.5)*.002;
    this.alpha  = 0;
    this.phase  = init ? 'in' : 'in';
    this.life   = 0;
    this.maxLife= 280 + Math.random()*200;
    this.fadeSpeed = .006;
  }
  draw(){
    this.life++;
    if(this.phase==='in'){ this.alpha=Math.min(.55, this.alpha+this.fadeSpeed); if(this.alpha>=.55) this.phase='hold'; }
    else if(this.phase==='hold'){ if(this.life>this.maxLife) this.phase='out'; }
    else{ this.alpha=Math.max(0, this.alpha-this.fadeSpeed); if(this.alpha<=0){this.reset();return;} }
    this.angle += this.dAngle;
    const pts = this.pts.map(p => {
      const rx = p.ox*Math.cos(this.angle) - p.oy*Math.sin(this.angle);
      const ry = p.ox*Math.sin(this.angle) + p.oy*Math.cos(this.angle);
      return {x: this.cx+rx, y: this.cy+ry};
    });
    ctx.strokeStyle = `rgba(0,112,243,${this.alpha*.5})`;
    ctx.lineWidth = .6;
    this.edges.forEach(([a,b]) => {
      ctx.beginPath(); ctx.moveTo(pts[a].x,pts[a].y); ctx.lineTo(pts[b].x,pts[b].y); ctx.stroke();
    });
    pts.forEach(p => {
      ctx.beginPath(); ctx.arc(p.x,p.y,1.5,0,Math.PI*2);
      ctx.fillStyle = `rgba(59,158,255,${this.alpha})`;
      ctx.fill();
      // Tiny glow
      ctx.beginPath(); ctx.arc(p.x,p.y,4,0,Math.PI*2);
      ctx.fillStyle = `rgba(0,112,243,${this.alpha*.15})`;
      ctx.fill();
    });
  }
}

function mkConstellations(){ constellations = Array.from({length:6}, ()=>new Constellation()); }

// ── Chart Patterns ─────────────────────────────────────────────────
const PATTERN_TYPES = ['head_shoulders','double_top','bull_flag','channel','triangle','doji_candles'];

class ChartPattern {
  constructor(){ this.reset(true); }
  reset(init=false){
    this.type  = PATTERN_TYPES[Math.floor(Math.random()*PATTERN_TYPES.length)];
    this.x     = Math.random()*(W-220)+80;
    this.y     = Math.random()*(H-160)+80;
    this.scale = Math.random()*0.7+0.6;
    this.alpha = 0;
    this.phase = 'in';
    this.life  = 0;
    this.maxLife = 220+Math.random()*160;
    this.fadeSpeed = .005;
    this.color = Math.random()>.5 ? BLUE : BLUE2;
  }
  buildPath(type){
    // Returns array of [x,y] points
    const s = this.scale;
    switch(type){
      case 'head_shoulders': return [
        [0,0],[20,-20*s],[40,5],[60,-40*s],[80,5],[100,-20*s],[120,0]
      ];
      case 'double_top': return [
        [0,10],[20,-35*s],[40,0],[60,-35*s],[80,10],[100,20]
      ];
      case 'bull_flag': return [
        [0,30*s],[30,-10*s],[35,-5*s],[70,-30*s],[75,-25*s],[110,-50*s]
      ];
      case 'channel': return [
        [0,0],[25,-10*s],[50,5*s],[75,-5*s],[100,10*s],
        [100,25*s],[75,10*s],[50,20*s],[25,5*s],[0,15*s],[0,0]
      ];
      case 'triangle': return [
        [0,40*s],[80,0],[0,-40*s],[0,40*s]
      ];
      case 'doji_candles': return null; // drawn specially
    }
  }
  draw(){
    this.life++;
    const fs = this.fadeSpeed;
    if(this.phase==='in'){ this.alpha=Math.min(.35,this.alpha+fs); if(this.alpha>=.35) this.phase='hold'; }
    else if(this.phase==='hold'){ if(this.life>this.maxLife) this.phase='out'; }
    else{ this.alpha=Math.max(0,this.alpha-fs); if(this.alpha<=0){this.reset();return;} }

    ctx.save();
    ctx.translate(this.x, this.y);
    const a = this.alpha;
    const col = `rgba(0,112,243,${a})`;
    const colD = `rgba(59,158,255,${a*.5})`;

    if(this.type==='doji_candles'){
      // Draw 5 mini candles
      for(let i=0;i<5;i++){
        const cx = (i-2)*18*this.scale;
        const ch = (Math.random()*30+10)*this.scale;
        const bull = Math.random()>.5;
        ctx.strokeStyle = bull ? `rgba(0,214,143,${a})` : `rgba(255,59,92,${a})`;
        ctx.fillStyle   = bull ? `rgba(0,214,143,${a*.3})` : `rgba(255,59,92,${a*.3})`;
        ctx.lineWidth   = 1;
        const body = ch*.4;
        ctx.fillRect(cx-5*this.scale, -body/2, 10*this.scale, body);
        ctx.strokeRect(cx-5*this.scale, -body/2, 10*this.scale, body);
        ctx.beginPath();
        ctx.moveTo(cx, -ch/2); ctx.lineTo(cx, -body/2);
        ctx.moveTo(cx, body/2); ctx.lineTo(cx, ch/2);
        ctx.stroke();
      }
    } else {
      const pts = this.buildPath(this.type);
      if(!pts){ctx.restore();return;}
      ctx.beginPath();
      ctx.moveTo(pts[0][0], pts[0][1]);
      pts.slice(1).forEach(([px,py]) => ctx.lineTo(px, py));
      ctx.strokeStyle = col;
      ctx.lineWidth   = 1.2;
      ctx.stroke();
      // Label
      ctx.fillStyle = colD;
      ctx.font = `${Math.floor(9*this.scale)}px 'Geist Mono',monospace`;
      const labels = {head_shoulders:'HEAD & SHOULDERS',double_top:'DOUBLE TOP',bull_flag:'BULL FLAG',channel:'CHANNEL',triangle:'TRIANGLE'};
      ctx.fillText(labels[this.type]||this.type.toUpperCase(), 0, 30*this.scale);
    }
    ctx.restore();
  }
}

function mkPatterns(){ patterns = Array.from({length:7}, ()=>new ChartPattern()); }

mkStars(); mkConstellations(); mkPatterns();

let _bgRafId = null;
function animate(){
  ctx.clearRect(0,0,W,H);
  // Deep radial vignette
  const grad = ctx.createRadialGradient(W/2,H/2,0,W/2,H/2,W*.7);
  grad.addColorStop(0,'rgba(0,16,40,.04)');
  grad.addColorStop(1,'rgba(0,0,0,.0)');
  ctx.fillStyle=grad; ctx.fillRect(0,0,W,H);

  drawStars();
  constellations.forEach(c=>c.draw());
  patterns.forEach(p=>p.draw());
  _bgRafId = requestAnimationFrame(animate);
}
animate();
window.addEventListener('pagehide', ()=>{ if(_bgRafId) cancelAnimationFrame(_bgRafId); });
})();

// ══════════════════════════════════════════════════════════════════════
// TRADING PSYCHOLOGY QUOTES TICKER
// ══════════════════════════════════════════════════════════════════════
const QUOTES = [
  "The trend is your friend until the end where it bends. — Ed Seykota",
  "Cut your losses short and let your profits run. — Jesse Livermore",
  "Risk comes from not knowing what you're doing. — Warren Buffett",
  "The goal of a successful trader is to make the best trades. Money is secondary. — Alexander Elder",
  "In trading, the impossible happens about twice a year. — Henri M. Simoes",
  "Markets are never wrong — opinions often are. — Jesse Livermore",
  "The most important thing in trading is capital preservation. — Paul Tudor Jones",
  "Win or lose, everybody gets what they want out of the market. — Ed Seykota",
  "Do more of what works and less of what doesn't. — Steve Clark",
  "The key is consistency and discipline. — Michael Marcus",
  "Trade what you see, not what you think. — Ancient trading proverb",
  "Amateurs want to be right. Professionals want to make money. — Unknown",
  "Price is the only truth. Everything else is an opinion. — Unknown",
  "Your biggest enemy as a trader is yourself. — Unknown",
  "Patience is the ultimate edge in any market. — Unknown",
];

(function buildTicker(){
  const inner = document.getElementById('quote-inner');
  // Duplicate for seamless loop
  const all = [...QUOTES,...QUOTES];
  inner.innerHTML = all.map(q=>`<span class="q-item">${q}</span>`).join('');
})();

// ══════════════════════════════════════════════════════════════════════
// APP STATE
// ══════════════════════════════════════════════════════════════════════
let BRIDGE='http://127.0.0.1:8000', ws=null, wsActive=false, _cdTimer=null;
let logTab='all', localLogs=[], symMap={}, currentRole='master';
let mastersData=[], slavesData=[];
const equityHistory = {};
let _linkMasterId=null, _confirmCb=null;
const sbState={guide:true,cfg:true,add:true,sym:false};

// Clock
setInterval(()=>{ document.getElementById('clock').textContent=new Date().toUTCString().slice(17,25); },1000);

// ══════════════════════════════════════════════════════════════════════
// SIDEBAR TOGGLES
// ══════════════════════════════════════════════════════════════════════
function toggleSB(id){
  sbState[id]=!sbState[id];
  const body  = document.getElementById(id+'-body');
  const arrow = document.getElementById(id+'-arrow');
  body.style.display  = sbState[id]?'block':'none';
  arrow.textContent   = sbState[id]?'▾':'▸';
}

function setRole(r){
  currentRole=r;
  document.getElementById('role-master').classList.toggle('active',r==='master');
  document.getElementById('role-slave').classList.toggle('active',r==='slave');
  document.getElementById('master-fields').style.display=r==='master'?'block':'none';
  document.getElementById('slave-fields').style.display=r==='slave'?'block':'none';
}

// ══════════════════════════════════════════════════════════════════════
// WEBSOCKET + BRIDGE CONNECTION
// ══════════════════════════════════════════════════════════════════════
function connectBridge(){
  BRIDGE = document.getElementById('cfg-url').value.replace(/\/$/,'');
  try{ localStorage.setItem('omni_bridge_url', BRIDGE); }catch(e){}
  const key=document.getElementById('cfg-apikey')?.value;
  if(key) try{ localStorage.setItem('omni_api_key', key); }catch(e){}
  hideBridgeError();
  if(_cdTimer){clearInterval(_cdTimer);_cdTimer=null;}
  if(ws){ws.close();ws=null;}

  // Pass the API key as a query param — the WS handshake can't set headers.
  const wsKey = document.getElementById('cfg-apikey')?.value;
  const wsUrl = BRIDGE.replace(/^http/,'ws')+'/ws/status'+(wsKey?('?api_key='+encodeURIComponent(wsKey)):'');
  try{
    ws=new WebSocket(wsUrl);
    ws.onopen=()=>{
      wsActive=true;
      if(_cdTimer){clearInterval(_cdTimer);_cdTimer=null;}
      setPill('blue','Live · '+BRIDGE.replace('http://',''));
      refreshAll();
    };
    ws.onmessage=e=>{
      try{ const msg=JSON.parse(e.data); if(msg.type==='status') applyStatus(msg.data); }catch(e){}
    };
    ws.onclose=()=>{
      wsActive=false;
      if(_cdTimer) clearInterval(_cdTimer);
      let secs=5;
      _cdTimer=setInterval(()=>{
        setPill('gray',`Reconnecting in ${secs}s…`);
        secs--;
        if(secs<0){clearInterval(_cdTimer);_cdTimer=null;if(!wsActive)connectBridge();}
      },1000);
    };
    ws.onerror=()=>{
      wsActive=false;
      setPill('red','Error — Check bridge URL');
      showBridgeError(`Cannot reach ${BRIDGE}\n\nMake sure:\n1. Python bridge is running: python bridge/main.py\n2. Port 8000 is open\n3. URL is correct (VPS IP or localhost)`);
    };
  }catch(err){
    setPill('red','Failed');
    showBridgeError(err.message);
  }
}

function setPill(color,label){
  const dot=document.getElementById('ws-dot'), lbl=document.getElementById('ws-label');
  dot.className=`dot dot-${color}`;
  lbl.textContent=label;
  lbl.style.color=color==='blue'?'var(--blue-bright)':color==='red'?'var(--red)':'var(--text3)';
}

function showBridgeError(msg){
  const el=document.getElementById('bridge-error');
  el.style.display='block';
  el.style.whiteSpace='pre-wrap';
  el.textContent=msg;
}
function hideBridgeError(){ document.getElementById('bridge-error').style.display='none'; }

// ══════════════════════════════════════════════════════════════════════
// STATUS
// ══════════════════════════════════════════════════════════════════════
function applyStatus(s){
  mastersData=s.masters||[];
  slavesData=s.slaves||[];
  document.getElementById('h-masters').textContent=`${s.masters_connected}/${s.masters_total}`;
  document.getElementById('h-slaves').textContent=`${s.slaves_connected}/${s.slaves_total}`;
  document.getElementById('h-copied').textContent=s.trades_copied_today??0;
  const lat=s.avg_latency_ms??0;
  document.getElementById('h-lat').textContent=lat?`${lat.toFixed(1)}ms`:'—';
  document.getElementById('s-masters').textContent=`${s.masters_connected}/${s.masters_total}`;
  document.getElementById('s-slaves').textContent=`${s.slaves_connected}/${s.slaves_total}`;
  document.getElementById('s-copied').textContent=s.trades_copied_today??0;
  document.getElementById('s-failed').textContent=s.trades_failed_today??0;
  const latEl=document.getElementById('s-lat');
  latEl.textContent=lat?`${lat.toFixed(1)}ms`:'—';
  latEl.style.color=lat<50?'var(--green)':lat<100?'var(--amber)':'var(--red)';
  renderMasters(); renderSlaves(); updateLogMasterFilter();
}

async function refreshAll(){
  try{
    const s=await apiFetch('/status'); applyStatus(s);
    const logs=await apiFetch('/logs?limit=400'); localLogs=logs; renderLog();
    const sm=await apiFetch('/symbol-map'); symMap=sm.global||{}; renderSymMap();
    hideBridgeError();
  }catch(e){
    setPill('red','Unreachable');
    showBridgeError(`Cannot reach ${BRIDGE}\n\nThe Python bridge server is not running or the URL is wrong.\n\nStart it with: python bridge/main.py`);
  }
}

// ══════════════════════════════════════════════════════════════════════
// RENDER MASTERS
// ══════════════════════════════════════════════════════════════════════
let _mastersSig='';
function renderMasters(){
  const el=document.getElementById('masters-grid');
  document.getElementById('master-count').textContent=`${mastersData.length} master${mastersData.length!==1?'s':''}`;
  mastersData.forEach(m=>{
    const k='M_'+m.master_id;
    if(!equityHistory[k]) equityHistory[k]=[];
    equityHistory[k].push(m.equity??0);
    if(equityHistory[k].length>30) equityHistory[k].shift();
  });
  // Skip the full innerHTML rebuild when nothing that affects the DOM changed —
  // rebuilding every 2s status push otherwise destroys hover/focus state.
  const sig=JSON.stringify(mastersData.map(m=>[m.master_id,m.label,m.connection_status,m.equity,m.balance,m.trades_today,m.linked_slaves,m.error]))
           +'|'+JSON.stringify(slavesData.map(s=>[s.account_id,s.label,s.connection_status,s.equity,s.server]));
  if(sig===_mastersSig && el.childElementCount) return;
  _mastersSig=sig;
  if(!mastersData.length){
    el.innerHTML=`<div class="empty-state"><div class="empty-icon">⬆</div><div class="empty-title">No Masters Registered</div><div class="empty-sub">Add a master account from the sidebar</div></div>`;
    return;
  }
  el.innerHTML=mastersData.map(m=>{
    const conn=m.connection_status==='connected', err=m.connection_status==='error';
    const dotCls=conn?'dot-blue':err?'dot-red':'dot-gray';
    const cardCls=conn?'connected':err?'error':'';
    const linked=m.linked_slaves||[];
    const chips=linked.map(sid=>{
      const s=slavesData.find(x=>x.account_id===sid);
      if(!s) return '';
      const sc=s.connection_status==='connected';
      return `<div class="sub-chip">
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="dot ${sc?'dot-blue':'dot-gray'}"></span>
          <div><div class="sub-chip-label">${esc(s.label)}</div><div class="sub-chip-server">${esc(s.server)}</div></div>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="sub-chip-eq">$${fmt(s.equity)}</span>
          <button class="chip-unlink" onclick="unlinkSlave('${m.master_id}','${sid}')" title="Unlink">✕</button>
        </div>
      </div>`;
    }).join('');
    return `<div class="master-card ${cardCls}">
      <div class="mc-head">
        <div style="display:flex;align-items:flex-start;gap:10px;">
          <span class="dot ${dotCls}" style="margin-top:4px;"></span>
          <div>
            <div class="mc-label">${esc(m.label)}</div>
            <div class="mc-meta">${err?`<span style="color:var(--red)">${esc(m.error||'Connection error')}</span>`:m.last_ping?'Ping '+relTime(m.last_ping):'Pending connection'}</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:7px;flex-shrink:0;">
          <span class="mc-magic">magic:${m.magic_number}</span>
          <button class="copy-btn" onclick="copyToClipboard('${m.magic_number}','Magic number')" title="Copy magic number">⎘</button>
          <button class="btn btn-danger btn-xs" onclick="removeMaster('${m.master_id}','${esc(m.label)}')">Remove</button>
        </div>
      </div>
      <div class="mc-stats">
        <div class="mst"><div class="mst-l">Equity</div><div class="mst-v" style="color:var(--blue-bright);display:flex;align-items:center;">$${fmt(m.equity)}${sparkline('M_'+m.master_id,80,24)}</div></div>
        <div class="mst"><div class="mst-l">Balance</div><div class="mst-v">$${fmt(m.balance)}</div></div>
        <div class="mst"><div class="mst-l">Trades Today</div><div class="mst-v" style="color:var(--green)">${m.trades_today??0}</div></div>
      </div>
      <div class="subs-head">
        <span class="subs-label">Subscribers (${linked.length})</span>
        <button class="btn btn-ghost btn-xs" onclick="openLinkModal('${m.master_id}','${esc(m.label)}')">+ Link Slave</button>
      </div>
      <div class="subs-list">
        ${linked.length?chips:'<div class="no-subs">No slaves linked — click + Link Slave to add</div>'}
      </div>
    </div>`;
  }).join('');
}

// ══════════════════════════════════════════════════════════════════════
// RENDER SLAVES
// ══════════════════════════════════════════════════════════════════════
let _slavesSig='';
function renderSlaves(){
  const el=document.getElementById('slave-list');
  document.getElementById('slave-count').textContent=`${slavesData.length}`;
  slavesData.forEach(s=>{
    const k='S_'+s.account_id;
    if(!equityHistory[k]) equityHistory[k]=[];
    equityHistory[k].push(s.equity??0);
    if(equityHistory[k].length>30) equityHistory[k].shift();
  });
  const sig=JSON.stringify(slavesData.map(s=>[s.account_id,s.label,s.connection_status,s.equity,s.server,s.master_ids,s.open_trades,s.error,s.lot_sizing_mode]))
           +'|'+JSON.stringify(mastersData.map(m=>[m.master_id,m.label]));
  if(sig===_slavesSig && el.childElementCount) return;
  _slavesSig=sig;
  if(!slavesData.length){
    el.innerHTML=`<div class="empty-state" style="padding:28px 12px;"><div class="empty-icon">⬇</div><div class="empty-title">No Slaves</div><div class="empty-sub">Add slave accounts via the sidebar</div></div>`;
    return;
  }
  el.innerHTML=slavesData.map(s=>{
    const conn=s.connection_status==='connected', err=s.connection_status==='error';
    const dotCls=conn?'dot-blue':err?'dot-red':'dot-gray';
    const mids=s.master_ids||[];
    const badges=mids.map(mid=>{ const m=mastersData.find(x=>x.master_id===mid); return m?`<span class="mbadge">${esc(m.label)}</span>`:''; }).join('');
    const mode=s.lot_sizing_mode==='equity_ratio'?'EqRatio':s.lot_sizing_mode==='fixed'?'Fixed':'Mult';
    return `<div class="slave-row">
      <span class="dot ${dotCls}"></span>
      <div class="slave-row-info">
        <div class="slave-row-label">${esc(s.label)}</div>
        <div class="slave-row-meta">${esc(s.server)} · <span class="tag tag-gray" style="font-size:9px;padding:0 4px;">${mode}</span></div>
        <div class="slave-row-eq" style="display:flex;align-items:center;gap:2px;">$${fmt(s.equity)}${sparkline('S_'+s.account_id,60,18)}</div>
        ${mids.length?`<div class="mbadges">${badges}</div>`:'<div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-top:2px;">Unlinked</div>'}
        ${s.open_trades?`<div style="font-family:var(--mono);font-size:9px;color:var(--text2);margin-top:2px;">${s.open_trades} open</div>`:''}
        ${err?`<div style="font-family:var(--mono);font-size:9px;color:var(--red);margin-top:2px;">${esc(s.error||'Error')}</div>`:''}
      </div>
      <button class="btn btn-danger btn-xs" onclick="removeSlave('${s.account_id}','${esc(s.label)}')">✕</button>
    </div>`;
  }).join('');
}

// ══════════════════════════════════════════════════════════════════════
// LOG WITH MASTER FILTER
// ══════════════════════════════════════════════════════════════════════
function updateLogMasterFilter(){
  const sel=document.getElementById('log-master-filter');
  const cur=sel.value;
  sel.innerHTML='<option value="">All Masters</option>'+
    mastersData.map(m=>`<option value="${m.master_id}">${esc(m.label)}</option>`).join('');
  sel.value=cur;
}

function renderLog(){
  const el=document.getElementById('log-entries');
  const masterFilter=document.getElementById('log-master-filter').value;
  const searchQ=(document.getElementById('log-search')?.value||'').toLowerCase().trim();
  let filtered=logTab==='error'?localLogs.filter(l=>l.level==='ERROR'):localLogs;
  if(masterFilter) filtered=filtered.filter(l=>l.master_id===masterFilter);
  if(searchQ) filtered=filtered.filter(l=>(l.message||'').toLowerCase().includes(searchQ)||(l.symbol||'').toLowerCase().includes(searchQ));
  if(!filtered.length){
    el.innerHTML=`<div style="text-align:center;padding:16px;color:var(--text3);font-size:11px;">No log entries${masterFilter?' for this master':''}${searchQ?' matching "'+searchQ+'"':''}</div>`;
    return;
  }
  const atTop = el.scrollTop < 20;
  el.innerHTML=filtered.slice(-400).reverse().map(l=>{
    const ts=new Date(l.timestamp).toISOString().slice(11,23);
    const mLabel=l.master_id?(mastersData.find(m=>m.master_id===l.master_id)?.label||l.master_id):'';
    const sLabel=l.account_id?(slavesData.find(s=>s.account_id===l.account_id)?.label||l.account_id):'';
    const latHtml=l.latency_ms!=null?`<span class="lat-chip ${l.latency_ms<50?'lat-g':l.latency_ms<100?'lat-o':'lat-r'}">${l.latency_ms.toFixed(0)}ms</span>`:'';
    return `<div class="log-row ${l.level}">
      <span class="log-ts">${ts}</span>
      <span class="log-lvl ${l.level}">${l.level}</span>
      ${mLabel?`<span class="log-master">[${esc(mLabel)}]</span>`:''}
      ${sLabel?`<span class="log-slave">${esc(sLabel)}</span>`:''}
      <span class="log-msg">${esc(l.message)}</span>
      ${latHtml}
    </div>`;
  }).join('');
  if(atTop) el.scrollTop = 0;
}

function setLogTab(t){
  logTab=t;
  const all=document.getElementById('tab-all'), err=document.getElementById('tab-error');
  all.style.borderColor=t==='all'?'var(--blue)':'var(--border)';
  all.style.color=t==='all'?'var(--blue)':'var(--text2)';
  err.style.borderColor=t==='error'?'var(--red)':'var(--border)';
  err.style.color=t==='error'?'var(--red)':'var(--text2)';
  renderLog();
}
function clearLocalLog(){
  if(!localLogs.length) return;
  if(!confirm('Clear all log entries? This cannot be undone.')) return;
  localLogs=[]; renderLog();
}

function exportLogCSV(){
  if(!localLogs.length) return toast('No log entries to export','error');
  const escCSV=v=>{const s=String(v??'');return s.includes(',')||s.includes('"')||s.includes('\n')?'"'+s.replace(/"/g,'""')+'"':s;};
  const rows=localLogs.map(l=>{
    const mLabel=l.master_id?(mastersData.find(m=>m.master_id===l.master_id)?.label||l.master_id):'';
    const sLabel=l.account_id?(slavesData.find(s=>s.account_id===l.account_id)?.label||l.account_id):'';
    return [l.timestamp||l.time||'',l.level||'',mLabel,sLabel,l.message||'',l.latency_ms??''].map(escCSV).join(',');
  });
  const csv=['time,level,master,slave,message,latency_ms',...rows].join('\r\n');
  const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url; a.download=`omniroute-log-${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
  toast(`Exported ${localLogs.length} entries as CSV`,'success');
}

// ══════════════════════════════════════════════════════════════════════
// SYMBOL MAP
// ══════════════════════════════════════════════════════════════════════
function renderSymMap(){
  const el=document.getElementById('sym-list');
  const entries=Object.entries(symMap);
  if(!entries.length){ el.innerHTML='<span style="font-family:var(--mono);font-size:10px;color:var(--text3)">No mappings yet</span>'; return; }
  el.innerHTML=entries.map(([k,v])=>`
    <div style="display:flex;align-items:center;justify-content:space-between;background:var(--bg3);border:1px solid var(--border);border-radius:5px;padding:5px 9px;">
      <span style="font-family:var(--mono);font-size:11px;color:var(--blue-bright)">${esc(k)} → ${esc(v)}</span>
      <button onclick="removeSymMap('${k}')" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:12px;">✕</button>
    </div>`).join('');
}
async function addSymMap(){
  const from=document.getElementById('sym-from').value.trim().toUpperCase();
  const to=document.getElementById('sym-to').value.trim().toUpperCase();
  if(!from||!to) return toast('Fill both symbol fields','error');
  symMap[from]=to; renderSymMap();
  try{ await apiFetch('/symbol-map','POST',symMap); toast(`Mapped ${from} → ${to}`,'info'); }
  catch(e){ toast('Failed: '+e.message,'error'); }
  document.getElementById('sym-from').value='';
  document.getElementById('sym-to').value='';
}
async function removeSymMap(key){ delete symMap[key]; renderSymMap(); try{ await apiFetch('/symbol-map','POST',symMap); }catch(e){} }

// ══════════════════════════════════════════════════════════════════════
// ADD ACCOUNT
// ══════════════════════════════════════════════════════════════════════
async function addAccount(){
  const label=document.getElementById('f-label').value.trim();
  const login=parseInt(document.getElementById('f-login').value);
  const pass=document.getElementById('f-pass').value;
  const server=document.getElementById('f-server').value.trim();
  const path=document.getElementById('f-path').value.trim()||'C:\\Program Files\\MetaTrader 5\\terminal64.exe';
  if(!label||!login||!pass||!server) return toast('Fill all required fields (*)','error');
  if(!wsActive&&!BRIDGE) return toast('Connect to bridge first','error');
  const payload={role:currentRole,label,login,password:pass,server,terminal_path:path};
  if(currentRole==='master'){
    const magic=parseInt(document.getElementById('f-magic').value);
    if(!magic) return toast('Magic number is required for master','error');
    payload.magic_number=magic;
  } else {
    payload.lot_sizing_mode=document.getElementById('f-lot-mode').value;
    payload.max_lot=parseFloat(document.getElementById('f-maxlot').value)||10.0;
    payload.multiplier=parseFloat(document.getElementById('f-mult').value)||1.0;
  }
  const btn = document.getElementById('add-account-btn');
  await withBtn(btn, 'Adding…', async () => {
    await apiFetch('/account','POST',payload);
    toast(`${currentRole==='master'?'Master':'Slave'} "${label}" added ✓`,'success');
    ['f-label','f-login','f-pass','f-server','f-path','f-magic','f-maxlot','f-mult'].forEach(id=>{ const e=document.getElementById(id); if(e) e.value=''; });
    await refreshAll();
  });
}

// ══════════════════════════════════════════════════════════════════════
// REMOVE
// ══════════════════════════════════════════════════════════════════════
function removeMaster(id,label){
  showConfirm(`Remove master "${label}"? All slave links will be removed.`,async()=>{
    try{ await apiFetch(`/masters/${id}`,'DELETE'); toast(`Master "${label}" removed`,'info'); await refreshAll(); }
    catch(e){ toast('Remove failed: '+e.message,'error'); }
  });
}
function removeSlave(id,label){
  showConfirm(`Remove slave "${label}"?`,async()=>{
    try{ await apiFetch(`/slaves/${id}`,'DELETE'); toast(`Slave "${label}" removed`,'info'); await refreshAll(); }
    catch(e){ toast('Remove failed: '+e.message,'error'); }
  });
}

// ══════════════════════════════════════════════════════════════════════
// LINK / UNLINK
// ══════════════════════════════════════════════════════════════════════
function openLinkModal(masterId,masterLabel){
  _linkMasterId=masterId;
  document.getElementById('link-master-name').textContent=masterLabel;
  const master=mastersData.find(m=>m.master_id===masterId);
  const linked=master?master.linked_slaves||[]:[]; 
  const listEl=document.getElementById('link-slave-list');
  if(!slavesData.length){
    listEl.innerHTML='<div style="text-align:center;padding:20px;color:var(--text3);font-family:var(--mono);font-size:11px;">No slave accounts yet. Add slaves from the sidebar.</div>';
  } else {
    listEl.innerHTML=slavesData.map(s=>{
      const already=linked.includes(s.account_id);
      const conn=s.connection_status==='connected';
      return `<div class="link-slave-item ${already?'linked':''}" ${already?'':`onclick="linkSlave('${s.account_id}')"`}>
        <div style="display:flex;align-items:center;gap:10px;">
          <span class="dot ${conn?'dot-blue':'dot-gray'}"></span>
          <div>
            <div style="font-size:13px;font-weight:500;">${esc(s.label)}</div>
            <div style="font-family:var(--mono);font-size:10px;color:var(--text3);">${esc(s.server)} · $${fmt(s.equity)}</div>
          </div>
        </div>
        ${already?'<span class="tag tag-blue" style="font-size:9px;">Linked</span>':'<span style="font-family:var(--mono);font-size:10px;color:var(--text3);">Click to link</span>'}
      </div>`;
    }).join('');
  }
  document.getElementById('link-modal').classList.remove('hidden');
}

async function linkSlave(slaveId){
  if(!_linkMasterId) return;
  try{
    await apiFetch('/link','POST',{master_id:_linkMasterId,account_id:slaveId});
    const s=slavesData.find(x=>x.account_id===slaveId), m=mastersData.find(x=>x.master_id===_linkMasterId);
    toast(`"${s?.label}" linked to "${m?.label}" ✓`,'success');
    closeModal('link-modal');
    await refreshAll();
  }catch(e){ toast('Link failed: '+e.message,'error'); }
}

async function unlinkSlave(masterId,slaveId){
  const s=slavesData.find(x=>x.account_id===slaveId), m=mastersData.find(x=>x.master_id===masterId);
  try{
    await apiFetch('/unlink','POST',{master_id:masterId,account_id:slaveId});
    toast(`"${s?.label}" unlinked from "${m?.label}"`,'info');
    await refreshAll();
  }catch(e){ toast('Unlink failed: '+e.message,'error'); }
}

// ══════════════════════════════════════════════════════════════════════
// MODALS
// ══════════════════════════════════════════════════════════════════════
function closeModal(id){ document.getElementById(id).classList.add('hidden'); }
function openModal(id){ document.getElementById(id).classList.remove('hidden'); }
document.querySelectorAll('.modal-backdrop').forEach(el=>{ el.addEventListener('click',e=>{ if(e.target===el) el.classList.add('hidden'); }); });
function showConfirm(msg,cb){
  document.getElementById('confirm-msg').textContent=msg;
  _confirmCb=cb;
  document.getElementById('confirm-modal').classList.remove('hidden');
}
document.getElementById('confirm-ok').addEventListener('click',()=>{ closeModal('confirm-modal'); if(_confirmCb){_confirmCb();_confirmCb=null;} });

// ══════════════════════════════════════════════════════════════════════
// TOASTS
// ══════════════════════════════════════════════════════════════════════
function toast(msg,type='success'){
  const el=document.createElement('div');
  const icon=type==='success'?'✓':type==='error'?'✕':'ℹ';
  el.className=`toast ${type}`;
  // Use textContent for the message — it can contain raw server error strings.
  const iconEl=document.createElement('span'); iconEl.textContent=icon;
  const msgEl=document.createElement('span'); msgEl.textContent=msg;
  el.append(iconEl,msgEl);
  document.getElementById('toasts').appendChild(el);
  setTimeout(()=>el.remove(),3800);
}

// ══════════════════════════════════════════════════════════════════════
// API HELPER
// ══════════════════════════════════════════════════════════════════════
async function apiFetch(path,method='GET',body=null){
  const opts={method,headers:{'Content-Type':'application/json'}};
  const key=document.getElementById('cfg-apikey')?.value;
  if(key) opts.headers['X-API-Key']=key;
  if(body) opts.body=JSON.stringify(body);
  const r=await fetch(BRIDGE+path,opts);
  if(!r.ok){ const t=await r.text(); throw new Error(`HTTP ${r.status}: ${t.slice(0,120)}`); }
  return r.json();
}

// ══════════════════════════════════════════════════════════════════════
// UTILS
// ══════════════════════════════════════════════════════════════════════
function esc(s){ if(!s) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function fmt(n){ return n!=null?Number(n).toLocaleString('en',{minimumFractionDigits:2,maximumFractionDigits:2}):'0.00'; }
function relTime(iso){ const s=Math.floor((Date.now()-new Date(iso))/1000); if(s<5) return 'just now'; if(s<60) return `${s}s ago`; if(s<3600) return `${Math.floor(s/60)}m ago`; return `${Math.floor(s/3600)}h ago`; }

async function withBtn(btn, loadingText, fn) {
  if (!btn || btn.disabled) return;
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = loadingText;
  try { await fn(); } finally { btn.disabled = false; btn.textContent = orig; }
}

function togglePw(inputId, btn) {
  const inp = document.getElementById(inputId);
  if (!inp) return;
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.textContent = show ? '🙈' : '👁';
}

function copyToClipboard(text, label) {
  navigator.clipboard.writeText(text).then(()=>toast(`${label||'Value'} copied`,'success')).catch(()=>{
    const ta=document.createElement('textarea'); ta.value=text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); toast(`${label||'Value'} copied`,'success');
  });
}

const _sliderDebounce = {};
function debouncedRefreshSummary(sid) {
  clearTimeout(_sliderDebounce[sid]);
  _sliderDebounce[sid] = setTimeout(() => refreshSummary(sid), 120);
}

function sparkline(key,w,h){
  const pts=equityHistory[key]||[];
  if(pts.length<2) return '<span style="color:var(--text3);font-size:10px;opacity:.5;margin-left:6px">—</span>';
  const mn=Math.min(...pts), mx=Math.max(...pts), range=mx-mn||1;
  const xStep=w/(pts.length-1);
  const coords=pts.map((v,i)=>{
    const x=i*xStep;
    const y=h-((v-mn)/range)*(h-2)-1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const color=pts[pts.length-1]>pts[0]?'var(--green)':pts[pts.length-1]<pts[0]?'var(--red)':'var(--text3)';
  return `<svg class="sparkline" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><polyline points="${coords}" style="fill:none;stroke:${color};stroke-width:1.5;stroke-linejoin:round;stroke-linecap:round"/></svg>`;
}

// ══════════════════════════════════════════════════════════════════════
// MAIN TAB SWITCHING
// ══════════════════════════════════════════════════════════════════════
function switchMainTab(tab) {
  ['dashboard','protection','analytics','bots','settings'].forEach(t => {
    document.getElementById('maintab-'+t).classList.toggle('active', t===tab);
    document.getElementById('tabview-'+t).classList.toggle('active', t===tab);
  });
  if (tab==='settings')   refreshSettingsPage();
  if (tab==='protection') refreshProtection();
  if (tab==='analytics')  renderAnalytics();
  if (tab==='bots')       refreshBotsTab();
  clearInterval(_botsPollTimer);
  if (tab==='bots') _botsPollTimer = setInterval(refreshBotsTab, 10000);
}

// ══════════════════════════════════════════════════════════════════════
// SETTINGS PAGE
// ══════════════════════════════════════════════════════════════════════
function refreshSettingsPage() {
  document.getElementById('settings-bridge-url').textContent = BRIDGE;
  const wsOk = wsActive;
  const wsEl = document.getElementById('settings-ws-status');
  wsEl.textContent = wsOk ? 'Connected' : 'Disconnected';
  wsEl.className   = wsOk ? 'tag tag-green' : 'tag tag-red';
  loadTelegramConfig();
  // Uptime from last status
  const up = window._lastUptime || 0;
  const h=Math.floor(up/3600), m=Math.floor((up%3600)/60), s=Math.floor(up%60);
  document.getElementById('settings-uptime').textContent =
    up ? `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}` : '—';
}

// ══════════════════════════════════════════════════════════════════════
// TELEGRAM CONFIG
// ══════════════════════════════════════════════════════════════════════
async function loadTelegramConfig() {
  try {
    const cfg = await apiFetch('/telegram/config');
    document.getElementById('tg-master-toggle').checked = cfg.enabled;
    updateTgStatusUI(cfg);
  } catch(e) { /* bridge offline */ }
}

function updateTgStatusUI(cfg) {
  const badge = document.getElementById('tg-status-badge');
  const row   = document.getElementById('tg-status-row');
  const title = document.getElementById('tg-status-title');
  const sub   = document.getElementById('tg-status-sub');

  if (!cfg.configured) {
    badge.textContent = 'Not configured'; badge.className = 'tag tag-gray';
    row.style.background = 'rgba(74,88,120,.1)';
    row.style.borderColor = 'var(--border)';
    title.textContent = 'Not Configured';
    sub.textContent   = 'Enter your Bot Token and Chat ID below to enable';
    document.getElementById('tg-status-row').querySelector('.tg-status-icon').textContent = '🔘';
  } else if (!cfg.enabled) {
    badge.textContent = 'Paused'; badge.className = 'tag tag-amber';
    row.style.background = 'rgba(255,173,13,.06)';
    row.style.borderColor = 'rgba(255,173,13,.25)';
    title.textContent = 'Paused';
    sub.textContent = 'Toggle the switch to resume notifications';
    document.getElementById('tg-status-row').querySelector('.tg-status-icon').textContent = '⏸️';
  } else {
    badge.textContent = 'Active'; badge.className = 'tag tag-green';
    row.style.background = 'rgba(0,214,143,.06)';
    row.style.borderColor = 'rgba(0,214,143,.2)';
    title.textContent = 'Notifications Active';
    sub.textContent   = `Sending to chat ${cfg.token_preview}`;
    document.getElementById('tg-status-row').querySelector('.tg-status-icon').textContent = '✅';
  }
}

async function saveTelegramConfig() {
  const token  = document.getElementById('tg-token').value.trim();
  const chatId = document.getElementById('tg-chatid').value.trim();
  if (!token && !chatId) return toast('Enter bot token and chat ID','error');
  const btn = document.getElementById('tg-save-btn');
  await withBtn(btn, 'Saving…', async () => {
    const payload = {};
    if (token)  payload.bot_token = token;
    if (chatId) payload.chat_id   = chatId;
    await apiFetch('/telegram/config','PATCH', payload);
    toast('Telegram config saved ✓','success');
    document.getElementById('tg-token').value = '';
    loadTelegramConfig();
  });
}

async function toggleTelegram(enabled) {
  try {
    await apiFetch('/telegram/toggle?enabled='+enabled,'PATCH');
    toast(`Telegram notifications ${enabled?'enabled':'paused'}`,'info');
    loadTelegramConfig();
  } catch(e) {
    toast('Toggle failed: '+e.message,'error');
    document.getElementById('tg-master-toggle').checked = !enabled;
  }
}

async function testTelegram() {
  const banner = document.getElementById('tg-test-banner');
  const btn = document.getElementById('tg-test-btn');
  banner.style.display = 'none';
  if (btn) { btn.disabled = true; btn.textContent = 'Sending…'; }
  try {
    await apiFetch('/telegram/test','POST');
    banner.className = 'test-banner ok';
    banner.textContent = '✓ Test message sent! Check your Telegram.';
    banner.style.display = 'block';
    toast('Test message sent!','success');
  } catch(e) {
    banner.className = 'test-banner fail';
    banner.textContent = '✕ '+e.message;
    banner.style.display = 'block';
    toast('Test failed: '+e.message,'error');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Send Test Message'; }
  }
}

function saveEventPrefs() {
  // Store in localStorage for persistence (UI-only preference, bridge sends all events)
  const prefs = {
    detected:    document.getElementById('evt-detected').checked,
    copied:      document.getElementById('evt-copied').checked,
    failed:      document.getElementById('evt-failed').checked,
    slaveConn:   document.getElementById('evt-slave-conn').checked,
    masterConn:  document.getElementById('evt-master-conn').checked,
    bridge:      document.getElementById('evt-bridge').checked,
  };
  try { localStorage.setItem('omni_tg_prefs', JSON.stringify(prefs)); } catch(e) {}
  toast('Preferences saved','info');
}

function loadEventPrefs() {
  try {
    const prefs = JSON.parse(localStorage.getItem('omni_tg_prefs') || '{}');
    if ('detected'   in prefs) document.getElementById('evt-detected').checked   = prefs.detected;
    if ('copied'     in prefs) document.getElementById('evt-copied').checked     = prefs.copied;
    if ('failed'     in prefs) document.getElementById('evt-failed').checked     = prefs.failed;
    if ('slaveConn'  in prefs) document.getElementById('evt-slave-conn').checked = prefs.slaveConn;
    if ('masterConn' in prefs) document.getElementById('evt-master-conn').checked= prefs.masterConn;
    if ('bridge'     in prefs) document.getElementById('evt-bridge').checked     = prefs.bridge;
  } catch(e) {}
}

// ══════════════════════════════════════════════════════════════════════
// TELEGRAM MESSAGE PREVIEW
// ══════════════════════════════════════════════════════════════════════
const PREVIEWS = {
  detected: `<b>📡 OmniRoute — Signal Detected</b>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<b>Master:</b>  Master A
<b>Magic:</b>   <code>111</code>
<b>Signal:</b>  <code>a1b2c3d4</code>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<span class="tg-green">🟢 BUY</span>  <b>EURUSD</b>
<b>Volume:</b>  <code>0.10 lots</code>
<b>Price:</b>   <code>1.08520</code>
<b>SL:</b>      <code>1.08000</code>
<b>TP:</b>      <code>1.09500</code>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<i>2025-01-15 14:32:07 UTC</i>`,

  copied: `<b>✅ OmniRoute — Trade Copied</b>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<b>Slave:</b>   Slave 1 — ICMarkets
<b>Account:</b> <code>ACC001</code>
<b>Master:</b>  Master A
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<span class="tg-green">🟢 BUY</span>  <b>EURUSD</b>
<b>Volume:</b>  <code>0.08 lots</code>
<b>Price:</b>   <code>1.08523</code>
<b>Ticket:</b>  <code>#100847</code>
<b>Signal:</b>  <code>a1b2c3d4</code>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
⚡ Latency: <code>23.4ms</code>
<i>2025-01-15 14:32:07 UTC</i>`,

  failed: `<b>❌ OmniRoute — Copy Failed</b>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<b>Slave:</b>   Slave 3 — Pepperstone
<b>Account:</b> <code>ACC003</code>
<b>Master:</b>  Master B
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<b>Symbol:</b>  XAUUSD
<b>Action:</b>  BUY
<b>Signal:</b>  <code>x9y8z7w6</code>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
⚠️ <b>Error:</b> <span class="tg-red">Market closed</span> (code <code>10018</code>)
<i>2025-01-15 14:32:08 UTC</i>`,

  conn: `<b>🔗 OmniRoute — Slave Connected</b>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<b>Slave:</b>   Slave 2 — Exness
<b>Account:</b> <code>ACC002</code>
<b>Server:</b>  Exness-MT5Real
<b>Equity:</b>  <code>$8,450.00</code>
<span class="tg-divider">━━━━━━━━━━━━━━━━━━━━</span>
<i>2025-01-15 14:30:01 UTC</i>`,
};

function showPreview(type) {
  const el = document.getElementById('tg-preview');
  el.innerHTML = PREVIEWS[type] || '';
}

// ══════════════════════════════════════════════════════════════════════
// PROTECTION PAGE
// ══════════════════════════════════════════════════════════════════════
const protState = {};

function refreshProtection() { renderProtectionCards(); }

function renderProtectionCards() {
  const grid = document.getElementById('prot-slave-grid');
  if (!slavesData.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">🛡</div><div class="empty-title">No Slave Accounts</div><div class="empty-sub">Add slaves from the Dashboard tab first</div></div>';
    return;
  }
  grid.innerHTML = slavesData.map(s => buildProtCard(s)).join('');
}

function buildProtCard(slave) {
  const sid = slave.account_id;
  const p   = Object.assign({}, slave.protection || {});
  const slipEnabled = p.slippage_enabled  !== false;
  const slipMax     = p.slippage_max      ?? 3.0;
  const slipMode    = p.slippage_mode     ?? 'points';
  const slipAction  = p.slippage_action   ?? 'cancel';
  const riskMult    = p.risk_multiplier   ?? 1.0;
  const riskMax     = p.risk_max_lot      ?? 10.0;
  const riskMin     = p.risk_min_lot      ?? 0.01;
  const riskLabel   = p.risk_profile_label ?? 'default';
  const sltpEnabled = p.sltp_sync_enabled !== false;
  const sltpMode    = p.sltp_sync_mode    ?? 'full';
  const sltpSL      = p.sltp_scale_sl     ?? 1.0;
  const sltpTP      = p.sltp_scale_tp     ?? 1.0;
  const offSL       = p.sltp_offset_sl    ?? 0;
  const offTP       = p.sltp_offset_tp    ?? 0;
  const conn = slave.connection_status === 'connected';

  const slipChip = slipEnabled
    ? `<span class="prot-chip prot-chip-slip">🛡 ${slipMax}${slipMode[0]} slip</span>`
    : `<span class="prot-chip prot-chip-off">slip off</span>`;
  const lotChip  = `<span class="prot-chip prot-chip-lot">×${riskMult} lot</span>`;
  const sltpChip = sltpEnabled
    ? `<span class="prot-chip prot-chip-sltp">SL/TP ${sltpMode.replace('_',' ')}</span>`
    : `<span class="prot-chip prot-chip-off">no sync</span>`;

  const syncOpts = [['full','Both SL+TP'],['sl_only','SL Only'],['tp_only','TP Only'],['none','Disabled']];
  const slipModes = ['points','pips','percent'];

  return `<div class="prot-card" id="pc-${sid}">
    <div class="prot-card-head">
      <div class="prot-card-title">
        <span class="dot ${conn?'dot-blue':'dot-gray'}"></span>${esc(slave.label)}
      </div>
      <span class="tag tag-gray" style="font-size:9px;">${esc(slave.server||'')}</span>
    </div>
    <div class="prot-card-body">
      <div class="prot-summary">${slipChip}${lotChip}${sltpChip}</div>
      <div style="font-family:var(--mono);font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px;">Quick Preset</div>
      <div class="preset-grid">
        <button class="preset-btn ultra_safe"   onclick="applyPreset('${sid}','ultra_safe')">🟢 Ultra Safe</button>
        <button class="preset-btn conservative" onclick="applyPreset('${sid}','conservative')">🔵 Conservative</button>
        <button class="preset-btn default"      onclick="applyPreset('${sid}','default')">⬜ Default</button>
        <button class="preset-btn aggressive"   onclick="applyPreset('${sid}','aggressive')">🟠 Aggressive</button>
        <button class="preset-btn no_protection" onclick="applyPreset('${sid}','no_protection')">🔴 Off</button>
      </div>

      <div class="prot-feature">
        <div class="prot-feature-head">
          <div class="prot-feature-title">🛡 Slippage Protection</div>
          <label class="toggle"><input type="checkbox" id="slip-en-${sid}" ${slipEnabled?'checked':''}
            onchange="updateProtField('${sid}','slippage_enabled',this.checked);refreshSummary('${sid}')"/>
            <div class="toggle-track"><div class="toggle-thumb"></div></div></label>
        </div>
        <div class="slider-row">
          <span class="slider-lbl">Max deviation</span>
          <input type="range" class="slider-input" id="slip-max-${sid}" min="0.5" max="50" step="0.5" value="${slipMax}"
            oninput="document.getElementById('sv-slip-${sid}').textContent=parseFloat(this.value);updateProtField('${sid}','slippage_max',parseFloat(this.value));debouncedRefreshSummary('${sid}')"/>
          <span class="slider-val" id="sv-slip-${sid}">${slipMax}</span>
        </div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:5px;">Unit:</div>
        <div class="mode-pills">${slipModes.map(m=>`<span class="mode-pill ${slipMode===m?'active':''}" onclick="setSlipMode('${sid}','${m}')">${m}</span>`).join('')}</div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:4px;">If exceeded:</div>
        <div class="action-row">
          <div class="action-opt ${slipAction==='cancel'?'active-cancel':''}" id="act-cancel-${sid}"
            onclick="setSlipAction('${sid}','cancel')">✕ Cancel trade</div>
          <div class="action-opt ${slipAction==='execute_anyway'?'active-anyway':''}" id="act-anyway-${sid}"
            onclick="setSlipAction('${sid}','execute_anyway')">⚠ Warn, execute anyway</div>
        </div>
      </div>

      <div class="prot-feature">
        <div class="prot-feature-head">
          <div class="prot-feature-title">⚖ Lot Scaling</div>
          <span class="tag tag-blue" style="font-size:9px;" id="lot-label-${sid}">${esc(riskLabel)}</span>
        </div>
        <div class="slider-row">
          <span class="slider-lbl">Risk multiplier</span>
          <input type="range" class="slider-input" id="risk-mult-${sid}" min="0.1" max="5" step="0.1" value="${riskMult}"
            oninput="document.getElementById('sv-mult-${sid}').textContent=parseFloat(this.value)+'×';updateProtField('${sid}','risk_multiplier',parseFloat(this.value));debouncedRefreshSummary('${sid}');updateLotLabel('${sid}',parseFloat(this.value))"/>
          <span class="slider-val" id="sv-mult-${sid}">${riskMult}×</span>
        </div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin:3px 0 6px;">Final lot = base × <span style="color:var(--blue-bright)">${riskMult}</span> → clamped [${riskMin}, ${riskMax}]</div>
        <div class="frow">
          <div><div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:3px;">Min lot</div>
            <input class="fi" style="font-size:11px;padding:5px 8px;" id="rmin-${sid}" type="number" step="0.01" value="${riskMin}" min="0.01"
              onchange="updateProtField('${sid}','risk_min_lot',parseFloat(this.value)||0.01)"/></div>
          <div><div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:3px;">Max lot</div>
            <input class="fi" style="font-size:11px;padding:5px 8px;" id="rmax-${sid}" type="number" step="0.5" value="${riskMax}" min="0.01"
              onchange="updateProtField('${sid}','risk_max_lot',parseFloat(this.value)||10)"/></div>
        </div>
      </div>

      <div class="prot-feature">
        <div class="prot-feature-head">
          <div class="prot-feature-title">🔄 SL/TP Synchronisation</div>
          <label class="toggle"><input type="checkbox" id="sltp-en-${sid}" ${sltpEnabled?'checked':''}
            onchange="updateProtField('${sid}','sltp_sync_enabled',this.checked);refreshSummary('${sid}')"/>
            <div class="toggle-track"><div class="toggle-thumb"></div></div></label>
        </div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:5px;">Sync mode:</div>
        <div class="sync-mode-grid" id="sync-mode-${sid}">
          ${syncOpts.map(([v,l])=>`<div class="sync-mode-opt ${sltpMode===v?'active':''}" onclick="setSyncMode('${sid}','${v}')">${l}</div>`).join('')}
        </div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin:8px 0 5px;">Scale SL/TP distance from entry:</div>
        <div class="slider-row">
          <span class="slider-lbl">SL scale</span>
          <input type="range" class="slider-input" id="sl-scale-${sid}" min="0.1" max="3" step="0.1" value="${sltpSL}"
            oninput="document.getElementById('sv-sls-${sid}').textContent=parseFloat(this.value)+'×';updateProtField('${sid}','sltp_scale_sl',parseFloat(this.value))"/>
          <span class="slider-val" id="sv-sls-${sid}">${sltpSL}×</span>
        </div>
        <div class="slider-row">
          <span class="slider-lbl">TP scale</span>
          <input type="range" class="slider-input" id="tp-scale-${sid}" min="0.1" max="3" step="0.1" value="${sltpTP}"
            oninput="document.getElementById('sv-tps-${sid}').textContent=parseFloat(this.value)+'×';updateProtField('${sid}','sltp_scale_tp',parseFloat(this.value))"/>
          <span class="slider-val" id="sv-tps-${sid}">${sltpTP}×</span>
        </div>
        <div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin:4px 0 5px;">Fixed offset in points (+ wider, − tighter):</div>
        <div class="frow">
          <div><div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:3px;">SL offset</div>
            <input class="fi" style="font-size:11px;padding:5px 8px;" id="sl-off-${sid}" type="number" step="1" value="${offSL}"
              onchange="updateProtField('${sid}','sltp_offset_sl',parseFloat(this.value)||0)"/></div>
          <div><div style="font-family:var(--mono);font-size:9px;color:var(--text3);margin-bottom:3px;">TP offset</div>
            <input class="fi" style="font-size:11px;padding:5px 8px;" id="tp-off-${sid}" type="number" step="1" value="${offTP}"
              onchange="updateProtField('${sid}','sltp_offset_tp',parseFloat(this.value)||0)"/></div>
        </div>
      </div>

      <button class="btn btn-primary prot-save-btn" onclick="saveProtection('${sid}')">💾 Save Protection Config</button>
    </div>
  </div>`;
}

function updateProtField(sid, field, value) {
  if (!protState[sid]) {
    const s = slavesData.find(x => x.account_id === sid);
    protState[sid] = Object.assign({}, s?.protection || {});
  }
  protState[sid][field] = value;
  // Mark save button as having unsaved changes
  const saveBtn = document.querySelector(`#pc-${sid} .prot-save-btn`);
  if (saveBtn) { saveBtn.classList.add('has-changes'); saveBtn.textContent = '💾 Save (unsaved changes)'; }
}

function getProtState(sid) {
  const s = slavesData.find(x => x.account_id === sid);
  return Object.assign({}, s?.protection || {}, protState[sid] || {});
}

function setSlipMode(sid, mode) {
  updateProtField(sid, 'slippage_mode', mode);
  refreshSummary(sid);
  const card = document.getElementById('pc-'+sid);
  if (card) card.querySelectorAll('.mode-pill').forEach(el => el.classList.toggle('active', el.textContent.trim() === mode));
}

function setSlipAction(sid, action) {
  updateProtField(sid, 'slippage_action', action);
  const c = document.getElementById('act-cancel-'+sid);
  const a = document.getElementById('act-anyway-'+sid);
  if (c) c.className = 'action-opt'+(action==='cancel'?' active-cancel':'');
  if (a) a.className = 'action-opt'+(action==='execute_anyway'?' active-anyway':'');
}

function setSyncMode(sid, mode) {
  updateProtField(sid, 'sltp_sync_mode', mode);
  refreshSummary(sid);
  const grid = document.getElementById('sync-mode-'+sid);
  if (!grid) return;
  const vals = {full:'Both SL+TP', sl_only:'SL Only', tp_only:'TP Only', none:'Disabled'};
  grid.querySelectorAll('.sync-mode-opt').forEach(el => {
    const v = Object.keys(vals).find(k => vals[k] === el.textContent) || 'none';
    el.classList.toggle('active', v === mode);
  });
}

function updateLotLabel(sid, mult) {
  const el = document.getElementById('lot-label-'+sid);
  if (!el) return;
  el.textContent = mult<=0.3?'ultra_safe':mult<=0.7?'conservative':mult<=1.2?'default':mult<=2.5?'aggressive':'extreme';
}

function refreshSummary(sid) {
  const p    = getProtState(sid);
  const card = document.getElementById('pc-'+sid);
  if (!card) return;
  const sum = card.querySelector('.prot-summary');
  if (!sum) return;
  const se = p.slippage_enabled!==false, te = p.sltp_sync_enabled!==false;
  const sm = p.slippage_max??3, smo = p.slippage_mode??'points';
  const rm = p.risk_multiplier??1, tm = p.sltp_sync_mode??'full';
  sum.innerHTML =
    (se?`<span class="prot-chip prot-chip-slip">🛡 ${sm}${smo[0]} slip</span>`:`<span class="prot-chip prot-chip-off">slip off</span>`)+
    `<span class="prot-chip prot-chip-lot">×${rm} lot</span>`+
    (te?`<span class="prot-chip prot-chip-sltp">SL/TP ${tm.replace('_',' ')}</span>`:`<span class="prot-chip prot-chip-off">no sync</span>`);
}

async function saveProtection(sid) {
  const p = getProtState(sid);
  const saveBtn = document.querySelector(`#pc-${sid} .prot-save-btn`);
  await withBtn(saveBtn, '💾 Saving…', async () => {
    await apiFetch(`/slaves/${sid}/protection`,'PUT', p);
    const s = slavesData.find(x=>x.account_id===sid);
    if (s) s.protection = p;
    delete protState[sid];
    if (saveBtn) { saveBtn.classList.remove('has-changes'); saveBtn.textContent = '💾 Save Protection Config'; }
    toast('Protection saved ✓','success');
    refreshSummary(sid);
  });
}

async function applyPreset(sid, name) {
  try {
    const r = await apiFetch(`/slaves/${sid}/protection/preset?preset_name=${name}`,'POST');
    const s = slavesData.find(x=>x.account_id===sid);
    if (s) s.protection = r.protection;
    delete protState[sid];
    const card = document.getElementById('pc-'+sid);
    if (card && s) card.outerHTML = buildProtCard(s);
    toast(`Preset "${name}" applied ✓`,'success');
  } catch(e) { toast('Preset failed: '+e.message,'error'); }
}

async function applyPresetAll(name) {
  if (!slavesData.length) return toast('No slaves registered','error');
  showConfirm(`Apply "${name.replace('_',' ')}" to ALL ${slavesData.length} slave(s)?`, async ()=>{
    let ok=0, fail=0;
    for (const s of slavesData) {
      try { const r=await apiFetch(`/slaves/${s.account_id}/protection/preset?preset_name=${name}`,'POST'); s.protection=r.protection; delete protState[s.account_id]; ok++; }
      catch(e){ fail++; }
    }
    toast(`Preset applied: ${ok} ok${fail?' · '+fail+' failed':''}`, ok>0?'success':'error');
    renderProtectionCards();
  });
}

// ══════════════════════════════════════════════════════════════════════
// ANALYTICS TAB
// ══════════════════════════════════════════════════════════════════════
function renderAnalytics(){
  renderAnalyticsKPIs();
  renderAnalyticsChart();
  renderAnalyticsTables();
}

function renderAnalyticsKPIs(){
  const s=window._lastStatus||{};
  const copied=s.trades_copied_today??0, failed=s.trades_failed_today??0, blocked=s.trades_blocked_today??0;
  const avgLat=s.avg_latency_ms??0;
  const total=copied+failed+blocked, denom=copied+failed;
  const rate=denom>0?((copied/denom)*100).toFixed(1):'—';
  const posLats=localLogs.map(l=>l.latency_ms).filter(v=>v!=null&&v>0);
  const bestLat=posLats.length?Math.min(...posLats):null;

  const rEl=document.getElementById('an-success-rate');
  if(rEl){
    rEl.textContent=rate!=='—'?rate+'%':'—';
    const rv=parseFloat(rate);
    rEl.style.color=rv>=95?'var(--green)':rv>=80?'var(--amber)':'var(--red)';
  }
  const subEl=document.getElementById('an-success-sub');
  if(subEl) subEl.textContent=denom>0?`${copied} copied / ${failed} failed`:'No trades yet';
  const totEl=document.getElementById('an-total-trades');
  if(totEl) totEl.textContent=total;
  const blEl=document.getElementById('an-best-lat');
  if(blEl) blEl.textContent=bestLat!=null?bestLat.toFixed(0)+'ms':'—';
  const alEl=document.getElementById('an-avg-lat');
  if(alEl){
    alEl.textContent=avgLat?avgLat.toFixed(1)+'ms':'—';
    alEl.style.color=avgLat<50?'var(--green)':avgLat<100?'var(--amber)':'var(--red)';
  }
}

function renderAnalyticsChart(){
  const svg=document.getElementById('an-bar-chart');
  if(!svg) return;
  const now=new Date();
  const buckets=Array.from({length:24},(_,i)=>{
    const h=new Date(now);
    h.setHours(now.getHours()-23+i,0,0,0);
    return {hour:h.getHours(),label:String(h.getHours()).padStart(2,'0'),copied:0,failed:0};
  });
  localLogs.forEach(l=>{
    const t=new Date(l.timestamp||l.time||0);
    if(now-t>86400000) return;
    const msg=(l.message||'').toLowerCase();
    const bkt=buckets.find(b=>b.hour===t.getHours());
    if(!bkt) return;
    if(msg.includes('copied')) bkt.copied++;
    else if(msg.includes('failed')) bkt.failed++;
  });
  const maxVal=Math.max(1,...buckets.map(b=>b.copied+b.failed));
  const W=960,chartH=90,colW=W/24,barW=Math.floor(colW*0.65);
  let markup='';
  buckets.forEach((b,i)=>{
    const x=i*colW, barX=x+(colW-barW)/2;
    const totalH=Math.round(((b.copied+b.failed)/maxVal)*chartH);
    const copH=b.copied+b.failed>0?Math.round((b.copied/(b.copied+b.failed))*totalH):0;
    const failH=totalH-copH;
    const dat=`data-hour="${b.label}" data-copied="${b.copied}" data-failed="${b.failed}"`;
    if(failH>0) markup+=`<rect x="${barX.toFixed(1)}" y="${(chartH-totalH).toFixed(1)}" width="${barW}" height="${failH.toFixed(1)}" style="fill:var(--red);fill-opacity:.75" rx="1" ${dat}/>`;
    if(copH>0)  markup+=`<rect x="${barX.toFixed(1)}" y="${(chartH-totalH+failH).toFixed(1)}" width="${barW}" height="${copH.toFixed(1)}" style="fill:var(--green);fill-opacity:.8" rx="1" ${dat}/>`;
    if(i%3===0) markup+=`<text x="${(x+colW/2).toFixed(1)}" y="114" text-anchor="middle" font-family="'Geist Mono',monospace" font-size="9" style="fill:var(--text3)">${b.label}h</text>`;
  });
  svg.innerHTML=markup;
  const tip=document.getElementById('bar-tooltip');
  svg.querySelectorAll('rect').forEach(rect=>{
    rect.style.cursor='default';
    rect.addEventListener('mouseenter',e=>{
      const r=e.target;
      tip.innerHTML=`<strong>${r.dataset.hour}:00</strong><br><span style="color:var(--green)">Copied: ${r.dataset.copied}</span><br><span style="color:var(--red)">Failed: ${r.dataset.failed}</span>`;
      tip.style.display='block';
    });
    rect.addEventListener('mousemove',e=>{ tip.style.left=Math.min(e.clientX+14,window.innerWidth-160)+'px'; tip.style.top=Math.max(e.clientY-44,4)+'px'; });
    rect.addEventListener('mouseleave',()=>{ tip.style.display='none'; });
  });
  const upd=document.getElementById('an-chart-updated');
  if(upd) upd.textContent='Updated '+new Date().toISOString().slice(11,19)+' UTC';
}

function renderAnalyticsTables(){
  const mTb=document.getElementById('an-master-tbody');
  if(mTb){
    if(!mastersData.length){
      mTb.innerHTML='<tr><td colspan="6" style="text-align:center;color:var(--text3);padding:20px;">No masters registered</td></tr>';
    } else {
      mTb.innerHTML=mastersData.map(m=>{
        const conn=m.connection_status==='connected', err=m.connection_status==='error';
        const dc=conn?'dot-blue':err?'dot-red':'dot-gray';
        const tc=conn?'tag-green':err?'tag-red':'tag-gray';
        return `<tr>
          <td><span class="dot ${dc}" style="margin-right:6px;"></span>${esc(m.label)}</td>
          <td><span class="mc-magic">${m.magic_number??'—'}</span></td>
          <td style="color:var(--blue-bright)">$${fmt(m.equity)}</td>
          <td style="color:var(--green)">${m.trades_today??0}</td>
          <td>${(m.linked_slaves||[]).length}</td>
          <td><span class="tag ${tc}">${m.connection_status??'unknown'}</span></td>
        </tr>`;
      }).join('');
    }
  }
  const sTb=document.getElementById('an-slave-tbody');
  if(sTb){
    if(!slavesData.length){
      sTb.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--text3);padding:20px;">No slaves registered</td></tr>';
    } else {
      sTb.innerHTML=slavesData.map(sv=>{
        const conn=sv.connection_status==='connected', err=sv.connection_status==='error';
        const dc=conn?'dot-blue':err?'dot-red':'dot-gray';
        const tc=conn?'tag-green':err?'tag-red':'tag-gray';
        const mLabels=(sv.master_ids||[]).map(mid=>{
          const m=mastersData.find(x=>x.master_id===mid);
          return m?`<span class="mbadge">${esc(m.label)}</span>`:'';
        }).join('');
        return `<tr>
          <td><span class="dot ${dc}" style="margin-right:6px;"></span>${esc(sv.label)}</td>
          <td>${sv.open_trades??0}</td>
          <td style="color:var(--blue-bright)">$${fmt(sv.equity)}</td>
          <td><span class="tag ${tc}">${sv.connection_status??'unknown'}</span></td>
          <td style="display:flex;gap:3px;flex-wrap:wrap;">${mLabels||'<span style="color:var(--text3);font-size:10px;">Unlinked</span>'}</td>
        </tr>`;
      }).join('');
    }
  }
}

// ══════════════════════════════════════════════════════════════════════
// PATCH applyStatus — uptime + blocked counter + protection refresh
// ══════════════════════════════════════════════════════════════════════
const _origApply = applyStatus;
applyStatus = function(s) {
  window._lastStatus = s;
  _origApply(s);

  // Blocked counter
  const bEl = document.getElementById('s-blocked');
  if (bEl) bEl.textContent = s.trades_blocked_today ?? 0;

  // Telegram toggle sync
  if (s.telegram_enabled !== undefined) {
    const tog = document.getElementById('tg-master-toggle');
    if (tog) tog.checked = s.telegram_enabled;
  }

  // Open trades total across all slaves
  const totalOpen = (s.slaves||[]).reduce((acc,sv)=>acc+(sv.open_trades||0),0);
  const hOpen = document.getElementById('h-open');
  if (hOpen) hOpen.textContent = totalOpen;

  // Uptime in header
  const up = s.uptime_seconds || 0;
  window._lastUptime = up;
  const hh=Math.floor(up/3600), mm=Math.floor((up%3600)/60), ss=Math.floor(up%60);
  const hUptime = document.getElementById('h-uptime');
  if (hUptime) hUptime.textContent = up
    ? `${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`
    : '—';

  // Re-render protection cards if tab is open, but never clobber unsaved edits.
  const protTab = document.getElementById('tabview-protection');
  if (protTab && protTab.classList.contains('active')) {
    const dirty = new Set(Object.keys(protState));
    if (dirty.size === 0) {
      renderProtectionCards();
    } else {
      // Refresh only the clean cards; leave dirty ones untouched
      slavesData.forEach(sv => {
        if (!dirty.has(sv.account_id)) {
          const existing = document.getElementById('pc-' + sv.account_id);
          if (existing) existing.outerHTML = buildProtCard(sv);
        }
      });
      // If no cards exist yet (first render), do a full render anyway
      if (!document.querySelector('.prot-card') && slavesData.length) renderProtectionCards();
    }
  }

  // Live-refresh analytics tab if open
  const analyticsTab = document.getElementById('tabview-analytics');
  if (analyticsTab && analyticsTab.classList.contains('active')) renderAnalytics();
};

// ══════════════════════════════════════════════════════════════════════
// SIDEBAR TOGGLE (mobile)
// ══════════════════════════════════════════════════════════════════════
function toggleSidebar(){
  const sb=document.querySelector('.sidebar');
  sb.classList.toggle('open');
}
// Close sidebar when clicking outside on mobile
document.addEventListener('click',e=>{
  const sb=document.querySelector('.sidebar');
  const tog=document.getElementById('sidebar-toggle');
  if(sb.classList.contains('open')&&!sb.contains(e.target)&&!tog.contains(e.target)) sb.classList.remove('open');
});

// Close modals on Escape key
document.addEventListener('keydown',e=>{
  if(e.key==='Escape')
    document.querySelectorAll('.modal-backdrop:not(.hidden)').forEach(el=>el.classList.add('hidden'));
});

// ══════════════════════════════════════════════════════════════════════
// BOOT
// ══════════════════════════════════════════════════════════════════════

// Restore persisted Bridge URL + API key
try {
  const savedUrl = localStorage.getItem('omni_bridge_url');
  const savedKey = localStorage.getItem('omni_api_key');
  if (savedUrl) { document.getElementById('cfg-url').value = savedUrl; BRIDGE = savedUrl.replace(/\/$/,''); }
  if (savedKey) document.getElementById('cfg-apikey').value = savedKey;
} catch(e) {}

// ══════════════════════════════════════════════════════════════════════
// BOT ENGINE TAB
// ══════════════════════════════════════════════════════════════════════
let botsData=[], stratsData=[], _botsPollTimer=null;
let _assignCtx=null;   // {type:'bot'|'strategy', id}
let _stratTab='visual';
let _botMode='standalone';

async function refreshBotsTab(){
  try{
    const [bots, strats] = await Promise.all([
      apiFetch('/api/v1/bots'), apiFetch('/api/v1/strategies')
    ]);
    botsData = bots; stratsData = strats;
    renderBots(); renderStrategies();
  }catch(e){
    document.getElementById('bots-list').innerHTML =
      `<div class="empty-state"><div class="empty-title">Bridge unreachable</div><div class="empty-sub">${esc(e.message)}</div></div>`;
  }
}

function renderBots(){
  const el=document.getElementById('bots-list');
  document.getElementById('bot-count').textContent=`${botsData.length} bot${botsData.length!==1?'s':''}`;
  if(!botsData.length){
    el.innerHTML=`<div class="empty-state"><div class="empty-icon">🤖</div><div class="empty-title">No Bots Yet</div><div class="empty-sub">Click + Add Bot to create your first trading bot</div></div>`;
    return;
  }
  el.innerHTML=botsData.map(b=>{
    const statusTag = b.status==='running' ? '<span class="tag tag-green">Running</span>'
                    : b.status==='killed'  ? '<span class="tag tag-red">Killed</span>'
                    : '<span class="tag tag-gray">Stopped</span>';
    const modeTag = b.mode==='connected' ? '<span class="tag tag-blue">Connected</span>' : '<span class="tag tag-purple">Standalone</span>';
    const fwdTag  = b.forward_test ? '<span class="tag tag-amber">Fwd Test</span>' : '';
    const pnl = b.daily_pnl||0;
    const pnlColor = pnl>0?'var(--green)':pnl<0?'var(--red)':'var(--text3)';
    return `<div class="bot-card ${b.status}">
      <div class="bc-head">
        <span class="dot ${b.status==='running'?'dot-green':b.status==='killed'?'dot-red':'dot-gray'}"></span>
        <span class="bc-label">${esc(b.label)}</span>
        ${statusTag}${modeTag}${fwdTag}
      </div>
      <div class="bc-meta">
        <span>${esc(b.symbol||'—')}</span><span>${esc(b.timeframe)}</span>
        <span>magic:${b.magic_number}</span><span>${b.base_volume}L</span>
        <span style="color:${pnlColor}">day P&L: ${pnl>=0?'+':''}${fmt(pnl)}</span>
        ${b.open_position?'<span style="color:var(--blue-bright)">position open</span>':''}
      </div>
      <div class="bc-strat">${b.strategy_name?`Strategy: <b>${esc(b.strategy_name)}</b>`:'<span style="color:var(--amber)">No strategy assigned — bot cannot run</span>'}</div>
      <div class="bc-actions">
        <button class="btn ${b.enabled?'btn-warn':'btn-primary'} btn-xs" onclick="toggleBot('${b.bot_id}',${!b.enabled})">${b.enabled?'⏸ Pause':'▶ Start'}</button>
        <button class="btn btn-ghost btn-xs" onclick="openAssignFromBot('${b.bot_id}')">Assign Strategy</button>
        <button class="btn btn-ghost btn-xs" onclick="viewResults('${b.bot_id}','${esc(b.label)}')">Results</button>
        <button class="btn btn-ghost btn-xs" onclick="openBotModal('${b.bot_id}')">Edit</button>
        <button class="btn btn-danger btn-xs" onclick="deleteBot('${b.bot_id}','${esc(b.label)}')">Delete</button>
      </div>
    </div>`;
  }).join('');
}

function renderStrategies(){
  const el=document.getElementById('strategies-list');
  document.getElementById('strat-count').textContent=`${stratsData.length} strateg${stratsData.length!==1?'ies':'y'}`;
  if(!stratsData.length){
    el.innerHTML=`<div class="empty-state"><div class="empty-icon">📐</div><div class="empty-title">No Strategies</div><div class="empty-sub">Create a visual or Python strategy to power your bots</div></div>`;
    return;
  }
  el.innerHTML=stratsData.map(s=>{
    const bot = botsData.find(b=>b.bot_id===s.assigned_bot_id);
    const modeTag = s.mode==='visual' ? '<span class="tag tag-blue">Visual</span>' : '<span class="tag tag-purple">Code</span>';
    return `<div class="strat-card">
      <div class="bc-head">
        <span class="bc-label">${esc(s.name)}</span>${modeTag}
        ${s.file_missing?'<span class="tag tag-red">File missing</span>':''}
      </div>
      <div class="bc-meta"><span>${esc(s.symbol)}</span><span>${esc(s.timeframe)}</span></div>
      <div class="bc-strat">${bot?`Assigned to: <b>${esc(bot.label)}</b>`:'<span style="color:var(--text3)">Not assigned</span>'}</div>
      <div class="bc-actions">
        <button class="btn btn-ghost btn-xs" onclick="openStrategyModal('${s.strategy_id}')">Edit</button>
        <button class="btn btn-ghost btn-xs" onclick="openAssignFromStrategy('${s.strategy_id}')">Assign to Bot</button>
        ${bot?`<button class="btn btn-ghost btn-xs" onclick="viewResults('${bot.bot_id}','${esc(s.name)}')">View Results</button>`:''}
        <button class="btn btn-danger btn-xs" onclick="deleteStrategy('${s.strategy_id}','${esc(s.name)}')">Delete</button>
      </div>
    </div>`;
  }).join('');
}

// ── Bot CRUD ──
function setBotMode(m){
  _botMode=m;
  document.getElementById('bm-mode-standalone').classList.toggle('active',m==='standalone');
  document.getElementById('bm-mode-connected').classList.toggle('active',m==='connected');
  document.getElementById('bm-mode-hint').textContent = m==='standalone'
    ? 'Bot trades directly on its own MT5 account'
    : 'Bot injects signals into the copier — linked slaves execute them';
}

function openBotModal(botId){
  const b = botId ? botsData.find(x=>x.bot_id===botId) : null;
  document.getElementById('bot-modal-title').textContent = b?'Edit Bot':'Add Bot';
  document.getElementById('bm-id').value     = b?b.bot_id:'';
  document.getElementById('bm-label').value  = b?b.label:'';
  document.getElementById('bm-symbol').value = b?(b.symbol||''):'';
  document.getElementById('bm-tf').value     = b?b.timeframe:'M5';
  document.getElementById('bm-volume').value = b?b.base_volume:0.1;
  document.getElementById('bm-magic').value  = b?b.magic_number:'';
  document.getElementById('bm-fwd').checked  = b?b.forward_test:false;
  setBotMode(b?b.mode:'standalone');
  openModal('bot-modal');
}

async function saveBot(){
  const id     = document.getElementById('bm-id').value;
  const label  = document.getElementById('bm-label').value.trim();
  const symbol = document.getElementById('bm-symbol').value.trim().toUpperCase();
  const magic  = parseInt(document.getElementById('bm-magic').value);
  const volume = parseFloat(document.getElementById('bm-volume').value);
  if(!label||!symbol||!magic||!volume){ toast('Label, symbol, magic number and volume are required','error'); return; }
  const payload = {label, symbol, timeframe:document.getElementById('bm-tf').value,
    magic_number:magic, base_volume:volume, mode:_botMode,
    forward_test:document.getElementById('bm-fwd').checked};
  const btn = document.getElementById('bm-save');
  await withBtn(btn,'Saving…',async()=>{
    try{
      if(id) await apiFetch(`/api/v1/bots/${id}`,'PATCH',payload);
      else   await apiFetch('/api/v1/bots','POST',payload);
      toast(id?'Bot updated':'Bot created','success');
      closeModal('bot-modal');
      await refreshBotsTab();
    }catch(e){ toast(e.message,'error'); }
  });
}

async function toggleBot(botId, enable){
  try{
    await apiFetch(`/api/v1/bots/${botId}`,'PATCH',{enabled:enable});
    toast(enable?'Bot started':'Bot paused','success');
    await refreshBotsTab();
  }catch(e){ toast(e.message,'error'); }
}

function deleteBot(botId, label){
  showConfirm(`Delete bot "${label}"? Its task will be stopped and its config removed.`, async()=>{
    try{
      await apiFetch(`/api/v1/bots/${botId}`,'DELETE');
      toast('Bot deleted','success');
      await refreshBotsTab();
    }catch(e){ toast(e.message,'error'); }
  });
}

// ── Strategy modal ──
function setStratTab(t){
  _stratTab=t;
  document.getElementById('sm-tab-btn-visual').classList.toggle('active',t==='visual');
  document.getElementById('sm-tab-btn-code').classList.toggle('active',t==='code');
  document.getElementById('sm-tab-visual').style.display = t==='visual'?'':'none';
  document.getElementById('sm-tab-code').style.display   = t==='code'?'':'none';
}

function openStrategyModal(stratId){
  const s = stratId ? stratsData.find(x=>x.strategy_id===stratId) : null;
  document.getElementById('strategy-modal-title').textContent = s?'Edit Strategy':'Create Strategy';
  document.getElementById('sm-id').value     = s?s.strategy_id:'';
  document.getElementById('sm-name').value   = s?s.name:'';
  document.getElementById('sm-symbol').value = s?s.symbol:'';
  document.getElementById('sm-tf').value     = s?s.timeframe:'M5';
  document.getElementById('sm-code-badge').className='sm-badge';
  document.getElementById('sm-code-badge').textContent='';
  // Always reset the visual builder to defaults first so a new strategy never
  // inherits the previously edited one's values.
  fillVisualBuilder(s && s.blocks ? s.blocks : {});
  document.getElementById('sm-code').value = (s && s.mode==='code') ? (s.source_code||'') : '';
  setStratTab(s && s.mode==='code' ? 'code' : 'visual');
  openModal('strategy-modal');
}

function fillVisualBuilder(bl){
  const e=bl.entry||{}, x=bl.exit||{}, p=bl.position||{}, f=bl.filters||{}, r=bl.risk||{};
  // Clear to '' when a value is absent so this doubles as a form reset.
  const set=(id,v)=>{ const el=document.getElementById(id); if(el!=null) el.value=(v==null?'':v); };
  set('sm-e-ind',e.indicator); set('sm-e-period',e.period); set('sm-e-op',e.operator);
  set('sm-e-val',e.value); set('sm-e-dir',e.direction);
  set('sm-x-type',x.type||'indicator'); set('sm-x-ind',x.indicator); set('sm-x-period',x.period);
  set('sm-x-op',x.operator); set('sm-x-val',x.value); set('sm-x-tp',x.tp_pips); set('sm-x-sl',x.sl_pips);
  document.getElementById('sm-x-ind-row').style.display=(x.type||'indicator')==='indicator'?'':'none';
  document.getElementById('sm-x-trail').checked=!!x.trailing_stop;
  document.getElementById('sm-x-trail-row').style.display=x.trailing_stop?'':'none';
  set('sm-x-trailtype',x.trail_type||'atr');
  set('sm-x-trailval',x.trail_type==='fixed'?x.trail_pips:x.trail_atr_multiplier);
  set('sm-p-pct',p.partial_close_pct); set('sm-p-rr',p.partial_close_at_rr);
  set('sm-p-max',p.max_concurrent||1);
  document.getElementById('sm-p-rev').checked=!!p.reverse_on_signal;
  document.querySelectorAll('.sm-f-sess').forEach(c=>c.checked=(f.sessions||[]).includes(c.value));
  // days===null/undefined means "no restriction" → all days checked. A real
  // restriction is only some days checked. (All-checked round-trips back to null.)
  document.querySelectorAll('.sm-f-day').forEach(c=>c.checked=(f.days==null)?true:f.days.includes(parseInt(c.value)));
  document.getElementById('sm-f-trend').checked=!!f.trend_tf;
  document.getElementById('sm-f-trend-row').style.display=f.trend_tf?'':'none';
  set('sm-f-trendtf',f.trend_tf||'H1'); set('sm-f-trendema',f.trend_ema_period||200);
  set('sm-f-atrmin',f.atr_min); set('sm-f-atrmax',f.atr_max);
  set('sm-r-daily',r.daily_loss_limit_pct); set('sm-r-dd',r.max_drawdown_pct);
  set('sm-r-cool',r.cooldown_after_losses); set('sm-r-kill',r.kill_switch_pct);
}

function collectVisualBlocks(){
  const num=id=>{ const v=document.getElementById(id).value; return v===''?null:parseFloat(v); };
  const xType=document.getElementById('sm-x-type').value;
  const trail=document.getElementById('sm-x-trail').checked;
  const trailType=document.getElementById('sm-x-trailtype').value;
  const trailVal=num('sm-x-trailval');
  const sessions=[...document.querySelectorAll('.sm-f-sess:checked')].map(c=>c.value);
  const dayBoxes=[...document.querySelectorAll('.sm-f-day')];
  const checkedDays=dayBoxes.filter(c=>c.checked).map(c=>parseInt(c.value));
  // All days checked = no restriction (null); a subset is a real filter.
  const days=(checkedDays.length===dayBoxes.length)?null:checkedDays;
  const trend=document.getElementById('sm-f-trend').checked;
  return {
    entry:{ indicator:document.getElementById('sm-e-ind').value, period:num('sm-e-period')||14,
      operator:document.getElementById('sm-e-op').value, value:num('sm-e-val'),
      direction:document.getElementById('sm-e-dir').value },
    exit:{ type:xType,
      indicator:xType==='indicator'?document.getElementById('sm-x-ind').value:null,
      period:xType==='indicator'?(num('sm-x-period')||14):null,
      operator:xType==='indicator'?document.getElementById('sm-x-op').value:null,
      value:xType==='indicator'?num('sm-x-val'):null,
      tp_pips:num('sm-x-tp'), sl_pips:num('sm-x-sl'),
      trailing_stop:trail, trail_type:trail?trailType:null,
      trail_atr_multiplier:trail&&trailType==='atr'?trailVal:null,
      trail_pips:trail&&trailType==='fixed'?trailVal:null },
    position:{ partial_close_pct:num('sm-p-pct'), partial_close_at_rr:num('sm-p-rr'),
      max_concurrent:num('sm-p-max')||1,
      reverse_on_signal:document.getElementById('sm-p-rev').checked },
    filters:{ sessions:sessions.length?sessions:null,
      trend_tf:trend?document.getElementById('sm-f-trendtf').value:null,
      trend_ema_period:trend?(num('sm-f-trendema')||200):null,
      atr_min:num('sm-f-atrmin'), atr_max:num('sm-f-atrmax'),
      days:days.length?days:null },
    risk:{ daily_loss_limit_pct:num('sm-r-daily'), max_drawdown_pct:num('sm-r-dd'),
      cooldown_after_losses:num('sm-r-cool'), kill_switch_pct:num('sm-r-kill') }
  };
}

function buildStrategyPayload(){
  const name=document.getElementById('sm-name').value.trim();
  const symbol=document.getElementById('sm-symbol').value.trim().toUpperCase();
  if(!name||!symbol){ toast('Name and symbol are required','error'); return null; }
  const payload={name, symbol, timeframe:document.getElementById('sm-tf').value, mode:_stratTab};
  if(_stratTab==='visual') payload.blocks=collectVisualBlocks();
  else {
    payload.source_code=document.getElementById('sm-code').value;
    if(!payload.source_code.trim()){ toast('Python source is empty','error'); return null; }
  }
  return payload;
}

async function validateStrategy(){
  const badge=document.getElementById('sm-code-badge');
  const src=document.getElementById('sm-code').value;
  badge.className='sm-badge';
  if(!/def\s+evaluate\s*\(/.test(src)){
    badge.className='sm-badge fail'; badge.textContent='✕ Must define evaluate(market_data)';
    return;
  }
  // server-side check: full syntax validation happens on save; a temp POST
  // would persist a draft, so confirm structure client-side only here
  badge.className='sm-badge ok'; badge.textContent='✓ evaluate() found — full syntax check runs on Save';
}

async function saveStrategy(){
  const payload=buildStrategyPayload();
  if(!payload) return;
  const id=document.getElementById('sm-id').value;
  const btn=document.getElementById('sm-save');
  const badge=document.getElementById('sm-code-badge');
  await withBtn(btn,'Saving…',async()=>{
    try{
      if(id) await apiFetch(`/api/v1/strategies/${id}`,'PUT',payload);
      else   await apiFetch('/api/v1/strategies','POST',payload);
      toast(id?'Strategy updated':'Strategy created','success');
      closeModal('strategy-modal');
      await refreshBotsTab();
    }catch(e){
      if(_stratTab==='code'){ badge.className='sm-badge fail'; badge.textContent='✕ '+e.message; }
      toast(e.message,'error');
    }
  });
}

function deleteStrategy(stratId, name){
  showConfirm(`Delete strategy "${name}"? Its file will be removed and any bot using it will stop.`, async()=>{
    try{
      await apiFetch(`/api/v1/strategies/${stratId}`,'DELETE');
      toast('Strategy deleted','success');
      await refreshBotsTab();
    }catch(e){ toast(e.message,'error'); }
  });
}

// ── Assign ──
function openAssignFromBot(botId){
  _assignCtx={type:'bot', id:botId};
  const bot=botsData.find(b=>b.bot_id===botId);
  document.getElementById('assign-modal-title').textContent=`Assign Strategy → ${bot?bot.label:''}`;
  document.getElementById('assign-select-label').textContent='Strategy';
  const sel=document.getElementById('assign-select');
  sel.innerHTML='<option value="">— Unassign —</option>'+stratsData.map(s=>
    `<option value="${s.strategy_id}" ${s.assigned_bot_id===botId?'selected':''}>${esc(s.name)} (${esc(s.symbol)} ${esc(s.timeframe)})</option>`).join('');
  openModal('assign-modal');
}

function openAssignFromStrategy(stratId){
  _assignCtx={type:'strategy', id:stratId};
  const s=stratsData.find(x=>x.strategy_id===stratId);
  document.getElementById('assign-modal-title').textContent=`Assign "${s?s.name:''}" → Bot`;
  document.getElementById('assign-select-label').textContent='Bot';
  const sel=document.getElementById('assign-select');
  sel.innerHTML=botsData.map(b=>
    `<option value="${b.bot_id}" ${s&&s.assigned_bot_id===b.bot_id?'selected':''}>${esc(b.label)} (${esc(b.symbol||'—')})</option>`).join('');
  if(!botsData.length) sel.innerHTML='<option value="">No bots — create one first</option>';
  openModal('assign-modal');
}

async function confirmAssign(){
  if(!_assignCtx) return;
  const sel=document.getElementById('assign-select').value;
  const btn=document.getElementById('assign-ok');
  let botId, stratId;
  if(_assignCtx.type==='bot'){ botId=_assignCtx.id; stratId=sel||null; }
  else { stratId=_assignCtx.id; botId=sel; if(!botId){ toast('Select a bot','error'); return; } }
  await withBtn(btn,'Assigning…',async()=>{
    try{
      await apiFetch(`/api/v1/bots/${botId}/strategy`,'POST',{strategy_id:stratId});
      toast(stratId?'Strategy assigned':'Strategy unassigned','success');
      closeModal('assign-modal');
      await refreshBotsTab();
    }catch(e){ toast(e.message,'error'); }
  });
}

// ── Results ──
async function viewResults(botId, label){
  document.getElementById('results-modal-title').textContent=`Results — ${label}`;
  const body=document.getElementById('results-body');
  body.innerHTML='<div style="font-family:var(--mono);font-size:11px;color:var(--text3);">Loading…</div>';
  openModal('results-modal');
  try{
    const rows=await apiFetch(`/api/v1/bots/${botId}/results`);
    if(!rows.length){
      body.innerHTML='<div class="empty-state"><div class="empty-title">No results yet</div><div class="empty-sub">Results appear when the bot opens or closes a trade</div></div>';
      return;
    }
    const closed=rows.filter(r=>r.pnl!=null);
    const totalPnl=closed.reduce((a,r)=>a+r.pnl,0);
    const wins=closed.filter(r=>r.pnl>0).length;
    body.innerHTML=`
      <div style="display:flex;gap:14px;margin-bottom:12px;font-family:var(--mono);font-size:11px;">
        <span>Trades: <b style="color:var(--text)">${rows.length}</b></span>
        <span>Closed: <b style="color:var(--text)">${closed.length}</b></span>
        <span>Win rate: <b style="color:var(--text)">${closed.length?Math.round(wins/closed.length*100)+'%':'—'}</b></span>
        <span>Total P&L: <b style="color:${totalPnl>=0?'var(--green)':'var(--red)'}">${totalPnl>=0?'+':''}${fmt(totalPnl)}</b></span>
      </div>
      <table class="results-table">
        <thead><tr><th>Time</th><th>Signal</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Mode</th></tr></thead>
        <tbody>${rows.map(r=>`<tr>
          <td>${esc((r.executed_at||'').replace('T',' ').slice(0,19))}</td>
          <td>${esc(r.signal_direction||'—')}</td>
          <td>${r.entry_price!=null?r.entry_price:'—'}</td>
          <td>${r.exit_price!=null?r.exit_price:'—'}</td>
          <td style="color:${r.pnl>0?'var(--green)':r.pnl<0?'var(--red)':'var(--text3)'}">${r.pnl!=null?(r.pnl>=0?'+':'')+fmt(r.pnl):'open'}</td>
          <td>${r.mode==='forward_test'?'<span class="tag tag-amber">fwd</span>':'<span class="tag tag-green">live</span>'}</td>
        </tr>`).join('')}</tbody>
      </table>`;
  }catch(e){
    body.innerHTML=`<div style="color:var(--red);font-family:var(--mono);font-size:11px;">${esc(e.message)}</div>`;
  }
}

document.getElementById('cfg-url').addEventListener('change',e=>{ BRIDGE=e.target.value.replace(/\/$/,''); });
setInterval(()=>{
  if(!wsActive) refreshAll();
  else apiFetch('/logs?limit=400').then(logs=>{localLogs=logs;renderLog();}).catch(()=>{});
},3000);
renderMasters(); renderSlaves(); renderLog();
loadEventPrefs();
showPreview('copied');
connectBridge();
