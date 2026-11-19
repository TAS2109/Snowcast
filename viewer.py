# -*- coding: utf-8 -*-
"""SNOW CAST データ確認ページ。

予報の計算（雪判別・統合）に入る前の「元データ」を、データ源ごとに1つずつ
地図・グラフ・表で目視確認するための単体ページ。

  - Flask版      : /inspect （データは /api/raw）
  - GitHub Pages : inspect.html （データは data/raw.json。build_static.py が出力）
"""

VIEWER_HTML = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b1424">
<title>SNOW CAST - データ確認</title>
<link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>
<style>
:root{--bg:#0b1424;--bg2:#101d33;--ln:#1f3350;--tx:#f2f7ff;--sub:#8ea3c0;--ice:#9fd3ff;--warm:#ffc27a;--ok:#7be0a1;--ng:#ff8fa8;--r:14px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.6 -apple-system,BlinkMacSystemFont,"Hiragino Sans","Noto Sans JP",sans-serif;
padding:env(safe-area-inset-top) 0 env(safe-area-inset-bottom)}
.wrap{max-width:1080px;margin:auto;padding:0 14px}
.top{display:flex;justify-content:space-between;align-items:center;padding:14px 0 6px;gap:10px}
.brand{font-weight:800;font-size:17px;letter-spacing:.14em}
.brand small{display:block;font-weight:400;font-size:12px;letter-spacing:0;color:var(--sub)}
.iconbtn{border:1px solid var(--ln);background:var(--bg2);color:var(--tx);border-radius:999px;padding:7px 14px;font-size:13px;cursor:pointer;text-decoration:none;display:inline-block;white-space:nowrap}
.iconbtn:focus-visible,.tab:focus-visible,.chip:focus-visible,a:focus-visible{outline:2px solid var(--ice);outline-offset:2px}
.btns{display:flex;gap:8px}
.note{font-size:12.5px;color:var(--sub);margin:8px 0}
.err{color:var(--warm)}
.tabs{position:sticky;top:0;z-index:5;margin:10px -14px 0;padding:8px 14px;background:rgba(11,20,36,.94);backdrop-filter:blur(8px);
display:flex;gap:6px;overflow-x:auto;border-bottom:1px solid var(--ln)}
.tab{flex:0 0 auto;border:0;background:transparent;color:var(--sub);font-size:14px;font-weight:600;padding:8px 12px;border-radius:10px;cursor:pointer;white-space:nowrap}
.tab[aria-selected=true]{background:var(--ice);color:#06223a}
.sub{display:flex;gap:6px;flex-wrap:wrap;margin:12px 0 4px}
.chip{border:1px solid var(--ln);background:var(--bg2);color:var(--sub);padding:5px 12px;border-radius:999px;font-size:13px;cursor:pointer;text-decoration:none;white-space:nowrap}
.chip.on{background:var(--tx);color:#0b1424;font-weight:700;border-color:var(--tx)}
.chip.bad{color:var(--ng)}
h2{font-size:16px;margin:22px 0 8px}
h2:first-child{margin-top:8px}
h3{font-size:14px;margin:0 0 6px;color:var(--ice)}
.card{background:var(--bg2);border:1px solid var(--ln);border-radius:var(--r);padding:12px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.grid3{display:grid;grid-template-columns:repeat(auto-fill,minmax(310px,1fr));gap:12px}
@media (max-width:760px){.grid2{grid-template-columns:1fr}}
.grid2>*,.grid3>*{min-width:0}
.map{height:320px;border-radius:12px;overflow:hidden;border:1px solid var(--ln);background:#0e1a2e}
.map.tall{height:420px}
.cv{position:relative;height:170px}
.cv.big{height:300px}
.tw{overflow:auto;max-height:430px;border:1px solid var(--ln);border-radius:12px}
.tw.free{max-height:none}
table.dt{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
.dt th,.dt td{padding:6px 8px;text-align:right;border-bottom:1px solid var(--ln);white-space:nowrap}
.dt th{position:sticky;top:0;background:#14243f;color:var(--sub);font-weight:600;z-index:1}
.dt th:first-child,.dt td:first-child{text-align:left}
.dt td.l,.dt th.l{text-align:left}
.dt td.wrapc{white-space:normal;min-width:180px}
.dt tr:hover td{background:rgba(159,211,255,.06)}
.dt th.sortable{cursor:pointer;user-select:none}
.dt a{color:var(--ice)}
.na{color:#52627a}
.badge{display:inline-block;font-size:12px;padding:0 8px;border-radius:999px;border:1px solid var(--ln)}
.badge.ok{color:var(--ok);border-color:#2c5a43}
.badge.ng{color:var(--ng);border-color:#6a2c3b}
.badge.part{color:var(--warm);border-color:#6a4f25}
.vd{display:inline-block;min-width:22px;text-align:center;font-size:11px;border-radius:5px;padding:0 3px;margin-right:2px;background:#14243f;color:#46587a}
.vd.on{background:#1c4a6e;color:#d8efff}
.kv{display:grid;grid-template-columns:auto 1fr;gap:3px 12px;font-size:13px}
.kv dt{color:var(--sub)}.kv dd{margin:0}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
select,input[type=range]{background:#0b1424;color:#fff;border:1px solid var(--ln);border-radius:8px;padding:6px}
input[type=range]{padding:0;width:min(520px,100%)}
.strip{display:flex;gap:2px;flex-wrap:wrap}
.strip i{display:block;width:10px;height:22px;border-radius:3px;background:#2a3b57}
.leaflet-tooltip.lt{background:#0b1424;color:#f2f7ff;border:1px solid #1f3350;font-size:11px;padding:1px 5px;box-shadow:none}
.leaflet-tooltip.lt:before{display:none}
.skel{height:30px;border-radius:8px;background:linear-gradient(90deg,var(--bg2),#17284a,var(--bg2));background-size:200% 100%;animation:sk 1.4s linear infinite}
@keyframes sk{to{background-position:-200% 0}}
@media (prefers-reduced-motion:reduce){.skel{animation:none}}
.foot{font-size:12px;color:var(--sub);padding:18px 0 40px}
</style>
</head>
<body>
<div class="wrap">
<div class="top"><div class="brand">データ確認<small>予報の計算に使う元データを、1つずつ目で確かめる</small></div>
<div class="btns"><a class="iconbtn" href="/">予報へ戻る</a><button class="iconbtn" id="btnReload">再読み込み</button></div></div>
<div class="note" id="gen">読み込み中…</div>
<nav class="tabs" id="cats" role="tablist"></nav>
<div class="sub" id="sub"></div>
<main id="main"><div class="skel" style="margin-top:14px">&nbsp;</div></main>
<div class="foot">ここに出るのは、雪判別・統合の前の「そのままのデータ」です（小数の丸めのみ）。確定論モデルとアンサンブルは甲府の地点に最も近い格子点の値です。防災判断は気象庁の情報を優先してください。</div>
</div>

<script>
const $=s=>document.querySelector(s);
const GRID='rgba(255,255,255,.07)', TICK='#8ea3c0';
const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const f=(x,d=1)=>x==null||x===''||isNaN(x)?'<span class="na">–</span>':(+x).toFixed(d);
const tl=t=>t.slice(5,10).replace('-','/')+' '+t.slice(11,13)+'時';
const km=(a,b,c,d)=>{const p=Math.PI/180,x=Math.sin((c-a)*p/2)**2+Math.cos(a*p)*Math.cos(c*p)*Math.sin((d-b)*p/2)**2;return 12742*Math.asin(Math.sqrt(x));};
const num=v=>{const x=parseFloat(v);return isNaN(x)?null:x;};

const VARS=[
 {k:'temperature_2m',n:'気温',s:'気温',u:'℃',c:'#ffc27a'},
 {k:'relative_humidity_2m',n:'相対湿度',s:'湿度',u:'%',c:'#7fdbda'},
 {k:'precipitation',n:'降水量',s:'降水',u:'mm/h',c:'#9fd3ff',bar:true},
 {k:'pressure_msl',n:'海面気圧',s:'気圧',u:'hPa',c:'#c9a6ff'},
 {k:'temperature_850hPa',n:'850hPa気温',s:'850',u:'℃',c:'#ff8fa8'},
 {k:'wind_speed_10m',n:'風速(10m)',s:'風',u:'m/s',c:'#7be0a1'},
 {k:'cloud_cover',n:'雲量',s:'雲',u:'%',c:'#9aa5b1'},
 {k:'snowfall',n:'モデル降雪量',s:'雪',u:'cm/h',c:'#e6e6ff',bar:true},
 {k:'freezing_level_height',n:'0℃高度',s:'0℃',u:'m',c:'#f5e663'}];
const ENSV=VARS.filter(v=>['temperature_2m','relative_humidity_2m','precipitation','pressure_msl','temperature_850hPa'].includes(v.k));
const OBSV=[
 {k:'temp',n:'気温',u:'℃',c:'#ffc27a'},{k:'rh',n:'相対湿度',u:'%',c:'#7fdbda'},
 {k:'precip',n:'降水量(1時間)',u:'mm',c:'#9fd3ff',bar:true},{k:'snow_depth',n:'積雪深',u:'cm',c:'#e6e6ff'},
 {k:'snow1h',n:'降雪量(1時間)',u:'cm',c:'#c9e8ff',bar:true},{k:'pressure',n:'気圧',u:'hPa',c:'#c9a6ff'},
 {k:'wind',n:'風速',u:'m/s',c:'#7be0a1'},{k:'sun1h',n:'日照時間(1時間)',u:'h',c:'#f5e663',bar:true}];
const CATS=[['ov','概要'],['det','確定論モデル'],['ens','アンサンブル'],['obs','アメダス甲府'],['near','周辺アメダス'],['off','公式予報'],['warn','警報・注意報'],['rad','雨雲レーダー']];

let RAW=null, CAT='ov', SEL={det:null,ens:null,v:'temperature_2m',ev:'temperature_2m',rng:48,hr:0,sort:{k:'dist',d:1}}, CH={}, MAPS=[], RT=0;

/* ---------- ライブラリ（失敗しても表は見られる） ---------- */
function loadScript(u){return new Promise((res,rej)=>{const s=document.createElement('script');s.src=u;s.onload=res;s.onerror=rej;document.head.appendChild(s);});}
let _cp=null,_lp=null;
function needChart(){
  if(window.Chart) return Promise.resolve(true);
  return _cp||(_cp=loadScript('https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js')
    .then(()=>{Chart.defaults.color=TICK;Chart.defaults.font.family='inherit';return true;}).catch(()=>false));
}
function needLeaf(){
  if(window.L) return Promise.resolve(true);
  return _lp||(_lp=(()=>{const l=document.createElement('link');l.rel='stylesheet';l.href='https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css';document.head.appendChild(l);
    return loadScript('https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js').then(()=>true).catch(()=>false);})());
}
function killAll(){Object.values(CH).forEach(c=>c.destroy());CH={};MAPS.forEach(m=>{try{m.remove()}catch(e){}});MAPS=[];}
function mk(id,cfg){const el=document.getElementById(id);if(!el||!window.Chart)return;if(CH[id])CH[id].destroy();CH[id]=new Chart(el,cfg);}

/* ---------- グラフ ---------- */
function lineCfg(labels,datasets,o={}){
  return {type:'line',data:{labels,datasets},options:{responsive:true,maintainAspectRatio:false,animation:false,spanGaps:false,
    interaction:{mode:'index',intersect:false},
    plugins:{legend:{display:o.legend!==false,labels:{boxWidth:12,filter:o.lfilter}},tooltip:{filter:o.tfilter}},
    scales:{x:{grid:{color:GRID},ticks:{maxTicksLimit:8,maxRotation:0,callback(v){const l=this.getLabelForValue(v);return /^\d{4}-\d\d-\d\dT/.test(l)?tl(l):l;}}},
            y:{grid:{color:GRID},beginAtZero:!!o.zero,title:{display:!!o.unit,text:o.unit||''}}}}};
}
function ds(label,data,color,o={}){
  return Object.assign({label,data,borderColor:color,backgroundColor:color,borderWidth:o.w||1.6,pointRadius:0,pointHitRadius:6,tension:0},o);
}
function barCfg(labels,datasets,o={}){
  const c=lineCfg(labels,datasets,o);c.type='bar';return c;
}

/* ---------- 地図 ---------- */
function svgMap(pts){
  if(!pts.length) return '';
  const W=600,H=320,pad=46,la=pts.map(p=>p.lat),lo=pts.map(p=>p.lon);
  const a0=Math.min(...la),a1=Math.max(...la),o0=Math.min(...lo),o1=Math.max(...lo),k=Math.cos((a0+a1)/2*Math.PI/180);
  const dx=Math.max((o1-o0)*k,0.02),dy=Math.max(a1-a0,0.02),sc=Math.min((W-2*pad)/dx,(H-2*pad)/dy);
  const X=p=>W/2+(p.lon-(o0+o1)/2)*k*sc, Y=p=>H/2-(p.lat-(a0+a1)/2)*sc;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:100%;background:#0e1a2e">`+
    pts.map(p=>`<circle cx="${X(p).toFixed(1)}" cy="${Y(p).toFixed(1)}" r="${p.r||7}" fill="${p.color||'#ff5a5a'}" stroke="#fff" stroke-width="1.5"/>`+
      (p.label&&!p.hover?`<text x="${(X(p)+10).toFixed(1)}" y="${(Y(p)+4).toFixed(1)}" fill="#f2f7ff" font-size="11">${esc(p.label)}</text>`:'')).join('')+
    `<text x="8" y="${H-8}" fill="#8ea3c0" font-size="11">地図タイルを読み込めないため簡易表示です（北が上）</text></svg>`;
}
async function drawMap(id,pts,o={}){
  const el=document.getElementById(id); if(!el) return;
  const tok=RT, ok=await needLeaf();
  if(tok!==RT||!document.getElementById(id)) return;
  if(!ok){el.innerHTML=svgMap(pts);return;}
  const m=L.map(el,{zoomControl:true,scrollWheelZoom:false}); MAPS.push(m);
  L.tileLayer('https://cyberjapandata.gsi.go.jp/xyz/pale/{z}/{x}/{y}.png',{minZoom:4,maxZoom:17,attribution:'地理院タイル'}).addTo(m);
  const b=[];
  pts.forEach(p=>{
    const c=L.circleMarker([p.lat,p.lon],{radius:p.r||7,color:p.ring||'#fff',weight:2,fillColor:p.color||'#ff5a5a',fillOpacity:.95}).addTo(m);
    if(p.popup) c.bindPopup(p.popup);
    if(p.label) c.bindTooltip(esc(p.label),{permanent:o.labels!==false&&!p.hover,direction:p.dir||'right',offset:[6,0],className:'lt'});
    b.push([p.lat,p.lon]);
  });
  if(b.length>1) m.fitBounds(b,{padding:[30,30],maxZoom:o.maxZoom||11}); else m.setView(b[0]||[35.66,138.57],o.zoom||11);
}
const KOFU=()=>({lat:RAW.location.lat,lon:RAW.location.lon,color:'#ff5a5a',r:9,label:RAW.location.name+'（予報地点）',
  popup:'<b>'+esc(RAW.location.name)+'</b><br>標高 '+RAW.location.elev+'m<br>'+RAW.location.lat+', '+RAW.location.lon});

/* ---------- 表・CSV ---------- */
function tbl(cols,rows,cls){
  return `<div class="tw ${cls||''}"><table class="dt"><thead><tr>${cols.map(c=>`<th class="${c.cls||''} ${c.sort?'sortable':''}" ${c.sort?`data-sort="${c.sort}"`:''}>${c.h}</th>`).join('')}</tr></thead>`+
    `<tbody>${rows.map(r=>`<tr>${cols.map(c=>`<td class="${c.cls||''}">${c.f(r)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
function dlCsv(name,head,rows){
  const q=v=>v==null?'':(/[",\n]/.test(String(v))?'"'+String(v).replace(/"/g,'""')+'"':v);
  const txt='\ufeff'+[head].concat(rows).map(r=>r.map(q).join(',')).join('\r\n');
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([txt],{type:'text/csv;charset=utf-8'}));a.download=name;
  document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(a.href),3000);
}
function stat(a){
  let n=0,mn=Infinity,mx=-Infinity,sum=0,first=null,last=null;
  a.forEach((x,i)=>{if(x==null)return;n++;sum+=x;if(x<mn)mn=x;if(x>mx)mx=x;if(first==null)first=i;last=i;});
  return {n,total:a.length,min:n?mn:null,max:n?mx:null,mean:n?sum/n:null,first,last};
}
function quant(sorted,q){
  if(!sorted.length) return null;
  const p=(sorted.length-1)*q,i=Math.floor(p),r=p-i;
  return i+1<sorted.length?sorted[i]*(1-r)+sorted[i+1]*r:sorted[i];
}
const badge=(ok,part,tip)=>ok?(part?`<span class="badge part" title="${esc(tip||'')}">△ 一部なし</span>`:'<span class="badge ok">✔ 取得</span>'):`<span class="badge ng" title="${esc(tip||'')}">✖ 失敗</span>`;
const dots=(vars,has)=>vars.map(v=>`<span class="vd ${has[v.k]?'on':''}" title="${v.n}${has[v.k]?'':'（なし）'}">${v.s}</span>`).join('');
function gridInfo(o){
  if(!o.grid||o.grid.lat==null) return null;
  return {lat:o.grid.lat,lon:o.grid.lon,d:km(RAW.location.lat,RAW.location.lon,o.grid.lat,o.grid.lon)};
}

/* ---------- ルーティング ---------- */
function parseHash(){
  const h=(location.hash||'').replace(/^#/,'').split('/');
  if(CATS.some(c=>c[0]===h[0])) CAT=h[0];
  if(h[1]&&(CAT==='det'||CAT==='ens')) SEL[CAT]=decodeURIComponent(h[1]);
}
function go(cat,key){location.hash='#'+cat+(key?'/'+encodeURIComponent(key):'');}
window.addEventListener('hashchange',()=>{parseHash();render();});

function drawNav(){
  $('#cats').innerHTML=CATS.map(c=>`<button class="tab" role="tab" data-c="${c[0]}" aria-selected="${c[0]===CAT}">${c[1]}</button>`).join('');
  $('#cats').querySelectorAll('.tab').forEach(b=>b.onclick=()=>go(b.dataset.c, b.dataset.c==='det'?SEL.det:b.dataset.c==='ens'?SEL.ens:''));
  let sub='';
  if(CAT==='det'||CAT==='ens'){
    const L_=RAW[CAT];
    sub=L_.map(m=>`<a class="chip ${m.key===SEL[CAT]?'on':''} ${m.ok?'':'bad'}" href="#${CAT}/${encodeURIComponent(m.key)}">${m.ok?'':'⚠ '}${esc(m.label)}</a>`).join('');
    if(CAT==='det') sub+=`<a class="chip ${SEL.det==='all'?'on':''}" href="#det/all">全モデル重ね</a>`;
  }
  $('#sub').innerHTML=sub;
}

async function render(){
  if(!RAW) return;
  const tok=++RT;
  killAll(); drawNav();
  if(CAT==='det'&&!SEL.det) SEL.det=(RAW.det.find(m=>m.ok)||RAW.det[0]||{}).key;
  if(CAT==='ens'&&!SEL.ens) SEL.ens=(RAW.ens.find(m=>m.ok)||RAW.ens[0]||{}).key;
  const fn={ov:vOverview,det:vDet,ens:vEns,obs:vObs,near:vNear,off:vOff,warn:vWarn,rad:vRad}[CAT];
  $('#main').innerHTML='';
  const chartOk=await needChart();
  if(tok!==RT) return;
  fn();
  if(!chartOk) $('#main').insertAdjacentHTML('afterbegin','<div class="note err">グラフ用のライブラリを読み込めなかったため、グラフは表示されません。表とCSVでは確認できます。</div>');
}

/* ======================= 概要 ======================= */
function vOverview(){
  const R=RAW, L_=R.live||{}, O=L_.official||{}, N=L_.nearby||{};
  const rows=[];
  R.det.forEach(m=>{
    const has={}; VARS.forEach(v=>has[v.k]=!!(m.vars&&m.vars[v.k]));
    const g=m.ok?gridInfo(m):null;
    rows.push({cat:'det',cl:'確定論',key:m.key,name:m.label,ok:m.ok,part:m.ok&&VARS.slice(0,5).some(v=>!has[v.k]),err:m.error,fetched:m.fetched,
      per:m.ok?tl(m.time[0])+' ～ '+tl(m.time[m.time.length-1]):'',cnt:m.ok?m.time.length+'時間':'',dots:dots(VARS,has),dist:g?g.d:null});
  });
  R.ens.forEach(m=>{
    const has={}; ENSV.forEach(v=>has[v.k]=!!(m.vars&&m.vars[v.k]));
    const g=m.ok?gridInfo(m):null;
    rows.push({cat:'ens',cl:'アンサンブル',key:m.key,name:m.label,ok:m.ok,part:m.ok&&ENSV.some(v=>!has[v.k]),err:m.error,fetched:m.fetched,
      per:m.ok?tl(m.time[0])+' ～ '+tl(m.time[m.time.length-1]):'',cnt:m.ok?m.n+'メンバー × '+m.time.length+'時間':'',dots:dots(ENSV,has),dist:g?g.d:null});
  });
  const ob=R.obs||[], obLast=ob.length?ob[ob.length-1].time:null;
  rows.push({cat:'obs',cl:'実測',name:'アメダス 甲府',ok:ob.length>0,err:'実測がまだ取り込まれていません',fetched:obLast?obLast.replace('T',' ')+'（最新の観測）':'',
    per:ob.length?tl(ob[0].time)+' ～ '+tl(obLast):'',cnt:ob.length+'時間分',dots:'',dist:0});
  rows.push({cat:'near',cl:'実測',name:'周辺アメダス（最新10分値）',ok:!!N.rows,part:!!N.stale,err:N.error,fetched:N.time?N.time.replace('T',' '):'',
    per:'',cnt:N.rows?(N.rows.length-1)+'地点':'',dots:'',dist:null});
  rows.push({cat:'off',cl:'公式',name:'気象庁 府県予報',ok:!!O.short,part:!!O.stale,err:O.forecast_error||O.error,fetched:O.short?O.report.replace('T',' ')+' 発表':'',per:'',cnt:O.short?(O.week||[]).length+'日（週間）':'',dots:'',dist:null});
  rows.push({cat:'warn',cl:'公式',name:'警報・注意報',ok:!!O.warnings,part:!!O.stale,err:O.warning_error||O.error,fetched:O.warnings?O.warnings.report.replace('T',' ')+' 発表':'',per:'',cnt:O.warnings?O.warnings.items.length+'件':'',dots:'',dist:null});
  const cols=[
    {h:'区分',cls:'l',f:r=>r.cl},
    {h:'データ源',cls:'l',f:r=>`<a href="#${r.cat}${r.key?'/'+encodeURIComponent(r.key):''}">${esc(r.name)}</a>`},
    {h:'状態',cls:'l',f:r=>badge(r.ok,r.part,r.err)},
    {h:'取得／発表',cls:'l',f:r=>esc(r.fetched||'')},
    {h:'期間',cls:'l',f:r=>esc(r.per)},
    {h:'量',cls:'l',f:r=>esc(r.cnt)},
    {h:'含まれる要素',cls:'l',f:r=>r.dots},
    {h:'格子点との距離',f:r=>r.dist==null?'':(r.dist<0.05?'—':f(r.dist,1)+' km')}];
  $('#main').innerHTML=`<h2>データ源の取得状況（名前を押すと、そのデータだけを表示）</h2>${tbl(cols,rows,'free')}
  <div class="note">「含まれる要素」は、そのデータ源が実際に返した要素（青）と返さなかった要素（灰）です。確定論モデルの要素は 気温・湿度・降水・気圧・850hPa気温・風・雲・降雪・0℃高度、アンサンブルは先頭の5つです。</div>
  <h2>地点マップ</h2><div id="mapOv" class="map tall"></div>
  <div class="note">赤: 予報地点（甲府）／ 水色（大）: 周辺アメダスで雪を観測 ／ 灰: 周辺アメダス（雪なし）／ 橙: 予報モデルの格子点（同じ位置は1つにまとめています。押すとモデル名）。</div>`;
  $('#gen').innerHTML='データ生成: '+esc(R.generated_at)+'　／　'+R.det.filter(m=>m.ok).length+'/'+R.det.length+' 確定論モデル、'+R.ens.filter(m=>m.ok).length+'/'+R.ens.length+' アンサンブルを取得';
  const pts=[KOFU()];
  (N.rows||[]).filter(x=>!x.self&&x.lat!=null).forEach(x=>pts.push(nearPt(x)));
  const grp={};
  R.det.concat(R.ens).filter(m=>m.ok&&gridInfo(m)).forEach(m=>{const g=gridInfo(m),k=g.lat.toFixed(3)+','+g.lon.toFixed(3);(grp[k]=grp[k]||{g,names:[]}).names.push(m.label);});
  Object.values(grp).forEach(o=>pts.push({lat:o.g.lat,lon:o.g.lon,color:'#ffb347',r:5,ring:'#7a4a00',label:o.names.length+'モデル',hover:true,dir:'top',
    popup:'<b>モデルの格子点</b>（甲府から '+o.g.d.toFixed(1)+' km）<br>'+o.names.map(esc).join('<br>')}));
  drawMap('mapOv',pts,{maxZoom:11});
}

/* ======================= 確定論モデル ======================= */
function hourRange(n){return SEL.rng>=999?n:Math.min(SEL.rng,n);}
function rangeChips(){return `<div class="row" style="margin:10px 0 6px">${[[48,'48時間'],[120,'5日'],[999,'全て']].map(a=>`<button class="chip ${SEL.rng===a[0]?'on':''}" data-rng="${a[0]}">${a[1]}</button>`).join('')}</div>`;}
function wireRange(fn){document.querySelectorAll('[data-rng]').forEach(b=>b.onclick=()=>{SEL.rng=+b.dataset.rng;document.querySelectorAll('[data-rng]').forEach(x=>x.classList.toggle('on',x===b));fn();});}

function vDet(){
  if(SEL.det==='all') return vDetAll();
  const m=RAW.det.find(x=>x.key===SEL.det);
  if(!m) {$('#main').innerHTML='<div class="note err">モデルが見つかりません</div>';return;}
  if(!m.ok){$('#main').innerHTML=`<h2>${esc(m.label)}</h2><div class="card"><div class="err">取得できませんでした: ${esc(m.error||'')}</div></div>`;return;}
  const g=gridInfo(m), T=m.time, vs=VARS.filter(v=>m.vars[v.k]);
  const stats=VARS.map(v=>({v,s:m.vars[v.k]?stat(m.vars[v.k]):null}));
  const kv=`<dl class="kv"><dt>モデル</dt><dd>${esc(m.label)}</dd><dt>取得時刻</dt><dd>${esc(m.fetched)}</dd>
    <dt>期間</dt><dd>${tl(T[0])} ～ ${tl(T[T.length-1])}（${T.length}時間）</dd>
    <dt>格子点</dt><dd>${g?g.lat.toFixed(3)+', '+g.lon.toFixed(3)+'（甲府から '+g.d.toFixed(1)+' km）':'座標が返されませんでした'}</dd>
    <dt>統合時の重み</dt><dd>${m.weight==null?'–':m.weight}</dd></dl>`;
  const cols=[{h:'要素',cls:'l',f:r=>r.v.n},{h:'有効',f:r=>r.s?r.s.n+'/'+r.s.total:'<span class="na">提供なし</span>'},
    {h:'最小',f:r=>r.s?f(r.s.min):''},{h:'最大',f:r=>r.s?f(r.s.max):''},{h:'平均',f:r=>r.s?f(r.s.mean):''},{h:'単位',cls:'l',f:r=>r.v.u}];
  $('#main').innerHTML=`<h2>${esc(m.label)}（確定論モデル 1本）</h2>
  <div class="grid2"><div class="card">${kv}<div style="margin-top:10px">${tbl(cols,stats,'free')}</div></div><div id="mapDet" class="map"></div></div>
  <h2>要素ごとの時系列</h2><div class="grid3">${vs.map(v=>`<div class="card"><h3>${v.n}（${v.u}）</h3><div class="cv"><canvas id="c_${v.k}"></canvas></div></div>`).join('')}</div>
  ${vs.length<VARS.length?`<div class="note">このモデルが返さなかった要素: ${VARS.filter(v=>!m.vars[v.k]).map(v=>v.n).join('、')}</div>`:''}
  <h2>時間別の値</h2><div class="row"><button class="iconbtn" id="csvDet">CSVで保存</button></div>${rangeChips()}<div id="tbl"></div>`;
  vs.forEach(v=>{
    const d=ds(v.n,m.vars[v.k],v.c,v.bar?{type:'bar',borderWidth:0}:{});
    mk('c_'+v.k,(v.bar?barCfg:lineCfg)(T,[d],{legend:false,unit:v.u,zero:!!v.bar}));
  });
  const draw=()=>{
    const n=hourRange(T.length);
    $('#tbl').innerHTML=tbl([{h:'時刻',cls:'l',f:i=>tl(T[i])}].concat(vs.map(v=>({h:v.n+'<br><small>'+v.u+'</small>',f:i=>f(m.vars[v.k][i],v.k==='precipitation'||v.k==='snowfall'?2:1)}))),
      Array.from({length:n},(_, i)=>i));
  };
  draw(); wireRange(draw);
  $('#csvDet').onclick=()=>dlCsv('det_'+m.key+'.csv',['time'].concat(vs.map(v=>v.k)),T.map((t,i)=>[t].concat(vs.map(v=>m.vars[v.k][i]))));
  const pts=[KOFU()]; if(g) pts.push({lat:g.lat,lon:g.lon,color:'#ffb347',r:7,ring:'#7a4a00',label:m.label+' の格子点',dir:'bottom',popup:esc(m.label)+'<br>甲府から '+g.d.toFixed(1)+' km'});
  drawMap('mapDet',pts,{maxZoom:12});
}

function vDetAll(){
  const ms=RAW.det.filter(m=>m.ok);
  const vk=SEL.v, V=VARS.find(v=>v.k===vk)||VARS[0];
  const have=ms.filter(m=>m.vars[V.k]);
  const COL=['#9fd3ff','#ffc27a','#7be0a1','#ff8fa8','#c9a6ff','#f5e663','#7fdbda','#ff9f6b','#6fa8ff','#e6a1ff','#b6e36b','#ff7a7a'];
  const times=Array.from(new Set(ms.flatMap(m=>m.time))).sort();
  const ix=new Map(times.map((t,i)=>[t,i]));
  const series=have.map((m,i)=>{const a=new Array(times.length).fill(null);m.time.forEach((t,j)=>a[ix.get(t)]=m.vars[V.k][j]);return {m,a,c:COL[i%COL.length]};});
  $('#main').innerHTML=`<h2>全モデルを重ねて比較</h2>
  <div class="row">${VARS.map(v=>`<button class="chip ${v.k===V.k?'on':''}" data-v="${v.k}">${v.n}</button>`).join('')}</div>
  ${rangeChips()}
  <div class="card" style="margin-top:8px"><div class="cv big"><canvas id="cAll"></canvas></div></div>
  <div class="note">線を1本ずつ見たいときは、上の「モデル名」で単体表示にしてください。凡例を押すと線を消せます。</div>
  <h2>${V.n}: モデルごとの要約</h2><div id="allTbl"></div>`;
  document.querySelectorAll('[data-v]').forEach(b=>b.onclick=()=>{SEL.v=b.dataset.v;render();});
  const draw=()=>{
    const n=hourRange(times.length), lab=times.slice(0,n);
    mk('cAll',(V.bar?barCfg:lineCfg)(lab,series.map(s=>ds(s.m.label,s.a.slice(0,n),s.c,V.bar?{type:'bar',borderWidth:0}:{})),{unit:V.u,zero:!!V.bar}));
  };
  draw(); wireRange(draw);
  const cols=[{h:'モデル',cls:'l',f:s=>`<a href="#det/${encodeURIComponent(s.m.key)}">${esc(s.m.label)}</a>`},
    {h:'有効/時間数',f:s=>{const t=stat(s.m.vars[V.k]);return t.n+'/'+t.total;}},
    {h:'最小',f:s=>f(stat(s.m.vars[V.k]).min)},{h:'最大',f:s=>f(stat(s.m.vars[V.k]).max)},{h:'平均',f:s=>f(stat(s.m.vars[V.k]).mean)},
    {h:'先頭の値',f:s=>f(s.m.vars[V.k][0])},
    {h:'24時間の合計',f:s=>V.bar?f(s.m.vars[V.k].slice(0,24).reduce((a,x)=>a+(x||0),0),1):''}];
  const miss=ms.filter(m=>!m.vars[V.k]);
  $('#allTbl').innerHTML=tbl(cols,series,'free')+(miss.length?`<div class="note">この要素を返さなかったモデル: ${miss.map(m=>esc(m.label)).join('、')}</div>`:'')+
    (RAW.det.some(m=>!m.ok)?`<div class="note err">取得失敗: ${RAW.det.filter(m=>!m.ok).map(m=>esc(m.label)).join('、')}</div>`:'');
}

/* ======================= アンサンブル ======================= */
function vEns(){
  const m=RAW.ens.find(x=>x.key===SEL.ens);
  if(!m){$('#main').innerHTML='<div class="note err">モデルが見つかりません</div>';return;}
  if(!m.ok){$('#main').innerHTML=`<h2>${esc(m.label)}</h2><div class="card"><div class="err">取得できませんでした: ${esc(m.error||'')}</div></div>`;return;}
  const vs=ENSV.filter(v=>m.vars[v.k]);
  if(!vs.length){$('#main').innerHTML='<div class="note err">要素がありません</div>';return;}
  if(!vs.some(v=>v.k===SEL.ev)) SEL.ev=vs[0].k;
  const V=vs.find(v=>v.k===SEL.ev), M=m.vars[V.k], T=m.time, N=M.length, g=gridInfo(m);
  const st=T.map((_,i)=>{const a=M.map(r=>r[i]).filter(x=>x!=null).sort((p,q)=>p-q);
    return {n:a.length,min:a[0],p10:quant(a,.1),p50:quant(a,.5),p90:quant(a,.9),max:a[a.length-1],mean:a.length?a.reduce((p,q)=>p+q,0)/a.length:null,vals:a};});
  const kv=`<dl class="kv"><dt>モデル</dt><dd>${esc(m.label)}</dd><dt>メンバー数</dt><dd>${m.n}</dd><dt>取得時刻</dt><dd>${esc(m.fetched)}</dd>
    <dt>期間</dt><dd>${tl(T[0])} ～ ${tl(T[T.length-1])}（${T.length}時間）</dd>
    <dt>格子点</dt><dd>${g?g.lat.toFixed(3)+', '+g.lon.toFixed(3)+'（甲府から '+g.d.toFixed(1)+' km）':'座標が返されませんでした'}</dd>
    <dt>含まれる要素</dt><dd>${dots(ENSV,Object.fromEntries(ENSV.map(v=>[v.k,!!m.vars[v.k]])))}</dd>
    <dt>統合時の重み</dt><dd>${m.weight==null?'–':m.weight}</dd></dl>`;
  if(SEL.hr>=T.length) SEL.hr=0;
  $('#main').innerHTML=`<h2>${esc(m.label)}（アンサンブル 1本）</h2>
  <div class="grid2"><div class="card">${kv}</div><div id="mapEns" class="map"></div></div>
  <h2>メンバーごとの時系列</h2>
  <div class="row">${vs.map(v=>`<button class="chip ${v.k===V.k?'on':''}" data-ev="${v.k}">${v.n}</button>`).join('')}</div>
  ${rangeChips()}
  <div class="card" style="margin-top:8px"><div class="cv big"><canvas id="cEns"></canvas></div></div>
  <div class="note">細い線: 各メンバー（${N}本）／ 太線: 中央値 ／ 帯: 10〜90パーセンタイル。</div>
  <h2>ある時刻のメンバーのばらつき</h2>
  <div class="row"><input type="range" id="hr" min="0" max="${T.length-1}" value="${SEL.hr}"><b id="hrL"></b></div>
  <div class="card" style="margin-top:8px"><div class="cv"><canvas id="cHist"></canvas></div><div class="note" id="histNote"></div></div>
  <h2>統計の表</h2><div class="row"><button class="iconbtn" id="csvStat">統計をCSVで保存</button><button class="iconbtn" id="csvMem">全メンバーをCSVで保存</button></div>${rangeChips().replace(/data-rng/g,'data-rng2')}<div id="tbl"></div>`;
  document.querySelectorAll('[data-ev]').forEach(b=>b.onclick=()=>{SEL.ev=b.dataset.ev;render();});
  const drawMain=()=>{
    const n=hourRange(T.length), lab=T.slice(0,n);
    const mem=M.map(r=>ds('',r.slice(0,n),'rgba(159,211,255,.22)',{borderWidth:.7,hidden:false}));
    const ds_=[ds('10%',st.slice(0,n).map(s=>s.p10),'rgba(0,0,0,0)',{borderWidth:0}),
      ds('10〜90%',st.slice(0,n).map(s=>s.p90),'rgba(0,0,0,0)',{borderWidth:0,fill:'-1',backgroundColor:'rgba(255,194,122,.22)'}),
      ds('中央値',st.slice(0,n).map(s=>s.p50),'#ffc27a',{borderWidth:2.4})].concat(mem);
    mk('cEns',lineCfg(lab,ds_,{unit:V.u,zero:V.k==='precipitation',lfilter:it=>it.datasetIndex>=1&&it.datasetIndex<=2,tfilter:it=>it.datasetIndex<3}));
  };
  const drawHist=()=>{
    const i=SEL.hr, a=st[i].vals;
    $('#hrL').textContent=tl(T[i]);
    if(!a.length){$('#histNote').textContent='この時刻のデータがありません';mk('cHist',{type:'bar',data:{labels:[],datasets:[]}});return;}
    const lo=a[0],hi=a[a.length-1],B=Math.min(12,Math.max(1,Math.ceil(Math.sqrt(a.length)))),w=(hi-lo)/B||1;
    const cnt=new Array(B).fill(0); a.forEach(x=>{cnt[Math.min(B-1,Math.floor((x-lo)/w))]++;});
    const labs=cnt.map((_,k)=>(lo+k*w).toFixed(V.k==='precipitation'?2:1));
    mk('cHist',barCfg(labs,[ds('メンバー数',cnt,'#9fd3ff',{borderWidth:0})],{legend:false,unit:'メンバー数',zero:true}));
    const s=st[i];
    $('#histNote').textContent=`${tl(T[i])}　有効 ${s.n}/${N}　最小 ${s.min.toFixed(1)}　中央値 ${s.p50.toFixed(1)}　最大 ${s.max.toFixed(1)}（${V.u}）`+
      (V.k==='precipitation'?`　／　降水ありのメンバー ${a.filter(x=>x>0).length}/${s.n}`:'');
  };
  const drawT=()=>{
    const n=hourRange(T.length);
    $('#tbl').innerHTML=tbl([{h:'時刻',cls:'l',f:i=>tl(T[i])},{h:'有効数',f:i=>st[i].n},{h:'最小',f:i=>f(st[i].min)},{h:'10%',f:i=>f(st[i].p10)},
      {h:'中央値',f:i=>f(st[i].p50)},{h:'90%',f:i=>f(st[i].p90)},{h:'最大',f:i=>f(st[i].max)},{h:'平均',f:i=>f(st[i].mean)}],Array.from({length:n},(_, i)=>i));
  };
  drawMain();drawHist();drawT();
  wireRange(()=>{drawMain();drawT();document.querySelectorAll('[data-rng2]').forEach(x=>x.classList.toggle('on',+x.dataset.rng2===SEL.rng));});
  document.querySelectorAll('[data-rng2]').forEach(b=>b.onclick=()=>{SEL.rng=+b.dataset.rng2;document.querySelectorAll('[data-rng],[data-rng2]').forEach(x=>x.classList.toggle('on',+(x.dataset.rng||x.dataset.rng2)===SEL.rng));drawMain();drawT();});
  $('#hr').oninput=e=>{SEL.hr=+e.target.value;drawHist();};
  $('#csvStat').onclick=()=>dlCsv('ens_'+m.key+'_'+V.k+'_stats.csv',['time','n','min','p10','median','p90','max','mean'],T.map((t,i)=>[t,st[i].n,st[i].min,st[i].p10,st[i].p50,st[i].p90,st[i].max,st[i].mean]));
  $('#csvMem').onclick=()=>dlCsv('ens_'+m.key+'_'+V.k+'_members.csv',['time'].concat(M.map((_,k)=>'m'+(k+1))),T.map((t,i)=>[t].concat(M.map(r=>r[i]))));
  const pts=[KOFU()]; if(g) pts.push({lat:g.lat,lon:g.lon,color:'#ffb347',r:7,ring:'#7a4a00',label:m.label+' の格子点',dir:'bottom',popup:esc(m.label)+'<br>甲府から '+g.d.toFixed(1)+' km'});
  drawMap('mapEns',pts,{maxZoom:12});
}

/* ======================= アメダス甲府 ======================= */
function vObs(){
  const ob=RAW.obs||[];
  if(!ob.length){$('#main').innerHTML='<h2>アメダス 甲府</h2><div class="card err">実測データがまだありません（取り込み前）。</div>';return;}
  const T=ob.map(r=>r.time), vs=OBSV.filter(v=>ob.some(r=>r[v.k]!=null));
  const flagCol=r=>r.snow_flag==null?'#2a3b57':r.snow_flag?'#9fd3ff':'#5f7896';
  const srcN={amedas_snow:'アメダス降雪',proxy:'代理（降水あり＆気温0.5℃以下）',manual:'手動登録'};
  const cnt={}; ob.forEach(r=>{const k=r.flag_src||'なし';cnt[k]=(cnt[k]||0)+1;});
  $('#main').innerHTML=`<h2>アメダス 甲府（実測 ${ob.length}時間分）</h2>
  <div class="grid2"><div class="card"><dl class="kv"><dt>観測所</dt><dd>${esc(RAW.location.name)}（コード ${esc((RAW.location.amedas||''))}）</dd>
   <dt>期間</dt><dd>${tl(T[0])} ～ ${tl(T[T.length-1])}</dd>
   <dt>雪の判定の根拠</dt><dd>${Object.keys(cnt).map(k=>esc(srcN[k]||k)+' '+cnt[k]+'件').join('／')}</dd></dl>
   <div style="margin-top:10px"><h3>雪あり判定（1マス=1時間、左が古い）</h3><div class="strip">${ob.map(r=>`<i style="background:${flagCol(r)}" title="${tl(r.time)} ${r.snow_flag==null?'判定なし':r.snow_flag?'雪あり':'雪なし'}"></i>`).join('')}</div>
   <div class="note">水色: 雪あり ／ 灰青: 雪なし ／ 暗色: 判定なし</div></div></div><div id="mapObs" class="map"></div></div>
  <h2>要素ごとの時系列</h2><div class="grid3">${vs.map(v=>`<div class="card"><h3>${v.n}（${v.u}）</h3><div class="cv"><canvas id="o_${v.k}"></canvas></div></div>`).join('')}</div>
  <h2>時間別の値</h2><div class="row"><button class="iconbtn" id="csvObs">CSVで保存</button></div>
  <div id="tbl" style="margin-top:8px"></div>`;
  vs.forEach(v=>mk('o_'+v.k,(v.bar?barCfg:lineCfg)(T,[ds(v.n,ob.map(r=>r[v.k]),v.c,v.bar?{type:'bar',borderWidth:0}:{})],{legend:false,unit:v.u,zero:!!v.bar})));
  const rows=ob.slice().reverse();
  $('#tbl').innerHTML=tbl([{h:'時刻',cls:'l',f:r=>tl(r.time)}].concat(vs.map(v=>({h:v.n+'<br><small>'+v.u+'</small>',f:r=>f(r[v.k],1)})),
    [{h:'雪判定',cls:'l',f:r=>r.snow_flag==null?'<span class="na">–</span>':(r.snow_flag?'雪あり':'雪なし')},{h:'根拠',cls:'l',f:r=>esc(srcN[r.flag_src]||r.flag_src||'')}]),rows);
  $('#csvObs').onclick=()=>dlCsv('amedas_kofu.csv',['time'].concat(OBSV.map(v=>v.k),['snow_flag','flag_src']),ob.map(r=>[r.time].concat(OBSV.map(v=>r[v.k]),[r.snow_flag,r.flag_src])));
  drawMap('mapObs',[KOFU()],{zoom:11});
}

/* ======================= 周辺アメダス ======================= */
function nearPt(x){
  const col=x.snow===1?'#9fd3ff':x.snow===0?'#5f7896':'#444c5c';
  const lab=x.name+(x.temp!=null?' '+(+x.temp).toFixed(1)+'℃':'')+((x.snow_depth||0)>0?' ❄'+x.snow_depth+'cm':'');
  return {lat:x.lat,lon:x.lon,color:col,r:x.snow===1?10:7,label:lab,
    popup:`<b>${esc(x.name)}</b>（${f(x.dist,1)}km／標高${x.alt==null?'–':x.alt}m）<br>気温 ${f(x.temp)}℃　湿度 ${f(x.rh,0)}%<br>降水1h ${f(x.precip1h)}mm　積雪深 ${f(x.snow_depth,0)}cm<br>雪判定: ${x.snow==null?'なし':(x.snow?'雪あり':'雪なし')}（${esc(x.basis||'')}）`};
}
function vNear(){
  const N=(RAW.live||{}).nearby||{};
  if(!N.rows){$('#main').innerHTML=`<h2>周辺アメダス</h2><div class="card err">取得できませんでした: ${esc(N.error||'')}</div>`;return;}
  const rows=N.rows, S=N.summary||{};
  $('#main').innerHTML=`<h2>周辺アメダス（最新10分値）</h2>
  <div class="note">観測時刻 ${esc(N.time.replace('T',' '))}${N.stale?'　<span class="err">※取得に失敗したため前回の値です</span>':''}　／　周辺${S.stations}地点、雪を観測 ${S.snowing}地点、積雪あり ${S.with_depth}地点（最大 ${S.max_depth}cm）</div>
  <div id="mapNear" class="map tall"></div>
  <div class="note">水色（大）: 雪あり ／ 灰青: 雪なし ／ 暗色: 判定なし ／ 赤: 甲府。ラベルは 気温・積雪深。</div>
  <div class="grid2" style="margin-top:12px"><div class="card"><h3>気温（℃）— 距離の近い順</h3><div class="cv"><canvas id="nT"></canvas></div></div>
  <div class="card"><h3>積雪深（cm）— 距離の近い順</h3><div class="cv"><canvas id="nD"></canvas></div></div></div>
  <h2>一覧</h2><div class="row"><button class="iconbtn" id="csvNear">CSVで保存</button></div><div id="tbl" style="margin-top:8px"></div>
  <div class="note">見出しを押すと並べ替えできます。「根拠」は 実測=アメダスの降雪観測、推定=降水と気温からの推定。</div>`;
  const byDist=rows.slice().sort((a,b)=>a.dist-b.dist), lab=byDist.map(x=>x.name.replace('（自地点）',''));
  mk('nT',barCfg(lab,[ds('気温',byDist.map(x=>x.temp),'#ffc27a',{borderWidth:0})],{legend:false,unit:'℃'}));
  mk('nD',barCfg(lab,[ds('積雪深',byDist.map(x=>x.snow_depth),'#e6e6ff',{borderWidth:0})],{legend:false,unit:'cm',zero:true}));
  const C=[{h:'地点',cls:'l',sort:'name',f:r=>esc(r.name)},{h:'距離(km)',sort:'dist',f:r=>f(r.dist,1)},{h:'標高(m)',sort:'alt',f:r=>r.alt==null?'–':r.alt},
    {h:'気温℃',sort:'temp',f:r=>f(r.temp)},{h:'湿度%',sort:'rh',f:r=>f(r.rh,0)},{h:'降水1h',sort:'precip1h',f:r=>f(r.precip1h)},
    {h:'積雪深cm',sort:'snow_depth',f:r=>f(r.snow_depth,0)},{h:'降雪1h',sort:'snow1h',f:r=>f(r.snow1h)},{h:'風速',sort:'wind',f:r=>f(r.wind)},{h:'日照h',sort:'sun1h',f:r=>f(r.sun1h)},
    {h:'雪判定',cls:'l',sort:'snow',f:r=>r.snow==null?'<span class="na">–</span>':(r.snow?'雪あり':'雪なし')+'<small class="na"> '+esc(r.basis||'')+'</small>'}];
  const draw=()=>{
    const k=SEL.sort.k,d=SEL.sort.d;
    const a=rows.slice().sort((p,q)=>{const x=p[k],y=q[k];if(x==null&&y==null)return 0;if(x==null)return 1;if(y==null)return -1;
      return (typeof x==='string'?x.localeCompare(y):x-y)*d;});
    $('#tbl').innerHTML=tbl(C,a,'free');
    document.querySelectorAll('th[data-sort]').forEach(th=>th.onclick=()=>{const kk=th.dataset.sort;SEL.sort={k:kk,d:SEL.sort.k===kk?-SEL.sort.d:1};draw();});
  };
  draw();
  $('#csvNear').onclick=()=>dlCsv('amedas_nearby.csv',['code','name','dist_km','alt_m','lat','lon','temp','rh','precip1h','snow_depth','snow1h','wind','sun1h','snow','basis'],
    rows.map(r=>[r.code,r.name,r.dist,r.alt,r.lat,r.lon,r.temp,r.rh,r.precip1h,r.snow_depth,r.snow1h,r.wind,r.sun1h,r.snow,r.basis]));
  const pts=[KOFU()];
  rows.filter(x=>!x.self&&x.lat!=null).forEach(x=>pts.push(nearPt(x)));
  drawMap('mapNear',pts,{maxZoom:11});
}

/* ======================= 公式予報 ======================= */
function vOff(){
  const O=(RAW.live||{}).official||{};
  if(!O.short){$('#main').innerHTML=`<h2>気象庁 府県予報</h2><div class="card err">取得できませんでした: ${esc(O.forecast_error||O.error||'')}</div>`;return;}
  const S=O.short, W=O.week||[];
  const pops=S.pops.map(p=>({t:p.time,v:num(p.pop)})), temps=S.temps.map(p=>({t:p.time,v:num(p.temp)}));
  $('#main').innerHTML=`<h2>気象庁 府県予報（${esc(O.area_name||'')}）</h2>
  <div class="note">発表: ${esc(O.report.replace('T',' '))}　／　取得: ${esc(O.fetched||'')}${O.stale?'　<span class="err">※前回の値です</span>':''}</div>
  <div class="grid2"><div class="card"><h3>天気（今日・明日・明後日）</h3>${tbl([{h:'日付',cls:'l',f:r=>esc(r.date)},{h:'天気',cls:'l wrapc',f:r=>esc(r.text)}],S.weather,'free')}</div>
  <div class="card"><h3>降水確率（%）</h3><div class="cv"><canvas id="oPop"></canvas></div><h3 style="margin-top:10px">気温（℃）</h3><div class="cv"><canvas id="oTmp"></canvas></div></div></div>
  <h2>週間予報</h2>
  <div class="grid2"><div class="card"><div class="cv big"><canvas id="oWeek"></canvas></div></div>
  <div class="card">${tbl([{h:'日付',cls:'l',f:r=>esc(r.date)},{h:'降水確率%',f:r=>esc(r.pop===''?'–':r.pop)},{h:'信頼度',f:r=>esc(r.rel||'–')},{h:'最低℃',f:r=>esc(r.tmin===''?'–':r.tmin)},{h:'最高℃',f:r=>esc(r.tmax===''?'–':r.tmax)}],W,'free')}</div></div>
  <div class="note">気象庁の公式予報は参考表示で、SNOW CAST の降雪確率の計算には使っていません。</div>`;
  mk('oPop',barCfg(pops.map(p=>p.t),[ds('降水確率',pops.map(p=>p.v),'#9fd3ff',{borderWidth:0})],{legend:false,unit:'%',zero:true}));
  mk('oTmp',lineCfg(temps.map(p=>p.t),[ds('気温',temps.map(p=>p.v),'#ffc27a',{pointRadius:3})],{legend:false,unit:'℃'}));
  const wc=lineCfg(W.map(w=>w.date.slice(5)),[
    ds('降水確率(%)',W.map(w=>num(w.pop)),'rgba(159,211,255,.55)',{type:'bar',borderWidth:0,yAxisID:'y1',order:2}),
    ds('最高気温',W.map(w=>num(w.tmax)),'#ff9f6b',{pointRadius:3,order:1}),ds('最低気温',W.map(w=>num(w.tmin)),'#7fdbda',{pointRadius:3,order:1})],{unit:'℃'});
  wc.options.scales.y1={position:'right',min:0,max:100,grid:{drawOnChartArea:false},title:{display:true,text:'降水確率 %'}};
  mk('oWeek',wc);
}

/* ======================= 警報・注意報 ======================= */
function vWarn(){
  const O=(RAW.live||{}).official||{}, W=O.warnings;
  if(!W){$('#main').innerHTML=`<h2>警報・注意報</h2><div class="card err">取得できませんでした: ${esc(O.warning_error||O.error||'')}</div>`;return;}
  $('#main').innerHTML=`<h2>警報・注意報（甲府市）</h2>
  <div class="note">発表: ${esc(W.report.replace('T',' '))}${O.stale?'　<span class="err">※前回の値です</span>':''}　／　対象地域の検索: ${W.found?'見つかりました':'<span class="err">見つかりませんでした（発表なしの場合もあります）</span>'}</div>
  <div class="card">${W.items.length?tbl([{h:'名称',cls:'l',f:r=>(r.snow?'❄️ ':'')+esc(r.name)},{h:'コード',f:r=>esc(r.code)},{h:'状態',cls:'l',f:r=>esc(r.status)},{h:'雪に関係',cls:'l',f:r=>r.snow?'はい':''}],W.items,'free'):'現在、発表中の警報・注意報はありません。'}</div>
  <div class="note">❄️ は雪に関係する種別（大雪・風雪・なだれ・着雪など）です。</div>`;
}

/* ======================= 雨雲レーダー ======================= */
const RAD={map:null,layer:null,frames:[],i:0,timer:null};
async function radarFrames(){
  const J='https://www.jma.go.jp/bosai/jmatile/data/nowc/';
  const [a,b]=await Promise.all(['targetTimes_N1.json','targetTimes_N2.json'].map(x=>fetch(J+x).then(r=>r.json())));
  const ok=x=>(x.elements||[]).includes('hrpns');
  const past=a.filter(ok).sort((p,q)=>p.validtime<q.validtime?-1:1).slice(-7).map(x=>({b:x.basetime,v:x.validtime,fc:false}));
  const last=past.length?past[past.length-1].v:'', seen={}, fut=[];
  b.filter(ok).sort((p,q)=>p.validtime<q.validtime?-1:1).forEach(x=>{if(x.validtime>last&&!seen[x.validtime]){seen[x.validtime]=1;fut.push({b:x.basetime,v:x.validtime,fc:true});}});
  return past.concat(fut);
}
function radarShow(i){
  const fr=RAD.frames[i]; if(!fr||!RAD.map) return; RAD.i=i;
  if(RAD.layer) RAD.map.removeLayer(RAD.layer);
  RAD.layer=L.tileLayer('https://www.jma.go.jp/bosai/jmatile/data/nowc/'+fr.b+'/none/'+fr.v+'/surf/hrpns/{z}/{x}/{y}.png',{opacity:.75,minZoom:4,maxZoom:12,maxNativeZoom:10}).addTo(RAD.map);
  const t=fr.v; $('#radTime').textContent=t.slice(4,6)+'/'+t.slice(6,8)+' '+t.slice(8,10)+':'+t.slice(10,12)+(fr.fc?'（予測）':'（実況）')+'　'+(i+1)+'/'+RAD.frames.length;
}
async function vRad(){
  const tok=RT;
  $('#main').innerHTML=`<h2>雨雲レーダー（気象庁 高解像度降水ナウキャスト）</h2>
  <div class="row"><button class="iconbtn" id="radPlay">再生</button><span class="note" id="radTime">読み込み中…</span></div>
  <div id="radMap" class="map tall" style="margin-top:8px"></div>
  <div class="note">ブラウザが気象庁から直接取得します（実況＋1時間先まで）。色は降水の強さ（薄い青→赤で強い）。赤い点が甲府です。</div>`;
  if(!(await needLeaf())){$('#radTime').textContent='地図ライブラリを読み込めませんでした。';return;}
  if(tok!==RT) return;
  clearInterval(RAD.timer); RAD.timer=null;
  const m=L.map('radMap',{scrollWheelZoom:false}).setView([RAW.location.lat,RAW.location.lon],8); MAPS.push(m); RAD.map=m;
  L.tileLayer('https://cyberjapandata.gsi.go.jp/xyz/pale/{z}/{x}/{y}.png',{minZoom:4,maxZoom:14,attribution:'地理院タイル'}).addTo(m);
  L.circleMarker([RAW.location.lat,RAW.location.lon],{radius:6,color:'#fff',weight:2,fillColor:'#ff5a5a',fillOpacity:1}).addTo(m);
  $('#radPlay').onclick=()=>{
    if(RAD.timer){clearInterval(RAD.timer);RAD.timer=null;$('#radPlay').textContent='再生';return;}
    $('#radPlay').textContent='停止';RAD.timer=setInterval(()=>radarShow((RAD.i+1)%Math.max(1,RAD.frames.length)),700);
  };
  try{
    RAD.frames=await radarFrames();
    if(tok!==RT) return;
    if(!RAD.frames.length) throw new Error('no frames');
    const k=RAD.frames.findIndex(x=>x.fc); radarShow(k>0?k-1:RAD.frames.length-1);
  }catch(e){ if(tok===RT) $('#radTime').innerHTML='<span class="err">レーダー画像を取得できませんでした。気象庁のサイトで確認してください。</span>'; }
}

/* ---------- 起動 ---------- */
async function load(){
  $('#gen').textContent='読み込み中…';
  try{
    const js=await fetch('/api/raw').then(r=>r.json());
    if(js.error) throw new Error(js.error);
    RAW=js;
  }catch(e){
    $('#gen').innerHTML='<span class="err">データを読み込めませんでした（'+esc(e.message||e)+'）。しばらくしてから再読み込みしてください。</span>';
    $('#main').innerHTML='';
    return;
  }
  $('#gen').textContent='データ生成: '+RAW.generated_at;
  parseHash(); render();
}
$('#btnReload').onclick=load;
load();
</script>
</body></html>
"""
