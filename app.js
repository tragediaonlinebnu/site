const WS_URL="wss://monitoramento.defesacivil.sc.gov.br/graphql";
const CLIENT="secretaria-de-defesa-civil";
const TAGS=`query Tags_data { tags_data(clients: ["${CLIENT}"]) { qualle_meteorologia { codigo name { prefix general local } show timestamp position { bacia latitude longitude regiao altitude } data { rio { rio_nome { value } rio_nivel { value show { value } format { value } unit { value } } rio_nivel_tendencia { value show { value } } } chuva { acumulado { min005 { value show { value } format { value } unit { value } } h003 { value show { value } unit { value } } h006 { value show { value } unit { value } } h012 { value show { value } unit { value } } h024 { value show { value } unit { value } } h048 { value show { value } unit { value } } h096 { value show { value } unit { value } } } } barramento { nivel { percentual { value show { value } unit { value } } montante { value show { value } unit { value } } jusante { value show { value } unit { value } } vertido { value show { value } unit { value } } } capacidade { atual { value show { value } unit { value } } maxima { value show { value } unit { value } } } comportas { comporta_1 { estado { value } habilitada { value } nome { value show { value } } } comporta_2 { estado { value } habilitada { value } nome { value show { value } } } comporta_3 { estado { value } habilitada { value } nome { value show { value } } } comporta_4 { estado { value } habilitada { value } nome { value show { value } } } comporta_5 { estado { value } habilitada { value } nome { value show { value } } } comporta_6 { estado { value } habilitada { value } nome { value show { value } } } comporta_7 { estado { value } habilitada { value } nome { value show { value } } } comporta_8 { estado { value } habilitada { value } nome { value show { value } } } comporta_9 { estado { value } habilitada { value } nome { value show { value } } } comporta_10 { estado { value } habilitada { value } nome { value show { value } } } } } } type filter { relacao { tem_chuva_acumulada tem_nivel_do_rio tem_barragem } } } } }`;

let ws=null, seq=0, pending=new Map(), allStations=[], rainStations=[], selected=new Set(), dams=[];

const $=id=>document.getElementById(id);
function status(t,c=""){$("status").textContent=t;$("status").className="status "+c}
function raw(v){return v&&typeof v==="object"&&"value" in v?v.value:v}
function num(v){v=raw(v);if(v==null||v==="")return null;if(typeof v==="number")return Number.isFinite(v)?v:null;let n=parseFloat(String(v).replace(/\s/g,"").replace(",","."));return Number.isFinite(n)?n:null}
function txt(v){return raw(v)==null?"":String(raw(v))}
function norm(v){return txt(v).normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase()}
function esc(v){return String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function name(s){return [s?.name?.local,s?.name?.general,s?.name?.prefix].map(txt).filter(Boolean).join(" — ")||txt(s?.codigo)||"Estação"}
function rain(s,k){return num(s?.data?.chuva?.acumulado?.[k]?.show?.value) ?? num(s?.data?.chuva?.acumulado?.[k]?.value)}

function request(query,variables={}){
 return new Promise((resolve,reject)=>{
  if(!ws||ws.readyState!==WebSocket.OPEN){reject(new Error("WebSocket indisponível"));return}
  const id=String(++seq),timer=setTimeout(()=>{if(pending.has(id)){pending.delete(id);reject(new Error("Tempo esgotado"))}},20000);
  pending.set(id,{resolve,reject,timer});
  ws.send(JSON.stringify({id,type:"subscribe",payload:{query,variables}}));
 })
}
function connect(){
 return new Promise((resolve,reject)=>{
  status("Conectando ao WebSocket oficial…");
  ws=new WebSocket(WS_URL,"graphql-transport-ws");let ack=false;
  ws.onopen=()=>ws.send(JSON.stringify({type:"connection_init",payload:{}}));
  ws.onmessage=e=>{let m;try{m=JSON.parse(e.data)}catch{return}
   if(m.type==="ping"){try{ws.send(JSON.stringify({type:"pong"}))}catch{};return}
   if(m.type==="connection_ack"){ack=true;resolve();return}
   const p=pending.get(String(m.id??""));if(!p)return;
   if(m.type==="next"){clearTimeout(p.timer);pending.delete(String(m.id));p.resolve(m.payload?.data??m.payload)}
   else if(m.type==="error"){clearTimeout(p.timer);pending.delete(String(m.id));p.reject(new Error(JSON.stringify(m.payload)))}
  };
  ws.onerror=()=>{if(!ack)reject(new Error("Falha na conexão WebSocket"))};
  ws.onclose=()=>{for(const [id,p] of pending){clearTimeout(p.timer);p.reject(new Error("Conexão fechada"));pending.delete(id)}};
 })
}

// Seleção por MUNICÍPIO: somente as cidades do Alto Vale (AMAVI)
// e do Médio Vale (AMVE). Não usamos caixa geográfica, sub-bacia ou
// "qualquer coisa com Itajaí" para decidir a seleção.
const ALTO_VALE = [
  "Agrolandia","Agronomica","Atalanta","Aurora","Braco do Trombudo",
  "Chapadao do Lageado","Dona Emma","Ibirama","Imbuia","Ituporanga",
  "Jose Boiteux","Laurentino","Lontras","Mirim Doce","Petrolandia",
  "Pouso Redondo","Presidente Getulio","Presidente Nereu","Rio do Campo",
  "Rio do Oeste","Rio do Sul","Salete","Santa Terezinha","Taio",
  "Trombudo Central","Vidal Ramos","Vitor Meireles","Witmarsum"
];
const MEDIO_VALE = [
  "Apiuna","Ascurra","Benedito Novo","Blumenau","Botuvera","Brusque",
  "Doutor Pedrinho","Gaspar","Guabiruba","Indaial","Pomerode",
  "Rio dos Cedros","Rodeio","Timbo"
];
const VALE_CITIES = [...ALTO_VALE, ...MEDIO_VALE];
const CITY_PATTERNS = VALE_CITIES.map(city => ({
  city,
  re: new RegExp('(?:^|[^a-z])'+norm(city).replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'(?:$|[^a-z])','i')
}));

function stationText(s){
  return norm([
    s?.codigo,
    s?.name?.prefix,s?.name?.general,s?.name?.local,
    s?.position?.regiao,s?.position?.bacia,
    s?.data?.rio?.rio_nome?.value
  ].map(txt).join(" "));
}
function stationCity(s){
  const h=stationText(s);
  return CITY_PATTERNS.find(x=>x.re.test(h))?.city || null;
}
function selectDefault(){
  // Rede fixa: todas as estações de chuva localizadas nos 42 municípios oficiais.
  // Não existe seleção manual pelo usuário.
  const picked=rainStations.filter(s=>stationCity(s));
  selected=new Set(picked.map(s=>String(s.codigo)));
  const alto=picked.filter(s=>ALTO_VALE.some(c=>norm(c)===norm(stationCity(s)))).length;
  const medio=picked.length-alto;
  $("selectionInfo").textContent=picked.length
    ? `Rede fixa: ${VALE_CITIES.length} municípios monitorados — 28 do Alto Vale (AMAVI) + 14 do Médio Vale (AMVE). ${picked.length} estações de chuva incluídas automaticamente: ${alto} no Alto Vale e ${medio} no Médio Vale.`
    : "Nenhuma estação teve município identificável nos metadados recebidos da Defesa Civil SC.";
}

function avg(k){
 const v=rainStations.filter(s=>selected.has(String(s.codigo))).map(s=>rain(s,k)).filter(Number.isFinite);
 return v.length?v.reduce((a,b)=>a+b,0)/v.length:null;
}
function rainNow(){
 const values=rainStations.filter(s=>selected.has(String(s.codigo))).map(s=>rain(s,"min005")).filter(Number.isFinite);
 if(!values.length)return {rate:null,count:0,total:selected.size};
 const mean5=values.reduce((a,b)=>a+b,0)/values.length;
 return {rate:Math.max(0,mean5*12),count:values.length,total:selected.size};
}
function rainNowClass(rate){
 if(!Number.isFinite(rate)||rate<=0.01)return "none";
 if(rate<=10)return "light";
 if(rate<=25)return "moderate";
 return "heavy";
}
// Previsão espacial: pontos distribuídos pela bacia contribuinte do Itajaí-Açu
// entre o Alto Vale e Blumenau. Os pontos são apenas amostras da grade ECMWF;
// a média regional é calculada sobre todos os pontos válidos de cada setor.
const ITAJAI_BASIN_POINTS = {
  alto: [
    [-27.21,-49.64], // Rio do Sul
    [-27.05,-49.54], // Laurentino / Rio do Oeste
    [-27.35,-49.57], // Pouso Redondo
    [-27.23,-49.78], // Taió
    [-27.43,-49.70], // Salete / Rio do Campo
    [-27.12,-49.37], // Ibirama
    [-27.03,-49.52], // Lontras
    [-26.98,-49.67], // Presidente Getúlio
    [-26.87,-49.23], // José Boiteux
    [-27.00,-49.39]  // Dona Emma / Witmarsum
  ],
  medio: [
    [-26.92,-49.07], // Blumenau
    [-26.89,-49.10], // Blumenau norte
    [-26.86,-49.27], // Indaial
    [-26.81,-49.27], // Timbó
    [-26.84,-49.38], // Rodeio / Rio dos Cedros
    [-26.70,-49.17], // Benedito Novo
    [-26.77,-49.00], // Gaspar
    [-26.75,-49.06]  // Ilhota / entorno do Médio Vale
  ]
};

async function loadRainForecast(){
  const card=$("rainForecastCard"); if(!card)return;
  const statusEl=$("rainForecastStatus"), meta=$("rainForecastMeta");
  try{
    const all=[...ITAJAI_BASIN_POINTS.alto.map(p=>[...p,"alto"]),...ITAJAI_BASIN_POINTS.medio.map(p=>[...p,"medio"])]
      .map((p,i)=>({lat:p[0],lon:p[1],region:p[2],i}));
    const url="https://api.open-meteo.com/v1/forecast?latitude="+all.map(p=>p.lat).join(",")
      +"&longitude="+all.map(p=>p.lon).join(",")
      +"&hourly=precipitation&models=ecmwf_ifs025&forecast_days=7&timezone=America%2FSao_Paulo";
    const res=await fetch(url,{cache:"no-store"});
    if(!res.ok)throw new Error("HTTP "+res.status);
    const data=await res.json();
    const rows=Array.isArray(data)?data:[data];
    const now=new Date(), limits=[12,24,48,72,168].map(h=>new Date(now.getTime()+h*3600*1000));
    const vals=all.map((p,i)=>{
      const d=rows[i], times=d?.hourly?.time||[], rainv=d?.hourly?.precipitation||[];
      const sums=[0,0,0,0,0], counts=[0,0,0,0,0];
      for(let j=0;j<Math.min(times.length,rainv.length);j++){
        const t=new Date(times[j]), v=Number(rainv[j]);
        if(t>=now && t<limits[3] && Number.isFinite(v)){
          const h=(t-now)/3600000;
          if(h<12)sums[0]+=v;
          if(h<24)sums[1]+=v;
          if(h<48)sums[2]+=v;
          if(h<72)sums[3]+=v;
          sums[4]+=v;
          counts[4]++;
          if(h<12)counts[0]++;
          if(h<24)counts[1]++;
          if(h<48)counts[2]++;
          if(h<72)counts[3]++;
        }
      }
      return {...p,sums,counts};
    }).filter(p=>p.counts[3]>0);
    const mean=(idx)=>{
      const v=vals.map(p=>p.sums[idx]).filter(Number.isFinite);
      return v.length?v.reduce((a,b)=>a+b,0)/v.length:null;
    };
    const a12=mean(0), a24=mean(1), a48=mean(2), a72=mean(3), a7d=mean(4);
    $("rainForecast12").textContent=a12==null?"—":fmt(a12);
    $("rainForecast24").textContent=a24==null?"—":fmt(a24);
    $("rainForecast48").textContent=a48==null?"—":fmt(a48);
    $("rainForecast72").textContent=a72==null?"—":fmt(a72);
    $("rainForecast7d").textContent=a7d==null?"—":fmt(a7d);
    statusEl.textContent=a7d==null?"SEM DADO":"ECMWF IFS";
    statusEl.className="rain-forecast-status "+(a7d==null?"warn":"ok");
    meta.textContent=a7d==null
      ? "Não foi possível obter a previsão ECMWF agora."
      : `Média espacial da bacia do Itajaí-Açu · Alto Vale até Blumenau · ${vals.length} pontos ECMWF válidos · atualização automática a cada 30 min.`;
  }catch(e){
    console.warn("Previsão ECMWF",e);
    statusEl.textContent="INDISPONÍVEL"; statusEl.className="rain-forecast-status warn";
    meta.textContent="Não foi possível consultar o modelo ECMWF neste momento.";
  }
}

function renderRainNow(){
 const card=$("rainNowCard");if(!card)return;
 const label=$("rainNowLabel"),value=$("rainNowValue"),meta=$("rainNowMeta"),badge=$("rainNowStatus");
 const r=rainNow(), cls=rainNowClass(r.rate);
 card.className=`rain-now-card rain-now-${cls}`;
 if(cls==="none"){label.textContent="SEM CHUVA REGISTRADA NO MOMENTO";value.innerHTML="0,0 <em>mm/h</em>";badge.textContent="SEM CHUVA";}
 else if(cls==="light"){label.textContent="CHUVA FRACA";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="FRACA";}
 else if(cls==="moderate"){label.textContent="CHUVA MODERADA";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="MODERADA";}
 else{label.textContent="CHUVA FORTE";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="FORTE";}
 meta.textContent=`Média dos últimos 5 minutos · ${r.count} de ${r.total||57} estações com leitura válida`;
}
function fmt(n,d=1){return Number.isFinite(n)?n.toLocaleString("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d}):"—"}
function damKey(d){return d?._damKey||((norm([name(d),d.codigo,d.position?.regiao,d.position?.bacia].map(txt).join(" ")).includes("ituporanga"))?"sul":"oeste")}
function damPercent(s){return num(s?.data?.barramento?.nivel?.percentual?.show?.value) ?? num(s?.data?.barramento?.nivel?.percentual?.value)}
function damLevel(s){
 const v=num(s?.data?.barramento?.nivel?.montante?.show?.value) ?? num(s?.data?.barramento?.nivel?.montante?.value);
 if(v==null)return null;
 // The API's montante is an IBGE elevation (cota). The public dam portal
 // displays NAR in metres above the 0 m reference of each reservoir.
 if(v>100)return v-(damKey(s)==="sul"?378:338);
 return v;
}
function gateList(s){
 const c=s?.data?.barramento?.comportas||{}, max=damKey(s)==="sul"?5:7, out=[];
 for(let i=1;i<=max;i++){
   const g=c[`comporta_${i}`]; if(!g) continue;
   const rawState=raw(g.estado), state=norm(rawState);
   const open = /abert|open|parcial/.test(state) || rawState===true || rawState===1 || String(rawState).trim()==="1";
   out.push({n:`C${i}`,open});
 }
 return out;
}
function damUpdated(s){
 const t=s?.timestamp;
 if(!t)return "Atualização recebida da Defesa Civil SC";
 const d=new Date(t);
 return isNaN(d)?"Atualização recebida da Defesa Civil SC":`Última atualização · ${d.toLocaleString("pt-BR")}`;
}
function renderDams(){
 const host=$("damCards");if(!host)return;
 if(!dams.length){host.innerHTML='<div class="muted">Nenhuma barragem encontrada nos dados oficiais.</div>';return}
 host.innerHTML=dams.map(d=>{
   const level=damLevel(d),pct=damPercent(d),g=gateList(d),open=g.filter(x=>x.open).length;
   return `<article class="dam-card"><div class="dam-head"><div><div class="dam-kicker">BARRAGEM</div><h3>${esc(d.label)}</h3><p>${esc(d.subtitle)}</p></div><span class="dam-live"><i></i> AO VIVO</span></div><div class="dam-main"><div><small>NÍVEL DO RESERVATÓRIO</small><strong>${level==null?'—':fmt(level,2)} <em>m</em></strong></div><div class="dam-percent"><small>CAPACIDADE</small><b>${pct==null?'—':fmt(pct,1)}%</b></div><div class="dam-gates"><small>COMPORTAS</small><b>${open} de ${g.length} abertas</b></div></div><div class="dam-gate-list">${g.map(x=>`<span class="gate ${x.open?'open':'closed'}"><i></i>${esc(x.n)} · ${x.open?'Aberta':'Fechada'}</span>`).join('')}</div><div class="dam-updated">${esc(damUpdated(d))}</div><div class="dam-source">Fonte oficial · Defesa Civil SC · estação ${esc(String(d.codigo))}</div></article>`
 }).join('');
}

function render(){
 $("received").textContent=allStations.length;
 $("rainCount").textContent=rainStations.length;
 $("selectedCount").textContent=selected.size;
 const cards=[["3h","h003"],["6h","h006"],["12h","h012"],["24h","h024"],["48h","h048"],["96h","__96h__"]];
 const a24=avg("h024"),a48=avg("h048"),a36=(Number.isFinite(a24)&&Number.isFinite(a48))?(a24+a48)/2:null;
 const cardHtml=cards.map(([l,k])=>{ const value=k==="__96h__"?avg("h096"):avg(k); return `<article class="rain-card"><small>${l}</small><b>${value==null?"—":fmt(value)+" mm"}</b></article>`; }).join("");
 $("rainCards").innerHTML=cardHtml;
 $("stations").innerHTML=rainStations.filter(s=>selected.has(String(s.codigo))).slice().sort((a,b)=>{
  const ca=stationCity(a)||"", cb=stationCity(b)||"";
  return ca.localeCompare(cb,"pt-BR") || name(a).localeCompare(name(b),"pt-BR");
 }).map(s=>{
  const code=String(s.codigo), city=stationCity(s), meta=[txt(s.position?.regiao),txt(s.position?.bacia)].filter(Boolean).join(" · ");
  const region=ALTO_VALE.some(c=>norm(c)===norm(city))?"Alto Vale":"Médio Vale";
  return `<article class="station"><span class="station-check" aria-hidden="true">✓</span><span><strong>${esc(name(s))}</strong><small>${esc(city||"Município não identificado")} · ${esc(code)}${meta?" · "+esc(meta):""}</small></span><em>${region}</em></article>`
 }).join("")||'<div class="muted">Nenhuma estação dos municípios do Alto + Médio Vale foi identificada.</div>';

}
async function load(){
 try{await connect();status("WebSocket conectado. Consultando estações…","ok");const d=await request(TAGS);const q=d?.tags_data?.qualle_meteorologia;allStations=Array.isArray(q)?q.filter(s=>s?.codigo):Object.values(q||{}).filter(s=>s?.codigo);rainStations=allStations.filter(s=>s?.data?.chuva?.acumulado);
   const damStations=allStations.filter(s=>s?.data?.barramento);
   dams=damStations.map(s=>{const k=norm([name(s),s.codigo,s.position?.regiao,s.position?.bacia].map(txt).join(" "));const sul=k.includes("ituporanga")||k.includes("barragem sul");return {...s,label:sul?"Barragem Sul — Ituporanga":"Barragem Oeste — Taió",subtitle:sul?"Reservatório · Rio Itajaí do Sul":"Reservatório · Rio Itajaí do Oeste",_k:k,_damKey:sul?"sul":"oeste"};});
   dams=dams.filter(d=>d._k.includes("ituporanga")||d._k.includes("taio")||d._k.includes("barragem sul")||d._k.includes("barragem oeste"));
   dams.sort((a,b)=>a.label.localeCompare(b.label,"pt-BR"));
   selectDefault();render();await renderDams();await loadRainForecast();status(`Conectado. ${allStations.length} estações recebidas; ${rainStations.length} com dados de chuva.`, "ok")}catch(e){console.error(e);status("Erro: "+e.message,"err")}}
$("refresh").onclick=()=>location.reload();
setInterval(async()=>{
 try{
   if(!ws||ws.readyState!==WebSocket.OPEN)return;
   const d=await request(TAGS);
   const q=d?.tags_data?.qualle_meteorologia;
   const fresh=Array.isArray(q)?q.filter(s=>s?.codigo):Object.values(q||{}).filter(s=>s?.codigo);
   allStations=fresh;
   rainStations=allStations.filter(s=>s?.data?.chuva?.acumulado);
   const byCode=new Map(fresh.map(s=>[String(s.codigo),s]));
   dams=dams.map(old=>{const freshDam=byCode.get(String(old.codigo));return freshDam?{...freshDam,label:old.label,subtitle:old.subtitle,_damKey:old._damKey}:old});
   selectDefault();
   render();
   await renderDams();
   await loadRainForecast();
   status(`Atualizado. ${allStations.length} estações recebidas; ${rainStations.length} com dados de chuva.`,"ok");
 }catch(e){console.warn("Atualização da rede",e)}
},60*1000);

load();

setInterval(loadRainForecast,30*60*1000);
