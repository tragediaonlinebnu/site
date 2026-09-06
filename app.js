const WS_URL="wss://monitoramento.defesacivil.sc.gov.br/graphql";
const CLIENT="secretaria-de-defesa-civil";
const TAGS=`query Tags_data { tags_data(clients: ["${CLIENT}"]) { qualle_meteorologia { codigo name { prefix general local } show timestamp position { bacia latitude longitude regiao altitude } data { rio { rio_nome { value } rio_nivel { value show { value } format { value } unit { value } } rio_nivel_tendencia { value show { value } } } chuva { acumulado { min005 { value show { value } format { value } unit { value } } h003 { value show { value } unit { value } } h006 { value show { value } unit { value } } h012 { value show { value } unit { value } } h024 { value show { value } unit { value } } h048 { value show { value } unit { value } } h096 { value show { value } unit { value } } } } barramento { nivel { percentual { value show { value } unit { value } } montante { value show { value } unit { value } } jusante { value show { value } unit { value } } vertido { value show { value } unit { value } } } capacidade { atual { value show { value } unit { value } } maxima { value show { value } unit { value } } } comportas { comporta_1 { estado { value } habilitada { value } nome { value show { value } } } comporta_2 { estado { value } habilitada { value } nome { value show { value } } } comporta_3 { estado { value } habilitada { value } nome { value show { value } } } comporta_4 { estado { value } habilitada { value } nome { value show { value } } } comporta_5 { estado { value } habilitada { value } nome { value show { value } } } comporta_6 { estado { value } habilitada { value } nome { value show { value } } } comporta_7 { estado { value } habilitada { value } nome { value show { value } } } comporta_8 { estado { value } habilitada { value } nome { value show { value } } } comporta_9 { estado { value } habilitada { value } nome { value show { value } } } comporta_10 { estado { value } habilitada { value } nome { value show { value } } } } } } type filter { relacao { tem_chuva_acumulada tem_nivel_do_rio tem_barragem } } } } }`;

let ws=null, seq=0, pending=new Map(), allStations=[], rainStations=[], selected=new Set(), dams=[];
let reconnecting=false;
const rainNowHistory=new Map();
let blumenauRiverLevel=null;
let blumenauRiverUpdated=null;
let blumenauRiverTrend=0;
let ecmwfForecastRain={"6":0,"12":0,"24":0};
let damPreviousOpenFractions={sul:0,oeste:0};
let damHistory=[];

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
  const contrib=BLUMENAU_CONTRIBUTING_CITIES.length;
  $("selectionInfo").textContent=picked.length
    ? `Rede fixa: ${VALE_CITIES.length} municípios monitorados — 28 do Alto Vale (AMAVI) + 14 do Médio Vale (AMVE). ${picked.length} estações de chuva incluídas automaticamente: ${alto} no Alto Vale e ${medio} no Médio Vale. Média acumulada: ${contrib} municípios contribuintes, com peso igual por município.`
    : "Nenhuma estação teve município identificável nos metadados recebidos da Defesa Civil SC.";
}

// Chuva acumulada voltada ao nível de Blumenau:
// usamos somente municípios que drenam para o sistema do Itajaí-Açu
// antes de Blumenau. Para não deixar municípios com muitas estações
// dominarem a média, primeiro calculamos a média de cada município e
// depois damos o mesmo peso a cada município com dado válido.
const BLUMENAU_CONTRIBUTING_CITIES = [
 ...ALTO_VALE,
 "Apiuna","Ascurra","Benedito Novo","Doutor Pedrinho","Indaial",
 "Pomerode","Rio dos Cedros","Rodeio","Timbo"
];
function avg(k){
 const byCity=new Map();
 rainStations.filter(s=>selected.has(String(s.codigo))).forEach(s=>{
  const city=stationCity(s);
  if(!city || !BLUMENAU_CONTRIBUTING_CITIES.some(c=>norm(c)===norm(city))) return;
  const value=rain(s,k);
  if(!Number.isFinite(value)) return;
  if(!byCity.has(norm(city))) byCity.set(norm(city),[]);
  byCity.get(norm(city)).push(value);
 });
 const cityMeans=[...byCity.values()].map(v=>v.reduce((a,b)=>a+b,0)/v.length);
 return cityMeans.length?cityMeans.reduce((a,b)=>a+b,0)/cityMeans.length:null;
}

// "Chuva agora" usa uma média móvel de 15 minutos.
// A Defesa Civil fornece min005; como o painel atualiza a cada 1 minuto,
// guardamos as três últimas leituras de cada estação e calculamos a média
// das três intensidades de 5 minutos, obtendo uma intensidade média móvel
// de aproximadamente 15 minutos.

const RAIN_NOW_STATIONS = ["indaial", "apiuna", "timbo", "rodeio", "rio do sul", "blumenau"];
function rainNow(){
 const candidates=rainStations.filter(s=>{
   const text=norm([name(s),stationCity(s)].map(txt).join(" "));
   return RAIN_NOW_STATIONS.some(city=>text.includes(norm(city)));
 });
 // Em Blumenau, prioriza a estação Água Verde SDC-SC, que aparece na rede
 // meteorológica estadual/municipal. Para as demais cidades, mantém a seleção
 // por município já definida.
 const blumenau=candidates.filter(s=>norm(stationCity(s)||"")==="blumenau");
 const preferredBlumenau=blumenau.find(s=>norm(name(s)).includes("agua verde"));
 const stations=candidates.filter(s=>norm(stationCity(s)||"")!=="blumenau" || s===preferredBlumenau);
 const readings=stations.map(s=>({station:s,value:rain(s,"min005")})).filter(x=>Number.isFinite(x.value));
 const now=Date.now();
 readings.forEach(x=>{
   const key=String(x.station.codigo);
   const history=rainNowHistory.get(key)||[];
   history.push({t:now,value:x.value});
   rainNowHistory.set(key,history.filter(h=>now-h.t<=31*60*1000).slice(-6));
 });
 if(!readings.length)return {rate:null,count:0,total:RAIN_NOW_STATIONS.length,stations:[],active:0};
 // Para não diluir chuva localizada, a média é feita somente entre as
 // estações que registraram chuva no período. Cada estação usa sua média
 // móvel de até três leituras min005 (aprox. 15 min).
 const smoothed=readings.map(x=>{
   const history=rainNowHistory.get(String(x.station.codigo))||[];
   const values=history.slice(-6).map(h=>h.value);
   const mean5=values.length?values.reduce((a,b)=>a+b,0)/values.length:x.value;
   return {...x,rate:mean5*12};
 });
 const rainy=smoothed.filter(x=>x.rate>0);
 if(!rainy.length)return {rate:0,count:readings.length,total:RAIN_NOW_STATIONS.length,stations:readings.map(x=>x.station),active:0};
 const mean30=rainy.reduce((sum,x)=>sum+x.rate,0)/rainy.length;
 return {rate:Math.max(0,mean30),count:readings.length,total:RAIN_NOW_STATIONS.length,stations:rainy.map(x=>x.station),active:rainy.length};
}
function rainNowClass(rate){
 // Classificação solicitada: 0,4–5,0 fraca; 5,1–25,0 moderada;
 // 25,1–50,0 forte; acima de 50,0 muito forte.
 if(!Number.isFinite(rate)||rate<0.4)return "none";
 if(rate<=5.0)return "light";
 if(rate<=25.0)return "moderate";
 if(rate<=50.0)return "strong";
 return "very-strong";
}
// Previsão espacial: pontos distribuídos pela bacia contribuinte do Itajaí-Açu
// entre o Alto Vale e Blumenau. Os pontos são apenas amostras da grade ECMWF;
// a média regional é calculada sobre todos os pontos válidos de cada setor.
const ITAJAI_BASIN_POINTS = {
  // Grade espacial regularizada: 12 amostras no Alto Vale e 12 no Médio Vale.
  // Os pontos são distribuídos para evitar que poucas cidades dominem a média.
  alto: [
    [-26.82,-49.39], [-26.92,-49.64], [-27.02,-49.29], [-27.08,-49.55],
    [-27.18,-49.78], [-27.22,-49.48], [-27.30,-49.70], [-27.34,-49.38],
    [-27.42,-49.64], [-27.48,-49.50], [-27.55,-49.72], [-27.58,-49.32]
  ],
  medio: [
    [-26.62,-49.38], [-26.65,-49.17], [-26.70,-49.03], [-26.75,-49.29],
    [-26.78,-49.12], [-26.82,-49.42], [-26.86,-49.27], [-26.89,-49.10],
    [-26.93,-49.34], [-26.96,-49.19], [-27.00,-49.06], [-27.03,-48.92]
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
      +"&hourly=precipitation&models=ecmwf_ifs025&forecast_days=8&timezone=America%2FSao_Paulo";
    const res=await fetch(url,{cache:"no-store"});
    if(!res.ok)throw new Error("HTTP "+res.status);
    const data=await res.json();
    const rows=Array.isArray(data)?data:[data];
    const now=new Date();
    const vals=all.map((p,i)=>{
      const d=rows[i], times=d?.hourly?.time||[], rainv=d?.hourly?.precipitation||[];
      const sums=[0,0,0,0,0,0], counts=[0,0,0,0,0,0];
      for(let j=0;j<Math.min(times.length,rainv.length);j++){
        const t=new Date(times[j]), v=Number(rainv[j]);
        if(t>=now && t<new Date(now.getTime()+192*3600*1000) && Number.isFinite(v)){
          const h=(t-now)/3600000;
          if(h<6)sums[0]+=v;
          if(h<12)sums[1]+=v;
          if(h<24)sums[2]+=v;
          if(h<48)sums[3]+=v;
          if(h<72)sums[4]+=v;
          sums[5]+=v;
          if(h<6)counts[0]++;
          if(h<12)counts[1]++;
          if(h<24)counts[2]++;
          if(h<48)counts[3]++;
          if(h<72)counts[4]++;
          counts[5]++;
        }
      }
      return {...p,sums,counts};
    }).filter(p=>p.counts[2]>0);
    const sectorMean=(region,idx)=>{
      const v=vals.filter(p=>p.region===region).map(p=>p.sums[idx]).filter(Number.isFinite);
      return v.length?v.reduce((a,b)=>a+b,0)/v.length:null;
    };
    const basinMean=(idx)=>{
      const alto=sectorMean('alto',idx), medio=sectorMean('medio',idx);
      if(alto!=null&&medio!=null)return (alto+medio)/2;
      return alto!=null?alto:medio;
    };
    const a6=basinMean(0),a12=basinMean(1),a24=basinMean(2),a48=basinMean(3),a72=basinMean(4),a8d=basinMean(5);
    ecmwfForecastRain={"6":a6??0,"12":a12??0,"24":a24??0};
    $("rainForecast12").textContent=a12==null?"—":fmt(a12);
    $("rainForecast24").textContent=a24==null?"—":fmt(a24);
    $("rainForecast48").textContent=a48==null?"—":fmt(a48);
    $("rainForecast72").textContent=a72==null?"—":fmt(a72);
    $("rainForecast8d").textContent=a8d==null?"—":fmt(a8d);
    statusEl.textContent=a8d==null?"SEM DADO":"ECMWF IFS";
    statusEl.className="rain-forecast-status "+(a8d==null?"warn":"ok");
    meta.textContent=a8d==null
      ? "Não foi possível obter a previsão ECMWF agora."
      : `Média espacial uniforme · Alto Vale + Médio Vale · ${vals.length} pontos ECMWF válidos (12+12) · atualização automática a cada 30 min.`;
    renderRiverCalculator();
  }catch(e){
    console.warn("Previsão ECMWF",e);
    statusEl.textContent="INDISPONÍVEL"; statusEl.className="rain-forecast-status warn";
    meta.textContent="Não foi possível consultar a previsão ECMWF neste momento.";
  }
}
function projectionStatus(level){
 if(!Number.isFinite(level)) return {label:"AGUARDANDO NÍVEL", cls:"waiting"};
 if(level<3)return {label:"NORMALIDADE",cls:"normal"};
 if(level<4)return {label:"OBSERVAÇÃO",cls:"observacao"};
 if(level<6)return {label:"ATENÇÃO",cls:"atencao"};
 if(level<8)return {label:"ALERTA",cls:"alerta"};
 return {label:"ALERTA MÁXIMO",cls:"critico"};
}

const RESPONSE_MEDIO_MIN_H=3;
const RESPONSE_MEDIO_MAX_H=6;
const RESPONSE_ALTO_MIN_H=10;
const RESPONSE_ALTO_MAX_H=15;
const REFERENCE_RAIN_MM=130;
const REFERENCE_RISE_M=5.90;

// Histórico do nível vem do próprio monitor oficial (últimas 24h).
// Não usamos localStorage: todos os visitantes recebem a mesma base.
let blumenauRiverHistory=[];
function setRiverHistory(history){
 const clean=(Array.isArray(history)?history:[]).map(x=>({
   t:new Date(x.t||x.updated).getTime(), level:Number(x.level)
 })).filter(x=>Number.isFinite(x.t)&&Number.isFinite(x.level));
 const byTime=new Map(clean.map(x=>[x.t,x]));
 blumenauRiverHistory=[...byTime.values()].sort((a,b)=>a.t-b.t).slice(-192);
}
function riverTrendHistory(){
 const a=blumenauRiverHistory, now=Date.now();
 const recent=a.filter(x=>now-x.t<=6*60*60*1000);
 if(recent.length<2)return {slope:0,rising:false,onsetHours:null};
 const first=recent[0],last=recent[recent.length-1], hours=Math.max(.25,(last.t-first.t)/3600000);
 const slope=(last.level-first.level)/hours;
 const rising=slope>0.005;
 let onset=null;
 if(Math.abs(slope)>0.005){
   for(let i=recent.length-1;i>0;i--){
     const a1=recent[i-1],a2=recent[i];
     const step=(a2.level-a1.level)/Math.max(.25,(a2.t-a1.t)/3600000);
     if((rising&&step>0.005)||(!rising&&step<-0.005)){onset=(now-a1.t)/3600000;break}
   }
 }
 return {slope,rising,onsetHours:onset};
}

function avgForCities(k,cities){
 const wanted=cities.map(norm);
 const byCity=new Map();
 rainStations.filter(s=>selected.has(String(s.codigo))).forEach(s=>{
  const city=stationCity(s);
  if(!city || !wanted.includes(norm(city))) return;
  const value=rain(s,k);
  if(!Number.isFinite(value)) return;
  if(!byCity.has(norm(city))) byCity.set(norm(city),[]);
  byCity.get(norm(city)).push(value);
 });
 const cityMeans=[...byCity.values()].map(v=>v.reduce((a,b)=>a+b,0)/v.length);
 return cityMeans.length?cityMeans.reduce((a,b)=>a+b,0)/cityMeans.length:null;
}

function getPanelRainAccumulated(){
 // Chuva observada: exclusivamente dos pluviômetros via WebSocket.
 return {r3:avg("h003"),r6:avg("h006"),r12:avg("h012"),r24:avg("h024"),r48:avg("h048"),r96:avg("h096")};
}
function observedRainPotential(r3,r6,r12,r24,r48,r96,horizon=24){
 const vals=[r3,r6,r12,r24,r48,r96].map(v=>Math.max(0,Number(v)||0));
 let [v3,v6,v12,v24,v48,v96]=vals;
 v6=Math.max(v3,v6);v12=Math.max(v6,v12);v24=Math.max(v12,v24);v48=Math.max(v24,v48);v96=Math.max(v48,v96);
 const h=Number(horizon);
 let effective=v3*1+(v6-v3)*.90;
 if(h>=12) effective+=(v12-v6)*.65;
 if(h>=24) effective+=(v24-v12)*.45;
 if(h>=48) effective+=(v48-v24)*.25;
 if(h>=96) effective+=(v96-v48)*.10;
 return Math.max(0,effective*(REFERENCE_RISE_M/REFERENCE_RAIN_MM)*.30);
}
function damSpill(s){return num(s?.data?.barramento?.nivel?.vertido?.show?.value) ?? num(s?.data?.barramento?.nivel?.vertido?.value) ?? 0}
function currentDamSignal(){
 if(!dams.length)return {openFraction:0,spillActive:0,eventOpening:0,recentEventSignal:0};
 let open=0,total=0,spill=0;
 let opening=0;
 for(const d of dams){
   const g=gateList(d); const opened=g.filter(x=>x.open).length;
   open+=opened; total+=g.length;
   const sr=damSpill(d); if(sr>0)spill+=1;
   const key=damKey(d);
   const frac=g.length?opened/g.length:0;
   if(key){ opening=Math.max(opening,Math.max(0,frac-Number(damPreviousOpenFractions[key]||0))); damPreviousOpenFractions[key]=frac; }
 }
 const frac=total?open/total:0;
 let recent=0; const now=Date.now();
 for(let i=damHistory.length-1;i>=0;i--){
   const item=damHistory[i], age=(now-Number(item?.t||0))/3600000;
   if(age<0||age>6)continue;
   const sig=Number(item?.signal?.event_opening||0);
   recent=Math.max(recent,sig*Math.exp(-age/3));
 }
 return {openFraction:frac,spillActive:spill,eventOpening:Math.min(1,opening),recentEventSignal:Math.min(1,Math.max(opening,recent))};
}

let learnedParams={trend_gain:{"6":1,"12":1,"24":1},rain_gain:{"6":1,"12":1,"24":1},forecast_rain_gain:{"6":1,"12":1,"24":1},dam_gain:{"6":1,"12":1,"24":1},damping:{"6":0.70,"12":0.55,"24":0.40},metrics:{evaluated:0,mae_m:{"6":null,"12":null,"24":null}}};
async function loadDamHistory(){
 try{
  const res=await fetch('./data/dam_history.json?ts='+Date.now(),{cache:'no-store'});
  if(res.ok){
   const d=await res.json();
   damHistory=Array.isArray(d)?d.slice(-500):[];
   const last=damHistory[damHistory.length-1];
   if(last?.dams){
    for(const key of ['sul','oeste']){
     const v=Number(last.dams?.[key]?.open_fraction);
     if(Number.isFinite(v)) damPreviousOpenFractions[key]=v;
    }
   }
   renderRiverCalculator();
  }
 }catch(e){console.warn('Histórico de barragens',e)}
}
async function loadLearnedParams(){
 try{
  const res=await fetch('./data/learning.json?ts='+Date.now(),{cache:'no-store'});
  if(!res.ok)return;
  const d=await res.json();
  learnedParams={...learnedParams,...d,trend_gain:{...learnedParams.trend_gain,...(d.trend_gain||{})},rain_gain:{...learnedParams.rain_gain,...(d.rain_gain||{})},forecast_rain_gain:{...learnedParams.forecast_rain_gain,...(d.forecast_rain_gain||{})},dam_gain:{...learnedParams.dam_gain,...(d.dam_gain||{})},damping:{...learnedParams.damping,...(d.damping||{})}};
  renderRiverCalculator();
 }catch(e){console.warn('Aprendizado automático',e)}
}
function calculateRiverProjection(){
 const current=Number(blumenauRiverLevel);
 if(!Number.isFinite(current)) return null;
 const {r3,r6,r12,r24,r48,r96}=getPanelRainAccumulated();
 if(![r3,r6,r12,r24,r48,r96].some(Number.isFinite))return null;
 const trend=riverTrendHistory();
 const rainPotential={6:observedRainPotential(r3,r6,r12,r24,r48,r96,6),12:observedRainPotential(r3,r6,r12,r24,r48,r96,12),24:observedRainPotential(r3,r6,r12,r24,r48,r96,24)};
 const dam=currentDamSignal();
 const out={current,r24:Number(r24)||0,r48:Number(r48)||0,effective48:rainPotential,status:projectionStatus(current),slope:trend.slope,rising:trend.rising,onsetHours:trend.onsetHours,responseWindow:'Resposta suavizada · aprendizado automático',observedRain:true,forecastRain:ecmwfForecastRain,dam};
 for(const h of ['6','12','24']){
   const maxDown=h==='6'?-0.30:(h==='12'?-0.45:-0.60), maxUp=h==='6'?0.60:(h==='12'?1.00:1.40);
   const base=trend.slope*Number(h);
   const trendPart=Math.max(maxDown,Math.min(maxUp,base))*(Number(learnedParams.trend_gain?.[h]??1))*(Number(learnedParams.damping?.[h]??({6:.70,12:.55,24:.40}[h])));
   const observedPart=(rainPotential[h]||0)*({6:.18,12:.38,24:.72}[h])*(Number(learnedParams.rain_gain?.[h]??1));
   const futurePart=(Number(ecmwfForecastRain[h])||0)*(5.90/130.0)*({6:.08,12:.15,24:.25}[h])*(Number(learnedParams.forecast_rain_gain?.[h]??1));
   const damSignal=Math.max(0,Math.min(2,(0.55*dam.openFraction+0.20*Number(dam.spillActive||0)+0.35*Math.max(Number(dam.eventOpening||0),Number(dam.recentEventSignal||0)))))*({6:.10,12:.20,24:.32}[h]);
   const damPart=damSignal*(Number(learnedParams.dam_gain?.[h]??1));
   out['p'+h]=Math.max(0,current+trendPart+observedPart+futurePart+damPart);
   out['components'+h]={trend:trendPart,observedRain:observedPart,forecastRain:futurePart,dam:damPart};
 }
 return out;
}
function renderRiverCalculator(){
 const host=$("riverCalculator"); if(!host)return;
 const p=calculateRiverProjection();
 if(!p){host.innerHTML=`<div class="river-calc-head"><div><div class="river-calc-kicker">CALCULADORA EXPERIMENTAL</div><h3>Projeção do nível em Blumenau</h3><p>Aguardando nível do rio e acumulados de chuva.</p></div><span class="river-calc-badge waiting">AGUARDANDO DADOS</span></div>`;return;}
 const status=p.status;
 const learnCount=Number(learnedParams.metrics?.evaluated||0);
 const updated=blumenauRiverUpdated?new Date(blumenauRiverUpdated).toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}):"agora";
 host.innerHTML=`
 <div class="river-calc-head"><div><div class="river-calc-kicker">PROJEÇÃO DO RIO · BLUMENAU</div><h3>Nível do Itajaí-Açu</h3><p>Leitura oficial atual + chuva observada dos pluviômetros do painel.</p></div><span class="river-calc-badge ${status.cls}">${status.label}</span></div>
 <div class="river-calc-layout">
   <div class="river-calc-current">
     <div class="river-calc-current-top"><span>NÍVEL ATUAL</span><span class="river-calc-live"><i></i> AO VIVO</span></div>
     <strong>${fmt(p.current,2)} <em>m</em></strong>
     <div class="river-calc-source">Fonte oficial · ${updated}</div>
   </div>
   <div class="river-calc-forecast-title"><span>PROJEÇÃO</span><small>tendência estimada</small></div>
   <div class="river-calc-forecast">
     <div class="river-calc-item"><small>EM 6 HORAS</small><b>${fmt(p.p6,2)} <em>m</em></b></div>
     <div class="river-calc-arrow" aria-hidden="true">→</div>
     <div class="river-calc-item"><small>EM 12 HORAS</small><b>${fmt(p.p12,2)} <em>m</em></b></div>
     <div class="river-calc-arrow" aria-hidden="true">→</div>
     <div class="river-calc-item"><small>EM 24 HORAS</small><b>${fmt(p.p24,2)} <em>m</em></b></div>
   </div>
 </div>
 <div class="river-calc-note">Entrada separada: chuva observada dos pluviômetros via WebSocket + chuva futura do ECMWF + operação atual das barragens. Estimativa experimental — não substitui os alertas oficiais da Defesa Civil. Aprendizado automático: ${learnCount} previsões avaliadas.</div>`;
}
function receiveBlumenauRiverMessage(event){
 if(event.origin!==window.location.origin)return;
 const d=event.data||{};
 if(d.type!=="blumenau-river-update")return;
 const level=Number(d.level);
 if(!Number.isFinite(level))return;
 blumenauRiverLevel=level;
 blumenauRiverUpdated=d.updated||null;
 blumenauRiverTrend=Number(d.trend)||0;
 setRiverHistory(d.history);
 renderRiverCalculator();
}
window.addEventListener("message",receiveBlumenauRiverMessage);

function renderRainNow(){
 const card=$("rainNowCard");if(!card)return;
 const label=$("rainNowLabel"),value=$("rainNowValue"),meta=$("rainNowMeta"),badge=$("rainNowStatus");
 const r=rainNow(), cls=rainNowClass(r.rate);
 card.className=`rain-now-card rain-now-${cls}`;
 if(cls==="none"){label.textContent="SEM CHUVA REGISTRADA NO MOMENTO";value.innerHTML="0,0 <em>mm/h</em>";badge.textContent="SEM CHUVA";}
 else if(cls==="light"){label.textContent="CHUVA FRACA";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="FRACA";}
 else if(cls==="moderate"){label.textContent="CHUVA MODERADA";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="MODERADA";}
 else if(cls==="strong"){label.textContent="CHUVA FORTE";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="FORTE";}
 else{label.textContent="CHUVA MUITO FORTE";value.innerHTML=`${fmt(r.rate)} <em>mm/h</em>`;badge.textContent="MUITO FORTE";}
 meta.textContent=r.active>0
   ? `Média móvel dos últimos 30 minutos · ${r.active} estação${r.active===1?"":"ões"} com chuva · Chuva detectada em: ${r.stations.map(s=>stationCity(s)||String(name(s)||"").replace(/^SDC-SC\s+/i, "")||"estação").join(", ")}`
   : `Últimos 30 minutos · nenhuma das 6 estações registrou chuva · Indaial, Apiúna, Timbó, Rodeio, Rio do Sul e Blumenau`;
}
function fmt(n,d=1){return Number.isFinite(n)?n.toLocaleString("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d}):"—"}
function damKey(d){ if(d?._damKey)return d._damKey; const k=norm([name(d),d?.codigo,d?.position?.regiao,d?.position?.bacia].map(txt).join(" ")); if(k.includes("ituporanga")||k.includes("barragem sul"))return "sul"; if(k.includes("taio")||k.includes("barragem oeste"))return "oeste"; return null; }
function damPercent(s){return num(s?.data?.barramento?.nivel?.percentual?.show?.value) ?? num(s?.data?.barramento?.nivel?.percentual?.value)}
function damLevel(s){
 const node=s?.data?.barramento?.nivel?.montante||{};
 const v=num(node?.show?.value) ?? num(node?.value);
 if(v==null)return null;
 const unit=norm(node?.unit?.value||node?.show?.unit?.value||"");
 // Só converte cota IBGE quando a própria API informa essa unidade.
 // Nunca inferimos o datum apenas pelo tamanho do número.
 if(/ibge/.test(unit)){
   const offset=damKey(s)==="sul"?370:damKey(s)==="oeste"?339:null;
   return offset==null?null:v-offset;
 }
 return v;
}
function gateList(s){
 const c=s?.data?.barramento?.comportas||{}, out=[];
 for(let i=1;i<=10;i++){
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
 renderRainNow();
 renderRiverCalculator();
 const cards=[["Últimas 3h","h003"],["Últimas 6h","h006"],["Últimas 12h","h012"],["Últimas 24h","h024"],["Últimas 48h","h048"],["Últimas 96h","__96h__"]];
 const a24=avg("h024"),a48=avg("h048");
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
 if(reconnecting)return;
 reconnecting=true;
 try{await connect();status("WebSocket conectado. Consultando estações…","ok");const d=await request(TAGS);const q=d?.tags_data?.qualle_meteorologia;allStations=Array.isArray(q)?q.filter(s=>s?.codigo):Object.values(q||{}).filter(s=>s?.codigo);rainStations=allStations.filter(s=>s?.data?.chuva?.acumulado);
   const damStations=allStations.filter(s=>s?.data?.barramento);
   dams=damStations.map(s=>{const k=norm([name(s),s.codigo,s.position?.regiao,s.position?.bacia].map(txt).join(" "));const sul=k.includes("ituporanga")||k.includes("barragem sul");const oeste=k.includes("taio")||k.includes("barragem oeste"); if(!sul&&!oeste)return null; return {...s,label:sul?"Barragem Sul — Ituporanga":"Barragem Oeste — Taió",subtitle:sul?"Reservatório · Rio Itajaí do Sul":"Reservatório · Rio Itajaí do Oeste",_k:k,_damKey:sul?"sul":"oeste"};}).filter(Boolean);
   dams.sort((a,b)=>a.label.localeCompare(b.label,"pt-BR"));
   selectDefault();render();await renderDams();await loadRainForecast();status(`Conectado. ${allStations.length} estações recebidas; ${rainStations.length} com dados de chuva.`, "ok")}catch(e){console.error(e);status("Erro: "+e.message,"err")}finally{reconnecting=false}}
$("refresh").onclick=()=>location.reload();
setInterval(async()=>{
 try{
   if(!ws||ws.readyState!==WebSocket.OPEN){ await load(); return; }
   const d=await request(TAGS);
   const q=d?.tags_data?.qualle_meteorologia;
   const fresh=Array.isArray(q)?q.filter(s=>s?.codigo):Object.values(q||{}).filter(s=>s?.codigo);
   allStations=fresh;
   rainStations=allStations.filter(s=>s?.data?.chuva?.acumulado);
   dams=fresh.map(s=>{
     const k=damKey(s); if(!k)return null;
     return {...s,label:k==='sul'?'Barragem Sul — Ituporanga':'Barragem Oeste — Taió',subtitle:k==='sul'?'Reservatório · Rio Itajaí do Sul':'Reservatório · Rio Itajaí do Oeste',_damKey:k};
   }).filter(Boolean).sort((a,b)=>a.label.localeCompare(b.label,'pt-BR'));
   selectDefault();
   render();
   await renderDams();
   await loadRainForecast();
   status(`Atualizado. ${allStations.length} estações recebidas; ${rainStations.length} com dados de chuva.`,"ok");
 }catch(e){console.warn("Atualização da rede",e)}
},60*1000);

load();
loadLearnedParams();
loadDamHistory();
setInterval(loadDamHistory,15*60*1000);
setInterval(loadLearnedParams,15*60*1000);

setInterval(loadRainForecast,30*60*1000);
