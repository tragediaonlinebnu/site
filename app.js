const APP_BUILD="2026-09-16.04";
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
let ecmwfHourlyBasin=[];
let floodPerspectiveTimer=null;
let damPreviousOpenFractions={sul:0,oeste:0};
let damHistory=[];
// A projeção exibida fica congelada após o primeiro cálculo válido.
// Ela só é recalculada quando chega uma nova leitura oficial do rio.
let displayedRiverProjection=null;
let displayedProjectionRiverTimestamp=null;
let backendProjection=null;
let backendProjectionLoadedAt=0;

const $=id=>document.getElementById(id);
function status(t,c=""){$("status").textContent=t;$("status").className="status "+c}
function raw(v){return v&&typeof v==="object"&&"value" in v?v.value:v}
function num(v){v=raw(v);if(v==null||v==="")return null;if(typeof v==="number")return Number.isFinite(v)?v:null;let n=parseFloat(String(v).replace(/\s/g,"").replace(",","."));return Number.isFinite(n)?n:null}
function txt(v){return raw(v)==null?"":String(raw(v))}
function norm(v){return txt(v).normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase()}
function esc(v){return String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function name(s){return [s?.name?.local,s?.name?.general,s?.name?.prefix].map(txt).filter(Boolean).join(" — ")||txt(s?.codigo)||"Estação"}
function rain(s,k){
 const acc=s?.data?.chuva?.acumulado;
 if(!acc)return null;
 const v=num(acc?.[k]?.show?.value) ?? num(acc?.[k]?.value);
 if(Number.isFinite(v))return Math.max(0,v);
 const now5=num(acc?.min005?.show?.value) ?? num(acc?.min005?.value);
 // Se a estação está fornecendo min005 e ele é zero, um acumulado ausente
 // nos horizontes significa período seco; mostramos 0,0 mm. Sem essa
 // confirmação, mantemos null para não mascarar falta de comunicação.
 return Number.isFinite(now5) && now5<=0 ? 0 : null;
}

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

// "Chuva agora" usa min005 e uma média móvel das leituras recentes.
// Mantemos até 31 minutos de histórico para suavizar oscilações curtas.

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

function floodFmtDate(d, withYear=false){
 const opts={day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"};
 if(withYear)opts.year="numeric";
 return new Date(d).toLocaleString("pt-BR",opts).replace(","," ·");
}
function floodShortDate(d){return new Date(d).toLocaleDateString("pt-BR",{day:"2-digit",month:"2-digit"});}
function floodWindowLabel(a,b){
 const da=new Date(a), db=new Date(b);
 const same=da.toLocaleDateString("pt-BR")==db.toLocaleDateString("pt-BR");
 return same ? `${floodShortDate(a)} · ${da.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})}–${db.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})}` : `${floodShortDate(a)}–${floodShortDate(b)}`;
}
function estimateObservedRainForEvent(eventStart, now){
 // Os pluviômetros do painel entregam acumulados móveis. Escolhemos a janela
 // que melhor cobre o tempo decorrido do evento, sem somar previsões passadas.
 const {r3,r6,r12,r24,r48,r96}=getPanelRainAccumulated();
 const elapsed=Math.max(0,(now-eventStart)/3600000);
 let value=null, windowH=null;
 if(elapsed<=3.5 && Number.isFinite(r3)){value=r3;windowH=3;}
 else if(elapsed<=7 && Number.isFinite(r6)){value=r6;windowH=6;}
 else if(elapsed<=13 && Number.isFinite(r12)){value=r12;windowH=12;}
 else if(elapsed<=30 && Number.isFinite(r24)){value=r24;windowH=24;}
 else if(elapsed<=54 && Number.isFinite(r48)){value=r48;windowH=48;}
 else if(Number.isFinite(r96)){value=r96;windowH=96;}
 else if(Number.isFinite(r48)){value=r48;windowH=48;}
 else if(Number.isFinite(r24)){value=r24;windowH=24;}
 else if(Number.isFinite(r12)){value=r12;windowH=12;}
 else if(Number.isFinite(r6)){value=r6;windowH=6;}
 else if(Number.isFinite(r3)){value=r3;windowH=3;}
 return {rain:Math.max(0,Number(value)||0),windowH};
}
function renderFloodPerspective(){
 const card=$("floodPerspectiveCard"), body=$("floodPerspectiveBody"), status=$("floodPerspectiveStatus"), meta=$("floodPerspectiveMeta");
 if(!card||!body||!status||!meta)return;
 if(!Number.isFinite(blumenauRiverLevel)||!ecmwfHourlyBasin.length){
   status.textContent="AGUARDANDO DADOS"; status.className="flood-perspective-status warn";
   body.innerHTML='<div class="flood-perspective-empty">Aguardando dados do rio e a atualização da previsão ECMWF.</div>'; return;
 }
 const now=Date.now();
 const allPts=ecmwfHourlyBasin.filter(x=>Number.isFinite(x.t)&&Number.isFinite(x.rain)).sort((a,b)=>a.t-b.t);
 const futurePts=allPts.filter(x=>x.t>=now);
 if(!futurePts.length){
   status.textContent="SEM DADO"; status.className="flood-perspective-status warn";
   body.innerHTML='<div class="flood-perspective-empty">Não foi possível montar a perspectiva com a previsão disponível.</div>'; return;
 }

 // Cada janela de precipitação é um evento independente. A janela inclui
 // algumas horas anteriores ao agora porque o ECMWF mantém os horários já
 // iniciados; isso permite reconhecer que um evento previsto já está em curso.
 const windows=[]; let cur=null; let lastRainT=null;
 for(let i=0;i<allPts.length;i++){
   const t=allPts[i].t, rain=Number(allPts[i].rain)||0;
   const start5=t-5*3600000;
   const five=allPts.filter(x=>x.t>=start5&&x.t<=t).reduce((a,x)=>a+(Number(x.rain)||0),0);
   if(!cur && five>=10){ cur={start:allPts[Math.max(0,i-5)]?.t||t,end:t}; lastRainT=t; }
   else if(cur && rain>0){ cur.end=t; lastRainT=t; }
   if(cur && lastRainT!=null && t-lastRainT>=24*3600000){ cur.end=lastRainT; windows.push(cur); cur=null; lastRainT=null; }
 }
 if(cur)windows.push(cur);
 windows.forEach(w=>{
   w.points=allPts.filter(x=>x.t>=w.start&&x.t<=w.end);
   w.rain=w.points.reduce((a,x)=>a+(Number(x.rain)||0),0);
 });
 let meaningful=windows.filter(w=>w.rain>=10 && w.end>=now-24*3600000).sort((a,b)=>a.start-b.start);

 // Se a chuva real chegou antes da previsão, o evento futuro mais próximo
 // passa a ser tratado como em andamento, desde que já exista chuva observada.
 // O próximo evento não substitui um evento que esteja acontecendo.
 const obsNow=getPanelRainAccumulated();
 const observedSignal=Math.max(0,Number(obsNow.r3)||0);
 const nearestFuture=meaningful.find(w=>w.start>now);
 let active=meaningful.find(w=>w.start<=now && w.end>=now);
 if(!active && observedSignal>=0.8 && nearestFuture && (nearestFuture.start-now)<=36*3600000){
   active={...nearestFuture,start:now,earlyArrival:true,forecastStart:nearestFuture.start};
 }
 let event=active || meaningful.find(w=>w.start>=now) || meaningful[0];
 if(!event){
   status.textContent="SEM PERSPECTIVA"; status.className="flood-perspective-status ok";
   body.innerHTML='<div class="flood-perspective-empty"><strong>Sem perspectiva de cheia.</strong><br>O cenário de precipitação disponível não indica elevação do nível acima de 4,00 m.</div>';
   meta.textContent=`ECMWF IFS · cenário recalculado automaticamente · última atualização ${new Date().toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"})}.`; return;
 }

 const trend=riverTrendHistory24h();
 const slope=Number.isFinite(trend.slope)?trend.slope:0;
 const responseDelayH=Math.max(0,Number(window.__learnedResponseDelayH)||9.307);
 const baseNow=Math.max(0,Number(blumenauRiverLevel)||0);
 const isActive=!!active;
 const actualStart=isActive?Math.min(now,event.start):event.start;
 const observed= isActive ? estimateObservedRainForEvent(actualStart,now) : {rain:0,windowH:null};
 const futureRainPoints=event.points.filter(p=>p.t>=now && p.t<=event.end);
 const futureRain=futureRainPoints.reduce((a,p)=>a+(Number(p.rain)||0),0);
 const totalScenarioRain=observed.rain+futureRain;
 const hoursToRain=Math.max(0,(actualStart-now)/3600000);
 const levelAtRainStart=Math.max(0,baseNow+slope*hoursToRain);

 // Durante o evento: a chuva que já ocorreu entra como observada e amadurece
 // pela defasagem da bacia. A previsão é usada somente para as horas futuras.
 // Assim, se chover mais que o previsto, o excedente passa imediatamente a
 // fazer parte do novo cenário sem contar novamente a chuva passada.
 const candidates=[{t:now,level:baseNow}];
 if(isActive && observed.rain>0){
   const observedMatureAt=now+responseDelayH*3600000;
   candidates.push({t:observedMatureAt,level:Math.max(0,baseNow+slope*(observedMatureAt-now)/3600000)+observed.rain*0.046});
 }
 if(!isActive) candidates.push({t:event.start,level:levelAtRainStart});
 for(const p of futureRainPoints){
   const matureAt=p.t+responseDelayH*3600000;
   const maturedFuture=futureRainPoints.filter(r=>r.t<=p.t).reduce((a,r)=>a+(Number(r.rain)||0),0);
   const maturedRain=(isActive?observed.rain:0)+maturedFuture;
   const level=baseNow+slope*((matureAt-now)/3600000)+maturedRain*0.046;
   candidates.push({t:matureAt,level});
 }
 const finalRainAt=futureRainPoints.length?futureRainPoints[futureRainPoints.length-1].t:(isActive?now:event.end);
 const finalT=finalRainAt+responseDelayH*3600000;
 if(!candidates.some(x=>Math.abs(x.t-finalT)<60000)){
   candidates.push({t:finalT,level:baseNow+slope*((finalT-now)/3600000)+totalScenarioRain*0.046});
 }
 const peak=candidates.reduce((best,x)=>x.level>best.level?x:best,candidates[0]);
 const estimatedPeak=Math.max(0,peak.level);
 const peakAt=new Date(peak.t);
 const likely=estimatedPeak>=4;
 status.textContent=isActive?"EM ANDAMENTO":(likely?"ATENÇÃO":"POSSIBILIDADE");
 status.className="flood-perspective-status "+(isActive||likely?"alert":"warn");
 const eventLabel=isActive ? "EVENTO EM ANDAMENTO" : "PRÓXIMO EVENTO";
 body.innerHTML=`<div class="flood-perspective-summary flood-nearest-event">
   <div class="flood-perspective-main ${likely?'is-alert':''}">
     <small>PICO ESTIMADO · ${eventLabel}</small>
     <strong>${fmt(estimatedPeak,2)} m</strong>
   </div>
   <div class="flood-perspective-metric flood-peak-date">
     <small>DATA E HORA DO PICO</small>
     <b>${floodShortDate(peakAt)} · ${peakAt.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})}</b>
     <span>${isActive?'cálculo atualizado pela chuva observada + previsão futura':'previsão calculada para o evento de '+floodShortDate(event.start)+'–'+floodShortDate(event.end)}</span>
   </div>
 </div>`;
 const trendLabel=trend.falling?"queda":trend.rising?"subida":"estável";
 if(isActive){
   const earlyText=event.earlyArrival?` · chuva chegou antes do previsto (${floodShortDate(event.forecastStart)})`:"";
   meta.textContent=`Evento em andamento${earlyText} · ${fmt(observed.rain,1)} mm observados + ${fmt(futureRain,1)} mm ainda previstos = ${fmt(totalScenarioRain,1)} mm no cenário. A previsão passada foi descartada para evitar dupla contagem · tendência 24 h: ${trendLabel} (${slope>=0?'+':''}${fmt(slope,3)} m/h) · resposta da bacia: ${fmt(responseDelayH,1)} h.`;
 }else{
   meta.textContent=`Evento analisado: ${floodWindowLabel(event.start,event.end)} · ${fmt(futureRain,1)} mm previstos. Nível estimado no início da chuva: ${fmt(levelAtRainStart,2)} m · tendência 24 h: ${trendLabel} (${slope>=0?'+':''}${fmt(slope,3)} m/h) · resposta da bacia: ${fmt(responseDelayH,1)} h. O próximo evento só será analisado depois que este evento terminar.`;
 }
}
async function loadRainForecast(){
  const card=$("rainForecastCard"); if(!card)return;
  const statusEl=$("rainForecastStatus"), meta=$("rainForecastMeta");
  try{
    const all=[...ITAJAI_BASIN_POINTS.alto.map(p=>[...p,"alto"]),...ITAJAI_BASIN_POINTS.medio.map(p=>[...p,"medio"])]
      .map((p,i)=>({lat:p[0],lon:p[1],region:p[2],i}));
    const url="https://api.open-meteo.com/v1/forecast?latitude="+all.map(p=>p.lat).join(",")
      +"&longitude="+all.map(p=>p.lon).join(",")
      +"&hourly=precipitation&models=ecmwf_ifs025&forecast_days=15&timezone=America%2FSao_Paulo";
    const res=await fetch(url,{cache:"no-store"});
    if(!res.ok)throw new Error("HTTP "+res.status);
    const data=await res.json();
    const rows=Array.isArray(data)?data:[data];
    const now=new Date();
    const vals=all.map((p,i)=>{
      const d=rows[i], times=d?.hourly?.time||[], rainv=d?.hourly?.precipitation||[];
      const sums=[0,0,0,0,0,0,0], counts=[0,0,0,0,0,0,0];
      for(let j=0;j<Math.min(times.length,rainv.length);j++){
        const t=new Date(times[j]), v=Number(rainv[j]);
        if(t>=now && t<new Date(now.getTime()+360*3600*1000) && Number.isFinite(v)){
          const h=(t-now)/3600000;
          if(h<6)sums[0]+=v;
          if(h<12)sums[1]+=v;
          if(h<24)sums[2]+=v;
          if(h<48)sums[3]+=v;
          if(h<72)sums[4]+=v;
          if(h<192)sums[5]+=v;
          sums[6]+=v;
          if(h<6)counts[0]++;
          if(h<12)counts[1]++;
          if(h<24)counts[2]++;
          if(h<48)counts[3]++;
          if(h<72)counts[4]++;
          if(h<192)counts[5]++;
          counts[6]++;
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
    const a6=basinMean(0),a12=basinMean(1),a24=basinMean(2),a48=basinMean(3),a72=basinMean(4),a8d=basinMean(5),a15d=basinMean(6);
    const hourly=[];
    const baseTimes=rows.find(d=>Array.isArray(d?.hourly?.time))?.hourly?.time||[];
    for(let j=0;j<baseTimes.length;j++){
      const rainVals=rows.map(d=>Number(d?.hourly?.precipitation?.[j])).filter(Number.isFinite);
      if(rainVals.length)hourly.push({t:new Date(baseTimes[j]).getTime(),rain:rainVals.reduce((a,b)=>a+b,0)/rainVals.length});
    }
    ecmwfHourlyBasin=hourly;
    renderFloodPerspective();
    ecmwfForecastRain={"6":a6??0,"12":a12??0,"24":a24??0};
    $("rainForecast12").textContent=a12==null?"—":fmt(a12);
    $("rainForecast24").textContent=a24==null?"—":fmt(a24);
    $("rainForecast48").textContent=a48==null?"—":fmt(a48);
    $("rainForecast72").textContent=a72==null?"—":fmt(a72);
    $("rainForecast8d").textContent=a8d==null?"—":fmt(a8d);
    $("rainForecast15d").textContent=a15d==null?"—":fmt(a15d);
    statusEl.textContent=a15d==null?"SEM DADO":"ECMWF IFS";
    statusEl.className="rain-forecast-status "+(a15d==null?"warn":"ok");
    meta.textContent=a15d==null
      ? "Não foi possível obter a previsão ECMWF agora."
      : `Média espacial uniforme · Alto Vale + Médio Vale · ${vals.length} pontos ECMWF válidos (12+12) · atualização automática a cada 30 min.`;
    renderRiverCalculator();
    renderFloodPerspective();
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
let floodPerspectiveRiverHistory=[];
function setRiverHistory(history){
 const clean=(Array.isArray(history)?history:[]).map(x=>({
   t:new Date(x.t||x.updated).getTime(), level:Number(x.level)
 })).filter(x=>Number.isFinite(x.t)&&Number.isFinite(x.level));
 const byTime=new Map(clean.map(x=>[x.t,x]));
 blumenauRiverHistory=[...byTime.values()].sort((a,b)=>a.t-b.t).slice(-192);
}
function setFloodPerspectiveRiverHistory(history){
 const clean=(Array.isArray(history)?history:[]).map(x=>({
   t:new Date(x.t||x.updated).getTime(), level:Number(x.level)
 })).filter(x=>Number.isFinite(x.t)&&Number.isFinite(x.level));
 const byTime=new Map(clean.map(x=>[x.t,x]));
 floodPerspectiveRiverHistory=[...byTime.values()].sort((a,b)=>a.t-b.t).slice(-192);
}
function riverTrendHistory24h(){
 // A perspectiva usa EXCLUSIVAMENTE o historico JSON do rio, sem ser sobrescrito
 // pelo historico resumido enviado pelo monitor em tempo real.
 const source=floodPerspectiveRiverHistory.length>=2?floodPerspectiveRiverHistory:blumenauRiverHistory;
 if(source.length<2)return {slope:0,rising:false,falling:false,hours:0,samples:source.length};
 const ordered=source.slice().sort((x,y)=>x.t-y.t);
 // Usa as ultimas 24 h disponiveis a partir da amostra mais recente.
 const endT=ordered[ordered.length-1].t;
 const startT=endT-24*60*60*1000;
 const a=ordered.filter(x=>x.t>=startT&&x.t<=endT);
 if(a.length<2)return {slope:0,rising:false,falling:false,hours:0,samples:a.length};
 // Regressao linear sobre as leituras: evita que uma pequena virada nas ultimas
 // horas apague uma queda consistente observada ao longo das 24 h.
 const t0=a[0].t;
 const xs=a.map(x=>(x.t-t0)/3600000);
 const ys=a.map(x=>x.level);
 const mx=xs.reduce((u,v)=>u+v,0)/xs.length;
 const my=ys.reduce((u,v)=>u+v,0)/ys.length;
 let num=0,den=0;
 for(let i=0;i<xs.length;i++){num+=(xs[i]-mx)*(ys[i]-my);den+=(xs[i]-mx)*(xs[i]-mx);}
 const slope=den>0?num/den:0;
 const hours=(a[a.length-1].t-a[0].t)/3600000;
 return {slope,rising:slope>0.001,falling:slope<-0.001,hours,samples:a.length,startLevel:a[0].level,endLevel:a[a.length-1].level,historyStart:a[0].t,historyEnd:a[a.length-1].t};
}
async function loadFloodRiverHistory(){
 try{
   const res=await fetch('./data/river_history.json?ts='+Date.now(),{cache:'no-store'});
   if(!res.ok)throw new Error('HTTP '+res.status);
   const data=await res.json();
   setFloodPerspectiveRiverHistory(data);
   renderFloodPerspective();
 }catch(e){ console.warn('Historico do rio para perspectiva:',e); }
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

let learnedParams={trend_gain:{"2":1,"4":1,"6":1,"12":1,"24":1},rain_gain:{"2":1,"4":1,"6":1,"12":1,"24":1},forecast_rain_gain:{"2":0,"4":0,"6":0,"12":0,"24":0},dam_gain:{"2":1,"4":1,"6":1,"12":1,"24":1},bias_correction:{"2":0,"4":0,"6":0,"12":0,"24":0},persistence_blend:{"2":0,"4":0,"6":0,"12":0,"24":0},damping:{"2":0.88,"4":0.78,"6":0.70,"12":0.55,"24":0.40},metrics:{evaluated:0,mae_m:{"2":null,"4":null,"6":null,"12":null,"24":null}}};
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
async function loadBackendProjection(){
 try{
  const res=await fetch('./data/projection.json?ts='+Date.now(),{cache:'no-store'});
  if(!res.ok)return;
  const d=await res.json();
  const pred=d?.pred||{};
  const valid=['2','4','6','12','24'].every(h=>Number.isFinite(Number(pred[h])));
  const current=Number(d?.current);
  if(!valid||!Number.isFinite(current))return;
  backendProjection=d;
  backendProjectionLoadedAt=Date.now();
  renderRiverCalculator();
 }catch(e){console.warn('Projeção do workflow',e)}
}

async function loadLearnedParams(){
 try{
  const res=await fetch('./data/learning.json?ts='+Date.now(),{cache:'no-store'});
  if(!res.ok)return;
  const d=await res.json();
  learnedParams={...learnedParams,...d,trend_gain:{...learnedParams.trend_gain,...(d.trend_gain||{})},rain_gain:{...learnedParams.rain_gain,...(d.rain_gain||{})},forecast_rain_gain:{"2":0,"4":0,"6":0,"12":0,"24":0},dam_gain:{...learnedParams.dam_gain,...(d.dam_gain||{})},bias_correction:{...learnedParams.bias_correction,...(d.bias_correction||{})},persistence_blend:{...learnedParams.persistence_blend,...(d.persistence_blend||{})},damping:{...learnedParams.damping,...(d.damping||{})}};
  renderRiverCalculator();
 }catch(e){console.warn('Aprendizado automático',e)}
}
function calculateRiverProjection(){
 const current=Number(blumenauRiverLevel);
 if(!Number.isFinite(current)) return null;
 const {r3,r6,r12,r24,r48,r96}=getPanelRainAccumulated();
 if(![r3,r6,r12,r24,r48,r96].some(Number.isFinite))return null;
 const trend=riverTrendHistory24h();
 const rainPotential={2:observedRainPotential(r3,r6,r12,r24,r48,r96,2),4:observedRainPotential(r3,r6,r12,r24,r48,r96,4),6:observedRainPotential(r3,r6,r12,r24,r48,r96,6),12:observedRainPotential(r3,r6,r12,r24,r48,r96,12),24:observedRainPotential(r3,r6,r12,r24,r48,r96,24)};
 const dam=currentDamSignal();
 const out={current,r24:Number(r24)||0,r48:Number(r48)||0,effective48:rainPotential,status:projectionStatus(current),slope:trend.slope,rising:trend.rising,onsetHours:trend.onsetHours,responseWindow:'Resposta suavizada · aprendizado automático',observedRain:true,forecastRain:ecmwfForecastRain,dam};
 for(const h of ['2','4','6','12','24']){
   const maxDown=h==='2'?-0.10:(h==='4'?-0.20:(h==='6'?-0.30:(h==='12'?-0.45:-0.60)));
   const maxUp=h==='2'?0.20:(h==='4'?0.40:(h==='6'?0.60:(h==='12'?1.00:1.40)));
   const base=trend.slope*Number(h);
   const trendPart=Math.max(maxDown,Math.min(maxUp,base))*(Number(learnedParams.trend_gain?.[h]??1))*(Number(learnedParams.damping?.[h]??({2:.88,4:.78,6:.70,12:.55,24:.40}[h])));
   const observedPart=(rainPotential[h]||0)*({2:.07,4:.12,6:.18,12:.38,24:.72}[h])*(Number(learnedParams.rain_gain?.[h]??1));
   // ECMWF é apenas informativo e NÃO participa da projeção do rio.
   const futurePart=0;
   const damSignal=Math.max(0,Math.min(1,(0.20*Number(dam.spillActive||0)+0.35*Math.max(Number(dam.eventOpening||0),Number(dam.recentEventSignal||0)))))*({2:.05,4:.08,6:.10,12:.20,24:.32}[h]);
   const damPart=damSignal*(Number(learnedParams.dam_gain?.[h]??1));
   const biasPart=Number(learnedParams.bias_correction?.[h]??0);
   const blend=Math.max(0,Math.min(0.65,Number(learnedParams.persistence_blend?.[h]??0)));
   const rawProjection=current+trendPart+observedPart+futurePart+damPart+biasPart;
   out['p'+h]=Math.max(0,current+(rawProjection-current)*(1-blend));
   out['components'+h]={trend:trendPart,observedRain:observedPart,forecastRain:futurePart,dam:damPart,bias:biasPart,persistenceBlend:blend};
   out['components'+h]={trend:trendPart,observedRain:observedPart,forecastRain:futurePart,dam:damPart};
 }
 return out;
}
const damLastValid = new Map();

function projectionConfidence(h){
 const mae=Number(learnedParams?.metrics?.mae_m?.[String(h)]);
 const n=Number(learnedParams?.metrics?.evaluated||0);
 const tolerance={2:.45,4:.70,6:1.00,12:1.60,24:2.30}[h]||1;
 if(!Number.isFinite(mae)||mae<0||n<=0)return {value:null,label:"SEM HISTÓRICO SUFICIENTE"};
 const accuracy=Math.max(0,1-mae/tolerance);
 const sample=Math.min(1,Math.log10(n+1)/3);
 const value=Math.round(Math.max(25,Math.min(90,35+accuracy*55*sample)));
 const label=value>=75?"ALTA":value>=55?"MODERADA":"BAIXA";
 return {value,label};
}
function confidenceHtml(h){const c=projectionConfidence(h);return c.value==null?'<span class="river-confidence unavailable">CONFIABILIDADE EM CALIBRAÇÃO</span>':`<span class="river-confidence c-${c.label.toLowerCase()}">${c.value}% · ${c.label}</span>`}

function renderRiverCalculator(){
 const host=$("riverCalculator"); if(!host)return;
 // A projeção visual é recalculada somente quando a leitura oficial do rio muda.
 // Atualizações de chuva, barragens, WebSocket e parâmetros de aprendizado podem
 // ocorrer em segundo plano sem substituir o resultado já exibido.
 let p=displayedRiverProjection;
 // Prefere a projeção calculada pelo workflow, pois ela contém o mesmo
 // aprendizado hidrológico usado no backend. Só aceita dados recentes e
 // compatíveis com o nível atual para nunca mostrar uma projeção antiga como
 // se fosse atual. O cálculo local continua como fallback.
 const bpCurrent=Number(backendProjection?.current);
 const bpAge=(Date.now()-Number(backendProjectionLoadedAt||0))/60000;
 const backendFresh=backendProjection && Number.isFinite(bpCurrent) && bpAge<=35 && Number.isFinite(Number(blumenauRiverLevel)) && Math.abs(bpCurrent-Number(blumenauRiverLevel))<=0.12;
 if(backendFresh){
   p=calculateRiverProjection()||{};
   p={...p,current:bpCurrent};
   for(const h of ['2','4','6','12','24']){
     p['p'+h]=Number(backendProjection.pred[h]);
     p['components'+h]=backendProjection.components?.[h]||{};
   }
   p.status=projectionStatus(bpCurrent);
   p.responseWindow='Modelo adaptativo · aprendizado do workflow';
 }else if(!p){
   p=calculateRiverProjection();
   if(p){
     displayedRiverProjection=p;
     if(displayedProjectionRiverTimestamp===null) displayedProjectionRiverTimestamp=blumenauRiverUpdated||null;
   }
 }
 if(!p){host.innerHTML=`<div class="river-calc-head"><div><div class="river-calc-kicker">CALCULADORA EXPERIMENTAL</div><h3>Projeção do nível em Blumenau</h3><p>Aguardando nível do rio e acumulados de chuva.</p></div><span class="river-calc-badge waiting">AGUARDANDO DADOS</span></div>`;return;}
 const status=p.status;
 // Os cinco horizontes são obrigatórios. Se algum dado futuro chegar
 // incompleto, não exibimos "—" por causa de um campo ausente: calculamos
 // novamente a partir da mesma projeção atual, mantendo a fonte observada.
 const p2=Number.isFinite(p.p2)?p.p2:p.current;
 const p4=Number.isFinite(p.p4)?p.p4:p.current;
 const p6=Number.isFinite(p.p6)?p.p6:p.current;
 const p12=Number.isFinite(p.p12)?p.p12:p.current;
 const p24=Number.isFinite(p.p24)?p.p24:p.current;
 const learnCount=Number(learnedParams.metrics?.evaluated||0);
 const updated=blumenauRiverUpdated?new Date(blumenauRiverUpdated).toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}):"agora";
 const projectionBaseRaw=backendFresh?(backendProjection?.river_timestamp||blumenauRiverUpdated):blumenauRiverUpdated;
 const projectionBase=projectionBaseRaw?new Date(projectionBaseRaw):new Date();
 const projectionLabel=(hours)=>{
   const t=new Date(projectionBase.getTime()+Number(hours)*60*60*1000);
   const sameDay=t.toDateString()===projectionBase.toDateString();
   const hh=t.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit",hour12:false});
   return sameDay?hh:`AMANHÃ ${hh}`;
 };
 host.innerHTML=`
 <div class="river-calc-head"><div><div class="river-calc-kicker">PROJEÇÃO DO RIO · BLUMENAU</div><h3>Nível do Itajaí-Açu</h3><p>Leitura oficial atual + chuva observada dos pluviômetros do painel. ECMWF é apenas informativo.</p></div><span class="river-calc-badge ${status.cls}">${status.label}</span></div>
 <div class="river-calc-layout">
   <div class="river-calc-current">
     <div class="river-calc-current-top"><span>NÍVEL ATUAL</span><span class="river-calc-live"><i></i> AO VIVO</span></div>
     <strong>${fmt(p.current,2)} <em>m</em></strong>
     <div class="river-calc-source">Fonte oficial · ${updated}</div>
   </div>
   <div class="river-calc-forecast-title"><span>PROJEÇÃO</span><small>tendência estimada</small></div>
   <div class="river-calc-forecast">
     <div class="river-calc-item"><small>${projectionLabel(2)}</small><b>${fmt(p2,2)} <em>m</em></b>${confidenceHtml(2)}</div>
     <div class="river-calc-item"><small>${projectionLabel(4)}</small><b>${fmt(p4,2)} <em>m</em></b>${confidenceHtml(4)}</div>
     <div class="river-calc-item"><small>${projectionLabel(6)}</small><b>${fmt(p6,2)} <em>m</em></b>${confidenceHtml(6)}</div>
     <div class="river-calc-item"><small>${projectionLabel(12)}</small><b>${fmt(p12,2)} <em>m</em></b>${confidenceHtml(12)}</div>
   </div>
 </div>
 <div class="river-calc-note river-calc-learning-count">🧠 <span>${learnCount}</span> previsões avaliadas</div>`;
}
function receiveBlumenauRiverMessage(event){
 if(event.origin!==window.location.origin)return;
 const d=event.data||{};
 if(d.type!=="blumenau-river-update")return;
 const level=Number(d.level);
 if(!Number.isFinite(level))return;
 const newUpdated=d.updated||null;
 const riverReadingChanged=(newUpdated && newUpdated!==blumenauRiverUpdated);
 blumenauRiverLevel=level;
 blumenauRiverUpdated=newUpdated;
 blumenauRiverTrend=Number(d.trend)||0;
 setRiverHistory(d.history);
 if(riverReadingChanged || displayedProjectionRiverTimestamp===null){
   displayedRiverProjection=null;
   displayedProjectionRiverTimestamp=newUpdated;
 }
 renderRiverCalculator();
 renderFloodPerspective();
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
function damKey(d){ if(d?._damKey)return d._damKey; const k=norm([name(d),d?.codigo,d?.position?.regiao,d?.position?.bacia].map(txt).join(" ")); if(k.includes("jose boiteux")||k.includes("boiteux")||k.includes("barragem norte")||k.includes("norte"))return "norte"; if(k.includes("ituporanga")||k.includes("barragem sul"))return "sul"; if(k.includes("taio")||k.includes("barragem oeste"))return "oeste"; return null; }
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
 // Quantidade física de comportas de cada barragem. A API pode expor campos
 // genéricos até C10, mas não devemos exibir comportas que não existem.
 const limits={norte:2,sul:5,oeste:7};
 const key=damKey(s);
 const max=limits[key]||10;
 for(let i=1;i<=max;i++){
   const g=c[`comporta_${i}`]; if(!g) continue;
   const rawState=raw(g.estado), state=norm(rawState);
   const open = /abert|open|parcial/.test(state) || rawState===true || rawState===1 || String(rawState).trim()==="1";
   out.push({n:`C${i}`,open});
 }
 return out;
}
function damUpdated(s,stale=false){
 const t=s?.timestamp;
 const prefix=stale?'Último dado válido · ':'Última atualização · ';
 if(!t)return stale?'Último dado válido da Defesa Civil SC':'Atualização recebida da Defesa Civil SC';
 const d=new Date(t);
 return isNaN(d)?prefix+'Defesa Civil SC':prefix+d.toLocaleString("pt-BR");
}
function damScore(s){
 let score=0;
 if(damLevel(s)!=null)score+=4;
 if(damPercent(s)!=null)score+=3;
 const g=gateList(s); if(g.length)score+=2;
 if(s?.timestamp)score+=1;
 return score;
}
function mergeDamReadings(fresh){
 const grouped=new Map();
 for(const raw of fresh||[]){
   const k=damKey(raw); if(!k)continue;
   const s={...raw,label:k==='norte'?'Barragem Norte — José Boiteux':k==='sul'?'Barragem Sul — Ituporanga':'Barragem Oeste — Taió',subtitle:k==='norte'?'Reservatório · Rio Hercílio (Itajaí do Norte)':k==='sul'?'Reservatório · Rio Itajaí do Sul':'Reservatório · Rio Itajaí do Oeste',_damKey:k};
   const cur=grouped.get(k);
   if(!cur||damScore(s)>damScore(cur))grouped.set(k,s);
 }
 const out=[];
 for(const k of ['norte','sul','oeste']){
   const candidate=grouped.get(k), previous=damLastValid.get(k);
   if(candidate && damScore(candidate)>=4){
     candidate._stale=false; damLastValid.set(k,candidate); out.push(candidate);
   }else if(previous){ out.push({...previous,_stale:true}); }
   else if(candidate){ candidate._stale=true; out.push(candidate); }
 }
 return out;
}
function renderDams(){
 const host=$("damCards");if(!host)return;
 if(!dams.length){host.innerHTML='<div class="muted">Nenhuma leitura válida das barragens foi recebida. O painel preservará a última leitura válida quando disponível.</div>';return}
 host.innerHTML=dams.map(d=>{
   const pct=damPercent(d),g=gateList(d),open=g.filter(x=>x.open).length,closed=Math.max(0,g.length-open);
   const width=pct==null?0:Math.max(0,Math.min(100,Number(pct)));
   return `<article class="dam-card elegant-dam ${d._stale?'dam-stale':''}">
     <div class="dam-head elegant-head"><div><div class="dam-kicker">MONITORAMENTO DE BARRAGEM</div><h3>${esc(d.label)}</h3><p>${esc(d.subtitle)}</p></div><span class="dam-live ${d._stale?'stale':''}"><i></i> ${d._stale?'ÚLTIMO DADO':'AO VIVO'}</span></div>
     <section class="dam-capacity"><div class="dam-capacity-label">CAPACIDADE ATUAL</div><div class="dam-capacity-value">${pct==null?'—':fmt(pct,1)}<span>%</span></div><div class="dam-progress"><span style="width:${width}%"></span></div><div class="dam-progress-meta"><span>Ocupação do reservatório</span><span>${pct==null?'Aguardando leitura':'Dados da Defesa Civil'}</span></div></section>
     <section class="dam-gates-panel"><div class="dam-section-title"><span>COMPORTAS</span><b>${g.length?`${open} abertas`:'—'}</b></div>
       <div class="dam-gate-grid">${g.map(x=>`<div class="gate-tile ${x.open?'open':'closed'}"><span>${esc(x.n)}</span><small>${x.open?'ABERTA':'FECHADA'}</small></div>`).join('')||'<span class="muted">Estado das comportas não informado na última leitura válida.</span>'}</div>
       ${g.length?`<div class="gate-summary"><strong>${open}</strong> abertas <i></i> <strong>${closed}</strong> fechadas <i></i> <strong>${g.length}</strong> total</div>`:''}
     </section>
     <footer class="dam-footer"><div>${esc(damUpdated(d,d._stale))}</div><div>Fonte oficial · Defesa Civil SC</div></footer>
   </article>`
 }).join('');
}

function hydroEventDate(ts){
 const d=new Date(Number(ts));
 return Number.isFinite(d.getTime()) ? new Intl.DateTimeFormat("pt-BR",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).format(d) : "—";
}
function hydroEventTime(ts){
 const d=new Date(Number(ts));
 return Number.isFinite(d.getTime()) ? new Intl.DateTimeFormat("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}).format(d) : "—";
}
function hydroEventRainTotal(e){
 const vals=(Array.isArray(e?.samples)?e.samples:[]).map(s=>Number(s?.rain?.h096)).filter(Number.isFinite);
 if(Number.isFinite(e?.rain_total_mm)) return Number(e.rain_total_mm);
 return vals.length ? Math.max(...vals) : null;
}
function hydroEventDuration(e){
 if(Number.isFinite(e?.duration_hours)) return Number(e.duration_hours);
 if(Number.isFinite(e?.ended_at)&&Number.isFinite(e?.started_at)) return Math.max(0,(e.ended_at-e.started_at)/3600000);
 if(Number.isFinite(e?.peak_at)&&Number.isFinite(e?.started_at)) return Math.max(0,(e.peak_at-e.started_at)/3600000);
 return null;
}
function hydroEventAvgRise(e){
 if(Number.isFinite(e?.average_rise_cm_h)) return Number(e.average_rise_cm_h);
 if(Number.isFinite(e?.initial_level)&&Number.isFinite(e?.peak_level)&&Number.isFinite(e?.peak_at)&&Number.isFinite(e?.started_at)){
   const h=(e.peak_at-e.started_at)/3600000;
   return h>0 ? Math.max(0,(e.peak_level-e.initial_level)*100/h) : null;
 }
 return null;
}
function renderHydroEventHistory(){
 const body=$("hydroHistoryBody"), count=$("hydroHistoryCount"), modal=$("hydroHistoryModal"), openBtn=$("openHydroHistory");
 if(!body||!count)return;
 fetch('./data/flood_events.json?ts='+Date.now(),{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.json()}).then(events=>{
   const list=(Array.isArray(events)?events:[]).filter(e=>Number.isFinite(Number(e?.started_at))).sort((a,b)=>Number(b.started_at)-Number(a.started_at));
   count.textContent=`${list.length} ${list.length===1?'evento':'eventos'}`;
   if(!list.length){body.innerHTML='<div class="hydro-history-empty">Ainda não há eventos hidrológicos registrados.</div>';return;}
   body.innerHTML=list.map((e,idx)=>{
     const initial=Number(e.initial_level), peak=Number(e.peak_level), avg=hydroEventAvgRise(e), rain=hydroEventRainTotal(e), dur=hydroEventDuration(e);
     const samples=Array.isArray(e.samples)?e.samples:[];
     const peakDelta=Math.max(0,peak-initial);
     const levels=samples.map(s=>Number(s.level)).filter(Number.isFinite);
     const min=Math.min(...levels,initial,peak), max=Math.max(...levels,initial,peak), range=Math.max(0.01,max-min);
     const bars=levels.slice(-40).map(v=>`<i class="hydro-event-bar" style="height:${Math.max(8,Math.min(100,18+(v-min)/range*82))}%" title="${v.toFixed(2).replace('.',',')} m"></i>`).join('');
     const status=e.status==='closed'?'ENCERRADO':'EM ANDAMENTO';
     return `<article class="hydro-event" id="hydro-event-${idx}">
       <button class="hydro-event-select" type="button" data-event-select="${idx}" aria-expanded="false">
         <span class="hydro-event-select-main"><strong>Evento ${list.length-idx}</strong><small>${hydroEventDate(e.started_at)}</small></span>
         <span class="hydro-event-select-peak">${Number.isFinite(peak)?peak.toFixed(2).replace('.',',')+' m':'—'}<small>PICO</small></span>
         <span class="hydro-event-status">${status}</span><span class="hydro-event-chevron">›</span>
       </button>
       <div class="hydro-event-details">
         <div class="hydro-event-detail-grid">
           <div class="hydro-event-metric"><small>NÍVEL INICIAL</small><b>${Number.isFinite(initial)?initial.toFixed(2).replace('.',',')+' m':'—'}</b><span>${hydroEventTime(e.started_at)}</span></div>
           <div class="hydro-event-metric"><small>PICO</small><b>${Number.isFinite(peak)?peak.toFixed(2).replace('.',',')+' m':'—'}</b><span>${hydroEventTime(e.peak_at)}</span></div>
           <div class="hydro-event-metric"><small>SUBIDA MÉDIA</small><b>${Number.isFinite(avg)?avg.toFixed(1).replace('.',',')+' cm/h':'—'}</b><span>até o pico</span></div>
           <div class="hydro-event-metric"><small>CHUVA OBSERVADA</small><b>${Number.isFinite(rain)?rain.toFixed(1).replace('.',',')+' mm':'—'}</b><span>acumulado</span></div>
           <div class="hydro-event-metric"><small>DURAÇÃO</small><b>${Number.isFinite(dur)?(dur<48?dur.toFixed(1).replace('.',',')+' h':(dur/24).toFixed(1).replace('.',',')+' dias'):'—'}</b><span>evento</span></div>
         </div>
         <div class="hydro-event-details-title">EVOLUÇÃO REGISTRADA DO NÍVEL</div>
         <div class="hydro-event-curve">${bars||'<span class="hydro-history-empty">Sem amostras suficientes para a curva.</span>'}</div>
         <div class="hydro-event-samples">Elevação total até o pico: <strong>${Number.isFinite(peakDelta)?(peakDelta*100).toFixed(0)+' cm':'—'}</strong> · ${samples.length} leituras registradas.</div>
       </div>
     </article>`;
   }).join('');
   body.querySelectorAll('[data-event-select]').forEach(btn=>btn.addEventListener('click',()=>{
     const card=btn.closest('.hydro-event');
     const wasOpen=card.classList.contains('open');
     body.querySelectorAll('.hydro-event.open').forEach(x=>x.classList.remove('open'));
     body.querySelectorAll('[data-event-select]').forEach(x=>x.setAttribute('aria-expanded','false'));
     if(!wasOpen){card.classList.add('open');btn.setAttribute('aria-expanded','true');}
   }));
   if(openBtn && !openBtn.dataset.bound){
     openBtn.dataset.bound='1';
     openBtn.addEventListener('click',()=>{if(modal){modal.classList.add('open');modal.setAttribute('aria-hidden','false');document.body.classList.add('hydro-modal-open');}});
   }
   if(modal && !modal.dataset.bound){
     modal.dataset.bound='1';
     modal.querySelectorAll('[data-close-hydro-history]').forEach(el=>el.addEventListener('click',()=>{modal.classList.remove('open');modal.setAttribute('aria-hidden','true');document.body.classList.remove('hydro-modal-open');}));
     document.addEventListener('keydown',ev=>{if(ev.key==='Escape'&&modal.classList.contains('open')){modal.classList.remove('open');modal.setAttribute('aria-hidden','true');document.body.classList.remove('hydro-modal-open');}});
   }
 }).catch(err=>{
   console.warn('Histórico de eventos',err);
   count.textContent='— eventos';
   body.innerHTML='<div class="hydro-history-empty">Não foi possível carregar o histórico de eventos agora.</div>';
 });
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
 const stationsHost=$("stations");
 if(stationsHost) stationsHost.innerHTML=rainStations.filter(s=>selected.has(String(s.codigo))).slice().sort((a,b)=>{
  const ca=stationCity(a)||"", cb=stationCity(b)||"";
  return ca.localeCompare(cb,"pt-BR") || name(a).localeCompare(name(b),"pt-BR");
 }).map(s=>{
  const code=String(s.codigo), city=stationCity(s), meta=[txt(s.position?.regiao),txt(s.position?.bacia)].filter(Boolean).join(" · ");
  const region=ALTO_VALE.some(c=>norm(c)===norm(city))?"Alto Vale":"Médio Vale";
  return `<article class="station"><span class="station-check" aria-hidden="true">✓</span><span><strong>${esc(name(s))}</strong><small>${esc(city||"Município não identificado")} · ${esc(code)}${meta?" · "+esc(meta):""}</small></span><em>${region}</em></article>`
 }).join("")||'<div class="muted">Nenhuma estação dos municípios do Alto + Médio Vale foi identificada.</div>';

}

renderHydroEventHistory();
async function load(){
 if(reconnecting)return;
 reconnecting=true;
 try{await connect();status("WebSocket conectado. Consultando estações…","ok");const d=await request(TAGS);const q=d?.tags_data?.qualle_meteorologia;allStations=Array.isArray(q)?q.filter(s=>s?.codigo):Object.values(q||{}).filter(s=>s?.codigo);rainStations=allStations.filter(s=>s?.data?.chuva?.acumulado);
   const damStations=allStations.filter(s=>s?.data?.barramento);
   dams=mergeDamReadings(damStations);
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
   dams=mergeDamReadings(fresh);
   selectDefault();
   render();
   renderHydroEventHistory();
   await renderDams();
   await loadRainForecast();
   status(`Atualizado. ${allStations.length} estações recebidas; ${rainStations.length} com dados de chuva.`,"ok");
 }catch(e){console.warn("Atualização da rede",e)}
},60*1000);

load();
loadLearnedParams();
loadBackendProjection();
loadDamHistory();
setInterval(loadDamHistory,15*60*1000);
setInterval(loadLearnedParams,15*60*1000);
setInterval(loadBackendProjection,15*60*1000);

setInterval(loadRainForecast,30*60*1000);

// Maré de Itajaí: dados pré-processados pelo GitHub Actions a partir da imagem oficial.
function tideDate(e){ return new Date(e.date+'T'+e.time+':00-03:00'); }
function tideKind(e){ return e.level_m>=1?'ALTA':'BAIXA'; }
function tideFmt(v){ return Number(v).toFixed(2).replace('.',','); }

let tideEventsCache=[];
function renderTide(events, updatedAt){
  const now=new Date();
  const all=events.map(e=>({...e,dt:tideDate(e)})).sort((a,b)=>a.dt-b.dt);
  const idx=all.findIndex(e=>e.dt>now);
  const next=idx>=0?all[idx]:null;
  const prev=idx>0?all[idx-1]:null;
  const after=idx>=0?all[idx+1]:null;
  if(!next || !prev) throw new Error('sem eventos suficientes para a linha do tempo');

  const span=Math.max(1,next.dt-prev.dt);
  const f=Math.max(0,Math.min(1,(now-prev.dt)/span));
  // Interpolação suave: a maré desacelera perto da alta/baixa em vez de saltar entre os extremos.
  const smooth=(1-Math.cos(Math.PI*f))/2;
  const current=prev.level_m+(next.level_m-prev.level_m)*smooth;
  const direction=next.level_m<prev.level_m?'↓ Maré baixando':'↑ Maré subindo';
  const status=document.getElementById('tideStatus');
  const label=tideKind(next);
  status.textContent=label; status.className='tide-status '+(label==='ALTA'?'high':'low');

  document.getElementById('tideLevel').textContent=tideFmt(current);
  document.getElementById('tideDirection').textContent=direction;
  document.getElementById('tideNextMeta').textContent=`Próximo extremo às ${next.time} · ${tideFmt(next.level_m)} m`;
  document.getElementById('tideNext').textContent=`${next.time} · ${label.toLowerCase()} · ${tideFmt(next.level_m)} m`;

  const fmtTime=e=>e?new Intl.DateTimeFormat('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}).format(e.dt):'—';
  const put=(id,e,kindId)=>{
    document.getElementById(id+'Time').textContent=fmtTime(e);
    document.getElementById(id+'Level').textContent=e?tideFmt(e.level_m)+' m':'—';
    document.getElementById(id+'Kind').textContent=e?tideKind(e):'—';
  };
  put('tidePrev',prev); put('tideNext',next); put('tideAfter',after);

  const marker=document.getElementById('tideNowMarker');
  marker.style.left=(18+f*64)+'%';
  const curve=document.getElementById('tideCurve');
  curve.style.setProperty('--tide-progress',(18+f*64)+'%');
  curve.setAttribute('data-direction',direction.includes('baixando')?'down':'up');

  document.getElementById('tideMeta').textContent=`Fonte: Defesa Civil de Itajaí / UNIVALI · atualizado ${new Date(updatedAt).toLocaleString('pt-BR')}.`;
}

async function loadTide(){
  const card=document.getElementById('tideCard'); if(!card) return;
  try{
    const res=await fetch('./data/mare_itajai.json?ts='+Date.now(),{cache:'no-store'});
    if(!res.ok) throw new Error('dados da maré indisponíveis');
    const d=await res.json(); tideEventsCache=Array.isArray(d.events)?d.events:[];
    renderTide(tideEventsCache,d.updated_at);
  }catch(e){
    console.warn('Maré:',e);
    document.getElementById('tideStatus').textContent='INDISPONÍVEL';
    document.getElementById('tideDirection').textContent='Aguardando atualização';
    document.getElementById('tideNextMeta').textContent='Aguardando atualização da fonte oficial.';
  }
}
loadTide();
loadFloodRiverHistory();
setInterval(loadFloodRiverHistory,15*60*1000);
setInterval(()=>{ if(tideEventsCache.length){ try{ renderTide(tideEventsCache,new Date().toISOString()); }catch(e){ console.warn('Linha da maré:',e); } } },60*1000);
setInterval(loadTide,15*60*1000);
