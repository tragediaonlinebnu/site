import asyncio, json, os, re, math
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
from smart_model import load_params, save_params, weighted_observed_rain, trend_slope, project

WS_URL='wss://monitoramento.defesacivil.sc.gov.br/graphql'
CLIENT='secretaria-de-defesa-civil'
RIVER_URL='https://defesacivil.blumenau.sc.gov.br/static/data/nivel_oficial.json'
DATA_DIR=os.path.join(os.path.dirname(__file__),'..','data')
PARAMS=os.path.join(DATA_DIR,'learning.json')
SNAP=os.path.join(DATA_DIR,'learning_snapshots.json')
HIST=os.path.join(DATA_DIR,'river_history.json')
DAM_HIST=os.path.join(DATA_DIR,'dam_history.json')

ALTO=['Agrolandia','Agronomica','Atalanta','Aurora','Braco do Trombudo','Chapadao do Lageado','Dona Emma','Ibirama','Imbuia','Ituporanga','Jose Boiteux','Laurentino','Lontras','Mirim Doce','Petrolandia','Pouso Redondo','Presidente Getulio','Presidente Nereu','Rio do Campo','Rio do Oeste','Rio do Sul','Salete','Santa Terezinha','Taio','Trombudo Central','Vidal Ramos','Vitor Meireles','Witmarsum']
MEDIO=['Apiuna','Ascurra','Benedito Novo','Blumenau','Botuvera','Brusque','Doutor Pedrinho','Gaspar','Guabiruba','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']
CONTRIB=ALTO+['Apiuna','Ascurra','Benedito Novo','Doutor Pedrinho','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']

# Mesma grade ECMWF usada no painel: 12 pontos no Alto + 12 no Médio Vale.
POINTS={
 'alto':[[-26.82,-49.39],[-26.92,-49.64],[-27.02,-49.29],[-27.08,-49.55],[-27.18,-49.78],[-27.22,-49.48],[-27.30,-49.70],[-27.34,-49.38],[-27.42,-49.64],[-27.48,-49.50],[-27.55,-49.72],[-27.58,-49.32]],
 'medio':[[-26.62,-49.38],[-26.65,-49.17],[-26.70,-49.03],[-26.75,-49.29],[-26.78,-49.12],[-26.82,-49.42],[-26.86,-49.27],[-26.89,-49.10],[-26.93,-49.34],[-26.96,-49.19],[-27.00,-49.06],[-27.03,-48.92]]
}


def norm(s):
 import unicodedata
 return ''.join(ch for ch in unicodedata.normalize('NFD',str(s)).lower() if unicodedata.category(ch)!='Mn')

def clamp(x,a,b): return max(a,min(b,x))

def val(x):
 if isinstance(x,dict): x=x.get('show',{}).get('value',x.get('value'))
 try:return float(str(x).replace(',','.'))
 except:return None

def station_city(s):
 text=norm(' '.join(str(x or '') for x in [s.get('codigo'),(s.get('name') or {}).get('local'),(s.get('name') or {}).get('general'),(s.get('name') or {}).get('prefix'),(s.get('position') or {}).get('regiao'),(s.get('position') or {}).get('bacia')]))
 for c in ALTO+MEDIO:
  if norm(c) in text:return c
 return None

def rain(s,k):
 data=(s.get('data') or {}).get('chuva') or {}
 acc=data.get('acumulado') or {}
 v=val(acc.get(k))
 if v is not None: return max(0.0, v)
 # Quando a Defesa Civil entrega o bloco de acumulados, mas algum horizonte
 # vem sem valor durante período seco, usamos min005 como confirmação de
 # estação ativa sem chuva. Assim não confundimos estação sem dados com 0 mm.
 now5=val(acc.get('min005'))
 if now5 is not None and now5 <= 0.0:
  return 0.0
 return None

QUERY='''query Tags_data {
  tags_data(clients: ["%s"]) {
    qualle_meteorologia {
      codigo
      name { prefix general local }
      timestamp
      position { bacia latitude longitude regiao altitude }
      data {
        chuva { acumulado {
          min005 { value show { value } }
          h003 { value show { value } }
          h006 { value show { value } }
          h012 { value show { value } }
          h024 { value show { value } }
          h048 { value show { value } }
          h096 { value show { value } }
        }}
        barramento {
          nivel { percentual { value show { value } } montante { value show { value } } jusante { value show { value } } vertido { value show { value } } }
          capacidade { atual { value show { value } } maxima { value show { value } } }
          comportas {
            comporta_1 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_2 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_3 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_4 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_5 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_6 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_7 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_8 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_9 { estado { value } habilitada { value } nome { value show { value } } }
            comporta_10 { estado { value } habilitada { value } nome { value show { value } } }
          }
        }
      }
    }
  }
}'''%CLIENT

async def get_ws():
 import websockets
 async with websockets.connect(WS_URL,subprotocols=['graphql-transport-ws'],open_timeout=20,close_timeout=5) as ws:
  await ws.send(json.dumps({'type':'connection_init','payload':{}}))
  for _ in range(10):
   m=json.loads(await asyncio.wait_for(ws.recv(),20))
   if m.get('type')=='connection_ack':break
  await ws.send(json.dumps({'id':'1','type':'subscribe','payload':{'query':QUERY,'variables':{}}}))
  while True:
   m=json.loads(await asyncio.wait_for(ws.recv(),30))
   if m.get('type')=='next': return m.get('payload',{}).get('data',{})
   if m.get('type')=='error': raise RuntimeError(str(m))

def _parse_river_payload(d):
 if isinstance(d,dict): return d.get('niveis') or d.get('levels') or []
 return []

def _parse_river_text(text):
 try:
  d=json.loads(text); arr=_parse_river_payload(d)
  if arr:return arr
 except Exception: pass
 m=re.search(r'\{\s*"niveis"\s*:\s*\[.*\]\s*\}',text,re.S)
 if m:
  try:
   arr=_parse_river_payload(json.loads(m.group(0)))
   if arr:return arr
  except Exception: pass
 return []

def _request_river(url,verify=True):
 r=requests.get(url,timeout=20,verify=verify,headers={'User-Agent':'Mozilla/5.0'})
 r.raise_for_status(); return _parse_river_text(r.text)

def get_river():
 sources=[('direto',RIVER_URL,True),('corsproxy','https://corsproxy.io/?'+requests.utils.quote(RIVER_URL,safe=''),True),('jina','https://r.jina.ai/'+RIVER_URL,True),('direto-sem-validacao',RIVER_URL,False)]
 for label,url,verify in sources:
  try:
   arr=_request_river(url,verify)
   vals=[]
   for x in arr:
    t=x.get('horaLeitura') or x.get('timestamp') or x.get('hora'); lv=val(x.get('nivel'))
    if not t or lv is None: continue
    try:
     raw_t=str(t).replace('Z','+00:00')
     parsed=datetime.fromisoformat(raw_t)
     if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=ZoneInfo('America/Sao_Paulo'))
     ms=int(parsed.timestamp()*1000)
    except: continue
    vals.append({'t':ms,'level':lv})
   vals.sort(key=lambda x:x['t'])
   if vals:
    print(f'Fonte do rio: {label} ({len(vals)} leituras)'); return vals
  except Exception as e: print(f'Falha rio {label}: {type(e).__name__}: {e}')
 print('Nao foi possivel obter o historico do rio; mantendo historico local se existir.')
 return []

def avg_rain(stations,k):
 by={}
 for s in stations:
  c=station_city(s)
  if c not in CONTRIB: continue
  v=rain(s,k)
  if v is None: continue
  by.setdefault(norm(c),[]).append(v)
 means=[sum(v)/len(v) for v in by.values()]
 return sum(means)/len(means) if means else None

def dam_name(s):
 text=norm(' '.join(str(x or '') for x in [s.get('codigo'),(s.get('name') or {}).get('local'),(s.get('name') or {}).get('general'),(s.get('position') or {}).get('regiao'),(s.get('position') or {}).get('bacia')]))
 if 'ituporanga' in text or 'barragem sul' in text: return 'sul'
 if 'taio' in text or 'taio' in norm((s.get('name') or {}).get('local')) or 'barragem oeste' in text: return 'oeste'
 return None

def dam_open_fraction(s):
 c=((s.get('data') or {}).get('barramento') or {}).get('comportas') or {}
 opened=0; total=0
 for i in range(1,11):
  g=c.get(f'comporta_{i}')
  if not g: continue
  total+=1; raw=g.get('estado',{}).get('value') if isinstance(g.get('estado'),dict) else g.get('estado')
  state=norm(raw)
  if raw is True or raw==1 or str(raw).strip()=='1' or re.search(r'abert|open|parcial',state): opened+=1
 return opened/max(1,total),opened,total

def get_dams(stations):
 out={}
 for s in stations:
  if not ((s.get('data') or {}).get('barramento')): continue
  k=dam_name(s)
  if k is None: continue
  frac,opened,total=dam_open_fraction(s)
  b=s.get('data',{}).get('barramento',{})
  nivel=b.get('nivel') or {}
  spill=val((nivel.get('vertido') or {})) or 0
  pct=val((nivel.get('percentual') or {}))
  out[k]={'name':k,'codigo':s.get('codigo'),'timestamp':s.get('timestamp'),'open_fraction':frac,'open':opened,'total_gates':total,'spill':spill,'spill_active':1 if spill>0 else 0,'reservoir_percent':pct}
 return out

def dam_signal_now(dams):
 if not dams:
  return {'open_fraction':0.0,'spill_active':0,'open':0,'total':0,'event_opening':0.0,'recent_event_signal':0.0}
 total_open=sum(int(d.get('open',0) or 0) for d in dams.values())
 total_gates=sum(int(d.get('total_gates',0) or 0) for d in dams.values())
 spill_active=1 if any(int(d.get('spill_active',0) or 0)>0 for d in dams.values()) else 0
 return {
  'open_fraction':total_open/max(1,total_gates),
  'spill_active':spill_active,
  'open':total_open,
  'total':total_gates,
  'event_opening':0.0,
  'recent_event_signal':0.0
 }

def loadj(path,default):
 try:
  with open(path,encoding='utf-8') as f:return json.load(f)
 except:return default

def savej(path,obj):
 os.makedirs(os.path.dirname(path),exist_ok=True); tmp=path+'.tmp'
 with open(tmp,'w',encoding='utf-8') as f:json.dump(obj,f,ensure_ascii=False,indent=2)
 os.replace(tmp,path)

def nearest_actual(history,due):
 cand=[x for x in history if abs(x['t']-due)<=90*60*1000]
 return min(cand,key=lambda x:abs(x['t']-due)) if cand else None

def learn(params,snaps,history):
 total=0; abs_err={h:[] for h in ('6','12','24')}; now=int(datetime.now(timezone.utc).timestamp()*1000); remaining=[]
 for s in snaps:
  for h in ('6','12','24'):
   key='actual_'+h
   if s.get(key) is not None: continue
   due=s['t']+int(h)*3600000
   if due>now-15*60*1000: continue
   a=nearest_actual(history,due)
   if not a: continue
   pred_map=s.get('pred')
   if not isinstance(pred_map,dict) or h not in pred_map: continue
   pred=pred_map[h]
   if not isinstance(pred,(int,float)) or not math.isfinite(pred): continue
   err=pred-a['level']; abs_err[h].append(abs(err)); total+=1
   comps=s.get('components',{}).get(h,{})
   # Aprendizado robusto: erros extremos recebem peso reduzido (Huber) e cada
   # atualização é limitada para que um evento anômalo não domine o modelo.
   ae=abs(err); huber_weight=1.0 if ae<=0.35 else 0.35/max(0.35,ae)
   lr=0.0125*huber_weight
   if ae<0.02: lr*=0.5
   def adj(name,component,lo=.50,hi=1.60):
    try: c=float(component or 0)
    except: c=0.0
    # Preserva o sinal: uma contribuição negativa (ex.: vazante) precisa
    # ensinar o ganho na direção correta. Componentes quase inativos não aprendem.
    if not math.isfinite(c) or abs(c)<0.005: return
    c=clamp(c,-1.0,1.0)
    delta=lr*err*c
    delta=max(-0.035,min(0.035,delta))
    params[name][h]=max(lo,min(hi,params[name][h]-delta))
   adj('trend_gain',comps.get('trend',0))
   adj('rain_gain',comps.get('observed_rain',0))
   # O ECMWF não participa do aprendizado da calculadora.
   adj('dam_gain',comps.get('dam',0),.20,2.00)
   s[key]=a['level']; s['error_'+h]=err; s['resolved_'+h]=datetime.now(timezone.utc).isoformat()
  if now-s['t']<60*24*3600000: remaining.append(s)
 params['metrics']['evaluated']=int(params['metrics'].get('evaluated',0))+total
 for h in ('6','12','24'):
  if abs_err[h]:params['metrics']['mae_m'][h]=round(sum(abs_err[h])/len(abs_err[h]),4)
 return params,remaining

async def main():
 os.makedirs(DATA_DIR,exist_ok=True); params=load_params(PARAMS)
 history=get_river()
 if history:savej(HIST,history[-5000:])
 data=await get_ws(); stations=data.get('tags_data',{}).get('qualle_meteorologia',[]) or []
 r={k:avg_rain(stations,k) for k in ('h003','h006','h012','h024','h048','h096')}
 current=history[-1]['level'] if history else None; now=int(datetime.now(timezone.utc).timestamp()*1000)
 slope,_=trend_slope(history,now)
 rainp={h: weighted_observed_rain(r['h003'],r['h006'],r['h012'],r['h024'],r['h048'],r['h096'],horizon=int(h)) for h in ('6','12','24')}
 dams=get_dams(stations); dam_now=dam_signal_now(dams)
 # ECMWF é apenas informativo no painel e não entra na calculadora/aprendizado.
 future={}
 damhist=loadj(DAM_HIST,[]); previous=damhist[-1].get('dams') if damhist else None
 event=0.0
 if previous:
  for k in ('sul','oeste'):
   a=previous.get(k,{}).get('open_fraction',0); b=dams.get(k,{}).get('open_fraction',0)
   if b>a:event=max(event,float(b-a))
 # Memória de eventos recentes: decai com a idade e é limitado a 6 horas.
 recent_signal=0.0
 for item in reversed(damhist[-30:]):
  age_h=max(0,(now-int(item.get('t',now)))/3600000)
  if age_h>6: break
  sig=float((item.get('signal') or {}).get('event_opening',0) or 0)
  recent_signal=max(recent_signal,sig*math.exp(-age_h/3.0))
 dam_now['event_opening']=clamp(event,0,1)
 dam_now['recent_event_signal']=clamp(max(event,recent_signal),0,1)
 damhist=[x for x in damhist if int(x.get('t',-1))!=now]
 damhist.append({'model_version':6,'t':now,'dams':dams,'signal':dam_now}); damhist=damhist[-3000:]; savej(DAM_HIST,damhist)
 snaps=loadj(SNAP,[])
 # Um instante de coleta representa uma única previsão. Remove duplicatas por timestamp.
 if isinstance(snaps,list):
  dedup={}
  for item in snaps:
   if isinstance(item,dict) and item.get('t') is not None: dedup[int(item['t'])]=item
  snaps=sorted(dedup.values(),key=lambda x:int(x.get('t',0)))
 params,snaps=learn(params,snaps,history)
 pred={}; components={}
 if current is not None:
  for h in ('6','12','24'):
   p,tcomp,rcomp,fcomp,dcomp=project(current,rainp[h],slope,h,params,future,dam_now); pred[h]=round(p,4); components[h]={'trend':tcomp,'observed_rain':rcomp,'forecast_rain':fcomp,'dam':dcomp}
 snaps=[x for x in snaps if int(x.get('t',-1))!=now]
 if current is not None and all(h in pred for h in ('6','12','24')):
  snaps.append({'t':now,'current':current,'rain':r,'future_rain':{},'dams':dam_now,'slope':slope,'pred':pred,'components':components})
 snaps=snaps[-1500:]
 params['metrics']['last_snapshot']=datetime.fromtimestamp(now/1000,timezone.utc).isoformat(); save_params(PARAMS,params);savej(SNAP,snaps)
 savej(os.path.join(DATA_DIR,'learning_status.json'),{'updated_at':params['updated_at'],'evaluated':params['metrics']['evaluated'],'mae_m':params['metrics']['mae_m'],'trend_gain':params['trend_gain'],'rain_gain':params['rain_gain'],'forecast_rain_gain':params['forecast_rain_gain'],'dam_gain':params['dam_gain'],'damping':params['damping']})
 print(json.dumps({'current':current,'rain':r,'future_rain':future,'dams':dam_now,'slope':slope,'pred':pred,'evaluated':params['metrics']['evaluated']},ensure_ascii=False))

if __name__=='__main__':asyncio.run(main())
