import asyncio, json, os, re, math, statistics
from datetime import datetime, timezone
import requests
from smart_model import load_params, save_params, weighted_rain, trend_slope, project

WS_URL='wss://monitoramento.defesacivil.sc.gov.br/graphql'
CLIENT='secretaria-de-defesa-civil'
RIVER_URL='https://defesacivil.blumenau.sc.gov.br/static/data/nivel_oficial.json'
DATA_DIR=os.path.join(os.path.dirname(__file__),'..','data')
PARAMS=os.path.join(DATA_DIR,'learning.json')
SNAP=os.path.join(DATA_DIR,'learning_snapshots.json')
HIST=os.path.join(DATA_DIR,'river_history.json')

ALTO=['Agrolandia','Agronomica','Atalanta','Aurora','Braco do Trombudo','Chapadao do Lageado','Dona Emma','Ibirama','Imbuia','Ituporanga','Jose Boiteux','Laurentino','Lontras','Mirim Doce','Petrolandia','Pouso Redondo','Presidente Getulio','Presidente Nereu','Rio do Campo','Rio do Oeste','Rio do Sul','Salete','Santa Terezinha','Taio','Trombudo Central','Vidal Ramos','Vitor Meireles','Witmarsum']
MEDIO=['Apiuna','Ascurra','Benedito Novo','Blumenau','Botuvera','Brusque','Doutor Pedrinho','Gaspar','Guabiruba','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']
CONTRIB=ALTO+['Apiuna','Ascurra','Benedito Novo','Doutor Pedrinho','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']
CITIES={re.sub(r'[^a-z]','',c.lower()):c for c in ALTO+MEDIO}

def norm(s):
 import unicodedata
 return ''.join(ch for ch in unicodedata.normalize('NFD',str(s)).lower() if unicodedata.category(ch)!='Mn')
def station_city(s):
 text=norm(' '.join(str(x or '') for x in [s.get('codigo'),(s.get('name') or {}).get('local'),(s.get('name') or {}).get('general'),(s.get('name') or {}).get('prefix'),(s.get('position') or {}).get('regiao'),(s.get('position') or {}).get('bacia')]))
 for c in ALTO+MEDIO:
  if norm(c) in text:return c
 return None
def val(x):
 if isinstance(x,dict): x=x.get('show',{}).get('value',x.get('value'))
 try:return float(str(x).replace(',','.'))
 except:return None
def rain(s,k):
 return val((((s.get('data') or {}).get('chuva') or {}).get('acumulado') or {}).get(k))

QUERY='''query Tags_data { tags_data(clients: ["%s"]) { qualle_meteorologia { codigo name { prefix general local } timestamp position { bacia latitude longitude regiao altitude } data { chuva { acumulado { h006 { value show { value } } h012 { value show { value } } h024 { value show { value } } h048 { value show { value } } } } } } } } }'''%CLIENT

async def get_ws():
 try:
  import websockets
 except ImportError:
  os.system('pip -q install websockets')
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
 # Aceita tanto JSON puro quanto respostas encapsuladas por proxy/Jina.
 if isinstance(d, dict):
  arr=d.get('niveis') or d.get('levels') or []
  if arr:
   return arr
 return []

def _parse_river_text(text):
 # 1) JSON puro
 try:
  d=json.loads(text)
  if isinstance(d,dict):
   arr=_parse_river_payload(d)
   if arr:return arr
 except Exception:
  pass
 # 2) Jina Reader pode devolver texto ao redor do JSON.
 m=re.search(r'\\{\\s*"niveis"\\s*:\\s*\\[.*\\]\\s*\\}', text, re.S)
 if m:
  try:
   d=json.loads(m.group(0))
   arr=_parse_river_payload(d)
   if arr:return arr
  except Exception:
   pass
 return []

def _request_river(url, verify=True):
 r=requests.get(url,timeout=20,verify=verify,headers={'User-Agent':'Mozilla/5.0'})
 r.raise_for_status()
 return _parse_river_text(r.text)

def get_river():
 # O certificado HTTPS do servidor do AlertaBlu pode não ser aceito
 # pelo ambiente do GitHub Actions. Tentamos o endereço oficial primeiro,
 # depois os mesmos intermediários usados pelo painel e, por último,
 # uma conexão direta sem validação do certificado como fallback técnico.
 # O fallback é restrito a este endpoint público de leitura.
 sources=[
  ('direto', RIVER_URL, True),
  ('corsproxy', 'https://corsproxy.io/?'+requests.utils.quote(RIVER_URL,safe=''), True),
  ('jina', 'https://r.jina.ai/'+RIVER_URL, True),
  ('direto-sem-validacao', RIVER_URL, False),
 ]
 errors=[]
 for label,url,verify in sources:
  try:
   arr=_request_river(url,verify=verify)
   if arr:
    vals=[]
    for x in arr:
     t=x.get('horaLeitura') or x.get('timestamp') or x.get('hora')
     lv=val(x.get('nivel'))
     if t and lv is not None:
      try: ms=int(datetime.fromisoformat(str(t).replace('Z','+00:00')).timestamp()*1000)
      except: continue
      vals.append({'t':ms,'level':lv})
    vals.sort(key=lambda x:x['t'])
    if vals:
     print(f'Fonte do rio: {label} ({len(vals)} leituras)')
     return vals
   errors.append(f'{label}: resposta sem niveis')
  except Exception as e:
   errors.append(f'{label}: {type(e).__name__}: {e}')
 print('Nao foi possivel obter o historico do rio; mantendo historico local se existir.')
 for e in errors: print(e)
 return []

def avg_rain(stations,k):
 by={}
 for s in stations:
  c=station_city(s)
  if c not in CONTRIB:continue
  v=rain(s,k)
  if v is None:continue
  by.setdefault(norm(c),[]).append(v)
 means=[sum(v)/len(v) for v in by.values()]
 return sum(means)/len(means) if means else None

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
 if not cand:return None
 return min(cand,key=lambda x:abs(x['t']-due))

def learn(params,snaps,history):
 total=0; abs_err={h:[] for h in ('6','12','24')}
 now=int(datetime.now(timezone.utc).timestamp()*1000)
 remaining=[]
 for s in snaps:
  resolved=False
  for h in ('6','12','24'):
   key='actual_'+h
   if s.get(key) is not None: continue
   due=s['t']+int(h)*3600000
   if due>now-15*60*1000: continue
   a=nearest_actual(history,due)
   if not a: continue
   pred=s['pred'][h]; err=pred-a['level']
   abs_err[h].append(abs(err)); total+=1; resolved=True
   # Atualização robusta: erro em metros ajusta os ganhos, mas com limites fortes.
   tc=s['components'][h]['trend']; rc=s['components'][h]['rain']
   lr=0.025
   if abs(err)<0.02: lr*=0.5
   params['trend_gain'][h]=max(0.50,min(1.60,params['trend_gain'][h]-lr*err*max(0.05,min(1.0,abs(tc)))))
   params['rain_gain'][h]=max(0.50,min(1.60,params['rain_gain'][h]-lr*err*max(0.05,min(1.0,abs(rc)))))
   s[key]=a['level']; s['error_'+h]=err; s['resolved_'+h]=datetime.now(timezone.utc).isoformat()
  # mantém histórico de snapshots por 60 dias
  if now-s['t']<60*24*3600000: remaining.append(s)
 params['metrics']['evaluated'] = int(params['metrics'].get('evaluated',0))+total
 for h in ('6','12','24'):
  if abs_err[h]:
   params['metrics']['mae_m'][h]=round(sum(abs_err[h])/len(abs_err[h]),4)
 return params,remaining

async def main():
 os.makedirs(DATA_DIR,exist_ok=True)
 params=load_params(PARAMS)
 history=get_river()
 if history: savej(HIST,history[-5000:])
 data=await get_ws(); stations=data.get('tags_data',{}).get('qualle_meteorologia',[]) or []
 r={k:avg_rain(stations,k) for k in ('h006','h012','h024','h048')}
 current=history[-1]['level'] if history else None
 now=history[-1]['t'] if history else int(datetime.now(timezone.utc).timestamp()*1000)
 slope,_=trend_slope(history,now)
 rainp=weighted_rain(r['h006'],r['h012'],r['h024'],r['h048'])
 snaps=loadj(SNAP,[])
 params,snaps=learn(params,snaps,history)
 pred={}; components={}
 if current is not None:
  for h in ('6','12','24'):
   p,tcomp,rcomp=project(current,rainp,slope,h,params); pred[h]=round(p,4);components[h]={'trend':tcomp,'rain':rcomp}
 snaps.append({'t':now,'current':current,'rain':r,'slope':slope,'pred':pred,'components':components})
 snaps=snaps[-1500:]
 params['metrics']['last_snapshot']=datetime.fromtimestamp(now/1000,timezone.utc).isoformat()
 save_params(PARAMS,params);savej(SNAP,snaps)
 # resumo público para o painel
 savej(os.path.join(DATA_DIR,'learning_status.json'),{'updated_at':params['updated_at'],'evaluated':params['metrics']['evaluated'],'mae_m':params['metrics']['mae_m'],'trend_gain':params['trend_gain'],'rain_gain':params['rain_gain'],'damping':params['damping']})
 print(json.dumps({'current':current,'rain':r,'slope':slope,'pred':pred,'evaluated':params['metrics']['evaluated']},ensure_ascii=False))

if __name__=='__main__': asyncio.run(main())
