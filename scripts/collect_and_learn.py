import asyncio, json, os, re, math, unicodedata
from datetime import datetime, timezone
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
FLOOD_EVENTS=os.path.join(DATA_DIR,'flood_events.json')
RAIN_RESPONSE=os.path.join(DATA_DIR,'rain_response.json')

ALTO=['Agrolandia','Agronomica','Atalanta','Aurora','Braco do Trombudo','Chapadao do Lageado','Dona Emma','Ibirama','Imbuia','Ituporanga','Jose Boiteux','Laurentino','Lontras','Mirim Doce','Petrolandia','Pouso Redondo','Presidente Getulio','Presidente Nereu','Rio do Campo','Rio do Oeste','Rio do Sul','Salete','Santa Terezinha','Taio','Trombudo Central','Vidal Ramos','Vitor Meireles','Witmarsum']
MEDIO=['Apiuna','Ascurra','Benedito Novo','Blumenau','Botuvera','Brusque','Doutor Pedrinho','Gaspar','Guabiruba','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']
CONTRIB=ALTO+['Apiuna','Ascurra','Benedito Novo','Doutor Pedrinho','Indaial','Pomerode','Rio dos Cedros','Rodeio','Timbo']
ALL_CITIES=ALTO+MEDIO
CITY_PATTERNS=[(c,re.compile(r'(?:^|[^a-z])'+re.escape(''.join(ch for ch in unicodedata.normalize('NFD',c).lower() if unicodedata.category(ch)!='Mn'))+r'(?:$|[^a-z])',re.I)) for c in ALL_CITIES]

QUERY='''query Tags_data {
  tags_data(clients: ["%s"]) {
    qualle_meteorologia {
      codigo
      name { prefix general local }
      show
      timestamp
      position { bacia latitude longitude regiao altitude }
      data {
        rio { rio_nome { value } rio_nivel { value show { value } format { value } unit { value } } rio_nivel_tendencia { value show { value } } }
        chuva { acumulado {
          min005 { value show { value } format { value } unit { value } }
          h003 { value show { value } unit { value } }
          h006 { value show { value } unit { value } }
          h012 { value show { value } unit { value } }
          h024 { value show { value } unit { value } }
          h048 { value show { value } unit { value } }
          h096 { value show { value } unit { value } }
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
      filter { relacao { tem_chuva_acumulada tem_nivel_do_rio tem_barragem } }
    }
  }
}''' % CLIENT


def norm(v):
    return ''.join(ch for ch in unicodedata.normalize('NFD',str(v or '')).lower() if unicodedata.category(ch)!='Mn')

def unwrap(v):
    """Extrai o valor real do GraphQL sem confundir show.value com o dado.

    Na rede atual da Defesa Civil, um acumulado chega tipicamente como:
    {"value": 0.4, "show": {"value": true, "unit": null}}
    Portanto, ``value`` é o dado e ``show.value`` é apenas a flag de
    exibição. Só usamos show.value como fallback quando ele não for booleano.
    """
    seen=set()
    cur=v
    for _ in range(8):
        if id(cur) in seen: return None
        seen.add(id(cur))
        if cur is None: return None
        if isinstance(cur,(int,float,bool)): return cur
        if isinstance(cur,str): return cur
        if isinstance(cur,dict):
            # Prioridade absoluta ao campo numérico/real ``value``.
            if 'value' in cur:
                cur=cur['value']; continue
            # Em algumas respostas só existe show.value. Nunca trate a
            # flag booleana true/false como milímetros.
            show=cur.get('show')
            if isinstance(show,dict) and 'value' in show:
                candidate=show['value']
                if isinstance(candidate,bool): return None
                cur=candidate; continue
            return None
        return None
    return None

def val(x):
    x=unwrap(x)
    if x is None or x is True or x is False: return None
    if isinstance(x,(int,float)):
        return float(x) if math.isfinite(float(x)) else None
    s=str(x).strip().replace('\xa0',' ')
    if not s:return None
    # O navegador usa parseFloat(), que aceita valores como "0,4 mm" ou
    # "5.7 mm". O Python float() é mais rígido e antes transformava esses
    # valores em None. Extraímos o primeiro número e aceitamos vírgula decimal.
    m=re.search(r'[-+]?(?:\d+(?:[.,]\d+)?|[.,]\d+)',s.replace(' ',''))
    if not m:return None
    token=m.group(0)
    if ',' in token and '.' in token:
        if token.rfind(',')>token.rfind('.'):
            token=token.replace('.','').replace(',','.')
        else:
            token=token.replace(',','')
    else:
        token=token.replace(',','.')
    try:
        n=float(token)
        return n if math.isfinite(n) else None
    except Exception:
        return None

def text(v):
    x=unwrap(v)
    return '' if x is None else str(x)

def as_stations(value):
    # A API pode entregar qualle_meteorologia como lista ou como objeto
    # indexado pelo código da estação. Aceitamos os dois formatos e também
    # evitamos tratar metadados aninhados como estações.
    if isinstance(value,list):
        return [x for x in value if isinstance(x,dict) and x.get('codigo') is not None]
    if isinstance(value,dict):
        if value.get('codigo') is not None:
            return [value]
        rows=[]
        for x in value.values():
            if isinstance(x,dict) and x.get('codigo') is not None:
                rows.append(x)
        return rows
    return []

def _flatten_text(v, out=None, depth=0):
    if out is None: out=[]
    if depth>8 or v is None: return out
    if isinstance(v,(str,int,float,bool)):
        out.append(str(v))
        return out
    if isinstance(v,dict):
        for key,valv in v.items():
            # Do not discard nested GraphQL value/show structures.
            if key in ('value','local','general','prefix','regiao','bacia','rio_nome','nome','name','codigo'):
                _flatten_text(valv,out,depth+1)
            elif depth<4:
                _flatten_text(valv,out,depth+1)
    elif isinstance(v,list):
        for item in v:
            _flatten_text(item,out,depth+1)
    return out

def station_city(s):
    name=s.get('name') or {}; pos=s.get('position') or {}
    rio=((s.get('data') or {}).get('rio') or {})
    # Primeiro, os mesmos campos usados pelo painel.
    parts=[
        s.get('codigo'),
        name.get('local'), name.get('general'), name.get('prefix'),
        pos.get('regiao'), pos.get('bacia'),
        (rio.get('rio_nome') or {}).get('value')
    ]
    h=norm(' '.join(text(x) for x in parts))
    for city,pat in CITY_PATTERNS:
        if pat.search(h):
            return city

    # Fallback: algumas estações trazem o município em outro metadado
    # aninhado. Procuramos todos os textos da própria estação, sem usar
    # coordenadas nem inferir município por proximidade.
    broad=norm(' '.join(_flatten_text(s)))
    for city,pat in CITY_PATTERNS:
        if pat.search(broad):
            return city
    return None

def rain(s,k):
    data=(s.get('data') or {}).get('chuva') or {}
    acc=data.get('acumulado') or {}
    v=val(acc.get(k))
    if v is not None: return max(0.0,v)
    # Alguns períodos secos chegam com acumulado ausente, mas min005 explícito em 0.
    now5=val(acc.get('min005'))
    if now5 is not None and now5 <= 0.0: return 0.0
    return None

def rain_quality(stations):
    out={}
    for k in ('h003','h006','h012','h024','h048','h096'):
        by_city={}
        station_count=0

        for s in stations:
            city=station_city(s)
            if city not in CONTRIB:
                continue
            v=rain(s,k)
            if v is None:
                continue
            station_count += 1
            by_city.setdefault(norm(city),[]).append(v)

        city_means={
            city: sum(values)/len(values)
            for city,values in by_city.items()
            if values
        }

        value=(
            sum(city_means.values())/len(city_means)
            if city_means else None
        )

        out[k]={
            'stations':station_count,
            'cities':len(city_means),
            'value':value
        }

    return out


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

async def get_ws():
    import websockets
    async with websockets.connect(WS_URL,subprotocols=['graphql-transport-ws'],open_timeout=20,close_timeout=5) as ws:
        await ws.send(json.dumps({'type':'connection_init','payload':{}}))
        ack=False
        for _ in range(10):
            m=json.loads(await asyncio.wait_for(ws.recv(),20))
            if m.get('type')=='connection_ack': ack=True; break
            if m.get('type')=='error': raise RuntimeError(str(m))
        if not ack: raise RuntimeError('WebSocket não confirmou connection_ack')
        await ws.send(json.dumps({'id':'1','type':'subscribe','payload':{'query':QUERY,'variables':{}}}))
        while True:
            m=json.loads(await asyncio.wait_for(ws.recv(),30))
            if m.get('type')=='ping':
                await ws.send(json.dumps({'type':'pong'})); continue
            if m.get('type')=='next': return m.get('payload',{}).get('data',{})
            if m.get('type')=='error': raise RuntimeError(str(m))


def _parse_river_payload(d): return d.get('niveis') or d.get('levels') or [] if isinstance(d,dict) else []
def _parse_river_text(textv):
    try:
        d=json.loads(textv); arr=_parse_river_payload(d)
        if arr:return arr
    except Exception: pass
    m=re.search(r'\{\s*"niveis"\s*:\s*\[.*\]\s*\}',textv,re.S)
    if m:
        try:
            arr=_parse_river_payload(json.loads(m.group(0)))
            if arr:return arr
        except Exception: pass
    return []

def _request_river(url,verify=True):
    r=requests.get(url,timeout=20,verify=verify,headers={'User-Agent':'Mozilla/5.0'}); r.raise_for_status(); return _parse_river_text(r.text)

def get_river():
    sources=[('direto',RIVER_URL,True),('corsproxy','https://corsproxy.io/?'+requests.utils.quote(RIVER_URL,safe=''),True),('jina','https://r.jina.ai/'+RIVER_URL,True),('direto-sem-validacao',RIVER_URL,False)]
    for label,url,verify in sources:
        try:
            arr=_request_river(url,verify); vals=[]
            for x in arr:
                t=x.get('horaLeitura') or x.get('timestamp') or x.get('hora'); lv=val(x.get('nivel'))
                if not t or lv is None: continue
                try:
                    parsed=datetime.fromisoformat(str(t).replace('Z','+00:00'))
                    if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=ZoneInfo('America/Sao_Paulo'))
                    ms=int(parsed.timestamp()*1000)
                except Exception: continue
                vals.append({'t':ms,'level':lv})
            vals.sort(key=lambda x:x['t'])
            if vals:
                print(f'Fonte do rio: {label} ({len(vals)} leituras)'); return vals
        except Exception as e: print(f'Falha rio {label}: {type(e).__name__}: {e}')
    return []


def dam_name(s):
    textv=norm(' '.join(text(x) for x in [s.get('codigo'),(s.get('name') or {}).get('local'),(s.get('name') or {}).get('general'),(s.get('position') or {}).get('regiao'),(s.get('position') or {}).get('bacia')]))
    if 'ituporanga' in textv or 'barragem sul' in textv:return 'sul'
    if 'taio' in textv or 'barragem oeste' in textv:return 'oeste'
    return None

def dam_open_fraction(s):
    c=((s.get('data') or {}).get('barramento') or {}).get('comportas') or {}; opened=0; total=0
    for i in range(1,11):
        g=c.get(f'comporta_{i}')
        if not g: continue
        total+=1; raw=unwrap(g.get('estado')); state=norm(raw)
        if raw is True or raw==1 or str(raw).strip()=='1' or re.search(r'abert|open|parcial',state): opened+=1
    return opened/max(1,total),opened,total

def get_dams(stations):
    out={}
    for s in stations:
        if not ((s.get('data') or {}).get('barramento')): continue
        k=dam_name(s)
        if k is None: continue
        frac,opened,total=dam_open_fraction(s); b=s.get('data',{}).get('barramento',{}); nivel=b.get('nivel') or {}
        spill=val(nivel.get('vertido')) or 0; pct=val(nivel.get('percentual'))
        out[k]={'name':k,'codigo':s.get('codigo'),'timestamp':s.get('timestamp'),'open_fraction':frac,'open':opened,'total_gates':total,'spill':spill,'spill_active':1 if spill>0 else 0,'reservoir_percent':pct}
    return out

def dam_signal_now(dams):
    if not dams:return {'open_fraction':0.0,'spill_active':0,'open':0,'total':0,'event_opening':0.0,'recent_event_signal':0.0}
    total_open=sum(int(d.get('open',0) or 0) for d in dams.values()); total_gates=sum(int(d.get('total_gates',0) or 0) for d in dams.values())
    return {'open_fraction':total_open/max(1,total_gates),'spill_active':1 if any(int(d.get('spill_active',0) or 0)>0 for d in dams.values()) else 0,'open':total_open,'total':total_gates,'event_opening':0.0,'recent_event_signal':0.0}

def loadj(path,default):
    try:
        with open(path,encoding='utf-8') as f:return json.load(f)
    except Exception:return default

def savej(path,obj):
    os.makedirs(os.path.dirname(path),exist_ok=True); tmp=path+'.tmp'
    with open(tmp,'w',encoding='utf-8') as f:json.dump(obj,f,ensure_ascii=False,indent=2)
    os.replace(tmp,path)

# Camadas novas de observação/aprendizado. Elas NÃO alteram a previsão atual.
# Primeiro acumulam eventos reais; só no futuro poderão servir como referência.
FLOOD_START_LEVEL=4.00
FLOOD_END_LEVEL=3.90  # histerese para não abrir/fechar o evento na mesma faixa
RAIN_ONSET_MM_3H=2.0
RIVER_RESPONSE_RISE_M=0.05
RAIN_RESPONSE_MAX_HOURS=24.0

def default_rain_response():
    """Estado de aprendizado hidrológico, separado da previsão principal.

    A camada aprende padrões do tipo:
    quantidade/intensidade de chuva -> atraso -> subida -> velocidade.
    Por enquanto é somente observacional para evitar regressões.
    """
    return {
        'version': 2,
        'reference_delay_hours': 5.0,
        'learned_delay_hours': 5.0,
        'events': 0,
        'sum_delay_hours': 0.0,
        'mean_delay_hours': None,
        'sum_initial_rise_rate_cm_h': 0.0,
        'sum_max_rise_rate_cm_h': 0.0,
        'mean_initial_rise_rate_cm_h': None,
        'mean_max_rise_rate_cm_h': None,
        'patterns': {},
        'event_history': [],
        'active': None,
        'last_event': None,
        'updated_at': None
    }

def _num(v, default=0.0):
    try:
        x=float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default

def _rain_band(mm):
    mm=max(0.0,_num(mm))
    for lo,hi,label in ((0,10,'0_10'),(10,30,'10_30'),(30,60,'30_60'),(60,100,'60_100'),(100,150,'100_150')):
        if lo<=mm<hi:return label
    return '150_plus'

def _rain_pattern_key(active):
    # Usa a janela mais curta disponível que represente o evento, evitando
    # inventar uma intensidade instantânea a partir de acumulados longos.
    r3=max(0.0,_num(active.get('max_rain_h003')))
    r6=max(r3,_num(active.get('max_rain_h006')))
    r12=max(r6,_num(active.get('max_rain_h012')))
    if r3>=RAIN_ONSET_MM_3H:return _rain_band(r3)+'mm_3h'
    if r6>=RAIN_ONSET_MM_3H:return _rain_band(r6)+'mm_6h'
    if r12>=RAIN_ONSET_MM_3H:return _rain_band(r12)+'mm_12h'
    return _rain_band(max(r12,_num(active.get('max_rain_h024'))))+'mm_24h'

def _update_pattern(pattern, event):
    n=int(pattern.get('events',0) or 0)+1
    delay=_num(event.get('delay_hours'))
    rise_cm=_num(event.get('rise_cm'))
    initial_rate=_num(event.get('initial_rise_rate_cm_h'))
    max_rate=_num(event.get('max_rise_rate_cm_h'))
    for key,value in (('sum_delay_hours',delay),('sum_rise_cm',rise_cm),('sum_initial_rise_rate_cm_h',initial_rate),('sum_max_rise_rate_cm_h',max_rate)):
        pattern[key]=_num(pattern.get(key))+value
    pattern['events']=n
    pattern['mean_delay_hours']=round(pattern['sum_delay_hours']/n,3)
    pattern['mean_rise_cm']=round(pattern['sum_rise_cm']/n,3)
    pattern['mean_initial_rise_rate_cm_h']=round(pattern['sum_initial_rise_rate_cm_h']/n,3)
    pattern['mean_max_rise_rate_cm_h']=round(pattern['sum_max_rise_rate_cm_h']/n,3)
    return pattern

def update_rain_response(state, now, current, rain, slope):
    """Aprende chuva -> atraso -> subida -> velocidade, sem mexer na previsão.

    Um candidato é aberto por chuva significativa. A resposta só é aceita após
    subida real >=5 cm e tendência positiva. O evento é classificado por faixa
    de mm e janela, permitindo acumular padrões sem criar relações falsas do
    tipo '37.42 mm = exatamente X cm'.
    """
    if not isinstance(state,dict): state=default_rain_response()
    base=default_rain_response()
    for k,v in base.items(): state.setdefault(k,v)
    rain=rain or {}
    active=state.get('active')
    r3=max(0.0,_num(rain.get('h003')))
    r6=max(r3,_num(rain.get('h006')))
    r12=max(r6,_num(rain.get('h012')))
    r24=max(r12,_num(rain.get('h024')))

    if active is None and current is not None and (r3>=RAIN_ONSET_MM_3H or r6>=RAIN_ONSET_MM_3H):
        active={
            'started_at':now,
            'baseline_level':float(current),
            'max_level':float(current),
            'max_rain_h003':r3,'max_rain_h006':r6,
            'max_rain_h012':r12,'max_rain_h024':r24,
            'response_detected_at':None,
            'response_level':None,
            'initial_rise_rate_cm_h':None,
            'max_rise_rate_cm_h':max(0.0,_num(slope)*100.0)
        }
        state['active']=active

    if isinstance(active,dict) and current is not None:
        active['max_level']=max(_num(active.get('max_level'),current),float(current))
        active['max_rain_h003']=max(_num(active.get('max_rain_h003')),r3)
        active['max_rain_h006']=max(_num(active.get('max_rain_h006')),r6)
        active['max_rain_h012']=max(_num(active.get('max_rain_h012')),r12)
        active['max_rain_h024']=max(_num(active.get('max_rain_h024')),r24)
        active['max_rise_rate_cm_h']=max(_num(active.get('max_rise_rate_cm_h')),max(0.0,_num(slope)*100.0))
        delay_h=max(0.0,(now-int(active.get('started_at',now)))/3600000.0)
        rise=float(current)-_num(active.get('baseline_level'),float(current))

        if rise>=RIVER_RESPONSE_RISE_M and _num(slope)>0 and active.get('response_detected_at') is None:
            # Detecta o atraso imediatamente, mas continua observando por 12h
            # para aprender também a velocidade máxima e a subida acumulada.
            active['response_detected_at']=now
            active['response_level']=float(current)
            active['initial_rise_rate_cm_h']=max(0.0,_num(slope)*100.0)
            active['response_delay_hours']=delay_h

        response_at=active.get('response_detected_at')
        if response_at is not None:
            response_age_h=max(0.0,(now-int(response_at))/3600000.0)
            if response_age_h>=12.0:
                event={
                    'rain_started_at':active.get('started_at'),
                    'river_response_at':response_at,
                    'completed_at':now,
                    'delay_hours':round(_num(active.get('response_delay_hours')),3),
                    'rain_h003':round(_num(active.get('max_rain_h003')),3),
                    'rain_h006':round(_num(active.get('max_rain_h006')),3),
                    'rain_h012':round(_num(active.get('max_rain_h012')),3),
                    'rain_h024':round(_num(active.get('max_rain_h024')),3),
                    'rise_cm':round((float(active.get('max_level',current))-_num(active.get('baseline_level'),current))*100.0,3),
                    'initial_rise_rate_cm_h':round(_num(active.get('initial_rise_rate_cm_h')),3),
                    'max_rise_rate_cm_h':round(_num(active.get('max_rise_rate_cm_h')),3),
                    'pattern':_rain_pattern_key(active)
                }
                state['events']=int(state.get('events',0) or 0)+1
                n=state['events']
                state['sum_delay_hours']=_num(state.get('sum_delay_hours'))+_num(event['delay_hours'])
                state['mean_delay_hours']=round(state['sum_delay_hours']/n,3)
                state['learned_delay_hours']=clamp(state['mean_delay_hours'],1.0,24.0)
                state['sum_initial_rise_rate_cm_h']=_num(state.get('sum_initial_rise_rate_cm_h'))+_num(event['initial_rise_rate_cm_h'])
                state['sum_max_rise_rate_cm_h']=_num(state.get('sum_max_rise_rate_cm_h'))+_num(event['max_rise_rate_cm_h'])
                state['mean_initial_rise_rate_cm_h']=round(state['sum_initial_rise_rate_cm_h']/n,3)
                state['mean_max_rise_rate_cm_h']=round(state['sum_max_rise_rate_cm_h']/n,3)
                patterns=state.setdefault('patterns',{})
                key=event['pattern']; patterns[key]=_update_pattern(dict(patterns.get(key) or {}),event)
                state['event_history']=(list(state.get('event_history') or [])+[event])[-500:]
                state['last_event']=event
                state['active']=None
        elif delay_h>RAIN_RESPONSE_MAX_HOURS:
            state['active']=None
    state['updated_at']=datetime.now(timezone.utc).isoformat()
    return state

def update_flood_events(events, now, current, rain, dams, slope):
    """Registra a cheia completa acima de 4 m, sem interferir no modelo."""
    if not isinstance(events,list): events=[]
    active=None
    for e in reversed(events):
        if isinstance(e,dict) and e.get('status')=='active':
            active=e; break
    sample={
        't':now, 'level':round(float(current),4),
        'rain':dict(rain or {}), 'slope_m_per_h':round(float(slope or 0.0),6)
    }
    if active is None and current>=FLOOD_START_LEVEL:
        active={
            'id':datetime.fromtimestamp(now/1000,timezone.utc).strftime('flood-%Y%m%dT%H%M%SZ'),
            'status':'active','started_at':now,'threshold_crossed_at':now,
            'initial_level':round(float(current),4),'peak_level':round(float(current),4),'peak_at':now,
            'max_rise_rate_m_per_h':max(0.0,float(slope or 0.0)),
            'samples':[sample],'dam_events':[{'t':now,'dams':dict(dams or {})}]
        }
        events.append(active)
    elif active is not None:
        samples=active.setdefault('samples',[])
        if not samples or int(samples[-1].get('t',-1))!=now: samples.append(sample)
        if current>float(active.get('peak_level',current)):
            active['peak_level']=round(float(current),4); active['peak_at']=now
        active['max_rise_rate_m_per_h']=max(float(active.get('max_rise_rate_m_per_h',0.0)),max(0.0,float(slope or 0.0)))
        active.setdefault('dam_events',[]).append({'t':now,'dams':dict(dams or {})})
        # Encerra somente depois de cair abaixo de 3,90 m e estar sem tendência de alta.
        if current<FLOOD_END_LEVEL and slope<=0:
            active['status']='closed'; active['ended_at']=now
            active['duration_hours']=round((now-int(active['started_at']))/3600000.0,3)
            active['peak_rise_m']=round(float(active['peak_level'])-float(active['initial_level']),4)
    # Limite alto apenas para impedir crescimento ilimitado acidental.
    return events[-200:]

def nearest_actual(history,due):
    cand=[x for x in history if abs(x['t']-due)<=90*60*1000]; return min(cand,key=lambda x:abs(x['t']-due)) if cand else None

def learn(params,snaps,history):
    total=0; abs_err={h:[] for h in ('2','4','6','12','24')}; now=int(datetime.now(timezone.utc).timestamp()*1000); remaining=[]
    for s in snaps:
        for h in ('2','4','6','12','24'):
            key='actual_'+h
            if s.get(key) is not None: continue
            due=s['t']+int(h)*3600000
            if due>now-15*60*1000: continue
            a=nearest_actual(history,due)
            if not a: continue
            pred=s.get('pred',{}); pred=pred.get(h) if isinstance(pred,dict) else None
            if not isinstance(pred,(int,float)) or not math.isfinite(pred): continue
            err=pred-a['level']; abs_err[h].append(abs(err)); total+=1; comps=s.get('components',{}).get(h,{})
            # Métricas históricas acumuladas por horizonte. O campo antigo
            # mae_m é mantido para compatibilidade, mas passa a refletir o
            # histórico acumulado quando novos resultados forem resolvidos.
            metrics=params.setdefault('metrics',{})
            by=metrics.setdefault('by_horizon',{})
            m=by.setdefault(h,{'evaluated':0,'sum_abs_error':0.0,'sum_error':0.0,'mae_m':None,'bias_m':None,'recent_abs_errors':[]})
            m['evaluated']=int(m.get('evaluated',0) or 0)+1
            m['sum_abs_error']=float(m.get('sum_abs_error',0.0) or 0.0)+abs(err)
            m['sum_error']=float(m.get('sum_error',0.0) or 0.0)+err
            m['mae_m']=round(m['sum_abs_error']/m['evaluated'],4)
            m['bias_m']=round(m['sum_error']/m['evaluated'],4)
            recent=list(m.get('recent_abs_errors',[]) or [])
            recent.append(round(abs(err),4)); m['recent_abs_errors']=recent[-50:]
            ae=abs(err); huber_weight=1.0 if ae<=0.35 else 0.35/max(0.35,ae); lr=0.0125*huber_weight
            if ae<0.02: lr*=0.5
            def adj(name,component,lo=.50,hi=1.60):
                try:c=float(component or 0)
                except Exception:c=0.0
                if not math.isfinite(c) or abs(c)<0.005:return
                c=clamp(c,-1.0,1.0); delta=max(-0.035,min(0.035,lr*err*c)); params[name][h]=max(lo,min(hi,params[name][h]-delta))
            adj('trend_gain',comps.get('trend',0)); adj('rain_gain',comps.get('observed_rain',0)); adj('dam_gain',comps.get('dam',0),.20,2.00)
            s[key]=a['level']; s['error_'+h]=err; s['resolved_'+h]=datetime.now(timezone.utc).isoformat()
        if now-s['t']<60*24*3600000:remaining.append(s)
    params['metrics']['evaluated']=int(params['metrics'].get('evaluated',0))+total
    params['metrics'].setdefault('mae_m',{})
    for h in ('2','4','6','12','24'):
        m=(params['metrics'].get('by_horizon') or {}).get(h,{})
        if m.get('evaluated',0):
            params['metrics']['mae_m'][h]=m.get('mae_m')
        elif abs_err[h]:
            params['metrics']['mae_m'][h]=round(sum(abs_err[h])/len(abs_err[h]),4)
    return params,remaining

def clamp(x,a,b):return max(a,min(b,x))

async def main():
    os.makedirs(DATA_DIR,exist_ok=True); params=load_params(PARAMS)
    history=get_river()
    if history:
        existing=loadj(HIST,[])
        if not isinstance(existing,list):
            existing=[]

        merged={}
        for item in existing+history:
            if isinstance(item,dict) and item.get('t') is not None:
                merged[str(item['t'])]=item

        combined=sorted(
            merged.values(),
            key=lambda x: float(x.get('t',0))
        )
        savej(HIST,combined[-5000:])
    data=await get_ws(); stations=as_stations((data.get('tags_data') or {}).get('qualle_meteorologia'))
    if not stations: raise RuntimeError('WebSocket respondeu, mas nenhuma estação meteorológica foi encontrada.')
    rain_stations=[s for s in stations if isinstance(((s.get('data') or {}).get('chuva') or {}).get('acumulado'),dict)]
    print(f'Estacoes meteorologicas recebidas: {len(stations)}')
    print(f'Estacoes com bloco de chuva acumulada: {len(rain_stations)}')
    print(f'Estacoes com municipio identificado: {sum(1 for s in rain_stations if station_city(s))}')
    sample=[]
    for s in rain_stations:
        if station_city(s) in CONTRIB and len(sample)<5:
            acc=((s.get('data') or {}).get('chuva') or {}).get('acumulado') or {}
            sample.append({
                'codigo': s.get('codigo'),
                'municipio': station_city(s),
                'h003_raw': acc.get('h003'),
                'h024_raw': acc.get('h024')
            })
    if sample:
        print('Amostra bruta de chuva: '+json.dumps(sample,ensure_ascii=False,default=str))
    quality=rain_quality(rain_stations)
    r={k:quality[k]['value'] for k in quality}
    # O painel público demonstra que a rede entrega acumulados. Não salvamos um
    # snapshot silenciosamente nulo: se não houver nenhum dado contribuinte,
    # a execução falha e o próximo ciclo tenta novamente.
    required=('h003','h006','h012','h024','h048','h096')
    if not all(quality[k]['cities']>0 and quality[k]['value'] is not None for k in required):
        raise RuntimeError('Dados de chuva insuficientes: '+json.dumps(quality,ensure_ascii=False))
    current=history[-1]['level'] if history else None; now=int(datetime.now(timezone.utc).timestamp()*1000)
    slope,_=trend_slope(history,now)
    rainp={h:weighted_observed_rain(r['h003'],r['h006'],r['h012'],r['h024'],r['h048'],r['h096'],horizon=int(h)) for h in ('2','4','6','12','24')}
    # Novas camadas são somente observacionais: não alteram project().
    response_state=loadj(RAIN_RESPONSE,default_rain_response())
    response_state=update_rain_response(response_state,now,current,r,slope)
    savej(RAIN_RESPONSE,response_state)
    dams=get_dams(stations); dam_now=dam_signal_now(dams)
    future={}
    damhist=loadj(DAM_HIST,[]); previous=damhist[-1].get('dams') if damhist else None; event=0.0
    if previous:
        for k in ('sul','oeste'):
            a=previous.get(k,{}).get('open_fraction',0); b=dams.get(k,{}).get('open_fraction',0)
            if b>a:event=max(event,float(b-a))
    recent_signal=0.0
    for item in reversed(damhist[-30:]):
        age_h=max(0,(now-int(item.get('t',now)))/3600000)
        if age_h>6:break
        sig=float((item.get('signal') or {}).get('event_opening',0) or 0); recent_signal=max(recent_signal,sig*math.exp(-age_h/3.0))
    dam_now['event_opening']=clamp(event,0,1); dam_now['recent_event_signal']=clamp(max(event,recent_signal),0,1)
    if current is not None:
        flood_events=loadj(FLOOD_EVENTS,[])
        flood_events=update_flood_events(flood_events,now,current,r,dam_now,slope)
        savej(FLOOD_EVENTS,flood_events)
    damhist=[x for x in damhist if int(x.get('t',-1))!=now]; damhist.append({'model_version':8,'t':now,'dams':dams,'signal':dam_now}); savej(DAM_HIST,damhist[-3000:])
    snaps=loadj(SNAP,[]); snaps=snaps if isinstance(snaps,list) else []
    dedup={int(x['t']):x for x in snaps if isinstance(x,dict) and x.get('t') is not None}; snaps=sorted(dedup.values(),key=lambda x:int(x.get('t',0)))
    params,snaps=learn(params,snaps,history)
    pred={}; components={}
    if current is not None:
        for h in ('2','4','6','12','24'):
            p,tcomp,rcomp,fcomp,dcomp=project(current,rainp[h],slope,h,params,future,dam_now); pred[h]=round(p,4); components[h]={'trend':tcomp,'observed_rain':rcomp,'forecast_rain':fcomp,'dam':dcomp}
    snaps=[x for x in snaps if int(x.get('t',-1))!=now]
    if current is not None and all(h in pred for h in ('2','4','6','12','24')):
        snaps.append({'t':now,'current':current,'rain':r,'rain_quality':quality,'future_rain':{},'dams':dam_now,'slope':slope,'pred':pred,'components':components})
    snaps=snaps[-1500:]
    params['metrics']['last_snapshot']=datetime.fromtimestamp(now/1000,timezone.utc).isoformat(); save_params(PARAMS,params)
    savej(SNAP,snaps)
    metrics_by=params['metrics'].get('by_horizon',{})
    savej(os.path.join(DATA_DIR,'learning_status.json'),{'version':10,'updated_at':params['updated_at'],'evaluated':params['metrics']['evaluated'],'mae_m':params['metrics']['mae_m'],'metrics_by_horizon':metrics_by,'trend_gain':params['trend_gain'],'rain_gain':params['rain_gain'],'forecast_rain_gain':params['forecast_rain_gain'],'dam_gain':params['dam_gain'],'damping':params['damping'],'rain_quality':quality,'rain_response':response_state})
    print(json.dumps({'current':current,'rain':r,'rain_quality':quality,'future_rain':future,'dams':dam_now,'slope':slope,'pred':pred,'evaluated':params['metrics']['evaluated']},ensure_ascii=False))

if __name__=='__main__': asyncio.run(main())
