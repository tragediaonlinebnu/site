import json, math, os
from datetime import datetime, timezone

DEFAULT = {
  "version": 4,
  "trend_gain": {"6":1.0,"12":1.0,"24":1.0},
  "rain_gain": {"6":1.0,"12":1.0,"24":1.0},
  "forecast_rain_gain": {"6":1.0,"12":1.0,"24":1.0},
  "dam_gain": {"6":1.0,"12":1.0,"24":1.0},
  "damping": {"6":0.70,"12":0.55,"24":0.40},
  "metrics": {"evaluated":0,"mae_m":{"6":None,"12":None,"24":None},"last_update":None},
  "updated_at": None
}

HORIZONS=('6','12','24')

def clamp(x,a,b): return max(a,min(b,x))
def finite(x): return isinstance(x,(int,float)) and math.isfinite(x)

def load_params(path):
    try:
        with open(path,encoding='utf-8') as f: d=json.load(f)
    except Exception: d={}
    out=json.loads(json.dumps(DEFAULT))
    for k in ('trend_gain','rain_gain','forecast_rain_gain','dam_gain','damping'):
        if isinstance(d.get(k),dict):
            for h in HORIZONS:
                if finite(d[k].get(h)): out[k][h]=d[k][h]
    if isinstance(d.get('metrics'),dict):
        out['metrics'].update(d['metrics'])
    return out

def save_params(path,p):
    p['updated_at']=datetime.now(timezone.utc).isoformat()
    p['metrics']['last_update']=p['updated_at']
    tmp=path+'.tmp'
    with open(tmp,'w',encoding='utf-8') as f: json.dump(p,f,ensure_ascii=False,indent=2)
    os.replace(tmp,path)

def weighted_observed_rain(r3,r6,r12,r24,r48=0,r96=0,horizon=24):
    vals=[max(0,float(x or 0)) for x in (r3,r6,r12,r24,r48,r96)]
    v3,v6,v12,v24,v48,v96=vals
    v6=max(v3,v6); v12=max(v6,v12); v24=max(v12,v24); v48=max(v24,v48); v96=max(v48,v96)
    # Chuva observada exclusivamente dos pluviômetros.
    # Cada horizonte usa somente a janela observada que já existia naquele horizonte;
    # chuva de 48/96h não pode contaminar uma previsão de 6/12/24h.
    h=int(horizon)
    effective=v3*1.00+(v6-v3)*0.90
    if h>=12:
        effective+=(v12-v6)*0.65
    if h>=24:
        effective+=(v24-v12)*0.45
    if h>=48:
        effective+=(v48-v24)*0.25
    if h>=96:
        effective+=(v96-v48)*0.10
    return max(0,effective*(5.90/130.0)*0.30)

def weighted_rain(*args):
    # Compatibilidade com chamadas antigas.
    if len(args)==4:
        r6,r12,r24,r48=args
        return weighted_observed_rain(0,r6,r12,r24,r48,r48,horizon=24)
    return weighted_observed_rain(*args,horizon=24)

def forecast_rain_effect(future, horizon):
    if not isinstance(future,dict): return 0.0
    h=str(horizon)
    v=future.get(h)
    try: v=max(0,float(v or 0))
    except: return 0.0
    # Conversão conservadora: previsão futura só entra como fração da chuva de referência.
    factor={'6':0.08,'12':0.15,'24':0.25}[h]
    return v*(5.90/130.0)*factor

def dam_effect(dams, horizon):
    """Converte operação observada em um sinal conservador e com memória temporal.

    Não assume que número de comportas = vazão. Quando disponível, prioriza
    uma fração de vertimento/capacidade; a abertura atual e eventos recentes
    entram como sinais auxiliares. O ganho em metros é aprendido depois.
    """
    if not isinstance(dams,dict): return 0.0
    h=str(horizon)
    try: open_frac=clamp(float(dams.get('open_fraction',0) or 0),0,1)
    except: open_frac=0.0
    try: spill_active=1.0 if float(dams.get('spill_active',0) or 0)>0 else 0.0
    except: spill_active=0.0
    try: event=clamp(float(dams.get('event_opening',0) or 0),0,1)
    except: event=0.0
    try: recent_event=clamp(float(dams.get('recent_event_signal',0) or 0),0,1)
    except: recent_event=0.0
    # O estado atual recebe peso maior; evento recente decai com o tempo e evita
    # perder a memória hidráulica logo após uma alteração operacional.
    # Não divide vertido por capacidade: os campos podem ter unidades distintas.
    # Usamos somente sinais operacionais diretamente observados.
    signal=0.55*open_frac + 0.20*spill_active + 0.35*max(event,recent_event)
    factor={'6':0.10,'12':0.20,'24':0.32}[h]
    return clamp(signal,0,2.0)*factor

def trend_slope(history, now_ms):
    recent=[x for x in history if now_ms-x['t']<=6*3600*1000]
    if len(recent)<2:return 0.0,0
    first,last=recent[0],recent[-1]
    hours=max(.25,(last['t']-first['t'])/3600000)
    slope=(last['level']-first['level'])/hours
    return slope,len(recent)

def project(current, rainp, slope, horizon, params, future_rain=None, dams=None):
    h=str(horizon)
    raw_trend=slope*float(horizon)
    trend_component=clamp(raw_trend, -0.30 if h=='6' else (-0.45 if h=='12' else -0.60), 0.60 if h=='6' else (1.00 if h=='12' else 1.40))
    trend_component*=params['trend_gain'][h]*params['damping'][h]
    response_factor={'6':0.18,'12':0.38,'24':0.72}[h]
    # rainp pode ser um escalar legado; chamadas novas passam o valor específico do horizonte.
    rain_component=rainp*response_factor*params['rain_gain'][h]
    forecast_component=forecast_rain_effect(future_rain,h)*params['forecast_rain_gain'][h]
    dam_signal=dam_effect(dams,h)
    dam_component=dam_signal*params['dam_gain'][h]
    total=current+trend_component+rain_component+forecast_component+dam_component
    return max(0,total),trend_component,rain_component,forecast_component,dam_component
