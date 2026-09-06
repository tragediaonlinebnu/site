import json, math, os
from datetime import datetime, timezone

DEFAULT = {
  "version": 2,
  "trend_gain": {"6":1.0,"12":1.0,"24":1.0},
  "rain_gain": {"6":1.0,"12":1.0,"24":1.0},
  "damping": {"6":0.70,"12":0.55,"24":0.40},
  "metrics": {"evaluated":0,"mae_m":{"6":None,"12":None,"24":None},"last_update":None},
  "updated_at": None
}

def clamp(x,a,b): return max(a,min(b,x))
def finite(x): return isinstance(x,(int,float)) and math.isfinite(x)

def load_params(path):
    try:
        with open(path,encoding='utf-8') as f: d=json.load(f)
    except Exception: d={}
    out=json.loads(json.dumps(DEFAULT))
    for k in ('trend_gain','rain_gain','damping'):
        if isinstance(d.get(k),dict):
            for h in ('6','12','24'):
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

def weighted_rain(r6,r12,r24,r48):
    vals=[max(0,r6 or 0),max(0,r12 or 0),max(0,r24 or 0),max(0,r48 or 0)]
    v6,v12,v24,v48=vals
    v12=max(v6,v12); v24=max(v12,v24); v48=max(v24,v48)
    # Mantém a ideia de resposta temporal, mas reduz a escala histórica para evitar exageros.
    w=lambda h: 1.0 if 3<=h<=6 else (0.25 if h<3 else 0.45)
    effective=v6*w(3)+(v12-v6)*w(9)+(v24-v12)*w(18)+(v48-v24)*w(36)
    return max(0,effective*(5.90/130.0)*0.30)

def trend_slope(history, now_ms):
    recent=[x for x in history if now_ms-x['t']<=6*3600*1000]
    if len(recent)<2:return 0.0,0
    first,last=recent[0],recent[-1]
    hours=max(.25,(last['t']-first['t'])/3600000)
    slope=(last['level']-first['level'])/hours
    return slope,len(recent)

def project(current, rainp, slope, horizon, params):
    h=str(horizon)
    raw_trend=slope*float(horizon)
    trend_component=clamp(raw_trend, -0.30 if h=='6' else (-0.45 if h=='12' else -0.60), 0.60 if h=='6' else (1.00 if h=='12' else 1.40))
    trend_component*=params['trend_gain'][h]*params['damping'][h]
    response_factor={'6':0.18,'12':0.38,'24':0.72}[h]
    rain_component=rainp*response_factor*params['rain_gain'][h]
    return max(0,current+trend_component+rain_component),trend_component,rain_component
