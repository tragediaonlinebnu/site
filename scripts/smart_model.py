import json, math, os, statistics
from datetime import datetime, timezone

DEFAULT = {
  "version": 10,
  "trend_gain": {"2":1.0,"4":1.0,"6":1.0,"12":1.0,"24":1.0},
  "rain_gain": {"2":1.0,"4":1.0,"6":1.0,"12":1.0,"24":1.0},
  "forecast_rain_gain": {"2":0.0,"4":0.0,"6":0.25,"12":0.55,"24":0.75},
  "dam_gain": {"2":1.0,"4":1.0,"6":1.0,"12":1.0,"24":1.0},
  "damping": {"2":0.88,"4":0.78,"6":0.70,"12":0.55,"24":0.40},
  "metrics": {"evaluated":0,"mae_m":{"2":None,"4":None,"6":None,"12":None,"24":None},"rmse_m":{"2":None,"4":None,"6":None,"12":None,"24":None},"bias_m":{"2":None,"4":None,"6":None,"12":None,"24":None},"last_update":None},
  "updated_at": None
}
HORIZONS=('2','4','6','12','24')

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
    p['updated_at']=datetime.now(timezone.utc).isoformat(); p['metrics']['last_update']=p['updated_at']
    tmp=path+'.tmp'
    with open(tmp,'w',encoding='utf-8') as f: json.dump(p,f,ensure_ascii=False,indent=2)
    os.replace(tmp,path)

def weighted_observed_rain(r3,r6,r12,r24,r48=0,r96=0,horizon=24):
    vals=[max(0,float(x or 0)) for x in (r3,r6,r12,r24,r48,r96)]
    v3,v6,v12,v24,v48,v96=vals
    v6=max(v3,v6); v12=max(v6,v12); v24=max(v12,v24); v48=max(v24,v48); v96=max(v48,v96)
    h=int(horizon); effective=v3+(v6-v3)*.90
    if h>=12: effective+=(v12-v6)*.65
    if h>=24: effective+=(v24-v12)*.45
    if h>=48: effective+=(v48-v24)*.25
    if h>=96: effective+=(v96-v48)*.10
    # Unidade física: transforma chuva efetiva em uma expectativa de subida.
    # O fator é calibrado pelos eventos históricos quando existirem.
    return max(0,effective)

def trend_slope(history, now_ms):
    recent=[x for x in history if now_ms-x['t']<=6*3600*1000]
    if len(recent)<2:return 0.0,0
    first,last=recent[0],recent[-1]; hours=max(.25,(last['t']-first['t'])/3600000)
    return (last['level']-first['level'])/hours,len(recent)

def dam_effect(dams, horizon):
    if not isinstance(dams,dict): return 0.0
    h=str(horizon)
    try: spill=1.0 if float(dams.get('spill_active',0) or 0)>0 else 0.0
    except: spill=0.0
    try: event=clamp(float(dams.get('event_opening',0) or 0),0,1)
    except: event=0.0
    try: recent=clamp(float(dams.get('recent_event_signal',0) or 0),0,1)
    except: recent=0.0
    signal=.20*spill+.35*max(event,recent)
    return clamp(signal,0,1)*{'2':.02,'4':.04,'6':.07,'12':.13,'24':.20}[h]

def historical_response(events, rain48, horizon):
    """Analogia por eventos: retorna delta de nível esperado e confiança.
    Só usa eventos encerrados e dados reais. Nunca inventa histórico.
    """
    if not isinstance(events,list) or not finite(rain48) or rain48 <= 0:
        return 0.0, 0.0, 0, None
    rows=[]
    for e in events:
        if e.get('status')!='completed': continue
        r=float(e.get('rain_48h_at_peak_mm') or e.get('rain_48h_max_mm') or 0)
        rise=float(e.get('rise_m') or 0)
        if r < 20 or rise <= 0: continue
        tpeak=float(e.get('duration_to_peak_h') or 0)
        if tpeak <= 0: continue
        ratio=rise/r
        # Similaridade robusta por escala logarítmica.
        dist=abs(math.log((rain48+5)/(r+5)))
        weight=math.exp(-dist*3.0)
        # Estimativa de fração da subida até cada horizonte.
        frac=clamp(float(horizon)/tpeak,0,1)
        # Curva de resposta suave: subida mais lenta no começo.
        frac=frac**0.85
        rows.append((weight, rise*frac, r, tpeak, ratio, e))
    if not rows:return 0.0,0.0,0,None
    rows.sort(reverse=True,key=lambda x:x[0]); top=rows[:8]; sw=sum(x[0] for x in top)
    delta=sum(x[0]*x[1] for x in top)/sw
    mean_ratio=sum(x[0]*x[4] for x in top)/sw
    # Confiança aumenta com número e similaridade dos eventos, mas é limitada.
    similarity=sum(x[0] for x in top)/len(top)
    confidence=clamp((min(len(top),5)/5)*similarity,0,0.92)
    typical_peak_h=sum(x[0]*x[3] for x in top)/sw
    return delta,confidence,len(top),{'rise_per_100mm':mean_ratio*100,'peak_time_h':typical_peak_h}

def project(current, rainp, slope, horizon, params, future_rain=None, dams=None, events=None, rain48=None):
    h=str(horizon); hh=float(horizon)
    # Os horizontes podem chegar como strings (ex.: "2") vindas do JSON.
    # A sensibilidade padrão usa chaves inteiras, então normalizamos aqui.
    h_int=int(horizon)
    raw_trend=slope*hh
    trend_component=clamp(raw_trend, {'2':-.10,'4':-.20,'6':-.30,'12':-.45,'24':-.60}[h], {'2':.20,'4':.40,'6':.60,'12':1.00,'24':1.40}[h])
    trend_component*=params['trend_gain'][h]*params['damping'][h]

    # Antes de haver histórico suficiente, usamos uma sensibilidade conservadora.
    # Ela é muito maior que a versão anterior porque agora é aplicada em mm reais.
    default_mm_to_m={2:.0015,4:.0025,6:.0035,12:.0055,24:.0080}[h_int]
    rain_component=max(0,float(rainp or 0))*default_mm_to_m*params['rain_gain'][h]

    future_component=0.0
    if future_rain and finite(future_rain):
        future_component=max(0,float(future_rain))*default_mm_to_m*0.45*params['forecast_rain_gain'][h]

    dam_component=dam_effect(dams,h)*params['dam_gain'][h]
    analog_delta,confidence,n,analog_meta=historical_response(events,rain48,horizon)
    # Analogia histórica só domina quando há evidência; caso contrário não muda o modelo.
    analog_weight=confidence*(0.25 if h in ('2','4') else 0.45 if h in ('6','12') else 0.55)
    model_delta=trend_component+rain_component+future_component+dam_component
    total_delta=(1-analog_weight)*model_delta+analog_weight*analog_delta
    total=max(0,current+total_delta)
    return total,trend_component,rain_component,future_component,dam_component,analog_delta,confidence,analog_meta
