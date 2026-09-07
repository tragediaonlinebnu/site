import math
from datetime import datetime, timezone

HORIZONS = (2, 4, 6, 12, 24)
START_RAIN_MM_3H = 5.0
DRY_RAIN_MM_3H = 1.0
EVENT_END_HOURS = 18
RISE_THRESHOLD_M = 0.05
RISE_WINDOW_H = 3.0
LEVEL_MILESTONES = (4.0, 5.0, 6.0, 8.0)


def finite(v):
    return isinstance(v, (int, float)) and math.isfinite(v)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def iso(t_ms):
    return datetime.fromtimestamp(t_ms / 1000, timezone.utc).isoformat()


def rain_value(snapshot, key):
    try:
        v = snapshot.get('rain', {}).get(key)
        return float(v) if finite(v) else None
    except Exception:
        return None


def interpolate_level(history, t_ms):
    if not history:
        return None
    rows = sorted((x for x in history if finite(x.get('t')) and finite(x.get('level'))), key=lambda x: x['t'])
    if not rows:
        return None
    before = [x for x in rows if x['t'] <= t_ms]
    after = [x for x in rows if x['t'] >= t_ms]
    if before and after:
        a, b = before[-1], after[0]
        if a['t'] == b['t']:
            return a['level']
        frac = (t_ms - a['t']) / (b['t'] - a['t'])
        return a['level'] + (b['level'] - a['level']) * frac
    nearest = min(rows, key=lambda x: abs(x['t'] - t_ms))
    return nearest['level']


def sustained_rise(history, start_t, end_t=None):
    end_t = end_t or history[-1]['t']
    candidates = [x for x in history if start_t <= x['t'] <= end_t and finite(x.get('level'))]
    if len(candidates) < 2:
        return None
    base = candidates[0]
    for row in candidates[1:]:
        hours = (row['t'] - base['t']) / 3600000
        if hours >= RISE_WINDOW_H and row['level'] - base['level'] >= RISE_THRESHOLD_M:
            return base['t']
    return None


def threshold_crossing(history, threshold, start_t):
    rows = sorted((x for x in history if x['t'] >= start_t and finite(x.get('level'))), key=lambda x: x['t'])
    for x in rows:
        if x['level'] >= threshold:
            return x['t']
    return None


def complete_event(event, history, now_ms):
    if not event:
        return None
    start_t = int(event['start_t'])
    rows = [x for x in history if start_t <= x['t'] <= now_ms and finite(x.get('level'))]
    if not rows:
        return None
    peak = max(rows, key=lambda x: x['level'])
    peak_level = float(peak['level'])
    last = rows[-1]
    # Evento só termina quando o rio entrou em recessão e a chuva recente secou.
    dry_since = event.get('dry_since_t')
    ended = bool(dry_since and now_ms - int(dry_since) >= EVENT_END_HOURS * 3600000 and last['level'] <= peak_level - 0.05)
    if not ended:
        return None
    event['status'] = 'completed'
    event['end_t'] = int(last['t'])
    event['peak_t'] = int(peak['t'])
    event['peak_level_m'] = round(peak_level, 3)
    event['rise_m'] = round(peak_level - float(event.get('start_level_m', peak_level)), 3)
    event['duration_to_peak_h'] = round((peak['t'] - start_t) / 3600000, 2)
    event['duration_h'] = round((last['t'] - start_t) / 3600000, 2)
    event['response_onset_t'] = sustained_rise(history, start_t, peak['t'])
    if event['response_onset_t']:
        event['response_time_h'] = round((event['response_onset_t'] - start_t) / 3600000, 2)
    for h in (3, 6, 12, 24, 48, 96):
        event[f'rain_{h}h_at_peak_mm'] = event.get(f'rain_{h}h_at_peak_mm')
    for m in LEVEL_MILESTONES:
        t = threshold_crossing(history, m, start_t)
        event[f'to_{str(m).replace(".", "_")}_t'] = t
        event[f'to_{str(m).replace(".", "_")}_h'] = round((t - start_t) / 3600000, 2) if t else None
    if event.get('rain_48h_at_peak_mm'):
        event['rise_per_100mm_48h'] = round(event['rise_m'] / event['rain_48h_at_peak_mm'] * 100, 3)
    return event


def update_events(events, snapshots, river_history, current_rain, current_level, now_ms):
    events = [e for e in events if isinstance(e, dict)]
    snapshots = sorted([s for s in snapshots if isinstance(s, dict)], key=lambda s: int(s.get('t', 0)))
    active = next((e for e in reversed(events) if e.get('status') == 'active'), None)
    prev = snapshots[-2] if len(snapshots) >= 2 else None
    rain3 = current_rain.get('h003') if current_rain else None
    rain24 = current_rain.get('h024') if current_rain else None
    rain48 = current_rain.get('h048') if current_rain else None
    rain96 = current_rain.get('h096') if current_rain else None

    rain_start = False
    if finite(rain3):
        prev3 = rain_value(prev, 'h003') if prev else None
        rain_start = rain3 >= START_RAIN_MM_3H and (prev3 is None or rain3 > prev3 + 0.5)

    prev_level = None
    if river_history:
        prior_rows = [x for x in river_history if int(x.get('t', 0)) < now_ms and finite(x.get('level'))]
        if prior_rows: prev_level = prior_rows[-1]['level']
    cross_5 = finite(current_level) and current_level >= 5.0 and (prev_level is None or prev_level < 5.0)
    recent_completed = next((e for e in reversed(events) if e.get('status') == 'completed' and e.get('end_t') and now_ms-int(e['end_t']) < 24*3600000), None)
    if active is None and (rain_start or (cross_5 and recent_completed is None)):
        active = {
            'id': f"event-{now_ms}",
            'status': 'active',
            'start_t': now_ms,
            'start_level_m': round(float(current_level), 3) if finite(current_level) else None,
            'start_trigger': 'rain' if rain_start else 'river_level',
            'rain_3h_start_mm': round(rain3, 2) if finite(rain3) else None,
            'rain_24h_start_mm': round(rain24, 2) if finite(rain24) else None,
            'rain_48h_start_mm': round(rain48, 2) if finite(rain48) else None,
            'rain_96h_start_mm': round(rain96, 2) if finite(rain96) else None,
            'milestones': [],
            'version': 1
        }
        events.append(active)

    if active:
        for h in (3, 6, 12, 24, 48, 96):
            v = current_rain.get(f'h{h:03d}') if current_rain else None
            if finite(v):
                active[f'rain_{h}h_last_mm'] = round(v, 2)
                # Capture the maximum accumulated value observed during the event.
                key = f'rain_{h}h_max_mm'
                active[key] = round(max(float(v), float(active.get(key, 0))), 2)
        if finite(current_level):
            if active.get('peak_level_m') is None or current_level > active.get('peak_level_m', -1e9):
                active['peak_level_m'] = round(float(current_level), 3)
                active['peak_t'] = now_ms
                for h in (3, 6, 12, 24, 48, 96):
                    v = current_rain.get(f'h{h:03d}') if current_rain else None
                    if finite(v): active[f'rain_{h}h_at_peak_mm'] = round(v, 2)
            for m in LEVEL_MILESTONES:
                key = f'to_{str(m).replace(".", "_")}_t'
                if current_level >= m and not active.get(key):
                    active[key] = now_ms
                    active.setdefault('milestones', []).append({'level_m': m, 't': now_ms, 'hours': round((now_ms-active['start_t'])/3600000,2)})
        if finite(rain3) and rain3 <= DRY_RAIN_MM_3H:
            active.setdefault('dry_since_t', now_ms)
        elif finite(rain3) and rain3 > DRY_RAIN_MM_3H:
            active.pop('dry_since_t', None)
        completed = complete_event(active, river_history, now_ms)
        if completed:
            # Freeze peak rainfall fields from the actual peak timestamp when possible.
            for h in (3, 6, 12, 24, 48, 96):
                pt = completed.get('peak_t')
                if pt:
                    prior = min(snapshots, key=lambda s: abs(int(s.get('t', 0)) - int(pt))) if snapshots else None
                    v = rain_value(prior, f'h{h:03d}') if prior else None
                    if finite(v): completed[f'rain_{h}h_at_peak_mm'] = round(v, 2)
            completed['closed_at'] = iso(now_ms)
    # Keep one active event and completed history. Deduplicate by id.
    by_id = {str(e.get('id')): e for e in events if e.get('id')}
    return sorted(by_id.values(), key=lambda e: int(e.get('start_t', 0)))


def summarize_events(events):
    completed = [e for e in events if e.get('status') == 'completed' and finite(e.get('peak_level_m'))]
    active = next((e for e in reversed(events) if e.get('status') == 'active'), None)
    response = [e['response_time_h'] for e in completed if finite(e.get('response_time_h'))]
    coeff = [e['rise_per_100mm_48h'] for e in completed if finite(e.get('rise_per_100mm_48h')) and e.get('rain_48h_at_peak_mm', 0) > 20]
    return {
        'completed_events': len(completed),
        'active_event': active.get('id') if active else None,
        'median_response_h': round(sorted(response)[len(response)//2], 2) if response else None,
        'median_rise_per_100mm_48h': round(sorted(coeff)[len(coeff)//2], 3) if coeff else None,
        'highest_peak_m': round(max(e['peak_level_m'] for e in completed), 3) if completed else None,
        'last_event_id': completed[-1].get('id') if completed else None,
    }
