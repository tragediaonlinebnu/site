#!/usr/bin/env python3
import io, json, os, re
from datetime import datetime, timezone
from pathlib import Path

import requests
from PIL import Image, ImageOps, ImageFilter
import pytesseract

IMAGE_URL = os.environ.get('TIDE_IMAGE_URL', 'https://defesacivil.itajai.sc.gov.br/wp-content/uploads/2026/08/MARE-9.jpg')
OUT = Path('data/mare_itajai.json')

def normalize(txt):
    txt = txt.replace('O','0').replace('o','0').replace('I','1').replace('|','1')
    txt = txt.replace('—','-').replace('–','-')
    return txt

def parse_column(img, left, right):
    crop = img.crop((left, 85, right, img.height-90))
    scale = 3
    crop = crop.resize((crop.width*scale, crop.height*scale))
    crop = ImageOps.grayscale(crop)
    crop = ImageOps.autocontrast(crop)
    crop = crop.filter(ImageFilter.SHARPEN)
    text = pytesseract.image_to_string(crop, config='--psm 6', lang='eng')
    text = normalize(text)
    events=[]
    current_date=None
    for raw in text.splitlines():
        line=' '.join(raw.split())
        if not line: continue
        dm=re.search(r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})', line)
        if dm:
            current_date=f'{int(dm.group(3)):04d}-{int(dm.group(2)):02d}-{int(dm.group(1)):02d}'
            line=line[dm.end():].strip()
        # Capture one or more time/level pairs. OCR generally yields one per row.
        m=re.search(r'(\d{1,2})[:h](\d{2})\s+([01]?\d[.,]\d{2})', line)
        if m and current_date:
            hh=int(m.group(1)); mm=int(m.group(2)); level=float(m.group(3).replace(',','.'))
            if 0<=hh<=23 and 0<=mm<=59 and 0<=level<=3:
                events.append({'date':current_date,'time':f'{hh:02d}:{mm:02d}','level_m':round(level,2)})
    return events

def main():
    r=requests.get(IMAGE_URL,timeout=30,headers={'User-Agent':'Mozilla/5.0'})
    r.raise_for_status()
    img=Image.open(io.BytesIO(r.content)).convert('RGB')
    w,h=img.size
    # The official image is a three-column table. Crop below the header and above the footer.
    cols=[(0,w//3),(w//3,2*w//3),(2*w//3,w)]
    events=[]
    for a,b in cols: events.extend(parse_column(img,a,b))
    # De-duplicate and sort; OCR can repeat a row.
    uniq={(e['date'],e['time']):e for e in events}
    events=sorted(uniq.values(),key=lambda e:(e['date'],e['time']))
    if len(events)<20:
        raise RuntimeError(f'OCR produced only {len(events)} tide events; refusing to publish bad data')
    month_match=re.search(r'MARE-(\d+)', IMAGE_URL, re.I)
    payload={
      'source_image': IMAGE_URL,
      'updated_at': datetime.now(timezone.utc).isoformat(),
      'events': events,
      'parser': 'github-actions-pytesseract'
    }
    OUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Published {len(events)} tide events to {OUT}')

if __name__=='__main__': main()
