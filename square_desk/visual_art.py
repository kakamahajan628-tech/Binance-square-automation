"""Bundled editorial artwork; no generation API or database image bytes."""
from collections import Counter
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib,io,json,math,re,textwrap
from PIL import Image,ImageDraw
from .models import digest
from .visual_design import FONT_PACK

VERSION='editorial-art-v32-1'
MIX_VERSION='mixed-visuals-v32-1'
ROOT=Path(__file__).with_name('visual_assets')
CONFIRMED={'published','paper_published'}
COUNTED=CONFIRMED|{'approved','queued','chart_rendering','uploading_image','preparing_media','sending','uncertain'}

@lru_cache(maxsize=1)
def catalog():
    value=json.loads((ROOT/'catalog.json').read_text(encoding='utf-8'))
    if value.get('version')!=VERSION:raise ValueError('Artwork catalog version unavailable')
    return tuple(value['assets'])

def source(asset_id,expected):
    if not isinstance(asset_id,str) or not re.fullmatch(r'[a-z][a-z0-9-]{1,40}',asset_id):
        raise ValueError('Artwork identifier invalid')
    path=ROOT/(asset_id+'.png')
    if path.is_symlink() or path.resolve().parent!=ROOT.resolve() or not path.is_file() or path.stat().st_size>250000:
        raise ValueError('Bundled artwork unavailable')
    data=path.read_bytes()
    if hashlib.sha256(data).hexdigest()!=expected:raise ValueError('Bundled artwork changed')
    return data

def day_rows(store,day,timezone):
    start=datetime.fromisoformat(day).replace(tzinfo=ZoneInfo(timezone)).timestamp()
    result=[]
    for row in store.list('draft',list(COUNTED),since=start-2*86400,limit=1000):
        p=row['payload']
        if p.get('source')=='telegram_manual' or p.get('campaign_id'):continue
        mix=p.get('visual_mix') or {}
        if row['status'] in CONFIRMED or row['status'] in ('sending','uncertain','preparing_media'):
            at=p.get('submitted_at',row['updated'])
            if not start<=at<start+86400:continue
        elif not ((mix.get('day')==day or start<=row['created']<start+86400) and (p.get('image') or mix)):
            continue
        result.append(row)
    return result

def statistics(store,day,timezone):
    rows=day_rows(store,day,timezone)
    confirmed=[r for r in rows if r['status'] in CONFIRMED]
    is_art=lambda r:r['payload'].get('visual_mix',{}).get('kind')=='illustration'
    return {'day':day,'confirmed_posts':len(confirmed),'confirmed_illustrations':sum(map(is_art,confirmed)),
        'counted_posts_and_pending':len(rows),'counted_illustrations_and_pending':sum(map(is_art,rows))}

def art_slot(ordinal,percent,day):
    # Alternate the half-slot phase by day: a complete 50-slot day gets 17 or 18
    # illustration opportunities at 35%, without a catch-up burst after rollout.
    phase=(datetime.fromisoformat(day).date().toordinal()%2)*50
    return ((ordinal+1)*percent+phase)//100 > (ordinal*percent+phase)//100

def target_count(total,percent,day):
    phase=(datetime.fromisoformat(day).date().toordinal()%2)*50
    return (total*percent+phase)//100

def choose(settings,store,draft,ident,day,chart_context):
    percent=settings.illustration_share_percent
    if not percent:return chart_context
    # Approved missing files recover via image_recipe instead of selecting again.
    rows=[r for r in day_rows(store,day,settings.timezone) if r['id']!=ident]
    ordinal=len(rows);concept=draft.get('editorial_concept') or {}
    marker={'version':MIX_VERSION,'day':day,'kind':'chart','slot':ordinal,'target_percent':percent}
    draft['visual_mix']=marker
    event=draft.get('event') or {}
    used=sum(r['payload'].get('visual_mix',{}).get('kind')=='illustration' for r in rows)
    recent=max(rows,key=lambda r:r['payload'].get('submitted_at',r['updated']),default=None)
    need=used<target_count(ordinal+1,percent,day)
    if percent<=50 and recent and recent['payload'].get('visual_mix',{}).get('kind')=='illustration':need=False
    if (draft.get('source')=='telegram_manual' or draft.get('campaign_id')
        or draft.get('risk')=='high' or event.get('category') in ('setup','shock')
        or not concept or not need):
        return chart_context
    title=draft.get('title','');topic=concept.get('topic','');body=draft.get('body','')[:1400]
    suffix=draft.get('presentation_suffix')
    if suffix and body.endswith(suffix):body=body[:-len(suffix)]
    candidates=[]
    for asset in catalog():
        if concept.get('family') not in asset['families']:continue
        pattern=asset['pattern']
        score=sum(weight*len(re.findall(pattern,text,re.I)) for text,weight in ((topic,5),(title,3),(body,1)))
        if score<4:continue
        try:source(asset['id'],asset['sha256'])
        except (OSError,ValueError):continue
        candidates.append((score,asset))
    if not candidates:return chart_context
    top=max(score for score,_ in candidates)
    candidates=[a for score,a in candidates if score>=max(4,top*.6)]
    counts=Counter(r['payload'].get('visual_mix',{}).get('asset_id') for r in rows)
    latest=next((r['payload'].get('visual_mix',{}).get('asset_id') for r in rows
        if r['payload'].get('visual_mix',{}).get('asset_id')),None)
    if len(candidates)>1:candidates=[a for a in candidates if a['id']!=latest] or candidates
    seed=digest([day,event.get('event_key'),concept.get('id'),ordinal])
    asset=min(candidates,key=lambda a:(counts[a['id']],digest([seed,a['id']])))
    marker.update(kind='illustration',asset_id=asset['id'])
    return {'renderer':VERSION,'asset_id':asset['id'],'asset_sha256':asset['sha256'],
        'title':title,'symbol':event.get('symbol',''),'concept_id':concept['id'],
        'meaning':asset['meaning'],'caption':draft.get('body','').split('\n\n',1)[0][:800],
        'layout':int(seed[:2],16)%3,'font_pack':FONT_PACK}


def render(snapshot,metrics,directory,*,context):
    from PIL import ImageOps
    from .visual_design import Canvas,save
    if context.get('renderer')!=VERSION or context.get('font_pack')!=FONT_PACK:
        raise ValueError('Artwork resources unavailable')
    if type(context.get('layout',0)) is not int or context.get('layout',0) not in (0,1,2):
        raise ValueError('Artwork layout invalid')
    data=source(context.get('asset_id'),context.get('asset_sha256'))
    symbol=context.get('symbol','')
    if symbol!=snapshot.symbol or not re.fullmatch(r'[A-Z0-9]{2,12}',symbol):
        raise ValueError('Artwork asset mismatch')
    c=Canvas();c.image.paste('#F6F1E7',(0,0,1440,900));c.ink='#18323C';c.muted='#5B7179';c.panel='#E8E6DC';c.line='#CED8D2'
    layout=context.get('layout',0);right=layout==1
    image_box=(780,65,1380,790) if right else (45,65,645,790)
    tx=55 if right else 710;tw=660
    if layout==2:image_box=(70,240,560,795);tx=640;tw=730
    with Image.open(io.BytesIO(data)) as original:
        if original.format!='PNG' or original.width>800 or original.height>900:
            raise ValueError('Bundled artwork format invalid')
        x0,y0,x1,y1=image_box
        art=ImageOps.contain(original.convert('RGB'),(x1-x0,y1-y0),Image.Resampling.LANCZOS)
        c.image.paste(art,(x0+(x1-x0-art.width)//2,y0+(y1-y0-art.height)//2));art.close()
    c.d=ImageDraw.Draw(c.image)
    c.text(45 if layout==2 else tx,50 if layout==2 else 65,'$'+symbol,32,'#267E7B','bold')
    title=str(context.get('title','')).replace('$'+symbol+':','').strip()
    end=c.wrap(45 if layout==2 else tx,108 if layout==2 else 130,title,1330 if layout==2 else tw,45,2 if layout==2 else 5,role='bold')
    caption=context.get('caption') or context.get('meaning','A concept to examine')
    if caption.strip().casefold()==title.strip().casefold():caption='A question about process, evidence and uncertainty.'
    c.wrap(tx,320 if layout==2 else max(390,end+22),caption,tw,27,4 if layout==2 else 3,color=c.muted)
    c.pill(tx,570,310,'Recorded close',f"{metrics['price']:.6g}",'#267E7B')
    rv=metrics.get('relative_volume')
    c.pill(tx+330,570,310,'Relative volume',f'{rv:.6g}x' if type(rv) in (float,int) else 'N/A','#267E7B')
    c.wrap(tx,680,'A visual analogy, not a statement about traders, an outcome or a live market chart.',tw,24,3,color=c.muted)
    c.text(45,840,'Concept illustration · '+snapshot.source+' spot observation · '+snapshot.quote,22,c.muted)
    return save(c.image,directory)
