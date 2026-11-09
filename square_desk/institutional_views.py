"""Versioned, deterministic candle graphics. No database, network, time or randomness."""
from datetime import datetime,timezone
from pathlib import Path
from statistics import mean,pstdev
import hashlib,io,math,textwrap,re
from PIL import Image,ImageDraw
from .analysis import ema
from .models import digest,stamp

LEGACY_VERSION='institutional-v27-1'
VERSION='institutional-v29-clean-1'
PALETTES=(
 ('#0B0F17','#131B2E','#1E293B','#10B981','#F43F5E','#F59E0B','#38BDF8'),
 ('#101827','#182235','#263548','#25CA98','#F06B85','#EAB75D','#86A8EF'),
 ('#0D1117','#161B22','#21262D','#2EA043','#F85149','#D29922','#A371F7'),
 ('#07111E','#0F1E32','#1B314E','#06D6A0','#EF476F','#FFD166','#118AB2'),
 ('#081412','#102420','#1C3832','#34D399','#FB7185','#FBBF24','#60A5FA'),
 ('#121316','#1C1E24','#2A2D36','#4ADE80','#F87171','#FACC15','#C084FC'))
NAMES={1:'Candle-distributed volume profile',2:'Close-location volume pressure proxy',
 3:'Prior-range wick rejection map',4:'Rolling VWAP and dispersion',5:'Effort versus candle movement',
 6:'Conditional setup R ladder',7:'Recorded return horizons',8:'Volatility compression context',
 9:'Moving-average ribbon',10:'Range quartiles and close occupancy',11:'Indexed peer curves',
 12:'Watchlist return and participation',13:'Saved peer momentum comparison',14:'UTC hour buckets',
 15:'Candle wick asymmetry',16:'Behavioural research diagnostic',17:'Heuristic factor gauges',
 18:'Three observable candle clues',19:'Research evidence checklist',20:'Historical ATR buffers',
 21:'Two conditional paths',22:'Five-factor heuristic radar',23:'Empirical candle-return distribution',
 24:'Last four candles under the lens',25:'Recorded paper tracking ledger'}
NOTES={1:'Volume is distributed evenly across each candle range; this is not traded-at-price data.',
 2:'CLV x volume is a candle proxy, not measured CVD, taker flow or buying pressure.',
 3:'Wicks beyond a prior range do not prove stops, trapped traders or liquidations.',
 4:'VWAP uses typical price x volume. Bands show weighted price dispersion, not forecasts.',
 5:'Activity versus movement does not establish absorption or participant identity.',
 6:'Levels are conditional research references; no fills, success rate or returns are promised.',
 7:'Only recorded horizons appear. Missing history stays unavailable.',
 8:'Width and ATR describe historical variation; compression need not resolve in a direction.',
 9:'EMA 12/20/26/50 are lagging transformations of the same closing prices.',
 10:'Quartiles and occupancy describe this sample, not actual supply or demand.',
 11:'Saved peer closes share exact candle times. Indexed paths do not establish causation.',
 12:'Fresh watchlist observations only, not the global market or proof of capital rotation.',
 13:'Same-venue saved RSI values are not a correlation or capital-rotation measurement.',
 14:'Fixed UTC buckets are descriptive; no session liquidity or causal claim is made.',
 15:'Wick proportions describe OHLC geometry, not derivatives positions.',
 16:'A research self-check cannot measure traders\' actual emotions or motives.',
 17:'Normalized factor readings are heuristics, not FOMO probabilities or trading signals.',
 18:'Pins mark volume, price extremum and last close; they do not explain crowd behaviour.',
 19:'An evidence checklist is not a safety rating or a recommendation.',
 20:'Historical crossings of current reference levels do not prove future stop survival.',
 21:'Dashed paths are illustrations, not forecasts, probabilities or future candles.',
 22:'Normalized scores are disclosed heuristics, not calibrated probabilities.',
 23:'Observed open-to-close returns only; no Gaussian distribution is assumed.',
 24:'Closed-candle geometry does not reveal the path of trades inside each bar.',
 25:'Paper tracking includes losses. R is simulated full-position research, not account returns.'}
FAMILY_ENGINES={1:(16,17,18,24),2:(3,5,8,20,23),3:(16,18,21,23),4:(17,19,22),
 5:(6,19,20,22),6:(16,18,24),7:(1,2,3,4,5,9,10,14,15,24),
 8:(7,11,12,13,22),9:(5,16,18,24),10:(19,21,22,25)}
VIEW_ENGINES={'value':(1,2,4,5),'volatility':(8,20,23),'trend':(9,21),
              'momentum':(7,17,22),'structure':(3,10,18,24)}
CONTENT_TERMS=((r'\bvwap\b',(4,)),(r'\b(?:ema|moving.average)\b',(9,)),
    (r'\b(?:bollinger|compression)\b',(8,)),(r'\b(?:volume|turnover)\b',(1,2,5)),
    (r'\b(?:wick|doji|candle body)\b',(3,15,24)),(r'\b(?:stop|invalidation distance)\b',(6,20)),
    (r'\b(?:rsi|momentum)\b',(13,17)),(r'\b(?:histogram|return distribution|tail.event)\b',(23,)),
    (r'\b(?:paper tracking|expectancy|drawdown)\b',(25,)),
    (r'\b(?:correlation|peer|cross.asset|watchlist comparison)\b',(11,12,13)))

def finite(value):return type(value) in (int,float) and math.isfinite(value)
def number(v):return f'{v:.6g}' if finite(v) else 'N/A'
def clip(v):return max(0,min(100,v))

def eligible(engine,metrics,context):
    # These require additional verified saved datasets, not invented placeholders.
    if engine==11:return len(context.get('peer_curves',[]))==2
    if engine==12:return len(context.get('watchlist',[]))>=3
    if engine==25:return bool(context.get('ledger',{}).get('verified'))
    if engine==6:return all(finite(metrics.get(k)) for k in ('entry','stop','target1','target2')) and metrics['entry']!=metrics['stop']
    if engine==13:return len(context.get('peers',[]))>=2
    return engine in NAMES

def select_engine(draft,context):
    c=draft['editorial_concept'];event=draft['event'];m=event['metrics']
    pool=list(dict.fromkeys(FAMILY_ENGINES[c['family']]+VIEW_ENGINES[c['view']]))
    scores={}
    body=draft.get('body','')
    suffix=draft.get('presentation_suffix')
    if suffix and body.endswith(suffix):body=body[:-len(suffix)]
    for pattern,engines in CONTENT_TERMS:
        score=sum(weight*len(re.findall(pattern,text,re.I)) for text,weight in
            ((draft.get('title',''),5),(c['topic'],3),(body,1)))
        if score:
            for engine in engines:
                if engine not in pool:pool.append(engine)
                scores[engine]=scores.get(engine,0)+score
    pool=[i for i in pool if eligible(i,m,context)]
    best=max((scores.get(i,0) for i in pool),default=0)
    if best:pool=[i for i in pool if scores.get(i,0)==best]
    seed=int(digest([event,c['id'],c['day']])[:12],16)
    context['engine']=pool[seed%len(pool)]
    return context

def context_for(draft):
    c=draft['editorial_concept'];event=draft['event'];m=event['metrics']
    peers=[]
    from .content_mix import valid_provenance
    if valid_provenance(draft):
        for p in draft['internal_evidence'].get('related',[])[:2]:
            if (p.get('source')==m.get('source') and p.get('quote')==m.get('quote')
                and finite(p.get('as_of')) and abs(p['as_of']-event['as_of'])<=900
                and finite(p.get('metrics',{}).get('rsi'))):
                peers.append({'symbol':p['symbol'],'rsi':p['metrics']['rsi'],'as_of':p['as_of']})
    context={'renderer':VERSION,'concept_id':c['id'],'family':c['family'],
        'topic':c['topic'],'palette':c['palette'],'changes':event.get('changes',{}),'peers':peers}
    context['chart_view']=c['view']
    return select_engine(draft,context)

def enrich(store,draft,context,settings):
    """Bounded reads of existing observations only; never fetch new market data."""
    c=draft['editorial_concept'];event=draft['event'];m=event['metrics'];as_of=event['as_of']
    if c['family']==8:
        from .chart_restore import snapshot_row
        from .models import Snapshot
        from .content_mix import valid_provenance
        if valid_provenance(draft):
            primary=snapshot_row(store,digest([m['source'],event['symbol'],900,as_of]))
            if primary:
                ends=[x['end'] for x in primary['payload']['candles'][-48:]];curves=[]
                for peer in draft['internal_evidence'].get('related',[])[:2]:
                    row=snapshot_row(store,digest([m['source'],peer['symbol'],900,as_of]))
                    if not row:continue
                    try:
                        snap=Snapshot.read(row['payload'])
                        snap.validate(as_of+1,settings.candle_max_age)
                    except (KeyError,ValueError,TypeError):continue
                    if snap.source!=m['source'] or snap.quote!=m['quote'] or snap.interval!=900:continue
                    if [x.end for x in snap.candles[-48:]]!=ends:continue
                    closes=[x.close for x in snap.candles[-48:]]
                    curves.append({'symbol':snap.symbol,'indexed':[x/closes[0]*100 for x in closes],
                        'snapshot_digest':digest(row['payload'])})
                if len(curves)==2:context['peer_curves']=curves
        watch=[]
        for row in store.state('latest_market',[])[:100]:
            metric=row.get('metrics',{});change=row.get('changes',{}).get('24h');rv=metric.get('relative_volume')
            if (metric.get('source')==m['source'] and metric.get('quote')==m['quote']
                    and finite(metric.get('as_of')) and 0<=as_of-metric['as_of']<=900
                    and finite(change) and finite(rv)):
                watch.append({'symbol':row['symbol'],'change':change,'relative_volume':rv})
        if len(watch)>=3:context['watchlist']=watch[:25]
    if c['family']==10:
        from .tracking import statistics
        rows=store.list('signal',since=as_of-7*86400,limit=1001)
        # Do not silently present a truncated history as a complete ledger.
        if 0<len(rows)<=1000:
            closed=[r for r in rows if finite(r['payload'].get('result_r'))]
            if closed:
                stats=statistics(rows);total=0;curve=[]
                for row in sorted(closed,key=lambda r:r['payload'].get('closed_at',r['updated'])):
                    total+=row['payload']['result_r'];curve.append(total)
                step=max(1,math.ceil(len(curve)/120));sample=curve[::step]
                if sample[-1]!=curve[-1]:sample.append(curve[-1])
                context['ledger']={'verified':True,'period_days':7,'generated':stats['generated'],
                    'closed':stats['closed'],'stopped':stats['stopped'],'target1':stats['target1'],
                    'expectancy_r':stats['expectancy_r'],'max_drawdown_r':stats['max_drawdown_r'],
                    'curve':sample,'history_digest':digest([(r['id'],r['payload']) for r in rows])}
    return select_engine(draft,context)

def calculate(snapshot,m,context):
    cs=snapshot.candles;cl=[c.close for c in cs];n=len(cs);last=cs[-1]
    atr=m.get('atr') or 0;rv=m.get('relative_volume')
    prices=[(c.high+c.low+c.close)/3 for c in cs[-96:]];vol=[c.volume for c in cs[-96:]]
    total=sum(vol);vwap=sum(p*v for p,v in zip(prices,vol))/total if total else None
    sigma=math.sqrt(sum(v*(p-vwap)**2 for p,v in zip(prices,vol))/total) if total else None
    profile_cs=cs[-120:];lo=min(c.low for c in profile_cs);hi=max(c.high for c in profile_cs)
    step=(hi-lo)/24 if hi>lo else max(hi*.00001,1e-12);profile=[0.0]*24
    for c in profile_cs:
        start=max(0,min(23,int((c.low-lo)/step)));end=max(0,min(23,int((c.high-lo)/step)))
        for i in range(start,end+1):profile[i]+=c.volume/(end-start+1)
    pressure=[];value=0
    for c in cs:
        value+=((2*c.close-c.high-c.low)/(c.high-c.low) if c.high>c.low else 0)*c.volume
        pressure.append(value)
    rejections=[]
    for i in range(max(20,n-70),n):
        c=cs[i];prior=cs[i-20:i];h=max(x.high for x in prior);l=min(x.low for x in prior)
        if c.high>h and c.close<=h:rejections.append((i,'upper'))
        if c.low<l and c.close>=l:rejections.append((i,'lower'))
    width=[]
    for i in range(19,n):
        window=cl[i-19:i+1];avg=mean(window);width.append(4*pstdev(window)/avg*100 if avg else 0)
    returns=[(c.close/c.open-1)*100 for c in cs]
    percentile=sum(r<=returns[-1] for r in returns)/len(returns)*100
    support=m.get('support',min(c.low for c in cs[-20:]));resistance=m.get('resistance',max(c.high for c in cs[-20:]))
    span=resistance-support
    occupancy=[sum(min(3,max(0,int((v-support)/span*4)))==i for v in cl[-70:]) for i in range(4)] if span>0 else [len(cl[-70:]),0,0,0]
    buckets=[]
    for hour in range(0,24,6):
        sample=[c for c in cs if hour<=datetime.fromtimestamp(c.start,timezone.utc).hour<hour+6]
        buckets.append((f'{hour:02d}-{hour+6:02d} UTC',sum(c.volume for c in sample),len(sample)))
    ext=abs(last.close-vwap)/atr if vwap is not None and atr>0 else None
    gauges=[m.get('rsi'),clip(rv/4*100) if finite(rv) else None,clip(ext/3*100) if ext is not None else None]
    scores=[clip(50+(ema(cl,20)[-1]-ema(cl,50)[-1])/atr*20) if atr>0 else None,
        clip(100-ext/3*100) if ext is not None else None,
        clip(100-abs((m.get('rsi') or 50)-50)*2),
        100*sum(w>=width[-1] for w in width)/len(width),gauges[1]]
    return dict(profile=profile,profile_low=lo,profile_step=step,pressure=pressure,rejections=rejections,
        vwap=vwap,sigma=sigma,width=width,returns=returns,percentile=percentile,occupancy=occupancy,
        buckets=buckets,extension=ext,gauges=gauges,scores=scores)

def save(image,directory):
    # Version-specific compression; legacy approved images keep their old byte path.
    image=image.quantize(colors=64,method=Image.Quantize.FASTOCTREE)
    out=io.BytesIO();image.save(out,format='PNG',optimize=True,compress_level=9)
    data=out.getvalue()
    if len(data)>1_000_000:raise ValueError('Chart exceeds image byte limit')
    path=Path(directory)/(hashlib.sha256(data).hexdigest()+'.png');path.write_bytes(data)
    return path.name

def render(snapshot,m,directory,*,context,font):
    if context.get('renderer') not in (LEGACY_VERSION,VERSION):raise ValueError('Unknown institutional renderer')
    clean=context['renderer']==VERSION
    engine=context['engine'];palette=context['palette']
    if type(palette) is not int or not 1<=palette<=6 or not eligible(engine,m,context):raise ValueError('Unsupported evidence for image engine')
    snapshot.validate(snapshot.as_of+1,1200)
    a=calculate(snapshot,m,context);cs=snapshot.candles;close=[c.close for c in cs]
    bg,panel,grid,up,down,accent,secondary=PALETTES[palette-1];white='#F1F5F9';muted='#CBD5E1'
    image=Image.new('RGB',(1600,1000),bg);d=ImageDraw.Draw(image)
    def text(x,y,value,size=23,color=white):d.text((x,y),str(value),font=font(size),fill=color)
    def wrap(x,y,value,width=40,size=22,lines=3,color=muted):
        for i,line in enumerate(textwrap.wrap(str(value),width)[:lines]):text(x,y+i*(size+7),line,size,color)
    def card(box):d.rounded_rectangle(box,radius=12,fill=panel)
    def line(points,color=secondary,width=3):
        if len(points)>1:d.line(points,fill=color,width=width)
    def bars(values,x,y,w,h,labels=None):
        maximum=max([abs(v) for v in values if finite(v)]+[1e-12]);step=h/max(1,len(values))
        for i,v in enumerate(values):
            if not finite(v):continue
            length=abs(v)/maximum*(w-145)
            d.rectangle((x+140,y+i*step+7,x+141+length,y+(i+1)*step-7),fill=up if v>=0 else down)
            text(x,y+i*step+5,labels[i] if labels else str(i+1),19)
            text(x+145+length,y+i*step+5,number(v),18,muted)
    text(50,28,f'${snapshot.symbol}/{snapshot.quote}  |  {snapshot.interval//60}m CLOSED CANDLES',27,accent)
    if not clean:text(1210,31,f'RESEARCH  |  IMG-{engine:02d}',22,muted)
    text(50,78,NAMES[engine],36)
    wrap(50,128,context.get('topic','Conditional market research'),95,23,1)
    telemetry='   |   '.join(k+': '+number(v) for k,v in (('Close',m.get('price')),('VWAP',a['vwap']),('ATR',m.get('atr')),('RelVol',m.get('relative_volume')),('RSI',m.get('rsi'))))
    text(50,178,telemetry,23)
    card((40,226,1020,695));card((1050,226,1560,867));card((40,722,1020,867))
    # Genuine OHLC, fixed 70-bar window. All transformations use these saved candles.
    visible=cs[-70:];start=len(cs)-len(visible);x0,y0,x1,y1=65,264,925,640
    lower=min(c.low for c in visible);upper=max(c.high for c in visible)
    if engine==9:
        for period in (12,20,26,50):
            values=ema(close,period)[start:];lower=min(lower,min(values));upper=max(upper,max(values))
    if engine in (4,):
        if a['vwap'] is not None:lower=min(lower,a['vwap']-2*a['sigma']);upper=max(upper,a['vwap']+2*a['sigma'])
    if engine==6:lower=min(lower,m['stop'],m['target1'],m['target2']);upper=max(upper,m['stop'],m['target1'],m['target2'])
    if engine==20 and finite(m.get('atr')):lower=min(lower,m['support']-1.5*m['atr'])
    if engine==21:lower=min(lower,m['support']-1.5*m['atr']);upper=max(upper,m['resistance'])
    spread=max(upper-lower,abs(upper)*.00001,1e-12);lower-=spread*.08;upper+=spread*.08
    def py(v):return y1-(v-lower)/(upper-lower)*(y1-y0)
    candle_width=(x1-x0)/len(visible)
    def px(i):return x0+(i-start+.5)*candle_width
    for i in range(5):
        y=y0+i*(y1-y0)/4;d.line((x0,y,x1,y),fill=grid);text(936,y-10,number(upper-i*(upper-lower)/4),16,muted)
    for i,c in enumerate(cs[start:],start):
        x=px(i);color=up if c.close>=c.open else down
        d.line((x,py(c.high),x,py(c.low)),fill=color,width=2)
        d.rectangle((x-3,min(py(c.open),py(c.close)),x+3,max(py(c.open),py(c.close))+1),fill=color)
    text(x0,656,'Closed history  |  '+stamp(visible[0].end)[:16]+' UTC',18,muted)
    if not clean:text(1072,247,f'CONCEPT {context.get("concept_id",0):03d}',20,accent)
    def ref(v,label,color=accent):
        if finite(v):d.line((x0,py(v),x1,py(v)),fill=color,width=2)
    if engine==9:
        for period,color in ((12,up),(20,accent),(26,secondary),(50,down)):
            line([(px(i),py(v)) for i,v in enumerate(ema(close,period)) if i>=start],color)
        wrap(1072,295,'EMA 12 / 20 / 26 / 50',36,25,2)
        wrap(1072,380,'Distance to EMA50 / ATR: '+number((close[-1]-ema(close,50)[-1])/m['atr'] if m['atr']>0 else None),33,24,3)
    elif engine==4:
        if a['vwap'] is not None:
            for factor in (-2,-1,0,1,2):ref(a['vwap']+factor*a['sigma'],str(factor),accent if factor==0 else secondary)
        wrap(1072,297,'Rolling VWAP: '+number(a['vwap'])+'\nWeighted sigma: '+number(a['sigma']),34,25,4)
        wrap(1072,450,'Lines: VWAP, +/- one and two sigma',31,23,3)
    elif engine==1:
        maximum=max(a['profile']) or 1
        for i,v in enumerate(a['profile']):
            y=780-i*18;d.rectangle((1090,y,1090+v/maximum*380,y+11),fill=accent if v==maximum else secondary)
        text(1072,289,'24 price bins | 120 bars',23)
    elif engine==2:
        values=a['pressure'][-70:];lo=min(values);span=max(values)-lo or 1
        line([(1075+i*6.2,725-(v-lo)/span*350) for i,v in enumerate(values)],accent,4)
        wrap(1072,294,'Cumulative CLV x volume; zero is an arbitrary sample origin.',33,24,3)
    elif engine==3:
        for i,kind in a['rejections']:
            if i>=start:text(px(i)-4,py(cs[i].high if kind=='upper' else cs[i].low)-15,'x',22,accent)
        wrap(1072,300,'Prior 20-bar range rejection observations: '+str(len(a['rejections'])),34,26,3)
    elif engine==5:
        wrap(1072,300,'Relative volume: '+number(m.get('relative_volume'))+'x',30,28,2)
        wrap(1072,412,'Last body / ATR: '+number(abs(cs[-1].close-cs[-1].open)/m['atr'] if m['atr']>0 else None),30,28,2)
        wrap(1072,540,'High effort + low displacement is a question, not proof of absorption.',32,24,4)
    elif engine==6:
        risk=abs(m['entry']-m['stop'])
        for k,color in (('entry',accent),('stop',down),('target1',up),('target2',secondary)):ref(m[k],k,color)
        for i,k in enumerate(('entry','stop','target1','target2')):text(1072,310+i*85,k+': '+number(m[k]),25)
        text(1072,697,'Risk distance: '+number(risk),23,accent)
    elif engine==7:
        labels=['15m','1h','4h','12h','24h','3d','7d'];values=[context.get('changes',{}).get(k) for k in labels]
        for i,(label,value) in enumerate(zip(labels,values)):text(1072,300+i*65,label+': '+number(value)+(' %' if finite(value) else ''),26,up if finite(value) and value>=0 else muted)
    elif engine==8:
        w=a['width'];rank=100*sum(v<=w[-1] for v in w)/len(w)
        wrap(1072,300,'Bollinger width: '+number(w[-1])+' %',32,27,2)
        wrap(1072,405,'Width sample percentile: '+number(rank),32,26,2)
        wrap(1072,530,'Current ATR: '+number(m['atr']),30,27,2)
    elif engine==10:
        for i in range(5):ref(m['support']+(m['resistance']-m['support'])*i/4,str(i),secondary)
        text(1072,300,'Close occupancy | 70 bars',24)
        bars(a['occupancy'],1072,360,400,300,['Q1','Q2','Q3','Q4'])
    elif engine==11:
        text(1072,300,'Indexed at first close = 100',23)
        curves=context['peer_curves'];values=[v for p in curves for v in p['indexed']];lo=min(values);span=max(values)-lo or 1
        for j,p in enumerate(curves):
            color=accent if j==0 else secondary
            line([(1080+i*8.4,725-(v-lo)/span*340) for i,v in enumerate(p['indexed'])],color,4)
            text(1072,780+j*30,p['symbol'],23,color)
    elif engine==12:
        values=context['watchlist'];low=min(p['change'] for p in values);span=max(p['change'] for p in values)-low or 1
        maxvol=max(p['relative_volume'] for p in values) or 1
        text(1072,300,'Sample: '+str(len(values))+' watchlist assets',24)
        text(1072,750,'X: daily change | Y: relative volume',20,muted)
        for p in values:
            x=1080+(p['change']-low)/span*395;y=700-p['relative_volume']/maxvol*330
            d.ellipse((x-5,y-5,x+5,y+5),fill=accent);text(x+7,y-5,p['symbol'][:6],15)
    elif engine==13:
        peers=context['peers'];text(1072,300,'Saved RSI comparison',25)
        bars([m['rsi']]+[p['rsi'] for p in peers],1072,370,400,300,[snapshot.symbol]+[p['symbol'] for p in peers])
    elif engine==14:
        text(1072,300,'Base volume by UTC bucket',24)
        bars([b[1] for b in a['buckets']],1072,370,400,320,[b[0] for b in a['buckets']])
        text(1072,740,'Unequal sample counts; descriptive',19,muted)
    elif engine in (15,24):
        sample=cs[-4:]
        for i,c in enumerate(sample):
            span=c.high-c.low or 1;upper_w=(c.high-max(c.open,c.close))/span*100;lower_w=(min(c.open,c.close)-c.low)/span*100
            y=305+i*120;text(1072,y,f'Closed bar {-3+i}',23,accent)
            text(1072,y+32,'Upper '+number(upper_w)+'% | Lower '+number(lower_w)+'%',21)
            text(1072,y+62,'Body '+number(abs(c.close-c.open)/span*100)+'%',21,muted)
            if engine==24:
                x=1515;top=y+10;bottom=y+95
                def small_y(value):return bottom-(value-c.low)/span*(bottom-top)
                color=up if c.close>=c.open else down
                d.line((x,small_y(c.high),x,small_y(c.low)),fill=color,width=3)
                d.rectangle((x-9,min(small_y(c.open),small_y(c.close)),x+9,max(small_y(c.open),small_y(c.close))+1),fill=color)
    elif engine in (16,19):
        entries=(('VWAP extension / ATR',a['extension']),('Relative volume',m.get('relative_volume')),('RSI',m.get('rsi')),('Candle interval (minutes)',snapshot.interval/60))
        for i,(label,value) in enumerate(entries):text(1072,305+i*103,label,22,muted);text(1072,336+i*103,number(value),30,accent)
        wrap(1072,747,'Self-check: what would weaken this interpretation?',32,23,3)
    elif engine==17:
        for i,(label,value) in enumerate(zip(('RSI','RelVol / 4 x 100','Extension / 3 ATR x 100'),a['gauges'])):
            y=315+i*155;text(1072,y,label,24);d.arc((1090,y+37,1460,y+145),180,360,fill=grid,width=14)
            if finite(value):d.arc((1090,y+37,1460,y+145),180,180+value*1.8,fill=accent,width=14)
            text(1215,y+85,number(value),28)
    elif engine==18:
        volume=max(range(len(cs)-35,len(cs)),key=lambda i:cs[i].volume);extremum=max(range(start,len(cs)),key=lambda i:cs[i].high)
        for j,i in enumerate((volume,extremum,len(cs)-1),1):text(px(i)-8,py(cs[i].high)-26,str(j),24,accent)
        wrap(1072,305,'Pins: max recent volume / highest visible high / latest closed bar',32,27,5)
    elif engine==20:
        ref(m['support'],'support');ref(m['support']-m['atr'],'buffer',secondary)
        counts=[sum(c.low<level for c in cs[-40:]) for level in (m['support'],m['support']-m['atr'],m['support']-1.5*m['atr'])]
        wrap(1072,300,'Historical lows below current reference levels (40 bars)',31,25,3)
        bars(counts,1072,420,400,250,['Support','-1 ATR','-1.5 ATR'])
    elif engine==21:
        anchor=(px(len(cs)-1),py(close[-1]))
        # A separate scenario panel makes the historical/scenario boundary explicit.
        text(1072,300,'ILLUSTRATIONS ONLY',26,accent)
        for i,(label,v,color) in enumerate((('Hold -> resistance',m['resistance'],up),('Fail -> support - 1.5 ATR',m['support']-1.5*m['atr'],down))):
            y=420+i*180
            slope=-.2 if i==0 else .2
            for x in range(1090,1460,24):
                d.line((x,y+slope*(x-1090),x+12,y+slope*(x+12-1090)),fill=color,width=3)
            text(1072,y-80,label,22,color);text(1072,y+40,number(v),28,color)
    elif engine==22:
        center=(1300,505);radius=150;vertices=[]
        for i,score in enumerate(a['scores']):
            angle=-math.pi/2+i*2*math.pi/5;x=center[0]+radius*math.cos(angle);y=center[1]+radius*math.sin(angle)
            d.line((*center,x,y),fill=grid,width=2)
            scale=(score or 0)/100;vertices.append((center[0]+radius*scale*math.cos(angle),center[1]+radius*scale*math.sin(angle)))
        d.polygon(vertices,outline=accent,width=4)
        wrap(1072,295,'Trend / value proximity / RSI balance / width compression / participation',34,23,3)
        wrap(1072,700,'Normalized heuristics; formulas documented in release notes.',34,22,4)
    elif engine==23:
        values=a['returns'];lo=min(values);span=max(values)-lo or 1;hist=[0]*15
        for v in values:hist[min(14,int((v-lo)/span*15))]+=1
        maximum=max(hist) or 1
        for i,v in enumerate(hist):d.rectangle((1085+i*29,700-v/maximum*320,1107+i*29,700),fill=secondary)
        wrap(1072,300,'Latest open-to-close return: '+number(values[-1])+'%',33,25,3)
        text(1072,760,'Empirical percentile: '+number(a['percentile']),22,accent)
    elif engine==25:
        ledger=context['ledger'];curve=ledger['curve'];lo=min(0,min(curve));span=max(curve)-lo or 1
        text(1072,300,'SEVEN-DAY COHORT | PAPER',24,accent)
        for i,(label,key) in enumerate((('Tracked','generated'),('Closed','closed'),('Stopped','stopped'),('Expectancy R','expectancy_r'),('Max drawdown R','max_drawdown_r'))):
            text(1072,345+i*48,label+': '+number(ledger[key]),23)
        line([(1080+i*430/max(1,len(curve)-1),790-(v-lo)/span*180) for i,v in enumerate(curve)],secondary,3)
    text(65,740,'SAVED EVIDENCE  /  '+snapshot.source.upper()+' SPOT',20,accent)
    wrap(65,780,NOTES.get(engine,'Use only saved and verified observations.'),85,22,2)
    d.line((50,897,1550,897),fill=grid,width=2)
    footer=snapshot.source+' spot  |  Prices in '+snapshot.quote
    if not clean:footer=stamp(snapshot.as_of)+'  |  '+footer
    text(50,915,footer,22,muted)
    text(50,952,'Conditional research. No recommendation, guaranteed outcome or affiliation implied.',22,muted)
    return save(image,directory)
