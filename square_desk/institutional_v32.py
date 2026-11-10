"""Engine-led data graphics: historical candles never come from imagegen."""
import math
from PIL import ImageDraw
from .visual_design import Canvas,FONT_PACK,save
from .institutional_views import NAMES,NOTES,PALETTES,calculate,eligible,finite,number
from .analysis import ema

VERSION='institutional-v32-1'


def render(snapshot,m,directory,*,context):
    if context.get('renderer')!=VERSION or context.get('font_pack')!=FONT_PACK:
        raise ValueError('Versioned chart resources unavailable')
    engine=context.get('engine')
    if type(engine) is not int or type(context.get('palette',1)) is not int:
        raise ValueError('Invalid chart context')
    if not eligible(engine,m,context):raise ValueError('Engine evidence unavailable')
    c=Canvas(palette=PALETTES[(context.get('palette',1)-1)%len(PALETTES)])
    a=calculate(snapshot,m,context);cs=snapshot.candles;cl=[b.close for b in cs];start=max(0,len(cs)-70)
    c.text(40,24,'$'+snapshot.symbol+'/'+snapshot.quote,30,c.accent,'bold')
    c.text(420,30,str(snapshot.interval//60)+'m closed candles',22,c.muted)
    c.wrap(40,78,NAMES[engine],1330,40,2,role='bold')
    values=(('Closed price',number(m['price'])),('Rolling VWAP',number(a['vwap'])),
            ('ATR',number(m.get('atr'))),('Relative volume',number(m.get('relative_volume'))+'x'),('RSI',number(m.get('rsi'))))
    for i,(label,value) in enumerate(values):
        color=c.up if i==0 and finite(a['vwap']) and m['price']>a['vwap'] else c.accent if i==3 and (m.get('relative_volume') or 0)>2 else c.line
        c.pill(40+i*276,165,260,label,value,color)
    c.card((40,260,1035,735));c.card((1060,260,1400,735))
    plot=(85,315,970,650)
    def caption(title,detail=''):
        c.wrap(1085,290,title,290,28,3,c.accent,'bold')
        if detail:c.wrap(1085,410,detail,290,24,7)
    def chart(overlay=False):
        visible=cs[-70:];lo=min(b.low for b in visible);hi=max(b.high for b in visible)
        refs=[m.get(k) for k in ('support','resistance','support_24h','resistance_24h')]
        if overlay:
            lo=min([lo]+[v for v in refs if finite(v)]);hi=max([hi]+[v for v in refs if finite(v)])
        span=hi-lo or max(hi*.00001,.001);lo-=span*.08;hi+=span*.08
        x0,y0,x1,y1=plot
        py=lambda v:y1-(v-lo)/(hi-lo)*(y1-y0)
        step=(x1-x0)/len(visible)
        for j in range(5):
            y=y0+j*(y1-y0)/4;c.d.line((x0,y,x1,y),fill=c.line,width=1)
        if overlay:
            for key,label,color in (('support','Local low',c.secondary),('resistance','Local high',c.secondary),
                                    ('support_24h','Prior-day low',c.accent),('resistance_24h','Prior-day high',c.accent)):
                v=m.get(key)
                if finite(v):
                    y=py(v);c.d.rectangle((x0,y-4,x1,y+4),fill=c.line);c.d.line((x0,y,x1,y),fill=color,width=2)
                    c.text(x0+8,y-24,label+' '+number(v),17,color,'mono')
        for i,b in enumerate(visible):
            x=x0+(i+.5)*step;color=c.up if b.close>=b.open else c.down
            c.d.line((x,py(b.high),x,py(b.low)),fill=color,width=2)
            c.d.rectangle((x-3,min(py(b.open),py(b.close)),x+3,max(py(b.open),py(b.close))+1),fill=color)
        y=py(visible[-1].close);c.d.rounded_rectangle((x1-168,y-18,x1,y+18),radius=8,fill=c.accent)
        c.text(x1-157,y-14,number(visible[-1].close),19,c.bg,'mono')
        c.text(85,688,'Saved OHLC · '+str(len(visible))+' closed bars',19,c.muted)
        return py,step

    if engine in (3,9,10,18,20):
        py,step=chart(overlay=engine!=9)
        if engine==9:
            for period,color in ((12,c.up),(20,c.accent),(26,c.secondary),(50,c.down)):
                vals=ema(cl,period)[-70:];points=[(85+(i+.5)*step,py(v)) for i,v in enumerate(vals)]
                c.d.line(points,fill=color,width=3)
            caption('EMA ribbon','Lagging transforms of the same closes. Color does not imply a forecast.')
        elif engine==10:
            for fraction in (.25,.5,.75):
                y=py(m['support']+(m['resistance']-m['support'])*fraction)
                c.d.line((85,y,970,y),fill=c.line,width=2)
            caption('Local quartiles','Local '+number(m.get('range_seconds',snapshot.interval*20)/3600)+'-hour range; bars below count close occupancy.')
            c.bars(a['occupancy'],['Q1','Q2','Q3','Q4'],(1100,565,1365,665),c.secondary)
        elif engine==18:
            indexes=(max(range(start,len(cs)),key=lambda i:cs[i].volume),max(range(start,len(cs)),key=lambda i:cs[i].high),len(cs)-1)
            for j,index in enumerate(indexes,1):
                x=85+(index-start+.5)*step;y=py(cs[index].high)
                c.d.ellipse((x-10,y-10,x+10,y+10),outline=c.accent,width=3);c.text(x+12,y-20,str(j),18,c.accent)
            caption('Three recorded clues','Pins: maximum visible volume, highest visible high, latest closed candle. No trader motives are implied.')
        elif engine==20:
            levels=(m['support'],m['support']-m['atr'],m['support']-1.5*m['atr'])
            for value in levels[1:]:
                y=py(value)
                if 315<=y<=650:c.d.line((85,y,970,y),fill=c.secondary,width=2)
            counts=[sum(b.low<level for b in cs[-40:]) for level in levels]
            caption('ATR references','Bars count historical lows below current support and ATR offsets; not future stop survival.')
            c.bars(counts,['S','-A','-1.5A'],(1100,570,1365,665),c.secondary)
        else:
            for index,kind in a['rejections']:
                if index>=start:
                    x=85+(index-start+.5)*step;y=py(cs[index].high if kind=='upper' else cs[index].low)
                    c.d.ellipse((x-7,y-7,x+7,y+7),outline=c.accent,width=3)
            caption('Recorded wick rejections',str(len(a['rejections']))+' historical prior-range observations. Rings do not establish stop clusters or order flow.')
    elif engine==1:
        vals=a['profile'];maximum=max(vals) or 1
        for i,v in enumerate(vals):
            y=680-i*15;c.d.rectangle((285,y,285+v/maximum*650,y+10),fill=c.accent if v==maximum else c.secondary)
            if i%4==0:c.text(85,y-4,number(a['profile_low']+i*a['profile_step']),18,c.muted,'mono')
        caption('Candle-distributed volume','Horizontal bars distribute candle volume across each range. Not measured traded-at-price.')
    elif engine in (2,4,8,25):
        vals=a['pressure'][-100:] if engine==2 else cl[-96:] if engine==4 else a['width'][-96:] if engine==8 else context['ledger']['curve']
        c.lineplot(vals,plot,color=c.secondary)
        c.text(85,280,'Observed sample' if engine!=25 else 'Simulated paper cohort',22,c.accent,'bold')
        c.text(85,680,'Start '+number(vals[0])+'   Latest '+number(vals[-1]),21,c.muted,'mono')
        if engine==4:
            caption('Rolling VWAP bands','VWAP '+number(a['vwap'])+'; weighted dispersion '+number(a['sigma'])+'. These are historical, not confidence bands.')
            if finite(a['vwap']):
                lo,hi=min(vals),max(vals);span=hi-lo or 1
                for factor in (-1,0,1):
                    v=a['vwap']+factor*a['sigma'];y=650-(v-lo)/span*335
                    if 315<=y<=650:c.d.line((85,y,970,y),fill=c.accent if factor==0 else c.line,width=2)
        elif engine==8:caption('Width over time','Bollinger width in percent. Compression can persist and does not specify direction.')
        elif engine==25:
            p=context['ledger'];caption('Both wins and losses','Closed: '+str(p['closed'])+'\nStopped: '+str(p['stopped'])+'\nExpectancy: '+number(p['expectancy_r'])+' R\nDrawdown: '+number(p['max_drawdown_r'])+' R')
        else:caption('CLV × volume proxy','The curve has an arbitrary origin. It is not CVD, taker flow or measured buying pressure.')
    elif engine in (7,13,14,23):
        if engine==7:
            pairs=[(k,v) for k,v in context.get('changes',{}).items() if finite(v)]
            labels=[x[0] for x in pairs];vals=[x[1] for x in pairs];caption('Recorded horizons','Percentage changes only for available saved history.')
        elif engine==13:
            labels=[snapshot.symbol]+[p['symbol'] for p in context['peers']];vals=[m['rsi']]+[p['rsi'] for p in context['peers']];caption('Saved RSI comparison','Same-venue peer observations; this is not correlation.')
        elif engine==14:
            labels=[x[0] for x in a['buckets']];vals=[x[1] for x in a['buckets']];caption('UTC hour buckets','Base volume, unequal sample counts. No measured audience or liquidity peak is implied.')
        else:
            rs=a['returns'];lo=min(rs);span=max(rs)-lo or 1;vals=[0]*15
            for v in rs:vals[min(14,int((v-lo)/span*15))]+=1
            labels=['']*15;labels[0]=number(lo);labels[-1]=number(max(rs));caption('Empirical distribution','Observed candle returns; latest percentile '+number(a['percentile'])+'. No normal distribution assumption.')
        c.bars(vals or [0],labels or ['N/A'],plot)
    elif engine==11:
        curves=context['peer_curves'];bounds=(min(v for p in curves for v in p['indexed']),max(v for p in curves for v in p['indexed']))
        for i,p in enumerate(curves):
            color=c.accent if i==0 else c.secondary;c.lineplot(p['indexed'],plot,color,bounds=bounds)
            c.text(1085,320+i*70,p['symbol'],30,color,'bold')
        c.wrap(1085,500,'Indexed first close = 100; exact shared candle times. No causal claim.',290,23,6)
    elif engine==12:
        ps=context['watchlist'];lo=min(p['change'] for p in ps);span=max(p['change'] for p in ps)-lo or 1;mv=max(p['relative_volume'] for p in ps) or 1
        for p in ps:
            x=100+(p['change']-lo)/span*800;y=640-p['relative_volume']/mv*300
            c.d.ellipse((x-6,y-6,x+6,y+6),fill=c.accent);c.text(x+8,y-8,p['symbol'],18)
        caption('Watchlist sample','Horizontal: daily change. Vertical: relative volume. This is not the entire market.')
    elif engine==5:
        sample=cs[-50:];mv=max(b.volume for b in sample) or 1;ma=max(abs(b.close-b.open) for b in sample) or 1
        for b in sample:
            x=90+b.volume/mv*850;y=650-abs(b.close-b.open)/ma*320;c.d.ellipse((x-6,y-6,x+6,y+6),fill=c.secondary)
        caption('Effort versus movement','X: base volume; Y: absolute candle body. Neither axis establishes absorption or trader identity.')
    elif engine==15:
        for i,b in enumerate(cs[-4:]):
            span=b.high-b.low or 1
            values=((b.high-max(b.open,b.close))/span,abs(b.close-b.open)/span,(min(b.open,b.close)-b.low)/span)
            y=350+i*85;x=255;c.text(90,y,'Bar '+str(i+1),25,c.muted)
            for value,color in zip(values,(c.down,c.line,c.up)):
                end=x+value*650;c.d.rectangle((x,y,end,y+35),fill=color);x=end
        c.text(90,285,'Upper wick / body / lower wick',25,c.accent,'bold')
        caption('OHLC proportions','Each row totals its candle high-low range. Red: upper wick; neutral: body; green: lower wick.')
    elif engine==24:
        for i,b in enumerate(cs[-4:]):
            x=180+i*225;span=b.high-b.low or 1;py=lambda v:650-(v-b.low)/span*270;color=c.up if b.close>=b.open else c.down
            c.d.line((x,py(b.high),x,py(b.low)),fill=color,width=5)
            c.d.rectangle((x-25,min(py(b.open),py(b.close)),x+25,max(py(b.open),py(b.close))+2),fill=color)
            c.text(x-65,320,'Bar '+str(i+1),23,c.muted)
            c.text(x-70,675,number(abs(b.close-b.open)/span*100)+'%',20,color,'mono')
        caption('Separate candle geometry','Each candle is scaled to its own high/low. Body percentages are not cross-bar price changes.')
    elif engine==6:
        keys=('stop','entry','target1','target2');lo=min(m[k] for k in keys);span=max(m[k] for k in keys)-lo or 1
        for key in keys:
            y=650-(m[key]-lo)/span*300;color=c.down if key=='stop' else c.accent if key=='entry' else c.up
            c.d.line((150,y,850,y),fill=color,width=4);c.text(180,y-30,key+'  '+number(m[key]),25,color,'mono')
        caption('Conditional reference ladder','These are stated scenario levels, not observed fills, promises or account results.')
    elif engine==21:
        c.card((95,430,370,540),outline=c.accent);c.wrap(120,450,'Recorded close\n'+number(m['price']),230,26,2)
        for y,label,color in ((345,'If structure holds',c.up),(600,'If the assumption fails',c.down)):
            c.d.line((370,485,550,y),fill=color,width=4);c.card((560,y-40,970,y+55),outline=color);c.text(585,y-15,label,27,color,'bold')
        caption('Hypothetical branches','No future candles, probabilities or projected returns are drawn.')
    elif engine in (17,22):
        vals=a['gauges'] if engine==17 else a['scores'];labels=['RSI','RelVol scale','ATR extension'] if engine==17 else ['Trend','VWAP proximity','RSI balance','Width rank','Participation']
        for i,(label,v) in enumerate(zip(labels,vals)):
            y=325+i*(65 if engine==22 else 105);c.text(90,y,label,24,c.muted);c.d.rounded_rectangle((390,y,950,y+27),radius=10,fill=c.line)
            c.d.rounded_rectangle((390,y,390+560*max(0,min(100,v or 0))/100,y+27),radius=10,fill=c.secondary)
        caption('Disclosed heuristics','Normalized transformations, not calibrated win probabilities, safety scores or measured emotions.')
    elif engine==19:
        entries=(('Recorded source',snapshot.source+' spot / '+snapshot.quote),
                 ('Historical coverage',str(len(cs))+' closed candles; '+str(snapshot.interval//60)+'-minute intervals'),
                 ('Not supplied here','No on-chain or liquidation dataset is supplied to this graphic.'))
        for i,(title,detail) in enumerate(entries):
            y=310+i*125;c.card((85,y,975,y+95),outline=c.line);c.text(110,y+10,title,25,c.accent,'bold');c.wrap(390,y+18,detail,550,23,2)
        caption('Evidence inventory','Dataset scope, not a platform endorsement, safety rating or forecast.')
    else:
        for i,(title,detail) in enumerate((('Observe','What is directly recorded?'),('Separate','Which explanation is still hypothetical?'),('Recheck','What would weaken the interpretation?'))):
            y=310+i*125;c.card((85,y,975,y+95),outline=c.line);c.text(110,y+12,title,28,c.accent,'bold');c.text(330,y+22,detail,24)
        caption('Research self-check','A decision framework does not measure the minds or intentions of market participants.')
    c.wrap(45,760,NOTES[engine],1330,23,2,color=c.muted)
    c.text(45,842,snapshot.source+' spot · '+snapshot.quote+' · saved closed observations',21,c.muted)
    return save(c.image,directory)
