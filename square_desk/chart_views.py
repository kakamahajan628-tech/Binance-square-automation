"""Five deterministic, evidence-only chart views; no IO beyond the PNG save."""
from datetime import datetime, timezone
import hashlib
import math
from statistics import mean, pstdev

from PIL import Image, ImageDraw
from .analysis import ema
from .models import stamp

VIEWS = ('trend', 'value', 'volatility', 'momentum', 'structure')
NAMES = {'trend':'Trend and moving averages', 'value':'Volume-weighted value',
         'volatility':'Volatility bands', 'momentum':'Momentum beneath price',
         'structure':'Range and reference zones'}
NOTES = {'trend':'EMA lines smooth price and respond with a lag.',
         'value':'Rolling VWAP is a volume-weighted reference, not a price target.',
         'volatility':'Bollinger bands show recent dispersion; a touch is not a reversal signal.',
         'momentum':'MACD measures momentum; a crossover alone does not confirm direction.',
         'structure':'Zones describe a prior range. A level can fail or be crossed.'}
BG,GRID,TEXT,MUTED = '#101827','#263548','#edf4ff','#a7bbd2'
GREEN,RED,GOLD,BLUE,PURPLE = '#25ca98','#f06b85','#eab75d','#86a8ef','#ba9cfa'


def finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def price(value):
    if value==0:
        return '0'
    places=max(0,5-int(math.floor(math.log10(abs(value)))))
    if places>12 or abs(value)>=1e10:
        return f'{value:.6g}'
    return f'{value:,.{places}f}'.rstrip('0').rstrip('.') if places else f'{value:,.0f}'


def rsi_series(values,period=14):
    output=[None]*len(values)
    if len(values)<=period:
        return output
    delta=[b-a for a,b in zip(values,values[1:])]
    gain,loss=mean(max(d,0) for d in delta[:period]),mean(max(-d,0) for d in delta[:period])
    def value():
        return 100 if loss==0 and gain else 50 if gain==loss==0 else 100-100/(1+gain/loss)
    output[period]=value()
    for i,difference in enumerate(delta[period:],period+1):
        gain=(gain*(period-1)+max(difference,0))/period
        loss=(loss*(period-1)+max(-difference,0))/period
        output[i]=value()
    return output


def indicators(snapshot):
    candles=snapshot.candles
    closes=[c.close for c in candles]
    short,long=ema(closes,12),ema(closes,26)
    macd=[a-b for a,b in zip(short,long)]
    signal=ema(macd,9)
    middle,upper,lower,vwap=[],[],[],[]
    for i in range(len(candles)):
        if i>=19:
            window=closes[i-19:i+1]
            avg,deviation=mean(window),pstdev(window)
            middle.append(avg);upper.append(avg+2*deviation);lower.append(avg-2*deviation)
        else:
            middle.append(None);upper.append(None);lower.append(None)
        if i>=95:
            bars=candles[i-95:i+1]
            total=sum(c.volume for c in bars)
            value=sum((c.high+c.low+c.close)/3*c.volume for c in bars)/total if total else None
            vwap.append(value if finite(value) else None)
        else:
            vwap.append(None)
    return {'ema20':ema(closes,20),'ema50':ema(closes,50),'rsi':rsi_series(closes),
            'macd':macd,'signal':signal,'hist':[a-b for a,b in zip(macd,signal)],
            'middle':middle,'upper':upper,'lower':lower,'vwap':vwap}


def choose_view(snapshot,context=None):
    requested=(context or {}).get('chart_view')
    if requested in VIEWS:
        return requested
    seed=int(hashlib.sha256(snapshot.symbol.encode()).hexdigest()[:8],16)
    return VIEWS[(seed+int(snapshot.as_of//snapshot.interval))%len(VIEWS)]


def derivatives_context(snapshot,metrics,context=None):
    data=(context or {}).get('derivatives') or metrics.get('derivatives')
    if (not isinstance(data,dict) or data.get('source')!='binance_usdt_perpetual'
            or data.get('symbol')!=snapshot.symbol or snapshot.quote!='USDT'
            or snapshot.source!='binance' or not finite(data.get('as_of'))
            or not abs(snapshot.fetched_at-data['as_of'])<=300
            or not finite(data.get('open_interest_base_units')) or data['open_interest_base_units']<0):
        return None
    return data


def render(snapshot,metrics,directory,*,context=None,font,save):
    snapshot.validate(snapshot.fetched_at,max(snapshot.interval+300,1200))
    support,resistance=metrics.get('support'),metrics.get('resistance')
    if not finite(support) or not finite(resistance) or not 0<support<=resistance:
        raise ValueError('Invalid chart reference levels')
    all_series=indicators(snapshot)
    data=snapshot.candles[-70:]
    series={k:v[-len(data):] for k,v in all_series.items()}
    view=choose_view(snapshot,context)
    aligned=(context or {}).get('alignment_version')=='post-aligned-v1'
    show_macd=view=='momentum' and (not aligned or context.get('show_macd'))
    show_vwap=not aligned or context.get('show_vwap')
    image=Image.new('RGB',(1600,1100),BG)
    d=ImageDraw.Draw(image,'RGBA')
    left,right=70,1385
    top,bottom=190,(540 if show_macd else 580 if view=='momentum' else 655)
    volume_top,volume_bottom=(605,685) if show_macd else (640,720) if view=='momentum' else (715,800)
    rsi_top,rsi_bottom=(735,825) if show_macd else (795,965) if view=='momentum' else (855,960)
    spacing=(right-left)/len(data)
    def x(i):return left+(i+.5)*spacing
    d.text((55,28),f'{snapshot.symbol}/{snapshot.quote} | {snapshot.interval//60}m closed candles',font=font(36),fill=TEXT)
    source='SYNTHETIC DEMO, NOT MARKET DATA' if snapshot.source.startswith('demo') else snapshot.source
    def fitted(text,size,width):
        while size>14 and d.textlength(text,font=font(size))>width:size-=1
        while d.textlength(text,font=font(size))>width:text=text[:-2]+'…'
        return text,font(size)
    d.text((55,80),f'{source} | Candle close (UTC): {stamp(snapshot.as_of)}',font=font(19),fill=MUTED)
    name=('Volume against its baseline' if view=='value' and not show_vwap else
          'RSI momentum beneath price' if view=='momentum' and not show_macd else NAMES[view])
    d.text((55,123),name,font=font(25),fill=GOLD)
    d.text((975,129),f'Close {price(data[-1].close)} | RSI {series["rsi"][-1]:.2f}',font=font(20),fill=TEXT)
    relation='above prior resistance' if data[-1].close>resistance else 'below prior support' if data[-1].close<support else 'inside the prior range'
    d.text((55,158),'Last close: '+relation,font=font(19),fill=MUTED)
    overlays={'trend':[('ema20','EMA20',GOLD),('ema50','EMA50',BLUE)],
              'value':([('vwap','VWAP96',PURPLE)] if aligned else [('vwap','VWAP96',PURPLE),('ema20','EMA20',GOLD)]) if show_vwap else [],
              'volatility':[('upper','Upper band',BLUE),('middle','SMA20',GOLD),('lower','Lower band',BLUE)],
              'momentum':[] if aligned else [('ema20','EMA20',GOLD)],'structure':[]}[view]
    axis=[c.low for c in data]+[c.high for c in data]+[support,resistance]
    axis.extend(v for key,_,_ in overlays for v in series[key] if finite(v))
    lo,hi=min(axis),max(axis)
    pad=max((hi-lo)*.09,hi*.002)
    lo,hi=lo-pad,hi+pad
    def y(value):return bottom-(value-lo)/(hi-lo)*(bottom-top)
    def line(values,map_y,color,width=3):
        segment=[]
        for i,value in enumerate(values):
            if finite(value):
                segment.append((x(i),map_y(value)))
            else:
                if len(segment)>1:d.line(segment,fill=color,width=width)
                segment=[]
        if len(segment)>1:d.line(segment,fill=color,width=width)
    def dashed(yy,color):
        for start in range(left,right,15):d.line((start,yy,min(start+8,right),yy),fill=color,width=1)
    for i in range(6):
        value=lo+(hi-lo)*i/5
        yy=y(value)
        d.line((left,yy,right,yy),fill=GRID,width=1)
        d.text((1400,yy-10),price(value),font=font(18),fill=MUTED)
    atr=metrics.get('atr')
    zone=min(atr*.12,(hi-lo)*.035) if finite(atr) and atr>0 else 0
    if (view=='structure' or aligned) and zone:
        for level,color in ((support,(234,183,93,28)),(resistance,(134,168,239,28))):
            d.rectangle((left,y(min(hi,level+zone)),right,y(max(lo,level-zone))),fill=color)
    if view=='volatility':
        for i in range(1,len(data)):
            a,b=series['upper'],series['lower']
            if all(finite(v) for v in (a[i-1],a[i],b[i-1],b[i])):
                d.polygon([(x(i-1),y(a[i-1])),(x(i),y(a[i])),(x(i),y(b[i])),(x(i-1),y(b[i-1]))],fill=(134,168,239,22))
    width=max(3,min(6,spacing*.28))
    for i,candle in enumerate(data):
        color=GREEN if candle.close>=candle.open else RED
        d.line((x(i),y(candle.high),x(i),y(candle.low)),fill=color,width=2)
        d.rectangle((x(i)-width,min(y(candle.open),y(candle.close)),x(i)+width,max(y(candle.open),y(candle.close))+1),fill=color)
    legend_x=700
    for key,label,color in overlays:
        line(series[key],y,color)
        d.line((legend_x,168,legend_x+25,168),fill=color,width=3)
        d.text((legend_x+33,157),label,font=font(19),fill=color)
        legend_x+=175
    tag_positions=[]
    for key,level,color in (('Prior 20-bar support',support,GOLD),('Prior 20-bar resistance',resistance,BLUE)):
        d.line((left,y(level),right,y(level)),fill=color,width=2)
        label=f'{key}: {price(level)}'
        yy=max(top+3,min(bottom-24,y(level)-27))
        if any(abs(yy-prior)<26 for prior in tag_positions):
            yy=yy-29 if yy-29>=top else min(bottom-24,yy+29)
        tag_positions.append(yy)
        d.rectangle((left+5,yy-1,left+15+d.textlength(label,font=font(18)),yy+24),fill=BG)
        d.text((left+10,yy),label,font=font(18),fill=color)
    if view=='structure':
        middle=(support+resistance)/2
        dashed(y(middle),MUTED)
        d.text((right-250,y(middle)+5),'Range midpoint',font=font(18),fill=MUTED)
    dashed(y(data[-1].close),GREEN if data[-1].close>=data[-1].open else RED)
    for index in (0,len(data)//4,len(data)//2,3*len(data)//4,len(data)-1):
        label=datetime.fromtimestamp(data[index].end,timezone.utc).strftime('%d %b %H:%M')
        d.text((max(left,min(right-130,x(index)-55)),bottom+12),label,font=font(16),fill=MUTED)
    baseline=[mean(c.volume for c in snapshot.candles[i-20:i]) if i>=20 else None
              for i in range(len(snapshot.candles))][-len(data):]
    rv=data[-1].volume/baseline[-1] if baseline[-1] else None
    volume_label='Volume | base units | dashed: previous 20-bar mean'
    if rv is not None:volume_label+=f' | Latest {rv:.2f}x'
    d.text((left,volume_top-29),volume_label,font=font(19),fill=MUTED)
    max_volume=max([c.volume for c in data]+[v for v in baseline if finite(v)]) or 1
    for i,candle in enumerate(data):
        if candle.volume:
            d.rectangle((x(i)-width,volume_bottom-candle.volume/max_volume*(volume_bottom-volume_top),x(i)+width,volume_bottom),
                        fill=GREEN if candle.close>=candle.open else RED)
    for i in range(1,len(data),2):
        if finite(baseline[i-1]) and finite(baseline[i]):
            d.line((x(i-1),volume_bottom-baseline[i-1]/max_volume*(volume_bottom-volume_top),
                    x(i),volume_bottom-baseline[i]/max_volume*(volume_bottom-volume_top)),fill=GOLD,width=2)
    d.text((left,rsi_top-30),'RSI(14) | 30 / 50 / 70 reference lines',font=font(19),fill=MUTED)
    def ry(value):return rsi_bottom-value/100*(rsi_bottom-rsi_top)
    d.rectangle((left,ry(100),right,ry(70)),fill=(240,107,133,18))
    d.rectangle((left,ry(30),right,ry(0)),fill=(37,202,152,18))
    for level in (30,50,70):
        dashed(ry(level),GRID)
        d.text((1400,ry(level)-10),str(level),font=font(17),fill=MUTED)
    line(series['rsi'],ry,PURPLE)
    if show_macd:
        mt,mb=875,965
        d.text((left,842),'MACD(12,26,9) | line, signal and histogram',font=font(19),fill=MUTED)
        bound=max(abs(v) for key in ('macd','signal','hist') for v in series[key]) or 1e-12
        def my(v):return (mt+mb)/2-v/bound*(mb-mt)*.45
        dashed(my(0),GRID)
        for i,value in enumerate(series['hist']):
            d.rectangle((x(i)-width,min(my(0),my(value)),x(i)+width,max(my(0),my(value))+1),fill=GREEN if value>=0 else RED)
        line(series['macd'],my,BLUE,2);line(series['signal'],my,GOLD,2)
        d.text((1400,my(0)-10),'0',font=font(17),fill=MUTED)
    extra=derivatives_context(snapshot,metrics,context)
    note=NOTES[view]
    if view=='value' and not show_vwap:
        note='Volume bars show traded base units. Volume alone does not establish order-book liquidity or accumulation.'
    elif view=='momentum' and not show_macd:
        note='RSI is calculated from these closed candles. The reading alone does not confirm a reversal.'
    elif view=='value' and series['vwap'][-1] is None:
        note='VWAP96 unavailable: requires 96 closed bars with nonzero volume.'
    label,face=fitted(note,19,1490)
    d.text((left,1003),label,font=face,fill=MUTED)
    if extra:
        oi=f'Binance USDT perpetual OI: {price(extra["open_interest_base_units"])} {snapshot.symbol} | {stamp(extra["as_of"])}'
        d.text((left,1035),oi,font=font(17),fill=MUTED)
    else:
        peers=[]
        for peer in (context or {}).get('related',[])[:2]:
            observation=peer.get('observations',{})
            direction=observation.get('hourly_direction')
            volume=observation.get('volume')
            if direction in ('up','down','flat','unavailable') and volume in ('above its baseline','not above its baseline','unavailable'):
                peers.append(peer['symbol']+': '+direction+', volume '+volume)
        note=('Peer context (hourly): '+' | '.join(peers)+'. No measured correlation.' if peers else
              'Closed-candle research. Shaded reference zones use ATR padding; indicators do not guarantee outcomes.')
        label,face=fitted(note,18,1490)
        d.text((left,1035),label,font=face,fill=MUTED)
    return save(image,directory)
