"""Pinned fonts and compact deterministic drawing for v32 only."""
from pathlib import Path
from functools import lru_cache
import hashlib,io,json
from PIL import Image,ImageDraw,ImageFont

ROOT=Path(__file__).with_name('fonts')
HASHES={'Sans-Regular.ttf':'7fb917c89019896d0b52ee84b7cbb3304c18cb90b19a62f5e32712bd23e97669',
        'Sans-Bold.ttf':'5a3b0c8cc8ca545155150b4512a1fa248298df121c50d6557e651e61fbdab92f',
        'Mono-Medium.ttf':'a9b4c49bb299e05b5f6c481e7fb5e78943d2793249a0c8874ab574a2d1ea6755'}
FONT_PACK=hashlib.sha256(json.dumps(HASHES,sort_keys=True).encode()).hexdigest()

@lru_cache(maxsize=48)
def font(size,role='regular'):
    name={'regular':'Sans-Regular.ttf','bold':'Sans-Bold.ttf','mono':'Mono-Medium.ttf'}[role]
    data=(ROOT/name).read_bytes()
    if hashlib.sha256(data).hexdigest()!=HASHES[name]:raise ValueError('Bundled font changed')
    return ImageFont.truetype(io.BytesIO(data),size=size,layout_engine=ImageFont.Layout.BASIC)


def save(image,directory,colors=128):
    encoded=io.BytesIO()
    base=image if image.mode=='RGB' else image.convert('RGB')
    base.quantize(colors=colors,method=Image.Quantize.FASTOCTREE,
        dither=Image.Dither.NONE).save(encoded,format='PNG',optimize=True,compress_level=9)
    data=encoded.getvalue()
    from .media import validate_png
    hashed=validate_png(data)
    root=Path(directory);root.mkdir(parents=True,exist_ok=True)
    (root/(hashed+'.png')).write_bytes(data)
    return hashed+'.png'


class Canvas:
    def __init__(self,width=1440,height=900,palette=None):
        self.bg,self.panel,self.line,self.up,self.down,self.accent,self.secondary=palette or (
            '#09141E','#122332','#29404C','#31C59C','#F77788','#F3C568','#6BBFCE')
        self.ink,self.muted='#F1F5F5','#ABC0C8'
        self.image=Image.new('RGB',(width,height),self.bg);self.d=ImageDraw.Draw(self.image)

    def text(self,x,y,value,size=24,color=None,role='regular'):
        self.d.text((x,y),str(value),font=font(size,role),fill=color or self.ink)

    def wrap(self,x,y,value,width,size=26,lines=3,color=None,role='regular'):
        words=str(value).split();out=[];line=''
        for word in words:
            candidate=(line+' '+word).strip()
            if line and self.d.textlength(candidate,font=font(size,role))>width:
                out.append(line);line=word
            else:line=candidate
        if line:out.append(line)
        for i,line in enumerate(out[:lines]):
            if i==lines-1 and len(out)>lines:
                while line and self.d.textlength(line+'…',font=font(size,role))>width:line=line.rsplit(' ',1)[0] if ' ' in line else line[:-1]
                line+='…'
            self.text(x,y+i*(size+8),line,size,color,role)
        return y+min(lines,len(out))*(size+8)

    def card(self,box,fill=None,outline=None):
        self.d.rounded_rectangle(box,radius=18,fill=fill or self.panel,outline=outline,width=2)

    def pill(self,x,y,width,label,value,color=None):
        self.card((x,y,x+width,y+70),outline=color or self.line)
        self.text(x+15,y+8,label,17,self.muted)
        self.text(x+15,y+32,value,24,self.ink if not color or color==self.line else color,'mono')

    def lineplot(self,values,box,color=None,*,bounds=None,width=4):
        x0,y0,x1,y1=box;lo,hi=bounds or (min(values),max(values));span=hi-lo or 1
        points=[(x0+i*(x1-x0)/max(1,len(values)-1),y1-(v-lo)/span*(y1-y0)) for i,v in enumerate(values)]
        if len(points)>1:self.d.line(points,fill=color or self.accent,width=width)
        else:self.d.ellipse((x0-3,y1-3,x0+3,y1+3),fill=color or self.accent)
        return points

    def bars(self,values,labels,box,color=None):
        x0,y0,x1,y1=box;maximum=max(abs(v) for v in values) or 1
        negative=any(v<0 for v in values);zero=(y0+y1)/2 if negative else y1
        scale=(y1-y0)/(2 if negative else 1)/maximum;step=(x1-x0)/max(1,len(values))
        self.d.line((x0,zero,x1,zero),fill=self.line,width=2)
        for i,(v,label) in enumerate(zip(values,labels)):
            x=x0+i*step+step*.13;right=x+step*.68;y=zero-v*scale
            self.d.rectangle((x,min(y,zero),right,max(y,zero)+1),fill=color or (self.up if v>=0 else self.down))
            self.text(x,y1+12,label,18,self.muted)
