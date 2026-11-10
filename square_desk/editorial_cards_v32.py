"""Post excerpts and original artwork; no fabricated diagnostics or KPI scores."""
from PIL import Image,ImageDraw,ImageOps
import io,re
from .models import digest,utc
from .visual_design import Canvas,FONT_PACK,save
from .editorial_inbox import card_binding,card_subtitle

VERSION='editorial-card-v32-1'


def recipe(draft):
    if draft.get('category') not in ('education','news','campaign','scenario_audit'):
        raise ValueError('Unsupported editorial card')
    # Excerpts come from the post, not another model or a synthetic checklist.
    text=re.sub(r'https?://\S+|(?<!\w)#[A-Za-z][A-Za-z0-9_]*','',draft.get('body',''))
    excerpts=[p.strip() for p in text.split('\n\n') if len(p.split())>=5][:3]
    selected=None
    if draft.get('category')=='education':
        from .visual_art import catalog,source
        scores=[]
        for asset in catalog():
            score=len(re.findall(asset['pattern'],draft['title']+' '+text[:1200],re.I))
            if score:
                try:source(asset['id'],asset['sha256'])
                except (ValueError,OSError):continue
                scores.append((score,asset))
        if scores:
            selected=max(scores,key=lambda x:(x[0],digest([draft['title'],x[1]['id']])))[1]
    return dict(version=VERSION,title=draft['title'],subtitle=card_subtitle(draft),
        excerpts=excerpts,binding=card_binding(draft),font_pack=FONT_PACK,as_of=utc(),
        asset_id=selected['id'] if selected else None,asset_sha256=selected['sha256'] if selected else None)


def render(saved,directory):
    if saved.get('version')!=VERSION or saved.get('font_pack')!=FONT_PACK:
        raise ValueError('Editorial card resources changed')
    c=Canvas();c.wrap(45,38,saved['title'],1340,43,3,role='bold')
    c.wrap(45,180,saved['subtitle'],1340,23,2,color=c.accent)
    if saved.get('asset_id'):
        from .visual_art import source
        with Image.open(io.BytesIO(source(saved['asset_id'],saved['asset_sha256']))) as original:
            image=ImageOps.contain(original.convert('RGB'),(440,535),Image.Resampling.LANCZOS)
            c.image.paste(image,(950+(440-image.width)//2,275+(535-image.height)//2))
        c.d=ImageDraw.Draw(c.image);width=830
    else:width=1300
    for i,paragraph in enumerate(saved['excerpts']):
        y=280+i*170;c.card((45,y,65+width,y+145),outline=c.line)
        c.text(65,y+12,'From the post' if i==0 else 'Further context' if i==1 else 'Read with the full post',19,c.accent,'bold')
        c.wrap(65,y+42,paragraph,width-25,24,3)
    c.text(45,845,'Editorial excerpt · Read the full post for source, conditions and limitations',22,c.muted)
    return save(c.image,directory)
