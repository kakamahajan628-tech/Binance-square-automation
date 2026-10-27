"""Choose presentation from saved post prose; no network or storage access."""
import re
from .content_mix import valid_provenance

REVISION='post-aligned-v1'
TERMS={
    'momentum':r'\b(rsi|macd|momentum|overbought|oversold)\b',
    'volatility':r'\b(volatility|bollinger|bands|atr|compression)\b',
    'value':r'\b(volume|vwap|liquidity|turnover)\b',
    'trend':r'\b(ema\s*(?:20|50)?|moving averages?|trend)\b',
    'structure':r'\b(support|resistance|breakout|breakdown|range|retest)\b'}


def for_draft(draft):
    title=str(draft.get('title',''))[:200]
    body=str(draft.get('body',''))[:20000]
    suffix=draft.get('presentation_suffix')
    if suffix and body.endswith(suffix):body=body[:-len(suffix)]
    body=body.split('[audit:',1)[0]
    style=draft.get('content_style','')
    scores={key:4*len(re.findall(pattern,title,re.I))+len(re.findall(pattern,body,re.I))
            for key,pattern in TERMS.items()}
    if style in ('psychology','evergreen_learning','community_ethics','crypto_exploration','meme_learn'):
        view='structure'
    else:
        view=max(scores,key=lambda key:scores[key]) if max(scores.values()) else 'structure'
    text=title+' '+body
    context={'chart_view':view,'alignment_version':REVISION,
             'show_macd':bool(re.search(r'\bmacd\b',text,re.I)),
             'show_vwap':bool(re.search(r'\bvwap\b',text,re.I))}
    related=[]
    if style=='multi_coin' and valid_provenance(draft):
        event=draft['event']
        for peer in draft['internal_evidence'].get('related',[])[:2]:
            if (peer.get('source')==event['metrics'].get('source')
                    and peer.get('quote')==event['metrics'].get('quote')
                    and isinstance(peer.get('as_of'),(int,float))
                    and abs(peer['as_of']-event['as_of'])<=900
                    and re.fullmatch(r'[A-Z0-9]{2,12}',str(peer.get('symbol','')))):
                related.append({key:peer[key] for key in ('symbol','observations') if key in peer})
    context['related']=related
    return context
