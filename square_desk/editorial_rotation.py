"""Small local, fact-gated daily shuffle. No network calls or extra AI requests."""
from collections import Counter
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib,json,math,re
from .models import utc,digest

FAMILIES={1:'Emotional regulation',2:'Market paradoxes',3:'Cognitive biases',
    4:'Mindful research',5:'Execution discipline',6:'Useful analogies',
    7:'Evidence detective',8:'Cross-asset questions',9:'Humour and myths',10:'Community ethics'}
STYLE={1:'psychology',2:'myth_qa',3:'psychology',4:'risk_clinic',5:'risk_clinic',
       6:'crypto_exploration',7:'market_detective',8:'multi_coin',9:'meme_learn',10:'community_ethics'}
STRUCTURES=(('The question','Evidence and an alternative','A practical self-check'),
    ('A hypothetical decision','Where the analogy helps','Where the analogy fails'),
    ('A common assumption','The counterexample to consider','What could change the reading'),
    ('The observation','A conditional explanation','The limits of the evidence'),
    ('The research dilemma','The trade-off','A rule worth testing'),
    ('The tempting shortcut','A more careful interpretation','A useful next observation'))

@lru_cache(maxsize=1)
def catalog():
    value=json.loads(Path(__file__).with_name('concepts.json').read_text(encoding='utf-8'))
    assert len(value['concepts'])==500 and len({c['id'] for c in value['concepts']})==500
    return value

def eligible(c,event):
    if c['event']!=event['category']:return False
    facts={**event.get('metrics',{}),**{'change_'+k:v for k,v in event.get('changes',{}).items()}}
    # Signal-only facts can never be inferred from ordinary mover observations.
    return all(type(facts.get(k)) in (int,float) and math.isfinite(facts[k]) for k in c['required'])

def choose(settings,db,event,now=None):
    now=utc() if now is None else now
    day=datetime.fromtimestamp(now,ZoneInfo(settings.timezone)).date().isoformat()
    day_number=datetime.fromisoformat(day).date().toordinal()
    history=db.state('content_mix_progress',{}).get('concept_history',[])
    pool=[c for c in catalog()['concepts'] if eligible(c,event)]
    if not pool:return None
    last={};today=Counter();used_today=set()
    for ident,used_day in history:
        last[ident]=max(last.get(ident,0),used_day)
        if used_day==day_number:
            used_today.add(ident);today[(ident-1)//50+1]+=1
    # Exhaust fresh eligible topics before relaxing the seven-day cooldown.
    fresh=[c for c in pool if day_number-last.get(c['id'],0)>=7]
    pool=fresh or [c for c in pool if c['id'] not in used_today] or pool
    seed=day+':'+digest(event)
    def score(c):
        shuffled=hashlib.sha256((seed+':'+str(c['id'])).encode()).hexdigest()
        return (today[c['family']],last.get(c['id'],0) if not fresh else 0,shuffled)
    c=min(pool,key=score)
    n=int(hashlib.sha256((seed+':layout').encode()).hexdigest()[:8],16)
    return {**c,'version':catalog()['version'],'day':day,'selected_at':now,
            'structure':n%len(STRUCTURES),'palette':n%6+1,
            'cooldown_relaxed':not bool(fresh)}

def apply(settings,db,event,brief,recent):
    c=choose(settings,db,event)
    if not c:return
    # Use source topics as questions. Original claims/outlines are never promoted to facts.
    brief['concept']={k:c[k] for k in ('id','family','version','day','selected_at','structure','palette','topic','view','cooldown_relaxed')}
    brief['instruction'] += (' Explore this editorial research topic as a question: '+c['topic']+
        '. Treat it as a hypothetical mechanism, never an assertion about actual traders or this asset. '
        'Do not infer motives, order-book liquidity, stop clusters, absorption, order flow, capital rotation, '
        'psychology statistics, historical success, personal experience, or causation from candles. '
        'State the limits and use only the supplied bound facts. A counterexample and self-check should explain the idea.')
    brief['article_outline']=STRUCTURES[c['structure']]
    # Existing setup/shock style and human review rules remain in force.
    if event['category'] not in ('setup','shock'):
        style=STYLE[c['family']]
        if style=='multi_coin':
            from .content_mix import peers
            related=peers(settings,db,event)
            if len(related)==2:brief['related']=related
            else:style='market_detective'
        brief['style']=style
    title=c['title'].replace('$SYMBOL:','').replace('$SYMBOL','').strip()
    if re.search(r'\d|\b(we|our|never|always|proof|proves|safe|guarantee)\b',title,re.I):
        title='A Research Question About '+FAMILIES[c['family']]
    brief['_concept_title']=title[:100]
    question=c['question'].replace('\u2014','. ').replace('\ufffd',' ')
    question=re.sub(r'\b15-minute\b','short-interval',question)
    question=re.sub(r'\b24h\b','daily',question)
    if ('{{' not in question and not re.search(r'\d|\$|follow|like this|comment below|dm me',question,re.I)):
        brief['question']=question
    from .content_mix import STYLES
    brief['instruction']=STYLES[brief['style']][0]+' Explore this editorial research topic as a question: '+brief['instruction'].split(' Explore this editorial research topic as a question: ',1)[1]
    brief['tag']=STYLES[brief['style']][4]

def remember(progress,c):
    history=progress.get('concept_history',[])
    history.append([c['id'],datetime.fromisoformat(c['day']).date().toordinal()])
    progress['concept_history']=history[-500:]
