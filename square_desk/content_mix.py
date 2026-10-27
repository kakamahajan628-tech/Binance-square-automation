"""Content presentation only. No scheduling, network, publishing or image work."""
import re
from .models import digest, stamp, utc

REVISION = '2026-10-06-content-mix-1'
DISCLAIMER = 'Probabilistic market research, not a recommendation or guaranteed return.'
CYCLE = ('market_detective', 'psychology', 'multi_coin', 'myth_qa', 'meme_learn',
         'risk_clinic', 'multi_coin', 'crypto_exploration', 'community_ethics', 'market_detective',
         'multi_coin', 'psychology', 'meme_learn', 'myth_qa', 'multi_coin',
         'risk_clinic', 'crypto_exploration', 'community_ethics', 'multi_coin', 'market_detective')
STYLES = {
    'market_detective': ('Open with the specific question raised by this observation. Explain a clue, an alternative reading and what would weaken each interpretation. Avoid an indicator inventory.',
        ('The clue and its limits', 'Competing readings of the same move', 'What would change the interpretation'),
        ('What Would Make This Move Convincing?', 'The Close Matters More Than the Headline', 'An Interesting Move, an Unfinished Story', 'What the Fast Candle Leaves Unanswered'),
        ('What would convince you: a clean close or a patient retest?', 'Which observation would make you change your reading?'), 'MarketAnalysis'),
    'psychology': ('Teach one human decision bias using this market observation as context. Explore attention, anchoring, regret, loss aversion or fear of missing out. Describe possible reactions, never claim to know actual traders\' motives. Include a practical self-check.',
        ('The decision bias', 'How it changes a research decision', 'A practical self-check'),
        ('When Excitement Starts Writing the Plan', 'The Chart Is Moving. Is Your Rule Moving Too?', 'A Missed Move Can Still Teach Something', 'Confidence Can Arrive Before Evidence'),
        ('Would you keep the same rule after missing a move?', 'What helps you notice when regret is influencing a decision?'), 'TradingPsychology'),
    'multi_coin': ('Compare the supplied primary asset with both verified peer observations. Explain same-venue hourly differences and their limits. A shared direction is not measured correlation, capital rotation, sector membership or a common cause. Never label a watchlist ranking the whole market\'s top gainer.',
        ('The primary asset question', 'What the peer observations add', 'What this comparison cannot establish'),
        ('A Stronger Question Than Who Is Winning', 'Same Watchlist, Different Stories', 'The Comparison Behind the Headline', 'A Leaderboard Is Only the Starting Point'),
        ('Would you focus on the leader or the asset holding its structure?', 'Which matters more in this comparison: direction or how the move holds?'), 'CryptoWatchlist'),
    'myth_qa': ('Use a natural question-and-answer or myth-versus-explanation format. Challenge one misconception about volume, breakouts, stop distance or certainty. Explain the mechanism in plain words and connect it to the observation. Do not turn a hypothetical answer into a live trade instruction.',
        ('The common question', 'The mechanism behind the answer', 'Where the answer stops applying'),
        ('Does a Loud Candle Tell the Whole Story?', 'A Popular Trading Shortcut Meets a Hard Question', 'The Signal and the Story Are Different Things', 'A Simple Question With a Conditional Answer'),
        ('Which trading assumption would you like to question next?', 'What evidence would you need before treating this as confirmation?'), 'CryptoLearning'),
    'meme_learn': ('Use gentle original text humour about a hypothetical research habit, then a useful lesson. Example contrast: the watchlist is calm while the mind is rushing. No copied meme caption, invented personal trade, shaming, profit joke or claim about real crowds. Make the learning deeper than the joke.',
        ('The humorous decision trap', 'Why the trap is understandable', 'A useful habit after the joke'),
        ('The Watchlist Is Calm. The Mind Has Other Plans', 'When the Candle Runs Faster Than the Checklist', 'The Chart Did Not Ask for an Emotional Reply', 'The Funniest Part Is Usually the Forgotten Rule'),
        ('Which research habit has saved you from an impulsive click?', 'Have you caught yourself changing a rule after seeing a fast move?'), 'LearnCrypto'),
    'risk_clinic': ('Explain one practical trade-off: entry quality versus chasing, meaningful invalidation versus stop distance, or waiting versus acting. Use a thought experiment, not invented PnL, holdings, position sizes or advice. Connect risk to the current evidence without dumping indicators.',
        ('The practical trade-off', 'What changes when the assumption fails', 'A decision checklist'),
        ('An Interesting Chart Can Still Be a Difficult Decision', 'A Clear Direction Can Hide an Unclear Risk', 'The Entry Question Comes Before the Victory Story', 'A Small Pause Can Reveal a Large Assumption'),
        ('Would you wait for clearer structure or reassess the risk first?', 'Which matters more to you: seeing the opportunity or defining its failure?'), 'RiskManagement'),
    'crypto_exploration': ('Explore a general crypto concept related to the observation: liquidity, spread, spot turnover, market versus limit orders, or the difference between token price and project value. Explain a mechanism with a clear analogy. No unverified project news, tokenomics, on-chain flows or product yields.',
        ('The concept worth exploring', 'How the mechanism works', 'How to apply it without overclaiming'),
        ('A Price Move Is Also a Lesson in Liquidity', 'The Useful Detail Behind a Familiar Crypto Word', 'What the Ticker Does Not Explain', 'The Question Between Price and Value'),
        ('Which crypto concept becomes clearer when you look at an actual chart?', 'What would you check before trusting a smooth-looking price move?'), 'CryptoEducation'),
    'community_ethics': ('Explore a fair dilemma about uncertainty, public analysis, social proof or incentives in crypto content. Teach readers how to separate evidence from persuasion. Discuss Write to Earn only as a general incentive dilemma, never invent current rewards, eligibility or guaranteed income. Invite thoughtful disagreement without engagement bait.',
        ('The fair dilemma', 'Why reasonable readers may disagree', 'A transparent way to discuss uncertainty'),
        ('When a Confident Caption Meets an Uncertain Chart', 'What Does a Helpful Crypto Post Owe Its Reader?', 'Attention Is Easy. Trust Takes More Work', 'A Fair Question Before a Strong Opinion'),
        ('Should a creator explain what could invalidate the idea before sharing a target?', 'What makes a crypto post helpful rather than merely persuasive?'), 'CryptoCommunity'),
    'signal_review': ('Explain the supplied conditional setup and its failure conditions. Keep direction, recomputed entry, stop, targets and heuristic disclosure intact. Use simple language; no certainty or realised outcome.',
        ('The conditional setup', 'Why the invalidation matters', 'Execution limits and unanswered questions'),
        ('A Conditional Setup Needs a Clear Failure Rule', 'The Setup Is a Question Until the Evidence Holds'),
        ('Which condition would invalidate this scenario first?',), 'RiskManagement'),
}


def qualitative(metrics, changes):
    change = changes.get('1h')
    volume = metrics.get('relative_volume')
    return {'hourly_direction': 'unavailable' if change is None else 'up' if change > 0 else 'down' if change < 0 else 'flat',
            'volume': 'unavailable' if volume is None else 'above its baseline' if volume > 1 else 'not above its baseline'}


def peers(settings, db, event):
    now, primary = utc(), event['metrics']
    universe = set(db.state('market_universe', {}).get('symbols', settings.symbols))
    if settings.market_universe != 'top_liquid':
        universe = set(settings.symbols)
    candidates = []
    for row in db.state('latest_market', []):
        symbol, m = row.get('symbol'), row.get('metrics', {})
        if (symbol == event['symbol'] or symbol not in universe or not isinstance(symbol, str)
                or not re.fullmatch(r'[A-Z0-9]{2,12}', symbol)
                or m.get('source') != primary.get('source') or m.get('quote') != primary.get('quote')
                or not 0 <= now - m.get('as_of', 0) <= min(settings.candle_max_age, settings.draft_max_age)
                or abs(m.get('as_of', 0) - event['as_of']) > 900):
            continue
        candidates.append({'symbol': symbol, 'source': m['source'], 'quote': m['quote'], 'as_of': m['as_of'],
                           'metrics': {k: v for k, v in m.items() if k in ('price', 'relative_volume', 'rsi')},
                           'changes': {k: v for k, v in row.get('changes', {}).items() if k in ('1h', '24h')},
                           'observations': qualitative(m, row.get('changes', {}))})
    # Benchmark if available, then a current watchlist mover. No claimed sectors,
    # global trending rank or measured correlation are synthesized.
    candidates.sort(key=lambda r: (r['symbol'] not in ('BTC', 'ETH'), -abs(r['changes'].get('24h') or 0), r['symbol']))
    chosen = candidates[:1]
    others = [r for r in candidates if not chosen or r['symbol'] != chosen[0]['symbol']]
    others.sort(key=lambda r: (-(r['changes'].get('24h') or 0), r['symbol']))
    return chosen + others[:1] if len(candidates) >= 2 else []


def plan(settings, db, event):
    progress = db.state('content_mix_progress', {})
    ordinal = int(progress.get('market_drafts', 0))
    slot = int(progress.get('mix_slots', 0))
    signal = event.get('manual_signal_review') or event['category'] in ('setup', 'shock')
    style = 'signal_review' if event['category'] == 'setup' else 'market_detective' if signal else CYCLE[slot % len(CYCLE)]
    related = peers(settings, db, event) if style == 'multi_coin' else []
    if style == 'multi_coin' and len(related) != 2:
        style = 'psychology'  # No stale comparisons or ticker stuffing to fill a ratio.
    description, outline, hooks, questions, tag = STYLES[style]
    total = int(progress.get('total_drafts', ordinal))
    visible = ((total + 1) >= 10 * (int(progress.get('proof_shown', 0)) + 1)
               and event['metrics']['source'] == 'binance')
    recent = db.list('draft', since=utc() - 7 * 86400, limit=500)
    prior_titles = [r['payload'].get('title', '') for r in recent]
    brief = {'version': REVISION, 'style': style, 'ordinal': ordinal, 'slot': slot,
             'instruction': description, 'article_outline': outline,
             'question': questions[ordinal % len(questions)], 'tag': tag,
             'public_proof': visible, 'related': related,
             'recent_titles': prior_titles[:6],
             '_prior_title_keys': [title_key(t) for t in prior_titles],
             'recent_openings': [r['payload'].get('body', '')[:150] for r in recent[:4]],
             'counts_as_mix_slot': not signal}
    brief['title'] = choose_title(event['symbol'], hooks, ordinal, prior_titles)
    brief['suffix'] = suffix(brief, event['symbol'])
    return brief


def title_key(title):
    title = re.sub(r'\$[A-Za-z0-9]+', '', title)
    return ' '.join(re.findall(r'[a-z]+', title.lower()))


def choose_title(symbol, hooks, ordinal, history):
    used = {title_key(t) for t in history}
    # Plain, substantive hooks. Never add IDs, random digits or fake urgency.
    lenses = ('Before the Next Decision', 'Through a Risk Lens', 'After the Excitement',
              'Beyond the First Reaction', 'While the Evidence Develops', 'With the Assumptions in View',
              'From a Patient Perspective', 'For a Clearer Watchlist', 'Without Rushing the Conclusion')
    openings = ('', 'A Research Question: ', 'A Closer Look: ', 'A Watchlist Puzzle: ',
                'The Decision Lens: ', 'A Useful Cross-Check: ', 'A Patient View: ', 'The Learning Angle: ')
    for offset in range(len(hooks) * (len(lenses) + 1) * len(openings)):
        i = ordinal + offset
        hook = hooks[i % len(hooks)]
        lens = '' if i // len(hooks) % (len(lenses) + 1) == 0 else ': ' + lenses[(i // len(hooks) - 1) % len(lenses)]
        opening = openings[(i // (len(hooks) * (len(lenses) + 1))) % len(openings)]
        candidate = '$' + symbol + ': ' + opening + hook + lens
        if title_key(candidate) not in used and len(candidate) <= 150:
            return candidate
    # An AI-authored unique hook is preferred at formatting time; this bounded
    # fallback does not exempt any draft from semantic duplicate validation.
    return '$' + symbol + ': ' + hooks[ordinal % len(hooks)]


def suffix(brief, symbol):
    related = brief['related']
    context = ''
    if related:
        a, b = related
        intro = ('Watchlist contrast', 'A useful cross-check', 'For a wider view', 'The comparison to keep in mind')[brief['ordinal'] % 4]
        context = (f"{intro}: ${a['symbol']} has hourly direction {a['observations']['hourly_direction']} and volume "
                   f"{a['observations']['volume']}; ${b['symbol']} has hourly direction {b['observations']['hourly_direction']} "
                   f"and volume {b['observations']['volume']}. This alone does not establish correlation.")
    assets = [symbol] + [r['symbol'] for r in related]
    tags = ['#' + s for s in assets] + ['#' + brief['tag'], '#CryptoLearning']
    tags = list(dict.fromkeys(tags))[:1 + brief['ordinal'] % 5]
    return '\n\n'.join(p for p in (context, brief['question'], ' '.join(tags)) if p)


def strip_metadata(text, evidence):
    # The intermediate provider/router result retains source+time for its
    # unchanged validation contract. Only the public rendering drops metadata.
    footer = evidence.get('transport_footer', '')
    if footer and text.endswith('\n\n' + footer):
        text = text[:-(len(footer) + 2)]
    text = re.sub(r'(?ms)^Recorded evidence for \$[A-Z0-9]+:.*\Z', '', text)
    timestamp = re.escape(evidence['timestamp'])
    source = re.escape(evidence['source'])
    text = re.sub(r'(?:Source:\s*' + source + r'(?:\s+spot candles)?\.?\s*)?'
                  r'(?:Data timestamp|Closed-candle timestamp):\s*' + timestamp + r'\.?', '', text, flags=re.I)
    text = re.sub(r'\bSource\s*:?[ \t]*' + source + r'(?:\s+spot candles)?\s*(?:at\s+)?' + timestamp + r'\.?', '', text, flags=re.I)
    text = re.sub(r'\bSource\s*:\s*' + source + r'(?:\s+spot candles)?\.?', '', text, flags=re.I)
    # Source/time occur only as presentation metadata, not substantive prose.
    text = text.replace(evidence['timestamp'], '').replace('[audit:' + evidence['source'] + '|]', '')
    return re.sub(r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n', text).strip()


def format_result(raw, evidence):
    brief = evidence.get('editorial_brief')
    if not brief:
        return dict(raw)
    body = strip_metadata(raw['body'], evidence)
    body = re.sub(r'(?<!\w)#[A-Za-z][A-Za-z0-9_]*', '', body)  # Relevant tags are application-owned.
    # Keep the existing no-em-dash writing rule. Limit emoji to one rather than
    # decorating every heading or using visual urgency.
    body = body.replace('\u2014', '. ')
    emojis = list(re.finditer(r'[\U0001F300-\U0001FAFF\u2600-\u27BF]', body))
    for match in reversed(emojis[1:]):
        body = body[:match.start()] + body[match.end():]
    question = brief['question']
    body = body.replace(question, '').rstrip()
    body = body.replace(DISCLAIMER, '').rstrip()
    body = body + '\n\n' + DISCLAIMER + '\n\n' + brief['suffix']
    title = brief['title']
    proposed = re.sub(r'[\U0001F300-\U0001FAFF\u2600-\u27BF]', '', raw['title']).strip().replace('\u2014', ': ')
    boring = re.search(r'volume[- ]driven|volume (?:context|analysis|outlook)|market observations|heuristic|\banalysis$', proposed, re.I)
    if (10 <= len(proposed) <= 110 and not boring
            and title_key(proposed) not in brief.get('_prior_title_keys', ())):
        title = proposed if '$' + evidence['symbol'] in proposed else '$' + evidence['symbol'] + ': ' + proposed
    return {**raw, 'title': title, 'body': body, 'content_rendered_version': REVISION,
            'presentation_suffix': brief['suffix']}


def provenance(event, brief):
    evidence = {'source': event['metrics']['source'], 'timestamp': stamp(event['as_of']),
                'event_digest': digest(event), 'related': brief['related']}
    evidence['digest'] = digest(evidence)
    return evidence


def valid_provenance(draft):
    stored, event = draft.get('internal_evidence'), draft.get('event')
    if draft.get('content_mix_version') != REVISION or not isinstance(stored, dict) or not event:
        return False
    expected = {'source': event['metrics']['source'], 'timestamp': stamp(event['as_of']),
                'event_digest': digest(event), 'related': stored.get('related', [])}
    return {**expected, 'digest': digest(expected)} == stored


def commit_progress(db, brief):
    progress = db.state('content_mix_progress', {})
    progress['total_drafts'] = int(progress.get('total_drafts', progress.get('market_drafts', 0))) + 1
    progress['market_drafts'] = int(progress.get('market_drafts', 0)) + 1
    progress['mix_slots'] = int(progress.get('mix_slots', 0)) + int(brief['counts_as_mix_slot'])
    progress['proof_shown'] = int(progress.get('proof_shown', 0)) + int(brief['public_proof'])
    progress['last_style'] = brief['style']
    db.set('content_mix_progress', progress)


def commit_education(db):
    progress = db.state('content_mix_progress', {})
    progress['total_drafts'] = int(progress.get('total_drafts', progress.get('market_drafts', 0))) + 1
    db.set('content_mix_progress', progress)
