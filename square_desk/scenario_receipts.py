"""Review-only receipts for exact, confirmed published scenarios; never trades."""
import math
from .models import digest, stamp, utc

KIND = 'published_scenario'
TERMINAL = ('target1', 'stopped', 'time_exit', 'invalidated', 'expired_unresolved')
VERSION = 'published-scenario-v31'


def outcome_binding(value):
    return digest({k: v for k, v in value.items() if k != 'review_created'})


def capture(store, row, now=None):
    """Idempotent after remote confirmation. Never reuse a paper signal's levels."""
    now = utc() if now is None else now
    p = row['payload']
    event = p.get('event') or {}
    receipt = p.get('receipt') or {}
    if (row['status'] != 'published' or not receipt.get('id')
            or event.get('category') != 'setup' or p.get('archived')):
        return None
    if (not isinstance(event.get('metrics'), dict) or not isinstance(event.get('changes'), dict)
            or not all(k in event['metrics'] for k in ('entry', 'stop', 'target1', 'target2', 'confidence', 'source', 'quote'))
            or not all(k in event for k in ('symbol', 'as_of', 'direction'))):
        return None
    ident = digest([VERSION, row['id']])[:32]
    if store.get(ident):
        return ident
    from .content import signal_text_errors, facts
    if signal_text_errors(p['title'], p['body'], facts(event), event.get('direction')):
        return None
    published_at = p.get('submitted_at', row['updated'])
    expiry = min(event['as_of'] + 14400, event.get('signal_expires_at', event['as_of'] + 14400))
    if not event['as_of'] <= published_at <= now or not published_at < expiry:
        return None
    try:
        # The visible text uses six significant figures. Track those exact
        # published levels, not the higher-precision or original paper values.
        levels = {k: float(f"{event['metrics'][k]:.6g}") for k in ('entry', 'stop', 'target1', 'target2')}
        if not all(math.isfinite(v) and v > 0 for v in levels.values()):
            return None
        sign = 1 if event['direction'] == 'long' else -1
        risk = sign * (levels['entry'] - levels['stop'])
        if risk <= 0 or sign * (levels['target1'] - levels['entry']) <= 0:
            return None
    except (KeyError, ValueError, TypeError):
        return None
    value = {**levels, 'symbol': event['symbol'], 'quote': event['metrics']['quote'],
             'source': event['metrics']['source'], 'direction': event['direction'],
             'type': KIND, 'context': {'name': 'published scenario'},
             'risk_reward': abs(levels['target1'] - levels['entry']) / risk,
             'as_of': published_at, 'last_bar': published_at, 'expires_at': expiry,
             'created_at': now, 'triggered_at': None, 'mfe_r': 0, 'mae_r': 0,
             'tracked_candles': [],
             'result_r': None, 'target1_reached': False,
             'publication_id': row['id'], 'receipt_id': str(receipt['id']),
             'published_text_hash': digest([p['title'], p['body']]), 'version': VERSION}
    store.insert(KIND, value, 'watching', id=ident, fingerprint=ident)
    return ident


def payload(row, compact=False):
    p, status = row['payload'], row['status']
    explanation = {
        'target1': 'The first target was touched after a later candle recorded an entry touch.',
        'stopped': 'The stop condition was met after an entry touch; a gap can worsen the simulated exit.',
        'time_exit': 'The observation window ended after an entry touch, without a prior recorded exit.',
        'invalidated': 'The scenario expired or met invalidation before a separately confirmed entry touch.',
        'expired_unresolved': 'Coverage was insufficient to assign a supported outcome; this remains unresolved.',
    }[status]
    result = ('No simulated return is assigned.' if p.get('result_r') is None else
              f"Simulated full-position result: {p['result_r']:.6g} R after modelled fees and slippage; funding excluded.")
    body = (f"${p['symbol']}: this is a review of a previously published conditional {p['direction']} scenario, not a trade receipt.\n\n"
            f"{explanation} {result}\n\n"
            f"Published reference entry: {p['entry']:.6g} {p['quote']}; invalidation stop: {p['stop']:.6g}; "
            f"first target: {p['target1']:.6g}. Original publication time: {stamp(p['as_of'])}. "
            f"Evidence venue: {p['source']} spot.\n\n"
            'Only closed candles beginning after publication are eligible. An entry touch is a paper observation, '
            'not proof that an order filled. The entry candle cannot establish a target win. If both exits are '
            'touched in a later candle, the model checks the stop first because intrabar order is unknown.\n\n'
            'The useful lesson is the difference between a stated failure condition and a later result. Candles '
            'can document a level touch; they cannot establish who traded, why price moved, or whether anyone '
            'made money. A target touch does not validate every part of the original explanation, and a stop '
            'does not prove the opposite explanation.\n\n'
            'Apply the same review standard to targets, stops, time exits and unresolved cases. This record '
            'uses the published levels rather than substituting a different paper signal or retrospectively '
            'improving the entry.\n\n'
            'This is evidence about the stated scenario, not a new entry instruction. '
            'Simulated research only; no realized account performance or recommendation.')
    if compact:
        paragraphs = body.split('\n\n')
        body = '\n\n'.join(p for p in paragraphs if not p.startswith('The useful lesson'))
    return {'title': f"${p['symbol']}: Conditional Scenario Review", 'body': body,
            'category': 'scenario_audit', 'article': False, 'priority': 40,
            'risk': 'high', 'human_review_required': True, 'generated_by': 'scenario_ledger',
            'approved_at': None, 'image': None,
            'scenario_review': {'id': row['id'], 'binding': outcome_binding(p), 'status': status,
                                'version': VERSION, 'compact': compact}}


def check(store, draft):
    binding = draft.get('scenario_review') or {}
    row = store.get(binding.get('id'))
    if (not row or row['kind'] != KIND or row['status'] not in TERMINAL
            or type(binding.get('compact')) is not bool
            or binding != payload(row, binding.get('compact', False))['scenario_review']):
        return ['Published scenario receipt source changed or unavailable']
    expected = payload(row, binding['compact'])
    if any(draft.get(k) != expected[k] for k in ('title', 'body', 'category', 'generated_by')):
        return ['Published scenario receipt must match its recorded outcome']
    return []


def build_reviews(store, checker, limit=1):
    """Chronological, both positive and negative; never approve or reserve a slot."""
    # A symbol may leave the discovered universe. Lack of later observations
    # must not leave its public scenario active indefinitely or fabricate a stop.
    for active in store.list(KIND, ['watching', 'triggered'], limit=1000):
        if utc() > active['payload']['expires_at']:
            p = active['payload']
            p['result_r'] = None
            p['coverage_limit'] = 'No supported terminal outcome before the observation window expired'
            store.update(active['id'], status='expired_unresolved', payload=p)
    pending = store.list('draft', ['review'], limit=50)
    if len(pending) >= 45 or any(r['payload'].get('category') == 'scenario_audit' for r in pending):
        return []
    rows = sorted(store.list(KIND, list(TERMINAL), limit=1000), key=lambda r: (r['updated'], r['id']))
    made = []
    for row in rows:
        ident = digest([VERSION, 'review', row['id']])[:32]
        if store.get(ident) or row['payload'].get('review_created'):
            continue
        draft = payload(row)
        if checker.check(draft):
            draft = payload(row, compact=True)
            if checker.check(draft):
                continue
        with store.transaction():
            if store.get(ident):
                continue
            store.insert('draft', draft, 'review', id=ident, fingerprint=ident)
            p = row['payload']
            p['review_created'] = ident
            store.update(row['id'], payload=p)
        made.append(ident)
        if len(made) >= limit:
            break
    return made
