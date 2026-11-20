"""Reconcile abandoned pre-publication jobs under the live database lease."""
import math
from .models import utc

STATES = ('chart_rendering', 'uploading_image')


def owned(store, owner):
    lease = store.state('worker_lease', {})
    until = lease.get('until') if isinstance(lease, dict) else None
    return (isinstance(lease, dict) and lease.get('owner') == owner
            and type(until) in (int, float) and math.isfinite(until) and until > utc())


def reconcile(store, settings, owner, active=lambda status: False, now=None):
    now = utc() if now is None else now
    if not owner:
        return []
    rows = store.list('draft', STATES, limit=100)
    candidates = []
    for row in rows:
        p = row['payload']
        if active(row['status']) or p.get('submitted_at') or p.get('receipt'):
            continue
        at = row.get('updated')
        if type(at) not in (int, float) or not math.isfinite(at) or now - at < 300:
            continue
        candidates.append(row)
    if not candidates:
        return []
    recovered = []
    with store.transaction():
        if not owned(store, owner):
            return []
        for old in candidates:
            current = store.get(old['id'])
            if not current or any(current.get(k) != old.get(k) for k in ('status', 'updated', 'payload')):
                continue
            if active(current['status']):
                continue
            if not owned(store, owner):
                break
            p = dict(current['payload'])
            at = (p.get('event') or {}).get('as_of')
            stale = type(at) in (int, float) and math.isfinite(at) and now - at > settings.draft_max_age
            state = 'expired' if stale else 'review'
            p.update(approved_at=None, human_review_required=True,
                     media_recovery={'at': now, 'previous_status': current['status'],
                                     'reason': 'stale_market_evidence' if stale else 'abandoned_media_job'})
            store.update(current['id'], status=state, payload=p, clear_due=True)
            store.log('IMAGE', 'Abandoned pre-publication media job reconciled; status=' + state, current['id'])
            recovered.append({'id': current['id'], 'status': state})
    return recovered
