"""Append-only paper outcomes, source consistency, fees and conservative bar ambiguity."""
from .models import utc


def record_setup(store, signal):
    key = f"{signal['symbol']}:{signal['source']}:{signal['type']}:{int(signal['as_of'] // 14400)}"
    return store.insert('signal', signal, 'watching', fingerprint=key, id=signal['id'])


def advance(store, snapshot, settings):
    for row in store.list('signal', ['watching', 'triggered'], limit=10000):
        p = row['payload']
        if p['symbol'] != snapshot.symbol or p['source'] != snapshot.source or p['quote'] != snapshot.quote:
            continue
        if snapshot.candles[0].start > p['last_bar']:
            store.update(row['id'], status='expired_unresolved')
            continue
        status = row['status']
        risk = abs(p['entry'] - p['stop'])
        sign = 1 if p['direction'] == 'long' else -1
        for c in snapshot.candles:
            if c.end <= p['last_bar'] or c.start < p['as_of']:
                continue
            p['last_bar'] = c.end
            if status == 'watching':
                invalid = c.low <= p['stop'] if sign == 1 else c.high >= p['stop']
                if c.end >= p['expires_at'] or invalid:
                    status = 'invalidated'
                    break
                if c.low <= p['entry'] <= c.high:
                    status, p['triggered_at'] = 'triggered', c.end
                    # Ordering inside the entry candle is unknowable. No same-bar
                    # target wins; a simultaneous invalidation was rejected above.
                    continue
            else:
                favorable = c.high - p['entry'] if sign == 1 else p['entry'] - c.low
                adverse = p['entry'] - c.low if sign == 1 else c.high - p['entry']
                p['mfe_r'] = max(p['mfe_r'], favorable / risk)
                p['mae_r'] = max(p['mae_r'], adverse / risk)
                stop_hit = c.low <= p['stop'] if sign == 1 else c.high >= p['stop']
                one_hit = c.high >= p['target1'] if sign == 1 else c.low <= p['target1']
                two_hit = c.high >= p['target2'] if sign == 1 else c.low <= p['target2']
                # Stop first if both exits occur in a bar. Full position exits at
                # target one; target two is a reference, not a second realized win.
                if stop_hit:
                    stop_fill = min(c.open, p['stop']) if sign == 1 else max(c.open, p['stop'])
                    status, gross = 'stopped', sign * (stop_fill - p['entry']) / risk
                elif one_hit:
                    p['target1_reached'], p['target2_reached'] = True, two_hit
                    status, gross = 'target1', 2
                elif c.end >= p['expires_at']:
                    status, gross = 'time_exit', sign * (c.close - p['entry']) / risk
                else:
                    continue
                cost = (settings.fee_bps + settings.slippage_bps) * 2 / 10000 * p['entry'] / risk
                p['result_r'] = gross - cost
                p['closed_at'] = c.end
                p['simulation_model'] = 'OHLC stop-first, full target-one exit, fees/slippage, no funding'
                break
        if status in ('watching', 'triggered') and utc() > p['expires_at'] and snapshot.as_of <= p['last_bar']:
            # Missing coverage cannot be turned into a fabricated stop/win.
            status = 'expired_unresolved'
        store.update(row['id'], status=status, payload=p)


def statistics(rows):
    closed = [r for r in rows if r['payload'].get('result_r') is not None]
    values = [r['payload']['result_r'] for r in closed]
    total, peak, drawdown = 0, 0, 0
    for row in sorted(closed, key=lambda r: r['payload'].get('closed_at', r['updated'])):
        total += row['payload']['result_r']
        peak = max(peak, total)
        drawdown = max(drawdown, peak - total)
    groups = {}
    for row in closed:
        for key in ('type', 'regime'):
            name = row['payload']['context']['name'] if key == 'regime' else row['payload']['type']
            groups.setdefault(key + ':' + name, []).append(row['payload']['result_r'])
    return {'generated': len(rows), 'triggered': sum(r['payload'].get('triggered_at') is not None for r in rows),
            'invalidated': sum(r['status'] == 'invalidated' for r in rows),
            'target1': sum(r['status'] == 'target1' for r in rows),
            'target2_observed': sum(r['payload'].get('target2_reached', False) for r in rows),
            'stopped': sum(r['status'] == 'stopped' for r in rows), 'closed': len(closed),
            'unresolved': sum(r['status'] == 'expired_unresolved' for r in rows),
            'win_rate': sum(x > 0 for x in values) / len(values) if values else None,
            'expectancy_r': sum(values) / len(values) if values else None,
            'max_drawdown_r': drawdown if values else None,
            'mean_mfe_r': sum(r['payload']['mfe_r'] for r in closed) / len(closed) if closed else None,
            'mean_mae_r': sum(r['payload']['mae_r'] for r in closed) / len(closed) if closed else None,
            'mean_risk_reward': sum(r['payload']['risk_reward'] for r in rows) / len(rows) if rows else None,
            'by_group': {k: {'count': len(v), 'expectancy_r': sum(v) / len(v)} for k, v in groups.items()},
            'units': 'R per simulated full-position trade; no portfolio sizing or realized account returns'}
