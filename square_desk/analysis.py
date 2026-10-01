"""Closed-candle studies and conservative, explicitly heuristic watchlist setups."""
from statistics import mean
from .models import uid, utc

WINDOWS = {'15m': 900, '1h': 3600, '4h': 14400, '12h': 43200,
           '24h': 86400, '3d': 259200, '7d': 604800}


def ema(values, period):
    result = [values[0]]
    alpha = 2 / (period + 1)
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def rsi(values, period=14):
    diffs = [b - a for a, b in zip(values, values[1:])]
    gain = mean(max(d, 0) for d in diffs[:period])
    loss = mean(max(-d, 0) for d in diffs[:period])
    for d in diffs[period:]:
        gain = (gain * (period - 1) + max(d, 0)) / period
        loss = (loss * (period - 1) + max(-d, 0)) / period
    return 100 if loss == 0 and gain else (50 if gain == loss == 0 else 100 - 100 / (1 + gain / loss))


def changes(snapshot, slow=None):
    output = {}
    for label, seconds in WINDOWS.items():
        series = snapshot if seconds <= 86400 else slow
        if series is None:
            output[label] = None
            continue
        target = series.as_of - seconds
        matches = [c for c in series.candles if abs(c.end - target) < .01]
        output[label] = (series.candles[-1].close / matches[-1].close - 1) * 100 if matches else None
    return output


def studies(snapshot):
    candles = snapshot.candles
    close = [c.close for c in candles]
    tr = [max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)) for p, c in zip(candles, candles[1:])]
    atr = mean(tr[:14])
    for value in tr[14:]:
        atr = (atr * 13 + value) / 14
    prior = candles[-21:-1]
    volume = mean(c.volume for c in prior)
    fast, medium, slow = ema(close, 12), ema(close, 26), ema(close, 50)
    macd = [a - b for a, b in zip(fast, medium)]
    total_volume = sum(c.volume for c in candles[-96:])
    return {'price': close[-1], 'atr': atr, 'rsi': rsi(close), 'ema20': ema(close, 20)[-1],
            'ema50': slow[-1], 'macd': macd[-1], 'macd_signal': ema(macd, 9)[-1],
            'support': min(c.low for c in prior), 'resistance': max(c.high for c in prior),
            'relative_volume': candles[-1].volume / volume if volume else None,
            'quote_volume_24h': sum(c.close * c.volume for c in candles[-96:]),
            'vwap_24h': sum((c.high + c.low + c.close) / 3 * c.volume for c in candles[-96:]) / total_volume if total_volume else None,
            'source': snapshot.source, 'quote': snapshot.quote, 'as_of': snapshot.as_of}


def rank(rows, window='24h', reverse=True):
    return sorted([r for r in rows if r['changes'].get(window) is not None],
                  key=lambda r: r['changes'][window], reverse=reverse)


def regime(rows):
    btc = next((r for r in rows if r['symbol'] == 'BTC'), None)
    eth = next((r for r in rows if r['symbol'] == 'ETH'), None)
    eligible = [r['changes']['1h'] for r in rows if r['changes'].get('1h') is not None]
    breadth = sum(x > 0 for x in eligible) / len(eligible) if eligible else None
    change = btc['changes'].get('1h') if btc else None
    name = 'unknown' if change is None else ('risk_off' if change < -1 else ('risk_on' if change > 1 else 'range'))
    return {'name': name, 'btc_1h': change, 'eth_1h': eth['changes'].get('1h') if eth else None,
            'watchlist_breadth': breadth, 'sample': len(eligible)}


def setup(row, context, settings):
    m = row['metrics']
    rv = m['relative_volume']
    if rv is None or rv < settings.min_relative_volume or m['quote_volume_24h'] < settings.min_quote_volume:
        return None
    if context['btc_1h'] is None or context['eth_1h'] is None:
        return None
    long = m['price'] > m['resistance'] and m['ema20'] > m['ema50'] and 50 <= m['rsi'] <= 75
    short = m['price'] < m['support'] and m['ema20'] < m['ema50'] and 25 <= m['rsi'] <= 50
    if not (long or short) or m['atr'] <= 0:
        return None
    if (long and context['name'] == 'risk_off') or (short and context['name'] == 'risk_on'):
        return None
    score = min(95, 60 + min(rv, 4) * 5 + (10 if context['name'] != 'range' else 5))
    if score < settings.min_confidence:
        return None
    direction = 1 if long else -1
    level = m['resistance'] if long else m['support']
    entry = level + direction * .1 * m['atr']
    stop = level - direction * m['atr']
    risk = abs(entry - stop)
    return {'id': uid(), 'symbol': row['symbol'], 'direction': 'long' if long else 'short',
            'type': 'breakout_retest' if long else 'breakdown_retest', 'classification': 'potential_setup',
            'current_price': m['price'], 'entry': entry, 'entry_zone': [entry - .1 * m['atr'], entry + .1 * m['atr']],
            'stop': stop, 'invalidation': stop, 'target1': entry + direction * 2 * risk,
            'target2': entry + direction * 3 * risk, 'risk_reward': 2, 'confidence': score,
            'confidence_method': 'heuristic screening score, not a calibrated probability',
            'support': m['support'], 'resistance': m['resistance'], 'atr': m['atr'], 'rsi': m['rsi'],
            'relative_volume': rv, 'context': context, 'derivatives': None,
            'source': m['source'], 'quote': m['quote'], 'as_of': m['as_of'], 'created_at': utc(),
            'expires_at': utc() + 14400, 'last_bar': m['as_of'], 'triggered_at': None,
            'mfe_r': 0, 'mae_r': 0, 'result_r': None, 'target1_reached': False,
            'reason': 'Closed-candle range break with volume expansion; watch for a retest. Short is research only, not spot execution.'}


def events(row, settings):
    m, c = row['metrics'], row['changes']
    observations = []
    if c.get('15m') is not None and abs(c['15m']) >= settings.shock_percent:
        observations.append(('shock', 100, 'risk'))
    if m['relative_volume'] is not None and m['relative_volume'] >= 2:
        observations.append(('volume', 70, 'volume'))
    if m['price'] > m['resistance'] or m['price'] < m['support']:
        observations.append(('range_break', 60, 'structure'))
    if c.get('24h') is not None and abs(c['24h']) >= 5:
        observations.append(('mover', 50, 'momentum'))
    return [{'symbol': row['symbol'], 'category': cat, 'priority': priority, 'angle': angle,
             'metrics': m, 'changes': c, 'as_of': m['as_of'],
             'event_key': f"{row['symbol']}:{cat}:{int(m['as_of'] // 14400)}"}
            for cat, priority, angle in observations]
