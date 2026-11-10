"""Opt-in perpetual context, separately sourced from spot candles."""
import asyncio
import math
import re
from typing import Protocol
from .db_actor import run_db
from .models import utc
from .providers import ProviderError

VERSION = 'perpetual-context-v31'


class DerivativesProvider(Protocol):
    async def context(self, symbol: str) -> dict: ...


def valid_context(value, symbol, now=None):
    now = utc() if now is None else now
    try:
        if (not isinstance(value, dict) or value.get('version') != VERSION
                or value.get('symbol') != symbol or value.get('source') != 'binance_usdt_perpetual'
                or value.get('quote') != 'USDT'
                or not 0 <= now - value['as_of'] <= 300
                or not 0 <= now - value['fetched_at'] <= 300):
            return False
        for key in ('mark_price', 'funding_rate', 'funding_rate_percent', 'open_interest_base_units'):
            if type(value.get(key)) not in (float, int) or not math.isfinite(value[key]):
                return False
        if (value['mark_price'] <= 0 or value['open_interest_base_units'] < 0
                or not math.isclose(value['funding_rate_percent'], value['funding_rate'] * 100,
                                    rel_tol=1e-12, abs_tol=1e-12)):
            return False
        if value.get('open_interest_change_1h') is not None:
            return (type(value['open_interest_change_1h']) in (float, int)
                    and math.isfinite(value['open_interest_change_1h'])
                    and value['open_interest_change_1h'] >= -100
                    and 0 <= now - value['oi_history_as_of'] <= 7200)
        return True
    except (KeyError, TypeError, ValueError):
        return False


def content_context(event, now=None):
    value = event.get('derivatives')
    if (valid_context(value, event.get('symbol'), now)
            and event.get('metrics', {}).get('quote') == 'USDT'
            and abs(value['as_of'] - event['as_of']) <= 1200):
        return value
    return None


def text_errors(event, text, now=None):
    context = content_context(event, now)
    funding = re.search(r'\bfunding(?:\s+rate)?\s+(?:is|was|remains|turned)\s+(positive|negative)', text, re.I)
    oi = re.search(r'\bopen interest\s+(?:is\s+|has\s+)?(rising|falling|increased|decreased)', text, re.I)
    if not context:
        return ['Derivative assertion has no fresh matching evidence'] if funding or oi else []
    errors = []
    if re.search(r'\bfunding|\bopen interest', text, re.I) and 'Binance USDT perpetual' not in text:
        errors.append('Derivative claims require perpetual venue and unit disclosure')
    if funding:
        sign = 'positive' if context['funding_rate'] > 0 else 'negative' if context['funding_rate'] < 0 else 'zero'
        if funding[1].lower() != sign:
            errors.append('Funding direction contradicts recorded evidence')
    if oi:
        delta = context.get('open_interest_change_1h')
        positive = oi[1].lower() in ('rising', 'increased')
        if delta is None or (delta <= 0 if positive else delta >= 0):
            errors.append('Open interest direction contradicts recorded evidence')
    number = r'([-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)'
    for match in re.finditer(r'\bfunding(?:\s+rate)?\s*(?:is\s+|at\s+|[:=]\s*)?' + number +
                             r'\s*(%|percent|fraction)?', text, re.I):
        unit = (match[2] or '').lower()
        expected = context['funding_rate_percent'] if unit in ('%', 'percent') else context['funding_rate']
        if not unit or not math.isclose(float(match[1]), float(f'{expected:.6g}'), rel_tol=1e-6, abs_tol=1e-12):
            errors.append('Funding value or percentage units contradict recorded evidence')
    return list(dict.fromkeys(errors))


class BinanceDerivatives:
    HOST = 'https://fapi.binance.com'

    def __init__(self, transport, store):
        self.http, self.db = transport, store
        self.lock = asyncio.Lock()
        self.failed_until = {}

    async def for_event(self, event):
        # A Coinbase USD fallback must remain usable without a USDT conversion
        # or an irrelevant futures request. Unusable optional context is omitted.
        value = None
        if event.get('metrics', {}).get('quote') == 'USDT':
            value = await self.context(event['symbol'])
        enriched = {**event, 'derivatives': value}
        if value and not content_context(enriched):
            enriched['derivatives'] = None
        return enriched

    async def context(self, symbol):
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9]{2,12}', symbol):
            return None
        async with self.lock:
            key = 'derivatives:' + symbol
            cached = await run_db(self.db, lambda: self.db.state(key))
            if valid_context(cached, symbol):
                return cached
            if utc() < self.failed_until.get(symbol, 0):
                return None
            try:
                value = await asyncio.wait_for(self._fetch(symbol), timeout=8)
                await run_db(self.db, lambda: self.db.set(key, value))
                return value
            except (ProviderError, asyncio.TimeoutError, ValueError, TypeError, KeyError, IndexError, OverflowError):
                # Transport enforces host-wide 418/429 Retry-After; no bypass.
                self.failed_until = {k: v for k, v in self.failed_until.items() if v > utc()}
                self.failed_until[symbol] = utc() + 300
                await run_db(self.db, lambda: self.db.log('DATA',
                    'Derivatives unavailable for ' + symbol + '; spot drafting continues without substitutes'))
                return None

    async def _fetch(self, symbol):
        pair = symbol + 'USDT'
        mark = await self.http.get(self.HOST, '/fapi/v1/premiumIndex', {'symbol': pair})
        oi = await self.http.get(self.HOST, '/fapi/v1/openInterest', {'symbol': pair})
        hist = await self.http.get(self.HOST, '/futures/data/openInterestHist',
                                   {'symbol': pair, 'period': '1h', 'limit': 2})
        now = utc()
        if mark['symbol'] != pair or oi['symbol'] != pair:
            raise ValueError('Derivative symbol mismatch')
        times = [float(mark['time']) / 1000, float(oi['time']) / 1000]
        if not all(math.isfinite(t) and 0 <= now - t <= 300 for t in times):
            raise ValueError('Derivative timestamp stale or future')
        value = {'version': VERSION, 'symbol': symbol, 'quote': 'USDT',
                 'source': 'binance_usdt_perpetual', 'as_of': min(times), 'fetched_at': now,
                 'mark_price': float(mark['markPrice']), 'funding_rate': float(mark['lastFundingRate']),
                 'open_interest_base_units': float(oi['openInterest']), 'liquidations': None,
                 'open_interest_change_1h': None}
        value['funding_rate_percent'] = value['funding_rate'] * 100
        if isinstance(hist, list) and len(hist) == 2:
            a, b = sorted(hist, key=lambda r: float(r['timestamp']))
            old, new = float(a['sumOpenInterest']), float(b['sumOpenInterest'])
            ta, tb = float(a['timestamp']) / 1000, float(b['timestamp']) / 1000
            if (a.get('symbol') == pair and b.get('symbol') == pair
                    and all(math.isfinite(v) for v in (old, new, ta, tb))
                    and old > 0 and new >= 0 and abs(tb - ta - 3600) < .001
                    and 0 <= now - tb <= 7200):
                value['open_interest_change_1h'] = (new / old - 1) * 100
                value['oi_history_as_of'] = tb
        if not valid_context(value, symbol, now):
            raise ValueError('Invalid derivative evidence')
        return value
