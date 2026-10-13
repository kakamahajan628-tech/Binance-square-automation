"""Optional, public, single-venue derivative context. Never invent liquidations."""
import math
from typing import Protocol
from .models import utc
from .providers import ProviderError


class DerivativesProvider(Protocol):
    async def context(self, symbol: str) -> dict: ...


class BinanceDerivatives:
    HOST = 'https://fapi.binance.com'

    def __init__(self, transport, store):
        self.http, self.db = transport, store

    async def context(self, symbol):
        key = 'derivatives:' + symbol
        cached = self.db.state(key)
        if cached and utc() - cached['fetched_at'] < 300 and utc() - cached['as_of'] < 300:
            return cached
        try:
            mark = await self.http.get(self.HOST, '/fapi/v1/premiumIndex', {'symbol': symbol + 'USDT'})
            oi = await self.http.get(self.HOST, '/fapi/v1/openInterest', {'symbol': symbol + 'USDT'})
            oi_hist = await self.http.get(self.HOST, '/futures/data/openInterestHist',
                                          {'symbol': symbol + 'USDT', 'period': '1h', 'limit': 2})
            as_of = min(float(mark['time']), float(oi['time'])) / 1000
            p = {'symbol': symbol, 'source': 'binance_usdt_perpetual', 'as_of': as_of, 'fetched_at': utc(),
                 'mark_price': float(mark['markPrice']), 'funding_rate': float(mark['lastFundingRate']),
                 'open_interest_base_units': float(oi['openInterest']), 'liquidations': None,
                 'open_interest_change_1h': None}
            if not 0 <= utc() - as_of <= 300:
                raise ValueError('Derivative timestamp stale')
            if len(oi_hist) == 2:
                a, b = sorted(oi_hist, key=lambda r: r['timestamp'])
                if float(a['sumOpenInterest']) > 0 and abs(b['timestamp'] - a['timestamp'] - 3600000) < 1 and utc() - b['timestamp'] / 1000 < 7200:
                    p['open_interest_change_1h'] = (float(b['sumOpenInterest']) / float(a['sumOpenInterest']) - 1) * 100
                    p['oi_history_as_of'] = b['timestamp'] / 1000
            if any(not math.isfinite(v) for v in p.values() if isinstance(v, (float, int))):
                raise ValueError('Nonfinite derivatives')
            if p['open_interest_base_units'] < 0 or p['mark_price'] <= 0:
                raise ValueError('Invalid derivatives')
            self.db.set(key, p)
            return p
        except (ProviderError, ValueError, TypeError, KeyError, IndexError):
            self.db.log('DATA', 'Derivatives unavailable for ' + symbol + '; no proxy or invented substitute')
            return None
