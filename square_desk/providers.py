"""Public market adapters with source provenance, cooldowns, and strict freshness."""
from collections import defaultdict
from datetime import datetime, timezone
from typing import Protocol
import asyncio
import time
import math
import re
import httpx
from .models import Candle, Snapshot, utc


class ProviderError(Exception):
    pass


class MarketDataProvider(Protocol):
    name: str
    async def candles(self, symbol: str, interval: int) -> Snapshot: ...


class Transport:
    def __init__(self, client=None):
        self.client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
        self.locks = defaultdict(asyncio.Lock)
        self.next_at = defaultdict(float)
        self.health = {}
        self.slots = defaultdict(lambda: asyncio.Semaphore(2))
        self.spacing_seconds = 1.1
        self.attempt_seconds = 6

    async def pace(self, host):
        # Only request starts are serialized. A slow socket must not own the
        # pacing lock through retries and block unrelated fresh observations.
        async with self.locks[host]:
            while True:
                delay = self.next_at[host] - time.monotonic()
                if delay > 10:
                    raise ProviderError('Provider cooling down')
                if delay <= 0:
                    self.next_at[host] = time.monotonic() + self.spacing_seconds
                    return
                await asyncio.sleep(delay)

    async def get(self, host, path, params=None):
        for attempt in range(2):
            async with self.slots[host]:
                await self.pace(host)
                try:
                    response = await asyncio.wait_for(self.client.get(host + path, params=params,
                        timeout=self.attempt_seconds), timeout=self.attempt_seconds)
                    code = response.status_code
                    if code in (401, 403, 418, 451, 429):
                        try:
                            delay = max(60, float(response.headers.get('Retry-After', '3600')))
                        except ValueError:
                            delay = 3600
                        self.next_at[host] = max(self.next_at[host], time.monotonic() + delay)
                        self.health[host] = {'status': 'restricted' if code != 429 else 'rate_limited', 'http': code, 'at': utc()}
                        raise ProviderError(f'HTTP {code}; request suspended')
                    if code >= 500:
                        raise httpx.ConnectError('Upstream unavailable')
                    if code != 200:
                        raise ProviderError(f'HTTP {code}')
                    data = response.json()
                    self.health[host] = {'status': 'ok', 'at': utc()}
                    return data
                except (httpx.HTTPError, ValueError, asyncio.TimeoutError):
                    pass
            if attempt == 0:
                await asyncio.sleep(1)
        self.health[host] = {'status': 'unavailable', 'at': utc()}
        raise ProviderError('Provider unavailable after bounded retries')


class BinanceProvider:
    name = 'binance'
    host = 'https://data-api.binance.vision'

    def __init__(self, transport):
        self.http = transport
        self.metadata = None

    async def liquid_symbols(self, limit, min_quote_volume):
        metadata = await self.http.get(self.host, '/api/v3/exchangeInfo')
        if not isinstance(metadata, dict) or 'rateLimits' not in metadata:
            raise ProviderError('Missing official market metadata')
        self.metadata = metadata
        stable = {'USDT', 'USDC', 'FDUSD', 'TUSD', 'DAI', 'USDP', 'BUSD', 'USD1', 'USDE', 'EUR', 'AEUR', 'EURI'}
        eligible = {}
        for pair in metadata.get('symbols', []):
            base = pair.get('baseAsset', '')
            if (pair.get('quoteAsset') == 'USDT' and pair.get('status') == 'TRADING'
                    and pair.get('isSpotTradingAllowed', False) and base not in stable
                    and re.fullmatch(r'[A-Z0-9]{2,12}', base)
                    and not base.endswith(('UP', 'DOWN', 'BULL', 'BEAR'))):
                eligible[pair['symbol']] = base
        tickers = await self.http.get(self.host, '/api/v3/ticker/24hr', {'type': 'MINI'})
        if not isinstance(tickers, list):
            raise ProviderError('Invalid market discovery response')
        ranked = []
        for ticker in tickers:
            base = eligible.get(ticker.get('symbol'))
            try:
                volume = float(ticker.get('quoteVolume', 0))
            except (TypeError, ValueError):
                continue
            if base and math.isfinite(volume) and volume >= min_quote_volume:
                ranked.append((volume, base))
        ranked.sort(reverse=True)
        symbols = [base for _, base in ranked[:limit]]
        if not symbols:
            raise ProviderError('No liquid spot symbols discovered')
        return symbols

    async def candles(self, symbol, interval):
        if self.metadata is None:
            self.metadata = await self.http.get(self.host, '/api/v3/exchangeInfo')
            if 'rateLimits' not in self.metadata:
                raise ProviderError('Missing official rate-limit metadata')
        pair = symbol + 'USDT'
        if not any(x['symbol'] == pair and x['status'] == 'TRADING' for x in self.metadata['symbols']):
            raise ProviderError('Asset not available on this provider')
        rows = await self.http.get(self.host, '/api/v3/klines',
                                   {'symbol': pair, 'interval': {900: '15m', 3600: '1h'}[interval], 'limit': 200})
        now = utc()
        candles = [Candle(float(r[0]) / 1000, (float(r[6]) + 1) / 1000,
                          *map(float, r[1:6])) for r in rows if (float(r[6]) + 1) / 1000 <= now]
        return Snapshot(symbol, 'USDT', self.name, interval, now, candles)


class CoinbaseProvider:
    name = 'coinbase'
    host = 'https://api.exchange.coinbase.com'

    def __init__(self, transport):
        self.http = transport

    async def candles(self, symbol, interval):
        rows = await self.http.get(self.host, f'/products/{symbol}-USD/candles', {'granularity': interval})
        now = utc()
        candles = [Candle(float(r[0]), float(r[0]) + interval, float(r[3]), float(r[2]),
                          float(r[1]), float(r[4]), float(r[5])) for r in rows if float(r[0]) + interval <= now]
        return Snapshot(symbol, 'USD', self.name, interval, now, sorted(candles, key=lambda c: c.start))


class ProviderPool:
    def __init__(self, providers, store, max_age):
        self.providers, self.store, self.max_age = providers, store, max_age
        self.fetch_locks = defaultdict(asyncio.Lock)

    async def fetch(self, symbol, interval=900):
        # Shared scan/manual requests reuse a completed validated observation;
        # cancelling a waiter never cancels the request belonging to another job.
        async with self.fetch_locks[(symbol, interval)]:
            return await self._fetch(symbol, interval)

    async def _fetch(self, symbol, interval):
        key = f'cache:{symbol}:{interval}'
        cached = self.store.state(key)
        # Five minute cache TTL; original observation timestamp is never rewritten.
        if cached and utc() - cached['fetched_at'] < 300:
            try:
                snap = Snapshot.read(cached)
                snap.validate(utc(), self.max_age if interval == 900 else max(self.max_age, interval + 300))
                return snap
            except (ValueError, KeyError, TypeError):
                pass  # A recent fetch can contain an observation that has aged.
        for provider in self.providers:
            try:
                snap = await provider.candles(symbol, interval)
                snap.validate(utc(), self.max_age if interval == 900 else max(self.max_age, interval + 300))
                self.store.set(key, snap.dict())
                self.store.set('provider:' + provider.name, {'status': 'ok', 'at': utc()})
                return snap
            except (ProviderError, ValueError, KeyError, TypeError, IndexError):
                self.store.set('provider:' + provider.name, {'status': 'unavailable', 'at': utc()})
                self.store.log('DATA', f'{provider.name} rejected or unavailable for {symbol}/{interval}')
        if cached:
            snap = Snapshot.read(cached)
            snap.validate(utc(), self.max_age if interval == 900 else max(self.max_age, interval + 300))
            return snap
        raise ProviderError('No eligible fresh market observation')
