"""Public market adapters with source provenance, cooldowns, and strict freshness."""
from collections import defaultdict
from datetime import datetime, timezone
from typing import Protocol
import asyncio
import time
import math
import re
import httpx
import logging
from .models import Candle, Snapshot, utc


VALIDATION_CODES = {
    'At least 55 closed candles required': 'insufficient_closed_candles',
    'Nonfinite candle': 'nonfinite_candle',
    'Invalid OHLC': 'invalid_ohlc',
    'Invalid volume or candle time': 'invalid_candle_time_or_volume',
    'Incomplete candle': 'incomplete_candle',
    'Incorrect candle interval': 'incorrect_candle_interval',
    'Missing or duplicated candles; do not synthesize gaps': 'candle_history_gap',
    'Stale candles': 'stale_candles',
    'Provider cooling down': 'provider_cooling_down',
    'Asset not available on this provider': 'asset_unavailable',
    'Missing official rate-limit metadata': 'invalid_market_metadata',
    'Missing official market metadata': 'invalid_market_metadata',
    'Invalid market discovery response': 'invalid_market_response',
    'No liquid spot symbols discovered': 'no_liquid_symbols',
}
FAILURE_CODES = set(VALIDATION_CODES.values()) | {
    'request_timeout', 'connection_failed', 'pool_timeout', 'protocol_failed',
    'network_failed', 'invalid_json', 'upstream_unavailable', 'http_rejected',
    'rate_limited', 'access_restricted', 'provider_unavailable', 'invalid_market_response',
}


def failure_code(error):
    code = getattr(error, 'code', None)
    if code in FAILURE_CODES:
        return code
    if type(error) in (ValueError, ProviderError):
        return VALIDATION_CODES.get(str(error), 'provider_unavailable')
    if isinstance(error, (KeyError, TypeError, IndexError)):
        return 'invalid_market_response'
    return 'provider_unavailable'


class ProviderError(Exception):
    def __init__(self, message, *, code=None, http_status=None):
        super().__init__(message)
        self.code = code if code in FAILURE_CODES else VALIDATION_CODES.get(message, 'provider_unavailable')
        self.http_status = http_status if type(http_status) is int and 100 <= http_status <= 599 else None


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
        last_code, last_http = 'provider_unavailable', None
        for attempt in range(2):
            last_http, upstream_failure = None, False
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
                        reason = 'rate_limited' if code == 429 else 'access_restricted'
                        self.health[host] = {'status': 'restricted' if code != 429 else 'rate_limited',
                                             'http': code, 'code': reason, 'at': utc()}
                        raise ProviderError(f'HTTP {code}; request suspended', code=reason, http_status=code)
                    if code >= 500:
                        last_code, last_http = 'upstream_unavailable', code
                        upstream_failure = True
                        raise httpx.ConnectError('Upstream unavailable')
                    if code != 200:
                        self.health[host] = {'status': 'unavailable', 'code': 'http_rejected', 'http': code, 'at': utc()}
                        raise ProviderError(f'HTTP {code}', code='http_rejected', http_status=code)
                    last_http = code
                    data = response.json()
                    self.health[host] = {'status': 'ok', 'at': utc()}
                    return data
                except (httpx.HTTPError, ValueError, asyncio.TimeoutError) as error:
                    if isinstance(error, httpx.PoolTimeout): last_code = 'pool_timeout'
                    elif isinstance(error, (httpx.TimeoutException, asyncio.TimeoutError)): last_code = 'request_timeout'
                    elif isinstance(error, httpx.ConnectError): last_code = 'upstream_unavailable' if upstream_failure else 'connection_failed'
                    elif isinstance(error, httpx.ProtocolError): last_code = 'protocol_failed'
                    elif isinstance(error, ValueError): last_code = 'invalid_json'
                    elif not isinstance(error, httpx.ConnectError): last_code = 'network_failed'
            if attempt == 0:
                await asyncio.sleep(1)
        self.health[host] = {'status': 'unavailable', 'code': last_code, 'at': utc()}
        if last_http is not None: self.health[host]['http'] = last_http
        raise ProviderError('Provider unavailable after bounded retries', code=last_code, http_status=last_http)


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
        self.observed = {}
        self.last_logged = {}

    def record(self, provider, status, code, symbol, interval, http_status=None):
        # Fixed codes only. No exception text, URLs, response bodies or secrets.
        if provider not in ('binance', 'coinbase'): return
        row = {'status': status, 'code': code if code in FAILURE_CODES | {'fresh_observation', 'fresh_cache'} else 'provider_unavailable',
               'at': utc(), 'symbol': symbol if re.fullmatch(r'[A-Z0-9]{2,12}', str(symbol)) else 'unknown',
               'interval': interval if interval in (900, 3600) else 0}
        if type(http_status) is int and 100 <= http_status <= 599: row['http_status'] = http_status
        previous = self.observed.get(provider, {})
        if previous.get('last_failure'): row['last_failure'] = dict(previous['last_failure'])
        if status != 'ok': row['last_failure'] = {k: v for k, v in row.items() if k != 'last_failure'}
        self.observed[provider] = row
        if status != 'ok':
            key = (provider, row['code'])
            if time.monotonic() - self.last_logged.get(key, -math.inf) >= 300:
                self.last_logged[key] = time.monotonic()
                logging.getLogger('square_desk').warning('MARKET_DATA provider=%s code=%s http=%s symbol=%s interval=%s',
                    provider, row['code'], row.get('http_status', 'none'), row['symbol'], row['interval'])

    def observations(self, *, compact=False):
        result = {}
        for provider in self.providers:
            name = provider.name
            if name not in ('binance', 'coinbase'): continue
            row = dict(self.observed.get(name, {'status': 'not_checked'}))
            if compact: row.pop('last_failure', None)
            elif row.get('last_failure'): row['last_failure'] = dict(row['last_failure'])
            transport = getattr(provider, 'http', None)
            health = dict(getattr(transport, 'health', {}).get(getattr(provider, 'host', None), {}))
            if not compact and health:
                row['transport'] = {key: health[key] for key in ('status', 'code', 'http', 'at') if key in health}
            until = getattr(transport, 'next_at', {}).get(getattr(provider, 'host', None), 0)
            remaining = until - time.monotonic()
            if math.isfinite(remaining) and remaining > 10: row['cooldown_remaining_seconds'] = int(min(remaining, 86400))
            result[name] = row
        return result

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
                self.record(snap.source, 'ok', 'fresh_cache', symbol, interval)
                return snap
            except (ValueError, KeyError, TypeError):
                pass  # A recent fetch can contain an observation that has aged.
        for provider in self.providers:
            try:
                snap = await provider.candles(symbol, interval)
                snap.validate(utc(), self.max_age if interval == 900 else max(self.max_age, interval + 300))
                self.record(provider.name, 'ok', 'fresh_observation', symbol, interval)
                self.store.set(key, snap.dict())
                self.store.set('provider:' + provider.name, {'status': 'ok', 'at': utc()})
                return snap
            except (ProviderError, ValueError, KeyError, TypeError, IndexError) as error:
                code, http_status = failure_code(error), getattr(error, 'http_status', None)
                if code == 'provider_cooling_down':
                    health = getattr(getattr(provider, 'http', None), 'health', {}).get(getattr(provider, 'host', None), {})
                    code = health.get('code', code)
                    http_status = health.get('http', http_status)
                self.record(provider.name, 'unavailable', code, symbol, interval, http_status)
                self.store.set('provider:' + provider.name, {'status': 'unavailable', 'at': utc()})
                self.store.log('DATA', f'{provider.name} rejected or unavailable for {symbol}/{interval}; code={code}')
        if cached:
            snap = Snapshot.read(cached)
            snap.validate(utc(), self.max_age if interval == 900 else max(self.max_age, interval + 300))
            self.record(snap.source, 'ok', 'fresh_cache', symbol, interval)
            return snap
        raise ProviderError('No eligible fresh market observation')
