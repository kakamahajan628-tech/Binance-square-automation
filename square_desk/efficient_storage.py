"""Bounded public-observation RAM; exact draft/signal evidence stays durable.

Critical controls, worker leases, budgets and transactional reads never use a
history cache. Provider/fencing changes and uncertain outcomes clear caches.
"""
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
import base64
import json
import os
import time
import zlib
from .models import digest


STATE_TAG = '_square_state_zlib_v1'
MAX_VALUE = 32_000_000
OBSERVATION_CACHE_BYTES = 8_000_000
HISTORY_CACHE_BYTES = 4_000_000


def cache_encode(value):
    raw = json.dumps(value, allow_nan=False).encode()
    if len(raw) > MAX_VALUE:
        raise ValueError('Observation exceeds RAM bounds')
    return zlib.compress(raw, level=1)


def cache_decode(data):
    decoder = zlib.decompressobj()
    raw = decoder.decompress(data, MAX_VALUE + 1)
    if len(raw) > MAX_VALUE or not decoder.eof or decoder.unused_data:
        raise ValueError('Compressed RAM cache invalid')
    return json.loads(raw)


def volatile_observation(key):
    return key.startswith(('cache:', 'provider:')) or key == 'latest_market'


def packed(value):
    text = json.dumps(value, allow_nan=False)
    if len(text.encode()) < 1500:
        return value
    compressed = {STATE_TAG: base64.b64encode(zlib.compress(text.encode())).decode('ascii')}
    return compressed if len(json.dumps(compressed)) < len(text) else value


def unpacked(value):
    if not isinstance(value, dict) or set(value) != {STATE_TAG}:
        return value
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(base64.b64decode(value[STATE_TAG], validate=True), MAX_VALUE + 1)
        if len(raw) > MAX_VALUE or not decoder.eof or decoder.unused_data:
            raise ValueError
        return json.loads(raw)
    except (ValueError, TypeError, zlib.error):
        raise ValueError('Compressed database state invalid') from None


class EfficientStorage:
    def __init__(self, path, **kwargs):
        choice = os.getenv('DESK_STORAGE_EFFICIENT', 'true' if path.startswith(('postgres://','postgresql://')) else 'false')
        if choice not in ('true', 'false'):
            raise ValueError('DESK_STORAGE_EFFICIENT must be true or false')
        self.efficient = choice == 'true'
        self._volatile, self._frames, self._history = OrderedDict(), OrderedDict(), {}
        self._ram_bytes, self._history_bytes = 0, 0
        self._depth, self._history_token = 0, None
        self._pinned = OrderedDict()
        self.transfer_stats = {'volatile_writes_avoided':0,'history_reads_avoided':0,'evidence_frames_pinned':0}
        super().__init__(path, **kwargs)

    def _ram_set(self, collection, key, value):
        data = cache_encode(value)
        size = len(data) + len(key.encode()) + 256
        with self.lock:
            if key in collection:
                self._ram_bytes -= collection.pop(key)[1]
            collection[key] = (data, size)
            self._ram_bytes += size
            while self._ram_bytes > OBSERVATION_CACHE_BYTES or len(collection) > 2048:
                other = collection if len(collection) > 2048 else (self._frames if self._frames else self._volatile)
                _, (_, size) = other.popitem(last=False)
                self._ram_bytes -= size

    def state(self, key, default=None):
        if self.efficient and volatile_observation(key):
            with self.lock:
                item = self._volatile.get(key)
                return cache_decode(item[0]) if item else deepcopy(default)
        return unpacked(super().state(key, default))

    def set(self, key, value):
        if self.efficient and volatile_observation(key):
            self._ram_set(self._volatile,key,value)
            self.transfer_stats['volatile_writes_avoided'] += 1
            return
        return super().set(key, packed(value) if self.efficient else value)

    def set_if_changed(self, key, value):
        if self.efficient and volatile_observation(key):
            return self.set(key,value)
        return super().set_if_changed(key, packed(value) if self.efficient else value)

    def archive_snapshot(self, snapshot):
        if not self.efficient:
            return super().archive_snapshot(snapshot)
        key = digest([snapshot['source'],snapshot['symbol'],snapshot['interval'],snapshot['candles'][-1]['end']])
        existed = key in self._frames or key in self._pinned
        self._ram_set(self._frames,key,snapshot)
        self.transfer_stats['volatile_writes_avoided'] += 1
        return not existed

    def get(self, ident):
        if self.efficient:
            with self.lock:
                frame = self._frames.get(ident)
                if frame:
                    return {'id':ident,'kind':'snapshot','status':'observed','payload':cache_decode(frame[0])}
        return super().get(ident)

    def _pin(self, payload, kind):
        if not self.efficient or kind not in ('draft','signal'):
            return
        event = payload.get('event', {}) if kind == 'draft' else payload
        source = event.get('metrics', {}).get('source') or event.get('source')
        if not source or not event.get('symbol') or not event.get('as_of'):
            return
        ident = digest([source,event['symbol'],900,event['as_of']])
        if ident in self._pinned:
            return
        frame = self._frames.get(ident)
        if frame:
            super().archive_snapshot(cache_decode(frame[0]))
        else:
            row = super().get(ident)
            if not row or row['kind'] != 'snapshot':
                raise ValueError('Exact chart evidence unavailable; regenerate draft')
        self._pinned[ident] = True
        if len(self._pinned) > 4096:
            self._pinned.popitem(last=False)
        self.transfer_stats['evidence_frames_pinned'] += 1

    @contextmanager
    def transaction(self):
        try:
            with super().transaction():
                self._depth += 1
                try:
                    yield
                finally:
                    self._depth -= 1
        except BaseException:
            self._history.clear()
            self._history_bytes = 0
            self._pinned.clear()
            raise

    @contextmanager
    def _atomic_write(self):
        if self._depth:
            yield
        else:
            with self.transaction():
                yield

    def _after_entity(self, ident):
        # Refresh only the changed row, never reread all history after each write.
        for kind, cached in list(self._history.items()):
            if ident in cached['rows']:
                row = super().get(ident)
                if row:
                    self._cache_put(kind,row)
                else:
                    removed = cached['rows'].pop(ident,None)
                    if removed:
                        self._history_bytes -= removed['bytes']
                return

    def insert(self, kind, payload, status='pending', due=None, fingerprint=None, id=None):
        if not self.efficient:
            return super().insert(kind,payload,status,due,fingerprint,id)
        try:
            with self._atomic_write():
                self._pin(payload,kind)
                ident = super().insert(kind,payload,status,due,fingerprint,id)
            if kind in self._history:
                self._cache_put(kind,super().get(ident))
            return ident
        except BaseException:
            self._history.clear()
            self._history_bytes = 0
            self._pinned.clear()
            raise

    def update(self, ident, **kwargs):
        try:
            result = super().update(ident, **kwargs)
            if self.efficient:
                self._after_entity(ident)
            return result
        except BaseException:
            self._history.clear()
            self._history_bytes = 0
            raise

    def claim(self, ident, expected, status):
        try:
            result = super().claim(ident,expected,status)
            if result and self.efficient:
                self._after_entity(ident)
            return result
        except BaseException:
            self._history.clear()
            self._history_bytes = 0
            raise

    def _token(self):
        if self._depth:
            return None
        journal = getattr(self.conn,'journal',None)
        if journal:
            if journal.epoch is None or time.monotonic() >= journal.deadline:
                return None
            return (journal.epoch,self.conn.current)
        return ('single',int(time.monotonic()//30))

    def _cache_put(self,kind,row):
        cached = self._history.get(kind)
        if not cached or not row:
            return
        ident = row['id']
        old = cached['rows'].get(ident)
        try:
            data = cache_encode(row)
        except ValueError:
            self._history.clear()
            self._history_bytes = 0
            return
        size = len(data) + len(ident.encode()) + 256
        delta = size - (old['bytes'] if old else 0)
        cached['rows'][ident] = {'created':row['created'],'status':row['status'],'data':data,'bytes':size}
        self._history_bytes += delta
        if len(cached['rows']) > 10000:
            oldest = min(cached['rows'],key=lambda key:cached['rows'][key]['created'])
            self._history_bytes -= cached['rows'].pop(oldest)['bytes']
        if self._history_bytes > HISTORY_CACHE_BYTES:
            self._history.clear()
            self._history_bytes = 0

    def list(self, kind, statuses=None, since=0, limit=500):
        if not self.efficient or kind not in ('draft','signal','campaign','outbox','news','metric'):
            return super().list(kind,statuses,since,limit)
        token = self._token()
        if token is None:
            return super().list(kind,statuses,since,limit)
        with self.lock:
            if token != self._history_token:
                self._history.clear()
                self._history_bytes = 0
                self._history_token = token
            if kind not in self._history:
                rows = super().list(kind,limit=10000)
                entries = {}
                size = 0
                for row in rows:
                    try:
                        data = cache_encode(row)
                    except ValueError:
                        return super().list(kind,statuses,since,limit)
                    charged = len(data) + len(row['id'].encode()) + 256
                    size += charged
                    if size > HISTORY_CACHE_BYTES:
                        return super().list(kind,statuses,since,limit)
                    entries[row['id']] = {'created':row['created'],'status':row['status'],'data':data,'bytes':charged}
                if self._history_bytes + size > HISTORY_CACHE_BYTES:
                    self._history.clear()
                    self._history_bytes = 0
                self._history[kind] = {'rows':entries}
                self._history_bytes += size
            else:
                self.transfer_stats['history_reads_avoided'] += 1
            rows = [r for r in self._history[kind]['rows'].values()
                    if r['created'] >= since and (not statuses or r['status'] in statuses)]
            return [cache_decode(row['data']) for row in sorted(rows,key=lambda r:r['created'],reverse=True)[:min(limit,10000)]]

    def prune(self,*args,**kwargs):
        try:
            return super().prune(*args,**kwargs)
        finally:
            self._history.clear()
            self._history_bytes = 0

    def recover(self):
        try:
            return super().recover()
        finally:
            self._history.clear()
            self._history_bytes = 0

    def recover_submissions(self):
        try:return super().recover_submissions()
        finally:
            self._history.clear()
            self._history_bytes=0
