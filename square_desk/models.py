from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import json
import math
import uuid


def utc():
    return datetime.now(timezone.utc).timestamp()


def stamp(ts=None):
    return datetime.fromtimestamp(utc() if ts is None else ts, timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def uid():
    return uuid.uuid4().hex


@dataclass(frozen=True)
class Candle:
    start: float
    end: float
    open: float
    high: float
    low: float
    close: float
    volume: float

    def validate(self):
        values = asdict(self).values()
        if any(not math.isfinite(x) for x in values):
            raise ValueError('Nonfinite candle')
        if not (0 < self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high):
            raise ValueError('Invalid OHLC')
        if self.volume < 0 or self.end <= self.start:
            raise ValueError('Invalid volume or candle time')


@dataclass
class Snapshot:
    symbol: str
    quote: str
    source: str
    interval: int
    fetched_at: float
    candles: list[Candle]

    @property
    def as_of(self):
        return self.candles[-1].end

    def validate(self, now, max_age):
        if len(self.candles) < 55:
            raise ValueError('At least 55 closed candles required')
        for c in self.candles:
            c.validate()
            if c.end > now:
                raise ValueError('Incomplete candle')
            if abs(c.end - c.start - self.interval) > .01:
                raise ValueError('Incorrect candle interval')
        for a, b in zip(self.candles, self.candles[1:]):
            if abs(b.start - a.end) > .01:
                raise ValueError('Missing or duplicated candles; do not synthesize gaps')
        if now - self.as_of > max_age:
            raise ValueError('Stale candles')

    def dict(self):
        return asdict(self)

    @classmethod
    def read(cls, data):
        return cls(**{**data, 'candles': [Candle(**c) for c in data['candles']]})
