"""Bounded editorial helpers; no publication authorization or invented metrics."""
from collections import OrderedDict
from datetime import datetime
from threading import RLock
from zoneinfo import ZoneInfo
import hashlib
import re


def normalize_labels(text):
    rules = (
        (r'(?<![\w$])15[ -]?(?:m|min(?:ute)?s?)(?!\w)', 'short-interval'),
        (r'(?<![\w$])24[ -]?(?:h|hours?)(?!\w)', 'daily'),
        (r'(?<![\w$])1[ -]?(?:h|hours?)(?!\w)', 'hourly'),
        (r'(?<![\w$])4[ -]?(?:h|hours?)(?!\w)', 'four-hour'),
        (r'\bEMA[ -]?20\b', 'short-term EMA'),
        (r'\bEMA[ -]?50\b', 'medium-term EMA'),
    )
    parts = re.split(r'(\{\{[^{}]+\}\})', text)
    for i in range(0, len(parts), 2):
        for pattern, replacement in rules:
            parts[i] = re.sub(pattern, replacement, parts[i], flags=re.I)
    return ''.join(parts)


class Fingerprints:
    def __init__(self, max_bytes=4_000_000):
        self.limit, self.bytes, self.hits = max_bytes, 0, 0
        self.items, self.lock = OrderedDict(), RLock()

    def get(self, text, calculate):
        # Cache keys never retain article bodies, URLs or credentials.
        key = hashlib.sha256(text.encode()).digest()
        with self.lock:
            if key in self.items:
                value, cost = self.items.pop(key)
                self.items[key] = value, cost
                self.hits += 1
                return value
        value = calculate(text)
        cost = 256 + sum(len(k.encode()) + 160 for k in value)
        if cost > self.limit:
            return value
        with self.lock:
            if key in self.items:
                return self.items[key][0]
            while self.items and (self.bytes + cost > self.limit or len(self.items) >= 4096):
                _, (_, removed) = self.items.popitem(last=False)
                self.bytes -= removed
            self.items[key] = value, cost
            self.bytes += cost
        return value


FINGERPRINTS = Fingerprints()


def cached_fingerprint(text, calculate):
    return FINGERPRINTS.get(text, calculate)


def generation_hold(settings, event, published, pending, now, cap, gap):
    """Two-item pipeline only when the *next slot's* evidence will still be fresh."""
    zone = ZoneInfo(settings.timezone)
    day = datetime.fromtimestamp(now, zone).date()
    count = sum(datetime.fromtimestamp(r['payload'].get('submitted_at', r['updated']), zone).date() == day for r in published)
    if count + len(pending) >= cap:
        return 'Daily publication capacity already reserved'
    if len(pending) >= 2:
        return 'Fresh rolling draft buffer full'
    sent = max((r['payload'].get('submitted_at', r['updated']) for r in published), default=0)
    first = max(now, sent + gap)
    queued = [r['due'] for r in pending if r.get('due') is not None]
    candidate = max(first + len(pending) * gap, max(queued, default=0) + gap if queued else first)
    local = datetime.fromtimestamp(candidate, ZoneInfo(settings.timezone))
    if local.hour not in settings.publishing_hours:
        return 'Next buffered slot is outside publishing hours'
    if candidate + 60 > event['as_of'] + settings.draft_max_age:
        return 'Next buffered slot requires fresher evidence'
    if not pending and now < sent + gap - min(300, gap / 4):
        return 'Next publication slot not yet near'
    return None
