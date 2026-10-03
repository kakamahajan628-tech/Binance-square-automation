"""Volatile public-market cache; publication decisions remain in PostgreSQL."""
from collections import deque
from copy import deepcopy


class MarketCache:
    def __init__(self):
        self.values = {}
        self.messages = deque(maxlen=100)

    def state(self, key, default=None):
        return deepcopy(self.values.get(key, default))

    def set(self, key, value):
        if not key.startswith(('cache:', 'provider:')):
            raise ValueError('Only public market observations belong in the volatile cache')
        self.values[key] = deepcopy(value)

    def log(self, category, message, correlation='system'):
        self.messages.append((category, message, correlation))

    def flush(self, store):
        for key, value in list(self.values.items()):
            store.set_if_changed(key, value)
        while self.messages:
            category, message, correlation = self.messages[0]
            store.log(category, message, correlation)
            self.messages.popleft()
