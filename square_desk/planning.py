"""Read cache scoped to one allocation pass; publishing uses the real store."""
from copy import deepcopy
from .models import utc


class PlanningStore:
    def __init__(self, store):
        self.store = store
        self.states, self.queries, self.rows, self.changed = {}, {}, {}, {}

    def state(self, key, default=None):
        if key not in self.states:
            self.states[key] = self.store.state(key, default)
        return deepcopy(self.states[key])

    def get(self, ident):
        if ident not in self.rows:
            self.rows[ident] = self.store.get(ident)
        return deepcopy(self.rows[ident])

    def list(self, kind, statuses=None, since=0, limit=500):
        key = kind, tuple(statuses or ()), since, min(limit, 10000)
        if key not in self.queries:
            records = self.store.list(kind, statuses, since, limit)
            self.queries[key] = {row['id']: row for row in records}
            self.rows.update(self.queries[key])
        records = {**self.queries[key], **self.changed}
        result = [row for row in records.values() if row['kind'] == kind and row['created'] >= since
                  and (not statuses or row['status'] in statuses)]
        return deepcopy(sorted(result, key=lambda row: row['created'], reverse=True)[:min(limit, 10000)])

    def update(self, ident, *, status=None, payload=None, due=None, clear_due=False):
        self.store.update(ident, status=status, payload=payload, due=due, clear_due=clear_due)
        row = self.get(ident)
        if row:
            row['updated'] = utc()
            if status is not None:
                row['status'] = status
            if payload is not None:
                row['payload'] = deepcopy(payload)
            if due is not None or clear_due:
                row['due'] = None if clear_due else due
            self.rows[ident] = self.changed[ident] = row
