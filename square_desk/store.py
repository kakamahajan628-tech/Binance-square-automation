"""Bounded indexed SQLite repository. Transactions serialize the single service worker."""
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
import json
import sqlite3
from .models import utc, uid


SCHEMA = '''
CREATE TABLE IF NOT EXISTS entities (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
 created REAL NOT NULL, updated REAL NOT NULL, due REAL,
 fingerprint TEXT, payload TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS unique_fingerprint ON entities(kind,fingerprint) WHERE fingerprint IS NOT NULL;
CREATE INDEX IF NOT EXISTS kind_status_due ON entities(kind,status,due);
CREATE INDEX IF NOT EXISTS kind_created ON entities(kind,created);
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit (
 id INTEGER PRIMARY KEY, at REAL NOT NULL, category TEXT NOT NULL,
 correlation TEXT NOT NULL, message TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS budgets (day TEXT NOT NULL, category TEXT NOT NULL, used INTEGER NOT NULL,
 PRIMARY KEY(day,category));
'''


class Store:
    def __init__(self, path):
        if path != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.lock = RLock()
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA busy_timeout=10000')
        self.conn.executescript(SCHEMA)
        self.set('schema_version', 1)

    @contextmanager
    def transaction(self):
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                yield
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

    def insert(self, kind, payload, status='pending', due=None, fingerprint=None, id=None):
        ident, now = id or uid(), utc()
        with self.lock:
            self.conn.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
                              (ident, kind, status, now, now, due, fingerprint,
                               json.dumps(payload, allow_nan=False)))
        return ident

    def get(self, ident):
        with self.lock:
            row = self.conn.execute('SELECT * FROM entities WHERE id=?', (ident,)).fetchone()
        return self.unpack(row) if row else None

    @staticmethod
    def unpack(row):
        d = dict(row)
        d['payload'] = json.loads(d['payload'])
        return d

    def list(self, kind, statuses=None, since=0, limit=500):
        sql, args = 'SELECT * FROM entities WHERE kind=? AND created>=?', [kind, since]
        if statuses:
            sql += ' AND status IN (' + ','.join('?' for _ in statuses) + ')'
            args.extend(statuses)
        sql += ' ORDER BY created DESC LIMIT ?'
        args.append(min(limit, 10000))
        with self.lock:
            return [self.unpack(r) for r in self.conn.execute(sql, args)]

    def update(self, ident, *, status=None, payload=None, due=None, clear_due=False):
        fields, args = ['updated=?'], [utc()]
        for key, value in (('status', status), ('payload', json.dumps(payload, allow_nan=False) if payload is not None else None), ('due', due)):
            if value is not None:
                fields.append(key + '=?')
                args.append(value)
        if clear_due:
            fields.append('due=NULL')
        with self.lock:
            self.conn.execute('UPDATE entities SET ' + ','.join(fields) + ' WHERE id=?', (*args, ident))

    def state(self, key, default=None):
        with self.lock:
            row = self.conn.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.lock:
            self.conn.execute('INSERT INTO state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                              (key, json.dumps(value, allow_nan=False)))

    def claim(self, ident, expected, status):
        with self.lock:
            result = self.conn.execute('UPDATE entities SET status=?,updated=? WHERE id=? AND status=?',
                                       (status, utc(), ident, expected))
        return result.rowcount == 1

    def reserve_budget(self, day, category, amount, cap):
        with self.transaction():
            row = self.conn.execute('SELECT used FROM budgets WHERE day=? AND category=?', (day, category)).fetchone()
            used = row[0] if row else 0
            if used + amount > cap:
                return False
            self.conn.execute('INSERT INTO budgets VALUES (?,?,?) ON CONFLICT(day,category) DO UPDATE SET used=excluded.used',
                              (day, category, used + amount))
        return True

    def log(self, category, message, correlation='system'):
        # Messages supplied by our code, not raw HTTP exceptions containing tokens or bodies.
        with self.lock:
            self.conn.execute('INSERT INTO audit(at,category,correlation,message) VALUES (?,?,?,?)',
                              (utc(), category, correlation, message[:1500]))

    def logs(self, limit=50):
        with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM audit ORDER BY id DESC LIMIT ?', (limit,))]

    def recover(self):
        # A process may die after a successful remote POST, before committing the receipt.
        # Never retry this uncertainty automatically.
        with self.lock:
            self.conn.execute("UPDATE entities SET status='uncertain',updated=? WHERE status='sending'", (utc(),))

    def acquire_lease(self, owner, seconds=120):
        with self.transaction():
            lease = self.state('worker_lease', {})
            if lease.get('until', 0) > utc() and lease.get('owner') != owner:
                return False
            self.set('worker_lease', {'owner': owner, 'until': utc() + seconds})
        return True

    def prune(self, days):
        cutoff = utc() - days * 86400
        with self.lock:
            self.conn.execute("DELETE FROM entities WHERE kind='snapshot' AND created<?", (cutoff,))
            self.conn.execute('DELETE FROM audit WHERE at<?', (cutoff,))
            self.conn.execute('DELETE FROM budgets WHERE day<?', (__import__('datetime').datetime.fromtimestamp(cutoff, __import__('datetime').timezone.utc).date().isoformat(),))

    def close(self):
        self.conn.close()
