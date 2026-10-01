"""Pooled PostgreSQL transport for the existing repository API.

Never falls back to SQLite or retries a failed database mutation. Connection
strings and raw driver errors must not escape into Telegram or deployment logs.
"""
from contextlib import contextmanager
import sqlite3


class DatabaseUnavailable(RuntimeError):
    pass


class Row(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return tuple(self.values())[key]
        return super().__getitem__(key)


class Result:
    def __init__(self, rows, rowcount):
        self.rows, self.rowcount = rows, rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class PostgresConnection:
    def __init__(self, url):
        import psycopg
        from psycopg.rows import dict_row
        self.driver, self.row_factory = psycopg, dict_row
        self.url, self.raw, self.active = url, None, False
        self.connect()

    def connect(self):
        try:
            self.raw = self.driver.connect(
                self.url, autocommit=True, connect_timeout=10,
                prepare_threshold=None, row_factory=self.row_factory,
            )
        except self.driver.Error:
            raise DatabaseUnavailable('PostgreSQL connection unavailable; publishing stopped') from None

    def ready(self):
        if self.raw is None or self.raw.closed:
            if self.active:
                raise DatabaseUnavailable('PostgreSQL transaction interrupted; publishing stopped')
            self.connect()

    def execute(self, sql, args=None):
        self.ready()
        try:
            # Only repository-owned SQL reaches this adapter; values stay bound.
            statement = sql.replace('?', '%s') if args is not None else sql
            with self.raw.cursor() as cursor:
                cursor.execute(statement, args)
                rows = [Row(row) for row in cursor.fetchall()] if cursor.description else []
                return Result(rows, cursor.rowcount)
        except self.driver.IntegrityError:
            # Preserve existing duplicate handling without leaking row contents.
            raise sqlite3.IntegrityError('Database constraint rejected') from None
        except self.driver.Error:
            if not self.active:
                self.raw.close()
            raise DatabaseUnavailable('PostgreSQL operation failed; outcome requires review') from None

    @contextmanager
    def transaction(self):
        self.ready()
        if self.active:
            raise RuntimeError('Nested repository transactions are unsupported')
        try:
            with self.raw.transaction():
                self.active = True
                self.execute("SET LOCAL statement_timeout = '15s'")
                self.execute("SET LOCAL lock_timeout = '10s'")
                # Transaction-level lock survives pooled backend changes and
                # serializes lease acquisition and budget reservation across workers.
                self.execute('SELECT pg_advisory_xact_lock(739221840125)')
                yield
        except self.driver.Error:
            self.raw.close()
            raise DatabaseUnavailable('PostgreSQL transaction failed; outcome requires review') from None
        finally:
            self.active = False

    def close(self):
        if self.raw is not None:
            self.raw.close()
