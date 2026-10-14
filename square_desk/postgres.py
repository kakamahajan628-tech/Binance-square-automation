"""Pooled PostgreSQL transport for the existing repository API.

Never falls back to SQLite or retries a failed database mutation. Connection
strings and raw driver errors must not escape into Telegram or deployment logs.
"""
from contextlib import contextmanager
import sqlite3
import json
from .bandwidth import record


class DatabaseUnavailable(RuntimeError):
    def __init__(self, message, *, code='database_unavailable'):
        super().__init__(message)
        self.code = code


def driver_error_code(error, stage):
    # Never include driver messages: they may contain credentials/row values.
    state = getattr(error, 'sqlstate', None)
    known = {'28P01': 'authentication_failed', '28000': 'authentication_failed',
             '42501': 'permission_denied', '42P01': 'table_missing',
             '42703': 'column_missing', '53300': 'connection_limit',
             '57P03': 'database_unavailable', '57014': 'statement_timeout',
             '55P03': 'lock_unavailable', '25006': 'read_only_connection'}
    if state in known:
        return known[state]
    if isinstance(state, str) and state.startswith('08'):
        return 'connection_failed'
    return 'connection_or_tls_failed' if stage == 'connect' else 'postgres_operation_failed'


class ConstraintRejected(sqlite3.IntegrityError):
    def __init__(self, sqlstate=None, constraint=None):
        super().__init__('Database constraint rejected')
        self.code = {'23505': 'unique_constraint_rejected', '23502': 'not_null_constraint_rejected',
                     '23503': 'foreign_key_constraint_rejected', '23514': 'check_constraint_rejected'}.get(
                         sqlstate, 'constraint_rejected')
        known = {'unique_fingerprint', 'entities_pkey', 'audit_pkey', 'state_pkey', 'budgets_pkey'}
        self.constraint = constraint if isinstance(constraint, str) and constraint in known else 'other_constraint'


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
    def __init__(self, url, *, connect_timeout=10):
        import psycopg
        from psycopg.rows import dict_row
        self.driver, self.row_factory = psycopg, dict_row
        self.url, self.raw, self.active = url, None, False
        self.connect_timeout = connect_timeout
        self.connect()

    def connect(self):
        try:
            self.raw = self.driver.connect(
                self.url, autocommit=True, connect_timeout=self.connect_timeout,
                prepare_threshold=None, row_factory=self.row_factory,
            )
        except self.driver.Error as exc:
            raise DatabaseUnavailable('PostgreSQL connection unavailable; publishing stopped',
                                      code=driver_error_code(exc, 'connect')) from None

    def ready(self):
        if self.raw is None or self.raw.closed:
            if self.active:
                raise DatabaseUnavailable('PostgreSQL transaction interrupted; publishing stopped')
            self.connect()

    def execute(self, sql, args=None):
        self.ready()
        try:
            # Only repository-owned SQL reaches this adapter; values stay bound.
            # Escape literal SQL percent signs before introducing psycopg's
            # placeholders. Bound values (JSON, cashtags, percent text) stay intact.
            statement = sql.replace('%', '%%').replace('?', '%s') if args is not None else sql
            record('postgres_request_payload_bytes',len(statement.encode())+len(json.dumps(args,default=str).encode())+32)
            with self.raw.cursor() as cursor:
                cursor.execute(statement, args)
                rows = [Row(row) for row in cursor.fetchall()] if cursor.description else []
                record('postgres_response_payload_bytes',len(json.dumps(rows,default=str).encode()))
                return Result(rows, cursor.rowcount)
        except self.driver.IntegrityError as exc:
            # Preserve existing duplicate handling without leaking row contents.
            raise ConstraintRejected(getattr(exc, 'sqlstate', None),
                getattr(getattr(exc, 'diag', None), 'constraint_name', None)) from None
        except self.driver.Error as exc:
            if not self.active:
                self.raw.close()
            raise DatabaseUnavailable('PostgreSQL operation failed; outcome requires review',
                                      code=driver_error_code(exc, 'execute')) from None

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
        except self.driver.Error as exc:
            self.raw.close()
            raise DatabaseUnavailable('PostgreSQL transaction failed; outcome requires review',
                                      code=driver_error_code(exc, 'transaction')) from None
        finally:
            self.active = False

    def execute_replay_batch(self, statements):
        """Secondary replay only; caller owns an explicit rollbackable transaction."""
        if not self.active or len(statements) > 256:
            raise DatabaseUnavailable('Replica batch requires a bounded transaction')
        if not self.driver.capabilities.has_pipeline():
            return [self.execute(sql,args).rowcount for sql,args in statements]
        cursors = []
        try:
            with self.raw.pipeline() as pipeline:
                for sql,args in statements:
                    query = sql.replace('%','%%').replace('?','%s')
                    record('postgres_request_payload_bytes',len(query.encode())+len(json.dumps(args,default=str).encode())+32)
                    cursor = self.raw.cursor()
                    cursors.append(cursor)
                    cursor.execute(query,args)
                pipeline.sync()
                return [cursor.rowcount for cursor in cursors]
        except self.driver.IntegrityError as exc:
            raise ConstraintRejected(getattr(exc,'sqlstate',None),
                getattr(getattr(exc,'diag',None),'constraint_name',None)) from None
        except self.driver.Error as exc:
            raise DatabaseUnavailable('PostgreSQL replica batch failed; outcome requires review',
                                      code=driver_error_code(exc,'execute')) from None
        finally:
            for cursor in cursors:
                cursor.close()

    def close(self):
        if self.raw is not None:
            self.raw.close()
