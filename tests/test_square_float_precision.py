"""Float-text round trips and real snapshot/rebuild checks; no remote DB access.

The server shim models PostgreSQL's documented extra_float_digits=0 output
(15 significant digits for float8). Psycopg's real text loader decodes it.
"""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from psycopg.types.numeric import FloatLoader

from square_desk.postgres import DatabaseUnavailable, PostgresConnection, Result
from square_desk.replica_snapshot import SnapshotCache, rebuild
from test_square_database_failover import cluster
from test_square_postgres import FakeRaw, FakeCursor, URL
from test_square_snapshot_rebuild import SnapshotAdapter, setup, table_rows


STAMP = 1792245728.2282267


def decoded_float(value, precision):
    text = repr(value) if precision > 0 else format(value, '.15g')
    return FloatLoader(701).load(text.encode())


class PrecisionCursor(FakeCursor):
    def execute(self, sql, args):
        super().execute(sql, args)
        if 'SET LOCAL extra_float_digits = 3' in sql:
            self.raw.precision = 3

    def fetchall(self):
        return [{'created': decoded_float(STAMP, self.raw.precision)}]


class PooledRaw(FakeRaw):
    precision = 0

    def cursor(self):
        return PrecisionCursor(self)

    @contextmanager
    def transaction(self):
        try:
            with super().transaction():
                yield
        finally:
            # A different pooled transaction cannot inherit our local setting.
            self.precision = 0


def test_real_psycopg_loader_reproduces_reported_float_output_precision():
    rounded = decoded_float(STAMP, 0)
    assert rounded == 1792245728.22823
    assert rounded != STAMP
    assert decoded_float(STAMP, 3) == STAMP


@pytest.mark.parametrize('context', ('transaction', 'read_transaction', 'snapshot'))
def test_every_pg_transaction_restores_exact_floats_after_pooler_reset(monkeypatch, context):
    import psycopg
    raw = PooledRaw()
    monkeypatch.setattr(psycopg, 'connect', lambda *args, **kwargs: raw)
    adapter = PostgresConnection(URL)
    for _ in range(2):
        assert raw.precision == 0
        with getattr(adapter, context)():
            row = adapter.execute('SELECT created FROM entities').fetchone()
            assert row['created'] == STAMP
        assert raw.precision == 0


def test_precision_setup_failure_rolls_back_before_application_query(monkeypatch):
    import psycopg
    raw = PooledRaw()
    original = PrecisionCursor.execute
    def execute(cursor, sql, args):
        if 'SET LOCAL extra_float_digits = 3' in sql:
            raise psycopg.OperationalError('private-connection-value')
        return original(cursor, sql, args)
    monkeypatch.setattr(PrecisionCursor, 'execute', execute)
    monkeypatch.setattr(psycopg, 'connect', lambda *args, **kwargs: raw)
    adapter = PostgresConnection(URL)
    with pytest.raises(DatabaseUnavailable) as error:
        with adapter.transaction():
            adapter.execute('SELECT created FROM entities')
    assert raw.outcome == 'rollback' and not adapter.active
    assert raw.precision == 0
    assert not any(sql == 'SELECT created FROM entities' for sql, _ in raw.calls)
    assert 'private' not in str(error.value)


class PrecisionSnapshot(SnapshotAdapter):
    """Exercise actual Postgres transaction setup around the SQL-backed replica."""
    def __init__(self, backend):
        super().__init__(backend)
        self.precision = 0
        self.corrupt = False
        owner = self
        class Session:
            @contextmanager
            def transaction(self):
                try:
                    with SnapshotAdapter.transaction(owner):
                        yield
                finally:
                    owner.precision = 0
        class DriverError(Exception):
            pass
        self.transport = object.__new__(PostgresConnection)
        self.transport.active = False
        self.transport.raw = Session()
        self.transport.driver = SimpleNamespace(Error=DriverError)
        self.transport.ready = lambda: None
        def execute(sql, args=None):
            if 'SET LOCAL extra_float_digits = 3' in sql:
                self.precision = 3
            return Result([], 0)
        self.transport.execute = execute

    def transaction(self):
        return self.transport.transaction()

    def snapshot(self):
        return self.transport.snapshot()

    def stream_rows(self, table):
        for original in super().stream_rows(table):
            row = {k: decoded_float(v, self.precision) if type(v) is float else v
                   for k, v in original.items()}
            if self.corrupt and table == 'entities':
                row['status'] = 'unverified-change'
            yield row


@pytest.mark.parametrize('source_uses_rounded_default', (False, True))
def test_checkpoint_restore_retains_original_microseconds_with_supabase_default(cluster, source_uses_rounded_default):
    _, journal, nodes, engine = cluster
    source, initial_target, failure = setup(cluster)
    initial_target.close()
    if source_uses_rounded_default:
        source.close()
        source = PrecisionSnapshot(nodes['aiven'])
    target = PrecisionSnapshot(nodes['supabase'])
    engine.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
                   ('precise-timestamp', 'outbox', 'pending', STAMP, STAMP, None,
                    'precise-time-fingerprint', '{"message":"retain-original"}'))
    cache = SnapshotCache()
    try:
        result = rebuild(target, source, journal, journal.head, failure,
                         provider='aiven', cache=cache,
                         source_current=lambda: True, secondary=lambda: True)
        assert result['status'] == 'restored_verified_checkpoint'
        restored = target.execute('SELECT created,updated FROM entities WHERE id=?',
                                  ('precise-timestamp',)).fetchone()
        assert restored['created'] == restored['updated'] == STAMP
        assert target.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == journal.head['seq']
        assert target.precision == 0
    finally:
        source.close(); target.close(); cache.close()


def test_precise_output_never_bypasses_real_restore_corruption(cluster):
    _, journal, nodes, _ = cluster
    source, initial_target, failure = setup(cluster)
    initial_target.close()
    target = PrecisionSnapshot(nodes['supabase'])
    before = table_rows(target)
    metadata = dict(target.execute('SELECT * FROM square_replica_meta').fetchone())
    target.corrupt = True
    cache = SnapshotCache()
    try:
        with pytest.raises(DatabaseUnavailable) as error:
            rebuild(target, source, journal, journal.head, failure,
                    provider='aiven', cache=cache,
                    source_current=lambda: True, secondary=lambda: True)
        assert error.value.code == 'snapshot_restore_unverified'
        target.corrupt = False
        assert table_rows(target) == before
        assert dict(target.execute('SELECT * FROM square_replica_meta').fetchone()) == metadata
        assert target.execute("SELECT name FROM sqlite_master WHERE name='square_replica_rebuild_proof'").fetchone() is None
    finally:
        source.close(); target.close(); cache.close()
