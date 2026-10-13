from contextlib import contextmanager
from pathlib import Path
import sqlite3
import pytest

from square_desk.config import Settings
from square_desk.postgres import PostgresConnection, DatabaseUnavailable


URL = 'postgresql://test:private-password@unit-pooler.example/db?sslmode=require'


def test_neon_url_precedence_and_redaction(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', URL)
    monkeypatch.setenv('DESK_DATABASE', '/var/data/desk.sqlite3')
    monkeypatch.setenv('DESK_ARTIFACTS', str(tmp_path / 'artifacts'))
    settings = Settings.from_env()
    assert settings.database == URL
    assert URL not in str(settings.public())
    assert 'private-password' not in repr(settings)
    monkeypatch.setenv('DESK_DATABASE_URL', URL.replace('/db?', '/other?'))
    assert '/other?' in Settings.from_env().database


def test_neon_keeps_live_enabled_when_artifact_disk_missing(monkeypatch, tmp_path):
    from square_desk import config
    original = Path.exists
    monkeypatch.setattr(Path, 'exists', lambda path: False if path == Path('/var/data') else original(path))
    monkeypatch.setattr(config, '__file__', str(tmp_path / 'application/square_desk/config.py'))
    settings = Settings(database=URL, artifacts='/var/data/artifacts', live_enabled=True,
                        square_key='test-key', admin_token='test-token', policy_reviewed='2026-10-01')
    settings.validate()
    assert settings.live_enabled
    assert settings.database == URL
    assert Path(settings.artifacts).is_dir()


def test_postgres_requires_tls(tmp_path):
    settings = Settings(database=URL.split('?')[0], artifacts=str(tmp_path / 'artifacts'))
    with pytest.raises(ValueError, match='sslmode'):
        settings.validate()


def test_invalid_external_url_does_not_create_local_database(monkeypatch):
    monkeypatch.setenv('DATABASE_URL', 'not-a-postgres-url')
    with pytest.raises(ValueError, match='PostgreSQL connection URL'):
        Settings.from_env()


class FakeCursor:
    description = True
    rowcount = 1

    def __init__(self, raw):
        self.raw = raw

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql, args):
        self.raw.calls.append((sql, args))
        if self.raw.failure:
            raise self.raw.failure

    def fetchall(self):
        return [{'value': '42'}]


class FakeRaw:
    closed = False

    def __init__(self):
        self.calls, self.failure, self.outcome = [], None, None

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True

    @contextmanager
    def transaction(self):
        try:
            yield
        except BaseException:
            self.outcome = 'rollback'
            raise
        else:
            self.outcome = 'commit'


@pytest.fixture
def connection(monkeypatch):
    import psycopg
    raw = FakeRaw()
    options = {}
    def connect(url, **kwargs):
        options.update(kwargs)
        raw.closed = False
        return raw
    monkeypatch.setattr(psycopg, 'connect', connect)
    adapter = PostgresConnection(URL)
    assert options['autocommit'] is True
    assert options['prepare_threshold'] is None
    return adapter, raw


def test_bound_values_and_repository_row_compatibility(connection):
    adapter, raw = connection
    row = adapter.execute('SELECT value FROM state WHERE key=?', ('secret?',)).fetchone()
    assert raw.calls[-1] == ('SELECT value FROM state WHERE key=%s', ('secret?',))
    assert row[0] == row['value'] == '42'
    assert dict(row) == {'value': '42'}


def test_transaction_lock_and_rollback(connection):
    adapter, raw = connection
    with pytest.raises(ValueError):
        with adapter.transaction():
            raise ValueError('rollback')
    assert raw.outcome == 'rollback'
    assert any('pg_advisory_xact_lock' in call[0] for call in raw.calls)
    assert not adapter.active


def test_integrity_failure_preserves_duplicate_handling(connection):
    adapter, raw = connection
    raw.failure = adapter.driver.IntegrityError('private row contents')
    with pytest.raises(sqlite3.IntegrityError, match='Database constraint rejected'):
        adapter.execute('INSERT INTO state VALUES (?,?)', ('a', 'b'))


def test_database_failure_is_redacted_and_not_retried(connection):
    adapter, raw = connection
    raw.failure = adapter.driver.OperationalError(URL)
    with pytest.raises(DatabaseUnavailable) as error:
        adapter.execute('UPDATE state SET value=?', ('x',))
    assert 'private-password' not in str(error.value)
    assert len(raw.calls) == 1
    assert raw.closed


def test_pooled_client_reconnects_after_batch_idle(connection):
    adapter, raw = connection
    adapter.close()
    assert raw.closed
    assert adapter.execute('SELECT value FROM state WHERE key=?', ('test',)).fetchone()[0] == '42'
    assert not raw.closed


def test_connection_failure_never_falls_back_or_exposes_url(monkeypatch):
    import psycopg
    def failed(*args, **kwargs):
        raise psycopg.OperationalError(URL)
    monkeypatch.setattr(psycopg, 'connect', failed)
    with pytest.raises(DatabaseUnavailable) as error:
        PostgresConnection(URL)
    assert 'private-password' not in str(error.value)
