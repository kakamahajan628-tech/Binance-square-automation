import sqlite3
from types import SimpleNamespace

import pytest

from test_square_database_failover import cluster
from square_desk.database_failover import CatchUpPending, catch_up, replica_error
from square_desk.postgres import DatabaseUnavailable, driver_error_code


def test_partial_batch_reports_committed_progress_but_cannot_promote(cluster):
    remote, journal, nodes, engine = cluster
    for i in range(5):
        engine.execute('INSERT INTO state VALUES (?,?)', ('progress-' + str(i), 'true'))
    replica = nodes['neon'].connection()
    progress = []
    try:
        with pytest.raises(CatchUpPending) as caught:
            catch_up(replica, journal, journal.head, limit=2, progress=progress.append)
        assert caught.value.sequence == 2
        assert progress[-1] == 2
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 2
        assert catch_up(replica, journal, journal.head) == 5
    finally:
        replica.close()


def test_background_continues_partial_batches_without_failure_cooldown(cluster):
    remote, journal, nodes, engine = cluster
    for i in range(140):
        engine.execute('INSERT INTO state VALUES (?,?)', ('batch-' + str(i), 'true'))
    snapshots = []
    class Wake:
        calls = 0
        def wait(self, timeout):
            self.calls += 1
            if self.calls > 1:
                snapshots.append(engine.diagnostics()['providers'])
            if self.calls > 2:
                engine.stopped.set()
        def clear(self):
            pass
        def set(self):
            pass
    engine.wake = Wake()
    clients = {}
    try:
        engine._replicate_clients(clients)
        first = snapshots[0]['neon']
        assert first['status'] == 'catching_up'
        assert first['applied_sequence'] == 128
        assert first['pending_entries'] == 12
        assert first['last_error'] == ''
        assert engine.nodes['neon'].until == 0
        final = engine.diagnostics()['providers']['neon']
        assert final['status'] == 'ready'
        assert final['applied_sequence'] == 140
        assert final['pending_entries'] == 0
    finally:
        for connection in clients.values():
            connection.close()


def test_replica_mismatch_exposes_safe_reason_and_does_not_advance(cluster):
    remote, journal, nodes, engine = cluster
    engine.execute('UPDATE entities SET status=? WHERE id=?', ('sending', 'pending'))
    replica = nodes['neon'].connection()
    replica.execute('DELETE FROM entities WHERE id=?', ('pending',))
    progress = []
    try:
        with pytest.raises(DatabaseUnavailable) as caught:
            catch_up(replica, journal, journal.head, progress=progress.append)
        assert replica_error(caught.value) == 'replica_row_count_mismatch'
        assert progress == [0]
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
    finally:
        replica.close()


@pytest.mark.parametrize('state,expected', [
    ('28P01', 'authentication_failed'), ('42501', 'permission_denied'),
    ('42P01', 'table_missing'), ('53300', 'connection_limit'),
    ('08006', 'connection_failed'), ('57014', 'statement_timeout'),
    ('25006', 'read_only_connection'), (None, 'connection_or_tls_failed'),
])
def test_driver_diagnostics_use_sqlstate_without_private_driver_text(state, expected):
    error = SimpleNamespace(sqlstate=state, message='private host password and query')
    assert driver_error_code(error, 'connect') == expected
    assert replica_error(DatabaseUnavailable('private host password', code=expected)) == expected


def test_unknown_diagnostic_codes_cannot_expose_secrets():
    assert replica_error(DatabaseUnavailable('private', code='secret-token-value')) == 'database_unavailable'
    assert replica_error(sqlite3.IntegrityError('private row')) == 'constraint_rejected'
