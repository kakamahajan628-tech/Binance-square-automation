import sqlite3

import pytest

from test_square_database_failover import cluster
from square_desk.database_failover import catch_up, ReplayRejected, replica_error
from square_desk.postgres import ConstraintRejected


def test_conflicting_replica_reports_entry_and_constraint_without_skipping(cluster):
    remote, journal, nodes, engine = cluster
    with engine.transaction():
        engine.execute('INSERT INTO state VALUES (?,?)', ('first', 'true'))
        engine.execute('INSERT INTO state VALUES (?,?)', ('conflict-private', 'committed-private'))
    replica = nodes['neon'].connection()
    replica.execute('INSERT INTO state VALUES (?,?)', ('conflict-private', 'unmanaged-private'))
    original_execute = replica.execute
    def execute(sql, args=None):
        try:
            return original_execute(sql, args)
        except sqlite3.IntegrityError:
            raise ConstraintRejected('23505', 'state_pkey') from None
    replica.execute = execute
    try:
        with pytest.raises(ReplayRejected) as caught:
            catch_up(replica, journal, journal.head)
        assert replica_error(caught.value) == 'unique_constraint_rejected'
        detail = caught.value.failure_detail
        assert detail == {'journal_sequence': 1, 'statement_position': 2, 'table': 'state', 'constraint': 'state_pkey'}
        assert 'private' not in str(detail)
        assert 'private' not in str(caught.value)
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
        assert replica.execute('SELECT value FROM state WHERE key=?', ('first',)).fetchone() is None
        assert replica.execute('SELECT value FROM state WHERE key=?', ('conflict-private',)).fetchone()[0] == 'unmanaged-private'
    finally:
        replica.close()


@pytest.mark.parametrize('state,code', [
    ('23505', 'unique_constraint_rejected'), ('23502', 'not_null_constraint_rejected'),
    ('23503', 'foreign_key_constraint_rejected'), ('23514', 'check_constraint_rejected'),
    ('unknown-private', 'constraint_rejected'),
])
def test_constraint_types_preserve_existing_integrity_handling_and_redact_names(state, code):
    error = ConstraintRejected(state, 'private-customer-constraint')
    assert isinstance(error, sqlite3.IntegrityError)
    assert replica_error(error) == code
    assert error.constraint == 'other_constraint'
    detail = ReplayRejected(error, 67, 3, 'INSERT INTO audit(id,message) VALUES (?,?)').failure_detail
    assert detail['table'] == 'audit'
    assert detail['constraint'] == 'other_constraint'
    assert 'private' not in str(detail)
