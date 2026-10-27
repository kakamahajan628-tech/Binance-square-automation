import pytest

from test_square_database_failover import cluster
from square_desk.database_failover import catch_up, ReplayRejected, RowCountMismatch
from square_desk.replica_batch import replay


@pytest.mark.parametrize('runner',(catch_up,replay))
@pytest.mark.parametrize('operation',('insert','update','delete'))
def test_failed_entity_operation_exposes_only_counts_and_preserves_checkpoint(cluster,runner,operation):
    remote,journal,nodes,engine=cluster
    engine.execute('INSERT INTO state VALUES (?,?)',('safe-prefix','true'))
    replica=nodes['neon'].connection()
    try:
        if operation=='insert':
            args=('private-row','command','applied',1.0,1.0,None,'private-fingerprint','private-payload')
            engine.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',args)
            replica.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',args)
            actual=0
        elif operation=='update':
            engine.execute('UPDATE entities SET status=? WHERE id=? AND status=?',('queued','pending','approved'))
            replica.execute('UPDATE entities SET status=? WHERE id=?',('review','pending'))
            actual=0
        else:
            engine.execute('DELETE FROM entities WHERE kind=? AND status=?',('draft','published'))
            replica.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
                            ('private-extra','draft','published',1.0,1.0,None,None,'private-payload'))
            actual=2
        before=[dict(row) for row in replica.execute('SELECT * FROM entities ORDER BY id')]
        with pytest.raises(ReplayRejected) as caught:
            runner(replica,journal,journal.head)
        error=caught.value
        assert error.code=='replica_row_count_mismatch'
        assert error.failure_detail=={'journal_sequence':2,'statement_position':1,'table':'entities',
                                     'operation':operation,'expected_rows':1,'actual_rows':actual}
        assert 'private' not in str(error.failure_detail)+str(error)
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        assert replica.execute('SELECT value FROM state WHERE key=?',('safe-prefix',)).fetchone()[0]=='true'
        assert [dict(row) for row in replica.execute('SELECT * FROM entities ORDER BY id')]==before
        assert engine.diagnostics()['active_provider']=='aiven'
        assert engine.diagnostics()['providers']['aiven']['applied_sequence']==2
    finally:
        replica.close()


def test_error_detail_never_serializes_arbitrary_count_objects():
    error=RowCountMismatch('private-expected',{'private-actual':'private-value'})
    detail=ReplayRejected(error,33819,1,'UPDATE entities SET payload=? WHERE id=?').failure_detail
    assert detail=={'journal_sequence':33819,'statement_position':1,'table':'entities','operation':'update'}
