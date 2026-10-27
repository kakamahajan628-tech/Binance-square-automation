import json
import sqlite3

import pytest

from test_square_database_failover import cluster, BASE
from square_desk.database_failover import catch_up, ReplayRejected
from square_desk.postgres import ConstraintRejected, DatabaseUnavailable, Result
from square_desk.replica_audit_repair import repair


def fresh(journal, remote):
    journal.fresh_baseline = BASE
    remote.db.execute('UPDATE square_cluster SET migration_marker=?',
                      (json.dumps({'origin':'authorized_fresh_start','history_preserved':False}),))
    return journal.acquire(force=True)


def pg_features(connection):
    original = connection.execute
    def execute(sql, args=None):
        if sql.startswith('ALTER TABLE square_replica_audit_archive'):
            return Result([], 0)  # SQLite simulation of PostgreSQL owner-only RLS.
        try:
            return original(sql.replace(' FOR UPDATE',''), args)
        except sqlite3.IntegrityError:
            raise ConstraintRejected('23505','audit_pkey') from None
    connection.execute = execute
    return connection


def conflict(cluster, preceding=0):
    remote,journal,nodes,engine = cluster
    fresh(journal,remote)
    replica = pg_features(nodes['neon'].connection())
    for i in range(preceding):
        engine.execute('INSERT INTO state VALUES (?,?)',('proof-'+str(i),'true'))
    if preceding:
        catch_up(replica,journal,journal.head)
    engine.execute('INSERT INTO audit(at,category,correlation,message) VALUES (?,?,?,?)',
                   (123.0,'TEST','canonical','source audit'))
    replica.execute('INSERT INTO audit VALUES (?,?,?,?,?)',(1,111.0,'OLD','external','preserve this exact row'))
    with pytest.raises(ReplayRejected) as caught:
        catch_up(replica,journal,journal.head)
    return replica,caught.value


@pytest.mark.parametrize('preceding',[0,3])
def test_only_proven_extra_audit_is_archived_then_normal_replay_advances(cluster,preceding):
    remote,journal,nodes,engine = cluster
    replica,error=conflict(cluster,preceding)
    before_state=list(replica.execute('SELECT * FROM state ORDER BY key'))
    before_entities=list(replica.execute('SELECT * FROM entities ORDER BY id'))
    before_budgets=list(replica.execute('SELECT * FROM budgets ORDER BY day,category'))
    try:
        report=repair(replica,journal,journal.head,error,is_secondary=lambda:True)
        assert report['at_sequence']==preceding and report['retry_sequence']==preceding+1
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==preceding
        assert replica.execute('SELECT * FROM audit').fetchone() is None
        archived=json.loads(replica.execute('SELECT payload FROM square_replica_audit_archive').fetchone()[0])
        assert archived=={'id':1,'at':111.0,'category':'OLD','correlation':'external','message':'preserve this exact row'}
        assert before_state==list(replica.execute('SELECT * FROM state ORDER BY key'))
        assert before_entities==list(replica.execute('SELECT * FROM entities ORDER BY id'))
        assert before_budgets==list(replica.execute('SELECT * FROM budgets ORDER BY day,category'))
        assert catch_up(replica,journal,journal.head)==preceding+1
        assert replica.execute('SELECT message FROM audit').fetchone()[0]=='source audit'
    finally:
        replica.close()


@pytest.mark.parametrize('case',['primary','not_fresh','missing_history','corrupt_history','bound','archive_failure'])
def test_repair_refuses_incomplete_proof_and_preserves_conflict(cluster,case):
    remote,journal,nodes,engine=cluster
    replica,error=conflict(cluster,1)
    head=dict(journal.head)
    secondary=lambda:case!='primary'
    if case=='not_fresh':
        head['migration_marker']='{}'
    elif case=='missing_history':
        remote.db.execute('DELETE FROM square_journal WHERE seq=1')
    elif case=='corrupt_history':
        remote.db.execute("UPDATE square_journal SET payload='corrupt' WHERE seq=1")
    elif case=='bound':
        error.failure_detail['journal_sequence']=1026
    elif case=='archive_failure':
        original=replica.execute
        def execute(sql,args=None):
            if sql.startswith('INSERT INTO square_replica_audit_archive'):
                raise DatabaseUnavailable('private driver text')
            return original(sql,args)
        replica.execute=execute
    try:
        with pytest.raises(DatabaseUnavailable):
            repair(replica,journal,head,error,is_secondary=secondary)
        assert replica.execute('SELECT message FROM audit WHERE id=1').fetchone()[0]=='preserve this exact row'
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        assert replica.execute("SELECT name FROM sqlite_master WHERE name='square_replica_audit_archive'").fetchone() is None
    finally:
        replica.close()


def test_canonical_existing_id_is_never_removed(cluster):
    remote,journal,nodes,engine=cluster
    fresh(journal,remote)
    replica=pg_features(nodes['neon'].connection())
    engine.execute('INSERT INTO audit(at,category,correlation,message) VALUES (?,?,?,?)',
                   (123.0,'TEST','canonical','valid committed history'))
    catch_up(replica,journal,journal.head)
    journal.append([{'sql':'INSERT INTO audit(id,at,category,correlation,message) VALUES (?,?,?,?,?)',
                     'args':[1,125.0,'TEST','bad','conflict'],'count':1}])
    error=ReplayRejected(ConstraintRejected('23505','audit_pkey'),2,1,
                         'INSERT INTO audit(id,at,category,correlation,message) VALUES (?,?,?,?,?)')
    try:
        with pytest.raises(DatabaseUnavailable) as caught:
            repair(replica,journal,journal.head,error,is_secondary=lambda:True)
        assert caught.value.code=='audit_repair_not_proven_extra'
        assert replica.execute('SELECT message FROM audit WHERE id=1').fetchone()[0]=='valid committed history'
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
    finally:
        replica.close()


def test_background_repairs_secondary_then_replays_without_touching_primary(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=conflict(cluster)
    replica.close()
    factory=engine.factory
    engine.factory=lambda name:pg_features(factory(name))
    class Wake:
        calls=0
        def wait(self,timeout):
            self.calls+=1
            if self.calls>2:
                engine.stopped.set()
        def clear(self):
            pass
        def set(self):
            pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        report=engine.diagnostics()['providers']['neon']
        assert report['status']=='ready'
        assert report['applied_sequence']==journal.head['seq']==1
        assert report['audit_repair']['status']=='archived_extra_audit_row'
        assert report['audit_repair']['archived_rows']==1
        assert report['last_error']=='' and report['failure_detail']=={}
        primary=nodes['aiven'].connection()
        try:
            assert primary.execute('SELECT message FROM audit').fetchone()[0]=='source audit'
            assert primary.execute("SELECT name FROM sqlite_master WHERE name='square_replica_audit_archive'").fetchone() is None
            assert engine.current=='aiven' and engine.switches==0
        finally:
            primary.close()
    finally:
        for connection in clients.values():
            connection.close()
