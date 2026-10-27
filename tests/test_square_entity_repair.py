import json
import sqlite3
from threading import Event

import pytest

from test_square_database_failover import cluster,BASE
from square_desk.database_failover import catch_up,ReplayRejected
from square_desk.postgres import DatabaseUnavailable,Result
from square_desk.replica_entity_repair import repair


def pg(connection):
    original=connection.execute
    def execute(sql,args=None):
        if sql.startswith('ALTER TABLE square_replica_entity_archive'):
            return Result([],0)
        return original(sql.replace(' FOR UPDATE',''),args)
    connection.execute=execute
    return connection


def setup(cluster,mode='missing',unsupported=False):
    remote,journal,nodes,engine=cluster
    journal.fresh_baseline=BASE
    remote.db.execute('UPDATE square_cluster SET migration_marker=?',
        (json.dumps({'origin':'authorized_fresh_start','history_preserved':False}),))
    journal.acquire(force=True)
    for backend in nodes.values():
        connection=backend.connection()
        connection.execute('DELETE FROM entities')
        connection.close()
    engine.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
                   ('canonical-row','draft','approved',10.0,11.0,12.0,'canonical-fp','{"receipt":"private"}'))
    if unsupported:
        engine.execute('UPDATE entities SET payload=payload WHERE id=?',('canonical-row',))
    replica=pg(nodes['neon'].connection())
    catch_up(replica,journal,journal.head)
    if mode=='missing':
        replica.execute('DELETE FROM entities WHERE id=?',('canonical-row',))
    else:
        replica.execute('UPDATE entities SET status=? WHERE id=?',('review','canonical-row'))
    engine.execute('UPDATE entities SET status=?,updated=? WHERE id=? AND status=?',
                   ('queued',20.0,'canonical-row','approved'))
    with pytest.raises(ReplayRejected) as caught:
        catch_up(replica,journal,journal.head)
    return replica,caught.value


@pytest.mark.parametrize('mode',('missing','wrong_status'))
def test_proven_preimage_is_archived_restored_without_advancing_then_replayed(cluster,mode):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster,mode)
    other_tables={table:[dict(r) for r in replica.execute('SELECT * FROM '+table)] for table in ('state','audit','budgets')}
    before=replica.execute('SELECT * FROM entities WHERE id=?',('canonical-row',)).fetchone()
    try:
        report=repair(replica,journal,journal.head,error,is_secondary=lambda:True)
        assert report['status']=='restored_verified_entity_preimage'
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        row=replica.execute('SELECT * FROM entities WHERE id=?',('canonical-row',)).fetchone()
        assert row['status']=='approved' and row['updated']==11.0 and row['due']==12.0
        archive=json.loads(replica.execute('SELECT payload FROM square_replica_entity_archive').fetchone()[0])
        assert archive['before']==(dict(before) if before else None)
        assert archive['canonical']==dict(row)
        for table,values in other_tables.items():
            assert [dict(r) for r in replica.execute('SELECT * FROM '+table)]==values
        assert catch_up(replica,journal,journal.head)==2
        assert replica.execute('SELECT status FROM entities').fetchone()[0]=='queued'
        assert engine.execute('SELECT status FROM entities').fetchone()[0]=='queued'
    finally:
        replica.close()


@pytest.mark.parametrize('case',('primary','not_fresh','missing_history','corrupt_history','unsupported','cancelled','archive_failure','fingerprint_conflict'))
def test_unproven_or_failed_repair_keeps_rows_and_checkpoint(cluster,case):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster,unsupported=case=='unsupported')
    head=dict(journal.head)
    if case=='not_fresh':head['migration_marker']='{}'
    if case=='missing_history':remote.db.execute('DELETE FROM square_journal WHERE seq=1')
    if case=='corrupt_history':remote.db.execute("UPDATE square_journal SET payload='corrupt' WHERE seq=1")
    if case=='fingerprint_conflict':
        replica.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
                        ('unmanaged','draft','review',1.0,1.0,None,'canonical-fp','{}'))
    if case=='archive_failure':
        original=replica.execute
        def execute(sql,args=None):
            if sql.startswith('INSERT INTO square_replica_entity_archive'):
                raise DatabaseUnavailable('private driver details')
            return original(sql,args)
        replica.execute=execute
    stop=Event()
    if case=='cancelled':stop.set()
    before=[dict(r) for r in replica.execute('SELECT * FROM entities')]
    checkpoint=dict(replica.execute('SELECT * FROM square_replica_meta').fetchone())
    try:
        with pytest.raises((DatabaseUnavailable,sqlite3.IntegrityError)):
            repair(replica,journal,head,error,is_secondary=lambda:case!='primary',stop=stop)
        assert [dict(r) for r in replica.execute('SELECT * FROM entities')]==before
        assert dict(replica.execute('SELECT * FROM square_replica_meta').fetchone())==checkpoint
        assert replica.execute("SELECT name FROM sqlite_master WHERE name='square_replica_entity_archive'").fetchone() is None
    finally:
        replica.close()


def test_no_entity_is_invented_when_primary_update_is_not_supported_by_journal(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    # Remove the creation but recompute a valid chain: history itself cannot
    # prove a row existed at the checkpoint, despite a later UPDATE count.
    from square_desk.turso_journal import encode,chain
    first=encode([{'sql':'INSERT INTO state VALUES (?,?)','args':['x','true'],'count':1}])
    first_hash=chain(BASE,first)
    remote.db.execute('UPDATE square_journal SET payload=?,hash=? WHERE seq=1',(first,first_hash))
    second=remote.db.execute('SELECT payload FROM square_journal WHERE seq=2').fetchone()[0]
    second_hash=chain(first_hash,second)
    remote.db.execute('UPDATE square_journal SET prev=?,hash=? WHERE seq=2',(first_hash,second_hash))
    replica.execute('UPDATE square_replica_meta SET head_hash=?',(first_hash,))
    try:
        with pytest.raises(DatabaseUnavailable) as caught:
            repair(replica,journal,journal.head,error,is_secondary=lambda:True)
        assert caught.value.code=='entity_canonical_missing'
        assert replica.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==0
    finally:
        replica.close()


def test_proof_cache_is_reused_for_same_checkpoint_on_second_replica(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    second=pg(nodes['supabase'].connection())
    # Apply only the creation to give the second replica exactly the same prefix.
    first_head={**journal.head,'seq':1,'head_hash':replica.execute('SELECT head_hash FROM square_replica_meta').fetchone()[0]}
    catch_up(second,journal,first_head)
    second.execute('DELETE FROM entities')
    cache={}
    def reads():
        return sum('ORDER BY seq LIMIT 256' in request['stmt']['sql'] for call in remote.requests
                   for request in call['requests'] if request['type']=='execute')
    try:
        repair(replica,journal,journal.head,error,is_secondary=lambda:True,cache=cache)
        count=reads()
        repair(second,journal,journal.head,error,is_secondary=lambda:True,cache=cache)
        assert count>0 and reads()==count
        assert len(cache)==2
    finally:
        replica.close();second.close()


def test_background_repair_and_quarantine_do_not_touch_primary(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    replica.close()
    original_factory=engine.factory
    engine.factory=lambda name:pg(original_factory(name))
    class Wake:
        calls=0
        def wait(self,timeout):
            self.calls+=1
            if self.calls>3:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        status=engine.diagnostics()
        assert status['active_provider']=='aiven'
        assert status['providers']['neon']['status']=='ready'
        assert status['providers']['neon']['applied_sequence']==2
        assert status['providers']['neon']['entity_repair']['repaired_rows']==1
        primary=nodes['aiven'].connection()
        try:
            assert primary.execute('SELECT status FROM entities').fetchone()[0]=='queued'
        finally:
            primary.close()
    finally:
        for client in clients.values():client.close()


def test_failed_proof_is_quarantined_instead_of_retried_each_minute(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    replica.close()
    journal.fresh_baseline=None
    class Wake:
        calls=0
        def wait(self,timeout):
            self.calls+=1
            if self.calls>3:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        status=engine.diagnostics()['providers']['neon']
        assert status['status']=='verification_failed'
        assert status['applied_sequence']==1
        assert status['retry_paused_until_restart'] is True
        assert status['entity_repair']['reason']=='entity_requires_complete_fresh_history'
        assert engine.nodes['neon'].entity_repairs==0
    finally:
        for client in clients.values():client.close()


def test_checkpoint_change_during_proof_prevents_any_restore(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    original=journal.proof_entries
    def advancing(*args):
        entries=original(*args)
        replica.execute('UPDATE square_replica_meta SET seq=?,head_hash=?',(journal.head['seq'],journal.head['head_hash']))
        return entries
    journal.proof_entries=advancing
    try:
        with pytest.raises(DatabaseUnavailable) as caught:
            repair(replica,journal,journal.head,error,is_secondary=lambda:True)
        assert caught.value.code=='replica_checkpoint_mismatch'
        assert replica.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==0
        assert replica.execute("SELECT name FROM sqlite_master WHERE name='square_replica_entity_archive'").fetchone() is None
    finally:
        replica.close()


def test_point_update_without_status_predicate_uses_the_same_verified_preimage(cluster):
    remote,journal,nodes,engine=cluster
    replica,error=setup(cluster)
    from square_desk.turso_journal import encode,chain
    previous=replica.execute('SELECT head_hash FROM square_replica_meta').fetchone()[0]
    payload=encode([{'sql':'UPDATE entities SET payload=?,updated=? WHERE id=?',
                     'args':['{"new":"private-value"}',20.0,'canonical-row'],'count':1}])
    digest=chain(previous,payload)
    remote.db.execute('UPDATE square_journal SET payload=?,hash=? WHERE seq=2',(payload,digest))
    head={**journal.head,'head_hash':digest}
    try:
        repair(replica,journal,head,error,is_secondary=lambda:True)
        assert replica.execute('SELECT updated FROM entities').fetchone()[0]==11.0
        assert catch_up(replica,journal,head)==2
        assert replica.execute('SELECT payload FROM entities').fetchone()[0]=='{"new":"private-value"}'
    finally:
        replica.close()
