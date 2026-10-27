from contextlib import contextmanager
import json
from threading import Event

import pytest

from test_square_database_failover import cluster,SqliteTransport
from square_desk.database_failover import catch_up,ReplayRejected
from square_desk.database_transfer import COLUMNS
from square_desk.postgres import DatabaseUnavailable,Result
from square_desk.replica_snapshot import SnapshotCache,rebuild,inspect


class SnapshotAdapter(SqliteTransport):
    def execute(self,sql,args=None):
        if sql.startswith('ALTER TABLE square_replica_rebuild_proof'):
            return Result([],0)
        return super().execute(sql,args)
    @contextmanager
    def snapshot(self):
        self.db.execute('BEGIN')
        self.active=True
        try:yield
        except BaseException:
            self.db.execute('ROLLBACK');raise
        else:self.db.execute('COMMIT')
        finally:self.active=False
    def stream_rows(self,table):
        order={'entities':'id','state':'key','audit':'id','budgets':'day,category'}[table]
        yield from self.execute('SELECT * FROM '+table+' ORDER BY '+order)
    def archive_replica(self,ident):
        self.execute('CREATE TABLE IF NOT EXISTS square_replica_rebuild_archive(rebuild_id TEXT,table_name TEXT,row_key TEXT,payload TEXT,PRIMARY KEY(rebuild_id,table_name,row_key))')
        counts={}
        for table in (*COLUMNS,'square_replica_meta'):
            rows=list(self.execute('SELECT * FROM '+table))
            for i,row in enumerate(rows):
                self.execute('INSERT INTO square_replica_rebuild_archive VALUES (?,?,?,?)',
                             (ident,table,str(i),json.dumps(dict(row),sort_keys=True)))
            counts[table]=len(rows)
        return counts


def setup(cluster):
    remote,journal,nodes,engine=cluster
    source=SnapshotAdapter(nodes['aiven'])
    target=SnapshotAdapter(nodes['neon'])
    # This row never had an INSERT in the journal: the exact reported fault.
    source.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
        ('unlogged-row','outbox','pending',1.0,2.0,None,'telegram-fingerprint','{"text":"original-private"}'))
    engine.execute('UPDATE entities SET status=?,updated=? WHERE id=?',('delivered',3.0,'unlogged-row'))
    with pytest.raises(ReplayRejected) as caught:catch_up(target,journal,journal.head)
    return source,target,caught.value


def table_rows(connection):
    return {table:[dict(row) for row in connection.stream_rows(table)] for table in COLUMNS}


def test_missing_canonical_row_is_rebuilt_from_verified_active_checkpoint_and_archived(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    before=table_rows(target)
    primary=table_rows(source)
    try:
        result=rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,
                       source_current=lambda:True,secondary=lambda:True)
        assert result['status']=='restored_verified_checkpoint'
        assert result['checkpoint_sequence']==journal.head['seq']==1
        assert table_rows(target)==primary==table_rows(source)
        assert target.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        archived={table:[] for table in COLUMNS}
        for row in target.execute('SELECT * FROM square_replica_rebuild_archive ORDER BY table_name,row_key'):
            if row['table_name'] in COLUMNS:archived[row['table_name']].append(json.loads(row['payload']))
        for table in COLUMNS:
            assert sorted(archived[table],key=lambda row:json.dumps(row,sort_keys=True))==sorted(before[table],key=lambda row:json.dumps(row,sort_keys=True))
        assert remote.db.execute('SELECT ready FROM square_restore_checkpoints').fetchone()[0]==1
        assert cache.info['data_hash']==inspect(cache.path)['data_hash']
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
        assert catch_up(target,journal,journal.head)==2
        assert target.execute('SELECT used FROM budgets WHERE category=?',('posts',)).fetchone()[0]==8
        assert target.execute('SELECT value FROM state WHERE key=?',('publisher_blocked',)).fetchone()[0]=='true'
    finally:
        source.close();target.close();cache.close()


@pytest.mark.parametrize('case',('primary','source_changed','cancelled','corrupt_file','archive_failure','restore_failure','wrong_source_marker'))
def test_failed_rebuild_preserves_target_and_never_advances_checkpoint(cluster,case):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    before=table_rows(target)
    meta=dict(target.execute('SELECT * FROM square_replica_meta').fetchone())
    stop=Event()
    if case=='cancelled':stop.set()
    if case=='wrong_source_marker':
        source.execute('UPDATE state SET value=? WHERE key=?',('{"required":true,"status":"needs_import"}','database_migration'))
    if case=='archive_failure':
        target.archive_replica=lambda ident:(_ for _ in ()).throw(DatabaseUnavailable('private details'))
    if case=='restore_failure':
        original=target.execute
        def execute(sql,args=None):
            if sql.startswith('INSERT INTO entities'):raise DatabaseUnavailable('private failure')
            return original(sql,args)
        target.execute=execute
    if case=='corrupt_file':
        from square_desk.replica_snapshot import checkpoint
        checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
        cache.path.write_bytes(b'corrupt')
    try:
        with pytest.raises(DatabaseUnavailable):
            rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,stop=stop,
                    source_current=lambda:case!='source_changed',secondary=lambda:case!='primary')
        assert table_rows(target)==before
        assert dict(target.execute('SELECT * FROM square_replica_meta').fetchone())==meta
    finally:
        source.close();target.close();cache.close()


def test_snapshot_is_consistent_while_live_source_advances_and_replay_finishes(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    original=source.stream_rows
    advanced=[]
    def rows(table):
        if not advanced:
            advanced.append(True)
            engine.execute('INSERT INTO state VALUES (?,?)',('later-mutation','true'))
        yield from original(table)
    source.stream_rows=rows
    try:
        result=rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,
                       source_current=lambda:True,secondary=lambda:True)
        assert result['checkpoint_sequence']==1 and journal.head['seq']==2
        assert target.execute('SELECT value FROM state WHERE key=?',('later-mutation',)).fetchone() is None
        assert catch_up(target,journal,journal.head)==2
        assert target.execute('SELECT value FROM state WHERE key=?',('later-mutation',)).fetchone()[0]=='true'
    finally:
        source.close();target.close();cache.close()


def test_published_checkpoint_reused_for_second_replica_without_reexport(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    second=SnapshotAdapter(nodes['supabase'])
    cache=SnapshotCache()
    another=SnapshotCache()
    try:
        rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,
                source_current=lambda:True,secondary=lambda:True)
        source.snapshot=lambda:(_ for _ in ()).throw(AssertionError('Must reuse durable checkpoint'))
        result=rebuild(second,source,journal,journal.head,error,provider='aiven',cache=another,
                       source_current=lambda:True,secondary=lambda:True)
        assert result['checkpoint_id']==cache.info['id']
        assert table_rows(second)==table_rows(target)
    finally:
        source.close();target.close();second.close();cache.close();another.close()


def test_entity_afterimages_replicate_unlogged_rows_without_retrying_primary_mutation(cluster):
    remote,journal,nodes,engine=cluster
    engine.entity_afterimages=True
    source=SnapshotAdapter(nodes['aiven'])
    target=SnapshotAdapter(nodes['neon'])
    source.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
        ('afterimage-row','draft','approved',10.0,11.0,None,'afterimage-fp','{"receipt":"real"}'))
    try:
        engine.execute('UPDATE entities SET status=?,updated=? WHERE id=? AND status=?',
                       ('published',12.0,'afterimage-row','approved'))
        assert catch_up(target,journal,journal.head)==1
        row=target.execute('SELECT * FROM entities WHERE id=?',('afterimage-row',)).fetchone()
        assert row['status']=='published' and row['payload']=='{"receipt":"real"}' and row['created']==10.0
        seq=journal.head['seq']
        assert engine.execute('UPDATE entities SET status=? WHERE id=? AND status=?',
                              ('queued','afterimage-row','approved')).rowcount==0
        assert journal.head['seq']==seq
    finally:source.close();target.close()


def test_background_rebuilds_both_replicas_and_retains_active_provider(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    source.close();target.close()
    engine.snapshot_rebuilds=True
    engine.factory=lambda name:SnapshotAdapter(nodes[name])
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
        report=engine.diagnostics()
        assert report['active_provider']=='aiven'
        for name in ('neon','supabase'):
            assert report['providers'][name]['status']=='ready'
            assert report['providers'][name]['applied_sequence']==1
            assert report['providers'][name]['snapshot_rebuild']['status']=='restored_verified_checkpoint'
    finally:
        for connection in clients.values():connection.close()
        if engine.rebuild_cache:engine.rebuild_cache.close()


@pytest.mark.parametrize('case',('fence_loss','target_promoted','source_changed','readback_corrupt','checkpoint_corrupt'))
def test_rebuild_verification_losses_roll_back_all_target_changes(cluster,case):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    before=table_rows(target)
    meta=dict(target.execute('SELECT * FROM square_replica_meta').fetchone())
    current=[True]
    secondary=[True]
    original=target.archive_replica
    def archive(ident):
        result=original(ident)
        if case=='fence_loss':remote.db.execute("UPDATE square_cluster SET owner='other-writer'")
        if case=='target_promoted':secondary[0]=False
        if case=='source_changed':current[0]=False
        return result
    target.archive_replica=archive
    if case=='readback_corrupt':
        rows=target.stream_rows
        def stream(table):
            for row in rows(table):
                if table=='entities':row={**row,'status':'corrupted'}
                yield row
        target.stream_rows=stream
    if case=='checkpoint_corrupt':
        from square_desk.replica_snapshot import checkpoint
        checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
        cache.close();cache=SnapshotCache()
        remote.db.execute("UPDATE square_restore_chunks SET data='invalid-base64' WHERE part=0")
    try:
        with pytest.raises(DatabaseUnavailable):
            rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,
                    source_current=lambda:current[0],secondary=lambda:secondary[0])
        # Bypass deliberately corrupt readback hook when checking actual data.
        target.stream_rows=lambda table:SnapshotAdapter.stream_rows(target,table)
        assert table_rows(target)==before
        assert dict(target.execute('SELECT * FROM square_replica_meta').fetchone())==meta
    finally:source.close();target.close();cache.close()


def test_rebuild_lost_commit_ack_recovers_by_watermark_without_second_restore(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    nodes['neon'].lose_commit_ack=True
    try:
        with pytest.raises(DatabaseUnavailable):
            rebuild(target,source,journal,journal.head,error,provider='aiven',cache=cache,
                    source_current=lambda:True,secondary=lambda:True)
        assert table_rows(target)==table_rows(source)
        assert catch_up(target,journal,journal.head)==1
        assert target.execute('SELECT COUNT(*) FROM square_replica_rebuild_proof').fetchone()[0]==1
    finally:source.close();target.close();cache.close()


def test_restart_selects_current_aiven_and_immediately_enables_background_rebuild(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    source.close();target.close()
    from square_desk.database_failover import FailoverConnection
    restarted=FailoverConnection({name:name for name in ('neon','aiven','supabase')},journal,
        factory=lambda name:SnapshotAdapter(nodes[name]),background=False)
    try:
        assert restarted.current=='aiven'
        assert restarted.nodes['neon'].last_error=='replica_row_count_mismatch'
        assert restarted.nodes['neon'].until==0
    finally:restarted.close()


def test_failed_new_capture_invalidates_file_cache_and_old_durable_checkpoint_remains_usable(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    from square_desk.replica_snapshot import checkpoint
    cache=SnapshotCache()
    first=checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
    engine.execute('INSERT INTO state VALUES (?,?)',('new-checkpoint','true'))
    original=source.stream_rows
    source.stream_rows=lambda table:(_ for _ in ()).throw(DatabaseUnavailable('Interrupted capture'))
    try:
        with pytest.raises(DatabaseUnavailable):checkpoint(cache,source,journal,journal.head,'aiven',2,lambda:True,None)
        assert cache.info is None
        source.stream_rows=original
        reused=checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
        assert reused['snapshot_hash']==first['snapshot_hash']==inspect(cache.path)['snapshot_hash']
    finally:source.close();target.close();cache.close()


def test_checkpoint_reclaims_only_unpublished_staging_and_preserves_verified_snapshots(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    from square_desk.replica_snapshot import checkpoint
    cache=SnapshotCache()
    try:
        first=checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
        remote.db.execute('INSERT INTO square_restore_checkpoints VALUES (?,?,?,?,?,?,?,?,?,?)',
            ('partial',journal.cluster,journal.head['baseline'],1,journal.head['head_hash'],'aiven','{}',1000,1,0))
        remote.db.execute('INSERT INTO square_restore_chunks VALUES (?,?,?)',('partial',0,'incomplete'))
        engine.execute('INSERT INTO state VALUES (?,?)',('later-checkpoint','true'))
        checkpoint(cache,source,journal,journal.head,'aiven',2,lambda:True,None)
        assert remote.db.execute("SELECT COUNT(*) FROM square_restore_checkpoints WHERE id='partial'").fetchone()[0]==0
        assert remote.db.execute('SELECT ready FROM square_restore_checkpoints WHERE id=?',(first['id'],)).fetchone()[0]==1
        assert remote.db.execute('SELECT COUNT(*) FROM square_restore_checkpoints WHERE ready=1').fetchone()[0]==2
    finally:source.close();target.close();cache.close()
