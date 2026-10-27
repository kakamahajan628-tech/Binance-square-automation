"""Replica repair must survive normal rolling-deploy writer handoff."""
import math
import time

import pytest

from test_square_database_failover import cluster
from test_square_snapshot_rebuild import SnapshotAdapter,setup,table_rows
from square_desk.postgres import DatabaseUnavailable


def prepare(cluster):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster)
    source.close();target.close()
    engine.snapshot_rebuilds=True
    engine.factory=lambda name:SnapshotAdapter(nodes[name])
    return remote,journal,nodes,engine


def finish(engine,clients):
    for connection in clients.values():connection.close()
    if engine.rebuild_cache:engine.rebuild_cache.close()


def test_standby_during_rolling_deploy_repairs_after_handoff_without_restart(cluster,monkeypatch):
    remote,journal,nodes,engine=prepare(cluster)
    import square_desk.replica_snapshot as snapshots
    real=snapshots.rebuild
    calls=[]
    def checked(*args,**kwargs):
        calls.append((journal.epoch,time.monotonic()<journal.deadline))
        return real(*args,**kwargs)
    monkeypatch.setattr(snapshots,'rebuild',checked)
    remote.db.execute("UPDATE square_cluster SET owner='retiring-writer',epoch=epoch+1,lease_until=unixepoch()+60")
    journal.epoch,journal.deadline=None,0
    journal.head=journal.observe()
    class Wake:
        turns=0
        def wait(self,timeout):
            self.turns+=1
            if self.turns==2:
                for name in ('neon','supabase'):
                    node=engine.nodes[name]
                    assert node.status=='rebuild_waiting_for_writer',node.snapshot_rebuild
                    assert math.isfinite(node.until) and node.rebuild_attempts==0
                    assert node.seq==0
                    node.until=0  # Advance past the five-second standby wait.
                assert not calls
                remote.db.execute("UPDATE square_cluster SET owner='',lease_until=0")
                journal.acquire(force=True)  # Normal foreground ownership acquisition.
            if self.turns>=3:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        assert len(calls)==2 and engine.current=='aiven'
        assert all(epoch is not None and valid for epoch,valid in calls)
        for name in ('neon','supabase'):
            node=engine.nodes[name]
            assert node.status=='ready' and node.seq==journal.head['seq']==1
            assert node.snapshot_rebuild['status']=='restored_verified_checkpoint'
            assert node.rebuild_attempts==1
    finally:finish(engine,clients)


@pytest.mark.parametrize('code',('snapshot_fence_changed','snapshot_source_changed','snapshot_target_changed'))
def test_temporary_snapshot_authority_changes_retry_without_quarantine(cluster,monkeypatch,code):
    remote,journal,nodes,engine=prepare(cluster)
    import square_desk.replica_snapshot as snapshots
    real=snapshots.rebuild
    calls={}
    waits=[]
    def flaky(target,*args,**kwargs):
        key=target.backend.path
        calls[key]=calls.get(key,0)+1
        if calls[key]<=2:raise DatabaseUnavailable('Authority changed during checkpoint',code=code)
        return real(target,*args,**kwargs)
    monkeypatch.setattr(snapshots,'rebuild',flaky)
    class Wake:
        turns=0
        def wait(self,timeout):
            self.turns+=1
            for node in engine.nodes.values():
                if node.status=='rebuild_retry_wait':
                    assert math.isfinite(node.until)
                    waits.append(node.snapshot_rebuild['reason'])
                    node.until=0  # Simulate elapsed backoff without sleeping.
            if self.turns>=4:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        assert waits==[code]*4
        for name in ('neon','supabase'):
            node=engine.nodes[name]
            assert node.status=='ready' and node.rebuild_attempts==3
            assert node.seq==journal.head['seq']==1
    finally:finish(engine,clients)


def test_real_fence_loss_rolls_back_then_waits_without_taking_over_other_writer(cluster):
    remote,journal,nodes,engine=prepare(cluster)
    original=SnapshotAdapter.archive_replica
    captures=[]
    class LosingTarget(SnapshotAdapter):
        def archive_replica(self,ident):
            result=original(self,ident)
            captures.append(True)
            remote.db.execute("UPDATE square_cluster SET owner='new-writer',epoch=epoch+1,lease_until=unixepoch()+60")
            return result
    engine.factory=lambda name:LosingTarget(nodes[name])
    before={}
    for name in ('neon','supabase'):
        client=SnapshotAdapter(nodes[name]);before[name]=table_rows(client);client.close()
    class Wake:
        turns=0
        def wait(self,timeout):
            self.turns+=1
            if self.turns==2:
                for node in engine.nodes.values():
                    if node.status=='rebuild_retry_wait':
                        assert math.isfinite(node.until)
                        node.until=0
                # Foreground observes ownership loss; background cannot acquire it.
                journal.deadline=0
            if self.turns>=3:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        assert len(captures)==1
        assert remote.db.execute('SELECT owner FROM square_cluster').fetchone()[0]=='new-writer'
        for name in ('neon','supabase'):
            client=SnapshotAdapter(nodes[name])
            assert table_rows(client)==before[name]
            assert client.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==0
            client.close()
            assert engine.nodes[name].status=='rebuild_waiting_for_writer'
    finally:finish(engine,clients)
