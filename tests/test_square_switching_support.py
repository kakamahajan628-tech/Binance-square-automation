import asyncio
import json

import pytest

from square_desk.database_failover import FailoverConnection
from square_desk.postgres import DatabaseUnavailable
from square_desk.replica_snapshot import SnapshotCache,checkpoint
from square_desk.models import utc
from square_desk.store import Store
from test_square_database_failover import cluster
from test_square_snapshot_rebuild import SnapshotAdapter,setup
from test_square_image_responsiveness import ready_text
from test_square_media import live_settings
from square_desk.service import Desk


def adapter_factory(nodes):
    def create(name):
        if nodes[name].down:raise DatabaseUnavailable('Offline provider')
        return SnapshotAdapter(nodes[name])
    return create


def test_bootstrap_uses_durable_verified_checkpoint_when_old_source_is_offline(cluster):
    remote,journal,nodes,old=cluster
    source,target,error=setup(cluster)
    cache=SnapshotCache()
    checkpoint(cache,source,journal,journal.head,'aiven',1,lambda:True,None)
    source.close();target.close();cache.close()
    nodes['aiven'].down=True
    restarted=FailoverConnection({name:name for name in ('neon','aiven','supabase')},journal,
        factory=adapter_factory(nodes),background=False)
    try:
        assert restarted.current in ('neon','supabase')
        row=restarted.execute('SELECT * FROM entities WHERE id=?',('unlogged-row',)).fetchone()
        assert row['status']=='delivered' and row['payload']=='{"text":"original-private"}'
        assert restarted.nodes[restarted.current].seq==journal.head['seq']
        assert restarted.nodes[restarted.current].snapshot_rebuild['status']=='restored_verified_checkpoint'
    finally:
        restarted.close()
        if restarted.rebuild_cache:restarted.rebuild_cache.close()


def test_no_checkpoint_and_missing_primary_refuses_to_invent_a_replica(cluster):
    remote,journal,nodes,old=cluster
    source,target,error=setup(cluster)
    source.close();target.close()
    nodes['aiven'].down=True
    with pytest.raises(DatabaseUnavailable):
        FailoverConnection({name:name for name in ('neon','aiven','supabase')},journal,
            factory=adapter_factory(nodes),background=False)
    for name in ('neon','supabase'):
        replica=SnapshotAdapter(nodes[name])
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==0
        assert replica.execute('SELECT * FROM entities WHERE id=?',('unlogged-row',)).fetchone() is None
        replica.close()


def test_bootstrap_finishes_more_than_one_bounded_turn_without_restart(cluster):
    remote,journal,nodes,old=cluster
    for index in range(95):old.execute('INSERT INTO state VALUES (?,?)',('later:'+str(index),'true'))
    nodes['aiven'].down=True
    restarted=FailoverConnection({name:name for name in ('neon','aiven','supabase')},journal,
        factory=adapter_factory(nodes),background=False,snapshot_rebuilds=False)
    try:
        assert restarted.nodes[restarted.current].seq==95
        assert restarted.execute('SELECT value FROM state WHERE key=?',('later:94',)).fetchone()[0]=='true'
    finally:restarted.close()


def test_read_and_write_failover_before_caller_body_runs_exactly_once(cluster):
    remote,journal,nodes,engine=cluster
    nodes['aiven'].down=True
    calls=[]
    with engine.transaction():
        calls.append(engine.current)
        engine.execute('INSERT INTO state VALUES (?,?)',('single-execution','true'))
    assert len(calls)==1 and calls[0]!='aiven' and journal.head['seq']==1
    assert engine.execute('SELECT value FROM state WHERE key=?',('single-execution',)).fetchone()[0]=='true'


def test_idle_closed_primary_connection_failure_switches_before_mutation(cluster):
    remote,journal,nodes,engine=cluster
    engine.nodes['aiven'].close()
    nodes['aiven'].down=True
    engine.execute('INSERT INTO state VALUES (?,?)',('after-idle','true'))
    assert engine.current!='aiven' and journal.head['seq']==1


def test_normal_reads_use_one_verified_transaction_and_no_redundant_replay(cluster):
    remote,journal,nodes,engine=cluster
    connection=engine.nodes[engine.current].ready()
    queries=[]
    connection.db.set_trace_callback(queries.append)
    engine.execute('SELECT value FROM state WHERE key=?',('paused',)).fetchone()
    assert sum(sql.startswith('BEGIN') for sql in queries)==1
    assert sum('square_replica_meta' in sql for sql in queries)==1


def test_scheduler_flushes_one_final_transition_instead_of_reset_then_requeue(tmp_path):
    desk=Desk(live_settings(tmp_path))
    ident=ready_text(desk)
    writes=[]
    original=desk.db.update
    def update(*args,**kwargs):writes.append((args,kwargs));return original(*args,**kwargs)
    desk.db.update=update
    try:
        now=utc();desk.scheduler.allocate(now)
        assert len(writes)==1 and desk.db.get(ident)['status']=='queued'
        writes.clear();desk.scheduler.allocate(now)
        assert not writes and desk.db.get(ident)['due']==now
        assert desk.scheduler.last_plan['queued']==1
    finally:asyncio.run(desk.close())


def test_recovery_separates_outbox_delivery_from_uncertain_binance_submissions(tmp_path):
    store=Store(':memory:')
    draft=store.insert('draft',{'title':'Possibly accepted'},'sending')
    outbox=store.insert('outbox',{'text':'Telegram reply'},'sending')
    rendering=store.insert('draft',{'title':'Rendering'},'chart_rendering')
    try:
        assert store.recover_submissions()==1
        assert store.get(draft)['status']=='uncertain'
        assert store.get(outbox)['status']=='sending'
        assert store.get(rendering)['status']=='chart_rendering'
        store.recover()
        assert store.get(outbox)['status']=='pending'
        assert store.get(draft)['status']=='uncertain'
    finally:store.close()


def test_slow_notification_flush_does_not_delay_next_posting_cycle(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path))
        from test_square_image_responsiveness import suppress_unrelated_work
        suppress_unrelated_work(desk)
        ident=ready_text(desk)
        async def blocked():await asyncio.Event().wait()
        desk.telegram_task=asyncio.create_task(blocked())
        desk.telegram.flush=blocked
        class Adapter:
            async def send(self,payload):return {'id':'confirmed'}
        desk.publisher.adapter=Adapter()
        try:
            await asyncio.wait_for(desk.cycle(),.5)
            assert desk.db.get(ident)['status']=='published'
        finally:await desk.close()
    asyncio.run(run())


def test_transient_rebuild_errors_recover_after_three_attempts_without_restart(cluster,monkeypatch):
    remote,journal,nodes,engine=cluster
    source,target,error=setup(cluster);source.close();target.close()
    engine.snapshot_rebuilds=True
    engine.factory=adapter_factory(nodes)
    import square_desk.replica_snapshot as snapshots
    real=snapshots.rebuild
    calls={}
    def flaky(target,*args,**kwargs):
        key=target.backend.path
        calls[key]=calls.get(key,0)+1
        if calls[key]<=3:raise DatabaseUnavailable('Temporary error',code='connection_failed')
        return real(target,*args,**kwargs)
    monkeypatch.setattr(snapshots,'rebuild',flaky)
    class Wake:
        count=0
        def wait(self,timeout):
            self.count+=1
            for node in engine.nodes.values():
                if node.status=='rebuild_retry_wait':node.until=0
            if self.count>6:engine.stopped.set()
        def clear(self):pass
        def set(self):pass
    engine.wake=Wake()
    clients={}
    try:
        engine._replicate_clients(clients)
        for name in ('neon','supabase'):
            assert engine.nodes[name].status=='ready'
            assert engine.nodes[name].rebuild_attempts==4
    finally:
        for client in clients.values():client.close()
        if engine.rebuild_cache:engine.rebuild_cache.close()


@pytest.mark.parametrize('fault',('primary_down_before_publish','receipt_ack_lost_after_post'))
def test_real_scheduler_and_publisher_complete_once_across_database_switch(cluster,tmp_path,fault):
    async def run():
        import httpx
        remote,journal,nodes,engine=cluster
        engine.entity_afterimages=True
        facade=Store(':memory:');facade.conn.close()
        facade.conn,facade.postgres=engine,True
        # Isolate the publication scenario from intentionally restricted rows
        # in the legacy fault fixture, retaining the real gates and scheduler.
        engine.execute("DELETE FROM entities WHERE kind='draft'")
        facade.set('publisher_blocked',False)
        requests=[]
        def respond(request):
            assert request.url.path.endswith('/content/add')
            requests.append(request)
            if fault=='receipt_ack_lost_after_post':remote.lose_append_ack=True
            return httpx.Response(200,json={'code':'000000','data':{'id':'one-real-shaped-receipt'}})
        client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        desk=Desk(live_settings(tmp_path,mode='automatic',ai_auto_publish=True),store=facade,client=client)
        facade.close=lambda:None  # The cluster fixture owns engine shutdown.
        desk.lease_owned=True
        desk.publisher.worker_owner=desk.owner
        facade.set('worker_lease',{'owner':desk.owner,'until':utc()+120})
        ident=ready_text(desk)
        from test_square_image_responsiveness import suppress_unrelated_work
        suppress_unrelated_work(desk);desk.cleanup.maybe_start=lambda:None
        try:
            if fault=='primary_down_before_publish':nodes['aiven'].down=True
            if fault=='receipt_ack_lost_after_post':
                with pytest.raises(DatabaseUnavailable):await desk.cycle()
            else:await desk.cycle()
            row=facade.get(ident)
            assert row['status']=='published' and row['payload']['receipt']['id']=='one-real-shaped-receipt'
            assert engine.current!='aiven'
            await desk.cycle()
            assert len(requests)==1 and facade.get(ident)['status']=='published'
        finally:await desk.close()
    asyncio.run(run())
