import asyncio
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from square_desk.bandwidth import request_hook
from square_desk.config import Settings
from square_desk.models import utc
from square_desk.postgres import DatabaseUnavailable
from square_desk.service import Desk
from square_desk.store import Store
from square_desk.telegram_control import TelegramControl
from square_desk.tracking import advance
from square_desk.turso_journal import encode
from test_square_control_runtime import desk_for, update
from test_square_database_failover import HttpJournal, BASE
from test_square_desk import snapshot


def test_journal_sends_encoded_payload_once_and_retains_capacity_check():
    remote=HttpJournal();journal=remote.journal()
    work=[{'sql':'INSERT INTO state VALUES (?,?)','args':['large-evidence',' '.join(str(i*i) for i in range(5000))],'count':1}]
    try:
        journal.initialize(BASE)
        journal.append(work)
        item=next(item for request in remote.requests for item in request['requests']
            if item.get('stmt',{}).get('sql','').startswith('INSERT INTO square_journal'))['stmt']
        encoded=encode(work)
        assert sum(argument.get('value')==encoded for argument in item['args'])==1
        assert item['args'][9]=={'type':'integer','value':str(len(encoded))}
        assert 'log_bytes+?<=?' in item['sql']
        assert remote.db.execute('SELECT payload FROM square_journal').fetchone()[0]==encoded
    finally:
        journal.client.close();remote.db.close()


@pytest.mark.parametrize('delta,accepted',[(0,True),(-1,False)])
@pytest.mark.parametrize('value',['simple value','unicode value ₹ 日 😀'])
def test_single_payload_capacity_boundary_preserves_exact_guard(delta,accepted,value):
    remote=HttpJournal();journal=remote.journal()
    work=[{'sql':'INSERT INTO state VALUES (?,?)','args':['test',value],'count':1}]
    try:
        journal.initialize(BASE)
        journal.max_bytes=len(encode(work))+delta
        if accepted:
            journal.append(work)
            assert remote.db.execute('SELECT seq,log_bytes FROM square_cluster').fetchone()==(1,len(encode(work)))
        else:
            with pytest.raises(DatabaseUnavailable):journal.append(work)
            assert remote.db.execute('SELECT seq,log_bytes FROM square_cluster').fetchone()==(0,0)
            assert not remote.db.execute('SELECT * FROM square_journal').fetchall()
    finally:
        journal.client.close();remote.db.close()


def signal_payload(snap):
    return {'id':'paper-test','symbol':snap.symbol,'source':snap.source,'quote':snap.quote,
        'entry':200.0,'stop':50.0,'target1':500.0,'target2':650.0,'direction':'long',
        'as_of':snap.as_of,'last_bar':snap.as_of,'expires_at':utc()+86400,'mfe_r':0,'mae_r':0}


@pytest.mark.parametrize('state',['watching','triggered'])
def test_signal_with_no_new_bar_or_logical_change_does_not_write(state):
    db=Store(':memory:');snap=snapshot()
    try:
        db.insert('signal',signal_payload(snap),state,id='paper-test')
        before=db.get('paper-test')
        queries=[];db.conn.set_trace_callback(queries.append)
        advance(db,snap,Settings())
        assert db.get('paper-test')==before
        assert not any(q.startswith('UPDATE entities') for q in queries)
    finally:db.close()


@pytest.mark.parametrize('state',['watching','triggered'])
def test_new_candle_tracking_remains_durable(state):
    db=Store(':memory:');snap=snapshot()
    try:
        db.insert('signal',signal_payload(snap),state,id='paper-test')
        later=snapshot(end=snap.as_of+900)
        advance(db,later,Settings())
        after=db.get('paper-test')
        assert after['payload']['last_bar']==later.as_of
        assert after['status']==state
        if state=='triggered':assert after['payload']['mae_r']>0
    finally:db.close()


def test_expired_unchanged_signal_is_still_closed_as_unresolved():
    db=Store(':memory:');snap=snapshot()
    try:
        payload=signal_payload(snap);payload['expires_at']=utc()-1
        db.insert('signal',payload,'watching',id='paper-test')
        advance(db,snap,Settings())
        assert db.get('paper-test')['status']=='expired_unresolved'
    finally:db.close()


def test_automatic_scan_buffer_already_preserves_unchanged_signal_without_write():
    from square_desk.planning import PlanningStore
    db=Store(':memory:');snap=snapshot()
    try:
        db.insert('signal',signal_payload(snap),'watching',id='paper-test')
        queries=[];db.conn.set_trace_callback(queries.append)
        buffered=PlanningStore(db)
        advance(buffered,snap,Settings())
        buffered.flush()
        assert not any(q.startswith('UPDATE entities') for q in queries)
    finally:db.close()


def test_completed_cycle_is_memory_observation_not_replicated_timestamp_write(tmp_path):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={}))) as client:
            desk=Desk(Settings(database=':memory:',artifacts=str(tmp_path)),client=client)
            desk.db.set('last_scan_attempt',utc());desk.db.set('last_maintenance',utc())
            desk.weekly=lambda:None;desk.cleanup.maybe_start=lambda:None
            original=desk.db.set
            def save(key,value):
                assert key!='last_cycle','Idle liveness timestamp must not create a journal mutation'
                return original(key,value)
            desk.db.set=save
            try:
                await desk.cycle()
                observed=desk.runtime_status.snapshot()['worker']
                assert observed['last_completed_cycle_at']>0
                assert 0<=observed['last_completed_cycle_age_seconds']<1
            finally:await desk.close()
    asyncio.run(run())


def test_completed_cycle_starts_unknown_after_restart_and_never_reads_database(tmp_path):
    async def run():
        desk=desk_for(tmp_path)
        state=desk.db.state
        desk.db.state=lambda *a:(_ for _ in ()).throw(AssertionError('RAM status read database'))
        try:
            assert desk.runtime_status.snapshot()['worker']['last_completed_cycle_at'] is None
            desk.runtime_status.completed()
            assert desk.runtime_status.snapshot()['worker']['last_completed_cycle_at']>0
        finally:
            desk.db.state=state;await desk.close()
    asyncio.run(run())


def test_empty_live_image_queue_needs_no_extra_control_reads(tmp_path):
    async def run():
        desk=desk_for(tmp_path);desk.s.paper_mode=False;desk.s.live_enabled=True
        original=desk.db.state
        def state(key,default=None):
            assert key not in ('paused','emergency_stop','publisher_blocked','image_retry_after')
            return original(key,default)
        desk.db.state=state
        try:
            desk.start_image_upload()
            assert desk.image_upload_task is None
        finally:
            desk.db.state=original;await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('control',['paused','emergency_stop','publisher_blocked','image_retry_after'])
def test_real_image_candidate_retains_all_control_gates(tmp_path,control):
    async def run():
        desk=desk_for(tmp_path);desk.s.paper_mode=False;desk.s.live_enabled=True
        desk.db.insert('draft',{'image':'a'*64+'.png','priority':50},'approved',id='real-candidate')
        desk.db.set(control,utc()+300 if control=='image_retry_after' else True)
        try:
            desk.start_image_upload()
            assert desk.image_upload_task is None
            assert desk.db.get('real-candidate')['status']=='approved'
        finally:await desk.close()
    asyncio.run(run())


def test_idle_poll_uses_long_poll_with_transport_deadline_and_short_send_deadline(tmp_path):
    async def run():
        desk=desk_for(tmp_path);control=TelegramControl(desk,asyncio.get_running_loop());calls=[]
        def respond(request):
            calls.append((request.url.path.split('/')[-1],json.loads(request.content),request.extensions['timeout']))
            return httpx.Response(200,json={'ok':True,'result':[]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client
            await control.step()
            await control.api('sendMessage',{'chat_id':7,'text':'bounded reply'})
        assert calls[0][1]['timeout']==20
        assert calls[0][2]['read']==25 and calls[0][2]['connect']==4
        assert calls[1][2]['read']==4
        await desk.close()
    asyncio.run(run())


def test_pending_reply_does_not_wait_for_long_poll(tmp_path):
    async def run():
        desk=desk_for(tmp_path);control=TelegramControl(desk,asyncio.get_running_loop());calls=[]
        item=update(1,'/status')
        control.pending[1]={'update':item,'message':item['message'],'done':False,'direct':True,'sent':False}
        def respond(request):
            body=json.loads(request.content)
            calls.append((request.url.path.split('/')[-1],body))
            return httpx.Response(200,json={'ok':True,'result':[] if request.url.path.endswith('/getUpdates') else {'message_id':1}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client
            await control.step()
            assert calls[0][1]['timeout']==0 and any(name=='sendMessage' for name,body in calls)
            if control.offset_future:await asyncio.wrap_future(control.offset_future[1])
        await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('after',[False,None])
def test_authority_is_rechecked_after_poll_before_mutation_or_offset_ack(tmp_path,after):
    async def run():
        desk=desk_for(tmp_path);control=TelegramControl(desk,asyncio.get_running_loop())
        observations=iter([True,after]);sent=[]
        async def authority():return next(observations)
        control.authority=authority
        def respond(request):
            if request.url.path.endswith('/getUpdates'):
                return httpx.Response(200,json={'ok':True,'result':[update(1,'/pause')]})
            sent.append(request)
            return httpx.Response(200,json={'ok':True,'result':{'message_id':1}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client;await control.step();await asyncio.sleep(0)
            assert control.forward is None and control.offset==0 and not sent
            assert not desk.db.state('paused',False) and not desk.db.list('command')
        await desk.close()
    asyncio.run(run())


def test_shutdown_cancels_outstanding_long_poll_without_processing_or_ack(tmp_path):
    async def run():
        desk=desk_for(tmp_path);control=TelegramControl(desk,asyncio.get_running_loop());entered=asyncio.Event()
        async def respond(request):
            entered.set();await asyncio.Event().wait()
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client;task=asyncio.create_task(control.step())
            await asyncio.wait_for(entered.wait(),1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
            assert control.offset==0 and control.forward is None
        await desk.close()
    asyncio.run(run())


def test_default_independent_client_counts_outgoing_payloads_without_logging_values(tmp_path):
    async def run():
        desk=desk_for(tmp_path);control=TelegramControl(desk,asyncio.get_running_loop())
        async with control.client_factory() as client:
            assert request_hook in client.event_hooks['request']
        await desk.close()
    asyncio.run(run())
