import asyncio
import json
from threading import Event,get_ident
import time

import httpx
import pytest

from square_desk.config import Settings
from square_desk.service import Desk
from square_desk.telegram_control import TelegramControl
from square_desk.worker_runtime import WorkerRuntime


def update(ident,text,*,sender=7,chat=None,kind='private'):
    return {'update_id':ident,'message':{'from':{'id':sender},'chat':{'id':sender if chat is None else chat,'type':kind},'text':text}}


def desk_for(tmp_path):
    client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
        httpx.Response(200,json={'ok':True,'result':{'message_id':1}})))
    desk=Desk(Settings(database=':memory:',artifacts=str(tmp_path),telegram_token='unit-token',
        telegram_admins=(7,),webhook_secret='unit-secret'),client=client)
    desk.lease_owned=True
    return desk


def test_status_is_served_while_database_state_and_outbox_are_unavailable(tmp_path):
    async def run():
        desk=desk_for(tmp_path)
        sent=[]
        def response(request):
            if request.url.path.endswith('/getUpdates'):return httpx.Response(200,json={'ok':True,'result':[update(1,'/status')]})
            sent.append(json.loads(request.content));return httpx.Response(200,json={'ok':True,'result':{'message_id':1}})
        control=TelegramControl(desk,asyncio.get_running_loop())
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            control.client=client
            state=desk.db.state
            desk.db.state=lambda *args:(_ for _ in ()).throw(RuntimeError('private connection detail'))
            try:
                await asyncio.wait_for(control.step(),.3)
                report=json.loads(sent[0]['text'])
                assert report['status_source']=='memory_observations'
                assert report['publication']['global_controls'].startswith('Unknown')
                assert len(sent)==1 and control.offset==2
                assert 'private connection detail' not in sent[0]['text']
                if control.offset_future:
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
            finally:
                desk.db.state=state
                await desk.close()
    asyncio.run(run())


def test_status_passes_a_stalled_mutating_command_without_acknowledging_or_replaying_it(tmp_path):
    async def run():
        desk=desk_for(tmp_path)
        sent=[]
        items=[update(1,'/pause'),update(2,'/status')]
        def response(request):
            if request.url.path.endswith('/getUpdates'):return httpx.Response(200,json={'ok':True,'result':items})
            sent.append(json.loads(request.content));return httpx.Response(200,json={'ok':True,'result':{'message_id':1}})
        control=TelegramControl(desk,asyncio.get_running_loop())
        original=desk.telegram.receive
        gate=asyncio.Event()
        calls=[]
        async def receive(item,**kwargs):
            calls.append(item['update_id']);await gate.wait();await original(item,**kwargs)
        desk.telegram.receive=receive
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            control.client=client
            try:
                await control.step()
                await asyncio.sleep(0);await asyncio.sleep(0)
                assert len(sent)==1 and control.offset==0 and calls==[1]
                await control.step()
                assert len(sent)==1 and calls==[1] and control.offset==0
                gate.set()
                await asyncio.wrap_future(control.forward[1])
                await control.step()
                assert desk.db.state('paused') is True
                assert control.offset==3 and calls==[1]
                assert desk.db.list('command')[0]['status']=='applied'
                if control.offset_future:await asyncio.wrap_future(control.offset_future[1])
            finally:
                gate.set();await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('item',(update(1,'/status',sender=8),update(1,'/status',chat=-7,kind='group')))
def test_control_authorization_rejects_other_users_and_groups(tmp_path,item):
    async def run():
        desk=desk_for(tmp_path)
        sent=[]
        def response(request):
            if request.url.path.endswith('/getUpdates'):return httpx.Response(200,json={'ok':True,'result':[item]})
            sent.append(request);raise AssertionError('Unauthorized reply')
        control=TelegramControl(desk,asyncio.get_running_loop())
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            control.client=client
            try:
                await control.step()
                assert not sent and not desk.db.list('command')
                if control.offset_future:await asyncio.wrap_future(control.offset_future[1])
            finally:await desk.close()
    asyncio.run(run())


def test_independent_thread_responds_during_synchronous_worker_block_and_closes_its_client(tmp_path):
    async def run():
        desk=desk_for(tmp_path)
        async def cycle():await asyncio.sleep(100)
        desk.cycle=cycle
        runtime=WorkerRuntime(desk)
        await runtime.start()
        entered,release,sent=Event(),Event(),Event()
        threads=[]
        def respond(request):
            threads.append(get_ident())
            if request.url.path.endswith('/getUpdates'):
                return httpx.Response(200,json={'ok':True,'result':[update(3,'/status')] if entered.is_set() else []})
            report=json.loads(json.loads(request.content)['text'])
            assert report['worker']['loop_responsive'] is False
            assert not release.is_set()
            sent.set();return httpx.Response(200,json={'ok':True,'result':{'message_id':1}})
        control=TelegramControl(desk,runtime.loop,client_factory=lambda:
            httpx.AsyncClient(transport=httpx.MockTransport(respond)))
        desk.telegram.control_runtime=control
        async def block():
            desk.runtime_status.tick_at=time.time()-60
            entered.set();release.wait(timeout=4)
        try:
            await control.start()
            blocker=asyncio.run_coroutine_threadsafe(block(),runtime.loop)
            assert await asyncio.to_thread(entered.wait,1)
            assert await asyncio.to_thread(sent.wait,3)
            assert control.thread.ident in threads and runtime.thread.ident not in threads
            release.set();await asyncio.wrap_future(blocker)
        finally:
            release.set();await runtime.close()
        assert not control.thread.is_alive() and control.client.is_closed
    asyncio.run(run())


@pytest.mark.parametrize('status,body,expected',((409,{},'poller_conflict'),(429,{'parameters':{'retry_after':5}},'rate_limited'),
    (200,{'ok':False},'invalid_response')))
def test_control_reports_api_conflicts_and_respects_backoff(tmp_path,status,body,expected):
    async def run():
        desk=desk_for(tmp_path)
        control=TelegramControl(desk,asyncio.get_running_loop())
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(status,json=body))) as client:
            control.client=client
            await control.api('getUpdates',{})
            assert control.api_status==expected
            if status!=200:assert control.retry_until>time.monotonic()
        await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('case',('cached_owner','server_owner','new_owner','unavailable'))
def test_control_authority_uses_fence_and_stops_retired_pollers(tmp_path,case):
    async def run():
        from types import SimpleNamespace
        desk=desk_for(tmp_path)
        connection=desk.db.conn
        journal=SimpleNamespace(epoch=4,owner='old-owner',cluster='test-cluster',token='private-token',
            deadline=time.monotonic()+30 if case=='cached_owner' else 0,url='https://test.turso.io/v2/pipeline')
        desk.db.conn=SimpleNamespace(journal=journal,replicated=True)
        requests=[]
        def respond(request):
            requests.append(request)
            body=json.loads(request.content)
            assert body['requests'][0]['stmt']['args'][1]['value']=='old-owner'
            if case=='unavailable':raise httpx.ConnectError('private URL')
            rows=[] if case=='new_owner' else [[{'type':'integer','value':'4'}]]
            return httpx.Response(200,json={'results':[{'type':'ok','response':{'type':'execute','result':{'rows':rows}}}]})
        control=TelegramControl(desk,asyncio.get_running_loop())
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client
            try:
                result=await control.authority()
                assert result is (None if case=='unavailable' else False if case=='new_owner' else True)
                assert len(requests)==(0 if case=='cached_owner' else 1)
                if case=='new_owner':
                    await control.step()
                    assert len(requests)==2  # Ownership probes only; no Telegram poll/ack.
            finally:
                desk.db.conn=connection;await desk.close()
    asyncio.run(run())


def test_unverified_authority_serves_readonly_status_without_forwarding_or_acknowledging_mutations(tmp_path):
    async def run():
        desk=desk_for(tmp_path)
        control=TelegramControl(desk,asyncio.get_running_loop())
        async def authority():return None
        control.authority=authority
        sent=[]
        def respond(request):
            if request.url.path.endswith('/getUpdates'):
                return httpx.Response(200,json={'ok':True,'result':[update(1,'/pause'),update(2,'/status')]})
            sent.append(request);return httpx.Response(200,json={'ok':True,'result':{'message_id':1}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client
            await control.step()
            assert len(sent)==1 and control.forward is None and control.offset==0
            assert not desk.db.state('paused',False) and not desk.db.list('command')
        await desk.close()
    asyncio.run(run())


def test_explicit_polling_mode_clears_conflicting_webhook_without_dropping_pending_updates(tmp_path):
    async def run():
        desk=desk_for(tmp_path);desk.s.telegram_polling=True
        control=TelegramControl(desk,asyncio.get_running_loop())
        requests=[]
        def respond(request):
            requests.append((request.url.path.split('/')[-1],json.loads(request.content)))
            if request.url.path.endswith('/getUpdates'):return httpx.Response(409)
            if request.url.path.endswith('/getWebhookInfo'):
                return httpx.Response(200,json={'ok':True,'result':{'url':'https://unit.invalid/private-webhook'}})
            assert request.url.path.endswith('/deleteWebhook')
            return httpx.Response(200,json={'ok':True,'result':True})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client=client
            await control.api('getUpdates',{})
            assert requests[-1]==('deleteWebhook',{'drop_pending_updates':False})
            assert control.api_status=='polling_webhook_conflict_resolved'
            assert 'private-webhook' not in json.dumps(desk.runtime_status.snapshot())
        await desk.close()
    asyncio.run(run())


def test_authenticated_runtime_and_public_observed_readiness_do_not_query_database(tmp_path):
    from fastapi.testclient import TestClient
    from square_desk.app import create_app
    desk=desk_for(tmp_path);desk.s.admin_token='unit-admin'
    original=desk.db.state
    desk.db.state=lambda *args:(_ for _ in ()).throw(AssertionError('Runtime queried database'))
    app=create_app(desk.s,desk=desk,start_worker=False)
    try:
        with TestClient(app) as client:
            assert client.get('/api/runtime').status_code==401
            result=client.get('/api/runtime',auth=('desk','unit-admin'))
            assert result.status_code==200 and result.json()['status_source']=='memory_observations'
            assert client.get('/health').status_code==200
            assert client.get('/ready').status_code==503
            desk.db.state=original
    finally:desk.db.state=original
