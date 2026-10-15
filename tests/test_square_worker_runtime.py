import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Event,get_ident
from time import perf_counter

from fastapi.testclient import TestClient

from square_desk.app import create_app
from square_desk.config import Settings


class FakeDesk:
    def __init__(self):
        self.telegram=self
        self.started,self.release=Event(),Event()
        self.closed=False
        self.worker_thread=None
    async def start(self):
        self.worker_thread=get_ident()
        self.loop=asyncio.get_running_loop()
    async def close(self):
        assert get_ident()==self.worker_thread
        self.closed=True
    async def command(self,text):
        assert asyncio.get_running_loop() is self.loop
        assert get_ident()==self.worker_thread
        return text
    async def stalled_operation(self):
        self.started.set()
        self.release.wait(5)
    def status(self):
        self.started.set()
        self.release.wait(5)
        return {'blocked':True,'database':'unavailable'}


def settings(tmp_path):
    return Settings(database=':memory:',artifacts=str(tmp_path/'artifacts'),admin_token='local-private')


def test_worker_database_stall_does_not_block_http_liveness(tmp_path):
    desk=FakeDesk()
    app=create_app(settings(tmp_path),desk)
    with TestClient(app) as client:
        future=asyncio.run_coroutine_threadsafe(desk.stalled_operation(),desk.loop)
        try:
            assert desk.started.wait(1)
            start=perf_counter()
            response=client.get('/health')
            assert response.status_code==200 and perf_counter()-start<.75
            assert not future.done()  # It responded while the DB/worker stayed blocked.
            assert client.head('/health').status_code==200
        finally:
            desk.release.set()
            future.result(timeout=2)
    assert desk.closed and not app.state.worker_runtime.thread.is_alive()


def test_database_status_request_does_not_block_http_liveness(tmp_path):
    desk=FakeDesk()
    with TestClient(create_app(settings(tmp_path),desk)) as client:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(client.get,'/health/details',auth=('desk','local-private'))
            try:
                assert desk.started.wait(1)
                start=perf_counter()
                assert client.get('/health').status_code==200 and perf_counter()-start<.75
                assert not future.done()
            finally:
                desk.release.set()
                assert future.result(timeout=2).json()['database']=='unavailable'


def test_http_commands_are_delegated_to_the_same_operational_loop(tmp_path):
    desk=FakeDesk()
    with TestClient(create_app(settings(tmp_path),desk)) as client:
        response=client.post('/api/command',json={'command':'/pause'},auth=('desk','local-private'))
        assert response.status_code==200 and response.json()=={'message':'/pause'}
        assert client.post('/api/command',json={'command':'/pause'}).status_code==401


def test_real_desk_network_client_and_http_controls_share_worker_loop(tmp_path):
    import httpx
    from square_desk.service import Desk
    loops=[]
    async def respond(request):
        loops.append(asyncio.get_running_loop())
        return httpx.Response(200,json={'ok':True})
    desk=Desk(settings(tmp_path),client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
    observed=Event()
    async def cycle():
        await desk.client.get('https://local-test.invalid/public-test')
        observed.set()
    desk.cycle=cycle
    app=create_app(desk.s,desk)
    with TestClient(app) as client:
        assert observed.wait(1)
        response=client.post('/api/command',json={'command':'/pause'},auth=('desk','local-private'))
        assert response.status_code==200 and desk.db.state('paused') is True
        assert loops and all(loop is app.state.worker_runtime.loop for loop in loops)
        assert client.get('/health').status_code==200
