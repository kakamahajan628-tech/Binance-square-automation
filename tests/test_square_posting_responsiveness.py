import asyncio
import json

import httpx
import pytest

from square_desk.service import Desk
from square_desk.models import utc
from test_square_image_responsiveness import ready_text,suppress_unrelated_work
from test_square_media import live_settings


def test_stalled_scan_does_not_delay_due_post_or_status_and_uses_one_scan_slot(tmp_path):
    async def run():
        sent=[]
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
            (sent.append(request) or httpx.Response(200,json={'code':'000000','data':{'id':'one-receipt'}}))))
        desk=Desk(live_settings(tmp_path,hourly_cap=3,gap_seconds=1200),client=client)
        desk.background_scans=True
        suppress_unrelated_work(desk)
        desk.db.set('scan_requested',True)
        text_id=ready_text(desk)
        entered,release,cancelled=asyncio.Event(),asyncio.Event(),asyncio.Event()
        calls=[]
        async def stalled():
            calls.append(True);entered.set()
            try:await release.wait()
            finally:cancelled.set()
        desk.scan=stalled
        try:
            await asyncio.wait_for(desk.cycle(),.5)
            await asyncio.wait_for(entered.wait(),.5)
            assert desk.db.get(text_id)['status']=='published' and len(sent)==1
            report=json.loads(await asyncio.wait_for(desk.telegram.command('/status'),.5))
            assert report['scan_running'] and report['images']['daily_cap']==25
            desk.db.set('scan_requested',True)
            await asyncio.wait_for(desk.cycle(),.5)
            assert calls==[True] and len(sent)==1 and desk.db.state('scan_requested') is True
            await desk.telegram.command('/pause')
            assert desk.db.state('paused') is True
            await desk.close()
            assert cancelled.is_set() and desk.scan_task is None
        finally:
            release.set()
            if not desk.stopping:await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('control',('paused','emergency_stop','publisher_blocked','spacing'))
def test_background_scan_keeps_publication_gates_and_spacing(tmp_path,control):
    async def run():
        sent=[]
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
            (sent.append(request) or httpx.Response(200,json={'code':'000000','data':{'id':'receipt'}}))))
        desk=Desk(live_settings(tmp_path,hourly_cap=3,gap_seconds=1200),client=client)
        desk.background_scans=True
        suppress_unrelated_work(desk)
        ident=ready_text(desk)
        if control=='spacing':
            payload=desk.db.get(ident)['payload']
            desk.db.insert('draft',{**payload,'submitted_at':utc()},'published')
        else:desk.db.set(control,True)
        desk.db.set('scan_requested',True)
        async def scan():await asyncio.Event().wait()
        desk.scan=scan
        try:
            await asyncio.wait_for(desk.cycle(),.5)
            assert not sent and desk.db.get(ident)['status']!='published'
            assert desk.s.image_daily_cap==25 and desk.s.hourly_cap==3 and desk.s.gap_seconds==1200
        finally:await desk.close()
    asyncio.run(run())


def test_lease_loss_cancels_background_scan(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path))
        entered,cancelled=asyncio.Event(),asyncio.Event()
        async def stalled():
            entered.set()
            try:await asyncio.Event().wait()
            finally:cancelled.set()
        desk.scan_task=asyncio.create_task(stalled())
        desk.task=asyncio.create_task(asyncio.sleep(100))
        desk.lease_owned=True
        await entered.wait()
        desk.db.acquire_lease=lambda *args:False
        await desk.heartbeat()
        assert not desk.lease_owned and cancelled.is_set() and desk.scan_task is None
        await desk.close()
        assert desk.task.cancelled()
    asyncio.run(run())


def test_background_scan_error_is_reaped_and_next_requested_scan_can_run(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path))
        desk.background_scans=True
        suppress_unrelated_work(desk)
        async def failed():raise ValueError('private-provider-response')
        desk.scan_task=asyncio.create_task(failed())
        await asyncio.sleep(0)
        try:
            await desk.cycle()
            assert desk.scan_last_error=='ValueError' and desk.scan_task is None
            assert 'private-provider-response' not in json.dumps(desk.db.logs())
            ran=asyncio.Event()
            async def next_scan():ran.set()
            desk.scan=next_scan
            desk.db.set('scan_requested',True)
            await desk.cycle()
            await asyncio.wait_for(ran.wait(),.5)
            await desk.cycle()
            assert desk.scan_last_error==''
        finally:await desk.close()
    asyncio.run(run())


def test_compact_status_uses_two_selects_and_full_output_remains_available(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path))
        queries=[]
        desk.db.conn.set_trace_callback(queries.append)
        try:
            report=json.loads(await desk.telegram.command('/status'))
            assert len([sql for sql in queries if sql.startswith('SELECT')])==2
            assert len(json.dumps(report,indent=2))<3900
            assert report['images']['daily_cap']==25
            full=json.loads(await desk.telegram.command('/status full'))
            assert 'ai' in full and 'cleanup' in full and 'publication' in full
        finally:await desk.close()
    asyncio.run(run())
