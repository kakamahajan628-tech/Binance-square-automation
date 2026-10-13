import asyncio
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from square_desk.config import Settings
from square_desk.education import educational_payload
from square_desk.models import utc
from square_desk.service import Desk
from test_square_desk import draft_payload, event, snapshot
from test_square_media import attached_chart, live_settings


def ready_text(desk):
    p = {**educational_payload(0), 'approved_at': utc()}
    assert not desk.content.checker.check(p)
    return desk.db.insert('draft', p, 'approved')


def suppress_unrelated_work(desk):
    desk.db.set('last_scan_attempt', utc())
    desk.db.set('last_maintenance', utc())
    desk.weekly = lambda: None


def test_stalled_upload_does_not_delay_status_pause_or_ready_text_post(tmp_path):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        submissions, presigns = [], []
        async def respond(request):
            if request.url.path.endswith('/image/presignedUrl'):
                presigns.append(request)
                started.set()
                await release.wait()
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                return httpx.Response(200)
            if request.url.path.endswith('/image/imageStatus'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'status': 1, 'imageUrl': 'https://public.bnbstatic.com/chart.png'}})
            assert request.url.path.endswith('/content/add')
            submissions.append(request)
            return httpx.Response(200, json={'code': '000000', 'data': {'id': 'receipt'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path, gap_seconds=1), client=client)
            image_id, filename = attached_chart(desk)
            text_id = ready_text(desk)
            suppress_unrelated_work(desk)
            try:
                await asyncio.wait_for(desk.cycle(), timeout=.5)
                await asyncio.wait_for(started.wait(), timeout=.5)
                assert desk.db.get(image_id)['status'] == 'uploading_image'
                assert desk.db.get(text_id)['status'] == 'published'
                assert len(submissions) == 1
                status = await asyncio.wait_for(desk.telegram.command('/status'), timeout=.5)
                assert '"upload_active": true' in status
                await asyncio.wait_for(desk.telegram.command('/pause'), timeout=.5)
                assert desk.db.state('paused') is True
                assert 'uploading_image' in await desk.telegram.command('/queue')
                release.set()
                await asyncio.wait_for(desk.image_upload_task, timeout=.5)
                assert desk.db.get(image_id)['status'] == 'approved'
                assert len(submissions) == 1  # The background worker never posts content.
                p = desk.db.get(text_id)['payload']
                p['submitted_at'] = utc() - 2
                desk.db.update(text_id, payload=p)
                await desk.telegram.command('/resume')
                await desk.cycle()
                assert desk.db.get(image_id)['status'] == 'published'
                assert len(submissions) == 2 and len(presigns) == 1  # Reuse processed upload.
            finally:
                release.set()
                await desk.close()
    asyncio.run(run())


def test_stalled_render_runs_on_one_thread_and_does_not_block_controls_or_text(tmp_path):
    async def run():
        started, release = threading.Event(), threading.Event()
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
                httpx.Response(200, json={'code': '000000', 'data': {'id': 'receipt'}}))) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            snap = snapshot()
            desk.db.set('cache:BTC:900', snap.dict())
            image_id = desk.db.insert('draft', draft_payload(event(snap)), 'review')
            text_id = ready_text(desk)
            suppress_unrelated_work(desk)
            render = desk.render_image
            def slow_render(plan):
                assert threading.current_thread().name.startswith('square-chart')
                started.set()
                release.wait(timeout=2)
                return render(plan)
            desk.render_image = slow_render
            task = asyncio.create_task(desk.make_image_async(image_id))
            try:
                assert await asyncio.to_thread(started.wait, 1)
                assert desk.db.get(image_id)['status'] == 'chart_rendering'
                await asyncio.wait_for(desk.telegram.command('/status'), timeout=.5)
                with pytest.raises(ValueError, match='renderer busy'):
                    await desk.make_image_async(image_id)
                await asyncio.wait_for(desk.cycle(), timeout=.5)
                assert desk.db.get(text_id)['status'] == 'published'
                release.set()
                await asyncio.wait_for(task, timeout=1)
                assert desk.db.get(image_id)['status'] == 'review'
                day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
                assert desk.db.budget_used(day, 'images') == 1
            finally:
                release.set()
                await desk.close()
    asyncio.run(run())


def test_background_upload_cancel_recovers_as_approved_without_content_uncertainty(tmp_path):
    async def run():
        started, hold = asyncio.Event(), asyncio.Event()
        async def respond(request):
            assert not request.url.path.endswith('/content/add')
            started.set()
            await hold.wait()
            raise AssertionError('Upload should have been cancelled')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk)
            desk.start_image_upload()
            await asyncio.wait_for(started.wait(), .5)
            await desk.stop_image_upload()
            desk.db.recover()
            assert desk.db.get(ident)['status'] == 'approved'
            assert not desk.publisher.status()['uncertain_submissions']
            await desk.close()
    asyncio.run(run())


def test_render_timeout_refunds_budget_restores_draft_and_does_not_attach_late_png(tmp_path):
    async def run():
        release = threading.Event()
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        snap = snapshot()
        desk.db.set('cache:BTC:900', snap.dict())
        ident = desk.db.insert('draft', draft_payload(event(snap)), 'approved')
        render = desk.render_image
        def slow_render(plan):
            release.wait(timeout=2)
            return render(plan)
        desk.render_image = slow_render
        # Keep the test fast; validated deployment settings enforce >=5 seconds.
        desk.s.image_render_timeout_seconds = .03
        try:
            with pytest.raises(asyncio.TimeoutError):
                await desk.make_image_async(ident)
            row = desk.db.get(ident)
            assert row['status'] == 'approved' and row['payload']['image'] is None
            day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
            assert desk.db.budget_used(day, 'images') == 0
        finally:
            release.set()
            await desk.close()
    asyncio.run(run())


def test_ai_illustration_cap_is_configured_but_feature_has_no_provider_or_calls(tmp_path, monkeypatch):
    monkeypatch.setenv('DESK_DATABASE', ':memory:')
    monkeypatch.setenv('DESK_ARTIFACTS', str(tmp_path))
    monkeypatch.setenv('DESK_AI_IMAGE_DAILY_CAP', '5')
    settings = Settings.from_env()
    desk = Desk(settings)
    status = desk.status()['images']
    assert status['ai_illustrations_daily_cap'] == 5
    assert status['ai_illustrations_enabled'] is False
    assert status['ai_illustrations_configured'] is False
    assert not desk.image_upload_task
    assert status['render_workers'] == 1 and status['upload_workers'] == 1
    asyncio.run(desk.close())


def test_retiring_upload_worker_cannot_reset_successor_draft_on_unexpected_error(tmp_path):
    async def run():
        desk = Desk(live_settings(tmp_path))
        ident, filename = attached_chart(desk)
        desk.db.update(ident, status='uploading_image')
        desk.publisher.worker_owner = 'retiring-worker'
        async def fail_after_handoff(draft_id):
            desk.db.set('worker_lease', {'owner': 'successor', 'until': utc() + 120})
            raise OSError('Unexpected local upload error')
        desk.publisher.prepare_image_job = fail_after_handoff
        try:
            await desk.upload_image_background(ident)
            row = desk.db.get(ident)
            assert row['status'] == 'uploading_image'
            assert row['payload']['approved_at']
        finally:
            await desk.close()
    asyncio.run(run())
