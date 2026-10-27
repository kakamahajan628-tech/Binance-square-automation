"""Real render lifecycle regressions; no live provider requests or posting."""
import asyncio
from datetime import datetime
import json
from threading import Event
from zoneinfo import ZoneInfo

import httpx
import pytest

from square_desk.config import Settings
from square_desk.service import Desk
from test_square_desk import draft_payload, event, snapshot
from test_square_media import live_settings
from square_desk import chart_views, images
from square_desk.analysis import studies
from square_desk.models import utc
from square_desk.media import MediaError
from square_desk.postgres import DatabaseUnavailable
from square_desk.telegram_control import TelegramControl
from test_square_control_runtime import update


def candidate(desk, *, symbol='BTC'):
    snap = snapshot(symbol=symbol)
    desk.db.set(f'cache:{symbol}:900', snap.dict())
    payload = draft_payload(event(snap))
    payload.update(generated_by='ai', risk='medium', human_review_required=False)
    return desk.db.insert('draft', payload, 'approved')


def used(desk):
    day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
    return desk.db.budget_used(day, 'images')


def test_failed_render_claim_does_not_leak_image_budget(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        ident = candidate(desk)
        original = desk.db.update
        def fail_claim(key, **values):
            if values.get('status') == 'chart_rendering':
                raise OSError('private-storage-path')
            return original(key, **values)
        desk.db.update = fail_claim
        try:
            with pytest.raises(OSError):
                await desk.make_image_async(ident)
            assert used(desk) == 0
            assert desk.db.get(ident)['status'] == 'approved'
        finally:
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure,expected', [
    ('cap', 'budget_exhausted'), ('expired', 'evidence_expired'),
    ('missing', 'evidence_missing'), ('invalid', 'render_invalid'),
    ('storage', 'storage_io_error'), ('png', 'png_invalid'),
])
def test_automatic_failures_have_exact_redacted_reason_and_keep_text(tmp_path, failure, expected):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True, image_daily_cap=0 if failure == 'cap' else 25))
        ident = candidate(desk)
        if failure == 'expired':
            row = desk.db.get(ident)
            row['payload']['event']['as_of'] = utc() - 2000
            desk.db.update(ident, payload=row['payload'])
        elif failure == 'missing':
            desk.db.set('cache:BTC:900', None)
        elif failure in ('invalid', 'storage', 'png'):
            error = {'invalid': ValueError('private token or URL'),
                     'storage': OSError('private storage path'), 'png': MediaError('private-response')}[failure]
            def fail(plan):
                raise error
            desk.render_image = fail
        try:
            await desk.automatic_image_async(ident)
            report = desk.image_status(full=True)
            assert report['last_failure']['code'] == expected
            assert report['last_saved_outcome']['code'] == expected
            assert report['last_failure']['stage'] == ('planning' if failure in ('cap', 'expired', 'missing') else 'rendering')
            assert report['budget']['used_or_reserved'] == 0
            assert desk.db.get(ident)['status'] == 'approved'
            assert not desk.db.get(ident)['payload'].get('image')
            assert 'private' not in json.dumps(report)
            logs = desk.db.logs(10)
            assert any(expected in row['message'] for row in logs if row['category'] == 'IMAGE')
            assert all('private' not in row['message'] for row in logs)
        finally:
            await desk.close()
    asyncio.run(run())


def test_cleanup_failure_keeps_original_exception_and_retired_worker_cannot_refund(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        ident = candidate(desk)
        owner = 'retiring-worker'
        desk.publisher.worker_owner = owner
        desk.db.set('worker_lease', {'owner': owner, 'until': utc() + 120})
        entered, release = Event(), Event()
        def fail(plan):
            entered.set()
            release.wait(2)
            raise OSError('private renderer path')
        desk.render_image = fail
        task = asyncio.create_task(desk.make_image_async(ident))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            desk.db.set('worker_lease', {'owner': 'successor', 'until': utc() + 120})
            release.set()
            with pytest.raises(OSError, match='private renderer path'):
                await task
            assert used(desk) == 1  # Successor/recovery owns any unresolved reservation.
            assert desk.db.get(ident)['status'] == 'chart_rendering'
            assert desk.image_status()['last_failure']['budget_refunded'] is False
        finally:
            release.set()
            await desk.close()
    asyncio.run(run())


def test_diagnostic_failures_cannot_mask_render_error_or_print_secrets(tmp_path, caplog):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        ident = candidate(desk)
        state = desk.db.state
        def fail(plan):
            raise OSError('private renderer path')
        desk.render_image = fail
        def unavailable(*args, **kwargs):
            raise RuntimeError('postgresql://private:password@private.invalid/db')
        saved_set = desk.db.set
        def fail_diagnostic(key, value):
            if key == 'image_last_outcome':
                raise RuntimeError('postgresql://private:password@private.invalid/db')
            return saved_set(key, value)
        desk.db.set = fail_diagnostic
        # Fail recovery and observation persistence after the plan was reserved.
        original = desk.db.update
        def claim(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get('status') == 'chart_rendering':
                desk.db.state = unavailable
            return result
        desk.db.update = claim
        try:
            with pytest.raises(OSError):
                await desk.make_image_async(ident)
            report = desk.image_status()
            assert report['last_failure']['code'] == 'storage_io_error'
            assert report['budget']['used_or_reserved'] is None
            assert 'postgresql://' not in caplog.text and 'private renderer path' not in caplog.text
        finally:
            desk.db.state = state
            await desk.close()
    asyncio.run(run())


def test_image_status_is_independent_and_contains_no_database_payloads(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path), telegram_token='unit-token',
            telegram_admins=(7,), webhook_secret='unit-secret'))
        desk.lease_owned = True
        state = desk.db.state
        desk.db.state = lambda *args: (_ for _ in ()).throw(RuntimeError('private database connection'))
        sent = []
        def respond(request):
            if request.url.path.endswith('/getUpdates'):
                return httpx.Response(200, json={'ok': True, 'result': [update(1, '/image_status')]})
            sent.append(json.loads(request.content))
            return httpx.Response(200, json={'ok': True, 'result': {'message_id': 1}})
        control = TelegramControl(desk, asyncio.get_running_loop())
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            control.client = client
            try:
                await asyncio.wait_for(control.step(), .3)
                report = json.loads(sent[0]['text'])
                assert report['daily_cap'] == 25 and report['budget']['used_or_reserved'] is None
                assert report['status_source'] == 'memory_observations'
                assert 'private' not in sent[0]['text']
                assert control.offset == 2
                if control.offset_future:
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
            finally:
                desk.db.state = state
                await desk.close()
    asyncio.run(run())


def test_font_reuse_preserves_exact_chart_pixels_for_all_views(tmp_path):
    snap = snapshot()
    metrics = studies(snap)
    for view in chart_views.VIEWS:
        before = chart_views.render(snap, metrics, tmp_path, context={'chart_view': view},
            font=images.font, save=images.save_png)
        raw = (tmp_path / before).read_bytes()
        after = images.chart(snap, metrics, tmp_path, context={'chart_view': view})
        assert raw == (tmp_path / after).read_bytes()


def test_automatic_real_chart_upload_and_single_publish_with_image_list(tmp_path):
    async def run():
        requests = []
        def respond(request):
            requests.append(request)
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                assert request.content.startswith(b'\x89PNG')
                return httpx.Response(200)
            if request.url.path.endswith('/image/imageStatus'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'status': 1, 'imageUrl': 'https://public.bnbstatic.com/chart.png'}})
            assert request.url.path.endswith('/content/add')
            assert json.loads(request.content)['imageList'] == ['https://public.bnbstatic.com/chart.png']
            return httpx.Response(200, json={'code': '000000', 'data': {'id': 'mock-receipt'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path, mode='automatic', ai_auto_publish=True, auto_images=True,
                gap_seconds=1200, hourly_cap=3), client=client)
            ident = candidate(desk)
            try:
                assert not desk.content.checker.check(desk.db.get(ident)['payload'])
                await desk.automatic_image_async(ident)
                assert desk.image_status()['last_attachment']['code'] == 'attached'
                assert desk.db.get(ident)['status'] == 'approved' and used(desk) == 1
                desk.start_image_upload()
                assert desk.image_upload_task is not None
                await desk.image_upload_task
                assert desk.image_status()['last_upload']['code'] == 'upload_prepared'
                notices = []
                desk.telegram.notify = lambda text, **kwargs: notices.append(text)
                desk.db.set('last_scan_attempt', utc())
                desk.db.set('last_maintenance', utc())
                desk.weekly = lambda: None
                desk.cleanup.maybe_start = lambda: None
                await desk.cycle()
                assert desk.db.get(ident)['status'] == 'published'
                assert any(ident in text and 'Chart: sent with this post; Square receipt confirmed.' in text for text in notices)
                await desk.publisher.publish(ident)
                assert len(requests) == 4
                assert desk.s.image_daily_cap == 25 and desk.s.gap_seconds == 1200 and desk.s.hourly_cap == 3
            finally:
                await desk.close()
    asyncio.run(run())


def test_unpublished_chart_reservations_still_enforce_existing_cap_25(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True))
        ident = candidate(desk)
        day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
        assert desk.db.reserve_budget(day, 'images', 25, 25)
        try:
            await desk.automatic_image_async(ident)
            assert desk.image_status()['last_failure']['code'] == 'budget_exhausted'
            assert desk.image_status(full=True)['budget']['used_or_reserved'] == 25
            assert not desk.db.get(ident)['payload'].get('image')
            assert desk.db.get(ident)['status'] == 'approved'
        finally:
            await desk.close()
    asyncio.run(run())


def test_pending_media_limit_has_distinct_reason_without_spending_budget(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True))
        for _ in range(3):
            ident = candidate(desk)
            payload = desk.db.get(ident)['payload']
            payload['image'] = 'a' * 64 + '.png'
            desk.db.update(ident, payload=payload)
        ident = candidate(desk, symbol='ETH')
        try:
            await desk.automatic_image_async(ident)
            assert desk.image_status()['last_failure']['code'] == 'queue_full'
            assert used(desk) == 0 and desk.s.image_queue_limit == 3
        finally:
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('committed', [False, True])
def test_attachment_failure_refunds_only_a_confirmed_unused_reservation(tmp_path, monkeypatch, committed):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True))
        ident = candidate(desk)
        if committed:
            attach = desk.attach_rendered_image
            def lost_ack(plan, filename):
                attach(plan, filename)
                raise DatabaseUnavailable('Commit outcome requires confirmation')
            desk.attach_rendered_image = lost_ack
        else:
            def invalid(*args):
                raise MediaError('media_invalid')
            monkeypatch.setattr('square_desk.service.persist_png', invalid)
        try:
            if committed:
                with pytest.raises(DatabaseUnavailable):
                    await desk.make_image_async(ident, automatic=True)
            else:
                with pytest.raises(MediaError):
                    await desk.make_image_async(ident, automatic=True)
            assert used(desk) == (1 if committed else 0)
            row = desk.db.get(ident)
            assert row['status'] == 'approved'
            assert bool(row['payload'].get('image')) is committed
            report = desk.image_status()
            assert report['last_failure']['stage'] == 'attaching'
            assert report['last_failure']['code'] == ('database_unavailable' if committed else 'png_invalid')
            assert report['last_failure']['budget_refunded'] is (not committed)
        finally:
            await desk.close()
    asyncio.run(run())


def test_timed_out_thread_cannot_accumulate_another_render_job(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        ident, other = candidate(desk), candidate(desk, symbol='ETH')
        release, finished = Event(), Event()
        render, calls = desk.render_image, []
        def blocked(plan):
            calls.append(plan['id'])
            try:
                release.wait(2)
                return render(plan)
            finally:
                finished.set()
        desk.render_image = blocked
        desk.s.image_render_timeout_seconds = .03
        try:
            with pytest.raises(asyncio.TimeoutError):
                await desk.make_image_async(ident)
            with pytest.raises(ValueError, match='renderer busy'):
                await desk.make_image_async(other)
            assert used(desk) == 0 and calls == [ident]
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
            await asyncio.sleep(.02)
            assert calls == [ident]
            assert desk.db.get(ident)['payload'].get('image') is None
            assert desk.db.get(other)['payload'].get('image') is None
            desk.s.image_render_timeout_seconds = 5
            assert await desk.make_image_async(other)
            assert used(desk) == 1
        finally:
            release.set()
            await desk.close()
    asyncio.run(run())
