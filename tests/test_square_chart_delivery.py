"""Exercise actual Desk deadlines across synchronous operational-loop stalls."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event, Timer
import time

import pytest

from square_desk.config import Settings
from square_desk.models import utc
from square_desk.chart_worker import RenderWork
from square_desk.service import Desk
from test_square_desk import draft_payload, event, snapshot
from test_square_image_recovery import used


def ready_chart(desk):
    snap = snapshot()
    desk.db.set('cache:BTC:900', snap.dict())
    payload = draft_payload(event(snap))
    payload.update(generated_by='ai', human_review_required=False)
    ident = desk.db.insert('draft', payload, 'approved')
    filename = desk.render_image({'payload': payload, 'data': snap.dict()})
    return ident, filename


@pytest.mark.parametrize('on_time', [True, False])
def test_event_loop_stall_uses_actual_render_completion_time(tmp_path, on_time):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True))
        ident, filename = ready_chart(desk)
        started, release = Event(), Event()
        times = {}
        def render(plan):
            times['start'] = time.monotonic()
            started.set()
            assert release.wait(2)
            times['finished'] = time.monotonic()
            return filename
        desk.render_image = render
        desk.s.image_render_timeout_seconds = .2
        task = asyncio.create_task(desk.make_image_async(ident, automatic=True))
        timer = Timer(.02 if on_time else .32, release.set)
        try:
            assert await asyncio.to_thread(started.wait, 1)
            timer.start()
            time.sleep(.5)  # Simulate synchronous PostgreSQL/journal work on the loop.
            assert (times['finished'] - times['start'] < .2) is on_time
            if on_time:
                assert await task == filename
                assert desk.db.get(ident)['payload']['image'] == filename
                assert used(desk) == 1
            else:
                with pytest.raises(asyncio.TimeoutError):
                    await task
                assert desk.db.get(ident)['payload'].get('image') is None
                assert used(desk) == 0
            timing = desk.image_status()['last_render_timing']
            assert timing['completed_within_deadline'] is on_time
            assert timing['completion_delivery_delay_seconds'] > .1
            if on_time:
                assert timing['worker_seconds'] < .2
                assert timing['result_observed_after_seconds'] > .2
            assert desk.db.get(ident)['status'] == 'approved'
        finally:
            release.set()
            timer.cancel()
            if task.done():
                try:
                    task.result()
                except BaseException:
                    pass
            else:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('guard', ['cancelled', 'lease_changed', 'expired', 'worker_error'])
def test_on_time_result_still_obeys_cancellation_ownership_freshness_and_render_errors(tmp_path, monkeypatch, guard):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            mode='automatic', ai_auto_publish=True, auto_images=True))
        ident, filename = ready_chart(desk)
        desk.publisher.worker_owner = 'old-worker'
        desk.db.set('worker_lease', {'owner': 'old-worker', 'until': utc() + 120})
        started, release = Event(), Event()
        def render(plan):
            started.set()
            assert release.wait(2)
            if guard == 'worker_error':
                raise ValueError('private invalid chart payload')
            return filename
        desk.render_image = render
        desk.s.image_render_timeout_seconds = .15
        task = asyncio.create_task(desk.make_image_async(ident, automatic=True))
        timer = Timer(.01, release.set)
        try:
            assert await asyncio.to_thread(started.wait, 1)
            timer.start()
            time.sleep(.3)
            if guard == 'cancelled':
                task.cancel()
                expected, code = asyncio.CancelledError, 'cancelled'
            elif guard == 'lease_changed':
                desk.db.set('worker_lease', {'owner': 'successor', 'until': utc() + 120})
                expected, code = ValueError, 'worker_lease_lost'
            elif guard == 'expired':
                future_time = utc() + 3600
                # Only chart evidence ages here; the replacement lease stays valid.
                desk.db.set('worker_lease', {'owner': 'old-worker', 'until': future_time + 120})
                monkeypatch.setattr('square_desk.service.utc', lambda: future_time)
                expected, code = ValueError, 'evidence_expired'
            else:
                expected, code = ValueError, 'render_invalid'
            with pytest.raises(expected):
                await task
            row = desk.db.get(ident)
            assert not row['payload'].get('image')
            assert row['status'] == ('chart_rendering' if guard == 'lease_changed' else 'approved')
            assert used(desk) == (1 if guard == 'lease_changed' else 0)
            report = desk.image_status()
            assert report['last_failure']['code'] == code
            assert report['last_render_timing']['completed_within_deadline'] is True
            assert 'private invalid chart payload' not in json.dumps(report)
        finally:
            release.set()
            timer.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('on_time', [True, False])
def test_already_finished_work_is_accepted_only_if_the_original_deadline_was_met(on_time):
    async def run():
        def render(plan):
            if not on_time:
                time.sleep(.08)
            return 'chart.png'
        work = RenderWork(render, {}, .04)
        with ThreadPoolExecutor(max_workers=1) as pool:
            raw = pool.submit(work)
            await asyncio.to_thread(raw.result, 1)
            await asyncio.sleep(.06)
            if on_time:
                assert await work.wait(raw) == 'chart.png'
            else:
                with pytest.raises(asyncio.TimeoutError):
                    await work.wait(raw)
            assert work.timings()['completed_within_deadline'] is on_time
    asyncio.run(run())
