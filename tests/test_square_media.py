import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient

from square_desk.app import create_app
from square_desk.config import Settings
from square_desk.media import MediaError, restore_png
from square_desk.models import utc
from square_desk.service import Desk
from square_desk.content import deterministic_draft
from test_square_desk import draft_payload, event, snapshot


def live_settings(tmp_path, **kwargs):
    return Settings(database=':memory:', artifacts=str(tmp_path), paper_mode=False, live_enabled=True,
        square_key='private-square-key', admin_token='test-admin-password',
        publishing_hours=tuple(range(24)),
        policy_reviewed=datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat(), **kwargs)


def attached_chart(desk, article=False):
    snap = snapshot()
    desk.db.set('cache:BTC:900', snap.dict())
    p = draft_payload(event(snap))
    if article:
        p.update(article=True, body=(p['body'] + '\n\n') * (1200 // len(p['body'].split()) + 1))
    ident = desk.db.insert('draft', p, 'approved')
    filename = desk.make_image(ident)
    desk.review(ident, True)
    desk.scheduler.allocate()
    assert desk.db.get(ident)['status'] == 'queued'
    return ident, filename


@pytest.mark.parametrize('article', [False, True])
def test_official_upload_then_image_or_cover_publish_from_disk(tmp_path, article):
    async def run():
        requests, states = [], []
        desk = None
        def respond(request):
            requests.append(request)
            if request.url.path.endswith('/image/presignedUrl'):
                assert request.headers['X-Square-OpenAPI-Key'] == 'private-square-key'
                assert request.url.path.startswith('/bapi/composite/v2/')
                states.append(desk.db.list('draft')[0]['status'])
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart?signature=private', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                assert 'X-Square-OpenAPI-Key' not in request.headers
                assert request.headers['Content-Type'] == 'image/png'
                assert request.content.startswith(b'\x89PNG')
                return httpx.Response(200)
            if request.url.path.endswith('/image/imageStatus'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'status': 1, 'imageUrl': 'https://public.bnbstatic.com/chart.png'}})
            assert request.url.path.endswith('/content/add')
            assert request.url.path.startswith('/bapi/composite/v1/')
            states.append(desk.db.list('draft')[0]['status'])
            body = json.loads(request.content)
            if article:
                assert body['contentType'] == 2 and body['cover'].endswith('chart.png')
                assert 'imageList' not in body and body['title']
            else:
                assert body['contentType'] == 1 and body['imageList'] == ['https://public.bnbstatic.com/chart.png']
                assert 'cover' not in body
            return httpx.Response(200, json={'code': '000000', 'data': {'id': 'square-receipt'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk, article)
            original = (tmp_path / filename).read_bytes()
            await desk.publisher.publish(ident)
            p = desk.db.get(ident)
            assert p['status'] == 'published'
            assert p['payload']['receipt']['adapter'] == 'official_square_image'
            assert (tmp_path / filename).read_bytes() == original
            assert len(requests) == 4 and states == ['preparing_media', 'sending']
            await desk.publisher.publish(ident)
            assert len(requests) == 4  # Published content is never replayed.
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['processing', 'changed', 'transport', 'quota'])
def test_upload_failures_do_not_submit_or_block_text_publisher(tmp_path, failure):
    async def run():
        requests = []
        def respond(request):
            requests.append(request)
            assert not request.url.path.endswith('/content/add')
            if request.url.path.endswith('/image/presignedUrl'):
                if failure == 'quota':
                    return httpx.Response(200, json={'code': '220014'})
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                if failure == 'transport':
                    raise httpx.ReadTimeout('https://s3/?private-signature')
                return httpx.Response(200)
            return httpx.Response(200, json={'code': '000000', 'data': {'status': 2, 'failedReason': 'private-body'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk)
            if failure == 'changed':
                (tmp_path / filename).write_bytes(b'changed')
            await desk.publisher.publish(ident)
            row = desk.db.get(ident)
            if failure=='transport':
                assert row['status']=='approved' and row['payload']['approved_at']
                assert row['payload']['image_pending']['file_ticket']=='ticket'
            else:
                assert row['status'] == 'review' and row['payload']['approved_at'] is None
            assert not desk.db.state('publisher_blocked', False)
            rejection = desk.publisher.status()['last_rejection']
            if failure!='transport': assert rejection['scope'] == 'draft'
            assert not any(secret in json.dumps(desk.db.logs()) for secret in ('private-signature', 'private-body', 'private-square-key'))
            assert failure != 'changed' or not requests
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('control', ['stop', 'lease', 'expiry', 'approval'])
def test_controls_are_rechecked_after_processing_before_content_post(tmp_path, control):
    async def run():
        desk, ident = None, None
        def respond(request):
            assert not request.url.path.endswith('/content/add')
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                return httpx.Response(200)
            if control == 'stop':
                desk.db.set('emergency_stop', True)
            elif control == 'lease':
                desk.db.set('worker_lease', {'owner': 'different-worker', 'until': utc() + 100})
            elif control == 'approval':
                desk.s.mode = 'approval'
            else:
                p = desk.db.get(ident)['payload']
                p['event']['as_of'] = utc() - desk.s.draft_max_age - 1
                desk.db.update(ident, payload=p)
            return httpx.Response(200, json={'code': '000000', 'data': {
                'status': 1, 'imageUrl': 'https://public.bnbstatic.com/chart.png'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk)
            if control == 'lease':
                desk.publisher.worker_owner = 'worker'
                desk.db.set('worker_lease', {'owner': 'worker', 'until': utc() + 100})
            elif control == 'approval':
                desk.s.mode = 'automatic'
                p = desk.db.get(ident)['payload']
                p.update(approved_at=None, human_review_required=False)
                desk.db.update(ident, payload=p)
            await desk.publisher.publish(ident)
            assert desk.db.get(ident)['status'] == {'stop': 'queued', 'lease': 'queued', 'expiry': 'expired', 'approval': 'review'}[control]
            assert any('withheld' in row['message'] or 'lease lost' in row['message'] for row in desk.db.logs())
            await desk.close()
    asyncio.run(run())


def test_recovery_distinguishes_uploading_from_uncertain_content_post(tmp_path):
    desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
    uploading = desk.db.insert('draft', draft_payload(), 'preparing_media', due=utc())
    submitted = desk.db.insert('draft', draft_payload(), 'sending')
    desk.db.recover()
    assert desk.db.get(uploading)['status'] == 'approved' and desk.db.get(uploading)['due'] is None
    assert desk.db.get(submitted)['status'] == 'uncertain'
    asyncio.run(desk.close())


def test_automatic_image_opt_in_and_manual_change_invalidates_approval(tmp_path):
    desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path), mode='automatic', ai_auto_publish=True))
    snap = snapshot()
    # Preserve exact archived evidence; current cache can contain another candle.
    desk.db.archive_snapshot(snap.dict())
    p = {**draft_payload(event(snap)), 'generated_by': 'ai', 'approved_at': None}
    ident = desk.db.insert('draft', p, 'approved')
    desk.automatic_image(ident)
    assert desk.db.get(ident)['payload']['image'] is None
    desk.s.auto_images = True
    desk.automatic_image(ident)
    row = desk.db.get(ident)
    assert row['status'] == 'approved' and not row['payload']['human_review_required']
    assert row['payload']['image_hash']
    desk.make_image(ident)
    assert desk.db.get(ident)['status'] == 'review' and desk.db.get(ident)['payload']['human_review_required']
    asyncio.run(desk.close())


def test_chart_cap_retains_validated_text_and_failed_chart_does_not_spend_budget(tmp_path):
    desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path), mode='automatic', ai_auto_publish=True,
                         auto_images=True, image_daily_cap=0))
    snap = snapshot()
    desk.db.archive_snapshot(snap.dict())
    p = {**draft_payload(event(snap)), 'generated_by': 'ai'}
    ident = desk.db.insert('draft', p, 'approved')
    desk.automatic_image(ident)
    assert desk.db.get(ident)['status'] == 'approved' and not desk.db.get(ident)['payload']['image']
    desk.s.image_daily_cap = 25
    desk.s.draft_max_age = 1
    p['event']['as_of'] = utc() - 10
    desk.db.update(ident, payload=p)
    with pytest.raises(ValueError, match='expired'):
        desk.make_image(ident)
    day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
    assert desk.db.budget_used(day, 'images') == 0
    asyncio.run(desk.close())


def test_disk_preview_is_private_and_missing_file_is_not_restored_from_database(tmp_path):
    desk = Desk(live_settings(tmp_path))
    ident, filename = attached_chart(desk)
    original = (tmp_path / filename).read_bytes()
    desk.db.conn.execute('UPDATE entities SET created=? WHERE id=?', (utc() - 10 * 86400, filename))
    desk.db.prune(30, 2, 7)
    assert desk.db.get(filename) is not None
    app = create_app(desk.s, desk=desk, start_worker=False)
    with TestClient(app) as client:
        assert client.get('/artifacts/' + filename).status_code == 401
        response = client.get('/artifacts/' + filename, auth=('desk', 'test-admin-password'))
        assert response.status_code == 200 and response.content == original
        (tmp_path / filename).unlink()
        assert client.get('/artifacts/' + filename, auth=('desk', 'test-admin-password')).status_code == 404
        assert not (tmp_path / filename).exists()
        desk.db.update(ident, status='published')
        desk.db.prune(30, 2, 7)
        assert desk.db.get(filename) is None


def test_changed_or_traversal_media_cannot_be_restored(tmp_path):
    desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),publishing_hours=tuple(range(24))))
    with pytest.raises(MediaError):
        restore_png(desk.db, tmp_path, '../private.png')
    ident, filename = attached_chart(desk)
    with pytest.raises(MediaError) as raised:
        restore_png(desk.db, tmp_path, filename, 'wrong-approved-hash')
    assert raised.value.code == 'media_changed'
    asyncio.run(desk.close())


def test_processing_pending_is_resumable_and_is_not_an_uncertain_post(tmp_path, monkeypatch):
    async def no_delay(seconds):
        return None
    monkeypatch.setattr('square_desk.media.asyncio.sleep', no_delay)
    async def run():
        polls = []
        def respond(request):
            assert not request.url.path.endswith('/content/add')
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 123}})
            if request.method == 'PUT':
                return httpx.Response(200)
            polls.append(request)
            return httpx.Response(200, json={'code': '000000', 'data': {'status': 0}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk)
            await desk.publisher.publish(ident)
            assert len(polls) == 1
            assert desk.db.get(ident)['status'] == 'approved'
            assert desk.db.get(ident)['payload']['image_pending']['file_ticket']==123
            assert desk.db.get(ident)['payload']['approved_at']
            assert 'rejection_code' not in desk.db.get(ident)['payload']
            assert not desk.publisher.status()['uncertain_submissions']
            await desk.close()
    asyncio.run(run())


def test_image_content_submission_timeout_stays_uncertain_and_is_not_replayed(tmp_path):
    async def run():
        posts = []
        def respond(request):
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'presignedUrl': 'https://test.s3.amazonaws.com/chart', 'fileTicket': 'ticket'}})
            if request.method == 'PUT':
                return httpx.Response(200)
            if request.url.path.endswith('/image/imageStatus'):
                return httpx.Response(200, json={'code': '000000', 'data': {
                    'status': 1, 'imageUrl': 'https://public.bnbstatic.com/chart.png'}})
            posts.append(request)
            raise httpx.ReadTimeout('Private content endpoint')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk)
            await desk.publisher.publish(ident)
            assert desk.db.get(ident)['status'] == 'uncertain'
            desk.db.recover()
            await desk.publisher.publish(ident)
            assert len(posts) == 1
            await desk.close()
    asyncio.run(run())


def test_automatic_scan_does_not_attach_same_chart_twice_in_paper_mode(tmp_path, monkeypatch):
    async def run():
        snap = snapshot()
        ev = event(snap, category='volume')
        class AI:
            async def generate(self, *args):
                return deterministic_draft(ev)
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path), symbols=('BTC',),
            mode='automatic', ai_auto_publish=True, auto_images=True, publishing_hours=tuple(range(24))))
        desk.content.ai = AI()
        async def fetch(symbol, interval=900):
            return snap if interval == 900 else snapshot(interval=interval)
        desk.pool.fetch = fetch
        monkeypatch.setattr('square_desk.service.events', lambda row, settings: [ev])
        monkeypatch.setattr('square_desk.service.setup', lambda *args: None)
        await desk.scan()
        row = desk.db.list('draft')[0]
        assert row['status'] == 'approved' and row['payload']['image']
        day = datetime.now(ZoneInfo(desk.s.timezone)).date().isoformat()
        assert desk.db.budget_used(day, 'images') == 1
        await desk.close()
    asyncio.run(run())
