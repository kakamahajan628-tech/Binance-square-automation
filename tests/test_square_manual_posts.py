import asyncio
from dataclasses import replace
from datetime import datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
from PIL import Image
import pytest

from square_desk.config import Settings
from square_desk.compliance import Compliance
from square_desk.manual_posts import (ManualPosts, ManualAwarePolicy, KIND, CAP,
    QUEUED, MEDIA, SENDING, PUBLISHED, UNCERTAIN, REVIEW, WAITING, CANCELLED, aligned_text)
from square_desk.manual_media import normalized_png, download_image
from square_desk.models import utc
from square_desk.postgres import DatabaseUnavailable
from square_desk.retention import run_cleanup, LEDGER
from square_desk.service import Desk
from square_desk.store import Store


def picture(mid=1, caption='/manual My original meme caption 😂', sender=99):
    return {'message_id': mid, 'from': {'id': sender}, 'chat': {'id': sender, 'type': 'private'},
            'photo': [{'file_id': 'photo_' + str(mid), 'width': 64, 'height': 64, 'file_size': 200}],
            'caption': caption}


def text_message(text, mid=20, sender=99):
    return {'message_id': mid, 'from': {'id': sender}, 'chat': {'id': sender, 'type': 'private'}, 'text': text}


@pytest.fixture
def lane(tmp_path):
    data = io.BytesIO(); Image.new('RGB', (64, 64), 'gold').save(data, format='JPEG')
    raw = data.getvalue()
    calls, notices = [], []
    behaviour = {}
    def respond(request):
        calls.append(request)
        if request.url.path.endswith('/getFile'):
            return httpx.Response(200, json={'ok': True, 'result': {
                'file_path': behaviour.get('path', 'photos/file_1.jpg'), 'file_size': len(raw)}})
        if request.method == 'GET':
            return httpx.Response(behaviour.get('download_status', 200), content=raw)
        if request.url.path.endswith('/image/presignedUrl'):
            return httpx.Response(200, json={'code': '000000', 'data': {
                'presignedUrl': 'https://upload.example/image', 'fileTicket': 'unit-ticket'}})
        if request.method == 'PUT':
            return httpx.Response(200)
        if request.url.path.endswith('/image/imageStatus'):
            if behaviour.get('pause_after_upload'): db.set('paused', True)
            if behaviour.get('lose_lease'): db.set('worker_lease', {'owner': 'new-worker', 'until': utc()+120})
            if behaviour.get('edit_after_upload'):
                row = db.list(KIND, [MEDIA])[0]
                row['payload']['body'] = 'Different text after authorization'
                db.update(row['id'], payload=row['payload'])
            return httpx.Response(200, json={'code': '000000', 'data': {
                'status': behaviour.get('processing', 1), 'imageUrl': 'https://image.example/manual.png'}})
        if request.url.path.endswith('/content/add'):
            if behaviour.get('lose_lease_after_submit'):
                db.set('worker_lease', {'owner': 'new-worker', 'until': utc()+120})
            if behaviour.get('timeout'): raise httpx.ReadTimeout('private-token-url', request=request)
            if behaviour.get('malformed_receipt'): return httpx.Response(200, json={})
            if 'receipt_override' in behaviour: return httpx.Response(200, json=behaviour['receipt_override'])
            if behaviour.get('http_status'): return httpx.Response(behaviour['http_status'], headers=behaviour.get('response_headers', {}))
            return httpx.Response(200, json={'code': behaviour.get('code', '000000'),
                'data': {'id': 'confirmed-manual', 'shareLink': 'https://square.example/post'}})
        raise AssertionError('Unexpected endpoint')
    s = Settings(database=':memory:', artifacts=str(tmp_path/'artifacts'), paper_mode=False,
        live_enabled=True, square_key='unit-square-key', admin_token='unit-admin',
        policy_reviewed=datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat(),
        telegram_token='unit-telegram-token', telegram_admins=(99,), webhook_secret='unit-webhook',
        daily_target=50, hourly_cap=3, gap_seconds=1200, image_daily_cap=25, neon_batch_seconds=0,
        publishing_hours=tuple(range(24)), mode='automatic', ai_auto_publish=True)
    s.validate()
    db = Store(':memory:')
    desk = SimpleNamespace(s=s, db=db, owner='unit-worker', lease_owned=True, stopping=False,
                           image_upload_task=None, telegram=SimpleNamespace(
                           notify=lambda text, key=None: notices.append((text, key))))
    db.acquire_lease(desk.owner, 120)
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    manual = ManualPosts(desk, client=client)
    manual.start = lambda: None  # Process deterministically; no background service/network.
    desk.manual = manual
    bundle = SimpleNamespace(manual=manual, desk=desk, db=db, s=s, calls=calls,
                             notices=notices, behaviour=behaviour, client=client, raw=raw)
    yield bundle
    asyncio.run(client.aclose())
    db.close()


def accepted(lane, mid=1, caption='/manual My original meme caption 😂'):
    lane.manual.receive(picture(mid, caption))
    return lane.db.list(KIND)[0]['id']


def submissions(lane):
    return [r for r in lane.calls if r.url.path.endswith('/content/add')]


def test_explicit_caption_publishes_original_text_and_user_image_without_auto_budgets(lane):
    original = 'My funny first line 😂\n\nSecond paragraph #Meme'
    ident = accepted(lane, caption='/manual ' + original)
    lane.db.set('last_publication', 123.0)
    asyncio.run(lane.manual.process(ident))
    row = lane.db.get(ident)
    assert row['kind'] == KIND and row['status'] == PUBLISHED
    assert row['payload']['body'] == original
    assert json.loads(submissions(lane)[0].content) == {
        'contentType': 1, 'bodyTextOnly': original, 'imageList': ['https://image.example/manual.png']}
    assert lane.db.budget_used(lane.manual.day(), 'manual_posts') == 1
    assert lane.db.budget_used(lane.manual.day(), 'images') == 0
    assert lane.db.count('draft') == 0 and lane.db.state('last_publication') == 123.0
    assert row['payload']['receipt']['adapter'] == 'official_square_image'
    assert 'file/bot' not in json.dumps(lane.manual.status())


def test_photo_then_separate_text_pairs_and_does_not_submit_early(lane):
    lane.manual.receive(text_message('/manual'))
    lane.manual.receive(picture(caption=''))
    row = lane.db.list(KIND)[0]
    assert row['status'] == WAITING and not lane.calls
    assert lane.db.budget_used(lane.manual.day(), 'manual_posts') == 0
    lane.manual.receive(text_message('A long original caption\n\nWith paragraphs.'))
    assert lane.db.get(row['id'])['status'] == QUEUED
    asyncio.run(lane.manual.process(row['id']))
    assert len(submissions(lane)) == 1


def test_reply_to_own_photo_with_manual_text_is_supported(lane):
    message = {**text_message('/manual The text for my picture.'), 'reply_to_message': picture(caption='')}
    lane.manual.receive(message)
    assert lane.db.list(KIND)[0]['payload']['body'] == 'The text for my picture.'
    message['reply_to_message']['from']['id'] = 123
    with pytest.raises(ValueError): lane.manual.receive(message)


def test_ten_manual_reservations_are_separate_and_eleventh_is_rejected(lane):
    for n in range(10): accepted(lane, n+1, '/manual My original caption ' + str(n))
    with pytest.raises(ValueError, match='10/10'): accepted(lane, 11)
    assert lane.db.count(KIND, [QUEUED]) == CAP
    assert lane.db.count('draft') == 0 and lane.s.daily_target == 50
    assert lane.manual.status()['used_or_reserved'] == 10
    ident = lane.db.list(KIND)[0]['id']
    lane.manual.control('/manual_cancel', ident, 99)
    assert lane.db.get(ident)['status'] == CANCELLED
    assert lane.manual.status()['used_or_reserved'] == 9
    accepted(lane, 12)
    assert lane.manual.status()['used_or_reserved'] == 10


@pytest.mark.parametrize('caption', ('An unarmed picture', '/status', '/post_now BTC'))
def test_random_images_and_unrelated_commands_never_authorize_publication(lane, caption):
    lane.manual.receive(picture(caption=caption))
    assert lane.db.count(KIND) == 0 and not lane.calls


def test_same_message_id_is_received_at_most_once(lane):
    accepted(lane)
    lane.manual.receive(picture())
    assert lane.db.count(KIND) == 1 and lane.manual.status()['used_or_reserved'] == 1


@pytest.mark.parametrize('bad', ('', 'guaranteed profit for everyone', 'bad\x01text', 'x'*2101))
def test_invalid_manual_text_is_not_silently_rewritten_or_submitted(lane, bad):
    with pytest.raises(ValueError): aligned_text(bad, lane.s)
    assert not lane.calls


def test_daily_manual_reset_requires_explicit_retry_of_old_queued_content(lane):
    ident = accepted(lane)
    old_day = lane.manual.day()
    lane.manual.day = lambda: '2099-01-01'
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert lane.db.budget_used(old_day, 'manual_posts') == 0
    assert not lane.calls
    lane.manual.control('/manual_retry', ident, 99)
    assert lane.db.get(ident)['status'] == QUEUED
    assert lane.db.budget_used('2099-01-01', 'manual_posts') == 1


@pytest.mark.parametrize('control', ('paused', 'emergency_stop', 'publisher_blocked'))
def test_manual_lane_respects_global_controls_before_any_http(lane, control):
    ident = accepted(lane)
    lane.db.set(control, True)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == QUEUED and not lane.calls


@pytest.mark.parametrize('change', ('pause_after_upload', 'lose_lease'))
def test_controls_or_lease_lost_after_upload_prevent_content_submission(lane, change):
    ident = accepted(lane)
    lane.behaviour[change] = True
    asyncio.run(lane.manual.process(ident))
    assert not submissions(lane)
    assert lane.db.get(ident)['status'] != PUBLISHED


def test_automatic_due_post_has_priority_without_manual_reallocating_it(lane):
    ident = accepted(lane)
    auto = lane.db.insert('draft', {'title':'Automatic','body':'original automatic content'}, 'queued', due=utc())
    before = lane.db.get(auto)
    asyncio.run(lane.manual.process(ident))
    assert not lane.calls and lane.db.get(auto) == before
    assert lane.db.get(ident)['status'] == QUEUED


def test_manual_publication_does_not_reset_automatic_gap_or_daily_counts(lane):
    lane.db.insert('draft', {'submitted_at': utc()-1201}, 'published')
    candidate = {'id': 'next-auto', 'payload': {}}
    regular = ManualAwarePolicy(lane.s, lane.db, lane.manual)
    before = regular.caps()
    assert regular.gate(candidate) is None
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    assert regular.gate(candidate) is None
    assert regular.caps() == before == {'daily':50,'hourly':3,'gap':1200}
    assert lane.db.count('draft', ['published']) == 1


def test_manual_ten_do_not_consume_automatic_fifty_slots(lane):
    lane.s.hourly_cap = 100
    lane.s.gap_seconds = 1
    policy = ManualAwarePolicy(lane.s, lane.db, lane.manual)
    lane.manual.has_items = True
    for n in range(10):
        lane.db.insert(KIND, {'submitted_at': utc()-120-n}, PUBLISHED)
    for n in range(50):
        assert policy.gate({'id': 'new', 'payload': {}}) is None
        lane.db.insert('draft', {'submitted_at': utc()-120-n}, 'published')
    assert policy.gate({'id':'new','payload':{}}) == 'Daily publication limit reached'


def test_manual_can_publish_after_automatic_daily_target_is_reached(lane):
    for _ in range(50): lane.db.insert('draft', {'submitted_at':utc()}, 'published')
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == PUBLISHED
    assert lane.db.count('draft', ['published']) == 50


def test_shared_platform_cap_still_blocks_additional_manual_publication(lane):
    for _ in range(100): lane.db.insert('draft', {'submitted_at':utc()}, 'published')
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == QUEUED and not lane.calls


@pytest.mark.parametrize('failure', ('timeout', '500'))
def test_ambiguous_content_post_is_never_automatically_retried(lane, failure):
    ident = accepted(lane)
    lane.behaviour['timeout' if failure == 'timeout' else 'http_status'] = True if failure == 'timeout' else 500
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == UNCERTAIN
    assert lane.manual.status()['used_or_reserved'] == 1
    asyncio.run(lane.manual.process(ident))
    with pytest.raises(ValueError): lane.manual.control('/manual_retry', ident, 99)
    assert len(submissions(lane)) == 1
    assert 'private-token-url' not in str(lane.notices)


def test_lost_receipt_database_write_recovers_uncertain_without_another_post(lane):
    ident = accepted(lane)
    original = lane.db.update
    def update(ident, **kwargs):
        if kwargs.get('status') == PUBLISHED: raise DatabaseUnavailable('private-data')
        return original(ident, **kwargs)
    lane.db.update = update
    with pytest.raises(DatabaseUnavailable): asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == SENDING
    lane.db.update = original
    lane.manual.recover()
    assert lane.db.get(ident)['status'] == UNCERTAIN
    asyncio.run(lane.manual.process(ident))
    assert len(submissions(lane)) == 1


def test_media_processing_resumes_same_ticket_and_put_is_not_repeated(lane):
    ident = accepted(lane)
    lane.behaviour['processing'] = 0
    asyncio.run(lane.manual.process(ident))
    row = lane.db.get(ident)
    assert row['status'] == QUEUED and row['payload']['image_pending']['file_ticket'] == 'unit-ticket'
    row['payload']['image_pending']['next_check_at'] = utc()-1
    lane.db.update(ident, payload=row['payload'])
    lane.behaviour['processing'] = 1
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == PUBLISHED
    assert sum(r.method == 'PUT' for r in lane.calls) == 1 and len(submissions(lane)) == 1


def test_known_sensitive_word_rejection_is_manual_only_and_refunds_slot(lane):
    ident = accepted(lane)
    lane.behaviour['code'] = '20022'
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert lane.manual.status()['used_or_reserved'] == 0
    assert not lane.db.state('publisher_blocked', False)
    assert lane.s.daily_target == 50 and lane.s.gap_seconds == 1200


@pytest.mark.parametrize('code', ('429', '220009', '401'))
def test_account_level_responses_keep_shared_safety_controls(lane, code):
    ident = accepted(lane)
    if code == '429': lane.behaviour['http_status'] = 429
    else: lane.behaviour['code'] = code
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert (lane.db.state('publish_retry_after', 0) > utc()) if code in ('429','220009') else lane.db.state('publisher_blocked')
    assert lane.s.daily_target == 50 and lane.s.hourly_cap == 3


def test_reconcile_not_posted_requires_confirmation_before_explicit_retry(lane):
    ident = accepted(lane)
    lane.behaviour['timeout'] = True
    asyncio.run(lane.manual.process(ident))
    with pytest.raises(ValueError): lane.manual.control('/manual_reconcile', ident + ' not_posted', 99)
    lane.manual.control('/manual_reconcile', ident + ' not_posted CONFIRM', 99)
    assert lane.db.get(ident)['status'] == REVIEW and lane.manual.status()['used_or_reserved'] == 0
    lane.manual.control('/manual_retry', ident, 99)
    assert lane.db.get(ident)['status'] == QUEUED and lane.manual.status()['used_or_reserved'] == 1


def test_exact_duplicate_manual_picture_and_text_is_not_posted_twice(lane):
    first = accepted(lane)
    asyncio.run(lane.manual.process(first))
    second = accepted(lane, mid=2)
    asyncio.run(lane.manual.process(second))
    assert lane.db.get(second)['status'] == REVIEW and len(submissions(lane)) == 1


@pytest.mark.parametrize('path', ('../secret', 'photos/../secret', 'https://evil.example/file', '/photos/file.png'))
def test_telegram_download_path_cannot_redirect_or_escape_to_other_hosts(lane, path):
    lane.behaviour['path'] = path
    with pytest.raises(ValueError): asyncio.run(download_image(lane.client, 'unit-token', 'photo_1'))
    assert not any(r.method == 'GET' for r in lane.calls)


def test_image_validation_rejects_non_images_and_does_not_redraw(lane):
    with pytest.raises(ValueError): normalized_png(b'not an image')
    png = normalized_png(lane.raw)
    with Image.open(io.BytesIO(png)) as image:
        assert image.format == 'PNG' and image.size == (64,64)


def test_cleanup_preserves_queued_manual_media_and_compacts_confirmed_receipts(lane, tmp_path):
    image = 'a'*64 + '.png'
    lane.db.insert('media', {'png_base64':'private-bytes'}, 'stored', id=image)
    pending = lane.db.insert(KIND, {'image':image,'body':'My image'}, QUEUED)
    done = lane.db.insert(KIND, {'body':'My old post','source':'telegram_manual','sender':99,
        'manual_binding':'keep-proof','receipt':{'id':'confirmed'},'submitted_at':utc()-9*86400}, PUBLISHED)
    lane.db.conn.execute('UPDATE entities SET created=?,updated=?', (utc()-9*86400, utc()-9*86400))
    run_cleanup(lane.db, tmp_path)
    assert lane.db.get(pending) and lane.db.get(image)
    p = lane.db.get(done)['payload']
    assert p[LEDGER] and p['receipt']['id'] == 'confirmed' and p['manual_binding'] == 'keep-proof'


def test_telegram_receive_auth_and_update_dedup_are_preserved_for_images(tmp_path):
    async def run():
        s = Settings(database=':memory:', artifacts=str(tmp_path/'telegram'), telegram_token='unit-token',
                     telegram_admins=(99,), webhook_secret='unit-secret', neon_batch_seconds=0)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'ok':True,'result':{}}))) as client:
            desk = Desk(s, client=client)
            desk.manual.start = lambda: None
            update = {'update_id':101,'message':picture()}
            await desk.telegram.receive(update, flush=False)
            await desk.telegram.receive(update, flush=False)
            assert desk.db.count(KIND) == 1
            await desk.telegram.receive({'update_id':102,'message':picture(2,sender=123)}, flush=False)
            assert desk.db.count(KIND) == 1
            await desk.close()
    asyncio.run(run())


def test_regular_publisher_still_obeys_shared_platform_cap_when_manual_posts_exist(lane):
    lane.manual.has_items = True
    lane.s.platform_daily_cap = 2
    for _ in range(2): lane.db.insert(KIND, {'submitted_at':utc()}, PUBLISHED)
    policy = ManualAwarePolicy(lane.s, lane.db, lane.manual)
    assert policy.gate({'id':'new-auto','payload':{}}) == 'Shared Square daily platform limit reached'


def test_fresh_scheduler_and_regular_publisher_keep_original_slot_and_quality_rules(lane):
    from test_square_image_responsiveness import ready_text
    async def run():
        desk = Desk(lane.s, store=lane.db, client=lane.client)
        desk.manual.start = lambda: None
        desk.manual.client = lane.client
        desk.manual.owns_client = False
        desk.owner = lane.desk.owner
        desk.lease_owned = True
        desk.publisher.worker_owner = desk.owner
        # Finish one manual item, then let the actual regular scheduler/publisher
        # handle its own approved text with the unchanged checker.
        ident = accepted(lane)
        await lane.manual.process(ident)
        desk.manual.has_items = True
        automated = ready_text(desk)
        original = lane.db.get(automated)['payload'].copy()
        now = utc()
        desk.scheduler.allocate(now)
        assert lane.db.get(automated)['due'] == now
        assert [r['id'] for r in desk.scheduler.due(now)] == [automated]
        await desk.publisher.publish(automated)
        assert lane.db.get(automated)['status'] == 'published'
        assert lane.db.get(automated)['payload']['body'] == original['body']
        assert desk.publisher.policy.caps() == {'daily':50,'hourly':3,'gap':1200}
        assert lane.db.budget_used(lane.manual.day(), 'images') == 0
        await desk.close()
    asyncio.run(run())


def test_resumable_media_survives_new_manual_lane_without_second_upload(lane):
    ident = accepted(lane)
    lane.behaviour['processing'] = 0
    asyncio.run(lane.manual.process(ident))
    row = lane.db.get(ident)
    row['payload']['image_pending']['next_check_at'] = utc()-1
    lane.db.update(ident, payload=row['payload'])
    restarted = ManualPosts(lane.desk, client=lane.client)
    restarted.recover()
    lane.behaviour['processing'] = 1
    asyncio.run(restarted.process(ident))
    assert lane.db.get(ident)['status'] == PUBLISHED
    assert sum(r.method == 'PUT' for r in lane.calls) == 1
    assert len(submissions(lane)) == 1


def test_completed_upload_is_reused_after_pause_or_automatic_priority_hold(lane):
    ident = accepted(lane)
    lane.behaviour['pause_after_upload'] = True
    asyncio.run(lane.manual.process(ident))
    row = lane.db.get(ident)
    assert row['status'] == QUEUED
    assert row['payload']['image_upload']['url'] == 'https://image.example/manual.png'
    assert 'image_pending' not in row['payload']
    before = len(lane.calls)
    lane.db.set('paused', False)
    lane.behaviour['pause_after_upload'] = False
    restarted = ManualPosts(lane.desk, client=lane.client)
    restarted.recover()
    asyncio.run(restarted.process(ident))
    assert lane.db.get(ident)['status'] == PUBLISHED
    assert len(lane.calls) == before + 1  # Only /content/add; no download, PUT or status poll.
    assert len(submissions(lane)) == 1


def test_restart_recovery_distinguishes_pre_submission_from_possibly_sent(lane):
    first = accepted(lane)
    second = accepted(lane, 2, '/manual My second picture')
    lane.db.update(first, status=MEDIA)
    lane.db.update(second, status=SENDING)
    lane.manual.recover()
    assert lane.db.get(first)['status'] == QUEUED
    assert lane.db.get(second)['status'] == UNCERTAIN
    assert lane.manual.has_items and not lane.calls


def test_caption_edit_during_upload_blocks_content_and_refunds_reservation(lane):
    ident = accepted(lane)
    lane.behaviour['edit_after_upload'] = True
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert lane.manual.status()['used_or_reserved'] == 0
    assert not submissions(lane)


def test_late_timeout_cannot_overwrite_successors_state_after_worker_lease_changes(lane):
    ident = accepted(lane)
    lane.behaviour.update(timeout=True, lose_lease_after_submit=True)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == SENDING
    assert not lane.manual.recovered
    lane.desk.owner = 'new-worker'
    lane.manual.recover()
    assert lane.db.get(ident)['status'] == UNCERTAIN
    asyncio.run(lane.manual.process(ident))
    assert len(submissions(lane)) == 1


def test_malformed_content_receipt_is_uncertain_and_does_not_block_regular_publisher(lane):
    ident = accepted(lane)
    lane.behaviour['malformed_receipt'] = True
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == UNCERTAIN
    assert not lane.db.state('publisher_blocked', False)
    assert len(submissions(lane)) == 1


def test_manual_background_worker_wakes_and_closes_without_automatic_task_changes(lane):
    async def run():
        ManualPosts.start(lane.manual)
        await asyncio.sleep(0)
        task = lane.manual.task
        assert task and not task.done() and not lane.calls
        ident = accepted(lane)
        for _ in range(100):
            if lane.db.get(ident)['status'] == PUBLISHED: break
            await asyncio.sleep(.01)
        assert lane.db.get(ident)['status'] == PUBLISHED
        assert lane.desk.image_upload_task is None
        await lane.manual.close()
        assert task.done()
    asyncio.run(run())


def test_manual_media_and_budget_are_replayed_exactly_on_verified_database_switch(cluster):
    remote, journal, nodes, engine = cluster
    body = {'body':'My exact meme text','file_id':'telegram-image','sender':99,'manual_budget_day':'2026-10-18'}
    with engine.transaction():
        engine.execute('INSERT INTO entities VALUES (?,?,?,?,?,?,?,?)',
            ('manual-one', KIND, QUEUED, utc(), utc(), None, 'manual-message', json.dumps(body)))
        engine.execute('INSERT INTO budgets VALUES (?,?,?)', ('2026-10-18','manual_posts',1))
    nodes['aiven'].down = True
    row = engine.execute('SELECT * FROM entities WHERE id=?', ('manual-one',)).fetchone()
    assert engine.current != 'aiven'
    assert row['kind'] == KIND and row['status'] == QUEUED
    assert json.loads(row['payload']) == body
    assert engine.execute('SELECT used FROM budgets WHERE category=?', ('manual_posts',)).fetchone()[0] == 1
    assert engine.execute('SELECT status FROM entities WHERE id=?', ('published',)).fetchone()[0] == 'published'


def test_non_image_and_oversize_photo_fail_before_content_with_cap_refunded(lane):
    with pytest.raises(ValueError):
        normalized_png(b'x' * 8_000_001)
    message = picture()
    message['photo'][0]['file_size'] = 8_000_001
    with pytest.raises(ValueError): lane.manual.receive(message)
    assert lane.manual.status()['used_or_reserved'] == 0
    assert not lane.calls


def test_document_png_and_photo_caption_limits_support_long_text_separately(lane):
    photo = picture(caption='/manual')
    photo['document'] = {'file_id':'doc_1','file_size':100,'mime_type':'image/png'}
    del photo['photo']
    lane.manual.receive(photo)
    lane.manual.receive(text_message('x' * 2100))
    row = lane.db.list(KIND)[0]
    assert row['status'] == QUEUED and len(row['payload']['body']) == 2100
    asyncio.run(lane.manual.process(row['id']))
    assert lane.db.get(row['id'])['status'] == PUBLISHED


def test_manual_cleanup_does_not_revisit_compacted_receipt_ledgers(lane, tmp_path):
    ident = lane.db.insert(KIND, {'source':'telegram_manual','sender':99,'body':'old',
        'receipt':{'id':'confirmed'},'manual_binding':'hash'}, PUBLISHED)
    lane.db.conn.execute('UPDATE entities SET created=?,updated=? WHERE id=?', (utc()-9*86400,utc()-9*86400,ident))
    first = run_cleanup(lane.db, tmp_path)
    assert first['receipt_ledgers'] == 1
    assert run_cleanup(lane.db, tmp_path)['receipt_ledgers'] == 0


def test_real_worker_runtime_routes_manual_input_to_isolated_lane_and_shuts_down(lane):
    from square_desk.worker_runtime import WorkerRuntime
    async def run():
        lane.s.telegram_polling = False
        desk = Desk(lane.s, store=lane.db, client=lane.client)
        desk.owner = lane.desk.owner
        desk.manual.client, desk.manual.owns_client = lane.client, False
        async def idle(): await asyncio.Event().wait()
        desk.cycle = idle
        desk.observe_market = idle
        runtime = WorkerRuntime(desk)
        try:
            await runtime.start()
            await runtime.call(desk.telegram.receive, {'update_id':901,'message':picture()}, flush=False)
            async def state(): return desk.db.list(KIND)[0]['status']
            for _ in range(100):
                if await runtime.call(state) == PUBLISHED: break
                await asyncio.sleep(.01)
            assert await runtime.call(state) == PUBLISHED
            assert len(submissions(lane)) == 1
            assert desk.manual.task.get_loop() is runtime.loop
        finally:
            await runtime.close()
        assert not runtime.thread.is_alive() and desk.manual.task.done()
    asyncio.run(run())


from test_square_database_failover import cluster


def test_audit_rearming_cannot_attach_new_text_to_an_old_incomplete_picture(lane):
    lane.manual.receive(text_message('/manual'))
    lane.manual.receive(picture(caption=''))
    old = lane.db.list(KIND)[0]['id']
    lane.manual.receive(text_message('/manual', mid=21))
    lane.manual.receive(text_message('This belongs to my NEW composition', mid=22))
    assert lane.db.get(old)['status'] == CANCELLED
    assert lane.db.count(KIND, [QUEUED]) == 0 and not lane.calls


def test_audit_missing_caption_is_not_converted_to_the_word_none(lane):
    lane.manual.receive(text_message('/manual'))
    lane.manual.receive(picture(caption=None))
    assert lane.db.list(KIND)[0]['status'] == WAITING
    assert lane.manual.status()['used_or_reserved'] == 0


def test_audit_palette_transparency_survives_png_normalization():
    source = Image.new('P', (4,4), 0)
    source.putpalette([255,0,0] + [0,0,0]*255)
    raw = io.BytesIO(); source.save(raw, format='PNG', transparency=0)
    with Image.open(io.BytesIO(normalized_png(raw.getvalue()))) as converted:
        assert converted.mode == 'RGBA' and converted.getpixel((0,0))[3] == 0


def test_audit_shared_platform_ceiling_is_checked_before_startup_recovery(lane):
    lane.s.platform_daily_cap = 2
    for _ in range(2): lane.db.insert(KIND, {'submitted_at':utc()}, PUBLISHED)
    lane.manual.has_items = lane.manual.recovered = False
    policy = ManualAwarePolicy(lane.s, lane.db, lane.manual)
    assert policy.gate({'id':'startup-auto','payload':{}}) == 'Shared Square daily platform limit reached'


def test_audit_lost_worker_lease_cannot_write_another_workers_media_state(lane):
    from square_desk.publisher import WithheldPublication
    ident = accepted(lane)
    lane.db.update(ident, status=MEDIA)
    before = lane.db.get(ident)
    lane.db.set('worker_lease', {'owner':'new-worker','until':utc()+120})
    with pytest.raises(WithheldPublication): lane.manual.before_submit(ident, 'irrelevant')
    assert lane.db.get(ident) == before


def test_audit_failed_recovery_keeps_watching_until_lease_returns_without_new_input(lane, monkeypatch):
    import square_desk.manual_posts as module
    monkeypatch.setattr(module, 'POLL_SECONDS', .01, raising=False)
    async def run():
        ident = accepted(lane)
        lane.db.update(ident, status=MEDIA)
        lane.db.set('worker_lease', {'owner':'retiring-worker','until':utc()+120})
        ManualPosts.start(lane.manual)
        await asyncio.sleep(.03)
        lane.db.set('worker_lease', {'owner':lane.desk.owner,'until':utc()+120})
        try:
            for _ in range(80):
                if lane.db.get(ident)['status'] == PUBLISHED: break
                await asyncio.sleep(.01)
            assert lane.db.get(ident)['status'] == PUBLISHED
        finally:
            await lane.manual.close()
    asyncio.run(run())


def test_audit_saved_original_text_is_checked_before_any_external_request(lane):
    ident = accepted(lane)
    row = lane.db.get(ident)
    row['payload']['body'] = 'Someone changed the body after I authorized it'
    lane.db.update(ident, payload=row['payload'])
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert lane.manual.status()['used_or_reserved'] == 0 and not lane.calls


def test_audit_duplicate_receipt_older_than_latest_thousand_is_still_protected(lane):
    first = accepted(lane)
    asyncio.run(lane.manual.process(first))
    old = lane.db.get(first)
    old['payload']['submitted_at'] = utc()-20*86400
    lane.db.update(first, payload=old['payload'])
    for n in range(1001):
        lane.db.insert(KIND, {'manual_binding':'other-'+str(n),'submitted_at':utc()-20*86400}, PUBLISHED)
    second = accepted(lane, mid=2)
    asyncio.run(lane.manual.process(second))
    assert lane.db.get(second)['status'] == REVIEW
    assert len(submissions(lane)) == 1


def test_audit_existing_shared_image_file_is_never_partially_overwritten(lane, monkeypatch):
    from hashlib import sha256
    png = normalized_png(lane.raw)
    target = Path(lane.s.artifacts)/(sha256(png).hexdigest()+'.png')
    target.write_bytes(png)
    original = Path.write_bytes
    def fail_if_overwritten(path, data):
        if path == target:
            original(path, b'partial-write')
            raise OSError('simulated partial overwrite')
        return original(path, data)
    monkeypatch.setattr(Path, 'write_bytes', fail_if_overwritten)
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    assert target.read_bytes() == png
    assert lane.db.get(ident)['status'] == PUBLISHED


@pytest.mark.parametrize('receipt', ({'code':False}, {'code':'<html>'},
    {'code':'000000','data':{'id':True}}, {'code':'000000','data':{'id':{'bad':'type'}}},
    {'code':'000000','data':{'id':'   '}}))
def test_audit_malformed_receipts_neither_confirm_nor_block_automatic_posts(lane, receipt):
    ident = accepted(lane)
    lane.behaviour['receipt_override'] = receipt
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == UNCERTAIN
    assert not lane.db.state('publisher_blocked', False)
    assert len(submissions(lane)) == 1


def test_audit_absolute_content_deadline_leaves_uncertain_without_repeating_post(lane, monkeypatch):
    import square_desk.manual_posts as module
    monkeypatch.setattr(module, 'CONTENT_SECONDS', .02, raising=False)
    attempts = []
    async def respond(request):
        if request.url.path.endswith('/content/add'):
            attempts.append(request)
            await asyncio.sleep(.08)
        return await lane.client._transport.handle_async_request(request)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            lane.manual.client = client
            ident = accepted(lane)
            await lane.manual.process(ident)
            await lane.manual.process(ident)
            assert lane.db.get(ident)['status'] == UNCERTAIN and len(attempts) == 1
    asyncio.run(run())


def test_audit_slow_trickle_download_has_an_absolute_deadline(lane, monkeypatch):
    import square_desk.manual_media as module
    monkeypatch.setattr(module, 'DOWNLOAD_SECONDS', .02, raising=False)
    class Trickle(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(4):
                await asyncio.sleep(.03)
                yield b'data'
    async def respond(request):
        if request.method == 'GET': return httpx.Response(200, stream=Trickle())
        return await lane.client._transport.handle_async_request(request)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(ValueError, match='timed out'):
                await download_image(client, 'unit-token', 'photo_1')
    asyncio.run(run())


def test_audit_sending_state_remains_non_replayable_after_real_verified_failover(cluster):
    remote, journal, nodes, engine = cluster
    store = Store(':memory:')
    store.conn.close()
    store.conn, store.postgres = engine, True
    with store.transaction():
        store.insert(KIND, {'source':'telegram_manual','sender':99,'body':'Already attempted',
            'manual_budget_day':'2026-10-18','submitted_at':utc()}, SENDING, id='switch-manual')
        engine.execute('INSERT INTO budgets VALUES (?,?,?)', ('2026-10-18','manual_posts',1))
    nodes['aiven'].down = True
    desk = SimpleNamespace(s=Settings(), db=store, lease_owned=True, owner='worker')
    lane = ManualPosts(desk)
    lane.recover()
    assert engine.current != 'aiven' and store.get('switch-manual')['status'] == UNCERTAIN
    assert store.budget_used('2026-10-18','manual_posts') == 1
    asyncio.run(lane.process('switch-manual'))
    assert lane.client is None


def test_audit_photo_and_pending_reference_commit_atomically(lane, monkeypatch):
    lane.manual.receive(text_message('/manual'))
    original = lane.db.set
    def fail_pending(key, value):
        if key.startswith('manual_pending:'): raise DatabaseUnavailable('private database details')
        return original(key, value)
    monkeypatch.setattr(lane.db, 'set', fail_pending)
    with pytest.raises(DatabaseUnavailable): lane.manual.receive(picture(caption=''))
    assert lane.db.count(KIND) == 0
    assert lane.db.state('manual_armed:99') > utc()
    assert lane.manual.status()['used_or_reserved'] == 0


def test_audit_cancel_failure_preserves_pending_reference_and_reservation(lane, monkeypatch):
    ident = accepted(lane)
    before = lane.db.state('manual_pending:99')
    original = lane.db.update
    def fail_cancel(ident, **kwargs):
        if kwargs.get('status') == CANCELLED: raise DatabaseUnavailable('private database details')
        return original(ident, **kwargs)
    monkeypatch.setattr(lane.db, 'update', fail_cancel)
    with pytest.raises(DatabaseUnavailable): lane.manual.control('/manual_cancel', '', 99)
    assert lane.db.state('manual_pending:99') == before
    assert lane.db.get(ident)['status'] == QUEUED
    assert lane.manual.status()['used_or_reserved'] == 1


def test_audit_replied_waiting_image_can_be_recovered_after_expired_composition(lane):
    lane.manual.receive(text_message('/manual'))
    photo = picture(caption='')
    lane.manual.receive(photo)
    ident = lane.db.list(KIND)[0]['id']
    lane.db.set('manual_pending:99', {})
    lane.manual.receive({**text_message('/manual Exact recovered text'), 'reply_to_message':photo})
    assert lane.db.count(KIND) == 1
    assert lane.db.get(ident)['status'] == QUEUED
    asyncio.run(lane.manual.process(ident))
    assert len(submissions(lane)) == 1


@pytest.mark.parametrize('value', ('inf', 'NaN', '-10', 'invalid'))
def test_audit_invalid_retry_after_still_saves_a_finite_account_cooldown(lane, value):
    import math
    ident = accepted(lane)
    lane.behaviour.update(http_status=429, response_headers={'Retry-After':value})
    asyncio.run(lane.manual.process(ident))
    until = lane.db.state('publish_retry_after', 0)
    assert lane.db.get(ident)['status'] == REVIEW
    assert math.isfinite(until) and until > utc()
    assert lane.manual.status()['used_or_reserved'] == 0
    assert lane.db.state('policy_caps', {}) == {}


def test_audit_shutdown_after_content_submission_does_not_enable_replay(lane):
    started = asyncio.Event()
    async def respond(request):
        if request.url.path.endswith('/content/add'):
            started.set()
            await asyncio.Event().wait()
        return await lane.client._transport.handle_async_request(request)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            lane.manual.client = client
            ident = accepted(lane)
            ManualPosts.start(lane.manual)
            await asyncio.wait_for(started.wait(), 1)
            assert lane.db.get(ident)['status'] == SENDING
            await lane.manual.close()
            assert lane.db.get(ident)['status'] == SENDING
            lane.manual.recover()
            assert lane.db.get(ident)['status'] == UNCERTAIN
            await lane.manual.process(ident)
    asyncio.run(run())


def test_audit_failed_atomic_image_replace_leaves_no_partial_target_or_temporary_file(lane, monkeypatch):
    import square_desk.manual_media as module
    def fail_replace(source, target): raise OSError('simulated disk error')
    monkeypatch.setattr(module.os, 'replace', fail_replace)
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert not list(Path(lane.s.artifacts).iterdir())
    assert not submissions(lane) and lane.manual.status()['used_or_reserved'] == 0


def test_audit_durable_image_integrity_failure_is_reviewed_without_retry_loop(lane, monkeypatch):
    import square_desk.manual_posts as module
    from square_desk.media import MediaError
    def corrupted_media(*args): raise MediaError('media_changed')
    monkeypatch.setattr(module, 'persist_png', corrupted_media)
    ident = accepted(lane)
    asyncio.run(lane.manual.process(ident))
    count = len(lane.calls)
    asyncio.run(lane.manual.process(ident))
    assert lane.db.get(ident)['status'] == REVIEW
    assert len(lane.calls) == count and not submissions(lane)
    assert lane.manual.status()['used_or_reserved'] == 0
    assert not lane.db.state('publisher_blocked', False)
