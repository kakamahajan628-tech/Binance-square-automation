import asyncio
import base64
import hashlib
import io
import json
from pathlib import Path

import httpx
from PIL import Image
import pytest

from square_desk.media import MediaError, persist_png, restore_png
from square_desk.manual_media import save_png
from square_desk.store import Store
from square_desk.service import Desk
from square_desk.replica_batch import replay
from square_desk.turso_journal import decode
from test_square_database_failover import cluster
from test_square_manual_posts import lane, accepted, submissions, KIND, QUEUED, REVIEW
from test_square_media import live_settings, attached_chart


def picture_bytes():
    output = io.BytesIO()
    Image.new('RGB', (70, 70), 'navy').save(output, format='PNG')
    return output.getvalue()


def test_new_image_stores_tiny_metadata_without_encoded_bytes_and_is_idempotent(tmp_path):
    db = Store(':memory:')
    data = picture_bytes()
    filename = save_png(tmp_path, data)
    hashed = persist_png(db, tmp_path, filename)
    assert hashed == hashlib.sha256(data).hexdigest()
    assert db.get(filename)['payload'] == {'sha256':hashed, 'bytes':len(data), 'storage':'disk'}
    assert len(json.dumps(db.get(filename)['payload'])) < 180
    assert (tmp_path/filename).read_bytes() == data
    changes = db.conn.total_changes
    traces = []; db.conn.set_trace_callback(traces.append)
    assert persist_png(db, tmp_path, filename) == hashed
    assert db.conn.total_changes == changes
    assert all('SELECT *' not in sql for sql in traces)
    db.close()


def test_legacy_copy_is_removed_only_after_exact_local_file_validation(tmp_path):
    db = Store(':memory:')
    data = picture_bytes(); filename = save_png(tmp_path, data)
    hashed = Path(filename).stem
    db.insert('media', {'sha256':hashed,'bytes':len(data),'png_base64':base64.b64encode(data).decode()}, 'stored', id=filename)
    db.insert('draft', {'image':filename,'image_hash':hashed,'body':'keep approved text'}, 'approved', id='pending')
    assert persist_png(db, tmp_path, filename) == hashed
    assert db.get(filename)['payload'] == {'sha256':hashed,'bytes':len(data),'storage':'disk'}
    assert db.get('pending')['status'] == 'approved'
    assert db.get('pending')['payload']['body'] == 'keep approved text'
    assert restore_png(db, tmp_path, filename, hashed) == data
    db.close()


@pytest.mark.parametrize('failure', ['missing', 'changed'])
def test_failed_local_validation_never_strips_only_legacy_copy(tmp_path, failure):
    db = Store(':memory:')
    data = picture_bytes(); filename = save_png(tmp_path, data)
    legacy = {'sha256':Path(filename).stem, 'bytes':len(data), 'png_base64':base64.b64encode(data).decode()}
    db.insert('media', legacy, 'stored', id=filename)
    if failure == 'missing': (tmp_path/filename).unlink()
    else: (tmp_path/filename).write_bytes(b'changed image')
    with pytest.raises(MediaError): persist_png(db, tmp_path, filename)
    assert db.get(filename)['payload'] == legacy
    db.close()


def test_disk_reads_make_zero_database_calls_and_reject_a_wrong_filename_hash(tmp_path):
    class UnavailableDB:
        def get(self, *args): raise AssertionError('Image reader must not query the DB')
    data = picture_bytes(); filename = save_png(tmp_path, data)
    assert restore_png(UnavailableDB(), tmp_path, filename) == data
    wrong = 'a'*64+'.png'; (tmp_path/wrong).write_bytes(data)
    with pytest.raises(MediaError) as error: restore_png(UnavailableDB(), tmp_path, wrong)
    assert error.value.code == 'media_changed'


def test_missing_disk_file_does_not_decode_even_a_valid_legacy_database_copy(tmp_path):
    db = Store(':memory:')
    data = picture_bytes(); filename = save_png(tmp_path, data)
    db.insert('media', {'sha256':Path(filename).stem, 'png_base64':base64.b64encode(data).decode()}, 'stored', id=filename)
    (tmp_path/filename).unlink()
    db.get = lambda *args: pytest.fail('Legacy image bytes must not be read')
    with pytest.raises(MediaError) as error: restore_png(db, tmp_path, filename)
    assert error.value.code == 'media_missing'
    assert not (tmp_path/filename).exists()
    db.close()


@pytest.mark.parametrize('article', [False, True])
def test_lost_automatic_image_without_saved_evidence_enters_review_without_http_or_global_block(tmp_path, article):
    async def run():
        calls = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(500))) as client:
            desk = Desk(live_settings(tmp_path), client=client)
            ident, filename = attached_chart(desk, article)
            (tmp_path/filename).unlink()
            desk.db.conn.execute("DELETE FROM entities WHERE kind='snapshot'")
            await desk.publisher.publish(ident)
            row = desk.db.get(ident)
            assert row['status'] == 'review'
            assert row['payload']['rejection_code'] == 'media_recovery_unavailable'
            assert row['payload']['image'] == filename
            assert not calls
            assert not desk.db.state('publisher_blocked', False)
            assert desk.db.state('publish_last_rejection')['scope'] == 'draft'
            await desk.close()
    asyncio.run(run())


def test_lost_manual_image_refunds_reservation_without_reupload_or_text_fallback(lane):
    ident = accepted(lane)
    lane.behaviour['processing'] = 0
    asyncio.run(lane.manual.process(ident))
    row = lane.db.get(ident)
    assert row['status'] == QUEUED and not submissions(lane)
    filename = row['payload']['image']
    (Path(lane.s.artifacts)/filename).unlink()
    row['payload']['image_pending']['next_check_at'] = 0
    lane.db.update(ident, payload=row['payload'])
    lane.calls.clear()
    asyncio.run(lane.manual.process(ident))
    current = lane.db.get(ident)
    assert current['status'] == REVIEW
    assert current['payload']['rejection_code'] == 'media_missing'
    assert not lane.calls
    assert lane.db.budget_used(lane.manual.day(), 'manual_posts') == 0
    assert not lane.db.state('publisher_blocked', False)
    assert current['payload']['body'] == current['payload']['original_text']


def test_new_media_journal_and_all_replicas_contain_only_metadata(cluster, tmp_path):
    remote, journal, nodes, engine = cluster
    db = Store(':memory:'); db.conn.close(); db.conn = engine
    data = picture_bytes(); filename = save_png(tmp_path, data)
    persist_png(db, tmp_path, filename)
    entry = remote.db.execute('SELECT payload FROM square_journal ORDER BY seq DESC LIMIT 1').fetchone()[0]
    statements = decode(entry)
    serialized = json.dumps(statements)
    assert 'png_base64' not in serialized
    assert base64.b64encode(data).decode() not in serialized
    assert len(serialized) < 2000
    for name, backend in nodes.items():
        connection = backend.connection()
        try:
            replay(connection, journal, journal.head)
            row = connection.execute('SELECT payload FROM entities WHERE id=?', (filename,)).fetchone()
            assert json.loads(row[0]) == {'sha256':Path(filename).stem,'bytes':len(data),'storage':'disk'}
        finally: connection.close()


def test_status_reports_disk_storage_and_no_durable_database_image_bytes(tmp_path):
    desk = Desk(live_settings(tmp_path))
    images = desk.status()['images']
    assert images['durable_png_storage'] is False
    assert images['database_image_bytes'] is False
    assert images['image_storage'] == 'disk_and_hash'
    asyncio.run(desk.close())
