import asyncio
from copy import deepcopy
import hashlib
from pathlib import Path
import time

import httpx
import PIL
from PIL import Image
import pytest

from square_desk import chart_restore
from square_desk.analysis import studies
from square_desk.images import chart
from square_desk.media import MediaError
from square_desk.models import digest, utc
from square_desk.service import Desk
from test_square_desk import snapshot
from test_square_media import live_settings, attached_chart

GOLDEN = {
 'trend':'b04a14283d2beede409e5308953c1aba922a5d1b77c1da0b9b1958b75e06b966',
 'value':'57548dab9416ce7abd947ee9065dcd2e3cec5359d195ef8d97c4bc516d3f0329',
 'volatility':'18f10ade8376b0b8e67ed4cb1a7bac8afb7272e329e39f9adce13c5247abdaaf',
 'momentum':'07aa317cf6f15c48ea6b432f3e01c78c84ff76ad204b2af453df125ba7fb627c',
 'structure':'05d0a9ab114147bf1e55363a62bf2926a447e92606f31abf4b187d6ff0d7cb8c'}


@pytest.mark.parametrize('view', GOLDEN)
def test_pinned_pillow_chart_pixels_match_golden_fixture(tmp_path, view):
    assert PIL.__version__ == '13.0.0'
    snap = snapshot(end=1791936000); snap.fetched_at = snap.as_of+1
    context = {'chart_view':view,'alignment_version':'post-aligned-v1','show_macd':True,'show_vwap':True,'related':[]}
    filename = chart(snap,studies(snap),tmp_path,context=context)
    with Image.open(tmp_path/filename) as image:
        assert image.size == (1600,1100)
        assert hashlib.sha256(image.convert('RGB').tobytes()).hexdigest() == GOLDEN[view]


def test_missing_chart_recovers_exact_bytes_without_new_image_budget_or_ai(tmp_path):
    async def run():
        desk = Desk(live_settings(tmp_path))
        ident, filename = attached_chart(desk)
        p = desk.db.get(ident)['payload']
        before = deepcopy(p)
        original = (tmp_path/filename).read_bytes()
        used = desk.db.conn.execute("SELECT used FROM budgets WHERE category='images'").fetchone()[0]
        (tmp_path/filename).unlink()
        async def no_fetch(*args, **kwargs): pytest.fail('Recovery must not fetch market data')
        desk.pool.fetch = no_fetch
        await desk.recover_missing_chart(p)
        assert (tmp_path/filename).read_bytes() == original
        assert desk.db.get(ident)['payload'] == before
        assert desk.db.conn.execute("SELECT used FROM budgets WHERE category='images'").fetchone()[0] == used
        assert desk.db.get(filename)['payload']['storage'] == 'disk'
        await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['expired','future','snapshot_missing','snapshot_changed','wrong_hash','wrong_source','bad_recipe','manual'])
def test_recovery_refuses_unbound_or_stale_inputs_without_writing_a_file(tmp_path, failure):
    async def run():
        desk = Desk(live_settings(tmp_path))
        ident, filename = attached_chart(desk)
        p = desk.db.get(ident)['payload']; (tmp_path/filename).unlink()
        if failure == 'expired': p['event']['as_of'] = utc()-desk.s.draft_max_age-1
        elif failure == 'future': p['event']['as_of'] = utc()+901
        elif failure == 'snapshot_missing': desk.db.conn.execute("DELETE FROM entities WHERE kind='snapshot'")
        elif failure == 'snapshot_changed':
            ident = p['image_recipe']['snapshot_id']; row = desk.db.get(ident)
            row['payload']['fetched_at'] += 1; desk.db.update(ident,payload=row['payload'])
        elif failure == 'wrong_hash': p['image_hash'] = '0'*64
        elif failure == 'wrong_source': p['event']['metrics']['source'] = 'coinbase'
        elif failure == 'bad_recipe': p['image_recipe']['version'] = 'unrecognized'
        else: p['source'] = 'telegram_manual'
        with pytest.raises(MediaError): await desk.recover_missing_chart(p)
        assert not (tmp_path/filename).exists()
        await desk.close()
    asyncio.run(run())


def test_changed_renderer_output_never_replaces_the_approved_hash(tmp_path, monkeypatch):
    async def run():
        desk = Desk(live_settings(tmp_path)); ident, filename = attached_chart(desk)
        p = desk.db.get(ident)['payload']; (tmp_path/filename).unlink()
        original = chart_restore.images.chart
        def different(snap, metrics, folder, **kwargs):
            changed = deepcopy(metrics); changed['support'] *= .9
            return original(snap, changed, folder, **kwargs)
        monkeypatch.setattr(chart_restore.images,'chart',different)
        with pytest.raises(MediaError) as error: await desk.recover_missing_chart(p)
        assert error.value.code == 'media_changed'
        assert not (tmp_path/filename).exists()
        await desk.close()
    asyncio.run(run())


def test_saved_recipe_keeps_the_original_presentation_during_replay(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path)); ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; original=(tmp_path/filename).read_bytes(); (tmp_path/filename).unlink()
        p['title']='MACD Bollinger RSI new wording in later presentation'
        p['body']='Unrelated later words mentioning VWAP and volatility'
        await desk.recover_missing_chart(p)
        assert (tmp_path/filename).read_bytes()==original
        await desk.close()
    asyncio.run(run())


def test_recovery_uses_pinned_database_evidence_instead_of_later_ram_fetch(tmp_path, monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    async def run():
        desk=Desk(live_settings(tmp_path))
        snap=snapshot(); desk.db.archive_snapshot(snap.dict())
        # Pin the original version before a later fetch changes only fetched_at.
        from square_desk.store import Repository
        with desk.db.transaction(): Repository.archive_snapshot(desk.db,snap.dict())
        later=snap.dict(); later['fetched_at']+=10
        desk.db.archive_snapshot(later); desk.db.set('cache:BTC:900',later)
        ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; original=(tmp_path/filename).read_bytes(); (tmp_path/filename).unlink()
        assert p['image_recipe']['snapshot_digest']==digest(snap.dict())
        await desk.recover_missing_chart(p)
        assert (tmp_path/filename).read_bytes()==original
        await desk.close()
    asyncio.run(run())


def test_recovery_timeout_does_not_attach_late_output(tmp_path, monkeypatch):
    async def run():
        desk=Desk(live_settings(tmp_path)); ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; (tmp_path/filename).unlink()
        def slow(plan): time.sleep(.12); return b'late output'
        monkeypatch.setattr(chart_restore,'render',slow)
        desk.s.image_render_timeout_seconds=.01
        with pytest.raises(MediaError) as error: await desk.recover_missing_chart(p)
        assert error.value.code=='media_timeout'
        await asyncio.sleep(.15)
        assert not (tmp_path/filename).exists()
        await desk.close()
    asyncio.run(run())


def test_webhook_mode_never_starts_either_polling_task(tmp_path):
    async def run():
        settings=live_settings(tmp_path); settings.telegram_token='unit-token'; settings.telegram_admins=(1,)
        settings.webhook_secret='unit-secret'; settings.telegram_polling=False
        desk=Desk(settings); desk.independent_control=True
        async def idle(): await asyncio.Event().wait()
        desk.supervise=idle
        await desk.start()
        assert desk.telegram.control_runtime is None and desk.telegram_task is None
        await desk.close()
    asyncio.run(run())


def test_legacy_chart_without_recipe_still_requires_the_exact_approved_hash(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path)); ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; p.pop('image_recipe')
        original=(tmp_path/filename).read_bytes(); (tmp_path/filename).unlink()
        await desk.recover_missing_chart(p)
        assert (tmp_path/filename).read_bytes()==original
        await desk.close()
    asyncio.run(run())


def test_evidence_expiring_during_recovery_is_not_attached(tmp_path, monkeypatch):
    async def run():
        desk=Desk(live_settings(tmp_path)); ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; data=(tmp_path/filename).read_bytes(); (tmp_path/filename).unlink()
        def expires(plan):
            monkeypatch.setattr('square_desk.service.utc',lambda:p['event']['as_of']+desk.s.draft_max_age+1)
            return data
        monkeypatch.setattr(chart_restore,'render',expires)
        with pytest.raises(MediaError) as error: await desk.recover_missing_chart(p)
        assert error.value.code=='media_recovery_unavailable'
        assert not (tmp_path/filename).exists()
        await desk.close()
    asyncio.run(run())


def test_cancelled_recovery_never_attaches_late_thread_output(tmp_path, monkeypatch):
    async def run():
        desk=Desk(live_settings(tmp_path)); ident,filename=attached_chart(desk)
        p=desk.db.get(ident)['payload']; data=(tmp_path/filename).read_bytes(); (tmp_path/filename).unlink()
        def slow(plan): time.sleep(.12); return data
        monkeypatch.setattr(chart_restore,'render',slow)
        task=asyncio.create_task(desk.recover_missing_chart(p)); await asyncio.sleep(.01); task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        await asyncio.sleep(.15)
        assert not (tmp_path/filename).exists()
        await desk.close()
    asyncio.run(run())
