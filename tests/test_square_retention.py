import json
import os
from threading import Event
import time
import pytest

from square_desk.store import Store
from square_desk.retention import run_cleanup, CleanupWorker, ARCHIVE, LEDGER, INTERVAL
from square_desk.models import utc, digest
from square_desk.content import FactChecker
from square_desk.config import Settings


def old_row(db,kind,payload,status,age=4,id=None):
    ident=db.insert(kind,payload,status,id=id)
    at=utc()-age*86400
    db.conn.execute('UPDATE entities SET created=?,updated=? WHERE id=?',(at,at,ident))
    return ident


def test_three_day_cleanup_compresses_exact_quality_history_and_retains_receipts(tmp_path):
    db=Store(':memory:')
    body='A clear lesson about liquidity and execution. '*35
    payload={'title':'An original title','body':body,'category':'education','priority':20,
             'risk':'low','article':False,'receipt':{'id':'real-remote-id'},'submitted_at':utc()-4*86400}
    ident=old_row(db,'draft',payload,'published')
    old_stamp=db.get(ident)['updated']
    report=run_cleanup(db,tmp_path)
    raw=json.loads(db.conn.execute('SELECT payload FROM entities WHERE id=?',(ident,)).fetchone()[0])
    assert ARCHIVE in raw and len(json.dumps(raw))<len(json.dumps(payload))
    assert db.get(ident)['payload']==payload
    assert db.get(ident)['updated']==old_stamp
    assert report['archived_posts']==1
    checker=FactChecker(Settings(),db)
    assert checker.duplicate(payload)=='Draft too similar to recent content'
    assert db.get(ident)['status']=='published'
    db.close()


def test_after_quality_window_raw_data_becomes_small_nonreplayable_ledger(tmp_path):
    db=Store(':memory:')
    payload={'title':'Keep receipt','body':'Large body '*2000,'category':'setup','risk':'high',
             'article':True,'submitted_at':utc()-9*86400,'campaign_id':'active-campaign',
             'event':{'signal_id':'tracked-id','event_key':'covered-once','metrics':{'large':'X'*10000}},
             'receipt':{'id':'remote-accepted','url':'https://example.org/post'}}
    ident=old_row(db,'draft',payload,'published',age=9)
    report=run_cleanup(db,tmp_path)
    row=db.get(ident)
    assert row['payload'][LEDGER] and row['payload']['body']==''
    assert row['payload']['receipt']==payload['receipt']
    assert row['payload']['campaign_id']=='active-campaign'
    assert row['payload']['event']['signal_id']=='tracked-id'
    assert row['payload']['event']['event_key']=='covered-once'
    assert row['status']=='published' and report['receipt_ledgers']==1
    assert len(json.dumps(row['payload']))<1000
    db.close()


def test_active_uncertain_approved_work_and_exact_assets_survive(tmp_path):
    db=Store(':memory:')
    as_of=utc()-12*86400
    snapshot_id=digest(['binance','BTC',900,as_of])
    image='a'*32+'.png'
    old_row(db,'snapshot',{'kept':'exact candles'},'observed',age=12,id=snapshot_id)
    old_row(db,'media',{'sha256':'approved-hash','png_base64':'exact bytes'},'stored',age=12,id=image)
    for status in ('review','approved','queued','sending','uncertain','manual_ready'):
        old_row(db,'draft',{'title':'Protected '+status,'body':'Retain full approved text','image':image,
                           'event':{'symbol':'BTC','as_of':as_of,'metrics':{'source':'binance'}}},status,age=12)
    old_row(db,'signal',{'symbol':'BTC','source':'binance','as_of':as_of},'watching',age=12)
    path=tmp_path/image
    path.write_bytes(b'exact approved bytes')
    os.utime(path,(utc()-12*86400,utc()-12*86400))
    report=run_cleanup(db,tmp_path)
    assert report['complete']
    assert db.get(snapshot_id) and db.get(image)
    assert len(db.list('draft'))==6
    assert path.read_bytes()==b'exact approved bytes'
    db.close()


def test_cleanup_removes_only_expired_data_and_preserves_telegram_dedup_budget_controls(tmp_path):
    db=Store(':memory:')
    expired=old_row(db,'draft',{'body':'old expired'},'expired',age=5)
    signal=old_row(db,'signal',{'confidence':80},'stopped',age=10)
    unneeded=old_row(db,'media',{'png_base64':'unused'},'stored',age=5)
    command=old_row(db,'command',{'sender':123,'command':'/post_now'},'applied',age=5)
    db.conn.execute('UPDATE entities SET fingerprint=? WHERE id=?',('telegram:123',command))
    db.set('telegram_offset',999)
    db.set('emergency_stop',True)
    db.set('publisher_blocked',True)
    day=__import__('datetime').datetime.now(__import__('datetime').timezone.utc).date().isoformat()
    db.reserve_budget(day,'posts',5,50)
    generated=tmp_path/('b'*32+'.txt')
    generated.write_text('old local export')
    unrelated=tmp_path/'notes.txt'
    unrelated.write_text('keep user file')
    os.utime(generated,(utc()-5*86400,utc()-5*86400))
    os.utime(unrelated,(utc()-5*86400,utc()-5*86400))
    run_cleanup(db,tmp_path)
    assert not db.get(expired) and not db.get(signal) and not db.get(unneeded)
    assert db.get(command)['payload']=={'compacted':True}
    assert db.get(command)['fingerprint']=='telegram:123'
    assert db.state('telegram_offset')==999 and db.state('emergency_stop') is True
    assert db.state('publisher_blocked') is True and db.budget_used(day,'posts')==5
    assert not generated.exists() and unrelated.exists()
    db.close()


def test_weekly_signal_statistics_are_unchanged_by_archival(tmp_path):
    from square_desk.tracking import statistics
    db=Store(':memory:')
    payload={'type':'breakout','context':{'name':'risk_on'},'result_r':1.8,
             'risk_reward':2,'mfe_r':2.1,'mae_r':.2,'triggered_at':utc()-5*86400,
             'closed_at':utc()-4*86400,'target2_reached':False}
    old_row(db,'signal',payload,'target1',age=4)
    before=statistics(db.list('signal',since=utc()-7*86400))
    run_cleanup(db,tmp_path)
    assert statistics(db.list('signal',since=utc()-7*86400))==before
    db.close()


def test_lost_worker_lease_or_cancellation_prevents_cleanup_mutations(tmp_path):
    db=Store(':memory:')
    old=old_row(db,'draft',{'body':'never delete while another worker owns it'},'expired',age=5)
    db.set('worker_lease',{'owner':'new-worker','until':utc()+120})
    report=run_cleanup(db,tmp_path,owner='old-worker')
    assert report['complete'] is False and db.get(old)
    stop=Event();stop.set()
    assert run_cleanup(db,tmp_path,stop=stop)['complete'] is False
    assert db.get(old)
    db.close()


def test_cleanup_worker_runs_once_then_waits_three_days(tmp_path):
    db=Store(':memory:')
    db.set('worker_lease',{'owner':'worker','until':utc()+120})
    worker=CleanupWorker(db,tmp_path,'worker')
    try:
        worker.maybe_start()
        worker.future.result(timeout=5)
        worker.maybe_start()
        assert worker.future is None
        assert worker.next_due>utc()+INTERVAL-5
        worker.maybe_start()
        assert worker.future is None
        assert db.state('master_cleanup')['interval_days']==3
    finally:
        worker.close()
        db.close()


def test_media_metadata_check_does_not_select_base64_blob(tmp_path):
    from square_desk.media import persist_png
    from PIL import Image
    db=Store(':memory:')
    import io
    from hashlib import sha256
    stream=io.BytesIO()
    Image.new('RGB',(1200,800),'blue').save(stream,format='PNG')
    data=stream.getvalue()
    filename=sha256(data).hexdigest()+'.png'
    (tmp_path/filename).write_bytes(data)
    first=persist_png(db,tmp_path,filename)
    trace=[]
    db.conn.set_trace_callback(trace.append)
    assert persist_png(db,tmp_path,filename)==first
    assert any('json_extract' in sql for sql in trace)
    assert not any(sql.startswith('SELECT * FROM entities') for sql in trace)
    db.close()
