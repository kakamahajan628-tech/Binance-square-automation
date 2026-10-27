import base64
from hashlib import sha256
import io
import json
from pathlib import Path

from PIL import Image
import pytest

from square_desk.database_transfer import write_backup, TransferError
from square_desk.media_cleanup import cleanup, CONFIRM
from square_desk.models import utc
from square_desk.store import Store
from test_square_database_transfer import Bridge


@pytest.fixture
def legacy(tmp_path):
    db=Store(':memory:'); stream=io.BytesIO(); Image.new('RGB',(64,64),'gold').save(stream,format='PNG')
    data=stream.getvalue(); name=sha256(data).hexdigest()+'.png'
    payload={'sha256':Path(name).stem,'bytes':len(data),'png_base64':base64.b64encode(data).decode()}
    db.insert('media',payload,'stored',id=name)
    backup=tmp_path/'verified.jsonl.gz'; write_backup(Bridge(db),backup)
    yield db,name,payload,backup
    db.close()


def test_cleanup_plan_is_read_only_and_apply_removes_only_backed_up_image_bytes(legacy):
    db,name,payload,backup=legacy
    before=db.conn.total_changes
    report=cleanup(db,backup)
    assert report['eligible']==1 and report['changed']==0 and db.conn.total_changes==before
    db.insert('draft',{'body':'preserve text','receipt':{'id':'confirmed'}},'published',id='published')
    report=cleanup(db,backup,apply=True,confirm=CONFIRM)
    assert report['changed']==1
    assert db.get(name)['payload']=={'sha256':payload['sha256'],'bytes':payload['bytes'],'storage':'disk'}
    assert db.get('published')['payload']['receipt']['id']=='confirmed'
    assert db.state('worker_lease')['until']==0
    assert cleanup(db,backup,apply=True,confirm=CONFIRM)['changed']==0


@pytest.mark.parametrize('failure', ['confirmation','corrupt_backup','row_changed','active_worker'])
def test_cleanup_refuses_unsafe_or_unverified_purge(legacy, failure):
    db,name,payload,backup=legacy
    if failure=='corrupt_backup': backup.write_bytes(b'broken gzip')
    elif failure=='row_changed': db.update(name,payload={**payload,'extra':'not backed up'})
    elif failure=='active_worker': db.acquire_lease('real-worker',120)
    original=db.get(name)['payload']; changes=db.conn.total_changes
    with pytest.raises((ValueError,TransferError)):
        cleanup(db,backup,apply=True,confirm='' if failure=='confirmation' else CONFIRM)
    assert db.get(name)['payload']==original
    assert db.conn.total_changes==changes


@pytest.mark.parametrize('kind,status',[('draft','approved'),('draft','uncertain'),('manual_post','manual_queued'),('manual_post','manual_uncertain')])
def test_cleanup_preserves_active_and_uncertain_image_references(legacy, kind, status):
    db,name,payload,backup=legacy
    db.insert(kind,{'image':name},status,id='protected')
    report=cleanup(db,backup,apply=True,confirm=CONFIRM)
    assert report['changed']==0 and report['protected_or_invalid_skipped']==1
    assert db.get(name)['payload']==payload
