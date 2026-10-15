import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def release_root():
    root=Path(__file__).resolve().parents[1]
    staged=root/'dist/square-desk-ready-20261013'
    return staged if (staged/'MANIFEST.json').exists() else root


def test_release_manifest_and_free_entrypoint_complete():
    root=release_root()
    manifest=json.loads((root/'MANIFEST.json').read_text(encoding='utf-8'))
    for name,sha in manifest.items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha,name
        assert Path(name).name!='k' and '__pycache__' not in name
        assert not (Path(name).name.startswith('.env') and name!='.env.square.example')
    for name in ('square_desk/config.py','square_desk/retention.py','square_desk/fresh_cluster.py',
                 'square_desk/database_failover.py','square_desk/planning.py','requirements-square.txt',
                 'square_desk/telegram_control.py','square_desk/runtime_status.py',
                 'START_HERE.md','AUDIT_2026_10_15.md'):
        assert name in manifest
    from square_desk.runtime_status import REVISION
    assert REVISION=='2026-10-15-control-switching-10'
    yaml=(root/'render.yaml').read_text(encoding='utf-8')
    assert 'plan: free' in yaml and 'disk:' not in yaml
    assert 'python -m square_desk' in yaml and 'requirements-square.txt' in yaml
    assert not (root/'render-square.yaml').exists()


def test_actual_release_login_summary_details_and_cleanup_status(tmp_path):
    root=release_root()
    env={k:v for k,v in os.environ.items() if not k.startswith('DESK_')
         and k not in ('DATABASE_URL','DATABASE_URL_POOLED','PYTHONPATH')}
    env['PYTHONPATH']=str(root)
    script='''
import base64
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials
from fastapi.testclient import TestClient
from square_desk.config import Settings
from square_desk.store import Store
from square_desk.service import Desk
from square_desk.app import create_app
s=Settings(database=':memory:',artifacts='artifacts',admin_token='local-private')
db=Store(':memory:')
desk=Desk(s,store=db)
ident=db.insert('draft',{'title':'Detailed original content','body':'Clear learning words. '*50,'category':'education','risk':'low'},'review')
app=create_app(s,desk=desk,start_worker=False)
route=next(r for r in app.routes if r.path=='/health/details')
private=route.dependant.dependencies[0].call
try:
    private(HTTPBasicCredentials(username='देस्क',password='local-private'))
    raise AssertionError('Unicode username accepted')
except HTTPException as e:
    assert e.status_code==401
with TestClient(app) as client:
    assert client.get('/health').status_code==200
    assert client.head('/health').status_code==200
    wire='Basic '+base64.b64encode('देस्क:local-private'.encode()).decode()
    assert client.get('/health/details',headers={'Authorization':wire}).status_code==401
    auth=('desk','local-private')
    data=client.get('/api/overview',auth=auth).json()
    assert data['status']['cleanup']['interval_days']==3
    assert data['drafts'][0]['payload']['summary_only'] is True
    assert len(data['drafts'][0]['payload']['body'])<=300
    full=client.get('/api/drafts/'+ident,auth=auth).json()
    assert len(full['payload']['body'])>300
print('Complete release imports and HTTP/cleanup smoke OK')
'''
    result=subprocess.run([sys.executable,'-c',script],cwd=tmp_path,env=env,
                          capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stderr
