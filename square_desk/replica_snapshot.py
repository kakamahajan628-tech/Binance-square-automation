"""Fenced current-source checkpoints and atomic, archived secondary rebuilds."""
from pathlib import Path
import base64
import gzip
import hashlib
import json
import tempfile
import uuid

from .postgres import DatabaseUnavailable
from .database_failover import metadata,replica_error
from .database_transfer import COLUMNS,validate_record
from .efficient_storage import unpacked
from .migration import pending

MAX_COMPRESSED=32_000_000
MAX_STORAGE=64_000_000
MAX_DATA=128_000_000
MAX_LINE=4_000_000
CHUNK=96_000
FORMAT='square-replica-checkpoint-v1'
SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS square_restore_checkpoints (
      id TEXT PRIMARY KEY,cluster TEXT NOT NULL,baseline TEXT NOT NULL,seq INTEGER NOT NULL,
      head_hash TEXT NOT NULL,provider TEXT NOT NULL,manifest TEXT NOT NULL,
      bytes INTEGER NOT NULL,chunks INTEGER NOT NULL,ready INTEGER NOT NULL DEFAULT 0)''',
    '''CREATE TABLE IF NOT EXISTS square_restore_chunks (
      checkpoint TEXT NOT NULL,part INTEGER NOT NULL,data TEXT NOT NULL,
      PRIMARY KEY(checkpoint,part))''')


def held(code):
    raise DatabaseUnavailable('Replica checkpoint rebuild held; no unverified promotion',code=code)


def safe_code(error):
    code=getattr(error,'code','')
    allowed={'snapshot_source_unavailable','snapshot_source_changed','snapshot_source_unverified',
             'snapshot_anchor_unverified','snapshot_data_bound','snapshot_file_invalid','snapshot_storage_bound',
             'snapshot_fence_changed','snapshot_chunk_unverified','snapshot_archive_unverified',
             'snapshot_restore_unverified','snapshot_target_changed','snapshot_cancelled'}
    return code if code in allowed else replica_error(error)


def wire(value):
    return (json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False,separators=(',',':'))+'\n').encode()


def file_hash(path):
    result=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(128_000),b''):result.update(chunk)
    return result.hexdigest()


def anchor(journal,row):
    head=journal.observe()
    if row['cluster']!=head['cluster'] or row['baseline']!=head['baseline'] or row['seq']>head['seq']:
        held('snapshot_anchor_unverified')
    if row['seq']==head['seq'] and row['head_hash']==head['head_hash']:return
    if row['seq']==0 and row['head_hash']==head['baseline']:return
    entry=journal.entries(row['seq']-1,row['seq'])
    if len(entry)!=1 or entry[0]['seq']!=row['seq'] or entry[0]['hash']!=row['head_hash']:
        held('snapshot_anchor_unverified')


def inspect(path):
    if path.stat().st_size>MAX_COMPRESSED:held('snapshot_data_bound')
    counts=dict.fromkeys(COLUMNS,0)
    hashed=hashlib.sha256()
    size=0
    try:
        with gzip.open(path,'rb') as stream:
            header=json.loads(stream.readline(MAX_LINE+1))
            if (not isinstance(header,dict) or set(header)!={'type','format','checkpoint','source'}
                    or header.get('type')!='header' or header.get('format')!=FORMAT
                    or header['source'] not in {'aiven','supabase','neon'}
                    or not isinstance(header['checkpoint'],dict)):
                held('snapshot_file_invalid')
            prior=-1
            while True:
                raw=stream.readline(MAX_LINE+1)
                if not raw or len(raw)>MAX_LINE:held('snapshot_file_invalid')
                value=json.loads(raw)
                if not isinstance(value,dict):held('snapshot_file_invalid')
                if value.get('type')=='end':
                    if value.get('counts')!=counts or value.get('data_hash')!=hashed.hexdigest() or stream.read(1):
                        held('snapshot_file_invalid')
                    return {'header':header,'counts':counts,'data_hash':hashed.hexdigest(),
                            'snapshot_hash':file_hash(path),'bytes':path.stat().st_size}
                validate_record(value)
                index=list(COLUMNS).index(value['table'])
                if index<prior:held('snapshot_file_invalid')
                prior=index
                size+=len(raw)
                if size>MAX_DATA or sum(counts.values())>=100_000:held('snapshot_data_bound')
                counts[value['table']]+=1
                hashed.update(raw)
    except (OSError,EOFError,ValueError,TypeError,KeyError):
        held('snapshot_file_invalid')


class SnapshotCache:
    def __init__(self):
        self.directory=tempfile.TemporaryDirectory(prefix='square-rebuild-')
        self.root=Path(self.directory.name).resolve()
        assert self.root.is_relative_to(Path(tempfile.gettempdir()).resolve())
        self.path=self.root/'checkpoint.gz'
        self.info=None
    def close(self):self.directory.cleanup()


def cancelled(stop):
    if stop is not None and stop.is_set():held('snapshot_cancelled')


def fence(journal):
    rows=journal.query('''UPDATE square_cluster SET id=id WHERE id=1 AND cluster=?
       AND owner=? AND epoch=? AND lease_until>unixepoch()+5 RETURNING seq''',
       (journal.cluster,journal.owner,journal.epoch))
    if len(rows)!=1:held('snapshot_fence_changed')


def capture(source,journal,head,provider,path,source_current,stop):
    counts=dict.fromkeys(COLUMNS,0)
    hashed=hashlib.sha256()
    size=0
    with source.snapshot():
        row=metadata(source,head,allow_ahead=True)
        anchor(journal,row)
        schema=source.execute('SELECT value FROM state WHERE key=?',('schema_version',)).fetchone()
        marker=source.execute('SELECT value FROM state WHERE key=?',('database_migration',)).fetchone()
        migration=unpacked(json.loads(marker[0])) if marker else {}
        if (not schema or json.loads(schema[0])!=1 or not isinstance(migration,dict)
                or migration.get('required') is not True or pending(migration)):
            held('snapshot_source_unverified')
        header={'type':'header','format':FORMAT,'checkpoint':dict(row),'source':provider}
        with path.open('wb') as output,gzip.GzipFile(fileobj=output,filename='',mode='wb',mtime=0) as zipped:
            zipped.write(wire(header))
            for table,columns in COLUMNS.items():
                for values in source.stream_rows(table):
                    cancelled(stop)
                    if not source_current():held('snapshot_source_changed')
                    record={'type':'row','table':table,'values':[values[key] for key in columns]}
                    validate_record(record)
                    raw=wire(record)
                    size+=len(raw)
                    if len(raw)>MAX_LINE or size>MAX_DATA or sum(counts.values())>=100_000:
                        held('snapshot_data_bound')
                    counts[table]+=1
                    hashed.update(raw);zipped.write(raw)
            zipped.write(wire({'type':'end','counts':counts,'data_hash':hashed.hexdigest()}))
    return inspect(path)


def download(journal,manifest,path,stop):
    total=0
    with path.open('wb') as stream:
        for offset in range(0,manifest['chunks'],8):
            cancelled(stop)
            rows=journal.query('SELECT part,data FROM square_restore_chunks WHERE checkpoint=? AND part>=? AND part<? ORDER BY part',
                               (manifest['id'],offset,min(offset+8,manifest['chunks'])))
            if len(rows)!=min(8,manifest['chunks']-offset):held('snapshot_chunk_unverified')
            for index,row in enumerate(rows,offset):
                if row['part']!=index:held('snapshot_chunk_unverified')
                try:data=base64.b64decode(row['data'],validate=True)
                except (ValueError,TypeError):held('snapshot_chunk_unverified')
                total+=len(data)
                if total>MAX_COMPRESSED:held('snapshot_data_bound')
                stream.write(data)
    result=inspect(path)
    for key in ('header','counts','data_hash','snapshot_hash','bytes'):
        if result[key]!=manifest[key]:held('snapshot_chunk_unverified')


def checkpoint(cache,source,journal,head,provider,minimum,source_current,stop):
    journal.pipeline([(sql,()) for sql in SCHEMA])
    if cache.info and cache.info['header']['source']==provider and cache.info['header']['checkpoint']['seq']>=minimum:
        anchor(journal,cache.info['header']['checkpoint'])
        return cache.info
    cache.info=None
    rows=journal.query('''SELECT manifest FROM square_restore_checkpoints WHERE ready=1 AND cluster=? AND baseline=?
      AND provider=? AND seq>=? AND seq<=(SELECT seq FROM square_cluster WHERE id=1)
      AND seq>=(SELECT COALESCE(MIN(seq)-1,0) FROM square_journal) ORDER BY seq DESC LIMIT 1''',
      (head['cluster'],head['baseline'],provider,minimum))
    if rows:
        manifest=json.loads(rows[0]['manifest'])
        anchor(journal,manifest['header']['checkpoint'])
        download(journal,manifest,cache.path,stop)
        cache.info=manifest
        return manifest
    if source is None:held('snapshot_source_unavailable')
    # Incomplete uploads are never eligible for restore. A current fenced owner
    # may reclaim this staging data; published verified checkpoints are retained.
    clause='''EXISTS (SELECT 1 FROM square_cluster WHERE id=1 AND cluster=? AND owner=?
        AND epoch=? AND lease_until>unixepoch()+5)'''
    journal.query('''DELETE FROM square_restore_chunks WHERE checkpoint IN
        (SELECT id FROM square_restore_checkpoints WHERE ready=0 AND cluster=? AND baseline=?) AND '''+clause,
        (head['cluster'],head['baseline'],journal.cluster,journal.owner,journal.epoch))
    journal.query('DELETE FROM square_restore_checkpoints WHERE ready=0 AND cluster=? AND baseline=? AND '+clause,
        (head['cluster'],head['baseline'],journal.cluster,journal.owner,journal.epoch))
    total=journal.query('SELECT COALESCE(SUM(bytes),0) AS bytes FROM square_restore_checkpoints')[0]['bytes']
    if total>=MAX_STORAGE:held('snapshot_storage_bound')
    manifest=capture(source,journal,head,provider,cache.path,source_current,stop)
    point=manifest['header']['checkpoint']
    if point['seq']<minimum:held('snapshot_source_unverified')
    manifest['id']=hashlib.sha256((point['cluster']+'\n'+manifest['snapshot_hash']).encode()).hexdigest()
    manifest['chunks']=(manifest['bytes']+CHUNK-1)//CHUNK
    if total+manifest['bytes']>MAX_STORAGE:held('snapshot_storage_bound')
    encoded=json.dumps(manifest,sort_keys=True,allow_nan=False)
    existing=journal.query('SELECT manifest FROM square_restore_checkpoints WHERE id=?',(manifest['id'],))
    if existing:
        if existing[0]['manifest']!=encoded:held('snapshot_chunk_unverified')
    else:
        inserted=journal.query('''INSERT INTO square_restore_checkpoints
          SELECT ?,?,?,?,?,?,?,?, ?,0 FROM square_cluster WHERE id=1 AND cluster=? AND baseline=?
          AND owner=? AND epoch=? AND lease_until>unixepoch()+5 AND seq>=?
          AND COALESCE((SELECT SUM(bytes) FROM square_restore_checkpoints),0)+?<=? RETURNING id''',
          (manifest['id'],point['cluster'],point['baseline'],point['seq'],point['head_hash'],provider,encoded,
           manifest['bytes'],manifest['chunks'],head['cluster'],head['baseline'],journal.owner,journal.epoch,
           point['seq'],manifest['bytes'],MAX_STORAGE))
        if len(inserted)!=1:held('snapshot_fence_changed')
    with cache.path.open('rb') as stream:
        for offset in range(0,manifest['chunks'],8):
            cancelled(stop)
            if not source_current():held('snapshot_source_changed')
            statements=[]
            for part in range(offset,min(offset+8,manifest['chunks'])):
                data=base64.b64encode(stream.read(CHUNK)).decode('ascii')
                statements.append(('''INSERT INTO square_restore_chunks SELECT ?,?,? WHERE EXISTS
                  (SELECT 1 FROM square_cluster WHERE id=1 AND cluster=? AND owner=? AND epoch=? AND lease_until>unixepoch()+5)
                  ON CONFLICT(checkpoint,part) DO UPDATE SET data=excluded.data
                  WHERE square_restore_chunks.data=excluded.data RETURNING part''',
                  (manifest['id'],part,data,head['cluster'],journal.owner,journal.epoch)))
            results,_=journal.pipeline(statements)
            if any(len(result)!=1 for result in results):held('snapshot_fence_changed')
    downloaded=cache.root/'verified.gz'
    download(journal,manifest,downloaded,stop)
    if not source_current():held('snapshot_source_changed')
    published=journal.query('''UPDATE square_restore_checkpoints SET ready=1 WHERE id=? AND EXISTS
       (SELECT 1 FROM square_cluster WHERE id=1 AND cluster=? AND owner=? AND epoch=? AND lease_until>unixepoch()+5)
       AND chunks=(SELECT COUNT(*) FROM square_restore_chunks WHERE checkpoint=?) RETURNING id''',
       (manifest['id'],head['cluster'],journal.owner,journal.epoch,manifest['id']))
    if len(published)!=1:held('snapshot_fence_changed')
    downloaded.replace(cache.path)
    cache.info=manifest
    return manifest


def rebuild(target,source,journal,head,failure,*,provider,cache,stop=None,source_current,secondary):
    cancelled(stop)
    if not secondary() or not source_current():held('snapshot_source_changed')
    fence(journal)
    before=metadata(target,head,allow_ahead=True)
    anchor(journal,before)
    manifest=checkpoint(cache,source,journal,head,provider,failure.failure_detail['journal_sequence'],source_current,stop)
    point=manifest['header']['checkpoint']
    if point['seq']<before['seq']:held('snapshot_target_changed')
    # Revalidate the shared checkpoint/file before changing a target.
    anchor(journal,point)
    verified=inspect(cache.path)
    if verified['snapshot_hash']!=manifest['snapshot_hash']:held('snapshot_chunk_unverified')
    rebuild_id=uuid.uuid4().hex
    with target.transaction():
        now=target.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
        if dict(now or {})!=dict(before) or not secondary():held('snapshot_target_changed')
        if not source_current():held('snapshot_source_changed')
        fence(journal)
        cancelled(stop)
        archived=target.archive_replica(rebuild_id)
        for table in COLUMNS:target.execute('DELETE FROM '+table)
        work=[]
        def flush():
            if not work:return
            if hasattr(target,'execute_replay_batch'):
                counts=target.execute_replay_batch(work)
            else:counts=[target.execute(sql,args).rowcount for sql,args in work]
            if len(counts)!=len(work) or any(count!=1 for count in counts):held('snapshot_restore_unverified')
            work.clear()
        with gzip.open(cache.path,'rb') as stream:
            stream.readline(MAX_LINE+1)
            size=0
            for raw in stream:
                cancelled(stop)
                record=json.loads(raw)
                if record['type']=='end':break
                table=record['table']
                work.append(('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in COLUMNS[table])+')',tuple(record['values'])))
                size+=len(raw)
                if len(work)>=32 or size>=1_000_000:flush();size=0
            flush()
        digest=hashlib.sha256()
        counts=dict.fromkeys(COLUMNS,0)
        for table,columns in COLUMNS.items():
            for row in target.stream_rows(table):
                cancelled(stop)
                digest.update(wire({'type':'row','table':table,'values':[row[key] for key in columns]}))
                counts[table]+=1
        if digest.hexdigest()!=manifest['data_hash'] or counts!=manifest['counts']:held('snapshot_restore_unverified')
        if not secondary():held('snapshot_target_changed')
        if not source_current():held('snapshot_source_changed')
        fence(journal)
        target.execute('''CREATE TABLE IF NOT EXISTS square_replica_rebuild_proof (
            id TEXT PRIMARY KEY,checkpoint TEXT NOT NULL,data_hash TEXT NOT NULL,seq BIGINT NOT NULL)''')
        target.execute('ALTER TABLE square_replica_rebuild_proof ENABLE ROW LEVEL SECURITY')
        target.execute('INSERT INTO square_replica_rebuild_proof VALUES (?,?,?,?)',
                       (rebuild_id,manifest['id'],manifest['data_hash'],point['seq']))
        moved=target.execute('UPDATE square_replica_meta SET seq=?,head_hash=? WHERE id=1 AND seq=? AND head_hash=?',
                             (point['seq'],point['head_hash'],before['seq'],before['head_hash']))
        if moved.rowcount!=1:held('snapshot_target_changed')
    return {'status':'restored_verified_checkpoint','source':provider,'checkpoint_sequence':point['seq'],
            'previous_sequence':before['seq'],'checkpoint_id':manifest['id'],
            'archived_rows':sum(archived.values()),'restored_rows':sum(counts.values())}
