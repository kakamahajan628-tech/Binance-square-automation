"""Three-day maintenance with protected active work and seven-day quality history."""
import base64
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
from threading import Event
import time
import zlib
from .models import utc, digest

INTERVAL = 3*86400
QUALITY_WINDOW = 7*86400
ARCHIVE = '_square_entity_zlib_v1'
LEDGER = '_square_publication_ledger_v1'
PROTECTED_DRAFTS = ('review','approved','queued','chart_rendering','uploading_image',
                    'preparing_media','sending','uncertain','manual_ready')
CONFIRMED = ('published','paper_published','manual_published')
TERMINAL_SIGNALS = ('target1','stopped','time_exit','invalidated','expired_unresolved','cancelled')


def decode_payload(payload):
    if not isinstance(payload,dict) or ARCHIVE not in payload:
        return payload
    try:
        decoder=zlib.decompressobj()
        raw=decoder.decompress(base64.b64decode(payload[ARCHIVE],validate=True),32_000_001)
        if len(raw)>32_000_000 or not decoder.eof or decoder.unused_data:
            raise ValueError
        value=json.loads(raw)
        if not isinstance(value,dict):
            raise ValueError
        return value
    except (ValueError,TypeError,zlib.error):
        raise ValueError('Archived entity payload invalid; no publication accepted') from None


def compressed(payload):
    raw=json.dumps(payload,allow_nan=False).encode()
    return {ARCHIVE:base64.b64encode(zlib.compress(raw)).decode('ascii')}


def ledger(payload,now):
    keys=('title','category','article','priority','risk','generated_by','approved_at',
          'submitted_at','receipt','campaign_id','image_hash','image')
    result={key:payload[key] for key in keys if key in payload}
    if payload.get('source') == 'telegram_manual':
        result.update({key:payload[key] for key in ('source','sender','manual_binding','manual_budget_day','reserved_at') if key in payload})
    if isinstance(payload.get('event'),dict):
        result['event']={key:payload['event'][key] for key in ('signal_id','event_key','symbol','as_of','category')
                         if key in payload['event']}
    result.update(body='',history_compacted_at=now,archived=True)
    result[LEDGER]=True
    return result


def candidates(store,kind,statuses,cutoff):
    cursor=''
    while True:
        with store.lock:
            omit=''
            if (kind=='draft' and tuple(statuses)==CONFIRMED) or (kind=='manual_post' and tuple(statuses)==('manual_published',)):
                omit=(" AND COALESCE(CAST(payload AS jsonb)->>'"+LEDGER+"','false')!='true'" if store.postgres else
                      " AND COALESCE(json_extract(payload,'$."+LEDGER+"'),0)!=1")
            rows=list(store.conn.execute('SELECT * FROM entities WHERE kind=? AND updated<? AND id>?'
                  + (' AND status IN ('+','.join('?' for _ in statuses)+')' if statuses else '')
                  +omit+' ORDER BY id LIMIT 16',(kind,cutoff,cursor,*statuses)))
        if not rows:
            return
        cursor=rows[-1]['id']
        yield rows


def owned(store,owner):
    if owner is None:
        return True
    lease=store.state('worker_lease',{})
    return lease.get('owner')==owner and lease.get('until',0)>utc()


def invalidate(store):
    if hasattr(store,'_history'):
        store._history.clear()
        store._history_bytes=0


def inventory(store):
    protected=set()
    query='SELECT id,kind,payload FROM entities WHERE (kind=\'draft\' AND status IN ('+','.join('?' for _ in PROTECTED_DRAFTS)+")) OR (kind='signal' AND status IN ('watching','triggered')) OR (kind='campaign' AND status='active')"
    with store.lock:
        for row in store.conn.execute(query,PROTECTED_DRAFTS):
            protected.add(row['id'])
            p=decode_payload(json.loads(row['payload']))
            if p.get('image'):
                protected.add(p['image'])
            event=p.get('event',{}) if row['kind']=='draft' else p
            source=event.get('metrics',{}).get('source') or event.get('source')
            if source and event.get('symbol') and event.get('as_of'):
                protected.add(digest([source,event['symbol'],900,event['as_of']]))
            if event.get('snapshot_id'):
                protected.add(event['snapshot_id'])
    # User media is not a candle/chart and has no automatic-draft event.
    # Preserve queued/in-flight/uncertain manual images through cleanup.
    from .manual_posts import KIND, PROTECTED, REVIEW, WAITING
    for row in store.list(KIND, list(PROTECTED), limit=10000):
        if row['status'] in (REVIEW, WAITING) and row['updated'] < utc()-INTERVAL:
            continue
        protected.add(row['id'])
        image = row['payload'].get('image')
        if image:
            protected.add(image)
    return protected


def run_cleanup(store,directory,*,owner=None,now=None,stop=None):
    now=utc() if now is None else now
    stop=stop or Event()
    protected=inventory(store)
    result={'interval_days':3,'raw_post_compression_after_days':3,'quality_history_days':7,
            'protected_references':len(protected),'archived_posts':0,'receipt_ledgers':0,
            'archived_signals':0,'deleted_rows':0,'compacted_commands':0,'deleted_artifacts':0,
            'complete':False}
    def batches(kind,statuses,cutoff,action):
        for batch in candidates(store,kind,statuses,cutoff):
            if stop.is_set():
                return False
            with store.transaction():
                if not owned(store,owner):
                    return False
                for original in batch:
                    # Reread under the lock: queue/receipt state may have changed.
                    row=store.conn.execute('SELECT * FROM entities WHERE id=?',(original['id'],)).fetchone()
                    if not row or row['id'] in protected or row['updated']>=cutoff or (statuses and row['status'] not in statuses):
                        continue
                    action(row)
                invalidate(store)
            time.sleep(.01)  # Background task yields between bounded batches.
        return True
    def set_payload(row,payload):
        # Preserve original created/updated/due timestamps and fingerprint.
        store.conn.execute('UPDATE entities SET payload=? WHERE id=?',(json.dumps(payload,allow_nan=False),row['id']))
    def remove(row):
        store.conn.execute('DELETE FROM entities WHERE id=?',(row['id'],))
        result['deleted_rows']+=1
    def archive_post(row):
        raw=json.loads(row['payload'])
        p=decode_payload(raw)
        last=max(row['created'],row['updated'],p.get('submitted_at',0) or 0)
        if last<now-QUALITY_WINDOW:
            if not raw.get(LEDGER):
                set_payload(row,ledger(p,now))
                result['receipt_ledgers']+=1
        elif ARCHIVE not in raw and not raw.get(LEDGER):
            set_payload(row,compressed(p))
            result['archived_posts']+=1
    def archive_signal(row):
        if row['created']<now-QUALITY_WINDOW and row['updated']<now-QUALITY_WINDOW:
            remove(row)
        else:
            raw=json.loads(row['payload'])
            if ARCHIVE not in raw:
                set_payload(row,compressed(raw))
                result['archived_signals']+=1
    def compact_command(row):
        p=json.loads(row['payload'])
        if p!={'compacted':True}:
            set_payload(row,{'compacted':True})
            result['compacted_commands']+=1
    def metric(row):
        p=decode_payload(json.loads(row['payload']))
        if p.get('as_of',row['created'])<now-QUALITY_WINDOW:
            remove(row)
    operations=(('draft',CONFIRMED,now-INTERVAL,archive_post),
                ('manual_post',('manual_published',),now-INTERVAL,archive_post),
                ('manual_post',('manual_cancelled','manual_review','manual_waiting_text'),now-INTERVAL,remove),
                ('draft',('expired','rejected'),now-INTERVAL,remove),
                ('signal',TERMINAL_SIGNALS,now-INTERVAL,archive_signal),
                ('snapshot',(),now-INTERVAL,remove),('media',(),now-INTERVAL,remove),
                ('outbox',('delivered',),now-INTERVAL,remove),
                ('command',('applied','rejected','cancelled'),now-INTERVAL,compact_command),
                ('metric',(),now-QUALITY_WINDOW,metric),
                ('report',(),now-INTERVAL,remove),('news',('expired','rejected'),now-INTERVAL,remove))
    for operation in operations:
        if not batches(*operation):
            return result
    with store.transaction():
        if stop.is_set() or not owned(store,owner):
            return result
        store.conn.execute('DELETE FROM audit WHERE at<?',(now-INTERVAL,))
        day=__import__('datetime').datetime.fromtimestamp(now-INTERVAL,__import__('datetime').timezone.utc).date().isoformat()
        store.conn.execute('DELETE FROM budgets WHERE day<?',(day,))
    root=Path(directory).resolve()
    if root.is_dir():
        for path in root.iterdir():
            if stop.is_set():
                return result
            if (path.is_symlink() or path.resolve().parent!=root or path.name in protected
                    or not path.is_file() or path.stat().st_mtime>=now-INTERVAL):
                continue
            if re.fullmatch(r'(?:[a-f0-9]{32,64}|weekly-\d{4}-W\d{2})\.(?:txt|png|json)',path.name):
                path.unlink()
                result['deleted_artifacts']+=1
    result.update(complete=True,finished_at=utc())
    return result


class CleanupWorker:
    def __init__(self,store,directory,owner):
        self.store,self.directory,self.owner=store,directory,owner
        self.stop=Event()
        self.executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix='square-cleanup')
        self.future=None
        self.next_due=0
        self.last_error=''
    def _run(self):
        report=run_cleanup(self.store,self.directory,owner=self.owner,stop=self.stop)
        if report['complete']:
            with self.store.transaction():
                if not owned(self.store,self.owner):
                    return
                self.store.set('master_cleanup',report)
                self.store.set('master_cleanup_last_run',utc())
        return report
    def maybe_start(self):
        if self.stop.is_set():
            return
        if self.future:
            if not self.future.done():
                return
            try:
                report=self.future.result()
                if report and report.get('complete'):
                    self.next_due=self.store.state('master_cleanup_last_run',0)+INTERVAL
                    self.last_error=''
                else:
                    self.next_due=utc()+300
                    self.last_error='cleanup_interrupted_or_lease_lost'
            except Exception as error:
                self.last_error=type(error).__name__
                self.next_due=utc()+300
            self.future=None
        if not self.next_due:
            self.next_due=self.store.state('master_cleanup_last_run',0)+INTERVAL
        if utc()>=self.next_due:
            self.future=self.executor.submit(self._run)
    def status(self):
        return {'interval_days':3,'quality_history_days':7,
                'running':bool(self.future and not self.future.done()),
                'next_due':self.next_due or None,'last_error':self.last_error}
    def close(self):
        self.stop.set()
        if self.future:
            self.future.cancel()
        self.executor.shutdown(wait=False,cancel_futures=True)

    async def aclose(self):
        import asyncio
        self.close()
        # A cancelled Future may already be running native database calls.
        # Join it off the event loop before the repository can be closed.
        await asyncio.to_thread(self.executor.shutdown,wait=True,cancel_futures=True)
