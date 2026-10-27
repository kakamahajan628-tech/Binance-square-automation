"""Small Telegram status from two bounded repository reads, no provider probes."""
from contextlib import nullcontext
import json

from .efficient_storage import unpacked
from .migration import pending
from .models import utc


def status(desk):
    db,s=desk.db,desk.s
    now=utc()
    keys=('schema_version','paused','emergency_stop','publisher_blocked','publisher_block_reason',
          'database_migration','last_scan','last_publication','publish_retry_after','last_ai_generation')
    with db.lock:
        context=getattr(db.conn,'read_transaction',nullcontext)
        with context():
            state={row['key']:unpacked(json.loads(row['value'])) for row in db.conn.execute(
                'SELECT key,value FROM state WHERE key IN ('+','.join('?' for _ in keys)+')',keys)}
            counts={row['status']:row['n'] for row in db.conn.execute(
                "SELECT status,COUNT(*) AS n FROM entities WHERE kind='draft' AND status IN "
                "('queued','approved','review','chart_rendering','uploading_image','uncertain') GROUP BY status")}
    failover=getattr(db.conn,'diagnostics',lambda:{'enabled':False})()
    providers={}
    for name,row in failover.get('providers',{}).items():
        providers[name]={key:row[key] for key in ('status','applied_sequence','pending_entries','last_error') if key in row}
        for key in ('snapshot_rebuild','entity_repair'):
            repair=row.get(key,{})
            if repair:
                providers[name][key]={field:repair[field] for field in ('status','reason','source','checkpoint_sequence') if field in repair}
    brief={key:failover[key] for key in ('enabled','implementation_revision','active_provider','writer_owned',
                                       'journal_available','journal_sequence','last_error') if key in failover}
    if providers:brief['providers']=providers
    last=state.get('last_scan')
    generation=state.get('last_ai_generation',{})
    return {'paper_mode':s.paper_mode,'publication_mode':s.mode,'live_adapter_enabled':s.live_enabled,
            'paused':state.get('paused',False),'emergency_stop':state.get('emergency_stop',False),
            'publication_blocked':bool(state.get('publisher_blocked',False) or pending(state.get('database_migration',{}))),
            'publication_block_code':state.get('publisher_block_reason',{}).get('code',''),
            'scheduler':('running' if desk.lease_owned else 'batch_idle' if desk.idle_until>now else 'waiting_for_worker_lease')
                        if desk.task and not desk.task.done() else 'stopped',
            'database':'ok' if state.get('schema_version')==1 else 'unknown','database_failover':brief,
            'last_scan':last,'data_fresh':bool(last and now-last<=s.scan_seconds*2),
            'scan_running':bool(desk.scan_task and not desk.scan_task.done()),'scan_last_error':desk.scan_last_error,
            'queue_size':sum(value for key,value in counts.items() if key!='uncertain'),
            'uncertain_submissions':counts.get('uncertain',0),'last_publication':state.get('last_publication'),
            'retry_after':state.get('publish_retry_after',0),
            'images':{'daily_cap':s.image_daily_cap,'automatic_charts':s.auto_images,
                      'upload_active':bool(desk.image_upload_task and not desk.image_upload_task.done())},
            'market_universe':{'selected_count':len(desk.universe),'scan_batch_size':s.scan_batch_size},
            'ai_last_generation':{key:generation[key] for key in ('status','at','provider','model') if key in generation},
            'details':'/status full, /publish_status, /ai_status'}
