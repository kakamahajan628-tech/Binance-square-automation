"""Nonblocking observations only: never used to authorize a publication."""
import asyncio
import logging
import time

REVISION='2026-11-09-verified-startup-recovery-28'


class RuntimeStatus:
    def __init__(self,desk):
        self.desk=desk
        self.phase='initializing'
        self.phase_at=time.time()
        self.tick_at=0
        self.warning_at=0
        self.completed_at=None
        self.last_failure={}
        self._scan_seen=None
        self._scan_failures=0
        self._scan_warning_after=0

    def progress_notice(self):
        """Alert on observed data failure, not legitimate lack of market setups."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        desk=self.desk;now=time.time()
        if not desk.lease_owned or datetime.fromtimestamp(now,ZoneInfo(desk.s.timezone)).hour not in desk.s.publishing_hours:return
        scan=dict(getattr(desk,'scan_observations',{}))
        finished=scan.get('finished_at')
        if finished and finished!=self._scan_seen:
            self._scan_seen=finished
            failed=scan.get('observed',0)==0 and scan.get('unavailable',0)>0
            self._scan_failures=self._scan_failures+1 if failed else 0
        if self._scan_failures>=2 and now>=self._scan_warning_after:
            desk.telegram.notify('Market-data warning: consecutive scans returned no valid observations. New numerical drafts may be unavailable; check /providers and /status. No freshness gate was bypassed.',key='market-progress:'+str(int(now//3600)))
            self._scan_warning_after=now+3600

    def failure(self, component, error):
        from .database_failover import replica_error
        from .postgres import DatabaseUnavailable
        allowed={'worker_cycle','background_scan','telegram_control','telegram_delivery','heartbeat','worker_supervisor'}
        self.last_failure={'component':component if component in allowed else 'worker_cycle',
            'at':time.time(),'code':replica_error(error) if isinstance(error,DatabaseUnavailable) else 'internal_error'}
        connection=self.desk.db.conn
        provider=getattr(connection,'current',None)
        if provider in ('neon','aiven','supabase'):self.last_failure['active_provider']=provider
        code=getattr(connection,'last_error','')
        if code in {'journal_unavailable_or_writer_busy','no_verified_current_replica',
                    'operation_outcome_requires_journal_reconciliation','waiting_for_verified_replica'}:
            self.last_failure['database_state']=code
        return dict(self.last_failure)

    def stage(self,name):
        self.phase,self.phase_at=name,time.time()

    def completed(self):
        # Completion telemetry is observational, never a publication gate.
        self.completed_at=time.time()

    async def pulse(self):
        while not self.desk.stopping:
            self.tick_at=time.time()
            await asyncio.sleep(1)

    def snapshot(self):
        desk=self.desk
        now=time.time()
        connection=desk.db.conn
        journal=getattr(connection,'journal',None)
        head=dict(getattr(journal,'head',None) or {})
        providers={}
        for name,node in getattr(connection,'nodes',{}).items():
            providers[name]={'status':node.status,'applied_sequence':node.seq,
                'pending_entries':None if node.seq is None else max(0,head.get('seq',0)-node.seq),
                'last_error':node.last_error}
            for key in ('snapshot_rebuild','entity_repair'):
                value=dict(getattr(node,key,{}))
                if value:providers[name][key]={k:value[k] for k in ('status','reason','source','checkpoint_sequence') if k in value}
        age=None if not self.tick_at else round(max(0,now-self.tick_at),1)
        runtime=getattr(desk.telegram,'control_runtime',None)
        ai=getattr(getattr(desk,'content',None),'ai',None)
        observations=getattr(ai,'observations',None)
        market_observations=getattr(getattr(desk,'pool',None),'observations',None)
        return {'implementation_revision':REVISION,'status_source':'memory_observations',
            'observed_at':now,'publication_authority':'Database checks still run before every post; this report cannot authorize posting',
            'worker':{'phase':self.phase,'phase_age_seconds':round(max(0,now-self.phase_at),1),
                'loop_tick_age_seconds':age,'loop_responsive':None if age is None else age<15,
                'lease_observed_held':desk.lease_owned,'scan_running':bool(desk.scan_task and not desk.scan_task.done()),
                'last_completed_cycle_at':self.completed_at,
                'last_completed_cycle_age_seconds':None if self.completed_at is None else round(max(0,now-self.completed_at),1)},
            'database_failover':{'enabled':bool(getattr(connection,'replicated',False)),
                'active_provider':getattr(connection,'current',None),'journal_sequence':head.get('seq'),
                'writer_lease_locally_valid':bool(journal and journal.epoch is not None and time.monotonic()<journal.deadline),
                'last_error':getattr(connection,'last_error',''),'providers':providers},
            'publication':{'last_hold':dict(getattr(desk.publisher,'last_hold',{})),
                'last_confirmed':dict(getattr(desk.publisher,'last_confirmed',{})),
                'last_plan':dict(getattr(desk.scheduler,'last_plan',{})),
                'global_controls':'Unknown in this memory-only view; /status full reads database controls'},
            'scan':dict(getattr(desk,'scan_observations',{})),
            'market_data':market_observations(compact=True) if callable(market_observations) else {},
            'last_runtime_failure':dict(self.last_failure),
            'ai':observations(compact=True) if callable(observations) else {'configured':ai is not None},
            'manual_posts':{'daily_cap':10,'separate_from_automatic':True,
                'last':dict(getattr(getattr(desk,'manual',None),'last',{}))},
            'configuration':{'paper_mode':desk.s.paper_mode,'mode':desk.s.mode,
                'ai_auto_publish':desk.s.ai_auto_publish,'daily_target':desk.s.daily_target,
                'hourly_cap':desk.s.hourly_cap,'gap_seconds':desk.s.gap_seconds,
                'image_daily_cap':desk.s.image_daily_cap},
            'telegram':{'poller':'independent' if runtime else 'worker',
                'last_api_status':getattr(runtime,'api_status','not_started'),
                'control_authority':getattr(runtime,'authority_status','not_checked')},
            'details':'/status full reads current database state; it can wait if the database is slow'}

    def watchdog(self):
        now=time.time()
        if self.tick_at and now-self.tick_at>=45 and now-self.warning_at>=60:
            self.warning_at=now
            logging.getLogger('square_desk').warning('WORKER_STALLED phase=%s seconds=%d',self.phase,now-self.tick_at)
