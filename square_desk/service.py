from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import httpx
from .models import Snapshot, uid, utc, digest
from .store import Store
from .providers import Transport, BinanceProvider, CoinbaseProvider, ProviderPool, ProviderError
from .analysis import studies, changes, regime, events, setup
from .content import ContentEngine
from .ai_router import AIRouter, AI_JOB
from .campaigns import CampaignManager
from .analytics import Analytics
from .scheduler import Scheduler
from .publisher import PublicationService
from .telegram import Telegram, rejection_message
from .compliance import PUBLICATION_STATES
from .tracking import record_setup, advance
from . import images
from .media import MediaError, persist_png, prepared_image_url
from .image_diagnostics import ImageFailure, failure_code, outcome
from .chart_worker import RenderWork
from .derivatives import BinanceDerivatives
from .news import NewsMonitor
from .neon_runtime import MarketCache


class Desk:
    def __init__(self, settings, store=None, client=None):
        settings.validate()
        self.s = settings
        # Library request logs can include the Telegram token in URL paths.
        # Operational diagnostics are emitted by our redacted audit layer.
        for name in ('httpx', 'httpcore'):
            logging.getLogger(name).disabled = True
        self.db = store or Store(settings.database)
        self.owned_client=client is None
        self.client = client or httpx.AsyncClient(timeout=20, follow_redirects=False,
                                                 limits=httpx.Limits(max_connections=10, max_keepalive_connections=5))
        from .bandwidth import request_hook
        if hasattr(self.client,'event_hooks') and request_hook not in self.client.event_hooks.get('request',[]):
            self.client.event_hooks.setdefault('request',[]).append(request_hook)
        self.transport = Transport(self.client)
        types = {'binance': BinanceProvider, 'coinbase': CoinbaseProvider}
        self.batch_seconds = settings.neon_batch_seconds if self.db.postgres else 0
        self.market_cache = MarketCache() if self.batch_seconds else None
        self.pool = ProviderPool([types[p](self.transport) for p in settings.providers],
                                 self.market_cache or self.db, settings.candle_max_age)
        self.derivatives = BinanceDerivatives(self.transport, self.db) if settings.enable_derivatives else None
        self.news = NewsMonitor(settings, self.db, self.client)
        router = AIRouter(settings, self.db, self.client)
        ai = router if router.configured else None
        self.content = ContentEngine(settings, self.db, ai)
        self.campaigns = CampaignManager(settings, self.db, self.content)
        self.analytics = Analytics(settings, self.db)
        self.scheduler = Scheduler(settings, self.db, self.content.checker)
        self.publisher = PublicationService(settings, self.db, self.content.checker, self.client)
        self.telegram = Telegram(settings, self.db, self, self.client)
        self.scan_lock = asyncio.Lock()
        self.signal_draft_lock = asyncio.Lock()
        self.image_render_lock = asyncio.Lock()
        self.image_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='square-chart')
        # Keep the concurrent Future: cancelling an asyncio wrapper does not
        # stop a running Pillow thread. Never enqueue work behind that thread.
        self.image_render_future = None
        self.image_observations = {}
        self.image_upload_task = None
        self.universe = list(settings.symbols)
        self.universe_checked_at = 0
        self.scan_cursor = 0
        self.universe_state = 'watchlist'
        self.owner = uid()
        from .retention import CleanupWorker
        self.cleanup = CleanupWorker(self.db,self.s.artifacts,self.owner)
        self.task = None
        self.heartbeat_task = None
        self.market_task = None
        self.telegram_task = None
        self.scan_task = None
        self.background_scans = False
        self.scan_last_error = ''
        self.scan_observations = {}
        self.independent_control=False
        self.pulse_task=None
        self.recovery_attempts=0
        self.idle_until = 0
        self.stopping = False
        self.lease_owned = False
        from .runtime_status import RuntimeStatus
        self.runtime_status=RuntimeStatus(self)
        self.seen_switches=getattr(self.db.conn,'switches',0)
        for key, value in self.db.state('runtime_settings', {}).items():
            if key in ('daily_target', 'hourly_cap', 'gap_seconds', 'mode', 'ai_auto_publish'):
                setattr(settings, key, value)

    def status(self):
        now = utc()
        last = self.db.state('last_scan')
        return {'paper_mode': self.s.paper_mode, 'publication_mode': self.s.mode,
                'live_adapter_enabled': self.s.live_enabled, 'paused': self.db.state('paused', False),
                'emergency_stop': self.db.state('emergency_stop', False),
                'publisher_blocked': self.db.state('publisher_blocked', False),
                'publication': self.publisher.status(),
                'scheduler': ('running' if self.lease_owned else 'batch_idle' if self.idle_until > now else 'waiting_for_worker_lease')
                             if self.task and not self.task.done() else 'stopped',
                'neon_batch_seconds': self.batch_seconds,
                'next_database_batch': self.idle_until or None,
                'database_storage': self.db.state('storage_usage', {}),
                'last_scan': last, 'data_fresh': bool(last and now - last <= self.s.scan_seconds * 2),
                'scan_running': bool(self.scan_task and not self.scan_task.done()),
                'scan_last_error': self.scan_last_error,
                'scan': dict(self.scan_observations),
                'queue_size': min(10000,self.db.count('draft',['queued','approved','review','chart_rendering','uploading_image'])),
                'cleanup': {**self.cleanup.status(),'last_run':self.db.state('master_cleanup',{})},
                'last_publication': self.db.state('last_publication'),
                'images': {'automatic_charts': self.s.auto_images, 'daily_cap': self.s.image_daily_cap,
                           'retention_days': self.s.image_retention_days,
                           'upload_adapter': 'official_square', 'durable_png_storage': self.db.postgres,
                           'render_workers': 1, 'upload_workers': 1, 'queue_limit': self.s.image_queue_limit,
                           'upload_active': bool(self.image_upload_task and not self.image_upload_task.done()),
                           'ai_illustrations_enabled': False, 'ai_illustrations_configured': False,
                           'ai_illustrations_daily_cap': self.s.ai_image_daily_cap},
                'telegram': 'configured' if self.s.telegram_token else 'disabled',
                'database': 'ok' if self.db.state('schema_version') == 1 else 'unknown',
                'providers': {p: self.db.state('provider:' + p, {'status': 'not_checked'}) for p in self.s.providers},
                'error_count_recent': sum(r['category'] == 'ERROR' for r in self.db.logs(100)),
                'missing_integrations': ['on_chain', 'liquidation_feed', 'automatic_account_metrics'],
                'derivatives': 'enabled' if self.derivatives else 'disabled',
                'market_universe': {'mode': self.s.market_universe, 'status': self.universe_state,
                                    'selected_count': len(self.universe), 'symbols': self.universe,
                                    'scan_batch_size': self.s.scan_batch_size},
                'ai': {**(self.content.ai.status() if isinstance(self.content.ai, AIRouter) else {}),
                       'configured': self.content.ai is not None, 'model': self.s.ai_model,
                       'automatic_validated_posts': self.s.mode == 'automatic' and self.s.ai_auto_publish,
                       'last_generation': self.db.state('last_ai_generation', {})}}

    async def refresh_universe(self):
        if self.s.market_universe == 'watchlist':
            self.universe = list(self.s.symbols)
            return
        if utc() - self.universe_checked_at < 3600:
            return
        self.universe_checked_at = utc()
        for provider in self.pool.providers:
            if not hasattr(provider, 'liquid_symbols'):
                continue
            try:
                self.universe = await provider.liquid_symbols(self.s.universe_limit,
                                                             max(1_000_000, self.s.min_quote_volume))
                self.universe_state = 'discovered'
                self.db.set('market_universe', {'symbols': self.universe, 'at': utc(), 'source': provider.name})
                return
            except (ProviderError, ValueError, KeyError, TypeError):
                pass
        self.universe_state = 'discovery_unavailable_using_previous_or_watchlist'

    def scan_symbols(self):
        if self.s.market_universe == 'watchlist':
            return self.universe
        anchors = [symbol for symbol in ('BTC', 'ETH') if symbol in self.universe]
        others = [symbol for symbol in self.universe if symbol not in anchors]
        size = min(len(others), self.s.scan_batch_size - len(anchors))
        batch = [others[(self.scan_cursor + i) % len(others)] for i in range(size)] if others else []
        if others:
            self.scan_cursor = (self.scan_cursor + size) % len(others)
        return anchors + batch

    def get_draft(self, ident):
        row = self.db.get(ident)
        if row and row['kind'] == 'signal':
            raise ValueError('Paper signal ID is not a draft ID')
        if not row or row['kind'] != 'draft':
            raise ValueError('Unknown draft')
        return row

    def review(self, ident, approve):
        row = self.get_draft(ident)
        if row['status'] not in ('review', 'approved', 'queued'):
            raise ValueError('Draft is immutable after publication or expiry')
        p = row['payload']
        if approve:
            reasons = self.content.checker.check(p)
            duplicate = self.content.checker.duplicate(p, exclude=ident)
            if duplicate:
                reasons.append(duplicate)
            if reasons:
                raise ValueError('; '.join(reasons))
            p['approved_at'] = utc()
        self.db.update(ident, status='approved' if approve else 'rejected', payload=p, clear_due=True)
        self.db.log('CONTENT', 'Administrator approved draft' if approve else 'Administrator rejected draft', ident)

    def edit(self, ident, title, body):
        row = self.get_draft(ident)
        if row['status'] not in ('review', 'approved', 'queued') or len(body) > 20000 or len(title) > 180:
            raise ValueError('Invalid edit or immutable draft')
        p = {**row['payload'], 'title': title, 'body': body, 'approved_at': None, 'human_review_required': True}
        reasons = self.content.checker.check(p)
        if reasons:
            raise ValueError('; '.join(reasons))
        self.db.update(ident, status='review', payload=p, clear_due=True)
        self.db.log('CONTENT', 'Edited draft; approval invalidated', ident)

    async def create_for_symbol(self, symbol, article=False):
        job = AI_JOB.get()
        if job:
            job['stage'] = 'fresh_market_data'
        async def fetch():
            await self.refresh_universe()
            if symbol not in self.universe and symbol not in self.s.symbols:
                raise ValueError('Symbol must be on configured watchlist')
            current = await self.pool.fetch(symbol)
            current.validate(utc(), self.s.candle_max_age)
            return current
        try:
            snap = await asyncio.wait_for(fetch(), timeout=45)
        except asyncio.TimeoutError:
            raise ValueError('Signal market data timed out') from None
        except ProviderError:
            raise ValueError('Market data unavailable') from None
        m = studies(snap)
        event = {'symbol': symbol, 'category': 'mover', 'priority': 40, 'angle': 'scenario',
                 'metrics': m, 'changes': changes(snap), 'as_of': snap.as_of,
                 'event_key': f'{symbol}:manual:{"article" if article else "post"}:{int(snap.as_of // 14400)}'}
        # Preserve this exact snapshot for chart generation even if the live
        # cache advances during AI generation or a restart.
        self.db.archive_snapshot(snap.dict())
        ident = await self.content.draft(event, article)
        await self.automatic_image_async(ident)
        return ident

    async def create_for_signal(self, ident):
        """Revalidate a paper candidate; create a separate, approval-only draft."""
        async with self.signal_draft_lock:
            row = self.db.get(ident)
            if not row or row['kind'] != 'signal':
                raise ValueError('Unknown signal')
            signal = row['payload']
            now = utc()
            if (row['status'] != 'watching' or signal.get('expires_at', 0) <= now
                    or signal.get('as_of', now + 1) > now):
                raise ValueError('Signal is inactive or stale; use a fresh signal')
            for draft in self.db.list('draft', limit=10000):
                if (draft['payload'].get('event') or {}).get('signal_id') != ident:
                    continue
                if draft['status'] in ('review', 'approved', 'queued'):
                    errors = self.content.checker.check(draft['payload'])
                    if not errors and (draft['payload'].get('event') or {}).get('manual_signal_review'):
                        return draft['id']
                if draft['status'] in PUBLICATION_STATES:
                    raise ValueError('Same underlying event already covered')
            if not self.content.ai:
                raise ValueError('Signal analysis requires a configured AI endpoint')
            symbol = signal['symbol']
            context_job = AI_JOB.get()
            if context_job:
                context_job['stage'] = 'fresh_signal_market_data'
            async def fresh_snapshots():
                snapshots = {}
                for asset in dict.fromkeys((symbol, 'BTC', 'ETH')):
                    snap = await self.pool.fetch(asset)
                    snap.validate(utc(), self.s.candle_max_age)
                    if snap.symbol != asset:
                        raise ValueError('Signal market evidence does not match')
                    snapshots[asset] = snap
                return snapshots
            try:
                snapshots = await asyncio.wait_for(fresh_snapshots(), timeout=45)
            except asyncio.TimeoutError:
                raise ValueError('Signal market data timed out') from None
            except ProviderError:
                raise ValueError('Market data unavailable') from None
            if context_job:
                context_job['stage'] = 'signal_setup_revalidation'
            snap = snapshots[symbol]
            if snap.source != signal['source'] or snap.quote != signal['quote']:
                raise ValueError('Signal market evidence does not match')
            # An intervening stop, trigger or expiry cannot be presented as a
            # pending entry setup. Tracking remains separate from publication.
            advance(self.db, snap, self.s)
            if self.db.get(ident)['status'] != 'watching':
                raise ValueError('Signal is inactive or stale; use a fresh signal')
            rows = [{'symbol': asset, 'metrics': studies(s), 'changes': changes(s)}
                    for asset, s in snapshots.items()]
            context = regime(rows)
            market = next(r for r in rows if r['symbol'] == symbol)
            fresh = setup(market, context, self.s)
            if not fresh or fresh['direction'] != signal['direction'] or fresh['type'] != signal['type']:
                raise ValueError('Signal setup no longer qualifies on fresh evidence')
            event = {'symbol': symbol, 'category': 'setup', 'priority': 85, 'angle': 'risk',
                'metrics': {**market['metrics'], **{key: fresh[key] for key in
                    ('entry', 'stop', 'target1', 'target2', 'confidence', 'risk_reward')}},
                'changes': market['changes'], 'direction': fresh['direction'], 'as_of': snap.as_of,
                'signal_id': ident, 'manual_signal_review': True,
                'signal_expires_at': signal['expires_at'], 'original_signal_as_of': signal['as_of'],
                'setup_type': fresh['type'], 'setup_regime': context['name'],
                'event_key': f'{symbol}:signal_review:{ident}:{int(snap.as_of)}'}
            self.db.archive_snapshot(snap.dict())
            return await self.content.draft(event)

    def image_status(self, *, full=False):
        """Default is safe on the independent control thread: no database IO."""
        from .runtime_status import REVISION
        day = datetime.now(ZoneInfo(self.s.timezone)).date().isoformat()
        observed = dict(self.image_observations)
        report = {'implementation_revision': REVISION, 'status_source': 'memory_observations',
            'automatic_charts': self.s.auto_images, 'day': day, 'timezone': self.s.timezone,
            'daily_cap': self.s.image_daily_cap,
            'budget_counts': 'Generated/reserved charts, including unpublished drafts; not published image posts.',
            'render_busy': bool(self.image_render_lock.locked() or
                self.image_render_future and not self.image_render_future.done()),
            'upload_active': bool(self.image_upload_task and not self.image_upload_task.done()),
            'render_timeout_seconds': self.s.image_render_timeout_seconds,
            'upload_request_timeout_seconds': self.s.image_upload_timeout_seconds,
            'upload_recovery_window_seconds': 180,
            'queue_limit': self.s.image_queue_limit,
            **observed,
            'details': '/image_status full reads current budget and last saved outcome from the database.'}
        if observed.get('budget', {}).get('day') != day:
            report['budget'] = {'day': day, 'used_or_reserved': None, 'observed_at': None}
        if full:
            report['status_source'] = 'database_and_memory'
            report['budget'] = {'day': day, 'used_or_reserved': self.db.budget_used(day, 'images'), 'observed_at': utc()}
            self.image_observations = {**self.image_observations, 'budget': report['budget']}
            report['last_saved_outcome'] = self.db.state('image_last_outcome', {})
            report['upload_retry_after'] = self.db.state('image_retry_after', 0)
            # Only aggregate counts; do not download historical drafts or PNGs.
            report['rendering_drafts'] = self.db.count('draft', ['chart_rendering'])
            report['uploading_drafts'] = self.db.count('draft', ['uploading_image'])
        return report

    def record_image_outcome(self, ident, code, stage, started, *, budget_refunded=False, render_timing=None):
        item = outcome(ident, code, stage, started, budget_refunded=budget_refunded)
        values = {**self.image_observations, 'last_attempt': item}
        if code == 'attached':
            values['last_attachment'] = item
        elif code in ('upload_prepared','upload_pending'):
            values['last_upload'] = item
        else:
            values['last_failure'] = item
        if render_timing is not None:
            values['last_render_timing'] = {'draft_id': item['draft_id'], **render_timing}
        self.image_observations = values
        saved = {**item, **({'render_timing': render_timing} if render_timing is not None else {})}
        logger=logging.getLogger('square_desk')
        (logger.info if code in ('attached','upload_prepared','upload_pending') else logger.warning)(
            'IMAGE_RESULT %s', json.dumps(saved, separators=(',', ':')))
        # Observations are best effort. A failed diagnostic write cannot mask
        # the original failure, change approvals or authorize a publication.
        try:
            # Read the actual budget only on /image_status full. Do not add a
            # remote budget SELECT to every scan/render on a slow connection.
            with self.db.transaction():
                self.db.set('image_last_outcome', saved)
                if code not in ('attached', 'upload_prepared','upload_pending'):
                    self.db.log('IMAGE', 'Chart outcome: ' + json.dumps(saved, separators=(',', ':')), item['draft_id'])
        except Exception as error:
            logging.getLogger('square_desk').warning('IMAGE_DIAGNOSTIC_UNSAVED type=%s', type(error).__name__)

    def recover_failed_image(self, plan, filename):
        if plan is None:
            return False
        try:
            with self.db.transaction():
                lease = self.db.state('worker_lease', {})
                owns = self.publisher.worker_owner is None or (lease.get('owner') == self.publisher.worker_owner
                    and lease.get('until', 0) > utc())
                current = self.db.get(plan['id'])
                if not owns or not current:
                    return False
                # A commit acknowledgement may be lost after attachment. Never
                # refund or replace an attachment confirmed on a later read.
                if filename and current['payload'].get('image') == filename:
                    return False
                if current['status'] == 'chart_rendering':
                    self.db.update(plan['id'], status=plan['row']['status'], payload=plan['row']['payload'], clear_due=True)
                # Same bounded release as Repository.release_budget, within the
                # ownership/restoration transaction (nested transactions fail).
                self.db.conn.execute('UPDATE budgets SET used=CASE WHEN used>=? THEN used-? ELSE 0 END WHERE day=? AND category=?',
                    (1, 1, plan['day'], 'images'))
            return True
        except Exception as error:
            logging.getLogger('square_desk').warning('IMAGE_RECOVERY_UNCONFIRMED type=%s', type(error).__name__)
            return False

    def make_image(self, ident, *, automatic=False):
        plan, filename, stage, started = None, None, 'planning', time.monotonic()
        try:
            if self.image_render_lock.locked() or self.image_render_future and not self.image_render_future.done():
                raise ImageFailure('renderer_busy', 'Image renderer busy; text posting remains available')
            plan = self.image_plan(ident, automatic)
            stage = 'rendering'
            filename = self.render_image(plan)
            stage = 'attaching'
            result = self.attach_rendered_image(plan, filename)
        except BaseException as error:
            refunded = self.recover_failed_image(plan, filename)
            self.record_image_outcome(ident, failure_code(error, stage), stage, started, budget_refunded=refunded)
            raise
        self.record_image_outcome(ident, 'attached', 'attached', started)
        return result

    def image_plan(self, ident, automatic):
        row = self.get_draft(ident)
        if row['status'] not in ('review', 'approved', 'queued'):
            raise ImageFailure('immutable', 'Cannot change published media')
        p = dict(row['payload'])
        event = p.get('event')
        pending = [r for r in self.db.list('draft', ['approved', 'queued', 'chart_rendering', 'uploading_image'])
                   if r['id'] != ident and r['payload'].get('image')
                   and not prepared_image_url(r['payload'], self.s.square_key)]
        if len(pending) >= self.s.image_queue_limit:
            raise ImageFailure('queue_full', 'Image queue full; text posting remains available')
        if automatic and not (self.s.auto_images and self.s.mode == 'automatic' and self.s.ai_auto_publish
                and row['status'] == 'approved' and p.get('generated_by') == 'ai'
                and p.get('risk') == 'medium' and not p.get('human_review_required') and event
                and not p.get('campaign_id')):
            raise ImageFailure('ineligible', 'Automatic chart requires an eligible AI draft')
        day = datetime.now(ZoneInfo(self.s.timezone)).date().isoformat()
        if event:
            if utc() - event['as_of'] > self.s.draft_max_age:
                raise ImageFailure('evidence_expired', 'Chart evidence expired')
            data = self.db.state(f"cache:{event['symbol']}:900")
            if not data or data['source'] != event['metrics']['source'] or data['candles'][-1]['end'] != event['as_of']:
                archived = self.db.get(digest([event['metrics']['source'], event['symbol'], 900, event['as_of']]))
                data = archived['payload'] if archived and archived['kind'] == 'snapshot' else None
                if not data:
                    raise ImageFailure('evidence_missing', 'Exact chart evidence unavailable; regenerate draft')
        if not self.db.reserve_budget(day, 'images', 1, self.s.image_daily_cap):
            raise ImageFailure('budget_exhausted', 'Image budget exhausted')
        return {'id': ident, 'payload': p, 'row': row, 'data': data if event else None,
                'day': day, 'automatic': automatic}

    def render_image(self, plan):
        # This function runs on the one chart thread. No database access here.
        p = plan['payload']
        event = p.get('event')
        from .chart_alignment import for_draft
        return (images.chart(Snapshot.read(plan['data']), event['metrics'], self.s.artifacts,
                             context=for_draft(p)) if event
                else images.graphic(p['title'], 'Administrator supplied campaign · Verify official terms', self.s.artifacts))

    def attach_rendered_image(self, plan, filename):
        p, ident = plan['payload'], plan['id']
        with self.db.transaction():
            if self.publisher.worker_owner is not None:
                lease = self.db.state('worker_lease', {})
                if lease.get('owner') != self.publisher.worker_owner or lease.get('until', 0) <= utc():
                    raise ImageFailure('worker_lease_lost', 'Image preparation lost worker lease')
            current = self.db.get(ident)
            if not current or current['status'] not in ('review', 'approved', 'queued', 'chart_rendering'):
                raise ImageFailure('immutable', 'Cannot change published media')
            event = p.get('event')
            if event and utc() - event['as_of'] > self.s.draft_max_age:
                raise ImageFailure('evidence_expired', 'Chart evidence expired')
            p['image_hash'] = persist_png(self.db, self.s.artifacts, filename)
            p['image'] = filename
            p.pop('image_upload', None)
            p.pop('image_pending', None)
            automatic = plan['automatic'] and self.s.auto_images and self.s.mode == 'automatic' and self.s.ai_auto_publish
            if not automatic:
                p['approved_at'], p['human_review_required'] = None, True
            self.db.update(ident, payload=p, status='approved' if automatic else 'review', clear_due=True)
            self.db.log('IMAGE', 'Grounded chart attached by explicit automatic-image setting' if automatic
                        else 'Validated raster saved; media change requires approval', ident)
        return filename

    async def make_image_async(self, ident, *, automatic=False):
        plan, filename, stage, started = None, None, 'planning', time.monotonic()
        work = None
        try:
            if (self.stopping or self.image_render_lock.locked() or
                    self.image_render_future and not self.image_render_future.done()):
                raise ImageFailure('renderer_busy', 'Image renderer busy; text posting remains available')
            async with self.image_render_lock:
                plan = self.image_plan(ident, automatic)
                stage = 'claiming'
                # Must be inside recovery: the image budget is already reserved.
                self.db.update(ident, status='chart_rendering', clear_due=True)
                stage = 'rendering'
                work = RenderWork(self.render_image, plan, self.s.image_render_timeout_seconds)
                self.image_render_future = self.image_executor.submit(work)
                filename = await work.wait(self.image_render_future)
                stage = 'attaching'
                result = self.attach_rendered_image(plan, filename)
        except BaseException as error:
            refunded = self.recover_failed_image(plan, filename)
            self.record_image_outcome(ident, failure_code(error, stage), stage, started, budget_refunded=refunded,
                render_timing=work.timings() if work is not None else None)
            raise
        self.record_image_outcome(ident, 'attached', 'attached', started, render_timing=work.timings())
        return result

    def automatic_image(self, ident):
        if not self.s.auto_images:
            return
        row = self.db.get(ident)
        p = row['payload']
        if row['status'] != 'approved' or p.get('generated_by') != 'ai' or p.get('risk') != 'medium':
            return
        try:
            self.make_image(ident, automatic=True)
        except (ValueError, OSError, MediaError):
            # Never invent a chart or silently remove an existing attachment.
            # If no chart was created, the validated text draft remains usable.
            pass  # make_image saved the specific reason; validated text remains usable.

    async def automatic_image_async(self, ident):
        if not self.s.auto_images:
            return
        row = self.db.get(ident)
        p = row['payload']
        if row['status'] != 'approved' or p.get('generated_by') != 'ai' or p.get('risk') != 'medium':
            return
        try:
            await self.make_image_async(ident, automatic=True)
        except (ValueError, OSError, MediaError, asyncio.TimeoutError):
            pass  # make_image_async saved the specific reason, not a generic skip.

    def start_image_upload(self):
        if self.image_upload_task and not self.image_upload_task.done():
            return
        if (self.stopping or self.s.paper_mode or not self.s.live_enabled
                or self.db.state('paused', False) or self.db.state('emergency_stop', False)
                or self.db.state('publisher_blocked', False) or utc() < self.db.state('image_retry_after', 0)):
            return
        rows = self.db.list('draft', ['approved', 'queued'])
        rows.sort(key=lambda r: (-r['payload']['priority'], r['created']))
        for row in rows:
            p = row['payload']
            if not p.get('image') or prepared_image_url(p, self.s.square_key):
                continue
            pending=p.get('image_pending')
            retry=pending.get('next_check_at',0) if isinstance(pending,dict) else 0
            if type(retry) in (int,float) and 0<retry-utc()<=180:
                continue
            gate=self.publisher.policy.gate(row)
            if (self.content.checker.check(p) or self.publisher.policy.live_gate()
                    or gate not in (None,'Minimum spacing conflict')):
                continue
            if (p.get('risk') == 'high' or p.get('human_review_required') or self.s.mode == 'approval') and not p.get('approved_at'):
                continue
            self.db.update(row['id'], status='uploading_image', clear_due=True)
            self.image_upload_task = asyncio.create_task(self.upload_image_background(row['id']))
            return

    async def upload_image_background(self, ident):
        started = time.monotonic()
        try:
            await self.publisher.prepare_image_job(ident)
            row = self.db.get(ident)
            ready = row and prepared_image_url(row['payload'], self.s.square_key)
            pending=row and row['status']=='approved' and row['payload'].get('image_pending')
            self.record_image_outcome(ident, 'upload_prepared' if ready else 'upload_pending' if pending else 'upload_not_prepared',
                                      'processing' if pending else 'upload', started)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            with self.db.transaction():
                lease = self.db.state('worker_lease', {})
                owns = self.publisher.worker_owner is None or (
                    lease.get('owner') == self.publisher.worker_owner and lease.get('until', 0) > utc())
                if not owns:
                    return
                self.db.log('IMAGE', 'Background upload unavailable: ' + type(error).__name__, ident)
                row = self.db.get(ident)
                if row and row['status'] == 'uploading_image':
                    p = row['payload']
                    p.update(approved_at=None, human_review_required=True)
                    self.db.update(ident, status='review', payload=p, clear_due=True)
            self.record_image_outcome(ident, 'upload_failed', 'upload', started)

    async def stop_image_upload(self):
        if self.image_upload_task:
            self.image_upload_task.cancel()
            try:
                await self.image_upload_task
            except asyncio.CancelledError:
                pass
            self.image_upload_task = None

    async def scan(self):
        async with self.scan_lock:
            started = time.monotonic()
            self.scan_observations = {'started_at': utc(), 'phase': 'discovery', 'observed': 0,
                                      'unavailable': 0, 'candidates': 0, 'drafts_created': 0}
            await self.refresh_universe()
            rows, snapshots = [], {}
            from .planning import PlanningStore
            tracking_store = PlanningStore(self.db)
            semaphore = asyncio.Semaphore(2)
            async def observe(symbol):
                async with semaphore:
                    try:
                        snap = await self.pool.fetch(symbol)
                        try:
                            slow = await self.pool.fetch(symbol, 3600)
                        except (ProviderError, ValueError):
                            slow = None
                        return symbol, snap, slow
                    except (ProviderError, ValueError):
                        return symbol, None, None
            selected = self.scan_symbols()
            tasks = [asyncio.create_task(observe(symbol)) for symbol in selected]
            self.scan_observations.update(phase='market_data', requested=len(tasks))
            try:
                for completed in asyncio.as_completed(tasks):
                    symbol, snap, slow = await completed
                    if snap is None:
                        self.scan_observations['unavailable'] += 1
                        self.db.log('DATA', f'No fresh validated candles for {symbol}')
                        continue
                    if slow and (slow.source != snap.source or slow.quote != snap.quote):
                        slow = None
                    # Validation is repeated after concurrent delivery; a fresh
                    # socket result may have waited behind synchronous storage.
                    try:
                        snap.validate(utc(), self.s.candle_max_age)
                        snapshots[symbol] = snap
                        self.db.archive_snapshot(snap.dict())
                        row = {'symbol': symbol, 'metrics': studies(snap), 'changes': changes(snap, slow)}
                        row['derivatives'] = await self.derivatives.context(symbol) if self.derivatives else None
                        rows.append(row)
                        advance(tracking_store, snap, self.s)
                        self.scan_observations['observed'] += 1
                    except (ProviderError, ValueError):
                        self.scan_observations['unavailable'] += 1
                    await asyncio.sleep(0)
            finally:
                for task in tasks:
                    if not task.done(): task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            # Persist the buffered tracking pass. This flush was missing after
            # the history-read optimization, leaving every outcome in RAM only.
            changed = list(tracking_store.changed.items())
            for offset in range(0, len(changed), 64):
                batch = PlanningStore(self.db)
                batch.changed = dict(changed[offset:offset+64])
                batch.originals = tracking_store.originals
                with self.db.transaction():
                    lease = self.db.state('worker_lease', {})
                    if self.publisher.worker_owner is not None and not (
                        lease.get('owner') == self.publisher.worker_owner and lease.get('until', 0) > utc()):
                        raise ValueError('Worker lease lost before tracking persistence')
                    batch.flush()
                await asyncio.sleep(0)
            self.scan_observations['phase'] = 'candidate_selection'
            rows.sort(key=lambda row: selected.index(row['symbol']))
            if self.market_cache:
                self.market_cache.flush(self.db)
            if not rows:
                self.telegram.notify('Market scan blocked: no fresh validated candles. Numerical publications are withheld.', key=f'data_fail:{int(utc() // 3600)}')
                self.db.set('latest_market', [])
                self.scan_observations.update(phase='finished', reason='No fresh validated candles',
                    seconds=round(time.monotonic()-started, 2), finished_at=utc())
                return
            previous = self.db.state('latest_market', []) if self.s.market_universe == 'top_liquid' else []
            current = {r['symbol']: r for r in previous if r['symbol'] in self.universe
                       and 0 <= utc() - r['metrics']['as_of'] <= self.s.candle_max_age}
            current.update({r['symbol']: r for r in rows})
            self.db.set('latest_market', list(current.values()))
            self.db.set('last_scan', utc())
            context = regime(rows)
            self.db.set('regime', context)
            candidates = []
            for row in rows:
                signal = setup(row, context, self.s)
                if signal:
                    signal['derivatives'] = row.get('derivatives')
                    try:
                        ident = record_setup(self.db, signal)
                        self.telegram.notify(f"Paper signal ${signal['symbol']} {signal['direction']}\nHeuristic score {signal['confidence']:.2f}; not probability.\nEntry {signal['entry']:.6g}, stop {signal['stop']:.6g}, target {signal['target1']:.6g} {signal['quote']}\nSignal ID: {ident}\nPaper tracking only; this ID is not a publishable draft.\nDetails: /preview {ident}\nCreate reviewed AI draft: /signal_draft {ident}", key='signal:' + ident)
                    except sqlite3.IntegrityError:
                        pass
                    candidates.append({'symbol': row['symbol'], 'category': 'setup', 'priority': 85,
                                       'angle': 'risk', 'metrics': {**row['metrics'], 'entry': signal['entry'],
                                       'stop': signal['stop'], 'target1': signal['target1'], 'target2': signal['target2'],
                                       'confidence': signal['confidence'], 'risk_reward': signal['risk_reward']},
                                       'changes': row['changes'], 'direction': signal['direction'],
                                       'as_of': row['metrics']['as_of'], 'signal_id': signal['id'],
                                       'event_key': f"{row['symbol']}:setup:{int(row['metrics']['as_of'] // 14400)}"})
                candidates.extend(events(row, self.s))
            weights = self.db.state('category_weights', {})
            candidates.sort(key=lambda e: -(e['priority'] * weights.get(e['category'], 1)))
            self.scan_observations['candidates'] = len(candidates)
            self.scan_observations['reason'] = 'No qualifying market events' if not candidates else 'Evaluating candidates'
            created = 0
            attempted = 0
            planning = PlanningStore(self.db)
            # This is a generation plan only. Refresh it after every awaited
            # draft attempt; the publisher always reads current controls again.
            from .compliance import Compliance
            planning_policy = Compliance(self.s, planning)
            for event in candidates:
                if attempted >= self.s.max_drafts_per_scan or len(planning.list('draft', ['review', 'approved', 'queued'])) >= 50:
                    self.scan_observations['reason'] = 'Draft attempt or review queue limit reached'
                    break
                published = planning.list('draft', PUBLICATION_STATES, limit=10000)
                start, hour = self.publisher.policy.window(utc())
                total_today = sum(self.publisher.policy.window(r['payload'].get('submitted_at', r['updated']))[0] == start for r in published)
                pending = len(planning.list('draft', ['approved', 'queued'] if self.s.mode == 'automatic' else ['approved', 'queued', 'review']))
                if total_today >= planning_policy.caps()['daily'] or pending >= self.s.max_drafts_per_scan:
                    self.scan_observations['reason'] = 'Daily publication cap or pending draft limit reached'
                    break
                if self.s.mode == 'automatic' and self.s.ai_auto_publish:
                    if hour not in self.s.publishing_hours or event['category'] in ('shock', 'setup'):
                        self.scan_observations['reason'] = 'Outside publication hours or event requires review'
                        continue
                    gap = planning_policy.caps()['gap']
                    # Prepare one fresh draft shortly before the next possible
                    # slot. A separate generation gap used to add scan + AI time
                    # after every publication gap, progressively losing slots.
                    last_sent = max((r['payload'].get('submitted_at', r['updated']) for r in published), default=0)
                    media_pending = len(planning.list('draft', ['chart_rendering', 'uploading_image']))
                    if pending or media_pending or utc() < last_sent + gap - min(300, gap/4):
                        self.scan_observations['reason'] = 'Fresh draft already pending or next slot not yet near'
                        break
                article = False
                if self.s.auto_articles and self.s.mode == 'automatic' and self.s.ai_auto_publish and self.content.ai:
                    expected = min(self.s.article_daily_cap, int(hour >= 10) + int(hour >= 18))
                    related = published + planning.list('draft', ['approved', 'queued', 'review'])
                    articles_today = sum(r['payload'].get('article', False)
                                         and self.publisher.policy.window(r['payload'].get('submitted_at', r['created']))[0] == start
                                         for r in related)
                    article = articles_today < expected
                if article:
                    event = {**event, 'event_key': event['event_key'] + ':article'}
                if any(r['status'] not in ('expired', 'rejected')
                       and (r['payload'].get('event') or {}).get('event_key') == event['event_key']
                       for r in planning.list('draft', since=int(start) - 7*86400, limit=10000)):
                    self.scan_observations['reason'] = 'Event already has a draft'
                    continue
                attempted += 1
                if self.telegram.job_task and not self.telegram.job_task.done():
                    # Collect/track market evidence normally, but give manual
                    # generation the next AI slot rather than queueing a scan.
                    self.scan_observations['reason'] = 'Manual generation owns the next AI slot'
                    break
                try:
                    if not 0 <= utc() - event['as_of'] <= self.s.candle_max_age:
                        self.scan_observations['reason'] = 'Observation aged before drafting; next scan must refresh it'
                        continue
                    self.scan_observations.update(phase='draft_generation', symbol=event['symbol'])
                    ident = await self.content.draft(event, article=article)
                    await self.automatic_image_async(ident)
                    # Generate real charts only for useful events. Text-only live
                    # publishing can be used without requiring a media adapter.
                    if (event['category'] in ('volume', 'shock') and (self.s.paper_mode or not self.s.live_enabled)
                            and not self.db.get(ident)['payload'].get('image')):
                        try:
                            await self.make_image_async(ident)
                        except (ValueError, OSError, MediaError, asyncio.TimeoutError):
                            self.db.log('IMAGE', 'Image omitted: budget, evidence or rendering failure', ident)
                    row = self.db.get(ident)
                    controls = f'\n\n/approve {ident}\n/reject {ident}' if row['status'] == 'review' else '\nValidated draft approved; awaiting scheduler and publication gates.'
                    if row['payload'].get('image'):
                        controls += '\nChart attached. Dashboard: Preview chart.'
                    self.telegram.notify(f"Draft {ident} [{row['status']}] [{row['payload']['generated_by']}]\n{row['payload']['title']}\n\n{row['payload']['body']}" + controls, key='draft:' + ident)
                    if row['status'] == 'approved' and self.s.mode == 'automatic':
                        self.db.set('last_auto_draft', utc())
                    created += 1
                except (ValueError, sqlite3.IntegrityError) as error:
                    self.db.log('CONTENT', rejection_message(error))
                    self.scan_observations['reason'] = rejection_message(error)
                finally:
                    planning = PlanningStore(self.db)
                    planning_policy = Compliance(self.s, planning)
            day = datetime.now(ZoneInfo(self.s.timezone)).date().isoformat()
            if created == 0 and self.content.ai is None and self.db.state('last_education_day') != day:
                ident = self.content.education()
                if ident:
                    self.db.set('last_education_day', day)
                    self.telegram.notify('Educational draft ' + ident + '\n' + self.db.get(ident)['payload']['title'], key='draft:' + ident)
            self.scan_observations.update(phase='finished', drafts_created=created, attempted=attempted,
                seconds=round(time.monotonic()-started, 2), finished_at=utc())
            if created: self.scan_observations['reason'] = 'Validated drafts created; publication gates apply'

    def weekly(self):
        now = datetime.now(ZoneInfo(self.s.timezone))
        key = now.strftime('%G-W%V')
        if now.weekday() != self.s.report_weekday or (now.hour, now.minute) < (self.s.report_hour, self.s.report_minute):
            return
        if self.db.state('last_weekly_report') == key:
            return
        report = self.analytics.report()
        self.analytics.optimize(report)
        filename = 'weekly-' + key + '.json'
        Path(self.s.artifacts, filename).write_text(json.dumps(report, indent=2), encoding='utf-8')
        self.db.insert('report', {'file': filename, 'report': report}, 'generated', fingerprint=key)
        self.telegram.notify('Weekly report\n' + json.dumps(report, indent=2), key='weekly:' + key)
        self.db.set('last_weekly_report', key)

    async def cycle(self):
        self.runtime_status.stage('cycle_start')
        switches=getattr(self.db.conn,'switches',0)
        if switches!=self.seen_switches:
            self.db.recover_submissions()
            self.seen_switches=switches
        if self.scan_task and self.scan_task.done():
            try:
                self.scan_task.result()
                self.scan_last_error=''
            except asyncio.CancelledError:
                pass
            except Exception as error:
                self.scan_last_error=type(error).__name__
                self.scan_observations.update(phase='failed', reason=self.scan_last_error, finished_at=utc())
                self.db.log('ERROR','Background scan failed: '+self.scan_last_error)
            self.scan_task=None
        self.start_image_upload()
        self.runtime_status.stage('expiring_drafts')
        # Expired review drafts must not indefinitely block fresh content.
        for row in self.db.list('draft', ['review', 'approved', 'queued']):
            event = row['payload'].get('event')
            if event and utc() - event['as_of'] > self.s.draft_max_age:
                self.db.update(row['id'], status='expired', clear_due=True)
            await asyncio.sleep(0)
        if not self.telegram_task:
            await self.telegram.poll()
            await self.telegram.flush()
        self.campaigns.expire()
        self.runtime_status.stage('news_and_scan')
        await self.news.poll()
        if utc() - self.db.state('last_scan_attempt', 0) >= self.s.scan_seconds or self.db.state('scan_requested', False):
            if not self.scan_task:
                self.db.set('last_scan_attempt', utc())
                self.db.set('scan_requested', False)
                if self.background_scans and not self.batch_seconds:
                    self.scan_task=asyncio.create_task(self.scan())
                else:
                    await self.scan()
        self.start_image_upload()
        background_media = self.s.live_enabled and not self.s.paper_mode
        self.runtime_status.stage('scheduling')
        self.scheduler.allocate(ready_media_only=background_media)
        await asyncio.sleep(0)
        if not self.telegram_task:
            await self.telegram.poll()
            await self.telegram.flush()
        if utc() >= self.db.state('publish_retry_after', 0):
            for row in self.scheduler.due():
                self.runtime_status.stage('publishing')
                await self.publisher.publish(row['id'])
                after = self.db.get(row['id'])
                if after['status'] in ('published', 'paper_published', 'manual_ready', 'uncertain'):
                    self.db.set('last_publication', utc())
                    chart = ('\nChart: sent with this post; Square receipt confirmed.'
                        if after['status'] == 'published' and after['payload'].get('receipt', {}).get('adapter') == 'official_square_image'
                        else '')
                    self.telegram.notify(f"Publication {row['id']}: {after['status']}" + chart, key='publish:' + row['id'])
        if self.batch_seconds and self.image_upload_task:
            # Saver mode finishes its bounded upload before closing the database;
            # normal zero-batch mode never waits for this background worker.
            await self.image_upload_task
        self.runtime_status.stage('notifications_and_maintenance')
        if not self.telegram_task:
            await self.telegram.flush()
        self.weekly()
        self.cleanup.maybe_start()
        if utc() - self.db.state('last_maintenance', 0) >= 3600:
            size = self.db.storage_bytes()
            self.db.set('storage_usage', {'postgres_database_bytes': size, 'checked_at': utc(),
                                         'note': 'Database size only; Neon quota/history usage must be checked in Neon dashboard'})
            if self.db.postgres and size >= 350_000_000:
                self.telegram.notify('Database size exceeds 350 MB. Check Neon storage/compute quotas and backups; do not delete publication history.',
                                     key='storage-warning:' + datetime.now(ZoneInfo(self.s.timezone)).date().isoformat())
            self.db.set('last_maintenance', utc())
        self.db.set('last_cycle', utc())
        self.runtime_status.stage('idle')

    def prune_artifacts(self):
        import re
        cutoff = utc() - self.s.retention_days * 86400
        root = Path(self.s.artifacts).resolve()
        pending = self.db.list('draft', ['review', 'approved', 'queued', 'chart_rendering', 'uploading_image', 'preparing_media', 'sending', 'uncertain'])
        protected = {r['payload'].get('image') for r in pending}
        for path in root.iterdir():
            # Only app-generated names inside the configured artifact root.
            if path.is_symlink() or path.resolve().parent != root or path.name in protected:
                continue
            file_cutoff = utc() - self.s.image_retention_days * 86400 if path.suffix == '.png' else cutoff
            if re.fullmatch(r'(?:[a-f0-9]{32,64}|weekly-\d{4}-W\d{2})\.(?:txt|png|json)', path.name) and path.stat().st_mtime < file_cutoff:
                path.unlink()

    async def heartbeat(self):
        while not self.stopping:
            try:
                self.lease_owned = self.db.acquire_lease(self.owner, 120)
            except Exception as exc:
                self.lease_owned = False
                logging.getLogger('square_desk').error('Worker lease renewal failed: %s', type(exc).__name__)
            if not self.lease_owned:
                await self.stop_scan()
                await self.telegram.stop_job()
                await self.stop_image_upload()
                if self.task:
                    self.task.cancel()
                return
            await asyncio.sleep(30)

    async def stop_scan(self):
        if self.scan_task:
            task=self.scan_task
            self.scan_task=None
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as error:
                logging.getLogger('square_desk').error('Scan stopped: %s',type(error).__name__)

    async def serve_telegram(self):
        while not self.stopping:
            if self.lease_owned:
                try:
                    await self.telegram.poll()
                    await self.telegram.flush()
                except Exception as error:
                    logging.getLogger('square_desk').error('Telegram control unavailable: %s', type(error).__name__)
            else:
                await self.telegram.stop_job()
            await asyncio.sleep(3)

    async def observe_market(self):
        # Public API observations stay in RAM between durable batches. A restart
        # discards only this cache; fresh source candles are fetched again.
        while not self.stopping:
            # Only warm configured anchors; rotating discovery is done in a
            # durable scan rather than fetching the entire universe twice.
            for symbol in self.s.symbols:
                for interval in (900, 3600):
                    try:
                        await self.pool.fetch(symbol, interval)
                    except (ProviderError, ValueError):
                        pass
            await asyncio.sleep(self.s.scan_seconds)

    async def finish_batch(self):
        await self.stop_image_upload()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None
        try:
            self.db.release_lease(self.owner)
            self.db.idle()
        finally:
            self.lease_owned = False
            await self.stop_scan()

    async def run(self):
        while not self.stopping:
            batch_started = utc()
            try:
                if not self.lease_owned:
                    self.lease_owned = self.db.acquire_lease(self.owner, 120)
                    if not self.lease_owned:
                        await asyncio.sleep(5)
                        continue
                    # Recovery must only happen after exclusive ownership.
                    self.db.recover()
                    self.heartbeat_task = asyncio.create_task(self.heartbeat())
                    logging.getLogger('square_desk').info('Worker lease acquired; scheduler active')
                await self.cycle()
                self.recovery_attempts=0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Exception class only; network exception strings may hold keys.
                logging.getLogger('square_desk').error(json.dumps({'category': 'ERROR', 'at': utc(), 'correlation': self.owner, 'error_type': type(exc).__name__}))
                try:
                    self.db.log('ERROR', 'Worker cycle failed: ' + type(exc).__name__)
                    self.telegram.notify('A worker cycle failed. Review /errors; publication safety checks remain active.', key=f'worker_error:{int(utc() // 3600)}')
                except Exception:
                    # Database errors cannot themselves be recorded in an
                    # unavailable database. The supervisor retries safely.
                    raise
            if self.batch_seconds:
                await self.finish_batch()
                # Keep at least ten minutes without background DB queries.
                # Neon can scale to zero after its idle timeout. Interactive
                # dashboard/webhook traffic can still wake the database.
                delay = max(600, self.batch_seconds - (utc() - batch_started))
                self.idle_until = utc() + delay
                await asyncio.sleep(delay)
                self.idle_until = 0
            else:
                await asyncio.sleep(10)

    async def supervise(self):
        while not self.stopping:
            try:
                await self.run()
                return
            except asyncio.CancelledError:
                if self.stopping:
                    raise
                # Lease renewal failed: cancel the current cycle and retry only
                # after a cooldown and fresh exclusive lease acquisition.
                logging.getLogger('square_desk').warning('Worker interrupted; waiting before safe restart')
            except Exception as exc:
                logging.getLogger('square_desk').error('Worker unavailable: %s', type(exc).__name__)
            self.lease_owned = False
            await self.stop_scan()
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            self.recovery_attempts+=1
            delay = self.batch_seconds or min(120,5*2**min(self.recovery_attempts-1,5))
            self.runtime_status.stage('recovery_wait')
            self.idle_until = utc() + delay
            await asyncio.sleep(delay)
            self.idle_until = 0

    async def start(self):
        self.runtime_status.stage('acquiring_worker_lease')
        logging.getLogger('square_desk').warning('WORKER_STARTING revision=2026-10-16-render-deadline-13')
        self.background_scans=True
        self.publisher.worker_owner = self.owner
        self.lease_owned = self.db.acquire_lease(self.owner, 120)
        if self.lease_owned:
            self.db.recover()
            self.seen_switches=getattr(self.db.conn,'switches',0)
            self.heartbeat_task = asyncio.create_task(self.heartbeat())
        else:
            # During a rolling deploy Render keeps the old instance running
            # until this instance is healthy. Serve the dashboard/health while
            # waiting; collection, Telegram polling and publishing stay idle.
            logging.getLogger('square_desk').info(
                'Waiting for previous worker lease; HTTP ready, scheduler standby')
        self.pulse_task=asyncio.create_task(self.runtime_status.pulse())
        if (self.independent_control and self.owned_client and not self.batch_seconds
                and self.s.telegram_polling and self.s.telegram_token):
            from .telegram_control import TelegramControl
            control=TelegramControl(self,asyncio.get_running_loop(),self.db.state('telegram_offset',0))
            self.telegram.control_runtime=control
            await control.start()
        logging.getLogger('square_desk').warning('WORKER_READY lease_owned=%s telegram_poller=%s',
            self.lease_owned,'independent' if self.telegram.control_runtime else 'worker_or_disabled')
        self.task = asyncio.create_task(self.supervise())
        if not self.batch_seconds and self.s.telegram_polling and self.s.telegram_token:
            self.telegram_task = asyncio.create_task(self.serve_telegram())
        if self.market_cache:
            self.market_task = asyncio.create_task(self.observe_market())

    async def close(self):
        self.stopping = True
        self.runtime_status.stage('stopping')
        if self.telegram.control_runtime:
            await self.telegram.control_runtime.close()
        if self.pulse_task:
            self.pulse_task.cancel()
            try:await self.pulse_task
            except asyncio.CancelledError:pass
        await self.stop_scan()
        await self.cleanup.aclose()
        for task in (self.task, self.heartbeat_task, self.market_task, self.telegram_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logging.getLogger('square_desk').error('Worker stopped with error: %s', type(exc).__name__)
        await self.telegram.stop_job()
        await self.stop_image_upload()
        self.image_executor.shutdown(wait=False, cancel_futures=True)
        try:
            # Checking owner and releasing must be one transaction so a retiring
            # instance cannot erase the successor's lease.
            self.db.release_lease(self.owner)
        finally:
            await self.client.aclose()
            await asyncio.to_thread(self.db.close)
