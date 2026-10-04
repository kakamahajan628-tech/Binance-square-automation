from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import asyncio
import json
import logging
import sqlite3
import httpx
from .models import Snapshot, uid, utc, digest
from .store import Store
from .providers import Transport, BinanceProvider, CoinbaseProvider, ProviderPool, ProviderError
from .analysis import studies, changes, regime, events, setup
from .content import ContentEngine
from .ai_router import AIRouter
from .campaigns import CampaignManager
from .analytics import Analytics
from .scheduler import Scheduler
from .publisher import PublicationService
from .telegram import Telegram, rejection_message
from .compliance import PUBLICATION_STATES
from .tracking import record_setup, advance
from . import images
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
        self.client = client or httpx.AsyncClient(timeout=20, follow_redirects=False,
                                                 limits=httpx.Limits(max_connections=10, max_keepalive_connections=5))
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
        self.universe = list(settings.symbols)
        self.universe_checked_at = 0
        self.scan_cursor = 0
        self.universe_state = 'watchlist'
        self.owner = uid()
        self.task = None
        self.heartbeat_task = None
        self.market_task = None
        self.telegram_task = None
        self.idle_until = 0
        self.stopping = False
        self.lease_owned = False
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
                'scheduler': ('running' if self.lease_owned else 'batch_idle' if self.idle_until > now else 'waiting_for_worker_lease')
                             if self.task and not self.task.done() else 'stopped',
                'neon_batch_seconds': self.batch_seconds,
                'next_database_batch': self.idle_until or None,
                'database_storage': self.db.state('storage_usage', {}),
                'last_scan': last, 'data_fresh': bool(last and now - last <= self.s.scan_seconds * 2),
                'queue_size': len(self.db.list('draft', ['queued', 'approved', 'review'], limit=10000)),
                'last_publication': self.db.state('last_publication'),
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
        await self.refresh_universe()
        if symbol not in self.universe and symbol not in self.s.symbols:
            raise ValueError('Symbol must be on configured watchlist')
        snap = await self.pool.fetch(symbol)
        m = studies(snap)
        event = {'symbol': symbol, 'category': 'mover', 'priority': 40, 'angle': 'scenario',
                 'metrics': m, 'changes': changes(snap), 'as_of': snap.as_of,
                 'event_key': f'{symbol}:manual:{"article" if article else "post"}:{int(snap.as_of // 14400)}'}
        return await self.content.draft(event, article)

    def make_image(self, ident):
        row = self.get_draft(ident)
        if row['status'] not in ('review', 'approved', 'queued'):
            raise ValueError('Cannot change published media')
        p = row['payload']
        event = p.get('event')
        day = datetime.now(ZoneInfo(self.s.timezone)).date().isoformat()
        if not self.db.reserve_budget(day, 'images', 1, self.s.image_daily_cap):
            raise ValueError('Image budget exhausted')
        if event:
            if utc() - event['as_of'] > self.s.draft_max_age:
                raise ValueError('Chart evidence expired')
            data = self.db.state(f"cache:{event['symbol']}:900")
            if not data or data['source'] != event['metrics']['source'] or data['candles'][-1]['end'] != event['as_of']:
                raise ValueError('Exact chart evidence unavailable; regenerate draft')
            filename = images.chart(Snapshot.read(data), event['metrics'], self.s.artifacts)
        else:
            filename = images.graphic(p['title'], 'Administrator supplied campaign · Verify official terms', self.s.artifacts)
        p['image'], p['approved_at'], p['human_review_required'] = filename, None, True
        import hashlib
        p['image_hash'] = hashlib.sha256(Path(self.s.artifacts, filename).read_bytes()).hexdigest()
        self.db.update(ident, payload=p, status='review', clear_due=True)
        self.db.log('IMAGE', 'Validated raster saved; media change requires approval', ident)
        return filename

    async def scan(self):
        async with self.scan_lock:
            await self.refresh_universe()
            rows, snapshots = [], {}
            for symbol in self.scan_symbols():
                try:
                    snap = await self.pool.fetch(symbol)
                    try:
                        slow = await self.pool.fetch(symbol, 3600)
                    except (ProviderError, ValueError):
                        slow = None
                    if slow and (slow.source != snap.source or slow.quote != snap.quote):
                        slow = None
                    snapshots[symbol] = snap
                    self.db.archive_snapshot(snap.dict())
                    row = {'symbol': symbol, 'metrics': studies(snap), 'changes': changes(snap, slow)}
                    row['derivatives'] = await self.derivatives.context(symbol) if self.derivatives else None
                    rows.append(row)
                    advance(self.db, snap, self.s)
                except (ProviderError, ValueError):
                    self.db.log('DATA', f'No fresh validated candles for {symbol}')
            if self.market_cache:
                self.market_cache.flush(self.db)
            if not rows:
                self.telegram.notify('Market scan blocked: no fresh validated candles. Numerical publications are withheld.', key=f'data_fail:{int(utc() // 3600)}')
                self.db.set('latest_market', [])
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
                        self.telegram.notify(f"Potential setup ${signal['symbol']} {signal['direction']}\nHeuristic score {signal['confidence']}; not probability.\nEntry {signal['entry']:.6g}, stop {signal['stop']:.6g}, target {signal['target1']:.6g} {signal['quote']}\n{ident}\nPaper tracking only.", key='signal:' + ident)
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
            created = 0
            attempted = 0
            for event in candidates:
                if attempted >= self.s.max_drafts_per_scan or len(self.db.list('draft', ['review', 'approved', 'queued'])) >= 50:
                    break
                published = self.db.list('draft', PUBLICATION_STATES, limit=10000)
                start, hour = self.publisher.policy.window(utc())
                total_today = sum(self.publisher.policy.window(r['payload'].get('submitted_at', r['updated']))[0] == start for r in published)
                pending = len(self.db.list('draft', ['approved', 'queued'] if self.s.mode == 'automatic' else ['approved', 'queued', 'review']))
                if total_today >= self.publisher.policy.caps()['daily'] or pending >= self.s.max_drafts_per_scan:
                    break
                if self.s.mode == 'automatic' and self.s.ai_auto_publish:
                    if hour not in self.s.publishing_hours or event['category'] in ('shock', 'setup'):
                        continue
                    gap = self.publisher.policy.caps()['gap']
                    if utc() - self.db.state('last_auto_draft', 0) < gap:
                        break
                article = False
                if self.s.auto_articles and self.s.mode == 'automatic' and self.s.ai_auto_publish and self.content.ai:
                    expected = min(self.s.article_daily_cap, int(hour >= 10) + int(hour >= 18))
                    related = published + self.db.list('draft', ['approved', 'queued', 'review'])
                    articles_today = sum(r['payload'].get('article', False)
                                         and self.publisher.policy.window(r['payload'].get('submitted_at', r['created']))[0] == start
                                         for r in related)
                    article = articles_today < expected
                if article:
                    event = {**event, 'event_key': event['event_key'] + ':article'}
                if any(r['status'] not in ('expired', 'rejected')
                       and (r['payload'].get('event') or {}).get('event_key') == event['event_key']
                       for r in self.db.list('draft', since=utc() - 7*86400, limit=10000)):
                    continue
                attempted += 1
                try:
                    ident = await self.content.draft(event, article=article)
                    # Generate real charts only for useful events. Text-only live
                    # publishing can be used without requiring a media adapter.
                    if event['category'] in ('volume', 'shock') and (self.s.paper_mode or not self.s.live_enabled):
                        try:
                            self.make_image(ident)
                        except (ValueError, OSError):
                            self.db.log('IMAGE', 'Image omitted: budget, evidence or rendering failure', ident)
                    row = self.db.get(ident)
                    controls = f'\n\n/approve {ident}\n/reject {ident}' if row['status'] == 'review' else '\nValidated draft scheduled automatically.'
                    self.telegram.notify(f"Draft {ident} [{row['status']}] [{row['payload']['generated_by']}]\n{row['payload']['title']}\n\n{row['payload']['body']}" + controls, key='draft:' + ident)
                    if row['status'] == 'approved' and self.s.mode == 'automatic':
                        self.db.set('last_auto_draft', utc())
                    created += 1
                except (ValueError, sqlite3.IntegrityError) as error:
                    self.db.log('CONTENT', rejection_message(error))
            day = datetime.now(ZoneInfo(self.s.timezone)).date().isoformat()
            if created == 0 and self.content.ai is None and self.db.state('last_education_day') != day:
                ident = self.content.education()
                if ident:
                    self.db.set('last_education_day', day)
                    self.telegram.notify('Educational draft ' + ident + '\n' + self.db.get(ident)['payload']['title'], key='draft:' + ident)

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
        # Expired review drafts must not indefinitely block fresh content.
        for row in self.db.list('draft', ['review', 'approved', 'queued']):
            event = row['payload'].get('event')
            if event and utc() - event['as_of'] > self.s.draft_max_age:
                self.db.update(row['id'], status='expired', clear_due=True)
        if not self.telegram_task:
            await self.telegram.poll()
            await self.telegram.flush()
        self.campaigns.expire()
        await self.news.poll()
        if utc() - self.db.state('last_scan_attempt', 0) >= self.s.scan_seconds or self.db.state('scan_requested', False):
            self.db.set('last_scan_attempt', utc())
            self.db.set('scan_requested', False)
            await self.scan()
        self.scheduler.allocate()
        if not self.telegram_task:
            await self.telegram.poll()
            await self.telegram.flush()
        if utc() >= self.db.state('publish_retry_after', 0):
            for row in self.scheduler.due():
                await self.publisher.publish(row['id'])
                after = self.db.get(row['id'])
                if after['status'] in ('published', 'paper_published', 'manual_ready', 'uncertain'):
                    self.db.set('last_publication', utc())
                    self.telegram.notify(f"Publication {row['id']}: {after['status']}", key='publish:' + row['id'])
        await self.telegram.flush()
        self.weekly()
        if utc() - self.db.state('last_maintenance', 0) >= 3600:
            self.db.prune(self.s.retention_days, self.s.snapshot_retention_days)
            self.prune_artifacts()
            size = self.db.storage_bytes()
            self.db.set('storage_usage', {'postgres_database_bytes': size, 'checked_at': utc(),
                                         'note': 'Database size only; Neon quota/history usage must be checked in Neon dashboard'})
            if self.db.postgres and size >= 350_000_000:
                self.telegram.notify('Database size exceeds 350 MB. Check Neon storage/compute quotas and backups; do not delete publication history.',
                                     key='storage-warning:' + datetime.now(ZoneInfo(self.s.timezone)).date().isoformat())
            self.db.set('last_maintenance', utc())
        self.db.set('last_cycle', utc())

    def prune_artifacts(self):
        import re
        cutoff = utc() - self.s.retention_days * 86400
        root = Path(self.s.artifacts).resolve()
        pending = self.db.list('draft', ['review', 'approved', 'queued'])
        protected = {r['payload'].get('image') for r in pending}
        for path in root.iterdir():
            # Only app-generated names inside the configured artifact root.
            if path.is_symlink() or path.resolve().parent != root or path.name in protected:
                continue
            if re.fullmatch(r'(?:[a-f0-9]{32,64}|weekly-\d{4}-W\d{2})\.(?:txt|png|json)', path.name) and path.stat().st_mtime < cutoff:
                path.unlink()

    async def heartbeat(self):
        while not self.stopping:
            try:
                self.lease_owned = self.db.acquire_lease(self.owner, 120)
            except Exception as exc:
                self.lease_owned = False
                logging.getLogger('square_desk').error('Worker lease renewal failed: %s', type(exc).__name__)
            if not self.lease_owned:
                await self.telegram.stop_job()
                if self.task:
                    self.task.cancel()
                return
            await asyncio.sleep(30)

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
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            delay = self.batch_seconds or 120
            self.idle_until = utc() + delay
            await asyncio.sleep(delay)
            self.idle_until = 0

    async def start(self):
        self.publisher.worker_owner = self.owner
        self.lease_owned = self.db.acquire_lease(self.owner, 120)
        if self.lease_owned:
            self.db.recover()
            self.heartbeat_task = asyncio.create_task(self.heartbeat())
        else:
            # During a rolling deploy Render keeps the old instance running
            # until this instance is healthy. Serve the dashboard/health while
            # waiting; collection, Telegram polling and publishing stay idle.
            logging.getLogger('square_desk').info(
                'Waiting for previous worker lease; HTTP ready, scheduler standby')
        self.task = asyncio.create_task(self.supervise())
        if not self.batch_seconds and self.s.telegram_polling and self.s.telegram_token:
            self.telegram_task = asyncio.create_task(self.serve_telegram())
        if self.market_cache:
            self.market_task = asyncio.create_task(self.observe_market())

    async def close(self):
        self.stopping = True
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
        try:
            # Checking owner and releasing must be one transaction so a retiring
            # instance cannot erase the successor's lease.
            self.db.release_lease(self.owner)
        finally:
            await self.client.aclose()
            self.db.close()
