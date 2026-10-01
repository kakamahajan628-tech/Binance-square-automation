import asyncio
from dataclasses import replace
from pathlib import Path
import json
import sqlite3
import time
import pytest
import httpx
from fastapi.testclient import TestClient
from square_desk.config import Settings
from square_desk.store import Store
from square_desk.models import Candle, Snapshot, utc
from square_desk.providers import ProviderPool, ProviderError
from square_desk.analysis import studies, changes, rank, rsi, setup
from square_desk.content import ContentEngine, FactChecker, deterministic_draft, similarity
from square_desk.campaigns import parse_campaign
from square_desk.tracking import statistics, record_setup, advance
from square_desk.compliance import Compliance
from square_desk.scheduler import Scheduler
from square_desk.publisher import SquarePublisher, AmbiguousPublication
from square_desk.service import Desk
from square_desk.app import create_app


@pytest.fixture
def settings(tmp_path):
    return Settings(database=str(tmp_path / 'desk.sqlite3'), artifacts=str(tmp_path / 'artifacts'),
                    admin_token='test-private-password', publishing_hours=tuple(range(24)))


def snapshot(length=200, interval=900, end=None, symbol='BTC'):
    end = int(utc() // interval) * interval if end is None else end
    candles = [Candle(end - (length-i)*interval, end - (length-i-1)*interval,
                      100+i*.1, 101+i*.1, 99+i*.1, 100.5+i*.1, 100) for i in range(length)]
    return Snapshot(symbol, 'USDT', 'binance', interval, utc(), candles)


def event(snap=None, category='mover', key=None):
    s = snap or snapshot()
    return {'symbol': s.symbol, 'category': category, 'priority': 50 if category == 'mover' else 100,
            'angle': 'structure', 'metrics': studies(s), 'changes': changes(s), 'as_of': s.as_of,
            'event_key': key or s.symbol + ':mover:' + str(s.as_of)}


def draft_payload(ev=None):
    ev = ev or event()
    return {**deterministic_draft(ev), 'event': ev, 'article': False, 'risk': 'medium',
            'priority': ev['priority'], 'category': ev['category'], 'approved_at': utc(),
            'human_review_required': False, 'image': None}


def test_rank_windows_and_insufficient_history():
    s = snapshot()
    c = changes(s, snapshot(interval=3600))
    assert c['15m'] > 0 and c['7d'] > c['3d'] > 0 and c['24h'] > 0
    assert changes(snapshot(60))['24h'] is None
    rows = [{'symbol': 'A', 'changes': {'1h': 2}}, {'symbol': 'B', 'changes': {'1h': -3}}, {'symbol': 'C', 'changes': {'1h': None}}]
    assert rank(rows, '1h')[0]['symbol'] == 'A'
    assert rank(rows, '1h', False)[0]['symbol'] == 'B'


def test_studies_and_support_resistance():
    s = snapshot()
    m = studies(s)
    assert m['support'] == min(c.low for c in s.candles[-21:-1])
    assert m['resistance'] == max(c.high for c in s.candles[-21:-1])
    assert m['relative_volume'] == 1 and m['atr'] > 0
    assert rsi([1] * 100) == 50
    assert rsi(list(range(100))) == 100


def test_setup_filter_and_geometry(settings):
    s = snapshot()
    row = {'symbol': 'BTC', 'metrics': studies(s), 'changes': changes(s)}
    context = {'name': 'risk_on', 'btc_1h': 2, 'eth_1h': 2}
    assert setup(row, context, settings) is None
    row['metrics'].update(price=130, resistance=125, ema20=120, ema50=115, rsi=60, relative_volume=3, quote_volume_24h=1e8)
    signal = setup(row, context, settings)
    assert signal['stop'] < signal['entry'] < signal['target1'] < signal['target2']
    assert signal['risk_reward'] == pytest.approx((signal['target1']-signal['entry'])/(signal['entry']-signal['stop']))
    assert setup(row, {**context, 'btc_1h': None}, settings) is None
    assert setup(row, {**context, 'name': 'risk_off'}, settings) is None


def test_freshness_and_gap_validation():
    s = snapshot()
    s.validate(utc(), 1200)
    with pytest.raises(ValueError, match='Stale'):
        s.validate(s.as_of + 1201, 1200)
    gap = snapshot()
    gap.candles.pop(-3)
    with pytest.raises(ValueError, match='Missing'):
        gap.validate(utc(), 1200)
    incomplete = snapshot(end=utc()+900)
    with pytest.raises(ValueError, match='Incomplete'):
        incomplete.validate(utc(), 1200)


def test_provider_failover_does_not_refresh_stale_cache():
    db = Store(':memory:')
    class Bad:
        name = 'bad'
        async def candles(self, symbol, interval):
            raise ProviderError('unavailable')
    class Good:
        name = 'good'
        async def candles(self, symbol, interval):
            return snapshot(symbol=symbol)
    pool = ProviderPool([Bad(), Good()], db, 1200)
    assert asyncio.run(pool.fetch('BTC')).source == 'binance'
    assert db.state('provider:bad')['status'] == 'unavailable'
    data = snapshot(end=utc()-10000).dict()
    data['fetched_at'] = utc()-10000
    db.set('cache:BTC:900', data)
    pool.providers = [Bad()]
    with pytest.raises(ValueError, match='Stale'):
        asyncio.run(pool.fetch('BTC'))


def test_fact_check_numeric_and_missing_source(settings):
    db = Store(':memory:')
    checker = FactChecker(settings, db)
    p = draft_payload()
    assert checker.check(p) == []
    p['body'] += ' Profit target 999999.'
    assert any('Number' in r for r in checker.check(p))
    p = draft_payload()
    p['body'] = p['body'].replace('binance', 'unknown')
    assert any('source' in r for r in checker.check(p))
    assert any('expired' in r for r in checker.check(draft_payload(), utc()+7200))


def test_semantic_and_logical_duplicates(settings):
    db = Store(':memory:')
    engine = ContentEngine(settings, db)
    ident = asyncio.run(engine.draft(event()))
    with pytest.raises(ValueError, match='Same underlying'):
        asyncio.run(engine.draft(event()))
    assert similarity('BTC volume surging above resistance', 'BTC turnover rising above resistance') > .7
    assert db.get(ident)['status'] == 'review'


def test_scheduler_conflicts_and_priority(settings):
    db = Store(':memory:')
    checker = FactChecker(settings, db)
    # Different manually reviewed campaign-like content, no market expiry.
    a = {**draft_payload(), 'event': None, 'priority': 10}
    b = {**draft_payload(), 'event': None, 'priority': 100}
    ia = db.insert('draft', a, 'approved')
    ib = db.insert('draft', b, 'approved')
    scheduler = Scheduler(settings, db, checker)
    now = utc()
    scheduler.allocate(now)
    assert db.get(ib)['due'] == now
    assert db.get(ia)['due'] >= now + settings.gap_seconds
    scheduler.allocate(now+10)
    assert db.get(ib)['due'] < db.get(ia)['due']


def test_compliance_daily_hourly_and_spacing(settings):
    db = Store(':memory:')
    policy = Compliance(settings, db)
    now = utc()
    p = {**draft_payload(), 'submitted_at': now-100}
    db.insert('draft', p, 'published')
    row = {'id': 'new', 'payload': draft_payload()}
    assert 'spacing' in policy.gate(row, now).lower()
    db.set('policy_caps', {'daily': 1})
    assert 'Daily' in policy.gate(row, now)
    db.set('emergency_stop', True)
    assert 'paused' in policy.gate(row, now)


def test_campaign_parsing_and_expiry(settings):
    p = parse_campaign('Project name: Official campaign\nTerms: Supplied conditions\nStart date: 2026-01-01\nEnd date: 2026-01-02\nRequired hashtags: #Campaign\nRequired number of posts: 3')
    assert p['required'] == ['#Campaign'] and p['quota'] == 3
    assert p['end']-p['start'] == 86400
    with pytest.raises(ValueError):
        parse_campaign('name: absent dates')
    desk = Desk(settings)
    ident = desk.db.insert('campaign', p, 'active')
    draft = desk.db.insert('draft', {**draft_payload(), 'campaign_id': ident}, 'queued')
    desk.campaigns.expire()
    assert desk.db.get(ident)['status'] == 'expired'
    assert desk.db.get(draft)['status'] == 'expired'
    asyncio.run(desk.close())


def test_telegram_auth_rate_limit_and_update_idempotency(settings):
    settings.telegram_admins = (42,)
    settings.telegram_token = 'not-a-real-token'
    settings.webhook_secret = 'test-webhook-secret'
    desk = Desk(settings)
    unauthorized = {'update_id': 1, 'message': {'from': {'id': 43}, 'chat': {'id': 43, 'type': 'private'}, 'text': '/pause'}}
    asyncio.run(desk.telegram.receive(unauthorized))
    assert not desk.db.state('paused', False)
    authorized = {'update_id': 2, 'message': {'from': {'id': 42}, 'chat': {'id': 42, 'type': 'private'}, 'text': '/pause'}}
    asyncio.run(desk.telegram.receive(authorized))
    desk.db.set('paused', False)
    asyncio.run(desk.telegram.receive(authorized))
    assert not desk.db.state('paused')
    assert len(desk.db.list('command')) == 1
    assert not desk.telegram.authorized({'from': {'id': 42}, 'chat': {'id': -1, 'type': 'group'}})
    assert 'not-a-real-token' not in json.dumps(desk.db.logs())
    asyncio.run(desk.close())


def test_paper_publish_and_restart_deduplication(settings):
    desk = Desk(settings)
    ident = desk.db.insert('draft', draft_payload(), 'queued', due=utc())
    asyncio.run(desk.publisher.publish(ident))
    row = desk.db.get(ident)
    assert row['status'] == 'paper_published'
    assert Path(settings.artifacts, ident+'.txt').is_file()
    asyncio.run(desk.publisher.publish(ident))
    assert len(desk.db.list('draft')) == 1
    uncertain = desk.db.insert('draft', draft_payload(), 'sending')
    asyncio.run(desk.close())
    restarted = Desk(settings)
    restarted.db.recover()
    assert restarted.db.get(ident)['status'] == 'paper_published'
    assert restarted.db.get(uncertain)['status'] == 'uncertain'
    asyncio.run(restarted.close())


def test_square_timeout_has_no_retry():
    calls = []
    async def handler(request):
        calls.append(request)
        return httpx.Response(504)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = SquarePublisher('secret-test-key', client)
    with pytest.raises(AmbiguousPublication):
        asyncio.run(adapter.send(draft_payload()))
    assert len(calls) == 1
    assert calls[0].headers['X-Square-OpenAPI-Key'] == 'secret-test-key'
    asyncio.run(client.aclose())


def test_publishing_restriction_and_cooldown(settings):
    settings.paper_mode, settings.live_enabled = False, True
    settings.square_key = 'test-square-key'
    settings.policy_reviewed = time.strftime('%Y-%m-%d', time.gmtime())
    async def handler(request):
        return httpx.Response(429, headers={'Retry-After': '7200'})
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    desk = Desk(settings, client=client)
    ident = desk.db.insert('draft', draft_payload(), 'queued')
    asyncio.run(desk.publisher.publish(ident))
    assert desk.db.get(ident)['status'] == 'review'
    assert desk.db.state('publish_retry_after') > utc()+7000
    assert desk.publisher.policy.caps()['daily'] < settings.daily_target
    asyncio.run(desk.close())


def test_paper_signal_stop_first_and_fees(settings):
    db = Store(':memory:')
    base = snapshot()
    end = base.as_of
    signal = {'id': 'test', 'symbol': 'BTC', 'source': 'binance', 'quote': 'USDT', 'type': 'breakout_retest',
              'direction': 'long', 'entry': 100, 'stop': 90, 'target1': 120, 'target2': 130,
              'context': {'name': 'range'}, 'as_of': end, 'last_bar': end, 'expires_at': end+10000,
              'triggered_at': None, 'mfe_r': 0, 'mae_r': 0, 'result_r': None, 'risk_reward': 2,
              'target1_reached': False}
    record_setup(db, signal)
    observed = Snapshot('BTC', 'USDT', 'binance', 900, end+1800, [
        Candle(end, end+900, 101, 110, 95, 105, 100),
        Candle(end+900, end+1800, 105, 135, 85, 100, 100)])
    advance(db, observed, settings)
    row = db.get('test')
    assert row['status'] == 'stopped' and row['payload']['result_r'] < -1
    assert not row['payload']['target1_reached']
    result = statistics([row])
    assert result['win_rate'] == 0 and result['max_drawdown_r'] > 1
    assert statistics([])['win_rate'] is None


def test_persistent_budgets_and_lease(tmp_path):
    path = str(tmp_path / 'desk.db')
    db = Store(path)
    assert db.reserve_budget('2026-10-01', 'tokens', 5, 10)
    assert not db.reserve_budget('2026-10-01', 'tokens', 6, 10)
    assert db.acquire_lease('first')
    second = Store(path)
    assert not second.acquire_lease('second')
    db.close()
    restarted = Store(path)
    assert restarted.reserve_budget('2026-10-01', 'tokens', 5, 10)
    assert not restarted.reserve_budget('2026-10-01', 'tokens', 1, 10)
    second.close()
    restarted.close()


def test_health_auth_webhook_and_injection(settings):
    settings.telegram_token = 'private-token'
    settings.telegram_admins = (42,)
    settings.webhook_secret = 'secret-webhook'
    desk = Desk(settings)
    app = create_app(settings, desk, start_worker=False)
    with TestClient(app) as client:
        assert client.get('/health').status_code == 200
        assert client.get('/health/details').status_code == 401
        assert client.get('/api/overview').status_code == 401
        auth = ('desk', settings.admin_token)
        assert client.get('/', auth=auth).status_code == 200
        overview = client.get('/api/overview', auth=auth)
        assert settings.telegram_token not in overview.text and settings.webhook_secret not in overview.text
        assert client.post('/telegram/webhook', json={'update_id': 1}).status_code == 403
        assert client.post('/api/command', json={'command':'/pause'}, auth=auth, headers={'Origin':'https://evil.example'}).status_code == 403
        assert client.post('/api/command', json={'command':'/pause'}, auth=auth).status_code == 200
        assert client.get('/artifacts/secret.env', auth=auth).status_code == 400
        assert client.get('/dashboard.js', auth=auth).headers['content-security-policy']


def test_images_real_data_and_edit_resets_approval(settings):
    from PIL import Image
    desk = Desk(settings)
    s = snapshot()
    desk.db.set('cache:BTC:900', s.dict())
    ident = desk.db.insert('draft', draft_payload(event(s)), 'approved')
    filename = desk.make_image(ident)
    row = desk.db.get(ident)
    assert row['status'] == 'review' and row['payload']['approved_at'] is None
    with Image.open(Path(settings.artifacts, filename)) as image:
        assert image.size == (1200, 800)
        image.verify()
    desk.review(ident, True)
    desk.edit(ident, row['payload']['title'], row['payload']['body'])
    assert desk.db.get(ident)['status'] == 'review'
    asyncio.run(desk.close())


def test_metrics_missing_values_and_paper_separation(settings):
    desk = Desk(settings)
    report = desk.analytics.report()
    assert report['followers'] is None and report['follower_growth'] is None
    desk.analytics.import_metrics({'followers': 30000, 'as_of': utc()-86400})
    desk.analytics.import_metrics({'followers': 30012})
    assert desk.analytics.report()['follower_growth'] == 12
    ident = desk.db.insert('draft', draft_payload(), 'paper_published')
    with pytest.raises(ValueError, match='real publication'):
        desk.analytics.import_metrics({'draft_id': ident, 'impressions': 100})
    asyncio.run(desk.close())


def test_scan_integration_mock_apis(settings):
    settings.symbols = ('BTC', 'ETH')
    desk = Desk(settings)
    class Market:
        async def fetch(self, symbol, interval=900):
            result = snapshot(interval=interval, symbol=symbol)
            # Realistic volume expansion and mover, without claiming live data.
            c = result.candles[-1]
            result.candles[-1] = replace(c, volume=400)
            return result
    desk.pool = Market()
    asyncio.run(desk.scan())
    assert len(desk.db.state('latest_market')) == 2
    assert desk.db.list('draft')
    row = desk.db.list('draft')[0]
    desk.review(row['id'], True)
    desk.scheduler.allocate()
    for due in desk.scheduler.due():
        asyncio.run(desk.publisher.publish(due['id']))
    assert desk.db.list('draft', ['paper_published'])
    asyncio.run(desk.close())


def test_optional_derivatives_freshness_and_units():
    from square_desk.derivatives import BinanceDerivatives
    db = Store(':memory:')
    now = utc()
    class HTTP:
        async def get(self, host, path, params):
            if path.endswith('premiumIndex'):
                return {'time': now*1000, 'markPrice': '110', 'lastFundingRate': '0.0001'}
            if path.endswith('openInterest'):
                return {'time': now*1000, 'openInterest': '1000'}
            return [{'timestamp': (now-3600)*1000, 'sumOpenInterest': '900'},
                    {'timestamp': now*1000, 'sumOpenInterest': '1000'}]
    derivative = BinanceDerivatives(HTTP(), db)
    context = asyncio.run(derivative.context('BTC'))
    assert context['funding_rate'] == .0001
    assert context['open_interest_base_units'] == 1000
    assert context['open_interest_change_1h'] == pytest.approx(100/9)
    assert context['liquidations'] is None


def test_educational_hybrid_and_similarity_limit(settings):
    settings.mode = 'hybrid'
    desk = Desk(settings)
    ident = desk.content.education()
    assert desk.db.get(ident)['status'] == 'approved'
    assert desk.content.checker.check(desk.db.get(ident)['payload']) == []
    made = 1
    while desk.content.education():
        made += 1
        assert made <= 3
    assert made == 3
    asyncio.run(desk.close())


def test_rss_observations_are_not_verified(settings):
    from square_desk.news import NewsMonitor
    settings.news_feeds = ('https://primary.example/rss',)
    db = Store(':memory:')
    xml = '<rss><channel><item><title>Project notice</title><link>https://primary.example/notice</link></item></channel></rss>'
    async def handler(request):
        return httpx.Response(200, text=xml, headers={'ETag':'feed-v1'})
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monitor = NewsMonitor(settings, db, client)
    asyncio.run(monitor.poll())
    rows = db.list('news')
    assert len(rows) == 1 and rows[0]['status'] == 'unconfirmed'
    assert rows[0]['payload']['summary'] == ''
    asyncio.run(monitor.poll())
    assert len(db.list('news')) == 1
    asyncio.run(client.aclose())


def test_environment_names_only_example_uses_defaults(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('DESK_PAPER_MODE', '')
    monkeypatch.setenv('DESK_DAILY_TARGET', '')
    monkeypatch.setenv('DESK_SYMBOLS', '')
    assert Settings.from_env().paper_mode
    assert Settings.from_env().daily_target == 12
    assert Settings.from_env().symbols


def test_missing_paper_history_remains_unresolved(settings):
    db = Store(':memory:')
    s = snapshot()
    row = {'symbol': 'BTC', 'metrics': studies(s), 'changes': changes(s)}
    row['metrics'].update(price=130, resistance=125, ema20=120, ema50=115, rsi=60, relative_volume=3, quote_volume_24h=1e8)
    signal = setup(row, {'name': 'risk_on', 'btc_1h': 2, 'eth_1h': 2}, settings)
    record_setup(db, signal)
    later = snapshot(end=s.as_of+1000000)
    advance(db, later, settings)
    observed = db.get(signal['id'])
    assert observed['status'] == 'expired_unresolved'
    assert observed['payload']['result_r'] is None
