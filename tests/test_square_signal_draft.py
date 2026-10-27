import asyncio
from dataclasses import replace
import json

import httpx
import pytest

from square_desk.config import Settings
from square_desk.content import signal_text_errors
from square_desk.models import stamp, utc
from square_desk.service import Desk
from test_square_desk import snapshot


class Writer:
    def __init__(self):
        self.calls = []

    async def generate(self, evidence, article=False):
        self.calls.append(evidence)
        f = evidence['facts']
        body = (f"${evidence['symbol']} potential {evidence['signal_setup']['direction']} scenario. "
            f"Reference entry {f['entry']}; invalidation stop {f['stop']}; first target {f['target1']}. "
            f"Screening score {f['confidence']}. The screening score is heuristic, not a calibrated probability. "
            + 'Market ' * 110 + f" Source {evidence['source']} at {evidence['timestamp']}. "
            'Probabilistic market research, not a recommendation or guaranteed return.')
        return {'title': '$SOL conditional analysis', 'body': body}


def make_desk(tmp_path, monkeypatch, **overrides):
    s = replace(Settings(database=':memory:', artifacts=str(tmp_path), publishing_hours=tuple(range(24))), **overrides)
    desk = Desk(s)
    writer = Writer()
    desk.content.ai = writer
    snap = snapshot(symbol='SOL')
    signal = {'symbol': 'SOL', 'direction': 'long', 'type': 'breakout_retest',
        'source': 'binance', 'quote': 'USDT', 'as_of': snap.as_of, 'expires_at': utc() + 14400,
        'last_bar': snap.as_of, 'entry': 108, 'stop': 98, 'target1': 128, 'target2': 138,
        'confidence': 80, 'mfe_r': 0, 'mae_r': 0}
    ident = desk.db.insert('signal', signal, 'watching')
    async def fetch(symbol, interval=900):
        return snapshot(symbol=symbol)
    desk.pool.fetch = fetch
    fresh = {'direction': 'long', 'type': 'breakout_retest', 'entry': 110, 'stop': 100,
             'target1': 130, 'target2': 140, 'confidence': 80, 'risk_reward': 2}
    monkeypatch.setattr('square_desk.service.setup', lambda *args: fresh)
    return desk, writer, ident


def test_signal_becomes_separate_review_draft_and_only_publishes_after_approval(tmp_path, monkeypatch):
    async def run():
        desk, writer, signal_id = make_desk(tmp_path, monkeypatch, mode='automatic', ai_auto_publish=True)
        reply = await desk.telegram.command('/signal_draft ' + signal_id)
        drafts = desk.db.list('draft')
        assert len(drafts) == 1
        draft = drafts[0]
        ident, p = draft['id'], draft['payload']
        assert ident != signal_id and draft['status'] == 'review'
        assert p['risk'] == 'high' and p['human_review_required'] and p['approved_at'] is None
        assert p['event']['signal_id'] == signal_id
        assert p['event']['metrics']['entry'] == 110  # Fresh levels, not original 108.
        assert desk.db.get(signal_id)['payload']['entry'] == 108  # Paper history preserved.
        assert '/approve ' + ident in reply and '/preview ' + ident in reply
        assert writer.calls[0]['signal_setup']['classification'] == 'potential_setup'
        assert 'signal_id' not in writer.calls[0]  # Record IDs are not market claims.
        desk.scheduler.allocate()
        assert desk.scheduler.due() == []
        with pytest.raises(ValueError, match='Paper signal ID is not a draft ID'):
            await desk.telegram.command('/approve ' + signal_id)
        preview = await desk.telegram.command('/preview ' + signal_id)
        assert '/preview ' + ident in preview
        assert await desk.create_for_signal(signal_id) == ident
        assert len(writer.calls) == 1
        await desk.telegram.command('/approve ' + ident)
        desk.scheduler.allocate()
        assert desk.db.get(ident)['status'] == 'queued'
        await desk.publisher.publish(ident)
        assert desk.db.get(ident)['status'] == 'paper_published'
        with pytest.raises(ValueError, match='Same underlying event already covered'):
            await desk.create_for_signal(signal_id)
        await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('status', ['triggered', 'invalidated', 'stopped', 'target1', 'expired_unresolved'])
def test_inactive_signal_is_rejected_before_market_or_ai_calls(status, tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        desk.db.update(ident, status=status)
        async def forbidden(*args):
            pytest.fail('Inactive signal fetched data')
        desk.pool.fetch = forbidden
        with pytest.raises(ValueError, match='inactive or stale'):
            await desk.create_for_signal(ident)
        assert not writer.calls and not desk.db.list('draft')
        await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['stale_candles', 'expired', 'wrong_source', 'no_setup', 'changed_direction', 'no_ai'])
def test_invalid_evidence_does_not_create_signal_draft(failure, tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        row = desk.db.get(ident)
        p = row['payload']
        if failure == 'stale_candles':
            async def stale_fetch(symbol, interval=900):
                current = snapshot(symbol=symbol)
                return replace(current, candles=[replace(c, start=c.start-7200, end=c.end-7200) for c in current.candles])
            desk.pool.fetch = stale_fetch
        if failure == 'expired':
            p['expires_at'] = utc() - 1
        if failure == 'wrong_source':
            p['source'] = 'coinbase'
        desk.db.update(ident, payload=p)
        if failure == 'no_setup':
            monkeypatch.setattr('square_desk.service.setup', lambda *args: None)
        if failure == 'changed_direction':
            monkeypatch.setattr('square_desk.service.setup', lambda *args: {'direction': 'short', 'type': 'breakdown_retest'})
        if failure == 'no_ai':
            desk.content.ai = None
        with pytest.raises(ValueError):
            await desk.create_for_signal(ident)
        assert not writer.calls and not desk.db.list('draft')
        await desk.close()
    asyncio.run(run())


def test_invalidation_after_approval_still_blocks_publication(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        draft_id = await desk.create_for_signal(ident)
        desk.review(draft_id, True)
        desk.scheduler.allocate()
        assert desk.db.get(draft_id)['status'] == 'queued'
        desk.db.update(ident, status='invalidated')
        await desk.publisher.publish(draft_id)
        assert desk.db.get(draft_id)['status'] != 'paper_published'
        assert not desk.db.get(draft_id)['payload'].get('receipt')
        await desk.close()
    asyncio.run(run())


def test_signal_command_uses_background_job_slot(tmp_path, monkeypatch):
    async def run():
        sent = []
        desk, writer, ident = make_desk(tmp_path, monkeypatch,
            telegram_token='test-token', telegram_admins=(7,), webhook_secret='test-secret')
        async def api(method, payload):
            sent.append(payload.get('text', ''))
            return {'message_id': len(sent)}
        desk.telegram.api = api
        await desk.telegram.receive({'update_id': 731, 'message': {'from': {'id': 7},
            'chat': {'id': 7, 'type': 'private'}, 'text': '/signal_draft ' + ident}})
        assert desk.telegram.job_task is not None
        await desk.telegram.job_task
        assert any('Command received' in text for text in sent)
        assert any('AI draft' in text and '/approve ' in text for text in sent)
        await desk.close()
    asyncio.run(run())


def test_signal_text_requires_levels_direction_and_score_disclosure():
    values = {'entry': '110', 'stop': '100', 'target1': '130', 'confidence': '80'}
    assert signal_text_errors('SOL', 'Generic research', values, 'long')
    assert signal_text_errors('SOL', 'long 110 100 130 80', values, 'long') == ['Signal draft missing heuristic score disclosure']


def test_signal_validation_failure_tries_next_model_with_same_numeric_evidence(tmp_path, monkeypatch):
    from square_desk.ai_router import AIRouter
    from square_desk.store import Store
    async def sleep(seconds):
        pass
    monkeypatch.setattr('square_desk.ai_router.asyncio.sleep', sleep)
    async def run():
        calls = []
        def respond(request):
            request_body = json.loads(request.content)
            calls.append(request_body['model'])
            assert 'signal_setup' in request_body['messages'][1]['content']
            assert 'heuristic, not a calibrated probability' in request_body['messages'][0]['content']
            body = 'Market ' * 110 + ' {{ticker}} {{source}} {{timestamp}} '
            if len(calls) > 1:
                body += ('potential long entry {{entry}}, stop {{stop}}, first target {{target1}}, '
                    'score {{confidence}}. The screening score is heuristic, not a calibrated probability.')
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop',
                'message': {'content': json.dumps({'title': '{{ticker}} potential setup', 'body': body})}}],
                'usage': {'total_tokens': 500}})
        db = Store(':memory:')
        s = replace(Settings(database=':memory:', artifacts=str(tmp_path)), openrouter_api_key='key',
            openrouter_models=('google/gemma-4-31b-it:free', 'google/gemma-4-26b-a4b-it:free'),
            ai_provider_order=('openrouter',), ai_daily_requests=50, ai_daily_tokens=100000)
        s.ai_evidence_footer = False  # Test model-written levels without the deterministic evidence block.
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            router = AIRouter(s, db, client)
            result = await router.generate({'facts': {'entry': '110', 'stop': '100', 'target1': '130', 'confidence': '80'},
                'symbol': 'SOL', 'source': 'binance', 'timestamp': stamp(), 'signal_setup': {'direction': 'long'}})
            assert len(calls) == 2 and result['ai_model'] == calls[1]
            assert 'entry 110' in result['body']
            assert router.status()['model_health']['openrouter'][calls[0]]['reason'] == 'Signal draft missing setup levels or direction'
        db.close()
    asyncio.run(run())


def test_signal_invalidated_while_ai_runs_is_not_saved(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        original = writer.generate
        async def invalidate(evidence, article=False):
            result = await original(evidence, article)
            desk.db.update(ident, status='invalidated')
            return result
        writer.generate = invalidate
        with pytest.raises(ValueError, match='Signal is inactive or stale'):
            await desk.create_for_signal(ident)
        assert not desk.db.list('draft')
        await desk.close()
    asyncio.run(run())
