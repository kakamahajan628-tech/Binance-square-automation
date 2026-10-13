"""Generation failures, provenance and cancellation without live API calls."""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json

import httpx
import pytest

from square_desk.ai_router import AIRouter, AI_JOB, GenerationFailed
from square_desk.config import Settings
from square_desk.content import CompatibleAI, evidence_footer, normalize_numeric_formatting
from square_desk.content import FactChecker
from square_desk.models import stamp, utc
from square_desk.store import Store
from test_square_signal_draft import make_desk
from test_square_telegram_responsiveness import update


def evidence(signal=False):
    result = {'symbol': 'SOL', 'source': 'binance', 'timestamp': stamp(),
              'facts': {'price': '110', 'entry': '110', 'stop': '100', 'target1': '130', 'confidence': '80'}}
    if signal:
        result['signal_setup'] = {'direction': 'long'}
    return result


def completion(body, title='{{ticker}} research'):
    return {'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({'title': title, 'body': body})}}],
            'usage': {'total_tokens': 500}}


def test_exact_binding_is_label_specific_and_never_rounds():
    data = {'facts': {'price': '123.45'}}
    assert normalize_numeric_formatting({'title': 'Research', 'body': 'Price 123.45.\n1. Risk'}, data)['body'] == 'Price {{price}}.\n- Risk'
    for body in ('Price 123.4', 'Stop 123.45', 'Price 123.450', 'Price 123.45e5', 'Price 123.45.7'):
        assert '{{price}}' not in normalize_numeric_formatting({'title': 'Research', 'body': body}, data)['body']


def test_signal_facts_are_inserted_from_recorded_evidence(tmp_path):
    async def run():
        ev = evidence(True)
        def respond(request):
            return httpx.Response(200, json=completion('Conditional market structure deserves careful interpretation. ' * 18))
        db = Store(':memory:')
        s = Settings(database=':memory:', artifacts=str(tmp_path), ai_key='test',
                     ai_url='https://api.groq.com/openai/v1/chat/completions', ai_model='openai/gpt-oss-120b')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await AIRouter(s, db, client).generate(ev)
            assert evidence_footer(ev) in result['body']
            assert 'Reference entry: 110' in result['body'] and 'Invalidation stop: 100' in result['body']
            assert 100 <= len(result['body'].split()) <= 200
            assert db.state('ai_last_job')['status'] == 'validated'
        db.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure', [None, 'third_section', 'repetition'])
def test_article_sections_must_all_succeed_and_be_distinct(tmp_path, failure):
    async def run():
        calls = []
        # Separate vocabularies make accidental repetition detectable.
        sections = ['Liquidity spread execution slippage depth fills. ',
                    'Momentum range trend volatility context observations. ',
                    'Invalidation uncertainty scenario limitations research assumptions. ']
        def respond(request):
            payload = json.loads(request.content)
            calls.append(payload)
            index = len(calls) - 1
            assert 'article_section' in payload['messages'][1]['content']
            if failure == 'third_section' and index == 2:
                return httpx.Response(503)
            body = sections[0 if failure == 'repetition' else index] * 70
            return httpx.Response(200, json=completion(body))
        db = Store(':memory:')
        s = Settings(database=':memory:', artifacts=str(tmp_path), ai_key='test',
                     ai_url='https://example.com/chat/completions', ai_model='test', ai_daily_tokens=200000)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            writer = CompatibleAI(s, db, client)
            if failure:
                with pytest.raises(ValueError, match='service rejected|sections repeat'):
                    await writer.generate(evidence(), article=True)
                assert not db.list('draft')
            else:
                result = await writer.generate(evidence(), article=True)
                assert len(calls) == 3
                assert 1000 <= len(result['body'].split()) <= 2000
                assert result['body'].count('Recorded evidence for $SOL') == 1
                assert all(section.strip() in result['body'] for section in sections)
        db.close()
    asyncio.run(run())


def test_fair_failover_preserves_per_command_http_diagnostics(tmp_path, monkeypatch):
    async def sleep(seconds):
        pass
    monkeypatch.setattr('square_desk.ai_router.asyncio.sleep', sleep)
    async def run():
        calls = []
        def respond(request):
            calls.append(request.url.host)
            return httpx.Response(429, json={'error': {'message': 'daily quota private-secret'}})
        db = Store(':memory:')
        s = Settings(database=':memory:', artifacts=str(tmp_path), ai_key='test',
            ai_url='https://api.groq.com/openai/v1/chat/completions', ai_model='openai/gpt-oss-120b',
            groq_models=('openai/gpt-oss-120b', 'openai/gpt-oss-20b'), google_api_key='test',
            openrouter_api_key='test', ai_provider_order=('groq', 'google', 'openrouter'))
        job = {'id': 'manual-job', 'command': '/signal_draft'}
        token = AI_JOB.set(job)
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                router = AIRouter(s, db, client)
                with pytest.raises(GenerationFailed) as failed:
                    await router.generate(evidence(True))
                assert calls[:3] == ['api.groq.com', 'generativelanguage.googleapis.com', 'openrouter.ai']
                report = failed.value.report
                assert report['id'] == 'manual-job' and report['attempts'][0]['http_status'] == 429
                assert 'private-secret' not in json.dumps(router.status())
                assert db.state('ai_last_manual_job')['status'] == 'failed'
                assert not router.lock.locked() and not db.state('ai_active_generation')
        finally:
            AI_JOB.reset(token)
            db.close()
    asyncio.run(run())


def test_explicit_unsupported_format_gets_one_compatible_retry(tmp_path):
    async def run():
        calls = []
        def respond(request):
            payload = json.loads(request.content)
            calls.append(payload)
            if len(calls) == 1:
                return httpx.Response(400, json={'error': {'param': 'response_format', 'message': 'private-key'}})
            assert 'response_format' not in payload
            return httpx.Response(200, json=completion('Recorded structure and liquidity require careful observation. ' * 16))
        db = Store(':memory:')
        s = Settings(database=':memory:', artifacts=str(tmp_path), ai_key='test',
                     ai_url='https://example.com/completions', ai_model='test')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            assert evidence_footer(evidence()).splitlines()[0] in (await CompatibleAI(s, db, client).generate(evidence()))['body']
            assert len(calls) == 2
        db.close()
    asyncio.run(run())


@pytest.mark.parametrize('sent', [False, True])
def test_cancellation_refunds_only_requests_not_sent(tmp_path, sent):
    async def run():
        started = asyncio.Event()
        class Client:
            async def post(self, *args, **kwargs):
                started.set()
                await asyncio.Event().wait()
        db = Store(':memory:')
        s = Settings(database=':memory:', artifacts=str(tmp_path), ai_key='test',
                     ai_url='https://api.groq.com/openai/v1/chat/completions', ai_model='openai/gpt-oss-120b')
        writer = CompatibleAI(s, db, Client())
        if not sent:
            await writer.request_lock.acquire()
        task = asyncio.create_task(writer.generate(evidence()))
        if sent:
            await started.wait()
        else:
            await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        day = datetime.now(timezone.utc).date().isoformat()
        assert db.budget_used(day, 'ai_requests') == int(sent)
        assert bool(db.budget_used(day, 'ai_tokens')) == sent
        db.close()
    asyncio.run(run())


def test_old_watching_signal_revalidates_without_rewriting_history(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        p = desk.db.get(ident)['payload']
        old = utc() - desk.s.draft_max_age - 300
        p['as_of'] = old
        desk.db.update(ident, payload=p)
        draft_id = await desk.create_for_signal(ident)
        assert desk.db.get(ident)['payload']['as_of'] == old
        assert desk.db.get(draft_id)['payload']['event']['as_of'] > old
        assert len(writer.calls) == 1
        await desk.close()
    asyncio.run(run())


def test_total_command_timeout_cancels_job_and_frees_slot(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch,
            telegram_token='test', telegram_admins=(7,), webhook_secret='test')
        desk.s.ai_command_timeout_seconds = .03
        desk.s.ai_progress_seconds = .01
        cancelled = asyncio.Event()
        async def wait(signal_id):
            AI_JOB.get()['stage'] = 'fresh_signal_market_data'
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        async def api(*args):
            return {'message_id': 1}
        desk.create_for_signal = wait
        desk.telegram.api = api
        await desk.telegram.receive(update(901, '/signal_draft ' + ident))
        await desk.telegram.job_task
        assert cancelled.is_set()
        assert desk.db.state('command_last_job')['status'] == 'timeout'
        assert not desk.db.state('command_active_job')
        assert not desk.db.list('draft')
        await desk.close()
    asyncio.run(run())


def test_existing_publication_latch_reports_code_without_clearing_it(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        draft_id = await desk.create_for_signal(ident)
        row = desk.db.get(draft_id)
        desk.db.update(draft_id, payload={**row['payload'], 'rejection_code': '403'})
        desk.db.set('publisher_blocked', True)
        report = json.loads(await desk.telegram.command('/publish_status'))
        assert report['blocked'] and report['reason']['code'] == '403'
        assert report['reason']['draft_id'] == draft_id
        assert desk.db.state('publisher_blocked')
        await desk.close()
    asyncio.run(run())


def test_ai_job_does_not_attach_an_older_commands_generation(tmp_path, monkeypatch):
    async def run():
        desk, writer, ident = make_desk(tmp_path, monkeypatch)
        desk.db.set('command_last_job', {'id': 'current', 'status': 'failed', 'stage': 'fresh_market_data'})
        desk.db.set('ai_last_manual_job', {'id': 'old', 'status': 'validated'})
        reply = json.loads(await desk.telegram.command('/ai_job'))
        assert reply['command']['id'] == 'current' and reply['generation'] == {}
        await desk.close()
    asyncio.run(run())


def test_duplicate_detection_ignores_shared_evidence_footer_but_keeps_prose_guard(tmp_path):
    db = Store(':memory:')
    s = Settings(database=':memory:', artifacts=str(tmp_path))
    footer = 'Recorded evidence for $SOL: ' + 'source timestamp entry stop target screening score ' * 40
    first = {'title': 'Market research', 'body': 'Liquidity spreads depth fills execution friction. ' * 20 + '\n\n' + footer,
             'recorded_evidence_footer': footer}
    db.insert('draft', first, 'review')
    distinct = {**first, 'body': 'Structure trend volatility observations range interpretation. ' * 20 + '\n\n' + footer}
    checker = FactChecker(s, db)
    assert not checker.duplicate(distinct)
    assert checker.duplicate(first) == 'Draft too similar to recent content'
    db.close()
