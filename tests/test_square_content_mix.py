import asyncio
from copy import deepcopy
from dataclasses import replace
import json
import re

import httpx
import pytest

from square_desk.config import Settings
from square_desk.content import ContentEngine, CompatibleAI, FactChecker, facts, evidence_footer, text_units
from square_desk import content_mix
from square_desk.education import LESSONS, educational_payload
from square_desk.models import utc, stamp
from square_desk.store import Store
from square_desk.ai_router import AIRouter
from test_square_desk import event, snapshot


def setup_context(tmp_path):
    settings = Settings(database=':memory:', artifacts=str(tmp_path), symbols=('ZEC', 'BTC', 'ETH', 'SOL'))
    db = Store(':memory:')
    main = event(snapshot(symbol='ZEC'))
    rows = []
    for symbol, move in [('BTC', .5), ('ETH', -.2), ('SOL', 7)]:
        peer = event(snapshot(symbol=symbol))
        rows.append({'symbol': symbol, 'metrics': peer['metrics'], 'changes': {'1h': move, '24h': move}})
    db.set('latest_market', rows)
    return settings, db, main


def evidence_for(settings, db, main):
    brief = content_mix.plan(settings, db, main)
    return {'facts': facts(main), 'symbol': main['symbol'], 'source': main['metrics']['source'],
            'timestamp': stamp(main['as_of']), 'quote': main['metrics']['quote'],
            'editorial_brief': brief,
            'tickers': {f'peer{i}_ticker': '$' + peer['symbol'] for i, peer in enumerate(brief['related'], 1)}}


def test_twenty_content_slots_have_eight_angles_and_quarter_verified_multi_coin_posts(tmp_path):
    settings, db, main = setup_context(tmp_path)
    styles, visible = [], []
    for _ in range(20):
        brief = content_mix.plan(settings, db, main)
        styles.append(brief['style'])
        visible.append(brief['public_proof'])
        if brief['style'] == 'multi_coin':
            assert len(brief['related']) == 2
            assert len({p['symbol'] for p in brief['related']}) == 2
            assert '$BTC' in brief['suffix'] and '$SOL' in brief['suffix']
            assert 'does not establish correlation' in brief['suffix']
        content_mix.commit_progress(db, brief)
    assert len(set(styles)) == 8
    assert styles.count('multi_coin') == 5
    assert sum(visible) == 2 and visible[9] and visible[19]


@pytest.mark.parametrize('problem', ['stale', 'future', 'venue', 'quote', 'unlisted'])
def test_multi_coin_slot_falls_back_without_inventing_or_fetching_peer_data(tmp_path, problem):
    settings, db, main = setup_context(tmp_path)
    rows = db.state('latest_market')
    for row in rows:
        if problem == 'stale':
            row['metrics']['as_of'] = utc() - 9999
        elif problem == 'future':
            row['metrics']['as_of'] = utc() + 100
        elif problem == 'venue':
            row['metrics']['source'] = 'coinbase'
        elif problem == 'quote':
            row['metrics']['quote'] = 'USD'
        else:
            row['symbol'] = 'UNLISTED'
    db.set('latest_market', rows)
    db.set('content_mix_progress', {'mix_slots': 2})
    brief = content_mix.plan(settings, db, main)
    assert brief['style'] == 'psychology'
    assert brief['related'] == []
    assert not re.search(r'\$(BTC|ETH|SOL)', brief['suffix'])


def test_proof_quota_carries_to_binance_without_relabelling_coinbase(tmp_path):
    settings, db, main = setup_context(tmp_path)
    db.set('content_mix_progress', {'market_drafts': 9, 'proof_shown': 0})
    main['metrics']['source'] = 'coinbase'
    brief = content_mix.plan(settings, db, main)
    assert not brief['public_proof']
    content_mix.commit_progress(db, brief)
    main['metrics']['source'] = 'binance'
    brief = content_mix.plan(settings, db, main)
    assert brief['public_proof']
    ev = evidence_for(settings, db, main)
    assert 'Source: binance' in evidence_footer(ev)
    assert 'coinbase' not in evidence_footer(ev)


def test_recent_titles_do_not_repeat_when_coin_name_changes(tmp_path):
    settings, db, main = setup_context(tmp_path)
    used = set()
    for i in range(360):
        main['symbol'] = ('BTC', 'ETH', 'SOL', 'ZEC')[i % 4]
        brief = content_mix.plan(settings, db, main)
        key = content_mix.title_key(brief['title'])
        assert key not in used
        used.add(key)
        db.insert('draft', {'title': brief['title'], 'body': 'A different complete explanation'}, 'published')
        content_mix.commit_progress(db, brief)


def test_hidden_metadata_preserves_internal_freshness_numeric_and_signature_checks(tmp_path):
    settings, db, main = setup_context(tmp_path)
    class Writer:
        async def generate(self, evidence, article=False):
            return {'title': 'A Clue Worth Questioning', 'body': educational_payload(7)['body'] +
                    f"\n\nSource: binance spot candles. Data timestamp: {evidence['timestamp']}."}
    engine = ContentEngine(settings, db, Writer())
    ident = asyncio.run(engine.draft(main))
    p = db.get(ident)['payload']
    assert p['content_mix_version'] == content_mix.REVISION
    assert not p['public_evidence']
    assert 'binance' not in p['body'].lower() and stamp(main['as_of']) not in p['body']
    assert p['internal_evidence']['source'] == 'binance'
    assert p['internal_evidence']['timestamp'] == stamp(main['as_of'])
    assert engine.checker.check(p) == []
    assert db.get(ident)['status'] == 'review'  # Existing mode/risk gates unchanged.
    altered = deepcopy(p)
    altered['body'] += ' Price 999999.'
    assert 'Number is not bound to recorded evidence' in engine.checker.check(altered)
    altered = deepcopy(p)
    altered['internal_evidence']['source'] = 'coinbase'
    assert 'Missing source or timestamp' in engine.checker.check(altered)
    assert 'Market evidence expired or future dated' in engine.checker.check(p, main['as_of'] + 9999)


def test_provider_prompt_and_render_keep_existing_request_loop_and_public_length(tmp_path):
    async def run():
        settings, db, main = setup_context(tmp_path)
        settings.ai_url, settings.ai_key, settings.ai_model = 'https://test.example/chat', 'private-key', 'test-model'
        requests = []
        async def respond(request):
            sent = json.loads(request.content)
            requests.append(sent)
            system, data = sent['messages'][0]['content'], json.loads(sent['messages'][1]['content'])
            assert 'market_detective' == data['editorial_brief']['style']
            assert 'price prophecy' in system and 'Do not insert source names' in system
            assert '_prior_title_keys' not in data['editorial_brief']
            low, high = map(int, re.search(r'BODY contains (\d+)–(\d+)', system).groups())
            body = educational_payload(3)['body']
            assert low <= len(body.split()) <= high
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
                'title': 'Volume-Driven Outlook for ZEC', 'body': body})}}], 'usage': {'total_tokens': 500}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            writer = CompatibleAI(settings, db, client)
            engine = ContentEngine(settings, db, writer)
            ident = await engine.draft(main)
            p = db.get(ident)['payload']
            assert len(requests) == 1  # No added AI requests, retries or network lookups.
            assert 'Volume-Driven' not in p['title']
            assert p['body'].endswith('#ZEC')
            assert '?' in p['body'] and '\u2014' not in p['body']
            assert 'audit:' not in p['body'] and 'binance' not in p['body'].lower()
            assert 100 <= len(p['body'].split()) <= 200
            assert text_units(p['title'] + '\n\n' + p['body']) <= 2100
            assert engine.checker.check(p) == []
    asyncio.run(run())


def test_transport_result_retains_source_for_unchanged_router_validation(tmp_path):
    async def run():
        settings, db, main = setup_context(tmp_path)
        settings.ai_url, settings.ai_key, settings.ai_model = 'https://test.example/chat', 'key', 'model'
        async def respond(request):
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
                'title': 'An Interesting Question', 'body': educational_payload(3)['body']})}}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await CompatibleAI(settings, db, client).generate(evidence_for(settings, db, main))
            assert 'binance' in result['body'] and stamp(main['as_of']) in result['body']
            assert '[audit:' in result['body']
            assert not FactChecker(settings, db).check(result)
    asyncio.run(run())


def test_cosmetic_questions_tags_and_title_changes_cannot_bypass_duplicate_guard(tmp_path):
    settings, db, main = setup_context(tmp_path)
    brief = content_mix.plan(settings, db, main)
    base = educational_payload(3)
    first = {**base, 'title': 'A researched explanation', 'body': base['body'] + '\n\n' + brief['suffix'],
             'presentation_suffix': brief['suffix']}
    db.insert('draft', first, 'published')
    second = {**base, 'title': 'A fresh-looking explanation', 'body': base['body'] + '\n\nDifferent question? #BTC',
              'presentation_suffix': 'Different question? #BTC'}
    assert FactChecker(settings, db).duplicate(second) == 'Draft too similar to recent content'


@pytest.mark.parametrize('bounds', [(100, 200), (200, 400), (300, 400)])
def test_evergreen_topics_are_deeper_and_fit_existing_word_limits(tmp_path, bounds):
    settings = Settings(database=':memory:', artifacts=str(tmp_path), post_min_words=bounds[0], post_max_words=bounds[1])
    checker = FactChecker(settings, Store(':memory:'))
    assert len(LESSONS) > 3
    valid = []
    for i in range(len(LESSONS)):
        p = educational_payload(i, settings)
        if not checker.check(p):
            valid.append(p)
    assert len(valid) >= 8
    assert all('?' in p['body'] and '#CryptoLearning' in p['body'] for p in valid)
    assert all('\u2014' not in p['title'] + p['body'] for p in valid)


def test_signal_content_keeps_required_levels_disclosure_and_high_risk_review(tmp_path):
    settings, db, main = setup_context(tmp_path)
    main['category'], main['direction'] = 'setup', 'long'
    main['metrics'].update(entry=110, stop=100, target1=130, target2=140, confidence=80)
    engine = ContentEngine(settings, db)
    ident = asyncio.run(engine.draft(main))
    p = db.get(ident)['payload']
    assert p['risk'] == 'high' and db.get(ident)['status'] == 'review'
    assert p['content_style'] == 'signal_review'
    assert all(str(n) in p['body'] for n in (110, 100, 130, 140, 80))
    assert 'heuristic' in p['body'] and 'not a calibrated probability' in p['body']
    assert not p['internal_evidence']['related']


@pytest.mark.parametrize('slot', [0, 2, 8, 9])
@pytest.mark.parametrize('footer_enabled', [True, False])
def test_actual_router_accepts_new_presentation_without_changing_provider_or_request_count(tmp_path, slot, footer_enabled):
    async def run():
        settings, db, main = setup_context(tmp_path)
        settings.ai_url, settings.ai_key, settings.ai_model = 'https://api.groq.com/openai/v1/chat/completions', 'key', 'test-model'
        settings.ai_provider_order = ('groq',)
        settings.ai_evidence_footer = footer_enabled
        db.set('content_mix_progress', {'mix_slots': slot, 'market_drafts': slot})
        requests = []
        def respond(request):
            sent = json.loads(request.content)
            requests.append(sent)
            low, high = map(int, re.search(r'BODY contains (\d+)–(\d+)', sent['messages'][0]['content']).groups())
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
                'title': 'A More Useful Research Question', 'body': 'Clue ' * ((low + high) // 2)})}}],
                'usage': {'total_tokens': 500}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            engine = ContentEngine(settings, db, AIRouter(settings, db, client))
            ident = await engine.draft(main)
            p = db.get(ident)['payload']
            assert len(requests) == 1
            assert engine.checker.check(p) == []
            assert 'audit:' not in p['body']
            assert bool('Source: binance' in p['body']) == (slot == 9)
            assert db.state('last_ai_generation')['content_mix_revision'] == content_mix.REVISION
            if slot == 2:
                assert p['content_style'] == 'multi_coin' and '$BTC' in p['body'] and '$SOL' in p['body']
                stale = deepcopy(p)
                stale['internal_evidence']['related'][0]['as_of'] = utc() - 9999
                assert engine.checker.check(stale)
    asyncio.run(run())


def test_profiled_article_retains_three_section_calls_and_one_risk_note(tmp_path):
    async def run():
        settings, db, main = setup_context(tmp_path)
        settings.ai_url, settings.ai_key, settings.ai_model = 'https://test.example/chat', 'key', 'test'
        settings.ai_daily_tokens = 200000
        settings.ai_evidence_footer = False
        requests = []
        def respond(request):
            sent = json.loads(request.content)
            data = json.loads(sent['messages'][1]['content'])
            assert 'article_section' in data
            requests.append(data)
            low, high = map(int, re.search(r'BODY contains (\d+)–(\d+)', sent['messages'][0]['content']).groups())
            word = ('Clue', 'Risk', 'Rule')[len(requests) - 1]
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
                'title': 'A Research Puzzle', 'body': (word + ' ') * ((low + high) // 2)})}}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            engine = ContentEngine(settings, db, CompatibleAI(settings, db, client))
            ident = await engine.draft(main, article=True)
            p = db.get(ident)['payload']
            assert len(requests) == 3
            assert p['article'] and 1000 <= len(p['body'].split()) <= 2000
            assert p['body'].count(content_mix.DISCLAIMER) == 1
            assert 'audit:' not in p['body'] and 'binance' not in p['body'].lower()
            assert engine.checker.check(p) == []
    asyncio.run(run())


def test_unknown_cashtag_uses_existing_single_correction_then_fails_without_saving(tmp_path):
    async def run():
        settings, db, main = setup_context(tmp_path)
        settings.ai_url, settings.ai_key, settings.ai_model = 'https://test.example/chat', 'key', 'test'
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
                'title': 'An Interesting Clue', 'body': 'Clue ' * 130 + '$UNVERIFIED'})}}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            engine = ContentEngine(settings, db, CompatibleAI(settings, db, client))
            with pytest.raises(ValueError, match='Unknown fact token'):
                await engine.draft(main)
            assert len(calls) == 2  # Existing one-correction policy, no new loop.
            assert not db.list('draft')
            assert not db.state('content_mix_progress')
    asyncio.run(run())


def test_comparison_publication_uses_existing_draft_age_without_a_new_shorter_window(tmp_path):
    settings, db, main = setup_context(tmp_path)
    settings.post_max_words = 400
    db.set('content_mix_progress', {'mix_slots': 2, 'market_drafts': 2})
    brief = content_mix.plan(settings, db, main)
    raw = content_mix.format_result({'title': 'A Fair Watchlist Question', 'body': educational_payload(3)['body']},
                                    {'symbol': main['symbol'], 'source': 'binance', 'timestamp': stamp(main['as_of']),
                                     'editorial_brief': brief})
    p = {**raw, 'event': main, 'article': False, 'content_mix_version': content_mix.REVISION,
         'internal_evidence': content_mix.provenance(main, brief)}
    assert brief['related']
    checker = FactChecker(settings, db)
    assert not checker.check(p, main['as_of'] + 1500)  # Same existing 30-minute draft window.
    assert 'Market evidence expired or future dated' in checker.check(p, main['as_of'] + 1801)
