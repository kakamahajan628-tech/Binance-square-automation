import asyncio
from dataclasses import replace
from types import SimpleNamespace
import json

import httpx
import pytest

from square_desk.config import Settings
from square_desk.content import CompatibleAI, FactChecker, evidence_footer, square_length_error, text_units
from square_desk.models import stamp
from square_desk.publisher import SquarePublisher, RejectedPublication
from square_desk.store import Store
from test_square_generation_audit import evidence, completion


def test_short_post_counts_title_and_separator_at_exact_boundary():
    draft = {'title': 'A', 'body': 'x' * 2097}
    assert not square_length_error(draft)
    assert square_length_error({**draft, 'body': draft['body'] + 'x'}) == 'Square short post length 2101; maximum 2100 characters'
    assert not square_length_error({**draft, 'article': True, 'body': 'x' * 80000})
    assert square_length_error({**draft, 'article': True, 'body': 'x' * 80001})


def test_emoji_count_uses_conservative_utf16_units():
    assert text_units('😀') == 2
    assert square_length_error({'title': 'A', 'body': '😀' * 1049})
    assert not square_length_error({'title': 'A', 'body': '😀' * 1048})


def test_reported_old_post_is_blocked_locally_even_when_words_fit():
    s = Settings(post_min_words=200, post_max_words=400)
    # Match the user's metadata without requiring their private draft text.
    body = 'word ' * 332 + 'x' * 625
    assert len(body) == 2285 and len(body.split()) == 333
    draft = {'title': 't' * 18, 'body': body}
    db = Store(':memory:')
    errors = FactChecker(s, db).check(draft)
    assert 'Word count outside configured limits' not in errors
    assert errors == ['Square short post length 2305; maximum 2100 characters']
    db.close()


def test_character_feedback_rewrites_complete_draft_and_keeps_evidence():
    async def run():
        calls = []
        ev = evidence(True)
        def respond(request):
            payload = json.loads(request.content)
            calls.append(payload)
            prompt = payload['messages'][0]['content']
            assert 'rendered post must fit 2100 characters' in prompt
            if len(calls) == 1:
                return httpx.Response(200, json=completion('Liquidity observations require careful interpretation. ' * 55))
            assert 'character_validation' in payload['messages'][-1]['content']
            return httpx.Response(200, json=completion('Structure and liquidity warrant cautious interpretation. ' * 30))
        db = Store(':memory:')
        s = Settings(database=':memory:', ai_url='https://example.com/completions', ai_key='test', ai_model='test',
                     post_min_words=200, post_max_words=400)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await CompatibleAI(s, db, client).generate(ev)
            assert len(calls) == 2
            assert 200 <= len(result['body'].split()) <= 400
            assert result['body'].endswith(evidence_footer(ev))
            assert not square_length_error(result, s)
            assert result['body'].startswith('Structure and liquidity')  # Complete regeneration, not slicing the first answer.
        db.close()
    asyncio.run(run())


def test_article_sections_do_not_inherit_short_post_cap():
    async def run():
        calls = []
        sections = ['Market observations contextualize trends liquidity risk volatility interpretation. ',
                    'Scenarios depend upon conditional invalidation execution uncertainty structure. ',
                    'Evidence limitations distinguish historical records potential forecasts assumptions. ']
        def respond(request):
            calls.append(request)
            # Each section exceeds 2,100 characters, while remaining in the
            # allocated word range and below the complete article limit.
            return httpx.Response(200, json=completion(sections[len(calls)-1] * 60))
        db = Store(':memory:')
        s = Settings(ai_url='https://example.com/completions', ai_key='test', ai_model='test', ai_daily_tokens=200000)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await CompatibleAI(s, db, client).generate(evidence(), True)
            assert len(calls) == 3
            assert 1000 <= len(result['body'].split()) <= 2000
            assert not square_length_error({**result, 'article': True}, s)
        db.close()
    asyncio.run(run())


def test_adapter_never_sends_overlong_short_post_and_does_not_change_type():
    async def run():
        calls = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: calls.append(request))) as client:
            adapter = SquarePublisher('test', client)
            draft = {'title': 'Research', 'body': 'a' * 2305}
            with pytest.raises(RejectedPublication) as error:
                await adapter.send(draft)
            assert error.value.code == 'local_length' and not calls
            assert not draft.get('article')  # No unapproved conversion to article format.
    asyncio.run(run())


def test_operator_character_caps_cannot_exceed_verified_platform_bounds():
    for fields in ({'post_max_characters': 2101}, {'article_max_characters': 80001}, {'post_max_characters': 0}):
        with pytest.raises(ValueError, match='Square character limits'):
            replace(Settings(), **fields).validate()
