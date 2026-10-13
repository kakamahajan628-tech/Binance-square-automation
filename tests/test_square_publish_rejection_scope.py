import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from square_desk.config import Settings
from square_desk.models import utc
from square_desk.service import Desk
from test_square_desk import draft_payload


@pytest.mark.parametrize('code,blocked', [('20013', False), ('20002', False), ('20022', False),
    ('20020', False), ('220011', False), ('220003', True), ('30008', True), ('unknown', True)])
def test_content_rejection_stays_on_draft_and_account_rejection_blocks(tmp_path, code, blocked):
    async def run():
        requests = []
        def respond(request):
            requests.append(request)
            return httpx.Response(200, json={'code': code})
        s = Settings(database=':memory:', artifacts=str(tmp_path), paper_mode=False, live_enabled=True,
            square_key='test', admin_token='test-admin-token', publishing_hours=tuple(range(24)),
            policy_reviewed=datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat())
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk = Desk(s, client=client)
            ident = desk.db.insert('draft', {**draft_payload(), 'approved_at': utc()}, 'queued')
            await desk.publisher.publish(ident)
            row = desk.db.get(ident)
            assert len(requests) == 1
            assert row['status'] == 'review' and row['payload']['approved_at'] is None
            assert row['payload']['human_review_required']
            assert desk.db.state('publisher_blocked', False) == blocked
            assert desk.publisher.status()['last_rejection']['code'] == code
            await desk.publisher.publish(ident)
            assert len(requests) == 1  # No retry of unchanged rejected content.
            await desk.close()
    asyncio.run(run())


def test_legacy_length_latch_explains_lengths_without_clearing_it(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path)))
        p = {**draft_payload(), 'rejection_code': '20013'}
        ident = desk.db.insert('draft', p, 'review')
        desk.db.set('publisher_blocked', True)
        report = desk.publisher.status()
        assert report['blocked'] and report['reason']['code'] == '20013'
        assert report['reason']['scope'] == 'draft'
        assert report['reason']['lengths']['body_characters'] == len(p['body'])
        assert report['reason']['lengths']['content_type'] == 1
        assert report['reason']['draft_id'] == ident
        assert 'legacy' in report['next_step'] and 'fix Square access' not in report['next_step']
        assert desk.db.state('publisher_blocked')
        await desk.close()
    asyncio.run(run())
