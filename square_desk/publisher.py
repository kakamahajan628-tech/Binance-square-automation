"""Official Square OpenAPI only. Uncertain POST outcomes are never blindly retried."""
from pathlib import Path
from typing import Protocol
import httpx
from .models import utc, digest
from .compliance import Compliance


class Publisher(Protocol):
    async def send(self, draft: dict) -> dict: ...


class AmbiguousPublication(Exception):
    pass


class RejectedPublication(Exception):
    def __init__(self, code, retry_after=0):
        self.code, self.retry_after = str(code), retry_after


class SquarePublisher:
    # Endpoints and header contract verified against Binance's official repository.
    URL = 'https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add'

    def __init__(self, key, client):
        self.key, self.client = key, client

    async def send(self, draft):
        body = {'contentType': 2 if draft.get('article') else 1,
                'bodyTextOnly': draft['body'] if draft.get('article') else draft['title'] + '\n\n' + draft['body']}
        if draft.get('article'):
            body['title'] = draft['title']
        try:
            response = await self.client.post(self.URL, headers={'X-Square-OpenAPI-Key': self.key,
                        'clienttype': 'binanceSkill', 'Content-Type': 'application/json'}, json=body)
            # Official script treats 504 as possibly accepted. Keep it uncertain
            # here rather than claiming a confirmed receipt or risking duplicates.
            if response.status_code >= 500:
                raise AmbiguousPublication('Server response does not establish whether content was accepted')
            if response.status_code == 429:
                try:
                    delay = max(60, float(response.headers.get('Retry-After', 3600)))
                except ValueError:
                    delay = 3600
                raise RejectedPublication('429', delay)
            if response.status_code in (401, 403, 418, 451):
                raise RejectedPublication(response.status_code)
            data = response.json()
            if str(data.get('code')) != '000000':
                raise RejectedPublication(str(data.get('code', 'invalid_response'))[:30])
            receipt = data.get('data')
            if not isinstance(receipt, dict) or not receipt.get('id'):
                raise AmbiguousPublication('Accepted response lacks a post identifier')
            return {'id': str(receipt['id']), 'url': receipt.get('shareLink'), 'adapter': 'official_square_text'}
        except (httpx.HTTPError, ValueError) as exc:
            raise AmbiguousPublication('Network timeout or malformed receipt; inspect Creator Center') from None


class PublicationService:
    def __init__(self, settings, store, checker, client):
        self.s, self.db, self.checker = settings, store, checker
        self.policy = Compliance(settings, store)
        self.adapter = SquarePublisher(settings.square_key, client)
        self.worker_owner = None

    async def publish(self, ident):
        # The worker holds a service lease. Reservation and state change are atomic,
        # and no transaction stays open during remote I/O.
        with self.db.transaction():
            if self.worker_owner is not None:
                lease = self.db.state('worker_lease', {})
                if lease.get('owner') != self.worker_owner or lease.get('until', 0) <= utc():
                    self.db.log('PUBLISH', 'Worker lease not held; publication withheld', ident)
                    return
            row = self.db.get(ident)
            if not row or row['status'] != 'queued':
                return
            draft = row['payload']
            reasons = self.checker.check(draft)
            duplicate = self.checker.duplicate(draft, exclude=ident)
            if duplicate:
                reasons.append(duplicate)
            if reasons:
                self.db.update(ident, status='expired' if any('expired' in x for x in reasons) else 'review')
                self.db.log('PUBLISH', 'Blocked: ' + '; '.join(reasons), ident)
                return
            gate = self.policy.gate(row)
            if gate:
                return
            if draft.get('risk') == 'high' or draft.get('human_review_required') or self.s.mode == 'approval':
                if not draft.get('approved_at'):
                    self.db.update(ident, status='review')
                    return
            draft['submitted_at'] = utc()
            if not self.s.paper_mode and self.s.live_enabled:
                gate = self.policy.live_gate()
                if gate:
                    self.db.log('PUBLISH', gate, ident)
                    return
            self.db.update(ident, status='sending', payload=draft)
        try:
            if self.s.paper_mode or not self.s.live_enabled:
                path = Path(self.s.artifacts) / (ident + '.txt')
                path.write_text(draft['title'] + '\n\n' + draft['body'], encoding='utf-8')
                receipt = {'adapter': 'paper' if self.s.paper_mode else 'manual_export', 'file': path.name,
                           'image': draft.get('image')}
                status = 'paper_published' if self.s.paper_mode else 'manual_ready'
            else:
                if draft.get('image'):
                    # Media workflow can be completed manually; never silently
                    # discard an approved attachment in a text-only adapter.
                    raise RejectedPublication('media_requires_manual_export')
                receipt, status = await self.adapter.send(draft), 'published'
            draft['receipt'] = receipt
            self.db.update(ident, status=status, payload=draft)
            self.db.log('PUBLISH', status, ident)
        except AmbiguousPublication:
            self.db.update(ident, status='uncertain')
            self.db.log('PUBLISH', 'Uncertain submission; automatic retry forbidden, reconcile in Creator Center', ident)
        except RejectedPublication as exc:
            draft['rejection_code'] = exc.code
            if exc.code in ('429', '220009'):
                self.db.set('publish_retry_after', utc() + max(exc.retry_after, 3600))
                self.db.set('policy_caps', {'daily': max(1, self.policy.caps()['daily'] - 1)})
            elif exc.code != 'media_requires_manual_export':
                self.db.set('publisher_blocked', True)
            self.db.update(ident, status='review', payload=draft)
            self.db.log('PUBLISH', 'Rejected; code=' + exc.code, ident)
        except OSError:
            self.db.update(ident, status='review')
            self.db.log('PUBLISH', 'Local artifact write failed', ident)
