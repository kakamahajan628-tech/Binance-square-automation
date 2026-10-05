"""Official Square OpenAPI only. Uncertain POST outcomes are never blindly retried."""
from pathlib import Path
from typing import Protocol
import httpx
from .models import utc, digest
from .compliance import Compliance
from .content import square_length_error
from .media import MediaError, SquareMedia, restore_png, prepared_image_url
import hashlib


# These official codes reject the submitted content, not the whole account.
# Unknown, authentication and account-restriction codes retain the global latch.
DRAFT_REJECTIONS = {
    'media_missing': 'Approved image is missing; restore it from durable storage or create a new reviewed image',
    'media_invalid': 'Image format, size or filename is invalid; no content submission was made',
    'media_changed': 'Image bytes differ from the approved hash; fresh image review is required',
    'media_invalid_response': 'Image API returned an invalid upload response; no content submission was made',
    'media_timeout': 'Image processing timed out before content submission; review before retrying',
    'media_processing_failed': 'Image processing failed before content submission; review the image',
    'media_transport_failed': 'Image upload failed before content submission; review before retrying',
    '220014': 'Square daily image upload limit reached; review the image draft after the quota resets',
    'local_length': 'Content exceeds configured Square character limit; no HTTP publish request was sent',
    '20002': 'Sensitive words detected in this draft',
    '20022': 'Sensitive words detected in this draft',
    '20013': 'Content length rejected by Square; check title/body length and short-post versus article type',
    '20020': 'Content body must not be empty',
    '220011': 'Content body must not be empty',
}


def publication_lengths(draft, settings=None):
    title, body = draft.get('title', ''), draft.get('body', '')
    article = bool(draft.get('article'))
    submitted = body if article else title + '\n\n' + body
    return {'content_type': 2 if article else 1, 'title_characters': len(title),
            'body_characters': len(body), 'body_words': len(body.split()),
            'submitted_text_characters': len(submitted),
            'maximum_text_units': getattr(settings, 'article_max_characters' if article else 'post_max_characters', 80000 if article else 2100),
            'submitted_text_utf16_units': len(submitted.encode('utf-16-le')) // 2}


class Publisher(Protocol):
    async def send(self, draft: dict) -> dict: ...


class AmbiguousPublication(Exception):
    pass


class WithheldPublication(Exception):
    pass


class RejectedPublication(Exception):
    def __init__(self, code, retry_after=0):
        self.code, self.retry_after = str(code), retry_after


class SquarePublisher:
    # Endpoints and header contract verified against Binance's official repository.
    URL = 'https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add'

    def __init__(self, key, client, directory=None, store=None, upload_timeout=30):
        self.key, self.client, self.directory, self.store = key, client, directory, store
        self.upload_timeout = upload_timeout

    async def prepare_image(self, draft):
        try:
            if not self.directory or not draft.get('image_hash'):
                raise MediaError('media_missing')
            data = restore_png(self.store, self.directory, draft['image'], draft['image_hash'])
            url = prepared_image_url(draft, self.key)
            if not url:
                url = await SquareMedia(self.key, self.client, self.upload_timeout).upload(draft['image'], data)
            return {'url': url, 'sha256': draft['image_hash'], 'at': utc(),
                    'key_fingerprint': hashlib.sha256(self.key.encode()).hexdigest()}
        except MediaError as error:
            raise RejectedPublication(error.code, error.retry_after) from None
        except OSError:
            raise RejectedPublication('media_missing') from None

    async def send(self, draft, before_submit=None):
        if square_length_error(draft):
            raise RejectedPublication('local_length')
        body = {'contentType': 2 if draft.get('article') else 1,
                'bodyTextOnly': draft['body'] if draft.get('article') else draft['title'] + '\n\n' + draft['body']}
        if draft.get('article'):
            body['title'] = draft['title']
        if draft.get('image'):
            prepared = await self.prepare_image(draft)
            url = prepared['url']
            body['cover' if draft.get('article') else 'imageList'] = url if draft.get('article') else [url]
        if before_submit:
            before_submit()
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
            return {'id': str(receipt['id']), 'url': receipt.get('shareLink'),
                    'adapter': 'official_square_image' if draft.get('image') else 'official_square_text'}
        except (httpx.HTTPError, ValueError) as exc:
            raise AmbiguousPublication('Network timeout or malformed receipt; inspect Creator Center') from None


class PublicationService:
    def __init__(self, settings, store, checker, client):
        self.s, self.db, self.checker = settings, store, checker
        self.policy = Compliance(settings, store)
        self.adapter = SquarePublisher(settings.square_key, client, settings.artifacts, store,
                                       settings.image_upload_timeout_seconds)
        self.worker_owner = None

    async def prepare_image_job(self, ident):
        """Upload only. The scheduler performs the separately gated content POST."""
        row = self.db.get(ident)
        if not row or row['status'] != 'uploading_image':
            return
        p = row['payload']
        try:
            prepared = await self.adapter.prepare_image(p)
            with self.db.transaction():
                current = self.db.get(ident)
                lease = self.db.state('worker_lease', {})
                owns = self.worker_owner is None or (lease.get('owner') == self.worker_owner and lease.get('until', 0) > utc())
                if current and current['status'] == 'uploading_image' and owns:
                    payload = current['payload']
                    payload['image_upload'] = prepared
                    self.db.update(ident, status='approved', payload=payload, clear_due=True)
                    self.db.log('IMAGE', 'Upload prepared; content awaits publication checks', ident)
        except RejectedPublication as error:
            current = self.db.get(ident)
            lease = self.db.state('worker_lease', {})
            owns = self.worker_owner is None or (lease.get('owner') == self.worker_owner and lease.get('until', 0) > utc())
            if not current or current['status'] != 'uploading_image' or not owns:
                return
            p.update(rejection_code=error.code, approved_at=None, human_review_required=True)
            self.db.update(ident, status='review', payload=p, clear_due=True)
            details = {'code': error.code, 'draft_id': ident, 'at': utc(), 'stage': 'image_upload',
                       'lengths': publication_lengths(p, self.s)}
            if error.code in DRAFT_REJECTIONS:
                details.update(scope='draft', message=DRAFT_REJECTIONS[error.code])
            elif error.code != '429':
                self.db.set('publisher_blocked', True)
                self.db.set('publisher_block_reason', {'code': error.code, 'draft_id': ident, 'at': utc()})
            if error.code in ('429', '220014'):
                self.db.set('image_retry_after', utc() + max(error.retry_after, 3600))
            self.db.set('publish_last_rejection', details)
            self.db.log('IMAGE', 'Upload rejected before content submission; code=' + error.code, ident)

    def status(self):
        blocked = self.db.state('publisher_blocked', False)
        reason = self.db.state('publisher_block_reason', {})
        if blocked and not reason:
            for row in self.db.list('draft', limit=10000):
                code = row['payload'].get('rejection_code')
                if code and code not in ('429', '220009', 'media_requires_manual_export'):
                    reason = {'code': code, 'draft_id': row['id'], 'at': row['updated']}
                    break
        # Remote text is never included. Codes have a short, fixed character set.
        import re
        code = str(reason.get('code', ''))
        code = code if re.fullmatch(r'[A-Za-z0-9_-]{1,30}', code) else 'unavailable'
        rejected = self.db.get(reason.get('draft_id')) if reason.get('draft_id') else None
        if rejected and rejected['kind'] == 'draft':
            reason = {**reason, 'lengths': publication_lengths(rejected['payload'], self.s)}
        if code in DRAFT_REJECTIONS:
            reason = {**reason, 'message': DRAFT_REJECTIONS[code], 'scope': 'draft'}
        next_step = ('Correct the rejected draft; do not re-approve unchanged content. '
                     'Use /policy_clear CONFIRM to clear this legacy draft-error latch after review.'
                     if blocked and code in DRAFT_REJECTIONS else
                     'Review the rejection and fix Square access; then /policy_clear CONFIRM. Re-approve only a rejected draft.'
                     if blocked else 'Publication gates and approval still apply.')
        return {'blocked': blocked, 'reason': {**reason, 'code': code} if reason else {},
                'last_rejection': self.db.state('publish_last_rejection', {}),
                'retry_after': self.db.state('publish_retry_after', 0),
                'uncertain_submissions': len(self.db.list('draft', ['uncertain'])),
                'next_step': next_step}

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
            self.db.update(ident, status='preparing_media' if draft.get('image') and not self.s.paper_mode
                           and self.s.live_enabled else 'sending', payload=draft)
        try:
            if self.s.paper_mode or not self.s.live_enabled:
                path = Path(self.s.artifacts) / (ident + '.txt')
                path.write_text(draft['title'] + '\n\n' + draft['body'], encoding='utf-8')
                receipt = {'adapter': 'paper' if self.s.paper_mode else 'manual_export', 'file': path.name,
                           'image': draft.get('image')}
                status = 'paper_published' if self.s.paper_mode else 'manual_ready'
            else:
                if draft.get('image'):
                    receipt = await self.adapter.send(draft, before_submit=lambda: self.media_ready(ident))
                    draft = self.db.get(ident)['payload']
                else:
                    receipt = await self.adapter.send(draft)
                status = 'published'
            draft['receipt'] = receipt
            self.db.update(ident, status=status, payload=draft)
            self.db.log('PUBLISH', status, ident)
        except AmbiguousPublication:
            self.db.update(ident, status='uncertain')
            self.db.log('PUBLISH', 'Uncertain submission; automatic retry forbidden, reconcile in Creator Center', ident)
        except WithheldPublication:
            # A stop, expiry or lost lease during upload prevents /content/add.
            # media_ready already records the precise local reason and state.
            pass
        except RejectedPublication as exc:
            draft['rejection_code'] = exc.code
            draft['approved_at'] = None
            draft['human_review_required'] = True
            details = {'code': exc.code, 'draft_id': ident, 'at': utc(),
                       'lengths': publication_lengths(draft, self.s)}
            if exc.code in DRAFT_REJECTIONS:
                details.update(scope='draft', message=DRAFT_REJECTIONS[exc.code])
            self.db.set('publish_last_rejection', details)
            if exc.code in ('429', '220009'):
                self.db.set('publish_retry_after', utc() + max(exc.retry_after, 3600))
                self.db.set('policy_caps', {'daily': max(1, self.policy.caps()['daily'] - 1)})
            elif exc.code != 'media_requires_manual_export' and exc.code not in DRAFT_REJECTIONS:
                self.db.set('publisher_blocked', True)
                self.db.set('publisher_block_reason', {'code': exc.code, 'draft_id': ident, 'at': utc()})
            self.db.update(ident, status='review', payload=draft)
            self.db.log('PUBLISH', 'Rejected; code=' + exc.code, ident)
        except OSError:
            self.db.update(ident, status='review')
            self.db.log('PUBLISH', 'Local artifact write failed', ident)

    def media_ready(self, ident):
        """Recheck evidence and controls after upload, immediately before POST."""
        with self.db.transaction():
            row = self.db.get(ident)
            if not row or row['status'] != 'preparing_media':
                raise WithheldPublication()
            lease_lost = False
            if self.worker_owner is not None:
                lease = self.db.state('worker_lease', {})
                if lease.get('owner') != self.worker_owner or lease.get('until', 0) <= utc():
                    lease_lost = True
            reasons = self.checker.check(row['payload'])
            duplicate = self.checker.duplicate(row['payload'], exclude=ident)
            if duplicate:
                reasons.append(duplicate)
            if reasons:
                self.db.update(ident, status='expired' if any('expired' in r for r in reasons) else 'review', clear_due=True)
                self.db.log('PUBLISH', 'Image preparation withheld: ' + '; '.join(reasons), ident)
                # Commit the withheld state before raising outside the transaction.
                blocked = True
            elif (row['payload'].get('risk') == 'high' or row['payload'].get('human_review_required')
                  or self.s.mode == 'approval') and not row['payload'].get('approved_at'):
                self.db.update(ident, status='review', clear_due=True)
                self.db.log('PUBLISH', 'Image preparation withheld: fresh approval required', ident)
                blocked = True
            else:
                gate = ('Worker lease lost during image preparation; no content submitted' if lease_lost
                        else self.policy.gate(row) or self.policy.live_gate())
                if gate:
                    self.db.update(ident, status='queued')
                    self.db.log('PUBLISH', 'Image preparation withheld: ' + gate, ident)
                    blocked = True
                else:
                    p = row['payload']
                    p['submitted_at'] = utc()
                    self.db.update(ident, status='sending', payload=p)
                    blocked = False
        if blocked:
            raise WithheldPublication()
