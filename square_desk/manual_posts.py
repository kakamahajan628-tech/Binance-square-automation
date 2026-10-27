"""An isolated, explicit administrator image+text publishing lane (10/day)."""
import asyncio
from datetime import datetime, timezone
from hashlib import sha256
import json
import logging
import math
import re
import sqlite3
from zoneinfo import ZoneInfo

import httpx

from .compliance import Compliance
from .content import FactChecker, text_units
from .media import MediaError, MediaPending, persist_png
from .models import utc
from .publisher import (SquarePublisher, AmbiguousPublication, RejectedPublication,
                        WithheldPublication, DRAFT_REJECTIONS)
from .manual_media import download_image, normalized_png, save_png

KIND = 'manual_post'
CAP = 10
POLL_SECONDS = 5
CONTENT_SECONDS = 15
QUEUED = 'manual_queued'
MEDIA = 'manual_media'
SENDING = 'manual_sending'
PUBLISHED = 'manual_published'
UNCERTAIN = 'manual_uncertain'
REVIEW = 'manual_review'
WAITING = 'manual_waiting_text'
CANCELLED = 'manual_cancelled'
PROTECTED = (WAITING, QUEUED, MEDIA, SENDING, UNCERTAIN, REVIEW)
COUNTED = (MEDIA, SENDING, PUBLISHED, UNCERTAIN)
COMMANDS = {'/manual', '/manual_status', '/manual_queue', '/manual_preview',
            '/manual_cancel', '/manual_retry', '/manual_reconcile'}


def message_text(message):
    text = message.get('text')
    if not isinstance(text, str):
        text = message.get('caption')
    return text.strip() if isinstance(text, str) else ''


def aligned_text(text, settings):
    if not isinstance(text, str):
        raise ValueError('Manual text is required')
    text = text.replace('\r\n', '\n').replace('\r', '\n').strip()
    if not text:
        raise ValueError('Send the text for this image')
    if any(ord(c) < 32 and c not in '\n\t' for c in text):
        raise ValueError('Manual text contains control characters')
    if text_units(text) > settings.post_max_characters:
        raise ValueError('Manual text exceeds the configured Square character limit')
    if FactChecker.BANNED.search(text.replace('Probabilistic market research, not a recommendation or guaranteed return.', '')):
        raise ValueError('Manual text contains prohibited promotion or unsupported profit language')
    return text


def account_gate(settings, db, now, exclude=None):
    """Shared platform cap only; automated cadence and daily target stay separate."""
    day = datetime.fromtimestamp(now, timezone.utc).date()
    count = 0
    for kind, statuses in (('draft', ('published', 'manual_published', 'sending', 'uncertain', 'preparing_media')),
                           (KIND, COUNTED)):
        for row in db.list(kind, statuses, limit=10000):
            if row['id'] == exclude:
                continue
            at = row['payload'].get('submitted_at', row['payload'].get('reserved_at', row['updated']))
            if datetime.fromtimestamp(at, timezone.utc).date() == day:
                count += 1
    return 'Shared Square daily platform limit reached' if count >= settings.platform_daily_cap else None


class ManualAwarePolicy(Compliance):
    def __init__(self, settings, store, lane):
        super().__init__(settings, store)
        self.lane = lane

    def gate(self, row, now=None, scheduled=False):
        reason = super().gate(row, now, scheduled)
        if reason or self.lane.recovered and not self.lane.has_items:
            return reason
        return account_gate(self.s, self.db, utc() if now is None else now, row['id'])


class ManualAdapter(SquarePublisher):
    async def send(self, draft, before_submit=None, *, checkpoint=None, on_prepared=None):
        # Keep user text intact; the regular title/body formatter is untouched.
        if not draft.get('body') or text_units(draft['body']) > 2100:
            raise RejectedPublication('local_length')
        prepared = await self.prepare_image(draft, checkpoint=checkpoint)
        if on_prepared is not None:
            on_prepared(prepared)
        if before_submit is None:
            raise WithheldPublication()
        before_submit()
        try:
            response = await asyncio.wait_for(self.client.post(self.URL,
                headers={'X-Square-OpenAPI-Key': self.key, 'clienttype': 'binanceSkill', 'Content-Type': 'application/json'},
                json={'contentType': 1, 'bodyTextOnly': draft['body'], 'imageList': [prepared['url']]}, timeout=10), CONTENT_SECONDS)
            if response.status_code >= 500:
                raise AmbiguousPublication()
            if response.status_code == 429:
                try: delay = max(60, float(response.headers.get('Retry-After', 3600)))
                except ValueError: delay = 3600
                if not math.isfinite(delay): delay = 3600
                raise RejectedPublication('429', delay)
            if response.status_code in (401, 403, 418, 451):
                raise RejectedPublication(response.status_code)
            data = response.json()
            if not isinstance(data, dict):
                raise AmbiguousPublication()
            if type(data.get('code')) not in (str, int):
                raise AmbiguousPublication()
            code = str(data['code'])
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,30}', code):
                raise AmbiguousPublication()
            if str(data.get('code')) != '000000':
                raise RejectedPublication(code)
            receipt = data.get('data')
            if (not response.is_success or not isinstance(receipt, dict)
                    or type(receipt.get('id')) not in (str, int) or not receipt['id']
                    or not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', str(receipt['id']))):
                raise AmbiguousPublication()
            return {'id': str(receipt['id']), 'url': receipt.get('shareLink'), 'adapter': 'official_square_image'}
        except (httpx.HTTPError, ValueError, TimeoutError):
            raise AmbiguousPublication() from None


class ManualPosts:
    def __init__(self, desk, client=None):
        self.desk, self.s, self.db = desk, desk.s, desk.db
        self.client, self.owns_client = client, client is None
        self.task = None
        self.wake = asyncio.Event()
        self.has_items = False
        self.recovered = False
        self.last = {}

    def day(self):
        return datetime.fromtimestamp(utc(), ZoneInfo(self.s.timezone)).date().isoformat()

    def owns(self):
        lease = self.db.state('worker_lease', {})
        return bool(self.desk.lease_owned and lease.get('owner') == self.desk.owner and lease.get('until', 0) > utc())

    def reserve(self, day):
        # Caller owns the existing fenced transaction; never open a nested one.
        used = self.db.budget_used(day, 'manual_posts')
        if used >= CAP:
            raise ValueError('Manual daily cap reached: 10/10; automatic posts continue normally')
        self.db.conn.execute('INSERT INTO budgets VALUES (?,?,?) ON CONFLICT(day,category) DO UPDATE SET used=excluded.used',
                             (day, 'manual_posts', used + 1))

    def refund(self, payload):
        day = payload.pop('manual_budget_day', None)
        if day:
            self.db.conn.execute('UPDATE budgets SET used=CASE WHEN used>0 THEN used-1 ELSE 0 END WHERE day=? AND category=?',
                                 (day, 'manual_posts'))

    def notify(self, text, key):
        self.desk.telegram.notify(text, key='manual:' + key)

    def handles(self, message):
        text = message_text(message)
        name = text.split(maxsplit=1)[0].split('@')[0].lower() if text else ''
        if name in COMMANDS or message.get('photo') or message.get('document'):
            return True
        if text and not text.startswith('/'):
            pending = self.db.state('manual_pending:' + str(message['from']['id']), {})
            return pending.get('until', 0) > utc()
        return False

    def _photo(self, message):
        if message.get('media_group_id'):
            raise ValueError('Send one image per manual post, not an album')
        if isinstance(message.get('photo'), list) and message['photo']:
            photo = max(message['photo'], key=lambda p: p.get('width', 0) * p.get('height', 0))
        else:
            photo = message.get('document', {})
            if photo.get('mime_type') not in ('image/png', 'image/jpeg', 'image/webp'):
                raise ValueError('Send a JPG, PNG or WebP image')
        if (not isinstance(photo.get('file_id'), str) or type(photo.get('file_size', 0)) is not int
                or not 0 <= photo.get('file_size', 0) <= 8_000_000):
            raise ValueError('Manual image is unavailable or larger than 8 MB')
        return {'file_id': photo['file_id'], 'message_id': message['message_id']}

    def _queue(self, row, text):
        body = aligned_text(text, self.s)
        with self.db.transaction():
            current = self.db.get(row['id'])
            if not current or current['kind'] != KIND:
                raise ValueError('Manual image is no longer available; start /manual again')
            if current['status'] != WAITING:
                return 'This manual image already has text; use /manual_queue.'
            day = self.day()
            self.reserve(day)
            p = current['payload']
            p.update(title='', body=body, original_text=body, manual_budget_day=day,
                     authorized_at=utc(), reserved_at=utc(), source='telegram_manual', image_origin='user_supplied')
            self.db.update(row['id'], status=QUEUED, payload=p)
        self.has_items = True
        self.kick()
        return 'Manual post ' + row['id'] + ' accepted. Your text + image will publish directly when controls allow. Separate cap: 10/day; automatic queue unchanged.'

    def receive(self, message):
        sender = message['from']['id']
        text = message_text(message)
        parts = text.split(maxsplit=1)
        name = parts[0].split('@')[0].lower() if parts else ''
        arg = parts[1] if len(parts) == 2 else ''
        if name == '/manual_status':
            return json.dumps(self.status(), indent=2)
        if name == '/manual_queue':
            return '\n'.join(r['id'] + ' ' + r['status'] for r in self.db.list(KIND, limit=15)) or 'No manual posts'
        if name in ('/manual_preview', '/manual_cancel', '/manual_retry', '/manual_reconcile'):
            return self.control(name, arg, sender)
        picture = message if message.get('photo') or message.get('document') else None
        reply = message.get('reply_to_message', {})
        if name == '/manual' and (reply.get('photo') or reply.get('document')):
            if reply.get('from', {}).get('id') != sender:
                raise ValueError('Reply to an image you sent yourself')
            picture = reply
        if name == '/manual' and picture is None and not arg:
            with self.db.transaction():
                pending = self.db.state('manual_pending:' + str(sender), {})
                previous = self.db.get(pending.get('id'))
                if previous and previous['kind'] == KIND and previous['status'] == WAITING and previous['payload'].get('sender') == sender:
                    self.db.update(previous['id'], status=CANCELLED)
                self.db.set('manual_pending:' + str(sender), {})
                self.db.set('manual_armed:' + str(sender), utc() + 900)
            return 'Manual mode ready for 15 minutes. Send ONE image with its caption, or send the image then its text. Completed pair authorizes direct publication; /manual_cancel cancels composition. Separate cap: 10/day.'
        if picture is not None:
            if text.startswith('/') and name != '/manual':
                return 'Use /manual in the image caption to authorize a manual post.'
            armed = self.db.state('manual_armed:' + str(sender), 0) > utc()
            if name != '/manual' and not armed:
                return 'For a manual image post, first send /manual, or put /manual followed by your text in the image caption.'
            if self.db.budget_used(self.day(), 'manual_posts') >= CAP:
                raise ValueError('Manual daily cap reached: 10/10; automatic posts continue normally')
            photo = self._photo(picture)
            fingerprint = 'telegram-manual:' + str(sender) + ':' + str(photo['message_id'])
            try:
                with self.db.transaction():
                    old = self.db.conn.execute('SELECT id FROM entities WHERE kind=? AND fingerprint=?', (KIND, fingerprint)).fetchone()
                    if old:
                        row = self.db.get(old[0])
                        if row['status'] != WAITING:
                            return 'This image message was already received. Use /manual_queue.'
                        ident = row['id']
                    else:
                        ident = self.db.insert(KIND, {**photo, 'sender': sender}, WAITING, fingerprint=fingerprint)
                    self.db.set('manual_armed:' + str(sender), 0)
                    self.db.set('manual_pending:' + str(sender), {'id': ident, 'until': utc() + 900})
            except sqlite3.IntegrityError:
                return 'This image message was already received. Use /manual_queue.'
            self.has_items = True
            caption = arg if name == '/manual' else text
            if caption:
                return self._queue(self.db.get(ident), caption)
            return 'Manual image ' + ident + ' received. Send its text within 15 minutes; it has not been submitted yet.'
        pending = self.db.state('manual_pending:' + str(sender), {})
        if pending.get('until', 0) > utc() and (name == '/manual' or not text.startswith('/')):
            row = self.db.get(pending.get('id'))
            if row and row['kind'] == KIND and row['payload'].get('sender') == sender:
                return self._queue(row, arg if name == '/manual' else text)
        return 'Use /manual, then send your image and text. /manual_status and /manual_queue show only manual posts.'

    def control(self, name, arg, sender):
        with self.db.transaction():
            if name == '/manual_cancel' and not arg:
                self.db.set('manual_armed:' + str(sender), 0)
                pending = self.db.state('manual_pending:' + str(sender), {})
                self.db.set('manual_pending:' + str(sender), {})
                arg = pending.get('id', '')
                if not arg:
                    return 'Manual composition cancelled.'
            parts = arg.split()
            row = self.db.get(parts[0]) if parts else None
            if not row or row['kind'] != KIND or row['payload'].get('sender') != sender:
                raise ValueError('Unknown manual post; use /manual_queue')
            if name == '/manual_preview':
                return row['id'] + ' [' + row['status'] + ']\n' + row['payload'].get('body', 'Awaiting text')
            p = row['payload']
            if name == '/manual_reconcile':
                if len(parts) != 3 or parts[1] not in ('posted', 'not_posted') or parts[2] != 'CONFIRM' or row['status'] != UNCERTAIN:
                    raise ValueError('Inspect Square, then /manual_reconcile ID posted|not_posted CONFIRM')
                if parts[1] == 'posted':
                    self.db.update(row['id'], status=PUBLISHED)
                else:
                    self.refund(p)
                    p.pop('submitted_at', None)
                    self.db.update(row['id'], status=REVIEW, payload=p)
                return 'Manual outcome reconciled. A not-posted item needs /manual_retry ID.'
            if name == '/manual_cancel':
                if row['status'] not in (WAITING, QUEUED, REVIEW):
                    raise ValueError('Cannot cancel an active, confirmed or uncertain submission')
                self.refund(p)
                self.db.update(row['id'], status=CANCELLED, payload=p)
                return 'Manual post cancelled; automatic posts unchanged.'
            if name == '/manual_retry':
                if row['status'] != REVIEW or p.get('submitted_at'):
                    raise ValueError('Manual retry requires a known-not-submitted review item')
                aligned_text(p.get('body', ''), self.s)
                self.reserve(self.day())
                p.update(manual_budget_day=self.day(), reserved_at=utc(), authorized_at=utc())
                p.pop('rejection_code', None)
                p.pop('image_pending', None)
                self.db.update(row['id'], status=QUEUED, payload=p, clear_due=True)
        self.kick()
        return 'Manual post queued for an explicit retry.'

    def status(self):
        return {'daily_cap': CAP, 'day': self.day(), 'used_or_reserved': self.db.budget_used(self.day(), 'manual_posts'),
                'automatic_cap_and_spacing': 'unchanged', 'image_source': 'user_supplied',
                'pending': self.db.count(KIND, [QUEUED, MEDIA, SENDING]), 'last': dict(self.last)}

    def start(self):
        if self.s.telegram_token and (self.task is None or self.task.done()):
            self.task = asyncio.create_task(self.run())

    def kick(self):
        self.start()
        self.wake.set()

    async def close(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try: await self.task
            except asyncio.CancelledError: pass
        if self.owns_client and self.client:
            await self.client.aclose()

    def recover(self):
        with self.db.transaction():
            if not self.owns():
                return False
            rows = self.db.list(KIND, [MEDIA, SENDING], limit=100)
            for row in rows:
                self.db.update(row['id'], status=UNCERTAIN if row['status'] == SENDING else QUEUED)
            self.has_items = self.db.count(KIND) > 0
        self.recovered = True
        return True

    async def run(self):
        while not self.desk.stopping:
            delay = POLL_SECONDS
            try:
                if not self.desk.lease_owned:
                    self.recovered = False
                else:
                    ready = self.recovered or self.recover()
                    if ready:
                        rows = list(reversed(self.db.list(KIND, [QUEUED], limit=20)))
                        if rows:
                            await self.process(rows[0]['id'])
                        else:
                            self.wake.clear()
                            await self.wake.wait()
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.recovered = False
                logging.getLogger('square_desk').warning('Manual post lane unavailable: %s', type(error).__name__)
                delay = 15
            self.wake.clear()
            try: await asyncio.wait_for(self.wake.wait(), delay)
            except asyncio.TimeoutError: pass

    def gate(self, row):
        if not self.owns(): return 'Worker lease not held'
        if self.db.state('paused', False) or self.db.state('emergency_stop', False): return 'Publishing paused'
        if self.db.state('publisher_blocked', False): return 'Publisher restricted'
        if utc() < self.db.state('publish_retry_after', 0): return 'Account publication cooldown'
        if utc() < self.db.state('image_retry_after', 0): return 'Account image cooldown'
        reason = Compliance(self.s, self.db).live_gate()
        if reason: return reason
        return account_gate(self.s, self.db, utc(), row['id'])

    def automatic_due(self):
        for row in self.db.list('draft', ['queued', 'preparing_media', 'sending'], limit=100):
            if row['status'] != 'queued' or (row.get('due') or 0) <= utc() + 30:
                return True
        return bool(self.desk.image_upload_task and not self.desk.image_upload_task.done())

    def duplicate(self, ident, binding):
        # Receipt ledgers stay useful beyond the newest 1,000 posts. Decode
        # compressed recent history and compact old ledgers using the repository.
        with self.db.lock:
            rows = self.db.conn.execute('SELECT id,kind,payload FROM entities WHERE kind=? AND id<>? AND status IN (?,?,?)',
                                       (KIND, ident, PUBLISHED, SENDING, UNCERTAIN))
            return any(self.db.unpack(row)['payload'].get('manual_binding') == binding for row in rows)

    def checkpoint(self, ident, image_hash, pending):
        with self.db.transaction():
            row = self.db.get(ident)
            if not self.owns() or not row or row['status'] != MEDIA or row['payload'].get('image_hash') != image_hash:
                raise WithheldPublication()
            p = row['payload']; p['image_pending'] = pending
            self.db.update(ident, payload=p)

    def prepared(self, ident, image_hash, upload):
        # Keep a verified URL when an automatic slot or pause defers submission;
        # do not expire a completed upload as an unfinished processing ticket.
        with self.db.transaction():
            row = self.db.get(ident)
            if not self.owns() or not row or row['status'] != MEDIA or row['payload'].get('image_hash') != image_hash:
                raise WithheldPublication()
            p = row['payload']; p['image_upload'] = upload
            p.pop('image_pending', None)
            self.db.update(ident, payload=p)

    def before_submit(self, ident, binding):
        with self.db.transaction():
            row = self.db.get(ident)
            if not row or row['kind'] != KIND or row['status'] != MEDIA:
                raise WithheldPublication()
            if not self.owns():
                raise WithheldPublication()
            p = row['payload']
            reason = self.gate(row)
            valid = (p.get('body') == p.get('original_text') and
                     sha256((p.get('body', '') + '\n' + p.get('image_hash', '')).encode()).hexdigest() == binding)
            duplicate = self.duplicate(ident, binding)
            if not valid or duplicate:
                self.refund(p)
                p.pop('submitted_at', None)
                p['rejection_code'] = 'manual_binding_or_duplicate'
                self.db.update(ident, status=REVIEW, payload=p)
                withheld = True
            elif reason or self.automatic_due() or p.get('manual_budget_day') != self.day():
                self.db.update(ident, status=QUEUED)
                withheld = True
            else:
                aligned_text(p['body'], self.s)
                p['submitted_at'] = utc()
                p.pop('image_pending', None)
                self.db.update(ident, status=SENDING, payload=p)
                withheld = False
        if withheld: raise WithheldPublication()

    async def process(self, ident):
        row = self.db.get(ident)
        if not row or row['kind'] != KIND or row['status'] != QUEUED:
            return
        p = row['payload']
        if p.get('image_pending', {}).get('next_check_at', 0) > utc(): return
        with self.db.transaction():
            row = self.db.get(ident); p = row['payload']
            if row['status'] != QUEUED or not self.owns(): return
            if p.get('manual_budget_day') != self.day():
                self.refund(p)
                self.db.update(ident, status=REVIEW, payload=p)
                self.last = {'id': ident, 'reason': 'Day changed; explicit manual retry required'}
                return
            reason = self.gate(row)
            if reason or self.automatic_due():
                self.last = {'id': ident, 'reason': reason or 'Automatic publication has priority'}
                return
            self.db.update(ident, status=MEDIA)
        try:
            if p.get('body') != p.get('original_text') or aligned_text(p.get('body', ''), self.s) != p.get('original_text'):
                raise ValueError('Saved manual text differs from the text you authorized; start a new /manual post')
            if self.client is None:
                self.client = httpx.AsyncClient(timeout=15, follow_redirects=False, trust_env=False,
                                               limits=httpx.Limits(max_connections=2, max_keepalive_connections=1))
            if not p.get('image'):
                raw = await download_image(self.client, self.s.telegram_token, p['file_id'])
                png = await asyncio.to_thread(normalized_png, raw)
                filename = await asyncio.to_thread(save_png, self.s.artifacts, png)
                with self.db.transaction():
                    current = self.db.get(ident)
                    if not self.owns() or current['status'] != MEDIA:
                        raise WithheldPublication()
                    hashed = persist_png(self.db, self.s.artifacts, filename)
                    p = current['payload']; p.update(image=filename, image_hash=hashed)
                    p['manual_binding'] = sha256((p['body'] + '\n' + hashed).encode()).hexdigest()
                    if self.duplicate(ident, p['manual_binding']):
                        raise ValueError('This exact manual image and text already has a publication or uncertain submission')
                    self.db.update(ident, payload=p)
            adapter = ManualAdapter(self.s.square_key, self.client, self.s.artifacts, self.db,
                                    self.s.image_upload_timeout_seconds)
            receipt = await adapter.send(p, before_submit=lambda: self.before_submit(ident, p['manual_binding']),
                                         checkpoint=lambda state: self.checkpoint(ident, p['image_hash'], state),
                                         on_prepared=lambda upload: self.prepared(ident, p['image_hash'], upload))
            with self.db.transaction():
                current = self.db.get(ident)
                if not self.owns() or current['status'] != SENDING:
                    raise WithheldPublication()
                saved = current['payload']; saved['receipt'] = receipt
                self.db.update(ident, status=PUBLISHED, payload=saved)
                self.db.log('MANUAL', 'Manual image+text publication confirmed', ident)
            self.last = {'id': ident, 'status': 'published', 'at': utc()}
            self.notify('Manual publication ' + ident + ': published with your image. Square receipt confirmed.', 'published:' + ident)
        except MediaPending as pending:
            with self.db.transaction():
                current = self.db.get(ident)
                if self.owns() and current and current['status'] == MEDIA:
                    saved = current['payload']; saved['image_pending'] = pending.state
                    self.db.update(ident, status=QUEUED, payload=saved)
                else:
                    self.recovered = False
        except AmbiguousPublication:
            with self.db.transaction():
                current = self.db.get(ident)
                if not self.owns() or not current or current['status'] != SENDING:
                    self.recovered = False
                    return
                self.db.update(ident, status=UNCERTAIN)
            self.last = {'id': ident, 'status': 'uncertain'}
            self.notify('Manual publication ' + ident + ': outcome uncertain. Inspect Square before /manual_reconcile; automatic retry is disabled.', 'uncertain:' + ident)
        except WithheldPublication:
            # Re-read private media/sending state after a lease change or a
            # failed checkpoint. A content POST is never blindly replayed.
            self.recovered = False
        except (RejectedPublication, MediaError, ValueError, OSError) as error:
            code = error.code if isinstance(error, (RejectedPublication, MediaError)) else 'manual_input_invalid'
            with self.db.transaction():
                current = self.db.get(ident)
                if not self.owns() or not current or current['status'] not in (MEDIA, SENDING):
                    self.recovered = False
                    return
                saved = current['payload']; saved['rejection_code'] = code
                saved.pop('submitted_at', None)
                self.refund(saved)
                self.db.update(ident, status=REVIEW, payload=saved)
                if code in ('429', '220009'):
                    self.db.set('publish_retry_after', utc() + max(getattr(error, 'retry_after', 0), 3600))
                elif code == '220014':
                    self.db.set('image_retry_after', utc() + 3600)
                elif isinstance(error, RejectedPublication) and code not in DRAFT_REJECTIONS:
                    self.db.set('publisher_blocked', True)
                    self.db.set('publisher_block_reason', {'code': code, 'draft_id': ident, 'at': utc()})
                self.db.log('MANUAL', 'Manual post requires review; code=' + code, ident)
            self.last = {'id': ident, 'status': 'review', 'code': code}
            detail = str(error) if isinstance(error, ValueError) else code
            self.notify('Manual post ' + ident + ' was not published: ' + detail + '. Its manual reservation was refunded.', 'review:' + ident)
