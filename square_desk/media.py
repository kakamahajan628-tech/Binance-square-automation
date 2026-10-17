"""Bounded PNG storage and Binance's documented presigned image upload flow."""
import asyncio
import base64
import hashlib
import io
import re
import math
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from PIL import Image

from .models import utc


MAX_IMAGE_BYTES = 1_000_000
PROCESSING_SECONDS = 180


class MediaPending(Exception):
    """A bounded, resumable pre-publication step; never a content receipt."""
    def __init__(self, state):
        self.state = deepcopy(state)
        super().__init__()


class MediaError(Exception):
    def __init__(self, code, retry_after=0):
        self.code, self.retry_after = code, retry_after


def image_path(directory, filename):
    if not isinstance(filename, str) or not re.fullmatch(r'[a-f0-9]{64}\.png', filename):
        raise MediaError('media_invalid')
    root = Path(directory).resolve()
    path = root / filename
    if path.is_symlink() or path.resolve().parent != root:
        raise MediaError('media_invalid')
    return path


def validate_png(data, expected_hash=None):
    if not data or len(data) > MAX_IMAGE_BYTES:
        raise MediaError('media_invalid')
    hashed = hashlib.sha256(data).hexdigest()
    if expected_hash and hashed != expected_hash:
        raise MediaError('media_changed')
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.format != 'PNG' or image.width > 2000 or image.height > 2000:
                raise MediaError('media_invalid')
            image.verify()
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError):
        raise MediaError('media_invalid') from None
    return hashed


def persist_png(store, directory, filename):
    path = image_path(directory, filename)
    if not path.is_file() or path.stat().st_size > MAX_IMAGE_BYTES:
        raise MediaError('media_missing')
    data = path.read_bytes()
    hashed = validate_png(data)
    lookup=getattr(store,'media_metadata',None)
    existing = lookup(filename) if lookup else store.get(filename)
    if existing:
        stored_hash=existing.get('sha256') if lookup else existing['payload'].get('sha256')
        if existing['kind'] != 'media' or stored_hash != hashed:
            raise MediaError('media_changed')
    else:
        store.insert('media', {'sha256': hashed, 'bytes': len(data),
                              'png_base64': base64.b64encode(data).decode('ascii')},
                     'stored', id=filename)
    return hashed


def restore_png(store, directory, filename, expected_hash=None):
    path = image_path(directory, filename)
    if path.is_file():
        if path.stat().st_size > MAX_IMAGE_BYTES:
            raise MediaError('media_invalid')
        data = path.read_bytes()
    else:
        row = store.get(filename) if store else None
        if not row or row['kind'] != 'media':
            raise MediaError('media_missing')
        p = row['payload']
        encoded = p.get('png_base64', '')
        if not isinstance(encoded, str) or len(encoded) > 4 * ((MAX_IMAGE_BYTES + 2) // 3):
            raise MediaError('media_invalid')
        try:
            data = base64.b64decode(encoded, validate=True)
        except ValueError:
            raise MediaError('media_invalid') from None
        validate_png(data, p.get('sha256'))
        validate_png(data, expected_hash)
        # Persist exactly the approved bytes; never redraw with newer candles.
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    validate_png(data, expected_hash)
    return data


def https_url(value):
    if not isinstance(value, str) or len(value) > 8192:
        raise MediaError('media_invalid_response')
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise MediaError('media_invalid_response')
    return value


def prepared_image_url(draft, key):
    prepared = draft.get('image_upload', {})
    if not isinstance(prepared, dict) or prepared.get('sha256') != draft.get('image_hash'):
        return None
    if prepared.get('key_fingerprint') != hashlib.sha256(key.encode()).hexdigest():
        return None
    at = prepared.get('at', 0)
    if not isinstance(at, (int, float)) or not 0 <= utc() - at < 3600:
        return None
    try:
        return https_url(prepared.get('url'))
    except (MediaError, ValueError):
        return None


class SquareMedia:
    BASE = 'https://www.binance.com/bapi/composite/v2/public/pgc/openApi'

    def __init__(self, key, client, timeout=30):
        self.key, self.client, self.timeout = key, client, timeout

    async def api(self, endpoint, body):
        response = await self.client.post(self.BASE + endpoint,
            headers={'X-Square-OpenAPI-Key': self.key, 'clienttype': 'binanceSkill',
                     'Content-Type': 'application/json'}, json=body)
        if response.status_code == 429:
            try:
                delay = max(60, float(response.headers.get('Retry-After', 3600)))
            except ValueError:
                delay = 3600
            raise MediaError('429', delay)
        if response.status_code in (401, 403, 418, 451):
            raise MediaError(str(response.status_code))
        if response.status_code != 200:
            raise MediaError('media_transport_failed')
        result = response.json()
        if not isinstance(result, dict):
            raise MediaError('media_invalid_response')
        code = str(result.get('code', ''))
        if code != '000000':
            raise MediaError(code if re.fullmatch(r'\d{1,12}', code) else 'media_invalid_response')
        if not isinstance(result.get('data'), dict):
            raise MediaError('media_invalid_response')
        return result['data']

    async def upload(self, filename, data, *, resume=None, checkpoint=None):
        hashed = validate_png(data)
        now = utc()
        binding = {'image': filename, 'sha256': hashed,
                   'key_fingerprint': hashlib.sha256(self.key.encode()).hexdigest()}
        if resume:
            if (not isinstance(resume, dict) or any(resume.get(k) != v for k,v in binding.items())
                    or any(type(resume.get(k)) not in (int,float) or not math.isfinite(resume[k])
                           for k in ('started_at','deadline_at','next_check_at'))
                    or not 0 <= now-resume['started_at']
                    or resume['deadline_at'] != resume['started_at']+PROCESSING_SECONDS
                    or type(resume.get('attempts')) is not int or not 0 <= resume['attempts'] <= 3
                    or type(resume.get('checks')) is not int or not 0 <= resume['checks'] <= 30
                    or resume.get('phase') not in ('presign','processing')):
                raise MediaError('media_changed')
            state = deepcopy(resume)
        else:
            state = {**binding, 'started_at': now, 'deadline_at': now+PROCESSING_SECONDS,
                     'next_check_at': now, 'attempts': 0, 'checks': 0, 'phase': 'presign'}
        if now >= state['deadline_at'] or state['checks'] >= 30:
            raise MediaError('media_processing_expired')
        if state['next_check_at'] > now:
            raise MediaPending(state)
        def save():
            if checkpoint is not None: checkpoint(deepcopy(state))
        async def bounded(operation):
            remaining = min(self.timeout, state['deadline_at']-utc())
            if remaining <= 0:
                operation.close()
                raise MediaError('media_processing_expired')
            return await asyncio.wait_for(operation, timeout=remaining)
        try:
            if state['phase'] == 'presign':
                if state['attempts'] >= 3: raise MediaError('media_transport_failed')
                state['attempts'] += 1
                save()  # Persist attempt bound before external allocation.
                response = await bounded(self.api('/image/presignedUrl', {'imageName': filename}))
                url, ticket = https_url(response.get('presignedUrl')), response.get('fileTicket')
                if isinstance(ticket,bool) or not isinstance(ticket,(str,int)) or not ticket or len(str(ticket))>4096:
                    raise MediaError('media_invalid_response')
                state.update(file_ticket=ticket, phase='processing')
                save()  # A lost PUT response/restart resumes status, never PUT.
                uploaded = await bounded(self.client.put(url, headers={'Content-Type':'image/png'}, content=data))
                if not uploaded.is_success: raise MediaError('media_transport_failed')
            ticket = state.get('file_ticket')
            if isinstance(ticket,bool) or not isinstance(ticket,(str,int)) or not ticket or len(str(ticket))>4096:
                raise MediaError('media_changed')
            state['checks'] += 1
            result = await bounded(self.api('/image/imageStatus', {'fileTicket':ticket}))
            if utc() >= state['deadline_at']:
                raise MediaError('media_processing_expired')
            if type(result.get('status')) is not int or result['status'] not in (0,1,2):
                raise MediaError('media_invalid_response')
            if result['status'] == 1: return https_url(result.get('imageUrl'))
            if result['status'] == 2: raise MediaError('media_processing_failed')
            state['next_check_at'] = utc()+3
        except (httpx.TimeoutException, asyncio.TimeoutError):
            state['next_check_at'] = utc()+10
        except MediaError as error:
            if error.code != 'media_transport_failed': raise
            state['next_check_at'] = utc()+10
        except (httpx.HTTPError, ValueError, TypeError):
            # Status transport failures are safe to recheck with the same ticket.
            # Malformed API payloads are not interpreted as ready.
            state['next_check_at'] = utc()+10
        if utc() >= state['deadline_at'] or state['checks'] >= 30:
            raise MediaError('media_processing_expired')
        if state['phase'] == 'presign' and state['attempts'] >= 3:
            raise MediaError('media_transport_failed')
        raise MediaPending(state)
