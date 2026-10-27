"""Bounded ingestion of administrator-supplied static images, without AI."""
import asyncio
from hashlib import sha256
import io
import os
import re
import tempfile

import httpx
from PIL import Image, ImageOps

MAX_INPUT_BYTES = 8_000_000
MAX_PIXELS = 8_000_000
DOWNLOAD_SECONDS = 25


def normalized_png(data):
    if not data or len(data) > MAX_INPUT_BYTES:
        raise ValueError('Manual image must be at most 8 MB')
    try:
        with Image.open(io.BytesIO(data)) as original:
            if (original.format not in ('PNG', 'JPEG', 'WEBP') or getattr(original, 'is_animated', False)
                    or original.width * original.height > MAX_PIXELS or max(original.size) > 4096):
                raise ValueError('Use a static JPG, PNG or WebP, at most 4096 px and 8 megapixels')
            original.load()
            alpha = 'A' in original.getbands() or 'transparency' in original.info
            image = ImageOps.exif_transpose(original).convert('RGBA' if alpha else 'RGB')
            for maximum in (1600, 1280, 960, 720, 640):
                copy = image.copy()
                copy.thumbnail((maximum, maximum), Image.Resampling.LANCZOS)
                copy.info.clear()
                output = io.BytesIO()
                copy.save(output, format='PNG', compress_level=6)
                result = output.getvalue()
                if len(result) <= 1_000_000:
                    return result
    except (OSError, SyntaxError, Image.DecompressionBombError):
        raise ValueError('Image could not be decoded safely') from None
    raise ValueError('Image is too large after PNG conversion; send a smaller image')


def save_png(directory, data):
    """Publish a complete file atomically; never truncate a shared chart file."""
    from .media import image_path, validate_png, MediaError
    try:
        hashed = validate_png(data)
        filename = sha256(data).hexdigest() + '.png'
        path = image_path(directory, filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            validate_png(path.read_bytes(), hashed)
            return filename
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.manual-', suffix='.tmp', delete=False) as output:
                temporary = output.name
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
        return filename
    except MediaError:
        raise ValueError('Manual image storage failed validation; no post was submitted') from None


async def download_image(client, token, file_id):
    try:
        async with asyncio.timeout(DOWNLOAD_SECONDS):
            return await _download_image(client, token, file_id)
    except TimeoutError:
        raise ValueError('Telegram image download timed out; no post was submitted') from None


async def _download_image(client, token, file_id):
    if not isinstance(file_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,512}', file_id):
        raise ValueError('Invalid Telegram image identifier')
    try:
        response = await client.post('https://api.telegram.org/bot' + token + '/getFile',
                                     json={'file_id': file_id}, timeout=10)
        try:
            data = response.json()
        except ValueError:
            raise ValueError('Telegram image lookup returned an invalid response') from None
        if not isinstance(data, dict):
            raise ValueError('Telegram image lookup returned an invalid response')
        info = data.get('result') if response.status_code == 200 and data.get('ok') else None
        if not isinstance(info, dict):
            raise ValueError('Telegram image is unavailable; send it again')
        path = info.get('file_path', '')
        if (not isinstance(path, str) or len(path) > 400 or not re.fullmatch(r'(photos|documents)/[A-Za-z0-9_./-]+', path)
                or any(part in ('', '.', '..') for part in path.split('/'))
                or type(info.get('file_size', 0)) is not int or not 0 <= info.get('file_size', 0) <= MAX_INPUT_BYTES):
            raise ValueError('Telegram image path or size is invalid')
        payload = bytearray()
        async with client.stream('GET', 'https://api.telegram.org/file/bot' + token + '/' + path,
                                 timeout=15, follow_redirects=False) as stream:
            if stream.status_code != 200:
                raise ValueError('Telegram image download failed')
            async for chunk in stream.aiter_bytes():
                payload.extend(chunk)
                if len(payload) > MAX_INPUT_BYTES:
                    raise ValueError('Manual image must be at most 8 MB')
        return bytes(payload)
    except (httpx.HTTPError, KeyError, TypeError):
        raise ValueError('Telegram image download failed; no post was submitted') from None
