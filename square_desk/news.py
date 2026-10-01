"""Operator-curated primary source records. Rumors cannot become publication facts."""
from urllib.parse import urlsplit
from .models import digest, utc
from email.utils import parsedate_to_datetime
import asyncio
import sqlite3
import xml.etree.ElementTree as ET
import httpx


def add_source_record(store, payload):
    if not isinstance(payload, dict) or set(payload) - {'title', 'summary', 'source_url', 'confirmation_url', 'verification'}:
        raise ValueError('Invalid news source record')
    if not all(isinstance(payload.get(k), str) and payload[k].strip() for k in ('title', 'summary', 'source_url')):
        raise ValueError('Title, summary and primary source URL required')
    for key in ('source_url', 'confirmation_url'):
        if payload.get(key):
            url = urlsplit(payload[key])
            if url.scheme != 'https' or not url.hostname or url.username or url.password:
                raise ValueError('Source records require HTTPS URLs without credentials')
    verification = payload.get('verification', 'unconfirmed')
    if verification not in ('verified', 'likely', 'unconfirmed', 'rumor'):
        raise ValueError('Invalid verification classification')
    if len(payload['summary']) > 5000:
        raise ValueError('Use an original summary, not a full third-party article')
    record = {**payload, 'verification': verification, 'verified_by': 'administrator', 'recorded_at': utc()}
    return store.insert('news', record, verification, fingerprint=digest(record))


class NewsMonitor:
    """Conditional RSS polling. Feed observations stay unconfirmed until reviewed."""
    def __init__(self, settings, store, client):
        self.s, self.db, self.client = settings, store, client

    async def poll(self):
        for url in self.s.news_feeds:
            key = 'rss:' + digest(url)
            previous = self.db.state(key, {})
            if utc() - previous.get('checked_at', 0) < 3600:
                continue
            headers = {}
            if previous.get('etag'):
                headers['If-None-Match'] = previous['etag']
            if previous.get('modified'):
                headers['If-Modified-Since'] = previous['modified']
            state = {'checked_at': utc(), 'etag': previous.get('etag'), 'modified': previous.get('modified')}
            try:
                async with self.client.stream('GET', url, headers=headers) as response:
                    if response.status_code == 304:
                        self.db.set(key, state)
                        continue
                    if response.status_code != 200:
                        self.db.log('DATA', 'Configured news feed unavailable: HTTP ' + str(response.status_code))
                        self.db.set(key, state)
                        continue
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > 512000:
                            raise ValueError('Feed exceeds memory limit')
                    state.update(etag=response.headers.get('etag'), modified=response.headers.get('last-modified'))
                if b'<!DOCTYPE' in body.upper() or b'<!ENTITY' in body.upper():
                    raise ValueError('Unsafe XML declaration')
                root = ET.fromstring(body)
                # RSS only: unsupported formats fail explicitly instead of being
                # interpreted as reliable records.
                for item in root.findall('./channel/item')[:30]:
                    title, link = item.findtext('title', ''), item.findtext('link', '')
                    if not title or urlsplit(link).scheme != 'https':
                        continue
                    published = item.findtext('pubDate')
                    at = parsedate_to_datetime(published).timestamp() if published else None
                    if at is not None and not 0 <= utc() - at <= 7 * 86400:
                        continue
                    record = {'title': title[:250], 'source_url': link, 'feed_url': url,
                              'summary': '', 'verification': 'unconfirmed', 'recorded_at': utc(),
                              'published_at': at, 'source_type': 'configured_primary_feed'}
                    try:
                        self.db.insert('news', record, 'unconfirmed', fingerprint=digest({'link': link}))
                    except sqlite3.IntegrityError:
                        pass
                self.db.set(key, state)
            except (httpx.HTTPError, ValueError, ET.ParseError, TypeError):
                self.db.set(key, state)
                self.db.log('DATA', 'News feed unavailable or malformed; no news publication generated')


def news_draft(store, checker, ident):
    row = store.get(ident)
    if not row or row['kind'] != 'news' or row['status'] != 'verified':
        raise ValueError('Verify the primary source and supply an original summary first')
    p = row['payload']
    if not p.get('summary'):
        raise ValueError('Provide an original administrator summary; feed text is never reposted')
    draft = {'title': p['title'], 'body': p['summary'] + '\n\nPrimary source: ' + p['source_url'],
             'category': 'news', 'priority': 90, 'risk': 'high', 'article': False,
             'news_id': ident, 'news_verification': 'verified', 'human_review_required': True,
             'generated_by': 'administrator_summary', 'approved_at': None, 'image': None}
    errors = checker.check(draft)
    duplicate = checker.duplicate(draft)
    if duplicate:
        errors.append(duplicate)
    if errors:
        raise ValueError('; '.join(errors))
    return store.insert('draft', draft, 'review', fingerprint=digest({'news_id': ident, 'summary': p['summary']}))
