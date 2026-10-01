from datetime import datetime
import json
import re
from zoneinfo import ZoneInfo
from .models import utc, digest


def parse_campaign(text, timezone='Asia/Kolkata'):
    if len(text) > 20000:
        raise ValueError('Campaign input too long')
    try:
        p = json.loads(text)
    except json.JSONDecodeError:
        p = {}
        aliases = {'project name': 'name', 'start date': 'start', 'end date': 'end',
                   'required tags': 'required', 'required hashtags': 'required', 'forbidden claims': 'forbidden',
                   'required number of posts': 'quota', 'description': 'terms'}
        for line in text.splitlines():
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            key = aliases.get(key.strip().lower(), key.strip().lower())
            if key in ('required', 'forbidden') and key in p:
                p[key] += ',' + value.strip()
            else:
                p[key] = value.strip()
    if not isinstance(p, dict):
        raise ValueError('Campaign must be an object or key: value lines')
    if not p.get('name') or not p.get('terms') or not p.get('start') or not p.get('end'):
        raise ValueError('Campaign requires name, terms, start and end; no inferred dates')
    for key in ('start', 'end'):
        date = datetime.fromisoformat(str(p[key]).replace('Z', '+00:00'))
        if date.tzinfo is None:
            date = date.replace(tzinfo=ZoneInfo(timezone))
        p[key] = date.timestamp()
    if p['end'] <= p['start']:
        raise ValueError('End must follow start')
    p['quota'] = int(p.get('quota', 3))
    p['article_quota'] = int(p.get('article_quota', 0))
    p['image_quota'] = int(p.get('image_quota', 0))
    if not 1 <= p['quota'] <= 100 or not 0 <= p['article_quota'] <= 10 or not 0 <= p['image_quota'] <= 100:
        raise ValueError('Campaign quotas outside limits')
    for key in ('required', 'forbidden'):
        value = p.get(key, [])
        p[key] = [x.strip() for x in value.split(',') if x.strip()] if isinstance(value, str) else list(value)
        if any(not isinstance(x, str) or len(x) > 200 for x in p[key]):
            raise ValueError('Invalid campaign requirement')
    p['name'], p['terms'] = str(p['name'])[:200], str(p['terms'])[:12000]
    p['verified'] = False
    return p


class CampaignManager:
    def __init__(self, settings, store, content):
        self.s, self.db, self.content = settings, store, content

    def add(self, text):
        p = parse_campaign(text, self.s.timezone)
        return self.db.insert('campaign', p, 'review', fingerprint=digest(p))

    def activate(self, ident):
        row = self.db.get(ident)
        if not row or row['kind'] != 'campaign' or row['payload']['end'] <= utc():
            raise ValueError('Invalid or expired campaign')
        p = row['payload']
        p['verified'] = True
        self.db.update(ident, status='active', payload=p)
        self.db.log('PROJECT', 'Administrator verified supplied terms and activated campaign', ident)

    def count(self, ident):
        return sum(1 for r in self.db.list('draft', ['published', 'paper_published', 'manual_published', 'uncertain'], limit=10000)
                   if r['payload'].get('campaign_id') == ident)

    def expire(self):
        for row in self.db.list('campaign', ['active', 'paused', 'review']):
            if row['payload']['end'] <= utc():
                self.db.update(row['id'], status='expired')
        for row in self.db.list('draft', ['review', 'approved', 'queued']):
            cid = row['payload'].get('campaign_id')
            if cid:
                campaign = self.db.get(cid)
                if not campaign or campaign['status'] in ('expired', 'ended'):
                    self.db.update(row['id'], status='expired')

    def edit(self, ident, text):
        row = self.db.get(ident)
        if not row or row['kind'] != 'campaign':
            raise ValueError('Unknown campaign')
        self.db.update(ident, payload=parse_campaign(text, self.s.timezone), status='review')
        for draft in self.db.list('draft', ['review', 'approved', 'queued']):
            if draft['payload'].get('campaign_id') == ident:
                self.db.update(draft['id'], status='rejected')
        self.db.log('PROJECT', 'Terms changed; pending drafts invalidated', ident)

    async def draft(self, ident):
        row = self.db.get(ident)
        if not row or row['kind'] != 'campaign' or row['status'] != 'active':
            raise ValueError('Verify and activate the project first')
        p = row['payload']
        if not p['start'] <= utc() < p['end'] or self.count(ident) >= p['quota']:
            raise ValueError('Campaign expired, not started, or quota reached')
        # Supplied terms are never promoted as independently verified benefits.
        body = ('Sponsored campaign: ' + p['name'] + '\n\nAdministrator-supplied campaign terms:\n' + p['terms'] +
                '\n\n' + ' '.join(p['required']) + '\n\nReview the official campaign terms before participating. '
                'Eligibility, deadlines and rewards depend on those terms. No reward or investment return is guaranteed.')
        draft = {'title': p['name'], 'body': body, 'campaign_id': ident, 'article': False,
                 'category': 'campaign', 'priority': 80, 'risk': 'high', 'human_review_required': True,
                 'generated_by': 'supplied_terms', 'image': None, 'approved_at': None}
        reasons = self.content.checker.check(draft)
        duplicate = self.content.checker.duplicate(draft)
        if duplicate:
            reasons.append(duplicate)
        if reasons:
            raise ValueError('; '.join(reasons) + '. Adjust terms or use /edit with a reviewed original draft.')
        return self.db.insert('draft', draft, 'review', fingerprint=digest({'campaign': ident, 'body': body}))
