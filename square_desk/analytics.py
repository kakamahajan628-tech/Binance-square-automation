from datetime import datetime
from zoneinfo import ZoneInfo
from .models import utc, digest
from .tracking import statistics


class Analytics:
    def __init__(self, settings, store):
        self.s, self.db = settings, store

    def import_metrics(self, payload):
        if not isinstance(payload, dict):
            raise ValueError('Metrics must be an object')
        allowed = {'draft_id', 'as_of', 'followers', 'impressions', 'likes', 'comments', 'shares'}
        if set(payload) - allowed:
            raise ValueError('Unknown metric fields')
        if not any(k in payload for k in ('followers', 'impressions', 'likes', 'comments', 'shares')):
            raise ValueError('Supply at least one observed metric')
        at = float(payload.get('as_of', utc()))
        if not 0 < at <= utc() + 60:
            raise ValueError('Invalid metrics timestamp')
        for key in ('followers', 'impressions', 'likes', 'comments', 'shares'):
            if key in payload and (type(payload[key]) is not int or not 0 <= payload[key] <= 10**12):
                raise ValueError('Metrics must be nonnegative integer observations')
        if payload.get('draft_id'):
            draft = self.db.get(payload['draft_id'])
            if not draft or draft['kind'] != 'draft' or draft['status'] not in ('published', 'manual_published'):
                raise ValueError('Account metrics require a confirmed real publication')
        payload = {**payload, 'as_of': at, 'source': 'administrator_import'}
        ident = self.db.insert('metric', payload, 'observed', fingerprint=digest(payload))
        self.db.log('REPORT', 'Observed account metrics imported', ident)
        return ident

    def report(self, days=7):
        since = utc() - days * 86400
        drafts = self.db.list('draft', since=since, limit=10000)
        signals = self.db.list('signal', since=since, limit=10000)
        metrics = self.db.list('metric', limit=10000)
        observations = [r['payload'] for r in metrics if r['payload']['as_of'] >= since]
        followers = sorted([p for p in observations if 'followers' in p], key=lambda p: p['as_of'])
        latest = {}
        for p in sorted(observations, key=lambda p: p['as_of']):
            if p.get('draft_id'):
                latest[p['draft_id']] = p
        categories, hours, media, tickers = {}, {}, {}, {}
        measured = []
        for ident, p in latest.items():
            draft = self.db.get(ident)
            if not draft or not p.get('impressions') or not all(k in p for k in ('likes', 'comments', 'shares')):
                continue
            score = (p['likes'] + p['comments'] + p['shares']) / p['impressions']
            body = draft['payload']
            measured.append({'draft_id': ident, 'engagement_rate': score, 'impressions': p['impressions']})
            hour = datetime.fromtimestamp(body.get('submitted_at', draft['updated']), ZoneInfo(self.s.timezone)).hour
            for group, key in ((categories, body['category']), (hours, str(hour)),
                               (media, 'image' if body.get('image') else 'article' if body.get('article') else 'text'),
                               (tickers, (body.get('event') or {}).get('symbol', 'campaign'))):
                group.setdefault(key, []).append(score)
        rollup = lambda groups: {k: {'sample': len(v), 'mean_engagement_rate': sum(v) / len(v)} for k, v in groups.items()}
        return {'period_days': days, 'as_of': utc(), 'drafts': len(drafts),
                'live_publications': sum(r['status'] in ('published', 'manual_published') for r in drafts),
                'paper_publications': sum(r['status'] == 'paper_published' for r in drafts),
                'uncertain_publications': sum(r['status'] == 'uncertain' for r in drafts),
                'followers': followers[-1]['followers'] if followers else None,
                'follower_growth': followers[-1]['followers'] - followers[0]['followers'] if len(followers) >= 2 else None,
                'observed_posts': len(latest), 'categories': rollup(categories), 'posting_hours': rollup(hours),
                'media': rollup(media), 'tickers': rollup(tickers),
                'best_posts': sorted(measured, key=lambda x: -x['engagement_rate'])[:5],
                'weak_posts': sorted(measured, key=lambda x: x['engagement_rate'])[:5],
                'signals': statistics(signals),
                'metric_limit': 'Manually imported observed metrics; missing values remain null. No follower attribution inferred.'}

    def optimize(self, report):
        weights = self.db.state('category_weights', {})
        qualified = {k: v for k, v in report['categories'].items() if v['sample'] >= 5}
        if len(qualified) >= 2:
            average = sum(v['mean_engagement_rate'] for v in qualified.values()) / len(qualified)
            for key, v in qualified.items():
                direction = .05 if v['mean_engagement_rate'] > average else -.05
                weights[key] = round(min(1.2, max(.8, weights.get(key, 1) + direction)), 3)
            self.db.set('category_weights', weights)
        hours = {k: v for k, v in report['posting_hours'].items() if v['sample'] >= 5}
        self.db.set('audience_hours', hours)
        self.db.log('REPORT', 'Weekly bounded category adjustment; minimum five measured posts per group')
        return weights
