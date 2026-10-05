from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from .models import utc


PUBLICATION_STATES = ['published', 'paper_published', 'manual_published', 'manual_ready', 'preparing_media', 'sending', 'uncertain']


class Compliance:
    def __init__(self, settings, store):
        self.s, self.db = settings, store

    def caps(self):
        override = self.db.state('policy_caps', {})
        return {'daily': min(self.s.daily_target, self.s.platform_daily_cap, override.get('daily', 100)),
                'hourly': min(self.s.hourly_cap, override.get('hourly', self.s.hourly_cap)),
                'gap': max(self.s.gap_seconds, override.get('gap', 0))}

    def window(self, timestamp):
        local = datetime.fromtimestamp(timestamp, ZoneInfo(self.s.timezone))
        return local.replace(hour=0, minute=0, second=0, microsecond=0).timestamp(), local.hour

    def gate(self, row, now=None, scheduled=False):
        now = utc() if now is None else now
        if self.db.state('paused', False) or self.db.state('emergency_stop', False):
            return 'Publishing paused'
        if self.db.state('publisher_blocked', False):
            return 'Publisher restricted; administrator must review provider error'
        start, hour = self.window(now)
        if hour not in self.s.publishing_hours:
            return 'Outside publishing window'
        cap = self.caps()
        rows = [r for r in self.db.list('draft', PUBLICATION_STATES, limit=10000) if r['id'] != row['id']]
        if scheduled:
            rows += [r for r in self.db.list('draft', ['queued']) if r['id'] != row['id']]
        times = [(r['due'] if r['status'] == 'queued' else r['payload'].get('submitted_at', r['updated'])) for r in rows]
        if sum(self.window(t)[0] == start for t in times) >= cap['daily']:
            return 'Daily publication limit reached'
        if sum(now - 3600 < t <= now for t in times) >= cap['hourly']:
            return 'Hourly publication limit reached'
        if any(abs(now - t) < cap['gap'] for t in times):
            return 'Minimum spacing conflict'
        draft = row['payload']
        if draft.get('campaign_id'):
            campaign = self.db.get(draft['campaign_id'])
            if not campaign or campaign['status'] != 'active':
                return 'Campaign inactive'
            p = campaign['payload']
            if not p['start'] <= now < p['end']:
                return 'Campaign outside active dates'
            related = [r for r in rows if r['payload'].get('campaign_id') == campaign['id']]
            if len(related) >= p['quota']:
                return 'Campaign quota reached'
            if sum(self.window(r['payload'].get('submitted_at', r['updated']))[0] == start
                   for r in rows if r['payload'].get('campaign_id')) >= self.s.campaign_daily_cap:
                return 'Campaign daily cap reached'
        if draft.get('article') and sum(r['payload'].get('article', False) and self.window(t)[0] == start
                                      for r, t in zip(rows, times)) >= self.s.article_daily_cap:
            return 'Article daily cap reached'
        return None

    def live_gate(self):
        if self.s.paper_mode or not self.s.live_enabled:
            return 'Live publication disabled'
        try:
            review_date = datetime.fromisoformat(self.s.policy_reviewed)
            reviewed = review_date.replace(tzinfo=ZoneInfo(self.s.timezone)).timestamp() if review_date.tzinfo is None else review_date.timestamp()
        except ValueError:
            return 'Policy review date missing or invalid'
        if not 0 <= utc() - reviewed <= self.s.policy_max_age_days * 86400:
            return 'Policy review expired'
        if not self.s.square_key:
            return 'Square key missing'
        return None
