from datetime import datetime
from zoneinfo import ZoneInfo
from .models import utc
from .compliance import Compliance
from .planning import PlanningStore
from copy import copy
from .media import prepared_image_url


class Scheduler:
    def __init__(self, settings, store, checker):
        self.s, self.db, self.checker = settings, store, checker
        self.policy = Compliance(settings, store)

    def allocate(self, now=None, *, ready_media_only=False):
        now = utc() if now is None else now
        db = PlanningStore(self.db)
        policy = Compliance(self.s, db)
        checker = copy(self.checker)
        checker.db = db
        if db.state('paused', False) or db.state('emergency_stop', False):
            return
        rows = db.list('draft', ['approved', 'queued'])
        # Rebuild queued slots each cycle. Priority content takes the first available
        # slot; lower priority conflicts move later without ignoring platform limits.
        for row in rows:
            db.update(row['id'], status='approved', clear_due=True)
        rows.sort(key=lambda r: (-r['payload']['priority'], r['created']))
        for row in rows:
            draft = row['payload']
            if ready_media_only and draft.get('image') and not prepared_image_url(draft, self.s.square_key):
                # An upload must not reserve a slot ahead of ready text posts.
                continue
            candidate = max(now, draft.get('not_before', now))
            if draft['category'] == 'education':
                audience = db.state('audience_hours', {})
                choices = []
                for minutes in range(24 * 60):
                    ts = candidate + minutes * 60
                    hour = datetime.fromtimestamp(ts, ZoneInfo(self.s.timezone)).hour
                    if hour in self.s.publishing_hours and str(hour) in audience:
                        choices.append((audience[str(hour)]['mean_engagement_rate'], -ts))
                if choices:
                    candidate = -max(choices)[1]
            placed = False
            for _ in range(24 * 60):
                if checker.check(draft, candidate):
                    break
                if not policy.gate(row, candidate, scheduled=True):
                    db.update(row['id'], status='queued', due=candidate)
                    placed = True
                    break
                candidate += 60
            if not placed and draft.get('event') and now - draft['event']['as_of'] > self.s.draft_max_age:
                db.update(row['id'], status='expired')

    def due(self, now=None):
        now = utc() if now is None else now
        return sorted([r for r in self.db.list('draft', ['queued']) if r['due'] is not None and r['due'] <= now],
                      key=lambda r: (-r['payload']['priority'], r['due']))

    def reschedule(self, ident, timestamp):
        row = self.db.get(ident)
        if not row or row['kind'] != 'draft' or row['status'] not in ('review', 'approved', 'queued'):
            raise ValueError('Draft cannot be rescheduled')
        date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        if date.tzinfo is None:
            date = date.replace(tzinfo=ZoneInfo(self.s.timezone))
        p = row['payload']
        p['not_before'] = date.timestamp()
        if p['not_before'] < utc():
            raise ValueError('Reschedule time must be in the future')
        self.db.update(ident, payload=p, status='review' if row['status'] == 'review' else 'approved', clear_due=True)
