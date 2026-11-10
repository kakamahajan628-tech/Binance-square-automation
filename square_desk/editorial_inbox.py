"""Verified source review slots inside existing caps; no automatic approval."""
from datetime import datetime
from zoneinfo import ZoneInfo
from .db_actor import run_db
from .models import utc, digest

CARD_VERSION = 'review-card-v31-1'
CARD_LABELS = {
    'news': 'Reviewed news context · Check the primary announcement',
    'campaign': 'Administrator supplied campaign · Verify official terms',
    'scenario_audit': 'Published scenario review · Simulated outcomes, not account performance',
}


def card_subtitle(draft):
    if draft.get('category') == 'education':
        return 'Educational research · Conditional learning, not a trade recommendation'
    return CARD_LABELS.get(draft.get('category'), 'Administrator supplied campaign · Verify official terms')


def card_binding(draft):
    return digest({k: draft.get(k) for k in ('title', 'body', 'category', 'news_id', 'campaign_id', 'scenario_review')})


def card_recipe(draft):
    from PIL import __version__ as pillow
    if draft.get('category') not in CARD_LABELS:
        raise ValueError('Unsupported reviewed source card')
    return dict(version=CARD_VERSION, title=draft['title'], subtitle=card_subtitle(draft),
                binding=card_binding(draft), as_of=utc(), pillow=pillow)


def status(store):
    news = store.list('news', ['verified'], limit=100)
    campaigns = store.list('campaign', ['active'], limit=100)
    reviews = store.list('draft', ['review'], limit=100)
    lines = ['Editorial review inbox; approvals share existing posting caps and spacing.']
    for row in news[:10]:
        ready = bool(row['payload'].get('summary'))
        lines.append(f"News {row['id']}: " + ('reviewed summary ready; /news_draft ' + row['id']
                                             if ready else 'original administrator summary missing'))
    for row in campaigns[:10]:
        lines.append('Campaign ' + row['id'] + ': /project_draft ' + row['id'])
    for row in reviews:
        if row['payload'].get('category') in ('news', 'campaign', 'scenario_audit'):
            lines.append('Review /preview ' + row['id'] + '; media and fresh approval required')
    return '\n'.join(lines) if len(lines) > 1 else lines[0] + '\nNo verified source or scenario reviews available.'


def prepare(store, settings, checker, campaigns, now=None):
    """At most one fresh news and one eligible campaign review per local day.

    Existing records only. Never copy feed text, fetch extra feeds, activate a
    project, synthesize rewards, auto-approve, or append tags to market drafts.
    """
    now = utc() if now is None else now
    if store.state('emergency_stop', False) or store.state('paused', False):
        return []
    pending = store.list('draft', ['review'], limit=50)
    if len(pending) >= 45:
        return []
    day = datetime.fromtimestamp(now, ZoneInfo(settings.timezone)).date().isoformat()
    progress = store.state('editorial_review_slots', {})
    if progress.get('day') != day:
        progress = {'day': day}
    made = []
    from .news import news_draft
    for kind, statuses in (('news', ['verified']), ('campaign', ['active'])):
        if kind in progress or any(r['payload'].get('category') == kind for r in pending):
            continue
        for row in reversed(store.list(kind, statuses, limit=100)):
            p = row['payload']
            if kind == 'news' and (not p.get('summary') or not 0 <= now-p.get('recorded_at', 0) <= 86400):
                continue
            if kind == 'campaign' and (not p.get('verified') or not p.get('start', now+1) <= now < p.get('end', 0)):
                continue
            # Source association, rather than superficial title differences,
            # prevents re-drafting the same announcement or campaign template.
            key = 'news_id' if kind == 'news' else 'campaign_id'
            if any(r['payload'].get(key) == row['id'] for r in store.list('draft', limit=1000)):
                continue
            try:
                ident = (news_draft(store, checker, row['id']) if kind == 'news'
                         else campaigns.reviewed_draft(row['id']))
            except ValueError:
                # Do not weaken word limits, terms, duplication or source gates
                # to fill an editorial slot. Show the source in /editorial.
                progress[kind] = {'source': row['id'], 'status': 'needs_review'}
            else:
                progress[kind] = {'source': row['id'], 'draft': ident, 'status': 'review'}
                made.append(ident)
            store.set('editorial_review_slots', progress)
            break
    return made


async def prepare_async(desk):
    # Memory throttle: no heartbeat-like database writes or API requests.
    if utc() < getattr(desk, '_editorial_inbox_after', 0):
        return
    desk._editorial_inbox_after = utc() + 300
    def work():
        with desk.db.transaction():
            return prepare(desk.db, desk.s, desk.content.checker, desk.campaigns)
    for ident in await run_db(desk.db, work):
        desk.telegram.notify('Verified-source draft requires review: /preview ' + ident +
                             '\nAttach media, inspect the source and approve to use an existing slot.',
                             key='editorial-review:' + ident)
