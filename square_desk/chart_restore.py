"""Rebuild only exact, still-current chart evidence; never fetch new candles."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import math

from PIL import __version__ as PILLOW_VERSION
from .models import Snapshot, digest, utc
from .media import MediaError, image_path, validate_png
from .chart_alignment import for_draft
from . import images

VERSION = 'exact-chart-replay-v1'


def snapshot_row(store, ident):
    # Read the durable pinned version, not a later RAM fetch of the same candle.
    with store.lock:
        row = store.conn.execute("SELECT * FROM entities WHERE id=? AND kind='snapshot'", (ident,)).fetchone()
    return store.unpack(row) if row else None


def recipe(plan):
    event = plan['payload']['event']
    return {'version':VERSION, 'snapshot_id':digest([event['metrics']['source'], event['symbol'], 900, event['as_of']]),
            'snapshot_digest':digest(plan['data']), 'metrics':deepcopy(event['metrics']),
            'context':deepcopy(plan.get('context') or for_draft(plan['payload'])), 'pillow':PILLOW_VERSION}


def plan(store, draft, max_age):
    try:
        event = draft.get('event')
        if not isinstance(event, dict) or draft.get('source') == 'telegram_manual':
            raise ValueError
        age = utc()-event['as_of']
        if not math.isfinite(age) or not 0 <= age <= max_age:
            raise ValueError
        ident = digest([event['metrics']['source'], event['symbol'], 900, event['as_of']])
        row = snapshot_row(store, ident)
        if not row or row['kind'] != 'snapshot':
            raise ValueError
        data = row['payload']
        snapshot = Snapshot.read(data)
        if (snapshot.symbol != event['symbol'] or snapshot.source != event['metrics']['source']
                or snapshot.quote != event['metrics'].get('quote') or snapshot.interval != 900
                or snapshot.as_of != event['as_of']):
            raise ValueError
        saved = draft.get('image_recipe')
        if saved is not None:
            if (not isinstance(saved, dict) or saved.get('version') != VERSION or saved.get('snapshot_id') != ident
                    or saved.get('snapshot_digest') != digest(data)
                    or not isinstance(saved.get('metrics'), dict) or not isinstance(saved.get('context'), dict)):
                raise ValueError
            metrics, context = saved['metrics'], saved['context']
        else:
            # Older drafts may recover only if their derived bytes still match.
            metrics, context = event['metrics'], for_draft(draft)
        image_path('.', draft['image'])  # Strict content-addressed filename.
        if draft.get('image_hash') != Path(draft['image']).stem:
            raise ValueError
        return {'snapshot':snapshot, 'metrics':deepcopy(metrics), 'context':deepcopy(context),
                'hash':draft['image_hash'], 'as_of':event['as_of']}
    except (KeyError, TypeError, ValueError, OverflowError):
        raise MediaError('media_recovery_unavailable') from None


def render(plan):
    # Late or mismatching work never writes into the approved artifact directory.
    with TemporaryDirectory(prefix='square-chart-replay-') as directory:
        filename = images.chart(plan['snapshot'], plan['metrics'], directory, context=plan['context'])
        data = image_path(directory, filename).read_bytes()
        validate_png(data, plan['hash'])
        return data
