"""Small, redacted chart observations. Never grants publication authority."""
import asyncio
import re
import time
from .media import MediaError


REASONS = {
    'renderer_busy': 'Previous render is still running; no additional render was queued.',
    'queue_full': 'Pending image uploads reached the configured queue limit.',
    'ineligible': 'Draft needs review or is not eligible for automatic charts.',
    'immutable': 'Draft is no longer editable.',
    'evidence_expired': 'Original chart evidence expired; create a fresh draft.',
    'evidence_missing': 'Exact original candles are unavailable; create a fresh draft.',
    'budget_exhausted': 'Daily chart generation/reservation cap reached; it counts unpublished charts too.',
    'render_timeout': 'Chart exceeded the configured render timeout; late output cannot attach.',
    'render_invalid': 'Renderer rejected the chart data.',
    'storage_io_error': 'Chart file could not be read or saved.',
    'png_invalid': 'PNG is missing, changed or outside the accepted size/format limits.',
    'worker_lease_lost': 'Worker no longer owns the draft; no attachment or ownership bypass.',
    'database_unavailable': 'Database operation did not confirm; no automatic mutation replay.',
    'cancelled': 'Render was cancelled; late output cannot attach.',
    'render_failed': 'Unexpected render failure; exception text is omitted.',
    'attached': 'Validated chart attached; upload and publication checks still apply.',
    'upload_prepared': 'Image upload prepared; content still awaits publication checks.',
    'upload_pending': 'Image preparation is pending; saved progress will resume within its recovery window, without repeating the image PUT or content POST.',
    'upload_not_prepared': 'Upload did not become ready; inspect the draft rejection and IMAGE audit.',
    'upload_failed': 'Background upload failed; inspect IMAGE audit.',
}


class ImageFailure(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def failure_code(error, stage):
    if isinstance(error, ImageFailure):
        return error.code if error.code in REASONS else 'render_failed'
    if isinstance(error, asyncio.CancelledError):
        return 'cancelled'
    if isinstance(error, asyncio.TimeoutError):
        return 'render_timeout'
    if isinstance(error, MediaError):
        return 'png_invalid'
    if stage in ('planning', 'claiming', 'attaching') and not isinstance(error, (ValueError, OSError)):
        return 'database_unavailable'
    if isinstance(error, OSError):
        return 'storage_io_error'
    if isinstance(error, (ValueError, KeyError, TypeError)):
        return 'render_invalid'
    return 'render_failed'


def outcome(ident, code, stage, started, *, budget_refunded=False):
    code = code if code in REASONS else 'render_failed'
    ident = ident if isinstance(ident, str) and re.fullmatch(r'[a-f0-9]{32}', ident) else 'unrecognized_draft'
    return {'draft_id': ident, 'at': time.time(), 'stage': stage, 'code': code,
            'reason': REASONS[code], 'seconds': round(max(0, time.monotonic() - started), 2),
            'budget_refunded': budget_refunded}
