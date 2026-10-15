"""Verified database-import gate; never an automatic database failover."""
import math
import re
from urllib.parse import urlsplit
from .models import utc


KEY = 'database_migration'


def is_aiven_url(url):
    host = (urlsplit(url).hostname or '').lower()
    return host.endswith('.aivencloud.com')


def pending(marker):
    if not isinstance(marker, dict) or marker.get('required') is not True:
        return False
    at = marker.get('verified_at')
    return not (marker.get('status') == 'verified'
                and re.fullmatch(r'[a-f0-9]{64}', str(marker.get('backup_sha256', '')))
                and isinstance(at, (int, float)) and not isinstance(at, bool)
                and math.isfinite(at) and 0 < at <= utc() + 60)


def initialize(store, required=False):
    marker = store.state(KEY, {})
    managed = required or isinstance(marker, dict) and marker.get('required') is True
    if managed and (not isinstance(marker, dict) or marker.get('required') is not True):
        store.set(KEY, {'required': True, 'status': 'needs_import', 'at': utc(),
                       'note': 'Worker standby until production history is restored and verified.'})
    return bool(managed)
