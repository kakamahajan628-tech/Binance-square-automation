"""Explicit, backup-verified maintenance; never part of the publishing loop."""
import base64
import hashlib
import json
from pathlib import Path
import uuid

from .database_transfer import read_backup, TransferError
from .media import MAX_IMAGE_BYTES, MediaError, image_path, validate_png
from .models import utc
from .retention import inventory

CONFIRM = 'PURGE_BACKED_UP_LEGACY_MEDIA'


def backup_index(path):
    result = {}
    # Consume to EOF before returning: footer/checksum validation is mandatory.
    for record in read_backup(path):
        if record['type'] != 'row' or record['table'] != 'entities':
            continue
        values = record['values']
        if values[1] != 'media':
            continue
        payload = json.loads(values[7])
        if 'png_base64' in payload:
            if len(result) >= 10000 or values[0] in result:
                raise ValueError('Legacy backup index exceeds bounds or contains duplicates')
            result[values[0]] = hashlib.sha256(values[7].encode()).hexdigest()
    return result


def cleanup(store, backup, *, apply=False, confirm='', now=None):
    if apply and confirm != CONFIRM:
        raise ValueError('Explicit legacy-media purge confirmation required')
    indexed = backup_index(backup)
    now = utc() if now is None else now
    # Maintenance never displaces a running worker or modifies its lease.
    if store.state('worker_lease', {}).get('until', 0) > now:
        raise ValueError('Stop the application worker before legacy image maintenance')
    protected = inventory(store)
    planned, skipped = [], 0
    cursor = ''
    while True:
        with store.lock:
            rows = list(store.conn.execute("SELECT id,payload FROM entities WHERE kind='media' AND id>? ORDER BY id LIMIT 8", (cursor,)))
        if not rows: break
        cursor = rows[-1]['id']
        for row in rows:
            p = json.loads(row['payload'])
            if 'png_base64' not in p: continue
            if row['id'] in protected:
                skipped += 1
                continue
            expected = hashlib.sha256(row['payload'].encode()).hexdigest()
            if indexed.get(row['id']) != expected:
                raise ValueError('A legacy image is missing or changed in the verified backup; no purge started')
            try:
                image_path('.', row['id'])
                encoded = p['png_base64']
                if not isinstance(encoded, str) or len(encoded) > 4*((MAX_IMAGE_BYTES+2)//3): raise ValueError
                data = base64.b64decode(encoded, validate=True)
                hashed = validate_png(data, p.get('sha256'))
                if hashed != Path(row['id']).stem: raise ValueError
            except (ValueError, MediaError):
                skipped += 1
                continue
            planned.append((row['id'], expected, {'sha256':hashed,'bytes':len(data),'storage':'disk'}))
            if len(planned) > 10000: raise ValueError('Legacy maintenance plan exceeds bounds')
    changed = 0
    if apply:
        owner = 'media-maintenance-'+uuid.uuid4().hex
        if not store.acquire_lease(owner, 120):
            raise ValueError('Worker lease unavailable; no purge started')
        try:
            for ident, expected, metadata in planned:
                if not store.acquire_lease(owner, 120): raise ValueError('Maintenance lease lost')
                with store.transaction():
                    lease = store.state('worker_lease', {})
                    if lease.get('owner') != owner or lease.get('until',0) <= utc():
                        raise ValueError('Maintenance lease lost')
                    row = store.conn.execute("SELECT payload FROM entities WHERE id=? AND kind='media'", (ident,)).fetchone()
                    if not row or hashlib.sha256(row[0].encode()).hexdigest() != expected:
                        raise ValueError('Legacy row changed after planning; remaining purge stopped')
                    if ident in inventory(store):
                        raise ValueError('Legacy image became protected; remaining purge stopped')
                    store.update(ident, payload=metadata)
                    changed += 1
        finally:
            store.release_lease(owner)
    return {'mode':'apply' if apply else 'plan','eligible':len(planned),'changed':changed,'protected_or_invalid_skipped':skipped,
            'historical_journal':'Unchanged; normal verified checkpoint compaction still applies'}


def main(argv=None):
    import argparse
    from .config import Settings
    from .store import Store
    parser = argparse.ArgumentParser(description='Offline legacy image cleanup after a verified full app backup. Stop the bot first.')
    parser.add_argument('action', choices=('plan','apply'))
    parser.add_argument('--backup', required=True)
    parser.add_argument('--confirm', default='')
    args = parser.parse_args(argv)
    store = None
    try:
        # Reject damaged backup / missing confirmation before connecting anywhere.
        backup_index(args.backup)
        if args.action == 'apply' and args.confirm != CONFIRM: raise ValueError('Purge confirmation required')
        store = Store(Settings.from_env().database)
        print(json.dumps(cleanup(store,args.backup,apply=args.action=='apply',confirm=args.confirm),indent=2))
        return 0
    except (ValueError, TransferError, OSError):
        print('Maintenance stopped: verify backup, confirmation and stopped-worker state. No credentials or row data exposed.')
        return 1
    except Exception:
        print('Maintenance unavailable: database/writer authority must be current. No automatic retries.')
        return 1
    finally:
        if store is not None: store.close()
