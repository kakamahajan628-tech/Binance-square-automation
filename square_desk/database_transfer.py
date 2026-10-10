"""Streaming, consistent PostgreSQL export and guarded empty-target restore.

No external content publishing, no automatic provider switching and no DSNs
inside backups or diagnostics. Uses the existing psycopg dependency.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit

from .models import utc, stamp
from .migration import KEY
from .store import SCHEMA


FORMAT = 'square-desk-postgres-jsonl-v1'
COLUMNS = {
    'entities': ('id', 'kind', 'status', 'created', 'updated', 'due', 'fingerprint', 'payload'),
    'state': ('key', 'value'),
    'audit': ('id', 'at', 'category', 'correlation', 'message'),
    'budgets': ('day', 'category', 'used'),
}
ORDER = {'entities': 'id', 'state': 'key', 'audit': 'id', 'budgets': 'day,category'}
CONTROL_KEYS = {KEY, 'worker_lease', 'emergency_stop'}
MAX_LINE = 8_000_000
MAX_UNCOMPRESSED = 2_000_000_000
RECOVERY_STATES = {'sending': 'uncertain', 'preparing_media': 'approved',
                   'uploading_image': 'approved', 'chart_rendering': 'review', 'ai_image_generating': 'review'}


class TransferError(ValueError):
    pass


def line(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                       separators=(',', ':')) + '\n').encode('utf-8')


def normalized(record):
    if record['table'] == 'state' and record['values'][0] in CONTROL_KEYS:
        return None
    result = {'type': 'row', 'table': record['table'], 'values': list(record['values'])}
    if result['table'] == 'entities':
        result['values'][2] = RECOVERY_STATES.get(result['values'][2], result['values'][2])
    return result


def validate_record(record):
    if (not isinstance(record, dict) or set(record) != {'type', 'table', 'values'}
            or record.get('type') != 'row' or record.get('table') not in COLUMNS
            or not isinstance(record.get('values'), list)
            or len(record['values']) != len(COLUMNS[record['table']])):
        raise TransferError('Backup row invalid')
    if any(v is not None and type(v) not in (str, int, float) for v in record['values']):
        raise TransferError('Backup value invalid')
    if record['table'] in ('entities', 'state'):
        try:
            json.loads(record['values'][-1])
        except (ValueError, TypeError):
            raise TransferError('Backup application JSON invalid') from None


def read_backup(path):
    """Bounded iterator. Checks the footer and both digests before finishing."""
    hashed, data_hash = hashlib.sha256(), hashlib.sha256()
    counts = dict.fromkeys(COLUMNS, 0)
    header, total, ended, prior = None, 0, False, -1
    try:
        with gzip.open(path, 'rb') as source:
            while True:
                raw = source.readline(MAX_LINE + 1)
                if not raw:
                    break
                total += len(raw)
                if ended or len(raw) > MAX_LINE or total > MAX_UNCOMPRESSED:
                    raise TransferError('Backup exceeds bounds or has trailing data')
                record = json.loads(raw)
                if header is None:
                    if (not isinstance(record, dict) or set(record) != {'type', 'format', 'schema_version', 'created_at'}
                            or record.get('type') != 'header' or record.get('format') != FORMAT
                            or type(record.get('schema_version')) is not int or record['schema_version'] != 1
                            or not isinstance(record.get('created_at'), str)):
                        raise TransferError('Unsupported backup format or schema')
                    try:
                        at = datetime.fromisoformat(record['created_at'])
                        if at.tzinfo is None or at.utcoffset().total_seconds() != 0:
                            raise ValueError
                    except ValueError:
                        raise TransferError('Backup creation timestamp invalid') from None
                    header = record
                    hashed.update(raw)
                elif record.get('type') == 'end':
                    if (set(record) != {'type', 'counts', 'sha256', 'data_sha256'}
                            or record.get('counts') != counts or record.get('sha256') != hashed.hexdigest()
                            or record.get('data_sha256') != data_hash.hexdigest()):
                        raise TransferError('Backup checksum or row count mismatch')
                    ended = True
                    yield record
                    continue
                else:
                    validate_record(record)
                    index = list(COLUMNS).index(record['table'])
                    if index < prior:
                        raise TransferError('Backup table order invalid')
                    prior = index
                    counts[record['table']] += 1
                    hashed.update(raw)
                    canonical = normalized(record)
                    if canonical is not None:
                        data_hash.update(line(canonical))
                yield record
        if not header or not ended:
            raise TransferError('Backup incomplete')
    except TransferError:
        raise
    except (OSError, EOFError, ValueError, TypeError, AttributeError, UnicodeError):
        raise TransferError('Backup unreadable or corrupt') from None


def inspect_backup(path):
    header, footer, real_publications = None, None, 0
    controls = {}
    for record in read_backup(path):
        if record['type'] == 'header':
            header = record
        elif record['type'] == 'end':
            footer = record
        elif record['table'] == 'entities' and record['values'][1] == 'draft':
            if record['values'][2] in ('published', 'manual_published', 'sending', 'uncertain'):
                real_publications += 1
        elif record['table'] == 'state' and record['values'][0] in ('paused', 'emergency_stop', 'publisher_blocked'):
            controls[record['values'][0]] = bool(json.loads(record['values'][1]))
    return {**footer, 'format': header['format'], 'created_at': header.get('created_at'),
            'real_publication_records': real_publications, 'source_controls': controls}


def records(connection):
    for table, columns in COLUMNS.items():
        # Server-side cursors prevent loading the complete database into RAM.
        with connection.cursor(name='square_export_' + table) as cursor:
            cursor.itersize = 32
            cursor.execute('SELECT ' + ','.join(columns) + ' FROM public.' + table + ' ORDER BY ' + ORDER[table])
            for values in cursor:
                yield {'type': 'row', 'table': table, 'values': list(values)}


def write_backup(connection, path):
    target = Path(path).resolve()
    partial = target.with_name(target.name + '.partial')
    if target.exists() or partial.exists():
        raise TransferError('Backup output already exists; choose a new filename')
    target.parent.mkdir(parents=True, exist_ok=True)
    hashed, data_hash = hashlib.sha256(), hashlib.sha256()
    counts = dict.fromkeys(COLUMNS, 0)
    header = {'type': 'header', 'format': FORMAT, 'schema_version': 1, 'created_at': stamp()}
    created = False
    try:
        with connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            schema = connection.execute("SELECT value FROM public.state WHERE key='schema_version'").fetchone()
            if not schema or json.loads(schema[0]) != 1:
                raise TransferError('Source application schema is unavailable or unsupported')
            with partial.open('xb') as output:
                created = True
                with gzip.GzipFile(fileobj=output, mode='wb') as zipped:
                    raw = line(header)
                    zipped.write(raw)
                    hashed.update(raw)
                    for record in records(connection):
                        validate_record(record)
                        counts[record['table']] += 1
                        raw = line(record)
                        if len(raw) > MAX_LINE:
                            raise TransferError('Source row exceeds export bounds')
                        hashed.update(raw)
                        zipped.write(raw)
                        canonical = normalized(record)
                        if canonical is not None:
                            data_hash.update(line(canonical))
                    zipped.write(line({'type': 'end', 'counts': counts, 'sha256': hashed.hexdigest(),
                                       'data_sha256': data_hash.hexdigest()}))
        inspect_backup(partial)
        # Never overwrite an existing backup path on an accidental rerun.
        with target.open('xb') as final, partial.open('rb') as source:
            import shutil
            shutil.copyfileobj(source, final, length=256000)
        partial.unlink()
    except BaseException:
        if created and partial.is_file():
            partial.unlink()
        raise
    return inspect_backup(target)


def data_digest(connection):
    hashed = hashlib.sha256()
    for record in records(connection):
        canonical = normalized(record)
        if canonical is not None:
            hashed.update(line(canonical))
    return hashed.hexdigest()


def restore_backup(connection, path):
    manifest = inspect_backup(path)  # Verify file before any database mutation.
    if manifest['counts']['entities'] == 0 or manifest['real_publication_records'] == 0:
        raise TransferError('Production publication history missing; this is not a verified recovery backup')
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(739221840125)')
        schema = SCHEMA.replace('REAL', 'DOUBLE PRECISION').replace('id INTEGER PRIMARY KEY',
                     'id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY')
        for statement in schema.split(';'):
            if statement.strip():
                connection.execute(statement)
        for table in ('entities', 'audit', 'budgets'):
            if connection.execute('SELECT COUNT(*) FROM public.' + table).fetchone()[0]:
                raise TransferError('Target contains application data; restore refused without changing it')
        keys = {row[0] for row in connection.execute('SELECT key FROM public.state')}
        if keys - {'schema_version', KEY, 'worker_lease'}:
            raise TransferError('Target contains application state; restore refused without changing it')
        with connection.cursor() as writer:
            batch, current_table = [], None
            def flush_batch():
                if not batch:
                    return
                sql = ('INSERT INTO public.' + current_table + ' (' + ','.join(COLUMNS[current_table]) +
                       ') VALUES (' + ','.join(['%s'] * len(COLUMNS[current_table])) + ')')
                if current_table == 'state':
                    sql += ' ON CONFLICT(key) DO UPDATE SET value=excluded.value'
                writer.executemany(sql, batch)
                batch.clear()
            for record in read_backup(path):
                if record['type'] != 'row':
                    continue
                table, values = record['table'], list(record['values'])
                if table != current_table:
                    flush_batch()
                    current_table = table
                if table == 'entities':
                    values[2] = RECOVERY_STATES.get(values[2], values[2])
                batch.append(values)
                if len(batch) >= 32:
                    flush_batch()
            flush_batch()
        if data_digest(connection) != manifest['data_sha256']:
            raise TransferError('Restored data digest mismatch; transaction rolled back')
        # Preserve identity allocation after importing explicit audit IDs.
        connection.execute("SELECT setval(pg_get_serial_sequence('public.audit','id'), "
                           "GREATEST(COALESCE((SELECT MAX(id) FROM public.audit),0)+1,1), false)")
        marker = {'required': True, 'status': 'restored', 'backup_sha256': manifest['sha256'],
                  'data_sha256': manifest['data_sha256'], 'source_created_at': manifest['created_at'],
                  'counts': manifest['counts'], 'source_controls': manifest['source_controls'], 'restored_at': utc()}
        set_state(connection, KEY, marker)
        set_state(connection, 'worker_lease', {'owner': '', 'until': 0})
        set_state(connection, 'emergency_stop', True)
    return {**marker, 'next_step': 'Run activate against this exact backup; then inspect status and reconcile uncertain submissions.'}


def set_state(connection, key, value):
    connection.execute('INSERT INTO public.state (key,value) VALUES (%s,%s) '
                       'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                       (key, json.dumps(value, allow_nan=False)))


def activate_backup(connection, path):
    manifest = inspect_backup(path)
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(739221840125)')
        row = connection.execute('SELECT value FROM public.state WHERE key=%s', (KEY,)).fetchone()
        marker = json.loads(row[0]) if row else {}
        if (marker.get('required') is not True or marker.get('status') != 'restored'
                or marker.get('backup_sha256') != manifest['sha256']
                or marker.get('data_sha256') != manifest['data_sha256']):
            raise TransferError('Target restore marker does not match this backup')
        if data_digest(connection) != manifest['data_sha256']:
            raise TransferError('Target changed after restore; activation refused')
        marker.update(status='verified', verified_at=utc())
        set_state(connection, 'worker_lease', {'owner': '', 'until': 0})
        set_state(connection, 'emergency_stop', True)
        set_state(connection, KEY, marker)
    return {**marker, 'next_step': 'Worker may start; emergency stop stays ON until operator checks /status and uncertain publications.'}


def check_connection(connection):
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        version = connection.execute('SELECT version()').fetchone()[0]
        if not str(version).startswith('PostgreSQL'):
            raise TransferError('Target is not a supported PostgreSQL server')
        # Do not print host/user/connection credentials or mutate the schema.
        return {'postgresql_reachable': True, 'read_only_check': True,
                'note': 'Connection check only; no production history imported or live publishing enabled.'}


def connection_url(env_name):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{0,80}', env_name):
        raise TransferError('Invalid environment variable name')
    value = os.getenv(env_name, '')
    try:
        parsed = urlsplit(value)
        mode = parse_qs(parsed.query).get('sslmode', [''])[0]
        if (parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname
                or not parsed.path.strip('/') or mode not in ('require', 'verify-ca', 'verify-full')):
            raise ValueError
    except ValueError:
        raise TransferError('Database environment variable missing or invalid; TLS is required') from None
    return value


@contextmanager
def connect_environment(env_name):
    import psycopg
    url = connection_url(env_name)
    try:
        with psycopg.connect(url, autocommit=True, connect_timeout=15, prepare_threshold=None) as connection:
            yield connection
    except psycopg.Error:
        raise TransferError('Database connection/operation failed; check provider access, quota and permissions. No credentials exposed.') from None


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Guarded PostgreSQL backup/restore for Square Desk; database URLs stay in environment variables.')
    parser.add_argument('action', choices=('check', 'export', 'inspect', 'restore', 'activate'))
    parser.add_argument('--file')
    parser.add_argument('--env', help='Environment variable containing the source or target PostgreSQL URI')
    parser.add_argument('--confirm', default='')
    args = parser.parse_args(argv)
    try:
        if args.action != 'check' and not args.file:
            raise TransferError('Supply --file with a backup path')
        if args.action == 'inspect':
            report = inspect_backup(args.file)
        else:
            if not args.env:
                raise TransferError('Supply --env with an environment variable name, never a connection URI')
            if args.action == 'restore' and args.confirm != 'RESTORE_EMPTY_TARGET':
                raise TransferError('Restore requires --confirm RESTORE_EMPTY_TARGET')
            if args.action == 'activate' and args.confirm != 'VERIFY_IMPORTED_HISTORY':
                raise TransferError('Activation requires --confirm VERIFY_IMPORTED_HISTORY')
            with connect_environment(args.env) as connection:
                if args.action == 'check':
                    report = check_connection(connection)
                elif args.action == 'export':
                    report = write_backup(connection, args.file)
                elif args.action == 'restore':
                    report = restore_backup(connection, args.file)
                else:
                    report = activate_backup(connection, args.file)
        print(json.dumps(report, indent=2, allow_nan=False))
        return 0
    except TransferError as error:
        print('Transfer stopped: ' + str(error))
        return 1
    except (OSError, ValueError, TypeError, KeyError):
        print('Transfer stopped: local backup/storage/format error. No database URL or row data exposed.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
