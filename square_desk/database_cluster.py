"""Operator-only registration of matching, verified PostgreSQL checkpoints."""
from contextlib import ExitStack
import hashlib
import json
import os
from . import migration
from .database_failover import environment_nodes, META_SCHEMA
from .database_transfer import records, line, write_backup, connect_environment, TransferError
from .models import utc
from .turso_journal import TursoJournal
from .postgres import DatabaseUnavailable


def actual_digest(connection):
    hashed = hashlib.sha256()
    for record in records(connection):
        if record['table'] == 'state' and record['values'][0] in (migration.KEY, 'worker_lease'):
            continue
        # Unlike backup recovery normalization, exact live row states matter here.
        hashed.update(line(record))
    return hashed.hexdigest()


def verified_checkpoint(connection):
    states = dict(connection.execute("SELECT key,value FROM state WHERE key IN ('database_migration','emergency_stop','worker_lease')").fetchall())
    marker = json.loads(states.get(migration.KEY, '{}'))
    if not marker.get('required') or migration.pending(marker):
        raise TransferError('Restore/activate verified production history on every node first')
    if json.loads(states.get('emergency_stop', 'false')) is not True:
        raise TransferError('Enable emergency stop and stop every old worker before registration')
    lease = json.loads(states.get('worker_lease', '{}'))
    if lease.get('owner') and lease.get('until', 0) > utc():
        raise TransferError('A worker lease is still active; stop workers and wait for its expiry')
    count = connection.execute("SELECT COUNT(*) FROM entities WHERE kind='draft' AND status IN ('published','manual_published','uncertain')").fetchone()[0]
    if count <= 0:
        raise TransferError('Production publication history is absent; fresh start is not authorized by this tool')
    return marker


def register(connection, head, marker):
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(739221840125)')
        verified_checkpoint(connection)
        if actual_digest(connection) != head['baseline']:
            raise TransferError('Replica differs from the initial checkpoint; registration refused')
        connection.execute(META_SCHEMA)
        existing = connection.execute('SELECT cluster,baseline,seq,head_hash FROM square_replica_meta WHERE id=1').fetchone()
        expected = (head['cluster'], head['baseline'], 0, head['baseline'])
        if existing and tuple(existing) != expected:
            raise TransferError('Existing replica registration differs; do not overwrite a newer node')
        connection.execute('INSERT INTO square_replica_meta VALUES (1,%s,%s,0,%s) ON CONFLICT DO NOTHING',
                           (head['cluster'], head['baseline'], head['baseline']))
        connection.execute('INSERT INTO state VALUES (%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                           (migration.KEY, json.dumps(marker, allow_nan=False)))
        connection.execute('INSERT INTO state VALUES (%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                           ('worker_lease', json.dumps({'owner': '', 'until': 0})))


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Register PostgreSQL replicas against one Turso journal; URLs only in ENV')
    parser.add_argument('action', choices=('bootstrap', 'enroll', 'status'))
    parser.add_argument('--env', help='Variable name for one restored replica (enroll only)')
    parser.add_argument('--checkpoint', help='New local compressed backup path (bootstrap only)')
    parser.add_argument('--confirm', default='')
    args = parser.parse_args(argv)
    journal = None
    try:
        journal = TursoJournal(os.getenv('DESK_TURSO_JOURNAL_URL', ''), os.getenv('DESK_TURSO_JOURNAL_TOKEN', ''),
                               os.getenv('DESK_DB_CLUSTER_ID', ''))
        if args.action == 'status':
            rows = journal.query('SELECT cluster,seq,head_hash,log_bytes FROM square_cluster WHERE id=1 AND cluster=?', (journal.cluster,))
            if len(rows) != 1:
                raise TransferError('Journal is not initialized for this cluster')
            report = {'journal': 'turso', **rows[0], 'note': 'Metadata only; not a provider quota reading'}
        else:
            if args.confirm != 'STOPPED_WRITERS':
                raise TransferError('Registration requires --confirm STOPPED_WRITERS after every old service is stopped')
            if args.action == 'bootstrap':
                if not args.checkpoint:
                    raise TransferError('Supply a new --checkpoint backup filename')
                default = os.getenv('DESK_DATABASE_URL') or os.getenv('DATABASE_URL') or ''
                nodes = environment_nodes(default)
                with ExitStack() as stack:
                    connections = []
                    for name in nodes:
                        key = {'aiven':'DESK_AIVEN_DATABASE_URL','supabase':'DESK_SUPABASE_DATABASE_URL','neon':'DESK_NEON_DATABASE_URL'}[name]
                        if name == 'aiven' and not os.getenv(key):
                            key = 'DESK_DATABASE_URL' if os.getenv('DESK_DATABASE_URL') else 'DATABASE_URL'
                        connections.append(stack.enter_context(connect_environment(key)))
                    # Locks and transactions remain held while all checkpoints are checked.
                    for connection in connections:
                        stack.enter_context(connection.transaction())
                        connection.execute('SELECT pg_advisory_xact_lock(739221840125)')
                    markers = [verified_checkpoint(c) for c in connections]
                    digests = [actual_digest(c) for c in connections]
                    if len(set(digests)) != 1:
                        raise TransferError('Providers do not have identical production history and controls')
                    # A new, separately verified backup is kept locally for late enrollment.
                    # write_backup opens a nested savepoint on this locked, stopped source.
                    write_backup(connections[0], args.checkpoint, locked_checkpoint=True)
                    journal.initialize(digests[0], markers[0])
                    head = journal.acquire(force=True)
                    for connection in connections:
                        register(connection, head, json.loads(head['migration_marker']))
                report = {'status': 'registered', 'providers': list(nodes), 'sequence': 0,
                          'emergency_stop': True, 'checkpoint': args.checkpoint}
            else:
                if not args.env:
                    raise TransferError('Supply --env with a variable name for the initial-checkpoint replica')
                head = journal.acquire(force=True)
                with connect_environment(args.env) as connection:
                    # Marker is canonical on the journal's initial checkpoint backup.
                    verified_checkpoint(connection)
                    register(connection, head, json.loads(head['migration_marker']))
                report = {'status': 'registered_at_initial_checkpoint', 'catch_up_required': head['seq'] > 0}
        print(json.dumps(report, indent=2))
        return 0
    except (TransferError, DatabaseUnavailable, ValueError, OSError, KeyError, TypeError):
        print('Cluster setup held: provider access, checkpoint verification, writer lock or configuration failed. No credentials exposed.')
        return 1
    finally:
        if journal:
            try:
                journal.release()
            except DatabaseUnavailable:
                pass
            journal.close()


if __name__ == '__main__':
    raise SystemExit(main())
