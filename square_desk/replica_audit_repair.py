"""Archive a provably extra audit row on a secondary fresh-start replica.

No application entity, budget, state or watermark is rewritten. Complete initial
journal history proves the conflicting ID was absent at the replica checkpoint.
"""
import hashlib
import json
import re
import sqlite3

from .postgres import DatabaseUnavailable
from .turso_journal import chain, decode

MAX_PREFIX = 1024
INSERT = re.compile(r'^INSERT INTO audit\(id,at,category,correlation,message\) VALUES \(\?,\?,\?,\?,\?\)$', re.I)
DELETE = re.compile(r'^DELETE FROM audit WHERE at\s*<\s*\?$', re.I)
TARGET = re.compile(r'^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+audit\b', re.I)


def held(code):
    raise DatabaseUnavailable('Replica audit repair proof unavailable; no repair performed', code=code)


def verified_entry(journal, sequence, previous):
    entries = journal.entries(sequence - 1, sequence)
    if len(entries) != 1:
        held('journal_history_missing')
    entry = entries[0]
    if (entry['seq'] != sequence or entry['prev'] != previous
            or entry['hash'] != chain(previous, entry['payload'])):
        held('journal_chain_mismatch')
    return entry, decode(entry['payload'])


def repair(connection, journal, head, failure, *, is_secondary, stop=None):
    detail = getattr(failure, 'failure_detail', {})
    if (getattr(failure, 'code', '') != 'unique_constraint_rejected'
            or detail.get('table') != 'audit' or detail.get('constraint') != 'audit_pkey'
            or detail.get('statement_position') != 1 or not is_secondary()):
        held('audit_repair_not_applicable')
    marker = json.loads(head.get('migration_marker', '{}'))
    baseline = getattr(journal, 'fresh_baseline', None)
    if (marker.get('origin') != 'authorized_fresh_start' or marker.get('history_preserved') is not False
            or not baseline or baseline != head['baseline']):
        held('audit_repair_requires_fresh_history')
    failed_seq = detail.get('journal_sequence')
    if type(failed_seq) is not int or not 1 <= failed_seq <= MAX_PREFIX + 1:
        held('audit_repair_history_bound')
    # Read-only checkpoint capture. Recheck inside the target transaction below.
    row = connection.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
    if (not row or row['cluster'] != head['cluster'] or row['baseline'] != baseline
            or row['seq'] != failed_seq - 1):
        held('replica_checkpoint_mismatch')
    prefix_hash, checkpoint_hash = baseline, row['head_hash']
    canonical = sqlite3.connect(':memory:')
    canonical.execute('CREATE TABLE audit (id INTEGER PRIMARY KEY,at REAL NOT NULL,category TEXT NOT NULL,correlation TEXT NOT NULL,message TEXT NOT NULL)')
    try:
        for sequence in range(1, failed_seq):
            if stop is not None and stop.is_set():
                held('audit_repair_cancelled')
            entry, statements = verified_entry(journal, sequence, prefix_hash)
            for statement in statements:
                sql = statement['sql']
                if not TARGET.match(sql):
                    continue
                if not (INSERT.fullmatch(sql) or DELETE.fullmatch(sql)):
                    held('audit_repair_unsupported_history')
                try:
                    result = canonical.execute(sql, tuple(statement['args']))
                except sqlite3.Error:
                    held('audit_repair_invalid_history')
                if result.rowcount != statement['count']:
                    held('audit_repair_invalid_history')
            prefix_hash = entry['hash']
        if prefix_hash != checkpoint_hash:
            held('journal_chain_mismatch')
        entry, statements = verified_entry(journal, failed_seq, prefix_hash)
        if not statements:
            held('audit_repair_not_applicable')
        statement = statements[0]
        args = statement['args']
        if (not INSERT.fullmatch(statement['sql']) or statement['count'] != 1
                or len(args) != 5 or type(args[0]) is not int or args[0] <= 0
                or canonical.execute('SELECT 1 FROM audit WHERE id=?', (args[0],)).fetchone()):
            held('audit_repair_not_proven_extra')
        with connection.transaction():
            current = connection.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
            if (not current or current['cluster'] != row['cluster'] or current['baseline'] != baseline
                    or current['seq'] != row['seq'] or current['head_hash'] != checkpoint_hash
                    or not is_secondary() or (stop is not None and stop.is_set())):
                held('replica_checkpoint_mismatch')
            conflict = connection.execute('SELECT id,at,category,correlation,message FROM audit WHERE id=? FOR UPDATE',
                                          (args[0],)).fetchone()
            if not conflict:
                held('audit_repair_conflict_changed')
            payload = json.dumps(dict(conflict), sort_keys=True, allow_nan=False)
            if len(payload.encode()) > 1_000_000:
                held('audit_repair_archive_bound')
            archive_id = hashlib.sha256((row['cluster'] + '\n' + str(row['seq']) + '\n' + payload).encode()).hexdigest()
            connection.execute('''CREATE TABLE IF NOT EXISTS square_replica_audit_archive (
                id TEXT PRIMARY KEY,cluster TEXT NOT NULL,seq BIGINT NOT NULL,payload TEXT NOT NULL)''')
            connection.execute('ALTER TABLE square_replica_audit_archive ENABLE ROW LEVEL SECURITY')
            connection.execute('INSERT INTO square_replica_audit_archive VALUES (?,?,?,?) ON CONFLICT DO NOTHING',
                               (archive_id, row['cluster'], row['seq'], payload))
            saved = connection.execute('SELECT payload FROM square_replica_audit_archive WHERE id=?', (archive_id,)).fetchone()
            if not saved or saved['payload'] != payload:
                held('audit_repair_archive_unverified')
            removed = connection.execute('DELETE FROM audit WHERE id=?', (args[0],))
            if removed.rowcount != 1:
                held('audit_repair_conflict_changed')
        return {'status': 'archived_extra_audit_row', 'at_sequence': row['seq'], 'retry_sequence': failed_seq,
                'archived_rows': 1}
    finally:
        canonical.close()
