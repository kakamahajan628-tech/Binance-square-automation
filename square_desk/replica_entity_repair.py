"""Restore one secondary preimage only from complete verified fresh history."""
import hashlib
import json
import re
import sqlite3
from time import monotonic

from .postgres import DatabaseUnavailable
from .turso_journal import chain,decode

MAX_SEQUENCE=100_000
MAX_ENCODED=64_000_000
MAX_ROW=2_000_000
MAX_SECONDS=120
COLUMNS=('id','kind','status','created','updated','due','fingerprint','payload')
INSERT=re.compile(r'^INSERT INTO entities VALUES \(\?,\?,\?,\?,\?,\?,\?,\?\)(?: ON CONFLICT DO NOTHING)?$',re.I)
FIELD=re.compile(r"^(?:updated|status|payload|due)=(?:\?|NULL|'uncertain'|'approved'|'review')$",re.I)
POINT=re.compile(r'^id=\?(?: AND status=\?)?$',re.I)
RECOVER=re.compile(r"^status='(?:sending|preparing_media|uploading_image|chart_rendering)'$",re.I)
DELETE=re.compile(r"^DELETE FROM entities WHERE (?:id=\?|kind='snapshot' AND created<\?|kind='outbox' AND status='delivered' AND created<\?)$",re.I)


def held(code):
    raise DatabaseUnavailable('Entity repair proof held; data and checkpoint preserved',code=code)


def normalized(sql):
    return ' '.join(sql.split())


def update_shape(sql):
    match=re.fullmatch(r'UPDATE entities SET (.+) WHERE (.+)',normalized(sql),re.I)
    if not match or not all(FIELD.fullmatch(field.strip()) for field in match[1].split(',')):
        return None
    return match[2] if POINT.fullmatch(match[2]) or RECOVER.fullmatch(match[2]) else None


def entity_sql(sql):
    sql=normalized(sql)
    return bool(INSERT.fullmatch(sql) or update_shape(sql) or DELETE.fullmatch(sql))


def proof(journal,head,row,failure,*,stop=None,cache=None):
    detail=failure.failure_detail
    seq=detail.get('journal_sequence')
    if (failure.code!='replica_row_count_mismatch' or detail.get('table')!='entities'
            or detail.get('statement_position')!=1 or detail.get('operation')!='update'
            or detail.get('expected_rows')!=1 or detail.get('actual_rows')!=0):
        held('entity_repair_not_applicable')
    if type(seq) is not int or not 1<=seq<=MAX_SEQUENCE or row['seq']!=seq-1:
        held('entity_history_bound_or_checkpoint')
    marker=json.loads(head.get('migration_marker','{}'))
    if (marker.get('origin')!='authorized_fresh_start' or marker.get('history_preserved') is not False
            or getattr(journal,'fresh_baseline',None)!=head['baseline']):
        held('entity_requires_complete_fresh_history')
    if row['cluster']!=head['cluster'] or row['baseline']!=head['baseline']:
        held('replica_checkpoint_mismatch')
    entries=journal.entries(seq-1,seq)
    if len(entries)!=1:
        held('journal_history_missing')
    entry=entries[0]
    if entry['seq']!=seq or entry['prev']!=row['head_hash'] or entry['hash']!=chain(entry['prev'],entry['payload']):
        held('journal_chain_mismatch')
    statements=decode(entry['payload'])
    if not statements:
        held('entity_repair_not_applicable')
    target=statements[0]
    where=update_shape(target['sql'])
    if not where or not POINT.fullmatch(where) or target['count']!=1:
        held('entity_repair_not_applicable')
    args=target['args']
    index=-2 if ' AND ' in where.upper() else -1
    if len(args)<abs(index) or not isinstance(args[index],str) or not 1<=len(args[index])<=128:
        held('entity_repair_not_applicable')
    ident=args[index]
    key=(row['cluster'],row['baseline'],row['seq'],row['head_hash'],seq,entry['hash'],ident)
    if cache is not None and cache.get('key')==key:
        return cache['row']
    canonical=sqlite3.connect(':memory:')
    canonical.row_factory=sqlite3.Row
    canonical.execute('PRAGMA page_size=4096')
    canonical.execute('PRAGMA max_page_count=8192')  # 32 MB canonical entity store.
    canonical.execute('CREATE TABLE entities(id TEXT PRIMARY KEY,kind TEXT NOT NULL,status TEXT NOT NULL,created REAL NOT NULL,updated REAL NOT NULL,due REAL,fingerprint TEXT,payload TEXT NOT NULL)')
    canonical.execute('CREATE UNIQUE INDEX unique_fingerprint ON entities(kind,fingerprint) WHERE fingerprint IS NOT NULL')
    sequence,previous,encoded,started=0,head['baseline'],0,monotonic()
    try:
        while sequence<row['seq']:
            if (stop is not None and stop.is_set()) or monotonic()-started>MAX_SECONDS:
                held('entity_proof_cancelled_or_time_bound')
            batch=journal.proof_entries(sequence,row['seq'])
            if not batch:
                held('journal_history_missing')
            for item in batch:
                if (stop is not None and stop.is_set()) or monotonic()-started>MAX_SECONDS:
                    held('entity_proof_cancelled_or_time_bound')
                if (item['seq']!=sequence+1 or item['seq']>row['seq'] or item['prev']!=previous
                        or item['hash']!=chain(previous,item['payload'])):
                    held('journal_chain_mismatch')
                encoded+=len(item['payload'])
                if encoded>MAX_ENCODED:
                    held('entity_proof_payload_bound')
                for statement in decode(item['payload']):
                    sql=statement['sql']
                    if not re.match(r'^(INSERT INTO|UPDATE|DELETE FROM)\s+entities\b',sql,re.I):
                        continue
                    if not entity_sql(sql):
                        held('entity_history_unsupported')
                    result=canonical.execute(sql,tuple(statement['args']))
                    if result.rowcount!=statement['count']:
                        held('entity_history_count_mismatch')
                sequence,previous=item['seq'],item['hash']
        if previous!=row['head_hash']:
            held('journal_chain_mismatch')
        expected=canonical.execute('SELECT * FROM entities WHERE id=?',(ident,)).fetchone()
        if not expected:
            held('entity_canonical_missing')
        expected=dict(expected)
        if len(json.dumps(expected,allow_nan=False).encode())>MAX_ROW:
            held('entity_row_bound')
        if canonical.execute(target['sql'],tuple(target['args'])).rowcount!=1:
            held('entity_target_not_proven')
        if cache is not None:
            cache.clear()
            cache.update(key=key,row=expected)
        return expected
    except sqlite3.Error:
        held('entity_proof_sql_or_storage_bound')
    finally:
        canonical.close()


def repair(connection,journal,head,failure,*,is_secondary,stop=None,cache=None):
    if not is_secondary():
        held('entity_primary_protected')
    row=connection.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
    if not row:
        held('replica_checkpoint_mismatch')
    expected=proof(journal,head,row,failure,stop=stop,cache=cache)
    with connection.transaction():
        current=connection.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
        if (dict(current or {})!=dict(row) or not is_secondary() or (stop is not None and stop.is_set())):
            held('replica_checkpoint_mismatch')
        before=connection.execute('SELECT * FROM entities WHERE id=? FOR UPDATE',(expected['id'],)).fetchone()
        if before and dict(before)==expected:
            held('entity_row_already_canonical')
        archive=json.dumps({'before':dict(before) if before else None,'canonical':expected},sort_keys=True,allow_nan=False)
        if len(archive.encode())>MAX_ROW*2+1024:
            held('entity_archive_bound')
        archive_id=hashlib.sha256((row['cluster']+'\n'+str(row['seq'])+'\n'+archive).encode()).hexdigest()
        connection.execute('''CREATE TABLE IF NOT EXISTS square_replica_entity_archive (
            id TEXT PRIMARY KEY,cluster TEXT NOT NULL,seq BIGINT NOT NULL,payload TEXT NOT NULL)''')
        connection.execute('ALTER TABLE square_replica_entity_archive ENABLE ROW LEVEL SECURITY')
        connection.execute('INSERT INTO square_replica_entity_archive VALUES (?,?,?,?) ON CONFLICT DO NOTHING',
                           (archive_id,row['cluster'],row['seq'],archive))
        saved=connection.execute('SELECT payload FROM square_replica_entity_archive WHERE id=?',(archive_id,)).fetchone()
        if not saved or saved['payload']!=archive:
            held('entity_archive_unverified')
        restored=connection.execute('''INSERT INTO entities VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
            kind=excluded.kind,status=excluded.status,created=excluded.created,updated=excluded.updated,
            due=excluded.due,fingerprint=excluded.fingerprint,payload=excluded.payload''',
            tuple(expected[column] for column in COLUMNS))
        readback=connection.execute('SELECT * FROM entities WHERE id=?',(expected['id'],)).fetchone()
        if restored.rowcount!=1 or dict(readback or {})!=expected:
            held('entity_restore_unverified')
    return {'status':'restored_verified_entity_preimage','at_sequence':row['seq'],
            'retry_sequence':row['seq']+1,'repaired_rows':1}
