"""Bounded background replay; no primary application mutation uses this path."""
from time import monotonic
import sqlite3

from .postgres import DatabaseUnavailable
from .turso_journal import chain, decode

MAX_DECODED_BATCH = 4_000_000
MAX_STATEMENTS = 256


def decoded_size(statements):
    # Bound SQL/value bytes and statement object overhead. A single entry keeps
    # the existing journal bounds; never combine several oversized entries.
    return sum(256+len(s['sql'].encode())+sum(len(v.encode()) if isinstance(v,str) else 32
                                            for v in s['args']) for s in statements)


def prepared(entries, start, budget):
    previous, sequence = start['head_hash'], start['seq']
    result, size, count = [], 0, 0
    for entry in entries:
        if (entry['seq'] != sequence+1 or entry['prev'] != previous
                or entry['hash'] != chain(previous,entry['payload'])):
            raise DatabaseUnavailable('Journal chain mismatch; promotion held',code='journal_chain_mismatch')
        statements = decode(entry['payload'])
        added = decoded_size(statements)
        if result and (size+added>MAX_DECODED_BATCH or count+len(statements)>MAX_STATEMENTS):
            break
        result.append((entry,statements))
        size,count = size+added,count+len(statements)
        previous,sequence = entry['hash'],entry['seq']
        if len(result)>=budget or count>MAX_STATEMENTS or size>MAX_DECODED_BATCH:
            break
    return result


def apply_entries(connection, batch, *, stop=None, pipelined=True):
    from .database_failover import ReplayRejected
    work = [(entry,position,statement) for entry,statements in batch
            for position,statement in enumerate(statements,1)]
    if stop is not None and stop.is_set():
        raise DatabaseUnavailable('Replica background task stopped')
    if pipelined and hasattr(connection,'execute_replay_batch') and len(work)<=MAX_STATEMENTS:
        counts=connection.execute_replay_batch([(s['sql'],tuple(s['args'])) for _,_,s in work])
        if len(counts)!=len(work):
            raise DatabaseUnavailable('Replica result count invalid',code='replica_row_count_mismatch')
        for (entry,position,statement),count in zip(work,counts):
            if count!=statement['count']:
                error=DatabaseUnavailable('Replica rows differ from history',code='replica_row_count_mismatch')
                raise ReplayRejected(error,entry['seq'],position,statement['sql'])
    else:
        for entry,position,statement in work:
            if stop is not None and stop.is_set():
                raise DatabaseUnavailable('Replica background task stopped')
            try:
                result=connection.execute(statement['sql'],tuple(statement['args']))
                if result.rowcount!=statement['count']:
                    raise DatabaseUnavailable('Replica rows differ from history',code='replica_row_count_mismatch')
            except (DatabaseUnavailable,sqlite3.IntegrityError) as exc:
                raise ReplayRejected(exc,entry['seq'],position,statement['sql']) from None


def replay(connection,journal,head,*,limit=128,stop=None,progress=None):
    from .database_failover import metadata,CatchUpPending,ReplayRejected
    applied,started=0,monotonic()
    while True:
        if stop is not None and stop.is_set():
            raise DatabaseUnavailable('Replica background task stopped')
        # Fetch journal data outside the PostgreSQL lock/transaction.
        with connection.transaction():
            row=metadata(connection,head,allow_ahead=True)
            if row['seq']>head['seq']:
                head=journal.observe()
                row=metadata(connection,head)
            if progress is not None:
                progress(row['seq'])
            if row['seq']==head['seq']:
                if row['head_hash']!=head['head_hash']:
                    raise DatabaseUnavailable('Replica history hash mismatch',code='replica_hash_mismatch')
                return row['seq']
            # Bound one provider turn so another backup is not starved.
            if applied>=limit or (applied and monotonic()-started>=5):
                raise CatchUpPending(row['seq'])
        entries=journal.entries_batch(row['seq'],head['seq'],limit=min(32,limit-applied))
        if not entries:
            raise DatabaseUnavailable('Required journal history missing',code='journal_history_missing')
        batch=prepared(entries,row,limit-applied)
        try:
            with connection.transaction():
                current=metadata(connection,head,allow_ahead=True)
                if current['seq']!=row['seq'] or current['head_hash']!=row['head_hash']:
                    continue  # Another replay advanced it; never apply twice.
                apply_entries(connection,batch,stop=stop)
                if stop is not None and stop.is_set():
                    raise DatabaseUnavailable('Replica background task stopped')
                last=batch[-1][0]
                result=connection.execute('''UPDATE square_replica_meta SET seq=?,head_hash=?
                    WHERE id=1 AND seq=? AND head_hash=?''',
                    (last['seq'],last['hash'],row['seq'],row['head_hash']))
                if result.rowcount!=1:
                    raise DatabaseUnavailable('Replica checkpoint changed',code='replica_checkpoint_mismatch')
        except (ReplayRejected,sqlite3.IntegrityError):
            # Transaction rollback has completed. Replay individually so a
            # conflict after valid entries has the correct committed prefix.
            # Lost COMMIT/rollback acknowledgements raise DatabaseUnavailable
            # instead and are reconciled on the next metadata read, not retried.
            for entry,statements in batch:
                with connection.transaction():
                    current=metadata(connection,head,allow_ahead=True)
                    if current['seq']>=entry['seq']:
                        break
                    if current['seq']!=entry['seq']-1 or current['head_hash']!=entry['prev']:
                        raise DatabaseUnavailable('Replica checkpoint changed',code='replica_checkpoint_mismatch')
                    apply_entries(connection,[(entry,statements)],stop=stop,pipelined=False)
                    if stop is not None and stop.is_set():
                        raise DatabaseUnavailable('Replica background task stopped')
                    result=connection.execute('UPDATE square_replica_meta SET seq=?,head_hash=? WHERE id=1',
                                              (entry['seq'],entry['hash']))
                    if result.rowcount!=1:
                        raise DatabaseUnavailable('Replica checkpoint changed',code='replica_checkpoint_mismatch')
                applied+=1
                if progress is not None:
                    progress(entry['seq'])
            continue
        applied+=len(batch)
        if progress is not None:
            progress(last['seq'])
