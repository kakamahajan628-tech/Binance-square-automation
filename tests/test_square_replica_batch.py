import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from threading import Event

import pytest

from test_square_database_failover import cluster
from square_desk.database_failover import catch_up, ReplayRejected
from square_desk.postgres import DatabaseUnavailable,PostgresConnection,ConstraintRejected
from square_desk.replica_batch import replay


def count_batches(remote):
    return sum('WITH candidates AS' in req['stmt']['sql']
               for call in remote.requests for req in call['requests'] if req['type']=='execute')


def test_96_entries_fetch_in_three_requests_and_resume_without_replay(cluster):
    remote,journal,nodes,engine=cluster
    for i in range(96):
        engine.execute('INSERT INTO state VALUES (?,?)',('batched-'+str(i),'true'))
    remote.requests.clear()
    replica=nodes['neon'].connection()
    progress=[]
    try:
        assert replay(replica,journal,journal.head,progress=progress.append)==96
        assert count_batches(remote)==3
        assert progress[-1]==96
        assert replica.execute("SELECT COUNT(*) FROM state WHERE key LIKE 'batched-%'").fetchone()[0]==96
        assert replay(replica,journal,journal.head)==96
        assert count_batches(remote)==3
    finally:
        replica.close()


def test_row_count_failure_rolls_back_batch_then_commits_only_valid_prefix(cluster):
    remote,journal,nodes,engine=cluster
    engine.execute('INSERT INTO state VALUES (?,?)',('safe-prefix','true'))
    engine.execute('UPDATE entities SET status=? WHERE id=?',('sending','pending'))
    replica=nodes['neon'].connection()
    replica.execute('DELETE FROM entities WHERE id=?',('pending',))
    try:
        with pytest.raises(ReplayRejected) as caught:
            replay(replica,journal,journal.head)
        assert caught.value.failure_detail['journal_sequence']==2
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        assert replica.execute('SELECT value FROM state WHERE key=?',('safe-prefix',)).fetchone()[0]=='true'
    finally:
        replica.close()


def test_lost_batch_commit_ack_reconciles_watermark_without_reapplying_increment(cluster):
    remote,journal,nodes,engine=cluster
    for _ in range(4):
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
    replica=nodes['neon'].connection()
    nodes['neon'].lose_commit_ack=True
    try:
        with pytest.raises(DatabaseUnavailable):
            replay(replica,journal,journal.head)
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==4
        assert replay(replica,journal,journal.head)==4
        assert replica.execute('SELECT used FROM budgets WHERE category=?',('posts',)).fetchone()[0]==11
    finally:
        replica.close()


def test_corrupt_batch_is_rejected_before_any_mutation(cluster):
    remote,journal,nodes,engine=cluster
    for i in range(3):
        engine.execute('INSERT INTO state VALUES (?,?)',('corrupt-batch-'+str(i),'true'))
    remote.db.execute("UPDATE square_journal SET payload='invalid' WHERE seq=3")
    replica=nodes['neon'].connection()
    try:
        with pytest.raises(DatabaseUnavailable):
            replay(replica,journal,journal.head)
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==0
        assert replica.execute("SELECT COUNT(*) FROM state WHERE key LIKE 'corrupt-batch-%'").fetchone()[0]==0
    finally:
        replica.close()


def test_payload_bound_allows_one_large_entry_alone(cluster):
    remote,journal,nodes,engine=cluster
    for i in range(3):
        engine.execute('INSERT INTO state VALUES (?,?)',('payload-'+str(i),'true'))
    entries=journal.entries_batch(0,3,limit=32,max_payload=1)
    assert len(entries)==1 and entries[0]['seq']==1


def test_large_decoded_values_are_not_combined_in_one_transaction(cluster):
    remote,journal,nodes,engine=cluster
    for i in range(3):
        engine.execute('INSERT INTO state VALUES (?,?)',('large-batch-'+str(i),'x'*2_100_000))
    replica=nodes['neon'].connection()
    commits=[]
    original=replica.transaction
    @contextmanager
    def transaction():
        start=replica.db.total_changes
        with original():
            yield
        if replica.db.total_changes>start:
            commits.append(replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0])
    replica.transaction=transaction
    try:
        assert replay(replica,journal,journal.head)==3
        assert commits==[1,2,3]
    finally:
        replica.close()


def test_postgres_pipeline_sends_all_queries_before_single_sync_and_preserves_parameters():
    calls=[]
    class DriverError(Exception):
        pass
    class IntegrityError(DriverError):
        pass
    class Cursor:
        rowcount=-1
        def execute(self,sql,args):
            calls.append(('execute',sql,args))
        def close(self):
            calls.append(('close',))
    cursors=[]
    class Raw:
        @contextmanager
        def pipeline(self):
            yield self
        def sync(self):
            calls.append(('sync',))
            for c in cursors:
                c.rowcount=1
        def cursor(self):
            cursor=Cursor();cursors.append(cursor);return cursor
    connection=object.__new__(PostgresConnection)
    connection.active=True
    connection.raw=Raw()
    connection.driver=SimpleNamespace(Error=DriverError,IntegrityError=IntegrityError,
                                     capabilities=SimpleNamespace(has_pipeline=lambda:True))
    statements=[('INSERT INTO state VALUES (?,?)',('a','literal %s ? private')),
                ('UPDATE state SET value=? WHERE key=?',('value','a'))]
    assert connection.execute_replay_batch(statements)==[1,1]
    assert [c[0] for c in calls[:3]]==['execute','execute','sync']
    assert calls[0][1]=='INSERT INTO state VALUES (%s,%s)'
    assert calls[0][2]==statements[0][1]


def test_pipeline_constraint_rolls_back_then_identifies_exact_conflict(cluster):
    remote,journal,nodes,engine=cluster
    for key in ('safe','duplicate'):
        engine.execute('INSERT INTO state VALUES (?,?)',(key,'canonical'))
    replica=nodes['neon'].connection()
    replica.execute('INSERT INTO state VALUES (?,?)',('duplicate','outside-history'))
    original_execute=replica.execute
    def execute(sql,args=None):
        try:
            return original_execute(sql,args)
        except sqlite3.IntegrityError:
            raise ConstraintRejected('23505','state_pkey') from None
    replica.execute=execute
    def batch(statements):
        return [replica.execute(sql,args).rowcount for sql,args in statements]
    replica.execute_replay_batch=batch
    try:
        with pytest.raises(ReplayRejected) as caught:
            replay(replica,journal,journal.head)
        assert caught.value.code=='unique_constraint_rejected'
        assert caught.value.failure_detail['journal_sequence']==2
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==1
        assert replica.execute('SELECT value FROM state WHERE key=?',('duplicate',)).fetchone()[0]=='outside-history'
    finally:
        replica.close()


def test_checkpoint_advance_during_prefetch_is_not_replayed_twice(cluster):
    remote,journal,nodes,engine=cluster
    for _ in range(3):
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
    replica=nodes['neon'].connection()
    original=journal.entries_batch
    def prefetch(*args,**kwargs):
        entries=original(*args,**kwargs)
        catch_up(replica,journal,journal.head)
        return entries
    journal.entries_batch=prefetch
    try:
        assert replay(replica,journal,journal.head)==3
        assert replica.execute('SELECT used FROM budgets WHERE category=?',('posts',)).fetchone()[0]==10
    finally:
        replica.close()


def test_shutdown_after_pipeline_work_rolls_back_before_watermark_commit(cluster):
    remote,journal,nodes,engine=cluster
    engine.execute('INSERT INTO state VALUES (?,?)',('cancelled-batch','true'))
    replica=nodes['neon'].connection()
    stop=Event()
    def batch(statements):
        counts=[replica.execute(sql,args).rowcount for sql,args in statements]
        stop.set()
        return counts
    replica.execute_replay_batch=batch
    try:
        with pytest.raises(DatabaseUnavailable):
            replay(replica,journal,journal.head,stop=stop)
        assert replica.execute('SELECT seq FROM square_replica_meta').fetchone()[0]==0
        assert replica.execute('SELECT value FROM state WHERE key=?',('cancelled-batch',)).fetchone() is None
    finally:
        replica.close()
