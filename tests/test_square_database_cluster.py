from contextlib import contextmanager
import json
import pytest

from square_desk.database_cluster import actual_digest, verified_checkpoint, register, main
from square_desk.database_transfer import TransferError
from square_desk.models import utc
from square_desk.store import Store
from test_square_database_transfer import Bridge


class RegistrationBridge(Bridge):
    @contextmanager
    def transaction(self):
        if self.store.conn.in_transaction:
            yield
        else:
            with self.store.transaction():
                yield


def restored_store():
    db = Store(':memory:', migration_required=True)
    db.insert('draft', {'receipt':{'id':'real-publication'}}, 'published', id='real-publication')
    db.set('database_migration', {'required':True,'status':'verified','backup_sha256':'a'*64,'verified_at':utc()})
    db.set('worker_lease', {'owner':'', 'until':0})
    db.set('emergency_stop', True)
    return db


def test_registration_exact_digest_and_refusal_of_wrong_checkpoint():
    db = restored_store()
    connection = RegistrationBridge(db)
    sha = actual_digest(connection)
    head = {'cluster':'square-prod','baseline':sha}
    marker = verified_checkpoint(connection)
    register(connection,head,marker)
    assert db.conn.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
    db.set('publisher_blocked', True)
    with pytest.raises(TransferError,match='differs'):
        register(connection,head,marker)
    assert db.state('publisher_blocked') is True
    db.close()


def test_matching_history_but_newer_replica_is_not_overwritten():
    db = restored_store()
    connection = RegistrationBridge(db)
    head = {'cluster':'square-prod','baseline':actual_digest(connection)}
    marker = verified_checkpoint(connection)
    register(connection,head,marker)
    db.conn.execute('UPDATE square_replica_meta SET seq=7')
    with pytest.raises(TransferError,match='registration differs'):
        register(connection,head,marker)
    assert db.conn.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 7
    db.close()


@pytest.mark.parametrize('reason',['unverified','unlocked','active_worker','missing_history'])
def test_registration_requires_recovery_and_stopped_writers(reason):
    db = restored_store()
    if reason == 'unverified':
        db.set('database_migration', {'required':True,'status':'needs_import'})
    elif reason == 'unlocked':
        db.set('emergency_stop', False)
    elif reason == 'active_worker':
        db.set('worker_lease', {'owner':'running','until':utc()+60})
    else:
        db.conn.execute('DELETE FROM entities')
    with pytest.raises(TransferError):
        verified_checkpoint(RegistrationBridge(db))
    db.close()


def test_configuration_failure_is_redacted_and_does_not_register(monkeypatch,capsys):
    monkeypatch.setenv('DESK_TURSO_JOURNAL_URL','https://private-password@bad-host.example')
    monkeypatch.setenv('DESK_TURSO_JOURNAL_TOKEN','private-key')
    monkeypatch.setenv('DESK_DB_CLUSTER_ID','square-production')
    assert main(['bootstrap','--checkpoint','not-created.gz','--confirm','STOPPED_WRITERS']) == 1
    output = capsys.readouterr().out
    assert 'private-password' not in output and 'private-key' not in output


def test_bootstrap_checks_identical_history_and_creates_portable_checkpoint(monkeypatch,tmp_path):
    from test_square_database_failover import HttpJournal
    from square_desk.database_transfer import inspect_backup
    db = restored_store()
    second = Store(':memory:')
    db.conn.backup(second.conn)
    remote = HttpJournal()
    journal = remote.journal()
    connections = {'DESK_AIVEN_DATABASE_URL':RegistrationBridge(db),
                   'DESK_SUPABASE_DATABASE_URL':RegistrationBridge(second)}
    @contextmanager
    def connect(name):
        yield connections[name]
    monkeypatch.setattr('square_desk.database_cluster.connect_environment',connect)
    monkeypatch.setattr('square_desk.database_cluster.TursoJournal',lambda *a,**k:journal)
    monkeypatch.setenv('DESK_AIVEN_DATABASE_URL','postgresql://u:p@aiven.test/db?sslmode=require')
    monkeypatch.setenv('DESK_SUPABASE_DATABASE_URL','postgresql://u:p@supabase.test/db?sslmode=require')
    monkeypatch.setenv('DESK_DB_FALLBACK_ORDER','aiven,supabase')
    checkpoint = tmp_path/'initial.jsonl.gz'
    try:
        assert main(['bootstrap','--checkpoint',str(checkpoint),'--confirm','STOPPED_WRITERS']) == 0
        assert inspect_backup(checkpoint)['real_publication_records'] == 1
        assert db.conn.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
        assert second.conn.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
        assert db.state('emergency_stop') is True
        assert remote.db.execute('SELECT owner FROM square_cluster').fetchone()[0] == ''
    finally:
        db.close()
        second.close()
        journal.client.close()
        remote.db.close()
