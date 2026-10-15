import pytest

from square_desk.postgres import DatabaseUnavailable,Result,Row
from test_square_postgres import connection,FakeCursor


def test_postgres_export_is_repeatable_read_readonly_without_live_advisory_lock(connection):
    adapter,raw=connection
    with adapter.snapshot():
        assert adapter.active
        adapter.execute('SELECT * FROM square_replica_meta WHERE id=1')
    assert raw.outcome=='commit' and not adapter.active
    assert raw.calls[0][0]=='SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
    assert not any('advisory' in sql for sql,args in raw.calls)


def test_normal_postgres_read_is_repeatable_read_without_exclusive_advisory_lock(connection):
    adapter,raw=connection
    with adapter.read_transaction():
        adapter.execute('SELECT value FROM state WHERE key=?',('control',))
    assert raw.outcome=='commit' and not adapter.active
    assert raw.calls[0][0].startswith('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;')
    assert not any('advisory' in sql for sql,args in raw.calls)


def test_postgres_snapshot_driver_failure_rolls_back_and_redacts(connection):
    adapter,raw=connection
    with pytest.raises(DatabaseUnavailable) as error:
        with adapter.snapshot():
            raise adapter.driver.OperationalError('private DSN or row')
    assert raw.outcome=='rollback' and raw.closed and not adapter.active
    assert 'private' not in str(error.value)


def test_postgres_named_cursor_streams_without_fetching_whole_table(connection):
    adapter,raw=connection
    opened=[]
    class Cursor(FakeCursor):
        def execute(self,sql,args=None):return super().execute(sql,args)
        def __iter__(self):return iter([{'id':'only-one-at-a-time'}])
        def fetchall(self):raise AssertionError('Unbounded fetch forbidden')
    def cursor(name=None):
        result=Cursor(raw);opened.append((name,result));return result
    raw.cursor=cursor
    adapter.active=True
    assert list(adapter.stream_rows('entities'))==[Row(id='only-one-at-a-time')]
    assert opened[0][0]=='square_snapshot_entities' and opened[0][1].itersize==16
    assert 'ORDER BY id COLLATE "C"' in raw.calls[0][0]
    with pytest.raises(DatabaseUnavailable):list(adapter.stream_rows('private-other-table'))


@pytest.mark.parametrize('fault',(None,'insert_count','readback_count'))
def test_postgres_archive_is_server_side_and_requires_both_counts(connection,fault):
    adapter,raw=connection
    statements=[]
    def execute(sql,args=None):
        statements.append((sql,args))
        if sql.startswith('SELECT COUNT'):
            count=0 if fault=='readback_count' and 'WHERE rebuild_id' in sql else 2
            return Result([Row(n=count)],1)
        if sql.startswith('INSERT INTO square_replica_rebuild_archive'):
            return Result([],1 if fault=='insert_count' else 2)
        return Result([],0)
    adapter.execute=execute
    if fault:
        with pytest.raises(DatabaseUnavailable) as error:adapter.archive_replica('test-proof-id')
        assert error.value.code=='snapshot_archive_unverified'
    else:
        counts=adapter.archive_replica('test-proof-id')
        assert sum(counts.values())==10
        assert sum('row_to_json(old)::text FROM' in sql for sql,args in statements)==5
        assert any('ENABLE ROW LEVEL SECURITY' in sql for sql,args in statements)
        assert not any('SELECT *' in sql for sql,args in statements)
