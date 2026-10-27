"""Deployment regression: rotating HTTP batons and network transaction expiry."""
import json

import httpx
import pytest

from square_desk.postgres import DatabaseUnavailable
from test_square_database_failover import HttpJournal, BASE, CLUSTER


class RotatingJournal(HttpJournal):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self.expected = None
        self.begin_request = None

    def handler(self, request):
        self.counter += 1
        body = json.loads(request.content)
        supplied = body.get('baton')
        if supplied and supplied != self.expected:
            return httpx.Response(400, text='private expired baton')
        if self.begin_request is not None and self.counter - self.begin_request >= 4:
            # Simulate >5s spent in network round trips without sleeping.
            self.db.execute('ROLLBACK')
            self.begin_request = None
            return httpx.Response(400, text='private transaction timeout')
        response = super().handler(request)
        data = response.json()
        sqls = [r['stmt']['sql'] for r in body['requests'] if r['type'] == 'execute']
        if any(sql.startswith('BEGIN') for sql in sqls):
            self.begin_request = self.counter
        if any(sql in ('COMMIT', 'ROLLBACK') for sql in sqls):
            self.begin_request = None
        self.batons.clear()
        self.expected = None
        if data.get('baton'):
            self.expected = 'connection-' + str(self.counter)
            self.batons.add(self.expected)
        data['baton'] = self.expected
        return httpx.Response(200, json=data)


def test_initialization_completes_in_one_request_with_rotating_batons():
    remote = RotatingJournal()
    journal = remote.journal()
    try:
        journal.initialize(BASE, {'origin': 'authorized_fresh_start'})
        assert remote.counter == 1
        assert remote.db.execute('SELECT cluster,baseline,seq FROM square_cluster').fetchone() == (CLUSTER, BASE, 0)
        assert not remote.db.in_transaction
        assert not remote.batons
        journal.acquire()
        journal.append([{'sql': 'INSERT INTO state VALUES (?,?)', 'args': ['a', 'true'], 'count': 1}])
        original = remote.db.execute('SELECT * FROM square_cluster').fetchone()
        with pytest.raises(DatabaseUnavailable, match='does not match'):
            journal.initialize('b' * 64)
        assert remote.db.execute('SELECT * FROM square_cluster').fetchone() == original
        assert remote.db.execute('SELECT COUNT(*) FROM square_journal').fetchone()[0] == 1
    finally:
        journal.client.close()
        remote.db.close()


def test_setup_transaction_uses_latest_baton_for_queries_and_commit():
    remote = RotatingJournal()
    journal = remote.journal()
    try:
        with journal.setup_transaction() as execute:
            execute('CREATE TABLE example (value TEXT)')
            execute('INSERT INTO example VALUES (?)', ('kept',))
        assert remote.db.execute('SELECT value FROM example').fetchone()[0] == 'kept'
        assert not remote.db.in_transaction
        assert not remote.batons
    finally:
        journal.client.close()
        remote.db.close()


@pytest.mark.parametrize('status', [401, 403, 429, 500])
def test_http_diagnostics_reveal_status_without_credentials(status):
    remote = HttpJournal()
    journal = remote.journal()
    journal.client.close()
    journal.client = httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(status, text='private URL and token')))
    try:
        with pytest.raises(DatabaseUnavailable, match='HTTP ' + str(status)) as caught:
            journal.query('SELECT 1')
        assert 'private' not in str(caught.value)
        assert 'test-org' not in str(caught.value)
    finally:
        journal.client.close()
        remote.db.close()
