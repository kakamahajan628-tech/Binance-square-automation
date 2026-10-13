"""Fenced PostgreSQL failover backed by an independent Turso mutation journal.

Journal commit precedes the PostgreSQL commit and every subsequent publication.
Lost commit acknowledgements are reconciled by sequence/hash, never by retrying
the user's mutation or a Binance POST. All nodes require a verified baseline.
"""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import os
import re
import sqlite3
from threading import Event, RLock
import time
from urllib.parse import urlsplit, parse_qs
from .postgres import DatabaseUnavailable, PostgresConnection
from .turso_journal import TursoJournal, WriterBusy, chain, decode


META_SCHEMA = '''CREATE TABLE IF NOT EXISTS square_replica_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), cluster TEXT NOT NULL, baseline TEXT NOT NULL,
 seq BIGINT NOT NULL, head_hash TEXT NOT NULL)'''
ENABLED = 'DESK_DB_FALLBACK_ENABLED'
URL_KEYS = {'aiven': 'DESK_AIVEN_DATABASE_URL', 'supabase': 'DESK_SUPABASE_DATABASE_URL',
            'neon': 'DESK_NEON_DATABASE_URL'}


def postgres_url(value):
    try:
        p = urlsplit(value)
        port = p.port  # Validate even when the optional default port is omitted.
        modes = parse_qs(p.query).get('sslmode', [])
        if (p.scheme not in ('postgres', 'postgresql') or not p.hostname or not p.path.strip('/')
                or len(modes) != 1 or modes[0] not in ('require', 'verify-ca', 'verify-full')
                or any(c.isspace() for c in value) or p.fragment):
            raise ValueError
    except ValueError:
        raise ValueError('Invalid fallback PostgreSQL URI; include host, database and one secure sslmode') from None
    return value


def environment_nodes(default_url):
    order = [v.strip().lower() for v in os.getenv('DESK_DB_FALLBACK_ORDER', 'neon,aiven,supabase').split(',')]
    if len(set(order)) != len(order) or not order or any(v not in URL_KEYS for v in order):
        raise ValueError('Database fallback order must contain unique aiven, supabase or neon names')
    nodes = {name: os.getenv(URL_KEYS[name], '') for name in order}
    if not nodes.get('aiven') and 'aiven' in nodes and (urlsplit(default_url).hostname or '').endswith('.aivencloud.com'):
        nodes['aiven'] = default_url
    if not nodes.get('neon') and 'neon' in nodes and (urlsplit(default_url).hostname or '').endswith('.neon.tech'):
        nodes['neon'] = default_url
    nodes = {name: postgres_url(url) for name, url in nodes.items() if url}
    if len(nodes) < 2 or len({(urlsplit(url).hostname, urlsplit(url).port or 5432, urlsplit(url).path) for url in nodes.values()}) != len(nodes):
        raise ValueError('Fallback requires at least two distinct, verified PostgreSQL databases')
    return nodes


def make_connection(path):
    value = os.getenv(ENABLED, 'false').strip().lower()
    if value not in ('true', 'false'):
        raise ValueError('DESK_DB_FALLBACK_ENABLED must be true or false')
    if value == 'false':
        connection = PostgresConnection(path)
        try:
            row = connection.execute("SELECT to_regclass('public.square_replica_meta') AS name").fetchone()
            if row and row.get('name') and connection.execute('SELECT cluster FROM square_replica_meta WHERE id=1').fetchone():
                raise DatabaseUnavailable('Registered cluster cannot use a direct unfenced writer; enable its verified fallback configuration')
            return connection
        except BaseException:
            connection.close()
            raise
    nodes = environment_nodes(path)
    journal = TursoJournal(os.getenv('DESK_TURSO_JOURNAL_URL', ''),
                          os.getenv('DESK_TURSO_JOURNAL_TOKEN', ''),
                          os.getenv('DESK_DB_CLUSTER_ID', ''),max_bytes=1_024_000_000)
    try:
        from .fresh_cluster import ensure
        ensure(nodes,journal)
        def connection(url):
            result = PostgresConnection(url,connect_timeout=3)
            if getattr(journal,'fresh_states',None):
                from .fresh_cluster import prepare_node
                try:
                    exists = result.execute("SELECT to_regclass('public.square_replica_meta') AS name").fetchone()
                    if not exists or not exists['name']:
                        prepare_node(result,journal.cluster,journal.fresh_baseline,journal.fresh_states)
                except BaseException:
                    result.close()
                    raise
            return result
        return FailoverConnection(nodes, journal, factory=connection)
    except BaseException:
        try:
            journal.release()
        except DatabaseUnavailable:
            pass
        journal.close()
        raise


class Node:
    def __init__(self, name, url, factory):
        self.name, self.url, self.factory = name, url, factory
        self.connection, self.until, self.status, self.seq = None, 0, 'not_checked', None
        self.last_error = ''

    def ready(self):
        if self.connection is None:
            self.connection = self.factory(self.url)
        return self.connection

    def failed(self, status='unavailable'):
        self.status, self.until = status, time.monotonic() + 60
        self.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class CatchUpPending(DatabaseUnavailable):
    def __init__(self, sequence):
        super().__init__('Replica catch-up pending; promotion held', code='catch_up_pending')
        self.sequence = sequence


def replica_error(error):
    if isinstance(error, sqlite3.IntegrityError):
        return 'constraint_rejected'
    allowed = {'authentication_failed', 'permission_denied', 'table_missing',
               'column_missing', 'connection_limit', 'database_unavailable',
               'statement_timeout', 'lock_unavailable', 'read_only_connection',
               'connection_failed', 'connection_or_tls_failed', 'postgres_operation_failed',
               'replica_checkpoint_mismatch', 'replica_hash_mismatch', 'journal_history_missing',
               'journal_chain_mismatch', 'replica_row_count_mismatch', 'catch_up_pending'}
    code = getattr(error, 'code', 'database_unavailable')
    return code if code in allowed else 'database_unavailable'


def metadata(connection, head, *, allow_ahead=False):
    row = connection.execute('SELECT * FROM square_replica_meta WHERE id=1').fetchone()
    if (not row or row['cluster'] != head['cluster'] or row['baseline'] != head['baseline']
            or type(row['seq']) is not int or row['seq'] < 0 or (row['seq'] > head['seq'] and not allow_ahead)
            or not re.fullmatch('[a-f0-9]{64}', row['head_hash'])):
        raise DatabaseUnavailable('Replica not registered against this checkpoint; promotion held',
                                  code='replica_checkpoint_mismatch')
    return row


def catch_up(connection, journal, head, *, limit=128, stop=None, progress=None):
    applied = 0
    while True:
        if stop is not None and stop.is_set():
            raise DatabaseUnavailable('Replica background task stopped')
        with connection.transaction():
            row = metadata(connection, head, allow_ahead=True)
            if progress is not None:
                progress(row['seq'])
            if row['seq'] > head['seq']:
                # Another instance may have committed between the observer's
                # authority read and the node lock. Refresh under that lock.
                head = journal.observe()
                row = metadata(connection, head)
            if row['seq'] == head['seq']:
                if row['head_hash'] != head['head_hash']:
                    raise DatabaseUnavailable('Replica history hash mismatch; promotion held', code='replica_hash_mismatch')
                return row['seq']
            if applied >= limit:
                raise CatchUpPending(row['seq'])
            entries = journal.entries(row['seq'], head['seq'])
            if len(entries) != 1:
                raise DatabaseUnavailable('Required journal history missing; promotion held', code='journal_history_missing')
            entry = entries[0]
            if (entry['seq'] != row['seq'] + 1 or entry['prev'] != row['head_hash']
                    or entry['hash'] != chain(entry['prev'], entry['payload'])):
                raise DatabaseUnavailable('Journal chain mismatch; promotion held', code='journal_chain_mismatch')
            for statement in decode(entry['payload']):
                result = connection.execute(statement['sql'], tuple(statement['args']))
                if result.rowcount != statement['count']:
                    raise DatabaseUnavailable('Replica rows differ from committed history; promotion held', code='replica_row_count_mismatch')
            connection.execute('UPDATE square_replica_meta SET seq=?,head_hash=? WHERE id=1',
                               (entry['seq'], entry['hash']))
        applied += 1
        if progress is not None:
            progress(entry['seq'])


class FailoverConnection:
    def __init__(self, nodes, journal, *, factory=PostgresConnection, background=True):
        self.journal, self.factory = journal, factory
        self.nodes = {name: Node(name, url, factory) for name, url in nodes.items()}
        self.active, self.current, self.statements = False, None, []
        self.replicated, self.readonly = True, False
        self.lock, self.wake, self.stopped = RLock(), Event(), Event()
        self.switches, self.last_error, self.journal_ok = 0, '', False
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='square-db-replica') if background else None
        # Do not create empty app tables or choose a stale node during startup.
        try:
            head = self._head()
        except WriterBusy:
            head = self.journal.observe()
            self.journal.head, self.journal_ok, self.last_error = head, True, 'writer_standby'
        self._select(head)
        if self.executor:
            self.executor.submit(self._replicate)

    def writer_available(self):
        try:
            self._head()
            return True
        except WriterBusy:
            return False

    @contextmanager
    def read_transaction(self):
        with self.lock:
            try:
                head = self._head()
            except WriterBusy:
                head = self.journal.observe()
                self.journal.head, self.journal_ok, self.last_error = head, True, 'writer_standby'
            connection = self._select(head)
            try:
                with connection.transaction():
                    self.active, self.readonly = True, True
                    yield
            except DatabaseUnavailable:
                self.nodes[self.current].failed()
                raise
            finally:
                self.active, self.readonly = False, False

    def _head(self):
        try:
            head = self.journal.acquire()
            self.journal_ok, self.last_error = True, ''
            return head
        except DatabaseUnavailable:
            self.journal_ok, self.last_error = False, 'journal_unavailable_or_writer_busy'
            raise

    def _select(self, head):
        # Keep the working node. Recovered providers are caught up in background,
        # but never force an automatic failback or rewrite newer history.
        names = list(self.nodes)
        if self.current:
            names.remove(self.current)
            names.insert(0, self.current)
        for name in names:
            node = self.nodes[name]
            if time.monotonic() < node.until:
                continue
            try:
                connection = node.ready()
                node.seq = catch_up(connection, self.journal, head)
                node.status, node.last_error = 'ready', ''
                if self.current and self.current != name:
                    self.switches += 1
                self.current = name
                return connection
            except (DatabaseUnavailable, sqlite3.IntegrityError) as exc:
                node.last_error = replica_error(exc)
                node.failed('unavailable_or_not_current')
        self.last_error = 'no_verified_current_replica'
        raise DatabaseUnavailable('No verified current PostgreSQL replica; publication held')

    @contextmanager
    def transaction(self):
        with self.lock:
            if self.active:
                raise RuntimeError('Nested repository transactions are unsupported')
            head = self._head()
            connection = self._select(head)
            self.statements = []
            try:
                with connection.transaction():
                    # Recheck after taking the node's transaction lock.
                    row = metadata(connection, head)
                    if row['seq'] != head['seq'] or row['head_hash'] != head['head_hash']:
                        raise DatabaseUnavailable('Replica changed outside this writer; transaction held')
                    self.active = True
                    yield
                    if self.statements:
                        # Atomic CAS + trigger records complete transaction first.
                        # If HTTP acknowledgement is lost, local PG rolls back.
                        # Next operation replays the journal iff it actually exists.
                        head = self.journal.append(self.statements)
                        connection.execute('UPDATE square_replica_meta SET seq=?,head_hash=? WHERE id=1',
                                           (head['seq'], head['head_hash']))
                self.nodes[self.current].seq = head['seq']
                self.wake.set()
            except DatabaseUnavailable:
                self.last_error = 'operation_outcome_requires_journal_reconciliation'
                self.journal.deadline = 0
                self.nodes[self.current].failed()
                raise  # NEVER rerun caller code or return success on uncertain commit.
            finally:
                self.active, self.statements = False, []

    def execute(self, sql, args=None):
        if not self.active:
            context = self.read_transaction if sql.strip().upper().startswith('SELECT ') else self.transaction
            with context():
                return self.execute(sql, args)
        connection = self.nodes[self.current].ready()
        sql = sql.strip()
        verb = sql.split(None, 1)[0].upper()
        if self.readonly and verb != 'SELECT':
            raise DatabaseUnavailable('Standby reads cannot mutate application data')
        if verb == 'CREATE':
            # Existing Store bootstraps only IF NOT EXISTS schema/index definitions.
            # Cluster registration already verified those tables independently.
            if not re.match(r'^CREATE (?:UNIQUE )?(?:TABLE|INDEX) IF NOT EXISTS ', sql, re.I):
                raise DatabaseUnavailable('Schema change requires coordinated maintenance')
            return connection.execute(sql, args)
        if verb not in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
            raise DatabaseUnavailable('Unsupported SQL in replicated repository')
        bound = tuple(args or ())
        # Audit identities must be identical across providers. Do not replay
        # independent serial generators with potentially different sequence values.
        if re.match(r'^INSERT INTO audit\(at,category,correlation,message\)', sql, re.I):
            ident = connection.execute('SELECT COALESCE(MAX(id),0)+1 AS id FROM audit').fetchone()[0]
            sql, bound = 'INSERT INTO audit(id,at,category,correlation,message) VALUES (?,?,?,?,?)', (ident, *bound)
        result = connection.execute(sql, bound if args is not None or bound else None)
        if verb != 'SELECT' and result.rowcount > 0:
            if len(self.statements) >= 1000:
                raise DatabaseUnavailable('Database transaction statement limit reached')
            self.statements.append({'sql': sql, 'args': list(bound), 'count': result.rowcount})
        return result

    def _replicate(self):
        clients = {}
        try:
            self._replicate_clients(clients)
        finally:
            for connection in clients.values():
                connection.close()

    def _replicate_clients(self,clients):
        while not self.stopped.is_set():
            self.wake.wait(5)
            self.wake.clear()
            if self.stopped.is_set():
                break
            with self.lock:
                head = dict(self.journal.head or {})
                current = self.current
            if not head:
                continue
            for name, node in self.nodes.items():
                if name == current or time.monotonic() < node.until or node.seq == head['seq']:
                    continue
                connection = None
                try:
                    # Separate connection and one bounded background thread.
                    connection = clients.get(name)
                    if connection is None:
                        connection = clients[name] = self.factory(node.url)
                    def progress(sequence):
                        # Diagnostics only: never take the foreground lock
                        # while holding the replica's PostgreSQL transaction.
                        node.seq = sequence
                        node.status = 'catching_up'
                    node.seq = catch_up(connection, self.journal, head, limit=128, stop=self.stopped,
                                        progress=progress)
                    node.status, node.last_error = 'ready', ''
                except CatchUpPending as exc:
                    # A bounded successful batch is progress, not an outage.
                    # Continue in the background without a 60-second cooldown.
                    node.seq, node.status, node.last_error, node.until = exc.sequence, 'catching_up', '', 0
                    self.wake.set()
                except (DatabaseUnavailable, sqlite3.IntegrityError) as exc:
                    node.last_error = replica_error(exc)
                    node.status, node.until = 'catch_up_pending_or_unavailable', time.monotonic() + 60
                    if connection:
                        connection.close()
                        clients.pop(name,None)
            with self.lock:
                self._trim_ready()

    def _trim_ready(self):
        head = self.journal.head or {}
        sequences = [node.seq for node in self.nodes.values()]
        if (head.get('log_bytes',0)>32_000_000 and sequences and all(s is not None for s in sequences)):
            # Never remove a mutation that any configured node still needs.
            floor = min(sequences)-1024
            if floor>0:
                try:
                    self.journal.trim(floor)
                except DatabaseUnavailable:
                    self.last_error = 'journal_trim_held'

    def diagnostics(self):
        with self.lock:
            head = self.journal.head or {}
            return {'enabled': True, 'implementation_revision': '2026-10-13-replica-progress-2',
                    'active_provider': self.current, 'switches': self.switches,
                    'writer_owned': self.journal.epoch is not None and time.monotonic() < self.journal.deadline,
                    'journal': 'turso', 'journal_available': self.journal_ok,
                    'journal_sequence': head.get('seq'), 'journal_bytes': head.get('log_bytes'),
                    'journal_capacity_bytes': self.journal.max_bytes,
                    'last_error': self.last_error,
                    'providers': {name: {'status': node.status, 'applied_sequence': node.seq,
                                        'pending_entries': None if node.seq is None else max(0, head.get('seq',0)-node.seq),
                                        'last_error': node.last_error}
                                  for name, node in self.nodes.items()}}

    def close(self):
        # Existing saver idle() calls close(): drop PG clients, preserve authority
        # and background replication. shutdown() is the final lifecycle operation.
        with self.lock:
            for node in self.nodes.values():
                node.close()

    def shutdown(self):
        self.stopped.set()
        self.wake.set()
        if self.executor:
            self.executor.shutdown(wait=False, cancel_futures=True)
        self.close()
        try:
            self.journal.release()
        except DatabaseUnavailable:
            pass  # Server-side lease expires; shutdown must still close clients.
        finally:
            self.journal.close()
