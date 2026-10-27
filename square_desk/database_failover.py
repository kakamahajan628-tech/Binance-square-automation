"""Fenced PostgreSQL failover backed by an independent Turso mutation journal.

Journal commit precedes the PostgreSQL commit and every subsequent publication.
Lost commit acknowledgements are reconciled by sequence/hash, never by retrying
the user's mutation or a Binance POST. All nodes require a verified baseline.
"""
from contextlib import contextmanager,ExitStack
from concurrent.futures import ThreadPoolExecutor
import os
import logging
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
ENTITY_AFTERIMAGE='''INSERT INTO entities VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
    kind=excluded.kind,status=excluded.status,created=excluded.created,updated=excluded.updated,
    due=excluded.due,fingerprint=excluded.fingerprint,payload=excluded.payload'''
ENTITY_COLUMNS=('id','kind','status','created','updated','due','fingerprint','payload')
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
        self.failure_detail = {}
        self.audit_repair = {}
        self.audit_repairs = 0
        self.audit_attempted = None
        self.entity_repair = {}
        self.entity_repairs = 0
        self.entity_attempted = None
        self.snapshot_rebuild = {}
        self.rebuild_attempts = 0

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
        code = getattr(error, 'code', '')
        return code if code in {'unique_constraint_rejected', 'not_null_constraint_rejected',
                               'foreign_key_constraint_rejected', 'check_constraint_rejected'} else 'constraint_rejected'
    allowed = {'authentication_failed', 'permission_denied', 'table_missing',
               'column_missing', 'connection_limit', 'database_unavailable',
               'statement_timeout', 'lock_unavailable', 'read_only_connection',
               'connection_failed', 'connection_or_tls_failed', 'postgres_operation_failed',
               'replica_checkpoint_mismatch', 'replica_hash_mismatch', 'journal_history_missing',
               'journal_chain_mismatch', 'replica_row_count_mismatch', 'catch_up_pending',
               'unique_constraint_rejected', 'not_null_constraint_rejected',
               'foreign_key_constraint_rejected', 'check_constraint_rejected', 'constraint_rejected'}
    code = getattr(error, 'code', 'database_unavailable')
    return code if code in allowed else 'database_unavailable'


class RowCountMismatch(DatabaseUnavailable):
    def __init__(self, expected, actual):
        super().__init__('Replica rows differ from committed history; promotion held',
                         code='replica_row_count_mismatch')
        self.expected_rows, self.actual_rows = expected, actual


class ReplayRejected(DatabaseUnavailable):
    def __init__(self, error, sequence, position, sql):
        super().__init__('Replica replay rejected; promotion held', code=replica_error(error))
        match = re.match(r'^(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z_]+)\b', sql, re.I)
        table = match[2].lower() if match else ''
        self.failure_detail = {'journal_sequence': sequence, 'statement_position': position,
                               'table': table if table in {'entities', 'state', 'audit', 'budgets'} else 'unknown'}
        if isinstance(error, RowCountMismatch):
            operation = match[1].split()[0].lower() if match else 'unknown'
            self.failure_detail['operation'] = operation if operation in {'insert','update','delete'} else 'unknown'
            for name in ('expected_rows', 'actual_rows'):
                value = getattr(error, name, None)
                if type(value) is int and -1 <= value <= 1_000_000_000:
                    self.failure_detail[name] = value
        known = {'unique_fingerprint', 'entities_pkey', 'audit_pkey', 'state_pkey', 'budgets_pkey'}
        constraint = getattr(error, 'constraint', None)
        if isinstance(error, sqlite3.IntegrityError):
            self.failure_detail['constraint'] = constraint if isinstance(constraint, str) and constraint in known else 'other_constraint'


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
            for position, statement in enumerate(decode(entry['payload']), 1):
                try:
                    result = connection.execute(statement['sql'], tuple(statement['args']))
                    if result.rowcount != statement['count']:
                        raise RowCountMismatch(statement['count'], result.rowcount)
                except (DatabaseUnavailable, sqlite3.IntegrityError) as exc:
                    raise ReplayRejected(exc, entry['seq'], position, statement['sql']) from None
            connection.execute('UPDATE square_replica_meta SET seq=?,head_hash=? WHERE id=1',
                               (entry['seq'], entry['hash']))
        applied += 1
        if progress is not None:
            progress(entry['seq'])


class FailoverConnection:
    def __init__(self, nodes, journal, *, factory=PostgresConnection, background=True,entity_afterimages=True,snapshot_rebuilds=True):
        self.journal, self.factory = journal, factory
        self.nodes = {name: Node(name, url, factory) for name, url in nodes.items()}
        self.active, self.current, self.statements = False, None, []
        self.replicated, self.readonly = True, False
        self.lock, self.wake, self.stopped = RLock(), Event(), Event()
        self.switches, self.last_error, self.journal_ok = 0, '', False
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='square-db-replica') if background else None
        self.entity_proof_cache = {}
        self.entity_afterimages = entity_afterimages
        self.snapshot_rebuilds = snapshot_rebuilds
        self.rebuild_cache = None
        self.bootstrap=True
        # Do not create empty app tables or choose a stale node during startup.
        try:
            head = self._head()
        except WriterBusy:
            head = self.journal.observe()
            self.journal.head, self.journal_ok, self.last_error = head, True, 'writer_standby'
        started=time.monotonic()
        while True:
            try:
                self._select(head)
                break
            except DatabaseUnavailable:
                progressing=any(node.status=='catching_up' for node in self.nodes.values())
                if not progressing or time.monotonic()-started>=60:raise
                logging.getLogger('square_desk').warning('DATABASE_BOOTSTRAP catch_up_in_progress')
                try:head=self._head()
                except WriterBusy:head=self.journal.observe()
        self.bootstrap=False
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
            try:
                with self._verified_transaction(head,readonly=True):
                    self.active, self.readonly = True, True
                    yield
            except DatabaseUnavailable:
                self.nodes[self.current].failed()
                raise
            finally:
                self.active, self.readonly = False, False

    @contextmanager
    def _verified_transaction(self,head,*,readonly=False):
        # Retry selection/setup only. The caller's body is yielded exactly once;
        # no application mutation or external POST is replayed after an error.
        stack=None
        for attempt in range(len(self.nodes)):
            connection=self._select(head)
            stack=ExitStack()
            try:
                context=getattr(connection,'read_transaction',connection.transaction) if readonly else connection.transaction
                stack.enter_context(context())
                row=metadata(connection,head)
                if row['seq']!=head['seq'] or row['head_hash']!=head['head_hash']:
                    raise DatabaseUnavailable('Replica changed outside this writer; operation held',code='replica_checkpoint_mismatch')
            except DatabaseUnavailable as error:
                stack.__exit__(type(error),error,error.__traceback__)
                node=self.nodes[self.current]
                node.last_error=replica_error(error)
                node.failed()
                stack=None
                continue
            except BaseException as error:
                stack.__exit__(type(error),error,error.__traceback__);raise
            break
        if stack is None:raise DatabaseUnavailable('No verified current replica; operation held')
        with stack:yield connection

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
            node=self.nodes[self.current]
            if (node.status=='ready' and node.seq==head['seq'] and time.monotonic()>=node.until
                    and head.get('owner')==self.journal.owner and head.get('epoch')==self.journal.epoch):
                # Every caller still checks metadata under its own transaction.
                # Avoid a second, redundant locked transaction on a known writer.
                try:return node.ready()
                except DatabaseUnavailable as error:
                    node.last_error=replica_error(error)
                    node.failed()
        for name in names:
            node = self.nodes[name]
            if time.monotonic() < node.until:
                continue
            try:
                connection = node.ready()
                from .replica_batch import replay
                node.seq = replay(connection,self.journal,head,limit=32,stop=self.stopped)
                node.status, node.last_error, node.failure_detail = 'ready', '', {}
                if self.current and self.current != name:
                    self.switches += 1
                self.current = name
                return connection
            except CatchUpPending as exc:
                node.seq,node.status,node.until=exc.sequence,'catching_up',0
                node.last_error,node.failure_detail='',{}
                self.wake.set()
                if self.bootstrap:
                    self._bootstrap_checkpoint(connection,node,head)
            except (DatabaseUnavailable, sqlite3.IntegrityError) as exc:
                node.last_error = replica_error(exc)
                node.failure_detail = getattr(exc, 'failure_detail', {})
                node.failed('unavailable_or_not_current')
                if (self.snapshot_rebuilds and node.last_error=='replica_row_count_mismatch'
                        and node.failure_detail.get('table')=='entities'):
                    # The background worker can rebuild once another node has
                    # been selected; a deterministic fault needs no outage wait.
                    node.until=0
                    if self.bootstrap:self._bootstrap_checkpoint(connection,node,head)
        self.last_error = 'no_verified_current_replica'
        raise DatabaseUnavailable('No verified current PostgreSQL replica; publication held')

    def _bootstrap_checkpoint(self,connection,node,head):
        # A previously verified durable snapshot can recover a new process
        # even when its old source provider is offline. No new snapshot is
        # fabricated and no stale target is promoted before strict catch-up.
        if not self.snapshot_rebuilds or self.current is not None:return
        from types import SimpleNamespace
        from .replica_snapshot import SCHEMA,SnapshotCache,rebuild,safe_code
        try:
            connection=node.ready()
            self.journal.pipeline([(sql,()) for sql in SCHEMA])
            row=metadata(connection,head,allow_ahead=True)
            choices=self.journal.query('''SELECT provider FROM square_restore_checkpoints WHERE ready=1
                AND cluster=? AND baseline=? AND seq>? ORDER BY seq DESC LIMIT 1''',
                (head['cluster'],head['baseline'],row['seq']))
            if not choices:return
            if self.rebuild_cache is None:self.rebuild_cache=SnapshotCache()
            proof=SimpleNamespace(failure_detail={'journal_sequence':row['seq']+1})
            node.snapshot_rebuild=rebuild(connection,None,self.journal,head,proof,
                provider=choices[0]['provider'],cache=self.rebuild_cache,stop=self.stopped,
                source_current=lambda:self.current is None,secondary=lambda:self.current is None)
            node.seq=node.snapshot_rebuild['checkpoint_sequence']
            node.status,node.last_error,node.failure_detail,node.until='catching_up','',{},0
        except (DatabaseUnavailable,sqlite3.IntegrityError,OSError,ValueError,TypeError,KeyError) as error:
            node.snapshot_rebuild={'status':'held','reason':safe_code(error)}

    @contextmanager
    def transaction(self):
        with self.lock:
            if self.active:
                raise RuntimeError('Nested repository transactions are unsupported')
            head = self._head()
            self.statements = []
            entered=False
            try:
                with self._verified_transaction(head) as connection:
                    self.active = True
                    entered=True
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
                if entered:
                    self.last_error = 'operation_outcome_requires_journal_reconciliation'
                    self.journal.deadline = 0
                    self.nodes[self.current].failed()
                else:
                    self.last_error='waiting_for_verified_replica'
                    self.wake.set()
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
        afterimage = self.entity_afterimages and bool(re.match(r'^UPDATE\s+entities\s+SET\b',sql,re.I))
        query=sql+' RETURNING '+','.join(ENTITY_COLUMNS) if afterimage else sql
        result = connection.execute(query, bound if args is not None or bound else None)
        if verb != 'SELECT' and result.rowcount > 0:
            needed=result.rowcount if afterimage else 1
            if len(self.statements)+needed > 1000:
                raise DatabaseUnavailable('Database transaction statement limit reached')
            if afterimage:
                if len(result.rows)!=result.rowcount:
                    raise DatabaseUnavailable('Entity afterimage verification failed')
                for row in result.rows:
                    self.statements.append({'sql':ENTITY_AFTERIMAGE,
                        'args':[row[column] for column in ENTITY_COLUMNS],'count':1})
            else:
                self.statements.append({'sql': sql, 'args': list(bound), 'count': result.rowcount})
        return result

    def _replicate(self):
        clients = {}
        try:
            self._replicate_clients(clients)
        finally:
            for connection in clients.values():
                connection.close()
            if self.rebuild_cache is not None:
                self.rebuild_cache.close()

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
                    from .replica_batch import replay
                    node.seq = replay(connection, self.journal, head, limit=128, stop=self.stopped,
                                      progress=progress)
                    node.status, node.last_error, node.failure_detail = 'ready', '', {}
                except CatchUpPending as exc:
                    # A bounded successful batch is progress, not an outage.
                    # Continue in the background without a 60-second cooldown.
                    node.seq, node.status, node.last_error, node.until = exc.sequence, 'catching_up', '', 0
                    node.failure_detail = {}
                    self.wake.set()
                except (DatabaseUnavailable, sqlite3.IntegrityError) as exc:
                    node.last_error = replica_error(exc)
                    node.failure_detail = getattr(exc, 'failure_detail', {})
                    if (self.snapshot_rebuilds and node.last_error=='replica_row_count_mismatch'
                            and node.failure_detail.get('table')=='entities' and connection is not None):
                        # A new Render process can read the current provider
                        # while the retiring process still owns the journal.
                        # Standby is normal: do not attempt a fenced repair or
                        # quarantine this replica before ownership is acquired.
                        if self.journal.epoch is None or time.monotonic()>=self.journal.deadline:
                            node.status,node.until='rebuild_waiting_for_writer',time.monotonic()+5
                            node.snapshot_rebuild={'status':'waiting_for_writer',
                                'reason':'writer_lease_not_locally_valid'}
                            continue
                        from .replica_snapshot import SnapshotCache,rebuild,safe_code
                        if self.rebuild_cache is None:
                            self.rebuild_cache=SnapshotCache()
                        node.rebuild_attempts+=1
                        node.status='snapshot_rebuilding'
                        node.snapshot_rebuild={'status':'capturing_verified_checkpoint'}
                        try:
                            source_name=self.current
                            if source_name is None or source_name==name:
                                raise DatabaseUnavailable('Snapshot source unavailable',code='snapshot_source_unavailable')
                            source=self.factory(self.nodes[source_name].url)
                            try:
                                node.snapshot_rebuild=rebuild(connection,source,self.journal,head,exc,
                                    provider=source_name,cache=self.rebuild_cache,stop=self.stopped,
                                    source_current=lambda:self.current==source_name,
                                    secondary=lambda:self.current!=name)
                            finally:
                                source.close()
                        except (DatabaseUnavailable,sqlite3.IntegrityError,OSError,ValueError,TypeError,KeyError) as error:
                            code=safe_code(error)
                            node.snapshot_rebuild={'status':'held','reason':code}
                            if code in {'database_unavailable','postgres_operation_failed','connection_failed',
                                        'connection_or_tls_failed','statement_timeout','lock_unavailable',
                                        'snapshot_fence_changed','snapshot_source_changed','snapshot_target_changed'}:
                                delay=min(3600,60*2**min(node.rebuild_attempts-1,6))
                                node.status,node.until='rebuild_retry_wait',time.monotonic()+delay
                                connection.close();clients.pop(name,None)
                                continue
                            node.status,node.until='verification_failed',float('inf')
                            connection.close();clients.pop(name,None)
                            continue
                        else:
                            node.seq=node.snapshot_rebuild['checkpoint_sequence']
                            node.status='ready' if node.seq==head['seq'] else 'catching_up'
                            node.last_error,node.failure_detail,node.until='',{},0
                            self.wake.set()
                            continue
                    if (node.entity_repairs < 4 and node.last_error == 'replica_row_count_mismatch'
                            and node.failure_detail.get('table') == 'entities'
                            and node.failure_detail.get('operation') == 'update'
                            and node.failure_detail.get('expected_rows') == 1
                            and node.failure_detail.get('actual_rows') == 0
                            and node.entity_attempted != (node.seq,node.failure_detail.get('journal_sequence'))
                            and connection is not None):
                        from .replica_entity_repair import repair
                        node.entity_attempted = (node.seq,node.failure_detail.get('journal_sequence'))
                        node.status = 'verifying_entity_history'
                        try:
                            node.entity_repair = repair(connection,self.journal,head,exc,
                                is_secondary=lambda: name != self.current,stop=self.stopped,cache=self.entity_proof_cache)
                        except (DatabaseUnavailable,sqlite3.IntegrityError,ValueError,TypeError,KeyError) as repair_error:
                            code = getattr(repair_error,'code','entity_repair_held')
                            allowed = {'entity_repair_not_applicable','entity_history_bound_or_checkpoint',
                                'entity_requires_complete_fresh_history','replica_checkpoint_mismatch',
                                'journal_history_missing','journal_chain_mismatch','entity_proof_cancelled_or_time_bound',
                                'entity_proof_payload_bound','entity_history_unsupported','entity_history_count_mismatch',
                                'entity_canonical_missing','entity_row_bound','entity_target_not_proven',
                                'entity_proof_sql_or_storage_bound','entity_primary_protected','entity_row_already_canonical',
                                'entity_archive_bound','entity_archive_unverified','entity_restore_unverified'}
                            node.entity_repair = {'status':'held','reason':code if code in allowed else 'entity_repair_held'}
                        else:
                            node.entity_repairs += 1
                            node.entity_attempted = None
                            node.status,node.last_error,node.failure_detail,node.until = 'catching_up','',{},0
                            self.wake.set()
                            continue
                    if (node.audit_repairs < 8 and node.last_error == 'unique_constraint_rejected'
                            and node.failure_detail.get('table') == 'audit'
                            and node.failure_detail.get('constraint') == 'audit_pkey'
                            and node.audit_attempted != (node.seq,node.failure_detail.get('journal_sequence'))
                            and connection is not None):
                        from .replica_audit_repair import repair
                        node.audit_attempted = (node.seq,node.failure_detail.get('journal_sequence'))
                        try:
                            node.audit_repair = repair(connection,self.journal,head,exc,
                                is_secondary=lambda: name != self.current,stop=self.stopped)
                        except (DatabaseUnavailable,sqlite3.IntegrityError,ValueError,TypeError,KeyError) as repair_error:
                            code = getattr(repair_error,'code','audit_repair_held')
                            allowed = {'audit_repair_requires_fresh_history','audit_repair_history_bound',
                                'audit_repair_not_applicable','audit_repair_not_proven_extra','audit_repair_cancelled',
                                'audit_repair_unsupported_history','audit_repair_invalid_history',
                                'journal_history_missing','journal_chain_mismatch','replica_checkpoint_mismatch',
                                'audit_repair_conflict_changed','audit_repair_archive_bound','audit_repair_archive_unverified'}
                            node.audit_repair = {'status':'held','reason':code if code in allowed else 'audit_repair_held'}
                        else:
                            node.audit_repairs += 1
                            node.audit_attempted = None
                            node.status,node.last_error,node.failure_detail,node.until = 'catching_up','',{},0
                            self.wake.set()
                            continue
                    node.status, node.until = 'catch_up_pending_or_unavailable', time.monotonic() + 60
                    if node.last_error in {'replica_row_count_mismatch','replica_hash_mismatch',
                            'replica_checkpoint_mismatch','journal_chain_mismatch','journal_history_missing'}:
                        # Deterministic verification failure is not an outage.
                        # Do not replay the same bad entry every minute forever.
                        node.status,node.until = 'verification_failed',float('inf')
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
            return {'enabled': True, 'implementation_revision': '2026-10-15-snapshot-handoff-11',
                    'active_provider': self.current, 'switches': self.switches,
                    'writer_owned': self.journal.epoch is not None and time.monotonic() < self.journal.deadline,
                    'journal': 'turso', 'journal_available': self.journal_ok,
                    'journal_sequence': head.get('seq'), 'journal_bytes': head.get('log_bytes'),
                    'journal_capacity_bytes': self.journal.max_bytes,
                    'last_error': self.last_error,
                    'providers': {name: {'status': node.status, 'applied_sequence': node.seq,
                                        'pending_entries': None if node.seq is None else max(0, head.get('seq',0)-node.seq),
                                        'last_error': node.last_error, 'failure_detail': dict(node.failure_detail),
                                        'audit_repair': dict(node.audit_repair),
                                        'entity_repair': dict(node.entity_repair),
                                        'snapshot_rebuild': dict(node.snapshot_rebuild),
                                        'retry_paused_until_restart': node.until == float('inf')}
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
            self.executor.shutdown(wait=True, cancel_futures=True)
        self.close()
        try:
            self.journal.release()
        except DatabaseUnavailable:
            pass  # Server-side lease expires; shutdown must still close clients.
        finally:
            self.journal.close()
