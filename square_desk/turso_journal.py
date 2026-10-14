"""One authoritative, fenced mutation journal over Turso's libSQL HTTP API.

PostgreSQL providers are interchangeable materialized copies, not independent
writers. A journal outage stops all writes. No raw service errors are exposed.
"""
from contextlib import contextmanager
import base64
import hashlib
import json
import re
import time
import uuid
import zlib
from urllib.parse import urlsplit
import httpx
from .postgres import DatabaseUnavailable
from .bandwidth import record


MAX_ENTRY = 8_000_000
MAX_DECODED = 32_000_000
SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS square_cluster (
      id INTEGER PRIMARY KEY CHECK(id=1), cluster TEXT NOT NULL,
      baseline TEXT NOT NULL, seq INTEGER NOT NULL, head_hash TEXT NOT NULL,
      owner TEXT NOT NULL, epoch INTEGER NOT NULL, lease_until INTEGER NOT NULL,
      log_bytes INTEGER NOT NULL, migration_marker TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS square_journal (
      seq INTEGER PRIMARY KEY, op TEXT NOT NULL UNIQUE,
      prev TEXT NOT NULL, hash TEXT NOT NULL, payload TEXT NOT NULL)''',
    '''CREATE TRIGGER IF NOT EXISTS square_journal_advance
      AFTER INSERT ON square_journal BEGIN
      UPDATE square_cluster SET seq=NEW.seq, head_hash=NEW.hash,
      log_bytes=log_bytes+length(NEW.payload), lease_until=unixepoch()+60 WHERE id=1;
      END''',
    '''CREATE TRIGGER IF NOT EXISTS square_journal_trim
      AFTER DELETE ON square_journal BEGIN
      UPDATE square_cluster SET log_bytes=MAX(0,log_bytes-length(OLD.payload)) WHERE id=1;
      END''',
)


class WriterBusy(DatabaseUnavailable):
    """A healthy previous instance owns authority; HTTP standby is permitted."""


def encode(statements):
    raw = json.dumps(statements, allow_nan=False, ensure_ascii=False,
                     separators=(',', ':')).encode()
    if len(raw) > MAX_DECODED:
        raise DatabaseUnavailable('Database transaction exceeds journal bounds')
    payload = base64.b64encode(zlib.compress(raw)).decode('ascii')
    if len(payload) > MAX_ENTRY:
        raise DatabaseUnavailable('Database journal entry exceeds bounds')
    return payload


def decode(payload):
    try:
        if not isinstance(payload, str) or len(payload) > MAX_ENTRY:
            raise ValueError
        compressed = base64.b64decode(payload, validate=True)
        unpacker = zlib.decompressobj()
        raw = unpacker.decompress(compressed, MAX_DECODED + 1)
        if len(raw) > MAX_DECODED or not unpacker.eof or unpacker.unused_data:
            raise ValueError
        statements = json.loads(raw)
        if not isinstance(statements, list) or len(statements) > 1000:
            raise ValueError
        for item in statements:
            if (not isinstance(item, dict) or set(item) != {'sql', 'args', 'count'}
                    or not isinstance(item['sql'], str) or not isinstance(item['args'], list)
                    or type(item['count']) is not int or item['count'] < 0
                    or not re.match(r'^(INSERT|UPDATE|DELETE)\s', item['sql'], re.I)
                    or any(v is not None and type(v) not in (str, int, float) for v in item['args'])):
                raise ValueError
        return statements
    except (ValueError, TypeError, zlib.error, UnicodeError):
        raise DatabaseUnavailable('Database journal entry invalid; promotion held') from None


def chain(previous, payload):
    return hashlib.sha256((previous + '\n' + payload).encode()).hexdigest()


def argument(value):
    if value is None:
        return {'type': 'null'}
    if type(value) is int:
        return {'type': 'integer', 'value': str(value)}
    if type(value) is float:
        return {'type': 'float', 'value': value}
    if isinstance(value, str):
        return {'type': 'text', 'value': value}
    raise DatabaseUnavailable('Unsupported journal parameter')


class TursoJournal:
    def __init__(self, url, token, cluster, *, client=None, max_bytes=256_000_000):
        parsed = urlsplit(url.replace('libsql://', 'https://', 1))
        if (parsed.scheme != 'https' or not (parsed.hostname or '').endswith('.turso.io')
                or parsed.username or parsed.password or parsed.port not in (None, 443)
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment
                or not token or not re.fullmatch(r'[a-zA-Z0-9_-]{8,64}', cluster)):
            raise ValueError('Invalid Turso journal configuration; use a libSQL database URL and database token')
        self.url = 'https://' + parsed.hostname + '/v2/pipeline'
        self.token, self.cluster, self.max_bytes = token, cluster, max_bytes
        self.client = client or httpx.Client(timeout=4, follow_redirects=False, trust_env=False)
        self.owned_client = client is None
        self.owner, self.epoch, self.deadline = uuid.uuid4().hex, None, 0
        self.head = None

    def pipeline(self, statements, *, baton=None, close=True):
        requests = [{'type': 'execute', 'stmt': {'sql': sql, 'args': [argument(v) for v in values]}}
                    for sql, values in statements]
        if close:
            requests.append({'type': 'close'})
        request = {'requests': requests}
        if baton:
            request['baton'] = baton
        record('turso_request_payload_bytes',len(json.dumps(request,ensure_ascii=False).encode())+300)
        failure = 'response invalid'
        try:
            with self.client.stream('POST', self.url,
                    headers={'Authorization': 'Bearer ' + self.token}, json=request) as response:
                if response.status_code != 200:
                    failure = 'HTTP ' + str(response.status_code)
                    raise ValueError
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > 12_000_000:
                        raise ValueError
                    chunks.append(chunk)
                body = json.loads(b''.join(chunks))
            results = body['results']
            if len(results) != len(requests):
                raise ValueError
            decoded = []
            for index, item in enumerate(results[:len(statements)]):
                if item.get('type') != 'ok' or item['response']['type'] != 'execute':
                    failure = 'SQL operation ' + str(index + 1) + ' rejected'
                    raise ValueError
                data = item['response']['result']
                rows = []
                for values in data['rows']:
                    row = {}
                    if len(values) != len(data['cols']):
                        raise ValueError
                    for col, value in zip(data['cols'], values):
                        kind = value['type']
                        v = value.get('value')
                        row[col['name']] = (None if kind == 'null' else int(v) if kind == 'integer'
                                            else float(v) if kind == 'float' else v if kind == 'text' else None)
                    rows.append(row)
                decoded.append(rows)
            return decoded, body.get('baton')
        except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError) as exc:
            self.deadline = 0
            if isinstance(exc, httpx.TimeoutException):
                failure = 'request timed out'
            elif isinstance(exc, httpx.HTTPError):
                failure = 'connection failed'
            raise DatabaseUnavailable('Turso journal ' + failure + '; database switching held') from None

    def query(self, sql, values=()):
        return self.pipeline([(sql, values)])[0][0]

    @contextmanager
    def setup_transaction(self):
        results, baton = self.pipeline([('BEGIN IMMEDIATE', ())], close=False)
        if not baton:
            raise DatabaseUnavailable('Journal setup transaction unavailable')
        def execute(sql, values=()):
            nonlocal baton
            rows, next_baton = self.pipeline([(sql, values)], baton=baton, close=False)
            if not next_baton:
                raise DatabaseUnavailable('Journal setup connection expired')
            baton = next_baton
            return rows[0]
        try:
            yield execute
            self.pipeline([('COMMIT', ())], baton=baton)
        except BaseException:
            try:
                self.pipeline([('ROLLBACK', ())], baton=baton)
            except DatabaseUnavailable:
                pass
            raise

    def initialize(self, baseline, marker=None):
        if not re.fullmatch('[a-f0-9]{64}', baseline):
            raise ValueError('Invalid baseline digest')
        # One HTTP round trip avoids rotating batons and the five-second
        # transaction window being consumed by several network round trips.
        # The only metadata insert is conditional: existing history is never
        # overwritten, even if validation below refuses this initialization.
        statements = [('BEGIN IMMEDIATE', ())]
        statements.extend((sql, ()) for sql in SCHEMA)
        statements.extend([
            ('INSERT INTO square_cluster VALUES (1,?,?,0,?,\'\',0,0,0,?) ON CONFLICT DO NOTHING',
             (self.cluster, baseline, baseline, json.dumps(marker or {}, allow_nan=False))),
            ('SELECT * FROM square_cluster WHERE id=1', ()),
            ('COMMIT', ()),
        ])
        results, _ = self.pipeline(statements)
        rows = results[-2]
        if (len(rows) != 1 or rows[0]['cluster'] != self.cluster
                or rows[0]['baseline'] != baseline or rows[0]['seq'] != 0):
            raise DatabaseUnavailable('Existing journal does not match initial checkpoint; setup refused')

    def acquire(self, *, force=False):
        if not force and self.head and time.monotonic() < self.deadline:
            return self.head
        started = time.monotonic()
        rows = self.query('''UPDATE square_cluster SET owner=?,
          epoch=CASE WHEN owner=? THEN epoch ELSE epoch+1 END, lease_until=unixepoch()+60
          WHERE id=1 AND cluster=? AND (owner=? OR lease_until<=unixepoch()) RETURNING *''',
          (self.owner, self.owner, self.cluster, self.owner))
        if len(rows) != 1:
            self.deadline = 0
            raise WriterBusy('Another database writer owns the journal; standby')
        self.head = rows[0]
        self.epoch = self.head['epoch']
        self.deadline = started + 45  # Conservative server lease; local wall clock is never authority.
        return self.head

    def observe(self):
        # A no-op write forces authoritative routing; do not read a possibly
        # lagging HTTP read replica when deciding which PG prefix is current.
        rows = self.query('UPDATE square_cluster SET id=id WHERE id=1 AND cluster=? RETURNING *',
                          (self.cluster,))
        if len(rows) != 1:
            raise DatabaseUnavailable('Journal cluster unavailable; standby reads held')
        return rows[0]

    def append(self, statements):
        head = self.acquire()
        payload = encode(statements)
        op, sha, started = uuid.uuid4().hex, chain(head['head_hash'], payload), time.monotonic()
        rows = self.query('''INSERT INTO square_journal(seq,op,prev,hash,payload)
          SELECT seq+1,?,?,?,? FROM square_cluster
          WHERE id=1 AND cluster=? AND owner=? AND epoch=? AND lease_until>unixepoch()+5
            AND seq=? AND head_hash=? AND log_bytes+length(?)<=?
          RETURNING seq,op,prev,hash''',
          (op, head['head_hash'], sha, payload, self.cluster, self.owner, self.epoch,
           head['seq'], head['head_hash'], payload, self.max_bytes))
        if len(rows) != 1:
            self.deadline = 0
            raise DatabaseUnavailable('Journal fence, history or capacity changed; mutation withheld')
        self.head = {**head, 'seq': rows[0]['seq'], 'head_hash': sha,
                     'log_bytes': head['log_bytes'] + len(payload)}
        self.deadline = started + 45
        return self.head

    def entries(self, after, through):
        # One bounded entry per request: media can make an entry several MB.
        return self.query('SELECT * FROM square_journal WHERE seq>? AND seq<=? ORDER BY seq LIMIT 1',
                          (after, through))

    def entries_batch(self, after, through, *, limit=32, max_payload=2_000_000):
        if type(limit) is not int or not 1 <= limit <= 32 or not 1 <= max_payload <= 2_000_000:
            raise ValueError('Invalid bounded journal batch')
        # Payload text is ASCII base64. Return a large first entry alone, or
        # at most 32 entries with <=2 MB combined payload, below the HTTP cap.
        return self.query('''WITH candidates AS (
            SELECT seq,op,prev,hash,payload FROM square_journal
            WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?
          ), sized AS (
            SELECT *,SUM(length(payload)) OVER (ORDER BY seq) AS total FROM candidates
          ) SELECT seq,op,prev,hash,payload FROM sized
            WHERE total<=? OR seq=(SELECT MIN(seq) FROM candidates) ORDER BY seq''',
          (after,through,limit,max_payload))

    def trim(self, through):
        head = self.acquire(force=True)
        if through <= 0 or through >= head['seq']:
            return
        self.query('''DELETE FROM square_journal WHERE seq<=? AND EXISTS
             (SELECT 1 FROM square_cluster WHERE id=1 AND cluster=? AND owner=? AND epoch=?
              AND lease_until>unixepoch()+5 AND seq>?)''',
             (through,self.cluster,self.owner,self.epoch,through))
        self.acquire(force=True)

    def release(self):
        if self.epoch is not None:
            try:
                self.query('UPDATE square_cluster SET owner=\'\',lease_until=0 WHERE id=1 AND owner=? AND epoch=?',
                           (self.owner, self.epoch))
            finally:
                self.deadline = 0

    def close(self):
        if self.owned_client:
            self.client.close()
