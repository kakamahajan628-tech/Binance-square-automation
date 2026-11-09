"""Bounded, passive outbound estimates. No probes or cloud-billing claims."""
from collections import Counter, defaultdict
from threading import Lock
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
import json
import re
import time

_counts, _lock, _started = Counter(), Lock(), time.time()
IST = timezone(timedelta(hours=5, minutes=30))
SOURCES = {
    'postgres.neon':'DB Neon', 'postgres.aiven':'DB Aiven',
    'postgres.supabase':'DB Supabase', 'postgres.other':'DB other',
    'turso':'Turso journal',
    'market.binance.candles':'Binance candles', 'market.binance.discovery':'Binance discovery',
    'market.binance.other':'Binance market other', 'market.coinbase':'Coinbase market',
    'market.derivatives':'Derivatives',
    'ai.groq':'AI Groq', 'ai.google':'AI Google', 'ai.openrouter':'AI OpenRouter',
    'ai.cerebras':'AI Cerebras', 'ai.mistral':'AI Mistral', 'ai.cloudflare':'AI Cloudflare',
    'ai.kilo':'AI Kilo', 'ai.nvidia':'AI Nvidia', 'ai.cohere':'AI Cohere',
    'ai.other':'AI other', 'square.image_upload':'Square image bytes',
    'square.media_api':'Square image prepare/status', 'square.publish':'Square publish text',
    'square.other':'Square other', 'telegram.poll':'Telegram polling',
    'telegram.send':'Telegram messages', 'telegram.files':'Telegram image download requests',
    'telegram.other':'Telegram control', 'http.other':'Other HTTP/news',
    'web.responses':'HTTP responses/health',
}
OPERATIONS = {'http','stream','query','entities','state','audit','budgets','replica_meta',
              'transaction','replica_replay','snapshot_read','authority','mutations',
              'replication','snapshots','cleanup','setup','response','connect'}
AI_HOSTS = {
    'api.groq.com':'groq', 'generativelanguage.googleapis.com':'google',
    'openrouter.ai':'openrouter', 'api.cerebras.ai':'cerebras', 'api.mistral.ai':'mistral',
    'api.cloudflare.com':'cloudflare', 'api.kilo.ai':'kilo',
    'integrate.api.nvidia.com':'nvidia', 'api.cohere.ai':'cohere',
}


class OutboundMeter:
    """Fixed source/operation labels, lifetime counters and <=48 hourly buckets."""
    def __init__(self, clock=None):
        self.clock = clock or time.time
        self.started = self.clock()
        self.lock = Lock()
        self.totals, self.hours = defaultdict(Counter), {}

    @staticmethod
    def hour(now):
        return int((now + 19800) // 3600) * 3600 - 19800

    def add(self, source, *, operation='http', requests=0, estimated_bytes=0,
            body_bytes=0, header_estimate_bytes=0, streaming_body_requests=0,
            connection_attempts=0, connection_failures=0):
        source = source if source in SOURCES else 'http.other'
        operation = operation if operation in OPERATIONS else 'http'
        values = {k:max(0, int(v)) for k,v in {
            'requests':requests, 'estimated_bytes':estimated_bytes, 'body_bytes':body_bytes,
            'header_estimate_bytes':header_estimate_bytes,
            'streaming_body_requests':streaming_body_requests,
            'connection_attempts':connection_attempts, 'connection_failures':connection_failures,
        }.items()}
        values['op:' + operation] = max(0, int(requests))
        hour = self.hour(self.clock())
        with self.lock:
            bucket = self.hours.setdefault(hour, defaultdict(Counter))
            self.totals[source].update(values)
            bucket[source].update(values)
            # A wall-clock jump cannot create unbounded counters.
            for old in sorted(self.hours):
                if old < hour - 47*3600 or len(self.hours) > 48:
                    del self.hours[old]

    @staticmethod
    def rows(values):
        result = []
        for source,counters in values.items():
            row = {'source':source, 'label':SOURCES[source],
                   **{k:int(v) for k,v in counters.items() if not k.startswith('op:')},
                   'operations':{k[3:]:int(v) for k,v in counters.items() if k.startswith('op:') and v}}
            result.append(row)
        return sorted(result, key=lambda r:(-r.get('estimated_bytes',0),r['source']))

    def snapshot(self):
        now = self.clock()
        hour = self.hour(now)
        midnight = int(datetime.fromtimestamp(now, IST).replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        with self.lock:
            totals = {s:dict(c) for s,c in self.totals.items()}
            hours = {h:{s:dict(c) for s,c in bucket.items()} for h,bucket in self.hours.items()}
        def window(start, end):
            values = defaultdict(Counter)
            for h,bucket in hours.items():
                if start <= h < end:
                    for source,counters in bucket.items(): values[source].update(counters)
            observed = max(0, min(now,end)-max(self.started,start))
            return {'started_at':start, 'ended_at':min(now,end),
                    'observed_seconds':round(observed,1), 'sources':self.rows(values)}
        return {'revision':'2026-11-07-outbound-audit-24', 'observed_at':now,
                'since_process_started':self.started, 'timezone':'Asia/Kolkata',
                'measurement':'Application estimates, including attempted requests; not exact network-wire or Render billed bytes.',
                'excluded':'Inbound API/DB responses; TLS/TCP/DNS/connection overhead and transport-level retransmissions are not measured.',
                'history':'RAM only; restart resets counters. Past months cannot be reconstructed.',
                'sources':self.rows(totals), 'today':window(midnight,now+1),
                'current_hour':window(hour,now+1), 'previous_hour':window(hour-3600,hour),
                'hourly_buckets_retained':len(hours)}


_meter = OutboundMeter()


def http_source(request):
    """Inspect routing only; never retain a hostname, URL, header or body."""
    host, path = request.url.host.lower(), request.url.path.lower()
    if host == 'data-api.binance.vision' or host == 'api.binance.com':
        return 'market.binance.candles' if path.endswith('/klines') else (
            'market.binance.discovery' if path.endswith(('/exchangeinfo','/ticker/24hr')) else 'market.binance.other')
    if host in ('api.exchange.coinbase.com','api.coinbase.com'): return 'market.coinbase'
    if host == 'fapi.binance.com': return 'market.derivatives'
    if host in AI_HOSTS: return 'ai.' + AI_HOSTS[host]
    if host.endswith('.turso.io'): return 'turso'
    if host == 'api.telegram.org':
        if path.startswith('/file/'): return 'telegram.files'
        method = path.rsplit('/',1)[-1]
        if method == 'getupdates': return 'telegram.poll'
        if method in ('sendmessage','sendphoto','senddocument'): return 'telegram.send'
        if method == 'getfile': return 'telegram.files'
        return 'telegram.other'
    if request.method == 'PUT' and request.headers.get('content-type','').split(';')[0] == 'image/png':
        return 'square.image_upload'
    if host in ('www.binance.com','binance.com') and '/openapi/' in path:
        if '/image/' in path: return 'square.media_api'
        if path.endswith('/content/add'): return 'square.publish'
        return 'square.other'
    if path.endswith(('/chat/completions',':generatecontent')): return 'ai.other'
    return 'http.other'


def postgres_source(url):
    try: host = (urlsplit(url).hostname or '').lower()
    except (ValueError, TypeError, AttributeError): return 'postgres.other'
    for suffix,name in (('.neon.tech','neon'),('.aivencloud.com','aiven'),
                         ('.supabase.co','supabase'),('.supabase.com','supabase'),('.supabase.net','supabase')):
        if host.endswith(suffix): return 'postgres.' + name
    return 'postgres.other'


def observe_postgres(url, sql, args=None, *, replay=False, snapshot=False):
    size = len(sql.encode()) + len(json.dumps(args,default=str).encode()) + 32
    operation = 'replica_replay' if replay else 'snapshot_read' if snapshot else 'query'
    if operation == 'query':
        lowered = sql.lower()
        if 'square_replica_meta' in lowered: operation = 'replica_meta'
        elif lowered.startswith('set ') or 'pg_advisory_xact_lock' in lowered: operation = 'transaction'
        else:
            for table in ('entities','state','audit','budgets'):
                if re.search(r'\b(?:from|into|update)\s+'+table+r'\b',lowered):
                    operation = table
                    break
    _meter.add(postgres_source(url),operation=operation,requests=1,estimated_bytes=size,body_bytes=size)
    return size


def observe_postgres_connection(url, *, failed=False):
    _meter.add(postgres_source(url),operation='connect',
               connection_attempts=0 if failed else 1,connection_failures=1 if failed else 0)


def observe_turso(request, token, statements):
    sql = ' '.join(s for s,a in statements).lower()
    operation = ('snapshots' if 'square_restore_' in sql else
                 'mutations' if 'insert into square_journal' in sql else
                 'cleanup' if 'delete from square_journal' in sql else
                 'replication' if 'from square_journal' in sql else
                 'authority' if 'update square_cluster' in sql else 'setup')
    body = len(json.dumps(request,ensure_ascii=False,separators=(',',':')).encode())
    headers = len(token.encode()) + 300
    _meter.add('turso',operation=operation,requests=1,estimated_bytes=body+headers,
               body_bytes=body,header_estimate_bytes=headers)


def compact_status():
    report = _meter.snapshot()
    day = {r['source']:r for r in report['today']['sources']}
    previous = {r['source']:r for r in report['previous_hour']['sources']}
    def mb(value): return f'{value/1_000_000:.3f}'
    started = datetime.fromtimestamp(report['since_process_started'],IST).strftime('%d %b %H:%M IST')
    rows = report['sources']
    total = sum(r['estimated_bytes'] for r in rows if r['source']!='web.responses')
    lines = ['Outbound audit: application estimates', 'Since '+started,
             'Service-initiated total: '+mb(total)+' MB',
             'Source: total MB | today MB | previous hour MB | requests']
    shown = 0
    for row in rows:
        source = row['source']
        group = [f"{row['label']}: {mb(row['estimated_bytes'])} | {mb(day.get(source,{}).get('estimated_bytes',0))} | {mb(previous.get(source,{}).get('estimated_bytes',0))} | {row['requests']}"]
        if row['connection_attempts'] or row['connection_failures']:
            group.append(f"  Connections attempted/failed: {row['connection_attempts']}/{row['connection_failures']}")
        if row['streaming_body_requests']:
            group.append(f"  Streaming body sizes unmeasured: {row['streaming_body_requests']}")
        if len('\n'.join(lines+group).encode('utf-16-le'))>5600:
            break
        lines.extend(group)
        shown += 1
    if len(rows)>shown: lines.append('Remaining sources in authenticated /api/bandwidth.')
    lines.extend(['Previous hour observed: '+str(report['previous_hour']['observed_seconds'])+' sec (max 3600).',
                  'Inbound excluded. TLS/TCP/DNS/retransmits not measured. Attempts may fail before bytes leave.',
                  'RAM counters reset on restart. Full operations/windows: authenticated /api/bandwidth.'])
    return '\n'.join(lines)


def record(category, size):
    with _lock:
        _counts[category] += max(0,int(size))


async def request_hook(request):
    source = http_source(request)
    headers = sum(len(k)+len(v)+4 for k,v in request.headers.raw)+100
    try:
        size = len(request.content)
    except Exception:
        size = 0
        _meter.add(source,streaming_body_requests=1)
    # Count lengths, never retain/log headers, bodies, tokens, URLs or DB values.
    record('http_request_payload_bytes',size+headers)
    _meter.add(source,operation='authority' if source=='turso' else 'http',requests=1,
               estimated_bytes=size+headers,body_bytes=size,header_estimate_bytes=headers)


def status():
    with _lock:
        legacy = dict(_counts)
    return {'since_process_started':_started,'payload_bytes':legacy,'outbound_audit':_meter.snapshot(),
            'note':'Do not add legacy counters to outbound_audit: they overlap. postgres_response_payload_bytes is inbound. Process-local estimates reset on restart; Render Metrics/Billing remains authoritative.'}


class ResponseCounter:
    def __init__(self,app):
        self.app = app
    async def __call__(self,scope,receive,send):
        if scope['type'] != 'http':
            return await self.app(scope,receive,send)
        async def counted(message):
            if message['type'] == 'http.response.start':
                size = sum(len(k)+len(v)+4 for k,v in message.get('headers',[]))+50
                record('web_response_payload_bytes',size)
                _meter.add('web.responses',operation='response',requests=1,estimated_bytes=size,header_estimate_bytes=size)
            elif message['type'] == 'http.response.body':
                size = len(message.get('body',b''))
                record('web_response_payload_bytes',size)
                _meter.add('web.responses',operation='response',estimated_bytes=size,body_bytes=size)
            await send(message)
        await self.app(scope,receive,counted)
