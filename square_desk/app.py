from contextlib import asynccontextmanager
from pathlib import Path
from secrets import compare_digest
import json
import sqlite3
from threading import Lock
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from starlette.requests import ClientDisconnect
from .config import Settings
from .service import Desk
from .news import add_source_record
from .telegram import rejection_message
from .media import MediaError, restore_png
from .worker_runtime import WorkerRuntime


def create_app(settings=None, desk=None, start_worker=True):
    settings = settings or Settings.from_env()
    settings.validate()
    desk = desk or Desk(settings)
    runtime=WorkerRuntime(desk)

    @asynccontextmanager
    async def lifespan(app):
        if start_worker:
            await runtime.start()
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(title='Square Desk', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    from starlette.middleware.gzip import GZipMiddleware
    from .bandwidth import ResponseCounter
    app.add_middleware(GZipMiddleware,minimum_size=1000)
    app.add_middleware(ResponseCounter)
    app.state.desk = desk
    app.state.worker_runtime = runtime
    basic = HTTPBasic(auto_error=False)

    def private(credentials: HTTPBasicCredentials | None = Depends(basic)):
        if not settings.admin_token or not credentials or not compare_digest(credentials.username.encode('utf-8'), b'desk') or \
                not compare_digest(credentials.password.encode(), settings.admin_token.encode()):
            raise HTTPException(401, 'Administrator authentication required', headers={'WWW-Authenticate': 'Basic'})

    async def bounded_json(request):
        body = bytearray()
        try:
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 50000:
                    raise HTTPException(413, 'Request too large')
        except ClientDisconnect:
            # A valid-looking prefix is not a complete request. Do not execute
            # or replay any command/update when its body upload was interrupted.
            raise HTTPException(408, 'Request body interrupted; no action accepted') from None
        return json.loads(body)

    def mutation_guard(request):
        if not request.headers.get('content-type', '').startswith('application/json'):
            raise HTTPException(415, 'JSON required')
        origin = request.headers.get('origin')
        if origin and origin.rstrip('/') != str(request.base_url).rstrip('/'):
            raise HTTPException(403, 'Same-origin requests required')

    @app.middleware('http')
    async def harden(request, call_next):
        if request.headers.get('content-length', '0').isdigit() and int(request.headers.get('content-length', 0)) > 50000:
            return HTMLResponse('Request too large', status_code=413)
        response = await call_next(request)
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        return response

    @app.api_route('/health', methods=['GET', 'HEAD'])
    async def health():
        return {'status': 'ok', 'service': 'square_desk'}

    @app.get('/ready')
    async def ready():
        observed=desk.runtime_status.snapshot()
        worker=observed['worker']
        fenced=observed['database_failover']
        current=bool(worker['loop_responsive'] and worker['lease_observed_held'] and
                     (not fenced['enabled'] or fenced['writer_lease_locally_valid']))
        return JSONResponse({'ready_observed':current,'worker_phase':worker['phase'],
            'loop_tick_age_seconds':worker['loop_tick_age_seconds'],
            'note':'Memory observation; /health is HTTP liveness and publication performs database checks'},
            status_code=200 if current else 503)

    @app.get('/health/details', dependencies=[Depends(private)])
    def details():
        return desk.status()

    @app.get('/api/runtime', dependencies=[Depends(private)])
    async def runtime_status():
        return desk.runtime_status.snapshot()

    @app.get('/', dependencies=[Depends(private)], response_class=HTMLResponse)
    async def dashboard():
        return Path(__file__).with_name('dashboard.html').read_text(encoding='utf-8')

    @app.get('/dashboard.js', dependencies=[Depends(private)])
    async def script():
        return FileResponse(Path(__file__).with_name('dashboard.js'), media_type='text/javascript')

    @app.get('/api/bandwidth', dependencies=[Depends(private)])
    async def bandwidth_status():
        from .bandwidth import status
        return {**status(),'storage':getattr(desk.db,'transfer_stats',{})}

    report_cache = {'at':0,'value':None}
    report_lock=Lock()
    @app.get('/api/overview', dependencies=[Depends(private)])
    def overview():
        from .models import utc
        with report_lock:
            if report_cache['value'] is None or utc()-report_cache['at']>=3600:
                report_cache.update(at=utc(),value=desk.analytics.report())
        market = [{'symbol':r['symbol'],'metrics':{k:r['metrics'].get(k) for k in ('quote','source')},
                   'changes':{k:r['changes'].get(k) for k in ('1h','24h')}}
                  for r in desk.db.state('latest_market',[])]
        drafts=[]
        for row in desk.db.list('draft',limit=20):
            p=row['payload']
            preview={k:p.get(k) for k in ('title','risk','generated_by','image','receipt')}
            preview.update(body=p.get('body','')[:300],word_count=len(p.get('body','').split()),summary_only=True)
            drafts.append({**row,'payload':preview})
        return {'status':desk.status(),'drafts':drafts,
                'market':market,'signals':desk.db.list('signal',limit=10),
                'campaigns':desk.db.list('campaign',limit=5),'report':report_cache['value'],
                'report_cached_at':report_cache['at'],'logs':desk.db.logs(10)}

    @app.get('/api/drafts/{ident}', dependencies=[Depends(private)])
    def draft_detail(ident: str):
        row=desk.db.get(ident)
        if not row or row['kind']!='draft':
            raise HTTPException(404,'Unknown draft')
        return row

    @app.post('/api/command', dependencies=[Depends(private)])
    async def command(request: Request):
        # JSON request header and same-origin check prevent browser form CSRF.
        mutation_guard(request)
        try:
            payload = await bounded_json(request)
            text = payload['command']
            if not isinstance(text, str):
                raise ValueError('Command must be text')
            return {'message': await runtime.call(desk.telegram.command,text)}
        except (ValueError, TypeError, KeyError, sqlite3.IntegrityError) as error:
            raise HTTPException(400, rejection_message(error)) from None

    @app.get('/artifacts/{filename}', dependencies=[Depends(private)])
    def artifact(filename: str):
        if not __import__('re').fullmatch(r'[a-zA-Z0-9_-]+\.(png|txt|json)', filename):
            raise HTTPException(400, 'Invalid filename')
        root = Path(settings.artifacts).resolve()
        path = (root / filename).resolve()
        if filename.endswith('.png'):
            try:
                restore_png(desk.db, settings.artifacts, filename)
            except (MediaError, OSError):
                raise HTTPException(404, 'Image missing or invalid') from None
        if path.parent != root or not path.is_file():
            raise HTTPException(404, 'Artifact missing')
        return FileResponse(path)

    @app.post('/api/news', dependencies=[Depends(private)])
    async def news(request: Request):
        mutation_guard(request)
        try:
            payload=await bounded_json(request)
            return {'id': await runtime.call(add_source_record,desk.db,payload)}
        except (ValueError, TypeError, sqlite3.IntegrityError):
            raise HTTPException(400, 'Invalid news source record') from None

    @app.post('/telegram/webhook')
    async def webhook(request: Request):
        supplied = request.headers.get('x-telegram-bot-api-secret-token', '')
        if not settings.webhook_secret or not settings.telegram_token or settings.telegram_polling or \
                not compare_digest(supplied.encode(), settings.webhook_secret.encode()):
            raise HTTPException(403, 'Webhook rejected')
        try:
            payload=await bounded_json(request)
            await runtime.call(desk.telegram.receive,payload)
        except (ValueError, TypeError, KeyError):
            raise HTTPException(400, 'Invalid update') from None
        return {'ok': True}

    return app
