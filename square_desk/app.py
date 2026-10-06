from contextlib import asynccontextmanager
from pathlib import Path
from secrets import compare_digest
import json
import sqlite3
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, FileResponse
from starlette.requests import ClientDisconnect
from .config import Settings
from .service import Desk
from .news import add_source_record
from .telegram import rejection_message
from .media import MediaError, restore_png


def create_app(settings=None, desk=None, start_worker=True):
    settings = settings or Settings.from_env()
    settings.validate()
    desk = desk or Desk(settings)

    @asynccontextmanager
    async def lifespan(app):
        if start_worker:
            await desk.start()
        yield
        await desk.close()

    app = FastAPI(title='Square Desk', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.desk = desk
    basic = HTTPBasic(auto_error=False)

    def private(credentials: HTTPBasicCredentials | None = Depends(basic)):
        if not settings.admin_token or not credentials or not compare_digest(credentials.username, 'desk') or \
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

    @app.get('/health/details', dependencies=[Depends(private)])
    async def details():
        return desk.status()

    @app.get('/', dependencies=[Depends(private)], response_class=HTMLResponse)
    async def dashboard():
        return Path(__file__).with_name('dashboard.html').read_text(encoding='utf-8')

    @app.get('/dashboard.js', dependencies=[Depends(private)])
    async def script():
        return FileResponse(Path(__file__).with_name('dashboard.js'), media_type='text/javascript')

    @app.get('/api/overview', dependencies=[Depends(private)])
    async def overview():
        return {'status': desk.status(), 'drafts': desk.db.list('draft', limit=100),
                'market': desk.db.state('latest_market', []), 'signals': desk.db.list('signal', limit=50),
                'campaigns': desk.db.list('campaign'), 'report': desk.analytics.report(), 'logs': desk.db.logs(30)}

    @app.post('/api/command', dependencies=[Depends(private)])
    async def command(request: Request):
        # JSON request header and same-origin check prevent browser form CSRF.
        mutation_guard(request)
        try:
            payload = await bounded_json(request)
            text = payload['command']
            if not isinstance(text, str):
                raise ValueError('Command must be text')
            return {'message': await desk.telegram.command(text)}
        except (ValueError, TypeError, KeyError, sqlite3.IntegrityError) as error:
            raise HTTPException(400, rejection_message(error)) from None

    @app.get('/artifacts/{filename}', dependencies=[Depends(private)])
    async def artifact(filename: str):
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
            return {'id': add_source_record(desk.db, await bounded_json(request))}
        except (ValueError, TypeError, sqlite3.IntegrityError):
            raise HTTPException(400, 'Invalid news source record') from None

    @app.post('/telegram/webhook')
    async def webhook(request: Request):
        supplied = request.headers.get('x-telegram-bot-api-secret-token', '')
        if not settings.webhook_secret or not settings.telegram_token or settings.telegram_polling or \
                not compare_digest(supplied.encode(), settings.webhook_secret.encode()):
            raise HTTPException(403, 'Webhook rejected')
        try:
            await desk.telegram.receive(await bounded_json(request))
        except (ValueError, TypeError, KeyError):
            raise HTTPException(400, 'Invalid update') from None
        return {'ok': True}

    return app
