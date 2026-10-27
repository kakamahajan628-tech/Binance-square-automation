import asyncio
import base64

import pytest
from fastapi.testclient import TestClient

from square_desk.app import create_app
from square_desk.config import Settings


def exercise_request(scope_body, tmp_path, route='/api/command', *, webhook=False):
    async def run():
        settings = Settings(database=':memory:', artifacts=str(tmp_path), admin_token='test-password',
                            telegram_token='test-token', telegram_admins=(7,), webhook_secret='test-webhook')
        app = create_app(settings=settings, start_worker=False)
        called, output = [], []
        async def command(text):
            called.append(text)
            return 'accepted'
        async def receive_update(update):
            called.append(update)
        app.state.desk.telegram.command = command
        app.state.desk.telegram.receive = receive_update
        headers = [(b'content-type', b'application/json')]
        if webhook:
            headers.append((b'x-telegram-bot-api-secret-token', b'test-webhook'))
        else:
            headers.append((b'authorization', b'Basic ' + base64.b64encode(b'desk:test-password')))
        messages = list(scope_body)
        async def receive():
            return messages.pop(0) if messages else {'type': 'http.disconnect'}
        async def send(message):
            output.append(message)
        scope = {'type': 'http', 'asgi': {'version': '3.0', 'spec_version': '2.4'},
                 'method': 'POST', 'scheme': 'http', 'path': route, 'raw_path': route.encode(),
                 'query_string': b'', 'root_path': '', 'headers': headers,
                 'client': ('127.0.0.1', 1234), 'server': ('testserver', 80), 'http_version': '1.1'}
        try:
            await app(scope, receive, send)
            return called, output
        finally:
            await app.state.desk.close()
    return asyncio.run(run())


@pytest.mark.parametrize('route', ['/api/command', '/api/news', '/telegram/webhook'])
def test_disconnected_body_is_handled_without_mutation_or_uncaught_asgi_exception(tmp_path, route):
    # Even a syntactically valid prefix is not accepted until the stream ends.
    called, output = exercise_request([
        {'type': 'http.request', 'body': b'{"command":"/pause"}', 'more_body': True},
        {'type': 'http.disconnect'}], tmp_path, route, webhook=route == '/telegram/webhook')
    assert called == []
    response = next(m for m in output if m['type'] == 'http.response.start')
    assert response['status'] == 408


def test_complete_stream_still_executes_command_once(tmp_path):
    called, output = exercise_request([
        {'type': 'http.request', 'body': b'{"command":', 'more_body': True},
        {'type': 'http.request', 'body': b'"/pause"}', 'more_body': False}], tmp_path)
    assert called == ['/pause']
    assert next(m for m in output if m['type'] == 'http.response.start')['status'] == 200


def test_existing_auth_body_limit_and_health_checks_are_preserved(tmp_path):
    settings = Settings(database=':memory:', artifacts=str(tmp_path), admin_token='test-password')
    app = create_app(settings=settings, start_worker=False)
    with TestClient(app) as client:
        assert client.get('/health').status_code == 200
        assert client.head('/health').status_code == 200
        assert client.post('/api/command', json={'command': '/pause'}).status_code == 401
        auth = ('desk', 'test-password')
        assert client.post('/api/command', json={'command': '/pause'}, auth=auth,
                           headers={'Origin': 'https://other.example'}).status_code == 403
        assert client.post('/api/command', content=b'x' * 50001, auth=auth,
                           headers={'Content-Type': 'application/json'}).status_code == 413
        assert client.post('/api/command', content=b'{bad json', auth=auth,
                           headers={'Content-Type': 'application/json'}).status_code == 400
        assert not app.state.desk.db.state('paused', False)
        assert client.post('/api/command', json={'command': '/pause'}, auth=auth).status_code == 200
        assert app.state.desk.db.state('paused')
