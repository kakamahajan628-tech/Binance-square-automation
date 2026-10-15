"""Payload-size observations only; never claims to reproduce cloud billing."""
from collections import Counter
from threading import Lock
import time

_counts, _lock, _started = Counter(), Lock(), time.time()


def record(category, size):
    with _lock:
        _counts[category] += max(0,int(size))


async def request_hook(request):
    try:
        size = len(request.content)
    except Exception:
        size = 0
    # Count lengths, never retain/log headers, bodies, tokens, URLs or DB values.
    record('http_request_payload_bytes',size + sum(len(k)+len(v)+4 for k,v in request.headers.raw)+100)


def status():
    with _lock:
        return {'since_process_started':_started,'payload_bytes':dict(_counts),
                'note':'Process-local estimates; reset on restart. TLS, TCP, retries, other services and provider billing are not fully measured. Check Render Metrics/Billing and each database dashboard.'}


class ResponseCounter:
    def __init__(self,app):
        self.app = app
    async def __call__(self,scope,receive,send):
        if scope['type'] != 'http':
            return await self.app(scope,receive,send)
        async def counted(message):
            if message['type'] == 'http.response.start':
                record('web_response_payload_bytes',sum(len(k)+len(v)+4 for k,v in message.get('headers',[]))+50)
            elif message['type'] == 'http.response.body':
                record('web_response_payload_bytes',len(message.get('body',b'')))
            await send(message)
        await self.app(scope,receive,counted)
