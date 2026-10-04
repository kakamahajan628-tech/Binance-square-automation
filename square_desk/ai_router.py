"""Bounded generation failover. Publication retries are never performed here."""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import time
from urllib.parse import quote, urlsplit

import httpx

from .content import CompatibleAI, AIRequestError, FactChecker
from .models import utc


class GoogleClient:
    """Gemma/Gemini REST adapter with keys in headers, not query strings."""
    def __init__(self, client):
        self.client = client

    async def post(self, url, headers, json, timeout):
        key = headers.get('Authorization', '').removeprefix('Bearer ')
        prompt = '\n\n'.join(m['role'].upper() + ':\n' + m['content'] for m in json['messages'])
        body = {'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
                'generationConfig': {'maxOutputTokens': json.get('max_tokens', json.get('max_completion_tokens', 6000))}}
        if json.get('model') == 'gemini-3-flash-preview':
            # Bound thinking so that the output allowance remains available for
            # the draft. Gemma does not accept these Gemini-specific options.
            body['generationConfig'].update({
                'thinkingConfig': {'thinkingLevel': 'low'},
                'responseMimeType': 'application/json',
                'responseSchema': {
                    'type': 'OBJECT', 'properties': {
                        'title': {'type': 'STRING'}, 'body': {'type': 'STRING'}},
                    'required': ['title', 'body']}})
        response = await self.client.post(url, headers={'x-goog-api-key': key}, json=body, timeout=timeout)
        if response.status_code != 200:
            return response
        try:
            data = response.json()
            candidate = data['candidates'][0]
            answer = ''.join(part.get('text', '') for part in candidate['content']['parts'] if not part.get('thought'))
            finish = 'length' if candidate.get('finishReason') == 'MAX_TOKENS' else 'stop'
            usage = data.get('usageMetadata', {})
            return httpx.Response(200, headers=response.headers, json={
                'choices': [{'finish_reason': finish, 'message': {'content': answer}}],
                'usage': {'total_tokens': usage.get('totalTokenCount')}})
        except (KeyError, IndexError, TypeError, ValueError, AttributeError):
            # CompatibleAI returns a fixed malformed-response diagnostic.
            return httpx.Response(200, headers=response.headers, json={'choices': []})


class OptionalBearerClient:
    def __init__(self, client):
        self.client = client

    async def post(self, url, headers, json, timeout):
        if headers.get('Authorization') == 'Bearer anonymous':
            headers = {}
        return await self.client.post(url, headers=headers, json=json, timeout=timeout)


class PacedAI(CompatibleAI):
    def __init__(self, settings, store, client, spacing):
        super().__init__(settings, store, client)
        self.spacing = spacing

    async def _send(self, request, article):
        if urlsplit(self.s.ai_url).hostname == 'api.groq.com':
            return await super()._send(request, article)
        async with self.request_lock:
            await asyncio.sleep(max(0, self.next_request_at - time.monotonic()))
            self.next_request_at = time.monotonic() + self.spacing
            return await self.client.post(self.s.ai_url, headers={'Authorization': 'Bearer ' + self.s.ai_key},
                                          json=request, timeout=90)


class AIRouter:
    def __init__(self, settings, store, client):
        self.s, self.db, self.client = settings, store, client
        self.providers = {}
        self.lock = asyncio.Lock()
        definitions = {}
        if settings.ai_url and settings.ai_key and settings.ai_model:
            host = urlsplit(settings.ai_url).hostname
            name = 'groq' if host == 'api.groq.com' else 'primary'
            definitions[name] = (settings.ai_url, settings.ai_key, settings.ai_model, client, 3)
        optional = [
            ('cerebras', 'https://api.cerebras.ai/v1/chat/completions', settings.cerebras_api_key, settings.cerebras_model, client, 3),
            ('google', 'https://generativelanguage.googleapis.com/v1beta/models/' + quote(settings.google_model, safe='') + ':generateContent', settings.google_api_key, settings.google_model, GoogleClient(client), 3),
            ('openrouter', 'https://openrouter.ai/api/v1/chat/completions', settings.openrouter_api_key, settings.openrouter_model, client, 4),
            ('mistral', 'https://api.mistral.ai/v1/chat/completions', settings.mistral_api_key, settings.mistral_model, client, 3),
            ('cloudflare', f'https://api.cloudflare.com/client/v4/accounts/{settings.cloudflare_account_id}/ai/v1/chat/completions', settings.cloudflare_api_token if settings.cloudflare_account_id else '', settings.cloudflare_model, client, 3),
            ('kilo', 'https://api.kilo.ai/api/gateway/chat/completions', (settings.kilo_api_key or 'anonymous') if settings.kilo_enabled else '', settings.kilo_model, OptionalBearerClient(client), 20),
            ('nvidia', 'https://integrate.api.nvidia.com/v1/chat/completions', settings.nvidia_api_key, settings.nvidia_model, client, 3),
            ('cohere', 'https://api.cohere.ai/compatibility/v1/chat/completions', settings.cohere_api_key, settings.cohere_model, client, 4),
        ]
        for name, url, key, model, adapter, spacing in optional:
            if key and model:
                definitions[name] = (url, key, model, adapter, spacing)
        order = list(settings.ai_provider_order)
        if 'primary' in definitions and 'primary' not in order:
            order.insert(0, 'primary')
        for name in order:
            if name in definitions:
                url, key, model, adapter, spacing = definitions[name]
                derived = replace(settings, ai_url=url, ai_key=key, ai_model=model)
                self.providers[name] = PacedAI(derived, store, adapter, spacing)

    @property
    def configured(self):
        return bool(self.providers)

    def status(self):
        day = datetime.now(timezone.utc).date().isoformat()
        states = self.db.state('ai_provider_health', {})
        return {'configured': bool(self.providers), 'configured_order': list(self.providers),
                'max_provider_attempts_per_draft': self.s.ai_max_provider_attempts,
                'budget_day_utc': day,
                'global_budget': {
                    'requests_used': self.db.budget_used(day, 'ai_requests'), 'requests_cap': self.s.ai_daily_requests,
                    'tokens_used_or_reserved': self.db.budget_used(day, 'ai_tokens'), 'tokens_cap': self.s.ai_daily_tokens},
                'providers': {name: {'model': provider.s.ai_model, **states.get(name, {}),
                                    'cooldown_remaining_seconds': max(0, int(states.get(name, {}).get('until', 0) - utc()))}
                              for name, provider in self.providers.items()},
                'last_success': self.db.state('ai_last_success', {})}

    def mark(self, name, status, reason='', cooldown=0):
        states = self.db.state('ai_provider_health', {})
        old = states.get(name, {})
        states[name] = {'status': status, 'reason': reason, 'at': utc(), 'until': utc() + cooldown if cooldown else 0,
                        'failures': old.get('failures', 0) + 1 if status != 'ok' else 0}
        self.db.set('ai_provider_health', states)

    async def generate(self, evidence, article=False):
        # One generation at a time prevents dashboard/Telegram/worker requests
        # from all hammering a provider immediately after a quota failure.
        async with self.lock:
            attempted = 0
            for name, provider in self.providers.items():
                state = self.db.state('ai_provider_health', {}).get(name, {})
                if state.get('until', 0) > utc():
                    continue
                if attempted >= self.s.ai_max_provider_attempts:
                    break
                attempted += 1
                try:
                    as_of = datetime.fromisoformat(evidence['timestamp'].replace('Z', '+00:00')).timestamp()
                    if not 0 <= utc() - as_of <= self.s.draft_max_age:
                        raise ValueError('Market evidence expired or future dated')
                    result = await provider.generate(evidence, article)
                    reasons = FactChecker(self.s, self.db).check({**result, 'article': article})
                    if evidence['source'] not in result['body'] + result['title'] or evidence['timestamp'] not in result['body'] + result['title']:
                        reasons.append('Missing source or timestamp')
                    if '$' + evidence['symbol'] not in result['body'] + result['title']:
                        reasons.append('Missing relevant ticker')
                    if reasons:
                        raise ValueError('; '.join(reasons))
                except AIRequestError as error:
                    self.mark(name, 'rate_limited' if error.status == 429 else 'unavailable', str(error), error.cooldown)
                    self.db.log('AI', f'{name}: HTTP {error.status}; trying next configured provider')
                    continue
                except ValueError as error:
                    if str(error) in ('Daily AI request budget exhausted', 'Daily AI token reservation exhausted',
                                      'Market evidence expired or future dated'):
                        # Global local budgets cover every provider. A backup
                        # must not silently bypass the user's configured cap.
                        raise
                    # Only fixed reason/type, never the rejected output, is
                    # retained. Every backup passes the identical validator.
                    self.mark(name, 'validation_failed', 'Output failed generation validation', 300)
                    self.db.log('AI', name + ': invalid output; trying next configured provider')
                    continue
                self.mark(name, 'ok')
                self.db.set('ai_last_success', {'provider': name, 'model': provider.s.ai_model, 'at': utc()})
                return {**result, 'ai_provider': name, 'ai_model': provider.s.ai_model}
            raise ValueError('All configured AI providers unavailable')
