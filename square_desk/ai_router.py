"""Bounded generation failover. Publication retries are never performed here."""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import re
import socket
import ssl
import time
from urllib.parse import quote, urlsplit

import httpx

from .content import CompatibleAI, AIRequestError, FactChecker, openrouter_reasoning, rate_limit_diagnostic
from .models import utc


def transport_diagnostic(error):
    # All HTTPX exception classes are application/library-owned. Allowing their
    # names avoids collapsing DecodingError/ProxyError into a generic HTTPError.
    names = {name for name in dir(httpx) if isinstance(getattr(httpx, name), type)
             and issubclass(getattr(httpx, name), httpx.HTTPError)}
    kind = type(error).__name__
    reason = ('Timeout' if isinstance(error, httpx.TimeoutException) else
              'Connection failed' if isinstance(error, httpx.ConnectError) else
              'Response read failed' if isinstance(error, httpx.ReadError) else
              'Response decoding failed' if isinstance(error, httpx.DecodingError) else
              'Proxy connection failed' if isinstance(error, httpx.ProxyError) else
              'Unsupported URL protocol' if isinstance(error, httpx.UnsupportedProtocol) else
              'Remote HTTP protocol failed' if isinstance(error, httpx.RemoteProtocolError) else
              'Local HTTP protocol failed' if isinstance(error, httpx.LocalProtocolError) else
              'Transport failed')
    result = {'reason': reason, 'transport_type': kind if kind in names else 'HTTPError'}
    cause = error
    for _ in range(6):
        if isinstance(cause, ssl.SSLCertVerificationError):
            result['network_cause'] = 'TLS certificate verification failed'
            break
        if isinstance(cause, ssl.SSLError):
            result['network_cause'] = 'TLS handshake failed'
            break
        if isinstance(cause, socket.gaierror):
            result['network_cause'] = 'DNS lookup failed'
            break
        cause = cause.__cause__ or cause.__context__
        if cause is None:
            break
    return result


def validation_reason(error):
    """Allow only application-owned diagnostics; never persist draft text."""
    reason = str(error)
    fixed = {
        'AI wrote unbound numerical claims', 'AI numerical correction failed',
        'Unknown fact token', 'Invalid AI structure', 'AI output too large',
        'AI response unavailable or malformed', 'AI output token limit reached',
        'AI evidence too large', 'Prohibited promotion or unsupported profit language',
        'Invalid formatting', 'Control characters are not permitted',
        'Word count outside configured limits', 'Missing source or timestamp',
        'Missing relevant ticker'}
    if reason in fixed or re.fullmatch(r'AI body word count \d+; required \d+-\d+', reason):
        return reason
    return 'Output failed generation validation'


class GoogleClient:
    """Gemma/Gemini REST adapter with keys in headers, not query strings."""
    def __init__(self, client):
        self.client = client

    async def post(self, url, headers, json, timeout):
        key = headers.get('Authorization', '').removeprefix('Bearer ')
        prompt = '\n\n'.join(m['role'].upper() + ':\n' + m['content'] for m in json['messages'])
        body = {'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
                'generationConfig': {'maxOutputTokens': json.get('max_tokens', json.get('max_completion_tokens', 6000))}}
        if json.get('model') in ('gemini-3-flash-preview', 'gemini-3.1-flash-lite'):
            # Bound thinking so that the output allowance remains available for
            # the draft. Gemma does not accept these Gemini-specific options.
            body['generationConfig'].update({
                'thinkingConfig': {'thinkingLevel': 'low'},
                'responseMimeType': 'application/json',
                'responseSchema': {
                    'type': 'OBJECT', 'properties': {
                        'title': {'type': 'STRING'}, 'body': {'type': 'STRING'}},
                    'required': ['title', 'body']}})
        response = await self.client.post(url, headers={'x-goog-api-key': key,
            'Accept': 'application/json', 'Accept-Encoding': 'identity'}, json=body, timeout=timeout)
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


class OpenRouterClient:
    # Verified against the public catalog on 2026-10-05. Models without JSON
    # mode still receive the JSON instruction and must pass the local parser.
    plain_json_models = {
        'inclusionai/ling-3.0-flash-sante:free', 'qwen/qwen3.8-27b:free',
        'nvidia/nemotron-3.5-lightning:free', 'nvidia/nemotron-3-ultra-550b-a55b:free',
        'nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free'}
    optional_thinking_models = plain_json_models | {
        'google/gemma-4-31b-it:free', 'google/gemma-4-26b-a4b-it:free',
        'apodex/apodex-1.1-mini:free', 'dots-studio/dots-3-note-preview:free',
        'nvidia/nemotron-3-super-120b-a12b:free'}

    def __init__(self, client):
        self.client = client

    async def post(self, url, headers, json, timeout):
        body = dict(json)
        model = body['model']
        if model in self.plain_json_models:
            body.pop('response_format', None)
        if model in self.optional_thinking_models:
            body['reasoning'] = {'enabled': False, 'exclude': True}
        # Prevent an implicit paid fallback within OpenRouter.
        body['provider'] = {'max_price': {'prompt': 0, 'completion': 0}}
        if not hasattr(self, 'request_lock'):
            self.request_lock, self.next_request_at = asyncio.Lock(), 0
        async with self.request_lock:
            await asyncio.sleep(max(0, self.next_request_at - time.monotonic()))
            self.next_request_at = time.monotonic() + 4
            return await self.client.post(url, headers=headers, json=body, timeout=timeout)


class PacedAI(CompatibleAI):
    def __init__(self, settings, store, client, spacing):
        super().__init__(settings, store, client)
        self.spacing = spacing

    async def _send(self, request, article, timeout=90):
        if urlsplit(self.s.ai_url).hostname == 'openrouter.ai':
            return await self.client.post(self.s.ai_url, headers={'Authorization': 'Bearer ' + self.s.ai_key},
                                          json=request, timeout=timeout)
        owner = getattr(self, 'pacing_owner', self)
        spacing = (61 if article else 20) if urlsplit(self.s.ai_url).hostname == 'api.groq.com' else self.spacing
        async with owner.request_lock:
            await asyncio.sleep(max(0, owner.next_request_at - time.monotonic()))
            owner.next_request_at = time.monotonic() + spacing
            return await self.client.post(self.s.ai_url, headers={'Authorization': 'Bearer ' + self.s.ai_key},
                                          json=request, timeout=timeout)


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
            ('openrouter', 'https://openrouter.ai/api/v1/chat/completions', settings.openrouter_api_key, settings.openrouter_model, OpenRouterClient(client), 4),
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
        self.openrouter_variants = []
        if 'openrouter' in self.providers and settings.openrouter_models:
            original = self.providers['openrouter']
            for model in settings.openrouter_models:
                self.openrouter_variants.append(PacedAI(replace(original.s, ai_model=model), store, original.client, 4))
            # All variants share one pacing clock and lock, including probes.
            self.providers['openrouter'] = self.openrouter_variants[0]
        self.model_variants = {'openrouter': self.openrouter_variants} if self.openrouter_variants else {}
        for name in ('groq', 'google'):
            models = getattr(settings, name + '_models')
            if name in self.providers and models:
                original = self.providers[name]
                variants = []
                for model in models:
                    url = original.s.ai_url
                    if name == 'google':
                        url = 'https://generativelanguage.googleapis.com/v1beta/models/' + quote(model, safe='') + ':generateContent'
                    variant = PacedAI(replace(original.s, ai_model=model, ai_url=url), store, original.client, original.spacing)
                    variant.pacing_owner = original
                    variants.append(variant)
                self.model_variants[name] = variants
                self.providers[name] = variants[0]

    def candidates(self, name):
        variants = self.model_variants.get(name, [self.providers[name]])
        states = self.db.state('ai_' + name + '_model_health', {})
        return [p for p in variants
                if name not in self.model_variants or states.get(p.s.ai_model, {}).get('until', 0) <= utc()][:getattr(self.s, name + '_max_model_attempts', 1)]

    def mark_candidate(self, name, provider, status, reason='', cooldown=0, account=False):
        if name not in self.model_variants:
            self.mark(name, status, reason, cooldown)
            return
        state_key = 'ai_' + name + '_model_health'
        states = self.db.state(state_key, {})
        old = states.get(provider.s.ai_model, {})
        states[provider.s.ai_model] = {'status': status, 'reason': reason, 'at': utc(),
            'until': utc() + cooldown if cooldown else 0,
            'failures': old.get('failures', 0) + 1 if status != 'ok' else 0}
        self.db.set(state_key, states)
        self.mark(name, status, reason, cooldown if account else 0)

    def generation_candidates(self):
        attempted = 0
        for name in self.providers:
            if self.db.state('ai_provider_health', {}).get(name, {}).get('until', 0) > utc():
                continue
            candidates = self.candidates(name)
            if not candidates:
                continue
            if attempted >= self.s.ai_max_provider_attempts:
                break
            attempted += 1
            for provider in candidates:
                # Account/auth/network failures stop this whole chain. Model
                # capacity or invalid content only cool down the failed model.
                if self.db.state('ai_provider_health', {}).get(name, {}).get('until', 0) > utc():
                    break
                yield name, provider

    @property
    def configured(self):
        return bool(self.providers)

    def status(self):
        day = datetime.now(timezone.utc).date().isoformat()
        states = self.db.state('ai_provider_health', {})
        return {'configured': bool(self.providers), 'configured_order': list(self.providers),
                'implementation_revision': '2026-10-05-all-provider-models-5',
                'model_orders': {name: [p.s.ai_model for p in variants] for name, variants in self.model_variants.items()},
                'model_health': {name: {p.s.ai_model: {
                    **self.db.state('ai_' + name + '_model_health', {}).get(p.s.ai_model, {}),
                    'cooldown_remaining_seconds': max(0, int(self.db.state('ai_' + name + '_model_health', {}).get(p.s.ai_model, {}).get('until', 0) - utc()))}
                    for p in variants} for name, variants in self.model_variants.items()},
                'openrouter_model_order': [p.s.ai_model for p in self.openrouter_variants],
                'openrouter_model_health': {
                    p.s.ai_model: {**self.db.state('ai_openrouter_model_health', {}).get(p.s.ai_model, {}),
                        'cooldown_remaining_seconds': max(0, int(self.db.state('ai_openrouter_model_health', {}).get(p.s.ai_model, {}).get('until', 0) - utc()))}
                    for p in self.openrouter_variants},
                'max_provider_attempts_per_draft': self.s.ai_max_provider_attempts,
                'budget_day_utc': day,
                'global_budget': {
                    'requests_used': self.db.budget_used(day, 'ai_requests'), 'requests_cap': self.s.ai_daily_requests,
                    'tokens_used_or_reserved': self.db.budget_used(day, 'ai_tokens'), 'tokens_cap': self.s.ai_daily_tokens},
                'providers': {name: {'model': provider.s.ai_model, **states.get(name, {}),
                                    'cooldown_remaining_seconds': max(0, int(states.get(name, {}).get('until', 0) - utc()))}
                              for name, provider in self.providers.items()},
                'last_success': self.db.state('ai_last_success', {}),
                'last_connection_tests': self.db.state('ai_connection_tests', {})}

    async def test(self, name):
        """One short, budgeted contract test; never creates or publishes content."""
        if name not in self.providers:
            raise ValueError('Unknown AI provider')
        async with self.lock:
            state = self.db.state('ai_provider_health', {}).get(name, {})
            remaining = max(0, int(state.get('until', 0) - utc()))
            if remaining:
                return {'provider': name, 'status': 'cooldown', 'seconds_remaining': remaining,
                        'note': 'No API request sent. Wait for cooldown; quota is not reset.'}
            candidates = self.candidates(name)
            if not candidates:
                return {'provider': name, 'status': 'cooldown', 'note': 'All configured models cooling down; no API request sent.'}
            provider = candidates[0]
            day = datetime.now(timezone.utc).date().isoformat()
            request = {'model': provider.s.ai_model,
                       'messages': [{'role': 'user', 'content':
                           'Return only a JSON object with title and body string fields. '
                           'Set title to Connection test and body to API response received. '
                           'Do not add commentary, numbers or market claims.'}],
                       'response_format': {'type': 'json_object'}}
            host = urlsplit(provider.s.ai_url).hostname
            reasoning = host in ('api.groq.com', 'api.cerebras.ai') and 'gpt-oss' in provider.s.ai_model
            request['max_completion_tokens' if reasoning else 'max_tokens'] = 1024
            if host == 'api.groq.com' and reasoning:
                request['reasoning_effort'] = 'low'
            elif host == 'api.groq.com' and provider.s.ai_model == 'qwen/qwen3.8-27b':
                request['reasoning_effort'] = 'none'
            if host == 'openrouter.ai':
                request['reasoning'] = openrouter_reasoning(provider.s.ai_model)
            reserve = 1024 + len(request['messages'][0]['content'].encode('utf-8')) + 512
            if not self.db.reserve_budget(day, 'ai_tokens', reserve, self.s.ai_daily_tokens):
                raise ValueError('Daily AI token reservation exhausted')
            if not self.db.reserve_budget(day, 'ai_requests', 1, self.s.ai_daily_requests):
                self.db.release_budget(day, 'ai_tokens', reserve)
                raise ValueError('Daily AI request budget exhausted')
            result = {'provider': name, 'model': provider.s.ai_model, 'at': utc(),
                      'note': 'Connection/JSON test only; not market validation or publication.'}
            try:
                response = await provider._send(request, False, timeout=30)
                result['http_status'] = response.status_code
                if response.status_code != 200:
                    self.db.release_budget(day, 'ai_tokens', reserve)
                    reason = {400: 'Request rejected', 401: 'Authentication failed',
                              402: 'Credits unavailable', 403: 'Access denied',
                              404: 'Model or endpoint unavailable', 429: 'Provider quota/rate limit'}.get(
                                  response.status_code, 'Provider HTTP error')
                    result.update(status='failed', reason=reason)
                    result['limit_scope'] = rate_limit_diagnostic(response)
                    cooldown = 3600 if response.status_code in (400, 401, 402, 403, 404) else 120
                    if response.status_code == 429:
                        try:
                            cooldown = min(86400, max(300, float(response.headers.get('retry-after', '300'))))
                        except (ValueError, TypeError):
                            cooldown = 300
                        if re.search(r'daily|per[- ]day', response.text[:4000], re.I):
                            cooldown = max(cooldown, 3600)
                    # A diagnostic must not let callers hammer a real rate limit.
                    account = response.status_code in (401, 402) or result['limit_scope'] == 'account' or (name == 'openrouter' and result['limit_scope'] == 'daily')
                    self.mark_candidate(name, provider, 'rate_limited' if response.status_code == 429 else 'unavailable',
                                        reason, cooldown, account=account)
                else:
                    data = response.json()
                    usage = data.get('usage', {})
                    used = usage.get('total_tokens') if isinstance(usage, dict) else None
                    if type(used) is int and 0 < used <= reserve:
                        self.db.release_budget(day, 'ai_tokens', reserve - used)
                    choice = data['choices'][0]
                    finish = choice.get('finish_reason')
                    result['finish_reason'] = finish if finish in ('stop', 'length', 'content_filter', 'tool_calls') else 'other'
                    answer = choice['message']['content']
                    if finish == 'length':
                        result.update(status='failed', reason='Output token limit reached')
                    elif not isinstance(answer, str) or not answer.strip():
                        result.update(status='failed', reason='Empty content from provider')
                    else:
                        fenced = re.fullmatch(r'\s*```(?:json)?\s*\n?(.*?)\n?```\s*', answer, re.S)
                        raw = json.loads(fenced.group(1) if fenced else answer)
                        valid = (isinstance(raw, dict) and isinstance(raw.get('title'), str)
                                 and isinstance(raw.get('body'), str) and bool(raw['title'].strip()) and bool(raw['body'].strip()))
                        result.update(status='ok' if valid else 'failed',
                                      reason='API and JSON response working' if valid else 'Invalid title/body JSON structure')
            except httpx.HTTPError as error:
                result.update(status='failed', **transport_diagnostic(error))
                self.mark_candidate(name, provider, 'unavailable', result['reason'], 120, account=True)
            except (ValueError, KeyError, IndexError, TypeError, AttributeError):
                result.update(status='failed', reason='Malformed response or JSON')
            tests = self.db.state('ai_connection_tests', {})
            tests[name] = result
            self.db.set('ai_connection_tests', tests)
            # A probe never marks a validated market draft successful and does
            # not clear generation health, cooldowns or publication controls.
            return result

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
            deadline = time.monotonic() + (300 if article else 180)
            for name, provider in self.generation_candidates():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    as_of = datetime.fromisoformat(evidence['timestamp'].replace('Z', '+00:00')).timestamp()
                    if not 0 <= utc() - as_of <= self.s.draft_max_age:
                        raise ValueError('Market evidence expired or future dated')
                    result = await asyncio.wait_for(provider.generate(evidence, article), timeout=min(90, remaining))
                    reasons = FactChecker(self.s, self.db).check({**result, 'article': article})
                    if evidence['source'] not in result['body'] + result['title'] or evidence['timestamp'] not in result['body'] + result['title']:
                        reasons.append('Missing source or timestamp')
                    if '$' + evidence['symbol'] not in result['body'] + result['title']:
                        reasons.append('Missing relevant ticker')
                    if reasons:
                        raise ValueError('; '.join(reasons))
                except asyncio.TimeoutError:
                    self.mark_candidate(name, provider, 'unavailable', 'AI request timed out', 120)
                    continue
                except AIRequestError as error:
                    account = error.status in (0, 401, 402) or error.limit_scope == 'account' or (name == 'openrouter' and error.limit_scope == 'daily')
                    self.mark_candidate(name, provider, 'rate_limited' if error.status == 429 else 'unavailable',
                                        str(error), error.cooldown, account=account)
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
                    self.mark_candidate(name, provider, 'validation_failed', validation_reason(error), 300)
                    self.db.log('AI', name + ': invalid output; trying next configured provider')
                    continue
                self.mark_candidate(name, provider, 'ok')
                self.db.set('ai_last_success', {'provider': name, 'model': provider.s.ai_model, 'at': utc()})
                return {**result, 'ai_provider': name, 'ai_model': provider.s.ai_model}
            raise ValueError('All configured AI providers unavailable')
