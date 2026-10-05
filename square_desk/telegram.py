"""Allowlisted controls, durable update deduplication, bounded retries and private delivery."""
import json
import asyncio
import logging
import re
import sqlite3
import httpx
from .models import utc, uid
from .ai_router import AI_JOB, GenerationFailed


def rejection_message(error):
    # Only fixed application-owned messages may be exposed. Raw exceptions
    # can contain database credentials, provider URLs or administrator input.
    if isinstance(error, GenerationFailed):
        report = error.report
        lines = ['Draft nahi bana. Job ' + report['id'] + ': ' + report.get('reason', 'Generation failed')]
        for attempt in report.get('attempts', [])[-6:]:
            label = attempt['provider'] + '/' + attempt['model']
            detail = attempt.get('reason', attempt['status'])
            if attempt.get('http_status'):
                detail += ' (HTTP ' + str(attempt['http_status']) + ')'
            if attempt.get('parameter'):
                detail += '; parameter=' + attempt['parameter']
            lines.append(label + ': ' + detail)
        for skipped in report.get('skipped', []):
            lines.append(skipped['provider'] + ': skipped; ' + skipped['reason'] +
                         ('; wait ' + str(skipped['seconds']) + ' seconds' if skipped.get('seconds') else ''))
        lines.append('/ai_job se isi command ka report dekho. Draft save/publish nahi hua.')
        return '\n'.join(lines)
    safe = {
        'Signal market data timed out': 'Fresh signal candles fetch karte waqt time limit aayi; AI request nahi hui. Provider data /providers mein dekho.',
        'Market data unavailable': 'Fresh market candles available nahi hain; AI ko stale data nahi bheja gaya.',
        'Stale candles': 'Provider ki latest closed candles purani hain; fresh evidence ke bina draft nahi bana.',
        'Incomplete candle': 'Provider ne incomplete/future candle di; fresh closed candles chahiye.',
        'Missing or duplicated candles; do not synthesize gaps': 'Market candle history mein gap ya duplicate hai; unsupported history se analysis nahi bana.',
        'Evidence block exceeds configured word budget': 'Post ki word limit evidence block se chhoti hai; /settings mein post limits check karo.',
        'Evidence and word minimum cannot fit configured character budget': 'Word minimum aur evidence block character budget mein fit nahi ho sakte; post settings adjust karo ya article use karo.',
        'AI article sections repeat': 'AI ne article sections repeat kiye; quality check ne repetitive article save/publish nahi kiya.',
        'Daily AI request budget exhausted': 'Bot ka daily AI request budget khatam hai; DESK_AI_DAILY_REQUESTS check karo.',
        'Daily AI token reservation exhausted': 'Bot ka daily AI token reservation budget khatam hai; DESK_AI_DAILY_TOKENS check karo.',
        'AI request parameters rejected': 'AI provider ne request parameters reject kiye (HTTP 400); model aur JSON mode compatibility check karo.',
        'AI structured output generation failed': 'AI provider valid structured JSON generate nahi kar paya (HTTP 400); output validation fail hui.',
        'AI authentication failed': 'AI API key accept nahi hui (HTTP 401); DESK_AI_KEY check karo.',
        'AI access denied': 'AI provider ne access deny kiya (HTTP 403); account/model permissions check karo.',
        'AI endpoint or model unavailable': 'AI endpoint ya model nahi mila (HTTP 404); DESK_AI_URL aur DESK_AI_MODEL check karo.',
        'AI provider rate limit reached': 'AI provider ki rate limit lagi (HTTP 429); daily ya per-minute quota check karo, turant repeat mat karo.',
        'AI provider credits unavailable': 'AI provider credits unavailable hain; paid model automatically use nahi kiya gaya.',
        'All configured AI providers unavailable': 'Configured AI providers unavailable/cooldown mein hain ya output validation fail hui. /ai_status se provider health aur global bot budget dekho.',
        'Unknown AI provider': 'Provider name /ai_status ke configured_order se copy karo.',
        'AI output token limit reached': 'AI output token limit par ruk gaya; article/JSON incomplete hai. AI output budget ka code fix chahiye.',
        'AI service rejected request': 'AI provider ne request reject ki; provider status check karo.',
        'AI network request failed': 'AI request timeout ya network failure hua.',
        'AI request timed out': 'AI response timeout hua; provider cooldown aur backup /ai_status mein dekho.',
        'AI connection failed': 'AI provider se connection nahi bana; provider cooldown aur backup /ai_status mein dekho.',
        'AI network protocol failed': 'AI provider connection incomplete hua; provider cooldown aur backup /ai_status mein dekho.',
        'AI response read failed': 'AI provider ka response read nahi ho paya; /ai_status mein cooldown aur backup dekho.',
        'AI response unavailable or malformed': 'AI response valid JSON format mein nahi aaya.',
        'Invalid AI structure': 'AI JSON mein title/body text missing hai.',
        'AI output too large': 'AI output allowed size se bada hai.',
        'AI wrote unbound numerical claims': 'AI ne evidence tokens ke bina numbers likhe; quality validation ne draft roka.',
        'AI numerical correction failed': 'AI ne ek correction attempt ke baad bhi unsupported numbers likhe; draft save/publish nahi hua.',
        'AI evidence too large': 'AI evidence input allowed size se bada hai.',
        'Unknown fact token': 'AI ne unknown evidence token use kiya; quality validation ne draft roka.',
        'Same underlying event already covered': 'BTC ya selected symbol ka event pehle se covered hai. /queue aur /posts check karo; existing draft ID use karo.',
        'Draft too similar to recent content': 'Draft recent content jaisa hai. /queue aur /posts check karo; duplicate post blocked hai.',
        'Unknown draft': 'Draft ID nahi mili. /queue se current database ki actual ID copy karo.',
        'Paper signal ID is not a draft ID': 'Ye paper signal ID hai. /preview ID se signal details aur /queue se actual draft ID dekho; signal ko approve/publish nahi kiya ja sakta.',
        'Draft is immutable after publication or expiry': 'Draft publish, reject ya expire ho chuka hai. /queue se active draft select karo.',
        'Symbol must be on configured watchlist': 'Symbol watchlist mein nahi hai. /settings mein symbols check karo.',
        'Market evidence expired or future dated': 'Market evidence expired ya future dated hai. Fresh draft chahiye.',
        'Word count outside configured limits': 'Draft word count configured limits se match nahi karta. /settings check karo.',
        'Number is not bound to recorded evidence': 'Draft mein ek number recorded evidence se match nahi karta; publishing blocked hai.',
        'Missing source or timestamp': 'Draft ka source ya timestamp missing hai.',
        'Missing relevant ticker': 'Draft mein relevant ticker missing hai.',
        'Unknown signal': 'Signal ID nahi mili. /signals se actual Signal ID copy karo.',
        'Signal analysis requires a configured AI endpoint': 'Signal analysis ke liye AI integration chahiye; /ai_status check karo.',
        'Signal is inactive or stale; use a fresh signal': 'Signal stale, triggered, closed ya invalidated hai. /signals se fresh watching signal use karo.',
        'Signal market evidence does not match': 'Signal ke symbol/source/quote se fresh market data match nahi hua; draft nahi bana.',
        'Signal setup no longer qualifies on fresh evidence': 'Fresh candles par setup qualify nahi hua ya direction badal gayi; purana signal publish nahi hoga.',
        'Signal draft missing setup levels or direction': 'AI signal analysis mein required levels/direction missing hain; draft validation fail hui.',
        'Signal draft missing heuristic score disclosure': 'AI ne heuristic score ka disclosure nahi diya; draft validation fail hui.',
        'Invalid formatting': 'Draft formatting validation fail hui.',
        'Prohibited promotion or unsupported profit language': 'Draft content validation fail hui; unsupported promotional claim blocked hai.',
        'Control characters are not permitted': 'Draft mein invalid control characters hain.',
        'Long articles require a configured AI endpoint; short analysis stays available': 'Article ke liye AI integration chahiye. Short post ke liye /post_now BTC use karo.',
    }
    if isinstance(error, ValueError):
        chars = re.fullmatch(r'Square (short post|article) length (\d+); maximum (\d+) characters', str(error))
        if chars:
            kind, actual, maximum = chars.groups()
            return f'Command rejected: {kind} mein {actual} character units aaye; limit {maximum}. Complete content rewrite chahiye; publish request nahi bheji gayi.'
        length = re.fullmatch(r'AI body word count (\d{1,6}); required (\d{1,6})-(\d{1,6})', str(error))
        if length:
            count, lower, upper = length.groups()
            return f'Command rejected: AI body mein {count} words aaye; required {lower}-{upper}. Draft save/publish nahi hua.'
        messages = [safe[reason] for reason in str(error).split('; ') if reason in safe]
        if messages:
            return 'Command rejected: ' + ' '.join(dict.fromkeys(messages))
    if isinstance(error, sqlite3.IntegrityError):
        return 'Command rejected: database constraint conflict. /queue aur /posts check karo; duplicate automatically retry nahi kiya gaya.'
    return 'Command rejected. Check input, draft state, evidence freshness, or campaign requirements.'


HELP = '''Square Desk controls
/status /ai_status /ai_job /publish_status /ai_test PROVIDER /ai_retry PROVIDER /auto /approval /universe /dashboard /queue /next /today /posts /articles
/movers [15m|1h|4h|12h|24h|3d|7d] /gainers /losers /signals /alerts
/scan /education /post_now SYMBOL /article_now SYMBOL /signal_draft SIGNAL_ID /image_now ID
/preview ID /approve ID /reject ID /edit ID Title|Body
/regenerate ID /reschedule ID ISO_DATE /delete_queue ID
/pause /resume /emergency_stop /unlock CONFIRM
/projects /project_add key:value lines or JSON
/project_activate ID /project_draft ID /project_pause ID
/project_resume ID /project_end ID /project_edit ID JSON
/reports /performance /metrics JSON /providers /errors /logs
/news /news_add JSON /news_verify ID /news_draft ID
/settings /set daily_target|hourly_cap|gap_seconds VALUE
/policy_clear CONFIRM /reconcile ID posted|not_posted CONFIRM
Paper mode is default. Live trading is never supported.'''


class Telegram:
    def __init__(self, settings, store, desk, client):
        self.s, self.db, self.desk, self.client = settings, store, desk, client
        self.job_task = None
        self.flush_lock = asyncio.Lock()

    def authorized(self, message):
        # Only private chats, with both sender and destination matching an admin.
        if not isinstance(message, dict) or not isinstance(message.get('from'), dict) or not isinstance(message.get('chat'), dict):
            return False
        return message.get('from', {}).get('id') in self.s.telegram_admins and \
            message.get('chat', {}).get('id') == message.get('from', {}).get('id') and \
            message.get('chat', {}).get('type') == 'private'

    def notify(self, text, key=None):
        if not self.s.telegram_token:
            return
        # Articles and previews must not be silently truncated to one message.
        chunks, chars, units = [], [], 0
        for char in text[:100000]:
            size = 2 if ord(char) > 0xffff else 1
            if units + size > 3500:
                chunks.append(''.join(chars))
                chars, units = [], 0
            chars.append(char)
            units += size
        if chars:
            chunks.append(''.join(chars))
        for admin in self.s.telegram_admins:
            for index, chunk in enumerate(chunks):
                try:
                    label = f'[{index+1}/{len(chunks)}]\n' if len(chunks) > 1 else ''
                    self.db.insert('outbox', {'chat_id': admin, 'text': label + chunk},
                                   fingerprint=f'{admin}:{key}:{index}' if key else None)
                except sqlite3.IntegrityError:
                    pass

    async def api(self, method, body):
        try:
            response = await self.client.post(f'https://api.telegram.org/bot{self.s.telegram_token}/{method}', json=body)
            if response.status_code == 429:
                data = response.json()
                delay = float(data.get('parameters', {}).get('retry_after', 60))
                self.db.set('telegram_retry_after', utc() + max(1, delay))
                return None
            if response.status_code != 200:
                return None
            data = response.json()
            return data.get('result') if data.get('ok') else None
        except (httpx.HTTPError, ValueError, TypeError):
            # Never log the exception URL: it contains the bot token.
            self.db.log('TELEGRAM', 'API unavailable or invalid response')
            return None

    async def flush(self):
        async with self.flush_lock:
            await self._flush()

    async def _flush(self):
        if not self.s.telegram_token or utc() < self.db.state('telegram_retry_after', 0):
            return
        for row in reversed(self.db.list('outbox', ['pending'], limit=10)):
            p = row['payload']
            if p.get('next_attempt', 0) > utc():
                continue
            if not self.db.claim(row['id'], 'pending', 'sending'):
                continue
            buttons = {'inline_keyboard': [[{'text': label, 'callback_data': command}
                       for label, command in pair] for pair in [
                       [('Status', '/status'), ('Queue', '/queue')],
                       [('Movers', '/movers'), ('Signals', '/signals')],
                       [('Projects', '/projects'), ('Reports', '/reports')],
                       [('Pause', '/pause'), ('Resume', '/resume')],
                       [('Emergency stop', '/emergency_stop')]]]}
            result = await self.api('sendMessage', {'chat_id': p['chat_id'], 'text': p['text'], 'reply_markup': buttons})
            if result:
                self.db.update(row['id'], status='delivered')
                self.db.set('last_telegram_delivery', utc())
            else:
                p['attempts'] = p.get('attempts', 0) + 1
                p['next_attempt'] = utc() + min(3600, 15 * 2 ** p['attempts'])
                self.db.update(row['id'], status='failed' if p['attempts'] >= 5 else 'pending', payload=p)
                if utc() < self.db.state('telegram_retry_after', 0):
                    break

    async def poll(self):
        if not self.s.telegram_token or not self.s.telegram_polling or utc() < self.db.state('telegram_retry_after', 0):
            return
        result = await self.api('getUpdates', {'offset': self.db.state('telegram_offset', 0),
                                              'timeout': 0, 'limit': 30, 'allowed_updates': ['message', 'callback_query']})
        if isinstance(result, list):
            for update in result:
                await self.receive(update)
                self.db.set('telegram_offset', int(update['update_id']) + 1)

    async def receive(self, update):
        if not isinstance(update, dict) or type(update.get('update_id')) is not int:
            raise ValueError('Invalid Telegram update')
        callback = update.get('callback_query')
        message = update.get('message', {})
        if callback:
            message = {**callback.get('message', {}), 'from': callback.get('from', {}), 'text': callback.get('data', '')}
        if not self.authorized(message):
            self.db.log('SECURITY', 'Unauthorized Telegram command rejected')
            return
        ident = str(message['from']['id'])
        # Persist a fixed minute bucket so restarts cannot reset command limits.
        minute = int(utc() // 60)
        rate_key = f'command_rate:{ident}'
        with self.db.transaction():
            rate = self.db.state(rate_key, {'minute': minute, 'count': 0})
            if rate['minute'] != minute:
                rate = {'minute': minute, 'count': 0}
            if rate['count'] >= 15:
                return
            try:
                command_id = self.db.insert('command', {'sender': int(ident), 'command': str(message.get('text', '')).split(' ', 1)[0][:60]},
                               'processing', fingerprint='telegram:' + str(update['update_id']))
            except sqlite3.IntegrityError:
                return
            rate['count'] += 1
            self.db.set(rate_key, rate)
        text = str(message.get('text', ''))
        name = text.strip().split(maxsplit=1)[0].split('@')[0].lower() if text.strip() else ''
        # Only generation jobs are detached, with one bounded slot. Fast safety
        # commands remain responsive while AI waits for a provider or pacing.
        if name in ('/post_now', '/article_now', '/signal_draft', '/regenerate', '/project_draft', '/news_draft', '/ai_test') and not getattr(self.desk, 'batch_seconds', 0):
            if self.job_task and not self.job_task.done():
                self.db.update(command_id, status='rejected')
                self.notify('Ek AI command abhi processing mein hai. Uska result aane do; /status aur /ai_status available hain.',
                            key='command:' + str(update['update_id']))
            else:
                self.notify('Command received. AI draft processing shuru ho rahi hai; result alag message mein aayega. Command repeat mat karo.',
                            key='command-start:' + str(update['update_id']))
                self.job_task = asyncio.create_task(self._background_command(text, command_id, update['update_id']))
            if callback:
                await self.api('answerCallbackQuery', {'callback_query_id': callback['id']})
            await self.flush()
            return
        # Commands are at-most-once on crash; the administrator can inspect state
        # and issue a new update. Raw pasted credentials are never logged/stored.
        try:
            result = await self.command(str(message.get('text', '')))
            self.db.update(command_id, status='applied')
        except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as error:
            self.db.update(command_id, status='rejected')
            result = rejection_message(error)
            self.db.log('TELEGRAM', result)
        self.notify(result, key='command:' + str(update['update_id']))
        if callback:
            await self.api('answerCallbackQuery', {'callback_query_id': callback['id']})
        await self.flush()

    async def _background_command(self, text, command_id, update_id):
        command = text.strip().split(maxsplit=1)[0].split('@')[0].lower()
        job = {'id': command_id, 'command': command, 'stage': 'command_validation', 'started_at': utc()}
        token = AI_JOB.set(job)
        progress = asyncio.create_task(self._job_progress(job))
        try:
            try:
                parts = text.strip().split(maxsplit=1)
                previous = self.db.get(parts[1]) if command == '/regenerate' and len(parts) > 1 else None
                article = command == '/article_now' or bool(previous and previous['payload'].get('article'))
                timeout = self.s.ai_article_timeout_seconds if article else self.s.ai_command_timeout_seconds
                result = await asyncio.wait_for(self.command(text), timeout=timeout)
                self.db.update(command_id, status='applied')
                job.update(status='completed', result=result[:3500])
            except asyncio.TimeoutError:
                result = 'Job ' + command_id + ': command time limit reached at ' + job['stage'] + '. /ai_job dekho; command repeat karne se pehle /queue check karo.'
                self.db.update(command_id, status='rejected')
                job.update(status='timeout', reason=result)
            except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as error:
                result = rejection_message(error)
                if not isinstance(error, GenerationFailed):
                    result = 'Job ' + command_id + ' at ' + job['stage'] + ': ' + result
                self.db.update(command_id, status='rejected')
                self.db.log('TELEGRAM', result)
                job.update(status='failed', reason=result)
            except Exception as error:
                self.db.update(command_id, status='rejected')
                self.db.log('ERROR', 'Telegram generation failed: ' + type(error).__name__)
                result = 'Job ' + command_id + ': internal ' + type(error).__name__ + ' at ' + job['stage'] + '. /ai_job aur /queue dekho; success confirm nahi hua.'
                job.update(status='failed', reason=result)
            job['finished_at'] = utc()
            self.db.set('command_last_job', job)
            self.notify(result, key='command:' + str(update_id))
            await self.flush()
        except asyncio.CancelledError:
            # No automatic replay after a lease loss or restart.
            job.update(status='cancelled', reason='Worker stopped or lease lost', finished_at=utc())
            self.db.set('command_last_job', job)
            raise
        except Exception as error:
            logging.getLogger('square_desk').error('Telegram job unavailable: %s', type(error).__name__)
        finally:
            progress.cancel()
            try:
                await progress
            except asyncio.CancelledError:
                pass
            AI_JOB.reset(token)
            self.db.set('command_active_job', {})

    async def _job_progress(self, job):
        while True:
            self.db.set('command_active_job', job)
            await asyncio.sleep(self.s.ai_progress_seconds)
            self.notify('Job ' + job['id'] + ' processing: ' + job['stage'] +
                        '. /ai_job available hai; command repeat mat karo.')
            await self.flush()

    async def stop_job(self):
        if self.job_task and not self.job_task.done():
            self.job_task.cancel()
            try:
                await self.job_task
            except asyncio.CancelledError:
                pass

    async def command(self, text):
        if len(text) > 20000:
            raise ValueError('Command too large')
        parts = text.strip().split(maxsplit=1)
        if not parts:
            return HELP
        command, arg = parts[0].split('@')[0].lower(), parts[1].strip() if len(parts) > 1 else ''
        if command in ('/start', '/help'):
            return HELP
        if command in ('/status', '/dashboard'):
            return json.dumps(self.desk.status(), indent=2)
        if command == '/ai_job':
            job = self.db.state('command_active_job', {}) or self.db.state('command_last_job', {})
            generation = self.db.state('ai_active_generation', {})
            if not generation or generation.get('id') != job.get('id'):
                generation = self.db.state('ai_last_manual_job', {})
            if generation.get('id') != job.get('id'):
                generation = {}
            return json.dumps({'command': job, 'generation': generation}, indent=2)
        if command == '/publish_status':
            return json.dumps(self.desk.publisher.status(), indent=2)
        if command == '/ai_status':
            ai = self.desk.content.ai
            return json.dumps(ai.status() if ai and hasattr(ai, 'status') else {'configured': ai is not None}, indent=2)
        if command == '/ai_test':
            ai = self.desk.content.ai
            if not ai or not hasattr(ai, 'test'):
                raise ValueError('Unknown AI provider')
            return json.dumps(await ai.test(arg.lower()), indent=2)
        if command == '/ai_retry':
            ai = self.desk.content.ai
            if not ai or not hasattr(ai, 'providers') or arg not in ai.providers:
                raise ValueError('Unknown AI provider')
            states = self.db.state('ai_provider_health', {})
            states.pop(arg, None)
            self.db.set('ai_provider_health', states)
            return f'{arg} local cooldown cleared. Provider quota and global daily budgets are unchanged.'
        if command in ('/auto', '/approval'):
            values = self.db.state('runtime_settings', {})
            values.update(mode='automatic' if command == '/auto' else 'approval',
                          ai_auto_publish=command == '/auto')
            self.db.set('runtime_settings', values)
            self.s.mode, self.s.ai_auto_publish = values['mode'], values['ai_auto_publish']
            self.db.log('SECURITY', 'Publication mode changed: ' + self.s.mode)
            return ('Automatic mode enabled for new validated medium-risk text drafts. '
                    'Daily/hourly caps, pause, freshness, duplicate and live gates remain active. '
                    'High-risk, edited, campaign and image drafts still require review.' if command == '/auto'
                    else 'Approval mode enabled; new drafts require approval.')
        if command == '/universe':
            await self.desk.refresh_universe()
            return json.dumps({'mode': self.s.market_universe, 'status': self.desk.universe_state,
                               'selected_count': len(self.desk.universe), 'symbols': self.desk.universe,
                               'scan_batch_size': self.s.scan_batch_size}, indent=2)
        if command in ('/queue', '/next', '/schedule'):
            rows = self.db.list('draft', ['review', 'approved', 'queued'])
            return '\n'.join(f"{r['id']} {r['status']} {r['payload']['title']}" for r in rows[:20]) or 'Queue empty'
        if command in ('/posts', '/articles', '/today', '/alerts'):
            rows = self.db.list('draft', since=utc() - 86400 if command == '/today' else 0)
            if command == '/articles':
                rows = [r for r in rows if r['payload'].get('article')]
            if command == '/alerts':
                rows = [r for r in rows if r['payload']['category'] == 'shock']
            return '\n'.join(f"{r['id']} {r['status']} {r['payload']['title']}" for r in rows[:20]) or 'No records'
        if command in ('/movers', '/gainers', '/losers'):
            from .analysis import rank, WINDOWS
            window = arg or '24h'
            if window not in WINDOWS:
                raise ValueError('Invalid ranking window')
            rows = rank(self.db.state('latest_market', []), window, command != '/losers')[:10]
            return '\n'.join(f"${r['symbol']} {r['changes'][window]:+.2f}% ({window}, {r['metrics']['source']}/{r['metrics']['quote']})" for r in rows) or 'No fresh ranked data'
        if command == '/signals':
            rows = self.db.list('signal', limit=10)
            return '\n'.join(f"{r['id']} ${r['payload']['symbol']} {r['payload']['direction']} {r['status']} score={r['payload']['confidence']}" for r in rows) or 'No eligible setups'
        if command in ('/pause', '/emergency_stop'):
            self.db.set('paused' if command == '/pause' else 'emergency_stop', True)
            self.db.log('SECURITY', 'Administrator paused publication')
            return 'Publication stopped. Collection and paper tracking continue.'
        if command == '/resume':
            self.db.set('paused', False)
            return 'Ordinary pause cleared. Emergency stop remains independent.'
        if command == '/unlock':
            if arg != 'CONFIRM':
                return 'Send /unlock CONFIRM to clear the emergency stop.'
            self.db.set('emergency_stop', False)
            return 'Emergency stop cleared.'
        if command == '/scan':
            self.db.set('scan_requested', True)
            return 'Scan requested for the next worker cycle.'
        if command == '/education':
            ident = self.desk.content.education()
            return 'Educational draft ' + ident if ident else 'Available lessons already covered recently.'
        if command in ('/post_now', '/article_now'):
            ident = await self.desk.create_for_symbol(arg.upper(), article=command == '/article_now')
            return f'Draft {ident} created. Review with /preview {ident}; publishing limits still apply.'
        if command == '/signal_draft':
            ident = await self.desk.create_for_signal(arg)
            row = self.desk.get_draft(ident)
            p = row['payload']
            return (f"Signal {arg} → AI draft {ident} [{row['status']}]\n"
                    f"{p['title']}\n\n{p['body']}\n\n"
                    f"/preview {ident}\n/approve {ident}\n/reject {ident}\n"
                    'Fresh revalidated analysis; approval and scheduled publication required.')
        if command == '/preview':
            signal = self.db.get(arg)
            if signal and signal['kind'] == 'signal':
                p = signal['payload']
                linked = [r for r in self.db.list('draft', limit=500)
                          if (r['payload'].get('event') or {}).get('signal_id') == arg]
                links = '\n'.join(f"Draft {r['id']} [{r['status']}] /preview {r['id']}" for r in linked)
                return (f"Paper signal {signal['id']} [{signal['status']}]\n"
                        f"${p.get('symbol', '')} {p.get('direction', '')}\n"
                        f"Entry {p.get('entry', 'unavailable')}, stop {p.get('stop', 'unavailable')}, "
                        f"target {p.get('target1', 'unavailable')} {p.get('quote', '')}\n"
                        'Ye tracking signal hai; is ID ko approve/publish nahi kiya ja sakta.\n'
                        + (links if links else f'Is signal ka linked post draft nahi mila. Create: /signal_draft {arg}'))
            row = self.desk.get_draft(arg)
            p = row['payload']
            return f"{row['id']} [{row['status']}] [{p.get('generated_by', 'built-in/manual')}] {len(p['body'].split())} words\n{p['title']}\n\n{p['body']}"
        if command in ('/approve', '/reject', '/delete_queue'):
            self.desk.review(arg, command == '/approve')
            return 'Draft approved for scheduling.' if command == '/approve' else 'Draft rejected and retained in audit history.'
        if command == '/edit':
            ident, content = arg.split(maxsplit=1)
            title, body = content.split('|', 1)
            self.desk.edit(ident, title.strip(), body.strip())
            return 'Edited draft requires fresh approval.'
        if command == '/regenerate':
            row = self.desk.get_draft(arg)
            if row['status'] not in ('review', 'approved', 'queued') or not row['payload'].get('event'):
                raise ValueError('Only pending market drafts can be regenerated')
            if not self.desk.content.ai:
                return 'This grounded draft uses deterministic evidence. Use /edit ID Title|Body for a reviewed revision, or configure an AI endpoint.'
            ident = await self.desk.content.draft(row['payload']['event'], row['payload'].get('article', False), replace_id=arg)
            self.desk.review(arg, False)
            return 'Replacement draft ' + ident
        if command == '/reschedule':
            ident, timestamp = arg.split(maxsplit=1)
            self.desk.scheduler.reschedule(ident, timestamp)
            return 'Requested time recorded; limits and data expiry still apply.'
        if command == '/image_now':
            return 'Image created: ' + self.desk.make_image(arg)
        if command in ('/projects', '/giveaways'):
            return '\n'.join(f"{r['id']} {r['status']} {r['payload']['name']} remaining={max(0, r['payload']['quota'] - self.desk.campaigns.count(r['id']))}"
                             for r in self.db.list('campaign')) or 'No campaigns'
        if command == '/project_add':
            ident = self.desk.campaigns.add(arg)
            return f'Project {ident} parsed. Inspect supplied terms and activate with /project_activate {ident}.'
        if command == '/project_activate':
            self.desk.campaigns.activate(arg)
            return 'Supplied campaign terms marked reviewed; project activated.'
        if command == '/project_draft':
            return 'Campaign draft ' + await self.desk.campaigns.draft(arg)
        if command in ('/project_pause', '/project_resume', '/project_end'):
            row = self.db.get(arg)
            if not row or row['kind'] != 'campaign' or not row['payload']['verified'] or row['payload']['end'] <= utc():
                raise ValueError('Invalid project state')
            self.db.update(arg, status={'/project_pause': 'paused', '/project_resume': 'active', '/project_end': 'ended'}[command])
            return 'Project state updated.'
        if command == '/project_edit':
            ident, payload = arg.split(maxsplit=1)
            self.desk.campaigns.edit(ident, payload)
            return 'Project changed. Verification and draft approvals reset.'
        if command in ('/reports', '/performance'):
            return json.dumps(self.desk.analytics.report(), indent=2)
        if command == '/metrics':
            return 'Metrics import ' + self.desk.analytics.import_metrics(json.loads(arg))
        if command == '/news':
            return '\n'.join(f"{r['id']} {r['status']} {r['payload']['title']}\n{r['payload']['source_url']}" for r in self.db.list('news', limit=10)) or 'No news source records'
        if command == '/news_add':
            from .news import add_source_record
            return 'News source record ' + add_source_record(self.db, json.loads(arg))
        if command == '/news_verify':
            row = self.db.get(arg)
            if not row or row['kind'] != 'news':
                raise ValueError('Unknown news source record')
            p = row['payload']
            p['verification'], p['verified_by'] = 'verified', 'administrator'
            self.db.update(arg, status='verified', payload=p)
            self.db.log('CONTENT', 'Administrator reviewed primary news source', arg)
            return 'Source marked reviewed. Feed records still require an original summary via /news_add.'
        if command == '/news_draft':
            from .news import news_draft
            return 'Reviewed news draft ' + news_draft(self.db, self.desk.content.checker, arg)
        if command in ('/providers', '/data_status'):
            return json.dumps({name: self.db.state('provider:' + name, {'status': 'not_checked'}) for name in self.s.providers}, indent=2)
        if command in ('/errors', '/logs'):
            return json.dumps(self.db.logs(15), indent=2)
        if command == '/settings':
            return json.dumps({'configuration': self.s.public(), 'effective_caps': self.desk.publisher.policy.caps()}, indent=2)
        if command == '/set':
            key, value = arg.split()
            if key not in ('daily_target', 'hourly_cap', 'gap_seconds'):
                raise ValueError('Only publication caps can be changed through Telegram')
            value = int(value)
            if not 1 <= value <= (100 if key == 'daily_target' else 10 if key == 'hourly_cap' else 86400):
                raise ValueError('Setting outside bounds')
            values = self.db.state('runtime_settings', {})
            values[key] = value
            self.db.set('runtime_settings', values)
            setattr(self.s, key, value)
            self.db.log('SECURITY', 'Runtime publication setting changed: ' + key)
            return 'Setting persisted.'
        if command == '/policy_clear':
            if arg != 'CONFIRM':
                return 'After reviewing the provider restriction, send /policy_clear CONFIRM.'
            self.db.set('publisher_blocked', False)
            self.db.set('publisher_block_reason', {})
            return 'Restriction latch cleared. Lowered caps and cooldown remain.'
        if command == '/reconcile':
            ident, outcome, confirm = arg.split()
            if confirm != 'CONFIRM' or outcome not in ('posted', 'not_posted'):
                raise ValueError('Inspect Creator Center before reconciling')
            row = self.desk.get_draft(ident)
            if row['status'] not in ('uncertain', 'manual_ready'):
                raise ValueError('No uncertain or manual submission')
            self.db.update(ident, status='manual_published' if outcome == 'posted' else 'review')
            self.db.log('PUBLISH', 'Administrator reconciled remote outcome: ' + outcome, ident)
            return 'Submission reconciled.'
        return HELP
