from .db_actor import run_db
'Allowlisted controls, durable update deduplication, bounded retries and private delivery.'
import json
import asyncio
import logging
import re
import sqlite3
import httpx
from .models import utc, uid
from .ai_router import AI_JOB, GenerationFailed

def rejection_message(error):
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
            lines.append(skipped['provider'] + ': skipped; ' + skipped['reason'] + ('; wait ' + str(skipped['seconds']) + ' seconds' if skipped.get('seconds') else ''))
        lines.append('/ai_job se isi command ka report dekho. Draft save/publish nahi hua.')
        return '\n'.join(lines)
    safe = {'Signal market data timed out': 'Fresh signal candles fetch karte waqt time limit aayi; AI request nahi hui. Provider data /providers mein dekho.', 'Market data unavailable': 'Fresh market candles available nahi hain; AI ko stale data nahi bheja gaya.', 'Stale candles': 'Provider ki latest closed candles purani hain; fresh evidence ke bina draft nahi bana.', 'Incomplete candle': 'Provider ne incomplete/future candle di; fresh closed candles chahiye.', 'Missing or duplicated candles; do not synthesize gaps': 'Market candle history mein gap ya duplicate hai; unsupported history se analysis nahi bana.', 'Evidence block exceeds configured word budget': 'Post ki word limit evidence block se chhoti hai; /settings mein post limits check karo.', 'Evidence and word minimum cannot fit configured character budget': 'Word minimum aur evidence block character budget mein fit nahi ho sakte; post settings adjust karo ya article use karo.', 'AI article sections repeat': 'AI ne article sections repeat kiye; quality check ne repetitive article save/publish nahi kiya.', 'Daily AI request budget exhausted': 'Bot ka daily AI request budget khatam hai; DESK_AI_DAILY_REQUESTS check karo.', 'Daily AI token reservation exhausted': 'Bot ka daily AI token reservation budget khatam hai; DESK_AI_DAILY_TOKENS check karo.', 'AI request parameters rejected': 'AI provider ne request parameters reject kiye (HTTP 400); model aur JSON mode compatibility check karo.', 'AI structured output generation failed': 'AI provider valid structured JSON generate nahi kar paya (HTTP 400); output validation fail hui.', 'AI authentication failed': 'AI API key accept nahi hui (HTTP 401); DESK_AI_KEY check karo.', 'AI access denied': 'AI provider ne access deny kiya (HTTP 403); account/model permissions check karo.', 'AI endpoint or model unavailable': 'AI endpoint ya model nahi mila (HTTP 404); DESK_AI_URL aur DESK_AI_MODEL check karo.', 'AI provider rate limit reached': 'AI provider ki rate limit lagi (HTTP 429); daily ya per-minute quota check karo, turant repeat mat karo.', 'AI provider credits unavailable': 'AI provider credits unavailable hain; paid model automatically use nahi kiya gaya.', 'All configured AI providers unavailable': 'Configured AI providers unavailable/cooldown mein hain ya output validation fail hui. /ai_status se provider health aur global bot budget dekho.', 'Unknown AI provider': 'Provider name /ai_status ke configured_order se copy karo.', 'AI output token limit reached': 'AI output token limit par ruk gaya; article/JSON incomplete hai. AI output budget ka code fix chahiye.', 'AI service rejected request': 'AI provider ne request reject ki; provider status check karo.', 'AI network request failed': 'AI request timeout ya network failure hua.', 'AI request timed out': 'AI response timeout hua; provider cooldown aur backup /ai_status mein dekho.', 'AI connection failed': 'AI provider se connection nahi bana; provider cooldown aur backup /ai_status mein dekho.', 'AI network protocol failed': 'AI provider connection incomplete hua; provider cooldown aur backup /ai_status mein dekho.', 'AI response read failed': 'AI provider ka response read nahi ho paya; /ai_status mein cooldown aur backup dekho.', 'AI response unavailable or malformed': 'AI response valid JSON format mein nahi aaya.', 'Invalid AI structure': 'AI JSON mein title/body text missing hai.', 'AI output too large': 'AI output allowed size se bada hai.', 'AI wrote unbound numerical claims': 'AI ne evidence tokens ke bina numbers likhe; quality validation ne draft roka.', 'AI numerical correction failed': 'AI ne ek correction attempt ke baad bhi unsupported numbers likhe; draft save/publish nahi hua.', 'AI evidence too large': 'AI evidence input allowed size se bada hai.', 'Unknown fact token': 'AI ne unknown evidence token use kiya; quality validation ne draft roka.', 'Same underlying event already covered': 'BTC ya selected symbol ka event pehle se covered hai. /queue aur /posts check karo; existing draft ID use karo.', 'Draft too similar to recent content': 'Draft recent content jaisa hai. /queue aur /posts check karo; duplicate post blocked hai.', 'Unknown draft': 'Draft ID nahi mili. /queue se current database ki actual ID copy karo.', 'Cannot change published media': 'Image sirf pending draft par add hoti hai. Published post ke liye fresh draft chahiye.', 'Image budget exhausted': 'Daily chart cap reached. Text posting available hai; /settings mein image_daily_cap dekho.', 'Chart evidence expired': 'Draft ka chart evidence expire ho gaya hai. Fresh draft par image banao.', 'Exact chart evidence unavailable; regenerate draft': 'Draft ki exact closed-candle snapshot available nahi hai. Fresh /post_now SYMBOL draft banao.', 'Image creation failed; draft unchanged': 'Chart render/storage fail hua; draft aur approval unchanged hain. /errors aur artifact storage check karo.', 'Image queue full; text posting remains available': 'Image queue full hai; text posting continue karegi. Pending image complete hone ke baad chart command do.', 'Image renderer busy; text posting remains available': 'Ek chart render ho raha hai; /status aur text workflow available hain. Image command repeat mat karo.', 'Paper signal ID is not a draft ID': 'Ye paper signal ID hai. /preview ID se signal details aur /queue se actual draft ID dekho; signal ko approve/publish nahi kiya ja sakta.', 'Draft is immutable after publication or expiry': 'Draft publish, reject ya expire ho chuka hai. /queue se active draft select karo.', 'Symbol must be on configured watchlist': 'Symbol watchlist mein nahi hai. /settings mein symbols check karo.', 'Market evidence expired or future dated': 'Market evidence expired ya future dated hai. Fresh draft chahiye.', 'Word count outside configured limits': 'Draft word count configured limits se match nahi karta. /settings check karo.', 'Number is not bound to recorded evidence': 'Draft mein ek number recorded evidence se match nahi karta; publishing blocked hai.', 'Missing source or timestamp': 'Draft ka source ya timestamp missing hai.', 'Missing relevant ticker': 'Draft mein relevant ticker missing hai.', 'Unknown signal': 'Signal ID nahi mili. /signals se actual Signal ID copy karo.', 'Signal analysis requires a configured AI endpoint': 'Signal analysis ke liye AI integration chahiye; /ai_status check karo.', 'Signal is inactive or stale; use a fresh signal': 'Signal stale, triggered, closed ya invalidated hai. /signals se fresh watching signal use karo.', 'Signal market evidence does not match': 'Signal ke symbol/source/quote se fresh market data match nahi hua; draft nahi bana.', 'Signal setup no longer qualifies on fresh evidence': 'Fresh candles par setup qualify nahi hua ya direction badal gayi; purana signal publish nahi hoga.', 'Signal draft missing setup levels or direction': 'AI signal analysis mein required levels/direction missing hain; draft validation fail hui.', 'Signal draft missing heuristic score disclosure': 'AI ne heuristic score ka disclosure nahi diya; draft validation fail hui.', 'Invalid formatting': 'Draft formatting validation fail hui.', 'Prohibited promotion or unsupported profit language': 'Draft content validation fail hui; unsupported promotional claim blocked hai.', 'Control characters are not permitted': 'Draft mein invalid control characters hain.', 'Long articles require a configured AI endpoint; short analysis stays available': 'Article ke liye AI integration chahiye. Short post ke liye /post_now BTC use karo.'}
    if isinstance(error, ValueError):
        chars = re.fullmatch('Square (short post|article) length (\\d+); maximum (\\d+) characters', str(error))
        if chars:
            kind, actual, maximum = chars.groups()
            return f'Command rejected: {kind} mein {actual} character units aaye; limit {maximum}. Complete content rewrite chahiye; publish request nahi bheji gayi.'
        length = re.fullmatch('AI body word count (\\d{1,6}); required (\\d{1,6})-(\\d{1,6})', str(error))
        if length:
            count, lower, upper = length.groups()
            return f'Command rejected: AI body mein {count} words aaye; required {lower}-{upper}. Draft save/publish nahi hua.'
        messages = [safe[reason] for reason in str(error).split('; ') if reason in safe]
        if messages:
            return 'Command rejected: ' + ' '.join(dict.fromkeys(messages))
    if isinstance(error, sqlite3.IntegrityError):
        return 'Command rejected: database constraint conflict. /queue aur /posts check karo; duplicate automatically retry nahi kiya gaya.'
    return 'Command rejected. Check input, draft state, evidence freshness, or campaign requirements.'
HELP = 'Square Desk controls\n/status /ai_status /ai_job /publish_status /image_status [full] /ai_test PROVIDER /ai_retry PROVIDER /auto /approval /universe /dashboard /queue /next /today /posts /articles\n/movers [15m|1h|4h|12h|24h|3d|7d] /gainers /losers /signals /alerts\n/scan /education /post_now SYMBOL /article_now SYMBOL /signal_draft SIGNAL_ID /image_now ID\n/manual /manual_status /manual_queue /manual_preview ID\n/manual_cancel [ID] /manual_retry ID /manual_reconcile ID posted|not_posted CONFIRM\n/preview ID /approve ID /reject ID /edit ID Title|Body\n/regenerate ID /reschedule ID ISO_DATE /delete_queue ID\n/pause /resume /emergency_stop /unlock CONFIRM\n/projects /project_add key:value lines or JSON\n/project_activate ID /project_draft ID /project_pause ID\n/project_resume ID /project_end ID /project_edit ID JSON\n/reports /performance /metrics JSON /providers /bandwidth /errors /logs\n/news /news_add JSON /news_verify ID /news_draft ID\n/settings /set daily_target|hourly_cap|gap_seconds VALUE\n/policy_clear CONFIRM /reconcile ID posted|not_posted CONFIRM\nPaper mode is default. Live trading is never supported.'

class Telegram:

    def __init__(self, settings, store, desk, client):
        self.s, self.db, self.desk, self.client = (settings, store, desk, client)
        self.job_task = None
        self.flush_lock = asyncio.Lock()
        self.control_runtime = None
        self.next_outbox_check = 0
        self.outbox_authority = None

    def authorized(self, message):
        if not isinstance(message, dict) or not isinstance(message.get('from'), dict) or (not isinstance(message.get('chat'), dict)):
            return False
        return message.get('from', {}).get('id') in self.s.telegram_admins and message.get('chat', {}).get('id') == message.get('from', {}).get('id') and (message.get('chat', {}).get('type') == 'private')

    def notify(self, text, key=None):
        if not self.s.telegram_token:
            return
        chunks, chars, units = ([], [], 0)
        for char in text[:100000]:
            size = 2 if ord(char) > 65535 else 1
            if units + size > 3500:
                chunks.append(''.join(chars))
                chars, units = ([], 0)
            chars.append(char)
            units += size
        if chars:
            chunks.append(''.join(chars))
        for admin in self.s.telegram_admins:
            for index, chunk in enumerate(chunks):
                try:
                    label = f'[{index + 1}/{len(chunks)}]\n' if len(chunks) > 1 else ''
                    self.db.insert('outbox', {'chat_id': admin, 'text': label + chunk}, fingerprint=f'{admin}:{key}:{index}' if key else None)
                    self.next_outbox_check = 0
                except sqlite3.IntegrityError:
                    pass

    async def api(self, method, body):
        try:
            response = await self.client.post(f'https://api.telegram.org/bot{self.s.telegram_token}/{method}', json=body)
            if False:
                data = None
            if False:
                delay = None

            def _db_work_1():
                nonlocal data, delay
                if response.status_code == 429:
                    data = response.json()
                    delay = float(data.get('parameters', {}).get('retry_after', 60))
                    self.db.set('telegram_retry_after', utc() + max(1, delay))
                    return ('return', None)
                if response.status_code != 200:
                    return ('return', None)
                data = response.json()
                return ('return', data.get('result') if data.get('ok') else None)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_1)
            if _db_control == 'return':
                return _db_result
        except (httpx.HTTPError, ValueError, TypeError):

            def _db_work_2():
                self.db.log('TELEGRAM', 'API unavailable or invalid response')
                return ('return', None)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_2)
            if _db_control == 'return':
                return _db_result

    async def flush(self):
        async with self.flush_lock:
            await self._flush()

    async def _flush(self):
        if False:
            authority = None
        if False:
            conn = None
        if False:
            journal = None
        if False:
            rows = None

        def _db_work_3():
            nonlocal authority, conn, journal, rows
            conn = self.db.conn
            journal = getattr(conn, 'journal', None)
            authority = (getattr(conn, 'current', None), getattr(journal, 'epoch', None), getattr(self.db, '_outbox_version', 0))
            if authority != self.outbox_authority:
                self.next_outbox_check = 0
                self.outbox_authority = authority
            if utc() < self.next_outbox_check:
                return ('return', None)
            if not self.s.telegram_token or utc() < self.db.state('telegram_retry_after', 0):
                self.next_outbox_check = utc() + 30
                return ('return', None)
            rows = self.db.pending_notifications()
            self.next_outbox_check = utc() + 30 if not rows else 0
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_3)
        if _db_control == 'return':
            return _db_result
        for row in rows:
            if False:
                buttons = None
            if False:
                command = None
            if False:
                label = None
            if False:
                p = None
            if False:
                pair = None

            def _db_work_4():
                nonlocal buttons, command, label, p, pair
                p = row['payload']
                if p.get('next_attempt', 0) > utc():
                    if row.get('due') != p['next_attempt']:
                        self.db.update(row['id'], due=p['next_attempt'])
                    return ('continue', None)
                if not self.db.claim(row['id'], 'pending', 'sending'):
                    return ('continue', None)
                buttons = {'inline_keyboard': [[{'text': label, 'callback_data': command} for label, command in pair] for pair in [[('Status', '/status'), ('Queue', '/queue')], [('Movers', '/movers'), ('Signals', '/signals')], [('Projects', '/projects'), ('Reports', '/reports')], [('Pause', '/pause'), ('Resume', '/resume')], [('Emergency stop', '/emergency_stop')]]]}
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_4)
            if _db_control == 'continue':
                continue
            result = await self.api('sendMessage', {'chat_id': p['chat_id'], 'text': p['text'], 'reply_markup': buttons})

            def _db_work_5():
                if result:
                    self.db.update(row['id'], status='delivered')
                    self.db.set('last_telegram_delivery', utc())
                else:
                    p['attempts'] = p.get('attempts', 0) + 1
                    p['next_attempt'] = utc() + min(3600, 15 * 2 ** p['attempts'])
                    self.db.update(row['id'], status='failed' if p['attempts'] >= 5 else 'pending', payload=p, due=p['next_attempt'])
                    if utc() < self.db.state('telegram_retry_after', 0):
                        return ('break', None)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_5)
            if _db_control == 'break':
                break

    async def poll(self):

        def _db_work_6():
            if self.control_runtime is not None:
                return ('return', None)
            if not self.s.telegram_token or not self.s.telegram_polling or utc() < self.db.state('telegram_retry_after', 0):
                return ('return', None)
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_6)
        if _db_control == 'return':
            return _db_result
        result = await self.api('getUpdates', {'offset': self.db.state('telegram_offset', 0), 'timeout': 0, 'limit': 30, 'allowed_updates': ['message', 'callback_query']})
        if isinstance(result, list):
            for update in result:
                await self.receive(update)

                def _db_work_7():
                    self.db.set('telegram_offset', int(update['update_id']) + 1)
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_7)

    async def receive(self, update, *, flush=True, answer_callback=True):
        if False:
            callback = None
        if False:
            command_id = None
        if False:
            ident = None
        if False:
            manual = None
        if False:
            message = None
        if False:
            minute = None
        if False:
            name = None
        if False:
            rate = None
        if False:
            rate_key = None
        if False:
            text = None

        def _db_work_8():
            nonlocal callback, command_id, ident, manual, message, minute, name, rate, rate_key, text
            if not isinstance(update, dict) or type(update.get('update_id')) is not int:
                raise ValueError('Invalid Telegram update')
            callback = update.get('callback_query')
            message = update.get('message', {})
            if callback:
                message = {**callback.get('message', {}), 'from': callback.get('from', {}), 'text': callback.get('data', '')}
            if not self.authorized(message):
                self.db.log('SECURITY', 'Unauthorized Telegram command rejected')
                return ('return', None)
            ident = str(message['from']['id'])
            minute = int(utc() // 60)
            rate_key = f'command_rate:{ident}'
            with self.db.transaction():
                rate = self.db.state(rate_key, {'minute': minute, 'count': 0})
                if rate['minute'] != minute:
                    rate = {'minute': minute, 'count': 0}
                if rate['count'] >= 15:
                    return ('return', None)
                try:
                    command_id = self.db.insert('command', {'sender': int(ident), 'command': str(message.get('text', '')).split(' ', 1)[0][:60]}, 'processing', fingerprint='telegram:' + str(update['update_id']))
                except sqlite3.IntegrityError:
                    return ('return', None)
                rate['count'] += 1
                self.db.set(rate_key, rate)
            text = str(message.get('text', ''))
            name = text.strip().split(maxsplit=1)[0].split('@')[0].lower() if text.strip() else ''
            manual = getattr(self.desk, 'manual', None)
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_8)
        if _db_control == 'return':
            return _db_result
        if manual and manual.handles(message):
            try:
                result = manual.receive(message)

                def _db_work_9():
                    self.db.update(command_id, status='applied')
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_9)
            except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as error:
                if False:
                    result = None

                def _db_work_10():
                    nonlocal result
                    self.db.update(command_id, status='rejected')
                    result = str(error) if isinstance(error, ValueError) else 'Manual input rejected; use /manual_queue.'
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_10)

            def _db_work_11():
                self.notify(result, key='command:' + str(update['update_id']))
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_11)
            if callback and answer_callback:
                await self.api('answerCallbackQuery', {'callback_query_id': callback['id']})
            if flush:
                await self.flush()
            return
        if name in ('/post_now', '/article_now', '/signal_draft', '/regenerate', '/project_draft', '/news_draft', '/ai_test', '/image_now') and (not getattr(self.desk, 'batch_seconds', 0)):
            if self.job_task and (not self.job_task.done()):

                def _db_work_12():
                    self.db.update(command_id, status='rejected')
                    self.notify('Ek AI command abhi processing mein hai. Uska result aane do; /status aur /ai_status available hain.', key='command:' + str(update['update_id']))
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_12)
            else:

                def _db_work_13():
                    self.notify('Command received. Draft/image processing shuru ho rahi hai; result alag message mein aayega. Command repeat mat karo.', key='command-start:' + str(update['update_id']))
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_13)
                self.job_task = asyncio.create_task(self._background_command(text, command_id, update['update_id']))
            if callback and answer_callback:
                await self.api('answerCallbackQuery', {'callback_query_id': callback['id']})
            if flush:
                await self.flush()
            return
        try:
            result = await self.command(str(message.get('text', '')))

            def _db_work_14():
                self.db.update(command_id, status='applied')
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_14)
        except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as error:
            if False:
                result = None

            def _db_work_15():
                nonlocal result
                self.db.update(command_id, status='rejected')
                result = rejection_message(error)
                self.db.log('TELEGRAM', result)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_15)

        def _db_work_16():
            self.notify(result, key='command:' + str(update['update_id']))
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_16)
        if callback and answer_callback:
            await self.api('answerCallbackQuery', {'callback_query_id': callback['id']})
        if flush:
            await self.flush()

    async def _background_command(self, text, command_id, update_id):
        command = text.strip().split(maxsplit=1)[0].split('@')[0].lower()
        job = {'id': command_id, 'command': command, 'stage': 'command_validation', 'started_at': utc()}
        token = AI_JOB.set(job)
        progress = asyncio.create_task(self._job_progress(job))
        try:
            try:
                if False:
                    article = None
                if False:
                    parts = None
                if False:
                    previous = None
                if False:
                    timeout = None

                def _db_work_17():
                    nonlocal article, parts, previous, timeout
                    parts = text.strip().split(maxsplit=1)
                    previous = self.db.get(parts[1]) if command == '/regenerate' and len(parts) > 1 else None
                    article = command == '/article_now' or bool(previous and previous['payload'].get('article'))
                    timeout = self.s.ai_article_timeout_seconds if article else self.s.ai_command_timeout_seconds
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_17)
                result = await asyncio.wait_for(self.command(text), timeout=timeout)

                def _db_work_18():
                    self.db.update(command_id, status='applied')
                    job.update(status='completed', result=result[:3500])
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_18)
            except asyncio.TimeoutError:
                if False:
                    result = None

                def _db_work_19():
                    nonlocal result
                    result = 'Job ' + command_id + ': command time limit reached at ' + job['stage'] + '. /ai_job dekho; command repeat karne se pehle /queue check karo.'
                    self.db.update(command_id, status='rejected')
                    job.update(status='timeout', reason=result)
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_19)
            except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as error:
                if False:
                    result = None

                def _db_work_20():
                    nonlocal result
                    result = rejection_message(error)
                    if not isinstance(error, GenerationFailed):
                        result = 'Job ' + command_id + ' at ' + job['stage'] + ': ' + result
                    self.db.update(command_id, status='rejected')
                    self.db.log('TELEGRAM', result)
                    job.update(status='failed', reason=result)
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_20)
            except Exception as error:
                if False:
                    result = None

                def _db_work_21():
                    nonlocal result
                    self.db.update(command_id, status='rejected')
                    self.db.log('ERROR', 'Telegram generation failed: ' + type(error).__name__)
                    result = 'Job ' + command_id + ': internal ' + type(error).__name__ + ' at ' + job['stage'] + '. /ai_job aur /queue dekho; success confirm nahi hua.'
                    job.update(status='failed', reason=result)
                    return (None, None)
                _db_control, _db_result = await run_db(self.db, _db_work_21)
            job['finished_at'] = utc()
            progress.cancel()
            try:
                await progress
            except asyncio.CancelledError:
                pass

            def _db_work_22():
                self.db.set('command_active_job', {})
                self.db.set('command_last_job', job)
                self.notify(result, key='command:' + str(update_id))
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_22)
            await self.flush()
        except asyncio.CancelledError:

            def _db_work_23():
                job.update(status='cancelled', reason='Worker stopped or lease lost', finished_at=utc())
                self.db.set('command_last_job', job)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_23)
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

            def _db_work_24():
                self.db.set('command_active_job', {})
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_24)

    async def _job_progress(self, job):
        while True:

            def _db_work_25():
                self.db.set('command_active_job', job)
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_25)
            await asyncio.sleep(self.s.ai_progress_seconds)

            def _db_work_26():
                if job.get('finished_at') or job.get('status') in ('completed', 'failed', 'timeout', 'cancelled'):
                    return ('return', None)
                self.notify('Job ' + job['id'] + ' processing: ' + job['stage'] + '. /ai_job available hai; command repeat mat karo.')
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_26)
            if _db_control == 'return':
                return _db_result
            await self.flush()

    async def stop_job(self):
        if self.job_task and (not self.job_task.done()):
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
        command, arg = (parts[0].split('@')[0].lower(), parts[1].strip() if len(parts) > 1 else '')
        if command in ('/start', '/help'):
            return HELP
        if command == '/status' and (not arg):
            from .compact_status import status
            return json.dumps(status(self.desk), indent=2)
        if False:
            ai = None
        if False:
            generation = None
        if False:
            job = None

        def _db_work_27():
            nonlocal ai, generation, job
            if command in ('/status', '/dashboard'):
                return ('return', json.dumps(self.desk.status(), indent=2))
            if command == '/ai_job':
                job = self.db.state('command_active_job', {}) or self.db.state('command_last_job', {})
                generation = self.db.state('ai_active_generation', {})
                if not generation or generation.get('id') != job.get('id'):
                    generation = self.db.state('ai_last_manual_job', {})
                if generation.get('id') != job.get('id'):
                    generation = {}
                return ('return', json.dumps({'command': job, 'generation': generation}, indent=2))
            if command == '/publish_status':
                return ('return', json.dumps(self.desk.publisher.status(), indent=2))
            if command == '/image_status':
                return ('return', json.dumps(self.desk.image_status(full=arg.lower() == 'full'), indent=2))
            if command == '/ai_status':
                ai = self.desk.content.ai
                return ('return', json.dumps(ai.status() if ai and hasattr(ai, 'status') else {'configured': ai is not None}, indent=2))
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_27)
        if _db_control == 'return':
            return _db_result
        if command == '/ai_test':
            ai = self.desk.content.ai
            if not ai or not hasattr(ai, 'test'):
                raise ValueError('Unknown AI provider')
            return json.dumps(await ai.test(arg.lower()), indent=2)
        if False:
            ai = None
        if False:
            states = None
        if False:
            values = None

        def _db_work_28():
            nonlocal ai, states, values
            if command == '/ai_retry':
                ai = self.desk.content.ai
                if not ai or not hasattr(ai, 'providers') or arg not in ai.providers:
                    raise ValueError('Unknown AI provider')
                states = self.db.state('ai_provider_health', {})
                states.pop(arg, None)
                self.db.set('ai_provider_health', states)
                return ('return', f'{arg} local cooldown cleared. Provider quota and global daily budgets are unchanged.')
            if command in ('/auto', '/approval'):
                values = self.db.state('runtime_settings', {})
                values.update(mode='automatic' if command == '/auto' else 'approval', ai_auto_publish=command == '/auto')
                self.db.set('runtime_settings', values)
                self.s.mode, self.s.ai_auto_publish = (values['mode'], values['ai_auto_publish'])
                self.db.log('SECURITY', 'Publication mode changed: ' + self.s.mode)
                return ('return', 'Automatic mode enabled for new validated medium-risk text drafts. Daily/hourly caps, pause, freshness, duplicate and live gates remain active. High-risk, edited, campaign and image drafts still require review.' if command == '/auto' else 'Approval mode enabled; new drafts require approval.')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_28)
        if _db_control == 'return':
            return _db_result
        if command == '/universe':
            await self.desk.refresh_universe()
            return json.dumps({'mode': self.s.market_universe, 'status': self.desk.universe_state, 'selected_count': len(self.desk.universe), 'symbols': self.desk.universe, 'scan_batch_size': self.s.scan_batch_size}, indent=2)
        if False:
            r = None
        if False:
            rows = None

        def _db_work_29():
            nonlocal r, rows
            if command in ('/queue', '/next', '/schedule'):
                rows = self.db.list('draft', ['review', 'approved', 'queued', 'chart_rendering', 'uploading_image'])
                return ('return', '\n'.join((f"{r['id']} {r['status']} {r['payload']['title']}" for r in rows[:20])) or 'Queue empty')
            if command in ('/posts', '/articles', '/today', '/alerts'):
                rows = self.db.list('draft', since=utc() - 86400 if command == '/today' else 0)
                if command == '/articles':
                    rows = [r for r in rows if r['payload'].get('article')]
                if command == '/alerts':
                    rows = [r for r in rows if r['payload']['category'] == 'shock']
                return ('return', '\n'.join((f"{r['id']} {r['status']} {r['payload']['title']}" for r in rows[:20])) or 'No records')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_29)
        if _db_control == 'return':
            return _db_result
        if command in ('/movers', '/gainers', '/losers'):
            from .analysis import rank, WINDOWS
            if False:
                r = None
            if False:
                rows = None
            if False:
                window = None

            def _db_work_30():
                nonlocal r, rows, window
                window = arg or '24h'
                if window not in WINDOWS:
                    raise ValueError('Invalid ranking window')
                rows = rank(self.db.state('latest_market', []), window, command != '/losers')[:10]
                return ('return', '\n'.join((f"${r['symbol']} {r['changes'][window]:+.2f}% ({window}, {r['metrics']['source']}/{r['metrics']['quote']})" for r in rows)) or 'No fresh ranked data')
                return (None, None)
            _db_control, _db_result = await run_db(self.db, _db_work_30)
            if _db_control == 'return':
                return _db_result
        if False:
            ident = None
        if False:
            r = None
        if False:
            rows = None

        def _db_work_31():
            nonlocal ident, r, rows
            if command == '/signals':
                rows = self.db.list('signal', limit=10)
                return ('return', '\n'.join((f"{r['id']} ${r['payload']['symbol']} {r['payload']['direction']} {r['status']} score={r['payload']['confidence']}" for r in rows)) or 'No eligible setups')
            if command in ('/pause', '/emergency_stop'):
                self.db.set('paused' if command == '/pause' else 'emergency_stop', True)
                self.db.log('SECURITY', 'Administrator paused publication')
                return ('return', 'Publication stopped. Collection and paper tracking continue.')
            if command == '/resume':
                self.db.set('paused', False)
                return ('return', 'Ordinary pause cleared. Emergency stop remains independent.')
            if command == '/unlock':
                if arg != 'CONFIRM':
                    return ('return', 'Send /unlock CONFIRM to clear the emergency stop.')
                self.db.set('emergency_stop', False)
                return ('return', 'Emergency stop cleared.')
            if command == '/scan':
                self.db.set('scan_requested', True)
                return ('return', 'Scan requested for the next worker cycle.')
            if command == '/education':
                ident = self.desk.content.education()
                return ('return', 'Educational draft ' + ident if ident else 'Available lessons already covered recently.')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_31)
        if _db_control == 'return':
            return _db_result
        if command in ('/post_now', '/article_now'):
            ident = await self.desk.create_for_symbol(arg.upper(), article=command == '/article_now')
            return f'Draft {ident} created. Review with /preview {ident}; publishing limits still apply.'
        if command == '/signal_draft':
            ident = await self.desk.create_for_signal(arg)
            row = self.desk.get_draft(ident)
            p = row['payload']
            return f"Signal {arg} → AI draft {ident} [{row['status']}]\n{p['title']}\n\n{p['body']}\n\n/preview {ident}\n/approve {ident}\n/reject {ident}\nFresh revalidated analysis; approval and scheduled publication required."
        if False:
            body = None
        if False:
            content = None
        if False:
            ident = None
        if False:
            linked = None
        if False:
            links = None
        if False:
            p = None
        if False:
            r = None
        if False:
            row = None
        if False:
            signal = None
        if False:
            title = None

        def _db_work_32():
            nonlocal body, content, ident, linked, links, p, r, row, signal, title
            if command == '/preview':
                signal = self.db.get(arg)
                if signal and signal['kind'] == 'signal':
                    p = signal['payload']
                    linked = [r for r in self.db.list('draft', limit=500) if (r['payload'].get('event') or {}).get('signal_id') == arg]
                    links = '\n'.join((f"Draft {r['id']} [{r['status']}] /preview {r['id']}" for r in linked))
                    return ('return', f"Paper signal {signal['id']} [{signal['status']}]\n${p.get('symbol', '')} {p.get('direction', '')}\nEntry {p.get('entry', 'unavailable')}, stop {p.get('stop', 'unavailable')}, target {p.get('target1', 'unavailable')} {p.get('quote', '')}\nYe tracking signal hai; is ID ko approve/publish nahi kiya ja sakta.\n" + (links if links else f'Is signal ka linked post draft nahi mila. Create: /signal_draft {arg}'))
                row = self.desk.get_draft(arg)
                p = row['payload']
                return ('return', f"{row['id']} [{row['status']}] [{p.get('generated_by', 'built-in/manual')}] {len(p['body'].split())} words\n{p['title']}\n\n{p['body']}")
            if command in ('/approve', '/reject', '/delete_queue'):
                self.desk.review(arg, command == '/approve')
                return ('return', 'Draft approved for scheduling.' if command == '/approve' else 'Draft rejected and retained in audit history.')
            if command == '/edit':
                ident, content = arg.split(maxsplit=1)
                title, body = content.split('|', 1)
                self.desk.edit(ident, title.strip(), body.strip())
                return ('return', 'Edited draft requires fresh approval.')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_32)
        if _db_control == 'return':
            return _db_result
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
            job = AI_JOB.get()
            if job:
                job['stage'] = 'chart_rendering'
            return 'Chart attached: ' + await self.desk.make_image_async(arg) + '\nDashboard: Preview chart.\nMedia change requires fresh approval: /approve ' + arg + '\nApproved text+chart publishes through the Square image-upload adapter.'
        if False:
            ident = None
        if False:
            r = None

        def _db_work_33():
            nonlocal ident, r
            if command in ('/projects', '/giveaways'):
                return ('return', '\n'.join((f"{r['id']} {r['status']} {r['payload']['name']} remaining={max(0, r['payload']['quota'] - self.desk.campaigns.count(r['id']))}" for r in self.db.list('campaign'))) or 'No campaigns')
            if command == '/project_add':
                ident = self.desk.campaigns.add(arg)
                return ('return', f'Project {ident} parsed. Inspect supplied terms and activate with /project_activate {ident}.')
            if command == '/project_activate':
                self.desk.campaigns.activate(arg)
                return ('return', 'Supplied campaign terms marked reviewed; project activated.')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_33)
        if _db_control == 'return':
            return _db_result
        if command == '/project_draft':
            return 'Campaign draft ' + await self.desk.campaigns.draft(arg)
        if False:
            ident = None
        if False:
            payload = None
        if False:
            r = None
        if False:
            row = None

        def _db_work_34():
            nonlocal ident, payload, r, row
            if command in ('/project_pause', '/project_resume', '/project_end'):
                row = self.db.get(arg)
                if not row or row['kind'] != 'campaign' or (not row['payload']['verified']) or (row['payload']['end'] <= utc()):
                    raise ValueError('Invalid project state')
                self.db.update(arg, status={'/project_pause': 'paused', '/project_resume': 'active', '/project_end': 'ended'}[command])
                return ('return', 'Project state updated.')
            if command == '/project_edit':
                ident, payload = arg.split(maxsplit=1)
                self.desk.campaigns.edit(ident, payload)
                return ('return', 'Project changed. Verification and draft approvals reset.')
            if command in ('/reports', '/performance'):
                return ('return', json.dumps(self.desk.analytics.report(), indent=2))
            if command == '/metrics':
                return ('return', 'Metrics import ' + self.desk.analytics.import_metrics(json.loads(arg)))
            if command == '/news':
                return ('return', '\n'.join((f"{r['id']} {r['status']} {r['payload']['title']}\n{r['payload']['source_url']}" for r in self.db.list('news', limit=10))) or 'No news source records')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_34)
        if _db_control == 'return':
            return _db_result
        if command == '/news_add':
            from .news import add_source_record
            return 'News source record ' + add_source_record(self.db, json.loads(arg))
        if False:
            p = None
        if False:
            row = None

        def _db_work_35():
            nonlocal p, row
            if command == '/news_verify':
                row = self.db.get(arg)
                if not row or row['kind'] != 'news':
                    raise ValueError('Unknown news source record')
                p = row['payload']
                p['verification'], p['verified_by'] = ('verified', 'administrator')
                self.db.update(arg, status='verified', payload=p)
                self.db.log('CONTENT', 'Administrator reviewed primary news source', arg)
                return ('return', 'Source marked reviewed. Feed records still require an original summary via /news_add.')
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_35)
        if _db_control == 'return':
            return _db_result
        if command == '/news_draft':
            from .news import news_draft
            return 'Reviewed news draft ' + news_draft(self.db, self.desk.content.checker, arg)
        if command in ('/providers', '/data_status'):
            return json.dumps({'status_source': 'memory_observations', 'providers': self.desk.pool.observations()}, indent=2)
        if command == '/bandwidth':
            from .bandwidth import compact_status
            return compact_status()
        if False:
            confirm = None
        if False:
            ident = None
        if False:
            key = None
        if False:
            outcome = None
        if False:
            row = None
        if False:
            value = None
        if False:
            values = None

        def _db_work_36():
            nonlocal confirm, ident, key, outcome, row, value, values
            if command in ('/errors', '/logs'):
                return ('return', json.dumps(self.db.logs(15), indent=2))
            if command == '/settings':
                return ('return', json.dumps({'configuration': self.s.public(), 'effective_caps': self.desk.publisher.policy.caps()}, indent=2))
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
                if key == 'daily_target' and self.s.require_post_image:
                    self.s.image_daily_cap = value
                self.db.log('SECURITY', 'Runtime publication setting changed: ' + key)
                return ('return', 'Setting persisted.')
            if command == '/policy_clear':
                if arg != 'CONFIRM':
                    return ('return', 'After reviewing the provider restriction, send /policy_clear CONFIRM.')
                self.db.set('publisher_blocked', False)
                self.db.set('publisher_block_reason', {})
                return ('return', 'Restriction latch cleared. Lowered caps and cooldown remain.')
            if command == '/reconcile':
                ident, outcome, confirm = arg.split()
                if confirm != 'CONFIRM' or outcome not in ('posted', 'not_posted'):
                    raise ValueError('Inspect Creator Center before reconciling')
                row = self.desk.get_draft(ident)
                if row['status'] not in ('uncertain', 'manual_ready'):
                    raise ValueError('No uncertain or manual submission')
                self.db.update(ident, status='manual_published' if outcome == 'posted' else 'review')
                self.db.log('PUBLISH', 'Administrator reconciled remote outcome: ' + outcome, ident)
                return ('return', 'Submission reconciled.')
            return ('return', HELP)
            return (None, None)
        _db_control, _db_result = await run_db(self.db, _db_work_36)
        if _db_control == 'return':
            return _db_result
