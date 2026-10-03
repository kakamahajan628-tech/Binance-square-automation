"""Allowlisted controls, durable update deduplication, bounded retries and private delivery."""
import json
import re
import sqlite3
import httpx
from .models import utc, uid


def rejection_message(error):
    # Only fixed application-owned messages may be exposed. Raw exceptions
    # can contain database credentials, provider URLs or administrator input.
    safe = {
        'Daily AI request budget exhausted': 'Bot ka daily AI request budget khatam hai; DESK_AI_DAILY_REQUESTS check karo.',
        'Daily AI token reservation exhausted': 'Bot ka daily AI token reservation budget khatam hai; DESK_AI_DAILY_TOKENS check karo.',
        'AI request parameters rejected': 'AI provider ne request parameters reject kiye (HTTP 400); model aur JSON mode compatibility check karo.',
        'AI authentication failed': 'AI API key accept nahi hui (HTTP 401); DESK_AI_KEY check karo.',
        'AI access denied': 'AI provider ne access deny kiya (HTTP 403); account/model permissions check karo.',
        'AI endpoint or model unavailable': 'AI endpoint ya model nahi mila (HTTP 404); DESK_AI_URL aur DESK_AI_MODEL check karo.',
        'AI provider rate limit reached': 'AI provider ki rate limit lagi (HTTP 429); daily ya per-minute quota check karo, turant repeat mat karo.',
        'AI output token limit reached': 'AI output token limit par ruk gaya; article/JSON incomplete hai. AI output budget ka code fix chahiye.',
        'AI service rejected request': 'AI provider ne request reject ki; provider status check karo.',
        'AI network request failed': 'AI request timeout ya network failure hua.',
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
        'Draft is immutable after publication or expiry': 'Draft publish, reject ya expire ho chuka hai. /queue se active draft select karo.',
        'Symbol must be on configured watchlist': 'Symbol watchlist mein nahi hai. /settings mein symbols check karo.',
        'Market evidence expired or future dated': 'Market evidence expired ya future dated hai. Fresh draft chahiye.',
        'Word count outside configured limits': 'Draft word count configured limits se match nahi karta. /settings check karo.',
        'Number is not bound to recorded evidence': 'Draft mein ek number recorded evidence se match nahi karta; publishing blocked hai.',
        'Missing source or timestamp': 'Draft ka source ya timestamp missing hai.',
        'Missing relevant ticker': 'Draft mein relevant ticker missing hai.',
        'Invalid formatting': 'Draft formatting validation fail hui.',
        'Prohibited promotion or unsupported profit language': 'Draft content validation fail hui; unsupported promotional claim blocked hai.',
        'Control characters are not permitted': 'Draft mein invalid control characters hain.',
        'Long articles require a configured AI endpoint; short analysis stays available': 'Article ke liye AI integration chahiye. Short post ke liye /post_now BTC use karo.',
    }
    if isinstance(error, ValueError):
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
/status /dashboard /queue /next /today /posts /articles
/movers [15m|1h|4h|12h|24h|3d|7d] /gainers /losers /signals /alerts
/scan /education /post_now SYMBOL /article_now SYMBOL /image_now ID
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
        for admin in self.s.telegram_admins:
            try:
                self.db.insert('outbox', {'chat_id': admin, 'text': text[:3900]},
                               fingerprint=f'{admin}:{key}' if key else None)
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
        if command == '/preview':
            row = self.desk.get_draft(arg)
            return f"{row['id']} [{row['status']}]\n{row['payload']['title']}\n\n{row['payload']['body']}"
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
