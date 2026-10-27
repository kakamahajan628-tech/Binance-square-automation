"""Explicit one-time webhook setup; secrets are never printed or passed in arguments."""
import argparse
import asyncio
from pathlib import Path
import sys
from urllib.parse import urlsplit
import httpx
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from square_desk.config import Settings


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    args = parser.parse_args()
    parsed = urlsplit(args.url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.path != '/telegram/webhook' or parsed.username:
        raise SystemExit('Use your HTTPS service URL ending /telegram/webhook')
    settings = Settings.from_env()
    if not settings.telegram_token or not settings.webhook_secret or settings.telegram_polling:
        raise SystemExit('Configure Telegram token/secret and disable polling first')
    async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
        try:
            response = await client.post('https://api.telegram.org/bot' + settings.telegram_token + '/setWebhook',
                                         json={'url': args.url, 'secret_token': settings.webhook_secret,
                                               'allowed_updates': ['message', 'callback_query']})
            print('Webhook configured' if response.status_code == 200 and response.json().get('ok') else 'Webhook rejected; inspect Telegram account configuration')
        except (httpx.HTTPError, ValueError):
            raise SystemExit('Webhook configuration unavailable; no secret-bearing exception printed') from None


if __name__ == '__main__':
    asyncio.run(main())
