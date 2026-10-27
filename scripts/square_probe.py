"""Read-only provider connectivity check. No Telegram or publishing requests."""
import asyncio
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from square_desk.providers import Transport, BinanceProvider, CoinbaseProvider
from square_desk.models import utc, stamp


async def main():
    transport = Transport()
    result = {}
    for provider in (BinanceProvider(transport), CoinbaseProvider(transport)):
        try:
            data = await provider.candles('BTC', 900)
            data.validate(utc(), 1200)
            result[provider.name] = {'status': 'ok', 'candles': len(data.candles), 'quote': data.quote,
                                     'data_timestamp': stamp(data.as_of)}
        except Exception as exc:
            result[provider.name] = {'status': 'unavailable', 'error_type': type(exc).__name__}
    await transport.client.aclose()
    print(json.dumps(result, indent=2))
    if not any(r['status'] == 'ok' for r in result.values()):
        raise SystemExit(1)


if __name__ == '__main__':
    asyncio.run(main())
