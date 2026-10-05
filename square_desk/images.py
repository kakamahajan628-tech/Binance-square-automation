"""Real-data raster charts and separate editorial cards. No generated candles."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from .models import stamp
import hashlib
import io


def font(size):
    # Pillow's bundled scalable font keeps Render and Windows output consistent.
    return ImageFont.load_default(size=size)


def save_png(image, directory):
    stream = io.BytesIO()
    image.save(stream, format='PNG')
    data = stream.getvalue()
    # Content-addressed names prevent a redraw or rendering update from
    # overwriting another pending draft's already reviewed chart.
    path = Path(directory) / (hashlib.sha256(data).hexdigest() + '.png')
    with Image.open(io.BytesIO(data)) as verified:
        verified.verify()
    path.write_bytes(data)
    return path.name


def chart(snapshot, metrics, directory):
    snapshot.validate(snapshot.fetched_at, max(snapshot.interval + 300, 1200))
    data = snapshot.candles[-70:]
    image = Image.new('RGB', (1200, 800), '#101827')
    d = ImageDraw.Draw(image)
    d.text((40, 24), f'{snapshot.symbol}/{snapshot.quote} · {snapshot.interval // 60}m closed candles', font=font(27), fill='#eff6ff')
    d.text((40, 65), f'{snapshot.source} | Data: {stamp(snapshot.as_of)}', font=font(17), fill='#a7bbd2')
    low, high = min(c.low for c in data), max(c.high for c in data)
    span = max(high - low, high * .001)
    low, high = low - span * .08, high + span * .08
    def y(p):
        return 555 - (p - low) / (high - low) * 430
    for i in range(6):
        value = low + (high - low) * i / 5
        yy = y(value)
        d.line((45, yy, 1070, yy), fill='#263548', width=1)
        d.text((1080, yy - 8), f'{value:.6g}', fill='#a7bbd2', font=font(15))
    spacing = 1010 / len(data)
    max_volume = max(c.volume for c in data) or 1
    for i, c in enumerate(data):
        x = 50 + i * spacing
        color = '#25ca98' if c.close >= c.open else '#f06b85'
        d.line((x, y(c.high), x, y(c.low)), fill=color, width=2)
        d.rectangle((x - 4, min(y(c.open), y(c.close)), x + 4, max(y(c.open), y(c.close)) + 1), fill=color)
        d.rectangle((x - 4, 710 - c.volume / max_volume * 100, x + 4, 710), fill=color)
    for key, color in (('support', '#eab75d'), ('resistance', '#86a8ef')):
        level = metrics[key]
        if low <= level <= high:
            d.line((45, y(level), 1070, y(level)), fill=color, width=2)
            d.text((55, y(level) - 23), f'{key.capitalize()} {level:.6g}', fill=color, font=font(16))
    d.text((45, 580), 'Volume (base asset units)', font=font(16), fill='#a7bbd2')
    d.text((45, 737), f"RSI {metrics['rsi']:.1f} | ATR {metrics['atr']:.6g} | Research reference levels, no guaranteed outcome", font=font(18), fill='#a7bbd2')
    return save_png(image, directory)


def graphic(title, subtitle, directory):
    import textwrap
    image = Image.new('RGB', (1200, 675), '#101827')
    d = ImageDraw.Draw(image)
    d.rectangle((0, 0, 14, 675), fill='#f3c34a')
    d.text((60, 60), 'SQUARE DESK · MARKET RESEARCH', font=font(24), fill='#f3c34a')
    for i, line in enumerate(textwrap.wrap(title[:150], 40)[:4]):
        d.text((60, 160 + i * 58), line, font=font(38), fill='#f3f6fc')
    for i, line in enumerate(textwrap.wrap(subtitle[:220], 80)[:3]):
        d.text((60, 480 + i * 32), line, font=font(22), fill='#a7bbd2')
    return save_png(image, directory)
