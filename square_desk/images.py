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


def chart(snapshot, metrics, directory, *, context=None):
    from .chart_views import render
    return render(snapshot,metrics,directory,context=context,font=font,save=save_png)


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
