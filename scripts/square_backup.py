"""Consistent SQLite online backup; run from the workspace root."""
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from square_desk.config import Settings
from square_desk.models import stamp


def main():
    settings = Settings.from_env()
    path = Path(settings.database).resolve()
    if not path.is_file():
        raise SystemExit('Database not found; start the service first')
    root = Path('output/square-backups').resolve()
    root.mkdir(parents=True, exist_ok=True)
    name = stamp().replace(':', '-').replace('+', '_') + '.sqlite3'
    target = root / name
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as source:
        with sqlite3.connect(target) as backup:
            source.backup(backup)
    print(str(target))


if __name__ == '__main__':
    main()
