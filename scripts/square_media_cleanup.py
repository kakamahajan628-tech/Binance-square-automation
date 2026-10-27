"""Explicit offline maintenance. Reuse private process ENV; stop the bot first."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from square_desk.media_cleanup import main

if __name__ == '__main__':
    raise SystemExit(main())
