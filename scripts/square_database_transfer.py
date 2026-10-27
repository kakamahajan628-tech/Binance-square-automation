"""Run from the repository root. DSNs must be supplied through process ENV."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from square_desk.database_transfer import main

if __name__ == '__main__':
    raise SystemExit(main())
