"""Small deployable source archive with a strict allowlist; no credentials or runtime data."""
from pathlib import Path
import hashlib
import json
import zipfile


def main():
    root = Path(__file__).resolve().parents[1]
    files = [p for p in (root / 'square_desk').iterdir() if p.suffix in ('.py', '.html', '.js')]
    files += [root / name for name in ('SQUARE_DESK.md', 'requirements-square.txt',
                                      'requirements-square-dev.txt', '.env.square.example', '.python-version')]
    files += [root / 'scripts' / name for name in ('square_probe.py', 'square_backup.py',
                                                 'square_telegram_setup.py', 'package_square.py')]
    files += [root / 'tests/test_square_desk.py']
    destination = root / 'dist/square-desk-source.zip'
    destination.parent.mkdir(exist_ok=True)
    manifest = {}
    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(files):
            name = path.relative_to(root).as_posix()
            archive.write(path, name)
            manifest[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        deployment = (root / 'render-square.yaml').read_bytes()
        archive.writestr('render.yaml', deployment)
        archive.writestr('render-square.yaml', deployment)
        archive.writestr('README.md', '# Square Desk\n\nRead [SQUARE_DESK.md](SQUARE_DESK.md) for architecture, integration boundaries, setup and launch checks.\n\nInstall `requirements-square.txt`, configure `.env.square`, then run `python -m square_desk`. Paper and approval modes are default.\n')
        archive.writestr('.gitignore', '.venv/\n__pycache__/\n.pytest_cache/\n.env\n.env.*\n!.env.square.example\nruntime/\noutput/\ndist/\n*.log\n')
        archive.writestr('MANIFEST.json', json.dumps(manifest, indent=2))
    print(f'{destination} ({destination.stat().st_size:,} bytes)')


if __name__ == '__main__':
    main()
