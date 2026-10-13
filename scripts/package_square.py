"""Package this source with no real credentials, runtime data or cached bytecode."""
from pathlib import Path
import zipfile
import hashlib
import json

def main():
    root=Path(__file__).resolve().parents[1]
    files=[]
    for folder in ('square_desk','scripts','tests'):
        for path in (root/folder).glob('*'):
            if not path.is_symlink() and path.is_file() and path.suffix in ('.py','.js','.html'):
                files.append(path)
    for name in ('README.md','SETUP.md','ENV_VALUES.md','AUDIT_REVIEW.md','VALIDATION.md','SQUARE_DESK.md',
                 'requirements-square.txt','requirements-square-dev.txt','render.yaml','.python-version','.gitignore','.env.square.example'):
        if (root/name).is_file(): files.append(root/name)
    output=root/'dist/square-desk-ready.zip'
    output.parent.mkdir(exist_ok=True)
    manifest={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as archive:
        for path in files: archive.write(path,path.relative_to(root).as_posix())
        archive.writestr('MANIFEST.json',json.dumps(manifest,indent=2))
    print(output)

if __name__=='__main__': main()
