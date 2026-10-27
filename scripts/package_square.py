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
    for name in ('README.md','EXACT_CHART_V22.md','REDEPLOY_V22.md','DISK_MEDIA_V21.md','REDEPLOY_V21.md','EXTREME_SAVER_V20.md','REDEPLOY_V20.md','STATUS_OBSERVATIONS_V19.md','MANUAL_POSTS_V18.md','MANUAL_AUDIT_V18.md','SUPABASE_PRECISION_V16.md','ALIGNED_CHARTS_V15.md','FLOW_AUDIT_V14.md','CHART_DEADLINE_V13.md','CHART_RECOVERY_V12.md','SNAPSHOT_HANDOFF_V11.md','START_HERE.md','AUDIT_2026_10_15.md','SETUP.md','ENV_VALUES.md','AUDIT_REVIEW.md','VALIDATION.md','SQUARE_DESK.md',
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
