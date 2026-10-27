# Deploy v22 exact chart recovery

Revision: 2026-10-26-exact-chart-recovery-22.
Missing automatic PNGs can recover only from exact fresh pinned evidence and
the identical approved hash. PNG bytes stay disk-only. Pillow is pinned to 13.0.0.
Read EXACT_CHART_V22.md and REDEPLOY_V22.md before replacing the old service.

For new services use the COMPLETE v22 source ZIP with existing private ENV and
external databases/Turso journal. Build pip install -r requirements-square.txt;
start python -m square_desk. No schema or ENV change is required.

On complete v21 copy all seven files from Square-Desk-v22-Only-Changes: four
replacements (including requirements) and three additions. Do not omit new modules.
Legacy image cleanup is optional offline maintenance and is never automatic.
