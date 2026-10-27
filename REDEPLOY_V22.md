# v22 deployment and recovery checks

Use the COMPLETE v22 source archive for a new web service, at repository root.
Build: pip install -r requirements-square.txt (includes Pillow==13.0.0).
Start: python -m square_desk. Health endpoint: /health. Keep one worker/instance.

Before removing an old web service save private ENV, source, verified database
backup, Turso resources and required local user PNGs outside Render. Reuse the
SAME PostgreSQL URLs, Turso journal/token, cluster ID, failover order, credentials,
policy settings, hours and caps. Do not bootstrap, reset, enroll or force a new
writer merely because the web service changed. Allow normal lease handoff.

After deployment /status should identify 2026-10-26-exact-chart-recovery-22 and a
responsive worker with a valid lease/current DB. Verify retained history and
ordinary automatic image publication with one confirmed Square receipt. Healthy
publishing and AI generation remain identical. Local chart recovery now works
only for exact saved still-current evidence and an identical approved image hash;
otherwise the individual draft needs review. No stale chart window is extended.

Render disk remains ephemeral. Chart recovery is not a backup for manual
photos/memes, campaign graphics, missing pinned evidence or arbitrary old files.
Keep needed exact PNG files externally. Automatic recovery of a lost local chart
does not re-approve an already reviewed/rejected/expired draft or replay uncertain
content. Do not repeatedly issue /post_now to test recovery on a live account.

The legacy cleanup command is optional, explicit and OFFLINE. Do not run it while
the publishing worker is active; choose outside posting hours. See EXACT_CHART_V22.md
for verified backup, dry-run plan, confirmation, protected-reference behavior,
partial-run restart semantics and historical journal compaction limitations.
No actual deploy, live backup, purge, remote webhook change or publication has
been performed in preparing this source release.
