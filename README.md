# Current release: exact chart recovery v22

Read [EXACT_CHART_V22.md](EXACT_CHART_V22.md) and [REDEPLOY_V22.md](REDEPLOY_V22.md).
Revision 2026-10-26-exact-chart-recovery-22. Exact-hash recovery from saved current
evidence, pinned Pillow and explicit backup-verified legacy maintenance. Existing
AI writing, publishing schedule, quality and caps remain unchanged. PNG bytes
stay disk-only. Earlier release instructions below are historical.

---

# Current release: disk-only images v21

Revision: `2026-10-26-disk-media-21`. PNG bytes stay only on disk; DB and replicas
receive hash/size metadata. Read [DISK_MEDIA_V21.md](DISK_MEDIA_V21.md) for the
restart/redeploy durability tradeoff and [REDEPLOY_V21.md](REDEPLOY_V21.md) before
removing the old service. All v20 efficiency improvements are included.
Earlier release instructions below are historical; v21 replaces DB image recovery.

---

# Current release: extreme saver v20

Revision: `2026-10-26-extreme-saver-20`. Read [EXTREME_SAVER_V20.md](EXTREME_SAVER_V20.md) and
[REDEPLOY_V20.md](REDEPLOY_V20.md). Lossless bounded RAM caches and reduced
redundant outgoing traffic; existing automatic/manual publishing behavior retained.
The complete source package does not contain a backup of live data or private ENV.
Earlier release details below are historical and do not replace the v20 instructions.

---

# Current release: AI status latency repair v19

Revision: `2026-10-19-status-observations-19`. See [STATUS_OBSERVATIONS_V19.md](STATUS_OBSERVATIONS_V19.md)
for the reproduced 62-read report defect, two-file repair and RAM-only AI progress.
Automatic publishing, audited manual posts and database failover remain preserved.
Earlier release details follow as history.

---

# Current release: audited manual image posts v18

Revision: `2026-10-18-manual-audit-18`. Use [MANUAL_POSTS_V18.md](MANUAL_POSTS_V18.md) for the
additional 10/day manual lane and six-file deploy instructions. See
[MANUAL_AUDIT_V18.md](MANUAL_AUDIT_V18.md) for the fresh fault-injection audit.
The automatic posting flow, chart cap and v16 database precision repair remain
preserved. This release supersedes the initial v17 manual patch.

---

# Current release: separate manual image posts v17

Revision: `2026-10-18-manual-image-posts-17`. See [MANUAL_POSTS_V18.md](MANUAL_POSTS_V18.md) for the
additional 10/day manual lane, commands, six-file deployment and verification.
The automatic posting flow and chart budget are preserved; the v16 database
precision repair remains included. Earlier release notes follow as history.

---

# Current release: Supabase precision repair v16

Revision: `2026-10-17-replica-float-precision-16`. See [SUPABASE_PRECISION_V16.md](SUPABASE_PRECISION_V16.md) for the observed precision mismatch, two-file patch, test scope and live verification. All other v15 runtime behavior is preserved. Older release details below are retained as history.

# Square Desk — aligned charts and media recovery v15



Read START_HERE.md for the single existing-service update and ALIGNED_CHARTS_V15.md for implementation/validation details. This complete release retains v14 flow fixes and v13 render deadline correctness.



Charts follow saved post topics with the original exact evidence. Media preparation is a durable, resumable pre-publication state machine instead of a combined short timeout that demotes valid slow-processing images. A known file ticket is polled without repeating its PNG PUT or blindly replaying a content POST.



Build: `pip install -r requirements-square.txt`. Start: `python -m square_desk`. Health: `/health`. Running revision: **2026-10-17-aligned-media-recovery-15**.



Existing ENV, secrets, cluster identity, publication schedule/caps, AI validation, chart budget and cleanup remain in place. No database reset or new ENV is required. Runtime files are verified by MANIFEST.json. Historical notes are background; START_HERE.md is the current deployment guide.

