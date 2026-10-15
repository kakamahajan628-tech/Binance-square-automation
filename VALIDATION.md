# Release validation

Release: **complete repair v10**, 15 October 2026. Revision: **2026-10-15-control-switching-10**.

Final verification: **364 tests passed in 33.08 seconds** from the complete deployment folder, including the regenerated manifest and actual-release HTTP/import smoke. Test results are recorded in the sibling `Complete-Repair-v10-Tests.xml`; file/ZIP integrity checks are recorded in `Complete-Repair-v10-Package-Verification.json`. Final documentation changes were followed by manifest regeneration and release smoke verification.

Runtime code retains existing publishing windows/gaps/caps, configured 25-image limit, content/chart quality checks and three-day cleanup. Existing test clocks are deterministic across midnight rather than changing production scheduling.

The release contains complete application/configuration/deployment files, required scripts and self-contained regression tests. MANIFEST.json records SHA-256 for included source/doc files. The ZIP is checked for missing modules, unsafe paths, junk/cached/runtime files and real ENV files.

Verification includes three-day archival/purge with seven-day exact duplicate/report semantics, receipt/campaign/signal references, current budgets, Telegram deduplication, protected pending/uncertain work, PNG/snapshot protection, worker ownership/cancellation, asynchronous cleanup cadence, media metadata-only lookups, Unicode login 401 handling, compact/manual dashboard/full-detail responses, failover fencing, lost acknowledgements and existing content/media/scheduler behavior.

New v10 scenarios cover setup-only provider switching before caller entry, one actual-shaped mocked Binance POST across primary failure or lost receipt-journal acknowledgement, source-offline verified checkpoint recovery, bounded multi-turn bootstrap, transient repair backoff beyond three attempts, failed capture cache invalidation, unpublished checkpoint reclamation, an independent Telegram thread responding while the operational thread is deliberately blocked, pending mutation ordering and offsets, journal owner/standby/unknown handling, 409/429 and webhook conflict handling, authenticated nonblocking runtime status, stale worker readiness, no-op scheduler writes, and cleanup/database close serialization. A native SQLite shutdown race observed in a full-suite run was fixed and given deterministic regression coverage.

Tests and SQL/Turso faults are local simulations; PostgreSQL RLS/advisory locks are modeled, not tested against a PostgreSQL server or cloud accounts. Driver protocol tests cover bound SQL, transaction setup, rollback, cursor streaming and secondary archive queries. No real credentials, provider connection, cloud billing changes, Render deployment, Telegram message or Binance publication occurred during packaging. Real bandwidth/memory/latency, credentials, roles, poolers and quotas require live verification.

Test runtime: Windows, Python 3.12, supplied requirements and pytest. One upstream Starlette/httpx TestClient deprecation warning is informational; it is not an application failure. This release does not claim zero cloud errors, uninterrupted Free-instance compute or a permanent 5 GB bandwidth guarantee.

Earlier synthetic experiment reduced expanded observation-write SQL lengths by 98.93% for 1,200 observations/50 saved drafts while retaining exact evidence. It excludes protocol overhead, replicas, AI/Telegram/images/web traffic and other services, so it is not a monthly billing forecast or a 5 GB guarantee.

Build: `pip install -r requirements-square.txt`

Start: `python -m square_desk`

Tests: `python -m pytest tests -q -p no:cacheprovider`

Optional source repackaging: `python scripts/package_square.py` (excludes actual credentials, .env.square and runtime data).
