# Release validation

Final verification on 13 October 2026: **217 tests passed** from the complete deployment directory, using its own application modules and packaged test helpers. Runtime code retained the existing publishing windows/gaps/caps; test clocks were made deterministic across midnight instead of changing production scheduling.

The release contains complete application/configuration/deployment files, required scripts and self-contained regression tests. MANIFEST.json records SHA-256 for included source/doc files. The ZIP is checked for missing modules, unsafe paths, junk/cached/runtime files and real ENV files.

Verification includes three-day archival/purge with seven-day exact duplicate/report semantics, receipt/campaign/signal references, current budgets, Telegram deduplication, protected pending/uncertain work, approved PNG/snapshot protection, worker ownership/cancellation, asynchronous cleanup cadence, media metadata-only lookups, login 401 handling, compact/manual dashboard/full-detail responses, failover fencing, lost acknowledgements and existing content/media/scheduler behavior.

Tests and SQL/Turso faults are local simulations; PostgreSQL-specific RLS/advisory locks are modeled, not tested against cloud accounts. No real credentials, provider connection, cloud billing changes, Render deployment or Binance publication occurred during packaging. Real bandwidth/memory/latency must be measured in Render/provider dashboards after deployment.

Earlier synthetic experiment reduced expanded observation-write SQL lengths by 98.93% for 1,200 observations/50 saved drafts while retaining exact evidence. It excludes protocol overhead, replicas, AI/Telegram/images/web traffic and other services, so it is not a monthly billing forecast or a 5 GB guarantee.

Build: `pip install -r requirements-square.txt`

Start: `python -m square_desk`

Tests: `python -m pytest tests -q -p no:cacheprovider`

Optional source repackaging: `python scripts/package_square.py` (excludes actual credentials, .env.square and runtime data).
