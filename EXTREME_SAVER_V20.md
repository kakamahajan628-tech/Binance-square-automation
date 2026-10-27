# Extreme RAM and outbound savings v20

Revision: `2026-10-26-extreme-saver-20`. Based on the complete audited v19 release.

## What changes

RAM observations and history now retain losslessly compressed JSON bytes instead
of full nested Python objects. Only requested rows are decoded for callers.
The observation cache has an 8 MB charged budget and the history cache 4 MB,
including a per-entry allowance. These are cache accounting limits, not a whole
process RSS ceiling. Chart rendering, HTTP requests, database libraries, decoded
results and initial history loading still need additional transient/native RAM.
Observation entries remain bounded at 2048 per collection; history at 10000 per
kind. A full history cache falls back to the database. Cache eviction never
deletes durable data. Missing exact chart evidence still prevents draft creation
until it can be regenerated; it is never substituted with unrelated candles.

The Turso journal no longer binds the full encoded transaction twice just to
check capacity. It sends the exact encoded payload once and its ASCII length as
an integer. Stored journal contents, hashes, sequences, byte-capacity enforcement
and writer fencing are unchanged. This removes a duplicate request parameter,
not an existing duplicate journal record.

Empty Telegram polling uses a 20 second server long-poll, with a bounded 25
second read deadline and 4 second connect/write/pool deadlines. Telegram returns
immediately when a command arrives. Pending jobs/replies use the short path.
Authority is rechecked after the poll before processing or acknowledging updates.
The independent Telegram client now contributes to the outgoing payload counter.

Completed cycle timestamps stay in RAM instead of generating a replicated write
every idle cycle. A restart reports unknown until the next completed cycle.
An empty image upload queue no longer causes four extra database control reads;
real candidates still pass every control and publication check.
Direct signal tracking avoids writes when neither status nor payload changed.
Correction to the early bandwidth audit: automatic scans already use PlanningStore
to skip unchanged tracking writes; this additional saving chiefly affects direct
tracking callers. It must not be counted twice for the automatic scan loop.
Startup logs now print the actual revision instead of the old fixed v13 label.

## Measurements and limits

Local, isolated comparison: 100 assets with 200 candles in both observation caches,
plus 1500 published drafts in history. Retained Python allocations measured by
tracemalloc fell from 20,154,063 to 2,825,973 bytes (about 86%). Peak traced
allocations in that fixture fell from 26,948,605 to 7,214,356 bytes. These are
fixture allocations, not production service RAM or a memory-limit guarantee.
All 200 observation entries and 1500 history entries remained available exactly.

The separate mocked network model includes idle cycles, lease renewals, empty
Telegram polling and illustrative image persistence/replay/upload. Its selected
31-day components dropped from about 6.95 GB to 3.43 GB when including 25 sample
83 KB automatic charts and ten roughly 901 KB manual PNGs per day. These are
assumptions, not a complete monthly forecast. Active scanning, AI, real publishing
transactions, retries, failover, snapshots and network protocol overhead are not
fully represented. Large manual images alone cost about 1.78 GB in this model.
Inbound external database response bodies must not be counted as Render outbound.

The 5 GB Hobby allowance is per workspace/month. Recreating a service in the same
workspace does not reset used bandwidth. Optimization cannot resume a workspace
already suspended for exhausted bandwidth. Official reference:
https://render.com/docs/outbound-bandwidth

After deployment use Render's actual outbound graph for 24-48 hours. A practical
planning target is at most about 120-130 MB/day for the entire workspace, allowing
roughly 1 GB of a 5 GB month for variation. A sustained rate above this needs a
new plan based on measured traffic; this release does not promise a free full month.
Do not reduce chart quality or publishing caps as a hidden bandwidth workaround.

## Preserved behavior

Automatic target 50/day, hourly cap 3, gap 1200 seconds, configured posting hours,
manual extra cap 10/day, chart budget 25/day, image resolution/quality, rendering
deadline, automatic/manual publication validators and uncertain-submission rules
are unchanged. Targets are ceilings/goals, not a guarantee of 50+10 posts daily.
Critical controls, worker leases, budgets and transactional reads remain durable
and are never authorized from a RAM cache. No schema, data reset, credential,
provider ordering or cloud setting is changed. All unlisted runtime and config
files match the complete v19 baseline byte for byte.

## Files

Replace six files together on a complete v19 checkout:

- square_desk/efficient_storage.py
- square_desk/turso_journal.py
- square_desk/tracking.py
- square_desk/telegram_control.py
- square_desk/runtime_status.py
- square_desk/service.py

No new runtime module. For a new web service use the complete v20 source archive,
not just the six replacements. DESK_STORAGE_EFFICIENT must be true (the existing
PostgreSQL default is true). All current private ENV values must be preserved.
See REDEPLOY_V20.md before removing any old service.
