# Audit assessment and implemented changes

This assessment uses the supplied code/ZIP and local tests, not live Render/Neon traffic. The latest supplied ZIP was used as the complete source baseline; the older 10 October audit is checked against that baseline rather than applied blindly.

| Finding | Decision and implementation |
|---|---|
| Repeated full post-history reads | Relevant. Bounded fenced-writer history caches update individual rows on mutations; transactional publication reads remain real. Queue counts use COUNT rather than full payloads. Seven-day duplicate/history limits already existed in several checks; they remain unchanged. |
| Never cleaning old post/signal/command data | Relevant. Added background master cleanup every 3 days with protected active work and bounded batches. Full confirmed payloads compress after 3 days; after the existing 7-day quality/report window, raw post data becomes small nonreplayable receipt/campaign/signal ledgers. Old terminal signals, expired drafts, unused snapshots/media, old reports, delivered outbox/log/budget data are purged. Terminal command payloads compact while their Telegram fingerprints remain. |
| Temporary market caches rewritten | Relevant; latest storage update included. Public observations stay bounded in RAM; exact draft/signal evidence is pinned durably. Original timestamps and all freshness/numeric checks are retained. |
| Dashboard downloads too much | Relevant. Manual refresh only; compact overview; full post text loads on click. Hourly cached dashboard reports and compressed HTTP responses. Bot scan/publish timings do not depend on UI refresh. |
| Whole PNG downloaded for existence/hash check | Relevant. Added database-side hash/size metadata query. Keep exact approved PNG bytes durable: Free Render local disk is ephemeral, so fingerprint-only local storage would lose images on restart/failover. Blob retrieval occurs when exact bytes actually need restoration. |
| Active signals repeatedly read per coin | Relevant. One PlanningStore pass per scan tracks active signals and updates; underlying simulation/stop-first/fee rules stay identical. |
| Reduce AI retries | Not applied. Existing retries/timeouts/global budgets remain bounded. Changing them without measured savings would reduce recovery/quality. Byte observations are available for later measurement. |
| Educational daily maximum is 3 | Incorrect interpretation of the failing test. Its assertion counts available distinct lesson templates, not actual daily published posts. The old test assumed a 3-lesson library; the library now contains 11. Test updated to the real library, without an invented new publishing cap. Existing fallback flow remains at most one reviewed lesson per day without AI and normal publication caps still apply. |
| Paid disk/Neon YAML mismatch | Relevant. Canonical render.yaml uses Free web compute, external DB connection placeholders, correct build/start/health commands and no disk. No forced mode/schedule/secrets regeneration. Existing manually configured Render ENV is retained. |
| Duplicate render YAML | Removed from the full ZIP; render.yaml is canonical. Repackaging script no longer generates the duplicate. Delete an old render-square.yaml from an existing Git repo if desired; manual service deployment does not use it. |
| Non-ASCII login name raises 500 | Hardened username comparison with UTF-8 bytes; wrong/non-ASCII usernames return 401. Existing Basic Auth libraries may already reject such wire input; direct dependency tests cover the previously unsafe comparison. |
| Stray k files | Excluded from square_desk/scripts/tests in the cleaned ZIP. No credentials/runtime caches included. |

## Audit recommendations intentionally rejected

- Setting batching to 1800/3600 would delay the existing checks/publication flow and may expire evidence. Keep the current zero-batch flow if that is your configured schedule; actual repeated transfer is fixed rather than introducing a long sleep.
- Moving production data/images to Free Render disk is not durable. Paid disks require paid compute; the previous YAML did not make one available on a manually configured Free service.
- Adding a card is not a no-cost guarantee. No billing changes are performed/recommended as part of this patch.
- The emails alone do not prove every byte came from one cause. Render bandwidth is workspace-wide and includes API/uploads/browser traffic; no exact cloud savings are claimed.

Sources: [Render Free limitations](https://render.com/docs/free), [Render outbound bandwidth](https://render.com/docs/outbound-bandwidth), [Neon Free quotas](https://github.com/neondatabase/website/blob/main/content/faqs/free-plan-limits-and-quotas.md).

## Three-day cleanup boundary

Three days is the cleanup cadence and raw archival age, not a permission to destroy pending approvals or uncertain receipts. The original seven-day content-similarity/title/event checks and weekly reports require recent history. Compressing that history retains exactly the authored text and data; deleting it after only three days would compromise the user's stated quality requirement.

Compact publication ledgers retain IDs, original timestamps/status/fingerprint, receipt, campaign usage and linked signal/event identifiers. They are not scheduled drafts and cannot automatically replay an accepted/uncertain submission. They are intentionally small, not an unlimited free-storage claim. Active/uncertain records may outlive three days and need operator resolution.

Cleanup is one background worker, at most 16 entities per transaction, yielding between batches. It checks writer ownership before mutations, preserves timestamps and does not fetch media blobs to delete old media metadata. Generated local artifacts are removed only by allowed app filename patterns; symlinks, unrelated files and active image references are protected.

Fresh Neon primary + Aiven/Supabase fallback + mandatory Turso authority, connection reuse, safe journal trimming and explicit empty-cluster initialization are included. Existing populated stores are never cleared by the fresh initializer. Old Binance posts remain on Binance; no old export is required for the authorized fresh start.

Provider quotas, coordinator outages, sustained traffic and eventual compact-ledger growth still mean no promise of free service/bandwidth forever. Render's already-used workspace allowance is not reset by deleting/redeploying a service.
