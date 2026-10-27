# 17 October 2026 flow audit

## Scope and evidence

Inspected Telegram screenshots, the supplied `Binance-square-automation-main (2).zip`, verified v13 source, older Square Desk scan code, and market/AI/content/chart/scheduling/publication/storage/control paths. Compared runtime files and ran regression, integration and failover tests. Live Render logs, credentials, quota measurements and new publication receipts were not available. Screenshots prove a fresh-market-data timeout before AI; they do not establish which remote host timed out.

The uploaded ZIP matches 46 v13 runtime files byte-for-byte. `service.py` and `runtime_status.py` differ and `chart_worker.py` is absent. This comparison does not establish what is running in the cloud. The consolidated update includes the chart worker and all required runtime files.

## Confirmed findings

| Finding | Evidence / effect | Repair |
| --- | --- | --- |
| Paper tracking buffer never flushed | Actual scan leaves an invalidated signal `watching`; older scan wrote directly | Persist changed outcomes in batches of 64 with worker lease verification |
| Slow fetch locks unrelated market requests | Stalled mocked request prevents a fresh request from reaching its response handler | Serialize request starts only; retain 1.1-second spacing, max two in flight per host, two bounded six-second attempts and Retry-After holds |
| Provider health unnecessarily replicated | 25 symbols with two intervals can create 50 observational writes per pass | Bounded RAM for provider health in efficient mode; controls, leases, budgets and pinned evidence stay durable |
| Telegram newest-ten selection starves old replies | With 12 queued messages the oldest two are omitted | Query oldest due messages directly; persist retry due times |
| Failed job keeps saying processing | Mocked terminal failure emits progress during slow reply delivery | Stop progress before final delivery and check terminal job status |
| Independent generation gap loses preparation time | Full draft gap followed by scan/AI delay; behavior predates recent fallback changes | Prepare at most one fresh pending automatic draft within five minutes of next possible publication slot; actual publication gates unchanged |
| Serial scan and repeated planning reads | All symbols waited behind earlier network requests; policy/history reads repeated per candidate | Two bounded observation workers, cancelled/joined on cancellation; shared validated fetches; planning cache refreshed after awaited draft attempts |
| Recent cache can contain aged candles | Cache validation failed without trying fresh providers | Refresh aged observations; never extend observation timestamps |
| No useful scan diagnosis | Sparse posts previously required guessing among data/AI/queue issues | Compact status exposes phase, observations, unavailable count, candidates, drafts and last reason |

Four behavioral regressions were reproduced against unmodified v13 runtime, then passed with the repair: lost tracking persistence, blocked unrelated fetch, oldest replies starved, and terminal-job progress. Tracking persistence is distinct from new heuristic generation; it does not alone explain only three new posts.

## Preserved behavior

Analysis thresholds, indicators, universe size and rotation, scan interval, content styles/prompts, AI model/provider choices and budgets, numerical validation, duplicate protection, high-risk review, moderation handling, actual publication hours/caps/gap, exact chart evidence, chart variations and image budgets remain intact. Three-day cleanup and seven-day quality/receipt ledgers are retained. No reset, caller-write replay or bypass of journal fencing.

07:00–23:59 Asia/Kolkata includes the final hour before midnight. Screenshots' 00:18/00:42/04:37/06:04 paper signals occur outside the public posting window. Paper tracking can run overnight; a signal ID is not an approved draft. Quiet markets may produce no qualifying events. The daily target cannot guarantee a number of valid publications. The image cap counts generated/reserved charts, including unpublished drafts, rather than only confirmed image posts.

## Database and hosting status

Last supplied v13 status: Neon ready/current, Aiven three entries behind, Supabase held at sequence 33818 with `snapshot_restore_unverified`. No newer replica proof was supplied. The exact cause of that restore-verification failure is not established, and this release does not claim it is repaired. Verification remains strict and an unverified replica cannot take over. Retained tests cover switching, journal acknowledgement loss, no blind caller-write replay, uncertain publication receipts and checkpoint rollback/corruption rejection. Tests use local mock transports, not live cloud PostgreSQL.

The source ZIP predates the latest chart worker. Deploy all seven consolidated runtime files, or the complete source root, in one commit with existing ENV. No cloud credentials, database reset or paid hosting change was applied.

Render documents that Free services sleep after 15 minutes without inbound traffic. External API polling does not establish continuous inbound traffic. This is a possible additional cause of sparse overnight activity, not a verified diagnosis of this service. An HTTP liveness response does not prove uninterrupted scans or posting. See [Render Free limits](https://render.com/docs/free).

## Verification and limits

See VALIDATION.md and exported test reports. Mocked tests establish local transitions/safety, not live billable traffic or a percentage improvement. Avoiding 50 provider-health writes in a representative two-interval/25-symbol pass is an operation count, not a billing forecast.

After deployment `/status` exposes scan observations. `/image_status full` distinguishes attachment, upload readiness and failure stages. A confirmed Square receipt and publication notification establish public image delivery. AI 429/503, moderation, stale data, sleep and unavailable databases can still cause legitimate safe holds. No zero-error or fixed-output guarantee is made.

Transport references checked: [Binance official REST limits and Retry-After](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md), [Coinbase REST rate limits](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits). Existing conservative start spacing and restriction handling are retained.
