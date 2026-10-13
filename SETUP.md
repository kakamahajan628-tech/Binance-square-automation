# Fresh start on the same Render service, Neon preferred

Bhai, **Render service delete mat karo**. Existing GitHub repository, service URL, Telegram/Square/AI secrets and the rest of Render ENV can stay. Upload only this folder's 14 files, then replace/add the storage connection variables below. No old Neon export is required for this expressly authorized fresh start.

Old Binance posts stay on Binance. Old queues/signals/receipts/settings overrides are not imported or replayed. Duplicate history and local daily counters begin again: the app cannot compare a new draft with unavailable old content. Binance's actual account limits/restrictions still apply. Nothing in this package deletes the old Neon project, old records or snapshots.

## What the screenshots mean

- Neon: the old project used its 5 GB monthly **public network transfer**, not 5 GB database storage. Keep it separate from the new cluster.
- Render: the email shows 70% of the workspace's 5 GB monthly **outbound bandwidth**. Deleting/recreating one service does not reset a workspace-wide allowance. External DB/API requests, uploads and browser responses can use this bandwidth; database fallback alone cannot fix it.
- The old 3.5 GB approximately used is not refunded by this update. Confirm today's actual usage and renewal date in Render Billing/Metrics.

Sources: [Render bandwidth rules](https://render.com/docs/outbound-bandwidth), [Render Free limits](https://render.com/docs/free), [Neon quotas](https://github.com/neondatabase/website/blob/main/content/faqs/free-plan-limits-and-quotas.md).

**“Forever below 5 GB with no posting interruptions” cannot be guaranteed on these Free services.** Traffic, provider quotas, TLS overhead, images, other services in the same workspace and future plan changes matter. This update removes unnecessary bulk transfer and adds measurements; it does not reset quotas, stop scheduled publishing to enforce a byte cap, or claim exact billing counters. Render also reserves the right to suspend unusually high service-initiated traffic separately from the 5 GB allowance. If that happens, provider support or a suitable hosting plan is necessary; rotating databases does not remove that hosting restriction.

## Integrated architecture

| Service | Role |
|---|---|
| New Neon PostgreSQL | Preferred primary on startup |
| Aiven Free PostgreSQL | First standby/fallback |
| Supabase Free PostgreSQL | Second standby/fallback |
| Dedicated Turso **libSQL** database | Shared mutation journal and writer fencing authority |

Use independent PostgreSQL databases, not multiple aliases for one database. At least two PostgreSQL destinations must be reachable/empty for initial setup. Configure all three from the start for the full requested setup. A failed preferred provider is skipped only after a verified replica catches up. A working fallback is retained; recovered Neon catches up, and a normal restart can select the preferred current Neon again. No competing direct writers.

Turso is **not** selected as a PostgreSQL app-table replacement. If Turso is down, all mutations/publication stop. This implementation does not provide automatic failover of its coordinator. Cockroach was excluded because its current new-deployment offer is a trial and its locking/SQL assumptions need a separate adapter.

## What reduces transfer

1. Live public candle caches and the latest market list stay in a bounded RAM cache. Original observation/candle timestamps and existing freshness checks stay intact. After restart these observations are fetched again.
2. Exact candle evidence for every saved market draft/signal is pinned durably in the same transaction as that entity. Drafts, signals, publication receipts, stop/block controls, budgets, Telegram deduplication and approved PNG bytes remain durable and replicated. Stored draft evidence remains available for charts after restart.
3. Repeated draft/signal history reads reuse bounded process caches while the same fenced writer owns authority. Entity writes refresh only the changed row. Critical controls/budgets and transactional publication reads always go to the real repository. Provider/fencing changes and uncertain outcomes invalidate caches.
4. Large durable state JSON is losslessly compressed; previous plain JSON remains readable. No AI prompt/content or numerical evidence is shortened.
5. Replica PostgreSQL connections are reused. Only committed incremental changes are copied; no full database dump per scan.
6. The journal trims older entries only after **every configured node** has applied them, retaining at least 1024 entries. While a node is offline its missing changes stay retained. The runtime journal cap is 1.024 GB; if a prolonged outage fills it, writes stop safely rather than lose recoverable history. A new blank replacement cannot catch up through already-trimmed history; a current verified checkpoint would then be needed. Registered existing replicas are preserved on redeploy.
7. Dashboard uses **manual refresh only**. Its overview sends short post previews; full text loads through Read full post. Responses are compressed, market rows contain only displayed fields, and overview lists show the latest 20 drafts/10 signals/5 campaigns/10 logs. Dashboard reports cache for one hour; explicit Telegram `/report` and weekly reports retain their existing behavior.
8. Background master cleanup runs every **3 days**. It compresses confirmed post/signal payloads after 3 days, retains full semantics for the existing 7-day duplicate/weekly window, then keeps compact publication receipts/campaign/signal references while purging old bulky data. Active work, uncertain submissions, current budgets and command deduplication remain protected. See AUDIT_REVIEW.md in the complete ZIP.

The scheduler, scan interval, publishing hours, gap, daily/hourly caps, content mix, AI model order, approval requirements and signal rules are preserved from your supplied ZIP. **Manual UI refresh does not change the posting interval.** The dropped AI-illustration feature is not added. One bounded replica thread is used; true Render memory/latency and cloud quota behavior have not been measured.

## 1. Prepare the new services

### Neon

Open [Neon Console](https://console.neon.tech/) and create a new clean project/database. A new project on the same account can have its own project allowances; changing Gmail is not required just to create a fresh project. Use the account/project options permitted by the provider; no account-rotation/limit-bypass automation is included.

Copy the new project's **pooled PostgreSQL connection URI** privately. Keep `sslmode=require` or stronger and any genuine channel-binding parameter. Do not use the old quota-paused URI. [Neon project quotas](https://github.com/neondatabase/website/blob/main/content/faqs/free-plan-limits-and-quotas.md).

### Aiven

Keep/create a **Free PostgreSQL** service with empty app tables. An earlier empty guarded Aiven setup is allowed; a destination with real entities/audit/budgets is refused.

Download its CA certificate. In the existing Render service, Environment → Secret Files, use filename `aiven-ca.pem`, contents = the complete certificate text. Aiven URI:

```text
postgresql://avnadmin:YOUR_ENCODED_PASSWORD@YOUR_HOST.aivencloud.com:YOUR_PORT/defaultdb?sslmode=verify-full&sslrootcert=/etc/secrets/aiven-ca.pem
```

Use the actual copied URI's values, one sslmode only. The certificate Secret File remains if it was already added. [Aiven TLS](https://aiven.io/docs/platform/concepts/tls-ssl-certificates).

### Supabase

Create a new Free project. **Connect → Session pooler** provides the IPv4-compatible PostgreSQL URI. Use its actual username, host, password and port (normally 5432), database `postgres`, with `sslmode=require` or your verified TLS setup.

```text
postgresql://postgres.YOUR_PROJECT:YOUR_ENCODED_PASSWORD@YOUR_POOLER_HOST:5432/postgres?sslmode=require
```

This is not the Supabase HTTPS project URL/anon key. Use the database owner connection; bootstrap enables row-level security on the app tables so anonymous Data API access does not expose them. [Supabase connection guide](https://supabase.com/docs/guides/database/connecting-to-postgres).

### Turso

Create a **new dedicated libSQL-engine database**, copy its `libsql://...turso.io` URL and create a **read/write database token**. Do not use a management token or a journal from another cluster. This adapter targets libSQL `/v2/pipeline`, not an unverified different Turso engine/API. [Turso HTTP setup](https://docs.turso.tech/sdk/http/quickstart).

## 2. Upload these 14 replacement files to the EXISTING GitHub repo

Copy the `square_desk` folder's files into the same `square_desk` paths, in one complete commit:

```text
square_desk/store.py
square_desk/postgres.py
square_desk/database_failover.py
square_desk/turso_journal.py
square_desk/fresh_cluster.py
square_desk/efficient_storage.py
square_desk/bandwidth.py
square_desk/app.py
square_desk/dashboard.js
square_desk/service.py
square_desk/publisher.py
square_desk/config.py
square_desk/media.py
square_desk/retention.py
```

Other repository files remain in place, including `migration.py`, `database_transfer.py`, `planning.py`, `config.py`, scheduler/content files and requirements. This folder was built against your `Binance-square-automation-main (2).zip`, not an older AI-illustration bundle. Do not upload the old Aiven import/fallback folders over this one.

Deploy this complete commit first with current ENV if preparing in stages. Before the actual fresh cutover, stop/suspend the old service worker or hold it with its existing emergency stop. Do not run the old and new clusters as separate publishing services. Keep the service/repository/ENV; no delete/new repository is needed.

## 3. Change only storage ENV on the SAME Render service

Save these values privately in Render → Environment. The accompanying `ENV_VALUES.md` provides individual copy boxes.

| Name | Value |
|---|---|
| `DESK_DATABASE_URL` | New Neon pooled PostgreSQL URI |
| `DESK_NEON_DATABASE_URL` | Same new Neon URI |
| `DESK_AIVEN_DATABASE_URL` | Aiven URI with Render CA path |
| `DESK_SUPABASE_DATABASE_URL` | New Supabase Session-pooler URI |
| `DESK_TURSO_JOURNAL_URL` | New dedicated libSQL database URL |
| `DESK_TURSO_JOURNAL_TOKEN` | Its read/write database token |
| `DESK_DB_CLUSTER_ID` | `square-fresh-20261012` |
| `DESK_DB_FALLBACK_ORDER` | `neon,aiven,supabase` |
| `DESK_DB_FALLBACK_ENABLED` | `true` |
| `DESK_DB_FRESH_START` | `CONFIRM_EMPTY_NEW_CLUSTER` |
| `DESK_STORAGE_EFFICIENT` | `true` |

Keep all Telegram/Square/text-AI secrets, timezone, policy review and existing feature settings. `DESK_DATABASE_URL` takes precedence over `DATABASE_URL`; do not leave it pointing at old Aiven/Neon. A stale dedicated `DESK_NEON_DATABASE_URL` would override the new primary member, so set both Neon variables explicitly.

**Important schedule check before fresh start:** settings changed only through Telegram were stored in the old database and cannot magically survive without importing it. Put the intended current schedule into the existing Render ENV. The last schedule you supplied was:

| Existing setting | Last supplied value |
|---|---|
| `DESK_DAILY_TARGET` | `50` |
| `DESK_HOURLY_CAP` | `3` |
| `DESK_GAP_SECONDS` | `1200` |
| `DESK_PUBLISHING_HOURS` | `7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23` |
| `DESK_SCAN_SECONDS` | `300` |
| `DESK_MODE` | `automatic` |
| `DESK_AI_AUTO_PUBLISH` | `true` |
| `DESK_NEON_BATCH_SECONDS` | `0` for the current normal-response flow |

If your actual current schedule differs, retain those actual values instead. This patch does not change timing defaults or force these values in code. Existing quality checks/platform caps/provider availability still control whether a slot produces a publication; a daily target is not a guarantee of that many eligible posts.

Use **Save, rebuild, and deploy** / **Manual Deploy → Deploy latest commit** with all 14 files uploaded. Ensure any suspended service is resumed onto this complete build, not an older build connected to the new databases. No local database-transfer commands are needed for this fresh-start route.

## 4. First startup and one unlock

The first startup creates only empty app schemas, one stable fresh-start seed and replica registrations. New instances/redeploys do not reset a started cluster. It refuses to erase existing application history or overwrite another cluster. At least two destinations must be reachable; unsafe configuration/partial setup holds publishing.

Emergency stop is deliberately ON at first startup. Once logs show the new service live **and the old worker has finished/stopped**, send:

```text
/status
```

Expected:

```text
database: ok
publication.database_failover.enabled: true
publication.database_failover.active_provider: neon
publication.database_migration.origin: authorized_fresh_start
publication.database_migration.history_preserved: false
publication.storage_efficiency.enabled: true
emergency_stop: true
```

If Neon is unavailable a verified available fallback label is expected instead; diagnose its URI/access before calling it the primary. Telegram replies should work after startup because this is an authorized initialized fresh cluster, not the earlier unverified import standby.

Confirm `/settings` shows your intended schedule and automatic mode, then:

```text
/unlock CONFIRM
```

The existing automatic loop continues from newly generated, fresh validated content. `/resume` is needed only if a pause is intentionally still enabled. No old queue is replayed. Retain the same Telegram bot, dashboard login and UptimeRobot monitor; set the monitor URL to the lightweight `/health` endpoint (GET or HEAD), not the authenticated dashboard/overview.

## Bandwidth verification

The private dashboard endpoint `/api/bandwidth` shows byte-length observations for HTTP/Turso/PostgreSQL requests, PostgreSQL responses and compressed web responses, plus avoided reads/writes. Login is the existing `desk` username and admin token. These counters reset on process restart and exclude part of protocol overhead/provider accounting. They are **not** your remaining Render/Neon allowance.

Check Render **Metrics → Outbound Bandwidth** and workspace Billing after real operation; compare browser-response vs service-initiated traffic. Check each database's transfer/storage/compute usage. Manual dashboard refresh and RAM observations do not reduce publishing frequency. No automatic halt of legitimate scheduled posting is used as a fake bandwidth guarantee.

No cloud accounts, deployments, billing or real publications were modified by Codex. Local mock/in-memory checks are documented in `VALIDATION.md`; live quota/memory/latency still need observation after your deployment.
