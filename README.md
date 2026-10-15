# Square Desk — complete repair v10

Start with [START_HERE.md](START_HERE.md). This is a complete source release for the **existing Git repository and existing Render service**. Keep current Render environment variables and secret files. No new account, database reset, cluster ID change or history export is needed for this repair.

The uploaded 15 October ZIP contained the correct v9 runtime files. This release repairs additional runtime problems: redundant database transactions, foreground replica catch-up, Telegram replies waiting on the operational loop, notification backlog delaying publication cycles, repeated scheduler writes, and cleanup/database shutdown ordering.

- Build: `pip install -r requirements-square.txt`
- Start: `python -m square_desk`
- Render health check: `/health`
- Fast Telegram observations: `/status`, `/publish_status`, `/runtime`
- Detailed database-backed reports: `/status full`, `/publish_status full`
- Authenticated fast HTTP observations: `/api/runtime`

Database revision: **2026-10-15-control-switching-10**. Neon/Aiven/Supabase remain PostgreSQL providers; Turso remains the durable mutation journal and writer coordinator. A verified current provider can take over. A stale or unverifiable provider cannot publish. Replica repair verifies and archives the target before replacing its app tables from a checkpoint; the active source is protected.

Publishing hours, gaps, caps, evidence checks, content generation, chart variations, the configured **25-image daily cap**, three-day cleanup and seven-day quality/receipt checks are retained. Scheduling limits are unchanged; actual delivery still depends on the host staying awake and providers accepting requests.

See [AUDIT_2026_10_15.md](AUDIT_2026_10_15.md) for findings and [VALIDATION.md](VALIDATION.md) for test evidence and limits. Older repair notes are historical references; follow START_HERE for this upgrade rather than repeating fresh-start or unlock instructions.

`render.yaml` is the canonical Free web service blueprint with external databases and no persistent disk. Existing manually configured services retain their dashboard settings. A Free Render instance can sleep after 15 minutes without inbound traffic; database fallback does not provide continuous compute. See [Render's Free instance limitations](https://render.com/docs/free).

Never commit real environment files, credentials, certificates or runtime data. The supplied `.env.square.example` is a template, not a replacement for existing Render secrets. `MANIFEST.json` lists SHA-256 digests for packaged source and documentation.
