# AI status latency repair and RAM progress v19

Revision: `2026-10-19-status-observations-19`.

## Evidence and scope

The supplied 19 October status at 21:16 IST showed a responsive worker, a valid
lease, current Aiven primary, a confirmed publication at 20:55:14 IST, and a scan
started at 21:07:47 that was in draft generation for ZRO. The scan's total elapsed
time was 493 seconds; this includes data collection and tracking, not just AI.
Twenty-five observations were valid, with zero unavailable in that scan. The
earlier DATA errors were from 09:01-09:26 IST. The later `/ai_job` reply described
a manual command from 17 October at 07:09 IST, not the current worker generation.

Current root causes of all historical publication gaps are not established by
those snapshots. Neon and Supabase were behind the primary; Aiven was current.
This patch does not force promotion, alter replicas or bypass publication gates.

One definite reporting defect was reproduced locally: with the supplied layout
of three AI providers and 17 models, `/ai_status` issued 62 separate repository
SELECTs. These synchronous reads run on the operational worker, so slow database
responses can delay both the command and other worker tasks. The old method also
read OpenRouter model health a second time for a duplicate view of the same data.

## Repair

- Full AI status reads its state keys and two budget counters with two bounded
  SELECTs inside one existing verified read transaction. Compressed state values
  are decoded exactly as before. Jobs, model health, cooldowns, provider order,
  budgets and response fields are retained. No external API probe is performed.
- The normal compact `/status` now includes `ai.active_generation` and
  `ai.last_job` from RAM, without any additional database or network request.
- Active progress includes symbol, provider/model, total elapsed seconds,
  `progress_phase` and `progress_age_seconds`. Phases distinguish model selection,
  saving attempt metadata, generating, output validation, saving results and
  final cleanup. `generating` includes the provider wrapper's budget/pacing and
  HTTP work; it is not proof of a pure network delay.
- Observations are bounded, omit prompts/tokens/draft text, and do not authorize
  publication. A RAM result says whether its report was saved to the database.
  A cancelled waiting job cannot erase the active generation owner's progress.
- On restart RAM starts empty. `/ai_status` still reads stored history when the
  database is available. The compact report remains observational only.

AI prompts, models/order, request limits, generation deadlines, validators,
publication policy, scan/scheduling behavior, automatic charts, manual 10/day,
automatic daily/hourly/gap settings and all database failover code are unchanged.

## Deploy two files on v18

Replace only `square_desk/ai_router.py` and `square_desk/runtime_status.py` with
the files from `Square-Desk-v19-Only-Changes`, and deploy them together.
No ENV change, schema migration or manual-module replacement is required.
The full source package includes the existing audited v18 manual feature.

After deployment, send `/status` once. Confirm revision
`2026-10-19-status-observations-19` and inspect `ai.active_generation`,
`ai.last_job`, scan phase and last confirmed publication. The startup log still
contains an older fixed revision label inherited from the existing service file;
the compact `/status` revision identifies this patch.

Tests use local databases and mocked HTTP responses, including verified failover
and a deliberately stalled report write. No live deploy/publication was performed.
The repair removes redundant report reads and improves visibility; it cannot
guarantee remote provider availability or prove the cause of every earlier gap.
