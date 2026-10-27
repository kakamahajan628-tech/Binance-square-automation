# Charts v12 — deploy on the existing service

Revision: `2026-10-16-chart-recovery-12`.

The existing logs confirm that chart preparation was skipped before successful text publication. Those old messages discarded the failure reason, so they cannot establish whether the live cause was quota, evidence, storage, a busy renderer or timeout.

Two defects were reproduced against v11 and repaired:

- If the database update that marks a draft `chart_rendering` failed after reserving its image budget, the reservation was not released. The failed-start recovery now includes this update and atomically checks ownership, restores the draft and refunds a confirmed unused reservation.
- A timed-out asyncio wrapper left its underlying render thread running. Subsequent requests could queue behind it and time out again. The underlying thread future is now tracked; further work is refused immediately until it finishes. Late output cannot attach to a draft.

Font objects are reused within each render, reducing repeated font decoding. All five chart views produce exactly the same PNG bytes as the previous rendering method for the same inputs. No chart size, indicators, evidence checks or image limits changed.

## Replace/add these files

Copy the six files from `Deploy-Only-Charts-v12/square_desk` into the repository's existing `square_desk` directory. Replace the five matching names; add the new `image_diagnostics.py`. Do not put a second `square_desk` directory inside it.

```text
square_desk/service.py
square_desk/images.py
square_desk/image_diagnostics.py       NEW
square_desk/telegram.py
square_desk/telegram_control.py
square_desk/runtime_status.py
```

Commit once and deploy the latest commit on the existing Render service. Keep the current ENV, secret files, cluster ID, databases and Turso journal. No installation or new dependency is required. The posting schedule, AI/content rules, fallback implementation and three-day cleanup remain unchanged. The configured image cap remains 25; it is a cap, not a guarantee of 25 successful image publications.

## Verify one new draft

1. `/status` should show revision `2026-10-16-chart-recovery-12` and a held worker lease when the handoff finishes.
2. After a new automatic chart attempt, send `/image_status`. This small report uses memory and can reply while the database loop is busy. After a restart, unobserved values are explicitly unknown. Its budget is the last observation from `/image_status full`, with an observation time; no budget SELECT is added to every render attempt.
3. `/image_status full` reads the current daily budget and last saved outcome from the database; it may wait if the database is slow. Budget usage includes generated/reserved charts on unpublished drafts, not just posts visible in Creator Center. This update does not reset historical counters.

The report includes `last_attachment`, `last_upload`, `last_failure`, their times and draft IDs. A past `last_failure` may remain after a later success; compare the draft ID and time. `seconds` measures the attempt including planning/database work, not just Pillow CPU time.

| Code | Meaning |
| --- | --- |
| `attached` | Validated PNG attached; upload/publication checks still pending. |
| `upload_prepared` | Square image upload ready; content still awaits the scheduler and publication checks. |
| `budget_exhausted` | Configured daily chart budget exhausted, including unpublished work. |
| `queue_full` | Pending image uploads reached the existing queue limit. |
| `render_timeout` | Configured render deadline expired; late output cannot attach. |
| `renderer_busy` | Prior render still running; no additional render enqueued. |
| `evidence_expired` / `evidence_missing` | Original closed-candle evidence is stale or missing; a fresh draft is needed. |
| `render_invalid` | Renderer rejected chart inputs. |
| `png_invalid` / `storage_io_error` | PNG validation or local storage failed. |
| `worker_lease_lost` / `database_unavailable` | Ownership or database confirmation was lost; safety checks remain active. |
| `upload_not_prepared` / `upload_failed` | See IMAGE audit for the upload rejection/error. |

Each attempt also emits a redacted `IMAGE_RESULT` line in Render logs, even if saving database diagnostics fails. `/errors` records the specific chart failure instead of the old generic message. Successful attachment/upload retain their IMAGE audit messages. A confirmed image publication adds `Chart: sent with this post; Square receipt confirmed.` to the existing publication notification. Telegram delivery still depends on Telegram availability.

When a confirmed unused reservation cannot be safely recovered because ownership or database confirmation is lost, it is retained conservatively and `budget_refunded` is false. No automatic mutation/POST replay, approval bypass, chart removal or budget reset was added. Text publication can continue when no chart was created; an already attached image is not silently stripped.

Tests use real Pillow rendering and mocked upload/content responses. They do not establish live Render performance, Square image acceptance, provider quotas or private database connectivity. The next new live attempt is required to identify the cause hidden by the old generic logs.
