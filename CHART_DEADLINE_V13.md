# Chart deadline v13 — existing v12 service update

Revision: `2026-10-16-render-deadline-13`.

The latest live report showed `render_timeout`, a 15-second configured render deadline, budget 3/25 and a confirmed refund. The 29.33-second `seconds` field measured the whole attempt, including database planning/recovery; it did not measure render-thread execution alone. The report cannot establish how much time was actual rendering versus delayed event-loop delivery.

A timing bug was reproduced against the deployed v12 code: an already completed, validated chart was reported as a timeout when synchronous operational-loop work delayed delivery of its completion callback. The regression deliberately finishes work inside its original deadline, stalls the loop past that deadline, and then checks attachment. The unchanged v12 code fails that case.

## Three deployment files

Copy the following into the existing repository's `square_desk` directory:

```text
square_desk/service.py          REPLACE
square_desk/runtime_status.py   REPLACE
square_desk/chart_worker.py     ADD NEW
```

Keep the other v12 files already deployed, including `image_diagnostics.py`. Do not create a second nested `square_desk` directory. Commit these three files and deploy the latest commit on the same Render service.

Keep all existing ENV values and secret files. No dependency installation change, database reset, cluster change, budget reset or migration is required. The configured render timeout remains 15 seconds. Posting hours, gap, daily/hourly caps, the 25-image cap, AI/content/chart rules, fallback and cleanup remain unchanged.

## What changes

The render thread records its own start and finish times. A delayed asyncio timeout can accept an already completed result only if its actual completion met the original deadline. The code never waits synchronously for unfinished work and does not re-render or retry any submission to recover a callback.

A render that truly finishes after the deadline is rejected, even if its completion callback arrives before the overdue asyncio timer. Cancellation remains cancellation. Worker ownership and draft status are checked before attachment. Original evidence freshness is checked again before saving the attachment. Approval, scheduler and publication checks still run normally. Existing no-queue-after-timeout protection and conservative budget recovery are retained.

## Verify

1. `/status` should show revision `2026-10-16-render-deadline-13` after the deployment has loaded.
2. After the next new chart attempt, send `/image_status full`.
3. `last_render_timing` describes the most recent observed render. The saved render outcome also includes `render_timing` when available.

| Field | Meaning |
| --- | --- |
| `deadline_seconds` | Original configured deadline, unchanged. |
| `queue_wait_seconds` | Time before the render thread started. |
| `worker_seconds` | Elapsed time on the render thread, including file work and host scheduling. This is not a CPU utilization measurement. |
| `result_observed_after_seconds` | Time from submission until the operational loop handled the result/timeout. |
| `completion_delivery_delay_seconds` | Delay between completed work and loop handling, when completion was available by then. |
| `completed_within_deadline` | Whether actual completion met the original deadline; null while the work has not finished. |

Compare the draft IDs and timestamps: an older `last_failure` can remain visible after a newer successful attachment/upload. The default `/image_status` is memory-only and includes the last budget observation; `/image_status full` reads the current budget.

This fixes the reproduced v12 deadline misclassification. Real cloud CPU, private database latency and image API availability were not measured locally. A genuine slow render can still exceed the existing deadline; the new timing fields distinguish it from delayed completion delivery. Successful live chart publication still needs confirmation in the actual service.
