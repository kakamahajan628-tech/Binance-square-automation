# Snapshot handoff repair v11

Revision: **2026-10-15-snapshot-handoff-11**.

The supplied live v10 reports confirm the new worker and Telegram control loop are responsive, Aiven is current and its local journal lease is valid. They also show Neon and Supabase held at sequence 33818 with `snapshot_fence_changed`; they are **not ready fallback providers**. The increasing journal sequence shows activity on the current provider, not proof of new published posts.

## Confirmed defect

In a rolling deploy, the new process can read the verified current database while the retiring process still owns the journal. v10's background replica repair attempted its fenced restore during this normal standby period. The correct rejection, `snapshot_fence_changed`, was then misclassified as a permanent verification failure. After foreground ownership acquisition succeeded, the secondary replicas remained quarantined until another restart.

A regression test run against the unchanged v10 package reproduced the exact `verification_failed / held / snapshot_fence_changed` state. The repaired code waits for locally valid journal writer authority before beginning repair. Temporary fence, source or target changes enter bounded retry backoff. Every retry rechecks ownership and checkpoint proofs; it does not bypass a fence, skip journal entries, promote a stale node or seize another writer's authority.

Tests also cover a real simulated owner change during target restoration: all target changes roll back and the repair waits while the new owner retains authority. The healthy active Aiven source remains protected.

## Existing v10 service: smallest update

The sibling `Snapshot-Handoff-v11-files` folder contains these three updated runtime files at their correct paths, plus matching release documentation, regression tests and the regenerated manifest:

```text
square_desk/database_failover.py
square_desk/runtime_status.py
square_desk/service.py
```

Copy all update-folder contents over the matching paths in the existing v10 repository, preserving the other files already there. Commit once, then **existing Render service -> Manual Deploy -> Deploy latest commit**. No new ENV variables, database credentials, cluster ID, fresh start, database deletion, lease expiry override or service replacement is required. The complete v11 ZIP/folder is also supplied if you prefer a full source update.

The next deploy creates fresh per-process replica state, so the v10 permanent in-memory hold is not carried forward. Standby now waits for normal writer handoff; it does not mark this wait as corruption. `/status` should show revision `2026-10-15-snapshot-handoff-11`. Backups may pass through `rebuild_waiting_for_writer`, `snapshot_rebuilding` and `catching_up`; only verified catch-up reaches `ready`. `rebuild_retry_wait` means a temporary repair problem is waiting for bounded retry rather than being permanently held. Cloud permissions, quotas and connectivity still affect whether a repair can finish.

## Posting in the supplied observations

`No approved drafts`, `eligible:0` and `scan_running:true` mean no approved draft was available at the time the scheduler last planned; a scan was in progress. They do not establish a global publication block or the reason generation has not yet supplied a draft. `last_confirmed:{}` is RAM since restart, not a historical count. Detailed generation/control diagnosis is available through `/ai_status`, `/errors`, `/status full` and `/queue` if the scan finishes without producing work.

This change preserves publishing hours, gaps, caps, AI/content/chart behavior, the 25-image daily cap and three-day cleanup. Local tests do not verify the user's live provider accounts. Free hosting sleep, unavailable journal authority and external moderation can still require holds; no zero-error or uptime guarantee is made.
