# Manual publication audit v18

Revision: `2026-10-18-manual-audit-18`.

Scope: administrator photo/text intake, daily cap, source binding, duplicates,
queue/restart/lease recovery, upload/receipt handling, storage and cleanup,
and integration with the existing automatic publication flow.

## Reproduced and repaired cases

- Starting a new composition could reuse an older incomplete image. Rearming
  now cancels only that old waiting composition and clears its pairing reference.
- A null caption could become the literal text `None`. Missing/non-text captions
  remain empty and wait for explicit text.
- Photo intake and composition references, and cancellation/refunds, now commit
  together. A failed database transaction does not leave half-applied controls.
  A waiting image can be paired again by replying with an explicit manual command.
- A failed lease recovery could put the manual task to sleep without observing
  the returning lease. Recovery now retries while ownership is unverified.
  A retiring worker cannot rewrite a successor's media state.
- The shared platform ceiling now also covers startup before manual history has
  been observed. Automatic daily/hourly/gap calculations remain unchanged.
- Original saved text is checked before any HTTP request and immediately before
  content submission. Unexpected changes require a new explicit manual post.
- Duplicate receipts beyond the newest 1,000 records are still checked, including
  compressed history and compact receipt ledgers.
- Palette-based PNG transparency is preserved. Content-addressed files are
  written through atomic replacement; an existing identical chart file is never
  truncated. Failed replacements remove temporary files. Durable media integrity
  failures go to review/refund rather than repeating a download loop.
- Malformed success IDs/codes are uncertain, not confirmations or automatic
  account blocks. Actual account rejection/rate-limit codes retain shared gates.
  Invalid Retry-After values cannot produce non-finite database cooldown values.
- Absolute bounds cover a slow-trickle Telegram download and content submission.
  A timed-out/cancelled content submission remains non-replayable after restart
  or verified database switching. Completed uploads are reused after deferral.

The initial fault-injection selection reproduced 15 failing cases on v17, with
the verified-switching case already passing. That report is deliberately retained
as `Manual-v18-Before-Fixes.xml`; it describes the previous build, not this release.
Additional tests cover transactional rollback, explicit pairing recovery, rate
limit headers, shutdown and failed atomic file writes. Final passed-test counts
are recorded in `VALIDATION.md` and the external package verification report.

## Preserved behavior

Automatic target 50/day, hourly cap 3, gap 1200 seconds, publishing hours,
quality/evidence rules, regular queue and last-publication clock are unchanged.
User manual posts remain a separate 10/day reservation budget. They do not consume
the 25-chart generation budget. Automatic due posts and uploads have priority.
The same account's platform limit, pauses, restrictions and verified writer
authority remain shared. Manual content is not rewritten or passed to an AI.

The v16 database precision repair and all existing provider failover, fencing,
snapshot and journal code retain exact source bytes. The patch changes only six
runtime files relative to v16: four integrations and two new manual modules.

## Validation boundary

Tests use temporary databases, a real local SQL journal protocol/fault injection,
mocked Telegram/Square HTTP responses and actual scheduler/worker integration.
No live account, token, deploy, ENV setting or cloud database was changed.
These checks cannot guarantee a remote API or hosting provider will never fail.
An uncertain remote submission is held for inspection instead of being repeated.
