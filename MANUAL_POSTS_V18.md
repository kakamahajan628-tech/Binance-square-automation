# Manual image + text posts v18

Revision: `2026-10-18-manual-audit-18`.

## What is added

An administrator can submit one supplied image with their own text directly from
their existing private Telegram bot chat. This is a separate manual lane with a
fixed 10-per-day cap in the configured timezone (currently Asia/Kolkata).
There is no AI rewrite, new image generation or text overlay on the picture.
Only surrounding whitespace and line endings are cleaned; paragraphs are kept.

The existing automatic drafts, quality checks, daily target 50, hourly cap 3,
1200-second gap, publishing hours and chart budget 25 remain separate. Manual
posts do not change their counters, slots, evidence checks or last-publication
timestamp. Automatic due posts and in-progress automatic uploads take priority.
The existing database failover, writer fencing and v16 float-precision repair
are preserved. No new ENV variables or database migration are needed.

Both lanes use the same Square account: platform-wide daily limits, provider
restrictions, pause/emergency stop, policy review and verified database ownership
remain shared. Extra posts cannot bypass an account-wide restriction. The 10
manual slots are additional to the automatic target, not a guaranteed number of
successful posts. Unused slots do not create posts automatically.

## Send a post

1. Send `/manual` in your private bot chat.
2. Send ONE image with its caption. To use a longer caption, send the image first,
   then send its text as the next message within 15 minutes.
3. The completed image + text pair authorizes direct publication. No extra approve
   command is required. An accepted queue item is not yet a confirmed publication;
   wait for `Manual publication ... published with your image. Square receipt confirmed.`

You can repeat `/manual` for each of up to 10 daily posts. Alternatively put
`/manual Your exact caption` in a photo caption, or reply to an image YOU sent
with `/manual Your exact caption`. An ordinary, unarmed photo/screenshot is never
published automatically. Albums, videos and animations are not supported.

Manual text may be short (e.g. a meme); the automatic 200-word minimum is not
applied to supplied content. Existing prohibited-promotion wording checks and the
configured Square short-post character limit still apply. Standard Telegram
captions have a smaller limit, so use image-then-text for a long post. Text stays
as supplied; no extra ticker, claim, title or disclaimer is invented.

Static JPG, PNG and WebP are supported: input at most 8 MB, maximum side 4096 px
and 8 megapixels. For the existing image-upload API the bot converts a copy to PNG
within 1 MB, preserves aspect ratio and scales it down when necessary. It does
not crop, redraw or place caption text inside the picture. Image metadata is
removed from the converted copy. Your source picture is not edited in place.

## Commands

| Command | Use |
| --- | --- |
| `/manual` | Arm one image + text composition for 15 minutes. |
| `/manual_status` | Separate daily used/reserved count and manual pending state. |
| `/manual_queue` | Manual post IDs and states; regular `/queue` remains unchanged. |
| `/manual_preview ID` | Read the saved text and state for your manual post. |
| `/manual_cancel` | Cancel the current composition or its queued pending item. |
| `/manual_cancel ID` | Cancel your waiting/queued/review item; refund its reservation. |
| `/manual_retry ID` | Explicitly retry your known-not-submitted review item. |
| `/manual_reconcile ID posted CONFIRM` | After inspecting Square, mark an uncertain post as published. |
| `/manual_reconcile ID not_posted CONFIRM` | After inspecting Square, mark it not posted; then retry explicitly if desired. |

The daily count includes accepted reservations, confirmed posts and uncertain
submissions. A known failed or cancelled post refunds its manual reservation.
The cap resets by local calendar day. An old queued item that crosses midnight
goes to review; use `/manual_retry ID` to explicitly authorize it on the new day.

A content submission with a lost/ambiguous receipt is NEVER automatically sent
again. Check Square before reconciling it. Resumable image processing uses the
existing upload ticket; it does not repeatedly upload the same ticket's PNG.
Restart recovery distinguishes pre-submission media work from possibly sent
content. Manual failures are contained in this lane; an actual account-wide
rate limit or restriction still holds both lanes safely.

Manual queued/in-flight/uncertain images remain protected through cleanup.
Confirmed manual content follows the existing three-day compression and
seven-day receipt-ledger retention behavior. Receipts and duplicate fingerprints
are kept; abandoned review/waiting/cancelled items can be cleaned after three days.

## Deploy only these six runtime files

Replace:

- `square_desk/service.py`
- `square_desk/telegram.py`
- `square_desk/runtime_status.py`
- `square_desk/retention.py`

Add:

- `square_desk/manual_posts.py`
- `square_desk/manual_media.py`

Keep these paths under your repository's existing `square_desk` folder and deploy
all six together. This patch builds on the complete v16 release, including its
database precision fix. It does not revert missing older releases automatically.
No credentials or runtime database files are included.

After deployment, `/status` should show revision
`2026-10-18-manual-audit-18`. `/manual_status` shows the separate cap 10.
Use one intended image + caption for the first real post, and verify its receipt
and appearance in Square. Automated tests use local databases and mocked network
responses; no real account publication or deployment was performed during build.


## Fresh audit v18

Use the v18 files together; they supersede the initial v17 manual patch.
See `MANUAL_AUDIT_V18.md` for reproduced edge cases and the final verification scope.
Rearming `/manual` cancels a previous incomplete composition; it does not cancel
an already queued post. Replying to your waiting image with `/manual Your text`
can recover an expired composition without creating a second record.
Image downloads have a 25-second absolute bound and content submission a 15-second
absolute bound (with the existing shorter HTTP inactivity timeout). An ambiguous
content timeout retains the slot and requires inspection, never automatic replay.
PNG palette transparency is preserved. Shared image files are written atomically.
