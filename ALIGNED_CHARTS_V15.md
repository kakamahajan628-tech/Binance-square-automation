# Aligned charts and media timeout recovery

## Evidence and root causes

The supplied 07:57 Telegram screenshot confirms one image publication with a Square receipt. That working flow is retained. A prior live record contains `media_timeout` before content submission. Source inspection shows the old adapter put presign, PNG PUT and ten three-second processing polls inside one 30-second wait. It discarded a still-processing image into review. The renderer also selected its default view from coin/time because the saved post context was never passed in.

Two local reproductions fail against unmodified v14: processing pending demotes the approved draft, and the render call receives no topic context. These are preserved in Charts-v15-Baseline-Regressions.xml.

## Presentation changes

- Saved title/prose chooses among existing trend, value, volatility, momentum and structure views. Explicit RSI gets a larger RSI panel; MACD appears only when discussed. Volume focus does not add an unrelated VWAP overlay; VWAP appears when the post discusses it. Psychology/general education uses structure.
- Candles, price, indicators and reference levels continue to use the exact archived snapshot associated with the draft. No fresh candles are substituted during image generation.
- Support/resistance shading is a labelled ATR-padding presentation around prior 20-bar levels, not measured order-book depth. Close position inside/above/below that range is derived from those candles.
- Dashed volume reference is the mean of the previous 20 bars, excluding the current bar. Base-asset units and the latest relative-volume ratio are explicit.
- Pair, 15-minute closed-candle timeframe and UTC close timestamp remain visible. Peer observations are explicitly hourly, extracted only from the post's original valid provenance, with no claim of measured correlation.
- Fonts/labels are fitted to the existing 1600x1100 canvas. No AI images, external chart services, derivative feeds or additional market API fetches were introduced.

## Upload recovery and safety

Each network operation has the configured upload timeout; synchronous checkpoint work and deliberate processing waits no longer share a single 30-second network budget. Recovery also has a fixed overall 180-second bound, at most 30 status checks and 3 presign attempts. It is not an unlimited loop or a blanket timeout increase.

Private progress is bound to image filename, approved PNG hash and Square-key fingerprint. Presign attempts are saved before allocation; a returned file ticket is saved before the PNG PUT. A lost PUT response resumes status checks against that ticket. A known ticket never triggers another PUT. The presigned URL and API key are not stored in this progress state or diagnostic output.

Processing pending and transient transport waits retain the original approval and schedule another bounded check. Invalid media, explicit processing failure, authentication/quota rejection, exhausted bounds and expiry keep safe rejection/review behavior. Readiness is never inferred from a timeout. Progress after restart restores exactly approved PNG bytes if local artifacts were lost.

Every checkpoint/readiness transition verifies current draft state, image hash and worker lease. A retiring worker cannot overwrite a successor. Publication still rechecks controls, freshness, approval, duplicate protection and actual spacing immediately before content POST. If a content POST has an ambiguous outcome, it remains uncertain and is never automatically repeated. Only media status reads are resumed.

Background media can prepare during a minimum-spacing conflict; the scheduler and publisher still enforce the original actual publication gap. Media preparation never sends content. Existing pending/failed historical drafts are not automatically promoted back to approved.

## Verification

Targeted tests cover same-ticket resume, lost presign/PUT/status responses, restart with missing PNG, one content POST, slow synchronous checkpoints outside the per-request budget, bounded presign attempts, overall recovery expiry, lease loss, and media preparation before a future slot without early publication. Existing tests retain moderation, image-byte binding, source freshness, quotas, cancellation, real render deadlines and publication uncertainty checks.

Synthetic preview PNGs are visibly marked as demo data; no market conclusions should be drawn from them. Local timing/byte measurements are in Charts-v15-Render-Check.json. They are not a Render performance or live upload guarantee. Full-suite totals and package CRC/hash checks are reported in VALIDATION.md and Charts-v15-* reports.

Protocol source checked: [Binance official image helper](https://raw.githubusercontent.com/binance/binance-skills-hub/main/skills/binance/square-post/scripts/lib.mjs). Existing endpoints, success/processing/failure states and content request format remain unchanged. This repair changes local orchestration and recovery, not remote API behavior.

Database failover, snapshot verification, AI provider/model order, quality thresholds, content prompts and cleanup are unchanged from v14. Last supplied Supabase restore remains unverified; this release does not certify it or bypass its hold. External outages, sleeping compute and provider processing failures cannot be eliminated by local tests, and are never reported as confirmed publication.
