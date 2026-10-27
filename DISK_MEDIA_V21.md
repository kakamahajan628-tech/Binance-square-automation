# Disk-only images v21

Revision: `2026-10-26-disk-media-21`. Based on complete v20.

## Image storage contract

New automatic charts and administrator-supplied manual images keep their exact
PNG bytes only in DESK_ARTIFACTS on the application's filesystem. Database media
rows contain only sha256, byte count and storage="disk". No png_base64 field or
image-byte copy is created by the normal media persistence path. Consequently
the new image bytes do not enter the Turso journal or PostgreSQL replicas.
The raw image still travels once to the normal Square presigned upload endpoint;
content submission, hash binding and receipt checks remain intact.

Reads validate the PNG size/format and compare its bytes with both the
content-addressed filename and the approved hash when provided. They make no
database request and never decode a legacy base64 image or redraw a chart.
Missing or changed images stop that image post before HTTP upload/content POST.
The existing draft-scoped review handling applies, without a text-only fallback
or a global account restriction. Manual failed-before-submit reservations are
refunded by the existing manual lane; uncertain content submissions are never
automatically repeated. Authenticated previews return 404 for a missing file.

## Explicit durability tradeoff

Render Free's local filesystem is ephemeral. Restarts, redeployments, service
replacement or another instance can lose/cannot see these files. DATABASE
FAILOVER NO LONGER PROVIDES IMAGE-BYTE RECOVERY. Existing post text, hashes,
budgets, controls, receipts and publication history remain in the databases.
Already-published Square images remain on Square. A pending image lost from disk
requires restoring the EXACT hash-matching file from your own backup, or creating
a new reviewed chart/manual post. Different bytes must never reuse old approval.

Before deleting the old Render service, save required local PNGs outside Render
and copy them to the new service's configured artifact directory, preserving
filenames and hashes. The code/source ZIP contains no live artifact files.
An image that is absent from the old disk cannot be recovered by this release.
Do not delete the only legacy database backup until required images are exported.
For restart/redeploy durability use your own backup, persistent disk or object
storage; none is created or enabled by this release.

## Existing database images

Deployment does not bulk-delete old image rows, reset the journal or rewrite old
checkpoints. A matching old media row is converted to tiny disk metadata only
when persist_png is called and its exact hash-matching local PNG is validated.
Missing/changed files never cause the last legacy DB copy to be erased by that
operation. Other historical base64 rows remain until their normal protected
retention/explicit maintenance; historical journal/checkpoint copies follow
existing compaction. They are not read as a recovery fallback by v21.
This avoids silently discarding an unavailable user's only remaining image.
No live cleanup or deployment has been performed as part of preparing the files.

## Scope and deployment

Replace five files together on complete v20: square_desk/media.py, store.py,
publisher.py, service.py and runtime_status.py. No new runtime files or schema
migration. New web services should use the COMPLETE v21 archive, preserving
existing private ENV, database URLs and Turso cluster/journal. DESK_ARTIFACTS must
be writable. Existing v20 RAM/bandwidth optimizations remain included unchanged.

Posting schedule/caps, manual extra 10/day, image dimensions/quality, chart
rendering timeout, provider ordering, approval gates and uncertain-submission
safety are unchanged. The intended functional change is removal of DB image-byte
storage/recovery. Full status now reports image_storage="disk_and_hash",
database_image_bytes=false and durable_png_storage=false. These fields describe
the current write/recovery path, not proof that all old DB copies were purged.
The overall 5 GB monthly Render budget still needs actual usage measurement.

In the same mocked three-replica fixture used for v20, storing/replaying a chart
and one raw Square PUT fell from an average 525040 bytes to 90485 bytes (five
charts averaging 82648 PNG bytes). The large 901466-byte manual PNG scenario
fell from 5729044 to 909309 bytes. These are serialized request/session-setup
estimates, not measured Render billing. They exclude full active publication,
real transport overhead, retries and historical catch-up/rebuild traffic.

See REDEPLOY_V21.md before removing or replacing the old web service.
