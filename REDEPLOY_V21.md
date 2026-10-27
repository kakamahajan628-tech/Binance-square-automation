# Recreate a web service without losing its state or pending images

This is a source release. No private ENV, live DB or image backup is included.

1. Save all old Render private ENV values and current source privately. Preserve
   the SAME Neon/Aiven/Supabase URLs, Turso journal URL/token, cluster ID, failover
   order, Telegram/admin/Square/AI credentials, policy configuration and caps.
2. Save required LOCAL PNG files from DESK_ARTIFACTS outside Render BEFORE the
   old service is deleted. v21 cannot restore images from database base64. Hashes
   and post text alone cannot reconstruct approved image bytes. Do not erase old
   DB backups containing the only remaining copy of an unavailable image.
3. Confirm the active provider is caught up to the Turso head; stop in-flight
   publishing and the old instance for the final handoff, and disable auto-deploy.
   Retain service settings until the replacement is checked. Back up the current
   databases/journal using the provider's backup tools. The existing application
   exporter can additionally save the four application tables read-only:

   python scripts/square_database_transfer.py export --env DESK_AIVEN_DATABASE_URL --file saved-backups/redeploy-disk-v21.jsonl.gz
   python scripts/square_database_transfer.py inspect --file saved-backups/redeploy-disk-v21.jsonl.gz

   Use an already-configured private ENV variable NAME for the verified current
   source. Do not put its URL in the command. Use a fresh output filename and save
   the verified file outside Render. This app export does not back up the whole
   Turso journal/provider metadata or local artifact directory.
4. Deploy the COMPLETE v21 ZIP contents at the repository root. Build with
   pip install -r requirements-square.txt; start with python -m square_desk;
   health path /health. Keep one application worker/instance.
5. Restore old ENV and copy exact required PNGs to DESK_ARTIFACTS. Reuse all
   existing external databases and the journal. Do not bootstrap, enroll, reset,
   clear leases, change cluster ID or set a new fresh-start flag for redeployment.
6. Allow normal writer lease handoff. Check /status revision
   2026-10-26-disk-media-21, responsive worker, held lease and current active DB.
   Check retained post history and caps, then an ordinary chart publication with
   a confirmed Square image receipt. Missing-image posts must enter review and
   must not silently publish text. Do not repeat uncertain submissions.
7. Retire the old web service only after ENV, real backups, artifact copies and
   the replacement are verified. Keep all external DB/Turso resources. Update
   changed dashboard/monitor/webhook URLs if applicable.

The new local disk can be lost again at later restart/redeploy. This release
intentionally accepts that durability loss in exchange for removing DB image
copies. A new service in an exhausted Render workspace does not reset bandwidth.
Official references: https://render.com/docs/free and
https://render.com/docs/outbound-bandwidth
