# Preserve data when recreating the Render web service

This release contains source and replacement files. It is NOT a backup of your
live account, database, ENV, Telegram uploads or current production state.
No cloud export, migration, deletion or deployment has been performed here.

## Before deleting the old web service

1. Save all current Render ENV values privately, including ENV-group references.
   Keep the same external PostgreSQL URLs, Turso journal URL/token, cluster ID,
   failover order, Telegram token/admin IDs, Square credentials, AI settings,
   admin credentials, policy settings, schedule and caps. Do not paste secrets
   into chat, Git, the deployment ZIP or a public report. The blank ENV example
   and blueprint defaults are NOT replacements for your existing ENV values.
2. Preserve the latest source commit and any required local-only artifacts/files.
   Generated PNG bytes normally live durably in the external database; previously
   retained or old files may still exist only on the web service's local disk.
   Render's ephemeral disk is lost when the service is deleted/replaced.
3. If reachable, record /status and database head/replica synchronization, and
   finish/stop in-flight publishing before the final backup. Disable auto-deploy
   and stop the old instance for the final handoff; keep the service configuration
   available until the replacement has passed checks. Do not run two active
   independent writers, force ownership, clear leases or reset the journal.
4. Take and inspect a real backup from a provider verified current at the journal
   head. Provider-native backups are preferable for full cluster metadata; retain
   the Turso journal and each existing database. The application's export helper
   can additionally save all four app tables with hashes and row counts:

   python scripts/square_database_transfer.py export --env DESK_AIVEN_DATABASE_URL --file saved-backups/redeploy-20261026.jsonl.gz
   python scripts/square_database_transfer.py inspect --file saved-backups/redeploy-20261026.jsonl.gz

   Run from the complete project with dependencies installed, in a private local
   environment where that ENV variable already contains the source URL. Replace
   the ENV variable NAME if another provider is the verified current source.
   Do not put the URL on the command line. Use a new backup filename each time;
   save it outside Render. The export is read-only and does not include Turso or
   full provider metadata. It includes private post/history data; protect it.
   Successful export does not itself prove the source was caught up to Turso.
   If the old service is suspended, verify the provider/journal state separately
   before treating the export as the final recovery copy.
5. Verify the saved ENV, source and backup are accessible and the inspect command
   succeeds. Keep Neon, Aiven, Supabase and Turso resources. Deleting the Render
   web service is not a database migration and does not require deleting them.

## New web service

Use the COMPLETE v20 archive contents at the repository root. Build:

    pip install -r requirements-square.txt

Start:

    python -m square_desk

Use /health for the health check. Keep one application worker/instance; do not
add Uvicorn workers or additional chart/upload workers to save RAM. Restore
existing private ENV, including DESK_STORAGE_EFFICIENT=true. Preserve the current
failover order rather than copying a blueprint default over your configured
preference. Reuse the SAME external databases and journal. Do not run bootstrap,
enroll, fresh-start initialization, restore or activate merely because the web
service changed. Do not set a new DESK_DB_FRESH_START flag or new cluster ID.

Wait for normal fenced lease handoff. WORKER_READY lease_owned=False alone does
not prove failure: the worker may be waiting for the old lease to expire. Verify
the current /status shows 2026-10-26-extreme-saver-20, a responsive worker and a
valid held lease, current active provider, expected controls/schedule/caps and
the original retained history. RAM observations start empty after every restart.
Then verify an ordinary automatic publication and image receipt; do not repeat
/post_now while recovery or an uncertain submission is in progress. Check the
separate manual budget and history remain present. Only retire the old service
after the replacement passes these checks and the backup is verified.

Changing service may change the public URL; update your dashboard/health monitors
and any configured inbound webhook URLs as applicable. Outbound Telegram polling
keeps its existing token/admin configuration. A workspace bandwidth suspension
still needs its quota reset or the normal Render billing resolution: creating
a new service in that exhausted workspace does not restore the allowance.
