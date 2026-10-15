# Replica progress and safe connection diagnostics

Local verification: 241 packaged tests passed on 13 October 2026. No cloud failover was forced or tested.

## Supabase connection correction

The user reports a host ending in supabase.co:5432/postgres?sslmode=require, which appears to be the Direct connection endpoint. Get a complete Session pooler URI from Supabase project > Connect > Session pooler. It supports IPv4 and uses a different username from the Direct endpoint. Copy the actual host/username rather than constructing a URL by swapping only the hostname.

Set DESK_SUPABASE_DATABASE_URL to the copied URI, replace [YOUR-PASSWORD] with the URL-encoded database password (without brackets), and append ?sslmode=require when there is no existing query string, or &sslmode=require when there are other parameters. Keep one secure sslmode parameter. Keep the Neon primary variables, Aiven URI and Turso token/cluster unchanged.

Official instructions: https://supabase.com/docs/guides/database/connecting-to-postgres

The supplied status shows Neon at the Turso journal sequence (13033), while Aiven reports 59 and Supabase has no successful sequence. The old reporter updates node.seq only when a full catch-up returns successfully. A bounded 128-entry batch raises a generic database error, leaves the displayed sequence stale and causes a 60-second failure cooldown even after successful commits. The status alone cannot distinguish a progressing replica from a failed connection or divergent history.

This correction exposes each replica's committed progress, pending entry count and an allowlisted error code. Successful partial batches continue in the single background replica worker without an outage cooldown. Real connection, TLS, permission, row-count, journal-chain and missing-history errors retain the cooldown and hold promotion. Driver response text, passwords, hosts and SQL values are never included in diagnostics.

## Replace two application files

Replace square_desk/database_failover.py and square_desk/postgres.py in the existing repository. Keep the already corrected square_desk/turso_journal.py. Commit and deploy to the existing Render service. All environment variables, cluster identity, PostgreSQL databases, posting schedule and content code remain unchanged.

After startup, send /publish_status. The implementation_revision will be 2026-10-13-replica-diagnostics-3 in this release. Each provider now includes applied_sequence, pending_entries and last_error. catching_up means a bounded successful batch is continuing; ready means the last verified target was reached. Check a subsequent status to compare the replica with the current journal sequence before treating it as ready for takeover.

Examples of safe error codes: connection_or_tls_failed (driver gave no specific SQLSTATE), authentication_failed, permission_denied, replica_checkpoint_mismatch, replica_row_count_mismatch and journal_history_missing. Each needs its own connection or history correction. This patch does not reset databases or bypass a mismatch, and does not prove a real failover by forcing an outage.
