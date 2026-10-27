# Bounded background replica batching

The user reports overnight catch-up. The previous worker performed one Turso read and a PostgreSQL transaction with several round trips per journal entry, serially for two backups. The live status confirms progress but supplies no timestamps for a measured throughput or finish-time estimate.

## Update from audit repair revision 4

Replace square_desk/database_failover.py, square_desk/postgres.py and square_desk/turso_journal.py. Add square_desk/replica_batch.py. Keep replica_audit_repair.py. Commit and deploy to the existing Render service; no ENV, cluster identity or posting settings change is required.

Only background materialization uses batching. Primary application writes and foreground promotion retain the existing path. Turso reads at most 32 entries with up to 2 MB combined compressed payload; an oversized first entry is fetched alone within the existing 8 MB entry/12 MB response limits. Decoded batches are bounded to 4 MB SQL/value bytes and 256 statements, with an oversized existing entry processed alone under its original limits. PostgreSQL pipelining reduces per-statement network waits inside an explicit transaction when the installed libpq supports it. Per-entry hash chains and every affected-row count are checked before the single end-of-batch watermark is committed.

Data is prefetched outside the PostgreSQL lock, then the checkpoint is checked again under the target transaction. If another replay advanced it, the old buffer is discarded. Constraint/count errors roll back the entire batch; only after confirmed rollback does individual replay locate and commit the valid prefix. Lost commit/rollback acknowledgements are held and reconciled from metadata on the next attempt. No primary caller code or publication POST is retried. Existing guarded audit repair remains available afterward.

Each backup turn handles at most 128 entries and yields after a completed batch once about five seconds have elapsed, so a large backlog on one provider does not monopolize the single worker. An individual slow network operation or original oversized entry can take longer. This is not a guaranteed wall-clock deadline or cloud performance claim.

## Verify

After deployment /publish_status should show revision 2026-10-14-batched-replica-5. Compare pending_entries in two reports a few minutes apart; confirm both backups reach the current journal sequence with ready status and no error. Existing committed checkpoints are reused, not reset. Cloud throughput, memory usage and provider-specific pipelining still require live verification.

Local simulation reads 96 journal entries in three Turso batch requests instead of 96 individual reads. This measures request reduction only, not an actual cloud speedup or completion time. The PostgreSQL pipeline behavior is documented at https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html .
