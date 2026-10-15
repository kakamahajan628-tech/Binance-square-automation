# Repair for the reported Aiven audit_pkey conflict

The live status reports Aiven at sequence 66 rejecting journal entry 67, statement 1, table audit, constraint audit_pkey. Neon remains the active synced writer; Supabase is connected and catching up. This release has not been applied to the live service by Codex.

## Install two files after diagnostics revision 3

Replace square_desk/database_failover.py and add square_desk/replica_audit_repair.py. Keep the previously updated postgres.py and turso_journal.py. Commit and deploy to the same Render service; no ENV, database URL, cluster ID, posting schedule or content changes are required.

The background replica worker can repair only a unique audit_pkey conflict on a secondary with verified fresh-start provenance. It reads and verifies every journal entry from the initial empty-audit checkpoint through the replica prefix, bounded to 1024 entries. It proves the conflicting ID was absent at that exact prefix. Missing/corrupt history, an unsupported audit operation, a valid recorded ID, a changed checkpoint or a primary target holds repair.

The conflicting row is locked, copied exactly into an owner-only square_replica_audit_archive table, read back to verify the copy, and removed from the active audit table in the same target transaction. Its data stays in the archive. No draft, publication receipt, queue, budget, state or watermark is changed by repair. Normal strict journal replay then retries the entry; there is no skipped mutation or bypassed row-count/hash check. At most eight proven repairs run per replica per process to bound repeated interference.

## Verify after deployment

Send /publish_status. implementation_revision should be 2026-10-13-replica-audit-repair-4. Aiven's audit_repair reports archived_extra_audit_row after a proven repair, or held with a safe reason if proof cannot be established. applied_sequence should then increase above 66. Aiven and Supabase are current backups only when they reach the current journal sequence; catching_up alone is not readiness. Do not force a primary outage to test until a replica is verified current.

Archive rows deliberately remain available for inspection; normal three-day audit cleanup does not remove this one-time conflict evidence. An unsuccessful proof is not re-downloaded repeatedly for the same checkpoint in the same process. After correcting a held condition, redeploy to recheck it. Recurrent conflicts can indicate another writer or external mutation; their origin has not been proven from the supplied status. This release does not automatically delete an entire database or repair arbitrary table divergence.
