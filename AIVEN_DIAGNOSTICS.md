# Aiven constraint diagnosis

The supplied status confirms Neon at journal sequence 14834, Supabase connected and catching up at 488, and Aiven blocked by a constraint after sequence 66. No Aiven repair has been performed. The exact rejected row/constraint is unknown from that report.

The diagnostic update changes only square_desk/postgres.py and square_desk/database_failover.py relative to the preceding replica update. It preserves existing sqlite3.IntegrityError handling for application duplicates and reports only allowlisted PostgreSQL constraint types/names. Replica replay errors include a failed journal sequence, statement position, known app table and known constraint name. SQL text, row values and credentials are excluded.

Replace those two files, commit and deploy to the same Render service. Keep the corrected Turso file and existing environment variables. Send /publish_status after the next replica retry. implementation_revision will be 2026-10-13-replica-diagnostics-3. Share Aiven's last_error and failure_detail fields. Supabase progress continues independently; it becomes a verified current backup only after it reaches the current journal sequence.

This update diagnoses a constraint conflict; it does not repair it. No entries are skipped, no conflicting rows are overwritten, and no database is reset. A hash, checkpoint, row-count or constraint mismatch still prevents promotion. After its exact cause is identified, the next correction must preserve the authoritative journal and current Neon data.
