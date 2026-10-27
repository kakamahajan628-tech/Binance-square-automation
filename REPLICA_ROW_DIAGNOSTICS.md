# Replica row-count diagnostics

The reported live snapshot showed Aiven ready at sequence 34138 after one provider switch. Neon and Supabase both stopped at 33818 because statement 1 of entry 33819 affected an unexpected number of rows in `entities`. This is a replay verification failure, not evidence of slow catch-up or a network quota problem. A watermark and hash prove the logged sequence, but cannot establish that all physical rows are equal after unjournaled edits or other divergence. The exact cause cannot be inferred from the original status.

Replace both `square_desk/database_failover.py` and `square_desk/replica_batch.py`, commit, and redeploy the existing service. No ENV changes are needed. This is a diagnostic patch, not a data repair or a guarantee of uninterrupted posting. A rolling deploy can briefly wait for the old writer lease.

After the next failed replay, `/publish_status` provides the failed operation and row counts, for example:

```json
{
  "journal_sequence": 33819,
  "statement_position": 1,
  "table": "entities",
  "operation": "update",
  "expected_rows": 1,
  "actual_rows": 0
}
```

This example is illustrative; the reported live failed operation is not yet known. `update` with fewer matched rows can indicate missing rows or different predicates/statuses. `insert` with zero rows under `ON CONFLICT DO NOTHING` can indicate an existing conflicting row or fingerprint. `delete` can match fewer or more rows because the underlying row set differs. None of these counts alone proves which copy is correct or authorizes automatic deletion/copying.

No SQL text, bound values, entity identifiers, passwords or provider connection URLs are added to the diagnostics. The strict expected-row-count check, transaction rollback, journal hash verification, promotion checks, background batching and guarded audit-only repair remain intact. Entries are not skipped, replica watermarks are not forced, and no entity data is repaired automatically.

Publishing service, scheduler, approval and uncertainty handling, three-day cleanup, image generation and 25-image configuration remain byte-identical to the Charts-v6 release. The full ZIP contains that complete release plus this diagnostic patch. Confirmed ready Aiven is retained while the process keeps running; the patch does not add automatic failback. Restart selection still uses configured provider order and verified-current checks.

Once deployed, send only the `database_failover` section of `/publish_status` for the next diagnosis. Exact reconstruction or verified resynchronization requires additional evidence; never treat `entities` divergence as the previously guarded extra-audit-row case.
