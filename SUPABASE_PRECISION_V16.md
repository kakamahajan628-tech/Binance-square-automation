# Supabase replica precision repair v16

Revision: `2026-10-17-replica-float-precision-16`.

The user's Supabase diagnostic reported the expected column types/order, no
user triggers, and `extra_float_digits=0` in the SQL Editor session. The current
replica was held at a missing-row replay, and its checkpoint rebuild failed
strict read-back verification.

PostgreSQL documents that `extra_float_digits=0` rounds float8 text to 15
significant digits. This changes a timestamp such as `1792245728.2282267` to
`1792245728.22823` when decoded from text. A checkpoint exported with precise
output therefore fails the unchanged digest when read back through a rounded
target connection. The SQL Editor report alone does not prove the application's
session settings; this failure mechanism was reproduced locally with the actual
psycopg text loader and a simulated PostgreSQL/pooler default.

## Runtime change

Owned write transactions, consistent read transactions and snapshot exports now
set `SET LOCAL extra_float_digits = 3` before application reads and writes. The
setting is appended to the existing setup SQL, without another network round
trip. It is reapplied after pooler resets and expires on commit or rollback.
This also protects floating timestamps returned in journal afterimages.

No database-wide ALTER ROLE/ALTER DATABASE, table migration, data reset or RLS
change is needed. The SQL Editor may continue to show `0`: the patch controls
the application's transaction, not the SQL Editor's separate session.

The replica snapshot/archive/hash verification, fencing, journal reconciliation
and promotion checks are unchanged. Corrupt restored data must still roll back;
no tolerance, rounding of the canonical hash, or forced sequence advance exists.
All other v15 runtime files are identical, including scheduling, media recovery,
chart selection, 25-chart cap, publication safety and retention.

## Deploy over the verified v15 runtime

Replace only:

- `square_desk/postgres.py`
- `square_desk/runtime_status.py`

There are no new runtime files. Keep existing Render settings and secrets.
Deploy once. A fresh process clears the previous in-memory paused repair and
attempts the existing verified background rebuild. The active verified database
continues serving; Supabase is not forced into the writer role.

Check `/status` for the v16 revision and `/publish_status` for Supabase progress.
Success requires Supabase to reach `ready`, the current journal sequence,
`pending_entries: 0`, and an empty `last_error`. `catching_up` means repair/replay
has made progress but the replica is not yet current. Do not change its sequence
or mark it ready manually. If it remains held, keep the precise safe status for
the next diagnosis rather than resetting data or bypassing verification.

## Validation scope

The new tests exercise pooled transaction resets, PostgreSQL-style rounded
float text through psycopg, checkpoint rebuild with precise and rounded-default
sources, preservation of original timestamps, and rollback on real corruption.
They also check that a precision-setup failure prevents application work.
Regression tests cover failover, journal acknowledgements, media and publication
safety. Results are recorded in the supplied v16 test reports.

No remote database was accessed or modified for validation. The live rebuild is
confirmed only by the post-deploy provider status; other independent faults may
still be reported by the existing safety checks.

PostgreSQL reference:
https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
