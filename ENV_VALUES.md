# Add/edit in the existing Render service; do not replace unrelated ENV

Private URI/token placeholders below must be replaced with your actual service values in Render only. No quotes or `KEY=` prefix in a Value field. Keep exactly one secure `sslmode` in each PostgreSQL URI.

| Name | Value |
|---|---|
| `DESK_DATABASE_URL` | New Neon pooled URI |
| `DESK_NEON_DATABASE_URL` | Same new Neon pooled URI |
| `DESK_AIVEN_DATABASE_URL` | Aiven URI, `sslmode=verify-full&sslrootcert=/etc/secrets/aiven-ca.pem` |
| `DESK_SUPABASE_DATABASE_URL` | Supabase Session-pooler URI with `sslmode=require` |
| `DESK_TURSO_JOURNAL_URL` | New libSQL journal database URL |
| `DESK_TURSO_JOURNAL_TOKEN` | Its read/write database token |

Individual static values:

`DESK_DB_CLUSTER_ID`

```text
square-fresh-20261012
```

`DESK_DB_FALLBACK_ORDER`

```text
neon,aiven,supabase
```

`DESK_DB_FALLBACK_ENABLED`

```text
true
```

`DESK_DB_FRESH_START`

```text
CONFIRM_EMPTY_NEW_CLUSTER
```

`DESK_STORAGE_EFFICIENT`

```text
true
```

No export required. The confirmation is for a new empty cluster; existing populated databases/another cluster are refused. Never change the cluster ID to try to clear an error on a started cluster.

Telegram/Square/AI keys stay unchanged. Verify the schedule values already in Render ENV; old Telegram-only runtime overrides are not imported. Full steps are in `START_HERE.md`.
