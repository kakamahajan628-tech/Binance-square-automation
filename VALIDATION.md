# v22 validation

All tests use local databases and mocked APIs. No production deploy, purge,
backup export, webhook change or publication was performed.
Golden charts across all five views have identical pixels and PNG bytes in the
local prior Pillow 12.3.0 and pinned Pillow 13.0.0 builds. Exact-hash recovery,
restart/upload-ticket resume, evidence expiry/mutation, cancellation/late output,
no extra image budget, no market fetch/AI rewrite, manual exclusions, webhook
startup and backup-verified/protected legacy cleanup are covered. Full-suite and
exact packaged-source checks are recorded during verified packaging.

## Final v22 results

Complete suite: 600 passed; zero failures/errors/skips. Exact packaged-source smoke and overlay checks are verified separately.
