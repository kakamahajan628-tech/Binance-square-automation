# Turso startup correction, 13 October 2026

The reported deployment passed initial Turso schema/seed requests and PostgreSQL preparation, then failed inside journal.initialize. The previous initializer reused the original HTTP baton across round trips and spread a five-second transaction over several network calls. The underlying service response was deliberately redacted, so the exact live rejection is not known.

Journal initialization now sends BEGIN, idempotent schema definitions, a conditional cluster INSERT, a metadata SELECT and COMMIT in one HTTP pipeline request. It does not overwrite existing cluster metadata or history. The interactive setup helper also follows newly returned batons. Errors expose only HTTP status, SQL operation position or a safe connection/timeout classification; credentials and remote response text remain hidden.

Only square_desk/turso_journal.py changed in the application. Posting schedule, content generation, budgets, three-day cleanup, PostgreSQL URLs and the other modules are identical to the previous complete ZIP.

## Deploy to the existing service

1. Replace square_desk/turso_journal.py in the existing GitHub repository with this corrected version. Alternatively upload the complete corrected ZIP contents to the repository root. Commit the replacement.
2. Keep your current database URLs, token, cluster ID and fresh-start flag. Do not delete/recreate databases or rotate the token just because of this traceback.
3. Render: Manual Deploy > Deploy latest commit, unless the commit already triggers an automatic deploy. Keep the same service and existing ENV/secrets.
4. After startup completes, check Telegram /status and /settings. For the initial fresh cluster only, use /unlock CONFIRM after confirming the intended schedule and database health.
5. If startup still fails, share the final traceback lines only. The new safe reason helps distinguish HTTP authorization/quota errors, SQL rejection and timeouts. Never share tokens or passwords.

This corrects a demonstrated client bug; local tests do not prove the live Turso engine, network and PostgreSQL replicas are operational.
