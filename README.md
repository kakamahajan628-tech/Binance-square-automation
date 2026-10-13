# Square Desk — fresh Neon primary, PostgreSQL fallback, three-day cleanup

Upload the extracted source into your **existing GitHub repository**, then deploy the latest commit on the **same Render service**. Keep existing Telegram/Square/AI secrets and posting schedule ENV. No old database export is required for the explicitly authorized empty-cluster fresh start.

1. Read [SETUP.md](SETUP.md), add the new storage connections from [ENV_VALUES.md](ENV_VALUES.md).
2. Build: `pip install -r requirements-square.txt`
3. Start: `python -m square_desk`; health check: `/health`.
4. Wait for the old worker to stop, verify `/status` and `/settings`, then `/unlock CONFIRM`.

Master cleanup runs every three days. It protects active/uncertain work, compresses old post data, preserves the existing seven-day duplicate/weekly checks and retains compact receipts and command fingerprints. It does not delete posts on Binance. Dashboard uses manual refresh and lazy post details.

`render.yaml` is the only canonical Blueprint: Free compute, external PostgreSQL, no paid disk. Existing manually configured services do not automatically apply this YAML. If your Git repository still contains the old `render-square.yaml`, you can remove that obsolete duplicate; it is not used by this package.

Do not commit `.env.square`, database URIs, tokens, certificates or runtime data. Database/API credentials must be supplied in Render; no deploy ZIP can contain your private cloud setup safely. Free bandwidth/uptime is finite and cannot be guaranteed forever.

See [AUDIT_REVIEW.md](AUDIT_REVIEW.md) and [VALIDATION.md](VALIDATION.md).
