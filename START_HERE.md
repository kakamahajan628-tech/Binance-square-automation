# Existing service upgrade — complete repair v10

Bhai, is baar **poora folder/ZIP** diya hai. Alag patch files combine karne ki zarurat nahi hai.

## 1. Existing Git repository update karo

1. ZIP extract karo. Jis folder mein `square_desk`, `scripts`, `tests`, `requirements-square.txt` aur `render.yaml` dikhe, wahi source root hai.
2. Us root ka **saara content existing Git repository ke root mein replace/add** karo. `square_desk` ke andar doosra `square_desk` folder mat banana. Sirf do Python files upload mat karna.
3. Ek commit karo, jaise `Complete runtime and database repair v10`.

Existing Render service delete mat karo. Naya repo ya nayi service create karna zaruri nahi hai. Is ZIP mein credentials nahi hain; existing Render ENV aur Aiven CA secret file wahi rahenge.

## 2. Existing ENV rakho

Is repair ke liye naya ENV key required nahi hai. Current configured URLs, tokens, CA file, cluster ID aur posting settings preserve karo.

- `DESK_DB_FALLBACK_ENABLED=true`
- `DESK_DB_CLUSTER_ID`: **abhi jo value hai wahi**, naya cluster ID nahi.
- `DESK_DB_FALLBACK_ORDER`: existing `neon,aiven,supabase` order reh sakta hai. Jo current verified provider healthy hai, use baar-baar preferred provider par switch nahi kiya jata.
- Neon/Aiven/Supabase connection variables aur Turso journal URL/token: existing values rakho. Backup provider ko current hone tak ready nahi kaha jayega.
- `DESK_NEON_BATCH_SECONDS=0`: current continuous schedule ke liye isi value par rakho. Is repair mein 1800/3600-second batch delay add nahi hota.
- `DESK_TELEGRAM_POLLING=true`: current polling setup ke liye rakho.
- `DESK_IMAGE_DAILY_CAP=25`: current cap preserve karo.
- Publishing mode, AI auto publish, paper mode, daily/hourly caps, gap, scan interval, publishing hours aur policy date: current intended values preserve karo. Purane fresh-start/unlock steps dobara mat chalao.

`DESK_DB_FRESH_START` ka existing marker registered cluster ko har redeploy par reset nahi karta. Is repair ke liye marker, schema, journal ya any database manually delete/reset mat karo. Empty databases/new accounts add karna is upgrade ka step nahi hai.

## 3. Same Render service deploy karo

Existing service -> **Manual Deploy -> Deploy latest commit**.

Verify settings:

```text
Build Command: pip install -r requirements-square.txt
Start Command: python -m square_desk
Health Check Path: /health
```

Logs mein ye revision dikhegi:

```text
WORKER_STARTING revision=2026-10-15-control-switching-10
WORKER_READY lease_owned=... telegram_poller=independent
```

Rolling deploy mein old worker retire hone tak lease ka wait ho sakta hai. `lease_owned=False` old lease/standby bhi ho sakta hai; nayi empty database ya reset ki zarurat ka proof nahi hai. `WORKER_READY` initialization batata hai; working lease aur replica progress `/status` mein dekho.

## 4. Deploy verify karo

Telegram mein **ek baar `/status`** bhejo. Default reply ab compact memory observations hai; woh database lock lene ya backlog flush karne ka wait nahi karta. Ye fields dekho:

```text
implementation_revision: 2026-10-15-control-switching-10
worker.loop_responsive: true
worker.lease_observed_held: true
telegram.poller: independent
database_failover.active_provider: current verified provider
```

`/publish_status` bhi fast observation report deta hai. `publication.last_plan` aur `publication.last_hold` mein last known scheduling/hold reason milega. Ye cached observations hain, posting permission ka replacement nahi. Restart ke turant baad last-plan/last-confirmed empty ho sakte hain.

Detailed original report ke liye `/status full` ya `/publish_status full` bhejo. Ye reports live database read karti hain, isliye database slow ho to wait ho sakta hai. `/settings`, `/queue`, `/errors`, approval/safety/publishing commands existing durable checks se hi chalti hain.

`/health` HTTP liveness hai. New `/ready` RAM observations se worker readiness ka 200/503 deta hai. Render ka deployment health check **`/health` hi rakho**; `/ready` ko deployment gate banane se rolling lease handoff atak sakta hai. `/api/runtime` existing dashboard Basic Auth se protected fast endpoint hai.

## Repair behavior

- Old `entities` mismatch: healthy current source ka verified checkpoint, target backup/archive, restore verification aur strict remaining journal replay. Target current verify hone ke baad hi fallback eligible hota hai.
- Primary outage: another verified current PostgreSQL provider select hota hai. Caller ke mutation body ya Binance POST ko blindly replay nahi kiya jata.
- Lost publication response/commit acknowledgement: uncertainty and journal reconciliation rules preserve hoti hain; duplicate Binance POST automatically nahi bheja jata.
- Temporary replica repair/network failures: bounded backoff ke baad retry; permanent verification mismatch ka specific hold visible rehta hai.
- Polling mode mein Telegram 409 aur old webhook conflict mile to **sirf verified owner** webhook delete karta hai with `drop_pending_updates=false`. Pending updates drop nahi hoti. Another running poller ho to conflict visible rahega; same token par do copies parallel run mat karo.
- Authorized status replies operational database work se isolated hain. Mutating commands original worker par serialize aur deduplicate hote hain. Worker/database unavailable hone par status ko successful mutation response nahi maana jata.
- Three-day cleaner, seven-day duplicate/report evidence, receipts, active/uncertain drafts aur existing image cap retained hain. Binance par published posts delete nahi hote.

## Hosting limit jo code remove nahi kar sakta

[Render Free service](https://render.com/docs/free) 15 minutes tak inbound traffic na milne par sleep ho sakti hai. Sleeping service mein Telegram polling aur scheduled posting nahi chalenge. Exact uninterrupted schedule ke liye always-on compute chahiye; sirf database fallback, workspace upgrade ya ZIP replacement se Free-instance sleep guarantee remove nahi hoti. Is release ne koi paid change ya external monitor create nahi kiya.

Cloud quotas, provider outages, moderation rejections aur already uncertain posts ki wajah se safe holds ab bhi possible hain. Local tests pass hone ka matlab live credentials, quota aur 24/7 uptime verify hona nahi hai. Bandwidth waste reduce kiya gaya hai; traffic ko forever 5 GB ke andar rakhne ki guarantee nahi hai.
