# Book2SFT — upgraded original dashboard (private staging)

This deliverable **reuses `frontend/index.html`, `frontend/styles.css`, and the original upload and monitoring views**. The separate `frontend/ops` login dashboard was removed.

## Scope
- The original `/` page now shows the existing Book2SFT interface. The Monitoring tab reads `/v1/ops/*`, not the SQLite `/v1/monitoring/*` API.
- PostgreSQL/Temporal/S3/MinIO workflow/worker implementation is retained; generated SFT logic and Chunker stay in their own repositories.
- Private staging mode (`OPS_TRUSTED_LOCAL_MODE=true`) has no browser login, user menu or session management. The UI is only exposed on `127.0.0.1:${OPS_HTTP_PORT}` via Compose. **Never expose or publicly reverse-proxy this port while trusted mode is enabled**. Use SSH tunnel/VPN and local firewall. The backend's old `ops_users` tables remain only for schema compatibility; existing data is not deleted.
- Chunk/job detail, command requests, trace correlation, retention policy and preview, dynamic recorded analytics, and server-side keyset pagination are available in the **original UI**.
- Langfuse server is **not bundled**; optional outbound telemetry requires `LANGFUSE_ENABLED=true`, `LANGFUSE_HOST`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY` and `LANGFUSE_TRACE_URL_TEMPLATE` (e.g. `https://langfuse.example/project/PROJECT_ID/traces/{trace_id}`). Instrumentation must be validated with real LLM requests on staging.
- Graphs use recorded SFT attempt and book rollups, not fake values. The SFT accepted metric means quality-passed pairs, **not final book-selected pairs**.

## Install

Copy the contents of this directory into the existing `book2sft` repository (preserve your private `.env.staging`; never replace it with the example). Before deployment, check `git diff` and avoid accidentally committing runtime DB/logs.

```sh
cd ~/book2sft
# .env.staging already exists from the previous staging install
sudo docker compose -p book2sft-staging --env-file .env.staging -f compose.task2.yml config --quiet
sudo docker compose -p book2sft-staging --env-file .env.staging -f compose.task2.yml build api worker cleanup
sudo docker compose -p book2sft-staging --env-file .env.staging -f compose.task2.yml up -d --no-deps --force-recreate api
curl -fsS http://127.0.0.1:18088/health
```

Open through SSH tunnel (from your laptop):

```sh
ssh -N -L 28088:127.0.0.1:18088 bagher2
```

Browse `http://127.0.0.1:28088/`. No login. **Only the API UI has been restarted by the commands above**; worker and SFT services require their separate deployment/validation, with valid OCR/model URLs.

## Important limits
- Not production-ready; workflow controls/retention must be E2E-tested on isolated staging. The legacy three-day cleaner must not be modified by this step.
- Retention TTL in the UI applies to **new** matching artifacts only; it does not currently synchronize downstream request caches or Langfuse retention. Keep destructive production cleanup disabled until lifecycle coordination is validated.
- Queries are bounded per request; live summary aggregation still queries the operational DB and is not a proof of 500k-job throughput under concurrent production load.
- Keep copies of current Compose and all persistent volumes; never `down -v` or `docker system prune` as part of routine upgrades.
