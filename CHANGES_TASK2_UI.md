# Book2SFT dashboard change summary

This package improves the **original** Book2SFT frontend, rather than introducing a separate dashboard.

## Source changes

- `frontend/index.html`: original upload view retained; monitoring tab redesigned with operational actions, charts, service health, detail inspector and retention settings.
- `frontend/monitoring.js`: uses PostgreSQL-backed `/v1/ops/*`, charts from recorded metric rollups, cursor pagination, trace links and job controls.
- `frontend/styles.css`: keeps original colors/structure; adds responsive detail and management components.
- `backend/ops/web_routes.py`: `/` and `/static/*` serve original frontend; data-plane service health includes generator, assessment and quality services.
- `backend/ops/api.py`: removes browser login/session endpoints; trusted-local console is enabled only by environment on private staging.
- `backend/ops/queries.py`: newest-first cursor pagination.
- `backend/ops/cli.py`: removes `create-user` command.
- `compose.task2.yml`: enables trusted local console, exposes only `127.0.0.1`, adds service probe environment and Chainguard MinIO image used in staging.
- `frontend/ops/*` and `backend/ops/auth.py`: obsolete separate login dashboard and login implementation removed (legacy DB tables retained to avoid destructive migration).
- `.dockerignore` / `.gitignore`: keep runtime monitoring DB and logs out of future source commits / Docker context.
- `tests/test_ops.py`: API and cursor regression updated.

## Validations performed

- Python unittest: 25 total, 24 passed, 1 native Temporal/PostgreSQL/S3 test skipped because external services unavailable.
- `node --check` passed for frontend JavaScript.
- Playwright checked rendering and data/graphs/Job Explorer on mock responses (no JavaScript page errors). Mock test data are illustrative only, **not evidence of runtime output or 500k-job service capacity**.
- No Docker build, real OCR/model call or real Temporal/PostgreSQL/MinIO integration executed in this workspace.

See `README_MONITORING.md` before deploying. Preserve existing `.env.staging`, mounted volumes and legacy jobs.
