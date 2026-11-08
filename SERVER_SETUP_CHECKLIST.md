# Server setup and validation checklist

Current status: **DEVELOPMENT INCOMPLETE**. Do not deploy publicly or label this
production-ready. Native SDK bindings and real adapters still need validation;
local mock tests are not the server acceptance tests below. No server commands
were executed during development.

1. Review the diffs in all three repositories. Preserve local changes. Create a
   development branch/commit/push yourself after review; the agent did none.
2. Read TASK_2_FINAL_REPORT.md for implementation limitations. Confirm hardware,
   book/chunk/pair concurrency and performance budgets. Existing defaults remain
   2 books, 4 chunks/book, 2 pairs, generator global semaphore 64, Chunker LLM 16.
3. Prepare an isolated staging project, filesystem and credentials. Keep the
   current server cleaner unchanged. Never mount real legacy data read-write into
   a test service. Reserve separate PostgreSQL DB/bucket for synthetic tests.
4. Check Docker/Compose and image availability. Review pinned image/package
   versions for current vulnerabilities and compatibility. These image builds
   have not been executed locally. No sensitive image uses latest.
5. Copy .env.staging.example to private .env.staging (mode 0600). Populate URL-safe
   OPS_DB_PASSWORD, TEMPORAL_DB_PASSWORD, OPS_S3_USER, OPS_S3_PASSWORD and a strong
   OPS_SERVICE_TOKEN. Set real model/OCR endpoints only for the approved real-SFT
   validation; code does not provision a model or OCR server. Configure the
   existing translation/quality dependency URLs without changing their logic.
6. Static review and initial startup (owner executes):

   ```sh
   docker compose --env-file .env.staging -f compose.task2.yml config --quiet
   docker compose --env-file .env.staging -f compose.task2.yml build
   docker compose --env-file .env.staging -f compose.task2.yml up -d postgres temporal-db temporal minio
   docker compose --env-file .env.staging -f compose.task2.yml run --rm migrate
   docker compose --env-file .env.staging -f compose.task2.yml run --rm storage-init
   docker compose --env-file .env.staging -f compose.task2.yml run --rm api python -m backend.ops.cli create-user owner --role admin
   docker compose --env-file .env.staging -f compose.task2.yml up -d
   docker compose --env-file .env.staging -f compose.task2.yml ps
   ```

   Migrations are additive ops_* tables and version markers 1/2, including book
   metadata backfill. They do not alter legacy SQLite/job folders. Review
   backend/ops/migrations/001_initial.sql, including its additive v2 section,
   before applying to an existing operational DB. CLI migration uses equivalent
   SQLAlchemy schema creation/backfill. Future column changes need new explicit
   migrations, not create_all. Run EXPLAIN ANALYZE for jobs/filter/rollup queries;
   optional pg_trgm/GIN search indexes require separate admin review/privilege.
7. Verify /health, /ready, worker and cleanup heartbeat health. /ready probes the
   operational schema, bucket and Temporal. Submissions may remain durable in
   the outbox while an executor is down; 202 is not an execution acknowledgement.
   Keep HTTP bound to localhost until TLS/reverse-proxy/RBAC tests pass.
8. Run local-independent tests in base_env and inspect actual results:

   ```sh
   conda run -n base_env --no-capture-output python -m unittest discover -s tests -v
   conda run -n base_env --no-capture-output python -m pytest
   ```

   pytest/temporalio/python-multipart/OTLP SDK must be installed after reviewing
   dependencies; do not broadly upgrade shared base_env. Its pre-existing
   hazm/numpy conflict still requires separate assessment.
9. Native infrastructure test: create an empty database named book2sft_ops_test
   and a private bucket ending -test. Export OPS_TEST_DATABASE_URL (asyncpg URL),
   OPS_TEMPORAL_TEST_ADDRESS, OPS_TEST_S3_ENDPOINT, OPS_TEST_S3_BUCKET and test-only
   AWS credentials. Run the existing test_workflow.py suite above. Its
   NativeServerCase uses real Temporal/PostgreSQL/S3 with synthetic activities.
   It deliberately refuses non-test DB/bucket names and leaves metadata for
   review. It does NOT validate real Chunker/SFT or model quality.
10. Validate actual end-to-end flow with a small approved synthetic OCR JSON:
    sign into the new dashboard, upload, observe queued/running, all four book
    stages, chunks/attempts and downloadable artifacts. Verify original SFT
    payloads, prompts, quality/diversity, ordering and outputs against baseline.
    Run the existing eight fast SFT tests too. Confirm generated versus
    quality-passed versus final-selected counts have distinct denominators.
11. Exercise pause mid-generation (>4 chunks), wait for safe acknowledgement,
    record completed hashes/attempt IDs, restart API/worker, resume and verify
    committed chunks do not regenerate. Test failed-chunk and whole-job retry,
    partial success, duplicate delivery, cancel-before-submit, late result and
    conflicting commands. Cancel supersedes pending pause/resume deterministically.
12. Crash worker during HTTP and during checkpoint publication. Verify heartbeats,
    activity retry, lease fencing, request-cache reuse and slot reconciliation.
    Unknown interrupted downstream requests intentionally require explicit retry;
    they are not silently reissued. Pause during an ambiguous lost activity stays
    pause_requested until reconciliation/cancel, not falsely paused. Automatic
    reconciliation of that pause edge case remains a development limitation.
13. Crash/restart Chunker. A committed chunker_done checkpoint can be reused.
    Unknown uncommitted subprocess outcomes are marked failed for explicit review;
    do not blindly restart an orphan process or reuse an arbitrary historical PID.
14. Retention: set short test-only policies, preview, dry-run, pause/resume cleanup,
    restart cleanup worker, then expire only synthetic terminal/nonresumable data.
    Check object expiry, metadata redaction, audit preservation, path/symlink
    protection, concurrent retry/deletion and idempotent deletion after DB failure.
    Downstream request-cache and Langfuse retention are not yet coordinated by the
    central cleanup worker; do not enable destructive production TTLs yet.
15. Security/browser checks: Viewer cannot mutate/read raw I/O; Operator cannot
    change policies or delete metadata; all legacy sensitive paths require service
    credentials during drain. Test expired/revoked tokens, rate limits, XSS-like
    book titles, denied artifact access, oversized inputs and SSE reconnect/bounds.
    Browser QA was not run locally. Check all dashboard pages, errors, pagination,
    confirmations, stale/no-data labels and mobile layout in a real browser.
16. Langfuse/OTel: deploy a separately reviewed compatible Langfuse server or use
    its private URL. Set LANGFUSE_ENABLED=true, LANGFUSE_HOST, PUBLIC_KEY,
    SECRET_KEY and LANGFUSE_TRACE_URL_TEMPLATE (with {trace_id}). Optionally set
    OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 and enable observability
    profile. OTLP HTTP spans go to /api/public/otel/v1/traces. TRACE_CAPTURE_CONTENT
    defaults false; enable only under the project's access/retention policy.
    TRACE_CONTENT_LIMIT controls previews. Token usage remains unavailable when
    the provider/client does not return it. Verify a known attempt's trace ID,
    child service spans, prompt/response previews, model and quality attributes.
    Turn Langfuse/Collector off and confirm pipeline continues at equal concurrency.
17. Benchmark isolated test metadata at 10k/100k/500k and representative millions
    of task/event rows. Measure p50/p95, query plans, RSS and generation throughput
    before/after tracing. Do not extrapolate the old summary benchmark to the new
    production schema. Validate histogram/rollup downsampling and search indexes.
18. Backup/restore metadata to an EMPTY test DB using CLI; separately snapshot
    objects and Temporal persistence. Verify checksums, interrupted workflow
    recovery and rollback. Portable metadata backup does not contain object bytes.
19. Legacy cutover: first drain old book workers. The guarded legacy API rejects
    new submissions by default (LEGACY_DRAIN_ONLY=true); existing authorized
    work can finish. Run CLI import-legacy <read-only-copy> without --apply first.
    It skips active jobs and imports terminal history only; --apply is an explicit
    reviewed step. Verify counts/downloads before routing clients to the new API.
    No legacy folder or SQLite database is deleted. Do not run old/new execution
    concurrently on the same jobs. The old server cleanup must remain unchanged
    until its actual paths/policies are identified and replacement is approved.
20. Only after remaining development gaps and all validation pass, consider the
    the hardening already included in compose.task2.yml, scoped object IAM, TLS, backups,
    secret rotation and traffic routing. This override is not an HA design.
    Stop only this staging project's containers after tests. Preserve volumes
    until synthetic ownership is reviewed and deletion approved; never global-prune.
