# Task 2 — local development handoff

**STATUS: DEVELOPMENT INCOMPLETE**

The new operational path is implemented substantially further than the previous
report, but it is not yet complete or production-ready. No server deployment,
commit or push was performed. All local Python ran in the existing base_env.

## What changed

- A compact Temporal book workflow, activity bindings, worker, at-least-once
  outbox dispatcher, durable book-slot capacity, heartbeat health and terminal
  workflow reconciliation are implemented. New submissions use PostgreSQL state
  and Temporal rather than the legacy in-memory queue.
- The production adapter restores S3 checkpoints into activity scratch space and
  reuses existing Chunker/SFT/selection/review functions. Completed chunk results
  are reused; retry invalidates dependent stages. Stage leases and attempt fencing
  prevent old workers from publishing over newer work. Resume re-aggregation
  resets prior metrics to avoid double counting.
- Pause/resume/cancel/retry/failed-chunk retry/archive/soft delete connect to the
  workflow path. Cancel supersedes a pending pause/resume in a defined order.
  Issued downstream chunk work may finish; cancellation is not reported complete
  until the service acknowledges exit, and late publication is fenced.
- Chunker has token-protected cancellation, deterministic controlled submission
  IDs/checksums, subprocess-group termination and committed chunker checkpoints.
  Ambiguous crash outcomes require explicit intervention instead of blind replay.
- SFT's operational request cache is durable and prevents duplicate replay after
  a lost response. Unknown interrupted requests require explicit retry. Existing
  Translation/DPO branches bypass this control path.
- The dashboard includes authenticated overview, filtered/cursor-paginated jobs,
  job/stage/chunk/attempt/artifact detail, controls, worker/service health,
  analytics, retention settings/history and audit/user listing. Charts consume
  recorded rollups, including retries, quality/rejects, book completions and
  duration buckets. ETA explicitly covers known chunks only and shows uncertainty.
- Optional OTLP/Langfuse code propagates correlation IDs, records generation and
  SFT quality spans, and uses bounded background export. Prompt/response previews
  are opt-in; missing tracing does not stop generation. Token usage is unavailable
  where the existing model client returns only text; no counts are invented.
- Retention includes protected artifacts, dry-run/preview, durable scheduling,
  run controls and metadata/event/error-detail expiry while preserving audit.
  Backup/restore and terminal legacy import are implemented. Imports skip active
  legacy jobs and do not silently resume them.
- Sensitive legacy API access is guarded during drain; the new API requires user
  authorization. Secrets are excluded from Git and Docker build contexts.

## Files and repositories

All three repositories changed: book2sft, Chunker_Book and
Data-construction-pipeline. Changes to generation services are transport/control
and optional instrumentation; generation prompts/policies and Translation/DPO
logic were not intentionally changed. Full quality/performance equivalence is
not established by the available tests.

**One new main launch YAML: compose.task2.yml.** It contains PostgreSQL, Temporal,
MinIO, operations API/worker/cleanup, SFT services and optional Collector, with
Collector configuration inline and hardening included. The temporary infra and
separate production override YAML files were consolidated/removed. Existing
legacy Compose files are preserved for compatibility. Python/JS/tests remain
separate because they are application code, not infrastructure configuration.

## Actual verification

- Operations/workflow suite: 26 discovered, **25 passed, 1 skipped**. The skipped
  NativeServerCase explicitly requires Temporal/PostgreSQL/S3 test endpoints.
- Existing SFT fast suite: **8 passed** in base_env.
- Four actual SFT ASGI modules import with a synthetic model URL configured;
  no model call was made. Quality service otherwise requires its configured URL.
- Python AST validation and dashboard JavaScript V8 syntax compilation passed.
  These are not browser/E2E tests.
- The consolidated Compose parses with synthetic validation-only environment
  values. No images were built and no containers/volumes were created.
- Existing SQLite monitoring baseline: 500k synthetic summaries improved from
  2.5829 s / 327,320 KiB peak-RSS growth to 0.5269 s / 4,224 KiB; a separate
  600-job comparison preserved all response fields. These earlier measurements
  do NOT establish the new PostgreSQL dashboard's 500k-book capacity.
- pytest, temporalio, multipart and OTLP packages are missing locally; package
  registry/Docker access remain restricted. No fake replacement SDK or passing
  pytest result is claimed. Existing hazm/numpy incompatibility was not changed.

## Remaining development and validation

1. Automatic reconciliation of a pause after an ambiguous interrupted activity
   is conservative/incomplete: it remains pause_requested instead of falsely
   acknowledging paused. Operator cancel is available; automatic repair is not.
2. Central retention does not yet coordinate downstream request-cache contents
   or Langfuse storage. Do not enable destructive production TTLs before closing
   this lifecycle gap. Metadata overrides/advanced retention policy application
   to existing artifacts also remain limited.
3. Full task-level drill-down beyond recorded raw chunk/attempt outputs, broad
   service-level performance aggregation and complete user-management UI are
   not finished. The current dashboard is functional code, not full original
   Task 2 acceptance coverage.
4. Native Temporal bindings, PostgreSQL locks/query plans, S3 semantics, actual
   Chunker/SFT control races, SDK/exporter compatibility, browser behavior and
   restart/E2E/load/security/backup recovery MUST be tested on isolated staging.
   No full mock-LLM Compose profile or real server test was executed locally.
5. Image builds and dependency resolution have not been validated. Review pinned
   versions for availability/security before server startup. A valid Compose file
   does not prove image compatibility, readiness or an HA architecture.

## Server handoff

Follow SERVER_SETUP_CHECKLIST.md in order. It includes exact staging commands,
required secrets/endpoints, NativeServerCase invocation, actual pipeline/crash
scenarios, Langfuse/OTLP setup, legacy cutover and backup/rollback checks.

Essential environment groups: database/storage credentials, OPS_SERVICE_TOKEN,
Temporal address/namespace/queue, OCR/model/service URLs, unchanged concurrency
limits, and optional LANGFUSE_*/OTEL_* settings. .env.staging.example contains
placeholders only. LANGFUSE_ENABLED and TRACE_CAPTURE_CONTENT default false.

No original cleaner, real book data, production endpoint or unrelated Docker
resource was changed. There are no task-created containers/images to remove.
