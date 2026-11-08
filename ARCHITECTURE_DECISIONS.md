# Task 2 architecture decisions

Status: partially implemented foundations; infrastructure proof of concept has not passed.

Continuation: the operations schema, transactional state repository, auth/API,
artifact interfaces, retention scheduler/worker and portable metadata backup are
implemented independently. SQLite is used only by contract tests; the production
database constructor requires PostgreSQL/asyncpg. There is no SQL/SQLite execution
scheduler replacing Temporal, and no real book executor is connected yet. See
TASK_2_FINAL_REPORT.md for the current implementation/verification matrix.
Missing infrastructure does not count as a passed phase gate.

1. Keep Book2SFT as the operations API/UI. Separate API lifetime from worker
   execution. Preserve existing job downloads and historical reads during migration.
2. Use Temporal as the default execution engine. No PostgreSQL queue substitution
   has been approved or justified. Before integration, demonstrate safe-boundary
   pause, resume, cancellation, heartbeat, crash recovery and idempotent retry.
3. Use PostgreSQL for operational entities, commands, audit and query projections;
   S3/MinIO for bulky artifacts. Retain existing SQLite and files until migration
   reconciliation and owner review. Do not put full OCR/LLM bodies in hot rows.
4. Persist command intent transactionally with an outbox; reconcile delivery and
   worker acknowledgements. Temporal and PostgreSQL cannot share one atomic
   transaction. UI status must distinguish request acceptance from completion.
5. Give each book, job, stage, chunk, task and attempt its own identity. Publish
   outputs atomically with idempotency and fencing against late/cancelled results.
   A completed chunk checkpoint includes artifact hash/reference and configuration
   version. Activity success alone is not sufficient evidence of committed output.
6. Start with a workflow per book and bounded stage/chunk batches; choose final
   granularity and Continue-As-New thresholds after history/load measurements.
   Preserve effective book/chunk/pair concurrency and shared generator limits.
7. Keep generation logic unchanged. Introduce correlation and cooperative control
   at service boundaries only after regression baselines pass. Cancellation of an
   HTTP client does not prove the remote LLM stopped; track acknowledgement and
   reject late publication until downstream work is reconciled.
8. Observability is optional and bounded. Operational truth must work while
   Langfuse/Grafana are down. Verify current official versions/licenses and SDK
   compatibility when network access exists; no versions are guessed here.
9. Cleanup starts in dry-run, references artifact manifests, and protects active
   and resumable jobs. No inferred permission to delete existing data or replace
   an unknown server cleanup scheduler.

## Execution gates

0. Establish approved Task 1 behavior/quality/concurrency baseline, runnable local
   dependencies, Docker access, and architecture/gap report.
1. Pass isolated Temporal lifecycle proof with synthetic work; then implement
   PostgreSQL migrations, manifests, object storage and historical import.
2. Pass durable control and downstream acknowledgement tests, including crash,
   duplicate delivery, contradictory commands and late results.
3. Add bounded telemetry and verify equal-concurrency behavior and performance.
4. Build authenticated API/UI against real durable state and paginated queries.
5. Implement retention policy/preview/control with race, path and recovery tests.
6. Execute full mock integration/security/migration/backup/load tests; document
   deployment and rollback without deploying.

Each gate requires a real end-to-end scenario. Static files or mock-only controls
cannot mark a gate complete. The current environment blocks gates 0 and 1.

## Provisional local performance budgets

These are test targets, not measured claims or server commitments: at 500,000
book metadata records, warm list/detail p95 <= 500 ms, overview p95 <= 1 s,
at 5 concurrent dashboard readers; web steady RSS <= 512 MiB and historical
growth from 10k to 500k <= 64 MiB. Test page size 100 and bounded chart points.
Require generation throughput loss <= 5% with telemetry at equal concurrency;
measure CPU, RSS, latency and variability across repeated runs before accepting.
Owner/server-specific load and budgets still require agreement. An existing-index
single-process benchmark is only a baseline, not this acceptance workload.
