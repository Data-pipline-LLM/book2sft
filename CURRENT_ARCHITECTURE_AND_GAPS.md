# Task 2: current architecture and gaps

Audit date: 2026-11-08. Status: NOT READY. This is a source audit, not a claim
that production services or server configuration were inspected.

Continuation note: this baseline is retained as the pre-change audit. Current
implementation/verification is in TASK_2_FINAL_REPORT.md. In base_env, eight
existing SFT fast tests now pass. Legacy Overview now uses SQL aggregation.
The legacy in-memory queue/reconciliation identifier set remain. New independent
ops modules are not yet connected to production job execution.

## Source baseline

| Repository | Branch | HEAD | Initial working tree |
| --- | --- | --- | --- |
| book2sft | main | 42dfa04 | clean |
| Chunker_Book | main | 2f343ff | clean |
| Data-construction-pipeline | feature/pipeline-edits | 38599a1 | clean |

Task 1's existing SFT_TASK1_REVIEW.md still says NOT READY. Its original baseline
was d88c88e; the current commit includes the refactor. Full behavioral, quality,
throughput and concurrency approval has not been established by this audit.

## Current execution

Book2SFT backend/main.py accepts PDF/OCR uploads at POST /v1/jobs, writes an
input file and job.json, and adds the ID to asyncio.Queue. JobManager runs
workers inside the API process. It submits to Chunker, polls status, retrieves
chunks, calls the generation orchestrator's /run per chunk, then
/select_instruction_book, and generates final artifacts/review PDF.

Chunker_Book/api/main.py runs jobs in a ThreadPoolExecutor and launches the
semantic chunker using subprocess.Popen. It exposes job/status/events/download
routes. Control of this subprocess must be included in any cancellation design.

The generation Compose file launches services.orchestrator.app:app through
Uvicorn. This is the configured service entry point on the current branch;
the running production entry point cannot be confirmed without server access.
Assessment, generation, quality and selection dependencies must preserve their
existing payloads, prompts, decisions and concurrency. Translation/DPO logic is
out of scope, including shared client behavior.

## Source defaults (not deployed measurements)

Book2SFT: 2 book workers, 4 concurrent chunks per book, pair concurrency 2.
Chunker: 1 job worker, LLM concurrency 16, clamped to 1–64. Task 1 reports a
process-wide generator semaphore of 64 and target concurrency 4. These defaults
must be captured alongside resolved runtime configuration in later benchmarks.

## Confirmed gaps and integration boundaries

| Gap | Evidence | Proposed connection |
| --- | --- | --- |
| Queue/active execution is process-local | JobManager.jobs and asyncio.Queue | Temporal worker separate from API; PostgreSQL projection/outbox |
| Restart does not resume execution | MonitorIndex.reconcile marks orphan running/queued jobs interrupted | persisted child IDs, reconciliation and committed checkpoints |
| Overview materializes matching history | MonitorIndex.overview fetchall and Python aggregation | SQL aggregates and bounded minute/hour/day rollups |
| Reconciliation retains historical identifiers | existing dictionary and seen set across all indexed jobs | bounded database reconciliation batches |
| Monitoring and execution truth are separate | SQLite summary versus job.json/worker state | transactional command/state model and reconciliation |
| Existing listing uses offset pagination | MonitorIndex list query | cursor pagination over stable indexed keys |
| Control is not durable across services | existing routes do not implement required lifecycle | command IDs, acknowledgements, downstream cooperative controls |
| Artifacts live in local job directories | DATA_ROOT, downloads, events.jsonl | manifest + S3 references + protected checkpoints |
| Core infrastructure absent from current Compose | single book2sft service | independently runnable core and optional observability profiles |

Current SSE queues are capped at 200 events, but total subscribers, replay,
timeouts and disconnect handling still need validation against Task 2.
Terminal jobs are trimmed from the in-memory registry to 200 in reconciliation;
this does not make the active queue or historical reconciliation bounded.

## Cleanup and real data

The inspected API code removes rejected upload directories with rmtree. These
paths are validation cleanup, not evidence of the historical three-day cleaner.
No historical cleanup source was identified in the inspected service code.
Cron/systemd and the historical PID have not been inspected or acted upon.
No real input, output, log, SQLite database or job directory was changed.

## Local verification and blockers

- docker info: denied access to /var/run/docker.sock. Running containers and
  service availability cannot be established through Docker in this session.
- Python lacks fastapi/httpx/tqdm/temporalio/psycopg/boto3.
- Installing existing pinned Book2SFT requirements in /tmp/sft-task1-venv fails
  with package-index DNS resolution errors. No dependency workaround/stubs used.
- Task 1 unittest discovery fails at import: ModuleNotFoundError: tqdm.
- Host reports 12 logical CPUs, about 15.3 GiB RAM and 38 GiB free workspace
  disk. Future server hardware and target request load remain unspecified.

The standalone tools/task2/benchmark_monitoring.py exercises existing SQLite
monitoring with bounded synthetic seeding in a temporary directory. It validates
summary counts and measures cold/cached overview and Linux peak RSS. It does
not establish end-to-end readiness for 500,000 books.
