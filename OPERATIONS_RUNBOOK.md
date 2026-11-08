# Operations runbook: implemented subset

Read TASK_2_FINAL_REPORT.md first. This is not an end-to-end production runbook.

## Inspect a job

Authenticate, list jobs with a bounded cursor, retrieve job/chunks/attempts and
command history. The new operational database currently contains only jobs
created through its store, not automatically imported legacy jobs. A returned
202 means a command was accepted; only a completed command acknowledgement is
proof of the store's transition. No production workflow adapter is installed.
Never manually acknowledge a production cancellation to make the UI look stopped.

## Recovery

Preserve checkpoints and input configuration. Store.recover fences stale attempts,
marks running attempts interrupted and resets unfinished chunks. It must only be
invoked after the execution engine has established the previous lease is dead.
Do not invoke it while a remote child might still publish without reconciliation.
Completed chunk checkpoints are retained. Real Temporal/Chunker/SFT recovery
and orphaned-child handling remain implementation work.

## Retention

Use admin policies and preview first. Run cleanup in dry-run mode. Review count,
bytes and resumability protection. Disabling recovery is a separate audited admin
command. Cleanup can be paused/resumed/cancelled without controlling generation.
A failed run records an error class; inspect it rather than retrying unknown paths.
No cleanup of existing job directories is configured.

## Backup and empty-database restore

With private OPS_DATABASE_URL exported and working database:

```sh
conda run -n base_env --no-capture-output python -m backend.ops.cli backup private.ops-backup.jsonl
conda run -n base_env --no-capture-output python -m backend.ops.cli restore-empty private.ops-backup.jsonl
```

Use a distinct empty database for restore. The restore command refuses existing
operational rows and validates the streamed checksum/count in one transaction.
Backup files are created exclusively with mode 0600, never overwritten. Sessions
and temporary auth-rate rows are excluded. This portable backup includes hashes
and references, not object bytes, PostgreSQL roles, engine history or secrets.
Back up the S3 bucket and Temporal persistence separately, with consistent
checkpoint/recovery windows. PostgreSQL-native pg_dump/pg_restore and object
restore have not been exercised in this session. SQLite contract tests passed
round-trip restore and corruption rollback.

## Sandbox diagnosis

Run docker info and docker ps in the host terminal. If they succeed there and
fail only in this task, adjust the task's granted Docker access in the host
execution settings. Host group changes alone cannot repair a restricted mount or
socket syscall policy. DNS failure is confirmed with getent hosts pypi.org and
curl --head https://pypi.org/simple/temporalio/. No proxy variables are configured.
Do not change /etc/resolv.conf or make the Docker socket world-writable.
