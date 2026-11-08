# Current retention behavior

The new retention module is independent of legacy generation and the unknown
historical three-day server cleaner. No server cleanup was inspected or changed.

Policies are disabled by default. Supported artifact labels: temporary_files,
job_metadata, chunk_inputs, chunk_outputs, task_details, error_logs,
monitoring_events, generated_artifacts, completed_datasets. These are manifest
categories: metadata/events tables do NOT yet have automatic row TTL deletion.
Policies apply to future artifact registrations only, with explicit UI/API text.
Changing existing artifact expiry and global/batch/job overrides are not implemented.

An artifact is eligible only after its stored expiry, with a terminal job whose
recovery has been explicitly disabled by an admin. All active and resumable jobs,
including paused jobs, are protected regardless of category or required flag.
Cleanup never deletes job summaries, attempts or audit records. Soft deletion
also requires recovery to be disabled and a terminal job.

Preview aggregates count/bytes without touching objects. Dry-run runs never
delete files. Cleanup run status and pause/resume/cancel commands are persisted.
The separate worker processes at most ten runs per tick and 100 artifacts per
batch, with a maximum configurable batch bound of 250. Scheduling is a persisted
UTC interval (60 seconds..31 days), disabled until admin enables it. Cron syntax,
timezone selection and schedule UI remain unimplemented.

Each actual deletion locks the run, job and manifest and rechecks recoverability.
All job lifecycle changes use the same job lock. Successful object deletion and
expiry/audit counters are committed together in the DB transaction. If a process
crashes after object deletion but before database commit, idempotent object deletion
is retried and counted once on commit. Dry-run preview does not constitute a
promise that later counts cannot change.

Objects use immutable generated UUID/SHA256 keys. A key is globally unique in
manifest records, preventing accidental shared-object deletion. Local test store
uses directory file descriptors and O_NOFOLLOW to prevent symlink parent traversal.
Production S3 uses create-only object publication; bucket IAM must prohibit
uncontrolled overwrites. S3 deletion, locking races under real PostgreSQL and
worker crash recovery remain unverified because infrastructure is unavailable.
No automatic cleanup is connected to existing data/jobs folders.

Continuation: MetadataRetention now expires selected job/task/error/event details
for eligible terminal, explicitly nonresumable jobs, preserving IDs/counts/audit.
The worker processes this before artifact cleanup. Preview reports categories.
Downstream request-cache and Langfuse lifecycle coordination remain unimplemented;
production destructive TTLs must remain disabled until that gap is closed.
