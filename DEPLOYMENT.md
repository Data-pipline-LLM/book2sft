# Future deployment and rollback (not executed)

STATUS: NOT READY FOR DEPLOYMENT. The new state/control modules are isolated from
the live book pipeline. Do not route production job submissions to them yet.

Before deployment, complete the Temporal proof of concept and downstream control
contracts, run PostgreSQL/S3 integration and migration tests, preserve an approved
SFT behavior/performance baseline, implement/authenticate the dashboard and fully
validate trace/retention/load/security behavior from the original Task 2 document.

Production must separate web API and workers; use private PostgreSQL, S3 and
Temporal endpoints. Configure credentials via environment/secret manager, TLS,
reverse-proxy trust, rate limiting, backup recovery and key rotation. No account,
host, port or production secret is discovered or contacted by these files.
Local Compose localhost bindings and volumes do not establish HA or server sizing.

## Additive migration / rollback boundary

The initial operational migration uses new ops_* tables, version marker 1 and
PostgreSQL advisory serialization. Review backend/ops/migrations/001_initial.sql;
the CLI applies the equivalent metadata schema. It never changes the legacy
SQLite or job.json layout. Later schema changes require explicit numbered
migrations, not reliance on create_all to alter existing tables.

No automatic legacy job migration has been implemented. Before routing traffic,
implement a streaming importer with source hashes/child IDs/checkpoint validation,
verify counts and artifact availability, and keep old data available for rollback.
A current running legacy job cannot simply be declared resumable from its summary.

For code rollback of the changes delivered so far, stop the separate operations
API/cleanup process and run the previous application version. Retain the new DB,
objects and migration marker for review; do not DROP tables or delete volumes as
a rollback shortcut. Legacy generation code is unchanged. Before switching any
future durable executor back, drain and reconcile its child jobs and leases to
prevent duplicate execution.

## Storage / cleaner migration

Identify the actual old server cleanup service using read-only commands such as
systemctl list-timers --all and crontab -l on the intended host. Inspect referenced
scripts, root paths and ownership. No specific process was identified here and the
historical PID must not be reused. After verified import, previews and explicit
owner approval, disable the identified old cleaner before enabling new cleanup
on overlapping data. No SSH, service change or server cleanup was performed.

## Version compatibility sources

Review current official contracts before installing/pinning the missing stack:

- [Temporal Python messages](https://docs.temporal.io/develop/python/workflows/message-passing)
- [Temporal error handling](https://docs.temporal.io/develop/python/best-practices/error-handling)
- [Langfuse SDK compatibility](https://langfuse.com/docs/observability/sdk/overview)
- [OpenTelemetry Python exporters](https://opentelemetry.io/docs/languages/python/exporters/)

Langfuse server and SDK compatibility must be checked together; no server image
or SDK version is selected or installed by this incomplete integration. The
infra Compose deliberately requires explicit reviewed image references rather
than pulling latest. A successful config parse is not a verified image pull.
