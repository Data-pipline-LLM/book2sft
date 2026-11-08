# Implemented operations API (partial Task 2)

Factory: backend.ops.api.create_app; separate production factory:
backend.ops.main.configured_app. Existing Book2SFT API compatibility is unchanged.
The new API does not yet import or execute legacy jobs.

Authentication: POST /v1/auth/login {user,password}, returning a one-hour bearer
token; POST /v1/auth/logout revokes it. Tokens are random and stored as SHA256.
Password hashes use salted PBKDF2-SHA256 (600,000 rounds). Roles are fetched from
the DB per request. Disabled users and expired/revoked sessions are rejected.
Login is limited persistently to ten requests per source per minute, with four
concurrent password verifications per API process. Reverse-proxy client identity
configuration and multi-process abuse/load verification are still outstanding.

| Endpoint | Minimum role | Behavior |
| --- | --- | --- |
| GET /health | public | liveness only |
| GET /ready | public | schema/database/executor readiness; 503 while executor unavailable |
| POST /v1/auth/login | public | sign in; generic credential failure |
| GET /v1/ops/overview | viewer | DB aggregate counts; resources/quality explicitly unavailable |
| GET /v1/ops/jobs | viewer | status filter and cursor pagination; config omitted |
| GET /v1/ops/jobs/{id} | viewer | configuration hidden from viewer |
| POST /v1/ops/jobs | operator | durable creation intent; currently 503 without executor |
| POST /v1/ops/jobs/{id}/{action} | operator/admin | durable command acceptance, not completion |
| DELETE /v1/ops/jobs/{id} | admin | terminal, recovery-disabled soft delete |
| GET /v1/ops/jobs/{id}/chunks | operator | bounded chunk states and references |
| GET /v1/ops/jobs/{id}/attempts | operator | attempts/errors/worker/trace/model/quality fields |
| GET /v1/ops/jobs/{id}/events | viewer | bounded state change records |
| GET /v1/ops/jobs/{id}/artifacts | operator | bounded artifact manifests |
| GET /v1/ops/artifacts/{id} | operator | attachment <=8 MiB; 410 if expired |
| GET /v1/ops/commands/{id} | operator | persisted status/result |
| GET/PUT /v1/admin/retention/policies | admin | future artifact policies |
| PUT /v1/admin/retention/schedule | admin | enabled + interval_seconds |
| POST /v1/admin/retention/preview | admin | aggregate eligibility/bytes/impact |
| POST /v1/admin/retention/runs | admin | durable dry-run/live run intent |
| GET /v1/admin/retention/runs | admin | cleanup history |
| POST /v1/admin/retention/runs/{id}/{pause,resume,cancel} | admin | durable run control |
| GET /v1/admin/audit | admin | bounded audit records |

Pagination uses limit 1..200 and next_cursor, with chunk ordinal or entity primary
ID as the cursor. This is not a live-event replay protocol. Jobs are currently
ordered by ID, not creation date. Full text search, date/batch/stage filters, bulk
commands, stages/tasks API, CSV export, SSE replay and metric/chart queries remain
unimplemented.

Job actions: pause, resume, cancel, retry, retry_chunk (body {chunk_id}), archive,
disable_recovery (admin). All mutations need Idempotency-Key except cleanup
configuration/runs, which currently do not have a full idempotency-key contract.
Same actor/key/request returns the prior command; conflicting reuse gives 409.
409 also reports invalid transitions or missing safe-boundary acknowledgement.
Validation errors are 422, missing entities 404, forbidden roles 403, missing
bearer tokens 401. Execution commands return 503 before acceptance when the
workflow executor is unavailable. Cleanup controls are separate from generation.

The store's acknowledge/recover/publication methods are worker-internal methods,
not client endpoints. Acknowledge requires no active chunk for pause/cancel.
Cancellation fences late results. Successful publication requires a registered
available artifact with matching job and hash. Completed checkpoints are reused;
failed retries create new attempt IDs. A crash-recovery call is legal only after
Temporal confirms the prior activity lease has ended; that integration remains
unimplemented. Directly calling these trusted methods is not proof that remote
Chunker/LLM cancellation has occurred.

## Current continuation (supersedes the initial partial-executor notes above)

Temporal bindings/outbox/worker and the production adapter now exist; real-server
verification is pending. The configured application accepts durable intents while
execution infrastructure recovers; /ready separately probes schema/bucket/Temporal.
/v1/auth/me supplies the current role. Job queries support title/ID search,
book/status/date filters and creation-time cursors. Job detail includes stages;
chunk detail, errors, analytics/rollups, services, bounded bulk operations and
compatible authenticated /v1/jobs uploads/status/events/downloads are implemented.
Artifact responses stream, with optional short-lived S3 download links. Cancel
supersedes pending pause/resume. See the current report for remaining gaps; do not
interpret this API surface as completed full Task 2 acceptance.
