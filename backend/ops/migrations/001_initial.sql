-- Initial operational schema; generated from backend/ops/schema.py.

-- Legacy SQLite and job files are never modified by this migration.

BEGIN;

SELECT pg_advisory_xact_lock(2074251201);


CREATE TABLE IF NOT EXISTS ops_audit (
	id VARCHAR(36) NOT NULL, 
	actor VARCHAR(128) NOT NULL, 
	at FLOAT NOT NULL, 
	action VARCHAR(64) NOT NULL, 
	target VARCHAR(128) NOT NULL, 
	detail TEXT NOT NULL, 
	PRIMARY KEY (id)
)

;

CREATE INDEX IF NOT EXISTS ops_audit_at ON ops_audit (at, id);


CREATE TABLE IF NOT EXISTS ops_auth_limits (
	identity VARCHAR(64) NOT NULL, 
	count INTEGER NOT NULL, 
	reset_at FLOAT NOT NULL, 
	PRIMARY KEY (identity)
)

;


CREATE TABLE IF NOT EXISTS ops_jobs (
	id VARCHAR(36) NOT NULL, 
	book_id VARCHAR(36) NOT NULL, 
	title VARCHAR(512) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	created FLOAT NOT NULL, 
	updated FLOAT NOT NULL, 
	revision INTEGER NOT NULL, 
	epoch INTEGER NOT NULL, 
	attempt INTEGER NOT NULL, 
	resumable INTEGER NOT NULL, 
	config TEXT NOT NULL, 
	PRIMARY KEY (id)
)

;

CREATE INDEX IF NOT EXISTS ops_jobs_book ON ops_jobs (book_id, created);

CREATE INDEX IF NOT EXISTS ops_jobs_status_created ON ops_jobs (status, created, id);


CREATE TABLE IF NOT EXISTS ops_retention_policies (
	category VARCHAR(64) NOT NULL, 
	days INTEGER NOT NULL, 
	enabled INTEGER NOT NULL, 
	updated FLOAT NOT NULL, 
	PRIMARY KEY (category)
)

;


CREATE TABLE IF NOT EXISTS ops_retention_runs (
	id VARCHAR(36) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	actor VARCHAR(128) NOT NULL, 
	dry_run INTEGER NOT NULL, 
	created FLOAT NOT NULL, 
	updated FLOAT NOT NULL, 
	deleted INTEGER NOT NULL, 
	bytes INTEGER NOT NULL, 
	error TEXT, 
	PRIMARY KEY (id)
)

;


CREATE TABLE IF NOT EXISTS ops_schema_versions (
	version SERIAL NOT NULL, 
	PRIMARY KEY (version)
)

;


CREATE TABLE IF NOT EXISTS ops_settings (
	id VARCHAR(64) NOT NULL, 
	value TEXT NOT NULL, 
	updated FLOAT NOT NULL, 
	PRIMARY KEY (id)
)

;


CREATE TABLE IF NOT EXISTS ops_users (
	id VARCHAR(128) NOT NULL, 
	role VARCHAR(16) NOT NULL, 
	password_hash TEXT NOT NULL, 
	enabled INTEGER NOT NULL, 
	PRIMARY KEY (id)
)

;


CREATE TABLE IF NOT EXISTS ops_artifacts (
	id VARCHAR(36) NOT NULL, 
	job_id VARCHAR(36) NOT NULL, 
	category VARCHAR(64) NOT NULL, 
	key TEXT NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	size INTEGER NOT NULL, 
	created FLOAT NOT NULL, 
	expires FLOAT, 
	required INTEGER NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ops_artifact_key UNIQUE (key), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;

CREATE INDEX IF NOT EXISTS ops_artifacts_expiry ON ops_artifacts (status, expires, id);


CREATE TABLE IF NOT EXISTS ops_attempts (
	id VARCHAR(36) NOT NULL, 
	job_id VARCHAR(36) NOT NULL, 
	chunk_id VARCHAR(128) NOT NULL, 
	number INTEGER NOT NULL, 
	epoch INTEGER NOT NULL, 
	worker VARCHAR(128) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	started FLOAT NOT NULL, 
	ended FLOAT, 
	trace_id VARCHAR(32) NOT NULL, 
	error TEXT, 
	model VARCHAR(256), 
	quality FLOAT, 
	input_ref TEXT, 
	output_ref TEXT, 
	PRIMARY KEY (id), 
	UNIQUE (job_id, chunk_id, number), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;

CREATE INDEX IF NOT EXISTS ops_attempts_job ON ops_attempts (job_id, started, id);


CREATE TABLE IF NOT EXISTS ops_chunks (
	job_id VARCHAR(36) NOT NULL, 
	id VARCHAR(128) NOT NULL, 
	ordinal INTEGER NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	attempt INTEGER NOT NULL, 
	input_ref TEXT NOT NULL, 
	output_ref TEXT, 
	output_hash VARCHAR(64), 
	updated FLOAT NOT NULL, 
	PRIMARY KEY (job_id, id), 
	UNIQUE (job_id, ordinal), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;


CREATE TABLE IF NOT EXISTS ops_commands (
	id VARCHAR(36) NOT NULL, 
	job_id VARCHAR(36) NOT NULL, 
	actor VARCHAR(128) NOT NULL, 
	key VARCHAR(128) NOT NULL, 
	action VARCHAR(32) NOT NULL, 
	payload TEXT NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	created FLOAT NOT NULL, 
	completed FLOAT, 
	result TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT ops_command_idempotency UNIQUE (actor, key), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;

CREATE INDEX IF NOT EXISTS ops_commands_delivery ON ops_commands (status, created);


CREATE TABLE IF NOT EXISTS ops_events (
	id VARCHAR(36) NOT NULL, 
	job_id VARCHAR(36) NOT NULL, 
	at FLOAT NOT NULL, 
	kind VARCHAR(64) NOT NULL, 
	detail TEXT NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;

CREATE INDEX IF NOT EXISTS ops_events_job ON ops_events (job_id, at, id);


CREATE TABLE IF NOT EXISTS ops_sessions (
	token_hash VARCHAR(64) NOT NULL, 
	user_id VARCHAR(128) NOT NULL, 
	expires FLOAT NOT NULL, 
	PRIMARY KEY (token_hash), 
	FOREIGN KEY(user_id) REFERENCES ops_users (id)
)

;

INSERT INTO ops_schema_versions(version) VALUES (1) ON CONFLICT DO NOTHING;

COMMIT;

-- Migration 2: durable workflow integration

BEGIN;

SELECT pg_advisory_xact_lock(2074251201);


CREATE TABLE IF NOT EXISTS ops_book_rollups (
	bucket SERIAL NOT NULL, 
	completed INTEGER NOT NULL, 
	failed INTEGER NOT NULL, 
	lead_time_sum FLOAT NOT NULL, 
	PRIMARY KEY (bucket)
)

;


CREATE TABLE IF NOT EXISTS ops_books (
	id VARCHAR(36) NOT NULL, 
	title VARCHAR(512) NOT NULL, 
	created FLOAT NOT NULL, 
	PRIMARY KEY (id)
)

;


CREATE TABLE IF NOT EXISTS ops_metric_rollups (
	bucket INTEGER NOT NULL, 
	service VARCHAR(64) NOT NULL, 
	stage VARCHAR(64) NOT NULL, 
	calls INTEGER NOT NULL, 
	errors INTEGER NOT NULL, 
	retries INTEGER NOT NULL, 
	generated INTEGER NOT NULL, 
	accepted INTEGER NOT NULL, 
	duration_sum FLOAT NOT NULL, 
	duration_hist TEXT NOT NULL, 
	quality_sum FLOAT NOT NULL, 
	quality_count INTEGER NOT NULL, 
	reasons TEXT NOT NULL, 
	PRIMARY KEY (bucket, service, stage)
)

;


CREATE TABLE IF NOT EXISTS ops_rejection_rollups (
	bucket INTEGER NOT NULL, 
	reason VARCHAR(80) NOT NULL, 
	count INTEGER NOT NULL, 
	PRIMARY KEY (bucket, reason)
)

;


CREATE TABLE IF NOT EXISTS ops_workers (
	id VARCHAR(128) NOT NULL, 
	kind VARCHAR(64) NOT NULL, 
	heartbeat FLOAT NOT NULL, 
	capacity INTEGER NOT NULL, 
	detail TEXT NOT NULL, 
	PRIMARY KEY (id)
)

;


CREATE TABLE IF NOT EXISTS ops_book_receipts (
	job_id VARCHAR(36) NOT NULL, 
	attempt INTEGER NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	PRIMARY KEY (job_id, attempt, status), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;


CREATE TABLE IF NOT EXISTS ops_book_slots (
	job_id VARCHAR(36) NOT NULL, 
	acquired FLOAT NOT NULL, 
	PRIMARY KEY (job_id), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;


CREATE TABLE IF NOT EXISTS ops_metadata_expiry (
	job_id VARCHAR(36) NOT NULL, 
	category VARCHAR(64) NOT NULL, 
	at FLOAT NOT NULL, 
	PRIMARY KEY (job_id, category), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;


CREATE TABLE IF NOT EXISTS ops_stages (
	job_id VARCHAR(36) NOT NULL, 
	name VARCHAR(64) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	snapshot TEXT NOT NULL, 
	lease VARCHAR(256) DEFAULT '' NOT NULL, 
	started FLOAT, 
	ended FLOAT, 
	updated FLOAT NOT NULL, 
	PRIMARY KEY (job_id, name), 
	FOREIGN KEY(job_id) REFERENCES ops_jobs (id)
)

;


CREATE TABLE IF NOT EXISTS ops_metric_receipts (
	attempt_id VARCHAR(36) NOT NULL, 
	at FLOAT NOT NULL, 
	PRIMARY KEY (attempt_id), 
	FOREIGN KEY(attempt_id) REFERENCES ops_attempts (id)
)

;

CREATE INDEX IF NOT EXISTS ops_audit_at ON ops_audit (at, id);

CREATE INDEX IF NOT EXISTS ops_books_created ON ops_books (created, id);

CREATE INDEX IF NOT EXISTS ops_jobs_book ON ops_jobs (book_id, created);

CREATE INDEX IF NOT EXISTS ops_jobs_created_cursor ON ops_jobs (created, id);

CREATE INDEX IF NOT EXISTS ops_jobs_status_created ON ops_jobs (status, created, id);

CREATE INDEX IF NOT EXISTS ops_rollups_window ON ops_metric_rollups (bucket, service, stage);

CREATE INDEX IF NOT EXISTS ops_artifacts_expiry ON ops_artifacts (status, expires, id);

CREATE INDEX IF NOT EXISTS ops_attempts_job ON ops_attempts (job_id, started, id);

CREATE INDEX IF NOT EXISTS ops_commands_delivery ON ops_commands (status, created);

CREATE INDEX IF NOT EXISTS ops_events_job ON ops_events (job_id, at, id);

CREATE INDEX IF NOT EXISTS ops_events_time ON ops_events (at, id);

INSERT INTO ops_books(id,title,created) SELECT j.book_id, MIN(j.title), MIN(j.created) FROM ops_jobs j WHERE NOT EXISTS (SELECT 1 FROM ops_books b WHERE b.id=j.book_id) GROUP BY j.book_id;

INSERT INTO ops_schema_versions(version) VALUES (2) ON CONFLICT DO NOTHING;

COMMIT;
