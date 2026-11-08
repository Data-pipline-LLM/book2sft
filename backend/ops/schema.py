"""Versioned operational schema shared by PostgreSQL and SQLite contract tests."""
from sqlalchemy import (MetaData, Table, Column as C, String as S, Integer as I,
                        Float as F, Text as T, Index, ForeignKey, UniqueConstraint)

metadata = MetaData()
versions = Table('ops_schema_versions', metadata, C('version', I, primary_key=True))
jobs = Table('ops_jobs', metadata,
    C('id', S(36), primary_key=True), C('book_id', S(36), nullable=False),
    C('title', S(512), nullable=False), C('status', S(32), nullable=False),
    C('created', F, nullable=False), C('updated', F, nullable=False),
    C('revision', I, nullable=False), C('epoch', I, nullable=False),
    C('attempt', I, nullable=False), C('resumable', I, nullable=False),
    C('config', T, nullable=False))
Index('ops_jobs_status_created', jobs.c.status, jobs.c.created, jobs.c.id)
Index('ops_jobs_book', jobs.c.book_id, jobs.c.created)
commands = Table('ops_commands', metadata,
    C('id', S(36), primary_key=True), C('job_id', ForeignKey('ops_jobs.id'), nullable=False),
    C('actor', S(128), nullable=False), C('key', S(128), nullable=False),
    C('action', S(32), nullable=False), C('payload', T, nullable=False),
    C('status', S(32), nullable=False), C('created', F, nullable=False),
    C('completed', F), C('result', T),
    UniqueConstraint('actor', 'key', name='ops_command_idempotency'))
Index('ops_commands_delivery', commands.c.status, commands.c.created)
chunks = Table('ops_chunks', metadata,
    C('job_id', ForeignKey('ops_jobs.id'), primary_key=True),
    C('id', S(128), primary_key=True), C('ordinal', I, nullable=False),
    C('status', S(32), nullable=False), C('attempt', I, nullable=False),
    C('input_ref', T, nullable=False), C('output_ref', T), C('output_hash', S(64)),
    C('updated', F, nullable=False), UniqueConstraint('job_id', 'ordinal'))
attempts = Table('ops_attempts', metadata,
    C('id', S(36), primary_key=True), C('job_id', ForeignKey('ops_jobs.id'), nullable=False),
    C('chunk_id', S(128), nullable=False), C('number', I, nullable=False),
    C('epoch', I, nullable=False), C('worker', S(128), nullable=False),
    C('status', S(32), nullable=False), C('started', F, nullable=False), C('ended', F),
    C('trace_id', S(32), nullable=False), C('error', T), C('model', S(256)),
    C('quality', F), C('input_ref', T), C('output_ref', T),
    UniqueConstraint('job_id', 'chunk_id', 'number'))
Index('ops_attempts_job', attempts.c.job_id, attempts.c.started, attempts.c.id)
events = Table('ops_events', metadata,
    C('id', S(36), primary_key=True), C('job_id', ForeignKey('ops_jobs.id'), nullable=False),
    C('at', F, nullable=False), C('kind', S(64), nullable=False), C('detail', T, nullable=False))
Index('ops_events_job', events.c.job_id, events.c.at, events.c.id)
audit = Table('ops_audit', metadata,
    C('id', S(36), primary_key=True), C('actor', S(128), nullable=False),
    C('at', F, nullable=False), C('action', S(64), nullable=False),
    C('target', S(128), nullable=False), C('detail', T, nullable=False))
Index('ops_audit_at', audit.c.at, audit.c.id)
artifacts = Table('ops_artifacts', metadata,
    C('id', S(36), primary_key=True), C('job_id', ForeignKey('ops_jobs.id'), nullable=False),
    C('category', S(64), nullable=False), C('key', T, nullable=False),
    C('sha256', S(64), nullable=False), C('size', I, nullable=False),
    C('created', F, nullable=False), C('expires', F), C('required', I, nullable=False),
    C('status', S(32), nullable=False), UniqueConstraint('key', name='ops_artifact_key'))
Index('ops_artifacts_expiry', artifacts.c.status, artifacts.c.expires, artifacts.c.id)
policies = Table('ops_retention_policies', metadata,
    C('category', S(64), primary_key=True), C('days', I, nullable=False),
    C('enabled', I, nullable=False), C('updated', F, nullable=False))
runs = Table('ops_retention_runs', metadata,
    C('id', S(36), primary_key=True), C('status', S(32), nullable=False),
    C('actor', S(128), nullable=False), C('dry_run', I, nullable=False),
    C('created', F, nullable=False), C('updated', F, nullable=False),
    C('deleted', I, nullable=False), C('bytes', I, nullable=False), C('error', T))
users = Table('ops_users', metadata,
    C('id', S(128), primary_key=True), C('role', S(16), nullable=False),
    C('password_hash', T, nullable=False), C('enabled', I, nullable=False))
sessions = Table('ops_sessions', metadata,
    C('token_hash', S(64), primary_key=True), C('user_id', ForeignKey('ops_users.id'), nullable=False),
    C('expires', F, nullable=False))
auth_limits = Table('ops_auth_limits', metadata,
    C('identity', S(64), primary_key=True), C('count', I, nullable=False),
    C('reset_at', F, nullable=False))
settings = Table('ops_settings', metadata,
    C('id', S(64), primary_key=True), C('value', T, nullable=False),
    C('updated', F, nullable=False))

# Migration 2: workflow stage checkpoints, fleet heartbeats and analytics rollups.
stages = Table('ops_stages', metadata,
    C('job_id', ForeignKey('ops_jobs.id'), primary_key=True), C('name', S(64), primary_key=True),
    C('status', S(32), nullable=False), C('snapshot', T, nullable=False),
    C('lease', S(256), nullable=False, server_default=''),
    C('started', F), C('ended', F), C('updated', F, nullable=False))
slots = Table('ops_book_slots', metadata,
    C('job_id', ForeignKey('ops_jobs.id'), primary_key=True), C('acquired', F, nullable=False))
workers = Table('ops_workers', metadata,
    C('id', S(128), primary_key=True), C('kind', S(64), nullable=False),
    C('heartbeat', F, nullable=False), C('capacity', I, nullable=False), C('detail', T, nullable=False))
rollups = Table('ops_metric_rollups', metadata,
    C('bucket', I, primary_key=True), C('service', S(64), primary_key=True),
    C('stage', S(64), primary_key=True), C('calls', I, nullable=False),
    C('errors', I, nullable=False), C('retries', I, nullable=False),
    C('generated', I, nullable=False), C('accepted', I, nullable=False),
    C('duration_sum', F, nullable=False), C('duration_hist', T, nullable=False),
    C('quality_sum', F, nullable=False), C('quality_count', I, nullable=False),
    C('reasons', T, nullable=False))
Index('ops_rollups_window', rollups.c.bucket, rollups.c.service, rollups.c.stage)
metric_receipts = Table('ops_metric_receipts', metadata,
    C('attempt_id', ForeignKey('ops_attempts.id'), primary_key=True), C('at', F, nullable=False))
metadata_expiry = Table('ops_metadata_expiry', metadata,
    C('job_id', ForeignKey('ops_jobs.id'), primary_key=True), C('category', S(64), primary_key=True),
    C('at', F, nullable=False))
Index('ops_events_time', events.c.at, events.c.id)
Index('ops_jobs_created_cursor', jobs.c.created, jobs.c.id)
books = Table('ops_books', metadata,
    C('id', S(36), primary_key=True), C('title', S(512), nullable=False), C('created', F, nullable=False))
Index('ops_books_created', books.c.created, books.c.id)
rejections = Table('ops_rejection_rollups', metadata,
    C('bucket', I, primary_key=True), C('reason', S(80), primary_key=True), C('count', I, nullable=False))
book_rollups = Table('ops_book_rollups', metadata,
    C('bucket', I, primary_key=True), C('completed', I, nullable=False),
    C('failed', I, nullable=False), C('lead_time_sum', F, nullable=False))
book_receipts = Table('ops_book_receipts', metadata,
    C('job_id', ForeignKey('ops_jobs.id'), primary_key=True), C('attempt', I, primary_key=True),
    C('status', S(32), primary_key=True))
