"""Bounded, read-only live pipeline snapshot for the original Book2SFT UI.

Stage counts come from claimed/running stage rows, not service worker capacity.
LLM requests are deliberately NOT inferred from chunk/attempt counts.
"""
import json
from collections import defaultdict

from sqlalchemy import func, select

from .schema import attempts, jobs, stages

STAGE_ORDER = ('ocr', 'chunker', 'sft', 'review', 'publish')
LIVE_STATUSES = ('running', 'starting', 'recovering', 'pause_requested', 'resume_requested')


def _safe_object(raw):
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(raw or '{}')
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def _current_stage(job, stage_rows):
    status = job['status']
    if status == 'queued':
        return 'queued', 'queued', None
    if status in ('paused', 'cancelled', 'archived', 'deleted'):
        return status, status, None
    if status == 'completed':
        return 'completed', 'completed', None

    active = [s for s in stage_rows if s['status'] == 'running']
    if active:
        choice = max(active, key=lambda s: s['updated'])
        return choice['name'], 'running', choice
    if status == 'failed':
        failed = [s for s in stage_rows if s['status'] == 'failed']
        choice = max(failed or stage_rows, key=lambda s: s['updated'], default=None)
        return (choice['name'] if choice else 'failed'), 'failed', choice
    completed = {s['name'] for s in stage_rows if s['status'] == 'completed'}
    next_stage = next((name for name in STAGE_ORDER if name not in completed), 'publish')
    latest = max(stage_rows, key=lambda s: s['updated'], default=None)
    # No stage lease yet, or one stage finished before the next was claimed.
    return next_stage, 'waiting_for_stage', latest


def _job_view(job, stage_rows, chunk_attempts):
    config = _safe_object(job['config'])
    stage, stage_state, chosen = _current_stage(job, stage_rows)
    snapshot = _safe_object(chosen['snapshot']) if chosen else {}
    progress = _safe_object(snapshot.get('job'))
    metrics = _safe_object(progress.get('metrics'))
    pipeline = _safe_object(metrics.get('pipeline'))
    sft = _safe_object(pipeline.get('sft'))
    chunker = _safe_object(pipeline.get('chunker'))
    ocr = _safe_object(pipeline.get('ocr'))
    return {
        'id': job['id'], 'title': job['title'], 'status': job['status'],
        'stage': stage, 'stage_state': stage_state,
        'input_type': config.get('input_type', 'unknown'),
        'ocr_skipped': config.get('input_type') == 'ocr_json',
        'progress': progress.get('progress'),
        'stage_progress': progress.get('stage_progress'),
        'substage': progress.get('stage'),
        'message': progress.get('message'),
        'downstream_job_id': progress.get('downstream_job_id'),
        'chunk_count': chunker.get('chunk_count'),
        'sft_chunks_total': sft.get('total_chunks'),
        'sft_chunks_completed': sft.get('completed_chunks'),
        'sft_chunks_active': sft.get('active_chunks'),
        'sft_chunks_failed': sft.get('failed_chunks'),
        'sft_pairs_generated': sft.get('generated_pairs'),
        'sft_pairs_quality_pass': sft.get('quality_pass_pairs'),
        'sft_chunk_attempts': chunk_attempts,
        'stage_last_update': chosen['updated'] if chosen else None,
        'updated': job['updated'],
        # A chunk attempt is a downstream SFT /run, not a model request.
        'llm_calls_per_book': None,
    }


async def live_snapshot(store, limit=50):
    if not 1 <= limit <= 100:
        raise ValueError('invalid live snapshot limit')
    async with store.db.transaction() as conn:
        status_rows = (await conn.execute(
            select(jobs.c.status, func.count()).group_by(jobs.c.status)
        )).all()
        stage_rows = (await conn.execute(
            select(stages.c.name, func.count(func.distinct(stages.c.job_id)))
            .select_from(stages.join(jobs, stages.c.job_id == jobs.c.id))
            .where(stages.c.status == 'running', jobs.c.status.in_(LIVE_STATUSES))
            .group_by(stages.c.name)
        )).all()
        # Fetch bounded recent jobs; full history is served by the existing
        # cursor-paginated explorer and is never fetched into Python here.
        rows = [dict(row) for row in (await conn.execute(
            select(jobs).order_by(jobs.c.created.desc(), jobs.c.id.desc()).limit(limit)
        )).mappings().all()]
        ids = [job['id'] for job in rows]
        stage_by_job = defaultdict(list)
        attempt_by_job = defaultdict(int)
        if ids:
            for row in (await conn.execute(
                select(stages).where(stages.c.job_id.in_(ids))
            )).mappings().all():
                stage_by_job[row['job_id']].append(dict(row))
            for jid, count in (await conn.execute(
                select(attempts.c.job_id, func.count())
                .where(attempts.c.job_id.in_(ids))
                .group_by(attempts.c.job_id)
            )).all():
                attempt_by_job[jid] = count

    statuses = {name: int(count) for name, count in status_rows}
    stage_counts = {name: 0 for name in STAGE_ORDER}
    for name, count in stage_rows:
        if name in stage_counts:
            stage_counts[name] += int(count)
    active = sum(statuses.get(s, 0) for s in LIVE_STATUSES)
    return {
        'at': store.clock(),
        'total_jobs': sum(statuses.values()),
        'statuses': statuses,
        'stages': stage_counts,
        'queued': statuses.get('queued', 0),
        'between_stages': max(0, active - sum(stage_counts.values())),
        'paused': statuses.get('paused', 0),
        'completed': statuses.get('completed', 0),
        'failed': statuses.get('failed', 0),
        'items': [_job_view(job, stage_by_job[job['id']], attempt_by_job[job['id']]) for job in rows],
        'limit': limit,
        'more_recent_jobs': sum(statuses.values()) > len(rows),
        'llm_per_book_supported': False,
        'note': 'Stage counts are running stage leases; SFT chunk attempts are not LLM calls.',
    }
