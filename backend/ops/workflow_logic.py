"""SDK-independent deterministic orchestration policy, exercised by mock tests.

Only activity results determine state. Sleep/wait/activity calls are injected by
Temporal; this module does not read files, clocks, random generators or a DB.
"""
STAGES = ('chunker', 'sft', 'review', 'publish')
TERMINAL = {'completed', 'failed', 'cancelled', 'archived', 'deleted'}


async def drive_book(job_id, call, wait, *, max_cycles=100):
    for _ in range(max_cycles):
        snapshot = await call('sync', {'job_id': job_id})
        status = snapshot['status']
        if status in TERMINAL:
            await call('release', {'job_id': job_id})
            return status
        if status != 'running':
            await wait()
            continue
        completed = set(snapshot.get('completed_stages', []))
        stage = next((name for name in STAGES if name not in completed), None)
        if stage is None:
            await call('finish', {'job_id': job_id})
            continue
        try:
            outcome = await call('stage', {'job_id': job_id, 'stage': stage})
        except Exception as exc:
            await call('failure', {'job_id': job_id, 'error_type': type(exc).__name__})
            continue
        if outcome['state'] in {'pause_requested', 'paused', 'cancel_requested'}:
            continue
        if outcome['state'] == 'failed':
            await call('failure', {'job_id': job_id, 'error_type': outcome.get('error_type', 'StageFailed')})
    return 'continue_as_new'
