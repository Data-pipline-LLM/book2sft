"""Activities use existing SFT code through an adapter, never workflow-side I/O."""
import asyncio
from sqlalchemy import select, and_
from .runtime import Runtime
from .schema import stages, jobs, chunks
from .store import Conflict


class BookActivities:
    def __init__(self, store, adapter, capacity=2, heartbeat=lambda _: None):
        self.store, self.adapter, self.capacity = store, adapter, capacity
        self.runtime, self.pulse = Runtime(store), heartbeat

    async def sync(self, request):
        jid = request['job_id']
        job = await self.store.get(jid)
        pending = await self.runtime.pending(jid)
        if job['status'] == 'cancel_requested':
            if not await self.adapter.cancel(jid):
                return {'status': 'cancel_requested'}
            await self.runtime.settle_cancel(jid)
            for command in pending:
                if command['action'] in {'create', 'cancel'}:
                    await self.store.acknowledge(command['id'])
            await self.runtime.release(jid)
        elif job['status'] == 'pause_requested':
            # A lost activity's remote calls may still be settling. Never claim
            # paused merely because a lease-expired claim was reset to pending.
            from .schema import attempts
            async with self.store.db.transaction() as conn:
                uncertain = (await conn.execute(select(attempts.c.id).where(and_(
                    attempts.c.job_id == jid, attempts.c.epoch == job['epoch'],
                    attempts.c.status == 'interrupted')).limit(1))).first()
            if uncertain:
                return {'status':'pause_requested','reconciliation_required':True}
            # sync only runs when the stage activity reached a safe boundary.
            for command in pending:
                if command['action'] in {'create', 'pause'}:
                    await self.store.acknowledge(command['id'])
            await self.runtime.release(jid)
        elif job['status'] in {'queued', 'resume_requested', 'retrying', 'running'}:
            if not await self.runtime.acquire(jid, self.capacity):
                return {'status': 'queued'}
            for command in pending:
                if command['action'] in {'create', 'resume', 'retry', 'retry_chunk'}:
                    await self.store.acknowledge(command['id'])
        job = await self.store.get(jid)
        async with self.store.db.transaction() as conn:
            done = (await conn.execute(select(stages.c.name).where(and_(stages.c.job_id == jid,
                stages.c.status == 'completed')))).scalars().all()
        # Task 2's combined Chunker stage already performed OCR. This synthetic
        # completion marker keeps upgraded/retried jobs from OCR-ing twice.
        if 'chunker' in done and 'ocr' not in done:
            done.append('ocr')
        return {'status': job['status'], 'completed_stages': done}

    async def stage(self, request, *, retry_attempt=1, lease=None):
        jid, name = request['job_id'], request['stage']
        if retry_attempt > 1:
            await self.runtime.recover_claims(jid)
        old = await self.runtime.stage(jid, name)
        if old and old['status'] == 'completed':
            return {'state': 'completed', 'cached': True}
        state = (await self.store.get(jid))['status']
        if state != 'running':
            return {'state': state}
        if lease:
            await self.runtime.claim_stage(jid, name, lease)
        stop = asyncio.Event()
        async def beat():
            while not stop.is_set():
                self.pulse({'job_id': jid, 'stage': name})
                await asyncio.sleep(5)
        ticker = asyncio.create_task(beat())
        try:
            if request.get('legacy_combined'):
                result = await self.adapter.execute(jid, name, lease=lease, legacy_combined=True)
            else:
                result = await self.adapter.execute(jid, name, lease=lease) if lease else await self.adapter.execute(jid, name)
            return result
        finally:
            stop.set()
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)

    async def release(self, request):
        await self.runtime.release(request['job_id'])
        return {'released': True}

    async def failure(self, request):
        if (await self.store.get(request['job_id']))['status'] == 'running':
            await self.runtime.recover_claims(request['job_id'])
        await self.runtime.fail(request['job_id'], request['error_type'])
        return {'recorded': True}

    async def finish(self, request):
        # Publishing the book output and summary occurs inside the publish stage.
        jid = request['job_id']
        async with self.store.db.transaction() as conn:
            job = await self.store._job(conn, jid)
            if job['status'] == 'running':
                from .analytics import record_book
                await record_book(conn, self.store, job, 'completed')
                await self.store._state(conn, job, 'completed')
        return {'state': 'completed'}

    async def external_cancel(self, request):
        jid = request['job_id']
        job = await self.store.get(jid)
        if job['status'] not in {'completed', 'failed', 'cancelled', 'deleted', 'archived', 'cancel_requested'}:
            await self.store.command(jid, 'cancel', 'workflow', 'admin', 'workflow-cancel-' + str(job['epoch']))
        return await self.sync(request)

    def temporal_bindings(self, names=None):
        from temporalio import activity
        bound = []
        selected = names or ('sync', 'stage', 'release', 'failure', 'finish', 'external_cancel')
        for name in selected:
            method = getattr(self, name)
            def make(method, is_stage):
                async def execute(request: dict) -> dict:
                    if is_stage:
                        info = activity.info()
                        lease = f'{info.workflow_run_id}:{info.activity_id}:{info.attempt}'
                        return await method(request, retry_attempt=info.attempt, lease=lease)
                    return await method(request)
                return execute
            bound.append(activity.defn(name='book_' + name)(make(method, name == 'stage')))
        return bound
