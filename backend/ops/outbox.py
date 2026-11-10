"""At-least-once Temporal delivery with command acknowledgements kept separate."""
import asyncio
import json
from sqlalchemy import select, and_, func
from .schema import commands, jobs, slots
from .runtime import Runtime


class Outbox:
    def __init__(self, store, transport, capacity=2):
        self.store, self.transport = store, transport
        self.capacity = capacity

    async def tick(self, limit=100):
        if not 1 <= limit <= 250:
            raise ValueError('invalid delivery batch')
        async with self.store.db.transaction() as conn:
            active = (await conn.execute(select(func.count()).select_from(slots))).scalar_one()
            starting = (await conn.execute(select(func.count()).select_from(commands.join(jobs,
                jobs.c.id == commands.c.job_id)).where(and_(commands.c.action == 'create',
                commands.c.status == 'delivered', jobs.c.status == 'queued')))).scalar_one()
            budget = max(0, self.capacity - active - starting)
            # Controls must never sit behind a large historical submission backlog.
            control_query = select(commands).where(and_(commands.c.status == 'pending',
                commands.c.action != 'create')).order_by(commands.c.created, commands.c.id).limit(limit)
            batch = [dict(row) for row in (await conn.execute(control_query)).mappings()]
            create_query = select(commands).where(and_(commands.c.status == 'pending',
                commands.c.action == 'create')).order_by(commands.c.created, commands.c.id).limit(min(budget, limit - len(batch)))
            batch += [dict(row) for row in (await conn.execute(create_query)).mappings()]
        delivered = 0
        for cmd in batch:
            if cmd['action'] == 'create':
                if budget <= 0:
                    continue
                budget -= 1
            await self.transport.deliver(cmd['job_id'], {'command_id': cmd['id'], 'action': cmd['action']})
            async with self.store.db.transaction() as conn:
                # A very fast worker may already have completed this command.
                await conn.execute(commands.update().where(and_(commands.c.id == cmd['id'],
                    commands.c.status == 'pending')).values(status='delivered',
                    result=json.dumps({'delivered_at': self.store.clock()})))
            delivered += 1
        return delivered


class TemporalTransport:
    def __init__(self, client, task_queue='book2sft-books', stage_queues=None):
        self.client, self.task_queue = client, task_queue
        self.stage_queues = dict(stage_queues or {})

    async def deliver(self, jid, message):
        from temporalio.common import WorkflowIDReusePolicy
        from .temporal_workflow import BookWorkflow
        await self.client.start_workflow(BookWorkflow.run, jid,
            self.stage_queues,
            id='book2sft/' + jid, task_queue=self.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            start_signal='wake', start_signal_args=[message])

    async def reconcile_slots(self, store):
        """Release only engine-confirmed terminal workflows, never an old heartbeat."""
        from .runtime import Runtime
        async with store.db.transaction() as conn:
            owners = (await conn.execute(select(slots.c.job_id).limit(1000))).scalars().all()
        for jid in owners:
            description = await self.client.get_workflow_handle('book2sft/' + jid).describe()
            if description.status.name in {'COMPLETED','FAILED','CANCELED','TERMINATED','TIMED_OUT'}:
                runtime = Runtime(store)
                await runtime.recover_claims(jid)
                await runtime.fail(jid, 'WorkflowClosedBeforeAcknowledgement')
                await runtime.release(jid)
