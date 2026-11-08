"""Separate Temporal worker and transactional outbox dispatcher process."""
import asyncio
import logging
import os
import socket
from .db import Database
from .store import Store
from .runtime import Runtime
from .artifacts import S3Artifacts
from .pipeline_adapter import PipelineAdapter
from .activities import BookActivities
from .outbox import Outbox, TemporalTransport
from .telemetry import configure


async def run():
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.worker import Worker
    from .temporal_workflow import BookWorkflow
    capacity = max(1, int(os.getenv('JOB_WORKERS', '2')))
    configure('book2sft-worker')
    db = Database(os.environ['OPS_DATABASE_URL'])
    store = Store(db)
    objects = S3Artifacts(os.environ['OPS_S3_BUCKET'], endpoint_url=os.environ.get('OPS_S3_ENDPOINT') or None)
    client = await Client.connect(os.environ.get('TEMPORAL_ADDRESS', 'temporal:7233'),
        namespace=os.environ.get('TEMPORAL_NAMESPACE', 'default'))
    queue = os.environ.get('TEMPORAL_TASK_QUEUE', 'book2sft-books')
    activities = BookActivities(store, PipelineAdapter(store, objects), capacity, heartbeat=activity.heartbeat)
    transport = TemporalTransport(client, queue)
    dispatcher = Outbox(store, transport, capacity=capacity)
    async def upkeep():
        while True:
            try:
                await Runtime(store).heartbeat(socket.gethostname(), 'book-worker', capacity)
                await transport.reconcile_slots(store)
                await dispatcher.tick()
            except Exception as exc:
                logging.getLogger(__name__).error('Dispatcher retry: %s', type(exc).__name__)
            await asyncio.sleep(2)
    upkeep_task = asyncio.create_task(upkeep())
    try:
        async with Worker(client, task_queue=queue, workflows=[BookWorkflow],
                          activities=activities.temporal_bindings(),
                          max_concurrent_activities=capacity + 4):
            await asyncio.Event().wait()
    finally:
        upkeep_task.cancel()
        await asyncio.gather(upkeep_task, return_exceptions=True)
        await db.close()


if __name__ == '__main__':
    asyncio.run(run())
