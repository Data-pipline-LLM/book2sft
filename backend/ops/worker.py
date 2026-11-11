"""Temporal workflow dispatcher with independently capacity-limited stage queues."""
import asyncio
import logging
import os
import socket
import time
from contextlib import AsyncExitStack

from .db import Database
from .store import Store
from .runtime import Runtime
from .artifacts import S3Artifacts
from .pipeline_adapter import PipelineAdapter
from .activities import BookActivities
from .outbox import Outbox, TemporalTransport
from .telemetry import configure


def _positive_int(name, default, maximum=10000):
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(1, min(maximum, value))


async def run():
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.worker import Worker
    from .temporal_workflow import BookWorkflow

    # Workflow admission is deliberately separate from per-stage execution
    # capacity. A book may wait between stages without occupying an OCR/SFT slot.
    workflow_capacity = _positive_int('BOOK_WORKFLOW_CAPACITY', 1000, 1000)
    legacy_activity_capacity = _positive_int('LEGACY_ACTIVITY_WORKERS', 16, 1000)
    configure('book2sft-worker')

    stage_config = {
        'ocr': (
            os.getenv('TEMPORAL_OCR_TASK_QUEUE', 'book2sft-ocr'),
            _positive_int('OCR_STAGE_WORKERS', 16, 2000),
        ),
        'chunker': (
            os.getenv('TEMPORAL_CHUNKER_TASK_QUEUE', 'book2sft-chunker'),
            _positive_int('CHUNKER_STAGE_WORKERS', 16, 2000),
        ),
        'sft': (
            os.getenv('TEMPORAL_SFT_TASK_QUEUE', 'book2sft-sft'),
            _positive_int('SFT_STAGE_WORKERS', 16, 2000),
        ),
        'review': (
            os.getenv('TEMPORAL_REVIEW_TASK_QUEUE', 'book2sft-review'),
            _positive_int('REVIEW_STAGE_WORKERS', 4, 1000),
        ),
        'publish': (
            os.getenv('TEMPORAL_PUBLISH_TASK_QUEUE', 'book2sft-publish'),
            _positive_int('PUBLISH_STAGE_WORKERS', 4, 1000),
        ),
    }
    queue_names = [name for name, _ in stage_config.values()]
    workflow_queue = os.environ.get('TEMPORAL_TASK_QUEUE', 'book2sft-books')
    if len(queue_names) != len(set(queue_names)) or workflow_queue in queue_names:
        raise RuntimeError('Workflow and pipeline stages must use distinct Temporal task queues')

    db = Database(os.environ['OPS_DATABASE_URL'])
    store = Store(db)
    objects = S3Artifacts(
        os.environ['OPS_S3_BUCKET'],
        endpoint_url=os.environ.get('OPS_S3_ENDPOINT') or None,
    )
    client = await Client.connect(
        os.environ.get('TEMPORAL_ADDRESS', 'temporal:7233'),
        namespace=os.environ.get('TEMPORAL_NAMESPACE', 'default'),
    )
    stage_queues = {stage: queue for stage, (queue, _) in stage_config.items()}

    activities = BookActivities(
        store,
        PipelineAdapter(store, objects),
        workflow_capacity,
        heartbeat=activity.heartbeat,
    )
    transport = TemporalTransport(client, workflow_queue, stage_queues)
    dispatcher = Outbox(store, transport, capacity=workflow_capacity)
    hostname = socket.gethostname()

    async def upkeep():
        last_reconcile = time.monotonic()
        while True:
            try:
                runtime = Runtime(store)
                await runtime.heartbeat(
                    hostname, 'book-worker', workflow_capacity,
                    {'task_queue': workflow_queue},
                )
                for stage, (task_queue, capacity) in stage_config.items():
                    await runtime.heartbeat(
                        f'{hostname}:{stage}', 'book-stage', capacity,
                        {'stage': stage, 'task_queue': task_queue},
                    )
                # Workflow descriptions are an expensive Temporal RPC. With a
                # 512-book admission window, checking every slot every 2s would
                # create needless control-plane traffic. Dispatch and heartbeats
                # stay frequent; terminal reconciliation runs every 60s.
                now = time.monotonic()
                if now - last_reconcile >= 60.0:
                    await transport.reconcile_slots(store)
                    last_reconcile = time.monotonic()
                await dispatcher.tick()
            except Exception:
                # Keep the traceback: an exception class alone cannot identify
                # whether dispatch, heartbeat or reconciliation has failed.
                logging.getLogger(__name__).exception('Dispatcher/upkeep retry')
            await asyncio.sleep(2)

    upkeep_task = asyncio.create_task(upkeep())
    workers = [
        Worker(
            client,
            task_queue=workflow_queue,
            workflows=[BookWorkflow],
            # Keep stage registered here for already-running pre-v1 workflows.
            activities=activities.temporal_bindings(),
            max_concurrent_activities=legacy_activity_capacity,
        )
    ]
    for stage, (task_queue, capacity) in stage_config.items():
        workers.append(
            Worker(
                client,
                task_queue=task_queue,
                activities=activities.temporal_bindings(names=('stage',)),
                max_concurrent_activities=capacity,
            )
        )

    try:
        async with AsyncExitStack() as stack:
            for temporal_worker in workers:
                await stack.enter_async_context(temporal_worker)
            await asyncio.Event().wait()
    finally:
        upkeep_task.cancel()
        await asyncio.gather(upkeep_task, return_exceptions=True)
        await db.close()


if __name__ == '__main__':
    asyncio.run(run())
