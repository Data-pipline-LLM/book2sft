"""Book workflow: compact stage activities, durable waits, bounded history."""
import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
with workflow.unsafe.imports_passed_through():
    from .workflow_logic import drive_book, STAGES, LEGACY_STAGES


@workflow.defn
class BookWorkflow:
    def __init__(self):
        self.woken = False

    @workflow.signal
    def wake(self, command):
        # No command list accumulates here; the transactional outbox is truth.
        self.woken = True

    @workflow.run
    async def run(
        self,
        job_id: str,
        stage_queues: dict | None = None,
        legacy_mode: bool | None = None,
    ) -> str:
        # Keep already-running Task 2 histories on their original task queue and
        # stage sequence. New runs use independent durable stage task queues.
        use_stage_queues = (
            workflow.patched('stage-task-queues-v1')
            if legacy_mode is None else not legacy_mode
        )
        async def call(operation, payload):
            is_stage = operation == 'stage'
            if is_stage and not use_stage_queues and payload.get('stage') == 'chunker':
                # Pre-Task-3.1 histories still execute the original combined
                # OCR+Chunker activity and must retain its idempotency key.
                payload = {**payload, 'legacy_combined': True}
            task_queue = None
            if is_stage and use_stage_queues and stage_queues:
                task_queue = stage_queues.get(payload.get('stage'))
            options = {'task_queue': task_queue} if task_queue else {}
            return await workflow.execute_activity(
                'book_' + operation, payload,
                start_to_close_timeout=timedelta(hours=24) if is_stage else timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30) if is_stage else None,
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2),
                    maximum_interval=timedelta(seconds=30), maximum_attempts=3),
                **options,
            )
        async def wait():
            if not self.woken:
                try:
                    await workflow.wait_condition(lambda: self.woken, timeout=timedelta(seconds=15))
                except asyncio.TimeoutError:
                    pass
            self.woken = False
        try:
            result = await drive_book(
                job_id, call, wait,
                stage_names=STAGES if use_stage_queues else LEGACY_STAGES,
            )
            if result == 'continue_as_new':
                # Carry the selected execution contract across Continue-As-New;
                # a fresh run has a fresh history and cannot infer the old patch.
                workflow.continue_as_new(args=[job_id, stage_queues, not use_stage_queues])
            return result
        except asyncio.CancelledError:
            # External cancellation is not an implicit successful business cancel.
            # Reconciliation remains visible until downstream acknowledgements.
            await asyncio.shield(call('external_cancel', {'job_id': job_id}))
            raise
