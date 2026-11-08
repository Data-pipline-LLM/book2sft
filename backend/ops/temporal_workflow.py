"""Book workflow: compact stage activities, durable waits, bounded history."""
import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
with workflow.unsafe.imports_passed_through():
    from .workflow_logic import drive_book


@workflow.defn
class BookWorkflow:
    def __init__(self):
        self.woken = False

    @workflow.signal
    def wake(self, command):
        # No command list accumulates here; the transactional outbox is truth.
        self.woken = True

    @workflow.run
    async def run(self, job_id: str) -> str:
        async def call(operation, payload):
            is_stage = operation == 'stage'
            return await workflow.execute_activity(
                'book_' + operation, payload,
                start_to_close_timeout=timedelta(hours=24) if is_stage else timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30) if is_stage else None,
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2),
                    maximum_interval=timedelta(seconds=30), maximum_attempts=3))
        async def wait():
            if not self.woken:
                try:
                    await workflow.wait_condition(lambda: self.woken, timeout=timedelta(seconds=15))
                except asyncio.TimeoutError:
                    pass
            self.woken = False
        try:
            result = await drive_book(job_id, call, wait)
            if result == 'continue_as_new':
                workflow.continue_as_new(job_id)
            return result
        except asyncio.CancelledError:
            # External cancellation is not an implicit successful business cancel.
            # Reconciliation remains visible until downstream acknowledgements.
            await asyncio.shield(call('external_cancel', {'job_id': job_id}))
            raise
