"""Catch Temporal Python SDK multi-argument call regressions without a server."""
import importlib
import sys
import types
import unittest
from contextlib import nullcontext
from unittest.mock import patch

from backend.ops.outbox import TemporalTransport


class TemporalArgsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_transport_delivers_multi_arg_workflow_using_args_keyword(self):
        temporalio = types.ModuleType('temporalio')
        common = types.ModuleType('temporalio.common')
        common.WorkflowIDReusePolicy = types.SimpleNamespace(ALLOW_DUPLICATE='allow_duplicate')
        wf = types.ModuleType('backend.ops.temporal_workflow')
        wf.BookWorkflow = type('BookWorkflow', (), {'run': lambda self, *a: None})
        received = []

        class StrictClient:
            async def start_workflow(self, workflow, arg=None, *, args=(), id,
                                     task_queue, id_reuse_policy, start_signal,
                                     start_signal_args):
                if arg is not None:
                    raise AssertionError('Second positional argument is not allowed')
                received.append(dict(workflow=workflow, args=args, id=id,
                                     task_queue=task_queue, signal=start_signal,
                                     signal_args=start_signal_args))

        with patch.dict(sys.modules, {
            'temporalio': temporalio,
            'temporalio.common': common,
            'backend.ops.temporal_workflow': wf,
        }):
            transport = TemporalTransport(StrictClient(), 'books-queue',
                                          {'ocr': 'ocr-queue'})
            message = {'command_id': 'cmd-123', 'action': 'create'}
            await transport.deliver('job-123', message)

        self.assertEqual(received[0]['args'], ['job-123', {'ocr': 'ocr-queue'}])
        self.assertEqual(received[0]['id'], 'book2sft/job-123')
        self.assertEqual(received[0]['signal'], 'wake')
        self.assertEqual(received[0]['signal_args'], [message])

    async def test_continue_as_new_uses_args_keyword(self):
        temporalio = types.ModuleType('temporalio')
        common = types.ModuleType('temporalio.common')
        common.RetryPolicy = lambda **kwargs: None
        captured = []
        sentinel = object()

        class Continued(BaseException):
            pass

        def continue_as_new(arg=sentinel, *, args=()):
            if arg is not sentinel:
                raise AssertionError('Unexpected positional argument')
            captured.append(list(args))
            raise Continued()

        workflow = types.SimpleNamespace(
            unsafe=types.SimpleNamespace(imports_passed_through=lambda: nullcontext()),
            defn=lambda cls: cls,
            signal=lambda fn: fn,
            run=lambda fn: fn,
            patched=lambda _: True,
            continue_as_new=continue_as_new,
        )
        temporalio.workflow = workflow

        with patch.dict(sys.modules, {
            'temporalio': temporalio,
            'temporalio.common': common,
        }):
            # No Temporal instance needed: force the workflow's rollover branch.
            sys.modules.pop('backend.ops.temporal_workflow', None)
            from backend.ops import temporal_workflow as module

            async def return_rollover(*args, **kwargs):
                return 'continue_as_new'

            with patch.object(module, 'drive_book', return_rollover):
                with self.assertRaises(Continued):
                    await module.BookWorkflow().run('job-123', {'ocr': 'ocr-queue'})
            sys.modules.pop('backend.ops.temporal_workflow', None)

        self.assertEqual(captured, [['job-123', {'ocr': 'ocr-queue'}, False]])


if __name__ == '__main__':
    unittest.main()
