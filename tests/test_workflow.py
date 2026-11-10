import asyncio
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from sqlalchemy import select, func
import test_ops as support
from backend.ops.activities import BookActivities
from backend.ops.runtime import Runtime
from backend.ops.workflow_logic import drive_book, LEGACY_STAGES
from backend.ops.outbox import Outbox
from backend.ops.schema import commands, chunks, stages, attempts, slots, jobs, audit
from backend.ops.analytics import Analytics
from backend.ops.metadata_retention import MetadataRetention


class WorkflowCase(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = support.OpsCase.asyncSetUp
    asyncTearDown = support.OpsCase.asyncTearDown
    new = support.OpsCase.new
    started = support.OpsCase.started
    chunk = support.OpsCase.chunk
    finish = support.OpsCase.finish

    async def test_mock_workflow_end_to_end_and_checkpoint(self):
        jid = await self.new()
        runtime = Runtime(self.store)
        calls = []
        test = self
        class Adapter:
            async def cancel(self, jid): return True
            async def execute(self, jid, stage):
                calls.append(stage)
                if stage == 'sft':
                    claim = await test.chunk(jid)
                    await test.finish(claim)
                await runtime.checkpoint(jid, stage, 'completed', {'files': {}})
                return {'state': 'completed'}
        activities = BookActivities(self.store, Adapter())
        async def call(name, payload): return await getattr(activities, name)(payload)
        async def wait(): self.fail('unexpected wait')
        self.assertEqual(await drive_book(jid, call, wait), 'completed')
        self.assertEqual(calls, ['ocr','chunker','sft','review','publish'])
        self.assertEqual(await drive_book(jid, call, wait), 'completed')
        self.assertEqual(len(calls), 5)
        self.assertEqual((await self.store.get(jid))['status'], 'completed')

    async def test_legacy_stage_sequence_remains_available_for_old_workflows(self):
        jid = await self.new()
        runtime = Runtime(self.store)
        calls = []
        class Adapter:
            async def cancel(self, unused): return True
            async def execute(self, current, stage):
                calls.append(stage)
                await runtime.checkpoint(current, stage, 'completed', {'files': {}})
                return {'state': 'completed'}
        activities = BookActivities(self.store, Adapter())
        async def call(name, payload): return await getattr(activities, name)(payload)
        async def wait(): self.fail('unexpected wait')
        self.assertEqual(
            await drive_book(jid, call, wait, stage_names=LEGACY_STAGES),
            'completed',
        )
        self.assertEqual(calls, list(LEGACY_STAGES))

    async def test_mock_pause_resume_and_slot_capacity(self):
        jid = await self.new()
        runtime = Runtime(self.store)
        paused = False
        test = self
        class Adapter:
            async def cancel(self, jid): return True
            async def execute(self, jid, stage):
                nonlocal paused
                if stage == 'sft' and not paused:
                    paused = True
                    await test.store.command(jid,'pause','op','operator','pause')
                    return {'state':'pause_requested'}
                await runtime.checkpoint(jid,stage,'completed',{})
                return {'state':'completed'}
        activities = BookActivities(self.store, Adapter(), capacity=1)
        async def call(name,payload):return await getattr(activities,name)(payload)
        async def wait():
            self.assertEqual((await self.store.get(jid))['status'],'paused')
            async with self.db.transaction() as conn:
                self.assertEqual((await conn.execute(select(func.count()).select_from(slots))).scalar_one(),0)
            await self.store.command(jid,'resume','op','operator','resume')
        self.assertEqual(await drive_book(jid,call,wait),'completed')
        first,second=await self.new('first'),await self.new('second')
        self.assertTrue(await runtime.acquire(first,1))
        self.assertFalse(await runtime.acquire(second,1))
        await runtime.release(first)
        self.assertTrue(await runtime.acquire(second,1))

    async def test_outbox_duplicate_transport_and_control_priority(self):
        first=await self.new('first')
        second=await self.new('second')
        sent=[]
        class Transport:
            async def deliver(self,jid,message):sent.append((jid,message))
        dispatcher=Outbox(self.store,Transport(),capacity=1)
        self.assertEqual(await dispatcher.tick(),1)
        self.assertEqual(await dispatcher.tick(),0)
        target = second if sent[0][0] == first else first
        await self.store.command(target,'pause','op','operator','pause-second')
        self.assertEqual(await dispatcher.tick(),1)
        self.assertEqual(sent[-1][1]['action'],'pause')
        self.assertEqual((await self.store.get(target))['status'],'pause_requested')
        async with self.db.transaction() as conn:
            self.assertEqual((await conn.execute(select(commands.c.status).where(commands.c.id==sent[-1][1]['command_id']))).scalar_one(),'delivered')

    async def test_rollup_receipt_and_bounded_series(self):
        jid=await self.started()
        claim=await self.chunk(jid)
        await self.finish(claim)
        analytics=Analytics(self.store)
        result={'generated_pairs_count':3,'quality_pass_count':2,'final_pairs':[]}
        await analytics.observe_chunk(jid,claim['attempt_id'],result,2.5)
        await analytics.observe_chunk(jid,claim['attempt_id'],result,2.5)
        series=await analytics.series(self.now-120,self.now+60,10)
        self.assertEqual(sum(p['calls'] for p in series['points']),1)
        self.assertEqual(sum(p['generated'] for p in series['points']),3)
        with self.assertRaises(ValueError):await analytics.series(0,99999999999)

    async def test_metadata_cleanup_protects_recovery_and_keeps_audit(self):
        jid=await self.started()
        await self.finish(await self.chunk(jid))
        await self.store.complete(jid)
        await self.retention.policy('job_metadata',1,True,'a','admin')
        self.now+=172800
        cleanup=MetadataRetention(self.store)
        self.assertEqual((await cleanup.preview())['job_metadata'],0)
        await self.store.command(jid,'disable_recovery','a','admin','disable')
        self.now+=172800
        rid=await self.retention.start('a','admin',False)
        self.assertEqual(await cleanup.batch(rid),1)
        self.assertEqual((await self.store.get(jid))['title'],'[expired]')
        self.assertEqual(await cleanup.batch(rid),0)
        async with self.db.transaction() as conn:
            self.assertGreater((await conn.execute(select(func.count()).select_from(audit))).scalar_one(),0)

    async def test_service_request_cache_cancel_and_interruption(self):
        import importlib.util
        from fastapi import HTTPException
        path=Path(__file__).resolve().parents[2]/'Data-construction-pipeline/services/ops_control.py'
        spec=importlib.util.spec_from_file_location('test_request_control',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        control=module.RequestControl(Path(self.temp.name)/'requests')
        request=SimpleNamespace(headers={'X-Operation-ID':'test-operation','X-Service-Token':'test-secret'})
        calls=0
        async def work():
            nonlocal calls
            calls+=1
            return {'result':'same'}
        with patch.dict('os.environ',{'OPS_SERVICE_TOKEN':'test-secret'}):
            self.assertEqual(await control.execute(request,{'text':'x'},work),{'result':'same'})
            self.assertEqual(await control.execute(request,{'text':'x'},work),{'result':'same'})
            self.assertEqual(calls,1)
            with self.assertRaises(HTTPException):await control.execute(request,{'text':'different'},work)
            control.cancel('before-submit')
            request.headers['X-Operation-ID']='before-submit'
            with self.assertRaises(HTTPException):await control.execute(request,{'text':'x'},work)
            self.assertEqual(calls,1)

    async def test_trace_disabled_and_redaction(self):
        from backend.ops.telemetry import preview, redact, traced_model
        with patch.dict('os.environ',{'TRACE_CAPTURE_CONTENT':'false'}):
            self.assertIsNone(preview({'prompt':'private'}))
        self.assertEqual(redact({'api_key':'secret','text':'allowed'})['api_key'],'[redacted]')
        class Model:
            def generate(self,*args,**kwargs):return (args,kwargs)
        model=traced_model(Model())
        self.assertEqual(model.generate('input',temperature=0.3),(('input',),{'temperature':0.3}))


@unittest.skipUnless(__import__('os').getenv('OPS_TEMPORAL_TEST_ADDRESS') and __import__('os').getenv('OPS_TEST_DATABASE_URL'),
                     'requires explicitly isolated Temporal/PostgreSQL/S3 server test environment')
class NativeServerCase(unittest.IsolatedAsyncioTestCase):
    async def test_native_workflow_postgres_and_s3(self):
        """Real infrastructure, synthetic activities; NOT a real SFT quality test."""
        import os, uuid, hashlib
        from sqlalchemy.engine import make_url
        from temporalio import activity
        from temporalio.client import Client
        from temporalio.worker import Worker
        from backend.ops.db import Database
        from backend.ops.store import Store
        from backend.ops.artifacts import S3Artifacts
        from backend.ops.retention import Retention
        from backend.ops.temporal_workflow import BookWorkflow
        from backend.ops.outbox import TemporalTransport
        dsn, bucket = os.environ['OPS_TEST_DATABASE_URL'], os.environ['OPS_TEST_S3_BUCKET']
        if not (make_url(dsn).database or '').endswith('_test') or not bucket.endswith('-test'):
            self.fail('requires database name ending _test and bucket ending -test')
        db = Database(dsn)
        await db.migrate()
        store = Store(db)
        objects = S3Artifacts(bucket, endpoint_url=os.environ['OPS_TEST_S3_ENDPOINT'])
        runtime, retention = Runtime(store), Retention(store, objects)
        jid = await store.create('SYNTHETIC TEMPORAL SERVER CONTRACT',{},'server-test','operator',str(uuid.uuid4()))
        class Adapter:
            async def cancel(self, unused): return True
            async def execute(self, job_id, stage, lease=None):
                if stage == 'sft':
                    await store.add_chunk(job_id,'synthetic',0,'synthetic test input')
                    claim = await store.begin_chunk(job_id,'synthetic','server-contract',stage_lease=lease)
                    raw = b'isolated synthetic infrastructure verification'
                    manifest = await asyncio.to_thread(objects.put,raw)
                    reference = await retention.register(job_id,'chunk_outputs',manifest)
                    self_bytes = await asyncio.to_thread(objects.get,manifest['key'])
                    if self_bytes != raw: raise AssertionError('S3 checksum round trip failed')
                    await store.finish_chunk(claim['attempt_id'],reference,manifest['sha256'])
                await runtime.checkpoint(job_id,stage,'completed',{},lease=lease)
                return {'state':'completed'}
        activities = BookActivities(store,Adapter(),heartbeat=activity.heartbeat)
        client = await Client.connect(os.environ['OPS_TEMPORAL_TEST_ADDRESS'])
        queue = 'book2sft-server-contract-' + uuid.uuid4().hex
        try:
            async with Worker(client,task_queue=queue,workflows=[BookWorkflow],activities=activities.temporal_bindings()):
                transport = TemporalTransport(client,queue)
                await transport.deliver(jid,{'action':'create'})
                result = await asyncio.wait_for(client.get_workflow_handle('book2sft/'+jid).result(),120)
                self.assertEqual(result,'completed')
                self.assertEqual((await store.get(jid))['status'],'completed')
                async with db.transaction() as conn:
                    self.assertEqual((await conn.execute(select(func.count()).select_from(attempts).where(attempts.c.job_id==jid))).scalar_one(),1)
        finally:
            await db.close()
