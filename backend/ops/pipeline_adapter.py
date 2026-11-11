"""Bridge durable execution to the unchanged generation/selection implementation.

SDK imports are isolated in worker.py. This module restores real artifact copies
for each activity; local scratch state is never the recovery source of truth.
"""
import asyncio
import dataclasses
import hashlib
import json
import os
import tempfile
from pathlib import Path
from contextlib import asynccontextmanager
import httpx
from sqlalchemy import select, and_
from .schema import chunks, artifacts, stages
from .runtime import Runtime
from .retention import Retention
from .store import Conflict, Missing, encoded


class PipelineAdapter:
    def __init__(self, store, objects):
        self.store, self.objects = store, objects
        self.runtime, self.retention = Runtime(store), Retention(store, objects)

    async def put(self, jid, category, data, required=True):
        manifest = await asyncio.to_thread(self.objects.put, data)
        reference = await self.retention.register(jid, category, manifest, required)
        return reference, manifest

    async def read(self, reference):
        async with self.store.db.transaction() as conn:
            row = (await conn.execute(select(artifacts).where(artifacts.c.id == reference))).mappings().first()
        if not row or row['status'] != 'available':
            raise Missing('required artifact unavailable')
        return await asyncio.to_thread(self.objects.get, row['key'])

    async def execute(self, jid, stage, lease=None, legacy_combined=False):
        # Import the actual deployed implementation only in the worker process.
        from .. import main as legacy
        adapter, runtime, store = self, self.runtime, self.store
        record = await store.get(jid)
        config = json.loads(record['config'])
        snapshots = []
        stage_order = ('chunker', 'sft', 'review', 'publish') if legacy_combined else (
            'ocr', 'chunker', 'sft', 'review', 'publish'
        )
        for name in stage_order:
            row = await runtime.stage(jid, name)
            if row and row['status'] != 'pending':
                snapshots.append(json.loads(row['snapshot']))
            if name == stage:
                break
        files, data = {}, {}
        for snapshot in snapshots:
            files.update(snapshot.get('files', {}))
            data.update(snapshot.get('job', {}))
        args = dict(job_id=jid, filename=config.get('filename', 'book.pdf'), size_bytes=config.get('size_bytes', 0),
            input_type=config.get('input_type', 'pdf'), book_title=record['title'],
            category=config.get('category', ''), domain=config.get('domain', legacy.SFT_DEFAULT_DOMAIN))
        allowed = {field.name for field in dataclasses.fields(legacy.JobState)}
        args.update({key: value for key, value in data.items() if key in allowed and key not in {'job_id'}})
        job = legacy.JobState(**args)
        current_stage = await runtime.stage(jid, stage)
        if stage in {'ocr', 'chunker'}:
            current_snapshot = json.loads(current_stage['snapshot']) if current_stage else {}
            job.downstream_job_id = (current_snapshot.get('job') or {}).get('downstream_job_id')
        job.status, job.error = 'running', None
        job.metrics.setdefault('pipeline', {'chunker': {}, 'sft': {}, 'pdf': {}})
        in_flight, all_tasks = set(), set()

        class Runner(legacy.JobManager):
            resume_children = True

            def __init__(self, root):
                # Do not create/launch the legacy queue, worker or SQLite index.
                self.root = root
                self.limit = asyncio.Semaphore(legacy.SFT_CONCURRENCY)
                self.last_saved = 0
                self.last_child = None

            def job_dir(self, unused):
                return self.root

            async def persist(self, current):
                now = store.clock()
                if now - self.last_saved >= 1 or current.downstream_job_id != self.last_child or current.status in {'failed', 'completed'}:
                    await runtime.checkpoint(jid, stage, 'running', {'job': current.as_dict(), 'files': files}, lease=lease)
                    self.last_saved, self.last_child = now, current.downstream_job_id

            async def emit(self, *args, **kwargs):
                return None

            def sft_request_headers(self, current, index):
                from .telemetry import propagation_headers
                return {**propagation_headers(), 'X-Service-Token': os.environ.get('OPS_SERVICE_TOKEN', ''),
                    'X-Operation-ID': f'{jid}:{record["attempt"]}:{index}',
                    'X-Book-ID': record['book_id'], 'X-Job-ID': jid}

            def chunker_request_headers(self, current):
                child_stage = getattr(current, '_ops_chunker_stage', 'chunker')
                input_sha256 = getattr(
                    current,
                    '_ops_chunker_input_sha256',
                    config.get('input_object', {}).get('sha256', ''),
                )
                operation_id = (
                    f'chunker:{jid}:{record["attempt"]}'
                    if getattr(current, '_ops_legacy_combined_chunker', False)
                    else f'chunker:{jid}:{record["attempt"]}:{child_stage}'
                )
                return {'X-Service-Token': os.environ.get('OPS_SERVICE_TOKEN', ''),
                        'X-Operation-ID': operation_id,
                        'X-Input-SHA256': input_sha256,
                        'X-Book-ID': record['book_id'], 'X-Job-ID': jid}

            async def run_ocr(self, current):
                """Run only OCR; the next Temporal stage owns semantic chunking."""
                input_path = self.root / 'input' / 'book.pdf'
                await self.set_stage(
                    current, 'ocr', message='Submitting PDF to the OCR service', local_progress=0,
                )
                timeout = httpx.Timeout(legacy.CHUNKER_TIMEOUT_SEC, connect=30.0)
                headers = self.chunker_request_headers(current)
                headers['X-Chunker-Mode'] = 'ocr-only'
                async with httpx.AsyncClient(
                    timeout=timeout,
                    headers={'X-Service-Token': headers['X-Service-Token']},
                ) as client:
                    downstream_job_id = current.downstream_job_id if self.resume_children else None
                    submit = {}
                    if not downstream_job_id:
                        with input_path.open('rb') as stream:
                            response = await client.post(
                                f'{legacy.CHUNKER_BASE_URL}/v1/jobs',
                                headers=headers,
                                files={'pdf_file': (current.filename, stream, 'application/pdf')},
                            )
                        if response.status_code >= 400:
                            await self.fail(current, 'OCR_SUBMIT_FAILED', response.text)
                            return
                        submit = response.json()
                        downstream_job_id = submit.get('job_id')
                    if not downstream_job_id:
                        await self.fail(current, 'OCR_BAD_RESPONSE', json.dumps(submit, ensure_ascii=False))
                        return

                    current.downstream_job_id = downstream_job_id
                    current.metrics.setdefault('pipeline', {}).setdefault('ocr', {}).update({
                        'job_id': downstream_job_id,
                        'status': 'queued',
                    })
                    await self.persist(current)

                    while True:
                        status_response = await client.get(
                            f'{legacy.CHUNKER_BASE_URL}/v1/jobs/{downstream_job_id}'
                        )
                        if status_response.status_code >= 400:
                            await self.fail(current, 'OCR_STATUS_FAILED', status_response.text)
                            return
                        downstream = status_response.json()
                        child_status = str(downstream.get('status') or 'queued').lower()
                        try:
                            progress = float(downstream.get('progress') or 0.0)
                        except (TypeError, ValueError):
                            progress = 0.0
                        current.metrics['pipeline']['ocr'].update({
                            'status': child_status,
                            'stage': downstream.get('stage'),
                            'progress': progress,
                            'updated_at': legacy.utcnow(),
                        })
                        await self.set_stage(
                            current, 'ocr',
                            message=str(downstream.get('message') or f'OCR: {downstream.get("stage", "queued")}'),
                            downstream_progress=progress,
                        )
                        if child_status == 'failed':
                            await self.fail(current, 'OCR_FAILED', 'Downstream OCR job failed', downstream=downstream)
                            return
                        if child_status == 'cancelled':
                            raise RuntimeError('Downstream OCR job was cancelled before the Book2SFT cancel command')
                        if child_status == 'completed':
                            break
                        await asyncio.sleep(legacy.POLL_INTERVAL_SEC)

                    artifacts_dir = self.root / 'artifacts'
                    artifacts_dir.mkdir(parents=True, exist_ok=True)
                    if not await self.download_chunker_artifact(
                        client, downstream_job_id, 'ocr.json', artifacts_dir / 'ocr.json'
                    ):
                        await self.fail(current, 'OCR_ARTIFACT_MISSING', 'OCR completed but ocr.json was not available')
                        return
                    current.metrics['pipeline']['ocr'].update({
                        'status': 'completed',
                        'progress': 100,
                        'ocr_skipped': False,
                    })
                    await self.set_stage(current, 'ocr', message='OCR completed; handing off to Chunker', local_progress=100)
                    await self.finish_stage(current, 'ocr')
                    await self.persist(current)

            async def run_one_sft_chunk(self, client, semaphore, current, source, index):
                task = asyncio.current_task()
                all_tasks.add(task)
                cid = legacy.first_text(source[index].get('id'), source[index].get('original_id'), f'chunk_{index:06d}')
                try:
                    async with store.db.transaction() as conn:
                        row = (await conn.execute(select(chunks).where(and_(chunks.c.job_id == jid, chunks.c.id == cid)))).mappings().one()
                    if row['status'] in {'completed', 'failed'} and row['output_ref']:
                        cached = json.loads(await adapter.read(row['output_ref']))
                        return index, cached, cached.get('_book2sft', {}).get('elapsed_sec', 0)
                    async with self.limit:
                        while (await store.get(jid))['status'] != 'running':
                            await asyncio.sleep(0.2)
                        claim = await store.begin_chunk(jid, cid, os.environ.get('HOSTNAME', 'worker'), stage_lease=lease)
                        in_flight.add(cid)
                        @asynccontextmanager
                        async def already_acquired():
                            yield
                        try:
                            from .telemetry import span, correlation
                            import uuid
                            identity = {'book_id': record['book_id'], 'job_id': jid, 'chunk_id': cid,
                                        'task_id': cid, 'attempt_id': claim['attempt_id']}
                            context_token = correlation.set(identity)
                            try:
                                with span('book.chunk', attributes=identity, carrier={
                                    'traceparent': f'00-{claim["trace_id"]}-{uuid.uuid4().hex[:16]}-01'}):
                                    result_index, result, elapsed = await super().run_one_sft_chunk(
                                        client, already_acquired(), current, source, index)
                            finally:
                                correlation.reset(context_token)
                            ref, manifest = await adapter.put(jid, 'chunk_outputs', encoded(result).encode())
                            await store.finish_chunk(claim['attempt_id'], ref, manifest['sha256'],
                                error=result if result.get('error') else None)
                            from .analytics import Analytics
                            await Analytics(store).observe_chunk(jid, claim['attempt_id'], result, elapsed)
                            return result_index, result, elapsed
                        finally:
                            in_flight.discard(cid)
                finally:
                    all_tasks.discard(task)

            async def run_pdf_command(self, command, log_path):
                process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                                                               stderr=asyncio.subprocess.STDOUT)
                try:
                    output, _ = await asyncio.wait_for(process.communicate(), timeout=1800)
                    text = (output or b'').decode('utf-8', errors='replace')
                    log_path.write_text(text)
                    return process.returncode == 0, text[-4000:]
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                    raise

        with tempfile.TemporaryDirectory(prefix='book2sft-' + jid + '-') as directory:
            root = Path(directory)
            runner = Runner(root)
            for relative, reference in files.items():
                target = root / relative
                if target.is_absolute() and root.resolve() not in target.resolve().parents:
                    raise ValueError('invalid checkpoint artifact path')
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(await self.read(reference))
            input_object = config.get('input_object')
            if input_object:
                source = root / 'input' / ('ocr.json' if job.input_type == 'ocr_json' else 'book.pdf')
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_bytes(await asyncio.to_thread(self.objects.get, input_object['key']))
            if stage == 'sft':
                # Cached results are re-aggregated by the original function;
                # do not add them to metrics from a previous activity attempt.
                job.metrics.setdefault('pipeline', {})['sft'] = {}
                source_chunks = legacy.jsonl_rows(root / 'artifacts/chunks.jsonl')
                for index, chunk in enumerate(source_chunks):
                    cid = legacy.first_text(chunk.get('id'), chunk.get('original_id'), f'chunk_{index:06d}')
                    async with store.db.transaction() as conn:
                        old = (await conn.execute(select(chunks).where(and_(chunks.c.job_id == jid, chunks.c.id == cid)))).mappings().first()
                    if not old:
                        ref, _ = await self.put(jid, 'chunk_inputs', encoded(chunk).encode())
                        await store.add_chunk(jid, cid, index, ref)
            async def run():
                if stage == 'ocr':
                    if job.input_type == 'ocr_json':
                        # A supplied OCR export already satisfies the OCR stage.
                        source = root / 'input' / 'ocr.json'
                        target = root / 'artifacts' / 'ocr.json'
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(source.read_bytes())
                        job.metrics.setdefault('pipeline', {}).setdefault('ocr', {}).update({
                            'status': 'completed', 'progress': 100, 'ocr_skipped': True,
                        })
                        await runner.set_stage(job, 'ocr', message='OCR skipped: supplied OCR JSON', local_progress=100)
                        await runner.finish_stage(job, 'ocr')
                    else:
                        job._ops_chunker_stage = 'ocr'
                        job._ops_chunker_input_sha256 = config.get('input_object', {}).get('sha256', '')
                        await runner.run_ocr(job)
                elif stage == 'chunker':
                    if legacy_combined:
                        # A pre-Task-3.1 Temporal history owns one combined
                        # Chunker activity; preserve its PDF/OCR input and
                        # deterministic child operation ID during replay.
                        job._ops_legacy_combined_chunker = True
                        await runner.run_chunker(job)
                    else:
                        ocr_reference = files.get('artifacts/ocr.json')
                        if not ocr_reference:
                            raise Missing('OCR checkpoint artifact is required before Chunker')
                        ocr_bytes = await adapter.read(ocr_reference)
                        ocr_input = root / 'input' / 'ocr.json'
                        ocr_input.parent.mkdir(parents=True, exist_ok=True)
                        ocr_input.write_bytes(ocr_bytes)
                        job._chunker_use_ocr_input = True
                        job._chunker_upload_filename = 'ocr.json'
                        job._ops_chunker_stage = 'chunker'
                        job._ops_chunker_input_sha256 = hashlib.sha256(ocr_bytes).hexdigest()
                        await runner.run_chunker(job)
                elif stage == 'sft':
                    await runner.run_sft(job)
                elif stage == 'review':
                    await runner.run_review_pdf(job)
                elif stage == 'publish':
                    await runner.complete(job)
                else:
                    raise ValueError('unknown stage')
            operation = asyncio.create_task(run())
            reason = None
            try:
                while not operation.done():
                    state = (await store.get(jid))['status']
                    if state == 'cancel_requested':
                        # Persisted cancellation reaches both downstream services.
                        if await self.cancel(jid):
                            reason = 'cancel_requested'
                            break
                    elif state == 'pause_requested' and stage == 'sft' and job.stage == 'sft_generation' and not in_flight:
                        reason = 'pause_requested'
                        break
                    await asyncio.sleep(0.25)
                if reason:
                    operation.cancel()
                else:
                    await operation
            finally:
                if not operation.done():
                    operation.cancel()
                for task in list(all_tasks):
                    task.cancel()
                await asyncio.gather(operation, *list(all_tasks), return_exceptions=True)
            if reason:
                await runtime.checkpoint(jid, stage, 'paused' if reason == 'pause_requested' else 'cancelled',
                    {'job': job.as_dict(), 'files': files}, lease=lease)
                return {'state': reason}
            for path in root.rglob('*'):
                if path.is_file() and 'input' not in path.relative_to(root).parts:
                    relative = str(path.relative_to(root))
                    category = 'completed_datasets' if relative.startswith(('sft/', 'review/')) else 'generated_artifacts'
                    ref, _ = await self.put(jid, category, path.read_bytes())
                    files[relative] = ref
            state = 'failed' if job.status == 'failed' else 'completed'
            await runtime.checkpoint(jid, stage, state, {'job': job.as_dict(), 'files': files}, lease=lease)
            return {'state': state, 'error_type': (job.error or {}).get('code')}

    async def cancel(self, jid):
        """Tombstone requests before joining local work; external model stop is best effort."""
        from ..main import CHUNKER_BASE_URL, SFT_ORCHESTRATOR_BASE_URL
        import uuid
        record = await self.store.get(jid)
        headers = {'X-Service-Token': os.environ.get('OPS_SERVICE_TOKEN', '')}
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            children = []
            for stage_name in ('ocr', 'chunker'):
                child_stage = await self.runtime.stage(jid, stage_name)
                if child_stage is None:
                    continue
                snapshot = json.loads(child_stage['snapshot'])
                child = (snapshot.get('job') or {}).get('downstream_job_id')
                if not child:
                    candidates = [str(uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f'chunker:{jid}:{record["attempt"]}:{stage_name}',
                    ))]
                    if stage_name == 'chunker':
                        # Also tombstone the Task 2 combined-child ID in case an
                        # older activity created it but crashed before checkpoint.
                        candidates.append(str(uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f'chunker:{jid}:{record["attempt"]}',
                        )))
                else:
                    candidates = [child]
                for candidate in candidates:
                    if candidate not in children:
                        children.append(candidate)
            for child in children:
                response = await client.post(f'{CHUNKER_BASE_URL}/v1/jobs/{child}/cancel')
                if response.status_code == 404:
                    continue
                if response.status_code >= 400:
                    return False
                if response.json().get('status') not in {'cancelled', 'completed', 'failed'}:
                    return False
            cursor = None
            while True:
                page = await self.store.page(chunks, job_id=jid, cursor=cursor, limit=100)
                for chunk in page['items']:
                    op = f'{jid}:{record["attempt"]}:{chunk["ordinal"]}'
                    response = await client.post(f'{SFT_ORCHESTRATOR_BASE_URL}/v1/ops/requests/{op}/cancel')
                    if response.status_code >= 400 or response.json().get('status') not in {'cancelled', 'completed', 'failed'}:
                        return False
                cursor = page['next_cursor']
                if cursor is None:
                    break
        return True
