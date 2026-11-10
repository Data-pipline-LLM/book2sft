"""Bounded operational UI data, upload compatibility and streaming downloads."""
import asyncio
import json
import os
import time
import uuid
from pathlib import Path
import httpx
from fastapi import Depends, HTTPException, Header, Request
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import select, and_, func
from .schema import stages, chunks, attempts, artifacts, jobs, events
from .queries import job_detail, event_page, pack
from .analytics import Analytics
from .store import authorize, Missing, encoded


class Bulk(BaseModel):
    job_ids: list[str] = Field(max_length=100, min_length=1)
    action: str


def install(app, store, objects, principal, worker_available):
    analytics = Analytics(store)
    app.state.sse_count = 0

    @app.get('/', response_class=HTMLResponse)
    async def dashboard():
        return (Path(__file__).resolve().parents[2] / 'frontend/index.html').read_text()
    asset_root = Path(__file__).resolve().parents[2] / 'frontend'
    if asset_root.exists():
        app.mount('/static', StaticFiles(directory=asset_root), name='static')

    @app.post('/v1/jobs', status_code=202)
    async def compatibility_upload(request: Request, p=Depends(principal)):
        authorize(p['role'])
        await worker_available()
        from .ingestion import submit
        form = await request.form(max_files=3, max_fields=10)
        uploads = [form.get(key) for key in ('pdf_file', 'ocr_file', 'input_file') if form.get(key) is not None]
        if len(uploads) != 1 or not hasattr(uploads[0], 'file'):
            raise HTTPException(422, 'supply exactly one PDF/OCR file')
        upload = uploads[0]
        extension = Path(upload.filename or '').suffix.lower()
        if extension not in {'.pdf', '.json'}:
            raise HTTPException(415, 'PDF or OCR JSON required')
        kind = 'ocr_json' if extension == '.json' else 'pdf'
        try:
            jid = await submit(store, objects, upload, title=str(form.get('book_title') or ''),
                category=str(form.get('category') or ''), domain=str(form.get('domain') or 'general'),
                input_type=kind, actor=p['id'], role=p['role'],
                key=request.headers.get('Idempotency-Key') or str(uuid.uuid4()),
                max_bytes=int(os.getenv('MAX_UPLOAD_MB', '500')) * 1024 * 1024)
        finally:
            await upload.close()
        return {'job_id': jid, 'status': (await store.get(jid))['status'], 'input_type': kind,
            'ocr_skipped': kind == 'ocr_json', 'status_url': f'/v1/jobs/{jid}', 'events_url': f'/v1/jobs/{jid}/events'}

    @app.get('/v1/jobs/{jid}')
    async def legacy_status(jid: str, p=Depends(principal)):
        detail = await job_detail(store, jid, p['role'])
        async with store.db.transaction() as conn:
            latest = (await conn.execute(select(stages.c.snapshot).where(stages.c.job_id == jid)
                .order_by(stages.c.updated.desc()).limit(1))).scalar_one_or_none()
        original = json.loads(latest).get('job', {}) if latest else {}
        if p['role'] == 'viewer':
            original.pop('error', None)
        return {**original, 'job_id': jid, 'book_title': detail['title'],
            'stage': original.get('stage') or ('completed' if detail['status'] == 'completed' else 'queued'),
            'progress': original.get('progress', 100 if detail['status'] == 'completed' else 0),
            **detail}

    @app.get('/v1/jobs/{jid}/download/{name}')
    async def legacy_download(jid: str, name: str, p=Depends(principal)):
        authorize(p['role'])
        if '/' in name or '\\' in name or '..' in name:
            raise Missing(name)
        async with store.db.transaction() as conn:
            snapshots = (await conn.execute(select(stages.c.snapshot).where(stages.c.job_id == jid)
                .order_by(stages.c.updated.desc()).limit(5))).scalars().all()
        for snapshot in snapshots:
            for path, ref in json.loads(snapshot).get('files', {}).items():
                if Path(path).name == name:
                    return RedirectResponse('/v1/ops/artifacts/' + ref, status_code=307)
        raise Missing(name)

    @app.get('/v1/jobs/{jid}/events')
    async def live(jid: str, request: Request, p=Depends(principal), last_event_id: str | None = Header(default=None)):
        await store.get(jid)
        if app.state.sse_count >= 100:
            raise HTTPException(429, 'live connection capacity reached')
        app.state.sse_count += 1
        async def stream():
            cursor, started = last_event_id, time.monotonic()
            try:
                while time.monotonic() - started < 300 and not await request.is_disconnected():
                    rows = await event_page(store, jid, cursor)
                    for row in rows:
                        cursor = pack([row['at'], row['id']])
                        yield 'id: ' + cursor + '\ndata: ' + encoded(row) + '\n\n'
                    if not rows:
                        yield ': heartbeat\n\n'
                    await asyncio.sleep(2)
            finally:
                app.state.sse_count -= 1
        return StreamingResponse(stream(), media_type='text/event-stream')

    @app.get('/v1/ops/jobs/{jid}/stages')
    async def stage_list(jid: str, p=Depends(principal)):
        return {'items': (await job_detail(store, jid, p['role']))['stages']}

    @app.get('/v1/ops/jobs/{jid}/errors')
    async def errors(jid: str, cursor: str | None = None, limit: int = 100, p=Depends(principal)):
        authorize(p['role'])
        return await store.page(attempts, job_id=jid, cursor=cursor, limit=limit, status='failed')

    @app.get('/v1/ops/jobs/{jid}/chunks/{cid}')
    async def chunk_detail(jid: str, cid: str, p=Depends(principal)):
        authorize(p['role'])
        async with store.db.transaction() as conn:
            row = (await conn.execute(select(chunks).where(and_(chunks.c.job_id == jid, chunks.c.id == cid)))).mappings().first()
            history = [dict(r) for r in (await conn.execute(select(attempts).where(and_(
                attempts.c.job_id == jid, attempts.c.chunk_id == cid)).order_by(attempts.c.number.desc()).limit(100))).mappings()]
        if not row:
            raise Missing(cid)
        template = os.getenv('LANGFUSE_TRACE_URL_TEMPLATE', '')
        for item in history:
            item['trace_url'] = template.replace('{trace_id}', item['trace_id']) if template else None
        return {**dict(row), 'attempts': history, 'attempt_history_truncated': len(history) == 100}

    @app.get('/v1/ops/analytics')
    async def charts(since: float | None = None, until: float | None = None, max_points: int = 240, p=Depends(principal)):
        end = until if until is not None else store.clock()
        return await analytics.series(since if since is not None else end - 86400, end, max_points)

    @app.get('/v1/ops/services')
    async def services(p=Depends(principal)):
        urls = {'chunker': os.getenv('CHUNKER_BASE_URL', 'http://chunker:8080'),
                'llm_gateway': os.getenv('LLM_GATEWAY_BASE_URL', 'http://llm-gateway:8090'),
                'sft_orchestrator': os.getenv('SFT_ORCHESTRATOR_BASE_URL', 'http://orchestrator:8000'),
                'sft_generator': os.getenv('SFT_GENERATOR_BASE_URL', 'http://generator:8000'),
                'sft_assessment': os.getenv('SFT_ASSESSMENT_BASE_URL', 'http://assessment:8000'),
                'sft_quality': os.getenv('SFT_QUALITY_BASE_URL', 'http://quality:8000')}
        async def check(name, url):
            started = time.monotonic()
            try:
                async with httpx.AsyncClient(timeout=3) as client:
                    health_path = '/ready' if name == 'llm_gateway' else '/health'
                    response = await client.get(url.rstrip('/') + health_path)
                payload = response.json() if response.is_success else {}
                result = {'name': name, 'health': 'healthy' if response.is_success else 'unhealthy',
                          'probe_latency_sec': time.monotonic() - started, 'at': store.clock()}
                if name == 'llm_gateway' and response.is_success:
                    async with httpx.AsyncClient(timeout=3) as client:
                        metrics_response = await client.get(url.rstrip('/') + '/internal/metrics')
                    metrics_payload = metrics_response.json() if metrics_response.is_success else {}
                    result['metrics'] = {
                        'active': int(metrics_payload.get('active') or 0),
                        'capacity': int(metrics_payload.get('capacity') or 0),
                        'queued': int(metrics_payload.get('queued') or 0),
                        'slots_available': int(metrics_payload.get('slots_available') or 0),
                        'target_reached': bool(metrics_payload.get('target_reached')),
                        'clients': metrics_payload.get('clients') or {},
                    }
                return result
            except httpx.HTTPError:
                return {'name': name, 'health': 'unknown', 'probe_latency_sec': None, 'at': store.clock()}
        fleet = await analytics.fleet()
        fleet['services'] = await asyncio.gather(*(check(name, url) for name, url in urls.items()))
        fleet['telemetry'] = __import__('backend.ops.telemetry', fromlist=['status']).status
        url_template = os.getenv('LANGFUSE_TRACE_URL_TEMPLATE', '')
        fleet['trace_url_template'] = url_template if url_template.startswith('https://') and '{trace_id}' in url_template else ''
        return fleet

    @app.post('/v1/ops/bulk', status_code=202)
    async def bulk(body: Bulk, p=Depends(principal), idempotency_key: str = Header()):
        import hashlib
        authorize(p['role'])
        if body.action not in {'pause', 'resume', 'cancel', 'retry', 'archive'}:
            raise HTTPException(422, 'unsupported bulk action')
        await worker_available()
        result = []
        for jid in dict.fromkeys(body.job_ids):
            key = hashlib.sha256((idempotency_key + ':' + jid).encode()).hexdigest()
            try:
                command = await store.command(jid, body.action, p['id'], p['role'], key)
                result.append({'job_id': jid, 'command_id': command['id'], 'status': 'accepted'})
            except (ValueError, Missing) as exc:
                result.append({'job_id': jid, 'status': 'rejected', 'error': type(exc).__name__})
            except Exception as exc:
                from .store import Conflict
                if not isinstance(exc, Conflict):
                    raise
                result.append({'job_id': jid, 'status': 'conflict'})
        return {'items': result}

