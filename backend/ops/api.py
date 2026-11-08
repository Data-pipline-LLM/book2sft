"""Operations API factory. Production requires PostgreSQL; worker is separate.

No route claims execution succeeded when only a durable command was accepted.
"""
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from fastapi.responses import HTMLResponse, Response, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from .auth import Auth
from .store import Store, Conflict, Missing, Forbidden, authorize
from .retention import Retention
from .schema import jobs, chunks, attempts, commands, events, artifacts, policies, runs, audit


class Login(BaseModel):
    user: str = Field(min_length=1, max_length=128)
    password: str = Field(max_length=1024)


class CreateJob(BaseModel):
    title: str = Field(max_length=512)
    book_id: str | None = Field(default=None, max_length=36)
    config: dict = Field(default_factory=dict)


class Policy(BaseModel):
    category: str
    days: int = Field(ge=1, le=36500)
    enabled: bool = False


class Cleanup(BaseModel):
    dry_run: bool = True


class Schedule(BaseModel):
    enabled: bool = False
    interval_seconds: int = Field(ge=60, le=31 * 86400)


def create_app(db, objects, *, workflow_ready=None):
    store = Store(db)
    auth = Auth(store)
    retention = Retention(store, objects)

    @asynccontextmanager
    async def lifespan(app):
        # Migrations are explicit CLI operations, not implicit on every web start.
        try:
            yield
        finally:
            await db.close()

    app = FastAPI(title='Book2SFT Operations', lifespan=lifespan)
    app.state.store, app.state.auth, app.state.retention = store, auth, retention
    app.state.workflow_ready = workflow_ready
    # No raw body logging, cookie auth, permissive CORS or embedded credentials.
    @app.middleware('http')
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'no-referrer'
        return response

    @app.exception_handler(Conflict)
    async def conflict(_, exc):
        return JSONResponse(status_code=409, content={'code': 'conflict', 'detail': str(exc)})

    @app.exception_handler(Missing)
    async def missing(_, exc):
        return JSONResponse(status_code=404, content={'code': 'not_found'})

    @app.exception_handler(Forbidden)
    async def forbidden(_, exc):
        return JSONResponse(status_code=403, content={'code': 'forbidden', 'detail': str(exc)})

    @app.exception_handler(ValueError)
    async def invalid(_, exc):
        return JSONResponse(status_code=422, content={'code': 'invalid_request', 'detail': str(exc)})

    async def principal(authorization: str = Header(default='')):
        if not authorization.startswith('Bearer '):
            raise HTTPException(401, 'Bearer token required')
        return await auth.principal(authorization[7:])

    async def worker_available():
        probe = app.state.workflow_ready
        if probe is None or not await probe():
            raise HTTPException(503, 'workflow executor is unavailable; command not accepted')

    @app.get('/health')
    async def health():
        return {'status': 'alive'}

    @app.get('/ready')
    async def ready():
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError
        try:
            async with db.transaction() as conn:
                version = (await conn.execute(text('SELECT version FROM ops_schema_versions WHERE version=2'))).first()
        except (SQLAlchemyError, OSError):
            raise HTTPException(503, 'operational database unavailable')
        if not version:
            raise HTTPException(503, 'operational schema migration required')
        probe = getattr(app.state, 'readiness', None)
        if probe:
            try:
                await probe()
            except Exception:
                raise HTTPException(503, 'storage or workflow infrastructure is not ready')
        else:
            await worker_available()
        return {'status': 'ready'}

    @app.post('/v1/auth/login')
    async def login(body: Login, request: Request):
        source = request.client.host if request.client else 'unknown'
        return {'access_token': await auth.login(body.user, body.password, source), 'token_type': 'bearer', 'expires_in': 3600}

    @app.post('/v1/auth/logout')
    async def logout(p=Depends(principal), authorization: str = Header()):
        await auth.logout(authorization[7:])
        return {'status': 'logged_out'}

    @app.get('/v1/auth/me')
    async def me(p=Depends(principal)):
        return p

    @app.get('/v1/ops/overview')
    async def overview(p=Depends(principal)):
        from .analytics import estimate_eta
        result = await store.overview()
        result['eta'] = await estimate_eta(store)
        return result

    @app.get('/v1/ops/jobs')
    async def job_list(cursor: str | None = None, limit: int = 100, status: str | None = None,
                       q: str = '', since: float | None = None, until: float | None = None,
                       book_id: str | None = None, p=Depends(principal)):
        from .queries import jobs_page
        result = await jobs_page(store, cursor=cursor, limit=limit, status=status, q=q,
                                 since=since, until=until, book_id=book_id)
        for item in result['items']:
            item.pop('config', None)
        return result

    @app.post('/v1/ops/jobs', status_code=202)
    async def create(body: CreateJob, p=Depends(principal), idempotency_key: str = Header()):
        authorize(p['role'])
        await worker_available()
        jid = await store.create(body.title, body.config, p['id'], p['role'], idempotency_key, body.book_id)
        return {'job_id': jid, 'status': 'accepted'}

    @app.get('/v1/ops/jobs/{job_id}')
    async def detail(job_id: str, p=Depends(principal)):
        from .queries import job_detail
        return await job_detail(store, job_id, p['role'])

    @app.post('/v1/ops/jobs/{job_id}/{action}', status_code=202)
    async def command(job_id: str, action: str, request: Request, p=Depends(principal), idempotency_key: str = Header()):
        authorize(p['role'])
        if action not in {'pause', 'resume', 'cancel', 'retry', 'retry_chunk', 'archive', 'disable_recovery'}:
            raise HTTPException(404, 'unknown action')
        if action not in {'archive', 'disable_recovery'}:
            await worker_available()
        raw = await request.body()
        if len(raw) > 8192:
            raise HTTPException(413, 'command body too large')
        payload = json.loads(raw) if raw else {}
        return await store.command(job_id, action, p['id'], p['role'], idempotency_key, payload)

    @app.delete('/v1/ops/jobs/{job_id}', status_code=202)
    async def delete(job_id: str, p=Depends(principal), idempotency_key: str = Header()):
        return await store.command(job_id, 'delete', p['id'], p['role'], idempotency_key)

    # Fixed route registration avoids arbitrary table/column access from user input.
    def register_page(path, table, raw=False):
        async def endpoint(job_id: str, cursor: str | None = None, limit: int = 100, p=Depends(principal)):
            if raw:
                authorize(p['role'])
            await store.get(job_id)
            return await store.page(table, job_id=job_id, cursor=cursor, limit=limit)
        app.add_api_route('/v1/ops/jobs/{job_id}/' + path, endpoint, methods=['GET'])
    for path, table, raw in [('chunks', chunks, True), ('attempts', attempts, True),
                             ('events', events, False), ('artifacts', artifacts, True)]:
        register_page(path, table, raw)

    @app.get('/v1/ops/commands/{command_id}')
    async def command_detail(command_id: str, p=Depends(principal)):
        authorize(p['role'])
        async with db.transaction() as conn:
            row = (await conn.execute(select(commands).where(commands.c.id == command_id))).mappings().first()
        if not row:
            raise Missing(command_id)
        return dict(row)

    @app.get('/v1/ops/artifacts/{artifact_id}')
    async def download(artifact_id: str, p=Depends(principal)):
        authorize(p['role'])
        async with db.transaction() as conn:
            row = (await conn.execute(select(artifacts).where(artifacts.c.id == artifact_id))).mappings().first()
            if not row:
                raise Missing(artifact_id)
            if row['status'] != 'available':
                raise HTTPException(410, 'artifact expired')
            try:
                stream = await asyncio.to_thread(objects.open_reader, row['key'])
            except FileNotFoundError:
                raise HTTPException(410, 'artifact unavailable')
            await store._audit(conn, p['id'], 'artifact_download', artifact_id, {})
        async def content():
            import hashlib
            digest = hashlib.sha256()
            try:
                while True:
                    block = await asyncio.to_thread(stream.read, 65536)
                    if not block:
                        break
                    digest.update(block)
                    yield block
                if digest.hexdigest() != row['sha256']:
                    raise RuntimeError('artifact checksum mismatch')
            finally:
                await asyncio.to_thread(stream.close)
        from fastapi.responses import StreamingResponse
        return StreamingResponse(content(), media_type='application/octet-stream',
            headers={'Content-Disposition': 'attachment; filename="artifact.bin"', 'X-Content-SHA256':row['sha256']})

    @app.get('/v1/ops/artifacts/{artifact_id}/link')
    async def download_link(artifact_id: str, p=Depends(principal)):
        authorize(p['role'])
        async with db.transaction() as conn:
            row = (await conn.execute(select(artifacts).where(artifacts.c.id == artifact_id))).mappings().first()
            if not row:
                raise Missing(artifact_id)
            if row['status'] != 'available':
                raise HTTPException(410, 'artifact expired')
            if not hasattr(objects, 'download_link'):
                raise HTTPException(503, 'direct storage download unavailable')
            url = await asyncio.to_thread(objects.download_link, row['key'])
            await store._audit(conn, p['id'], 'artifact_link', artifact_id, {'expires_in':60})
        return {'url':url, 'expires_in':60}

    @app.get('/v1/admin/retention/policies')
    async def get_policies(p=Depends(principal)):
        authorize(p['role'], 'admin')
        return await policy_rows()

    @app.put('/v1/admin/retention/schedule')
    async def schedule(body: Schedule, p=Depends(principal)):
        from .cleanup_worker import CleanupWorker
        return await CleanupWorker(retention).configure(body.enabled, body.interval_seconds, p['id'], p['role'])

    async def policy_rows():
        async with db.transaction() as conn:
            return {'items': [dict(r) for r in (await conn.execute(select(policies))).mappings()]}

    @app.put('/v1/admin/retention/policies')
    async def policy(body: Policy, p=Depends(principal)):
        await retention.policy(body.category, body.days, body.enabled, p['id'], p['role'])
        return {'status': 'saved', 'applies_to': 'future artifacts'}

    @app.post('/v1/admin/retention/preview')
    async def preview(p=Depends(principal)):
        return await retention.preview(p['role'])

    @app.post('/v1/admin/retention/runs', status_code=202)
    async def cleanup(body: Cleanup, p=Depends(principal)):
        # Separate worker consumes durable runs. No in-process background task.
        return {'run_id': await retention.start(p['id'], p['role'], body.dry_run), 'status': 'accepted'}

    @app.post('/v1/admin/retention/runs/{run_id}/{action}')
    async def cleanup_control(run_id: str, action: str, p=Depends(principal)):
        await retention.control(run_id, action, p['id'], p['role'])
        return {'status': 'accepted'}

    def admin_page(path, table):
        async def endpoint(cursor: str | None = None, limit: int = 100, p=Depends(principal)):
            authorize(p['role'], 'admin')
            return await store.page(table, cursor=cursor, limit=limit)
        app.add_api_route(path, endpoint, methods=['GET'])
    admin_page('/v1/admin/retention/runs', runs)
    admin_page('/v1/admin/audit', audit)
    from .web_routes import install
    install(app, store, objects, principal, worker_available)
    return app
