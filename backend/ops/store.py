"""Transactional commands, checkpoint publication and query contracts.

Temporal owns scheduling; this repository never schedules work from RAM or SQL.
"""
import hashlib
import json
import time
import uuid
from sqlalchemy import select, func, and_, or_, text
from .schema import jobs, chunks, attempts, commands, events, audit, artifacts, books


def uid():
    return str(uuid.uuid4())


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class Conflict(Exception):
    pass


class Missing(Exception):
    pass


class Forbidden(Exception):
    pass


ROLE = {'viewer': 0, 'operator': 1, 'admin': 2}


def authorize(role, required='operator'):
    if ROLE.get(role, -1) < ROLE[required]:
        raise Forbidden('insufficient role')


class Store:
    def __init__(self, db, clock=time.time):
        self.db, self.clock = db, clock

    async def _job(self, conn, job_id, lock=True):
        query = select(jobs).where(jobs.c.id == job_id)
        if lock:
            query = query.with_for_update()
        row = (await conn.execute(query)).mappings().first()
        if row is None:
            raise Missing(job_id)
        return dict(row)

    async def _audit(self, conn, actor, action, target, detail):
        await conn.execute(audit.insert().values(id=uid(), actor=actor, at=self.clock(),
            action=action, target=target, detail=encoded(detail)))

    async def _state(self, conn, job, status, **values):
        now = self.clock()
        values.update(status=status, updated=now, revision=job['revision'] + 1)
        await conn.execute(jobs.update().where(jobs.c.id == job['id']).values(**values))
        await conn.execute(events.insert().values(id=uid(), job_id=job['id'], at=now,
            kind='state', detail=encoded({'from': job['status'], 'to': status,
                                       'revision': values['revision']})))

    async def create(self, title, config, actor, role, key, book_id=None, *, import_id=None, historical_status=None):
        authorize(role)
        if historical_status is not None:
            authorize(role, 'admin')
            if historical_status not in {'completed', 'failed', 'archived'}:
                raise ValueError('invalid historical import status')
        if not key or len(key) > 128 or len(title) > 512:
            raise ValueError('invalid idempotency key/title')
        semantic_config = dict(config)
        if isinstance(config.get('input_object'), dict):
            semantic_config['input_object'] = {k: v for k, v in config['input_object'].items() if k != 'key'}
        body = encoded({'title': title, 'config': semantic_config, 'book_id': book_id})
        if len(body.encode()) > 65536:
            raise ValueError('job configuration exceeds 64 KiB; use artifact references')
        async with self.db.transaction() as conn:
            await self._serialize_key(conn, actor, key)
            old = await self._existing(conn, actor, key, 'create', body)
            if old:
                return old['job_id']
            jid, now = str(uuid.UUID(import_id)) if import_id else uid(), self.clock()
            bid = book_id or uid()
            await self._serialize_key(conn, 'book', bid)
            if not (await conn.execute(select(books.c.id).where(books.c.id == bid))).first():
                await conn.execute(books.insert().values(id=bid, title=title, created=now))
            await conn.execute(jobs.insert().values(id=jid, book_id=bid, title=title,
                config=encoded(config), status=historical_status or 'queued', created=now, updated=now, revision=0,
                epoch=1, attempt=1, resumable=0 if historical_status else 1))
            await conn.execute(commands.insert().values(id=uid(), job_id=jid, actor=actor, key=key,
                action='create', payload=body, status='completed' if historical_status else 'pending', created=now))
            await self._audit(conn, actor, 'create', jid, {'status': 'queued'})
            return jid

    async def _serialize_key(self, conn, actor, key):
        if not self.db.test_sqlite:
            # Idempotency is scoped to actor; hash collisions only serialize extra calls.
            digest = int.from_bytes(hashlib.sha256((actor + '\0' + key).encode()).digest()[:8], 'big', signed=True)
            await conn.execute(text('SELECT pg_advisory_xact_lock(:key)').bindparams(key=digest))

    async def _existing(self, conn, actor, key, action, payload):
        old = (await conn.execute(select(commands).where(and_(commands.c.actor == actor,
                     commands.c.key == key)))).mappings().first()
        if old and (old['action'] != action or old['payload'] != payload):
            raise Conflict('idempotency key reused with different request')
        return dict(old) if old else None

    async def command(self, job_id, action, actor, role, key, payload=None):
        authorize(role, 'admin' if action == 'delete' else 'operator')
        if not key or len(key) > 128:
            raise ValueError('invalid idempotency key')
        body = encoded({'job_id': job_id, 'data': payload or {}})
        async with self.db.transaction() as conn:
            await self._serialize_key(conn, actor, key)
            old = await self._existing(conn, actor, key, action, body)
            if old:
                return old
            job = await self._job(conn, job_id)
            status, values = job['status'], {}
            if action == 'pause' and status in {'queued', 'running', 'recovering'}:
                target = 'pause_requested'
            elif action == 'resume' and status == 'paused' and job['resumable']:
                target = 'resume_requested'
            elif action == 'cancel' and status in {'queued', 'running', 'paused', 'recovering', 'pause_requested', 'resume_requested'}:
                target, values = 'cancel_requested', {'epoch': job['epoch'] + 1}
                await conn.execute(commands.update().where(and_(commands.c.job_id == job_id,
                    commands.c.status.in_(['pending','delivered']), commands.c.action.in_(['pause','resume']))).values(
                    status='superseded', completed=self.clock(), result=encoded({'reason':'cancel takes precedence'})))
            elif action in {'retry', 'retry_chunk'} and (status == 'failed' or
                    action == 'retry_chunk' and status == 'completed') and job['resumable']:
                target = 'retrying'
                query = and_(chunks.c.job_id == job_id, chunks.c.status == 'failed')
                if action == 'retry_chunk':
                    chunk_id = (payload or {}).get('chunk_id')
                    if not chunk_id:
                        raise ValueError('chunk_id required')
                    query = and_(query, chunks.c.id == chunk_id)
                result = await conn.execute(chunks.update().where(query).values(status='pending'))
                if action == 'retry_chunk' and not result.rowcount:
                    raise Conflict('chunk is not failed')
                from .schema import stages
                invalidate = stages.c.status == 'failed'
                if result.rowcount:
                    invalidate = stages.c.name.in_(['sft', 'review', 'publish'])
                await conn.execute(stages.update().where(and_(stages.c.job_id == job_id,
                    invalidate)).values(status='pending', updated=self.clock()))
                values = {'epoch': job['epoch'] + 1, 'attempt': job['attempt'] + 1}
            elif action == 'archive' and status in {'completed', 'failed', 'cancelled'}:
                target = 'archived'
            elif action == 'delete' and status in {'completed', 'failed', 'cancelled', 'archived'}:
                if job['resumable']:
                    raise Conflict('disable recovery explicitly before deleting metadata')
                target = 'deleted'
            elif action == 'disable_recovery' and status in {'completed', 'failed', 'cancelled', 'archived'}:
                authorize(role, 'admin')
                target, values = status, {'resumable': 0}
            else:
                raise Conflict(f'{action} not allowed from {status}')
            await self._state(conn, job, target, **values)
            cid = uid()
            immediate = action in {'archive', 'delete', 'disable_recovery'}
            await conn.execute(commands.insert().values(id=cid, job_id=job_id, actor=actor, key=key,
                action=action, payload=body, status='completed' if immediate else 'pending',
                created=self.clock(), completed=self.clock() if immediate else None))
            await self._audit(conn, actor, action, job_id, {'from': status, 'to': target, 'command_id': cid})
            return dict((await conn.execute(select(commands).where(commands.c.id == cid))).mappings().one())

    async def acknowledge(self, command_id):
        """Called by the workflow after delivery; terminal commands require safe boundaries."""
        async with self.db.transaction() as conn:
            cmd = (await conn.execute(select(commands).where(commands.c.id == command_id))).mappings().first()
            if not cmd:
                raise Missing(command_id)
            job = await self._job(conn, cmd['job_id'])
            if cmd['status'] == 'completed':
                return
            expected = {'pause': ('pause_requested', 'paused'), 'resume': ('resume_requested', 'running'),
                        'cancel': ('cancel_requested', 'cancelled'), 'create': ('queued', 'running'),
                        'retry': ('retrying', 'running'), 'retry_chunk': ('retrying', 'running')}
            before, after = expected[cmd['action']]
            # A pause/cancel accepted before the create outbox was delivered wins.
            if cmd['action'] == 'create' and job['status'] != 'queued':
                after = job['status']
            elif job['status'] != before:
                raise Conflict('command superseded or invalid acknowledgement')
            active = (await conn.execute(select(func.count()).select_from(chunks).where(and_(
                chunks.c.job_id == job['id'], chunks.c.status == 'running')))).scalar_one()
            if cmd['action'] in {'pause', 'cancel'} and active:
                raise Conflict('downstream work has not acknowledged the safe boundary')
            if after != job['status']:
                await self._state(conn, job, after)
            await conn.execute(commands.update().where(commands.c.id == command_id).values(
                status='completed', completed=self.clock(), result=encoded({'status': after})))

    async def add_chunk(self, job_id, chunk_id, ordinal, input_ref):
        if not chunk_id or len(chunk_id) > 128 or ordinal < 0:
            raise ValueError('invalid chunk')
        async with self.db.transaction() as conn:
            await self._job(conn, job_id)
            old = (await conn.execute(select(chunks).where(and_(chunks.c.job_id == job_id,
                chunks.c.id == chunk_id)))).mappings().first()
            if old:
                if old['input_ref'] != input_ref or old['ordinal'] != ordinal:
                    raise Conflict('chunk input changed')
                return
            await conn.execute(chunks.insert().values(job_id=job_id, id=chunk_id, ordinal=ordinal,
                input_ref=input_ref, status='pending', attempt=0, updated=self.clock()))

    async def begin_chunk(self, job_id, chunk_id, worker, *, stage_lease=None):
        async with self.db.transaction() as conn:
            job = await self._job(conn, job_id)
            if stage_lease:
                from .schema import stages
                current_lease = (await conn.execute(select(stages.c.lease).where(and_(
                    stages.c.job_id == job_id, stages.c.name == 'sft')))).scalar_one_or_none()
                if current_lease != stage_lease:
                    raise Conflict('stage lease superseded')
            if job['status'] != 'running':
                raise Conflict('job does not accept new work')
            chunk = (await conn.execute(select(chunks).where(and_(chunks.c.job_id == job_id,
                chunks.c.id == chunk_id)))).mappings().first()
            if not chunk:
                raise Missing(chunk_id)
            if chunk['status'] == 'completed':
                return {'cached': True, 'output_ref': chunk['output_ref'], 'output_hash': chunk['output_hash']}
            if chunk['status'] != 'pending':
                raise Conflict('chunk already claimed or failed')
            aid, trace = uid(), uuid.uuid4().hex
            await conn.execute(chunks.update().where(and_(chunks.c.job_id == job_id,
                chunks.c.id == chunk_id)).values(status='running', attempt=chunk['attempt'] + 1, updated=self.clock()))
            await conn.execute(attempts.insert().values(id=aid, job_id=job_id, chunk_id=chunk_id,
                number=chunk['attempt'] + 1, epoch=job['epoch'], worker=worker, status='running',
                started=self.clock(), trace_id=trace, input_ref=chunk['input_ref']))
            return {'cached': False, 'attempt_id': aid, 'epoch': job['epoch'], 'trace_id': trace}

    async def finish_chunk(self, attempt_id, output_ref=None, output_hash=None, error=None, model=None, quality=None):
        if error is None and (not output_ref or not output_hash or len(output_hash) != 64):
            raise ValueError('committed artifact reference and SHA256 required')
        async with self.db.transaction() as conn:
            attempt = (await conn.execute(select(attempts).where(attempts.c.id == attempt_id))).mappings().first()
            if not attempt:
                raise Missing(attempt_id)
            job = await self._job(conn, attempt['job_id'])
            if attempt['status'] != 'running':
                if attempt['status'] == 'completed' and attempt['output_ref'] == output_ref:
                    checkpoint = (await conn.execute(select(chunks.c.output_hash).where(and_(
                        chunks.c.job_id == attempt['job_id'], chunks.c.id == attempt['chunk_id'])))).scalar_one()
                    if checkpoint == output_hash:
                        return True
                raise Conflict('attempt already finalized')
            current = (await conn.execute(select(chunks.c.attempt).where(and_(
                chunks.c.job_id == job['id'], chunks.c.id == attempt['chunk_id'])))).scalar_one()
            valid = (attempt['epoch'] == job['epoch'] and current == attempt['number']
                     and job['status'] in {'running', 'pause_requested'})
            if valid and error is None:
                manifest = (await conn.execute(select(artifacts).where(and_(
                    artifacts.c.id == output_ref, artifacts.c.job_id == job['id'],
                    artifacts.c.sha256 == output_hash, artifacts.c.status == 'available')))).mappings().first()
                if not manifest:
                    raise Conflict('output artifact must be persisted and registered before publication')
            state = ('failed' if error else 'completed') if valid else 'discarded'
            await conn.execute(attempts.update().where(attempts.c.id == attempt_id).values(
                status=state, ended=self.clock(), error=encoded(error) if error else None,
                output_ref=output_ref if valid else None, model=model, quality=quality))
            await conn.execute(chunks.update().where(and_(chunks.c.job_id == job['id'],
                chunks.c.id == attempt['chunk_id'], chunks.c.attempt == attempt['number'])).values(
                status=state if valid else 'cancelled', output_ref=output_ref if valid else None,
                output_hash=output_hash if valid else None, updated=self.clock()))
            return valid

    async def recover(self, job_id):
        """Fence a known-dead activity after Temporal confirms its old lease ended."""
        async with self.db.transaction() as conn:
            job = await self._job(conn, job_id)
            if job['status'] not in {'running', 'recovering'}:
                raise Conflict('cannot recover this state')
            await self._state(conn, job, 'recovering', epoch=job['epoch'] + 1)
            await conn.execute(attempts.update().where(and_(attempts.c.job_id == job_id,
                attempts.c.status == 'running')).values(status='interrupted', ended=self.clock()))
            await conn.execute(chunks.update().where(and_(chunks.c.job_id == job_id,
                chunks.c.status == 'running')).values(status='pending', updated=self.clock()))

    async def resume_recovered(self, job_id):
        async with self.db.transaction() as conn:
            job = await self._job(conn, job_id)
            if job['status'] != 'recovering':
                raise Conflict('not recovering')
            await self._state(conn, job, 'running')

    async def complete(self, job_id):
        async with self.db.transaction() as conn:
            job = await self._job(conn, job_id)
            if job['status'] != 'running':
                raise Conflict('not running')
            states = dict((await conn.execute(select(chunks.c.status, func.count()).where(
                chunks.c.job_id == job_id).group_by(chunks.c.status))).all())
            if not states or any(k not in {'completed', 'failed'} for k in states):
                raise Conflict('unfinished chunks')
            await self._state(conn, job, 'failed' if states.get('failed') else 'completed')

    async def get(self, job_id):
        async with self.db.transaction() as conn:
            return await self._job(conn, job_id, lock=False)

    async def page(self, table, *, job_id=None, cursor=None, limit=100, status=None):
        if not 1 <= limit <= 200:
            raise ValueError('limit must be 1..200')
        order = table.c.ordinal if table is chunks else table.c.id
        query = select(table)
        if job_id is not None:
            query = query.where(table.c.job_id == job_id)
        if status:
            query = query.where(table.c.status == status)
        if cursor is not None:
            query = query.where(order > (int(cursor) if table is chunks else cursor))
        async with self.db.transaction() as conn:
            rows = [dict(r) for r in (await conn.execute(query.order_by(order).limit(limit + 1))).mappings()]
        return {'items': rows[:limit], 'next_cursor': str(rows[limit-1][order.name]) if len(rows) > limit else None}

    async def overview(self):
        async with self.db.transaction() as conn:
            book_count = (await conn.execute(select(func.count()).select_from(books))).scalar_one()
            statuses = dict((await conn.execute(select(jobs.c.status, func.count()).group_by(jobs.c.status))).all())
            chunk_states = dict((await conn.execute(select(chunks.c.status, func.count()).group_by(chunks.c.status))).all())
            counts = (await conn.execute(select(func.count(), func.sum(attempts.c.ended - attempts.c.started))
                .select_from(attempts).where(attempts.c.ended.is_not(None)))).one()
        return {'books': book_count, 'statuses': statuses, 'chunks': chunk_states, 'attempts_finished': counts[0],
                'attempt_duration_sum_sec': counts[1], 'as_of': self.clock(),
                'source': 'operational database', 'quality': 'unavailable',
                'resources': 'unavailable', 'scope': 'all time'}
