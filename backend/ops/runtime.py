"""Durable execution repository; no SDK dependency and no in-memory job queue."""
import json
from sqlalchemy import select, and_, func
from .schema import stages, jobs, chunks, attempts, commands, slots, workers
from .store import Conflict, Missing, encoded


class Runtime:
    def __init__(self, store):
        self.store = store

    async def stage(self, jid, name):
        async with self.store.db.transaction() as conn:
            row = (await conn.execute(select(stages).where(and_(stages.c.job_id == jid, stages.c.name == name)))).mappings().first()
        return dict(row) if row else None

    async def claim_stage(self, jid, name, lease):
        async with self.store.db.transaction() as conn:
            await self.store._job(conn, jid)
            condition = and_(stages.c.job_id == jid, stages.c.name == name)
            row = (await conn.execute(select(stages).where(condition))).mappings().first()
            if row:
                await conn.execute(stages.update().where(condition).values(lease=lease))
            else:
                await conn.execute(stages.insert().values(job_id=jid, name=name, lease=lease,
                    status='running', snapshot='{}', started=self.store.clock(), updated=self.store.clock()))

    async def checkpoint(self, jid, name, status, snapshot, lease=None):
        async with self.store.db.transaction() as conn:
            job = await self.store._job(conn, jid)
            # Cancellation fencing applies to stage snapshots as well as chunks.
            if status == 'completed' and job['status'] == 'cancel_requested':
                raise Conflict('cancelled stage cannot publish')
            row = (await conn.execute(select(stages).where(and_(stages.c.job_id == jid, stages.c.name == name)))).mappings().first()
            if lease is not None and row and row['lease'] != lease:
                raise Conflict('stage lease superseded')
            now = self.store.clock()
            values = dict(status=status, snapshot=encoded(snapshot), updated=now,
                          ended=now if status in {'completed', 'failed', 'cancelled'} else None)
            if row:
                await conn.execute(stages.update().where(and_(stages.c.job_id == jid, stages.c.name == name)).values(**values))
            else:
                await conn.execute(stages.insert().values(job_id=jid, name=name, started=now, **values))

    async def acquire(self, jid, capacity):
        if not 1 <= capacity <= 1000:
            raise ValueError('invalid book capacity')
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'execution', 'book-slots')
            if (await conn.execute(select(slots.c.job_id).where(slots.c.job_id == jid))).first():
                return True
            if (await conn.execute(select(func.count()).select_from(slots))).scalar_one() >= capacity:
                return False
            job = await self.store._job(conn, jid)
            if job['status'] in {'cancelled', 'completed', 'failed', 'archived', 'deleted', 'paused'}:
                return False
            await conn.execute(slots.insert().values(job_id=jid, acquired=self.store.clock()))
            return True

    async def release(self, jid):
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'execution', 'book-slots')
            await conn.execute(slots.delete().where(slots.c.job_id == jid))

    async def pending(self, jid=None, limit=100):
        query = select(commands).where(commands.c.status.in_(['pending', 'delivered']))
        if jid:
            query = query.where(commands.c.job_id == jid)
        async with self.store.db.transaction() as conn:
            return [dict(r) for r in (await conn.execute(query.order_by(commands.c.created, commands.c.id).limit(limit))).mappings()]

    async def recover_claims(self, jid):
        """Only after Temporal's prior activity attempt is closed/expired.

        Each old attempt remains recorded. Old processes cannot publish into a
        newer attempt, even if a remote provider finishes after its lease expires.
        """
        async with self.store.db.transaction() as conn:
            await self.store._job(conn, jid)
            await conn.execute(attempts.update().where(and_(attempts.c.job_id == jid,
                attempts.c.status == 'running')).values(status='interrupted', ended=self.store.clock()))
            await conn.execute(chunks.update().where(and_(chunks.c.job_id == jid,
                chunks.c.status == 'running')).values(status='pending', updated=self.store.clock()))

    async def settle_cancel(self, jid):
        """After downstream acknowledgements and local task joins, never before."""
        async with self.store.db.transaction() as conn:
            job = await self.store._job(conn, jid)
            if job['status'] != 'cancel_requested':
                raise Conflict('cancel not requested')
            await conn.execute(attempts.update().where(and_(attempts.c.job_id == jid,
                attempts.c.status == 'running')).values(status='discarded', ended=self.store.clock()))
            await conn.execute(chunks.update().where(and_(chunks.c.job_id == jid,
                chunks.c.status.in_(['pending', 'running']))).values(status='cancelled', updated=self.store.clock()))

    async def fail(self, jid, error):
        async with self.store.db.transaction() as conn:
            job = await self.store._job(conn, jid)
            if job['status'] == 'running':
                from .analytics import record_book
                await record_book(conn, self.store, job, 'failed')
                await self.store._state(conn, job, 'failed')
                await conn.execute(stages.update().where(and_(stages.c.job_id == jid,
                    stages.c.status == 'running')).values(status='failed', updated=self.store.clock()))
                await self.store._audit(conn, 'worker', 'job_failed', jid, {'error_type': error})

    async def heartbeat(self, worker, kind, capacity, detail=None):
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'worker', worker)
            row = (await conn.execute(select(workers.c.id).where(workers.c.id == worker))).first()
            values = dict(kind=kind, capacity=capacity, heartbeat=self.store.clock(), detail=encoded(detail or {}))
            if row:
                await conn.execute(workers.update().where(workers.c.id == worker).values(**values))
            else:
                await conn.execute(workers.insert().values(id=worker, **values))
