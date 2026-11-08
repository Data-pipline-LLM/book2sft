"""Persistent cleanup scheduling and bounded worker batches.

Run in a separate process. A scheduler tick never enables cleanup by default.
"""
import asyncio
import json
import os
from sqlalchemy import select
from .schema import settings, runs
from .store import authorize, uid, encoded
from .retention import Retention


class CleanupWorker:
    def __init__(self, retention):
        self.retention = retention
        self.store = retention.store

    async def configure(self, enabled, interval_seconds, actor, role):
        authorize(role, 'admin')
        if type(interval_seconds) is not int or not 60 <= interval_seconds <= 31 * 86400:
            raise ValueError('schedule interval must be 60 seconds..31 days')
        value = {'enabled': bool(enabled), 'interval_seconds': interval_seconds,
                 'next_run': self.store.clock() + interval_seconds, 'timezone': 'UTC'}
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'settings', 'cleanup')
            old = (await conn.execute(select(settings).where(settings.c.id == 'cleanup'))).mappings().first()
            if old:
                await conn.execute(settings.update().where(settings.c.id == 'cleanup').values(
                    value=encoded(value), updated=self.store.clock()))
            else:
                await conn.execute(settings.insert().values(id='cleanup', value=encoded(value), updated=self.store.clock()))
            await self.store._audit(conn, actor, 'cleanup_schedule', 'cleanup',
                {'before': json.loads(old['value']) if old else None, 'after': value})
        return value

    async def schedule(self):
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'settings', 'cleanup')
            row = (await conn.execute(select(settings).where(settings.c.id == 'cleanup').with_for_update())).mappings().first()
            if not row:
                return None
            config = json.loads(row['value'])
            if not config['enabled'] or config['next_run'] > self.store.clock():
                return None
            config['next_run'] = self.store.clock() + config['interval_seconds']
            await conn.execute(settings.update().where(settings.c.id == 'cleanup').values(
                value=encoded(config), updated=self.store.clock()))
            # A paused run remains paused, without spawning another scheduled run.
            active = (await conn.execute(select(runs.c.id).where(runs.c.status.in_(['running', 'paused'])).limit(1))).first()
            if active:
                return None
            rid, now = uid(), self.store.clock()
            await conn.execute(runs.insert().values(id=rid, status='running', actor='scheduler', dry_run=0,
                created=now, updated=now, deleted=0, bytes=0))
            await self.store._audit(conn, 'scheduler', 'retention_start', rid, {'scheduled': True})
            return rid

    async def tick(self):
        await self.schedule()
        async with self.store.db.transaction() as conn:
            ids = (await conn.execute(select(runs.c.id).where(runs.c.status == 'running')
                .order_by(runs.c.created).limit(10))).scalars().all()
        for rid in ids:
            from .metadata_retention import MetadataRetention
            processed = await MetadataRetention(self.store).batch(rid, limit=100)
            if not processed:
                await self.retention.batch(rid, limit=100)
        return len(ids)


async def main():
    from .db import Database
    from .store import Store
    from .artifacts import S3Artifacts
    db = Database(os.environ['OPS_DATABASE_URL'])
    objects = S3Artifacts(os.environ['OPS_S3_BUCKET'], endpoint_url=os.environ.get('OPS_S3_ENDPOINT') or None)
    worker = CleanupWorker(Retention(Store(db), objects))
    try:
        while True:
            from .runtime import Runtime
            import socket
            await Runtime(worker.store).heartbeat(socket.gethostname(), 'cleanup', 1)
            await worker.tick()
            await asyncio.sleep(2)
    finally:
        await db.close()


if __name__ == '__main__':
    asyncio.run(main())
