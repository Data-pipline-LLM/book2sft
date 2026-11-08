"""Redact expired details while preserving operational counts, IDs and audit."""
from sqlalchemy import select, and_, exists, func
from .schema import jobs, chunks, attempts, events, policies, metadata_expiry, runs, stages
from .store import encoded


class MetadataRetention:
    CATEGORIES = ('job_metadata', 'task_details', 'error_logs', 'monitoring_events')

    def __init__(self, store):
        self.store = store

    def condition(self, category, days):
        seen = exists(select(metadata_expiry.c.job_id).where(and_(
            metadata_expiry.c.job_id == jobs.c.id, metadata_expiry.c.category == category)))
        return and_(jobs.c.status.in_(['completed', 'failed', 'cancelled', 'archived', 'deleted']),
            jobs.c.resumable == 0, jobs.c.updated < self.store.clock() - days * 86400, ~seen)

    async def preview(self):
        result = {}
        async with self.store.db.transaction() as conn:
            settings = (await conn.execute(select(policies).where(and_(policies.c.enabled == 1,
                policies.c.category.in_(self.CATEGORIES))))).mappings().all()
            for setting in settings:
                result[setting['category']] = (await conn.execute(select(func.count()).select_from(jobs).where(
                    self.condition(setting['category'], setting['days'])))).scalar_one()
        return result

    async def batch(self, rid, limit=100):
        count = 0
        async with self.store.db.transaction() as conn:
            run = (await conn.execute(select(runs).where(runs.c.id == rid))).mappings().one()
            if run['status'] != 'running' or run['dry_run']:
                return 0
            settings = (await conn.execute(select(policies).where(and_(policies.c.enabled == 1,
                policies.c.category.in_(self.CATEGORIES))))).mappings().all()
        for setting in settings:
            category = setting['category']
            async with self.store.db.transaction() as conn:
                ids = (await conn.execute(select(jobs.c.id).where(self.condition(category, setting['days']))
                    .order_by(jobs.c.id).limit(limit - count))).scalars().all()
            for jid in ids:
                async with self.store.db.transaction() as conn:
                    run = (await conn.execute(select(runs).where(runs.c.id == rid).with_for_update())).mappings().one()
                    if run['status'] != 'running':
                        return count
                    job = await self.store._job(conn, jid)
                    # Repeat the complete policy/status/expiry check under the job lock.
                    if not (await conn.execute(select(jobs.c.id).where(and_(jobs.c.id == jid,
                        self.condition(category, setting['days']))))).first():
                        continue
                    if category == 'job_metadata':
                        await conn.execute(jobs.update().where(jobs.c.id == jid).values(title='[expired]', config='{}'))
                        await conn.execute(stages.update().where(stages.c.job_id == jid).values(snapshot='{"expired":true}'))
                    elif category == 'task_details':
                        await conn.execute(attempts.update().where(attempts.c.job_id == jid).values(
                            input_ref=None, output_ref=None, model=None, quality=None))
                    elif category == 'error_logs':
                        await conn.execute(attempts.update().where(attempts.c.job_id == jid).values(error=None))
                    elif category == 'monitoring_events':
                        await conn.execute(events.delete().where(and_(events.c.job_id == jid,
                            events.c.at < self.store.clock() - setting['days'] * 86400)))
                    await conn.execute(metadata_expiry.insert().values(job_id=jid, category=category, at=self.store.clock()))
                    await self.store._audit(conn, run['actor'], 'metadata_expired', jid, {'category': category})
                    count += 1
                if count >= limit:
                    return count
        return count
