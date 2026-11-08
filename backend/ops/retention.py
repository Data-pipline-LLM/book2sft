"""Retention independent of generation; all destructive steps recheck job locks."""
import asyncio
from sqlalchemy import select, and_, func
from .schema import artifacts, policies, runs, jobs
from .store import authorize, uid, Conflict, Missing

CATEGORIES = {'temporary_files', 'job_metadata', 'chunk_inputs', 'chunk_outputs',
              'task_details', 'error_logs', 'monitoring_events', 'generated_artifacts',
              'completed_datasets'}
TERMINAL = {'completed', 'failed', 'cancelled', 'archived', 'deleted'}


class Retention:
    def __init__(self, store, objects):
        self.store, self.objects = store, objects

    async def policy(self, category, days, enabled, actor, role):
        authorize(role, 'admin')
        if category not in CATEGORIES or type(days) is not int or not 1 <= days <= 36500:
            raise ValueError('invalid retention category/days')
        async with self.store.db.transaction() as conn:
            # Lock a dedicated category via PostgreSQL advisory lock, including first insert.
            await self.store._serialize_key(conn, 'retention-policy', category)
            old = (await conn.execute(select(policies).where(policies.c.category == category))).mappings().first()
            values = dict(days=days, enabled=int(enabled), updated=self.store.clock())
            if old:
                await conn.execute(policies.update().where(policies.c.category == category).values(**values))
            else:
                await conn.execute(policies.insert().values(category=category, **values))
            await self.store._audit(conn, actor, 'retention_policy', category,
                {'before': dict(old) if old else None, 'after': values, 'applies_to': 'future artifacts'})

    async def register(self, job_id, category, manifest, required=True):
        from .artifacts import validate_key
        validate_key(manifest['key'])
        if category not in CATEGORIES or manifest['sha256'] != manifest['key'].split('/')[1] or manifest['size'] < 0:
            raise ValueError('invalid manifest')
        async with self.store.db.transaction() as conn:
            job = await self.store._job(conn, job_id)
            if job['status'] == 'deleted':
                raise Conflict('job deleted')
            policy = (await conn.execute(select(policies).where(policies.c.category == category))).mappings().first()
            expires = self.store.clock() + policy['days'] * 86400 if policy and policy['enabled'] else None
            aid = uid()
            await conn.execute(artifacts.insert().values(id=aid, job_id=job_id, category=category,
                key=manifest['key'], sha256=manifest['sha256'], size=manifest['size'],
                created=self.store.clock(), expires=expires, required=int(required), status='available'))
            return aid

    def _eligible(self):
        return and_(artifacts.c.status.in_(['available', 'deleting']),
            artifacts.c.expires.is_not(None), artifacts.c.expires <= self.store.clock(),
            jobs.c.status.in_(TERMINAL), jobs.c.resumable == 0)

    async def preview(self, role):
        authorize(role, 'admin')
        async with self.store.db.transaction() as conn:
            count, size = (await conn.execute(select(func.count(), func.coalesce(func.sum(artifacts.c.size), 0))
                .select_from(artifacts.join(jobs, artifacts.c.job_id == jobs.c.id)).where(self._eligible()))).one()
        from .metadata_retention import MetadataRetention
        metadata = await MetadataRetention(self.store).preview()
        return {'count': count, 'bytes': size, 'metadata_jobs_by_category': metadata, 'dry_run': True,
                'protection': 'active and all resumable jobs excluded',
                'impact': 'eligible artifact downloads expire; job summaries and audit remain'}

    async def start(self, actor, role, dry_run=True):
        authorize(role, 'admin')
        rid = uid()
        async with self.store.db.transaction() as conn:
            await conn.execute(runs.insert().values(id=rid, actor=actor, dry_run=int(dry_run),
                status='running', created=self.store.clock(), updated=self.store.clock(), deleted=0, bytes=0))
            await self.store._audit(conn, actor, 'retention_start', rid, {'dry_run': dry_run})
        return rid

    async def control(self, run_id, action, actor, role):
        authorize(role, 'admin')
        async with self.store.db.transaction() as conn:
            row = (await conn.execute(select(runs).where(runs.c.id == run_id).with_for_update())).mappings().first()
            if not row:
                raise Missing(run_id)
            states = {('running', 'pause'): 'paused', ('paused', 'resume'): 'running',
                      ('running', 'cancel'): 'cancelled', ('paused', 'cancel'): 'cancelled'}
            target = states.get((row['status'], action))
            if not target:
                raise Conflict('invalid cleanup transition')
            await conn.execute(runs.update().where(runs.c.id == run_id).values(status=target, updated=self.store.clock()))
            await self.store._audit(conn, actor, 'retention_' + action, run_id, {'status': target})

    async def batch(self, run_id, limit=100):
        if not 1 <= limit <= 250:
            raise ValueError('invalid batch size')
        # A dry run stores aggregate evidence and never touches objects.
        async with self.store.db.transaction() as conn:
            run = (await conn.execute(select(runs).where(runs.c.id == run_id).with_for_update())).mappings().first()
            if not run:
                raise Missing(run_id)
            if run['status'] != 'running':
                return 0
            if run['dry_run']:
                await conn.execute(runs.update().where(runs.c.id == run_id).values(status='completed', updated=self.store.clock()))
                return 0
            candidates = (await conn.execute(select(artifacts.c.id, artifacts.c.job_id)
                .select_from(artifacts.join(jobs, artifacts.c.job_id == jobs.c.id))
                .where(self._eligible()).order_by(artifacts.c.expires, artifacts.c.id).limit(limit))).all()
            if not candidates:
                await conn.execute(runs.update().where(runs.c.id == run_id).values(status='completed', updated=self.store.clock()))
        removed = 0
        for aid, job_id in candidates:
            # All lifecycle operations take the same job lock; a concurrent retry
            # cannot begin while its required artifacts are being deleted.
            async with self.store.db.transaction() as conn:
                run = (await conn.execute(select(runs).where(runs.c.id == run_id).with_for_update())).mappings().one()
                if run['status'] != 'running':
                    break
                job = await self.store._job(conn, job_id)
                artifact = (await conn.execute(select(artifacts).where(artifacts.c.id == aid).with_for_update())).mappings().one()
                if job['status'] not in TERMINAL or job['resumable'] or artifact['status'] not in {'available', 'deleting'}:
                    continue
                try:
                    await asyncio.to_thread(self.objects.delete, artifact['key'])
                except Exception as exc:
                    await conn.execute(runs.update().where(runs.c.id == run_id).values(
                        status='failed', error=type(exc).__name__, updated=self.store.clock()))
                    await self.store._audit(conn, run['actor'], 'retention_error', aid, {'error_type': type(exc).__name__})
                    break
                # If DB commit fails after object deletion, another batch retries
                # the idempotent delete. Only this transaction counts the result.
                await conn.execute(artifacts.update().where(artifacts.c.id == aid).values(status='expired'))
                await conn.execute(runs.update().where(runs.c.id == run_id).values(
                    deleted=runs.c.deleted + 1, bytes=runs.c.bytes + artifact['size'], updated=self.store.clock()))
                await self.store._audit(conn, run['actor'], 'artifact_expired', aid, {'job_id': job_id, 'bytes': artifact['size']})
                removed += 1
        return removed
