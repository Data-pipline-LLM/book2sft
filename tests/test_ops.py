import os
import asyncio
import tempfile
import unittest
from pathlib import Path
from sqlalchemy import select
from backend.ops.db import Database
from backend.ops.store import Store, Conflict, Forbidden
from backend.ops.schema import commands, chunks, attempts, jobs, artifacts, runs, audit, users
from backend.ops.artifacts import LocalArtifacts
from backend.ops.retention import Retention


class OpsCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Sandbox blocks socket wakeups from executor threads. A test-only timer
        # keeps the selector polling; it does not mock the threaded operation.
        loop = asyncio.get_running_loop()
        def selector_tick():
            loop.call_later(0.01, selector_tick)
        selector_tick()
        self.temp = tempfile.TemporaryDirectory()
        self.url = 'sqlite:///' + self.temp.name + '/ops.sqlite'
        self.db = Database(self.url, test_sqlite=True)
        await self.db.migrate()
        self.now = 1000000.0
        self.store = Store(self.db, clock=lambda: self.now)
        self.objects = LocalArtifacts(Path(self.temp.name) / 'artifacts')
        self.retention = Retention(self.store, self.objects)
        self.checkpoints = {}

    async def asyncTearDown(self):
        await self.db.close()
        self.temp.cleanup()

    async def new(self, key='new'):
        return await self.store.create('کتاب آزمایشی', {}, 'op', 'operator', key)

    async def started(self):
        jid = await self.new()
        async with self.db.transaction() as conn:
            cid = (await conn.execute(select(commands.c.id).where(commands.c.job_id == jid))).scalar_one()
        await self.store.acknowledge(cid)
        return jid

    async def chunk(self, jid, cid='chunk-1', ordinal=0):
        await self.store.add_chunk(jid, cid, ordinal, 's3://test/input')
        return await self.store.begin_chunk(jid, cid, 'test-worker')

    async def finish(self, claim, error=None):
        aid = claim['attempt_id']
        async with self.db.transaction() as conn:
            jid = (await conn.execute(select(attempts.c.job_id).where(attempts.c.id == aid))).scalar_one()
        if aid not in self.checkpoints:
            manifest = self.objects.put(b'durable synthetic result')
            reference = await self.retention.register(jid, 'chunk_outputs', manifest)
            self.checkpoints[aid] = (reference, manifest['sha256'])
        reference, digest = self.checkpoints[aid]
        return await self.store.finish_chunk(aid, reference, digest, error)

    async def test_idempotency(self):
        jid = await self.new()
        self.assertEqual(jid, await self.new())
        with self.assertRaises(Conflict):
            await self.store.create('different', {}, 'op', 'operator', 'new')
        cmd = await self.store.command(jid, 'pause', 'op', 'operator', 'pause')
        self.assertEqual(cmd['id'], (await self.store.command(jid, 'pause', 'op', 'operator', 'pause'))['id'])
        with self.assertRaises(Conflict):
            await self.store.command(jid, 'cancel', 'op', 'operator', 'pause')
        await self.store.acknowledge(cmd['id'])
        self.assertEqual((await self.store.get(jid))['status'], 'paused')

    async def test_pause_resume_safe_boundary(self):
        jid = await self.started()
        claim = await self.chunk(jid)
        cmd = await self.store.command(jid, 'pause', 'op', 'operator', 'pause')
        with self.assertRaises(Conflict):
            await self.store.acknowledge(cmd['id'])
        await self.store.add_chunk(jid, 'chunk-2', 1, 'input2')
        with self.assertRaises(Conflict):
            await self.store.begin_chunk(jid, 'chunk-2', 'w')
        self.assertTrue(await self.finish(claim))
        await self.store.acknowledge(cmd['id'])
        cmd = await self.store.command(jid, 'resume', 'op', 'operator', 'resume')
        await self.store.acknowledge(cmd['id'])
        self.assertTrue((await self.store.begin_chunk(jid, 'chunk-1', 'w'))['cached'])
        self.assertFalse((await self.store.begin_chunk(jid, 'chunk-2', 'w'))['cached'])

    async def test_cancel_late_result(self):
        jid = await self.started()
        claim = await self.chunk(jid)
        cmd = await self.store.command(jid, 'cancel', 'op', 'operator', 'cancel')
        with self.assertRaises(Conflict):
            await self.store.acknowledge(cmd['id'])
        self.assertFalse(await self.finish(claim))
        await self.store.acknowledge(cmd['id'])
        self.assertEqual((await self.store.get(jid))['status'], 'cancelled')
        async with self.db.transaction() as conn:
            row = (await conn.execute(select(chunks))).mappings().one()
            self.assertIsNone(row['output_ref'])
            self.assertEqual((await conn.execute(select(attempts.c.status))).scalar_one(), 'discarded')

    async def test_retry_failed_chunk_keeps_success(self):
        jid = await self.started()
        await self.finish(await self.chunk(jid))
        await self.finish(await self.chunk(jid, 'chunk-2', 1), {'code': 503})
        await self.store.complete(jid)
        self.assertEqual((await self.store.get(jid))['status'], 'failed')
        cmd = await self.store.command(jid, 'retry_chunk', 'op', 'operator', 'retry', {'chunk_id': 'chunk-2'})
        await self.store.acknowledge(cmd['id'])
        self.assertTrue((await self.store.begin_chunk(jid, 'chunk-1', 'w'))['cached'])
        await self.finish(await self.store.begin_chunk(jid, 'chunk-2', 'w'))
        await self.store.complete(jid)
        self.assertEqual((await self.store.get(jid))['status'], 'completed')
        async with self.db.transaction() as conn:
            self.assertEqual(len((await conn.execute(select(attempts))).all()), 3)

    async def test_restart_crash_fencing(self):
        jid = await self.started()
        await self.finish(await self.chunk(jid))
        old = await self.chunk(jid, 'chunk-2', 1)
        await self.db.close()
        self.db = Database(self.url, test_sqlite=True)
        await self.db.migrate()
        self.store = Store(self.db, clock=lambda: self.now)
        self.retention = Retention(self.store, self.objects)
        await self.store.recover(jid)
        await self.store.resume_recovered(jid)
        self.assertTrue((await self.store.begin_chunk(jid, 'chunk-1', 'new-worker'))['cached'])
        with self.assertRaises(Conflict):
            await self.finish(old)
        await self.finish(await self.store.begin_chunk(jid, 'chunk-2', 'new-worker'))
        await self.store.complete(jid)

    async def test_duplicate_claim_publish_and_conflicting_commands(self):
        jid = await self.started()
        claim = await self.chunk(jid)
        with self.assertRaises(Conflict):
            await self.store.begin_chunk(jid, 'chunk-1', 'other')
        with self.assertRaises(Conflict):
            await self.store.add_chunk(jid, 'chunk-1', 0, 'changed input')
        self.assertTrue(await self.finish(claim))
        self.assertTrue(await self.finish(claim))
        self.assertEqual((await self.store.overview())['attempts_finished'], 1)
        await self.store.command(jid, 'pause', 'op', 'operator', 'pause')
        cancellation = await self.store.command(jid, 'cancel', 'op', 'operator', 'cancel')
        self.assertEqual(cancellation['status'], 'pending')
        self.assertEqual((await self.store.get(jid))['status'], 'cancel_requested')

    async def test_rbac_safe_delete_audit(self):
        jid = await self.started()
        with self.assertRaises(Forbidden):
            await self.store.command(jid, 'pause', 'v', 'viewer', 'x')
        with self.assertRaises(Conflict):
            await self.store.command(jid, 'delete', 'a', 'admin', 'x')
        with self.assertRaises(Forbidden):
            await self.store.command(jid, 'delete', 'op', 'operator', 'x')
        await self.finish(await self.chunk(jid))
        await self.store.complete(jid)
        with self.assertRaises(Conflict):
            await self.store.command(jid, 'delete', 'a', 'admin', 'x')
        await self.store.command(jid, 'disable_recovery', 'a', 'admin', 'disable')
        await self.store.command(jid, 'delete', 'a', 'admin', 'delete')
        self.assertEqual((await self.store.get(jid))['status'], 'deleted')
        async with self.db.transaction() as conn:
            self.assertGreater(len((await conn.execute(select(audit))).all()), 0)

    async def test_retention_preview_protection_dry_run_control(self):
        jid = await self.started()
        await self.retention.policy('chunk_outputs', 1, True, 'a', 'admin')
        manifest = self.objects.put(b'checkpoint')
        aid = await self.retention.register(jid, 'chunk_outputs', manifest)
        self.now += 2 * 86400
        self.assertEqual((await self.retention.preview('admin'))['count'], 0)
        await self.finish(await self.chunk(jid))
        await self.store.complete(jid)
        self.assertEqual((await self.retention.preview('admin'))['count'], 0)
        await self.store.command(jid, 'disable_recovery', 'a', 'admin', 'disable')
        self.assertEqual((await self.retention.preview('admin'))['count'], 1)
        dry = await self.retention.start('a', 'admin')
        await self.retention.batch(dry)
        self.assertEqual(self.objects.get(manifest['key']), b'checkpoint')
        run = await self.retention.start('a', 'admin', False)
        await self.retention.control(run, 'pause', 'a', 'admin')
        self.assertEqual(await self.retention.batch(run), 0)
        await self.retention.control(run, 'resume', 'a', 'admin')
        self.assertEqual(await self.retention.batch(run), 1)
        self.assertEqual(await self.retention.batch(run), 0)
        async with self.db.transaction() as conn:
            self.assertEqual((await conn.execute(select(artifacts.c.status).where(artifacts.c.id == aid))).scalar_one(), 'expired')
            row = (await conn.execute(select(runs).where(runs.c.id == run))).mappings().one()
            self.assertEqual(row['deleted'], 1)
            self.assertEqual(row['status'], 'completed')
        with self.assertRaises(FileNotFoundError):
            self.objects.get(manifest['key'])

    async def test_path_traversal_and_symlink(self):
        with self.assertRaises(Forbidden):
            await self.retention.preview('operator')
        for key in ['../../outside', '/etc/passwd', 'a/../../b']:
            with self.assertRaises(ValueError):
                self.objects.delete(key)
        manifest = self.objects.put(b'payload')
        prefix, digest = manifest['key'].split('/')
        target = Path(self.temp.name) / 'outside'
        target.mkdir()
        (target / digest).write_bytes(b'protected')
        os.rename(self.objects.root / prefix, self.objects.root / (prefix + '-original'))
        os.symlink(target, self.objects.root / prefix)
        with self.assertRaises(OSError):
            self.objects.delete(manifest['key'])
        self.assertEqual((target / digest).read_bytes(), b'protected')

    async def test_bounded_cursor_page(self):
        for i in range(7):
            await self.new(str(i))
        seen, cursor = [], None
        while True:
            page = await self.store.page(jobs, cursor=cursor, limit=2)
            self.assertLessEqual(len(page['items']), 2)
            seen += [r['id'] for r in page['items']]
            cursor = page['next_cursor']
            if cursor is None:
                break
        self.assertEqual(len(set(seen)), 7)
        with self.assertRaises(ValueError):
            await self.store.page(jobs, limit=500000)

    async def test_production_rejects_sqlite(self):
        with self.assertRaises(ValueError):
            Database('sqlite://')

    async def test_api_original_ui_trusted_mode_without_login_and_unavailable_executor(self):
        import httpx
        from unittest.mock import patch
        from backend.ops.api import create_app
        app = create_app(self.db, self.objects)
        app.state.store.clock = lambda: self.now
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            with patch.dict(os.environ, {'OPS_TRUSTED_LOCAL_MODE': 'false'}):
                self.assertEqual((await client.get('/v1/ops/jobs')).status_code, 503)
            with patch.dict(os.environ, {'OPS_TRUSTED_LOCAL_MODE': 'true'}):
                home = await client.get('/')
                self.assertEqual(home.status_code, 200)
                self.assertIn('مانیتورینگ پایپلاین', home.text)
                self.assertNotIn('ورود امن', home.text)
                self.assertEqual((await client.get('/v1/auth/login')).status_code, 404)
                self.assertEqual((await client.get('/v1/admin/users')).status_code, 404)
                self.assertEqual((await client.get('/static/monitoring.js')).status_code, 200)
                self.assertEqual((await client.get('/v1/ops/overview')).status_code, 200)
                self.assertEqual((await client.get('/v1/ops/jobs?limit=500000')).status_code, 422)
                self.assertEqual((await client.get('/v1/ops/jobs')).json()['items'], [])
                self.assertEqual((await client.post('/v1/ops/jobs', headers={'Idempotency-Key': 'x'}, json={'title': 'x'})).status_code, 503)
                self.assertEqual((await client.get('/ready')).status_code, 503)

    async def test_jobs_page_newest_first_and_cursor(self):
        from backend.ops.queries import jobs_page
        first = await self.store.create('first', {}, 'operator', 'operator', 'key1')
        self.now += 10
        second = await self.store.create('second', {}, 'operator', 'operator', 'key2')
        self.now += 10
        third = await self.store.create('third', {}, 'operator', 'operator', 'key3')
        page = await jobs_page(self.store, limit=2)
        self.assertEqual([j['id'] for j in page['items']], [third, second])
        self.assertIsNotNone(page['next_cursor'])
        rest = await jobs_page(self.store, cursor=page['next_cursor'], limit=2)
        self.assertEqual([j['id'] for j in rest['items']], [first])
        self.assertIsNone(rest['next_cursor'])

    async def test_duplicate_publish_hash_conflict(self):
        jid = await self.started()
        claim = await self.chunk(jid)
        await self.finish(claim)
        with self.assertRaises(Conflict):
            await self.store.finish_chunk(claim['attempt_id'], self.checkpoints[claim['attempt_id']][0], 'b' * 64)

    async def test_cleanup_schedule_durable_disabled_and_no_duplicate(self):
        from backend.ops.cleanup_worker import CleanupWorker
        worker = CleanupWorker(self.retention)
        self.assertIsNone(await worker.schedule())
        with self.assertRaises(Forbidden):
            await worker.configure(True, 60, 'op', 'operator')
        await worker.configure(False, 60, 'a', 'admin')
        self.now += 100
        self.assertIsNone(await worker.schedule())
        await worker.configure(True, 60, 'a', 'admin')
        self.now += 61
        run = await worker.schedule()
        self.assertIsNotNone(run)
        self.assertIsNone(await worker.schedule())
        await self.retention.control(run, 'pause', 'a', 'admin')
        self.now += 61
        self.assertIsNone(await worker.schedule())
        await self.retention.control(run, 'resume', 'a', 'admin')
        self.assertEqual(await worker.tick(), 1)
        async with self.db.transaction() as conn:
            self.assertEqual((await conn.execute(select(runs.c.status).where(runs.c.id == run))).scalar_one(), 'completed')

    async def test_checkpoint_requires_registered_output(self):
        jid = await self.started()
        claim = await self.chunk(jid)
        with self.assertRaises(Conflict):
            await self.store.finish_chunk(claim['attempt_id'], 'unregistered-output', 'a' * 64)

    async def test_backup_restore_checksum_and_refuse_existing(self):
        from backend.ops.backup import backup, restore
        jid = await self.started()
        await self.finish(await self.chunk(jid))
        path = Path(self.temp.name) / 'backup.jsonl'
        result = await backup(self.db, path)
        self.assertGreater(result['rows'], 0)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(Conflict):
            await restore(self.db, path)
        target = Database('sqlite:///' + self.temp.name + '/restored.sqlite', test_sqlite=True)
        await target.migrate()
        try:
            await restore(target, path)
            copy = Store(target)
            self.assertEqual((await copy.get(jid))['status'], 'running')
            self.assertTrue((await copy.begin_chunk(jid, 'chunk-1', 'new-worker'))['cached'])
        finally:
            await target.close()
        broken = Path(self.temp.name) / 'broken.jsonl'
        broken.write_bytes(path.read_bytes().replace(b'"sha256":', b'"sha256":"corrupt","ignored":', 1))
        clean = Database('sqlite:///' + self.temp.name + '/clean.sqlite', test_sqlite=True)
        await clean.migrate()
        try:
            with self.assertRaises(ValueError):
                await restore(clean, broken)
            self.assertEqual((await Store(clean).page(jobs))['items'], [])
        finally:
            await clean.close()
