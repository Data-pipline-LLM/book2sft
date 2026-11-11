"""Contract tests for read-only stage telemetry and OCR JSON semantics."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from sqlalchemy import select
from backend.ops.db import Database
from backend.ops.live_pipeline import live_snapshot
from backend.ops.runtime import Runtime
from backend.ops.schema import commands
from backend.ops.store import Store


class LivePipelineTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database('sqlite:///' + str(Path(self.tmp.name) / 'ops.sqlite'), test_sqlite=True)
        await self.db.migrate()
        self.store = Store(self.db, clock=lambda: 1700000000.0)
        self.runtime = Runtime(self.store)

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def create(self, name, input_type='pdf', start=False):
        jid = await self.store.create(name, {'input_type': input_type}, 'op', 'operator', name)
        if start:
            async with self.db.transaction() as conn:
                cid = (await conn.execute(select(commands.c.id).where(commands.c.job_id == jid))).scalar_one()
            await self.store.acknowledge(cid)
        return jid

    async def test_live_counts_and_json_ocr_skip(self):
        pdf_ocr = await self.create('PDF OCR', start=True)
        await self.runtime.claim_stage(pdf_ocr, 'ocr', 'ocr-lease')
        await self.runtime.checkpoint(pdf_ocr, 'ocr', 'running', {'job': {
            'stage': 'ocr', 'progress': 15, 'message': 'OCR in progress'
        }}, lease='ocr-lease')
        json_chunker = await self.create('Supplied OCR JSON', 'ocr_json', start=True)
        await self.runtime.claim_stage(json_chunker, 'ocr', 'json-lease')
        await self.runtime.checkpoint(json_chunker, 'ocr', 'completed',
            {'job': {'stage': 'ocr', 'progress': 100, 'ocr_skipped': True}}, lease='json-lease')
        await self.runtime.claim_stage(json_chunker, 'chunker', 'chunk-lease')
        await self.runtime.checkpoint(json_chunker, 'chunker', 'running',
            {'job': {'stage': 'chunking', 'progress': 22, 'metrics': {'pipeline': {
                'chunker': {'chunk_count': 12}, 'sft': {'total_chunks': 12}
            }}}}, lease='chunk-lease')
        queued = await self.create('Queued PDF')
        pdf_sft = await self.create('PDF SFT', start=True)
        await self.runtime.claim_stage(pdf_sft, 'sft', 'sft-lease')
        await self.runtime.checkpoint(pdf_sft, 'sft', 'running', {'job': {
            'stage': 'sft_generation', 'progress': 68,
            'metrics': {'pipeline': {'sft': {'total_chunks': 8, 'completed_chunks': 5,
                                            'active_chunks': 2, 'generated_pairs': 36}}}
        }}, lease='sft-lease')
        result = await live_snapshot(self.store)
        self.assertEqual(result['total_jobs'], 4)
        self.assertEqual(result['stages']['ocr'], 1)
        self.assertEqual(result['stages']['chunker'], 1)
        self.assertEqual(result['stages']['sft'], 1)
        self.assertEqual(result['queued'], 1)
        self.assertEqual(result['between_stages'], 0)
        books = {row['id']: row for row in result['items']}
        self.assertEqual(books[json_chunker]['stage'], 'chunker')
        self.assertTrue(books[json_chunker]['ocr_skipped'])
        self.assertEqual(books[json_chunker]['input_type'], 'ocr_json')
        self.assertEqual(books[pdf_sft]['sft_chunks_active'], 2)
        self.assertEqual(books[pdf_sft]['sft_chunks_completed'], 5)
        self.assertEqual(books[queued]['stage'], 'queued')
        self.assertIsNone(books[pdf_sft]['llm_calls_per_book'])

    async def test_interstage_is_not_reported_as_ocr_running(self):
        jid = await self.create('Between OCR and Chunker', start=True)
        await self.runtime.claim_stage(jid, 'ocr', 'lease')
        await self.runtime.checkpoint(jid, 'ocr', 'completed', {'job': {'progress': 12}}, lease='lease')
        view = await live_snapshot(self.store)
        self.assertEqual(view['stages']['ocr'], 0)
        self.assertEqual(view['stages']['chunker'], 0)
        self.assertEqual(view['between_stages'], 1)
        self.assertEqual(view['items'][0]['stage'], 'chunker')
        self.assertEqual(view['items'][0]['stage_state'], 'waiting_for_stage')

    async def test_pagination_and_limits(self):
        for n in range(4):
            await self.create(f'Book {n}')
        result = await live_snapshot(self.store, limit=2)
        self.assertEqual(result['total_jobs'], 4)
        self.assertEqual(len(result['items']), 2)
        self.assertTrue(result['more_recent_jobs'])
        with self.assertRaises(ValueError):
            await live_snapshot(self.store, limit=101)


if __name__ == '__main__':
    unittest.main()
