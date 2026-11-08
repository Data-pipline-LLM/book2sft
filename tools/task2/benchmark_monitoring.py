"""Measure the existing SQLite overview on synthetic metadata, never real jobs.

Run from book2sft: python3 tools/task2/benchmark_monitoring.py --jobs 500000
Uses only the standard library. Temporary data is removed automatically.
"""
from __future__ import annotations

import argparse
import json
import resource
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from backend.monitoring import MonitorIndex, extract


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jobs', type=int, default=10000)
    parser.add_argument('--baseline-source', help='Trusted original monitoring.py extracted from Git')
    args = parser.parse_args()
    if not 1 <= args.jobs <= 500000:
        parser.error('--jobs must be between 1 and 500000')
    stamp = datetime.now(timezone.utc).isoformat()
    with tempfile.TemporaryDirectory(prefix='book2sft-task2-') as root:
        index_type = MonitorIndex
        if args.baseline_source:
            import importlib.util
            spec = importlib.util.spec_from_file_location('trusted_baseline', args.baseline_source)
            baseline = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(baseline)
            index_type = baseline.MonitorIndex
        index = index_type(Path(root) / 'synthetic.sqlite3', Path(root))
        index.initialize()
        started = time.perf_counter()
        with index.connection() as conn:
            for start in range(0, args.jobs, 250):
                rows = [extract({
                    'job_id': f'synthetic-{number}', 'created_at': stamp,
                    'updated_at': stamp, 'status': 'completed', 'stage': 'completed',
                    'total_elapsed_sec': 60,
                    'metrics': {'pipeline': {'sft': {
                        'total_chunks': 4, 'completed_chunks': 4,
                        'generated_pairs': 12, 'selected_pairs': 8}}},
                }) for number in range(start, min(start + 250, args.jobs))]
                index.upsert_many(conn, rows)
        seed_seconds = time.perf_counter() - started
        rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        started = time.perf_counter()
        overview = index.overview(days=0)
        cold_seconds = time.perf_counter() - started
        rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        assert overview['total_jobs'] == args.jobs
        assert overview['statuses'] == {'completed': args.jobs}
        assert overview['total_chunks'] == args.jobs * 4
        assert overview['selected_pairs'] == args.jobs * 8
        started = time.perf_counter()
        cached = index.overview(days=0)
        cached_seconds = time.perf_counter() - started
        assert cached == overview
        print(json.dumps({
            'synthetic_jobs': args.jobs, 'seed_seconds': round(seed_seconds, 4),
            'overview_cold_seconds': round(cold_seconds, 4),
            'overview_cached_seconds': round(cached_seconds, 6),
            'peak_rss_before_kib_linux': rss_before,
            'peak_rss_after_kib_linux': rss_after,
            'peak_rss_growth_kib_linux': rss_after - rss_before,
            'counts_verified': True,
            'scope': 'existing SQLite index only; no HTTP, LLM, workflow or concurrency validation',
        }, indent=2))


if __name__ == '__main__':
    main()
