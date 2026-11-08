"""Compare new SQL overview with a supplied original monitoring.py on synthetic data.

Only load trusted project source, e.g. a file extracted using git show.
"""
import importlib.util
import json
import random
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from backend.monitoring import MonitorIndex


def main():
    spec = importlib.util.spec_from_file_location('trusted_monitoring_baseline', sys.argv[1])
    baseline = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline)
    random.seed(247)
    with tempfile.TemporaryDirectory(prefix='task2-monitor-regression-') as root:
        root = Path(root)
        current = MonitorIndex(root / 'new.sqlite', root)
        previous = baseline.MonitorIndex(root / 'old.sqlite', root)
        current.initialize()
        previous.initialize()
        for i in range(600):
            status = random.choice(['completed', 'running', 'failed', 'interrupted', 'queued'])
            timing = {name: {'status': random.choice(['waiting', 'running', 'completed', 'failed', 'skipped']),
                             'elapsed_sec': random.randrange(100)}
                      for name in random.sample(list(baseline.STAGE_GROUPS), random.randrange(8))}
            job = {'job_id': str(i), 'status': status, 'input_type': random.choice(['pdf', 'ocr_json']),
                   'created_at': f'2026-01-{i % 28 + 1:02d}T00:00:00+00:00',
                   'total_elapsed_sec': random.randrange(1, 1000), 'timings': timing,
                   'error': {'code': random.choice(['', '503', 'TIMEOUT'])},
                   'metrics': {'pipeline': {'sft': {'total_chunks': 10,
                       'failed_chunks': random.randrange(3), 'generated_pairs': random.randrange(30),
                       'selected_pairs': random.randrange(10), 'timings_sum_sec': {'request': i % 9}}}}}
            current.upsert(job)
            previous.upsert(job)
        actual, expected = current.overview(0), previous.overview(0)
        actual['errors'].sort(key=lambda r: r['code'])
        expected['errors'].sort(key=lambda r: r['code'])
        assert actual == expected, '\n'.join(f'{key}: {expected[key]!r} != {actual.get(key)!r}'
            for key in expected if expected[key] != actual.get(key))
        empty = MonitorIndex(root / 'empty.sqlite', root)
        empty.initialize()
        result = empty.overview(0)
        assert result['total_jobs'] == 0 and result['duration_sec']['p50'] is None
        print(json.dumps({'synthetic_jobs': 600, 'all_overview_fields_equal': True,
                          'empty_dataset_verified': True}))


if __name__ == '__main__':
    main()
