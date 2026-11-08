"""Bounded-memory overview queries for the legacy monitoring index.

SQLite performs historical aggregation/sorting. Python receives aggregate groups
and at most two rows per percentile, never all historical job records.
"""
import math
from datetime import datetime, timedelta, timezone


def summary(conn, days):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat() if days else ''
    where = ' WHERE created_at >= ?' if since else ''
    params = [since] if since else []
    base = 'WITH filtered AS (SELECT * FROM jobs' + where + ') '

    def groups(column):
        return dict(conn.execute(base + f'SELECT {column}, COUNT(*) FROM filtered GROUP BY {column}', params))

    statuses, inputs = groups('status'), groups('input_type')
    total, chunks, failed, generated, selected, avg = conn.execute(base + '''
        SELECT COUNT(*), COALESCE(SUM(chunk_count),0), COALESCE(SUM(failed_chunks),0),
        COALESCE(SUM(generated_pairs),0), COALESCE(SUM(selected_pairs),0), AVG(duration_sec)
        FROM filtered''', params).fetchone()

    def quantile(source, condition, count, p, prefix=base, extra=()):
        if not count:
            return None
        index = (count - 1) * p / 100
        low, high = math.floor(index), math.ceil(index)
        rows = conn.execute(prefix + f'SELECT value FROM {source} WHERE {condition} ORDER BY value LIMIT ? OFFSET ?',
                            [*params, *extra, high - low + 1, low]).fetchall()
        return round(float(rows[0][0]) + (float(rows[-1][0]) - float(rows[0][0])) * (index - low), 2)

    durations = base + ', duration_values AS (SELECT duration_sec AS value FROM filtered WHERE duration_sec IS NOT NULL) '
    duration_count = conn.execute(base + 'SELECT COUNT(duration_sec) FROM filtered', params).fetchone()[0]
    daily = [dict(r) for r in conn.execute(base + '''
        SELECT substr(created_at,1,10) AS date, COUNT(*) AS total,
               SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
               SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed
        FROM filtered GROUP BY substr(created_at,1,10) ORDER BY date''', params)]
    errors = [dict(r) for r in conn.execute(base + '''
        SELECT CASE WHEN error_code != '' THEN error_code ELSE 'PARTIAL_SFT_CHUNK_FAILURES' END AS code,
               COUNT(*) AS count FROM filtered
        WHERE error_code != '' OR failed_chunks > 0 GROUP BY code ORDER BY count DESC, code LIMIT 12''', params)]
    internal = dict(conn.execute(base + '''
        SELECT metric.key, SUM(CASE WHEN metric.type IN ('integer','real') THEN metric.value ELSE 0 END) AS value
        FROM filtered, json_each(filtered.internal_timings_json) AS metric
        GROUP BY metric.key ORDER BY value DESC, metric.key LIMIT 25''', params))
    from .monitoring import STAGE_GROUPS
    # Parameterized CASE maps only the fixed stage vocabulary, not user input.
    stage_case = 'CASE stage.key ' + ' '.join('WHEN ? THEN ?' for _ in STAGE_GROUPS) + ' END'
    stage_params = [value for pair in STAGE_GROUPS.items() for value in pair]
    stages = base + ''', stage_rows AS (
        SELECT filtered.job_id, ''' + stage_case + ''' AS name,
          COALESCE(json_extract(stage.value,'$.status'),'waiting') AS state,
          CAST(COALESCE(json_extract(stage.value,'$.elapsed_sec'),0) AS REAL) AS elapsed
        FROM filtered, json_each(filtered.timings_json) AS stage WHERE stage.type='object'
      ), per_job AS (
        SELECT job_id, name,
          SUM(CASE WHEN state IN ('completed','failed') THEN elapsed ELSE 0 END) AS value,
          SUM(CASE WHEN state IN ('completed','failed') THEN 1 ELSE 0 END) AS measured,
          MAX(CASE state WHEN 'failed' THEN 4 WHEN 'running' THEN 3 WHEN 'completed' THEN 2
                         WHEN 'waiting' THEN 0 ELSE 1 END) AS rank
        FROM stage_rows WHERE name IS NOT NULL GROUP BY job_id,name
      ) '''
    services = []
    aggregates = conn.execute(stages + '''SELECT name, COUNT(*) AS count, AVG(value) AS avg
        FROM per_job WHERE measured > 0 GROUP BY name ORDER BY name''', [*params, *stage_params]).fetchall()
    for name, count, mean in aggregates:
        state_rows = conn.execute(stages + '''SELECT rank,COUNT(*) FROM per_job
            WHERE name=? AND rank>0 GROUP BY rank''', [*params, *stage_params, name])
        states = {{4: 'failed', 3: 'running', 2: 'completed', 1: 'skipped'}[rank]: n for rank, n in state_rows}
        # stage-case bindings occur between the period bindings and the group name.
        def stage_quantile(p):
            return quantile('per_job', 'measured>0 AND name=?', count, p,
                            prefix=stages, extra=(*stage_params, name))
        services.append({'name': name, 'jobs': count, 'avg_sec': round(mean, 2),
                         'p50_sec': stage_quantile(50), 'p95_sec': stage_quantile(95), 'states': states})
    return {'period_days': days, 'total_jobs': total, 'statuses': statuses, 'inputs': inputs,
            'total_chunks': chunks, 'failed_chunks': failed, 'generated_pairs': generated,
            'selected_pairs': selected,
            'job_success_rate': round(100 * statuses.get('completed', 0) / max(1, statuses.get('completed', 0) + statuses.get('failed', 0)), 1),
            'chunk_failure_rate': round(100 * failed / max(1, chunks), 1),
            'duration_sec': {'avg': round(avg, 2) if avg is not None else None,
                'p50': quantile('duration_values', '1=1', duration_count, 50, prefix=durations),
                'p95': quantile('duration_values', '1=1', duration_count, 95, prefix=durations)},
            'services': services, 'internal_timings_sum_sec': internal, 'errors': errors, 'daily': daily}
