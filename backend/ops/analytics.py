"""Minute rollups with exactly-once receipts and bounded chart responses."""
import json
import math
import re
from collections import Counter
from sqlalchemy import select, and_, func
from .schema import rollups, metric_receipts, attempts, jobs, chunks, workers, slots, stages, rejections
from .store import encoded

BUCKETS = [1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 86400]


class Analytics:
    def __init__(self, store):
        self.store = store

    async def observe_chunk(self, jid, aid, result, elapsed):
        minute = int(self.store.clock() // 60) * 60
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'metric', aid)
            if (await conn.execute(select(metric_receipts.c.attempt_id).where(metric_receipts.c.attempt_id == aid))).first():
                return
            attempt = (await conn.execute(select(attempts).where(attempts.c.id == aid))).mappings().one()
            if attempt['status'] not in {'completed', 'failed'}:
                return
            await self.store._serialize_key(conn, 'rollup', str(minute) + ':sft:generation')
            condition = and_(rollups.c.bucket == minute, rollups.c.service == 'sft', rollups.c.stage == 'generation')
            old = (await conn.execute(select(rollups).where(condition))).mappings().first()
            hist = json.loads(old['duration_hist']) if old else [0] * (len(BUCKETS) + 1)
            position = next((i for i, bound in enumerate(BUCKETS) if elapsed <= bound), len(BUCKETS))
            hist[position] += 1
            pairs = result.get('final_pairs') or []
            added_reasons = Counter()
            reasons = Counter(json.loads(old['reasons']) if old else {})
            quality = []
            for pair in pairs:
                if not isinstance(pair, dict):
                    continue
                decision = pair.get('quality_decision', pair.get('final_decision'))
                if decision == 'reject':
                    tags = pair.get('llm_error_tags') or [pair.get('rejection_reason') or 'unspecified']
                    for tag in tags:
                        reason = str(tag)
                        if not re.fullmatch(r'[A-Za-z0-9_:-]{1,80}', reason):
                            reason = 'unclassified'
                        reasons[reason] += 1
                        added_reasons[reason] += 1
                score = pair.get('selection_quality_score', pair.get('final_score', pair.get('quality_score')))
                if isinstance(score, (float, int)) and math.isfinite(score):
                    quality.append(score)
            values = dict(calls=(old['calls'] if old else 0) + 1,
                errors=(old['errors'] if old else 0) + int(bool(result.get('error'))),
                retries=(old['retries'] if old else 0) + int(attempt['number'] > 1),
                generated=(old['generated'] if old else 0) + int(result.get('generated_pairs_count') or 0),
                accepted=(old['accepted'] if old else 0) + int(result.get('quality_pass_count') or 0),
                duration_sum=(old['duration_sum'] if old else 0) + elapsed,
                duration_hist=encoded(hist), quality_sum=(old['quality_sum'] if old else 0) + sum(quality),
                quality_count=(old['quality_count'] if old else 0) + len(quality),
                reasons=encoded(dict(reasons.most_common(50))))
            if old:
                await conn.execute(rollups.update().where(condition).values(**values))
            else:
                await conn.execute(rollups.insert().values(bucket=minute, service='sft', stage='generation', **values))
            await conn.execute(metric_receipts.insert().values(attempt_id=aid, at=self.store.clock()))
            for reason, count in added_reasons.items():
                condition = and_(rejections.c.bucket == minute, rejections.c.reason == reason)
                if (await conn.execute(select(rejections.c.count).where(condition))).first():
                    await conn.execute(rejections.update().where(condition).values(count=rejections.c.count + count))
                else:
                    await conn.execute(rejections.insert().values(bucket=minute, reason=reason, count=count))

    async def series(self, since, until, max_points=240):
        if not math.isfinite(since) or not math.isfinite(until) or not 0 < until - since <= 366 * 86400:
            raise ValueError('invalid chart window; maximum 366 days')
        if not 10 <= max_points <= 1000:
            raise ValueError('max_points must be 10..1000')
        step = max(60, math.ceil((until - since) / (max_points - 1) / 60) * 60)
        bucket = func.floor(rollups.c.bucket / step) * step
        query = select(bucket.label('at'), func.sum(rollups.c.calls).label('calls'),
            func.sum(rollups.c.errors).label('errors'), func.sum(rollups.c.retries).label('retries'),
            func.sum(rollups.c.generated).label('generated'), func.sum(rollups.c.accepted).label('accepted'),
            func.sum(rollups.c.duration_sum).label('duration_sum'), func.sum(rollups.c.quality_sum).label('quality_sum'),
            func.sum(rollups.c.quality_count).label('quality_count')).where(and_(
                rollups.c.bucket >= since, rollups.c.bucket < until)).group_by(bucket).order_by(bucket).limit(max_points + 1)
        async with self.store.db.transaction() as conn:
            rows = [dict(row) for row in (await conn.execute(query)).mappings()]
            from sqlalchemy import cast, Integer
            if self.store.db.test_sqlite:
                values = [func.json_extract(rollups.c.duration_hist, f'$[{i}]') for i in range(len(BUCKETS)+1)]
            else:
                from sqlalchemy.dialects.postgresql import JSONB
                values = [cast(rollups.c.duration_hist, JSONB)[i].astext for i in range(len(BUCKETS)+1)]
            histogram_query = select(*(func.coalesce(func.sum(cast(value,Integer)),0) for value in values)).where(
                and_(rollups.c.bucket >= since, rollups.c.bucket < until))
            histogram = (await conn.execute(histogram_query)).one()
            from .schema import book_rollups
            book_bucket = func.floor(book_rollups.c.bucket / step) * step
            book_points = [dict(row) for row in (await conn.execute(select(book_bucket.label('at'),
                func.sum(book_rollups.c.completed).label('completed'), func.sum(book_rollups.c.failed).label('failed'),
                func.sum(book_rollups.c.lead_time_sum).label('lead_time_sum')).where(and_(book_rollups.c.bucket >= since,
                book_rollups.c.bucket < until)).group_by(book_bucket).order_by(book_bucket).limit(max_points))).mappings()]
            rejected = [dict(row) for row in (await conn.execute(select(rejections.c.reason,
                func.sum(rejections.c.count).label('count')).where(and_(rejections.c.bucket >= since,
                rejections.c.bucket < until)).group_by(rejections.c.reason).order_by(func.sum(rejections.c.count).desc()).limit(20))).mappings()]
        return {'points': rows, 'book_points': book_points, 'rejections': rejected,
                'duration_histogram':{'upper_seconds':BUCKETS+['+Inf'],'counts':list(histogram)},
                'from': since, 'to': until, 'step_seconds': step,
                'source': 'ops_metric_rollups; published attempt receipts', 'timezone': 'UTC',
                'status': 'partial' if rows else 'no_data', 'units': {'duration_sum': 'seconds', 'calls': 'attempts'},
                'acceptance_denominator': 'generated pairs; quality-pass count is not final book selection',
                'updated_at': self.store.clock()}

    async def fleet(self):
        now = self.store.clock()
        async with self.store.db.transaction() as conn:
            fleet = [dict(row) for row in (await conn.execute(select(workers).order_by(workers.c.heartbeat.desc()).limit(200))).mappings()]
            active = (await conn.execute(select(func.count()).select_from(slots))).scalar_one()
        for row in fleet:
            row['health'] = 'healthy' if now - row['heartbeat'] < 30 else 'stale'
        return {'workers': fleet, 'active_book_slots': active, 'as_of': now}


async def record_book(conn, store, job, status):
    from .schema import book_rollups, book_receipts
    identity = and_(book_receipts.c.job_id == job['id'], book_receipts.c.attempt == job['attempt'],
                    book_receipts.c.status == status)
    if (await conn.execute(select(book_receipts).where(identity))).first():
        return
    bucket = int(store.clock() // 60) * 60
    await store._serialize_key(conn, 'book-rollup', str(bucket))
    old = (await conn.execute(select(book_rollups).where(book_rollups.c.bucket == bucket))).mappings().first()
    values = {'completed': (old['completed'] if old else 0) + int(status == 'completed'),
              'failed': (old['failed'] if old else 0) + int(status == 'failed'),
              'lead_time_sum': (old['lead_time_sum'] if old else 0) + max(0,store.clock()-job['created'])}
    if old:
        await conn.execute(book_rollups.update().where(book_rollups.c.bucket == bucket).values(**values))
    else:
        await conn.execute(book_rollups.insert().values(bucket=bucket,**values))
    await conn.execute(book_receipts.insert().values(job_id=job['id'],attempt=job['attempt'],status=status))


async def estimate_eta(store):
    """Only known pending chunks, with explicitly approximate Poisson bounds."""
    end = store.clock()
    async with store.db.transaction() as conn:
        pending = (await conn.execute(select(func.count()).select_from(chunks).where(chunks.c.status == 'pending'))).scalar_one()
        completed = (await conn.execute(select(func.coalesce(func.sum(rollups.c.calls - rollups.c.errors),0)).where(
            rollups.c.bucket >= end - 3600))).scalar_one()
        unknown = (await conn.execute(select(func.count()).select_from(jobs).where(and_(
            jobs.c.status.in_(['queued','running','starting']), ~select(chunks.c.job_id).where(chunks.c.job_id == jobs.c.id).exists())))).scalar_one()
    result = {'pending_known_chunks':pending,'books_with_unknown_chunk_count':unknown,'observed_successes':completed,
              'window_seconds':3600,'eta_seconds':None,'lower_seconds':None,'upper_seconds':None,
              'scope':'known chunks only; excludes unchunked books', 'uncertainty':'insufficient_data'}
    if completed >= 10:
        rate, spread = completed/3600, 1.96 * math.sqrt(completed)/3600
        result.update(eta_seconds=pending/rate, lower_seconds=pending/(rate+spread),
            upper_seconds=pending/(rate-spread) if rate>spread else None,
            uncertainty='approximate 95% Poisson bounds; assumes stationary throughput, excludes unknown books')
    return result
