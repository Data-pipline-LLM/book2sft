import base64
import json
from sqlalchemy import select, and_, or_, func
from .schema import jobs, chunks, attempts, events, stages


def pack(values):
    return base64.urlsafe_b64encode(json.dumps(values).encode()).decode().rstrip('=')


def unpack(cursor):
    if not cursor or len(cursor) > 512:
        raise ValueError('invalid cursor')
    try:
        value = json.loads(base64.urlsafe_b64decode(cursor + '=' * (-len(cursor) % 4)))
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError()
        return value
    except Exception as exc:
        raise ValueError('invalid cursor') from exc


async def jobs_page(store, *, cursor=None, limit=100, q='', status=None, since=None, until=None, book_id=None):
    if not 1 <= limit <= 200 or len(q) > 120:
        raise ValueError('invalid page size/search')
    query = select(jobs)
    if status:
        query = query.where(jobs.c.status == status)
    if book_id:
        query = query.where(jobs.c.book_id == book_id)
    if q:
        escaped = q.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        query = query.where(or_(jobs.c.id == q, jobs.c.book_id == q,
            jobs.c.title.ilike('%' + escaped + '%', escape='\\')))
    if since is not None:
        query = query.where(jobs.c.created >= since)
    if until is not None:
        query = query.where(jobs.c.created < until)
    if cursor:
        at, jid = unpack(cursor)
        query = query.where(or_(jobs.c.created > float(at), and_(jobs.c.created == float(at), jobs.c.id > str(jid))))
    async with store.db.transaction() as conn:
        rows = [dict(row) for row in (await conn.execute(query.order_by(jobs.c.created, jobs.c.id).limit(limit + 1))).mappings()]
    return {'items': rows[:limit], 'next_cursor': pack([rows[limit-1]['created'], rows[limit-1]['id']]) if len(rows) > limit else None}


async def event_page(store, jid, cursor=None, limit=100):
    query = select(events).where(events.c.job_id == jid)
    if cursor:
        at, eid = unpack(cursor)
        query = query.where(or_(events.c.at > float(at), and_(events.c.at == float(at), events.c.id > str(eid))))
    async with store.db.transaction() as conn:
        rows = [dict(row) for row in (await conn.execute(query.order_by(events.c.at, events.c.id).limit(limit))).mappings()]
    return rows


async def job_detail(store, jid, role):
    job = await store.get(jid)
    async with store.db.transaction() as conn:
        states = [dict(row) for row in (await conn.execute(select(stages).where(stages.c.job_id == jid).order_by(stages.c.started, stages.c.updated))).mappings()]
        counts = dict((await conn.execute(select(chunks.c.status, func.count()).where(
            chunks.c.job_id == jid).group_by(chunks.c.status))).all())
    if role == 'viewer':
        job.pop('config', None)
    for state in states:
        snapshot = json.loads(state.pop('snapshot'))
        state['progress'] = snapshot.get('job', {}).get('progress')
        state['message'] = snapshot.get('job', {}).get('message')
        if role != 'viewer':
            state['checkpoint'] = snapshot
    job['stages'], job['chunks'] = states, counts
    return job
