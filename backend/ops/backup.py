"""Streaming metadata backup/restore; restore refuses a nonempty operational DB.

Artifact bytes require a separate object-store backup. Treat this file as secret:
password hashes and operational input references are included, sessions excluded.
"""
import hashlib
import json
import os
from sqlalchemy import select, func, tuple_
from .schema import metadata, versions, sessions, auth_limits
from .store import Conflict, encoded

TABLES = [t for t in metadata.sorted_tables if t not in (versions, sessions, auth_limits)]


async def backup(db, filename):
    digest = hashlib.sha256()
    count = 0
    fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        def emit(value):
            line = (encoded(value) + '\n').encode()
            stream.write(line)
            digest.update(line)
        emit({'format': 'book2sft-ops', 'schema': 1, 'artifacts': 'metadata only'})
        async with db.snapshot() as conn:
            for table in TABLES:
                keys = list(table.primary_key.columns)
                last = None
                while True:
                    query = select(table).order_by(*keys).limit(250)
                    if last is not None:
                        query = query.where(tuple_(*keys) > tuple_(*last))
                    rows = (await conn.execute(query)).mappings().all()
                    if not rows:
                        break
                    for row in rows:
                        emit({'table': table.name, 'row': dict(row)})
                        count += 1
                    last = [rows[-1][k.name] for k in keys]
        stream.write((encoded({'sha256': digest.hexdigest(), 'rows': count}) + '\n').encode())
        stream.flush()
        os.fsync(stream.fileno())
    return {'rows': count, 'sha256': digest.hexdigest(), 'artifacts': 'not included'}


async def restore(db, filename):
    allowed = {t.name: t for t in TABLES}
    digest, count = hashlib.sha256(), 0
    with open(filename, 'rb') as stream:
        header = stream.readline(1048576)
        if json.loads(header) != {'format': 'book2sft-ops', 'schema': 1, 'artifacts': 'metadata only'}:
            raise ValueError('unsupported backup')
        digest.update(header)
        async with db.transaction() as conn:
            for table in metadata.sorted_tables:
                if table is versions:
                    continue
                if (await conn.execute(select(func.count()).select_from(table))).scalar_one():
                    raise Conflict('restore requires an empty operational database')
            while True:
                line = stream.readline(8 * 1024 * 1024 + 1)
                if not line or len(line) > 8 * 1024 * 1024:
                    raise ValueError('truncated backup or oversized row')
                record = json.loads(line)
                if 'sha256' in record:
                    if record['sha256'] != digest.hexdigest() or record['rows'] != count or stream.read(1):
                        raise ValueError('backup checksum/count mismatch')
                    break
                digest.update(line)
                table = allowed.get(record.get('table'))
                if table is None or set(record.get('row', {})) != set(table.columns.keys()):
                    raise ValueError('invalid backup table/columns')
                await conn.execute(table.insert().values(**record['row']))
                count += 1
    return {'rows': count, 'sessions': 'not restored; sign in again'}
