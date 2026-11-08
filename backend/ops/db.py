from contextlib import asynccontextmanager
from sqlalchemy import create_engine, event, select
from sqlalchemy.ext.asyncio import create_async_engine
from .schema import metadata, versions


async def backfill_books(conn):
    from sqlalchemy import exists, func
    from .schema import books, jobs
    source = select(jobs.c.book_id, func.min(jobs.c.title), func.min(jobs.c.created)).where(
        ~exists(select(books.c.id).where(books.c.id == jobs.c.book_id))).group_by(jobs.c.book_id)
    await conn.execute(books.insert().from_select(['id','title','created'], source))


class _TestConnection:
    def __init__(self, conn):
        self.conn = conn

    async def execute(self, statement):
        return self.conn.execute(statement)


class Database:
    """PostgreSQL in production. SQLite is explicitly restricted to contract tests.

    All operations await execute; the SQLite adapter runs synchronously and must
    never be used as a production async web database or execution scheduler.
    """
    def __init__(self, url, *, test_sqlite=False):
        self.test_sqlite = test_sqlite
        if test_sqlite:
            if not url.startswith('sqlite:'):
                raise ValueError('test adapter requires sqlite')
            self.engine = create_engine(url)
            @event.listens_for(self.engine, 'connect')
            def configure(conn, _):
                conn.execute('PRAGMA foreign_keys=ON')
                conn.execute('PRAGMA busy_timeout=10000')
        else:
            if not url.startswith('postgresql+asyncpg://'):
                raise ValueError('production requires postgresql+asyncpg://')
            self.engine = create_async_engine(url, pool_size=10, max_overflow=5, pool_pre_ping=True)

    @asynccontextmanager
    async def transaction(self):
        if self.test_sqlite:
            with self.engine.connect() as conn:
                conn.exec_driver_sql('BEGIN IMMEDIATE')
                try:
                    yield _TestConnection(conn)
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        else:
            async with self.engine.begin() as conn:
                yield conn

    async def migrate(self):
        # Migration 1 is additive. Serialize concurrent PostgreSQL bootstraps.
        if self.test_sqlite:
            metadata.create_all(self.engine)
        else:
            from sqlalchemy import text
            async with self.engine.begin() as conn:
                await conn.execute(text('SELECT pg_advisory_xact_lock(2074251201)'))
                await conn.run_sync(metadata.create_all)
                rows = (await conn.execute(select(versions.c.version))).scalars().all()
                if any(v > 2 for v in rows):
                    raise RuntimeError('database schema is newer than this application')
                if 1 not in rows:
                    await conn.execute(versions.insert().values(version=1))
                if 2 not in rows:
                    await backfill_books(conn)
                    await conn.execute(versions.insert().values(version=2))
            return
        async with self.transaction() as conn:
            rows = (await conn.execute(select(versions.c.version))).scalars().all()
            if any(v > 2 for v in rows):
                raise RuntimeError('database schema is newer than this application')
            if 1 not in rows:
                await conn.execute(versions.insert().values(version=1))
            if 2 not in rows:
                await backfill_books(conn)
                await conn.execute(versions.insert().values(version=2))

    async def close(self):
        if self.test_sqlite:
            self.engine.dispose()
        else:
            await self.engine.dispose()

    @asynccontextmanager
    async def snapshot(self):
        if self.test_sqlite:
            async with self.transaction() as conn:
                yield conn
        else:
            async with self.engine.connect() as conn:
                conn = await conn.execution_options(isolation_level='REPEATABLE READ')
                async with conn.begin():
                    yield conn
