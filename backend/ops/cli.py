"""Explicit, non-destructive operations setup. Never implicitly mutates legacy jobs."""
import argparse
import asyncio
import getpass
import os
from .db import Database
from .store import Store
from .auth import Auth


async def run(args):
    db = Database(os.environ['OPS_DATABASE_URL'])
    try:
        if args.action == 'migrate':
            await db.migrate()
            print('Operational schema migrations applied; legacy data untouched.')
        elif args.action == 'health':
            from sqlalchemy import select
            from .schema import workers
            import socket, time
            async with db.transaction() as conn:
                row = (await conn.execute(select(workers).where(workers.c.id == socket.gethostname()))).mappings().first()
            if not row or row['kind'] != args.kind or time.time() - row['heartbeat'] > 30:
                raise RuntimeError('worker heartbeat is absent or stale')
            print('healthy')
        elif args.action == 'init-storage':
            from .artifacts import S3Artifacts
            from botocore.exceptions import ClientError
            import time
            objects = S3Artifacts(os.environ['OPS_S3_BUCKET'], endpoint_url=os.getenv('OPS_S3_ENDPOINT'))
            for attempt in range(30):
                try:
                    try:
                        objects.client.head_bucket(Bucket=objects.bucket)
                    except ClientError as exc:
                        code = str(exc.response.get('Error', {}).get('Code'))
                        if code not in {'404','NoSuchBucket','NotFound'}:
                            raise
                        objects.client.create_bucket(Bucket=objects.bucket)
                    break
                except Exception:
                    if attempt == 29:
                        raise
                    await asyncio.sleep(2)
            print('Dedicated object bucket verified; no existing objects deleted.')
        elif args.action == 'import-legacy':
            from .migration import import_legacy
            from .artifacts import S3Artifacts
            objects = S3Artifacts(os.environ['OPS_S3_BUCKET'], endpoint_url=os.getenv('OPS_S3_ENDPOINT'))
            print(await import_legacy(Store(db), objects, args.root, apply=args.apply))
        elif args.action == 'create-user':
            password = getpass.getpass('New password (12+ characters): ')
            if password != getpass.getpass('Repeat password: '):
                raise ValueError('passwords differ')
            await Auth(Store(db)).create_user(args.user, password, args.role, 'local-bootstrap', 'admin')
            print('User created; password was not persisted in plaintext.')
        elif args.action in {'backup', 'restore-empty'}:
            from .backup import backup, restore
            result = await (backup(db, args.path) if args.action == 'backup' else restore(db, args.path))
            print(result)
    finally:
        await db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('migrate')
    sub.add_parser('init-storage')
    health = sub.add_parser('health')
    health.add_argument('--kind', choices=['book-worker','cleanup'], required=True)
    migrate = sub.add_parser('import-legacy')
    migrate.add_argument('root')
    migrate.add_argument('--apply', action='store_true', help='Default is a read-only dry run')
    user = sub.add_parser('create-user')
    user.add_argument('user')
    user.add_argument('--role', choices=['viewer', 'operator', 'admin'], default='admin')
    for action in ('backup', 'restore-empty'):
        command = sub.add_parser(action)
        command.add_argument('path')
    asyncio.run(run(parser.parse_args()))


if __name__ == '__main__':
    main()
