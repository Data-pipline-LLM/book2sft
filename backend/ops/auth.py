import hashlib
import hmac
import secrets
from sqlalchemy import select, and_
from .schema import users, sessions, auth_limits
from .store import authorize, Conflict, Forbidden, ROLE


def password_hash(password, salt=None):
    if len(password) < 12 or len(password) > 1024:
        raise ValueError('password must contain 12..1024 characters')
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(salt), 600000).hex()
    return f'pbkdf2_sha256$600000${salt}${digest}'


def verify(password, stored):
    if len(password) > 1024:
        return False
    try:
        scheme, rounds, salt, digest = stored.split('$')
        if scheme != 'pbkdf2_sha256' or int(rounds) != 600000:
            return False
        actual = hashlib.pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(salt), int(rounds)).hex()
        return hmac.compare_digest(actual, digest)
    except (ValueError, TypeError):
        return False


class Auth:
    def __init__(self, store):
        import asyncio
        self.store = store
        self.password_slots = asyncio.Semaphore(4)
        # Precomputed dummy hash prevents a user-existence shortcut.
        self.dummy = password_hash(secrets.token_urlsafe(24))

    async def create_user(self, user_id, password, role, actor, actor_role):
        authorize(actor_role, 'admin')
        if not user_id or len(user_id) > 128 or role not in ROLE:
            raise ValueError('invalid user/role')
        digest = password_hash(password)
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'user', user_id)
            old = (await conn.execute(select(users.c.id).where(users.c.id == user_id))).first()
            if old:
                raise Conflict('user already exists')
            await conn.execute(users.insert().values(id=user_id, password_hash=digest, role=role, enabled=1))
            await self.store._audit(conn, actor, 'user_create', user_id, {'role': role})

    async def login(self, user_id, password, source='local'):
        import asyncio
        identity = hashlib.sha256(source.encode()).hexdigest()
        async with self.store.db.transaction() as conn:
            await self.store._serialize_key(conn, 'auth-rate', identity)
            limit = (await conn.execute(select(auth_limits).where(auth_limits.c.identity == identity))).mappings().first()
            if limit and limit['reset_at'] > self.store.clock() and limit['count'] >= 10:
                raise Forbidden('login rate limit exceeded')
            if limit:
                count = limit['count'] + 1 if limit['reset_at'] > self.store.clock() else 1
                reset = limit['reset_at'] if limit['reset_at'] > self.store.clock() else self.store.clock() + 60
                await conn.execute(auth_limits.update().where(auth_limits.c.identity == identity).values(count=count, reset_at=reset))
            else:
                await conn.execute(auth_limits.insert().values(identity=identity, count=1, reset_at=self.store.clock() + 60))
        async with self.store.db.transaction() as conn:
            row = (await conn.execute(select(users).where(users.c.id == user_id))).mappings().first()
        if self.password_slots.locked():
            raise Forbidden('authentication capacity reached; retry later')
        async with self.password_slots:
            valid = await asyncio.to_thread(verify, password, row['password_hash'] if row else self.dummy)
        if not valid or not row or not row['enabled']:
            async with self.store.db.transaction() as conn:
                await self.store._audit(conn, 'anonymous', 'login_failed', identity, {})
            raise Forbidden('invalid credentials')
        token = secrets.token_urlsafe(32)
        async with self.store.db.transaction() as conn:
            await conn.execute(sessions.insert().values(token_hash=hashlib.sha256(token.encode()).hexdigest(),
                user_id=user_id, expires=self.store.clock() + 3600))
            await self.store._audit(conn, user_id, 'login', user_id, {})
        return token

    async def principal(self, token):
        if len(token) > 128:
            raise Forbidden('invalid token')
        digest = hashlib.sha256(token.encode()).hexdigest()
        async with self.store.db.transaction() as conn:
            row = (await conn.execute(select(users.c.id, users.c.role).select_from(
                users.join(sessions, users.c.id == sessions.c.user_id)).where(and_(
                sessions.c.token_hash == digest, sessions.c.expires > self.store.clock(), users.c.enabled == 1)))).mappings().first()
        if not row:
            raise Forbidden('invalid or expired token')
        return dict(row)

    async def logout(self, token):
        digest = hashlib.sha256(token.encode()).hexdigest()
        async with self.store.db.transaction() as conn:
            await conn.execute(sessions.delete().where(sessions.c.token_hash == digest))
