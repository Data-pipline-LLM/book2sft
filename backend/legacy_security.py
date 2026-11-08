"""Guard legacy API during drain/cutover; no public unauthenticated data routes."""
import hmac
import os
from starlette.responses import JSONResponse


class LegacySecurity:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http' and scope.get('path', '').startswith('/v1/'):
            expected = os.getenv('OPS_SERVICE_TOKEN', '')
            headers = {k.decode().lower(): v.decode() for k, v in scope.get('headers', [])}
            supplied = headers.get('authorization', '').removeprefix('Bearer ')
            if not expected or not hmac.compare_digest(supplied, expected):
                response = JSONResponse({'detail': 'Legacy API is restricted during migration; use the authenticated operations API.'}, status_code=401)
                return await response(scope, receive, send)
            if scope.get('method') == 'POST' and os.getenv('LEGACY_DRAIN_ONLY', 'true').lower() == 'true':
                return await JSONResponse({'detail': 'Legacy submissions disabled; use the durable operations API.'}, status_code=409)(scope, receive, send)
        await self.app(scope, receive, send)
