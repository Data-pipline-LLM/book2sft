"""Production operations API factory; legacy generation implementation is reused by workers."""
import os
import asyncio
import httpx
from sqlalchemy import select, func
from .api import create_app
from .db import Database
from .artifacts import S3Artifacts
from .schema import workers
from .telemetry import configure


def configured_app():
    configure('book2sft-api')
    db = Database(os.environ['OPS_DATABASE_URL'])
    objects = S3Artifacts(os.environ['OPS_S3_BUCKET'],
        endpoint_url=os.environ.get('OPS_S3_ENDPOINT') or None,
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
    async def workflow_ready():
        # Durable submission is allowed while workers/Temporal recover; the
        # outbox retains intents. It is not an execution-success acknowledgement.
        return os.getenv('OPS_ACCEPT_SUBMISSIONS', 'true').lower() == 'true'
    app = create_app(db, objects, workflow_ready=workflow_ready)
    async def readiness():
        await asyncio.wait_for(asyncio.to_thread(objects.client.head_bucket, Bucket=objects.bucket), timeout=5)
        from temporalio.client import Client
        client = await asyncio.wait_for(Client.connect(os.getenv('TEMPORAL_ADDRESS', 'temporal:7233'),
            namespace=os.getenv('TEMPORAL_NAMESPACE', 'default')), timeout=5)
        if not await asyncio.wait_for(client.service_client.check_health(), timeout=5):
            raise RuntimeError('workflow health check failed')
    app.state.readiness = readiness
    return app
