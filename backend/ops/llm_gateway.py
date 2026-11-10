"""Fair, process-wide admission control and reverse proxy for the shared vLLM.

All Book2SFT SFT and Chunker model calls are routed through this service. A
fixed pool of admission workers keeps up to MAX_CONCURRENCY requests active at
the model endpoint. When demand exceeds capacity, requests wait in bounded,
round-robin per-client queues instead of creating unbounded upstream pressure.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


MAX_CONCURRENCY = _env_int("LLM_GATEWAY_MAX_CONCURRENCY", 400, 1, 2000)
MAX_QUEUE = _env_int("LLM_GATEWAY_MAX_QUEUE", 4000, 0, 100000)
try:
    UPSTREAM_TIMEOUT_SEC = max(30.0, float(os.getenv("LLM_GATEWAY_TIMEOUT_SEC", "7200")))
except (TypeError, ValueError):
    UPSTREAM_TIMEOUT_SEC = 7200.0

_raw_upstream = (
    os.getenv("LLM_GATEWAY_UPSTREAM_BASE_URL")
    or os.getenv("VLLM_BASE_URL")
    or "http://model:8000/v1"
).rstrip("/")
UPSTREAM_V1_BASE = _raw_upstream if _raw_upstream.endswith("/v1") else _raw_upstream + "/v1"
UPSTREAM_API_KEY = os.getenv("LLM_GATEWAY_UPSTREAM_API_KEY") or os.getenv("VLLM_API_KEY", "")

_HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
}


class QueueFull(Exception):
    pass


@dataclass
class Admission:
    client: str
    ready: asyncio.Future
    queued_at: float = field(default_factory=time.monotonic)
    started_at: Optional[float] = None
    released: bool = False


class FairAdmissionScheduler:
    """A fixed number of upstream slots with per-client round-robin fairness."""

    def __init__(self, capacity: int, max_queue: int):
        self.capacity = capacity
        self.max_queue = max_queue
        self._condition = asyncio.Condition()
        self._queues: Dict[str, Deque[Admission]] = {}
        self._rotation: Deque[str] = deque()
        self._waiting = 0
        self._active = 0
        self._active_by_client: Counter[str] = Counter()
        self._completed_by_client: Counter[str] = Counter()
        self._failed_by_client: Counter[str] = Counter()
        self._wait_ms_by_client: Dict[str, float] = {}
        self._service_ms_by_client: Dict[str, float] = {}
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._closed = False
        self._total_completed = 0
        self._total_failed = 0
        self._total_rejected = 0

    async def start(self) -> None:
        self._dispatcher_task = asyncio.create_task(
            self._dispatch(), name="llm-admission-dispatcher"
        )

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            for queue in self._queues.values():
                for admission in queue:
                    if not admission.ready.done():
                        admission.ready.set_exception(QueueFull("LLM gateway is shutting down"))
            self._queues.clear()
            self._rotation.clear()
            self._waiting = 0
            self._condition.notify_all()
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            await asyncio.gather(self._dispatcher_task, return_exceptions=True)
            self._dispatcher_task = None

    async def acquire(self, client: str) -> Admission:
        loop = asyncio.get_running_loop()
        admission = Admission(client=client, ready=loop.create_future())
        async with self._condition:
            if self._closed:
                raise QueueFull("LLM gateway is shutting down")
            # max_queue is additional waiting capacity beyond the active slots.
            # Include available-but-not-yet-dispatched slots in the admission
            # budget so a burst of 400 requests is not rejected before the
            # dispatcher gets its first scheduling turn.
            if self._active + self._waiting >= self.capacity + self.max_queue:
                self._total_rejected += 1
                raise QueueFull("LLM gateway queue is full")
            queue = self._queues.get(client)
            if queue is None:
                queue = deque()
                self._queues[client] = queue
                self._rotation.append(client)
            queue.append(admission)
            self._waiting += 1
            self._condition.notify()
        try:
            await admission.ready
            return admission
        except asyncio.CancelledError:
            # If dispatch won the race with client cancellation, return the slot.
            # Otherwise mark the queued waiter so the dispatcher can skip it.
            if admission.started_at is not None:
                await asyncio.shield(self.release(admission, success=False))
            else:
                admission.ready.cancel()
                async with self._condition:
                    self._condition.notify()
            raise

    async def release(self, admission: Admission, *, success: bool) -> None:
        async with self._condition:
            if admission.released:
                return
            admission.released = True
            self._active = max(0, self._active - 1)
            self._active_by_client[admission.client] -= 1
            if self._active_by_client[admission.client] <= 0:
                self._active_by_client.pop(admission.client, None)
            elapsed_ms = max(0.0, (time.monotonic() - (admission.started_at or time.monotonic())) * 1000.0)
            self._service_ms_by_client[admission.client] = elapsed_ms
            if success:
                self._total_completed += 1
                self._completed_by_client[admission.client] += 1
            else:
                self._total_failed += 1
                self._failed_by_client[admission.client] += 1
            self._condition.notify()

    def _pop_next(self) -> Optional[Admission]:
        while self._rotation:
            client = self._rotation.popleft()
            queue = self._queues.get(client)
            if not queue:
                self._queues.pop(client, None)
                continue
            admission = queue.popleft()
            if queue:
                self._rotation.append(client)
            else:
                self._queues.pop(client, None)
            self._waiting = max(0, self._waiting - 1)
            if admission.ready.cancelled():
                continue
            return admission
        return None

    async def _dispatch(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._closed or (self._active < self.capacity and self._waiting > 0)
                )
                if self._closed:
                    return
                while self._active < self.capacity and self._waiting > 0:
                    admission = self._pop_next()
                    if admission is None:
                        continue
                    admission.started_at = time.monotonic()
                    self._active += 1
                    self._active_by_client[admission.client] += 1
                    self._wait_ms_by_client[admission.client] = max(
                        0.0, (admission.started_at - admission.queued_at) * 1000.0
                    )
                    if not admission.ready.done():
                        admission.ready.set_result(None)

    def snapshot(self) -> dict:
        queued = {client: len(queue) for client, queue in self._queues.items() if queue}
        clients = set(queued) | set(self._active_by_client) | set(self._completed_by_client) | set(self._failed_by_client)
        return {
            "capacity": self.capacity,
            "active": self._active,
            "queued": self._waiting,
            "queue_limit": self.max_queue,
            "slots_available": max(0, self.capacity - self._active),
            "saturated": self._active >= self.capacity,
            "backlog_waiting": self._waiting > 0,
            "target_reached": self._active >= self.capacity,
            "total_completed": self._total_completed,
            "total_failed": self._total_failed,
            "total_rejected": self._total_rejected,
            "clients": {
                client: {
                    "active": self._active_by_client.get(client, 0),
                    "queued": queued.get(client, 0),
                    "completed": self._completed_by_client.get(client, 0),
                    "failed": self._failed_by_client.get(client, 0),
                    "last_queue_wait_ms": round(self._wait_ms_by_client.get(client, 0.0), 2),
                    "last_service_ms": round(self._service_ms_by_client.get(client, 0.0), 2),
                }
                for client in sorted(clients)
            },
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=MAX_CONCURRENCY + 16,
        max_keepalive_connections=MAX_CONCURRENCY,
        keepalive_expiry=60.0,
    )
    timeout = httpx.Timeout(
        UPSTREAM_TIMEOUT_SEC,
        connect=30.0,
        pool=30.0,
    )
    app.state.upstream = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=False)
    app.state.scheduler = FairAdmissionScheduler(MAX_CONCURRENCY, MAX_QUEUE)
    await app.state.scheduler.start()
    try:
        yield
    finally:
        await app.state.scheduler.close()
        await app.state.upstream.aclose()


app = FastAPI(title="Shared LLM Admission Gateway", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    scheduler: FairAdmissionScheduler = app.state.scheduler
    return {"status": "alive", "service": "llm-gateway", **scheduler.snapshot()}


@app.get("/ready")
async def ready() -> dict:
    upstream: httpx.AsyncClient = app.state.upstream
    headers = {"Authorization": f"Bearer {UPSTREAM_API_KEY}"} if UPSTREAM_API_KEY else {}
    try:
        response = await upstream.get(f"{UPSTREAM_V1_BASE}/models", headers=headers, timeout=5.0)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"LLM upstream unavailable: {type(exc).__name__}") from exc
    if response.status_code >= 400:
        raise HTTPException(status_code=503, detail=f"LLM upstream readiness returned HTTP {response.status_code}")
    return {"status": "ready", "upstream_status": response.status_code, "upstream_v1": UPSTREAM_V1_BASE}


@app.get("/internal/metrics")
async def metrics() -> dict:
    scheduler: FairAdmissionScheduler = app.state.scheduler
    return scheduler.snapshot()


@app.api_route(
    "/v1/{path:path}",
    methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
)
async def proxy_v1(path: str, request: Request):
    scheduler: FairAdmissionScheduler = request.app.state.scheduler
    upstream: httpx.AsyncClient = request.app.state.upstream
    client_name = (request.headers.get("x-llm-client") or "unclassified").strip().lower()
    client_name = "".join(ch for ch in client_name if ch.isalnum() or ch in "-_:")[:48] or "unclassified"

    # Discovery and metadata calls do not consume a generation slot.
    uses_slot = request.method.upper() not in {"GET", "HEAD", "OPTIONS"}
    admission: Optional[Admission] = None
    handed_to_stream = False
    success = False

    if uses_slot:
        try:
            admission = await scheduler.acquire(client_name)
        except QueueFull as exc:
            return JSONResponse(
                status_code=503,
                content={"error": {"type": "llm_gateway_queue_full", "message": str(exc)}},
                headers={"Retry-After": "1"},
            )

    try:
        body = await request.body()
        headers: Dict[str, str] = {}
        for key, value in request.headers.items():
            lowered = key.lower()
            if lowered in _HOP_BY_HOP_HEADERS or lowered in {"content-length", "x-llm-client", "authorization"}:
                continue
            headers[key] = value
        if UPSTREAM_API_KEY:
            headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"
        elif request.headers.get("authorization"):
            headers["Authorization"] = request.headers["authorization"]

        target_url = f"{UPSTREAM_V1_BASE}/{path.lstrip('/')}"
        if request.url.query:
            target_url += "?" + request.url.query
        upstream_request = upstream.build_request(
            request.method,
            target_url,
            headers=headers,
            content=body,
        )
        upstream_response = await upstream.send(upstream_request, stream=True)
        response_headers = {
            key: value
            for key, value in upstream_response.headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS
            and key.lower() not in {"content-length", "content-encoding"}
        }
        async def response_stream():
            nonlocal success
            try:
                async for chunk in upstream_response.aiter_bytes():
                    yield chunk
                success = 200 <= upstream_response.status_code < 400
            except BaseException:
                success = False
                raise
            finally:
                try:
                    await upstream_response.aclose()
                finally:
                    if admission is not None:
                        await scheduler.release(admission, success=success)

        downstream_response = StreamingResponse(
            response_stream(),
            status_code=upstream_response.status_code,
            headers=response_headers,
            media_type=None,
        )
        handed_to_stream = True
        return downstream_response
    except httpx.TimeoutException as exc:
        if admission is not None:
            await scheduler.release(admission, success=False)
        raise HTTPException(status_code=504, detail=f"Upstream LLM timeout: {type(exc).__name__}") from exc
    except httpx.HTTPError as exc:
        if admission is not None:
            await scheduler.release(admission, success=False)
        raise HTTPException(status_code=502, detail=f"Upstream LLM transport error: {type(exc).__name__}") from exc
    except Exception:
        if admission is not None:
            await scheduler.release(admission, success=False)
        raise
    finally:
        if admission is not None and not handed_to_stream:
            await scheduler.release(admission, success=False)
