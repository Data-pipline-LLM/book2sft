"""Optional bounded OTLP tracing. Missing/exporter-down telemetry never gates work.

This small module is vendored in SFT/Chunker services; no LangChain dependency.
"""
import base64
import contextvars
import functools
import json
import logging
import os
import re
from contextlib import contextmanager

log = logging.getLogger('book2sft.telemetry')
_tracer = None
_provider = None
status = {'enabled': False, 'error': None}
correlation = contextvars.ContextVar('book_correlation', default={})
SECRET = re.compile(r'(authorization|api.?key|password|secret|access.?token|cookie)', re.I)


def redact(value, depth=0):
    if depth > 8:
        return '[depth limit]'
    if isinstance(value, dict):
        return {str(k): '[redacted]' if SECRET.search(str(k)) else redact(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v, depth + 1) for v in value[:100]]
    return value if isinstance(value, (str, int, float, bool, type(None))) else type(value).__name__


def preview(value):
    # Content is opt-in; the default carries only IDs/references, never prompts.
    if os.getenv('TRACE_CAPTURE_CONTENT', 'false').lower() != 'true':
        return None
    try:
        limit = max(0, min(int(os.getenv('TRACE_CONTENT_LIMIT', '16384')), 1048576))
        return json.dumps(redact(value), ensure_ascii=False)[:limit]
    except Exception:
        return None


def configure(service):
    global _provider, _tracer
    if _provider is not None:
        return status
    enabled = os.getenv('LANGFUSE_ENABLED', 'false').lower() == 'true'
    endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', '').rstrip('/')
    if not endpoint and not enabled:
        return status
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        provider = TracerProvider(resource=Resource.create({'service.name': service}))
        destinations = []
        if endpoint:
            destinations.append((endpoint + '/v1/traces', {}))
        if enabled:
            host = (os.getenv('LANGFUSE_HOST') or os.getenv('LANGFUSE_BASE_URL', '')).rstrip('/')
            public, secret = os.getenv('LANGFUSE_PUBLIC_KEY', ''), os.getenv('LANGFUSE_SECRET_KEY', '')
            if not host or not public or not secret:
                raise ValueError('Langfuse endpoint and credentials required')
            auth = base64.b64encode((public + ':' + secret).encode()).decode()
            destinations.append((host + '/api/public/otel/v1/traces',
                                 {'Authorization': 'Basic ' + auth, 'x-langfuse-ingestion-version': '4'}))
        for url, headers in destinations:
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=url, headers=headers, timeout=2),
                max_queue_size=1024, max_export_batch_size=64, schedule_delay_millis=1000, export_timeout_millis=2500))
        _provider, _tracer = provider, provider.get_tracer('book2sft.pipeline')
        status.update(enabled=True, error=None)
    except Exception as exc:
        status.update(enabled=False, error=type(exc).__name__)
        log.warning('Tracing unavailable: %s', type(exc).__name__)
    return status


def propagation_headers():
    headers = {}
    try:
        from opentelemetry.propagate import inject
        inject(headers)
    except Exception:
        pass
    for key, value in correlation.get().items():
        if key in {'book_id', 'job_id', 'chunk_id', 'task_id', 'attempt_id'}:
            headers['X-' + key.replace('_', '-')] = str(value)[:128]
    return headers


@contextmanager
def span(name, *, attributes=None, input=None, carrier=None):
    current, token, span_object = None, None, None
    try:
        if _tracer is not None:
            from opentelemetry.propagate import extract
            context = extract(carrier) if carrier else None
            current = _tracer.start_as_current_span(name, context=context, record_exception=False, set_status_on_exception=False)
            span_object = current.__enter__()
            for key, value in {**correlation.get(), **(attributes or {})}.items():
                if value is not None and not SECRET.search(key):
                    span_object.set_attribute(key, value)
            content = preview(input)
            if content is not None:
                span_object.set_attribute('langfuse.observation.input', content)
    except Exception:
        span_object = None
    try:
        yield span_object
    except BaseException as exc:
        if span_object:
            try:
                from opentelemetry.trace import Status, StatusCode
                span_object.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            except Exception:
                pass
        raise
    finally:
        if current:
            try:
                current.__exit__(None, None, None)
            except Exception:
                pass


def traced_model(model):
    if model is None or getattr(model, '_ops_traced', False):
        return model
    original = model.generate
    @functools.wraps(original)
    def generate(*args, **kwargs):
        with span('llm.generate', attributes={'langfuse.observation.type': 'generation',
                  'gen_ai.request.model': str(getattr(model, 'model_name', 'unknown'))}, input={'args': args, 'kwargs': kwargs}) as observation:
            output = original(*args, **kwargs)
            content = preview(output) if observation is not None else None
            if observation is not None and content is not None:
                try:
                    observation.set_attribute('langfuse.observation.output', content)
                except Exception:
                    pass
            return output
    model.generate = generate
    model._ops_traced = True
    return model


class TelemetryMiddleware:
    def __init__(self, app, paths=('/generate', '/assess')):
        self.app, self.paths = app, set(paths)

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope.get('path') not in self.paths or not status['enabled']:
            return await self.app(scope, receive, send)
        headers = {key.decode().lower(): value.decode() for key, value in scope.get('headers', [])}
        ids = {key: headers.get('x-' + key.replace('_', '-'), '')
               for key in ('book_id', 'job_id', 'chunk_id', 'task_id', 'attempt_id')}
        token = correlation.set(ids)
        try:
            with span('http' + scope['path'], attributes=ids, carrier=headers):
                await self.app(scope, receive, send)
        finally:
            correlation.reset(token)
