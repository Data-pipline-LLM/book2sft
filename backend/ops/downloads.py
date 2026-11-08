"""Recover a human-readable filename and MIME for an immutable stored artifact.

The object store has opaque hash keys; completed-stage checkpoints retain the
original relative filenames. Never rely on a client-supplied filename or MIME.
"""
import json
import mimetypes
import re
from pathlib import PurePosixPath
from urllib.parse import quote


_NAME = re.compile(r'[^\w.()\[\] -]', re.UNICODE)


def _safe_name(value):
    name = PurePosixPath(str(value or '').replace('\\', '/')).name
    name = _NAME.sub('_', name).strip(' .')[:150]
    return name if name not in {'', '.', '..'} else None


def file_info(row, stage_snapshots=(), job_config=None):
    """Return filename, media type and content disposition for a DB artifact row."""
    name = None
    config = job_config or {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (ValueError, TypeError):
            config = {}
    if (config.get('input_object') or {}).get('key') == row['key']:
        name = _safe_name(config.get('filename'))
    if not name:
        for snapshot in stage_snapshots:
            try:
                files = json.loads(snapshot).get('files', {}) if isinstance(snapshot, str) else snapshot.get('files', {})
            except (ValueError, TypeError, AttributeError):
                continue
            for relative, reference in files.items():
                if reference == row['id']:
                    name = _safe_name(relative)
                    break
            if name:
                break
    if not name:
        fallback_extensions = {'chunk_outputs': 'json', 'chunk_inputs': 'json', 'error_logs': 'log',
                               'task_details': 'json', 'monitoring_events': 'json'}
        extension = fallback_extensions.get(row['category'], 'bin')
        name = f"{row['category']}-{row['id'][:8]}.{extension}"
    extension = PurePosixPath(name).suffix.lower()
    media = {'.pdf': 'application/pdf', '.json': 'application/json',
             '.jsonl': 'application/x-ndjson', '.log': 'text/plain',
             '.txt': 'text/plain', '.csv': 'text/csv'}.get(extension)
    if not media:
        media = mimetypes.guess_type(name)[0] or 'application/octet-stream'
    # RFC 5987 supports Persian names without corrupting the header.
    disposition = "attachment; filename*=UTF-8''" + quote(name, safe='')
    return name, media, disposition
