"""Compatibility upload ingestion; no in-memory execution queue."""
import asyncio
import hashlib
import json
from .store import encoded, Conflict
from .retention import Retention


def inspect_file(stream, input_type, max_bytes):
    stream.seek(0)
    digest, size = hashlib.sha256(), 0
    while True:
        block = stream.read(1024 * 1024)
        if not block:
            break
        size += len(block)
        if size > max_bytes:
            raise ValueError('upload exceeds configured size limit')
        digest.update(block)
    stream.seek(0)
    if input_type == 'pdf':
        if stream.read(5) != b'%PDF-':
            raise ValueError('invalid PDF signature')
    elif input_type == 'ocr_json':
        obj = json.load(stream)
        pages = obj.get('pages') if isinstance(obj, dict) else None
        if not isinstance(pages, list) or not pages:
            raise ValueError('OCR JSON needs a nonempty pages array')
        if not any(isinstance(cell, dict) and str(cell.get('plain_text') or cell.get('text') or '').strip()
            for page in pages if isinstance(page, dict)
            for cell in (page.get('cells') or page.get('blocks') or [])):
            raise ValueError('OCR pages need cells/blocks with text')
    else:
        raise ValueError('unsupported input type')
    stream.seek(0)
    return size, digest.hexdigest()


async def submit(store, objects, upload, *, title, category, domain, input_type, actor, role, key, max_bytes):
    size, digest = await asyncio.to_thread(inspect_file, upload.file, input_type, max_bytes)
    manifest = await asyncio.to_thread(objects.put_file, upload.file)
    config = {'filename': upload.filename, 'size_bytes': size, 'input_type': input_type,
              'category': category, 'domain': domain, 'input_object': manifest}
    jid = await store.create(title or upload.filename, config, actor, role, key)
    record = await store.get(jid)
    # A repeated idempotent submission returns the original input reference.
    if json.loads(record['config'])['input_object']['key'] == manifest['key']:
        await Retention(store, objects).register(jid, 'chunk_inputs', manifest)
    return jid
