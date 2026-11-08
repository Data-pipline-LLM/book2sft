"""Explicit legacy import. Running jobs must drain; imports never resume them."""
import json
import uuid
from pathlib import Path
from .runtime import Runtime
from .retention import Retention


async def import_legacy(store, objects, root, *, apply=False):
    root = Path(root).resolve()
    counts = {'eligible': 0, 'running_skipped': 0, 'imported': 0, 'files': 0, 'bytes': 0, 'dry_run': not apply}
    for directory in root.iterdir():
        if directory.is_symlink() or not directory.is_dir():
            continue
        try:
            jid = str(uuid.UUID(directory.name))
        except ValueError:
            continue
        state = directory / 'job.json'
        if not state.is_file() or state.is_symlink():
            continue
        data = json.loads(state.read_text())
        if data.get('job_id') != jid:
            continue
        if data.get('status') not in {'completed', 'failed'}:
            counts['running_skipped'] += 1
            continue
        counts['eligible'] += 1
        files = {}
        for path in directory.rglob('*'):
            if path.is_symlink() or not path.is_file() or path.name == 'job.json':
                continue
            relative = path.relative_to(directory)
            if relative.parts[0] not in {'input', 'artifacts', 'sft', 'review'}:
                continue
            if directory.resolve() not in path.resolve().parents:
                raise ValueError('legacy artifact escapes job directory')
            counts['files'] += 1
            counts['bytes'] += path.stat().st_size
            if apply:
                with path.open('rb') as stream:
                    import asyncio
                    manifest = await asyncio.to_thread(objects.put_file, stream)
                files[str(relative)] = manifest
        if not apply:
            continue
        config = {key:data.get(key) for key in ('filename','input_type','size_bytes','category','domain')}
        input_name = 'input/ocr.json' if data.get('input_type') == 'ocr_json' else 'input/book.pdf'
        if input_name in files:
            config['input_object'] = files[input_name]
        config['legacy_import'] = True
        imported = await store.create(data.get('book_title') or data.get('filename') or jid,
            config, 'legacy-import', 'admin', 'legacy-' + jid, import_id=jid, historical_status=data['status'])
        references = {}
        retention = Retention(store, objects)
        for name, manifest in files.items():
            category = 'completed_datasets' if name.startswith(('sft/', 'review/')) else 'chunk_inputs'
            references[name] = await retention.register(imported, category, manifest)
        await Runtime(store).checkpoint(imported, 'legacy_import', 'completed', {'job':data,'files':references})
        counts['imported'] += 1
    return counts
