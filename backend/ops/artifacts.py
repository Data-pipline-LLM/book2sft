"""Immutable artifacts: local contract-test store and S3-compatible production store."""
import hashlib
import os
import re
import uuid
from pathlib import Path

KEY = re.compile(r'^[a-f0-9]{32}/[a-f0-9]{64}$')


def validate_key(key):
    if not KEY.fullmatch(key):
        raise ValueError('invalid immutable artifact key')


class LocalArtifacts:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise ValueError('artifact root cannot be a symlink')

    def put_file(self, stream):
        return _put_file(self, stream)

    def open_reader(self, key):
        return _open_reader(self, key)

    def _root_fd(self):
        return os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

    def put(self, data):
        digest, prefix = hashlib.sha256(data).hexdigest(), uuid.uuid4().hex
        key = prefix + '/' + digest
        root = self._root_fd()
        try:
            os.mkdir(prefix, dir_fd=root)
            folder = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            try:
                fd = os.open(digest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=folder)
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(folder)
            finally:
                os.close(folder)
            os.fsync(root)
        finally:
            os.close(root)
        return {'key': key, 'sha256': digest, 'size': len(data)}

    def get(self, key):
        validate_key(key)
        root = self._root_fd()
        try:
            prefix, digest = key.split('/')
            folder = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            try:
                fd = os.open(digest, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=folder)
                with os.fdopen(fd, 'rb') as stream:
                    data = stream.read()
                if hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError('artifact checksum mismatch')
                return data
            finally:
                os.close(folder)
        finally:
            os.close(root)

    def delete(self, key):
        validate_key(key)
        root = self._root_fd()
        try:
            prefix, digest = key.split('/')
            try:
                folder = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            except FileNotFoundError:
                return
            try:
                try:
                    os.unlink(digest, dir_fd=folder)
                    os.fsync(folder)
                except FileNotFoundError:
                    pass
            finally:
                os.close(folder)
            # Keep the empty prefix: deleting/recreating a parent introduces a race.
        finally:
            os.close(root)


class S3Artifacts:
    def __init__(self, bucket, **client_options):
        import boto3
        self.client = boto3.client('s3', **client_options)
        self.bucket = bucket

    def put_file(self, stream):
        return _put_file(self, stream)

    def open_reader(self, key):
        return _open_reader(self, key)

    def download_link(self, key):
        return _download_link(self, key)

    def put(self, data):
        digest = hashlib.sha256(data).hexdigest()
        key = uuid.uuid4().hex + '/' + digest
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data,
                               Metadata={'sha256': digest}, IfNoneMatch='*')
        return {'key': key, 'sha256': digest, 'size': len(data)}

    def get(self, key):
        validate_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        with response['Body'] as body:
            data = body.read()
        if hashlib.sha256(data).hexdigest() != key.split('/')[1]:
            raise ValueError('artifact checksum mismatch')
        return data

    def delete(self, key):
        validate_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=key)


def _put_file(self, stream):
    """Bounded input upload. Rewindable file/spool required; no full-book read."""
    stream.seek(0)
    digest, size = hashlib.sha256(), 0
    while True:
        block = stream.read(1024 * 1024)
        if not block:
            break
        digest.update(block)
        size += len(block)
    checksum = digest.hexdigest()
    key = uuid.uuid4().hex + '/' + checksum
    stream.seek(0)
    if isinstance(self, S3Artifacts):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=stream, ContentLength=size,
                               Metadata={'sha256': checksum}, IfNoneMatch='*')
    else:
        prefix, name = key.split('/')
        root = self._root_fd()
        try:
            os.mkdir(prefix, dir_fd=root)
            folder = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            try:
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=folder)
                with os.fdopen(fd, 'wb') as destination:
                    while True:
                        block = stream.read(1024 * 1024)
                        if not block:
                            break
                        destination.write(block)
                    destination.flush()
                    os.fsync(destination.fileno())
                os.fsync(folder)
            finally:
                os.close(folder)
            os.fsync(root)
        finally:
            os.close(root)
    stream.seek(0)
    return {'key': key, 'sha256': checksum, 'size': size}




def _open_reader(self, key):
    validate_key(key)
    if isinstance(self, S3Artifacts):
        return self.client.get_object(Bucket=self.bucket, Key=key)['Body']
    root = self._root_fd()
    try:
        prefix, name = key.split('/')
        folder = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=folder)
            return os.fdopen(fd, 'rb')
        finally:
            os.close(folder)
    finally:
        os.close(root)


def _download_link(self, key):
    import boto3
    validate_key(key)
    endpoint = os.getenv('OPS_S3_PUBLIC_ENDPOINT')
    if not endpoint:
        raise ValueError('public object-store endpoint is not configured')
    client = boto3.client('s3', endpoint_url=endpoint, region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    return client.generate_presigned_url('get_object', Params={'Bucket':self.bucket,'Key':key}, ExpiresIn=60)
