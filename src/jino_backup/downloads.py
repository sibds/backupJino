"""Durable checkpoints for explicitly requested unencrypted file downloads."""

import hashlib
import json
import os
import sqlite3
import stat
import threading
import time
from pathlib import Path

from .errors import BackupError
from .state import private_dir, write_json
from .storage import CHUNK, sha256_file
from .transfer import relative_path


def local_path(root, relative):
    relative_path(relative)
    root = Path(root).resolve()
    target = root.joinpath(*relative.split('/')) if relative else root
    current = target
    while current != root:
        if current.exists() or current.is_symlink():
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
                raise BackupError('LOCAL_PATH_UNSAFE')
        current = current.parent
    if not target.resolve().is_relative_to(root):
        raise BackupError('LOCAL_PATH_UNSAFE')
    return target


class FileLock:
    def __init__(self, path):
        self.path, self.stream = Path(path), None

    def __enter__(self):
        private_dir(self.path.parent)
        self.stream = self.path.open('a+b')
        if self.path.stat().st_size == 0:
            self.stream.write(b'0')
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            self.stream = None
            raise BackupError('DOWNLOAD_BUSY') from None
        return self

    def __exit__(self, *args):
        if self.stream is not None:
            self.stream.close()  # OS releases the lock, including after process termination.
            self.stream = None


class DirectoryCheckpoint:
    def __init__(self, root, scope, notify=None):
        self.root, self.scope = Path(root).absolute(), scope
        self.db = self.lock = None
        self.notify = notify or (lambda event: None)
        self.last_notice = 0
        self.mutex = threading.RLock()
        self.on_commit = None

    def __enter__(self):
        if self.root.is_symlink():
            raise BackupError('LOCAL_PATH_UNSAFE')
        marker = self.root / '.jino-owner.json'
        if self.root.exists():
            if not marker.is_file():
                raise BackupError('DESTINATION_NOT_OWNED')
            try:
                owner = json.loads(marker.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                raise BackupError('DESTINATION_NOT_OWNED') from None
            if owner != {'schema_version':'1.0', 'scope':self.scope}:
                raise BackupError('DOWNLOAD_SCOPE_CHANGED')
        else:
            self.root.mkdir(parents=True, exist_ok=False, mode=0o700)
            write_json(marker, {'schema_version':'1.0', 'scope':self.scope})
        self.work = local_path(self.root, '.jino-progress')
        private_dir(self.work)
        self.lock = FileLock(local_path(self.root, '.jino-progress/download.lock'))
        self.lock.__enter__()
        try:
            self.db = sqlite3.connect(local_path(self.root, '.jino-progress/progress.sqlite3'),
                                      check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.db.execute('''CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY, size INTEGER NOT NULL, modified TEXT,
                state TEXT NOT NULL, committed INTEGER NOT NULL, sha256 TEXT NOT NULL)''')
            self.db.commit()
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *args):
        try:
            if self.db is not None:
                self.db.close()
                self.db = None
        finally:
            if self.lock is not None:
                self.lock.__exit__()

    def record(self, relative, entry, state, size, digest):
        event = None
        with self.mutex:
            with self.db:
                self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?,?)',
                                (relative, entry.size, entry.modified, state, size, digest))
            if time.monotonic() - self.last_notice >= 10:
                row = self.db.execute("SELECT COUNT(*) AS files, COALESCE(SUM(committed),0) AS bytes FROM files").fetchone()
                event = {'event':'progress', 'files_seen':row['files'], 'bytes_saved':row['bytes']}
                self.last_notice = time.monotonic()
        if self.on_commit:
            self.on_commit(relative, state, size)
        if event:
            self.notify(event)

    def fetch(self, source, relative, entry, local, deadline):
        local = local_path(self.root, 'files/' + relative)
        private_dir(local.parent)
        partial = local_path(self.root, '.jino-progress/' + hashlib.sha256(relative.encode()).hexdigest() + '.part')
        with self.mutex:
            row = self.db.execute('SELECT * FROM files WHERE path=?', (relative,)).fetchone()
        unchanged = bool(row and row['size'] == entry.size and row['modified'] == entry.modified and entry.modified)
        if row and row['state'] == 'done':
            if not unchanged:
                raise BackupError('SOURCE_CHANGED')
            if not local.is_file() or local.stat().st_size != entry.size or sha256_file(local) != row['sha256']:
                raise BackupError('LOCAL_FILE_CHANGED')
            return
        # A crash after rename but before the final database commit is recoverable.
        if local.exists():
            if (unchanged and row['committed'] == entry.size and local.is_file()
                    and sha256_file(local) == row['sha256']):
                self.record(relative, entry, 'done', entry.size, row['sha256'])
                return
            raise BackupError('LOCAL_DESTINATION_CONFLICT')
        offset = row['committed'] if unchanged else 0
        digest = hashlib.sha256()
        if offset:
            if not partial.is_file() or partial.stat().st_size < offset:
                raise BackupError('LOCAL_PROGRESS_INVALID')
            with partial.open('rb') as stream:
                remaining = offset
                while remaining:
                    chunk = stream.read(min(CHUNK, remaining))
                    if not chunk:
                        raise BackupError('LOCAL_PROGRESS_INVALID')
                    digest.update(chunk)
                    remaining -= len(chunk)
            if digest.hexdigest() != row['sha256']:
                raise BackupError('LOCAL_PROGRESS_INVALID')
        with partial.open('r+b' if partial.exists() else 'x+b') as stream:
            stream.truncate(offset)  # Discard only uncommitted bytes owned by this download.
            stream.seek(offset)
            writer = CheckpointWriter(stream, digest, offset, entry.size, deadline,
                                      lambda size, checksum: self.record(relative, entry, 'partial', size, checksum))
            try:
                if source.stat(relative) != entry:
                    raise BackupError('SOURCE_CHANGED')
                if offset < entry.size:
                    source.download(relative, writer, offset)
            finally:
                writer.commit()
        if partial.stat().st_size != entry.size or source.stat(relative) != entry:
            raise BackupError('SOURCE_CHANGED')
        os.replace(partial, local)
        self.record(relative, entry, 'done', entry.size, digest.hexdigest())

    def verify_done(self, relative, entry, control, *, require_modified=True):
        """Check a completed local file without a network session or DB lock during hashing."""
        local = local_path(self.root, 'files/' + relative)
        with self.mutex:
            row = self.db.execute('SELECT * FROM files WHERE path=?', (relative,)).fetchone()
        if (not row or row['state'] != 'done' or row['size'] != entry.size
                or require_modified and not entry.modified or row['modified'] != entry.modified):
            raise BackupError('SOURCE_CHANGED')
        if not local.is_file() or local.stat().st_size != entry.size:
            raise BackupError('LOCAL_FILE_CHANGED')
        digest = hashlib.sha256()
        with local.open('rb') as stream:
            while True:
                control.checkpoint()
                chunk = stream.read(CHUNK)
                if not chunk:
                    break
                digest.update(chunk)
        if local.stat().st_size != entry.size or digest.hexdigest() != row['sha256']:
            raise BackupError('LOCAL_FILE_CHANGED')
        return {'path':relative, 'size_bytes':entry.size, 'sha256':digest.hexdigest()}

    def committed_offset(self, relative):
        with self.mutex:
            row = self.db.execute('SELECT committed FROM files WHERE path=?', (relative,)).fetchone()
        return row[0] if row else 0

    def validate_inventory(self, items):
        expected = {item['path'] for item in items}
        previous = {row[0] for row in self.db.execute('SELECT path FROM files')}
        if previous != expected:
            raise BackupError('SOURCE_CHANGED')
        actual = set()
        for directory, names, files in os.walk(self.root / 'files', followlinks=False):
            for name in [*names, *files]:
                relative = (Path(directory) / name).relative_to(self.root).as_posix()
                local_path(self.root, relative)
            actual.update((Path(directory) / name).relative_to(self.root / 'files').as_posix() for name in files)
        if actual != expected:
            raise BackupError('LOCAL_DESTINATION_CONFLICT')


class CheckpointWriter:
    def __init__(self, stream, digest, offset, size, deadline, commit):
        self.stream, self.digest = stream, digest
        self.offset, self.limit, self.deadline = offset, size, deadline
        self.callback, self.committed = commit, offset
        self.persisted = None

    def write(self, data):
        if time.monotonic() > self.deadline:
            raise BackupError('TRANSFER_TIMEOUT')
        if self.offset + len(data) > self.limit:
            raise BackupError('SOURCE_CHANGED')
        self.stream.write(data)
        self.digest.update(data)
        self.offset += len(data)
        if self.offset - self.committed >= 8 * CHUNK:
            self.commit()
        return len(data)

    def commit(self):
        if self.persisted == self.offset:
            return
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.callback(self.offset, self.digest.hexdigest())
        self.committed = self.offset
        self.persisted = self.offset
