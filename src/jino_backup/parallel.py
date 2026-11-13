"""Streaming inventory and independent FTPS workers under a shared session budget."""
import posixpath
import queue
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager

from .curl_transfer import CurlFileSource
from .downloads import local_path
from .errors import BackupError
from .retry import retry_call, TRANSIENT_ERRORS
from .state import private_dir
from .storage import sha256_file
from .transfer import name_compatibility_issue, relative_path, source_for, display_remote_path, unsupported_type


class ConnectionBudget:
    def __init__(self, limit=10):
        if type(limit) is not int or not 1 <= limit <= 10:
            raise BackupError("CONNECTION_LIMIT_INVALID")
        self.limit, self.used = limit, 0
        self.condition = threading.Condition()
        self.verification_slot = threading.Semaphore(1)

    def snapshot(self):
        with self.condition:
            return {"used": self.used, "limit": self.limit}

    @contextmanager
    def lease(self, control):
        while True:
            control.checkpoint()
            with self.condition:
                if self.used < self.limit:
                    self.used += 1
                    break
                self.condition.wait(0.1)
        try:
            yield
        finally:
            with self.condition:
                self.used -= 1
                self.condition.notify_all()


class RunControl:
    def __init__(self, base, abort, deadline, wait=None):
        self.base, self.abort, self.deadline, self.test_wait = base, abort, deadline, wait

    def checkpoint(self):
        self.base.checkpoint(interrupted=self.abort.is_set)
        if time.monotonic() > self.deadline:
            raise BackupError("TRANSFER_TIMEOUT")

    def wait(self, seconds):
        self.checkpoint()
        if self.test_wait:
            self.test_wait(seconds)
        else:
            self.base.wait(seconds, interrupted=self.abort.is_set)
        self.checkpoint()


class MetadataSession:
    def __init__(self, source, site, control, notify):
        self.source, self.site, self.control, self.notify = source, site, control, notify
        self.mutex = threading.Lock()
        self.priority = threading.Lock()
        self.waiting = 0
        self.connected = False
        self.canonical_root = None

    @contextmanager
    def locked(self, low_priority=False):
        registered = False
        acquired = False
        if not low_priority:
            with self.priority:
                self.waiting += 1
                registered = True
        try:
            while not acquired:
                self.control.checkpoint()
                with self.priority:
                    if not low_priority or not self.waiting:
                        acquired = self.mutex.acquire(blocking=False)
                    if acquired and registered:
                        self.waiting -= 1
                        registered = False
                if not acquired:
                    self.control.abort.wait(0.01)
            yield
        finally:
            if registered:
                with self.priority:
                    self.waiting -= 1
            if acquired:
                self.mutex.release()

    def call(self, operation, low_priority=False):
        with self.locked(low_priority):
            try:
                if not self.connected:
                    self.source.__enter__()
                    canonical = getattr(self.source, "canonical_root", self.site["remote_root"])
                    if self.canonical_root is not None and canonical != self.canonical_root:
                        raise BackupError("SOURCE_CHANGED")
                    self.canonical_root = canonical
                    self.connected = True
                return operation()
            except BackupError as error:
                if error.code in TRANSIENT_ERRORS:
                    self.close()
                raise

    def open(self):
        return retry_call(lambda: self.call(lambda: None), wait=self.control.wait, notify=self.notify,
                          phase="SCANNING")

    def close(self):
        self.source.__exit__()
        self.connected = False

    def listdir(self, parent):
        return retry_call(lambda: self.call(lambda: list(self.source.listdir(parent))),
                          wait=self.control.wait,
                          notify=lambda **event: self.notify(directory=parent or "/", current_file="", **event),
                          phase="SCANNING")

    def stat(self, relative, low_priority=False):
        return self.call(lambda: self.source.stat(relative), low_priority)


class ParallelCollector:
    def __init__(self, control, progress, budget, *, connections=3,
                 metadata_factory=source_for, download_factory=CurlFileSource,
                 skip_unsupported_names=False, skip_unsupported_types=False, continue_on_error=True,
                 max_bytes=1024**4, max_files=1000000, timeout_seconds=86400, wait=None):
        if type(connections) is not int or not 2 <= connections <= 5:
            raise BackupError("CONNECTION_LIMIT_INVALID")
        self.base_control, self.progress, self.budget = control, progress, budget
        self.connections, self.metadata_factory, self.download_factory = connections, metadata_factory, download_factory
        self.skip_names, self.continue_on_error = skip_unsupported_names, continue_on_error
        self.skip_types = skip_unsupported_types
        self.max_bytes, self.max_files, self.timeout, self.wait = max_bytes, max_files, timeout_seconds, wait
        self.verifying = 0
        self.verify_queue = queue.Queue(maxsize=max_files)

    def collect(self, credentials, site, snapshot, checkpoint):
        self.snapshot, self.checkpoint = snapshot, checkpoint
        private_dir(local_path(snapshot, "files"))
        self.abort, self.ready, self.scanned = threading.Event(), threading.Event(), threading.Event()
        self.control = RunControl(self.base_control, self.abort, time.monotonic() + self.timeout, self.wait)
        self.files_queue = queue.Queue(maxsize=32)
        # Completed files must never fill the download queue or block inventory.
        self.verify_queue = queue.Queue(maxsize=self.max_files)
        self.verifying = 0
        self.mutex = threading.RLock()
        self.emit_mutex = threading.RLock()
        self.failure = None
        self.items, self.directories, self.errors, self.skipped = [], [], [], []
        self.active, self.planned_bytes, self.planned_files, self.directories_seen = {}, 0, 0, 0
        self.incomplete = False
        self.inventory_complete = False
        self.transfer_limit = self.connections - 1
        self.recovery_at = time.monotonic() + 60
        with checkpoint.mutex:
            self.committed = {row["path"]: (row["state"], row["committed"])
                              for row in checkpoint.db.execute("SELECT path,state,committed FROM files")}
        checkpoint.on_commit = self.committed_file
        metadata = MetadataSession(self.metadata_factory(credentials, site), site, self.control, self.emit)
        self.metadata = metadata
        barrier = threading.Barrier(self.connections + 1)

        def run(scanner=False, index=1, verifier=False):
            self.base_control.register_worker()
            try:
                barrier.wait(timeout=15)
                if verifier:
                    self.verify()
                elif scanner:
                    metadata.open()
                    self.ready.set()
                    self.scan()
                else:
                    self.download(credentials, site, index)
            except BaseException as error:
                with self.mutex:
                    if self.failure is None:
                        self.failure = error
                self.abort.set()
                self.ready.set()
                self.scanned.set()
            finally:
                self.base_control.finish_worker()

        downloads = []
        try:
            with self.budget.lease(self.control):
                try:
                    with ThreadPoolExecutor(max_workers=self.connections + 1, thread_name_prefix="ftps") as pool:
                        downloads.extend(pool.submit(run, False, index + 1) for index in range(self.connections - 1))
                        downloads.append(pool.submit(run, verifier=True))
                        scanner = pool.submit(run, True)
                        scanner.result()
                        for future in downloads:
                            future.result()
                finally:
                    metadata.close()
            if self.failure:
                raise self.failure
            if not self.items and not self.errors:
                raise BackupError("FILES_MISSING")
            if not self.errors:
                checkpoint.validate_inventory(self.items)
            result = {"count": len(self.items), "bytes": sum(item["size_bytes"] for item in self.items),
                      "items": sorted(self.items, key=lambda item: item["path"]),
                      "directories": sorted(self.directories)}
            if self.errors:
                result["errors"] = self.errors
            if self.skipped:
                result["skipped"] = self.skipped
            self.emit(stage="FILES_DOWNLOAD", log=True)
            return result
        finally:
            checkpoint.on_commit = None

    def committed_file(self, path, state, size):
        with self.mutex:
            previous_state, previous_size = self.committed.get(path, (None, 0))
            advanced = size > previous_size or state == "done" and previous_state != "done"
            now = time.monotonic()
            if advanced and now >= self.recovery_at and self.transfer_limit < self.connections - 1:
                self.transfer_limit += 1
                self.recovery_at = now + 60
            self.committed[path] = (state, size)
            for value in self.active.values():
                if value["path"] == path:
                    value["received_bytes"] = size
        self.emit()

    def emit(self, **event):
        with self.emit_mutex:
            self._emit(**event)

    def _emit(self, **event):
        with self.mutex:
            if event.get("stage") == "RETRY_WAIT" and not event.get('verification_retry'):
                self.transfer_limit = max(1, self.transfer_limit - 1)
                self.recovery_at = time.monotonic() + 60
            active = [dict(value) for value in self.active.values()]
            if event.get('stage') == 'FILE_VERIFY' and active:
                event['stage'] = 'FILE_DOWNLOAD'
            pending_checks = self.verify_queue.unfinished_tasks
            default_stage = 'FILE_DOWNLOAD' if active else 'FILE_VERIFY' if pending_checks else 'FILES_DOWNLOAD'
            values = {"stage": default_stage, "received_bytes": sum(size for _, size in self.committed.values()),
                      "verified_files": sum(state == "done" for state, _ in self.committed.values()),
                      "planned_bytes": self.planned_bytes, "planned_files": self.planned_files,
                      "directories_seen": self.directories_seen,
                      "inventory_complete": self.inventory_complete,
                      "processed_files": len(getattr(self, 'items', [])),
                      "local_verifications": self.verifying,
                      "pending_verifications": pending_checks,
                      "active_files": active, "connections_limit": self.connections,
                      "active_transfers": len(active), **event}
            values["parallel_limit"] = self.transfer_limit
            metadata_source = getattr(getattr(self, 'metadata', None), 'source', None)
            values['metadata_mlst_checks'] = getattr(metadata_source, 'mlst_checks', 0)
            values['metadata_listing_checks'] = getattr(metadata_source, 'stat_listings', 0)
            selected = next((item for item in active if item["path"] == event.get("current_file")), None)
            if "current_file" not in event:
                selected = active[0] if active else None
                values["current_file"] = selected["path"] if selected else ""
            values.update(current_size=selected["size_bytes"] if selected else 0,
                          current_bytes=selected["received_bytes"] if selected else 0)
        self.progress(**values)

    def scan(self):
        pending, seen = [("", 0)], set()
        while pending:
            self.control.checkpoint()
            parent, depth = pending.pop()
            if depth > 64:
                raise BackupError("TRANSFER_LIMIT_EXCEEDED")
            self.emit(stage="SCANNING", log=True, directory=parent or "/")
            try:
                entries = self.metadata.listdir(parent)
            except BackupError as error:
                self.emit(stage="DIRECTORY_ERROR", log=True, directory=parent or "/",
                          error_code=error.code, **error.details)
                if not parent or not self.continue_on_error or error.code != "REMOTE_DIRECTORY_UNAVAILABLE":
                    raise
                self.incomplete = True
                self.errors.append({"path": parent, "kind": "dir", "error_code": error.code, **error.details})
                continue
            self.directories_seen += 1
            for entry in entries:
                self.control.checkpoint()
                issue = name_compatibility_issue(entry.name)
                path = posixpath.join(parent, entry.name)
                if path.casefold() in seen:
                    raise BackupError("REMOTE_PATH_COLLISION")
                seen.add(path.casefold())
                if len(seen) > self.max_files:
                    raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                if issue:
                    if not self.skip_names:
                        raise BackupError("REMOTE_PATH_UNSAFE")
                    display_path = display_remote_path(path)
                    omission = {"path": display_path, "kind": entry.kind if entry.kind in {'file', 'dir'} else 'special', "reason": issue, "error_code": "REMOTE_NAME_UNSUPPORTED"}
                    self.skipped.append(omission)
                    self.errors.append(dict(omission))
                    self.emit(stage="FILE_SKIPPED", log=True, skipped_count=len(self.skipped),
                              skipped_path=display_path, skipped_kind=omission['kind'], skip_reason=issue,
                              error_code="REMOTE_NAME_UNSUPPORTED")
                    continue
                relative_path(path)
                special = unsupported_type(entry.kind)
                if self.skip_types and special:
                    omission = {"path": display_remote_path(path), "kind": "special",
                                "reason": special, "error_code": "REMOTE_TYPE_UNSUPPORTED"}
                    self.skipped.append(omission)
                    self.errors.append(dict(omission))
                    self.emit(stage="FILE_SKIPPED", log=True, skipped_count=len(self.skipped),
                              skipped_path=omission['path'], skipped_kind="special",
                              skip_reason=special, error_code="REMOTE_TYPE_UNSUPPORTED")
                    continue
                if entry.kind == "dir":
                    private_dir(local_path(self.snapshot, "files/" + path))
                    self.directories.append(path)
                    pending.append((path, depth + 1))
                elif entry.kind == "file":
                    self.planned_files += 1
                    self.planned_bytes += entry.size
                    if entry.size < 0 or self.planned_bytes > self.max_bytes:
                        raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                    if self.committed.get(path, (None, 0))[0] == 'done':
                        self.verify_queue.put_nowait((path, entry))
                        continue
                    while True:
                        self.control.checkpoint()
                        try:
                            self.files_queue.put((path, entry), timeout=0.1)
                            break
                        except queue.Full:
                            pass
                else:
                    raise BackupError("REMOTE_TYPE_UNSAFE")
            self.emit(stage="SCANNING")
        self.inventory_complete = not self.incomplete
        self.scanned.set()
        self.emit(stage="FILES_DOWNLOAD", log=True)

    def verify(self):
        """One local verifier per job; at most one local hash across this executor."""
        while not self.ready.wait(0.1):
            self.control.checkpoint()
        while True:
            self.control.checkpoint()
            try:
                path, entry = self.verify_queue.get(timeout=0.1)
            except queue.Empty:
                if self.scanned.is_set():
                    return
                continue
            item = None
            try:
                self.emit(stage='FILE_VERIFY', log=True)
                def once():
                    nonlocal item
                    if self.metadata.stat(path, low_priority=True) != entry:
                        raise BackupError('SOURCE_CHANGED')
                    while not self.budget.verification_slot.acquire(timeout=0.1):
                        self.control.checkpoint()
                    try:
                        with self.mutex:
                            self.verifying += 1
                        self.emit()
                        item = self.checkpoint.verify_done(path, entry, self.control)
                    finally:
                        with self.mutex:
                            self.verifying -= 1
                        self.budget.verification_slot.release()
                    if self.metadata.stat(path, low_priority=True) != entry:
                        raise BackupError('SOURCE_CHANGED')
                retry_call(once, wait=self.control.wait,
                           notify=lambda **event: self.emit(verification_retry=True, **event),
                           phase='FILE_VERIFY')
                with self.mutex:
                    self.items.append(item)
            except BackupError as error:
                self.emit(stage='FILE_ERROR', log=True, error_code=error.code, **error.details)
                if not self.continue_on_error or error.code not in {
                    'SOURCE_CHANGED', 'TRANSFER_DENIED', 'LOCAL_FILE_CHANGED',
                    'LOCAL_PROGRESS_INVALID', 'REMOTE_DIRECTORY_UNAVAILABLE',
                }:
                    raise
                with self.mutex:
                    self.errors.append({'path':path, 'error_code':error.code, **error.details})
            finally:
                self.verify_queue.task_done()
                self.emit()

    def download(self, credentials, site, index):
        while not self.ready.wait(0.1):
            self.control.checkpoint()
        self.control.checkpoint()
        with ExitStack() as stack:
            source = None
            while True:
                self.control.checkpoint()
                if index > self.transfer_limit:
                    stack.close()
                    source = None
                    if self.scanned.is_set() and self.files_queue.empty():
                        return
                    self.abort.wait(0.1)
                    continue
                try:
                    path, entry = self.files_queue.get(timeout=0.1)
                except queue.Empty:
                    if self.scanned.is_set():
                        return
                    continue
                if source is None:
                    stack.enter_context(self.budget.lease(self.control))
                    source = stack.enter_context(self.download_factory(credentials, site, self.metadata, self.control))
                worker = threading.get_ident()
                with self.mutex:
                    self.active[worker] = {"path": path, "size_bytes": entry.size, "received_bytes": 0}
                is_verified_copy = self.committed.get(path, (None, 0))[0] == 'done'
                self.emit(stage="FILE_VERIFY" if is_verified_copy else "FILE_DOWNLOAD", log=True, current_file=path)
                local = local_path(self.snapshot, "files/" + path)
                final_event = {}
                try:
                    def once():
                        self.control.checkpoint()
                        saved_local = local.exists()
                        if saved_local and source.stat(path) != entry:
                            raise BackupError('SOURCE_CHANGED')
                        if shutil.disk_usage(self.snapshot).free < entry.size + 1024 * 1024:
                            raise BackupError("STORAGE_SPACE_LOW")
                        self.checkpoint.fetch(source, path, entry, local, self.control.deadline)
                        if saved_local and source.stat(path) != entry:
                            raise BackupError('SOURCE_CHANGED')
                    retry_call(once, reconnect=getattr(source, "reconnect", None), wait=self.control.wait,
                               notify=lambda **event: self.emit(current_file=path, **event),
                               progress=lambda: self.checkpoint.committed_offset(path), phase="FILES_DOWNLOAD")
                    item = {"path": path, "size_bytes": entry.size, "sha256": sha256_file(local)}
                    with self.mutex:
                        self.items.append(item)
                except BackupError as error:
                    self.emit(stage="FILE_ERROR", log=True, current_file=path, error_code=error.code, **error.details)
                    if not self.continue_on_error or error.code not in {
                        "SOURCE_CHANGED", "TRANSFER_DENIED", "LOCAL_FILE_CHANGED", "LOCAL_PROGRESS_INVALID",
                        "REMOTE_DIRECTORY_UNAVAILABLE",
                    }:
                        if error.code in TRANSIENT_ERRORS:
                            final_event = {"stage": "RETRY_EXHAUSTED", "log": True,
                                           "current_file": path, **error.details,
                                           "error_code": error.code, "retry_attempt": 5, "retry_limit": 5}
                        raise
                    with self.mutex:
                        self.errors.append({"path": path, "error_code": error.code, **error.details})
                finally:
                    with self.mutex:
                        self.active.pop(worker, None)
                    self.files_queue.task_done()
                    self.emit(**final_event)
