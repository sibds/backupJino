"""Streaming inventory and independent FTPS workers under a shared session budget."""
import posixpath
import queue
import shutil
import threading
import time
import weakref
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager

from .curl_transfer import CurlFileSource
from .downloads import local_path
from .errors import BackupError
from .retry import retry_call, TRANSIENT_ERRORS
from .state import private_dir
from .transfer import Entry, name_compatibility_issue, relative_path, source_for, display_remote_path, unsupported_type
from .unsafe_paths import display_path,omission,log_omission


class ConnectionBudget:
    def __init__(self, limit=10):
        if type(limit) is not int or not 1 <= limit <= 10:
            raise BackupError("CONNECTION_LIMIT_INVALID")
        self.limit, self.used = limit, 0
        self.condition = threading.Condition()
        self.waiters = 0
        self.verification_slot = threading.Semaphore(1)
        self.scan_slots=threading.Semaphore(3)
        self.metadata_limit=min(3,limit)
        self.metadata_reserve=min(3,max(1,limit//3))
        self.data_limit=max(1,limit-self.metadata_reserve)
        self.metadata_used=self.data_used=0
        self.metadata_waiters=deque()
        self.metadata_sessions=weakref.WeakSet()

    def snapshot(self):
        with self.condition:
            return {"used": self.used, "limit": self.limit}

    @contextmanager
    def lease(self, control, role='general'):
        waiting = False
        ticket=object()
        started=time.monotonic()
        if role=='metadata':
            with self.condition:self.metadata_waiters.append(ticket)
        try:
            while True:
                control.checkpoint()
                with self.condition:
                    eligible=(role!='metadata' or self.metadata_used<self.metadata_limit and self.metadata_waiters[0] is ticket)
                    eligible=eligible and (role!='data' or self.data_used<self.data_limit)
                    if role!='metadata' and self.metadata_sessions:
                        eligible=eligible and self.used<self.limit-max(0,self.metadata_reserve-self.metadata_used)
                    if self.used < self.limit and eligible:
                        self.used += 1
                        if role=='metadata':
                            self.metadata_used+=1;self.metadata_waiters.popleft()
                        if role=='data':self.data_used+=1
                        break
                    if not waiting:
                        self.waiters += 1
                        waiting = True
                with self.condition:
                    reclaim=(role=='metadata' or role=='data' and self.metadata_used>self.metadata_reserve
                             and self.data_used<self.data_limit)
                if reclaim:
                    self.reclaim_metadata(time.monotonic()-started>=1)
                with self.condition:self.condition.wait(0.05)
        finally:
            if role=='metadata':
                with self.condition:
                    if ticket in self.metadata_waiters:self.metadata_waiters.remove(ticket)
                    self.condition.notify_all()
            if waiting:
                with self.condition:
                    self.waiters -= 1
        try:
            yield
        finally:
            with self.condition:
                self.used -= 1
                if role=='metadata':self.metadata_used-=1
                if role=='data':self.data_used-=1
                self.condition.notify_all()

    def register_metadata(self,session):
        with self.condition:self.metadata_sessions.add(session)

    def unregister_metadata(self,session):
        with self.condition:
            self.metadata_sessions.discard(session)
            self.condition.notify_all()

    def reclaim_metadata(self,urgent=False):
        with self.condition:sessions=sorted(self.metadata_sessions,key=lambda s:s.last_used)
        for session in sessions:
            if session.release_idle(0 if urgent else .25):return

    def contended(self):
        with self.condition:
            return self.waiters > 0


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
    def __init__(self, source, site, control, notify, budget=None):
        self.source, self.site, self.control, self.notify = source, site, control, notify
        self.mutex = threading.RLock()
        self.priority = threading.Lock()
        self.waiting = 0
        self.parent_readers=0
        self.active_calls=0
        self.connected = False
        self.canonical_root = None
        self.checked_parents = set()
        self.budget,self.connection_lease=budget,None
        self.last_used=time.monotonic()
        self.opened_at=self.last_used
        if budget:budget.register_metadata(self)

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
            self.active_calls+=1
            try:
                if not self.connected:
                    self.checked_parents.clear()
                    if self.budget:
                        self.connection_lease=self.budget.lease(self.control,role='metadata')
                        self.connection_lease.__enter__()
                    try:self.source.__enter__()
                    except BaseException:
                        self.close();raise
                    canonical = getattr(self.source, "canonical_root", self.site["remote_root"])
                    if self.canonical_root is not None and canonical != self.canonical_root:
                        raise BackupError("DOWNLOAD_SCOPE_CHANGED")
                    self.canonical_root = canonical
                    self.connected = True
                    self.opened_at=time.monotonic()
                return operation()
            except BackupError as error:
                if error.code in TRANSIENT_ERRORS or not self.connected:
                    self.close()
                raise
            finally:
                self.active_calls-=1
                self.last_used=time.monotonic()

    def open(self):
        return retry_call(lambda: self.call(lambda: None), wait=self.control.wait,
                          notify=lambda **event:self.notify(metadata_retry=True, **event),
                          phase="SCANNING")

    def close(self):
        with self.mutex:
            try:self.source.__exit__()
            finally:
                self.connected = False
                self.checked_parents.clear()
                if self.connection_lease is not None:
                    lease,self.connection_lease=self.connection_lease,None
                    lease.__exit__(None,None,None)

    def release_idle(self,age):
        if not self.mutex.acquire(blocking=False):return False
        try:
            now=time.monotonic()
            with self.priority:validating=self.parent_readers>0
            if not validating and not self.active_calls and self.connected and now-self.last_used>=age and (age>0 or now-self.opened_at>=2):
                self.close();return True
            return False
        finally:self.mutex.release()

    def listdir(self, parent):
        return retry_call(lambda: self.call(lambda: list(self.source.listdir(parent))),
                          wait=self.control.wait,
                          notify=lambda **event: self.notify(metadata_retry=True, directory=parent or "/", current_file="", **event),
                          phase="SCANNING")

    def stat(self, relative, low_priority=False):
        return self.call(lambda: self.source.stat(relative), low_priority)

    def validate_parents(self,relative,low_priority=False):
        with self.priority:self.parent_readers+=1
        try:
            parts=relative_path(relative).split('/')[:-1]
            for count in range(1,len(parts)+1):
                parent='/'.join(parts[:count])
                if parent in self.checked_parents:continue
                entry=self.stat(parent,low_priority=low_priority)
                if entry.kind!='dir':raise BackupError('REMOTE_TYPE_UNSAFE')
                self.checked_parents.add(parent)
        finally:
            with self.priority:self.parent_readers-=1


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
        self.verify_queue = queue.PriorityQueue(maxsize=max_files)
        self.saved_bytes = self.saved_done = 0
        self.resumed_files = self.cached_directories = 0

    def collect(self, credentials, site, snapshot, checkpoint):
        self.snapshot, self.checkpoint = snapshot, checkpoint
        self.walk=None
        private_dir(local_path(snapshot, "files"))
        self.abort, self.ready, self.scanned = threading.Event(), threading.Event(), threading.Event()
        deadline = float('inf') if self.timeout is None else time.monotonic() + self.timeout
        self.control = RunControl(self.base_control, self.abort, deadline, self.wait)
        self.files_queue = queue.Queue(maxsize=32)
        # Completed files must never fill the download queue or block inventory.
        self.verify_queue = queue.PriorityQueue(maxsize=self.max_files)
        self.downloads_done = threading.Event()
        self.verifying = 0
        self.mutex = threading.RLock()
        self.emit_mutex = threading.RLock()
        self.failure = None
        self.download_phase_complete = False
        self.enqueued_paths = set()
        self.resumed_inventory_bytes=0
        self.resumed_files = self.cached_directories = 0
        self.items, self.directories, self.errors, self.skipped = [], [], [], []
        self.collisions, self.duplicates, self.local_conflicts = [], [], []
        self.active, self.planned_bytes, self.planned_files, self.directories_seen = {}, 0, 0, 0
        self.incomplete = False
        self.inventory_complete = False
        self.transfer_limit = self.connections - 1
        self.recovery_at = time.monotonic() + 60
        with checkpoint.mutex:
            self.committed = {row["path"]: (row["state"], row["committed"])
                              for row in checkpoint.db.execute("SELECT path,state,committed FROM files")}
        self.saved_bytes = sum(size for _, size in self.committed.values())
        self.saved_done = sum(state == 'done' for state, _ in self.committed.values())
        checkpoint.on_commit = self.committed_file
        metadata = MetadataSession(self.metadata_factory(credentials, site), site, self.control, self.emit,self.budget)
        self.metadata = metadata
        self.emit(stage='RESUMING_FILES')
        barrier = threading.Barrier(self.connections + 1)

        def run(scanner=False, index=1, verifier=False):
            self.base_control.register_worker()
            try:
                barrier.wait(timeout=15)
                if verifier:
                    self.verify()
                elif scanner:
                    metadata.open()
                    from .resume_inventory import ResumeInventory
                    self.walk=ResumeInventory(checkpoint,metadata.canonical_root,self.max_files)
                    self.ready.set()
                    self.resume_discovered()
                    for _ in self.walk.import_pending_cached():
                        self.control.checkpoint()
                        self.resume_discovered()
                    self.resume_pending()
                    while not self.budget.scan_slots.acquire(timeout=.1):self.control.checkpoint()
                    try:self.scan()
                    finally:self.budget.scan_slots.release()
                else:
                    self.download(credentials, site, index)
            except BaseException as error:
                if scanner and self.walk is None and isinstance(error,BackupError) and error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                    self.skip_unsafe('/',error,'dir')
                    self.ready.set();self.scanned.set()
                    return
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
            try:
                with ThreadPoolExecutor(max_workers=self.connections + 1, thread_name_prefix="ftps") as pool:
                    downloads.extend(pool.submit(run, False, index + 1) for index in range(self.connections - 1))
                    verifier = pool.submit(run, verifier=True)
                    scanner = pool.submit(run, True)
                    scanner.result()
                    try:
                        for future in downloads:future.result()
                    finally:self.downloads_done.set()
                    self.download_phase_complete=True
                    self.emit(stage='FILE_VERIFY')
                    verifier.result()
                    if self.failure:raise self.failure
                    if self.collisions or self.local_conflicts:self.reconcile_conflicts(credentials, site)
            finally:metadata.close()
            if self.failure:
                raise self.failure
            if not self.items and not self.errors:
                raise BackupError("FILES_MISSING")
            if not self.errors:
                checkpoint.validate_inventory(self.items)
            result = {"count": len(self.items), "bytes": sum(item["size_bytes"] for item in self.items),
                      "items": sorted(self.items, key=lambda item: item["path"]),
                      'inventory_observed_since':getattr(self.walk,'observed_at',None),'inventory_resumed':getattr(self.walk,'resumed',False),
                      "directories": sorted(self.directories)}
            if self.errors:
                result["errors"] = self.errors
            if self.skipped:
                result["skipped"] = self.skipped
            if self.duplicates:
                result['duplicates'] = self.duplicates
            if checkpoint.retained_items():
                result['retained_local'] = checkpoint.retained_items()
            if not self.errors:self.walk.finish()
            self.emit(stage="FILES_DOWNLOAD", log=True)
            return result
        finally:
            checkpoint.on_commit = None
            self.budget.unregister_metadata(metadata)

    def committed_file(self, path, state, size):
        with self.mutex:
            previous_state, previous_size = self.committed.get(path, (None, 0))
            advanced = size > previous_size or state == "done" and previous_state != "done"
            now = time.monotonic()
            if advanced and now >= self.recovery_at and self.transfer_limit < self.connections - 1:
                self.transfer_limit += 1
                self.recovery_at = now + 60
            self.committed[path] = (state, size)
            self.saved_bytes += size - previous_size
            self.saved_done += int(state == 'done') - int(previous_state == 'done')
            for value in self.active.values():
                if value["path"] == path:
                    value["received_bytes"] = size
        self.emit()

    def emit(self, **event):
        with self.emit_mutex:
            self._emit(**event)

    def _emit(self, **event):
        with self.mutex:
            if (event.get("stage") == "RETRY_WAIT" and not event.get('verification_retry')
                    and not event.get('metadata_retry')
                    and event.get('operation') not in {'CWD','PWD','MLST','MLSD'}):
                self.transfer_limit = max(1, self.transfer_limit - 1)
                self.recovery_at = time.monotonic() + 60
            active = [dict(value) for value in self.active.values()]
            if event.get('stage') == 'FILE_VERIFY' and active:
                event['stage'] = 'FILE_DOWNLOAD'
            pending_checks = self.verify_queue.unfinished_tasks
            default_stage = 'FILE_DOWNLOAD' if active else 'FILE_VERIFY' if pending_checks else 'FILES_DOWNLOAD'
            values = {"stage": default_stage, "received_bytes": self.saved_bytes,
                      "verified_files": self.saved_done,
                      "planned_bytes": self.planned_bytes, "planned_files": self.planned_files,
                      "directories_seen": self.directories_seen,
                      "inventory_complete": self.inventory_complete,
                      "processed_files": len(getattr(self, 'items', [])),
                      "local_verifications": self.verifying,
                      "resumed_files": self.resumed_files,
                      "cached_directories": self.cached_directories,
                      "download_phase_complete": getattr(self,'download_phase_complete',False),
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

    def enqueue(self,path,entry):
        if path in self.enqueued_paths:return
        self.walk.register(path,entry)
        self.enqueued_paths.add(path)
        while True:
            self.control.checkpoint()
            try:
                self.files_queue.put((path,entry),timeout=0.1)
                return
            except queue.Full:pass

    def skip_unsafe(self,path,error,kind='file'):
        item=omission(path,error,kind)
        if self.walk:self.walk.omit(path,error.code)
        with self.mutex:
            if any(old['path']==item['path'] and old.get('reason')==item['reason'] for old in self.skipped):return
            self.incomplete=True
            self.skipped.append(item);self.errors.append(dict(item))
            count=len(self.skipped)
        log_omission(lambda event:self.emit(log=True,**event),item,count)

    def resume_discovered(self):
        for path,entry in self.walk.pending():
            self.control.checkpoint()
            if path in self.enqueued_paths or not self.walk.safe_cached(path):continue
            if entry.size<0:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
            self.resumed_inventory_bytes+=entry.size
            if self.resumed_inventory_bytes>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
            self.enqueue(path,entry)
            self.resumed_files+=1
            self.emit(stage='RESUMING_FILES')

    def resume_pending(self):
        """Old checkpoints are enough to resume bytes before any tree listing."""
        with self.checkpoint.mutex:
            rows=list(self.checkpoint.db.execute("SELECT path,size,modified FROM files WHERE state='partial' ORDER BY committed DESC,path"))
        if len(rows)>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        if sum(row['size'] for row in rows)>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        pending_bytes=0
        for row in rows:
            self.control.checkpoint()
            if row['path'] in self.enqueued_paths:continue
            try:path=relative_path(row['path'])
            except BackupError as error:
                if error.code!='REMOTE_PATH_UNSAFE' or not self.continue_on_error:raise
                self.skip_unsafe(row['path'],error);continue
            expected=Entry(posixpath.basename(path),'file',row['size'],row['modified'])
            if expected.size<0 or expected.size>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
            try:
                def checked():
                    self.metadata.validate_parents(path)
                    return self.metadata.stat(path)
                actual=retry_call(checked,wait=self.control.wait,
                    notify=lambda **event:self.emit(metadata_retry=True,**event),phase='RESUMING_FILES')
            except BackupError as error:
                if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                    self.skip_unsafe(path,error);continue
                if error.code in {'SOURCE_CHANGED','TRANSFER_DENIED','REMOTE_DIRECTORY_UNAVAILABLE'}:continue
                raise
            if actual!=expected:
                if not self.checkpoint.refresh_changed_sources or actual.kind!='file':continue
                if actual.name!=expected.name:
                    error=BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='SOURCE_NAME_CHANGED',
                                      unsafe_path=display_path(path),expected_name=display_path(expected.name),actual_name=display_path(actual.name))
                    if not self.continue_on_error:raise error
                    self.skip_unsafe(path,error);continue
                if actual.size<0 or actual.size>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
                self.walk.update_entry(path,actual)
                self.emit(stage='SOURCE_REFRESH',log=True,current_file=path)
            pending_bytes+=actual.size
            if pending_bytes>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
            self.enqueue(path,actual)
            self.resumed_files+=1
            self.emit(stage='RESUMING_FILES')

    def scan(self):
        pending, seen = [("", 0)], {}
        while pending:
            self.control.checkpoint()
            parent, depth = pending.pop()
            if depth > 64:
                raise BackupError("TRANSFER_LIMIT_EXCEEDED")
            self.emit(stage="SCANNING", log=True, directory=parent or "/")
            try:
                def listing():
                    if parent:self.metadata.validate_parents(parent+'/.listing')
                    return self.metadata.listdir(parent)
                for attempt in range(3):
                    try:
                        entries,cached = self.walk.listing(parent,listing)
                        break
                    except BackupError as changed:
                        if changed.code!='SOURCE_CHANGED' or not self.checkpoint.refresh_changed_sources:raise
                        self.walk.invalidate_directory(parent)
                        self.metadata.checked_parents.discard(parent)
                        if attempt==2:raise
                        self.emit(stage='SOURCE_REFRESH',log=True,directory=parent or '/')
                if cached:self.cached_directories+=1
            except BackupError as error:
                self.emit(stage="DIRECTORY_ERROR", log=True, directory=parent or "/",
                          error_code=error.code, **error.details)
                if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                    self.skip_unsafe(parent or '/',error,'dir');continue
                isolated = error.code=='REMOTE_DIRECTORY_UNAVAILABLE' or (
                    error.code=='SOURCE_CHANGED' and self.checkpoint.refresh_changed_sources)
                if not parent or not self.continue_on_error or not isolated:
                    raise
                self.incomplete = True
                self.errors.append({"path": parent, "kind": "dir", "error_code": error.code, **error.details})
                continue
            self.directories_seen += 1
            for entry in entries:
                self.control.checkpoint()
                try:
                    issue = name_compatibility_issue(entry.name)
                    if issue and not self.skip_names:
                        raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason=issue,unsafe_name=display_path(entry.name))
                except BackupError as error:
                    if error.code!='REMOTE_PATH_UNSAFE' or not self.continue_on_error:raise
                    self.skip_unsafe(parent+('/' if parent else '')+(entry.name if isinstance(entry.name,str) else '<invalid name>'),error,entry.kind)
                    continue
                path = posixpath.join(parent, entry.name)
                try:storage_key = (path if issue else self.checkpoint.storage_path(path)).casefold()
                except BackupError as error:
                    if error.code!='REMOTE_PATH_UNSAFE' or not self.continue_on_error:raise
                    self.skip_unsafe(path,error,entry.kind);continue
                if storage_key in seen:
                    peer, previous = seen[storage_key]
                    if not self.continue_on_error:
                        raise BackupError("REMOTE_PATH_COLLISION", path=path, other_path=peer)
                    if entry.kind == previous.kind == 'file':
                        self.planned_files += 1
                        self.planned_bytes += entry.size
                        if entry.size < 0 or self.planned_bytes > self.max_bytes:
                            raise BackupError('TRANSFER_LIMIT_EXCEEDED')
                        self.collisions.append((path, entry, peer))
                        if len(self.collisions) + len(seen) > self.max_files:
                            raise BackupError('TRANSFER_LIMIT_EXCEEDED')
                        continue
                    else:
                        from .file_conflicts import directory_conflict
                        try:
                            directory_conflict(self.checkpoint,path,entry,peer,previous)
                        except BackupError as error:
                            if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                                self.skip_unsafe(path,error,entry.kind);continue
                            if error.code not in {'FILE_CONFLICT_PENDING','FILE_CONFLICT_SKIPPED'}:
                                raise
                            self.errors.append({'path':path,'error_code':error.code,**error.details})
                            self.emit(stage='FILE_CONFLICT',log=True,error_code=error.code,**error.details)
                            continue
                seen[storage_key] = (path, entry)
                if len(seen) > self.max_files:
                    raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                if issue:
                    self.walk.omit(path,'REMOTE_NAME_UNSUPPORTED')
                    displayed_path = display_remote_path(path)
                    omission = {"path": displayed_path, "kind": entry.kind if entry.kind in {'file', 'dir'} else 'special', "reason": issue, "error_code": "REMOTE_NAME_UNSUPPORTED"}
                    self.skipped.append(omission)
                    self.errors.append(dict(omission))
                    self.emit(stage="FILE_SKIPPED", log=True, skipped_count=len(self.skipped),
                              skipped_path=displayed_path, skipped_kind=omission['kind'], skip_reason=issue,
                              error_code="REMOTE_NAME_UNSUPPORTED")
                    continue
                relative_path(path)
                special = unsupported_type(entry.kind)
                if self.skip_types and special:
                    self.walk.omit(path,'REMOTE_TYPE_UNSUPPORTED')
                    omission = {"path": display_remote_path(path), "kind": "special",
                                "reason": special, "error_code": "REMOTE_TYPE_UNSUPPORTED"}
                    self.skipped.append(omission)
                    self.errors.append(dict(omission))
                    self.emit(stage="FILE_SKIPPED", log=True, skipped_count=len(self.skipped),
                              skipped_path=omission['path'], skipped_kind="special",
                              skip_reason=special, error_code="REMOTE_TYPE_UNSUPPORTED")
                    continue
                if entry.kind == "dir":
                    if not cached:self.metadata.checked_parents.add(path)
                    try:
                        self.checkpoint.prepare_directory(path,entry)
                    except BackupError as error:
                        if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                            self.skip_unsafe(path,error,'dir');continue
                        if error.code not in {'FILE_CONFLICT_PENDING','FILE_CONFLICT_SKIPPED'}:
                            raise
                        self.errors.append({'path':path,'error_code':error.code,**error.details})
                        self.emit(stage='FILE_CONFLICT',log=True,error_code=error.code,**error.details)
                        continue
                    self.directories.append(self.checkpoint.storage_path(path))
                    pending.append((path, depth + 1))
                elif entry.kind == "file":
                    self.planned_files += 1
                    self.planned_bytes += entry.size
                    if entry.size < 0 or self.planned_bytes > self.max_bytes:
                        raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                    if path in self.enqueued_paths:continue
                    with self.checkpoint.mutex:
                        saved = self.checkpoint.db.execute('SELECT size,modified FROM files WHERE path=?',(path,)).fetchone()
                    if (self.committed.get(path, (None, 0))[0] == 'done'
                            and saved and saved['size']==entry.size and saved['modified']==entry.modified):
                        self.verify_queue.put_nowait((1, path, entry))
                        continue
                    self.enqueue(path,entry)
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
                check_source, path, entry = self.verify_queue.get(timeout=0.1)
            except queue.Empty:
                if self.scanned.is_set() and self.downloads_done.is_set():
                    return
                continue
            item = None
            verified_signature = None
            try:
                # Local filesystem/signature work belongs to the verifier, not
                # the source-discovery thread feeding the transfer queue.
                while not self.budget.verification_slot.acquire(timeout=.1):self.control.checkpoint()
                try:receipt=self.walk.receipt(path,entry)
                finally:self.budget.verification_slot.release()
                if receipt:
                    with self.mutex:
                        self.items.append(receipt)
                    continue
                self.emit(stage='FILE_VERIFY', log=True)
                def once():
                    nonlocal item,verified_signature
                    if check_source:
                        self.metadata.validate_parents(path,low_priority=True)
                        if self.metadata.stat(path,low_priority=True)!=entry:
                            raise BackupError('SOURCE_CHANGED')
                    while not self.budget.verification_slot.acquire(timeout=0.1):
                        self.control.checkpoint()
                    with self.mutex:
                        self.verifying += 1
                    try:
                        self.emit()
                        verified_signature=self.walk.signature(path)
                        item = self.checkpoint.verify_done(path, entry, self.control,
                                                          require_modified=bool(check_source))
                    finally:
                        with self.mutex:
                            self.verifying -= 1
                        self.budget.verification_slot.release()
                    if check_source and self.metadata.stat(path, low_priority=True) != entry:
                        raise BackupError('SOURCE_CHANGED')
                retry_call(once, wait=self.control.wait,
                           notify=lambda **event: self.emit(verification_retry=True, **event),
                           phase='FILE_VERIFY')
                with self.mutex:
                    self.walk.verified(path,entry,item,verified_signature)
                    self.items.append(item)
            except BackupError as error:
                self.emit(stage='FILE_ERROR', log=True, current_file=path, error_code=error.code, **error.details)
                if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                    self.skip_unsafe(path,error);continue
                if self.checkpoint.resolve_conflicts and error.code in {'LOCAL_FILE_CHANGED','SOURCE_CHANGED'}:
                    self.local_conflicts.append((path, entry))
                    continue
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

    def refresh_entry(self,path,previous):
        self.control.checkpoint()
        current=retry_call(lambda:self.metadata.stat(path),wait=self.control.wait,
                           notify=lambda **event:self.emit(metadata_retry=True,**event),phase='SOURCE_REFRESH')
        if current.kind!='file':raise BackupError('SOURCE_CHANGED')
        if current.size<0 or current.size>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        if current!=previous:
            with self.mutex:
                delta=current.size-previous.size
                if self.planned_bytes+max(0,delta)>self.max_bytes:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
                self.planned_bytes=max(0,self.planned_bytes+delta)
                for active in self.active.values():
                    if active['path']==path:active['size_bytes']=current.size
                self.walk.update_entry(path,current)
            self.emit(stage='SOURCE_REFRESH',log=True,current_file=path)
        return current

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
                    if source is not None and self.budget.contended():
                        stack.close()
                        source = None
                    if self.scanned.is_set():
                        return
                    continue
                if not self.walk.claim(path):
                    self.files_queue.task_done()
                    continue
                if source is None:
                    stack.enter_context(self.budget.lease(self.control,role='data'))
                    source = stack.enter_context(self.download_factory(credentials, site, self.metadata, self.control))
                worker = threading.get_ident()
                with self.mutex:
                    self.active[worker] = {"path": path, "size_bytes": entry.size, "received_bytes": 0}
                is_verified_copy = False  # Saved unchanged files are handled only by verify_queue.
                self.emit(stage="FILE_VERIFY" if is_verified_copy else "FILE_DOWNLOAD", log=True, current_file=path)
                final_event = {}
                try:
                    local = self.checkpoint.file_path(path)
                    def once():
                        self.control.checkpoint()
                        self.metadata.validate_parents(path)
                        saved_local = local.exists()
                        if saved_local and source.stat(path) != entry:
                            raise BackupError('SOURCE_CHANGED')
                        if shutil.disk_usage(self.snapshot).free < entry.size + 1024 * 1024:
                            raise BackupError("STORAGE_SPACE_LOW")
                        self.checkpoint.fetch(source, path, entry, local, self.control.deadline)
                        if saved_local and source.stat(path) != entry:
                            raise BackupError('SOURCE_CHANGED')
                    for refresh in range(3):
                        try:
                            retry_call(once, reconnect=getattr(source, "reconnect", None), wait=self.control.wait,
                                       notify=lambda **event: self.emit(current_file=path, **event),
                                       progress=lambda: self.checkpoint.committed_offset(path), phase="FILES_DOWNLOAD")
                            break
                        except BackupError as changed:
                            if changed.code!='SOURCE_CHANGED' or not self.checkpoint.refresh_changed_sources or refresh==2:raise
                            entry=self.refresh_entry(path,entry)
                    # Source was checked by fetch; verify the written artifact separately.
                    self.walk.transferred(path)
                    self.verify_queue.put_nowait((0, path, entry))
                except BackupError as error:
                    self.walk.failed(path,error.code)
                    self.emit(stage="FILE_ERROR", log=True, current_file=path, error_code=error.code, **error.details)
                    if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                        self.skip_unsafe(path,error);continue
                    if not self.continue_on_error or error.code not in {
                        "SOURCE_CHANGED", "TRANSFER_DENIED", "LOCAL_FILE_CHANGED", "LOCAL_PROGRESS_INVALID",
                        "REMOTE_DIRECTORY_UNAVAILABLE", 'FILE_CONFLICT_PENDING', 'FILE_CONFLICT_SKIPPED',
                    }:
                        if error.code in TRANSIENT_ERRORS:
                            final_event = {"stage": "RETRY_EXHAUSTED", "log": True,
                                           "current_file": path, **error.details,
                                           "error_code": error.code, "retry_attempt": 5, "retry_limit": 5}
                        raise
                    with self.mutex:
                        self.errors.append({"path": path, "error_code": error.code, **error.details})
                    if error.code in {'FILE_CONFLICT_PENDING','FILE_CONFLICT_SKIPPED'}:
                        self.emit(stage='FILE_CONFLICT',log=True,error_code=error.code,**error.details)
                finally:
                    with self.mutex:
                        self.active.pop(worker, None)
                    self.files_queue.task_done()
                    self.emit(**final_event)

    def reconcile_conflicts(self, credentials, site):
        from .file_conflicts import reconcile
        with self.budget.lease(self.control,role='data'), self.download_factory(credentials, site, self.metadata, self.control) as source:
            for path, entry in self.local_conflicts:
                self.control.checkpoint()
                try:
                    if self.checkpoint.refresh_changed_sources:entry=self.refresh_entry(path,entry)
                    if source.stat(path) != entry:
                        raise BackupError('SOURCE_CHANGED')
                    self.checkpoint.fetch(source,path,entry,None,self.control.deadline)
                    if source.stat(path) != entry:
                        raise BackupError('SOURCE_CHANGED')
                    self.items.append(self.checkpoint.verify_done(path,entry,self.control))
                except BackupError as error:
                    if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                        self.skip_unsafe(path,error);continue
                    if error.code not in {'FILE_CONFLICT_PENDING','FILE_CONFLICT_SKIPPED','SOURCE_CHANGED','LOCAL_FILE_CHANGED'}:
                        raise
                    self.errors.append({'path':path,'error_code':error.code,**error.details})
                    self.emit(stage='FILE_CONFLICT',log=True,error_code=error.code,**error.details)
            for path, entry, peer in self.collisions:
                self.control.checkpoint()
                try:
                    outcome = reconcile(self.checkpoint,source,path,entry,self.control.deadline,peer=peer)
                    if outcome['action']=='identical':
                        self.duplicates.append({'source_path':path,'stored_path':self.checkpoint.storage_path(peer),
                                                'sha256':outcome['conflict']['remote_sha256']})
                    else:
                        self.items.append(self.checkpoint.verify_done(path,entry,self.control))
                    self.emit(stage='FILE_RECONCILED',log=True,**outcome)
                except BackupError as error:
                    if error.code=='REMOTE_PATH_UNSAFE' and self.continue_on_error:
                        self.skip_unsafe(path,error);continue
                    if error.code not in {'FILE_CONFLICT_PENDING','FILE_CONFLICT_SKIPPED','SOURCE_CHANGED','LOCAL_FILE_CHANGED'}:
                        raise
                    self.errors.append({'path':path,'error_code':error.code,**error.details})
                    self.emit(stage='FILE_CONFLICT',log=True,error_code=error.code,**error.details)
