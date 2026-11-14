"""Non-destructive reconciliation of private file-name conflicts."""
import hashlib
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timezone

from .errors import BackupError
from .storage import sha256_file


def modified_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        if len(value.split('.')[0]) == 14 and value.split('.')[0].isdigit():
            return datetime.strptime(value.split('.')[0], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).timestamp()
        if value.replace('.', '', 1).isdigit() and 9 <= len(value.split('.')[0]) <= 19:
            return float(value) / (10**9 if len(value.split('.')[0]) > 12 else 1)
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() if 'T' in value else None
    except ValueError:
        return None


def conflict_key(scope, remote, local):
    return hashlib.sha256(json.dumps([scope, remote, local], ensure_ascii=True).encode()).hexdigest()


def directory_conflict(checkpoint, path, entry, peer, previous):
    key = conflict_key(checkpoint.scope, path, json.dumps([peer,entry.kind,entry.modified,previous.kind,previous.modified]))
    details = {'id':key,'remote_path':path,'local_path':peer,'remote_kind':entry.kind,
               'local_kind':previous.kind,'remote_size':entry.size,'local_size':previous.size,
               'remote_modified':entry.modified,'local_modified':previous.modified,
               'reason':'TYPE_OR_DIRECTORY_COLLISION'}
    decision = checkpoint.conflict_decision(key)
    if decision == 'keep_both':
        checkpoint.set_location(path,key)
        return
    raise BackupError('FILE_CONFLICT_SKIPPED' if decision == 'keep_local' else 'FILE_CONFLICT_PENDING', conflict=details)


def reconcile(checkpoint, source, remote, entry, deadline, *, peer=None):
    """Stage and verify the server version before comparing or replacing anything."""
    from .downloads import DirectoryCheckpoint, local_path
    peer = peer or remote
    local = checkpoint.file_path(peer)
    key = conflict_key(checkpoint.scope, remote, peer)
    details = {'id': key, 'remote_path': remote, 'local_path': peer,
               'remote_size': entry.size, 'remote_modified': entry.modified,
               'local_size': local.stat().st_size if local.is_file() else None,
               'same_content': False, 'reason': 'CASE_COLLISION' if peer != remote else 'LOCAL_CONTENT_DIFFERS'}
    if not local.is_file():
        details['reason'] = 'TYPE_OR_DIRECTORY_COLLISION'
        if local.is_dir():
            info=local.stat()
            details.update(local_kind='dir',remote_kind=entry.kind,
                           local_modified=datetime.fromtimestamp(info.st_mtime,timezone.utc).isoformat())
            key=conflict_key(checkpoint.scope,remote,peer+'\0dir\0'+str(info.st_mtime_ns)+'\0'+str(entry.modified))
            details['id']=key
            decision=checkpoint.conflict_decision(key)
            if decision=='keep_both':
                checkpoint.retain_tree(local)
                checkpoint.set_location(remote,key)
                checkpoint.fetch(source,remote,entry,None,deadline)
                return {'action':'kept_both','conflict':details}
            if decision=='keep_local':
                raise BackupError('FILE_CONFLICT_SKIPPED',conflict=details)
        raise BackupError('FILE_CONFLICT_PENDING', conflict=details)
    initial = local.stat()
    with checkpoint.mutex:
        previous = checkpoint.db.execute('SELECT * FROM files WHERE path=?', (peer,)).fetchone()
    details['local_modified'] = previous['modified'] if previous else datetime.fromtimestamp(initial.st_mtime, timezone.utc).isoformat()
    signature = hashlib.sha256(json.dumps([entry.size, entry.modified]).encode()).hexdigest()[:24]
    candidate_root = local_path(checkpoint.root, '.jino-progress/conflicts/' + key + '/' + signature)

    class CandidateSource:
        def stat(self, path): return source.stat(remote)
        def download(self, path, writer, offset): return source.download(remote, writer, offset)

    with DirectoryCheckpoint(candidate_root, key + signature) as staged:
        staged.fetch(CandidateSource(), 'candidate', entry, None, deadline)
        candidate = staged.file_path('candidate')
        remote_hash = sha256_file(candidate)
        local_hash = sha256_file(local)
        if source.stat(remote) != entry:
            raise BackupError('SOURCE_CHANGED')
        current = local.stat()
        if (current.st_size, current.st_mtime_ns) != (initial.st_size, initial.st_mtime_ns):
            raise BackupError('LOCAL_FILE_CHANGED')
        details.update(remote_sha256=remote_hash, local_sha256=local_hash, same_content=remote_hash == local_hash)
        key = conflict_key(checkpoint.scope, remote, peer + '\0' + local_hash + '\0' + remote_hash
                           + '\0' + str(entry.modified))
        details['id'] = key
        decision = checkpoint.conflict_decision(key)
        if remote_hash == local_hash:
            if peer == remote:
                checkpoint.record(remote, entry, 'done', entry.size, remote_hash)
            return {'action': 'identical', 'conflict': details}
        owned = bool(previous and previous['state'] == 'done' and previous['sha256'] == local_hash)
        remote_at = modified_timestamp(entry.modified)
        local_at = modified_timestamp(previous['modified']) if owned else None
        newer = peer == remote and remote_at is not None and local_at is not None and remote_at > local_at
        details['remote_newer'] = newer
        if decision not in {'keep_both', 'use_remote', 'keep_local'} and not newer:
            raise BackupError('FILE_CONFLICT_PENDING', conflict=details)
        if decision == 'keep_local':
            raise BackupError('FILE_CONFLICT_SKIPPED', conflict=details)
        if peer != remote and decision == 'use_remote':
            # Distinct remote paths cannot replace each other's verified artifacts.
            decision = 'keep_both'
        if decision == 'keep_both':
            if peer == remote:
                checkpoint.retain_local(checkpoint.storage_path(peer),initial.st_size,local_hash)
            checkpoint.set_location(remote, key)
            target = checkpoint.file_path(remote)
            if target.exists() and sha256_file(target) != remote_hash:
                raise BackupError('LOCAL_FILE_CHANGED')
        else:
            preserved = local_path(checkpoint.root, '.jino-progress/preserved/' + local_hash)
            preserved.parent.mkdir(parents=True, exist_ok=True)
            if preserved.exists():
                if sha256_file(preserved) != local_hash:
                    raise BackupError('LOCAL_FILE_CHANGED')
            else:
                shutil.copy2(local, preserved)
                if sha256_file(preserved) != local_hash:
                    raise BackupError('LOCAL_FILE_CHANGED')
            target = local
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = local_path(checkpoint.root, '.jino-progress/apply-' + uuid.uuid4().hex)
        with candidate.open('rb') as incoming, temporary.open('xb') as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024*1024)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if time.monotonic() > deadline:
            raise BackupError('TRANSFER_TIMEOUT')
        if sha256_file(local) != local_hash:
            raise BackupError('LOCAL_FILE_CHANGED')
        os.replace(temporary, target)
        checkpoint.record(remote, entry, 'done', entry.size, remote_hash)
        return {'action': 'kept_both' if decision == 'keep_both' else 'updated', 'conflict': details}
