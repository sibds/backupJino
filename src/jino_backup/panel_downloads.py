"""Read-only account tree downloads over Jino's observed HTTPS file transport."""

import hashlib
import http.client
import json
import posixpath
from contextlib import ExitStack
from datetime import date,datetime,timezone
from pathlib import Path

from .downloads import DirectoryCheckpoint,FileLock,local_path
from .errors import BackupError
from .importers import identity
from .server_folders import server_directory
from .state import write_json
from .storage import CHUNK,sha256_file
from .transfer import Entry,collect_files,relative_path,safe_name,name_compatibility_issue


def canonical_account_root(home,requested):
    if isinstance(home,str) and home.startswith('/') and '..' not in home.split('/'):
        return home
    if requested=='/':
        return '/'  # The observed, objectId-bound virtual account root, not a server OS root.
    raise BackupError('REMOTE_ROOT_UNVERIFIED')


def latest_mysql_snapshot(rows):
    from .panel import MONTHS,select_snapshot_row
    import re
    dates=[]
    for row in rows:
        if 'MySQL' not in row.get('kinds',''):
            continue
        match=re.fullmatch(r'Копия от (\d{1,2}) (\S+) (\d{4})',row.get('label','').strip())
        if not match or match[2] not in MONTHS:
            raise BackupError('UI_CHANGED')
        value=date(int(match[3]),MONTHS[match[2]],int(match[1])).isoformat()
        select_snapshot_row(rows,value)
        dates.append(value)
    return max(dates) if dates else None


class PanelFileSource:
    def __init__(self,panel,scope,*,skip_unsupported_names=False):
        if not scope.get('binding_verified'):
            raise BackupError('PROFILE_UNVERIFIED')
        self.panel,self.scope=panel,scope
        self.skip_unsupported_names=skip_unsupported_names
        self.paths={'':panel.root_file_token}
        self.directory_tokens={panel.root_file_token:''}
        self.links={}

    def __enter__(self):
        return self

    def __exit__(self,*args):
        pass  # The account runner owns the authenticated browser context.

    def reconnect(self):
        # Refresh the file channel in the existing authenticated session only.
        self.panel.prepare_file_transport(self.scope)

    @property
    def links_count(self):
        return len(self.links)

    def _safe_token(self,token):
        root=self.scope['canonical_root'].rstrip('/') or '/'
        if (not isinstance(token,str) or not token.startswith('/') or '..' in token.split('/')
                or root!='/' and token!=root and not token.startswith(root+'/')):
            raise BackupError('REMOTE_PATH_UNSAFE')
        return token

    def listdir(self,relative):
        relative_path(relative)
        if relative not in self.paths:
            raise BackupError('REMOTE_PATH_UNSAFE')
        result=[]
        for item in self.panel._list(self.paths[relative]):
            name=item.get('name')
            if self.skip_unsupported_names:
                name_compatibility_issue(name)
            else:
                safe_name(name)
            path=posixpath.join(relative,name)
            if item.get('targetType') is not None or item.get('targetDisplayPath') is not None:
                self.links[path]={'target_type':item.get('targetType'),'target':item.get('targetDisplayPath')}
                continue
            token=self._safe_token(item.get('path'))
            kind=item.get('type')
            if kind not in {'dir','file'}:
                raise BackupError('REMOTE_TYPE_UNSAFE')
            if kind=='dir':
                previous=self.directory_tokens.get(token)
                if previous is not None and previous!=path:
                    raise BackupError('REMOTE_PATH_UNSAFE')
                self.directory_tokens[token]=path
            self.paths[path]=token
            result.append(Entry(name,kind,item.get('size',0),
                                str(item['mtime']) if item.get('mtime') is not None else None))
        return result

    def stat(self,relative):
        parent,name=posixpath.split(relative_path(relative))
        matches=[x for x in self.listdir(parent) if x.name==name]
        if len(matches)!=1:
            raise BackupError('SOURCE_CHANGED')
        return matches[0]

    def download(self,relative,outgoing,offset):
        relative_path(relative)
        token=self._safe_token(self.paths[relative])
        try:
            incoming=None
            for attempt in range(2):
                try:
                    incoming=self.panel.transfer.open(token,offset=offset)
                    break
                except BackupError as exc:
                    if exc.code=='FILETRANSFER_HTTP_FAILED' and exc.details.get('http_status') in {401,403}:
                        if attempt==0:
                            self.panel.prepare_file_transport(self.scope)
                            continue
                        raise BackupError('FILETRANSFER_AUTH_EXPIRED') from None
                    raise
            with incoming:
                if offset and incoming.status==200:
                    # Some Jino transfer servers ignore Range. Compare the prefix before appending.
                    digest=hashlib.sha256()
                    remaining=offset
                    while remaining:
                        chunk=incoming.read(min(CHUNK,remaining))
                        if not chunk:
                            raise BackupError('TRANSFER_INTERRUPTED')
                        digest.update(chunk)
                        remaining-=len(chunk)
                    if not hasattr(outgoing,'digest') or digest.hexdigest()!=outgoing.digest.hexdigest():
                        raise BackupError('SOURCE_CHANGED')
                while True:
                    chunk=incoming.read(CHUNK)
                    if not chunk:
                        break
                    outgoing.write(chunk)
        except BackupError as exc:
            if exc.code=='FILETRANSFER_CONNECTION_FAILED':
                raise BackupError('TRANSFER_INTERRUPTED') from None
            if exc.code=='FILETRANSFER_HTTP_FAILED':
                raise BackupError('TRANSFER_DENIED') from None
            raise
        except (OSError,TimeoutError,http.client.HTTPException):
            raise BackupError('TRANSFER_INTERRUPTED') from None


class PanelFileJob:
    def __init__(self,destination,server_id,folder_name,batch_id,source_factory,*,
                 notify=None,max_bytes=1024**4,max_files=1000000,timeout_seconds=86400,state_dir=None,
                 skip_unsupported_names=False):
        self.destination=Path(destination).absolute()
        self.server_id,self.folder_name=server_id,folder_name
        self.job_id=identity('download',batch_id,'https-panel')
        self.scope=identity('scope',server_id,'https-panel','/')
        self.source_factory=source_factory
        self.notify=notify or (lambda value:None)
        self.max_bytes,self.max_files,self.timeout_seconds=max_bytes,max_files,timeout_seconds
        self.state_dir=Path(state_dir) if state_dir else None
        self.skip_unsupported_names=skip_unsupported_names

    def run(self):
        with ExitStack() as stack:
            if self.state_dir:
                account=self.server_id.replace('server-','panel-',1)
                stack.enter_context(FileLock(self.state_dir/'panel_jobs'/(account+'.lock')))
            stack.enter_context(FileLock(local_path(self.destination,'.jino-locks/'+self.server_id+'.lock')))
            server=server_directory(self.destination,self.server_id,self.folder_name)
            output=local_path(server,self.job_id)
            with DirectoryCheckpoint(output,self.scope,self.notify) as progress:
                manifest_path=local_path(output,'manifest.json')
                if manifest_path.exists():
                    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
                    if manifest.get('scope')!=self.scope:
                        raise BackupError('DOWNLOAD_SCOPE_CHANGED')
                    if manifest.get('status')=='FILES_ONLY':
                        for item in manifest['files']['items']:
                            file=local_path(output,'files/'+item['path'])
                            if file.stat().st_size!=item['size_bytes'] or sha256_file(file)!=item['sha256']:
                                raise BackupError('LOCAL_FILE_CHANGED')
                        progress.validate_inventory(manifest['files']['items'])
                        return self._result(manifest,True)
                started=datetime.now(timezone.utc).isoformat()
                with self.source_factory() as source:
                    files=collect_files(source,output/'files',max_bytes=self.max_bytes,max_files=self.max_files,
                        timeout_seconds=self.timeout_seconds,checkpoint=progress,continue_on_error=True,
                        skip_unsupported_names=self.skip_unsupported_names)
                    links=source.links
                    binding=source.scope.get('container_ref')
                manifest={'schema_version':'1.0','scope':self.scope,'job_id':self.job_id,
                    'site_id':self.server_id,'status':'PARTIAL' if files.get('errors') else 'FILES_ONLY',
                    'capture_method':'https_panel','capture_consistency':'BEST_EFFORT',
                    'capture_started_at':started,'capture_finished_at':datetime.now(timezone.utc).isoformat(),
                    'container_ref':binding,'files':files,'links':links,'databases':[],
                    'encrypted':False,'skip_unsupported_names':self.skip_unsupported_names,
                    'warnings':['DB_NOT_INCLUDED','LIVE_FILES_BEST_EFFORT']+
                        (['SYMLINKS_RECORDED_WITHOUT_FOLLOWING'] if links else [])}
                write_json(manifest_path,manifest)
                return self._result(manifest,False)

    def _result(self,manifest,already):
        return {'status':manifest['status'],'job_id':self.job_id,'server_id':self.server_id,
                'files_count':manifest['files']['count'],'files_bytes':manifest['files']['bytes'],
                'file_errors_count':len(manifest['files'].get('errors',[])),
                'files_skipped_count':len(manifest['files'].get('skipped',[])),
                'links_count':len(manifest.get('links',{})),'already_downloaded':already,
                'backup_complete':False,'capture_consistency':'BEST_EFFORT'}
