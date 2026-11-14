"""Bounded account batch: inspect, reuse/export database snapshots, download live files."""

import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone,date
from pathlib import Path

from cryptography.fernet import Fernet

from .downloads import FileLock,local_path
from .errors import BackupError
from .importers import identity,jino_rows,parse_jino
from .panel import JinoPanelBrowser,select_panel_source
from .panel_downloads import PanelFileJob,PanelFileSource
from .panel_export import PanelExportJob
from .panel_protocol import validate_panel_sql
from .state import Repository,Settings,private_dir,write_json
from .storage import sha256_file


def account_plan(path,mapping):
    rows=jino_rows(path)
    imported=parse_jino(path,mapping)
    available={c.profile_id for c in imported.candidates if c.kind=='panel'}
    selected=[]
    issues=[]
    for index,row in enumerate(rows,1):
        ref=identity('panel',row.get(mapping['id']))
        if ref in available:
            selected.append(select_panel_source(path,mapping,index))
        else:
            code=next((x['code'] for x in imported.report['issues'] if x['row']==index),'ACCOUNT_UNUSABLE')
            issues.append({'row':index,'error_code':code})
    return {'rows_total':len(rows),'accounts_ready':len(selected),'needs_review':len(issues),'issues':issues},selected


def completed_database_copy(state_dir,destination,account_ref,folder,scope,key,*,allow_subset=False):
    directory=Path(state_dir)/'panel_jobs'
    cipher=Fernet(key)
    for path in directory.glob('*.enc'):
        events=path.with_suffix('.events.jsonl')
        if events.exists():
            try:
                with events.open('rb') as stream:
                    first=json.loads(stream.readline(16384))
                if first.get('account_ref')!=account_ref:
                    continue
            except (OSError,ValueError):
                continue
        state=json.loads(cipher.decrypt(path.read_bytes()))
        binding=state.get('scope',{})
        if ((not allow_subset and state.get('phase')!='DB_ONLY') or binding.get('account_ref')!=account_ref
                or binding.get('snapshot_date')!=scope['snapshot_date'] or state.get('snapshot_ref')!=scope['snapshot_ref']):
            continue
        entries=state.get('databases',{})
        if allow_subset:
            entries = {alias:entry for alias,entry in entries.items() if entry.get('name') in set(scope['databases'])}
            if any(not entry.get('artifact') or entry['artifact'].get('validation')!='PASS' for entry in entries.values()):
                continue
        if {x.get('name') for x in entries.values()}!=set(scope['databases']):
            continue
        output=local_path(Path(destination),folder+'/'+path.stem)
        if str(output)!=binding.get('output'):
            continue
        artifacts=[]
        for entry in entries.values():
            artifact=entry.get('artifact')
            if not artifact:
                raise BackupError('INTEGRITY_FAILED')
            file=local_path(output,artifact['relative_path'])
            if not file.is_file() or file.stat().st_size!=artifact['size_bytes'] or sha256_file(file)!=artifact['sha256']:
                raise BackupError('DB_INTEGRITY_FAILED',repair_job_id=path.stem)
            try:
                warnings=validate_panel_sql(file) or []
            except BackupError as error:
                if error.code=='DB_INTEGRITY_FAILED':
                    raise BackupError('DB_INTEGRITY_FAILED',repair_job_id=path.stem) from None
                raise
            artifacts.append({**artifact,**({'warnings':warnings} if warnings else {})})
        warnings=sorted({w for a in artifacts for w in a.get('warnings',[])})
        return {'status':'DB_ONLY','job_id':path.stem,'databases_count':len(artifacts),
                'artifacts':artifacts,**({'warnings':warnings} if warnings else {}),
                'source_snapshot_date':scope['snapshot_date'],'reused_existing':True}
    return None


def safe_failure(exc):
    code=exc.code if isinstance(exc,BackupError) and re.fullmatch('[A-Z][A-Z0-9_]*',exc.code) else 'INTERNAL_ERROR'
    return {'status':'FAILED','error_code':code,'exception_type':type(exc).__name__,'backup_complete':False}


def run_workers(rows,worker,report_path,*,workers=2,notify=None,metadata=None):
    if not 1<=workers<=2:
        raise BackupError('LIMIT_INVALID')
    notify=notify or (lambda event:None)
    report_path=Path(report_path)
    private_dir(report_path.parent)
    lock=threading.Lock()
    items=[{'row':row,'status':'QUEUED','backup_complete':False} for row in rows]
    report={'schema_version':'1.0',**(metadata or {}),'started_at':datetime.now(timezone.utc).isoformat(),
            'finished':False,'servers':items}
    def update(item,**values):
        with lock:
            item.update(values)
            write_json(report_path,report)
            notify({'time':datetime.now(timezone.utc).isoformat(),'row':item['row'],**values})
    def task(item):
        update(item,status='RUNNING',stage='AUTHENTICATE')
        transient={'PANEL_RESPONSE_FAILED','PANEL_BROWSER_OR_LOGIN_FAILED','FILETRANSFER_AUTH_EXPIRED',
                   'FILETRANSFER_CONNECTION_FAILED','CONNECTION_FAILED','TRANSFER_INTERRUPTED'}
        for attempt in range(3):
            try:
                result=worker(item['row'],lambda **values:update(item,**values))
            except Exception as exc:
                result=safe_failure(exc)
            code=result.get('error_code') or result.get('files',{}).get('error_code')
            if code not in transient or attempt==2:
                break
            update(item,stage='RECONNECT',account_attempt=attempt+2)
            time.sleep(2**attempt)
        update(item,**result,stage='FINISHED')
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures=[executor.submit(task,item) for item in items]
        for future in as_completed(futures):
            future.result()
    report['finished']=True
    report['finished_at']=datetime.now(timezone.utc).isoformat()
    write_json(report_path,report)
    return report


class BorrowedPanel:
    def __init__(self,panel,scope):
        self.panel,self.scope=panel,scope
    def __enter__(self):
        return self
    def __exit__(self,*args):
        pass
    def inspect(self,snapshot_date):
        if snapshot_date!=self.scope['snapshot_date']:
            raise BackupError('SNAPSHOT_MISMATCH')
        return self.scope
    def __getattr__(self,name):
        return getattr(self.panel,name)


class AccountBatch:
    def __init__(self,state_dir,destination,source,mapping,batch_id,*,workers=2,inspect_only=False,
                 timeout_seconds=86400,max_bytes=1024**4,max_files=1000000,notify=None):
        if not isinstance(batch_id,str) or not 1<=len(batch_id)<=160:
            raise BackupError('IDEMPOTENCY_KEY_INVALID')
        if not (1<=workers<=2 and 1<=timeout_seconds<=86400 and 1<=max_bytes<=1024**4 and 1<=max_files<=1000000):
            raise BackupError('LIMIT_INVALID')
        self.settings=Settings.load(Path(state_dir))
        self.destination=Path(destination).absolute()
        self.batch_id=batch_id
        self.report_plan,self.selected=account_plan(source,mapping)
        self.workers,self.inspect_only=workers,inspect_only
        self.timeout_seconds,self.max_bytes,self.max_files=timeout_seconds,max_bytes,max_files
        self.notify=notify
        self.report_path=self.destination/(identity('jino-all',batch_id)+'.json')

    def run(self):
        repo=Repository(self.settings.state_dir/'registry.sqlite3',self.settings.read_key())
        for selected in self.selected:
            repo.import_profiles(selected.candidates)
        by_row={x.source_row:x for x in self.selected}
        def worker(row,update):
            selected=by_row[row]
            with FileLock(self.settings.state_dir/'account_batches'/(selected.account_ref+'.lock')):
                with JinoPanelBrowser(repo.credentials(selected.account_ref)) as panel:
                    panel.inspect_files()
                    update(stage='ACCESS_VERIFIED')
                    try:
                        latest=panel.latest_mysql_snapshot()
                    except Exception as exc:
                        latest=None
                        db=safe_failure(exc)
                    else:
                        db={'status':'MISSING','error_code':'SNAPSHOT_NOT_FOUND'}
                    if self.inspect_only:
                        return {'status':'ACCESSIBLE','latest_mysql_snapshot':latest,'backup_complete':False}
                    if latest:
                        update(stage='DATABASES',source_snapshot_date=latest)
                        try:
                            scope=panel.inspect(latest)
                            db=completed_database_copy(self.settings.state_dir,self.destination,selected.account_ref,
                                selected.folder_name,scope,self.settings.read_key())
                            if db is None:
                                db=PanelExportJob(self.settings.state_dir,self.destination,selected.account_ref,
                                    self.batch_id+'-'+latest,latest,self.settings.read_key(),
                                    lambda:BorrowedPanel(panel,scope),server_folder=selected.folder_name,
                                    timeout_seconds=300,max_bytes=self.max_bytes,
                                    notify=lambda event:update(stage='DATABASES',db_stage=event['stage'])).run()
                            db['snapshot_stale']=date.fromisoformat(latest)<datetime.now(timezone.utc).date()
                        except Exception as exc:
                            db=safe_failure(exc)
                        update(database_status=db['status'],databases_count=db.get('databases_count',0))
                    update(stage='FILES_DOWNLOAD')
                    def source_factory():
                        return PanelFileSource(panel,panel.inspect_files())
                    try:
                        files=PanelFileJob(self.destination,selected.account_ref.replace('panel-','server-',1),
                            selected.folder_name,self.batch_id,source_factory,state_dir=self.settings.state_dir,
                            timeout_seconds=self.timeout_seconds,max_bytes=self.max_bytes,max_files=self.max_files,
                            notify=lambda event:update(stage='FILES_DOWNLOAD',**event)).run()
                    except Exception as exc:
                        files=safe_failure(exc)
                    status=('PARTIAL' if db.get('status')=='DB_ONLY' else 'FILES_ONLY') if files.get('status')=='FILES_ONLY' else (
                        'PARTIAL' if files.get('status')=='PARTIAL' else 'DB_ONLY' if db.get('status')=='DB_ONLY' else 'FAILED')
                    return {'status':status,'files':files,'databases':db,'backup_complete':False,
                            'capture_consistency':'BEST_EFFORT','application_restore_tested':False}
        with FileLock(local_path(self.destination,'.'+identity('jino-all',self.batch_id)+'.lock')):
            return run_workers(list(by_row),worker,self.report_path,workers=self.workers,notify=self.notify,
                               metadata={**self.report_plan,'batch_id':identity('jino-all',self.batch_id)})
