"""Journalled database-only export workflow; private state never enters public logs."""

import json
import os
import re
import time
import uuid
import shutil
from contextlib import ExitStack
from datetime import date, datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from .panel_protocol import validate_panel_sql as validate_sql
from .downloads import FileLock, local_path
from .errors import BackupError
from .importers import identity
from .state import private_dir, replace_with_retry, safe_id, write_json
from .storage import sha256_file
from .server_folders import server_work


def atomic_bytes(path, value):
    temporary = path.with_name(path.name + '.partial-' + uuid.uuid4().hex)
    try:
        with temporary.open('xb') as stream:
            os.chmod(temporary, 0o600)
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        replace_with_retry(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class PanelExportJob:
    def __init__(self, state_dir, destination, account_ref, idempotency_key,
                 snapshot_date, key, panel_factory, *, timeout_seconds=300,
                 max_bytes=10*1024**3, notify=None,cleanup_remote=False,server_folder=None,control=None,selected_aliases=None,
                 resume_job_id=None,existing_exports=None,defer_export_conflicts=False):
        try:
            if date.fromisoformat(snapshot_date).isoformat() != snapshot_date:
                raise ValueError
        except (TypeError, ValueError):
            raise BackupError('SNAPSHOT_DATE_INVALID') from None
        if not isinstance(idempotency_key,str) or not 1 <= len(idempotency_key) <= 160:
            raise BackupError('IDEMPOTENCY_KEY_INVALID')
        if not 1 <= timeout_seconds <= 86400 or not 1 <= max_bytes <= 1024**4:
            raise BackupError('LIMIT_INVALID')
        self.root = Path(state_dir).resolve() / 'panel_jobs'
        safe_id(account_ref)
        self.job_id = identity('panel-job', account_ref, idempotency_key)
        if resume_job_id is not None:
            if not isinstance(resume_job_id,str) or not re.fullmatch('panel-job-[a-f0-9]{24}',resume_job_id):
                raise BackupError('REQUEST_INVALID')
            self.job_id=resume_job_id
        self.destination=Path(destination).absolute()
        self.server_id=account_ref.replace('panel-','server-',1)
        self.server_folder=server_folder
        self.selected_aliases = sorted(selected_aliases) if selected_aliases is not None else None
        self.legacy_output=local_path(self.destination,self.server_id+'/'+self.job_id)
        self.output=local_path(self.destination,(server_folder or self.server_id)+'/'+self.job_id)
        self.account_ref, self.snapshot_date = account_ref, snapshot_date
        self.cipher, self.panel_factory = Fernet(key), panel_factory
        self.timeout_seconds, self.max_bytes = timeout_seconds, max_bytes
        self.notify = notify or (lambda event: None)
        self.cleanup_remote=cleanup_remote
        if existing_exports is not None and cleanup_remote:raise BackupError('REQUEST_INVALID')
        self.existing_exports=existing_exports
        self.defer_export_conflicts=defer_export_conflicts
        self.private_path = self.root / (self.job_id + '.enc')
        self.report_path = self.root / (self.job_id + '.json')
        self.event_path = self.root / (self.job_id + '.events.jsonl')
        self.state = None
        self.control = control or (lambda: None)

    def _save(self):
        atomic_bytes(self.private_path,self.cipher.encrypt(json.dumps(self.state).encode()))

    def _event(self, stage, **details):
        event = {'time':datetime.now(timezone.utc).isoformat(), 'job_id':self.job_id,
                 'account_ref':self.account_ref, 'stage':stage, **details}
        with self.event_path.open('a',encoding='utf-8') as stream:
            os.chmod(self.event_path,0o600)
            stream.write(json.dumps(event,ensure_ascii=True)+'\n')
            stream.flush()
            os.fsync(stream.fileno())
        self.notify(event)

    def _load(self):
        scope = {'account_ref':self.account_ref,'snapshot_date':self.snapshot_date,'output':str(self.output)}
        if self.selected_aliases is not None:
            scope['selected_db_aliases'] = self.selected_aliases
        if self.private_path.exists():
            try:
                self.state = json.loads(self.cipher.decrypt(self.private_path.read_bytes()))
            except (InvalidToken, ValueError):
                raise BackupError('PANEL_STATE_INTEGRITY_FAILED') from None
            if self.state.get('scope') != scope:
                legacy={**scope,'output':str(self.legacy_output)}
                if not self.server_folder or self.state.get('scope')!=legacy:
                    raise BackupError('IDEMPOTENCY_SCOPE_CHANGED')
                if self.output.exists():
                    marker=local_path(self.output,'.jino-panel-owner.json')
                    if not marker.is_file() or json.loads(marker.read_text(encoding='utf-8'))!={'job_id':self.job_id}:
                        raise BackupError('DESTINATION_NOT_OWNED')
                    for entry in self.state['databases'].values():
                        if entry.get('artifact'):
                            self._verify_local(entry['artifact'])
                elif self.state['phase']=='DB_ONLY':
                    raise BackupError('INTEGRITY_FAILED')
                self.state['scope']=scope
                self._save()
                self._event('OUTPUT_DIRECTORY_MIGRATED')
        else:
            self.state = {'scope':scope,'databases':{},'phase':'CREATED','snapshot_ref':None}
            self._save()

    def _public(self, status, **details):
        artifacts = [entry['artifact'] for entry in self.state['databases'].values()
                     if entry.get('artifact') and entry['artifact'].get('validation')=='PASS']
        warnings=sorted({warning for artifact in artifacts for warning in artifact.get('warnings',[])})
        return {'job_id':self.job_id,'account_ref':self.account_ref,'status':status,
                'test_passed':status=='DB_ONLY','source_snapshot_at':None,
                'source_snapshot_date':None if any(x.get('source_snapshot_verified') is False for x in artifacts) else self.snapshot_date,
                'snapshot_time_precision':'unknown' if any(x.get('source_snapshot_verified') is False for x in artifacts) else 'day',
                'databases_count':len(artifacts),'artifacts':artifacts,
                **({'warnings':warnings} if warnings else {}),
                'application_restore_tested':False,'files_included':False,
                'cleanup_policy':'DELETE_OWNED' if self.cleanup_remote else 'KEEP_REMOTE',
                'remote_exports_retained':sum(bool(x.get('remote')) for x in self.state['databases'].values()) if not self.cleanup_remote else 0,
                'cleanup_pending':self.cleanup_remote and any(x['phase'] in {'SUBMITTING','WAITING','DOWNLOADED'}
                                      for x in self.state['databases'].values()),**details}

    def _verify_local(self, artifact):
        file = local_path(self.output,artifact['relative_path'])
        if not file.is_file() or file.stat().st_size != artifact['size_bytes'] or sha256_file(file) != artifact['sha256']:
            raise BackupError('DB_INTEGRITY_FAILED')
        warnings=validate_sql(file) or []
        if warnings:
            artifact['warnings']=sorted(set(artifact.get('warnings',[])+warnings))
        return warnings

    def _prepare_redownload(self, entry):
        if self.cleanup_remote or not entry.get('remote'):
            raise BackupError('DB_INTEGRITY_FAILED')
        artifact=entry.pop('artifact')
        target=local_path(self.output,artifact['relative_path'])
        entry['invalid_artifact']=artifact
        entry['replacement_baseline']=({'size':target.stat().st_size,'sha256':sha256_file(target)}
                                       if target.is_file() else None)
        entry['replace_corrupt']=True
        entry['phase']='WAITING'
        self.state['phase']='CREATED'
        self._save()

    def _verify_or_prepare_redownload(self, entry):
        if entry['artifact'].get('validation')!='PASS':
            self._prepare_redownload(entry)
            return False
        try:
            self._verify_local(entry['artifact'])
            return True
        except BackupError as error:
            if error.code!='DB_INTEGRITY_FAILED': raise
            self._prepare_redownload(entry)
            return False

    def _prepare_output(self):
        marker = self.output / '.jino-panel-owner.json'
        if self.output.exists():
            if self.output.is_symlink() or not marker.is_file():
                raise BackupError('DESTINATION_NOT_OWNED')
            if json.loads(marker.read_text(encoding='utf-8')) != {'job_id':self.job_id}:
                raise BackupError('DESTINATION_NOT_OWNED')
        else:
            self.output.mkdir(parents=True,exist_ok=False,mode=0o700)
            write_json(marker,{'job_id':self.job_id})
        private_dir(local_path(self.output,'databases'))

    def run(self, inspect_only=False,preflight_only=False):
        private_dir(self.root)
        with ExitStack() as stack:
            stack.enter_context(FileLock(self.root / (self.account_ref + '.lock')))
            if self.server_folder:
                stack.enter_context(server_work(self.destination, self.server_id, self.server_folder, 'databases'))
            self._load()
            try:
                changed_selection=any(item.get('decision')!='keep_local' and item['id']!=self.state['databases'].get(alias,{}).get('imported_candidate_id')
                                      for alias,item in (self.existing_exports or {}).items())
                if self.state['phase']=='DB_ONLY' and not changed_selection:
                    all_valid=True
                    for entry in self.state['databases'].values():
                        if not self._verify_or_prepare_redownload(entry): all_valid=False
                    if all_valid:
                        result = self._public('DB_ONLY',already_finished=True,cleanup_verified=self.cleanup_remote)
                        write_json(self.report_path,result)
                        self._event('ALREADY_VERIFIED')
                        return result
                self._event('AUTHENTICATE')
                with self.panel_factory() as panel:
                    self._event('INSPECT_SNAPSHOT')
                    scope = panel.inspect(self.snapshot_date)
                    if not scope.get('binding_verified'):
                        raise BackupError('PROFILE_UNVERIFIED')
                    if scope.get('snapshot_date') != self.snapshot_date:
                        raise BackupError('SNAPSHOT_MISMATCH')
                    names = scope.get('databases')
                    if not isinstance(names,list) or not names or not all(isinstance(n,str) and n for n in names) or len(set(names)) != len(names):
                        raise BackupError('DATABASE_SCOPE_UNVERIFIED')
                    if self.selected_aliases is not None:
                        selected = set(self.selected_aliases)
                        available = {identity('db', self.account_ref, name):name for name in names}
                        if not selected or not selected <= available.keys():
                            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
                        names = [name for alias,name in available.items() if alias in selected]
                    if self.state['snapshot_ref'] and self.state['snapshot_ref'] != scope['snapshot_ref']:
                        raise BackupError('SNAPSHOT_MISMATCH')
                    self.state['snapshot_ref'] = scope['snapshot_ref']
                    self._save()
                    self._event('SNAPSHOT_SELECTED',databases_count=len(names),transport=scope['transport'])
                    if inspect_only:
                        result = self._public('INSPECTED',inspection_ok=True,
                                              snapshot_databases_count=len(names),transport=scope['transport'],
                                              capabilities=scope.get('capabilities',{}))
                        write_json(self.report_path,result)
                        return result
                    self._event('PREFLIGHT_TRANSPORT')
                    panel.preflight(scope)
                    if hasattr(panel,'restore_job'):
                        panel.restore_job(self.state['databases'])
                    self._event('TRANSPORT_READY')
                    if preflight_only:
                        result=self._public('TRANSPORT_VERIFIED',transport_verified=True,
                                            snapshot_databases_count=len(names),transport=scope['transport'])
                        write_json(self.report_path,result)
                        return result
                    self._prepare_output()
                    if any(x['phase']=='FAILED_CLEANED' for x in self.state['databases'].values()):
                        raise BackupError('PREVIOUS_EXPORT_FAILED')
                    aliases = {identity('db',self.account_ref,name):name for name in names}
                    if set(self.state['databases']) - set(aliases):
                        raise BackupError('DATABASE_SCOPE_CHANGED')
                    pending=[]
                    skipped=[]
                    for alias,name in aliases.items():
                        self.control()
                        entry = self.state['databases'].setdefault(alias,{'name':name,'phase':'NEW'})
                        selected=(self.existing_exports or {}).get(alias)
                        if selected and selected.get('decision')=='keep_local':
                            if entry.get('artifact'):self._verify_local(entry['artifact'])
                            else:skipped.append(alias)
                            continue
                        if selected and entry.get('imported_candidate_id')!=selected['id']:
                            panel.check_existing_export(scope,selected)
                            old=entry.pop('artifact',None)
                            if old:
                                self._verify_local(old)
                                entry.setdefault('previous_artifacts',[]).append(old)
                                if selected['decision']=='use_remote':
                                    entry['replacement_baseline']={'size':old['size_bytes'],'sha256':old['sha256']}
                                    entry['replace_existing']=True
                                    entry['selected_relative_path']=old['relative_path']
                            entry.update(remote=selected,phase='WAITING',imported_candidate_id=selected['id'],
                                         imported_existing=True)
                            self.state['phase']='CREATED'
                            if selected['decision']=='new_version':
                                entry['selected_relative_path']='versions/'+selected['id'][:24]+'/databases/'+alias+'.sql'
                            self._save()
                        if entry.get('phase') in {'CLEANED','RETAINED'}:
                            if self._verify_or_prepare_redownload(entry): continue
                        try:
                            if self.existing_exports is not None and not selected and not entry.get('artifact'):
                                raise BackupError('REMOTE_DESTINATION_EXISTS')
                            self._export_one(panel,scope,alias,entry)
                        except BackupError as error:
                            if not self.defer_export_conflicts or error.code!='REMOTE_DESTINATION_EXISTS':raise
                            pending.append(alias)
                    if pending:
                        result=self._public('NEEDS_ACTION',error_code='REMOTE_DESTINATION_EXISTS',pending_databases_count=len(pending))
                        write_json(self.report_path,result)
                        return result
                    if skipped:
                        result=self._public('PARTIAL',skipped_databases_count=len(skipped))
                        write_json(self.report_path,result)
                        return result
                    if (len(self.state['databases']) != len(names)
                            or any(x['phase'] not in {'CLEANED','RETAINED'} or not x.get('artifact') for x in self.state['databases'].values())):
                        raise BackupError('DATABASE_SCOPE_INCOMPLETE')
                self.state['phase']='DB_ONLY'
                self._save()
                result = self._public('DB_ONLY',cleanup_verified=self.cleanup_remote,transport=scope['transport'])
                write_json(local_path(self.output,'manifest.json'),{
                    'schema_version':'1.0',**result,'capture_consistency':'BEST_EFFORT' if result['source_snapshot_date'] is None else 'PROVIDER_SNAPSHOT',
                    'encrypted':False,'warnings':result.get('warnings',[])+['FILES_NOT_INCLUDED','RESTORE_NOT_TESTED']})
                write_json(self.report_path,result)
                self._event('DB_ONLY',databases_count=result['databases_count'])
                return result
            except KeyboardInterrupt:
                self._event('CANCELLED')
                result = self._public('CANCELLED',error_code='CANCELLED')
                write_json(self.report_path,result)
                raise
            except Exception as exc:
                code = exc.code if isinstance(exc,BackupError) and re.fullmatch('[A-Z][A-Z0-9_]*',exc.code) else 'INTERNAL_ERROR'
                if code=='UI_MUTATION_BLOCKED':
                    code='UI_CHANGED'
                diagnostics={'exception_type':type(exc).__name__}
                if isinstance(exc,BackupError):
                    steps={'OPEN_HOSTING_LIST','WAIT_CONTAINER_LINKS','OPEN_CONTAINER','OPEN_BACKUPS',
                           'SELECT_SNAPSHOT','VERIFY_EXPORT_DESTINATION','READ_SNAPSHOT_DATABASES',
                           'INSPECT_FILE_ACCESS','OPEN_FILE_MANAGER','PREFLIGHT_FILE_MANAGER',
                           'OPEN_SNAPSHOT_MENU','WAIT_SNAPSHOT_MENU','OPEN_DB_EXPORT_FORM'}
                    if exc.details.get('ui_step') in steps:
                        diagnostics['ui_step']=exc.details['ui_step']
                    if exc.details.get('exception_type') in {'TimeoutError','Error','ValueError','KeyError','TypeError','AttributeError'}:
                        diagnostics['exception_type']=exc.details['exception_type']
                    features=exc.details.get('features',{})
                    if isinstance(features,dict):
                        diagnostics['features']={k:v for k,v in features.items()
                                                 if k in {'container_links_count','container_path_depths','regular_file',
                                                          'sql_suffix','gzip_suffix','has_link_target','size_is_int','size_bytes'}
                                                 and (isinstance(v,int) or isinstance(v,list) and all(isinstance(n,int) for n in v))}
                self._event('FAILED',error_code=code,diagnostics=diagnostics)
                result = self._public('FAILED',error_code=code,diagnostics=diagnostics)
                write_json(self.report_path,result)
                return result

    def _export_one(self,panel,scope,alias,entry):
        remote = entry.get('remote')
        if not self.cleanup_remote:
            entry['download_attempt']=entry.get('download_attempt',0)+1
            self._save()
        suffix='' if self.cleanup_remote else '-'+str(entry['download_attempt'])
        relative=entry.get('selected_relative_path','databases/'+alias+'.sql')
        target=local_path(self.output,relative)
        private_dir(target.parent)
        partial=local_path(self.output,relative+'.partial'+suffix)
        try:
            if entry['phase']=='NEW':
                self.control()
                if panel.existing_export(scope,entry['name']) is not None:
                    raise BackupError('REMOTE_DESTINATION_EXISTS')
                if hasattr(panel,'capture_baseline'):
                    entry['baseline']=panel.capture_baseline(scope,entry['name'])
                entry['phase']='SUBMITTING'
                self._save()  # Persist BEFORE the mutation, including if the process dies in submit.
                self._event('EXPORT_SUBMITTING',db_alias=alias)
                try:
                    panel.submit_export(scope,entry['name'])
                except BackupError as exc:
                    if exc.code=='UI_MUTATION_BLOCKED':
                        entry['phase']='NEW'
                        self._save()
                        raise
                    if exc.code != 'PANEL_RESPONSE_LOST':
                        raise
                    self._event('EXPORT_RESPONSE_UNKNOWN',db_alias=alias)
            if entry['phase'] in {'SUBMITTING','WAITING','DOWNLOADED'}:
                self._event('EXPORT_WAIT',db_alias=alias)
                if entry.get('imported_existing'):
                    panel.check_existing_export(scope,remote)
                if remote is not None and not self.cleanup_remote and entry.get('download_attempt',0)>1 and not entry.get('artifact') and not entry.get('imported_existing'):
                    refreshed=panel.wait_export(scope,entry['name'],self.timeout_seconds)
                    if not refreshed or refreshed.get('path')!=remote.get('path'):
                        raise BackupError('EXPORT_RESULT_UNVERIFIED')
                    remote=refreshed
                remote = remote or panel.wait_export(scope,entry['name'],self.timeout_seconds)
                if remote is None:
                    raise BackupError('EXPORT_RESULT_UNKNOWN')
                entry['remote']=remote
                entry['phase']='WAITING'
                self._save()
                if entry.get('pending_artifact') and target.exists() and not entry.get('artifact'):
                    try:
                        self._verify_local(entry['pending_artifact'])
                    except BackupError as error:
                        if not entry.get('replace_corrupt') or error.code!='DB_INTEGRITY_FAILED': raise
                    else:
                        entry['artifact']=entry.pop('pending_artifact')
                        entry['phase']='DOWNLOADED'
                        self._save()
                        self._event('LOCAL_COMMIT_RECOVERED',db_alias=alias)
                if not entry.get('artifact'):
                    self.control()
                    self._event('DOWNLOAD',db_alias=alias)
                    replacing=entry.get('replace_corrupt') or entry.get('replace_existing')
                    if target.exists() and not replacing:
                        raise BackupError('LOCAL_DESTINATION_CONFLICT')
                    reusable=None
                    for candidate in sorted(partial.parent.glob(alias+'.sql.partial-*')):
                        if entry.get('imported_existing'):break
                        if candidate==partial or candidate.is_symlink() or candidate.stat().st_size!=remote.get('size_bytes'):
                            continue
                        try:
                            validate_sql(candidate)
                        except BackupError:
                            continue
                        reusable=candidate
                        break
                    if reusable is not None:
                        with reusable.open('rb') as source,partial.open('xb') as output:
                            os.chmod(partial,0o600)
                            shutil.copyfileobj(source,output,1024*1024)
                            output.flush()
                            os.fsync(output.fileno())
                        self._event('LOCAL_DOWNLOAD_REUSED',db_alias=alias)
                    else:
                        panel.download(scope,remote,partial,self.max_bytes)
                    if not partial.is_file() or not 0 < partial.stat().st_size <= self.max_bytes:
                        raise BackupError('DB_EXPORT_LIMIT_EXCEEDED')
                    warnings=validate_sql(partial) or []
                    if entry.get('imported_existing'):panel.check_existing_export(scope,remote)
                    if entry.get('imported_existing') and partial.stat().st_size!=remote['size_bytes']:raise BackupError('SOURCE_CHANGED')
                    artifact={'db_alias':alias,'relative_path':relative,
                              'size_bytes':partial.stat().st_size,'sha256':sha256_file(partial),
                              'validation':'PASS'}
                    if warnings:
                        artifact['warnings']=warnings
                    if entry.get('imported_existing'):
                        artifact['source_snapshot_verified']=False
                        artifact['warnings']=artifact.get('warnings',[])+['EXISTING_EXPORT_SOURCE_DATE_UNVERIFIED']
                    entry['pending_artifact']=artifact
                    self._save()  # Recover a crash between final rename and journal commit.
                    if replacing:
                        baseline=entry.get('replacement_baseline')
                        if target.exists() and (not baseline or not target.is_file()
                                or target.stat().st_size!=baseline['size'] or sha256_file(target)!=baseline['sha256']):
                            raise BackupError('LOCAL_DESTINATION_CONFLICT')
                        if entry.get('replace_existing') and target.exists():
                            preserved=local_path(self.output,'history/'+baseline['sha256']+'.sql')
                            private_dir(preserved.parent)
                            if not preserved.exists():shutil.copy2(target,preserved)
                            if sha256_file(preserved)!=baseline['sha256']:raise BackupError('LOCAL_FILE_CHANGED')
                            for previous in entry.get('previous_artifacts',[]):
                                if previous['sha256']==baseline['sha256'] and previous['relative_path']==relative:
                                    previous['relative_path']='history/'+baseline['sha256']+'.sql'
                    replace_with_retry(partial,target)
                    entry['artifact']=artifact
                    entry.pop('pending_artifact',None)
                    entry['phase']='DOWNLOADED'
                    self._save()
                    self._verify_local(artifact)
                    entry.pop('invalid_artifact',None)
                    entry.pop('replacement_baseline',None)
                    entry.pop('replace_corrupt',None)
                    entry.pop('replace_existing',None)
                    self._save()
                    for warning in artifact.get('warnings',[]):
                        self._event('SQL_WARNING',db_alias=alias,warning_code=warning)
                    self._event('SQL_VERIFIED',db_alias=alias,size_bytes=artifact['size_bytes'],sha256=artifact['sha256'],
                                source_snapshot_verified=artifact.get('source_snapshot_verified',True))
                else:
                    self._verify_local(entry['artifact'])
        except BackupError as error:
            if error.code=='DB_INTEGRITY_FAILED' and entry.get('artifact'):
                entry['artifact']['validation']='FAIL'
                self._save()
            raise
        finally:
            if self.cleanup_remote:
                partial.unlink(missing_ok=True)
            if remote is not None and not self.cleanup_remote:
                entry['remote']=remote
                entry['phase']='RETAINED' if entry.get('artifact') else 'WAITING'
                self._save()
                self._event('REMOTE_EXPORT_RETAINED',db_alias=alias)
            elif remote is not None:
                self._event('CLEANUP',db_alias=alias)
                try:
                    panel.cleanup(scope,remote)
                    entry.pop('remote',None)
                    entry['phase']='CLEANED' if entry.get('artifact') else 'FAILED_CLEANED'
                    self._save()
                    self._event('CLEANUP_VERIFIED',db_alias=alias)
                except Exception:
                    entry['remote']=remote
                    entry['phase']='DOWNLOADED' if entry.get('artifact') else 'WAITING'
                    self._save()
                    raise BackupError('CLEANUP_FAILED') from None
