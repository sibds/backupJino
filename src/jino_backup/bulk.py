"""Sequential, resumable downloads of explicitly selected server account roots."""

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .downloads import DirectoryCheckpoint, FileLock, local_path
from .errors import BackupError
from .importers import identity, is_hash, jino_rows, valid_endpoint
from .state import private_dir, safe_root, write_json
from .storage import sha256_file
from .transfer import collect_files, source_for
from .retry import RecoveringSource, retry_call
from .server_folders import folder_names,server_work


@dataclass
class Server:
    server_id: str
    row: int
    credentials: dict = field(repr=False)
    folder_name: str | None = field(default=None,repr=False)


@dataclass
class ServerPlan:
    report: dict
    servers: list[Server] = field(default_factory=list, repr=False)


def plan_servers(path, mapping):
    required = ('id', 'ftp_host', 'ftp_login', 'ftp_password')
    if (not isinstance(mapping, dict)
            or any(not isinstance(mapping.get(key), str) for key in required)
            or len({mapping[key] for key in required}) != len(required)
            or mapping.get('ftp_protocol') not in {'ftps', 'sftp'}):
        raise BackupError('MAPPING_INVALID')
    rows = jino_rows(path)
    fields = {key for row in rows for key in row}
    if rows and any(mapping[key] not in fields for key in required):
        raise BackupError('MAPPING_INVALID')
    source_ids = Counter(str(row.get(mapping['id'])) for row in rows)
    endpoints = Counter((row.get(mapping['ftp_host']), row.get(mapping['ftp_login'])) for row in rows
                        if isinstance(row.get(mapping['ftp_host']), str) and isinstance(row.get(mapping['ftp_login']), str))
    result = ServerPlan({'rows_total':len(rows), 'servers_ready':0, 'needs_review':0, 'issues':[]})
    for index, row in enumerate(rows, 1):
        source_id = row.get(mapping['id'])
        credentials = {'host':row.get(mapping['ftp_host']), 'username':row.get(mapping['ftp_login']),
                       'password':row.get(mapping['ftp_password']), 'protocol':mapping['ftp_protocol'],
                       'port':mapping.get('ftp_port', 22 if mapping['ftp_protocol'] == 'sftp' else 21)}
        code = None
        if not all(isinstance(value,str) and value.strip() for value in [source_id, *[credentials[k] for k in ('host','username','password')]]):
            code = 'FTP_CREDENTIAL_MISSING'
        elif source_ids[str(source_id)] > 1 or endpoints[(credentials['host'], credentials['username'])] > 1:
            code = 'ACCOUNT_AMBIGUOUS'
        elif not valid_endpoint(credentials) or is_hash(credentials['password']):
            code = 'FTP_PROFILE_NEEDS_REVIEW'
        if code:
            result.report['issues'].append({'row':index, 'code':code})
        else:
            result.servers.append(Server(identity('server', source_id), index, credentials))
    named_rows=[(i,row) for i,row in enumerate(rows,1)
                if any(isinstance(row.get(mapping.get(key,'')),str) and row[mapping[key]].strip()
                       for key in ('login','ftp_login') if key in mapping)]
    names=dict(zip([i for i,row in named_rows],folder_names([row for i,row in named_rows],mapping)))
    for server in result.servers:
        server.folder_name=names[server.row]
    result.report['servers_ready'] = len(result.servers)
    result.report['needs_review'] = len(rows) - len(result.servers)
    return result


class DownloadBatch:
    def __init__(self, plan, destination, batch_id, remote_root, source_factory=source_for,
                 *, notify=None, max_bytes=1024**4, max_files=1000000,
                 timeout_seconds=86400, known_hosts=None, continue_on_error=False,
                 skip_unsupported_names=False, retry_wait=None, collector=None):
        if not isinstance(batch_id, str) or not 1 <= len(batch_id) <= 160:
            raise BackupError('IDEMPOTENCY_KEY_INVALID')
        self.plan, self.destination = plan, Path(destination).absolute()
        self.skip_unsupported_names = skip_unsupported_names
        self.retry_wait = retry_wait
        self.collector = collector
        self.batch_id, self.run_id = batch_id, identity('download', batch_id)
        self.remote_root, self.source_factory = safe_root(remote_root), source_factory
        self.max_bytes, self.max_files, self.timeout_seconds = max_bytes, max_files, timeout_seconds
        if not (1 <= max_bytes <= 1024**5 and 1 <= max_files <= 1000000 and 1 <= timeout_seconds <= 86400):
            raise BackupError('LIMIT_INVALID')
        self.known_hosts = known_hosts
        self.continue_on_error = continue_on_error
        self.notify = notify or (lambda event: None)
        self.report_path = self.destination / (self.run_id + '.json')

    def _download(self, server):
        with server_work(self.destination, server.server_id, server.folder_name, 'files') as directory:
            return self._download_locked(server, directory)

    def _download_locked(self, server, directory):
        snapshot = local_path(directory,self.run_id)
        scope = identity('scope', server.server_id, server.credentials['host'],
                         server.credentials['username'], server.credentials['port'],
                         server.credentials['protocol'], self.remote_root)
        event = lambda data: self.notify({'server_id':server.server_id, 'row':server.row, **data,
            **({'event':'retry_wait','attempt':data['retry_attempt'], 'delay_seconds':data['retry_delay_seconds']}
               if data.get('stage')=='RETRY_WAIT' else {})})
        with DirectoryCheckpoint(snapshot, scope, event) as progress:
            manifest_path = local_path(snapshot, 'manifest.json')
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                    if (manifest['scope'] != scope or manifest['status'] not in {'FILES_ONLY','PARTIAL'}
                            or manifest['job_id'] != self.run_id):
                        raise BackupError('INTEGRITY_FAILED')
                    for item in manifest['files']['items']:
                        file = local_path(snapshot, 'files/' + item['path'])
                        if file.stat().st_size != item['size_bytes'] or sha256_file(file) != item['sha256']:
                            raise BackupError('LOCAL_FILE_CHANGED')
                    if manifest['status']=='FILES_ONLY':
                        progress.validate_inventory(manifest['files']['items'])
                        return {'status':'FILES_ONLY', 'files_count':manifest['files']['count'],
                                'files_bytes':manifest['files']['bytes'], 'already_downloaded':True}
                except (OSError, KeyError, ValueError, TypeError):
                    raise BackupError('INTEGRITY_FAILED') from None
            started = datetime.now(timezone.utc).isoformat()
            site = {'remote_root':self.remote_root, 'known_hosts':self.known_hosts}
            if self.collector:
                captured = self.collector.collect(server.credentials, site, snapshot, progress)
            else:
                raw = retry_call(lambda: self.source_factory(server.credentials, site),
                                 wait=self.retry_wait, notify=lambda **values: event(values), phase='SCANNING')
                recovering = raw if hasattr(raw, 'retry_file') else RecoveringSource(
                    raw, wait=self.retry_wait, notify=lambda **values: event(values))
                with recovering as source:
                    captured = collect_files(source, snapshot / 'files', max_bytes=self.max_bytes,
                                             max_files=self.max_files, timeout_seconds=self.timeout_seconds,
                                             checkpoint=progress, continue_on_error=self.continue_on_error,
                                             skip_unsupported_names=self.skip_unsupported_names)
            manifest = {'schema_version':'1.0', 'job_id':self.run_id, 'site_id':server.server_id,
                        'scope':scope, 'status':'PARTIAL' if captured.get('errors') else 'FILES_ONLY', 'capture_method':server.credentials['protocol'],
                        'capture_started_at':started, 'capture_finished_at':datetime.now(timezone.utc).isoformat(),
                        'source_snapshot_at':None, 'capture_consistency':'BEST_EFFORT',
                        'files':captured, 'databases':[], 'encrypted':False,
                        'skip_unsupported_names':self.skip_unsupported_names,
                        'warnings':['DB_NOT_EXPORTED', 'LIVE_FILES_BEST_EFFORT']}
            write_json(manifest_path, manifest)
            return {'status':manifest['status'], 'files_count':captured['count'], 'files_bytes':captured['bytes'],
                    'files_skipped_count':len(captured.get('skipped',[])),
                    'file_errors_count':len(captured.get('errors',[])),
                    'already_downloaded':False}

    def run(self):
        private_dir(self.destination)
        results = []
        report = {'schema_version':'1.0', 'batch_id':self.run_id, **self.plan.report, 'servers':results}
        with FileLock(local_path(self.destination, '.jino-' + self.run_id + '.lock')):
            for server in self.plan.servers:
                item = {'server_id':server.server_id, 'row':server.row, 'status':'RUNNING'}
                results.append(item)
                write_json(self.report_path, report)
                self.notify({'event':'server_started', **item})
                try:
                    item.update(self._download(server))
                except KeyboardInterrupt:
                    item.update(status='CANCELLED', error_code='CANCELLED')
                    write_json(self.report_path, report)
                    raise
                except BackupError as exc:
                    item.update(status='FAILED', error_code=exc.code)
                except OSError:
                    item.update(status='FAILED', error_code='LOCAL_STORAGE_FAILED')
                except Exception:
                    item.update(status='FAILED', error_code='INTERNAL_ERROR')
                if item['status'] != 'FAILED':
                    item.pop('error_code', None)
                write_json(self.report_path, report)
                self.notify({'event':'server_finished', **item})
        report['finished'] = True
        write_json(self.report_path, report)
        return report
