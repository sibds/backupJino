"""Operator-selected bulk work and fixed-hour recurring server schedules."""
import hashlib
import json
import re
import time
from datetime import datetime, timezone

from .errors import BackupError
from .importers import identity

INTERVALS = (3,6,12,24,72,120,168,336,720,2160,4320,6480,8760)
PERMANENT_FAILURES = {'TLS_CERTIFICATE_INVALID','TLS_HANDSHAKE_FAILED','SFTP_HOST_KEY_UNTRUSTED',
    'SFTP_HOST_KEY_CHANGED','AUTH_FAILED','FTP_LOGIN_DENIED','FTP_ROOT_DENIED','AUTH_OR_ROOT_FAILED',
    'REMOTE_PATH_UNSAFE','LOCAL_PATH_UNSAFE','REMOTE_TYPE_UNSAFE','DOWNLOAD_SCOPE_CHANGED',
    'SERVER_CONFIG_CHANGED','PROFILE_UNVERIFIED','DB_INTEGRITY_FAILED','UI_CHANGED','CAPTCHA_REQUIRED',
    'MFA_REQUIRED','INTERNAL_ERROR'}


class WorkPlans:
    def __init__(self, management):
        self.management=management
        self.scheduling=False
        with management.catalog.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS work_rules (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS work_requests (id TEXT PRIMARY KEY, signature TEXT NOT NULL, payload TEXT NOT NULL)')

    def list(self):
        with self.management.catalog.connection() as db:
            rules=[json.loads(row[0]) for row in db.execute('SELECT payload FROM work_rules ORDER BY id')]
        for rule in rules:
            server=self.management.catalog.get(rule['row'],include_deleted=True)
            rule.update(server_name=server['name'],server_login=server['login'],
                        stale=server['binding_revision']!=rule['revision'],blocked=server['blocked'] or server['deleted'])
        return rules

    def _save(self,rule,db):
        stored={k:v for k,v in rule.items() if k not in {'server_name','server_login','stale','blocked'}}
        db.execute('INSERT OR REPLACE INTO work_rules VALUES (?,?)',(rule['id'],json.dumps(stored)))

    def _validate(self,payload):
        m=self.management
        rows=payload.get('rows')
        if (not isinstance(rows,list) or not 1<=len(rows)<=500 or any(type(r) is not int for r in rows)
                or type(payload.get('files')) is not bool or type(payload.get('databases')) is not bool
                or not (payload['files'] or payload['databases'])):
            raise BackupError('REQUEST_INVALID')
        hours=payload.get('every_hours')
        if hours is not None and (type(hours) is not int or hours not in INTERVALS):
            raise BackupError('SCHEDULE_INVALID')
        start=payload.get('scheduled_at')
        m._scheduled_timestamp(start)
        priority=payload.get('priority','normal')
        if priority not in {'normal','critical'}: raise BackupError('REQUEST_INVALID')
        servers=[]
        for row in sorted(set(rows)):
            server=m.catalog.get(row)
            if server['blocked'] or server['deleted']: raise BackupError('SERVER_BLOCKED')
            if m._legacy_busy(row): raise BackupError('LEGACY_JOB_ACTIVE')
            if payload['files'] and not server['ftp_ready']: raise BackupError(server['ftp_issue'] or 'FTP_CREDENTIAL_MISSING')
            if payload['databases'] and not server['can_check']: raise BackupError(server['issue'] or 'CREDENTIAL_MISSING')
            if not hours and payload['databases'] and any(t['kind']=='databases' and t['state'] in {'QUEUED','SCHEDULED','RUNNING','PAUSED','PAUSING','STOPPING','WAITING_RATE_LIMIT','WAITING_OPERATOR'}
                    and (t.get('db_aliases') is not None or t.get('inspect_only')) for t in m.tasks(row)):
                raise BackupError('DOWNLOAD_BUSY')
            servers.append(server)
        return servers,hours,start,priority

    def submit(self,payload):
        m=self.management
        with m.lock:
            request_id=payload.get('request_id')
            if not isinstance(request_id,str) or not re.fullmatch('[A-Za-z0-9-]{16,80}',request_id): raise BackupError('REQUEST_INVALID')
            signature=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
            with m.catalog.connection() as db:
                old=db.execute('SELECT signature,payload FROM work_requests WHERE id=?',(request_id,)).fetchone()
            if old:
                if old[0]!=signature: raise BackupError('REQUEST_INVALID')
                response=json.loads(old[1])
                if response.get('complete'): return response
            servers,hours,start,priority=self._validate(payload)
            kinds=[kind for kind,enabled in [('files',payload['files']),('databases',payload['databases'])] if enabled]
            response=json.loads(old[1]) if old else {'ok':True,'tasks_count':0,'rules_count':0,'task_ids':[],'complete':False}
            # Freeze links to already active work before creating any jobs.
            links=response.get('links',{})
            resume_ids=response.get('resume_ids',[])
            if not old:
                for server in servers:
                    for kind in kinds:
                        task=next((t for t in m.tasks(server['row']) if t['kind']==kind and t['state'] in {'QUEUED','SCHEDULED','RUNNING','PAUSED','PAUSING','STOPPING','WAITING_RATE_LIMIT','WAITING_OPERATOR'}),None)
                        if task: links[f"{server['row']}:{kind}"]=task['id']
                        elif kind=='files':
                            candidate=next((t for t in m.tasks(server['row']) if t['kind']=='files'),None)
                            partial=candidate and candidate['state']=='FINISHED' and (candidate.get('result') or {}).get('status')=='PARTIAL'
                            if (candidate and (candidate['state'] in {'FAILED','STOPPED','INTERRUPTED'} or partial)
                                    and candidate['revision']==server['binding_revision'] and candidate['transport']==server['protocol']):
                                links[f"{server['row']}:{kind}"]=candidate['id']
                                resume_ids.append(candidate['id'])
                response['links']=links
                response['resume_ids']=resume_ids
                with m.catalog.connection() as db:
                    db.execute('INSERT INTO work_requests VALUES (?,?,?)',(request_id,signature,json.dumps(response)))
            if hours:
                with m.catalog.connection() as db:
                    for server in servers:
                        for kind in kinds:
                            rule={'id':identity('rule',server['row'],server['binding_revision'],kind),
                                  'row':server['row'],'revision':server['binding_revision'],'kind':kind,
                                  'transport':server['protocol'] if kind=='files' else 'https_panel',
                                  'every_hours':hours,'next_at':start or datetime.now(timezone.utc).isoformat(),
                                  'enabled':True,'priority':priority,'last_error':None}
                            self._save(rule,db)
                    response.update(rules_count=len(servers)*len(kinds),complete=True)
                    db.execute('UPDATE work_requests SET payload=? WHERE id=?',(json.dumps(response),request_id))
            else:
                previous=m.scheduling_databases
                m.scheduling_databases=True
                try:
                    for server in servers:
                        for kind in kinds:
                            label=f"{server['row']}:{kind}"
                            occurrence='bulk:'+request_id+':'+label
                            task=next((t for t in m.tasks(server['row']) if t.get('schedule_occurrence')==occurrence),None)
                            if label in links:
                                task=m.task(links[label])
                                partial=task['state']=='FINISHED' and (task.get('result') or {}).get('status')=='PARTIAL'
                                if task['id'] in resume_ids and (task['state'] in {'FAILED','STOPPED','INTERRUPTED'} or partial):
                                    updates={'scheduled_at':start,'priority':priority}
                                    if start and m._scheduled_timestamp(start)>time.time(): updates['first_started_at']=None
                                    m._update(task['id'],**updates)
                                    task=m.action(task['id'],'resume')
                            if not task:
                                task=m.start(server['row'],kind,{'transport':server['protocol'] if kind=='files' else 'https_panel',
                                    'scheduled_at':start,'priority':priority,'schedule_occurrence':occurrence,
                                    'new_version':kind=='databases'})
                            links[label]=task['id']
                            response['links']=links
                            response['task_ids']=sorted(set(links.values()))
                            response['tasks_count']=len(response['task_ids'])
                            with m.catalog.connection() as db:
                                db.execute('UPDATE work_requests SET payload=? WHERE id=?',(json.dumps(response),request_id))
                    response['complete']=True
                    with m.catalog.connection() as db:
                        db.execute('UPDATE work_requests SET payload=? WHERE id=?',(json.dumps(response),request_id))
                finally: m.scheduling_databases=previous
            m._schedule()
            return response

    def tick(self,now=None):
        if self.scheduling: return
        self.scheduling=True
        m=self.management
        previous=m.scheduling_databases
        m.scheduling_databases=True
        now=time.time() if now is None else now
        try:
            for rule in self.list():
                if not rule['enabled'] or rule['stale'] or rule['blocked']: continue
                due=m._scheduled_timestamp(rule['next_at'])
                if due>now: continue
                occurrence='plan:'+rule['id']+':'+rule['next_at']
                tasks=m.tasks(rule['row'])
                previous_task=next((t for t in tasks if t['id']==rule.get('last_task_id')),None)
                if previous_task and previous_task['state']=='FAILED' and previous_task.get('error_code') in PERMANENT_FAILURES:
                    if previous_task['error_code']=='DB_INTEGRITY_FAILED' and not previous_task.get('db_redownload_exhausted'):
                        continue
                    rule.update(enabled=False,last_error=previous_task['error_code'])
                    with m.catalog.connection() as db: self._save(rule,db)
                    continue
                existing=next((t for t in tasks if t.get('schedule_occurrence')==occurrence),None)
                if not existing and any(t['kind']==rule['kind'] and (t['id'] in m.running or t['state'] in {'QUEUED','SCHEDULED','RUNNING','PAUSED','PAUSING','STOPPING','WAITING_RATE_LIMIT','WAITING_OPERATOR'}) for t in tasks): continue
                try:
                    task=existing or m.start(rule['row'],rule['kind'],{'transport':rule['transport'],'priority':rule['priority'],
                        'new_version':rule['kind']=='databases','fresh_run':True,'schedule_occurrence':occurrence})
                    seconds=rule['every_hours']*3600
                    next_at=due+(int((now-due)//seconds)+1)*seconds
                    rule.update(next_at=datetime.fromtimestamp(next_at,timezone.utc).isoformat(),last_task_id=task['id'],last_error=None)
                except BackupError as error:
                    rule['last_error']=error.code
                with m.catalog.connection() as db: self._save(rule,db)
        finally:
            m.scheduling_databases=previous
            self.scheduling=False

    def action(self,payload):
        m=self.management
        with m.lock:
            rows=payload.get('rows'); action=payload.get('action')
            if not isinstance(rows,list) or not rows or len(rows)>500 or any(type(r) is not int for r in rows): raise BackupError('REQUEST_INVALID')
            if action not in {'pause','resume','stop','disable_schedule','enable_schedule'}: raise BackupError('REQUEST_INVALID')
            for row in set(rows):
                server=m.catalog.get(row)
                if action=='resume' and server['blocked']: raise BackupError('SERVER_BLOCKED')
            kinds=[k for k in ('files','databases') if payload.get(k) is True]
            if not kinds: raise BackupError('REQUEST_INVALID')
            if action=='resume':
                for row in set(rows):
                    server=m.catalog.get(row)
                    for kind in kinds:
                        task=next((t for t in m.tasks(row) if t['kind']==kind),None)
                        if task and task['revision']!=server['binding_revision']: raise BackupError('SERVER_CONFIG_CHANGED')
            if action.endswith('schedule'):
                rules=[r for r in self.list() if r['row'] in rows and r['kind'] in kinds]
                rule_id=payload.get('rule_id')
                if rule_id is not None:
                    rules=[r for r in rules if r['id']==rule_id]
                    if len(rules)!=1: raise BackupError('REQUEST_INVALID')
                elif action=='enable_schedule':
                    rules=[r for r in rules if not r['stale']]
                with m.catalog.connection() as db:
                    for rule in rules:
                        if action=='enable_schedule' and (rule['stale'] or rule['blocked']): raise BackupError('SERVER_CONFIG_CHANGED')
                        rule['enabled']=action=='enable_schedule'
                        if rule['enabled']: rule.update(last_error=None,last_task_id=None)
                        self._save(rule,db)
                m._schedule()
                return {'ok':True,'updated_count':len(rules)}
            count=0
            previous=m.scheduling_databases
            try:
                for row in set(rows):
                    for kind in kinds:
                        task=next((t for t in m.tasks(row) if t['kind']==kind),None)
                        if not task: continue
                        partial=task['kind']=='files' and task['state']=='FINISHED' and (task.get('result') or {}).get('status')=='PARTIAL'
                        if action=='resume' and task['state'] not in {'PAUSED','PAUSING','STOPPED','INTERRUPTED','FAILED'} and not partial: continue
                        m.action(task['id'],action);count+=1
            finally: m.scheduling_databases=previous
            return {'ok':True,'updated_count':count}
