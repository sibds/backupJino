"""Validated paused task registration for an executor that predates a catalog fix."""
import json
import uuid
from datetime import datetime,timezone
from .dashboard_catalog import DashboardCatalog
from .errors import BackupError


def stage_all_databases(data,row):
    catalog=DashboardCatalog(data)
    server=catalog.get(row)
    if server['blocked']:raise BackupError('SERVER_BLOCKED')
    if not server['can_check']:raise BackupError(server['issue'] or 'CREDENTIAL_MISSING')
    now=datetime.now(timezone.utc).isoformat()
    with catalog.connection() as db:
        db.execute('BEGIN IMMEDIATE')
        records=[json.loads(value[0]) for value in db.execute('SELECT metadata FROM servers')]
        actual=next(record for record in records if record['row']==row)
        if actual['revision']!=server['revision'] or actual['blocked'] or actual['deleted']:
            raise BackupError('SERVER_CONFIG_CHANGED')
        if any(other['row']!=row and not other['blocked'] and not other['deleted'] and other['panel_login']==actual['panel_login'] for other in records):
            raise BackupError('ACCOUNT_AMBIGUOUS')
        tasks=[json.loads(value[0]) for value in db.execute('SELECT payload FROM tasks ORDER BY rowid DESC')]
        active={'QUEUED','SCHEDULED','WAITING_RATE_LIMIT','WAITING_OPERATOR','RUNNING','PAUSING','PAUSED','STOPPING'}
        existing=next((task for task in tasks if task['row']==row and task['kind']=='databases' and task['state'] in active),None)
        if existing:
            if existing.get('db_aliases') is not None or existing.get('inspect_only'):raise BackupError('DOWNLOAD_BUSY')
            return existing
        task={'id':'operator-'+uuid.uuid4().hex[:24],'row':row,'kind':'databases','transport':'https_panel',
            'state':'PAUSED','stage':'WAITING','revision':server['binding_revision'],'folder':server['folder'],
            'created_at':now,'updated_at':now,'progress':{},'error_code':None,'result':None,
            'snapshot_date':None,'db_aliases':None,'inspect_only':False,'schedule_occurrence':None,
            'skip_unsupported_names':False,'ftp_connections':server['ftp_connections'],
            'scheduled_at':None,'priority':'normal','position':max((t.get('position',0) for t in tasks),default=0)+1}
        db.execute('INSERT INTO tasks VALUES (?,?,?)',(task['id'],row,json.dumps(task)))
        db.execute('INSERT INTO events(row_id,payload) VALUES (?,?)',(row,json.dumps({
            'time':now,'row':row,'task_id':task['id'],'kind':'databases','stage':'TASK_STAGED',
            'reason':'BLOCKED_DUPLICATE_COMPATIBILITY'})))
    return task
