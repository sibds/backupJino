"""Private encrypted database inventory and operator-approved interval schedules."""
import json
import sqlite3
import threading
import os
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

from cryptography.fernet import Fernet, InvalidToken
from .errors import BackupError
from .importers import identity
from .state import private_dir


class DatabaseInventory:
    def __init__(self, path, key):
        self.path = path
        self.cipher = Fernet(key) if key else None
        self.lock = threading.RLock()
        if self.cipher is None:
            return  # Historical read-only dashboard can run before initialization.
        private_dir(path.parent)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS databases (row_id INTEGER, revision INTEGER, alias TEXT, payload BLOB, PRIMARY KEY(row_id,revision,alias))')
        os.chmod(self.path, 0o600)

    @contextmanager
    def connection(self):
        with self.lock:
            db = sqlite3.connect(self.path, timeout=5)
            try:
                with db:
                    yield db
            finally:
                db.close()

    def list(self, row, revision):
        if self.cipher is None:
            return []
        with self.connection() as db:
            values = db.execute('SELECT payload FROM databases WHERE row_id=? AND revision=?', (row, revision)).fetchall()
        return sorted([json.loads(self.cipher.decrypt(x[0])) for x in values], key=lambda x:x['name'])

    def _update(self, row, revision, alias, **changes):
        if self.cipher is None:
            raise BackupError('NOT_INITIALIZED')
        with self.connection() as db:
            found = db.execute('SELECT payload FROM databases WHERE row_id=? AND revision=? AND alias=?', (row, revision, alias)).fetchone()
            item = json.loads(self.cipher.decrypt(found[0])) if found else {'db_alias':alias}
            item.update(changes)
            db.execute('INSERT OR REPLACE INTO databases VALUES (?,?,?,?)', (row, revision, alias, self.cipher.encrypt(json.dumps(item).encode())))

    def discover(self, row, revision, account, names, snapshot=None):
        if not isinstance(names, list) or not all(isinstance(n, str) and n and not any(ord(c)<32 for c in n) for n in names):
            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
        for name in names:
            self._update(row, revision, identity('db', account, name), name=name,
                         **({'available_snapshot_date':snapshot} if snapshot else {}))

    def verified(self, row, revision, alias, snapshot, downloaded_at, size):
        item = next((x for x in self.list(row, revision) if x['db_alias']==alias), None)
        if item and item.get('downloaded_at') and downloaded_at is None:
            return
        if item and (not item.get('downloaded_at') or downloaded_at >= item['downloaded_at']):
            self._update(row, revision, alias, snapshot_date=snapshot, downloaded_at=downloaded_at,
                         size_bytes=size, validation='PASS')

    def schedule(self, row, revision, payload):
        if self.cipher is None:
            raise BackupError('NOT_INITIALIZED')
        aliases = payload.get('db_aliases', [payload.get('db_alias')])
        if ('db_aliases' in payload and 'db_alias' in payload
                or not isinstance(aliases, list) or not aliases
                or len(aliases) > 500 or not all(isinstance(x, str) for x in aliases)):
            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
        hours, enabled = payload.get('every_hours'), payload.get('enabled')
        if type(hours) is not int or not 1 <= hours <= 8760 or type(enabled) is not bool:
            raise BackupError('REQUEST_INVALID')
        next_at = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat() if enabled else None
        schedules = {alias:{'enabled':enabled, 'every_hours':hours, 'next_at':next_at} for alias in aliases}
        self._set_schedules(row, revision, schedules)
        return {'ok':True, 'updated_count':len(schedules), 'next_at':next_at}

    def _set_schedules(self, row, revision, schedules):
        with self.connection() as db:
            rows = db.execute('SELECT alias,payload FROM databases WHERE row_id=? AND revision=?', (row, revision)).fetchall()
            known = {alias:payload for alias,payload in rows}
            if not schedules.keys() <= known.keys():
                raise BackupError('DATABASE_SCOPE_UNVERIFIED')
            updates = []
            for alias, schedule in schedules.items():
                item = json.loads(self.cipher.decrypt(known[alias]))
                item['schedule'] = schedule
                updates.append((self.cipher.encrypt(json.dumps(item).encode()), row, revision, alias))
            db.executemany('UPDATE databases SET payload=? WHERE row_id=? AND revision=? AND alias=?', updates)

    def advance_group(self, row, revision, items, task_id):
        next_at = (datetime.now(timezone.utc)+timedelta(hours=items[0]['schedule']['every_hours'])).isoformat()
        self._set_schedules(row, revision, {item['db_alias']:{**item['schedule'], 'next_at':next_at,
            'last_task_id':task_id} for item in items})

    def advance(self, row, revision, alias, schedule, task_id):
        next_at = (datetime.now(timezone.utc)+timedelta(hours=schedule['every_hours'])).isoformat()
        self._update(row, revision, alias, schedule={**schedule,'next_at':next_at,'last_task_id':task_id})

    def import_history(self, catalog, tasks, state_dir):
        # Called before starting workers, never by HTTP polling. Active journals are not read.
        if self.cipher is None:
            return
        for task in tasks:
            if task['kind'] != 'databases' or task['state'] in {'RUNNING','PAUSING','STOPPING'}:
                continue
            server = catalog.get(task['row'], include_deleted=True)
            if task['revision'] != server['binding_revision']:
                continue
            result = task.get('result') or {}
            job = result.get('job_id')
            if not isinstance(job, str) or not job.startswith('panel-job-') or not all(c in '0123456789abcdef-' for c in job[10:]):
                continue
            path = state_dir / 'panel_jobs' / (job+'.enc')
            if not path.is_file():
                continue
            try:
                state = json.loads(self.cipher.decrypt(path.read_bytes()))
                scope = state.get('scope', {})
                if scope.get('account_ref') != server['account_ref']:
                    continue
                entries = state.get('databases', {})
                self.discover(task['row'], task['revision'], server['account_ref'], [x['name'] for x in entries.values()])
                times = {}
                events = path.with_suffix('.events.jsonl')
                if events.is_file():
                    with events.open(encoding='utf-8') as stream:
                        for line in stream:
                            event = json.loads(line)
                            if event.get('stage') == 'SQL_VERIFIED':
                                times[event.get('db_alias')] = event.get('time')
                for alias, entry in entries.items():
                    artifact = entry.get('artifact') or {}
                    if artifact.get('validation') == 'PASS':
                        self.verified(task['row'], task['revision'], alias, scope.get('snapshot_date'),
                                      times.get(alias), artifact['size_bytes'])
            except (OSError, ValueError, InvalidToken, KeyError):
                continue
