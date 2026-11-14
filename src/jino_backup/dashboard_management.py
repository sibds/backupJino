"""Persistent operator tasks and cooperative controls for local backup jobs."""

import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone, timedelta

from .dashboard_catalog import DashboardCatalog
from .downloads import FileLock
from .errors import BackupError
from .importers import identity
from .panel_access import PanelAccessPolicy, iso_time


WAITING = {"QUEUED", "SCHEDULED", "WAITING_RATE_LIMIT", "WAITING_OPERATOR"}
ACTIVE = WAITING | {"RUNNING", "PAUSING", "PAUSED", "STOPPING"}
TERMINAL = {"FINISHED", "FAILED", "STOPPED", "INTERRUPTED"}
AUTO_FILE_ERRORS = {"TRANSFER_INTERRUPTED", "CONNECTION_FAILED", "SFTP_CONNECTION_FAILED",
                    "OPERATION_TIMEOUT", "TRANSFER_TIMEOUT", "FILETRANSFER_CONNECTION_FAILED",
                    "FILETRANSFER_AUTH_EXPIRED", "PANEL_RESPONSE_FAILED", "WORKER_RESTARTED"}
AUTO_FILE_DELAYS = (300, 900, 1800, 3600)


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class TaskControl:
    def __init__(self, changed):
        self.condition = threading.Condition()
        self.paused = False
        self.stopped = False
        self.changed = changed
        self.workers = set()
        self.waiting_workers = set()
        self.pause_announced = False

    def register_worker(self):
        with self.condition:
            self.workers.add(threading.get_ident())

    def finish_worker(self):
        with self.condition:
            self.workers.discard(threading.get_ident())
            self.waiting_workers.discard(threading.get_ident())
            announce = self.paused and bool(self.workers) and self.workers <= self.waiting_workers and not self.pause_announced
            if announce:
                self.pause_announced = True
        if announce:
            self.changed("PAUSED")

    def checkpoint(self, *, interrupted=None):
        with self.condition:
            waiting = self.paused and not self.stopped
        if waiting:
            with self.condition:
                self.waiting_workers.add(threading.get_ident())
                announce = (not self.workers or self.workers <= self.waiting_workers) and not self.pause_announced
                if announce:
                    self.pause_announced = True
            if announce:
                self.changed("PAUSED")
            with self.condition:
                while self.paused and not self.stopped:
                    if interrupted and interrupted():
                        self.waiting_workers.discard(threading.get_ident())
                        raise BackupError("CANCELLED")
                    self.condition.wait(0.25)
                resumed = not self.stopped
                self.waiting_workers.discard(threading.get_ident())
                announce = resumed and self.pause_announced
                self.pause_announced = False
            if announce:
                self.changed("RUNNING")
        with self.condition:
            if self.stopped or interrupted and interrupted():
                raise BackupError("CANCELLED")

    def request(self, action):
        with self.condition:
            if action == "stop":
                self.stopped = True
            else:
                self.paused = action == "pause"
            self.condition.notify_all()

    def wait(self, seconds, *, interrupted=None):
        deadline = time.monotonic() + seconds
        while True:
            self.checkpoint(interrupted=interrupted)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            with self.condition:
                self.condition.wait(min(remaining, 0.25))


class DashboardManagement:
    def __init__(self, data, runner=None, access_policy=None):
        self.data = data
        self.catalog = DashboardCatalog(data)
        from .database_inventory import DatabaseInventory
        from .state import Settings
        try:
            inventory_key = Settings.load(data.state_dir).read_key()
        except BackupError as error:
            if error.code != 'NOT_INITIALIZED':
                raise
            inventory_key = None
        self.inventory = DatabaseInventory(data.state_dir / 'database-inventory.sqlite3', inventory_key)
        data.database_inventory = self.inventory
        self.scheduling_databases = False
        from .work_plans import WorkPlans
        self.work_plans=WorkPlans(self)
        self.access_policy = access_policy or PanelAccessPolicy(data.state_dir)
        self.guard = FileLock(data.state_dir / "dashboard-management.lock")
        self.guard.__enter__()
        self.lock = threading.RLock()
        self.controls = {}
        self.running = set()
        self.closed = False
        self.timer = None
        self.executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="backup-operator")
        if runner is None:
            from .dashboard_runner import OperatorRunner

            runner = OperatorRunner(data, self.catalog)
        self.runner = runner
        with self.catalog.connection() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, row_id INTEGER, payload TEXT NOT NULL)"
            )
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, row_id INTEGER, payload TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS events_server ON events(row_id,id)")
            db.execute("CREATE INDEX IF NOT EXISTS events_server_stage ON events(row_id,json_extract(payload,'$.stage'),id)")
            rows = list(db.execute("SELECT id,payload FROM tasks"))
            for position, row in enumerate(rows):
                task = json.loads(row["payload"])
                task.setdefault("position", position)
                task.setdefault("priority", "normal")
                task.setdefault("scheduled_at", None)
                if task["state"] in {"RUNNING", "PAUSING", "STOPPING"}:
                    task.update(
                        state="INTERRUPTED", updated_at=timestamp(), error_code="WORKER_RESTARTED"
                    )
                db.execute("UPDATE tasks SET payload=? WHERE id=?", (json.dumps(task), row["id"]))
                if task["state"] in WAITING:
                    task_id = task["id"]
                    self.controls[task_id] = TaskControl(
                        lambda state, identifier=task_id: self._control_state(identifier, state)
                    )
        self.inventory.import_history(self.catalog, self.tasks(), self.data.state_dir)
        with self.lock:
            latest_files = {}
            latest_databases = {}
            for task in self.tasks():
                if task['kind'] == 'files': latest_files.setdefault(task['row'], task)
                if task['kind'] == 'databases': latest_databases.setdefault(task['row'],task)
            for task in latest_files.values(): self._queue_file_resume(task['id'])
            for task in latest_databases.values(): self._queue_database_redownload(task['id'])
            self._schedule()

    def task(self, task_id):
        with self.catalog.connection() as db:
            row = db.execute("SELECT payload FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise BackupError("TASK_NOT_FOUND")
        return json.loads(row[0])

    def tasks(self, row=None):
        with self.catalog.connection() as db:
            query = "SELECT payload FROM tasks"
            values = ()
            if row is not None:
                query += " WHERE row_id=?"
                values = (row,)
            return [
                json.loads(record[0])
                for record in db.execute(query + " ORDER BY rowid DESC", values)
            ]

    def _update(self, task_id, **changes):
        with self.lock, self.catalog.connection() as db:
            task = json.loads(
                db.execute("SELECT payload FROM tasks WHERE id=?", (task_id,)).fetchone()[0]
            )
            task.update(changes, updated_at=timestamp())
            db.execute("UPDATE tasks SET payload=? WHERE id=?", (json.dumps(task), task_id))
        return task

    def event(self, row, stage, *, kind=None, code=None, task_id=None, **details):
        event = {
            "time": timestamp(),
            "row": row,
            "stage": stage,
            "kind": kind,
            "error_code": code,
            "task_id": task_id,
        }
        for key in (
            "received_bytes",
            "planned_bytes",
            "verified_files",
            "verified_count",
            "expected_count",
            "skipped_count",
            "retry_attempt", "retry_limit", "retry_delay_seconds",
            "retry_total", "committed_bytes",
        ):
            if type(details.get(key)) is int:
                event[key] = details[key]
        if isinstance(details.get("current_file"), str):
            event["file"] = details["current_file"][:300]
        if details.get('warning_code')=='SQL_NO_OBJECTS':
            event['warning_code']='SQL_NO_OBJECTS'
            event['level']='warning'
        if isinstance(details.get("retry_at"), str):
            event["retry_at"] = details["retry_at"]
        if isinstance(details.get("directory"), str):
            event["directory"] = details["directory"][:2000]
        conflict = details.get('conflict')
        if stage in {'FILE_CONFLICT','FILE_RECONCILED'} and isinstance(conflict, dict):
            allowed = {'id','remote_path','local_path','remote_size','local_size','remote_modified',
                       'local_modified','remote_sha256','local_sha256','same_content','remote_newer',
                       'reason','remote_kind','local_kind'}
            event['conflict'] = {key:value for key,value in conflict.items() if key in allowed
                                 and (value is None or type(value) in {str,int,bool})}
            if details.get('action') in {'identical','updated','kept_both'}:
                event['action'] = details['action']
        if details.get("operation") in {"CWD", "PWD", "MLSD", "MLST", "RETR"}:
            event["operation"] = details["operation"]
        if type(details.get("ftp_reply_code")) is int and 100 <= details["ftp_reply_code"] <= 599:
            event["ftp_reply_code"] = details["ftp_reply_code"]
        from .errors import CURL_ERROR_KINDS
        if type(details.get("curl_error_code")) is int and 0 < details["curl_error_code"] < 1000:
            event["curl_error_code"] = details["curl_error_code"]
        if details.get("curl_error_kind") in CURL_ERROR_KINDS:
            event["curl_error_kind"] = details["curl_error_kind"]
        if type(details.get("progress_advanced")) is bool:
            event["progress_advanced"] = details["progress_advanced"]
        if stage == "FILE_SKIPPED":
            if isinstance(details.get("skipped_path"), str):
                event["file"] = details["skipped_path"][:2000]
            if details.get("skip_reason") in {"WINDOWS_CHARACTERS", "TRAILING_DOT_OR_SPACE", "WINDOWS_RESERVED_NAME", "INVALID_UTF8_NAME", "SYMLINK", "FIFO", "SOCKET", "CHAR_DEVICE", "BLOCK_DEVICE"}:
                event["skip_reason"] = details["skip_reason"]
            if details.get("skipped_kind") in {"file", "dir", "special"}:
                event["skipped_kind"] = details["skipped_kind"]
        with self.catalog.connection() as db:
            db.execute("INSERT INTO events(row_id,payload) VALUES (?,?)", (row, json.dumps(event)))

    def events(self, row, limit=300, *, stage=None):
        self.catalog.get(row, include_deleted=True)
        with self.catalog.connection() as db:
            query = "SELECT payload FROM events WHERE row_id=?"
            parameters = [row]
            if stage is not None:
                query += " AND json_extract(payload,'$.stage')=?"
                parameters.append(stage)
            parameters.append(limit)
            return [
                json.loads(value[0])
                for value in db.execute(
                    query + " ORDER BY id DESC LIMIT ?",
                    parameters,
                )
            ]

    def busy(self, row):
        return any(task["state"] in ACTIVE for task in self.tasks(row))

    def _legacy_busy(self, row):
        return self.data.process_alive() and any(
            item.get("row") == row and item.get("status") == "RUNNING"
            for item in self.data.report()["servers"]
        )

    def save_server(self, row, payload):
        with self.lock:
            if row is not None and (self.busy(row) or self._legacy_busy(row)):
                raise BackupError("SERVER_BUSY")
            if (
                row is not None
                and self.data.monitor
                and self.data.monitor.snapshot().get(str(row), {}).get("state") == "CHECKING"
            ):
                raise BackupError("SERVER_BUSY")
            config = self.catalog.save(row, payload)
            if row is not None and self.data.monitor:
                self.data.monitor.invalidate(row)
            self.event(config["row"], "SERVER_UPDATED" if row else "SERVER_ADDED")
            return config

    def server_action(self, row, action):
        with self.lock:
            self.catalog.get(row)
            if action == "delete":
                if self.busy(row) or self._legacy_busy(row):
                    raise BackupError("SERVER_BUSY")
                self.catalog.set_flag(row, "deleted", True)
            elif action in {"block", "unblock"}:
                if self._legacy_busy(row):
                    raise BackupError("LEGACY_JOB_ACTIVE")
                self.catalog.set_flag(row, "blocked", action == "block")
                if action == "block":
                    for task in self.tasks(row):
                        if task["state"] in ACTIVE:
                            self.action(task["id"], "stop")
            else:
                raise BackupError("REQUEST_INVALID")
            self.event(row, "SERVER_" + action.upper())
            self._schedule()
        return {"ok": True}

    def start(self, row, kind, options):
        if kind not in {"files", "databases"} or not isinstance(options, dict):
            raise BackupError("REQUEST_INVALID")
        with self.lock:
            server = self.catalog.get(row)
            if self.closed or server["blocked"]:
                raise BackupError("SERVER_BLOCKED")
            if self._legacy_busy(row):
                raise BackupError("LEGACY_JOB_ACTIVE")
            transport = (
                "https_panel"
                if kind == "databases"
                else options.get("transport", server["protocol"])
            )
            if transport not in {"ftps", "sftp", "https_panel"}:
                raise BackupError("SERVER_CONFIG_INVALID")
            if (
                kind == "files"
                and transport in {"ftps", "sftp"}
                and transport != server["protocol"]
            ):
                raise BackupError("PROTOCOL_NOT_CONFIGURED")
            if kind == "databases" or transport == "https_panel":
                if not server["can_check"]:
                    raise BackupError(server["issue"] or "CREDENTIAL_MISSING")
            elif not server["ftp_ready"]:
                raise BackupError(server["ftp_issue"] or "FTP_CREDENTIAL_MISSING")
            snapshot = options.get("snapshot_date")
            selected = options.get('db_aliases')
            inspect_only = options.get('inspect_only', False)
            if type(options.get('fresh_run',False)) is not bool:
                raise BackupError('REQUEST_INVALID')
            if type(options.get('new_version', False)) is not bool or type(inspect_only) is not bool or kind != 'databases' and (selected is not None or inspect_only or options.get('new_version')):
                raise BackupError('REQUEST_INVALID')
            if selected is not None:
                known = {x['db_alias'] for x in self.inventory.list(row, server['binding_revision'])}
                if not isinstance(selected, list) or not selected or not all(isinstance(x, str) and x in known for x in selected):
                    raise BackupError('DATABASE_SCOPE_UNVERIFIED')
                selected = sorted(set(selected))
            if snapshot is not None:
                try:
                    if date.fromisoformat(snapshot).isoformat() != snapshot:
                        raise ValueError
                except (TypeError, ValueError):
                    raise BackupError("SNAPSHOT_DATE_INVALID") from None
            for task in self.tasks(row):
                if task["kind"] != kind:
                    continue
                if task["state"] in ACTIVE:
                    if kind == 'databases' and (task.get('db_aliases') != selected or task.get('inspect_only', False) != inspect_only):
                        raise BackupError('DOWNLOAD_BUSY')
                    return task
                if (
                    (task["state"] in {"FAILED", "STOPPED", "INTERRUPTED"}
                     or kind == "files" and task["state"] == "FINISHED" and (task.get("result") or {}).get("status") == "PARTIAL")
                    and task["revision"] == server["binding_revision"]
                    and task["transport"] == transport
                    and (snapshot is None or task.get("snapshot_date") == snapshot)
                    and task.get('db_aliases') == selected
                    and task.get('inspect_only', False) == inspect_only
                    and not options.get('new_version', False)
                    and not options.get('fresh_run', False)
                ):
                    return self.action(task["id"], "resume")
                break
            task_id = "operator-" + uuid.uuid4().hex[:24]
            scheduled_at = options.get("scheduled_at")
            self._scheduled_timestamp(scheduled_at)
            priority = options.get("priority", "normal")
            if priority not in {"normal", "critical"}:
                raise BackupError("REQUEST_INVALID")
            task = {
                "id": task_id,
                "row": row,
                "kind": kind,
                "transport": transport,
                "state": "QUEUED",
                "stage": "WAITING",
                "revision": server["binding_revision"],
                "folder": server["folder"],
                "created_at": timestamp(),
                "updated_at": timestamp(),
                "progress": {},
                "error_code": None,
                "result": None,
                "snapshot_date": snapshot,
                "db_aliases": selected,
                "inspect_only": inspect_only,
                "schedule_occurrence": options.get('schedule_occurrence'),
                "skip_unsupported_names": kind == "files" and server["skip_unsupported_names"],
                "ftp_connections": server["ftp_connections"],
                "scheduled_at": scheduled_at,
                "priority": priority,
                "position": max((item.get("position", 0) for item in self.tasks()), default=0) + 1,
            }
            with self.catalog.connection() as db:
                db.execute("INSERT INTO tasks VALUES (?,?,?)", (task_id, row, json.dumps(task)))
            self.controls[task_id] = TaskControl(lambda state: self._control_state(task_id, state))
            self.event(row, "QUEUED", kind=kind, task_id=task_id)
            if priority == "critical":
                self._move(task_id, "top")
            if not self.scheduling_databases:
                self._schedule()
            return self.task(task_id)

    def _control_state(self, task_id, state):
        with self.lock:
            task = self.task(task_id)
            if task["state"] == "STOPPING":
                return
            self._update(task_id, state=state)
            self.event(task["row"], state, kind=task["kind"], task_id=task_id)

    def resolve_conflict(self, task_id, payload):
        import re
        with self.lock:
            task = self.task(task_id)
            key, decision = payload.get('conflict_id'), payload.get('decision')
            if (task['kind'] != 'files' or not isinstance(key,str) or not re.fullmatch('[a-f0-9]{64}',key)
                    or decision not in {'keep_both','use_remote','keep_local'}):
                raise BackupError('REQUEST_INVALID')
            server = self.catalog.get(task['row'])
            if server['blocked'] or server['binding_revision'] != task['revision']:
                raise BackupError('SERVER_CONFIG_CHANGED')
            with self.catalog.connection() as db:
                found = db.execute("SELECT payload FROM events WHERE row_id=? AND json_extract(payload,'$.task_id')=? AND json_extract(payload,'$.stage')='FILE_CONFLICT' AND json_extract(payload,'$.conflict.id')=? LIMIT 1",
                                   (task['row'],task_id,key)).fetchone()
            if not found:
                raise BackupError('REQUEST_INVALID')
            decisions = dict(task.get('conflict_decisions',{}))
            decisions[key] = decision
            self._update(task_id,conflict_decisions=decisions)
            return {'ok':True,'apply_on_resume':True}

    def action(self, task_id, action):
        if action not in {"pause", "resume", "stop"}:
            raise BackupError("REQUEST_INVALID")
        with self.lock:
            task = self.task(task_id)
            control = self.controls.get(task_id)
            active = task_id in self.running
            if action == "resume":
                server = self.catalog.get(task["row"])
                if server["blocked"]:
                    raise BackupError("SERVER_BLOCKED")
                if server["binding_revision"] != task["revision"]:
                    raise BackupError("SERVER_CONFIG_CHANGED")
                if task['kind']=='databases' and task.get('db_redownload_exhausted'):
                    self._update(task_id,db_redownload_count=0,db_redownload_exhausted=False)
                partial = task["kind"] == "files" and task["state"] == "FINISHED" and (task.get("result") or {}).get("status") == "PARTIAL"
                automatic_wait = task['state'] == 'SCHEDULED' and task.get('auto_resume_at')
                if not partial and not automatic_wait and task["state"] not in {"PAUSED", "PAUSING", "STOPPED", "INTERRUPTED", "FAILED"}:
                    return task
                if task.get('auto_resume_at'):
                    self._update(task_id, scheduled_at=None, auto_resume_at=None)
                first = task.get('first_started_at')
                if first and time.time() - self._scheduled_timestamp(first) >= 86400:
                    self._update(task_id, first_started_at=timestamp())
                if not active:
                    control = TaskControl(lambda state: self._control_state(task_id, state))
                    self.controls[task_id] = control
                    if task["kind"] == "files":
                        self._update(task_id, skip_unsupported_names=server["skip_unsupported_names"],
                                     ftp_connections=server["ftp_connections"])
                    progress = dict(task["progress"])
                    progress.update(retry_attempt=0, retry_delay_seconds=0, retry_at=None,
                                    active_files=[], active_transfers=0)
                    self._update(task_id, stage="WAITING", progress=progress)
                self._update(task_id, state="RUNNING" if active else "QUEUED", error_code=None, result=None)
            elif task["state"] not in ACTIVE:
                return task
            else:
                if task.get('auto_resume_at'):
                    self._update(task_id, scheduled_at=None, auto_resume_at=None)
                self._update(
                    task_id,
                    state=("PAUSING" if active else "PAUSED")
                    if action == "pause"
                    else ("STOPPING" if active else "STOPPED"),
                )
            if control:
                control.request(action)
            self.event(
                task["row"], action.upper() + "_REQUESTED", kind=task["kind"], task_id=task_id
            )
            self._schedule()
            return self.task(task_id)

    def _schedule(self):
        if self.closed:
            return
        if self.timer:
            self.timer.cancel()
            self.timer = None
        self._schedule_databases()
        self.work_plans.tick()
        all_tasks = self.tasks()
        active = [task for task in all_tasks if task["id"] in self.running]
        next_wake = []
        for task in sorted(all_tasks, key=lambda item: item.get("position", 0)):
            if task["state"] not in WAITING:
                continue
            if task.get('auto_resume_at') or task['kind']=='databases' and task.get('db_redownload_count',0)>0:
                server = self.catalog.get(task['row'], include_deleted=True)
                if server['blocked'] or server['deleted']:
                    self._update(task['id'], state='PAUSED', auto_resume_at=None, scheduled_at=None)
                    continue
                if server['binding_revision'] != task['revision']:
                    self._update(task['id'], state='FAILED', auto_resume_at=None, error_code='SERVER_CONFIG_CHANGED')
                    continue
            limit = 1 if task["kind"] == "databases" else 5
            if sum(item["kind"] == task["kind"] for item in active) >= limit:
                continue
            # ConnectionBudget grants actual leases in the runner. Idle reservations
            # must not prevent other servers from using the free connection slots.
            if any(item["row"] == task["row"] and (
                item["kind"] == task["kind"]
                or self._uses_panel(item) and self._uses_panel(task)
            ) for item in active):
                continue
            due = self._scheduled_timestamp(task.get("scheduled_at"))
            if due > time.time():
                self._update(task["id"], state="SCHEDULED", next_at=iso_time(due))
                next_wake.append(due)
                continue
            if task['kind'] == 'files' and task.get('first_started_at') and time.time() - self._scheduled_timestamp(task['first_started_at']) >= 86400:
                self._update(task['id'], state='FAILED', error_code='TRANSFER_TIMEOUT', auto_resume_at=None)
                continue
            if self._uses_panel(task):
                try:
                    self.access_policy.reserve(task["row"], task["kind"])
                except BackupError as error:
                    if error.code not in {"PANEL_RATE_LIMIT", "PANEL_AUTH_PAUSED"}:
                        raise
                    policy = self.access_policy.snapshot()
                    self._update(
                        task["id"],
                        state="WAITING_OPERATOR" if policy["blocked"] else "WAITING_RATE_LIMIT",
                        next_at=policy["next_at"],
                        wait_reason=error.code,
                        error_code=None,
                    )
                    if not policy["blocked"]:
                        next_wake.append(policy["next_timestamp"])
                    continue
            self.running.add(task["id"])
            active.append(task)
            self._update(
                task["id"], state="RUNNING", started_at=timestamp(),
                first_started_at=task.get('first_started_at') or timestamp(),
                auto_resume_at=None, next_at=None, error_code=None
            )
            self.executor.submit(self._run, task["id"])
        if any(x.get('schedule', {}).get('enabled') for server in self.catalog.all() if not server['deleted'] for x in self.inventory.list(server['row'], server['binding_revision'])):
            next_wake.append(time.time()+60)
        if any(rule['enabled'] and not rule['stale'] and not rule['blocked'] for rule in self.work_plans.list()):
            next_wake.append(time.time()+60)
        if next_wake:
            delay = min(60, max(0.1, min(next_wake) - time.time()))
            self.timer = threading.Timer(delay, self._wake)
            self.timer.daemon = True
            self.timer.start()

    def _schedule_databases(self):
        if self.scheduling_databases:
            return
        self.scheduling_databases = True
        try:
            for server in self.catalog.all():
                if server['blocked'] or server['deleted'] or not server['can_check']:
                    continue
                if any(t['kind']=='databases' and (t['state'] in ACTIVE or t['id'] in self.running) for t in self.tasks(server['row'])):
                    continue
                for item in self.inventory.list(server['row'], server['binding_revision']):
                    schedule = item.get('schedule') or {}
                    if schedule.get('enabled') and self._scheduled_timestamp(schedule['next_at']) <= time.time():
                        group = [other for other in self.inventory.list(server['row'], server['binding_revision'])
                                 if other.get('schedule', {}).get('enabled')
                                 and other['schedule'].get('next_at') == schedule['next_at']
                                 and other['schedule'].get('every_hours') == schedule['every_hours']]
                        aliases = sorted(other['db_alias'] for other in group)
                        occurrence = (aliases[0] if len(aliases) == 1 else identity('db-group', *aliases)) + ':' + schedule['next_at']
                        existing = next((t for t in self.tasks(server['row']) if t.get('schedule_occurrence') == occurrence), None)
                        try:
                            task = existing or self.start(server['row'], 'databases', {'db_aliases':aliases, 'new_version':True, 'schedule_occurrence':occurrence})
                        except BackupError as error:
                            if schedule.get('last_error') != error.code:
                                self.event(server['row'], 'SCHEDULE_WAIT', kind='databases', code=error.code)
                            self.inventory._update(server['row'], server['binding_revision'], item['db_alias'], schedule={**schedule, 'last_error':error.code})
                            continue
                        self.inventory.advance_group(server['row'], server['binding_revision'], group, task['id'])
                        break
        finally:
            self.scheduling_databases = False

    def _wake(self):
        with self.lock:
            self._schedule()

    @staticmethod
    def _file_slots(task):
        if task["kind"] != "files":
            return 0
        return task.get("ftp_connections", 3) if task["transport"] == "ftps" else 1

    @staticmethod
    def _uses_panel(task):
        return task["kind"] == "databases" or task["transport"] == "https_panel"

    @staticmethod
    def _scheduled_timestamp(value):
        if value is None or value == "":
            return 0
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                raise ValueError
            return parsed.timestamp()
        except (ValueError, TypeError):
            raise BackupError("SCHEDULE_INVALID") from None

    def _move(self, task_id, direction):
        pending = sorted(
            (
                item
                for item in self.tasks()
                if item["state"] in WAITING
                or (item["state"] == "PAUSED" and item["id"] not in self.running)
            ),
            key=lambda item: item.get("position", 0),
        )
        index = next((number for number, item in enumerate(pending) if item["id"] == task_id), None)
        if index is None:
            raise BackupError("TASK_BUSY")
        target = {
            "up": max(0, index - 1),
            "down": min(len(pending) - 1, index + 1),
            "top": 0,
            "bottom": len(pending) - 1,
        }.get(direction)
        if target is None:
            raise BackupError("REQUEST_INVALID")
        moved = pending.pop(index)
        pending.insert(target, moved)
        with self.catalog.connection() as db:
            for position, item in enumerate(pending):
                item["position"] = position
                db.execute("UPDATE tasks SET payload=? WHERE id=?", (json.dumps(item), item["id"]))

    def change(self, task_id, action, payload):
        with self.lock:
            task = self.task(task_id)
            if task_id in self.running or task["state"] in TERMINAL:
                raise BackupError("TASK_BUSY")
            if action == "move":
                self._move(task_id, payload.get("direction"))
            elif action == "priority":
                if payload.get("priority") not in {"normal", "critical"}:
                    raise BackupError("REQUEST_INVALID")
                self._update(task_id, priority=payload["priority"])
                if payload["priority"] == "critical":
                    self._move(task_id, "top")
            elif action == "schedule":
                value = payload.get("scheduled_at")
                self._scheduled_timestamp(value)
                self._update(task_id, scheduled_at=value)
            else:
                raise BackupError("REQUEST_INVALID")
            self.event(task["row"], "QUEUE_UPDATED", kind=task["kind"], task_id=task_id)
            self._schedule()
            return self.task(task_id)

    def plan(self):
        with self.lock:
            servers = {server["row"]: server for server in self.catalog.all()}
            tasks = self.tasks()
            policy = self.access_policy.snapshot()
            slot = policy["next_timestamp"]
            pending, completed = [], []
            for task in sorted(tasks, key=lambda item: item.get("position", 0)):
                server = servers[task["row"]]
                task.update(server_name=server["name"], server_login=server["login"])
                task.update(server_deleted=server["deleted"], server_blocked=server["blocked"])
                task["can_reorder"] = (
                    task["id"] not in self.running and task["state"] not in TERMINAL
                )
                task["estimated_at"] = task.get("scheduled_at")
                if task["state"] in TERMINAL:
                    completed.append(task)
                else:
                    pending.append(task)
            completed.sort(key=lambda task: task["updated_at"], reverse=True)
            reorderable = [task for task in pending if task["can_reorder"]]
            for rank, task in enumerate(reorderable):
                task.update(queue_rank=rank, queue_total=len(reorderable))
            candidates = [
                task
                for task in pending
                if self._uses_panel(task)
                and task["id"] not in self.running
                and task["state"] in WAITING
            ]
            while candidates and not policy["blocked"]:
                ready = [
                    task
                    for task in candidates
                    if self._scheduled_timestamp(task.get("scheduled_at")) <= slot
                ]
                chosen = (
                    ready[0]
                    if ready
                    else min(
                        candidates,
                        key=lambda task: self._scheduled_timestamp(task.get("scheduled_at")),
                    )
                )
                slot = max(slot, self._scheduled_timestamp(chosen.get("scheduled_at")))
                chosen["estimated_at"] = iso_time(slot)
                slot += policy["interval_seconds"]
                candidates.remove(chosen)
            if policy["blocked_row"] in servers:
                policy["blocked_server_name"] = servers[policy["blocked_row"]]["name"]
            return {"pending": pending, "completed": completed, "policy": policy,'rules':self.work_plans.list()}

    def resume_panel_queue(self):
        with self.lock:
            self.access_policy.resume()
            self._schedule()
        if self.data.monitor:
            self.data.monitor.retry_waiting()
        return {"ok": True}

    def _run(self, task_id):
        task = self.task(task_id)
        control = self.controls[task_id]
        try:
            server = self.catalog.get(task["row"])
            control.checkpoint()
            result = self.runner(
                server, task, control, lambda **values: self.progress(task_id, values)
            )
            if control.stopped:
                raise BackupError("CANCELLED")
            failed = result.get("status") in {"FAILED", "CANCELLED"}
            if result.get("error_code") == "CAPTCHA_REQUIRED":
                self.access_policy.block(task["row"])
            if task["kind"] == "databases" and result.get('artifacts'):
                verified_artifacts = [x for x in result['artifacts'] if x.get('validation') == 'PASS']
                self.progress(
                    task_id,
                    {
                        "verified_count": len(verified_artifacts),
                        "received_bytes": sum(
                            item["size_bytes"] for item in verified_artifacts
                        ),
                    },
                )
            self._update(
                task_id,
                state="FAILED" if failed else "FINISHED",
                result=result,
                error_code=result.get("error_code"),
            )
        except Exception as error:
            code = (
                error.code
                if isinstance(error, BackupError)
                else (
                    "OPERATION_TIMEOUT"
                    if type(error).__name__ == "TimeoutError"
                    else "INTERNAL_ERROR"
                )
            )
            if code == "CAPTCHA_REQUIRED":
                self.access_policy.block(task["row"])
            self._update(task_id, state="STOPPED" if control.stopped else "FAILED", error_code=code)
        finally:
            with self.lock:
                task = self.task(task_id)
                self.event(
                    task["row"],
                    task["state"],
                    kind=task["kind"],
                    code=task.get("error_code"),
                    task_id=task_id,
                )
                self.running.discard(task_id)
                self._queue_file_resume(task_id)
                self._queue_database_redownload(task_id)
                self._schedule()

    def _queue_database_redownload(self,task_id):
        task=self.task(task_id)
        if (self.closed or task['kind']!='databases' or task['state']!='FAILED'
                or task.get('error_code')!='DB_INTEGRITY_FAILED'): return False
        control=self.controls.get(task_id)
        if control and (control.stopped or control.paused): return False
        server=self.catalog.get(task['row'],include_deleted=True)
        if server['blocked'] or server['deleted'] or server['binding_revision']!=task['revision']: return False
        latest=next(t for t in self.tasks(task['row']) if t['kind']=='databases')
        if latest['id']!=task_id: return False
        count=task.get('db_redownload_count',0)
        if count>=3:
            self._update(task_id,db_redownload_exhausted=True)
            return False
        due=time.time()+(300,900,3600)[count]
        policy=self.access_policy.snapshot()
        if not policy['blocked']: due=max(due,policy['next_timestamp'])
        when=iso_time(due)
        self._update(task_id,state='SCHEDULED',stage='DB_REDOWNLOAD_WAIT',scheduled_at=when,next_at=when,
                     db_redownload_count=count+1,db_redownload_exhausted=False,
                     last_attempt_result=task.get('result'),result=None,error_code=None)
        self.controls[task_id]=TaskControl(lambda state:self._control_state(task_id,state))
        self.event(task['row'],'DB_REDOWNLOAD_WAIT',kind='databases',code='DB_INTEGRITY_FAILED',task_id=task_id)
        return True

    def _queue_file_resume(self, task_id):
        task = self.task(task_id)
        if self.closed or task['kind'] != 'files' or task['state'] not in {'FAILED','INTERRUPTED'} or task.get('error_code') not in AUTO_FILE_ERRORS:
            return False
        control = self.controls.get(task_id)
        if control and (control.stopped or control.paused): return False
        server = self.catalog.get(task['row'], include_deleted=True)
        if server['blocked'] or server['deleted'] or server['binding_revision'] != task['revision']: return False
        latest = next(t for t in self.tasks(task['row']) if t['kind']=='files')
        if latest['id'] != task_id: return False
        first = task.get('first_started_at') or task.get('started_at')
        if not first: return False
        if time.time() - self._scheduled_timestamp(first) >= 86400:
            self._update(task_id, error_code='TRANSFER_TIMEOUT', auto_resume_at=None)
            return False
        progress = dict(task.get('progress', {}))
        saved = progress.get('received_bytes', 0)
        previous_attempt = task.get('auto_resume_count', 0)
        if saved > task.get('auto_resume_bytes', 0): previous_attempt = 0
        attempt = previous_attempt + 1
        due = min(time.time()+AUTO_FILE_DELAYS[min(attempt-1,len(AUTO_FILE_DELAYS)-1)], self._scheduled_timestamp(first)+86400)
        scheduled = iso_time(due)
        progress.update(active_files=[],active_transfers=0,current_file='',retry_at=None,retry_delay_seconds=0)
        self._update(task_id, state='SCHEDULED', stage='AUTO_RESUME_WAIT', scheduled_at=scheduled,
                     next_at=scheduled, auto_resume_at=scheduled, auto_resume_count=attempt,
                     auto_resume_bytes=saved, auto_resume_error=task['error_code'], first_started_at=first,
                     last_attempt_result=task.get('result'), result=None, error_code=None, progress=progress,
                     ftp_connections=server['ftp_connections'], skip_unsupported_names=server['skip_unsupported_names'])
        self.controls[task_id] = TaskControl(lambda state:self._control_state(task_id,state))
        self.event(task['row'],'AUTO_RESUME_WAIT',kind='files',code=task['error_code'],task_id=task_id)
        return True

    def progress(self, task_id, values):
        with self.lock:
            task = self.task(task_id)
            import re
            repair=values.get('db_repair_job_id')
            if isinstance(repair,str) and re.fullmatch('panel-job-[a-f0-9]{24}',repair):
                self._update(task_id,db_repair_job_id=repair)
            progress = dict(task["progress"])
            for key in (
                'metadata_mlst_checks',
                'metadata_listing_checks',
                'processed_files',
                'local_verifications', 'pending_verifications',
                "received_bytes",
                "planned_bytes",
                "planned_files",
                "verified_files",
                "verified_count",
                "expected_count",
                "directories_seen",
                "current_bytes",
                "current_size",
                "skipped_count",
                "retry_attempt", "retry_limit", "retry_delay_seconds",
                "retry_total", "committed_bytes",
                "connections_limit", "active_transfers", "parallel_limit",
            ):
                if type(values.get(key)) is int and values[key] >= 0:
                    progress[key] = values[key]
            if isinstance(values.get("current_file"), str):
                progress["current_file"] = values["current_file"][:300]
            if type(values.get("inventory_complete")) is bool:
                progress["inventory_complete"] = values["inventory_complete"]
            if isinstance(values.get("active_files"), list):
                progress["active_files"] = [
                    {"path": item["path"][:2000], "size_bytes": item["size_bytes"],
                     "received_bytes": item["received_bytes"]}
                    for item in values["active_files"][:5]
                    if isinstance(item, dict) and isinstance(item.get("path"), str)
                    and type(item.get("size_bytes")) is int and type(item.get("received_bytes")) is int
                    and 0 <= item["received_bytes"] <= item["size_bytes"]
                ]
            if "retry_at" in values and (values["retry_at"] is None or isinstance(values["retry_at"], str)):
                progress["retry_at"] = values["retry_at"]
            stage = values.get("stage", task["stage"])
            if not isinstance(stage, str) or not stage.replace("_", "").isalnum():
                stage = task["stage"]
            self._update(
                task_id,
                progress=progress,
                stage=stage,
                snapshot_date=values.get("snapshot_date", task.get("snapshot_date")),
                workflow_key=values.get("workflow_key", task.get("workflow_key")),
            )
            if stage != task["stage"] or values.get("log"):
                self.event(
                    task["row"],
                    stage,
                    kind=task["kind"],
                    code=values.get("error_code"),
                    task_id=task_id,
                    **progress,
                    **{key: values[key] for key in ("skipped_path", "skip_reason", "skipped_kind") if key in values},
                    **{key: values[key] for key in ("directory", "operation", "ftp_reply_code",
                       "curl_error_code", "curl_error_kind", "progress_advanced",'conflict','action','warning_code') if key in values},
                )

    def close(self):
        with self.lock:
            self.closed = True
            if self.timer:
                self.timer.cancel()
            for control in self.controls.values():
                control.request("stop")
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.guard.__exit__()
