"""Private loopback dashboard. Reading progress never acquires backup locks."""

import ctypes
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from .dashboard_journal import AppendJournal
from .downloads import local_path
from .errors import BackupError
from .importers import identity, jino_rows, parse_jino
from .server_folders import folder_names
from .state import Settings, write_json, private_dir


ERROR_LABELS = {
    "PANEL_RATE_LIMIT": "Ожидает следующего разрешённого входа в панель",
    "PANEL_AUTH_PAUSED": "Входы в панель приостановлены после CAPTCHA",
    "SCHEDULE_INVALID": "Укажите дату и время с часовым поясом",
    "TASK_BUSY": "Порядок можно менять у ожидающих заданий",
    "PROTOCOL_NOT_CONFIGURED": "Выберите нужный протокол и порт в настройках сервера",
    "OPERATION_TIMEOUT": "Превышено время ожидания ответа",
    "FTP_LOGIN_DENIED": "FTP отказал на этапе входа",
    "FTP_ROOT_DENIED": "FTP не разрешил открыть указанный корневой каталог",
    "FTP_DATA_PROTECTION_DENIED": "FTP отклонил защиту канала передачи данных",
    "AUTH_OR_ROOT_FAILED": "FTP отклонил вход или указанный корневой каталог",
    "TLS_HANDSHAKE_FAILED": "Не удалось установить защищённое FTP-соединение",
    "FTP_CREDENTIAL_MISSING": "Проверьте FTP-хост, логин и пароль",
    "FTP_PROFILE_NEEDS_REVIEW": "Параметры FTP требуют проверки",
    "SFTP_CONNECTION_FAILED": "Не удалось подключиться по SFTP",
    "KNOWN_HOSTS_REQUIRED": "Для SFTP укажите файл проверенных ключей сервера",
    "REMOTE_LIST_UNSUPPORTED": "FTP-сервер не вернул поддерживаемый список файлов",
    "REMOTE_LIST_INVALID": "FTP вернул некорректные сведения о файлах",
    "REMOTE_LIST_ENCODING_INVALID": "FTP вернул имена, которые не удалось прочитать в UTF-8",
    "REMOTE_DIRECTORY_UNAVAILABLE": "FTP не разрешил прочитать каталог или каталог недоступен",
    "FTP_LIST_REJECTED": "FTP отклонил запрос списка; код ответа указан в журнале",
    "CONNECTION_LIMIT_INVALID": "Выберите от 2 до 5 FTPS-сеансов на сервер",
    "FTP_ROOT_UNVERIFIED": "FTPS-сеанс не подтвердил разрешённый корень",
    "REMOTE_PATH_UNSAFE": "Обнаружен путь за разрешённым корнем или недопустимое имя",
    "REMOTE_NAME_UNSUPPORTED": "Пропущено имя, несовместимое с Windows",
    "REMOTE_TYPE_UNSAFE": "Обнаружен неподдерживаемый тип удалённого объекта",
    "INTERNAL_ERROR": "Внутренняя ошибка задания. Проверьте его журнал",
    "SERVER_NOT_FOUND": "Сервер удалён из списка или отсутствует",
    "SERVER_BLOCKED": "Сервер заблокирован. Разблокируйте его для запуска",
    "SERVER_BUSY": "Сначала остановите задания сервера",
    "LEGACY_JOB_ACTIVE": "Сервер занят массовой выгрузкой из командной строки",
    "SERVER_CONFIG_INVALID": "Проверьте имя, логин, защищённый протокол, порт и корень",
    "SERVER_CONFIG_CHANGED": "Настройки изменились. Создайте задание с текущими параметрами",
    "TASK_NOT_FOUND": "Задание отсутствует",
    "CANCELLED": "Остановлено оператором. Сохранённые части доступны для продолжения",
    "WORKER_RESTARTED": "Процесс перезапущен. Нажмите «Продолжить»",
    "FOLDER_NAME_AMBIGUOUS": "Логин и FTP-хост уже используются другой папкой сервера",
    "REQUEST_INVALID": "Проверьте заполненные поля",
    "SNAPSHOT_DATE_INVALID": "Укажите дату снимка в формате ГГГГ-ММ-ДД",
    "AUTH_FAILED": "Хостинг отклонил логин или пароль",
    "MFA_REQUIRED": "Требуется подтверждение входа",
    "CAPTCHA_REQUIRED": "Требуется проверка CAPTCHA",
    "ACCOUNT_AMBIGUOUS": "Логин повторяется в исходных данных",
    "CREDENTIAL_MISSING": "Не хватает данных для входа",
    "CREDENTIAL_NOT_RECOVERABLE": "Нет пригодного пароля",
    "CONNECTION_FAILED": "Не удалось подключиться",
    "PANEL_BROWSER_OR_LOGIN_FAILED": "Не удалось открыть панель хостинга",
    "PANEL_RESPONSE_FAILED": "Панель хостинга не ответила корректно",
    "DATABASE_SCOPE_UNVERIFIED": "Выбранная БД отсутствует в подтверждённом списке. Обновите список из Jino",
    "UI_CHANGED": "Интерфейс хостинга изменился",
    "CONTAINER_AMBIGUOUS": "Не удалось однозначно выбрать хостинг",
    "TLS_CERTIFICATE_INVALID": "Сертификат соединения не прошёл проверку",
    "SOURCE_CHANGED": "Файл изменился во время передачи",
    "TRANSFER_DENIED": "Не удалось прочитать файл",
    "TRANSFER_INTERRUPTED": "Соединение прервалось",
    "TRANSFER_TIMEOUT": "Истекло время передачи",
    "EXPORT_TIMEOUT": "Дамп ещё не сформирован за отведённое время",
    "DB_INTEGRITY_FAILED": "Дамп не прошёл проверку",
    "STORAGE_SPACE_LOW": "Недостаточно свободного места",
    "REMOTE_DESTINATION_EXISTS": "В каталоге экспорта уже есть другие файлы",
    "SNAPSHOT_NOT_FOUND": "Нет доступного снимка MySQL",
    "PROFILE_UNVERIFIED": "Связь аккаунта и источника не подтверждена",
    "PROFILE_NOT_FOUND": "Доступ ещё не импортирован",
    "CHECK_RATE_LIMIT": "Повторная проверка доступна через минуту",
    "CHECK_FAILED": "Проверка доступа завершилась ошибкой",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_shared(path, limit=8 * 1024 * 1024, *, tail=False, offset=None):
    """Read immutable configuration or append-only logs, never a live replaceable report."""
    path = Path(path)
    if os.name == "nt":
        import msvcrt

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        kernel.CreateFileW.restype = ctypes.c_void_p
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel.CreateFileW(str(path.absolute()), 0x80000000, 0x7, None, 3, 0x80, None)
        if handle == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_last_error(), "READ_FAILED")
        try:
            fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
        except Exception:
            kernel.CloseHandle(handle)
            raise
        stream = os.fdopen(fd, "rb")
    else:
        stream = path.open("rb")
    with stream:
        if tail:
            stream.seek(0, 2)
            stream.seek(max(0, stream.tell() - limit))
        elif offset is not None:
            stream.seek(offset)
        raw = stream.read(limit if offset is not None else limit + 1)
    if len(raw) > limit:
        raise BackupError("REPORT_TOO_LARGE")
    return raw


def read_json(path, default=None):
    try:
        return json.loads(read_shared(path))
    except (OSError, ValueError, BackupError):
        return default


def pid_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        try:
            code = ctypes.c_uint32()
            return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class DashboardData:
    def __init__(self, state_dir, destination, source, mapping, batch_id, *, process_alive=None):
        self.state_dir = Path(state_dir).resolve()
        self.destination = Path(destination).absolute()
        self.source, self.mapping = Path(source), mapping
        self.batch_id = batch_id
        self.report_path = self.destination / (identity("jino-all", batch_id) + ".json")
        self.file_job = identity("download", batch_id, "https-panel")
        rows = jino_rows(source)
        batch = parse_jino(source, mapping)
        available = {c.profile_id for c in batch.candidates if c.kind == "panel"}
        names = folder_names(rows, mapping)
        self.accounts = []
        for row, (record, folder) in enumerate(zip(rows, names), 1):
            ref = identity("panel", record.get(mapping["id"]))
            login = (
                record.get(mapping.get("login", ""))
                or record.get(mapping.get("ftp_login", ""))
                or ""
            )
            name = record.get(mapping.get("name", ""))
            issue = next(
                (x["code"] for x in batch.report["issues"] if x["row"] == row), "CHECK_FAILED"
            )
            self.accounts.append(
                {
                    "row": row,
                    "name": name[:200]
                    if isinstance(name, str) and name.strip()
                    else "Сервер " + str(row),
                    "login": str(login)[:160],
                    "folder": folder,
                    "account_ref": ref,
                    "server_id": ref.replace("panel-", "server-", 1),
                    "can_check": ref in available,
                    "issue": None if ref in available else issue,
                }
            )
        self.process_alive = process_alive or self._process_alive
        self.monitor = None
        self.management = None
        self.demo = False
        self.active = read_json(self.state_dir / "active-download.json", {})
        self.cache_lock = threading.RLock()
        self.items = {}
        self.events = deque(maxlen=80)
        self.server_events = {}
        self.sql_jobs = {}
        self.sql_readers = {}
        path = self.active.get("stdout")
        self.journal = None
        if isinstance(path, str):
            path = Path(path)
            if not path.is_absolute():
                path = self.state_dir.parent / path
            if path.resolve().is_relative_to(self.state_dir):
                self.journal = AppendJournal(
                    path, lambda *args, **kwargs: read_shared(*args, **kwargs)
                )
        self.ftp_history = {}
        previous = self.destination / "download-dcc4059959a9fe22683715ca.json"
        historical = read_json(previous, {})
        if isinstance(historical, dict) and historical.get("finished"):
            self.ftp_history = {item.get("row"): item for item in historical.get("servers", [])}

    def enable_management(self):
        if self.management is None:
            from .dashboard_management import DashboardManagement

            self.management = DashboardManagement(self)
        return self.management

    def _process_alive(self):
        return self.active.get("batch_id") == self.batch_id and pid_alive(self.active.get("pid"))

    def account(self, row):
        if self.management:
            return self.management.catalog.get(row)
        if not isinstance(row, int) or not 1 <= row <= len(self.accounts):
            raise BackupError("SERVER_NOT_FOUND")
        return self.accounts[row - 1]

    def report(self):
        with self.cache_lock:
            if self.journal:
                generation = self.journal.generation
                updates = list(self.journal.updates())
                if generation != self.journal.generation:
                    self.items.clear()
                    self.events.clear()
                    self.server_events.clear()
                for value in updates:
                    row = value.get("row")
                    if not isinstance(row, int) or not 1 <= row <= len(self.accounts):
                        continue
                    allowed = {
                        "status",
                        "stage",
                        "db_stage",
                        "database_status",
                        "databases_count",
                        "source_snapshot_date",
                        "files_seen",
                        "bytes_saved",
                        "files",
                        "databases",
                        "error_code",
                        "exception_type",
                    }
                    item = self.items.setdefault(row, {"row": row})
                    item.update({k: v for k, v in value.items() if k in allowed})
                    try:
                        timestamp = datetime.fromisoformat(value["time"]).isoformat()
                        item["updated_at"] = timestamp
                        if value.get("stage") == "ACCESS_VERIFIED":
                            item["access_checked_at"] = timestamp
                    except (KeyError, TypeError, ValueError):
                        pass
                    self._add_event(value)
            available = {a["row"] for a in self.accounts if a["can_check"]}
            finished = bool(self.items) and all(
                self.items.get(row, {}).get("status")
                in {"FILES_ONLY", "DB_ONLY", "PARTIAL", "FAILED", "COMPLETE"}
                for row in available
            )
            return {
                "servers": list(self.items.values()),
                "finished": finished,
                "read_error": not bool(self.items),
                "started_at": self.active.get("started_at"),
            }

    def is_active(self, row):
        if self.management and self.management.busy(row):
            return True
        return self.process_alive() and any(
            x.get("row") == row and x.get("status") == "RUNNING"
            for x in self.report().get("servers", [])
        )

    def _files(self, account, item):
        result = {
            "received_bytes": int(item.get("bytes_saved", 0) or 0),
            "verified_files": 0,
            "current_file": None,
            "updated_at": None,
            "metadata_available": False,
        }
        try:
            root = local_path(
                self.destination,
                account.get("legacy_folder", account["folder"]) + "/" + self.file_job,
            )
            db_path = local_path(root, ".jino-progress/progress.sqlite3")
            if not db_path.is_file():
                return result
            db = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, timeout=0.1)
            try:
                aggregate = db.execute(
                    "SELECT COALESCE(SUM(committed),0),SUM(CASE WHEN state='done' THEN 1 ELSE 0 END) FROM files"
                ).fetchone()
                pending = db.execute(
                    "SELECT path,size,committed FROM files WHERE state='partial' ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
            finally:
                db.close()
            result.update(
                received_bytes=aggregate[0],
                verified_files=aggregate[1] or 0,
                metadata_available=True,
                updated_at=datetime.fromtimestamp(
                    db_path.stat().st_mtime, timezone.utc
                ).isoformat(),
            )
            if pending:
                path, size, committed = pending
                result["current_file"] = {
                    "name": Path(path).name,
                    "relative_path": path,
                    "size_bytes": size,
                    "received_bytes": committed,
                    "percent": round(min(100, 100 * committed / size), 1) if size > 0 else None,
                }
        except (OSError, sqlite3.Error, BackupError):
            result["read_error"] = True
        return result

    def _database_index(self):
        import re

        for path in (self.state_dir / "panel_jobs").glob("panel-job-*.events.jsonl"):
            job_id = path.name.removesuffix(".events.jsonl")
            if not re.fullmatch(r"panel-job-[a-f0-9]{24}", job_id):
                continue
            reader = self.sql_readers.setdefault(
                job_id, AppendJournal(path, lambda *a, **k: read_shared(*a, **k))
            )
            generation = reader.generation
            updates = list(reader.updates())
            if generation != reader.generation:
                self.sql_jobs.pop(job_id, None)
            for event in updates:
                job = self.sql_jobs.setdefault(job_id, {"entries": {}, "finished": False})
                stage = event.get("stage")
                if stage == "SNAPSHOT_SELECTED" and isinstance(event.get("databases_count"), int):
                    job["expected_count"] = event["databases_count"]
                if stage in {"DB_ONLY", "ALREADY_VERIFIED"}:
                    job["finished"] = True
                alias = event.get("db_alias")
                if not isinstance(alias, str) or not re.fullmatch(r"db-[a-f0-9]{24}", alias):
                    continue
                entry = job["entries"].setdefault(alias, {})
                entry["stage"] = stage
                if stage == "SQL_VERIFIED" and isinstance(event.get("size_bytes"), int):
                    entry.update(verified=True, size_bytes=event["size_bytes"])
        return self.sql_jobs

    def _databases(self, account, item, index):
        db = item.get("databases", {})
        result = {
            "verified_count": int(item.get("databases_count", db.get("databases_count", 0)) or 0),
            "snapshot_date": item.get("source_snapshot_date", db.get("source_snapshot_date")),
            "state": item.get("database_status", db.get("status", "UNKNOWN")),
            "current_file": None,
            "snapshot_stale": False,
            "received_bytes": 0,
        }
        job_id = db.get("job_id")
        if not job_id and result["snapshot_date"]:
            job_id = identity(
                "panel-job", account["account_ref"], self.batch_id + "-" + result["snapshot_date"]
            )
        job = index.get(job_id)
        if job:
            entries = job["entries"]
            result["expected_count"] = job.get("expected_count")
            result["verified_count"] = sum(bool(x.get("verified")) for x in entries.values())
            result["received_bytes"] = sum(
                x.get("size_bytes", 0) for x in entries.values() if x.get("verified")
            )
            if job["finished"]:
                result["state"] = "DB_ONLY"
            for alias, entry in entries.items():
                if entry.get("verified"):
                    continue
                try:
                    directory = local_path(
                        self.destination,
                        account.get("legacy_folder", account["folder"])
                        + "/"
                        + job_id
                        + "/databases",
                    )
                    parts = [
                        local_path(directory, p.name)
                        for p in directory.glob(alias + ".sql.partial*")
                    ]
                    partial = max(parts, key=lambda p: p.stat().st_mtime) if parts else None
                    received = partial.stat().st_size if partial else 0
                    result["received_bytes"] += received
                    result["current_file"] = {
                        "name": "Дамп базы данных",
                        "size_bytes": None,
                        "received_bytes": received,
                        "percent": None,
                        "updated_at": datetime.fromtimestamp(
                            partial.stat().st_mtime, timezone.utc
                        ).isoformat()
                        if partial
                        else None,
                    }
                except (OSError, BackupError):
                    pass
        if result["snapshot_date"]:
            result["snapshot_stale"] = (
                result["snapshot_date"] < datetime.now(timezone.utc).date().isoformat()
            )
        return result

    def snapshot(self):
        with self.cache_lock:
            return self._snapshot()

    def _snapshot(self):
        if self.management:
            self.accounts = self.management.catalog.all()
        report = self.report()
        items = {x.get("row"): x for x in report.get("servers", [])}
        alive = bool(self.process_alive())
        database_index = self._database_index()
        checks = self.monitor.snapshot() if self.monitor else {}
        servers = []
        for account in self.accounts:
            if account.get("deleted"):
                continue
            row = account["row"]
            item = items.get(row, {})
            status = item.get(
                "status",
                "UNKNOWN"
                if report.get("read_error") and account["can_check"]
                else "QUEUED"
                if account["can_check"]
                else "NEEDS_REVIEW",
            )
            progress = self._files(account, item)
            database = self._databases(account, item, database_index)
            available = {
                "state": "UNKNOWN",
                "checked_at": None,
                "message": "Доступ ещё не проверялся",
            }
            check = checks.get(str(row))
            if check:
                available = {k: check.get(k) for k in ("state", "checked_at", "message")}
            if not account["can_check"]:
                available = {
                    "state": "NEEDS_REVIEW",
                    "checked_at": None,
                    "message": ERROR_LABELS.get(account["issue"], "Требуется проверка данных"),
                }
            elif (
                item.get("stage") in {"ACCESS_VERIFIED", "DATABASES", "FILES_DOWNLOAD"}
                or "files" in item
                or item.get("source_snapshot_date")
                or item.get("files_seen", 0) > 0
            ):
                if available["state"] not in {
                    "CHECKING",
                    "AVAILABLE",
                    "UNAVAILABLE",
                    "AUTH_ERROR",
                    "ATTENTION",
                }:
                    available = {
                        "state": "CONFIRMED",
                        "checked_at": item.get("access_checked_at"),
                        "message": "Доступ подтверждён текущим заданием",
                    }
            code = (
                item.get("error_code")
                or item.get("files", {}).get("error_code")
                or item.get("databases", {}).get("error_code")
            )
            if code in {"AUTH_FAILED", "MFA_REQUIRED", "CAPTCHA_REQUIRED"} and not check:
                available = {
                    "state": "AUTH_ERROR" if code == "AUTH_FAILED" else "ATTENTION",
                    "checked_at": item.get("updated_at"),
                    "message": ERROR_LABELS[code],
                }
            if code in {"CONNECTION_FAILED", "PANEL_BROWSER_OR_LOGIN_FAILED"} and not check:
                available = {
                    "state": "UNAVAILABLE",
                    "checked_at": item.get("updated_at"),
                    "message": ERROR_LABELS[code],
                }
            current = (
                database.get("current_file")
                if item.get("stage") == "DATABASES"
                else progress.get("current_file")
            )
            recent = (current or {}).get("updated_at") or progress.get("updated_at")
            if status == "RUNNING" and alive and recent:
                try:
                    if (
                        datetime.now(timezone.utc) - datetime.fromisoformat(recent)
                    ).total_seconds() < 90:
                        available = {
                            "state": "TRANSFERRING",
                            "checked_at": recent,
                            "message": "Есть обновления данных выгрузки",
                        }
                except (ValueError, TypeError):
                    pass
            job_state = "STOPPED" if status == "RUNNING" and not alive else status
            folder = local_path(self.destination, account["folder"])
            servers.append(
                {
                    "row": row,
                    "name": account["name"],
                    "login": account["login"],
                    "status": job_state,
                    "stage": item.get("stage", "WAITING"),
                    "db_stage": item.get("db_stage"),
                    "availability": available,
                    "progress": progress,
                    "databases": database,
                    "folder_path": str(folder),
                    "folder_exists": folder.is_dir(),
                    "can_check": account["can_check"],
                    "busy": status == "RUNNING" and alive,
                    "error_message": ERROR_LABELS.get(code, "Ошибка задания; подробности в журнале")
                    if code
                    else None,
                    "file_errors_count": item.get("files", {}).get("file_errors_count", 0),
                }
            )
            server = servers[-1]
            server["blocked"] = account.get("blocked", False)
            server["configuration"] = {
                key: account.get(key)
                for key in (
                    "name",
                    "login",
                    "panel_login",
                    "ftp_host",
                    "ftp_login",
                    "protocol",
                    "ftp_port",
                    "remote_root",
                    "known_hosts",
                    "revision",
                    "ftp_ready",
                    "skip_unsupported_names",
                    "ftp_connections",
                )
            }
            server["configuration"]["panel_credentials_saved"] = account.get(
                "has_panel_password", False
            )
            server["configuration"]["ftp_credentials_saved"] = account.get(
                "has_ftp_password", False
            )
            server["tasks"] = self.management.tasks(row) if self.management else []
            for task in server["tasks"]:
                task["error_message"] = (
                    ERROR_LABELS.get(task.get("error_code"), "Ошибка задания")
                    if task.get("error_code")
                    else None
                )
            server["diagnostics"] = []
            sources = [
                ("Панель", item),
                ("Файлы HTTPS", item.get("files", {})),
                ("Базы данных", item.get("databases", {})),
                ("FTP", self.ftp_history.get(row, {})),
                ("Проверка панели", checks.get(str(row), {})),
            ]
            for source, result in sources:
                error = result.get("error_code")
                if error == "INTERNAL_ERROR" and result.get("exception_type") == "TimeoutError":
                    error = "OPERATION_TIMEOUT"
                if error:
                    server["diagnostics"].append(
                        {
                            "source": source,
                            "code": error,
                            "message": ERROR_LABELS.get(error, "Ошибка операции"),
                        }
                    )
            latest = {}
            for task in server["tasks"]:
                latest.setdefault(task["kind"], task)
            for task in latest.values():
                if task.get("error_code") and task["error_code"] not in {
                    "CANCELLED",
                    "WORKER_RESTARTED",
                }:
                    error = task["error_code"]
                    server["diagnostics"].append(
                        {
                            "source": "Файлы" if task["kind"] == "files" else "Базы данных",
                            "code": error,
                            "message": ERROR_LABELS.get(error, "Ошибка задания"),
                        }
                    )
            if account.get("source_row") is None and self.management:
                server["status"] = "IDLE"
            if server["blocked"]:
                server["status"] = "BLOCKED"
                server["availability"] = {
                    "state": "BLOCKED",
                    "message": "Сервер заблокирован оператором",
                    "checked_at": None,
                }
            server["latest_tasks"] = latest
            active_tasks = [
                task
                for task in latest.values()
                if task["state"]
                in {
                    "RUNNING",
                    "PAUSING",
                    "PAUSED",
                    "QUEUED",
                    "STOPPING",
                    "SCHEDULED",
                    "WAITING_RATE_LIMIT",
                    "WAITING_OPERATOR",
                }
            ]
            if active_tasks and not server["blocked"]:
                chosen = next(
                    (task for task in active_tasks if task["state"] == "RUNNING"), active_tasks[0]
                )
                server["status"] = chosen["state"]
                server["stage"] = chosen["stage"] if chosen["stage"] in {"RETRY_WAIT", "RECONNECT", "RETRY_EXHAUSTED"} else ("DATABASES" if chosen["kind"] == "databases" else "FILES_DOWNLOAD")
                server["busy"] = True
            elif latest and not server["blocked"]:
                chosen = max(latest.values(), key=lambda task: task["updated_at"])
                server["status"] = (chosen.get("result") or {}).get("status", chosen["state"])
                server["busy"] = False
            server["previous_progress"] = server["progress"]
            server["previous_databases"] = server["databases"]
            file_task = latest.get("files")
            if file_task:
                current = file_task["progress"]
                size = current.get("current_size")
                file = None
                if current.get("current_file"):
                    file = {
                        "name": current["current_file"],
                        "size_bytes": size,
                        "received_bytes": current.get("current_bytes", 0),
                        "percent": round(min(100, 100 * current.get("current_bytes", 0) / size), 1)
                        if size
                        else None,
                    }
                server["progress"] = {
                    "received_bytes": current.get("received_bytes", 0),
                    "verified_files": current.get("verified_files", 0),
                    "current_file": file,
                    "updated_at": file_task["updated_at"],
                }
            db_task = latest.get("databases")
            if db_task:
                current = db_task["progress"]
                server["databases"] = {
                    "received_bytes": current.get("received_bytes", 0),
                    "verified_count": current.get("verified_count", 0),
                    "expected_count": current.get("expected_count"),
                    "snapshot_date": db_task.get("snapshot_date"),
                    "snapshot_stale": bool(
                        db_task.get("snapshot_date")
                        and db_task["snapshot_date"] < datetime.now(timezone.utc).date().isoformat()
                    ),
                    "current_file": None,
                    "state": (db_task.get("result") or {}).get("status", db_task["state"]),
                }
        summary = {
            "total": len(servers),
            "active": sum(x["status"] == "RUNNING" for x in servers),
            "queued": sum(
                x["status"] in {"QUEUED", "SCHEDULED", "WAITING_RATE_LIMIT", "WAITING_OPERATOR"}
                for x in servers
            ),
            "processed": sum(
                x["status"] in {"FILES_ONLY", "DB_ONLY", "PARTIAL", "FAILED", "COMPLETE"}
                for x in servers
            ),
            "errors": sum(x["status"] == "FAILED" for x in servers),
            "needs_review": sum(x["status"] == "NEEDS_REVIEW" for x in servers),
            "received_bytes": sum(
                x["progress"]["received_bytes"] + x["databases"]["received_bytes"] for x in servers
            ),
            "verified_files": sum(x["progress"]["verified_files"] for x in servers),
            "verified_databases": sum(x["databases"]["verified_count"] for x in servers),
        }
        latest_tasks = {}
        for server in servers:
            for task in server["tasks"]:
                latest_tasks.setdefault((task["row"], task["kind"]), task)
        tasks = list(latest_tasks.values())
        if tasks:
            completed = sum(
                task["state"] in {"FINISHED", "FAILED", "STOPPED", "INTERRUPTED"} for task in tasks
            )
            total = len(tasks)
            scope = "tasks"
        else:
            completed = summary["processed"]
            total = sum(
                server["status"] not in {"NEEDS_REVIEW", "BLOCKED", "IDLE"} for server in servers
            )
            scope = "accounts"
        overall = {
            "scope": scope,
            "completed": completed,
            "total": total,
            "percent": round(completed * 100 / total, 1) if total else 0,
            "failed": sum(task["state"] == "FAILED" for task in tasks)
            if tasks
            else summary["errors"],
            "running": sum(task["state"] == "RUNNING" for task in tasks),
            "paused": sum(task["state"] in {"PAUSED", "PAUSING"} for task in tasks),
        }
        file_tasks = [task for task in tasks if task["kind"] == "files"]
        planned = sum(task["progress"].get("planned_bytes", 0) for task in file_tasks)
        received = sum(task["progress"].get("received_bytes", 0) for task in file_tasks)
        known = bool(file_tasks) and all(
            task["progress"].get("inventory_complete") for task in file_tasks
        )
        overall["files"] = {
            "received_bytes": received
            if file_tasks
            else sum(server["progress"]["received_bytes"] for server in servers),
            "planned_bytes": planned if known else None,
            "percent": round(min(100, received * 100 / planned), 1) if known and planned else None,
        }
        return {
            "generated_at": utc_now(),
            "ftp_connections": self.management.runner.budget.snapshot()
            if self.management and hasattr(self.management.runner, "budget") else {"used": 0, "limit": 10},
            "report_updated_at": datetime.fromtimestamp(
                self.report_path.stat().st_mtime, timezone.utc
            ).isoformat()
            if self.report_path.exists()
            else None,
            "worker": {
                "state": (
                    "RUNNING"
                    if any(task["state"] in {"RUNNING", "PAUSING", "STOPPING"} for task in tasks)
                    else "WAITING"
                    if any(
                        task["state"]
                        in {"QUEUED", "SCHEDULED", "WAITING_RATE_LIMIT", "WAITING_OPERATOR"}
                        for task in tasks
                    )
                    else "PAUSED"
                    if any(task["state"] == "PAUSED" for task in tasks)
                    else "FINISHED"
                )
                if tasks
                else ("FINISHED" if report.get("finished") else "RUNNING" if alive else "STOPPED")
            },
            "summary": summary,
            "servers": servers,
            "destination": str(self.destination),
            "report_found": self.report_path.is_file(),
            "report_error": bool(report.get("read_error")) and not tasks,
            "checks_pending": sum(
                x.get("state")
                in {"CHECKING", "WAITING_RATE_LIMIT", "WAITING_OPERATOR", "WAITING_JOB"}
                for x in checks.values()
            ),
            "events": list(self.events)[::-1],
            "demo": self.demo,
            "overall": overall,
        }

    def server_detail(self, row):
        self.account(row)
        snapshot = self.snapshot()
        server = next((item for item in snapshot["servers"] if item["row"] == row), None)
        if server is None:
            raise BackupError("SERVER_NOT_FOUND")
        if self.management:
            config = self.management.catalog.get(row)
            server['database_inventory'] = self.management.inventory.list(row, config['binding_revision'])
            server['databases']['verified_count'] = max(server['databases'].get('verified_count', 0), sum(x.get('validation') == 'PASS' for x in server['database_inventory']))
        events = list(self.server_events.get(row, []))
        if self.management:
            for event in self.management.events(row):
                event["message"] = ERROR_LABELS.get(event.get("error_code"))
                events.append(event)
        events.sort(key=lambda event: event.get("time", ""), reverse=True)
        skipped_events = self.management.events(row, limit=500, stage="FILE_SKIPPED") if self.management else []
        for event in skipped_events:
            event["message"] = ERROR_LABELS.get(event.get("error_code"))
        directory_events = self.management.events(row, limit=500, stage="DIRECTORY_ERROR") if self.management else []
        for event in directory_events:
            event["message"] = ERROR_LABELS.get(event.get("error_code"))
        return {"server": server, "events": events[:500], "skipped_events": skipped_events,
                "directory_events": directory_events}

    def work_plan(self):
        plan = self.enable_management().plan()
        report = self.report()
        accounts = {account["row"]: account for account in self.management.catalog.all()}
        for item in report["servers"]:
            if item.get("stage") != "FINISHED" or item.get("row") not in accounts:
                continue
            account = accounts[item["row"]]
            children = [
                (kind, item.get(field))
                for kind, field in (("files", "files"), ("databases", "databases"))
                if isinstance(item.get(field), dict)
            ]
            if not children:
                children = [("access", item)]
            for kind, result in children:
                state = (
                    "FINISHED"
                    if result.get("status") in {"FILES_ONLY", "DB_ONLY", "PARTIAL"}
                    else "FAILED"
                )
                code = result.get("error_code")
                if code == "INTERNAL_ERROR" and result.get("exception_type") == "TimeoutError":
                    code = "OPERATION_TIMEOUT"
                plan["completed"].append(
                    {
                        "id": "history-" + str(account["row"]) + "-" + kind,
                        "row": account["row"],
                        "kind": kind,
                        "transport": "https_panel",
                        "server_name": account["name"],
                        "server_login": account["login"],
                        "server_deleted": account["deleted"],
                        "server_blocked": account["blocked"],
                        "state": state,
                        "result": {"status": result.get("status")},
                        "updated_at": item.get("updated_at") or report.get("started_at"),
                        "priority": "normal",
                        "progress": {},
                        "error_code": code,
                        "historical": True,
                    }
                )
        ftp_time = None
        if self.ftp_history:
            path = self.destination / "download-dcc4059959a9fe22683715ca.json"
            ftp_time = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        for row, result in self.ftp_history.items():
            if row not in accounts:
                continue
            account = accounts[row]
            plan["completed"].append(
                {
                    "id": "history-ftp-" + str(row),
                    "row": row,
                    "kind": "files",
                    "transport": "ftps",
                    "server_name": account["name"],
                    "server_login": account["login"],
                    "server_deleted": account["deleted"],
                    "server_blocked": account["blocked"],
                    "state": "FINISHED" if result.get("status") == "FILES_ONLY" else "FAILED",
                    "result": {"status": result.get("status")},
                    "updated_at": ftp_time,
                    "priority": "normal",
                    "progress": {},
                    "error_code": result.get("error_code"),
                    "historical": True,
                    "report_timestamp": True,
                }
            )
        for task in plan["pending"] + plan["completed"]:
            task["error_message"] = (
                ERROR_LABELS.get(task.get("error_code"), "Ошибка задания")
                if task.get("error_code")
                else None
            )
        plan["completed"].sort(key=lambda task: task.get("updated_at") or "", reverse=True)
        return plan

    def _add_event(self, value):
        labels = {
            "AUTHENTICATE": "Вход на хостинг",
            "ACCESS_VERIFIED": "Доступ подтверждён",
            "DATABASES": "Получение дампов БД",
            "FILES_DOWNLOAD": "Копирование файлов",
            "FINISHED": "Обработка аккаунта закончена",
            "RECONNECT": "Переподключение",
            "EXPORT_SUBMITTING": "Заказана выгрузка БД",
            "EXPORT_WAIT": "Ожидание SQL-файла",
            "DOWNLOAD": "Скачивание SQL-файла",
            "SQL_VERIFIED": "SQL-файл проверен",
            "REMOTE_EXPORT_RETAINED": "Дамп на хостинге сохранён",
            "FAILED": "Этап завершился ошибкой",
            "DB_ONLY": "Дампы аккаунта проверены",
        }
        try:
            row = value.get("row")
            message = labels.get(value.get("db_stage")) or labels.get(value.get("stage"))
            if not isinstance(row, int) or not message:
                return
            timestamp = datetime.fromisoformat(value["time"]).isoformat()
            event = {
                "row": row,
                "name": self.account(row)["name"],
                "time": timestamp,
                "message": ERROR_LABELS.get(value.get("error_code"), message),
                "stage": value.get("db_stage") or value.get("stage"),
                "error_code": value.get("error_code"),
            }
            self.events.append(event)
            self.server_events.setdefault(row, deque(maxlen=500)).append(event)
        except (ValueError, KeyError, BackupError, TypeError):
            pass


class AccessMonitor:
    def __init__(self, data, checker=None):
        self.data = data
        data.monitor = self
        self.path = data.state_dir / "dashboard-health.json"
        self.states = read_json(self.path, {})
        if not isinstance(self.states, dict):
            self.states = {}
        for key, value in list(self.states.items()):
            if not isinstance(value, dict) or value.get("state") == "CHECKING":
                self.states[key] = {
                    "state": "UNKNOWN",
                    "checked_at": None,
                    "message": "Предыдущая проверка не закончена",
                }
        self.checker = checker or self._check
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="availability")
        self.timer = None
        self.closed = False
        self._schedule_retry()

    def snapshot(self):
        with self.lock:
            return {k: dict(v) for k, v in self.states.items()}

    def invalidate(self, row):
        with self.lock:
            self.states.pop(str(row), None)
            write_json(self.path, self.states)

    def enqueue(self, row):
        account = self.data.account(row)
        if not account["can_check"]:
            raise BackupError("ACCOUNT_AMBIGUOUS")
        if self.data.is_active(row):
            if self.data.management:
                return {"queued": False, "using_active_job": True}
            return {"queued": False, "using_active_transfer": True}
        with self.lock:
            previous = self.states.get(str(row), {})
            if previous.get("state") in {
                "CHECKING",
                "WAITING_RATE_LIMIT",
                "WAITING_OPERATOR",
                "WAITING_JOB",
            }:
                return {"queued": True, "already_queued": True}
            if previous.get("checked_at"):
                elapsed = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(previous["checked_at"])
                ).total_seconds()
                if elapsed < 60:
                    raise BackupError("CHECK_RATE_LIMIT")
            self.states[str(row)] = {
                "state": "CHECKING",
                "checked_at": None,
                "message": "Проверка в очереди",
            }
            private_dir(self.path.parent)
            write_json(self.path, self.states)
        self.executor.submit(self._run, row)
        return {"queued": True}

    def _run(self, row):
        if self.closed:
            return
        management = self.data.management
        if management:
            pending = management.plan()["pending"]
            if management.busy(row) or any(management._uses_panel(task) for task in pending):
                self._defer(row, "WAITING_JOB", "Проверка ожидает завершения заданий панели")
                return
            try:
                management.access_policy.reserve(row, "check")
            except BackupError as error:
                if error.code not in {"PANEL_RATE_LIMIT", "PANEL_AUTH_PAUSED"}:
                    raise
                policy = management.access_policy.snapshot()
                self._defer(
                    row,
                    "WAITING_OPERATOR" if policy["blocked"] else "WAITING_RATE_LIMIT",
                    ERROR_LABELS[error.code],
                    policy["next_at"],
                )
                return
        if self.data.is_active(row):
            result = {"state": "CONFIRMED", "message": "Доступ оценивается по текущей передаче"}
        else:
            try:
                self.checker(row)
                result = {
                    "state": "AVAILABLE",
                    "message": "Вход и чтение списка файлов подтверждены",
                }
            except Exception as exc:
                code = (
                    exc.code
                    if isinstance(exc, BackupError) and exc.code in ERROR_LABELS
                    else "CHECK_FAILED"
                )
                result = {
                    "state": "AUTH_ERROR"
                    if code == "AUTH_FAILED"
                    else "ATTENTION"
                    if code in {"MFA_REQUIRED", "CAPTCHA_REQUIRED", "UI_CHANGED", "CHECK_FAILED"}
                    else "UNAVAILABLE",
                    "message": ERROR_LABELS[code],
                    "error_code": code,
                }
                if management and code == "CAPTCHA_REQUIRED":
                    management.access_policy.block(row)
                    management._wake()
        with self.lock:
            result["checked_at"] = utc_now()
            self.states[str(row)] = result
            write_json(self.path, self.states)
        self._schedule_retry()

    def _defer(self, row, state, message, next_at=None):
        with self.lock:
            self.states[str(row)] = {
                "state": state,
                "message": message,
                "next_at": next_at,
                "checked_at": None,
            }
            write_json(self.path, self.states)
        self._schedule_retry()

    def _schedule_retry(self):
        with self.lock:
            if (
                self.closed
                or self.timer
                or not any(
                    value.get("state") in {"WAITING_JOB", "WAITING_RATE_LIMIT"}
                    for value in self.states.values()
                )
            ):
                return
            self.timer = threading.Timer(60, self.retry_waiting)
            self.timer.daemon = True
            self.timer.start()

    def retry_waiting(self):
        with self.lock:
            self.timer = None
            if self.closed:
                return
            row = next(
                (
                    int(key)
                    for key, value in self.states.items()
                    if value.get("state")
                    in {"WAITING_JOB", "WAITING_RATE_LIMIT", "WAITING_OPERATOR"}
                ),
                None,
            )
            if row is not None:
                self.states[str(row)]["state"] = "CHECKING"
        if row is not None:
            self.executor.submit(self._run, row)

    def _check(self, row):
        from .panel import JinoPanelBrowser, select_panel_source
        from .state import Repository

        if self.data.management:
            credentials = self.data.management.catalog.credentials(row)["panel"]
            with JinoPanelBrowser(credentials, timeout_seconds=90) as panel:
                panel.inspect_files(prepare_transport=False)
            return
        selected = select_panel_source(self.data.source, self.data.mapping, row)
        settings = Settings.load(self.data.state_dir)
        repo = Repository(settings.state_dir / "registry.sqlite3", settings.read_key())
        repo.import_profiles([c for c in selected.candidates if c.kind == "panel"])
        with JinoPanelBrowser(repo.credentials(selected.account_ref), timeout_seconds=90) as panel:
            panel.inspect_files(prepare_transport=False)

    def close(self):
        with self.lock:
            self.closed = True
            if self.timer:
                self.timer.cancel()
        self.executor.shutdown(wait=False, cancel_futures=True)


def make_server(data, *, port=8765):
    assets = Path(__file__).parent / "web"
    token = secrets.token_urlsafe(32)
    monitor = data.monitor or AccessMonitor(data)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # URLs, account names and identifiers never enter HTTP logs.

        def allowed_host(self):
            return self.headers.get("Host") in {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }

        def authorized(self):
            try:
                cookie = SimpleCookie(self.headers.get("Cookie", ""))
                value = cookie.get("jino_session")
                return value is not None and hmac.compare_digest(value.value, token)
            except Exception:
                return False

        def send_body(
            self, body, status=200, content_type="application/json; charset=utf-8", cookie=False
        ):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
            )
            if cookie:
                self.send_header(
                    "Set-Cookie", "jino_session=" + token + "; HttpOnly; SameSite=Strict; Path=/"
                )
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def send_json(self, value, status=200):
            self.send_body(json.dumps(value, ensure_ascii=False).encode(), status)

        def do_GET(self):
            if not self.allowed_host():
                return self.send_json({"error": "HOST_FORBIDDEN"}, 403)
            path = urlsplit(self.path).path
            static = {
                "/": ("index.html", "text/html; charset=utf-8"),
                "/assets/app.js": ("app.js", "text/javascript; charset=utf-8"),
                "/assets/style.css": ("style.css", "text/css; charset=utf-8"),
                "/assets/manage.js": ("manage.js", "text/javascript; charset=utf-8"),
                "/assets/plan.js": ("plan.js", "text/javascript; charset=utf-8"),
                "/favicon.svg": ("favicon.svg", "image/svg+xml"),
            }
            if path in static:
                file, content_type = static[path]
                try:
                    body = (assets / file).read_bytes()
                except OSError:
                    return self.send_json({"error": "ASSET_NOT_FOUND"}, 404)
                return self.send_body(body, content_type=content_type, cookie=path == "/")
            detail = re.fullmatch(r"/api/servers/(\d+)", path)
            if path not in {"/api/state", "/api/plan"} and not detail:
                return self.send_json({"error": "NOT_FOUND"}, 404)
            if not self.authorized():
                return self.send_json({"error": "SESSION_REQUIRED"}, 401)
            try:
                if path == "/api/plan":
                    return self.send_json(data.work_plan())
                self.send_json(data.server_detail(int(detail[1])) if detail else data.snapshot())
            except BackupError as exc:
                code = exc.code if exc.code in ERROR_LABELS else "CHECK_FAILED"
                self.send_json({"error": code, "message": ERROR_LABELS[code]}, 404)
            except Exception:
                self.send_json({"error": "PROGRESS_READ_FAILED"}, 503)

        def do_POST(self):
            if not self.allowed_host():
                return self.send_json({"error": "HOST_FORBIDDEN"}, 403)
            expected = {
                "http://127.0.0.1:" + str(self.server.server_port),
                "http://localhost:" + str(self.server.server_port),
            }
            if not self.authorized() or self.headers.get("Origin") not in expected:
                return self.send_json({"error": "REQUEST_FORBIDDEN"}, 403)
            path = urlsplit(self.path).path
            server_route = re.fullmatch(
                r"/api/servers/(\d+)(?:/(jobs|database-schedules|block|unblock|delete))?", path
            )
            task_route = re.fullmatch(
                r"/api/tasks/(operator-[a-f0-9]{24})/(pause|resume|stop|move|schedule|priority)",
                path,
            )
            if (
                path not in {"/api/checks", "/api/servers", "/api/panel-policy/resume"}
                and not server_route
                and not task_route
            ):
                return self.send_json({"error": "NOT_FOUND"}, 404)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16384:
                    raise ValueError
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError
                if path != "/api/checks":
                    management = data.enable_management()
                    if path == "/api/panel-policy/resume":
                        result = management.resume_panel_queue()
                    elif task_route and task_route[2] in {"move", "schedule", "priority"}:
                        result = management.change(task_route[1], task_route[2], payload)
                    elif task_route:
                        result = management.action(task_route[1], task_route[2])
                    elif path == "/api/servers":
                        result = management.save_server(None, payload)
                    else:
                        row = int(server_route[1])
                        action = server_route[2]
                        if action == "jobs":
                            result = management.start(row, payload.get("kind"), payload)
                        elif action == 'database-schedules':
                            with management.lock:
                                config = management.catalog.get(row)
                                result = management.inventory.schedule(row, config['binding_revision'], payload)
                                management._schedule()
                        elif action:
                            result = management.server_action(row, action)
                        else:
                            result = management.save_server(row, payload)
                    return self.send_json(result, 202)
                rows = (
                    [a["row"] for a in data.accounts if a["can_check"]]
                    if payload.get("all") is True
                    else [payload["row"]]
                )
                results = []
                for row in rows:
                    try:
                        results.append({"row": row, **monitor.enqueue(row)})
                    except BackupError as exc:
                        if len(rows) == 1:
                            raise
                        results.append(
                            {
                                "row": row,
                                "queued": False,
                                "error": exc.code if exc.code in ERROR_LABELS else "CHECK_FAILED",
                            }
                        )
                self.send_json({"results": results}, 202)
            except BackupError as exc:
                code = exc.code if exc.code in ERROR_LABELS else "CHECK_FAILED"
                self.send_json(
                    {"error": code, "message": ERROR_LABELS[code]},
                    429 if code == "CHECK_RATE_LIMIT" else 409,
                )
            except Exception:
                self.send_json({"error": "REQUEST_INVALID"}, 400)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.monitor = monitor
    original_close = server.server_close

    def close():
        monitor.close()
        if data.management:
            data.management.close()
        original_close()

    server.server_close = close
    return server
