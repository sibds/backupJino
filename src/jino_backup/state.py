"""Local configuration, encrypted credential records and transactional job registry."""

import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from cryptography.fernet import Fernet

from .errors import BackupError
from .secrets import SQLiteSecrets

TERMINAL = {"FILES_ONLY", "COMPLETE", "DB_ONLY", "STALE_SNAPSHOT", "FAILED", "CANCELLED"}


def safe_id(value):
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", value)
            or re.fullmatch(r"(?i:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])", value)):
        raise BackupError("IDENTIFIER_INVALID")
    return value


def safe_root(value):
    if (not isinstance(value, str) or not value.startswith("/") or "\\" in value
            or any(ord(c) < 32 for c in value) or ".." in PurePosixPath(value).parts
            or ":" in value):
        raise BackupError("REMOTE_ROOT_INVALID")
    return str(PurePosixPath(value))


def private_dir(path):
    path.mkdir(parents=True, exist_ok=True, mode=0o700)


def replace_with_retry(source, destination):
    """Keep replacement atomic while allowing brief Windows filesystem locks."""
    for attempt in range(6):
        try:
            os.replace(source, destination)
            return
        except OSError as error:
            if getattr(error, "winerror", None) not in {5, 32, 33} or attempt == 5:
                raise
            time.sleep(0.02 * 2**attempt)


def write_json(path, data):
    temporary = path.with_name(path.name + ".partial-" + uuid.uuid4().hex)
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            os.chmod(temporary, 0o600)
            json.dump(data, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        replace_with_retry(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass
class Settings:
    state_dir: Path
    backup_dir: Path
    key_file: Path
    max_bytes: int = 10 * 1024**3
    max_files: int = 200000
    timeout_seconds: int = 3600
    mysql_dump: str = "mysqldump"
    mysql_ca_file: str | None = None

    def validate(self):
        if not self.backup_dir.is_absolute() or not self.key_file.is_absolute():
            raise BackupError("ABSOLUTE_PATH_REQUIRED")
        self.state_dir = self.state_dir.resolve()
        self.backup_dir = self.backup_dir.resolve()
        self.key_file = self.key_file.resolve()
        if (self.backup_dir.is_relative_to(self.state_dir)
                or self.state_dir.is_relative_to(self.backup_dir)
                or self.key_file.is_relative_to(self.backup_dir)):
            raise BackupError("STORAGE_LOCATION_UNSAFE")
        if not 1 <= self.max_bytes <= 60 * 1024**3 or not 1 <= self.max_files <= 1000000:
            raise BackupError("LIMIT_INVALID")
        if not 1 <= self.timeout_seconds <= 86400:
            raise BackupError("LIMIT_INVALID")

    @classmethod
    def initialize(cls, state_dir, backup_dir, key_file):
        settings = cls(Path(state_dir), Path(backup_dir), Path(key_file))
        settings.validate()
        if (settings.state_dir / "settings.json").exists():
            raise BackupError("ALREADY_INITIALIZED")
        private_dir(settings.state_dir)
        private_dir(settings.backup_dir)
        private_dir(settings.key_file.parent)
        # An existing key can be deliberately reused, but is never overwritten.
        if not settings.key_file.exists():
            with settings.key_file.open("xb") as stream:
                os.chmod(settings.key_file, 0o600)
                stream.write(Fernet.generate_key())
                stream.flush()
                os.fsync(stream.fileno())
        settings.read_key()
        settings.save()
        return settings

    @classmethod
    def load(cls, state_dir):
        path = Path(state_dir).resolve()
        try:
            data = json.loads((path / "settings.json").read_text(encoding="utf-8"))
            settings = cls(path, Path(data["backup_dir"]), Path(data["key_file"]),
                           data["max_bytes"], data["max_files"], data["timeout_seconds"],
                           data.get("mysql_dump", "mysqldump"), data.get("mysql_ca_file"))
            settings.validate()
            return settings
        except FileNotFoundError:
            raise BackupError("NOT_INITIALIZED") from None
        except (ValueError, KeyError, TypeError):
            raise BackupError("SETTINGS_INVALID") from None

    def save(self):
        self.validate()
        write_json(self.state_dir / "settings.json", self.public())

    def public(self):
        return {"backup_dir": str(self.backup_dir), "key_file": str(self.key_file),
                "max_bytes": self.max_bytes, "max_files": self.max_files,
                "timeout_seconds": self.timeout_seconds,
                "mysql_dump": self.mysql_dump, "mysql_ca_file": self.mysql_ca_file}

    def set_backup_dir(self, path):
        previous = self.backup_dir
        self.backup_dir = Path(path)
        try:
            self.validate()
            private_dir(self.backup_dir)
            self.save()
        except BaseException:
            self.backup_dir = previous
            raise

    def read_key(self):
        try:
            key = self.key_file.read_bytes().strip()
            Fernet(key)
            return key
        except (ValueError, OSError):
            raise BackupError("KEY_UNAVAILABLE") from None


class Repository:
    def __init__(self, path, key):
        self.path = Path(path)
        private_dir(self.path.parent)
        self.vault = SQLiteSecrets(self.path.with_name(self.path.stem + ".secrets.sqlite3"), key)
        with self.connection() as db:
            columns = {row["name"] for row in db.execute("PRAGMA table_info(profiles)")}
            if columns and "secret_ref" not in columns:
                raise BackupError("SCHEMA_UPGRADE_REQUIRED")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS profiles (
                    profile_id TEXT PRIMARY KEY, kind TEXT NOT NULL, secret_ref TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sites (
                    site_id TEXT PRIMARY KEY, client_id TEXT NOT NULL,
                    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
                    remote_root TEXT NOT NULL, confirmed INTEGER NOT NULL,
                    known_hosts TEXT, database_scope_confirmed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS databases (
                    db_id TEXT PRIMARY KEY, site_id TEXT NOT NULL REFERENCES sites(site_id),
                    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
                    name_secret_ref TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY, site_id TEXT NOT NULL REFERENCES sites(site_id),
                    idempotency_key TEXT NOT NULL, status TEXT NOT NULL,
                    created_at REAL NOT NULL, finished_at REAL, error_code TEXT,
                    result TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(site_id, idempotency_key));
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_job ON jobs(site_id)
                    WHERE status NOT IN ('FILES_ONLY','COMPLETE','DB_ONLY','STALE_SNAPSHOT',
                                         'FAILED','CANCELLED');
                CREATE TABLE IF NOT EXISTS schedules (
                    site_id TEXT PRIMARY KEY REFERENCES sites(site_id),
                    interval_seconds INTEGER NOT NULL, next_run INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1);
            """)
        os.chmod(self.path, 0o600)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            self.vault.attach(db)
            with db:
                yield db
        finally:
            db.close()

    def import_profiles(self, candidates):
        inserted = 0
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            for candidate in candidates:
                safe_id(candidate.profile_id)
                existing = db.execute("SELECT * FROM profiles WHERE profile_id=?",
                                      (candidate.profile_id,)).fetchone()
                if existing:
                    if existing["kind"] != candidate.kind or self.vault.get(db, existing["secret_ref"]) != candidate.credentials:
                        raise BackupError("PROFILE_CONFLICT", profile_id=candidate.profile_id)
                    continue
                secret_ref = self.vault.store(db, candidate.credentials)
                db.execute("INSERT INTO profiles VALUES (?,?,?)",
                           (candidate.profile_id, candidate.kind, secret_ref))
                inserted += 1
        return inserted

    def profiles(self):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT profile_id,kind FROM profiles ORDER BY profile_id")]

    def credentials(self, profile_id):
        with self.connection() as db:
            row = db.execute("SELECT secret_ref FROM profiles WHERE profile_id=?", (profile_id,)).fetchone()
            if row is None:
                raise BackupError("PROFILE_NOT_FOUND")
            return self.vault.get(db, row["secret_ref"])

    def add_site(self, site_id, client_id, profile_id, remote_root, confirmed=False, known_hosts=None):
        safe_id(site_id)
        safe_id(client_id)
        remote_root = safe_root(remote_root)
        with self.connection() as db:
            profile = db.execute("SELECT kind FROM profiles WHERE profile_id=?", (profile_id,)).fetchone()
            if profile is None or profile["kind"] != "ftp":
                raise BackupError("FTP_PROFILE_REQUIRED")
            try:
                db.execute("INSERT INTO sites (site_id,client_id,profile_id,remote_root,confirmed,known_hosts) VALUES (?,?,?,?,?,?)", (
                    site_id, client_id, profile_id, remote_root, int(confirmed),
                    str(Path(known_hosts).resolve()) if known_hosts else None))
            except sqlite3.IntegrityError:
                raise BackupError("SITE_EXISTS") from None

    def sites(self):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM sites ORDER BY site_id")]

    def add_database(self, site_id, db_id, profile_id, database_name):
        safe_id(db_id)
        if not isinstance(database_name, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_$-]{0,63}", database_name):
            raise BackupError("DATABASE_NAME_INVALID")
        self.site(site_id)
        with self.connection() as db:
            profile = db.execute("SELECT kind FROM profiles WHERE profile_id=?", (profile_id,)).fetchone()
            if not profile or profile["kind"] != "mysql":
                raise BackupError("MYSQL_PROFILE_REQUIRED")
            secret_ref = self.vault.store(db, {"name": database_name})
            try:
                db.execute("INSERT INTO databases VALUES (?,?,?,?)", (db_id, site_id, profile_id, secret_ref))
            except sqlite3.IntegrityError:
                raise BackupError("DATABASE_EXISTS") from None
            db.execute("UPDATE sites SET database_scope_confirmed=0 WHERE site_id=?", (site_id,))

    def databases(self, site_id):
        with self.connection() as db:
            return [dict(row) for row in db.execute(
                "SELECT db_id,site_id,profile_id FROM databases WHERE site_id=? ORDER BY db_id", (site_id,))]

    def database_name(self, db_id):
        with self.connection() as db:
            row = db.execute("SELECT name_secret_ref FROM databases WHERE db_id=?", (db_id,)).fetchone()
            if row is None:
                raise BackupError("DATABASE_NOT_FOUND")
            return self.vault.get(db, row["name_secret_ref"])["name"]

    def confirm_databases(self, site_id):
        self.site(site_id)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM databases WHERE site_id=?", (site_id,)).fetchone():
                raise BackupError("DATABASE_INVENTORY_EMPTY")
            db.execute("UPDATE sites SET database_scope_confirmed=1 WHERE site_id=?", (site_id,))

    def site(self, site_id):
        with self.connection() as db:
            row = db.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise BackupError("SITE_NOT_FOUND")
        return dict(row)

    @staticmethod
    def _job(row):
        result = dict(row)
        result["result"] = json.loads(result["result"])
        return result

    def claim_job(self, site_id, idempotency_key):
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 160:
            raise BackupError("IDEMPOTENCY_KEY_INVALID")
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            site = db.execute("SELECT confirmed FROM sites WHERE site_id=?", (site_id,)).fetchone()
            if not site:
                raise BackupError("SITE_NOT_FOUND")
            if not site["confirmed"]:
                raise BackupError("PROFILE_UNVERIFIED")
            row = db.execute("SELECT * FROM jobs WHERE site_id=? AND idempotency_key=?",
                             (site_id, idempotency_key)).fetchone()
            if row:
                return self._job(row), False
            job_id = "job-" + uuid.uuid4().hex
            try:
                db.execute("INSERT INTO jobs (job_id,site_id,idempotency_key,status,created_at) VALUES (?,?,?,'CREATED',?)",
                           (job_id, site_id, idempotency_key, time.time()))
            except sqlite3.IntegrityError:
                raise BackupError("SITE_BUSY") from None
            return self._job(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()), True

    def update_job(self, job_id, status, error_code=None, result=None):
        with self.connection() as db:
            db.execute("UPDATE jobs SET status=?, error_code=?, finished_at=?, result=? WHERE job_id=?",
                       (status, error_code, time.time() if status in TERMINAL else None,
                        json.dumps(result or {}), job_id))

    def job(self, job_id):
        with self.connection() as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise BackupError("JOB_NOT_FOUND")
        return self._job(row)

    def jobs(self, site_id=None):
        with self.connection() as db:
            if site_id:
                rows = db.execute("SELECT * FROM jobs WHERE site_id=? ORDER BY created_at DESC", (site_id,))
            else:
                rows = db.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 100")
            return [self._job(row) for row in rows]
