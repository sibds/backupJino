"""Verified file and database backup workflows and isolated local recovery."""

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .database import MySqlDump, validate_sql, validate_sql_stream
from .errors import BackupError
from .state import private_dir, safe_id
from .storage import CHUNK, LocalStorage, decrypt_file, encrypt_file, sha256_file
from .transfer import collect_files, relative_path, source_for


@contextmanager
def working_directory(staging, prefix):
    temporary = tempfile.TemporaryDirectory(prefix=prefix, dir=staging)
    try:
        yield Path(temporary.name)
    finally:
        try:
            temporary.cleanup()
        except OSError:
            raise BackupError("CLEANUP_FAILED") from None


def verify_archive(path, job_id):
    try:
        with tarfile.open(path, "r:gz") as archive:
            members = {}
            for member in archive:
                if len(members) >= 1000002:
                    raise BackupError("INTEGRITY_FAILED")
                if member.name in members or not (member.isfile() or member.isdir()):
                    raise BackupError("INTEGRITY_FAILED")
                relative_path(member.name)
                members[member.name] = member
            manifest_member = members.get("manifest.json")
            if not manifest_member or not manifest_member.isfile() or manifest_member.size > 32 * 1024**2:
                raise BackupError("INTEGRITY_FAILED")
            with archive.extractfile(manifest_member) as stream:
                manifest = json.load(stream)
            if manifest["job_id"] != job_id or manifest["schema_version"] != "1.0":
                raise BackupError("INTEGRITY_FAILED")
            expected = {"manifest.json"}
            total = 0
            for item in manifest["files"]["items"]:
                relative_path(item["path"])
                name = "files/" + item["path"]
                if name in expected:
                    raise BackupError("INTEGRITY_FAILED")
                expected.add(name)
                member = members.get(name)
                if not member or not member.isfile() or member.size != item["size_bytes"]:
                    raise BackupError("INTEGRITY_FAILED")
                digest = hashlib.sha256()
                with archive.extractfile(member) as stream:
                    for chunk in iter(lambda: stream.read(CHUNK), b""):
                        digest.update(chunk)
                if digest.hexdigest() != item["sha256"]:
                    raise BackupError("INTEGRITY_FAILED")
                total += member.size
            for directory in manifest["files"].get("directories", []):
                relative_path(directory)
                name = "files/" + directory
                expected.add(name)
                if name not in members or not members[name].isdir():
                    raise BackupError("INTEGRITY_FAILED")
            for item in manifest["databases"]:
                name = "databases/" + safe_id(item["db_id"]) + ".sql"
                if name in expected:
                    raise BackupError("INTEGRITY_FAILED")
                expected.add(name)
                member = members.get(name)
                if not member or not member.isfile() or member.size != item["size_bytes"]:
                    raise BackupError("INTEGRITY_FAILED")
                digest = hashlib.sha256()
                with archive.extractfile(member) as stream:
                    for chunk in iter(lambda: stream.read(CHUNK), b""):
                        digest.update(chunk)
                    stream.seek(0)
                    validate_sql_stream(stream, member.size)
                if digest.hexdigest() != item["sha256"] or item["validation"] != "PASS":
                    raise BackupError("INTEGRITY_FAILED")
            if (set(members) != expected or len(manifest["files"]["items"]) != manifest["files"]["count"]
                    or total != manifest["files"]["bytes"]):
                raise BackupError("INTEGRITY_FAILED")
            return manifest
    except (OSError, tarfile.TarError, KeyError, ValueError, TypeError, BackupError):
        raise BackupError("INTEGRITY_FAILED") from None


def unpack_archive(encrypted, destination, key, job_id):
    """Recover every verified artifact without depending on the metadata database."""
    destination = Path(destination).resolve()
    if destination.exists():
        raise BackupError("RESTORE_DESTINATION_EXISTS")
    private_dir(destination.parent)
    created = False
    try:
        with working_directory(destination.parent, "unpack-") as work:
            decrypted = work / "verified.tar.gz"
            decrypt_file(encrypted, decrypted, key, job_id)
            manifest = verify_archive(decrypted, job_id)
            destination.mkdir(mode=0o700, exist_ok=False)
            created = True
            with tarfile.open(decrypted, "r:gz") as archive:
                for member in archive:
                    target = destination.joinpath(*member.name.split("/"))
                    if member.isdir():
                        private_dir(target)
                    else:
                        private_dir(target.parent)
                        with archive.extractfile(member) as incoming, target.open("xb") as outgoing:
                            os.chmod(target, 0o600)
                            shutil.copyfileobj(incoming, outgoing, CHUNK)
        return {"job_id": job_id, "status": manifest["status"],
                "files_count": manifest["files"]["count"], "databases_count": len(manifest["databases"]),
                "database_imported": False, "application_restore_tested": False}
    except BaseException:
        if created:
            shutil.rmtree(destination)
        raise


class BackupService:
    def __init__(self, settings, repository, source_factory=source_for, db_exporter=None):
        self.settings, self.repository, self.source_factory = settings, repository, source_factory
        self.db_exporter = db_exporter or MySqlDump((settings.mysql_dump,), settings.mysql_ca_file)
        self.staging = settings.state_dir / "staging"
        private_dir(self.staging)

    def run(self, site_id, idempotency_key, mode="auto"):
        if mode not in {"auto", "files-only"}:
            raise BackupError("BACKUP_MODE_INVALID")
        job, claimed = self.repository.claim_job(site_id, idempotency_key)
        if not claimed:
            return job
        job_id = job["job_id"]
        try:
            with working_directory(self.staging, job_id + "-") as work:
                self.repository.update_job(job_id, "PREFLIGHT")
                self.settings.validate()
                site = self.repository.site(site_id)
                key = self.settings.read_key()
                credentials = self.repository.credentials(site["profile_id"])
                started = datetime.now(timezone.utc).isoformat()
                databases, database_errors = [], []
                bindings = self.repository.databases(site_id)
                scope_confirmed = site["database_scope_confirmed"] and bindings and mode == "auto"
                self.repository.update_job(job_id, "DB_EXPORT")
                private_dir(work / "databases")
                database_bytes = 0
                if scope_confirmed:
                    for binding in bindings:
                        db_id = binding["db_id"]
                        destination = work / "databases" / (db_id + ".sql")
                        try:
                            metadata = self.db_exporter.export(
                                self.repository.credentials(binding["profile_id"]),
                                self.repository.database_name(db_id), destination,
                                timeout_seconds=self.settings.timeout_seconds,
                                max_bytes=self.settings.max_bytes - database_bytes)
                            validate_sql(destination)
                            database_bytes += destination.stat().st_size
                            databases.append({**metadata, "db_id": db_id, "engine": "mysql",
                                              "size_bytes": destination.stat().st_size,
                                              "sha256": sha256_file(destination)})
                        except BackupError as exc:
                            destination.unlink(missing_ok=True)
                            database_errors.append({"db_id": db_id, "error_code": exc.code})
                self.repository.update_job(job_id, "FILE_EXPORT")
                with self.source_factory(credentials, site) as source:
                    capture = collect_files(source, work / "files", max_bytes=self.settings.max_bytes - database_bytes,
                                            max_files=self.settings.max_files,
                                            timeout_seconds=self.settings.timeout_seconds)
                status = "COMPLETE" if scope_confirmed and not database_errors else "FILES_ONLY"
                warnings = ["LIVE_FILES_BEST_EFFORT"]
                if status != "COMPLETE":
                    warnings.append("DB_UNAVAILABLE" if not bindings or database_errors or mode == "files-only" else "DB_SCOPE_UNVERIFIED")
                manifest = {
                    "schema_version": "1.0", "job_id": job_id, "site_id": site_id,
                    "status": status, "capture_method": credentials["protocol"],
                    "source_snapshot_at": None, "capture_started_at": started,
                    "capture_finished_at": datetime.now(timezone.utc).isoformat(),
                    "capture_consistency": "BEST_EFFORT", "files": capture, "databases": databases,
                    "storage_verification": "RECORDED_IN_JOB_RECEIPT",
                    "warnings": warnings, "database_errors": database_errors,
                }
                (work / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
                self.repository.update_job(job_id, "VERIFY")
                archive_path = work / "archive.tar.gz"
                with tarfile.open(archive_path, "w:gz") as archive:
                    archive.add(work / "manifest.json", arcname="manifest.json", recursive=False)
                    for directory in capture["directories"]:
                        archive.add(work / "files" / directory, arcname="files/" + directory, recursive=False)
                    for item in capture["items"]:
                        archive.add(work / "files" / item["path"], arcname="files/" + item["path"], recursive=False)
                    for item in databases:
                        name = "databases/" + item["db_id"] + ".sql"
                        archive.add(work / name, arcname=name, recursive=False)
                verify_archive(archive_path, job_id)
                encrypted = work / "archive.enc"
                encrypt_file(archive_path, encrypted, key, job_id)
                self.repository.update_job(job_id, "UPLOAD")
                storage = LocalStorage(self.settings.backup_dir)
                receipt = storage.put(site_id, job_id, encrypted)
                self.repository.update_job(job_id, "REMOTE_VERIFY")
                decrypt_file(storage.resolve(receipt["storage_key"]), work / "readback.tar.gz", key, job_id)
                verify_archive(work / "readback.tar.gz", job_id)
                result = {"artifact": receipt, "storage_verified": True, "files_count": capture["count"],
                          "files_bytes": capture["bytes"], "warnings": manifest["warnings"],
                          "databases_count": len(databases), "database_errors": database_errors}
            # Final success is recorded after sessions and temporary plaintext are cleaned up.
            self.repository.update_job(job_id, status, result=result)
        except KeyboardInterrupt:
            self.repository.update_job(job_id, "CANCELLED", error_code="CANCELLED")
            raise
        except BackupError as exc:
            self.repository.update_job(job_id, "FAILED", error_code=exc.code)
        except Exception:
            # Upstream messages may contain paths, usernames, passwords or server responses.
            self.repository.update_job(job_id, "FAILED", error_code="INTERNAL_ERROR")
        return self.repository.job(job_id)

    def _readback(self, job_id, work):
        job = self.repository.job(job_id)
        receipt = job["result"].get("artifact")
        if not receipt or not job["result"].get("storage_verified"):
            raise BackupError("ARTIFACT_UNAVAILABLE")
        storage = LocalStorage(receipt["storage_root"])
        storage.verify(receipt)
        archive_path = work / "verified.tar.gz"
        decrypt_file(storage.resolve(receipt["storage_key"]), archive_path, self.settings.read_key(), job_id)
        manifest = verify_archive(archive_path, job_id)
        return archive_path, manifest

    def verify(self, job_id):
        with working_directory(self.staging, "verify-") as work:
            _, manifest = self._readback(job_id, work)
        return {"job_id": job_id, "verified": True, "files_count": manifest["files"]["count"],
                "status": manifest["status"], "application_restore_tested": False}

    def restore_files(self, job_id, destination):
        destination = Path(destination).resolve()
        if destination.exists():
            raise BackupError("RESTORE_DESTINATION_EXISTS")
        created = False
        try:
            with working_directory(self.staging, "restore-") as work:
                archive_path, manifest = self._readback(job_id, work)
                destination.mkdir(parents=True, exist_ok=False, mode=0o700)
                created = True
                with tarfile.open(archive_path, "r:gz") as archive:
                    for directory in manifest["files"].get("directories", []):
                        private_dir(destination / directory)
                    for item in manifest["files"]["items"]:
                        path = destination / item["path"]
                        private_dir(path.parent)
                        with archive.extractfile("files/" + item["path"]) as incoming, path.open("xb") as outgoing:
                            shutil.copyfileobj(incoming, outgoing, CHUNK)
            return {"job_id": job_id, "files_count": manifest["files"]["count"],
                    "database_restored": False, "application_restore_tested": False}
        except BaseException:
            if created:
                shutil.rmtree(destination)
            raise
