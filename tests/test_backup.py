import contextlib
import io
import json
import shutil
import stat
import tarfile
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from jino_backup.errors import BackupError
from jino_backup.importers import Candidate
from jino_backup.service import BackupService, verify_archive
from jino_backup.state import Repository, Settings
from jino_backup.transfer import Entry, collect_files, safe_name


class FixtureSource:
    """Real filesystem fixture behind the same read-only source contract as FTPS/SFTP."""

    def __init__(self, root):
        self.root = root
        self.closed = False
        self.interrupt = False
        self.offsets = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def listdir(self, relative):
        return [self.stat(str((Path(relative) / path.name).as_posix()))
                for path in (self.root / relative).iterdir()]

    def stat(self, relative):
        path = self.root / relative
        info = path.lstat()
        kind = "link" if stat.S_ISLNK(info.st_mode) else "dir" if path.is_dir() else "file"
        return Entry(path.name, kind, info.st_size, str(info.st_mtime_ns))

    def download(self, relative, outgoing, offset):
        self.offsets.append(offset)
        with (self.root / relative).open("rb") as incoming:
            incoming.seek(offset)
            if self.interrupt:
                self.interrupt = False
                outgoing.write(incoming.read(3))
                raise BackupError("TRANSFER_INTERRUPTED")
            shutil.copyfileobj(incoming, outgoing)


@pytest.fixture
def system(tmp_path):
    source_dir = tmp_path / "website"
    source_dir.mkdir()
    (source_dir / ".htaccess").write_text("deny from example")
    (source_dir / "index.php").write_text("<?php echo 'example';")
    (source_dir / "media").mkdir()
    (source_dir / "media/file.txt").write_bytes(b"synthetic-customer-data")
    settings = Settings.initialize(tmp_path / "state", tmp_path / "archives", tmp_path / "keys/master.key")
    repo = Repository(settings.state_dir / "registry.sqlite3", settings.read_key())
    repo.import_profiles([Candidate("ftp-example", "ftp", {
        "host": "ftp.example.test", "port": 21, "protocol": "ftps",
        "username": "synthetic-user", "password": "synthetic-password",
    })])
    repo.add_site("site-1", "client-1", "ftp-example", "/public_html", confirmed=True)
    source = FixtureSource(source_dir)
    service = BackupService(settings, repo, source_factory=lambda credentials, site: source)
    return settings, repo, source, service


def test_run_keeps_files_only_status_and_encrypted_verified_copy(system):
    settings, repo, source, service = system
    job = service.run("site-1", "request-1")
    assert job["status"] == "FILES_ONLY"
    assert job["result"]["files_count"] == 3
    assert job["result"]["storage_verified"] is True
    assert "DB_UNAVAILABLE" in job["result"]["warnings"]
    assert source.closed
    assert not list((settings.state_dir / "staging").iterdir())
    encrypted = list(settings.backup_dir.rglob("*.enc"))
    assert len(encrypted) == 1
    assert b"synthetic-customer-data" not in encrypted[0].read_bytes()
    assert b"synthetic-password" not in json.dumps(job).encode()
    assert service.verify(job["job_id"])["verified"]


def test_idempotent_run_does_not_create_another_archive(system):
    settings, repo, source, service = system
    first = service.run("site-1", "request-1")
    assert service.run("site-1", "request-1")["job_id"] == first["job_id"]
    assert len(list(settings.backup_dir.rglob("*.enc"))) == 1


def test_archive_receipt_retains_previous_folder_after_settings_change(system, tmp_path):
    settings, repo, source, service = system
    job = service.run("site-1", "request-1")
    settings.set_backup_dir(tmp_path / "new-archives")
    assert service.verify(job["job_id"])["verified"]


def test_transfer_failure_is_not_success_and_staging_is_cleaned(system):
    settings, repo, source, service = system
    def fail(*args):
        raise BackupError("TRANSFER_INTERRUPTED")
    source.download = fail
    job = service.run("site-1", "request-1")
    assert job["status"] == "FAILED"
    assert job["error_code"] == "TRANSFER_INTERRUPTED"
    assert source.closed
    assert not list((settings.state_dir / "staging").iterdir())
    assert not list(settings.backup_dir.rglob("*.enc"))


def test_resume_after_interruption_requires_unchanged_metadata(system, tmp_path):
    settings, repo, source, service = system
    source.interrupt = True
    target = tmp_path / "captured"
    capture = collect_files(source, target, max_bytes=10000, max_files=100, timeout_seconds=10)
    assert capture["count"] == 3
    assert source.offsets[:2] == [0, 3]
    assert (target / ".htaccess").read_bytes() == (source.root / ".htaccess").read_bytes()


def test_changed_source_does_not_resume_or_publish_success(system):
    settings, repo, source, service = system
    original_download = source.download
    def change(relative, outgoing, offset):
        original_download(relative, outgoing, offset)
        (source.root / relative).write_bytes(b"changed-size")
    source.download = change
    job = service.run("site-1", "request-1")
    assert job["status"] == "FAILED"
    assert job["error_code"] == "SOURCE_CHANGED"


def test_limit_exceeded_before_download(system):
    settings, repo, source, service = system
    settings.max_bytes = 1
    job = service.run("site-1", "request-1")
    assert job["error_code"] == "TRANSFER_LIMIT_EXCEEDED"
    assert source.offsets == []


@pytest.mark.parametrize("name", ["..", ".", "../escape", "/escape", "C:escape", "a\\b",
                                  "bad\r\nRETR", "CON", "NUL.txt", "a.", "a "])
def test_hostile_remote_paths_rejected(name):
    with pytest.raises(BackupError, match="REMOTE_PATH_UNSAFE"):
        safe_name(name)


def test_remote_symlink_rejected_without_downloading(system):
    settings, repo, source, service = system
    source.listdir = lambda relative: [Entry("link", "link", 10, "mtime")]
    job = service.run("site-1", "request-1")
    assert job["error_code"] == "REMOTE_TYPE_UNSAFE"
    assert not source.offsets


def test_hidden_files_are_restored_to_new_directory(system, tmp_path):
    settings, repo, source, service = system
    job = service.run("site-1", "request-1")
    destination = tmp_path / "restored"
    result = service.restore_files(job["job_id"], destination)
    assert result["files_count"] == 3
    assert result["database_restored"] is False
    assert (destination / ".htaccess").read_bytes() == (source.root / ".htaccess").read_bytes()
    with pytest.raises(BackupError, match="RESTORE_DESTINATION_EXISTS"):
        service.restore_files(job["job_id"], destination)


def test_corrupt_archive_is_not_restored(system, tmp_path):
    settings, repo, source, service = system
    job = service.run("site-1", "request-1")
    path = settings.backup_dir / job["result"]["artifact"]["storage_key"]
    content = bytearray(path.read_bytes())
    content[-1] ^= 1
    path.write_bytes(content)
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        service.restore_files(job["job_id"], tmp_path / "restored")
    assert not (tmp_path / "restored").exists()


@pytest.mark.parametrize("name", ["../escape", "/absolute", "files/../escape"])
def test_archive_validation_rejects_traversal_even_with_manifest(tmp_path, name):
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        member = tarfile.TarInfo(name)
        member.size = 3
        tar.addfile(member, io.BytesIO(b"bad"))
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        verify_archive(archive, "job-1")


def test_cancellation_closes_source_and_cleans_staging(system):
    settings, repo, source, service = system
    def cancel(*args):
        raise KeyboardInterrupt
    source.download = cancel
    with pytest.raises(KeyboardInterrupt):
        service.run("site-1", "cancel-1")
    assert repo.jobs()[0]["status"] == "CANCELLED"
    assert source.closed
    assert not list((settings.state_dir / "staging").iterdir())


def test_cleanup_failure_is_explicit_and_never_a_success(system, monkeypatch):
    settings, repo, source, service = system
    original = shutil.rmtree
    with monkeypatch.context() as patch:
        def fail_cleanup(*args, **kwargs):
            raise PermissionError("synthetic-private-path")
        patch.setattr(shutil, "rmtree", fail_cleanup)
        job = service.run("site-1", "cleanup-1")
    assert job["status"] == "FAILED"
    assert job["error_code"] == "CLEANUP_FAILED"
    assert "synthetic-private-path" not in json.dumps(job)
    for path in (settings.state_dir / "staging").iterdir():
        original(path)


def test_unexpected_manifest_type_is_integrity_error(tmp_path):
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        manifest = {"schema_version":"1.0", "job_id":"job-1", "files":{"items":None}}
        content = json.dumps(manifest).encode()
        member = tarfile.TarInfo("manifest.json")
        member.size = len(content)
        tar.addfile(member, io.BytesIO(content))
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        verify_archive(archive, "job-1")


def test_unpack_without_registry_recovers_manifest_and_files(system, tmp_path):
    from jino_backup.service import unpack_archive
    settings, repo, source, service = system
    job = service.run("site-1", "standalone-recovery")
    archive = settings.backup_dir / job["result"]["artifact"]["storage_key"]
    destination = tmp_path / "recovered"
    result = unpack_archive(archive, destination, settings.read_key(), job["job_id"])
    assert result["status"] == "FILES_ONLY"
    assert (destination / "files/.htaccess").exists()
    assert json.loads((destination / "manifest.json").read_text())["job_id"] == job["job_id"]
    with pytest.raises(BackupError, match="RESTORE_DESTINATION_EXISTS"):
        unpack_archive(archive, destination, settings.read_key(), job["job_id"])
