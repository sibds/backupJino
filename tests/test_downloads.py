import json
from pathlib import Path

import pytest

from jino_backup.errors import BackupError
from jino_backup.transfer import collect_files
from test_backup import FixtureSource


@pytest.mark.parametrize(
    "denied,code", [("login", "FTP_LOGIN_DENIED"), ("root", "FTP_ROOT_DENIED")]
)
def test_ftps_distinguishes_login_failure_from_root_access(denied, code, monkeypatch):
    import ftplib
    from jino_backup.transfer import FtpsSource

    class DeniedFTP:
        def __init__(self, **kwargs):
            pass

        def connect(self, *args):
            pass

        def login(self, *args):
            if denied == "login":
                raise ftplib.error_perm("530 Synthetic login denial")

        def prot_p(self):
            pass

        def cwd(self, root):
            raise ftplib.error_perm("550 Synthetic root denial")

        def close(self):
            pass

    monkeypatch.setattr("jino_backup.transfer.ftplib.FTP_TLS", DeniedFTP)
    with pytest.raises(BackupError, match=code):
        with FtpsSource(
            {"host": "ftp.example.test", "port": 21, "username": "test", "password": "synthetic"},
            {"remote_root": "/"},
        ):
            pass


def source_fixture(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    (root / "a.txt").write_bytes(b"first-file")
    (root / "b.txt").write_bytes(b"second-file")
    return FixtureSource(root)


def capture(source, target, checkpoint):
    return collect_files(
        source, target, max_bytes=100000, max_files=100, timeout_seconds=10, checkpoint=checkpoint
    )


def test_restart_preserves_finished_files_and_resumes_partial(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    source = source_fixture(tmp_path)
    target = tmp_path / "output/files"
    original = source.download

    def interrupted(relative, stream, offset):
        if relative == "b.txt":
            if offset == 0:
                stream.write(b"sec")
            raise BackupError("TRANSFER_INTERRUPTED")
        return original(relative, stream, offset)

    source.download = interrupted
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        with pytest.raises(BackupError, match="TRANSFER_INTERRUPTED"):
            capture(source, target, checkpoint)
    assert (target / "a.txt").read_bytes() == b"first-file"
    assert not (target / "b.txt").exists()
    source.download = original
    source.offsets = []
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        result = capture(source, target, checkpoint)
    assert result["count"] == 2
    assert source.offsets == [3]
    assert (target / "b.txt").read_bytes() == b"second-file"


def test_completed_file_is_not_overwritten_when_remote_changes(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    source = source_fixture(tmp_path)
    target = tmp_path / "output/files"
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        capture(source, target, checkpoint)
    (source.root / "a.txt").write_bytes(b"new-content")
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        with pytest.raises(BackupError, match="SOURCE_CHANGED"):
            capture(source, target, checkpoint)
    assert (target / "a.txt").read_bytes() == b"first-file"


def test_wrong_scope_and_foreign_directory_are_rejected(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    root = tmp_path / "output"
    root.mkdir()
    (root / "foreign.txt").write_text("keep")
    with pytest.raises(BackupError, match="DESTINATION_NOT_OWNED"):
        with DirectoryCheckpoint(root, "scope-1"):
            pass
    assert (root / "foreign.txt").read_text() == "keep"
    owned = tmp_path / "owned"
    with DirectoryCheckpoint(owned, "scope-1"):
        pass
    with pytest.raises(BackupError, match="DOWNLOAD_SCOPE_CHANGED"):
        with DirectoryCheckpoint(owned, "scope-2"):
            pass


def test_two_workers_cannot_write_one_snapshot(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    root = tmp_path / "output"
    with DirectoryCheckpoint(root, "scope-1"):
        with pytest.raises(BackupError, match="DOWNLOAD_BUSY"):
            with DirectoryCheckpoint(root, "scope-1"):
                pass


def test_local_corruption_is_detected_before_skipping(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    source = source_fixture(tmp_path)
    target = tmp_path / "output/files"
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        capture(source, target, checkpoint)
    (target / "a.txt").write_bytes(b"corrupted!")
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        with pytest.raises(BackupError, match="LOCAL_FILE_CHANGED"):
            capture(source, target, checkpoint)


def test_server_plan_uses_ftp_independently_of_panel_and_never_exposes_credentials(tmp_path):
    from jino_backup.bulk import plan_servers

    source = tmp_path / "servers.json"
    rows = [
        {
            "id": str(i),
            "login": "same-panel",
            "password": "panel-secret",
            "ftp_host": "ftp.example.test",
            "ftp_login": f"user-{i}",
            "ftp_password": "ftp-secret",
        }
        for i in range(2)
    ]
    rows.append({"id": "3", "login": "panel", "password": "panel-secret"})
    source.write_text(json.dumps(rows))
    plan = plan_servers(
        source,
        {
            "id": "id",
            "ftp_host": "ftp_host",
            "ftp_login": "ftp_login",
            "ftp_password": "ftp_password",
            "ftp_protocol": "ftps",
        },
    )
    assert len(plan.servers) == 2
    assert plan.report["needs_review"] == 1
    assert "secret" not in repr(plan) + json.dumps(plan.report)
    assert len({server.server_id for server in plan.servers}) == 2


def test_batch_continues_after_failed_server_and_restart_skips_success(tmp_path):
    from jino_backup.bulk import DownloadBatch, plan_servers

    source = tmp_path / "servers.json"
    source.write_text(
        json.dumps(
            [
                {
                    "id": "1",
                    "ftp_host": "one.example.test",
                    "ftp_login": "u1",
                    "ftp_password": "secret-1",
                },
                {
                    "id": "2",
                    "ftp_host": "two.example.test",
                    "ftp_login": "u2",
                    "ftp_password": "secret-2",
                },
            ]
        )
    )
    plan = plan_servers(
        source,
        {
            "id": "id",
            "ftp_host": "ftp_host",
            "ftp_login": "ftp_login",
            "ftp_password": "ftp_password",
            "ftp_protocol": "ftps",
        },
    )
    fixture = source_fixture(tmp_path)
    calls = []

    def factory(credentials, site):
        calls.append(credentials["username"])
        if credentials["username"] == "u1":
            raise BackupError("AUTH_OR_ROOT_FAILED")
        return fixture

    batch = DownloadBatch(plan, tmp_path / "destination", "batch-1", "/", factory)
    result = batch.run()
    assert [row["status"] for row in result["servers"]] == ["FAILED", "FILES_ONLY"]
    assert "secret-" not in json.dumps(result)
    calls.clear()
    again = batch.run()
    assert calls == ["u1"]
    assert again["servers"][1]["status"] == "FILES_ONLY"
    assert len(list((tmp_path / "destination").rglob("manifest.json"))) == 1


def test_expired_certificate_is_distinct_and_never_retried_as_a_network_outage(monkeypatch):
    import ssl
    from jino_backup.transfer import FtpsSource

    class ExpiredCertificate:
        def __init__(self, **kwargs):
            pass

        def connect(self, *args):
            pass

        def login(self, *args):
            raise ssl.SSLCertVerificationError(1, "certificate expired")

        def close(self):
            pass

    monkeypatch.setattr("jino_backup.transfer.ftplib.FTP_TLS", ExpiredCertificate)
    with pytest.raises(BackupError, match="TLS_CERTIFICATE_INVALID"):
        with FtpsSource(
            {"host": "ftp.example.test", "port": 21, "username": "test", "password": "secret"},
            {"remote_root": "/"},
        ):
            pass


def test_power_loss_discards_uncommitted_tail_and_resumes_at_durable_offset(tmp_path):
    import os
    import subprocess
    import sys
    from jino_backup.downloads import DirectoryCheckpoint
    from jino_backup.storage import CHUNK

    source = source_fixture(tmp_path)
    (source.root / "a.txt").unlink()
    (source.root / "b.txt").write_bytes(b"x" * (12 * CHUNK))
    script = """
import os, sys
from pathlib import Path
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.transfer import collect_files
from jino_backup.storage import CHUNK
from test_backup import FixtureSource
source = FixtureSource(Path(sys.argv[1]))
def crash(relative, writer, offset):
    for _ in range(9):
        writer.write(b'x' * CHUNK)
    os._exit(9)
source.download = crash
with DirectoryCheckpoint(Path(sys.argv[2]), 'scope-1') as checkpoint:
    collect_files(source, Path(sys.argv[2]) / 'files', max_bytes=100*CHUNK,
                  max_files=100, timeout_seconds=30, checkpoint=checkpoint)
"""
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(Path("src").resolve()), str(Path("tests").resolve())]),
    }
    result = subprocess.run(
        [sys.executable, "-c", script, str(source.root), str(tmp_path / "output")],
        env=environment,
        capture_output=True,
    )
    assert result.returncode == 9
    with DirectoryCheckpoint(tmp_path / "output", "scope-1") as checkpoint:
        result = collect_files(
            source,
            tmp_path / "output/files",
            max_bytes=100 * CHUNK,
            max_files=100,
            timeout_seconds=30,
            checkpoint=checkpoint,
        )
    assert source.offsets == [8 * CHUNK]
    assert result["bytes"] == 12 * CHUNK
    assert (tmp_path / "output/files/b.txt").read_bytes() == b"x" * (12 * CHUNK)


def test_corrupt_partial_is_never_resumed(tmp_path):
    from jino_backup.downloads import DirectoryCheckpoint

    source = source_fixture(tmp_path)

    def fail(relative, writer, offset):
        if not offset:
            writer.write(b"fir")
        raise BackupError("TRANSFER_INTERRUPTED")

    source.download = fail
    root = tmp_path / "output"
    with DirectoryCheckpoint(root, "scope-1") as checkpoint:
        with pytest.raises(BackupError):
            capture(source, root / "files", checkpoint)
    next((root / ".jino-progress").glob("*.part")).write_bytes(b"bad")
    with DirectoryCheckpoint(root, "scope-1") as checkpoint:
        with pytest.raises(BackupError, match="LOCAL_PROGRESS_INVALID"):
            capture(source, root / "files", checkpoint)
