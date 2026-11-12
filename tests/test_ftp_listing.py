"""Synthetic FTP listing failures: classify, retain diagnostics, keep partial copies honest."""
import ftplib
import json

import pytest

from jino_backup.errors import BackupError
from jino_backup.transfer import Entry, FtpsSource, collect_files


@pytest.mark.parametrize("operation,reply,expected", [
    ("MLSD", "500", "REMOTE_LIST_UNSUPPORTED"),
    ("MLSD", "502", "REMOTE_LIST_UNSUPPORTED"),
    ("MLSD", "550", "REMOTE_DIRECTORY_UNAVAILABLE"),
    ("CWD", "550", "REMOTE_DIRECTORY_UNAVAILABLE"),
    ("MLSD", "530", "FTP_LOGIN_DENIED"),
    ("MLSD", "553", "FTP_LIST_REJECTED"),
])
def test_ftp_rejection_keeps_operation_and_reply_without_upstream_text(operation, reply, expected):
    source = FtpsSource({}, {"remote_root": "/"})

    def rejected(*args):
        raise ftplib.error_perm(reply + " NEVER-EXPOSE-server-message")

    class FTP:
        mlsd = staticmethod(rejected)

    source.ftp = FTP()
    source._cwd = rejected if operation == "CWD" else lambda relative: None
    with pytest.raises(BackupError) as caught:
        list(source.listdir("private-folder"))
    assert caught.value.code == expected
    assert caught.value.details == {"operation": operation, "ftp_reply_code": int(reply)}
    assert "NEVER-EXPOSE" not in json.dumps(caught.value.details)


def test_malformed_ftp_facts_are_not_reported_as_an_unsupported_command():
    source = FtpsSource({}, {"remote_root": "/"})
    source._cwd = lambda relative: None

    class FTP:
        def mlsd(self):
            yield "index.txt", {"type": "file", "size": "not-a-size"}

    source.ftp = FTP()
    with pytest.raises(BackupError) as caught:
        list(source.listdir(""))
    assert caught.value.code == "REMOTE_LIST_INVALID"
    assert caught.value.details == {"operation": "MLSD"}


def test_non_utf8_listing_has_a_distinct_error():
    source = FtpsSource({}, {"remote_root": "/"})
    source._cwd = lambda relative: None

    class FTP:
        def mlsd(self):
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid byte")

        def close(self):
            pass

    source.ftp = FTP()
    with pytest.raises(BackupError) as caught:
        list(source.listdir(""))
    assert caught.value.code == "REMOTE_LIST_ENCODING_INVALID"
    assert caught.value.details == {"operation": "MLSD"}


class DeniedDirectorySource:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def listdir(self, relative):
        if relative == "blocked":
            raise BackupError("REMOTE_DIRECTORY_UNAVAILABLE", operation="CWD", ftp_reply_code=550)
        return [Entry("blocked", "dir", 0), Entry("index.txt", "file", 4, "1")]

    def stat(self, relative):
        return Entry("index.txt", "file", 4, "1")

    def download(self, relative, outgoing, offset):
        outgoing.write(b"data"[offset:])


def test_directory_denied_keeps_accessible_files_and_an_explicit_partial_error(tmp_path):
    result = collect_files(DeniedDirectorySource(), tmp_path / "copy", max_bytes=100,
                           max_files=20, timeout_seconds=10, continue_on_error=True)
    assert (tmp_path / "copy/index.txt").read_bytes() == b"data"
    assert result["errors"] == [{"path": "blocked", "kind": "dir",
        "error_code": "REMOTE_DIRECTORY_UNAVAILABLE", "operation": "CWD", "ftp_reply_code": 550}]


def test_directory_denied_remains_fatal_in_strict_mode(tmp_path):
    with pytest.raises(BackupError, match="REMOTE_DIRECTORY_UNAVAILABLE"):
        collect_files(DeniedDirectorySource(), tmp_path / "copy", max_bytes=100,
                      max_files=20, timeout_seconds=10)


def test_denied_root_is_fatal_even_when_partial_copy_is_allowed(tmp_path):
    source = DeniedDirectorySource()
    source.listdir = lambda relative: (_ for _ in ()).throw(BackupError("REMOTE_DIRECTORY_UNAVAILABLE"))
    with pytest.raises(BackupError, match="REMOTE_DIRECTORY_UNAVAILABLE"):
        collect_files(source, tmp_path / "copy", max_bytes=100,
                      max_files=20, timeout_seconds=10, continue_on_error=True)


def test_controlled_inventory_logs_failed_directory_and_preserves_partial_result(tmp_path):
    from jino_backup.dashboard_runner import ControlledSource

    class Control:
        def checkpoint(self):
            pass

    events = []
    source = ControlledSource(DeniedDirectorySource(), Control(), lambda **data: events.append(data),
                              tmp_path / "out", continue_on_error=True)
    with source:
        result = collect_files(source, tmp_path / "files", max_bytes=100,
                               max_files=20, timeout_seconds=10, continue_on_error=True)
    failure = next(event for event in events if event.get("stage") == "DIRECTORY_ERROR")
    assert failure["directory"] == "blocked" and failure["ftp_reply_code"] == 550
    assert failure["operation"] == "CWD"
    assert result["errors"][0]["path"] == "blocked"
    assert not any(event.get("inventory_complete") is True for event in events)
