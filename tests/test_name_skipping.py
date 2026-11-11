"""Unsupported Linux names are explicit omissions, never traversal exceptions."""
import json

import pytest

from jino_backup.errors import BackupError
from jino_backup.transfer import Entry, collect_files


class MemorySource:
    def __init__(self, names=None):
        self.entries = names or [Entry("archive:old", "dir", 0), Entry("bad?.txt", "file", 8), Entry("ok.txt", "file", 2)]
        self.visited = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def listdir(self, relative):
        self.visited.append(relative)
        assert relative == "", "An unsupported directory must never be entered"
        return self.entries

    def stat(self, relative):
        assert relative == "ok.txt"
        return self.entries[-1]

    def download(self, relative, outgoing, offset):
        assert relative == "ok.txt"
        outgoing.write(b"ok"[offset:])


def test_skip_names_keeps_valid_files_and_records_directory_omission(tmp_path):
    source = MemorySource()
    result = collect_files(source, tmp_path / "files", max_bytes=1000, max_files=10,
                           timeout_seconds=10, skip_unsupported_names=True)
    assert result["count"] == 1 and (tmp_path / "files/ok.txt").read_bytes() == b"ok"
    assert len(result["skipped"]) == len(result["errors"]) == 2
    assert result["skipped"][0] == {"path": "archive:old", "kind": "dir", "reason": "WINDOWS_CHARACTERS", "error_code": "REMOTE_NAME_UNSUPPORTED"}
    assert source.visited == [""]


@pytest.mark.parametrize("name", ["..", "../escape", "/escape", "a\\b", "bad\x00name", "bad\nname"])
def test_skip_mode_never_waives_structural_path_guards(tmp_path, name):
    with pytest.raises(BackupError, match="REMOTE_PATH_UNSAFE"):
        collect_files(MemorySource([Entry(name, "file", 1)]), tmp_path / "files",
                      max_bytes=1000, max_files=10, timeout_seconds=10, skip_unsupported_names=True)


@pytest.mark.parametrize("name", ["has:colon", "has?question", "trailing.", "trailing ", "NUL.txt"])
def test_unsupported_only_tree_is_explicitly_partial_not_empty_success(tmp_path, name):
    result = collect_files(MemorySource([Entry(name, "dir", 0)]), tmp_path / "files",
                           max_bytes=1000, max_files=10, timeout_seconds=10, skip_unsupported_names=True)
    assert result["count"] == 0 and len(result["errors"]) == len(result["skipped"]) == 1


def test_strict_mode_keeps_existing_behavior(tmp_path):
    with pytest.raises(BackupError, match="REMOTE_PATH_UNSAFE"):
        collect_files(MemorySource(), tmp_path / "files", max_bytes=1000, max_files=10,
                      timeout_seconds=10)


def test_https_job_records_unsupported_entries_in_partial_manifest(tmp_path):
    from jino_backup.panel_downloads import PanelFileJob, PanelFileSource
    from test_panel_downloads import FilePanel
    panel = FilePanel()
    panel.rows["/"].append({"name": "bad:directory", "type": "dir", "path": "/bad:directory", "size": 0})
    def factory():
        return PanelFileSource(panel, {"binding_verified": True, "canonical_root": "/"}, skip_unsupported_names=True)
    job = PanelFileJob(tmp_path, "server-example", "synthetic-login", "skip", factory,
                       skip_unsupported_names=True)
    result = job.run()
    assert result["status"] == "PARTIAL" and result["files_skipped_count"] == 1
    manifest = json.loads(next(tmp_path.glob("*/*/manifest.json")).read_text())
    assert manifest["files"]["skipped"][0]["path"] == "bad:directory"


def test_https_skip_mode_still_rejects_outside_root_token():
    from jino_backup.panel_downloads import PanelFileSource
    from test_panel_downloads import FilePanel
    panel = FilePanel()
    panel.root_file_token = "/allowed"
    panel.rows["/allowed"] = [{"name": "bad:name", "type": "dir", "path": "/outside", "size": 0}]
    source = PanelFileSource(panel, {"binding_verified": True, "canonical_root": "/allowed"}, skip_unsupported_names=True)
    with pytest.raises(BackupError, match="REMOTE_PATH_UNSAFE"):
        list(source.listdir(""))
