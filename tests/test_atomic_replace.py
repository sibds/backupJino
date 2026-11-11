import errno
from pathlib import Path

import pytest

from jino_backup import state


@pytest.mark.parametrize("winerror", [5, 32, 33])
def test_atomic_replace_retries_windows_locks_without_losing_the_original(
    tmp_path, monkeypatch, winerror
):
    target = tmp_path / "report.json"
    source = tmp_path / "replacement.json"
    target.write_bytes(b"original")
    source.write_bytes(b"replacement")
    original_replace = state.os.replace
    attempts = []
    delays = []

    def temporarily_locked(source_path, target_path):
        attempts.append(1)
        if len(attempts) < 3:
            assert Path(target_path).read_bytes() == b"original"
            error = PermissionError(errno.EACCES, "Synthetic lock")
            error.winerror = winerror
            raise error
        original_replace(source_path, target_path)

    monkeypatch.setattr(state.os, "replace", temporarily_locked)
    monkeypatch.setattr(state.time, "sleep", delays.append)
    state.replace_with_retry(source, target)
    assert len(attempts) == 3
    assert delays == [0.02, 0.04]
    assert target.read_bytes() == b"replacement"
    assert not source.exists()


@pytest.mark.parametrize("winerror,expected_attempts", [(5, 6), (None, 1)])
def test_atomic_replace_bounds_failures_and_retains_both_files(
    tmp_path, monkeypatch, winerror, expected_attempts
):
    target = tmp_path / "report.json"
    source = tmp_path / "replacement.json"
    target.write_bytes(b"original")
    source.write_bytes(b"replacement")
    attempts = []

    def denied(*args):
        attempts.append(1)
        error = PermissionError(errno.EACCES, "Synthetic denial")
        error.winerror = winerror
        raise error

    monkeypatch.setattr(state.os, "replace", denied)
    monkeypatch.setattr(state.time, "sleep", lambda seconds: None)
    with pytest.raises(PermissionError):
        state.replace_with_retry(source, target)
    assert len(attempts) == expected_attempts
    assert target.read_bytes() == b"original"
    assert source.read_bytes() == b"replacement"


@pytest.mark.parametrize("writer", ["json", "private"])
def test_state_writers_recover_from_a_temporary_replacement_failure(
    tmp_path, monkeypatch, writer
):
    from jino_backup.panel_export import atomic_bytes

    target = tmp_path / "state.data"
    target.write_bytes(b"original")
    original_replace = state.os.replace
    attempts = []

    def temporarily_locked(source_path, target_path):
        attempts.append(1)
        if len(attempts) == 1:
            error = PermissionError(errno.EACCES, "Synthetic lock")
            error.winerror = 5
            raise error
        original_replace(source_path, target_path)

    monkeypatch.setattr(state.os, "replace", temporarily_locked)
    monkeypatch.setattr(state.time, "sleep", lambda seconds: None)
    if writer == "json":
        state.write_json(target, {"status": "saved"})
        assert target.read_text().strip() == '{\n  "status": "saved"\n}'
    else:
        atomic_bytes(target, b"replacement")
        assert target.read_bytes() == b"replacement"
    assert len(attempts) == 2
    assert not list(tmp_path.glob("*.partial-*"))
