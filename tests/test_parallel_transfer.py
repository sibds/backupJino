import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from jino_backup.errors import BackupError
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.dashboard_management import TaskControl
from test_backup import FixtureSource


def test_parallelism_recovers_only_after_cooldown_and_confirmed_progress(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    root = tmp_path / "remote"
    root.mkdir()
    (root / "file.txt").write_bytes(b"synthetic")
    events = []
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **e: events.append(e),
        ConnectionBudget(10), connections=4,
        metadata_factory=lambda *args: FixtureSource(root),
        download_factory=lambda *args: FixtureSource(root))
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    collector.emit(stage="RETRY_WAIT")
    collector.emit(stage="RETRY_WAIT")
    assert collector.transfer_limit == 1
    collector.committed_file("file.txt", "done", 9)
    assert collector.transfer_limit == 1
    collector.recovery_at = 0
    collector.committed_file("file.txt", "done", 9)
    assert collector.transfer_limit == 1, "Unchanged checkpoint is not progress"
    collector.committed_file("second.txt", "partial", 10)
    assert collector.transfer_limit == 2
    collector.committed_file("second.txt", "partial", 11)
    assert collector.transfer_limit == 2, "Recovery must be gradual"
    collector.recovery_at = 0
    collector.committed_file("second.txt", "done", 12)
    assert collector.transfer_limit == 3
    collector.recovery_at = 0
    collector.committed_file("third.txt", "done", 1)
    assert collector.transfer_limit == 3


def test_shared_connection_budget_is_bounded_and_released():
    from jino_backup.parallel import ConnectionBudget
    budget = ConnectionBudget(10)
    control = TaskControl(lambda state: None)
    entered, release = threading.Event(), threading.Event()
    counts = []
    def worker(index):
        with budget.lease(control):
            counts.append(budget.snapshot()["used"])
            if len(counts) >= 10:
                entered.set()
            release.wait(3)
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = [pool.submit(worker, i) for i in range(15)]
        assert entered.wait(2)
        assert budget.snapshot() == {"used": 10, "limit": 10}
        release.set()
        for future in futures:
            future.result(timeout=3)
    assert max(counts) <= 10 and budget.snapshot()["used"] == 0


def test_files_download_during_inventory_with_two_independent_workers(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    root = tmp_path / "remote"
    root.mkdir()
    for name in ("a.txt", "b.txt"):
        (root / name).write_bytes(b"synthetic")
    (root / "child").mkdir()
    downloaded = threading.Event()
    simultaneous = threading.Barrier(2)
    offsets = []
    def downloader(credentials, site, metadata, control):
        source = FixtureSource(root)
        original = source.download
        def transfer(relative, outgoing, offset):
            simultaneous.wait(timeout=3)
            downloaded.set()
            offsets.append((relative, offset))
            original(relative, outgoing, offset)
        source.download = transfer
        return source
    events = []
    def notice(**event):
        events.append(event)
    def metadata_factory(*args):
        source = FixtureSource(root)
        original = source.listdir
        def listing(relative):
            if relative == "child":
                assert downloaded.wait(3), "Files waited for the complete inventory"
            return original(relative)
        source.listdir = listing
        return source
    budget = ConnectionBudget(10)
    collector = ParallelCollector(TaskControl(lambda state: None), notice, budget,
        connections=3, metadata_factory=metadata_factory, download_factory=downloader)
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        result = collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    assert result["count"] == 2 and len(offsets) == 2
    assert all((tmp_path / "copy/files" / name).read_bytes() == b"synthetic" for name, _ in offsets)
    assert budget.snapshot()["used"] == 0
    assert any(event.get("inventory_complete") for event in events)


def test_parallel_transfer_preserves_committed_parts_and_resumes(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    root = tmp_path / "remote"
    root.mkdir()
    (root / "a.txt").write_bytes(b"first-file")
    offsets, delays = [], []
    broken = True
    def downloader(*args):
        source = FixtureSource(root)
        def transfer(relative, outgoing, offset):
            offsets.append(offset)
            if broken:
                outgoing.write(b"first-file"[offset:offset+1] if offset < 3 else b"")
                raise BackupError("TRANSFER_INTERRUPTED")
            outgoing.write(b"first-file"[offset:])
        source.download = transfer
        return source
    control = TaskControl(lambda state: None)
    collector = ParallelCollector(control, lambda **event: None, ConnectionBudget(10),
        connections=2, metadata_factory=lambda *args: FixtureSource(root), download_factory=downloader,
        wait=delays.append)
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        with pytest.raises(BackupError, match="TRANSFER_INTERRUPTED"):
            collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    assert offsets == [0, 1, 2, 3, 3, 3, 3, 3] and delays == [2, 2, 2, 4, 8, 16, 32]
    broken = False
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    assert offsets[-1] == 3
    assert (tmp_path / "copy/files/a.txt").read_bytes() == b"first-file"


def test_waiting_for_global_connection_slot_can_be_stopped():
    from jino_backup.parallel import ConnectionBudget
    budget = ConnectionBudget(1)
    control = TaskControl(lambda state: None)
    stopped = threading.Event()
    def waiting():
        try:
            with budget.lease(control):
                pytest.fail("A full budget granted another slot")
        except BackupError as error:
            assert error.code == "CANCELLED"
            stopped.set()
    with budget.lease(TaskControl(lambda state: None)):
        worker = threading.Thread(target=waiting)
        worker.start()
        control.request("stop")
        assert stopped.wait(1)
        worker.join(timeout=1)
        assert budget.snapshot()["used"] == 1
    assert budget.snapshot()["used"] == 0


def test_productive_interruptions_continue_beyond_five_reconnections(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    root = tmp_path / "remote"
    root.mkdir()
    content = b"0123456789"
    (root / "a.txt").write_bytes(content)
    offsets, events = [], []
    def downloader(*args):
        source = FixtureSource(root)
        def transfer(relative, outgoing, offset):
            offsets.append(offset)
            outgoing.write(content[offset:offset+1])
            if offset < 9:
                raise BackupError("TRANSFER_INTERRUPTED", operation="RETR", curl_error_code=56,
                                  curl_error_kind="CONNECTION_RESET")
        source.download = transfer
        return source
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **event: events.append(event),
        ConnectionBudget(10), connections=2, metadata_factory=lambda *args: FixtureSource(root),
        download_factory=downloader, wait=lambda seconds: None)
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        assert collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)["count"] == 1
    assert offsets == list(range(10))
    assert (tmp_path / "copy/files/a.txt").read_bytes() == content
    retries = [event for event in events if event["stage"] == "RETRY_WAIT"]
    assert len(retries) == 9
    assert all(event["current_file"] == "a.txt" and event["curl_error_code"] == 56 for event in retries)
    assert [event["committed_bytes"] for event in retries] == list(range(1, 10))


def test_parallel_log_keeps_the_failing_files_identity():
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    events = []
    collector = ParallelCollector(None, lambda **event: events.append(event), ConnectionBudget(10))
    collector.mutex, collector.emit_mutex = threading.RLock(), threading.RLock()
    collector.active = {1: {"path": "first.txt", "size_bytes": 20, "received_bytes": 10},
                        2: {"path": "failed.txt", "size_bytes": 30, "received_bytes": 12}}
    collector.committed, collector.transfer_limit = {}, 2
    collector.planned_bytes, collector.planned_files, collector.directories_seen = 50, 2, 1
    collector.inventory_complete = True
    collector.emit(stage="RETRY_WAIT", current_file="failed.txt")
    assert events[-1]["current_file"] == "failed.txt"
    assert events[-1]["current_bytes"] == 12
    collector.emit(stage="RETRY_WAIT", current_file="", directory="child")
    assert events[-1]["current_file"] == "" and events[-1]["directory"] == "child"


def test_partial_directory_error_is_retained_while_accessible_files_download(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    from test_ftp_listing import DeniedDirectorySource
    events = []
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **event: events.append(event),
        ConnectionBudget(10), connections=3, metadata_factory=lambda *args: DeniedDirectorySource(),
        download_factory=lambda *args: DeniedDirectorySource())
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        result = collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    assert result["count"] == 1
    assert result["errors"][0]["path"] == "blocked"
    assert (tmp_path / "copy/files/index.txt").read_bytes() == b"data"
    assert not events[-1]["inventory_complete"]


def test_recovery_reduces_new_parallel_transfers_without_losing_files(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    root = tmp_path / "remote"
    root.mkdir()
    for index in range(8):
        (root / (str(index) + ".txt")).write_bytes(b"data")
    events, failures = [], threading.Event()
    mutex = threading.Lock()
    def downloader(*args):
        source = FixtureSource(root)
        original = source.download
        def transfer(relative, outgoing, offset):
            with mutex:
                fail = not failures.is_set()
                failures.set()
            if fail:
                raise BackupError("TRANSFER_INTERRUPTED")
            original(relative, outgoing, offset)
        source.download = transfer
        return source
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **event: events.append(event),
        ConnectionBudget(10), connections=3, metadata_factory=lambda *args: FixtureSource(root),
        download_factory=downloader, wait=lambda seconds: None)
    with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
        result = collector.collect({}, {"remote_root": "/"}, tmp_path / "copy", checkpoint)
    assert result["count"] == 8
    assert events[-1]["parallel_limit"] == 1
    assert collector.budget.snapshot()["used"] == 0


def test_pause_is_confirmed_only_when_all_parallel_workers_are_paused():
    import time
    states = []
    control = TaskControl(states.append)
    barrier = threading.Barrier(3)
    second_ready = threading.Event()
    def worker(delayed=False):
        control.register_worker()
        barrier.wait(timeout=2)
        if delayed:
            second_ready.wait(2)
        try:
            control.checkpoint()
        finally:
            control.finish_worker()
    workers = [threading.Thread(target=worker), threading.Thread(target=worker, args=(True,))]
    for worker_thread in workers:
        worker_thread.start()
    control.request("pause")
    barrier.wait(timeout=2)
    deadline = time.monotonic() + 1
    while not control.waiting_workers and time.monotonic() < deadline:
        time.sleep(0.01)
    assert "PAUSED" not in states
    second_ready.set()
    deadline = time.monotonic() + 1
    while "PAUSED" not in states and time.monotonic() < deadline:
        time.sleep(0.01)
    assert "PAUSED" in states
    control.request("resume")
    for worker_thread in workers:
        worker_thread.join(timeout=1)
        assert not worker_thread.is_alive()


@pytest.mark.parametrize("value", [1, 6, True, "3", 0])
def test_connection_limits_outside_two_to_five_are_rejected(value):
    from jino_backup.parallel import ParallelCollector, ConnectionBudget
    with pytest.raises(BackupError, match="CONNECTION_LIMIT_INVALID"):
        ParallelCollector(TaskControl(lambda state: None), lambda **event: None,
                          ConnectionBudget(10), connections=value)
