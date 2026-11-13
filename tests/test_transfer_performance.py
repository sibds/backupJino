import threading
from concurrent.futures import ThreadPoolExecutor

from jino_backup.dashboard_management import TaskControl
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.parallel import ConnectionBudget, ParallelCollector
from jino_backup.transfer import collect_files
from test_backup import FixtureSource


def test_artifact_hash_does_not_hold_the_only_downloader(tmp_path, monkeypatch):
    root = tmp_path / 'remote'; root.mkdir()
    for name in ('a.txt', 'b.txt'):
        (root / name).write_bytes(b'synthetic')
    downloaded = threading.Event()
    checked = []
    original = DirectoryCheckpoint.verify_done
    def verify(self, path, *args, **kwargs):
        checked.append(path)
        if path == 'a.txt':
            assert downloaded.wait(3), 'Artifact check blocked the second download'
        return original(self, path, *args, **kwargs)
    monkeypatch.setattr(DirectoryCheckpoint, 'verify_done', verify)
    import jino_backup.parallel as module
    from jino_backup.storage import sha256_file
    original_hash = getattr(module, 'sha256_file', sha256_file)
    def old_hash(path):
        if path.name == 'a.txt':
            assert downloaded.wait(3), 'Old synchronous hash blocked the downloader'
        return original_hash(path)
    monkeypatch.setattr(module, 'sha256_file', old_hash, raising=False)
    def factory(*args):
        source = FixtureSource(root)
        transfer = source.download
        def download(path, *args):
            transfer(path, *args)
            if path == 'b.txt':
                downloaded.set()
        source.download = download
        return source
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **e:None,
        ConnectionBudget(10), connections=2, metadata_factory=factory, download_factory=factory)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        result = collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    assert downloaded.is_set() and sorted(checked) == ['a.txt','b.txt']
    assert result['count'] == 2


def test_progress_updates_do_not_scan_the_checkpoint(tmp_path):
    root = tmp_path / 'remote'; root.mkdir(); (root / 'file').write_bytes(b'x')
    make = lambda *args:FixtureSource(root)
    events = []
    collector = ParallelCollector(TaskControl(lambda state:None), lambda **e:events.append(e),
        ConnectionBudget(10), metadata_factory=make, download_factory=make)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    class NoScan(dict):
        def values(self):
            raise AssertionError('Progress rescanned every checkpoint row')
    collector.committed = NoScan(collector.committed)
    collector.emit()
    collector.committed_file('second', 'partial', 5)
    collector.committed_file('second', 'partial', 8)
    collector.committed_file('second', 'done', 8)
    collector.committed_file('second', 'done', 8)
    assert events[-1]['received_bytes'] == 9 and events[-1]['verified_files'] == 2


def test_metadata_retry_does_not_reduce_file_parallelism(tmp_path):
    root = tmp_path / 'remote'; root.mkdir(); (root / 'file').write_bytes(b'x')
    make = lambda *args:FixtureSource(root)
    collector = ParallelCollector(TaskControl(lambda state:None), lambda **e:None,
        ConnectionBudget(10), metadata_factory=make, download_factory=make)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    collector.emit(stage='RETRY_WAIT', operation='MLSD', directory='/')
    assert collector.transfer_limit == 2
    collector.emit(stage='RETRY_WAIT', operation='RETR')
    assert collector.transfer_limit == 1


def test_small_new_file_has_no_empty_checkpoint_fsync(tmp_path, monkeypatch):
    root = tmp_path / 'remote'; root.mkdir(); (root / 'file').write_bytes(b'x')
    import jino_backup.downloads as module
    syncs = []
    monkeypatch.setattr(module.os, 'fsync', lambda fd:syncs.append(fd))
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        syncs.clear()  # The ownership marker is separately made durable on entry.
        collect_files(FixtureSource(root), tmp_path / 'copy/files', max_bytes=100,
                      max_files=10, timeout_seconds=10, checkpoint=checkpoint)
    assert len(syncs) == 1


def test_exact_checkpoint_boundary_is_not_fsynced_twice(tmp_path, monkeypatch):
    import jino_backup.downloads as module
    root = tmp_path / 'remote'; root.mkdir(); (root / 'file').write_bytes(b'x' * (8 * module.CHUNK))
    syncs = []
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        monkeypatch.setattr(module.os, 'fsync', lambda fd:syncs.append(fd))
        collect_files(FixtureSource(root), tmp_path / 'copy/files', max_bytes=16*module.CHUNK,
                      max_files=10, timeout_seconds=10, checkpoint=checkpoint)
    assert len(syncs) == 1


def test_corrupted_new_artifact_cannot_be_counted_as_verified(tmp_path, monkeypatch):
    root = tmp_path / 'remote'; root.mkdir(); (root / 'file').write_bytes(b'good')
    make = lambda *args:FixtureSource(root)
    original = DirectoryCheckpoint.fetch
    def fetch(self, source, relative, entry, local, deadline):
        original(self, source, relative, entry, local, deadline)
        local.write_bytes(b'evil')
    monkeypatch.setattr(DirectoryCheckpoint, 'fetch', fetch)
    collector = ParallelCollector(TaskControl(lambda state:None), lambda **e:None,
        ConnectionBudget(10), metadata_factory=make, download_factory=make)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        result = collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    assert result['count'] == 0 and result['errors'][0]['error_code'] == 'LOCAL_FILE_CHANGED'


def test_idle_downloader_yields_connection_to_another_server(tmp_path):
    first = tmp_path / 'first'; first.mkdir(); (first / 'a').write_bytes(b'a')
    (first / 'child').mkdir()
    second = tmp_path / 'second'; second.mkdir(); (second / 'b').write_bytes(b'b')
    downloaded, connected = threading.Event(), threading.Event()
    budget = ConnectionBudget(2)
    controls = [TaskControl(lambda state:None) for _ in range(2)]
    def metadata_one(*args):
        source = FixtureSource(first)
        listing = source.listdir
        def listdir(path):
            if path == 'child':
                assert downloaded.wait(3)
                assert connected.wait(3), 'An idle data connection starved another server'
            return listing(path)
        source.listdir = listdir
        return source
    def metadata_two(*args):
        source = FixtureSource(second)
        enter = source.__class__.__enter__
        class Source(FixtureSource):
            def __enter__(self):
                connected.set()
                return enter(self)
        return Source(second)
    def download_one(*args):
        source = FixtureSource(first)
        transfer = source.download
        def download(*args):
            transfer(*args)
            downloaded.set()
        source.download = download
        return source
    def run(index, metadata, downloader):
        collector = ParallelCollector(controls[index], lambda **e:None, budget, connections=2,
            metadata_factory=metadata, download_factory=downloader)
        with DirectoryCheckpoint(tmp_path / f'copy{index}', 'scope') as checkpoint:
            return collector.collect({}, {'remote_root':'/'}, tmp_path / f'copy{index}', checkpoint)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            one = pool.submit(run, 0, metadata_one, download_one)
            assert downloaded.wait(3)
            two = pool.submit(run, 1, metadata_two, lambda *args:FixtureSource(second))
            assert one.result(timeout=5)['count'] == 1
            assert two.result(timeout=5)['count'] == 1
        finally:
            for control in controls:
                control.request('stop')
    assert budget.snapshot()['used'] == 0
