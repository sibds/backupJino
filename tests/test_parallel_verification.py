import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from jino_backup.dashboard_management import TaskControl
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.parallel import ConnectionBudget, ParallelCollector
from test_backup import FixtureSource


def test_slow_saved_checks_do_not_block_later_downloads(tmp_path, monkeypatch):
    root = tmp_path / 'remote'
    root.mkdir()
    for index in range(40):
        (root / f'old{index:02}.txt').write_bytes(b'saved')
    make = lambda *args: FixtureSource(root)
    def collector(factory=make):
        return ParallelCollector(TaskControl(lambda state: None), lambda **e: None,
            ConnectionBudget(10), connections=2, metadata_factory=make, download_factory=factory)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        collector().collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    (root / 'zz-new.txt').write_bytes(b'new')
    downloaded = threading.Event()
    checks = []
    original = checkpoint.fetch
    def gated_fetch(source, relative, *args, **kwargs):
        if relative.startswith('old'):
            checks.append(relative)
            assert downloaded.wait(3), 'Saved verification blocked new transfer'
        return original(source, relative, *args, **kwargs)
    # Gate the old shared-worker path and the new dedicated local-check path.
    monkeypatch.setattr(checkpoint, 'fetch', gated_fetch)
    if hasattr(checkpoint, 'verify_done'):
        original_verify = checkpoint.verify_done
        def gated_verify(relative, *args, **kwargs):
            if relative.startswith('old'):
                checks.append(relative)
                assert downloaded.wait(3), 'Local verification blocked new transfer'
            return original_verify(relative, *args, **kwargs)
        monkeypatch.setattr(checkpoint, 'verify_done', gated_verify)
    def downloader(*args):
        source = make()
        original_download = source.download
        def download(relative, *args):
            assert relative == 'zz-new.txt'
            original_download(relative, *args)
            downloaded.set()
        source.download = download
        return source
    with checkpoint:
        result = collector(downloader).collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    assert downloaded.is_set() and len(checks) == 40
    assert result['count'] == 41 and not result.get('errors')


def test_corrupt_saved_file_is_not_accepted_or_redownloaded(tmp_path):
    root = tmp_path / 'remote'; root.mkdir()
    (root / 'old.txt').write_bytes(b'saved')
    make = lambda *args: FixtureSource(root)
    def run():
        collector = ParallelCollector(TaskControl(lambda state: None), lambda **e: None,
            ConnectionBudget(10), metadata_factory=make, download_factory=make)
        with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
            return collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    run()
    (tmp_path / 'copy/files/old.txt').write_bytes(b'other')
    result = run()
    assert result['count'] == 0
    assert result['errors'][0]['error_code'] == 'LOCAL_FILE_CHANGED'
    assert (tmp_path / 'copy/files/old.txt').read_bytes() == b'other'


def test_saved_checks_use_no_download_sessions_and_are_cancellable(tmp_path, monkeypatch):
    from jino_backup.errors import BackupError
    root = tmp_path / 'remote'; root.mkdir()
    (root / 'old.txt').write_bytes(b'saved')
    make = lambda *args: FixtureSource(root)
    control = TaskControl(lambda state: None)
    budget = ConnectionBudget(10)
    def run(factory):
        collector = ParallelCollector(control, lambda **e: None, budget,
            metadata_factory=make, download_factory=factory)
        with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
            return collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    run(make)
    def no_session(*args):
        pytest.fail('A saved file must not open a data session')
    assert run(no_session)['count'] == 1
    entered, release = threading.Event(), threading.Event()
    original = DirectoryCheckpoint.verify_done
    def verify(self, relative, entry, task_control, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(self, relative, entry, task_control, **kwargs)
    monkeypatch.setattr(DirectoryCheckpoint, 'verify_done', verify)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run, no_session)
        assert entered.wait(3)
        control.request('stop')
        release.set()
        with pytest.raises(BackupError, match='CANCELLED'):
            future.result(timeout=3)
    assert budget.snapshot()['used'] == 0
    assert budget.verification_slot.acquire(blocking=False)
    budget.verification_slot.release()


def test_source_change_during_local_verification_remains_partial(tmp_path, monkeypatch):
    root = tmp_path / 'remote'; root.mkdir()
    remote = root / 'old.txt'; remote.write_bytes(b'saved')
    make = lambda *args: FixtureSource(root)
    def run():
        collector = ParallelCollector(TaskControl(lambda state: None), lambda **e: None,
            ConnectionBudget(10), metadata_factory=make, download_factory=make)
        with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
            return collector.collect({}, {'remote_root':'/'}, tmp_path / 'copy', checkpoint)
    run()
    original = DirectoryCheckpoint.verify_done
    def verify(*args, **kwargs):
        item = original(*args, **kwargs)
        remote.write_bytes(b'changed source')
        return item
    monkeypatch.setattr(DirectoryCheckpoint, 'verify_done', verify)
    result = run()
    assert result['count'] == 0 and result['errors'][0]['error_code'] == 'SOURCE_CHANGED'
