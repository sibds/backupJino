import pytest

from jino_backup.errors import BackupError
from jino_backup.transfer import Entry, collect_files
from test_name_skipping import MemorySource


@pytest.mark.parametrize('kind', ['symlink', 'OS.unix=slink:/outside', 'fifo', 'socket', 'chr', 'blk'])
def test_known_special_objects_are_recorded_without_reading(tmp_path, kind):
    source = MemorySource([Entry('special', kind, 0), Entry('ok.txt', 'file', 2)])
    result = collect_files(source, tmp_path, max_bytes=1000, max_files=10,
                           timeout_seconds=10, skip_unsupported_types=True)
    assert result['count'] == 1
    assert result['skipped'] == result['errors']
    assert result['skipped'][0]['error_code'] == 'REMOTE_TYPE_UNSUPPORTED'
    assert '/outside' not in str(result)
    assert not (tmp_path / 'special').exists()
    assert source.visited == ['']


@pytest.mark.parametrize('enabled,kind,code', [
    (False, 'symlink', 'REMOTE_TYPE_UNSAFE'),
    (True, 'unsafe', 'REMOTE_TYPE_UNSAFE'),
    (True, 'unknown', 'REMOTE_TYPE_UNSAFE'),
])
def test_strict_or_unknown_types_still_fail(tmp_path, enabled, kind, code):
    with pytest.raises(BackupError, match=code):
        collect_files(MemorySource([Entry('special', kind, 0)]), tmp_path,
                      max_bytes=1000, max_files=10, timeout_seconds=10,
                      skip_unsupported_types=enabled)


def test_special_object_cannot_bypass_path_validation(tmp_path):
    with pytest.raises(BackupError, match='REMOTE_PATH_UNSAFE'):
        collect_files(MemorySource([Entry('../escape', 'symlink', 0)]), tmp_path,
                      max_bytes=1000, max_files=10, timeout_seconds=10,
                      skip_unsupported_types=True)


def test_parallel_special_objects_are_logged_and_partial(tmp_path):
    from jino_backup.parallel import ConnectionBudget, ParallelCollector
    from jino_backup.dashboard_management import TaskControl
    from jino_backup.downloads import DirectoryCheckpoint
    events = []
    factory = lambda *args: MemorySource([Entry('special', 'OS.unix=slink:/outside', 0), Entry('ok.txt', 'file', 2)])
    collector = ParallelCollector(TaskControl(lambda state: None), lambda **e: events.append(e),
        ConnectionBudget(10), metadata_factory=factory, download_factory=factory,
        skip_unsupported_types=True)
    with DirectoryCheckpoint(tmp_path / 'copy', 'scope') as checkpoint:
        result = collector.collect({}, {'remote_root': '/'}, tmp_path / 'copy', checkpoint)
    assert result['count'] == 1 and result['errors'] == result['skipped']
    assert any(e.get('stage') == 'FILE_SKIPPED' and e.get('skip_reason') == 'SYMLINK' for e in events)
    assert '/outside' not in str(events)
    assert collector.budget.snapshot()['used'] == 0


def test_bulk_special_omission_never_claims_full_files(tmp_path):
    import json
    from jino_backup.bulk import DownloadBatch, Server, ServerPlan
    factory = lambda *args: MemorySource([Entry('special', 'symlink', 0), Entry('ok.txt', 'file', 2)])
    plan = ServerPlan({}, [Server('synthetic-server', 1,
        {'protocol':'sftp', 'host':'example.test', 'username':'synthetic', 'port':2222}, 'synthetic-login')])
    result = DownloadBatch(plan, tmp_path, 'synthetic-run', '/', factory,
                           skip_unsupported_types=True).run()['servers'][0]
    assert result['status'] == 'PARTIAL' and result['files_skipped_count'] == 1
    manifest = json.loads(next(tmp_path.glob('*/*/manifest.json')).read_text())
    assert manifest['status'] == 'PARTIAL'
    assert manifest['files']['errors'] == manifest['files']['skipped']


def test_controlled_source_logs_special_objects(tmp_path):
    from jino_backup.dashboard_runner import ControlledSource
    from jino_backup.dashboard_management import TaskControl
    events = []
    source = MemorySource([Entry('special', 'symlink', 0), Entry('ok.txt', 'file', 2)])
    with ControlledSource(source, TaskControl(lambda state: None), lambda **e: events.append(e),
                          tmp_path, skip_unsupported_types=True):
        pass
    assert any(e.get('stage') == 'FILE_SKIPPED' and e.get('skip_reason') == 'SYMLINK' for e in events)
