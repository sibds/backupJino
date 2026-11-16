import time
import pytest
from jino_backup.dashboard_management import TaskControl
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.errors import BackupError
from jino_backup.parallel import ConnectionBudget,ParallelCollector
from test_backup import FixtureSource
from test_dashboard_management import management


def run_copy(remote,copy,metadata):
    collector=ParallelCollector(TaskControl(lambda _:None),lambda **_:None,ConnectionBudget(10),
        metadata_factory=metadata,download_factory=lambda *args:FixtureSource(remote))
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        checkpoint.resolve_conflicts=True
        checkpoint.refresh_changed_sources=True
        return collector.collect({}, {'remote_root':'/'},copy,checkpoint)


def test_changed_file_downloads_current_bytes_and_keeps_other_files(tmp_path):
    remote=tmp_path/'remote';remote.mkdir()
    (remote/'change.txt').write_bytes(b'old');(remote/'other.txt').write_bytes(b'other')
    def metadata(*args):
        source=FixtureSource(remote);listing=source.listdir
        def listdir(path):
            entries=listing(path)
            if not path:(remote/'change.txt').write_bytes(b'updated source')
            return entries
        source.listdir=listdir;return source
    result=run_copy(remote,tmp_path/'copy',metadata)
    assert not result.get('errors') and result['count']==2
    assert (tmp_path/'copy/files/change.txt').read_bytes()==b'updated source'


@pytest.mark.parametrize('persistent',[False,True])
def test_changed_child_directory_does_not_abort_other_downloads(tmp_path,persistent):
    remote=tmp_path/'remote';remote.mkdir();(remote/'child').mkdir()
    (remote/'child/data.txt').write_bytes(b'data');(remote/'other.txt').write_bytes(b'other')
    calls=[]
    def metadata(*args):
        source=FixtureSource(remote);listing=source.listdir
        def listdir(path):
            if path=='child':
                calls.append(path)
                if persistent or len(calls)==1:raise BackupError('SOURCE_CHANGED')
            return listing(path)
        source.listdir=listdir;return source
    result=run_copy(remote,tmp_path/'copy',metadata)
    assert (tmp_path/'copy/files/other.txt').read_bytes()==b'other'
    assert bool(result.get('errors'))==persistent
    if not persistent:assert result['count']==2


def test_owned_source_with_same_timestamp_can_update_without_losing_old_bytes(tmp_path):
    import os
    remote=tmp_path/'remote';remote.mkdir();file=remote/'data.txt';file.write_bytes(b'old')
    copy=tmp_path/'copy';entry=FixtureSource(remote).stat('data.txt')
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        checkpoint.fetch(FixtureSource(remote),'data.txt',entry,None,time.monotonic()+10)
        stamp=file.stat().st_mtime_ns
        file.write_bytes(b'updated');os.utime(file,ns=(stamp,stamp))
        checkpoint.resolve_conflicts=True;checkpoint.refresh_changed_sources=True
        checkpoint.fetch(FixtureSource(remote),'data.txt',FixtureSource(remote).stat('data.txt'),None,time.monotonic()+10)
        assert checkpoint.file_path('data.txt').read_bytes()==b'updated'
        assert any(p.read_bytes()==b'old' for p in (checkpoint.work/'preserved').iterdir())


def test_sequential_source_refreshes_stale_listing(tmp_path):
    from jino_backup.transfer import collect_files
    remote=tmp_path/'remote';remote.mkdir();(remote/'data.txt').write_bytes(b'old')
    source=FixtureSource(remote);listing=source.listdir
    def changed(path):
        entries=listing(path)
        (remote/'data.txt').write_bytes(b'new contents')
        return entries
    source.listdir=changed
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        checkpoint.refresh_changed_sources=True;checkpoint.resolve_conflicts=True
        result=collect_files(source,tmp_path/'copy/files',max_bytes=100,max_files=10,timeout_seconds=10,
            checkpoint=checkpoint,continue_on_error=True)
        assert result['count']==1 and not result.get('errors')
        assert checkpoint.file_path('data.txt').read_bytes()==b'new contents'


def test_verifier_refreshes_owned_copy_changed_after_listing(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();file=remote/'data.txt';file.write_bytes(b'old')
    copy=tmp_path/'copy'
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        checkpoint.fetch(FixtureSource(remote),'data.txt',FixtureSource(remote).stat('data.txt'),None,time.monotonic()+10)
    def metadata(*args):
        source=FixtureSource(remote);listing=source.listdir
        def changed(path):
            entries=listing(path)
            if not path:file.write_bytes(b'updated during verification')
            return entries
        source.listdir=changed;return source
    result=run_copy(remote,copy,metadata)
    assert not result.get('errors') and result['count']==1
    assert (copy/'files/data.txt').read_bytes()==b'updated during verification'


def test_changed_source_partial_is_retried_with_same_task_key(management):
    from test_dashboard_management import wait_status
    management.runner=lambda *args:{'status':'PARTIAL','source_changed_count':1,'files_count':1}
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'SCHEDULED')
    current=management.task(task['id'])
    assert current['stage']=='SOURCE_REFRESH_WAIT'
    assert current['auto_resume_error']=='SOURCE_CHANGED'
    assert len(management.tasks())==1


def test_partial_walk_retains_completed_receipts_for_retry(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'child').mkdir();(remote/'other.txt').write_bytes(b'other')
    def metadata(*args):
        source=FixtureSource(remote);listing=source.listdir
        def changed(path):
            if path=='child':raise BackupError('SOURCE_CHANGED')
            return listing(path)
        source.listdir=changed;return source
    run_copy(remote,tmp_path/'copy',metadata)
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        assert checkpoint.db.execute('SELECT finished FROM walk_state').fetchone()[0]==0
        assert checkpoint.db.execute('SELECT count(*) FROM walk_verified').fetchone()[0]==1


def test_refresh_does_not_overwrite_an_operator_modified_local_file(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'data.txt').write_bytes(b'source old')
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        checkpoint.fetch(FixtureSource(remote),'data.txt',FixtureSource(remote).stat('data.txt'),None,time.monotonic()+10)
        checkpoint.file_path('data.txt').write_bytes(b'operator changes')
        (remote/'data.txt').write_bytes(b'source updated')
        checkpoint.resolve_conflicts=True;checkpoint.refresh_changed_sources=True
        with pytest.raises(BackupError,match='FILE_CONFLICT_PENDING'):
            checkpoint.fetch(FixtureSource(remote),'data.txt',FixtureSource(remote).stat('data.txt'),None,time.monotonic()+10)
        assert checkpoint.file_path('data.txt').read_bytes()==b'operator changes'
