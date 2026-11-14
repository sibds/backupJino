import time
from pathlib import Path

import pytest

from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.errors import BackupError
from jino_backup.transfer import Entry


class MemorySource:
    def __init__(self, files):
        self.files = files

    def __enter__(self): return self
    def __exit__(self, *args): pass
    def stat(self, path):
        data, modified = self.files[path]
        return Entry(path.rsplit('/', 1)[-1], 'file', len(data), modified)
    def listdir(self, path):
        return [self.stat(p) for p in self.files if '/' not in p]
    def download(self, path, output, offset): output.write(self.files[path][0][offset:])


def fetch(checkpoint, source, path):
    checkpoint.fetch(source, path, source.stat(path), None, time.monotonic()+30)


def test_identical_untracked_local_file_is_verified_and_adopted(tmp_path):
    source=MemorySource({'one.txt':(b'same', '20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy', 'scope') as cp:
        cp.resolve_conflicts=True
        (cp.root/'files').mkdir(); (cp.root/'files/one.txt').write_bytes(b'same')
        fetch(cp, source, 'one.txt')
        assert cp.db.execute("SELECT state FROM files").fetchone()[0]=='done'


def test_newer_remote_updates_owned_file_and_preserves_old_bytes(tmp_path):
    source=MemorySource({'one.txt':(b'old', '20261113100000')})
    with DirectoryCheckpoint(tmp_path/'copy', 'scope') as cp:
        cp.resolve_conflicts=True
        fetch(cp,source,'one.txt')
        source.files['one.txt']=(b'new content','20261114100000')
        fetch(cp,source,'one.txt')
        assert (cp.root/'files/one.txt').read_bytes()==b'new content'
        assert any(p.read_bytes()==b'old' for p in (cp.work/'preserved').rglob('*') if p.is_file())


def test_different_same_size_content_requires_decision_without_overwriting(tmp_path):
    source=MemorySource({'one.txt':(b'new', '20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy', 'scope') as cp:
        cp.resolve_conflicts=True
        (cp.root/'files').mkdir(); (cp.root/'files/one.txt').write_bytes(b'old')
        with pytest.raises(BackupError) as caught: fetch(cp,source,'one.txt')
        assert caught.value.code=='FILE_CONFLICT_PENDING'
        conflict=caught.value.details['conflict']
        assert conflict['remote_path']=='one.txt' and conflict['local_path']=='one.txt'
        assert conflict['same_content'] is False and conflict['local_size']==conflict['remote_size']==3
        assert (cp.root/'files/one.txt').read_bytes()==b'old'
        cp.conflict_decision=lambda key:'keep_both'
        fetch(cp,source,'one.txt')
        assert cp.file_path('one.txt').read_bytes()==b'new'
        assert (cp.root/'files/one.txt').read_bytes()==b'old'


@pytest.mark.parametrize('identical',[True,False])
def test_remote_case_collision_does_not_stop_unrelated_download(tmp_path, identical):
    from jino_backup.parallel import ConnectionBudget,ParallelCollector
    from jino_backup.dashboard_management import TaskControl
    source=MemorySource({'Logo.txt':(b'one','20261114100000'),
                         'logo.txt':(b'one' if identical else b'two','20261114100000'),
                         'other.txt':(b'other','20261114100000')})
    events=[]
    collector=ParallelCollector(TaskControl(lambda state:None),lambda **e:events.append(e),ConnectionBudget(10),
        metadata_factory=lambda *args:source, download_factory=lambda *args:source)
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        result=collector.collect({}, {'remote_root':'/'},cp.root,cp)
        assert (cp.root/'files/other.txt').read_bytes()==b'other'
        if identical:
            assert result['duplicates'][0]['source_path']=='logo.txt'
            assert not result.get('errors')
        else:
            assert result['errors'][0]['error_code']=='FILE_CONFLICT_PENDING'
            assert any(e.get('conflict',{}).get('remote_path')=='logo.txt' for e in events)


def test_keep_both_resumes_same_checkpoint_and_manifest_paths_are_readable(tmp_path):
    from jino_backup.transfer import collect_files
    source=MemorySource({'one.txt':(b'new','20261114100000'),'other.txt':(b'other','20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        (cp.root/'files').mkdir(); (cp.root/'files/one.txt').write_bytes(b'old')
        first=collect_files(source,cp.root/'files',max_bytes=10000,max_files=100,timeout_seconds=30,
                            checkpoint=cp,continue_on_error=True)
        assert first['errors'][0]['error_code']=='FILE_CONFLICT_PENDING'
        cp.conflict_decision=lambda key:'keep_both'
        result=collect_files(source,cp.root/'files',max_bytes=10000,max_files=100,timeout_seconds=30,
                             checkpoint=cp,continue_on_error=True)
        assert not result.get('errors')
        assert (cp.root/'files/one.txt').read_bytes()==b'old'
        assert all((cp.root/'files'/item['path']).is_file() for item in result['items'])


def test_remote_case_collision_keep_both_records_original_paths(tmp_path):
    from jino_backup.transfer import collect_files
    source=MemorySource({'Logo.txt':(b'one','20261114100000'),'logo.txt':(b'two','20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        cp.conflict_decision=lambda key:'keep_both'
        result=collect_files(source,cp.root/'files',max_bytes=10000,max_files=100,timeout_seconds=30,
                             checkpoint=cp,continue_on_error=True)
        assert not result.get('errors')
        assert {item.get('source_path',item['path']) for item in result['items']}=={'Logo.txt','logo.txt'}
        assert {(cp.root/'files'/item['path']).read_bytes() for item in result['items']}=={b'one',b'two'}


def test_directory_at_local_file_destination_is_pending_then_kept_both(tmp_path):
    source=MemorySource({'one.txt':(b'new','20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        (cp.root/'files/one.txt').mkdir(parents=True)
        with pytest.raises(BackupError) as caught: fetch(cp,source,'one.txt')
        assert caught.value.code=='FILE_CONFLICT_PENDING'
        cp.conflict_decision=lambda key:'keep_both'
        fetch(cp,source,'one.txt')
        assert (cp.root/'files/one.txt').is_dir()
        assert cp.file_path('one.txt').read_bytes()==b'new'


def test_directory_case_decision_applies_to_children_without_reprompting(tmp_path):
    from jino_backup.transfer import collect_files
    class Tree(MemorySource):
        def stat(self,path):
            if path in {'Images','images'}: return Entry(path,'dir',0,'20261114100000')
            return super().stat(path)
        def listdir(self,path):
            return [self.stat(p) for p in (['Images','images'] if not path else [path+'/logo.txt'])]
    source=Tree({'Images/logo.txt':(b'one','20261114100000'),'images/logo.txt':(b'two','20261114100000')})
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        asked=[]
        cp.conflict_decision=lambda key:asked.append(key) or 'keep_both'
        result=collect_files(source,cp.root/'files',max_bytes=1000,max_files=100,timeout_seconds=30,
                             checkpoint=cp,continue_on_error=True)
        assert not result.get('errors') and len(asked)==1
        assert {(cp.root/'files'/x['path']).read_bytes() for x in result['items']}=={b'one',b'two'}


def test_source_drift_during_verification_stays_partial(tmp_path):
    from jino_backup.parallel import ConnectionBudget,ParallelCollector
    from jino_backup.dashboard_management import TaskControl
    source=MemorySource({'one.txt':(b'old','20261113100000')})
    with DirectoryCheckpoint(tmp_path/'copy','scope') as cp:
        cp.resolve_conflicts=True
        fetch(cp,source,'one.txt')
        old=source.stat('one.txt')
        source.files['one.txt']=(b'new','20261114100000')
        source.listdir=lambda path:[old]
        collector=ParallelCollector(TaskControl(lambda state:None),lambda **e:None,ConnectionBudget(10),
            metadata_factory=lambda *args:source,download_factory=lambda *args:source)
        result=collector.collect({}, {'remote_root':'/'},cp.root,cp)
        assert result['errors'][0]['error_code']=='SOURCE_CHANGED'
