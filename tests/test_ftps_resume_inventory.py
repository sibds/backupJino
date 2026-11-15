import hashlib
import threading

import pytest
from jino_backup.dashboard_management import TaskControl
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.errors import BackupError
from jino_backup.parallel import ConnectionBudget,ParallelCollector
from test_backup import FixtureSource


def collector(metadata,downloader):
    return ParallelCollector(TaskControl(lambda state:None),lambda **event:None,ConnectionBudget(10),
                             metadata_factory=metadata,download_factory=downloader)


def test_partial_continues_before_root_listing(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'big.bin').write_bytes(b'123456789')
    downloaded=threading.Event();offsets=[]
    def metadata(*args):
        source=FixtureSource(remote);original=source.listdir
        def listing(path):
            assert downloaded.wait(2),'Resume waited for inventory'
            return original(path)
        source.listdir=listing;return source
    def downloader(*args):
        source=FixtureSource(remote);original=source.download
        def download(path,output,offset):
            offsets.append(offset);original(path,output,offset);downloaded.set()
        source.download=download;return source
    copy=tmp_path/'copy'
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        entry=FixtureSource(remote).stat('big.bin')
        checkpoint.record('big.bin',entry,'partial',3,hashlib.sha256(b'123').hexdigest())
        (checkpoint.work/(hashlib.sha256(b'big.bin').hexdigest()+'.part')).write_bytes(b'123')
        result=collector(metadata,downloader).collect({}, {'remote_root':'/'},copy,checkpoint)
    assert offsets==[3] and result['count']==1
    assert (copy/'files/big.bin').read_bytes()==b'123456789'


def test_interrupted_walk_reuses_listing_and_continues_frontier(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'child').mkdir()
    (remote/'child/new.txt').write_bytes(b'new')
    first=True;listed=[]
    def metadata(*args):
        source=FixtureSource(remote);original=source.listdir
        def listing(path):
            listed.append(path)
            if first and path=='child':raise BackupError('TEST_INTERRUPTION')
            if not first and path=='':pytest.fail('Completed root listing repeated')
            return original(path)
        source.listdir=listing;return source
    copy=tmp_path/'copy'
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        with pytest.raises(BackupError,match='TEST_INTERRUPTION'):
            collector(metadata,lambda *args:FixtureSource(remote)).collect({}, {'remote_root':'/'},copy,checkpoint)
    first=False
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        result=collector(metadata,lambda *args:FixtureSource(remote)).collect({}, {'remote_root':'/'},copy,checkpoint)
    assert listed.count('')==1 and result['count']==1


def test_finished_job_refreshes_inventory_on_explicit_rerun(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'a.txt').write_bytes(b'a')
    copy=tmp_path/'copy';make=lambda *args:FixtureSource(remote)
    for count in (1,2):
        with DirectoryCheckpoint(copy,'scope') as checkpoint:
            result=collector(make,make).collect({}, {'remote_root':'/'},copy,checkpoint)
        assert result['count']==count
        (remote/'b.txt').write_bytes(b'b')


def test_verified_file_is_not_hashed_again_after_interruption(tmp_path,monkeypatch):
    remote=tmp_path/'remote';remote.mkdir();(remote/'old.txt').write_bytes(b'old');(remote/'child').mkdir()
    (remote/'child/new.txt').write_bytes(b'new')
    verified=threading.Event();first=True
    original=DirectoryCheckpoint.verify_done
    def verify(self,path,*args,**kwargs):
        if path=='old.txt' and not first:pytest.fail('Verification restarted from the beginning')
        value=original(self,path,*args,**kwargs)
        if path=='old.txt':verified.set()
        return value
    monkeypatch.setattr(DirectoryCheckpoint,'verify_done',verify)
    def metadata(*args):
        source=FixtureSource(remote);listing=source.listdir
        def listdir(path):
            if first and path=='child':
                assert verified.wait(2)
                # Wait for the verification receipt, not merely its local hash.
                import time
                time.sleep(.05)
                raise BackupError('TEST_INTERRUPTION')
            return listing(path)
        source.listdir=listdir;return source
    copy=tmp_path/'copy'
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        with pytest.raises(BackupError,match='TEST_INTERRUPTION'):
            collector(metadata,lambda *args:FixtureSource(remote)).collect({}, {'remote_root':'/'},copy,checkpoint)
    first=False
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        assert collector(metadata,lambda *args:FixtureSource(remote)).collect({}, {'remote_root':'/'},copy,checkpoint)['count']==2


def test_inventory_root_binding_cannot_change(tmp_path):
    from jino_backup.resume_inventory import ResumeInventory
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        ResumeInventory(checkpoint,'/allowed',100)
        with pytest.raises(BackupError,match='DOWNLOAD_SCOPE_CHANGED'):
            ResumeInventory(checkpoint,'/other',100)


def test_invalid_saved_listing_is_not_used(tmp_path):
    from jino_backup.resume_inventory import ResumeInventory
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        inventory.listing('',lambda:[])
        with checkpoint.db:checkpoint.db.execute("UPDATE walk_listings SET payload='[broken]'")
        with pytest.raises(BackupError,match='LOCAL_PROGRESS_INVALID'):
            inventory.listing('',lambda:pytest.fail('Corrupt cache silently replaced'))


def test_changed_local_file_invalidates_verification_receipt(tmp_path):
    from jino_backup.resume_inventory import ResumeInventory
    remote=tmp_path/'remote';remote.mkdir();(remote/'a').write_bytes(b'good')
    copy=tmp_path/'copy';source=FixtureSource(remote)
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        entry=source.stat('a');checkpoint.fetch(source,'a',entry,copy/'files/a',float('inf'))
        item=checkpoint.verify_done('a',entry,TaskControl(lambda state:None))
        inventory.verified('a',entry,item,inventory.signature('a'))
        assert inventory.receipt('a',entry)==item
        (copy/'files/a').write_bytes(b'evil')
        assert inventory.receipt('a',entry) is None


def test_partial_below_remote_link_is_never_followed(tmp_path):
    from jino_backup.transfer import Entry
    copy=tmp_path/'copy'
    def metadata(*args):
        class Source:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def stat(self,path):return Entry('parent','link',0,'old')
        return Source()
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        checkpoint.record('parent/a',Entry('a','file',9,'old'),'partial',3,hashlib.sha256(b'123').hexdigest())
        with pytest.raises(BackupError,match='REMOTE_TYPE_UNSAFE'):
            collector(metadata,lambda *args:pytest.fail('Link followed')).collect({}, {'remote_root':'/'},copy,checkpoint)
