import threading
import time
from concurrent.futures import ThreadPoolExecutor
from jino_backup.parallel import ConnectionBudget,ParallelCollector
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.dashboard_management import TaskControl
from test_backup import FixtureSource
import pytest
from contextlib import ExitStack
from jino_backup.errors import BackupError
from jino_backup.parallel import MetadataSession,RunControl


def test_large_tree_yields_scan_capacity_to_another_server(tmp_path):
    first=tmp_path/'large'; first.mkdir()
    for index in range(60):(first/str(index)).mkdir()
    second=tmp_path/'small';second.mkdir();(second/'needed.txt').write_bytes(b'needed')
    entered=threading.Event();downloaded=threading.Event()
    budget=ConnectionBudget(10);budget.scan_slots=threading.Semaphore(1)
    def run(root,target,slow=False):
        class Source(FixtureSource):
            def listdir(self,path):
                if slow:
                    entered.set()
                    if path:time.sleep(.03)
                return super().listdir(path)
            def download(self,*args):
                result=super().download(*args)
                if not slow:downloaded.set()
                return result
        with DirectoryCheckpoint(target,'scope') as checkpoint:
            return ParallelCollector(TaskControl(lambda s:None),lambda **e:None,budget,connections=2,
                metadata_factory=lambda *a:Source(root),download_factory=lambda *a:Source(root)).collect({}, {'remote_root':'/'},target,checkpoint)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(run,first,tmp_path/'copy-large',True)
        try:
            assert entered.wait(2)
            b=pool.submit(run,second,tmp_path/'copy-small')
            assert downloaded.wait(.8),'One whole tree monopolized the shared scan slot'
        finally:
            # An empty large tree may report FILES_MISSING; let every worker exit.
            try:a.result(timeout=8)
            except BackupError as error:assert error.code=='FILES_MISSING'
        assert b.result(timeout=4)['count']==1


def test_verification_persistence_does_not_lock_transfer_progress(tmp_path,monkeypatch):
    from jino_backup.resume_inventory import ResumeInventory
    remote=tmp_path/'remote';remote.mkdir();(remote/'file.txt').write_bytes(b'content')
    collector=ParallelCollector(TaskControl(lambda s:None),lambda **e:None,ConnectionBudget(10),connections=2,
        metadata_factory=lambda *a:FixtureSource(remote),download_factory=lambda *a:FixtureSource(remote))
    original=ResumeInventory.verified
    def verified(self,*args):
        available=threading.Event()
        def probe():
            with collector.mutex:available.set()
        thread=threading.Thread(target=probe,daemon=True);thread.start()
        assert available.wait(.5),'Local receipt persistence retained the transfer-progress mutex'
        thread.join(1)
        return original(self,*args)
    monkeypatch.setattr(ResumeInventory,'verified',verified)
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        assert collector.collect({}, {'remote_root':'/'},tmp_path/'copy',checkpoint)['count']==1


def test_routine_progress_is_coalesced_but_errors_and_final_state_are_delivered(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'file.txt').write_bytes(b'content')
    events=[]
    collector=ParallelCollector(TaskControl(lambda s:None),lambda **e:events.append(e),ConnectionBudget(10),connections=2,
        metadata_factory=lambda *a:FixtureSource(remote),download_factory=lambda *a:FixtureSource(remote))
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        collector.collect({}, {'remote_root':'/'},tmp_path/'copy',checkpoint)
        events.clear()
        for index in range(100):collector.emit(stage='SCANNING',log=True,directory=str(index))
        assert len(events)<=2,'Routine progress synchronously writes once per file or directory'
        collector.emit(stage='FILE_ERROR',log=True,error_code='SOURCE_CHANGED')
        assert events[-1]['error_code']=='SOURCE_CHANGED'
        collector.emit(stage='FILES_DOWNLOAD',force=True)
        assert events[-1]['stage']=='FILES_DOWNLOAD'


def test_saved_verifiers_do_not_hold_all_connections_before_another_server_downloads(tmp_path,monkeypatch):
    old=tmp_path/'old';old.mkdir();(old/'old.txt').write_bytes(b'old')
    new=tmp_path/'new';new.mkdir();(new/'new.txt').write_bytes(b'new')
    release=threading.Event();downloaded=threading.Event();ready=threading.Event()
    lock=threading.Lock();opened=set()
    original=DirectoryCheckpoint.verify_done
    def verify(self,path,*args,**kwargs):
        if path=='old.txt':release.wait(8)
        return original(self,path,*args,**kwargs)
    monkeypatch.setattr(DirectoryCheckpoint,'verify_done',verify)
    def run(index):
        remote=old if index<3 else new
        def metadata(*args):
            class Metadata(FixtureSource):
                def __enter__(self):
                    with lock:
                        if index<3:opened.add(index)
                        if len(opened)==3:ready.set()
                    return super().__enter__()
            return Metadata(remote)
        def downloader(*args):
            source=FixtureSource(remote);fetch=source.download
            def download(path,stream,offset):
                fetch(path,stream,offset)
                if index==3:downloaded.set()
            source.download=download;return source
        copy=tmp_path/('copy'+str(index))
        with DirectoryCheckpoint(copy,'scope') as checkpoint:
            if index<3:
                source=FixtureSource(old)
                checkpoint.fetch(source,'old.txt',source.stat('old.txt'),checkpoint.file_path('old.txt'),float('inf'))
            return ParallelCollector(TaskControl(lambda s:None),lambda **e:None,budget,connections=2,
                metadata_factory=metadata,download_factory=downloader).collect({}, {'remote_root':'/'},copy,checkpoint)
    budget=ConnectionBudget(4)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures=[pool.submit(run,i) for i in range(3)]
        try:
            assert ready.wait(3)
            futures.append(pool.submit(run,3))
            assert downloaded.wait(3),'Old verification jobs retained every connection slot'
        finally:release.set()
        assert all(f.result(timeout=6)['count']==1 for f in futures)
    assert budget.snapshot()['used']==0


def test_data_capacity_keeps_room_for_metadata():
    budget=ConnectionBudget(10);control=TaskControl(lambda s:None)
    with ExitStack() as stack:
        for _ in range(7):stack.enter_context(budget.lease(control,role='data'))
        for _ in range(3):stack.enter_context(budget.lease(control,role='metadata'))
        assert budget.snapshot()['used']==10
        assert budget.data_used==7 and budget.metadata_used==3
    assert budget.snapshot()['used']==0


def test_busy_metadata_is_not_closed_and_reopened_root_must_match():
    budget=ConnectionBudget(4);entered=threading.Event();release=threading.Event()
    control=RunControl(TaskControl(lambda s:None),threading.Event(),float('inf'))
    class Source:
        canonical_root='/allowed'
        closed=0
        def __enter__(self):return self
        def __exit__(self,*args):self.closed+=1
    source=Source();session=MetadataSession(source,{'remote_root':'/allowed'},control,lambda **e:None,budget)
    def operation():
        entered.set();release.wait(3);return 'ok'
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(session.call,operation)
        try:
            assert entered.wait(1)
            assert not session.release_idle(0)
            assert source.closed==0
        finally:release.set()
        assert future.result(timeout=2)=='ok'
    session.opened_at=session.last_used=0
    assert session.release_idle(0)
    assert budget.snapshot()['used']==0
    source.canonical_root='/wrong'
    with pytest.raises(BackupError,match='DOWNLOAD_SCOPE_CHANGED'):
        session.call(lambda:pytest.fail('Operation ran outside original root'))
    assert budget.snapshot()['used']==0 and not session.connected


def test_parent_validation_cannot_lose_its_session_between_commands():
    budget=ConnectionBudget(4);gap=threading.Event();release=threading.Event()
    control=RunControl(TaskControl(lambda s:None),threading.Event(),float('inf'))
    class Parent:
        @property
        def kind(self):
            gap.set();release.wait(3);return 'dir'
    class Source:
        canonical_root='/allowed'
        closed=0
        def __enter__(self):return self
        def __exit__(self,*args):self.closed+=1
        def stat(self,path):return Parent()
    source=Source();session=MetadataSession(source,{'remote_root':'/allowed'},control,lambda **e:None,budget)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(session.validate_parents,'parent/file')
        try:
            assert gap.wait(1)
            session.opened_at=session.last_used=0
            assert not session.release_idle(0)
        finally:release.set()
        future.result(timeout=2)
    session.close()
    assert budget.snapshot()['used']==0


def test_other_protocols_cannot_consume_metadata_reserve_when_ftps_is_present():
    budget=ConnectionBudget(10)
    control=TaskControl(lambda s:None);release=threading.Event();entered=threading.Event()
    metadata=MetadataSession(object(),{'remote_root':'/'},None,None,budget)
    def eighth():
        try:
            with budget.lease(control):entered.set();release.wait(2)
        except BackupError as error:assert error.code=='CANCELLED'
    with ExitStack() as stack:
        for _ in range(7):stack.enter_context(budget.lease(TaskControl(lambda s:None)))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(eighth)
            try:assert not entered.wait(.2),'Other sessions consumed the metadata reserve'
            finally:control.request('stop');release.set()
            future.result(timeout=2)


def test_known_transfers_do_not_wait_for_full_scan_capacity(tmp_path):
    from jino_backup.resume_inventory import ResumeInventory
    root=tmp_path/'remote';root.mkdir();(root/'file').write_bytes(b'new')
    copy=tmp_path/'copy';downloaded=threading.Event();budget=ConnectionBudget(10)
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        ResumeInventory(checkpoint,'/',100).listing('',lambda:FixtureSource(root).listdir(''))
    def downloader(*args):
        source=FixtureSource(root);fetch=source.download
        def download(path,stream,offset):fetch(path,stream,offset);downloaded.set()
        source.download=download;return source
    def run():
        with DirectoryCheckpoint(copy,'scope') as checkpoint:
            return ParallelCollector(TaskControl(lambda s:None),lambda **e:None,budget,
                metadata_factory=lambda *args:FixtureSource(root),download_factory=downloader).collect({}, {'remote_root':'/'},copy,checkpoint)
    for _ in range(3):budget.scan_slots.acquire()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(run)
        try:assert downloaded.wait(2),'Known transfer waited for an unrelated full tree scan'
        finally:
            for _ in range(3):budget.scan_slots.release()
        assert future.result(timeout=3)['count']==1


def test_cached_local_receipt_checks_share_the_local_verification_limit(tmp_path,monkeypatch):
    from jino_backup.resume_inventory import ResumeInventory
    root=tmp_path/'remote';root.mkdir()
    for i in range(8):(root/str(i)).write_bytes(b'saved')
    for index in range(3):
        with DirectoryCheckpoint(tmp_path/str(index),'scope') as checkpoint:
            source=FixtureSource(root)
            for entry in source.listdir(''):
                checkpoint.fetch(source,entry.name,entry,checkpoint.file_path(entry.name),float('inf'))
    budget=ConnectionBudget(10);lock=threading.Lock();active=peak=0
    original=ResumeInventory.receipt
    def receipt(self,*args):
        nonlocal active,peak
        with lock:active+=1;peak=max(peak,active)
        try:time.sleep(.03);return original(self,*args)
        finally:
            with lock:active-=1
    monkeypatch.setattr(ResumeInventory,'receipt',receipt)
    def run(index):
        with DirectoryCheckpoint(tmp_path/str(index),'scope') as checkpoint:
            return ParallelCollector(TaskControl(lambda s:None),lambda **e:None,budget,
                metadata_factory=lambda *a:FixtureSource(root),download_factory=lambda *a:FixtureSource(root)).collect({}, {'remote_root':'/'},tmp_path/str(index),checkpoint)
    with ThreadPoolExecutor(max_workers=3) as pool:
        assert all(result['count']==8 for result in pool.map(run,range(3)))
    assert peak==1


def test_remote_postcheck_does_not_occupy_local_hash_slot(tmp_path,monkeypatch):
    old=tmp_path/'old';old.mkdir();(old/'old').write_bytes(b'old')
    new=tmp_path/'new';new.mkdir();(new/'new').write_bytes(b'new')
    copy=tmp_path/'copy-old';waiting=threading.Event();release=threading.Event();verified=threading.Event()
    with DirectoryCheckpoint(copy,'scope') as checkpoint:
        source=FixtureSource(old)
        checkpoint.fetch(source,'old',source.stat('old'),checkpoint.file_path('old'),float('inf'))
    budget=ConnectionBudget(10);original=DirectoryCheckpoint.verify_done
    def verify(self,path,*args,**kwargs):
        result=original(self,path,*args,**kwargs)
        if path=='new':verified.set()
        return result
    monkeypatch.setattr(DirectoryCheckpoint,'verify_done',verify)
    def metadata(*args):
        source=FixtureSource(old);stat=source.stat;calls=0
        def checked(path):
            nonlocal calls
            calls+=1
            if calls==3: # root listing stat, pre-check, then post-check
                waiting.set();release.wait(4)
            return stat(path)
        source.stat=checked;return source
    def run(remote,target,factory):
        with DirectoryCheckpoint(target,'scope') as checkpoint:
            return ParallelCollector(TaskControl(lambda s:None),lambda **e:None,budget,
                metadata_factory=factory,download_factory=lambda *a:FixtureSource(remote)).collect({}, {'remote_root':'/'},target,checkpoint)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(run,old,copy,metadata)
        try:
            assert waiting.wait(2)
            second=pool.submit(run,new,tmp_path/'copy-new',lambda *a:FixtureSource(new))
            assert verified.wait(2),'A network check retained the local hash slot'
        finally:release.set()
        assert first.result(timeout=3)['count']==1
        assert second.result(timeout=3)['count']==1


def test_metadata_cleanup_cannot_race_idle_reclamation():
    budget=ConnectionBudget(10);first=threading.Event();second=threading.Event();release=threading.Event();calls=[]
    control=RunControl(TaskControl(lambda s:None),threading.Event(),float('inf'))
    class Source:
        canonical_root='/allowed'
        def __enter__(self):return self
        def __exit__(self,*args):
            calls.append(1)
            (first if len(calls)==1 else second).set()
            release.wait(3)
    session=MetadataSession(Source(),{'remote_root':'/allowed'},control,lambda **e:None,budget)
    session.open()
    with ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(session.close)
        try:
            assert first.wait(1)
            b=pool.submit(session.close)
            assert not second.wait(.2),'Metadata teardown ran concurrently'
        finally:release.set()
        a.result(timeout=2);b.result(timeout=2)
    assert budget.snapshot()['used']==0
