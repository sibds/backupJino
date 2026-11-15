from jino_backup.transfer import collect_files
from jino_backup.parallel import ConnectionBudget,ParallelCollector
from jino_backup.dashboard_management import TaskControl
from jino_backup.downloads import DirectoryCheckpoint
from test_backup import FixtureSource


def test_managed_batch_accepts_no_total_deadline(tmp_path):
    from jino_backup.bulk import DownloadBatch,Server,ServerPlan
    remote=tmp_path/'remote';remote.mkdir();(remote/'data.txt').write_bytes(b'complete')
    plan=ServerPlan({},[Server('synthetic-server',1,
        {'protocol':'sftp','host':'example.test','username':'synthetic','port':2222},'synthetic-login')])
    batch=DownloadBatch(plan,tmp_path/'copy','synthetic-run','/',lambda *args:FixtureSource(remote),timeout_seconds=None)
    result=batch.run()['servers'][0]
    assert result['status']=='FILES_ONLY' and result['files_count']==1


def test_sequential_transfer_can_finish_without_total_deadline(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'data.txt').write_bytes(b'complete')
    result=collect_files(FixtureSource(remote),tmp_path/'copy',max_bytes=100,max_files=10,timeout_seconds=None)
    assert result['bytes']==8 and result['count']==1


def test_parallel_transfer_can_finish_without_total_deadline(tmp_path):
    remote=tmp_path/'remote';remote.mkdir();(remote/'data.txt').write_bytes(b'complete')
    factory=lambda *args:FixtureSource(remote)
    collector=ParallelCollector(TaskControl(lambda _:None),lambda **_:None,ConnectionBudget(10),
        metadata_factory=factory,download_factory=factory,timeout_seconds=None)
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        result=collector.collect({}, {'remote_root':'/'},tmp_path/'copy',checkpoint)
    assert result['bytes']==8 and result['count']==1
