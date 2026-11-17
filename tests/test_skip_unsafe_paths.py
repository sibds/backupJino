import pytest
from jino_backup.errors import BackupError
from jino_backup.transfer import Entry,collect_files,name_compatibility_issue
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.dashboard_management import TaskControl
from jino_backup.parallel import ParallelCollector,ConnectionBudget
from test_backup import FixtureSource
from test_dashboard_management import management


@pytest.mark.parametrize('parallel',[False,True])
def test_unsafe_names_subtrees_and_transfers_are_logged_and_skipped(tmp_path,parallel):
    root=tmp_path/'source';root.mkdir()
    (root/'good.txt').write_bytes(b'good')
    (root/'blocked').mkdir();(root/'blocked'/'secret.txt').write_bytes(b'never read')
    (root/'unsafe.txt').write_bytes(b'unsafe')
    accessed=[];events=[]
    class Source(FixtureSource):
        def listdir(self,path):
            accessed.append(('list',path))
            if path=='blocked':raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='OUTSIDE_ALLOWED_ROOT')
            values=super().listdir(path)
            return values+[Entry('../outside','file',10,None)] if not path else values
        def download(self,path,outgoing,offset):
            accessed.append(('download',path))
            if path=='unsafe.txt':raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='SOURCE_NAME_CHANGED')
            return super().download(path,outgoing,offset)
    output=tmp_path/'copy'
    with DirectoryCheckpoint(output,'scope',events.append) as checkpoint:
        if parallel:
            collector=ParallelCollector(TaskControl(lambda state:None),lambda **event:events.append(event),ConnectionBudget(10),
                metadata_factory=lambda *args:Source(root),download_factory=lambda *args:Source(root),skip_unsupported_names=True)
            result=collector.collect({}, {'remote_root':'/'},output,checkpoint)
        else:
            with Source(root) as source:
                result=collect_files(source,output/'files',max_bytes=100000,max_files=100,timeout_seconds=None,
                    checkpoint=checkpoint,continue_on_error=True,skip_unsupported_names=True)
    assert (output/'files/good.txt').read_bytes()==b'good'
    omissions=[item for item in result.get('skipped',[]) if item['error_code']=='REMOTE_PATH_UNSAFE']
    assert len(omissions)==3
    assert all(item.get('reason') for item in omissions)
    assert not any('../outside' in path or 'secret.txt' in path for _,path in accessed)
    logged=[event for event in events if event.get('stage')=='FILE_SKIPPED' and event.get('error_code')=='REMOTE_PATH_UNSAFE']
    assert len(logged)==3 and all(event.get('skipped_path') and event.get('skip_reason') for event in logged)


@pytest.mark.parametrize('name,reason',[('../outside','EMBEDDED_SEPARATOR'),('..','DOT_SEGMENT'),('a\x00b','CONTROL_CHARACTER'),('','EMPTY_NAME')])
def test_unsafe_name_errors_include_machine_readable_reason(name,reason):
    with pytest.raises(BackupError) as caught:name_compatibility_issue(name)
    assert caught.value.details['unsafe_reason']==reason


def test_private_managed_log_keeps_reason_path_and_warning_level(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management.progress(task['id'],{'stage':'FILE_SKIPPED','log':True,'error_code':'REMOTE_PATH_UNSAFE',
        'skipped_path':'folder/bad\\nname','skipped_kind':'file','skip_reason':'CONTROL_CHARACTER',
        'unsafe_reason':'CONTROL_CHARACTER','unsafe_name':'bad\\nname'})
    event=management.events(1,stage='FILE_SKIPPED')[0]
    assert event['level']=='warning' and event['skip_reason']=='CONTROL_CHARACTER'
    assert event['unsafe_reason']=='CONTROL_CHARACTER' and event['file']=='folder/bad\\nname'
    assert '\n' not in event['file']


def test_database_unsafe_export_is_logged_without_names_and_other_databases_continue(tmp_path):
    from test_panel_export import PanelFixture,SQL
    from jino_backup.panel_export import PanelExportJob
    from cryptography.fernet import Fernet
    class Panel(PanelFixture):
        def download(self,scope,remote,path,limit):
            if remote['path'].endswith('private_database_one.sql'):
                raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='OUTSIDE_ALLOWED_ROOT')
            path.write_bytes(SQL)
    panel=Panel();events=[]
    result=PanelExportJob(tmp_path/'state',tmp_path/'copy','panel-example','request','2026-11-10',
        Fernet.generate_key(),lambda:panel,defer_export_conflicts=True,notify=events.append).run()
    assert result['status']=='PARTIAL' and result['databases_count']==1
    skipped=[event for event in events if event['stage']=='SQL_SKIPPED']
    assert len(skipped)==1 and skipped[0]['unsafe_reason']=='OUTSIDE_ALLOWED_ROOT'
    assert 'private_database' not in str(events)


def test_unsafe_root_is_reported_as_omitted_without_opening_file_sessions(tmp_path):
    class Source(FixtureSource):
        def __enter__(self):raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='OUTSIDE_ALLOWED_ROOT')
    collector=ParallelCollector(TaskControl(lambda state:None),lambda **event:None,ConnectionBudget(10),
        metadata_factory=lambda *args:Source(tmp_path),download_factory=lambda *args:pytest.fail('Unsafe root used for download'))
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        result=collector.collect({}, {'remote_root':'/allowed'},tmp_path/'copy',checkpoint)
    assert result['count']==0 and result['skipped'][0]['kind']=='dir'
    assert result['skipped'][0]['reason']=='OUTSIDE_ALLOWED_ROOT'


def test_strict_collection_still_rejects_unsafe_name(tmp_path):
    class Source:
        def listdir(self,path):return [Entry('../outside','file',1,None)]
    with pytest.raises(BackupError,match='REMOTE_PATH_UNSAFE'):
        collect_files(Source(),tmp_path/'strict',max_bytes=100,max_files=10,timeout_seconds=None)


def test_managed_https_skips_unsafe_tokens_without_losing_safe_siblings(tmp_path):
    import io
    from types import SimpleNamespace
    from jino_backup.panel_downloads import PanelFileSource
    used=[]
    entries=[{'name':'good.txt','path':'/home/good.txt','type':'file','size':4,'mtime':1},
             {'name':'bad.txt','path':'/outside/file','type':'file','size':4,'mtime':1},
             {'name':'bad-dir','path':'/outside/directory','type':'dir','size':0,'mtime':1}]
    def listing(token):
        used.append(token)
        assert token=='/home'
        return entries
    def incoming(token,offset):
        assert token=='/home/good.txt';return io.BytesIO(b'good')
    panel=SimpleNamespace(root_file_token='/home',_list=listing,transfer=SimpleNamespace(open=incoming))
    source=PanelFileSource(panel,{'binding_verified':True,'canonical_root':'/home'},continue_on_error=True)
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        result=collect_files(source,tmp_path/'copy/files',max_bytes=100,max_files=10,timeout_seconds=None,checkpoint=checkpoint,continue_on_error=True)
    assert result['count']==1 and len(result['skipped'])==2
    assert all(item['reason']=='OUTSIDE_ALLOWED_ROOT' for item in result['skipped'])
    assert not any(token.startswith('/outside') for token in used)
