import io
import json

import pytest

from jino_backup.errors import BackupError
from jino_backup.transfer import collect_files


class Response(io.BytesIO):
    status=200
    def __init__(self,data):
        super().__init__(data)
        self.headers={'Content-Length':str(len(data))}


class FilePanel:
    root_file_token='/'
    def __init__(self):
        self.files={'/a.txt':b'one','/folder/b.txt':b'two'}
        self.rows={
            '/':[{'name':'a.txt','type':'file','path':'/a.txt','size':3,'mtime':1},
                 {'name':'folder','type':'dir','path':'/folder','size':0,'mtime':1},
                 {'name':'link','type':'dir','path':'/link','targetType':'dir','targetDisplayPath':'/outside','size':0}],
            '/folder':[{'name':'b.txt','type':'file','path':'/folder/b.txt','size':3,'mtime':1}]}
        self.transfer=self
    def _list(self,token):
        return self.rows[token]
    def open(self,path,**kwargs):
        return Response(self.files[path])


def test_https_source_downloads_regular_tree_and_records_links(tmp_path):
    from jino_backup.panel_downloads import PanelFileSource
    source=PanelFileSource(FilePanel(),{'binding_verified':True,'canonical_root':'/'})
    result=collect_files(source,tmp_path,max_bytes=100,max_files=100,timeout_seconds=10)
    assert result['count']==2 and result['bytes']==6
    assert (tmp_path/'folder/b.txt').read_bytes()==b'two'
    assert source.links_count==1 and not (tmp_path/'link').exists()


def test_https_source_rejects_directory_outside_account_root():
    from jino_backup.panel_downloads import PanelFileSource
    panel=FilePanel()
    panel.root_file_token='/account'
    panel.rows['/account']=[{'name':'escape','type':'dir','path':'/other','size':0}]
    source=PanelFileSource(panel,{'binding_verified':True,'canonical_root':'/account'})
    with pytest.raises(BackupError,match='REMOTE_PATH_UNSAFE'):
        list(source.listdir(''))


def test_ignored_range_resumes_only_after_comparing_remote_prefix():
    from jino_backup.panel_downloads import PanelFileSource
    import hashlib
    panel=FilePanel()
    source=PanelFileSource(panel,{'binding_verified':True,'canonical_root':'/'})
    list(source.listdir(''))
    out=io.BytesIO()
    out.digest=hashlib.sha256(b'o')
    source.download('a.txt',out,1)
    assert out.getvalue()==b'ne'
    out.digest=hashlib.sha256(b'x')
    with pytest.raises(BackupError,match='SOURCE_CHANGED'):
        source.download('a.txt',out,1)


def test_expired_file_token_is_refreshed_once_without_reauthorizing_account():
    from jino_backup.panel_downloads import PanelFileSource
    panel=FilePanel()
    open_file=panel.open
    calls=[]
    def open_once(path,**kwargs):
        calls.append(path)
        if len(calls)==1:
            raise BackupError('FILETRANSFER_HTTP_FAILED',http_status=401)
        return open_file(path,**kwargs)
    panel.open=open_once
    refreshed=[]
    panel.prepare_file_transport=lambda scope:refreshed.append(True)
    source=PanelFileSource(panel,{'binding_verified':True,'canonical_root':'/'})
    list(source.listdir(''))
    output=io.BytesIO()
    source.download('a.txt',output,0)
    assert output.getvalue()==b'one' and len(calls)==2 and refreshed==[True]


def test_partial_collection_keeps_other_files_when_one_is_unreadable(tmp_path):
    from test_backup import FixtureSource
    root=tmp_path/'source'
    root.mkdir()
    (root/'a.txt').write_bytes(b'a')
    (root/'b.txt').write_bytes(b'b')
    source=FixtureSource(root)
    download=source.download
    def broken(relative,outgoing,offset):
        if relative=='a.txt':
            raise BackupError('TRANSFER_DENIED')
        download(relative,outgoing,offset)
    source.download=broken
    result=collect_files(source,tmp_path/'out',max_bytes=100,max_files=100,
                         timeout_seconds=10,continue_on_error=True)
    assert result['count']==1 and result['errors']==[{'path':'a.txt','error_code':'TRANSFER_DENIED'}]
    assert (tmp_path/'out/b.txt').read_bytes()==b'b'


def test_latest_snapshot_is_based_on_source_date_and_mysql_kind():
    from jino_backup.panel_downloads import latest_mysql_snapshot
    rows=[{'index':0,'label':'Копия от 9 ноября 2026','kinds':'MySQL'},
          {'index':1,'label':'Копия от 10 ноября 2026','kinds':'Файлы'},
          {'index':2,'label':'Копия от 8 ноября 2026','kinds':'MySQL'}]
    assert latest_mysql_snapshot(rows)=='2026-11-09'
    assert latest_mysql_snapshot([]) is None


def test_virtual_root_uses_observed_account_root_when_breadcrumb_home_is_a_label():
    from jino_backup.panel_downloads import canonical_account_root
    assert canonical_account_root('~','/')=='/'
    assert canonical_account_root('/account','/')=='/account'
    with pytest.raises(BackupError,match='REMOTE_ROOT_UNVERIFIED'):
        canonical_account_root('~','/unknown')


def test_file_job_resume_verifies_local_copy_without_network(tmp_path):
    from jino_backup.panel_downloads import PanelFileJob,PanelFileSource
    calls=[]
    def factory():
        calls.append(True)
        return PanelFileSource(FilePanel(),{'binding_verified':True,'canonical_root':'/'})
    job=PanelFileJob(tmp_path,'server-example','client-login','batch',factory)
    first=job.run()
    assert first['status']=='FILES_ONLY' and first['files_count']==2
    assert first['links_count']==1 and first['backup_complete'] is False
    assert job.run()['already_downloaded'] and len(calls)==1
    assert 'client-login' not in json.dumps(first)
