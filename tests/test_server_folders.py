import json
import pytest
from cryptography.fernet import Fernet

from jino_backup.errors import BackupError
from jino_backup.importers import identity


def test_plan_uses_panel_login_for_both_files_and_database_folder(tmp_path):
    from jino_backup.bulk import plan_servers
    from jino_backup.panel import select_panel_source
    source=tmp_path/'source.json'
    source.write_text(json.dumps([{'id':'1','login':'client-login','password':'synthetic-panel',
        'ftp_host':'ftp.example.test','ftp_login':'different-ftp-login','ftp_password':'synthetic-ftp'}]))
    mapping={'id':'id','login':'login','password':'password','ftp_host':'ftp_host',
             'ftp_login':'ftp_login','ftp_password':'ftp_password','ftp_protocol':'ftps'}
    assert plan_servers(source,mapping).servers[0].folder_name=='client-login'
    assert select_panel_source(source,mapping,1).folder_name=='client-login'


def test_duplicate_logins_receive_readable_host_suffix():
    from jino_backup.server_folders import folder_names
    rows=[{'login':'Client','host':'one.example.test'},{'login':'client','host':'two.example.test'}]
    assert folder_names(rows,{'login':'login','ftp_host':'host'})==[
        'Client--one.example.test','client--two.example.test']
    with pytest.raises(BackupError,match='FOLDER_NAME_AMBIGUOUS'):
        folder_names([rows[0],rows[0]],{'login':'login','ftp_host':'host'})


def test_folder_choice_does_not_depend_on_other_rows_ftp_credentials(tmp_path):
    from jino_backup.bulk import plan_servers
    source=tmp_path/'source.json'
    source.write_text(json.dumps([
        {'id':'1','login':'client','ftp_host':'one.example.test','ftp_login':'u1','ftp_password':'synthetic'},
        {'id':'2','login':'client','ftp_host':'two.example.test','ftp_login':'u2'}]))
    mapping={'id':'id','login':'login','ftp_host':'ftp_host','ftp_login':'ftp_login',
             'ftp_password':'ftp_password','ftp_protocol':'ftps'}
    plan=plan_servers(source,mapping)
    assert plan.servers[0].folder_name=='client--one.example.test'
    assert plan.report['needs_review']==1


def test_windows_folder_names_are_safe_and_remain_readable():
    from jino_backup.server_folders import folder_names
    values=['normal.login','user@example.test','a/b:c','CON','..','name. ','.jino-locks']
    result=folder_names([{'login':x} for x in values],{'login':'login'})
    assert result[:4]==['normal.login','user@example.test','a_b_c','_CON']
    assert all(x not in {'','..','.'} and '/' not in x and ':' not in x and not x.endswith(('.', ' ')) for x in result)
    assert result[-1]!='.jino-locks'


def legacy_directory(root,server_id='server-example'):
    job=root/server_id/'download-example'
    job.mkdir(parents=True)
    (job/'.jino-owner.json').write_text(json.dumps({'schema_version':'1.0','scope':'synthetic-scope'}))
    (job/'keep.bin').write_bytes(b'original-data')
    return job


def test_rename_is_lossless_idempotent_and_dry_run_has_no_writes(tmp_path):
    from jino_backup.server_folders import server_directory
    old=legacy_directory(tmp_path)
    before={p.relative_to(old):p.read_bytes() for p in old.rglob('*') if p.is_file()}
    server_directory(tmp_path,'server-example','client-login',dry_run=True)
    assert old.exists() and not (tmp_path/'client-login').exists()
    server_directory(tmp_path,'server-example','client-login')
    assert not (tmp_path/'server-example').exists()
    target=server_directory(tmp_path,'server-example','client-login')/'download-example'
    assert before=={p.relative_to(target):p.read_bytes() for p in target.rglob('*') if p.is_file()}


def test_existing_user_folder_is_not_merged_or_overwritten(tmp_path):
    from jino_backup.server_folders import server_directory
    old=legacy_directory(tmp_path)
    (tmp_path/'client-login').mkdir()
    with pytest.raises(BackupError,match='SERVER_FOLDER_CONFLICT'):
        server_directory(tmp_path,'server-example','client-login')
    assert old.exists() and (old/'keep.bin').read_bytes()==b'original-data'


def test_unowned_legacy_folder_and_path_escape_are_rejected(tmp_path):
    from jino_backup.server_folders import server_directory
    old=tmp_path/'server-example'
    old.mkdir()
    (old/'user-file.txt').write_text('keep')
    with pytest.raises(BackupError,match='DESTINATION_NOT_OWNED'):
        server_directory(tmp_path,'server-example','client-login')
    with pytest.raises(BackupError):
        server_directory(tmp_path,'server-example','../outside')
    assert (old/'user-file.txt').read_text()=='keep'


def test_moved_panel_job_keeps_idempotency_and_never_logs_in_again(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    from test_panel_export import PanelFixture
    panel=PanelFixture()
    key=Fernet.generate_key()
    args=(tmp_path/'state',tmp_path/'output','panel-example','request-1','2026-11-10',key,lambda:panel)
    original=PanelExportJob(*args).run()
    assert original['status']=='DB_ONLY'
    count=len(panel.exports)
    def no_login():
        raise AssertionError('Unexpected login after folder rename')
    moved=PanelExportJob(*args[:-1],no_login,server_folder='client-login').run()
    assert moved['status']=='DB_ONLY' and moved['already_finished']
    assert len(panel.exports)==count
    assert len(list((tmp_path/'output'/'client-login').rglob('*.sql')))==2
    assert not (tmp_path/'output'/'server-example').exists()
    assert 'client-login' not in json.dumps(moved)


def test_bulk_download_continues_existing_checkpoint_under_login(tmp_path):
    from jino_backup.bulk import DownloadBatch,Server,ServerPlan
    from test_backup import FixtureSource
    source_dir=tmp_path/'source'
    source_dir.mkdir()
    (source_dir/'data.txt').write_bytes(b'synthetic data')
    credentials={'host':'ftp.example.test','username':'ftp-user','password':'synthetic','protocol':'ftps','port':21}
    server=Server('server-example',1,credentials)
    plan=ServerPlan({'rows_total':1,'servers_ready':1,'needs_review':0,'issues':[]},[server])
    assert DownloadBatch(plan,tmp_path/'out','batch','/',lambda *_:FixtureSource(source_dir)).run()['servers'][0]['status']=='FILES_ONLY'
    server.folder_name='client-login'
    def no_network(*args):
        raise AssertionError('Unexpected repeated download')
    result=DownloadBatch(plan,tmp_path/'out','batch','/',no_network).run()
    assert result['servers'][0]['already_downloaded']
    assert len(list((tmp_path/'out'/'client-login').rglob('data.txt')))==1
    assert 'client-login' not in json.dumps(result)


def test_cli_folder_migration_dry_run_never_exposes_logins(tmp_path):
    from test_cli import command
    source=tmp_path/'source.json'
    source.write_text('[{"id":"1","login":"private-client-login"}]')
    mapping=tmp_path/'mapping.json'
    mapping.write_text('{"id":"id","login":"login"}')
    root=tmp_path/'out'
    old=legacy_directory(root,identity('server','1'))
    state=tmp_path/'state'
    args=('--state-dir',state,'backup','name-server-folders','--file',source,
          '--mapping',mapping,'--destination',root)
    dry=command(*args)
    assert dry.returncode==0 and json.loads(dry.stdout)['would_rename']==1
    assert not state.exists() and old.exists()
    real=command(*args,'--execute')
    assert real.returncode==0 and json.loads(real.stdout)['renamed']==1
    assert (root/'private-client-login'/'download-example'/'keep.bin').exists()
    assert 'private-client-login' not in dry.stdout+real.stdout+dry.stderr+real.stderr
def test_file_transfer_and_database_export_use_independent_locks(tmp_path):
    from cryptography.fernet import Fernet
    from jino_backup.server_folders import server_work
    from jino_backup.panel_export import PanelExportJob
    from test_panel_export import PanelFixture
    panel = PanelFixture()
    destination = tmp_path / "copies"
    with server_work(destination, "server-example", "client-login", "files"):
        job = PanelExportJob(tmp_path / "state", destination, "panel-example", "request-1",
            "2026-11-10", Fernet.generate_key(), lambda: panel,
            server_folder="client-login")
        assert job.run()["status"] == "DB_ONLY"
        assert len(panel.exports) == 2
        assert not panel.deleted
        with pytest.raises(BackupError, match="DOWNLOAD_BUSY"):
            with server_work(destination, "server-example", "client-login", "files"):
                pass


@pytest.mark.parametrize('kind', ['files', 'databases'])
def test_migration_cannot_run_during_server_work(tmp_path, kind):
    from jino_backup.server_folders import server_work, migrate_folders
    from jino_backup.importers import identity
    rows = [{'id': 'synthetic', 'login': 'client-login'}]
    with server_work(tmp_path / 'copies', identity('server', 'synthetic'), 'client-login', kind):
        with pytest.raises(BackupError, match='DOWNLOAD_BUSY'):
            migrate_folders(rows, {'id': 'id', 'login': 'login'}, tmp_path / 'copies',
                            tmp_path / 'state', execute=True)
