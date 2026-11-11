import json
from pathlib import Path

from cryptography.fernet import Fernet

from jino_backup.errors import BackupError


def test_batch_continues_after_account_error_and_never_claims_complete(tmp_path):
    from jino_backup.account_batch import run_workers
    tasks=[1,2,3]
    def worker(row,update):
        if row==1:
            raise BackupError('AUTH_FAILED')
        update(stage='FILES_DOWNLOAD',bytes_saved=10)
        return {'status':'FILES_ONLY','files_count':1,'backup_complete':False}
    result=run_workers(tasks,worker,tmp_path/'report.json',workers=2)
    assert result['finished']
    assert [x['status'] for x in result['servers']]==['FAILED','FILES_ONLY','FILES_ONLY']
    assert all(x.get('backup_complete') is not True for x in result['servers'])
    assert json.loads((tmp_path/'report.json').read_text())['finished']


def test_completed_db_cache_requires_scope_database_set_and_local_integrity(tmp_path):
    from jino_backup.account_batch import completed_database_copy
    from jino_backup.panel_export import PanelExportJob
    from test_panel_export import PanelFixture
    key=Fernet.generate_key()
    panel=PanelFixture()
    job=PanelExportJob(tmp_path/'state',tmp_path/'out','panel-example','key','2026-11-10',key,
                       lambda:panel,server_folder='client-login')
    result=job.run()
    scope=panel.inspect('2026-11-10')
    cached=completed_database_copy(tmp_path/'state',tmp_path/'out','panel-example','client-login',scope,key)
    assert cached['databases_count']==2 and cached['reused_existing']
    assert completed_database_copy(tmp_path/'state',tmp_path/'out','panel-example','client-login',
                                   {**scope,'databases':['new_database']},key) is None
    path=job.output/result['artifacts'][0]['relative_path']
    path.write_bytes(b'corrupted')
    try:
        completed_database_copy(tmp_path/'state',tmp_path/'out','panel-example','client-login',scope,key)
    except BackupError as exc:
        assert exc.code=='INTEGRITY_FAILED'
    else:
        raise AssertionError('Corrupted SQL must not be reused')


def test_account_plan_reports_ambiguous_accesses_without_values(tmp_path):
    from jino_backup.account_batch import account_plan
    source=tmp_path/'source.json'
    source.write_text(json.dumps([
        {'id':'1','login':'first-private-login','password':'synthetic-one'},
        {'id':'2','login':'second-private-login','password':'synthetic-two'}]))
    report,selected=account_plan(source,{'id':'id','login':'login','password':'password'})
    assert report['accounts_ready']==2 and len(selected)==2
    assert 'private-login' not in json.dumps(report)+repr(selected)


def test_mass_cli_dry_run_does_not_initialize_state_or_contact_servers(tmp_path):
    from test_cli import command
    source=tmp_path/'source.json'
    source.write_text('[{"id":"1","login":"private-login","password":"synthetic"}]')
    mapping=tmp_path/'mapping.json'
    mapping.write_text('{"id":"id","login":"login","password":"password"}')
    state=tmp_path/'state'
    out=tmp_path/'out'
    result=command('--state-dir',state,'backup','download-accounts','--file',source,
        '--mapping',mapping,'--destination',out,'--batch-id','batch')
    assert result.returncode==0 and json.loads(result.stdout)['accounts_ready']==1
    assert not state.exists() and not out.exists()
    assert 'private-login' not in result.stdout+result.stderr


def test_worker_reconnects_for_transient_failures_but_does_not_retry_bad_password(tmp_path):
    from jino_backup.account_batch import run_workers
    calls=[]
    def worker(row,update):
        calls.append(row)
        if row==1:
            raise BackupError('AUTH_FAILED')
        if calls.count(row)==1:
            raise BackupError('FILETRANSFER_AUTH_EXPIRED')
        return {'status':'FILES_ONLY','backup_complete':False}
    result=run_workers([1,2],worker,tmp_path/'report.json',workers=1)
    assert calls==[1,2,2]
    assert result['servers'][1]['status']=='FILES_ONLY'
