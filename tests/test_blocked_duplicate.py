import pytest
from test_dashboard_management import management
from jino_backup.errors import BackupError


def test_blocked_panel_duplicate_does_not_prevent_active_database_job(management):
    catalog=management.catalog
    original=catalog.get(1)
    duplicate=catalog.save(None,{'name':'Blocked synthetic duplicate','login':original['panel_login'],
        'panel_password':'different-synthetic','ftp_host':'other.example.test','ftp_login':'other',
        'ftp_password':'other-synthetic','protocol':'ftps','ftp_port':21,'remote_root':'/'})
    assert not catalog.get(1)['can_check']
    catalog.set_flag(duplicate['row'],'blocked',True)
    assert catalog.get(1)['can_check']
    assert not catalog.get(duplicate['row'])['can_check']
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    assert task['row']==1 and task['db_aliases'] is None
    assert catalog.get(1)['binding_revision']==original['binding_revision']
    catalog.set_flag(duplicate['row'],'blocked',False)
    assert not catalog.get(1)['can_check']
    with pytest.raises(BackupError,match='ACCOUNT_AMBIGUOUS'):management.start(1,'databases',{})


def test_blocked_ftp_duplicate_does_not_disable_active_file_profile(management):
    catalog=management.catalog;original=catalog.get(1)
    duplicate=catalog.save(None,{'name':'Blocked synthetic FTP duplicate','login':'other-panel',
        'panel_password':'synthetic','ftp_host':original['ftp_host'],'ftp_login':original['ftp_login'],
        'ftp_password':'different-synthetic','protocol':'ftps','ftp_port':21,'remote_root':'/'})
    catalog.set_flag(duplicate['row'],'blocked',True)
    assert catalog.get(1)['ftp_ready']
    catalog.set_flag(duplicate['row'],'blocked',False)
    assert not catalog.get(1)['ftp_ready']


def test_staged_registration_is_paused_idempotent_and_adopted_by_existing_executor(management):
    from jino_backup.staged_tasks import stage_all_databases
    management._schedule=lambda:None
    original=management.catalog.get(1)
    duplicate=management.catalog.save(None,{'name':'Synthetic duplicate','login':original['panel_login'],
        'panel_password':'different','ftp_host':'other.example.test','ftp_login':'other',
        'ftp_password':'synthetic','protocol':'ftps','ftp_port':21,'remote_root':'/'})
    with pytest.raises(BackupError,match='ACCOUNT_AMBIGUOUS'):stage_all_databases(management.data,1)
    assert not management.tasks()
    management.catalog.set_flag(duplicate['row'],'blocked',True)
    task=stage_all_databases(management.data,1)
    assert task['state']=='PAUSED' and task['db_aliases'] is None
    assert task['id'] not in management.controls
    assert stage_all_databases(management.data,1)['id']==task['id']
    management.action(task['id'],'resume')
    assert management.task(task['id'])['state']=='QUEUED'
    assert task['id'] in management.controls
    assert len(management.tasks())==1
