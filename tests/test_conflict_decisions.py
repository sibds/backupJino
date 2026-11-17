import pytest
from jino_backup.errors import BackupError
from test_dashboard_management import management


def test_conflict_decision_is_bound_to_task_and_recorded_without_stopping(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    key='a'*64
    management.progress(task['id'],{'stage':'FILE_CONFLICT','log':True,'error_code':'FILE_CONFLICT_PENDING',
        'conflict':{'id':key,'remote_path':'logo.txt','local_path':'Logo.txt','same_content':False,
                    'remote_size':3,'local_size':3,'reason':'CASE_COLLISION'}})
    events=management.events(1,stage='FILE_CONFLICT')
    assert events[0]['conflict']['local_path']=='Logo.txt'
    response=management.resolve_conflict(task['id'],{'conflict_id':key,'decision':'keep_both'})
    assert response['ok']
    assert management.task(task['id'])['state']=='QUEUED'
    assert management.task(task['id'])['conflict_decisions'][key]=='keep_both'
    with pytest.raises(BackupError):
        management.resolve_conflict(task['id'],{'conflict_id':'b'*64,'decision':'use_remote'})


def test_conflict_decision_rejects_non_file_tasks_and_unknown_actions(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    with pytest.raises(BackupError):
        management.resolve_conflict(task['id'],{'conflict_id':'a'*64,'decision':'keep_both'})


def test_session_rule_is_saved_only_for_validated_file_and_task(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    conflict={'id':'a'*64,'rule_id':'b'*64,'remote_path':'log.txt','local_path':'log.txt','reason':'LOCAL_CONTENT_DIFFERS'}
    management.progress(task['id'],{'stage':'FILE_CONFLICT','log':True,'error_code':'FILE_CONFLICT_PENDING','conflict':conflict})
    management.resolve_conflict(task['id'],{'conflict_id':'a'*64,'decision':'new_version','apply_to_session':True})
    assert management.task(task['id'])['conflict_decisions']['b'*64]=='new_version'
