import pytest
from test_dashboard_management import management,wait_status
from jino_backup.errors import BackupError


@pytest.mark.parametrize('code',['REMOTE_DESTINATION_EXISTS','TLS_CERTIFICATE_INVALID','INTERNAL_ERROR'])
def test_unhandled_job_problem_requests_operator_instead_of_silent_failure(management,code):
    management.runner=lambda *args:{'status':'FAILED','error_code':code}
    task=management.start(1,'databases',{})
    wait_status(management,task['id'],'WAITING_OPERATOR')
    current=management.task(task['id'])
    assert current['operator_request']['code']==code
    assert current['operator_request']['id']
    assert task['id'] not in management.running


def test_operator_retry_reuses_task_and_rejects_stale_confirmation(management):
    management.runner=lambda *args:{'status':'FAILED','error_code':'REMOTE_DESTINATION_EXISTS'}
    task=management.start(1,'databases',{})
    wait_status(management,task['id'],'WAITING_OPERATOR')
    request=management.task(task['id'])['operator_request']
    management.runner=lambda *args:{'status':'DB_ONLY'}
    management.resolve_request(task['id'],{'request_id':request['id'],'action':'retry'})
    wait_status(management,task['id'],'FINISHED')
    assert len(management.tasks())==1 and not management.task(task['id']).get('operator_request')
    with pytest.raises(BackupError):
        management.resolve_request(task['id'],{'request_id':request['id'],'action':'retry'})


def test_waiting_for_decision_does_not_consume_worker_or_repeat_automatically(management):
    calls=[]
    def runner(server,task,*args):
        calls.append(task['kind'])
        return {'status':'FAILED','error_code':'REMOTE_DESTINATION_EXISTS'} if task['kind']=='databases' else {'status':'FILES_ONLY'}
    management.runner=runner
    db=management.start(1,'databases',{});wait_status(management,db['id'],'WAITING_OPERATOR')
    files=management.start(1,'files',{});wait_status(management,files['id'],'FINISHED')
    management._wake()
    assert calls.count('databases')==1


def test_private_export_names_are_absent_from_general_snapshot(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='WAITING_OPERATOR',operator_request={'id':'request','code':'REMOTE_DESTINATION_EXISTS','actions':['apply_exports']})
    management.inventory.save_export_candidates(1,1,task['id'],[{'id':'a'*64,'name':'private-db.sql','path':'private-location','supported':True}])
    management.data.management=management
    import json
    assert 'private-db.sql' not in json.dumps(management.data.snapshot())
    detail=management.data.server_detail(1)
    assert detail['server']['export_candidates'][task['id']][0]['name']=='private-db.sql'
    assert 'private-location' not in json.dumps(detail)
    assert 'private-db.sql' not in json.dumps(management.data.snapshot())


def test_file_variant_question_is_visible_while_other_files_are_still_running(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management._update(task['id'],state='RUNNING')
    management.progress(task['id'],{'stage':'FILE_CONFLICT','error_code':'FILE_CONFLICT_PENDING',
        'conflict':{'id':'a'*64,'remote_path':'one.txt','local_path':'one.txt'},'log':True})
    current=management.task(task['id'])
    assert current['state']=='RUNNING'
    assert current['operator_request']['blocking'] is False


def test_imported_sql_does_not_show_provider_snapshot_date_as_its_source_date(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],snapshot_date='2026-11-16',source_snapshot_verified=False)
    management.data.management=management
    assert management.data.snapshot()['servers'][0]['databases']['snapshot_date'] is None


def test_request_survives_restart_without_a_new_confirmation_id(management):
    from jino_backup.dashboard_management import DashboardManagement
    management.runner=lambda *args:{'status':'NEEDS_ACTION','error_code':'REMOTE_DESTINATION_EXISTS','candidates_count':1}
    task=management.start(1,'databases',{});wait_status(management,task['id'],'WAITING_OPERATOR')
    request_id=management.task(task['id'])['operator_request']['id']
    management.close()
    restarted=DashboardManagement(management.data,runner=lambda *args:pytest.fail('Repeated while waiting for user'),access_policy=management.access_policy)
    try:
        assert restarted.task(task['id'])['operator_request']['id']==request_id
    finally:restarted.close()


def test_existing_sql_assignment_requires_known_alias_in_current_scope(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management.inventory.discover(1,1,management.catalog.get(1)['account_ref'],['private_db'])
    alias=management.inventory.list(1,1)[0]['db_alias']
    candidate={'id':'c'*64,'name':'unmapped.sql','path':'private-token','supported':False,'db_alias':None,'size_bytes':100,'comparison':'untracked'}
    management.inventory.save_export_candidates(1,1,task['id'],[candidate])
    management._update(task['id'],state='FAILED',result={'status':'NEEDS_ACTION','error_code':'REMOTE_DESTINATION_EXISTS','candidates_count':1})
    management._request_operator(task['id'])
    request=management.task(task['id'])['operator_request']
    payload={'request_id':request['id'],'action':'apply_exports','selections':[{'candidate_id':candidate['id'],'decision':'new_version','db_alias':'unknown-alias'}]}
    with pytest.raises(BackupError):management.resolve_request(task['id'],payload)
    assert management.inventory.export_candidates(1,1,task['id'])[0]['db_alias'] is None
    payload['selections'][0]['db_alias']=alias
    management.resolve_request(task['id'],payload)
    assert management.task(task['id'])['export_decisions']=={candidate['id']:'new_version'}
    assert management.inventory.export_candidates(1,1,task['id'])[0]['db_alias']==alias


def test_request_remains_visible_during_read_only_export_inspection_queue(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='REMOTE_DESTINATION_EXISTS')
    management._request_operator(task['id'])
    request=management.task(task['id'])['operator_request']
    management.resolve_request(task['id'],{'request_id':request['id'],'action':'inspect_exports'})
    current=management.task(task['id'])
    assert current['state']=='QUEUED'
    assert current['operator_request']['inspection_pending'] is True
    assert current['operator_request']['blocking'] is False


def test_manual_pause_of_a_request_survives_restart(management):
    from jino_backup.dashboard_management import DashboardManagement
    management.runner=lambda *args:{'status':'NEEDS_ACTION','error_code':'REMOTE_DESTINATION_EXISTS'}
    task=management.start(1,'databases',{});wait_status(management,task['id'],'WAITING_OPERATOR')
    management.action(task['id'],'pause');management.close()
    restarted=DashboardManagement(management.data,runner=lambda *args:pytest.fail('Manual pause ignored'),access_policy=management.access_policy)
    try:assert restarted.task(task['id'])['state']=='PAUSED'
    finally:restarted.close()


def test_skip_defers_question_without_deleting_parts_or_restarting_it(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='REMOTE_DESTINATION_EXISTS',progress={'received_bytes':123})
    management._request_operator(task['id']);request=management.task(task['id'])['operator_request']
    management.resolve_request(task['id'],{'request_id':request['id'],'action':'skip'})
    deferred=management.task(task['id'])
    with pytest.raises(BackupError):management.resolve_request(task['id'],{'request_id':request['id'],'action':'skip'})
    assert not deferred.get('operator_request') and deferred['operator_deferred']['request']['id']==request['id']
    assert deferred['progress']['received_bytes']==123 and len(management.tasks())==1
    management._request_operator(task['id'])
    assert not management.task(task['id']).get('operator_request')
    management.resolve_request(task['id'],{'request_id':request['id'],'action':'reopen'})
    assert management.task(task['id'])['operator_request']['id']!=request['id']


def test_blocked_server_does_not_get_new_operator_requests(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='AUTH_FAILED')
    management.catalog.set_flag(1,'blocked',True)
    assert not management._request_operator(task['id'])


@pytest.mark.parametrize('new_failure',[False,True])
def test_skip_during_download_keeps_other_work_and_does_not_hide_new_failure(management,new_failure):
    import threading
    entered=threading.Event();release=threading.Event()
    def runner(server,task,control,progress):
        progress(stage='FILE_CONFLICT',error_code='FILE_CONFLICT_PENDING',conflict={'id':'d'*64},log=True)
        entered.set()
        while not release.wait(.01):control.checkpoint()
        if new_failure:raise BackupError('TLS_CERTIFICATE_INVALID')
        return {'status':'PARTIAL','file_conflicts_count':1}
    management.runner=runner
    try:
        task=management.start(1,'files',{});assert entered.wait(2)
        request=management.task(task['id'])['operator_request']
        management.resolve_request(task['id'],{'request_id':request['id'],'action':'skip'})
        assert management.task(task['id'])['state']=='RUNNING'
        release.set();wait_status(management,task['id'],'WAITING_OPERATOR')
        current=management.task(task['id'])
        if new_failure:assert current['operator_request']['code']=='TLS_CERTIFICATE_INVALID'
        else:assert not current.get('operator_request') and current['operator_deferred']['blocking']
    finally:release.set()
