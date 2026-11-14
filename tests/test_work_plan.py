from datetime import datetime
import pytest
from test_dashboard_management import management
from jino_backup.errors import BackupError


def test_bulk_plan_queues_files_and_databases_once(management):
    management._schedule=lambda:None
    payload={'rows':[1],'files':True,'databases':True,'every_hours':None,'request_id':'synthetic-request-123'}
    result=management.work_plans.submit(payload)
    assert result['tasks_count']==2 and len(management.tasks())==2
    assert management.work_plans.submit(payload)==result
    assert len(management.tasks())==2


def test_all_selected_servers_validate_before_mutation(management):
    management._schedule=lambda:None
    with pytest.raises(BackupError):
        management.work_plans.submit({'rows':[1,999],'files':True,'databases':True,
            'every_hours':72,'request_id':'synthetic-request-123'})
    assert not management.tasks() and not management.work_plans.list()


@pytest.mark.parametrize('hours',[3,6,12,24,72,120,168,336,720,2160,4320,6480,8760])
def test_all_requested_intervals_persist_and_tick_is_idempotent(management,hours):
    management._schedule=lambda:None
    management.work_plans.submit({'rows':[1],'files':True,'databases':True,'every_hours':hours,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'})
    assert len(management.work_plans.list())==2 and not management.tasks()
    management.work_plans.tick(now=1893456001)
    assert len(management.tasks())==2
    management.work_plans.tick(now=1893456001)
    assert len(management.tasks())==2
    assert all(datetime.fromisoformat(r['next_at']).timestamp()==1893456000+hours*3600
               for r in management.work_plans.list())
    assert {r['every_hours'] for r in management.work_plans.list()}=={hours}


def test_schedule_does_not_duplicate_active_job_or_revive_manual_pause(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management._update(task['id'],state='PAUSED')
    management.work_plans.submit({'rows':[1],'files':True,'databases':False,'every_hours':3,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'})
    management.work_plans.tick(now=1893456001)
    assert len(management.tasks())==1 and management.task(task['id'])['state']=='PAUSED'


def test_missed_intervals_coalesce_and_crash_between_enqueue_and_advance_recovers(management,monkeypatch):
    management._schedule=lambda:None
    management.work_plans.submit({'rows':[1],'files':True,'databases':False,'every_hours':3,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'})
    save=management.work_plans._save
    monkeypatch.setattr(management.work_plans,'_save',lambda *args:(_ for _ in ()).throw(RuntimeError('synthetic crash')))
    with pytest.raises(RuntimeError): management.work_plans.tick(now=1893456000+86400)
    assert len(management.tasks())==1
    monkeypatch.setattr(management.work_plans,'_save',save)
    management.work_plans.tick(now=1893456000+86400)
    assert len(management.tasks())==1
    assert datetime.fromisoformat(management.work_plans.list()[0]['next_at']).timestamp()==1893456000+86400+10800


def test_recurring_tls_failure_requires_operator_before_another_attempt(management):
    management._schedule=lambda:None
    management.work_plans.submit({'rows':[1],'files':True,'databases':False,'every_hours':3,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'})
    management.work_plans.tick(now=1893456001)
    task=management.tasks()[0]
    management._update(task['id'],state='FAILED',error_code='TLS_CERTIFICATE_INVALID')
    management.work_plans.tick(now=1893456001+10800)
    assert len(management.tasks())==1 and management.work_plans.list()[0]['enabled'] is False


def test_rule_toggle_targets_only_that_rule_after_binding_change(management):
    management._schedule=lambda:None
    payload={'rows':[1],'files':True,'databases':False,'every_hours':24,
             'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'}
    management.work_plans.submit(payload)
    old=management.work_plans.list()[0]
    management.save_server(1,{'ftp_host':'changed.example.test'})
    management.work_plans.submit({**payload,'request_id':'synthetic-request-456'})
    current=next(r for r in management.work_plans.list() if not r['stale'])
    management.work_plans.action({'rows':[1],'files':True,'databases':False,
                                 'action':'disable_schedule','rule_id':old['id']})
    assert next(r for r in management.work_plans.list() if r['id']==current['id'])['enabled']
    management.work_plans.action({'rows':[1],'files':True,'databases':False,
                                 'action':'enable_schedule','rule_id':current['id']})
    assert next(r for r in management.work_plans.list() if r['id']==old['id'])['enabled'] is False


def test_bulk_resume_continues_partial_files_with_same_id(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management._update(task['id'],state='FINISHED',result={'status':'PARTIAL'})
    management.work_plans.action({'rows':[1],'files':True,'databases':False,'action':'resume'})
    assert management.task(task['id'])['state']=='QUEUED' and len(management.tasks())==1


def test_bulk_retry_after_resumed_job_finished_does_not_create_new_id(management,monkeypatch):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management._update(task['id'],state='FAILED',error_code='CONNECTION_FAILED')
    payload={'rows':[1],'files':True,'databases':False,'every_hours':None,'request_id':'synthetic-request-123'}
    original=management.action
    def lose_response(task_id,action):
        result=original(task_id,action)
        management._update(task_id,state='FINISHED',result={'status':'FILES_ONLY'})
        raise RuntimeError('synthetic response loss')
    monkeypatch.setattr(management,'action',lose_response)
    with pytest.raises(RuntimeError): management.work_plans.submit(payload)
    monkeypatch.setattr(management,'action',original)
    management.work_plans.submit(payload)
    assert len(management.tasks())==1 and management.tasks()[0]['id']==task['id']


def test_future_bulk_continuation_keeps_parts_but_starts_deadline_when_due(management):
    management._schedule=lambda:None
    task=management.start(1,'files',{})
    management._update(task['id'],state='FAILED',error_code='CONNECTION_FAILED',first_started_at='2020-01-01T00:00:00+00:00')
    management.work_plans.submit({'rows':[1],'files':True,'databases':False,'every_hours':None,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-request-123'})
    current=management.task(task['id'])
    assert current['scheduled_at']=='2030-01-01T00:00:00+00:00'
    assert current['first_started_at'] is None
