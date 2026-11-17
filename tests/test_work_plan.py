from datetime import datetime
import pytest
from test_dashboard_management import management
from jino_backup.errors import BackupError


def recurring(management, files=True, databases=False):
    management._schedule=lambda:None
    management.work_plans.submit({'rows':[1],'files':files,'databases':databases,'every_hours':3,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'first-copy-request-123'})


def test_rules_resolve_servers_from_one_catalog_snapshot(management, monkeypatch):
    recurring(management, databases=True)
    calls=[]
    original=management.catalog.all
    def counted(**kwargs):
        calls.append(1)
        return original(**kwargs)
    monkeypatch.setattr(management.catalog, 'all', counted)
    rules=management.work_plans.list()
    assert len(rules)==2
    assert all(not r['stale'] and not r['blocked'] for r in rules)
    assert len(calls)==1


def test_recurring_partial_copy_keeps_original_key(management):
    recurring(management)
    management.work_plans.tick(now=1893456001)
    task=management.tasks()[0]
    management._update(task['id'],state='FINISHED',result={'status':'PARTIAL'})
    management.work_plans.tick(now=1893466801)
    assert len(management.tasks())==1
    assert management.task(task['id'])['state']=='QUEUED'


def test_first_complete_database_copy_waits_for_first_files(management):
    recurring(management,databases=True)
    management.work_plans.tick(now=1893456001)
    database=next(t for t in management.tasks() if t['kind']=='databases')
    management._update(database['id'],state='FINISHED',result={'status':'DB_ONLY'})
    management.work_plans.tick(now=1893466801)
    assert len(management.tasks())==2
    files=next(t for t in management.tasks() if t['kind']=='files')
    management._update(files['id'],state='FINISHED',result={'status':'FILES_ONLY'})
    management.work_plans.tick(now=1893466802)
    assert len(management.tasks())==4


def test_recurring_manual_stop_does_not_create_another_version(management):
    recurring(management)
    management.work_plans.tick(now=1893456001)
    task=management.tasks()[0]
    management._update(task['id'],state='STOPPED')
    management.work_plans.tick(now=1893466801)
    assert len(management.tasks())==1
    assert management.task(task['id'])['state']=='STOPPED'


def test_first_file_copy_has_no_total_deadline_and_keeps_original_start(management):
    recurring(management)
    task=management.start(1,'files',{})
    management._update(task['id'],state='FAILED',error_code='TRANSFER_TIMEOUT',
        first_started_at='2020-01-01T00:00:00+00:00',progress={'received_bytes':12345})
    assert management._queue_file_resume(task['id'])
    current=management.task(task['id'])
    assert current['state']=='SCHEDULED'
    assert current['first_started_at']=='2020-01-01T00:00:00+00:00'
    assert current['progress']['received_bytes']==12345
    assert current['auto_resume_count']==1


def test_first_copy_selects_largest_previous_partial(management):
    recurring(management)
    old=management.start(1,'files',{})
    management._update(old['id'],state='FAILED',error_code='CONNECTION_FAILED',
        first_started_at='2020-01-01T00:00:00+00:00',progress={'received_bytes':70000})
    new=management.start(1,'files',{'fresh_run':True})
    management._update(new['id'],state='FAILED',error_code='CONNECTION_FAILED',
        first_started_at='2020-01-01T00:00:00+00:00',progress={'received_bytes':12})
    management.work_plans.tick(now=1893456001)
    assert len(management.tasks())==2
    assert management.task(old['id'])['state']=='SCHEDULED'
    assert management.task(new['id'])['state']=='FAILED'


def test_subset_database_success_does_not_release_file_versions(management):
    recurring(management,databases=True)
    management.work_plans.tick(now=1893456001)
    for task in management.tasks():
        management._update(task['id'],state='FINISHED',result={'status':'FILES_ONLY' if task['kind']=='files' else 'DB_ONLY'})
        if task['kind']=='databases':management._update(task['id'],db_aliases=['synthetic-alias'])
    management.work_plans.tick(now=1893466801)
    assert len([t for t in management.tasks() if t['kind']=='files'])==1


def test_held_new_version_cannot_hide_or_block_initial_partial(management):
    recurring(management)
    original=management.start(1,'files',{})
    management._update(original['id'],state='FAILED',error_code='TRANSFER_TIMEOUT',
        first_started_at='2020-01-01T00:00:00+00:00',progress={'received_bytes':70000})
    newer=management.start(1,'files',{'fresh_run':True})
    management._update(newer['id'],first_copy_wait_for=original['id'],stage='FIRST_COPY_WAIT')
    assert management.tasks()[0]['id']==original['id']
    management.work_plans.tick(now=1893456001)
    assert management.task(original['id'])['state']=='SCHEDULED'
    assert len(management.tasks())==2


def test_reconcile_parks_queued_version_and_continues_advanced_baseline(management):
    recurring(management)
    original=management.start(1,'files',{})
    management._update(original['id'],state='FAILED',error_code='TRANSFER_TIMEOUT',
        first_started_at='2020-01-01T00:00:00+00:00',progress={'received_bytes':70000})
    newer=management.start(1,'files',{'fresh_run':True})
    management.work_plans.reconcile_first_copies()
    assert management.task(newer['id'])['first_copy_wait_for']==original['id']
    assert management.task(original['id'])['state']=='SCHEDULED'
    assert management.tasks()[0]['id']==original['id']
    management.work_plans.reconcile_first_copies()
    assert len(management.tasks())==2


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
