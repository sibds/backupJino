import json
from pathlib import Path
import pytest
from cryptography.fernet import Fernet
from test_panel_export import PanelFixture,SQL
from test_dashboard_management import management
from jino_backup.panel_export import PanelExportJob


def workflow(tmp_path,panel,key):
    return PanelExportJob(tmp_path/'state',tmp_path/'copies','panel-example','same-request',
                          '2026-11-10',key,lambda:panel)


def test_corrupt_partial_is_retained_and_same_export_downloaded_again(tmp_path):
    panel=PanelFixture(); key=Fernet.generate_key()
    original=panel.download
    panel.download=lambda scope,remote,path,limit:path.write_bytes(b'<html>bad</html>')
    first=workflow(tmp_path,panel,key).run()
    bad=next((tmp_path/'copies').rglob('*.partial-*'))
    assert first['error_code']=='DB_INTEGRITY_FAILED'
    panel.download=original
    second=workflow(tmp_path,panel,key).run()
    assert second['status']=='DB_ONLY' and second['job_id']==first['job_id']
    assert bad.read_bytes()==b'<html>bad</html>'
    assert len(panel.exports)==2 and not panel.deleted


def test_corrupt_committed_copy_survives_failed_replacement_then_is_replaced(tmp_path):
    panel=PanelFixture();key=Fernet.generate_key()
    instance=workflow(tmp_path,panel,key)
    assert instance.run()['status']=='DB_ONLY'
    target=next(instance.output.glob('databases/*.sql'))
    target.write_bytes(b'<html>corrupt</html>')
    original=panel.download
    def bad(scope,remote,path,limit):
        assert target.read_bytes()==b'<html>corrupt</html>'
        path.write_bytes(b'bad again')
    panel.download=bad
    result=workflow(tmp_path,panel,key).run()
    assert result['error_code']=='DB_INTEGRITY_FAILED'
    assert target.read_bytes()==b'<html>corrupt</html>'
    panel.download=original
    assert workflow(tmp_path,panel,key).run()['status']=='DB_ONLY'
    assert target.read_bytes()==SQL
    assert len(panel.exports)==2 and not panel.deleted


def test_database_integrity_failure_schedules_bounded_same_task_redownload(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    for attempt in range(1,4):
        management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED',result={'status':'FAILED','error_code':'DB_INTEGRITY_FAILED'})
        assert management._queue_database_redownload(task['id'])
        current=management.task(task['id'])
        assert current['state']=='SCHEDULED' and current['db_redownload_count']==attempt
        assert len(management.tasks())==1
    management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED')
    assert not management._queue_database_redownload(task['id'])
    assert management.task(task['id'])['db_redownload_exhausted']


def test_corrupt_cached_copy_identifies_owned_journal_for_redownload(tmp_path):
    from jino_backup.account_batch import completed_database_copy
    panel=PanelFixture(); key=Fernet.generate_key(); instance=workflow(tmp_path,panel,key)
    instance.run()
    next(instance.output.glob('databases/*.sql')).write_bytes(b'bad')
    with pytest.raises(Exception) as caught:
        completed_database_copy(tmp_path/'state',tmp_path/'copies','panel-example','server-example',panel.inspect('2026-11-10'),key)
    assert caught.value.code=='DB_INTEGRITY_FAILED'
    assert caught.value.details['repair_job_id']==instance.job_id
    repaired=PanelExportJob(tmp_path/'state',tmp_path/'copies','panel-example','another-ui-request',
        '2026-11-10',key,lambda:panel,resume_job_id=instance.job_id)
    assert repaired.run()['status']=='DB_ONLY'
    assert repaired.job_id==instance.job_id and len(panel.exports)==2


def test_retry_refreshes_size_of_the_owned_remote_sql(tmp_path):
    panel=PanelFixture();key=Fernet.generate_key()
    submit=panel.submit_export
    def initial(scope,name):
        submit(scope,name)
        if len(panel.exports)==1: panel.files[name]['size_bytes']=3
    panel.submit_export=initial
    panel.download=lambda scope,remote,path,limit:path.write_bytes(b'bad')
    assert workflow(tmp_path,panel,key).run()['error_code']=='DB_INTEGRITY_FAILED'
    for name,value in list(panel.files.items()): panel.files[name]={**value,'size_bytes':len(SQL)}
    def good(scope,remote,path,limit):
        assert remote['size_bytes']==len(SQL)
        path.write_bytes(SQL)
    panel.download=good
    assert workflow(tmp_path,panel,key).run()['status']=='DB_ONLY'
    assert len(panel.exports)==2


def test_retry_queue_is_idempotent_and_operator_pause_stop_win(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED')
    assert management._queue_database_redownload(task['id'])
    assert not management._queue_database_redownload(task['id'])
    management.action(task['id'],'pause')
    assert management.task(task['id'])['state']=='PAUSED'
    assert not management._queue_database_redownload(task['id'])
    management.action(task['id'],'stop')
    assert not management._queue_database_redownload(task['id'])


def test_cannot_replace_operator_changes_made_while_new_dump_downloads(tmp_path):
    panel=PanelFixture();key=Fernet.generate_key();instance=workflow(tmp_path,panel,key)
    instance.run();target=next(instance.output.glob('databases/*.sql'));target.write_bytes(b'broken')
    def edited(scope,remote,path,limit):
        target.write_bytes(b'operator changed this file');path.write_bytes(SQL)
    panel.download=edited
    result=workflow(tmp_path,panel,key).run()
    assert result['error_code']=='LOCAL_DESTINATION_CONFLICT'
    assert target.read_bytes()==b'operator changed this file'


def test_failed_final_verification_is_not_reported_as_pass(tmp_path,monkeypatch):
    from jino_backup.errors import BackupError
    panel=PanelFixture();instance=workflow(tmp_path,panel,Fernet.generate_key())
    monkeypatch.setattr(instance,'_verify_local',lambda artifact:(_ for _ in ()).throw(BackupError('DB_INTEGRITY_FAILED')))
    result=instance.run()
    assert result['error_code']=='DB_INTEGRITY_FAILED'
    assert result['artifacts']==[] and result['databases_count']==0


def test_runner_automatically_queues_integrity_failure(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management.runner=lambda *args:{'status':'FAILED','error_code':'DB_INTEGRITY_FAILED'}
    management._run(task['id'])
    assert management.task(task['id'])['state']=='SCHEDULED'
    assert management.task(task['id'])['db_redownload_count']==1


def test_scheduled_redownload_survives_restart_without_an_extra_attempt(management):
    from jino_backup.dashboard_management import DashboardManagement
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED')
    management._queue_database_redownload(task['id'])
    due=management.task(task['id'])['scheduled_at']
    management.close()
    restarted=DashboardManagement(management.data,runner=lambda *args:pytest.fail('Not yet due'))
    try:
        assert restarted.task(task['id'])['state']=='SCHEDULED'
        assert restarted.task(task['id'])['db_redownload_count']==1
        assert restarted.task(task['id'])['scheduled_at']==due
    finally: restarted.close()


def test_recurring_rule_waits_for_integrity_retry_budget(management):
    management._schedule=lambda:None
    management.work_plans.submit({'rows':[1],'files':False,'databases':True,'every_hours':3,
        'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-db-retry-rule'})
    management.work_plans.tick(now=1893456001)
    task=management.tasks()[0]
    management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED',db_redownload_count=1)
    management.work_plans.tick(now=1893456001+10800)
    assert management.work_plans.list()[0]['enabled']
    assert len(management.tasks())==1
    management._update(task['id'],db_redownload_exhausted=True)
    management.work_plans.tick(now=1893456001+10800)
    assert not management.work_plans.list()[0]['enabled']


def test_schedule_cannot_take_over_while_database_failure_is_finalizing(management):
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    management._update(task['id'],state='FAILED',error_code='DB_INTEGRITY_FAILED')
    management.running.add(task['id'])
    try:
        management.work_plans.submit({'rows':[1],'files':False,'databases':True,'every_hours':3,
            'scheduled_at':'2030-01-01T00:00:00+00:00','request_id':'synthetic-db-retry-rule'})
        management.work_plans.tick(now=1893456001)
        assert len(management.tasks())==1
    finally: management.running.discard(task['id'])
