import time
import pytest
from test_dashboard_management import management, wait_status
from jino_backup.errors import BackupError


@pytest.mark.parametrize('returned', [False, True])
def test_failed_file_transfer_returns_to_plan_with_same_key(management, returned, monkeypatch):
    def runner(server, task, control, progress):
        progress(received_bytes=123, stage='RETRY_EXHAUSTED')
        if returned: return {'status':'FAILED','error_code':'TRANSFER_INTERRUPTED'}
        raise BackupError('TRANSFER_INTERRUPTED')
    management.runner=runner
    task=management.start(1,'files',{'transport':'ftps'})
    wait_status(management,task['id'],'SCHEDULED')
    queued=management.task(task['id'])
    assert queued['stage']=='AUTO_RESUME_WAIT'
    assert queued['progress']['received_bytes']==123
    assert queued['auto_resume_count']==1 and queued['auto_resume_error']=='TRANSFER_INTERRUPTED'
    assert queued['scheduled_at'] and len(management.tasks())==1
    management.runner=lambda *a:{'status':'FILES_ONLY'}
    due=management._scheduled_timestamp(queued['scheduled_at'])
    monkeypatch.setattr('jino_backup.dashboard_management.time.time',lambda:due+1)
    management._wake()  # The same callback as the real queue timer, not an operator resume.
    wait_status(management,task['id'],'FINISHED')
    assert len(management.tasks())==1


@pytest.mark.parametrize('code',['TLS_CERTIFICATE_INVALID','AUTH_FAILED','REMOTE_PATH_UNSAFE','REMOTE_TYPE_UNSAFE','LOCAL_FILE_CHANGED','CAPTCHA_REQUIRED'])
def test_permanent_failures_do_not_loop(management,code):
    def runner(*args): raise BackupError(code)
    management.runner=runner
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'FAILED')
    assert not management.task(task['id']).get('auto_resume_at')


def test_stop_cancels_a_pending_automatic_resume(management):
    def runner(*args): raise BackupError('CONNECTION_FAILED')
    management.runner=runner
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'SCHEDULED')
    management.action(task['id'],'stop')
    assert management.task(task['id'])['state']=='STOPPED'
    assert not management.task(task['id']).get('auto_resume_at')


def test_auto_resume_backoff_is_bounded_and_deadline_is_not_reset(management):
    from datetime import datetime, timezone, timedelta
    future='2099-01-01T00:00:00Z'
    task=management.start(1,'files',{'scheduled_at':future})
    started=(datetime.now(timezone.utc)-timedelta(hours=25)).isoformat()
    management._update(task['id'],state='FAILED',error_code='TRANSFER_INTERRUPTED',first_started_at=started,started_at=started)
    assert not management._queue_file_resume(task['id'])
    management._update(task['id'],first_started_at=datetime.now(timezone.utc).isoformat(),auto_resume_count=99)
    assert management._queue_file_resume(task['id'])
    queued=management.task(task['id'])
    remaining=management._scheduled_timestamp(queued['scheduled_at'])-time.time()
    assert 3500<remaining<=3600


def test_file_restart_queues_resume_and_keeps_its_due_time_on_second_restart(management):
    from jino_backup.dashboard_management import DashboardManagement
    management.runner=lambda *args:{'status':'FILES_ONLY'}
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'FINISHED')
    management._update(task['id'],state='RUNNING')
    management.close()
    restarted=DashboardManagement(management.data,runner=lambda *args:pytest.fail('Resumed before due time'),access_policy=management.access_policy)
    try:
        queued=restarted.task(task['id'])
        assert queued['state']=='SCHEDULED' and queued['auto_resume_error']=='WORKER_RESTARTED'
        due=queued['scheduled_at']
    finally: restarted.close()
    again=DashboardManagement(management.data,runner=lambda *args:pytest.fail('Resumed before due time'),access_policy=management.access_policy)
    try:
        assert again.task(task['id'])['scheduled_at']==due
        assert again.task(task['id'])['auto_resume_count']==1
        assert len(again.tasks())==1
    finally: again.close()


@pytest.mark.parametrize('change',['blocked','binding'])
def test_auto_resume_does_not_cross_operator_block_or_changed_binding(management,change):
    from datetime import datetime, timezone
    task=management.start(1,'files',{'scheduled_at':'2099-01-01T00:00:00Z'})
    management._update(task['id'],state='FAILED',error_code='CONNECTION_FAILED',started_at=datetime.now(timezone.utc).isoformat())
    if change=='blocked': management.catalog.set_flag(1,'blocked',True)
    else: management.catalog.save(1,{'ftp_host':'changed.example.test'})
    assert not management._queue_file_resume(task['id'])
    assert management.task(task['id'])['state']=='FAILED'


def test_confirmed_byte_growth_resets_the_whole_job_backoff(management):
    from datetime import datetime, timezone
    task=management.start(1,'files',{'scheduled_at':'2099-01-01T00:00:00Z'})
    for size, expected in ((100,1),(100,2),(150,1)):
        management._update(task['id'],state='FAILED',error_code='TRANSFER_INTERRUPTED',
            started_at=datetime.now(timezone.utc).isoformat(),progress={'received_bytes':size})
        assert management._queue_file_resume(task['id'])
        assert management.task(task['id'])['auto_resume_count']==expected


def test_pause_cancels_a_pending_automatic_resume(management):
    def runner(*args): raise BackupError('TRANSFER_INTERRUPTED')
    management.runner=runner
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'SCHEDULED')
    management.action(task['id'],'pause')
    assert management.task(task['id'])['state']=='PAUSED'
    assert not management.task(task['id']).get('auto_resume_at')
    management._wake()
    assert management.task(task['id'])['state']=='PAUSED'


def test_changed_binding_is_checked_again_before_automatic_admission(management):
    def runner(*args): raise BackupError('TRANSFER_INTERRUPTED')
    management.runner=runner
    task=management.start(1,'files',{})
    wait_status(management,task['id'],'SCHEDULED')
    management.catalog.save(1,{'ftp_host':'changed.example.test'})
    management._wake()
    assert management.task(task['id'])['state']=='FAILED'
    assert management.task(task['id'])['error_code']=='SERVER_CONFIG_CHANGED'
