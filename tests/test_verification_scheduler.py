import threading
import time
from test_dashboard_management import management,wait_status


def test_verification_leaves_five_download_slots(management):
    rows=[1]
    for i in range(2,7):
        rows.append(management.catalog.save(None,{'name':f'Synthetic {i}','login':f'synthetic-{i}',
            'panel_password':'synthetic','ftp_host':'ftp.example.test','ftp_login':f'ftp-{i}',
            'ftp_password':'synthetic','protocol':'ftps','ftp_port':21,'remote_root':'/'})['row'])
    entered={row:threading.Event() for row in rows};release=threading.Event()
    def runner(server,task,control,progress):
        if task['row']==1:progress(download_phase_complete=True,stage='FILE_VERIFY')
        entered[task['row']].set()
        while not release.wait(.01):control.checkpoint()
        return {'status':'FILES_ONLY'}
    management.runner=runner
    try:
        for row in rows:management.start(row,'files',{'transport':'ftps'})
        for row in rows:assert entered[row].wait(2),'Verification occupied a download slot'
    finally:release.set()


def test_second_verification_waits_without_stopping_downloads(management):
    second=management.catalog.save(None,{'name':'Second','login':'second','panel_password':'synthetic',
        'ftp_host':'ftp.example.test','ftp_login':'second','ftp_password':'synthetic','protocol':'ftps',
        'ftp_port':21,'remote_root':'/'})['row']
    entered=threading.Event();release=threading.Event();second_done=threading.Event()
    def runner(server,task,control,progress):
        progress(download_phase_complete=True,stage='FILE_VERIFY')
        if task['row']==1:
            entered.set()
            while not release.wait(.01):control.checkpoint()
        else:
            while not release.wait(.01):control.checkpoint()
            second_done.set()
        return {'status':'FILES_ONLY'}
    management.runner=runner
    try:
        first=management.start(1,'files',{'transport':'ftps'});assert entered.wait(2)
        other=management.start(second,'files',{'transport':'ftps'})
        deadline=time.monotonic()+2
        while time.monotonic()<deadline and management.task(other['id'])['state']!='QUEUED':time.sleep(.01)
        assert management.task(other['id'])['state']=='QUEUED'
        assert management.task(other['id'])['stage']=='VERIFICATION_WAIT'
        assert not management.task(other['id'])['error_code']
    finally:release.set()
    wait_status(management,first['id'],'FINISHED');wait_status(management,other['id'],'FINISHED')
    assert second_done.is_set()
