from jino_backup.runtime_recovery import RecoveryTracker
import io
import json
import pytest
from urllib.error import URLError


def snapshot(state='WAITING_OPERATOR',code='CANCELLED',**changes):
    task={'id':'same-task','kind':'files','state':state,'error_code':code,
          'operator_request':{'id':'nonce','code':code,'actions':['retry']},
          'progress':{'received_bytes':10},**changes}
    return {'servers':[{'row':1,'tasks':[task],'latest_tasks':{'files':task}}]}


def test_recovery_waits_and_uses_same_task_and_confirmation_once():
    tracker=RecoveryTracker({})
    assert tracker.due(snapshot(),100)==[]
    jobs=tracker.due(snapshot(),400)
    assert len(jobs)==1 and jobs[0][1]['id']=='same-task'
    tracker.accepted(jobs[0][1])
    assert tracker.due(snapshot(),10000)==[]


def test_backoff_survives_restart_and_progress_resets_it():
    saved={};tracker=RecoveryTracker(saved)
    tracker.due(snapshot(),100)
    task=tracker.due(snapshot(),400)[0][1];tracker.accepted(task)
    second=snapshot(operator_request={'id':'second','code':'CANCELLED','actions':['retry']})
    assert RecoveryTracker(saved).due(second,500)==[]
    assert tracker.due(second,1399)==[]
    job=tracker.due(second,1400)[0][1];tracker.accepted(job)
    third=snapshot(operator_request={'id':'third','code':'CANCELLED','actions':['retry']},progress={'received_bytes':11})
    assert tracker.due(third,1500)==[]
    assert len(tracker.due(third,1800))==1


def test_manual_actions_blocks_deferred_and_unrelated_errors_are_untouched():
    def untouched(data):
        tracker=RecoveryTracker({})
        assert tracker.due(data,100)==[]
        assert tracker.due(data,10000)==[]
    for state in ['STOPPED','STOPPING','PAUSED','PAUSING','RUNNING','QUEUED']:
        untouched(snapshot(state))
    for flag in ['blocked','deleted']:
        data=snapshot();data['servers'][0][flag]=True
        untouched(data)
    for change in [{'operator_deferred':{'blocking':True}}, {'first_copy_wait_for':'another'},
                   {'operator_request':{'id':'x','code':'CANCELLED','actions':[]}}]:
        untouched(snapshot(**change))
    untouched(snapshot(code='TLS_CERTIFICATE_INVALID'))
    data=snapshot();data['servers'][0]['latest_tasks']['files']={'id':'newer'}
    untouched(data)


def test_manual_pause_or_new_request_during_backoff_prevents_old_retry():
    tracker=RecoveryTracker({});tracker.due(snapshot(),100)
    assert tracker.due(snapshot('PAUSED'),500)==[]
    changed=snapshot(operator_request={'id':'changed','code':'CANCELLED','actions':['retry']})
    assert tracker.due(changed,500)==[]
    assert tracker.due(changed,799)==[]
    assert len(tracker.due(changed,800))==1


@pytest.mark.parametrize('fresh_state',['WAITING_OPERATOR','PAUSED'])
@pytest.mark.parametrize('lost_response',[False,True])
def test_live_bridge_rechecks_manual_state_and_reconciles_lost_post_response(tmp_path,monkeypatch,fresh_state,lost_response):
    from jino_backup import runtime_recovery as module
    data=snapshot()
    marker={'pid':123,'started_at':'synthetic','url':'http://127.0.0.1:8765/'}
    (tmp_path/'dashboard-server.json').write_text(json.dumps(marker))
    records={};tracker=RecoveryTracker(records);tracker.due(data,100)
    (tmp_path/'runtime-recovery.json').write_text(json.dumps(records))
    posts=[];cycles=[]
    class Opener:
        def open(self,request,timeout):
            if isinstance(request,str):
                if request.endswith('/api/state'):
                    payload=data
                elif '/api/servers/' in request:
                    server=json.loads(json.dumps(data['servers'][0]))
                    server['tasks'][0]['state']=fresh_state
                    payload={'server':server}
                else:payload={}
                return io.BytesIO(json.dumps(payload).encode())
            posts.append(json.loads(request.data))
            assert request.full_url.endswith('/api/tasks/same-task/decision')
            data['servers'][0]['tasks'][0].update(state='QUEUED',operator_request=None)
            if lost_response:raise URLError('synthetic lost response after accepted action')
            return io.BytesIO(b'{"ok":true}')
    class Finished(Exception):pass
    def sleep(seconds):
        if seconds < 1:return
        cycles.append(seconds)
        if len(cycles)==2:raise Finished()
    monkeypatch.setattr('sys.argv',['runtime_recovery','--state-dir',str(tmp_path)])
    monkeypatch.setattr(module,'build_opener',lambda *args:Opener())
    monkeypatch.setattr(module.time,'time',lambda:1000)
    monkeypatch.setattr(module.time,'sleep',sleep)
    with pytest.raises(Finished):module.main()
    assert len(posts)==(1 if fresh_state=='WAITING_OPERATOR' else 0)
    if posts:assert posts[0]=={'request_id':'nonce','action':'retry'}
