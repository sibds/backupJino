"""API-only recovery bridge for an already running, older dashboard worker."""

import argparse
import json
import os
import time
from http.cookiejar import CookieJar
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .downloads import FileLock
from .state import write_json as write_state

RETRY_CODES = {'CANCELLED', 'TRANSFER_TIMEOUT', 'OPERATION_TIMEOUT',
               'LOW_SPEED_TIMEOUT', 'EXPORT_TIMEOUT', 'DB_EXPORT_TIMEOUT'}
DELAYS = (300, 900, 1800, 3600)


class RecoveryTracker:
    def __init__(self, records):
        self.records = records

    def due(self, snapshot, now):
        jobs = []
        for server in snapshot.get('servers', []):
            if server.get('blocked') or server.get('deleted') or server.get('status') == 'BLOCKED':
                continue
            for task in server.get('tasks', []):
                request = task.get('operator_request') or {}
                latest = server.get('latest_tasks', {}).get(task['kind'])
                if (task['state'] != 'WAITING_OPERATOR' or task.get('operator_deferred')
                        or task.get('first_copy_wait_for') or task.get('error_code') not in RETRY_CODES
                        or request.get('code') != task.get('error_code')
                        or 'retry' not in request.get('actions', []) or not request.get('id')
                        or (latest and latest['id'] != task['id']
                            and latest.get('first_copy_wait_for') != task['id'])):
                    continue
                record = self.records.get(task['id'], {})
                if record.get('accepted') == request['id']:
                    continue
                if record.get('request_id') != request['id']:
                    saved = task.get('progress', {}).get('received_bytes', 0)
                    attempt = 0 if saved > record.get('bytes', 0) else record.get('attempt', 0)
                    record = {**record, 'request_id': request['id'], 'bytes': saved,
                              'attempt': attempt, 'due': now + DELAYS[min(attempt, len(DELAYS)-1)]}
                    self.records[task['id']] = record
                if now >= record['due']:
                    jobs.append((server['row'], task))
        return jobs

    def accepted(self, task):
        record = self.records[task['id']]
        record.update(accepted=task['operator_request']['id'], attempt=record['attempt'] + 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    args = parser.parse_args()
    marker = args.state_dir / 'dashboard-server.json'
    deployment = json.loads(marker.read_text(encoding='utf-8'))
    base = deployment['url'].rstrip('/')
    if base not in {'http://127.0.0.1:8765', 'http://localhost:8765'}:
        raise ValueError('Only the local dashboard is supported')
    ledger = args.state_dir / 'runtime-recovery.json'
    status = args.state_dir / 'runtime-recovery-status.json'
    records = json.loads(ledger.read_text(encoding='utf-8')) if ledger.exists() else {}
    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    tracker = RecoveryTracker(records)
    recovered = 0
    with FileLock(args.state_dir / 'runtime-recovery.lock'):
        while True:
            current = json.loads(marker.read_text(encoding='utf-8'))
            if (current['pid'], current['started_at']) != (deployment['pid'], deployment['started_at']):
                write_state(status, {'state':'FINISHED', 'reason':'WORKER_REPLACED', 'recovered':recovered})
                return
            try:
                opener.open(base+'/', timeout=20).close()
                with opener.open(base+'/api/state', timeout=45) as response:
                    snapshot = json.load(response)
                jobs = tracker.due(snapshot, time.time())
                write_state(ledger, records)
                for row, task in jobs:
                    # Recheck manual actions, binding and nonce immediately before a mutation.
                    with opener.open(base+'/api/servers/'+str(row), timeout=30) as response:
                        detail = json.load(response)
                    fresh = tracker.due({'servers':[detail['server']]}, time.time())
                    if not any(candidate['id'] == task['id'] and candidate['operator_request']['id'] == task['operator_request']['id']
                               for _, candidate in fresh):
                        continue
                    payload = json.dumps({'request_id':task['operator_request']['id'], 'action':'retry'}).encode()
                    request = Request(base+'/api/tasks/'+task['id']+'/decision', data=payload, method='POST',
                                      headers={'Origin':base, 'Content-Type':'application/json'})
                    try:
                        with opener.open(request, timeout=60) as response:
                            json.load(response)
                    except (HTTPError, URLError, TimeoutError):
                        continue  # Next snapshot reconciles a lost response; never blindly repeat a POST.
                    tracker.accepted(task)
                    recovered += 1
                    write_state(ledger, records)
                write_state(status, {'state':'MONITORING', 'pid':os.getpid(), 'worker_pid':deployment['pid'],
                                     'checked_at':time.time(), 'recovered':recovered,
                                     'waiting':sum(r.get('accepted') != r.get('request_id') for r in records.values())})
            except (HTTPError, URLError, TimeoutError, ConnectionError):
                write_state(status, {'state':'WAITING_API', 'pid':os.getpid(), 'recovered':recovered})
            time.sleep(30)


if __name__ == '__main__':
    main()
