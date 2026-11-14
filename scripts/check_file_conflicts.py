"""Synthetic browser checks for conflict decisions; blocks all external requests."""
import json
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright, expect
from jino_backup.dashboard import DashboardData, make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.state import Settings


def main():
    root=Path(tempfile.mkdtemp(prefix='file-conflicts-',dir='.test-tmp')).absolute()
    source=root/'synthetic.json'
    source.write_text(json.dumps([{'id':'1','name':'Тестовый сервер','login':'demo','password':'synthetic',
        'ftp_host':'ftp.example.test','ftp_login':'demo','ftp_password':'synthetic'}]),encoding='utf-8')
    mapping={k:k for k in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root/'state',root/'archives',root/'state/master.key')
    data=DashboardData(root/'state',root/'copies',source,mapping,'synthetic-conflicts')
    management=DashboardManagement(data,runner=lambda *args:{'status':'FILES_ONLY'})
    management._schedule=lambda:None
    data.management=management
    task=management.start(1,'files',{})
    management._update(task['id'],state='RUNNING',stage='FILE_DOWNLOAD')
    server=make_server(data,port=0)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    errors=[]; writes=[]; blocked=[]
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,390):
                key=('a' if width==1440 else 'b')*64
                management.progress(task['id'],{'stage':'FILE_CONFLICT','log':True,'error_code':'FILE_CONFLICT_PENDING',
                    'conflict':{'id':key,'remote_path':'images/logo.png','local_path':'images/Logo.png',
                                'remote_size':1000,'local_size':1000,'remote_modified':'20261114124500',
                                'local_modified':'20261114124500','remote_sha256':'a'*64,'local_sha256':'b'*64,
                                'same_content':False,'reason':'CASE_COLLISION'}})
                page=browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda error:errors.append(str(error)))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port:
                        blocked.append(url.hostname);route.abort();return
                    if route.request.method!='GET': writes.append(route.request.post_data_json)
                    route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#server/1',wait_until='networkidle')
                page.locator('[data-card-tab="files"]').click()
                expect(page.locator('.file-conflicts')).to_be_visible()
                expect(page.locator('.file-conflicts')).to_contain_text('images/Logo.png')
                expect(page.locator('.file-conflicts')).to_contain_text('SHA-256 различается')
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                capture=Path('.impeccable/review');capture.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(capture/f'file-conflicts-{width}.png'),full_page=True)
                page.get_by_role('button',name='Сохранить оба',exact=True).last.click()
                expect(page.locator('.file-conflicts')).to_contain_text('Решение сохранено')
                assert management.task(task['id'])['state']=='RUNNING'
                assert management.task(task['id'])['conflict_decisions'][key]=='keep_both'
                page.close()
            browser.close()
        assert not errors, errors
        assert len(writes)==2
        print(json.dumps({'widths':[1440,390],'decisions':len(writes),'errors':errors,
                          'blocked_external_requests':len(blocked),'synthetic_only':True}))
    except Exception:
        print(json.dumps({'errors':errors,'conflict_events':len(management.events(1,stage='FILE_CONFLICT')),
                          'state':management.task(task['id'])['state'],
                          'detail_task':data.server_detail(1)['server'].get('latest_tasks',{}).get('files',{}).get('id'),
                          'event_task':management.events(1,stage='FILE_CONFLICT')[0].get('task_id')}))
        raise
    finally:
        server.shutdown();server.server_close()


if __name__=='__main__': main()
