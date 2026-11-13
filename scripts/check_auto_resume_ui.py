"""Synthetic automatic-resume state and operator cancellation in Chrome."""
import json
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright, expect
from jino_backup.dashboard import DashboardData, make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.errors import BackupError
from jino_backup.state import Settings


def main():
    root=Path(tempfile.mkdtemp(prefix='auto-resume-ui-',dir='.test-tmp')).absolute()
    source=root/'synthetic.json'
    source.write_text(json.dumps([{'id':'1','name':'Демонстрационный сервер','login':'demo',
        'password':'synthetic','ftp_host':'ftp.example.test','ftp_login':'demo','ftp_password':'synthetic'}]),encoding='utf-8')
    mapping={x:x for x in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root/'state',root/'archives',root/'state/master.key')
    data=DashboardData(root/'state',root/'copies',source,mapping,'synthetic')
    def broken(server,task,control,progress):
        progress(received_bytes=1234)
        raise BackupError('TRANSFER_INTERRUPTED')
    management=DashboardManagement(data,runner=broken);data.management=management
    task=management.start(1,'files',{})
    end=time.monotonic()+3
    while management.task(task['id'])['state']!='SCHEDULED' and time.monotonic()<end: time.sleep(.01)
    assert management.task(task['id'])['state']=='SCHEDULED'
    server=make_server(data,port=0)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    errors=[]
    captures=Path('.impeccable/review');captures.mkdir(parents=True,exist_ok=True)
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,390):
                page=browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda e:errors.append(type(e).__name__))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port: route.abort()
                    else: route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#server/1',wait_until='networkidle')
                page.locator('[data-card-tab="files"]').click()
                expect(page.locator('#files-job')).to_contain_text('Автопродолжение №1')
                expect(page.locator('#files-job')).to_contain_text('Сохранённые части будут использованы повторно')
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                page.evaluate('window.scrollTo(0,0)')
                page.screenshot(path=str(captures/f'auto-resume-{width}.png'),full_page=True)
                if width==390:
                    page.locator('#files-job').get_by_role('button',name='Остановить',exact=True).click()
                    expect(page.locator('#files-job')).to_contain_text('Остановлено')
                    assert management.task(task['id'])['state']=='STOPPED'
                    assert not management.task(task['id']).get('auto_resume_at')
                page.close()
            browser.close()
    finally:
        server.shutdown();server.server_close();management.close()
    assert not errors,errors
    print(json.dumps({'widths':[1440,390],'auto_resume_visible':True,'operator_stop_cancels':True,
        'same_task_count':len(management.tasks()),'page_errors':0,'synthetic_only':True}))


if __name__=='__main__': main()
