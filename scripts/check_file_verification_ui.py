"""Synthetic read-only card check; never contacts real accounts."""
import json,re,tempfile,threading
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright,expect
from jino_backup.dashboard import DashboardData,make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.state import Settings


def main():
    root=Path(tempfile.mkdtemp(prefix='file-verification-',dir='.test-tmp')).absolute()
    source=root/'synthetic.json'
    source.write_text(json.dumps([{'id':'synthetic','name':'Демонстрационный сервер','login':'demo',
        'password':'synthetic','ftp_host':'ftp.example.test','ftp_login':'demo','ftp_password':'synthetic'}]),encoding='utf-8')
    mapping={k:k for k in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root/'state',root/'archives',root/'state/master.key')
    data=DashboardData(root/'state',root/'copies',source,mapping,'synthetic-verification');data.demo=True
    management=DashboardManagement(data,runner=lambda *args:{'status':'FILES_ONLY'})
    management._schedule=lambda:None;data.management=management
    task=management.start(1,'files',{'transport':'ftps'})
    progress={'download_phase_complete':True,'inventory_complete':True,'planned_files':7673,
        'processed_files':1261,'verified_files':7902,'pending_verifications':6412,
        'received_bytes':714229120,'planned_bytes':714229120,'active_transfers':0}
    management._update(task['id'],state='RUNNING',stage='FILE_VERIFY',progress=progress)
    server=make_server(data,port=0);threading.Thread(target=server.serve_forever,daemon=True).start()
    captures=Path('.impeccable/review');captures.mkdir(parents=True,exist_ok=True)
    errors=[];blocked=[]
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,1000,800,390):
                page=browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda error:errors.append(str(error)))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port or route.request.method!='GET':
                        blocked.append(route.request.method);route.abort()
                    else:route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#server/1',wait_until='networkidle')
                target=page.locator('#card-file-verification')
                expect(target).to_be_visible()
                expect(target).to_contain_text('Передача файлов завершена')
                expect(target).to_contain_text(re.compile(r'Проверено 1\s261 из 7\s673'))
                expect(target).to_contain_text(re.compile(r'6\s412 ожидают проверки'))
                assert '7902' not in re.sub(r'\s','',page.locator('#card-backup-facts').inner_text())
                page.evaluate('document.activeElement?.blur();scrollTo(0,0)')
                page.screenshot(path=str(captures/f'file-verification-overview-{width}.png'),full_page=True)
                page.locator('[data-card-tab="files"]').click()
                expect(page.get_by_label('Передача файлов сервера',exact=True)).to_have_attribute('value','100')
                bar=page.locator('#files-job').get_by_label('Проверка файлов сервера',exact=True)
                expect(bar).to_have_attribute('max','7673');expect(bar).to_have_attribute('value','1261')
                expect(page.locator('#files-job')).to_contain_text(re.compile(r'6\s412 ожидают проверки'))
                assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
                page.evaluate('document.activeElement?.blur();scrollTo(0,0)')
                page.screenshot(path=str(captures/f'file-verification-files-{width}.png'),full_page=True)
                page.locator('[data-view="servers"]').click()
                expect(page.locator('#detail-file-verification')).to_contain_text(re.compile(r'Проверено 1\s261 из 7\s673'))
                page.close()
            browser.close()
        assert not errors,errors
        assert 'POST' not in blocked
        print(json.dumps({'widths':[1440,1000,800,390],'errors':errors,'synthetic_only':True,'mutations':0}))
    finally:
        server.shutdown();server.server_close();management.close()


if __name__=='__main__':main()
