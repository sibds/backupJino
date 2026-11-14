"""Synthetic-only visual and interaction checks for bulk work plans."""
import json
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright,expect
from jino_backup.dashboard import DashboardData,make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.state import Settings

def screenshot(page,path):
    page.evaluate('document.activeElement?.blur(); window.scrollTo(0,0)')
    page.wait_for_timeout(150)
    page.screenshot(path=str(path),full_page=True)


def main():
    root=Path(tempfile.mkdtemp(prefix='work-plan-',dir='.test-tmp')).absolute()
    source=root/'synthetic.json'
    source.write_text(json.dumps([{'id':str(i),'name':f'Сервер {i:02}','login':f'demo-{i:02}',
        'password':'synthetic' if i!=6 else '','ftp_host':'ftp.example.test','ftp_login':f'demo-{i:02}',
        'ftp_password':'synthetic'} for i in range(1,7)]),encoding='utf-8')
    mapping={key:key for key in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root/'state',root/'archives',root/'state/master.key')
    data=DashboardData(root/'state',root/'copies',source,mapping,'synthetic-work-plan')
    data.demo=True
    management=DashboardManagement(data,runner=lambda *args:{'status':'FILES_ONLY'})
    management._schedule=lambda:None
    data.management=management
    first=management.start(1,'files',{})
    management._update(first['id'],state='RUNNING',stage='FILE_DOWNLOAD',progress={'received_bytes':123456789,'verified_files':512})
    management.start(4,'databases',{})
    management.work_plans.submit({'rows':[5],'files':True,'databases':True,'every_hours':168,
        'scheduled_at':'2030-01-10T10:00:00+00:00','request_id':'synthetic-initial-rules'})
    server=make_server(data,port=0)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    errors=[];writes=[];blocked=[];reject=[False];rejected=[]
    capture=Path('.impeccable/review');capture.mkdir(parents=True,exist_ok=True)
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,1000,800,390):
                page=browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda e:errors.append(str(e)))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port:
                        blocked.append(url.hostname);route.abort();return
                    if route.request.method=='POST' and url.path=='/api/plan/bulk' and reject[0]:
                        reject[0]=False;rejected.append(route.request.post_data_json)
                        route.fulfill(status=503,content_type='application/json',body=json.dumps({'message':'План временно недоступен. Повторите действие.'}));return
                    if route.request.method!='GET': writes.append((url.path,route.request.post_data_json))
                    route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#plan',wait_until='networkidle')
                expect(page.locator('.plan-server-choice')).to_have_count(6)
                page.locator('[data-plan-row="2"]').check()
                page.locator('[data-plan-row="3"]').check()
                page.locator('[data-plan-row="6"]').check()
                page.locator('#plan-databases').check()
                expect(page.locator('[data-plan-row="6"]')).to_be_enabled()
                page.locator('[data-plan-row="6"]').uncheck()
                page.locator('#plan-repeat').select_option('72')
                page.locator('#plan-start-at').fill('2030-01-01T12:00')
                expect(page.locator('#plan-selected-count')).to_have_text('Выбрано 2')
                page.locator('#plan-search').fill('Сервер 02')
                expect(page.locator('.plan-server-choice')).to_have_count(1)
                expect(page.locator('#plan-selected-count')).to_have_text('Выбрано 2')
                page.locator('#plan-search').fill('')
                assert page.locator('#plan-repeat option').count()==14
                assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
                if width==1440:
                    reject[0]=True
                    page.locator('#plan-submit').click()
                    expect(page.locator('#plan-message')).to_contain_text('Повторите действие')
                    expect(page.locator('#plan-selected-count')).to_have_text('Выбрано 2')
                    screenshot(page,capture/'work-plan-error-1440.png')
                start=len(writes)
                page.locator('#plan-submit').dblclick()
                expect(page.locator('#plan-message')).to_contain_text('Расписание сохранено')
                expect(page.locator('#plan-submit')).to_be_enabled()
                screenshot(page,capture/f'work-plan-{width}.png')
                sent=[payload for path,payload in writes[start:] if path=='/api/plan/bulk']
                assert len(sent)==1 and sent[0]['rows']==[2,3] and sent[0]['databases'] is True
                if width==1440: assert sent[0]['request_id']==rejected[0]['request_id']
                assert len([r for r in management.work_plans.list() if r['row'] in {2,3}])==4
                page.locator('#plan-bulk-action').select_option('disable_schedule')
                page.locator('#plan-apply-action').click()
                expect(page.locator('#plan-message')).to_have_text('План обновлён')
                assert all(not r['enabled'] for r in management.work_plans.list() if r['row'] in {2,3})
                if width==1440:
                    page.locator('#plan-submit').click()
                    expect(page.locator('#plan-message')).to_contain_text('Расписание сохранено')
                    assert all(r['enabled'] for r in management.work_plans.list() if r['row'] in {2,3})
                page.locator('#plan-repeat').select_option('')
                page.locator('#plan-start-at').fill('')
                page.locator('#plan-submit').click()
                expect(page.locator('#plan-message')).to_contain_text('В очереди 4 заданий')
                assert len([t for t in management.tasks() if t['row'] in {2,3}])==4
                expect(page.locator('[data-plan-row="2"]')).to_be_checked()
                for action,expected_state in [('pause','PAUSED'),('resume','QUEUED'),('stop','STOPPED')]:
                    page.locator('#plan-bulk-action').select_option(action)
                    page.locator('#plan-apply-action').click()
                    expect(page.locator('#plan-message')).to_have_text('План обновлён')
                    assert all(t['state']==expected_state for t in management.tasks() if t['row'] in {2,3})
                if width==390:
                    page.locator('#plan-search').fill('нет такого сервера')
                    expect(page.locator('#plan-servers')).to_contain_text('Серверы не найдены')
                    screenshot(page,capture/'work-plan-empty-390.png')
                page.locator('[data-view="servers"]').click()
                assert not page.evaluate('document.body.classList.contains("plan-mode")')
                page.close()
            browser.close()
        assert not errors,errors
        print(json.dumps({'widths':[1440,1000,800,390],'errors':errors,'writes':len(writes),
                          'blocked_external_requests':len(blocked),'synthetic_only':True}))
    finally:
        server.shutdown();server.server_close()


if __name__=='__main__': main()
