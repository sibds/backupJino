"""Synthetic database card and scheduling UI; never contacts Jino."""
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
    root = Path(tempfile.mkdtemp(prefix='database-card-', dir='.test-tmp')).absolute()
    source = root / 'synthetic.json'
    source.write_text(json.dumps([{'id':'1','name':'Демонстрационный сервер', 'login':'demo',
        'password':'synthetic','ftp_host':'ftp.example.test','ftp_login':'demo','ftp_password':'synthetic'}]), encoding='utf-8')
    mapping = {x:x for x in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root / 'state', root / 'archives', root / 'state/master.key')
    data = DashboardData(root / 'state', root / 'copies', source, mapping, 'synthetic')
    management = DashboardManagement(data, runner=lambda *args:{'status':'DB_ONLY'})
    management._schedule = lambda:None
    data.management = management
    account=management.catalog.get(1)['account_ref']
    management.inventory.discover(1,1,account,['synthetic_store','synthetic_long_database_name_for_mobile_layout'])
    item=management.inventory.list(1,1)[0]
    management.inventory.verified(1,1,item['db_alias'],'2026-11-10','2026-11-13T01:00:00+00:00',123456)
    server=make_server(data,port=0)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    errors, writes = [], []
    captures=Path('.impeccable/review');captures.mkdir(parents=True,exist_ok=True)
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,1000,800,390):
                page=browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda e:errors.append(str(e)))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port:
                        route.abort()
                    else:
                        if route.request.method!='GET': writes.append(url.path)
                        route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#server/1',wait_until='networkidle')
                page.locator('[data-card-tab="databases"]').click()
                expect(page.locator('.database-item')).to_have_count(2)
                expect(page.locator('#database-catalog-list')).to_contain_text('Дамп проверен')
                expect(page.locator('#database-catalog-list')).to_contain_text('2026')
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
                if width==1440:
                    row=page.locator('.database-item').first
                    row.locator('input[type="checkbox"]').check()
                    row.get_by_role('button',name='Сохранить расписание').click()
                    expect(page.locator('.database-next').first).to_contain_text('Следующая постановка')
                    assert management.inventory.list(1,1)[0]['schedule']['enabled']
                page.evaluate('window.scrollTo(0,0)')
                page.screenshot(path=str(captures/f'database-card-{width}.png'),full_page=True)
                if width==390:
                    page.locator('.database-item').first.get_by_role('button',name='Выгрузить эту БД').click()
                    expect(page.locator('#card-message')).to_contain_text('Действие принято')
                    assert management.tasks(1)[0]['db_aliases']==[item['db_alias']]
                    assert management.tasks(1)[0]['state']=='QUEUED'
                page.close()
            browser.close()
    finally:
        server.shutdown();server.server_close();management.close()
    assert not errors,errors
    assert len(writes)==2,writes
    print(json.dumps({'widths':[1440,1000,800,390],'page_errors':0,
        'schedule_saved':True,'selected_job_queued':True,'synthetic_only':True}))


if __name__=='__main__': main()
