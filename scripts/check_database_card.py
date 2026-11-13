"""Synthetic database copies, bulk schedules and responsive UI; never contacts Jino."""
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
    account = management.catalog.get(1)['account_ref']
    names = ['client_catalog','client_catalog_long_database_name_for_mobile_layout',
             'client_orders','client_sessions','client_statistics','client_unsaved']
    management.inventory.discover(1,1,account,names,snapshot='2026-11-13')
    items = management.inventory.list(1,1)
    for index, size, snapshot, downloaded in ((0,123456,'2026-11-13','2026-11-13T01:00:00+00:00'),
        (1,8388608,'2026-11-10','2026-11-14T01:00:00+00:00'),(2,0,'2026-11-13',None),
        (4,None,None,None)):
        management.inventory.verified(1,1,items[index]['db_alias'],snapshot,downloaded,size)
    server = make_server(data,port=0)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    errors, writes = [], []
    reject = [False]
    captures = Path('.impeccable/review'); captures.mkdir(parents=True,exist_ok=True)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,1000,800,390):
                page = browser.new_page(viewport={'width':width,'height':1100})
                page.on('pageerror',lambda e:errors.append(str(e)))
                def guard(route):
                    url = urlsplit(route.request.url)
                    if url.hostname != '127.0.0.1' or url.port != server.server_port:
                        route.abort(); return
                    if route.request.method != 'GET':
                        writes.append((url.path,route.request.post_data_json))
                        if reject[0]:
                            reject[0] = False
                            route.fulfill(status=400,content_type='application/json',body=json.dumps({'message':'Не удалось сохранить расписание. Повторите действие.'})); return
                    route.continue_()
                page.route('**/*',guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/#server/1',wait_until='networkidle')
                page.locator('[data-card-tab="databases"]').click()
                expect(page.locator('.database-item')).to_have_count(6)
                expect(page.locator('#database-catalog-summary')).to_contain_text('Скачано 4 из 6')
                expect(page.locator('#db-download-missing')).to_have_text('Скачать недостающие (2)')
                expect(page.locator('#db-download-missing')).to_be_enabled()
                expect(page.locator('#database-catalog-list')).to_contain_text('Есть снимок новее')
                expect(page.locator('.database-item').nth(2).locator('.database-size')).to_have_text('0 Б')
                expect(page.locator('.database-item').nth(4).locator('.database-size')).to_have_text('Неизвестен')
                page.locator('#db-search').fill('client_catalog')
                expect(page.locator('.database-item')).to_have_count(2)
                page.locator('#db-select-all').check()
                expect(page.locator('#db-selected-count')).to_have_text('Выбрано 2')
                page.locator('#db-search').fill('')
                assert page.locator('#db-select-all').evaluate('(node)=>node.indeterminate')
                page.locator('#refresh').click()
                expect(page.locator('#db-selected-count')).to_have_text('Выбрано 2')
                for filter_value, count in [('missing',2),('saved',4),('newer',1),('all',6)]:
                    page.locator('#db-filter').select_option(filter_value)
                    expect(page.locator('.database-item')).to_have_count(count)
                if width == 1440:
                    page.locator('#db-schedule-toggle').click()
                    page.locator('#db-schedule-preset').select_option('custom')
                    page.locator('#db-schedule-hours').fill('0')
                    previous_writes = len(writes)
                    page.locator('#db-schedule-apply').click()
                    assert not page.locator('#db-schedule-hours').evaluate('(node)=>node.checkValidity()')
                    assert len(writes) == previous_writes
                    page.locator('#db-schedule-preset').select_option('168')
                    page.locator('#db-schedule-apply').click()
                    expect(page.locator('#card-message')).to_contain_text('Расписание сохранено для 2 БД')
                    assert all(x['schedule']['every_hours']==168 for x in management.inventory.list(1,1)[:2])
                    page.locator('#db-schedule-preset').select_option('72')
                    page.locator('#db-schedule-apply').click()
                    expect(page.locator('.database-next').first).to_contain_text('Следующая постановка')
                    expect(page.locator('.database-schedule-action').first).to_have_text('Каждые 72 часа')
                    assert all(x['schedule']['every_hours']==72 for x in management.inventory.list(1,1)[:2])
                    page.locator('#db-schedule-disable').click()
                    expect(page.locator('.database-next').first).to_have_text('Выключено')
                    page.locator('#db-schedule-apply').click()
                    expect(page.locator('.database-schedule-action').first).to_have_text('Каждые 72 часа')
                    reject[0] = True
                    page.locator('#db-schedule-apply').click()
                    expect(page.locator('#card-message')).to_contain_text('Не удалось сохранить расписание')
                    expect(page.locator('#db-selected-count')).to_have_text('Выбрано 2')
                    page.evaluate('window.scrollTo(0,0)')
                    page.screenshot(path=str(captures/f'database-error-{width}.png'),full_page=True)
                    page.locator('#db-schedule-apply').click()
                    expect(page.locator('#card-message')).to_contain_text('Расписание сохранено для 2 БД')
                    page.evaluate('window.scrollTo(0,0)')
                    page.screenshot(path=str(captures/f'database-schedule-{width}.png'),full_page=True)
                    page.locator('#db-schedule-toggle').click()
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
                page.locator('#db-search').fill('no-such-database')
                expect(page.locator('#database-catalog-list')).to_contain_text('Нет баз по выбранному фильтру')
                page.locator('#db-search').fill('')
                page.evaluate('window.scrollTo(0,0)')
                page.screenshot(path=str(captures/f'database-card-{width}.png'),full_page=True)
                if width == 390:
                    page.locator('#db-schedule-toggle').click()
                    page.locator('#db-schedule-preset').select_option('custom')
                    page.locator('#db-schedule-hours').fill('36')
                    page.locator('#refresh').click()
                    expect(page.locator('#db-schedule-hours')).to_have_value('36')
                    page.evaluate('window.scrollTo(0,0)')
                    page.screenshot(path=str(captures/f'database-schedule-{width}.png'),full_page=True)
                    page.locator('#db-schedule-toggle').click()
                    page.locator('#db-download-selected').click()
                    expect(page.locator('#card-message')).to_contain_text('Выгрузка 2 БД добавлена в очередь')
                    assert management.tasks(1)[0]['db_aliases']==sorted(x['db_alias'] for x in items[:2])
                    assert management.tasks(1)[0]['state']=='QUEUED'
                    expect(page.locator('#db-download-selected')).to_be_disabled()
                    expect(page.locator('#db-operation-note')).to_be_visible()
                    expect(page.locator('#db-download-missing')).to_be_disabled()
                    management.action(management.tasks(1)[0]['id'],'stop')
                    page.locator('#refresh').click()
                    expect(page.locator('#db-operation-note')).to_be_hidden()
                    page.locator('#db-search').fill('client_catalog')
                    expect(page.locator('.database-item')).to_have_count(2)
                    expect(page.locator('#db-download-missing')).to_be_enabled()
                    writes_before = sum(path.endswith('/jobs') for path,_ in writes)
                    page.locator('#db-download-missing').evaluate('(button)=>{button.click();button.click();}')
                    expect(page.locator('#db-operation-note')).to_be_visible()
                    missing = sorted(item['db_alias'] for index,item in enumerate(items) if index in (3,5))
                    assert management.tasks(1)[0]['db_aliases'] == missing
                    assert sum(path.endswith('/jobs') for path,_ in writes) == writes_before + 1
                    page.locator('#db-search').fill('')
                    page.evaluate('window.scrollTo(0,0)')
                    page.screenshot(path=str(captures/f'database-busy-{width}.png'),full_page=True)
                    def empty_catalog(route):
                        response = route.fetch()
                        result = response.json()
                        result['server']['database_inventory'] = []
                        result['server']['tasks'] = []
                        route.fulfill(response=response,json=result)
                    page.route('**/api/servers/1',empty_catalog)
                    page.reload(wait_until='networkidle')
                    page.locator('[data-card-tab="databases"]').click()
                    expect(page.locator('#database-catalog-list')).to_contain_text('Названия БД ещё не получены')
                    expect(page.locator('#db-download-selected')).to_be_disabled()
                    expect(page.locator('#db-download-missing')).to_be_disabled()
                    page.evaluate('window.scrollTo(0,0)')
                    page.screenshot(path=str(captures/f'database-empty-{width}.png'),full_page=True)
                page.close()
            browser.close()
    finally:
        server.shutdown(); server.server_close(); management.close()
    assert not errors,errors
    assert all(path.endswith(('/database-schedules','/jobs')) for path,_ in writes)
    assert not any(name in json.dumps(payload) for _,payload in writes for name in names)
    print(json.dumps({'widths':[1440,1000,800,390],'page_errors':0,'bulk_schedule_72h_weekly':True,
        'selection_survives_refresh':True,'selected_job_queued':True,'error_and_empty_states':True,'synthetic_only':True}))


if __name__=='__main__':
    main()
