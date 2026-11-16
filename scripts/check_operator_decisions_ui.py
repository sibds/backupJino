"""Synthetic operator prompts and explicit variant selection; no Jino access."""
import json,tempfile,threading
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright,expect
from jino_backup.dashboard import DashboardData,make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.state import Settings


def main():
    root=Path(tempfile.mkdtemp(prefix='operator-ui-',dir='.state')).resolve()
    source=root/'source.json'
    source.write_text(json.dumps([{'id':str(i),'name':f'Демонстрационный сервер {i}','login':f'demo-{i}',
        'password':'synthetic','ftp_host':'ftp.example.test','ftp_login':f'demo-{i}','ftp_password':'synthetic'} for i in (1,2)]),encoding='utf-8')
    mapping={key:key for key in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps',ftp_port=21)
    Settings.initialize(root/'state',root/'archives',root/'state/master.key')
    data=DashboardData(root/'state',root/'copies',source,mapping,'operator-demo');data.demo=True
    management=DashboardManagement(data,runner=lambda *args:{'status':'DB_ONLY'})
    management._schedule=lambda:None;data.management=management
    database=management.start(1,'databases',{})
    management.inventory.discover(1,1,management.catalog.get(1)['account_ref'],['synthetic_database'])
    alias=management.inventory.list(1,1)[0]['db_alias'];candidate_id='a'*64
    management.inventory.save_export_candidates(1,1,database['id'],[{'id':candidate_id,'name':'synthetic_database_'+('long_'*18)+'.sql',
        'path':'private-synthetic-path','size_bytes':12345678,'mtime':'2026-11-16T10:00:00Z','db_alias':alias,'supported':True,'comparison':'untracked'}])
    management._update(database['id'],state='FAILED',error_code='REMOTE_DESTINATION_EXISTS',
        result={'status':'NEEDS_ACTION','error_code':'REMOTE_DESTINATION_EXISTS','candidates_count':1})
    management._request_operator(database['id'])
    files=management.start(2,'files',{})
    key='b'*64
    management.event(2,'FILE_CONFLICT',kind='files',task_id=files['id'],code='FILE_CONFLICT_PENDING',
        conflict={'id':key,'remote_path':'folder/source.txt','local_path':'folder/source.txt','reason':'LOCAL_CONTENT_DIFFERS',
                  'remote_size':2000,'local_size':1000,'remote_modified':'20261116100000','local_modified':'20261115100000',
                  'remote_sha256':'a'*64,'local_sha256':'b'*64})
    management._update(files['id'],state='FINISHED',result={'status':'PARTIAL','file_conflicts_count':1})
    management._request_operator(files['id'])
    server=make_server(data,port=0);threading.Thread(target=server.serve_forever,daemon=True).start()
    base=f'http://127.0.0.1:{server.server_port}'
    captures=Path('.impeccable/review');captures.mkdir(parents=True,exist_ok=True)
    errors=[];posts=[]
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(channel='chrome',headless=True)
            for width in (1440,1000,800,390):
                page=browser.new_page(viewport={'width':width,'height':1050});page.emulate_media(reduced_motion='reduce')
                page.on('pageerror',lambda error:errors.append(str(error)))
                def guard(route):
                    url=urlsplit(route.request.url)
                    if url.hostname!='127.0.0.1' or url.port!=server.server_port:return route.abort()
                    if route.request.method=='POST':posts.append((url.path,route.request.post_data_json))
                    route.continue_()
                page.route('**/*',guard)
                page.goto(base,wait_until='networkidle')
                expect(page.locator('#operator-notifications')).to_contain_text('Нужно ваше решение · 2')
                page.locator('#operator-notifications a').filter(has_text='Демонстрационный сервер 1').click()
                expect(page.locator('#databases-operator-request .operator-request')).to_be_visible()
                expect(page.locator('#databases-operator-request .export-candidate')).to_contain_text('На Jino:')
                assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
                page.evaluate('document.activeElement?.blur();scrollTo(0,0)')
                page.screenshot(path=str(captures/f'operator-decisions-{width}.png'),full_page=True)
                if width==390:
                    page.evaluate("""async()=>{
                      const data=await (await fetch('/api/state')).json();const original=data.servers[0].tasks[0];
                      delete data.servers[0].latest_tasks;
                      for(let i=0;i<20;i++)data.servers[0].tasks.push({...original,id:'synthetic-extra-'+i,operator_request:{...original.operator_request,id:'extra-'+i}});
                      document.dispatchEvent(new CustomEvent('backup:state',{detail:data}));
                    }""")
                    expect(page.locator('#operator-notifications')).to_contain_text('Нужно ваше решение · 22')
                    assert page.locator('#operator-notifications > ul > li').count()==3
                    expect(page.locator('#operator-notifications summary')).to_contain_text('Ещё запросов: 19')
                    page.screenshot(path=str(captures/'operator-many-390.png'),full_page=True)
                page.close()
            page=browser.new_page(viewport={'width':1440,'height':1050});page.on('pageerror',lambda error:errors.append(str(error)))
            page.route('**/*',guard);page.goto(base,wait_until='networkidle')
            page.locator('#operator-notifications a').filter(has_text='Демонстрационный сервер 1').click()
            page.get_by_role('button',name='Пропустить',exact=True).evaluate('(button)=>{button.click();button.click()}')
            expect(page.locator('#operator-notifications')).to_contain_text('Нужно ваше решение · 1')
            expect(page.get_by_role('button',name='Вернуться к решению',exact=True)).to_be_visible()
            page.get_by_role('button',name='Вернуться к решению',exact=True).click()
            expect(page.locator('#operator-notifications')).to_contain_text('Нужно ваше решение · 2')
            page.locator('#databases-operator-request .export-candidate select').select_option('new_version')
            page.get_by_role('button',name='Применить выбранные действия',exact=True).evaluate('(button)=>{button.click();button.click()}')
            expect(page.locator('#operator-notifications')).to_contain_text('Нужно ваше решение · 1')
            assert management.task(database['id'])['export_decisions']=={candidate_id:'new_version'}
            page.locator('#operator-notifications a').filter(has_text='Демонстрационный сервер 2').click()
            page.locator('#files-operator-request').get_by_role('button',name='Новая папка версии',exact=True).evaluate('(button)=>{button.click();button.click()}')
            expect(page.locator('#operator-notifications')).to_be_hidden()
            assert management.task(files['id'])['conflict_decisions'][key]=='new_version'
            assert sum(path.endswith('/decision') for path,_ in posts)==3
            assert sum(path.endswith('/conflict') for path,_ in posts)==1
            page.close();browser.close()
        assert not errors,errors
        print(json.dumps({'widths':[1440,1000,800,390],'synthetic_posts':len(posts),'page_errors':errors,'synthetic_only':True}))
    finally:server.shutdown();server.server_close();management.close()


if __name__=='__main__':main()
