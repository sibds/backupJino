"""Jino panel account selection and observed browser adapter.

Credential values remain in the provider/adapter and are never included in reports.
"""

from dataclasses import dataclass,field
from datetime import date
import json
import re
import hashlib
import time
from pathlib import Path
from urllib.parse import unquote,urlsplit

from .errors import BackupError
from .importers import identity,jino_rows,parse_jino


@dataclass
class PanelSource:
    source_row: int
    account_ref: str
    candidates: list = field(repr=False)
    folder_name: str = field(default='',repr=False)


def select_panel_source(path,mapping,source_row):
    rows = jino_rows(path)
    if not isinstance(source_row,int) or not 1 <= source_row <= len(rows):
        raise BackupError('SOURCE_ROW_INVALID')
    batch = parse_jino(path,mapping)
    row = rows[source_row-1]
    account_ref = identity('panel',row.get(mapping['id']))
    if not any(c.kind=='panel' and c.profile_id==account_ref for c in batch.candidates):
        code = next((x['code'] for x in batch.report['issues'] if x['row']==source_row),'ACCOUNT_UNUSABLE')
        raise BackupError(code)
    refs = {account_ref,identity('ftp',row.get(mapping['id']))}
    from .server_folders import folder_names
    return PanelSource(source_row,account_ref,[c for c in batch.candidates if c.profile_id in refs],
                       folder_names(rows,mapping)[source_row-1])


MONTHS = dict(zip(('января','февраля','марта','апреля','мая','июня','июля','августа',
                   'сентября','октября','ноября','декабря'),range(1,13)))


def select_snapshot_row(rows,snapshot_date):
    matches = []
    for row in rows:
        if 'MySQL' not in row.get('kinds',''):
            continue
        match = re.fullmatch(r'Копия от (\d{1,2}) (\S+) (\d{4})',row.get('label','').strip())
        if not match or match[2] not in MONTHS:
            raise BackupError('UI_CHANGED')
        try:
            observed = date(int(match[3]),MONTHS[match[2]],int(match[1])).isoformat()
        except ValueError:
            raise BackupError('UI_CHANGED') from None
        if observed==snapshot_date:
            matches.append(row)
    if len(matches)>1:
        raise BackupError('SNAPSHOT_AMBIGUOUS')
    if not matches:
        raise BackupError('SNAPSHOT_NOT_FOUND')
    return matches[0]


def verify_export_destination(info,snapshot_date):
    text = re.sub(r'\s+',' ',info).strip()
    match = re.fullmatch(r'(?:Выберите дату и базу для восстановления\. )?SQL-дамп будет скопирован в (/backups/\d{4}-\d{2}-\d{2})',text)
    if not match or match[1] != '/backups/'+snapshot_date:
        raise BackupError('EXPORT_DESTINATION_UNVERIFIED')
    return match[1]


def unpack_json(value):
    try:
        for _ in range(2):
            if not isinstance(value,str):
                return value
            value = json.loads(value)
        return value
    except ValueError:
        raise BackupError('PANEL_RESPONSE_INVALID') from None


class JinoPanelBrowser:
    def __init__(self,credentials,*,channel='chrome',timeout_seconds=300,research_dir=None):
        self.credentials = credentials
        self.channel, self.timeout_seconds = channel,timeout_seconds
        self.pw = self.browser = self.context = self.page = None
        self.requests = []
        self.container_id = None
        self.capabilities = {}
        self.ui_step='NOT_STARTED'
        self.research_dir=Path(research_dir) if research_dir else None

    def __enter__(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise BackupError('PANEL_DEPENDENCY_MISSING') from None
        try:
            self.pw = sync_playwright().start()
            options = {'headless':True}
            if self.channel != 'chromium':
                options['channel']=self.channel
            self.browser=self.pw.chromium.launch(**options)
            self.context=self.browser.new_context(accept_downloads=True)
            self.context.set_default_timeout(min(15000,self.timeout_seconds*1000))
            self.context.on('request',self._remember_request)
            self.page=self.context.new_page()
            self._login()
            return self
        except Exception as exc:
            self.__exit__()
            if isinstance(exc,BackupError):
                raise
            raise BackupError('PANEL_BROWSER_OR_LOGIN_FAILED') from None

    def __exit__(self,*args):
        failed=False
        for item in (self.context,self.browser,self.pw):
            if item is not None:
                try:
                    item.close() if hasattr(item,'close') else item.stop()
                except Exception:
                    failed=True
        self.context=self.browser=self.pw=self.page=None
        self.requests.clear()  # Contains private requests in memory only, including auth headers.
        if failed:
            raise BackupError('CLEANUP_FAILED')

    def _remember_request(self,request):
        if request.resource_type not in {'xhr','fetch'} or request.method!='POST':
            return
        try:
            body=request.post_data_json
            query=body.get('query','') if isinstance(body,dict) else ''
            match=re.search(r'\b(?:query|mutation)\s+(\w+)',query)
            if match:
                self.requests.append((match[1],request))
                if len(self.requests)>1000:
                    del self.requests[0]
        except Exception:
            pass

    def _observed(self,name):
        matches=[req for op,req in self.requests if op==name]
        if not matches:
            raise BackupError('PANEL_OPERATION_NOT_OBSERVED')
        req=matches[-1]
        response=req.response()
        if response is None or not response.ok:
            raise BackupError('PANEL_RESPONSE_FAILED')
        data=response.json()
        if not isinstance(data,dict) or data.get('errors'):
            raise BackupError('PANEL_RESPONSE_FAILED')
        return req.post_data_json,data

    def _one(self,locator):
        locator.first.wait_for(state='visible')
        if locator.count()!=1:
            raise BackupError('UI_CHANGED')
        return locator

    def _login(self):
        self.page.goto('https://auth.jino.ru/login/?next=https://cp.jino.ru',wait_until='domcontentloaded',timeout=45000)
        login=self._one(self.page.locator('input[name="login"]'))
        password=self._one(self.page.locator('input[name="password"]'))
        submit=self._one(self.page.get_by_role('button',name='Войти',exact=True))
        login.fill(self.credentials['username'])
        password.fill(self.credentials['password'])
        submit.click()
        try:
            self.page.wait_for_url('https://cp.jino.ru/**',timeout=20000)
        except Exception:
            text=self.page.locator('body').inner_text()
            if re.search(r'одноразов|код подтверждения|код из приложения',text,re.I):
                raise BackupError('MFA_REQUIRED') from None
            if re.search(r'captcha|капч|робот|проверочный код',text,re.I):
                raise BackupError('CAPTCHA_REQUIRED') from None
            raise BackupError('AUTH_FAILED') from None

    def _open_container(self,open_backups=True):
        self.ui_step='OPEN_HOSTING_LIST'
        hosting=self.page.get_by_text('Хостинг',exact=True).locator('xpath=ancestor-or-self::a')
        self._one(hosting).click()
        self.ui_step='WAIT_CONTAINER_LINKS'
        self.page.wait_for_function("""() => [...document.querySelectorAll('a')].some(n=>{
            const u=new URL(n.href); return u.hostname==='cp.jino.ru'&&/^\/hosting\/[^/]+\/?$/.test(u.pathname)&&u.pathname!='/hosting/create'
        })""")
        hrefs=self.page.locator('a').evaluate_all("""nodes => [...new Set(nodes.map(n=>n.href).filter(h=>{
            const u=new URL(h); return u.hostname==='cp.jino.ru'&&/^\/hosting\/[^/]+\/?$/.test(u.pathname)&&u.pathname!='/hosting/create'
        }))]""")
        if len(hrefs)!=1:
            raise BackupError('CONTAINER_AMBIGUOUS',containers_count=len(hrefs))
        self.container_id=unquote(urlsplit(hrefs[0]).path.rstrip('/').split('/')[-1])
        self.ui_step='OPEN_CONTAINER'
        self.page.goto(hrefs[0],wait_until='domcontentloaded',timeout=45000)
        self.page.wait_for_load_state('networkidle',timeout=15000)
        if not open_backups:
            return
        self.ui_step='OPEN_BACKUPS'
        self._one(self.page.get_by_role('link',name='Бэкапы',exact=True)).click()
        self.page.wait_for_load_state('networkidle',timeout=15000)
        self.page.locator('.backups-page__row').first.wait_for(state='visible')

    def _snapshot_rows(self):
        return self.page.locator('.backups-page__row').evaluate_all("""nodes=>nodes.map((n,index)=>({
            index,label:n.querySelector('.backups-page__name')?.textContent?.trim()||'',
            kinds:n.querySelector('.backups-page__desc')?.textContent?.trim()||''
        }))""")

    def _open_export_form(self,snapshot_date):
        self.ui_step='SELECT_SNAPSHOT'
        if not self.page.url.split('?')[0].rstrip('/').endswith('/backups'):
            self._one(self.page.get_by_role('link',name='Бэкапы',exact=True)).click()
        self.page.locator('.backups-page__row').first.wait_for(state='visible')
        selected=select_snapshot_row(self._snapshot_rows(),snapshot_date)
        row=self.page.locator('.backups-page__row').nth(selected['index'])
        self.ui_step='OPEN_SNAPSHOT_MENU'
        self._one(row.get_by_role('button')).click()
        self.ui_step='WAIT_SNAPSHOT_MENU'
        restore=self._one(self.page.get_by_role('button',name='Восстановить базу MySQL',exact=True))
        self.ui_step='OPEN_DB_EXPORT_FORM'
        restore.click()  # Observed UI: opens a form; this does not submit a restore/export.
        self.ui_step='VERIFY_EXPORT_DESTINATION'
        info=self._one(self.page.locator('.backup-restore__info')).inner_text()
        destination=verify_export_destination(info,snapshot_date)
        self.page.locator('.backup-restore .ui-select__value').nth(1).wait_for(state='visible')
        self.page.wait_for_function("() => {const b=[...document.querySelectorAll('button')].find(x=>x.textContent.trim()==='Восстановить'); return b&&!b.disabled}")
        self.ui_step='READ_SNAPSHOT_DATABASES'
        request,data=self._observed('BackupDatabases')
        variables=request.get('variables',{})
        if variables.get('accountId')!=self.container_id:
            raise BackupError('PROFILE_UNVERIFIED')
        dbs=unpack_json(data.get('data',{}).get('me',{}).get('legacy',{}).get('getBackupMySQLDatabases'))
        names=dbs if isinstance(dbs,list) else dbs.get('dbs') if isinstance(dbs,dict) else None
        if not isinstance(names,list) or not names or not all(isinstance(x,str) and x for x in names):
            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
        return {'snapshot_date':snapshot_date,'snapshot_ref':identity('snapshot',self.container_id,variables.get('date'),variables.get('serverId')),
                'container_ref':identity('container',self.container_id),'container_id':self.container_id,
                'backup_name':variables.get('date'),'backup_server_id':variables.get('serverId'),
                'databases':names,'export_dir':destination,'binding_verified':True,'transport':'https_panel'}

    def inspect(self,snapshot_date):
        try:
            return self._inspect(snapshot_date)
        except BackupError:
            raise
        except Exception as exc:
            features={}
            try:
                paths=self.page.locator('a').evaluate_all("nodes=>nodes.map(n=>new URL(n.href)).filter(u=>u.hostname==='cp.jino.ru'&&u.pathname.startsWith('/hosting/')&&u.pathname!='/hosting/create').map(u=>u.pathname.split('/').filter(Boolean))")
                features={'container_links_count':len(paths),'container_path_depths':sorted({len(p) for p in paths})}
            except Exception:
                pass
            raise BackupError('UI_CHANGED',ui_step=self.ui_step,exception_type=type(exc).__name__,features=features) from None

    def _close_export_form(self):
        form=self.page.locator('.backup-restore')
        self._one(form.get_by_role('button',name='Закрыть',exact=True)).click()
        form.wait_for(state='hidden')

    def _inspect(self,snapshot_date):
        self._open_container()
        scope=self._open_export_form(snapshot_date)
        self._close_export_form()
        self.ui_step='INSPECT_FILE_ACCESS'
        self._one(self.page.get_by_role('link',name='Доступ к файлам',exact=True)).click()
        self.page.wait_for_timeout(800)
        controls={}
        for text in ['Файловый менеджер','Открыть файловый менеджер','Открыть менеджер файлов','Открыть','Запустить']:
            count=self.page.get_by_role('button',name=text,exact=True).count()+self.page.get_by_role('link',name=text,exact=True).count()
            if count:
                controls[text]=count
        headings=self.page.get_by_text('Файловый менеджер',exact=True).evaluate_all("""nodes=>nodes.map(n=>({tag:n.tagName,parentTag:n.parentElement.tagName,parentClass:n.parentElement.className,
            buttons:[...n.parentElement.querySelectorAll('button')].map(b=>({empty:!b.textContent.trim(),titlePresent:!!b.title}))}))""")
        scope['capabilities']={'file_manager_controls':controls,'file_manager_sections':headings,
                               'observed_operations':sorted({op for op,_ in self.requests}),
                               'file_transport_verified':False}
        if any(op=='FilemanagerFiles' for op,_ in self.requests):
            payload,data=self._observed('FilemanagerFiles')
            legacy=data.get('data',{}).get('me',{}).get('legacy',{})
            manager=data.get('data',{}).get('me',{}).get('filemanager',{})
            allowed={'name','path','files','dirs','directories','entries','items','children','type','size','mode','mtime','date',
                     'permissions','isDir','is_dir','filename','kind','content','data','result','error','total','currentDir',
                     'currentPath','parent','success','status','list','extension','fileType','url','downloadUrl'}
            def shape(value,depth=0):
                value=unpack_json(value) if isinstance(value,str) and value[:1] in {'[','{','"'} else value
                if isinstance(value,dict):
                    return {'type':'object','fields':{k:shape(v,depth+1) if depth<2 else type(v).__name__
                            for k,v in value.items() if k in allowed},'other_fields':len(set(value)-allowed)}
                if isinstance(value,list):
                    return {'type':'array','count':len(value),'sample_shape':shape(value[0],depth+1) if value and depth<2 else None}
                return {'type':type(value).__name__}
            query=re.sub(r'"(?:[^"\\]|\\.)*"','""',payload.get('query',''))
            scope['capabilities']['file_manager_schema']={
                'query':query,'variable_fields':sorted(payload.get('variables',{})),
                'response_fields':{k:shape(v) for k,v in legacy.items()},
                'modern_response':shape(manager),
                'object_binding_verified':payload.get('variables',{}).get('objectId')==self.container_id}
            listing=manager.get('files',{}) if isinstance(manager,dict) else {}
            items=listing.get('list',[]) if isinstance(listing,dict) else []
            from collections import Counter
            scope['capabilities']['file_manager_schema']['entry_types']=dict(Counter(str(x.get('type')) for x in items if isinstance(x,dict)))
            scope['capabilities']['file_manager_schema']['root_listing_count']=len(items)
            self.file_listing_request=next(req for op,req in reversed(self.requests) if op=='FilemanagerFiles')
            self.root_listing=listing
            self.root_file_token=payload.get('variables',{}).get('path')
            scope['capabilities']['file_manager_controls_schema']=self.page.locator('.fm button').evaluate_all("""nodes=>nodes.map(n=>({
                class:n.className, label_present:!!n.getAttribute('aria-label'),title_present:!!n.title,
                text:n.textContent.trim().match(/^(Файловый менеджер|FTP|SFTP|Загрузить|Создать|Скачать|Обновить|Назад)$/)?.[0]||null
            }))""")
        if self.research_dir is not None:
            from .state import private_dir
            private_dir(self.research_dir)
            urls=self.page.evaluate("() => performance.getEntriesByType('resource').map(r=>r.name).filter(n=>/\\.js(?:\\?|$)/.test(n))")
            contracts=[]
            for url in dict.fromkeys(urls):
                parsed=urlsplit(url)
                if parsed.scheme!='https' or parsed.hostname!='cp.jino.ru' or not parsed.path.startswith('/_nuxt/'):
                    continue
                response=self.context.request.get(url)
                code=response.text()
                if any(op in code for op in ('FilemanagerFiles','RestoreDatabaseBackup')) and ('export{' in code or 'export {' in code):
                    digest=hashlib.sha256(code.encode()).hexdigest()
                    cached=self.research_dir/('filemanager-'+digest[:16]+'.js')
                    if not cached.exists():
                        with cached.open('x',encoding='utf-8') as stream:
                            stream.write(code)
                    contracts.append({'file':cached.name,'sha256':digest})
            scope['capabilities']['public_ui_contracts']=contracts
        self.scope=scope
        return scope

    def preflight(self,scope):
        self.prepare_file_transport(scope)
        root=self._list(self.root_file_token)
        probes=[x for x in root if x.get('type')=='file' and x.get('size',0)>0
                and x.get('targetType') is None and x.get('targetDisplayPath') is None]
        if not probes:
            raise BackupError('FILETRANSFER_PROBE_UNAVAILABLE')
        probe=min(probes,key=lambda x:x['size'])
        self.transfer.probe(probe['path'])
        self._export_listing(scope)
        if not self._tasks_idle():
            raise BackupError('PANEL_ACCOUNT_BUSY')

    def prepare_file_transport(self,scope):
        from .panel_files import HTTPSFileTransfer
        from .panel_protocol import TOKEN_QUERY
        payload=self.file_listing_request.post_data_json
        variables=payload.get('variables',{})
        if variables.get('objectId')!=scope['container_id'] or variables.get('serviceName')!='hosting':
            raise BackupError('PROFILE_UNVERIFIED')
        self.listing_payload=payload
        self.owned_paths=set()
        self.baselines={}
        # Only this observed token operation is permitted in the API client.
        response=self._graphql(TOKEN_QUERY,{'serviceName':'hosting','objectId':scope['container_id']})
        value=unpack_json(response.get('data',{}).get('me',{}).get('legacy',{}).get('createFiletransferToken'))
        if not isinstance(value,dict):
            raise BackupError('FILETRANSFER_CREDENTIAL_INVALID')
        self.transfer=HTTPSFileTransfer(value.get('url'),value.get('token'))

    def inspect_files(self,prepare_transport=True):
        self._open_container(open_backups=False)
        self.ui_step='INSPECT_FILE_ACCESS'
        self._one(self.page.get_by_role('link',name='Доступ к файлам',exact=True)).click()
        self.page.wait_for_load_state('networkidle',timeout=15000)
        self.page.wait_for_function("() => document.querySelector('.fm')!==null")
        self.page.wait_for_timeout(400)
        payload,data=self._observed('FilemanagerFiles')
        if payload.get('variables',{}).get('objectId')!=self.container_id:
            raise BackupError('PROFILE_UNVERIFIED')
        self.file_listing_request=next(req for op,req in reversed(self.requests) if op=='FilemanagerFiles')
        self.root_file_token=payload['variables']['path']
        listing=data.get('data',{}).get('me',{}).get('filemanager',{}).get('files',{})
        root=listing.get('breadcrumbs',{}).get('home')
        from .panel_downloads import canonical_account_root
        root=canonical_account_root(root,self.root_file_token)
        scope={'binding_verified':True,'container_id':self.container_id,
               'container_ref':identity('container',self.container_id),'canonical_root':root,'transport':'https_panel'}
        if prepare_transport:
            self.prepare_file_transport(scope)
        return scope

    def latest_mysql_snapshot(self):
        from .panel_downloads import latest_mysql_snapshot
        self._one(self.page.get_by_role('link',name='Бэкапы',exact=True)).click()
        self.page.wait_for_load_state('networkidle',timeout=15000)
        self.page.locator('.backups-page__row').first.wait_for(state='visible',timeout=15000)
        return latest_mysql_snapshot(self._snapshot_rows())

    def _graphql(self,query,variables):
        parsed=urlsplit(self.file_listing_request.url)
        if (parsed.scheme!='https' or not parsed.hostname or not parsed.hostname.endswith('.jino.ru')
                or parsed.username is not None or parsed.password is not None):
            raise BackupError('PANEL_ENDPOINT_UNVERIFIED')
        header_source=next((req for op,req in reversed(self.requests) if req.url==self.file_listing_request.url),self.file_listing_request)
        headers={k:v for k,v in header_source.all_headers().items()
                 if k.lower() not in {'host','content-length','cookie','accept-encoding','connection'}
                 and not k.startswith(':')}
        response=self.context.request.post(self.file_listing_request.url,
                    data={'query':query,'variables':variables},headers=headers,timeout=30000,
                    max_redirects=0)
        try:
            if not response.ok:
                raise BackupError('PANEL_RESPONSE_FAILED')
            data=response.json()
            if not isinstance(data,dict) or data.get('errors'):
                raise BackupError('PANEL_RESPONSE_FAILED')
            return data
        finally:
            response.dispose()

    def _list(self,path):
        from .panel_files import listing_page
        entries=[]
        while True:
            variables={**self.listing_payload['variables'],'path':path,'start':len(entries),'count':200}
            page,total=listing_page(self._graphql(self.listing_payload['query'],variables))
            if total>100000 or not page and len(entries)<total:
                raise BackupError('FILE_LIST_LIMIT_EXCEEDED')
            entries.extend(page)
            if len(entries)>=total:
                if len({x.get('path') for x in entries})!=len(entries):
                    raise BackupError('REMOTE_PATH_AMBIGUOUS')
                return entries

    def _export_listing(self,scope):
        from .panel_files import resolve_export_directory
        directory=resolve_export_directory(self._list,self.root_file_token,scope['snapshot_date'])
        return self._list(directory) if directory is not None else []

    def restore_job(self,entries):
        self.owned_paths={x['remote']['path'] for x in entries.values() if x.get('remote')}
        self.baselines={x['name']:x['baseline'] for x in entries.values() if 'baseline' in x}

    def capture_baseline(self,scope,name):
        from .panel_protocol import check_owned_listing
        items=self._export_listing(scope)
        check_owned_listing(items,self.owned_paths)
        self.baselines[name]=[x['path'] for x in items]
        return self.baselines[name]

    def existing_export(self,scope,name):
        from .panel_protocol import check_owned_listing
        check_owned_listing(self._export_listing(scope),self.owned_paths)
        return None

    def _tasks_idle(self):
        requests={}
        for op,request in self.requests:
            if op=='Tasks':
                payload=request.post_data_json
                requests[json.dumps(payload.get('variables',{}),sort_keys=True)]=payload
        if not requests:
            raise BackupError('PANEL_TASKS_UNVERIFIED')
        for payload in requests.values():
            data=self._graphql(payload['query'],payload.get('variables',{}))
            try:
                edges=data['data']['me']['tasks']['edges']
                if not isinstance(edges,list):
                    raise ValueError
                for edge in edges:
                    node=edge['node']
                    if node.get('objectId')==self.container_id and 'backup' in node.get('taskClass',''):
                        return False
            except (KeyError,TypeError,ValueError):
                raise BackupError('PANEL_TASKS_UNVERIFIED') from None
        return True

    def submit_export(self,scope,name):
        from .panel_protocol import export_payload_allowed
        if name not in scope['databases'] or name not in self.baselines:
            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
        if not self._tasks_idle():
            raise BackupError('PANEL_ACCOUNT_BUSY')
        current=self._open_export_form(scope['snapshot_date'])
        if current['snapshot_ref']!=scope['snapshot_ref'] or current['export_dir']!=scope['export_dir']:
            raise BackupError('SNAPSHOT_MISMATCH')
        selector=self.page.locator('.backup-restore .ui-select__value').nth(1)
        if selector.inner_text().strip()!=name:
            selector.click()
            self._one(self.page.get_by_role('button',name=name,exact=True)).click()
        if selector.inner_text().strip()!=name:
            raise BackupError('DATABASE_SCOPE_UNVERIFIED')
        expected={'name':scope['backup_name'],'serverId':scope['backup_server_id'],
                  'db':name,'archive':False,'accountId':scope['container_id']}
        accepted=[]
        rejected=[]
        def guard(route):
            request=route.request
            if request.method!='POST':
                route.continue_()
                return
            try:
                payload=request.post_data_json
                query=payload.get('query','') if isinstance(payload,dict) else ''
                if re.search(r'\bmutation\b',query):
                    if accepted or not export_payload_allowed(payload,expected):
                        rejected.append(True)
                        route.abort()
                        return
                    accepted.append(True)
            except Exception:
                rejected.append(True)
                route.abort()
                return
            route.continue_()
        self.context.route(self.file_listing_request.url,guard)
        try:
            self._one(self.page.locator('.backup-restore').get_by_role('button',name='Восстановить',exact=True)).click()
            deadline=time.monotonic()+15
            while not accepted and not rejected and time.monotonic()<deadline:
                self.page.wait_for_timeout(100)
            if rejected:
                # If the permitted request was already sent, keep its uncertain journal.
                raise BackupError('UI_CHANGED' if accepted else 'UI_MUTATION_BLOCKED')
            if not accepted:
                raise BackupError('PANEL_RESPONSE_LOST')
            # The panel may return before the task finishes. Reconcile via read-only polling.
            self.page.wait_for_timeout(1000)
        finally:
            self.context.unroute(self.file_listing_request.url,guard)

    def wait_export(self,scope,name,timeout_seconds):
        from .panel_protocol import new_export
        if name not in self.baselines:
            raise BackupError('EXPORT_BASELINE_MISSING')
        deadline=time.monotonic()+timeout_seconds
        previous=None
        stable_at=None
        while time.monotonic()<deadline:
            if hasattr(self,'control'):
                before_control=time.monotonic()
                self.control()
                deadline+=time.monotonic()-before_control
            item=new_export(self._export_listing(scope),self.baselines[name])
            signature=(item.get('path'),item.get('size'),item.get('mtime')) if item else None
            now=time.monotonic()
            if signature and signature==previous:
                if now-stable_at>=5 and self._tasks_idle():
                    self.owned_paths.add(item['path'])
                    return {'path':item['path'],'size_bytes':item['size'],'mtime':item.get('mtime')}
            else:
                previous=signature
                stable_at=now
            self.page.wait_for_timeout(2000)
        raise BackupError('EXPORT_TIMEOUT')

    def download(self,scope,remote,destination,max_bytes):
        if hasattr(self,'control'):
            self.transfer.control = self.control
        if hasattr(self,'download_progress'):
            self.transfer.progress = lambda received: self.download_progress(received,remote['size_bytes'])
        self.transfer.download(remote['path'],destination,max_bytes)
        if destination.stat().st_size!=remote['size_bytes']:
            raise BackupError('INTEGRITY_FAILED')

    def cleanup(self,scope,remote):
        raise BackupError('REMOTE_DELETE_DISABLED')
