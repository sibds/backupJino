import pytest
from test_dashboard_management import management
from cryptography.fernet import Fernet
from jino_backup.errors import BackupError
from jino_backup.importers import identity
from jino_backup.panel import select_snapshot_row
from jino_backup.panel_protocol import RESTORE_QUERY, export_payload_allowed, validate_panel_sql
from jino_backup.panel_downloads import latest_mysql_snapshot

PG_SQL=b'--\n-- PostgreSQL database dump\n--\n\nSET statement_timeout = 0;\nCREATE TABLE public.example (id integer);\n\n--\n-- PostgreSQL database dump complete\n--\n'


def test_postgresql_snapshot_selection_is_separate_from_mysql():
    rows=[{'label':'Копия от 10 ноября 2026','kinds':'MySQL','index':0},
          {'label':'Копия от 11 ноября 2026','kinds':'PostgreSQL','index':1}]
    assert latest_mysql_snapshot(rows)=='2026-11-10'
    assert select_snapshot_row(rows,'2026-11-11',engine='postgresql')['index']==1


def test_postgresql_allowlist_accepts_only_corresponding_observed_mutation():
    variables={'name':'synthetic','serverId':'one','db':'example','archive':False,'accountId':'account'}
    query=RESTORE_QUERY.replace('restoreMySQLBackup','restorePostgreSQLBackup')
    assert export_payload_allowed({'query':query,'variables':variables},variables,engine='postgresql')
    assert not export_payload_allowed({'query':RESTORE_QUERY,'variables':variables},variables,engine='postgresql')


def test_postgresql_dump_validation_requires_header_and_finished_trailer(tmp_path):
    path=tmp_path/'pg.sql';path.write_bytes(PG_SQL)
    validate_panel_sql(path,engine='postgresql')
    with pytest.raises(BackupError):validate_panel_sql(path)
    for bad in [PG_SQL.replace(b'dump complete',b'dump interrupted'),PG_SQL[:80],b'<html>error</html>',b'PGDMP'+b'\x00'*100]:
        path.write_bytes(bad)
        with pytest.raises(BackupError):validate_panel_sql(path,engine='postgresql')


def test_same_database_name_gets_distinct_engine_aliases(tmp_path):
    from jino_backup.database_inventory import DatabaseInventory
    inventory=DatabaseInventory(tmp_path/'inventory.sqlite3',Fernet.generate_key())
    inventory.discover(1,1,'account',['same'],engine='mysql')
    inventory.discover(1,1,'account',['same'],engine='postgresql')
    entries=inventory.list(1,1)
    assert len(entries)==2
    assert {entry['engine'] for entry in entries}=={'mysql','postgresql'}
    assert next(entry for entry in entries if entry['engine']=='mysql')['db_alias']==identity('db','account','same')


def test_postgresql_export_job_checks_pg_and_resumes_without_duplicate_export(tmp_path):
    from test_panel_export import PanelFixture
    from jino_backup.panel_export import PanelExportJob
    class PgPanel(PanelFixture):
        def inspect(self,date):return {**super().inspect(date),'database_engine':'postgresql'}
        def submit_export(self,scope,name):
            super().submit_export(scope,name)
            self.files[name]['size_bytes']=len(PG_SQL)
        def download(self,scope,remote,path,limit):path.write_bytes(PG_SQL)
    panel=PgPanel();key=Fernet.generate_key()
    def job():return PanelExportJob(tmp_path/'state',tmp_path/'copies','panel-example','same-request',
        '2026-11-10',key,lambda:panel,database_engine='postgresql')
    first=job().run();second=job().run()
    assert first['status']==second['status']=='DB_ONLY'
    assert first['database_engine']=='postgresql'
    assert len(panel.exports)==2 and not panel.deleted
    assert first['job_id']==second['job_id']


def test_managed_all_databases_includes_both_engines_without_second_login(monkeypatch):
    from jino_backup.dashboard_runner import OperatorRunner
    from types import SimpleNamespace
    class Panel:
        database_engine='mysql'
        def database_snapshots(self):return {'mysql':'2026-11-10','postgresql':'2026-11-11'}
        def inspect(self,date):return {'binding_verified':True,'database_engine':self.database_engine,
            'databases':['same'],'snapshot_date':date}
    runner=OperatorRunner(SimpleNamespace(),None)
    seen=[]
    def execute(panel,server,task,control,progress,scope):
        seen.append((panel.database_engine,task.get('db_aliases')))
        return {'status':'DB_ONLY','database_engine':panel.database_engine,'databases_count':1,
            'artifacts':[{'size_bytes':20,'validation':'PASS'}],'source_snapshot_date':scope['snapshot_date']}
    monkeypatch.setattr(runner,'_database_engine',execute)
    result=runner._databases(Panel(),{'account_ref':'account'}, {'id':'task'},None,lambda **kw:None)
    assert [engine for engine,selection in seen]==['mysql','postgresql']
    assert result['status']=='DB_ONLY' and result['databases_count']==2
    assert result['source_snapshot_date'] is None


def test_managed_selection_is_validated_across_engines_before_export(monkeypatch):
    from jino_backup.dashboard_runner import OperatorRunner
    from types import SimpleNamespace
    from jino_backup.db_engines import database_alias
    class Panel:
        database_engine='mysql'
        def database_snapshots(self):return {'mysql':'2026-11-10','postgresql':'2026-11-11'}
        def inspect(self,date):return {'binding_verified':True,'database_engine':self.database_engine,
            'databases':['same'],'snapshot_date':date}
    runner=OperatorRunner(SimpleNamespace(),None);seen=[]
    def execute(panel,server,task,control,progress,scope):
        seen.append(panel.database_engine)
        return {'status':'DB_ONLY','databases_count':1,'artifacts':[]}
    monkeypatch.setattr(runner,'_database_engine',execute)
    runner._databases(Panel(),{'account_ref':'account'}, {'id':'task','db_aliases':[database_alias('account','same','postgresql')]},None,lambda **kw:None)
    assert seen==['postgresql']
    seen.clear()
    with pytest.raises(BackupError):runner._databases(Panel(),{'account_ref':'account'}, {'id':'task','db_aliases':['missing']},None,lambda **kw:None)
    assert seen==[]


def test_real_managed_engine_jobs_keep_typed_inventory_and_reuse_both(management,monkeypatch):
    from test_panel_export import PanelFixture,SQL
    from jino_backup.dashboard_runner import OperatorRunner
    from jino_backup.dashboard_management import TaskControl
    class MixedPanel(PanelFixture):
        database_engine='mysql'
        def restore_job(self,entries):
            self.owned_paths=getattr(self,'owned_paths',set())|{entry['remote']['path'] for entry in entries.values() if entry.get('remote')}
        def inspect_files(self,**kwargs):return {'binding_verified':True}
        def database_snapshots(self):return {'mysql':'2026-11-10','postgresql':'2026-11-11'}
        def inspect(self,date):
            return {**super().inspect(date),'database_engine':self.database_engine,
                    'snapshot_ref':'snapshot-'+self.database_engine,'databases':[self.database_engine+'_example']}
        def latest_mysql_snapshot(self):return '2026-11-10'
        def submit_export(self,scope,name):
            super().submit_export(scope,name)
            self.files[name].update(engine=self.database_engine,size_bytes=len(PG_SQL if self.database_engine=='postgresql' else SQL))
        def download(self,scope,remote,path,limit):path.write_bytes(PG_SQL if remote['engine']=='postgresql' else SQL)
    panel=MixedPanel()
    monkeypatch.setattr('jino_backup.dashboard_runner.JinoPanelBrowser',lambda credentials:panel)
    management._schedule=lambda:None
    task=management.start(1,'databases',{})
    runner=OperatorRunner(management.data,management.catalog)
    report=lambda **kw:management.progress(task['id'],kw)
    first=runner(management.catalog.get(1),task,TaskControl(lambda state:None),report)
    assert first['status']=='DB_ONLY' and first['databases_count']==2
    assert len(panel.exports)==2
    saved=management.inventory.list(1,1)
    assert len(saved)==2 and all(item['validation']=='PASS' for item in saved)
    assert {item['engine'] for item in saved}=={'mysql','postgresql'}
    from jino_backup.database_inventory import DatabaseInventory
    from jino_backup.state import Settings
    restored=DatabaseInventory(management.data.state_dir/'history-pg.sqlite3',Settings.load(management.data.state_dir).read_key())
    restored.import_history(management.catalog,[{**management.task(task['id']),'state':'FINISHED','result':first}],management.data.state_dir)
    assert {item['engine'] for item in restored.list(1,1)}=={'mysql','postgresql'}
    assert all(item['validation']=='PASS' for item in restored.list(1,1))
    panel.owned_paths=set()
    second=runner(management.catalog.get(1),management.task(task['id']),TaskControl(lambda state:None),report)
    assert second['status']=='DB_ONLY' and len(panel.exports)==2
    assert len(panel.owned_paths)==2
    assert management.task(task['id'])['database_snapshots']=={'mysql':'2026-11-10','postgresql':'2026-11-11'}


def test_one_engine_integrity_failure_does_not_prevent_the_other_engine(monkeypatch):
    from jino_backup.dashboard_runner import OperatorRunner
    from types import SimpleNamespace
    class Panel:
        def database_snapshots(self):return {'mysql':'2026-11-10','postgresql':'2026-11-11'}
        def inspect(self,date):return {'binding_verified':True,'database_engine':self.database_engine,'databases':['example'],'snapshot_date':date}
    runner=OperatorRunner(SimpleNamespace(),None);seen=[]
    def execute(panel,*args):
        seen.append(panel.database_engine)
        if panel.database_engine=='mysql':raise BackupError('DB_INTEGRITY_FAILED')
        return {'status':'DB_ONLY','databases_count':1,'artifacts':[]}
    monkeypatch.setattr(runner,'_database_engine',execute)
    result=runner._databases(Panel(),{'account_ref':'account'},{'id':'task'},None,lambda **kw:None)
    assert seen==['mysql','postgresql']
    assert result['error_code']=='DB_INTEGRITY_FAILED' and result['databases_count']==1
