import pytest
from cryptography.fernet import Fernet
from jino_backup.database_inventory import DatabaseInventory
from jino_backup.errors import BackupError
from test_panel_export import PanelFixture,SQL


def test_export_candidates_are_encrypted_and_scoped_to_task(tmp_path):
    inventory=DatabaseInventory(tmp_path/'private.sqlite3',Fernet.generate_key())
    candidate={'id':'a'*64,'name':'private-db.sql','path':'/private/path','db_alias':'db-a','size_bytes':123}
    inventory.save_export_candidates(1,1,'task-a',[candidate])
    assert inventory.export_candidates(1,1,'task-a')==[candidate]
    assert inventory.export_candidates(1,2,'task-a')==[]
    assert inventory.export_candidates(1,1,'task-b')==[]
    assert b'private-db' not in inventory.path.read_bytes()


def test_selected_existing_export_downloads_without_submit_or_remote_delete(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    from jino_backup.importers import identity
    panel=PanelFixture();name='private_database_one'
    alias=identity('db','panel-example',name)
    candidate={'id':'a'*64,'path':'/backups/2026-11-10/'+name+'.sql','size_bytes':len(SQL),'mtime':'1','decision':'new_version'}
    panel.check_existing_export=lambda scope,remote:None
    job=PanelExportJob(tmp_path/'state',tmp_path/'copy','panel-example','request-1','2026-11-10',
        Fernet.generate_key(),lambda:panel,selected_aliases=[alias],existing_exports={alias:candidate})
    result=job.run()
    assert result['status']=='DB_ONLY'
    assert panel.exports==[] and panel.deleted==[]
    assert result['source_snapshot_date'] is None
    artifact=result['artifacts'][0]
    assert artifact['relative_path'].startswith('versions/')
    assert artifact['source_snapshot_verified'] is False
    assert artifact['validation']=='PASS'


def test_existing_export_changed_after_selection_is_not_adopted(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    from jino_backup.importers import identity
    panel=PanelFixture();alias=identity('db','panel-example','private_database_one')
    def reject(*args):raise BackupError('SOURCE_CHANGED')
    panel.check_existing_export=reject
    candidate={'id':'a'*64,'path':'/backups/data.sql','size_bytes':len(SQL),'decision':'new_version'}
    job=PanelExportJob(tmp_path/'state',tmp_path/'copy','panel-example','request-1','2026-11-10',
        Fernet.generate_key(),lambda:panel,selected_aliases=[alias],existing_exports={alias:candidate})
    result=job.run()
    assert result['status']=='FAILED' and result['error_code']=='SOURCE_CHANGED'
    assert not result['artifacts'] and panel.exports==[] and panel.deleted==[]


def test_update_selected_sql_preserves_verified_previous_bytes(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    from jino_backup.importers import identity
    panel=PanelFixture();alias=identity('db','panel-example','private_database_one')
    panel.check_existing_export=lambda *args:None
    first={'id':'a'*64,'path':'/backups/data.sql','size_bytes':len(SQL),'decision':'new_version'}
    job=PanelExportJob(tmp_path/'state',tmp_path/'copy','panel-example','request-1','2026-11-10',
        Fernet.generate_key(),lambda:panel,selected_aliases=[alias],existing_exports={alias:first})
    assert job.run()['status']=='DB_ONLY'
    changed=SQL+b'-- updated\n'
    panel.download=lambda scope,remote,target,max_bytes:target.write_bytes(changed)
    job.existing_exports={alias:{**first,'id':'b'*64,'size_bytes':len(changed),'decision':'use_remote'}}
    result=job.run()
    assert result['status']=='DB_ONLY'
    assert (job.output/result['artifacts'][0]['relative_path']).read_bytes()==changed
    assert any(file.read_bytes()==SQL for file in (job.output/'history').glob('*.sql'))
    assert panel.exports==[] and panel.deleted==[]


def test_read_only_candidate_inspection_initializes_observed_listing_without_export():
    from types import SimpleNamespace
    from jino_backup.panel import JinoPanelBrowser
    panel=JinoPanelBrowser.__new__(JinoPanelBrowser)
    payload={'variables':{'objectId':'container-synthetic','serviceName':'hosting'},'query':'observed synthetic read'}
    panel.file_listing_request=SimpleNamespace(post_data_json=payload)
    def listing(scope):
        assert panel.listing_payload==payload
        return [{'type':'file','name':'synthetic_db.sql','path':'opaque-private-token','size':123,'mtime':'1'}]
    panel._export_listing=listing
    candidates=panel.export_candidates({'container_id':'container-synthetic','snapshot_ref':'snapshot','databases':['synthetic_db']})
    assert candidates[0]['supported'] and candidates[0]['size_bytes']==123
