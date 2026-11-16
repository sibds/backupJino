from jino_backup.export_inventory import list_export_entries,reconcile_export_entries
from jino_backup.errors import BackupError


def test_lists_directories_and_nested_files_without_following_links():
    root=[{'type':'dir','name':'folder','path':'dir-token','size':0},
          {'type':'file','name':'one.sql','path':'file-token','size':10,'mtime':'1'},
          {'type':'dir','name':'link','path':'unsafe-token','targetType':'dir'}]
    calls=[]
    def read(path):
        calls.append(path)
        assert path=='dir-token'
        return [{'type':'file','name':'two.sql','path':'nested-token','size':20,'mtime':'2'}]
    rows=list_export_entries(read,root,{'snapshot_ref':'snapshot','databases':['one','two']})
    assert {row['relative_path'] for row in rows}=={'folder','one.sql','link','folder/two.sql'}
    assert calls==['dir-token']
    assert next(row for row in rows if row['name']=='link')['selectable'] is False


def test_reconciles_registry_and_shows_missing_and_changed_files():
    known=[{'path':'file-token','name':'one.sql','size_bytes':10,'mtime':'1','local_verified':True},
           {'path':'gone-token','name':'gone.sql','size_bytes':5,'mtime':'1','local_verified':True}]
    rows=[{'path':'file-token','name':'one.sql','kind':'file','size_bytes':11,'mtime':'2','relative_path':'one.sql'},
          {'path':'new-token','name':'new.sql','kind':'file','size_bytes':3,'mtime':'3','relative_path':'new.sql'}]
    compared=reconcile_export_entries(rows,known)
    assert {row['name']:row['comparison'] for row in compared}=={
        'one.sql':'different','new.sql':'untracked','gone.sql':'missing_remote'}
    assert compared[0]['internal']['size_bytes']==10


def test_incomplete_directory_does_not_claim_missing_files():
    known=[{'path':'gone','name':'one.sql','size_bytes':10,'mtime':'1'}]
    rows=[{'path':'folder','kind':'dir','name':'folder','relative_path':'folder','listing_error':'PANEL_RESPONSE_FAILED'}]
    compared=reconcile_export_entries(rows,known)
    assert compared[-1]['comparison']=='not_checked'


def test_collision_always_collects_inventory_before_operator_question(tmp_path):
    from test_panel_export import PanelFixture,SQL
    from jino_backup.panel_export import PanelExportJob
    from cryptography.fernet import Fernet
    panel=PanelFixture();panel.files['private_database_one']={'path':'old-sql','size_bytes':len(SQL)}
    inspected=[]
    def compare(active,scope,entries):
        inspected.append(set(entries))
        return 3
    job=PanelExportJob(tmp_path/'state',tmp_path/'copy','panel-example','request','2026-11-10',Fernet.generate_key(),
        lambda:panel,defer_export_conflicts=True,collision_callback=compare)
    result=job.run()
    assert result['status']=='NEEDS_ACTION' and result['inventory_reconciled']
    assert result['candidates_count']==3 and inspected
    assert result['databases_count']==1  # The unrelated database continued.
    assert panel.deleted==[]


def test_registry_metadata_is_encrypted_and_compared_with_numeric_times(tmp_path):
    from cryptography.fernet import Fernet
    from jino_backup.database_inventory import DatabaseInventory
    inventory=DatabaseInventory(tmp_path/'private.sqlite3',Fernet.generate_key())
    inventory.remember_exports(1,1,'2026-11-16',{'db-a':{'name':'private_database_name',
        'remote':{'path':'/backups/2026-11-16/folder/private.sql','size_bytes':10,'mtime':1700000000},
        'artifact':{'validation':'PASS','size_bytes':10}}})
    rows=inventory.reconcile_exports(1,1,'2026-11-16',[
        {'path':'/backups/2026-11-16/folder','kind':'dir','relative_path':'folder'},
        {'path':'/backups/2026-11-16/folder/private.sql','kind':'file','relative_path':'folder/private.sql','size_bytes':10,'mtime':1700000000}])
    assert rows[0]['comparison']=='known_container' and rows[1]['comparison']=='same_metadata'
    assert b'private.sql' not in inventory.path.read_bytes()


def test_nested_sql_rechecks_parent_chain_before_download():
    from jino_backup.panel import JinoPanelBrowser
    panel=JinoPanelBrowser.__new__(JinoPanelBrowser)
    parent={'type':'dir','name':'folder','path':'folder-token'}
    panel._export_listing=lambda scope:[parent]
    calls=[]
    panel._list=lambda token:calls.append(token) or [{'type':'file','name':'one.sql','path':'file-token','size':10,'mtime':'1'}]
    panel._tasks_idle=lambda:True
    panel.check_existing_export({}, {'path':'file-token','size_bytes':10,'mtime':'1','ancestors':[{'name':'folder','path':'folder-token'}]})
    assert calls==['folder-token']
    parent['targetType']='dir'
    import pytest
    with pytest.raises(BackupError):
        panel.check_existing_export({}, {'path':'file-token','size_bytes':10,'mtime':'1','ancestors':[{'name':'folder','path':'folder-token'}]})
