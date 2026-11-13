from cryptography.fernet import Fernet
import pytest
from jino_backup.errors import BackupError


def test_inventory_preserves_verified_partial_jobs_and_encrypts_names(tmp_path):
    from jino_backup.database_inventory import DatabaseInventory
    inventory = DatabaseInventory(tmp_path / 'inventory.sqlite3', Fernet.generate_key())
    inventory.discover(1, 1, 'panel-example', ['synthetic_private_database'])
    item = inventory.list(1, 1)[0]
    inventory.verified(1, 1, item['db_alias'], '2026-11-10', '2026-11-13T01:00:00+00:00', 100)
    row = inventory.list(1, 1)[0]
    assert row['name'] == 'synthetic_private_database'
    assert row['validation'] == 'PASS' and row['snapshot_date'] == '2026-11-10'
    assert b'synthetic_private_database' not in inventory.path.read_bytes()
    assert not inventory.list(1, 2)


def test_database_schedule_is_persistent_and_bound_to_known_database(tmp_path):
    from jino_backup.database_inventory import DatabaseInventory
    key = Fernet.generate_key()
    inventory = DatabaseInventory(tmp_path / 'inventory.sqlite3', key)
    inventory.discover(1, 1, 'panel-example', ['example'])
    alias = inventory.list(1, 1)[0]['db_alias']
    with pytest.raises(BackupError):
        inventory.schedule(1, 1, {'db_alias':'unknown', 'every_hours':24, 'enabled':True})
    for value in (0, True, 8761, '24'):
        with pytest.raises(BackupError):
            inventory.schedule(1, 1, {'db_alias':alias, 'every_hours':value, 'enabled':True})
    inventory.schedule(1, 1, {'db_alias':alias, 'every_hours':24, 'enabled':True})
    restored = DatabaseInventory(inventory.path, key)
    assert restored.list(1, 1)[0]['schedule']['every_hours'] == 24


def test_selected_database_export_does_not_export_its_neighbours(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    from jino_backup.importers import identity
    from test_panel_export import PanelFixture
    panel = PanelFixture()
    selected = identity('db', 'panel-example', 'private_database_two')
    job = PanelExportJob(tmp_path / 'state', tmp_path / 'copies', 'panel-example', 'selected-job',
        '2026-11-10', Fernet.generate_key(), lambda: panel, selected_aliases=[selected])
    result = job.run()
    assert result['status'] == 'DB_ONLY' and result['databases_count'] == 1
    assert panel.exports == ['private_database_two']
    assert not panel.deleted


def test_history_import_keeps_verified_dumps_from_a_failed_job(tmp_path):
    from types import SimpleNamespace
    from jino_backup.database_inventory import DatabaseInventory
    from jino_backup.panel_export import PanelExportJob
    from test_panel_export import PanelFixture
    key = Fernet.generate_key()
    class Panel(PanelFixture):
        def download(self, scope, remote, destination, max_bytes):
            if 'private_database_two' in remote['path']:
                raise BackupError('DB_INTEGRITY_FAILED')
            return super().download(scope, remote, destination, max_bytes)
    job = PanelExportJob(tmp_path / 'state', tmp_path / 'copies', 'panel-example', 'partial',
        '2026-11-10', key, lambda: Panel())
    result = job.run()
    assert result['status'] == 'FAILED' and result['databases_count'] == 1
    inventory = DatabaseInventory(tmp_path / 'inventory.sqlite3', key)
    catalog = SimpleNamespace(get=lambda *a, **kw: {'binding_revision':1, 'account_ref':'panel-example'})
    inventory.import_history(catalog, [{'row':1,'revision':1,'kind':'databases','state':'FAILED','result':result}], tmp_path / 'state')
    rows = inventory.list(1, 1)
    assert len(rows) == 2
    passed = [x for x in rows if x.get('validation') == 'PASS']
    assert len(passed) == 1 and passed[0]['downloaded_at']
    assert passed[0]['snapshot_date'] == '2026-11-10'
    from jino_backup.account_batch import completed_database_copy
    scope = PanelFixture().inspect('2026-11-10')
    scope['databases'] = ['private_database_one']
    # A separate named server folder is required by the reuse ownership contract.
    folder = 'server-example'
    reused = completed_database_copy(tmp_path / 'state', tmp_path / 'copies', 'panel-example',
                                     folder, scope, key, allow_subset=True)
    assert reused['databases_count'] == 1 and reused['reused_existing']
