import json
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from jino_backup.errors import BackupError


SQL = b'-- MySQL dump 10.13\nCREATE TABLE example (id int);\n-- Dump completed on 2026-11-10\n'


class PanelFixture:
    def __init__(self):
        self.exports = []
        self.deleted = []
        self.files = {}
        self.preflight_error = None
        self.closed = False
        self.interrupt_after_submit = False
        self.cleanup_error = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def inspect(self, snapshot_date):
        return {'container_ref':'container-example', 'snapshot_date':snapshot_date,
                'snapshot_ref':'snapshot-example', 'databases':['private_database_one','private_database_two'],
                'transport':'https_panel', 'binding_verified':True}

    def preflight(self, scope):
        if self.preflight_error:
            raise BackupError(self.preflight_error)

    def existing_export(self, scope, database):
        return self.files.get(database)

    def submit_export(self, scope, database):
        self.exports.append(database)
        self.files[database] = {'path':'/backups/2026-11-10/'+database+'.sql', 'size_bytes':len(SQL)}
        if self.interrupt_after_submit:
            self.interrupt_after_submit = False
            raise BackupError('PANEL_RESPONSE_LOST')

    def wait_export(self, scope, database, timeout_seconds):
        return self.files.get(database)

    def download(self, scope, remote, destination, max_bytes):
        destination.write_bytes(SQL)

    def cleanup(self, scope, remote):
        if self.cleanup_error:
            raise BackupError('CLEANUP_FAILED')
        self.deleted.append(remote['path'])
        for key, value in list(self.files.items()):
            if value['path'] == remote['path']:
                del self.files[key]


def job(tmp_path, panel, key=None):
    from jino_backup.panel_export import PanelExportJob
    return PanelExportJob(tmp_path/'state', tmp_path/'output', 'panel-example', 'request-1',
                          '2026-11-10', key or Fernet.generate_key(), lambda:panel,cleanup_remote=True)


def test_cli_dry_run_does_not_login_or_create_output(tmp_path):
    from test_cli import command
    source, mapping = tmp_path/'source.json', tmp_path/'mapping.json'
    source.write_text('[{"id":"1","login":"alice","password":"synthetic-secret"}]')
    mapping.write_text('{"id":"id","login":"login","password":"password"}')
    result = command('backup','export-jino-db','--file',source,'--mapping',mapping,
                     '--source-row','1','--destination',tmp_path/'output',
                     '--idempotency-key','request-1','--reuse-snapshot','2026-11-10')
    assert result.returncode == 0
    assert json.loads(result.stdout)['dry_run']
    assert not (tmp_path/'output').exists()
    assert 'synthetic-secret' not in result.stdout+result.stderr


def test_transport_failure_is_reported_before_any_export(tmp_path):
    panel = PanelFixture()
    panel.preflight_error = 'TLS_CERTIFICATE_INVALID'
    result = job(tmp_path,panel).run()
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'TLS_CERTIFICATE_INVALID'
    assert panel.exports == []
    assert panel.closed
    assert not list((tmp_path/'output').rglob('*.sql'))


def test_preflight_only_verifies_transport_without_export_or_output(tmp_path):
    panel=PanelFixture()
    result=job(tmp_path,panel).run(preflight_only=True)
    assert result['status']=='TRANSPORT_VERIFIED'
    assert result['transport_verified'] and not result['test_passed']
    assert not panel.exports and not panel.deleted
    assert not (tmp_path/'output').exists()


def test_blocked_unexpected_mutation_is_ui_changed_and_has_no_uncertain_submit(tmp_path):
    panel=PanelFixture()
    def blocked(scope,name):
        raise BackupError('UI_MUTATION_BLOCKED')
    panel.submit_export=blocked
    instance=job(tmp_path,panel)
    result=instance.run()
    assert result['error_code']=='UI_CHANGED'
    assert not panel.exports
    assert all(x['phase']=='NEW' for x in instance.state['databases'].values())


def test_download_interruption_retains_partial_and_retry_does_not_export_again(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    class Interrupted(PanelFixture):
        broken=True
        def download(self,scope,remote,destination,max_bytes):
            if self.broken:
                self.broken=False
                destination.write_bytes(SQL[:10])
                raise BackupError('FILETRANSFER_CONNECTION_FAILED')
            super().download(scope,remote,destination,max_bytes)
    panel=Interrupted()
    key=Fernet.generate_key()
    def run():
        return PanelExportJob(tmp_path/'state',tmp_path/'output','panel-example','request-1',
                              '2026-11-10',key,lambda:panel).run()
    first=run()
    assert first['status']=='FAILED'
    partials=list((tmp_path/'output').rglob('*.partial-*'))
    assert len(partials)==1 and partials[0].read_bytes()==SQL[:10]
    second=run()
    assert second['status']=='DB_ONLY'
    assert panel.exports.count('private_database_one')==1
    assert len(panel.exports)==2 and not panel.deleted
    assert partials[0].read_bytes()==SQL[:10]


def test_restart_after_final_rename_recovers_journal_without_redownload(tmp_path,monkeypatch):
    import jino_backup.panel_export as module
    panel=PanelFixture()
    key=Fernet.generate_key()
    def run():
        return module.PanelExportJob(tmp_path/'state',tmp_path/'output','panel-example','request-1',
                                     '2026-11-10',key,lambda:panel).run()
    replace=module.os.replace
    stopped=False
    def stop_after_sql_rename(source,destination):
        nonlocal stopped
        replace(source,destination)
        if str(destination).endswith('.sql') and not stopped:
            stopped=True
            raise BackupError('SIMULATED_PROCESS_STOP')
    monkeypatch.setattr(module.os,'replace',stop_after_sql_rename)
    assert run()['status']=='FAILED'
    assert run()['status']=='DB_ONLY'
    assert panel.exports.count('private_database_one')==1


def test_complete_download_kept_after_failure_is_reused_without_network_copy(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    class Interrupted(PanelFixture):
        downloads=[]
        broken=True
        def download(self,scope,remote,destination,max_bytes):
            self.downloads.append(remote['path'])
            super().download(scope,remote,destination,max_bytes)
            if self.broken:
                self.broken=False
                raise BackupError('FILETRANSFER_CONNECTION_FAILED')
    panel=Interrupted()
    key=Fernet.generate_key()
    def run():
        return PanelExportJob(tmp_path/'state',tmp_path/'output','panel-example','request-1',
                              '2026-11-10',key,lambda:panel).run()
    assert run()['status']=='FAILED'
    partial=next((tmp_path/'output').rglob('*.partial-*'))
    assert run()['status']=='DB_ONLY'
    assert len(panel.downloads)==2
    assert partial.read_bytes()==SQL


def test_success_produces_db_only_verifies_every_dump_and_cleans_exact_files(tmp_path):
    panel = PanelFixture()
    result = job(tmp_path,panel).run()
    assert result['status'] == 'DB_ONLY'
    assert result['test_passed'] is True
    assert result['databases_count'] == 2
    assert len(panel.exports) == len(panel.deleted) == 2
    assert len(list((tmp_path/'output').rglob('*.sql'))) == 2
    assert panel.closed and not panel.files
    assert 'private_database' not in json.dumps(result)
    public = ''.join(p.read_text() for p in (tmp_path/'state').rglob('*.json'))
    assert 'private_database' not in public
    for path in (tmp_path/'state').rglob('*.jsonl'):
        assert 'private_database' not in path.read_text()


def test_reusing_job_does_not_submit_duplicate_exports(tmp_path):
    panel = PanelFixture()
    key = Fernet.generate_key()
    first = job(tmp_path,panel,key).run()
    count = len(panel.exports)
    second = job(tmp_path,panel,key).run()
    assert second['job_id'] == first['job_id']
    assert second['test_passed']
    assert len(panel.exports) == count


def test_lost_response_is_reconciled_without_resubmission(tmp_path):
    panel = PanelFixture()
    panel.interrupt_after_submit = True
    result = job(tmp_path,panel).run()
    assert result['status'] == 'DB_ONLY'
    assert panel.exports.count('private_database_one') == 1
    assert len(panel.deleted) == 2


def test_existing_foreign_export_is_never_overwritten_or_deleted(tmp_path):
    panel = PanelFixture()
    panel.files['private_database_one'] = {'path':'/backups/2026-11-10/private_database_one.sql', 'size_bytes':len(SQL)}
    result = job(tmp_path,panel).run()
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'REMOTE_DESTINATION_EXISTS'
    assert not panel.exports and not panel.deleted


def test_corrupt_sql_never_passes_and_owned_remote_is_cleaned(tmp_path):
    panel = PanelFixture()
    panel.download = lambda scope,remote,path,max_bytes: path.write_bytes(b'<html>failure</html>')
    result = job(tmp_path,panel).run()
    assert result['status'] == 'FAILED' and result['error_code'] == 'DB_INTEGRITY_FAILED'
    assert result['test_passed'] is False
    assert len(panel.deleted) == 1


def test_cleanup_failure_is_explicit(tmp_path):
    panel = PanelFixture()
    panel.cleanup_error = True
    result = job(tmp_path,panel).run()
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'CLEANUP_FAILED'
    assert not result['test_passed']


def test_binding_failure_does_not_export(tmp_path):
    panel = PanelFixture()
    inspect = panel.inspect
    panel.inspect = lambda date:{**inspect(date),'binding_verified':False}
    result = job(tmp_path,panel).run()
    assert result['error_code'] == 'PROFILE_UNVERIFIED'
    assert not panel.exports


def test_failed_validation_cannot_turn_into_success_on_retry(tmp_path):
    panel = PanelFixture()
    key = Fernet.generate_key()
    original = panel.download
    panel.download = lambda scope,remote,path,max_bytes:path.write_bytes(b'broken')
    first = job(tmp_path,panel,key).run()
    assert first['status']=='FAILED'
    panel.download = original
    second = job(tmp_path,panel,key).run()
    assert second['status']=='FAILED' and not second['test_passed']
    assert len(panel.exports)==1


def test_snapshot_parser_never_chooses_files_or_another_date():
    from jino_backup.panel import select_snapshot_row
    rows = [{'index':0,'label':'Копия от 10 ноября 2026','kinds':'Файлы'},
            {'index':1,'label':'Копия от 9 ноября 2026','kinds':'MySQL'}]
    with pytest.raises(BackupError,match='SNAPSHOT_NOT_FOUND'):
        select_snapshot_row(rows,'2026-11-10')
    rows.append({'index':2,'label':'Копия от 10 ноября 2026','kinds':'MySQL'})
    assert select_snapshot_row(rows,'2026-11-10')['index']==2
    rows.append({'index':3,'label':'Копия от 10 ноября 2026','kinds':'MySQL'})
    with pytest.raises(BackupError,match='SNAPSHOT_AMBIGUOUS'):
        select_snapshot_row(rows,'2026-11-10')


@pytest.mark.parametrize('text',[
    'SQL-дамп будет восстановлен в исходную базу',
    'SQL-дамп будет скопирован в /backups/../domains',
    'SQL-дамп будет скопирован в /backups/2025-11-10',
    'SQL-дамп будет скопирован в /backups/2026-11-10; затем импортирован в production',
])
def test_export_guard_rejects_unsafe_or_mismatched_destinations(text):
    from jino_backup.panel import verify_export_destination
    with pytest.raises(BackupError,match='EXPORT_DESTINATION_UNVERIFIED'):
        verify_export_destination(text,'2026-11-10')


def test_export_guard_accepts_only_explicit_scratch_folder():
    from jino_backup.panel import verify_export_destination
    text='Выберите дату и базу для восстановления. SQL-дамп будет скопирован в /backups/2026-11-10'
    assert verify_export_destination(text,'2026-11-10')=='/backups/2026-11-10'


def test_diagnostics_do_not_echo_private_upstream_details(tmp_path):
    panel=PanelFixture()
    def fail(date):
        raise BackupError('UI_CHANGED',ui_step='private_database',raw_secret='private-password')
    panel.inspect=fail
    result=job(tmp_path,panel).run()
    assert result['status']=='FAILED'
    assert 'private' not in json.dumps(result)
    assert 'private' not in ''.join(p.read_text() for p in (tmp_path/'state').rglob('*.jsonl'))


def test_browser_adapter_closes_its_modal_before_opening_another_tab():
    from jino_backup.panel import JinoPanelBrowser
    assert hasattr(JinoPanelBrowser,'_close_export_form')
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as pw:
        browser=pw.chromium.launch(channel='chrome',headless=True)
        try:
            page=browser.new_page()
            page.set_default_timeout(1000)
            page.set_content('''
                <a href="#files" onclick="window.opened=true">Доступ к файлам</a>
                <div class="backup-restore" style="position:fixed;inset:0;background:white;z-index:100">
                  <button aria-label="Закрыть" onclick="this.parentElement.remove()">X</button>
                  <div class="backup-restore__info">SQL-дамп будет скопирован в /backups/2026-11-10</div>
                </div>''')
            driver=JinoPanelBrowser({'username':'synthetic','password':'synthetic'})
            driver.page=page
            driver._close_export_form()
            page.get_by_role('link',name='Доступ к файлам').click()
            assert page.evaluate('window.opened') is True
        finally:
            browser.close()


def test_user_requested_retention_never_deletes_remote_dumps(tmp_path):
    from jino_backup.panel_export import PanelExportJob
    panel=PanelFixture()
    result=PanelExportJob(tmp_path/'state',tmp_path/'output','panel-example','request-1',
                          '2026-11-10',Fernet.generate_key(),lambda:panel,
                          cleanup_remote=False).run()
    assert result['status']=='DB_ONLY' and result['test_passed']
    assert len(panel.files)==2 and panel.deleted==[]
    assert result['cleanup_policy']=='KEEP_REMOTE'
    assert not result['cleanup_pending']
    assert result['remote_exports_retained']==2
