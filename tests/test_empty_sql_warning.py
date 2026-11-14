import pytest
from cryptography.fernet import Fernet
from jino_backup.errors import BackupError
from jino_backup.panel_protocol import validate_panel_sql
from jino_backup.panel_export import PanelExportJob
from test_panel_export import PanelFixture,SQL

SETTINGS=('CHARACTER_SET_CLIENT','CHARACTER_SET_RESULTS','COLLATION_CONNECTION','TIME_ZONE',
          'FOREIGN_KEY_CHECKS','SQL_MODE','SQL_NOTES')
EMPTY=('/*M!999999\\- enable the sandbox mode */\n'+
       '\n'.join(f'/*!40101 SET @OLD_{n}=@@{n} */;' for n in SETTINGS)+'\n'+
       '\n'.join(f'/*!40101 SET {n}=@OLD_{n} */;' for n in SETTINGS)+'\n').encode()


def test_complete_session_only_dump_returns_warning(tmp_path):
    p=tmp_path/'empty.sql';p.write_bytes(EMPTY)
    assert validate_panel_sql(p)==['SQL_NO_OBJECTS']


@pytest.mark.parametrize('value',[EMPTY[:-25],EMPTY+b'<html>error</html>',
    EMPTY.replace(b'/*!40101 SET SQL_NOTES=@OLD_SQL_NOTES */;',b'CREATE TABLE t (id int);\n/*!40101 SET SQL_NOTES=@OLD_SQL_NOTES */;'),
    EMPTY.replace(b'/*!40101 SET SQL_NOTES=@OLD_SQL_NOTES */;',b'-- ERROR 2006 source failed\n/*!40101 SET SQL_NOTES=@OLD_SQL_NOTES */;')])
def test_incomplete_or_nonempty_unknown_format_still_fails(tmp_path,value):
    p=tmp_path/'bad.sql';p.write_bytes(value)
    with pytest.raises(BackupError): validate_panel_sql(p)


def test_empty_dump_warns_and_next_database_is_exported(tmp_path):
    panel=PanelFixture();downloads=[];events=[]
    def download(scope,remote,path,limit):
        downloads.append(remote['path']);path.write_bytes(EMPTY if len(downloads)==1 else SQL)
    panel.download=download
    job=PanelExportJob(tmp_path/'state',tmp_path/'copies','panel-example','request','2026-11-10',
                       Fernet.generate_key(),lambda:panel,notify=events.append)
    result=job.run()
    assert result['status']=='DB_ONLY' and result['databases_count']==2
    assert result['warnings']==['SQL_NO_OBJECTS']
    assert len(panel.exports)==2 and len(downloads)==2
    assert any(e['stage']=='SQL_WARNING' for e in events)


def test_inventory_preserves_warning_and_clears_it_on_new_copy(tmp_path):
    from jino_backup.database_inventory import DatabaseInventory
    inventory=DatabaseInventory(tmp_path/'inventory.sqlite3',Fernet.generate_key())
    inventory.discover(1,1,'panel-example',['synthetic'])
    alias=inventory.list(1,1)[0]['db_alias']
    inventory.verified(1,1,alias,'2026-11-10','2026-11-10T00:00:00Z',950,warnings=['SQL_NO_OBJECTS'])
    assert inventory.list(1,1)[0]['warnings']==['SQL_NO_OBJECTS']
    inventory.verified(1,1,alias,'2026-11-11','2026-11-11T00:00:00Z',2000)
    assert inventory.list(1,1)[0]['warnings']==[]
