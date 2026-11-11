import pytest

from jino_backup.errors import BackupError


def test_export_mutation_guard_checks_every_argument_and_single_operation():
    from jino_backup.panel_protocol import export_payload_allowed, RESTORE_QUERY
    variables={'name':'snapshot','serverId':'server','db':'synthetic_db','archive':False,'accountId':'account'}
    payload={'query':RESTORE_QUERY,'variables':variables}
    assert export_payload_allowed(payload,variables)
    assert not export_payload_allowed({**payload,'variables':{**variables,'db':'other'}},variables)
    assert not export_payload_allowed({**payload,'query':RESTORE_QUERY+' mutation Delete { delete }'},variables)
    assert not export_payload_allowed({**payload,'variables':{**variables,'archive':True}},variables)
    assert not export_payload_allowed([payload],variables)


def test_new_export_rejects_foreign_files_and_links():
    from jino_backup.panel_protocol import check_owned_listing, new_export
    own={'path':'/returned/first.sql','name':'one.sql','type':'file','size':10}
    foreign={'path':'/returned/foreign.sql','name':'two.sql','type':'file','size':20}
    check_owned_listing([own],{own['path']})
    with pytest.raises(BackupError):
        check_owned_listing([own,foreign],{own['path']})
    assert new_export([own,foreign],[own['path']])==foreign
    with pytest.raises(BackupError):
        new_export([{**foreign,'targetType':'file'}],[])
    with pytest.raises(BackupError):
        new_export([own,foreign],[])


def test_existing_export_recovery_uses_saved_baseline():
    from jino_backup.panel_protocol import new_export
    assert new_export([],[]) is None
    assert new_export([{'path':'old','type':'file'}],['old']) is None


def test_export_waits_for_sql_instead_of_accepting_temporary_provider_file():
    from jino_backup.panel_protocol import new_export
    temporary={'name':'synthetic.sql.tmp','path':'/backup/temp','type':'file','size':200}
    assert new_export([temporary],[]) is None
    empty={'name':'synthetic.sql','path':'/backup/final','type':'file','size':0}
    assert new_export([empty],[]) is None
    final={**empty,'size':200}
    assert new_export([final],[])==final


@pytest.mark.parametrize('size',[42,42.0,'42'])
def test_api_size_is_normalized_without_rounding(size):
    from jino_backup.panel_files import normalize_file_size
    assert normalize_file_size(size)==42


@pytest.mark.parametrize('size',[True,-1,1.2,'1.2',float('inf'),None])
def test_invalid_api_size_is_rejected(size):
    from jino_backup.panel_files import normalize_file_size
    with pytest.raises(BackupError):
        normalize_file_size(size)


def test_commentless_provider_dump_requires_all_saved_and_restored_settings(tmp_path):
    from jino_backup.panel_protocol import validate_panel_sql
    names=('CHARACTER_SET_CLIENT','CHARACTER_SET_RESULTS','COLLATION_CONNECTION','TIME_ZONE',
           'UNIQUE_CHECKS','FOREIGN_KEY_CHECKS','SQL_MODE','SQL_NOTES')
    header='\n'.join('/*!40101 SET @OLD_'+n+'=@@'+n+' */;' for n in names)
    footer='\n'.join('/*!40101 SET '+n+'=@OLD_'+n+' */;' for n in names)
    content=('\n'+header+'\nCREATE TABLE synthetic (id int);\n'+footer+'\n').encode()
    path=tmp_path/'dump.sql'
    path.write_bytes(content)
    validate_panel_sql(path)
    path.write_bytes(content[:-20])
    with pytest.raises(BackupError):
        validate_panel_sql(path)
    path.write_bytes(content.replace(b'TIME_ZONE=@OLD_TIME_ZONE',b'UNVERIFIED=1'))
    with pytest.raises(BackupError):
        validate_panel_sql(path)
