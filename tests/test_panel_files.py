import io

import pytest

from jino_backup.errors import BackupError


@pytest.mark.parametrize('url',[
    'http://files.jino.ru','https://evil.example','https://user:secret@files.jino.ru',
    'https://files.jino.ru/?token=secret','https://files.jino.ru/#secret',
])
def test_filetransfer_origin_is_strict(url):
    from jino_backup.panel_files import validate_transfer_origin
    with pytest.raises(BackupError,match='FILETRANSFER_ORIGIN_UNVERIFIED'):
        validate_transfer_origin(url)


def test_filetransfer_origin_accepts_verified_provider_https_url():
    from jino_backup.panel_files import validate_transfer_origin
    assert validate_transfer_origin('https://files.jino.ru/transfer')=='https://files.jino.ru/transfer'


def test_bounded_copy_does_not_accept_more_than_declared_limit(tmp_path):
    from jino_backup.panel_files import copy_response
    target=tmp_path/'dump.sql'
    with pytest.raises(BackupError,match='DB_EXPORT_LIMIT_EXCEEDED'):
        copy_response(io.BytesIO(b'x'*11),target,10)
    assert target.exists()  # Current instruction: retain incomplete test artifacts.
    assert target.stat().st_size<=10


def test_export_directory_resolution_uses_returned_tokens_and_rejects_links():
    from jino_backup.panel_files import resolve_export_directory
    observed=[]
    listing={
        'ROOT':[{'name':'backups','type':'dir','path':'OPAQUE_BACKUPS','targetType':None}],
        'OPAQUE_BACKUPS':[{'name':'2026-11-10','type':'dir','path':'OPAQUE_DATE','targetType':None}],
    }
    def read(token):
        observed.append(token)
        return listing[token]
    assert resolve_export_directory(read,'ROOT','2026-11-10')=='OPAQUE_DATE'
    assert observed==['ROOT','OPAQUE_BACKUPS']
    listing['ROOT'][0]['targetType']='dir'
    with pytest.raises(BackupError,match='REMOTE_PATH_UNSAFE'):
        resolve_export_directory(read,'ROOT','2026-11-10')


def test_listing_parser_rejects_errors_and_impossible_pagination():
    from jino_backup.panel_files import listing_page
    with pytest.raises(BackupError,match='PANEL_RESPONSE_FAILED'):
        listing_page({'errors':[{'message':'private-secret'}]})
    with pytest.raises(BackupError,match='PANEL_RESPONSE_INVALID'):
        listing_page({'data':{'me':{'filemanager':{'files':{'list':[{'name':'x'}],'total':{'items':0}}}}}})
