import ftplib
import pytest
from jino_backup.transfer import FtpsSource, Entry
from jino_backup.errors import BackupError


class MetadataFTP:
    def __init__(self, response):
        self.response, self.commands = response, []
    def cwd(self, path): self.commands.append('CWD')
    def pwd(self): return '/site'
    def sendcmd(self, command):
        self.commands.append(command.split(' ',1)[0])
        if isinstance(self.response, Exception): raise self.response
        return self.response
    def close(self): pass


def source(response):
    value=FtpsSource({}, {'remote_root':'/site'})
    value.canonical_root='/site'
    value.ftp=MetadataFTP(response)
    return value


def test_stat_uses_one_control_reply_instead_of_listing_the_directory():
    value=source('250-Listing\n type=file;size=12;modify=20261113010000; /site/example.txt\n250 End')
    value.listdir=lambda *args:pytest.fail('Full directory listing used for one file')
    assert value.stat('example.txt')==Entry('example.txt','file',12,'20261113010000')
    assert value.ftp.commands==['CWD','MLST']


def test_unsupported_mlst_falls_back_once_and_retains_strict_listing_checks():
    value=source(ftplib.error_perm('502 Synthetic unsupported'))
    calls=[]
    value.listdir=lambda path:calls.append(path) or [Entry('example.txt','file',12,'1')]
    assert value.stat('example.txt').size==12
    assert value.stat('example.txt').size==12
    assert value.ftp.commands.count('MLST')==1 and len(calls)==2


@pytest.mark.parametrize('reply',[
    '250-Listing\n type=file;size=12;modify=1; /outside/example.txt\n250 End',
    '250-Listing\n type=file;size=-1;modify=1; example.txt\n250 End',
    '250-Listing\n type=file;size=abc;modify=1; example.txt\n250 End',
])
def test_ambiguous_or_invalid_mlst_is_not_accepted(reply):
    with pytest.raises(BackupError): source(reply).stat('example.txt')


def test_mlst_still_verifies_parent_root_before_querying_file():
    value=source('250 unused')
    value.ftp.pwd=lambda:'/outside'
    with pytest.raises(BackupError,match='REMOTE_PATH_UNSAFE'):
        value.stat('example.txt')
    assert 'MLST' not in value.ftp.commands


def test_mlst_stat_over_real_local_tls_does_not_open_a_data_connection(tmp_path, monkeypatch):
    from test_ftps import ftps_server
    with ftps_server(tmp_path) as (credentials, certificate, files, state):
        monkeypatch.setattr('certifi.where', lambda:str(certificate))
        state['support_mlst'] = True
        with FtpsSource(credentials, {'remote_root':'/public_html'}) as value:
            for name, content in files.items():
                item=value.stat(name)
                assert item.size==len(content) and item.kind=='file'
            assert value.mlst_checks == len(files)
        assert state['mlst_calls'] == len(files)
        assert state['protected_data_connections'] == 0
        assert not state['errors']
