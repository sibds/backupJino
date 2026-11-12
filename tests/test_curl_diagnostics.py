from types import SimpleNamespace

import pycurl
import pytest

from jino_backup.curl_transfer import CurlFileSource
from jino_backup.errors import BackupError


@pytest.mark.parametrize('number,message,kind', [
    (28, 'Operation too slow NEVER-EXPOSE', 'LOW_SPEED_TIMEOUT'),
    (56, 'Recv failure: Connection was reset NEVER-EXPOSE', 'CONNECTION_RESET'),
])
def test_curl_failure_retains_safe_code_and_kind_without_upstream_message(number, message, kind):
    class Curl:
        def reset(self): pass
        def setopt(self, *args): pass
        def perform(self): raise pycurl.error(number, message)
    source = CurlFileSource({'host':'ftp.example.test','port':21,'username':'synthetic',
        'password':'NEVER-EXPOSE'}, {}, SimpleNamespace(canonical_root='/'), None)
    source.handle = Curl()
    with pytest.raises(BackupError) as caught:
        source.download('file.bin', None, 0)
    assert caught.value.details['curl_error_code'] == number
    assert caught.value.details['curl_error_kind'] == kind
    assert 'NEVER-EXPOSE' not in str(caught.value.details)
