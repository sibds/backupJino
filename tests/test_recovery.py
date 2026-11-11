import threading

import pytest

from jino_backup.errors import BackupError


def test_five_reconnections_use_exponential_delays_and_then_succeed():
    from jino_backup.retry import retry_call
    calls, reconnects, delays, events = [], [], [], []
    def operation():
        calls.append(1)
        if len(calls) <= 5:
            raise BackupError("TRANSFER_INTERRUPTED")
        return "ok"
    assert retry_call(operation, reconnect=lambda: reconnects.append(1), wait=delays.append,
                      notify=lambda **event: events.append(event)) == "ok"
    assert len(calls) == 6 and len(reconnects) == 5
    assert delays == [2, 4, 8, 16, 32]
    assert [event["retry_attempt"] for event in events if event["stage"] == "RETRY_WAIT"] == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("code", ["TLS_CERTIFICATE_INVALID", "FTP_LOGIN_DENIED", "AUTH_FAILED", "REMOTE_PATH_UNSAFE", "CANCELLED"])
def test_permanent_errors_are_never_retried(code):
    from jino_backup.retry import retry_call
    waits = []
    def operation():
        raise BackupError(code)
    with pytest.raises(BackupError, match=code):
        retry_call(operation, wait=waits.append)
    assert waits == []


def test_exhausted_transfer_remains_resumable_at_committed_offset(tmp_path):
    from jino_backup.retry import RecoveringSource
    from jino_backup.downloads import DirectoryCheckpoint
    from jino_backup.transfer import collect_files
    from test_backup import FixtureSource
    directory = tmp_path / "remote"
    directory.mkdir()
    (directory / "a.txt").write_bytes(b"first-file")
    source = FixtureSource(directory)
    broken, offsets, delays = True, [], []
    def transfer(relative, outgoing, offset):
        offsets.append(offset)
        if broken:
            outgoing.write(b"first-file"[offset:offset+1])
            raise BackupError("TRANSFER_INTERRUPTED")
        outgoing.write(b"first-file"[offset:])
    source.download = transfer
    with RecoveringSource(source, wait=delays.append) as remote:
        with DirectoryCheckpoint(tmp_path / "copy", "synthetic-scope") as checkpoint:
            with pytest.raises(BackupError, match="TRANSFER_INTERRUPTED"):
                collect_files(remote, tmp_path / "copy/files", checkpoint=checkpoint,
                              max_bytes=100, max_files=10, timeout_seconds=10, continue_on_error=True)
            assert checkpoint.db.execute("SELECT committed FROM files").fetchone()[0] == 6
    assert offsets == [0, 1, 2, 3, 4, 5] and delays == [2, 4, 8, 16, 32]
    broken = False
    with RecoveringSource(source, wait=delays.append) as remote:
        with DirectoryCheckpoint(tmp_path / "copy", "synthetic-scope") as checkpoint:
            collect_files(remote, tmp_path / "copy/files", checkpoint=checkpoint,
                          max_bytes=100, max_files=10, timeout_seconds=10)
    assert offsets[-1] == 6
    assert (tmp_path / "copy/files/a.txt").read_bytes() == b"first-file"


def test_inventory_recovers_only_the_failed_directory(tmp_path):
    from jino_backup.retry import RecoveringSource
    from jino_backup.dashboard_runner import ControlledSource
    from jino_backup.transfer import Entry
    from jino_backup.dashboard_management import TaskControl
    class Source:
        def __init__(self):
            self.calls = []
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def listdir(self, parent):
            self.calls.append(parent)
            if parent == "": return [Entry("child", "dir", 0)]
            if self.calls.count("child") < 6: raise BackupError("TRANSFER_INTERRUPTED")
            return [Entry("a.txt", "file", 10)]
    source, waits = Source(), []
    with ControlledSource(RecoveringSource(source, wait=waits.append), TaskControl(lambda state: None),
                          lambda **values: None, tmp_path / "output") as controlled:
        assert controlled.inventory["child"][0].name == "a.txt"
    assert source.calls.count("") == 1 and source.calls.count("child") == 6
    assert waits == [2, 4, 8, 16, 32]


def test_stop_interrupts_retry_wait_without_waiting_for_the_deadline():
    from jino_backup.dashboard_management import TaskControl
    control = TaskControl(lambda state: None)
    entered, exited = threading.Event(), threading.Event()
    def wait():
        entered.set()
        try:
            control.wait(30)
        except BackupError as error:
            assert error.code == "CANCELLED"
            exited.set()
    thread = threading.Thread(target=wait)
    thread.start()
    assert entered.wait(1)
    control.request("stop")
    assert exited.wait(1)
    thread.join(timeout=1)


@pytest.mark.parametrize("certificate_error", [False, True])
def test_ftps_does_not_hide_the_failure_with_an_immediate_reconnect(monkeypatch, certificate_error):
    import io
    import ssl
    from jino_backup.transfer import FtpsSource
    class BrokenFTP:
        closed = False
        def retrbinary(self, *args, **kwargs):
            if certificate_error:
                raise ssl.SSLCertVerificationError(1, "synthetic certificate rejection")
            raise OSError("synthetic connection loss")
        def close(self): self.closed = True
    source = FtpsSource({}, {"remote_root":"/"})
    ftp = BrokenFTP()
    source.ftp = ftp
    source._cwd = lambda relative: None
    source.connect = lambda: pytest.fail("Recovery must happen after backoff, not inside the failed transfer")
    expected = "TLS_CERTIFICATE_INVALID" if certificate_error else "TRANSFER_INTERRUPTED"
    with pytest.raises(BackupError, match=expected):
        source.download("a.txt", io.BytesIO(), 0)
    assert ftp.closed


def test_fully_committed_partial_is_verified_without_downloading_again(tmp_path):
    from test_backup import FixtureSource
    from jino_backup.retry import RecoveringSource
    from jino_backup.downloads import DirectoryCheckpoint
    from jino_backup.transfer import collect_files
    root=tmp_path/'remote'
    root.mkdir()
    (root/'a.txt').write_bytes(b'complete')
    source=FixtureSource(root)
    calls=[]
    def download(relative,outgoing,offset):
        calls.append(offset)
        outgoing.write(b'complete')
        raise BackupError('TRANSFER_INTERRUPTED')
    source.download=download
    with RecoveringSource(source,wait=lambda seconds:None) as remote:
        with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
            result=collect_files(remote,tmp_path/'copy/files',checkpoint=checkpoint,max_bytes=100,max_files=10,timeout_seconds=10)
    assert calls==[0] and result['bytes']==8
