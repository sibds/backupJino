"""Real loopback FTPS control and encrypted data channels, using synthetic files only."""

import contextlib
import ipaddress
import socket
import ssl
import threading
from datetime import datetime, timedelta, timezone

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from jino_backup.errors import BackupError
from jino_backup.transfer import FtpsSource, collect_files


@contextlib.contextmanager
def ftps_server(tmp_path, *, include_ip=True):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(private.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")] +
            ([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))] if include_ip else [])), critical=False)
        .sign(private, hashes.SHA256()))
    cert, key = tmp_path / "server.pem", tmp_path / "server.key"
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(20)
    listener.settimeout(0.2)
    files = {".htaccess": b"synthetic-hidden-file", "index.php": b"synthetic-site-file"}
    state = {"protected_data_connections": 0, "tls_control": False, "errors": [],
             "active_sessions": 0, "peak_sessions": 0}
    mutex, finished, workers = threading.Lock(), threading.Event(), []

    def serve(connection):
        passive = reader = None
        with mutex:
            state["active_sessions"] += 1
            state["peak_sessions"] = max(state["peak_sessions"], state["active_sessions"])
        try:
            connection.settimeout(5)
            reader = connection.makefile("rb")
            connection.sendall(b"220 Synthetic test server\r\n")
            protected = False
            offset = 0
            while line := reader.readline():
                command, _, argument = line.decode().rstrip("\r\n").partition(" ")
                def reply(value):
                    connection.sendall((value + "\r\n").encode())
                if command == "AUTH":
                    reply("234 Proceed with TLS")
                    reader.close()
                    connection = context.wrap_socket(connection, server_side=True)
                    reader = connection.makefile("rb")
                    state["tls_control"] = True
                elif command == "USER":
                    reply("331 Password required")
                elif command == "PASS":
                    reply("230 Logged in")
                elif command == "PBSZ":
                    reply("200 Buffer set")
                elif command == "PROT":
                    protected = argument == "P"
                    reply("200 Protection set")
                elif command == "CWD":
                    reply("250 Directory changed" if argument.rstrip("/") == "/public_html" else "550 Denied")
                elif command == "PWD":
                    reply('257 "' + state.get("reported_directory", "/public_html") + '"')
                elif command == "SYST":
                    reply("215 UNIX Type: L8")
                elif command == "OPTS":
                    reply("200 Option accepted")
                elif command == "SIZE":
                    filename = argument.removeprefix("/public_html/")
                    reply("213 " + str(len(files[filename])))
                elif command == "TYPE":
                    reply("200 Type set")
                elif command == "PASV":
                    passive = socket.socket()
                    passive.bind(("127.0.0.1", 0))
                    passive.listen(1)
                    passive.settimeout(5)
                    port = passive.getsockname()[1]
                    reply(f"227 Entering Passive Mode (127,0,0,1,{port // 256},{port % 256})")
                elif command == "REST":
                    offset = int(argument)
                    reply("350 Restart accepted")
                elif command in {"MLSD", "RETR"}:
                    assert protected, "Test server rejects unencrypted data"
                    if command == "RETR" and state.get("retr_reply"):
                        reply(str(state["retr_reply"]) + " Synthetic temporary refusal")
                        continue
                    if command == "RETR" and state.get("transfer_barrier"):
                        state["transfer_barrier"].wait(timeout=5)
                    reply("150 Opening data connection")
                    data, _ = passive.accept()
                    passive.close()
                    passive = None
                    data.settimeout(5)
                    with context.wrap_socket(data, server_side=True) as secured:
                        state["protected_data_connections"] += 1
                        if command == "MLSD":
                            payload = b"".join(f"type=file;size={len(content)};modify=20261110090000; {name}\r\n".encode()
                                               for name, content in files.items())
                            payload += state.get("extra_listing", b"")
                        else:
                            payload = files[argument.removeprefix("/public_html/")][offset:]
                            offset = 0
                        secured.sendall(payload)
                        secured.unwrap().close()
                    reply("226 Transfer complete")
                else:
                    reply("502 Unsupported command")
        except ssl.SSLError:
            # Expected when the negative test refuses our self-signed certificate.
            pass
        except Exception as exc:
            state["errors"].append(type(exc).__name__)
        finally:
            if reader:
                reader.close()
            if connection:
                connection.close()
            if passive:
                passive.close()
            with mutex:
                state["active_sessions"] -= 1

    def accept():
        while not finished.is_set():
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            worker = threading.Thread(target=serve, args=(connection,), daemon=True)
            workers.append(worker)
            worker.start()

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    credentials = {"protocol": "ftps", "host": "127.0.0.1", "port": listener.getsockname()[1],
                   "username": "synthetic-user", "password": "synthetic-password"}
    try:
        yield credentials, cert, files, state
    finally:
        finished.set()
        listener.close()
        thread.join(timeout=6)
        for worker in workers:
            worker.join(timeout=6)
            assert not worker.is_alive()
        assert not thread.is_alive()
        assert not state["errors"]


def test_real_ftps_copies_hidden_files_over_tls(tmp_path, monkeypatch):
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        with FtpsSource(credentials, {"remote_root": "/public_html"}) as source:
            assert source.ftp.context.check_hostname
            assert source.ftp.context.verify_mode == ssl.CERT_REQUIRED
            capture = collect_files(source, tmp_path / "captured", max_bytes=10000,
                                    max_files=10, timeout_seconds=20)
        assert capture["count"] == 2
        for name, content in files.items():
            assert (tmp_path / "captured" / name).read_bytes() == content
        assert state["tls_control"]
        assert state["protected_data_connections"] >= 3


def test_real_ftps_rejects_untrusted_certificate(tmp_path):
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        with pytest.raises(BackupError, match="TLS_CERTIFICATE_INVALID"):
            with FtpsSource(credentials, {"remote_root": "/public_html"}):
                pytest.fail("Untrusted certificate was accepted")
        assert state["protected_data_connections"] == 0


def test_real_ftps_rejects_hostname_mismatch_with_trusted_ca(tmp_path, monkeypatch):
    with ftps_server(tmp_path, include_ip=False) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        with pytest.raises(BackupError, match="TLS_CERTIFICATE_INVALID"):
            with FtpsSource(credentials, {"remote_root": "/public_html"}):
                pytest.fail("Hostname mismatch was accepted")
        assert state["protected_data_connections"] == 0


def test_pycurl_ftps_verifies_tls_and_resumes_with_rest(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            result = io.BytesIO()
            source.download("index.php", result, 3)
        assert result.getvalue() == files["index.php"][3:]
        assert state["tls_control"] and state["protected_data_connections"] == 1


def test_pycurl_ftps_rejects_untrusted_certificate(tmp_path):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            with pytest.raises(BackupError, match="TLS_CERTIFICATE_INVALID"):
                source.download("index.php", io.BytesIO(), 0)
        assert state["protected_data_connections"] == 0


def test_pycurl_ftps_rejects_hostname_mismatch_with_trusted_ca(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path, include_ip=False) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            with pytest.raises(BackupError, match="TLS_CERTIFICATE_INVALID"):
                source.download("index.php", io.BytesIO(), 0)
        assert state["protected_data_connections"] == 0


def test_pycurl_ftps_rejects_directory_escape_before_file_transfer(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        state["reported_directory"] = "/outside"
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            with pytest.raises(BackupError, match="REMOTE_PATH_UNSAFE"):
                source.download("index.php", io.BytesIO(), 0)
        assert state["protected_data_connections"] == 0


def test_real_parallel_ftps_streams_two_files_and_respects_three_session_limit(tmp_path, monkeypatch):
    from jino_backup.parallel import ParallelCollector, ConnectionBudget
    from jino_backup.dashboard_management import TaskControl
    from jino_backup.downloads import DirectoryCheckpoint
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        state["transfer_barrier"] = threading.Barrier(2)
        budget = ConnectionBudget(10)
        collector = ParallelCollector(TaskControl(lambda state: None), lambda **event: None,
                                      budget, connections=3)
        with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
            result = collector.collect(credentials, {"remote_root": "/public_html"},
                                       tmp_path / "copy", checkpoint)
        assert result["count"] == 2
        assert state["peak_sessions"] == 3
        assert budget.snapshot()["used"] == 0
        for name, data in files.items():
            assert (tmp_path / "copy/files" / name).read_bytes() == data


def test_pycurl_ftps_preserves_utf8_filenames_and_spaces(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        filename = "файл с пробелами.txt"
        files[filename] = b"utf8-name-content"
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            result = io.BytesIO()
            source.download(filename, result, 0)
        assert result.getvalue() == files[filename]


def test_pycurl_ftps_treats_temporary_ftp_refusal_as_recoverable(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from jino_backup.curl_transfer import CurlFileSource
    from jino_backup.dashboard_management import TaskControl
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        state["retr_reply"] = 450
        with CurlFileSource(credentials, {}, SimpleNamespace(canonical_root="/public_html"),
                            TaskControl(lambda state: None)) as source:
            with pytest.raises(BackupError, match="TRANSFER_INTERRUPTED") as caught:
                source.download("index.php", io.BytesIO(), 0)
            assert caught.value.details["ftp_reply_code"] == 450


def test_invalid_utf8_name_is_an_explicit_skip_without_losing_valid_files(tmp_path, monkeypatch):
    from jino_backup.parallel import ParallelCollector, ConnectionBudget
    from jino_backup.dashboard_management import TaskControl
    from jino_backup.downloads import DirectoryCheckpoint
    import json
    events = []
    with ftps_server(tmp_path) as (credentials, cert, files, state):
        monkeypatch.setattr("certifi.where", lambda: str(cert))
        state["extra_listing"] = b"type=file;size=1;modify=20261110090000; bad_\xff.txt\r\n"
        collector = ParallelCollector(TaskControl(lambda state: None), lambda **event: events.append(event),
            ConnectionBudget(10), connections=3, skip_unsupported_names=True)
        with DirectoryCheckpoint(tmp_path / "copy", "scope") as checkpoint:
            result = collector.collect(credentials, {"remote_root": "/public_html"}, tmp_path / "copy", checkpoint)
        assert result["count"] == len(files)
        assert result["skipped"][0]["reason"] == "INVALID_UTF8_NAME"
        assert "bad_" in result["skipped"][0]["path"]
        json.dumps(events, ensure_ascii=False).encode("utf-8")
        json.dumps(result, ensure_ascii=False).encode("utf-8")
