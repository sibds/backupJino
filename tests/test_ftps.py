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
    listener.listen(1)
    listener.settimeout(5)
    files = {".htaccess": b"synthetic-hidden-file", "index.php": b"synthetic-site-file"}
    state = {"protected_data_connections": 0, "tls_control": False, "errors": []}

    def serve():
        passive = reader = connection = None
        try:
            connection, _ = listener.accept()
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
                    reply('257 "/public_html"')
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
                        else:
                            payload = files[argument][offset:]
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

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    credentials = {"protocol": "ftps", "host": "127.0.0.1", "port": listener.getsockname()[1],
                   "username": "synthetic-user", "password": "synthetic-password"}
    try:
        yield credentials, cert, files, state
    finally:
        listener.close()
        thread.join(timeout=6)
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
