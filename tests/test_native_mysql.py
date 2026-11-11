"""Opt-in smoke test against an isolated native MySQL server, never a user database."""

import ipaddress
import os
import shutil
import socket
import ssl
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from jino_backup.database import MySqlDump, option
from jino_backup.importers import Candidate
from jino_backup.service import BackupService, unpack_archive
from test_backup import system


@pytest.mark.skipif(os.environ.get("JINO_RUN_NATIVE_MYSQL_TEST") != "1",
                    reason="Opt-in native MySQL integration test")
def test_native_mysql_tls_backup_and_sql_recovery(system, tmp_path):
    binaries = {name: shutil.which(name) for name in ("mysqld", "mysql", "mysqldump")}
    if not all(binaries.values()):
        pytest.skip("Native MySQL server and client binaries are required")
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(private.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(private, hashes.SHA256()))
    cert, key = tmp_path / "mysql-test-ca.pem", tmp_path / "mysql-test.key"
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    data = tmp_path / "isolated-mysql-data"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    base = [binaries["mysqld"], "--no-defaults", f"--datadir={data}"]
    process = None
    with (tmp_path / "isolated-mysql.log").open("w+b") as logfile:
        initialized = subprocess.run([*base, "--initialize-insecure", "--console"],
            stdout=logfile, stderr=subprocess.STDOUT, timeout=60, creationflags=flags)
        assert initialized.returncode == 0, "Isolated MySQL initialization failed"
        try:
            process = subprocess.Popen([*base, "--console", "--bind-address=127.0.0.1",
                f"--port={port}", "--mysqlx=0", "--skip-log-bin", "--innodb-buffer-pool-size=32M",
                f"--ssl-ca={cert}", f"--ssl-cert={cert}", f"--ssl-key={key}",
                "--require-secure-transport=ON"], stdout=logfile, stderr=subprocess.STDOUT,
                creationflags=flags)
            defaults = tmp_path / "isolated-client.cnf"
            defaults.write_text("[client]\nuser=root\npassword=\nprotocol=TCP\nhost=127.0.0.1\n"
                f"port={port}\nssl-mode=VERIFY_IDENTITY\nssl-ca={option(str(cert))}\n")
            environment = {k: v for k, v in os.environ.items() if not k.startswith("MYSQL_")}
            environment["MYSQL_TEST_LOGIN_FILE"] = str(tmp_path / "no-login.cnf")
            client = [binaries["mysql"], f"--defaults-file={defaults}"]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                ready = subprocess.run([*client, "--execute", "SELECT 1"],
                    capture_output=True, env=environment, timeout=3, creationflags=flags)
                if ready.returncode == 0:
                    break
                assert process.poll() is None, "Isolated MySQL exited before startup"
                time.sleep(0.2)
            else:
                pytest.fail("Isolated MySQL did not become ready with verified TLS")
            created = subprocess.run([*client, "--execute",
                "CREATE DATABASE synthetic_site; CREATE TABLE synthetic_site.example(id INT); "
                "INSERT INTO synthetic_site.example VALUES(42)"], capture_output=True,
                env=environment, timeout=5, creationflags=flags)
            assert created.returncode == 0
            settings, repo, source, _ = system
            repo.import_profiles([Candidate("mysql-local-test", "mysql", {
                "host": "127.0.0.1", "port": port, "username": "root", "password": ""})])
            repo.add_database("site-1", "db-local", "mysql-local-test", "synthetic_site")
            repo.confirm_databases("site-1")
            service = BackupService(settings, repo, source_factory=lambda c, s: source,
                db_exporter=MySqlDump((binaries["mysqldump"],), cert))
            job = service.run("site-1", "native-mysql-test")
            assert job["status"] == "COMPLETE", job["result"].get("database_errors")
            assert service.verify(job["job_id"])["verified"]
            archive = settings.backup_dir / job["result"]["artifact"]["storage_key"]
            unpack_archive(archive, tmp_path / "recovered-package", settings.read_key(), job["job_id"])
            sql = (tmp_path / "recovered-package/databases/db-local.sql").read_bytes()
            assert b"INSERT INTO `example`" in sql
            assert b"(42)" in sql
            dropped = subprocess.run([*client, "--execute", "DROP DATABASE synthetic_site"],
                capture_output=True, env=environment, timeout=5, creationflags=flags)
            assert dropped.returncode == 0
            with (tmp_path / "recovered-package/databases/db-local.sql").open("rb") as incoming:
                restored = subprocess.run(client, stdin=incoming, stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE, env=environment, timeout=10, creationflags=flags)
            assert restored.returncode == 0
            checked = subprocess.run([*client, "--batch", "--skip-column-names", "--execute",
                "SELECT id FROM synthetic_site.example"], capture_output=True,
                env=environment, timeout=5, creationflags=flags)
            assert checked.returncode == 0 and checked.stdout.strip() == b"42"
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
