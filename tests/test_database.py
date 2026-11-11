import json
import sys
from pathlib import Path

import pytest

from jino_backup.database import MySqlDump, validate_sql
from jino_backup.errors import BackupError
from jino_backup.importers import Candidate, parse_jino
from jino_backup.service import BackupService
from test_backup import system

SQL = b"-- MySQL dump 10.13 Distrib 8.4\nCREATE TABLE `example` (`id` int);\n-- Dump completed on 2026-11-10 12:00:00\n"


def fake_client(tmp_path, body):
    script = tmp_path / "synthetic_client.py"
    script.write_text("import sys,time,pathlib,json,os\n" + body, encoding="utf-8")
    # Windows venv python.exe is a redirector spawning another interpreter.
    # Model a native mysqldump process using the actual interpreter executable.
    return (sys._base_executable, str(script))


def test_dump_uses_private_options_file_and_verified_tls(tmp_path):
    script = fake_client(tmp_path, """
options = next(a.split('=',1)[1] for a in sys.argv if a.startswith('--defaults-file='))
config = pathlib.Path(options).read_text()
assert 'password="synthetic-secret"' in config
assert 'ssl-mode=VERIFY_IDENTITY' in config
assert 'ssl-ca=' in config
assert 'synthetic-secret' not in ' '.join(sys.argv)
assert 'MYSQL_PWD' not in os.environ
assert '--no-login-paths' not in sys.argv
assert pathlib.Path(os.environ['MYSQL_TEST_LOGIN_FILE']).parent == pathlib.Path(options).parent
assert not pathlib.Path(os.environ['MYSQL_TEST_LOGIN_FILE']).exists()
sys.stdout.buffer.write(b'-- MySQL dump 10.13 Distrib 8.4\\nCREATE TABLE `example` (`id` int);\\n-- Dump completed on example\\n')
""")
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("synthetic-test-ca")
    exporter = MySqlDump(command=script, ca_file=ca_file)
    target = tmp_path / "database.sql"
    result = exporter.export({"host": "db.example.test", "port": 3306, "username": "alice",
                              "password": "synthetic-secret"}, "example", target,
                             timeout_seconds=5, max_bytes=10000)
    assert result["validation"] == "PASS"
    assert result["source_snapshot_at"]
    assert not list(tmp_path.glob("db-options-*"))
    validate_sql(target)


def test_failed_dump_does_not_leave_partial_sql_or_expose_stderr(tmp_path):
    script = fake_client(tmp_path, "sys.stdout.write('partial'); sys.stderr.write('synthetic-secret'); sys.exit(1)")
    ca_file = tmp_path / "ca.pem"
    ca_file.touch()
    target = tmp_path / "database.sql"
    with pytest.raises(BackupError, match="DB_EXPORT_FAILED") as caught:
        MySqlDump(script, ca_file).export({"host": "db.example.test", "port": 3306,
            "username": "alice", "password": "synthetic-secret"}, "example", target,
            timeout_seconds=5, max_bytes=10000)
    assert "synthetic-secret" not in str(caught.value)
    assert not target.exists()
    assert not list(tmp_path.glob("db-options-*"))


def test_dump_timeout_is_bounded_and_cleanup_occurs(tmp_path):
    script = fake_client(tmp_path, "time.sleep(10)")
    ca_file = tmp_path / "ca.pem"
    ca_file.touch()
    with pytest.raises(BackupError, match="DB_EXPORT_TIMEOUT"):
        MySqlDump(script, ca_file).export({"host": "db.example.test", "port": 3306,
            "username": "alice", "password": "synthetic-secret"}, "example", tmp_path / "dump.sql",
            timeout_seconds=0.1, max_bytes=10000)
    assert not (tmp_path / "dump.sql").exists()


@pytest.mark.parametrize("content", [b"", b"<html>Error</html>", b"-- MySQL dump\nCREATE TABLE x(id int);"])
def test_invalid_sql_is_rejected(tmp_path, content):
    path = tmp_path / "invalid.sql"
    path.write_bytes(content)
    with pytest.raises(BackupError, match="DB_INTEGRITY_FAILED"):
        validate_sql(path)


def bind_database(repo, db_id="db-1"):
    repo.import_profiles([Candidate("mysql-example", "mysql", {"host": "db.example.test",
        "port": 3306, "username": "synthetic-user", "password": "synthetic-db-secret"})])
    repo.add_database("site-1", db_id, "mysql-example", "private_database_name")


class FixtureDump:
    def export(self, credentials, name, destination, **kwargs):
        destination.write_bytes(SQL)
        return {"source_snapshot_at": "2026-11-10T09:00:00+00:00", "validation": "PASS"}


def test_complete_requires_explicit_database_inventory_confirmation(system):
    settings, repo, source, _ = system
    bind_database(repo)
    service = BackupService(settings, repo, source_factory=lambda c, s: source, db_exporter=FixtureDump())
    first = service.run("site-1", "unconfirmed-scope")
    assert first["status"] == "FILES_ONLY"
    assert "DB_SCOPE_UNVERIFIED" in first["result"]["warnings"]
    repo.confirm_databases("site-1")
    job = service.run("site-1", "confirmed-scope")
    assert job["status"] == "COMPLETE"
    assert job["result"]["databases_count"] == 1
    assert service.verify(job["job_id"])["verified"]
    assert "synthetic-db-secret" not in json.dumps(job)
    assert "private_database_name" not in json.dumps(job)


def test_one_missing_database_never_gives_complete(system):
    settings, repo, source, _ = system
    bind_database(repo, "db-1")
    bind_database(repo, "db-2")
    repo.confirm_databases("site-1")
    class PartialDump(FixtureDump):
        def export(self, credentials, name, destination, **kwargs):
            if destination.name == "db-2.sql":
                raise BackupError("DB_UNAVAILABLE")
            return super().export(credentials, name, destination, **kwargs)
    service = BackupService(settings, repo, source_factory=lambda c, s: source, db_exporter=PartialDump())
    job = service.run("site-1", "partial-databases")
    assert job["status"] == "FILES_ONLY"
    assert job["result"]["databases_count"] == 1
    assert job["result"]["database_errors"] == [{"db_id": "db-2", "error_code": "DB_UNAVAILABLE"}]


def test_binding_another_database_invalidates_inventory_confirmation(system):
    settings, repo, source, _ = system
    bind_database(repo, "db-1")
    repo.confirm_databases("site-1")
    bind_database(repo, "db-2")
    assert not repo.site("site-1")["database_scope_confirmed"]


def test_mysql_profiles_import_separately_and_never_guess_database_name(tmp_path):
    path = tmp_path / "rows.json"
    path.write_text(json.dumps([{"id": "1", "login": "panel-user", "password": "panel-password",
        "mysql_host": "db.example.test", "mysql_login": "db-user", "mysql_password": "db-password"}]))
    batch = parse_jino(path, {"id": "id", "login": "login", "password": "password",
        "mysql_host": "mysql_host", "mysql_login": "mysql_login", "mysql_password": "mysql_password"})
    db = next(c for c in batch.candidates if c.kind == "mysql")
    assert db.credentials["password"] == "db-password"
    assert "database" not in db.credentials
