"""Fresh logical MySQL exports using a TLS-verifying MySQL 8.x client."""

import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from .errors import BackupError
from .importers import valid_endpoint


def validate_sql_stream(stream, size):
    head = stream.read(min(4096, size))
    stream.seek(max(0, size - 4096))
    tail = stream.read(4096)
    if not head.startswith(b"-- MySQL dump ") or b"-- Dump completed on " not in tail:
        raise BackupError("DB_INTEGRITY_FAILED")


def validate_sql(path):
    with Path(path).open("rb") as stream:
        validate_sql_stream(stream, os.fstat(stream.fileno()).st_size)


def option(value):
    if not isinstance(value, str) or "\x00" in value:
        raise BackupError("DB_CREDENTIAL_INVALID")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    escaped = escaped.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return '"' + escaped + '"'


class MySqlDump:
    def __init__(self, command=("mysqldump",), ca_file=None):
        self.command = tuple(command)
        self.ca_file = Path(ca_file) if ca_file else None

    def export(self, credentials, database_name, destination, *, timeout_seconds, max_bytes):
        if not self.ca_file or not self.ca_file.is_file():
            raise BackupError("MYSQL_CA_REQUIRED")
        if (not valid_endpoint({**credentials, "protocol": "ftps"})
                or not isinstance(credentials.get("password"), str)):
            raise BackupError("DB_CREDENTIAL_INVALID")
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_$-]{0,63}", database_name):
            raise BackupError("DATABASE_NAME_INVALID")
        destination = Path(destination)
        created = False
        process = None
        started = datetime.now(timezone.utc).isoformat()
        deadline = time.monotonic() + timeout_seconds
        try:
            with tempfile.TemporaryDirectory(prefix="db-options-", dir=destination.parent) as temporary:
                defaults = Path(temporary) / "client.cnf"
                with defaults.open("x", encoding="utf-8") as options:
                    os.chmod(defaults, 0o600)
                    options.write("[client]\nprotocol=TCP\nssl-mode=VERIFY_IDENTITY\n")
                    for key, value in {"host": credentials["host"], "user": credentials["username"],
                                       "password": credentials["password"], "port": str(credentials["port"]),
                                       "ssl-ca": str(self.ca_file.resolve())}.items():
                        options.write(f"{key}={option(value)}\n")
                args = [*self.command, f"--defaults-file={defaults}",
                        "--single-transaction", "--quick", "--no-tablespaces", "--set-gtid-purged=OFF",
                        "--routines", "--events", "--triggers", "--hex-blob", "--comments", "--dump-date",
                        "--databases", database_name]
                environment = {k: v for k, v in os.environ.items() if not k.startswith("MYSQL_")}
                # Supported by older 8.0 clients too, unlike --no-login-paths.
                # The private, nonexistent login file prevents inherited .mylogin.cnf overrides.
                environment["MYSQL_TEST_LOGIN_FILE"] = str((Path(temporary) / "unused-login.cnf").resolve())
                with destination.open("xb") as output, (Path(temporary) / "stderr").open("w+b") as errors:
                    created = True
                    os.chmod(destination, 0o600)
                    try:
                        process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=output,
                                                   stderr=errors, env=environment,
                                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                    except FileNotFoundError:
                        raise BackupError("DB_CLIENT_UNAVAILABLE") from None
                    try:
                        while process.poll() is None:
                            if time.monotonic() >= deadline:
                                raise BackupError("DB_EXPORT_TIMEOUT")
                            if os.fstat(output.fileno()).st_size > max_bytes:
                                raise BackupError("DB_EXPORT_LIMIT_EXCEEDED")
                            try:
                                process.wait(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
                            except subprocess.TimeoutExpired:
                                pass
                        if process.returncode:
                            raise BackupError("DB_EXPORT_FAILED")
                    finally:
                        if process.poll() is None:
                            process.kill()
                        process.wait()
                if destination.stat().st_size > max_bytes:
                    raise BackupError("DB_EXPORT_LIMIT_EXCEEDED")
                validate_sql(destination)
            return {"source_snapshot_at": started, "export_finished_at": datetime.now(timezone.utc).isoformat(),
                    "validation": "PASS", "capture_consistency": "BEST_EFFORT"}
        except BaseException:
            if created:
                destination.unlink(missing_ok=True)
            raise
