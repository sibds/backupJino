"""Local server metadata with credentials in a separate encrypted SQLite vault."""

import json
import sqlite3
import uuid
from contextlib import contextmanager

from .errors import BackupError
from .importers import identity, is_hash, jino_rows, valid_endpoint
from .secrets import SQLiteSecrets
from .server_folders import component
from .state import Settings, private_dir, safe_root


class DashboardCatalog:
    def __init__(self, data):
        self.data = data
        self.path = data.state_dir / "dashboard.sqlite3"
        self.vault_path = data.state_dir / "dashboard.secrets.sqlite3"
        private_dir(data.state_dir)
        with self.connection() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS servers (
                row_id INTEGER PRIMARY KEY, metadata TEXT NOT NULL, secret_ref TEXT)""")
            for account, record in zip(data.accounts, jino_rows(data.source)):
                config = dict(account)
                config.update(self._source_config(record))
                config.update(
                    source_row=account["row"],
                    blocked=False,
                    deleted=False,
                    revision=1,
                    binding_revision=1,
                    legacy_folder=account["folder"],
                )
                db.execute(
                    "INSERT OR IGNORE INTO servers VALUES (?,?,NULL)",
                    (account["row"], json.dumps(config)),
                )

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def _source_config(self, record):
        mapping = self.data.mapping
        password = record.get(mapping.get("password", ""))
        ftp_password = record.get(mapping.get("ftp_password", ""))
        return {
            "panel_login": record.get(mapping.get("login", "")) or "",
            "ftp_host": record.get(mapping.get("ftp_host", "")) or "",
            "ftp_login": record.get(mapping.get("ftp_login", "")) or "",
            "protocol": mapping.get("ftp_protocol", "ftps"),
            "ftp_port": mapping.get("ftp_port", 21),
            "remote_root": "/",
            "known_hosts": "",
            "skip_unsupported_names": True,
            "ftp_connections": 3,
            "has_panel_password": isinstance(password, str)
            and bool(password)
            and not is_hash(password),
            "has_ftp_password": isinstance(ftp_password, str)
            and bool(ftp_password)
            and not is_hash(ftp_password),
        }

    def all(self, *, include_deleted=True):
        with self.connection() as db:
            records = [
                json.loads(row[0])
                for row in db.execute("SELECT metadata FROM servers ORDER BY row_id")
            ]
        for config in records:
            config.setdefault("skip_unsupported_names", True)
            config.setdefault("ftp_connections", 3)
            duplicate = bool(config["panel_login"]) and any(
                other["row"] != config["row"]
                and not other["deleted"]
                and other["panel_login"] == config["panel_login"]
                for other in records
            )
            config["issue"] = (
                "ACCOUNT_AMBIGUOUS"
                if duplicate
                else (
                    "CREDENTIAL_MISSING"
                    if not config["panel_login"] or not config["has_panel_password"]
                    else None
                )
            )
            config["can_check"] = config["issue"] is None and not config["blocked"]
            config.setdefault("binding_revision", config["revision"])
            endpoint = {
                "host": config["ftp_host"],
                "username": config["ftp_login"],
                "password": "present",
                "port": config["ftp_port"],
                "protocol": config["protocol"],
            }
            ftp_duplicate = bool(config["ftp_login"]) and any(
                other["row"] != config["row"]
                and not other["deleted"]
                and (other["ftp_host"], other["ftp_login"])
                == (config["ftp_host"], config["ftp_login"])
                for other in records
            )
            config["ftp_ready"] = (
                valid_endpoint(endpoint) and config["has_ftp_password"] and not ftp_duplicate
            )
            config["ftp_issue"] = (
                "ACCOUNT_AMBIGUOUS"
                if ftp_duplicate
                else (None if config["ftp_ready"] else "FTP_CREDENTIAL_MISSING")
            )
        return [record for record in records if include_deleted or not record["deleted"]]

    def get(self, row, *, include_deleted=False):
        if type(row) is not int:
            raise BackupError("SERVER_NOT_FOUND")
        for config in self.all(include_deleted=include_deleted):
            if config["row"] == row:
                return config
        raise BackupError("SERVER_NOT_FOUND")

    def credentials(self, row):
        config = self.get(row)
        with self.connection() as db:
            reference = db.execute(
                "SELECT secret_ref FROM servers WHERE row_id=?", (row,)
            ).fetchone()[0]
            if reference:
                vault = SQLiteSecrets(
                    self.vault_path, Settings.load(self.data.state_dir).read_key()
                )
                vault.attach(db)
                return vault.get(db, reference)
        original = jino_rows(self.data.source)[config["source_row"] - 1]
        mapping = self.data.mapping
        if identity("panel", original.get(mapping["id"])) != config["account_ref"]:
            raise BackupError("SOURCE_CHANGED")
        return {
            "panel": {
                "username": config["panel_login"],
                "password": original.get(mapping.get("password", "")),
            },
            "ftp": {
                "host": config["ftp_host"],
                "username": config["ftp_login"],
                "password": original.get(mapping.get("ftp_password", "")),
                "protocol": config["protocol"],
                "port": config["ftp_port"],
            },
        }

    def save(self, row, payload):
        if not isinstance(payload, dict):
            raise BackupError("REQUEST_INVALID")
        old = self.get(row) if row is not None else None
        credentials = self.credentials(row) if old else {"panel": {}, "ftp": {}}
        config = (
            dict(old)
            if old
            else {
                "source_row": None,
                "blocked": False,
                "deleted": False,
                "revision": 0,
                "protocol": "ftps",
                "ftp_port": 21,
                "remote_root": "/",
                "known_hosts": "",
                "ftp_host": "",
                "ftp_login": "",
                "panel_login": "",
                "name": "",
            }
        )
        for key in ("name", "ftp_host", "ftp_login", "protocol", "remote_root", "known_hosts"):
            if key in payload:
                if (
                    not isinstance(payload[key], str)
                    or len(payload[key]) > 512
                    or any(ord(c) < 32 for c in payload[key])
                ):
                    raise BackupError("SERVER_CONFIG_INVALID")
                config[key] = payload[key].strip()
        if "login" in payload:
            if not isinstance(payload["login"], str) or len(payload["login"]) > 160:
                raise BackupError("SERVER_CONFIG_INVALID")
            config["panel_login"] = payload["login"].strip()
        config["login"] = config["panel_login"] or config["ftp_login"]
        if not config["name"] or not config["login"] or config["protocol"] not in {"ftps", "sftp"}:
            raise BackupError("SERVER_CONFIG_INVALID")
        safe_root(config["remote_root"])
        port = payload.get("ftp_port", config["ftp_port"])
        if type(port) is not int or not 1 <= port <= 65535:
            raise BackupError("SERVER_CONFIG_INVALID")
        config["ftp_port"] = port
        if "skip_unsupported_names" in payload:
            if type(payload["skip_unsupported_names"]) is not bool:
                raise BackupError("SERVER_CONFIG_INVALID")
            config["skip_unsupported_names"] = payload["skip_unsupported_names"]
        config.setdefault("skip_unsupported_names", True)
        connections = payload.get("ftp_connections", config.get("ftp_connections", 3))
        if type(connections) is not int or not 2 <= connections <= 5:
            raise BackupError("CONNECTION_LIMIT_INVALID")
        config["ftp_connections"] = connections
        credentials["panel"]["username"] = config["panel_login"]
        credentials["ftp"].update(
            host=config["ftp_host"],
            username=config["ftp_login"],
            protocol=config["protocol"],
            port=port,
        )
        for field, kind in (("panel_password", "panel"), ("ftp_password", "ftp")):
            if field in payload and payload[field] != "":
                if (
                    not isinstance(payload[field], str)
                    or len(payload[field]) > 4096
                    or is_hash(payload[field])
                ):
                    raise BackupError("CREDENTIAL_NOT_RECOVERABLE")
                credentials[kind]["password"] = payload[field]
        config["has_panel_password"] = bool(credentials["panel"].get("password")) and not is_hash(
            credentials["panel"]["password"]
        )
        config["has_ftp_password"] = bool(credentials["ftp"].get("password")) and not is_hash(
            credentials["ftp"]["password"]
        )
        vault = SQLiteSecrets(self.vault_path, Settings.load(self.data.state_dir).read_key())
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if old:
                current = json.loads(
                    db.execute("SELECT metadata FROM servers WHERE row_id=?", (row,)).fetchone()[0]
                )
                if (
                    current["revision"] != old["revision"]
                    or payload.get("revision", old["revision"]) != old["revision"]
                ):
                    raise BackupError("SERVER_CONFIG_CHANGED")
            else:
                row = db.execute("SELECT COALESCE(MAX(row_id),0)+1 FROM servers").fetchone()[0]
                ref = "panel-" + uuid.uuid4().hex[:24]
                config.update(
                    row=row, account_ref=ref, server_id=ref.replace("panel-", "server-", 1)
                )
            folder = (
                old["folder"]
                if old and old["login"] == config["login"]
                else component(config["login"])
            )
            others = [
                json.loads(value[0])
                for value in db.execute("SELECT metadata FROM servers WHERE row_id!=?", (row,))
            ]
            if any(other["folder"].casefold() == folder.casefold() for other in others):
                folder += "--" + component(config["ftp_host"] or str(row))
            if any(other["folder"].casefold() == folder.casefold() for other in others):
                raise BackupError("FOLDER_NAME_AMBIGUOUS")
            config["folder"] = folder
            config.setdefault("legacy_folder", folder)
            binding_fields = (
                "panel_login",
                "ftp_host",
                "ftp_login",
                "protocol",
                "ftp_port",
                "remote_root",
                "known_hosts",
                "folder",
            )
            config["binding_revision"] = old["binding_revision"] if old else 1
            if old and any(config.get(key) != old.get(key) for key in binding_fields):
                config["binding_revision"] += 1
            config["revision"] += 1
            vault.attach(db)
            reference = vault.store(db, credentials)
            db.execute(
                "INSERT OR REPLACE INTO servers VALUES (?,?,?)",
                (row, json.dumps(config), reference),
            )
        return self.get(row)

    def set_flag(self, row, flag, value):
        if flag not in {"blocked", "deleted"} or type(value) is not bool:
            raise BackupError("REQUEST_INVALID")
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            record = db.execute("SELECT metadata FROM servers WHERE row_id=?", (row,)).fetchone()
            if record is None:
                raise BackupError("SERVER_NOT_FOUND")
            config = json.loads(record[0])
            config.setdefault("binding_revision", config["revision"])
            config[flag] = value
            config["revision"] += 1
            db.execute("UPDATE servers SET metadata=? WHERE row_id=?", (json.dumps(config), row))
        return config
