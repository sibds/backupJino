"""Local encrypted secret store, separate from registry metadata.

The attached SQLite database participates in the registry's rollback transaction.
This prototype provider can later be replaced with an external vault adapter.
"""

import json
import os
import uuid
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from .errors import BackupError


class SQLiteSecrets:
    def __init__(self, path, key):
        self.path = Path(path)
        self.cipher = Fernet(key)

    def attach(self, connection):
        connection.execute("ATTACH DATABASE ? AS vault", (str(self.path),))
        connection.execute("""CREATE TABLE IF NOT EXISTS vault.secrets (
            secret_ref TEXT PRIMARY KEY, payload BLOB NOT NULL)""")
        os.chmod(self.path, 0o600)

    def store(self, connection, credentials):
        secret_ref = "local-" + uuid.uuid4().hex
        payload = self.cipher.encrypt(json.dumps(credentials).encode())
        connection.execute("INSERT INTO vault.secrets VALUES (?,?)", (secret_ref, payload))
        return secret_ref

    def get(self, connection, secret_ref):
        row = connection.execute("SELECT payload FROM vault.secrets WHERE secret_ref=?",
                                 (secret_ref,)).fetchone()
        if row is None:
            raise BackupError("SECRET_UNAVAILABLE")
        try:
            return json.loads(self.cipher.decrypt(row["payload"]))
        except (InvalidToken, ValueError):
            raise BackupError("VAULT_INTEGRITY_FAILED") from None
