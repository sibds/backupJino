"""Streaming authenticated archives and a create-only local filesystem backend."""

import base64
import hashlib
import os
import shutil
import uuid
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .errors import BackupError
from .state import private_dir, safe_id

CHUNK = 1024 * 1024
MAGIC = b"JBM1"


def archive_key(master):
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None,
                info=b"jino-backup/archive/v1").derive(base64.urlsafe_b64decode(master))


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def encrypt_file(source, destination, key, context):
    source, destination = Path(source), Path(destination)
    if source.stat().st_size > 64 * 1024**3 - 1024:
        raise BackupError("ARCHIVE_TOO_LARGE")
    header = MAGIC + os.urandom(12)
    cipher = Cipher(algorithms.AES(archive_key(key)), modes.GCM(header[4:])).encryptor()
    cipher.authenticate_additional_data(header + context.encode())
    created = False
    try:
        with source.open("rb") as incoming, destination.open("xb") as outgoing:
            created = True
            os.chmod(destination, 0o600)
            outgoing.write(header)
            for chunk in iter(lambda: incoming.read(CHUNK), b""):
                outgoing.write(cipher.update(chunk))
            outgoing.write(cipher.finalize())
            outgoing.write(cipher.tag)
            outgoing.flush()
            os.fsync(outgoing.fileno())
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise


def decrypt_file(source, destination, key, context):
    source, destination = Path(source), Path(destination)
    temporary = destination.with_name(destination.name + ".partial-" + uuid.uuid4().hex)
    try:
        with source.open("rb") as incoming:
            length = os.fstat(incoming.fileno()).st_size
            header = incoming.read(16)
            if length < 32 or header[:4] != MAGIC:
                raise BackupError("INTEGRITY_FAILED")
            incoming.seek(-16, 2)
            tag = incoming.read(16)
            incoming.seek(16)
            cipher = Cipher(algorithms.AES(archive_key(key)), modes.GCM(header[4:], tag)).decryptor()
            cipher.authenticate_additional_data(header + context.encode())
            remaining = length - 32
            with temporary.open("xb") as outgoing:
                os.chmod(temporary, 0o600)
                while remaining:
                    chunk = incoming.read(min(CHUNK, remaining))
                    if not chunk:
                        raise BackupError("INTEGRITY_FAILED")
                    remaining -= len(chunk)
                    outgoing.write(cipher.update(chunk))
                outgoing.write(cipher.finalize())
            # Plaintext is published only after authentication succeeds. Never overwrite files.
            with temporary.open("rb") as incoming, destination.open("xb") as outgoing:
                os.chmod(destination, 0o600)
                shutil.copyfileobj(incoming, outgoing, CHUNK)
    except (InvalidTag, ValueError):
        raise BackupError("INTEGRITY_FAILED") from None
    finally:
        temporary.unlink(missing_ok=True)


class LocalStorage:
    def __init__(self, root):
        self.root = Path(root).resolve()
        private_dir(self.root)

    def resolve(self, storage_key):
        path = (self.root / storage_key).resolve()
        if not path.is_relative_to(self.root) or path == self.root:
            raise BackupError("STORAGE_PATH_UNSAFE")
        return path

    def put(self, site_id, job_id, source):
        safe_id(site_id)
        safe_id(job_id)
        key = f"{site_id}/{job_id}.tar.gz.enc"
        destination = self.resolve(key)
        private_dir(destination.parent)
        created = False
        try:
            with Path(source).open("rb") as incoming, destination.open("xb") as outgoing:
                created = True
                os.chmod(destination, 0o600)
                shutil.copyfileobj(incoming, outgoing, CHUNK)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            receipt = {"storage_key": key, "size_bytes": Path(source).stat().st_size,
                       "sha256": sha256_file(source), "storage_root": str(self.root)}
            self.verify(receipt)
            return receipt
        except FileExistsError:
            raise BackupError("ARTIFACT_EXISTS") from None
        except BaseException:
            if created:
                destination.unlink(missing_ok=True)
            raise

    def verify(self, receipt):
        path = self.resolve(receipt["storage_key"])
        if (not path.is_file() or path.stat().st_size != receipt["size_bytes"]
                or sha256_file(path) != receipt["sha256"]):
            raise BackupError("INTEGRITY_FAILED")
        return True
