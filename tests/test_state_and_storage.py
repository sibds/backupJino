import io
import json
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from jino_backup.errors import BackupError
from jino_backup.importers import Candidate
from jino_backup.state import Repository, Settings
from jino_backup.storage import LocalStorage, decrypt_file, encrypt_file


@pytest.fixture
def repo(tmp_path):
    return Repository(tmp_path / "state.sqlite3", Fernet.generate_key())


def test_settings_persist_user_selected_backup_folder(tmp_path):
    state, destination, key = tmp_path / "state", tmp_path / "archives", tmp_path / "keys/master.key"
    settings = Settings.initialize(state, destination, key)
    assert key.exists()
    assert Settings.load(state).backup_dir == destination
    assert settings.key_file == key
    new_destination = tmp_path / "other-archives"
    settings.set_backup_dir(new_destination)
    assert Settings.load(state).backup_dir == new_destination
    with pytest.raises(BackupError, match="ALREADY_INITIALIZED"):
        Settings.initialize(state, destination, key)


def test_backup_destination_cannot_include_state_or_key(tmp_path):
    with pytest.raises(BackupError, match="STORAGE_LOCATION_UNSAFE"):
        Settings.initialize(tmp_path / "state", tmp_path, tmp_path / "keys/master.key")


def test_secrets_encrypted_at_rest_and_reimport_is_idempotent(repo):
    candidate = Candidate("profile-1", "ftp", {"password": "very-private-password"})
    assert repo.import_profiles([candidate]) == 1
    assert repo.import_profiles([candidate]) == 0
    assert repo.credentials("profile-1")["password"] == "very-private-password"
    assert "very-private-password" not in json.dumps(repo.profiles())
    assert b"very-private-password" not in repo.path.read_bytes()
    with pytest.raises(BackupError, match="PROFILE_CONFLICT"):
        repo.import_profiles([Candidate("profile-1", "ftp", {"password": "changed"})])


def test_registry_contains_only_secret_references_and_vault_is_separate(repo):
    password = "very-private-password"
    repo.import_profiles([Candidate("profile-1", "ftp", {"password": password})])
    with repo.connection() as db:
        row = dict(db.execute("SELECT * FROM profiles").fetchone())
    assert "secret_ref" in row
    assert "payload" not in row
    assert repo.vault.path != repo.path
    assert password.encode() not in repo.vault.path.read_bytes()
    assert repo.credentials("profile-1") == {"password": password}


def test_missing_vault_fails_closed_and_never_replaces_a_secret(repo):
    repo.import_profiles([Candidate("profile-1", "ftp", {"password": "private"})])
    repo.vault.path.unlink()
    with pytest.raises(BackupError, match="SECRET_UNAVAILABLE"):
        repo.credentials("profile-1")


def add_site(repo, site_id="site-1", confirmed=True):
    repo.import_profiles([Candidate("profile-1", "ftp", {
        "host": "ftp.example.test", "port": 21, "protocol": "ftps",
        "username": "synthetic-user", "password": "synthetic-password",
    })])
    repo.add_site(site_id, "client-1", "profile-1", "/public_html", confirmed=confirmed)


def test_site_requires_explicit_confirmation_before_job(repo):
    add_site(repo, confirmed=False)
    with pytest.raises(BackupError, match="PROFILE_UNVERIFIED"):
        repo.claim_job("site-1", "request-1")


def test_job_claim_serializes_site_and_reuses_idempotency_key(repo):
    add_site(repo)
    first, claimed = repo.claim_job("site-1", "request-1")
    assert claimed
    same, claimed = repo.claim_job("site-1", "request-1")
    assert same["job_id"] == first["job_id"]
    assert not claimed
    with pytest.raises(BackupError, match="SITE_BUSY"):
        repo.claim_job("site-1", "request-2")
    repo.update_job(first["job_id"], "FAILED", error_code="TRANSFER_INTERRUPTED")
    second, claimed = repo.claim_job("site-1", "request-2")
    assert claimed and second["job_id"] != first["job_id"]


def test_import_transaction_does_not_partially_commit(repo):
    repo.import_profiles([Candidate("existing", "panel", {"password": "old"})])
    with pytest.raises(BackupError):
        repo.import_profiles([Candidate("new", "panel", {"password": "new"}),
                              Candidate("existing", "panel", {"password": "changed"})])
    assert len(repo.profiles()) == 1


def test_archive_encryption_roundtrip_and_aad_binding(tmp_path):
    key = Fernet.generate_key()
    plain, encrypted, restored = [tmp_path / name for name in ["input", "encrypted", "restored"]]
    plain.write_bytes(b"customer-private-content" * 100000)
    encrypt_file(plain, encrypted, key, "job-1")
    assert b"customer-private-content" not in encrypted.read_bytes()
    decrypt_file(encrypted, restored, key, "job-1")
    assert restored.read_bytes() == plain.read_bytes()
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        decrypt_file(encrypted, tmp_path / "wrong-job", key, "job-2")
    assert not (tmp_path / "wrong-job").exists()


@pytest.mark.parametrize("corruption", ["tag", "truncate", "header"])
def test_tampering_never_publishes_plaintext(tmp_path, corruption):
    key = Fernet.generate_key()
    plain, encrypted, restored = [tmp_path / name for name in ["input", "encrypted", "restored"]]
    plain.write_bytes(b"sensitive-content")
    encrypt_file(plain, encrypted, key, "job-1")
    content = bytearray(encrypted.read_bytes())
    if corruption == "tag":
        content[-1] ^= 1
    elif corruption == "header":
        content[0] ^= 1
    else:
        content = content[:-20]
    encrypted.write_bytes(content)
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        decrypt_file(encrypted, restored, key, "job-1")
    assert not restored.exists()
    assert not list(tmp_path.glob("*.partial-*"))


def test_storage_is_create_only_and_verifies_actual_bytes(tmp_path):
    storage = LocalStorage(tmp_path / "backups")
    source = tmp_path / "encrypted"
    source.write_bytes(b"ciphertext")
    receipt = storage.put("site-1", "job-1", source)
    assert storage.verify(receipt)
    with pytest.raises(BackupError, match="ARTIFACT_EXISTS"):
        storage.put("site-1", "job-1", source)
    (storage.root / receipt["storage_key"]).write_bytes(b"corrupted")
    with pytest.raises(BackupError, match="INTEGRITY_FAILED"):
        storage.verify(receipt)


@pytest.mark.parametrize("site_id", ["../escape", "C:/escape", "site\\escape", "CON", "a/b"])
def test_storage_rejects_unsafe_ids(tmp_path, site_id):
    source = tmp_path / "encrypted"
    source.write_bytes(b"data")
    with pytest.raises(BackupError, match="IDENTIFIER_INVALID"):
        LocalStorage(tmp_path / "backups").put(site_id, "job-1", source)
