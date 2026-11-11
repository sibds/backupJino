import base64
import json

import pytest

from jino_backup.errors import BackupError
from jino_backup.importers import inspect_jino, parse_filezilla, parse_jino


MAPPING = {"id": "id", "login": "login", "password": "password"}


def dump(tmp_path, rows, tables=1):
    path = tmp_path / "input.json"
    path.write_text(json.dumps([
        {"type": "header", "version": "5"},
        *[{"type": "table", "name": "abw_sibds__server", "data": rows}
          for _ in range(tables)],
        {"type": "table", "name": "unrelated", "data": [{"password": "other-secret"}]},
    ]), encoding="utf-8")
    return path


def test_inspection_reports_schema_without_values(tmp_path):
    path = dump(tmp_path, [{"id": "1", "login": "private-login", "password": "private-secret"}])
    report = inspect_jino(path)
    assert report["rows_total"] == 1
    assert report["fields"]["password"]["types"] == {"str": 1}
    assert "private" not in json.dumps(report)


def test_exact_table_is_required_and_ambiguity_rejected(tmp_path):
    with pytest.raises(BackupError, match="SOURCE_TABLE_AMBIGUOUS"):
        inspect_jino(dump(tmp_path, [], tables=2))


def test_array_of_rows_supported_but_not_arbitrary_nested_objects(tmp_path):
    path = tmp_path / "rows.json"
    path.write_text('[{"id":"1","login":"alice","password":"example-password"}]')
    assert len(parse_jino(path, MAPPING).candidates) == 1
    path.write_text('{"arbitrary":{"data":[]}}')
    with pytest.raises(BackupError, match="SOURCE_FORMAT_INVALID"):
        inspect_jino(path)


def test_duplicate_logins_are_all_reviewed_not_silently_overwritten(tmp_path):
    rows = [{"id": str(i), "login": "alice", "password": "example-password"} for i in range(2)]
    batch = parse_jino(dump(tmp_path, rows), MAPPING)
    assert batch.report["accounts_valid"] == 0
    assert batch.report["duplicate_accounts"] == 2
    assert not batch.candidates
    assert "example-password" not in repr(batch)


def test_panel_and_ftp_credentials_are_separate(tmp_path):
    row = {"id": "1", "login": "panel-user", "password": "panel-secret",
           "ftp_host": "ftp.example.test", "ftp_login": "ftp-user", "ftp_password": "ftp-secret"}
    mapping = {**MAPPING, "ftp_host": "ftp_host", "ftp_login": "ftp_login",
               "ftp_password": "ftp_password", "ftp_protocol": "ftps"}
    batch = parse_jino(dump(tmp_path, [row]), mapping)
    assert {c.kind for c in batch.candidates} == {"panel", "ftp"}
    ftp = next(c for c in batch.candidates if c.kind == "ftp")
    assert ftp.credentials["password"] == "ftp-secret"
    assert "secret" not in repr(ftp)


def test_missing_credentials_and_hashes_are_not_usable(tmp_path):
    rows = [{"id": "1", "login": "alice", "password": ""},
            {"id": "2", "login": "bob", "password": "$2y$10$" + "x" * 53}]
    batch = parse_jino(dump(tmp_path, rows), MAPPING)
    assert batch.report["missing_credentials"] == 1
    assert batch.report["unrecoverable_credentials"] == 1
    assert not batch.candidates


def xml_profile(protocol="0", password=None, host="ftp.example.test"):
    if password is None:
        password = base64.b64encode(b"ftp-secret").decode()
    return (f'<FileZilla3><Servers><Server><Host>{host}</Host><Port>21</Port>'
            f'<Protocol>{protocol}</Protocol><User>alice</User>'
            f'<Pass encoding="base64">{password}</Pass></Server></Servers></FileZilla3>')


def test_filezilla_plaintext_ftp_is_review_only(tmp_path):
    path = tmp_path / "sites.xml"
    path.write_text(xml_profile())
    batch = parse_filezilla(path)
    assert batch.report["profiles_total"] == 1
    assert batch.report["needs_review"] == 1
    assert not batch.candidates


def test_filezilla_explicit_tls_import_preserves_password(tmp_path):
    path = tmp_path / "sites.xml"
    path.write_text(xml_profile(protocol="4"))
    batch = parse_filezilla(path)
    assert batch.candidates[0].credentials["password"] == "ftp-secret"
    assert batch.candidates[0].credentials["protocol"] == "ftps"
    assert "ftp-secret" not in json.dumps(batch.report)


def test_xml_errors_do_not_echo_source_and_dtd_rejected(tmp_path):
    path = tmp_path / "sites.xml"
    path.write_text('<FileZilla3><Pass>secret&broken</Pass></FileZilla3>')
    with pytest.raises(BackupError, match="SOURCE_XML_INVALID") as caught:
        parse_filezilla(path)
    assert "secret" not in str(caught.value)
    path.write_text('<!DOCTYPE foo [<!ENTITY a "secret">]><FileZilla3/>')
    with pytest.raises(BackupError, match="SOURCE_XML_UNSAFE"):
        parse_filezilla(path)


def test_invalid_base64_is_not_repaired(tmp_path):
    path = tmp_path / "sites.xml"
    path.write_text(xml_profile(protocol="4", password="not valid!!"))
    batch = parse_filezilla(path)
    assert not batch.candidates
    assert batch.report["issues"][0]["code"] == "CREDENTIAL_ENCODING_INVALID"


def test_named_table_object_cannot_omit_exact_table_identity(tmp_path):
    path = tmp_path / "input.json"
    path.write_text('{"type":"table","data":[{"id":"1"}]}')
    with pytest.raises(BackupError, match="SOURCE_TABLE_AMBIGUOUS"):
        inspect_jino(path)


@pytest.mark.parametrize("mapping", [None, [], {"id":"id", "login":"login", "password":"login"}])
def test_mapping_requires_distinct_explicit_required_columns(tmp_path, mapping):
    path = dump(tmp_path, [{"id":"1", "login":"alice", "password":"example-password"}])
    with pytest.raises(BackupError, match="MAPPING_INVALID"):
        parse_jino(path, mapping)


def test_whitespace_only_login_is_not_an_account(tmp_path):
    path = dump(tmp_path, [{"id":"1", "login":"   ", "password":"example-password"}])
    batch = parse_jino(path, MAPPING)
    assert not batch.candidates
    assert batch.report["missing_credentials"] == 1


def test_inspection_includes_empty_fraction(tmp_path):
    path = dump(tmp_path, [{"id":"1", "login":"alice"}, {"id":"2", "login":""}])
    assert inspect_jino(path)["fields"]["login"]["empty_fraction"] == 0.5
