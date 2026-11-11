import json
import os
import subprocess
import sys

import pytest


def command(*arguments):
    environment = {**os.environ, "PYTHONPATH": str(__import__("pathlib").Path("src").resolve())}
    return subprocess.run([sys.executable, "-m", "jino_backup", *map(str, arguments)],
                          env=environment, capture_output=True, text=True, encoding="utf-8")


def test_help_works_without_configuration():
    result = command("--help")
    assert result.returncode == 0
    assert "backup" in result.stdout


def test_offline_inspection_does_not_require_key_or_persist_secrets(tmp_path):
    path = tmp_path / "source.json"
    path.write_text('[{"id":"1","login":"alice","password":"synthetic-password"}]')
    state = tmp_path / "state"
    result = command("--state-dir", state, "credentials", "inspect-jino", "--file", path)
    assert result.returncode == 0
    assert json.loads(result.stdout)["rows_total"] == 1
    assert "synthetic-password" not in result.stdout
    assert not state.exists()


def test_cli_initialize_settings_and_dry_run_then_commit(tmp_path):
    state = tmp_path / "state"
    common = ["--state-dir", state]
    result = command(*common, "init", "--backup-dir", tmp_path / "archives",
                     "--key-file", tmp_path / "keys/master.key")
    assert result.returncode == 0, result.stdout
    mapping, source = tmp_path / "mapping.json", tmp_path / "source.json"
    mapping.write_text('{"id":"id","login":"login","password":"password"}')
    source.write_text('[{"id":"1","login":"alice","password":"synthetic-password"}]')
    args = [*common, "credentials", "import-jino", "--file", source, "--mapping", mapping]
    result = command(*args)
    assert result.returncode == 0
    assert json.loads(result.stdout)["dry_run"] is True
    assert json.loads(command(*common, "profiles", "list").stdout) == []
    result = command(*args, "--confirm")
    assert result.returncode == 0
    assert json.loads(result.stdout)["imported"] == 1
    assert "synthetic-password" not in result.stdout
    result = command(*common, "settings", "set-backup-dir", "--path", tmp_path / "new-archives")
    assert result.returncode == 0
    result = command(*common, "settings", "show")
    assert json.loads(result.stdout)["backup_dir"] == str(tmp_path / "new-archives")


def test_bad_json_error_never_echoes_content_or_traceback(tmp_path):
    path = tmp_path / "source.json"
    path.write_text('{"password":"private-password" BROKEN}')
    result = command("credentials", "inspect-jino", "--file", path)
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "SOURCE_JSON_INVALID"
    assert "private-password" not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_partial_backup_exit_code_is_distinct():
    from jino_backup.cli import result_code
    assert result_code({"status": "COMPLETE"}) == 0
    assert result_code({"status": "FILES_ONLY"}) == 2
    assert result_code({"status": "FAILED"}) == 1


def test_mysql_configuration_and_inventory_are_explicit(tmp_path):
    common = ["--state-dir", tmp_path / "state"]
    assert command(*common, "init", "--backup-dir", tmp_path / "archives",
                   "--key-file", tmp_path / "keys/master.key").returncode == 0
    ca = tmp_path / "ca.pem"
    ca.touch()
    result = command(*common, "settings", "set-mysql", "--executable", "mysqldump",
                     "--ca-file", ca)
    assert result.returncode == 0
    assert json.loads(result.stdout)["mysql_ca_file"] == str(ca)


def test_failure_exit_code_takes_precedence_over_partial_results():
    from jino_backup.cli import batch_result_code
    assert batch_result_code([{"status": "FILES_ONLY"}, {"status": "FAILED"}]) == 1
    assert batch_result_code([{"status": "FILES_ONLY"}, {"status": "COMPLETE"}]) == 2
    assert batch_result_code([]) == 0


def test_import_without_mapping_inspects_only_and_does_not_initialize(tmp_path):
    source = tmp_path / "source.json"
    source.write_text('[{"id":"1","login":"alice","password":"synthetic-password"}]')
    state = tmp_path / "state"
    result = command("--state-dir", state, "credentials", "import-jino", "--file", source)
    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["dry_run"] and report["mapping_required"]
    assert not state.exists()
    assert "synthetic-password" not in result.stdout + result.stderr


def test_mapping_required_for_confirm_and_conflicting_flags_rejected(tmp_path):
    source = tmp_path / "source.json"
    source.write_text("[]")
    for flags, code in [(["--confirm"], "MAPPING_REQUIRED"),
                        (["--confirm", "--dry-run"], "CLI_ARGUMENT_INVALID")]:
        result = command("credentials", "import-jino", "--file", source, *flags)
        assert result.returncode == 1
        assert json.loads(result.stdout)["error_code"] == code


def test_argument_errors_never_echo_unknown_values():
    result = command("--private-password-example")
    assert result.returncode == 1
    assert "private-password-example" not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_invalid_mapping_is_safe_error(tmp_path):
    source, mapping = tmp_path / "source.json", tmp_path / "mapping.json"
    source.write_text("[]")
    mapping.write_text('["synthetic-secret"]')
    result = command("credentials", "import-jino", "--file", source, "--mapping", mapping)
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "MAPPING_INVALID"
    assert "synthetic-secret" not in result.stdout + result.stderr


def test_filezilla_dry_run_is_available_without_initialization(tmp_path):
    from test_importers import xml_profile
    source = tmp_path / "sites.xml"
    source.write_text(xml_profile("4"))
    state = tmp_path / "state"
    result = command("--state-dir", state, "sites", "import-filezilla", "--file", source)
    assert result.returncode == 0
    assert json.loads(result.stdout)["profiles_valid"] == 1
    assert not state.exists()


def test_registered_site_must_be_confirmed_and_validate_is_offline(tmp_path):
    from test_importers import xml_profile
    state = tmp_path / "state"
    common = ["--state-dir", state]
    assert command(*common, "init", "--backup-dir", tmp_path / "archives",
                   "--key-file", tmp_path / "keys/master.key").returncode == 0
    source = tmp_path / "sites.xml"
    source.write_text(xml_profile("4"))
    imported = command(*common, "sites", "import-filezilla", "--file", source, "--confirm")
    profile = json.loads(imported.stdout)["profiles"][0]["profile_id"]
    added = command(*common, "sites", "add", "--site-id", "site-1", "--client-id", "client-1",
                    "--ftp-profile-id", profile, "--remote-root", "/public_html")
    assert added.returncode == 0
    listed = command(*common, "sites", "list", "--status", "needs-review")
    assert len(json.loads(listed.stdout)) == 1
    checked = command(*common, "site", "validate", "--site-id", "site-1")
    assert checked.returncode == 1
    assert json.loads(checked.stdout)["error_code"] == "PROFILE_UNVERIFIED"
    job = command(*common, "backup", "run", "--site-id", "site-1", "--idempotency-key", "manual-1")
    assert job.returncode == 1
    assert json.loads(job.stdout)["error_code"] == "PROFILE_UNVERIFIED"


def test_cli_backup_verify_restore_and_schedule_use_service(tmp_path, capsys):
    from jino_backup.cli import main
    from test_backup import system as system_fixture

    settings, repo, source, service = system_fixture.__wrapped__(tmp_path)
    common = ["--state-dir", str(settings.state_dir)]
    from unittest.mock import patch
    with patch("jino_backup.cli.BackupService", return_value=service):
        assert main([*common, "backup", "run", "--site-id", "site-1",
                     "--idempotency-key", "manual-1", "--mode", "auto"]) == 2
        job = json.loads(capsys.readouterr().out)
        assert job["status"] == "FILES_ONLY"
        assert main([*common, "backup", "verify", "--job-id", job["job_id"]]) == 0
        assert json.loads(capsys.readouterr().out)["verified"]
        assert main([*common, "restore", "test", "--job-id", job["job_id"], "--isolated",
                     "--destination", str(tmp_path / "isolated")]) == 0
        restored = json.loads(capsys.readouterr().out)
        assert not restored["application_restore_tested"]
        assert (tmp_path / "isolated/.htaccess").exists()
        assert main([*common, "schedule", "set", "--site-id", "site-1",
                     "--interval-seconds", "86400", "--next-run", "1000"]) == 0
        capsys.readouterr()
        assert main([*common, "schedule", "tick", "--now", "1000"]) == 2
        jobs = json.loads(capsys.readouterr().out)
        assert len(jobs) == 1 and jobs[0]["status"] == "FILES_ONLY"


def test_restore_requires_explicit_isolated_flag():
    result = command("restore", "test", "--job-id", "job-1", "--destination", "some-folder")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "CLI_ARGUMENT_INVALID"


def test_cli_files_only_never_invokes_database_export(tmp_path, capsys):
    from unittest.mock import patch

    from jino_backup.cli import main
    from jino_backup.service import BackupService
    from test_backup import system as system_fixture
    from test_database import FixtureDump, bind_database

    settings, repo, source, _ = system_fixture.__wrapped__(tmp_path)
    bind_database(repo)
    repo.confirm_databases("site-1")
    class CountingDump(FixtureDump):
        calls = 0
        def export(self, *args, **kwargs):
            self.calls += 1
            return super().export(*args, **kwargs)
    exporter = CountingDump()
    service = BackupService(settings, repo, source_factory=lambda c, s: source,
                            db_exporter=exporter)
    with patch("jino_backup.cli.BackupService", return_value=service):
        code = main(["--state-dir", str(settings.state_dir), "backup", "run", "--site-id",
                     "site-1", "--idempotency-key", "only-files", "--mode", "files-only"])
    job = json.loads(capsys.readouterr().out)
    assert code == 2 and job["status"] == "FILES_ONLY"
    assert exporter.calls == 0


def test_bulk_download_dry_run_has_no_side_effects_and_plaintext_requires_flag(tmp_path):
    source = tmp_path / 'servers.json'
    mapping = tmp_path / 'mapping.json'
    target = tmp_path / 'destination'
    source.write_text('[{"id":"1","ftp_host":"ftp.example.test","ftp_login":"alice","ftp_password":"private-test-password"}]')
    mapping.write_text('{"id":"id","ftp_host":"ftp_host","ftp_login":"ftp_login","ftp_password":"ftp_password","ftp_protocol":"ftps"}')
    args = ['backup','download-servers','--file',source,'--mapping',mapping,
            '--destination',target,'--batch-id','first','--remote-root','/']
    result = command(*args)
    assert result.returncode == 0
    assert json.loads(result.stdout)['servers_ready'] == 1
    assert not target.exists()
    result = command(*args, '--execute')
    assert result.returncode == 1
    assert json.loads(result.stdout)['error_code'] == 'PLAINTEXT_ACKNOWLEDGEMENT_REQUIRED'
    assert not target.exists()
    assert 'private-test-password' not in result.stdout + result.stderr
