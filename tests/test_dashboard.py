import json
import sqlite3
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from jino_backup.importers import identity


@pytest.fixture
def dashboard_data(tmp_path):
    from jino_backup.dashboard import DashboardData

    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            [
                {
                    "id": "one",
                    "name": "Первый клиент",
                    "login": "first-login",
                    "password": "NEVER-EXPOSE-THIS",
                },
                {
                    "id": "two",
                    "name": "<script>unsafe</script>",
                    "login": "second-login",
                    "password": "NEVER-EXPOSE-THIS",
                },
                {
                    "id": "three",
                    "name": "Дубликат A",
                    "login": "duplicate",
                    "password": "NEVER-EXPOSE-THIS",
                    "ftp_host": "a.example.test",
                },
                {
                    "id": "four",
                    "name": "Дубликат B",
                    "login": "duplicate",
                    "password": "NEVER-EXPOSE-THIS",
                    "ftp_host": "b.example.test",
                },
            ]
        )
    )
    mapping = {
        "id": "id",
        "name": "name",
        "login": "login",
        "password": "password",
        "ftp_host": "ftp_host",
    }
    root = tmp_path / "out"
    root.mkdir()
    report = root / (identity("jino-all", "batch") + ".json")
    report.write_text(
        json.dumps(
            {
                "finished": False,
                "started_at": "2026-11-10T18:00:00+00:00",
                "servers": [
                    {
                        "row": 1,
                        "status": "RUNNING",
                        "stage": "FILES_DOWNLOAD",
                        "database_status": "DB_ONLY",
                        "databases_count": 3,
                    },
                    {"row": 2, "status": "QUEUED"},
                ],
                "unexpected_secret": "NEVER-EXPOSE-THIS",
            }
        )
    )
    state = tmp_path / "state"
    state.mkdir()
    journal = state / "worker.log"
    journal.write_text(
        json.dumps(
            {
                "time": "2026-11-10T18:00:00+00:00",
                "row": 1,
                "status": "RUNNING",
                "stage": "FILES_DOWNLOAD",
                "database_status": "DB_ONLY",
                "databases_count": 3,
            }
        )
        + "\n"
    )
    (state / "active-download.json").write_text(
        json.dumps(
            {
                "batch_id": "batch",
                "stdout": str(journal),
                "started_at": "2026-11-10T18:00:00+00:00",
                "pid": 1234,
            }
        )
    )
    data = DashboardData(state, root, source, mapping, "batch", process_alive=lambda: True)
    return data


def test_snapshot_has_human_names_and_never_exposes_credentials(dashboard_data):
    data = dashboard_data.snapshot()
    assert len(data["servers"]) == 4
    assert data["servers"][0]["name"] == "Первый клиент"
    assert data["servers"][0]["login"] == "first-login"
    assert data["summary"]["active"] == 1 and data["summary"]["needs_review"] == 2
    assert data["servers"][1]["availability"]["state"] == "UNKNOWN"
    assert "NEVER-EXPOSE-THIS" not in json.dumps(data)
    assert "password" not in json.dumps(data)


def test_progress_reads_sqlite_without_changing_checkpoint(dashboard_data):
    root = (
        dashboard_data.destination
        / "first-login"
        / identity("download", "batch", "https-panel")
        / ".jino-progress"
    )
    root.mkdir(parents=True)
    path = root / "progress.sqlite3"
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE files(path TEXT PRIMARY KEY,size INTEGER,modified TEXT,state TEXT,committed INTEGER,sha256 TEXT)"
    )
    db.executemany(
        "INSERT INTO files VALUES(?,?,?,?,?,?)",
        [
            ("index.html", 20, "1", "done", 20, "synthetic"),
            ("large.zip", 1000, "1", "partial", 250, "synthetic"),
        ],
    )
    db.commit()
    db.close()
    before = path.read_bytes()
    server = dashboard_data.snapshot()["servers"][0]
    assert server["progress"]["received_bytes"] == 270
    assert server["progress"]["verified_files"] == 1
    assert server["progress"]["current_file"]["percent"] == 25
    assert path.read_bytes() == before


def test_dead_worker_is_not_reported_as_working_or_server_offline(dashboard_data):
    dashboard_data.process_alive = lambda: False
    snapshot = dashboard_data.snapshot()
    assert snapshot["worker"]["state"] == "STOPPED"
    assert snapshot["servers"][0]["availability"]["state"] != "UNAVAILABLE"


def test_access_confirmation_uses_the_accounts_event_time(dashboard_data):
    confirmed = "2026-11-11T00:00:00+00:00"
    with (dashboard_data.state_dir / "worker.log").open("a") as journal:
        journal.write(
            json.dumps(
                {"time": confirmed, "row": 2, "status": "RUNNING", "stage": "ACCESS_VERIFIED"}
            )
            + "\n"
        )
    server = dashboard_data.snapshot()["servers"][1]
    assert server["availability"]["checked_at"] == confirmed


def test_failure_before_access_check_does_not_confirm_availability(dashboard_data):
    (dashboard_data.state_dir / "worker.log").write_text(
        json.dumps({"row": 1, "status": "FAILED", "stage": "FINISHED", "error_code": "UI_CHANGED"})
        + "\n"
    )
    assert dashboard_data.snapshot()["servers"][0]["availability"]["state"] != "CONFIRMED"


def test_corrupt_report_is_unknown_instead_of_a_fresh_empty_queue(dashboard_data):
    (dashboard_data.state_dir / "worker.log").write_text("{broken")
    snapshot = dashboard_data.snapshot()
    assert snapshot["report_error"]
    assert snapshot["servers"][0]["status"] == "UNKNOWN"


def test_truncated_journal_discards_previous_worker_state(dashboard_data):
    assert dashboard_data.snapshot()["summary"]["active"] == 1
    (dashboard_data.state_dir / "worker.log").write_text("{broken")
    snapshot = dashboard_data.snapshot()
    assert snapshot["report_error"]
    assert snapshot["summary"]["active"] == 0
    assert snapshot["servers"][0]["status"] == "UNKNOWN"


def test_access_probe_skips_active_and_ambiguous_accounts(dashboard_data):
    from jino_backup.dashboard import AccessMonitor

    calls = []
    monitor = AccessMonitor(dashboard_data, checker=lambda row: calls.append(row))
    try:
        assert monitor.enqueue(1)["using_active_transfer"]
        with pytest.raises(Exception):
            monitor.enqueue(3)
        assert not calls
    finally:
        monitor.close()


def test_dashboard_never_opens_live_report_and_does_not_block_its_replacement(
    dashboard_data, monkeypatch
):
    import jino_backup.dashboard as module

    target = dashboard_data.report_path
    replacement = target.with_name("replacement.json")
    replacement.write_bytes(b"new-report")
    original = module.read_shared

    def reader(path, *args, **kwargs):
        assert Path(path) != target, "Live JSON must never be opened by the dashboard"
        if Path(path).name == "worker.log" and replacement.exists():
            replacement.replace(target)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(module, "read_shared", reader)
    assert dashboard_data.snapshot()["summary"]["active"] == 1
    assert target.read_bytes() == b"new-report"


def test_dashboard_cli_help_has_local_port_without_starting_backup():
    from test_cli import command

    result = command("ui", "--help")
    assert result.returncode == 0 and "--port" in result.stdout


def test_plan_includes_old_database_and_file_results_as_read_only_history(dashboard_data):
    (dashboard_data.state_dir / "worker.log").write_text(
        json.dumps(
            {
                "time": "2026-11-11T00:00:00+00:00",
                "row": 1,
                "status": "DB_ONLY",
                "stage": "FINISHED",
                "files": {"status": "FAILED", "error_code": "TLS_CERTIFICATE_INVALID"},
                "databases": {"status": "DB_ONLY", "databases_count": 2},
            }
        )
        + "\n"
    )
    try:
        plan = dashboard_data.work_plan()
        assert {task["kind"] for task in plan["completed"]} == {"files", "databases"}
        assert all(task["historical"] for task in plan["completed"])
        assert {task["state"] for task in plan["completed"]} == {"FINISHED", "FAILED"}
        assert "NEVER-EXPOSE-THIS" not in json.dumps(plan)
    finally:
        dashboard_data.management.close()


def test_server_only_serves_fixed_assets_and_protects_private_api(dashboard_data):
    from jino_backup.dashboard import make_server

    server = make_server(dashboard_data, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = "http://127.0.0.1:" + str(server.server_port)
    try:
        with pytest.raises(HTTPError) as error:
            urlopen(address + "/api/state")
        assert error.value.code == 401
        response = urlopen(address + "/")
        cookie = response.headers["Set-Cookie"].split(";")[0]
        assert "HttpOnly" in response.headers["Set-Cookie"]
        assert "SameSite=Strict" in response.headers["Set-Cookie"]
        assert response.status == 200
        request = Request(address + "/api/state", headers={"Cookie": cookie})
        data = json.loads(urlopen(request).read())
        assert data["servers"][0]["login"] == "first-login"
        for path in ("/abw_sibds__server.json", "/.state/master.key", "/../PRODUCT.md"):
            with pytest.raises(HTTPError) as error:
                urlopen(address + path)
            assert error.value.code == 404
        request = Request(
            address + "/api/state", headers={"Host": "evil.example", "Cookie": cookie}
        )
        with pytest.raises(HTTPError) as error:
            urlopen(request)
        assert error.value.code == 403
        request = Request(
            address + "/api/checks",
            data=b'{"row":2}',
            headers={
                "Cookie": cookie,
                "Content-Type": "application/json",
                "Origin": "https://evil.example",
            },
        )
        with pytest.raises(HTTPError) as error:
            urlopen(request)
        assert error.value.code == 403
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
