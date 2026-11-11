import json
import threading
import time

import pytest

from jino_backup.errors import BackupError
from jino_backup.state import Settings


@pytest.fixture
def management(tmp_path):
    from jino_backup.dashboard import DashboardData
    from jino_backup.dashboard_management import DashboardManagement

    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            [
                {
                    "id": "one",
                    "name": "Тестовый сервер",
                    "login": "test-panel",
                    "password": "synthetic-panel-secret",
                    "ftp_host": "ftp.example.test",
                    "ftp_login": "test-ftp",
                    "ftp_password": "synthetic-ftp-secret",
                }
            ]
        )
    )
    mapping = {
        key: key
        for key in ("id", "name", "login", "password", "ftp_host", "ftp_login", "ftp_password")
    }
    mapping.update(ftp_protocol="ftps", ftp_port=21)
    state = tmp_path / "state"
    Settings.initialize(state, tmp_path / "archives", state / "master.key")
    data = DashboardData(state, tmp_path / "copies", source, mapping, "test")
    from jino_backup.panel_access import PanelAccessPolicy

    policy = PanelAccessPolicy(state)
    policy.interval_seconds = 0
    policy.window_seconds = 0
    service = DashboardManagement(data, access_policy=policy)
    yield service
    service.close()


def test_catalog_crud_keeps_secrets_encrypted_and_original_source_unchanged(management):
    catalog = management.catalog
    before = management.data.source.read_bytes()
    created = catalog.save(
        None,
        {
            "name": "Новый сервер",
            "login": "new-panel",
            "panel_password": "new-secret",
            "ftp_host": "new.example.test",
            "ftp_login": "new-ftp",
            "ftp_password": "ftp-new-secret",
            "protocol": "ftps",
            "ftp_port": 21,
            "remote_root": "/",
        },
    )
    assert created["row"] == 2
    assert created["folder"] == "new-panel"
    assert "new-secret" not in json.dumps(created)
    assert catalog.credentials(2)["panel"]["password"] == "new-secret"
    assert b"new-secret" not in catalog.path.read_bytes()
    assert b"new-secret" not in catalog.vault_path.read_bytes()
    catalog.save(2, {"name": "Изменённое имя", "panel_password": ""})
    assert catalog.credentials(2)["panel"]["password"] == "new-secret"
    catalog.set_flag(2, "blocked", True)
    assert catalog.get(2)["blocked"]
    catalog.set_flag(2, "blocked", False)
    catalog.set_flag(2, "deleted", True)
    assert catalog.get(2, include_deleted=True)["deleted"]
    with pytest.raises(BackupError):
        catalog.get(2)
    assert management.data.source.read_bytes() == before


def test_task_lifecycle_pauses_resumes_and_stops_without_deleting_copies(management):
    entered = threading.Event()

    def runner(server, task, control, progress):
        entered.set()
        while True:
            control.checkpoint()
            time.sleep(0.005)

    management.runner = runner
    task = management.start(1, "files", {"transport": "ftps"})
    assert entered.wait(2)
    management.action(task["id"], "pause")
    wait_status(management, task["id"], "PAUSED")
    management.action(task["id"], "resume")
    wait_status(management, task["id"], "RUNNING")
    management.action(task["id"], "stop")
    wait_status(management, task["id"], "STOPPED")
    assert management.events(1)


def test_separate_file_and_db_jobs_never_run_together_for_one_server(management):
    release = threading.Event()
    entered = threading.Event()
    running = []

    def runner(server, task, control, progress):
        assert not running
        running.append(task["kind"])
        entered.set()
        while not release.is_set():
            control.checkpoint()
            release.wait(0.01)
        running.clear()
        return {"status": "FILES_ONLY" if task["kind"] == "files" else "DB_ONLY"}

    management.runner = runner
    first = management.start(1, "files", {"transport": "ftps"})
    assert entered.wait(2)
    second = management.start(1, "databases", {})
    assert management.task(second["id"])["state"] == "QUEUED"
    queued = next(task for task in management.plan()["pending"] if task["id"] == second["id"])
    assert queued["queue_rank"] == 0 and queued["queue_total"] == 1
    release.set()
    wait_status(management, first["id"], "FINISHED")
    wait_status(management, second["id"], "FINISHED")


def test_blocked_server_cannot_start_and_edit_requires_valid_transport(management):
    management.catalog.set_flag(1, "blocked", True)
    with pytest.raises(BackupError, match="SERVER_BLOCKED"):
        management.start(1, "files", {})
    with pytest.raises(BackupError):
        management.catalog.save(1, {"protocol": "ftp"})


def wait_status(service, task_id, expected):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if service.task(task_id)["state"] == expected:
            return
        time.sleep(0.01)
    assert service.task(task_id)["state"] == expected


def test_operator_file_task_uses_real_ftps_and_reports_total_bytes(
    management, tmp_path, monkeypatch
):
    from test_ftps import ftps_server

    with ftps_server(tmp_path) as (credentials, certificate, files, server_state):
        monkeypatch.setattr("certifi.where", lambda: str(certificate))
        management.save_server(
            1,
            {
                "ftp_host": credentials["host"],
                "ftp_login": credentials["username"],
                "ftp_password": credentials["password"],
                "ftp_port": credentials["port"],
                "remote_root": "/public_html",
            },
        )
        task = management.start(1, "files", {"transport": "ftps"})
        wait_status(management, task["id"], "FINISHED")
        result = management.task(task["id"])
        assert result["result"]["status"] == "FILES_ONLY"
        assert result["progress"]["inventory_complete"]
        assert result["progress"]["planned_bytes"] == sum(map(len, files.values()))
        assert result["progress"]["received_bytes"] == sum(map(len, files.values()))
        assert result["progress"]["verified_files"] == len(files)
        assert server_state["tls_control"]
        assert server_state["protected_data_connections"] > 0


def test_operator_skips_unsupported_names_and_logs_each_private_path(management, monkeypatch):
    from test_name_skipping import MemorySource
    source = MemorySource()
    monkeypatch.setattr("jino_backup.dashboard_runner.source_for", lambda *args: source)
    task = management.start(1, "files", {"transport": "ftps"})
    wait_status(management, task["id"], "FINISHED")
    task = management.task(task["id"])
    assert task["skip_unsupported_names"] is True
    assert task["result"]["status"] == "PARTIAL"
    assert task["result"]["files_skipped_count"] == 2
    assert task["progress"]["skipped_count"] == 2
    events = [item for item in management.events(1) if item["stage"] == "FILE_SKIPPED"]
    assert {item["file"] for item in events} == {"archive:old", "bad?.txt"}
    assert all(item["skip_reason"] == "WINDOWS_CHARACTERS" for item in events)
    assert source.closed
    for _ in range(6):
        management.event(1, "FILE_DOWNLOAD", kind="files", current_file="ok.txt")
    assert management.events(1, limit=1)[0]["stage"] == "FILE_DOWNLOAD"
    assert len(management.events(1, limit=2, stage="FILE_SKIPPED")) == 2
    management.data.management = management
    detail = management.data.server_detail(1)
    assert len(detail["skipped_events"]) == 2
    assert detail["server"]["configuration"]["skip_unsupported_names"] is True


def test_failed_transfer_exposes_retry_history_partial_bytes_and_manual_resume(management, monkeypatch, tmp_path):
    from test_backup import FixtureSource
    from jino_backup.dashboard_management import TaskControl
    remote = tmp_path / "remote"
    remote.mkdir()
    (remote / "a.txt").write_bytes(b"first-file")
    source = FixtureSource(remote)
    broken, offsets, delays = True, [], []
    def transfer(relative, outgoing, offset):
        offsets.append(offset)
        outgoing.write(b"first-file"[offset:offset+1] if broken else b"first-file"[offset:])
        if broken:
            raise BackupError("TRANSFER_INTERRUPTED")
    source.download = transfer
    monkeypatch.setattr("jino_backup.dashboard_runner.source_for", lambda *args: source)
    monkeypatch.setattr(TaskControl, "wait", lambda self, delay: delays.append(delay))
    task = management.start(1, "files", {"transport": "ftps"})
    wait_status(management, task["id"], "FAILED")
    failed = management.task(task["id"])
    assert failed["progress"]["received_bytes"] == 6
    assert failed["stage"] == "RETRY_EXHAUSTED"
    events = [item for item in management.events(1) if item["stage"] == "RETRY_WAIT"]
    assert len(events) == 5 and sorted(item["retry_attempt"] for item in events) == [1,2,3,4,5]
    assert delays == [2,4,8,16,32]
    broken = False
    continued = management.start(1, "files", {"transport": "ftps"})
    assert continued["id"] == task["id"] and continued.get("result") is None
    wait_status(management, task["id"], "FINISHED")
    assert offsets[-1] == 6
    assert management.task(task["id"])["result"]["status"] == "FILES_ONLY"


def test_skip_name_setting_persists_and_applies_to_retry(management, monkeypatch):
    from test_name_skipping import MemorySource
    monkeypatch.setattr("jino_backup.dashboard_runner.source_for", lambda *args: MemorySource())
    config = management.save_server(1, {"skip_unsupported_names": False})
    binding = config["binding_revision"]
    task = management.start(1, "files", {"transport": "ftps"})
    wait_status(management, task["id"], "FAILED")
    assert management.task(task["id"])["error_code"] == "REMOTE_PATH_UNSAFE"
    config = management.save_server(1, {"skip_unsupported_names": True})
    assert config["binding_revision"] == binding
    management.action(task["id"], "resume")
    wait_status(management, task["id"], "FINISHED")
    assert management.task(task["id"])["result"]["status"] == "PARTIAL"
    with pytest.raises(BackupError, match="SERVER_CONFIG_INVALID"):
        management.save_server(1, {"skip_unsupported_names": "true"})


def test_partial_file_result_can_continue_with_the_same_task_key(management):
    management.runner=lambda *args: {'status':'PARTIAL','files_bytes':3,'file_errors_count':1}
    first=management.start(1,'files',{'transport':'ftps'})
    wait_status(management,first['id'],'FINISHED')
    management.runner=lambda *args: {'status':'FILES_ONLY','files_bytes':10}
    continued=management.start(1,'files',{'transport':'ftps'})
    assert continued['id']==first['id']
    wait_status(management,first['id'],'FINISHED')
    assert management.task(first['id'])['result']['status']=='FILES_ONLY'


def test_operator_db_task_reuses_panel_workflow_and_reports_verified_dumps(management, monkeypatch):
    from test_panel_export import PanelFixture

    class Panel(PanelFixture):
        def inspect_files(self, prepare_transport=True):
            return {}

        def latest_mysql_snapshot(self):
            return "2026-11-10"

    panel = Panel()
    monkeypatch.setattr("jino_backup.dashboard_runner.JinoPanelBrowser", lambda credentials: panel)
    task = management.start(1, "databases", {})
    wait_status(management, task["id"], "FINISHED")
    result = management.task(task["id"])
    assert result["result"]["status"] == "DB_ONLY"
    assert result["progress"]["expected_count"] == 2
    assert result["progress"]["verified_count"] == 2
    assert result["snapshot_date"] == "2026-11-10"
    assert panel.closed
    assert not panel.deleted


def test_db_stop_after_submission_resumes_without_duplicate_export(management, monkeypatch):
    from test_panel_export import PanelFixture

    class Panel(PanelFixture):
        first = True

        def inspect_files(self, prepare_transport=True):
            return {}

        def latest_mysql_snapshot(self):
            return "2026-11-10"

        def wait_export(self, scope, database, timeout_seconds):
            if self.first:
                self.first = False
                management.action(management.tasks(1)[0]["id"], "stop")
            return super().wait_export(scope, database, timeout_seconds)

    panel = Panel()
    monkeypatch.setattr("jino_backup.dashboard_runner.JinoPanelBrowser", lambda credentials: panel)
    task = management.start(1, "databases", {})
    wait_status(management, task["id"], "STOPPED")
    assert len(panel.exports) == 1 and panel.files
    management.server_action(1, "block")
    management.server_action(1, "unblock")
    management.save_server(1, {"name": "Другое имя", "panel_password": "updated-secret"})
    resumed = management.start(1, "databases", {})
    assert resumed["id"] == task["id"]
    wait_status(management, task["id"], "FINISHED")
    assert panel.exports.count("private_database_one") == 1
    assert len(panel.exports) == 2
    assert not panel.deleted


def test_new_ui_database_job_continues_an_unfinished_legacy_export(management, monkeypatch):
    from jino_backup.panel_export import PanelExportJob
    from test_panel_export import PanelFixture

    class Panel(PanelFixture):
        fail = True

        def inspect_files(self, prepare_transport=True):
            return {}

        def latest_mysql_snapshot(self):
            return "2026-11-10"

        def download(self, scope, remote, target, max_bytes):
            if self.fail:
                raise BackupError("TRANSFER_INTERRUPTED")
            return super().download(scope, remote, target, max_bytes)

    panel = Panel()
    server = management.catalog.get(1)
    key = management.data.batch_id + "-2026-11-10"
    old = PanelExportJob(
        management.data.state_dir,
        management.data.destination,
        server["account_ref"],
        key,
        "2026-11-10",
        Settings.load(management.data.state_dir).read_key(),
        lambda: panel,
        server_folder=server["folder"],
    )
    assert old.run()["status"] == "FAILED"
    panel.fail = False
    monkeypatch.setattr("jino_backup.dashboard_runner.JinoPanelBrowser", lambda credentials: panel)
    task = management.start(1, "databases", {})
    wait_status(management, task["id"], "FINISHED")
    assert management.task(task["id"])["workflow_key"] == key
    assert panel.exports.count("private_database_one") == 1
    assert not panel.deleted


def test_new_ui_file_job_reuses_partial_legacy_checkpoint(management, monkeypatch):
    from jino_backup.panel_downloads import PanelFileJob
    from test_backup import FixtureSource

    root = management.data.state_dir / "synthetic-files"
    root.mkdir()
    (root / "site.txt").write_bytes(b"synthetic-content")
    offsets = []
    fail = True

    def source_factory():
        source = FixtureSource(root)
        source.links = {}
        source.scope = {}
        download = source.download

        def transfer(relative, outgoing, offset):
            offsets.append(offset)
            if fail:
                outgoing.write(b"synthetic")
                raise BackupError("TRANSFER_INTERRUPTED")
            return download(relative, outgoing, offset)

        source.download = transfer
        return source

    server = management.catalog.get(1)
    old = PanelFileJob(
        management.data.destination,
        server["server_id"],
        server["folder"],
        management.data.batch_id,
        source_factory,
        state_dir=management.data.state_dir,
    )
    assert old.run()["status"] == "PARTIAL"
    fail = False

    class Panel:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def inspect_files(self, prepare_transport=True):
            return {}

    monkeypatch.setattr(
        "jino_backup.dashboard_runner.JinoPanelBrowser", lambda credentials: Panel()
    )
    monkeypatch.setattr(
        "jino_backup.dashboard_runner.PanelFileSource", lambda *args, **kwargs: source_factory()
    )
    task = management.start(1, "files", {"transport": "https_panel"})
    wait_status(management, task["id"], "FINISHED")
    assert management.task(task["id"])["workflow_key"] == management.data.batch_id
    assert offsets[0] == 0 and offsets[-1] == len(b"synthetic")
    assert management.task(task["id"])["result"]["files_bytes"] == len(b"synthetic-content")


def test_restart_marks_tasks_interrupted_and_never_restarts_them_automatically(management):
    from jino_backup.dashboard_management import DashboardManagement

    management.runner = lambda server, task, control, progress: {"status": "FILES_ONLY"}
    task = management.start(1, "files", {})
    wait_status(management, task["id"], "FINISHED")
    management._update(task["id"], state="RUNNING")
    management.close()
    calls = []
    restarted = DashboardManagement(
        management.data,
        runner=lambda *args: calls.append(1),
        access_policy=management.access_policy,
    )
    try:
        assert restarted.task(task["id"])["state"] == "INTERRUPTED"
        assert not calls
    finally:
        restarted.close()


def test_waiting_jobs_can_be_reordered_and_critical_jobs_go_first(management):
    future = "2099-01-01T00:00:00+00:00"
    first = management.start(1, "files", {"scheduled_at": future})
    second = management.start(1, "databases", {"scheduled_at": future})
    assert management.task(first["id"])["state"] == "SCHEDULED"
    management.change(second["id"], "move", {"direction": "up"})
    assert management.plan()["pending"][0]["id"] == second["id"]
    management.change(first["id"], "priority", {"priority": "critical"})
    assert management.plan()["pending"][0]["id"] == first["id"]
    assert management.task(first["id"])["scheduled_at"] == future
    management.action(first["id"], "stop")
    assert management.task(first["id"])["state"] == "STOPPED"


def test_panel_rate_limit_preserves_queue_and_does_not_block_ftp(management):
    from jino_backup.panel_access import PanelAccessPolicy

    policy = PanelAccessPolicy(management.data.state_dir)
    policy.reserve(99, "check")
    management.access_policy = policy
    management.runner = lambda server, task, control, progress: {"status": "FILES_ONLY"}
    panel = management.start(1, "databases", {})
    assert management.task(panel["id"])["state"] == "WAITING_RATE_LIMIT"
    ftp = management.start(1, "files", {})
    wait_status(management, ftp["id"], "FINISHED")
    assert management.task(panel["id"])["state"] == "WAITING_RATE_LIMIT"
    assert management.plan()["pending"][0]["estimated_at"]


def test_reordered_future_queue_survives_restart(management):
    from jino_backup.dashboard_management import DashboardManagement

    first = management.start(1, "files", {"scheduled_at": "2099-01-01T00:00:00+00:00"})
    second = management.start(1, "databases", {"scheduled_at": "2099-01-02T00:00:00+00:00"})
    management.change(second["id"], "move", {"direction": "top"})
    management.close()
    calls = []
    restarted = DashboardManagement(
        management.data,
        runner=lambda *args: calls.append(1),
        access_policy=management.access_policy,
    )
    try:
        assert [task["id"] for task in restarted.plan()["pending"]] == [second["id"], first["id"]]
        assert all(task["state"] == "SCHEDULED" for task in restarted.plan()["pending"])
        assert not calls
    finally:
        restarted.close()


def test_access_checks_share_the_panel_limit_with_database_jobs(management):
    from jino_backup.dashboard import AccessMonitor
    from jino_backup.panel_access import PanelAccessPolicy

    now = [100000.0]
    policy = PanelAccessPolicy(management.data.state_dir, clock=lambda: now[0])
    management.access_policy = policy
    management.data.management = management
    policy.reserve(99, "databases")
    calls = []
    monitor = AccessMonitor(management.data, checker=lambda row: calls.append(row))
    try:
        monitor.enqueue(1)
        deadline = time.monotonic() + 3
        while monitor.snapshot()["1"]["state"] == "CHECKING" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert monitor.snapshot()["1"]["state"] == "WAITING_RATE_LIMIT"
        assert not calls
        now[0] += 3600
        monitor.retry_waiting()
        deadline = time.monotonic() + 3
        while monitor.snapshot()["1"]["state"] == "CHECKING" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert calls == [1]
        assert monitor.snapshot()["1"]["state"] == "AVAILABLE"
    finally:
        monitor.close()


def test_a_captcha_failure_stops_later_panel_jobs_but_ftp_remains_available(management):
    calls = []

    def runner(server, task, control, progress):
        calls.append(task["kind"])
        if task["kind"] == "databases":
            raise BackupError("CAPTCHA_REQUIRED")
        return {"status": "FILES_ONLY"}

    management.runner = runner
    first = management.start(1, "databases", {})
    wait_status(management, first["id"], "FAILED")
    assert management.access_policy.snapshot()["blocked"]
    resumed = management.action(first["id"], "resume")
    assert resumed["state"] == "WAITING_OPERATOR"
    ftp = management.start(1, "files", {})
    wait_status(management, ftp["id"], "FINISHED")
    assert calls == ["databases", "files"]


def test_future_work_does_not_delay_the_estimate_for_a_ready_panel_job(management):
    management.access_policy.interval_seconds = 3600
    added = management.catalog.save(
        None,
        {"name": "Второй сервер", "login": "second-panel", "panel_password": "synthetic-secret"},
    )
    future = management.start(1, "databases", {"scheduled_at": "2099-01-01T00:00:00+00:00"})
    ready = management.start(
        added["row"], "databases", {"scheduled_at": "2099-01-02T00:00:00+00:00"}
    )
    management._update(ready["id"], scheduled_at=None, state="QUEUED")
    estimates = {task["id"]: task["estimated_at"] for task in management.plan()["pending"]}
    assert estimates[ready["id"]] < estimates[future["id"]]


def test_waiting_access_checks_are_scheduled_again_after_restart(management, monkeypatch):
    from jino_backup.dashboard import AccessMonitor

    callbacks = []

    class Timer:
        def __init__(self, delay, callback):
            callbacks.append(callback)

        def start(self):
            pass

        def cancel(self):
            pass

    path = management.data.state_dir / "dashboard-health.json"
    path.write_text(
        json.dumps({"1": {"state": "WAITING_RATE_LIMIT", "checked_at": None, "message": "Waiting"}})
    )
    monkeypatch.setattr("jino_backup.dashboard.threading.Timer", Timer)
    management.data.management = management
    calls = []
    monitor = AccessMonitor(management.data, checker=lambda row: calls.append(row))
    try:
        assert callbacks and not calls
        callbacks[0]()
        deadline = time.monotonic() + 3
        while monitor.snapshot()["1"]["state"] == "CHECKING" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert calls == [1]
        assert monitor.snapshot()["1"]["state"] == "AVAILABLE"
    finally:
        monitor.close()


def test_http_server_crud_and_task_controls_are_available_on_normal_routes(management):
    from urllib.request import Request, urlopen
    from jino_backup.dashboard import make_server

    management.data.management = management
    management.runner = lambda server, task, control, progress: {"status": "FILES_ONLY"}
    server = make_server(management.data, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = "http://127.0.0.1:" + str(server.server_port)
    cookie = urlopen(address + "/").headers["Set-Cookie"].split(";")[0]

    def request(path, payload=None):
        headers = {"Cookie": cookie, "Origin": address, "Content-Type": "application/json"}
        data = json.dumps(payload).encode() if payload is not None else None
        return json.loads(urlopen(Request(address + path, data=data, headers=headers)).read())

    try:
        assert request("/api/servers/1")["server"]["name"] == "Тестовый сервер"
        started = request("/api/servers/1/jobs", {"kind": "files"})
        wait_status(management, started["id"], "FINISHED")
        assert request("/api/servers/1")["events"]
        request("/api/servers/1/block", {})
        assert request("/api/servers/1")["server"]["blocked"]
        request("/api/servers/1/unblock", {})
        request("/api/servers/1", {"name": "Новое имя"})
        request("/api/servers/1/delete", {})
        assert not request("/api/state")["servers"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
