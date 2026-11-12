"""Render the real local UI with synthetic accounts only; never contact Jino."""
import json
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright

from jino_backup.dashboard import AccessMonitor, DashboardData, make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.errors import BackupError
from jino_backup.state import Settings


def unavailable(*args):
    raise BackupError("SYNTHETIC_PREVIEW_ONLY")


def main():
    workspace = Path(__file__).resolve().parents[1]
    scratch = workspace / ".test-tmp"
    scratch.mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="readme-preview-", dir=scratch))
    state = root / "state"
    Settings.initialize(state, root / "archives", state / "master.key")
    rows = [{"id": str(index), "name": f"Сервер {index:02}", "login": f"demo-{index:02}",
             "password": "synthetic-panel-password", "ftp_login": f"demo-{index:02}",
             "ftp_password": "synthetic-ftp-password", "ftp_host": f"demo-{index:02}.example.test"}
            for index in range(1, 7)]
    source = root / "source.json"
    source.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    mapping = {key: key for key in rows[0]}
    mapping.update(ftp_protocol="ftps", ftp_port=21)
    data = DashboardData(state, root / "copies", source, mapping, "readme-demo")
    data.demo = True
    management = DashboardManagement(data, runner=unavailable)
    management._schedule = lambda: None  # Fixtures only; no worker may run.
    data.management = management
    data.monitor = AccessMonitor(data, checker=unavailable)
    now = datetime.now(timezone.utc).isoformat()
    data.monitor.states = {str(index): {"state": "AVAILABLE", "checked_at": now,
                                       "message": "Демонстрационная проверка"}
                           for index in range(1, 7)}

    def task(row, kind, status, progress, result=None):
        created = management.start(row, kind, {})
        management._update(created["id"], state=status,
            stage="FILES_DOWNLOAD" if kind == "files" else "DATABASES",
            progress=progress, result=result)

    task(1, "files", "RUNNING", {"received_bytes": 4800000000, "planned_bytes": 8000000000,
        "planned_files": 2400, "verified_files": 1430, "inventory_complete": True,
        "current_file": "media/example.bin", "current_bytes": 160000000,
        "current_size": 400000000, "active_transfers": 2, "connections_limit": 3})
    task(1, "databases", "QUEUED", {})
    task(2, "files", "FINISHED", {"received_bytes": 2400000000, "planned_bytes": 2400000000,
        "verified_files": 980, "inventory_complete": True}, {"status": "FILES_ONLY"})
    task(2, "databases", "RUNNING", {"verified_count": 2, "expected_count": 3,
                                     "received_bytes": 240000000})
    task(3, "files", "FINISHED", {"received_bytes": 1200000000, "planned_bytes": 1200000000,
        "verified_files": 640, "inventory_complete": True}, {"status": "FILES_ONLY"})
    task(4, "files", "QUEUED", {})
    task(5, "files", "FINISHED", {"received_bytes": 850000000, "planned_bytes": 900000000,
        "verified_files": 510, "inventory_complete": False, "skipped_count": 2}, {"status": "PARTIAL"})
    management.catalog.set_flag(6, "blocked", True)
    server = make_server(data, port=0)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    destination = workspace / "docs" / "images" / "dashboard.png"
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel="chrome", headless=True)
            try:
                page = browser.new_page(viewport={"width": 1600, "height": 1640}, device_scale_factor=1)
                errors, blocked = [], []
                page.on("pageerror", lambda error: errors.append(str(error)))

                def local_only(route):
                    url = urlsplit(route.request.url)
                    if url.hostname != "127.0.0.1" or url.port != server.server_port or route.request.method != "GET":
                        blocked.append({"host": url.hostname, "method": route.request.method})
                        route.abort()
                    elif url.path == "/api/state":
                        response = route.fetch()
                        payload = response.json()
                        payload["destination"] = "F:\\jino"
                        for account in payload["servers"]:
                            account["folder_path"] = "F:\\jino\\" + account["login"]
                        route.fulfill(response=response, json=payload)
                    else:
                        route.continue_()

                page.route("**/*", local_only)
                page.goto(f"http://127.0.0.1:{server.server_port}/", wait_until="networkidle")
                page.locator("#auto-refresh").uncheck()
                expect(page.locator("#server-rows tr")).to_have_count(6)
                expect(page.locator("#server-rows")).to_contain_text("demo-01")
                expect(page.locator("#demo-banner")).to_be_visible()
                page.evaluate('''() => {
                    activity = new BackupActivity();
                    const now = Date.now();
                    for (let i=0; i<=240; i++) {
                        const sample = structuredClone(state.data);
                        for (const server of sample.servers) {
                            for (const [kind, task] of Object.entries(server.latest_tasks || {})) {
                                if (task.state !== 'RUNNING') continue;
                                const p = task.progress;
                                p.received_bytes = (p.received_bytes || 0) +
                                    Math.round(i*2200000 + Math.sin(i/9)*6000000);
                                if (kind === 'files') p.verified_files = (p.verified_files || 0) + Math.floor(i/12)*7;
                            }
                        }
                        activity.observe(sample, now-(240-i)*5000);
                    }
                    renderActivity(); renderTable();
                    document.getElementById('demo-banner').textContent =
                        'Демонстрационные данные. Серверы, пути и история графиков — искусственные примеры.';
                }''')
                assert not errors, errors
                assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
                page.locator("#auto-refresh").blur()
                page.evaluate("window.scrollTo(0, 0)")
                page.screenshot(path=str(destination), full_page=False)
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        serving.join(timeout=5)
    print(json.dumps({"screenshot": "docs/images/dashboard.png", "synthetic_accounts": 6,
                      "customer_data_read": False, "external_requests_sent": 0,
                      "blocked_requests": len(blocked), "page_errors": 0}))


if __name__ == "__main__":
    main()
