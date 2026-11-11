import threading

import pytest

from jino_backup.errors import BackupError
from jino_backup.panel_access import PanelAccessPolicy


def test_panel_logins_are_spaced_an_hour_and_persist_across_restart(tmp_path):
    now = [100000.0]
    policy = PanelAccessPolicy(tmp_path, clock=lambda: now[0])
    policy.reserve(1, "databases")
    for row in (2, 3):
        with pytest.raises(BackupError, match="PANEL_RATE_LIMIT"):
            policy.reserve(row, "files")
        now[0] += 3600
        policy.reserve(row, "files")
    state = policy.snapshot()
    assert state["recent_attempts"] == 3
    restarted = PanelAccessPolicy(tmp_path, clock=lambda: now[0])
    assert not restarted.snapshot()["allowed"]
    now[0] += 3600
    assert restarted.snapshot()["allowed"]
    restarted.reserve(4, "check")
    assert restarted.snapshot()["recent_attempts"] == 3


def test_captcha_stops_the_panel_queue_until_operator_resumes(tmp_path):
    now = [100000.0]
    policy = PanelAccessPolicy(tmp_path, clock=lambda: now[0])
    policy.reserve(1, "databases")
    policy.block(1)
    now[0] += 86400
    with pytest.raises(BackupError, match="PANEL_AUTH_PAUSED"):
        policy.reserve(2, "check")
    restarted = PanelAccessPolicy(tmp_path, clock=lambda: now[0])
    assert restarted.snapshot()["blocked"]
    restarted.resume()
    restarted.reserve(2, "check")


def test_parallel_checks_cannot_consume_the_same_panel_slot(tmp_path):
    policy = PanelAccessPolicy(tmp_path, clock=lambda: 100000.0)
    results = []

    def reserve(row):
        try:
            policy.reserve(row, "check")
            results.append("allowed")
        except BackupError as error:
            results.append(error.code)

    threads = [threading.Thread(target=reserve, args=(row,)) for row in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)
    assert sorted(results) == ["PANEL_RATE_LIMIT", "allowed"]
