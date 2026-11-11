import pytest

from jino_backup.errors import BackupError
from jino_backup.scheduler import Scheduler
from test_backup import system


def test_persistent_schedule_runs_once_and_skips_missed_intervals(system):
    settings, repo, source, service = system
    scheduler = Scheduler(repo, service)
    scheduler.set("site-1", interval_seconds=86400, next_run=1000)
    assert scheduler.tick(now=999) == []
    jobs = scheduler.tick(now=1000 + 3 * 86400 + 42)
    assert len(jobs) == 1 and jobs[0]["status"] == "FILES_ONLY"
    assert scheduler.tick(now=1000 + 3 * 86400 + 42) == []
    assert scheduler.list()[0]["next_run"] == 1000 + 4 * 86400
    assert len(repo.jobs()) == 1


def test_disable_schedule_and_minimum_interval(system):
    settings, repo, source, service = system
    scheduler = Scheduler(repo, service)
    with pytest.raises(BackupError, match="SCHEDULE_INTERVAL_INVALID"):
        scheduler.set("site-1", interval_seconds=0, next_run=1000)
    scheduler.set("site-1", interval_seconds=86400, next_run=1000)
    scheduler.disable("site-1")
    assert scheduler.tick(now=1000) == []
