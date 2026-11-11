"""Persistent UTC interval schedules. Dispatching is sequential and idempotent."""

import time

from .errors import BackupError
from .state import TERMINAL


class Scheduler:
    def __init__(self, repository, service):
        self.repository, self.service = repository, service

    def set(self, site_id, interval_seconds, next_run):
        if not isinstance(interval_seconds, int) or interval_seconds < 60:
            raise BackupError("SCHEDULE_INTERVAL_INVALID")
        if not isinstance(next_run, int) or next_run < 0:
            raise BackupError("SCHEDULE_TIME_INVALID")
        if not self.repository.site(site_id)["confirmed"]:
            raise BackupError("PROFILE_UNVERIFIED")
        with self.repository.connection() as db:
            db.execute("INSERT INTO schedules VALUES (?,?,?,1) ON CONFLICT(site_id) DO UPDATE SET interval_seconds=excluded.interval_seconds,next_run=excluded.next_run,enabled=1",
                       (site_id, interval_seconds, next_run))

    def disable(self, site_id):
        with self.repository.connection() as db:
            result = db.execute("UPDATE schedules SET enabled=0 WHERE site_id=?", (site_id,))
            if not result.rowcount:
                raise BackupError("SCHEDULE_NOT_FOUND")

    def list(self):
        with self.repository.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM schedules ORDER BY site_id")]

    def tick(self, now=None):
        now = int(time.time()) if now is None else int(now)
        due = [row for row in self.list() if row["enabled"] and row["next_run"] <= now]
        jobs = []
        for row in due:
            slot, site_id = row["next_run"], row["site_id"]
            try:
                job = self.service.run(site_id, f"schedule:{site_id}:{slot}")
            except BackupError as exc:
                jobs.append({"site_id": site_id, "status": "NOT_STARTED", "error_code": exc.code})
                continue
            jobs.append(job)
            if job["status"] in TERMINAL:
                interval = row["interval_seconds"]
                next_run = slot + ((now - slot) // interval + 1) * interval
                with self.repository.connection() as db:
                    db.execute("UPDATE schedules SET next_run=? WHERE site_id=? AND next_run=? AND interval_seconds=?",
                               (next_run, site_id, slot, interval))
        return jobs
