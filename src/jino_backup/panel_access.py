"""Persistent rate limits for normal panel logins and an operator CAPTCHA stop."""

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .errors import BackupError


def iso_time(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


class PanelAccessPolicy:
    interval_seconds = 3600
    window_seconds = 10800
    window_limit = 3

    def __init__(self, state_dir, *, clock=time.time):
        self.path = Path(state_dir) / "panel-access.sqlite3"
        self.clock = clock
        with self.connection() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY, started REAL NOT NULL, row_id INTEGER, purpose TEXT)""")
            db.execute("""CREATE TABLE IF NOT EXISTS gate (
                id INTEGER PRIMARY KEY CHECK(id=1), blocked INTEGER, row_id INTEGER, reason TEXT)""")
            db.execute("INSERT OR IGNORE INTO gate VALUES (1,0,NULL,NULL)")

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    def _snapshot(self, db):
        now = self.clock()
        blocked, row, reason = db.execute("SELECT blocked,row_id,reason FROM gate WHERE id=1").fetchone()
        recent = [record[0] for record in db.execute(
            "SELECT started FROM attempts WHERE started>? ORDER BY started", (now - self.window_seconds,))]
        last = db.execute("SELECT MAX(started) FROM attempts").fetchone()[0]
        due = max(now, last + self.interval_seconds) if last is not None else now
        if len(recent) >= self.window_limit:
            due = max(due, recent[-self.window_limit] + self.window_seconds)
        return {"allowed": not blocked and due <= now, "blocked": bool(blocked),
                "blocked_row": row, "reason": reason, "next_at": None if blocked else iso_time(due),
                "next_timestamp": due, "interval_seconds": self.interval_seconds,
                "window_seconds": self.window_seconds, "window_limit": self.window_limit,
                "recent_attempts": len(recent)}

    def snapshot(self):
        with self.connection() as db:
            return self._snapshot(db)

    def reserve(self, row, purpose):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._snapshot(db)
            if state["blocked"]:
                raise BackupError("PANEL_AUTH_PAUSED")
            if not state["allowed"]:
                raise BackupError("PANEL_RATE_LIMIT", next_at=state["next_at"])
            db.execute("INSERT INTO attempts(started,row_id,purpose) VALUES (?,?,?)", (self.clock(), row, purpose))

    def block(self, row, reason="CAPTCHA_REQUIRED"):
        with self.connection() as db:
            db.execute("UPDATE gate SET blocked=1,row_id=?,reason=? WHERE id=1", (row, reason))

    def resume(self):
        with self.connection() as db:
            db.execute("UPDATE gate SET blocked=0,row_id=NULL,reason=NULL WHERE id=1")
