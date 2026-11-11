"""Bounded recovery of read-only transfer operations, without repeating exports."""
import time
from datetime import datetime, timedelta, timezone

from .errors import BackupError

RETRY_DELAYS = (2, 4, 8, 16, 32)
DEFAULT_WAIT = time.sleep
TRANSIENT_ERRORS = frozenset({"TRANSFER_INTERRUPTED", "CONNECTION_FAILED", "SFTP_CONNECTION_FAILED"})


def retry_call(operation, *, reconnect=None, wait=None, notify=None, phase="FILES_DOWNLOAD"):
    wait = wait or DEFAULT_WAIT
    notify = notify or (lambda **values: None)
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            if attempt and reconnect:
                notify(stage="RECONNECT", log=True, retry_attempt=attempt, retry_limit=len(RETRY_DELAYS))
                reconnect()
            result = operation()
            if attempt:
                notify(stage=phase, log=True, retry_at=None, retry_delay_seconds=0)
            return result
        except BackupError as error:
            if error.code not in TRANSIENT_ERRORS:
                raise
            if attempt == len(RETRY_DELAYS):
                notify(stage="RETRY_EXHAUSTED", log=True, error_code=error.code,
                       retry_attempt=attempt, retry_limit=len(RETRY_DELAYS),
                       retry_at=None, retry_delay_seconds=0)
                raise
            delay = RETRY_DELAYS[attempt]
            notify(stage="RETRY_WAIT", log=True, error_code=error.code,
                   retry_attempt=attempt + 1, retry_limit=len(RETRY_DELAYS),
                   retry_delay_seconds=delay,
                   retry_at=(datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat())
            wait(delay)


class RecoveringSource:
    """Retry individual directory reads; file retries reopen their checkpoints."""
    def __init__(self, source, *, wait=None, notify=None):
        self.source, self.wait, self.notify = source, wait, notify

    def __getattr__(self, name):
        return getattr(self.source, name)

    def __enter__(self):
        retry_call(self.source.__enter__, wait=self.wait, notify=self.notify, phase="SCANNING")
        return self

    def __exit__(self, *args):
        return self.source.__exit__(*args)

    def reconnect(self):
        if hasattr(self.source, "reconnect"):
            self.source.reconnect()
        else:
            self.source.__exit__()
            self.source.__enter__()

    def listdir(self, relative):
        return retry_call(lambda: list(self.source.listdir(relative)), reconnect=self.reconnect,
                          wait=self.wait, notify=self.notify, phase="SCANNING")

    def retry_file(self, operation):
        return retry_call(operation, reconnect=self.reconnect, wait=self.wait, notify=self.notify)
