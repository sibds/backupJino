"""Bounded recovery of read-only transfer operations, without repeating exports."""
import time
from datetime import datetime, timedelta, timezone

from .errors import BackupError, CURL_ERROR_KINDS

RETRY_DELAYS = (2, 4, 8, 16, 32)
DEFAULT_WAIT = time.sleep
TRANSIENT_ERRORS = frozenset({"TRANSFER_INTERRUPTED", "CONNECTION_FAILED", "SFTP_CONNECTION_FAILED"})


def retry_call(operation, *, reconnect=None, wait=None, notify=None, phase="FILES_DOWNLOAD", progress=None):
    wait = wait or DEFAULT_WAIT
    notify = notify or (lambda **values: None)
    attempt, total = 0, 0
    committed = progress() if progress else 0
    while True:
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
            details = {}
            if error.details.get("operation") in {"CONNECT", "CWD", "PWD", "MLSD", "RETR"}:
                details["operation"] = error.details["operation"]
            for key, minimum, maximum in (("ftp_reply_code", 100, 599), ("curl_error_code", 1, 999)):
                value = error.details.get(key)
                if type(value) is int and minimum <= value <= maximum:
                    details[key] = value
            if error.details.get("curl_error_kind") in CURL_ERROR_KINDS:
                details["curl_error_kind"] = error.details["curl_error_kind"]
            if progress:
                current = progress()
                advanced = current > committed
                if advanced:
                    attempt = 0
                committed = current
                details.update(committed_bytes=current, progress_advanced=advanced)
            if attempt == len(RETRY_DELAYS):
                notify(stage="RETRY_EXHAUSTED", log=True, error_code=error.code,
                       retry_attempt=attempt, retry_limit=len(RETRY_DELAYS),
                       retry_at=None, retry_delay_seconds=0, retry_total=total, **details)
                raise
            delay = RETRY_DELAYS[attempt]
            attempt += 1
            total += 1
            notify(stage="RETRY_WAIT", log=True, error_code=error.code,
                   retry_attempt=attempt, retry_limit=len(RETRY_DELAYS), retry_total=total,
                   retry_delay_seconds=delay,
                   retry_at=(datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat(), **details)
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
