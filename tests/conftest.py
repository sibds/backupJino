import pytest


@pytest.fixture(autouse=True)
def no_real_retry_backoff(monkeypatch):
    """Tests record backoff values explicitly; only production waits in real time."""
    monkeypatch.setattr("jino_backup.retry.DEFAULT_WAIT", lambda seconds: None)
