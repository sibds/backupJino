class BackupError(Exception):
    """Public errors contain codes and safe diagnostics, never upstream exception text."""

    def __init__(self, code, **details):
        self.code = code
        self.details = details
        super().__init__(code)
