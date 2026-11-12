CURL_ERROR_KINDS = frozenset({"LOW_SPEED_TIMEOUT", "OPERATION_TIMEOUT", "CONNECTION_RESET",
                            "RECEIVE_ERROR", "SEND_ERROR", "TRUNCATED_TRANSFER", "DNS_ERROR",
                            "CONNECT_ERROR", "TLS_ERROR", "OTHER"})


class BackupError(Exception):
    """Public errors contain codes and safe diagnostics, never upstream exception text."""

    def __init__(self, code, **details):
        self.code = code
        self.details = details
        super().__init__(code)
