"""One verified FTPS download session per worker; metadata stays root-checked."""
import ftplib
import posixpath
from urllib.parse import quote

import certifi
import pycurl

from .errors import BackupError, CURL_ERROR_KINDS
from .transfer import relative_path


def curl_error_kind(number, message):
    # Inspect known markers locally; never retain the upstream text (may contain secrets).
    message = message.lower()
    if number == 28:
        return "LOW_SPEED_TIMEOUT" if "operation too slow" in message else "OPERATION_TIMEOUT"
    if number == 56:
        return "CONNECTION_RESET" if "reset" in message else "RECEIVE_ERROR"
    return {55: "SEND_ERROR", 18: "TRUNCATED_TRANSFER", 6: "DNS_ERROR", 7: "CONNECT_ERROR",
            35: "TLS_ERROR", 60: "TLS_ERROR"}.get(number, "OTHER")


class CurlFileSource:
    def __init__(self, credentials, site, metadata, control):
        self.credentials, self.site = credentials, site
        self.metadata, self.control = metadata, control
        self.handle = None

    def __enter__(self):
        self.handle = pycurl.Curl()
        return self

    def __exit__(self, *args):
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def reconnect(self):
        self.__exit__()
        self.__enter__()

    def stat(self, relative):
        return self.metadata.stat(relative)

    def download(self, relative, outgoing, offset):
        relative_path(relative)
        root = self.metadata.canonical_root
        absolute = posixpath.join(root, relative)
        parent = posixpath.dirname(absolute)
        host = self.credentials["host"].encode("idna").decode("ascii")
        authority = "[" + host + "]" if ":" in host else host
        url = "ftp://" + authority + ":" + str(self.credentials["port"]) + "/" + quote(absolute, safe="/")
        handle = self.handle
        handle.reset()  # libcurl keeps this worker's connection cache.
        for option, value in (
            (pycurl.URL, url), (pycurl.USERNAME, self.credentials["username"].encode("utf-8")),
            (pycurl.PASSWORD, self.credentials["password"].encode("utf-8")),
            (pycurl.USE_SSL, pycurl.USESSL_ALL), (pycurl.SSL_VERIFYPEER, 1),
            (pycurl.SSL_VERIFYHOST, 2), (pycurl.CAINFO, certifi.where()),
            (pycurl.PROTOCOLS, pycurl.PROTO_FTP), (pycurl.FOLLOWLOCATION, 0),
            (pycurl.CONNECTTIMEOUT, 30), (pycurl.LOW_SPEED_LIMIT, 1),
            (pycurl.LOW_SPEED_TIME, 30), (pycurl.NOSIGNAL, 1),
            (pycurl.FTP_FILEMETHOD, pycurl.FTPMETHOD_NOCWD), (pycurl.MAXCONNECTS, 1),
            (pycurl.QUOTE, [("CWD " + parent).encode("utf-8"), b"PWD"]),
            (pycurl.RESUME_FROM_LARGE, offset), (pycurl.NOPROGRESS, False),
        ):
            handle.setopt(option, value)
        callback_error = None
        operation, armed, validated, reply = "CWD", False, False, None

        def header(line):
            nonlocal callback_error, armed, validated, operation, reply
            try:
                if len(line) >= 3 and line[:3].isdigit():
                    reply = int(line[:3])
                if line.startswith(b"250 ") and not validated:
                    armed = True
                    operation = "PWD"
                if armed and line.startswith(b"257 "):
                    canonical = ftplib.parse257(line.decode("utf-8")).rstrip("/") or "/"
                    if root != "/" and canonical != root and not canonical.startswith(root + "/"):
                        raise BackupError("REMOTE_PATH_UNSAFE")
                    validated = True
                    operation = "RETR"
                return len(line)
            except BaseException as error:
                callback_error = error
                return 0

        def write(chunk):
            nonlocal callback_error
            try:
                self.control.checkpoint()
                if not validated:
                    raise BackupError("FTP_ROOT_UNVERIFIED")
                return outgoing.write(chunk)
            except BaseException as error:
                callback_error = error
                return 0

        def progress(*args):
            nonlocal callback_error
            try:
                self.control.checkpoint()
                return 0
            except BaseException as error:
                callback_error = error
                return 1

        handle.setopt(pycurl.HEADERFUNCTION, header)
        handle.setopt(pycurl.WRITEFUNCTION, write)
        handle.setopt(pycurl.XFERINFOFUNCTION, progress)
        try:
            handle.perform()
            if not validated:
                raise BackupError("FTP_ROOT_UNVERIFIED")
        except pycurl.error as error:
            if callback_error:
                raise callback_error from None
            number = error.args[0]
            details = {"operation": operation, "curl_error_code": number,
                       "curl_error_kind": curl_error_kind(number, str(error.args[1]) if len(error.args) > 1 else "")}
            if reply is not None and 400 <= reply <= 599:
                details["ftp_reply_code"] = reply
            code = (
                "TLS_CERTIFICATE_INVALID" if number == pycurl.E_PEER_FAILED_VERIFICATION
                else "TLS_HANDSHAKE_FAILED" if number == pycurl.E_SSL_CONNECT_ERROR
                else "TRANSFER_INTERRUPTED" if reply is not None and 400 <= reply < 500
                else "FTP_LOGIN_DENIED" if number == pycurl.E_LOGIN_DENIED
                else "REMOTE_DIRECTORY_UNAVAILABLE" if number == pycurl.E_QUOTE_ERROR and reply == 550
                else "FTP_ROOT_UNVERIFIED" if number == pycurl.E_QUOTE_ERROR
                else "TRANSFER_DENIED" if number in {pycurl.E_REMOTE_ACCESS_DENIED, pycurl.E_REMOTE_FILE_NOT_FOUND}
                else "TRANSFER_INTERRUPTED"
            )
            raise BackupError(code, **details) from None
