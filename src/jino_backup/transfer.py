"""Read-only FTPS/SFTP sources and bounded file collection."""

import ftplib
import os
import posixpath
import re
import shutil
import ssl
import stat
import time
from dataclasses import dataclass
from pathlib import Path

import certifi

from .errors import BackupError
from .importers import valid_endpoint
from .state import private_dir, safe_root
from .storage import CHUNK, sha256_file
from .retry import retry_call


@dataclass(frozen=True)
class Entry:
    name: str
    kind: str
    size: int
    modified: str | None = None


def name_compatibility_issue(name):
    """Separate skippable Windows names from always-fatal path structure."""
    if (not isinstance(name, str) or not name or name in {".", ".."}
            or re.search(r'[\x00-\x1f\x7f/\\]', name)):
        raise BackupError("REMOTE_PATH_UNSAFE")
    if re.search(r'[\ud800-\udfff]', name):
        return "INVALID_UTF8_NAME"
    if re.search(r'[:<>"|?*]', name):
        return "WINDOWS_CHARACTERS"
    if name[-1] in {".", " "}:
        return "TRAILING_DOT_OR_SPACE"
    if re.fullmatch(r"(?i:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\..*)?", name):
        return "WINDOWS_RESERVED_NAME"
    return None


def display_remote_path(path):
    """Escape undecodable name bytes for private diagnostics, never for commands."""
    return path.encode("utf-8", "backslashreplace").decode("utf-8")


def safe_name(name):
    if name_compatibility_issue(name):
        raise BackupError("REMOTE_PATH_UNSAFE")
    return name


def relative_path(value):
    if value == "":
        return value
    for part in value.split("/"):
        safe_name(part)
    return value


class ListingFTP_TLS(ftplib.FTP_TLS):
    def retrlines(self, cmd, callback=None):
        if cmd.split(" ", 1)[0].upper() != "MLSD":
            return super().retrlines(cmd, callback)
        self.sendcmd("TYPE A")
        with self.transfercmd(cmd) as connection:
            with connection.makefile("r", encoding=self.encoding, errors="surrogateescape") as stream:
                while True:
                    line = stream.readline(self.maxline + 1)
                    if len(line) > self.maxline:
                        raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                    if not line:
                        break
                    if callback:
                        callback(line.removesuffix("\n").removesuffix("\r"))
            if isinstance(connection, ssl.SSLSocket):
                connection.unwrap()
        return self.voidresp()


class FtpsSource:
    def __init__(self, credentials, site):
        self.credentials = credentials
        self.root = safe_root(site["remote_root"])
        self.ftp = None
        self.mlst_supported = True
        self.mlst_checks = 0
        self.stat_listings = 0

    def __enter__(self):
        self.connect()
        return self

    def connect(self):
        self.close()
        # A dedicated CA bundle avoids stale cross-signed intermediates in the
        # Windows store while retaining chain and hostname verification.
        self.ftp = ListingFTP_TLS(
            context=ssl.create_default_context(cafile=certifi.where()), timeout=30
        )
        phase = 'login'
        try:
            self.ftp.connect(self.credentials["host"], self.credentials["port"])
            self.ftp.login(self.credentials["username"], self.credentials["password"])
            phase = 'protection'
            self.ftp.prot_p()
            phase = 'root'
            self.ftp.cwd(self.root)
            self.canonical_root = self.ftp.pwd().rstrip("/") or "/"
        except ftplib.error_perm:
            self.close()
            code = {'login':'FTP_LOGIN_DENIED','protection':'FTP_DATA_PROTECTION_DENIED','root':'FTP_ROOT_DENIED'}[phase]
            raise BackupError(code) from None
        except ssl.SSLCertVerificationError:
            self.close()
            raise BackupError("TLS_CERTIFICATE_INVALID") from None
        except ssl.SSLError:
            self.close()
            raise BackupError("TLS_HANDSHAKE_FAILED") from None
        except (OSError, EOFError, ftplib.Error):
            self.close()
            raise BackupError("CONNECTION_FAILED") from None

    def close(self):
        if self.ftp is not None:
            self.ftp.close()
            self.ftp = None

    def reconnect(self):
        self.connect()

    def __exit__(self, *args):
        self.close()

    def _cwd(self, relative):
        relative_path(relative)
        path = posixpath.join(self.canonical_root, relative)
        try:
            self.ftp.cwd(path)
        except ftplib.error_perm as error:
            raise self._listing_rejection(error, "CWD") from None
        try:
            canonical = self.ftp.pwd().rstrip("/") or "/"
        except ftplib.error_perm as error:
            raise self._listing_rejection(error, "PWD") from None
        if self.canonical_root != "/" and canonical != self.canonical_root and not canonical.startswith(self.canonical_root + "/"):
            raise BackupError("REMOTE_PATH_UNSAFE")
        return canonical

    @staticmethod
    def _listing_rejection(error, operation):
        reply = str(error)[:3]
        details = {"operation": operation}
        if reply.isascii() and reply.isdigit():
            details["ftp_reply_code"] = int(reply)
        code = (
            "REMOTE_LIST_UNSUPPORTED" if operation == "MLSD" and reply in {"500", "502", "504"}
            else "REMOTE_DIRECTORY_UNAVAILABLE" if reply == "550"
            else "FTP_LOGIN_DENIED" if reply in {"530", "532"}
            else "FTP_LIST_REJECTED"
        )
        return BackupError(code, **details)

    def listdir(self, relative):
        operation = "CWD"
        try:
            self._cwd(relative)
            operation = "MLSD"
            for name, facts in self.ftp.mlsd():
                if any(re.search(r'[\ud800-\udfff]', key + value) for key, value in facts.items()):
                    raise ValueError("INVALID_MLSD_FACTS")
                kind = facts.get("type", "")
                if kind in {"cdir", "pdir"}:
                    continue
                yield Entry(name, kind, int(facts.get("size", "0")), facts.get("modify"))
        except ssl.SSLCertVerificationError:
            self.close()
            raise BackupError("TLS_CERTIFICATE_INVALID") from None
        except ftplib.error_perm as error:
            raise self._listing_rejection(error, operation) from None
        except UnicodeDecodeError:
            self.close()
            raise BackupError("REMOTE_LIST_ENCODING_INVALID", operation=operation) from None
        except ValueError:
            raise BackupError("REMOTE_LIST_INVALID", operation=operation) from None
        except (OSError, EOFError, ftplib.Error) as error:
            self.close()
            details = {"operation": operation}
            reply = str(error)[:3] if isinstance(error, ftplib.Error) else ""
            if reply.isascii() and reply.isdigit():
                details["ftp_reply_code"] = int(reply)
            raise BackupError("TRANSFER_INTERRUPTED", **details) from None

    def stat(self, relative):
        parent, name = posixpath.split(relative_path(relative))
        if self.mlst_supported:
            operation = 'CWD'
            try:
                canonical = self._cwd(parent)
                operation = 'MLST'
                try:
                    reply = self.ftp.sendcmd('MLST ' + name)
                except ftplib.error_perm as error:
                    if str(error)[:3] not in {'500','502','504'}:
                        raise self._listing_rejection(error, operation) from None
                    self.mlst_supported = False
                else:
                    lines = [line[1:] for line in reply.splitlines() if line.startswith(' ')]
                    if len(lines) != 1:
                        raise BackupError('REMOTE_LIST_INVALID', operation=operation)
                    raw, separator, reported = lines[0].partition(' ')
                    if not separator or not raw.endswith(';'):
                        raise BackupError('REMOTE_LIST_INVALID', operation=operation)
                    facts = {}
                    for fact in raw[:-1].split(';'):
                        key, equals, value = fact.partition('=')
                        key = key.lower()
                        if not equals or not key or key in facts:
                            raise BackupError('REMOTE_LIST_INVALID', operation=operation)
                        facts[key] = value
                    if reported not in {name, posixpath.join(canonical, name)}:
                        raise BackupError('SOURCE_CHANGED', operation=operation)
                    if not {'type','modify'} <= facts.keys() or facts.get('type','').lower() == 'file' and 'size' not in facts:
                        self.mlst_supported = False  # Insufficient facts: use the strict MLSD fallback.
                    else:
                        size = int(facts.get('size', '0'))
                        if size < 0:
                            raise ValueError('INVALID_SIZE')
                        self.mlst_checks += 1
                        return Entry(name, facts['type'].lower(), size, facts['modify'])
            except ssl.SSLCertVerificationError:
                self.close()
                raise BackupError('TLS_CERTIFICATE_INVALID') from None
            except UnicodeDecodeError:
                self.close()
                raise BackupError('REMOTE_LIST_ENCODING_INVALID', operation=operation) from None
            except ValueError:
                raise BackupError('REMOTE_LIST_INVALID', operation=operation) from None
            except (OSError, EOFError, ftplib.Error) as error:
                self.close()
                details = {'operation':operation}
                code = str(error)[:3] if isinstance(error, ftplib.Error) else ''
                if code.isascii() and code.isdigit(): details['ftp_reply_code'] = int(code)
                raise BackupError('TRANSFER_INTERRUPTED', **details) from None
        self.stat_listings += 1
        matches = [entry for entry in self.listdir(parent) if entry.name == name]
        if len(matches) != 1:
            raise BackupError("SOURCE_CHANGED")
        return matches[0]

    def download(self, relative, outgoing, offset):
        try:
            parent, name = posixpath.split(relative_path(relative))
            self._cwd(parent)
            self.ftp.retrbinary("RETR " + name, outgoing.write, CHUNK, rest=offset or None)
        except ssl.SSLCertVerificationError:
            self.close()
            raise BackupError("TLS_CERTIFICATE_INVALID") from None
        except ftplib.error_perm:
            raise BackupError("TRANSFER_DENIED") from None
        except (OSError, EOFError, ftplib.Error):
            # A failed data connection leaves the control stream ambiguous.
            self.close()
            raise BackupError("TRANSFER_INTERRUPTED") from None


class SftpSource:
    def __init__(self, credentials, site):
        self.credentials, self.site = credentials, site
        self.client = self.sftp = None

    def __enter__(self):
        try:
            import paramiko
        except ImportError:
            raise BackupError("SFTP_DEPENDENCY_MISSING") from None
        if not self.site.get("known_hosts"):
            raise BackupError("KNOWN_HOSTS_REQUIRED")
        self.client = paramiko.SSHClient()
        try:
            self.client.load_host_keys(self.site["known_hosts"])
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
            self.client.connect(self.credentials["host"], port=self.credentials["port"],
                                username=self.credentials["username"], password=self.credentials["password"],
                                timeout=30, auth_timeout=30, banner_timeout=30,
                                allow_agent=False, look_for_keys=False)
            self.sftp = self.client.open_sftp()
            self.sftp.get_channel().settimeout(30)
            self.root = self.sftp.normalize(safe_root(self.site["remote_root"]))
            return self
        except paramiko.AuthenticationException:
            self.__exit__()
            raise BackupError("AUTH_FAILED") from None
        except paramiko.BadHostKeyException:
            self.__exit__()
            raise BackupError("HOST_KEY_INVALID") from None
        except Exception:
            self.__exit__()
            raise BackupError("SFTP_CONNECTION_FAILED") from None

    def __exit__(self, *args):
        try:
            if self.sftp is not None:
                self.sftp.close()
        finally:
            if self.client is not None:
                self.client.close()

    def _path(self, relative):
        path = posixpath.join(self.root, relative_path(relative))
        canonical = self.sftp.normalize(path)
        if canonical != self.root and not canonical.startswith(self.root.rstrip("/") + "/"):
            raise BackupError("REMOTE_PATH_UNSAFE")
        return path

    @staticmethod
    def _entry(name, info):
        mode = info.st_mode or 0
        kind = "dir" if stat.S_ISDIR(mode) else "file" if stat.S_ISREG(mode) else "unsafe"
        return Entry(name, kind, info.st_size or 0, str(info.st_mtime))

    def listdir(self, relative):
        try:
            for info in self.sftp.listdir_iter(self._path(relative)):
                yield self._entry(info.filename, info)
        except OSError:
            raise BackupError("TRANSFER_INTERRUPTED") from None

    def stat(self, relative):
        try:
            return self._entry(posixpath.basename(relative), self.sftp.lstat(self._path(relative)))
        except OSError:
            raise BackupError("TRANSFER_INTERRUPTED") from None

    def download(self, relative, outgoing, offset):
        try:
            with self.sftp.open(self._path(relative), "rb") as incoming:
                incoming.seek(offset)
                shutil.copyfileobj(incoming, outgoing, CHUNK)
        except OSError:
            raise BackupError("TRANSFER_INTERRUPTED") from None


def source_for(credentials, site):
    if not valid_endpoint(credentials):
        raise BackupError("FTP_PROFILE_NEEDS_REVIEW")
    return SftpSource(credentials, site) if credentials["protocol"] == "sftp" else FtpsSource(credentials, site)


class BoundedWriter:
    def __init__(self, stream, limit, deadline):
        self.stream, self.limit, self.deadline = stream, limit, deadline
        self.written = 0

    def write(self, data):
        if time.monotonic() > self.deadline:
            raise BackupError("TRANSFER_TIMEOUT")
        if self.written + len(data) > self.limit:
            raise BackupError("SOURCE_CHANGED")
        self.written += len(data)
        return self.stream.write(data)


def collect_files(source, destination, *, max_bytes, max_files, timeout_seconds, checkpoint=None,continue_on_error=False,
                  skip_unsupported_names=False):
    destination = Path(destination)
    private_dir(destination)
    deadline = time.monotonic() + timeout_seconds
    queue, seen, files, directories = [("", 0)], set(), [], []
    total = planned = 0
    errors=[]
    skipped=[]
    while queue:
        parent, depth = queue.pop()
        if depth > 64:
            raise BackupError("TRANSFER_LIMIT_EXCEEDED")
        try:
            entries = list(source.listdir(parent)) if hasattr(source, 'retry_file') else retry_call(
                lambda: list(source.listdir(parent)), reconnect=getattr(source, 'reconnect', None), phase='SCANNING')
        except BackupError as error:
            if not parent or not continue_on_error or error.code != "REMOTE_DIRECTORY_UNAVAILABLE":
                raise
            errors.append({'path':parent, 'kind':'dir', 'error_code':error.code, **error.details})
            continue
        for entry in entries:
            if time.monotonic() > deadline:
                raise BackupError("TRANSFER_TIMEOUT")
            issue = name_compatibility_issue(entry.name)
            if issue and not skip_unsupported_names:
                raise BackupError("REMOTE_PATH_UNSAFE")
            relative = posixpath.join(parent, entry.name)
            if relative.casefold() in seen:
                raise BackupError("REMOTE_PATH_COLLISION")
            seen.add(relative.casefold())
            if len(seen) > max_files:
                raise BackupError("TRANSFER_LIMIT_EXCEEDED")
            if issue:
                omission = {'path':display_remote_path(relative), 'kind':entry.kind, 'reason':issue,
                            'error_code':'REMOTE_NAME_UNSUPPORTED'}
                skipped.append(omission)
                errors.append(dict(omission))
                continue
            local = destination.joinpath(*relative.split("/"))
            if entry.kind == "dir":
                private_dir(local)
                directories.append(relative)
                queue.append((relative, depth + 1))
                continue
            if entry.kind != "file":
                raise BackupError("REMOTE_TYPE_UNSAFE")
            planned+=entry.size
            if entry.size < 0 or planned > max_bytes:
                raise BackupError("TRANSFER_LIMIT_EXCEEDED")
            required_space = entry.size if checkpoint is not None else 3 * (total + entry.size)
            if shutil.disk_usage(destination).free < required_space + 1024 * 1024:
                raise BackupError("STORAGE_SPACE_LOW")
            try:
                def copy_once():
                    if time.monotonic() > deadline:
                        raise BackupError("TRANSFER_TIMEOUT")
                    current = source.stat(relative)
                    if current != entry:
                        raise BackupError("SOURCE_CHANGED")
                    if checkpoint is not None:
                        checkpoint.fetch(source, relative, entry, local, deadline)
                    else:
                        offset = local.stat().st_size if local.exists() and entry.modified else 0
                        with local.open("ab" if offset else "wb") as stream:
                            os.chmod(local, 0o600)
                            source.download(relative, BoundedWriter(stream, entry.size - offset, deadline), offset)
                    if local.stat().st_size != entry.size or source.stat(relative) != entry:
                        raise BackupError("SOURCE_CHANGED")
                recover = getattr(source, 'retry_file', None)
                if recover:
                    recover(copy_once)
                else:
                    retry_call(copy_once, reconnect=getattr(source, 'reconnect', None))
            except BackupError as exc:
                if not continue_on_error or exc.code not in {'SOURCE_CHANGED','TRANSFER_DENIED',
                        'LOCAL_FILE_CHANGED','LOCAL_PROGRESS_INVALID'}:
                    raise
                errors.append({'path':relative,'error_code':exc.code})
                continue
            files.append({"path": relative, "size_bytes": entry.size, "sha256": sha256_file(local)})
            total += entry.size
    if not files and not errors:
        raise BackupError("FILES_MISSING")
    if checkpoint is not None and not errors:
        checkpoint.validate_inventory(files)
    result={"count": len(files), "bytes": total, "items": files, "directories": directories}
    if errors:
        result['errors']=errors
    if skipped:
        result['skipped']=skipped
    return result
