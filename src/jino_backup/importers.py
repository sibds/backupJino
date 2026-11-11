"""Strict offline imports; candidate secrets must never be serialized into reports."""

import base64
import binascii
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from .errors import BackupError

TABLE = "abw_sibds__server"
MAX_IMPORT_BYTES = 32 * 1024 * 1024


@dataclass
class Candidate:
    profile_id: str
    kind: str
    credentials: dict = field(repr=False)


@dataclass
class ImportBatch:
    report: dict
    candidates: list[Candidate] = field(default_factory=list, repr=False)


def read_source(path):
    path = Path(path)
    with path.open("rb") as stream:
        data = stream.read(MAX_IMPORT_BYTES + 1)
    if len(data) > MAX_IMPORT_BYTES:
        raise BackupError("SOURCE_TOO_LARGE")
    return data


def jino_rows(path):
    try:
        data = json.loads(read_source(path).decode("utf-8-sig"))
    except (ValueError, UnicodeError):
        raise BackupError("SOURCE_JSON_INVALID") from None
    if isinstance(data, list):
        tables = [x for x in data if isinstance(x, dict) and x.get("type") == "table"]
        if tables:
            selected = [x for x in tables if x.get("name") == TABLE]
            if len(selected) != 1:
                raise BackupError("SOURCE_TABLE_AMBIGUOUS")
            rows = selected[0].get("data")
        elif any(isinstance(x, dict) and x.get("type") in {"header", "database"} for x in data):
            raise BackupError("SOURCE_TABLE_AMBIGUOUS")
        else:
            rows = data
    elif isinstance(data, dict) and isinstance(data.get("data"), list):
        if ((data.get("type") == "table" and data.get("name") != TABLE)
                or ("name" in data and data["name"] != TABLE)):
            raise BackupError("SOURCE_TABLE_AMBIGUOUS")
        rows = data["data"]
    else:
        raise BackupError("SOURCE_FORMAT_INVALID")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise BackupError("SOURCE_FORMAT_INVALID")
    return rows


def inspect_jino(path):
    rows = jino_rows(path)
    fields = sorted({key for row in rows for key in row})
    return {"rows_total": len(rows), "fields": {
        key: {"types": dict(Counter(type(row.get(key)).__name__ for row in rows)),
              "empty": sum(row.get(key) in (None, "") for row in rows),
              "empty_fraction": sum(row.get(key) in (None, "") for row in rows) / len(rows)}
        for key in fields
    }}


def identity(prefix, *values):
    digest = hashlib.sha256(json.dumps(values, ensure_ascii=True).encode()).hexdigest()[:24]
    return f"{prefix}-{digest}"


def is_hash(value):
    return bool(re.match(r"^\$(2[aby]\$|argon2|scrypt\$|[156]\$)", value)
                or re.fullmatch(r"[a-fA-F0-9]{32,128}", value))


def valid_endpoint(credentials):
    host = credentials.get("host", "")
    user = credentials.get("username", "")
    return (credentials.get("protocol") in {"ftps", "sftp"}
            and isinstance(host, str) and bool(host) and not re.search(r"[\s/@\\\x00-\x1f]", host)
            and isinstance(user, str) and bool(user) and not re.search(r"[\r\n\x00]", user)
            and isinstance(credentials.get("port"), int) and 1 <= credentials["port"] <= 65535)


def parse_jino(path, mapping):
    rows = jino_rows(path)
    required = ("id", "login", "password")
    if not isinstance(mapping, dict) or any(not isinstance(mapping.get(key), str) for key in required):
        raise BackupError("MAPPING_INVALID")
    if len({mapping[key] for key in required}) != len(required):
        raise BackupError("MAPPING_INVALID")
    if any(mapping[key] not in {k for r in rows for k in r} for key in required) and rows:
        raise BackupError("MAPPING_INVALID")
    get = lambda row, key: row.get(mapping.get(key, ""), "")
    logins = Counter(str(get(row, "login")).strip() for row in rows)
    ids = Counter(str(get(row, "id")) for row in rows)
    report = {"rows_total": len(rows), "accounts_valid": 0, "duplicate_accounts": 0,
              "missing_credentials": 0, "unrecoverable_credentials": 0, "issues": []}
    batch = ImportBatch(report)
    for index, row in enumerate(rows, 1):
        source_id, login, password = (get(row, key) for key in required)
        code = None
        if not all(isinstance(v, str) and v.strip() for v in (source_id, login, password)):
            report["missing_credentials"] += 1
            code = "CREDENTIAL_MISSING"
        elif logins[login.strip()] > 1 or ids[source_id] > 1:
            report["duplicate_accounts"] += 1
            code = "ACCOUNT_AMBIGUOUS"
        elif is_hash(password):
            report["unrecoverable_credentials"] += 1
            code = "CREDENTIAL_NOT_RECOVERABLE"
        if code:
            report["issues"].append({"row": index, "code": code})
            continue
        batch.candidates.append(Candidate(identity("panel", source_id), "panel", {
            "username": login.strip(), "password": password,
        }))
        report["accounts_valid"] += 1
        if all(key in mapping for key in ("mysql_host", "mysql_login", "mysql_password")):
            credentials = {"host": get(row, "mysql_host"), "username": get(row, "mysql_login"),
                           "password": get(row, "mysql_password"), "port": mapping.get("mysql_port", 3306)}
            if (valid_endpoint({**credentials, "protocol": "ftps"})
                    and isinstance(credentials["password"], str) and credentials["password"]
                    and not is_hash(credentials["password"])):
                batch.candidates.append(Candidate(identity("mysql", source_id), "mysql", credentials))
            else:
                report["issues"].append({"row": index, "code": "MYSQL_PROFILE_NEEDS_REVIEW"})
        if all(key in mapping for key in ("ftp_host", "ftp_login", "ftp_password", "ftp_protocol")):
            credentials = {"host": get(row, "ftp_host"), "username": get(row, "ftp_login"),
                           "password": get(row, "ftp_password"), "protocol": mapping["ftp_protocol"],
                           "port": mapping.get("ftp_port", 21)}
            if valid_endpoint(credentials) and isinstance(credentials["password"], str) and credentials["password"] and not is_hash(credentials["password"]):
                batch.candidates.append(Candidate(identity("ftp", source_id), "ftp", credentials))
            else:
                report["issues"].append({"row": index, "code": "FTP_PROFILE_NEEDS_REVIEW"})
    report["profiles"] = [{"profile_id": c.profile_id, "kind": c.kind} for c in batch.candidates]
    return batch


def parse_filezilla(path):
    raw = read_source(path)
    # Decode before checking declarations so UTF-16 cannot bypass this check.
    try:
        text = raw.decode("utf-16" if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig")
    except UnicodeError:
        raise BackupError("SOURCE_XML_INVALID") from None
    if re.search(r"<!\s*(DOCTYPE|ENTITY)", text, re.IGNORECASE):
        raise BackupError("SOURCE_XML_UNSAFE")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise BackupError("SOURCE_XML_INVALID", line=exc.position[0], column=exc.position[1]) from None
    if root.tag != "FileZilla3":
        raise BackupError("SOURCE_FORMAT_INVALID")
    servers = root.findall(".//Server")
    report = {"profiles_total": len(servers), "profiles_valid": 0, "needs_review": 0, "issues": []}
    batch = ImportBatch(report)
    seen = Counter()
    pending = []
    for index, server in enumerate(servers, 1):
        # FileZilla enum: 0=opportunistic FTP, 1=SFTP, 3=implicit TLS, 4=explicit TLS.
        protocol = {"1": "sftp", "4": "ftps"}.get(server.findtext("Protocol", "0"))
        password_node = server.find("Pass")
        code = "PROTOCOL_NEEDS_REVIEW" if protocol is None else None
        password = ""
        if not code:
            try:
                if password_node is None or password_node.get("encoding") != "base64":
                    raise ValueError
                password = base64.b64decode(password_node.text or "", validate=True).decode("utf-8")
                if not password:
                    raise ValueError
            except (ValueError, UnicodeError, binascii.Error):
                code = "CREDENTIAL_ENCODING_INVALID"
        try:
            port = int(server.findtext("Port", "22" if protocol == "sftp" else "21"))
        except ValueError:
            port = 0
        credentials = {"host": server.findtext("Host", ""), "port": port,
                       "username": server.findtext("User", ""), "protocol": protocol,
                       "password": password}
        if not code and not valid_endpoint(credentials):
            code = "FTP_PROFILE_NEEDS_REVIEW"
        profile_id = identity("fz", credentials["host"], port, credentials["username"])
        if code:
            report["issues"].append({"row": index, "code": code})
        else:
            seen[profile_id] += 1
            pending.append((index, Candidate(profile_id, "ftp", credentials)))
    for index, candidate in pending:
        if seen[candidate.profile_id] > 1:
            report["issues"].append({"row": index, "code": "ACCOUNT_AMBIGUOUS"})
        else:
            batch.candidates.append(candidate)
    report["profiles_valid"] = len(batch.candidates)
    report["needs_review"] = len(servers) - len(batch.candidates)
    report["profiles"] = [{"profile_id": c.profile_id, "kind": c.kind} for c in batch.candidates]
    return batch
