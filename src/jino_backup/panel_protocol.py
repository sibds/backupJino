"""Strict allowlist for the observed SQL-to-directory operation."""

import re
from pathlib import Path

from .errors import BackupError


RESTORE_QUERY='''mutation RestoreDatabaseBackup($name: String!, $serverId: String!, $db: String!, $archive: Boolean, $accountId: String) {
 me { legacy { restoreMySQLBackup(name: $name, serverId: $serverId, db: $db, archive: $archive, accountId: $accountId, validateAll: true, camelCaseErrors: true) } }
}'''
TOKEN_QUERY='''mutation CreateFiletransferToken($serviceName: String, $objectId: String) {
 me { legacy { createFiletransferToken(serviceName: $serviceName, objectId: $objectId) } }
}'''


def validate_panel_sql(path):
    path=Path(path)
    size=path.stat().st_size
    with path.open('rb') as stream:
        head=stream.read(min(8192,size))
        stream.seek(max(0,size-8192))
        tail=stream.read(8192)
    if head.startswith(b'-- MySQL dump ') and b'-- Dump completed on ' in tail:
        return
    settings=('CHARACTER_SET_CLIENT','CHARACTER_SET_RESULTS','COLLATION_CONNECTION','TIME_ZONE',
              'UNIQUE_CHECKS','FOREIGN_KEY_CHECKS','SQL_MODE','SQL_NOTES')
    if (not head.lstrip().startswith((b'/*!',b'/*M!'))
            or not all(('@OLD_'+n+'=').encode() in head and (n+'=@OLD_'+n).encode() in tail for n in settings)
            or not tail.rstrip().splitlines()
            or not re.fullmatch(rb'/\*![0-9]+ SET SQL_NOTES=@OLD_SQL_NOTES \*/;',tail.rstrip().splitlines()[-1])):
        raise BackupError('DB_INTEGRITY_FAILED')


def export_payload_allowed(payload,variables):
    return (isinstance(payload,dict) and payload.get('variables')==variables
            and re.sub(r'\s+','',payload.get('query',''))==re.sub(r'\s+','',RESTORE_QUERY))


def check_owned_listing(items,owned_paths):
    if any(item.get('path') not in owned_paths for item in items):
        raise BackupError('REMOTE_DESTINATION_EXISTS')


def new_export(items,baseline):
    new=[item for item in items if item.get('path') not in baseline]
    # Provider jobs can expose a growing temporary file before atomically naming the SQL.
    # Do not download or treat those files as completed exports; the caller keeps polling.
    new=[item for item in new if not (item.get('type')=='file'
         and item.get('targetType') is None and item.get('targetDisplayPath') is None
         and (not str(item.get('name','')).endswith('.sql') or item.get('size')==0))]
    if not new:
        return None
    if len(new)!=1:
        raise BackupError('EXPORT_RESULT_AMBIGUOUS')
    item=new[0]
    if (item.get('type')!='file' or item.get('targetType') is not None
            or item.get('targetDisplayPath') is not None
            or not isinstance(item.get('path'),str) or not item['path']
            or not str(item.get('name','')).endswith('.sql')
            or not isinstance(item.get('size'),int) or item['size']<=0):
        raise BackupError('EXPORT_RESULT_UNVERIFIED',features={
            'regular_file':item.get('type')=='file','sql_suffix':str(item.get('name','')).endswith('.sql'),
            'gzip_suffix':str(item.get('name','')).endswith('.gz'),
            'has_link_target':item.get('targetType') is not None or item.get('targetDisplayPath') is not None,
            'size_is_int':isinstance(item.get('size'),int),
            'size_bytes':item['size'] if isinstance(item.get('size'),int) else -1})
    return item
