"""Readable local folders; login values never enter public reports."""

import json
import re
import unicodedata
from collections import Counter
from contextlib import ExitStack, contextmanager
from pathlib import Path

from .downloads import FileLock,local_path
from .errors import BackupError
from .importers import identity
from .state import safe_id,write_json


INVALID=re.compile(r'[<>:"/\\|?*\x00-\x1f]')
RESERVED={'CON','PRN','AUX','NUL','CLOCK$',*(f'{p}{n}' for p in ('COM','LPT') for n in '123456789¹²³')}


def component(value):
    if not isinstance(value,str) or not value.strip():
        raise BackupError('SERVER_LOGIN_MISSING')
    value=INVALID.sub('_',unicodedata.normalize('NFC',value)).strip().rstrip('. ')
    value=''.join('_' if unicodedata.category(c).startswith('C') else c for c in value)
    value=value.encode('utf-16-le')[:192].decode('utf-16-le',errors='ignore').rstrip('. ')
    value=value or '_account'
    if value.split('.')[0].upper() in RESERVED:
        value='_'+value
    if value.casefold().startswith('.jino') or re.fullmatch(r'server-[a-f0-9]{24}',value):
        value='account-'+value
    return value


def folder_names(rows,mapping):
    def login(row):
        value=row.get(mapping.get('login',''))
        return value if isinstance(value,str) and value.strip() else row.get(mapping.get('ftp_login',''))
    names=[component(login(row)) for row in rows]
    counts=Counter(x.casefold() for x in names)
    names=[name+'--'+component(row.get(mapping.get('ftp_host',''))) if counts[name.casefold()]>1 else name
           for row,name in zip(rows,names)]
    if len({x.casefold() for x in names})!=len(names):
        raise BackupError('FOLDER_NAME_AMBIGUOUS')
    return names


def _read_marker(path,expected):
    try:
        local_path(path.parent,path.name)
        return json.loads(path.read_text(encoding='utf-8'))==expected
    except (OSError,ValueError):
        return False


def _owned_legacy(path,server_id):
    marker=path/'.jino-server.json'
    if marker.exists():
        if not _read_marker(marker,{'schema_version':'1.0','server_id':server_id}):
            raise BackupError('DESTINATION_NOT_OWNED')
        return
    children=list(path.iterdir())
    if not children:
        raise BackupError('DESTINATION_NOT_OWNED')
    for child in children:
        local_path(path,child.name)
        if not child.is_dir():
            raise BackupError('DESTINATION_NOT_OWNED')
        if child.name.startswith('panel-job-'):
            owned=_read_marker(child/'.jino-panel-owner.json',{'job_id':child.name})
        elif child.name.startswith('download-'):
            try:
                data=json.loads(local_path(child,'.jino-owner.json').read_text(encoding='utf-8'))
                owned=data.get('schema_version')=='1.0' and isinstance(data.get('scope'),str)
            except (OSError,ValueError,AttributeError):
                owned=False
        else:
            owned=False
        if not owned:
            raise BackupError('DESTINATION_NOT_OWNED')


def server_directory(destination,server_id,folder_name,*,dry_run=False):
    safe_id(server_id)
    if (not isinstance(folder_name,str) or not folder_name or INVALID.search(folder_name)
            or folder_name in {'.','..'} or folder_name!=folder_name.rstrip('. ')
            or folder_name.split('.')[0].upper() in RESERVED or len(folder_name.encode('utf-16-le'))>480
            or folder_name.casefold().startswith('.jino')):
        raise BackupError('SERVER_FOLDER_INVALID')
    root=Path(destination).absolute()
    if root.exists() and (not root.is_dir() or root.is_symlink()
                          or getattr(root.lstat(),'st_file_attributes',0)&0x400):
        raise BackupError('LOCAL_PATH_UNSAFE')
    old=local_path(root,server_id)
    target=local_path(root,folder_name)
    expected={'schema_version':'1.0','server_id':server_id}
    if target.exists():
        if old.exists() and old!=target or not _read_marker(target/'.jino-server.json',expected):
            raise BackupError('SERVER_FOLDER_CONFLICT')
        return target
    if old.exists():
        _owned_legacy(old,server_id)
        if not dry_run:
            write_json(old/'.jino-server.json',expected)
            old.rename(target)
    elif not dry_run:
        target.mkdir(parents=True,exist_ok=False,mode=0o700)
        write_json(target/'.jino-server.json',expected)
    return target


@contextmanager
def server_work(destination, server_id, folder_name, kind):
    safe_id(server_id)
    if kind not in {"files", "databases"}:
        raise BackupError("REQUEST_INVALID")
    with FileLock(local_path(destination, '.jino-locks/' + server_id + '-' + kind + '.lock')):
        with FileLock(local_path(destination, '.jino-locks/' + server_id + '.lock')):
            directory = (server_directory(destination, server_id, folder_name) if folder_name
                         else local_path(destination, server_id))
        yield directory


def migrate_folders(rows,mapping,destination,state_dir,*,execute=False):
    source_ids=[row.get(mapping.get('id','')) for row in rows]
    if any(not isinstance(x,str) or not x for x in source_ids) or len(set(source_ids))!=len(rows):
        raise BackupError('SOURCE_ID_AMBIGUOUS')
    ids=[identity('server',x) for x in source_ids]
    names=folder_names(rows,mapping)
    root=Path(destination).absolute()
    for server_id,name in zip(ids,names):
        server_directory(root,server_id,name,dry_run=True)
    results=[]
    for index,(server_id,name) in enumerate(zip(ids,names),1):
        old=local_path(root,server_id)
        target=local_path(root,name)
        status='RENAME' if old.exists() else 'ALREADY_NAMED' if target.exists() else 'ABSENT'
        if execute and status!='ABSENT':
            account_ref=server_id.replace('server-','panel-',1)
            with ExitStack() as stack:
                stack.enter_context(FileLock(Path(state_dir)/'panel_jobs'/(account_ref+'.lock')))
                for kind in ('files', 'databases'):
                    stack.enter_context(FileLock(local_path(root,'.jino-locks/'+server_id+'-'+kind+'.lock')))
                stack.enter_context(FileLock(local_path(root,'.jino-locks/'+server_id+'.lock')))
                server_directory(root,server_id,name)
        results.append({'row':index,'server_id':server_id,'action':status})
    return {'dry_run':not execute,'servers_count':len(rows),
            'renamed' if execute else 'would_rename':sum(x['action']=='RENAME' for x in results),
            'already_named':sum(x['action']=='ALREADY_NAMED' for x in results),
            'absent':sum(x['action']=='ABSENT' for x in results),'servers':results}
