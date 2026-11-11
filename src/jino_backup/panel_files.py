"""Read-only HTTPS file transport observed in the current Jino file manager."""

import os
import math
import ssl
from urllib.error import HTTPError,URLError
from urllib.parse import urlencode,urlsplit
from urllib.request import HTTPRedirectHandler,HTTPSHandler,Request,build_opener

from .errors import BackupError
from .storage import CHUNK


def normalize_file_size(value):
    if isinstance(value,str) and value.isascii() and value.isdecimal():
        value=int(value)
    if (isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value)
            or not 0<=value<=2**53 or value!=int(value)):
        raise BackupError('FILE_SIZE_INVALID')
    return int(value)


def validate_transfer_origin(url):
    try:
        parsed=urlsplit(url)
        port=parsed.port
    except (TypeError,ValueError):
        raise BackupError('FILETRANSFER_ORIGIN_UNVERIFIED') from None
    if (parsed.scheme!='https' or not parsed.hostname or not parsed.hostname.endswith('.jino.ru')
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or port is not None and not 1 <= port <= 65535):
        raise BackupError('FILETRANSFER_ORIGIN_UNVERIFIED')
    return url.rstrip('/')


def listing_page(response):
    if not isinstance(response,dict) or response.get('errors'):
        raise BackupError('PANEL_RESPONSE_FAILED')
    try:
        page=response['data']['me']['filemanager']['files']
        items=page['list']
        total=page['total']['items']
        if not isinstance(items,list) or not isinstance(total,int) or total<len(items) or not all(isinstance(x,dict) for x in items):
            raise ValueError
    except (KeyError,TypeError,ValueError):
        raise BackupError('PANEL_RESPONSE_INVALID') from None
    items=[{**x,'size':normalize_file_size(x['size'])} if x.get('size') is not None else x for x in items]
    return items,total


def resolve_export_directory(read,root,snapshot_date):
    token=root
    for name in ('backups',snapshot_date):
        matches=[entry for entry in read(token) if entry.get('name')==name]
        if not matches:
            return None
        if len(matches)!=1:
            raise BackupError('REMOTE_PATH_AMBIGUOUS')
        entry=matches[0]
        if entry.get('type')!='dir' or entry.get('targetType') is not None or entry.get('targetDisplayPath') is not None:
            raise BackupError('REMOTE_PATH_UNSAFE')
        token=entry.get('path')
        if not isinstance(token,str) or not token:
            raise BackupError('REMOTE_PATH_UNSAFE')
    return token


def copy_response(response,target,max_bytes,control=None,progress=None):
    received=0
    with target.open('xb') as stream:
        os.chmod(target,0o600)
        try:
            while True:
                if control:
                    control()
                chunk=response.read(min(CHUNK,max_bytes-received+1))
                if not chunk:
                    break
                if received+len(chunk)>max_bytes:
                    raise BackupError('DB_EXPORT_LIMIT_EXCEEDED')
                stream.write(chunk)
                received+=len(chunk)
                if progress:
                    progress(received)
        finally:
            stream.flush()
            os.fsync(stream.fileno())
    return received


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        return None


class HTTPSFileTransfer:
    def __init__(self,url,token):
        self.url=validate_transfer_origin(url)
        if not isinstance(token,str) or not token or len(token)>16384 or any(ord(c)<32 for c in token):
            raise BackupError('FILETRANSFER_CREDENTIAL_INVALID')
        self.token=token
        self.opener=build_opener(NoRedirect(),HTTPSHandler(context=ssl.create_default_context()))

    def __repr__(self):
        return 'HTTPSFileTransfer(<private session>)'

    def open(self,path,probe=False,offset=0):
        headers={'Authorization':'Bearer '+self.token}
        if probe:
            headers['Range']='bytes=0-0'
        elif offset:
            headers['Range']='bytes='+str(offset)+'-'
        request=Request(self.url+'/download?'+urlencode({'path':path,'force-download':'true'}),headers=headers)
        try:
            response=self.opener.open(request,timeout=30)
            if offset and response.status==206:
                import re
                match=re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',response.headers.get('Content-Range',''))
                if not match or int(match[1])!=offset:
                    response.close()
                    raise BackupError('FILETRANSFER_RANGE_INVALID')
            return response
        except HTTPError as exc:
            if 300 <= exc.code < 400:
                raise BackupError('FILETRANSFER_REDIRECT_BLOCKED') from None
            raise BackupError('FILETRANSFER_HTTP_FAILED',http_status=exc.code) from None
        except (ssl.SSLCertVerificationError,URLError) as exc:
            reason=exc.reason if isinstance(exc,URLError) else exc
            code='TLS_CERTIFICATE_INVALID' if isinstance(reason,ssl.SSLCertVerificationError) else 'FILETRANSFER_CONNECTION_FAILED'
            raise BackupError(code) from None
        except (OSError,TimeoutError):
            raise BackupError('FILETRANSFER_CONNECTION_FAILED') from None

    def probe(self,path):
        with self.open(path,probe=True) as response:
            if not response.read(1):
                raise BackupError('FILETRANSFER_PROBE_EMPTY')

    def download(self,path,target,max_bytes):
        with self.open(path) as response:
            length=response.headers.get('Content-Length')
            if length and (not length.isdigit() or int(length)>max_bytes):
                raise BackupError('DB_EXPORT_LIMIT_EXCEEDED')
            return copy_response(response,target,max_bytes,getattr(self,'control',None),getattr(self,'progress',None))
