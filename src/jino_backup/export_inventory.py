"""Read-only comparison of provider export trees and the private export registry."""
import hashlib
import json
import posixpath
from .errors import BackupError
from .file_conflicts import modified_timestamp


def list_export_entries(read,initial,scope,max_nodes=10000,max_depth=32,control=None):
    pending=[('',list(initial),0)];rows=[];visited=set()
    while pending:
        parent,items,depth=pending.pop()
        for item in items:
            if control:control()
            name=item.get('name')
            if not isinstance(name,str) or not name or name in {'.','..'} or '/' in name or '\\' in name:
                raise BackupError('REMOTE_LIST_INVALID')
            path=item.get('path')
            if not isinstance(path,str) or not path:raise BackupError('REMOTE_LIST_INVALID')
            relative=posixpath.join(parent,name)
            link=item.get('targetType') is not None or item.get('targetDisplayPath') is not None
            kind='link' if link else item.get('type','unknown')
            size=item.get('size') if type(item.get('size')) is int else None
            signature=[scope['snapshot_ref'],path,size if size is not None else 0,item.get('mtime')]
            regular=kind=='file' and size is not None and size>0
            match=next((db for db in scope['databases'] if regular and name==db+'.sql'),None)
            row={'id':hashlib.sha256(json.dumps(signature,sort_keys=True).encode()).hexdigest(),
                 'path':path,'name':name,'relative_path':relative,'kind':kind,
                 'size_bytes':size,'mtime':item.get('mtime'),'database_name':match,
                 'selectable':regular and name.endswith('.sql'),'supported':match is not None}
            rows.append(row)
            if len(rows)>=max_nodes:
                row['listing_error']='FILE_LIST_LIMIT_EXCEEDED'
                return rows
            if kind!='dir':continue
            if depth>=max_depth or path in visited:
                row['listing_error']='REMOTE_PATH_AMBIGUOUS'
                continue
            visited.add(path)
            try:children=list(read(path))
            except BackupError as error:
                if error.code in {'CANCELLED','VERIFICATION_DEFERRED'}:raise
                row['listing_error']=error.code
                continue
            pending.append((relative,children,depth+1))
    return rows


def reconcile_export_entries(observed,known):
    records={item['path']:item for item in known}
    rows=[];seen=set();incomplete=any(item.get('listing_error') or item.get('kind')=='link' for item in observed)
    for original in observed:
        row=dict(original);seen.add(row['path'])
        internal=records.get(row['path'])
        if internal:
            row['internal']={key:internal.get(key) for key in ('size_bytes','mtime','local_verified','local_size_bytes')}
            before=modified_timestamp(str(internal['mtime'])) if internal.get('mtime') is not None else None
            after=modified_timestamp(str(row['mtime'])) if row.get('mtime') is not None else None
            row['comparison']='known_container' if row.get('kind')==internal.get('kind')=='dir' else 'same_metadata' if row.get('kind')=='file' and row.get('size_bytes')==internal.get('size_bytes') and before is not None and after==before else 'different'
        else:row['comparison']='untracked'
        if row.get('listing_error'):row['comparison']='not_checked'
        rows.append(row)
    for path,internal in records.items():
        if path in seen:continue
        rows.append({'id':hashlib.sha256(('missing:'+path).encode()).hexdigest(),'path':path,
            'name':internal.get('name','Учтённый SQL'),'relative_path':internal.get('name','Учтённый SQL'),
            'kind':'missing','size_bytes':internal.get('size_bytes'),'mtime':internal.get('mtime'),
            'supported':False,'selectable':False,'db_alias':internal.get('db_alias'),
            'internal':{key:internal.get(key) for key in ('size_bytes','mtime','local_verified','local_size_bytes')},
            'comparison':'not_checked' if incomplete else 'missing_remote'})
    for row in rows:
        if row.get('kind')!='dir':continue
        prefix=row['relative_path']+'/'
        children=[child for child in rows if child.get('relative_path','').startswith(prefix)]
        row['known_children']=sum(child['comparison'] in {'same_metadata','different'} for child in children)
        row['unknown_children']=sum(child['comparison']=='untracked' for child in children)
        if row['known_children'] and not row.get('listing_error'):row['comparison']='known_container'
    return rows
