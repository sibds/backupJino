"""Private, durable directory observations for an interrupted FTPS walk."""
import hashlib
import json
import posixpath
from dataclasses import asdict
from datetime import datetime,timezone

from .errors import BackupError
from .transfer import Entry,relative_path


class ResumeInventory:
    def __init__(self,checkpoint,canonical_root,max_files):
        self.checkpoint,self.max_files=checkpoint,max_files
        with checkpoint.mutex,checkpoint.db:
            db=checkpoint.db
            db.execute('CREATE TABLE IF NOT EXISTS walk_state (id INTEGER PRIMARY KEY CHECK(id=1), root TEXT NOT NULL, finished INTEGER NOT NULL, observed_at TEXT)')
            if 'observed_at' not in {row[1] for row in db.execute('PRAGMA table_info(walk_state)')}:
                db.execute('ALTER TABLE walk_state ADD COLUMN observed_at TEXT')
            db.execute('CREATE TABLE IF NOT EXISTS walk_listings (path TEXT PRIMARY KEY, payload TEXT NOT NULL, checksum TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS walk_verified (path TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            state=db.execute('SELECT root,finished,observed_at FROM walk_state WHERE id=1').fetchone()
            if state and state[0]!=canonical_root:
                raise BackupError('DOWNLOAD_SCOPE_CHANGED')
            if state and state[1]:
                db.execute('DELETE FROM walk_listings')
                db.execute('DELETE FROM walk_verified')
            self.resumed=bool(state and not state[1])
            self.observed_at=(state[2] if self.resumed else None) or datetime.now(timezone.utc).isoformat()
            db.execute('INSERT OR REPLACE INTO walk_state VALUES (1,?,0,?)',(canonical_root,self.observed_at))

    def listing(self,path,fetch):
        relative_path(path)
        with self.checkpoint.mutex:
            saved=self.checkpoint.db.execute('SELECT payload,checksum FROM walk_listings WHERE path=?',(path,)).fetchone()
        if saved:
            try:
                if not isinstance(saved[0],str) or hashlib.sha256(saved[0].encode()).hexdigest()!=saved[1]:raise ValueError()
                values=json.loads(saved[0])
                if not isinstance(values,list) or len(values)>self.max_files:raise ValueError()
                entries=[Entry(**value) for value in values]
                if any(not isinstance(e.name,str) or not isinstance(e.kind,str) or type(e.size) is not int
                       or e.modified is not None and not isinstance(e.modified,str) for e in entries):raise ValueError()
                return entries,True
            except (ValueError,TypeError,KeyError):
                raise BackupError('LOCAL_PROGRESS_INVALID') from None
        entries=fetch()
        if len(entries)>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        payload=json.dumps([asdict(entry) for entry in entries],ensure_ascii=True)
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('INSERT INTO walk_listings VALUES (?,?,?)',
                (path,payload,hashlib.sha256(payload.encode()).hexdigest()))
        return entries,False

    def finish(self):
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('UPDATE walk_state SET finished=1 WHERE id=1')

    def invalidate_directory(self,path):
        relative_path(path)
        with self.checkpoint.mutex,self.checkpoint.db:
            for directory in {path,posixpath.dirname(path)}:
                self.checkpoint.db.execute('DELETE FROM walk_listings WHERE path=?',(directory,))

    def update_entry(self,path,entry):
        """Replace one stale observation without discarding completed siblings."""
        parent,name=posixpath.split(relative_path(path))
        if entry.name!=name:raise BackupError('REMOTE_PATH_UNSAFE')
        with self.checkpoint.mutex,self.checkpoint.db:
            db=self.checkpoint.db
            saved=db.execute('SELECT payload,checksum FROM walk_listings WHERE path=?',(parent,)).fetchone()
            if saved:
                if hashlib.sha256(saved[0].encode()).hexdigest()!=saved[1]:raise BackupError('LOCAL_PROGRESS_INVALID')
                entries=json.loads(saved[0])
                entries=[value for value in entries if value['name']!=name]+[asdict(entry)]
                if len(entries)>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
                payload=json.dumps(entries,ensure_ascii=True)
                db.execute('UPDATE walk_listings SET payload=?,checksum=? WHERE path=?',
                           (payload,hashlib.sha256(payload.encode()).hexdigest(),parent))
            db.execute('DELETE FROM walk_verified WHERE path=?',(path,))

    def signature(self,path):
        try:stat=self.checkpoint.file_path(path).stat()
        except OSError:raise BackupError('LOCAL_FILE_CHANGED') from None
        return [stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns]

    def verified(self,path,entry,item,signature):
        if signature!=self.signature(path):raise BackupError('LOCAL_FILE_CHANGED')
        payload=json.dumps({'entry':asdict(entry),'item':item,'signature':signature})
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('INSERT OR REPLACE INTO walk_verified VALUES (?,?)',(path,payload))

    def receipt(self,path,entry):
        with self.checkpoint.mutex:
            row=self.checkpoint.db.execute('SELECT payload FROM walk_verified WHERE path=?',(path,)).fetchone()
            saved=self.checkpoint.db.execute('SELECT state,size,modified,sha256 FROM files WHERE path=?',(path,)).fetchone()
        if not row:return None
        try:
            value=json.loads(row[0])
            if (value['entry']==asdict(entry) and value['signature']==self.signature(path)
                    and saved and saved['state']=='done' and saved['size']==entry.size
                    and saved['modified']==entry.modified and saved['sha256']==value['item']['sha256']
                    and value['item']['size_bytes']==entry.size
                    and value['item']['path']==self.checkpoint.storage_path(path)):
                return value['item']
        except (BackupError,OSError,ValueError,KeyError,TypeError):pass
        return None
