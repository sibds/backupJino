"""Private, durable directory observations for an interrupted FTPS walk."""
import hashlib
import json
import posixpath
from dataclasses import asdict
from datetime import datetime,timezone

from .errors import BackupError
from .transfer import Entry,relative_path,name_compatibility_issue
from .unsafe_paths import display_path


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
            db.execute('''CREATE TABLE IF NOT EXISTS walk_nodes (
                path TEXT PRIMARY KEY, parent TEXT NOT NULL, name TEXT NOT NULL,
                folded TEXT NOT NULL, kind TEXT NOT NULL, size INTEGER NOT NULL, modified TEXT)''')
            db.execute('CREATE INDEX IF NOT EXISTS walk_nodes_parent ON walk_nodes(parent,name)')
            db.execute('CREATE INDEX IF NOT EXISTS walk_nodes_folded ON walk_nodes(folded)')
            db.execute('''CREATE TABLE IF NOT EXISTS walk_queue (
                path TEXT PRIMARY KEY, state TEXT NOT NULL, error_code TEXT)''')
            db.execute('CREATE INDEX IF NOT EXISTS walk_queue_ready ON walk_queue(state,path)')
            db.execute('CREATE TABLE IF NOT EXISTS walk_indexed (path TEXT PRIMARY KEY, checksum TEXT NOT NULL)')
            state=db.execute('SELECT root,finished,observed_at FROM walk_state WHERE id=1').fetchone()
            if state and state[0]!=canonical_root:
                raise BackupError('DOWNLOAD_SCOPE_CHANGED')
            if state and state[1]:
                db.execute('DELETE FROM walk_listings')
                db.execute('DELETE FROM walk_verified')
                for table in ('walk_nodes','walk_queue','walk_indexed'):
                    db.execute('DELETE FROM '+table)
            # The caller owns DirectoryCheckpoint's exclusive file lock. An ACTIVE
            # entry can therefore only belong to a previous, interrupted owner.
            db.execute("UPDATE walk_queue SET state='PENDING' WHERE state='ACTIVE'")
            self.index_count=db.execute('SELECT count(*) FROM walk_nodes').fetchone()[0]
            if self.index_count>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
            self.resumed=bool(state and not state[1])
            self.observed_at=(state[2] if self.resumed else None) or datetime.now(timezone.utc).isoformat()
            db.execute('INSERT OR REPLACE INTO walk_state VALUES (1,?,0,?)',(canonical_root,self.observed_at))

    def listing(self,path,fetch):
        relative_path(path)
        with self.checkpoint.mutex:
            saved=self.checkpoint.db.execute('SELECT payload,checksum FROM walk_listings WHERE path=?',(path,)).fetchone()
        if saved:
            try:
                entries=self._decode(saved[0],saved[1])
                with self.checkpoint.mutex,self.checkpoint.db:
                    self._index_listing(path,entries,saved[1])
                return entries,True
            except (ValueError,TypeError,KeyError):
                raise BackupError('LOCAL_PROGRESS_INVALID') from None
        entries=fetch()
        if len(entries)>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        payload=json.dumps([asdict(entry) for entry in entries],ensure_ascii=True)
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('INSERT INTO walk_listings VALUES (?,?,?)',
                (path,payload,hashlib.sha256(payload.encode()).hexdigest()))
            self._index_listing(path,entries,hashlib.sha256(payload.encode()).hexdigest())
        return entries,False

    def _decode(self,payload,checksum):
        try:
            if not isinstance(payload,str) or hashlib.sha256(payload.encode()).hexdigest()!=checksum:raise ValueError()
            values=json.loads(payload)
            if not isinstance(values,list) or len(values)>self.max_files:raise ValueError()
            entries=[Entry(**value) for value in values]
            if any(not isinstance(e.name,str) or not isinstance(e.kind,str) or type(e.size) is not int
                   or e.modified is not None and not isinstance(e.modified,str) for e in entries):raise ValueError()
            return entries
        except (ValueError,TypeError,KeyError):raise BackupError('LOCAL_PROGRESS_INVALID') from None

    def _put_node(self,parent,entry):
        # Unsafe names remain in the original observation for the normal scanner
        # to report. They must never become eligible recovery transfers.
        try:
            entry.name.encode('utf8')
            if '/' in entry.name or entry.name in {'','.','..'}:return
            path=relative_path(posixpath.join(parent,entry.name))
        except (BackupError,UnicodeError):return
        db=self.checkpoint.db
        previous=db.execute('SELECT size,modified FROM walk_nodes WHERE path=?',(path,)).fetchone()
        if previous is None:
            self.index_count+=1
            if self.index_count>self.max_files:raise BackupError('TRANSFER_LIMIT_EXCEEDED')
        db.execute('INSERT OR REPLACE INTO walk_nodes VALUES (?,?,?,?,?,?,?)',
                   (path,parent,entry.name,path.casefold(),entry.kind,entry.size,entry.modified))
        if entry.kind!='file':return
        saved=db.execute('SELECT state,size,modified FROM files WHERE path=?',(path,)).fetchone()
        received=bool(saved and saved['state']=='done' and saved['size']==entry.size and saved['modified']==entry.modified)
        current=db.execute('SELECT state FROM walk_queue WHERE path=?',(path,)).fetchone()
        state=('RECEIVED' if received else current[0] if current and previous
               and previous['size']==entry.size and previous['modified']==entry.modified else 'PENDING')
        db.execute('INSERT INTO walk_queue(path,state) VALUES (?,?) ON CONFLICT(path) DO UPDATE SET state=excluded.state',
                   (path,state))

    def _index_listing(self,parent,entries,checksum):
        db=self.checkpoint.db
        indexed=db.execute('SELECT checksum FROM walk_indexed WHERE path=?',(parent,)).fetchone()
        if indexed and indexed[0]==checksum:return
        for entry in entries:self._put_node(parent,entry)
        db.execute('INSERT OR REPLACE INTO walk_indexed VALUES (?,?)',(parent,checksum))

    def import_cached(self):
        """Migrate one saved directory at a time; callers can drain its queue now."""
        cursor=None
        while True:
            with self.checkpoint.mutex:
                query='''SELECT l.path FROM walk_listings l
                    LEFT JOIN walk_indexed i ON i.path=l.path
                    WHERE (i.path IS NULL OR i.checksum!=l.checksum)'''
                if cursor is not None:query+=' AND l.path>?'
                row=self.checkpoint.db.execute(query+' ORDER BY l.path LIMIT 1',
                    () if cursor is None else (cursor,)).fetchone()
            if not row:return
            cursor=row[0]
            self.listing(cursor,lambda: (_ for _ in ()).throw(BackupError('LOCAL_PROGRESS_INVALID')))
            yield cursor

    def import_pending_cached(self):
        """Find missing objects without rewriting all completed legacy entries first."""
        with self.checkpoint.mutex:
            known={r['path']:(r['size'],r['modified']) for r in self.checkpoint.db.execute(
                "SELECT path,size,modified FROM files WHERE state='done'")}
        cursor=None
        while True:
            with self.checkpoint.mutex:
                query='SELECT path,payload,checksum FROM walk_listings'
                if cursor is not None:query+=' WHERE path>?'
                rows=self.checkpoint.db.execute(query+' ORDER BY path LIMIT 32',
                    () if cursor is None else (cursor,)).fetchall()
            if not rows:return
            for parent,payload,checksum in rows:
                cursor=parent
                entries=self._decode(payload,checksum)
                if not any(e.kind=='file' and known.get(posixpath.join(parent,e.name))!=(e.size,e.modified)
                           for e in entries):continue
                ancestors=['']+['/'.join(parent.split('/')[:i]) for i in range(1,len(parent.split('/')))] if parent else []
                with self.checkpoint.mutex:
                    available=all(self.checkpoint.db.execute('SELECT 1 FROM walk_listings WHERE path=?',(p,)).fetchone()
                                  for p in ancestors)
                if not available:continue  # Regular discovery validates an orphaned cache branch.
                for p in ancestors:
                    self.listing(p,lambda: (_ for _ in ()).throw(BackupError('LOCAL_PROGRESS_INVALID')))
                self.listing(parent,lambda: (_ for _ in ()).throw(BackupError('LOCAL_PROGRESS_INVALID')))
                yield parent

    def pending(self):
        cursor=''
        while True:
            with self.checkpoint.mutex:
                rows=self.checkpoint.db.execute('''SELECT n.path,n.name,n.kind,n.size,n.modified
                    FROM walk_queue q JOIN walk_nodes n ON n.path=q.path
                    WHERE q.state IN ('PENDING','RETRY') AND q.path>?
                    ORDER BY q.path LIMIT 128''',(cursor,)).fetchall()
            if not rows:return
            for row in rows:
                cursor=row['path']
                yield cursor,Entry(row['name'],row['kind'],row['size'],row['modified'])

    def safe_cached(self,path):
        """Defer ambiguous/unsupported cache paths to the regular conflict scanner."""
        try:
            parts=relative_path(path).split('/')
            if len(parts)>65 or any(name_compatibility_issue(part) for part in parts):return False
            with self.checkpoint.mutex:
                db=self.checkpoint.db
                for count in range(1,len(parts)+1):
                    prefix='/'.join(parts[:count])
                    peers=db.execute('SELECT path,kind FROM walk_nodes WHERE folded=? LIMIT 2',
                                     (prefix.casefold(),)).fetchall()
                    if len(peers)!=1 or peers[0]['path']!=prefix:return False
                    if peers[0]['kind']!=('file' if count==len(parts) else 'dir'):return False
            return True
        except (BackupError,UnicodeError):return False

    def register(self,path,entry):
        parent,name=posixpath.split(relative_path(path))
        if name!=entry.name:raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='SOURCE_NAME_CHANGED')
        with self.checkpoint.mutex:
            saved=self.checkpoint.db.execute('''SELECT n.kind,n.size,n.modified,q.state
                FROM walk_nodes n LEFT JOIN walk_queue q ON q.path=n.path WHERE n.path=?''',(path,)).fetchone()
            if saved and saved['kind']==entry.kind and saved['size']==entry.size and saved['modified']==entry.modified and saved['state']:
                return  # Already committed by observation; do not fsync it again for the RAM buffer.
            with self.checkpoint.db:self._put_node(parent,entry)

    def claim(self,path):
        with self.checkpoint.mutex,self.checkpoint.db:
            return self.checkpoint.db.execute("UPDATE walk_queue SET state='ACTIVE',error_code=NULL WHERE path=? AND state IN ('PENDING','RETRY')",(path,)).rowcount==1

    def transferred(self,path):
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute("UPDATE walk_queue SET state='RECEIVED',error_code=NULL WHERE path=?",(path,))

    def failed(self,path,code):
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute("UPDATE walk_queue SET state='RETRY',error_code=? WHERE path=?",(code,path))

    def omit(self,path,code):
        try:path.encode('utf8')
        except UnicodeError:return  # Such names were deliberately excluded from the SQL index.
        prefix=path.rstrip('/')+'/'
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('''UPDATE walk_queue SET state='SKIPPED',error_code=?
                WHERE path=? OR substr(path,1,?)=?''',(code,path,len(prefix),prefix))

    def finish(self):
        with self.checkpoint.mutex,self.checkpoint.db:
            self.checkpoint.db.execute('''UPDATE walk_queue SET state='RECEIVED',error_code=NULL
                WHERE EXISTS (SELECT 1 FROM walk_nodes n JOIN files f ON f.path=n.path
                    WHERE n.path=walk_queue.path AND f.state='done'
                    AND f.size=n.size AND f.modified IS n.modified)''')
            self.checkpoint.db.execute('UPDATE walk_state SET finished=1 WHERE id=1')

    def invalidate_directory(self,path):
        relative_path(path)
        with self.checkpoint.mutex,self.checkpoint.db:
            for directory in {path,posixpath.dirname(path)}:
                self.checkpoint.db.execute('DELETE FROM walk_listings WHERE path=?',(directory,))

    def update_entry(self,path,entry):
        """Replace one stale observation without discarding completed siblings."""
        parent,name=posixpath.split(relative_path(path))
        if entry.name!=name:raise BackupError('REMOTE_PATH_UNSAFE',unsafe_reason='SOURCE_NAME_CHANGED',unsafe_path=display_path(path),expected_name=display_path(name),actual_name=display_path(entry.name))
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
            self._put_node(parent,entry)

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
