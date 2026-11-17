import sqlite3
import pytest
from jino_backup.downloads import DirectoryCheckpoint
from jino_backup.resume_inventory import ResumeInventory
from jino_backup.transfer import Entry


def test_listing_and_pending_objects_commit_atomically(tmp_path,monkeypatch):
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        put=inventory._put_node
        def interrupted(parent,entry):
            put(parent,entry)
            if entry.name=='second':raise sqlite3.OperationalError('synthetic transaction failure')
        monkeypatch.setattr(inventory,'_put_node',interrupted)
        with pytest.raises(sqlite3.OperationalError):
            inventory.listing('',lambda:[Entry('first','file',1,'t'),Entry('second','file',2,'t')])
        for table in ('walk_listings','walk_nodes','walk_queue','walk_indexed'):
            assert checkpoint.db.execute('SELECT count(*) FROM '+table).fetchone()[0]==0


def test_claim_is_exclusive_and_interrupted_owner_is_recovered(tmp_path):
    path=tmp_path/'copy'
    with DirectoryCheckpoint(path,'scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        inventory.listing('',lambda:[Entry('file','file',1,'t')])
        assert inventory.claim('file')
        assert not inventory.claim('file')
        assert not list(inventory.pending())
    with DirectoryCheckpoint(path,'scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        assert [p for p,e in inventory.pending()]==['file']
        assert inventory.claim('file')
        inventory.transferred('file')
        assert not list(inventory.pending())


def test_case_collisions_and_links_are_deferred_before_cache_transfer(tmp_path):
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        inventory.listing('',lambda:[Entry('Alpha','dir',0,'t'),Entry('alpha','dir',0,'t'),Entry('link','link',0,'t'),Entry('safe','dir',0,'t')])
        for parent in ('Alpha','alpha','link','safe'):
            inventory.listing(parent,lambda:[Entry('file','file',1,'t')])
        assert not inventory.safe_cached('Alpha/file')
        assert not inventory.safe_cached('alpha/file')
        assert not inventory.safe_cached('link/file')
        assert inventory.safe_cached('safe/file')


def test_retries_remain_in_queue_with_reason(tmp_path):
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        inventory.listing('',lambda:[Entry('file','file',1,'t')])
        assert inventory.claim('file')
        inventory.failed('file','TRANSFER_INTERRUPTED')
        assert [p for p,e in inventory.pending()]==['file']
        assert checkpoint.db.execute('SELECT error_code FROM walk_queue').fetchone()[0]=='TRANSFER_INTERRUPTED'


def test_legacy_priority_import_does_not_materialize_finished_branches(tmp_path):
    import hashlib,json
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        root=[{'name':'old','kind':'dir','size':0,'modified':'t'},{'name':'new','kind':'dir','size':0,'modified':'t'}]
        file=[{'name':'file','kind':'file','size':1,'modified':'t'}]
        with checkpoint.db:
            for path,values in [('',root),('old',file),('new',file)]:
                payload=json.dumps(values)
                checkpoint.db.execute('INSERT INTO walk_listings VALUES (?,?,?)',
                    (path,payload,hashlib.sha256(payload.encode()).hexdigest()))
            checkpoint.db.execute("INSERT INTO files VALUES ('old/file',1,'t','done',1,?)",('0'*64,))
        assert list(inventory.import_pending_cached())==['new']
        assert [p for p,e in inventory.pending()]==['new/file']
        assert checkpoint.db.execute("SELECT count(*) FROM walk_nodes WHERE path='old/file'").fetchone()[0]==0
        assert inventory.safe_cached('new/file')


def test_explicit_skips_are_not_left_in_pending_queue(tmp_path):
    with DirectoryCheckpoint(tmp_path/'copy','scope') as checkpoint:
        inventory=ResumeInventory(checkpoint,'/',100)
        inventory.listing('',lambda:[Entry('skip','dir',0,'t'),Entry('skip-other','file',1,'t')])
        inventory.listing('skip',lambda:[Entry('file','file',1,'t')])
        inventory.omit('skip','REMOTE_NAME_UNSUPPORTED')
        assert [p for p,e in inventory.pending()]==['skip-other']
        assert checkpoint.db.execute("SELECT state,error_code FROM walk_queue WHERE path='skip/file'").fetchone()[:]==('SKIPPED','REMOTE_NAME_UNSUPPORTED')
