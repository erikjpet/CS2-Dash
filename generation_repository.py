"""Unactivated, saved-only immutable generation readers and atomic head CAS.

Import has no I/O. Factories/admission/budgets/complete parity come from the host.
No source/history/holdings writes, provider callbacks, deletion or timers exist.
"""
from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import uuid
import ctypes

import bulk_derived_staging as core
import lookup_group_codec as codec

HEAD_KEY='bulk_derived_generation_head:v1'
ROLLBACK_KEY='bulk_derived_generation_rollback:v1'
FORMAT='normalized-lookup-common-v1'
READER_ABI='saved-generation-reader:v1'
DICTIONARY_KEY='market_lookup_group_dictionary:v1'
RECEIPTS=(*core.RECEIPT_KEYS,DICTIONARY_KEY)
MAX_SEAL=131072

INDEX_DDL=(
 'CREATE INDEX lookup_rank_v1 ON lookup_windows(window_days,(change_pct) IS NULL ASC,change_pct DESC,LOWER(name) ASC,name COLLATE BINARY ASC,common_id)',
 'CREATE INDEX lookup_common_windows_v1 ON lookup_windows(common_id,window_days)',
 'CREATE INDEX lookup_common_search_v1 ON lookup_common(search_text)',
 'CREATE INDEX lookup_common_filters_v1 ON lookup_common(collection,item_type,rarity)',
 'CREATE INDEX lookup_common_price_v1 ON lookup_common(latest_price)',
)

def compact(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def identity(path):
    s=path.stat()
    return (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)

def safe_file(directory,generation):
    if not isinstance(generation,str) or not re.fullmatch('[0-9a-f]{32}',generation):
        raise core.Deferred('Invalid sealed generation identifier')
    root=Path(directory).resolve()
    if root.drive.upper()!='C:':
        raise core.Deferred('Private generation directory must be on C:')
    path=root/('bulk-derived-'+generation+'.sqlite')
    if path.is_symlink() or path.resolve()!=path or not path.is_file():
        raise core.Deferred('Sealed generation is missing or not a regular owned file')
    if any(Path(str(path)+suffix).exists() for suffix in ('-wal','-journal')):
        raise core.Deferred('A sealed generation may not have a writable journal')
    return path

def readonly(path,immutable=False):
    c=sqlite3.connect(path.as_uri()+'?mode=ro'+('&immutable=1' if immutable else ''),
                      uri=True,timeout=0,isolation_level=None,factory=ReadConnection)
    c.row_factory=sqlite3.Row
    option=sqlite3.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
    c.setconfig(option,True)
    if not c.getconfig(option):
        c.close();raise core.Deferred('No-close-checkpoint read guard is required')
    c.execute('PRAGMA temp_store=MEMORY')
    c.execute('PRAGMA cache_size=-2048')
    c.execute('PRAGMA query_only=ON')
    return c

class ReadConnection(sqlite3.Connection):
    """A reader pins its head/main snapshot before returning to any caller."""
    _snapshot_pinned=False
    _derived_generation=None
    _derived_generation_status=None
    _generation_pin_release=None
    _repository_closed=False

    def execute(self,sql,parameters=()):
        if self._snapshot_pinned and re.fullmatch(r'\s*BEGIN(?:\s+(?:DEFERRED|TRANSACTION))?\s*;?\s*',sql,re.I):
            # Existing saved-read callers start a transaction explicitly. The
            # repository already started that exact snapshot to select its head.
            return super().execute('SELECT 1 WHERE 0')
        if self._snapshot_pinned and re.match(r'\s*(?:COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH)\b',sql,re.I):
            raise core.Deferred('Generation snapshot lifetime belongs to the read connection')
        return super().execute(sql,parameters)

    def commit(self):
        if self._snapshot_pinned:
            raise core.Deferred('Saved generation readers cannot commit')
        return super().commit()

    def rollback(self):
        # Existing finally blocks may request rollback. Keep the selected
        # snapshot until close so no later statement can silently mix heads.
        if not self._snapshot_pinned:return super().rollback()

    def __enter__(self):return self

    def __exit__(self,kind,value,traceback):
        self.close()
        return False

    def close(self):
        if self._repository_closed:return
        self._snapshot_pinned=False
        try:
            if self.in_transaction:super().rollback()
        finally:super().close()
        self._repository_closed=True
        callback=self._generation_pin_release
        self._generation_pin_release=None
        if callback is not None:callback()

@dataclass(frozen=True)
class Seal:
    generation:str
    path:Path
    metadata:dict
    stat_identity:tuple

class ReadLease:
    """Windows handle denies writers/rename/delete while any generation is used."""
    def __init__(self,path,exclusive=False):
        if os.name!='nt':raise core.Deferred('Qualified Windows immutable-file lease is required')
        from ctypes import wintypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.CreateFileW.argtypes=(wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,
            ctypes.c_void_p,wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE)
        kernel.CreateFileW.restype=wintypes.HANDLE
        kernel.CloseHandle.argtypes=(wintypes.HANDLE,)
        kernel.CloseHandle.restype=wintypes.BOOL
        handle=kernel.CreateFileW(str(path),0x80000000,0 if exclusive else 1,None,3,0x10000000,None)
        if handle==ctypes.c_void_p(-1).value:
            raise core.Deferred('Generation writer/retention lease unavailable: '+str(ctypes.get_last_error()))
        self.kernel,self.handle=kernel,handle

    def close(self):
        if self.handle is not None:
            if not self.kernel.CloseHandle(self.handle):raise OSError(ctypes.get_last_error(),'Generation lease close failed')
            self.handle=None

class Repository:
    def __init__(self,directory,main_read_factory,main_write_factory,writer_lock,
                 external_fence,admission,growth_probe,stage_budget,publish_budget):
        self.directory=Path(directory).resolve()
        if self.directory.drive.upper()!='C:':raise ValueError('C-only generation directory required')
        self.main_read_factory=main_read_factory
        self.main_write_factory=main_write_factory
        self.writer_lock=writer_lock
        self.external=external_fence
        self.admission=admission
        self.growth_probe=growth_probe
        self.stage_budget=stage_budget
        self.publish_budget=publish_budget
        self.lock=threading.RLock()
        self.validated={}
        self.leases={}
        self.reader_pins={}
        self.closing=False

    def _lease(self,generation,path):
        with self.lock:
            if generation not in self.leases:
                if len(self.leases)>=3:raise core.Deferred('Retained generation lease capacity reached; no automatic deletion')
                self.leases[generation]=ReadLease(path)

    def pin_reader(self,generation):
        with self.lock:
            if self.closing:raise core.Deferred('Generation repository is closing')
            self.reader_pins[generation]=self.reader_pins.get(generation,0)+1
        def release():
            with self.lock:
                count=self.reader_pins.get(generation,0)
                if count<=0:raise core.Deferred('Reader pin accounting underflow')
                if count==1:
                    self.reader_pins.pop(generation)
                    if self.closing:
                        lease=self.leases.pop(generation,None)
                        if lease is not None:lease.close()
                        self.validated.pop(generation,None)
                else:self.reader_pins[generation]=count-1
        return release

    def release_unused(self,protected):
        with self.lock:
            for generation in tuple(self.leases):
                if generation not in protected and not self.reader_pins.get(generation):
                    self.leases.pop(generation).close()
                    self.validated.pop(generation,None)

    def close(self):
        # Existing readers keep their deny-delete leases through shutdown.
        with self.lock:
            self.closing=True
            self.release_unused(set(self.reader_pins))

    def seal(self,generation,expected_hashes):
        """Full parity, receipts and capacity are checked before writer admission."""
        self.admission('seal')
        path=safe_file(self.directory,generation)
        if path.stat().st_size>self.stage_budget.file_bytes:
            raise core.Deferred('Complete indexed generation exceeds the file cap')
        path.chmod(stat.S_IREAD)
        self._lease(generation,path)
        guard=core.Guard(self.stage_budget)
        with closing(readonly(path)) as c:
            c.set_progress_handler(guard.progress,1000)
            state=core.get_json(c,core.STATE_KEY)
            if (not state or not state.get('complete') or state.get('generation')!=generation
                or tuple(state.get('lookupDone',()))!=core.WINDOWS):
                raise core.Deferred('Only a complete three-window generation can be sealed')
            dictionary=core.get_setting(c,DICTIONARY_KEY)
            # Validates the exact dictionary even if a particular row is plain.
            codec.DictionaryBuilder(dictionary)
            receipts={key:core.get_setting(c,key) for key in RECEIPTS}
            if any(value is None for value in receipts.values()):
                raise core.Deferred('A complete generation receipt/dictionary is missing')
            hashes={};counts={}
            for table,columns in (('item_market_metrics',core.METRIC_COLUMNS),('market_item_lookup',core.LOOKUP_COLUMNS)):
                h=hashlib.sha256();n=0
                query='SELECT '+','.join(columns)+' FROM '+table+' ORDER BY rowid'
                plan=c.execute('EXPLAIN QUERY PLAN '+query).fetchall()
                if any('TEMP' in row[3] for row in plan):raise core.Deferred('Parity requires the bounded row-order access path')
                for row in c.execute(query):
                    row=list(row)
                    if table=='market_item_lookup':row[17]=codec.decode(row[17],dictionary)
                    h.update(json.dumps(row,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n')
                    n+=1;guard.rows+=1
                    if n%256==0:guard.check()
                hashes[table]=h.hexdigest();counts[table]=n
            if hashes!=expected_hashes:
                raise core.RevisionConflict('Complete decoded output does not match the captured source')
            if c.execute('PRAGMA foreign_key_check').fetchone():
                raise core.Deferred('Normalized generation has a missing common tuple')
            window_counts={}
            for window in core.WINDOWS:
                metric=json.loads(receipts['market_metric_receipt:'+str(window)])
                lookup=json.loads(receipts['market_lookup_receipt:'+str(window)])
                mc=c.execute("SELECT COUNT(*) FROM item_market_metrics WHERE provider='market' AND window_days=?",(window,)).fetchone()[0]
                lc=c.execute('SELECT COUNT(*) FROM lookup_windows WHERE window_days=?',(window,)).fetchone()[0]
                if (not metric.get('complete') or not lookup.get('metricsVerified') or
                    metric.get('rows')!=mc or lookup.get('rows')!=lc or
                    metric.get('sourceRevision')!=state['fence']['market_source_revision'] or
                    lookup.get('sourceRevision')!=state['fence']['market_source_revision']):
                    raise core.Deferred('A window count/source receipt does not match the complete data')
                window_counts[str(window)]=dict(metric=mc,lookup=lc,priced=lookup.get('priced'))
            schema=c.execute("SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name").fetchall()
            metadata=dict(version=1,format=FORMAT,readerABI=READER_ABI,generation=generation,
                fence=state['fence'],asOf=state['asOf'],counts=counts,rowHashes=hashes,
                schemaSHA256=hashlib.sha256(compact([tuple(r) for r in schema]).encode()).hexdigest(),
                dictionarySHA256=hashlib.sha256(dictionary.encode()).hexdigest(),
                receiptsSHA256=hashlib.sha256(compact(receipts).encode()).hexdigest(),
                fileBytes=path.stat().st_size,complete=True,windows=list(core.WINDOWS),windowCounts=window_counts)
            c.set_progress_handler(None,0)
        content=hashlib.sha256()
        with path.open('rb') as f:
            while chunk:=f.read(1024*1024):content.update(chunk);guard.check()
        metadata['fileSHA256']=content.hexdigest()
        metadata['sealSeconds']=self.stage_budget.seconds-(guard.deadline-time.monotonic())
        sealpath=path.with_suffix('.seal.json')
        raw=compact(metadata).encode()
        if len(raw)>MAX_SEAL:raise core.Deferred('Generation seal exceeds its metadata cap')
        if sealpath.exists() and sealpath.read_bytes()!=raw:
            raise core.Deferred('A generation seal is immutable; do not overwrite it')
        sealpath.write_bytes(raw)
        # The supported Windows read-only file attribute prevents accidental
        # producer writes. Product ownership keeps this private directory trusted.
        seal=Seal(generation,path,metadata,identity(path))
        with self.lock:self.validated[generation]=seal
        guard.check()
        return seal

    def _validate_head_file(self,head):
        generation=head.get('generation')
        path=safe_file(self.directory,generation)
        with self.lock:cached=self.validated.get(generation)
        if cached is not None and cached.stat_identity==identity(path) and cached.metadata==head.get('seal'):
            return cached
        self._lease(generation,path)
        sealpath=path.with_suffix('.seal.json')
        if sealpath.is_symlink() or not sealpath.is_file() or sealpath.stat().st_size>MAX_SEAL:
            raise core.Deferred('A bounded retained seal is required')
        metadata=json.loads(sealpath.read_text())
        if (metadata!=head.get('seal') or metadata.get('format')!=FORMAT or
            metadata.get('readerABI')!=READER_ABI or not metadata.get('complete') or
            metadata.get('fileBytes')!=path.stat().st_size):
            raise core.Deferred('Generation seal/reader/file identity does not match the saved head')
        h=hashlib.sha256();guard=core.Guard(self.stage_budget)
        with path.open('rb') as f:
            while chunk:=f.read(1024*1024):h.update(chunk);guard.check()
        if h.hexdigest()!=metadata.get('fileSHA256'):
            raise core.Deferred('Sealed generation content changed')
        seal=Seal(generation,path,metadata,identity(path))
        with self.lock:self.validated[generation]=seal
        return seal

    def publish(self,seal,expected_head,expected_pending_generation=None):
        """CAS only the saved head/pending setting; do not replace any data rows."""
        self.admission('publish')
        if self._validate_head_file({'generation':seal.generation,'seal':seal.metadata})!=seal:
            raise core.Deferred('The prequalified sealed file identity changed')
        if not self.writer_lock.acquire(blocking=False):
            raise core.Deferred('Writer opportunity is busy; no automatic retry')
        token=uuid.uuid4().hex
        conn=None;guard=core.Guard(self.publish_budget,self.growth_probe)
        head=dict(version=1,generation=seal.generation,publishToken=token,readerABI=READER_ABI,
                  seal=seal.metadata,previousGeneration=(expected_head or {}).get('generation'))
        raw=compact(head)
        if len(raw.encode())>MAX_SEAL:
            self.writer_lock.release();raise core.Deferred('Pointer metadata exceeds its cap')
        try:
            conn=self.main_write_factory()
            if conn.in_transaction:raise core.Deferred('Fresh writer connection is required')
            conn.set_progress_handler(guard.progress,1000)
            conn.execute('BEGIN IMMEDIATE')
            if expected_pending_generation is not None:
                core.assert_pending(conn,expected_pending_generation)
            if core.read_fence(conn,self.external)!=seal.metadata['fence']:
                raise core.RevisionConflict('Fresh pointer transaction source fence differs')
            if core.get_json(conn,HEAD_KEY)!=expected_head:
                raise core.RevisionConflict('Pointer compare-and-swap lost; retain the newer head')
            if expected_head and not expected_head.get('disabled') and expected_head.get('currentness')!='retained_saved_generation':
                core.put_json(conn,ROLLBACK_KEY,expected_head)
            core.put_json(conn,HEAD_KEY,head)
            core.put_json(conn,core.PENDING_KEY,dict(version=1,generation=seal.generation,
                state='published',publishToken=token,publicationKind='immutable_generation_pointer'))
            guard.rows+=3;guard.bytes+=len(raw.encode())+len(compact(expected_head).encode());guard.check()
            conn.commit()
            guard.check()
            return dict(state='published',head=head,guardVM=guard.steps,
                        seconds=self.publish_budget.seconds-(guard.deadline-time.monotonic()))
        except BaseException as error:
            if conn is not None:
                conn.set_progress_handler(None,0)
                try:
                    if conn.in_transaction:conn.rollback()
                    with closing(self.main_read_factory()) as check:
                        saved=core.get_json(check,HEAD_KEY)
                    if saved and saved.get('publishToken')==token:
                        return dict(state='published',head=saved,commitAcknowledgementRecovered=True)
                except BaseException as uncertainty:
                    raise core.PublishUncertain(token,uncertainty) from error
            raise
        finally:
            try:
                if conn is not None:
                    conn.set_progress_handler(None,0);conn.close()
            finally:self.writer_lock.release()

    def deactivate(self,expected_head,reason='compatible_source_rollback'):
        """Retain a rollback tombstone and all files; serve canonical as pending."""
        self.admission('deactivate')
        if not expected_head or expected_head.get('disabled'):
            raise core.Deferred('Exact active head is required for reversible deactivation')
        if not self.writer_lock.acquire(blocking=False):raise core.Deferred('Writer opportunity busy')
        token=uuid.uuid4().hex
        tombstone=dict(version=1,disabled=True,publishToken=token,reason=str(reason)[:128],
                       previousHead=expected_head)
        guard=core.Guard(self.publish_budget,self.growth_probe)
        c=None
        try:
            c=self.main_write_factory();c.set_progress_handler(guard.progress,1000)
            c.execute('BEGIN IMMEDIATE')
            if core.get_json(c,HEAD_KEY)!=expected_head:
                raise core.RevisionConflict('Rollback lost pointer CAS; do not replace a newer head')
            if expected_head.get('currentness')!='retained_saved_generation':
                core.put_json(c,ROLLBACK_KEY,expected_head)
            core.put_json(c,HEAD_KEY,tombstone)
            guard.rows=1;guard.bytes=len(compact(tombstone).encode());guard.check()
            c.commit();guard.check()
            return tombstone
        except BaseException as error:
            if c is not None:
                c.set_progress_handler(None,0)
                try:
                    if c.in_transaction:c.rollback()
                    with closing(self.main_read_factory()) as check:saved=core.get_json(check,HEAD_KEY)
                    if saved and saved.get('publishToken')==token:return saved
                except BaseException as uncertainty:raise core.PublishUncertain(token,uncertainty) from error
            raise
        finally:
            try:
                if c is not None:c.set_progress_handler(None,0);c.close()
            finally:self.writer_lock.release()

    def restore_saved(self,seal,expected_head):
        """Explicit rollback may serve a retained generation as stale, never current."""
        self.admission('restore_saved')
        self._validate_head_file({'generation':seal.generation,'seal':seal.metadata})
        if not self.writer_lock.acquire(blocking=False):raise core.Deferred('Writer opportunity busy')
        token=uuid.uuid4().hex
        head=dict(version=1,generation=seal.generation,publishToken=token,readerABI=READER_ABI,
            seal=seal.metadata,currentness='retained_saved_generation',
            previousGeneration=(expected_head or {}).get('generation'))
        guard=core.Guard(self.publish_budget,self.growth_probe);c=None
        try:
            c=self.main_write_factory();c.set_progress_handler(guard.progress,1000)
            c.execute('BEGIN IMMEDIATE')
            if core.get_json(c,HEAD_KEY)!=expected_head:
                raise core.RevisionConflict('Saved rollback lost pointer CAS')
            if expected_head and not expected_head.get('disabled') and expected_head.get('currentness')!='retained_saved_generation':
                core.put_json(c,ROLLBACK_KEY,expected_head)
            # Record the real current fence separately; do not rewrite source
            # revisions to pretend an older saved generation is fresh.
            head['observedFenceAtRollback']=core.read_fence(c,self.external)
            core.put_json(c,HEAD_KEY,head)
            guard.rows=1;guard.bytes=len(compact(head).encode());guard.check()
            c.commit();guard.check()
            return head
        except BaseException as error:
            if c is not None:
                c.set_progress_handler(None,0)
                try:
                    if c.in_transaction:c.rollback()
                    with closing(self.main_read_factory()) as check:saved=core.get_json(check,HEAD_KEY)
                    if saved and saved.get('publishToken')==token:return saved
                except BaseException as uncertainty:raise core.PublishUncertain(token,uncertainty) from error
            raise
        finally:
            try:
                if c is not None:c.set_progress_handler(None,0);c.close()
            finally:self.writer_lock.release()

    def reader(self):
        """Rows, three-window receipts and source metadata share one main snapshot."""
        self.admission('saved_read')
        # Cold content validation occurs outside a retained main read snapshot.
        # The fresh selection below rejects a head changed during preparation.
        with closing(self.main_read_factory()) as probe:
            prepared=core.get_json(probe,HEAD_KEY)
            if prepared is not None and not prepared.get('disabled') and (prepared.get('currentness')=='retained_saved_generation' or core.read_fence(probe,self.external)==prepared.get('seal',{}).get('fence')):
                self._validate_head_file(prepared)
        c=self.main_read_factory()
        if not isinstance(c,ReadConnection) or c.in_transaction:
            c.close();raise core.Deferred('Fresh owned read connection is required')
        try:
            c.execute('BEGIN')
            head=core.get_json(c,HEAD_KEY)
            c._derived_generation_status='legacy_canonical'
            if head is not None:
                if head.get('disabled'):
                    c._derived_generation_status='disabled_generation_canonical_fallback'
                elif head.get('currentness')!='retained_saved_generation' and core.read_fence(c,self.external)!=head.get('seal',{}).get('fence'):
                    c._derived_generation_status='stale_generation_canonical_fallback'
                else:
                    if head!=prepared:raise core.RevisionConflict('Generation head changed during reader preparation')
                    seal=self._validate_head_file(head)
                    c._generation_pin_release=self.pin_reader(head['generation'])
                    c.execute('PRAGMA query_only=OFF')
                    c.execute('ATTACH DATABASE ? AS saved_generation',
                              (seal.path.as_uri()+'?mode=ro&immutable=1',))
                    dictionary=c.execute('SELECT value FROM saved_generation.settings WHERE key=?',(DICTIONARY_KEY,)).fetchone()[0]
                    codec.DictionaryBuilder(dictionary)
                    c.create_function('saved_decode_groups',1,
                                      lambda raw:codec.decode(raw,dictionary),deterministic=True)
                    columns=[('saved_decode_groups(groups_json) AS groups_json' if name=='groups_json' else name)
                             for name in core.LOOKUP_COLUMNS]
                    c.execute('CREATE TEMP VIEW market_item_lookup AS SELECT rowid,'+
                              ','.join(columns)+' FROM saved_generation.market_item_lookup')
                    c.execute('CREATE TEMP VIEW item_market_metrics AS SELECT '+
                              ','.join(core.METRIC_COLUMNS)+' FROM saved_generation.item_market_metrics')
                    literals=','.join("'"+key.replace("'","''")+"'" for key in RECEIPTS)
                    c.execute('CREATE TEMP VIEW settings AS SELECT key,value FROM saved_generation.settings WHERE key IN ('+
                              literals+') UNION ALL SELECT key,value FROM main.settings WHERE key NOT IN ('+literals+')')
                    c._derived_generation=head['generation']
                    c._derived_generation_metadata=seal.metadata
                    c._derived_generation_indexes=frozenset(row[0] for row in c.execute(
                        "SELECT name FROM saved_generation.sqlite_schema WHERE type='index' AND sql IS NOT NULL LIMIT 32"))
                    c._derived_generation_status='retained_generation_stale' if head.get('currentness')=='retained_saved_generation' else 'current_generation'
            c.execute('PRAGMA query_only=ON')
            c._snapshot_pinned=True
            return c
        except BaseException:
            c.close()
            raise
