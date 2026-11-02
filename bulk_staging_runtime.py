"""Explicit local adoption + one derived-only continuation worker. Import has no I/O."""
from contextlib import closing
import ctypes
from ctypes import wintypes
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time
import uuid

import bulk_derived_staging as core
import bulk_staging_host

OPERATOR_KEY = 'bulk_staging_operator:v1'
STATUS_KEY = 'bulk_staging_runtime_status:v1'
ENGINE_SHA = '49bddb7e0339bbc31537568e379e088b156a53da5caaf6356d6bedd02c57549a'
HOST_SHA = '4488f2cf1130093c654ba35d63214bb157ef1a2b89f0727752a28d11c472c47d'
SCHEMA_SHA = '3a1ac6013963875eb2d09705ca977bdc36a8d0ae4cc7941efdcbd47beb794b1b'
MAX_BUDGET = dict(seconds=60,vm_steps=150_000_000,rows=1_000_000,
    result_bytes=256*1024**2,file_bytes=512*1024**2,growth_bytes=512*1024**2,
    python_lines=100_000_000,expanded_sql_bytes=512*1024**2)


def self_private_bytes():
    class Counters(ctypes.Structure):
        _fields_ = [('cb',wintypes.DWORD),('faults',wintypes.DWORD)] + [
            (name,ctypes.c_size_t) for name in ('peakWS','workingSet','peakPaged','paged',
                'peakNonPaged','nonPaged','pagefile','peakPagefile','private')]
    value = Counters()
    value.cb = ctypes.sizeof(value)
    kernel = ctypes.WinDLL('kernel32',use_last_error=True)
    psapi = ctypes.WinDLL('psapi',use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = (wintypes.HANDLE,ctypes.c_void_p,wintypes.DWORD)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(),ctypes.byref(value),value.cb):
        raise core.Deferred('Current process memory admission unavailable')
    return int(value.private)


def policy_checked(value, approved_root):
    required = {'directory','stage','read','publish','directoryBytes','maxGenerations',
                'maxPrivateBytes','cycleSeconds','cycleUnits','pruneObsoleteDerived'}
    if not isinstance(value,dict) or set(value) != required:
        raise ValueError('One complete reviewed derived-worker policy is required')
    result = json.loads(core.compact(value))
    directory = Path(result['directory']).resolve()
    approved_root = Path(approved_root).resolve()
    if directory.drive.upper() != 'C:' or directory == approved_root or approved_root not in directory.parents:
        raise ValueError('Derived cache must be a named child of the approved private C: root')
    for parent in [directory] + list(directory.parents):
        if parent.exists() and (parent.is_symlink() or getattr(parent,'is_junction',lambda:False)()):
            raise ValueError('Derived cache cannot traverse links/junctions')
        if parent == approved_root:
            break
    result['directory'] = str(directory)
    for phase in ('stage','read','publish'):
        budget = result[phase]
        if not isinstance(budget,dict) or set(budget) != set(MAX_BUDGET):
            raise ValueError('Every stage/read/publish bound must be explicit')
        if type(budget['seconds']) not in (int,float) or not math.isfinite(budget['seconds']):
            raise ValueError('Every deadline must be a finite positive number')
        core.Budget(**budget)
        if any(budget[key] > maximum for key,maximum in MAX_BUDGET.items()):
            raise ValueError('Reviewed maximum budget exceeded')
    if result['read']['seconds'] > 10 or result['publish']['seconds'] > 8:
        raise ValueError('Read/publication deadline exceeds reviewed initial ceiling')
    if (type(result['directoryBytes']) is not int or not 1 <= result['directoryBytes'] <= 1024**3
            or type(result['maxGenerations']) is not int or not 2 <= result['maxGenerations'] <= 3
            or type(result['maxPrivateBytes']) is not int or not 128*1024**2 <= result['maxPrivateBytes'] <= 1024**3
            or type(result['cycleUnits']) is not int or not 1 <= result['cycleUnits'] <= 256
            or type(result['cycleSeconds']) not in (int,float) or not math.isfinite(result['cycleSeconds'])
            or not 0 < result['cycleSeconds'] <= 20 or type(result['pruneObsoleteDerived']) is not bool):
        raise ValueError('Invalid directory/memory/cycle policy')
    if result['stage']['file_bytes'] > result['directoryBytes']:
        raise ValueError('One sidecar cannot exceed the entire cache directory')
    return result


class Runtime:
    def __init__(self, namespace, approved_root=None, memory_probe=self_private_bytes, host_factory=None):
        self.ns = namespace
        self.approved_root = Path(approved_root or (Path(os.environ.get('LOCALAPPDATA','C:/invalid'))/'CS2Dash'/'bulk-derived'))
        self.memory_probe, self.host_factory = memory_probe, host_factory
        self.lock = threading.RLock()
        self.event, self.stop_event = threading.Event(), threading.Event()
        self.thread = None
        self.host = None
        self.operator = None
        self.paused = True
        self.uncertain = None
        self.last = {'state':'not_adopted','workerRunning':False}
        self._memory_at, self._last_private = 0.0, None
        self._source_signature = None
        self._component_signatures = {}
        self._cache_lease = None

    def native_guard(self):
        enabled,writes = self.ns.get('CATALOG_STORE_ENABLED'),self.ns.get('CATALOG_STORE_WRITES')
        if type(enabled) is not bool or type(writes) is not bool or (writes and not enabled):
            raise core.Deferred('Supported adopted native modes are 0/0, 1/0 and 1/1')
        # v3 coverage is pinned by server/components; registry-only writes
        # are reconciled separately from all legacy numerical inputs.

    def source_identity(self):
        source = Path(self.ns['__file__']).resolve()
        if source.stat().st_size > 1024**2:
            raise core.Deferred('Source pin admission exceeded')
        return hashlib.sha256(source.read_bytes()).hexdigest(), (source.stat().st_size,source.stat().st_mtime_ns)

    def component_identity(self):
        app = Path(core.__file__).resolve().parent
        paths = (Path(core.__file__),Path(bulk_staging_host.__file__),Path(__file__),
                 app/'cash_prices.py',app/'bulk-staging-sidecar-schema.sql',
                 app/'catalog_store.py',app/'catalog_migrate.py',app/'catalog_migration_guard.py')
        pins,signatures = {},{}
        for path in paths:
            path = path.resolve()
            stat = path.stat()
            if stat.st_size > 1024**2:
                raise core.Deferred('Component source pin admission exceeded')
            pins[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
            signatures[str(path)] = (stat.st_size,stat.st_mtime_ns)
        if (pins['bulk_derived_staging.py'] != ENGINE_SHA or pins['bulk_staging_host.py'] != HOST_SHA
                or pins['bulk-staging-sidecar-schema.sql'] != SCHEMA_SHA):
            raise core.Deferred('Qualified engine/host/schema component pin changed')
        expected_native = {'catalog_store.py': 'dadad303e5f6803eed87703940006b51283ea688146f265ebb5c84aa3e89414b', 'catalog_migrate.py': '87d0ce266db86ad93c95a635ac47ee51488f63d35f007d91590dfa9dd23d66f7', 'catalog_migration_guard.py': '8671a0bad027db1059f6c90e06f9f1535436a06101c13a0c84ee11eabbc840ad'}
        if any(pins.get(name) != expected for name,expected in expected_native.items()):
            raise core.Deferred('Audited native producer component changed; explicit requalification required')
        return pins,signatures

    def external(self):
        self.native_guard()
        source = Path(self.ns['__file__'])
        signature = (source.stat().st_size,source.stat().st_mtime_ns)
        if signature != self._source_signature:
            raise core.Deferred('Running source file changed; explicit adoption is required')
        for name,expected in self._component_signatures.items():
            stat = Path(name).stat()
            if (stat.st_size,stat.st_mtime_ns) != expected:
                raise core.Deferred('A derived source component changed; explicit adoption is required')
        aliases = self.ns.get('COLLECTION_CASE_ALIAS_CACHE')
        if aliases is not None and (not isinstance(aliases,dict) or len(aliases) > 8192):
            raise core.Deferred('Collection alias dependency exceeds its reviewed bound')
        encoded = core.compact(aliases).encode()
        if len(encoded) > 512*1024:
            raise core.Deferred('Collection alias dependency byte admission exceeded')
        return {'candidateSourceSHA256':self.operator['sourceSHA256'],
                'engineSHA256':ENGINE_SHA,'schemaSHA256':SCHEMA_SHA,
                'componentSHA256':self.operator['componentSHA256'],
                'aliasSHA256':hashlib.sha256(encoded).hexdigest(),'nativeMode':str(int(self.ns['CATALOG_STORE_ENABLED']))+'/'+str(int(self.ns['CATALOG_STORE_WRITES']))}

    def admission(self, phase):
        self.native_guard()
        progress = phase in ('stage_progress','publication_progress')
        if (self.paused or self.stop_event.is_set() or self.uncertain) and not progress:
            raise core.Deferred('Derived continuation is paused/stopping/uncertain')
        now = time.monotonic()
        if progress and now-self._memory_at < 0.1:
            return
        self.external()
        if phase in ('stage','estimate','publish') and self.ns.get('ACTIVE_DATA_TASK_ID'):
            raise core.Deferred('An explicit source task owns current admission')
        for drive in ('C:/','D:/'):
            if shutil.disk_usage(drive).free < 10*1024**3:
                raise core.Deferred('Existing 10 GiB free-space floor blocks derived work')
        if now-self._memory_at >= 0.1:
            self._last_private = self.memory_probe()
            self._memory_at = now
        if self._last_private is None or self._last_private > self.operator['policy']['maxPrivateBytes']:
            raise core.Deferred('Current process memory admission exceeded')

    def live_growth(self):
        self.admission('publication_progress')
        path = Path(self.ns['DB_FILE']).resolve()
        return {str(p):p.stat().st_size if p.exists() else 0 for p in (path,Path(str(path)+'-wal'))}

    def _build_host(self):
        if self.host_factory:
            return self.host_factory(self)
        policy = self.operator['policy']
        app = Path(self.ns['__file__']).resolve().parent
        if hashlib.sha256(Path(core.__file__).read_bytes()).hexdigest() != ENGINE_SHA:
            raise core.Deferred('Qualified engine source pin changed')
        schema = (app/'bulk-staging-sidecar-schema.sql').read_text(encoding='utf-8')
        if hashlib.sha256(schema.encode()).hexdigest() != SCHEMA_SHA:
            raise core.Deferred('Qualified sidecar schema pin changed')
        engine = core.Staging(policy['directory'],schema,
            lambda:self.ns['steam_ask_db'](),lambda:self.ns['steam_ask_db'](write=True),
            self.ns['SQLITE_WRITE_LOCK'],self.external,self.admission,self.live_growth,
            core.Budget(**policy['stage']),core.Budget(**policy['read']),core.Budget(**policy['publish']),
            self.ns['METRIC_BUILD_VERSION'],self.ns['LOOKUP_BUILD_VERSION'])
        engine.directory_budget_bytes = policy['directoryBytes']
        engine.max_generations = policy['maxGenerations']
        original_probe = engine.stage_probe
        def guarded_probe(path):
            self.admission('stage_progress')
            return original_probe(path)
        engine.stage_probe = guarded_probe
        original_estimate = engine.estimate
        def remember_estimate(generation):
            proof = original_estimate(generation)
            with self.lock:
                self.last['admissionEstimate'] = self.compact_estimate(proof)
            return proof
        engine.estimate = remember_estimate
        return bulk_staging_host.Host(engine,self.ns,policy['cycleSeconds'],policy['cycleUnits'])

    @staticmethod
    def compact_estimate(proof):
        return {key:proof.get(key) for key in ('insertRows','oldCanonicalRows',
            'totalDeleteInsertOperations','payloadBytes','expandedSQLBytes',
            'stageFileBytes','preflightFits','admissionFailures')}

    def request(self, timings=None):
        # Source-writer notification only. GET/status never calls this method.
        with self.lock:
            if timings:
                self.last['sourceTransactionSeconds'] = timings.get('sourceTransactionSeconds')
            self.event.set()
            return {'state':'source_committed_derived_pending','complete':False,
                    'continuationScheduled':bool(self.thread and self.thread.is_alive())}

    def _start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                self.event.set()
                return
            self.stop_event.clear()
            import msvcrt
            lease_path = Path(self.operator['policy']['directory'])/'continuation.lock'
            lease = lease_path.open('a+b')
            try:
                if lease_path.stat().st_size == 0:
                    lease.write(b'0')
                    lease.flush()
                lease.seek(0)
                msvcrt.locking(lease.fileno(),msvcrt.LK_NBLCK,1)
            except BaseException:
                lease.close()
                raise core.Deferred('Another process owns this derived cache continuation')
            self._cache_lease = lease
            self.thread = threading.Thread(target=self._loop,name='bulk-derived-continuation',daemon=True)
            self.thread.start()

    def _save_status(self):
        if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):
            return
        try:
            with closing(self.ns['steam_ask_db'](write=True)) as conn:
                conn.execute('BEGIN IMMEDIATE')
                core.put_json(conn,STATUS_KEY,self.snapshot())
                conn.commit()
        except sqlite3.Error:
            pass  # Canonical publication/pending receipts remain authoritative.
        finally:
            self.ns['SQLITE_WRITE_LOCK'].release()

    def snapshot(self):
        with self.lock:
            return dict(self.last,workerRunning=bool(self.thread and self.thread.is_alive()),
                        paused=self.paused,stopping=self.stop_event.is_set(),uncertainPublishToken=self.uncertain)

    def _prune(self):
        if not self.operator['policy']['pruneObsoleteDerived'] or self.uncertain:
            return
        with closing(self.ns['steam_ask_db']()) as conn:
            conn.execute('BEGIN')
            head,pending = core.get_json(conn,core.PUBLISHED_KEY),core.get_json(conn,core.PENDING_KEY)
        protected = {record.get('generation') for record in (head,pending) if record}
        directory = Path(self.operator['policy']['directory']).resolve()
        entries = list(directory.iterdir())
        if len(entries) > 4*self.operator['policy']['maxGenerations']+16:
            raise core.Deferred('Private cache inventory exceeds cleanup admission')
        for path in entries:
            match = re.fullmatch(r'bulk-derived-([0-9a-f]{32})\.sqlite',path.name)
            if not match or match[1] in protected:
                continue
            if path.is_symlink() or path.resolve().parent != directory:
                raise core.Deferred('Unexpected cache path; derived cleanup refused')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=0)) as stage:
                stage.execute('PRAGMA query_only=ON')
                tables = {r[0] for r in stage.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
                if tables != {'settings','item_market_metrics','market_item_lookup'}:
                    raise core.Deferred('Unrecognized cache schema; derived cleanup refused')
                state = core.get_json(stage,core.STATE_KEY)
                if (not state or state.get('sourceSHA') != core.SOURCE_SHA or state.get('schemaSHA') != SCHEMA_SHA
                        or state.get('generation') != match[1]):
                    raise core.Deferred('Unrecognized cache owner; derived cleanup refused')
            # Only this worker creates derived files. Main head/pending IDs are
            # protected, no new generation is allocated concurrently in this process.
            for target in (path,Path(str(path)+'-journal'),Path(str(path)+'-wal'),Path(str(path)+'-shm')):
                if target.exists():
                    if target.is_symlink() or target.resolve().parent != directory:
                        raise core.Deferred('Cache auxiliary path escaped approved directory')
                    target.unlink()

    def _loop(self):
        try:
            self._run_loop()
        finally:
            if self._cache_lease is not None:
                import msvcrt
                try:
                    self._cache_lease.seek(0)
                    msvcrt.locking(self._cache_lease.fileno(),msvcrt.LK_UNLCK,1)
                finally:
                    self._cache_lease.close()
                    self._cache_lease = None

    def _run_loop(self):
        while not self.stop_event.is_set():
            if self.paused or self.uncertain:
                self.event.wait(1)
                self.event.clear()
                continue
            try:
                self.admission('stage')
                self._prune()
                with closing(self.ns['steam_ask_db']()) as conn:
                    pending = core.get_json(conn,core.PENDING_KEY)
                if not pending or pending.get('state') == 'published':
                    pending = self._queue_native_only_mismatch()
                if not pending or pending.get('state') == 'published':
                    self.event.wait(1)
                    self.event.clear()
                    continue
                started = time.monotonic()
                result = self.host.pump()
                with self.lock:
                    self.last.update({k:v for k,v in result.items() if k in (
                        'state','generation','complete','stageUnits','stagingSeconds','publicationSeconds','publishToken')})
                    estimate = result.get('admissionEstimate')
                    if estimate:
                        self.last['admissionEstimate'] = self.compact_estimate(estimate)
                    self.last.update(error=None,lastCycleSeconds=time.monotonic()-started,updatedAt=int(time.time()))
                self._save_status()
                self.stop_event.wait(0.05)  # Closed unit/cycle boundary; no main writer held.
            except core.PublishUncertain as exc:
                with self.lock:
                    self.uncertain = exc.publish_token
                    self.last.update(state='publication_uncertain',error=str(exc),updatedAt=int(time.time()))
                self._save_status()
            except core.RevisionConflict as exc:
                try:
                    self._restart_conflicting_stage()
                except (core.Deferred,sqlite3.Error,OSError,ValueError):
                    pass
                with self.lock:
                    self.last.update(state='source_committed_derived_pending',error=str(exc)[:512],updatedAt=int(time.time()))
                self._save_status()
                self.stop_event.wait(0.1)
            except (core.Deferred,sqlite3.Error,OSError,ValueError) as exc:
                with self.lock:
                    self.last.update(state='source_committed_derived_pending',error=str(exc)[:512],updatedAt=int(time.time()))
                self._save_status()
                self.stop_event.wait(2)
            except BaseException as exc:
                with self.lock:
                    self.paused = True
                    self.last.update(state='worker_error',error=repr(exc)[:512],updatedAt=int(time.time()))
                self._save_status()

    def _queue_native_only_mismatch(self):
        """Derived-only closed-boundary reconciliation of audited native writers.

        No legacy rows are read/changed. A native-only registry transaction can
        advance generation without a server callback or a pending generation.
        The fresh writer snapshot must prove every other input is unchanged.
        """
        # A current generation needs no main writer admission at all. This
        # query-only snapshot closes before the nonblocking writer recheck.
        with closing(self.ns['steam_ask_db']()) as probe:
            probe.execute('BEGIN')
            pending = core.get_json(probe,core.PENDING_KEY)
            if pending and pending.get('state') != 'published':
                return pending
            head = core.get_json(probe,core.PUBLISHED_KEY)
            if not head or not head.get('complete') or head.get('fence') == core.read_fence(probe,self.external):
                return pending
        if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):
            raise core.Deferred('Writer busy; native-only reconciliation deferred')
        try:
            with closing(self.ns['steam_ask_db'](write=True)) as conn:
                conn.execute('BEGIN IMMEDIATE')
                pending = core.get_json(conn,core.PENDING_KEY)
                if pending and pending.get('state') != 'published':
                    conn.rollback()
                    return pending
                head = core.get_json(conn,core.PUBLISHED_KEY)
                if not head or not head.get('complete'):
                    conn.rollback()
                    return pending
                current = core.read_fence(conn,self.external)
                previous = head.get('fence')
                if previous == current:
                    conn.rollback()
                    return pending
                old,new = dict(previous or {}),dict(current)
                old.pop('catalogStoreGeneration',None)
                new.pop('catalogStoreGeneration',None)
                old_external,new_external = dict(old.get('external') or {}),dict(new.get('external') or {})
                old_mode,new_mode = old_external.pop('nativeMode',None),new_external.pop('nativeMode',None)
                old['external'],new['external'] = old_external,new_external
                if old != new:
                    raise core.Deferred('Non-native dependency changed without pending receipt; explicit input audit required')
                # Environment mode transitions retain a conservative metric
                # restart. Pure generation changes retain all metric work.
                lane = 'native_registry_only' if old_mode == new_mode else 'qualified_native_mode_change'
                pending = core.invalidate_inputs(conn,lane)
                conn.commit()
                return pending
        finally:
            self.ns['SQLITE_WRITE_LOCK'].release()

    def _restart_conflicting_stage(self):
        """Requeue changed metric/alias dependencies; cash-only reuse stays in engine."""
        with closing(self.ns['steam_ask_db']()) as conn:
            pending = core.get_json(conn,core.PENDING_KEY)
        if not pending or pending.get('state') == 'published':
            return
        path = self.host.engine.path(pending['generation'])
        if not path.exists():
            return
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=0)) as stage:
            stage.execute('PRAGMA query_only=ON')
            state = core.get_json(stage,core.STATE_KEY)
        if not state:
            return
        if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):
            return
        try:
            with closing(self.ns['steam_ask_db'](write=True)) as conn:
                conn.execute('BEGIN IMMEDIATE')
                latest = core.get_json(conn,core.PENDING_KEY)
                if latest and latest.get('generation') == pending['generation']:
                    current = core.read_fence(conn,self.external)
                    if core.metric_fence(current) != core.metric_fence(state['fence']):
                        core.invalidate_inputs(conn,'changed_metric_or_alias_dependency')
                conn.commit()
        finally:
            self.ns['SQLITE_WRITE_LOCK'].release()

    def adopt(self, data):
        if set(data) != {'action','expectedSourceSHA256','expectedMarketSourceRevision','policy'}:
            raise ValueError('Exact source/revision and complete policy are required for local adoption')
        self.native_guard()
        if (not isinstance(data['expectedMarketSourceRevision'],str)
                or not 1 <= len(data['expectedMarketSourceRevision']) <= 128):
            raise ValueError('One exact existing source revision is required')
        if self.ns.get('ACTIVE_DATA_TASK_ID'):
            raise core.Deferred('Close the active explicit source task before adoption')
        source_sha,signature = self.source_identity()
        if data['expectedSourceSHA256'] != source_sha:
            raise ValueError('Running source SHA does not match the operator request')
        if self.thread and self.thread.is_alive() and not self.paused:
            raise core.Deferred('Pause the existing continuation before replacing policy')
        if self.thread and self.thread.is_alive() and not self.stop()['stopped']:
            raise core.Deferred('Waiting for the previous bounded unit to close before adoption')
        policy = policy_checked(data['policy'],self.approved_root)
        directory = Path(policy['directory'])
        directory.mkdir(parents=True,exist_ok=True)
        components,component_signatures = self.component_identity()
        operator = {'version':1,'sourceSHA256':source_sha,'componentSHA256':components,'policy':policy,'paused':False,
                    'adoptedAt':int(time.time()),'adoptionToken':uuid.uuid4().hex}
        old = self.operator,self.host,self.paused,self._source_signature,self._component_signatures,self.ns.get('BULK_STAGING_HOST')
        self.operator,self.paused,self._source_signature = operator,False,signature
        self._component_signatures = component_signatures
        self.stop_event.clear()
        committed = False
        try:
            self.host = self._build_host()
            self.admission('adopt')
            if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):
                raise core.Deferred('Source writer busy; adoption was not applied')
            try:
                with closing(self.ns['steam_ask_db'](write=True)) as conn:
                    conn.execute('BEGIN IMMEDIATE')
                    if core.get_setting(conn,'market_source_revision') != data['expectedMarketSourceRevision']:
                        raise core.RevisionConflict('Source revision changed before adoption')
                    conn.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)',(core.ABI_KEY,core.WRITER_ABI))
                    core.put_json(conn,OPERATOR_KEY,operator)
                    pending = core.invalidate_inputs(conn,'explicit_saved_data_adoption')
                    try:
                        conn.commit()
                        committed = True
                    except BaseException as commit_error:
                        if conn.in_transaction:
                            conn.rollback()
                            raise
                        try:
                            with closing(self.ns['steam_ask_db']()) as check:
                                observed = core.get_json(check,OPERATOR_KEY)
                            if not observed or observed.get('adoptionToken') != operator['adoptionToken']:
                                raise core.Deferred('Adoption token did not commit') from commit_error
                            committed = True
                        except sqlite3.Error as check_error:
                            self.uncertain = 'adoption:'+operator['adoptionToken']
                            raise core.PublishUncertain(self.uncertain,check_error) from commit_error
            finally:
                self.ns['SQLITE_WRITE_LOCK'].release()
            self.ns['BULK_STAGING_HOST'] = self.host
            self.last.update(state='source_committed_derived_pending',generation=pending['generation'],error=None)
            self._start()
            return dict(self.snapshot(),adopted=True,providerCalls=0)
        except BaseException as exc:
            if committed or self.uncertain:
                self.ns['BULK_STAGING_HOST'] = self.host
                self.paused = True
                self.last.update(state='adopted_worker_deferred' if committed else 'adoption_uncertain',error=str(exc)[:512])
                self._save_status()
            else:
                self.operator,self.host,self.paused,self._source_signature,self._component_signatures,self.ns['BULK_STAGING_HOST'] = old
            raise

    def control(self,data):
        if not isinstance(data,dict):
            raise ValueError('One derived continuation control object is required')
        if data.get('action') == 'adopt':
            return self.adopt(data)
        if set(data) != {'action'} or data['action'] not in ('pause','resume') or not self.operator:
            raise ValueError('Adopt, pause or resume an explicitly configured derived continuation')
        if data['action'] == 'resume' and self.uncertain:
            raise core.Deferred('Resolve the exact uncertain publish token before resuming')
        if data['action'] == 'resume' and self.stop_event.is_set() and self.thread and self.thread.is_alive():
            raise core.Deferred('Previous bounded unit is still closing')
        self.native_guard()
        paused = data['action'] == 'pause'
        updated = dict(self.operator,paused=paused,controlToken=uuid.uuid4().hex)
        if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):
            raise core.Deferred('Writer busy; continuation control was not applied')
        try:
            with closing(self.ns['steam_ask_db'](write=True)) as conn:
                conn.execute('BEGIN IMMEDIATE')
                core.put_json(conn,OPERATOR_KEY,updated)
                try:
                    conn.commit()
                except BaseException as commit_error:
                    if conn.in_transaction:
                        conn.rollback()
                        raise
                    try:
                        with closing(self.ns['steam_ask_db']()) as check:
                            observed = core.get_json(check,OPERATOR_KEY)
                        if not observed or observed.get('controlToken') != updated['controlToken']:
                            raise core.Deferred('Continuation control token did not commit') from commit_error
                    except sqlite3.Error as verify_error:
                        self.paused = True
                        self.uncertain = 'control:'+updated['controlToken']
                        raise core.PublishUncertain(self.uncertain,verify_error) from commit_error
        finally:
            self.ns['SQLITE_WRITE_LOCK'].release()
        self.operator,self.paused = updated,paused
        if not paused:
            self._start()
        self.event.set()
        return self.snapshot()

    def restore(self):
        with closing(self.ns['steam_ask_db']()) as conn:
            operator = core.get_json(conn,OPERATOR_KEY)
            abi = core.get_setting(conn,core.ABI_KEY)
        if not operator:
            return {'state':'not_adopted'}
        self.native_guard()
        sha,signature = self.source_identity()
        components,component_signatures = self.component_identity()
        if operator.get('sourceSHA256') != sha or operator.get('componentSHA256') != components or abi != core.WRITER_ABI:
            raise core.Deferred('Saved adoption source/ABI changed; explicit adoption is required')
        operator['policy'] = policy_checked(operator['policy'],self.approved_root)
        Path(operator['policy']['directory']).mkdir(parents=True,exist_ok=True)
        self.operator,self._source_signature = operator,signature
        self._component_signatures = component_signatures
        self.paused = bool(operator.get('paused',True))
        self.host = self._build_host()
        self.ns['BULK_STAGING_HOST'] = self.host
        with closing(self.ns['steam_ask_db']()) as conn:
            status = core.get_json(conn,STATUS_KEY)
        if status:
            self.last.update(status)
            self.uncertain = status.get('uncertainPublishToken')
            self.paused = self.paused or bool(status.get('paused'))
        self._start()
        return self.snapshot()

    def saved_status(self,conn):
        # Pure snapshot read: no _start/request/adoption/cleanup path is reachable.
        if not self.operator or not self.host:
            return self.snapshot()
        state = core.published_state(conn,self.external)
        saved = core.get_json(conn,STATUS_KEY) or {}
        memory = self.snapshot()
        return {**saved,**memory,'derivedCurrent':state['current'],
                'publishedGeneration':state['publishedGeneration'],'asOf':state['asOf'],
                'state':'current' if state['current'] else memory.get('state','source_committed_derived_pending')}

    def require_bulk_mode(self):
        with closing(self.ns['steam_ask_db']()) as conn:
            abi = core.get_setting(conn,core.ABI_KEY)
            if abi is not None and abi != core.WRITER_ABI:
                raise core.Deferred('Saved derived ABI requires explicit v3 adoption before provider work')
            adopted = abi == core.WRITER_ABI
        if adopted:
            self.native_guard()
            if not self.host or not self.operator:
                raise core.Deferred('Saved derived adoption must be restored before a provider bulk task')

    def decorate_saved(self,conn,receipt):
        result = dict(receipt or {})
        status = self.saved_status(conn)
        result['derivedPublication'] = status
        head = core.get_json(conn,core.PUBLISHED_KEY)
        if not head or not head.get('complete'):
            return result
        receipts = (head.get('proof') or {}).get('receipts') or {}
        if set(receipts) != {'7','30','90'}:
            return result
        metric_windows = [{'windowDays':w,'rows':receipts[str(w)]['metric']['rows']} for w in core.WINDOWS]
        lookup_windows = [{'windowDays':w,'rows':receipts[str(w)]['lookup']['rows'],
                           'priced':receipts[str(w)]['lookup'].get('priced')} for w in core.WINDOWS]
        metric_rows,lookup_rows = sum(r['rows'] for r in metric_windows),sum(r['rows'] for r in lookup_windows)
        result['metrics'] = {**result.get('metrics',{}),'rows':metric_rows,'windows':metric_windows,
                             'countsAsOf':head['publishedAt'],'generation':head['generation']}
        result['lookup'] = {**result.get('lookup',{}),'rows':lookup_rows,'windows':lookup_windows,
                            'countsAsOf':head['publishedAt'],'generation':head['generation']}
        result['stores'] = {**result.get('stores',{}),'currentMetricRows':metric_rows,'lookupRows':lookup_rows}
        result['derivedRebuildRequired'] = not status.get('derivedCurrent',False)
        result['derivedCountsAsOf'] = head['publishedAt']
        # Provider/catalog counts retain their own original saved receipt clock.
        return result

    def stop(self,timeout=2):
        self.stop_event.set()
        self.event.set()
        thread = self.thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout)
        return {'stopped':not bool(thread and thread.is_alive()),'safeBoundaryRequested':True}
