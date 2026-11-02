"""Explicit bounded pointer host. No adoption, timer, provider or cleanup at import."""
from contextlib import closing
from pathlib import Path
import hashlib,json,threading,time,types,uuid
import bulk_derived_staging as core
import bulk_staging_host as legacy_host
import bulk_staging_runtime as legacy_runtime
import generation_repository as saved
import generation_lifecycle
import normalized_staging

EXTRA_COMPONENTS=('generation_host.py','generation_repository.py','generation_lifecycle.py',
    'normalized_staging.py','normalized_lookup.py','lookup_group_codec.py','market_slice_cache.py')

def descriptor(head):
    if not head or head.get('disabled') or not head.get('seal'):return None
    return dict(generation=head['generation'],complete=True,fence=head['seal']['fence'],
                asOf=head['seal']['asOf'])

class PointerStaging(normalized_staging.NormalizedStaging):
    def __init__(self,*args,repository,lifecycle,**kwargs):
        super().__init__(*args,**kwargs)
        self.repository=repository;self.lifecycle=lifecycle;self.active_generation=None

    def stage_probe(self,path):
        self.admission('stage_progress')
        return super().stage_probe(path)

    def directory_admission(self,new_generation=False,unit_reserve=False):
        reserve=self.stage_budget.file_bytes if new_generation else 0
        if unit_reserve and self.active_generation:
            reserve=max(0,self.stage_budget.file_bytes-self.stage_size(self.path(self.active_generation)))
        return self.lifecycle.inventory(reserve,new_generation)

    def create_or_resume(self):
        # A complete sealed stage is immutable. It can be published again only
        # under its exact current fence, never reopened for a cash retarget.
        with self.source(core.Guard(self.read_budget)) as (c,fence):pending=core.get_json(c,core.PENDING_KEY)
        if pending and pending.get('state')!='published':
            generation=pending['generation'];path=self.path(generation);sealpath=path.with_suffix('.seal.json')
            if sealpath.is_file():
                metadata=json.loads(sealpath.read_text(encoding='utf-8'))
                seal=self.repository._validate_head_file(dict(generation=generation,seal=metadata))
                if metadata['fence']!=fence:raise core.RevisionConflict('Sealed unpublished stage inputs changed; queue a new generation')
                with closing(saved.readonly(seal.path,immutable=True)) as stage:return core.get_json(stage,core.STATE_KEY)
        # Reuse the pinned original algorithm and its unit/CAS behavior. Only
        # its old canonical-publish lookup is mapped to the new saved head.
        namespace=dict(core.Staging.create_or_resume.__globals__)
        def get_json(conn,key):
            return descriptor(core.get_json(conn,saved.HEAD_KEY)) if key==core.PUBLISHED_KEY else core.get_json(conn,key)
        namespace['get_json']=get_json
        method=types.FunctionType(core.Staging.create_or_resume.__code__,namespace,
            core.Staging.create_or_resume.__name__,core.Staging.create_or_resume.__defaults__)
        return method(self)

    def run_unit(self,generation,*args,**kwargs):
        self.active_generation=generation
        try:return super().run_unit(generation,*args,**kwargs)
        finally:self.active_generation=None

    def estimate(self,generation):
        # Scan only bounded C-derived data. The full old DELETE/INSERT costs no
        # longer apply; no main-derived data rows are rewritten by publication.
        factory=(lambda:saved.readonly(self.path(generation),immutable=True)) if self.path(generation).with_suffix('.seal.json').is_file() else (lambda:self.open_stage(generation))
        with closing(factory()) as c:
            state=core.get_json(c,core.STATE_KEY);guard=core.Guard(self.stage_budget)
            c.set_progress_handler(guard.progress,1000)
            try:
                proof=self.preflight(c,state,strict=False)
                dictionary_bytes=len((core.get_setting(c,saved.DICTIONARY_KEY) or '').encode())
                proof['settingBytes']+=dictionary_bytes
                proof['expandedSQLBytes']+=2*dictionary_bytes
                if proof['payloadBytes']+proof['settingBytes']>self.publish_budget.result_bytes:
                    proof['admissionFailures'].append('payload_and_dictionary_bytes')
                if proof['expandedSQLBytes']>self.publish_budget.expanded_sql_bytes:
                    proof['admissionFailures'].append('expanded_sql_and_dictionary_bytes')
                proof['preflightFits']=not proof['admissionFailures']
            finally:c.set_progress_handler(None,0)
        proof.update(oldCanonicalRows=None,totalDeleteInsertOperations=0,publicationKind='immutable_generation_pointer')
        return proof

    def publish(self,generation):
        # Hashing/sealing occurs entirely before the main writer is requested.
        preparation_started=time.perf_counter()
        path=self.path(generation);sealpath=path.with_suffix('.seal.json')
        if sealpath.is_file():
            metadata=json.loads(sealpath.read_text(encoding='utf-8'))
            seal=self.repository._validate_head_file(dict(generation=generation,seal=metadata))
        else:
            hashes={};guard=core.Guard(self.stage_budget)
            with closing(self.open_stage(generation)) as c:
                dictionary=core.get_setting(c,saved.DICTIONARY_KEY)
                for table,columns in (('item_market_metrics',core.METRIC_COLUMNS),('market_item_lookup',core.LOOKUP_COLUMNS)):
                    h=hashlib.sha256()
                    for raw in c.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY rowid'):
                        row=list(raw)
                        if table=='market_item_lookup':row[17]=saved.codec.decode(row[17],dictionary)
                        h.update(json.dumps(row,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n')
                        guard.rows+=1
                        if guard.rows%256==0:guard.check()
                    hashes[table]=h.hexdigest()
            seal=self.repository.seal(generation,hashes)
        with closing(self.source_reader()) as c:head=core.get_json(c,saved.HEAD_KEY)
        preparation_seconds=time.perf_counter()-preparation_started
        publication_started=time.perf_counter()
        result=self.repository.publish(seal,head,expected_pending_generation=generation)
        self.last_publication=dict(sealedPreparationSeconds=preparation_seconds,
                                  publicationSeconds=time.perf_counter()-publication_started)
        return dict(state=result['state'],generation=generation,publishToken=result['head']['publishToken'],
                    insertRows=0,guardVM=result.get('guardVM'),publicationKind='immutable_generation_pointer')

class PointerHost(legacy_host.Host):
    def pump(self,progress=None):
        result=super().pump(progress)
        measured=getattr(self.engine,'last_publication',None)
        if result.get('complete') and measured:
            result.update(measured)
            result['stagingSeconds']+=measured['sealedPreparationSeconds']
        return result

class Runtime(legacy_runtime.Runtime):
    def __init__(self,namespace,*args,**kwargs):
        super().__init__(namespace,*args,**kwargs)
        self.repository=None;self.lifecycle=None;self.normalized_schema_sha=None

    def native_guard(self):
        super().native_guard()
        if self.ns['CATALOG_STORE_ENABLED'] or self.ns['CATALOG_STORE_WRITES']:
            raise core.Deferred('Pointer v1 is qualified for existing native0/0 only; do not change catalog switches')

    def component_identity(self):
        pins,signatures=super().component_identity();app=Path(self.ns['__file__']).resolve().parent
        for name in EXTRA_COMPONENTS:
            path=app/name;s=path.stat()
            if s.st_size>1024**2:raise core.Deferred('Pointer component size admission failed')
            pins[name]=hashlib.sha256(path.read_bytes()).hexdigest();signatures[str(path)]=(s.st_size,s.st_mtime_ns)
        return pins,signatures

    def external(self):
        result=super().external()
        result.update(savedGenerationReaderABI=saved.READER_ABI,normalizationFormat=saved.FORMAT,
                      normalizedSchemaSHA256=self.normalized_schema_sha)
        return result

    def _build_host(self):
        policy=self.operator['policy'];app=Path(self.ns['__file__']).resolve().parent
        original=(app/'bulk-staging-sidecar-schema.sql').read_text(encoding='utf-8')
        import re
        match=re.search(r'CREATE TABLE IF NOT EXISTS item_market_metrics\s*\(.*?\);',original,re.S)
        if not match:raise core.Deferred('Pinned metric schema declaration missing')
        metric_sql=match.group(0).removesuffix(';').replace('CREATE TABLE IF NOT EXISTS','CREATE TABLE',1)
        schema=normalized_staging.schema(metric_sql)
        self.normalized_schema_sha=hashlib.sha256(schema.encode()).hexdigest()
        self.repository=saved.Repository(policy['directory'],lambda:saved.readonly(Path(self.ns['DB_FILE']).resolve()),
            lambda:self.ns['steam_ask_db'](write=True),self.ns['SQLITE_WRITE_LOCK'],self.external,
            self.admission,self.live_growth,core.Budget(**policy['stage']),core.Budget(**policy['publish']))
        # No existing policy flag can authorize live generation deletion.
        self.lifecycle=generation_lifecycle.Lifecycle(self.repository,policy['directoryBytes'],policy['maxGenerations'],cleanup_enabled=False)
        self.lifecycle.prepare()
        engine=PointerStaging(policy['directory'],schema,lambda:self.ns['steam_ask_db'](),
            lambda:self.ns['steam_ask_db'](write=True),self.ns['SQLITE_WRITE_LOCK'],self.external,
            self.admission,self.live_growth,core.Budget(**policy['stage']),core.Budget(**policy['read']),
            core.Budget(**policy['publish']),self.ns['METRIC_BUILD_VERSION'],self.ns['LOOKUP_BUILD_VERSION'],
            repository=self.repository,lifecycle=self.lifecycle)
        self.ns['DERIVED_GENERATION_REPOSITORY']=self.repository
        self.ns['DERIVED_GENERATION_READS_ENABLED']=True
        return PointerHost(engine,self.ns,policy['cycleSeconds'],policy['cycleUnits'])

    def adopt(self,data):
        previous=(self.ns.get('DERIVED_GENERATION_REPOSITORY'),self.ns.get('DERIVED_GENERATION_READS_ENABLED',False),self.host)
        try:
            result=super().adopt(data)
            if previous[0] is not None and previous[0] is not self.repository:previous[0].close()
            return result
        except BaseException:
            if self.host is previous[2]:
                if self.repository is not None and self.repository is not previous[0]:self.repository.close()
                self.ns['DERIVED_GENERATION_REPOSITORY'],self.ns['DERIVED_GENERATION_READS_ENABLED']=previous[:2]
            raise

    def _prune(self):
        if self.lifecycle is None:return
        self.lifecycle.prepare_candidates(maximum=1)
        plan=self.lifecycle.plan()
        with self.lock:self.last['lifecycle']=dict(protected=plan['protected'],eligible=len(plan['eligible']),
            blocked=plan['blocked'],retirementEnabled=False,inputArchiveBounded=True)

    def _queue_native_only_mismatch(self):
        # Native0/0 is fixed. All qualified producers already queue an input
        # epoch. A mismatch lacking that receipt fails closed for explicit audit.
        with closing(self.ns['steam_ask_db']()) as c:
            pending=core.get_json(c,core.PENDING_KEY);head=core.get_json(c,saved.HEAD_KEY)
            if pending and pending.get('state')!='published':return pending
            if head and head.get('seal',{}).get('fence')!=core.read_fence(c,self.external):
                raise core.Deferred('Pointer input changed without a pending writer receipt')
            return pending

    def _restart_conflicting_stage(self):
        with closing(self.ns['steam_ask_db']()) as c:pending=core.get_json(c,core.PENDING_KEY)
        if not pending or pending.get('state')=='published':return
        path=self.host.engine.path(pending['generation'])
        if not path.exists():return
        with closing(saved.readonly(path)) as c:state=core.get_json(c,core.STATE_KEY)
        if not state:return
        if not self.ns['SQLITE_WRITE_LOCK'].acquire(blocking=False):return
        try:
            with closing(self.ns['steam_ask_db'](write=True)) as c:
                guard=core.Guard(self.host.engine.publish_budget,self.live_growth)
                c.set_progress_handler(guard.progress,1000);c.execute('BEGIN IMMEDIATE')
                latest=core.get_json(c,core.PENDING_KEY);current=core.read_fence(c,self.external)
                if latest and latest.get('generation')==pending['generation'] and current!=state['fence']:
                    core.put_json(c,core.PENDING_KEY,dict(version=1,generation=uuid.uuid4().hex,
                        state='source_committed_derived_pending',lane='qualified_stale_stage_restart',requestedAt=int(time.time())))
                guard.check();c.commit();c.set_progress_handler(None,0)
        finally:self.ns['SQLITE_WRITE_LOCK'].release()

    def saved_status(self,conn):
        head=core.get_json(conn,saved.HEAD_KEY);state=self.snapshot()
        if not head or head.get('disabled'):return dict(state,derivedCurrent=False)
        metadata=head['seal'];current=head.get('currentness')!='retained_saved_generation' and metadata['fence']==core.read_fence(conn,self.external)
        return dict(state,derivedCurrent=current,publishedGeneration=head['generation'],asOf=metadata['asOf'],
                    state='current' if current else 'source_committed_derived_pending')

    def decorate_saved(self,conn,receipt):
        result=dict(receipt or {});status=self.saved_status(conn);result['derivedPublication']=status
        head=core.get_json(conn,saved.HEAD_KEY)
        if not head or head.get('disabled'):return result
        metadata=head['seal'];counts=metadata['counts']
        result.update(derivedCountsAsOf=metadata['asOf'],derivedRebuildRequired=not status.get('derivedCurrent'))
        for field,table in (('metrics','item_market_metrics'),('lookup','market_item_lookup')):
            result[field]={**result.get(field,{}),'rows':counts[table],'countsAsOf':metadata['asOf'],'generation':head['generation']}
        result['stores']={**result.get('stores',{}),'lookupRows':counts['market_item_lookup'],
            'currentMetricRows':counts['item_market_metrics'] if status.get('derivedCurrent') else None}
        return result
