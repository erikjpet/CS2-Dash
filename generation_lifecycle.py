"""Bounded, reversible logical-input archive for immutable derived generations.

Only selected saved metrics/lookup inputs are versioned. Raw provider payloads,
price history, holdings, cost basis and private configuration are never copied.
Live retirement is disabled by default and is not enabled by the host binding.
"""
from contextlib import closing
from pathlib import Path
import hashlib,json,os,stat,sys,time,uuid,zlib
import bulk_derived_staging as core
import generation_repository as saved
import normalized_lookup as layout

VERSION=1
ARCHIVE_FORMAT='selected-projection-reverse-inputs:v1'
TABLES=(('item_market_metrics',core.METRIC_COLUMNS,'name COLLATE BINARY,provider COLLATE BINARY,window_days'),
        ('market_item_lookup',core.LOOKUP_COLUMNS,'window_days,name COLLATE BINARY'))
CODE_FILES=('generation_lifecycle.py','generation_repository.py','normalized_staging.py',
            'normalized_lookup.py','lookup_group_codec.py','bulk_derived_staging.py','bulk_staging_host.py')
MAX_LINE=2*1024**2

def encoded(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n'
def record_key(record):return tuple(record[0])
def row_key(table,row):return [0,row[0],row[1],row[2]] if table==0 else [1,row[0],row[1]]
def add_hash(hasher,record):hasher.update(encoded(record))

def validate_record(record):
    if not isinstance(record,list) or len(record)!=3 or not isinstance(record[0],list):raise core.Deferred('Invalid logical source record')
    key,rowid,row=record
    if type(rowid) is not int or not -(2**63)<=rowid<2**63:raise core.Deferred('Invalid saved row identity')
    if key[0] not in (0,1) or len(row)!=len(TABLES[key[0]][1]) or row_key(key[0],row)!=key:
        raise core.Deferred('Logical primary key/fields disagree')
    if (key[0]==0 and (row[1]!='market' or row[2] not in core.WINDOWS)) or (key[0]==1 and row[0] not in core.WINDOWS):
        raise core.Deferred('Unexpected derived provider/window')
    return record

class Lifecycle:
    def __init__(self,repository,directory_bytes,max_generations=3,cleanup_enabled=False):
        self.repo=repository;self.directory=repository.directory
        self.directory_bytes=int(directory_bytes);self.max_generations=int(max_generations)
        if self.directory_bytes<=0 or self.max_generations<2:raise ValueError('Explicit bounded lifecycle capacity required')
        self.cleanup_enabled=bool(cleanup_enabled)
        self.inputs=self.directory/'rebuild-inputs';self.code=self.directory/'rebuild-code'
        self.receipts=self.directory/'retirement-receipts'
        self.inventory_cache=None
        self.validated_capsules={}

    def prepare(self):
        for path in (self.inputs,self.code,self.receipts):
            if path.exists() and (path.is_symlink() or not path.is_dir()):raise core.Deferred('Unexpected lifecycle directory')
            path.mkdir(exist_ok=True)

    def inventory(self,reserve=0,new_generation=False):
        self.repo.admission('stage_progress')
        self.prepare();total=0;generations=0;entries=0
        for path in self.directory.rglob('*'):
            entries+=1
            if entries>16384:raise core.Deferred('Bounded lifecycle inventory requires a paged audit; no deletion')
            if path.is_symlink() or not path.resolve().is_relative_to(self.directory):raise core.Deferred('Lifecycle path escapes private root')
            if path.is_file():
                total+=path.stat().st_size
                generations+=path.parent==self.directory and path.name.startswith('bulk-derived-') and path.suffix=='.sqlite'
        if total+reserve>self.directory_bytes or generations+int(new_generation)>self.max_generations:
            raise core.Deferred('Verified generation/input archive capacity full; no history or generation is silently removed')
        return dict(directoryBytes=total,generations=generations,entries=entries,reservedBytes=reserve)

    def _atomic_json(self,path,value):
        raw=encoded(value)
        if len(raw)>2*1024**2:raise core.Deferred('Bounded rebuild receipt is too large')
        temporary=path.with_name(path.name+'.tmp')
        with temporary.open('wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        os.replace(temporary,path)

    def _retain_code(self):
        result={}
        for name in CODE_FILES:
            source=Path(__file__).resolve().parent/name;raw=source.read_bytes()
            if len(raw)>1024**2:raise core.Deferred('Rebuild source exceeds bounded component size')
            digest=hashlib.sha256(raw).hexdigest();target=self.code/(digest+'.py')
            if not target.exists():
                with target.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise core.Deferred('Retained rebuild source is corrupt')
            target.chmod(stat.S_IREAD)
            result[name]=digest
        return result

    def records(self,seal):
        self.repo._validate_head_file(dict(generation=seal.generation,seal=seal.metadata))
        release=self.repo.pin_reader(seal.generation)
        try:
            with closing(saved.readonly(seal.path,immutable=True)) as c:
                dictionary=core.get_setting(c,saved.DICTIONARY_KEY)
                for index,(table,columns,order) in enumerate(TABLES):
                    for raw in c.execute('SELECT rowid,'+','.join(columns)+' FROM '+table+' ORDER BY '+order):
                        row=list(raw[1:])
                        if index==1:row[17]=saved.codec.decode(row[17],dictionary)
                        yield validate_record([row_key(index,row),raw[0],row])
        finally:release()

    def settings(self,seal):
        with closing(saved.readonly(seal.path,immutable=True)) as c:
            rows=[list(row) for row in c.execute('SELECT key,value FROM settings ORDER BY key')]
        if len(rows)>64 or len(encoded(rows))>1024**2:raise core.Deferred('Saved input metadata exceeds receipt bound')
        return rows

    def capture(self,source_seal,target_seal,target_settings=None,target_records=None):
        """Durably retain exact old selected inputs as a reverse field delta."""
        self.repo.admission('archive_inputs');self.prepare()
        if source_seal.generation==target_seal.generation:return None
        name='reverse-'+source_seal.generation+'-'+target_seal.generation
        path=self.inputs/(name+'.jsonl.zlib');receipt_path=self.inputs/(name+'.receipt.json')
        if receipt_path.exists():
            receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
            self.validate_capsule(receipt)
            if not receipt.get('reconstructionVerified'):
                for _ in self.apply(self.records(source_seal),receipt):pass
                receipt['reconstructionVerified']=True;self._atomic_json(receipt_path,receipt)
                path.chmod(stat.S_IREAD);receipt_path.chmod(stat.S_IREAD)
            return receipt
        guard=core.Guard(self.repo.stage_budget)
        code=self._retain_code()
        header=dict(version=VERSION,format=ARCHIVE_FORMAT,sourceGeneration=source_seal.generation,
            targetGeneration=target_seal.generation,sourceSeal=source_seal.metadata,
            targetSeal=target_seal.metadata,targetSettings=self.settings(target_seal) if target_settings is None else target_settings,codeSHA256=code,
            recoveryGuarantee='Exact logical rows, primary/row identities, saved receipts/dictionary/provenance; SQLite physical bytes are not promised')
        temporary=path.with_name(path.name+'.tmp')
        compressor=zlib.compressobj(6);hzip=hashlib.sha256();source_hash=hashlib.sha256();target_hash=hashlib.sha256()
        changes=source_rows=target_rows=0;expanded=0
        source_iter=self.records(source_seal);target_iter=self.records(target_seal) if target_records is None else target_records
        a=b=None
        try:
            with temporary.open('wb') as f:
                def write(value):
                    nonlocal expanded
                    raw=encoded(value);expanded+=len(raw)
                    if len(raw)>MAX_LINE or expanded>self.repo.stage_budget.expanded_sql_bytes:
                        raise core.Deferred('Reverse input expanded-byte/line cap reached')
                    chunk=compressor.compress(raw)
                    if chunk:f.write(chunk);hzip.update(chunk)
                    if f.tell()>self.repo.stage_budget.file_bytes:raise core.Deferred('Reverse input capsule file cap reached')
                    guard.check()
                write(header)
                a=next(source_iter,None);b=next(target_iter,None)
                while a is not None or b is not None:
                    if b is None or (a is not None and record_key(a)<record_key(b)):
                        add_hash(source_hash,a);source_rows+=1;write(['delete',a[0]])
                        changes+=1;a=next(source_iter,None)
                    elif a is None or record_key(b)<record_key(a):
                        add_hash(target_hash,b);target_rows+=1;write(['insert',b])
                        changes+=1;b=next(target_iter,None)
                    else:
                        add_hash(source_hash,a);add_hash(target_hash,b);source_rows+=1;target_rows+=1
                        if encoded(a)!=encoded(b):
                            fields=[[i,value] for i,value in enumerate(b[2]) if encoded(value)!=encoded(a[2][i])]
                            write(['set',b[0],b[1],fields]);changes+=1
                        a=next(source_iter,None);b=next(target_iter,None)
                    guard.rows=max(source_rows,target_rows)
                    if guard.rows%256==0:
                        guard.check();self.inventory()
                trailer=dict(end=True,sourceContentSHA256=source_hash.hexdigest(),targetContentSHA256=target_hash.hexdigest(),
                    sourceRows=source_rows,targetRows=target_rows,patchRecords=changes)
                write(trailer)
                chunk=compressor.flush();f.write(chunk);hzip.update(chunk);f.flush();os.fsync(f.fileno())
            if temporary.stat().st_size>self.repo.stage_budget.file_bytes:raise core.Deferred('Reverse input capsule exceeds its hard cap')
            self.inventory()
            os.replace(temporary,path)
            receipt=dict(version=VERSION,format=ARCHIVE_FORMAT,file=path.name,compressedBytes=path.stat().st_size,
                compressedSHA256=hzip.hexdigest(),header=header,**trailer)
            self._atomic_json(receipt_path,receipt)
            # Full streamed application validates every retained input before
            # this receipt may authorize eligibility. No duplicate DB is made.
            for _ in self.apply(self.records(source_seal),receipt):pass
            receipt['reconstructionVerified']=True
            self._atomic_json(receipt_path,receipt)
            path.chmod(stat.S_IREAD);receipt_path.chmod(stat.S_IREAD)
            guard.check();self.inventory();return receipt
        finally:
            source_iter.close();target_iter.close()

    def _lines(self,receipt):
        path=self.inputs/receipt['file']
        if path.parent!=self.inputs or path.is_symlink() or path.stat().st_size>self.repo.stage_budget.file_bytes:
            raise core.Deferred('Invalid bounded input capsule path')
        decoder=zlib.decompressobj();buffer=b'';expanded=0;h=hashlib.sha256()
        with path.open('rb') as f:
            while compressed:=f.read(65536):
                h.update(compressed)
                pending=compressed
                drain=True
                while pending or drain:
                    block=decoder.decompress(pending,65536);pending=decoder.unconsumed_tail
                    drain=len(block)==65536
                    expanded+=len(block);buffer+=block
                    if expanded>self.repo.stage_budget.expanded_sql_bytes or len(buffer)>MAX_LINE:
                        raise core.Deferred('Reverse input decoder bound exceeded')
                    while b'\n' in buffer:
                        line,buffer=buffer.split(b'\n',1)
                        yield json.loads(line)
            if buffer or not decoder.eof or decoder.unused_data or h.hexdigest()!=receipt['compressedSHA256']:
                raise core.Deferred('Truncated, trailing or corrupt retained input capsule')

    def validate_capsule(self,receipt):
        if receipt.get('version')!=VERSION or receipt.get('format')!=ARCHIVE_FORMAT:
            raise core.Deferred('Unsupported rebuild input version')
        path=self.inputs/receipt['file']
        code_signature=tuple((digest,saved.identity(self.code/(digest+'.py'))) for digest in receipt['header']['codeSHA256'].values())
        signature=(saved.identity(path),hashlib.sha256(encoded(receipt)).hexdigest(),code_signature)
        if self.validated_capsules.get(receipt['file'])==signature:return True
        lines=self._lines(receipt)
        try:
            header=next(lines)
            if header!=receipt['header']:raise core.Deferred('Input capsule header differs')
            for name,digest in header['codeSHA256'].items():
                if name not in CODE_FILES or hashlib.sha256((self.code/(digest+'.py')).read_bytes()).hexdigest()!=digest:
                    raise core.Deferred('Exact rebuild code is unavailable')
            last=None
            for item in lines:last=item
            if not isinstance(last,dict) or not last.get('end') or any(last.get(k)!=receipt[k] for k in
                ('sourceContentSHA256','targetContentSHA256','sourceRows','targetRows','patchRecords')):
                raise core.Deferred('Input capsule completion receipt differs')
        finally:lines.close()
        self.validated_capsules[receipt['file']]=signature
        return True

    def apply(self,source_records,receipt):
        lines=self._lines(receipt);source_hash=hashlib.sha256();target_hash=hashlib.sha256()
        source_count=target_count=changes=0;prior_key=None
        try:
            if next(lines)!=receipt['header']:raise core.Deferred('Rebuild input header mismatch')
            patch=next(lines,None);a=next(source_records,None)
            def patch_key(item):
                if isinstance(item,dict):return None
                if not isinstance(item,list) or item[0] not in ('insert','set','delete'):raise core.Deferred('Invalid reverse patch operation')
                return tuple(item[1][0]) if item[0]=='insert' else tuple(item[1])
            while a is not None or (patch is not None and not isinstance(patch,dict)):
                key=patch_key(patch) if patch is not None else None
                if key is not None and prior_key is not None and key<=prior_key:raise core.Deferred('Reverse patches are not strictly key-ordered')
                if key is not None and (a is None or key<record_key(a)):
                    if patch[0]!='insert':raise core.Deferred('Reverse patch source key is absent')
                    output=validate_record(patch[1]);add_hash(target_hash,output);target_count+=1;yield output
                    prior_key=key;changes+=1;patch=next(lines,None)
                elif a is not None and (key is None or record_key(a)<key):
                    add_hash(source_hash,a);source_count+=1;add_hash(target_hash,a);target_count+=1;yield a
                    a=next(source_records,None)
                else:
                    add_hash(source_hash,a);source_count+=1
                    if patch[0]=='insert':raise core.Deferred('Reverse insert would replace an existing source key')
                    if patch[0]=='set':
                        output=[list(a[0]),patch[2],list(a[2])];indices=set()
                        for index,value in patch[3]:
                            if type(index) is not int or not 0<=index<len(output[2]) or index in indices:raise core.Deferred('Invalid reverse field index')
                            indices.add(index);output[2][index]=value
                        validate_record(output);add_hash(target_hash,output);target_count+=1;yield output
                    prior_key=key;changes+=1;patch=next(lines,None);a=next(source_records,None)
            if not isinstance(patch,dict) or not patch.get('end') or next(lines,None) is not None:
                raise core.Deferred('Rebuild input completion is missing or duplicated')
            if (source_hash.hexdigest()!=receipt['sourceContentSHA256'] or target_hash.hexdigest()!=receipt['targetContentSHA256'] or
                source_count!=receipt['sourceRows'] or target_count!=receipt['targetRows'] or changes!=receipt['patchRecords']):
                raise core.Deferred('Exact rebuilt input hash/count differs')
        finally:
            lines.close()
            close=getattr(source_records,'close',None)
            if close is not None:close()

    def saved_roles(self,conn):
        head=core.get_json(conn,saved.HEAD_KEY);rollback=core.get_json(conn,saved.ROLLBACK_KEY);pending=core.get_json(conn,core.PENDING_KEY)
        return dict(head=head,rollback=rollback,pending=pending)

    def protected(self,roles):
        ids=set()
        for value in (roles['head'],roles['rollback'],roles['pending']):
            if value and value.get('generation'):ids.add(value['generation'])
        head=roles['head']
        if head and head.get('disabled') and head.get('previousHead',{}).get('generation'):ids.add(head['previousHead']['generation'])
        if head and not roles['rollback'] and head.get('previousGeneration'):ids.add(head['previousGeneration'])
        with self.repo.lock:ids.update(self.repo.reader_pins)
        return ids

    def graph(self):
        result={}
        for path in self.inputs.glob('reverse-*.receipt.json'):
            if path.stat().st_size>2*1024**2:raise core.Deferred('Oversized rebuild manifest')
            receipt=json.loads(path.read_text(encoding='utf-8'))
            if receipt.get('reconstructionVerified'):
                result.setdefault(receipt['header']['sourceGeneration'],[]).append(receipt)
        return result

    def find_path(self,roots,target):
        graph=self.graph();queue=[(root,[]) for root in roots];seen=set()
        while queue:
            generation,path=queue.pop(0)
            if generation==target:return path
            if generation in seen:continue
            seen.add(generation)
            if len(seen)>16384:raise core.Deferred('Bounded rebuild graph traversal exhausted')
            for edge in graph.get(generation,[]):queue.append((edge['header']['targetGeneration'],path+[edge]))
        return None

    def restored_records(self,root_seal,path):
        records=self.records(root_seal)
        for edge in path:
            self.validate_capsule(edge);records=self.apply(records,edge)
        return records

    def verify_path(self,root,edges,stream=False):
        source_hash=root.metadata['fileSHA256'];source_generation=root.generation;prior_content=None
        for edge in edges:
            self.validate_capsule(edge)
            header=edge['header']
            if (header['sourceGeneration']!=source_generation or header['sourceSeal']['fileSHA256']!=source_hash
                    or (prior_content is not None and prior_content!=edge['sourceContentSHA256'])):
                raise core.Deferred('Retained rebuild input chain has a missing or conflicting source')
            source_hash=header['targetSeal']['fileSHA256'];source_generation=header['targetGeneration'];prior_content=edge['targetContentSHA256']
        if stream:
            guard=core.Guard(self.repo.stage_budget)
            for _ in self.restored_records(root,edges):guard.rows+=1;guard.check()
        return True

    def plan(self):
        """Eligibility is a fresh, validated plan, never permission to unlink."""
        self.repo.admission('lifecycle_plan');self.inventory()
        with closing(self.repo.main_read_factory()) as c:
            c.execute('BEGIN');roles=self.saved_roles(c)
        protected=self.protected(roles)
        self.repo.release_unused(protected)
        roots={}
        for head in (roles['head'],roles['rollback']):
            if head and head.get('generation') and not head.get('disabled'):
                roots[head['generation']]=self.repo._validate_head_file(head)
        result=dict(version=1,roles=roles,protected=sorted(protected),eligible=[],blocked=[],cleanupEnabled=self.cleanup_enabled)
        for path in self.directory.glob('bulk-derived-*.sqlite'):
            generation=path.stem.removeprefix('bulk-derived-')
            if generation in protected:continue
            edges=self.find_path(roots,generation)
            if not edges:
                result['blocked'].append(dict(generation=generation,reason='Exact retained input/rebuild path unavailable'));continue
            root=roots[edges[0]['header']['sourceGeneration']]
            self.verify_path(root,edges)
            self.repo.release_unused(protected)
            try:
                exclusive=saved.ReadLease(path,exclusive=True);exclusive.close()
            except core.Deferred:
                result['blocked'].append(dict(generation=generation,reason='Another process or reader retains this file'));continue
            result['eligible'].append(dict(generation=generation,fileIdentity=list(saved.identity(path)),
                rootGeneration=root.generation,capsules=[edge['file'] for edge in edges],
                recoveredInputSHA256=edges[-1]['targetContentSHA256'],
                recovery='Exact selected logical inputs, row identities and saved receipts retained; source/user history untouched'))
        return result

    def prepare_candidates(self,maximum=1):
        """Retain inputs for obsolete sealed work before it can become eligible."""
        self.inventory()
        with closing(self.repo.main_read_factory()) as c:roles=self.saved_roles(c)
        protected=self.protected(roles)
        self.repo.release_unused(protected)
        head=roles['head']
        if not head or not head.get('generation') or head.get('disabled'):return []
        root=self.repo._validate_head_file(head);created=[]
        roots={head['generation']:root}
        rollback=roles['rollback']
        if rollback and rollback.get('generation'):roots[rollback['generation']]=self.repo._validate_head_file(rollback)
        for path in self.directory.glob('bulk-derived-*.sqlite'):
            generation=path.stem.removeprefix('bulk-derived-')
            if generation in protected or self.find_path(roots,generation) is not None:continue
            sealpath=path.with_suffix('.seal.json')
            if not sealpath.is_file():continue  # Incomplete/unknown work is never silently discarded.
            metadata=json.loads(sealpath.read_text(encoding='utf-8'))
            target=self.repo._validate_head_file(dict(generation=generation,seal=metadata))
            created.append(self.capture(root,target))
            self.repo.release_unused(protected)
            if len(created)>=maximum:break
        return created

    def retire(self,ticket):
        """Disabled for live hosts. Qualified C fixtures can exercise exact scope."""
        if not self.cleanup_enabled:raise core.Deferred('Live derived retirement is not authorized; eligibility only')
        # Keep exact retained facts/code immutable while rechecking the plan,
        # then keep these leases through the short writer/role CAS and unlink.
        input_leases=[]
        try:
            retained=set()
            for item in ticket['eligible']:
                for name in item['capsules']:
                    capsule=self.inputs/name
                    receipt=capsule.with_name(name.replace('.jsonl.zlib','.receipt.json'))
                    for path in (capsule,receipt):
                        if path not in retained:input_leases.append(saved.ReadLease(path));retained.add(path)
                    metadata=json.loads(receipt.read_text(encoding='utf-8'))
                    for digest in metadata['header']['codeSHA256'].values():
                        path=self.code/(digest+'.py')
                        if path not in retained:input_leases.append(saved.ReadLease(path));retained.add(path)
            confirmed=self.plan()
            if confirmed['roles']!=ticket['roles'] or any(item not in confirmed['eligible'] for item in ticket['eligible']):
                raise core.RevisionConflict('Retained inputs or reader roles changed; eligibility must be recomputed')
            roots={head['generation']:self.repo._validate_head_file(head) for head in
                   (confirmed['roles']['head'],confirmed['roles']['rollback']) if head and head.get('generation') and not head.get('disabled')}
            for item in ticket['eligible']:
                root=roots[item['rootGeneration']]
                edges=self.find_path(roots,item['generation'])
                self.verify_path(root,edges,stream=True)
            return self._retire_confirmed(ticket)
        finally:
            for lease in input_leases:lease.close()

    def _retire_confirmed(self,ticket):
        if not self.repo.writer_lock.acquire(blocking=False):raise core.Deferred('Writer gate busy for exact retirement recheck')
        conn=None
        try:
            conn=self.repo.main_write_factory();guard=core.Guard(self.repo.publish_budget,self.repo.growth_probe)
            conn.set_progress_handler(guard.progress,1000);conn.execute('BEGIN IMMEDIATE')
            roles=self.saved_roles(conn)
            if roles!=ticket['roles']:raise core.RevisionConflict('Lifecycle roles changed after eligibility; do not retire')
            protected=self.protected(roles)
            for item in ticket['eligible']:
                generation=item['generation']
                if generation in protected:raise core.RevisionConflict('Generation acquired a protected role or reader')
                path=saved.safe_file(self.directory,generation)
                if list(saved.identity(path))!=item['fileIdentity']:raise core.Deferred('Eligible file identity changed')
                self.repo.release_unused(protected)
                lease=saved.ReadLease(path,exclusive=True)
                receipt=dict(version=1,generation=generation,sourceInputs=item,
                    originalSeal=json.loads(path.with_suffix('.seal.json').read_text(encoding='utf-8')),
                    exactScope='Only this non-current/non-rollback/unpinned derived sqlite file',historyDeleted=False)
                self._atomic_json(self.receipts/(generation+'.json'),receipt)
                lease.close()
                # Protected roles cannot change while the main writer is held.
                # A racing external deny-delete reader makes unlink fail safely.
                with self.repo.lock:
                    if self.repo.reader_pins.get(generation):raise core.RevisionConflict('Reader pin acquired during cleanup')
                    try:path.chmod(stat.S_IREAD|stat.S_IWRITE);path.unlink()
                    except BaseException:
                        if path.exists():path.chmod(stat.S_IREAD)
                        raise
                guard.check()
            conn.rollback();return dict(retired=[x['generation'] for x in ticket['eligible']],historyDeleted=False)
        finally:
            try:
                if conn is not None:
                    conn.set_progress_handler(None,0)
                    if conn.in_transaction:conn.rollback()
                    conn.close()
            finally:self.repo.writer_lock.release()

    def recover(self,target_generation,output_directory=None):
        """Rebuild a retired saved snapshot into a new private generation ID."""
        self.repo.admission('recover_inputs')
        export=output_directory is not None
        self.inventory(reserve=self.repo.stage_budget.file_bytes,new_generation=not export)
        output_repo=self.repo
        if export:
            destination=Path(output_directory).resolve()
            if destination.drive.upper()!='C:' or destination==self.directory or not destination.is_relative_to(self.directory.parent):
                raise core.Deferred('Recovery export must be a new private sibling below the approved C root')
            if any(parent.exists() and (parent.is_symlink() or getattr(parent,'is_junction',lambda:False)())
                   for parent in (destination,*destination.parents)):
                raise core.Deferred('Recovery export may not traverse links')
            destination.mkdir(exist_ok=False)
            output_repo=saved.Repository(destination,self.repo.main_read_factory,self.repo.main_write_factory,
                self.repo.writer_lock,self.repo.external,self.repo.admission,self.repo.growth_probe,
                self.repo.stage_budget,self.repo.publish_budget)
        with closing(self.repo.main_read_factory()) as c:roles=self.saved_roles(c)
        roots={head['generation']:self.repo._validate_head_file(head) for head in
               (roles['head'],roles['rollback']) if head and head.get('generation') and not head.get('disabled')}
        edges=self.find_path(roots,target_generation)
        if not edges:raise core.Deferred('No complete retained input path from a protected generation')
        root=roots[edges[0]['header']['sourceGeneration']]
        header=edges[-1]['header'];target_metadata=header['targetSeal']
        import normalized_staging
        with closing(saved.readonly(root.path,immutable=True)) as c:
            metric_sql=c.execute("SELECT sql FROM sqlite_schema WHERE type='table' AND name='item_market_metrics'").fetchone()[0]
        schema=normalized_staging.schema(metric_sql)
        generation=uuid.uuid4().hex
        engine=normalized_staging.NormalizedStaging(output_repo.directory,schema,None,None,None,None,
            self.repo.admission,None,self.repo.stage_budget,self.repo.stage_budget,self.repo.publish_budget,6,16)
        guard=core.Guard(self.repo.stage_budget)
        c=engine.open_stage(generation,create=True)
        records=None
        try:
            c.set_progress_handler(guard.progress,1000)
            old_settings=dict(header['targetSettings'])
            dictionary=old_settings[saved.DICTIONARY_KEY]
            c.builder=saved.codec.DictionaryBuilder(dictionary)
            c.create_function('saved_encode_groups',1,c.builder.encode)
            c.execute('BEGIN IMMEDIATE')
            c.executemany('INSERT OR REPLACE INTO settings VALUES(?,?)',header['targetSettings'])
            c.commit()
            normalizer=layout.NormalizedLookup(c);records=self.restored_records(root,edges)
            c.execute('BEGIN IMMEDIATE');pending=0
            metric_buffer=[];metric_buffer_bytes=0
            def flush_metrics():
                nonlocal pending,metric_buffer_bytes
                # Key-ordered reverse facts need not be rowid-ordered. Buffer
                # only compact metric bytes, under the existing128MiB read
                # allowance, then append physical metric rows in rowid order.
                # This avoids B-tree fragmentation inflating a rebuilt cache.
                metric_buffer.sort(key=lambda pair:pair[0])
                insert='INSERT INTO item_market_metrics(rowid,'+','.join(core.METRIC_COLUMNS)+') VALUES('+','.join('?' for _ in range(1+len(core.METRIC_COLUMNS)))+')'
                for rowid,raw_bytes in metric_buffer:
                    c.execute(insert,(rowid,*json.loads(raw_bytes)))
                    pending+=1
                    if pending>=500:
                        c.commit();guard.check();self.inventory();c.execute('BEGIN IMMEDIATE');pending=0
                metric_buffer.clear();metric_buffer_bytes=0
            for key,rowid,raw in records:
                if key[0]==0:
                    packed=encoded(raw);pair=(rowid,packed)
                    metric_buffer_bytes+=sys.getsizeof(pair)+sys.getsizeof(rowid)+sys.getsizeof(packed)+8
                    if metric_buffer_bytes>min(128*1024**2,self.repo.stage_budget.result_bytes):
                        raise core.Deferred('Bounded metric rebuild ordering buffer reached128MiB')
                    metric_buffer.append(pair)
                else:
                    if metric_buffer:flush_metrics()
                    row=list(raw);row[17]=c.builder.encode(row[17]);normalizer.insert_many([(rowid,row)])
                    pending+=1
                guard.rows+=1
                if pending>=500:
                    c.commit();guard.check();self.inventory();c.execute('BEGIN IMMEDIATE');pending=0
                if guard.rows%256==0:guard.check();self.repo.admission('stage_progress')
            if metric_buffer:flush_metrics()
            state=json.loads(old_settings[core.STATE_KEY])
            state.update(generation=generation,schemaSHA=hashlib.sha256(schema.encode()).hexdigest(),
                         rebuiltFromGeneration=target_generation)
            core.put_json(c,core.STATE_KEY,state)
            if c.builder.serialized()!=dictionary:raise core.Deferred('Rebuild would change retained dictionary identities')
            c.commit();guard.check();c.set_progress_handler(None,0);c.close();c=None
            seal=output_repo.seal(generation,target_metadata['rowHashes'])
            from types import SimpleNamespace
            descriptor=SimpleNamespace(generation=target_generation,metadata=target_metadata)
            # A recovered generation becomes a valid future graph root without
            # pretending its new SQLite bytes equal the old physical file.
            if not export:self.capture(seal,descriptor,header['targetSettings'],self.restored_records(root,edges))
            return dict(seal=seal,fromGeneration=target_generation,logicalRowsExact=True,
                physicalSQLiteBytesIdentical=False,sourceInputs=edges[-1]['targetContentSHA256'])
        finally:
            if records is not None:records.close()
            if c is not None:
                c.set_progress_handler(None,0)
                if c.in_transaction:c.rollback()
                c.close()
            if export:output_repo.close()
