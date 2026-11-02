"""Unactivated candidate: bounded C: derived sidecar and atomic canonical publish.

No I/O happens at import. Host adapters must supply guarded SQLite factories,
resource admission, byte-growth probes, exact legacy callbacks and writer ABI.
History and source payloads are never persisted in the sidecar.
"""
from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
import types
import uuid

SOURCE_SHA = '3e04662d654182bcf74bf3ec6b098c5b5f00db84f03d26b811f614d4995d32cc'
WRITER_ABI = SOURCE_SHA + ':all-derived-inputs-v3-native-lookup-fence'
ABI_KEY = 'bulk_derived_writer_abi:v1'
EPOCH_KEY = 'bulk_derived_input_epoch:v1'
METRIC_EPOCH_KEY = 'bulk_derived_metric_input_epoch:v1'
PENDING_KEY = 'bulk_derived_pending:v1'
PUBLISHED_KEY = 'bulk_derived_published:v1'
STATE_KEY = 'bulk_stage_state:v1'
WINDOWS = (7, 30, 90)
FENCE_KEYS = ('market_source_revision', 'catalog_metadata_revision',
              'cash_source_revision:skinport_live', EPOCH_KEY, METRIC_EPOCH_KEY, ABI_KEY)
RECEIPT_KEYS = tuple('market_metric_receipt:' + str(w) for w in WINDOWS) + tuple(
    'market_lookup_receipt:' + str(w) for w in WINDOWS) + ('metric_build_version', 'lookup_build_version')
METRIC_COLUMNS = ('name provider window_days points clean_points edge_case_count volatility_pct '
                  'range_pct trend_pct first_price latest_price first_ts last_ts current_price '
                  'prior_price price_source updated_at raw').split()
LOOKUP_COLUMNS = ('window_days name market_hash_name kind item_type weapon collection rarity latest_price '
                  'prior_price change_pct change_dollar latest_at source volatility_pct history_points '
                  'edge_case_count groups_json search_text metric_updated_at steam_price '
                  'steam_lowest_sell_order steam_highest_buy_order steam_spread_pct steam_stability_pct '
                  'price_basis confidence_pct provider_prices_json').split()
SOURCE_TABLES = re.compile(r'\b(?:FROM|JOIN)\s+(?:main\.)?(price_history|price_observations|market_item_prices|market_items|item_catalog)\b', re.I)
STAGED_TABLES = re.compile(r'\b(FROM|JOIN)\s+(item_market_metrics|market_item_lookup)\b', re.I)
WRITE_TARGET = re.compile(r'^\s*(?:INSERT(?:\s+OR\s+\w+)?\s+INTO|REPLACE\s+INTO|DELETE\s+FROM|UPDATE)\s+(\w+)\b', re.I)


class Deferred(RuntimeError):
    """No complete new canonical projection was published."""


class RevisionConflict(Deferred):
    pass


class PublishUncertain(RuntimeError):
    """Caller must inspect publish_token before any retry; no rollback claim."""
    def __init__(self, token, cause):
        super().__init__('Publication outcome uncertain; inspect exact token ' + token)
        self.publish_token, self.cause = token, repr(cause)


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(compact(value).encode('utf-8')).hexdigest()


def get_setting(conn, key):
    row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
    return row[0] if row else None


def get_json(conn, key):
    raw = get_setting(conn, key)
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise Deferred('Invalid structured setting: ' + key)
    return value


def put_json(conn, key, value):
    conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                 (key, compact(value)))


def invalidate_inputs(conn, lane, now=None):
    """Call ONCE in each source mutation's existing transaction, never per row.

    No source/history values are changed. The host must qualify all writer hooks
    before installing ABI_KEY. Native metadata writers must participate too.
    """
    if not conn.in_transaction:
        raise Deferred('Input invalidation requires the source transaction')
    if get_setting(conn, ABI_KEY) != WRITER_ABI:
        raise Deferred('Derived input writer ABI is not qualified')
    token = uuid.uuid4().hex
    conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                 (EPOCH_KEY, token))
    cash_only = lane in ('skinport_live','native_registry_only')
    if not cash_only:
        conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                     (METRIC_EPOCH_KEY, token))
    elif not get_setting(conn, METRIC_EPOCH_KEY):
        raise Deferred('Lookup-only invalidation requires an initialized metric input epoch')
    old_pending = get_json(conn, PENDING_KEY)
    generation = old_pending['generation'] if cash_only and old_pending and old_pending.get('state') != 'published' else token
    pending = {'version': 1, 'generation': generation, 'state': 'source_committed_derived_pending',
               'lane': str(lane), 'requestedAt': int(time.time() if now is None else now)}
    put_json(conn, PENDING_KEY, pending)
    return pending


@dataclass(frozen=True)
class Budget:
    seconds: float
    vm_steps: int
    rows: int
    result_bytes: int
    file_bytes: int
    growth_bytes: int
    python_lines: int
    expanded_sql_bytes: int

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 for value in (
                self.vm_steps, self.rows, self.result_bytes, self.file_bytes,
                self.growth_bytes, self.python_lines, self.expanded_sql_bytes)) or self.seconds <= 0:
            raise ValueError('Every budget must be explicit and positive')


class Guard:
    def __init__(self, budget, growth_probe=None):
        self.budget, self.probe = budget, growth_probe
        self.deadline = time.monotonic() + budget.seconds
        self.steps = self.rows = self.bytes = self.lines = 0
        self.initial = dict(growth_probe()) if growth_probe else {}
        self.failure = None

    def check(self):
        if time.monotonic() >= self.deadline:
            raise Deferred('Operation deadline reached')
        if self.steps > self.budget.vm_steps or self.lines > self.budget.python_lines:
            raise Deferred('Operation work budget reached')
        if self.rows > self.budget.rows or self.bytes > self.budget.result_bytes:
            raise Deferred('Operation row/byte budget reached')
        if self.probe:
            current = dict(self.probe())
            if any(type(v) is not int or v < 0 for v in current.values()):
                raise Deferred('Invalid byte-growth probe')
            # Sum positive changes: a shrinking file never cancels WAL growth.
            growth = sum(max(0, value - self.initial.get(name, 0)) for name, value in current.items())
            if growth > self.budget.growth_bytes:
                raise Deferred('Operation physical byte-growth budget reached')

    def progress(self):
        self.steps += 1000
        try:
            self.check()
            return 0
        except BaseException as exc:
            self.failure = exc
            return 1

    def add(self, row):
        self.rows += 1
        self.bytes += len(compact(list(row)).encode('utf-8'))
        self.check()

    @contextmanager
    def python_deadline(self):
        if sys.gettrace() is not None:
            raise Deferred('An existing tracer prevents the stage deadline guard')
        def trace(frame, event, arg):
            if event == 'line':
                self.lines += 1
                if self.lines % 128 == 0:
                    self.check()
            return trace
        sys.settrace(trace)
        try:
            yield
        finally:
            sys.settrace(None)
            self.check()


class Rows:
    def __init__(self, rows):
        self.rows, self.index = rows, 0
    def __iter__(self):
        while self.index < len(self.rows):
            row = self.rows[self.index]
            self.index += 1
            yield row
    def fetchone(self):
        return next(iter(self), None)
    def fetchall(self):
        return list(self)


def read_fence(conn, external):
    values = {key: get_setting(conn, key) for key in FENCE_KEYS}
    if values[ABI_KEY] != WRITER_ABI or not values[EPOCH_KEY] or not values[METRIC_EPOCH_KEY] or not values['market_source_revision']:
        raise Deferred('Required source revision and qualified all-input writer epoch are absent')
    table = conn.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='catalog_store_meta'").fetchone()
    native = conn.execute("SELECT value FROM catalog_store_meta WHERE key='generation'").fetchone() if table else None
    values['catalogStoreGeneration'] = native[0] if native else None
    values['external'] = dict(external())
    return values


def metric_fence(fence):
    # Every legacy numerical/metadata producer must bump METRIC_EPOCH_KEY.
    # Native-only registry/binding changes cannot alter legacy metric inputs.
    # Keep native generation in read_fence and every lookup/publication CAS.
    return {key:value for key,value in fence.items() if key not in (EPOCH_KEY,'cash_source_revision:skinport_live','catalogStoreGeneration')}


def assert_pending(conn, generation):
    pending = get_json(conn, PENDING_KEY)
    if not pending or pending.get('generation') != generation or pending.get('state') == 'published':
        raise RevisionConflict('Pending generation was superseded')


class Facade:
    """Legacy writes go to C: staging; short fenced source reads remain read-only."""
    def __init__(self, host, stage, state, guard):
        self.host, self.stage, self.state, self.guard = host, stage, state, guard
        self._market_dirty = False
        self._bulk_sidecar = True
    @property
    def in_transaction(self):
        return self.stage.in_transaction
    def execute(self, sql, params=()):
        self.guard.check()
        target = WRITE_TARGET.match(sql)
        if target:
            if target.group(1).lower() not in ('item_market_metrics', 'market_item_lookup', 'settings'):
                raise Deferred('Legacy stage write target is not allowed')
            if target.group(1).lower() == 'settings' and 'market_source_revision' in sql:
                raise Deferred('Stage may not fabricate a source revision')
            return self.stage.execute(sql, params)
        if not sql.lstrip().upper().startswith('SELECT'):
            raise Deferred('Unexpected legacy statement')
        if SOURCE_TABLES.search(sql):
            return Rows(self.host.source_rows(sql, params, self.state, self.guard))
        return self.stage.execute(sql, params)
    def executemany(self, sql, rows):
        # Check Python-generated projections before allowing the sidecar write.
        def checked():
            for row in rows:
                self.guard.add(row)
                yield row
        target = WRITE_TARGET.match(sql)
        if not target or target.group(1).lower() not in ('item_market_metrics', 'market_item_lookup', 'settings'):
            raise Deferred('Unexpected legacy bulk write target')
        return self.stage.executemany(sql, checked())


def bind_legacy(metric_function, lookup_function, unpriced_function, group_function, alias_resolver):
    """Reuse code objects; isolate only the lookup's implicit global alias read.

    Host source pin and synthetic parity qualification are required. No source
    function is executed while binding. Unpriced's explicit resolver stays intact.
    """
    if (metric_function.__name__ != 'rebuild_market_metric_windows'
            or lookup_function.__name__ != 'rebuild_market_item_lookup'
            or unpriced_function.__name__ != 'build_unpriced_market_lookup_rows'):
        raise ValueError('Exact legacy functions required')
    namespace = dict(lookup_function.__globals__)
    def fenced_groups(item, collection_alias_resolver=None):
        return group_function(item, collection_alias_resolver=(
            alias_resolver if collection_alias_resolver is None else collection_alias_resolver))
    namespace['market_group_tags'] = fenced_groups
    for fn in (lookup_function, unpriced_function):
        namespace[fn.__name__] = types.FunctionType(fn.__code__, namespace, fn.__name__, fn.__defaults__, fn.__closure__)
    return metric_function, namespace['rebuild_market_item_lookup']


class Staging:
    def __init__(self, directory, schema_text, source_reader, publisher_factory, writer_lock,
                 external_fence, admission, live_growth_probe, stage_budget, read_budget,
                 publish_budget, metric_build_version, lookup_build_version, source_sha=SOURCE_SHA):
        self.directory = Path(directory).resolve()
        if self.directory.drive.upper() != 'C:' or source_sha != SOURCE_SHA:
            raise Deferred('Exact source and an approved private C: directory are required')
        if not self.directory.is_dir():
            raise Deferred('Host must prepare and qualify the private sidecar directory')
        self.schema = schema_text
        self.source_reader, self.publisher_factory = source_reader, publisher_factory
        self.writer_lock, self.external = writer_lock, external_fence
        self.admission, self.live_growth = admission, live_growth_probe
        self.stage_budget, self.read_budget, self.publish_budget = stage_budget, read_budget, publish_budget
        self.metric_build, self.lookup_build = int(metric_build_version), int(lookup_build_version)
        self._unit_sources = threading.local()
        # Host must replace these conservative limits explicitly for qualification.
        self.directory_budget_bytes = 1024 * 1024 * 1024
        self.max_generations = 3
        if self.metric_build <= 0 or self.lookup_build <= 0:
            raise ValueError('Pinned positive metric/lookup build versions required')

    def path(self, generation):
        if not isinstance(generation, str) or not re.fullmatch('[0-9a-f]{32}', generation):
            raise Deferred('Invalid generation identifier')
        path = self.directory / ('bulk-derived-' + generation + '.sqlite')
        if path.parent != self.directory:
            raise Deferred('Sidecar path escaped its private directory')
        return path

    def directory_admission(self, new_generation=False, unit_reserve=False):
        total, generations, entries = 0, 0, 0
        for entry in self.directory.iterdir():
            entries += 1
            if entries > 4 * self.max_generations + 16 or entry.is_symlink() or not entry.is_file():
                raise Deferred('Private sidecar directory inventory is not bounded/regular')
            total += entry.stat().st_size
            generations += bool(re.fullmatch(r'bulk-derived-[0-9a-f]{32}\.sqlite',entry.name))
        reserve = self.stage_budget.file_bytes if new_generation or unit_reserve else 0
        if total + reserve > self.directory_budget_bytes or generations + int(new_generation) > self.max_generations:
            raise Deferred('Sidecar directory total-byte/generation admission exhausted; no automatic deletion')
        return {'directoryBytes':total,'generations':generations,'reservedNewBytes':reserve}

    def stage_probe(self, path):
        size = self.stage_size(path)
        if size > self.stage_budget.file_bytes:
            raise Deferred('Sidecar plus journal capacity cap reached')
        return {'sidecar':size}

    def stage_size(self, path):
        return sum(candidate.stat().st_size for candidate in (
            path, Path(str(path) + '-wal'), Path(str(path) + '-journal')) if candidate.exists())

    @contextmanager
    def source(self, guard, state=None, attach=False):
        with closing(self.source_reader()) as conn:
            if conn.in_transaction or conn.execute('PRAGMA query_only').fetchone()[0] != 1:
                raise Deferred('Guarded query-only source connection required')
            conn.set_progress_handler(guard.progress, 1000)
            try:
                if attach:
                    uri = self.path(state['generation']).as_uri() + '?mode=ro'
                    conn.execute('ATTACH DATABASE ? AS bulk_stage', (uri,))
                conn.execute('BEGIN')
                fence = read_fence(conn, self.external)
                if state:
                    assert_pending(conn, state['generation'])
                    if fence != state['fence']:
                        raise RevisionConflict('Derived input fence changed')
                yield conn, fence
                guard.check()
            finally:
                conn.set_progress_handler(None, 0)
                if conn.in_transaction:
                    conn.rollback()
        # The read snapshot is closed before fresh external state is compared.
        if state and dict(self.external()) != state['fence']['external']:
            raise RevisionConflict('External alias/code input changed')

    @contextmanager
    def unit_source(self, read_guard, state, unit_guard):
        if getattr(self._unit_sources, 'lease', None) is not None:
            raise Deferred('A source unit already owns this thread')
        # Lookup keeps the original independent readers, closing each attached
        # stage snapshot before private writes can require a cache spill.
        if state['phase'] != 'metrics':
            yield
            return
        with self.source(read_guard, state) as (conn, _):
            self._unit_sources.lease = {
                'thread': threading.get_ident(), 'state': state,
                'unit_guard': unit_guard, 'read_guard': read_guard,
                'connection': conn, 'attached': False}
            try:
                yield
            finally:
                # Clear borrowing before source() rolls back/closes, including
                # callback, budget, fence and closure failures.
                self._unit_sources.lease = None

    def source_rows(self, sql, params, state, unit_guard):
        lease = getattr(self._unit_sources, 'lease', None)
        if lease is not None:
            if (lease['thread'] != threading.get_ident() or lease['state'] is not state
                    or lease['unit_guard'] is not unit_guard):
                raise Deferred('Unit source ownership mismatch')
            guard = lease['read_guard']
            guard.check()
            mixed = bool(STAGED_TABLES.search(sql))
            if mixed:
                if not lease['attached']:
                    raise Deferred('Mixed unit source query requires the preattached stage')
                sql = STAGED_TABLES.sub(lambda m: m.group(1) + ' bulk_stage.' + m.group(2), sql)
            rows = []
            for row in lease['connection'].execute(sql, params):
                guard.add(row)
                unit_guard.add(row)
                rows.append(row)
            guard.check()
            return rows
        # Outside a unit, preserve the original independent fenced snapshots.
        guard = Guard(self.read_budget)
        rows = []
        mixed = bool(STAGED_TABLES.search(sql))
        if mixed:
            sql = STAGED_TABLES.sub(lambda m: m.group(1) + ' bulk_stage.' + m.group(2), sql)
        with self.source(guard, state, attach=mixed) as (conn, _):
            for row in conn.execute(sql, params):
                guard.add(row)
                unit_guard.add(row)
                rows.append(row)
        return rows

    def open_stage(self, generation, create=False):
        path = self.path(generation)
        uri = path.as_uri() + ('?mode=rwc' if create else '?mode=rw')
        conn = sqlite3.connect(uri, uri=True, timeout=0.0)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=DELETE')
        page_size = conn.execute('PRAGMA page_size').fetchone()[0]
        conn.execute('PRAGMA max_page_count=' + str(self.stage_budget.file_bytes // page_size))
        if create:
            conn.executescript(self.schema)  # C: disposable sidecar only.
        if self.stage_size(path) > self.stage_budget.file_bytes:
            conn.close()
            raise Deferred('Sidecar capacity cap reached')
        return conn

    def create_or_resume(self):
        self.admission('stage')
        self.directory_admission()
        guard = Guard(self.read_budget)
        with self.source(guard) as (source, fence):
            pending = get_json(source, PENDING_KEY)
            if not pending or pending.get('state') == 'published':
                raise Deferred('No explicitly pending derived generation')
            generation = pending['generation']
            published = get_json(source, PUBLISHED_KEY)
        path = self.path(generation)
        if path.exists():
            with closing(self.open_stage(generation, create=True)) as stage:
                state = get_json(stage, STATE_KEY)
            if state and (state.get('sourceSHA') != SOURCE_SHA or state.get('schemaSHA') != hashlib.sha256(self.schema.encode()).hexdigest()):
                raise RevisionConflict('Saved stage source/schema is incompatible')
            if state and state.get('fence') != fence:
                if metric_fence(state['fence']) != metric_fence(fence):
                    raise RevisionConflict('Saved stage metric inputs changed')
                with closing(self.open_stage(generation)) as stage, stage:
                    stage.execute('BEGIN IMMEDIATE')
                    retarget_guard = Guard(self.stage_budget,lambda:self.stage_probe(path))
                    stage.set_progress_handler(retarget_guard.progress,1000)
                    current = get_json(stage, STATE_KEY)
                    if current != state:
                        raise Deferred('Concurrent sidecar owner advanced state; retry explicitly')
                    state['fence'] = fence
                    state['lookupDone'] = []
                    if state['phase'] != 'metrics':
                        state.update(phase='lookup',complete=False)
                    stage.execute('DELETE FROM market_item_lookup')
                    for w in WINDOWS:
                        stage.execute('DELETE FROM settings WHERE key=?',('market_lookup_receipt:'+str(w),))
                    for key in FENCE_KEYS:
                        if fence.get(key) is not None:
                            stage.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)',(key,fence[key]))
                    put_json(stage,STATE_KEY,state)
                    retarget_guard.check()
                    stage.set_progress_handler(None,0)
            if state:
                return state
        state = {'version': 1, 'generation': generation, 'sourceSHA': SOURCE_SHA,
                 'schemaSHA': hashlib.sha256(self.schema.encode()).hexdigest(),
                 'fence': fence, 'asOf': int(time.time()), 'phase': 'metrics', 'cursor': '',
                 'counts': {str(w): 0 for w in WINDOWS}, 'lookupDone': [], 'complete': False}
        self.directory_admission(new_generation=not path.exists())
        with closing(self.open_stage(generation, create=True)) as stage, stage:
            stage.execute('BEGIN IMMEDIATE')
            prior = get_json(stage, STATE_KEY)
            if prior:
                if prior.get('fence') != fence:
                    raise RevisionConflict('Concurrent sidecar initialization changed inputs')
                return prior
            for table in ('item_market_metrics', 'market_item_lookup'):
                if stage.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone():
                    raise Deferred('Uninitialized stage contains unexplained derived rows')
            for key, value in fence.items():
                if key in FENCE_KEYS and value is not None:
                    stage.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', (key, value))
            for key, value in (('metric_build_version', self.metric_build), ('lookup_build_version', self.lookup_build)):
                stage.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', (key, str(value)))
            for w in WINDOWS:
                put_json(stage, 'market_metric_receipt:' + str(w), {
                    'buildVersion': self.metric_build, 'builtAt': int(time.time()), 'complete': False,
                    'rows': 0, 'sourceRevision': fence['market_source_revision']})
            # Cash-only generations can reuse complete derived metrics from the
            # retained C: published sidecar, never a raw/history copy or newer clock.
            if published and metric_fence(published['fence']) == metric_fence(fence):
                prior_path = self.path(published['generation'])
                if prior_path.exists() and prior_path != path:
                    stage.execute('ATTACH DATABASE ? AS previous_stage',(prior_path.as_uri()+'?mode=ro',))
                    prior_raw = stage.execute('SELECT value FROM previous_stage.settings WHERE key=?',(STATE_KEY,)).fetchone()
                    previous = json.loads(prior_raw[0]) if prior_raw else None
                    if previous and previous.get('complete') and metric_fence(previous['fence']) == metric_fence(fence):
                        reuse_guard = Guard(self.stage_budget)
                        stage.set_progress_handler(reuse_guard.progress,1000)
                        try:
                            stage.execute('INSERT INTO item_market_metrics SELECT * FROM previous_stage.item_market_metrics')
                            for w in WINDOWS:
                                receipt_raw = stage.execute('SELECT value FROM previous_stage.settings WHERE key=?',('market_metric_receipt:'+str(w),)).fetchone()
                                if not receipt_raw:
                                    raise Deferred('Retained metric receipt absent')
                                receipt = json.loads(receipt_raw[0])
                                if not receipt.get('complete'):
                                    raise Deferred('Retained metric receipt incomplete')
                                put_json(stage,'market_metric_receipt:'+str(w),receipt)
                            state.update(phase='lookup',asOf=previous['asOf'],counts=previous['counts'],
                                         metricsReusedFrom=published['generation'])
                            reuse_guard.check()
                        finally:
                            stage.set_progress_handler(None,0)
            put_json(stage, STATE_KEY, state)
        return state

    def run_unit(self, generation, metric_function, lookup_function, providers):
        self.admission('stage')
        self.directory_admission(unit_reserve=True)
        path = self.path(generation)
        guard = Guard(self.stage_budget, lambda:self.stage_probe(path))
        with closing(self.open_stage(generation)) as stage:
            stage.execute('BEGIN IMMEDIATE')  # Only private C: writer ownership.
            stage.set_progress_handler(guard.progress, 1000)
            try:
                state = get_json(stage, STATE_KEY)
                if not state or state['generation'] != generation or state.get('complete'):
                    raise Deferred('No unfinished sidecar unit')
                facade = Facade(self, stage, state, guard)
                read_guard = Guard(self.read_budget)
                with self.unit_source(read_guard, state, guard), guard.python_deadline():
                    if state['phase'] == 'metrics':
                        if providers:
                            seek = ('SELECT name FROM (SELECT name FROM market_item_prices '
                                'WHERE provider=? AND name>? AND price IS NOT NULL '
                                'ORDER BY name LIMIT 16)')
                            names = self.source_rows('SELECT DISTINCT name FROM (' +
                                ' UNION ALL '.join(seek for _ in providers) + ') ORDER BY name LIMIT 16',
                                [value for provider in providers for value in (provider, state['cursor'])],
                                state, guard)
                        else:
                            names = []
                        if names:
                            batch = [row['name'] for row in names]
                            if any(not isinstance(name, str) or not name.strip() or name != name.strip()
                                   for name in batch):
                                raise Deferred('Noncanonical metric name needs explicit qualification')
                            counts = metric_function(facade, WINDOWS, names=batch, as_of=state['asOf'])
                            state['cursor'] = batch[-1]
                            for w in WINDOWS:
                                state['counts'][str(w)] += counts[w]
                        else:
                            for w in WINDOWS:
                                receipt = get_json(stage, 'market_metric_receipt:' + str(w))
                                if receipt is None:
                                    raise Deferred('Required metric receipt absent')
                                receipt.update(complete=True, rows=state['counts'][str(w)])
                                put_json(stage, 'market_metric_receipt:' + str(w), receipt)
                            state['phase'] = 'lookup'
                    elif state['phase'] == 'lookup':
                        window = next(w for w in WINDOWS if w not in state['lookupDone'])
                        lookup_function(facade, window)
                        state['lookupDone'].append(window)
                        if tuple(state['lookupDone']) == WINDOWS:
                            state.update(phase='ready', complete=True)
                    else:
                        raise Deferred('Unexpected sidecar phase')
                # The leased snapshot (and optional C-stage reader) is closed
                # before this fresh fence and before the private commit. Keep
                # the same aggregate read budget rather than resetting it.
                with self.source(read_guard, state):
                    pass
                put_json(stage, STATE_KEY, state)
                guard.check()
                stage.commit()  # Rows + cursor advance together; crash repeats one unit.
                self.directory_admission()
                return state
            except BaseException:
                stage.set_progress_handler(None, 0)
                stage.rollback()
                raise
            finally:
                stage.set_progress_handler(None, 0)

    def preflight(self, stage, state, strict=True):
        if not state.get('complete') or tuple(state.get('lookupDone', ())) != WINDOWS:
            raise Deferred('All metric membership and all three lookups are required')
        rows = 0
        payload_bytes = 0
        expanded_bytes = 0
        table_counts = {}
        for table, columns in (('item_market_metrics', METRIC_COLUMNS), ('market_item_lookup', LOOKUP_COLUMNS)):
            expression = '+'.join('COALESCE(length(CAST(' + column + ' AS BLOB)),0)' for column in columns)
            row = stage.execute('SELECT COUNT(*),COALESCE(SUM(' + expression + '),0) FROM ' + table).fetchone()
            rows += row[0]
            payload_bytes += row[1]
            table_counts[table] = row[0]
            quoted = '+'.join('length(CAST(quote(' + column + ') AS BLOB))' for column in columns)
            overhead = len(('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES();').encode()) + len(columns)
            expanded_bytes += stage.execute('SELECT COALESCE(SUM(' + quoted + '+?),0) FROM ' + table,
                                             (overhead,)).fetchone()[0]
        if stage.execute("SELECT 1 FROM item_market_metrics WHERE provider!='market' OR window_days NOT IN(7,30,90) LIMIT 1").fetchone():
            raise Deferred('Unexpected staged metric provider/window')
        if stage.execute('SELECT 1 FROM market_item_lookup WHERE window_days NOT IN(7,30,90) LIMIT 1').fetchone():
            raise Deferred('Unexpected staged lookup window')
        receipts = {}
        for w in WINDOWS:
            metric = get_json(stage, 'market_metric_receipt:' + str(w))
            lookup = get_json(stage, 'market_lookup_receipt:' + str(w))
            mc = stage.execute("SELECT COUNT(*) FROM item_market_metrics WHERE provider='market' AND window_days=?", (w,)).fetchone()[0]
            lc = stage.execute('SELECT COUNT(*) FROM market_item_lookup WHERE window_days=?', (w,)).fetchone()[0]
            if (not metric or not metric.get('complete') or metric.get('rows') != mc
                    or not lookup or not lookup.get('metricsVerified') or lookup.get('rows') != lc
                    or metric.get('sourceRevision') != state['fence']['market_source_revision']
                    or lookup.get('sourceRevision') != state['fence']['market_source_revision']):
                raise Deferred('Incomplete/mismatched window receipts')
            receipts[str(w)] = {'metric': metric, 'lookup': lookup}
        setting_bytes = sum(len((get_setting(stage, key) or '').encode('utf-8')) for key in RECEIPT_KEYS)
        expanded_bytes += 2 * setting_bytes + 4096  # Existing receipts plus head/pending envelope.
        file_bytes = self.stage_size(self.path(state['generation']))
        failures = []
        for over, label in (
            (rows > self.publish_budget.rows, 'insert_rows'),
            (payload_bytes + setting_bytes > self.publish_budget.result_bytes, 'payload_bytes'),
            (expanded_bytes > self.publish_budget.expanded_sql_bytes, 'expanded_sql_bytes'),
            (file_bytes > min(self.stage_budget.file_bytes, self.publish_budget.file_bytes), 'sidecar_bytes')):
            if over:
                failures.append(label)
        proof = {'insertRows': rows, 'tableRows': table_counts, 'payloadBytes': payload_bytes,
                 'settingBytes': setting_bytes, 'expandedSQLBytes': expanded_bytes,
                 'stageFileBytes': file_bytes, 'preflightFits': not failures,
                 'admissionFailures': failures, 'receipts': receipts}
        if strict and failures:
            raise Deferred('Full projection publication admission failed: ' + ','.join(failures))
        return proof

    def estimate(self, generation):
        """Bounded derived-only preflight; never acquires the main writer lock."""
        self.admission('estimate')
        with closing(self.open_stage(generation)) as stage:
            guard = Guard(self.stage_budget)
            stage.set_progress_handler(guard.progress, 1000)
            try:
                state = get_json(stage, STATE_KEY)
                proof = self.preflight(stage, state, strict=False)
                guard.check()
                read_guard = Guard(self.read_budget)
                with self.source(read_guard, state) as (source, _):
                    old_metrics = source.execute("SELECT COUNT(*) FROM item_market_metrics WHERE provider='market'").fetchone()[0]
                    old_lookup = source.execute('SELECT COUNT(*) FROM market_item_lookup').fetchone()[0]
                    read_guard.check()
                proof['oldCanonicalRows'] = old_metrics + old_lookup
                proof['totalDeleteInsertOperations'] = old_metrics + old_lookup + proof['insertRows']
                if proof['totalDeleteInsertOperations'] > self.publish_budget.rows:
                    proof['admissionFailures'].append('delete_plus_insert_rows')
                    proof['preflightFits'] = False
                return proof
            finally:
                stage.set_progress_handler(None, 0)

    def publish(self, generation):
        self.admission('publish')
        path = self.path(generation)
        with closing(self.open_stage(generation)) as stage:
            stage.execute('BEGIN IMMEDIATE')  # Serializes sidecar owners, never main.
            state = get_json(stage, STATE_KEY)
            preflight_guard = Guard(self.stage_budget)
            stage.set_progress_handler(preflight_guard.progress, 1000)
            try:
                proof = self.preflight(stage, state)  # Full derived scans occur before main admission.
                preflight_guard.check()
            finally:
                stage.set_progress_handler(None, 0)
            with closing(self.source_reader()) as check:
                prior = get_json(check, PUBLISHED_KEY)
            if prior and prior.get('generation') == generation:
                stage.rollback()
                return {'state': 'published', 'generation': generation,
                        'publishToken': prior['publishToken'], 'alreadyPublished': True}
            if not self.writer_lock.acquire(blocking=False):
                stage.rollback()
                raise Deferred('Main writer admission busy; no automatic retry')
            token = uuid.uuid4().hex
            conn = None
            committed = False
            try:
                guard = Guard(self.publish_budget, self.live_growth)
                conn = self.publisher_factory()
                if conn.in_transaction:
                    raise Deferred('Fresh short publisher connection required')
                conn.set_progress_handler(guard.progress, 1000)
                conn.execute('ATTACH DATABASE ? AS bulk_stage', (path.as_uri() + '?mode=ro',))
                conn.execute('BEGIN IMMEDIATE')
                assert_pending(conn, generation)
                if read_fence(conn, self.external) != state['fence']:
                    raise RevisionConflict('Fresh writer-transaction input CAS failed')
                old_metrics = conn.execute("SELECT COUNT(*) FROM item_market_metrics WHERE provider='market'").fetchone()[0]
                old_lookup = conn.execute('SELECT COUNT(*) FROM market_item_lookup').fetchone()[0]
                if old_metrics + old_lookup + proof['insertRows'] > self.publish_budget.rows:
                    raise Deferred('Complete delete-plus-insert row budget exceeded')
                # Delete costs and old-row counts also consume the admitted VM/time/growth budget.
                conn.execute("DELETE FROM item_market_metrics WHERE provider='market'")
                conn.execute('DELETE FROM market_item_lookup')
                for table, columns in (('item_market_metrics', METRIC_COLUMNS), ('market_item_lookup', LOOKUP_COLUMNS)):
                    names = ','.join(columns)
                    conn.execute('INSERT INTO ' + table + '(' + names + ') SELECT ' + names + ' FROM bulk_stage.' + table)
                    guard.check()
                for key in RECEIPT_KEYS:
                    value = get_setting(stage, key)
                    if value is None:
                        raise Deferred('A required complete projection setting is absent')
                    conn.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', (key, value))
                published = {'version': 1, 'generation': generation, 'publishToken': token,
                    'fence': state['fence'], 'asOf': state['asOf'], 'windows': list(WINDOWS),
                    'proof': proof, 'publishedAt': int(time.time()), 'complete': True}
                put_json(conn, PUBLISHED_KEY, published)
                put_json(conn, PENDING_KEY, {'version': 1, 'generation': generation, 'state': 'published',
                                           'publishToken': token})
                guard.check()
                conn.commit()  # All rows + 3 window summaries + head/pending become visible atomically.
                committed = True
                return {'state': 'published', 'generation': generation, 'publishToken': token,
                        'insertRows': proof['insertRows'], 'guardVM': guard.steps}
            except BaseException as exc:
                if conn is not None:
                    conn.set_progress_handler(None, 0)
                    try:
                        if conn.in_transaction:
                            conn.rollback()  # No false deadline guarantee for rollback/fsync.
                        else:
                            # A commit acknowledgement can fail after the durable commit.
                            with closing(self.source_reader()) as check:
                                receipt = get_json(check, PUBLISHED_KEY)
                            if receipt and receipt.get('publishToken') == token:
                                committed = True
                                return {'state': 'published', 'generation': generation,
                                        'publishToken': token, 'commitAcknowledgementRecovered': True}
                    except BaseException as verify_error:
                        raise PublishUncertain(token, verify_error) from exc
                raise
            finally:
                try:
                    if conn is not None:
                        conn.set_progress_handler(None, 0)
                        try:
                            conn.close()  # Host factory must have the verified no-close-checkpoint guard.
                        except BaseException as close_error:
                            raise PublishUncertain(token, close_error)
                finally:
                    self.writer_lock.release()
                    stage.rollback()  # Sealed data remains resumable; never delete history.


def published_state(conn, external):
    """Read in the caller's existing snapshot; ordinary GETs never schedule work."""
    head = get_json(conn, PUBLISHED_KEY)
    pending = get_json(conn, PENDING_KEY)
    current = bool(head and head.get('complete') and head.get('fence') == read_fence(conn, external))
    return {'state': 'current' if current else 'source_committed_derived_pending',
            'publishedGeneration': head.get('generation') if head else None,
            'asOf': head.get('asOf') if head else None, 'pending': pending,
            'current': current, 'savedCanonicalAvailable': True}
