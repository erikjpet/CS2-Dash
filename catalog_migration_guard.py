"""Logical migration guards and small recoverable receipts; never a DB backup."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time

from catalog_store import CatalogStore, SCHEMA, SCHEMA_VERSION

CONTROL_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS catalog_migration_control_v2(
        migration_key TEXT PRIMARY KEY,progress_json TEXT NOT NULL,
        receipt_no INTEGER NOT NULL,receipt_hash TEXT NOT NULL,schema_hash TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS catalog_migration_receipts_v2(
        migration_key TEXT NOT NULL,receipt_no INTEGER NOT NULL,
        receipt_json TEXT NOT NULL,receipt_hash TEXT NOT NULL,
        PRIMARY KEY(migration_key,receipt_no))''',
)
CONTROL = 'catalog_migration_control_v2'
RECEIPTS = 'catalog_migration_receipts_v2'
# Exact performance-only extension. It is not part of the legacy logical
# migration digest, but its presence never bypasses structural validation.
OPTIONAL_PERFORMANCE_INDEXES = {
    'catalog_metadata_source_v2': {
        'table': 'catalog_metadata_v2',
        'ddl': 'CREATE INDEX IF NOT EXISTS catalog_metadata_source_v2 ON catalog_metadata_v2(source,item_id)',
        'xinfo': ((0, 1, 'source', 0, 'BINARY', 1),
                  (1, 0, 'item_id', 0, 'BINARY', 1),
                  (2, -1, None, 0, 'BINARY', 0)),
    },
}
PROOF_TABLE = 'catalog_current_revisions_v1' 
PROOF_STORES = ('item_catalog', 'market_items', 'market_item_prices', 'price_quotes',
                'shared_item_prices', 'shared_source_quotes')
PROOF_ABI = 1


def current_proof_schema():
    """Optional source proof ABI; deliberately outside the existing registry hash."""
    statements = ['CREATE TABLE ' + PROOF_TABLE + '(store TEXT PRIMARY KEY,revision INTEGER NOT NULL CHECK(typeof(revision)=\'integer\' AND revision>=0))']
    for store in PROOF_STORES:
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            name = 'catalog_current_rev_' + store + '_' + event.lower() + '_v1'
            statements.append("CREATE TRIGGER " + name + ' AFTER ' + event + ' ON ' + store + ''' BEGIN
                SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ''' + PROOF_TABLE + " WHERE store='" + store + "' AND revision<9223372036854775807) THEN RAISE(ABORT,'current source revision unavailable or exhausted') END;\n" +
                'UPDATE ' + PROOF_TABLE + " SET revision=revision+1 WHERE store='" + store + "'; END")
    return tuple(statements)


def current_proof_manifest():
    result = {}
    for statement in current_proof_schema():
        kind, name = re.match(r'CREATE\s+(TABLE|TRIGGER)\s+(\w+)', statement).groups()
        result[name] = (kind.lower(), _normalized(statement))
    return result


def current_proof_snapshot(connection, expected_manifest=None):
    """Bounded ABI/counter snapshot in the caller's transaction; no source rows."""
    if not connection.in_transaction:
        raise MigrationFailure('current proof requires a caller transaction')
    manifest = current_proof_manifest()
    actual = {row[0]: (row[1], _normalized(row[2] or '')) for row in connection.execute(
        'SELECT name,type,sql FROM sqlite_master WHERE name IN (' + ','.join('?' for _ in manifest) + ')', list(manifest))}
    if actual != manifest or (expected_manifest is not None and digest(manifest) != expected_manifest):
        raise MigrationFailure('current proof schema/ABI mismatch; no automatic repair')
    # Other source triggers could rewrite/reset counters or suppress writes.
    # Their effects are not inferred; qualification requires this exact ABI.
    unexpected = connection.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN (" +
        ','.join('?' for _ in (PROOF_STORES + (PROOF_TABLE,))) + ') AND name NOT IN (' +
        ','.join('?' for _ in manifest) + ') LIMIT 1', list(PROOF_STORES) + [PROOF_TABLE] + list(manifest)).fetchone()
    if unexpected:
        raise MigrationFailure('unqualified trigger intersects current proof sources')
    if connection.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL LIMIT 1", (PROOF_TABLE,)).fetchone():
        raise MigrationFailure('unqualified index changes current proof write cost')
    rows = connection.execute('SELECT store,revision FROM ' + PROOF_TABLE + ' LIMIT 7').fetchall()
    counters = dict(rows)
    if (len(rows) != 6 or set(counters) != set(PROOF_STORES)
            or any(type(value) is not int or value < 0 for value in counters.values())):
        raise MigrationFailure('current proof counters missing/invalid')
    generation = connection.execute("SELECT value FROM catalog_store_meta WHERE key='generation'").fetchone()
    return {'abi': PROOF_ABI, 'manifestHash': digest(manifest), 'sourceCounters': counters,
            'schemaVersion': connection.execute('PRAGMA schema_version').fetchone()[0],
            'registryGeneration': generation[0] if generation else None}


def install_current_proof(connection):
    """Install empty counters plus source triggers atomically with a receipt."""
    if not connection.in_transaction:
        raise MigrationFailure('current proof installation requires a caller transaction')
    manifest = current_proof_manifest()
    if connection.execute('SELECT 1 FROM sqlite_master WHERE name IN (' +
            ','.join('?' for _ in manifest) + ') LIMIT 1', list(manifest)).fetchone():
        raise MigrationFailure('unowned or partial current proof schema; no automatic adoption/repair')
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN (" +
        ','.join('?' for _ in PROOF_STORES) + ')', PROOF_STORES)}
    if tables != set(PROOF_STORES):
        raise MigrationFailure('current proof requires all six public source tables')
    for statement in current_proof_schema():
        connection.execute(statement)
    connection.executemany('INSERT INTO ' + PROOF_TABLE + ' VALUES(?,0)', [(store,) for store in PROOF_STORES])
    return current_proof_snapshot(connection)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


class MigrationFailure(RuntimeError):
    def __init__(self, message, committed=None, candidate=None, outcome='rolled_back_pending_batch'):
        super().__init__(message)
        self.committed = list(committed or [])
        self.candidate = candidate
        self.outcome = outcome


class Budget:
    """Cooperative SQL limits, not hard physical I/O or fsync latency limits."""
    def __init__(self, seconds=3, queries=2000, vm_steps=1000000, changes=4000,
                 write_sql_bytes=1048576, new_page_bytes=1048576):
        values = ((seconds, .05, 10), (queries, 20, 4000), (vm_steps, 1000, 4000000),
                  (changes, 10, 12000), (write_sql_bytes, 4096, 4194304),
                  (new_page_bytes, 4096, 4194304))
        if any(isinstance(v, bool) or not low <= v <= high for v, low, high in values):
            raise ValueError('budget outside bounded supported range')
        self.started, self.seconds = time.monotonic(), seconds
        self.max_queries, self.max_vm_steps, self.max_changes = queries, vm_steps, changes
        self.max_sql_bytes, self.max_page_bytes = write_sql_bytes, new_page_bytes
        self.queries = self.vm_steps = self.sql_bytes = self.changes = 0
        self.reason = None
        self.connection = None

    def remaining_seconds(self):
        return max(0, self.seconds - (time.monotonic() - self.started))

    def attach(self, connection):
        self.connection, self.base_changes = connection, connection.total_changes
        connection.set_trace_callback(self.trace)
        connection.set_progress_handler(self.progress, 100)

    def detach(self):
        if self.connection is not None:
            self.changes += self.connection.total_changes - self.base_changes
            self.connection.set_trace_callback(None)
            self.connection.set_progress_handler(None, 0)
            self.connection = None

    def trace(self, sql):
        prefix = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ''
        if prefix not in ('BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE'):
            self.queries += 1
        if prefix in ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'CREATE', 'ALTER', 'DROP'):
            self.sql_bytes += len(sql.encode('utf-8'))
        self.exhausted()

    def exhausted(self):
        changed = self.changes + (self.connection.total_changes - self.base_changes if self.connection is not None else 0)
        checks = ((self.remaining_seconds() <= 0, 'SQL time budget exhausted'),
                  (self.queries > self.max_queries, 'SQL query budget exhausted'),
                  (self.vm_steps > self.max_vm_steps, 'VM budget exhausted'),
                  (changed > self.max_changes, 'logical row-write budget exhausted'),
                  (self.sql_bytes > self.max_sql_bytes, 'expanded write-SQL byte budget exhausted'))
        if self.reason is None:
            self.reason = next((reason for hit, reason in checks if hit), None)
        return self.reason

    def progress(self):
        self.vm_steps += 100
        return int(self.exhausted() is not None)

    def check(self, connection=None, page_start=None):
        if self.exhausted():
            raise MigrationFailure(self.reason)
        if connection is not None and page_start is not None:
            pages = connection.execute('PRAGMA page_count').fetchone()[0]
            size = connection.execute('PRAGMA page_size').fetchone()[0]
            if max(0, pages - page_start) * size > self.max_page_bytes:
                raise MigrationFailure('allocated page-growth budget exhausted')
        if self.exhausted():
            raise MigrationFailure(self.reason)

    def snapshot(self):
        return {'elapsedSqlWorkSeconds': time.monotonic() - self.started,
                'sqlStatements': self.queries, 'approximateVmSteps': self.vm_steps,
                'logicalChangedRows': self.changes + (self.connection.total_changes - self.base_changes if self.connection is not None else 0),
                'expandedWriteSqlBytes': self.sql_bytes,
                'physicalIoBounded': False, 'commitRollbackLatencyBounded': False}


def _normalized(sql):
    return re.sub(r'\s+', ' ', re.sub(r'\bIF\s+NOT\s+EXISTS\b', '', sql, flags=re.I)).strip().rstrip(';').casefold()


def declared_schema():
    """The unchanged logical schema ABI used by every preserved receipt.

    This one exact optional performance index is qualified independently. A
    different declaration bearing its name is rejected rather than excluded.
    """
    result = {}
    for statement in tuple(SCHEMA) + CONTROL_SCHEMA:
        match = re.match(r'CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)', statement, re.I)
        if not match:
            raise MigrationFailure('unsupported declared schema')
        kind, name = match.group(1).lower(), match.group(2)
        optional = OPTIONAL_PERFORMANCE_INDEXES.get(name)
        if optional is not None:
            if kind != 'index' or _normalized(statement) != _normalized(optional['ddl']):
                raise MigrationFailure('optional performance index declaration mismatch')
            continue
        result[name] = (kind, _normalized(statement))
    return result


def validate_optional_performance_indexes(connection):
    """Read-only exact optional-index qualification; no adoption or repair.

    Missing is permitted for legacy databases. Present must be the one ordinary
    complete nonunique index with the approved column order and collations.
    This returns evidence only and never rewrites control or receipt rows.
    """
    names = tuple(OPTIONAL_PERFORMANCE_INDEXES)
    rows = connection.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name IN (' +
        ','.join('?' for _ in names) + ') LIMIT ?', list(names) + [len(names) + 1]).fetchall()
    if len(rows) > len(names):
        raise MigrationFailure('optional performance index metadata exceeds bound')
    evidence = {}
    for kind, name, table, sql in rows:
        approved = OPTIONAL_PERFORMANCE_INDEXES[name]
        if kind != 'index' or table != approved['table'] or _normalized(sql or '') != _normalized(approved['ddl']):
            raise MigrationFailure('optional performance index schema mismatch')
        quoted = '"' + name.replace('"', '""') + '"'
        signature = [tuple(row) for row in connection.execute('PRAGMA index_xinfo(' + quoted + ')').fetchmany(4)]
        if signature != list(approved['xinfo']):
            raise MigrationFailure('optional performance index key/collation signature mismatch')
        quoted_table = '"' + table.replace('"', '""') + '"'
        entries = connection.execute('PRAGMA index_list(' + quoted_table + ')').fetchmany(129)
        if len(entries) > 128:
            raise MigrationFailure('optional performance index list exceeds bound')
        entry = [tuple(row) for row in entries if row[1] == name]
        if len(entry) != 1 or entry[0][2:] != (0, 'c', 0):
            raise MigrationFailure('optional performance index must be ordinary complete nonunique')
        evidence[name] = {'table': table, 'signature': signature,
                          'ordinary': True, 'unique': False, 'partial': False}
    return evidence


def validate_schema(connection, partial=False):
    expected = declared_schema()
    marks = ','.join('?' for _ in expected)
    actual = {row[1]: (row[0], _normalized(row[2] or '')) for row in connection.execute(
        'SELECT type,name,sql FROM sqlite_master WHERE name IN (' + marks + ') LIMIT ?',
        list(expected) + [len(expected) + 1])}
    if any(actual[name] != expected[name] for name in actual) or (not partial and set(actual) != set(expected)):
        raise MigrationFailure('registry/control schema mismatch; no automatic repair')
    tables = [name for name, (kind, _) in expected.items() if kind == 'table']
    if connection.execute('SELECT 1 FROM sqlite_master WHERE type=\'trigger\' AND tbl_name IN (' +
                          ','.join('?' for _ in tables) + ') LIMIT 1', tables).fetchone():
        raise MigrationFailure('unexpected registry trigger')
    allowed_indexes = list(expected) + list(OPTIONAL_PERFORMANCE_INDEXES)
    allowed_marks = ','.join('?' for _ in allowed_indexes)
    if connection.execute('SELECT 1 FROM sqlite_master WHERE type=\'index\' AND sql IS NOT NULL AND tbl_name IN (' +
                          ','.join('?' for _ in tables) + ') AND name NOT IN (' + allowed_marks + ') LIMIT 1',
                          tables + allowed_indexes).fetchone():
        raise MigrationFailure('unexpected registry index/write amplification')
    validate_optional_performance_indexes(connection)
    if 'catalog_store_meta' in actual:
        row = connection.execute("SELECT value FROM catalog_store_meta WHERE key='schema_version'").fetchone()
        allowed = {str(SCHEMA_VERSION)} | ({'1'} if partial else set())
        if row and row[0] not in allowed:
            raise MigrationFailure('unsupported registry schema version')
    # Keep every prior logical receipt/control schema_hash byte-for-byte valid.
    return digest(actual)


def load_checkpoint(connection, key, schema_hash, expected, initial, receipt_directory=None):
    row = connection.execute('SELECT progress_json,receipt_no,receipt_hash,schema_hash FROM ' + CONTROL +
                             ' WHERE migration_key=?', (key,)).fetchone()
    if row is None:
        if expected:
            raise MigrationFailure('expected receipt but no checkpoint exists')
        return initial, 0, None
    if not expected or expected != row[2] or schema_hash != row[3]:
        raise MigrationFailure('resume requires preserved matching receipt and schema digest')
    progress = json.loads(row[0])
    saved = connection.execute('SELECT receipt_json,receipt_hash FROM ' + RECEIPTS +
                               ' WHERE migration_key=? AND receipt_no=?', (key, row[1])).fetchone()
    body = json.loads(saved[0]) if saved else None
    if not saved or digest(body) != saved[1] or saved[1] != row[2] or body.get('progressAfter') != progress:
        raise MigrationFailure('receipt/progress integrity mismatch')
    if body.get('receiptNo') != row[1] or body.get('schemaHash') != schema_hash:
        raise MigrationFailure('receipt sequence/schema mismatch')
    if receipt_directory is None or progress.get('receiptDirectory') != str(Path(receipt_directory).resolve()):
        raise MigrationFailure('resume requires the original preserved receipt directory')
    encoded_body = canonical(body)
    valid_external = False
    for state in ('pending','committed'):
        target = Path(receipt_directory) / ('batch-%08d-%s.%s.json' % (row[1], row[2], state))
        if not target.is_file():
            continue
        if target.stat().st_size > 262144:
            raise MigrationFailure('oversized prior external receipt')
        wrapper = json.loads(target.read_text(encoding='utf-8'))
        if wrapper.get('receiptHash') == row[2] and wrapper.get('state') == state and canonical(wrapper.get('receipt')) == encoded_body:
            valid_external = True
            break
    if not valid_external:
        raise MigrationFailure('matching prior external receipt is missing or corrupt; preserve the complete older chain')
    if not CatalogStore(connection).validate_affected(body.get('result', {})).get('valid'):
        raise MigrationFailure('prior touched-row invariants no longer match')
    if canonical(snapshot_affected(connection,body.get('result', {}))) != canonical(body.get('affectedRegistryAfter')):
        raise MigrationFailure('prior affected registry rows changed or disappeared; qualified repair required')
    if row[1] > 1:
        prior = connection.execute('SELECT receipt_hash FROM ' + RECEIPTS +
                                   ' WHERE migration_key=? AND receipt_no=?', (key, row[1] - 1)).fetchone()
        if not prior or body.get('previousHash') != prior[0]:
            raise MigrationFailure('receipt chain link mismatch')
    return progress, row[1], row[2]


def require_fresh_or_checkpoint(connection, key):
    """Reject unjournaled prior registry state without scanning table contents."""
    declared = declared_schema()
    present = {row[0] for row in connection.execute(
        'SELECT name FROM sqlite_master WHERE type=\'table\' AND name IN (' +
        ','.join('?' for _ in declared) + ')', list(declared))}
    if CONTROL in present and connection.execute(
            'SELECT 1 FROM ' + CONTROL + ' WHERE migration_key=?', (key,)).fetchone():
        return True
    for name in present:
        if name == 'catalog_store_meta':
            rows = connection.execute('SELECT key,value FROM catalog_store_meta LIMIT 3').fetchall()
            if len(rows) <= 2 and all((row[0] == 'schema_version' and row[1] in ('1', str(SCHEMA_VERSION))) or
                                      (row[0] == 'generation' and row[1] == '0') for row in rows):
                continue
        if connection.execute('SELECT 1 FROM "' + name + '" LIMIT 1').fetchone():
            raise MigrationFailure('Nonempty preexisting registry requires a separately qualified recovery export: ' + name)
    return False


def reconcile_pending_receipt(connection, key, directory, receipt_no, receipt_hash, promote=False):
    """Classify one exact pending candidate in a caller-owned DB snapshot.

    No directory scan, database mutation, candidate deletion or automatic retry.
    Promotion only writes the identical committed wrapper after DB evidence.
    """
    if not connection.in_transaction:
        raise MigrationFailure('receipt reconciliation requires a stable caller transaction')
    if type(receipt_no) is not int or receipt_no < 1 or not re.fullmatch('[0-9a-f]{64}', receipt_hash or ''):
        raise ValueError('exact receipt number and SHA256 required')
    directory = Path(directory).resolve()
    target = directory / ('batch-%08d-%s.pending.json' % (receipt_no, receipt_hash))
    if not target.is_file() or target.stat().st_size > 262144:
        raise MigrationFailure('missing or oversized exact pending receipt')
    wrapper = json.loads(target.read_text(encoding='utf-8'))
    body = wrapper.get('receipt')
    if (not isinstance(body, dict) or wrapper.get('state') != 'pending'
            or wrapper.get('receiptHash') != receipt_hash or digest(body) != receipt_hash
            or body.get('receiptNo') != receipt_no
            or body.get('progressAfter', {}).get('receiptDirectory') != str(directory)):
        raise MigrationFailure('pending receipt identity/body/directory mismatch')
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?)",
                                                 (CONTROL, RECEIPTS))}
    head = connection.execute('SELECT progress_json,receipt_no,receipt_hash,schema_hash FROM ' + CONTROL +
                              ' WHERE migration_key=?', (key,)).fetchone() if CONTROL in tables else None
    saved = connection.execute('SELECT receipt_json,receipt_hash FROM ' + RECEIPTS +
                               ' WHERE migration_key=? AND receipt_no=?', (key, receipt_no)).fetchone() if RECEIPTS in tables else None
    result = {'receiptNo': receipt_no, 'receiptHash': receipt_hash, 'pendingReceipt': str(target),
              'pendingPreserved': True, 'databaseMutated': False, 'promoted': False}
    if not saved:
        if (head and receipt_no == head[1] + 1 and body.get('previousHash') == head[2]) or (
                not head and receipt_no == 1 and body.get('previousHash') is None):
            return {**result, 'status': 'not_committed_at_snapshot', 'retryAutomatic': False}
        raise MigrationFailure('pending candidate cannot be reconciled with persisted sequence')
    if (not head or receipt_no > head[1] or saved[1] != receipt_hash
            or canonical(json.loads(saved[0])) != canonical(body) or body.get('schemaHash') != head[3]):
        raise MigrationFailure('pending candidate conflicts with committed receipt/control')
    head_row = connection.execute('SELECT receipt_json,receipt_hash FROM ' + RECEIPTS +
                                  ' WHERE migration_key=? AND receipt_no=?', (key, head[1])).fetchone()
    head_body = json.loads(head_row[0]) if head_row else None
    if (not head_row or head_row[1] != head[2] or digest(head_body) != head[2]
            or head_body.get('progressAfter') != json.loads(head[0])):
        raise MigrationFailure('committed receipt head is inconsistent')
    if receipt_no < head[1]:
        successor = connection.execute('SELECT receipt_json,receipt_hash FROM ' + RECEIPTS +
            ' WHERE migration_key=? AND receipt_no=?', (key, receipt_no + 1)).fetchone()
        successor_body = json.loads(successor[0]) if successor else None
        if (not successor or digest(successor_body) != successor[1]
                or successor_body.get('receiptNo') != receipt_no + 1
                or successor_body.get('previousHash') != receipt_hash):
            raise MigrationFailure('candidate successor link is inconsistent')
    if promote:
        result['committedReceipt'] = write_external_receipt(directory, body, receipt_hash, 'committed')
        result['promoted'] = True
    return {**result, 'status': 'committed',
            'chainCheck': 'candidate_immediate_successor_and_control_head; not a global chain scan'}


def storage_observation(path, directory, database_free_floor, receipt_free_floor):
    """Fresh space observations, never a reservation or physical WAL cap."""
    if any(type(value) is not int or value < 1048576 for value in (database_free_floor, receipt_free_floor)):
        raise ValueError('free-space engineering floors must each be at least 1 MiB')
    path, directory = Path(path), Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    database_free = shutil.disk_usage(path.parent).free
    receipt_free = shutil.disk_usage(directory).free
    if database_free < database_free_floor or receipt_free < receipt_free_floor:
        raise MigrationFailure('database/receipt free-space engineering floor not met')
    wal = Path(str(path) + '-wal')
    return {'databaseBytes': path.stat().st_size, 'walBytes': wal.stat().st_size if wal.exists() else 0,
            'databaseVolumeFreeBytes': database_free, 'receiptVolumeFreeBytes': receipt_free,
            'databaseFreeFloorBytes': database_free_floor, 'receiptFreeFloorBytes': receipt_free_floor,
            'physicalAllocationCap': False, 'concurrentWriterSpaceReserved': False}


def write_external_receipt(directory, body, head, state):
    """File-fsync and atomically name a bounded logical record.

    POSIX also syncs the directory. Windows rename durability requires separately
    qualified receipt storage; this function cannot prove hardware durability.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    encoded = (canonical({'receiptHash': head, 'state': state, 'receipt': body}) + '\n').encode('utf-8')
    if len(encoded) > 262144:
        raise MigrationFailure('external logical receipt exceeds 256 KiB budget')
    target = directory / ('batch-%08d-%s.%s.json' % (body['receiptNo'], head, state))
    if target.exists():
        if target.stat().st_size != len(encoded) or target.read_bytes() != encoded:
            raise MigrationFailure('external receipt collision/mismatch')
        return str(target.resolve())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('wb', dir=directory, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        if os.name == 'posix':
            descriptor = os.open(str(directory), os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return str(target.resolve())
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def snapshot_affected(connection, result, max_rows=1000, max_bytes=131072):
    """Capture bounded actual IDs/aliases/locators; never copy original history."""
    ids = result.get('affectedItemIds')
    if not isinstance(ids, list) or len(ids) > 100:
        raise MigrationFailure('core batch must report <=100 affected item IDs')
    snapshots, total_rows = {}, 0
    if not ids:
        return snapshots
    tables = ('catalog_items_v2', 'catalog_identifiers_v2', 'catalog_aliases_v2',
              'catalog_metadata_v2', 'catalog_legacy_bindings_v2', 'catalog_pending_legacy_links_v2')
    for table in tables:
        cursor = connection.execute('SELECT * FROM ' + table + ' WHERE item_id IN (' +
                                    ','.join('?' for _ in ids) + ') LIMIT ?', ids + [max_rows - total_rows + 1])
        columns = [field[0] for field in cursor.description]
        rows = sorted((dict(zip(columns, row)) for row in cursor.fetchall()),key=canonical)
        total_rows += len(rows)
        if total_rows > max_rows:
            raise MigrationFailure('affected-row recovery snapshot budget exhausted')
        snapshots[table] = rows
        if len(canonical(snapshots).encode('utf-8')) > max_bytes:
            raise MigrationFailure('affected-row recovery snapshot byte budget exhausted')
    for item_id in ids:
        if not connection.execute('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (item_id,)).fetchone():
            raise MigrationFailure('missing affected identity')
    for table in tables[1:]:
        if connection.execute('SELECT 1 FROM ' + table + ' x LEFT JOIN catalog_items_v2 i ON i.item_id=x.item_id '
                              'WHERE x.item_id IN (' + ','.join('?' for _ in ids) + ') AND i.item_id IS NULL LIMIT 1', ids).fetchone():
            raise MigrationFailure('affected registry foreign-key invariant failed: ' + table)
    if connection.execute('SELECT 1 FROM catalog_pending_legacy_links_v2 x LEFT JOIN catalog_items_v2 i ON i.item_id=x.reference_item_id '
                          'WHERE x.item_id IN (' + ','.join('?' for _ in ids) + ') AND i.item_id IS NULL LIMIT 1',ids).fetchone():
        raise MigrationFailure('affected pending legacy reference is missing')
    return snapshots
