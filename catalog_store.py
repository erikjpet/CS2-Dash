"""Explicit catalog writes and pure identity/quote/history reads (stdlib only).

Constructor and request reads never create schema, migrate data or fetch network
content. Write methods never commit: the caller owns the transaction. Legacy
history remains in place and is accessed only through verified indexed keys.
"""

import base64
import hashlib
import json
import math
import re
import time
import uuid


SCHEMA_VERSION = 2
PROVIDERS = ('steam', 'steam_snapshot', 'steam_history', 'csfloat', 'buff163',
             'skinport', 'youpin', 'market')
MAX_HISTORY_STREAMS = 128
LEGACY_HISTORY_PREFIXES = {
    'price_history': ('name', 'provider'),
    'price_observations': ('name', 'provider'),
    'shared_price_history': ('name', 'lane', 'source'),
    'shared_price_observations': ('name', 'lane'),
}
LEGACY_CURRENT_PREFIXES = {
    'market_item_prices': ('name', 'provider'),
    'price_quotes': ('name', 'provider'),
    'shared_item_prices': ('name', 'lane'),
    'shared_source_quotes': ('name', 'source'),
}
LEGACY_PUBLIC_PREFIXES = {**LEGACY_HISTORY_PREFIXES, **LEGACY_CURRENT_PREFIXES}
# Preserve every available compact field, including optional currency columns,
# without loading raw evidence. Column availability is inspected once per store.
CURRENT_QUOTE_FIELDS = (
    'name', 'provider', 'lane', 'source', 'market_hash_name', 'price', 'median',
    'solid_price', 'lowest_sell_order', 'highest_buy_order', 'spread_pct',
    'stability_pct', 'confidence_pct', 'volume', 'basis', 'kind', 'url',
    'confidence', 'confidence_tier', 'source_count', 'history_source',
    'history_points', 'method', 'market_class', 'price_kind', 'source_url',
    'currency', 'currency_code', 'observed_at', 'updated_at',
)
LEGACY_HISTORY_KINDS = {'price_history': 'steam', 'price_observations': 'observations',
                        'shared_price_history': 'shared_history', 'shared_price_observations': 'shared_observations'}
LEGACY_POINT_PROJECTIONS = {
    'price_history': 'name,provider,point_time,point_ts,raw_time,price,volume,updated_at',
    'price_observations': 'name,provider,observed_at,price,median,volume,source,updated_at',
    'shared_price_history': 'name,lane,source,point_ts,point_time,price,volume,updated_at',
    'shared_price_observations': 'name,lane,observed_at,price,confidence,source_count,updated_at',
}
LEGACY_STORES = frozenset((
    'item_catalog', 'market_items', 'market_item_prices', 'price_quotes',
    'price_history', 'price_observations', 'shared_item_prices',
    'shared_source_quotes', 'shared_price_history', 'shared_price_observations',
    'portfolio_items', 'snapshot_items'))


class IdentityConflict(ValueError):
    pass


class LegacyAccessError(ValueError):
    def __init__(self, reason, report=None):
        self.reason, self.report = reason, report or {}
        super().__init__(reason)


class _LegacyBudget:
    """Counts statements before execution; time checks are cooperative only."""
    def __init__(self, max_queries=400, seconds=1):
        if type(max_queries) is not int or not 1 <= max_queries <= 4096:
            raise ValueError('max_queries must be 1..4096')
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or not 0 < seconds <= 10:
            raise ValueError('seconds must be positive and at most 10')
        self.maximum, self.queries = max_queries, 0
        self.started = time.monotonic()
        self.deadline = self.started + seconds

    def query(self, connection, sql, parameters=(), max_rows=128):
        if self.queries >= self.maximum:
            raise LegacyAccessError('query_budget')
        if time.monotonic() >= self.deadline:
            raise LegacyAccessError('deadline')
        self.queries += 1
        cursor = connection.execute(sql, parameters)
        names = [column[0] for column in cursor.description or []]
        values = cursor.fetchmany(max_rows + 1)
        if len(values) > max_rows:
            raise LegacyAccessError('metadata_row_budget')
        return [dict(zip(names, row)) for row in values]

    def report(self):
        return {'queries': self.queries, 'maxQueries': self.maximum,
                'queryBudgetScope': 'legacy schema/EXPLAIN/prefix/history reads; fixed catalog DTO and registry mutation/validation statements are separate',
                'elapsedSeconds': max(0, time.monotonic() - self.started),
                'wallTimeGuarantee': 'cooperative checks between indexed statements; pending calls may exceed deadline',
                'physicalIOGuarantee': False}


def _quoted(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _unjson(value, default=None):
    try:
        return json.loads(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _cursor(value):
    return base64.urlsafe_b64encode(_json(value).encode()).decode().rstrip('=')


def _uncursor(value):
    if not value:
        return None
    try:
        if len(value) > 65536:
            raise ValueError()
        result = json.loads(base64.urlsafe_b64decode(value + '=' * (-len(value) % 4)))
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except (TypeError, ValueError, UnicodeError):
        raise ValueError('invalid cursor') from None


def _rows(connection, sql, parameters=()):
    cursor = connection.execute(sql, parameters)
    names = [column[0] for column in cursor.description or []]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


SCHEMA = (
    'CREATE TABLE IF NOT EXISTS catalog_store_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)',
    '''CREATE TABLE IF NOT EXISTS catalog_items_v2(
        item_id TEXT PRIMARY KEY, name TEXT NOT NULL, entity_type TEXT NOT NULL,
        priceable INTEGER NOT NULL, kind TEXT, market_hash_name TEXT,
        display_source TEXT NOT NULL, first_seen_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)''',
    'CREATE INDEX IF NOT EXISTS catalog_items_name_v2 ON catalog_items_v2(name,item_id)',
    '''CREATE TABLE IF NOT EXISTS catalog_identifiers_v2(
        namespace TEXT NOT NULL,value TEXT NOT NULL,item_id TEXT NOT NULL,
        PRIMARY KEY(namespace,value),FOREIGN KEY(item_id) REFERENCES catalog_items_v2(item_id))''',
    'CREATE INDEX IF NOT EXISTS catalog_identifier_item_v2 ON catalog_identifiers_v2(item_id,namespace,value)',
    '''CREATE TABLE IF NOT EXISTS catalog_aliases_v2(
        namespace TEXT NOT NULL,value TEXT NOT NULL,item_id TEXT NOT NULL,kind TEXT NOT NULL,
        PRIMARY KEY(namespace,value,item_id),FOREIGN KEY(item_id) REFERENCES catalog_items_v2(item_id))''',
    'CREATE INDEX IF NOT EXISTS catalog_alias_value_v2 ON catalog_aliases_v2(value,item_id)',
    'CREATE INDEX IF NOT EXISTS catalog_alias_item_v2 ON catalog_aliases_v2(item_id,namespace,value)',
    '''CREATE TABLE IF NOT EXISTS catalog_metadata_v2(
        item_id TEXT NOT NULL,source TEXT NOT NULL,snapshot_id TEXT,
        metadata_json TEXT NOT NULL,raw_ref_json TEXT,active INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,PRIMARY KEY(item_id,source))''',
    '''CREATE TABLE IF NOT EXISTS catalog_sources_v2(
        source TEXT PRIMARY KEY,last_attempt_at INTEGER NOT NULL,last_success_at INTEGER,
        status TEXT NOT NULL,error TEXT,generation INTEGER NOT NULL DEFAULT 0)''',
    '''CREATE TABLE IF NOT EXISTS catalog_legacy_bindings_v2(
        item_id TEXT NOT NULL,store TEXT NOT NULL,legacy_name TEXT NOT NULL,
        provider TEXT NOT NULL DEFAULT '',lane TEXT NOT NULL DEFAULT '',source TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(item_id,store,legacy_name,provider,lane,source),
        UNIQUE(store,legacy_name,provider,lane,source))''',
    '''CREATE TABLE IF NOT EXISTS catalog_pending_legacy_links_v2(
        item_id TEXT NOT NULL,reference_item_id TEXT NOT NULL,store TEXT NOT NULL,legacy_name TEXT NOT NULL,
        provider TEXT NOT NULL,lane TEXT NOT NULL,source TEXT NOT NULL,status TEXT NOT NULL,created_at INTEGER NOT NULL,
        PRIMARY KEY(item_id,reference_item_id,store,legacy_name,provider,lane,source))''',
    '''CREATE INDEX IF NOT EXISTS catalog_pending_reference_v2
        ON catalog_pending_legacy_links_v2(reference_item_id,item_id)''',
    '''CREATE TABLE IF NOT EXISTS catalog_migration_v2(
        source TEXT PRIMARY KEY,last_key TEXT,processed INTEGER NOT NULL DEFAULT 0,
        done INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS catalog_payloads_v2(
        payload_id TEXT PRIMARY KEY,data BLOB NOT NULL,media_type TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS catalog_history_revisions_v2(
        revision_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,provider TEXT NOT NULL,
        point_key TEXT NOT NULL,point_ts INTEGER,raw_time TEXT,grain TEXT NOT NULL,
        price REAL,volume INTEGER,source TEXT NOT NULL,raw_ref_json TEXT,payload_id TEXT,
        first_fetched_at INTEGER NOT NULL,last_fetched_at INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS catalog_history_points_v2(
        item_id TEXT NOT NULL,provider TEXT NOT NULL,point_key TEXT NOT NULL,
        point_ts INTEGER,revision_id TEXT NOT NULL,first_fetched_at INTEGER NOT NULL,
        last_fetched_at INTEGER NOT NULL,PRIMARY KEY(item_id,provider,point_key))''',
    '''CREATE INDEX IF NOT EXISTS catalog_history_revision_key_v2
        ON catalog_history_revisions_v2(item_id,provider,point_key,revision_id)''',
    '''CREATE INDEX IF NOT EXISTS catalog_history_time_v2
        ON catalog_history_points_v2(item_id,provider,point_ts DESC,point_key DESC)''',
    '''CREATE TABLE IF NOT EXISTS catalog_history_coverage_v2(
        store TEXT PRIMARY KEY,cursor TEXT,done INTEGER NOT NULL DEFAULT 0,
        observed_streams INTEGER NOT NULL DEFAULT 0,registered_references INTEGER NOT NULL DEFAULT 0,
        verified_owned INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,error TEXT,
        pass_id TEXT NOT NULL,window_mode TEXT NOT NULL,updated_at INTEGER NOT NULL,last_batch_hash TEXT)''',
    '''CREATE TABLE IF NOT EXISTS catalog_history_batches_v2(
        batch_hash TEXT PRIMARY KEY,store TEXT NOT NULL,schema_version INTEGER NOT NULL,
        cursor_before TEXT,cursor_after TEXT,streams_json TEXT NOT NULL,
        affected_json TEXT NOT NULL,created_at INTEGER NOT NULL)''',
)


class CatalogStore:
    def __init__(self, connection, legacy_connection=None):
        self.connection = connection
        self.legacy_connection = legacy_connection if legacy_connection is not None else connection
        self._legacy_table_cache = {}
        self._legacy_index_cache = {}
        self._legacy_projection_cache = {}
        self._owner_schema_available = None

    def _all(self, sql, parameters=()):
        return _rows(self.connection, sql, parameters)

    def _one(self, sql, parameters=()):
        rows = self._all(sql, parameters)
        return rows[0] if rows else None

    def _has_table(self, name, legacy=False):
        if legacy and name in self._legacy_table_cache:
            return self._legacy_table_cache[name]
        connection = self.legacy_connection if legacy else self.connection
        result = bool(_rows(connection, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)))
        if legacy:
            self._legacy_table_cache[name] = result
        return result

    def ready(self):
        return self._has_table('catalog_store_meta') and bool(self._one(
            "SELECT value FROM catalog_store_meta WHERE key='schema_version' AND value=?", (str(SCHEMA_VERSION),)))

    def _require_ready(self):
        if not self.ready():
            raise RuntimeError('catalog storage requires explicit migration')

    def _begin_write(self):
        if not self.connection.in_transaction:
            self.connection.execute('BEGIN')

    def _bump_generation(self):
        self.connection.execute("UPDATE catalog_store_meta SET value=CAST(value AS INTEGER)+1 WHERE key='generation'")

    def create_schema(self):
        # executescript would implicitly commit a caller's pending transaction.
        if not self.connection.in_transaction:
            self.connection.execute('BEGIN')
        for statement in SCHEMA:
            self.connection.execute(statement)
        self.connection.execute("INSERT OR IGNORE INTO catalog_store_meta VALUES('schema_version',?)", (str(SCHEMA_VERSION),))
        self.connection.execute("INSERT OR IGNORE INTO catalog_store_meta VALUES('generation','0')")
        previous = self._one("SELECT value FROM catalog_store_meta WHERE key='schema_version'")
        if previous and previous['value'] == '1':
            # An explicit v2 migration adds only the small registry/receipt tables.
            self.connection.execute("UPDATE catalog_store_meta SET value=? WHERE key='schema_version'", (str(SCHEMA_VERSION),))
        if not self.ready():
            raise RuntimeError('unsupported catalog schema version')
        self._owner_schema_available = True

    @staticmethod
    def _source_priority(source):
        return 0 if source.startswith('catalog:') or source in ('legacy:item_catalog', 'manual') else 1

    @staticmethod
    def _legacy_record_source(source, name):
        return source + ':record:' + _json(name)

    def _identifiers(self, record, source, entity_type):
        explicit = record.get('identifiers') or []
        pairs = list(explicit.items()) if isinstance(explicit, dict) else [
            (value['namespace'], value['value']) if isinstance(value, dict) else tuple(value) for value in explicit]
        external_id = record.get('external_id', record.get('externalId', record.get('id')))
        if external_id is None:
            raw = _unjson(record.get('raw'), {}) if isinstance(record.get('raw'), str) else record.get('raw')
            if isinstance(raw, dict):
                external_id = raw.get('id')
        if external_id is not None:
            # Upstream base IDs may be reused for wear/StatTrak variants. A raw
            # source ID is not proof that distinct Steam listing hashes merge.
            scope = [entity_type, str(record.get('kind') or '')]
            if entity_type == 'listing':
                scope.append(str(record.get('market_hash_name', record.get('marketHashName')) or record['name']))
            pairs.append(('source:' + source + ':' + _json(scope), str(external_id)))
        market_hash = record.get('market_hash_name', record.get('marketHashName'))
        if entity_type == 'listing' and market_hash:
            pairs.append(('steam:730:market_hash_name', str(market_hash)))
        if not pairs:
            pairs.append(('source:' + source + ':' + entity_type + ':name', record['name']))
        return list(dict.fromkeys((str(namespace), str(value)) for namespace, value in pairs))

    def ensure_item(self, record, source='manual', attempted_at=None, snapshot_id=None):
        self._require_ready()
        self._begin_write()
        record = dict(record)
        name = record.get('name')
        if not isinstance(name, str) or not name:
            raise ValueError('nonempty exact item name required')
        now = int(time.time() if attempted_at is None else attempted_at)
        kind = record.get('kind')
        entity_type = record.get('entity_type', record.get('entityType')) or {
            'collections': 'collection', 'skin_families': 'skin_family'}.get(kind, 'listing')
        if entity_type not in ('listing', 'collection', 'skin_family', 'metadata', 'legacy_reference'):
            raise ValueError('unsupported catalog entity type')
        identifiers = self._identifiers(record, source, entity_type)
        mapped = {row['item_id'] for namespace, value in identifiers for row in self._all(
            'SELECT item_id FROM catalog_identifiers_v2 WHERE namespace=? AND value=?', (namespace, value))}
        requested = record.get('item_id', record.get('itemId'))
        if requested:
            if not self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (requested,)):
                raise ValueError('unknown explicit item ID')
            mapped.add(requested)
        if len(mapped) > 1:
            raise IdentityConflict('external identifiers refer to different existing items')
        item_id = next(iter(mapped)) if mapped else ('itm_' if entity_type == 'listing' else 'ent_') + uuid.uuid4().hex
        old = self._one('SELECT * FROM catalog_items_v2 WHERE item_id=?', (item_id,))
        if old and old['entity_type'] != entity_type:
            raise IdentityConflict('listing and metadata identities cannot be merged')
        market_hash = record.get('market_hash_name', record.get('marketHashName')) if entity_type == 'listing' else None
        if old and old['market_hash_name'] and market_hash and old['market_hash_name'] != market_hash and not requested:
            raise IdentityConflict('a changed exact Steam listing requires an explicit verified item ID')
        priceable = bool(record.get('priceable', entity_type == 'listing'))
        if old is None:
            self.connection.execute('INSERT INTO catalog_items_v2 VALUES(?,?,?,?,?,?,?,?,?)',
                (item_id, name, entity_type, int(priceable), kind, market_hash, source, now, now))
        elif self._source_priority(source) <= self._source_priority(old['display_source']):
            self.connection.execute('''UPDATE catalog_items_v2 SET name=?,kind=?,market_hash_name=?,
                priceable=?,display_source=?,updated_at=? WHERE item_id=?''',
                (name, kind or old['kind'], market_hash or old['market_hash_name'], int(priceable), source, now, item_id))
        for namespace, value in identifiers:
            self.connection.execute('INSERT OR IGNORE INTO catalog_identifiers_v2 VALUES(?,?,?)', (namespace, value, item_id))
            self.connection.execute('INSERT OR IGNORE INTO catalog_aliases_v2 VALUES(?,?,?,?)', (namespace, value, item_id, 'external'))
        aliases = [('name', name)]
        if market_hash:
            aliases.append(('steam:730:market_hash_name', str(market_hash)))
        for alias in record.get('aliases') or []:
            aliases.append((alias.get('namespace', 'name'), alias['value']) if isinstance(alias, dict) else ('name', alias))
        for namespace, value in aliases:
            if not isinstance(value, str) or not value:
                raise ValueError('nonempty exact aliases required')
            self.connection.execute('INSERT OR IGNORE INTO catalog_aliases_v2 VALUES(?,?,?,?)', (namespace, value, item_id, 'label'))
        metadata = dict(record.get('metadata') or {})
        metadata.pop('raw', None)
        metadata.pop('raw_ref', None)
        metadata.pop('rawRef', None)
        metadata.update({key: value for key, value in record.items() if key in (
            'name', 'kind', 'item_type', 'type', 'weapon', 'collection', 'rarity',
            'rarity_color', 'rarityColor', 'image', 'market_hash_name', 'marketHashName',
            'external_id', 'externalId', 'entity_type', 'entityType', 'priceable')})
        metadata.setdefault('type', record.get('item_type', kind))
        metadata.setdefault('rarityColor', record.get('rarity_color'))
        raw_ref = record.get('raw_ref', record.get('rawRef'))
        if raw_ref is None and record.get('raw') is not None:
            raw_ref = {'storage': 'catalog_payload', 'payloadId': self._store_payload(record['raw'])}
        record_source = None
        if (source in ('legacy:item_catalog', 'legacy:market_items') and isinstance(raw_ref, dict)
                and raw_ref.get('store') == source.split(':', 1)[1] and raw_ref.get('name') == name):
            previous_metadata = self._one('SELECT * FROM catalog_metadata_v2 WHERE item_id=? AND source=?', (item_id, source))
            previous_ref = _unjson(previous_metadata['raw_ref_json'], {}) if previous_metadata else {}
            collision = (isinstance(previous_ref, dict) and previous_ref.get('store') == raw_ref['store']
                         and isinstance(previous_ref.get('name'), str) and previous_ref['name'] != name)
            record_source = self._legacy_record_source(source, name)
            if collision:
                # Preserve the previous physical source row before the canonical
                # snapshot moves to a different alias of the same Steam item.
                self.connection.execute('''INSERT INTO catalog_metadata_v2 VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(item_id,source) DO UPDATE SET snapshot_id=excluded.snapshot_id,
                    metadata_json=excluded.metadata_json,raw_ref_json=excluded.raw_ref_json,
                    active=excluded.active,updated_at=excluded.updated_at''',
                    (item_id, self._legacy_record_source(source, previous_ref['name']),
                     previous_metadata['snapshot_id'], previous_metadata['metadata_json'],
                     previous_metadata['raw_ref_json'], previous_metadata['active'], previous_metadata['updated_at']))
            elif not self._one('SELECT 1 FROM catalog_metadata_v2 WHERE item_id=? AND source=?', (item_id, record_source)):
                record_source = None
        self.connection.execute('''INSERT INTO catalog_metadata_v2 VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(item_id,source) DO UPDATE SET snapshot_id=excluded.snapshot_id,
            metadata_json=excluded.metadata_json,raw_ref_json=excluded.raw_ref_json,
            active=1,updated_at=excluded.updated_at''',
            (item_id, source, snapshot_id, _json(metadata), _json(raw_ref) if raw_ref is not None else None, 1, now))
        if record_source is not None:
            self.connection.execute('''INSERT INTO catalog_metadata_v2
                SELECT item_id,?,snapshot_id,metadata_json,raw_ref_json,active,updated_at
                FROM catalog_metadata_v2 WHERE item_id=? AND source=?
                ON CONFLICT(item_id,source) DO UPDATE SET snapshot_id=excluded.snapshot_id,
                metadata_json=excluded.metadata_json,raw_ref_json=excluded.raw_ref_json,
                active=excluded.active,updated_at=excluded.updated_at''', (record_source, item_id, source))
        self._bump_generation()
        return item_id

    def bind_legacy(self, item_id, store, name, provider=None, lane=None, source=None):
        self._require_ready()
        self._begin_write()
        if store not in LEGACY_STORES or not isinstance(name, str) or not name:
            raise ValueError('known legacy store and exact name required')
        if not self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (item_id,)):
            raise ValueError('unknown item ID')
        provider, lane, source = provider or '', lane or '', source or ''
        conflicts = self._all('''SELECT item_id FROM catalog_legacy_bindings_v2 WHERE store=? AND legacy_name=?
            AND item_id<>? AND (provider='' OR ?='' OR provider=?)
            AND (lane='' OR ?='' OR lane=?) AND (source='' OR ?='' OR source=?)''',
            (store, name, item_id, provider, provider, lane, lane, source, source))
        if conflicts:
            raise IdentityConflict('legacy stream is already assigned to another identity')
        self.connection.execute('INSERT OR IGNORE INTO catalog_legacy_bindings_v2 VALUES(?,?,?,?,?,?)',
                                (item_id, store, name, provider, lane, source))

    def bind_legacy_or_defer(self, item_id, store, name, provider=None, lane=None, source=None):
        """Ingestion may save new facts while an old reference awaits verification.

        This records a decision request, never transfers a stream or treats a
        matching label as proof of identity. Conflicts with verified owners fail.
        """
        self._require_ready()
        self._begin_write()
        if store not in LEGACY_STORES or not isinstance(name, str) or not name:
            raise ValueError('known legacy store and exact name required')
        if not self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (item_id,)):
            raise ValueError('unknown item ID')
        provider, lane, source = provider or '', lane or '', source or ''
        conflicts = self._all('''SELECT b.*,i.entity_type FROM catalog_legacy_bindings_v2 b
            JOIN catalog_items_v2 i ON i.item_id=b.item_id WHERE b.store=? AND b.legacy_name=?
            AND b.item_id<>? AND (b.provider='' OR ?='' OR b.provider=?)
            AND (b.lane='' OR ?='' OR b.lane=?) AND (b.source='' OR ?='' OR b.source=?) LIMIT 129''',
            (store, name, item_id, provider, provider, lane, lane, source, source))
        if not conflicts:
            self.bind_legacy(item_id, store, name, provider, lane, source)
            return {'status': 'bound', 'itemId': item_id, 'store': store, 'name': name,
                    'provider': provider, 'lane': lane, 'source': source, 'pendingLinks': []}
        if len(conflicts) > 128 or any(row['entity_type'] != 'legacy_reference' for row in conflicts):
            raise IdentityConflict('legacy stream has a verified owner or exceeds the pending-link budget')
        pending = []
        changes_before = self.connection.total_changes
        for row in conflicts:
            scope = (item_id, row['item_id'], store, name, row['provider'], row['lane'], row['source'])
            self.connection.execute('''INSERT OR IGNORE INTO catalog_pending_legacy_links_v2
                VALUES(?,?,?,?,?,?,?,?,?)''', scope + ('requires_verified_decision', int(time.time())))
            pending.append(self._one('''SELECT * FROM catalog_pending_legacy_links_v2 WHERE item_id=?
                AND reference_item_id=? AND store=? AND legacy_name=? AND provider=? AND lane=? AND source=?''', scope))
        if self.connection.total_changes != changes_before:
            self._bump_generation()
        return {'status': 'pending', 'itemId': item_id, 'store': store, 'name': name,
                'provider': provider, 'lane': lane, 'source': source, 'pendingLinks': pending,
                'coverage': 'legacy reference remains separately addressable; verified link decision required'}

    def _pending_links(self, item_id):
        # Each branch uses its item-leading key; no global OR scan.
        rows = self._all('''SELECT * FROM catalog_pending_legacy_links_v2 WHERE item_id=?
            ORDER BY reference_item_id,store,legacy_name,provider,lane,source LIMIT 129''', (item_id,))
        remaining = max(0, 129 - len(rows))
        if remaining:
            rows.extend(self._all('''SELECT * FROM catalog_pending_legacy_links_v2
                WHERE reference_item_id=? ORDER BY item_id LIMIT ?''', (item_id, remaining)))
        return {'links': rows[:128], 'truncated': len(rows) > 128,
                'decision': 'requires explicit verification; never automatically reassigned'}

    def publish_source(self, source, records, attempted_at=None, complete=True, error=None):
        self._require_ready()
        self._begin_write()
        now = int(time.time() if attempted_at is None else attempted_at)
        if not source or not isinstance(source, str):
            raise ValueError('source required')
        old = self._one('SELECT * FROM catalog_sources_v2 WHERE source=?', (source,)) or {}
        if old and now < old['last_attempt_at']:
            return {'ok': False, 'source': source, 'published': 0,
                    'ignoredStaleAttempt': True, 'retainedLastGood': True}
        if error or not complete:
            self.connection.execute('''INSERT INTO catalog_sources_v2 VALUES(?,?,?,?,?,?)
                ON CONFLICT(source) DO UPDATE SET last_attempt_at=excluded.last_attempt_at,
                status=excluded.status,error=excluded.error''',
                (source, now, old.get('last_success_at'), 'failed' if error else 'partial',
                 str(error) if error else 'source snapshot incomplete', old.get('generation', 0)))
            return {'ok': False, 'source': source, 'published': 0, 'retainedLastGood': True}
        records = [dict(record) for record in records]
        snapshot = uuid.uuid4().hex
        self.connection.execute('SAVEPOINT catalog_source_publish')
        try:
            item_ids = [self.ensure_item(record, source=source, attempted_at=now, snapshot_id=snapshot)
                        for record in records]
            self.connection.execute('UPDATE catalog_metadata_v2 SET active=0 WHERE source=? AND snapshot_id IS NOT ?',
                                    (source, snapshot))
            generation = int(old.get('generation', 0)) + 1
            self.connection.execute('''INSERT INTO catalog_sources_v2 VALUES(?,?,?,?,?,?)
                ON CONFLICT(source) DO UPDATE SET last_attempt_at=excluded.last_attempt_at,
                last_success_at=excluded.last_success_at,status=excluded.status,error=NULL,
                generation=excluded.generation''', (source, now, now, 'complete', None, generation))
            self._bump_generation()
            self.connection.execute('RELEASE catalog_source_publish')
        except BaseException:
            self.connection.execute('ROLLBACK TO catalog_source_publish')
            self.connection.execute('RELEASE catalog_source_publish')
            raise
        return {'ok': True, 'source': source, 'published': len(item_ids), 'generation': generation, 'itemIds': item_ids}

    def bootstrap_legacy_batch(self, source='legacy:item_catalog', after=None, limit=500, restart=False, current_only=False):
        self._require_ready()
        self._begin_write()
        limit = max(1, min(500, int(limit)))
        if restart and after is not None:
            raise ValueError('metadata restart requires no cursor')
        progress = {} if restart else self._one('SELECT * FROM catalog_migration_v2 WHERE source=?', (source,)) or {}
        last = progress.get('last_key') if after is None else after
        if progress.get('done') and after is None:
            return {'processed': 0, 'lastKey': last, 'done': True, 'generation': self._generation(),
                    'affectedItemIds': [], 'affectedBindings': []}
        table = source[len('legacy:'):] if source.startswith('legacy:') else source
        if table not in ('item_catalog', 'market_items') or not self._has_table(table, legacy=True):
            raise ValueError('bootstrap supports existing current item_catalog or market_items only')
        columns = [row['name'] for row in _rows(self.legacy_connection, 'PRAGMA table_info(' + _quoted(table) + ')')]
        fields = [_quoted(column) for column in columns if column != 'raw']
        has_raw = 'raw' in columns
        if has_raw:
            if not self.legacy_connection.in_transaction:
                raise ValueError('raw identity extraction requires a caller-owned legacy snapshot')
            folded_columns = {column.casefold() for column in columns}
            rowid = next((name for name in ('_rowid_', 'rowid', 'oid') if name not in folded_columns), None)
            if rowid is None:
                raise ValueError('raw identity extraction requires an unshadowed SQLite rowid')
            encoding = _rows(self.legacy_connection, 'PRAGMA encoding')[0]['encoding']
            text_codec = {'UTF-8': 'utf-8', 'UTF-16le': 'utf-16-le', 'UTF-16be': 'utf-16-be'}.get(encoding)
            if text_codec is None:
                raise ValueError('unsupported SQLite TEXT encoding for raw identity extraction')
            fields.extend([rowid + ' AS _catalog_raw_rowid', 'typeof(raw) AS _catalog_raw_type'])
        records = _rows(self.legacy_connection, 'SELECT ' + ','.join(fields) + f' FROM {table} WHERE name' +
                        ('>=?' if last is None else '>?') + ' ORDER BY name LIMIT ?', (last or '', limit + 1))
        batch, done = records[:limit], len(records) <= limit
        affected_ids, affected_bindings, pending_links = [], [], []
        raw_bytes_read = 0
        for record in batch:
            record['raw_ref'] = {'store': table, 'name': record['name']}
            raw = {}
            if has_raw:
                raw_rowid, raw_type = record.pop('_catalog_raw_rowid'), record.pop('_catalog_raw_type')
                if raw_type in ('text', 'blob'):
                    # SQL scalar functions still load the full raw value and
                    # hit SQLITE_LIMIT_LENGTH. Incremental read-only BLOB access
                    # preserves raw.id without changing that write/receipt cap.
                    # blobopen does not invoke SQLite's SQL authorizer: readonly
                    # and exact same-snapshot rowid plus these caps are mandatory.
                    with self.legacy_connection.blobopen(table, 'raw', raw_rowid, readonly=True) as blob:
                        size = len(blob)
                        if size > 2 * 1024 * 1024 or raw_bytes_read + size > 8 * 1024 * 1024:
                            raise LegacyAccessError('raw_identity_byte_budget', {
                                'store': table, 'name': record['name'], 'recordBytes': size,
                                'batchBytesRead': raw_bytes_read, 'maxRecordBytes': 2 * 1024 * 1024,
                                'maxBatchBytes': 8 * 1024 * 1024})
                        payload = bytearray()
                        while len(payload) < size:
                            chunk = blob.read(min(65536, size - len(payload)))
                            if not chunk:
                                raise IOError('raw identity read ended before its declared length')
                            payload.extend(chunk)
                        raw_bytes_read += size
                    raw = _unjson(payload.decode(text_codec) if raw_type == 'text' else payload, {})
            if isinstance(raw, dict) and raw.get('id') is not None:
                record['external_id'] = raw['id']
            record['identifiers'] = [('legacy:' + table + ':' + str(record.get('kind') or ''), record['name'])]
            item_id = self.ensure_item(record, source=source, attempted_at=record.get('updated_at') or int(time.time()))
            affected_ids.append(item_id)
            # Only exact listing identities own price streams. Metadata entities
            # are addressable catalog records without invented price/history.
            if not current_only and self._one('SELECT entity_type FROM catalog_items_v2 WHERE item_id=?', (item_id,))['entity_type'] == 'listing':
                for name in dict.fromkeys((record['name'], record.get('market_hash_name') or record['name'])):
                    stores = (
                                  'market_item_prices', 'price_observations', 'price_history', 'shared_item_prices',
                                  'shared_source_quotes', 'shared_price_history', 'shared_price_observations',
                                  'portfolio_items', 'snapshot_items')
                    for store in stores:
                        binding = self.bind_legacy_or_defer(item_id, store, name)
                        if binding['status'] == 'bound':
                            affected_bindings.append({'itemId': item_id, 'store': store, 'name': name})
                        else:
                            pending_links.extend(binding['pendingLinks'])
            last = record['name']
        now = int(time.time())
        self.connection.execute('''INSERT INTO catalog_migration_v2 VALUES(?,?,?,?,?)
            ON CONFLICT(source) DO UPDATE SET last_key=excluded.last_key,
            processed=catalog_migration_v2.processed+excluded.processed,done=excluded.done,updated_at=excluded.updated_at''',
            (source, last, len(batch), int(done), now))
        if batch:
            self._bump_generation()
        return {'processed': len(batch), 'lastKey': last, 'done': done, 'generation': self._generation(),
                'rawIdentityBytesRead': raw_bytes_read,
                'affectedItemIds': sorted(set(affected_ids)), 'affectedBindings': affected_bindings,
                'affectedPendingLinks': pending_links}

    def verify_current_batch(self, store, cursor=None, limit=25, max_queries=400, seconds=1):
        """Pure source-to-registry parity, all metadata/unpriced rows included.

        Each page is an indexed source key range plus exact registry key probes.
        Completion describes this rolling traversal, not a frozen global view.
        """
        self._require_ready()
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('verification limit must be 1..100')
        budget = _LegacyBudget(max_queries, seconds)
        checked, witnesses = [], []
        if store in ('item_catalog', 'market_items'):
            token = _uncursor(cursor)
            if token and (token.get('store') != store or not isinstance(token.get('name'), str)):
                raise ValueError('current parity cursor scope changed')
            columns = {row['name'] for row in budget.query(self.legacy_connection,
                'PRAGMA table_info(' + _quoted(store) + ')')}
            if 'name' not in columns:
                raise LegacyAccessError('missing_table', {'store': store})
            fields = [field for field in ('name', 'kind', 'item_type', 'weapon', 'collection', 'rarity',
                       'rarity_color', 'image', 'market_hash_name', 'updated_at') if field in columns]
            last = token['name'] if token else ''
            sql = ('SELECT ' + ','.join(_quoted(field) for field in fields) + ' FROM ' + _quoted(store) +
                   ' WHERE name COLLATE BINARY' + ('>?' if token else '>=?') + ' ORDER BY name COLLATE BINARY LIMIT ?')
            plan = budget.query(self.legacy_connection, 'EXPLAIN QUERY PLAN ' + sql, (last, limit + 1), 16)
            details = [row['detail'].upper() for row in plan]
            if not any('SEARCH ' in value for value in details) or any('TEMP B-TREE' in value or 'SCAN ' in value for value in details):
                raise LegacyAccessError('no_index', {'store': store, 'plan': details})
            if budget.query(self.legacy_connection, 'SELECT name FROM ' + _quoted(store) + ' WHERE name IS NULL LIMIT 1', (), 1):
                raise IdentityConflict('NULL source name cannot be represented by a catalog identity')
            rows = budget.query(self.legacy_connection, sql, (last, limit + 1), limit + 1)
            done, rows = len(rows) <= limit, rows[:limit]
            next_cursor = None if done else _cursor({'store': store, 'name': rows[-1]['name']})
            for row in rows:
                namespace = 'legacy:' + store + ':' + str(row.get('kind') or '')
                canonical_source = 'legacy:' + store
                record_source = self._legacy_record_source(canonical_source, row['name'])
                owners = budget.query(self.connection, '''SELECT i.item_id,i.entity_type,i.priceable,m.metadata_json,m.raw_ref_json
                    FROM catalog_identifiers_v2 x JOIN catalog_items_v2 i ON i.item_id=x.item_id
                    JOIN catalog_metadata_v2 m ON m.item_id=i.item_id AND m.source IN (?,?)
                    WHERE x.namespace=? AND x.value=?
                    ORDER BY CASE WHEN m.source=? THEN 0 ELSE 1 END LIMIT 1''',
                    (record_source, canonical_source, namespace, row['name'], record_source), 1)
                entity_type = {'collections': 'collection', 'skin_families': 'skin_family'}.get(row.get('kind'), 'listing')
                if len(owners) != 1 or owners[0]['entity_type'] != entity_type:
                    raise IdentityConflict('missing or incompatible current metadata identity: ' + store + ':' + row['name'])
                owner = owners[0]
                metadata, raw_ref = _unjson(owner['metadata_json'], {}), _unjson(owner['raw_ref_json'], {})
                if (any(metadata.get(field) != value for field, value in row.items() if field != 'updated_at')
                        or raw_ref.get('store') != store or raw_ref.get('name') != row['name']
                        or bool(owner['priceable']) != (entity_type == 'listing')):
                    raise IdentityConflict('current metadata parity mismatch: ' + store + ':' + row['name'])
                checked.append(row)
                witnesses.append({'name': row['name'], 'itemId': owner['item_id']})
        elif store in LEGACY_CURRENT_PREFIXES:
            page = self._discover_prefixes(store, LEGACY_CURRENT_PREFIXES[store], cursor, limit, budget, 8192)
            if page['stoppedReason'] not in ('limit', 'eof'):
                raise LegacyAccessError(page['stoppedReason'], page)
            done, next_cursor = page['done'], page['cursor']
            for locator in page['streams']:
                ownership = self._locator_owners(locator, budget)
                if ownership['status'] != 'owned':
                    raise IdentityConflict('current physical stream lacks an unambiguous owner: ' + _json(locator))
                keys = LEGACY_CURRENT_PREFIXES[store]
                rows = budget.query(self.legacy_connection, 'SELECT ' + self._current_projection(store, budget) +
                    ' FROM ' + _quoted(store) + ' WHERE ' + ' AND '.join(_quoted(key) + '=?' for key in keys) +
                    ' LIMIT 2', [locator[key] for key in keys], 2)
                if len(rows) != 1:
                    raise IdentityConflict('current stream key no longer identifies exactly one row')
                checked.append(rows[0])
                witnesses.append({'locator': locator, 'itemId': ownership['owners'][0]})
        else:
            raise ValueError('verification supports the six public current stores only')
        # Additive digest is independent of page boundaries; it is not a
        # substitute for source revision qualification or a snapshot guarantee.
        digest_sum = sum(int(hashlib.sha256(_json(row).encode()).hexdigest(), 16) for row in checked) % (1 << 256)
        return {'store': store, 'processed': 0, 'checked': len(checked), 'done': done,
                'cursor': next_cursor, 'stoppedReason': 'eof' if done else 'limit',
                'sourceDigest': format(digest_sum, '064x'),
                'mappingDigest': hashlib.sha256(_json(witnesses).encode()).hexdigest(),
                'affectedItemIds': [], 'affectedBindings': [],
                'verification': 'source_to_registry_including_unpriced', 'snapshotComplete': False,
                **budget.report()}

    def _generation(self):
        row = self._one("SELECT value FROM catalog_store_meta WHERE key='generation'")
        return int(row['value']) if row else 0

    def _dto(self, row, metadata_rows=None):
        if metadata_rows is None:
            metadata_rows = self._all('''SELECT m.*,s.last_success_at,s.status FROM catalog_metadata_v2 m
                LEFT JOIN catalog_sources_v2 s ON s.source=m.source WHERE m.item_id=? ORDER BY m.source''', (row['item_id'],))
        metadata_rows = list(metadata_rows)
        metadata_rows.sort(key=lambda value: (not value['active'], self._source_priority(value['source']), value['source']))
        metadata = {}
        for value in reversed(metadata_rows):
            metadata.update(_unjson(value['metadata_json'], {}))
        preferred = metadata_rows[0] if metadata_rows else {}
        if row['entity_type'] == 'legacy_reference':
            quote_state = 'legacy_current_reference' if row['kind'] == 'legacy_current' else 'legacy_history_only'
        elif not row['priceable']:
            quote_state = 'not_priceable'
        elif row.get('_quotes_omitted'):
            quote_state = 'not_loaded'
        elif '_has_price' in row:
            quote_state = 'priced' if row['_has_price'] else 'unpriced'
        else:
            quote_state = self._quote_state(row['item_id'])
            self._attach_current_scope_issues([row])
        scope_issues = row.get('_quote_scope_issues', [])
        if quote_state == 'unpriced' and scope_issues:
            quote_state = 'scope_required'
        return {'itemId': row['item_id'], 'name': row['name'], 'entityType': row['entity_type'],
                'priceable': bool(row['priceable']), 'kind': row['kind'], 'marketHashName': row['market_hash_name'],
                'metadata': metadata, 'quoteState': quote_state,
                'quoteCoverage': {'complete': not scope_issues and not row.get('_quotes_omitted', False),
                                  'ignoredScopedBindings': scope_issues},
                'freshness': {'metadataUpdatedAt': row['updated_at'],
                              'lastSourceSuccessAt': preferred.get('last_success_at'),
                              'sourceStatus': preferred.get('status') or 'registered',
                              'quoteUpdatedAt': row.get('_quote_updated_at')},
                'active': any(value['active'] for value in metadata_rows),
                'provenance': [{'source': value['source'], 'rawRef': _unjson(value['raw_ref_json']),
                                'updatedAt': value['updated_at'], 'active': bool(value['active'])}
                               for value in metadata_rows]}

    def get_item(self, item_id):
        self._require_ready()
        projection = 'i.*'
        if self.legacy_connection is self.connection:
            projection += ',(' + self._price_expression() + ') AS _has_price,(' + self._quote_time_expression() + ') AS _quote_updated_at'
        row = self._one('SELECT ' + projection + ' FROM catalog_items_v2 i WHERE item_id=?', (item_id,))
        if not row:
            return None
        if self.legacy_connection is not self.connection:
            self._attach_quote_summary([row])
        self._attach_current_scope_issues([row])
        result = self._dto(row)
        result['pendingLegacyLinks'] = self._pending_links(item_id)
        return result

    def resolve(self, value, namespace=None):
        self._require_ready()
        if not isinstance(value, str) or not value:
            return {'status': 'not_found', 'candidates': []}
        if namespace is None and self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (value,)):
            return {'status': 'unique', 'candidates': [self.get_item(value)]}
        where, params = ('a.value=?', [value]) if namespace is None else ('a.namespace=? AND a.value=?', [namespace, value])
        rows = self._all(f'''SELECT DISTINCT i.* FROM catalog_aliases_v2 a JOIN catalog_items_v2 i ON i.item_id=a.item_id
            WHERE {where} ORDER BY i.item_id''', params)
        return {'status': 'unique' if len(rows) == 1 else ('ambiguous' if rows else 'not_found'),
                'candidates': [self._dto(row) for row in rows]}

    def list_items(self, query='', limit=100, cursor=None, entity_type=None, priced=None,
                   include_quotes=True, query_mode='contains', include_legacy_records=False):
        """Name search: compatible literal contains, or indexed BINARY prefix/exact.

        Contains searches and price filtering can examine many registry entries.
        Prefix/exact use the existing name index and do not promise case folding.
        Compact callers can omit quote work when serializing metadata only.
        """
        self._require_ready()
        if query_mode not in ('contains', 'prefix', 'exact'):
            raise ValueError('query_mode must be contains, prefix or exact')
        if type(include_quotes) is not bool:
            raise ValueError('include_quotes must be a boolean')
        if type(include_legacy_records) is not bool:
            raise ValueError('include_legacy_records must be a boolean')
        limit = max(1, min(500, int(limit)))
        token = _uncursor(cursor)
        scope = [query, entity_type, priced] if query_mode == 'contains' else [query, entity_type, priced, query_mode]
        if query and query_mode == 'contains':
            scope.append('name_alias_v1')
        if token and token.get('scope') != scope:
            raise ValueError('cursor belongs to another catalog query')
        generation = self._generation()
        if token and token.get('generation') != generation:
            raise ValueError('catalog changed; restart pagination')
        if token and (not isinstance(token.get('name'), str) or not isinstance(token.get('id'), str)):
            raise ValueError('invalid catalog cursor')
        if priced is not None and type(priced) is not bool:
            raise ValueError('priced must be a boolean')
        where, params = ['1=1'], []
        if entity_type:
            where.append('i.entity_type=?'); params.append(entity_type)
        if query:
            if query_mode == 'exact':
                where.append('i.name COLLATE BINARY=?'); params.append(str(query))
            elif query_mode == 'prefix':
                prefix = str(query)
                where.append('i.name COLLATE BINARY>=?'); params.append(prefix)
                # A lexicographic successor bounds all strings with this prefix.
                # The all-max-codepoint case has no greater nonmatching string.
                for position in range(len(prefix) - 1, -1, -1):
                    if ord(prefix[position]) < 0x10ffff:
                        next_codepoint = ord(prefix[position]) + 1
                        upper = prefix[:position] + chr(0xe000 if next_codepoint == 0xd800 else next_codepoint)
                        where.append('i.name COLLATE BINARY<?'); params.append(upper)
                        break
            else:
                where.append("(i.name LIKE ? ESCAPE '\\' OR EXISTS(SELECT 1 FROM catalog_aliases_v2 a "
                             "WHERE a.item_id=i.item_id AND a.namespace IN ('name','steam:730:market_hash_name') "
                             "AND a.value LIKE ? ESCAPE '\\'))")
                needle = '%' + str(query).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
                params.extend((needle, needle))
        if token:
            where.append('(i.name,i.item_id)>(?,?)')
            params.extend((token['name'], token['id']))
        # A price filter is explicit. Unpriced items remain included by default.
        if priced is not None and self.legacy_connection is not self.connection:
            raise ValueError('priced catalog filtering requires the normal shared connection')
        if priced is not None:
            expression = self._price_expression()
            where.append(expression if priced else 'NOT (' + expression + ')')
        projection = 'i.*'
        if include_quotes and self.legacy_connection is self.connection:
            projection += ',(' + self._price_expression() + ') AS _has_price,(' + self._quote_time_expression() + ') AS _quote_updated_at'
        rows = self._all('SELECT ' + projection + ' FROM catalog_items_v2 i WHERE ' + ' AND '.join(where) +
                         ' ORDER BY i.name,i.item_id LIMIT ?', params + [limit + 1])
        more, rows = len(rows) > limit, rows[:limit]
        if include_quotes and self.legacy_connection is not self.connection:
            self._attach_quote_summary(rows)
        elif not include_quotes:
            for row in rows:
                row.update(_has_price=False, _quotes_omitted=True, _quote_updated_at=None)
        if include_quotes:
            self._attach_current_scope_issues(rows)
        next_cursor = _cursor({'scope': scope, 'name': rows[-1]['name'], 'id': rows[-1]['item_id'],
                               'generation': generation}) if more else None
        metadata_by_item = {}
        if rows:
            ids = [row['item_id'] for row in rows]
            marks = ','.join('?' for _ in ids)
            for metadata in self._all(f'''SELECT m.*,s.last_success_at,s.status FROM catalog_metadata_v2 m
                LEFT JOIN catalog_sources_v2 s ON s.source=m.source WHERE m.item_id IN ({marks}) ORDER BY m.source''', ids):
                metadata_by_item.setdefault(metadata['item_id'], []).append(metadata)
        items = [self._dto(row, metadata_by_item.get(row['item_id'], [])) for row in rows]
        if include_legacy_records:
            for item in items:
                item['legacyMetadataRecords'] = self._legacy_metadata_records(metadata_by_item.get(item['itemId'], []))
        return {'items': items, 'nextCursor': next_cursor,
                'generation': generation, 'includesUnpriced': priced is None,
                'search': {'mode': query_mode, 'caseSensitive': query_mode != 'contains',
                           'indexedNameRange': query_mode != 'contains' or not query,
                           'includesNameAliases': query_mode == 'contains'},
                'quotesIncluded': include_quotes,
                'coverage': self._coverage()}

    def _legacy_metadata_records(self, metadata_rows):
        records = {}
        for metadata_row in sorted(metadata_rows, key=lambda value: value['source']):
            reference = _unjson(metadata_row['raw_ref_json'], {})
            metadata = _unjson(metadata_row['metadata_json'], {})
            if not isinstance(reference, dict) or not isinstance(metadata, dict):
                continue
            store, name = reference.get('store'), reference.get('name')
            if store not in ('item_catalog', 'market_items') or not isinstance(name, str) or not name:
                continue
            source = 'legacy:' + store
            if (metadata_row['source'] not in (source, self._legacy_record_source(source, name))
                    or metadata.get('name') != name):
                continue
            # Metadata rows arrive in source order: exact scoped evidence
            # supersedes its canonical fallback for the same source row.
            records[(store, name)] = {'name': name, 'store': store, 'source': metadata_row['source'],
                'metadata': metadata, 'rawRef': reference, 'updatedAt': metadata_row['updated_at'],
                'active': bool(metadata_row['active'])}
        return [records[key] for key in sorted(records)]

    def iter_compatibility_items(self):
        """Sequential registry projection for the full compatibility feed.

        The caller owns a read transaction spanning this iterator and status().
        Keep compact metadata groups in memory, releasing each after its item;
        no legacy bodies, quote tables, registry aliases or history are read.
        """
        if not self.connection.in_transaction:
            raise RuntimeError('compatibility iteration requires an open read transaction')
        self._require_ready()
        sources = {row['source']: row for row in self._all(
            'SELECT source,last_success_at,status FROM catalog_sources_v2')}

        def rows(sql):
            cursor = self.connection.execute(sql)
            names = [column[0] for column in cursor.description]
            try:
                for values in cursor:
                    yield dict(zip(names, values))
            finally:
                close = getattr(cursor, 'close', None)
                if close is not None:
                    close()

        metadata_by_item = {}
        for metadata in rows('SELECT item_id,source,metadata_json,raw_ref_json,active,updated_at '
                             'FROM catalog_metadata_v2 NOT INDEXED'):
            source = sources.get(metadata['source'], {})
            metadata.update(last_success_at=source.get('last_success_at'), status=source.get('status'))
            metadata_by_item.setdefault(metadata['item_id'], []).append(metadata)
        for row in rows('SELECT item_id,name,entity_type,priceable,kind,market_hash_name,updated_at '
                        'FROM catalog_items_v2 NOT INDEXED'):
            row.update(_has_price=False, _quotes_omitted=True, _quote_updated_at=None)
            metadata = metadata_by_item.pop(row['item_id'], [])
            item = self._dto(row, metadata)
            item['legacyMetadataRecords'] = self._legacy_metadata_records(metadata)
            yield item

    @staticmethod
    def _current_scope_sql(store, alias='b'):
        """Nonphysical dimensions need a verified decision, never broadening."""
        return ' AND '.join(alias + '.' + key + "=''" for key in ('provider', 'lane', 'source')
                            if key not in LEGACY_CURRENT_PREFIXES[store])

    @staticmethod
    def _current_scope_supported(binding):
        return not any(binding[key] for key in ('provider', 'lane', 'source')
                       if key not in LEGACY_CURRENT_PREFIXES[binding['store']])

    def _attach_current_scope_issues(self, rows):
        """One indexed per-page registry query exposes ignored binding scopes."""
        if not rows:
            return
        by_id = {row['item_id']: row for row in rows}
        clauses = ["(b.store='" + store + "' AND NOT (" + self._current_scope_sql(store) + '))'
                   for store in LEGACY_CURRENT_PREFIXES]
        issues = self._all('SELECT b.item_id,b.store,COUNT(*) AS n FROM catalog_legacy_bindings_v2 b '
            'WHERE b.item_id IN (' + ','.join('?' for _ in by_id) + ') AND (' +
            ' OR '.join(clauses) + ') GROUP BY b.item_id,b.store', list(by_id))
        for row in rows:
            row['_quote_scope_issues'] = []
        for issue in issues:
            by_id[issue['item_id']]['_quote_scope_issues'].append({
                'store': issue['store'], 'count': issue['n'],
                'reason': 'nonphysical_binding_dimensions_require_verified_decision'})

    def _attach_quote_summary(self, rows):
        """Batch keys across separate connections without copying raw bodies.

        Shared-connection reads retain the cheaper existing scalar expressions;
        a small synthetic fixture did not justify replacing that query shape.
        """
        by_id = {row['item_id']: row for row in rows}
        for row in rows:
            row.update(_has_price=False, _quote_updated_at=None)
        if not rows:
            return
        ids = list(by_id)
        binding_sql = ('SELECT item_id,store,legacy_name,provider,lane,source '
            'FROM catalog_legacy_bindings_v2 WHERE item_id IN (' + ','.join('?' for _ in ids) +
            ') AND store IN (' + ','.join('?' for _ in LEGACY_CURRENT_PREFIXES) + ') LIMIT 8193')
        parameters = ids + list(LEGACY_CURRENT_PREFIXES)
        bindings = self._all(binding_sql, parameters)
        active_stores = {value['store'] for value in bindings}
        if len(bindings) > 8192:
            raise LegacyAccessError('binding_budget', {'maximumBindings': 8192})
        for store, keys in LEGACY_CURRENT_PREFIXES.items():
            if store not in active_stores or not self._has_table(store, legacy=True):
                continue
            group = [binding for binding in bindings if binding['store'] == store and
                     self._current_scope_supported(binding)]
            dimension = keys[1]
            found = []
            for offset in range(0, len(group), 100):
                chunk = group[offset:offset + 100]
                parameters = [value for binding in chunk for value in
                              (binding['item_id'], binding['legacy_name'], binding[dimension])]
                # CROSS JOIN fixes the small supplied key set as outer loop;
                # the existing (name, dimension) key seeks each legacy name.
                found.extend(_rows(self.legacy_connection,
                    'WITH b(item_id,name,dimension) AS (VALUES ' + ','.join('(?,?,?)' for _ in chunk) +
                    ') SELECT b.item_id,MAX(p.price IS NOT NULL) AS has_price,'
                    'MAX(CASE WHEN p.price IS NOT NULL THEN p.updated_at END) AS updated_at '
                    'FROM b CROSS JOIN ' + store + ' p WHERE p.name=b.name '
                    "AND (b.dimension='' OR p." + dimension + '=b.dimension) GROUP BY b.item_id', parameters))
            for value in found:
                row = by_id[value['item_id']]
                row['_has_price'] = row['_has_price'] or bool(value['has_price'])
                updated = value['updated_at']
                if updated is not None and (row['_quote_updated_at'] is None or updated > row['_quote_updated_at']):
                    row['_quote_updated_at'] = updated

    def _price_expression(self):
        if self.legacy_connection is not self.connection:
            return '0'
        expressions = []
        for store, keys in LEGACY_CURRENT_PREFIXES.items():
            column = keys[1]
            if self._has_table(store, legacy=True):
                expressions.append(f'''EXISTS(SELECT 1 FROM catalog_legacy_bindings_v2 b JOIN {store} p
                    ON p.name=b.legacy_name AND (b.{column}='' OR b.{column}=p.{column})
                    WHERE b.item_id=i.item_id AND b.store='{store}' AND {self._current_scope_sql(store)}
                    AND p.price IS NOT NULL)''')
        return '(' + ' OR '.join(expressions) + ')' if expressions else '0'

    def _quote_time_expression(self):
        if self.legacy_connection is not self.connection:
            return 'NULL'
        expressions = []
        for store, keys in LEGACY_CURRENT_PREFIXES.items():
            column = keys[1]
            if self._has_table(store, legacy=True):
                expressions.append(f'''COALESCE((SELECT MAX(p.updated_at) FROM catalog_legacy_bindings_v2 b JOIN {store} p
                    ON p.name=b.legacy_name AND (b.{column}='' OR b.{column}=p.{column})
                    WHERE b.item_id=i.item_id AND b.store='{store}' AND {self._current_scope_sql(store)}
                    AND p.price IS NOT NULL),0)''')
        return 'NULLIF(MAX(0,' + ','.join(expressions) + '),0)' if expressions else 'NULL'

    def _bindings(self, item_id, stores, maximum=None):
        marks = ','.join('?' for _ in stores)
        sql = f'SELECT * FROM catalog_legacy_bindings_v2 WHERE item_id=? AND store IN ({marks}) ORDER BY store,legacy_name,provider,lane,source'
        rows = self._all(sql + (' LIMIT ?' if maximum is not None else ''),
                         [item_id] + list(stores) + ([maximum + 1] if maximum is not None else []))
        if maximum is not None and len(rows) > maximum:
            raise LegacyAccessError('binding_budget', {'maximumBindings': maximum})
        return rows

    def _quote_state(self, item_id):
        if self.legacy_connection is self.connection:
            result = self._one('SELECT (' + self._price_expression() + ') AS has_price '
                               'FROM catalog_items_v2 i WHERE item_id=?', (item_id,))
            return 'priced' if result and result['has_price'] else 'unpriced'
        row = {'item_id': item_id}
        self._attach_quote_summary([row])
        return 'priced' if row['_has_price'] else 'unpriced'

    def _store_payload(self, raw):
        if not isinstance(raw, (str, bytes, bytearray, memoryview)):
            raw = _json(raw)
        data = raw.encode() if isinstance(raw, str) else bytes(raw)
        payload_id = hashlib.sha256(data).hexdigest()
        self.connection.execute('INSERT OR IGNORE INTO catalog_payloads_v2 VALUES(?,?,?)',
                                (payload_id, data, 'application/json' if isinstance(raw, str) else 'application/octet-stream'))
        return payload_id

    def get_evidence(self, item_id, source=None):
        self._require_ready()
        if source is None:
            return {'itemId': item_id, 'sources': [
                {'source': row['source'], 'rawRef': _unjson(row['raw_ref_json'])}
                for row in self._all('SELECT source,raw_ref_json FROM catalog_metadata_v2 WHERE item_id=? ORDER BY source', (item_id,))]}
        row = self._one('SELECT raw_ref_json FROM catalog_metadata_v2 WHERE item_id=? AND source=?', (item_id, source))
        if not row:
            return None
        reference = _unjson(row['raw_ref_json'])
        result = {'itemId': item_id, 'source': source, 'rawRef': reference, 'raw': None}
        if not reference:
            return result
        if reference.get('storage') == 'catalog_payload':
            result['referenceSemantics'] = 'immutable_content_addressed'
            payload = self._one('SELECT data,media_type FROM catalog_payloads_v2 WHERE payload_id=?', (reference.get('payloadId'),))
            if payload:
                result['mediaType'] = payload['media_type']
                try:
                    result['raw'] = bytes(payload['data']).decode('utf-8')
                except UnicodeError:
                    result['dataBase64'] = base64.b64encode(payload['data']).decode()
        elif reference.get('store') in ('item_catalog', 'market_items') and self._has_table(reference['store'], legacy=True):
            result['referenceSemantics'] = 'mutable_legacy_current_cache'
            records = _rows(self.legacy_connection, 'SELECT raw FROM ' + reference['store'] + ' WHERE name=? LIMIT 1',
                            (reference.get('name'),))
            if records:
                result['raw'] = records[0]['raw']
                result['mediaType'] = 'application/json'
        return result

    def get_payload(self, payload_id):
        """Fetch one explicitly addressed preserved payload, never a bulk read."""
        self._require_ready()
        if not isinstance(payload_id, str) or not re.match(r'^[0-9a-f]{64}$', payload_id):
            raise ValueError('content-addressed payload ID required')
        payload = self._one('SELECT data,media_type FROM catalog_payloads_v2 WHERE payload_id=?', (payload_id,))
        if not payload:
            return None
        data = bytes(payload['data'])
        try:
            text, encoding = data.decode('utf-8'), 'utf-8'
        except UnicodeError:
            text, encoding = base64.b64encode(data).decode(), 'base64'
        return {'payloadId': payload_id, 'mediaType': payload['media_type'],
                'encoding': encoding, 'data': text, 'bytes': len(data),
                'referenceSemantics': 'immutable_content_addressed'}

    def _current_projection(self, store, budget=None):
        if store not in self._legacy_projection_cache:
            sql = 'PRAGMA table_info(' + _quoted(store) + ')'
            columns = (budget.query(self.legacy_connection, sql) if budget else
                       _rows(self.legacy_connection, sql))
            names = {column['name'] for column in columns}
            required = set(LEGACY_CURRENT_PREFIXES[store]) | {'price', 'updated_at'}
            if not required <= names:
                raise LegacyAccessError('type_conflict', {'store': store, 'missingColumns': sorted(required - names)})
            self._legacy_projection_cache[store] = ','.join(_quoted(field) for field in CURRENT_QUOTE_FIELDS if field in names)
        return self._legacy_projection_cache[store]

    def _cash_query(self, connection, sql, parameters=(), maximum=1, budget=None):
        return (budget.query(connection, sql, parameters, maximum) if budget else
                _rows(connection, sql, parameters))

    def _cash_listing_identity(self, item_id, name, identity=None, budget=None):
        if not item_id:
            return False
        if identity is None:
            rows = self._cash_query(self.connection,
                'SELECT entity_type,priceable,market_hash_name FROM catalog_items_v2 WHERE item_id=? LIMIT 1',
                (item_id,), budget=budget)
            identity = rows[0] if rows else {}
        return (identity.get('entity_type', identity.get('entityType')) == 'listing'
                and identity.get('priceable') == 1
                and identity.get('market_hash_name', identity.get('marketHashName')) == name)

    def _cash_read_rows(self, store, name, observed=None, budget=None):
        # Only these two known cash stores/physical keys may hydrate cash raw.
        if store not in ('market_item_prices', 'price_observations'):
            raise ValueError('dedicated cash evidence requires a supported physical store')
        fields = 'name,provider,price,median,volume,source,observed_at,updated_at'
        where, parameters = 'name=? AND provider=?', [name, 'skinport_live']
        maximum = 1
        if store == 'market_item_prices':
            fields += ',basis,kind,solid_price,lowest_sell_order,highest_buy_order,spread_pct,stability_pct,confidence_pct,url'
        else:
            if not observed or len(observed) > 100 or any(type(value) is not int for value in observed):
                raise ValueError('cash evidence requires 1..100 exact observation seconds')
            maximum = len(observed)
            where += ' AND observed_at IN (' + ','.join('?' for _ in observed) + ')'
            parameters.extend(observed)
        # At most 4KiB of TEXT raw per exact row crosses the SQLite boundary.
        # Oversized/non-TEXT evidence is reported without returning its body.
        fields += ",typeof(raw) AS _cash_raw_type,length(CAST(raw AS BLOB)) AS _cash_raw_bytes"
        fields += ",CASE WHEN typeof(raw)='text' AND length(CAST(raw AS BLOB))<=4096 THEN raw END AS _cash_raw"
        return self._cash_query(self.legacy_connection, 'SELECT ' + fields + ' FROM ' + store +
            ' WHERE ' + where + ' LIMIT ?', parameters + [maximum + 1], maximum + 1, budget)

    @staticmethod
    def _cash_raw_fields(row, current=False):
        if (row.get('_cash_raw_type') != 'text' or type(row.get('_cash_raw_bytes')) is not int
                or not 0 < row['_cash_raw_bytes'] <= 4096 or not isinstance(row.get('_cash_raw'), str)
                or len(row['_cash_raw'].encode('utf-8')) != row['_cash_raw_bytes']):
            return None, 'cash_raw_not_bounded_text'
        try:
            import cash_prices as cash_contract  # Pure import; no worker or transport is created.
        except ImportError:
            return None, 'cash_contract_unavailable'
        if (cash_contract.PROVIDER, cash_contract.SOURCE, cash_contract.CURRENCY, cash_contract.BASIS,
                cash_contract.REQUEST_URL) != ('skinport_live', 'skinport:items:730:USD:tradable0',
                    'USD', 'lowest_listed_price',
                    'https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0'):
            return None, 'cash_contract_mismatch'
        try:
            saved = dict(row, raw=row['_cash_raw'])
            raw = cash_contract._saved_raw(saved)
            cash_contract._json(raw)  # Reject non-finite JSON extensions, including unused fields.
            if (type(raw.get('contractVersion')) is not int or raw['contractVersion'] != 1
                    or row.get('provider') != cash_contract.PROVIDER or row.get('source') != cash_contract.SOURCE
                    or type(row.get('observed_at')) is not int or row['observed_at'] != row.get('updated_at')
                    or type(row.get('updated_at')) is not int
                    or raw.get('providerTimeSemantics') != 'upstream_updated_at_not_claimed_trade_time'
                    or type(raw.get('appId')) is not int or raw['appId'] != 730
                    or type(raw.get('tradable')) is not int or raw['tradable'] != 0
                    or (raw.get('providerUpdatedAt') is not None
                        and cash_contract._valid_time(raw['providerUpdatedAt']) is None)
                    or row.get('median') is not None or row.get('volume') is not None):
                raise ValueError('invalid dedicated cash facts')
            if raw['state'] in ('priced', 'no_listings'):
                if raw['state'] == 'priced' and not isinstance(raw.get('priceText'), str):
                    raise ValueError('cash price text must retain its saved decimal string')
                normalized = cash_contract.normalize_outcome(row['name'], 1, [{
                    'quantity': raw.get('quantityListed'), 'min_price': raw.get('priceText'),
                    'updated_at': raw.get('providerUpdatedAt')}])
                if (cash_contract._semantic(normalized) != cash_contract._semantic(raw)
                        or normalized['reason'] != raw.get('reason')):
                    raise ValueError('cash normalized facts disagree')
            elif raw['state'] == 'missing':
                if (raw.get('priceText') is not None or raw.get('quantityListed') is not None
                        or raw.get('providerUpdatedAt') is not None
                        or raw.get('reason') != 'not_in_successful_snapshot'):
                    raise ValueError('invalid missing cash facts')
            elif raw['state'] == 'ambiguous':
                if (raw.get('priceText') is not None or raw.get('quantityListed') is not None
                        or raw.get('providerUpdatedAt') is not None
                        or type(raw.get('matchCount')) is not int or not 2 <= raw['matchCount'] <= 150000
                        or raw.get('reason') != 'duplicate_exact_hash'):
                    raise ValueError('invalid ambiguous cash facts')
            elif (raw.get('priceText') is not None
                    or (raw.get('quantityListed') is not None and (
                        type(raw['quantityListed']) is not int or raw['quantityListed'] <= 0))
                    or raw.get('reason') not in ('invalid_listing_quantity',
                        'missing_zero_or_invalid_min_price', 'subcent_usd_min_price')):
                raise ValueError('invalid unavailable cash facts')
            if current and (row.get('basis') != cash_contract.BASIS
                    or row.get('kind') != 'cash_listing_reference'
                    or row.get('solid_price') != row['price'] or row.get('lowest_sell_order') != row['price']
                    or any(row.get(key) is not None for key in ('highest_buy_order', 'spread_pct',
                        'stability_pct', 'confidence_pct', 'url'))):
                raise ValueError('invalid dedicated current cash fields')
        except (cash_contract.CashError, ValueError, TypeError, KeyError, OverflowError):
            return None, 'cash_raw_contract_mismatch'
        return {'marketHashName': raw['marketHashName'], 'provider': raw['provider'], 'source': raw['source'],
                'currency': raw['currency'], 'currencyStatus': 'verified_cash_evidence',
                'basis': raw['basis'], 'cashState': raw['state'], 'cashReason': raw.get('reason'),
                'priceText': raw.get('priceText'), 'quantityListed': raw.get('quantityListed'),
                'capturedAt': row['updated_at'], 'receivedAtUs': raw['receivedAtUs'],
                'providerUpdatedAt': raw.get('providerUpdatedAt'),
                'providerTimeSemantics': raw['providerTimeSemantics'],
                'receiptBodySHA256': raw['bodySHA256'],
                'quoteRawSHA256': hashlib.sha256(row['_cash_raw'].encode('utf-8')).hexdigest(),
                'cashEvidenceStatus': 'verified'}, None

    @staticmethod
    def _cash_receipt_matches(receipt, fields, name, item_id):
        if not isinstance(receipt, dict):
            return False
        expected = {
            'contractVersion': 1, 'status': 'resolved', 'reason': None,
            'identityStatus': 'verified_listing', 'itemId': item_id, 'marketHashName': name,
            'provider': 'skinport_live', 'source': 'skinport:items:730:USD:tradable0',
            'currency': 'USD', 'basis': 'lowest_listed_price',
            'currentStore': 'market_item_prices', 'currentWriteDisposition': 'upserted_current',
            'currentValueDisposition': 'priced' if fields['cashState'] == 'priced' else 'explicit_unavailable',
            'historyStore': 'price_observations',
            'legacyStreamBindings': {'market_item_prices': 'bound', 'price_observations': 'bound'},
            'pendingLegacyStreamCount': 0,
            **{key: fields[key] for key in ('cashState', 'cashReason', 'capturedAt', 'receivedAtUs',
                'providerUpdatedAt', 'providerTimeSemantics', 'quoteRawSHA256')},
            'bodySHA256': fields['receiptBodySHA256'],
        }
        observed = receipt.get('meaningfulObservedAt')
        return (all(receipt.get(key) == value for key, value in expected.items())
                and all(type(receipt.get(key)) is int for key in
                        ('contractVersion', 'capturedAt', 'receivedAtUs', 'pendingLegacyStreamCount'))
                and (receipt.get('providerUpdatedAt') is None or type(receipt['providerUpdatedAt']) is int)
                and (observed is None or (type(observed) is int and observed == fields['capturedAt']))
                and receipt.get('historyDisposition') == (
                    'meaningful_outcome_recorded' if observed is not None else 'unchanged_outcome_no_history'))

    def _cash_current_quote_dto(self, store, row, item_id, budget=None, identity=None, ownership=None):
        result = self._current_quote_dto(store, row, item_id)
        if store != 'market_item_prices' or row.get('provider') != 'skinport_live':
            return result
        result.update(currency=None, currencyStatus='unverified_cash_evidence', cashEvidenceStatus='unverified')
        result['cashEvidenceIssue'] = 'cash_native_listing_or_ownership_unverified'
        name = row['name']
        if not self._cash_listing_identity(item_id, name, identity, budget):
            return result
        ownership = ownership if ownership is not None else self._locator_owners(
            {'store': store, 'name': name, 'provider': 'skinport_live'}, budget)
        if (ownership.get('status') != 'owned' or ownership.get('owners') != [item_id]
                or ownership.get('entityType') != 'listing'):
            return result
        result['cashEvidenceIssue'] = 'cash_typed_scope_mismatch'
        if (row.get('source') != 'skinport:items:730:USD:tradable0'
                or row.get('basis') != 'lowest_listed_price'
                or any(row.get(key) not in (None, 'USD') for key in ('currency', 'currency_code'))):
            return result
        key = 'catalog_cash_binding:v1:' + hashlib.sha256(json.dumps(
            ['skinport_live', 'skinport:items:730:USD:tradable0', name],
            ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()
        receipts = self._cash_query(self.connection,
            "SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=4096 THEN value END AS value "
            'FROM catalog_store_meta WHERE key=? LIMIT 1', (key,), budget=budget)
        receipt = _unjson(receipts[0]['value']) if receipts else None
        result['cashEvidenceIssue'] = 'cash_binding_evidence_missing_or_unresolved'
        if (not isinstance(receipt, dict) or receipt.get('status') != 'resolved'
                or receipt.get('itemId') != item_id or receipt.get('marketHashName') != name):
            return result
        saved = self._cash_read_rows(store, name, budget=budget)
        result['cashEvidenceIssue'] = 'cash_current_row_changed_or_missing'
        compared = ('name', 'provider', 'price', 'median', 'volume', 'source', 'observed_at', 'updated_at',
                    'basis', 'kind', 'solid_price', 'lowest_sell_order', 'highest_buy_order',
                    'spread_pct', 'stability_pct', 'confidence_pct', 'url')
        if len(saved) != 1 or any(saved[0].get(key) != row.get(key) for key in compared):
            return result
        fields, issue = self._cash_raw_fields(saved[0], current=True)
        result['cashEvidenceIssue'] = issue or 'cash_binding_evidence_mismatch'
        if fields is None or not self._cash_receipt_matches(receipt, fields, name, item_id):
            return result
        result.update(fields, cashEvidenceSource='current_binding_receipt_and_row_raw')
        result.pop('cashEvidenceIssue', None)
        return result

    def _cash_history_points(self, points, item_id, budget=None, identity=None, ownership=None):
        grouped = {}
        for point in points:
            if ((point.get('origin') or {}).get('storage') != 'legacy'
                    or (point.get('origin') or {}).get('store') != 'price_observations'
                    or point.get('provider') != 'skinport_live'):
                continue
            point.update(currency=None, currencyStatus='unverified_cash_evidence', cashEvidenceStatus='unverified',
                         cashEvidenceIssue='cash_native_listing_or_ownership_unverified')
            name = point['origin']['name']
            grouped.setdefault(name, []).append(point)
        for name, group in grouped.items():
            if not self._cash_listing_identity(item_id, name, identity, budget):
                continue
            owners = ownership if ownership is not None else self._locator_owners(
                {'store': 'price_observations', 'name': name, 'provider': 'skinport_live',
                 'source': 'skinport:items:730:USD:tradable0'}, budget)
            if (owners.get('status') != 'owned' or owners.get('owners') != [item_id]
                    or owners.get('entityType') != 'listing'):
                continue
            addressed = {}
            for point in group:
                point['cashEvidenceIssue'] = 'cash_history_key_or_scope_mismatch'
                if (type(point.get('ts')) is not int or point.get('pointKey') != point['ts']
                        or point.get('updatedAt') != point['ts']
                        or point.get('source') != 'skinport:items:730:USD:tradable0'):
                    continue
                addressed.setdefault(point['ts'], []).append(point)
            seconds = list(addressed)
            for start in range(0, len(seconds), 100):
                chunk = seconds[start:start + 100]
                saved = {}
                for row in self._cash_read_rows('price_observations', name, chunk, budget):
                    observed = row['observed_at']
                    saved[observed] = None if observed in saved else row
                for observed in chunk:
                    row = saved.get(observed)
                    for point in addressed[observed]:
                        point['cashEvidenceIssue'] = 'cash_history_row_changed_or_missing'
                        if row is None or any(row.get(column) != point.get(field) for column, field in (
                                ('price', 'price'), ('median', 'median'), ('volume', 'volume'),
                                ('source', 'source'), ('observed_at', 'ts'), ('updated_at', 'updatedAt'))):
                            continue
                        fields, issue = self._cash_raw_fields(row)
                        point['cashEvidenceIssue'] = issue
                        if fields is None:
                            continue
                        point.update(fields, cashEvidenceSource='own_observation_row_raw', grain='exact_capture_second')
                        point.pop('cashEvidenceIssue', None)


    @staticmethod
    def _current_quote_dto(store, row, item_id):
        locator = {'store': store, **{key: row[key] for key in LEGACY_CURRENT_PREFIXES[store]}}
        return {**row, 'itemId': item_id, 'updatedAt': row.get('updated_at'),
                'observedAt': row.get('observed_at'),
                'currency': row.get('currency') or row.get('currency_code'),
                'currencyStatus': 'stored' if row.get('currency') or row.get('currency_code') else 'not_stored',
                'origin': {'storage': 'legacy', **locator},
                'rawRef': locator, 'rawReferenceSemantics': 'mutable_legacy_current_cache'}

    def current_quotes(self, item_id, max_quotes=2000):
        self._require_ready()
        if type(max_quotes) is not int or not 1 <= max_quotes <= 2000:
            raise ValueError('max_quotes must be 1..2000')
        item = self._one('SELECT entity_type,priceable,market_hash_name FROM catalog_items_v2 WHERE item_id=?', (item_id,))
        if item and item['entity_type'] != 'legacy_reference' and not item['priceable']:
            return {'itemId': item_id, 'quotes': [], 'quoteState': 'not_priceable',
                    'quoteCoverage': {'complete': True, 'ignoredScopedBindings': []},
                    'coverage': self._coverage()}
        quotes, seen, scope_issues = [], set(), []
        for binding in self._bindings(item_id, tuple(LEGACY_CURRENT_PREFIXES), maximum=128):
            store = binding['store']
            if not self._current_scope_supported(binding):
                scope_issues.append({'store': store, 'name': binding['legacy_name'],
                    'provider': binding['provider'], 'lane': binding['lane'], 'source': binding['source'],
                    'reason': 'nonphysical_binding_dimensions_require_verified_decision'})
                continue
            if not self._has_table(store, legacy=True):
                continue
            where, parameters = ['name=?'], [binding['legacy_name']]
            for key in LEGACY_CURRENT_PREFIXES[store][1:]:
                if binding[key]:
                    where.append(key + '=?'); parameters.append(binding[key])
            records = _rows(self.legacy_connection, 'SELECT ' + self._current_projection(store) +
                ' FROM ' + store + ' WHERE ' + ' AND '.join(where) + ' ORDER BY ' +
                ','.join(LEGACY_CURRENT_PREFIXES[store]) + ' LIMIT ?', parameters + [max_quotes + 1])
            for row in records:
                origin = (store,) + tuple(row[key] for key in LEGACY_CURRENT_PREFIXES[store])
                if origin in seen:
                    continue
                seen.add(origin)
                if len(quotes) >= max_quotes:
                    raise LegacyAccessError('quote_budget', {'maximumQuotes': max_quotes})
                quotes.append(self._cash_current_quote_dto(store, row, item_id, identity=item))
        return {'itemId': item_id, 'quotes': quotes,
                'quoteState': 'priced' if any(row.get('price') is not None for row in quotes) else
                              'scope_required' if scope_issues else 'unpriced',
                'quoteCoverage': {'complete': not scope_issues, 'ignoredScopedBindings': scope_issues},
                'coverage': self._coverage()}

    @staticmethod
    def _coverage():
        return {'currentCatalogRegistry': True, 'legacyHistory': 'verified_catalog_bindings_and_explicit_physical_references',
                'legacyHistoryNamesFullyEnumerated': False,
                'legacyHistoryCopied': False, 'lostLegacyIntradayPointsRecoverable': False,
                'historicalOnlyReferences': 'bounded physical-stream discovery or pure exact lookup; not inferred Steam identity',
                'historicalDiscoveryWindow': 'observed passes; inserts before cursor require a new pass',
                'currentQuoteOnlyDiscovery': {'capable': True, 'status': 'explicit_pass_required',
                    'registration': 'register_legacy_current_batch; not included in CLI history stages',
                    'fullyEnumerated': False, 'stores': list(LEGACY_CURRENT_PREFIXES)},
                'pendingLegacyLinks': 'explicit verification required; history reference IDs keep ownership',
                'privateImportDiscovery': 'not performed by historical stream stages'}

    def status(self):
        if not self.ready():
            return {'available': False, 'schemaVersion': 0, 'generation': 0, 'coverage': self._coverage()}
        counts = self._all('SELECT entity_type,COUNT(*) AS n FROM catalog_items_v2 GROUP BY entity_type')
        listings = sum(row['n'] for row in counts if row['entity_type'] == 'listing')
        legacy_references = sum(row['n'] for row in counts if row['entity_type'] == 'legacy_reference')
        total = sum(row['n'] for row in counts)
        return {'available': True, 'schemaVersion': SCHEMA_VERSION, 'generation': self._generation(),
                'items': total, 'listings': listings, 'metadataEntities': total - listings - legacy_references,
                'legacyReferences': legacy_references,
                'sources': [{'source': row['source'], 'lastAttemptAt': row['last_attempt_at'],
                             'lastSuccessAt': row['last_success_at'], 'status': row['status'],
                             'error': row['error'], 'generation': row['generation']}
                            for row in self._all('SELECT * FROM catalog_sources_v2 ORDER BY source')],
                'migration': self._all('SELECT * FROM catalog_migration_v2 ORDER BY source'),
                'historicalPasses': self._all('SELECT * FROM catalog_history_coverage_v2 ORDER BY store'),
                'coverage': self._coverage()}

    def save_point(self, item_id, provider, point_key, point_ts, price, volume=None,
                   raw_time=None, grain='unknown', source='steam_history', fetched_at=None,
                   raw_ref=None, raw=None):
        self._require_ready()
        self._begin_write()
        if not self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=? AND entity_type=\'listing\'', (item_id,)):
            raise ValueError('exact history requires a known listing ID')
        if not provider or not point_key or not source or not grain:
            raise ValueError('provider, exact point key, source and granularity required')
        if point_ts is not None:
            if isinstance(point_ts, bool) or int(point_ts) != point_ts:
                raise ValueError('point timestamp must be exact integer seconds or None')
            point_ts = int(point_ts)
        fetched_at = int(time.time() if fetched_at is None else fetched_at)
        if price is not None:
            price = float(price)
            if not math.isfinite(price):
                raise ValueError('finite price required')
        payload_id = None
        if raw is not None:
            payload_id = self._store_payload(raw)
        fingerprint = _json([item_id, provider, str(point_key), point_ts, raw_time, grain,
                             price, volume, source, raw_ref, payload_id])
        revision_id = hashlib.sha256(fingerprint.encode()).hexdigest()
        self.connection.execute('''INSERT INTO catalog_history_revisions_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(revision_id) DO UPDATE SET first_fetched_at=MIN(first_fetched_at,excluded.first_fetched_at),
            last_fetched_at=MAX(last_fetched_at,excluded.last_fetched_at)''',
            (revision_id, item_id, provider, str(point_key), point_ts, raw_time, grain, price, volume, source,
             _json(raw_ref) if raw_ref is not None else None, payload_id, fetched_at, fetched_at))
        self.connection.execute('''INSERT INTO catalog_history_points_v2 VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(item_id,provider,point_key) DO UPDATE SET
            point_ts=CASE WHEN excluded.last_fetched_at>=last_fetched_at THEN excluded.point_ts ELSE point_ts END,
            revision_id=CASE WHEN excluded.last_fetched_at>=last_fetched_at THEN excluded.revision_id ELSE revision_id END,
            first_fetched_at=MIN(first_fetched_at,excluded.first_fetched_at),
            last_fetched_at=MAX(last_fetched_at,excluded.last_fetched_at)''',
            (item_id, provider, str(point_key), point_ts, revision_id, fetched_at, fetched_at))
        return revision_id

    def save_steam_points(self, item_id, points, fetched_at, provider='steam', source='steam_history', grain='unknown'):
        count = 0
        for point in points:
            ts = point.get('ts', point.get('point_ts'))
            if ts is not None:
                # Exact timestamps take precedence over the legacy daily time label.
                key = 'ts:' + str(int(ts))
            else:
                exact_source_key = point.get('point_key') or point.get('rawTime') or point.get('raw_time') or point.get('time')
                if exact_source_key is None:
                    raise ValueError('unparsed history point requires an exact source key')
                key = 'source:' + str(exact_source_key)
            self.save_point(item_id, provider, key, ts, point.get('price'), volume=point.get('volume'),
                            raw_time=point.get('rawTime', point.get('raw_time')),
                            grain=point.get('grain', point.get('granularity', grain)), source=point.get('source') or source,
                            fetched_at=fetched_at, raw_ref=point.get('rawRef'), raw=point.get('raw'))
            count += 1
        return count

    def get_revisions(self, item_id, point_key, provider='steam', limit=100, cursor=None):
        self._require_ready()
        limit = max(1, min(500, int(limit)))
        scope = [item_id, provider, str(point_key)]
        token = _uncursor(cursor)
        if token and (token.get('scope') != scope or not isinstance(token.get('revision'), str)):
            raise ValueError('invalid revision cursor')
        rows = self._all('''SELECT * FROM catalog_history_revisions_v2
            WHERE item_id=? AND provider=? AND point_key=? AND revision_id>?
            ORDER BY revision_id LIMIT ?''', scope + [token['revision'] if token else '', limit + 1])
        more, rows = len(rows) > limit, rows[:limit]
        return {'itemId': item_id, 'provider': provider, 'pointKey': str(point_key),
                'revisions': [{'revisionId': row['revision_id'], 'ts': row['point_ts'],
                               'rawTime': row['raw_time'], 'grain': row['grain'],
                               'price': row['price'], 'volume': row['volume'], 'source': row['source'],
                               'firstFetchedAt': row['first_fetched_at'], 'lastFetchedAt': row['last_fetched_at'],
                               'rawRef': _unjson(row['raw_ref_json']), 'payloadId': row['payload_id']}
                              for row in rows],
                'nextCursor': _cursor({'scope': scope, 'revision': rows[-1]['revision_id']}) if more else None,
                'order': 'stable_revision_id'}

    def _streams(self, item_id, kind, provider, lane, source, budget=None):
        budget = budget if budget is not None else _LegacyBudget(400, 1)
        streams = []
        if kind == 'steam':
            if provider is not None:
                providers = [provider]
            else:
                discovered = self._discover_prefixes('catalog_history_points_v2', ('item_id', 'provider'),
                    None, MAX_HISTORY_STREAMS + 1, budget, 8192, fixed=(item_id,), connection=self.connection)
                if not discovered['done']:
                    raise LegacyAccessError('provider_discovery_incomplete', discovered)
                providers = [value['provider'] for value in discovered['streams']]
            for value in providers:
                streams.append({'id': _json(['canonical', value]), 'storage': 'canonical',
                                'table': 'catalog_history_points_v2', 'provider': value,
                                'ts': 'p.point_ts', 'key': 'p.point_key',
                                'where': ['p.item_id=?', 'p.provider=?'], 'params': [item_id, value]})
            stores = ('price_history',)
        elif kind == 'observations':
            stores = ('price_observations',)
        elif kind in ('shared_history', 'shared_observations'):
            if kind == 'shared_observations' and source:
                raise ValueError('shared observations have lane provenance, not a source column')
            if not lane or (kind == 'shared_history' and not source):
                raise ValueError('shared history requires explicit lane and source prefixes')
            stores = ('shared_price_history',) if kind == 'shared_history' else ('shared_price_observations',)
        else:
            raise ValueError('unknown history kind')
        seen = set()
        for binding in self._bindings(item_id, stores, maximum=MAX_HISTORY_STREAMS):
            table, name = binding['store'], binding['legacy_name']
            if not self._has_table(table, legacy=True):
                continue
            if provider is not None:
                values = [provider]
            elif binding['provider']:
                values = [binding['provider']]
            elif not kind.startswith('shared_'):
                discovered = self._discover_prefixes(table, LEGACY_HISTORY_PREFIXES[table], None,
                    MAX_HISTORY_STREAMS + 1, budget, 8192, fixed=(name,))
                if not discovered['done']:
                    raise LegacyAccessError('provider_discovery_incomplete', discovered)
                values = [value['provider'] for value in discovered['streams']]
            else:
                values = [None]
            if kind.startswith('shared_'):
                values = [None]
            for value in values:
                if binding['provider'] and value != binding['provider']:
                    continue
                if binding['lane'] and binding['lane'] != lane:
                    continue
                if binding['source'] and source and binding['source'] != source:
                    continue
                effective_source = source or binding['source'] or None
                if kind == 'shared_observations' and effective_source:
                    raise ValueError('shared observations cannot use source-scoped legacy bindings')
                identity = (table, name, value, lane, effective_source)
                if identity in seen or (table, name, value, lane, None) in seen:
                    continue
                seen.add(identity)
                where, parameters = ['name=?'], [name]
                if value is not None:
                    where.append('provider=?'); parameters.append(value)
                if kind.startswith('shared_'):
                    where.append('lane=?'); parameters.append(lane)
                if effective_source:
                    if table == 'price_history':
                        if effective_source != 'steam_history':
                            continue
                    else:
                        where.append('source=?'); parameters.append(effective_source)
                ts = 'observed_at' if kind in ('observations', 'shared_observations') else 'point_ts'
                key = 'rowid' if kind == 'steam' else ('point_ts' if kind == 'shared_history' else 'observed_at')
                streams.append({'id': _json(identity), 'storage': 'legacy', 'table': table,
                                'name': name, 'provider': value, 'where': where, 'params': parameters,
                                'ts': ts, 'key': key, 'lane': lane, 'sourceScope': effective_source})
        # Normalize overlapping scopes before reads. A source-wide binding can
        # arrive after a source-specific binding through another provider scope.
        broad = {(stream['table'], stream.get('name'), stream.get('provider'), stream.get('lane'))
                 for stream in streams if stream['storage'] == 'legacy' and stream.get('sourceScope') is None}
        streams = [stream for stream in streams if stream['storage'] != 'legacy'
                   or stream.get('sourceScope') is None
                   or (stream['table'], stream.get('name'), stream.get('provider'), stream.get('lane')) not in broad]
        if len(streams) > MAX_HISTORY_STREAMS:
            raise ValueError('too many history streams; narrow provider/lane/source scope')
        return streams

    def _stream_rows(self, stream, position, limit, source=None, budget=None):
        canonical = stream['storage'] == 'canonical'
        connection = self.connection if canonical else self.legacy_connection
        where, params = list(stream['where']), list(stream['params'])
        ts, key = stream['ts'], stream['key']
        if canonical:
            select = '''SELECT r.*,p.first_fetched_at AS point_first_fetched_at,
                p.last_fetched_at AS point_last_fetched_at FROM catalog_history_points_v2 p
                JOIN catalog_history_revisions_v2 r ON r.revision_id=p.revision_id'''
            if source is not None:
                where.append('r.source=?'); params.append(source)
        else:
            # Raw bodies remain explicitly addressed evidence. Merely browsing
            # point values must not read potentially large raw overflow payloads.
            select = 'SELECT rowid AS _catalog_rowid,' + LEGACY_POINT_PROJECTIONS[stream['table']] + ' FROM ' + stream['table']
        def fetch(sql, parameters, maximum):
            return budget.query(connection, sql, parameters, maximum) if budget else _rows(connection, sql, parameters)
        null_phase = bool(position and position['ts'] is None)
        rows = []
        if not null_phase:
            conditions, parameters = where + [ts + ' IS NOT NULL'], list(params)
            if position:
                conditions.extend([ts + '<=?', '(' + ts + '<? OR ' + key + '<?)'])
                parameters.extend((position['ts'], position['ts'], position['key']))
            rows = fetch(select + ' WHERE ' + ' AND '.join(conditions) +
                         f' ORDER BY {ts} DESC,{key} DESC LIMIT ?', parameters + [limit], limit)
        if len(rows) < limit:
            conditions, parameters = where + [ts + ' IS NULL'], list(params)
            if null_phase:
                conditions.append(key + '<?'); parameters.append(position['key'])
            remaining = limit - len(rows)
            rows.extend(fetch(select + ' WHERE ' + ' AND '.join(conditions) +
                              f' ORDER BY {key} DESC LIMIT ?', parameters + [remaining], remaining))
        results = []
        for row in rows:
            cursor_key = row['point_key'] if canonical else (row['_catalog_rowid'] if stream['key'] == 'rowid' else row[stream['key']])
            point_key = row['point_key'] if canonical else row.get('point_time', cursor_key)
            timestamp = row.get('point_ts', row.get('observed_at'))
            point = {'itemId': None, 'ts': timestamp, 'time': row.get('point_time'),
                     'pointKey': point_key, 'price': row.get('price'), 'volume': row.get('volume'),
                     'median': row.get('median'), 'confidence': row.get('confidence'), 'sourceCount': row.get('source_count'),
                     'provider': row.get('provider', stream.get('provider')), 'lane': row.get('lane'),
                     'source': (row['source'] if 'source' in row else
                                ('steam_history' if stream['table'] == 'price_history' else row.get('provider'))),
                     'rawTime': row.get('raw_time'),
                     'grain': row.get('grain') or ('observation_bucket' if 'observations' in stream['table'] else 'legacy_daily_projection'),
                     'updatedAt': row.get('updated_at', row.get('point_last_fetched_at')),
                     'firstFetchedAt': row.get('point_first_fetched_at'),
                     'lastFetchedAt': row.get('point_last_fetched_at'),
                     'origin': {'storage': stream['storage'], 'store': stream['table'],
                                'name': stream.get('name'), 'pointKey': point_key,
                                'revisionId': row.get('revision_id')},
                     'rawRef': _unjson(row.get('raw_ref_json')) if canonical else
                               {'store': stream['table'], 'name': stream['name'],
                                'provider': row.get('provider'), 'lane': row.get('lane'),
                                'source': row.get('source'), 'pointKey': point_key}}
            if canonical:
                point['payloadId'] = row.get('payload_id')
            results.append((point, {'ts': timestamp, 'key': cursor_key}))
        return results

    def history(self, item_id, kind='auto', limit=200, cursor=None, provider=None, lane=None, source=None,
                max_queries=400, seconds=1):
        self._require_ready()
        if any(value is not None and (not isinstance(value, str) or not value)
               for value in (provider, lane, source)):
            raise ValueError('history filters must be nonempty TEXT; exact legacy lookup supports stored empty keys')
        item = self.get_item(item_id)
        if not item:
            return {'itemId': item_id, 'points': [], 'nextCursor': None, 'coverage': self._coverage()}
        if kind == 'auto':
            locator = item['metadata'].get('legacyLocator', {}) if item['entityType'] == 'legacy_reference' else {}
            kind = LEGACY_HISTORY_KINDS.get(locator.get('store'), 'steam')
            provider = provider if provider is not None else locator.get('provider')
            lane = lane if lane is not None else locator.get('lane')
            source = source if source is not None else locator.get('source')
        limit = max(1, min(1000, int(limit)))
        scope = [item_id, kind, provider, lane, source]
        token = _uncursor(cursor)
        if token and token.get('scope') != scope:
            raise ValueError('cursor belongs to another history query')
        positions = (token or {}).get('positions', {})
        if not isinstance(positions, dict) or any(not isinstance(key, str) or not isinstance(value, dict)
                or not ('ts' in value and 'key' in value)
                or (value['ts'] is not None and type(value['ts']) is not int)
                or not isinstance(value['key'], (str, int)) for key, value in positions.items()):
            raise ValueError('invalid history cursor positions')
        budget = _LegacyBudget(max_queries, seconds)
        streams = self._streams(item_id, kind, provider, lane, source, budget)
        stream_ids = sorted(stream['id'] for stream in streams)
        signature = hashlib.sha256(_json(stream_ids).encode()).hexdigest()
        if token and token.get('streams') != signature:
            raise ValueError('history streams changed; restart pagination')
        if any(key not in stream_ids for key in positions):
            raise ValueError('unknown history stream in cursor')
        for stream in streams:
            if stream['id'] in positions:
                key = positions[stream['id']]['key']
                if (stream['storage'] == 'canonical' and not isinstance(key, str)) or (
                        stream['storage'] == 'legacy' and type(key) is not int):
                    raise ValueError('history cursor key type does not match its stream')
        candidates = []
        for stream in streams:
            for point, position in self._stream_rows(stream, positions.get(stream['id']), limit + 1, source=source, budget=budget):
                point['itemId'] = item_id
                candidates.append((point, position, stream['id']))
        candidates.sort(key=lambda value: (value[0]['ts'] is not None, value[0]['ts'] or 0,
                                           value[2], value[1]['key']), reverse=True)
        chosen = candidates[:limit]
        for point, position, stream_id in chosen:
            positions[stream_id] = position
        self._cash_history_points([value[0] for value in chosen], item_id, budget, identity=item)
        next_cursor = _cursor({'scope': scope, 'positions': positions, 'streams': signature}) if len(candidates) > limit else None
        return {'itemId': item_id, 'points': [value[0] for value in chosen], 'nextCursor': next_cursor,
                'coverage': {**self._coverage(), 'streamsQueried': len(streams),
                             'pendingLegacyLinks': item['pendingLegacyLinks'],
                             'providersQueried': sorted({stream['provider'] for stream in streams if stream.get('provider') is not None}),
                             'providerScope': 'indexed_prefix_discovery_or_explicit_verified_binding',
                             'legacyReadBudget': budget.report()}}

    @staticmethod
    def _prefix_sql(table, index, keys, depth, prefix, previous=None):
        clauses, parameters = [], []
        for column, value in zip(keys[:depth], prefix[:depth]):
            clauses.append(_quoted(column) + ' COLLATE BINARY=?')
            parameters.append(value)
        # Empty TEXT is the smallest BINARY TEXT key and is INCLUDED by >=.
        # A strict > '' initial sentinel would silently omit stored empty keys.
        clauses.append(_quoted(keys[depth]) + ' COLLATE BINARY' + ('>=?' if previous is None else '>?'))
        parameters.append('' if previous is None else previous)
        sql = ('SELECT ' + _quoted(keys[depth]) + ' AS value FROM ' + _quoted(table) +
               ' INDEXED BY ' + _quoted(index) + ' WHERE ' + ' AND '.join(clauses) +
               ' ORDER BY ' + _quoted(keys[depth]) + ' COLLATE BINARY LIMIT 1')
        return sql, parameters

    def _inspect_prefix_access(self, table, keys, budget, connection=None):
        connection = connection if connection is not None else self.legacy_connection
        cache_key = (id(connection), table, tuple(keys))
        if cache_key in self._legacy_index_cache:
            return self._legacy_index_cache[cache_key]
        table_rows = budget.query(connection, 'SELECT sql FROM sqlite_master WHERE type=\'table\' AND name=? LIMIT 1', (table,), 1)
        if not table_rows:
            raise LegacyAccessError('missing_table', {'store': table})
        if 'WITHOUT ROWID' in (table_rows[0]['sql'] or '').upper():
            raise LegacyAccessError('type_conflict', {'store': table, 'detail': 'ordinary rowid history required'})
        columns = {row['name']: row for row in budget.query(connection, 'PRAGMA table_info(' + _quoted(table) + ')')}
        for column in keys:
            info = columns.get(column)
            if not info or (info.get('type') or '').upper().strip() != 'TEXT' or not info.get('notnull'):
                raise LegacyAccessError('type_conflict', {'store': table, 'column': column,
                    'detail': 'discovery requires declared TEXT NOT NULL prefix dimensions'})
        candidates = []
        for index in budget.query(connection, 'PRAGMA index_list(' + _quoted(table) + ')'):
            if index.get('partial'):
                continue
            details = budget.query(connection, 'PRAGMA index_xinfo(' + _quoted(index['name']) + ')')
            ordered = sorted((row for row in details if row.get('key', 1)), key=lambda row: row['seqno'])
            if tuple(row['name'] for row in ordered[:len(keys)]) != tuple(keys):
                continue
            if any((row.get('coll') or 'BINARY').upper() != 'BINARY' for row in ordered[:len(keys)]):
                continue
            candidates.append((len(ordered), index['name'], ordered))
        if not candidates:
            raise LegacyAccessError('no_index', {'store': table, 'requiredPrefix': list(keys),
                'detail': 'no existing nonpartial BINARY leading-prefix index; no index was created'})
        _, index_name, ordered = min(candidates, key=lambda value: (value[0], value[1]))
        plans = []
        for depth in range(len(keys)):
            for previous in (None, ''):
                sql, parameters = self._prefix_sql(table, index_name, keys, depth, [''] * depth, previous)
                rows = budget.query(connection, 'EXPLAIN QUERY PLAN ' + sql, parameters, 16)
                details = [row['detail'] for row in rows]
                if not any('SEARCH ' in value.upper() for value in details) or any(
                        'SCAN ' in value.upper() or 'TEMP B-TREE' in value.upper() for value in details):
                    raise LegacyAccessError('no_index', {'store': table, 'index': index_name, 'plan': details,
                        'detail': 'prefix query is not a validated indexed SEARCH'})
                plans.append(details)
        fingerprint = hashlib.sha256(_json([table, table_rows[0]['sql'], index_name,
                                            list(keys), ordered]).encode()).hexdigest()
        result = {'store': table, 'keys': list(keys), 'index': index_name,
                  'schemaFingerprint': fingerprint, 'plans': plans, 'validatedSearch': True}
        self._legacy_index_cache[cache_key] = result
        return result

    def inspect_legacy_access(self, store, max_queries=100, seconds=1):
        """Pure, bounded metadata/EXPLAIN validation; never creates an index."""
        if store not in LEGACY_PUBLIC_PREFIXES:
            raise ValueError('unsupported public legacy store')
        budget = _LegacyBudget(max_queries, seconds)
        self._legacy_index_cache.pop((id(self.legacy_connection), store, LEGACY_PUBLIC_PREFIXES[store]), None)
        try:
            result = self._inspect_prefix_access(store, LEGACY_PUBLIC_PREFIXES[store], budget)
            return {'safe': True, **result, **budget.report()}
        except LegacyAccessError as error:
            return {'safe': False, 'store': store, 'stoppedReason': error.reason,
                    'detail': error.report, **budget.report()}

    def _discover_prefixes(self, store, keys, cursor, limit, budget, max_cursor_bytes,
                           fixed=(), connection=None):
        connection = connection if connection is not None else self.legacy_connection
        original_cursor = cursor
        state = None
        streams, access = [], None
        try:
            access = self._inspect_prefix_access(store, keys, budget, connection)
            state = _uncursor(cursor)
            if state is None:
                state = {'version': 1, 'store': store, 'schema': access['schemaFingerprint'],
                         'fixed': list(fixed), 'path': list(fixed) + [None] * (len(keys) - len(fixed)),
                         'last': [None] * len(keys), 'depth': len(fixed)}
            if (state.get('version') != 1 or state.get('store') != store or state.get('fixed') != list(fixed)
                    or state.get('schema') != access['schemaFingerprint']):
                raise ValueError('prefix cursor scope/schema changed; begin a new pass')
            if (not isinstance(state.get('path'), list) or len(state['path']) != len(keys)
                    or not isinstance(state.get('last'), list) or len(state['last']) != len(keys)
                    or type(state.get('depth')) is not int or not len(fixed) <= state['depth'] < len(keys)
                    or any(value is not None and not isinstance(value, str) for value in state['path'] + state['last'])
                    or state['path'][:len(fixed)] != list(fixed)
                    or any(value is None for value in state['path'][:state['depth']])):
                raise ValueError('invalid prefix cursor')
            done, reason = False, 'limit'
            while len(streams) < limit:
                depth = state['depth']
                previous_state = json.loads(_json(state))
                sql, parameters = self._prefix_sql(store, access['index'], keys, depth, state['path'], state['last'][depth])
                found = budget.query(connection, sql, parameters, 1)
                if not found:
                    if depth == len(fixed):
                        done, reason = True, 'eof'
                        break
                    state['path'][depth] = None
                    state['last'][depth] = None
                    state['depth'] -= 1
                    continue
                value = found[0]['value']
                if not isinstance(value, str):
                    state = previous_state
                    raise LegacyAccessError('type_conflict', {'store': store, 'column': keys[depth],
                        'detail': 'stored prefix value is not TEXT; cursor was not advanced'})
                state['path'][depth], state['last'][depth] = value, value
                if depth + 1 < len(keys):
                    state['depth'] += 1
                    state['last'][depth + 1] = None
                    state['path'][depth + 1] = None
                token = _cursor(state)
                if len(token) > max_cursor_bytes:
                    state = previous_state
                    raise LegacyAccessError('cursor_budget', {'store': store, 'maxCursorBytes': max_cursor_bytes})
                if depth + 1 == len(keys):
                    streams.append({'store': store, **dict(zip(keys, state['path']))})
            return {'store': store, 'streams': streams, 'cursor': None if done else _cursor(state), 'done': done,
                    'stoppedReason': reason, 'index': access['index'], 'schemaFingerprint': access['schemaFingerprint'],
                    'coverage': {'window': 'observed_rolling_window', 'globalCompletenessProven': False,
                                 'rescanRequiredForEarlierInserts': True}, **budget.report()}
        except LegacyAccessError as error:
            safe_cursor = _cursor(state) if state is not None else original_cursor
            if safe_cursor and len(safe_cursor) > max_cursor_bytes:
                safe_cursor = original_cursor
            return {'store': store, 'streams': streams, 'cursor': safe_cursor,
                    'done': False, 'stoppedReason': error.reason, 'detail': error.report,
                    'index': access['index'] if access else None,
                    'coverage': {'window': 'observed_rolling_window', 'globalCompletenessProven': False},
                    **budget.report()}

    def discover_legacy_streams_batch(self, store, cursor=None, limit=100, max_queries=400,
                                      seconds=1, max_cursor_bytes=8192):
        """Pure distinct-prefix seeks, never DISTINCT/GROUP BY/history row walk."""
        if store not in LEGACY_PUBLIC_PREFIXES:
            raise ValueError('unsupported public legacy store')
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError('limit must be 1..500')
        if type(max_cursor_bytes) is not int or not 256 <= max_cursor_bytes <= 16384:
            raise ValueError('max_cursor_bytes must be 256..16384')
        if cursor and len(cursor) > max_cursor_bytes:
            raise ValueError('cursor exceeds explicit cursor budget')
        budget = _LegacyBudget(max_queries, seconds)
        self._legacy_index_cache.pop((id(self.legacy_connection), store, LEGACY_PUBLIC_PREFIXES[store]), None)
        return self._discover_prefixes(store, LEGACY_PUBLIC_PREFIXES[store], cursor, limit, budget, max_cursor_bytes)

    @staticmethod
    def _legacy_handle(locator):
        return 'legacy_' + _cursor({'version': 1, 'locator': locator})

    def _locator_owners(self, locator, budget=None):
        def query(sql, parameters=(), maximum=128):
            return budget.query(self.connection, sql, parameters, maximum) if budget else self._all(sql, parameters)
        if self._owner_schema_available is None:
            tables = query("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('catalog_store_meta','catalog_items_v2','catalog_legacy_bindings_v2') LIMIT 4", (), 4)
            version = query("SELECT value FROM catalog_store_meta WHERE key='schema_version' LIMIT 1", (), 1) if len(tables) == 3 else []
            self._owner_schema_available = bool(version and version[0]['value'] in ('1', str(SCHEMA_VERSION)))
        if not self._owner_schema_available:
            return {'status': 'unregistered', 'owners': []}
        parameters = [locator['store'], locator['name']]
        clauses = ['b.store=?', 'b.legacy_name=?']
        for dimension in ('provider', 'lane', 'source'):
            value = locator.get(dimension)
            if value is not None:
                clauses.append('(b.' + dimension + "='' OR b." + dimension + '=?)')
                parameters.append(value)
        rows = query('SELECT b.*,i.entity_type FROM catalog_legacy_bindings_v2 b JOIN catalog_items_v2 i ON i.item_id=b.item_id WHERE ' +
                      ' AND '.join(clauses) + ' ORDER BY b.item_id LIMIT 129', parameters, 129)
        if len(rows) > 128:
            return {'status': 'ambiguous', 'owners': [], 'reason': 'binding_budget'}
        partial = [row for row in rows if row['source'] and 'source' not in locator and
                   not (locator['store'] == 'price_history' and row['source'] == 'steam_history')]
        if locator['store'] in LEGACY_CURRENT_PREFIXES:
            partial = [row for row in rows if any(row[key] and key not in locator
                                                  for key in ('provider', 'lane', 'source'))]
        complete_ids = sorted({row['item_id'] for row in rows if row not in partial})
        all_ids = sorted({row['item_id'] for row in rows})
        if len(complete_ids) == 1 and all(value == complete_ids[0] for value in all_ids):
            owner = next(row for row in rows if row['item_id'] == complete_ids[0])
            return {'status': 'owned', 'owners': complete_ids, 'entityType': owner['entity_type']}
        if rows:
            return {'status': 'ambiguous', 'owners': all_ids,
                    'reason': 'source_scoped_owner_requires_decision' if partial else 'multiple_binding_owners'}
        return {'status': 'unregistered', 'owners': []}

    def register_legacy_history_batch(self, store, cursor=None, limit=100, max_queries=400,
                                      seconds=1, max_cursor_bytes=8192, quiescent=False, restart=False,
                                      recovery_max_bytes=1048576):
        if store not in LEGACY_HISTORY_PREFIXES:
            raise ValueError('unsupported legacy history store')
        return self._register_legacy_batch(store, cursor, limit, max_queries, seconds,
                                          max_cursor_bytes, quiescent, restart, recovery_max_bytes)

    def register_legacy_current_batch(self, store, cursor=None, limit=100, max_queries=400,
                                      seconds=1, max_cursor_bytes=8192, quiescent=False, restart=False,
                                      recovery_max_bytes=1048576):
        """Register physical current bindings using exact verified hashes or refs.

        Uses the unchanged v2 receipt tables/shape and caller-owned transaction.
        Discovery and exact lookup do not require registration.
        """
        if store not in LEGACY_CURRENT_PREFIXES:
            raise ValueError('unsupported public current store')
        return self._register_legacy_batch(store, cursor, limit, max_queries, seconds,
                                          max_cursor_bytes, quiescent, restart, recovery_max_bytes)

    def _register_legacy_batch(self, store, cursor=None, limit=100, max_queries=400,
                               seconds=1, max_cursor_bytes=8192, quiescent=False, restart=False,
                               recovery_max_bytes=1048576):
        """Explicit writes: add reference identities plus atomic pass/receipt state.

        A reference is one physical stream, not an assertion of Steam identity.
        Hard stop reports make no registry/checkpoint changes. Caller commits.
        """
        self._require_ready()
        if store not in LEGACY_PUBLIC_PREFIXES:
            raise ValueError('unsupported public legacy store')
        keys = LEGACY_PUBLIC_PREFIXES[store]
        current = store in LEGACY_CURRENT_PREFIXES
        self._begin_write()
        if type(recovery_max_bytes) is not int or not 1024 <= recovery_max_bytes <= 4194304:
            raise ValueError('recovery_max_bytes must be 1024..4194304')
        old = self._one('SELECT * FROM catalog_history_coverage_v2 WHERE store=?', (store,))
        expected = old['cursor'] if old else None
        if (restart and cursor is not None) or (cursor is not None and cursor != expected):
            return {'store': store, 'processed': 0, 'registered': 0, 'verifiedOwned': 0,
                    'ambiguous': 0, 'done': False, 'cursor': expected,
                    'stoppedReason': 'checkpoint_mismatch', 'affectedItemIds': [], 'affectedBindings': [],
                    'detail': 'use the exact persisted checkpoint, or explicit restart=True with no cursor'}
        if old and old['done'] and cursor is None and not restart:
            # This is the stored pass's no-op completion, not discovery EOF for
            # a caller's newer pass. Orchestrators must retain restart intent.
            return {'store': store, 'schemaVersion': SCHEMA_VERSION, 'passId': old['pass_id'],
                    'passAlreadyComplete': True,
                    'processed': 0, 'registered': 0, 'verifiedOwned': 0, 'ambiguous': 0, 'done': True,
                    'cursor': None, 'stoppedReason': 'observed_pass_already_complete',
                    'affectedItemIds': [], 'affectedBindings': [], 'batchHash': old['last_batch_hash'],
                    'coverage': {'window': old['window_mode'], 'globalCompletenessProven': False,
                                 'newPassRequiredToFindEarlierInserts': True}}
        before = cursor if cursor is not None else (old['cursor'] if old and not restart else None)
        result = self.discover_legacy_streams_batch(store, before, limit, max_queries, seconds, max_cursor_bytes)
        result.update(processed=0, registered=0, verifiedOwned=0, adoptedExactListings=0, ambiguous=0,
                      affectedItemIds=[], affectedBindings=[], schemaVersion=SCHEMA_VERSION)
        if result['stoppedReason'] not in ('limit', 'eof'):
            result['cursor'] = before
            return result
        decisions = []
        for locator in result['streams']:
            ownership = self._locator_owners(locator)
            if any(locator[key] == '' for key in keys) or ownership['status'] == 'ambiguous':
                result.update(stoppedReason='ambiguity' if ownership['status'] == 'ambiguous' else 'type_conflict',
                              ambiguous=1, done=False, cursor=before, blockedLocator=locator,
                              candidates=ownership['owners'], detail=ownership.get('reason') or
                              'empty physical key cannot be represented as a v1 wildcard binding; exact lookup remains available')
                return result
            if ownership['status'] == 'unregistered':
                identity = _json([locator[key] for key in ('store',) + keys])
                orphan = self._one('SELECT item_id FROM catalog_identifiers_v2 WHERE namespace=? AND value=?',
                                   ('legacy-stream:' + store, identity))
                if orphan:
                    result.update(stoppedReason='type_conflict', done=False, cursor=before,
                                  blockedLocator=locator, candidates=[orphan['item_id']],
                                  detail='orphan legacy identifier requires an explicit binding repair')
                    return result
                if current:
                    matches = self._all('''SELECT i.item_id,i.entity_type,i.market_hash_name,i.priceable
                        FROM catalog_identifiers_v2 x JOIN catalog_items_v2 i ON i.item_id=x.item_id
                        WHERE x.namespace='steam:730:market_hash_name' AND x.value=? LIMIT 2''', (locator['name'],))
                    if (len(matches) == 1 and matches[0]['entity_type'] == 'listing'
                            and matches[0]['market_hash_name'] == locator['name'] and matches[0]['priceable']):
                        ownership = {'status': 'adopt_exact_listing', 'owners': [matches[0]['item_id']],
                                     'entityType': 'listing'}
            decisions.append((locator, ownership))
        self.connection.execute('SAVEPOINT catalog_register_history')
        try:
            previous_generation = self._one("SELECT key,value FROM catalog_store_meta WHERE key='generation'")
            new_ids = []
            adopted_bindings = []
            for locator, ownership in decisions:
                if ownership['status'] == 'owned':
                    item_id = ownership['owners'][0]
                    result['verifiedOwned'] += int(ownership['entityType'] != 'legacy_reference')
                elif ownership['status'] == 'adopt_exact_listing':
                    item_id = ownership['owners'][0]
                    self.bind_legacy(item_id, store, locator['name'], provider=locator.get('provider'),
                                     lane=locator.get('lane'), source=locator.get('source'))
                    adopted_bindings.append({'item_id': item_id, 'store': store, 'legacy_name': locator['name'],
                        'provider': locator.get('provider', ''), 'lane': locator.get('lane', ''), 'source': locator.get('source', '')})
                    result['verifiedOwned'] += 1
                    result['adoptedExactListings'] += 1
                else:
                    identity = _json([locator[key] for key in ('store',) + keys])
                    item_id = self.ensure_item({'name': locator['name'], 'kind': 'legacy_current' if current else 'legacy_history',
                        'entity_type': 'legacy_reference', 'priceable': False,
                        'identifiers': [('legacy-stream:' + store, identity)],
                        'aliases': [{'namespace': 'legacy-name:' + store, 'value': locator['name']}],
                        'metadata': {'legacyLocator': locator,
                                     **({'currentQuoteStore': store} if current else {'historyKind': LEGACY_HISTORY_KINDS[store]}),
                                     'identityStatus': 'unverified_legacy_reference'},
                        'raw_ref': {'storage': 'legacy_stream', **locator}},
                        source=('current-reference:' if current else 'history-reference:') + store)
                    self.bind_legacy(item_id, store, locator['name'], provider=locator.get('provider'),
                                     lane=locator.get('lane'), source=locator.get('source'))
                    result['registered'] += 1
                    new_ids.append(item_id)
                result['affectedItemIds'].append(item_id)
                result['affectedBindings'].append({'itemId': item_id, **locator})
            result['affectedItemIds'] = sorted(set(result['affectedItemIds']))
            result['processed'] = len(decisions)
            if adopted_bindings:
                self._bump_generation()
            recovery_after = {}
            for table in ('catalog_items_v2', 'catalog_identifiers_v2', 'catalog_aliases_v2',
                          'catalog_metadata_v2', 'catalog_legacy_bindings_v2'):
                recovery_after[table] = self._all('SELECT * FROM ' + table + ' WHERE item_id IN (' +
                    ','.join('?' for _ in new_ids) + ')', new_ids) if new_ids else []
            # Existing listing facts are unchanged. Record only the exact new
            # bindings here; the CLI independently snapshots its bounded IDs.
            recovery_after['catalog_legacy_bindings_v2'].extend(adopted_bindings)
            recovery = {'mode': 'additive_reference_reconstruction_and_conditional_checkpoint_restore',
                        'notAnUnconditionalUndo': True, 'historyRowsIncluded': False,
                        'before': {'catalog_store_meta': [previous_generation],
                                   'catalog_history_coverage_v2': [old] if old else []},
                        'after': recovery_after, 'newItemIds': new_ids}
            if len(_json(recovery).encode()) > recovery_max_bytes:
                raise LegacyAccessError('recovery_budget', {'maximumBytes': recovery_max_bytes})
            pass_id = uuid.uuid4().hex if restart or not old else old['pass_id']
            evidence = {'schemaVersion': SCHEMA_VERSION, 'passId': pass_id, 'store': store, 'cursorBefore': before,
                        'cursorAfter': result['cursor'], 'streams': result['streams'],
                        'affectedBindings': result['affectedBindings'],
                        'registryRowsHash': hashlib.sha256(_json(recovery_after).encode()).hexdigest()}
            batch_hash = hashlib.sha256(_json(evidence).encode()).hexdigest()
            now = int(time.time())
            window = ('caller_declared_quiescent_window' if quiescent and
                      (restart or not old or old['window_mode'] == 'caller_declared_quiescent_window')
                      else 'observed_rolling_window')
            starting = {} if restart or not old else old
            self.connection.execute('''INSERT INTO catalog_history_coverage_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(store) DO UPDATE SET cursor=excluded.cursor,done=excluded.done,
                observed_streams=excluded.observed_streams,registered_references=excluded.registered_references,
                verified_owned=excluded.verified_owned,status=excluded.status,error=NULL,
                pass_id=excluded.pass_id,window_mode=excluded.window_mode,updated_at=excluded.updated_at,
                last_batch_hash=excluded.last_batch_hash''',
                (store, result['cursor'], int(result['done']), starting.get('observed_streams', 0) + result['processed'],
                 starting.get('registered_references', 0) + result['registered'],
                 starting.get('verified_owned', 0) + result['verifiedOwned'],
                 'observed_pass_complete' if result['done'] else 'in_progress', None, pass_id, window, now, batch_hash))
            recovery['after']['catalog_store_meta'] = [self._one("SELECT key,value FROM catalog_store_meta WHERE key='generation'")]
            recovery['after']['catalog_history_coverage_v2'] = [self._one('SELECT * FROM catalog_history_coverage_v2 WHERE store=?', (store,))]
            if len(_json(recovery).encode()) > recovery_max_bytes:
                raise LegacyAccessError('recovery_budget', {'maximumBytes': recovery_max_bytes})
            recovery_hash = hashlib.sha256(_json(recovery).encode()).hexdigest()
            self.connection.execute('INSERT OR IGNORE INTO catalog_history_batches_v2 VALUES(?,?,?,?,?,?,?,?)',
                (batch_hash, store, SCHEMA_VERSION, before, result['cursor'], _json(result['streams']),
                 _json({'bindings': result['affectedBindings'], 'recoveryHash': recovery_hash}), now))
            self.validate_affected(result)
            self.connection.execute('RELEASE catalog_register_history')
        except BaseException:
            self.connection.execute('ROLLBACK TO catalog_register_history')
            self.connection.execute('RELEASE catalog_register_history')
            raise
        result.update(batchHash=batch_hash, receiptId=batch_hash, passId=pass_id,
                      recovery=recovery, recoveryHash=recovery_hash,
                      coverage={'window': window, 'globalCompletenessProven': False,
                                'newPassRequiredToFindEarlierInserts': window != 'caller_declared_quiescent_window'})
        return result

    def validate_affected(self, result):
        """Only this bounded batch's IDs/bindings, never global foreign_key_check."""
        item_ids, bindings = result.get('affectedItemIds', []), result.get('affectedBindings', [])
        pending = result.get('affectedPendingLinks', [])
        if len(item_ids) > 500 or len(bindings) > 9000 or len(pending) > 9000:
            raise ValueError('affected batch exceeds validation budget')
        for item_id in item_ids:
            if not self._one('SELECT 1 FROM catalog_items_v2 WHERE item_id=?', (item_id,)):
                raise IdentityConflict('affected item is missing')
        for binding in bindings:
            locator = {key: value for key, value in binding.items() if key != 'itemId'}
            owner = self._locator_owners(locator)
            if owner['status'] != 'owned' or owner['owners'] != [binding['itemId']]:
                raise IdentityConflict('affected stream binding invariant failed')
        for link in pending:
            keys = ('item_id', 'reference_item_id', 'store', 'legacy_name', 'provider', 'lane', 'source')
            actual = self._one('SELECT * FROM catalog_pending_legacy_links_v2 WHERE ' +
                              ' AND '.join(key + '=?' for key in keys), [link[key] for key in keys])
            owner = self._one('''SELECT i.entity_type FROM catalog_legacy_bindings_v2 b
                JOIN catalog_items_v2 i ON i.item_id=b.item_id WHERE b.item_id=? AND b.store=?
                AND b.legacy_name=? AND b.provider=? AND b.lane=? AND b.source=?''',
                [link[key] for key in ('reference_item_id', 'store', 'legacy_name', 'provider', 'lane', 'source')])
            if actual != link or not owner or owner['entity_type'] != 'legacy_reference':
                raise IdentityConflict('affected pending legacy link invariant failed')
        return {'ok': True, 'valid': True, 'checkedItems': len(item_ids), 'checkedBindings': len(bindings),
                'checkedPendingLinks': len(pending)}

    def lookup_legacy(self, store, name, provider=None, lane=None, source=None, limit=100,
                      cursor=None, max_queries=100, seconds=1, max_cursor_bytes=8192):
        """Pure exact legacy access works even before registry schema/seed exists.

        Missing indexed dimensions return explicit stream choices/handles. A
        fully specified address reads one bounded keyset page without assigning
        an unregistered reference a canonical item ID.
        """
        if store not in LEGACY_PUBLIC_PREFIXES or not isinstance(name, str):
            raise ValueError('known public legacy store and exact name required')
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError('limit must be 1..500')
        if type(max_cursor_bytes) is not int or not 256 <= max_cursor_bytes <= 16384:
            raise ValueError('max_cursor_bytes must be 256..16384')
        if cursor and len(cursor) > max_cursor_bytes:
            raise ValueError('cursor exceeds explicit cursor budget')
        if source is not None and store == 'shared_price_observations':
            raise ValueError('shared observations have no source dimension')
        if source is not None and store == 'price_history' and source != 'steam_history':
            raise ValueError('legacy Steam history has no arbitrary source column')
        if store == 'price_observations' and source is not None and provider is None:
            return {'status': 'scope_required', 'store': store, 'name': name, 'points': [], 'references': [],
                    'requiredDimensions': ['provider'], 'requestedFilter': {'source': source},
                    'detail': 'an exact provider is required to retain the requested observation-source filter',
                    'cursor': None, 'queries': 0, 'maxQueries': max_queries,
                    'coverage': {'providerScope': 'not_discovered; enumerate physical provider prefixes without a source filter first'}}
        keys = LEGACY_PUBLIC_PREFIXES[store]
        supplied = {'name': name, 'provider': provider, 'lane': lane, 'source': source}
        if store in LEGACY_CURRENT_PREFIXES and any(value is not None and key not in keys
                                                   for key, value in supplied.items()):
            raise ValueError('current quote locator must use its physical key dimensions only')
        fixed = []
        for key in keys:
            if supplied[key] is None:
                break
            if not isinstance(supplied[key], str):
                raise ValueError('exact prefix dimensions must be TEXT')
            fixed.append(supplied[key])
        budget = _LegacyBudget(max_queries, seconds)
        if len(fixed) < len(keys):
            result = self._discover_prefixes(store, keys, cursor, limit, budget, max_cursor_bytes, fixed=fixed)
            references = []
            try:
                for locator in result['streams']:
                    references.append({'referenceHandle': self._legacy_handle(locator), 'locator': locator,
                                       'ownership': self._locator_owners(locator, budget=budget)})
            except LegacyAccessError as error:
                return {'status': 'blocked', 'store': store, 'name': name, 'points': [],
                        'references': references, 'cursor': cursor, 'done': False,
                        'stoppedReason': error.reason, 'detail': error.report, **budget.report()}
            return {**result, 'status': 'scope_required', 'store': store, 'name': name,
                    'points': [], 'references': references, **budget.report()}
        locator = {'store': store, **dict(zip(keys, fixed))}
        if store in LEGACY_CURRENT_PREFIXES:
            if cursor:
                raise ValueError('exact current quote lookup has no page cursor')
            return self._lookup_current_locator(locator, budget)
        if store == 'price_observations' and source is not None:
            locator['source'] = source
        token = _uncursor(cursor)
        if token and (token.get('scope') != locator or not isinstance(token.get('position'), dict)):
            raise ValueError('legacy history cursor scope changed')
        if token:
            position = token['position']
            if ('ts' not in position or 'key' not in position or
                    (position['ts'] is not None and type(position['ts']) is not int) or
                    type(position['key']) is not int):
                raise ValueError('invalid legacy history cursor position')
        try:
            access = self._inspect_prefix_access(store, keys, budget)
            where = [_quoted(key) + ' COLLATE BINARY=?' for key in keys]
            exists = budget.query(self.legacy_connection, 'SELECT 1 AS present FROM ' + _quoted(store) +
                ' INDEXED BY ' + _quoted(access['index']) + ' WHERE ' + ' AND '.join(where) + ' LIMIT 1', fixed, 1)
            if not exists:
                return {'status': 'not_found', 'locator': locator, 'referenceHandle': self._legacy_handle(locator),
                        'points': [], 'nextCursor': None, **budget.report()}
            kind = LEGACY_HISTORY_KINDS[store]
            stream = {'id': _json(locator), 'storage': 'legacy', 'table': store, 'name': name,
                      'provider': provider, 'where': [key + '=?' for key in keys], 'params': fixed,
                      'ts': 'observed_at' if 'observations' in store else 'point_ts',
                      'key': 'rowid' if store == 'price_history' else ('point_ts' if store == 'shared_price_history' else 'observed_at')}
            if store == 'price_observations' and source is not None:
                stream['where'].append('source=?')
                stream['params'] = list(fixed) + [source]
            for null_phase in (False, True):
                ts, key = stream['ts'], stream['key']
                plan_sql = ('EXPLAIN QUERY PLAN SELECT rowid AS _catalog_rowid,' + LEGACY_POINT_PROJECTIONS[store] +
                    ' FROM ' + store + ' WHERE ' + ' AND '.join(stream['where'] + [ts + (' IS NULL' if null_phase else ' IS NOT NULL')]) +
                    ' ORDER BY ' + (key + ' DESC' if null_phase else ts + ' DESC,' + key + ' DESC') + ' LIMIT 1')
                plan = budget.query(self.legacy_connection, plan_sql, stream['params'], 16)
                details = [row['detail'].upper() for row in plan]
                if not any('SEARCH ' in value for value in details) or any('SCAN ' in value or 'TEMP B-TREE' in value for value in details):
                    raise LegacyAccessError('no_index', {'store': store, 'detail': 'exact point page requires an indexed SEARCH without a temporary sort', 'plan': details})
            points = self._stream_rows(stream, token.get('position') if token else None, limit + 1, budget=budget)
            chosen = points[:limit]
            ownership = self._locator_owners(locator, budget=budget)
            for point, position in chosen:
                point['itemId'] = ownership['owners'][0] if ownership['status'] == 'owned' else None
            self._cash_history_points([point for point, position in chosen],
                ownership['owners'][0] if ownership['status'] == 'owned' else None, budget, ownership=ownership)
            next_cursor = _cursor({'scope': locator, 'position': chosen[-1][1]}) if len(points) > limit else None
            if next_cursor and len(next_cursor) > max_cursor_bytes:
                raise LegacyAccessError('cursor_budget')
            return {'status': 'found', 'locator': locator, 'referenceHandle': self._legacy_handle(locator),
                    'ownership': ownership, 'points': [point for point, position in chosen], 'nextCursor': next_cursor,
                    'coverage': {'scope': 'exact_physical_stream', 'semanticListingIdentityVerified':
                                 ownership['status'] == 'owned' and ownership.get('entityType') == 'listing'},
                    **budget.report()}
        except LegacyAccessError as error:
            return {'status': 'blocked', 'locator': locator, 'referenceHandle': self._legacy_handle(locator),
                    'points': [], 'nextCursor': cursor, 'stoppedReason': error.reason,
                    'detail': error.report, **budget.report()}

    def _lookup_current_locator(self, locator, budget):
        """One physical current key; raw remains in its native evidence store."""
        store = locator['store']
        keys = LEGACY_CURRENT_PREFIXES[store]
        try:
            access = self._inspect_prefix_access(store, keys, budget)
            projection = self._current_projection(store, budget)
            records = budget.query(self.legacy_connection, 'SELECT ' + projection + ' FROM ' + _quoted(store) +
                ' INDEXED BY ' + _quoted(access['index']) + ' WHERE ' +
                ' AND '.join(_quoted(key) + ' COLLATE BINARY=?' for key in keys) + ' LIMIT 2',
                [locator[key] for key in keys], 2)
            if len(records) > 1:
                raise LegacyAccessError('type_conflict', {'store': store, 'detail': 'current physical key is not unique'})
            ownership = self._locator_owners(locator, budget=budget)
            item_id = ownership['owners'][0] if ownership['status'] == 'owned' else None
            return {'status': 'found' if records else 'not_found', 'locator': locator,
                    'referenceHandle': self._legacy_handle(locator), 'ownership': ownership,
                    'quotes': [self._cash_current_quote_dto(store, row, item_id, budget, ownership=ownership) for row in records],
                    'points': [], 'nextCursor': None,
                    'coverage': {'scope': 'exact_physical_current_quote',
                                 'semanticListingIdentityVerified': ownership['status'] == 'owned' and
                                 ownership.get('entityType') == 'listing'}, **budget.report()}
        except LegacyAccessError as error:
            return {'status': 'blocked', 'locator': locator, 'referenceHandle': self._legacy_handle(locator),
                    'quotes': [], 'points': [], 'nextCursor': None, 'stoppedReason': error.reason,
                    'detail': error.report, **budget.report()}

    def lookup_legacy_handle(self, reference_handle, **parameters):
        if not isinstance(reference_handle, str) or not reference_handle.startswith('legacy_'):
            raise ValueError('legacy reference handle required')
        token = _uncursor(reference_handle[len('legacy_'):])
        locator = token.get('locator') if token else None
        if not isinstance(locator, dict) or token.get('version') != 1 or locator.get('store') not in LEGACY_PUBLIC_PREFIXES:
            raise ValueError('invalid legacy reference handle')
        if set(parameters) & {'store', 'name', 'provider', 'lane', 'source'}:
            raise ValueError('a reference handle cannot be combined with a different locator')
        allowed = {'store', 'name', 'provider', 'lane', 'source'}
        if set(locator) - allowed or not isinstance(locator.get('name'), str):
            raise ValueError('invalid legacy reference locator')
        return self.lookup_legacy(**locator, **parameters)
