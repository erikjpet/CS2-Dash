"""Bounded process-local cache around the unchanged saved market slice.

No database opens at import, disk cache, providers, jobs or implicit writes.
Revision fencing is an optimization of the existing saved-snapshot response.
"""
from collections import OrderedDict
from functools import wraps
import hashlib
import inspect
import json
import sqlite3
import threading
import time

BYPASS = object()
ALL_INPUT_KEYS = ('bulk_derived_generation_head:v1', 'cash_source_revision:skinport_live', 'bulk_derived_writer_abi:v1',
    'bulk_derived_input_epoch:v1', 'bulk_derived_metric_input_epoch:v1',
    'bulk_derived_pending:v1', 'bulk_derived_published:v1',
    'metric_build_version', 'lookup_build_version')


class RevisionChanged(RuntimeError):
    pass


def bounded_json(value, maximum):
    chunks, size = [], 0
    for part in json.JSONEncoder(separators=(',', ':'), ensure_ascii=False,
                                 allow_nan=False, sort_keys=True).iterencode(value):
        encoded = part.encode('utf-8')
        size += len(encoded)
        if size > maximum:
            return None
        chunks.append(encoded)
    return b''.join(chunks)


class SliceCache:
    """JSON bytes prevent shared mutable DTOs; singleflight has finite waits.

    Timeout/capacity/oversize bypass preserves the original uncached API path;
    it is not a concurrency limiter for that preexisting path.
    """
    def __init__(self, clock=time.monotonic, maximum_entries=8, maximum_bytes=16777216,
                 maximum_entry_bytes=2097152, ttl_seconds=10, maximum_flights=4,
                 wait_seconds=3):
        self.clock = clock
        self.maximum_entries, self.maximum_bytes = maximum_entries, maximum_bytes
        self.maximum_entry_bytes, self.ttl_seconds = maximum_entry_bytes, ttl_seconds
        self.maximum_flights, self.wait_seconds = maximum_flights, wait_seconds
        self.lock = threading.Lock()
        self.entries, self.flights = OrderedDict(), {}
        self.bytes = 0
        self.fence = None
        self.last_clock = None
        self.counts = dict(hits=0, builds=0, waits=0, bypasses=0, staleDiscards=0)

    def _observe(self, fence, now):
        if self.fence != fence or self.last_clock is not None and now < self.last_clock:
            self.entries.clear()
            self.bytes = 0
            for flight in self.flights.values():
                flight['invalidated'] = True
                flight['event'].set()
            self.fence = fence
        self.last_clock = now
        expired = [key for key, (until, _) in self.entries.items() if now >= until]
        for key in expired:
            self.bytes -= len(self.entries.pop(key)[1])

    def get(self, key, fence, build, still_current):
        flight_key = (key, fence)
        with self.lock:
            self._observe(fence, self.clock())
            entry = self.entries.get(key)
            if entry is not None:
                self.entries.move_to_end(key)
                self.counts['hits'] += 1
                return entry[1]
            flight = self.flights.get(flight_key)
            if flight is not None:
                self.counts['waits'] += 1
                leader = False
            elif len(self.flights) >= self.maximum_flights:
                self.counts['bypasses'] += 1
                return BYPASS
            else:
                flight = {'event': threading.Event(), 'invalidated': False, 'encoded': None, 'error': None}
                self.flights[flight_key] = flight
                self.counts['builds'] += 1
                leader = True
        if not leader:
            if not flight['event'].wait(self.wait_seconds):
                with self.lock:
                    self.counts['bypasses'] += 1
                return BYPASS
            if flight['invalidated']:
                raise RevisionChanged('Saved market revisions changed during the shared read')
            if flight['error'] is not None:
                raise flight['error']
            return flight['encoded'] if flight['encoded'] is not None else BYPASS
        try:
            value = build()
            try:
                encoded = bounded_json(value, self.maximum_entry_bytes)
            except (TypeError, ValueError, OverflowError):
                encoded = None
            if not still_current():
                raise RevisionChanged('Saved market revisions changed during the read')
            with self.lock:
                if self.fence != fence or flight['invalidated']:
                    raise RevisionChanged('An older inflight read cannot publish across revisions')
                if encoded is not None and len(encoded) <= self.maximum_bytes:
                    self.entries[key] = (self.clock() + self.ttl_seconds, encoded)
                    self.bytes += len(encoded)
                    while len(self.entries) > self.maximum_entries or self.bytes > self.maximum_bytes:
                        _, (_, removed) = self.entries.popitem(last=False)
                        self.bytes -= len(removed)
                    flight['encoded'] = encoded
                else:
                    self.counts['bypasses'] += 1
            return encoded if encoded is not None else value
        except BaseException as error:
            with self.lock:
                flight['error'] = error
                if isinstance(error, RevisionChanged):
                    self.counts['staleDiscards'] += 1
            raise
        finally:
            with self.lock:
                self.flights.pop(flight_key, None)
                flight['event'].set()

    def info(self):
        with self.lock:
            return {**self.counts, 'entries': len(self.entries), 'bytes': self.bytes,
                    'flights': len(self.flights)}

    def invalidate(self):
        with self.lock:
            self._observe(None, self.clock())


def saved_fence(read_db, windows, lookup_version, metric_version, publication_fence=None):
    """Only bounded, exact-key saved revision/receipt reads in one RO snapshot.

    A missing/oversized/unreadable fence bypasses caching, never asserts current.
    Receipt timestamps are hashed unchanged and never replaced by read time.
    """
    option = getattr(sqlite3, 'SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE', None)
    if option is None or not all(hasattr(sqlite3.Connection, name) for name in ('setconfig', 'getconfig')):
        return None
    windows = tuple(sorted(set(windows)))
    if len(windows) > 10 or any(type(value) is not int for value in windows):
        return None
    keys = ['market_source_revision', 'catalog_metadata_revision', *ALL_INPUT_KEYS]
    keys += [prefix + str(window) for prefix in ('market_lookup_receipt:', 'market_metric_receipt:') for window in windows]
    connection = None
    rows = None
    try:
        connection = read_db()
        connection.setconfig(option, True)
        if not connection.getconfig(option) or getattr(connection, '_market_dirty', False):
            return None
        connection.execute('BEGIN')
        rows = connection.execute('SELECT key,typeof(value) AS storage_type,length(CAST(value AS BLOB)) AS bytes,'
            'CASE WHEN length(CAST(value AS BLOB))<=131072 THEN value END AS value '
            'FROM settings WHERE key IN (' + ','.join('?' for _ in keys) + ') ORDER BY key', keys)
        values, size = {}, 0
        for row in rows:
            if row['storage_type'] != 'text' or row['bytes'] is None or row['bytes'] > 131072:
                return None
            size += row['bytes']
            if size > 524288:
                return None
            values[row['key']] = row['value']
        revision = values.get('market_source_revision')
        if not revision or len(revision) > 128 or not any(key.startswith('market_lookup_receipt:') for key in values):
            return None
        rows.close()
        rows = connection.execute("SELECT type FROM sqlite_schema WHERE name='catalog_store_meta' LIMIT 2")
        declared = rows.fetchall()
        rows.close()
        if len(declared) > 1 or declared and declared[0][0] != 'table':
            return None
        native = {'tablePresent': bool(declared), 'generationPresent': False}
        if declared:
            rows = connection.execute('SELECT typeof(value),length(CAST(value AS BLOB)),'
                'CASE WHEN length(CAST(value AS BLOB))<=131072 THEN value END '
                "FROM catalog_store_meta WHERE key='generation' LIMIT 2")
            generation = rows.fetchall()
            rows.close()
            if len(generation) > 1:
                return None
            if generation:
                kind, length, value = generation[0]
                if kind not in ('text', 'integer') or length is None or length > 131072:
                    return None
                size += length
                native = {'tablePresent': True, 'generationPresent': True,
                          'storageType': kind, 'value': value}
        if publication_fence is None and values.get('bulk_derived_writer_abi:v1') is not None:
            # A persisted ABI is not proof that the in-memory host is absent.
            # Missing callback cannot authorize adopted currentness caching.
            return None
        external = publication_fence(connection) if publication_fence is not None else None
        if external is not None and not isinstance(external, dict):
            return None
        external_bytes = bounded_json(external, 131072)
        if external_bytes is None or size + len(external_bytes) > 524288:
            return None
        encoded = bounded_json([2, lookup_version, metric_version, windows, values, native,
                               publication_fence is not None, external], 532480)
        return hashlib.sha256(encoded).hexdigest() if encoded is not None else None
    except (sqlite3.Error, ValueError, TypeError, OverflowError, RuntimeError):
        return None
    finally:
        if connection is not None:
            try:
                if rows is not None:
                    rows.close()
            finally:
                try:
                    if connection.in_transaction:
                        connection.rollback()
                finally:
                    connection.close()


def wrap_market_item_slice(original, read_db, windows, lookup_version, metric_version,
                           clock=time.time, cache=None, publication_fence=None):
    """Leave original SQL/filters/order/count/DTO/projection/auth untouched.

    Only table-v1 is cached. Two revision-rejected reads at most; every uncached
    fallback is revalidated and explicitly stale if revisions change/are unknown.
    """
    signature = inspect.signature(original)
    cache = cache if cache is not None else SliceCache()
    def fence():
        return saved_fence(read_db, windows, lookup_version, metric_version, publication_fence)
    def uncached(args, kwargs):
        before = fence()
        response = original(*args, **kwargs)
        after = fence()
        if before is not None and after == before:
            return response
        reason = 'revision_changed_during_read' if before is not None and after is not None else 'revision_unverified_during_read'
        freshness = {**(response.get('freshness') or {}), 'currentness': reason}
        coverage = {**(response.get('coverage') or {}), 'cacheState': 'stale',
                    'rebuildRequired': True, 'freshness': freshness}
        return {**response, 'cacheState': 'stale', 'rebuildRequired': True,
                'freshness': freshness, 'coverage': coverage}
    @wraps(original)
    def wrapped(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        bound.apply_defaults()
        if bound.arguments.get('projection') != 'market-table-v1':
            return original(*args, **kwargs)
        try:
            key = bounded_json(dict(bound.arguments), 2048)
        except (TypeError, ValueError, OverflowError):
            key = None
        if key is None:
            return uncached(args, kwargs)
        for _ in range(2):
            revision = fence()
            if revision is None:
                cache.invalidate()
                return uncached(args, kwargs)
            try:
                payload = cache.get(key, revision, lambda: original(*args, **kwargs),
                                    lambda: fence() == revision)
            except RevisionChanged:
                continue
            if payload is BYPASS:
                return uncached(args, kwargs)
            if fence() != revision:
                continue
            response = json.loads(payload) if isinstance(payload, bytes) else payload
            # This field means response generation only. Saved freshness,
            # quote currency, captured/updatedAt and receipt times stay exact.
            return {**response, 'generatedAt': int(clock())}
        return uncached(args, kwargs)
    wrapped.cache_info = cache.info
    return wrapped
