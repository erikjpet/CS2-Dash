"""Ephemeral legacy holdings/identity cache; no prices, schema, network or I/O at import."""
from collections import OrderedDict
import json
import threading
import uuid


class HoldingsCacheError(RuntimeError):
    pass


class HoldingsChanged(HoldingsCacheError):
    pass


class _Snapshot:
    """Small read facade: every reader-created cursor is closed at the boundary."""
    def __init__(self, connection):
        self.connection = connection
        self.cursors = []

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def execute(self, *args, **kwargs):
        cursor = self.connection.execute(*args, **kwargs)
        self.cursors.append(cursor)
        return cursor

    def cursor(self, *args, **kwargs):
        cursor = self.connection.cursor(*args, **kwargs)
        self.cursors.append(cursor)
        return cursor

    def close(self):
        first = None
        for cursor in self.cursors:
            try:
                cursor.close()
            except BaseException as error:
                first = first or error
        self.cursors.clear()
        if first is not None:
            raise first


def _json_value(value, depth=0):
    if depth > 64:
        raise HoldingsCacheError('Unexpectedly deep holdings result')
    if value is None or type(value) in (str, bool, int, float):
        return
    if type(value) is list:
        for item in value:
            _json_value(item, depth + 1)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _json_value(item, depth + 1)
        return
    raise HoldingsCacheError('Unsupported holdings result type; no coercion or stale fallback')


def _serialize(value):
    if (type(value) is not tuple or len(value) != 3 or type(value[0]) is not str
            or type(value[1]) is not list or type(value[2]) is not list):
        raise HoldingsCacheError('Expected the original cash_held_rows tuple')
    for part in value:
        _json_value(part)
    # A private transient serialization, not an external API or durable store.
    # Preserve float NaN/infinities for the existing holdings validator to classify.
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=True).encode('ascii')


def _copy(payload):
    value = json.loads(payload)
    return value[0], value[1], value[2]


class HoldingsCache:
    def __init__(self, connection_factory, reader, max_entries=4, max_serialized_bytes=8 * 1024 * 1024):
        if (not callable(connection_factory) or not callable(reader)
                or type(max_entries) is not int or max_entries < 1
                or type(max_serialized_bytes) is not int or max_serialized_bytes < 1):
            raise ValueError('Callable factory/reader and finite positive cache bounds required')
        self.factory, self.reader = connection_factory, reader
        self.max_entries, self.max_bytes = max_entries, max_serialized_bytes
        self._lock = threading.RLock()
        self._connection = self._broken = None
        self._nonce = uuid.uuid4().hex
        self._incarnation = 0
        self._version = None
        self._entries = OrderedDict()
        self._bytes = 0
        self._closed = False

    def _clear(self):
        self._entries.clear()
        self._bytes = 0
        self._version = None

    @staticmethod
    def _scalar(connection, sql):
        cursor = connection.execute(sql)
        try:
            row = cursor.fetchone()
            if row is None or type(row[0]) is not int:
                raise HoldingsCacheError('Unknown SQLite cache validation result')
            return row[0]
        finally:
            cursor.close()

    def _discard(self):
        self._clear()
        connection = self._connection or self._broken
        self._connection = self._broken = None
        if connection is None:
            return
        try:
            if connection.in_transaction:
                connection.rollback()
        finally:
            try:
                connection.close()
            except BaseException:
                self._broken = connection
                raise HoldingsCacheError('Broken cache connection closure unconfirmed; no reuse')

    def _open(self):
        if self._broken is not None:
            raise HoldingsCacheError('Broken cache connection closure unconfirmed; replace or close the cache')
        if self._connection is None:
            self._connection = self.factory()
            self._incarnation += 1
        connection = self._connection
        if connection.in_transaction or self._scalar(connection, 'PRAGMA query_only') != 1:
            raise HoldingsCacheError('Cache requires an idle read-only connection')
        return connection

    def _data_version(self, connection):
        if connection.in_transaction:
            raise HoldingsCacheError('data_version must be observed outside a read snapshot')
        value = self._scalar(connection, 'PRAGMA data_version')
        if value < 0:
            raise HoldingsCacheError('Unknown SQLite data_version')
        return value

    def read(self, portfolio_id, ignored=None):
        """Return a detached original tuple; the optional caller connection is ignored."""
        with self._lock:
            if self._closed:
                raise HoldingsCacheError('Holdings cache is closed')
            snapshot = None
            try:
                if type(portfolio_id) is not str or not 0 < len(portfolio_id) <= 128:
                    raise HoldingsCacheError('An explicit portfolio ID is required')
                connection = self._open()
                before = self._data_version(connection)
                if self._version != before:
                    self._clear()
                    self._version = before
                cached = self._entries.get(portfolio_id)
                if cached is not None:
                    result = _copy(cached)
                    if self._data_version(connection) != before:
                        raise HoldingsChanged('External commit during cached copy; nothing returned')
                    self._entries.move_to_end(portfolio_id)
                    return result
                snapshot = _Snapshot(connection)
                snapshot.execute('BEGIN')
                value = self.reader(portfolio_id, snapshot)
                if not connection.in_transaction:
                    raise HoldingsCacheError('Reader changed ownership of its snapshot')
                snapshot.close()
                snapshot = None
                connection.rollback()
                if self._data_version(connection) != before:
                    raise HoldingsChanged('External commit during holdings snapshot; nothing returned')
                payload = _serialize(value)
                result = _copy(payload)
                if self._data_version(connection) != before:
                    raise HoldingsChanged('External commit during result copy; nothing returned')
                if len(payload) <= self.max_bytes:
                    while self._entries and (len(self._entries) >= self.max_entries or self._bytes + len(payload) > self.max_bytes):
                        _, old = self._entries.popitem(last=False)
                        self._bytes -= len(old)
                    self._entries[portfolio_id] = payload
                    self._bytes += len(payload)
                return result
            except BaseException as error:
                cleanup_error = None
                try:
                    if snapshot is not None:
                        snapshot.close()
                except BaseException as failed:
                    cleanup_error = failed
                try:
                    self._discard()
                except BaseException as failed:
                    cleanup_error = failed
                if cleanup_error is not None:
                    raise HoldingsCacheError('Cache failed closed; cleanup/connection closure unconfirmed') from cleanup_error
                if isinstance(error, HoldingsCacheError):
                    raise
                if not isinstance(error, Exception):
                    raise
                raise HoldingsCacheError('Holdings cache failed closed; no stale value returned') from error

    def identity_revision(self):
        """Opaque read-only epoch for the exact same monitor used by read()."""
        with self._lock:
            if self._closed:
                raise HoldingsCacheError('Holdings cache is closed')
            try:
                connection = self._open()
                version = self._data_version(connection)
                if self._version != version:
                    self._clear()
                    self._version = version
                # A reopened monitor may reuse SQLite's numeric version. Both
                # process/cache replacement and connection replacement change
                # this opaque token without comparing across connections.
                return self._nonce + ':' + str(self._incarnation) + ':' + str(version)
            except BaseException as error:
                try:
                    self._discard()
                except BaseException as cleanup_error:
                    raise HoldingsCacheError('Identity revision failed closed; connection closure unconfirmed') from cleanup_error
                if isinstance(error, HoldingsCacheError) or not isinstance(error, Exception):
                    raise
                raise HoldingsCacheError('Identity revision unavailable; no cached value retained') from error

    def close(self):
        with self._lock:
            self._closed = True
            self._discard()
