"""Recover dated research caches into a verified offline clone, never live stores.

Only four compatibility tables are populated. Inventory, settings, the canonical
cash ledger and raw chart history are not modified. Archived aggregate metrics
are retained as dated evidence; they are never converted into invented chart
points. Use ``clone`` first, then ``plan`` and ``apply`` with explicit sources.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import zipfile
import zlib

HOT_MEMBER = 'compact-price-store-candidate/current-hot.sqlite'
COLD_MEMBER = 'compact-price-store-candidate/cold-evidence.sqlite'
TABLES = ('market_item_prices', 'price_quotes', 'item_market_metrics', 'market_item_lookup')
RECEIPTS = 'research_recovery_receipts'
MAX_MEMBER_BYTES = 512 * 1024 * 1024
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MARKER_SUFFIX = '.research-clone.json'
VERSION = 1

PRICE_COLUMNS = ('name', 'provider', 'price', 'median', 'solid_price', 'lowest_sell_order',
                 'highest_buy_order', 'spread_pct', 'stability_pct', 'confidence_pct',
                 'volume', 'source', 'basis', 'kind', 'url', 'raw', 'observed_at', 'updated_at')
QUOTE_COLUMNS = ('name', 'provider', 'price', 'median', 'volume', 'url', 'raw', 'updated_at')
METRIC_COLUMNS = ('name', 'provider', 'window_days', 'points', 'clean_points', 'edge_case_count',
                  'volatility_pct', 'range_pct', 'trend_pct', 'first_price', 'latest_price',
                  'first_ts', 'last_ts', 'current_price', 'prior_price', 'price_source', 'updated_at', 'raw')
LOOKUP_COLUMNS = ('window_days', 'name', 'market_hash_name', 'kind', 'item_type', 'weapon',
                  'collection', 'rarity', 'latest_price', 'prior_price', 'change_pct',
                  'change_dollar', 'latest_at', 'source', 'volatility_pct', 'history_points',
                  'edge_case_count', 'groups_json', 'search_text', 'metric_updated_at',
                  'steam_price', 'steam_lowest_sell_order', 'steam_highest_buy_order',
                  'steam_spread_pct', 'steam_stability_pct', 'price_basis', 'confidence_pct',
                  'provider_prices_json')
EXPECTED = dict(zip(TABLES, (PRICE_COLUMNS, QUOTE_COLUMNS, METRIC_COLUMNS, LOOKUP_COLUMNS)))
ATOM_FIELDS = ('name', 'provider', 'source', 'basis', 'kind', 'url')


class RecoveryError(ValueError):
    pass


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            hasher.update(block)
    return hasher.hexdigest()


def read_db(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    return conn


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def columns(conn, table):
    return {r['name'] for r in conn.execute('PRAGMA table_info(' + quote_identifier(table) + ')')}


def require_columns(conn, table, expected):
    if not set(expected).issubset(columns(conn, table)):
        raise RecoveryError('Incompatible required schema: ' + table)


def count(conn, table):
    return conn.execute('SELECT COUNT(*) FROM ' + quote_identifier(table)).fetchone()[0]


def marker_path(target):
    target = Path(target)
    return target.with_name(target.name + MARKER_SUFFIX)


def write_marker(target, value):
    target = Path(target).resolve()
    value = dict(value, target=str(target), sha256=digest(target))
    marker = marker_path(target)
    temporary = marker.with_suffix(marker.suffix + '.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    os.replace(temporary, marker)


def clone_database(source, target):
    """Make an online-consistent, separately fenced clone; source is read-only."""
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target or target.exists() or marker_path(target).exists():
        raise RecoveryError('Clone target must be a new separate file')
    if not source.is_file():
        raise RecoveryError('Existing source database required')
    target.parent.mkdir(parents=True, exist_ok=True)
    opened = read_db(source)
    destination = sqlite3.connect(target)
    deadline = time.monotonic() + 120
    def progress(status, remaining, total):
        if time.monotonic() > deadline:
            raise RecoveryError('Clone deadline exceeded')
    try:
        opened.backup(destination, pages=1024, progress=progress, sleep=.05)
        if destination.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RecoveryError('Clone integrity check failed')
        # A WAL source copies its journal-mode header. Consolidate only this
        # newly created offline destination before issuing its identity fence.
        if destination.execute('PRAGMA journal_mode=DELETE').fetchone()[0].lower() != 'delete':
            raise RecoveryError('Offline clone could not enter a consolidated journal mode')
    finally:
        opened.close()
        destination.close()
    write_marker(target, {'version': VERSION, 'source': str(source), 'clonedAt': int(time.time()),
                          'sourceReadOnly': True, 'offlineClone': True, 'journalMode': 'delete'})
    return {'status': 'cloned', 'target': str(target), 'sha256': digest(target), 'quickCheck': 'ok'}


def validate_clone(target):
    target = Path(target).resolve()
    try:
        marker = json.loads(marker_path(target).read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise RecoveryError('Use clone first; verified offline clone marker required') from exc
    if (marker.get('version') != VERSION or marker.get('offlineClone') is not True
            or marker.get('sourceReadOnly') is not True or marker.get('target') != str(target)
            or Path(marker.get('source', '')).resolve() == target
            or marker.get('sha256') != digest(target)):
        raise RecoveryError('Offline clone identity changed; no recovery applied')
    if Path(str(target) + '-wal').exists():
        raise RecoveryError('Offline clone has a WAL; close all users before recovery')
    return marker


@contextmanager
def open_sources(derived, archive):
    derived, archive = Path(derived).resolve(), Path(archive).resolve()
    opened = []
    try:
        source = read_db(derived)
        opened.append(source)
        members = {}
        manifest = {'derived': {'file': derived.name, 'sha256': digest(derived)},
                    'archive': {'file': archive.name, 'sha256': digest(archive)}, 'members': {}}
        with zipfile.ZipFile(archive) as compressed:
            for label, member in [('hot', HOT_MEMBER), ('cold', COLD_MEMBER)]:
                info = compressed.getinfo(member)
                if info.file_size <= 0 or info.file_size > MAX_MEMBER_BYTES:
                    raise RecoveryError('Archive member size outside recovery bound')
                raw = compressed.read(member)
                manifest['members'][label] = {'member': member, 'bytes': len(raw),
                                             'sha256': hashlib.sha256(raw).hexdigest()}
                conn = sqlite3.connect(':memory:')
                opened.append(conn)
                if not hasattr(conn, 'deserialize'):
                    raise RecoveryError('Archive recovery requires SQLite deserialize support')
                conn.deserialize(raw)
                conn.row_factory = sqlite3.Row
                conn.execute('PRAGMA query_only=ON')
                members[label] = conn
        yield source, members['hot'], members['cold'], manifest
    except (KeyError, sqlite3.Error, zipfile.BadZipFile) as exc:
        raise RecoveryError('Recovery source cannot be read with the expected schema') from exc
    finally:
        for conn in opened:
            conn.close()


def source_plan(target_conn, derived, hot, cold, manifest):
    for table, fields in EXPECTED.items():
        require_columns(target_conn, table, fields)
    require_columns(derived, 'item_market_metrics', METRIC_COLUMNS)
    require_columns(derived, 'lookup_common', ('common_id', 'name', 'market_hash_name'))
    require_columns(derived, 'lookup_windows', ('common_id', 'window_days', 'name'))
    require_columns(hot, 'quotes', PRICE_COLUMNS)
    require_columns(hot, 'atoms', ('id', 'value'))
    require_columns(cold, 'evidence', ('id', 'compressed', 'body'))
    quote_count = count(hot, 'quotes')
    metric_count = count(derived, 'item_market_metrics')
    lookup_count = count(derived, 'lookup_windows')
    if quote_count > 1000000 or metric_count > 2000000 or lookup_count > 2000000:
        raise RecoveryError('Source rows exceed bounded recovery limits')
    missing = 0
    for field in ATOM_FIELDS:
        missing += hot.execute('SELECT COUNT(*) FROM quotes q LEFT JOIN atoms a ON a.id=q.'
                               + field + ' WHERE q.' + field + ' IS NOT NULL AND a.id IS NULL').fetchone()[0]
    raw_refs = {r[0] for r in hot.execute('SELECT DISTINCT raw FROM quotes WHERE raw IS NOT NULL')}
    evidence_ids = {r[0] for r in cold.execute('SELECT id FROM evidence')}
    if missing or not raw_refs.issubset(evidence_ids):
        raise RecoveryError('Unresolved archived quote atoms or evidence references')
    if quote_count != hot.execute('SELECT COUNT(*) FROM (SELECT name,provider FROM quotes GROUP BY name,provider)').fetchone()[0]:
        raise RecoveryError('Duplicate archived quote identity')
    if lookup_count != derived.execute('SELECT COUNT(*) FROM (SELECT window_days,name FROM lookup_windows GROUP BY window_days,name)').fetchone()[0]:
        raise RecoveryError('Duplicate archived lookup identity')
    joined = derived.execute('SELECT COUNT(*) FROM lookup_windows w JOIN lookup_common c ON c.common_id=w.common_id').fetchone()[0]
    if joined != lookup_count:
        raise RecoveryError('Unresolved archived lookup references')
    expected = dict(zip(TABLES, (quote_count, quote_count, metric_count, lookup_count)))
    existing = {table: count(target_conn, table) for table in TABLES}
    asof = {
        'quotesObservedAt': list(hot.execute('SELECT MIN(observed_at),MAX(observed_at) FROM quotes').fetchone()),
        'quotesUpdatedAt': list(hot.execute('SELECT MIN(updated_at),MAX(updated_at) FROM quotes').fetchone()),
        'metricsUpdatedAt': list(derived.execute('SELECT MIN(updated_at),MAX(updated_at) FROM item_market_metrics').fetchone()),
        'metricsHistoryFirstAt': list(derived.execute('SELECT MIN(first_ts),MAX(first_ts) FROM item_market_metrics').fetchone()),
        'metricsHistoryLastAt': list(derived.execute('SELECT MIN(last_ts),MAX(last_ts) FROM item_market_metrics').fetchone()),
    }
    windows = [{'days': r[0], 'rows': r[1]} for r in derived.execute('SELECT window_days,COUNT(*) FROM lookup_windows GROUP BY window_days ORDER BY window_days')]
    fingerprint = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {'version': VERSION, 'sourceFingerprint': fingerprint, 'sources': manifest,
            'expectedRows': expected, 'existingRows': existing, 'asOf': asof,
            'windows': windows, 'rawSteamChartRowsImported': 0,
            'personalTablesModified': False, 'canonicalLedgerModified': False,
            'emptyTargetRequired': True, 'blockedByExistingRows': any(existing.values())}


def plan_recovery(target, derived, archive):
    conn = read_db(target)
    try:
        with open_sources(derived, archive) as sources:
            return source_plan(conn, *sources)
    finally:
        conn.close()


def recovery_status(conn):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (RECEIPTS,)).fetchone():
        return None
    row = conn.execute('SELECT receipt_json FROM ' + RECEIPTS + ' ORDER BY applied_at DESC LIMIT 1').fetchone()
    return json.loads(row[0]) if row else None


def insert_rows(conn, table, fields, rows):
    sql = 'INSERT INTO ' + quote_identifier(table) + '(' + ','.join(map(quote_identifier, fields)) + ') VALUES(' + ','.join('?' for _ in fields) + ')'
    batch = []
    for row in rows:
        batch.append(tuple(row))
        if len(batch) == 1000:
            conn.executemany(sql, batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)


def apply_recovery(target, derived, archive):
    """Atomic additive import to this helper's unchanged offline clone only."""
    target = Path(target).resolve()
    marker = validate_clone(target)
    with open_sources(derived, archive) as (derived_conn, hot, cold, manifest):
        read = read_db(target)
        try:
            plan = source_plan(read, derived_conn, hot, cold, manifest)
            previous = recovery_status(read)
        finally:
            read.close()
        if previous and previous['sourceFingerprint'] == plan['sourceFingerprint'] and previous['importedRows'] == plan['existingRows']:
            return dict(previous, status='already_applied')
        if plan['blockedByExistingRows']:
            raise RecoveryError('Research target tables are not empty; no existing data overwritten')
        # Recheck after source validation; another local user must not alter the clone.
        validate_clone(target)
        conn = sqlite3.connect(target.as_uri() + '?mode=rw', uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('BEGIN IMMEDIATE')
            if any(count(conn, table) for table in TABLES):
                raise RecoveryError('Research target changed before transaction')
            conn.execute('CREATE TABLE IF NOT EXISTS ' + RECEIPTS + '(source_fingerprint TEXT PRIMARY KEY,applied_at INTEGER NOT NULL,receipt_json TEXT NOT NULL)')
            @lru_cache(maxsize=2048)
            def raw_payload(identity):
                if identity is None:
                    return None
                row = cold.execute('SELECT compressed,body FROM evidence WHERE id=?', (identity,)).fetchone()
                if row is None:
                    raise RecoveryError('Archived quote evidence missing')
                payload = row['body']
                if row['compressed']:
                    decoder = zlib.decompressobj()
                    payload = decoder.decompress(payload, MAX_PAYLOAD_BYTES + 1)
                    if len(payload) > MAX_PAYLOAD_BYTES or not decoder.eof or decoder.unconsumed_tail:
                        raise RecoveryError('Archived quote evidence exceeds payload bound')
                if len(payload) > MAX_PAYLOAD_BYTES:
                    raise RecoveryError('Archived quote evidence exceeds payload bound')
                return payload.decode('utf-8')
            select = ','.join('a_' + field + '.value AS ' + field if field in ATOM_FIELDS else 'q.' + field for field in PRICE_COLUMNS)
            joins = ' '.join('LEFT JOIN atoms a_' + field + ' ON a_' + field + '.id=q.' + field for field in ATOM_FIELDS)
            query = 'SELECT ' + select + ' FROM quotes q ' + joins + ' ORDER BY q.legacy_rowid'
            def price_rows():
                for row in hot.execute(query):
                    value = dict(row)
                    value['raw'] = raw_payload(value['raw'])
                    if not isinstance(value['name'], str) or not isinstance(value['provider'], str):
                        raise RecoveryError('Archived quote identity is not text')
                    yield [value[key] for key in PRICE_COLUMNS]
            insert_rows(conn, 'market_item_prices', PRICE_COLUMNS, price_rows())
            # Copy the original recorded quotes, never turn them into raw chart points.
            quote_select = ','.join(quote_identifier(key) for key in QUOTE_COLUMNS)
            conn.execute('INSERT INTO price_quotes(' + quote_select + ') SELECT ' + quote_select + ' FROM market_item_prices')
            metric_select = ','.join(quote_identifier(key) for key in METRIC_COLUMNS)
            insert_rows(conn, 'item_market_metrics', METRIC_COLUMNS, derived_conn.execute('SELECT ' + metric_select + ' FROM item_market_metrics ORDER BY window_days,name,provider'))
            window_fields = {'window_days', 'name', 'prior_price', 'change_pct', 'change_dollar', 'volatility_pct',
                             'history_points', 'edge_case_count', 'groups_json', 'metric_updated_at'}
            lookup_select = ','.join(('w.' if key in window_fields else 'c.') + key for key in LOOKUP_COLUMNS)
            insert_rows(conn, 'market_item_lookup', LOOKUP_COLUMNS, derived_conn.execute('SELECT ' + lookup_select + ' FROM lookup_windows w JOIN lookup_common c ON c.common_id=w.common_id ORDER BY w.window_days,w.name'))
            fts = conn.execute("SELECT sql FROM sqlite_master WHERE name='market_item_lookup_fts'").fetchone()
            if fts:
                if "content='market_item_lookup'" not in fts[0].replace(' ', ''):
                    raise RecoveryError('Unexpected lookup FTS contract')
                conn.execute("INSERT INTO market_item_lookup_fts(market_item_lookup_fts) VALUES('rebuild')")
            imported = {table: count(conn, table) for table in TABLES}
            if imported != plan['expectedRows']:
                raise RecoveryError('Recovered row-count proof failed')
            receipt = dict(plan, status='applied', appliedAt=int(time.time()), importedRows=imported,
                           ftsRebuilt=bool(fts), sourceEvidenceRetained=True,
                           meaning='Dated archived snapshots and aggregate metrics, not current canonical cash or raw Steam chart history')
            conn.execute('INSERT INTO ' + RECEIPTS + ' VALUES(?,?,?)',
                         (plan['sourceFingerprint'], receipt['appliedAt'], json.dumps(receipt, sort_keys=True)))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
    write_marker(target, dict(marker, lastRecoveryFingerprint=receipt['sourceFingerprint']))
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    clone = actions.add_parser('clone')
    clone.add_argument('--source', required=True)
    clone.add_argument('--target', required=True)
    for action in ['plan', 'apply']:
        command = actions.add_parser(action)
        command.add_argument('--target', required=True)
        command.add_argument('--derived', required=True)
        command.add_argument('--archive', required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == 'clone':
            result = clone_database(args.source, args.target)
        else:
            result = (plan_recovery if args.action == 'plan' else apply_recovery)(args.target, args.derived, args.archive)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (RecoveryError, sqlite3.Error, OSError, UnicodeError) as error:
        print(json.dumps({'status': 'refused', 'error': str(error)}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
