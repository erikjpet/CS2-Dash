"""Authoritative typed current cash facts, common receipts and useful changes.

No I/O on import. Initialization is an explicit, offline migration operation.
Exact provider hashes are subjects, never guessed catalog IDs. Original history
and catalog identifiers remain retained in the cold legacy store.
"""
import contextlib
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import time

import cash_prices

SCHEMA_VERSION = 1
MAX_ROWS = 150000
MAX_FILE_BYTES = 128 * 1024 * 1024
SOURCE_META = dict(provider=cash_prices.PROVIDER, source=cash_prices.SOURCE,
                   requestUrl=cash_prices.REQUEST_URL, currency='USD',
                   basis=cash_prices.BASIS, contractVersion=1, tradable=0, appId=730,
                   providerTimeSemantics='upstream_updated_at_not_claimed_trade_time')


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(encode(value).encode('utf-8')).hexdigest()


def connection(path, *, write=False):
    """No create-on-connect, no WAL, bounded lock waits and statement work."""
    path = Path(path).resolve()
    if not path.is_file() or path.stat().st_size > MAX_FILE_BYTES:
        raise cash_prices.CashError('Qualified compact cash store is missing or exceeds its capacity')
    conn = sqlite3.connect(path.as_uri() + ('?mode=rw' if write else '?mode=ro'), uri=True,
                           timeout=0.5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA cache_size=-4096')
    if write:
        page_size = conn.execute('PRAGMA page_size').fetchone()[0]
        conn.execute('PRAGMA max_page_count=' + str(MAX_FILE_BYTES // page_size))
    if conn.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION:
        conn.close()
        raise cash_prices.CashError('Unsupported compact cash schema; acquisition blocked')
    if conn.execute('PRAGMA journal_mode').fetchone()[0] != 'delete':
        conn.close()
        raise cash_prices.CashError('Unexpected compact cash journal; acquisition blocked')
    deadline = time.monotonic() + (8 if write else 4)
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    if not write:
        conn.execute('PRAGMA query_only=ON')
    return conn


def initialize(path):
    """Offline only. Refuse overwrite; no legacy DB opens or provider calls."""
    path = Path(path)
    if path.exists():
        raise ValueError('Existing cash authority must be preserved')
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA journal_mode=DELETE')
        conn.execute('PRAGMA synchronous=FULL')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.executescript('''
          BEGIN IMMEDIATE;
          CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
          CREATE TABLE cash_subjects(id INTEGER PRIMARY KEY,market_hash_name TEXT NOT NULL UNIQUE COLLATE BINARY);
          CREATE TABLE cash_captures(id INTEGER PRIMARY KEY,captured_us INTEGER NOT NULL,
            body_sha TEXT NOT NULL,scope_sha TEXT NOT NULL,payload_sha TEXT NOT NULL,full_catalog INTEGER NOT NULL,
            receipt_json TEXT NOT NULL, UNIQUE(captured_us,body_sha,scope_sha));
          CREATE TABLE cash_heads(id INTEGER PRIMARY KEY CHECK(id=1),latest_capture INTEGER NOT NULL,
            full_capture INTEGER,revision TEXT NOT NULL,
            FOREIGN KEY(latest_capture) REFERENCES cash_captures(id),
            FOREIGN KEY(full_capture) REFERENCES cash_captures(id));
          CREATE TABLE cash_facts(subject_id INTEGER PRIMARY KEY,state TEXT NOT NULL,
            price_cents INTEGER,quantity INTEGER,provider_updated_at INTEGER,version TEXT,
            reason TEXT,variant_evidence TEXT,last_changed_capture INTEGER NOT NULL,scoped_capture INTEGER,
            FOREIGN KEY(subject_id) REFERENCES cash_subjects(id),
            FOREIGN KEY(last_changed_capture) REFERENCES cash_captures(id),
            FOREIGN KEY(scoped_capture) REFERENCES cash_captures(id));
          CREATE TABLE cash_changes(subject_id INTEGER NOT NULL,capture_id INTEGER NOT NULL,
            state TEXT NOT NULL,price_cents INTEGER,quantity INTEGER,provider_updated_at INTEGER,
            version TEXT,reason TEXT,variant_evidence TEXT,
            PRIMARY KEY(subject_id,capture_id),
            FOREIGN KEY(subject_id) REFERENCES cash_subjects(id),
            FOREIGN KEY(capture_id) REFERENCES cash_captures(id)) WITHOUT ROWID;
          CREATE VIEW cash_current_receipts AS
            SELECT f.*,s.market_hash_name,c.captured_us,c.body_sha,c.receipt_json
            FROM cash_facts f JOIN cash_subjects s ON s.id=f.subject_id
            LEFT JOIN cash_heads h ON h.id=1
            LEFT JOIN cash_captures complete_capture ON complete_capture.id=h.full_capture
            LEFT JOIN cash_captures scoped ON scoped.id=f.scoped_capture
            JOIN cash_captures c ON c.id=CASE WHEN complete_capture.captured_us IS NOT NULL AND
              (scoped.captured_us IS NULL OR complete_capture.captured_us>=scoped.captured_us)
              THEN complete_capture.id ELSE scoped.id END;
          PRAGMA user_version=1;
          COMMIT;
        ''')
    finally:
        conn.close()


def _facts(row):
    name, state = row.get('marketHashName'), row.get('state')
    if (not isinstance(name, str) or not name or len(name) > 512 or '\0' in name
            or state not in cash_prices.OUTCOME_STATES):
        raise ValueError('Invalid exact current-cash outcome')
    qty, updated, version = row.get('quantityListed'), row.get('providerUpdatedAt'), row.get('version', row.get('providerVersion'))
    if qty is not None and (type(qty) is not int or qty < 0 or qty > 2**63-1):
        raise ValueError('Invalid availability fact')
    if updated is not None and cash_prices._valid_time(updated) is None:
        raise ValueError('Invalid upstream fact clock')
    if version is not None and (not isinstance(version, str) or len(version) > 512):
        raise ValueError('Invalid version evidence')
    cents = None
    if state == 'priced':
        number = cash_prices._decimal(row.get('priceText'))
        scaled = number * 100 if number is not None else None
        if (scaled is None or scaled != scaled.to_integral_value() or scaled > 2**63-1
                or type(qty) is not int or qty <= 0):
            raise ValueError('Positive exact cents and availability are required')
        cents = int(scaled)
    elif row.get('priceText') is not None:
        raise ValueError('An unpriced outcome cannot contain a current price')
    reason = row.get('reason')
    if reason is not None and (not isinstance(reason, str) or len(reason) > 512):
        raise ValueError('Invalid outcome reason')
    # Positive/sold-out single-row values already have typed columns. Retain
    # duplicate/invalid evidence only, where it cannot be reconstructed from
    # those columns. Do not duplicate every ordinary listing as JSON again.
    evidence = row.get('variantEvidence') if state in ('ambiguous', 'invalid') else None
    evidence = encode(evidence) if evidence is not None else None
    if evidence is not None and len(evidence.encode('utf-8')) > 32768:
        raise ValueError('Bounded variant evidence required')
    return name, (state, cents, qty, updated, version, reason, evidence)


def _semantic(values):
    # The upstream update clock changes without a price change. Preserve it in
    # current facts, but do not append an identical price/availability history.
    return values[:3] + values[4:]


def ingest(conn, receipt, outcomes, *, full_catalog=False, revision=None, seed=False):
    """Use caller's transaction: facts, receipt, revision and job commit together.

    A complete response classifies every previously seen subject (absence means
    missing). Scoped responses freshen only their exact subjects. Unchanged full
    feeds advance one common head; no per-item raw/capture rewrite is needed.
    """
    if not conn.in_transaction:
        raise RuntimeError('Capture must share the reserved control transaction')
    if type(outcomes) is not list or len(outcomes) > MAX_ROWS:
        raise ValueError('Bounded complete classified outcomes required')
    captured, body = receipt.get('receivedAtUs'), receipt.get('bodySHA256')
    if (type(captured) is not int or cash_prices._valid_time(captured // 1000000) is None
            or not isinstance(body, str) or not cash_prices.re.fullmatch('[0-9a-f]{64}', body)
            or any(receipt.get(k) != v for k, v in SOURCE_META.items()
                   if k in ('provider', 'source', 'requestUrl', 'currency', 'providerTimeSemantics'))):
        raise ValueError('Qualified source capture receipt required')
    if len(encode(receipt).encode('utf-8')) > 16384:
        raise ValueError('Capture receipt exceeds bound')
    facts = dict(_facts(row) for row in outcomes)
    if len(facts) != len(outcomes):
        raise ValueError('Every exact subject must have one classified outcome')
    scope_sha = fingerprint(['full' if full_catalog else 'exact', sorted(facts)])
    payload_sha = fingerprint(sorted(facts.items()))
    head = conn.execute('SELECT h.*,c.captured_us,c.body_sha FROM cash_heads h JOIN cash_captures c ON c.id=h.latest_capture WHERE h.id=1').fetchone()
    prior = conn.execute('SELECT id,payload_sha FROM cash_captures WHERE captured_us=? AND body_sha=? AND scope_sha=?',
                         (captured, body, scope_sha)).fetchone()
    if prior:
        if prior['payload_sha'] != payload_sha:
            raise cash_prices.CashConflict('A capture receipt cannot be replayed with different normalized facts')
        return dict(reused=True, changedRows=0, historyRows=0, revision=head['revision'], captureId=prior['id'])
    if head and not seed and captured <= head['captured_us']:
        raise cash_prices.CashConflict('An older or conflicting capture cannot replace current facts')
    # A zero-sized complete feed needs explicit provider completeness evidence.
    # This transport cannot distinguish a valid zero-market from a broken feed.
    if full_catalog and not facts:
        raise ValueError('Empty complete feed is not admitted for authoritative publication')
    if full_catalog and receipt.get('catalogCompletenessAdmission', {}).get('admitted') is not True:
        raise ValueError('Global head requires explicit completeness admission')
    cap = conn.execute('INSERT INTO cash_captures(captured_us,body_sha,scope_sha,payload_sha,full_catalog,receipt_json) VALUES(?,?,?,?,?,?)',
                       (captured, body, scope_sha, payload_sha, int(full_catalog), encode(receipt))).lastrowid
    old = {r['market_hash_name']: r for r in conn.execute('SELECT s.market_hash_name,f.* FROM cash_facts f JOIN cash_subjects s ON s.id=f.subject_id')}
    if full_catalog:
        for name in old.keys() - facts.keys():
            facts[name] = ('missing', None, None, None, None, 'not_in_successful_snapshot', None)
    changed = history = 0
    for name, values in facts.items():
        before = old.get(name)
        old_values = tuple(before[k] for k in ('state', 'price_cents', 'quantity', 'provider_updated_at', 'version', 'reason', 'variant_evidence')) if before else None
        if before:
            subject = before['subject_id']
        else:
            conn.execute('INSERT INTO cash_subjects(market_hash_name) VALUES(?) ON CONFLICT DO NOTHING', (name,))
            subject = conn.execute('SELECT id FROM cash_subjects WHERE market_hash_name=?', (name,)).fetchone()[0]
        if old_values != values:
            conn.execute('INSERT INTO cash_facts VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(subject_id) DO UPDATE SET state=excluded.state,price_cents=excluded.price_cents,quantity=excluded.quantity,provider_updated_at=excluded.provider_updated_at,version=excluded.version,reason=excluded.reason,variant_evidence=excluded.variant_evidence,last_changed_capture=excluded.last_changed_capture,scoped_capture=excluded.scoped_capture',
                         (subject, *values, cap, None if full_catalog else cap))
            changed += 1
            if old_values is None or _semantic(old_values) != _semantic(values):
                conn.execute('INSERT INTO cash_changes VALUES(?,?,?,?,?,?,?,?,?)', (subject, cap, *values))
                history += 1
        elif not full_catalog:
            conn.execute('UPDATE cash_facts SET scoped_capture=? WHERE subject_id=?', (cap, subject))
    revision = revision or fingerprint([captured, body, scope_sha])
    if not head or captured > head['captured_us']:
        conn.execute('INSERT INTO cash_heads VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET latest_capture=excluded.latest_capture,full_capture=excluded.full_capture,revision=excluded.revision',
                     (cap, cap if full_catalog else head['full_capture'] if head else None, revision))
        conn.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (cash_prices.REVISION_KEY, revision))
    old_metrics = conn.execute("SELECT value FROM settings WHERE key='compact_metrics'").fetchone()
    metrics = json.loads(old_metrics[0]) if old_metrics else dict(subjects=0,captures=0,usefulChanges=0)
    metrics.update(subjects=len(old.keys() | facts.keys()), captures=metrics['captures'] + 1,
                   usefulChanges=metrics['usefulChanges'] + history,
                   fullCapture=cap if full_catalog else head['full_capture'] if head else None)
    conn.execute("INSERT INTO settings VALUES('compact_metrics',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (encode(metrics),))
    return dict(reused=False, changedRows=changed, historyRows=history, captureId=cap, revision=revision,
                classifiedRows=len(outcomes), fullCatalog=full_catalog)


def legacy_rows(conn, names):
    """Construct the existing read DTO only on demand; never save raw copies."""
    result = []
    for start in range(0, len(names), 256):
        chunk = names[start:start + 256]
        sql = 'SELECT * FROM cash_current_receipts WHERE market_hash_name IN (' + ','.join('?' for _ in chunk) + ')'
        for row in conn.execute(sql, chunk):
            raw = dict(SOURCE_META, marketHashName=row['market_hash_name'], state=row['state'],
                       priceText=str(Decimal(row['price_cents']) / 100) if row['price_cents'] is not None else None,
                       quantityListed=row['quantity'], providerUpdatedAt=row['provider_updated_at'],
                       version=row['version'], reason=row['reason'], receivedAtUs=row['captured_us'],
                       bodySHA256=row['body_sha'])
            if row['variant_evidence'] is not None:
                raw['variantEvidence'] = json.loads(row['variant_evidence'])
            # Provider version is retained evidence. It is never upgraded to a
            # verified cross-market phase binding by a storage reconstruction.
            result.append(dict(name=row['market_hash_name'], provider=cash_prices.PROVIDER,
                               price=row['price_cents'] / 100 if row['price_cents'] is not None else None,
                               raw=encode(raw), updated_at=row['captured_us'] // 1000000))
    return result


def source_metrics(conn):
    row = conn.execute("SELECT value FROM settings WHERE key='compact_metrics'").fetchone()
    return json.loads(row[0]) if row else dict(subjects=0,captures=0,usefulChanges=0,fullCapture=None)
