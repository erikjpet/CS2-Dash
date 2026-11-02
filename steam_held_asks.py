"""Dedicated Steam ask observations, current values, and one resumable holdings job.

No application imports, schema creation on read, network, legacy history, metrics,
or provider-priority changes. The adapter supplies short-lived SQLite connections
and the already rate-limited single-item fetcher. Install schema explicitly.
"""
import contextlib
import hashlib
import itertools
import json
import math
import re
import sqlite3
import threading
import time
import urllib.parse
import uuid

ABI = 1
MAX_HOLDING_ROWS = 10000
LEASE_SECONDS = 120
DEFAULT_FRESH_SECONDS = 3600
NEGATIVE_SECONDS = 300
CLAIM_BUSY_RETRY_SECONDS = 60
CLAIM_BUSY_WAIT_SECONDS = 1
TABLES = ('steam_ask_control', 'steam_ask_observations', 'steam_ask_current', 'steam_ask_jobs', 'steam_ask_job_items')
SCHEMA = (
    """CREATE TABLE steam_ask_control (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), abi INTEGER NOT NULL,
        schema_sha256 TEXT NOT NULL, installed_at INTEGER NOT NULL,
        retry_at INTEGER NOT NULL DEFAULT 0, cooldown_reason TEXT,
        retry_after TEXT, upstream_retry_at INTEGER,
        lease_token TEXT, lease_until INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE steam_ask_observations (
        receipt_id TEXT PRIMARY KEY, market_hash_name TEXT NOT NULL,
        currency TEXT NOT NULL CHECK(currency='USD'), lowest_ask_cents INTEGER NOT NULL CHECK(lowest_ask_cents>0),
        fetched_at INTEGER NOT NULL CHECK(fetched_at>0), received_at_micros INTEGER NOT NULL,
        request_url TEXT NOT NULL, raw_lowest_price TEXT NOT NULL,
        source TEXT NOT NULL CHECK(source='steam_priceoverview'),
        price_basis TEXT NOT NULL CHECK(price_basis='lowest_ask'),
        market_observed_at INTEGER CHECK(market_observed_at IS NULL), identity_evidence TEXT NOT NULL)""",
    'CREATE INDEX steam_ask_observations_item ON steam_ask_observations(market_hash_name,currency,received_at_micros,receipt_id)',
    """CREATE TABLE steam_ask_current (
        market_hash_name TEXT NOT NULL, currency TEXT NOT NULL CHECK(currency='USD'),
        receipt_id TEXT REFERENCES steam_ask_observations(receipt_id), lowest_ask_cents INTEGER,
        fetched_at INTEGER, received_at_micros INTEGER,
        last_attempt_at INTEGER, last_status TEXT NOT NULL, last_error TEXT,
        retry_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(market_hash_name,currency))""",
    """CREATE TABLE steam_ask_jobs (
        id TEXT PRIMARY KEY, scope TEXT NOT NULL, holdings_fingerprint TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN('queued','running','paused','complete','partial','cancelled')),
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        fresh_seconds INTEGER NOT NULL, total_items INTEGER NOT NULL,
        known_quantity REAL NOT NULL, quantity_complete INTEGER NOT NULL,
        completed_items INTEGER NOT NULL DEFAULT 0, succeeded_items INTEGER NOT NULL DEFAULT 0,
        reused_items INTEGER NOT NULL DEFAULT 0, unpriced_items INTEGER NOT NULL DEFAULT 0,
        reason TEXT, retry_at INTEGER NOT NULL DEFAULT 0)""",
    "CREATE UNIQUE INDEX steam_ask_one_active_job ON steam_ask_jobs((1)) WHERE state IN('queued','running','paused')",
    'CREATE INDEX steam_ask_jobs_scope ON steam_ask_jobs(scope,created_at)',
    """CREATE TABLE steam_ask_job_items (
        job_id TEXT NOT NULL REFERENCES steam_ask_jobs(id), item_key TEXT NOT NULL,
        market_hash_name TEXT, currency TEXT NOT NULL CHECK(currency='USD'), qty REAL,
        state TEXT NOT NULL CHECK(state IN('pending','leased','success','reused','unpriced','failed','cancelled')),
        ordinal INTEGER NOT NULL, issue TEXT, receipt_id TEXT REFERENCES steam_ask_observations(receipt_id),
        attempted_at INTEGER, lease_token TEXT, lease_until INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(job_id,item_key))""",
    'CREATE UNIQUE INDEX steam_ask_job_exact_item ON steam_ask_job_items(job_id,market_hash_name,currency) WHERE market_hash_name IS NOT NULL AND qty IS NOT NULL',
    'CREATE INDEX steam_ask_pending_items ON steam_ask_job_items(job_id,state,ordinal)',
)
SCHEMA_SHA256 = hashlib.sha256('\n'.join(SCHEMA).encode()).hexdigest()


class AskSchemaUnavailable(RuntimeError):
    pass


class AskJobConflict(RuntimeError):
    def __init__(self, job_id):
        self.job_id = job_id
        super().__init__('A holdings ask job is already active: ' + job_id)


def exact_hash(value):
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value.encode('utf-8')) > 512
            or any(ord(c) < 32 or 127 <= ord(c) < 160 for c in value)):
        raise ValueError('Missing or invalid exact Steam market hash')
    return value


def normalize_holdings(rows):
    """Preserve positive numeric quantities; deduplicate only exact hashes."""
    quantities, unresolved = {}, []
    count = 0
    for count, row in enumerate(rows, 1):
        if count > MAX_HOLDING_ROWS:
            raise ValueError('Holdings exceed the explicit 10000-row acquisition bound')
        row = row if isinstance(row, dict) else {}
        try:
            raw_qty = row.get('qty')
            qty = float(raw_qty) if not isinstance(raw_qty, bool) else float('nan')
            if not math.isfinite(qty) or qty < 0:
                raise ValueError('Invalid quantity')
        except (TypeError, ValueError, OverflowError):
            qty = None
        if qty == 0:
            continue  # A zero position is not a holding.
        try:
            name = exact_hash(row.get('marketHashName'))
        except (ValueError, UnicodeError):
            name = None
        if name is None or qty is None:
            unresolved.append({'key': 'unresolved:' + str(count), 'marketHashName': name, 'qty': qty,
                               'displayName': str(row.get('displayName') or '')[:512],
                               'issue': 'invalid_quantity' if qty is None else
                                   row.get('identityIssue') if row.get('identityIssue') in ('no_verified_listing_identity','ambiguous_listing_identity') else 'missing_exact_hash'})
        else:
            quantities.setdefault(name, []).append(qty)
    valid = [{'key': 'hash:' + hashlib.sha256(name.encode()).hexdigest(), 'marketHashName': name,
              'qty': math.fsum(qtys), 'issue': None} for name, qtys in sorted(quantities.items())]
    result = valid + unresolved
    if any(not math.isfinite(row['qty']) for row in result if row['qty'] is not None):
        raise ValueError('Holding quantity sum is not finite')
    fingerprint = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False,
                                           separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    return result, fingerprint


@contextlib.contextmanager
def transaction(conn):
    if conn.in_transaction:
        raise RuntimeError('Ask transaction cannot be nested in an application writer')
    conn.execute('BEGIN IMMEDIATE')
    try:
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def require_schema(conn):
    present = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='steam_ask_control'").fetchone()
    if not present:
        raise AskSchemaUnavailable('Explicit Steam ask schema installation is required')
    row = conn.execute('SELECT abi,schema_sha256 FROM steam_ask_control WHERE singleton=1').fetchone()
    if not row or row['abi'] != ABI or row['schema_sha256'] != SCHEMA_SHA256:
        raise AskSchemaUnavailable('Steam ask schema ABI or checksum differs from this reader')


def install_schema(conn, now, initial_cooldown=None):
    """Explicit additive installer entrypoint; not called by a route or worker."""
    present = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'steam_ask_%'")}
    if present:
        require_schema(conn)
        if present != set(TABLES):
            raise AskSchemaUnavailable('Unexpected or incomplete Steam ask table set')
        if initial_cooldown:
            AskStore(conn).persist_cooldown(initial_cooldown, now, retain_past=True)
        return {'installed': False, 'alreadyInstalled': True, 'schemaSHA256': SCHEMA_SHA256}
    with transaction(conn):
        for statement in SCHEMA:
            conn.execute(statement)
        conn.execute('INSERT INTO steam_ask_control(singleton,abi,schema_sha256,installed_at) VALUES(1,?,?,?)',
                     (ABI, SCHEMA_SHA256, int(now)))
        if initial_cooldown:
            # Enrollment and the known provider floor commit together, before any worker.
            AskStore(conn)._persist_cooldown(initial_cooldown, now, retain_past=True)
    return {'installed': True, 'alreadyInstalled': False, 'schemaSHA256': SCHEMA_SHA256}


class AskStore:
    def __init__(self, conn):
        self.conn = conn
        require_schema(conn)

    def cooldown(self, now, own_token=None):
        row = self.conn.execute('SELECT retry_at,cooldown_reason,retry_after,upstream_retry_at,lease_token,lease_until FROM steam_ask_control WHERE singleton=1').fetchone()
        if row['retry_at'] > now:
            return {'retryAt': row['retry_at'], 'cooldownReason': row['cooldown_reason'],
                    'retryAfter': row['retry_after'], 'upstreamRetryAt': row['upstream_retry_at']}
        token = row['lease_token']
        if token and (token != own_token or token.startswith('orphan:') or row['lease_until'] <= now):
            uncertain = token.startswith('orphan:') or row['lease_until'] <= now
            return {'retryAt': None, 'cooldownReason': 'uncertain_provider_request' if uncertain else 'provider_inflight',
                    'retryAfter': None, 'upstreamRetryAt': None, 'recoveryRequired': uncertain}
        return None

    def reconcile_startup(self, now):
        """Before any new claim/network in this process, retain evidence of an old lease.

        Prefixing its token commits together with the safety floor. This makes
        recovery idempotent across reads/restarts without adding a schema column.
        It is called only once per app process, before workers can claim new work.
        """
        row = self.conn.execute('SELECT lease_token FROM steam_ask_control WHERE singleton=1').fetchone()
        if not row['lease_token'] or row['lease_token'].startswith('orphan:'):
            return
        with transaction(self.conn):
            row = self.conn.execute('SELECT lease_token FROM steam_ask_control WHERE singleton=1').fetchone()
            token = row['lease_token']
            if not token or token.startswith('orphan:'):
                return
            self._persist_cooldown({'retryAt': now + 3600, 'cooldownReason': 'local_interrupted_backoff'}, now)
            self.conn.execute('UPDATE steam_ask_control SET lease_token=? WHERE singleton=1', ('orphan:' + token,))
            self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='interrupted_request',retry_at=0,updated_at=? WHERE state IN('queued','running','paused') AND id IN(SELECT job_id FROM steam_ask_job_items WHERE state='leased' AND lease_token=?)", (now, token))

    def current_receipt(self, name):
        row = self.conn.execute('''SELECT o.* FROM steam_ask_current c JOIN steam_ask_observations o ON o.receipt_id=c.receipt_id
            WHERE c.market_hash_name=? AND c.currency='USD' ''', (name,)).fetchone()
        if not row:
            return None
        return {'success': True, 'status': 'cached', 'cached': True, 'durable': True,
            'receiptId': row['receipt_id'], 'marketHashName': row['market_hash_name'], 'appid': 730,
            'currency': 'USD', 'currencyId': 1, 'lowestAskCents': row['lowest_ask_cents'],
            'lowestAsk': row['lowest_ask_cents'] / 100, 'rawLowestPrice': row['raw_lowest_price'],
            'source': row['source'], 'provider': 'steam', 'priceBasis': row['price_basis'],
            'fetchedAt': row['fetched_at'], 'receivedAtMicros': row['received_at_micros'],
            'marketObservedAt': None, 'requestUrl': row['request_url'], 'identityEvidence': row['identity_evidence'],
            'expiresAt': row['fetched_at'] + 60,
            'listingUrl': 'https://steamcommunity.com/market/listings/730/' + urllib.parse.quote(name, safe='')}

    def claim_single(self, now):
        with transaction(self.conn):
            blocked = self.cooldown(now)
            if blocked:
                return {'action': 'cooldown', **blocked}
            row = self.conn.execute('SELECT lease_token,lease_until FROM steam_ask_control WHERE singleton=1').fetchone()
            if row['lease_token']:
                return {'action': 'busy', 'reason': 'provider_inflight_or_interrupted'}
            token = uuid.uuid4().hex
            self.conn.execute('UPDATE steam_ask_control SET lease_token=?,lease_until=? WHERE singleton=1', (token, now + LEASE_SECONDS))
            return {'action': 'request', 'token': token}

    def finish_single(self, token, name, result, now):
        with transaction(self.conn):
            if result.get('success') is True:
                self._save_success(name, result, now)
            elif result.get('status') == 'governor_deferred':
                pass  # Operator/budget deferral is neither a429 nor a missing quote.
            elif result.get('status') in ('rate_limited', 'cooldown'):
                self._persist_cooldown(result, now)
            elif result.get('status') != 'busy':
                self.conn.execute('''INSERT INTO steam_ask_current(market_hash_name,currency,last_attempt_at,last_status,last_error,retry_at)
                    VALUES(?,'USD',?,?,?,?) ON CONFLICT(market_hash_name,currency) DO UPDATE SET
                    last_attempt_at=excluded.last_attempt_at,last_status=excluded.last_status,last_error=excluded.last_error,retry_at=excluded.retry_at''',
                    (name, now, str(result.get('status') or 'failed')[:40], str(result.get('error') or '')[:240], now + NEGATIVE_SECONDS))
            self.conn.execute('UPDATE steam_ask_control SET lease_token=NULL,lease_until=0 WHERE singleton=1 AND lease_token=?', (token,))

    def clear_interrupted_single_lease(self, now):
        """Explicit recovery only, after operator confirms the prior process ended."""
        with transaction(self.conn):
            if self.conn.execute("SELECT 1 FROM steam_ask_job_items i JOIN steam_ask_jobs j ON j.id=i.job_id WHERE i.state='leased' AND j.state!='cancelled' LIMIT 1").fetchone():
                raise ValueError('Resume the interrupted holdings job instead')
            control = self.conn.execute('SELECT lease_token,lease_until FROM steam_ask_control WHERE singleton=1').fetchone()
            if not control['lease_token']:
                return
            if control['lease_until'] > now:
                raise ValueError('Request lease is still active')
            self.conn.execute("UPDATE steam_ask_job_items SET state='cancelled',lease_token=NULL,lease_until=0 WHERE state='leased' AND lease_until<=? AND job_id IN(SELECT id FROM steam_ask_jobs WHERE state='cancelled')", (now,))
            self.conn.execute('UPDATE steam_ask_control SET lease_token=NULL,lease_until=0 WHERE singleton=1 AND lease_until<=?', (now,))
            if not control['lease_token'].startswith('orphan:'):
                self._persist_cooldown({'retryAt': now + 3600, 'cooldownReason': 'local_interrupted_backoff'}, now)

    def persist_cooldown(self, result, now, retain_past=False):
        with transaction(self.conn):
            self._persist_cooldown(result, now, retain_past=retain_past)

    def _persist_cooldown(self, result, now, retain_past=False):
        retry_at = result.get('retryAt')
        if type(retry_at) is not int or retry_at <= 0 or (retry_at <= now and not retain_past):
            retry_at = now + 3600
            reason = 'local_429_backoff'
        else:
            reason = str(result.get('cooldownReason') or 'upstream_retry_after')[:80]
        retry_after = result.get('retryAfter')
        retry_after = retry_after if isinstance(retry_after, str) and len(retry_after) <= 128 else None
        upstream = result.get('upstreamRetryAt')
        upstream = upstream if type(upstream) is int else None
        # Malformed enormous Retry-After values fail closed without overflowing SQLite.
        # Preserve the raw header while using the largest representable future deadline.
        retry_at = min(retry_at, 9223372036854775807)
        upstream = min(upstream, 9223372036854775807) if upstream is not None else None
        self.conn.execute('''UPDATE steam_ask_control SET retry_at=?,cooldown_reason=?,retry_after=?,upstream_retry_at=?
                             WHERE singleton=1 AND retry_at<=?''', (retry_at, reason, retry_after, upstream, retry_at))
        return self.conn.execute('SELECT retry_at FROM steam_ask_control WHERE singleton=1').fetchone()[0]

    def create_job(self, scope, rows, now, fresh_seconds=DEFAULT_FRESH_SECONDS):
        if not isinstance(scope, str) or not scope or len(scope) > 160:
            raise ValueError('Explicit bounded holdings scope required')
        if type(fresh_seconds) is not int or not 60 <= fresh_seconds <= 86400:
            raise ValueError('Acquisition freshness policy must be 60..86400 seconds')
        input_rows = list(itertools.islice(rows, MAX_HOLDING_ROWS + 1))
        holdings, fingerprint = normalize_holdings(input_rows)
        if not holdings:
            raise ValueError('No nonzero holdings to refresh')
        with transaction(self.conn):
            active = self.conn.execute("SELECT id,scope,holdings_fingerprint,fresh_seconds FROM steam_ask_jobs WHERE state IN('queued','running','paused') LIMIT 1").fetchone()
            if active:
                if active['scope'] == scope and active['holdings_fingerprint'] == fingerprint and active['fresh_seconds'] == fresh_seconds:
                    return self.job(active['id'])
                raise AskJobConflict(active['id'])
            job_id = uuid.uuid4().hex
            known_qty = math.fsum(row['qty'] for row in holdings if row['qty'] is not None)
            self.conn.execute('''INSERT INTO steam_ask_jobs
                (id,scope,holdings_fingerprint,state,created_at,updated_at,fresh_seconds,total_items,known_quantity,quantity_complete)
                VALUES(?,?,?,'queued',?,?,?,?,?,?)''',
                (job_id, scope, fingerprint, now, now, fresh_seconds, len(holdings), known_qty,
                 int(all(row['qty'] is not None for row in holdings))))
            names = [row['marketHashName'] for row in holdings if not row['issue']]
            currents = {}
            for start in range(0, len(names), 256):
                chunk = names[start:start + 256]
                for current in self.conn.execute("SELECT * FROM steam_ask_current WHERE currency='USD' AND market_hash_name IN (" + ','.join('?' for _ in chunk) + ')', chunk):
                    currents[current['market_hash_name']] = dict(current)
            reference_priorities = {}
            for row in input_rows:
                if not isinstance(row, dict):
                    continue
                try:
                    price = float(row.get('priorityReferenceUsd'))
                    if math.isfinite(price) and price > 0:
                        reference_priorities[row.get('marketHashName')] = max(price, reference_priorities.get(row.get('marketHashName'), 0))
                except (TypeError, ValueError, OverflowError):
                    pass
            def priority_key(row):
                current = currents.get(row['marketHashName']) or {}
                unit = ((current.get('lowest_ask_cents') or 0) / 100) or reference_priorities.get(row['marketHashName'], 0)
                exposure = (row['qty'] or 0) * unit
                return (row['issue'] is not None, bool(current.get('receipt_id')), -exposure,
                        current.get('fetched_at') or 0, -(row['qty'] or 0), row['key'])
            priority = sorted(holdings, key=priority_key)
            for ordinal, row in enumerate(priority):
                state, issue, receipt = 'pending', row['issue'], None
                if issue:
                    state = 'unpriced'
                else:
                    current = currents.get(row['marketHashName'])
                    if current and current['receipt_id'] and current['fetched_at'] <= now and now - current['fetched_at'] <= fresh_seconds:
                        state, receipt = 'reused', current['receipt_id']
                    elif current and current['retry_at'] > now:
                        state, issue = 'unpriced', 'negative_cooldown_until_' + str(current['retry_at'])
                self.conn.execute('''INSERT INTO steam_ask_job_items
                    (job_id,item_key,market_hash_name,currency,qty,state,ordinal,issue,receipt_id) VALUES(?,?,?,'USD',?,?,?,?,?)''',
                    (job_id, row['key'], row['marketHashName'], row['qty'], state, ordinal, issue, receipt))
            self._refresh_progress(job_id, now)
            return self.job(job_id)

    def job(self, job_id):
        row = self.conn.execute('SELECT * FROM steam_ask_jobs WHERE id=?', (job_id,)).fetchone()
        if not row:
            return None
        value = dict(row)
        value.update(passComplete=value['state'] in ('complete', 'partial'),
                     completionMeaning='snapshot_pass_all_classified',
                     quoteCoverageComplete=value['completed_items'] == value['total_items'] and not value['unpriced_items'],
                     newFetchCount=value['succeeded_items'], cachedCount=value['reused_items'])
        return value

    def job_status(self, job_id):
        value = self.job(job_id)
        if value:
            clocks = self.conn.execute('''SELECT MIN(o.fetched_at) oldest,MAX(o.fetched_at) newest
                FROM steam_ask_job_items i JOIN steam_ask_observations o ON o.receipt_id=i.receipt_id WHERE i.job_id=?''', (job_id,)).fetchone()
            value.update(oldestFetchedAt=clocks['oldest'], newestFetchedAt=clocks['newest'],
                         currency='USD', source='steam_priceoverview', marketObservedAt=None)
        return value

    def active_job(self):
        row = self.conn.execute("SELECT id FROM steam_ask_jobs WHERE state IN('queued','running','paused') LIMIT 1").fetchone()
        return self.job(row['id']) if row else None

    def _refresh_progress(self, job_id, now):
        counts = {row['state']: row['n'] for row in self.conn.execute(
            'SELECT state,count(*) n FROM steam_ask_job_items WHERE job_id=? GROUP BY state', (job_id,))}
        success, reused = counts.get('success', 0), counts.get('reused', 0)
        unpriced = counts.get('unpriced', 0) + counts.get('failed', 0)
        pending = counts.get('pending', 0) + counts.get('leased', 0)
        self.conn.execute('''UPDATE steam_ask_jobs SET completed_items=?,succeeded_items=?,reused_items=?,
            unpriced_items=?,updated_at=? WHERE id=?''', (success + reused + unpriced, success, reused, unpriced, now, job_id))
        if not pending:
            self.conn.execute("UPDATE steam_ask_jobs SET state=?,reason=NULL,retry_at=0 WHERE id=? AND state!='cancelled'",
                              ('partial' if unpriced else 'complete', job_id))

    def claim(self, now):
        """Commit one durable lease before caller does any network work."""
        with transaction(self.conn):
            job = self.active_job()
            if not job:
                return {'action': 'idle'}
            control = self.conn.execute('SELECT * FROM steam_ask_control WHERE singleton=1').fetchone()
            leased = self.conn.execute("SELECT item_key,lease_until FROM steam_ask_job_items WHERE job_id=? AND state='leased' LIMIT 1", (job['id'],)).fetchone()
            if leased:
                # Never steal an uncertain in-flight request. Explicit resume after restart is required.
                if leased['lease_until'] <= now:
                    self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='interrupted_request',retry_at=0,updated_at=? WHERE id=?", (now, job['id']))
                return {'action': 'paused', 'jobId': job['id'], 'reason': 'inflight_or_interrupted', 'retryAt': None}
            if job['state'] == 'paused' and not job['retry_at']:
                return {'action': 'paused', 'jobId': job['id'], 'reason': job['reason'], 'retryAt': None}
            deadline = max(control['retry_at'], job['retry_at'])
            if deadline > now:
                self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='provider_cooldown',retry_at=?,updated_at=? WHERE id=?", (deadline, now, job['id']))
                return {'action': 'paused', 'jobId': job['id'], 'reason': 'provider_cooldown', 'retryAt': deadline}
            if control['lease_token']:
                return {'action': 'paused', 'jobId': job['id'], 'reason': 'provider_inflight', 'retryAt': None}
            row = self.conn.execute("SELECT * FROM steam_ask_job_items WHERE job_id=? AND state='pending' ORDER BY ordinal LIMIT 1", (job['id'],)).fetchone()
            if not row:
                self._refresh_progress(job['id'], now)
                return {'action': 'idle'}
            current = self.conn.execute("SELECT receipt_id,fetched_at FROM steam_ask_current WHERE market_hash_name=? AND currency='USD'", (row['market_hash_name'],)).fetchone()
            if current and current['receipt_id'] and 0 <= now - current['fetched_at'] <= job['fresh_seconds']:
                self.conn.execute("UPDATE steam_ask_job_items SET state='reused',receipt_id=? WHERE job_id=? AND item_key=?", (current['receipt_id'], job['id'], row['item_key']))
                self._refresh_progress(job['id'], now)
                return {'action': 'reused', 'jobId': job['id']}
            token = uuid.uuid4().hex
            self.conn.execute("UPDATE steam_ask_job_items SET state='leased',lease_token=?,lease_until=?,attempted_at=? WHERE job_id=? AND item_key=?",
                              (token, now + LEASE_SECONDS, now, job['id'], row['item_key']))
            self.conn.execute('UPDATE steam_ask_control SET lease_token=?,lease_until=? WHERE singleton=1', (token, now + LEASE_SECONDS))
            self.conn.execute("UPDATE steam_ask_jobs SET state='running',reason=NULL,retry_at=0,updated_at=? WHERE id=?", (now, job['id']))
            return {'action': 'request', 'jobId': job['id'], 'itemKey': row['item_key'],
                    'marketHashName': row['market_hash_name'], 'currency': 'USD', 'token': token}

    def resume(self, job_id, now):
        with transaction(self.conn):
            job = self.job(job_id)
            if not job or job['state'] not in ('paused', 'queued', 'running'):
                raise ValueError('No resumable active job')
            inflight = self.conn.execute("SELECT lease_until FROM steam_ask_job_items WHERE job_id=? AND state='leased' LIMIT 1", (job_id,)).fetchone()
            control = self.conn.execute('SELECT lease_token,lease_until FROM steam_ask_control WHERE singleton=1').fetchone()
            if (inflight and inflight['lease_until'] > now) or (control['lease_token'] and control['lease_until'] > now):
                raise ValueError('Request lease is still active')
            if (inflight or control['lease_token']) and not str(control['lease_token'] or '').startswith('orphan:'):
                # An interrupted request may have seen an uncommitted 429. Preserve a conservative floor.
                self._persist_cooldown({'retryAt': now + 3600, 'cooldownReason': 'local_interrupted_backoff'}, now)
            self.conn.execute("UPDATE steam_ask_job_items SET state='pending',lease_token=NULL,lease_until=0,issue='resumed_after_interruption' WHERE job_id=? AND state='leased'", (job_id,))
            self.conn.execute('UPDATE steam_ask_control SET lease_token=NULL,lease_until=0 WHERE singleton=1 AND lease_until<=?', (now,))
            self.conn.execute("UPDATE steam_ask_jobs SET state='queued',reason=NULL,retry_at=0,updated_at=? WHERE id=?", (now, job_id))
            # The persisted provider cooldown is deliberately not cleared.
        return self.job(job_id)

    def cancel(self, job_id, now):
        with transaction(self.conn):
            self.conn.execute("UPDATE steam_ask_jobs SET state='cancelled',reason='user_cancelled',updated_at=? WHERE id=? AND state IN('queued','running','paused')", (now, job_id))
            self.conn.execute("UPDATE steam_ask_job_items SET state='cancelled' WHERE job_id=? AND state='pending'", (job_id,))
        return self.job(job_id)

    def _save_success(self, name, result, now):
        name = exact_hash(name)
        cents, fetched, micros = result.get('lowestAskCents'), result.get('fetchedAt'), result.get('receivedAtMicros')
        receipt = result.get('receiptId')
        expected_url = 'https://steamcommunity.com/market/priceoverview/?' + urllib.parse.urlencode(
            {'appid': 730, 'currency': 1, 'l': 'english', 'market_hash_name': name})
        raw = result.get('rawLowestPrice')
        match = re.fullmatch(r'\$(\d{1,9}|\d{1,3}(?:,\d{3}){1,2})\.(\d{2})(?: USD)?', raw) if isinstance(raw, str) and len(raw) <= 40 else None
        raw_cents = int(match[1].replace(',', '')) * 100 + int(match[2]) if match else None
        if (result.get('success') is not True or result.get('marketHashName') != name
                or result.get('currency') != 'USD' or result.get('currencyId') != 1 or result.get('appid') != 730
                or result.get('source') != 'steam_priceoverview' or result.get('priceBasis') != 'lowest_ask'
                or result.get('marketObservedAt') is not None or result.get('requestUrl') != expected_url
                or type(cents) is not int or not 0 < cents <= 99999999999 or raw_cents != cents
                or result.get('lowestAsk') != cents / 100 or type(fetched) is not int or not 0 < fetched <= now + 60
                or type(micros) is not int or not fetched * 1000000 <= micros < (fetched + 1) * 1000000
                or not isinstance(receipt, str) or re.fullmatch(r'[0-9a-f]{32}', receipt) is None):
            raise ValueError('Ask receipt identity, USD amount, provenance, or receipt clock is invalid')
        evidence = 'exact_request_url; response does not normally echo identity'
        values = (receipt, name, 'USD', cents, fetched, micros, expected_url, raw,
                  'steam_priceoverview', 'lowest_ask', None, evidence)
        prior = self.conn.execute('SELECT * FROM steam_ask_observations WHERE receipt_id=?', (receipt,)).fetchone()
        if prior and tuple(prior) != values:
            raise ValueError('Receipt ID was reused for different evidence')
        if prior:
            return receipt  # A cache hit or duplicate commit is not a fresh attempt/observation.
        self.conn.execute('INSERT OR IGNORE INTO steam_ask_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', values)
        self.conn.execute('''INSERT INTO steam_ask_current
            (market_hash_name,currency,receipt_id,lowest_ask_cents,fetched_at,received_at_micros,last_attempt_at,last_status,retry_at)
            VALUES(?,'USD',?,?,?,?,?,'success',0)
            ON CONFLICT(market_hash_name,currency) DO UPDATE SET receipt_id=excluded.receipt_id,
            lowest_ask_cents=excluded.lowest_ask_cents,fetched_at=excluded.fetched_at,
            received_at_micros=excluded.received_at_micros,last_attempt_at=excluded.last_attempt_at,
            last_status='success',last_error=NULL,retry_at=0
            WHERE steam_ask_current.received_at_micros IS NULL OR excluded.received_at_micros>steam_ask_current.received_at_micros
               OR (excluded.received_at_micros=steam_ask_current.received_at_micros AND excluded.receipt_id=steam_ask_current.receipt_id)''',
            (name, receipt, cents, fetched, micros, now))
        return receipt

    def save_success(self, name, result, now):
        with transaction(self.conn):
            return self._save_success(name, result, now)

    def finish(self, claim, result, now):
        with transaction(self.conn):
            item = self.conn.execute('SELECT * FROM steam_ask_job_items WHERE job_id=? AND item_key=?',
                                     (claim['jobId'], claim['itemKey'])).fetchone()
            if not item or item['lease_token'] != claim['token'] or item['state'] != 'leased':
                if result.get('success') is True:
                    self._save_success(claim['marketHashName'], result, now)
                return {'receiptSavedWithoutJobClaim': result.get('success') is True}
            status = result.get('status')
            if result.get('success') is True:
                receipt = self._save_success(claim['marketHashName'], result, now)
                state, issue = ('reused' if result.get('cached') else 'success'), None
            elif status == 'governor_deferred':
                reason = str(result.get('reason') or 'governor_deferred')[:80]
                due = result.get('retryAt')
                retry = due if type(due) is int and due > now and not result.get('operatorPaused') else 0
                job = self.conn.execute('SELECT state FROM steam_ask_jobs WHERE id=?', (claim['jobId'],)).fetchone()
                if not job:
                    raise ValueError('Claimed Steam job is missing')
                self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason=?,retry_at=?,updated_at=? WHERE id=? AND state!='cancelled'", (reason,retry,now,claim['jobId']))
                state, issue, receipt = ('cancelled' if job['state']=='cancelled' else 'pending'), reason, None
            elif status in ('rate_limited', 'cooldown'):
                retry_at = self._persist_cooldown(result, now)
                self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='provider_cooldown',retry_at=?,updated_at=? WHERE id=? AND state!='cancelled'", (retry_at, now, claim['jobId']))
                state, issue, receipt = 'pending', status, None
            elif status == 'busy':
                self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='provider_busy',retry_at=0,updated_at=? WHERE id=? AND state!='cancelled'", (now, claim['jobId']))
                state, issue, receipt = 'pending', 'provider_busy', None
            else:
                state = 'unpriced' if status == 'unavailable' else 'failed'
                issue, receipt = str(result.get('error') or status or 'unknown_result')[:240], None
                self.conn.execute('''INSERT INTO steam_ask_current(market_hash_name,currency,last_attempt_at,last_status,last_error,retry_at)
                    VALUES(?,'USD',?,?,?,?) ON CONFLICT(market_hash_name,currency) DO UPDATE SET
                    last_attempt_at=excluded.last_attempt_at,last_status=excluded.last_status,last_error=excluded.last_error,retry_at=excluded.retry_at''',
                    (claim['marketHashName'], now, state, issue, now + NEGATIVE_SECONDS))
                if state == 'failed':
                    self.conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='request_failed',retry_at=?,updated_at=? WHERE id=? AND state!='cancelled'", (now + NEGATIVE_SECONDS, now, claim['jobId']))
            self.conn.execute('''UPDATE steam_ask_job_items SET state=?,issue=?,receipt_id=?,lease_token=NULL,lease_until=0
                WHERE job_id=? AND item_key=?''', (state, issue, receipt, claim['jobId'], claim['itemKey']))
            self.conn.execute('UPDATE steam_ask_control SET lease_token=NULL,lease_until=0 WHERE singleton=1 AND lease_token=?', (claim['token'],))
            self._refresh_progress(claim['jobId'], now)
            return self.job(claim['jobId'])

    def valuation(self, rows, now, fresh_seconds=DEFAULT_FRESH_SECONDS):
        if type(fresh_seconds) is not int or not 60 <= fresh_seconds <= 86400:
            raise ValueError('Explicit acquisition freshness policy must be 60..86400 seconds')
        holdings, fingerprint = normalize_holdings(rows)
        names = [row['marketHashName'] for row in holdings if not row['issue']]
        quotes = {}
        for start in range(0, len(names), 256):
            chunk = names[start:start + 256]
            placeholders = ','.join('?' for _ in chunk)
            for row in self.conn.execute('SELECT * FROM steam_ask_current WHERE currency=? AND market_hash_name IN (' + placeholders + ')', ['USD'] + chunk):
                quotes[row['market_hash_name']] = dict(row)
        priced, stale, missing, invalid = [], [], [], []
        items = []
        for holding in holdings:
            quote = quotes.get(holding['marketHashName'])
            value = dict(holding, currency='USD', priceBasis='lowest_ask', source='steam_priceoverview',
                         marketObservedAt=None, lowestAsk=None, valueUsd=None, fetchedAt=None)
            if holding['issue']:
                value['state'] = 'unresolved'
                invalid.append(value)
            elif not quote or not quote['receipt_id']:
                value.update(state='unpriced', issue=(quote or {}).get('last_error') or 'ask_not_observed')
                missing.append(value)
            else:
                age = now - quote['fetched_at']
                amount = holding['qty'] * quote['lowest_ask_cents'] / 100
                if not math.isfinite(amount):
                    value.update(state='unresolved', issue='value_overflow')
                    invalid.append(value)
                    items.append(value)
                    continue
                value.update(state='fresh' if 0 <= age <= fresh_seconds else 'stale',
                    lowestAsk=quote['lowest_ask_cents'] / 100, valueUsd=amount,
                    fetchedAt=quote['fetched_at'], receiptId=quote['receipt_id'], ageSeconds=age,
                    lastAttemptStatus=quote['last_status'], lastAttemptAt=quote['last_attempt_at'])
                (priced if value['state'] == 'fresh' else stale).append(value)
            items.append(value)
        observed = priced + stale
        quantity_complete = all(row['qty'] is not None for row in holdings)
        all_observed = bool(holdings) and len(observed) == len(holdings)
        all_fresh = bool(holdings) and len(priced) == len(holdings)
        subtotal = math.fsum(row['valueUsd'] for row in observed)
        fresh_subtotal = math.fsum(row['valueUsd'] for row in priced)
        captures = [row['fetchedAt'] for row in observed]
        return {'lane': 'steam_lowest_ask', 'currency': 'USD', 'source': 'steam_priceoverview',
            'basisLabel': 'Steam lowest ask gross reference; not cash value, proceeds, or available liquidity',
            'holdingsFingerprint': fingerprint, 'marketObservedAt': None,
            'acquisitionFreshnessSeconds': fresh_seconds, 'valuationAsOf': now,
            'coveredSubtotalUsd': subtotal, 'freshCoveredSubtotalUsd': fresh_subtotal,
            'totalValueUsd': subtotal if all_observed else None,
            'freshTotalValueUsd': fresh_subtotal if all_fresh else None,
            'coverage': {'totalItems': len(holdings), 'observedItems': len(observed), 'freshItems': len(priced),
                'staleItems': len(stale), 'missingItems': len(missing), 'unresolvedItems': len(invalid),
                'totalQuantity': math.fsum(row['qty'] for row in holdings) if quantity_complete else None,
                'knownQuantity': math.fsum(row['qty'] for row in holdings if row['qty'] is not None),
                'observedQuantity': math.fsum(row['qty'] for row in observed),
                'freshQuantity': math.fsum(row['qty'] for row in priced),
                'complete': all_observed, 'freshComplete': all_fresh},
            'oldestFetchedAt': min(captures) if captures else None, 'newestFetchedAt': max(captures) if captures else None,
            'items': items}


class AskWorker:
    """One app-process thread; explicit kick, durable job, no independent schedule."""
    def __init__(self, connect, fetch_ask, clock=time.time, before_claim=None):
        self.connect, self.fetch_ask, self.clock = connect, fetch_ask, clock
        self.before_claim = before_claim
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.last_error = None

    def tick(self):
        retry_until = None
        while not self.stop_event.is_set():
            try:
                if self.before_claim is not None:
                    self.before_claim()
                now = int(self.clock())
                with contextlib.closing(self.connect()) as conn:
                    claim = AskStore(conn).claim(now)
                break
            except sqlite3.OperationalError as error:
                # This phase has not called HTTP. Never retry a fetched receipt.
                code = getattr(error, 'sqlite_errorcode', None)
                if type(code) is not int or (code & 0xff) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                if retry_until is None:
                    retry_until = time.monotonic() + CLAIM_BUSY_RETRY_SECONDS
                remaining = retry_until - time.monotonic()
                if remaining <= 0:
                    raise
                # Claim connection/transaction has unwound before this wait.
                if self.stop_event.wait(min(CLAIM_BUSY_WAIT_SECONDS, remaining)):
                    return {'action': 'stopped'}
        else:
            return {'action': 'stopped'}
        if claim['action'] != 'request':
            return claim
        # No SQLite connection/transaction is held across the network boundary.
        try:
            result = self.fetch_ask(claim['marketHashName'], claim)
        except Exception:
            result = {'success': False, 'status': 'failed', 'error': 'Ask fetch failed before a valid receipt was returned'}
        with contextlib.closing(self.connect()) as conn:
            job = AskStore(conn).finish(claim, result, int(self.clock()))
        return {'action': 'processed', 'job': job}

    def kick(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return False
            self.stop_event.clear()
            self.last_error = None
            self.thread = threading.Thread(target=self._run, name='steam-held-asks', daemon=True)
            self.thread.start()
            return True

    def _run(self):
        while not self.stop_event.is_set():
            try:
                result = self.tick()
            except Exception:
                # A failed durable commit leaves a visible lease for explicit recovery.
                self.last_error = 'worker_or_commit_failed'
                try:
                    with contextlib.closing(self.connect()) as conn:
                        with transaction(conn):
                            conn.execute("UPDATE steam_ask_jobs SET state='paused',reason='worker_or_commit_failed',retry_at=0,updated_at=? WHERE state IN('queued','running','paused')", (int(self.clock()),))
                except Exception:
                    pass  # The read API also exposes last_error / whether the worker is alive.
                return
            if result['action'] in ('processed', 'reused'):
                continue
            deadline = result.get('retryAt')
            if result['action'] == 'paused' and type(deadline) is int:
                # Bounded event wait, zero HTTP activity while paused, no busy retry loop.
                self.stop_event.wait(max(1, min(60, deadline - int(self.clock()))))
                continue
            return

    def stop(self):
        self.stop_event.set()
