"""Explicit, holdings-only Skinport cash reference lane.

No I/O occurs at import. The host supplies transport, guarded SQLite connections,
holdings, capacity checks and its writer lock. Existing Steam/snapshot lanes are
not consulted for cash valuation and no schema is created here.
"""
import contextlib
import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import email.utils
import hashlib
import json
import math
import re
import threading
import time
import uuid

PROVIDER = 'skinport_live'
SOURCE = 'skinport:items:730:USD:tradable0'
REQUEST_URL = 'https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0'
BASIS = 'lowest_listed_price'
CURRENCY = 'USD'
FRESHNESS_SECONDS = 600
ADMISSION_SECONDS = 300
UNCERTAIN_SECONDS = 3600
INITIAL_NOT_BEFORE = 1792559748  # Public qualification HTTP Date + five minutes.
MAX_HOLDING_ROWS = 10000
BATCH_ROWS = 100
CONTROL_KEY = 'cash_fetch_control:skinport_live:v1'
REVISION_KEY = 'cash_source_revision:skinport_live'
ACTIVE_STATES = {'queued', 'fetching', 'storing'}
OUTCOME_STATES = {'priced', 'missing', 'no_listings', 'ambiguous', 'invalid'}


class CashError(RuntimeError):
    status = 503


class CashAdmissionBusy(CashError):
    """Known writer-lock refusal before opening SQLite or reserving a request."""
    pass


class CashConflict(CashError):
    status = 409


class CashNotFound(CashError):
    status = 404


class CashCooldown(CashError):
    status = 429

    def __init__(self, retry_at, reason):
        super().__init__('Skinport refresh is paused until the saved retry deadline')
        self.retry_at = retry_at
        self.reason = reason


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _sha(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _decimal(value, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
            return None
        if not math.isfinite(float(number)):
            return None
        return number
    except (ValueError, InvalidOperation, OverflowError):
        return None


def _money(number):
    try:
        result = float(number.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    except (ValueError, InvalidOperation, OverflowError):
        raise ValueError('Cash value is outside the supported numeric range')
    if not math.isfinite(result):
        raise ValueError('Cash value is outside the supported numeric range')
    return result


def _utc(seconds):
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def freeze_holdings(portfolio_id, scope, rows, display_holdings):
    if scope != 'portfolio:' + portfolio_id or len(rows) > MAX_HOLDING_ROWS:
        raise ValueError('Invalid or oversized portfolio scope')
    frozen, hashes = [], set()
    for row in rows:
        display = row.get('displayName')
        name = row.get('marketHashName')
        if not isinstance(display, str) or not display or len(display) > 1024:
            raise ValueError('Invalid saved holding name')
        identity_issue = row.get('identityIssue')
        if not isinstance(name, str) or not name or len(name) > 512 or '\x00' in name:
            name = None
            identity_issue = identity_issue or 'no_verified_listing_identity'
        quantity = _decimal(row.get('qty'), allow_zero=True)
        quantity_issue = None if quantity is not None else 'invalid_holding_quantity'
        quantity_key = str(quantity.normalize()) if quantity is not None else None
        value = {'displayName': display, 'marketHashName': name,
                 'qty': float(quantity) if quantity is not None else None,
                 'quantityKey': quantity_key, 'identityIssue': identity_issue,
                 'quantityIssue': quantity_issue}
        frozen.append(value)
        if name and not identity_issue and quantity is not None and quantity > 0:
            hashes.add(name)
    scope_rows = sorted([[r['displayName'], r['marketHashName'], r['quantityKey'],
                          r['identityIssue'], r['quantityIssue']] for r in frozen], key=_json)
    if (not isinstance(display_holdings, list) or len(display_holdings) > MAX_HOLDING_ROWS
            or any(not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str)
                   or not pair[0] or _decimal(pair[1]) is None for pair in display_holdings)):
        raise ValueError('Invalid normalized visible-portfolio projection')
    return {'portfolioId': portfolio_id, 'holdingsFingerprint': _sha([portfolio_id, scope_rows]),
            'rows': frozen, 'hashes': sorted(hashes), 'displayHoldings': display_holdings}


def _valid_time(value):
    return value if type(value) is int and 0 < value < 253402300800 else None


def _receipt(result, requested):
    if type(result) is not dict or result.get('status') != 200:
        raise ValueError('A successful Skinport transport result is required')
    if result.get('requestUrl') != REQUEST_URL:
        raise ValueError('Skinport transport URL does not match the fixed USD source')
    received_us = result.get('receivedAtUs')
    if type(received_us) is not int or not _valid_time(received_us // 1000000):
        raise ValueError('Invalid acquisition clock')
    digest = result.get('bodySHA256')
    if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
        raise ValueError('Missing feed SHA256')
    if result.get('currencyMismatches') != 0 or result.get('malformed') != 0:
        raise ValueError('Skinport response currency/schema qualification failed')
    if type(result.get('rowCount')) is not int or not 0 <= result['rowCount'] <= 150000:
        raise ValueError('Invalid feed row count')
    if type(result.get('distinctNames')) is not int or not 0 <= result['distinctNames'] <= result['rowCount']:
        raise ValueError('Invalid feed distinct-name count')
    counts = result.get('matchCounts')
    if not isinstance(counts, dict) or set(counts) != set(requested):
        raise ValueError('Transport must classify every requested exact hash')
    if any(type(n) is not int or n < 0 or n > result['rowCount'] for n in counts.values()):
        raise ValueError('Invalid exact-hash match count')
    rows = result.get('rows')
    if not isinstance(rows, list) or len(rows) > 150000:
        raise ValueError('Invalid selected row envelope')
    grouped = {}
    for row in rows:
        if not isinstance(row, dict) or row.get('market_hash_name') not in counts:
            raise ValueError('Transport returned an unrequested or malformed row')
        if row.get('currency') != CURRENCY:
            raise ValueError('Selected row currency is not USD')
        grouped.setdefault(row['market_hash_name'], []).append(row)
    for name in requested:
        count = counts[name]
        supplied = len(grouped.get(name, []))
        if (count <= 1 and supplied != count) or (count > 1 and supplied > count):
            raise ValueError('Selected rows disagree with transport match counts')
    return {'bodySHA256': digest, 'receivedAtUs': received_us,
            'receivedAt': received_us // 1000000,
            'rowCount': result['rowCount'], 'distinctNames': result['distinctNames'],
            'requestedHashes': len(requested), 'provider': PROVIDER, 'currency': CURRENCY,
            'source': SOURCE, 'requestUrl': REQUEST_URL,
            'providerTimeSemantics': 'upstream_updated_at_not_claimed_trade_time'}, grouped


def normalize_outcome(name, count, rows):
    outcome = {'marketHashName': name, 'state': 'missing', 'priceText': None,
               'quantityListed': None, 'providerUpdatedAt': None, 'reason': 'not_in_successful_snapshot'}
    if count > 1:
        outcome.update(state='ambiguous', reason='duplicate_exact_hash', matchCount=count)
        return outcome
    if count == 0:
        return outcome
    row = rows[0]
    quantity = row.get('quantity')
    price = _decimal(row.get('min_price'))
    try:
        cents = price.quantize(Decimal('0.01')) if price is not None else None
    except InvalidOperation:
        cents = None
    outcome['providerUpdatedAt'] = _valid_time(row.get('updated_at'))
    if type(quantity) is not int or quantity < 0:
        outcome.update(state='invalid', reason='invalid_listing_quantity')
    elif quantity == 0:
        outcome.update(state='no_listings', quantityListed=0, reason='zero_available_listings')
    elif price is None or cents is None:
        outcome.update(state='invalid', quantityListed=quantity, reason='missing_zero_or_invalid_min_price')
    elif price != cents:
        outcome.update(state='invalid', quantityListed=quantity, reason='subcent_usd_min_price')
    else:
        outcome.update(state='priced', priceText=str(price), quantityListed=quantity, reason=None)
    return outcome


def _semantic(outcome):
    return {'state': outcome['state'], 'priceText': str(Decimal(outcome['priceText']).normalize())
            if outcome.get('priceText') is not None else None,
            'quantityListed': outcome.get('quantityListed')}


def _saved_raw(row):
    if row is None:
        return None
    raw = row['raw']
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > 4096:
        raise CashError('Invalid saved cash provenance')
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise CashError('Invalid saved cash provenance JSON') from error
    if (not isinstance(value, dict) or value.get('contractVersion') != 1
            or value.get('provider') != PROVIDER or value.get('currency') != CURRENCY
            or value.get('source') != SOURCE or value.get('basis') != BASIS
            or value.get('requestUrl') != REQUEST_URL
            or value.get('marketHashName') != row['name'] or value.get('state') not in OUTCOME_STATES
            or type(value.get('receivedAtUs')) is not int
            or not _valid_time(value['receivedAtUs'] // 1000000)
            or value['receivedAtUs'] // 1000000 != row['updated_at']
            or not isinstance(value.get('bodySHA256'), str)
            or not re.fullmatch('[0-9a-f]{64}', value['bodySHA256'])):
        raise CashError('Saved cash provenance does not match its exact current row')
    if value['state'] == 'priced':
        if (_decimal(value.get('priceText')) is None or type(value.get('quantityListed')) is not int
                or value['quantityListed'] <= 0 or _decimal(row['price']) is None
                or float(Decimal(value['priceText'])) != row['price']):
            raise CashError('Invalid saved positive cash quote')
    elif row['price'] is not None:
        raise CashError('Unpriced cash state has a numeric current price')
    return value


class CashPrices:
    def __init__(self, connection_factory, holdings_reader, transport, capacity_check, writer_lock,
                 clock=time.time, initial_not_before=INITIAL_NOT_BEFORE, binding_writer=None, input_revision_writer=None):
        self.connect = connection_factory
        self.binding_writer = binding_writer
        self.input_revision_writer = input_revision_writer
        self.holdings_reader = holdings_reader
        self.transport = transport
        self.capacity_check = capacity_check
        self.writer_lock = writer_lock
        self.clock = clock
        self.initial_not_before = int(initial_not_before)
        self.owner = uuid.uuid4().hex
        self.lock = threading.RLock()
        self.worker = None
        self.worker_error = None

    @contextlib.contextmanager
    def _write(self):
        self.capacity_check()
        if not self.writer_lock.acquire(timeout=0.1):
            raise CashAdmissionBusy('Cash write admission is busy; no automatic retry')
        conn = None
        try:
            conn = self.connect(write=True)
            conn.execute('BEGIN IMMEDIATE')
            yield conn
            conn.commit()
        except BaseException:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()
            self.writer_lock.release()

    def _control(self, conn):
        row = conn.execute('SELECT value FROM settings WHERE key=?', (CONTROL_KEY,)).fetchone()
        if row is None:
            return {'version': 1, 'nextAllowedAt': self.initial_not_before,
                    'cooldownReason': 'initial_public_qualification_local_cache_floor', 'job': None}
        try:
            value = json.loads(row['value'])
        except ValueError as error:
            raise CashError('Cash control receipt is invalid; acquisition is blocked') from error
        if (not isinstance(value, dict) or value.get('version') != 1
                or type(value.get('nextAllowedAt')) is not int):
            raise CashError('Cash control receipt is invalid; acquisition is blocked')
        return value

    def _save_control(self, conn, value):
        encoded = _json(value)
        if len(encoded.encode('utf-8')) > 16384:
            raise ValueError('Cash control receipt exceeded its bound')
        conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                     (CONTROL_KEY, encoded))

    def _revision(self, conn):
        row = conn.execute('SELECT value FROM settings WHERE key=?', (REVISION_KEY,)).fetchone()
        return row['value'] if row else None

    def _holdings(self, portfolio_id, conn=None):
        if not isinstance(portfolio_id, str) or not portfolio_id or len(portfolio_id) > 128:
            raise ValueError('An explicit valid portfolio_id is required')
        scope, rows, display_holdings = self.holdings_reader(portfolio_id, conn)
        return freeze_holdings(portfolio_id, scope, rows, display_holdings)

    def _effective(self, control):
        value = dict(control)
        job = dict(value['job']) if isinstance(value.get('job'), dict) else None
        running = self.worker is not None and self.worker.is_alive()
        if job and job.get('status') in ACTIVE_STATES and (value.get('owner') != self.owner or not running):
            job.update(status='interrupted', phase='recovery_required')
            value['nextAllowedAt'] = max(value['nextAllowedAt'], int(job.get('startedAt') or job['createdAt']) + UNCERTAIN_SECONDS)
            value['cooldownReason'] = 'uncertain_prior_request_local_backoff'
        value['job'] = job
        return value

    def status(self, job_id=None):
        if job_id is not None and (not isinstance(job_id, str) or not re.fullmatch('[0-9a-f]{32}', job_id)):
            raise ValueError('Invalid cash job ID')
        with self.lock, contextlib.closing(self.connect(write=False)) as conn:
            conn.execute('BEGIN')
            control = self._effective(self._control(conn))
            revision = self._revision(conn)
        job = control.get('job')
        if job_id is not None and (not job or job.get('id') != job_id):
            raise CashNotFound('Cash job does not exist')
        return {'provider': PROVIDER, 'currency': CURRENCY, 'job': job,
                'workerRunning': bool(self.worker and self.worker.is_alive()),
                'cashSourceRevision': revision,
                'nextAllowedAt': control['nextAllowedAt'],
                'retryAt': control['nextAllowedAt'] if control['nextAllowedAt'] > int(self.clock()) else None,
                'cooldownReason': control.get('cooldownReason'),
                'lastSuccessfulFetch': control.get('lastSuccessfulFetch'),
                'lastError': self.worker_error or control.get('lastError'),
                'freshnessSeconds': FRESHNESS_SECONDS}

    def action(self, request, *, expected_saved_job_id=None):
        if (not isinstance(request, dict) or set(request) - {'provider', 'portfolio_id', 'action', 'job_id'}
                or request.get('provider') != PROVIDER):
            raise ValueError('Only skinport_live, portfolio_id, action and optional job_id are accepted')
        if expected_saved_job_id is not None and (not isinstance(expected_saved_job_id, str)
                or not re.fullmatch('[0-9a-f]{32}', expected_saved_job_id)):
            raise ValueError('Invalid saved cash scope fence')
        action = request.get('action', 'start')
        if action not in ('start', 'resume'):
            raise ValueError('Cash action must be start or resume')
        if action == 'start' and request.get('job_id') is not None:
            raise ValueError('job_id is accepted only for resume')
        if action == 'resume' and (not isinstance(request.get('job_id'), str)
                                  or not re.fullmatch('[0-9a-f]{32}', request['job_id'])):
            raise ValueError('Resume requires an explicit valid job_id')
        if not callable(self.transport):
            raise CashError('Bounded Skinport transport is not installed')
        frozen = self._holdings(request.get('portfolio_id'))
        with self.lock:
            with self._write() as conn:
                control = self._effective(self._control(conn))
                old = control.get('job')
                if expected_saved_job_id is not None and (not old or old.get('id') != expected_saved_job_id
                        or old.get('portfolioId') != frozen['portfolioId']):
                    raise CashConflict('Latest saved cash scope changed; cadence did not start a request')
                if action == 'resume' and (not old or request['job_id'] != old.get('id')):
                    raise CashNotFound('Cash job does not exist')
                if self.worker and self.worker.is_alive():
                    if (old and old.get('portfolioId') == frozen['portfolioId']
                            and old.get('holdingsFingerprint') == frozen['holdingsFingerprint']):
                        return {'accepted': False, 'reused': True, 'job': old, 'provider': PROVIDER, 'currency': CURRENCY}
                    raise CashConflict('A different cash holding scope is already running')
                if action == 'resume':
                    if (not old or request.get('job_id') != old.get('id')
                            or old.get('portfolioId') != frozen['portfolioId']
                            or old.get('holdingsFingerprint') != frozen['holdingsFingerprint']):
                        raise CashConflict('Resume requires the saved job ID and unchanged exact holdings')
                    if old.get('status') == 'completed':
                        return {'accepted': False, 'reused': True, 'job': old, 'provider': PROVIDER, 'currency': CURRENCY}
                now = int(self.clock())
                if control['nextAllowedAt'] > now:
                    raise CashCooldown(control['nextAllowedAt'], control.get('cooldownReason'))
                job = {'id': old['id'] if action == 'resume' else uuid.uuid4().hex,
                       'portfolioId': frozen['portfolioId'], 'holdingsFingerprint': frozen['holdingsFingerprint'],
                       'status': 'queued', 'phase': 'queued', 'createdAt': old['createdAt'] if action == 'resume' else now,
                       'startedAt': now, 'updatedAt': now, 'receivedAt': None,
                       'totalDistinctHashes': len(frozen['hashes']), 'holdingRows': len(frozen['rows']),
                       'classifiedRows': 0, 'committedRows': 0, 'pricedRows': 0, 'missingRows': 0,
                       'noListingsRows': 0, 'ambiguousRows': 0, 'invalidRows': 0,
                       'unresolvedRows': sum(bool(r['identityIssue']) for r in frozen['rows']),
                       'freshnessSeconds': FRESHNESS_SECONDS, 'error': None}
                job['attempt'] = int(old.get('attempt', 0)) + 1 if action == 'resume' else 1
                control.update(job=job, owner=self.owner, nextAllowedAt=now + ADMISSION_SECONDS,
                               cooldownReason='local_five_minute_acquisition_interval', lastError=None)
                self._save_control(conn, control)
            self.worker_error = None
            self.worker = threading.Thread(target=self._run, args=(job['id'], frozen),
                                           name='skinport-cash-refresh', daemon=True)
            try:
                self.worker.start()
            except BaseException:
                self.worker = None
                self._fail(job['id'], 'Cash worker could not start')
                raise
            return {'accepted': True, 'reused': False, 'provider': PROVIDER, 'currency': CURRENCY, 'job': job}

    def _owned(self, conn, job_id):
        control = self._control(conn)
        if control.get('owner') != self.owner or not control.get('job') or control['job'].get('id') != job_id:
            raise CashConflict('Cash worker no longer owns the saved job')
        return control

    def _fail(self, job_id, message, retry_at=None, reason=None):
        self.worker_error = str(message)[:512]
        try:
            with self._write() as conn:
                control = self._owned(conn, job_id)
                control['job'].update(status='paused' if retry_at else 'failed', phase='backoff' if retry_at else 'failed',
                                      error=self.worker_error, updatedAt=int(self.clock()))
                control['lastError'] = self.worker_error
                if retry_at:
                    control['nextAllowedAt'] = max(control['nextAllowedAt'], retry_at)
                    control['cooldownReason'] = reason
                self._save_control(conn, control)
        except Exception as error:
            # Leave the committed reservation uncertain. The saved guard blocks
            # another request after restart; no success is reported from memory.
            self.worker_error += '; saved failure status could not commit: ' + str(error)[:160]

    def _retry_deadline(self, result):
        now = int(self.clock())
        value = (result.get('headers') or {}).get('retryAfter')
        deadline = None
        if isinstance(value, str):
            try:
                if value.isdigit():
                    deadline = now + int(value)
                else:
                    parsed = email.utils.parsedate_to_datetime(value)
                    if parsed.tzinfo is not None:
                        deadline = int(parsed.timestamp())
            except (ValueError, TypeError, OverflowError):
                pass
        return max(now + ADMISSION_SECONDS, deadline or now + UNCERTAIN_SECONDS), (
            'upstream_retry_after_plus_local_cache_floor' if deadline else 'http_429_local_fallback')

    def _run(self, job_id, frozen):
        try:
            if not frozen['hashes']:
                with self._write() as conn:
                    control = self._owned(conn, job_id)
                    control['job'].update(status='completed', phase='completed', updatedAt=int(self.clock()))
                    self._save_control(conn, control)
                return
            self.capacity_check()
            with self._write() as conn:
                control = self._owned(conn, job_id)
                control['job'].update(status='fetching', phase='fetching', updatedAt=int(self.clock()))
                self._save_control(conn, control)
            result = self.transport(list(frozen['hashes']))  # No writer/transaction is held.
            if isinstance(result, dict) and result.get('status') == 429:
                deadline, reason = self._retry_deadline(result)
                self._fail(job_id, 'Skinport returned HTTP 429', deadline, reason)
                return
            if not isinstance(result, dict) or result.get('status') != 200:
                raise CashError('Skinport transport failed: ' + str((result or {}).get('error') if isinstance(result, dict) else 'invalid response')[:256])
            receipt, grouped = _receipt(result, frozen['hashes'])
            if receipt['receivedAt'] < control['job']['startedAt'] - 5 or receipt['receivedAt'] > int(self.clock()) + 60:
                raise ValueError('Transport acquisition clock is outside this request attempt')
            receipt.update(portfolioId=frozen['portfolioId'], holdingsFingerprint=frozen['holdingsFingerprint'])
            outcomes = [normalize_outcome(name, result['matchCounts'][name], grouped.get(name, [])) for name in frozen['hashes']]
            del grouped, result
            for start in range(0, len(outcomes), BATCH_ROWS):
                batch = outcomes[start:start + BATCH_ROWS]
                with self._write() as conn:
                    control = self._owned(conn, job_id)
                    control['job'].update(status='storing', phase='storing', receivedAt=receipt['receivedAt'], updatedAt=int(self.clock()))
                    for outcome in batch:
                        self._store_outcome(conn, outcome, receipt)
                        key = {'priced': 'pricedRows', 'missing': 'missingRows', 'no_listings': 'noListingsRows',
                               'ambiguous': 'ambiguousRows', 'invalid': 'invalidRows'}[outcome['state']]
                        control['job'][key] += 1
                    control['job']['classifiedRows'] += len(batch)
                    control['job']['committedRows'] += len(batch)
                    control['nextAllowedAt'] = max(control['nextAllowedAt'], receipt['receivedAt'] + ADMISSION_SECONDS)
                    control['lastSuccessfulFetch'] = receipt
                    control['lastError'] = None
                    if start + len(batch) == len(outcomes):
                        control['job'].update(status='completed', phase='completed')
                    revision = uuid.uuid4().hex
                    conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                 (REVISION_KEY, revision))
                    if self.input_revision_writer is not None:
                        self.input_revision_writer(conn, PROVIDER)
                    self._save_control(conn, control)
        except Exception as error:
            self._fail(job_id, error)

    def _store_outcome(self, conn, outcome, receipt):
        name = outcome['marketHashName']
        old = conn.execute('SELECT name,price,raw,updated_at FROM market_item_prices WHERE name=? AND provider=?',
                           (name, PROVIDER)).fetchone()
        previous = _saved_raw(old)
        raw = dict(outcome, contractVersion=1, provider=PROVIDER, source=SOURCE, currency=CURRENCY,
                   basis=BASIS, receivedAtUs=receipt['receivedAtUs'], bodySHA256=receipt['bodySHA256'],
                   providerTimeSemantics=receipt['providerTimeSemantics'], requestUrl=REQUEST_URL,
                   tradable=0, appId=730)
        encoded = _json(raw)
        if len(encoded.encode('utf-8')) > 4096:
            raise ValueError('Cash row provenance exceeded its bound')
        amount = float(Decimal(outcome['priceText'])) if outcome['state'] == 'priced' else None
        captured = receipt['receivedAt']
        if old is not None and (type(old['updated_at']) is not int or old['updated_at'] > captured
                                or previous['receivedAtUs'] > receipt['receivedAtUs']):
            raise CashConflict('An older acquisition cannot replace newer saved cash data')
        meaningful_observation_at = None
        if previous is None or _semantic(previous) != _semantic(outcome):
            meaningful_observation_at = captured
            existing = conn.execute('SELECT raw FROM price_observations WHERE name=? AND provider=? AND observed_at=?',
                                    (name, PROVIDER, captured)).fetchone()
            if existing is not None:
                prior = json.loads(existing['raw'])
                if prior.get('bodySHA256') != raw['bodySHA256'] or _semantic(prior) != _semantic(raw):
                    raise CashConflict('A different cash observation already owns this capture second')
            else:
                conn.execute('''INSERT INTO price_observations
                    (name,provider,observed_at,price,median,volume,source,raw,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?)''', (name, PROVIDER, captured, amount, None, None, SOURCE, encoded, captured))
        conn.execute('''INSERT INTO market_item_prices
            (name,provider,price,median,solid_price,lowest_sell_order,highest_buy_order,spread_pct,
             stability_pct,confidence_pct,volume,source,basis,kind,url,raw,observed_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name,provider) DO UPDATE SET price=excluded.price,median=excluded.median,
             solid_price=excluded.solid_price,lowest_sell_order=excluded.lowest_sell_order,
             highest_buy_order=excluded.highest_buy_order,spread_pct=excluded.spread_pct,
             stability_pct=excluded.stability_pct,confidence_pct=excluded.confidence_pct,volume=excluded.volume,
             source=excluded.source,basis=excluded.basis,kind=excluded.kind,url=excluded.url,raw=excluded.raw,
             observed_at=excluded.observed_at,updated_at=excluded.updated_at''',
            (name, PROVIDER, amount, None, amount, amount, None, None, None, None, None,
             SOURCE, BASIS, 'cash_listing_reference', None, encoded, captured, captured))
        if self.binding_writer is not None:
            # Same connection/transaction as this outcome and its batch revision.
            # Callback failure must roll back the whole batch; never retry here.
            self.binding_writer(conn, name, meaningful_observation_at)

    def value(self, portfolio_id):
        prices = {}
        with contextlib.closing(self.connect(write=False)) as conn:
            conn.execute('BEGIN')
            frozen = self._holdings(portfolio_id, conn)
            for start in range(0, len(frozen['hashes']), 256):
                chunk = frozen['hashes'][start:start + 256]
                for row in conn.execute('SELECT name,price,raw,updated_at FROM market_item_prices WHERE provider=? AND name IN ('
                                        + ','.join('?' for _ in chunk) + ')', [PROVIDER] + chunk):
                    prices[row['name']] = _saved_raw(row)
            revision = self._revision(conn)
            control = self._control(conn)
        now = int(self.clock())
        items, covered, fresh_covered = [], Decimal(0), Decimal(0)
        coverage = {'holdingRows': len(frozen['rows']), 'distinctHashes': len(frozen['hashes']),
                    'pricedRows': 0, 'freshRows': 0, 'staleRows': 0, 'missingRows': 0,
                    'noListingsRows': 0, 'ambiguousRows': 0, 'invalidRows': 0,
                    'unresolvedRows': 0, 'zeroQuantityRows': 0}
        for held in frozen['rows']:
            quote = prices.get(held['marketHashName'])
            state = ('invalid' if held['quantityIssue'] else 'zero_quantity' if held['qty'] == 0
                     else 'unresolved' if held['identityIssue'] or not held['marketHashName']
                     else quote['state'] if quote else 'missing')
            captured = quote['receivedAtUs'] // 1000000 if quote else None
            if captured is not None and captured > now + 60 and state == 'priced':
                state = 'invalid'
            age = max(0, now - captured) if captured else None
            freshness = 'fresh' if state == 'priced' and age is not None and age <= FRESHNESS_SECONDS else 'stale' if state == 'priced' else 'missing'
            unit, value = None, None
            if state == 'priced':
                unit_decimal = Decimal(quote['priceText'])
                unit = float(unit_decimal)
                value_decimal = unit_decimal * Decimal(held['quantityKey'])
                value = _money(value_decimal)
                covered += Decimal(str(value))
                if freshness == 'fresh':
                    fresh_covered += Decimal(str(value))
                coverage['freshRows' if freshness == 'fresh' else 'staleRows'] += 1
            elif state == 'zero_quantity':
                value = 0
            key = {'priced': 'pricedRows', 'missing': 'missingRows', 'no_listings': 'noListingsRows',
                   'ambiguous': 'ambiguousRows', 'invalid': 'invalidRows', 'unresolved': 'unresolvedRows',
                   'zero_quantity': 'zeroQuantityRows'}[state]
            coverage[key] += 1
            items.append({'displayName': held['displayName'], 'marketHashName': held['marketHashName'],
                          'qty': held['qty'], 'state': state, 'unitPriceUsd': unit, 'valueUsd': value,
                          'quantityListed': quote.get('quantityListed') if quote else None,
                          'capturedAt': captured, 'capturedAtUtc': _utc(captured) if captured else None,
                          'providerUpdatedAt': quote.get('providerUpdatedAt') if quote else None,
                          'providerTimeSemantics': 'upstream_updated_at_not_claimed_trade_time',
                          'ageSeconds': age, 'freshnessState': freshness,
                          'reason': held['quantityIssue'] or held['identityIssue'] or (quote.get('reason') if quote else 'no_saved_cash_quote'),
                          'receiptBodySHA256': quote.get('bodySHA256') if quote else None})
        full = coverage['pricedRows'] + coverage['zeroQuantityRows'] == coverage['holdingRows']
        coverage['fullCoverage'] = full
        coverage['truncated'] = False
        job = control.get('job') or {}
        return {'provider': PROVIDER, 'currency': CURRENCY, 'valuationLane': 'cash_lowest_listed',
                'basis': BASIS, 'freshnessSeconds': FRESHNESS_SECONDS, 'portfolioId': portfolio_id,
                'holdingsFingerprint': frozen['holdingsFingerprint'],
                'displayHoldings': frozen['displayHoldings'],
                'holdingsChangedSinceJob': bool(job and (job.get('portfolioId') != portfolio_id
                    or job.get('holdingsFingerprint') != frozen['holdingsFingerprint'])),
                'cashSourceRevision': revision, 'coverage': coverage, 'items': items,
                'totals': {'coveredValueUsd': _money(covered), 'fullValueUsd': _money(covered) if full else None,
                           'freshCoveredValueUsd': _money(fresh_covered),
                           'freshFullValueUsd': _money(fresh_covered) if full and not coverage['staleRows'] else None},
                'sourceReceipt': control.get('lastSuccessfulFetch')}
