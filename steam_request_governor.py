"""Persisted, default-paused Steam request admission; no transport, DDL or bodies.

The caller owns the supplied guarded SQLite connection and a no-redirect HTTP
transport. A successful HTTP response is never asserted to be a semantic quote.
"""
import contextlib
from datetime import timezone
import email.utils
import hashlib
import json
import math
import re
import urllib.parse
import uuid

CONTROL_KEY = 'steam_request_governor:v1:control'
ENTRY_PREFIX = 'steam_request_governor:v1:entry:'
ASK_CACHE_SECONDS = 600
HISTORY_CACHE_SECONDS = 21600
MINIMUM_INTERVAL_SECONDS = 60  # Conservative operator policy, not a Valve quota.
MAXIMUM_INTERVAL_SECONDS = 31536000
LEASE_SECONDS = 120
MAX_STATE_BYTES = 16384
MAX_ENTRY_BYTES = 2048
MAX_TIME = 253402300799
MAX_METRIC = 9223372036854775807
PURPOSES = {'priceoverview', 'pricehistory', 'listings'}
COUNTERS = ('reservations', 'networkAttempts', 'responses', 'successfulHTTP',
            'rateLimited', 'transientFailures', 'transportFailures',
            'cancelledBeforeHTTP', 'bytesRead', 'elapsedMs')
STATE_FIELDS = {'version', 'revision', 'operatorPaused', 'pauseReason',
                'nextAllowedAt', 'intervalSeconds', 'remainingRequests',
                'lastClock', 'lease', 'counters', 'events'}
LEASE_FIELDS = {'token', 'key', 'purpose', 'reservedAt', 'leaseUntil', 'startedAt', 'eventId'}
EVENT_FIELDS = {'id', 'requestDigest', 'purpose', 'start', 'networkStart', 'end',
                'httpStatus', 'bytesRead', 'elapsedMs', 'retryAt', 'outcome'}
ENTRY_FIELDS = {'version', 'key', 'purpose', 'nextAllowedAt', 'cacheUntil',
                'activeToken', 'lastReservedAt', 'lastStartedAt', 'lastFinishedAt',
                'lastStatus', 'bytesRead', 'elapsedMs', 'lastOutcome'}
OUTCOMES = {'reserved', 'started', 'http_success', 'http_error', 'rate_limited',
            'transient_failure', 'transport_error', 'cancelled_before_http'}


class GovernorError(RuntimeError):
    pass


def _integer(value, maximum=MAX_METRIC):
    return type(value) is int and 0 <= value <= maximum


def _number(value):
    return type(value) in (int, float) and 0 <= value <= MAX_METRIC and math.isfinite(value)


def _now(value):
    if not _number(value) or value > MAX_TIME:
        raise GovernorError('A finite nonnegative epoch clock is required')
    return int(value)


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value) is not None


def _status_code(value):
    return value is None or type(value) is int and 100 <= value <= 599


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise GovernorError('Duplicate persisted JSON field')
        result[key] = value
    return result


def _decode(raw, maximum):
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > maximum:
        raise GovernorError('Persisted governor metadata exceeds its bound')
    try:
        return json.loads(raw, object_pairs_hook=_unique_pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, TypeError, RecursionError) as error:
        raise GovernorError('Malformed persisted governor metadata') from error


def _encode(value, maximum):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if len(raw.encode('utf-8')) > maximum:
        raise GovernorError('Governor metadata exceeded its finite bound')
    return raw


def _fresh(now):
    return {'version': 1, 'revision': uuid.uuid4().hex, 'operatorPaused': True,
            'pauseReason': 'operator_paused', 'nextAllowedAt': 0,
            'intervalSeconds': MINIMUM_INTERVAL_SECONDS, 'remainingRequests': 0,
            'lastClock': now, 'lease': None,
            'counters': {key: 0 for key in COUNTERS}, 'events': []}


def _check_state(value):
    if (not isinstance(value, dict) or set(value) != STATE_FIELDS or type(value['version']) is not int or value['version'] != 1
            or not _hex(value['revision'], 32) or type(value['operatorPaused']) is not bool
            or value['pauseReason'] not in {'operator_paused', 'validation', 'validation_budget_exhausted'}
            or not _integer(value['nextAllowedAt'], MAX_TIME) or not _integer(value['lastClock'], MAX_TIME)
            or not _integer(value['intervalSeconds'], MAXIMUM_INTERVAL_SECONDS)
            or value['intervalSeconds'] < MINIMUM_INTERVAL_SECONDS
            or not _integer(value['remainingRequests'], 3)
            or not value['operatorPaused'] and (not value['remainingRequests'] or value['pauseReason'] != 'validation')):
        raise GovernorError('Invalid persisted governor control state')
    counters = value['counters']
    if (not isinstance(counters, dict) or set(counters) != set(COUNTERS)
            or any(not (_number(counters[key]) if key == 'elapsedMs' else _integer(counters[key])) for key in COUNTERS)
            or counters['networkAttempts'] > counters['reservations']
            or counters['responses'] > counters['networkAttempts']
            or counters['successfulHTTP'] > counters['responses']
            or counters['rateLimited'] > counters['responses']):
        raise GovernorError('Invalid persisted governor counters')
    events = value['events']
    if not isinstance(events, list) or len(events) > 32:
        raise GovernorError('Invalid governor event bound')
    ids = set()
    for event in events:
        if (not isinstance(event, dict) or set(event) != EVENT_FIELDS or not _hex(event['id'], 32)
                or event['id'] in ids or not _hex(event['requestDigest'], 64) or event['purpose'] not in PURPOSES
                or not _integer(event['start'], MAX_TIME) or not _integer(event['retryAt'], MAX_TIME)
                or any(event[key] is not None and (not _integer(event[key], MAX_TIME) or event[key] < event['start'])
                       for key in ('networkStart', 'end'))
                or not _status_code(event['httpStatus']) or not _integer(event['bytesRead'])
                or not _number(event['elapsedMs']) or event['outcome'] not in OUTCOMES
                or event['end'] is not None and event['outcome'] in ('reserved', 'started')
                or event['end'] is None and (event['httpStatus'] is not None or event['bytesRead'] != 0
                                              or event['elapsedMs'] != 0
                                              or event['outcome'] not in ('reserved', 'started'))):
            raise GovernorError('Invalid persisted governor event')
        ids.add(event['id'])
    lease = value['lease']
    pending = [event for event in events if event['end'] is None]
    if lease is None:
        if pending:
            raise GovernorError('Governor contains an orphan pending event')
    else:
        if (not isinstance(lease, dict) or set(lease) != LEASE_FIELDS or not _hex(lease['token'], 32)
                or not _hex(lease['key'], 64) or lease['purpose'] not in PURPOSES or not _hex(lease['eventId'], 32)
                or not _integer(lease['reservedAt'], MAX_TIME) or not _integer(lease['leaseUntil'], MAX_TIME)
                or lease['leaseUntil'] < lease['reservedAt']
                or lease['startedAt'] is not None and (not _integer(lease['startedAt'], MAX_TIME)
                                                      or lease['startedAt'] < lease['reservedAt'])
                or len(pending) != 1):
            raise GovernorError('Invalid persisted governor lease')
        event = pending[0]
        if (event['id'] != lease['eventId'] or event['requestDigest'] != lease['key']
                or event['purpose'] != lease['purpose'] or event['start'] != lease['reservedAt']
                or event['networkStart'] != lease['startedAt']
                or event['outcome'] != ('reserved' if lease['startedAt'] is None else 'started')):
            raise GovernorError('Governor lease/event identity differs')
    return value


def _entry(key, purpose):
    return {'version': 1, 'key': key, 'purpose': purpose, 'nextAllowedAt': 0, 'cacheUntil': 0,
            'activeToken': None, 'lastReservedAt': None, 'lastStartedAt': None,
            'lastFinishedAt': None, 'lastStatus': None, 'bytesRead': 0,
            'elapsedMs': 0, 'lastOutcome': None}


def _check_entry(value, key, purpose):
    if (not isinstance(value, dict) or set(value) != ENTRY_FIELDS or type(value['version']) is not int or value['version'] != 1
            or value['key'] != key or value['purpose'] != purpose
            or not _integer(value['nextAllowedAt'], MAX_TIME) or not _integer(value['cacheUntil'], MAX_TIME)
            or value['activeToken'] is not None and not _hex(value['activeToken'], 32)
            or any(value[field] is not None and not _integer(value[field], MAX_TIME)
                   for field in ('lastReservedAt', 'lastStartedAt', 'lastFinishedAt'))
            or not _status_code(value['lastStatus']) or not _integer(value['bytesRead'])
            or not _number(value['elapsedMs'])
            or value['lastOutcome'] is not None and value['lastOutcome'] not in OUTCOMES):
        raise GovernorError('Invalid persisted per-request metadata')
    return value


def canonical_request(url):
    """Return digest/purpose only; exact held names are never persisted or returned."""
    try:
        encoded_length = len(url.encode('utf-8')) if isinstance(url, str) else 0
    except UnicodeError as error:
        raise GovernorError('Invalid Steam URL encoding') from error
    if (not isinstance(url, str) or not url or encoded_length > 2048
            or any(ord(c) <= 32 or ord(c) == 127 for c in url)
            or re.search(r'%(?![0-9a-fA-F]{2})', url)):
        raise GovernorError('Invalid Steam request URL')
    try:
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != 'https' or parsed.hostname != 'steamcommunity.com'
                or parsed.port not in (None, 443) or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise GovernorError('Only fixed HTTPS Steam community endpoints are permitted')
        paths = {'/market/priceoverview/': ('priceoverview', {'appid': '730', 'currency': '1', 'l': 'english'}),
                 '/market/pricehistory/': ('pricehistory', {'appid': '730', 'currency': '1', 'country': 'US'})}
        if parsed.path in paths:
            purpose, fixed = paths[parsed.path]
            pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True,
                                           encoding='utf-8', errors='strict', max_num_fields=5)
            params = dict(pairs)
            if (len(params) != len(pairs) or set(params) != set(fixed) | {'market_hash_name'}
                    or any(params[key] != value for key, value in fixed.items())):
                raise GovernorError('Unknown, duplicated or incompatible Steam parameters')
            name = params['market_hash_name']
        else:
            prefix = '/market/listings/730/'
            if (not parsed.path.startswith(prefix) or not parsed.path[len(prefix):]
                    or '/' in parsed.path[len(prefix):] or parsed.query):
                raise GovernorError('Unknown Steam endpoint or redirect target')
            purpose = 'listings'
            name = urllib.parse.unquote(parsed.path[len(prefix):], encoding='utf-8', errors='strict')
        if (not name or name != name.strip() or len(name.encode('utf-8')) > 512
                or any(ord(c) < 32 or 127 <= ord(c) < 160 for c in name)):
            raise GovernorError('An exact bounded Steam market name is required')
    except (ValueError, UnicodeError) as error:
        raise GovernorError('Malformed or disallowed Steam URL') from error
    identity = {'source': 'steamcommunity.com', 'app': 730, 'currency': 1, 'purpose': purpose, 'name': name}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False,
                                       separators=(',', ':')).encode('utf-8')).hexdigest()
    return {'key': digest, 'purpose': purpose}


def _retry_deadline(value, now):
    if not isinstance(value, str) or len(value) > 128:
        return None
    value = value.strip()
    if re.fullmatch('[0-9]{1,10}', value):
        return min(MAX_TIME, now + int(value))
    try:
        stamp = email.utils.parsedate_to_datetime(value)
        if stamp.tzinfo is None:
            return None
        result = int(stamp.astimezone(timezone.utc).timestamp())
        return result if now <= result <= MAX_TIME else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


class RequestGovernor:
    def __init__(self, conn):
        self.conn = conn

    def _read(self, key, maximum):
        row = self.conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return None if row is None else _decode(row[0], maximum)

    def _load(self):
        value = self._read(CONTROL_KEY, MAX_STATE_BYTES)
        return None if value is None else _check_state(value)

    def _load_entry(self, key, purpose):
        value = self._read(ENTRY_PREFIX + key, MAX_ENTRY_BYTES)
        return _entry(key, purpose) if value is None else _check_entry(value, key, purpose)

    def _write(self, key, value, maximum):
        raw = _encode(value, maximum)
        self.conn.execute('INSERT INTO settings(key,value) VALUES(?,?) '
                          'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, raw))

    def _save(self, state, entry=None):
        _check_state(state)
        if entry is not None:
            _check_entry(entry, entry['key'], entry['purpose'])
            self._write(ENTRY_PREFIX + entry['key'], entry, MAX_ENTRY_BYTES)
        self._write(CONTROL_KEY, state, MAX_STATE_BYTES)

    @contextlib.contextmanager
    def _atomic(self):
        if self.conn.in_transaction:
            raise GovernorError('Governor requires its own short standalone transaction')
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def _public(self, state, now):
        if state is None:
            return {'initialized': False, 'revision': None, 'operatorPaused': True,
                    'pauseReason': 'not_initialized', 'nextAllowedAt': 0, 'intervalSeconds': 60,
                    'remainingRequests': 0, 'leasePresent': False, 'leaseUntil': 0,
                    'recoveryRequired': False, 'counters': {key: 0 for key in COUNTERS},
                    'events': [], 'intervalIsOperatorPolicyNotValveQuota': True,
                    'successfulHTTPIsSemanticQuote': False}
        lease = state['lease']
        rollback = now < state['lastClock']
        return {'initialized': True, 'revision': state['revision'],
                'operatorPaused': state['operatorPaused'] or rollback,
                'pauseReason': 'clock_rollback' if rollback else state['pauseReason'],
                'nextAllowedAt': state['nextAllowedAt'], 'intervalSeconds': state['intervalSeconds'],
                'remainingRequests': state['remainingRequests'], 'leasePresent': lease is not None,
                'leaseUntil': lease['leaseUntil'] if lease else 0,
                'recoveryRequired': rollback or bool(lease and now >= lease['leaseUntil']),
                'counters': dict(state['counters']), 'events': [dict(event) for event in state['events']],
                'intervalIsOperatorPolicyNotValveQuota': True, 'successfulHTTPIsSemanticQuote': False}

    def _deferred(self, state, now, reason, retry_at=None, recovery=False):
        status = self._public(state, now)
        return {'action': 'deferred', 'started': False, 'reason': reason,
                'nextAllowedAt': status['nextAllowedAt'],
                'retryAt': retry_at if retry_at is not None else
                    status['nextAllowedAt'] if status['nextAllowedAt'] > now else None,
                'operatorPaused': status['operatorPaused'], 'recoveryRequired': recovery or status['recoveryRequired']}

    def _claim(self, state, claim):
        lease = state['lease'] if state else None
        if (not isinstance(claim, dict) or lease is None
                or any(claim.get(key) != lease[key] for key in ('token', 'key', 'purpose', 'leaseUntil'))):
            raise GovernorError('Unknown or mismatched reservation token; lease preserved')
        entry = self._load_entry(lease['key'], lease['purpose'])
        if entry['activeToken'] != lease['token']:
            raise GovernorError('Per-request token differs; lease preserved')
        event = next((event for event in state['events'] if event['id'] == lease['eventId']), None)
        if event is None:
            raise GovernorError('Reservation event is missing; lease preserved')
        return lease, entry, event

    def status(self, now):
        return self._public(self._load(), _now(now))

    def reserve(self, url, now):
        now = _now(now)
        request = canonical_request(url)
        with self._atomic():
            state = self._load()
            if state is None:
                return self._deferred(None, now, 'operator_paused')
            if now < state['lastClock']:
                return self._deferred(state, now, 'clock_rollback', recovery=True)
            if state['lease'] is not None:
                lease = state['lease']
                return self._deferred(state, now, 'uncertain_lease' if now >= lease['leaseUntil'] else 'request_inflight',
                                      lease['leaseUntil'], now >= lease['leaseUntil'])
            if state['operatorPaused'] or state['remainingRequests'] == 0:
                return self._deferred(state, now, 'operator_paused')
            entry = self._load_entry(request['key'], request['purpose'])
            if entry['activeToken'] is not None:
                raise GovernorError('Orphan per-request reservation; explicit recovery required')
            if any(entry[key] is not None and entry[key] > now for key in
                   ('lastReservedAt', 'lastStartedAt', 'lastFinishedAt')):
                return self._deferred(state, now, 'clock_rollback', recovery=True)
            deadline = max(state['nextAllowedAt'], entry['nextAllowedAt'], entry['cacheUntil'])
            if now < deadline:
                return self._deferred(state, now, 'recent_http_response' if entry['cacheUntil'] == deadline
                                      else 'interval_floor', deadline)
            token, event_id = uuid.uuid4().hex, uuid.uuid4().hex
            floor = min(MAX_TIME, now + state['intervalSeconds'])
            state['nextAllowedAt'] = max(state['nextAllowedAt'], floor)
            state['remainingRequests'] -= 1
            if state['remainingRequests'] == 0:
                state.update(operatorPaused=True, pauseReason='validation_budget_exhausted')
            state['lease'] = {'token': token, **request, 'reservedAt': now,
                              'leaseUntil': min(MAX_TIME, now + LEASE_SECONDS),
                              'startedAt': None, 'eventId': event_id}
            state['counters']['reservations'] += 1
            state['lastClock'], state['revision'] = now, uuid.uuid4().hex
            state['events'].append({'id': event_id, 'requestDigest': request['key'], 'purpose': request['purpose'],
                'start': now, 'networkStart': None, 'end': None, 'httpStatus': None, 'bytesRead': 0,
                'elapsedMs': 0, 'retryAt': state['nextAllowedAt'], 'outcome': 'reserved'})
            state['events'] = state['events'][-32:]
            entry.update(activeToken=token, lastReservedAt=now, lastStartedAt=None,
                         nextAllowedAt=max(entry['nextAllowedAt'], floor), lastOutcome='reserved')
            self._save(state, entry)
            return {'action': 'request', 'token': token, **request, 'leaseUntil': state['lease']['leaseUntil']}

    def mark_started(self, claim, now):
        now = _now(now)
        with self._atomic():
            state = self._load()
            lease, entry, event = self._claim(state, claim)
            if now < state['lastClock']:
                return self._deferred(state, now, 'clock_rollback', recovery=True)
            if lease['startedAt'] is not None:
                value = self._deferred(state, now, 'request_already_started', recovery=True)
                value['alreadyStarted'] = True
                return value
            if now >= lease['leaseUntil']:
                return self._deferred(state, now, 'uncertain_reservation', lease['leaseUntil'], True)
            if state['operatorPaused'] and not (state['pauseReason'] == 'validation_budget_exhausted'
                                                and state['remainingRequests'] == 0):
                return self._deferred(state, now, 'operator_paused')
            lease['startedAt'] = now
            floor = min(MAX_TIME, now + state['intervalSeconds'])
            state['nextAllowedAt'] = max(state['nextAllowedAt'], floor)
            entry.update(lastStartedAt=now, lastOutcome='started',
                         nextAllowedAt=max(entry['nextAllowedAt'], state['nextAllowedAt']))
            event.update(networkStart=now, outcome='started', retryAt=state['nextAllowedAt'])
            state['counters']['networkAttempts'] += 1
            state['lastClock'], state['revision'] = now, uuid.uuid4().hex
            self._save(state, entry)
            return {'action': 'request', 'started': True, 'alreadyStarted': False,
                    'key': lease['key'], 'purpose': lease['purpose'], 'leaseUntil': lease['leaseUntil']}

    def finish(self, claim, now, status, bytes_read, elapsed_ms,
               retry_after=None, upstream_retry_at=None, transport_error=None):
        now = _now(now)
        if not _status_code(status) or not _integer(bytes_read) or not _number(elapsed_ms):
            raise GovernorError('Bounded response telemetry required')
        elapsed_ms = round(elapsed_ms, 3)
        with self._atomic():
            state = self._load()
            lease, entry, event = self._claim(state, claim)
            if now < state['lastClock']:
                raise GovernorError('Clock rolled back; uncertain reservation preserved')
            cancelled = transport_error == 'cancelled_before_http'
            if lease['startedAt'] is None:
                if not cancelled or status is not None or bytes_read != 0:
                    raise GovernorError('Unstarted reservation may only close as cancelled_before_http')
                state['counters']['cancelledBeforeHTTP'] += 1
                outcome = 'cancelled_before_http'
            else:
                if cancelled:
                    raise GovernorError('Started attempt cannot be cleared as a pre-HTTP cancellation')
                transient = transport_error is not None or status is None or status in (408, 425) or status >= 500
                limited = status == 429
                if limited or transient:
                    state['intervalSeconds'] = min(MAXIMUM_INTERVAL_SECONDS, state['intervalSeconds'] * 2)
                    state['nextAllowedAt'] = max(state['nextAllowedAt'], min(MAX_TIME, now + state['intervalSeconds']))
                header_deadline = _retry_deadline(retry_after, now)
                upstream_deadline = int(upstream_retry_at) if _number(upstream_retry_at) and now <= upstream_retry_at <= MAX_TIME else None
                deadlines = [value for value in (header_deadline, upstream_deadline) if value is not None]
                if limited and not deadlines:
                    deadlines.append(min(MAX_TIME, now + 3600))
                state['nextAllowedAt'] = max([state['nextAllowedAt'], entry['nextAllowedAt'], *deadlines])
                if status is not None:
                    state['counters']['responses'] += 1
                successful = status is not None and 200 <= status < 300 and transport_error is None
                if successful:
                    state['counters']['successfulHTTP'] += 1
                    ttl = ASK_CACHE_SECONDS if lease['purpose'] == 'priceoverview' else HISTORY_CACHE_SECONDS
                    entry['cacheUntil'] = max(entry['cacheUntil'], min(MAX_TIME, now + ttl))
                if limited:
                    state['counters']['rateLimited'] += 1
                if transient:
                    state['counters']['transientFailures'] += 1
                if transport_error is not None or status is None:
                    state['counters']['transportFailures'] += 1
                outcome = ('rate_limited' if limited else 'transport_error' if transport_error is not None or status is None
                           else 'transient_failure' if transient else 'http_success' if successful else 'http_error')
            state['counters']['bytesRead'] += bytes_read
            state['counters']['elapsedMs'] = round(state['counters']['elapsedMs'] + elapsed_ms, 3)
            entry.update(activeToken=None, lastFinishedAt=now, lastStatus=status, bytesRead=bytes_read,
                         elapsedMs=elapsed_ms, lastOutcome=outcome,
                         nextAllowedAt=max(entry['nextAllowedAt'], state['nextAllowedAt']))
            event.update(end=now, httpStatus=status, bytesRead=bytes_read, elapsedMs=elapsed_ms,
                         retryAt=state['nextAllowedAt'], outcome=outcome)
            state['lease'] = None
            state['lastClock'], state['revision'] = now, uuid.uuid4().hex
            self._save(state, entry)
            return {'action': 'finished', 'nextAllowedAt': state['nextAllowedAt'],
                    'intervalSeconds': state['intervalSeconds'], 'operatorPaused': state['operatorPaused'],
                    'remainingRequests': state['remainingRequests'], 'successfulHTTPIsSemanticQuote': False}

    def control(self, action, now, expected_revision=None, validation_requests=None,
                minimum_interval_seconds=None):
        now = _now(now)
        if action not in ('pause', 'start_validation'):
            raise GovernorError('Only pause or finite start_validation is supported')
        if minimum_interval_seconds is not None and (not _integer(minimum_interval_seconds, MAXIMUM_INTERVAL_SECONDS)
                                                     or minimum_interval_seconds < MINIMUM_INTERVAL_SECONDS):
            raise GovernorError('Operator interval must be a finite integer of at least60 seconds')
        if action == 'start_validation' and not _integer(validation_requests, 3):
            raise GovernorError('Validation requires zero-to-pause or one-to-three finite permits')
        with self._atomic():
            state = self._load()
            if action == 'pause' or validation_requests == 0:
                state = state if state is not None else _fresh(now)
                state.update(operatorPaused=True, pauseReason='operator_paused')
                if minimum_interval_seconds is not None and minimum_interval_seconds > state['intervalSeconds']:
                    state['intervalSeconds'] = minimum_interval_seconds
                    state['nextAllowedAt'] = max(state['nextAllowedAt'], min(MAX_TIME, now + minimum_interval_seconds))
                state['lastClock'], state['revision'] = max(now, state['lastClock']), uuid.uuid4().hex
                self._save(state)
                return self._public(state, now)
            if state is None or expected_revision != state['revision']:
                raise GovernorError('Initialize paused state and supply its exact current revision')
            if now < state['lastClock']:
                raise GovernorError('Clock rollback requires inspection; no validation granted')
            if state['lease'] is not None:
                raise GovernorError('Active or uncertain lease blocks validation; no lease stealing')
            if now < state['nextAllowedAt']:
                raise GovernorError('Persisted request/backoff deadline cannot be shortened')
            state['intervalSeconds'] = max(state['intervalSeconds'], minimum_interval_seconds or state['intervalSeconds'])
            state.update(operatorPaused=False, pauseReason='validation', remainingRequests=validation_requests,
                         lastClock=now, revision=uuid.uuid4().hex)
            self._save(state)
            return self._public(state, now)
