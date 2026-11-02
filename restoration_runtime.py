"""Portable startup binding for version-one research and current cash prices.

Installation is an explicit startup step after init_db, never an import/GET side
effect. Personal data and legacy research tables stay in the existing database;
the normalized current ledger remains separate and is never overwritten.
"""
from contextlib import closing, contextmanager
from pathlib import Path
import hashlib
import json
import os
import shutil
import sqlite3
import urllib.error
import urllib.parse
import urllib.request

import pipeline_host
import price_features
import price_pipeline
import quote_book
import build_identity


HISTORY_REDIRECT_PREFIX = 'steam_history_redirect:v1:'


def _public_listing_url(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 512 or any(ord(c) < 32 for c in value):
        raise ValueError('Bounded public Steam listing URL required')
    target = urllib.parse.urlsplit(value)
    if (target.scheme != 'https' or target.hostname != 'steamcommunity.com'
            or target.port not in (None, 443) or target.username or target.password or target.fragment
            or not target.path.startswith('/market/listings/730/')
            or not target.path[len('/market/listings/730/'):].strip('/')):
        raise ValueError('Only same-origin HTTPS Steam CS2 market listing redirects are allowed')
    query = urllib.parse.parse_qsl(target.query, keep_blank_values=True)
    if len(query) > 3 or len({key for key, value in query}) != len(query):
        raise ValueError('Unexpected Steam listing redirect query')
    for key, item in query:
        if key not in ('l', 'cc', 'currency') or not item or len(item) > 32 or not all(c.isascii() and (c.isalnum() or c in '_-') for c in item):
            raise ValueError('Only public locale/currency parameters may follow a canonical redirect')
    return value


def remember_history_redirect(book, source, location):
    """Remember one observed canonical migration; never performs another HTTP call."""
    source = _public_listing_url(source)
    if not isinstance(location, str) or len(location) > 512:
        raise ValueError('Invalid bounded Steam redirect destination')
    target = _public_listing_url(urllib.parse.urljoin(source, location))
    if source == target:
        raise ValueError('Steam listing redirect loop rejected')
    key = HISTORY_REDIRECT_PREFIX + hashlib.sha256(source.encode()).hexdigest()
    with book.lock, book.connect(True) as connection:
        # Public canonical-address cache only; quote/history/control rows are
        # untouched. Bound this operational cache independently of holdings.
        if not connection.execute('SELECT 1 FROM meta WHERE key=?', (key,)).fetchone():
            rows = connection.execute('SELECT key FROM meta WHERE key LIKE ? ORDER BY key LIMIT 64', (HISTORY_REDIRECT_PREFIX + '%',)).fetchall()
            if len(rows) >= 64:
                connection.execute('DELETE FROM meta WHERE key=?', (rows[0]['key'],))
        value = quote_book.encode({'version': 1, 'source': source, 'target': target, 'recordedAt': int(book.clock())})
        connection.execute('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))
    return target


def resolved_history_url(book, source):
    if not urllib.parse.urlsplit(source).path.startswith('/market/listings/730/'):
        return source
    _public_listing_url(source)
    key = HISTORY_REDIRECT_PREFIX + hashlib.sha256(source.encode()).hexdigest()
    with book.connect() as connection:
        row = connection.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    if not row:
        return source
    try:
        value = json.loads(row['value'])
        if (value.get('version') != 1 or value.get('source') != source
                or type(value.get('recordedAt')) is not int
                or not 0 <= int(book.clock()) - value['recordedAt'] <= 30 * 86400):
            return source
        return _public_listing_url(value['target'])
    except (ValueError, TypeError, KeyError):
        return source


def research_capabilities(app, pipeline):
    """Report prerequisites separately from whether routes are implemented."""
    with closing(app.steam_ask_db()) as connection:
        raw_history = bool(connection.execute("SELECT 1 FROM price_history WHERE provider='steam' LIMIT 1").fetchone())
        metrics = bool(connection.execute("SELECT 1 FROM item_market_metrics WHERE provider='market' LIMIT 1").fetchone())
        lookup = bool(connection.execute('SELECT 1 FROM market_item_lookup LIMIT 1').fetchone())
    steam = next(provider for provider in pipeline.book.providers() if provider['id'] == 'steam')
    now = int(pipeline.clock())
    return {'ok': True, 'researchRoutesImplemented': True,
            'savedResearchLookupAvailable': lookup, 'savedResearchMetricsAvailable': metrics,
            'rawSteamHistoryAvailable': raw_history,
            'historyAcquisition': {
                'networkEnabled': pipeline.allow_network, 'providerEnabled': bool(steam['enabled']),
                'operatorPaused': not bool(steam['enabled']), 'requestInFlight': bool(steam['lease']),
                'nextAllowedAt': steam['next_allowed'], 'lastError': steam['last_error'],
                'admissionReady': bool(pipeline.allow_network and steam['enabled'] and not steam['lease'] and steam['next_allowed'] <= now),
                'capacityCheckedAtAdmission': True, 'sharedWalletAndHistoryGate': True,
                'method': 'Configured Steam history session, or one public listing request when no session is configured; no automatic fallback request.',
            },
            'missingHistoryBehavior': 'Empty genuine curve with coverage and prerequisites; no current-price backfill.',
            'legacyStartupResume': os.environ.get('CS2DASH_RESUME_RESEARCH_TASKS', '0').strip().lower() in ('1', 'true', 'yes'),
            'legacyAutomaticRefresh': os.environ.get('CS2DASH_RESEARCH_AUTO_REFRESH', '0').strip().lower() in ('1', 'true', 'yes')}


def _install_shared_steam(app, pipeline, capacity):
    """One persisted origin admission for chart history and current wallet asks."""
    book = pipeline.book
    original_fetch = pipeline._fetch

    def fetch(source, name=None):
        if source != 'steam':
            return original_fetch(source, name)
        with app.STEAM_NETWORK_SLOT:
            return original_fetch(source, name)

    @contextmanager
    def history_open(opener, request, timeout):
        original_url = request.full_url
        target = urllib.parse.urlsplit(request.full_url)
        if (target.scheme != 'https' or target.hostname != 'steamcommunity.com'
                or target.port not in (None, 443) or target.username or target.password
                or not (target.path in ('/market/pricehistory/', '/market/priceoverview/')
                        or target.path.startswith('/market/listings/730/'))):
            raise ValueError('Only the existing Steam market history/ask endpoints are admitted')
        if not pipeline.allow_network:
            raise app.SteamGovernorDeferred({'reason': 'offline', 'operatorPaused': True,
                                            'retryAt': None, 'recoveryRequired': False})
        with app.STEAM_NETWORK_SLOT:
            capacity()
            claim = book.claim('steam')
            if claim['action'] != 'request':
                source = next(p for p in book.providers() if p['id'] == 'steam')
                raise app.SteamGovernorDeferred({
                    'reason': claim.get('reason', claim['action']),
                    'operatorPaused': not bool(source['enabled']),
                    'retryAt': claim.get('retryAt'),
                    'recoveryRequired': claim.get('reason') == 'uncertain_request_recovery',
                })
            try:
                # Preserve the configured cookie processor/opener, while
                # rejecting redirects before a second endpoint can be fetched.
                if callable(getattr(opener, 'add_handler', None)):
                    opener.add_handler(app._NoRedirect())
                resolved = resolved_history_url(book, original_url)
                if resolved != original_url:
                    request = urllib.request.Request(resolved, headers=dict(request.header_items()), method='GET')
                with opener.open(request, timeout=min(float(timeout), 20)) as response:
                    status = response.status
                    if response.geturl() != request.full_url or not 200 <= status < 300:
                        raise ValueError('Unexpected Steam response target or HTTP status')
                    yield app.BoundedSteamResponse(response)
                book.finish_request_without_quote(claim, status)
            except urllib.error.HTTPError as error:
                if error.code in (301, 302, 303, 307, 308):
                    try:
                        if request.full_url != original_url:
                            raise ValueError('Steam canonical redirect chain exceeds the one-hop limit')
                        remember_history_redirect(book, original_url, error.headers.get('Location') if error.headers else None)
                    except (ValueError, TypeError) as redirect_error:
                        book.fail(claim, error.code, str(redirect_error))
                        error.close()
                        raise ValueError(str(redirect_error)) from None
                    book.finish_request_without_quote(claim, error.code)
                    source = next(p for p in book.providers() if p['id'] == 'steam')
                    error.close()
                    raise app.SteamGovernorDeferred({'reason': 'canonical_history_redirect',
                        'operatorPaused': not bool(source['enabled']), 'retryAt': source['next_allowed'],
                        'recoveryRequired': False, 'canonicalRedirect': True,
                        'networkAttempted': True, 'httpStatus': error.code}) from None
                retry_after = error.headers.get('Retry-After') if error.headers else None
                if not isinstance(retry_after, str) or len(retry_after) > 128:
                    retry_after = None
                book.fail(claim, error.code, 'Steam history HTTP ' + str(error.code), retry_after)
                error.close()
                raise
            except BaseException as error:
                book.fail(claim, None, 'Steam history transport ' + type(error).__name__)
                raise

    pipeline._fetch = fetch
    app.steam_network_open = history_open


def install(app, quote_path=None, allow_network=None):
    path = Path(quote_path or os.environ.get('CS2DASH_QUOTE_DB')
                or Path(app.DATA_DIR) / 'prices-current.sqlite').resolve()
    existing = getattr(app, '_RESTORATION_PIPELINE', None)
    if existing is not None:
        if path != app._RESTORATION_QUOTE_PATH:
            raise ValueError('Current quote authority is already installed at another path')
        return existing

    def database_connection(write=False, shared_read=False):
        if write and shared_read:
            raise ValueError('Shared holdings connections must be read-only')
        connection = sqlite3.connect(
            Path(app.DB_FILE).resolve().as_uri() + ('?mode=rw' if write else '?mode=ro'),
            uri=True, timeout=0.5, check_same_thread=not shared_read)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA busy_timeout=500')
            if not write:
                connection.execute('PRAGMA query_only=ON')
            return connection
        except BaseException:
            connection.close()
            raise

    def capacity():
        # Preserve the established floor on the actual application volumes.
        for directory in {Path(app.DATA_DIR).resolve(), path.parent}:
            if shutil.disk_usage(directory).free < 10 * 1024 ** 3:
                raise app.cash_prices.CashError('Acquisition paused: 10 GiB free-space floor on ' + str(directory))

    original_held = app.cash_held_rows

    def held(portfolio_id, connection=None):
        if connection is None:
            with closing(database_connection()) as opened:
                opened.execute('BEGIN')
                return held(portfolio_id, opened)
        scope, rows, display = original_held(portfolio_id, connection)
        # Carry forward explicit saved identity resolutions from the current
        # host when present; a fresh database needs no compatibility schema.
        if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='lean_held_identity'").fetchone():
            return scope, rows, display
        overrides = {}
        names = list(dict.fromkeys(row['displayName'] for row in rows))
        for start in range(0, len(names), 256):
            chunk = names[start:start + 256]
            marks = ','.join('?' for _ in chunk)
            for row in connection.execute('SELECT name,market_hash_name,identity_issue FROM lean_held_identity WHERE name IN (' + marks + ')', chunk):
                overrides[row['name']] = row
        for row in rows:
            saved = overrides.get(row['displayName'])
            if saved is not None:
                row.update(marketHashName=saved['market_hash_name'], identityIssue=saved['identity_issue'])
        return scope, rows, display

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        quote_book.initialize(path, price_pipeline.PROVIDERS)
        book = quote_book.Book(path)
        reference = next(provider for provider in price_pipeline.PROVIDERS if provider['id'] == 'skinport_sales')
        price_features.initialize(book, reference)
    else:
        # Existing quotes, leases, cooldowns, provider activation and history are
        # opened as-is. Missing feature schema requires an explicit migration.
        book = quote_book.Book(path)
        if not book.has_references:
            raise ValueError('Existing current quote ledger needs the explicit current-features migration')

    app.steam_ask_db = database_connection
    app.cash_price_capacity = capacity
    app.cash_held_rows = held
    if allow_network is None:
        allow_network = os.environ.get('CS2DASH_CURRENT_NETWORK', '1').strip().lower() not in ('0', 'false', 'no')
    pipeline = pipeline_host.install(app, path, held, bool(allow_network))
    _install_shared_steam(app, pipeline, capacity)
    app.research_capabilities = lambda: research_capabilities(app, pipeline)
    app.BUILD_IDENTITY = build_identity.capture(app.BASE_DIR, app.APP_VERSION)
    app.ANON_GET_PATHS.update({
        '/api/current-analysis', '/api/current-report', '/current-report',
        '/api/current-market-items', '/api/price-sources', '/api/portfolio-history',
        '/api/report', '/report.html',
        '/api/research-capabilities',
    })
    app._RESTORATION_PIPELINE = pipeline
    app._RESTORATION_QUOTE_PATH = path
    return pipeline
