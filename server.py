#!/usr/bin/env python3
"""
cs2dash v1.0

Local-first CS2 portfolio and market intelligence server. The app serves a
single-page dashboard, stores portfolio imports, caches provider prices and
Steam history in SQLite, and exposes REST endpoints for market search,
valuation, taxonomy analysis, and group history.
"""

import http.server
import atexit
import html
import json
import os
import re
import sys
import socket
import sqlite3
import time
import datetime
import email.utils
import gzip
import hashlib
import hmac
import secrets
import http.cookiejar
import math
import threading
import collections
import http.client
import contextlib
import uuid
import steam_held_asks
import cash_prices
import canonical_cash
import compact_cash_service
import holdings_cache
import cash_cadence
import bulk_derived_staging
import generation_repository
import generation_host
import bulk_staging_host
import bulk_staging_runtime
import market_slice_cache
import steam_request_governor
import shutil
import urllib.parse
import urllib.request
from collections import deque
from contextlib import closing
from pathlib import Path

import catalog_store

APP_NAME = 'cs2dash'
APP_VERSION = '1.0.1'
CATALOG_STORE_ENABLED = os.environ.get('CS2DASH_CATALOG_STORE_ENABLED', '0').strip().lower() in ('1', 'true', 'yes')
CATALOG_STORE_WRITES = os.environ.get('CS2DASH_CATALOG_STORE_WRITES', '0').strip().lower() in ('1', 'true', 'yes')
CATALOG_COMPATIBILITY_FEED_ENABLED = os.environ.get('CS2DASH_CATALOG_COMPATIBILITY_FEED_ENABLED', '0').strip().lower() in ('1', 'true', 'yes')
if CATALOG_COMPATIBILITY_FEED_ENABLED and not CATALOG_STORE_ENABLED:
    raise RuntimeError('CS2DASH_CATALOG_COMPATIBILITY_FEED_ENABLED requires CS2DASH_CATALOG_STORE_ENABLED=1.')
DERIVED_GENERATION_READS_ENABLED = False  # Explicit host admission only; no environment auto-activation.
DERIVED_GENERATION_REPOSITORY = None
CATALOG_INDEX_CACHE = None
CATALOG_INDEX_LOCK = threading.Lock()
ITEM_READ_PATHS = {
    '/api/items', '/api/items/resolve', '/api/items/detail',
    '/api/items/current', '/api/items/history', '/api/items/evidence', '/api/items/revisions',
    '/api/items/payload',
    '/api/items/legacy-streams', '/api/items/legacy-history',
}

# Config
try:
    PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
except (TypeError, ValueError):
    PORT = 8080
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('CS2DASH_DATA_DIR') or os.path.join(BASE_DIR, 'data')
SNAPS_FILE    = os.path.join(DATA_DIR, 'snaps.json')
PORTFOLIOS_FILE = os.path.join(DATA_DIR, 'portfolios.json')
SETTINGS_FILE = os.path.join(DATA_DIR, 'settings.json')
DB_FILE       = os.path.join(DATA_DIR, 'cs2dash.sqlite')
APPID         = 730
STEAM_HISTORY_TTL = 6 * 60 * 60
STEAM_HISTORY_FAILURE_TTL = 24 * 60 * 60
STEAM_HISTORY_FETCH_VERSION = 2
STEAM_HISTORY_FETCH_TIMEOUT = float(os.environ.get('STEAM_HISTORY_FETCH_TIMEOUT', '12') or 12)
CATALOG_TTL   = 7 * 24 * 60 * 60
DEFAULT_SYNC_STEAM_HISTORY_LIMIT = int(os.environ.get('CS2DASH_SYNC_STEAM_HISTORY_LIMIT', '120') or 120)
ITEM_IDX      = {'n': 0, 'q': 1, 'pct': 2, 'd': 3, 'ct': 4, 'pt': 5, 'cat': 6, 'grp': 7}
VOLATILITY_WINDOWS = {1, 3, 7, 14, 30, 60, 90, 180, 365, 0}
VOLATILITY_DETAIL_WINDOWS = (7, 30, 90)
RESEARCH_METRIC_WINDOWS = tuple(sorted(VOLATILITY_WINDOWS))
OBSERVATION_BUCKET_SECONDS = 15 * 60
PROVIDER_SNAPSHOT_INGESTION_VERSION = 1

CATALOG_SOURCES = {
    'skins': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/skins_not_grouped.json',
    'skin_families': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/skins.json',
    'stickers': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/stickers.json',
    'crates': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/crates.json',
    'collections': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/collections.json',
    'graffiti': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/graffiti.json',
    'patches': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/patches.json',
    'music_kits': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/music_kits.json',
    'charms': 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/keychains.json',
}

# Keep the nine legacy source names and their listing identities unchanged.
# The typed repository additionally covers published classes that have no safe
# name-keyed legacy representation. Never ingest all.json alongside these feeds.
CATALOG_LEGACY_KINDS = frozenset(CATALOG_SOURCES)
CATALOG_REPOSITORY_SOURCES = {**CATALOG_SOURCES, **{
    kind: 'https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/' + filename + '.json'
    for kind, filename in (
        ('agents', 'agents'), ('keys', 'keys'), ('collectibles', 'collectibles'),
        ('sticker_slabs', 'sticker_slabs'), ('base_weapons', 'base_weapons'),
        ('highlights', 'highlights'), ('pets', 'pets'), ('tools', 'inventory'))}}
CATALOG_DOCUMENTED_INVENTORY_CATEGORIES = frozenset((
    'skins', 'crates', 'collectibles', 'stickers', 'graffiti', 'music_kits',
    'keychains', 'highlights', 'agents', 'patches', 'keys', 'sticker_slabs', 'tools', 'pets'))
CATALOG_CONTRACT_VERSION = 1

os.makedirs(DATA_DIR, exist_ok=True)

LOCAL_ADDRS = {'127.0.0.1', '::1', '::ffff:127.0.0.1'}
SQLITE_BUSY_TIMEOUT_MS = 30000
DATA_TASK_PERSIST_BUSY_TIMEOUT_MS = 250
SQLITE_WRITE_LOCK = threading.RLock()
STEAM_REQUEST_LOCK = threading.Lock()
DATA_TASK_LOCK = threading.RLock()
DATA_TASKS = {}
DATA_TASK_WORKERS = {}
DATA_TASK_WAKE = threading.Event()
DATA_TASK_STOP_EVENT = threading.Event()
DATA_TASK_CLOCK = time.time
DATA_TASK_MAX_WAIT_SECONDS = 24 * 60 * 60
DATA_TASK_EVENTS = deque(maxlen=240)
DATA_TASK_PERSIST_BACKLOG = deque(maxlen=600)
DATA_TASK_PERSIST_NOTICE_AT = 0.0
ACTIVE_DATA_TASK_ID = None
DATA_TASK_RESUME_ENABLED = os.environ.get('DATA_TASK_RESUME_ENABLED', '1').strip().lower() not in ('0', 'false', 'no')
DATA_TASK_RESUME_MAX_AGE_SECONDS = int(os.environ.get('DATA_TASK_RESUME_MAX_AGE_SECONDS', str(24 * 60 * 60)))
# Pure, bounded single-item ask access; no startup fetch or database work.
def validate_market_hash(value):
    # Do not strip, normalize, guess variants, or accept a batch.
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value.encode('utf-8')) > 512
            or any(ord(char) < 32 or 127 <= ord(char) < 160 for char in value)):
        raise ValueError('One exact Steam market hash name is required (maximum 512 UTF-8 bytes).')
    return value


def parse_retry_after(value, now):
    if not isinstance(value, str) or len(value) > 128:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        return int(now) + int(value)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is not None:
            return max(int(now), int(parsed.timestamp()))
    except (TypeError, ValueError, OverflowError):
        pass
    return None


class SteamRequestCooldown(Exception):
    def __init__(self, state):
        super().__init__('Steam requests paused by the shared cooldown')
        self.state = dict(state)


class SteamAccessGate:
    """One process-wide pacing/deadline gate for existing history and new asks."""
    def __init__(self, min_interval=1.25, not_before=0, clock=time.time, sleep=time.sleep):
        self.lock = threading.Lock()
        self.clock, self.sleep = clock, sleep
        self.min_interval = max(0.0, float(min_interval))
        self.last_request_at = 0.0
        self.deadline = 0
        self.reason = None
        self.retry_after = None
        self.upstream_retry_at = None
        if not_before:
            self.honor_deadline(int(not_before), 'operator_not_before')

    def honor_deadline(self, deadline, reason='saved_history_retry_after'):
        if type(deadline) is not int:
            return
        with self.lock:
            if deadline > self.deadline:
                self.deadline = deadline
                self.reason = reason
                self.retry_after = None
                self.upstream_retry_at = deadline if reason == 'saved_history_retry_after' else None

    def _state(self):
        return {'retryAt': self.deadline, 'cooldownReason': self.reason,
                'retryAfter': self.retry_after, 'upstreamRetryAt': self.upstream_retry_at}

    def cooldown(self):
        with self.lock:
            return self._state() if self.deadline > self.clock() else None

    def note_429(self, retry_after, upstream_retry_at):
        with self.lock:
            now = int(self.clock())
            explicit = type(upstream_retry_at) is int and upstream_retry_at > now
            deadline = upstream_retry_at if explicit else now + 3600
            if deadline >= self.deadline:
                self.deadline = deadline
                self.reason = 'upstream_retry_after' if explicit else 'local_429_backoff'
                self.retry_after = retry_after
                self.upstream_retry_at = upstream_retry_at
            return self._state()

    def throttle(self, url):
        if urllib.parse.urlsplit(str(url)).hostname != 'steamcommunity.com':
            return
        with self.lock:
            if self.deadline > self.clock():
                raise SteamRequestCooldown(self._state())
            wait = self.min_interval - (self.clock() - self.last_request_at)
            if wait > 0:
                self.sleep(wait)
            if self.deadline > self.clock():
                raise SteamRequestCooldown(self._state())
            self.last_request_at = self.clock()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _NoSteamRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Other providers retain their redirects, but cannot enter Steam outside its gate.
        target = urllib.parse.urlsplit(newurl)
        if (target.hostname or '').lower().rstrip('.') == 'steamcommunity.com':
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def parse_usd_ask(value):
    # currency=1 and l=english must yield an unambiguous dollar amount.
    if not isinstance(value, str) or len(value) > 40:
        return None
    match = re.fullmatch(r'\$(\d{1,9}|\d{1,3}(?:,\d{3}){1,2})\.(\d{2})(?: USD)?', value)
    if not match:
        return None
    cents = int(match[1].replace(',', '')) * 100 + int(match[2])
    return cents if 0 < cents <= 99999999999 else None


class SteamCurrentAskService:
    SUCCESS_TTL = 60
    NEGATIVE_TTL = 300
    MAX_ENTRIES = 64
    MAX_RESPONSE_BYTES = 16384
    TIMEOUT_SECONDS = 6

    def __init__(self, gate, clock=time.time, open_url=None, retry_parser=parse_retry_after):
        self.gate, self.clock = gate, clock
        self.open_url = open_url
        self.retry_parser = retry_parser
        self.lock = threading.Lock()
        self.cache = collections.OrderedDict()

    def _base(self, name):
        query = urllib.parse.urlencode({'appid': 730, 'currency': 1, 'l': 'english',
                                       'market_hash_name': name})
        return {'marketHashName': name, 'appid': 730, 'currency': 'USD', 'currencyId': 1,
                'provider': 'steam', 'source': 'steam_priceoverview',
                'priceBasis': 'lowest_ask', 'marketObservedAt': None,
                'requestUrl': 'https://steamcommunity.com/market/priceoverview/?' + query,
                'listingUrl': 'https://steamcommunity.com/market/listings/730/' + urllib.parse.quote(name, safe='')}

    def _remember(self, name, result, ttl):
        self.cache[name] = (self.clock() + ttl, dict(result))
        self.cache.move_to_end(name)
        while len(self.cache) > self.MAX_ENTRIES:
            self.cache.popitem(last=False)
        return result

    def fetch(self, name, saved_deadline=None):
        name = validate_market_hash(name)
        base = self._base(name)
        if not self.lock.acquire(blocking=False):
            return dict(base, success=False, status='busy', error='Another Steam ask request is in progress.',
                        fetchedAt=None, lowestAsk=None, cached=False)
        try:
            cached = self.cache.get(name)
            if cached and cached[0] > self.clock():
                result = dict(cached[1], cached=True)
                if result['success']:
                    result['status'] = 'cached'
                self.cache.move_to_end(name)
                return result
            self.cache.pop(name, None)
            if saved_deadline is not None:
                try:
                    self.gate.honor_deadline(saved_deadline())
                except Exception:
                    # No request when an existing persisted provider block cannot be checked.
                    return self._remember(name, dict(base, success=False, status='unavailable',
                        error='Steam cooldown state is unavailable. Try again later.',
                        fetchedAt=None, lowestAsk=None, cached=False, retryAt=int(self.clock()) + 60), 60)
            try:
                blocked = self.gate.cooldown()
                if blocked:
                    raise SteamRequestCooldown(blocked)
            except SteamRequestCooldown as exc:
                return dict(base, success=False, status='cooldown', fetchedAt=None, lowestAsk=None,
                            cached=False, httpStatus=None, error='Steam requests are paused.', **exc.state)
            req = urllib.request.Request(base['requestUrl'], headers={
                'User-Agent': 'CS2-Dash/SteamCurrentAsk', 'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'identity'})
            try:
                # A fresh opener has no cookie processor, account state, or redirect fallback.
                opener = self.open_url or urllib.request.build_opener(_NoRedirect()).open
                with opener(req, timeout=self.TIMEOUT_SECONDS) as response:
                    if response.geturl() != base['requestUrl'] or response.status != 200:
                        raise ValueError('Unexpected Steam response target or status')
                    if response.headers.get('Content-Encoding', 'identity').lower() not in ('', 'identity'):
                        raise ValueError('Unexpected compressed Steam response')
                    body = response.read(self.MAX_RESPONSE_BYTES + 1)
                    if len(body) > self.MAX_RESPONSE_BYTES:
                        raise ValueError('Steam response exceeded the size limit')
                    data = json.loads(body.decode('utf-8'))
                received_precise = self.clock()
                received_at = int(received_precise)
                if not isinstance(data, dict) or data.get('success') is not True:
                    raise ValueError('Steam did not provide a successful price response')
                # priceoverview does not echo item identity: bind only to the exact fixed request URL.
                # If an upstream identity/currency appears, reject any mismatch rather than relabel it.
                if (data.get('market_hash_name', name) != name
                        or type(data.get('currency', 'USD')) is bool
                        or data.get('currency', 'USD') not in ('USD', 1, '1')
                        or data.get('appid', 730) not in (730, '730')):
                    raise ValueError('Steam response identity or currency did not match the request')
                cents = parse_usd_ask(data.get('lowest_price'))
                if cents is None:
                    return self._remember(name, dict(base, success=False, status='unavailable',
                        error='Steam did not return a valid USD lowest ask.', lowestAsk=None,
                        fetchedAt=received_at, cached=False, retryAt=received_at + self.NEGATIVE_TTL), self.NEGATIVE_TTL)
                # volume is traded volume, never quantity available for sale. Do not expose it here.
                return self._remember(name, dict(base, success=True, status='fetched',
                    receiptId=uuid.uuid4().hex, receivedAtMicros=int(received_precise * 1000000),
                    lowestAsk=cents / 100, lowestAskCents=cents, rawLowestPrice=data['lowest_price'], fetchedAt=received_at,
                    expiresAt=received_at + self.SUCCESS_TTL, cached=False,
                    identityEvidence='exact_request_url; response does not normally echo identity'), self.SUCCESS_TTL)
            except urllib.error.HTTPError as exc:
                code = exc.code
                raw_retry = exc.headers.get('Retry-After') if exc.headers is not None else None
                retry_after = raw_retry if isinstance(raw_retry, str) and len(raw_retry) <= 128 else None
                exc.close()
                if code == 429:
                    state = self.gate.note_429(retry_after, self.retry_parser(retry_after, int(self.clock())))
                    return dict(base, success=False, status='rate_limited', fetchedAt=None, lowestAsk=None,
                        cached=False, httpStatus=429, error='Steam rate limited this request (HTTP 429).', **state)
                return self._remember(name, dict(base, success=False, status='failed', fetchedAt=None,
                    lowestAsk=None, cached=False, httpStatus=code, error='Steam ask request failed.',
                    retryAt=int(self.clock()) + self.NEGATIVE_TTL), self.NEGATIVE_TTL)
            except (OSError, ValueError, UnicodeError, RecursionError, http.client.HTTPException):
                return self._remember(name, dict(base, success=False, status='failed', fetchedAt=None,
                    lowestAsk=None, cached=False, error='Steam ask request failed or returned an invalid response.',
                    retryAt=int(self.clock()) + self.NEGATIVE_TTL), self.NEGATIVE_TTL)
        finally:
            self.lock.release()


STEAM_LAST_REQUEST_AT = 0.0
STEAM_MIN_INTERVAL = float(os.environ.get('STEAM_MIN_INTERVAL', '1.25'))
STEAM_ACCESS_GATE = SteamAccessGate(STEAM_MIN_INTERVAL, os.environ.get('CS2DASH_STEAM_RETRY_NOT_BEFORE', '0'))
STEAM_CURRENT_ASK = SteamCurrentAskService(STEAM_ACCESS_GATE,
    open_url=lambda request, timeout: steam_ask_public_open(request, timeout),
    retry_parser=lambda value, now: steam_retry_after_timestamp(value, now))
MARKET_REFRESH_INTERVAL_SECONDS = int(os.environ.get('MARKET_REFRESH_INTERVAL_SECONDS', str(6 * 60 * 60)))
AUTO_MARKET_REFRESH = os.environ.get('AUTO_MARKET_REFRESH', '1').strip().lower() not in ('0', 'false', 'no')
MARKET_BULK_SOURCE = os.environ.get('MARKET_BULK_SOURCE', 'any').strip().lower()
MARKET_SOURCE_PRIORITY = [s.strip().lower() for s in os.environ.get(
    'MARKET_SOURCE_PRIORITY',
    'steam,csfloat,buff163,youpin,skinport'
).split(',') if s.strip()]
CSGOTRADER_PRICE_BASE = os.environ.get('CSGOTRADER_PRICE_BASE', 'https://prices.csgotrader.app/latest').rstrip('/')
CSGOTRADER_SOURCE_LABELS = {
    'steam': 'Steam',
    'csfloat': 'CSFloat',
    'buff163': 'BUFF163',
    'youpin': 'Youpin',
    'skinport': 'Skinport',
}
PRICE_PROVIDER_LABELS = {
    'steam': 'Legacy Steam',
    'steam_snapshot': 'Steam',
    'steam_history': 'Steam sale history',
    'csfloat': 'CSFloat',
    'buff163': 'BUFF163',
    'youpin': 'Youpin',
    'skinport': 'Skinport',
}
MARKET_ITEM_PROVIDER_COLUMNS = {
    'steam': 'steam_price',
    'steam_snapshot': 'steam_snapshot_price',
    'steam_history': 'steam_history_price',
    'csfloat': 'csfloat_price',
    'buff163': 'buff_price',
    'youpin': 'youpin_price',
    'skinport': 'skinport_price',
}
CURRENT_PRICE_PROVIDERS = ('steam_snapshot', 'csfloat', 'buff163', 'skinport', 'youpin')
REFERENCE_PRICE_PROVIDERS = ('steam', 'steam_history')
PRIMARY_PRICE_PROVIDER_ORDER = CURRENT_PRICE_PROVIDERS
METRIC_BUILD_VERSION = 6
LOOKUP_BUILD_VERSION = 16
COLLECTION_CASE_ALIAS_CACHE = None
AUTO_REFRESH_STATUS_LOCK = threading.Lock()
AUTO_REFRESH_STATUS = {}
IDLE_WAL_ANCHOR = None
IDLE_WAL_ANCHOR_OWNER = None
IDLE_WAL_ANCHOR_ATEXIT_REGISTERED = False
CASH_PRICE_SERVICE = None
CASH_PRICE_HOLDINGS_CACHE = None
CASH_PRICE_SERVICE_LOCK = threading.Lock()

# --- Authentication / hardening ---
# Bind to loopback by default so the app is only reachable through a reverse
# proxy. Set CS2DASH_BIND=0.0.0.0 for direct LAN access (no auth on the wire).
BIND_HOST = (os.environ.get('CS2DASH_BIND', '127.0.0.1') or '127.0.0.1').strip()
# Single-user login. Configure a username and a PBKDF2 hash (generate one with
# `python server.py --hash-password`). CS2DASH_AUTH_PASSWORD is a convenience
# for dev that hashes a plaintext password at startup.
AUTH_ENABLED = (os.environ.get('CS2DASH_AUTH_DISABLE', '') or '').strip().lower() not in ('1', 'true', 'yes')
AUTH_USERNAME = (os.environ.get('CS2DASH_AUTH_USER', 'admin') or 'admin').strip()
AUTH_PASSWORD_HASH = (os.environ.get('CS2DASH_AUTH_PASSWORD_HASH', '') or '').strip()
AUTH_PASSWORD_PLAIN = os.environ.get('CS2DASH_AUTH_PASSWORD', '') or ''
SESSION_COOKIE = 'cs2dash_session'
SESSION_TTL_SECONDS = int(os.environ.get('CS2DASH_SESSION_TTL', str(30 * 24 * 60 * 60)) or (30 * 24 * 60 * 60))
# Mark the session cookie Secure so browsers only send it over HTTPS. Behind the
# reverse proxy the browser connection is HTTPS, so this should stay on. Set
# CS2DASH_COOKIE_SECURE=0 only when testing over plain HTTP locally.
COOKIE_SECURE = (os.environ.get('CS2DASH_COOKIE_SECURE', '1') or '1').strip().lower() not in ('0', 'false', 'no')
# Public mode exposes a narrow read-only view while keeping every write behind
# the existing single-user login. It is off by default.
PUBLIC_READONLY = (os.environ.get('CS2DASH_PUBLIC_READONLY', '') or '').strip().lower() in ('1', 'true', 'yes')
# Reject oversized request bodies before reading them into memory.
MAX_BODY_BYTES = int(os.environ.get('CS2DASH_MAX_BODY_BYTES', str(64 * 1024 * 1024)) or (64 * 1024 * 1024))
# Resolved at startup by resolve_auth_hash().
AUTH_HASH = ''
PBKDF2_ITERATIONS = 200000
PUBLIC_GET_PATHS = {'/login', '/favicon.ico', '/api/health'}
PUBLIC_POST_PATHS = {'/api/login'}
ANON_GET_PATHS = {
    '/api/cash-value', '/api/cash-price-jobs',
    '/api/canonical-cash-value', '/api/canonical-cash-prices',
    '/api/steam-ask-value', '/api/steam-ask-jobs',
    '/', '/index.html', '/report', '/api/status', '/api/snaps',
    '/api/portfolios', '/api/access', '/api/catalog', '/api/prices',
    '/api/history', '/api/analysis', '/api/market-universe',
    '/api/market-items', '/api/taxonomy-analysis', '/api/group-history',
    '/api/group-item-history', '/api/inventory',
    '/api/shared-pricing-status', '/api/shared-item-pricing',
    '/api/market-universe-status', '/api/data-tasks',
}


def anon_sanitize_query(query):
    """Return anonymous query parameters with every refresh control disabled."""
    sanitized = {key: list(values) for key, values in query.items()}
    for key in ('force', 'refresh', 'refresh_history'):
        sanitized[key] = ['0']
    sanitized['refresh_limit'] = ['0']
    return sanitized


def hash_password(password, iterations=PBKDF2_ITERATIONS, salt=None):
    """Return an encoded PBKDF2-SHA256 hash: pbkdf2_sha256$iters$salthex$hashhex."""
    if salt is None:
        salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)
    return f'pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}'


def verify_password(password, encoded):
    try:
        algo, iters, salt_hex, hash_hex = encoded.split('$')
        if algo != 'pbkdf2_sha256':
            return False
        iterations = int(iters)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except (ValueError, AttributeError):
        return False
    dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)
    return hmac.compare_digest(dk, expected)


def resolve_auth_hash():
    """Pick the configured password hash, hashing a plaintext password if given."""
    if AUTH_PASSWORD_HASH:
        return AUTH_PASSWORD_HASH
    if AUTH_PASSWORD_PLAIN:
        return hash_password(AUTH_PASSWORD_PLAIN)
    return ''


def create_session(username):
    token = secrets.token_urlsafe(32)
    now = int(time.time())
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute(
                'INSERT INTO auth_sessions(token,username,created_at,expires_at) VALUES(?,?,?,?)',
                (token, username, now, now + SESSION_TTL_SECONDS),
            )
    return token


def get_session_user(token, readonly=False):
    if not token:
        return None
    now = int(time.time())
    username = None
    expired = False
    with (closing(catalog_read_db()) if readonly else db()) as conn:
        row = conn.execute(
            'SELECT username,expires_at FROM auth_sessions WHERE token=?', (token,)
        ).fetchone()
        if not row:
            return None
        username = row['username']
        expired = row['expires_at'] < now
    if expired:
        if not readonly:
            delete_session(token)
        return None
    return username


def delete_session(token):
    if not token:
        return
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute('DELETE FROM auth_sessions WHERE token=?', (token,))


def purge_expired_sessions():
    now = int(time.time())
    try:
        with SQLITE_WRITE_LOCK:
            with db() as conn:
                conn.execute('DELETE FROM auth_sessions WHERE expires_at < ?', (now,))
    except sqlite3.Error:
        pass


def build_session_cookie(token, max_age):
    parts = [
        f'{SESSION_COOKIE}={token}',
        'Path=/',
        'HttpOnly',
        'SameSite=Strict',
        f'Max-Age={int(max_age)}',
    ]
    if COOKIE_SECURE:
        parts.append('Secure')
    return '; '.join(parts)


# Helpers
def read_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def write_json(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _json_safe(value, _seen=None):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if _seen is None:
        _seen = set()
    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in _seen:
            return None
        _seen.add(obj_id)
        try:
            return {str(k): _json_safe(v, _seen) for k, v in value.items()}
        finally:
            _seen.discard(obj_id)
    if isinstance(value, (list, tuple, set, deque)):
        obj_id = id(value)
        if obj_id in _seen:
            return []
        _seen.add(obj_id)
        try:
            return [_json_safe(v, _seen) for v in value]
        finally:
            _seen.discard(obj_id)
    return str(value)


def _json_loads_or(value, default):
    if value in (None, ''):
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _data_task_from_row(row, events=None):
    task = {
        'id': row['id'],
        'name': row['name'],
        'mode': row['mode'],
        'pullMode': row['pull_mode'],
        'status': row['status'],
        'stage': row['stage'],
        'message': row['message'],
        'createdAt': row['created_at'],
        'startedAt': row['started_at'],
        'updatedAt': row['updated_at'],
        'finishedAt': row['finished_at'],
        'options': _json_loads_or(row['options_json'], {}),
        'progress': _json_loads_or(row['progress_json'], {}),
        'result': _json_loads_or(row['result_json'], None),
        'error': row['error'],
        'events': events or [],
    }
    return {k: v for k, v in task.items() if v is not None}


def _data_task_event_from_row(row):
    return {
        'taskId': row['task_id'],
        'ts': row['ts'],
        'stage': row['stage'],
        'message': row['message'],
        'payload': _json_loads_or(row['payload_json'], {}),
    }


def _data_task_result_summary(result):
    result = _json_safe(result)
    if not isinstance(result, dict):
        return result
    result = dict(result)
    result.pop('tasks', None)
    status = result.get('status')
    if isinstance(status, dict):
        status = dict(status)
        status.pop('tasks', None)
        result['status'] = status
    return result


def compact_data_task(task):
    if not task:
        return None
    keys = (
        'id', 'name', 'mode', 'pullMode', 'status', 'stage', 'message',
        'createdAt', 'startedAt', 'updatedAt', 'finishedAt', 'error',
    )
    compact = {k: _json_safe(task.get(k)) for k in keys if task.get(k) is not None}
    compact['options'] = _json_safe(task.get('options') or {})
    compact['progress'] = _json_safe(task.get('progress') or {})
    if task.get('result') is not None:
        compact['result'] = _data_task_result_summary(task.get('result'))
    compact['events'] = [_json_safe(e) for e in list(task.get('events') or [])[-80:]]
    return compact


def _persist_data_task_records(records):
    with SQLITE_WRITE_LOCK:
        with db(DATA_TASK_PERSIST_BUSY_TIMEOUT_MS) as conn:
            for record in records:
                task = record.get('task')
                event = record.get('event')
                if task:
                    conn.execute(
                        """INSERT OR REPLACE INTO data_tasks
                           (id, name, mode, pull_mode, status, stage, message,
                            options_json, progress_json, result_json, error,
                            created_at, started_at, updated_at, finished_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            task.get('id'),
                            task.get('name'),
                            task.get('mode'),
                            task.get('pullMode'),
                            task.get('status'),
                            task.get('stage'),
                            task.get('message'),
                            json.dumps(_json_safe(task.get('options') or {}), separators=(',', ':')),
                            json.dumps(_json_safe(task.get('progress') or {}), separators=(',', ':')),
                            json.dumps(_data_task_result_summary(task.get('result')), separators=(',', ':')) if task.get('result') is not None else None,
                            task.get('error'),
                            task.get('createdAt'),
                            task.get('startedAt'),
                            task.get('updatedAt'),
                            task.get('finishedAt'),
                        ),
                    )
                if event:
                    conn.execute(
                        """INSERT INTO data_task_events
                           (task_id, ts, stage, message, payload_json)
                           VALUES (?, ?, ?, ?, ?)""",
                        (
                            event.get('taskId'),
                            event.get('ts'),
                            event.get('stage'),
                            event.get('message'),
                            json.dumps(_json_safe(event.get('payload') or {}), separators=(',', ':')),
                        ),
                    )
            conn.execute(
                "DELETE FROM data_task_events WHERE id <= COALESCE((SELECT MAX(id) FROM data_task_events), 0) - 2000"
            )
            conn.commit()


def persist_data_task_update(task=None, event=None):
    global DATA_TASK_PERSIST_NOTICE_AT
    current = None
    if task or event:
        current = {
            'task': compact_data_task(task) if task else None,
            'event': _json_safe(dict(event)) if event else None,
        }
    with DATA_TASK_LOCK:
        records = list(DATA_TASK_PERSIST_BACKLOG)
        if current:
            records.append(current)
    if not records:
        return True
    try:
        _persist_data_task_records(records)
        with DATA_TASK_LOCK:
            DATA_TASK_PERSIST_BACKLOG.clear()
        return True
    except sqlite3.OperationalError as exc:
        if 'locked' in str(exc).lower():
            with DATA_TASK_LOCK:
                if current:
                    DATA_TASK_PERSIST_BACKLOG.append(current)
                backlog_size = len(DATA_TASK_PERSIST_BACKLOG)
            now = time.time()
            if now - DATA_TASK_PERSIST_NOTICE_AT > 30:
                DATA_TASK_PERSIST_NOTICE_AT = now
                print(f"  Data task persistence deferred while database is busy; {backlog_size} event(s) queued")
            return False
        print('  Data task persistence failed:', exc)
        return False
    except sqlite3.Error as exc:
        print('  Data task persistence failed:', exc)
        return False


def data_task_event(task_id, stage, message, **payload):
    return _record_data_task_event(task_id, stage, message, payload)


def _record_data_task_event(task_id, stage, message, payload, defer_persistence=False):
    event = {
        'taskId': task_id,
        'ts': int(time.time()),
        'stage': stage,
        'message': message,
        'payload': _json_safe(payload),
    }
    with DATA_TASK_LOCK:
        DATA_TASK_EVENTS.append(event)
        task = DATA_TASKS.get(task_id)
        if task:
            task['updatedAt'] = event['ts']
            task['stage'] = stage
            task['message'] = message
            task.setdefault('events', []).append(event)
            task['events'] = task['events'][-80:]
            if payload:
                task.setdefault('progress', {}).update(_json_safe(payload))
            task_copy = compact_data_task(task)
        else:
            task_copy = None
        if defer_persistence:
            # Provider transaction progress must never open a second writer.
            # Its next ordinary/post-commit event drains these exact records.
            DATA_TASK_PERSIST_BACKLOG.append({'task': task_copy, 'event': event})
    if not defer_persistence:
        persist_data_task_update(task_copy, event)
    print(f"  data-task[{task_id}] {stage}: {message}")
    return event


def data_task_snapshot():
    with DATA_TASK_LOCK:
        should_flush = bool(DATA_TASK_PERSIST_BACKLOG)
    # A status poll must not queue behind a provider transaction. Keep the
    # idle retry for deferred records; event/terminal updates still persist
    # through their existing writer path when that transaction completes.
    if should_flush and SQLITE_WRITE_LOCK.acquire(blocking=False):
        try:
            persist_data_task_update()
        finally:
            SQLITE_WRITE_LOCK.release()
    with DATA_TASK_LOCK:
        tasks = sorted(DATA_TASKS.values(), key=lambda t: t.get('startedAt') or 0, reverse=True)[:8]
        return {
            'ok': True,
            'activeTaskId': ACTIVE_DATA_TASK_ID,
            'active': compact_data_task(DATA_TASKS.get(ACTIVE_DATA_TASK_ID)) if ACTIVE_DATA_TASK_ID else None,
            'tasks': [compact_data_task(t) for t in tasks],
            'events': [_json_safe(e) for e in list(DATA_TASK_EVENTS)[-80:]],
            'persistBacklog': len(DATA_TASK_PERSIST_BACKLOG),
        }


def latest_data_task_active():
    with DATA_TASK_LOCK:
        active = DATA_TASKS.get(ACTIVE_DATA_TASK_ID) if ACTIVE_DATA_TASK_ID else None
        return bool(active and active.get('status') in ('queued', 'running', 'waiting'))


def restore_data_tasks_from_db(conn):
    global ACTIVE_DATA_TASK_ID, DATA_TASKS, DATA_TASK_EVENTS
    now = int(time.time())
    task_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM data_tasks ORDER BY COALESCE(updated_at, created_at, 0) DESC LIMIT 12"
    )]
    event_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM data_task_events ORDER BY id DESC LIMIT 240"
    )]
    event_rows.reverse()
    events = [_data_task_event_from_row(r) for r in event_rows]
    events_by_task = {}
    for event in events:
        events_by_task.setdefault(event.get('taskId'), []).append(event)

    restored = {}
    resume_candidates = []
    for row in task_rows:
        task = _data_task_from_row(row, events_by_task.get(row.get('id'), [])[-80:])
        status = task.get('status')
        age = now - int(task.get('updatedAt') or task.get('createdAt') or now)
        if status in ('queued', 'running', 'waiting'):
            if age <= DATA_TASK_RESUME_MAX_AGE_SECONDS:
                task['status'] = 'queued'
                task['stage'] = 'queued'
                task['message'] = 'Queued for resume after server restart'
                task['updatedAt'] = now
                resume_candidates.append(task)
                conn.execute(
                    """UPDATE data_tasks
                       SET status='queued', stage='queued', message=?, updated_at=?
                       WHERE id=?""",
                    (task['message'], now, task['id']),
                )
            else:
                task['status'] = 'error'
                task['stage'] = 'interrupted'
                task['message'] = 'Interrupted while server was offline'
                task['finishedAt'] = now
                task['updatedAt'] = now
                conn.execute(
                    """UPDATE data_tasks
                       SET status='error', stage='interrupted', message=?, error=?,
                           updated_at=?, finished_at=?
                       WHERE id=?""",
                    (task['message'], task['message'], now, now, task['id']),
                )
        restored[task['id']] = task

    active_id = None
    if resume_candidates:
        active_id = sorted(resume_candidates, key=lambda t: t.get('updatedAt') or 0, reverse=True)[0]['id']
    with DATA_TASK_LOCK:
        DATA_TASKS = restored
        DATA_TASK_EVENTS = deque(events, maxlen=240)
        ACTIVE_DATA_TASK_ID = active_id


def resume_persisted_data_task():
    global ACTIVE_DATA_TASK_ID
    if not DATA_TASK_RESUME_ENABLED:
        with DATA_TASK_LOCK:
            ACTIVE_DATA_TASK_ID = None
        return False
    active = None
    with DATA_TASK_LOCK:
        if ACTIVE_DATA_TASK_ID:
            active = DATA_TASKS.get(ACTIVE_DATA_TASK_ID)
    if not active or active.get('status') not in ('queued', 'running'):
        return False
    task_id = active.get('id')
    options = active.get('options') or {}
    data_task_event(task_id, 'resume', 'Resuming persisted data task after server restart', **options)
    thread = threading.Thread(
        target=run_data_accumulation_task,
        args=(task_id, options),
        name=f"data-task-{task_id}-resume",
        daemon=True,
    )
    with DATA_TASK_LOCK:
        DATA_TASK_WORKERS[task_id] = thread
    thread.start()
    return True


BULK_STAGING_HOST = None


class CanonicalReadCompatibleRuntime(generation_host.Runtime):
    """Preserve one exact paused adoption across a read/UI-only source update.

    No saved source identity is rewritten. The original builder remains paused;
    starting it under a new source requires explicit normal adoption. Rolling
    back the source restores the exact original saved adoption unchanged.
    """
    compatibility_paused = False

    def restore(self):
        with closing(self.ns['steam_ask_db']()) as conn:
            operator = bulk_derived_staging.get_json(conn, 'bulk_staging_operator:v1')
            abi = bulk_derived_staging.get_setting(conn, bulk_derived_staging.ABI_KEY)
            saved_status = bulk_derived_staging.get_json(conn, 'bulk_staging_runtime_status:v1') or {}
        sha, signature = self.source_identity()
        if not operator or operator.get('sourceSHA256') == sha:
            return super().restore()
        components, signatures = self.component_identity()
        if (operator.get('sourceSHA256') != '5b6ad1a5f140e7d9ed6fe6cb526ded2f0d755ae53c45871713673fda3f2a8788'
                or operator.get('paused') is not True or operator.get('componentSHA256') != components
                or abi != bulk_derived_staging.WRITER_ABI):
            raise bulk_derived_staging.Deferred('Saved paused adoption is not compatible with canonical read update')
        self.native_guard()
        operator = dict(operator, policy=generation_host.legacy_runtime.policy_checked(operator['policy'], self.approved_root))
        # Existing policy/schema/components remain byte identical; only the
        # running read-only consumer source identity changes in memory.
        self.operator = dict(operator, sourceSHA256=sha)
        self._source_signature, self._component_signatures = signature, signatures
        self.paused, self.compatibility_paused = True, True
        self.host = self._build_host()
        self.ns['BULK_STAGING_HOST'] = self.host
        # Builder pause must not reject ordinary saved reads. No pointer has
        # been published in this transition; retain the legacy read factory.
        self.ns['DERIVED_GENERATION_READS_ENABLED'] = False
        self.last.update(saved_status)
        self.uncertain = saved_status.get('uncertainPublishToken')
        self.last.update(state='compatible_paused_source', paused=True)
        return self.snapshot()

    def control(self, data):
        if self.compatibility_paused:
            if isinstance(data, dict) and data == {'action': 'pause'}:
                return self.snapshot()
            raise bulk_derived_staging.Deferred('Explicit requalification/adoption is required before resuming the old builder')
        return super().control(data)


BULK_STAGING_RUNTIME = CanonicalReadCompatibleRuntime(globals())


def bulk_staging_enabled(conn):
    return BULK_STAGING_HOST is not None and bulk_staging_adopted(conn)


def bulk_staging_adopted(conn):
    abi = bulk_derived_staging.get_setting(conn, bulk_derived_staging.ABI_KEY)
    if abi is not None and abi != bulk_derived_staging.WRITER_ABI:
        raise bulk_derived_staging.Deferred('Saved derived ABI requires explicit v3 adoption')
    selected = abi == bulk_derived_staging.WRITER_ABI
    if selected:
        BULK_STAGING_RUNTIME.native_guard()
    return selected


def bulk_staging_cash_revision(conn, lane):
    if bulk_staging_adopted(conn):
        bulk_derived_staging.invalidate_inputs(conn, lane)
        BULK_STAGING_RUNTIME.request()


def bulk_staging_queue_pending(conn):
    flush_market_source_revision(conn)
    return bulk_derived_staging.invalidate_inputs(conn, 'csgotrader_complete_snapshot')


class MarketWriteConnection(sqlite3.Connection):
    """Coalesce source revision invalidation within each write transaction."""
    _market_dirty = False

    def flush_market_revision(self):
        if self._market_dirty:
            super().execute("INSERT OR REPLACE INTO settings(key,value) VALUES('market_source_revision',?)",(str(time.time_ns()),))
            if bulk_staging_adopted(self):
                bulk_derived_staging.invalidate_inputs(self, 'market_source_or_metadata')
                BULK_STAGING_RUNTIME.request()
            self._market_dirty = False

    def commit(self):
        self.flush_market_revision()
        return super().commit()

    def rollback(self):
        self._market_dirty = False
        return super().rollback()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            try:
                self.flush_market_revision()
            except BaseException:
                self.rollback()
                raise
        else:
            self._market_dirty = False
        return super().__exit__(exc_type,exc_value,traceback)


def db(timeout_ms=None):
    timeout_ms = SQLITE_BUSY_TIMEOUT_MS if timeout_ms is None else timeout_ms
    conn = sqlite3.connect(DB_FILE, timeout=timeout_ms / 1000, factory=MarketWriteConnection)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    return conn


def close_idle_wal_anchor():
    """Close only on the startup thread; also safe as an atexit callback."""
    global IDLE_WAL_ANCHOR, IDLE_WAL_ANCHOR_OWNER
    if IDLE_WAL_ANCHOR is None:
        return True
    if threading.get_ident() != IDLE_WAL_ANCHOR_OWNER:
        print('  Idle WAL anchor close deferred to its owning startup thread.')
        return False
    try:
        IDLE_WAL_ANCHOR.close()
    except sqlite3.Error as error:
        print(f'  Idle WAL anchor close failed: {error}')
        return False
    IDLE_WAL_ANCHOR = None
    IDLE_WAL_ANCHOR_OWNER = None
    return True


def start_idle_wal_anchor():
    """Keep one idle read-only pager attached, without holding a read snapshot."""
    global IDLE_WAL_ANCHOR, IDLE_WAL_ANCHOR_OWNER, IDLE_WAL_ANCHOR_ATEXIT_REGISTERED
    if threading.current_thread() is not threading.main_thread():
        print('  Idle WAL anchor unavailable outside the startup thread; normal connections remain in use.')
        return False
    if IDLE_WAL_ANCHOR is not None:
        return True
    option = getattr(sqlite3, 'SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE', None)
    if option is None or not all(hasattr(sqlite3.Connection, name) for name in ('setconfig', 'getconfig')):
        print('  Idle WAL anchor unavailable: NO_CKPT_ON_CLOSE requires supported Python/SQLite; normal startup continues.')
        return False
    connection = None
    try:
        connection = sqlite3.connect(Path(DB_FILE).resolve().as_uri() + '?mode=ro',
                                     uri=True, timeout=0, isolation_level=None)
        connection.setconfig(option, False)
        if connection.getconfig(option):
            raise RuntimeError('Idle anchor last-close cleanup could not be enabled')
        # A constant SELECT 1 does not necessarily attach the database pager.
        # Consume and close this exact-key cursor before leaving startup.
        cursor = connection.execute("SELECT value FROM settings WHERE key='market_source_revision'")
        try:
            cursor.fetchall()
        finally:
            cursor.close()
        if connection.in_transaction:
            raise RuntimeError('idle anchor unexpectedly entered a transaction')
        if not IDLE_WAL_ANCHOR_ATEXIT_REGISTERED:
            atexit.register(close_idle_wal_anchor)
            IDLE_WAL_ANCHOR_ATEXIT_REGISTERED = True
        IDLE_WAL_ANCHOR = connection
        IDLE_WAL_ANCHOR_OWNER = threading.get_ident()
        print('  Idle read-only WAL anchor attached; no read transaction retained.')
        return True
    except (sqlite3.Error, OSError, RuntimeError, ValueError) as error:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        print(f'  Idle WAL anchor unavailable: {error}; normal startup continues.')
        return False


def catalog_read_db():
    """Open the existing database without creating files or repairing data."""
    if DERIVED_GENERATION_READS_ENABLED:
        if DERIVED_GENERATION_REPOSITORY is None:
            raise RuntimeError('Generation reader admission is not configured.')
        return DERIVED_GENERATION_REPOSITORY.reader()
    conn = sqlite3.connect(Path(DB_FILE).resolve().as_uri() + '?mode=ro', uri=True,
                           timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    return conn


def require_catalog_store(conn):
    store = catalog_store.CatalogStore(conn)
    if not store.ready():
        raise RuntimeError('Catalog migration is required before enabling the catalog repository.')
    return store


def catalog_writer(conn):
    if not CATALOG_STORE_ENABLED:
        return None
    if not CATALOG_STORE_WRITES:
        raise RuntimeError('Catalog writes are paused; keep CS2 data refresh paused until writer activation.')
    return require_catalog_store(conn)


def catalog_identity_for_legacy(conn, name, market_hash_name=None, source='legacy', unresolved_definition=None):
    store = catalog_writer(conn)
    if store is None:
        return None, None
    exact_name = market_hash_name or name
    meta = catalog_meta_for_name(conn, exact_name) or infer_market_meta_from_name(exact_name) or {}
    if not catalog_row_is_market_listing(meta):
        if unresolved_definition is not None:
            unresolved_definition.update(status='unresolved_definition',
                reason=(meta.get('_marketProjection') or {}).get('reason') or 'metadata_entity',
                metadataKind=meta.get('kind'), metadataName=meta.get('name'))
            return store, None
        raise catalog_store.IdentityConflict('Published definition has no verified exact market listing: ' + exact_name)
    item_id = store.ensure_item({
        'name': exact_name, 'market_hash_name': exact_name,
        'entity_type': 'listing', 'kind': meta.get('kind') or 'market_listing',
        'metadata': {
            'kind': meta.get('kind') or 'market_listing',
            'type': meta.get('item_type'), 'weapon': meta.get('weapon'),
            'collection': meta.get('collection'), 'rarity': meta.get('rarity'),
            'rarityColor': meta.get('rarity_color'), 'image': meta.get('image'),
        },
    }, source=source)
    return store, item_id


def catalog_sync_legacy_metadata(conn, table, names=None, records=None, reconcile_raw=True):
    """Mirror final metadata only; current quote values remain in native rows.

    Call from the caller's existing writer transaction after legacy changes.
    Stable exact identifiers connect feed-published entities to their legacy
    metadata projection. No speculative price bindings or raw copies are made.
    """
    if not CATALOG_STORE_ENABLED:
        return 0
    if table not in ('item_catalog', 'market_items'):
        raise ValueError('only current public metadata projections are supported')
    store = catalog_writer(conn)
    fields = ('name', 'kind', 'item_type', 'weapon', 'collection', 'rarity',
              'rarity_color', 'image', 'market_hash_name')
    if records is None:
        if names is None:
            raise ValueError('explicit affected metadata names required')
        names = list(dict.fromkeys(names))
        records = []
        for offset in range(0, len(names), 200):
            chunk = names[offset:offset + 200]
            records.extend(dict(row) for row in conn.execute(
                'SELECT ' + ','.join(fields) + (',raw' if reconcile_raw else '') + ',updated_at FROM ' + table +
                ' WHERE name IN (' + ','.join('?' for _ in chunk) + ')', chunk))
    source = 'legacy:' + table
    changed = 0
    for value in records:
        row = dict(value)
        record = {field: row.get(field) for field in fields}
        name = record['name']
        if not isinstance(name, str) or not name:
            raise ValueError('nonempty exact metadata name required')
        kind = record.get('kind')
        entity_type = {'collections': 'collection', 'skin_families': 'skin_family'}.get(kind, 'listing')
        namespace = source + ':' + str(kind or '')
        record['identifiers'] = [(namespace, name)]
        record['raw_ref'] = {'store': table, 'name': name}
        record_source = store._legacy_record_source(source, name)
        owners = conn.execute('''SELECT i.item_id,i.entity_type,m.metadata_json,m.raw_ref_json,m.active
            FROM catalog_identifiers_v2 x JOIN catalog_items_v2 i ON i.item_id=x.item_id
            LEFT JOIN catalog_metadata_v2 m ON m.item_id=i.item_id AND m.source IN (?,?)
            WHERE x.namespace=? AND x.value=?
            ORDER BY CASE WHEN m.source=? THEN 0 ELSE 1 END LIMIT 2''',
            (record_source, source, namespace, name, record_source)).fetchall()
        owner = owners[0] if owners else None
        if owner and owner['entity_type'] != entity_type:
            raise catalog_store.IdentityConflict('current metadata entity type changed')
        evidence = None
        for candidate in owners:
            reference = json.loads(candidate['raw_ref_json'] or '{}')
            if isinstance(reference, dict) and reference.get('store') == table and reference.get('name') == name:
                evidence = candidate
                break
        previous_metadata = json.loads(evidence['metadata_json'] or '{}') if evidence else {}
        if not reconcile_raw and evidence:
            # A quote-only ensure does not modify upstream raw evidence. Avoid
            # reading/parsing it again only when evidence belongs to this exact
            # physical name. Another alias's canonical snapshot is insufficient.
            if 'external_id' in previous_metadata:
                record['external_id'] = previous_metadata['external_id']
        else:
            if 'raw' not in row:
                saved_raw = conn.execute('SELECT raw FROM ' + table + ' WHERE name=?', (name,)).fetchone()
                row['raw'] = saved_raw['raw'] if saved_raw else None
            raw = row.get('raw')
            try:
                raw = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            except (TypeError, ValueError):
                raw = None
            if isinstance(raw, dict) and raw.get('id') is not None:
                record['external_id'] = raw['id']
        desired = {field: record[field] for field in fields}
        desired.update(type=record.get('item_type'), rarityColor=record.get('rarity_color'))
        if 'external_id' in record:
            desired['external_id'] = record['external_id']
        if owner:
            item_id = owner['item_id']
            same = bool(evidence and evidence['active'] and previous_metadata == desired
                        and json.loads(evidence['raw_ref_json'] or '{}') == record['raw_ref'])
        else:
            # Locate an existing strong/feed identity before adding a legacy
            # identifier, including nonpriceable families without Steam hashes.
            keys = store._identifiers(record, source, entity_type)
            if table == 'item_catalog':
                feed_record = {key: val for key, val in record.items() if key != 'identifiers'}
                keys += store._identifiers(feed_record, 'catalog:' + str(kind or ''), entity_type)
            owners = {found['item_id'] for key, val in keys for found in conn.execute(
                'SELECT item_id FROM catalog_identifiers_v2 WHERE namespace=? AND value=?', (key, val))}
            if len(owners) > 1:
                raise catalog_store.IdentityConflict('current metadata identifiers have conflicting owners')
            item_id = next(iter(owners)) if owners else None
            same = False
        changed_item = not same
        if changed_item:
            if item_id is not None:
                record['item_id'] = item_id
            item_id = store.ensure_item(record, source=source,
                                        attempted_at=row.get('updated_at') or int(time.time()))
        if table == 'item_catalog':
            # The upstream raw reference and source success receipt remain
            # intact. Only the final normalized fields (e.g. collection backfill)
            # override the feed's earlier normalization in the compatibility DTO.
            feed = conn.execute('''SELECT metadata_json FROM catalog_metadata_v2
                WHERE item_id=? AND source=? AND active=1''', (item_id, 'catalog:' + str(kind or ''))).fetchone()
            if feed:
                metadata = json.loads(feed['metadata_json'])
                final = {**metadata, **{field: desired[field] for field in fields},
                         'type': desired['type'], 'rarityColor': desired['rarityColor']}
                if final != metadata:
                    final['normalizationSource'] = source
                    conn.execute('''UPDATE catalog_metadata_v2 SET metadata_json=?,updated_at=?
                        WHERE item_id=? AND source=?''',
                        (json.dumps(final, ensure_ascii=False, sort_keys=True, separators=(',', ':')),
                         row.get('updated_at') or int(time.time()), item_id, 'catalog:' + str(kind or '')))
                    if not changed_item:
                        store._bump_generation()
                    changed_item = True
        if changed_item:
            changed += 1
    return changed


def catalog_read_payload(path, query):
    if not CATALOG_STORE_ENABLED:
        return {'ok': False, 'error': 'Catalog repository is disabled; migration and explicit activation are required.'}, 503
    if path == '/api/catalog' and not CATALOG_COMPATIBILITY_FEED_ENABLED:
        return {'ok': False, 'error': 'The monolithic native compatibility feed is disabled; use cursor-paged /api/items with include_quotes=false.'}, 503
    try:
        with closing(catalog_read_db()) as conn:
            conn.execute('BEGIN')
            # Exact physical history is addressable before registry seeding.
            # These reads never mint an ID or infer a Steam listing identity.
            if path in ('/api/items/legacy-streams', '/api/items/legacy-history'):
                store = catalog_store.CatalogStore(conn)
                legacy_store = query.get('store', [''])[0]
                limit = parse_int_param(query.get('limit', ['100'])[0],100,minimum=1,maximum=500)
                cursor = query.get('cursor',[None])[0]
                if path == '/api/items/legacy-streams':
                    return {'ok':True, **store.discover_legacy_streams_batch(
                        legacy_store,cursor=cursor,limit=limit,max_queries=400,seconds=1)},200
                name = query.get('name',[''])[0]
                if not name:
                    return {'ok':False,'error':'Missing exact name'},400
                return {'ok':True, **store.lookup_legacy(
                    legacy_store,name,provider=query.get('provider',[None])[0],
                    lane=query.get('lane',[None])[0],source=query.get('source',[None])[0],
                    limit=limit,cursor=cursor,max_queries=400,seconds=1)},200
            store = require_catalog_store(conn)
            if path == '/api/catalog':
                return catalog_payload_from_store(store), 200
            if path == '/api/items':
                priced_value = query.get('priced', [None])[0]
                if priced_value is None:
                    priced = None
                elif str(priced_value).lower() in ('1', 'true', 'yes'):
                    priced = True
                elif str(priced_value).lower() in ('0', 'false', 'no'):
                    priced = False
                else:
                    raise ValueError('priced must be true or false')
                include_quotes_value = str(query.get('include_quotes', ['true'])[0]).lower()
                if include_quotes_value in ('1', 'true', 'yes'):
                    include_quotes = True
                elif include_quotes_value in ('0', 'false', 'no'):
                    include_quotes = False
                else:
                    raise ValueError('include_quotes must be true or false')
                return {'ok': True, **store.list_items(
                    query=query.get('query', query.get('search', ['']))[0],
                    limit=parse_int_param(query.get('limit', ['100'])[0], 100, minimum=1, maximum=500),
                    cursor=query.get('cursor', [None])[0],
                    entity_type=query.get('entity_type', [None])[0],
                    priced=priced,
                    query_mode=query.get('query_mode', ['contains'])[0],
                    include_quotes=include_quotes,
                ), 'coverage': store.status()}, 200
            if path == '/api/items/resolve':
                value = query.get('value', query.get('name', ['']))[0]
                if not value:
                    return {'ok': False, 'error': 'Missing value'}, 400
                return {'ok': True, **store.resolve(value, namespace=query.get('namespace', [None])[0])}, 200
            if path == '/api/items/payload':
                payload_id = query.get('payload_id', [''])[0]
                if not payload_id:
                    return {'ok': False, 'error': 'Missing payload_id'}, 400
                payload = store.get_payload(payload_id)
                if payload is None:
                    return {'ok': False, 'error': 'Payload not found'}, 404
                return {'ok': True, 'payload': payload}, 200
            item_id = query.get('id', [''])[0]
            if not item_id:
                return {'ok': False, 'error': 'Missing id'}, 400
            item = store.get_item(item_id)
            if item is None:
                return {'ok': False, 'error': 'Item not found'}, 404
            if path == '/api/items/detail':
                return {'ok': True, 'item': item}, 200
            if path == '/api/items/current':
                return {'ok': True, 'itemId': item_id, **store.current_quotes(item_id)}, 200
            if path == '/api/items/evidence':
                return {'ok': True, 'itemId': item_id,
                        'evidence': store.get_evidence(item_id, source=query.get('source', [None])[0])}, 200
            if path == '/api/items/revisions':
                point_key = query.get('point_key', [''])[0]
                if not point_key:
                    return {'ok': False, 'error': 'Missing point_key'}, 400
                return {'ok': True, **store.get_revisions(
                    item_id, point_key, provider=query.get('provider', ['steam'])[0],
                    limit=parse_int_param(query.get('limit', ['100'])[0], 100, minimum=1, maximum=500),
                    cursor=query.get('cursor', [None])[0],
                )}, 200
            if path == '/api/items/history':
                return {'ok': True, 'itemId': item_id, **store.history(
                    item_id, kind=query.get('kind', ['auto'])[0],
                    limit=parse_int_param(query.get('limit', ['200'])[0], 200, minimum=1, maximum=1000),
                    cursor=query.get('cursor', [None])[0],
                    provider=query.get('provider', [None])[0],
                    lane=query.get('lane', [None])[0], source=query.get('source', [None])[0],
                )}, 200
    except (RuntimeError, sqlite3.OperationalError):
        return {'ok': False, 'error': 'Catalog repository is unavailable; complete the explicit catalog migration before activation.'}, 503
    except (ValueError, KeyError, TypeError) as exc:
        return {'ok': False, 'error': str(exc)}, 400
    return {'ok': False, 'error': 'Unknown item route'}, 404


def add_missing_columns(conn, table, specs):
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, sql_type in specs.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")


def init_db():
    with db() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id TEXT PRIMARY KEY,
            sort_key INTEGER NOT NULL,
            label TEXT NOT NULL,
            date TEXT,
            imported_at TEXT,
            data TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS auth_sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS data_tasks (
            id TEXT PRIMARY KEY,
            name TEXT,
            mode TEXT,
            pull_mode TEXT,
            status TEXT,
            stage TEXT,
            message TEXT,
            options_json TEXT,
            progress_json TEXT,
            result_json TEXT,
            error TEXT,
            created_at INTEGER,
            started_at INTEGER,
            updated_at INTEGER,
            finished_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS data_task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            ts INTEGER NOT NULL,
            stage TEXT,
            message TEXT,
            payload_json TEXT
        );
        CREATE TABLE IF NOT EXISTS portfolios (
            id TEXT PRIMARY KEY,
            sort_key INTEGER NOT NULL,
            label TEXT NOT NULL,
            imported_at TEXT,
            data TEXT
        );
        CREATE TABLE IF NOT EXISTS portfolio_items (
            portfolio_id TEXT NOT NULL,
            sort_key INTEGER NOT NULL,
            name TEXT NOT NULL,
            base_name TEXT NOT NULL,
            qty REAL,
            import_unit_price REAL,
            import_prior_unit_price REAL,
            import_curr REAL,
            import_past REAL,
            category TEXT,
            group_name TEXT
        );
        CREATE TABLE IF NOT EXISTS item_catalog (
            name TEXT PRIMARY KEY,
            kind TEXT,
            item_type TEXT,
            weapon TEXT,
            collection TEXT,
            rarity TEXT,
            rarity_color TEXT,
            image TEXT,
            market_hash_name TEXT,
            raw TEXT,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS market_items (
            name TEXT PRIMARY KEY,
            market_hash_name TEXT,
            kind TEXT,
            item_type TEXT,
            weapon TEXT,
            collection TEXT,
            rarity TEXT,
            rarity_color TEXT,
            image TEXT,
            steam_price REAL,
            steam_lowest_sell_order REAL,
            steam_highest_buy_order REAL,
            steam_spread_pct REAL,
            steam_stability_pct REAL,
            steam_price_basis TEXT,
            steam_price_updated_at INTEGER,
            steam_snapshot_price REAL,
            steam_history_price REAL,
            csfloat_price REAL,
            buff_price REAL,
            skinport_price REAL,
            youpin_price REAL,
            primary_price REAL,
            primary_provider TEXT,
            primary_price_basis TEXT,
            primary_confidence_pct REAL,
            primary_price_updated_at INTEGER,
            provider_prices_json TEXT,
            raw TEXT,
            first_seen_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS market_item_prices (
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            price REAL,
            median REAL,
            solid_price REAL,
            lowest_sell_order REAL,
            highest_buy_order REAL,
            spread_pct REAL,
            stability_pct REAL,
            confidence_pct REAL,
            volume INTEGER,
            source TEXT,
            basis TEXT,
            kind TEXT,
            url TEXT,
            raw TEXT,
            observed_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, provider)
        );
        CREATE TABLE IF NOT EXISTS price_quotes (
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            price REAL,
            median REAL,
            volume INTEGER,
            url TEXT,
            raw TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, provider)
        );
        CREATE TABLE IF NOT EXISTS price_history (
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            point_time TEXT NOT NULL,
            point_ts INTEGER,
            raw_time TEXT,
            price REAL,
            volume INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, provider, point_time)
        );
        CREATE TABLE IF NOT EXISTS steam_history_failures (
            name TEXT PRIMARY KEY,
            market_hash_name TEXT,
            error TEXT,
            failed_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS price_observations (
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            observed_at INTEGER NOT NULL,
            price REAL,
            median REAL,
            volume INTEGER,
            source TEXT,
            raw TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, provider, observed_at)
        );
        CREATE TABLE IF NOT EXISTS shared_item_prices (
            name TEXT NOT NULL,
            market_hash_name TEXT,
            lane TEXT NOT NULL,
            price REAL,
            confidence INTEGER,
            confidence_tier TEXT,
            source_count INTEGER,
            history_source TEXT,
            history_points INTEGER,
            method TEXT,
            raw TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, lane)
        );
        CREATE TABLE IF NOT EXISTS shared_price_observations (
            name TEXT NOT NULL,
            lane TEXT NOT NULL,
            observed_at INTEGER NOT NULL,
            price REAL,
            confidence INTEGER,
            source_count INTEGER,
            raw TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, lane, observed_at)
        );
        CREATE TABLE IF NOT EXISTS shared_price_history (
            name TEXT NOT NULL,
            lane TEXT NOT NULL,
            source TEXT NOT NULL,
            point_ts INTEGER NOT NULL,
            point_time TEXT,
            price REAL,
            volume INTEGER,
            raw TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, lane, source, point_ts)
        );
        CREATE TABLE IF NOT EXISTS shared_source_quotes (
            name TEXT NOT NULL,
            source TEXT NOT NULL,
            market_class TEXT,
            price_kind TEXT,
            price REAL,
            confidence INTEGER,
            source_url TEXT,
            raw TEXT,
            observed_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (name, source)
        );
        CREATE TABLE IF NOT EXISTS item_market_metrics (
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            points INTEGER,
            clean_points INTEGER,
            edge_case_count INTEGER,
            volatility_pct REAL,
            range_pct REAL,
            trend_pct REAL,
            first_price REAL,
            latest_price REAL,
            first_ts INTEGER,
            last_ts INTEGER,
            current_price REAL,
            prior_price REAL,
            price_source TEXT,
            updated_at INTEGER NOT NULL,
            raw TEXT,
            PRIMARY KEY (name, provider, window_days)
        );
        CREATE TABLE IF NOT EXISTS market_item_lookup (
            window_days INTEGER NOT NULL,
            name TEXT NOT NULL,
            market_hash_name TEXT,
            kind TEXT,
            item_type TEXT,
            weapon TEXT,
            collection TEXT,
            rarity TEXT,
            latest_price REAL,
            prior_price REAL,
            change_pct REAL,
            change_dollar REAL,
            latest_at INTEGER,
            source TEXT,
            volatility_pct REAL,
            history_points INTEGER,
            edge_case_count INTEGER,
            groups_json TEXT,
            search_text TEXT,
            metric_updated_at INTEGER,
            PRIMARY KEY (window_days, name)
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS market_item_lookup_fts
            USING fts5(search_text, content='market_item_lookup', content_rowid='rowid');
        -- price_history indexes are created after lightweight migrations below.
        CREATE VIEW IF NOT EXISTS inventory_dataset AS
            SELECT i.*, s.imported_at
            FROM snapshot_items i
            LEFT JOIN snapshots s ON s.id = i.snapshot_id;
        CREATE TABLE IF NOT EXISTS snapshot_items (
            snapshot_id TEXT NOT NULL,
            sort_key INTEGER NOT NULL,
            label TEXT NOT NULL,
            snapshot_date TEXT,
            horizon_days INTEGER,
            name TEXT NOT NULL,
            base_name TEXT NOT NULL,
            qty REAL,
            pct REAL,
            dollar REAL,
            curr REAL,
            past REAL,
            unit_curr REAL,
            unit_past REAL,
            category TEXT,
            group_name TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_snapshot_items_snap ON snapshot_items(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_snapshot_items_item ON snapshot_items(base_name, horizon_days, snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_snapshot_items_window ON snapshot_items(snapshot_date, horizon_days, sort_key);
        CREATE INDEX IF NOT EXISTS idx_portfolio_items_portfolio ON portfolio_items(portfolio_id);
        CREATE INDEX IF NOT EXISTS idx_portfolio_items_item ON portfolio_items(base_name, name);
        """)

        # Lightweight migrations for older local databases.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(price_history)").fetchall()}
        if 'point_ts' not in cols:
            conn.execute("ALTER TABLE price_history ADD COLUMN point_ts INTEGER")
        if 'raw_time' not in cols:
            conn.execute("ALTER TABLE price_history ADD COLUMN raw_time TEXT")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(steam_history_failures)").fetchall()}
        if 'fetch_version' not in cols:
            conn.execute("ALTER TABLE steam_history_failures ADD COLUMN fetch_version INTEGER")
        add_missing_columns(conn, 'market_items', {
            'steam_snapshot_price': 'REAL',
            'steam_history_price': 'REAL',
            'provider_prices_json': 'TEXT',
            'primary_confidence_pct': 'REAL',
        })
        add_missing_columns(conn, 'market_item_lookup', {
            'steam_price': 'REAL',
            'steam_lowest_sell_order': 'REAL',
            'steam_highest_buy_order': 'REAL',
            'steam_spread_pct': 'REAL',
            'steam_stability_pct': 'REAL',
            'price_basis': 'TEXT',
            'confidence_pct': 'REAL',
            'provider_prices_json': 'TEXT',
        })

        # Create indexes after migrations so older databases that do not yet
        # have point_ts/raw_time do not fail during startup.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_name ON price_history(name, provider, point_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_ts ON price_history(name, provider, point_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_steam_history_failures_failed_at ON steam_history_failures(failed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_price_observations_name_ts ON price_observations(name, provider, observed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_price_observations_anchor ON price_observations(name, provider, source, observed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_price_observations_ts ON price_observations(provider, observed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_item_prices_lane ON shared_item_prices(lane, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_price_observations_name_lane ON shared_price_observations(name, lane, observed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_price_history_name_lane ON shared_price_history(name, lane, source, point_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_source_quotes_name ON shared_source_quotes(name, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_market_metrics_name ON item_market_metrics(name, provider, window_days)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_market_metrics_window ON item_market_metrics(provider, window_days, name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_market_hash_name ON item_catalog(market_hash_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_items_hash ON market_items(market_hash_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_items_primary_price ON market_items(primary_price)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_prices_provider ON market_item_prices(provider, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_prices_provider_name ON market_item_prices(provider, name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_change ON market_item_lookup(window_days, change_pct)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_price ON market_item_lookup(window_days, latest_price)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_name ON market_item_lookup(window_days, name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_item_lookup_filters ON market_item_lookup(window_days, collection, item_type, rarity)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data_tasks_status_updated ON data_tasks(status, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data_task_events_task_id ON data_task_events(task_id, id)")

        if not conn.execute("SELECT 1 FROM snapshots LIMIT 1").fetchone():
            for idx, snap in enumerate(read_json(SNAPS_FILE, [])):
                put_snapshot(conn, snap, idx)

        if not conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone():
            settings = read_json(SETTINGS_FILE, {})
            conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('shared',?)",
                (json.dumps(settings),),
            )
        if not conn.execute("SELECT 1 FROM snapshot_items LIMIT 1").fetchone():
            rebuild_item_index(conn)
        if not conn.execute("SELECT 1 FROM portfolios LIMIT 1").fetchone():
            seed_portfolios(conn)
        restore_data_tasks_from_db(conn)
        if not conn.execute("SELECT 1 FROM price_observations LIMIT 1").fetchone():
            backfill_price_observations(conn)
        if not conn.execute("SELECT 1 FROM market_items LIMIT 1").fetchone():
            sync_market_items_from_catalog(conn)
        if not conn.execute("SELECT 1 FROM market_item_prices LIMIT 1").fetchone():
            backfill_market_item_prices(conn)
        # Metadata cleanup is an explicit ingestion task, not startup work.


def put_snapshot(conn, snap, sort_key):
    conn.execute(
        """INSERT OR REPLACE INTO snapshots
           (id, sort_key, label, date, imported_at, data)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            snap.get('id'),
            sort_key,
            snap.get('label') or snap.get('id') or 'Snapshot',
            snap.get('date'),
            snap.get('importedAt') or snap.get('imported_at'),
            json.dumps(snap.get('data', {}), separators=(',', ':')),
        ),
    )


def base_item_name(name):
    return re.sub(
        r'\s*\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred|FN|MW|FT|WW|BS)\)\s*$',
        '',
        re.sub(r'^(StatTrak(?:TM)?|Souvenir)\s+', '', name or '').strip(),
    )


def parse_snapshot_label(label):
    label = (label or '').strip()
    m = re.search(r'\((\d+)\s*d\)\s*$', label, re.I)
    if m:
        return re.sub(r'\s*\(\d+\s*d\)\s*$', '', label, flags=re.I).strip(), int(m.group(1))
    m = re.search(r'\s[-]\s*(\d+)\s*$', label)
    if m:
        return label[:m.start()].strip(), int(m.group(1))
    return label, None


def index_snapshot_items(conn, snap, sort_key):
    data = snap.get('data') or {}
    items = data.get('items') or []
    if not isinstance(items, list):
        return
    _, horizon = parse_snapshot_label(snap.get('label'))
    snap_date = snap.get('date') or (snap.get('importedAt') or '')[:10] or None
    rows = []
    for item in items:
        if not isinstance(item, list) or len(item) < 8:
            continue
        qty = float(item[ITEM_IDX['q']] or 0)
        rows.append((
            snap.get('id'), sort_key, snap.get('label') or snap.get('id') or 'Snapshot',
            snap_date, horizon, item[ITEM_IDX['n']], base_item_name(item[ITEM_IDX['n']]),
            qty, None, None,
            None, None, None, None,
            item[ITEM_IDX['cat']], item[ITEM_IDX['grp']],
        ))
    conn.executemany(
        """INSERT INTO snapshot_items
           (snapshot_id, sort_key, label, snapshot_date, horizon_days, name, base_name,
            qty, pct, dollar, curr, past, unit_curr, unit_past, category, group_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )


def rebuild_item_index(conn):
    conn.execute("DELETE FROM snapshot_items")
    rows = conn.execute("SELECT * FROM snapshots ORDER BY sort_key, imported_at, id").fetchall()
    for idx, row in enumerate(rows):
        index_snapshot_items(conn, {
            'id': row['id'],
            'label': row['label'],
            'date': row['date'],
            'importedAt': row['imported_at'],
            'data': json.loads(row['data']),
        }, row['sort_key'])


def get_snapshots():
    with db() as conn:
        rows = conn.execute("SELECT * FROM snapshots ORDER BY sort_key, imported_at, id").fetchall()
    return [
        {
            'id': r['id'],
            'label': r['label'],
            'date': r['date'],
            'importedAt': r['imported_at'],
            'data': json.loads(r['data']),
        }
        for r in rows
    ]


def replace_snapshots(snaps):
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute("DELETE FROM snapshots")
            conn.execute("DELETE FROM snapshot_items")
            for idx, snap in enumerate(snaps if isinstance(snaps, list) else [snaps]):
                put_snapshot(conn, snap, idx)
                index_snapshot_items(conn, snap, idx)
        write_json(SNAPS_FILE, snaps)


def portfolio_label_from_snapshot(label):
    clean, _ = parse_snapshot_label(label)
    return clean or label or 'Portfolio'


def portfolio_items_from_data(data):
    rows = []
    for idx, item in enumerate((data or {}).get('items') or []):
        if not isinstance(item, list) or len(item) < 8:
            continue
        try:
            qty = float(item[ITEM_IDX['q']] or 0)
        except (TypeError, ValueError):
            continue
        rows.append({
            'sort_key': idx,
            'name': item[ITEM_IDX['n']],
            'base_name': base_item_name(item[ITEM_IDX['n']]),
            'qty': qty,
            'import_unit_price': None,
            'import_prior_unit_price': None,
            'import_curr': None,
            'import_past': None,
            'category': item[ITEM_IDX['cat']],
            'group_name': item[ITEM_IDX['grp']],
        })
    return normalize_portfolio_item_rows(rows)


def normalize_portfolio_item_rows(rows):
    """Collapse duplicate market-item rows caused by doubled import payloads."""
    normalized = []
    seen_exact = set()
    by_name = {}
    for row in rows or []:
        name = ' '.join(str(row.get('name') or '').split())
        if not name:
            continue
        try:
            qty = float(row.get('qty') or 0)
        except (TypeError, ValueError):
            continue
        if qty <= 0:
            continue

        category = row.get('category')
        group_name = row.get('group_name')
        exact_key = (name.casefold(), qty, str(category or '').casefold(), str(group_name or '').casefold())
        if exact_key in seen_exact:
            continue
        seen_exact.add(exact_key)

        name_key = name.casefold()
        if name_key in by_name:
            by_name[name_key]['qty'] += qty
            continue

        clean = dict(row)
        clean['name'] = name
        clean['base_name'] = clean.get('base_name') or base_item_name(name)
        clean['qty'] = qty
        clean['sort_key'] = len(normalized)
        normalized.append(clean)
        by_name[name_key] = clean

    for idx, row in enumerate(normalized):
        row['sort_key'] = idx
    return normalized


def portfolio_data_from_rows(rows):
    r2 = lambda n: round(float(n or 0), 2)
    items = []
    for r in normalize_portfolio_item_rows(rows):
        qty = float(r.get('qty') or 0)
        items.append([
            r.get('name'), qty, None, None, None, None,
            r.get('category'), r.get('group_name'),
        ])

    cat_map = {}
    grp_map = {}
    for i in items:
        cat = i[ITEM_IDX['cat']]
        grp = i[ITEM_IDX['grp']]
        if cat not in cat_map:
            cat_map[cat] = {'name': cat, 'count': 0, 'past': 0, 'curr': 0, 'dollar': 0}
        if grp not in grp_map:
            grp_map[grp] = {'name': grp, 'count': 0, 'past': 0, 'curr': 0, 'dollar': 0}
        for bucket in (cat_map[cat], grp_map[grp]):
            bucket['count'] += 1
            bucket['past'] += 0
            bucket['curr'] += 0
            bucket['dollar'] += 0

    def finish(bucket):
        past = bucket['past']
        curr = bucket['curr']
        return {
            **bucket,
            'past': r2(past),
            'curr': r2(curr),
            'dollar': r2(bucket['dollar']),
            'pct': r2(((curr / past) - 1) * 100) if past > 0 else 0,
        }

    items.sort(key=lambda i: (i[ITEM_IDX['q']] or 0), reverse=True)
    by_dollar = []
    total_curr = None
    total_past = None
    total_dollar = None
    return {
        'totals': {'curr': total_curr, 'past': total_past, 'dollar': total_dollar, 'count': len(items)},
        'categories': sorted([finish(v) for v in cat_map.values()], key=lambda x: x['curr'], reverse=True),
        'rollup': sorted([finish(v) for v in grp_map.values()], key=lambda x: x['curr'], reverse=True),
        'topGainers': [
            {'n': i[ITEM_IDX['n']], 'pct': i[ITEM_IDX['pct']], 'd': i[ITEM_IDX['d']], 'ct': i[ITEM_IDX['ct']], 'cat': i[ITEM_IDX['cat']]}
            for i in by_dollar[:20]
        ],
        'topLosers': [],
        'items': items,
    }


def put_portfolio(conn, portfolio, sort_key):
    if not portfolio.get('id'):
        portfolio['id'] = 'portfolio_' + str(int(time.time() * 1000)) + '_' + str(sort_key)
    source_data = portfolio.get('data') or {}
    rows = portfolio_items_from_data(source_data)
    data = portfolio_data_from_rows(rows)
    if isinstance(source_data, dict) and source_data.get('importStats'):
        data['importStats'] = source_data.get('importStats')
    portfolio['data'] = data
    conn.execute(
        """INSERT OR REPLACE INTO portfolios
           (id, sort_key, label, imported_at, data)
           VALUES (?, ?, ?, ?, ?)""",
        (
            portfolio.get('id'),
            sort_key,
            portfolio.get('label') or portfolio.get('id') or 'Portfolio',
            portfolio.get('importedAt') or portfolio.get('imported_at'),
            json.dumps(data, separators=(',', ':')),
        ),
    )
    conn.execute("DELETE FROM portfolio_items WHERE portfolio_id=?", (portfolio.get('id'),))
    conn.executemany(
        """INSERT INTO portfolio_items
           (portfolio_id, sort_key, name, base_name, qty, import_unit_price,
            import_prior_unit_price, import_curr, import_past, category, group_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                portfolio.get('id'), r['sort_key'], r['name'], r['base_name'], r['qty'],
                r['import_unit_price'], r['import_prior_unit_price'], r['import_curr'],
                r['import_past'], r['category'], r['group_name'],
            )
            for r in rows
        ],
    )


def seed_portfolios(conn):
    portfolios = read_json(PORTFOLIOS_FILE, [])
    if not portfolios:
        row = conn.execute("SELECT * FROM snapshots ORDER BY sort_key DESC, imported_at DESC LIMIT 1").fetchone()
        if row:
            portfolios = [{
                'id': 'portfolio_' + (row['id'] or str(int(time.time()))),
                'label': portfolio_label_from_snapshot(row['label']),
                'importedAt': row['imported_at'],
                'data': json.loads(row['data']),
            }]
    for idx, portfolio in enumerate(portfolios if isinstance(portfolios, list) else [portfolios]):
        put_portfolio(conn, portfolio, idx)
    if portfolios:
        write_json(PORTFOLIOS_FILE, portfolios)


def get_portfolios():
    with db() as conn:
        rows = conn.execute("SELECT * FROM portfolios ORDER BY sort_key, imported_at, id").fetchall()
        out = []
        for r in rows:
            item_rows = [dict(x) for x in conn.execute(
                "SELECT * FROM portfolio_items WHERE portfolio_id=? ORDER BY sort_key",
                (r['id'],),
            )]
            data = portfolio_data_from_rows(item_rows)
            out.append({
                'id': r['id'],
                'label': r['label'],
                'importedAt': r['imported_at'],
                'data': data,
            })
    return out


def replace_portfolios(portfolios):
    portfolios = portfolios if isinstance(portfolios, list) else [portfolios]
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute("DELETE FROM portfolios")
            conn.execute("DELETE FROM portfolio_items")
            for idx, portfolio in enumerate(portfolios):
                put_portfolio(conn, portfolio, idx)
        write_json(PORTFOLIOS_FILE, portfolios)


def backfill_price_observations(conn):
    rows = conn.execute(
        "SELECT name, provider, price, median, volume, raw, updated_at FROM price_quotes WHERE provider='steam' AND (price IS NOT NULL OR median IS NOT NULL)"
    ).fetchall()
    for idx, row in enumerate(rows):
        quote = dict(row)
        source = quote_source_from_raw(quote.get('raw')) or 'steam_quote'
        provider_key = provider_key_from_source(source=source, provider=quote.get('provider'), quote=quote)
        save_price_observation(conn, quote, source=source, observed_at=quote.get('updated_at'), provider=provider_key)
        upsert_market_item_price(conn, quote, provider=provider_key, source=source)


def backfill_market_item_prices(conn):
    """Seed separated latest provider rows from the older observation cache."""
    latest = {}
    rows = conn.execute(
        """SELECT name, provider, observed_at, price, median, volume, source, raw, updated_at
           FROM price_observations
           WHERE price IS NOT NULL
           ORDER BY observed_at"""
    )
    for r in rows:
        quote = dict(r)
        source = quote.get('source') or quote.get('provider')
        provider_key = provider_key_from_source(source=source, provider=quote.get('provider'), quote=quote)
        key = (quote.get('name'), provider_key)
        if key[0] and (key not in latest or int(quote.get('observed_at') or 0) >= int(latest[key].get('observed_at') or 0)):
            latest[key] = quote
    stored = 0
    for (_name, provider_key), quote in latest.items():
        source = quote.get('source') or provider_key
        if upsert_market_item_price(conn, quote, provider=provider_key, source=source):
            stored += 1
    return stored


def get_settings():
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key='shared'").fetchone()
    return json.loads(row['value']) if row else {}


def set_settings(data):
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings(key,value) VALUES('shared',?)",
            (json.dumps(data, separators=(',', ':')),),
        )
    write_json(SETTINGS_FILE, data)


def first_name(obj, *paths):
    for path in paths:
        cur = obj
        for part in path:
            if isinstance(cur, list):
                cur = cur[0] if cur else None
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(part)
        if isinstance(cur, list):
            cur = cur[0] if cur else None
        if isinstance(cur, dict):
            cur = cur.get('name')
        if cur:
            return cur
    return None


def fetch_json(url):
    req = urllib.request.Request(url, headers={
        'Accept-Encoding': 'gzip',
        'User-Agent': f'{APP_NAME}/{APP_VERSION}',
    })
    return fetch_json_request(req, timeout=20)


def fetch_json_request(req, timeout=60, metadata=None):
    is_steam = urllib.parse.urlsplit(req.full_url).hostname == 'steamcommunity.com'
    context = steam_network_open(urllib.request.build_opener(_NoRedirect()), req, timeout) if is_steam else urllib.request.build_opener(_NoSteamRedirect()).open(req, timeout=timeout)
    with context as resp:
        body = resp.read()
        if resp.headers.get('Content-Encoding', '').lower() == 'gzip':
            body = gzip.decompress(body)
        value = json.loads(body.decode('utf-8', 'replace'))
        if metadata is not None:
            # Last-Modified describes the upstream HTTP representation. Neither
            # HTTP Date nor our fetch clock proves when a market trade occurred.
            modified = resp.headers.get('Last-Modified')
            modified = modified if isinstance(modified, str) and len(modified) <= 128 else None
            etag = resp.headers.get('ETag')
            etag = etag if isinstance(etag, str) and len(etag) <= 512 else None
            modified_at = None
            if modified:
                try:
                    parsed = email.utils.parsedate_to_datetime(modified)
                    if parsed.tzinfo is not None:
                        modified_at = int(parsed.timestamp())
                except (TypeError, ValueError, OverflowError):
                    pass
            metadata.update(fetchedAt=int(time.time()), upstreamLastModified=modified,
                            upstreamSnapshotModifiedAt=modified_at, upstreamEtag=etag,
                            contentSHA256=hashlib.sha256(body).hexdigest())
        return value


def save_provider_fetch_receipt(conn, provider, source, metadata, received_rows, valid_rows,
                                observation_as_of=None, content_replayed=False):
    """Join a successful fetch receipt to the caller's provider transaction."""
    receipt = {
        'version': 1, 'provider': provider, 'source': 'csgotrader:' + source,
        'ingestionVersion': PROVIDER_SNAPSHOT_INGESTION_VERSION,
        'contentSHA256': metadata.get('contentSHA256'),
        'observationAsOf': observation_as_of, 'contentReplayed': bool(content_replayed),
        'fetchedAt': metadata.get('fetchedAt'),
        'upstreamSnapshotModifiedAt': metadata.get('upstreamSnapshotModifiedAt'),
        'upstreamLastModified': metadata.get('upstreamLastModified'),
        'upstreamEtag': metadata.get('upstreamEtag'),
        'receivedRows': int(received_rows), 'validRows': int(valid_rows),
        'fetchReceiptState': 'last_successful_fetch',
        'upstreamTimeSemantics': 'http_representation_last_modified_not_trade_time',
    }
    conn.execute('''INSERT INTO settings(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value''',
        ('provider_fetch_receipt:csgotrader:' + provider,
         json.dumps(receipt, separators=(',', ':'))))
    return receipt


def provider_fetch_receipt(conn, provider):
    row = conn.execute('SELECT value FROM settings WHERE key=?',
                       ('provider_fetch_receipt:csgotrader:' + provider,)).fetchone()
    try:
        value = json.loads(row['value']) if row else None
        return value if isinstance(value, dict) and value.get('version') == 1 else {}
    except (ValueError, TypeError):
        return {}


def provider_snapshot_observation(metadata, previous, captured_at):
    """Keep one capture clock per consecutive exact body/version, not per GET.

    HTTP Last-Modified and ETag alone do not qualify replay. All source rows
    still pass through current-state/metadata repair and derived-view checks.
    A missing/old receipt or changed ingestion version takes the normal path.
    """
    digest = metadata.get('contentSHA256')
    prior = previous.get('observationAsOf')
    replay = (isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest) is not None
              and previous.get('contentSHA256') == digest
              and type(previous.get('ingestionVersion')) is int
              and previous['ingestionVersion'] == PROVIDER_SNAPSHOT_INGESTION_VERSION
              and type(prior) is int and 0 < prior <= captured_at)
    return (prior, True) if replay else (captured_at, False)


def parse_iso_ts(value):
    if not value:
        return int(time.time())
    try:
        return int(datetime.datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp())
    except (TypeError, ValueError):
        return int(time.time())


# Composition source: inserted into server.py, not imported as another runtime module.
STEAM_NETWORK_SLOT = threading.Lock()
STEAM_ASK_RECOVERY_LOCK = threading.Lock()
STEAM_ASK_PROCESS_READY = False
STEAM_ASK_REQUEST_CONTEXT = threading.local()


def steam_ask_db(write=False, shared_read=False):
    if shared_read and write:
        raise ValueError("Shared holdings connections must be read-only")
    # Fail before opening a database on runtimes without the required close guard.
    no_checkpoint = getattr(sqlite3, 'SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE', None)
    if (no_checkpoint is None or not callable(getattr(sqlite3.Connection, 'setconfig', None))
            or not callable(getattr(sqlite3.Connection, 'getconfig', None))):
        raise sqlite3.NotSupportedError('Steam ask storage requires verified no-checkpoint connection support')
    conn = sqlite3.connect(Path(DB_FILE).resolve().as_uri() + ('?mode=rw' if write else '?mode=ro'),
                           uri=True, timeout=0.1, check_same_thread=not shared_read)
    try:
        # These connection settings precede every database statement and transaction.
        suppress_checkpoint = not write
        auto_checkpoint_pages = 1000 if write else 0
        conn.setconfig(no_checkpoint, suppress_checkpoint)
        if conn.getconfig(no_checkpoint) is not suppress_checkpoint:
            raise sqlite3.OperationalError('Steam ask checkpoint close policy was not verified')
        conn.execute(f'PRAGMA wal_autocheckpoint={auto_checkpoint_pages}')
        checkpoint_setting = conn.execute('PRAGMA wal_autocheckpoint').fetchone()
        if not checkpoint_setting or checkpoint_setting[0] != auto_checkpoint_pages:
            raise sqlite3.OperationalError('Steam ask automatic checkpoint policy was not verified')
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout=100')
        conn.execute('PRAGMA foreign_keys=ON')
        if not write:
            conn.execute('PRAGMA query_only=ON')
        return conn
    except BaseException:
        conn.close()
        raise


def steam_held_rows(portfolio_id=None, include_reference_priorities=False):
    """Only held names and bounded exact metadata; no full catalog map/raw/history."""
    with closing(steam_ask_db()) as conn:
        conn.execute('BEGIN')
        if portfolio_id:
            portfolio = conn.execute('SELECT id FROM portfolios WHERE id=?', (portfolio_id,)).fetchone()
        else:
            portfolio = conn.execute('SELECT id FROM portfolios ORDER BY sort_key DESC,imported_at DESC LIMIT 1').fetchone()
        if not portfolio:
            raise ValueError('Selected portfolio does not exist')
        rows = [dict(row) for row in conn.execute('SELECT name,qty FROM portfolio_items WHERE portfolio_id=? ORDER BY sort_key,name LIMIT 10001', (portfolio['id'],))]
        if len(rows) > steam_held_asks.MAX_HOLDING_ROWS:
            raise ValueError('Portfolio exceeds the 10000-row acquisition bound')
        names = list(dict.fromkeys(row['name'] for row in rows if isinstance(row['name'], str)))
        candidates, native_verified, native_rejected, reference_prices = {}, set(), set(), {}
        for start in range(0, len(names), 256):
            chunk = names[start:start + 256]
            marks = ','.join('?' for _ in chunk)
            for table in ('market_items', 'item_catalog'):
                for meta in conn.execute('SELECT name,market_hash_name,kind FROM ' + table + ' WHERE name IN (' + marks + ')', chunk):
                    if meta['market_hash_name']:
                        candidates.setdefault(meta['name'], []).append(dict(meta))
            if include_reference_priorities:
                # Existing USD-normalized reference amounts rank work only; never enter ask valuation.
                ranks = {provider: rank for rank, provider in enumerate(PRIMARY_PRICE_PROVIDER_ORDER)}
                providers = ','.join('?' for _ in ranks)
                for price in conn.execute('SELECT name,provider,price,solid_price FROM market_item_prices WHERE name IN (' + marks + ') AND provider IN (' + providers + ')', chunk + list(ranks)):
                    amount = _positive_float(price['solid_price']) or _positive_float(price['price'])
                    if amount and math.isfinite(amount):
                        prior = reference_prices.get(price['name'])
                        if prior is None or ranks[price['provider']] < prior[0]:
                            reference_prices[price['name']] = (ranks[price['provider']], amount)
        native_tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN('catalog_items_v2','catalog_identifiers_v2')")}
        if native_tables == {'catalog_items_v2', 'catalog_identifiers_v2'}:
            # Read only indexed exact Steam identifier bindings. Never enroll/repair or infer an ID.
            identities = list(dict.fromkeys(names + [meta['market_hash_name'] for values in candidates.values() for meta in values]))
            for start in range(0, len(identities), 256):
                chunk = identities[start:start + 256]
                owners = {}
                for item in conn.execute('''SELECT x.value,x.item_id,i.entity_type,i.priceable,i.market_hash_name
                    FROM catalog_identifiers_v2 x LEFT JOIN catalog_items_v2 i ON i.item_id=x.item_id
                    WHERE x.namespace='steam:730:market_hash_name' AND x.value IN(''' + ','.join('?' for _ in chunk) + ')', chunk):
                    owners.setdefault(item['value'], []).append(dict(item))
                for value, bindings in owners.items():
                    if (len(bindings) == 1 and bindings[0]['entity_type'] == 'listing'
                            and bindings[0]['priceable'] == 1 and bindings[0]['market_hash_name'] == value):
                        native_verified.add(value)
                    else:
                        native_rejected.add(value)
        holdings = []
        for row in rows:
            exact = {row['name']} if row['name'] in native_verified else set()
            for meta in candidates.get(row['name'], []):
                value = meta['market_hash_name']
                if value in native_verified:
                    exact.add(value)
                elif (value not in native_rejected and meta['kind'] in CATALOG_LEGACY_KINDS
                      and catalog_row_is_market_listing(meta)):
                    exact.add(value)
            issue = 'ambiguous_listing_identity' if len(exact) > 1 else 'no_verified_listing_identity' if not exact else None
            holdings.append({'marketHashName': next(iter(exact)) if len(exact) == 1 else None, 'qty': row['qty'],
                             'displayName': row['name'], 'identityIssue': issue,
                             'priorityReferenceUsd': (reference_prices.get(row['name']) or (None, None))[1]})
        return 'portfolio:' + portfolio['id'], holdings


def steam_ask_reconcile_startup():
    global STEAM_ASK_PROCESS_READY
    with STEAM_ASK_RECOVERY_LOCK:
        if STEAM_ASK_PROCESS_READY:
            return
        with closing(steam_ask_db(write=True)) as conn:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='steam_ask_control'").fetchone():
                return  # No schema/ask claims before explicit enrollment.
            steam_held_asks.AskStore(conn).reconcile_startup(int(time.time()))
        STEAM_ASK_PROCESS_READY = True  # Failed recovery never opens the network gate.


@contextlib.contextmanager
def steam_ask_request_context(token):
    if getattr(STEAM_ASK_REQUEST_CONTEXT, 'token', None) is not None:
        raise RuntimeError('Nested Steam ask claim context is invalid')
    STEAM_ASK_REQUEST_CONTEXT.token = token
    try:
        yield
    finally:
        STEAM_ASK_REQUEST_CONTEXT.token = None


def steam_ask_load_cooldown():
    steam_ask_reconcile_startup()
    with closing(steam_ask_db()) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='steam_ask_control'").fetchone():
            return  # Pre-install history only. An incompatible existing schema fails closed.
        blocked = steam_held_asks.AskStore(conn).cooldown(int(time.time()), getattr(STEAM_ASK_REQUEST_CONTEXT, 'token', None))
    if blocked:
        STEAM_ACCESS_GATE.honor_deadline(blocked['retryAt'], blocked.get('cooldownReason') or 'persisted_provider_cooldown')
        raise SteamRequestCooldown(blocked)


def steam_ask_record_429(retry_after, retry_at):
    state = STEAM_ACCESS_GATE.note_429(retry_after, retry_at)
    try:
        with closing(steam_ask_db(write=True)) as conn:
            steam_held_asks.AskStore(conn).persist_cooldown(state, int(time.time()))
    except steam_held_asks.AskSchemaUnavailable:
        pass
    return state



class SteamGovernorDeferred(RuntimeError):
    def __init__(self, state):
        self.state = dict(state)
        super().__init__('Steam acquisition deferred by its durable request governor')


def steam_governor_status():
    """One saved settings lookup; missing state remains paused, with no write."""
    with closing(steam_ask_db()) as conn:
        return steam_request_governor.RequestGovernor(conn).status(int(time.time()))


def steam_governor_write(method, *args, **kwargs):
    with SQLITE_WRITE_LOCK:
        with closing(steam_ask_db(write=True)) as conn:
            return getattr(steam_request_governor.RequestGovernor(conn), method)(*args, **kwargs)


def steam_governor_deferral():
    try:
        status = steam_governor_status()
    except (sqlite3.Error, steam_request_governor.GovernorError):
        return {'reason': 'governor_unavailable', 'operatorPaused': True,
                'retryAt': None, 'recoveryRequired': True}
    if status.get('operatorPaused'):
        return {'reason': status.get('pauseReason') or 'operator_paused', 'operatorPaused': True,
                'retryAt': None, 'recoveryRequired': bool(status.get('recoveryRequired'))}
    if status.get('leasePresent'):
        return {'reason': 'governor_inflight', 'operatorPaused': False,
                'retryAt': None, 'recoveryRequired': bool(status.get('recoveryRequired'))}
    due = status.get('nextAllowedAt')
    if type(due) is int and due > int(time.time()):
        return {'reason': 'request_budget_wait', 'operatorPaused': False, 'retryAt': due,
                'recoveryRequired': False}
    return None


def steam_governor_deferred_result(name, state):
    return dict(STEAM_CURRENT_ASK._base(name), success=False, status='governor_deferred',
                fetchedAt=None, lowestAsk=None, cached=False, httpStatus=None,
                error='Steam acquisition is paused or waiting for its saved request budget; no request sent.',
                governorDeferred=True, **state)


def steam_governor_control(data):
    # A control POST must not wait behind a bulk transaction after its client times out.
    # Actual reserve/finish persistence retains its separate uncertainty-safe path.
    if not SQLITE_WRITE_LOCK.acquire(blocking=False):
        raise sqlite3.OperationalError('CS2 writer is busy; governor control was not applied')
    try:
        return steam_governor_control_locked(data)
    finally:
        SQLITE_WRITE_LOCK.release()


def steam_governor_control_locked(data):
    if not isinstance(data, dict):
        raise ValueError('One explicit Steam request-control object is required')
    action = data.get('action')
    if action == 'pause' and set(data) <= {'action', 'expectedRevision'}:
        return steam_governor_write('control', 'pause', int(time.time()),
                                    expected_revision=data.get('expectedRevision'))
    if action != 'start_validation' or set(data) != {'action','expectedRevision','requestLimit','minimumIntervalSeconds'}:
        raise ValueError('Only pause or one explicit bounded validation is supported')
    # The old provider cooldown and uncertain typed lease remain independently binding.
    with closing(steam_ask_db()) as conn:
        blocked = steam_held_asks.AskStore(conn).cooldown(int(time.time()))
    if blocked or STEAM_ACCESS_GATE.cooldown() or ACTIVE_DATA_TASK_ID:
        raise ValueError('Existing provider cooldown/request lease or history task still blocks validation')
    cash_price_capacity()
    return steam_governor_write('control', 'start_validation', int(time.time()),
        expected_revision=data['expectedRevision'], validation_requests=data['requestLimit'],
        minimum_interval_seconds=data['minimumIntervalSeconds'])


class BoundedSteamResponse:
    """Count a response without retaining its body; bound legacy listing/history reads."""
    def __init__(self, response):
        self.response, self.bytes_read = response, 0
    def __getattr__(self, name):
        return getattr(self.response, name)
    def read(self, size=-1):
        remaining = 8 * 1024 * 1024 - self.bytes_read
        wanted = remaining + 1 if size is None or size < 0 else min(size, remaining + 1)
        value = self.response.read(wanted)
        self.bytes_read += len(value)
        if self.bytes_read > 8 * 1024 * 1024:
            raise ValueError('Steam response exceeded8MiB network-body cap')
        return value


@contextlib.contextmanager
def steam_network_open(opener, request, timeout):
    # All controlled Steam collectors share this persisted reservation and HTTP lifetime.
    with STEAM_NETWORK_SLOT:
        steam_ask_load_cooldown()
        if urllib.parse.urlsplit(request.full_url).path != '/market/priceoverview/':
            with closing(steam_ask_db()) as priority_conn:
                pending = steam_held_asks.AskStore(priority_conn).active_job()
            if pending:
                raise SteamGovernorDeferred({'reason':'holdings_priority','operatorPaused':False,
                    'retryAt':None,'recoveryRequired':False})
        cash_price_capacity()
        claim = steam_governor_write('reserve', request.full_url, int(time.time()))
        if claim.get('action') != 'request':
            raise SteamGovernorDeferred({key:value for key,value in claim.items() if key != 'action'})
        started = time.monotonic()
        code, raw_retry, upstream, failure, counted = None, None, None, None, None
        network_started = False
        try:
            STEAM_ACCESS_GATE.throttle(request.full_url)
            admission = steam_governor_write('mark_started', claim, int(time.time()))
            if admission.get('started') is not True:
                raise SteamGovernorDeferred({key:value for key,value in admission.items() if key != 'action'})
            network_started = True
            with opener.open(request, timeout=timeout) as response:
                code = response.status
                if response.geturl() != request.full_url:
                    raise ValueError('Unexpected Steam response target')
                counted = BoundedSteamResponse(response)
                yield counted
        except urllib.error.HTTPError as error:
            code = error.code
            headers = getattr(error, 'headers', None)
            raw = headers.get('Retry-After') if headers is not None else None
            raw_retry = raw if isinstance(raw,str) and len(raw) <= 128 else None
            upstream = steam_retry_after_timestamp(raw_retry, int(time.time()))
            try:
                if code == 429:
                    steam_ask_record_429(raw_retry, upstream)
            finally:
                error.close()
            raise
        except BaseException as error:
            failure = type(error).__name__ if network_started else 'cancelled_before_http'
            raise
        finally:
            # The HTTP context has closed before this short durable outcome transaction.
            steam_governor_write('finish', claim, int(time.time()), code,
                counted.bytes_read if counted else 0, int((time.monotonic()-started)*1000),
                retry_after=raw_retry, upstream_retry_at=upstream, transport_error=failure)


def steam_ask_public_open(request, timeout):
    return steam_network_open(urllib.request.build_opener(_NoRedirect()), request, timeout)


def durable_steam_current_ask(name, claim=None):
    name = validate_market_hash(name)
    steam_ask_reconcile_startup()
    now = int(time.time())
    with closing(steam_ask_db()) as conn:
        store = steam_held_asks.AskStore(conn)
        cached = store.current_receipt(name)
        if cached and 0 <= now - cached['fetchedAt'] < steam_request_governor.ASK_CACHE_SECONDS:
            return dict(cached, expiresAt=cached['fetchedAt'] + steam_request_governor.ASK_CACHE_SECONDS)
        negative = conn.execute("SELECT retry_at,last_error,last_status FROM steam_ask_current WHERE market_hash_name=? AND currency='USD' AND retry_at>?", (name, now)).fetchone()
        if negative:
            return dict(STEAM_CURRENT_ASK._base(name), success=False,
                        status='failed' if negative['last_status'] == 'failed' else 'unavailable', cached=True,
                        lowestAsk=None, fetchedAt=None, retryAt=negative['retry_at'], error=negative['last_error'] or 'Ask acquisition cooldown')
    deferred = steam_governor_deferral()
    if deferred:
        return steam_governor_deferred_result(name, deferred)
    single = None
    if claim is None:
        with closing(steam_ask_db(write=True)) as conn:
            single = steam_held_asks.AskStore(conn).claim_single(now)
        if single['action'] != 'request':
            return dict(STEAM_CURRENT_ASK._base(name), success=False, status=single['action'],
                        cached=False, lowestAsk=None, fetchedAt=None, error='Steam ask acquisition is paused.',
                        **{key: value for key, value in single.items() if key != 'action'})
    try:
        with steam_ask_request_context(single['token'] if single else claim['token']):
            result = STEAM_CURRENT_ASK.fetch(name, saved_deadline=steam_saved_retry_deadline)
    except SteamGovernorDeferred as error:
        result = steam_governor_deferred_result(name, error.state)
    except SteamRequestCooldown as error:
        result = dict(STEAM_CURRENT_ASK._base(name), success=False, status='cooldown', cached=False,
                      lowestAsk=None, fetchedAt=None, httpStatus=None, error='Steam requests paused.', **error.state)
    except Exception:
        blocked = STEAM_ACCESS_GATE.cooldown()
        result = dict(STEAM_CURRENT_ASK._base(name), success=False, status='cooldown' if blocked else 'failed', cached=False,
                      lowestAsk=None, fetchedAt=None, error='Steam request or cooldown persistence failed; no automatic retry.',
                      **(blocked or {}))
    if single:
        with closing(steam_ask_db(write=True)) as conn:
            steam_held_asks.AskStore(conn).finish_single(single['token'], name, result, int(time.time()))
    if result.get('success'):
        result = dict(result, durable=bool(single))  # Worker atomically saves before publishing its progress.
    return result


STEAM_HELD_ASK_WORKER = steam_held_asks.AskWorker(lambda: steam_ask_db(write=True), durable_steam_current_ask,
    before_claim=steam_ask_reconcile_startup)


def steam_ask_value_response(portfolio_id=None, fresh_seconds=3600, summary=False):
    scope, rows = steam_held_rows(portfolio_id)
    with closing(steam_ask_db()) as conn:
        conn.execute('BEGIN')
        store = steam_held_asks.AskStore(conn)
        value = store.valuation(rows, int(time.time()), fresh_seconds)
        job = conn.execute('SELECT id,holdings_fingerprint,state,updated_at FROM steam_ask_jobs WHERE scope=? ORDER BY created_at DESC,id DESC LIMIT 1', (scope,)).fetchone()
        value.update(jobId=job['id'] if job else None,
                     jobStateAtValuation=job['state'] if job else None,
                     jobUpdatedAtValuation=job['updated_at'] if job else None,
                     jobHoldingsFingerprint=job['holdings_fingerprint'] if job else None,
                     holdingsChangedSinceJob=job['holdings_fingerprint'] != value['holdingsFingerprint'] if job else None)
    if summary:
        value.pop('items', None)
    return dict(value, scope=scope, summaryOnly=bool(summary))


def steam_ask_job_response(job_id=None):
    with closing(steam_ask_db()) as conn:
        store = steam_held_asks.AskStore(conn)
        job = store.job_status(job_id) if job_id else store.active_job()
        if not job and job_id is None:
            row = conn.execute('SELECT id FROM steam_ask_jobs ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            job = store.job_status(row['id']) if row else None
        elif job:
            job = store.job_status(job['id'])
        cooldown = store.cooldown(int(time.time()))
    return {'job': job, 'cooldown': cooldown, 'workerRunning': bool(STEAM_HELD_ASK_WORKER.thread and STEAM_HELD_ASK_WORKER.thread.is_alive()),
            'workerError': STEAM_HELD_ASK_WORKER.last_error, 'source': 'steam_priceoverview', 'currency': 'USD',
            'governor': steam_governor_status()}


def steam_ask_job_action(data):
    if not isinstance(data, dict):
        raise ValueError('One job action object is required')
    steam_ask_reconcile_startup()
    action = data.get('action', 'start')
    if action in ('start','resume') and steam_governor_status().get('operatorPaused', True):
        raise ValueError('Steam acquisition is paused by the saved request governor. Saved quotes remain available.')
    if action == 'start' and set(data) <= {'action', 'portfolioId', 'freshnessSeconds'}:
        portfolio_id = data.get('portfolioId')
        if portfolio_id is not None and (not isinstance(portfolio_id, str) or len(portfolio_id) > 128):
            raise ValueError('Invalid portfolio ID')
        scope, rows = steam_held_rows(portfolio_id, include_reference_priorities=True)
        with closing(steam_ask_db(write=True)) as conn:
            job = steam_held_asks.AskStore(conn).create_job(scope, rows, int(time.time()), data.get('freshnessSeconds', 3600))
    elif action in ('resume', 'cancel') and set(data) == {'action', 'jobId'}:
        job_id = data['jobId']
        if not isinstance(job_id, str) or re.fullmatch(r'[0-9a-f]{32}', job_id) is None:
            raise ValueError('Invalid job ID')
        with closing(steam_ask_db(write=True)) as conn:
            store = steam_held_asks.AskStore(conn)
            job = store.resume(job_id, int(time.time())) if action == 'resume' else store.cancel(job_id, int(time.time()))
    else:
        raise ValueError('Unsupported job action or fields')
    if action != 'cancel':
        STEAM_HELD_ASK_WORKER.kick()
    return {'job': job, 'started': action == 'start', 'currency': 'USD', 'source': 'steam_priceoverview'}


def throttle_steam_request(url):
    STEAM_ACCESS_GATE.throttle(url)


def catalog_stale(conn):
    row = conn.execute("SELECT COUNT(*) n, MIN(updated_at) oldest FROM item_catalog").fetchone()
    if row['n'] < 1000:
        return True
    return int(time.time()) - (row['oldest'] or 0) > CATALOG_TTL


def normalize_catalog_item(kind, obj):
    # Existing nine kinds deliberately retain their original normalization and
    # identifiers. New published classes require exact source identity, and an
    # explicit market hash before they can become a priceable listing.
    strict = kind not in CATALOG_LEGACY_KINDS
    if strict and kind not in CATALOG_REPOSITORY_SOURCES:
        raise ValueError('Unconfigured catalog class: ' + str(kind))
    if strict and kind != 'tools' and not isinstance(obj.get('id'), str):
        raise ValueError('Published catalog ID required')
    if strict and kind != 'tools' and not obj['id'].strip():
        raise ValueError('Nonempty published catalog ID required')
    market_hash = obj.get('market_hash_name')
    if strict:
        if market_hash is not None and (not isinstance(market_hash, str) or not market_hash.strip() or any(ord(char) < 32 for char in market_hash)):
            raise ValueError('Invalid non-null exact market_hash_name; retained last successful snapshot')
        if obj.get('marketable') is not None and type(obj['marketable']) is not bool:
            raise ValueError('Invalid marketable flag; retained last successful snapshot')
        market_hash = market_hash if isinstance(market_hash, str) and market_hash.strip() else None
        # Inventory rows describe definitions, not individual market variants.
        # marketable=true does not establish the exact Steam listing name.
        if kind in ('tools', 'base_weapons') or obj.get('marketable') is False:
            market_hash = None
    name = market_hash or obj.get('name')
    if not isinstance(name, str) or not name.strip():
        return None
    rarity = first_name(obj, ('rarity',)) or obj.get('rarity')
    collection = first_name(obj, ('collections',), ('crates',), ('collection',))
    row = {
        'name': name, 'kind': kind,
        'item_type': first_name(obj, ('category',), ('type',)) or obj.get('type') or kind,
        'weapon': first_name(obj, ('weapon',)), 'collection': collection,
        'rarity': rarity,
        'rarity_color': first_name(obj, ('rarity', 'color')) or (obj.get('rarity') or {}).get('color') if isinstance(obj.get('rarity'), dict) else None,
        'image': obj.get('image'),
        'market_hash_name': market_hash if strict else obj.get('market_hash_name') or name,
        'raw': json.dumps({key: value for key, value in obj.items() if key != '_catalog_inventory_key'}, separators=(',', ':')),
        'updated_at': int(time.time()),
    }
    if strict:
        row.update(entity_type='listing' if market_hash else 'metadata', priceable=bool(market_hash))
    return row


def catalog_source_batch(kind, data):
    """Validate one source and select tools only from the inventory envelope."""
    coverage = {}
    if kind == 'tools':
        if not isinstance(data, dict) or not isinstance(data.get('tools'), dict):
            raise ValueError('Inventory tools category missing; retained last successful snapshot')
        categories = sorted(data)
        coverage = {'observedInventoryCategories': categories,
                    'unmappedInventoryCategories': sorted(set(categories) - CATALOG_DOCUMENTED_INVENTORY_CATEGORIES),
                    'selectedInventoryCategory': 'tools',
                    'unselectedInventoryCategories': sorted(set(categories) - {'tools'})}
        selected = []
        for key, obj in data['tools'].items():
            if not isinstance(key, str) or not key or not isinstance(obj, dict):
                raise ValueError('Invalid inventory tools def_index record')
            selected.append({**obj, '_catalog_inventory_key': key})
        data = selected
    elif isinstance(data, dict):
        data = list(data.values())
    if not isinstance(data, list) or not data:
        raise ValueError('Catalog source missing or unexpectedly empty; retained last successful snapshot')
    if any(not isinstance(obj, dict) for obj in data):
        raise ValueError('Catalog source contained a non-object item')
    return data, coverage


def catalog_source_record(kind, obj, row):
    raw = {key: value for key, value in obj.items() if key != '_catalog_inventory_key'}
    metadata = {'kind': row['kind'], 'type': row['item_type'], 'weapon': row['weapon'],
                'collection': row['collection'], 'rarity': row['rarity'],
                'rarityColor': row['rarity_color'], 'image': row['image']}
    record = {**row, 'external_id': obj.get('id'), 'raw': raw, 'metadata': metadata}
    if kind not in CATALOG_LEGACY_KINDS:
        namespace = 'bymykel:published:' + kind + ':' + row['entity_type']
        identity_value = json.dumps([obj.get('id'), row['market_hash_name']], separators=(',', ':')) if row['priceable'] else obj.get('id')
        record['identifiers'] = [(namespace, identity_value)]
        metadata.update(publishedId=obj.get('id'), identityNamespace=namespace, identityValue=identity_value,
                        identityScheme='published_id_and_exact_market_hash' if row['priceable'] else 'published_definition_id',
                        priceabilityEvidence='explicit_market_hash_name' if row['priceable'] else 'no_verified_market_listing')
        for key in ('def_index', 'pet_id', 'style', 'marketable'):
            if key in obj:
                metadata[key] = obj[key]
        if kind == 'tools':
            key = obj.get('_catalog_inventory_key')
            if not isinstance(key, str) or not key:
                raise ValueError('Inventory tools def_index identity required')
            record['external_id'] = 'tools:' + key
            record['identifiers'] = [('bymykel:inventory:tools:def_index', key)]
            metadata.update(identityNamespace='bymykel:inventory:tools:def_index',
                            identityValue=key,
                            identityScheme='inventory_category_and_def_index',
                            inventoryCategory='tools', inventoryKey=key)
    return record


def catalog_projectable_row(row):
    # Legacy bootstrap/parity only knows the original two metadata kinds.
    # New definitions remain addressable through the typed item ID API.
    return row['kind'] in CATALOG_LEGACY_KINDS or catalog_row_is_market_listing(row)


def catalog_source_coverage(store, status=None):
    """Source snapshot coverage, not a claim of live prices or all game data."""
    statuses = {row['source']: row for row in (status if status is not None else store.status()).get('sources', [])}
    saved = dict(store.connection.execute("SELECT key,value FROM catalog_store_meta WHERE key LIKE 'catalog_contract_receipt:%'"))
    result = []
    for kind, url in CATALOG_REPOSITORY_SOURCES.items():
        status = statuses.get('catalog:' + kind, {})
        try:
            receipt = json.loads(saved['catalog_contract_receipt:' + kind]) if 'catalog_contract_receipt:' + kind in saved else None
        except (TypeError, ValueError):
            receipt = None
        result.append({'kind': kind, 'source': 'catalog:' + kind, 'url': url,
                       'status': status.get('status', 'not_fetched'),
                       'lastSuccessAt': status.get('lastSuccessAt'),
                       'lastAttemptAt': status.get('lastAttemptAt'), 'error': status.get('error'),
                       'lastGoodReceipt': receipt,
                       'covered': bool(status.get('lastSuccessAt')),
                       'receiptQualified': bool(receipt and receipt.get('sourceGeneration') == status.get('generation'))})
    unknown = sorted({value for source in result for value in
                      (source['lastGoodReceipt'] or {}).get('unmappedInventoryCategories', [])})
    return {'contractVersion': CATALOG_CONTRACT_VERSION,
            'semantics': 'configured published class snapshots; not real-time trade coverage',
            'documentedClassCoverageConfigured': True, 'sources': result,
            'allConfiguredSourcesHaveLastGood': all(source['covered'] for source in result),
            'allConfiguredSourcesCurrentAttemptComplete': all(source['status'] == 'complete' for source in result),
            'allReceiptsQualified': all(source['receiptQualified'] for source in result),
            'unmappedInventoryCategories': unknown,
            'unknownFutureClassesCovered': False,
            'inventoryPolicy': 'tools only; remaining categories use their dedicated class feeds',
            'legacyProjection': 'original nine kinds plus verified new listings; new definitions are native only'}


def catalog_row_is_market_listing(row):
    """Keep legacy identities stable; require evidence for new typed classes."""
    row = dict(row)
    kind = row.get('kind') or ''
    if kind in ('collections', 'skin_families'):
        return False
    if row.get('priceable') is False or row.get('entity_type', row.get('entityType')) in ('metadata', 'collection', 'skin_family', 'legacy_reference'):
        return False
    if kind in CATALOG_REPOSITORY_SOURCES and kind not in CATALOG_LEGACY_KINDS:
        return bool(row.get('market_hash_name', row.get('marketHashName')))
    return True


def backfill_catalog_collections(conn):
    collection_rows = conn.execute(
        "SELECT name, image, raw FROM item_catalog WHERE kind='collections'"
    ).fetchall()
    by_base = {}
    for row in collection_rows:
        try:
            raw = json.loads(row['raw'] or '{}')
        except (TypeError, json.JSONDecodeError):
            continue
        for child in raw.get('contains') or []:
            child_name = (child or {}).get('name')
            if child_name:
                by_base[base_item_name(child_name)] = row['name']
    if not by_base:
        return 0
    skin_rows = conn.execute(
        "SELECT name FROM item_catalog WHERE kind='skins' AND collection IS NULL"
    ).fetchall()
    updates = []
    for row in skin_rows:
        collection = by_base.get(base_item_name(row['name']))
        if collection:
            updates.append((collection, row['name']))
    if updates:
        conn.executemany("UPDATE item_catalog SET collection=? WHERE name=?", updates)
        mark_catalog_source_changed(conn)
        catalog_sync_legacy_metadata(conn, 'item_catalog', names=[name for _, name in updates])
    return len(updates)


def refresh_catalog_repository(force=False):
    """Publish each complete source independently; retain every failed source."""
    global COLLECTION_CASE_ALIAS_CACHE
    if not CATALOG_STORE_WRITES and not force:
        return
    now = int(time.time())
    with db() as conn:
        store = catalog_writer(conn)
        receipts = {row['source']: row for row in store.status().get('sources', [])}
    due = force or any(
        not receipts.get('catalog:' + kind, {}).get('lastSuccessAt')
        or now - receipts['catalog:' + kind]['lastSuccessAt'] > CATALOG_TTL
        for kind in CATALOG_REPOSITORY_SOURCES)
    if not due:
        return
    outcomes = []
    for kind, url in CATALOG_REPOSITORY_SOURCES.items():
        attempted_at = int(time.time())
        try:
            payload = fetch_json(url)
            fetched_at = int(time.time())
            data, coverage = catalog_source_batch(kind, payload)
            normalized, records = [], []
            for obj in data:
                row = normalize_catalog_item(kind, obj)
                if row is None:
                    raise ValueError('Catalog source contained an item without an identity')
                normalized.append(row)
                records.append(catalog_source_record(kind, obj, row))
            coverage.update(contractVersion=CATALOG_CONTRACT_VERSION, receivedRows=len(data),
                            listingRows=sum(catalog_row_is_market_listing(row) for row in normalized),
                            definitionRows=sum(not catalog_row_is_market_listing(row) for row in normalized),
                            attemptedAt=attempted_at, fetchedAt=fetched_at, sourceUrl=url)
            outcomes.append((kind, records, normalized, coverage, None))
        except Exception as exc:
            outcomes.append((kind, [], [], {'attemptedAt': attempted_at}, str(exc)))
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            store = catalog_writer(conn)
            projected, native_published = False, False
            projected_names = []
            for kind, records, rows, coverage, error in outcomes:
                try:
                    published = store.publish_source('catalog:' + kind, records, attempted_at=coverage.get('fetchedAt', coverage['attemptedAt']),
                                                     complete=error is None, error=error)
                except (ValueError, catalog_store.IdentityConflict) as exc:
                    published = store.publish_source('catalog:' + kind, [], attempted_at=coverage.get('fetchedAt', coverage['attemptedAt']),
                                                     complete=False, error=str(exc))
                if not published.get('ok'):
                    continue
                native_published = True
                coverage['sourceGeneration'] = published['generation']
                conn.execute('INSERT INTO catalog_store_meta(key,value) VALUES(?,?) '
                             'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                             ('catalog_contract_receipt:' + kind, json.dumps(coverage, separators=(',', ':'))))
                rows = [row for row in rows if catalog_projectable_row(row)]
                if not rows:
                    continue
                # Never send new definitions through legacy listing inference.
                conn.executemany(
                    """INSERT INTO item_catalog
                       (name, kind, item_type, weapon, collection, rarity, rarity_color,
                        image, market_hash_name, raw, updated_at)
                       VALUES (:name, :kind, :item_type, :weapon, :collection, :rarity,
                               :rarity_color, :image, :market_hash_name, :raw, :updated_at)
                       ON CONFLICT(name) DO UPDATE SET kind=excluded.kind,
                         item_type=excluded.item_type, weapon=excluded.weapon,
                         collection=excluded.collection, rarity=excluded.rarity,
                         rarity_color=excluded.rarity_color, image=excluded.image,
                         market_hash_name=excluded.market_hash_name, raw=excluded.raw,
                         updated_at=excluded.updated_at""", rows)
                projected = True
                projected_names.extend(row['name'] for row in rows)
            if projected:
                backfill_catalog_collections(conn)
                catalog_sync_legacy_metadata(conn, 'item_catalog', names=projected_names)
                sync_market_items_from_catalog(conn)
                COLLECTION_CASE_ALIAS_CACHE = None
            if native_published:
                mark_catalog_source_changed(conn)


def catalog_payload_from_store(store):
    by_name, identities, preferred = {}, {}, {}
    timestamps, physical_names, canonical_names = [], set(), set()
    for item in store.iter_compatibility_items():
        canonical_names.add(item['name'])
        rank = (item.get('entityType') == 'listing', item['itemId'])
        variants = []
        # Match the canonical projection's source preference. For a physical
        # alias, retain that exact row's normalized fields and provenance.
        for record in sorted(item.get('legacyMetadataRecords', []),
                             key=lambda row: row['store'] == 'item_catalog'):
            physical_names.add(record['name'])
            variants.append((record['name'], record['metadata'], [{'source': record['source'],
                'rawRef': record['rawRef'], 'updatedAt': record['updatedAt'], 'active': record['active']}]))
        # Canonical/feed-normalized fields retain their existing precedence.
        variants.append((item['name'], item.get('metadata') or {}, item.get('provenance', [])))
        for name, metadata, provenance in variants:
            identities.setdefault(name, set()).add(item['itemId'])
            if name in preferred and preferred[name] > rank:
                continue
            preferred[name] = rank
            by_name[name] = {
                'name': name, 'kind': metadata.get('kind') or item.get('kind'),
                'type': metadata.get('type'), 'weapon': metadata.get('weapon'),
                'collection': metadata.get('collection'), 'rarity': metadata.get('rarity'),
                'rarityColor': metadata.get('rarityColor'), 'image': metadata.get('image'),
                'marketHashName': item.get('marketHashName') if item.get('entityType') == 'listing' else None,
                'entityType': item.get('entityType'), 'priceable': bool(item.get('priceable')),
                'provenance': provenance,
            }
        updated_at = (item.get('freshness') or {}).get('metadataUpdatedAt')
        if updated_at is not None:
            timestamps.append(int(updated_at))
    for name, value in by_name.items():
        candidates = sorted(identities[name])
        value.update(itemId=candidates[0] if len(candidates) == 1 else None,
                     itemIds=candidates, identityStatus='unique' if len(candidates) == 1 else 'ambiguous')
        if len(candidates) > 1:
            value.update(priceable=False, marketHashName=None)
    status = store.status()
    return {
        'items': [by_name[name] for name in sorted(by_name)],
        'updatedAt': min(timestamps) if timestamps else None,
        'freshness': {'oldestMetadataAt': min(timestamps) if timestamps else None,
                      'newestMetadataAt': max(timestamps) if timestamps else None},
        'coverage': {**status, 'legacyNameCollisions': sum(len(ids) - 1 for ids in identities.values()),
                     'physicalMetadataNamesIncluded': len(physical_names),
                     'additionalAliasNames': len(set(by_name) - canonical_names),
                     'publishedClasses': catalog_source_coverage(store, status)},
    }


def refresh_catalog(force=False):
    if CATALOG_STORE_ENABLED:
        return refresh_catalog_repository(force=force)
    global COLLECTION_CASE_ALIAS_CACHE
    with db() as conn:
        stale = catalog_stale(conn)
    if not force and not stale:
        with SQLITE_WRITE_LOCK:
            with db() as conn:
                backfill_catalog_collections(conn)
                sync_market_items_from_catalog(conn)
                cleanup_market_universe(conn)
                COLLECTION_CASE_ALIAS_CACHE = None
        return
    rows = []
    for kind, url in CATALOG_SOURCES.items():
        try:
            data = fetch_json(url)
        except Exception:
            continue
        if isinstance(data, dict):
            data = list(data.values())
        for obj in data:
            if not isinstance(obj, dict):
                continue
            row = normalize_catalog_item(kind, obj)
            if row:
                rows.append(row)
    if not rows:
        return
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute("DELETE FROM item_catalog")
            conn.executemany(
                """INSERT OR REPLACE INTO item_catalog
                   (name, kind, item_type, weapon, collection, rarity, rarity_color, image,
                    market_hash_name, raw, updated_at)
                   VALUES (:name, :kind, :item_type, :weapon, :collection, :rarity,
                           :rarity_color, :image, :market_hash_name, :raw, :updated_at)""",
                rows,
            )
            backfill_catalog_collections(conn)
            sync_market_items_from_catalog(conn)
            cleanup_market_universe(conn)
            mark_catalog_source_changed(conn)
            COLLECTION_CASE_ALIAS_CACHE = None


def catalog_payload():
    if CATALOG_COMPATIBILITY_FEED_ENABLED:
        with closing(catalog_read_db()) as conn:
            conn.execute('BEGIN')
            return catalog_payload_from_store(require_catalog_store(conn))
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        rows = conn.execute(
            """SELECT name, kind, item_type AS type, weapon, collection, rarity,
                      rarity_color AS rarityColor, image, market_hash_name AS marketHashName,updated_at
               FROM item_catalog"""
        ).fetchall()
    items = [dict(row) for row in rows]
    updated = max((int(row.pop('updated_at') or 0) for row in items),default=None)
    return {'items':items,'updatedAt':updated,'dataState':'saved','cacheState':'cached' if items else 'missing',
            'freshness':{'sourceAsOf':updated,'currentness':'saved_metadata'}}


def catalog_market_projection(conn, row, projection_context=None):
    """Project verified listing evidence without replacing its namesake definition.

    Native registry enrollment is never attempted here. Only an existing exact
    Steam identifier and immutable published skins evidence can resolve a legacy
    family collision; aliases and family labels are not identity proof.
    """
    row = dict(row)
    if catalog_row_is_market_listing(row):
        return {'status': 'listing', 'row': row}
    if row.get('kind') != 'skin_families':
        return {'status': 'unresolved', 'reason': 'metadata_entity', 'row': None}

    def unresolved(reason):
        return {'status': 'unresolved', 'reason': reason, 'name': row.get('name'), 'row': None}

    name = row.get('name')
    try:
        family_raw = json.loads(row.get('raw') or '{}')
    except (TypeError, ValueError):
        return unresolved('invalid_family_evidence')
    if (not isinstance(family_raw, dict) or not isinstance(family_raw.get('id'), str)
            or not family_raw['id'] or family_raw.get('market_hash_name') not in (None, name)
            or not isinstance(name, str) or not name):
        return unresolved('no_usable_published_family_identity')
    if projection_context is not None:
        if projection_context.setdefault('connection', conn) is not conn:
            raise ValueError('Projection context belongs to another connection')
        cached = projection_context.get('resolved', {}).get((name, family_raw['id']))
        if cached is not None:
            return dict(cached, row=dict(cached['row']))
    ready = projection_context.get('registryPresent') if projection_context is not None else None
    if ready is None:
        required = {'catalog_identifiers_v2', 'catalog_items_v2', 'catalog_metadata_v2', 'catalog_payloads_v2'}
        present = {value[0] for value in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?)", tuple(sorted(required)))}
        ready = present == required
        if projection_context is not None:
            projection_context['registryPresent'] = ready
    if not ready:
        return unresolved('native_listing_registry_unavailable')
    owners = conn.execute('''SELECT i.item_id,i.name,i.entity_type,i.priceable,i.kind,i.market_hash_name,
        m.active,m.updated_at,
        CASE WHEN length(CAST(m.raw_ref_json AS BLOB)) <= 8192 THEN m.raw_ref_json END AS raw_ref_json
        FROM catalog_identifiers_v2 x
        LEFT JOIN catalog_items_v2 i ON i.item_id=x.item_id
        LEFT JOIN catalog_metadata_v2 m ON m.item_id=i.item_id AND m.source='catalog:skins'
        WHERE x.namespace='steam:730:market_hash_name' AND x.value=? LIMIT 2''', (name,)).fetchall()
    if len(owners) != 1:
        return unresolved('missing_or_ambiguous_native_listing')
    owner = dict(owners[0])
    if (owner['entity_type'] != 'listing' or owner['priceable'] != 1 or owner['kind'] != 'skins'
            or owner['name'] != name or owner['market_hash_name'] != name or owner['active'] != 1
            or type(owner['updated_at']) is not int or owner['updated_at'] <= 0):
        return unresolved('native_listing_identity_mismatch')
    try:
        reference = json.loads(owner['raw_ref_json'] or '{}')
    except (TypeError, ValueError):
        return unresolved('invalid_published_payload_reference')
    payload_id = reference.get('payloadId') if isinstance(reference, dict) else None
    if (not isinstance(reference, dict) or reference.get('storage') != 'catalog_payload'
            or not isinstance(payload_id, str) or re.fullmatch(r'[0-9a-f]{64}', payload_id) is None):
        return unresolved('invalid_published_payload_reference')
    payload = conn.execute('''SELECT CASE WHEN length(CAST(data AS BLOB)) <= 131072 THEN data END AS data
        FROM catalog_payloads_v2 WHERE payload_id=?''', (payload_id,)).fetchone()
    if not payload or not isinstance(payload['data'], (str, bytes)):
        return unresolved('published_payload_missing_or_oversized')
    data = payload['data'].encode('utf-8') if isinstance(payload['data'], str) else payload['data']
    if hashlib.sha256(data).hexdigest() != payload_id:
        return unresolved('published_payload_hash_mismatch')
    try:
        published = json.loads(data)
    except (TypeError, ValueError, UnicodeError):
        return unresolved('invalid_published_payload')
    if (not isinstance(published, dict) or published.get('id') != family_raw['id']
            or published.get('market_hash_name') != name or published.get('marketable') is False):
        return unresolved('published_listing_identity_mismatch')
    projected = normalize_catalog_item('skins', published)
    if not projected or projected['name'] != name or projected['market_hash_name'] != name:
        return unresolved('published_listing_normalization_mismatch')
    # Retain the immutable source bytes and its saved capture clock, not this read's time.
    projected.update(raw=data.decode('utf-8'), updated_at=owner['updated_at'])
    proof = {'status': 'resolved', 'itemId': owner['item_id'], 'source': 'catalog:skins',
             'publishedId': published['id'], 'payloadId': payload_id,
             'marketHashName': name, 'definitionRetained': True}
    projected['_marketProjection'] = proof
    result = dict(proof, row=projected)
    if projection_context is not None:
        projection_context.setdefault('resolved', {})[(name, family_raw['id'])] = result
    return result


def sync_market_items_from_catalog(conn):
    now = int(time.time())
    rows = [dict(r) for r in conn.execute(
        """SELECT name, market_hash_name, kind, item_type, weapon, collection,
                  rarity, rarity_color, image, raw, updated_at
           FROM item_catalog"""
    )]
    if not rows:
        return 0
    payload = []
    projection_context = {'connection': conn}
    for r in rows:
        if not catalog_row_is_market_listing(r):
            projection = catalog_market_projection(conn, r, projection_context)
            if projection['row'] is None:
                continue
            r = projection['row']
        payload.append({
            'name': r.get('name'),
            'market_hash_name': r.get('market_hash_name') or r.get('name'),
            'kind': r.get('kind'),
            'item_type': r.get('item_type'),
            'weapon': r.get('weapon'),
            'collection': r.get('collection'),
            'rarity': r.get('rarity'),
            'rarity_color': r.get('rarity_color'),
            'image': r.get('image'),
            'raw': r.get('raw'),
            'first_seen_at': now,
            'updated_at': r.get('updated_at') or now,
        })
    conn.executemany(
        """INSERT INTO market_items
           (name, market_hash_name, kind, item_type, weapon, collection, rarity,
            rarity_color, image, raw, first_seen_at, updated_at)
           VALUES (:name, :market_hash_name, :kind, :item_type, :weapon, :collection,
                   :rarity, :rarity_color, :image, :raw, :first_seen_at, :updated_at)
           ON CONFLICT(name) DO UPDATE SET
             market_hash_name=excluded.market_hash_name,
             kind=excluded.kind,
             item_type=excluded.item_type,
             weapon=excluded.weapon,
             collection=excluded.collection,
             rarity=excluded.rarity,
             rarity_color=excluded.rarity_color,
             image=excluded.image,
             raw=excluded.raw,
             updated_at=excluded.updated_at""",
        payload,
    )
    if payload:
        mark_catalog_source_changed(conn)
    catalog_sync_legacy_metadata(conn, 'market_items', records=payload)
    return len(payload)


def infer_market_meta_from_name(name):
    name = (name or '').strip()
    low = name.lower()
    if not name:
        return {}
    meta = {'name': name, 'market_hash_name': name}
    event = event_from_name(name)
    if name.startswith('Sticker Slab | '):
        meta.update({'kind': 'stickers', 'item_type': 'Sticker Slab', 'collection': event, 'rarity': 'Sticker'})
    elif name.startswith('★') or re.search(r'\b(knife|gloves|wraps)\b', name, re.I):
        meta.update({'kind': 'skins', 'item_type': 'Knives', 'weapon': base_item_name(name), 'rarity': 'Covert'})
    elif (
        ' capsule' in low or ' package' in low or ' case' in low or
        'viewer pass' in low or 'souvenir token' in low or
        low.endswith(' pass') or ' access pass' in low or ' premium pass' in low or
        '(holo/foil)' in low
    ):
        container_type = (
            'Sticker Capsule' if 'capsule' in low or '(holo/foil)' in low else
            'Souvenir Package' if 'package' in low else
            'Viewer Pass' if 'viewer pass' in low else
            'Operation Pass' if ' pass' in low else
            'Case'
        )
        meta.update({'kind': 'crates', 'item_type': container_type, 'collection': name if 'case' in low else event, 'rarity': 'Base Grade'})
    elif low.endswith(' key') or low == 'name tag' or 'swap tool' in low or 'storage unit' in low:
        tool_type = 'Key' if low.endswith(' key') else 'Name Tag' if low == 'name tag' else 'Storage Unit' if 'storage unit' in low else 'Tool'
        meta.update({'kind': 'tools', 'item_type': tool_type, 'rarity': 'Base Grade'})
    elif name.startswith('Sticker |'):
        sticker_type = 'Team' if event else 'Sticker'
        if any(term in low for term in ('holo', 'foil', 'gold', 'glitter')):
            sticker_type = 'Team' if event else 'Finish'
        meta.update({'kind': 'stickers', 'item_type': sticker_type, 'collection': event, 'rarity': 'Sticker'})
    elif name.startswith('Sealed Graffiti |'):
        meta.update({'kind': 'graffiti', 'item_type': 'Graffiti', 'collection': event, 'rarity': 'High Grade'})
    elif name.endswith(' Pin') or ' pin' in low:
        meta.update({'kind': 'pins', 'item_type': 'Pin', 'collection': event, 'rarity': 'Collectible'})
    elif ' | ' in name and not wear_from_name(name):
        # Agents use "Name | Faction" and are frequently absent from the catalog.
        meta.update({'kind': 'agents', 'item_type': 'Agent', 'weapon': name.rsplit(' | ', 1)[-1], 'collection': event, 'rarity': 'Agent'})
    return {k: v for k, v in meta.items() if v}


def repair_market_item_metadata(conn, limit=None):
    q = """SELECT name FROM market_items
           WHERE COALESCE(kind, item_type, weapon, collection, rarity, image, '')=''"""
    params = []
    if limit:
        q += " LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(q, params).fetchall()
    updated = 0
    updated_names = []
    for row in rows:
        meta = infer_market_meta_from_name(row['name'])
        if not meta.get('kind') and not meta.get('item_type'):
            continue
        conn.execute(
            """UPDATE market_items
               SET market_hash_name=COALESCE(market_hash_name, ?),
                   kind=COALESCE(kind, ?),
                   item_type=COALESCE(item_type, ?),
                   weapon=COALESCE(weapon, ?),
                   collection=COALESCE(collection, ?),
                   rarity=COALESCE(rarity, ?),
                   updated_at=?
               WHERE name=?""",
            (
                meta.get('market_hash_name') or row['name'],
                meta.get('kind'),
                meta.get('item_type'),
                meta.get('weapon'),
                meta.get('collection'),
                meta.get('rarity'),
                int(time.time()),
                row['name'],
            ),
        )
        updated += 1
        updated_names.append(row['name'])
    if updated_names:
        mark_catalog_source_changed(conn)
        catalog_sync_legacy_metadata(conn, 'market_items', names=updated_names)
    return updated


def cleanup_market_universe(conn):
    """Remove non-listing catalog templates that never received provider prices."""
    result = {'removed': 0, 'metadataUpdated': 0}
    result['metadataUpdated'] = repair_market_item_metadata(conn)
    cur = conn.execute(
        """DELETE FROM market_items
           WHERE kind IN ('collections','skin_families')
             AND NOT EXISTS (
               SELECT 1 FROM market_item_prices p
               WHERE p.name=market_items.name
                 AND p.provider IN ('steam_snapshot','csfloat','buff163','skinport','youpin')
                 AND p.price IS NOT NULL
             )"""
    )
    result['removed'] = cur.rowcount if cur.rowcount is not None else 0
    if result['removed']:
        mark_market_source_changed(conn)
    return result


def catalog_meta_for_name(conn, name):
    if not name:
        return None
    row = conn.execute(
        """SELECT name, market_hash_name, kind, item_type, weapon, collection,
                  rarity, rarity_color, image, raw, updated_at
           FROM item_catalog
           WHERE name=? OR market_hash_name=? ORDER BY CASE WHEN market_hash_name=? THEN 0 ELSE 1 END
           LIMIT 1""",
        (name, name, name),
    ).fetchone()
    if row is None:
        return None
    row = dict(row)
    if catalog_row_is_market_listing(row):
        return row
    projection = catalog_market_projection(conn, row)
    return projection['row'] if projection['row'] is not None else {**row, '_marketProjection': projection}


def ensure_market_item(conn, name, meta=None, metadata_memo=None, track_metadata=False):
    if not name:
        return None
    reconcile_raw = meta is not None
    memo = None
    if metadata_memo is not None:
        if metadata_memo.get('connection') is not conn:
            raise ValueError('metadata memo belongs to another writer connection')
        track_metadata = track_metadata or metadata_memo.get('trackMetadataChanges', False)
        memo = metadata_memo['names']
        if not conn.in_transaction:
            memo.clear()
        if meta is None and name in memo:
            return memo[name]
        memo.pop(name, None)
    if not meta:
        meta = catalog_meta_for_name(conn, name)
        if not meta:
            meta = infer_market_meta_from_name(name) or {}
            if name.startswith('Sticker Slab | '):
                existing_slab = conn.execute('SELECT market_hash_name FROM market_items WHERE name=?', (name,)).fetchone()
                if existing_slab or has_published_catalog_identity(conn, name):
                    # Quoted reads/ensures do not prove existing metadata was
                    # inferred. Only the guarded discovery path repairs it.
                    meta = {'name': name, 'market_hash_name':
                            (existing_slab['market_hash_name'] if existing_slab else None) or name}
    if not catalog_row_is_market_listing(meta):
        projection = meta.get('_marketProjection') or catalog_market_projection(conn, meta)
        if projection.get('row') is None:
            # A quote may retain its exact incoming name, but cannot overwrite a
            # listing with definition fields or manufacture listing metadata.
            if metadata_memo is not None:
                metadata_memo.setdefault('catalogProjectionUnresolved', {})[name] = {
                    key: value for key, value in projection.items() if key != 'row'}
            if memo is not None:
                memo[name] = name
            return name
        meta = projection['row']
    canonical = meta.get('name') or name
    metadata_fields = ('market_hash_name', 'kind', 'item_type', 'weapon', 'collection',
                       'rarity', 'rarity_color', 'image')
    previous = None
    if track_metadata and metadata_memo is not None:
        previous = conn.execute('SELECT ' + ','.join(metadata_fields) +
                                ' FROM market_items WHERE name=?', (canonical,)).fetchone()
    now = int(time.time())
    conn.execute(
        """INSERT INTO market_items
           (name, market_hash_name, kind, item_type, weapon, collection, rarity,
            rarity_color, image, raw, first_seen_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(name) DO UPDATE SET
             market_hash_name=COALESCE(excluded.market_hash_name, market_items.market_hash_name),
             kind=COALESCE(excluded.kind, market_items.kind),
             item_type=COALESCE(excluded.item_type, market_items.item_type),
             weapon=COALESCE(excluded.weapon, market_items.weapon),
             collection=COALESCE(excluded.collection, market_items.collection),
             rarity=COALESCE(excluded.rarity, market_items.rarity),
             rarity_color=COALESCE(excluded.rarity_color, market_items.rarity_color),
             image=COALESCE(excluded.image, market_items.image),
             raw=COALESCE(excluded.raw, market_items.raw),
             updated_at=MAX(market_items.updated_at, excluded.updated_at)""",
        (
            canonical,
            meta.get('market_hash_name') or name,
            meta.get('kind'),
            meta.get('item_type'),
            meta.get('weapon'),
            meta.get('collection'),
            meta.get('rarity'),
            meta.get('rarity_color'),
            meta.get('image'),
            meta.get('raw'),
            now,
            meta.get('updated_at') or now,
        ),
    )
    if canonical != name:
        ensure_market_item(conn, name, {'name': name, 'market_hash_name': name},
                           metadata_memo=metadata_memo, track_metadata=track_metadata)
    catalog_sync_legacy_metadata(conn, 'market_items', names=[canonical], reconcile_raw=reconcile_raw)
    if track_metadata and metadata_memo is not None:
        incoming = {**meta, 'market_hash_name': meta.get('market_hash_name') or name}
        if previous is None or any(incoming.get(key) is not None and previous[key] != incoming[key]
                                   for key in metadata_fields):
            metadata_memo.setdefault('catalogChangedNames', set()).add(canonical)
            if previous is None:
                metadata_memo.setdefault('catalogNewNames', set()).add(canonical)
    if memo is not None:
        memo[name] = canonical
        memo[canonical] = canonical
    return canonical


def provider_listing_name_is_usable(name):
    """Reject localization tokens and malformed keys, without inventing a price."""
    return (isinstance(name, str) and bool(name) and name == name.strip()
            and not name.startswith('#') and not any(ord(char) < 32 or ord(char) == 127 for char in name))


def has_published_catalog_identity(conn, name):
    if not CATALOG_STORE_ENABLED:
        return False
    # Resolve trusted evidence through a published ID/alias, rather than infer
    # identity or overwrite a source record merely because its name matches.
    return conn.execute('''SELECT 1 FROM catalog_aliases_v2 a
        JOIN catalog_metadata_v2 m ON m.item_id=a.item_id
        WHERE a.value=? AND a.namespace IN ('name','steam:730:market_hash_name')
          AND (m.source LIKE 'catalog:%' OR m.source IN ('legacy:item_catalog','manual'))
        LIMIT 1''', (name,)).fetchone() is not None


def discover_unpriced_market_item(conn, name, metadata_memo):
    """Admit exact known metadata or verified public-feed name families only."""
    discoveries = metadata_memo.setdefault('unpricedDiscoveries', {})
    if name in discoveries:
        return discoveries[name]
    if not provider_listing_name_is_usable(name):
        return 'rejected_name'
    meta = catalog_meta_for_name(conn, name)
    if meta:
        if not catalog_row_is_market_listing(meta):
            result = 'metadata_entity'
        else:
            ensure_market_item(conn, name, meta, metadata_memo=metadata_memo, track_metadata=True)
            result = 'known_metadata'
    else:
        existing = conn.execute('''SELECT market_hash_name,kind,item_type,weapon,collection,rarity,
            raw IS NOT NULL AS raw_present,image IS NOT NULL AS image_present,
            rarity_color IS NOT NULL AS rarity_color_present FROM market_items WHERE name=?''', (name,)).fetchone()
        recognized = next((prefix for prefix in ('Sticker Slab | ', 'Sticker | ', 'Sealed Graffiti | ')
                           if name.startswith(prefix) and all(part.strip() for part in name.split(' | '))), None)
        if not recognized:
            result = 'known_existing' if existing else 'unverified_name'
        elif has_published_catalog_identity(conn, name):
            result = 'published_identity_preserved'
        elif existing and recognized == 'Sticker Slab | ':
            inferred_agent = (existing['kind'] == 'agents' and existing['item_type'] == 'Agent'
                and existing['rarity'] == 'Agent' and existing['market_hash_name'] == name
                and existing['weapon'] == name.rsplit(' | ', 1)[-1]
                and existing['collection'] == event_from_name(name)
                and not existing['raw_present'] and not existing['image_present']
                and not existing['rarity_color_present'])
            if inferred_agent:
                # The old generic agent inference populated this exact field.
                # Clear it only under the old-inference fingerprint above.
                conn.execute('UPDATE market_items SET weapon=NULL WHERE name=?', (name,))
                ensure_market_item(conn, name, infer_market_meta_from_name(name),
                                   metadata_memo=metadata_memo, track_metadata=True)
                result = 'inferred_slab_repaired'
            else:
                result = 'existing_metadata_preserved'
        elif existing:
            result = 'known_existing'
        else:
            ensure_market_item(conn, name, infer_market_meta_from_name(name),
                               metadata_memo=metadata_memo, track_metadata=True)
            result = 'recognized_name'
    discoveries[name] = result
    return result


def provider_key_from_source(source=None, provider=None, quote=None):
    source = str(source or '').strip().lower()
    provider = str(provider or '').strip().lower()
    if not source and quote:
        source = str(quote.get('source') or quote_source_from_raw(quote.get('raw')) or '').lower()
        provider = str(quote.get('provider') or provider or '').lower()
    if source == 'steam_history' or provider == 'steam_history':
        return 'steam_history'
    if source.startswith('csgotrader:'):
        market = normalize_market_source(source.split(':', 1)[1].split(':', 1)[0])
        return 'steam_snapshot' if market == 'steam' else market
    if source == 'steam_quote' or provider == 'steam':
        return 'steam'
    if provider in PRICE_PROVIDER_LABELS:
        return provider
    market = normalize_market_source(source)
    return market if market in PRICE_PROVIDER_LABELS else (provider or 'market')


def provider_confidence(provider, quote, source=None):
    stability = _positive_float((quote or {}).get('priceStabilityPct'))
    if provider == 'steam' and stability is not None:
        return round(max(0, min(100, stability)), 1)
    if provider == 'steam':
        return 70.0
    if provider == 'steam_snapshot':
        return 55.0
    if provider == 'steam_history':
        return 35.0
    return 50.0


def market_price_record_from_quote(quote, provider=None, source=None):
    if not quote or not quote.get('name'):
        return None
    provider = provider_key_from_source(source=source, provider=provider or quote.get('provider'), quote=quote)
    price = _positive_float(quote.get('solidPrice')) or _positive_float(quote.get('price')) or _positive_float(quote.get('median'))
    if not price:
        return None
    source = source or quote.get('source') or quote_source_from_raw(quote.get('raw')) or provider
    observed_at = coerce_market_timestamp(quote.get('updated_at') or time.time())
    basis = quote.get('priceBasis')
    if not basis:
        if provider == 'steam':
            basis = 'steam_bid_ask'
        elif provider == 'steam_history':
            basis = 'steam_history_fallback'
        else:
            basis = 'provider_snapshot'
    return {
        'name': quote.get('name'),
        'provider': provider,
        'price': price,
        'median': _positive_float(quote.get('median')) or price,
        'solid_price': _positive_float(quote.get('solidPrice')) or price,
        'lowest_sell_order': _positive_float(quote.get('lowestSellOrder')),
        'highest_buy_order': _positive_float(quote.get('highestBuyOrder')),
        'spread_pct': _positive_float(quote.get('orderBookSpreadPct')),
        'stability_pct': _positive_float(quote.get('priceStabilityPct')),
        'confidence_pct': provider_confidence(provider, quote, source),
        'volume': quote.get('volume'),
        'source': source,
        'basis': basis,
        'kind': 'legacy_steam' if provider == 'steam' else ('steam_history' if provider == 'steam_history' else 'provider_snapshot'),
        'url': quote.get('url') or steam_market_url(quote.get('name')),
        'raw': quote.get('raw') or json.dumps({'source': source}, separators=(',', ':')),
        'observed_at': observed_at,
        'updated_at': int(quote.get('updated_at') or observed_at or time.time()),
    }


def provider_prices_summary(conn, name):
    rows = [dict(r) for r in conn.execute(
        """SELECT provider, price, solid_price, lowest_sell_order, highest_buy_order,
                  spread_pct, stability_pct, confidence_pct, volume, source, basis,
                  kind, url, observed_at, updated_at
           FROM market_item_prices
           WHERE name=? AND provider NOT IN ('steam','steam_history')
           ORDER BY updated_at DESC""",
        (name,),
    )]
    return {
        r['provider']: {
            'label': PRICE_PROVIDER_LABELS.get(r['provider'], r['provider']),
            'price': r.get('price'),
            'solidPrice': r.get('solid_price'),
            'lowestSellOrder': r.get('lowest_sell_order'),
            'highestBuyOrder': r.get('highest_buy_order'),
            'spreadPct': r.get('spread_pct'),
            'stabilityPct': r.get('stability_pct'),
            'confidencePct': r.get('confidence_pct'),
            'volume': r.get('volume'),
            'source': r.get('source'),
            'basis': r.get('basis'),
            'kind': r.get('kind'),
            'url': r.get('url'),
            'observedAt': r.get('observed_at'),
            'updatedAt': r.get('updated_at'),
        }
        for r in rows
    }


def provider_divergence_signal(provider_prices):
    """Compare Steam wallet pricing with external cash-market provider comps."""
    provider_prices = provider_prices or {}

    def price_for(provider):
        row = provider_prices.get(provider) or {}
        return _positive_float(row.get('solidPrice')) or _positive_float(row.get('price'))

    steam = price_for('steam_snapshot')
    if not steam:
        return {}
    external = []
    for provider in ('csfloat', 'buff163', 'skinport', 'youpin'):
        price = price_for(provider)
        if not price:
            continue
        ratio = price / steam
        if ratio < 0.05 or ratio > 5.0:
            continue
        row = provider_prices.get(provider) or {}
        pct_vs_steam = ((price - steam) / steam) * 100
        external.append({
            'provider': provider,
            'label': row.get('label') or PRICE_PROVIDER_LABELS.get(provider, provider),
            'price': round(price, 4),
            'pctVsSteam': round(pct_vs_steam, 2),
            'dollarVsSteam': round(price - steam, 4),
        })
    if not external:
        return {'steamProviderPrice': round(steam, 4)}
    real_world = min(external, key=lambda r: r['price'])
    worst = max(external, key=lambda r: abs(r['pctVsSteam']))
    return {
        'steamProviderPrice': round(steam, 4),
        'realWorldPrice': real_world['price'],
        'realWorldProvider': real_world['provider'],
        'realWorldProviderLabel': real_world['label'],
        'steamPremiumPct': round(((steam - real_world['price']) / steam) * 100, 2),
        'steamPremiumDollar': round(steam - real_world['price'], 4),
        'providerDivergencePct': worst['pctVsSteam'],
        'providerDivergenceAbsPct': round(abs(worst['pctVsSteam']), 2),
        'providerDivergenceDollar': worst['dollarVsSteam'],
        'divergentProvider': worst['provider'],
        'divergentProviderLabel': worst['label'],
        'externalProviderCount': len(external),
    }


def recalculate_market_item_primary(conn, name):
    rows = [dict(r) for r in conn.execute(
        """SELECT provider, price, solid_price, basis, confidence_pct, updated_at
           FROM market_item_prices
           WHERE name=? AND provider IN ({})""".format(','.join('?' for _ in CURRENT_PRICE_PROVIDERS)),
        (name, *CURRENT_PRICE_PROVIDERS),
    )]
    by_provider = {
        r['provider']: r
        for r in rows
        if _positive_float(r.get('price'))
    }
    if not by_provider:
        conn.execute(
            """UPDATE market_items
               SET primary_price=NULL,
                   primary_provider=NULL,
                   primary_price_basis=NULL,
                   primary_confidence_pct=NULL,
                   primary_price_updated_at=NULL,
                   provider_prices_json=?,
                   updated_at=?
               WHERE name=?""",
            (json.dumps(provider_prices_summary(conn, name), separators=(',', ':')), int(time.time()), name),
        )
        return None
    chosen = None
    for provider in PRIMARY_PRICE_PROVIDER_ORDER:
        if provider in by_provider:
            chosen = by_provider[provider]
            break
    if not chosen:
        chosen = max(by_provider.values(), key=lambda r: _positive_float(r.get('confidence_pct')) or 0)
    summary = provider_prices_summary(conn, name)
    conn.execute(
        """UPDATE market_items
           SET primary_price=?,
               primary_provider=?,
               primary_price_basis=?,
               primary_confidence_pct=?,
               primary_price_updated_at=?,
               provider_prices_json=?,
               updated_at=?
           WHERE name=?""",
        (
            _positive_float(chosen.get('solid_price')) or _positive_float(chosen.get('price')),
            chosen.get('provider'),
            chosen.get('basis'),
            chosen.get('confidence_pct'),
            chosen.get('updated_at'),
            json.dumps(summary, separators=(',', ':')),
            int(time.time()),
            name,
        ),
    )
    return chosen


def catalog_validate_quote_binding_receipt(receipt, scope):
    """Validate compact saved provenance before it can short-circuit reconciliation."""
    bad = catalog_store.IdentityConflict
    if (not isinstance(receipt, dict) or type(receipt.get('contractVersion')) is not int
            or receipt['contractVersion'] != 1 or receipt.get('scope') != scope
            or receipt.get('provider') != scope[0] or receipt.get('source') != scope[1]
            or receipt.get('exactProviderName') != scope[2]):
        raise bad('Invalid native quote binding receipt scope or version')
    capture = receipt.get('capturedAt')
    raw_sha = receipt.get('quoteRawSHA256')
    reported_hash = receipt.get('reportedMarketHashName')
    if (type(capture) is not int or capture <= 0
            or type(receipt.get('recordedAt')) is not int or receipt['recordedAt'] <= 0
            or not isinstance(raw_sha, str) or re.fullmatch(r'[0-9a-f]{64}', raw_sha) is None
            or receipt.get('legacyObservationKey') != [scope[2], scope[0], observation_bucket(capture)]
            or (reported_hash is not None and (not isinstance(reported_hash, str) or not reported_hash))
            or receipt.get('currentWriteDisposition') not in (
                'upserted_current', 'retained_newer_current', 'retained_unchanged_current')):
        raise bad('Invalid native quote binding receipt capture or provenance')
    state = receipt.get('status')
    bindings = receipt.get('legacyStreamBindings')
    pending_count = receipt.get('pendingLegacyStreamCount')
    if not isinstance(bindings, dict) or type(pending_count) is not int:
        raise bad('Invalid native quote binding stream status')
    if state == 'unresolved':
        if (receipt.get('identityStatus') != 'unresolved_definition' or receipt.get('itemId') is not None
                or bindings or pending_count != 0 or not isinstance(receipt.get('reason'), str)
                or not receipt['reason']):
            raise bad('Invalid unresolved native quote binding receipt')
    elif state in ('legacy_stream_binding_pending', 'resolved'):
        if (receipt.get('identityStatus') != 'verified_listing' or not isinstance(receipt.get('itemId'), str)
                or not receipt['itemId'] or set(bindings) != {'market_item_prices', 'price_observations'}
                or any(value not in ('bound', 'pending') for value in bindings.values())
                or pending_count != sum(value == 'pending' for value in bindings.values())
                or (state == 'resolved' and (pending_count != 0 or receipt.get('reason') is not None))
                or (state == 'legacy_stream_binding_pending' and (
                    pending_count == 0 or receipt.get('reason') != 'legacy_reference_requires_verified_decision'))):
            raise bad('Invalid qualified native quote binding receipt')
    else:
        raise bad('Unknown native quote binding receipt status')
    return receipt


def catalog_record_quote_binding(conn, record, quote, issue, item_id, disposition, metadata_memo=None,
                                 binding_states=None):
    """One bounded issue per exact provider/source/name, with identity and link states separate."""
    scope = [record['provider'], record['source'], record['name']]
    encoded_scope = json.dumps(scope, ensure_ascii=False, separators=(',', ':'))
    key = 'catalog_native_binding_issue:v1:' + hashlib.sha256(encoded_scope.encode('utf-8')).hexdigest()
    saved = conn.execute('SELECT value FROM catalog_store_meta WHERE key=?', (key,)).fetchone()
    previous = catalog_validate_quote_binding_receipt(json.loads(saved['value']), scope) if saved else None
    bindings = dict(binding_states or {})
    if issue is None:
        if (not item_id or set(bindings) != {'market_item_prices', 'price_observations'}
                or any(value not in ('bound', 'pending') for value in bindings.values())):
            raise ValueError('Verified identity and both explicit legacy stream results required')
    elif issue.get('status') != 'unresolved_definition' or item_id is not None or bindings:
        raise ValueError('Explicit unresolved definition result required')
    if issue is None and previous is None:
        return None  # Ordinary qualified quotes retain the core's existing pending-link contract.
    captured_at = record['updated_at']
    if type(captured_at) is not int or captured_at <= 0:
        raise ValueError('Binding provenance requires the original valid quote capture')
    raw = record.get('raw') or ''
    raw_bytes = raw.encode('utf-8') if isinstance(raw, str) else bytes(raw)
    pending_count = sum(value == 'pending' for value in bindings.values())
    state = 'unresolved' if issue is not None else ('legacy_stream_binding_pending' if pending_count else 'resolved')
    receipt = dict(previous or {}, contractVersion=1, scope=scope, status=state,
                   identityStatus='unresolved_definition' if issue is not None else 'verified_listing',
                   legacyStreamBindings=bindings, pendingLegacyStreamCount=pending_count,
                   exactProviderName=record['name'], provider=record['provider'], source=record['source'],
                   itemId=item_id, currentWriteDisposition=disposition,
                   identitySemantics='provider quote name retained; no definition-to-listing promotion')
    if previous is None or captured_at >= previous['capturedAt']:
        receipt.update(capturedAt=captured_at, quoteRawSHA256=hashlib.sha256(raw_bytes).hexdigest(),
                       reportedMarketHashName=quote.get('marketHashName') or None,
                       legacyObservationKey=[record['name'], record['provider'], observation_bucket(captured_at)])
    if issue is not None:
        receipt.update(reason=issue.get('reason') or 'unresolved_definition',
                       metadataKind=issue.get('metadataKind'), metadataName=issue.get('metadataName'))
    else:
        prior_reason = (previous or {}).get('previousUnresolvedReason')
        if previous and previous['status'] == 'unresolved':
            prior_reason = previous['reason']
        receipt.update(reason='legacy_reference_requires_verified_decision' if pending_count else None,
                       previousUnresolvedReason=prior_reason)
    comparable = {key: value for key, value in receipt.items() if key != 'recordedAt'}
    previous_comparable = {key: value for key, value in (previous or {}).items() if key != 'recordedAt'}
    changed = comparable != previous_comparable
    if changed:
        receipt['recordedAt'] = int(time.time())  # Local bookkeeping, never market time.
        catalog_validate_quote_binding_receipt(receipt, scope)
        serialized = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        if len(serialized.encode('utf-8')) > 4096:
            raise ValueError('Native quote binding provenance exceeds its compact 4KiB bound')
        conn.execute('INSERT INTO catalog_store_meta(key,value) VALUES(?,?) '
                     'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, serialized))
    if metadata_memo is not None:
        if issue is not None:
            metadata_memo.setdefault('nativeBindingUnresolvedReceipts', set()).add(key)
        elif pending_count:
            metadata_memo.setdefault('nativeBindingPendingReceipts', set()).add(key)
        elif previous and previous['status'] != 'resolved':
            metadata_memo.setdefault('nativeBindingResolvedReceipts', set()).add(key)
    return {'key': key, 'status': receipt['status'], 'changed': changed,
            'identityStatus': receipt['identityStatus'], 'pendingLegacyStreamCount': pending_count}


def upsert_market_item_price(conn, quote, provider=None, source=None, recalculate=True, metadata_memo=None):
    record = market_price_record_from_quote(quote, provider=provider, source=source)
    if not record:
        return False
    if CATALOG_STORE_ENABLED:
        catalog_writer(conn)
    name = ensure_market_item(conn, record['name'], metadata_memo=metadata_memo)
    if name != record['name']:
        record['name'] = name
    binding_issue, item_id, binding_states = None, None, {}
    if CATALOG_STORE_ENABLED:
        resolution = {}
        store, item_id = catalog_identity_for_legacy(
            conn, record['name'], market_hash_name=quote.get('marketHashName') or record['name'],
            source=record['source'], unresolved_definition=resolution,
        )
        if item_id is None:
            binding_issue = resolution
        else:
            binding_states['market_item_prices'] = store.bind_legacy_or_defer(item_id, 'market_item_prices', record['name'], provider=record['provider'])['status']
            binding_states['price_observations'] = store.bind_legacy_or_defer(item_id, 'price_observations', record['name'], provider=record['provider'])['status']
    existing = conn.execute(
        "SELECT updated_at FROM market_item_prices WHERE name=? AND provider=?",
        (record['name'], record['provider']),
    ).fetchone()
    if existing and int(existing['updated_at'] or 0) > int(record['updated_at'] or 0):
        if CATALOG_STORE_ENABLED:
            catalog_record_quote_binding(conn, record, quote, binding_issue, item_id,
                                         'retained_newer_current', metadata_memo, binding_states=binding_states)
        return False
    conn.execute(
        """INSERT OR REPLACE INTO market_item_prices
           (name, provider, price, median, solid_price, lowest_sell_order,
            highest_buy_order, spread_pct, stability_pct, confidence_pct, volume,
            source, basis, kind, url, raw, observed_at, updated_at)
           VALUES (:name, :provider, :price, :median, :solid_price, :lowest_sell_order,
                   :highest_buy_order, :spread_pct, :stability_pct, :confidence_pct,
                   :volume, :source, :basis, :kind, :url, :raw, :observed_at, :updated_at)""",
        record,
    )
    column = MARKET_ITEM_PROVIDER_COLUMNS.get(record['provider'])
    if column:
        fields = [f"{column}=?", "updated_at=?"]
        values = [record['price'], int(time.time())]
        if record['provider'] == 'steam':
            fields.extend([
                "steam_lowest_sell_order=?",
                "steam_highest_buy_order=?",
                "steam_spread_pct=?",
                "steam_stability_pct=?",
                "steam_price_basis=?",
                "steam_price_updated_at=?",
            ])
            values.extend([
                record['lowest_sell_order'],
                record['highest_buy_order'],
                record['spread_pct'],
                record['stability_pct'],
                record['basis'],
                record['updated_at'],
            ])
        values.append(record['name'])
        conn.execute(f"UPDATE market_items SET {', '.join(fields)} WHERE name=?", values)
    if recalculate:
        recalculate_market_item_primary(conn, record['name'])
    if CATALOG_STORE_ENABLED:
        catalog_record_quote_binding(conn, record, quote, binding_issue, item_id,
                                     'upserted_current', metadata_memo, binding_states=binding_states)
    mark_market_source_changed(conn)
    return True


def remove_missing_provider_current_prices(conn, provider, valid_names, as_of=None):
    """Remove provider rows absent from a successful full snapshot pull.

    Provider snapshots are the current market view. Historical observations are
    kept in price_observations, but rows missing or null in the newest full
    provider payload must not remain available as current valuation data.
    """
    provider = provider_key_from_source(provider=provider) if provider else None
    if provider not in CURRENT_PRICE_PROVIDERS:
        return 0
    keep_names = sorted({n for n in (valid_names or []) if n})
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS provider_current_keep(name TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM provider_current_keep")
    if keep_names:
        conn.executemany(
            "INSERT OR IGNORE INTO provider_current_keep(name) VALUES (?)",
            [(n,) for n in keep_names],
        )
    stale_names = [
        r['name'] for r in conn.execute(
            """SELECT name FROM market_item_prices
               WHERE provider=? AND name NOT IN (SELECT name FROM provider_current_keep)
                 AND (? IS NULL OR COALESCE(updated_at,0)<=?)""",
            (provider, as_of, as_of),
        )
    ]
    if not stale_names:
        return 0
    conn.execute(
        """DELETE FROM market_item_prices
           WHERE provider=? AND name NOT IN (SELECT name FROM provider_current_keep)
             AND (? IS NULL OR COALESCE(updated_at,0)<=?)""",
        (provider, as_of, as_of),
    )
    column = MARKET_ITEM_PROVIDER_COLUMNS.get(provider)
    if column:
        for start in range(0, len(stale_names), 500):
            chunk = stale_names[start:start + 500]
            marks = ','.join('?' for _ in chunk)
            conn.execute(
                f"UPDATE market_items SET {column}=NULL, updated_at=? WHERE name IN ({marks})",
                [int(time.time())] + chunk,
            )
    for name in stale_names:
        recalculate_market_item_primary(conn, name)
    return len(stale_names)


def csgotrader_steam_aggregate_basis(raw):
    """Recognize the saved aggregate selector without treating it as an ask."""
    basis = raw.get('chosen_basis') if isinstance(raw, dict) else None
    return basis if isinstance(basis, str) and basis in (
        'last_24h_confirmed_by_rolling', 'last_7d_rolling_anchor',
        'rolling_median_anchor', 'last_24h_only') else None


def market_price_row_to_quote(row):
    if not row:
        return {}
    row = dict(row)
    basis = row.get('basis')
    steam_aggregate = row.get('provider') == 'steam_snapshot'
    if steam_aggregate and basis in (None, '', 'provider_snapshot'):
        # Existing snapshots retained the selector in compact raw evidence.
        # This read only exposes provenance; it never rewrites the saved quote.
        raw = row.get('raw')
        if isinstance(raw, str) and len(raw) <= 65536:
            try:
                evidence = json.loads(raw)
            except (TypeError, ValueError):
                evidence = None
            if (isinstance(evidence, dict) and evidence.get('source') == 'csgotrader'
                    and evidence.get('market') == 'steam'):
                basis = csgotrader_steam_aggregate_basis(evidence.get('raw')) or basis
    quote = {
        'name': row.get('name'),
        'provider': row.get('provider'),
        'price': row.get('price'),
        'median': row.get('median') or row.get('price'),
        'volume': row.get('volume'),
        'url': row.get('url'),
        'updated_at': row.get('updated_at'),
        'source': row.get('source') or row.get('provider'),
        'priceBasis': basis,
        'priceKind': row.get('kind'),
        'confidencePct': row.get('confidence_pct'),
        'lowestSellOrder': row.get('lowest_sell_order'),
        'highestBuyOrder': row.get('highest_buy_order'),
        'solidPrice': row.get('solid_price') or row.get('price'),
        'orderBookSpreadPct': row.get('spread_pct'),
        'priceStabilityPct': row.get('stability_pct'),
    }
    if steam_aggregate:
        quote.update(priceSemantics='aggregate_reference', executableAsk=False,
                     lowestAskState='saved' if _positive_float(row.get('lowest_sell_order')) else 'unavailable')
    return quote


def best_market_quotes(conn, names):
    out = {}
    for name in [n for n in dict.fromkeys(names or []) if n]:
        candidates = [name]
        meta = catalog_meta_for_name(conn, name)
        if meta:
            candidates.extend([meta.get('name'), meta.get('market_hash_name')])
        candidates = [n for n in dict.fromkeys(candidates) if n]
        qmarks = ','.join('?' for _ in candidates)
        rows = [dict(r) for r in conn.execute(
            f"""SELECT * FROM market_item_prices
                WHERE name IN ({qmarks}) AND price IS NOT NULL""",
            candidates,
        )]
        if not rows:
            continue
        by_provider = {}
        for row in rows:
            if row.get('provider') not in CURRENT_PRICE_PROVIDERS:
                continue
            current = by_provider.get(row['provider'])
            if current is None or int(row.get('updated_at') or 0) > int(current.get('updated_at') or 0):
                by_provider[row['provider']] = row
        if not by_provider:
            continue
        chosen = None
        for provider in PRIMARY_PRICE_PROVIDER_ORDER:
            if provider in by_provider:
                chosen = by_provider[provider]
                break
        if not chosen:
            chosen = max(rows, key=lambda r: _positive_float(r.get('confidence_pct')) or 0)
        quote = market_price_row_to_quote(chosen)
        quote['providerPrices'] = {
            provider: market_price_row_to_quote(row)
            for provider, row in by_provider.items()
        }
        out[name] = quote
    return out


def money_to_float(text):
    if not text:
        return None
    cleaned = ''.join(c for c in text if c.isdigit() or c in '.-')
    try:
        return float(cleaned)
    except ValueError:
        return None


def steam_market_url(name):
    return 'https://steamcommunity.com/market/listings/730/' + urllib.parse.quote(str(name or ''), safe='')


def concise_steam_fetch_error(errors):
    """Deduplicate noisy urllib/Steam failures into one user-readable reason."""
    cleaned = []
    for err in errors or []:
        text = re.sub(r'\s+', ' ', str(err or '')).strip()
        if not text:
            continue
        lower = text.lower()
        if 'redirect error' in lower and 'infinite loop' in lower:
            text = (
                'Steam redirected repeatedly while loading chart history. '
                'This usually means Steam rejected the request, the market hash URL was malformed, '
                'or the server needs a fresh STEAM_COOKIE.'
            )
        if text not in cleaned:
            cleaned.append(text)
    return ' | '.join(cleaned[-3:]) or 'Steam returned no chart data'


def observation_bucket(ts=None):
    ts = int(ts or time.time())
    return ts - (ts % OBSERVATION_BUCKET_SECONDS)


def price_source_priority(source):
    source = str(source or '').lower()
    if source == 'steam_quote':
        return 100
    if source.startswith('csgotrader:') or source in ('market_snapshot', 'steam', 'csfloat', 'buff163', 'skinport', 'youpin'):
        return 80
    if source == 'steam_history':
        return 30
    if source == 'local_observations':
        return 20
    return 50


def save_price_observation(conn, quote, source='steam_quote', observed_at=None, provider=None):
    price = _positive_float((quote or {}).get('price')) or _positive_float((quote or {}).get('median'))
    if not quote or not quote.get('name') or not price:
        return False
    observed_at = observation_bucket(observed_at or quote.get('updated_at') or time.time())
    provider = provider or quote.get('provider') or 'steam'
    existing = conn.execute(
        "SELECT source FROM price_observations WHERE name=? AND provider=? AND observed_at=?",
        (quote.get('name'), provider, observed_at),
    ).fetchone()
    if existing and price_source_priority(existing['source']) > price_source_priority(source):
        return False
    raw = quote.get('raw')
    if not raw:
        raw = json.dumps({'source': source}, separators=(',', ':'))
    changes_before = conn.total_changes
    conn.execute(
        """INSERT INTO price_observations
           (name, provider, observed_at, price, median, volume, source, raw, updated_at)
           VALUES (:name, :provider, :observed_at, :price, :median, :volume, :source, :raw, :updated_at)
           ON CONFLICT(name, provider, observed_at) DO UPDATE SET
               price=excluded.price, median=excluded.median, volume=excluded.volume,
               source=excluded.source, raw=excluded.raw, updated_at=excluded.updated_at
           WHERE price_observations.price IS NOT excluded.price
              OR price_observations.median IS NOT excluded.median
              OR price_observations.volume IS NOT excluded.volume
              OR price_observations.source IS NOT excluded.source
              OR price_observations.raw IS NOT excluded.raw
              OR price_observations.updated_at IS NOT excluded.updated_at""",
        {
            'name': quote.get('name'),
            'provider': provider,
            'observed_at': observed_at,
            'price': price,
            'median': _positive_float(quote.get('median')) or price,
            'volume': quote.get('volume'),
            'source': source,
            'raw': raw,
            'updated_at': int(quote.get('updated_at') or time.time()),
        },
    )
    if conn.total_changes != changes_before:
        mark_market_source_changed(conn)
    return True


def quote_source_from_raw(raw_text):
    try:
        raw = json.loads(raw_text or '{}')
    except (TypeError, json.JSONDecodeError):
        return None
    source = raw.get('source')
    if source == 'csgotrader':
        return 'csgotrader:' + str(raw.get('market') or 'snapshot')
    return source


def get_price_quotes(names, max_names=25, force=False):
    names = [n for n in dict.fromkeys(names) if n][:max_names]
    with db() as conn:
        out = best_market_quotes(conn, names)
    for quote in out.values():
        quote.pop('raw', None)
        quote.pop('success', None)
    return {'provider': 'csgotrader', 'currency': 'USD', 'quotes': out}



def steam_cookie_header():
    """
    Optional: set STEAM_COOKIE before starting the server if Steam returns no history.
    Example:
      export STEAM_COOKIE='steamLoginSecure=...; sessionid=...'
    Keep this private. Do not commit it into this source file.
    """
    cookie = os.environ.get('STEAM_COOKIE', '').strip()
    return cookie or None


def steam_saved_retry_deadline():
    """Read one existing provider deadline; never initialize, repair, or write data."""
    with closing(catalog_read_db()) as conn:
        conn.execute('PRAGMA busy_timeout=50')
        row = conn.execute("SELECT value FROM settings WHERE key='steam_history_retry_after' AND length(value)<=2048").fetchone()
        if not row:
            return None
        value = json.loads(row['value'])
        deadline = value.get('retryAt') if isinstance(value, dict) else None
        return deadline if type(deadline) is int else None


def steam_url_opener():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()), _NoRedirect())


def parse_steam_history_time(raw_time):
    """Convert Steam's odd history labels into a stable sortable timestamp.

    Steam commonly returns strings like:
      "Mar 31 2014 01: +0"
      "Mar 31 2014 01: +0000"
      "Mar 31 2014"

    Browsers and SQLite string sorting do not handle those reliably, so we
    normalize once on the server and return both ISO date and numeric ts.
    """
    raw = str(raw_time or '').strip()
    cleaned = re.sub(r'\s+', ' ', raw)
    candidates = [cleaned]

    # Steam sometimes returns timezone as +0, which Python expects as +0000.
    candidates.append(re.sub(r' ([+-]\d)$', r' \g<1>000', cleaned))
    candidates.append(re.sub(r' ([+-]\d{2})$', r' \g<1>00', cleaned))

    formats = [
        '%b %d %Y %H: %z',
        '%b %d %Y %H:%M %z',
        '%b %d %Y %H:',
        '%b %d %Y %H:%M',
        '%b %d %Y',
        '%b %d, %Y',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d',
    ]

    for cand in candidates:
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(cand, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                else:
                    dt = dt.astimezone(datetime.timezone.utc)
                return int(dt.timestamp()), dt.date().isoformat()
            except ValueError:
                pass

    # Last resort for already numeric timestamps.
    try:
        ts = int(float(cleaned))
        # Steam/browser timestamps may be milliseconds.
        if ts > 10_000_000_000:
            ts = ts // 1000
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        return ts, dt.date().isoformat()
    except Exception:
        return None, cleaned


def parse_steam_price_points(raw_rows):
    """Normalize Steam price-history rows into sorted [{time, ts, price, volume}]."""
    points = []
    for row in raw_rows or []:
        if isinstance(row, dict):
            raw_time = row.get('time') or row.get('date') or row.get('point_time')
            price_raw = row.get('price_median', row.get('price', row.get('median')))
            vol_raw = row.get('purchases', row.get('volume', row.get('sold')))
        elif isinstance(row, list) and len(row) >= 2:
            raw_time = row[0]
            price_raw = row[1]
            vol_raw = row[2] if len(row) >= 3 else ''
        else:
            continue
        price = money_to_float(str(price_raw))
        vol_raw = str(vol_raw).replace(',', '').strip() if vol_raw is not None else ''
        try:
            volume = int(vol_raw) if vol_raw else None
        except ValueError:
            volume = None
        if price is not None:
            ts, iso_date = parse_steam_history_time(raw_time)
            points.append({
                'time': iso_date,
                'rawTime': str(raw_time),
                'ts': ts,
                'price': price,
                'volume': volume,
            })

    # Critical fix: SQLite and Chart.js were previously receiving Steam's raw
    # date strings sorted alphabetically, causing the timeline to jump between
    # years. Sort by parsed timestamp before caching/returning.
    points.sort(key=lambda p: (p.get('ts') is None, p.get('ts') or 0, p.get('rawTime') or ''))
    return points


def parse_steam_ssr_price_points(page, market_hash_name):
    """Extract raw Steam chart history from the modern SSR market listing page."""
    if not isinstance(page, str) or len(page) > 8 * 1024 * 1024:
        return []
    render_ctx = None
    modern_container = False
    # Steam's canonical G-ID listing pages now publish the same exact-item
    # query cache in a JSON script instead of a window assignment.
    for attrs, content in re.findall(r'<script\b([^>]*)>(.*?)</script>', page, re.S | re.I):
        if not re.search(r'\bid\s*=\s*["\x27]valve-ssr-data["\x27]', attrs, re.I):
            continue
        if not re.search(r'\btype\s*=\s*["\x27]application/json["\x27]', attrs, re.I):
            continue
        try:
            root = json.loads(content)
            render_ctx = root.get('renderContext') if isinstance(root, dict) else None
            modern_container = True
        except (ValueError, TypeError):
            return []
        break
    if render_ctx is None:
        marker = re.search(r'window\.SSR\.renderContext\s*=\s*JSON\.parse\s*\(', page)
        if not marker:
            return []
        try:
            encoded, _idx = json.JSONDecoder().raw_decode(page[marker.end():])
            render_ctx = json.loads(encoded)
        except (ValueError, TypeError):
            return []
    try:
        if not isinstance(render_ctx, dict):
            return []
        query_data = render_ctx.get('queryData') or {}
        if isinstance(query_data, str):
            query_data = json.loads(query_data)
        if not isinstance(query_data, dict):
            return []
    except (ValueError, TypeError):
        return []
    for query in query_data.get('queries') or []:
        if not isinstance(query, dict):
            continue
        key = query.get('queryKey') or []
        if (
            len(key) >= 4
            and key[0] == 'market'
            and key[1] == 'pricehistory'
            and str(key[2]) == str(APPID)
            and str(key[3]) == str(market_hash_name)
        ):
            data = (query.get('state') or {}).get('data') or {}
            if not isinstance(data, dict):
                continue
            # Modern SSR explicitly identifies its currency. Never record a
            # localized chart as USD merely because the page is public.
            if modern_container and data.get('ecurrency') not in (1, '1'):
                continue
            return parse_steam_price_points(data.get('prices') or [])
    # An embedded chart for a different listing is never evidence for this item.
    return []


def steam_retry_after_timestamp(value, now=None):
    """Honor an explicit upstream delta/date; never invent a retry delay."""
    now = int(time.time() if now is None else now)
    if not isinstance(value, str) or len(value) > 128:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        return now + int(value)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is not None:
            return max(now, int(parsed.timestamp()))
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def steam_history_rate_limit_response(error, market_hash_name, display_name):
    if isinstance(error, SteamGovernorDeferred):
        state = dict(error.state)
        status = state.pop('httpStatus', None)
        return dict(name=display_name, marketHashName=market_hash_name, provider='steam', success=False,
            points=[], url=steam_market_url(market_hash_name), updated_at=int(time.time()),
            error=('Steam returned a canonical market address; its next request waits for the shared saved pacing deadline.' if state.get('canonicalRedirect') else 'Steam history acquisition deferred by its saved request governor; no request sent'),
            governorDeferred=True, rateLimited=False, httpStatus=status, cooldownActive=True, **state)
    if isinstance(error, SteamRequestCooldown):
        return {'name': display_name, 'marketHashName': market_hash_name,
                'provider': 'steam', 'success': False, 'points': [],
                'url': steam_market_url(market_hash_name), 'updated_at': int(time.time()),
                'error': 'Steam chart refresh paused by the shared provider cooldown; no request sent',
                'rateLimited': True, 'httpStatus': None, 'cooldownActive': True, **error.state}
    if getattr(error, 'code', None) != 429:
        return None
    now = int(time.time())
    headers = getattr(error, 'headers', None)
    retry_after = headers.get('Retry-After') if headers is not None else None
    retry_after = retry_after if isinstance(retry_after, str) and len(retry_after) <= 128 else None
    retry_at = steam_retry_after_timestamp(retry_after, now)
    steam_ask_record_429(retry_after, retry_at)
    try:
        error.close()
    except Exception:
        pass
    return {'name': display_name, 'marketHashName': market_hash_name,
            'provider': 'steam', 'success': False, 'points': [],
            'url': steam_market_url(market_hash_name), 'updated_at': now,
            'error': 'Steam rate limited chart history (HTTP429); stopped this batch' +
                     ('; Retry-After: ' + retry_after if retry_at is not None else ''),
            'rateLimited': True, 'httpStatus': 429, 'retryAfter': retry_after,
            'retryAt': retry_at, 'cooldownActive': False}


def fetch_steam_history(market_hash_name, display_name=None):
    """Use one existing eligible method; no automatic second endpoint request.

    Anonymous acquisition uses the public listing chart. An already configured
    history session uses JSON. Missing/failed charts remain explicit and saved
    history stays available; this avoids hidden multi-request fallback demand.
    """
    market_hash_name = (market_hash_name or display_name or '').strip()
    display_name = (display_name or market_hash_name).strip()
    listing_url = steam_market_url(market_hash_name)
    base = dict(name=display_name, marketHashName=market_hash_name, provider='steam',
                success=False, url=listing_url, points=[], updated_at=int(time.time()),
                automaticFallbackRequests=False)
    if not market_hash_name:
        return dict(base, error='Missing market_hash_name')
    headers = {'User-Agent':'CS2-Dash/SteamHistory',
               'Accept':'application/json,text/html,text/plain,*/*',
               'Accept-Language':'en-US,en;q=0.9','Referer':listing_url}
    cookie = steam_cookie_header()
    if cookie:
        headers['Cookie'] = cookie
        method = 'pricehistory'
        url = 'https://steamcommunity.com/market/pricehistory/?' + urllib.parse.urlencode(
            {'appid':APPID,'currency':1,'country':'US','market_hash_name':market_hash_name})
    else:
        method, url = 'listing_page', listing_url
    base['acquisitionMethod'] = method
    try:
        request = urllib.request.Request(url,headers=headers)
        with steam_network_open(steam_url_opener(),request,timeout=STEAM_HISTORY_FETCH_TIMEOUT) as response:
            body = response.read().decode('utf-8','replace')
        if method == 'pricehistory':
            value = json.loads(body)
            points = parse_steam_price_points(value.get('prices') or [])
            source = 'pricehistory'
        else:
            match = re.search(r'var\s+line1\s*=\s*(\[.*\]);',body,re.S)
            if not match:
                match = re.search(r'line1\s*=\s*(\[.*\]);',body,re.S)
            points = parse_steam_price_points(json.loads(html.unescape(match.group(1)))) if match else []
            source = 'listing_page'
            if not points:
                points = parse_steam_ssr_price_points(body,market_hash_name)
                source = 'listing_ssr'
        if points:
            return dict(base,success=True,source=source,points=points,updated_at=int(time.time()))
        return dict(base,error='The selected Steam history method returned no chart rows; no second endpoint was requested.')
    except Exception as error:
        deferred = steam_history_rate_limit_response(error,market_hash_name,display_name)
        if deferred:
            return dict(deferred,acquisitionMethod=method,automaticFallbackRequests=False)
        return dict(base,error='The selected Steam history method failed ('+type(error).__name__+'); saved history is retained and no automatic fallback request was sent.')


def get_price_history(name, force=False, market_hash_name=None, rebuild_metrics=True, _refresh_state=None):
    """Return locally cached Steam chart history for a single item.

    Network refreshes are explicit (`force=1`) so item graphs remain usable on
    page refresh even when Steam, the internet connection, or the Steam session
    cookie is unavailable.
    """
    name = (name or '').strip()
    if not name:
        return {'provider': 'steam', 'currency': 'USD', 'error': 'Missing name', 'points': []}

    now = int(time.time())
    steam_error = None
    save_error = None
    save_warning = None
    history_saved = False
    rate_limit = {}
    refresh_state = _refresh_state if _refresh_state is not None else ({} if force and rebuild_metrics else None)
    with (db() if force else closing(catalog_read_db())) as conn:
        if not force:
            conn.execute('BEGIN')
        if force:
            if CATALOG_STORE_ENABLED:
                # Fail before fetching if the approved catalog writer is not
                # ready. Never silently fall back to lossy daily-only saves.
                catalog_writer(conn)
            # One indexed settings lookup honors an explicit Retry-After even
            # after restart. Explicit force never bypasses this provider deadline.
            saved_limit = conn.execute("SELECT value FROM settings WHERE key='steam_history_retry_after'").fetchone()
            try:
                saved_limit = json.loads(saved_limit['value']) if saved_limit else {}
            except (TypeError, ValueError):
                saved_limit = {}
            retry_at = saved_limit.get('retryAt') if isinstance(saved_limit, dict) else None
            if isinstance(retry_at, int) and not isinstance(retry_at, bool) and retry_at > now:
                hist = {'points': [], 'error': 'Steam chart refresh paused until its Retry-After deadline',
                        'rateLimited': True, 'httpStatus': 429, 'retryAt': retry_at,
                        'retryAfter': saved_limit.get('retryAfter'), 'cooldownActive': True}
            else:
                hist = fetch_steam_history(market_hash_name or name, display_name=name)
            if hist.get('rateLimited') or hist.get('governorDeferred'):
                rate_limit = {key: hist.get(key) for key in
                              ('rateLimited','httpStatus','retryAt','retryAfter','cooldownActive','governorDeferred','reason','operatorPaused','recoveryRequired','canonicalRedirect','networkAttempted')}
                # No default cooldown is invented when Retry-After is absent or invalid.
                if not hist.get('cooldownActive') and hist.get('retryAt') is not None:
                    with SQLITE_WRITE_LOCK:
                        conn.execute("INSERT INTO settings(key,value) VALUES('steam_history_retry_after',?) "
                                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                     (json.dumps(rate_limit, separators=(',', ':')),))
                        conn.commit()
            if hist.get('points'):
                try:
                    with SQLITE_WRITE_LOCK:
                        conn.execute('BEGIN IMMEDIATE')
                        begin_history_refresh_write(conn, refresh_state)
                        if CATALOG_STORE_ENABLED:
                            store, item_id = catalog_identity_for_legacy(
                                conn, name, market_hash_name=market_hash_name or name,
                                source='steam_history',
                            )
                            store.bind_legacy_or_defer(item_id, 'price_history', name, provider='steam')
                            store.save_steam_points(
                                item_id, hist['points'], fetched_at=hist['updated_at'],
                                provider='steam', source=hist.get('source') or 'steam_history',
                                grain='unknown',
                            )
                        conn.executemany(
                            """INSERT INTO price_history
                               (name, provider, point_time, point_ts, raw_time, price, volume, updated_at)
                               VALUES (?, 'steam', ?, ?, ?, ?, ?, ?)
                               ON CONFLICT(name, provider, point_time) DO UPDATE SET
                                   point_ts=excluded.point_ts, raw_time=excluded.raw_time,
                                   price=excluded.price, volume=excluded.volume, updated_at=excluded.updated_at
                               WHERE price_history.point_ts IS NOT excluded.point_ts
                                  OR price_history.raw_time IS NOT excluded.raw_time
                                  OR price_history.price IS NOT excluded.price
                                  OR price_history.volume IS NOT excluded.volume
                                  OR price_history.updated_at IS NOT excluded.updated_at""",
                            [(name, p['time'], p.get('ts'), p.get('rawTime'), p.get('price'), p.get('volume'), hist['updated_at']) for p in hist['points']],
                        )
                        mark_market_source_changed(conn)
                        finish_history_refresh_write(conn, refresh_state)
                        conn.commit()
                        history_saved = True
                        if refresh_state is not None:
                            refresh_state.setdefault('names', set()).update([name, market_hash_name or name])
                    save_history_quote(name, market_hash_name or name, hist['points'], hist['updated_at'],
                                       _refresh_state=refresh_state)
                except sqlite3.OperationalError as exc:
                    if CATALOG_STORE_ENABLED or 'locked' not in str(exc).lower():
                        raise
                    if history_saved:
                        save_warning = 'Steam history saved, but its reference quote could not be saved: ' + str(exc)
                    else:
                        conn.rollback()
                        save_error = 'Steam history refresh could not be saved: ' + str(exc)
            else:
                steam_error = hist.get('error') or 'Steam returned no price-history rows. Try setting STEAM_COOKIE.'

        rows = conn.execute(
            """SELECT point_time AS time, point_ts AS ts, raw_time AS rawTime, price, volume, updated_at
               FROM price_history
               WHERE name=? AND provider='steam'
               ORDER BY COALESCE(point_ts, 999999999999), point_time""",
            (name,),
        ).fetchall()

    point_rows = [dict(r) for r in rows]
    if history_saved and rebuild_metrics:
        rebuild_metrics_for_names([name, market_hash_name or name], refresh_state=refresh_state)

    refresh_result = {'refreshAttempted': bool(force) and (bool(rate_limit.get('networkAttempted')) or not rate_limit.get('cooldownActive')),
                      'historySaved': history_saved, **rate_limit}
    if force:
        refresh_result['lastAttemptAt'] = now
    if steam_error or save_error:
        refresh_result['refreshError'] = steam_error or save_error
        if steam_error:
            refresh_result['steamError'] = steam_error
    if save_warning:
        refresh_result['refreshWarning'] = save_warning

    if not point_rows:
        if steam_error or save_error:
            return {
                'provider': 'steam',
                'currency': 'USD',
                'name': name,
                'url': steam_market_url(market_hash_name or name),
                'marketHashName': market_hash_name or name,
                'success': False,
                'error': steam_error or save_error,
                'source': 'steam_history',
                'points': [],
                'updatedAt': None,
                'dataState':'saved','cacheState':'missing','rebuildRequired':True,
                **refresh_result,
            }
        return {
            'provider': 'steam',
            'currency': 'USD',
            'name': name,
            'url': steam_market_url(market_hash_name or name),
            'marketHashName': market_hash_name or name,
            'success': False,
            'source': 'steam_history',
            'points': [],
            'error': 'No raw Steam chart history is cached for this item.',
            'updatedAt': None,'dataState':'saved','cacheState':'missing','rebuildRequired':True,
            **refresh_result,
        }

    metrics = volatility_metrics_from_rows(point_rows)
    edge_lookup = {(e.get('ts'), round(float(e.get('price') or 0), 6)) for e in metrics.get('edgeCaseSales') or []}
    for p in point_rows:
        key = (p.get('ts'), round(float(p.get('price') or 0), 6))
        p['edgeCase'] = key in edge_lookup
        if p['edgeCase']:
            p['edgeCaseReason'] = next((e.get('edgeCaseReason') for e in metrics.get('edgeCaseSales') or []
                                        if (e.get('ts'), round(float(e.get('price') or 0), 6)) == key), 'edge case sale')
    return {
        'provider': 'steam',
        'currency': 'USD',
        'name': name,
        'url': steam_market_url(market_hash_name or name),
        'marketHashName': market_hash_name or name,
        'success': bool(rows),
        'source': 'steam_history',
        'points': point_rows,
        'metrics': metrics,
        'edgeCaseSales': metrics.get('edgeCaseSales') or [],
        'edgeCaseCount': metrics.get('edgeCaseCount') or 0,
        'updatedAt': max((int(r['updated_at']) for r in rows), default=now),
        'dataState':'saved','cacheState':'cached','rebuildRequired':False,
        'freshness':{'sourceAsOf':max((int(r['updated_at']) for r in rows),default=None),
                     'currentness':'saved_history','timestampRepairNeeded':any(r['ts'] is None for r in rows)},
        **refresh_result,
    }

def pct(v):
    return round(v * 100, 2)



def parse_int_param(value, default, allowed=None, minimum=None, maximum=None):
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    if allowed and v not in allowed:
        return default
    if minimum is not None:
        v = max(minimum, v)
    if maximum is not None:
        v = min(maximum, v)
    return v


class LegacyBulkCapacityDeferred(RuntimeError):
    pass


def legacy_bulk_capacity_admission():
    """Reserve space before the older reference writer; current cash is independent.

    The last measured normal legacy transaction extended its WAL by about2.01GB.
    A2GiB admission reserve exceeds that observation but is not a universal peak
    guarantee. No writer is killed, rolled back or interrupted by this check.
    """
    floor = 10 * 1024 ** 3
    reserve = 2 * 1024 ** 3
    volume = Path(DB_FILE).resolve().anchor
    try:
        free = shutil.disk_usage(volume).free
    except OSError as error:
        raise LegacyBulkCapacityDeferred('Legacy reference refresh deferred: free space could not be verified; saved reads and current cash remain available') from error
    if free < floor + reserve:
        raise LegacyBulkCapacityDeferred('Legacy reference refresh deferred: requires10GiB free floor plus2GiB write reserve; saved reads and current cash remain available')


def cash_price_capacity():
    """Admission only: application storage retains the reviewed 10 GiB floor."""
    floor = 10 * 1024 ** 3
    for volume in {str(Path(DATA_DIR).resolve())}:
        try:
            free = shutil.disk_usage(volume).free
        except OSError as error:
            raise cash_prices.CashError('Cash acquisition capacity could not be verified') from error
        if free < floor:
            raise cash_prices.CashError('Cash acquisition paused: 10 GiB free-space floor on ' + volume)


def cash_price_transport(exact_hashes):
    # Import only on the worker's explicit invocation; the bridge has no import I/O.
    from skinport_cash_transport import fetch_skinport_cash
    return fetch_skinport_cash(exact_hashes)


def cash_held_rows(portfolio_id, conn=None):
    """Same exact bounded identity policy as Steam; no provider/AskStore access."""
    if conn is None:
        with closing(steam_ask_db()) as opened:
            opened.execute('BEGIN')
            return cash_held_rows(portfolio_id, opened)
    if portfolio_id:
        portfolio = conn.execute('SELECT id FROM portfolios WHERE id=?', (portfolio_id,)).fetchone()
    else:
        portfolio = conn.execute('SELECT id FROM portfolios ORDER BY sort_key DESC,imported_at DESC LIMIT 1').fetchone()
    if not portfolio:
        raise cash_prices.CashNotFound('Selected portfolio does not exist')
    rows = [dict(row) for row in conn.execute('SELECT * FROM portfolio_items WHERE portfolio_id=? ORDER BY sort_key LIMIT 10001', (portfolio['id'],))]
    if len(rows) > steam_held_asks.MAX_HOLDING_ROWS:
        raise ValueError('Portfolio exceeds the 10000-row acquisition bound')
    names = list(dict.fromkeys(row['name'] for row in rows if isinstance(row['name'], str)))
    candidates, native_verified, native_rejected = {}, set(), set()
    for start in range(0, len(names), 256):
        chunk = names[start:start + 256]
        marks = ','.join('?' for _ in chunk)
        for table in ('market_items', 'item_catalog'):
            for meta in conn.execute('SELECT name,market_hash_name,kind FROM ' + table + ' WHERE name IN (' + marks + ')', chunk):
                if meta['market_hash_name']:
                    candidates.setdefault(meta['name'], []).append(dict(meta))
    native_tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN('catalog_items_v2','catalog_identifiers_v2')")}
    if native_tables == {'catalog_items_v2', 'catalog_identifiers_v2'}:
        # Read only indexed exact Steam identifier bindings. Never enroll/repair or infer an ID.
        identities = list(dict.fromkeys(names + [meta['market_hash_name'] for values in candidates.values() for meta in values]))
        for start in range(0, len(identities), 256):
            chunk = identities[start:start + 256]
            owners = {}
            for item in conn.execute('''SELECT x.value,x.item_id,i.entity_type,i.priceable,i.market_hash_name
                FROM catalog_identifiers_v2 x LEFT JOIN catalog_items_v2 i ON i.item_id=x.item_id
                WHERE x.namespace='steam:730:market_hash_name' AND x.value IN(''' + ','.join('?' for _ in chunk) + ')', chunk):
                owners.setdefault(item['value'], []).append(dict(item))
            for value, bindings in owners.items():
                if (len(bindings) == 1 and bindings[0]['entity_type'] == 'listing'
                        and bindings[0]['priceable'] == 1 and bindings[0]['market_hash_name'] == value):
                    native_verified.add(value)
                else:
                    native_rejected.add(value)
    holdings = []
    for row in rows:
        exact = {row['name']} if row['name'] in native_verified else set()
        for meta in candidates.get(row['name'], []):
            value = meta['market_hash_name']
            if value in native_verified:
                exact.add(value)
            elif (value not in native_rejected and meta['kind'] in CATALOG_LEGACY_KINDS
                  and catalog_row_is_market_listing(meta)):
                exact.add(value)
        issue = 'ambiguous_listing_identity' if len(exact) > 1 else 'no_verified_listing_identity' if not exact else None
        holdings.append({'marketHashName': next(iter(exact)) if len(exact) == 1 else None, 'qty': row['qty'],
                         'displayName': row['name'], 'identityIssue': issue,
                         'priorityReferenceUsd': None})
    display_data = portfolio_data_from_rows(rows)
    return 'portfolio:' + portfolio['id'], holdings, [[item[ITEM_IDX['n']], item[ITEM_IDX['q']]] for item in display_data['items']]


def catalog_bind_cash_outcome(conn, name, meaningful_observation_at=None):
    """Bind the already-saved exact cash row; never rewrite it as a generic quote.

    The caller owns the short cash batch transaction and its source revision.
    NULL current rows explicitly mirror unavailable cash outcomes. Historical
    stream ownership stays intact, including the core's pending-link contract.
    """
    if not CATALOG_STORE_ENABLED:
        return None
    if not conn.in_transaction:
        raise RuntimeError('Cash binding requires the existing outcome transaction')
    catalog_writer(conn)  # Enforces the existing independent writer gate.
    provider, source = cash_prices.PROVIDER, cash_prices.SOURCE
    row = conn.execute('SELECT * FROM market_item_prices WHERE name=? AND provider=?',
                       (name, provider)).fetchone()
    if row is None:
        raise ValueError('Cash binding requires its saved exact current row')
    raw = cash_prices._saved_raw(row)
    captured = row['updated_at']
    amount = row['price']
    if (row['source'] != source or row['basis'] != cash_prices.BASIS
            or row['kind'] != 'cash_listing_reference' or row['observed_at'] != captured
            or row['solid_price'] != amount or row['lowest_sell_order'] != amount
            or any(row[key] is not None for key in (
                'median', 'highest_buy_order', 'spread_pct', 'stability_pct',
                'confidence_pct', 'volume', 'url'))
            or raw.get('providerTimeSemantics') != 'upstream_updated_at_not_claimed_trade_time'):
        raise ValueError('Cash binding requires the dedicated cash row contract')
    observation_at = None
    if meaningful_observation_at is not None:
        if type(meaningful_observation_at) is not int or meaningful_observation_at != captured:
            raise ValueError('Cash history uses the exact capture second, never a generic bucket')
        observation = conn.execute('''SELECT name,price,raw,updated_at,observed_at,source
            FROM price_observations WHERE name=? AND provider=? AND observed_at=?''',
            (name, provider, captured)).fetchone()
        if observation is None:
            raise ValueError('Meaningful cash history must exist before its binding')
        observed_raw = cash_prices._saved_raw(observation)
        if (observation['source'] != source or observation['updated_at'] != captured
                or observed_raw['bodySHA256'] != raw['bodySHA256']
                or cash_prices._semantic(observed_raw) != cash_prices._semantic(raw)):
            raise ValueError('Meaningful cash history does not match the saved outcome')
        # A same-second same-body retry may retain its earlier microsecond raw.
        # This is an actual legacy key, not a claim that both raw strings match.
        observation_at = captured
    resolution, bindings = {}, {}
    identity_store, item_id = catalog_identity_for_legacy(
        conn, name, market_hash_name=name, source=source, unresolved_definition=resolution)
    if item_id is None:
        if resolution.get('status') != 'unresolved_definition':
            raise ValueError('Cash identity requires an explicit unresolved result')
        identity_status, status = 'unresolved_definition', 'unresolved'
        reason = resolution.get('reason') or 'unresolved_definition'
    else:
        identity = conn.execute('''SELECT entity_type,priceable,market_hash_name
            FROM catalog_items_v2 WHERE item_id=?''', (item_id,)).fetchone()
        if (identity is None or identity['entity_type'] != 'listing' or identity['priceable'] != 1
                or identity['market_hash_name'] != name):
            raise catalog_store.IdentityConflict('Cash binding must retain the exact selected listing hash')
        for legacy_store in ('market_item_prices', 'price_observations'):
            bindings[legacy_store] = identity_store.bind_legacy_or_defer(
                item_id, legacy_store, name, provider=provider)['status']
        if any(value not in ('bound', 'pending') for value in bindings.values()):
            raise ValueError('Cash binding requires explicit bound or pending stream states')
        identity_status = 'verified_listing'
        status = 'legacy_stream_binding_pending' if 'pending' in bindings.values() else 'resolved'
        reason = 'legacy_reference_requires_verified_decision' if 'pending' in bindings.values() else None
    scope = [provider, source, name]
    key = 'catalog_cash_binding:v1:' + hashlib.sha256(
        json.dumps(scope, ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()
    evidence = {
        'contractVersion': 1, 'status': status, 'reason': reason,
        'identityStatus': identity_status, 'itemId': item_id,
        'marketHashName': name,
        'provider': provider, 'source': source, 'currency': cash_prices.CURRENCY,
        'basis': cash_prices.BASIS, 'cashState': raw['state'], 'cashReason': raw.get('reason'),
        'capturedAt': captured, 'receivedAtUs': raw['receivedAtUs'],
        'providerUpdatedAt': raw.get('providerUpdatedAt'),
        'providerTimeSemantics': raw['providerTimeSemantics'],
        'bodySHA256': raw['bodySHA256'],
        'quoteRawSHA256': hashlib.sha256(row['raw'].encode('utf-8')).hexdigest(),
        'currentStore': 'market_item_prices', 'currentWriteDisposition': 'upserted_current',
        'currentValueDisposition': 'priced' if raw['state'] == 'priced' else 'explicit_unavailable',
        'legacyStreamBindings': bindings,
        'pendingLegacyStreamCount': sum(value == 'pending' for value in bindings.values()),
        'historyStore': 'price_observations', 'meaningfulObservedAt': observation_at,
        'historyDisposition': 'meaningful_outcome_recorded' if observation_at is not None else 'unchanged_outcome_no_history',
        'identitySemantics': 'exact selected hash retained; no definition-to-listing promotion',
    }
    encoded = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if len(encoded.encode('utf-8')) > 4096:
        raise ValueError('Cash binding evidence exceeded its compact 4KiB bound')
    conn.execute('INSERT INTO catalog_store_meta(key,value) VALUES(?,?) '
                 'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, encoded))
    return {'key': key, 'status': status, 'cashState': raw['state'], 'itemId': item_id}


def cash_price_service():
    global CASH_PRICE_SERVICE, CASH_PRICE_HOLDINGS_CACHE
    BULK_STAGING_RUNTIME.require_bulk_mode()
    with CASH_PRICE_SERVICE_LOCK:
        if CASH_PRICE_SERVICE is None:
            # The controller performs an offline exact quote/control handoff.
            # Never create a store, replay DDL or reset a cooldown on a GET.
            path = Path(DATA_DIR) / 'cash-current-v1.sqlite'
            cache = holdings_cache.HoldingsCache(lambda: steam_ask_db(shared_read=True), cash_held_rows)
            def saved_holdings(portfolio_id, ignored=None):
                if portfolio_id is None:
                    return cash_held_rows(None, None)
                try:
                    return cache.read(portfolio_id)
                except holdings_cache.HoldingsCacheError as error:
                    # Preserve the original explicit missing-scope/input response after cleanup.
                    if isinstance(error.__cause__, (cash_prices.CashNotFound, ValueError)):
                        raise error.__cause__
                    raise cash_prices.CashError('Saved holdings identity is temporarily unavailable') from error
            try:
                service = compact_cash_service.CompactCash(
                    path, saved_holdings, cash_price_transport, cash_price_capacity)
            except BaseException:
                cache.close()
                raise
            CASH_PRICE_HOLDINGS_CACHE = cache
            CASH_PRICE_SERVICE = service
        return CASH_PRICE_SERVICE


def cash_holdings_identity_revision():
    cash_price_service()
    try:
        return CASH_PRICE_HOLDINGS_CACHE.identity_revision()
    except holdings_cache.HoldingsCacheError as error:
        raise cash_prices.CashError('Saved holdings identity is temporarily unavailable') from error


CANONICAL_CASH_SERVICE = None
CANONICAL_CASH_SERVICE_LOCK = threading.Lock()


def canonical_cash_service():
    global CANONICAL_CASH_SERVICE
    with CANONICAL_CASH_SERVICE_LOCK:
        if CANONICAL_CASH_SERVICE is None:
            service = cash_price_service()
            CANONICAL_CASH_SERVICE = canonical_cash.CanonicalCash(service.connect, service)
        return CANONICAL_CASH_SERVICE


CASH_PRICE_CADENCE = cash_cadence.CashCadence(cash_price_service)


def latest_portfolio(conn, portfolio_id=None):
    if portfolio_id:
        row = conn.execute(
            "SELECT id AS portfolio_id, label, imported_at, sort_key FROM portfolios WHERE id=?",
            (portfolio_id,),
        ).fetchone()
        if row:
            return dict(row)
    row = conn.execute(
        "SELECT id AS portfolio_id, label, imported_at, sort_key FROM portfolios ORDER BY sort_key DESC, imported_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def portfolio_inventory_rows(conn, portfolio_id, limit):
    return [dict(r) for r in conn.execute("""
        SELECT pi.portfolio_id AS snapshot_id,
               pi.portfolio_id AS portfolio_id,
               pi.sort_key,
               p.label,
               NULL AS snapshot_date,
               NULL AS horizon_days,
               pi.name,
               pi.base_name,
               pi.qty,
               NULL AS pct,
               NULL AS dollar,
               NULL AS curr,
               NULL AS past,
               NULL AS unit_curr,
               NULL AS unit_past,
               pi.category,
               pi.group_name
        FROM portfolio_items pi
        JOIN portfolios p ON p.id = pi.portfolio_id
        WHERE pi.portfolio_id=? ORDER BY COALESCE(pi.qty, 0) DESC, pi.sort_key
        LIMIT ?
    """, (portfolio_id, limit))]


def history_cutoff_ts(days):
    if not days or days <= 0:
        return 0
    return int(time.time()) - int(days) * 86400


def median_value(values):
    values = sorted(float(v) for v in values if v is not None)
    if not values:
        return None
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2


def robust_price_points(points):
    """Flag extreme one-off sales and return points suitable for trend math."""
    if len(points) < 4:
        for p in points:
            p['edgeCase'] = False
            p['edgeCaseReason'] = None
        return points, []
    prices = [p['price'] for p in points]
    med = median_value(prices)
    if not med or med <= 0:
        return points, []
    deviations = [abs(p - med) for p in prices]
    mad = median_value(deviations) or 0
    edge = []
    cleaned = []
    for idx, p in enumerate(points):
        price = p['price']
        reason = None
        if price <= med * 0.25:
            reason = 'extreme low sale'
        elif price >= med * 4:
            reason = 'extreme high sale'
        elif mad > 0 and abs(price - med) > max(med * 0.75, mad * 8):
            reason = 'statistical outlier'
        elif 0 < idx < len(points) - 1 and int(p.get('volume') or 0) <= 2:
            left = points[idx - 1]['price']
            right = points[idx + 1]['price']
            neighbor_mid = (left + right) / 2 if left and right else None
            if neighbor_mid and abs(left - right) / neighbor_mid <= 0.25:
                neighbor_deviation = abs(price - neighbor_mid) / neighbor_mid
                if neighbor_deviation >= 0.45:
                    reason = 'low volume isolated sale'
        p['edgeCase'] = bool(reason)
        p['edgeCaseReason'] = reason
        if reason:
            edge.append({**p, 'medianReference': round(med, 4)})
        else:
            cleaned.append(p)
    if len(cleaned) < 2:
        cleaned = points
        edge = []
        for p in points:
            p['edgeCase'] = False
            p['edgeCaseReason'] = None
    return cleaned, edge


def trim_edge_anchor_outliers(points):
    if len(points or []) < 4:
        return points or []
    cleaned = list(points)
    changed = True
    while changed and len(cleaned) >= 4:
        changed = False
        head = _positive_float(cleaned[0].get('price'))
        tail = _positive_float(cleaned[-1].get('price'))
        next_anchor = median_value([p.get('price') for p in cleaned[1:4]])
        prev_anchor = median_value([p.get('price') for p in cleaned[-4:-1]])
        if head and next_anchor and (head / next_anchor <= 0.35 or head / next_anchor >= 2.85):
            cleaned.pop(0)
            changed = True
            continue
        if tail and prev_anchor and (tail / prev_anchor <= 0.35 or tail / prev_anchor >= 2.85):
            cleaned.pop()
            changed = True
    return cleaned


def volatility_metrics_from_rows(rows):
    pts = []
    for r in rows:
        price = r.get('price') if isinstance(r, dict) else r['price']
        ts = r.get('point_ts', r.get('ts')) if isinstance(r, dict) else (r['point_ts'] if 'point_ts' in r.keys() else r['ts'])
        vol = r.get('volume') if isinstance(r, dict) else r['volume']
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        pts.append({'ts': int(ts or 0), 'price': price, 'volume': int(vol or 0)})
    pts.sort(key=lambda x: x['ts'])
    clean_pts, edge_cases = robust_price_points(pts)
    clean_pts = trim_edge_anchor_outliers(clean_pts)
    if len(pts) < 2:
        return {
            'points': len(pts),
            'cleanPoints': len(clean_pts),
            'edgeCaseSales': edge_cases,
            'edgeCaseCount': len(edge_cases),
            'volatilityPct': None,
            'rangePct': None,
            'trendPct': None,
            'avgVolume': None,
            'anchorPrice': recent_series_anchor(clean_pts),
            'firstPrice': pts[0]['price'] if pts else None,
            'latestPrice': pts[-1]['price'] if pts else None,
            'firstTs': pts[0]['ts'] if pts else None,
            'lastTs': pts[-1]['ts'] if pts else None,
        }
    prices = [p['price'] for p in clean_pts]
    returns = []
    for a, b in zip(prices, prices[1:]):
        if a > 0:
            returns.append((b / a) - 1)
    weights = []
    for a, b in zip(clean_pts, clean_pts[1:]):
        av = int(a.get('volume') or 0)
        bv = int(b.get('volume') or 0)
        if av <= 0 and bv <= 0:
            weights.append(1.0)
        else:
            weights.append(max(0.2, min(1.0, math.sqrt(max(1, min(av or bv, bv or av)) / 8))))
    if len(returns) > 1:
        weight_total = sum(weights) or len(returns)
        mean = sum(r * w for r, w in zip(returns, weights)) / weight_total
        variance = sum(w * ((r - mean) ** 2) for r, w in zip(returns, weights)) / weight_total
        vol_pct = round(math.sqrt(variance) * 100, 2)
    else:
        vol_pct = 0
    first = prices[0]
    latest = prices[-1]
    avg_vol = round(sum(p['volume'] for p in pts) / len(pts), 1) if pts else None
    return {
        'points': len(pts),
        'cleanPoints': len(clean_pts),
        'edgeCaseSales': edge_cases[:12],
        'edgeCaseCount': len(edge_cases),
        'volatilityPct': vol_pct,
        'rangePct': round(((max(prices) - min(prices)) / first) * 100, 2) if first else None,
        'trendPct': round(((latest / first) - 1) * 100, 2) if first else None,
        'avgVolume': avg_vol,
        'anchorPrice': round(recent_series_anchor(clean_pts) or latest, 2),
        'firstPrice': round(first, 2),
        'latestPrice': round(latest, 2),
        'firstTs': clean_pts[0]['ts'],
        'lastTs': clean_pts[-1]['ts'],
    }


def store_item_market_metric(conn, name, window_days, metrics, current_price=None, prior_price=None, price_source=None):
    if not name or not metrics:
        return
    conn.execute(
        """INSERT OR REPLACE INTO item_market_metrics
           (name, provider, window_days, points, clean_points, edge_case_count,
            volatility_pct, range_pct, trend_pct, first_price, latest_price,
            first_ts, last_ts, current_price, prior_price, price_source, updated_at, raw)
           VALUES (?, 'market', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            name,
            int(window_days or 0),
            metrics.get('points'),
            metrics.get('cleanPoints'),
            metrics.get('edgeCaseCount'),
            metrics.get('volatilityPct'),
            metrics.get('rangePct'),
            metrics.get('trendPct'),
            metrics.get('firstPrice'),
            metrics.get('latestPrice'),
            metrics.get('firstTs'),
            metrics.get('lastTs'),
            current_price if current_price is not None else metrics.get('latestPrice'),
            prior_price,
            price_source or 'history',
            int(time.time()),
            json.dumps({
                'edgeCaseSales': metrics.get('edgeCaseSales') or [],
                'avgVolume': metrics.get('avgVolume'),
                'spanSeconds': metrics.get('spanSeconds'),
                'spanDays': metrics.get('spanDays'),
                'windowCoveragePct': metrics.get('windowCoveragePct'),
                'isPartialWindow': bool(metrics.get('isPartialWindow')),
                'metricBasis': metrics.get('metricBasis') or price_source,
                'metricBuildVersion': METRIC_BUILD_VERSION,
                'metricState': metrics.get('metricState') or ('partial' if metrics.get('isPartialWindow') else 'real'),
                'hasPrior': prior_price is not None,
                'confidencePct': metrics.get('confidencePct'),
            }, separators=(',', ':')),
        ),
    )


def metric_window_min_span(window_days):
    if not window_days:
        return 0
    return max(6 * 3600, int(window_days * 86400 * 0.4))


def metric_source_confidence(metric_basis, span, window_days, points):
    if metric_basis == 'steam_history':
        return 95 if not window_days or span >= metric_window_min_span(window_days) else 78
    if metric_basis == 'csgotrader_window_anchor':
        return 72
    if metric_basis == 'provider_observations':
        coverage = 100 if not window_days else min(100, (span / max(1, window_days * 86400)) * 100)
        return round(max(25, min(68, coverage * 0.8 + min(points, 12) * 1.5)), 1)
    return 0


def csgotrader_window_anchor_rows(conn, name, window_days, current_price=None, current_ts=None):
    if int(window_days or 0) not in VOLATILITY_DETAIL_WINDOWS:
        return []
    anchor = conn.execute(
        """SELECT observed_at AS point_ts, price, volume, source, provider
           FROM price_observations
           WHERE name=? AND provider='steam_snapshot' AND source=?
             AND price IS NOT NULL
           ORDER BY observed_at DESC
           LIMIT 1""",
        (name, f'csgotrader:steam:last_{int(window_days)}d'),
    ).fetchone()
    if not anchor:
        return []
    latest = conn.execute(
        """SELECT updated_at AS point_ts, price, volume, source, provider
           FROM market_item_prices
           WHERE name=? AND provider='steam_snapshot' AND price IS NOT NULL
           ORDER BY updated_at DESC
           LIMIT 1""",
        (name,),
    ).fetchone()
    if latest:
        latest_row = dict(latest)
    elif current_price:
        latest_row = {
            'point_ts': int(current_ts or time.time()),
            'price': current_price,
            'volume': 0,
            'source': 'current_provider_price',
            'provider': 'steam_snapshot',
        }
    else:
        return []
    anchor_row = dict(anchor)
    if int(anchor_row.get('point_ts') or 0) >= int(latest_row.get('point_ts') or 0):
        anchor_row['point_ts'] = int(latest_row.get('point_ts') or time.time()) - int(window_days) * 86400
    return [anchor_row, latest_row]


def rebuild_market_metrics(conn, window_days=90, names=None):
    """Compatibility entry point for one complete or explicitly scoped window."""
    window_days = parse_int_param(window_days, 90, allowed=VOLATILITY_WINDOWS)
    return rebuild_market_metric_windows(conn, (window_days,), names=names)[window_days]


def rebuild_market_metric_windows(conn, windows=VOLATILITY_DETAIL_WINDOWS, names=None, as_of=None):
    """Read each name's overlapping inputs once; keep window statistics separate.

    The caller owns the transaction. No commit, history truncation or global
    history cache is introduced. A common cutoff clock defines this rebuild.
    """
    windows = tuple(dict.fromkeys(parse_int_param(w, 90, allowed=VOLATILITY_WINDOWS) for w in windows))
    if not windows:
        return {}
    if preserve_recovered_research_without_inputs(conn):
        return {window: conn.execute(
            "SELECT COUNT(*) FROM item_market_metrics WHERE provider='market' AND window_days=?",
            (window,)).fetchone()[0] for window in windows}
    flush_market_source_revision(conn)
    as_of = int(time.time()) if as_of is None else int(as_of)
    cutoffs = {w: as_of - w * 86400 if w else 0 for w in windows}
    bounds = {w: max(0, cutoff - OBSERVATION_BUCKET_SECONDS) if cutoff else None
              for w, cutoff in cutoffs.items()}
    clean_names = [(n or '').strip() for n in (names or []) if (n or '').strip()]
    name_filter = ''
    params = list(CURRENT_PRICE_PROVIDERS)
    if clean_names:
        marks = ','.join('?' for _ in clean_names)
        name_filter = f" AND name IN ({marks})"
        params.extend(clean_names)
    for window in windows:
        conn.execute("DELETE FROM item_market_metrics WHERE provider='market' AND window_days=?" + name_filter,
                     [window] + clean_names)
    provider_marks = ','.join('?' for _ in CURRENT_PRICE_PROVIDERS)
    by_name = {}
    for result in conn.execute(
        f"""SELECT name, provider, price, solid_price, updated_at, source
            FROM market_item_prices
            WHERE provider IN ({provider_marks}) AND price IS NOT NULL{name_filter}
            ORDER BY name, updated_at""", params):
        row = dict(result)
        bucket = by_name.setdefault(row['name'], {})
        existing = bucket.get(row['provider'])
        if existing is None or int(row.get('updated_at') or 0) >= int(existing.get('updated_at') or 0):
            bucket[row['provider']] = row

    def read_series(name, selected_windows, provider=None):
        # Eligibility stays in SQLite: do not replace affinity/NULL semantics
        # with Python numeric comparisons on legacy timestamp values.
        timestamp = 'point_ts' if provider is None else 'observed_at'
        flags, flag_params = [], []
        for index, window in enumerate(selected_windows):
            bound = bounds[window]
            if bound is None:
                flags.append(f'1 AS eligible_{index}')
            else:
                expression = timestamp if bound > 0 else f'COALESCE({timestamp},0)'
                flags.append(f'({expression} >= ?) AS eligible_{index}')
                flag_params.append(bound)
        widest = None if any(bounds[w] is None for w in selected_windows) else min(bounds[w] for w in selected_windows)
        where = ''
        tail_params = [name] if provider is None else [name, provider]
        if widest is not None:
            expression = timestamp if widest > 0 else f'COALESCE({timestamp},0)'
            where = f' AND {expression} >= ?'
            tail_params.append(widest)
        if provider is None:
            sql = f"""SELECT name, point_ts, price, volume, 'steam_history' AS source,
                       'steam_history' AS provider, {', '.join(flags)}
                       FROM price_history WHERE provider='steam' AND name=? AND price IS NOT NULL {where}
                       ORDER BY point_ts"""
        else:
            sql = f"""SELECT name, observed_at AS point_ts, price, volume,
                       COALESCE(source, provider) AS source, provider, {', '.join(flags)}
                       FROM price_observations WHERE name=? AND provider=? AND price IS NOT NULL {where}
                         AND COALESCE(source, '') NOT LIKE 'csgotrader:steam:last_%'
                       ORDER BY observed_at"""
        return [dict(row) for row in conn.execute(sql, flag_params + tail_params)]

    def window_rows(rows, index):
        return [row for row in rows if row[f'eligible_{index}']]

    rebuilt = {window: 0 for window in windows}
    for name, provider_rows in by_name.items():
        primary = next((provider_rows[p] for p in PRIMARY_PRICE_PROVIDER_ORDER if p in provider_rows), None)
        if not primary:
            continue
        steam_rows = read_series(name, windows)
        fallback_windows = []
        for index, window in enumerate(windows):
            rows = window_rows(steam_rows, index)
            if rows:
                rebuilt[window] += store_market_metric_window_rows(conn, name, window, primary, rows, 'steam_history')
                continue
            # Keep the exact source-specific anchor seek and its no-cutoff rule.
            # The helper's current quote lookup is small and preserves its full
            # compatibility behavior; only the large raw ranges are shared.
            steam_current = provider_rows.get('steam_snapshot') or {}
            current_ts = int(primary.get('updated_at') or time.time())
            anchors = csgotrader_window_anchor_rows(
                conn, name, window,
                current_price=_positive_float(steam_current.get('solid_price')) or _positive_float(steam_current.get('price')),
                current_ts=int(steam_current.get('updated_at') or current_ts),
            )
            if anchors:
                rebuilt[window] += store_market_metric_window_rows(conn, name, window, primary, anchors, 'csgotrader_window_anchor')
            else:
                fallback_windows.append(window)
        # Drop the largest series before loading the alternative source.
        del steam_rows
        rows = None
        if fallback_windows:
            observations = read_series(name, fallback_windows, provider=primary.get('provider'))
            for index, window in enumerate(fallback_windows):
                rows = window_rows(observations, index)
                source = 'provider_observations' if rows else 'insufficient'
                rebuilt[window] += store_market_metric_window_rows(conn, name, window, primary, rows, source)
            del observations
            rows = None
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('metric_build_version',?)", (str(METRIC_BUILD_VERSION),))
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('lookup_build_version',?)", (str(LOOKUP_BUILD_VERSION),))
    revision = (conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone() or {'value': None})['value']
    for window in windows:
        save_market_read_receipt(conn, 'market_metric_receipt:'+str(window), {
            'buildVersion': METRIC_BUILD_VERSION, 'builtAt': int(time.time()),
            'complete': not bool(clean_names), 'rows': rebuilt[window] if not clean_names else None,
            'sourceRevision': revision,
        })
    return rebuilt


def store_market_metric_window_rows(conn, name, window_days, primary, metric_rows, metric_source):
    """Use the existing complete-window statistics after selecting its exact input."""
    provider = primary.get('provider')
    current_price = _positive_float(primary.get('solid_price')) or _positive_float(primary.get('price'))
    current_ts = int(primary.get('updated_at') or time.time())
    if current_price and metric_source in ('steam_history', 'provider_observations'):
        metric_rows = [r for r in metric_rows if int(r.get('point_ts') or 0) != current_ts]
        metric_rows.append({
            'name': name,
            'point_ts': current_ts,
            'price': current_price,
            'volume': 0,
            'source': primary.get('source') or provider,
            'provider': provider,
        })
    metrics = volatility_metrics_from_rows(metric_rows)
    if metrics.get('points'):
        span = int(metrics.get('lastTs') or current_ts) - int(metrics.get('firstTs') or current_ts)
        min_span = metric_window_min_span(window_days)
        prior = sane_prior_price(current_price, metrics.get('firstPrice')) if int(metrics.get('cleanPoints') or metrics.get('points') or 0) >= 2 and span >= min_span else None
        if metric_source == 'csgotrader_window_anchor':
            prior = sane_prior_price(current_price, metrics.get('firstPrice'))
            if prior is None:
                # The CSGO Trader window anchor is only a two-point hint.
                # If that prior cannot pass the same sanity guard used for
                # returns, do not let it appear as a volatility headline.
                metrics['volatilityPct'] = None
                metrics['rangePct'] = None
                metrics['trendPct'] = None
                metrics['metricState'] = 'insufficient'
                metric_source = 'insufficient'
            else:
                movement_pct = _finite_float(metrics.get('trendPct'))
                range_pct = _finite_float(metrics.get('rangePct'))
                if movement_pct is not None or range_pct is not None:
                    metrics['volatilityPct'] = round(abs(movement_pct if movement_pct is not None else range_pct), 2)
        coverage_pct = 100 if not window_days else round(min(100, (span / max(1, window_days * 86400)) * 100), 1)
        metrics['spanSeconds'] = span
        metrics['spanDays'] = round(span / 86400, 2) if span else 0
        metrics['windowCoveragePct'] = coverage_pct
        metrics['isPartialWindow'] = bool(window_days and span < min_span and metric_source != 'csgotrader_window_anchor')
        metrics['metricBasis'] = metric_source
        metrics['metricState'] = metrics.get('metricState') or ('estimated' if metric_source == 'csgotrader_window_anchor' else ('partial' if metrics['isPartialWindow'] else 'real'))
        metrics['confidencePct'] = metric_source_confidence(metric_source, span, window_days, int(metrics.get('cleanPoints') or metrics.get('points') or 0))
        store_item_market_metric(
            conn,
            name,
            window_days,
            metrics,
            current_price=current_price,
            prior_price=prior,
            price_source=metric_source,
        )
        return 1
    return 0



def begin_history_refresh_write(conn, state):
    """Track only a continuous sequence of explicit history/quote writes."""
    if state is None:
        return
    revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
    revision = revision['value'] if revision else None
    if 'expectedRevision' not in state:
        state['completeWindows'] = {
            window: metric_window_is_current(conn, window) for window in RESEARCH_METRIC_WINDOWS
        }
    elif state['expectedRevision'] != revision:
        # Another writer changed inputs that this bounded batch cannot repair.
        state['completeWindows'] = {}
    state['expectedRevision'] = revision


def finish_history_refresh_write(conn, state):
    if state is None:
        return
    flush_market_source_revision(conn)
    revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
    state['expectedRevision'] = revision['value'] if revision else None


def rebuild_metrics_for_names(names, windows=RESEARCH_METRIC_WINDOWS, refresh_state=None):
    """Finalize affected metrics and their cached lookup after an explicit batch.

    Raw history work stays bounded to names. The lookup projection/summary is
    rebuilt once per window from cached metrics, never from an ordinary GET.
    """
    clean = [n for n in dict.fromkeys(str(n or '').strip() for n in names or []) if n]
    if not clean:
        return 0
    with closing(steam_ask_db()) as stage_probe:
        if bulk_staging_adopted(stage_probe):
            BULK_STAGING_RUNTIME.request()
            return 0
    rebuilt = 0
    with SQLITE_WRITE_LOCK:
        with closing(db()) as conn, conn:
            conn.execute('BEGIN IMMEDIATE')
            begin_history_refresh_write(conn, refresh_state)
            windows = tuple(windows or RESEARCH_METRIC_WINDOWS)
            counts = rebuild_market_metric_windows(conn, windows, names=clean)
            rebuilt += sum(counts.values())
            for window in windows:
                if refresh_state and refresh_state.get('completeWindows', {}).get(window):
                    receipt = market_read_receipt(conn, 'market_metric_receipt:'+str(window))
                    receipt['complete'] = True
                    receipt['rows'] = conn.execute(
                        "SELECT COUNT(*) FROM item_market_metrics WHERE provider='market' AND window_days=?",
                        (window,),
                    ).fetchone()[0]
                    save_market_read_receipt(conn, 'market_metric_receipt:'+str(window), receipt)
                rebuild_market_item_lookup(conn, window)
            conn.commit()
    return rebuilt


def normalized_history_series(rows):
    pts = []
    for r in rows or []:
        if isinstance(r, dict):
            price = r.get('price')
            ts = r.get('point_ts', r.get('ts'))
            vol = r.get('volume')
            source = r.get('source') or r.get('provider') or 'market_observation'
        else:
            price = r['price']
            ts = r['point_ts'] if 'point_ts' in r.keys() else r['ts']
            vol = r['volume']
            source = r['source'] if 'source' in r.keys() else (r['provider'] if 'provider' in r.keys() else 'market_observation')
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        ts = int(ts or 0)
        if ts <= 0 or price <= 0:
            continue
        pts.append({
            'ts': ts,
            'price': price,
            'volume': int(vol or 0),
            'source': source,
        })
    def point_priority(point):
        key = provider_key_from_price_point(point)
        source = str((point or {}).get('source') or '').lower()
        provider = str((point or {}).get('provider') or '').lower()
        aliases = [provider, key, source]
        for idx, preferred in enumerate(PRIMARY_PRICE_PROVIDER_ORDER):
            if preferred in aliases:
                return 100 - idx
        if 'steam_history' in aliases:
            return 10
        if 'steam' in aliases:
            return 5
        return 0

    by_ts = {}
    for p in pts:
        existing = by_ts.get(p['ts'])
        if existing is None or point_priority(p) > point_priority(existing):
            by_ts[p['ts']] = p
    series = sorted(by_ts.values(), key=lambda x: x['ts'])
    if not series:
        return []
    clean, _edge_cases = robust_price_points([dict(p) for p in series])
    return clean or series


def sane_prior_price(latest_price, prior_price):
    latest_price = _positive_float(latest_price)
    prior_price = _positive_float(prior_price)
    if latest_price is None or prior_price is None:
        return prior_price
    ratio = latest_price / prior_price if prior_price > 0 else None
    if ratio is None:
        return None
    if ratio >= 20 or ratio <= 0.05:
        return None
    if prior_price < 1 and latest_price >= 25:
        return None
    if latest_price >= 20 and ratio >= 8:
        return None
    return prior_price


def provider_key_from_price_point(point):
    source = str((point or {}).get('source') or '').lower()
    provider = str((point or {}).get('provider') or '').lower()
    key = provider or source
    if key.startswith('csgotrader:'):
        key = key.split(':', 1)[1]
    if key == 'steam':
        source_lower = source.lower()
        if source_lower == 'steam_history':
            return 'steam_history'
        return 'steam_snapshot'
    return key


def recent_series_anchor(series, window=3):
    prices = []
    for point in reversed(series or []):
        price = _positive_float((point or {}).get('price'))
        if not price:
            continue
        prices.append(price)
        if len(prices) >= max(1, int(window or 1)):
            break
    if not prices:
        return None
    return median_value(prices)


def reconcile_market_snapshot_price(latest_price, source=None, series=None, history_latest=None):
    latest_price = _positive_float(latest_price)
    history_latest = _positive_float(history_latest)
    source = str(source or '')
    anchor = _positive_float(recent_series_anchor(series)) or history_latest
    if latest_price is None:
        return anchor, False
    if anchor is None or not source.startswith('csgotrader:steam'):
        return latest_price, False
    ratio = latest_price / anchor if anchor > 0 else None
    if ratio is None:
        return latest_price, False
    if latest_price < 2 and anchor >= 5:
        return latest_price, True
    # CSGO Trader's Steam snapshot can occasionally collapse to a thin/odd sale
    # while the recent band is elsewhere. Keep the provider quote as valuation
    # so displayed provider prices and valuation never silently diverge; flag
    # the disagreement for the UI and confidence model instead.
    if ratio <= 0.55 or ratio >= 1.85:
        return latest_price, True
    return latest_price, False


def get_volatility_map(names, history_days=90):
    """Use local price observations/history to compute volatility without new API calls."""
    clean = sorted({(n or '').strip() for n in names if n})
    if not clean:
        return {}
    cutoff = history_cutoff_ts(history_days)
    out = {}
    with db() as conn:
        for i in range(0, len(clean), 400):
            chunk = clean[i:i+400]
            q = ','.join('?' for _ in chunk)
            params = list(chunk)
            where_ts = ''
            if cutoff:
                where_ts = ' AND COALESCE(point_ts,0) >= ?'
                params.append(cutoff)
            rows = conn.execute(f"""
                SELECT name, point_ts, price, volume
                FROM price_history
                WHERE provider='steam' AND name IN ({q}) {where_ts}
                ORDER BY name, point_ts
            """, params).fetchall()
            grouped = {}
            for r in rows:
                grouped.setdefault(r['name'], []).append(r)
            obs_params = list(chunk)
            obs_where = ''
            if cutoff:
                obs_where = ' AND COALESCE(observed_at,0) >= ?'
                obs_params.append(max(0, cutoff - OBSERVATION_BUCKET_SECONDS))
            obs_rows = conn.execute(f"""
                SELECT name, observed_at AS point_ts, price, volume, COALESCE(source, provider) AS source, provider
                FROM price_observations
                WHERE name IN ({q}) AND price IS NOT NULL {obs_where}
                  AND COALESCE(source, '') NOT LIKE 'csgotrader:steam:last_%'
                ORDER BY name, observed_at
            """, obs_params).fetchall()
            for r in obs_rows:
                grouped.setdefault(r['name'], []).append(r)
            for name, rs in grouped.items():
                metrics = volatility_metrics_from_rows(rs)
                out[name] = metrics
    return out


def metric_row_to_volatility(row):
    if not row:
        return {}
    row = dict(row)
    latest_price = _positive_float(row.get('current_price')) or _positive_float(row.get('latest_price'))
    prior_price = _positive_float(row.get('prior_price'))
    raw = {}
    try:
        raw = json.loads(row.get('raw') or '{}')
    except (TypeError, json.JSONDecodeError):
        raw = {}
    return {
        'points': int(row.get('points') or 0),
        'cleanPoints': int(row.get('clean_points') or 0),
        'edgeCaseCount': int(row.get('edge_case_count') or 0),
        'volatilityPct': row.get('volatility_pct'),
        'rangePct': row.get('range_pct'),
        'trendPct': row.get('trend_pct'),
        'avgVolume': raw.get('avgVolume'),
        'anchorPrice': latest_price,
        'firstPrice': _positive_float(row.get('first_price')),
        'priorPrice': prior_price,
        'latestPrice': latest_price,
        'firstTs': row.get('first_ts'),
        'lastTs': row.get('last_ts'),
        'metricWindowDays': row.get('window_days'),
        'metricSource': 'item_market_metrics',
        'metricBasis': raw.get('metricBasis') or row.get('price_source'),
        'metricState': raw.get('metricState') or ('partial' if raw.get('isPartialWindow') else 'real'),
        'confidencePct': raw.get('confidencePct'),
        'metricBuildVersion': raw.get('metricBuildVersion'),
        'spanDays': raw.get('spanDays'),
        'windowCoveragePct': raw.get('windowCoveragePct'),
        'isPartialWindow': bool(raw.get('isPartialWindow')),
        'hasPrior': bool(raw.get('hasPrior')) if 'hasPrior' in raw else prior_price is not None,
    }


def market_read_receipt(conn, key):
    row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
    try:
        value = json.loads(row['value']) if row else None
        return value if isinstance(value, dict) else None
    except (ValueError, TypeError):
        return None


def save_market_read_receipt(conn, key, value):
    """Called by explicit ingestion/rebuild transactions, never by a GET."""
    conn.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)',
                 (key,json.dumps(value,separators=(',',':'),allow_nan=False)))


def mark_market_source_changed(conn):
    if isinstance(conn,MarketWriteConnection):
        conn._market_dirty = True
        return
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('market_source_revision',?)",
                 (str(time.time_ns()),))
    if not getattr(conn, '_bulk_sidecar', False) and bulk_staging_adopted(conn):
        bulk_derived_staging.invalidate_inputs(conn, 'legacy_plain_source_or_metadata')
        BULK_STAGING_RUNTIME.request()


def flush_market_source_revision(conn):
    if isinstance(conn,MarketWriteConnection):
        conn.flush_market_revision()


def mark_catalog_source_changed(conn):
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('catalog_metadata_revision',?)",(str(time.time_ns()),))
    mark_market_source_changed(conn)


def market_cache_state(conn, window_days):
    receipt = market_read_receipt(conn,'market_lookup_receipt:'+str(window_days))
    row = conn.execute('SELECT value FROM settings WHERE key=?',('market_source_revision',)).fetchone()
    revision = row['value'] if row else None
    available = bool(conn.execute('SELECT 1 FROM market_item_lookup WHERE window_days=? LIMIT 1',(window_days,)).fetchone())
    prepared = bool(receipt and receipt.get('lookupBuildVersion')==LOOKUP_BUILD_VERSION and
                    receipt.get('metricBuildVersion')==METRIC_BUILD_VERSION and receipt.get('metricsVerified'))
    current = prepared and revision is not None and receipt.get('sourceRevision')==revision and not getattr(conn,'_market_dirty',False)
    generation_status = getattr(conn, '_derived_generation_status', None)
    if generation_status and generation_status != 'legacy_canonical':
        current = current and generation_status == 'current_generation'
    elif bulk_staging_enabled(conn) and not getattr(conn, '_bulk_sidecar', False):
        current = current and bulk_derived_staging.published_state(conn, BULK_STAGING_HOST.engine.external)['current']
    recovered = recovered_research_status(conn, window_days) if available and not current else None
    return {'cacheState':'missing' if not available else ('current' if current else 'recovered_historical' if recovered else 'stale'),
            'dataState':'saved','rebuildRequired':not current,
            'freshness':{'builtAt':receipt.get('builtAt') if receipt else None,
                         'sourceAsOf':receipt.get('sourceAsOf') if receipt else recovered.get('sourceAsOf') if recovered else None,
                         'currentness':'matched_writer_revision' if current else 'recovered_historical_snapshot' if recovered else 'unverified_saved_cache'},
            'receiptAvailable':bool(receipt),
            **({'recovery':recovered} if recovered else {})}


def recovered_research_status(conn, window_days):
    """Small provenance read; recovery time never becomes a quote timestamp."""
    try:
        import research_recovery
        receipt = research_recovery.recovery_status(conn)
    except (ImportError, sqlite3.Error, ValueError, TypeError):
        return None
    if not receipt:
        return None
    window = next((row for row in receipt.get('windows', []) if row.get('days') == window_days), None)
    if not window:
        return None
    as_of = receipt.get('asOf') or {}
    clocks = [value for key in ('quotesObservedAt', 'quotesUpdatedAt', 'metricsHistoryLastAt', 'metricsUpdatedAt')
              for value in as_of.get(key, []) if isinstance(value, (int, float)) and value > 0]
    return {'state':'recovered_historical','windowDays':window_days,'lookupRowsAtRecovery':window.get('rows'),
            'sourceAsOf':max(clocks) if clocks else None,'originalAsOf':as_of,
            'sourceFingerprint':receipt.get('sourceFingerprint'),'recoveredAt':receipt.get('appliedAt'),
            'rawSteamHistoryRestored':False,'rawSteamChartRowsImported':0,'currentQuotes':False,
            'meaning':'Preserved dated provider snapshots and aggregate metrics; not fresh cash prices or raw Steam chart observations.'}


def preserve_recovered_research_without_inputs(conn):
    """A recovered aggregate is evidence, not a substitute raw input series."""
    try:
        import research_recovery
        if not research_recovery.recovery_status(conn):
            return False
    except (ImportError, sqlite3.Error, ValueError, TypeError):
        return False
    raw = conn.execute("SELECT 1 FROM price_history WHERE provider='steam' AND price>0 LIMIT 1").fetchone()
    observed = conn.execute('SELECT 1 FROM price_observations WHERE price>0 LIMIT 1').fetchone()
    return not raw and not observed


def metric_window_is_current(conn, window_days):
    receipt = market_read_receipt(conn,'market_metric_receipt:'+str(window_days))
    revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
    return bool(receipt and revision and receipt.get('complete') and receipt.get('buildVersion')==METRIC_BUILD_VERSION and
                receipt.get('sourceRevision')==revision['value'] and not getattr(conn,'_market_dirty',False))


def ensure_metric_window_current(conn, window_days):
    """Compatibility probe; reads must never schedule or perform a rebuild."""
    return False


def get_cached_market_metric_map(conn, names, history_days=90):
    """Read persisted item movement metrics instead of recomputing raw history.

    This is the hot path for page refreshes. Raw observations and Steam chart
    rows are append-only history stores; `item_market_metrics` is the local,
    precomputed view the UI should use immediately.
    """
    clean = [n for n in dict.fromkeys((n or '').strip() for n in names or []) if n]
    if not clean:
        return {}, parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS), False
    requested_window = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    metric_window = requested_window
    exact_window = metric_window_is_current(conn, metric_window)
    version = conn.execute("SELECT value FROM settings WHERE key='metric_build_version'").fetchone()
    if not exact_window or not version or str(version['value']) != str(METRIC_BUILD_VERSION):
        ensure_metric_window_current(conn, metric_window)
        exact_window = metric_window_is_current(conn, metric_window)
    out = {}
    for i in range(0, len(clean), 400):
        chunk = clean[i:i+400]
        q = ','.join('?' for _ in chunk)
        rows = conn.execute(
            f"""SELECT *
                FROM item_market_metrics
                WHERE provider='market' AND window_days=? AND name IN ({q})""",
            [metric_window] + chunk,
        ).fetchall()
        for row in rows:
            out[row['name']] = metric_row_to_volatility(row)
    return out, metric_window, exact_window


def _positive_float(value):
    try:
        value = float(value)
        return value if value > 0 else None
    except (TypeError, ValueError):
        return None


def _finite_float(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def history_quote_from_points(name, market_hash_name, points, updated_at=None):
    clean_points = []
    for p in points or []:
        price = _positive_float(p.get('price') if isinstance(p, dict) else p['price'])
        if not price:
            continue
        ts = p.get('ts') if isinstance(p, dict) else p['point_ts']
        try:
            ts = int(ts or 0)
        except (TypeError, ValueError):
            ts = 0
        volume = p.get('volume') if isinstance(p, dict) else p['volume']
        clean_points.append({'price': price, 'ts': ts, 'volume': volume})
    if not clean_points:
        return None
    clean_points.sort(key=lambda p: p['ts'])
    trend_points, edge_cases = robust_price_points(clean_points)
    latest = trend_points[-1] if trend_points else clean_points[-1]
    return {
        'name': name,
        'provider': 'steam',
        'price': latest['price'],
        'median': latest['price'],
        'volume': int(latest.get('volume') or 0) or None,
        'url': steam_market_url(market_hash_name or name),
        'raw': json.dumps({
            'source': 'steam_history',
            'latest_ts': latest['ts'],
            'edge_case_count': len(edge_cases),
            'edge_cases_excluded': bool(edge_cases),
        }, separators=(',', ':')),
        'updated_at': int(updated_at or time.time()),
        'source': 'steam_history',
    }


def save_history_quote(name, market_hash_name, points, updated_at=None, _refresh_state=None):
    quote = history_quote_from_points(name, market_hash_name, points, updated_at)
    if not quote:
        return None
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.execute('BEGIN IMMEDIATE')
            begin_history_refresh_write(conn, _refresh_state)
            for quote_name in dict.fromkeys([name, market_hash_name or name]):
                if not quote_name:
                    continue
                row = {**quote, 'name': quote_name}
                upsert_market_item_price(conn, row, provider='steam_history', source='steam_history')
                save_price_observation(conn, row, source='steam_history', observed_at=updated_at, provider='steam_history')
            mark_market_source_changed(conn)
            finish_history_refresh_write(conn, _refresh_state)
    return quote


def attach_market_valuation(item, quote, volatility):
    qty = _positive_float(item.get('qty')) or 0
    quote_unit = _positive_float((quote or {}).get('price')) or _positive_float((quote or {}).get('median'))
    history_latest = _positive_float((volatility or {}).get('latestPrice'))
    history_anchor = _positive_float((volatility or {}).get('anchorPrice')) or history_latest
    quote_source = (quote or {}).get('source')
    market_unit, guarded_snapshot = reconcile_market_snapshot_price(quote_unit, quote_source, history_latest=history_anchor)
    if quote_source == 'steam_history':
        market_source = 'steam_history'
    elif quote_source and str(quote_source).startswith('csgotrader:'):
        market_source = 'market_snapshot'
    elif quote_unit:
        market_source = 'steam_quote'
    elif history_latest:
        market_source = 'steam_history'
    else:
        market_source = 'missing'
    history_first = _positive_float((volatility or {}).get('firstPrice'))
    prior_unit = sane_prior_price(market_unit, history_first)
    curr_total = market_unit * qty if market_unit and qty else None
    past_total = prior_unit * qty if prior_unit and qty else None

    item['marketCurr'] = round(curr_total, 2) if curr_total is not None else None
    item['marketPast'] = round(past_total, 2) if past_total is not None else None
    item['marketUnitPrice'] = round(market_unit, 2) if market_unit else None
    item['marketPriorUnitPrice'] = round(prior_unit, 2) if prior_unit else None
    item['marketPriceSource'] = market_source
    item['marketQuoteProvider'] = (quote or {}).get('provider') or quote_source or market_source
    item['lowestSellOrder'] = round(_positive_float((quote or {}).get('lowestSellOrder')), 2) if _positive_float((quote or {}).get('lowestSellOrder')) else None
    item['highestBuyOrder'] = round(_positive_float((quote or {}).get('highestBuyOrder')), 2) if _positive_float((quote or {}).get('highestBuyOrder')) else None
    item['orderBookSpreadPct'] = round(_positive_float((quote or {}).get('orderBookSpreadPct')), 2) if _positive_float((quote or {}).get('orderBookSpreadPct')) is not None else None
    item['priceStabilityPct'] = round(_positive_float((quote or {}).get('priceStabilityPct')), 1) if _positive_float((quote or {}).get('priceStabilityPct')) is not None else None
    item['solidPrice'] = round(_positive_float((quote or {}).get('solidPrice')) or market_unit, 2) if market_unit else None
    raw_confidence = _positive_float((quote or {}).get('confidencePct'))
    if guarded_snapshot and raw_confidence is not None:
        raw_confidence = min(raw_confidence, 35)
    item['priceConfidencePct'] = round(raw_confidence, 1) if raw_confidence is not None else None
    item['providerPrices'] = (quote or {}).get('providerPrices') or {}
    item['marketAnchorPrice'] = round(history_anchor, 2) if history_anchor else None
    item['marketPriceWarning'] = (
        'Provider quote disagrees with recent history anchor; valuation still uses the visible provider quote.'
        if guarded_snapshot else None
    )
    item['marketPriceBasis'] = (quote or {}).get('priceBasis') or ('lowest listed sell order' if market_source == 'steam_quote' else market_source)
    item['marketIsCached'] = market_source != 'missing'
    item['marketQuoteStale'] = bool((quote or {}).get('stale'))
    if curr_total is not None and past_total is not None:
        item['marketDollar'] = round(curr_total - past_total, 2)
        item['marketPct'] = round(((curr_total / past_total) - 1) * 100, 2) if past_total > 0 else None
    else:
        item['marketDollar'] = None
        item['marketPct'] = None
    return item


def get_portfolio_current_value(limit=2000, portfolio_id=None):
    """Read saved current valuations only; analytics and freshness are separate."""
    limit = parse_int_param(limit, 2000, minimum=1, maximum=10000)
    exact_meta, base_meta, _catalog_count = catalog_index()
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        portfolio = latest_portfolio(conn, portfolio_id)
        revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
        source_revision = revision['value'] if revision else None
        total = conn.execute('SELECT COUNT(*) FROM portfolio_items WHERE portfolio_id=?',
                             (portfolio['portfolio_id'],)).fetchone()[0] if portfolio else 0
        held = portfolio_inventory_rows(conn, portfolio['portfolio_id'], limit) if portfolio else []
        enriched = [enrich_item(row, exact_meta, base_meta) for row in held]
        names = list(dict.fromkeys(name for item in enriched
                                  for name in (item.get('name'), item.get('marketHashName')) if name))
        quotes = best_market_quotes(conn, names)

    items = []
    groups = {}
    for item in enriched:
        quote = quotes.get(item.get('marketHashName')) or quotes.get(item.get('name')) or {}
        unit = _positive_float(quote.get('price')) or _positive_float(quote.get('median'))
        qty = _positive_float(item.get('qty')) or 0
        value = round(unit * qty, 2) if unit and qty else None
        provider = quote.get('provider') if unit else None
        basis = 'steam_aggregate' if provider == 'steam_snapshot' else 'cash_fallback' if provider else 'missing'
        captured = int(quote.get('updated_at') or 0) or None
        selected = {key: quote.get(key) for key in
                    ('provider', 'price', 'median', 'source', 'priceBasis', 'updated_at',
                     'priceSemantics', 'executableAsk')}
        row = {
            'name': item.get('name'), 'marketHashName': item.get('marketHashName'),
            'qty': item.get('qty'), 'marketCurr': value,
            'marketUnitPrice': round(unit, 2) if unit else None,
            'marketPast': None, 'marketQuoteProvider': provider,
            'marketPriceSource': quote.get('source'), 'marketPriceBasis': quote.get('priceBasis'),
            'marketQuoteCapturedAt': captured, 'marketQuoteStale': None,
            'quoteAvailable': bool(unit),
            'availableProviders': [key for key, value in (quote.get('providerPrices') or {}).items()
                                   if _positive_float(value.get('price')) or _positive_float(value.get('median'))],
            'valueBasis': basis, 'quote': selected,
        }
        items.append(row)
        group = groups.setdefault(basis, {'items': 0, 'quantity': 0, 'value': 0,
                                         'oldestCaptureAt': None, 'latestCaptureAt': None})
        group['items'] += 1
        group['quantity'] += qty
        group['value'] += value or 0
        if captured:
            group['oldestCaptureAt'] = min(group['oldestCaptureAt'] or captured, captured)
            group['latestCaptureAt'] = max(group['latestCaptureAt'] or captured, captured)
    for group in groups.values():
        group['quantity'] = round(group['quantity'], 2)
        group['value'] = round(group['value'], 2)
    steam = groups.get('steam_aggregate', {})
    cash = groups.get('cash_fallback', {})
    return {
        'projection': 'portfolio-value-v1', 'portfolio': portfolio, 'items': items,
        'currentSourceRevision': source_revision,
        'valuationState': 'saved_current_quotes', 'freshnessState': 'not_evaluated',
        'analyticsState': 'not_requested',
        'totals': {'savedValue': round(sum(row['marketCurr'] or 0 for row in items), 2),
                   'steamAggregateValue': steam.get('value', 0),
                   'cashFallbackValue': cash.get('value', 0)},
        'coverage': {'totalPortfolioItems': total, 'returnedItems': len(items),
                     'truncated': len(items) < total, 'quoteCoverage': sum(row['quoteAvailable'] for row in items),
                     'missingMarketPrices': sum(not row['quoteAvailable'] for row in items),
                     'groups': groups, 'historyFallbackUsed': False},
    }


def get_inventory_dataset(scale_days=90, history_days=None, limit=2000, refresh=False, refresh_limit=25, refresh_history=False, history_limit=10, portfolio_id=None, force=False):
    """Return a portfolio with locally cached per-item quote/volatility fields."""
    history_days = parse_int_param(history_days if history_days is not None else scale_days, 90, allowed=VOLATILITY_WINDOWS)
    limit = parse_int_param(limit, 2000, minimum=1, maximum=10000)
    refresh_limit = parse_int_param(refresh_limit, 25, minimum=1, maximum=200)
    history_limit = parse_int_param(history_limit, 10, minimum=1, maximum=50)
    exact_meta, base_meta, catalog_count = catalog_index()
    with closing(catalog_read_db()) as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            return {'scaleDays': history_days, 'historyDays': history_days, 'portfolio': None, 'snapshot': None, 'items': [], 'coverage': {'items': 0}}
        rows = portfolio_inventory_rows(conn, portfolio['portfolio_id'], limit)
        items = [enrich_item(r, exact_meta, base_meta) for r in rows]
        names = [r['name'] for r in rows]
        quote_names = []
        for item in items:
            quote_names.append(item.get('name'))
            quote_names.append(item.get('marketHashName'))
        quote_names = [n for n in dict.fromkeys(quote_names) if n]

    if refresh and quote_names:
        market_universe_bulk_sync('csgotrader', 'any')

    if refresh_history:
        refresh_state = {}
        for item in items[:history_limit]:
            get_price_history(item.get('name'), force=force, market_hash_name=item.get('marketHashName'),
                              rebuild_metrics=False, _refresh_state=refresh_state)
        rebuild_metrics_for_names(refresh_state.get('names'), refresh_state=refresh_state)

    with closing(catalog_read_db()) as conn:
        quotes = best_market_quotes(conn, quote_names)
        vol, metric_window, metric_window_exact = get_cached_market_metric_map(conn, quote_names, history_days)
        volatility_windows = {}
        for win in VOLATILITY_DETAIL_WINDOWS:
            win_map, _win_metric, _win_exact = get_cached_market_metric_map(conn, quote_names, win)
            volatility_windows[str(win)] = win_map
    if not vol:
        vol = get_volatility_map(names, history_days)
        metric_window = history_days
        metric_window_exact = True
    for item in items:
        q = quotes.get(item.get('marketHashName')) or quotes.get(item.get('name')) or {}
        vm = vol.get(item.get('marketHashName')) or vol.get(item.get('name')) or {}
        item_windows = {}
        for win in VOLATILITY_DETAIL_WINDOWS:
            win_map = volatility_windows.get(str(win)) or {}
            metric = win_map.get(item.get('marketHashName')) or win_map.get(item.get('name')) or {}
            if metric:
                item_windows[str(win)] = metric
        item['quote'] = q
        item['volatility'] = vm
        item['volatilityWindows'] = item_windows
        attach_market_valuation(item, q, vm)
        item['liveDeltaPct'] = None
    snapshot_quote_count = sum(1 for i in items if i.get('marketPriceSource') == 'market_snapshot')
    direct_quote_count = sum(1 for i in items if i.get('quote', {}).get('price') and i.get('marketPriceSource') == 'steam_quote')
    history_price_count = sum(1 for i in items if i.get('marketPriceSource') == 'steam_history')
    market_count = sum(1 for i in items if i.get('marketIsCached'))
    vol_count = sum(1 for i in items if i.get('volatility', {}).get('points', 0) >= 2)
    latest_quote_at = max((int((i.get('quote') or {}).get('updated_at') or 0) for i in items), default=0)
    latest_history_ts = max((int((i.get('volatility') or {}).get('lastTs') or 0) for i in items), default=0)
    total_items = len(items)
    return {
        'scaleDays': history_days,
        'historyDays': history_days,
        'portfolio': portfolio,
        'snapshot': {
            'snapshot_id': portfolio['portfolio_id'],
            'label': portfolio['label'],
            'snapshot_date': (portfolio.get('imported_at') or '')[:10],
            'horizon_days': history_days,
        },
        'items': items,
        'coverage': {
            'items': total_items,
            'catalogItems': catalog_count,
            'quoteCoverage': sum(1 for i in items if i.get('quote', {}).get('price')),
            'snapshotQuoteCoverage': snapshot_quote_count,
            'directQuoteCoverage': direct_quote_count,
            'historyPriceCoverage': history_price_count,
            'itemGraphCoverage': vol_count,
            'marketCoverage': market_count,
            'marketCoveragePct': round((market_count / total_items) * 100, 1) if total_items else 0,
            'snapshotQuoteCoveragePct': round((snapshot_quote_count / total_items) * 100, 1) if total_items else 0,
            'quoteCoveragePct': round((direct_quote_count / total_items) * 100, 1) if total_items else 0,
            'historyPriceCoveragePct': round((history_price_count / total_items) * 100, 1) if total_items else 0,
            'volatilityCoverage': vol_count,
            'volatilityCoveragePct': round((vol_count / total_items) * 100, 1) if total_items else 0,
            'missingMarketPrices': sum(1 for i in items if not i.get('marketUnitPrice')),
            'latestQuoteAt': latest_quote_at,
            'latestHistoryTs': latest_history_ts,
            'dataset': {
                'id': f"portfolio-observed:{portfolio['portfolio_id']}:{max(latest_quote_at, latest_history_ts)}:{total_items}",
                'source': 'local provider price cache and persisted item movement metrics',
                'asOf': max(latest_quote_at, latest_history_ts),
                'items': total_items,
                'windowDays': history_days,
                'metricWindowDays': metric_window,
                'metricWindowExact': metric_window_exact,
                'implication': 'Changing the day window changes prior/reference and volatility math only; current prices remain tied to the latest observed cache rows.',
            },
        },
    }


def chart_item_aliases(item):
    aliases = [
        item.get('marketHashName'),
        item.get('market_hash_name'),
        item.get('name'),
    ]
    return [a for a in dict.fromkeys(str(a or '').strip() for a in aliases) if a]


def chart_item_quantity(item, default=1.0):
    qty = (
        _positive_float(item.get('qty'))
        or _positive_float(item.get('quantity'))
        or _positive_float(item.get('count'))
    )
    return qty if qty is not None else default


def is_fetchable_steam_history_item(item):
    """True when the row should correspond to an actual Steam listing page."""
    name = str((item or {}).get('marketHashName') or (item or {}).get('name') or '').strip()
    if not name:
        return False
    kind = str((item or {}).get('kind') or '').strip().lower()
    if kind == 'skin_families':
        return False
    # Provider snapshots include family/index rows such as "★ Butterfly Knife";
    # Steam charts exist for concrete listings, not for those aggregate rows.
    if name.startswith('★') and '|' not in name and '(' not in name:
        return False
    return True


def recent_steam_history_failures(conn, names, ttl=STEAM_HISTORY_FAILURE_TTL):
    clean = [n for n in dict.fromkeys(str(n or '').strip() for n in names or []) if n]
    if not clean:
        return {}
    cutoff = int(time.time()) - int(ttl or 0)
    failures = {}
    for i in range(0, len(clean), 400):
        chunk = clean[i:i+400]
        q = ','.join('?' for _ in chunk)
        rows = conn.execute(
            f"""SELECT name, error, failed_at, attempts
                FROM steam_history_failures
                WHERE failed_at >= ?
                  AND COALESCE(fetch_version, 0)=?
                  AND name IN ({q})""",
            [cutoff, STEAM_HISTORY_FETCH_VERSION] + chunk,
        ).fetchall()
        for row in rows:
            failures[row['name']] = dict(row)
    return failures


def save_steam_history_failures(failures):
    rows = [
        (
            f.get('name'),
            f.get('marketHashName') or f.get('name'),
            (f.get('error') or 'Steam returned no chart rows')[:1000],
            int(time.time()),
            STEAM_HISTORY_FETCH_VERSION,
        )
        for f in failures or []
        if f.get('name')
    ]
    if not rows:
        return
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            conn.executemany(
                """INSERT INTO steam_history_failures(name, market_hash_name, error, failed_at, fetch_version, attempts)
                   VALUES (?, ?, ?, ?, ?, 1)
                   ON CONFLICT(name) DO UPDATE SET
                       market_hash_name=excluded.market_hash_name,
                       error=excluded.error,
                       failed_at=excluded.failed_at,
                       fetch_version=excluded.fetch_version,
                       attempts=steam_history_failures.attempts + 1""",
                rows,
            )


def clear_steam_history_failures(names):
    clean = [n for n in dict.fromkeys(str(n or '').strip() for n in names or []) if n]
    if not clean:
        return
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            for i in range(0, len(clean), 400):
                chunk = clean[i:i+400]
                q = ','.join('?' for _ in chunk)
                conn.execute(f"DELETE FROM steam_history_failures WHERE name IN ({q})", chunk)


def refresh_steam_history_for_chart_items(
    items,
    limit=0,
    force=False,
    retry_failures=False,
    progress=None,
    stage_prefix='steam_history',
):
    """Warm raw Steam chart rows for the highest-impact missing chart items."""
    def report(stage, message, **payload):
        if not progress:
            return
        try:
            progress(f"{stage_prefix}_{stage}", message, **payload)
        except Exception:
            pass

    limit = parse_int_param(limit, 0, minimum=0, maximum=250)
    if limit <= 0:
        return {'attempted': 0, 'refreshed': 0, 'failed': 0, 'cachedFallback': 0, 'errors': []}
    raw_items = list(items or [])
    fetchable_items = [item for item in raw_items if is_fetchable_steam_history_item(item)]
    skipped_unfetchable = len(raw_items) - len(fetchable_items)
    candidate_names = [
        (item.get('name') or item.get('marketHashName') or '').strip()
        for item in fetchable_items
        if (item.get('name') or item.get('marketHashName') or '').strip()
    ]
    cached_names = set()
    recent_failures = {}
    if candidate_names and not force:
        with db() as conn:
            for i in range(0, len(candidate_names), 400):
                chunk = list(dict.fromkeys(candidate_names[i:i+400]))
                q = ','.join('?' for _ in chunk)
                cached_names.update(
                    row['name'] for row in conn.execute(
                        f"""SELECT DISTINCT name
                            FROM price_history
                            WHERE provider='steam' AND price IS NOT NULL AND name IN ({q})""",
                        chunk,
                    )
                )
            if not retry_failures:
                recent_failures = recent_steam_history_failures(conn, candidate_names)
    ranked = sorted(
        fetchable_items,
        key=lambda item: (
            (_positive_float(item.get('currentValue')) or 0)
            or ((_positive_float(item.get('latestPrice')) or 0) * chart_item_quantity(item))
        ),
        reverse=True,
    )
    # A canonical redirect needs a later, separately admitted request for the
    # same item. Count actual HTTP attempts independently of unique candidates.
    max_attempts = max(limit * 4, limit + 12)
    attempted = 0
    refreshed = 0
    cached_fallback = 0
    skipped_failures = 0
    errors = []
    failure_records = []
    successful_names = []
    refresh_state = {}
    rate_limit = {}
    report(
        'queued',
        f"Queued up to {limit:,} Steam chart fetches from {len(ranked):,} candidates",
        limit=limit,
        candidateCount=len(ranked),
        skippedUnfetchable=skipped_unfetchable,
    )
    for item in ranked:
        # Cached fallbacks consumed this budget before refresh reporting was
        # separated from chart availability. Keep the same provider work cap.
        if refreshed + cached_fallback >= limit or attempted >= max_attempts:
            break
        name = (item.get('name') or item.get('marketHashName') or '').strip()
        if not name:
            continue
        if name in cached_names and not force:
            continue
        if name in recent_failures and not force and not retry_failures:
            skipped_failures += 1
            continue
        wait_for_provider = getattr(progress, '_wait_for_provider', None)
        while True:
            attempted += 1
            if wait_for_provider and not wait_for_provider():
                hist = {'governorDeferred': True, 'reason': 'task_control', 'points': []}
                break
            report('item', f"Steam chart {attempted}/{limit}: {name}", item=name,
                   marketHashName=item.get('marketHashName') or name,
                   attempted=attempted, limit=limit, refreshed=refreshed,
                   skippedRecentFailures=skipped_failures)
            hist = get_price_history(name, force=True,
                market_hash_name=(item.get('marketHashName') or name),
                rebuild_metrics=False, _refresh_state=refresh_state)
            deferred = hist.get('governorDeferred') or hist.get('rateLimited')
            if deferred and wait_for_provider:
                if attempted >= max_attempts:
                    hist = dict(hist, historyAttemptCapReached=True)
                    break
                if wait_for_provider(hist):
                    if not hist.get('networkAttempted') and not hist.get('refreshAttempted'):
                        attempted -= 1
                    continue
            break
        if hist.get('governorDeferred'):
            attempted -= 0 if hist.get('networkAttempted') else 1
            cached_fallback += bool(hist.get('points'))
            rate_limit = {key:hist.get(key) for key in ('governorDeferred','reason','retryAt','operatorPaused','recoveryRequired','canonicalRedirect','networkAttempted','httpStatus','historyAttemptCapReached')}
            report('governor_deferred', 'Steam acquisition paused or waiting for its durable budget; stopped this batch', **rate_limit)
            break
        if hist.get('rateLimited'):
            rate_limit = {key: hist.get(key) for key in
                          ('rateLimited', 'httpStatus', 'retryAt', 'retryAfter', 'cooldownActive','historyAttemptCapReached')}
            if hist.get('cooldownActive'):
                attempted -= 1  # The provider was not contacted for this target.
                cached_fallback += bool(hist.get('points'))
                report('rate_limited', 'Steam Retry-After cooldown is active; stopped this batch', **rate_limit)
                break
        if hist.get('historySaved'):
            refreshed += 1
            successful_names.append(name)
            report(
                'item_complete',
                f"Steam chart cached {refreshed}/{limit}: {name}",
                item=name,
                attempted=attempted,
                limit=limit,
                refreshed=refreshed,
                pointCount=len(hist.get('points') or []),
            )
        else:
            cached_fallback += bool(hist.get('points'))
            failure = {
                'name': name,
                'marketHashName': item.get('marketHashName') or name,
                'error': hist.get('refreshError') or hist.get('error') or 'No new Steam chart rows were saved',
            }
            failure_records.append(failure)
            if len(errors) < 5:
                errors.append(failure)
            report(
                'item_error',
                f"Steam chart failed {attempted}/{limit}: {name}",
                item=name,
                attempted=attempted,
                limit=limit,
                refreshed=refreshed,
                error=failure['error'],
            )
            if hist.get('rateLimited'):
                report('rate_limited', 'Steam returned HTTP429; stopped this batch', **rate_limit)
                break
    save_steam_history_failures(failure_records)
    clear_steam_history_failures(successful_names)
    rebuilt_metrics = rebuild_metrics_for_names(refresh_state.get('names'), refresh_state=refresh_state)
    return {
        'attempted': attempted,
        'refreshed': refreshed,
        'failed': len(failure_records),
        'cachedFallback': cached_fallback,
        'rebuiltMetrics': rebuilt_metrics,
        'skippedRecentFailures': skipped_failures,
        'skippedUnfetchable': skipped_unfetchable,
        'errors': errors,
        'stoppedReason': 'governor_deferred' if rate_limit.get('governorDeferred') else 'rate_limited' if rate_limit else None,
        **rate_limit,
    }


def latest_portfolio_history_candidates(limit=120, portfolio_id=None, all_items=False):
    """Return portfolio items most likely to drive visible value curves."""
    limit = parse_int_param(limit, 120, minimum=0, maximum=10000 if all_items else 1000)
    if limit <= 0:
        return []
    exact_meta, base_meta, _catalog_count = catalog_index()
    with db() as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            if portfolio_id:
                raise ValueError('Selected portfolio no longer exists; no other portfolio was warmed.')
            return []
        if portfolio_id and portfolio['portfolio_id'] != portfolio_id:
            raise ValueError('Selected portfolio no longer exists; no other portfolio was warmed.')
        if all_items:
            count = conn.execute('SELECT COUNT(*) FROM portfolio_items WHERE portfolio_id=?',
                                 (portfolio['portfolio_id'],)).fetchone()[0]
            if count > 10000:
                raise ValueError('Selected portfolio exceeds the 10,000-row tracked history scope; no truncated sync was started.')
        rows = portfolio_inventory_rows(conn, portfolio['portfolio_id'], 10000)
        items = [enrich_item(r, exact_meta, base_meta) for r in rows]
        quote_names = []
        for item in items:
            quote_names.extend(chart_item_aliases(item))
        quotes = best_market_quotes(conn, quote_names)
    out = []
    for item in items:
        quote = {}
        for alias in chart_item_aliases(item):
            quote = quotes.get(alias) or quote
            if quote:
                break
        latest_price = _positive_float((quote or {}).get('solid_price')) or _positive_float((quote or {}).get('price'))
        qty = chart_item_quantity(item, default=1.0)
        out.append({
            'name': item.get('name'),
            'marketHashName': item.get('marketHashName') or item.get('name'),
            'latestPrice': latest_price,
            'currentValue': (latest_price or 0) * qty,
            'qty': qty,
            'source': 'portfolio',
            'kind': item.get('kind'),
            'item_type': item.get('item_type') or item.get('type'),
            'weapon': item.get('weapon'),
            'collection': item.get('collection'),
            'rarity': item.get('rarity'),
        })
    out.sort(key=lambda item: (_positive_float(item.get('currentValue')) or 0, item.get('name') or ''), reverse=True)
    return out[:limit]


def market_history_candidates(limit=120, history_days=90):
    """Return market items that make the overview/breakdown pages useful first."""
    limit = parse_int_param(limit, 120, minimum=0, maximum=1000)
    if limit <= 0:
        return []
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    with closing(catalog_read_db()) as conn:
        metric_window, _exact_window, _metric_count = resolve_market_metric_window(conn, history_days)
        ensure_market_item_lookup(conn, metric_window)
        query_limit = max(limit * 2, 80)
        orderings = [
            "COALESCE(latest_price, 0) DESC",
            "COALESCE(volatility_pct, 0) DESC",
            "COALESCE(ABS(change_pct), 0) DESC",
        ]
        candidates = {}
        for ordering in orderings:
            rows = conn.execute(
                f"""SELECT name, market_hash_name AS marketHashName, kind, item_type, weapon,
                           collection, rarity, latest_price AS latestPrice,
                           latest_price AS currentValue, volatility_pct AS volatilityPct,
                           change_pct AS changePct
                    FROM market_item_lookup
                    WHERE window_days=? AND latest_price IS NOT NULL AND steam_price IS NOT NULL
                      AND name NOT LIKE 'Sticker Slab |%'
                    ORDER BY {ordering}, LOWER(name) ASC
                    LIMIT ?""",
                (metric_window, query_limit),
            ).fetchall()
            for row in rows:
                name = row['name']
                if not name or name in candidates:
                    continue
                item = dict(row)
                item['source'] = 'market'
                candidates[name] = item
    ranked = sorted(
        candidates.values(),
        key=lambda item: (
            (_positive_float(item.get('currentValue')) or 0) * 0.05
            + abs(_finite_float(item.get('volatilityPct')) or 0)
            + abs(_finite_float(item.get('changePct')) or 0) * 0.35
        ),
        reverse=True,
    )
    return ranked[:limit]


def warm_steam_history_after_market_sync(limit=None, portfolio_id=None, force=False, retry_failures=False, progress=None):
    """Warm enough raw Steam chart history for the main release pages to work."""
    limit = parse_int_param(
        DEFAULT_SYNC_STEAM_HISTORY_LIMIT if limit is None else limit,
        DEFAULT_SYNC_STEAM_HISTORY_LIMIT,
        minimum=0,
        maximum=500,
    )
    if limit <= 0:
        return {'attempted': 0, 'refreshed': 0, 'errors': [], 'candidateItems': 0, 'enabled': False}
    # Candidate pool and refresh target are intentionally separate. A portfolio
    # may already have chart rows for its most valuable items, so the warmer
    # must keep scanning lower-ranked candidates until it finds missing charts.
    portfolio_target = min(limit, max(30, int(limit * 0.7)))
    market_target = max(0, limit - portfolio_target)
    portfolio_pool = min(1000, max(portfolio_target * 8, portfolio_target + 80, 500))
    market_pool = min(1000, max(market_target * 8, market_target + 80, 500)) if market_target else 0
    candidates = []
    candidates.extend(latest_portfolio_history_candidates(portfolio_pool, portfolio_id=portfolio_id))
    candidates.extend(market_history_candidates(market_pool, history_days=90))
    deduped = []
    seen = set()
    for item in candidates:
        key = item.get('marketHashName') or item.get('name')
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    result = refresh_steam_history_for_chart_items(
        deduped,
        limit=limit,
        force=force,
        retry_failures=retry_failures,
        progress=progress,
    )
    result.update({
        'enabled': True,
        'candidateItems': len(deduped),
        'portfolioRefreshTarget': portfolio_target,
        'marketRefreshTarget': market_target,
        'portfolioCandidateLimit': portfolio_pool,
        'marketCandidateLimit': market_pool,
    })
    return result


def aggregate_value_history_for_items(items, history_days=90, points=220, value_basis='portfolio_value', refresh=False, refresh_limit=0, force=False):
    """Build a reusable value curve from cached Steam history.

    Only items with genuine dated Steam chart rows contribute. Missing history
    is excluded with explicit coverage; current quotes never fill a past date.
    Interpolation between actual observations and later carry are flagged.
    """
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    points = parse_int_param(points, 220, minimum=24, maximum=480)
    chart_items = []
    query_names = []
    seen_query_names = set()
    for item in items or []:
        aliases = chart_item_aliases(item)
        if not aliases:
            continue
        qty = chart_item_quantity(item, default=1.0)
        if qty <= 0:
            continue
        chart_items.append({**item, 'aliases': aliases, 'chartQty': qty})
        for alias in aliases:
            if alias not in seen_query_names:
                query_names.append(alias)
                seen_query_names.add(alias)

    if refresh:
        refresh_result = refresh_steam_history_for_chart_items(chart_items, limit=refresh_limit, force=force)
    else:
        refresh_result = {'attempted': 0, 'refreshed': 0}

    item_count = len(chart_items)
    cutoff = history_cutoff_ts(history_days)
    series_by_name = {}
    latest_history_ts = 0
    earliest_history_ts = 0
    with db() as conn:
        for i in range(0, len(query_names), 400):
            chunk = query_names[i:i+400]
            if not chunk:
                continue
            q = ','.join('?' for _ in chunk)
            params = list(chunk)
            row = conn.execute(
                f"""SELECT MAX(point_ts) AS latest_ts, MIN(point_ts) AS earliest_ts
                    FROM price_history
                    WHERE provider='steam' AND name IN ({q}) AND price IS NOT NULL""",
                params,
            ).fetchone()
            latest_history_ts = max(latest_history_ts, int(row['latest_ts'] or 0) if row else 0)
            row_earliest = int(row['earliest_ts'] or 0) if row else 0
            if row_earliest:
                earliest_history_ts = min(earliest_history_ts or row_earliest, row_earliest)
            hist_params = list(chunk)
            where = ''
            if cutoff:
                where = 'AND COALESCE(point_ts, 0) >= ?'
                hist_params.append(max(0, cutoff - 86400))
            hist_rows = conn.execute(f"""
                SELECT name, point_ts, price, volume, 'steam_history' AS source
                FROM price_history
                WHERE provider='steam' AND name IN ({q}) AND price IS NOT NULL {where}
                ORDER BY name, point_ts
            """, hist_params).fetchall()
            for row in hist_rows:
                series_by_name.setdefault(row['name'], []).append({
                    'ts': int(row['point_ts'] or 0),
                    'price': float(row['price']),
                    'volume': int(row['volume'] or 0),
                    'source': 'steam_history',
                })

    normalized_series = {}
    backfill_prices = {}
    current_anchor_count = 0
    for item in chart_items:
        raw_series = []
        for alias in item.get('aliases') or []:
            raw_series.extend(series_by_name.get(alias, []))
        series = normalized_history_series(raw_series) if raw_series else []
        key = item['aliases'][0]
        if len(series) >= 2:
            normalized_series[key] = series

    items_with_history = len(normalized_series)
    if not normalized_series:
        return {
            'ok': True,
            'historyDays': history_days,
            'generatedAt': int(time.time()),
            'points': [],
            'historyAvailable': False,
            'historyState': 'missing_raw_steam_history',
            'historyPrerequisite': 'Genuine cached Steam chart observations are required. Use an authenticated history refresh; provider pacing, access and cooldowns still apply.',
            'coverage': {
                'items': item_count,
                'itemsWithHistory': 0,
                'coveragePct': 0,
                'steamHistoryItems': 0,
                'observationHistoryItems': 0,
                'priceSource': 'steam_chart_history_unavailable',
                'basis': value_basis,
                'backfilledCurrentItems': len(backfill_prices),
                'currentAnchoredItems': current_anchor_count,
                'latestObservationAt': latest_history_ts,
                'firstValue': None,
                'latestValue': None,
                'interpolated': False,
                'cleaned': True,
                'carriedForwardPoints': 0,
                'refreshAttempted': refresh_result.get('attempted', 0),
                'refreshRefreshed': refresh_result.get('refreshed', 0),
                'refreshFailed': refresh_result.get('failed', 0),
                'refreshCachedFallback': refresh_result.get('cachedFallback', 0),
                'refreshSkippedRecentFailures': refresh_result.get('skippedRecentFailures', 0),
                'refreshErrors': refresh_result.get('errors') or [],
                'skippedUnfetchable': refresh_result.get('skippedUnfetchable', 0),
                'skippedRecentFailures': refresh_result.get('skippedRecentFailures', 0),
            },
        }
    latest_history_ts = latest_history_ts or int(time.time())
    bucket_step = 3600 if history_days <= 7 else 14400 if history_days <= 30 else 43200 if history_days <= 90 else 86400
    if cutoff:
        start_ts = cutoff
        total_span = max(bucket_step, latest_history_ts - start_ts)
    else:
        start_ts = earliest_history_ts or max(0, latest_history_ts - bucket_step * (points - 1))
        total_span = max(bucket_step, latest_history_ts - start_ts)
    bucket_step = max(bucket_step, int(total_span / max(1, points - 1)))
    timeline = list(range(start_ts, latest_history_ts + 1, bucket_step))
    if not timeline or timeline[-1] != latest_history_ts:
        timeline.append(latest_history_ts)

    chart_points = []
    latest_value = None
    for ts in timeline:
        total = 0.0
        contributing = 0
        interpolated_items = 0
        carried_forward_items = 0
        backfilled_items = 0
        not_started_items = 0
        for item in chart_items:
            key = (item.get('aliases') or [''])[0]
            series = normalized_series.get(key) or []
            if not series:
                continue
            price = None
            if ts < series[0]['ts']:
                # Never project the first observation backward into dates for
                # which this item has no historical evidence.
                continue
            elif ts >= series[-1]['ts']:
                price = series[-1]['price']
                if ts > series[-1]['ts']:
                    carried_forward_items += 1
            else:
                for idx in range(1, len(series)):
                    right = series[idx]
                    left = series[idx - 1]
                    if ts == right['ts']:
                        price = right['price']
                        break
                    if left['ts'] <= ts <= right['ts']:
                        span = max(1, right['ts'] - left['ts'])
                        mix = (ts - left['ts']) / span
                        price = left['price'] + (right['price'] - left['price']) * mix
                        if ts != left['ts'] and ts != right['ts']:
                            interpolated_items += 1
                        break
            if price:
                total += price * item['chartQty']
                contributing += 1
        if contributing <= 0:
            continue
        latest_value = total
        chart_points.append({
            'ts': ts,
            'value': round(total, 2),
            'label': datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).date().isoformat(),
            'contributingItems': contributing,
            'coveragePct': round((contributing / item_count) * 100, 1) if item_count else 0,
            'interpolatedItems': interpolated_items,
            'carriedForwardItems': carried_forward_items,
            'notStartedItems': not_started_items,
            'backfilledItems': backfilled_items,
            'isCarriedForward': bool(carried_forward_items or backfilled_items),
        })

    coverage_pct = round((items_with_history / item_count) * 100, 1) if item_count else 0
    backfilled_item_count = len(backfill_prices)
    first_value = chart_points[0]['value'] if chart_points else None
    latest_value = round(latest_value, 2) if latest_value is not None else None
    price_source = 'steam_chart_history_with_current_backfill' if backfilled_item_count else 'steam_chart_history'
    return {
        'ok': True,
        'historyDays': history_days,
        'generatedAt': int(time.time()),
        'points': chart_points,
        'historyAvailable': bool(chart_points),
        'historyState': 'genuine_cached_steam_history',
        'firstValue': first_value,
        'latestValue': latest_value,
        'coverage': {
            'items': item_count,
            'itemsWithHistory': min(item_count, items_with_history),
            'coveragePct': coverage_pct,
            'steamHistoryItems': min(item_count, items_with_history),
            'observationHistoryItems': 0,
            'priceSource': price_source,
            'basis': value_basis,
            'backfilledCurrentItems': backfilled_item_count,
            'currentAnchoredItems': current_anchor_count,
            'latestObservationAt': latest_history_ts,
            'firstValue': first_value,
            'latestValue': latest_value,
            'interpolated': True,
            'cleaned': True,
            'carriedForwardPoints': sum(1 for p in chart_points if p.get('carriedForwardItems')),
            'refreshAttempted': refresh_result.get('attempted', 0),
            'refreshRefreshed': refresh_result.get('refreshed', 0),
            'refreshFailed': refresh_result.get('failed', 0),
            'refreshCachedFallback': refresh_result.get('cachedFallback', 0),
            'refreshSkippedRecentFailures': refresh_result.get('skippedRecentFailures', 0),
            'refreshErrors': refresh_result.get('errors') or [],
            'skippedUnfetchable': refresh_result.get('skippedUnfetchable', 0),
            'skippedRecentFailures': refresh_result.get('skippedRecentFailures', 0),
        },
    }


def portfolio_value_history(portfolio_id=None, history_days=90, points=220, refresh=False, refresh_limit=0, force=False):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    points = parse_int_param(points, 220, minimum=24, maximum=480)
    exact_meta, base_meta, _catalog_count = catalog_index()
    with db() as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            return {'ok': False, 'error': 'No portfolio selected', 'points': [], 'historyDays': history_days}
        rows = portfolio_inventory_rows(conn, portfolio['portfolio_id'], 10000)
    items = []
    quote_names = []
    for row in rows:
        name = (row.get('name') or '').strip()
        qty = _positive_float(row.get('qty')) or 0
        if not name or qty <= 0:
            continue
        enriched = enrich_item(row, exact_meta, base_meta)
        market_hash = (enriched.get('marketHashName') or name)
        quote_names.extend([name, market_hash])
        items.append({
            'name': name,
            'marketHashName': market_hash,
            'qty': qty,
            'currentValue': 0,
            'latestPrice': 0,
            'kind': enriched.get('kind'),
            'item_type': enriched.get('item_type') or enriched.get('type'),
            'weapon': enriched.get('weapon'),
            'collection': enriched.get('collection'),
            'rarity': enriched.get('rarity'),
        })
    quote_names = [n for n in dict.fromkeys(quote_names) if n]
    with db() as conn:
        quotes = best_market_quotes(conn, quote_names)
    for item in items:
        q = quotes.get(item.get('marketHashName')) or quotes.get(item.get('name')) or {}
        latest = _positive_float(q.get('solid_price')) or _positive_float(q.get('price'))
        item['latestPrice'] = latest or 0
        item['currentValue'] = (latest or 0) * (_positive_float(item.get('qty')) or 0)
        item['latestAt'] = int(q.get('updated_at') or q.get('latestAt') or 0)
    out = aggregate_value_history_for_items(
        items,
        history_days=history_days,
        points=points,
        value_basis='portfolio_value',
        refresh=refresh,
        refresh_limit=refresh_limit,
        force=force,
    )
    out['portfolio'] = portfolio
    return out


def portfolio_sync_batch(scale_days=90, history_days=None, offset=0, limit=20, include_history=True, portfolio_id=None, force=True):
    """Refresh Steam quote/history cache for a slice of a portfolio."""
    history_days = parse_int_param(history_days if history_days is not None else scale_days, 90, allowed=VOLATILITY_WINDOWS)
    offset = parse_int_param(offset, 0, minimum=0, maximum=100000)
    limit = parse_int_param(limit, 20, minimum=1, maximum=100)
    exact_meta, base_meta, catalog_count = catalog_index()

    with db() as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            return {'ok': False, 'error': 'No portfolio selected', 'items': 0, 'total': 0}
        total = conn.execute(
            "SELECT COUNT(*) FROM portfolio_items WHERE portfolio_id=?",
            (portfolio['portfolio_id'],),
        ).fetchone()[0]
        rows = [dict(r) for r in conn.execute("""
            SELECT pi.portfolio_id AS snapshot_id,
                   pi.portfolio_id AS portfolio_id,
                   pi.sort_key,
                   p.label,
                   NULL AS snapshot_date,
                   NULL AS horizon_days,
                   pi.name,
                   pi.base_name,
                   pi.qty,
                   NULL AS pct,
                   NULL AS dollar,
                   NULL AS curr,
                   NULL AS past,
                   NULL AS unit_curr,
                   NULL AS unit_past,
                   pi.category,
                   pi.group_name
            FROM portfolio_items pi
            JOIN portfolios p ON p.id = pi.portfolio_id
            WHERE pi.portfolio_id=? ORDER BY COALESCE(pi.qty, 0) DESC, pi.sort_key
            LIMIT ? OFFSET ?
        """, (portfolio['portfolio_id'], limit, offset))]

    items = [enrich_item(r, exact_meta, base_meta) for r in rows]
    quote_names = []
    for item in items:
        quote_names.append(item.get('marketHashName') or item.get('name'))
        if item.get('name') != item.get('marketHashName'):
            quote_names.append(item.get('name'))
    quote_names = [n for n in dict.fromkeys(quote_names) if n]
    if quote_names and force:
        market_universe_bulk_sync('csgotrader', 'any')

    history_refreshed = 0
    if include_history:
        refresh_state = {}
        for item in items:
            hist = get_price_history(item.get('name'), force=force, market_hash_name=item.get('marketHashName'),
                                     rebuild_metrics=False, _refresh_state=refresh_state)
            if hist.get('historySaved'):
                history_refreshed += 1
        rebuild_metrics_for_names(refresh_state.get('names'), refresh_state=refresh_state)

    all_names = []
    with db() as conn:
        all_rows = [dict(r) for r in conn.execute(
            """SELECT portfolio_id AS snapshot_id, sort_key, name, base_name, qty,
                      NULL AS pct, NULL AS dollar, NULL AS curr, NULL AS past,
                      NULL AS unit_curr, NULL AS unit_past,
                      category, group_name
               FROM portfolio_items WHERE portfolio_id=?""",
            (portfolio['portfolio_id'],),
        )]
        all_names = [r['name'] for r in all_rows]
        all_quote_names = []
        for row in all_rows:
            enriched = enrich_item(row, exact_meta, base_meta)
            all_quote_names.append(enriched.get('marketHashName') or enriched.get('name'))
            if enriched.get('name') != enriched.get('marketHashName'):
                all_quote_names.append(enriched.get('name'))
        all_quote_names = [n for n in dict.fromkeys(all_quote_names) if n]
        quote_count = 0
        snapshot_quote_count = 0
        direct_quote_count = 0
        history_quote_count = 0
        history_count = 0
        for i in range(0, len(all_quote_names), 400):
            chunk = all_quote_names[i:i+400]
            q = ','.join('?' for _ in chunk)
            quote_rows = conn.execute(
                f"SELECT provider FROM market_item_prices WHERE price IS NOT NULL AND name IN ({q})",
                chunk,
            ).fetchall()
            for qr in quote_rows:
                provider = qr['provider']
                if provider == 'steam_history':
                    history_quote_count += 1
                elif provider == 'steam':
                    direct_quote_count += 1
                else:
                    snapshot_quote_count += 1
            quote_count += len(quote_rows)
            history_count += conn.execute(
                f"""SELECT COUNT(*) FROM (
                    SELECT name FROM price_history
                    WHERE provider='steam' AND name IN ({q})
                    GROUP BY name HAVING COUNT(*) >= 2
                )""",
                chunk,
            ).fetchone()[0]

    history_count = sum(1 for v in get_volatility_map(all_names, history_days).values() if (v or {}).get('points', 0) >= 2)
    next_offset = min(total, offset + len(items))
    return {
        'ok': True,
        'scaleDays': history_days,
        'historyDays': history_days,
        'portfolio': portfolio,
        'snapshot': {
            'snapshot_id': portfolio['portfolio_id'],
            'label': portfolio['label'],
            'snapshot_date': (portfolio.get('imported_at') or '')[:10],
            'horizon_days': history_days,
        },
        'offset': offset,
        'limit': limit,
        'processed': len(items),
        'nextOffset': next_offset,
        'done': next_offset >= total,
        'total': total,
        'quotesRequested': len(quote_names),
        'historyRequested': len(items) if include_history else 0,
        'historyRefreshed': history_refreshed,
        'coverage': {
            'items': total,
            'catalogItems': catalog_count,
            'quoteCoverage': quote_count,
            'snapshotQuoteCoverage': snapshot_quote_count,
            'directQuoteCoverage': direct_quote_count,
            'historyPriceCoverage': history_quote_count,
            'marketCoverage': quote_count,
            'volatilityCoverage': history_count,
            'marketCoveragePct': round((quote_count / total) * 100, 1) if total else 0,
            'quoteCoveragePct': round((quote_count / total) * 100, 1) if total else 0,
            'snapshotQuoteCoveragePct': round((snapshot_quote_count / total) * 100, 1) if total else 0,
            'directQuoteCoveragePct': round((direct_quote_count / total) * 100, 1) if total else 0,
            'historyPriceCoveragePct': round((history_quote_count / total) * 100, 1) if total else 0,
            'volatilityCoveragePct': round((history_count / total) * 100, 1) if total else 0,
            'missingMarketPrices': max(0, total - quote_count),
        },
    }

def portfolio_analysis(portfolio_id=None, history_days=90):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    with db() as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            return {
                'latestDate': None,
                'selectedScale': history_days,
                'historyDays': history_days,
                'horizons': [history_days],
                'latestSnapshots': {},
                'duplicates': [],
                'signals': {'broadUp': [], 'broadDown': [], 'reversalUp': [], 'reversalDown': [], 'mixed': []},
                'categories': [],
                'coverage': {'snapshots': 0, 'indexedRows': 0, 'items': 0, 'duplicateWindows': 0, 'volatilityCoverage': 0},
                'topVolatile': [],
            }
        rows = [dict(r) for r in conn.execute(
            "SELECT name, category FROM portfolio_items WHERE portfolio_id=?",
            (portfolio['portfolio_id'],),
        )]
    vol_map = get_volatility_map([r['name'] for r in rows], history_days)
    top_volatile = []
    for r in rows:
        vm = vol_map.get(r['name']) or {}
        if vm.get('volatilityPct') is not None:
            top_volatile.append({
                'name': r['name'],
                'category': r['category'],
                'volatilityPct': vm.get('volatilityPct'),
                'rangePct': vm.get('rangePct'),
                'trendPct': vm.get('trendPct'),
                'historyPoints': vm.get('points', 0),
            })
    return {
        'latestDate': (portfolio.get('imported_at') or '')[:10],
        'selectedScale': history_days,
        'historyDays': history_days,
        'horizons': [history_days],
        'latestSnapshots': {str(history_days): {'id': portfolio['portfolio_id'], 'label': portfolio['label'], 'date': (portfolio.get('imported_at') or '')[:10]}},
        'duplicates': [],
        'signals': {'broadUp': [], 'broadDown': [], 'reversalUp': [], 'reversalDown': [], 'mixed': []},
        'categories': [],
        'coverage': {
            'snapshots': 0,
            'indexedRows': len(rows),
            'items': len(rows),
            'duplicateWindows': 0,
            'volatilityCoverage': sum(1 for r in rows if (vol_map.get(r['name']) or {}).get('points', 0) >= 2),
        },
        'topVolatile': sorted(top_volatile, key=lambda x: x.get('volatilityPct') or 0, reverse=True)[:20],
    }


def catalog_index():
    global CATALOG_INDEX_CACHE
    # At most one immutable metadata index per process, coalesced across tabs.
    # The short TTL also catches external writers that do not publish revision.
    with CATALOG_INDEX_LOCK:
        with closing(catalog_read_db()) as conn:
            conn.execute('BEGIN')
            revision = conn.execute("SELECT value FROM settings WHERE key='catalog_metadata_revision'").fetchone()
            key = (os.path.abspath(DB_FILE),revision['value'] if revision else None)
            if CATALOG_INDEX_CACHE and CATALOG_INDEX_CACHE[0]==key and time.monotonic()-CATALOG_INDEX_CACHE[1]<30:
                return CATALOG_INDEX_CACHE[2]
            rows = [dict(r) for r in conn.execute(
                """SELECT name, kind, item_type AS type, weapon, collection, rarity,
                          rarity_color AS rarityColor, image, market_hash_name AS marketHashName FROM item_catalog""")]
        exact = {row['name']:row for row in rows}
        base = {}
        for row in rows:
            name = base_item_name(row['name'])
            if name not in base or row['name']==name:
                base[name]=row
        result = (exact,base,len(rows))
        CATALOG_INDEX_CACHE = (key,time.monotonic(),result)
        return result


def enrich_item(row, exact_meta, base_meta):
    row = dict(row)
    exact = exact_meta.get(row['name']) or {}
    base = base_meta.get(row['base_name']) or {}
    meta = {**base, **exact}
    if not meta.get('collection') and base.get('collection'):
        meta['collection'] = base['collection']
    row.update({
        'image': meta.get('image'),
        'rarity': meta.get('rarity'),
        'rarityColor': meta.get('rarityColor'),
        'class': meta.get('weapon') or meta.get('type') or row.get('category'),
        'collection': meta.get('collection'),
        'kind': meta.get('kind'),
        # Family metadata may enrich a variant, but cannot change its listing.
        'marketHashName': exact.get('marketHashName') or row['name'],
    })
    for key in ('qty', 'pct', 'dollar', 'curr', 'past', 'unit_curr', 'unit_past'):
        if row.get(key) is not None:
            row[key] = round(float(row[key]), 2)
    return row


WEAR_RE = re.compile(r'\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred|FN|MW|FT|WW|BS)\)\s*$', re.I)
EVENT_PATTERNS = tuple(re.compile(pat, re.I) for pat in (
    r'(Austin 2025)', r'(Budapest 2025)', r'(Shanghai 2024)', r'(Copenhagen 2024)',
    r'(Paris 2023)', r'(Rio 2022)', r'(Antwerp 2022)', r'(Stockholm 2021)',
    r'(2020 RMR)', r'(Berlin 2019)', r'(Katowice 2019)', r'(London 2018)',
    r'(Boston 2018)', r'(Krakow 2017)', r'(Atlanta 2017)', r'(Cologne 2016)',
    r'(MLG Columbus 2016)', r'(Cologne 2015)', r'(DreamHack 2014)', r'(Cologne 2014)',
    r'(Cluj-Napoca 2015)', r'(Katowice 2014)',
))
WEAPON_FAMILY_PATTERNS = (
    (re.compile(r'AK-47|M4A1-S|M4A4|AWP|SSG 08|SG 553|AUG|FAMAS|Galil|SCAR-20|G3SG1', re.I), 'Rifles'),
    (re.compile(r'Desert Eagle|Glock|USP-S|P2000|P250|Five-SeveN|Tec-9|CZ75|R8|Dual Berettas', re.I), 'Pistols'),
    (re.compile(r'MAC-10|MP5|MP7|MP9|P90|PP-Bizon|UMP-45', re.I), 'SMGs'),
    (re.compile(r'MAG-7|Nova|XM1014|Sawed-Off|M249|Negev', re.I), 'Heavy'),
    (re.compile(r'Knife|Karambit|Bayonet|Butterfly|Daggers|Gloves|Wraps', re.I), 'Knives & Gloves'),
)
KNIFE_WEAPON_RE = re.compile(r'Knife|Karambit|Bayonet|Butterfly|Daggers|Talon|Kukri|M9', re.I)
GLOVE_WEAPON_RE = re.compile(r'Gloves|Wraps|Hand Wraps|Moto Gloves|Driver Gloves|Sport Gloves|Specialist Gloves|Broken Fang Gloves|Hydra Gloves', re.I)


def wear_from_name(name):
    m = WEAR_RE.search(name or '')
    if not m:
        return None
    aliases = {'FN': 'Factory New', 'MW': 'Minimal Wear', 'FT': 'Field-Tested', 'WW': 'Well-Worn', 'BS': 'Battle-Scarred'}
    return aliases.get(m.group(1).upper(), m.group(1))


def event_from_name(name):
    n = name or ''
    for pat in EVENT_PATTERNS:
        m = pat.search(n)
        if m:
            return m.group(1)
    return None


KNIFE_TERMS = (
    'bayonet', 'bowie knife', 'butterfly knife', 'classic knife', 'falchion knife',
    'flip knife', 'gut knife', 'huntsman knife', 'karambit', 'kukri knife',
    'm9 bayonet', 'navaja knife', 'nomad knife', 'paracord knife', 'shadow daggers',
    'skeleton knife', 'stiletto knife', 'survival knife', 'talon knife', 'ursus knife',
)


def is_knife_item(item):
    name = str((item or {}).get('name') or '').lower()
    weapon = str((item or {}).get('weapon') or '').lower()
    text = f"{name} {weapon}"
    return text.startswith('\u2605') or any(term in text for term in KNIFE_TERMS)


def is_event_container(item):
    name = str((item or {}).get('name') or '').lower()
    if event_from_name((item or {}).get('name')):
        return any(term in name for term in ('capsule', 'souvenir package', 'sticker', 'viewer pass', 'token'))
    return False


def is_weapon_case_name(name):
    low = str(name or '').lower()
    if not low or is_event_container({'name': name}):
        return False
    if any(term in low for term in ('capsule', 'package', 'graffiti box', 'sticker', 'patch pack')):
        return False
    return 'case' in low or low.endswith(' collection')


def collection_case_alias(collection):
    """Return the case/container name for case-backed skin collections."""
    global COLLECTION_CASE_ALIAS_CACHE
    collection = (collection or '').strip()
    if not collection:
        return None
    if COLLECTION_CASE_ALIAS_CACHE is None:
        alias = {}
        try:
            with db() as conn:
                rows = conn.execute(
                    "SELECT name, collection FROM item_catalog WHERE kind='collections' AND collection IS NOT NULL"
                ).fetchall()
            for row in rows:
                case_name = (row['collection'] or '').strip()
                if case_name and is_weapon_case_name(case_name):
                    alias[(row['name'] or '').strip()] = case_name
        except sqlite3.Error:
            alias = {}
        COLLECTION_CASE_ALIAS_CACHE = alias
    return COLLECTION_CASE_ALIAS_CACHE.get(collection)


def knife_set_name(item):
    text = ' '.join(str((item or {}).get(k) or '') for k in ('collection', 'name')).lower()
    sets = [
        ('gamma', 'Gamma Knives Index'), ('chroma', 'Chroma Knives Index'),
        ('prisma', 'Prisma Knives Index'), ('spectrum', 'Spectrum Knives Index'),
        ('dreams & nightmares', 'Dreams & Nightmares Knives Index'),
        ('fracture', 'Fracture Knives Index'), ('danger zone', 'Danger Zone Knives Index'),
        ('horizon', 'Horizon Knives Index'), ('revolution', 'Revolution Knives Index'),
        ('recoil', 'Recoil Knives Index'), ('shattered web', 'Shattered Web Knives Index'),
        ('snakebite', 'Snakebite Knives Index'), ('operation riptide', 'Riptide Knives Index'),
        ('kilowatt', 'Kilowatt Knives Index'),
    ]
    for needle, label in sets:
        if needle in text:
            return label
    return 'Knife Market Index'


def market_group_tags(item, collection_alias_resolver=None):
    tags = []
    def add(dim, value):
        value = (value or '').strip() if isinstance(value, str) else value
        row = {'dimension': dim, 'name': str(value)} if value else None
        if row and row not in tags:
            tags.append(row)
    kind = item.get('kind')
    item_type = item.get('item_type') or item.get('type')
    if kind in ('skins', 'skin_families'):
        sector = 'Skins'
    elif kind == 'stickers':
        sector = 'Stickers'
    elif kind == 'crates':
        sector = 'Cases & Containers'
    elif kind == 'graffiti':
        sector = 'Graffiti'
    elif kind == 'patches':
        sector = 'Patches'
    elif kind == 'music_kits':
        sector = 'Music Kits'
    elif kind == 'charms':
        sector = 'Charms'
    else:
        sector = item_type or kind
    add('Market Sector', sector)
    weapon = item.get('weapon') or ''
    if weapon:
        for pattern, family in WEAPON_FAMILY_PATTERNS:
            if pattern.search(weapon):
                add('Weapon Family', family)
                break
        add('Weapon', weapon)
    if item_type and item_type.lower() not in {str(sector).lower(), str(kind).lower(), 'skin'}:
        add('Item Class', item_type)
    add('Item Kind', {
        'skins': 'Skin',
        'stickers': 'Sticker',
        'crates': 'Container',
        'graffiti': 'Graffiti',
        'patches': 'Patch',
        'music_kits': 'Music Kit',
        'charms': 'Charm',
    }.get(kind, kind.title() if isinstance(kind, str) else kind))
    name = item.get('name') or ''
    low = name.lower()
    event = event_from_name(name)
    if is_knife_item(item):
        add('Collection', knife_set_name(item))
    elif kind == 'skins' and item.get('collection'):
        resolve_alias = collection_case_alias if collection_alias_resolver is None else collection_alias_resolver
        case_alias = resolve_alias(item.get('collection'))
        add('Collection', case_alias or item.get('collection'))
        if case_alias:
            add('Skin Collection', item.get('collection'))
    elif kind == 'crates' and is_weapon_case_name(name):
        add('Collection', name)
    add('Rarity', item.get('rarity'))
    add('Wear', wear_from_name(name))
    add('Event', event)
    if 'stattrak' in low:
        add('Special', 'StatTrak')
    if 'souvenir' in low:
        add('Special', 'Souvenir')
    if 'holo' in low:
        add('Finish', 'Holo')
    if 'foil' in low:
        add('Finish', 'Foil')
    if 'gold' in low:
        add('Finish', 'Gold')
    if 'fade' in low:
        add('Finish Family', 'Fade')
    if 'doppler' in low:
        add('Finish Family', 'Doppler')
    if kind == 'crates':
        if 'capsule' in low:
            add('Container Type', 'Capsule')
        elif 'souvenir package' in low or 'package' in low:
            add('Container Type', 'Souvenir Package')
        else:
            add('Container Type', 'Case')
    if kind == 'skins':
        if weapon and KNIFE_WEAPON_RE.search(weapon):
            add('Skin Segment', 'Knives')
        elif weapon and GLOVE_WEAPON_RE.search(weapon):
            add('Skin Segment', 'Gloves')
        else:
            add('Skin Segment', 'Weapon Skins')
    price = _positive_float(item.get('latestPrice'))
    if price:
        if price < 1:
            add('Price Band', 'Under $1')
        elif price < 5:
            add('Price Band', '$1-$5')
        elif price < 25:
            add('Price Band', '$5-$25')
        elif price < 100:
            add('Price Band', '$25-$100')
        elif price < 500:
            add('Price Band', '$100-$500')
        else:
            add('Price Band', '$500+')
    vol = item.get('volume')
    try:
        vol = int(vol or 0)
    except (TypeError, ValueError):
        vol = 0
    if vol:
        if vol >= 10000:
            add('Liquidity', 'Very High Volume')
        elif vol >= 1000:
            add('Liquidity', 'High Volume')
        elif vol >= 100:
            add('Liquidity', 'Medium Volume')
        else:
            add('Liquidity', 'Thin Volume')
    return tags


def normalize_group_tags(tags):
    out = []
    seen = set()
    for tag in tags or []:
        if isinstance(tag, dict):
            dim = str(tag.get('dimension') or '').strip()
            name = str(tag.get('name') or '').strip()
        elif isinstance(tag, str) and ':' in tag:
            dim, name = (part.strip() for part in tag.split(':', 1))
        else:
            continue
        if not dim or not name:
            continue
        key = (dim, name)
        if key in seen:
            continue
        seen.add(key)
        out.append({'dimension': dim, 'name': name})
    return out


def group_tag_label(tag):
    if isinstance(tag, dict):
        dim = str(tag.get('dimension') or '').strip()
        name = str(tag.get('name') or '').strip()
        return f'{dim}: {name}' if dim and name else name or dim
    return str(tag or '').strip()


def pct_change(latest, prior):
    latest = _positive_float(latest)
    prior = _positive_float(prior)
    if not latest or not prior:
        return None
    return round(((latest / prior) - 1) * 100, 2)


def configured_bulk_providers():
    return [{
        'id': 'csgotrader',
        'label': 'CSGO Trader static snapshots',
        'configured': True,
        'free': True,
    }]


def normalize_market_source(source):
    source = (source or '').strip().lower()
    if source == 'buff':
        return 'buff163'
    return source


def quote_from_market_price(name, price, volume=None, upstream='market', source='market', updated_at=None, raw=None):
    price = _positive_float(price)
    if not name or not price:
        return None
    updated_at = coerce_market_timestamp(updated_at)
    quote = {
        'name': name,
        'provider': 'market',
        'price': price,
        'median': price,
        'volume': volume,
        'url': steam_market_url(name),
        'raw': json.dumps({
            'source': upstream,
            'market': source,
            'raw': raw or {},
        }, separators=(',', ':')),
        'updated_at': updated_at,
    }
    # A CSGO Trader Steam value is a selected aggregate, not a live order book.
    # Preserve its existing numeric price and capture clock while naming the
    # selector already stored in the raw evidence.
    basis = csgotrader_steam_aggregate_basis(raw) if upstream == 'csgotrader' and source == 'steam' else None
    if basis:
        quote['priceBasis'] = basis
    return quote


def coerce_market_timestamp(value):
    if isinstance(value, str):
        if value.isdigit():
            value = int(value)
        else:
            return parse_iso_ts(value)
    try:
        ts = int(value or time.time())
    except (TypeError, ValueError):
        ts = int(time.time())
    if ts > 100000000000:
        ts = int(ts / 1000)
    return ts


def catalog_reconcile_saved_quote_binding(conn, name, provider, source, metadata_memo=None):
    """Reconcile an existing issue without fabricating a replay observation/capture.

    Qualified ordinary rows take one indexed receipt lookup and no native proof
    work. An explicit-refresh memo can also expose a definition rejected before
    an unchanged quote reaches this function. Both paths use the saved quote.
    """
    if not CATALOG_STORE_ENABLED:
        return None
    scope = [provider, source, name]
    key = 'catalog_native_binding_issue:v1:' + hashlib.sha256(
        json.dumps(scope, ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()
    saved = conn.execute('SELECT value FROM catalog_store_meta WHERE key=?', (key,)).fetchone()
    previous = catalog_validate_quote_binding_receipt(json.loads(saved['value']), scope) if saved else None
    pending = (metadata_memo or {}).get('catalogProjectionUnresolved', {}).get(name)
    if not pending and (previous is None or previous.get('status') == 'resolved'):
        return None
    current = conn.execute('SELECT * FROM market_item_prices WHERE name=? AND provider=?',
                           (name, provider)).fetchone()
    if current is None or current['source'] != source:
        return None  # A different source's newer current row cannot resolve this scope.
    record = dict(current)
    if type(record['updated_at']) is not int or record['updated_at'] <= 0:
        return None  # Preserve the existing replay rule for an unqualified legacy clock.
    reported_hash = (previous or {}).get('reportedMarketHashName')
    resolution, binding_states = {}, {}
    store, item_id = catalog_identity_for_legacy(
        conn, name, market_hash_name=reported_hash or name, source=source,
        unresolved_definition=resolution)
    if item_id is not None:
        binding_states['market_item_prices'] = store.bind_legacy_or_defer(item_id, 'market_item_prices', name, provider=provider)['status']
        binding_states['price_observations'] = store.bind_legacy_or_defer(item_id, 'price_observations', name, provider=provider)['status']
    disposition = (previous or {}).get('currentWriteDisposition') or 'retained_unchanged_current'
    return catalog_record_quote_binding(conn, record, {'marketHashName': reported_hash},
                                         resolution if item_id is None else None,
                                         item_id, disposition, metadata_memo, binding_states=binding_states)


def store_market_quote(conn, quote, source, recalculate=True, metadata_memo=None, snapshot_replay=False):
    if not quote:
        return None
    provider_key = provider_key_from_source(source=source, provider=quote.get('provider'), quote=quote)
    if snapshot_replay:
        expected = market_price_record_from_quote(quote, provider=provider_key, source=source)
        if not expected:
            return None
        current = conn.execute(
            "SELECT * FROM market_item_prices WHERE name=? AND provider=?",
            (quote.get('name'), provider_key),
        ).fetchone()
        if current:
            # Replaying an already ingested representation is not a new market
            # observation. Repair current facts without rewriting any history,
            # and keep the existing no-regression rule for newer current data.
            # INTEGER affinity also permits legacy TEXT/REAL/NULL values. An
            # older replay cannot safely order an unqualified capture clock.
            current_at = current['updated_at']
            if type(current_at) is not int or current_at <= 0:
                catalog_reconcile_saved_quote_binding(conn, quote.get('name'), provider_key, source, metadata_memo)
                return 'unchanged'
            newer = current_at > int(expected['updated_at'] or 0)
            material = tuple(key for key in expected if key not in ('observed_at', 'updated_at'))
            if newer or all(current[key] == expected[key] for key in material):
                catalog_reconcile_saved_quote_binding(conn, quote.get('name'), provider_key, source, metadata_memo)
                return 'unchanged'
        repaired = upsert_market_item_price(conn, quote, provider=provider_key, source=source,
                                            recalculate=recalculate, metadata_memo=metadata_memo)
        return 'repaired' if repaired else 'unchanged'
    observed_at = observation_bucket(quote.get('updated_at'))
    existed = conn.execute(
        "SELECT price, median, volume FROM price_observations WHERE name=? AND provider=? AND observed_at=?",
        (quote.get('name'), provider_key, observed_at),
    ).fetchone()
    price = _positive_float(quote.get('price')) or _positive_float(quote.get('median')) or _positive_float(quote.get('solidPrice'))
    median = _positive_float(quote.get('median')) or price
    def same_num(left, right):
        left = _positive_float(left)
        right = _positive_float(right)
        if left is None and right is None:
            return True
        if left is None or right is None:
            return False
        return abs(left - right) <= 0.000001
    if existed and same_num(existed['price'], price) and same_num(existed['median'], median):
        current = conn.execute(
            "SELECT 1 FROM market_item_prices WHERE name=? AND provider=?",
            (quote.get('name'), provider_key),
        ).fetchone()
        if current:
            catalog_reconcile_saved_quote_binding(conn, quote.get('name'), provider_key, source, metadata_memo)
            return 'unchanged'
    save_price_observation(
        conn,
        quote,
        source=source,
        observed_at=quote.get('updated_at'),
        provider=provider_key,
    )
    upsert_market_item_price(conn, quote, provider=provider_key, source=source,
                             recalculate=recalculate, metadata_memo=metadata_memo)
    return 'updated' if existed else 'inserted'


def csgotrader_sources(preferred_source=None):
    sources = []
    for source in MARKET_SOURCE_PRIORITY:
        source = normalize_market_source(source)
        if source not in sources:
            sources.append(source)
    return [s for s in sources if s in CSGOTRADER_SOURCE_LABELS]


def choose_csgotrader_price(raw_price, source):
    if isinstance(raw_price, (int, float)):
        return {'price': _positive_float(raw_price), 'raw': {'value': raw_price}}
    if not isinstance(raw_price, dict):
        return None
    if source == 'steam':
        rolling = [(key, _positive_float(raw_price.get(key))) for key in ('last_7d', 'last_30d', 'last_90d')]
        rolling = [(key, price) for key, price in rolling if price]
        if rolling:
            baseline = median_value([price for _key, price in rolling])
            last_24h = _positive_float(raw_price.get('last_24h'))
            if last_24h and baseline and 0.7 <= (last_24h / baseline) <= 1.3:
                return {'price': last_24h, 'raw': raw_price, 'basis': 'last_24h_confirmed_by_rolling'}
            last_7d = next((price for key, price in rolling if key == 'last_7d'), None)
            if last_7d and baseline and 0.65 <= (last_7d / baseline) <= 1.55:
                return {'price': last_7d, 'raw': raw_price, 'basis': 'last_7d_rolling_anchor'}
            return {'price': baseline, 'raw': raw_price, 'basis': 'rolling_median_anchor'}
        price = _positive_float(raw_price.get('last_24h'))
        if price:
            return {'price': price, 'raw': raw_price, 'basis': 'last_24h_only'}
        return None
    price = (_positive_float(raw_price.get('price')) or _positive_float(raw_price.get('suggested_price')) or
             _positive_float(raw_price.get('starting_at')) or _positive_float(raw_price.get('highest_order')) or
             _positive_float(raw_price.get('last_24h')) or _positive_float(raw_price.get('last_7d')))
    if not price and isinstance(raw_price.get('starting_at'), dict):
        price = _positive_float(raw_price['starting_at'].get('price'))
    if not price and isinstance(raw_price.get('highest_order'), dict):
        price = _positive_float(raw_price['highest_order'].get('price'))
    if not price:
        return None
    return {'price': price, 'raw': raw_price}


def store_csgotrader_history_points(conn, name, raw_price, source, observed_at):
    if source != 'steam' or not isinstance(raw_price, dict):
        return 0
    stored = 0
    for key, days in (('last_7d', 7), ('last_30d', 30), ('last_90d', 90)):
        price = _positive_float(raw_price.get(key))
        if not price:
            continue
        quote = quote_from_market_price(
            name,
            price,
            upstream='csgotrader',
            source=source + ':' + key,
            updated_at=observed_at - days * 86400,
            raw=raw_price,
        )
        if save_price_observation(
            conn,
            quote,
            source='csgotrader:' + source + ':' + key,
            observed_at=quote.get('updated_at'),
            provider='steam_snapshot',
        ):
            stored += 1
    return stored


def market_universe_bulk_sync_csgotrader(preferred_source=None, warm_history=False, history_limit=None, portfolio_id=None, retry_history_failures=False, progress=None):
    BULK_STAGING_RUNTIME.require_bulk_mode()
    legacy_bulk_capacity_admission()
    started = time.perf_counter()
    timings = {}
    sources = csgotrader_sources(preferred_source)
    if not sources:
        return {'ok': False, 'error': 'No supported free CSGO Trader source selected.', 'provider': 'csgotrader', 'status': market_universe_status()}
    count = 0
    inserted = 0
    updated = 0
    seen = 0
    by_source = {}
    replayed_sources = []
    errors = []
    unchanged = 0
    unpriced_discovery = {}
    rebuilt_lookups = 0
    rebuilt_metrics = 0
    removed_stale = 0
    repaired_current = 0
    changed_primary_names = set()
    observed_at = int(time.time())
    source_payloads = []
    stage_start = time.perf_counter()
    for source in sources:
        url = f"{CSGOTRADER_PRICE_BASE}/{source}.json"
        try:
            if progress:
                progress('provider_fetch', f"Fetching {CSGOTRADER_SOURCE_LABELS.get(source, source)} snapshot", source=source)
            req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip', 'User-Agent': f'{APP_NAME}/{APP_VERSION}'})
            fetch_metadata = {}
            raw = fetch_json_request(req, timeout=120, metadata=fetch_metadata)
        except Exception as exc:
            errors.append({'source': source, 'error': str(exc)})
            if progress:
                progress('provider_error', f"{CSGOTRADER_SOURCE_LABELS.get(source, source)} snapshot failed", source=source, error=str(exc))
            continue
        if not isinstance(raw, dict):
            errors.append({'source': source, 'error': 'Price snapshot was not a JSON object'})
            if progress:
                progress('provider_error', f"{CSGOTRADER_SOURCE_LABELS.get(source, source)} snapshot was not usable JSON", source=source)
            continue
        seen += len(raw)
        source_payloads.append((source, raw, fetch_metadata))
        if progress:
            progress('provider_received', f"Received {len(raw):,} {CSGOTRADER_SOURCE_LABELS.get(source, source)} rows", source=source, rows=len(raw))
    timings['fetchProviderSnapshotsSeconds'] = round(time.perf_counter() - stage_start, 2)
    stage_start = time.perf_counter()
    memory_progress = (progress if getattr(progress, '_memory_only', False)
                       else getattr(progress, '_memory_only_callback', None))
    if memory_progress:
        memory_progress('waiting_for_writer', 'Waiting to process provider snapshots')
    source_transaction_started = time.perf_counter()
    derived_publication = None
    with SQLITE_WRITE_LOCK:
        legacy_bulk_capacity_admission()
        with db() as conn:
            if memory_progress:
                memory_progress('provider_transaction_started', 'Processing provider snapshots; commit pending')
            # This memo never escapes this one writer connection/transaction.
            # No global metadata refresh/repair scan runs inside the provider-row loop.
            metadata_memo = {'connection': conn, 'names': {}, 'trackMetadataChanges': True}
            for source, raw, fetch_metadata in source_payloads:
                provider_key = provider_key_from_source(source='csgotrader:' + source, provider=source)
                source_observed_at, content_replayed = provider_snapshot_observation(
                    fetch_metadata, provider_fetch_receipt(conn, provider_key), observed_at)
                if content_replayed:
                    replayed_sources.append(source)
                valid_names = set()
                before_source_count = count
                for name, raw_price in raw.items():
                    if not provider_listing_name_is_usable(name):
                        unpriced_discovery['rejected_name'] = unpriced_discovery.get('rejected_name', 0) + 1
                        continue
                    chosen = choose_csgotrader_price(raw_price, source)
                    if not chosen or not _positive_float(chosen.get('price')):
                        discovery = discover_unpriced_market_item(conn, name, metadata_memo)
                        unpriced_discovery[discovery] = unpriced_discovery.get(discovery, 0) + 1
                        continue
                    if name.startswith('Sticker Slab | '):
                        # Price availability is not evidence of metadata origin.
                        # Reuse the same guarded repair before a memoized ensure.
                        discover_unpriced_market_item(conn, name, metadata_memo)
                    quote = quote_from_market_price(
                        name,
                        chosen['price'],
                        upstream='csgotrader',
                        source=source,
                        updated_at=source_observed_at,
                        raw={**(chosen.get('raw') or {}), 'chosen_basis': chosen.get('basis')},
                    )
                    canonical_name = ensure_market_item(conn, quote.get('name'), metadata_memo=metadata_memo) or quote.get('name')
                    if canonical_name != quote.get('name'):
                        quote = {**quote, 'name': canonical_name}
                    stored_state = store_market_quote(conn, quote, 'csgotrader:' + source,
                                                      recalculate=False, metadata_memo=metadata_memo,
                                                      snapshot_replay=content_replayed)
                    valid_names.add(canonical_name)
                    if stored_state:
                        count += 1
                        if stored_state == 'inserted':
                            inserted += 1
                            changed_primary_names.add(canonical_name)
                            store_csgotrader_history_points(conn, name, raw_price, source, source_observed_at)
                        elif stored_state == 'updated':
                            updated += 1
                            changed_primary_names.add(canonical_name)
                            store_csgotrader_history_points(conn, name, raw_price, source, source_observed_at)
                        elif stored_state == 'repaired':
                            repaired_current += 1
                            changed_primary_names.add(canonical_name)
                        elif stored_state == 'unchanged':
                            unchanged += 1
                        by_source[source] = by_source.get(source, 0) + 1
                removed_stale += remove_missing_provider_current_prices(
                    conn, provider_key, valid_names, as_of=source_observed_at if content_replayed else None)
                save_provider_fetch_receipt(conn, provider_key, source, fetch_metadata,
                                            received_rows=len(raw), valid_rows=len(valid_names),
                                            observation_as_of=source_observed_at, content_replayed=content_replayed)
                if memory_progress:
                    memory_progress(
                        'provider_stored',
                        f"Processed {count - before_source_count:,} {CSGOTRADER_SOURCE_LABELS.get(source, source)} current-price rows; commit pending",
                        source=source,
                        stored=count - before_source_count,
                        validNames=len(valid_names),
                    )
            changed_primary_names.update(metadata_memo.get('catalogChangedNames', ()))
            if changed_primary_names:
                for changed_name in sorted(changed_primary_names):
                    recalculate_market_item_primary(conn, changed_name)
            timings['storeProviderRowsSeconds'] = round(time.perf_counter() - stage_start, 2)
            stage_start = time.perf_counter()
            material_changes = inserted + updated + removed_stale + repaired_current
            catalog_changes = len(metadata_memo.get('catalogChangedNames', ()))
            needs_derived_rebuild = material_changes or catalog_changes or any(
                market_cache_state(conn,window)['rebuildRequired'] for window in RESEARCH_METRIC_WINDOWS)
            if needs_derived_rebuild:
                if material_changes or catalog_changes:
                    mark_market_source_changed(conn)
                    cleanup_market_universe(conn)
                if bulk_staging_enabled(conn):
                    bulk_staging_queue_pending(conn)
                else:
                    if memory_progress:
                        memory_progress('rebuilding_metrics', 'Rebuilding every supported research period from shared inputs; commit pending', windowDays=None)
                    metric_counts = rebuild_market_metric_windows(conn, RESEARCH_METRIC_WINDOWS)
                    rebuilt_metrics += sum(metric_counts.values())
                    for window in RESEARCH_METRIC_WINDOWS:
                        rebuilt_lookups += rebuild_market_item_lookup(conn, window)
                    conn.execute(
                        "INSERT OR REPLACE INTO settings(key,value) VALUES('metric_build_version',?)",
                        (str(METRIC_BUILD_VERSION),),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO settings(key,value) VALUES('lookup_build_version',?)",
                        (str(LOOKUP_BUILD_VERSION),),
                    )
            timings['rebuildMetricsLookupSeconds'] = round(time.perf_counter() - stage_start, 2)
    timings['sourceTransactionSeconds'] = round(time.perf_counter() - source_transaction_started, 3)
    if BULK_STAGING_HOST is not None:
        with closing(steam_ask_db()) as stage_probe:
            staging_selected = bulk_staging_enabled(stage_probe)
        if staging_selected:
            if progress:
                progress('source_transaction_committed', 'Source snapshot committed; derived publication pending', commitState='source_committed_derived_pending', processedRows=count)
            derived_publication = BULK_STAGING_RUNTIME.request(timings)
            timings['derivedStagingSeconds'] = derived_publication.get('stagingSeconds')
            timings['derivedPublicationSeconds'] = derived_publication.get('publicationSeconds')
    if progress and derived_publication is None:
        progress('provider_transaction_committed', 'Provider and derived-data transaction committed', processedRows=count)
    ok = count > 0 or catalog_changes > 0
    steam_history_warm = {'attempted': 0, 'refreshed': 0, 'errors': [], 'enabled': False}
    stage_start = time.perf_counter()
    if ok and warm_history:
        steam_history_warm = warm_steam_history_after_market_sync(
            limit=history_limit,
            portfolio_id=portfolio_id,
            force=False,
            retry_failures=retry_history_failures,
            progress=progress,
        )
    timings['warmSteamChartsSeconds'] = round(time.perf_counter() - stage_start, 2)
    stage_start = time.perf_counter()
    if derived_publication is None:
        with SQLITE_WRITE_LOCK:
            with closing(db()) as receipt_conn, receipt_conn:
                save_market_read_receipt(receipt_conn,'market_status_receipt',_collect_market_universe_status())
    status = market_universe_status()
    if derived_publication is not None:
        status = dict(status, derivedPublication=derived_publication,
                      commitState=derived_publication.get('state','source_committed_derived_pending'))
    timings['statusSeconds'] = round(time.perf_counter() - stage_start, 2)
    timings['totalSeconds'] = round(time.perf_counter() - started, 2)
    return {
        'ok': ok,
        'provider': 'csgotrader',
        'derivedPublication': derived_publication,
        'preferredSource': 'all',
        'sourcesPulled': sources,
        'itemsSeen': seen,
        'itemsStored': count,
        'newObservationRows': inserted,
        'updatedObservationRows': updated,
        'unchangedObservationRows': unchanged,
        'unpricedCatalog': {
            'newNames': len(metadata_memo.get('catalogNewNames', ())),
            'changedNames': catalog_changes,
            'metadataChangeScope': 'all ensured names, including priced metadata-row repairs',
            'discoveryOutcomes': unpriced_discovery,
            'nativeBindingUnresolvedReceipts': len(metadata_memo.get('nativeBindingUnresolvedReceipts', ())),
            'nativeBindingPendingReceipts': len(metadata_memo.get('nativeBindingPendingReceipts', ())),
            'nativeBindingResolvedReceipts': len(metadata_memo.get('nativeBindingResolvedReceipts', ())),
            'nativeBindingSemantics': 'legacy quote/history retained; unresolved definitions are not promoted',
            'outcomeCountSemantics': 'provider rows; the same name may occur in several snapshots',
            'priceSemantics': 'discovery preserves unknown prices; valid provider-price removal remains unchanged',
        },
        'removedStaleCurrentRows': removed_stale,
        'rebuiltLookups': rebuilt_lookups,
        'rebuiltMetrics': rebuilt_metrics,
        'bySource': by_source,
        'replayedSources': replayed_sources,
        'repairedCurrentRows': repaired_current,
        'observationPolicy': 'one_capture_per_consecutive_decoded_body_and_ingestion_version',
        'steamHistoryWarm': steam_history_warm,
        'errors': errors,
        'timings': timings,
        'status': status,
        'error': None if ok else 'No prices were stored from the free CSGO Trader snapshots.',
    }


def _collect_market_universe_status():
    """Current/materialized summaries for an explicit refresh task only."""
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        source_revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
        derived_rebuild_required = any(market_cache_state(conn,window)['rebuildRequired'] for window in VOLATILITY_DETAIL_WINDOWS)
        game_catalog_count = conn.execute("SELECT COUNT(*) FROM item_catalog").fetchone()[0]
        provider_marks = ','.join('?' for _ in CURRENT_PRICE_PROVIDERS)
        market_item_count = conn.execute("SELECT COUNT(*) FROM market_items").fetchone()[0]
        metadata_count = conn.execute(
            """SELECT COUNT(*)
                FROM market_items
                WHERE TRIM(COALESCE(kind,'') || COALESCE(item_type,'') ||
                           COALESCE(weapon,'') || COALESCE(collection,'') ||
                           COALESCE(rarity,'') || COALESCE(image,'')) <> ''"""
        ).fetchone()[0]
        quote_count = conn.execute("SELECT COUNT(*) FROM market_items WHERE primary_price IS NOT NULL").fetchone()[0]
        obs_count = None
        observed_items = quote_count
        market_universe_count = max(market_item_count, game_catalog_count, observed_items)
        latest_obs = conn.execute(
            f"SELECT MAX(updated_at) FROM market_item_prices WHERE provider IN ({provider_marks})",
            CURRENT_PRICE_PROVIDERS,
        ).fetchone()[0] or 0
        source_rows = [dict(r) for r in conn.execute("""
            SELECT provider AS source, COUNT(*) AS rows, COUNT(*) AS items,
                   MAX(updated_at) AS latestAt
            FROM market_item_prices
            GROUP BY provider
            ORDER BY items DESC, rows DESC
        """)]
        all_provider_price_rows = sum(int(row.get('rows') or 0) for row in source_rows)
        source_rows = [row for row in source_rows if row['source'] in CURRENT_PRICE_PROVIDERS][:12]
        source_map = {row['source']: row for row in source_rows}
        provider_coverage = []
        for provider in CURRENT_PRICE_PROVIDERS:
            row = source_map.get(provider, {})
            fetch_receipt = provider_fetch_receipt(conn, provider)
            items = int(row.get('items') or 0)
            latest = int(row.get('latestAt') or 0)
            provider_coverage.append({
                'id': provider,
                'label': PRICE_PROVIDER_LABELS.get(provider, provider),
                'items': items,
                'rows': int(row.get('rows') or 0),
                'missingItems': max(0, market_universe_count - items),
                'coveragePct': round((items / market_universe_count) * 100, 1) if market_universe_count else 0,
                'latestAt': latest,
                'ageSeconds': max(0, int(time.time()) - latest) if latest else None,
                **{key: fetch_receipt.get(key) for key in (
                    'fetchedAt', 'upstreamSnapshotModifiedAt', 'upstreamLastModified',
                    'upstreamEtag', 'receivedRows', 'validRows', 'fetchReceiptState',
                    'upstreamTimeSemantics', 'observationAsOf', 'contentReplayed', 'ingestionVersion')},
            })
        price_history_rows = price_history_items = None
        steam_history_failures = conn.execute(
            """SELECT COUNT(*) FROM steam_history_failures
               WHERE failed_at >= ? AND COALESCE(fetch_version, 0)=?""",
            (int(time.time()) - STEAM_HISTORY_FAILURE_TTL, STEAM_HISTORY_FETCH_VERSION),
        ).fetchone()[0]
        metric_version = conn.execute("SELECT value FROM settings WHERE key='metric_build_version'").fetchone()
        metric_build_current = bool(metric_version and str(metric_version['value']) == str(METRIC_BUILD_VERSION))
        metric_rows = [dict(r) for r in conn.execute(
            """SELECT window_days, COUNT(*) AS rows, COUNT(DISTINCT name) AS items,
                      MAX(updated_at) AS latestAt
               FROM item_market_metrics m
               WHERE m.provider='market' AND window_days IN (7,30,90)
               GROUP BY window_days
               ORDER BY window_days"""
        )]
        lookup_rows = [dict(r) for r in conn.execute(
            """SELECT window_days, COUNT(*) AS rows, MAX(latest_at) AS latestAt
               FROM market_item_lookup
               WHERE window_days IN (7,30,90)
               GROUP BY window_days
               ORDER BY window_days"""
        )]
        lookup_total = conn.execute("SELECT COUNT(*) FROM market_item_lookup").fetchone()[0]
        for row in metric_rows:
            verified = metric_window_is_current(conn,int(row['window_days']))
            row['currentRows'] = int(row.get('rows') or 0) if verified else None
            row['cacheState'] = 'current' if verified else 'stale'
        verified_metrics = all(row['currentRows'] is not None for row in metric_rows)
        current_metric_rows = sum(int(row['currentRows']) for row in metric_rows) if verified_metrics else None
        current_metric_items = max((int(row.get('items') or 0) for row in metric_rows),default=0) if verified_metrics else None
        metric_total_rows = sum(int(r.get('rows') or 0) for r in metric_rows)
        gaps = []
        def add_gap(gap_id, label, missing, total, action, severity='warn'):
            missing = int(missing or 0)
            total = int(total or 0)
            if missing <= 0:
                return
            gaps.append({
                'id': gap_id,
                'label': label,
                'missing': missing,
                'total': total,
                'coveragePct': round(((total - missing) / total) * 100, 1) if total else 0,
                'action': action,
                'severity': severity,
            })
        add_gap(
            'current_prices',
            'Items without any current provider price',
            max(0, market_universe_count - observed_items),
            market_universe_count,
            'Pull Market Data downloads every supported CSGO Trader source. After a fresh pull, remaining names are not supplied by the configured provider snapshots.',
            'critical',
        )
        add_gap(
            'metadata',
            'Items without local metadata',
            max(0, market_universe_count - metadata_count),
            market_universe_count,
            'Refresh catalog metadata, then pull market data again. Provider-only names are also classified locally when possible.',
        )
        add_gap(
            'metric_windows',
            'Items without current 7d/30d/90d metric rows',
            max(0, (observed_items * len(VOLATILITY_DETAIL_WINDOWS)) - current_metric_rows) if current_metric_rows is not None else 0,
            observed_items * len(VOLATILITY_DETAIL_WINDOWS),
            'Pull Market Data explicitly rebuilds metric rows. Page reads serve saved metrics.',
        )
    return {
        'sourceRevision':source_revision['value'] if source_revision else None,
        'derivedRebuildRequired':derived_rebuild_required,
        'countsAsOf':int(time.time()),'countState':'saved_current_summary_history_not_counted',
        'catalogItems': market_universe_count,
        'marketItems': market_universe_count,
        'gameCatalogItems': game_catalog_count,
        'catalogMatchedItems': metadata_count,
        'metadataCoveragePct': round((metadata_count / market_universe_count) * 100, 1) if market_universe_count else 0,
        'quotedItems': quote_count,
        'observedItems': observed_items,
        'observationRows': obs_count,
        'latestObservationAt': latest_obs,
        'coveragePct': round((observed_items / market_universe_count) * 100, 1) if market_universe_count else 0,
        'bulkProvider': 'csgotrader',
        'bulkConfigured': True,
        'bulkProviders': configured_bulk_providers(),
        'steamCookieConfigured': bool(steam_cookie_header()),
        'sources': source_rows,
        'providerCoverage': provider_coverage,
        'gaps': gaps,
        'stores': {
            'catalogItems': game_catalog_count,
            'marketItems': market_item_count,
            'providerPriceRows': sum(int(r.get('rows') or 0) for r in source_rows),
            'allProviderPriceRows': all_provider_price_rows,
            'providerObservationRows': obs_count,
            'steamHistoryRows': price_history_rows,
            'steamHistoryItems': price_history_items,
            'steamHistoryRecentFailures': steam_history_failures,
            'metricRows': metric_total_rows,
            'currentMetricRows': current_metric_rows,
            'metricItems': current_metric_items,
            'lookupRows': lookup_total,
        },
        'metrics': {
            'buildVersion': METRIC_BUILD_VERSION,
            'items': current_metric_items,
            'rows': current_metric_rows,
            'windows': metric_rows,
        },
        'lookup': {
            'buildVersion': LOOKUP_BUILD_VERSION,
            'rows': lookup_total,
            'windows': lookup_rows,
        },
        'refreshPlan': {
            'primaryAction': 'Pull Market Data',
            'description': 'One pull refreshes every supported CSGO Trader provider snapshot, appends local observations, rebuilds 7d/30d/90d metrics, and warms priority Steam chart histories.',
            'automaticRefreshHours': round(MARKET_REFRESH_INTERVAL_SECONDS / 3600, 1),
            'autoRefreshEnabled': AUTO_MARKET_REFRESH,
        },
        'dataset': {
            'id': f"market-items:{latest_obs}:{market_universe_count}",
            'source': 'local market database',
            'asOf': latest_obs,
            'observedItems': observed_items,
            'observationRows': obs_count,
            'implication': 'Portfolios are subsets of this local market database. External calls append/update provider rows; page reads come from SQLite.',
        },
        'tasks': data_task_snapshot(),
    }


def project_refresh_status(status, current_revision=None):
    """Fields consumed by the status poll; manual tasks use /api/data-tasks."""
    def pick(value, fields):
        value = value or {}
        return {key: value[key] for key in fields if key in value}
    result = pick(status, ('catalogItems', 'observedItems', 'observationRows', 'latestObservationAt',
        'steamCookieConfigured', 'sourceRevision', 'countState', 'countsAsOf',
        'dataState', 'cacheState', 'rebuildRequired', 'autoRefreshStatus'))
    result.update(projection='refresh-status-v1', currentSourceRevision=current_revision)
    result['derivedPublication'] = pick(status.get('derivedPublication'), ('state','derivedCurrent',
        'publishedGeneration','asOf','workerRunning','paused','stopping','sourceTransactionSeconds',
        'stagingSeconds','publicationSeconds','lastCycleSeconds'))
    result['derivedCountsAsOf'] = status.get('derivedCountsAsOf')
    result['sourceCountsCurrent'] = status.get('sourceCountsCurrent')
    result['freshness'] = pick(status.get('freshness'), ('sourceAsOf', 'countsAsOf', 'currentness'))
    result['stores'] = pick(status.get('stores'), ('providerPriceRows', 'currentMetricRows',
        'steamHistoryItems', 'steamHistoryRows', 'steamHistoryRecentFailures', 'lookupRows'))
    result['metrics'] = pick(status.get('metrics'), ('rows',))
    result['lookup'] = pick(status.get('lookup'), ('rows',))
    result['dataset'] = pick(status.get('dataset'), ('id', 'source', 'asOf'))
    result['refreshPlan'] = pick(status.get('refreshPlan'), ('autoRefreshEnabled', 'automaticRefreshHours'))
    result['sources'] = [pick(row, ('source', 'items', 'rows', 'latestAt')) for row in (status.get('sources') or [])[:8]]
    result['providerCoverage'] = [pick(row, ('id', 'label', 'items', 'rows', 'latestAt', 'coveragePct',
        'fetchedAt', 'fetchedAgeSeconds', 'upstreamSnapshotModifiedAt', 'upstreamSnapshotAgeSeconds',
        'upstreamLastModified', 'upstreamEtag', 'receivedRows', 'validRows', 'fetchReceiptState',
        'upstreamTimeSemantics', 'observationAsOf', 'contentReplayed', 'ingestionVersion'))
                                  for row in status.get('providerCoverage') or []]
    result['gaps'] = [pick(row, ('severity', 'label', 'action', 'missing', 'coveragePct'))
                      for row in (status.get('gaps') or [])[:6]]
    return result


def market_universe_status(projection=None):
    """Serve a persisted small summary; never aggregate raw history on a GET."""
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        receipt = market_read_receipt(conn,'market_status_receipt')
        row = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
        generation_status = getattr(conn, '_derived_generation_status', None)
        if generation_status and generation_status != 'legacy_canonical':
            metadata = getattr(conn, '_derived_generation_metadata', {})
            derived_state = {'current': generation_status == 'current_generation'}
            current_counts = derived_state['current']
            counts = metadata.get('counts', {})
            derived_view = {
                'derivedPublication': {'state': generation_status,
                    'derivedCurrent': current_counts,
                    'publishedGeneration': getattr(conn, '_derived_generation', None),
                    'asOf': metadata.get('asOf')},
                'derivedCountsAsOf': metadata.get('asOf'),
                'derivedRebuildRequired': not current_counts,
                'metrics': {'rows': counts.get('item_market_metrics')},
                'lookup': {'rows': counts.get('market_item_lookup')},
                'stores': {'metricRows': counts.get('item_market_metrics'),
                    'currentMetricRows': counts.get('item_market_metrics') if current_counts else None,
                    'lookupRows': counts.get('market_item_lookup')},
            }
        else:
            derived_state = bulk_derived_staging.published_state(conn, BULK_STAGING_HOST.engine.external) if bulk_staging_enabled(conn) else None
            derived_view = BULK_STAGING_RUNTIME.decorate_saved(conn, receipt) if derived_state is not None else None
    current = bool(receipt and row and receipt.get('sourceRevision')==row['value'])
    if derived_state is not None:
        current = current and derived_state['current']
    if not receipt:
        receipt = {key:None for key in ('catalogItems','marketItems','gameCatalogItems','catalogMatchedItems',
            'metadataCoveragePct','quotedItems','observedItems','observationRows','latestObservationAt','coveragePct')}
        receipt.update({'sources':[],'providerCoverage':[],'gaps':[],
            'stores':{key:None for key in ('catalogItems','marketItems','providerPriceRows','allProviderPriceRows','providerObservationRows',
                'steamHistoryRows','steamHistoryItems','steamHistoryRecentFailures','metricRows','currentMetricRows','metricItems','lookupRows')},
            'metrics':{'buildVersion':METRIC_BUILD_VERSION,'items':None,'rows':None,'windows':[]},
            'lookup':{'buildVersion':LOOKUP_BUILD_VERSION,'rows':None,'windows':[]},
            'dataset':{'id':'market-items:unprepared','source':'saved market data','asOf':None,'observedItems':None,'observationRows':None},
            'countState':'not_counted','countsAsOf':None})
    if derived_view is not None:
        receipt = dict(receipt)
        for key in ('derivedPublication','derivedCountsAsOf','derivedRebuildRequired'):
            if key in derived_view:
                receipt[key] = derived_view[key]
        for key in ('metrics','lookup','stores'):
            if key in derived_view:
                receipt[key] = {**receipt.get(key,{}), **derived_view[key]}
    now = int(time.time())
    for provider in receipt.get('providerCoverage') or []:
        latest = provider.get('latestAt')
        provider['ageSeconds'] = max(0,now-int(latest)) if latest else None
        fetched = provider.get('fetchedAt')
        modified = provider.get('upstreamSnapshotModifiedAt')
        provider['fetchedAgeSeconds'] = max(0, now-int(fetched)) if fetched is not None else None
        provider['upstreamSnapshotAgeSeconds'] = max(0, now-int(modified)) if modified is not None else None
    compact = projection == 'refresh-status-v1'
    if derived_state is not None:
        receipt = dict(receipt, derivedPublication=(derived_view or {}).get('derivedPublication',derived_state))
    result = {**receipt,'bulkProvider':'csgotrader','bulkConfigured':True,
        'bulkProviders':configured_bulk_providers(),'steamCookieConfigured':bool(steam_cookie_header()),
        'tasks':None if compact else data_task_snapshot(),'autoRefreshStatus':automatic_refresh_snapshot(),'dataState':'saved','cacheState':'current' if current else ('stale' if receipt.get('countsAsOf') else 'missing'),
        'rebuildRequired':(not derived_state['current']) if derived_state is not None else (not current or receipt.get('derivedRebuildRequired',True)),
        'sourceCountsCurrent':bool(receipt and row and receipt.get('sourceRevision')==row['value']),
        'freshness':{'sourceAsOf':receipt.get('latestObservationAt'),'countsAsOf':receipt.get('countsAsOf'),
                     'currentness':'matched_writer_revision' if current else 'unverified_saved_receipt'},
        'refreshPlan':{'primaryAction':'Pull Market Data','description':'Explicit tasks refresh provider data and saved metrics; viewing does not start work.',
                       'automaticRefreshHours':round(MARKET_REFRESH_INTERVAL_SECONDS/3600,1),'autoRefreshEnabled':AUTO_MARKET_REFRESH}}

    if compact:
        return project_refresh_status(result, row['value'] if row else None)
    return result


def application_status():
    status = market_universe_status()
    stores = status.get('stores') or {}
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        snaps = conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0]
        portfolios = conn.execute('SELECT COUNT(*) FROM portfolios').fetchone()[0]
    return {'ok':True,'snaps':snaps,'portfolios':portfolios,
        'catalog':stores.get('catalogItems'),'marketItems':stores.get('marketItems'),'prices':stores.get('allProviderPriceRows'),
        'priceHistoryRows':None,'priceObservationRows':None,'sharedPriceRows':None,'sharedPriceHistoryRows':None,
        'sharedPriceObservationRows':None,'indexedRows':None,'portfolioItems':None,
        'countState':status.get('countState'),'historyCountState':'not_counted','freshness':status.get('freshness'),
        'dataState':'saved','cacheState':status.get('cacheState'),'rebuildRequired':status.get('rebuildRequired'),
        'data_dir':DATA_DIR,'database':DB_FILE}


def shared_pricing_status():
    with closing(catalog_read_db()) as conn:
        receipt = market_read_receipt(conn,'shared_status_receipt')
    return {'ok':True,'db':DB_FILE,'lanes':(receipt or {}).get('lanes',[]),
        'items':(receipt or {}).get('items'),'currentRows':(receipt or {}).get('currentRows'),
        'observationRows':None,'historyRows':None,'sourceQuoteRows':(receipt or {}).get('sourceQuoteRows'),
        'latestAt':(receipt or {}).get('latestAt'),'dataState':'saved','cacheState':'cached' if receipt else 'missing',
        'countState':'saved_receipt' if receipt else 'not_counted','historyCountState':'not_counted'}


def shared_item_pricing(name, history_limit=500):
    name = (name or '').strip()
    if not name:
        return {'ok': False, 'error': 'Missing name'}
    history_limit = parse_int_param(history_limit, 500, minimum=1, maximum=5000)
    with db() as conn:
        prices = [dict(r) for r in conn.execute(
            """SELECT name, market_hash_name, lane, price, confidence, confidence_tier,
                      source_count, history_source, history_points, method, raw, updated_at
               FROM shared_item_prices
               WHERE name=?
               ORDER BY lane""",
            (name,),
        )]
        sources = [dict(r) for r in conn.execute(
            """SELECT source, market_class AS marketClass, price_kind AS priceKind,
                      price, confidence, source_url AS sourceUrl, raw, observed_at AS observedAt,
                      updated_at AS updatedAt
               FROM shared_source_quotes
               WHERE name=?
               ORDER BY source""",
            (name,),
        )]
        history = [dict(r) for r in conn.execute(
            """SELECT lane, source, point_ts AS ts, point_time AS time, price, volume, raw, updated_at AS updatedAt
               FROM shared_price_history
               WHERE name=?
               ORDER BY lane, source, point_ts DESC
               LIMIT ?""",
            (name, history_limit),
        )]
        observations = [dict(r) for r in conn.execute(
            """SELECT lane, observed_at AS observedAt, price, confidence, source_count AS sourceCount,
                      raw, updated_at AS updatedAt
               FROM shared_price_observations
               WHERE name=?
               ORDER BY observed_at DESC
               LIMIT ?""",
            (name, history_limit),
        )]
    return {
        'ok': True,
        'name': name,
        'prices': {
            r['lane']: {
                'price': r.get('price'),
                'confidence': r.get('confidence'),
                'confidenceTier': r.get('confidence_tier'),
                'sourceCount': r.get('source_count'),
                'historySource': r.get('history_source'),
                'historyPoints': r.get('history_points'),
                'method': r.get('method'),
                'updatedAt': r.get('updated_at'),
                'raw': _json_loads_or(r.get('raw'), {}),
            }
            for r in prices
        },
        'sourceQuotes': [
            {**r, 'raw': _json_loads_or(r.get('raw'), {})}
            for r in sources
        ],
        'history': [
            {**r, 'raw': _json_loads_or(r.get('raw'), {})}
            for r in reversed(history)
        ],
        'observations': [
            {**r, 'raw': _json_loads_or(r.get('raw'), {})}
            for r in reversed(observations)
        ],
    }


def market_universe_sync_batch(offset=0, limit=20, force=False, kind=None):
    refresh_catalog(False)
    offset = parse_int_param(offset, 0, minimum=0, maximum=1000000)
    limit = parse_int_param(limit, 20, minimum=1, maximum=100)
    params = []
    where = ''
    if kind:
        where = 'WHERE kind=?'
        params.append(kind)
    with db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM item_catalog {where}", params).fetchone()[0]
        rows = [dict(r) for r in conn.execute(
            f"""SELECT name, market_hash_name AS marketHashName, kind, item_type, weapon, collection, rarity
                FROM item_catalog {where}
                ORDER BY name LIMIT ? OFFSET ?""",
            params + [limit, offset],
        )]
    names = [r.get('marketHashName') or r.get('name') for r in rows if r.get('marketHashName') or r.get('name')]
    quote_result = get_price_quotes(names, max_names=len(names), force=force) if names else {'quotes': {}}
    quotes = quote_result.get('quotes') or {}
    priced = sum(1 for q in quotes.values() if q.get('price') or q.get('median'))
    status = market_universe_status()
    next_offset = min(total, offset + len(rows))
    return {
        'ok': True,
        'offset': offset,
        'limit': limit,
        'processed': len(rows),
        'priced': priced,
        'nextOffset': next_offset,
        'done': next_offset >= total,
        'total': total,
        'kind': kind,
        'status': status,
        'coverage': {
            'items': total,
            'catalogItems': status['catalogItems'],
            'observedItems': status['observedItems'],
            'observationRows': status['observationRows'],
            'marketCoverage': status['observedItems'],
            'marketCoveragePct': status['coveragePct'],
        },
    }


def market_universe_bulk_sync(provider='auto', preferred_source=None, warm_history=False, history_limit=None, portfolio_id=None, retry_history_failures=False, progress=None):
    provider = (provider or 'auto').lower()
    if provider in ('auto', 'free', 'csgotrader'):
        return market_universe_bulk_sync_csgotrader(
            preferred_source,
            warm_history=warm_history,
            history_limit=history_limit,
            portfolio_id=portfolio_id,
            retry_history_failures=retry_history_failures,
            progress=progress,
        )
    return {
        'ok': False,
        'error': 'Unsupported market provider. Use the free CSGO Trader snapshot provider.',
        'provider': provider,
        'status': market_universe_status(),
    }


def compact_market_item_row(row):
    groups = row.get('groups') or []
    group_labels = [label for label in (group_tag_label(g) for g in groups) if label]
    divergence = provider_divergence_signal(row.get('providerPrices') or {})
    compact = {
        'name': row.get('name'),
        'marketHashName': row.get('marketHashName') if row.get('marketHashName') != row.get('name') else None,
        'kind': row.get('kind'),
        'type': row.get('type'),
        'weapon': row.get('weapon'),
        'collection': row.get('collection'),
        'rarity': row.get('rarity'),
        'latestPrice': row.get('latestPrice'),
        'steamPrice': row.get('steamPrice'),
        'lowestSellOrder': row.get('lowestSellOrder'),
        'highestBuyOrder': row.get('highestBuyOrder'),
        'orderBookSpreadPct': row.get('orderBookSpreadPct'),
        'priceStabilityPct': row.get('priceStabilityPct'),
        'priceBasis': row.get('priceBasis'),
        'confidencePct': row.get('confidencePct'),
        'providerPrices': row.get('providerPrices'),
        'realWorldPrice': divergence.get('realWorldPrice'),
        'realWorldProvider': divergence.get('realWorldProvider'),
        'realWorldProviderLabel': divergence.get('realWorldProviderLabel'),
        'steamPremiumPct': divergence.get('steamPremiumPct'),
        'steamPremiumDollar': divergence.get('steamPremiumDollar'),
        'providerDivergencePct': divergence.get('providerDivergencePct'),
        'providerDivergenceAbsPct': divergence.get('providerDivergenceAbsPct'),
        'providerDivergenceDollar': divergence.get('providerDivergenceDollar'),
        'divergentProvider': divergence.get('divergentProvider'),
        'divergentProviderLabel': divergence.get('divergentProviderLabel'),
        'externalProviderCount': divergence.get('externalProviderCount'),
        'priorPrice': row.get('priorPrice'),
        'changePct': row.get('changePct'),
        'changeDollar': row.get('changeDollar'),
        'volatilityWindows': row.get('volatilityWindows'),
        'volume': row.get('volume'),
        'latestAt': row.get('latestAt'),
        'source': row.get('source'),
        'groups': group_labels[:8],
    }
    return compact


def project_market_item_response(response, projection=''):
    """Opt-in table DTO; preserve the legacy response for every other value."""
    if projection != 'market-table-v1':
        return response
    item_fields = (
        'name', 'marketHashName', 'kind', 'type', 'weapon', 'collection', 'rarity',
        'latestPrice', 'steamPrice', 'priceBasis', 'confidencePct', 'providerPrices',
        'realWorldPrice', 'realWorldProvider', 'realWorldProviderLabel',
        'steamPremiumPct', 'steamPremiumDollar', 'providerDivergencePct',
        'divergentProvider', 'divergentProviderLabel', 'priorPrice', 'changePct',
        'changeDollar', 'volatilityWindows', 'volume', 'latestAt', 'source', 'groups',
    )
    provider_fields = ('price', 'solidPrice', 'label', 'basis', 'updatedAt')
    window_fields = ('volatilityPct', 'trendPct', 'isPartialWindow',
                     'windowCoveragePct', 'metricBasis', 'metricState')

    def fields(value, names):
        # Preserve null/missing objects and null field values without inventing data.
        return {key: value[key] for key in names if key in value} if isinstance(value, dict) else value

    def objects(value, names):
        return {key: fields(row, names) for key, row in value.items()} if isinstance(value, dict) else value

    items = []
    for row in response['items']:
        item = fields(row, item_fields)
        if 'providerPrices' in item:
            item['providerPrices'] = objects(item['providerPrices'], provider_fields)
        if 'volatilityWindows' in item:
            item['volatilityWindows'] = objects(item['volatilityWindows'], window_fields)
        items.append(item)
    return {**response, 'items': items, 'projection': 'market-table-v1'}


def run_data_accumulation_task(task_id, options):
    global ACTIVE_DATA_TASK_ID
    mode = (options.get('mode') or 'accumulate_missing').strip().lower()
    pull_mode = (options.get('pullMode') or 'gentle').strip().lower()
    portfolio_id = options.get('portfolio') or None
    history_scope = str(options.get('historyScope') or 'mixed').strip().lower()
    history_all = options.get('historyAll') is True
    force = bool(options.get('force'))
    retry_failures = bool(options.get('retryFailures', True))
    history_limit = parse_int_param(options.get('historyLimit'), 40 if pull_mode == 'gentle' else 120, minimum=0, maximum=500)
    cycles = parse_int_param(options.get('cycles'), 1 if mode != 'accumulate_missing' else 2, minimum=1, maximum=12)
    sleep_seconds = parse_int_param(options.get('sleepSeconds'), 3 if pull_mode == 'gentle' else 1, minimum=0, maximum=120)

    def progress(stage, message, **payload):
        with DATA_TASK_LOCK:
            state = (DATA_TASKS.get(task_id) or {}).get('status')
        if state in ('paused', 'cancelled') and stage != state:
            return
        data_task_event(task_id, stage, message, **payload)

    def transaction_progress(stage, message, **payload):
        _record_data_task_event(task_id, stage, message, payload, defer_persistence=True)

    progress._memory_only_callback = transaction_progress

    def stop_if_requested():
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            control = (task.get('options') or {}).get('_control')
            state = 'queued' if DATA_TASK_STOP_EVENT.is_set() else control
            if state not in ('paused', 'cancelled', 'queued'):
                return True
            task.update(status=state, updatedAt=int(time.time()))
            if state == 'cancelled':
                task['finishedAt'] = int(time.time())
            else:
                task.pop('finishedAt', None)
        progress('interrupted' if state == 'queued' else state,
                 'Server stopping; explicit task checkpoint remains saved.' if state == 'queued' else 'Explicit data task ' + state + '.')
        return False

    def pause_provider(message):
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            task.update(status='paused', updatedAt=int(time.time()))
            task.setdefault('options', {})['_control'] = 'paused'
            task.pop('finishedAt', None)
        progress('paused', message)
        return False

    def wait_for_provider(history=None):
        if not stop_if_requested():
            return False
        if history is None:
            return True
        if history.get('operatorPaused'):
            return pause_provider('Steam acquisition is operator-paused; the same explicit task remains saved.')
        retry_at = history.get('retryAt')
        now = DATA_TASK_CLOCK()
        if type(retry_at) is not int or retry_at <= now:
            return False  # No valid future deadline: no premature or busy retry.
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            task.update(status='waiting', updatedAt=int(time.time()))
        progress('waiting_provider', 'Waiting for the saved shared Steam admission deadline; this job will continue automatically.', retryAt=retry_at, reason=history.get('reason'))
        while True:
            DATA_TASK_WAKE.clear()
            if not stop_if_requested():
                return False
            pipeline = globals().get('PRICE_PIPELINE')
            if pipeline:
                source = next(row for row in pipeline.book.providers() if row['id'] == 'steam')
                if not source['enabled']:
                    return pause_provider('Steam acquisition was paused while this explicit task waited.')
            remaining = retry_at - DATA_TASK_CLOCK()
            if remaining <= 0:
                with DATA_TASK_LOCK:
                    DATA_TASKS[task_id]['status'] = 'running'
                progress('provider_resumed', 'Saved Steam deadline reached; retrying the same bounded item.', retryAt=retry_at)
                return True
            with DATA_TASK_LOCK:
                options_saved = DATA_TASKS[task_id].setdefault('options', {})
                used = max(0, _finite_float(options_saved.get('_providerWaitSeconds')) or 0)
            if used >= DATA_TASK_MAX_WAIT_SECONDS:
                return pause_provider('The explicit task reached its 24-hour provider-wait budget; its checkpoint remains saved for owner resume.')
            duration = min(60, remaining, DATA_TASK_MAX_WAIT_SECONDS - used)
            before = DATA_TASK_CLOCK()
            DATA_TASK_WAKE.wait(duration)
            elapsed = max(0, DATA_TASK_CLOCK() - before)
            with DATA_TASK_LOCK:
                DATA_TASKS[task_id].setdefault('options', {})['_providerWaitSeconds'] = used + elapsed

    progress._wait_for_provider = wait_for_provider

    def pending(result, stage, message):
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            task.update(status='pending', stage=stage, updatedAt=int(time.time()),
                        result=_data_task_result_summary(result), error=None)
            task.pop('finishedAt', None)
        progress(stage, message, result=_data_task_result_summary(result))

    def check_history(outcome):
        if not isinstance(outcome, dict):
            return False
        if outcome.get('governorDeferred') or outcome.get('rateLimited'):
            if outcome.get('historyAttemptCapReached'):
                raise RuntimeError('The bounded Steam history HTTP-attempt budget was exhausted; saved observations are retained.')
            if not stop_if_requested():
                return True
            pending(outcome, 'waiting_provider', 'History work remains pending at the shared provider deadline; saved observations remain available.')
            return True
        if outcome.get('failed'):
            detail = (outcome.get('errors') or [{}])[0].get('error') or 'Steam chart acquisition did not return genuine observations'
            raise RuntimeError(detail)
        for value in outcome.values():
            if isinstance(value, dict) and check_history(value):
                return True
        return False

    with DATA_TASK_LOCK:
        task = DATA_TASKS[task_id]
        task['status'] = 'running'
        task['startedAt'] = int(time.time())
        task['updatedAt'] = task['startedAt']
    try:
        if mode not in ('provider_snapshots', 'full_pull', 'accumulate_missing', 'steam_history'):
            raise ValueError('Unsupported explicit data-task mode')
        if history_scope not in ('mixed', 'portfolio') or (history_scope == 'portfolio' and not portfolio_id):
            raise ValueError('Portfolio history scope requires a selected portfolio')
        if history_all and (history_scope != 'portfolio' or mode != 'steam_history'):
            raise ValueError('All-holdings history requires a portfolio-scoped history task')
        progress('started', f"Started {mode.replace('_', ' ')} using {pull_mode} pull mode", mode=mode, pullMode=pull_mode, historyLimit=history_limit, cycles=cycles)
        result = None
        for cycle in range(1, cycles + 1):
            if not stop_if_requested():
                return
            progress('cycle', f"Cycle {cycle}/{cycles}", cycle=cycle, cycles=cycles)
            if mode in ('provider_snapshots', 'full_pull', 'accumulate_missing'):
                progress('provider_sync', 'Refreshing CSGO Trader provider snapshots')
                result = market_universe_bulk_sync(
                    provider='csgotrader',
                    preferred_source='any',
                    warm_history=(mode == 'full_pull'),
                    history_limit=history_limit if mode == 'full_pull' else 0,
                    portfolio_id=portfolio_id,
                    retry_history_failures=retry_failures,
                    progress=progress,
                )
                if not stop_if_requested():
                    with DATA_TASK_LOCK:
                        DATA_TASKS[task_id]['result'] = _data_task_result_summary(result)
                    return
                if not isinstance(result, dict) or result.get('ok') is False:
                    with DATA_TASK_LOCK:
                        DATA_TASKS[task_id]['result'] = _data_task_result_summary(result)
                    raise RuntimeError(result.get('error') or 'Provider acquisition did not complete' if isinstance(result, dict) else 'Invalid provider result')
                derived = result.get('derivedPublication') or (result.get('status') or {}).get('derivedPublication') or {}
                commit_state = (result.get('status') or {}).get('commitState')
                if derived.get('complete') is False or commit_state == 'source_committed_derived_pending':
                    pending(result, 'derived_pending', 'Source data committed; derived publication remains pending and has not been reported complete.')
                    return
                if check_history(result.get('steamHistoryWarm')):
                    return
                progress(
                    'provider_complete',
                    f"Provider sync checked {int(result.get('itemsStored') or 0):,} rows; new {int(result.get('newObservationRows') or 0):,}, updated {int(result.get('updatedObservationRows') or 0):,}",
                    itemsStored=result.get('itemsStored'),
                    newRows=result.get('newObservationRows'),
                    updatedRows=result.get('updatedObservationRows'),
                    unchangedRows=result.get('unchangedObservationRows'),
                    bySource=result.get('bySource') or {},
                )
            if mode in ('steam_history', 'accumulate_missing') and history_limit:
                if portfolio_id:
                    portfolio_limit = min(250, history_limit) if history_scope == 'portfolio' else max(1, history_limit // 2)
                    progress('portfolio_history', f"Warming up to {portfolio_limit:,} missing portfolio Steam charts", limit=portfolio_limit)
                    candidates = (latest_portfolio_history_candidates(10000, portfolio_id=portfolio_id, all_items=True)
                                  if history_all else latest_portfolio_history_candidates(
                                      max(portfolio_limit * 8, 80), portfolio_id=portfolio_id))
                    batches = [candidates[offset:offset + portfolio_limit]
                               for offset in range(0, len(candidates), portfolio_limit)] if history_all else [candidates]
                    portfolio_result = {'attempted': 0, 'refreshed': 0, 'failed': 0, 'cachedFallback': 0, 'errors': []}
                    for batch_number, batch in enumerate(batches, 1):
                        if not stop_if_requested():
                            return
                        if history_all:
                            progress('portfolio_batch', f"Selected portfolio batch {batch_number}/{len(batches)}",
                                     batch=batch_number, batches=len(batches), selectedItems=len(candidates))
                        batch_result = refresh_steam_history_for_chart_items(
                            batch, limit=portfolio_limit, force=False if history_all else force,
                            retry_failures=retry_failures, progress=progress, stage_prefix='portfolio_history')
                        for key in ('attempted', 'refreshed', 'failed', 'cachedFallback'):
                            portfolio_result[key] += int(batch_result.get(key) or 0)
                        portfolio_result['errors'] = (portfolio_result['errors'] + list(batch_result.get('errors') or []))[:50]
                        if check_history(batch_result):
                            return
                    if history_scope == 'portfolio':
                        result = {'ok': True, 'historyScope': 'portfolio',
                                  'selectedItems': len(candidates) if history_all else min(portfolio_limit, len(candidates)),
                                  'historyAll': history_all, 'portfolioHistory': portfolio_result}
                    progress(
                        'portfolio_history_complete',
                        f"Portfolio Steam charts cached {int(portfolio_result.get('refreshed') or 0):,}/{int(portfolio_result.get('attempted') or 0):,}",
                        **portfolio_result,
                    )
                    market_limit = 0 if history_scope == 'portfolio' else max(0, history_limit - portfolio_limit)
                else:
                    market_limit = history_limit
                if market_limit:
                    progress('market_history', f"Warming up to {market_limit:,} missing market Steam charts", limit=market_limit)
                    market_result = refresh_steam_history_for_chart_items(
                        market_history_candidates(max(market_limit * 8, 120), history_days=90),
                        limit=market_limit,
                        force=force,
                        retry_failures=retry_failures,
                        progress=progress,
                        stage_prefix='market_history',
                    )
                    if check_history(market_result):
                        return
                    progress(
                        'market_history_complete',
                        f"Market Steam charts cached {int(market_result.get('refreshed') or 0):,}/{int(market_result.get('attempted') or 0):,}",
                        **market_result,
                    )
            status = market_universe_status()
            steam_history_count = (status.get('stores') or {}).get('steamHistoryItems')
            steam_history_summary = (f"{int(steam_history_count):,} items with raw Steam charts"
                                     if steam_history_count is not None else 'saved Steam chart count not measured')
            progress(
                'coverage',
                f"Coverage now {status.get('coveragePct', 0)}% priced, {steam_history_summary}",
                coveragePct=status.get('coveragePct'),
                observedItems=status.get('observedItems'),
                catalogItems=status.get('catalogItems'),
                steamHistoryItems=(status.get('stores') or {}).get('steamHistoryItems'),
                gaps=status.get('gaps') or [],
            )
            if cycle < cycles and sleep_seconds:
                DATA_TASK_WAKE.wait(sleep_seconds)
        if not stop_if_requested():
            return
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            partial = bool(isinstance(result, dict) and result.get('errors'))
            task['status'] = 'partial' if partial else 'complete'
            task['finishedAt'] = int(time.time())
            task['updatedAt'] = task['finishedAt']
            task['result'] = _data_task_result_summary(result or {'ok': True})
        progress('partial' if partial else 'complete', 'Data task finished with provider errors; requested coverage remains incomplete.' if partial else 'Data task complete')
    except Exception as exc:
        with DATA_TASK_LOCK:
            task = DATA_TASKS.get(task_id)
            if task:
                task['status'] = 'error'
                task['finishedAt'] = int(time.time())
                task['updatedAt'] = task['finishedAt']
                task['error'] = str(exc)
        progress('error', f"Data task failed: {exc}", error=str(exc))
    finally:
        with DATA_TASK_LOCK:
            DATA_TASK_WORKERS.pop(task_id, None)
            if ACTIVE_DATA_TASK_ID == task_id:
                ACTIVE_DATA_TASK_ID = None


def start_data_task(options):
    global ACTIVE_DATA_TASK_ID
    if not isinstance(options, dict):
        return {'ok': False, 'error': 'One data-task object is required'}
    action = options.get('action')
    if action in ('pause', 'cancel', 'resume'):
        task_id = options.get('taskId') or options.get('task_id')
        with DATA_TASK_LOCK:
            task = DATA_TASKS.get(task_id)
            if not task:
                return {'ok': False, 'error': 'Saved data task not found'}
            if action != 'resume':
                if task.get('status') in ('complete', 'cancelled'):
                    return {'ok': False, 'error': 'Task is already terminal', 'task': compact_data_task(task)}
                state = 'paused' if action == 'pause' else 'cancelled'
                task.setdefault('options', {})['_control'] = state
                task.update(status=state, updatedAt=int(time.time()))
                if state == 'cancelled':
                    task['finishedAt'] = int(time.time())
                DATA_TASK_WAKE.set()
            else:
                worker = DATA_TASK_WORKERS.get(task_id)
                if worker and callable(getattr(worker, 'is_alive', None)) and worker.is_alive():
                    return {'ok': False, 'error': 'The previous task worker is still closing; resume after its saved boundary', 'task': compact_data_task(task)}
                if task.get('status') not in ('paused', 'pending', 'error', 'queued', 'partial'):
                    return {'ok': False, 'error': 'This task is not resumable', 'task': compact_data_task(task)}
                if latest_data_task_active():
                    return {'ok': False, 'error': 'Another explicit task is active'}
                task.setdefault('options', {}).pop('_control', None)
                task['options']['_providerWaitSeconds'] = 0
                task.update(status='queued', stage='queued', updatedAt=int(time.time()))
                task.pop('finishedAt', None)
                options = dict(task['options'])
                ACTIVE_DATA_TASK_ID = task_id
        data_task_event(task_id, task['status'], 'Explicit data task ' + action + ' requested.')
        if action != 'resume':
            return {'ok': True, 'task': compact_data_task(task)}
        thread = threading.Thread(target=run_data_accumulation_task, args=(task_id, options), name=f'data-task-{task_id}', daemon=True)
        with DATA_TASK_LOCK:
            DATA_TASK_WORKERS[task_id] = thread
        thread.start()
        return {'ok': True, 'task': compact_data_task(task)}
    if DATA_TASK_STOP_EVENT.is_set():
        return {'ok': False, 'error': 'Server is stopping; no new source task admitted'}
    if latest_data_task_active():
        snapshot = data_task_snapshot()
        snapshot.update({'ok': False, 'error': 'A data task is already running.'})
        return snapshot
    task_id = f"data-{int(time.time())}-{len(DATA_TASKS) + 1}"
    task = {
        'id': task_id,
        'name': options.get('name') or (options.get('mode') or 'accumulate_missing').replace('_', ' ').title(),
        'mode': options.get('mode') or 'accumulate_missing',
        'pullMode': options.get('pullMode') or 'gentle',
        'status': 'queued',
        'stage': 'queued',
        'message': 'Queued',
        'createdAt': int(time.time()),
        'updatedAt': int(time.time()),
        'options': _json_safe(options),
        'events': [],
        'progress': {},
    }
    with DATA_TASK_LOCK:
        DATA_TASKS[task_id] = task
        ACTIVE_DATA_TASK_ID = task_id
    data_task_event(task_id, 'queued', f"Queued {task['name']}", **task['options'])
    thread = threading.Thread(target=run_data_accumulation_task, args=(task_id, options), name=f"data-task-{task_id}", daemon=True)
    with DATA_TASK_LOCK:
        DATA_TASK_WORKERS[task_id] = thread
    thread.start()
    snapshot = data_task_snapshot()
    snapshot.update({'ok': True, 'task': compact_data_task(task)})
    return snapshot


def stop_data_tasks(timeout=40):
    """Wake explicit provider waits and close workers at their unit boundary."""
    DATA_TASK_STOP_EVENT.set()
    DATA_TASK_WAKE.set()
    deadline = time.monotonic() + max(0, float(timeout))
    with DATA_TASK_LOCK:
        workers = list(DATA_TASK_WORKERS.values())
    for worker in workers:
        if worker is not threading.current_thread() and callable(getattr(worker, 'join', None)):
            worker.join(max(0, deadline - time.monotonic()))
    return all(not worker.is_alive() for worker in workers)


def market_item_metric_windows(conn, rows, windows=VOLATILITY_DETAIL_WINDOWS):
    aliases = []
    row_keys = {}
    for idx, row in enumerate(rows or []):
        for alias in (row.get('name'), row.get('marketHashName')):
            alias = (alias or '').strip()
            if not alias:
                continue
            if alias not in row_keys:
                aliases.append(alias)
                row_keys[alias] = []
            row_keys[alias].append(idx)
    out = {idx: {} for idx in range(len(rows or []))}
    if not aliases:
        return out
    windows = tuple(windows or VOLATILITY_DETAIL_WINDOWS)
    for win in windows:
        ensure_metric_window_current(conn, win)
    for i in range(0, len(aliases), 400):
        chunk = aliases[i:i+400]
        name_marks = ','.join('?' for _ in chunk)
        win_marks = ','.join('?' for _ in windows)
        metric_rows = conn.execute(
            f"""SELECT name, window_days, volatility_pct, trend_pct, range_pct, points,
                      clean_points, first_ts, last_ts, prior_price, price_source,
                      json_extract(raw, '$.metricBasis') AS metricBasis,
                      json_extract(raw, '$.metricState') AS metricState,
                      json_extract(raw, '$.confidencePct') AS confidencePct,
                      json_extract(raw, '$.metricBuildVersion') AS metricBuildVersion,
                      json_extract(raw, '$.spanDays') AS spanDays,
                      json_extract(raw, '$.windowCoveragePct') AS windowCoveragePct,
                      json_extract(raw, '$.isPartialWindow') AS isPartialWindow
                FROM item_market_metrics
                WHERE provider='market'
                  AND window_days IN ({win_marks})
                  AND name IN ({name_marks})""",
            list(windows) + chunk,
        ).fetchall()
        for metric in metric_rows:
            win = str(int(metric['window_days'] or 0))
            for idx in row_keys.get(metric['name'], []):
                if win in out[idx]:
                    continue
                is_partial = bool(metric['isPartialWindow'])
                out[idx][win] = {
                    'volatilityPct': metric['volatility_pct'],
                    'trendPct': metric['trend_pct'],
                    'rangePct': metric['range_pct'],
                    'points': metric['points'],
                    'cleanPoints': metric['clean_points'],
                    'firstTs': metric['first_ts'],
                    'lastTs': metric['last_ts'],
                    'hasPrior': metric['prior_price'] is not None,
                    'metricBasis': metric['metricBasis'] or metric['price_source'],
                    'metricState': metric['metricState'] or ('partial' if is_partial else 'real'),
                    'confidencePct': metric['confidencePct'],
                    'metricBuildVersion': metric['metricBuildVersion'],
                    'spanDays': metric['spanDays'],
                    'windowCoveragePct': metric['windowCoveragePct'],
                    'isPartialWindow': is_partial,
                }
    return out


def average_volatility_windows(items, require_full=True):
    out = {}
    for win in VOLATILITY_DETAIL_WINDOWS:
        vals = []
        for item in items or []:
            metric = (item.get('volatilityWindows') or {}).get(str(win)) or {}
            if require_full and metric.get('isPartialWindow') and metric.get('metricBasis') != 'csgotrader_window_anchor':
                continue
            val = _finite_float(metric.get('volatilityPct'))
            if val is not None:
                vals.append(val)
        if vals:
            out[str(win)] = round(sum(vals) / len(vals), 2)
    return out


def market_item_lookup_filter_options(conn, window_days):
    receipt = market_read_receipt(conn,'market_lookup_receipt:'+str(window_days))
    return receipt.get('filters',{}) if receipt else {'collections':[],'types':[],'grades':[],'available':False}


def split_filter_terms(value):
    if isinstance(value, (list, tuple)):
        raw = []
        for item in value:
            raw.extend(split_filter_terms(item))
        return raw
    return [
        part.strip().lower()
        for part in re.split(r'[|,]', str(value or ''))
        if part.strip()
    ]


def resolve_market_metric_window(conn, requested):
    requested = parse_int_param(requested, 90, allowed=VOLATILITY_WINDOWS)
    # Exact requested windows need no probes of unrelated saved windows.
    # Prefer genuine all-history metrics for zero; older caches without that
    # projection retain the explicitly labelled largest-window fallback.
    if conn.execute(
            "SELECT 1 FROM item_market_metrics WHERE provider='market' AND window_days=? LIMIT 1",
            (requested,)).fetchone():
        receipt = market_read_receipt(conn,'market_metric_receipt:'+str(requested))
        return requested, True, receipt.get('rows') if receipt else None
    rows = []
    for window in sorted(VOLATILITY_WINDOWS):
        if conn.execute("SELECT 1 FROM item_market_metrics WHERE provider='market' AND window_days=? LIMIT 1",(window,)).fetchone():
            receipt = market_read_receipt(conn,'market_metric_receipt:'+str(window))
            rows.append({'window_days':window,'n':receipt.get('rows') if receipt else None})
    if not rows:
        return requested, False, 0
    if requested == 0:
        chosen = max(rows, key=lambda r: int(r['window_days'] or 0))
    else:
        chosen = min(rows, key=lambda r: abs(int(r['window_days'] or 0) - requested))
    return int(chosen['window_days'] or 0), int(chosen['window_days'] or 0) == requested, chosen['n']


def parse_float_param(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def market_item_lookup_is_current(conn, window_days):
    return market_cache_state(conn,window_days)['cacheState']=='current'


def build_unpriced_market_lookup_rows(conn, window_days, max_rows=5000, missing_only=True,
                                     after_name=None, allow_more=False, return_batch=False):
    """Bounded metadata projection for explicit writes, never called by a GET.

    The caller owns the transaction and SQLite progress/time/storage budgets.
    Current quotes are tested only through name/provider keys. No history is read.
    """
    if type(max_rows) is not int or not 1 <= max_rows <= 5000:
        raise ValueError('Unpriced projection row cap must be between 1 and 5000')
    if window_days not in VOLATILITY_WINDOWS:
        raise ValueError('Unsupported lookup window')
    def positive(value):
        value = _finite_float(value)
        return value if value is not None and value > 0 else None
    missing_sql = ("NOT EXISTS (SELECT 1 FROM market_item_lookup l "
                   "WHERE l.window_days=? AND l.name=m.name)") if missing_only else (
                   "NOT EXISTS (SELECT 1 FROM item_market_metrics im WHERE im.provider='market' "
                   "AND im.window_days=? AND im.name=m.name)")
    cursor_sql = 'AND m.name > ?' if after_name is not None else ''
    params = [window_days] + ([after_name] if after_name is not None else []) + [max_rows + 1]
    keys = [r['name'] for r in conn.execute(
        f'SELECT m.name FROM market_items m WHERE {missing_sql} {cursor_sql} ORDER BY m.name LIMIT ?',params)]
    if len(keys) > max_rows and not allow_more:
        raise RuntimeError('Unpriced projection row cap exceeded before insertion')
    has_more = len(keys) > max_rows
    keys = keys[:max_rows]
    rows = []
    for start in range(0,len(keys),400):
        chunk = keys[start:start+400]
        marks = ','.join('?' for _ in chunk)
        rows.extend(dict(r) for r in conn.execute(
            f"""SELECT m.name, COALESCE(c.market_hash_name,m.market_hash_name,m.name) AS marketHashName,
                       m.market_hash_name AS legacyMarketHashName,
                       COALESCE(c.kind,m.kind) AS kind, COALESCE(c.item_type,m.item_type) AS itemType,
                       COALESCE(c.weapon,m.weapon) AS weapon, COALESCE(c.collection,m.collection) AS collection,
                       COALESCE(c.rarity,m.rarity) AS rarity,
                       m.primary_price, m.primary_provider, m.primary_price_basis,
                       m.primary_price_updated_at, m.primary_confidence_pct
                  FROM market_items m LEFT JOIN item_catalog c ON c.name=m.name
                 WHERE m.name IN ({marks}) ORDER BY m.name""",chunk))
    aliases = list(dict.fromkeys(n for r in rows for n in (r['name'],r['marketHashName'],r['legacyMarketHashName']) if n))
    providers = tuple(CURRENT_PRICE_PROVIDERS)
    provider_marks = ','.join('?' for _ in providers)
    prices = {}
    for start in range(0,len(aliases),400):
        chunk = aliases[start:start+400]
        marks = ','.join('?' for _ in chunk)
        for saved in conn.execute(
            f"""SELECT name,provider,price,solid_price,lowest_sell_order,highest_buy_order,
                       spread_pct,stability_pct,confidence_pct,source,basis,kind,updated_at
                  FROM market_item_prices WHERE name IN ({marks}) AND provider IN ({provider_marks})""",
            chunk+list(providers)):
            quote = dict(saved)
            if positive(quote['solid_price']) or positive(quote['price']):
                prices.setdefault(quote['name'],{})[quote['provider']] = quote
    payload = []
    for row in rows:
        quotes = {}
        for alias in (row['name'],row['marketHashName'],row['legacyMarketHashName']):
            for provider,quote in prices.get(alias,{}).items():
                if provider not in quotes or int(quote['updated_at'] or 0) > int(quotes[provider]['updated_at'] or 0):
                    quotes[provider] = quote
        primary = next((quotes[p] for p in PRIMARY_PRICE_PROVIDER_ORDER if p in quotes),None)
        if primary is None and row['primary_provider'] in providers and positive(row['primary_price']):
            primary = {'provider':row['primary_provider'],'price':row['primary_price'],
                       'basis':row['primary_price_basis'],'updated_at':row['primary_price_updated_at'],
                       'confidence_pct':row['primary_confidence_pct']}
        primary = primary or {}
        latest = positive(primary.get('solid_price')) or positive(primary.get('price'))
        summary = {provider:{'label':PRICE_PROVIDER_LABELS.get(provider,provider),
                   'price':q.get('price'),'solidPrice':q.get('solid_price'),
                   'lowestSellOrder':q.get('lowest_sell_order'),'highestBuyOrder':q.get('highest_buy_order'),
                   'spreadPct':q.get('spread_pct'),'stabilityPct':q.get('stability_pct'),
                   'confidencePct':q.get('confidence_pct'),'source':q.get('source'),
                   'basis':q.get('basis'),'kind':q.get('kind'),'updatedAt':q.get('updated_at')}
                   for provider,q in quotes.items()}
        source = primary.get('provider') or ('saved_primary' if latest else 'catalog_unpriced')
        steam = quotes.get('steam_snapshot',{})
        # Keep full name-derived tags but avoid opening the global alias cache.
        meta = {**row,'item_type':row['itemType'],'type':row['itemType'],'latestPrice':latest,'volume':None}
        groups = market_group_tags(meta,collection_alias_resolver=lambda collection:None)
        labels = [group_tag_label(tag) for tag in groups]
        search = ' '.join(str(v) for v in [row['name'],row['marketHashName'],row['kind'],
                 row['itemType'],row['weapon'],row['collection'],row['rarity'],source,
                 ' '.join(labels)] if v).lower()
        payload.append((window_days,row['name'],row['marketHashName'],row['kind'],row['itemType'],
                        row['weapon'],row['collection'],row['rarity'],latest,None,None,None,
                        primary.get('updated_at'),source,None,None,None,json.dumps(groups,separators=(',',':')),
                        search,0,positive(steam.get('price')),positive(steam.get('lowest_sell_order')),
                        positive(steam.get('highest_buy_order')),positive(steam.get('spread_pct')),
                        positive(steam.get('stability_pct')),primary.get('basis'),
                        positive(primary.get('confidence_pct')),json.dumps(summary,separators=(',',':'))))
    if return_batch:
        return {'rows':payload,'requested':len(keys),'lastKey':keys[-1] if keys else after_name,'hasMore':has_more}
    return payload


def append_missing_unpriced_market_lookup(conn, window_days, max_rows=5000,
                                         after_name=None, allow_more=False):
    """Insert missing metadata/current-price rows; caller owns the transaction."""
    if not conn.in_transaction:
        raise RuntimeError('Explicit transaction required for unpriced projection')
    batch = build_unpriced_market_lookup_rows(conn,window_days,max_rows=max_rows,
                after_name=after_name,allow_more=allow_more,return_batch=True)
    rows = batch.pop('rows')
    if not rows:
        return {'inserted':0,'windowDays':window_days,**batch}
    inserted_cursor = conn.executemany(
        """INSERT INTO market_item_lookup
           (window_days,name,market_hash_name,kind,item_type,weapon,collection,rarity,
            latest_price,prior_price,change_pct,change_dollar,latest_at,source,volatility_pct,
            history_points,edge_case_count,groups_json,search_text,metric_updated_at,
            steam_price,steam_lowest_sell_order,steam_highest_buy_order,steam_spread_pct,
            steam_stability_pct,price_basis,confidence_pct,provider_prices_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(window_days,name) DO NOTHING""",rows)
    inserted = inserted_cursor.rowcount
    receipt = market_read_receipt(conn,'market_lookup_receipt:'+str(window_days)) or {}
    filters = receipt.get('filters') or {}
    # A metadata-only insertion cannot verify a stale whole-window price receipt.
    # Retain its last price timestamp and mark totals/currentness unknown.
    receipt.update({'sourceRevision':None,'rows':None,'priced':None,'priorPriced':None,
                    'sources':[], 'unpricedProjectionAdded':inserted,'unpricedProjectionAt':int(time.time()),
                    'filters':{'collections':sorted(set(filters.get('collections') or []) | {r[6] for r in rows if r[6]}),
                               'types':sorted(set(filters.get('types') or []) | {r[4] or r[5] or r[3] for r in rows if r[4] or r[5] or r[3]}),
                               'grades':sorted(set(filters.get('grades') or []) | {r[7] for r in rows if r[7]}),
                               'available':True}})
    save_market_read_receipt(conn,'market_lookup_receipt:'+str(window_days),receipt)
    return {'inserted':inserted,'windowDays':window_days,'receiptCurrentness':'unverified',**batch}


def rebuild_market_item_lookup(conn, window_days):
    if preserve_recovered_research_without_inputs(conn):
        return conn.execute('SELECT COUNT(*) FROM market_item_lookup WHERE window_days=?',
                            (window_days,)).fetchone()[0]
    flush_market_source_revision(conn)
    meta_name_expr = "COALESCE(c1.name, c2.name, m.name)"
    meta_hash_expr = "COALESCE(c1.market_hash_name, c2.market_hash_name, m.name)"
    meta_type_expr = "COALESCE(c1.item_type, c2.item_type)"
    meta_weapon_expr = "COALESCE(c1.weapon, c2.weapon)"
    meta_kind_expr = "COALESCE(c1.kind, c2.kind)"
    meta_collection_expr = "COALESCE(c1.collection, c2.collection)"
    meta_rarity_expr = "COALESCE(c1.rarity, c2.rarity)"
    rows = [dict(r) for r in conn.execute(
        f"""SELECT
              m.name AS metricName,
              {meta_name_expr} AS name,
              {meta_hash_expr} AS marketHashName,
              {meta_kind_expr} AS kind,
              {meta_type_expr} AS itemType,
              {meta_weapon_expr} AS weapon,
              {meta_collection_expr} AS collection,
              {meta_rarity_expr} AS rarity,
              m.current_price AS latestPrice,
              m.prior_price AS priorPrice,
              CASE WHEN m.prior_price > 0 THEN ROUND(((m.current_price / m.prior_price) - 1) * 100, 2) END AS changePct,
              CASE WHEN m.prior_price IS NOT NULL THEN ROUND(m.current_price - m.prior_price, 4) END AS changeDollar,
              m.last_ts AS latestAt,
              COALESCE(m.price_source, 'market') AS source,
              m.volatility_pct AS volatilityPct,
              m.points AS historyPoints,
              m.edge_case_count AS edgeCaseCount,
              m.updated_at AS metricUpdatedAt
            FROM item_market_metrics m
            LEFT JOIN item_catalog c1 ON c1.name=m.name
            LEFT JOIN item_catalog c2 ON c2.market_hash_name=m.name AND c1.name IS NULL
            WHERE m.provider='market' AND m.window_days=?""",
        (window_days,),
    )]
    price_rows = [dict(r) for r in conn.execute(
        """SELECT name, provider, price, solid_price, lowest_sell_order, highest_buy_order,
                  spread_pct, stability_pct, confidence_pct, volume, source, basis,
                  kind, url, observed_at, updated_at
           FROM market_item_prices
           WHERE price IS NOT NULL
             AND provider NOT IN ('steam','steam_history')"""
    )]
    prices_by_name = {}
    for pr in price_rows:
        bucket = prices_by_name.setdefault(pr.get('name'), {})
        existing = bucket.get(pr.get('provider'))
        if existing is None or int(pr.get('updated_at') or 0) > int(existing.get('updated_at') or 0):
            bucket[pr.get('provider')] = pr

    def merged_provider_prices(*names):
        merged = {}
        for n in names:
            for provider, pr in (prices_by_name.get(n) or {}).items():
                existing = merged.get(provider)
                if existing is None or int(pr.get('updated_at') or 0) > int(existing.get('updated_at') or 0):
                    merged[provider] = pr
        return merged

    def provider_summary(prices):
        return {
            provider: {
                'label': PRICE_PROVIDER_LABELS.get(provider, provider),
                'price': pr.get('price'),
                'solidPrice': pr.get('solid_price'),
                'lowestSellOrder': pr.get('lowest_sell_order'),
                'highestBuyOrder': pr.get('highest_buy_order'),
                'spreadPct': pr.get('spread_pct'),
                'stabilityPct': pr.get('stability_pct'),
                'confidencePct': pr.get('confidence_pct'),
                'source': pr.get('source'),
                'basis': pr.get('basis'),
                'kind': pr.get('kind'),
                'updatedAt': pr.get('updated_at'),
            }
            for provider, pr in prices.items()
            if provider in CURRENT_PRICE_PROVIDERS
        }

    def choose_primary_market_price(prices, fallback_price, fallback_source):
        for provider in PRIMARY_PRICE_PROVIDER_ORDER:
            if provider in prices and _positive_float(prices[provider].get('price')):
                return prices[provider]
        if fallback_price:
            return {'provider': fallback_source or 'market_metric', 'price': fallback_price, 'solid_price': fallback_price, 'basis': fallback_source, 'updated_at': None}
        return None

    payload = []
    for row in rows:
        provider_prices = merged_provider_prices(row.get('name'), row.get('marketHashName'), row.get('metricName'))
        primary = choose_primary_market_price(provider_prices, _positive_float(row.get('latestPrice')), row.get('source'))
        latest_price = _positive_float((primary or {}).get('solid_price')) or _positive_float((primary or {}).get('price'))
        prior_price = sane_prior_price(latest_price, row.get('priorPrice'))
        change_pct = pct_change(latest_price, prior_price)
        change_dollar = round(latest_price - prior_price, 4) if latest_price is not None and prior_price is not None else None
        summary = provider_summary(provider_prices)
        steam = provider_prices.get('steam_snapshot') or {}
        meta = {
            'name': row.get('name'),
            'marketHashName': row.get('marketHashName'),
            'kind': row.get('kind'),
            'item_type': row.get('itemType'),
            'type': row.get('itemType'),
            'weapon': row.get('weapon'),
            'collection': row.get('collection'),
            'rarity': row.get('rarity'),
            'latestPrice': latest_price,
            'volume': row.get('historyPoints'),
        }
        group_tags = market_group_tags(meta)
        group_labels = [group_tag_label(tag) for tag in group_tags]
        search_text = ' '.join(str(v) for v in [
            row.get('name'), row.get('marketHashName'), row.get('kind'), row.get('itemType'),
            row.get('weapon'), row.get('collection'), row.get('rarity'), row.get('source'),
            ' '.join(group_labels),
        ] if v).lower()
        provider_updated_at = max(
            (int((p or {}).get('updated_at') or 0) for p in provider_prices.values()),
            default=0,
        )
        lookup_updated_at = max(int(row.get('metricUpdatedAt') or 0), provider_updated_at)
        payload.append((
            window_days,
            row.get('name'),
            row.get('marketHashName'),
            row.get('kind'),
            row.get('itemType'),
            row.get('weapon'),
            row.get('collection'),
            row.get('rarity'),
            latest_price,
            prior_price,
            change_pct,
            change_dollar,
            (primary or {}).get('updated_at') or row.get('latestAt'),
            (primary or {}).get('provider') or row.get('source'),
            row.get('volatilityPct'),
            row.get('historyPoints'),
            row.get('edgeCaseCount'),
            json.dumps(group_tags, separators=(',', ':')),
            search_text,
            lookup_updated_at,
            _positive_float(steam.get('price')),
            _positive_float(steam.get('lowest_sell_order')),
            _positive_float(steam.get('highest_buy_order')),
            _positive_float(steam.get('spread_pct')),
            _positive_float(steam.get('stability_pct')),
            (primary or {}).get('basis') or row.get('source'),
            _positive_float((primary or {}).get('confidence_pct')),
            json.dumps(summary, separators=(',', ':')),
        ))
    # Preserve listings without metrics in explicit rebuilds. Missing prices
    # remain unknown; current saved quotes do not require history coverage.
    priced_names = {row[1] for row in payload}
    projection_cursor = None
    while True:
        projected = build_unpriced_market_lookup_rows(
            conn, window_days, missing_only=False, after_name=projection_cursor, allow_more=True)
        payload.extend(row for row in projected if row[1] not in priced_names)
        if len(projected) < 5000:
            break
        projection_cursor = projected[-1][1]
    conn.execute("DELETE FROM market_item_lookup WHERE window_days=?", (window_days,))
    conn.executemany(
        """INSERT OR REPLACE INTO market_item_lookup
           (window_days, name, market_hash_name, kind, item_type, weapon, collection,
            rarity, latest_price, prior_price, change_pct, change_dollar, latest_at,
            source, volatility_pct, history_points, edge_case_count, groups_json,
            search_text, metric_updated_at, steam_price, steam_lowest_sell_order,
            steam_highest_buy_order, steam_spread_pct, steam_stability_pct,
            price_basis, confidence_pct, provider_prices_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        payload,
    )
    # Reuse the rows this explicit task has already materialized. GETs need no
    # global COUNT/MAX/DISTINCT pass to describe this saved cache.
    saved = list({row[1]:row for row in payload}.values())
    revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
    if not revision:
        mark_market_source_changed(conn)
        flush_market_source_revision(conn)
        revision = conn.execute("SELECT value FROM settings WHERE key='market_source_revision'").fetchone()
    sources = {}
    for row in saved:
        if row[13]:
            sources[row[13]] = sources.get(row[13],0)+1
    save_market_read_receipt(conn,'market_lookup_receipt:'+str(window_days),{
        'lookupBuildVersion':LOOKUP_BUILD_VERSION,'metricBuildVersion':METRIC_BUILD_VERSION,
        'metricsVerified':metric_window_is_current(conn,window_days),
        'sourceRevision':revision['value'],'builtAt':int(time.time()),
        'sourceAsOf':max((int(row[12] or 0) for row in saved),default=0),
        'rows':len(saved),'priced':sum(row[8] is not None for row in saved),
        'priorPriced':sum(row[9] is not None for row in saved),
        'sources':[{'source':source,'items':count} for source,count in sorted(sources.items(),key=lambda pair:(-pair[1],pair[0]))[:12]],
        'filters':{'collections':sorted({row[6] for row in saved if row[6]}),
                   'types':sorted({row[4] or row[5] or row[3] for row in saved if row[4] or row[5] or row[3]}),
                   'grades':sorted({row[7] for row in saved if row[7]}),'available':True},
    })
    return len(payload)


def ensure_market_item_lookup(conn, window_days):
    return market_cache_state(conn,window_days)


def market_item_lookup_status(conn, window_days):
    receipt = market_read_receipt(conn,'market_lookup_receipt:'+str(window_days)) or {}
    total,priced,prior_priced = receipt.get('rows'),receipt.get('priced'),receipt.get('priorPriced')
    latest = receipt.get('sourceAsOf')
    recovered = recovered_research_status(conn, window_days) if not receipt else None
    if recovered:
        total = recovered.get('lookupRowsAtRecovery')
        latest = recovered.get('sourceAsOf')
    return {
        'catalogItems': total,
        'marketItems': total,
        'quotedItems': priced,
        'observedItems': priced,
        'observationRows': total,
        'priorItems': prior_priced,
        'priorCoveragePct': round((prior_priced / priced) * 100, 1) if priced else None,
        'latestObservationAt': latest,
        'coveragePct': round((priced / total) * 100, 1) if total and priced is not None else None,
        'sources': receipt.get('sources',[]),
        'countState':'saved_receipt' if receipt else 'recovered_window_receipt' if recovered else 'not_counted',
        **market_cache_state(conn,window_days),
        'dataset': {
            'id': f'market-item-lookup:{window_days}:{latest}:{total}',
            'source': 'cached market item lookup',
            'asOf': latest,
            'observedItems': priced,
            'observationRows': total,
            'implication': 'Search and sorting are served from a local ranked lookup table.',
        },
    }


def market_universe_item_slice(
    history_days=90,
    limit=500,
    search='',
    terms=None,
    blacklist=None,
    collection='',
    item_type='',
    grade='',
    min_price=None,
    max_price=None,
    sort_key='changePct',
    sort_dir=-1,
    projection='',
    offset=0,
):
    limit = parse_int_param(limit, 500, minimum=25, maximum=1000)
    offset = parse_int_param(offset, 0, minimum=0, maximum=100000)
    terms = split_filter_terms(terms or []) + split_filter_terms(search)
    blacklist = split_filter_terms(blacklist or [])
    collection = (collection or '').strip()
    item_type = (item_type or '').strip()
    grade = (grade or '').strip()
    min_price = parse_float_param(min_price)
    max_price = parse_float_param(max_price)
    sort_dir = -1 if str(sort_dir).strip() not in ('1', 'asc', 'ASC') else 1
    def usable_metric_expr(days):
        return (
            "(SELECT CASE WHEN COALESCE(json_extract(im.raw, '$.isPartialWindow'), 0) "
            "THEN NULL ELSE im.volatility_pct END "
            "FROM item_market_metrics im "
            f"WHERE im.provider='market' AND im.window_days={int(days)} "
            "AND im.name=market_item_lookup.name LIMIT 1)"
        )
    metadata_factor = "CASE WHEN COALESCE(collection, item_type, weapon, kind, '')='' THEN 0.45 ELSE 1 END"
    movement_expr = f"(COALESCE({usable_metric_expr(90)}, {usable_metric_expr(30)}, {usable_metric_expr(7)}, ABS(change_pct)) * {metadata_factor})"
    steam_provider_expr = "CAST(COALESCE(json_extract(provider_prices_json, '$.steam_snapshot.solidPrice'), json_extract(provider_prices_json, '$.steam_snapshot.price')) AS REAL)"
    external_price_exprs = [
        "CAST(COALESCE(json_extract(provider_prices_json, '$.csfloat.solidPrice'), json_extract(provider_prices_json, '$.csfloat.price')) AS REAL)",
        "CAST(COALESCE(json_extract(provider_prices_json, '$.buff163.solidPrice'), json_extract(provider_prices_json, '$.buff163.price')) AS REAL)",
        "CAST(COALESCE(json_extract(provider_prices_json, '$.skinport.solidPrice'), json_extract(provider_prices_json, '$.skinport.price')) AS REAL)",
        "CAST(COALESCE(json_extract(provider_prices_json, '$.youpin.solidPrice'), json_extract(provider_prices_json, '$.youpin.price')) AS REAL)",
    ]
    qualified_external_exprs = [
        f"CASE WHEN {steam_provider_expr} > 0 AND {expr} >= {steam_provider_expr} * 0.05 AND {expr} <= {steam_provider_expr} * 5.0 THEN {expr} END"
        for expr in external_price_exprs
    ]
    real_world_expr = "MIN({})".format(', '.join(f"COALESCE({expr}, 1e18)" for expr in qualified_external_exprs))
    steam_premium_pct_expr = (
        f"CASE WHEN {steam_provider_expr} > 0 AND {real_world_expr} < 1e18 "
        f"THEN (({steam_provider_expr} - {real_world_expr}) / {steam_provider_expr}) * 100 END"
    )
    steam_premium_dollar_expr = (
        f"CASE WHEN {steam_provider_expr} > 0 AND {real_world_expr} < 1e18 "
        f"THEN {steam_provider_expr} - {real_world_expr} END"
    )
    sort_map = {
        'name': "LOWER(name)",
        'rarity': "LOWER(rarity)",
        'type': "LOWER(type)",
        'latestPrice': "latest_price",
        'steamPrice': "steam_price",
        'realWorldPrice': real_world_expr,
        'steamPremiumPct': steam_premium_pct_expr,
        'steamPremiumDollar': steam_premium_dollar_expr,
        'csfloatPrice': "CAST(COALESCE(json_extract(provider_prices_json, '$.csfloat.solidPrice'), json_extract(provider_prices_json, '$.csfloat.price')) AS REAL)",
        'buffPrice': "CAST(COALESCE(json_extract(provider_prices_json, '$.buff163.solidPrice'), json_extract(provider_prices_json, '$.buff163.price')) AS REAL)",
        'changePct': "change_pct",
        'changeDollar': "change_dollar",
        'priorPrice': "prior_price",
        'volume': "history_points",
        'movementScore': movement_expr,
        'volatility7': usable_metric_expr(7),
        'volatility30': usable_metric_expr(30),
        'volatility90': usable_metric_expr(90),
        'source': "LOWER(source)",
        'latestAt': "latest_at",
        'orderBookSpreadPct': "steam_spread_pct",
    }
    sort_expr = sort_map.get(sort_key, sort_map['changePct'])
    sort_clause = f"({sort_expr}) IS NULL ASC, {sort_expr} {'ASC' if sort_dir == 1 else 'DESC'}"
    if sort_key != 'name':
        sort_clause += ", LOWER(name) ASC"
    # Stabilize case-distinct names across pages and all query plans.
    sort_clause += ", name COLLATE BINARY ASC"

    params = []
    with closing(catalog_read_db()) as conn:
        conn.execute('BEGIN')
        metric_window, exact_window, metric_count = resolve_market_metric_window(conn, history_days)
        filters = ["window_days=?"]
        params.append(metric_window)
        for term in terms:
            filters.append("search_text LIKE ?")
            params.append('%' + term + '%')
        for term in blacklist:
            filters.append("LOWER(name) NOT LIKE ?")
            params.append('%' + term + '%')
        if collection:
            filters.append("collection=?")
            params.append(collection)
        if item_type:
            filters.append("COALESCE(item_type, weapon, kind)=?")
            params.append(item_type)
        if grade:
            filters.append("rarity=?")
            params.append(grade)
        if min_price is not None:
            filters.append("latest_price >=?")
            params.append(min_price)
        if max_price is not None:
            filters.append("latest_price <=?")
            params.append(max_price)
        where = " AND ".join(filters)
        base_sql = f"FROM market_item_lookup WHERE {where}"
        count_sql = f"SELECT COUNT(*) {base_sql}"
        if len(filters) == 1 and not getattr(conn, '_derived_generation', None):
            # Unfiltered counts include NULL prices. Prefer the existing small
            # window/price index only after verifying its complete definition.
            count_index = conn.execute(
                "SELECT sql FROM sqlite_schema WHERE type='index' "
                "AND tbl_name='market_item_lookup' "
                "AND name='idx_market_item_lookup_window_price'").fetchone()
            expected_index = ("CREATE INDEX idx_market_item_lookup_window_price "
                              "ON market_item_lookup(window_days, latest_price)")
            if (count_index and re.sub(r"\s+", "", count_index['sql'] or '').lower()
                    == re.sub(r"\s+", "", expected_index).lower()):
                count_sql = ("SELECT COUNT(*) FROM market_item_lookup "
                             "INDEXED BY idx_market_item_lookup_window_price WHERE window_days=?")
        if len(filters) == 1 and getattr(conn, '_derived_generation', None):
            count_sql = 'SELECT COUNT(*) FROM saved_generation.lookup_windows WHERE window_days=?'
        prepared_generation_ids = None
        generation_indexes = getattr(conn, '_derived_generation_indexes', ())
        if (offset == 0 and getattr(conn, '_derived_generation', None) and terms and not blacklist
                and not collection and not item_type and not grade
                and min_price is None and max_price is None
                and sort_key == 'changePct' and sort_dir == -1
                and {'lookup_common_search_v1', 'lookup_common_windows_v1'} <= set(generation_indexes)):
            contains = ' AND '.join(['w.window_days=?'] + ['c.search_text LIKE ?'] * len(terms))
            matches = conn.execute(
                'SELECT w.source_rowid,COUNT(*) OVER() AS matched_total '
                'FROM saved_generation.lookup_common c INDEXED BY lookup_common_search_v1 '
                'CROSS JOIN saved_generation.lookup_windows w INDEXED BY lookup_common_windows_v1 '
                'WHERE w.common_id=c.common_id AND ' + contains +
                ' ORDER BY (w.change_pct) IS NULL ASC,w.change_pct DESC,LOWER(w.name) ASC,w.name COLLATE BINARY ASC LIMIT ?',
                params + [limit]).fetchall()
            total = matches[0]['matched_total'] if matches else 0
            prepared_generation_ids = [row['source_rowid'] for row in matches]
        else:
            total = conn.execute(count_sql, params).fetchone()[0]
        # Contains searches can examine the compact rank/search index first,
        # then fetch payload columns only for the final page. Without that
        # optional index the same SQL remains valid; never force an index.
        page_sql, page_params = base_sql, params + [limit]
        if (offset == 0 and not getattr(conn, '_derived_generation', None) and terms and not blacklist and not collection and not item_type and not grade
                and min_price is None and max_price is None
                and sort_key == 'changePct' and sort_dir == -1
                and conn.execute("SELECT 1 FROM sqlite_schema WHERE type='index' "
                                 "AND tbl_name='market_item_lookup' "
                                 "AND name='idx_market_lookup_search_order_v1'").fetchone()):
            page_sql = ("FROM market_item_lookup WHERE rowid IN ("
                        f"SELECT rowid {base_sql} ORDER BY {sort_clause} LIMIT ?)")
            page_params = params + [limit, limit]
        generation_indexes = getattr(conn, '_derived_generation_indexes', ())
        generation_active = bool(getattr(conn, '_derived_generation', None))
        simple_generation_filter = (offset == 0 and generation_active and not blacklist and not collection
            and not item_type and not grade and min_price is None and max_price is None)
        if prepared_generation_ids is not None:
            page_sql = 'FROM market_item_lookup WHERE rowid IN (SELECT value FROM json_each(?))'
            page_params = [json.dumps(prepared_generation_ids,separators=(',',':')), limit]
        elif simple_generation_filter and not terms and sort_key == 'name':
            page_sql = ('FROM market_item_lookup WHERE rowid IN (SELECT source_rowid '
                'FROM saved_generation.lookup_windows WHERE window_days=? '
                'ORDER BY LOWER(name) ' + ('ASC' if sort_dir == 1 else 'DESC') + ' LIMIT ?)')
            page_params = params + [limit, limit]
        elif (simple_generation_filter and not terms and sort_key == 'latestPrice'
                and {'lookup_common_price_v1', 'lookup_common_windows_v1'} <= set(generation_indexes)):
            page_sql = ('FROM market_item_lookup WHERE rowid IN ('
                'SELECT w.source_rowid FROM saved_generation.lookup_common c '
                'INDEXED BY lookup_common_price_v1 CROSS JOIN saved_generation.lookup_windows w '
                'INDEXED BY lookup_common_windows_v1 WHERE w.common_id=c.common_id AND w.window_days=? '
                'ORDER BY (c.latest_price) IS NULL ASC,c.latest_price ' +
                ('ASC' if sort_dir == 1 else 'DESC') + ',LOWER(w.name) ASC LIMIT ?)')
            page_params = params + [limit, limit]
        page_params = page_params + [offset]
        rows = [dict(r) for r in conn.execute(
            f"""SELECT
                  name,
                  market_hash_name AS marketHashName,
                  kind,
                  item_type AS type,
                  weapon,
                  collection,
                  rarity,
                  latest_price AS latestPrice,
                  steam_price AS steamPrice,
                  steam_lowest_sell_order AS lowestSellOrder,
                  steam_highest_buy_order AS highestBuyOrder,
                  steam_spread_pct AS orderBookSpreadPct,
                  steam_stability_pct AS priceStabilityPct,
                  price_basis AS priceBasis,
                  confidence_pct AS confidencePct,
                  provider_prices_json AS providerPricesJson,
                  prior_price AS priorPrice,
                  change_pct AS changePct,
                  change_dollar AS changeDollar,
                  latest_at AS latestAt,
                  source,
                  volatility_pct AS volatilityPct,
                  history_points AS historyPoints,
                  edge_case_count AS edgeCaseCount,
                  groups_json AS groupsJson
                {page_sql}
                ORDER BY {sort_clause}
                LIMIT ? OFFSET ?""",
            page_params,
        )]
        window_metrics = market_item_metric_windows(conn, rows)
        options = market_item_lookup_filter_options(conn, metric_window)
        status = market_item_lookup_status(conn, metric_window)

    out = []
    for idx, row in enumerate(rows):
        try:
            groups = json.loads(row.get('groupsJson') or '[]')
        except (TypeError, json.JSONDecodeError):
            groups = []
        try:
            provider_prices = json.loads(row.get('providerPricesJson') or '{}')
        except (TypeError, json.JSONDecodeError):
            provider_prices = {}
        out.append(compact_market_item_row({
            **row,
            'groups': groups,
            'providerPrices': provider_prices,
            'volatilityWindows': window_metrics.get(idx, {}),
        }))

    return project_market_item_response({
        'ok': True,
        'generatedAt': int(time.time()),
        'historyDays': parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS),
        'metricWindowDays': metric_window,
        'metricWindowExact': exact_window,
        'metricRows': metric_count,
        'limit': limit,
        'offset': offset,
        'hasMore': offset + len(out) < total,
        'nextOffset': offset + len(out) if offset + len(out) < total else None,
        'totalItems': total,
        'returnedItems': len(out),
        'items': out,
        'filters': options,
        'dataState':status['dataState'],'cacheState':status['cacheState'],
        'rebuildRequired':status['rebuildRequired'],'freshness':status['freshness'],
        'coverage': {
            **status,
            'movingItems': total,
            'returnedItems': len(out),
            'metricWindowDays': metric_window,
            'metricWindowExact': exact_window,
            'metricRows': metric_count,
        },
    }, projection)


market_universe_item_slice = market_slice_cache.wrap_market_item_slice(
    market_universe_item_slice, catalog_read_db, VOLATILITY_WINDOWS,
    LOOKUP_BUILD_VERSION, METRIC_BUILD_VERSION,
    publication_fence=lambda conn: BULK_STAGING_HOST.engine.external()
        if bulk_staging_enabled(conn) else None)


def market_lookup_analysis_items(history_days=90, limit=100000):
    """Return market analysis rows from the materialized lookup table.

    The lookup is the release path for full-market reads: item search, market
    taxonomy, and group history all share this compact table so they do not
    rejoin catalog rows or recompute volatility for every request.
    """
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    limit = parse_int_param(limit, 100000, minimum=1, maximum=100000)
    with closing(catalog_read_db()) as conn:
        metric_window, _exact_window, _metric_count = resolve_market_metric_window(conn, history_days)
        ensure_market_item_lookup(conn, metric_window)
        rows = [dict(r) for r in conn.execute(
            """SELECT
                  name,
                  market_hash_name AS marketHashName,
                  kind,
                  item_type AS type,
                  weapon,
                  collection,
                  rarity,
                  latest_price AS latestPrice,
                  steam_price AS steamPrice,
                  steam_lowest_sell_order AS lowestSellOrder,
                  steam_highest_buy_order AS highestBuyOrder,
                  steam_spread_pct AS orderBookSpreadPct,
                  steam_stability_pct AS priceStabilityPct,
                  price_basis AS priceBasis,
                  confidence_pct AS confidencePct,
                  prior_price AS priorPrice,
                  change_pct AS changePct,
                  change_dollar AS changeDollar,
                  latest_at AS latestAt,
                  source,
                  volatility_pct AS volatilityPct,
                  history_points AS historyPoints,
                  edge_case_count AS edgeCaseCount,
                  groups_json AS groupsJson
               FROM market_item_lookup
               WHERE window_days=? AND latest_price IS NOT NULL
               ORDER BY COALESCE(ABS(change_pct), 0) DESC, LOWER(name) ASC
               LIMIT ?""",
            (metric_window, limit),
        )]
        window_metrics = market_item_metric_windows(conn, rows)
    items = []
    for idx, row in enumerate(rows):
        latest_price = _positive_float(row.get('latestPrice'))
        if latest_price is None:
            continue
        prior_price = sane_prior_price(latest_price, row.get('priorPrice'))
        current_value = latest_price
        prior_value = prior_price
        change_dollar = row.get('changeDollar')
        if change_dollar is None and prior_price is not None:
            change_dollar = latest_price - prior_price
        meta = {
            'name': row.get('name'),
            'marketHashName': row.get('marketHashName') or row.get('name'),
            'kind': row.get('kind'),
            'type': row.get('type'),
            'weapon': row.get('weapon'),
            'collection': row.get('collection'),
            'rarity': row.get('rarity'),
            'latestPrice': latest_price,
            'volume': row.get('historyPoints'),
        }
        try:
            tags = json.loads(row.get('groupsJson') or '[]')
        except (TypeError, json.JSONDecodeError):
            tags = []
        tags = normalize_group_tags(tags)
        if not tags:
            tags = market_group_tags(meta)
        items.append({
            'name': row.get('name'),
            'marketHashName': row.get('marketHashName') or row.get('name'),
            'latestPrice': latest_price,
            'steamPrice': row.get('steamPrice'),
            'lowestSellOrder': row.get('lowestSellOrder'),
            'highestBuyOrder': row.get('highestBuyOrder'),
            'solidPrice': latest_price,
            'orderBookSpreadPct': row.get('orderBookSpreadPct'),
            'priceStabilityPct': row.get('priceStabilityPct'),
            'priceBasis': row.get('priceBasis') or row.get('source') or 'market_lookup',
            'confidencePct': row.get('confidencePct'),
            'priorPrice': prior_price,
            'currentValue': current_value,
            'priorValue': prior_value,
            'changePct': row.get('changePct'),
            'changeDollar': round(change_dollar, 4) if change_dollar is not None else None,
            'volume': row.get('historyPoints'),
            'volatilityPct': row.get('volatilityPct'),
            'volatilityWindows': window_metrics.get(idx, {}),
            'quantity': 1.0,
            'kind': row.get('kind'),
            'type': row.get('type'),
            'itemType': row.get('type'),
            'weapon': row.get('weapon'),
            'collection': row.get('collection'),
            'rarity': row.get('rarity'),
            'source': row.get('source'),
            'latestAt': row.get('latestAt'),
            'priorAt': None,
            'tags': tags,
        })
    return items


def market_universe_analysis(history_days=90, limit=120, min_group=3):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    limit = parse_int_param(limit, 120, minimum=10, maximum=500)
    min_group = parse_int_param(min_group, 3, minimum=1, maximum=100)
    catalog = market_lookup_analysis_items(history_days=history_days, limit=100000)
    with closing(catalog_read_db()) as conn:
        metric_window, exact_window, metric_count = resolve_market_metric_window(conn, history_days)
        ensure_market_item_lookup(conn, metric_window)
        coverage = market_item_lookup_status(conn, metric_window)
    items = []
    sectors = {}
    for meta in catalog:
        chg = meta.get('changePct')
        if chg is None:
            continue
        item = {
            'name': meta.get('name'),
            'marketHashName': meta.get('marketHashName') or meta.get('name'),
            'kind': meta.get('kind'),
            'type': meta.get('item_type') or meta.get('type') or meta.get('itemType'),
            'weapon': meta.get('weapon'),
            'collection': meta.get('collection'),
            'rarity': meta.get('rarity'),
            'rarityColor': meta.get('rarityColor'),
            'image': meta.get('image'),
            'latestPrice': round(meta.get('latestPrice'), 4),
            'priorPrice': round(meta.get('priorPrice'), 4),
            'changePct': chg,
            'changeDollar': round(meta.get('changeDollar'), 4),
            'volume': meta.get('volume'),
            'latestAt': meta.get('latestAt'),
            'priorAt': meta.get('priorAt'),
            'source': meta.get('source') or 'steam_quote',
        }
        items.append(item)
        for tag in market_group_tags({**meta, **item, 'name': item['name']}):
            gid = tag['dimension'] + ':' + tag['name']
            g = sectors.setdefault(gid, {
                'dimension': tag['dimension'],
                'name': tag['name'],
                'count': 0,
                'priced': 0,
                'up': 0,
                'down': 0,
                'flat': 0,
                'sumChangePct': 0,
                'sumAbsChangePct': 0,
                'sumDollar': 0,
                'volume': 0,
                'topItem': None,
            })
            g['count'] += 1
            g['priced'] += 1
            g['sumChangePct'] += chg
            g['sumAbsChangePct'] += abs(chg)
            g['sumDollar'] += item['changeDollar']
            g['volume'] += int(item.get('volume') or 0)
            if chg > 1:
                g['up'] += 1
            elif chg < -1:
                g['down'] += 1
            else:
                g['flat'] += 1
            if not g['topItem'] or abs(chg) > abs(g['topItem'].get('changePct') or 0):
                g['topItem'] = {'name': item['name'], 'changePct': chg, 'latestPrice': item['latestPrice']}

    sector_rows = []
    for g in sectors.values():
        if g['priced'] < min_group:
            continue
        g['avgChangePct'] = round(g['sumChangePct'] / g['priced'], 2)
        g['avgAbsChangePct'] = round(g['sumAbsChangePct'] / g['priced'], 2)
        g['breadthUpPct'] = round((g['up'] / g['priced']) * 100, 1) if g['priced'] else 0
        g['breadthDownPct'] = round((g['down'] / g['priced']) * 100, 1) if g['priced'] else 0
        g['score'] = round(g['avgAbsChangePct'] * math.log(g['priced'] + 1, 2), 2)
        g['sumDollar'] = round(g['sumDollar'], 2)
        for key in ('sumChangePct', 'sumAbsChangePct'):
            g.pop(key, None)
        sector_rows.append(g)

    dimension_map = {}
    for row in sector_rows:
        d = dimension_map.setdefault(row['dimension'], {
            'dimension': row['dimension'],
            'groups': 0,
            'priced': 0,
            'avgAbsChangePct': 0,
            'avgChangePct': 0,
            'upGroups': 0,
            'downGroups': 0,
            'leader': None,
        })
        d['groups'] += 1
        d['priced'] += row.get('priced') or 0
        d['avgAbsChangePct'] += row.get('avgAbsChangePct') or 0
        d['avgChangePct'] += row.get('avgChangePct') or 0
        if (row.get('avgChangePct') or 0) > 1:
            d['upGroups'] += 1
        elif (row.get('avgChangePct') or 0) < -1:
            d['downGroups'] += 1
        if not d['leader'] or (row.get('score') or 0) > (d['leader'].get('score') or 0):
            d['leader'] = {
                'name': row.get('name'),
                'avgChangePct': row.get('avgChangePct'),
                'score': row.get('score'),
                'priced': row.get('priced'),
            }
    dimension_rows = []
    for d in dimension_map.values():
        if d['groups']:
            d['avgAbsChangePct'] = round(d['avgAbsChangePct'] / d['groups'], 2)
            d['avgChangePct'] = round(d['avgChangePct'] / d['groups'], 2)
        dimension_rows.append(d)

    gainers = sorted([i for i in items if i['changePct'] > 0], key=lambda x: (x['changePct'], x['changeDollar']), reverse=True)[:limit]
    losers = sorted([i for i in items if i['changePct'] < 0], key=lambda x: (x['changePct'], x['changeDollar']))[:limit]
    active_sectors = sorted(sector_rows, key=lambda x: x['score'], reverse=True)[:limit]
    market_sector_rows = [r for r in sector_rows if r.get('dimension') == 'Market Sector']
    up_sectors = sorted([r for r in market_sector_rows if (r.get('avgChangePct') or 0) > 0],
                        key=lambda x: (x['avgChangePct'], x['breadthUpPct'], x['priced']), reverse=True)[:limit]
    down_sectors = sorted([r for r in market_sector_rows if (r.get('avgChangePct') or 0) < 0],
                          key=lambda x: (x['avgChangePct'], -x['breadthDownPct']))[:limit]
    status = market_universe_status()
    return {
        'generatedAt': int(time.time()),
        'historyDays': history_days,
        'coverage': {
            **coverage,
            'movingItems': len(items),
            'sectors': len(sector_rows),
            'metricWindowDays': metric_window,
            'metricWindowExact': exact_window,
            'metricRows': metric_count,
        },
        'items': {
            'gainers': gainers,
            'losers': losers,
            'mostActive': sorted(items, key=lambda x: abs(x['changePct']), reverse=True)[:limit],
        },
        'sectors': {
            'active': active_sectors,
            'up': up_sectors,
            'down': down_sectors,
            'byDimension': sorted(sector_rows, key=lambda x: (x['dimension'], -abs(x['avgChangePct'])))[:limit * 2],
            'dimensions': sorted(dimension_rows, key=lambda x: (x['avgAbsChangePct'], x['groups']), reverse=True),
        },
    }


def analysis_scope_items(scope='portfolio', history_days=90, portfolio_id=None):
    scope = (scope or 'portfolio').strip().lower()
    if scope == 'market':
        return market_lookup_analysis_items(history_days=history_days, limit=100000)
    data = get_inventory_dataset(scale_days=history_days, history_days=history_days, limit=10000, portfolio_id=portfolio_id)
    items = []
    for row in data.get('items') or []:
        latest_price = _positive_float(row.get('marketUnitPrice'))
        prior_price = sane_prior_price(latest_price, row.get('marketPriorUnitPrice'))
        current_value = _positive_float(row.get('marketCurr'))
        prior_value = sane_prior_price(current_value, row.get('marketPast'))
        meta = {
            'name': row.get('name'),
            'marketHashName': row.get('marketHashName') or row.get('name'),
            'kind': row.get('kind'),
            'item_type': row.get('kind') == 'skins' and 'Skin' or row.get('class') or row.get('category'),
            'weapon': row.get('class'),
            'collection': row.get('collection'),
            'rarity': row.get('rarity'),
            'rarityColor': row.get('rarityColor'),
            'latestPrice': latest_price,
            'volume': (row.get('quote') or {}).get('volume'),
        }
        items.append({
            'name': row.get('name'),
            'marketHashName': row.get('marketHashName') or row.get('name'),
            'latestPrice': latest_price,
            'priorPrice': prior_price,
            'currentValue': current_value,
            'priorValue': prior_value,
            'changePct': row.get('marketPct'),
            'changeDollar': row.get('marketDollar'),
            'volume': (row.get('quote') or {}).get('volume'),
            'volatilityPct': ((row.get('volatility') or {}).get('volatilityPct')),
            'volatilityWindows': row.get('volatilityWindows') or {},
            'lowestSellOrder': _positive_float(row.get('lowestSellOrder')),
            'highestBuyOrder': _positive_float(row.get('highestBuyOrder')),
            'solidPrice': _positive_float(row.get('solidPrice')),
            'orderBookSpreadPct': row.get('orderBookSpreadPct'),
            'priceStabilityPct': row.get('priceStabilityPct'),
            'quantity': _positive_float(row.get('qty')) or 0,
            'kind': row.get('kind'),
            'itemType': row.get('category'),
            'weapon': row.get('class'),
            'collection': row.get('collection'),
            'rarity': row.get('rarity'),
            'rarityColor': row.get('rarityColor'),
            'image': row.get('image'),
            'source': row.get('marketPriceSource'),
            'latestAt': (row.get('quote') or {}).get('updated_at'),
            'priorAt': (row.get('volatility') or {}).get('firstTs'),
            'tags': market_group_tags(meta),
        })
    return items


def summarize_group_items(items, dimension, group):
    selected = [i for i in items if any(t.get('dimension') == dimension and t.get('name') == group for t in i.get('tags') or [])]
    count = len(selected)
    priced = sum(1 for i in selected if _positive_float(i.get('latestPrice')) is not None)
    current_total = sum(_positive_float(i.get('currentValue')) or 0 for i in selected)
    prior_items = [
        i for i in selected
        if _positive_float(i.get('priorValue')) is not None and _positive_float(i.get('currentValue')) is not None
    ]
    prior_total = sum(_positive_float(i.get('priorValue')) or 0 for i in prior_items)
    current_with_prior = sum(_positive_float(i.get('currentValue')) or 0 for i in prior_items)
    dollar = current_with_prior - prior_total if prior_total and current_with_prior else None
    weighted_pct = round(((current_with_prior / prior_total) - 1) * 100, 2) if prior_total and current_with_prior else None
    prior_coverage = round((len(prior_items) / priced) * 100, 1) if priced else 0
    move_pcts = [float(i.get('changePct')) for i in selected if i.get('changePct') is not None]
    avg_pct = round(sum(move_pcts) / len(move_pcts), 2) if move_pcts else None
    vols = [float(i.get('volatilityPct')) for i in selected if i.get('volatilityPct') is not None]
    avg_vol = round(sum(vols) / len(vols), 2) if vols else None
    avg_vol_windows = average_volatility_windows(selected)
    high_vol = sum(1 for v in vols if v >= 12)
    breadth_up = round((sum(1 for v in move_pcts if v > 1) / len(move_pcts)) * 100, 1) if move_pcts else 0
    breadth_down = round((sum(1 for v in move_pcts if v < -1) / len(move_pcts)) * 100, 1) if move_pcts else 0
    top_movers = sorted(
        [i for i in selected if i.get('changePct') is not None],
        key=lambda x: abs(x.get('changePct') or 0),
        reverse=True
    )[:8]
    return {
        'dimension': dimension,
        'name': group,
        'items': selected,
        'count': count,
        'priced': priced,
        'currentValue': round(current_total, 2),
        'priorValue': round(prior_total, 2) if prior_total else None,
        'priorCoverageItems': len(prior_items),
        'priorCoveragePct': prior_coverage,
        'changeDollar': round(dollar, 2) if dollar is not None else None,
        'weightedChangePct': weighted_pct,
        'avgChangePct': avg_pct,
        'avgVolatilityPct': avg_vol,
        'avgVolatilityWindows': avg_vol_windows,
        'highVolItems': high_vol,
        'breadthUpPct': breadth_up,
        'breadthDownPct': breadth_down,
        'topMovers': top_movers,
    }


def empty_taxonomy_group(dimension, name):
    return {
        'dimension': dimension,
        'name': name,
        'items': [],
        'count': 0,
        'priced': 0,
        'currentValue': 0,
        'priorValue': 0,
        'currentValueWithPrior': 0,
        'priorCoverageItems': 0,
        'changeDollar': 0,
        '_pcts': [],
        '_vols': [],
        '_vol_windows': {str(win): [] for win in VOLATILITY_DETAIL_WINDOWS},
        '_up': 0,
        '_down': 0,
    }


def taxonomy_analysis(scope='portfolio', history_days=90, portfolio_id=None, min_group=2, top_groups=12):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    min_group = parse_int_param(min_group, 2, minimum=1, maximum=100)
    top_groups = parse_int_param(top_groups, 12, minimum=3, maximum=1000)
    items = analysis_scope_items(scope=scope, history_days=history_days, portfolio_id=portfolio_id)
    mover_fields = [
        'name', 'marketHashName', 'latestPrice', 'lowestSellOrder', 'highestBuyOrder',
        'priceBasis', 'priorPrice', 'currentValue', 'priorValue', 'changePct',
        'changeDollar', 'volatilityPct', 'volatilityWindows', 'kind', 'type', 'itemType', 'weapon',
        'collection', 'rarity', 'rarityColor', 'image', 'source',
    ]
    metric_fields = (
        'volatilityPct',
        'windowCoveragePct', 'isPartialWindow', 'metricBasis', 'metricState',
    )

    def slim_volatility_windows(windows):
        out = {}
        for win, metric in (windows or {}).items():
            if not isinstance(metric, dict):
                continue
            slim = {field: metric.get(field) for field in metric_fields if metric.get(field) is not None}
            if slim:
                out[str(win)] = slim
        return out

    def slim_taxonomy_mover(item):
        out = {}
        for field in mover_fields:
            value = item.get(field)
            if value is None:
                continue
            if field == 'volatilityWindows':
                value = slim_volatility_windows(value)
                if not value:
                    continue
            out[field] = value
        return out

    def item_volatility_score(item):
        cached = item.get('_volatilityScore')
        if cached is not None:
            return cached
        vals = []
        if item.get('volatilityPct') is not None:
            vals.append(_finite_float(item.get('volatilityPct')) or 0)
        for metric in (item.get('volatilityWindows') or {}).values():
            if (metric or {}).get('isPartialWindow') and (metric or {}).get('metricBasis') != 'csgotrader_window_anchor':
                continue
            val = _finite_float((metric or {}).get('volatilityPct'))
            if val is not None:
                vals.append(val)
        if not vals:
            return 0
        score = max(vals)
        if not (item.get('collection') or item.get('type') or item.get('itemType') or item.get('weapon')):
            score *= 0.45
        if item.get('priorPrice') is None:
            score *= 0.7
        if len(item.get('volatilityWindows') or {}) <= 1:
            score *= 0.85
        item['_volatilityScore'] = score
        return score

    dimensions = {}
    for item in items:
        for tag in item.get('tags') or []:
            dim = tag.get('dimension')
            name = tag.get('name')
            if not dim or not name:
                continue
            bucket = dimensions.setdefault(dim, {})
            if name not in bucket:
                bucket[name] = empty_taxonomy_group(dim, name)
            g = bucket[name]
            g['items'].append(item)
            g['count'] += 1
            if _positive_float(item.get('latestPrice')) is not None:
                g['priced'] += 1
            item_current = _positive_float(item.get('currentValue'))
            item_prior = _positive_float(item.get('priorValue'))
            g['currentValue'] += item_current or 0
            if item_current is not None and item_prior is not None:
                g['currentValueWithPrior'] += item_current
                g['priorValue'] += item_prior
                g['priorCoverageItems'] += 1
                g['changeDollar'] += item_current - item_prior
            if item.get('changePct') is not None:
                g['_pcts'].append(float(item.get('changePct')))
                if float(item.get('changePct')) > 1:
                    g['_up'] += 1
                elif float(item.get('changePct')) < -1:
                    g['_down'] += 1
            if item.get('volatilityPct') is not None:
                g['_vols'].append(float(item.get('volatilityPct')))
            for win in VOLATILITY_DETAIL_WINDOWS:
                metric = (item.get('volatilityWindows') or {}).get(str(win)) or {}
                if metric.get('isPartialWindow') and metric.get('metricBasis') != 'csgotrader_window_anchor':
                    continue
                val = _finite_float(metric.get('volatilityPct'))
                if val is not None:
                    g['_vol_windows'][str(win)].append(val)
    dimension_rows = []
    for dim, groups in dimensions.items():
        group_rows = []
        for g in groups.values():
            if g['count'] < min_group:
                continue
            prior_total = g['priorValue']
            current_total = g['currentValue']
            current_with_prior = g.pop('currentValueWithPrior')
            prior_coverage_items = g['priorCoverageItems']
            pct_values = g.pop('_pcts')
            vol_values = g.pop('_vols')
            vol_window_values = g.pop('_vol_windows')
            up = g.pop('_up')
            down = g.pop('_down')
            g['currentValue'] = round(current_total, 2)
            g['priorValue'] = round(prior_total, 2) if prior_total else None
            g['priorCoveragePct'] = round((prior_coverage_items / g['priced']) * 100, 1) if g.get('priced') else 0
            g['changeDollar'] = round(g['changeDollar'], 2) if prior_total and current_with_prior else None
            g['weightedChangePct'] = round(((current_with_prior / prior_total) - 1) * 100, 2) if prior_total and current_with_prior else None
            g['avgChangePct'] = round(sum(pct_values) / len(pct_values), 2) if pct_values else None
            g['avgVolatilityPct'] = round(sum(vol_values) / len(vol_values), 2) if vol_values else None
            g['avgVolatilityWindows'] = {
                win: round(sum(vals) / len(vals), 2)
                for win, vals in vol_window_values.items()
                if vals
            }
            g['highVolItems'] = sum(1 for v in vol_values if v >= 12)
            g['breadthUpPct'] = round((up / len(pct_values)) * 100, 1) if pct_values else 0
            g['breadthDownPct'] = round((down / len(pct_values)) * 100, 1) if pct_values else 0
            g['score'] = round((abs(g['weightedChangePct'] or 0) + abs(g['avgChangePct'] or 0)) * math.log(g['count'] + 1, 2), 2)
            group_rows.append(g)
        group_rows.sort(key=lambda x: (x.get('score') or 0, x.get('count') or 0), reverse=True)
        if not group_rows:
            continue
        leaders = group_rows[:top_groups]
        leader_ids = {id(g) for g in leaders}
        for g in group_rows:
            group_items = g.get('items') or []
            if id(g) in leader_ids:
                top_movers = sorted(
                    [i for i in group_items if i.get('changePct') is not None],
                    key=lambda x: abs(x.get('changePct') or 0),
                    reverse=True
                )[:8]
                scored_volatile = [
                    (item_volatility_score(i), i)
                    for i in group_items
                ]
                top_volatile = [
                    i for score, i in sorted(scored_volatile, key=lambda x: x[0], reverse=True)
                    if score > 0
                ][:8]
                g['topMovers'] = [slim_taxonomy_mover(i) for i in top_movers]
                g['topVolatile'] = [slim_taxonomy_mover(i) for i in top_volatile]
            g.pop('items', None)
        dim_vol_windows = {}
        for win in VOLATILITY_DETAIL_WINDOWS:
            vals = [
                _finite_float((g.get('avgVolatilityWindows') or {}).get(str(win)))
                for g in group_rows
            ]
            vals = [v for v in vals if v is not None]
            if vals:
                dim_vol_windows[str(win)] = round(sum(vals) / len(vals), 2)
        dimension_rows.append({
            'dimension': dim,
            'groups': len(group_rows),
            'items': sum(g['count'] for g in group_rows),
            'priced': sum(g['priced'] for g in group_rows),
            'avgWeightedChangePct': round(sum((g.get('weightedChangePct') or 0) for g in group_rows) / len(group_rows), 2),
            'avgVolatilityPct': round(sum((g.get('avgVolatilityPct') or 0) for g in group_rows) / len(group_rows), 2),
            'avgVolatilityWindows': dim_vol_windows,
            'leaders': leaders,
        })
    preferred_dims = [
        'Collection', 'Market Sector', 'Item Class', 'Weapon Family', 'Weapon',
        'Event', 'Rarity', 'Wear', 'Price Band', 'Liquidity', 'Item Kind',
        'Special', 'Finish', 'Finish Family', 'Container Type', 'Skin Segment'
    ]
    dimension_rows.sort(
        key=lambda x: (
            preferred_dims.index(x.get('dimension')) if x.get('dimension') in preferred_dims else 999,
            -(x.get('groups') or 0),
            -(x.get('avgVolatilityPct') or 0),
        )
    )
    source_counts = {}
    for item in items:
        source = str(item.get('source') or 'unknown')
        bucket = source_counts.setdefault(source, {'source': source, 'items': 0, 'rows': 0, 'latestAt': 0})
        bucket['items'] += 1
        bucket['rows'] += 1
        bucket['latestAt'] = max(bucket['latestAt'], int(item.get('latestAt') or 0))
    coverage = {
        'items': len(items),
        'dimensions': len(dimension_rows),
        'groups': sum(d['groups'] for d in dimension_rows),
        'priced': sum(1 for i in items if _positive_float(i.get('latestPrice')) is not None),
        'priorItems': sum(1 for i in items if _positive_float(i.get('priorValue')) is not None),
        'latestObservationAt': max((int(i.get('latestAt') or 0) for i in items), default=0),
        'sources': sorted(source_counts.values(), key=lambda x: x['items'], reverse=True),
    }
    coverage['priorCoveragePct'] = round((coverage['priorItems'] / coverage['priced']) * 100, 1) if coverage['priced'] else 0
    coverage['dataset'] = {
        'id': f"{scope}-observed:{coverage['latestObservationAt']}:{len(items)}",
        'source': 'local observed price cache',
        'asOf': coverage['latestObservationAt'],
        'items': len(items),
        'windowDays': history_days,
        'implication': 'Changing the day window changes prior/reference and volatility math only; the current item universe and latest prices come from this dataset.',
    }
    return {
        'ok': True,
        'scope': scope,
        'historyDays': history_days,
        'generatedAt': int(time.time()),
        'coverage': coverage,
        'dimensions': dimension_rows,
    }


def item_history_line_points(rows, history_days=90, max_points=140):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    max_points = parse_int_param(max_points, 140, minimum=24, maximum=400)
    cutoff = history_cutoff_ts(history_days)
    series = normalized_history_series([
        {'point_ts': r.get('point_ts', r.get('ts')), 'price': r.get('price'), 'volume': r.get('volume'), 'source': 'steam_history'}
        for r in rows or []
        if not cutoff or int(r.get('point_ts', r.get('ts')) or 0) >= cutoff
    ])
    if len(series) <= max_points:
        return series
    keep = []
    step = (len(series) - 1) / max(1, max_points - 1)
    seen = set()
    for idx in range(max_points):
        src_idx = round(idx * step)
        if src_idx in seen or src_idx >= len(series):
            continue
        seen.add(src_idx)
        keep.append(series[src_idx])
    return keep


def volatility_windows_for_detail(selected_days):
    return list(VOLATILITY_DETAIL_WINDOWS)


def history_refresh_score(item):
    vals = []
    if item.get('volatilityPct') is not None:
        vals.append(_finite_float(item.get('volatilityPct')) or 0)
    for metric in (item.get('volatilityWindows') or {}).values():
        vals.append(_finite_float((metric or {}).get('volatilityPct')) or 0)
    return max(vals, default=0)


def classify_history_basis(rows):
    sources = {str((r or {}).get('source') or '').lower() for r in rows or []}
    if any(s == 'steam_history' for s in sources):
        return 'steam_history'
    if any(s.startswith('csgotrader:steam:last_') for s in sources):
        return 'csgotrader_window_anchor'
    if rows:
        return 'provider_observations'
    return 'insufficient'


def group_item_history(scope='portfolio', dimension=None, group=None, history_days=90, points=140, limit=10, portfolio_id=None, refresh=False, refresh_limit=0, force=False):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    points = parse_int_param(points, 140, minimum=24, maximum=320)
    limit = parse_int_param(limit, 10, minimum=3, maximum=16)
    dimension = (dimension or '').strip()
    group = (group or '').strip()
    if not dimension or not group:
        return {'ok': False, 'error': 'Missing dimension or group', 'series': []}

    items = analysis_scope_items(scope=scope, history_days=history_days, portfolio_id=portfolio_id)
    selected = [i for i in items if any(t.get('dimension') == dimension and t.get('name') == group for t in i.get('tags') or [])]
    if not selected:
        return {'ok': False, 'error': 'No items in selected group', 'series': []}

    refresh_result = {'attempted': 0, 'refreshed': 0, 'skippedRecentFailures': 0, 'errors': []}
    if refresh:
        ranked_for_refresh = sorted(
            selected,
            key=lambda item: (
                history_refresh_score(item),
                _positive_float(item.get('currentValue')) or _positive_float(item.get('latestPrice')) or 0,
            ),
            reverse=True,
        )
        refresh_result = refresh_steam_history_for_chart_items(
            ranked_for_refresh,
            limit=parse_int_param(refresh_limit, limit, minimum=0, maximum=80),
            force=force,
        )

    windows = volatility_windows_for_detail(history_days)
    max_window = max(windows) if windows else history_days
    max_cutoff = history_cutoff_ts(max_window)
    aliases = []
    item_by_alias = {}
    for item in selected:
        item_aliases = [item.get('marketHashName'), item.get('name')]
        item_aliases = [a for a in dict.fromkeys(str(a or '').strip() for a in item_aliases) if a]
        if not item_aliases:
            continue
        key = item_aliases[0]
        for alias in item_aliases:
            if alias not in item_by_alias:
                aliases.append(alias)
                item_by_alias[alias] = (key, item)

    rows_by_key = {}
    with db() as conn:
        for i in range(0, len(aliases), 400):
            chunk = aliases[i:i+400]
            if not chunk:
                continue
            qmarks = ','.join('?' for _ in chunk)
            params = list(chunk)
            if max_cutoff:
                params.append(max_cutoff)
            hist_rows = conn.execute(f"""
                SELECT name, point_ts, price, volume
                FROM price_history
                WHERE provider='steam' AND name IN ({qmarks}) AND price IS NOT NULL
                  {'AND point_ts >= ?' if max_cutoff else ''}
                ORDER BY name, point_ts
            """, params).fetchall()
            for row in hist_rows:
                mapped = item_by_alias.get(row['name'])
                if not mapped:
                    continue
                key, item = mapped
                rec = dict(row)
                rec['source'] = 'steam_history'
                rows_by_key.setdefault(key, {'item': item, 'rows': []})['rows'].append(rec)
    ranked = []
    now = int(time.time())
    for key, payload in rows_by_key.items():
        raw_rows = normalized_history_series(payload['rows'])
        if len(raw_rows) < 2:
            continue
        series_basis = classify_history_basis(raw_rows)
        metrics_by_window = {}
        selected_metric = None
        for win in windows:
            cutoff = history_cutoff_ts(win)
            win_rows = [r for r in raw_rows if not cutoff or int(r.get('point_ts', r.get('ts')) or 0) >= cutoff]
            metrics = volatility_metrics_from_rows(win_rows)
            if metrics.get('points', 0) >= 2:
                win_basis = classify_history_basis(win_rows)
                metrics['metricBasis'] = win_basis
                metrics['metricState'] = 'estimated' if win_basis == 'csgotrader_window_anchor' else ('real' if win_basis == 'steam_history' else 'partial')
                metrics_by_window[str(win)] = metrics
                if win == history_days:
                    selected_metric = metrics
        selected_metric = selected_metric or metrics_by_window.get(str(history_days))
        max_vol = max((_positive_float(m.get('volatilityPct')) or 0 for m in metrics_by_window.values()), default=0)
        selected_vol = _positive_float((selected_metric or {}).get('volatilityPct')) or 0
        if not metrics_by_window:
            continue
        ranked.append({
            'key': key,
            'item': payload['item'],
            'rows': raw_rows,
            'metricsByWindow': metrics_by_window,
            'sortVol': selected_vol or max_vol,
            'maxVol': max_vol,
            'lastTs': max((int(r.get('point_ts', r.get('ts')) or 0) for r in raw_rows), default=0),
        })
    ranked.sort(key=lambda r: (r['sortVol'], r['maxVol'], r['lastTs']), reverse=True)

    series = []
    for entry in ranked[:limit]:
        item = entry['item']
        line = item_history_line_points(entry['rows'], history_days=history_days, max_points=points)
        if len(line) < 2:
            continue
        metrics_summary = {}
        for win, metrics in entry['metricsByWindow'].items():
            metrics_summary[win] = {
                'volatilityPct': metrics.get('volatilityPct'),
                'trendPct': metrics.get('trendPct'),
                'rangePct': metrics.get('rangePct'),
                'points': metrics.get('points'),
                'metricBasis': metrics.get('metricBasis'),
                'metricState': metrics.get('metricState'),
            }
        series.append({
            'name': item.get('name'),
            'marketHashName': item.get('marketHashName') or item.get('name'),
            'image': item.get('image'),
            'kind': item.get('kind'),
            'itemType': item.get('itemType') or item.get('type'),
            'weapon': item.get('weapon'),
            'collection': item.get('collection'),
            'rarity': item.get('rarity'),
            'latestPrice': item.get('latestPrice'),
            'currentValue': item.get('currentValue'),
            'quantity': item.get('quantity'),
            'changePct': item.get('changePct'),
            'volatilityPct': entry['sortVol'],
            'basis': classify_history_basis(entry['rows']),
            'metricsByWindow': metrics_summary,
            'points': [
                {
                    'ts': int(p.get('ts') or 0),
                    'label': datetime.datetime.fromtimestamp(int(p.get('ts') or now), tz=datetime.timezone.utc).date().isoformat(),
                    'price': round(float(p.get('price') or 0), 4),
                    'volume': int(p.get('volume') or 0),
                }
                for p in line
            ],
        })

    return {
        'ok': True,
        'scope': scope,
        'dimension': dimension,
        'group': group,
        'historyDays': history_days,
        'windows': windows,
        'generatedAt': now,
        'basis': 'item_lines_ranked_by_multi_window_volatility',
        'basisDetail': 'Each item line is built from cached raw Steam chart history. Missing series are warmed from Steam on request.',
        'selectedItems': len(selected),
        'refresh': refresh_result,
        'series': series,
    }


def aggregate_group_history(scope='portfolio', dimension=None, group=None, history_days=90, points=120, portfolio_id=None, refresh=False, refresh_limit=0, force=False):
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    points = parse_int_param(points, 120, minimum=24, maximum=320)
    dimension = (dimension or '').strip()
    group = (group or '').strip()
    if not dimension or not group:
        return {'ok': False, 'error': 'Missing dimension or group', 'points': []}
    items = analysis_scope_items(scope=scope, history_days=history_days, portfolio_id=portfolio_id)
    selected = [i for i in items if any(t.get('dimension') == dimension and t.get('name') == group for t in i.get('tags') or [])]
    if not selected:
        return {'ok': False, 'error': 'No items in selected group', 'points': []}
    summary = summarize_group_items(items, dimension, group)
    summary.pop('items', None)
    chart_items = []
    for item in selected:
        qty = _positive_float(item.get('quantity')) if scope == 'portfolio' else 1
        chart_items.append({
            **item,
            'qty': qty or 1,
            'currentValue': item.get('currentValue'),
            'latestPrice': item.get('latestPrice'),
        })
    out = aggregate_value_history_for_items(
        chart_items,
        history_days=history_days,
        points=points,
        value_basis='market_one_each_basket_value' if scope == 'market' else 'portfolio_group_value',
        refresh=refresh,
        refresh_limit=refresh_limit,
        force=force,
    )
    coverage = out.get('coverage') or {}
    has_backfill = coverage.get('priceSource') == 'steam_chart_history_with_current_backfill'
    out.update({
        'scope': scope,
        'dimension': dimension,
        'group': group,
        'summary': summary,
        'basis': 'steam_history_cleaned_interpolated' if out.get('points') else 'history_unavailable',
        'basisDetail': (
            'Cleaned/interpolated raw Steam chart history with current-price backfill for group items without cached chart rows. Dashed points identify carried/backfilled values.'
            if has_backfill else
            'Cleaned/interpolated raw Steam chart history. Carried-forward points are flagged per point.'
        ) if out.get('points') else 'No qualified cached raw Steam chart history is available for this group. An authenticated refresh can request genuine observations within shared provider pacing and access limits.',
        'carriedForwardPoints': coverage.get('carriedForwardPoints', 0),
    })
    return out


def full_report():
    analysis = portfolio_analysis()
    exact_meta, base_meta, catalog_count = catalog_index()
    with db() as conn:
        portfolio = latest_portfolio(conn)
        raw_items = portfolio_inventory_rows(conn, portfolio['portfolio_id'], 10000) if portfolio else []
    full_items = [enrich_item(r, exact_meta, base_meta) for r in raw_items]
    report_vol = get_volatility_map([i.get('name') for i in full_items], 90)
    for i in full_items:
        i['volatility'] = report_vol.get(i.get('name')) or {}
    meta_hits = sum(1 for i in full_items if i.get('image') or i.get('rarity') or i.get('collection'))
    return {
        'generatedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'coverage': {
            **analysis['coverage'],
            'catalogItems': catalog_count,
            'metadataCoveragePct': round(meta_hits / len(full_items) * 100, 1) if full_items else 0,
            'volatilityCoverage': sum(1 for i in full_items if i.get('volatility', {}).get('points', 0) >= 2),
            'dateStart': None,
            'dateEnd': None,
            'dedupedWindows': 0,
            'priceSource': 'observed_market_only',
        },
        'horizons': analysis['horizons'],
        'latestSnapshots': analysis['latestSnapshots'],
        'duplicates': [],
        'windows': [],
        'series': {},
        'signals': analysis['signals'],
        'categories': [],
        'items': full_items,
    }


# HTTP handler
def h(value):
    return html.escape('' if value is None else str(value), quote=True)


def fmt_money(value):
    if value is None:
        return 'Unavailable'
    try:
        value = float(value)
        return '${:,.2f}'.format(value) if math.isfinite(value) else 'Unavailable'
    except (TypeError, ValueError):
        return 'Unavailable'


def fmt_pct(value):
    if value is None:
        return 'Unavailable'
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 'Unavailable'
    if not math.isfinite(value):
        return 'Unavailable'
    return '{}{:.2f}%'.format('+' if value > 0 else '', value)


def item_img(item):
    if item.get('image'):
        return '<img class="thumb" src="{}" alt="">'.format(h(item['image']))
    return '<span class="thumb missing"></span>'


def item_title(item):
    meta = ' / '.join([x for x in [item.get('rarity'), item.get('collection'), item.get('class')] if x])
    return (
        '<div class="item-id">{img}<div><strong>{name}</strong>'
        '<small>{meta}</small></div></div>'
    ).format(img=item_img(item), name=h(item.get('name')), meta=h(meta or item.get('category') or 'Unknown'))


def signal_rows(items):
    if not items:
        return '<tr><td colspan="6" class="muted">No entries.</td></tr>'
    return ''.join(
        '<tr><td>{title}</td><td>{p1}</td><td>{p3}</td><td>{p7}</td><td>{value}</td><td>{dollar}</td></tr>'.format(
            title=item_title(item),
            p1=fmt_pct(item.get('pct1')),
            p3=fmt_pct(item.get('pct3')),
            p7=fmt_pct(item.get('pct7')),
            value=fmt_money(item.get('value')),
            dollar=fmt_money(item.get('dollar')),
        )
        for item in items
    )


def item_rows(items):
    return ''.join(
        '<tr><td>{title}</td><td>{qty:g}</td><td>{value}</td><td>{dollar}</td><td>{h1}</td><td>{h3}</td><td>{h7}</td><td>{cat}</td></tr>'.format(
            title=item_title(item),
            qty=float(item.get('qty') or 0),
            value=fmt_money(item.get('curr')),
            dollar=fmt_money(item.get('dollar')),
            h1=fmt_pct((item.get('h1') or {}).get('pct')),
            h3=fmt_pct((item.get('h3') or {}).get('pct')),
            h7=fmt_pct((item.get('h7') or {}).get('pct')),
            cat=h(item.get('category')),
        )
        for item in items
    )


def category_table_rows(categories):
    categories = sorted(categories, key=lambda x: abs(float(x.get('dollar') or 0)), reverse=True)[:40]
    return ''.join(
        '<tr><td>{window}d</td><td>{cat}</td><td>{count}</td><td>{value}</td><td>{dollar}</td><td>{pct}</td><td>{up}</td><td>{down}</td></tr>'.format(
            window=h(c.get('horizon_days')),
            cat=h(c.get('category')),
            count=h(c.get('count')),
            value=fmt_money(c.get('value')),
            dollar=fmt_money(c.get('dollar')),
            pct=fmt_pct(c.get('pct')),
            up=h(c.get('up')),
            down=h(c.get('down')),
        )
        for c in categories
    )


def duplicate_rows(duplicates):
    if not duplicates:
        return '<tr><td colspan="4" class="muted">No duplicate windows detected.</td></tr>'
    return ''.join(
        '<tr><td>{date}</td><td>{window}d</td><td>{count}</td><td>{kept}</td></tr>'.format(
            date=h(d.get('date')),
            window=h(d.get('horizonDays')),
            count=h(d.get('count')),
            kept=h(d.get('kept')),
        )
        for d in duplicates[:80]
    )


def enrich_signal_list(signals, items):
    by_base = {i.get('base_name'): i for i in items}
    by_name = {i.get('name'): i for i in items}
    enriched = []
    for signal in signals:
        meta = by_base.get(signal.get('baseName')) or by_name.get(signal.get('name')) or {}
        enriched.append({**meta, **signal, 'image': meta.get('image')})
    return enriched


def render_login_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>cs2dash - Sign in</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; min-height: 100vh; display: flex; align-items: center;
    justify-content: center; background: #0e1116; color: #e6e6e6;
    font: 15px/1.4 system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
  .card { width: 320px; padding: 28px; background: #161b22; border: 1px solid #262c36;
    border-radius: 12px; box-shadow: 0 10px 40px rgba(0,0,0,.4); }
  h1 { margin: 0 0 4px; font-size: 20px; }
  p.sub { margin: 0 0 20px; color: #8b949e; font-size: 13px; }
  label { display: block; font-size: 12px; color: #8b949e; margin: 14px 0 4px; }
  input { width: 100%; padding: 10px 12px; background: #0e1116; color: #e6e6e6;
    border: 1px solid #30363d; border-radius: 8px; font-size: 14px; }
  input:focus { outline: none; border-color: #388bfd; }
  button { width: 100%; margin-top: 20px; padding: 10px; background: #238636;
    color: #fff; border: none; border-radius: 8px; font-size: 14px; font-weight: 600;
    cursor: pointer; }
  button:hover { background: #2ea043; }
  button:disabled { opacity: .6; cursor: default; }
  .err { margin-top: 14px; color: #f85149; font-size: 13px; min-height: 18px; }
</style>
</head>
<body>
  <form class="card" id="f" autocomplete="on">
    <h1>cs2dash</h1>
    <p class="sub">Sign in to continue</p>
    <label for="u">Username</label>
    <input id="u" name="username" autocomplete="username" autofocus required>
    <label for="p">Password</label>
    <input id="p" name="password" type="password" autocomplete="current-password" required>
    <button id="b" type="submit">Sign in</button>
    <div class="err" id="e"></div>
  </form>
<script>
  const f = document.getElementById('f'), b = document.getElementById('b'), e = document.getElementById('e');
  f.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    e.textContent = ''; b.disabled = true;
    try {
      const r = await fetch('/api/login', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: f.username.value, password: f.password.value })
      });
      if (r.ok) { window.location.href = '/'; return; }
      const j = await r.json().catch(() => ({}));
      e.textContent = j.error || 'Sign in failed';
    } catch (_) { e.textContent = 'Network error'; }
    b.disabled = false;
  });
</script>
</body>
</html>"""


def render_report_page():
    report = full_report()
    coverage = report['coverage']
    items = report['items']
    signals = report['signals']
    image_count = sum(1 for item in items if item.get('image'))
    generated = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    return """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>cs2dash Market Report</title>
<style>
:root {{ color-scheme: dark; --bg:#0b1118; --panel:#101923; --line:#263445; --text:#e8eef6; --muted:#91a0b2; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Inter, Segoe UI, Arial, sans-serif; }}
header, main {{ max-width:1440px; margin:0 auto; padding:24px; }}
header {{ display:flex; justify-content:space-between; gap:20px; align-items:end; border-bottom:1px solid var(--line); }}
h1, h2 {{ margin:0; letter-spacing:0; }}
h1 {{ font-size:28px; }}
h2 {{ font-size:18px; margin:24px 0 10px; }}
.muted, small {{ color:var(--muted); }}
.grid {{ display:grid; grid-template-columns:repeat(6, minmax(120px, 1fr)); gap:10px; margin-top:18px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:12px; }}
.card b {{ display:block; font-size:20px; margin-top:4px; }}
.two {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
table {{ width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }}
th, td {{ padding:10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:middle; }}
th {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
tr:last-child td {{ border-bottom:0; }}
.item-id {{ display:flex; align-items:center; gap:10px; min-width:260px; }}
.item-id strong, .item-id small {{ display:block; }}
.thumb {{ width:52px; height:38px; object-fit:contain; flex:0 0 52px; background:#0a0f15; border:1px solid var(--line); border-radius:6px; }}
.missing {{ display:inline-block; }}
.pill {{ display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; color:var(--muted); }}
@media (max-width:1000px) {{ .grid, .two {{ grid-template-columns:1fr 1fr; }} header {{ display:block; }} }}
@media (max-width:700px) {{ header, main {{ padding:16px; }} .grid, .two {{ grid-template-columns:1fr; }} table {{ display:block; overflow-x:auto; }} }}
</style>
</head>
<body>
<header>
  <div><h1>cs2dash Market Report</h1><div class="muted">Server-rendered with direct item images. Generated {generated}.</div></div>
  <div class="pill">{date_start} to {date_end}</div>
</header>
<main>
  <section class="grid">
    <div class="card">Catalog items<b>{catalog}</b></div>
    <div class="card">Tracked items<b>{items}</b></div>
    <div class="card">Items with images<b>{images}</b></div>
    <div class="card">Metadata coverage<b>{coverage}%</b></div>
    <div class="card">Indexed rows<b>{rows}</b></div>
    <div class="card">Deduped windows<b>{windows}</b></div>
  </section>
  <section class="two">
    <div><h2>Consistent Strength</h2><table><thead><tr><th>Item</th><th>1d</th><th>3d</th><th>7d</th><th>Value</th><th>Move</th></tr></thead><tbody>{up}</tbody></table></div>
    <div><h2>Consistent Weakness</h2><table><thead><tr><th>Item</th><th>1d</th><th>3d</th><th>7d</th><th>Value</th><th>Move</th></tr></thead><tbody>{down}</tbody></table></div>
  </section>
  <section class="two">
    <div><h2>Reversal Up</h2><table><thead><tr><th>Item</th><th>1d</th><th>3d</th><th>7d</th><th>Value</th><th>Move</th></tr></thead><tbody>{rev_up}</tbody></table></div>
    <div><h2>Reversal Down</h2><table><thead><tr><th>Item</th><th>1d</th><th>3d</th><th>7d</th><th>Value</th><th>Move</th></tr></thead><tbody>{rev_down}</tbody></table></div>
  </section>
  <h2>Category Momentum</h2>
  <table><thead><tr><th>Window</th><th>Category</th><th>Items</th><th>Value</th><th>Move</th><th>%</th><th>Up</th><th>Down</th></tr></thead><tbody>{cats}</tbody></table>
  <h2>Duplicate Imported Windows</h2>
  <table><thead><tr><th>Date</th><th>Window</th><th>Copies</th><th>Kept</th></tr></thead><tbody>{dupes}</tbody></table>
  <h2>Full Item Appendix</h2>
  <table><thead><tr><th>Item</th><th>Qty</th><th>Value</th><th>Move</th><th>1d</th><th>3d</th><th>7d</th><th>Category</th></tr></thead><tbody>{item_rows}</tbody></table>
</main>
</body>
</html>""".format(
        generated=h(generated),
        date_start=h(coverage.get('dateStart')),
        date_end=h(coverage.get('dateEnd')),
        catalog=h(coverage.get('catalogItems')),
        items=h(len(items)),
        images=h(image_count),
        coverage=h(coverage.get('metadataCoveragePct')),
        rows=h(coverage.get('indexedRows')),
        windows=h(coverage.get('dedupedWindows')),
        up=signal_rows(enrich_signal_list(signals.get('broadUp', []), items)),
        down=signal_rows(enrich_signal_list(signals.get('broadDown', []), items)),
        rev_up=signal_rows(enrich_signal_list(signals.get('reversalUp', []), items)),
        rev_down=signal_rows(enrich_signal_list(signals.get('reversalDown', []), items)),
        cats=category_table_rows(report.get('categories', [])),
        dupes=duplicate_rows(report.get('duplicates', [])),
        item_rows=item_rows(items),
    )


class Handler(http.server.BaseHTTPRequestHandler):

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, BrokenPipeError):
            return
        super().handle_error(request, client_address)

    def log_message(self, fmt, *args):
        if args and str(args[1]) not in ('200', '204', '304'):
            super().log_message(fmt, *args)

    def safe_write(self, body):
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def send_json(self, data, status=200, extra_headers=None):
        body = json.dumps(data, separators=(',', ':')).encode('utf-8')
        use_gzip = (
            len(body) >= 1024
            and 'gzip' in (self.headers.get('Accept-Encoding') or '').lower()
        )
        if use_gzip:
            body = gzip.compress(body, compresslevel=5)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        if use_gzip:
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Vary', 'Accept-Encoding')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        for name, value in (extra_headers or []):
            self.send_header(name, value)
        self.end_headers()
        self.safe_write(body)

    def read_body_json(self):
        length = int(self.headers.get('Content-Length', 0))
        if length > MAX_BODY_BYTES:
            raise ValueError('Request body too large')
        raw = self.rfile.read(length)
        return json.loads(raw)

    def do_OPTIONS(self):
        # Same-origin app; no cross-origin CORS surface is exposed.
        self.send_response(204)
        self.send_header('Allow', 'GET, POST, DELETE, OPTIONS')
        self.end_headers()

    def _cookies(self):
        jar = {}
        raw = self.headers.get('Cookie', '') or ''
        for part in raw.split(';'):
            if '=' in part:
                key, value = part.split('=', 1)
                jar[key.strip()] = value.strip()
        return jar

    def current_user(self):
        if not AUTH_ENABLED:
            return AUTH_USERNAME
        path = self.path.split('?')[0]
        readonly = self.command in ('GET','HEAD')
        return get_session_user(self._cookies().get(SESSION_COOKIE), readonly=readonly)

    def require_auth(self, path, method):
        """Return True if the request may proceed; otherwise emit a denial."""
        if not AUTH_ENABLED:
            return True
        if PUBLIC_READONLY and method == 'GET' and path in ANON_GET_PATHS:
            return True
        if method == 'GET' and path in PUBLIC_GET_PATHS:
            return True
        if method == 'POST' and path in PUBLIC_POST_PATHS:
            return True
        if self.current_user():
            return True
        if path.startswith('/api/') or method != 'GET':
            self.send_json({'error': 'Authentication required'}, 401)
        else:
            self.send_response(302)
            self.send_header('Location', '/login')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
        return False

    def do_GET(self):
        try:
            return self._do_GET_with_capacity_response()
        except LegacyBulkCapacityDeferred as error:
            return self.send_json({'ok': False, 'state': 'capacity_deferred',
                                   'error': str(error), 'savedDataAvailable': True,
                                   'currentCashIndependent': True}, 503)

    def _do_GET_with_capacity_response(self):
        path = self.path.split('?')[0]

        if not self.require_auth(path, 'GET'):
            return

        user = self.current_user()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if path in ('/api/canonical-cash-value', '/api/canonical-cash-prices'):
            try:
                if path == '/api/canonical-cash-value':
                    value = canonical_cash_service().value(query.get('portfolio_id', [None])[0])
                else:
                    names_json = query.get('names', ['[]'])[0]
                    if len(names_json.encode('utf-8')) > 131072:
                        raise ValueError('Exact-name request exceeds its bound')
                    value = canonical_cash_service().prices(json.loads(names_json))
                self.send_json(value)
            except cash_prices.CashError as error:
                self.send_json({'error': str(error)}, error.status)
            except (ValueError, UnicodeError) as error:
                self.send_json({'error': str(error)}, 400)
            except (sqlite3.Error, bulk_derived_staging.Deferred):
                self.send_json({'error': 'Saved current cash quotes are unavailable; no refresh was requested.'}, 503)
            return

        if path in ('/api/cash-value', '/api/cash-price-jobs'):
            try:
                if query.get('provider', ['skinport_live'])[0] != cash_prices.PROVIDER:
                    raise ValueError('Only skinport_live cash pricing is supported')
                if path == '/api/cash-value':
                    value = cash_price_service().value(query.get('portfolio_id', [None])[0])
                else:
                    value = cash_price_service().status(query.get('id', [None])[0])
                    value['cadence'] = CASH_PRICE_CADENCE.status(value)
                    value['holdingsIdentityRevision'] = cash_holdings_identity_revision()
                self.send_json(value)
            except cash_prices.CashError as error:
                self.send_json({'error': str(error), 'provider': cash_prices.PROVIDER}, error.status)
            except (ValueError, UnicodeError) as error:
                self.send_json({'error': str(error)}, 400)
            except sqlite3.Error:
                self.send_json({'error': 'Saved cash storage is unavailable; no refresh was requested.'}, 503)
            return

        if path == '/api/bulk-derived-status':
            if self.client_address[0] not in LOCAL_ADDRS or not self.require_auth(path,'GET'):
                if self.client_address[0] not in LOCAL_ADDRS:
                    self.send_json({'error':'Derived operator status is local-only'},403)
                return
            try:
                with closing(steam_ask_db()) as status_conn:
                    status_conn.execute('BEGIN')
                    result = BULK_STAGING_RUNTIME.saved_status(status_conn)
                    head = bulk_derived_staging.get_json(status_conn,bulk_derived_staging.PUBLISHED_KEY)
                    result['publishedToken'] = head.get('publishToken') if head else None
                self.send_json(result)
            except (sqlite3.Error,bulk_derived_staging.Deferred,OSError) as error:
                self.send_json({'error':str(error)},503)
            return

        if path in ('/api/steam-ask-value', '/api/steam-ask-jobs'):
            try:
                if path == '/api/steam-ask-value':
                    portfolio_id = query.get('portfolio_id', [None])[0]
                    if portfolio_id is not None and len(portfolio_id) > 128:
                        raise ValueError('Invalid portfolio ID')
                    value = steam_ask_value_response(portfolio_id, int(query.get('freshness_seconds', ['3600'])[0]), summary=query.get('summary',['0'])[0] == '1')
                else:
                    job_id = query.get('id', [None])[0]
                    if job_id is not None and re.fullmatch(r'[0-9a-f]{32}', job_id) is None:
                        raise ValueError('Invalid job ID')
                    value = steam_ask_job_response(job_id)
                self.send_json(value)
            except ValueError as error:
                self.send_json({'error': str(error)}, 400)
            except (sqlite3.Error, steam_held_asks.AskSchemaUnavailable, steam_request_governor.GovernorError):
                self.send_json({'error': 'Durable Steam ask storage or request governor is unavailable.'}, 503)
            return

        if PUBLIC_READONLY and not user:
            query = anon_sanitize_query(query)

        if path in ITEM_READ_PATHS or (CATALOG_COMPATIBILITY_FEED_ENABLED and path == '/api/catalog'):
            payload, status = catalog_read_payload(path, query)
            self.send_json(payload, status)
            return

        if path == '/api/health':
            self.send_json({'ok': True, 'app': APP_NAME, 'version': APP_VERSION,
                            'build': globals().get('BUILD_IDENTITY')})

        elif path == '/login':
            body = render_login_page().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.safe_write(body)

        elif path in ('/report', '/report.html'):
            body = render_report_page().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.safe_write(body)

        elif path in ('/', '/index.html'):
            html_path = os.path.join(BASE_DIR, 'index.html')
            try:
                with open(html_path, 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.safe_write(body)
            except FileNotFoundError:
                self.send_error(404, 'index.html not found')

        elif path == '/favicon.ico':
            self.send_response(204)
            self.send_header('Cache-Control', 'public, max-age=86400')
            self.end_headers()

        elif path == '/api/status':
            self.send_json(application_status())

        elif path == '/api/snaps':
            self.send_json(get_snapshots())

        elif path == '/api/portfolios':
            self.send_json(get_portfolios())

        elif path == '/api/settings':
            self.send_json(get_settings())

        elif path == '/api/access':
            self.send_json({
                'canDelete': bool(user),
                'canEdit': bool(user),
                'readonly': PUBLIC_READONLY and not user,
            })

        elif path == '/api/catalog':
            self.send_json(catalog_payload())

        elif path == '/api/prices':
            raw_names = query.get('names', ['[]'])[0]
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            try:
                names = json.loads(raw_names)
            except json.JSONDecodeError:
                names = raw_names.split('|') if raw_names else []
            self.send_json(get_price_quotes(names, force=force))

        elif path == '/api/shared-pricing-status':
            self.send_json(shared_pricing_status())

        elif path == '/api/shared-item-pricing':
            name = query.get('name', [''])[0]
            history_limit = query.get('history_limit', ['500'])[0]
            self.send_json(shared_item_pricing(name, history_limit=history_limit))

        elif path == '/api/history':
            name = query.get('name', [''])[0]
            market_hash_name = query.get('hash', [''])[0] or query.get('market_hash_name', [''])[0]
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            self.send_json(get_price_history(name, force=force, market_hash_name=market_hash_name or None))

        elif path == '/api/analysis':
            days = query.get('days', query.get('scale', query.get('history_days', query.get('history', ['90']))))[0]
            limit = query.get('limit', ['120'])[0]
            min_group = query.get('min_group', ['3'])[0]
            self.send_json(market_universe_analysis(history_days=days, limit=limit, min_group=min_group))

        elif path == '/api/market-universe':
            days = query.get('days', query.get('history_days', ['90']))[0]
            limit = query.get('limit', ['120'])[0]
            min_group = query.get('min_group', ['3'])[0]
            self.send_json(market_universe_analysis(history_days=days, limit=limit, min_group=min_group))

        elif path == '/api/market-items':
            days = query.get('days', query.get('history_days', ['90']))[0]
            limit = query.get('limit', ['60000'])[0]
            self.send_json(market_universe_item_slice(
                history_days=days,
                limit=limit,
                search=query.get('search', [''])[0],
                terms=query.get('terms', [''])[0],
                blacklist=query.get('blacklist', [''])[0],
                collection=query.get('collection', [''])[0],
                item_type=query.get('type', [''])[0],
                grade=query.get('grade', [''])[0],
                min_price=query.get('min_price', [''])[0],
                max_price=query.get('max_price', [''])[0],
                sort_key=query.get('sort', ['changePct'])[0],
                sort_dir=query.get('dir', ['-1'])[0],
                projection=query.get('projection', [''])[0],
                offset=query.get('offset', ['0'])[0],
            ))

        elif path == '/api/taxonomy-analysis':
            scope = query.get('scope', ['portfolio'])[0]
            days = query.get('days', query.get('history_days', ['90']))[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            min_group = query.get('min_group', ['2'])[0]
            top_groups = query.get('top_groups', ['12'])[0]
            self.send_json(taxonomy_analysis(
                scope=scope,
                history_days=days,
                portfolio_id=portfolio_id,
                min_group=min_group,
                top_groups=top_groups,
            ))

        elif path == '/api/group-history':
            scope = query.get('scope', ['portfolio'])[0]
            days = query.get('days', query.get('history_days', ['90']))[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            points = query.get('points', ['120'])[0]
            dimension = query.get('dimension', [''])[0]
            group = query.get('group', [''])[0]
            refresh = query.get('refresh', ['0'])[0].lower() in ('1', 'true', 'yes')
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            refresh_limit = query.get('refresh_limit', ['12'])[0]
            self.send_json(aggregate_group_history(
                scope=scope,
                dimension=dimension,
                group=group,
                history_days=days,
                points=points,
                portfolio_id=portfolio_id,
                refresh=refresh,
                refresh_limit=refresh_limit,
                force=force,
            ))

        elif path == '/api/group-item-history':
            scope = query.get('scope', ['portfolio'])[0]
            days = query.get('days', query.get('history_days', ['90']))[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            points = query.get('points', ['140'])[0]
            limit = query.get('limit', ['10'])[0]
            dimension = query.get('dimension', [''])[0]
            group = query.get('group', [''])[0]
            refresh = query.get('refresh', ['0'])[0].lower() in ('1', 'true', 'yes')
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            refresh_limit = query.get('refresh_limit', [limit])[0]
            self.send_json(group_item_history(
                scope=scope,
                dimension=dimension,
                group=group,
                history_days=days,
                points=points,
                limit=limit,
                portfolio_id=portfolio_id,
                refresh=refresh,
                refresh_limit=refresh_limit,
                force=force,
            ))

        elif path == '/api/market-universe-sync':
            mode = query.get('mode', ['batch'])[0].lower()
            if mode == 'bulk':
                provider = query.get('provider', ['auto'])[0]
                source = 'all'
                warm_history = query.get('warm_history', ['1'])[0].lower() not in ('0', 'false', 'no')
                history_limit = query.get('history_limit', [str(DEFAULT_SYNC_STEAM_HISTORY_LIMIT)])[0]
                retry_history_failures = query.get('retry_history_failures', ['0'])[0].lower() in ('1', 'true', 'yes')
                portfolio_id = query.get('portfolio', [''])[0] or None
                self.send_json(market_universe_bulk_sync(
                    provider=provider,
                    preferred_source=source,
                    warm_history=warm_history,
                    history_limit=history_limit,
                    portfolio_id=portfolio_id,
                    retry_history_failures=retry_history_failures,
                ))
                return
            offset = query.get('offset', ['0'])[0]
            limit = query.get('limit', ['20'])[0]
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            kind = query.get('kind', [''])[0] or None
            self.send_json(market_universe_sync_batch(offset=offset, limit=limit, force=force, kind=kind))

        elif path == '/api/market-universe-status':
            self.send_json(market_universe_status(projection=query.get('projection', [None])[0]))

        elif path == '/api/data-tasks':
            self.send_json(data_task_snapshot())

        elif path == '/api/inventory':
            projection = query.get('projection', [''])[0]
            if projection == 'portfolio-value-v1':
                if any(query.get(key, ['0'])[0].lower() in ('1', 'true', 'yes')
                       for key in ('refresh', 'refresh_history', 'force')):
                    self.send_json({'error': 'portfolio-value-v1 only reads saved current quotes'}, 400)
                    return
                self.send_json(get_portfolio_current_value(
                    limit=query.get('limit', ['2000'])[0],
                    portfolio_id=query.get('portfolio', [''])[0] or None))
                return
            scale = query.get('days', query.get('scale', ['90']))[0]
            hist_days = query.get('history_days', query.get('history', [None]))[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            limit = query.get('limit', ['2000'])[0]
            refresh = query.get('refresh', ['0'])[0].lower() in ('1', 'true', 'yes')
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            refresh_limit = query.get('refresh_limit', ['25'])[0]
            refresh_history = query.get('refresh_history', ['0'])[0].lower() in ('1', 'true', 'yes')
            history_limit = query.get('history_limit', ['10'])[0]
            self.send_json(get_inventory_dataset(
                scale_days=scale,
                history_days=hist_days,
                limit=limit,
                refresh=refresh,
                refresh_limit=refresh_limit,
                refresh_history=refresh_history,
                history_limit=history_limit,
                portfolio_id=portfolio_id,
                force=force,
            ))

        elif path == '/api/portfolio-history':
            days = query.get('days', query.get('history_days', ['90']))[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            points = query.get('points', ['220'])[0]
            refresh = query.get('refresh', ['0'])[0].lower() in ('1', 'true', 'yes')
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            refresh_limit = query.get('refresh_limit', ['24'])[0]
            self.send_json(portfolio_value_history(
                portfolio_id=portfolio_id,
                history_days=days,
                points=points,
                refresh=refresh,
                refresh_limit=refresh_limit,
                force=force,
            ))

        elif path == '/api/portfolio-sync':
            scale = query.get('days', query.get('scale', ['90']))[0]
            hist_days = query.get('history_days', [None])[0]
            portfolio_id = query.get('portfolio', [''])[0] or None
            offset = query.get('offset', ['0'])[0]
            limit = query.get('limit', ['20'])[0]
            include_history = query.get('history', ['1'])[0].lower() in ('1', 'true', 'yes')
            force = query.get('force', ['1'])[0].lower() in ('1', 'true', 'yes')
            self.send_json(portfolio_sync_batch(
                scale_days=scale,
                history_days=hist_days,
                offset=offset,
                limit=limit,
                include_history=include_history,
                portfolio_id=portfolio_id,
                force=force,
            ))

        elif path == '/api/report':
            self.send_json(full_report())

        else:
            self.send_error(404)

    def do_POST(self):
        path = self.path.split('?')[0]
        if not self.require_auth(path, 'POST'):
            return

        if path == '/api/bulk-derived-control':
            if self.client_address[0] not in LOCAL_ADDRS:
                self.send_json({'error':'Derived operator control is local-only'},403)
                return
            try:
                length = int(self.headers.get('Content-Length','0'))
                if length < 1 or length > 16384 or self.headers.get('Transfer-Encoding') or self.headers.get('Content-Type','').split(';')[0].strip().lower() != 'application/json':
                    raise ValueError('One bounded derived-operator JSON object is required')
                self.send_json(BULK_STAGING_RUNTIME.control(self.read_body_json()))
            except (ValueError,UnicodeError) as error:
                self.send_json({'error':str(error)},400)
            except (sqlite3.Error,bulk_derived_staging.Deferred,bulk_derived_staging.PublishUncertain,OSError) as error:
                self.send_json({'error':str(error),'derivedStatus':BULK_STAGING_RUNTIME.snapshot()},503)
            return

        if path == '/api/steam-request-control':
            try:
                length = int(self.headers.get('Content-Length','0'))
                if length < 1 or length > 2048 or self.headers.get('Transfer-Encoding') or self.headers.get('Content-Type','').split(';')[0].strip().lower() != 'application/json':
                    raise ValueError('One small request-control object is required')
                self.send_json(steam_governor_control(self.read_body_json()))
            except (ValueError, UnicodeError) as error:
                self.send_json({'error':str(error)},400)
            except (sqlite3.Error,steam_request_governor.GovernorError):
                self.send_json({'error':'Saved Steam request control is unavailable; no request was permitted.'},503)
            return

        if path == '/api/cash-price-jobs':
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if (length < 1 or length > 2048 or self.headers.get('Transfer-Encoding')
                        or self.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json'):
                    raise ValueError('One small JSON object is required')
                value = cash_price_service().action(self.read_body_json())
                self.send_json(value, 202 if value.get('accepted') else 200)
            except cash_prices.CashCooldown as error:
                self.send_json({'error': str(error), 'provider': cash_prices.PROVIDER,
                                'retryAt': error.retry_at, 'cooldownReason': error.reason}, 429)
            except cash_prices.CashError as error:
                self.send_json({'error': str(error), 'provider': cash_prices.PROVIDER}, error.status)
            except (ValueError, UnicodeError) as error:
                self.close_connection = True
                self.send_json({'error': str(error)}, 400)
            except sqlite3.Error:
                self.send_json({'error': 'Cash job storage is unavailable; no completed write is claimed.'}, 503)
            return

        if path in ('/api/steam-current-ask', '/api/steam-ask-jobs'):
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if (length < 1 or length > 2048 or self.headers.get('Transfer-Encoding')
                        or self.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json'):
                    raise ValueError('One small JSON object is required')
                data = self.read_body_json()
                if path == '/api/steam-current-ask':
                    if not isinstance(data, dict) or set(data) != {'marketHashName'}:
                        raise ValueError('Only marketHashName is accepted')
                    value = durable_steam_current_ask(validate_market_hash(data['marketHashName']))
                else:
                    value = steam_ask_job_action(data)
                self.send_json(value)
            except (ValueError, UnicodeError) as error:
                self.close_connection = True
                self.send_json({'error': str(error)}, 400)
            except steam_held_asks.AskJobConflict as error:
                self.send_json({'error': str(error), 'activeJobId': error.job_id}, 409)
            except (sqlite3.Error, steam_held_asks.AskSchemaUnavailable):
                self.send_json({'error': 'Durable Steam ask storage is unavailable; no completed write is claimed.'}, 503)
            return

        if path == '/api/logout':
            delete_session(self._cookies().get(SESSION_COOKIE))
            self.send_json(
                {'ok': True},
                extra_headers=[('Set-Cookie', build_session_cookie('', 0))],
            )
            return

        try:
            data = self.read_body_json()
        except (json.JSONDecodeError, ValueError):
            self.send_json({'error': 'Invalid JSON'}, 400)
            return

        if path == '/api/login':
            username = (data.get('username') or '').strip() if isinstance(data, dict) else ''
            password = (data.get('password') or '') if isinstance(data, dict) else ''
            if (AUTH_ENABLED and AUTH_HASH and username == AUTH_USERNAME
                    and verify_password(password, AUTH_HASH)):
                token = create_session(username)
                purge_expired_sessions()
                self.send_json(
                    {'ok': True},
                    extra_headers=[('Set-Cookie', build_session_cookie(token, SESSION_TTL_SECONDS))],
                )
            else:
                self.send_json({'error': 'Invalid credentials'}, 401)
            return

        if path == '/api/snaps':
            replace_snapshots(data)
            self.send_json({'ok': True, 'saved': len(data) if isinstance(data, list) else 1})

        elif path == '/api/portfolios':
            replace_portfolios(data)
            self.send_json({'ok': True, 'saved': len(data) if isinstance(data, list) else 1})

        elif path == '/api/settings':
            set_settings(data)
            self.send_json({'ok': True})

        elif path == '/api/catalog/refresh':
            refresh_catalog(True)
            self.send_json({'ok': True})

        elif path == '/api/data-tasks':
            self.send_json(start_data_task(data if isinstance(data, dict) else {}))

        else:
            self.send_error(404)

    def do_DELETE(self):
        path = self.path.split('?')[0]
        if not self.require_auth(path, 'DELETE'):
            return

        # DELETE /api/snaps/<id>
        if path.startswith('/api/snaps/'):
            snap_id = path.split('/api/snaps/')[1]
            if not snap_id:
                self.send_json({'error': 'Missing snapshot ID'}, 400)
                return
            snaps = get_snapshots()
            before = len(snaps)
            snaps = [s for s in snaps if s.get('id') != snap_id]
            if len(snaps) == before:
                self.send_json({'error': 'Snapshot not found'}, 404)
                return
            replace_snapshots(snaps)
            self.send_json({'ok': True, 'remaining': len(snaps)})

        elif path.startswith('/api/portfolios/'):
            portfolio_id = path.split('/api/portfolios/')[1]
            if not portfolio_id:
                self.send_json({'error': 'Missing portfolio ID'}, 400)
                return
            portfolios = get_portfolios()
            before = len(portfolios)
            portfolios = [p for p in portfolios if p.get('id') != portfolio_id]
            if len(portfolios) == before:
                self.send_json({'error': 'Portfolio not found'}, 404)
                return
            replace_portfolios(portfolios)
            self.send_json({'ok': True, 'remaining': len(portfolios)})

        else:
            self.send_error(404)


# Network helpers
def get_lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('1.1.1.1', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


# Background refresh and entry point
def _automatic_refresh_defaults():
    return {'active': False, 'status': 'idle', 'phase': 'idle',
            'startedAt': None, 'updatedAt': None, 'finishedAt': None,
            'source': None, 'windowDays': None, 'receivedRows': 0, 'processedRows': 0,
            'committedProcessedRows': None, 'commitState': 'not_started',
            'providerErrors': 0, 'error': None}


def automatic_refresh_snapshot():
    # Scalar-only bounded state; status reads never persist these events.
    with AUTO_REFRESH_STATUS_LOCK:
        return {**_automatic_refresh_defaults(), **AUTO_REFRESH_STATUS}


def _automatic_refresh_begin():
    now = int(time.time())
    with AUTO_REFRESH_STATUS_LOCK:
        AUTO_REFRESH_STATUS.clear()
        AUTO_REFRESH_STATUS.update(_automatic_refresh_defaults())
        AUTO_REFRESH_STATUS.update(active=True, status='running', phase='fetching',
                                   startedAt=now, updatedAt=now)


def automatic_refresh_progress(stage, message, **payload):
    def count(value):
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError, OverflowError):
            return 0
    with AUTO_REFRESH_STATUS_LOCK:
        if not AUTO_REFRESH_STATUS.get('active'):
            return
        state = AUTO_REFRESH_STATUS
        state['updatedAt'] = int(time.time())
        if payload.get('source') is not None:
            state['source'] = str(payload['source'])[:100]
        if stage == 'provider_fetch':
            state['phase'] = 'fetching'
        elif stage == 'provider_received':
            state['receivedRows'] += count(payload.get('rows'))
        elif stage == 'provider_error':
            state['providerErrors'] += 1
        elif stage == 'waiting_for_writer':
            state['phase'] = 'waiting_for_writer'
        elif stage == 'provider_transaction_started':
            state.update(phase='writing', commitState='uncommitted')
        elif stage == 'provider_stored':
            # Includes unchanged valid rows, not a count of new observations.
            state['processedRows'] += count(payload.get('stored'))
            state.update(phase='writing', commitState='uncommitted')
        elif stage == 'rebuilding_metrics':
            state.update(phase='rebuilding_metrics', source=None,
                         windowDays=payload.get('windowDays'), commitState='uncommitted')
        elif stage == 'source_transaction_committed':
            state.update(phase='derived_pending', source=None, windowDays=None,
                         commitState='source_committed_derived_pending',
                         committedProcessedRows=count(payload.get('processedRows')))
        elif stage == 'provider_transaction_committed':
            state.update(phase='finishing', source=None, windowDays=None, commitState='committed',
                         committedProcessedRows=count(payload.get('processedRows')))


# Only this pure callback receives progress while the automatic writer is held.
automatic_refresh_progress._memory_only = True


def _automatic_refresh_finish(result=None, error=None):
    with AUTO_REFRESH_STATUS_LOCK:
        state = AUTO_REFRESH_STATUS
        result = result if isinstance(result, dict) else {}
        state['providerErrors'] = max(state.get('providerErrors', 0), len(result.get('errors') or []))
        committed = state.get('commitState') in ('committed', 'source_committed_derived_pending')
        succeeded = error is None and bool(result.get('ok')) and committed
        now = int(time.time())
        state.update(active=False, updatedAt=now, finishedAt=now, source=None, windowDays=None)
        if succeeded:
            state.update(status='completed_with_errors' if state['providerErrors'] else 'completed',
                         phase='derived_pending' if state.get('commitState') == 'source_committed_derived_pending' else 'completed', error=None)
        else:
            # A later summary failure may follow a successful data commit.
            # Without the commit event, do not claim the transaction rolled back.
            if state.get('commitState') == 'uncommitted':
                state['commitState'] = 'unknown'
            detail = str(error) if error is not None else str(result.get('error') or 'Automatic refresh did not complete with a confirmed commit.')
            state.update(status='failed', phase='failed', error=detail[:1000])


def run_automatic_market_refresh():
    _automatic_refresh_begin()
    try:
        result = market_universe_bulk_sync('csgotrader', 'any', progress=automatic_refresh_progress)
    except BaseException as error:
        _automatic_refresh_finish(error=error)
        raise
    _automatic_refresh_finish(result=result)
    return result


def market_refresh_due_from_receipts(receipts, now, interval, automatic=None):
    """Fetch cadence only: saved local HTTP completion is not quote freshness."""
    if interval <= 0 or type(now) is not int or now <= 0 or not receipts:
        return False
    automatic = automatic if isinstance(automatic, dict) else {}
    if automatic.get('active'):
        return False
    finished = automatic.get('finishedAt')
    if (automatic.get('status') in ('failed', 'completed_with_errors')
            and type(finished) is int and 0 < finished <= now
            and now - finished < max(300, interval)):
        return False  # Preserve this process's existing error/rate-limit sleep.
    sources = {'steam_snapshot':'steam', 'csfloat':'csfloat', 'buff163':'buff163',
               'youpin':'youpin', 'skinport':'skinport'}
    clocks = []
    for provider, receipt in receipts.items():
        if provider not in sources or not isinstance(receipt, dict):
            continue
        fetched = receipt.get('fetchedAt')
        digest = receipt.get('contentSHA256')
        received, valid = receipt.get('receivedRows'), receipt.get('validRows')
        if (type(receipt.get('version')) is int and receipt['version'] == 1
                and receipt.get('provider') == provider
                and receipt.get('source') == 'csgotrader:' + sources[provider]
                and receipt.get('fetchReceiptState') == 'last_successful_fetch'
                and type(fetched) is int and 0 < fetched <= now
                and isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest)
                and type(received) is int and type(valid) is int and 0 <= valid <= received):
            clocks.append(fetched)
    # Missing/old/malformed provenance permits the ordinary initial/expired pull;
    # never replace it with a global quote scan or an inferred success clock.
    return not clocks or now - max(clocks) >= interval


def market_refresh_is_due():
    if MARKET_REFRESH_INTERVAL_SECONDS <= 0:
        return False
    automatic = automatic_refresh_snapshot()
    if automatic.get('active'):
        return False
    sources = csgotrader_sources()
    with closing(steam_ask_db()) as conn:
        conn.execute('BEGIN')
        receipts = {('steam_snapshot' if source == 'steam' else source):
                    provider_fetch_receipt(conn, 'steam_snapshot' if source == 'steam' else source)
                    for source in sources}
    return market_refresh_due_from_receipts(receipts, int(time.time()),
                                            MARKET_REFRESH_INTERVAL_SECONDS, automatic)


def background_market_refresh_loop():
    # The UI reads from SQLite; this task only appends provider observations.
    while True:
        try:
            time.sleep(60)
            if market_refresh_is_due():
                run_automatic_market_refresh()
        except Exception as exc:
            print('  Market background refresh failed:', exc)
        time.sleep(max(300, MARKET_REFRESH_INTERVAL_SECONDS))


def start_background_tasks():
    if not AUTO_MARKET_REFRESH:
        return
    thread = threading.Thread(target=background_market_refresh_loop, name='market-refresh', daemon=True)
    thread.start()


if __name__ == '__main__':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

    # Utility: generate a password hash for CS2DASH_AUTH_PASSWORD_HASH.
    if len(sys.argv) > 1 and sys.argv[1] in ('--hash-password', '--hash'):
        import getpass
        pw1 = getpass.getpass('New password: ')
        pw2 = getpass.getpass('Confirm password: ')
        if pw1 != pw2 or not pw1:
            print('Passwords did not match (or were empty).')
            sys.exit(1)
        print()
        print('Add this to your EnvironmentFile / shell env:')
        print(f"CS2DASH_AUTH_PASSWORD_HASH='{hash_password(pw1)}'")
        sys.exit(0)

    AUTH_HASH = resolve_auth_hash()
    if AUTH_ENABLED and not AUTH_HASH:
        print()
        print('  cs2dash: authentication is enabled but no password is configured.')
        print('  Set CS2DASH_AUTH_PASSWORD_HASH (recommended) or CS2DASH_AUTH_PASSWORD.')
        print('  Generate a hash with:  python server.py --hash-password')
        print('  For a trusted local network only, disable auth with CS2DASH_AUTH_DISABLE=1.')
        print()
        sys.exit(1)

    init_db()
    # Version-one research keeps its original handlers and cached-history data.
    # Current cash prices use the same portable entry point and a separate ledger.
    import restoration_runtime
    restoration_runtime.install(sys.modules[__name__])
    start_idle_wal_anchor()
    try:
        BULK_STAGING_RUNTIME.restore()
    except (sqlite3.Error,bulk_derived_staging.Deferred,ValueError,OSError) as error:
        print('  Saved derived continuation unavailable:',error)
    purge_expired_sessions()
    # Reconcile old provider leases before any resumed/automatic Steam work.
    try:
        steam_ask_reconcile_startup()
    except (sqlite3.Error, steam_held_asks.AskSchemaUnavailable):
        pass  # Every network entry retries this guard and remains fail-closed.
    # Never revive a retired large research job merely by starting the restored
    # server. Explicit dashboard tasks remain available with shared admission.
    resumed_data_task = False
    if os.environ.get('CS2DASH_RESUME_RESEARCH_TASKS', '0').strip().lower() in ('1', 'true', 'yes'):
        resumed_data_task = resume_persisted_data_task()
    if os.environ.get('CS2DASH_RESEARCH_AUTO_REFRESH', '0').strip().lower() in ('1', 'true', 'yes'):
        start_background_tasks()
    ip = get_lan_ip()

    print()
    print(f'  {APP_NAME} v{APP_VERSION}')
    print('  ----------------------------------------')
    print(f'  Bind:    http://{BIND_HOST}:{PORT}')
    if BIND_HOST in ('0.0.0.0', '::') and ip:
        print(f'  Network: http://{ip}:{PORT}')
    print()
    print(f'  Data:    {DATA_DIR}')
    print(f'  Auth:    {"enabled (user: " + AUTH_USERNAME + ")" if AUTH_ENABLED else "DISABLED"}')
    if resumed_data_task:
        print('  Data:    resumed persisted data update task')
    print()
    print('  Note: Write and delete operations require sign-in.')
    print('  Press Ctrl+C to stop.')
    print()

    # Resume only a previously explicit durable job; absent schema does no work.
    try:
        with closing(steam_ask_db()) as ask_conn:
            steam_held_asks.require_schema(ask_conn)
        if not getattr(sys.modules[__name__], 'PRICE_PIPELINE', None):
            STEAM_HELD_ASK_WORKER.kick()
    except (sqlite3.Error, steam_held_asks.AskSchemaUnavailable):
        pass
    server = http.server.ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    try:
        CASH_PRICE_CADENCE.start()
        stop_file = os.environ.get('CS2DASH_STOP_FILE')
        if stop_file:
            marker = Path(stop_file).resolve()
            server.timeout = 1
            while not marker.exists():
                server.handle_request()
        else:
            server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Stopped.')
    finally:
        if not stop_data_tasks():
            print('  Explicit data task is closing its bounded source unit; saved task state is retained.')
        server.server_close()
        if not CASH_PRICE_CADENCE.stop():
            print('  Cash cadence stopping at its closed-unit boundary; no new cycles admitted.')
        BULK_STAGING_RUNTIME.stop()
        STEAM_HELD_ASK_WORKER.stop()
        if CASH_PRICE_HOLDINGS_CACHE is not None:
            CASH_PRICE_HOLDINGS_CACHE.close()
        close_idle_wal_anchor()




