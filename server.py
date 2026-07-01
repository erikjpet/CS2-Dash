#!/usr/bin/env python3
"""
cs2dash v1.0

Local-first CS2 portfolio and market intelligence server. The app serves a
single-page dashboard, stores portfolio imports, caches provider prices and
Steam history in SQLite, and exposes REST endpoints for market search,
valuation, taxonomy analysis, and group history.
"""

import http.server
import html
import json
import os
import re
import sys
import socket
import sqlite3
import time
import datetime
import gzip
import hashlib
import hmac
import secrets
import http.cookiejar
import math
import threading
import urllib.parse
import urllib.request
from collections import deque

APP_NAME = 'cs2dash'
APP_VERSION = '1.0'

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
OBSERVATION_BUCKET_SECONDS = 15 * 60

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

os.makedirs(DATA_DIR, exist_ok=True)

LOCAL_ADDRS = {'127.0.0.1', '::1', '::ffff:127.0.0.1'}
SQLITE_BUSY_TIMEOUT_MS = 30000
DATA_TASK_PERSIST_BUSY_TIMEOUT_MS = 250
SQLITE_WRITE_LOCK = threading.RLock()
STEAM_REQUEST_LOCK = threading.Lock()
DATA_TASK_LOCK = threading.RLock()
DATA_TASKS = {}
DATA_TASK_EVENTS = deque(maxlen=240)
DATA_TASK_PERSIST_BACKLOG = deque(maxlen=600)
DATA_TASK_PERSIST_NOTICE_AT = 0.0
ACTIVE_DATA_TASK_ID = None
DATA_TASK_RESUME_ENABLED = os.environ.get('DATA_TASK_RESUME_ENABLED', '1').strip().lower() not in ('0', 'false', 'no')
DATA_TASK_RESUME_MAX_AGE_SECONDS = int(os.environ.get('DATA_TASK_RESUME_MAX_AGE_SECONDS', str(24 * 60 * 60)))
STEAM_LAST_REQUEST_AT = 0.0
STEAM_MIN_INTERVAL = float(os.environ.get('STEAM_MIN_INTERVAL', '1.25'))
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
# Reject oversized request bodies before reading them into memory.
MAX_BODY_BYTES = int(os.environ.get('CS2DASH_MAX_BODY_BYTES', str(64 * 1024 * 1024)) or (64 * 1024 * 1024))
# Resolved at startup by resolve_auth_hash().
AUTH_HASH = ''
PBKDF2_ITERATIONS = 200000
PUBLIC_GET_PATHS = {'/login', '/favicon.ico', '/api/health'}
PUBLIC_POST_PATHS = {'/api/login'}


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


def get_session_user(token):
    if not token:
        return None
    now = int(time.time())
    username = None
    expired = False
    with db() as conn:
        row = conn.execute(
            'SELECT username,expires_at FROM auth_sessions WHERE token=?', (token,)
        ).fetchone()
        if not row:
            return None
        username = row['username']
        expired = row['expires_at'] < now
    if expired:
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
    persist_data_task_update(task_copy, event)
    print(f"  data-task[{task_id}] {stage}: {message}")
    return event


def data_task_snapshot():
    with DATA_TASK_LOCK:
        should_flush = bool(DATA_TASK_PERSIST_BACKLOG)
    if should_flush:
        persist_data_task_update()
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
        return bool(active and active.get('status') in ('queued', 'running'))


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
        if status in ('queued', 'running'):
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
    thread.start()
    return True


def db(timeout_ms=None):
    timeout_ms = SQLITE_BUSY_TIMEOUT_MS if timeout_ms is None else timeout_ms
    conn = sqlite3.connect(DB_FILE, timeout=timeout_ms / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    return conn


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

        if conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 0:
            for idx, snap in enumerate(read_json(SNAPS_FILE, [])):
                put_snapshot(conn, snap, idx)

        if conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0:
            settings = read_json(SETTINGS_FILE, {})
            conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('shared',?)",
                (json.dumps(settings),),
            )
        if conn.execute("SELECT COUNT(*) FROM snapshot_items").fetchone()[0] == 0:
            rebuild_item_index(conn)
        if conn.execute("SELECT COUNT(*) FROM portfolios").fetchone()[0] == 0:
            seed_portfolios(conn)
        restore_data_tasks_from_db(conn)
        if conn.execute("SELECT COUNT(*) FROM price_observations").fetchone()[0] == 0:
            backfill_price_observations(conn)
        if conn.execute("SELECT COUNT(*) FROM market_items").fetchone()[0] == 0:
            sync_market_items_from_catalog(conn)
        if conn.execute("SELECT COUNT(*) FROM market_item_prices").fetchone()[0] == 0:
            backfill_market_item_prices(conn)
        cleanup_market_universe(conn)


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
    throttle_steam_request(url)
    req = urllib.request.Request(url, headers={
        'Accept-Encoding': 'gzip',
        'User-Agent': f'{APP_NAME}/{APP_VERSION}',
    })
    return fetch_json_request(req, timeout=20)


def fetch_json_request(req, timeout=60):
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        if resp.headers.get('Content-Encoding', '').lower() == 'gzip':
            body = gzip.decompress(body)
        return json.loads(body.decode('utf-8', 'replace'))


def parse_iso_ts(value):
    if not value:
        return int(time.time())
    try:
        return int(datetime.datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp())
    except (TypeError, ValueError):
        return int(time.time())


def throttle_steam_request(url):
    global STEAM_LAST_REQUEST_AT
    if 'steamcommunity.com' not in str(url):
        return
    with STEAM_REQUEST_LOCK:
        wait = STEAM_MIN_INTERVAL - (time.time() - STEAM_LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)
        STEAM_LAST_REQUEST_AT = time.time()


def catalog_stale(conn):
    row = conn.execute("SELECT COUNT(*) n, MIN(updated_at) oldest FROM item_catalog").fetchone()
    if row['n'] < 1000:
        return True
    return int(time.time()) - (row['oldest'] or 0) > CATALOG_TTL


def normalize_catalog_item(kind, obj):
    name = obj.get('market_hash_name') or obj.get('name')
    if not name:
        return None
    rarity = first_name(obj, ('rarity',)) or obj.get('rarity')
    collection = first_name(obj, ('collections',), ('crates',), ('collection',))
    return {
        'name': name,
        'kind': kind,
        'item_type': first_name(obj, ('category',), ('type',)) or obj.get('type') or kind,
        'weapon': first_name(obj, ('weapon',)),
        'collection': collection,
        'rarity': rarity,
        'rarity_color': first_name(obj, ('rarity', 'color')) or (obj.get('rarity') or {}).get('color') if isinstance(obj.get('rarity'), dict) else None,
        'image': obj.get('image'),
        'market_hash_name': obj.get('market_hash_name') or name,
        'raw': json.dumps(obj, separators=(',', ':')),
        'updated_at': int(time.time()),
    }


def catalog_row_is_market_listing(row):
    """Return whether a catalog identity is a direct market listing.

    CSGO-API also publishes collection definitions and grouped skin families.
    Those are useful metadata, but most are not priced Steam listings and should
    not inflate market coverage gaps.
    """
    kind = (row.get('kind') if isinstance(row, dict) else row['kind']) or ''
    if kind == 'collections':
        return False
    if kind == 'skin_families':
        return False
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
    return len(updates)


def refresh_catalog(force=False):
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
            COLLECTION_CASE_ALIAS_CACHE = None


def catalog_payload():
    refresh_catalog(False)
    with db() as conn:
        rows = conn.execute(
            """SELECT name, kind, item_type AS type, weapon, collection, rarity,
                      rarity_color AS rarityColor, image, market_hash_name AS marketHashName
               FROM item_catalog"""
        ).fetchall()
    return {'items': [dict(r) for r in rows], 'updatedAt': int(time.time())}


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
    for r in rows:
        if not catalog_row_is_market_listing(r):
            continue
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
    return len(payload)


def infer_market_meta_from_name(name):
    name = (name or '').strip()
    low = name.lower()
    if not name:
        return {}
    meta = {'name': name, 'market_hash_name': name}
    event = event_from_name(name)
    if name.startswith('★') or re.search(r'\b(knife|gloves|wraps)\b', name, re.I):
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
    return dict(row) if row else None


def ensure_market_item(conn, name, meta=None):
    if not name:
        return None
    meta = meta or catalog_meta_for_name(conn, name) or infer_market_meta_from_name(name) or {}
    canonical = meta.get('name') or name
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
        ensure_market_item(conn, name, {'name': name, 'market_hash_name': name})
    return canonical


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


def upsert_market_item_price(conn, quote, provider=None, source=None, recalculate=True):
    record = market_price_record_from_quote(quote, provider=provider, source=source)
    if not record:
        return False
    name = ensure_market_item(conn, record['name'])
    if name != record['name']:
        record['name'] = name
    existing = conn.execute(
        "SELECT updated_at FROM market_item_prices WHERE name=? AND provider=?",
        (record['name'], record['provider']),
    ).fetchone()
    if existing and int(existing['updated_at'] or 0) > int(record['updated_at'] or 0):
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
    return True


def remove_missing_provider_current_prices(conn, provider, valid_names):
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
               WHERE provider=? AND name NOT IN (SELECT name FROM provider_current_keep)""",
            (provider,),
        )
    ]
    if not stale_names:
        return 0
    conn.execute(
        """DELETE FROM market_item_prices
           WHERE provider=? AND name NOT IN (SELECT name FROM provider_current_keep)""",
        (provider,),
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


def market_price_row_to_quote(row):
    if not row:
        return {}
    row = dict(row)
    return {
        'name': row.get('name'),
        'provider': row.get('provider'),
        'price': row.get('price'),
        'median': row.get('median') or row.get('price'),
        'volume': row.get('volume'),
        'url': row.get('url'),
        'updated_at': row.get('updated_at'),
        'source': row.get('source') or row.get('provider'),
        'priceBasis': row.get('basis'),
        'priceKind': row.get('kind'),
        'confidencePct': row.get('confidence_pct'),
        'lowestSellOrder': row.get('lowest_sell_order'),
        'highestBuyOrder': row.get('highest_buy_order'),
        'solidPrice': row.get('solid_price') or row.get('price'),
        'orderBookSpreadPct': row.get('spread_pct'),
        'priceStabilityPct': row.get('stability_pct'),
    }


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
    conn.execute(
        """INSERT OR REPLACE INTO price_observations
           (name, provider, observed_at, price, median, volume, source, raw, updated_at)
           VALUES (:name, :provider, :observed_at, :price, :median, :volume, :source, :raw, :updated_at)""",
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


def steam_url_opener():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


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
    marker = 'window.SSR.renderContext=JSON.parse('
    pos = page.find(marker)
    if pos < 0:
        return []
    raw = page[pos + len(marker):]
    try:
        render_json, _idx = json.JSONDecoder().raw_decode(raw)
        render_ctx = json.loads(render_json)
        query_data = json.loads(render_ctx.get('queryData') or '{}')
    except Exception:
        return []
    fallback_prices = []
    for query in query_data.get('queries') or []:
        key = query.get('queryKey') or []
        if (
            len(key) >= 4
            and key[0] == 'market'
            and key[1] == 'pricehistory'
            and str(key[2]) == str(APPID)
        ):
            data = (query.get('state') or {}).get('data') or {}
            points = parse_steam_price_points(data.get('prices') or [])
            if str(key[3]) == str(market_hash_name):
                return points
            if points and not fallback_prices:
                fallback_prices = points
    return fallback_prices


def fetch_steam_history(market_hash_name, display_name=None):
    """
    Pull Steam Community Market chart data for one CS2 item.

    The JSON endpoint can return HTTP 400 without a valid Steam session cookie,
    so this function tries both:
      1) /market/pricehistory/ JSON endpoint
      2) the public market listing page and its embedded var line1 chart data

    Set STEAM_COOKIE for the most reliable endpoint behavior:
      export STEAM_COOKIE='steamLoginSecure=...; sessionid=...'
    """
    market_hash_name = (market_hash_name or display_name or '').strip()
    display_name = (display_name or market_hash_name).strip()
    if not market_hash_name:
        return {
            'name': display_name,
            'marketHashName': market_hash_name,
            'provider': 'steam',
            'success': False,
            'error': 'Missing market_hash_name',
            'url': '',
            'points': [],
            'updated_at': int(time.time()),
        }

    listing_url = steam_market_url(market_hash_name)
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
        'Accept': 'application/json,text/html,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': listing_url,
    }
    cookie = steam_cookie_header()
    if cookie:
        headers['Cookie'] = cookie

    errors = []
    opener = steam_url_opener()

    def fetch_listing_page():
        throttle_steam_request(listing_url)
        req = urllib.request.Request(listing_url, headers=headers)
        with opener.open(req, timeout=STEAM_HISTORY_FETCH_TIMEOUT) as resp:
            return resp.read().decode('utf-8', 'replace')

    def parse_listing_page(page):
        m = re.search(r'var\s+line1\s*=\s*(\[.*\]);', page, re.S)
        if not m:
            m = re.search(r'line1\s*=\s*(\[.*\]);', page, re.S)
        if m:
            raw_rows = json.loads(html.unescape(m.group(1)))
            points = parse_steam_price_points(raw_rows)
            if points:
                return points, 'listing_page'
        points = parse_steam_ssr_price_points(page, market_hash_name)
        if points:
            return points, 'listing_ssr'
        return [], None

    def listing_result():
        page = fetch_listing_page()
        points, source = parse_listing_page(page)
        if points:
            return {
                'name': display_name,
                'marketHashName': market_hash_name,
                'provider': 'steam',
                'success': True,
                'source': source,
                'url': listing_url,
                'points': points,
                'updated_at': int(time.time()),
            }
        errors.append('Steam listing page did not contain chart rows')
        return None

    # Public listing SSR is usually enough and avoids the authenticated JSON
    # endpoint when no Steam session cookie is configured.
    if not cookie:
        try:
            result = listing_result()
            if result:
                return result
        except Exception as exc:
            errors.append('Listing fallback failed: ' + str(exc))

    # Try Steam's JSON endpoint. It is most complete when STEAM_COOKIE is set.
    history_url = 'https://steamcommunity.com/market/pricehistory/?' + urllib.parse.urlencode({
        'appid': APPID,
        'currency': 1,
        'country': 'US',
        'market_hash_name': market_hash_name,
    })
    try:
        throttle_steam_request(history_url)
        req = urllib.request.Request(history_url, headers=headers)
        with opener.open(req, timeout=STEAM_HISTORY_FETCH_TIMEOUT) as resp:
            raw = json.loads(resp.read().decode('utf-8', 'replace'))
        points = parse_steam_price_points(raw.get('prices') or [])
        if points:
            return {
                'name': display_name,
                'marketHashName': market_hash_name,
                'provider': 'steam',
                'success': True,
                'url': listing_url,
                'points': points,
                'raw': raw,
                'updated_at': int(time.time()),
            }
        errors.append(raw.get('message') or raw.get('error') or 'Steam JSON endpoint returned no prices')
    except Exception as exc:
        errors.append(str(exc))

    if cookie:
        try:
            result = listing_result()
            if result:
                return result
        except Exception as exc:
            errors.append('Listing fallback failed: ' + str(exc))

    return {
        'name': display_name,
        'marketHashName': market_hash_name,
        'provider': 'steam',
        'success': False,
        'error': concise_steam_fetch_error(errors),
        'url': listing_url,
        'points': [],
        'updated_at': int(time.time()),
    }


def get_price_history(name, force=False, market_hash_name=None, rebuild_metrics=True):
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
    history_saved = False
    with db() as conn:
        newest = conn.execute(
            "SELECT MAX(updated_at) AS updated_at, COUNT(*) AS n, MAX(point_ts) AS max_ts FROM price_history WHERE name=? AND provider='steam'",
            (name,),
        ).fetchone()
        stale = bool(
            newest
            and newest['n']
            and not newest['max_ts']
        )

        if force or stale:
            hist = fetch_steam_history(market_hash_name or name, display_name=name)
            if hist.get('points'):
                try:
                    with SQLITE_WRITE_LOCK:
                        conn.executemany(
                            """INSERT OR REPLACE INTO price_history
                               (name, provider, point_time, point_ts, raw_time, price, volume, updated_at)
                               VALUES (?, 'steam', ?, ?, ?, ?, ?, ?)""",
                            [(name, p['time'], p.get('ts'), p.get('rawTime'), p.get('price'), p.get('volume'), hist['updated_at']) for p in hist['points']],
                        )
                        conn.commit()
                    save_history_quote(name, market_hash_name or name, hist['points'], hist['updated_at'])
                    history_saved = True
                except sqlite3.OperationalError as exc:
                    if 'locked' not in str(exc).lower():
                        raise
            elif force and (not newest or not newest['n']):
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
        rebuild_metrics_for_names([name, market_hash_name or name])

    if rows:
        with db() as conn:
            has_quote = conn.execute(
                "SELECT 1 FROM market_item_prices WHERE name=? AND provider='steam_history'",
                (name,),
            ).fetchone()
        if not has_quote:
            save_history_quote(name, market_hash_name or name, [dict(r) for r in rows], max((int(r['updated_at']) for r in rows), default=now))
    elif not point_rows:
        if steam_error:
            return {
                'provider': 'steam',
                'currency': 'USD',
                'name': name,
                'url': steam_market_url(market_hash_name or name),
                'marketHashName': market_hash_name or name,
                'success': False,
                'error': steam_error,
                'source': 'steam_history',
                'points': [],
                'updatedAt': now,
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
            'updatedAt': now,
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
    """Materialize provider-consistent movement metrics from local cached rows."""
    window_days = parse_int_param(window_days, 90, allowed=VOLATILITY_WINDOWS)
    clean_names = [(n or '').strip() for n in (names or []) if (n or '').strip()]
    name_filter = ''
    params = list(CURRENT_PRICE_PROVIDERS)
    if clean_names:
        marks = ','.join('?' for _ in clean_names)
        name_filter = f" AND name IN ({marks})"
        params.extend(clean_names)
    provider_marks = ','.join('?' for _ in CURRENT_PRICE_PROVIDERS)
    if clean_names:
        marks = ','.join('?' for _ in clean_names)
        conn.execute(
            f"DELETE FROM item_market_metrics WHERE provider='market' AND window_days=? AND name IN ({marks})",
            [window_days] + clean_names,
        )
    else:
        conn.execute(
            "DELETE FROM item_market_metrics WHERE provider='market' AND window_days=?",
            (window_days,),
        )
    latest_rows = [dict(r) for r in conn.execute(
        f"""SELECT name, provider, price, solid_price, updated_at, source
            FROM market_item_prices
            WHERE provider IN ({provider_marks}) AND price IS NOT NULL{name_filter}
            ORDER BY name, updated_at""",
        params,
    )]
    by_name = {}
    for row in latest_rows:
        bucket = by_name.setdefault(row['name'], {})
        existing = bucket.get(row['provider'])
        if existing is None or int(row.get('updated_at') or 0) >= int(existing.get('updated_at') or 0):
            bucket[row['provider']] = row
    cutoff = history_cutoff_ts(window_days)
    rebuilt = 0
    for name, provider_rows in by_name.items():
        primary = None
        for provider in PRIMARY_PRICE_PROVIDER_ORDER:
            if provider in provider_rows:
                primary = provider_rows[provider]
                break
        if not primary:
            continue
        provider = primary.get('provider')
        steam_params = [name]
        steam_where = ''
        if cutoff:
            steam_where = ' AND COALESCE(point_ts,0) >= ?'
            steam_params.append(max(0, cutoff - OBSERVATION_BUCKET_SECONDS))
        steam_rows = [dict(r) for r in conn.execute(f"""
            SELECT name, point_ts, price, volume, 'steam_history' AS source, 'steam_history' AS provider
            FROM price_history
            WHERE provider='steam' AND name=? AND price IS NOT NULL {steam_where}
            ORDER BY point_ts
        """, steam_params)]
        current_price = _positive_float(primary.get('solid_price')) or _positive_float(primary.get('price'))
        current_ts = int(primary.get('updated_at') or time.time())
        metric_rows = []
        metric_source = 'insufficient'
        if steam_rows:
            metric_rows = steam_rows
            metric_source = 'steam_history'
        else:
            steam_current = provider_rows.get('steam_snapshot') or {}
            steam_current_price = _positive_float(steam_current.get('solid_price')) or _positive_float(steam_current.get('price'))
            steam_current_ts = int(steam_current.get('updated_at') or current_ts)
            anchor_rows = csgotrader_window_anchor_rows(
                conn,
                name,
                window_days,
                current_price=steam_current_price,
                current_ts=steam_current_ts,
            )
            if anchor_rows:
                metric_rows = anchor_rows
                metric_source = 'csgotrader_window_anchor'
            else:
                obs_params = [name, provider]
                obs_where = ''
                if cutoff:
                    obs_where = ' AND COALESCE(observed_at,0) >= ?'
                    obs_params.append(max(0, cutoff - OBSERVATION_BUCKET_SECONDS))
                metric_rows = [dict(r) for r in conn.execute(f"""
                    SELECT name, observed_at AS point_ts, price, volume,
                           COALESCE(source, provider) AS source, provider
                    FROM price_observations
                    WHERE name=? AND provider=? AND price IS NOT NULL {obs_where}
                      AND COALESCE(source, '') NOT LIKE 'csgotrader:steam:last_%'
                    ORDER BY observed_at
                """, obs_params)]
                metric_source = 'provider_observations' if metric_rows else 'insufficient'
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
            rebuilt += 1
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('metric_build_version',?)", (str(METRIC_BUILD_VERSION),))
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('lookup_build_version',?)", (str(LOOKUP_BUILD_VERSION),))
    return rebuilt


def rebuild_metrics_for_names(names, windows=VOLATILITY_DETAIL_WINDOWS):
    """Refresh derived movement rows after raw Steam history changes."""
    clean = [n for n in dict.fromkeys(str(n or '').strip() for n in names or []) if n]
    if not clean:
        return 0
    rebuilt = 0
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            for window in windows or VOLATILITY_DETAIL_WINDOWS:
                rebuilt += rebuild_market_metrics(conn, window, names=clean)
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


def metric_window_is_current(conn, window_days):
    row = conn.execute(
        """SELECT COUNT(*) AS n,
                  SUM(CASE WHEN raw LIKE ? THEN 1 ELSE 0 END) AS current_rows
           FROM item_market_metrics
           WHERE provider='market' AND window_days=?""",
        (f'%"metricBuildVersion":{METRIC_BUILD_VERSION}%', int(window_days or 0)),
    ).fetchone()
    total = int(row['n'] or 0) if row else 0
    current = int(row['current_rows'] or 0) if row else 0
    return total > 0 and total == current


def ensure_metric_window_current(conn, window_days):
    """Serialize metric rebuilds that can be triggered from read-heavy pages."""
    if metric_window_is_current(conn, window_days):
        return False
    with SQLITE_WRITE_LOCK:
        with db() as write_conn:
            if metric_window_is_current(write_conn, window_days):
                return False
            rebuild_market_metrics(write_conn, window_days)
            write_conn.commit()
            return True


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


def save_history_quote(name, market_hash_name, points, updated_at=None):
    quote = history_quote_from_points(name, market_hash_name, points, updated_at)
    if not quote:
        return None
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            for quote_name in dict.fromkeys([name, market_hash_name or name]):
                if not quote_name:
                    continue
                row = {**quote, 'name': quote_name}
                upsert_market_item_price(conn, row, provider='steam_history', source='steam_history')
                save_price_observation(conn, row, source='steam_history', observed_at=updated_at, provider='steam_history')
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


def get_inventory_dataset(scale_days=90, history_days=None, limit=2000, refresh=False, refresh_limit=25, refresh_history=False, history_limit=10, portfolio_id=None, force=False):
    """Return a portfolio with locally cached per-item quote/volatility fields."""
    history_days = parse_int_param(history_days if history_days is not None else scale_days, 90, allowed=VOLATILITY_WINDOWS)
    limit = parse_int_param(limit, 2000, minimum=1, maximum=10000)
    refresh_limit = parse_int_param(refresh_limit, 25, minimum=1, maximum=200)
    history_limit = parse_int_param(history_limit, 10, minimum=1, maximum=50)
    exact_meta, base_meta, catalog_count = catalog_index()
    with db() as conn:
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
        for item in items[:history_limit]:
            get_price_history(item.get('name'), force=force, market_hash_name=item.get('marketHashName'))

    with db() as conn:
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
        return {'attempted': 0, 'refreshed': 0, 'errors': []}
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
    max_attempts = min(len(ranked), max(limit * 4, limit + 12))
    attempted = 0
    refreshed = 0
    skipped_failures = 0
    errors = []
    failure_records = []
    successful_names = []
    report(
        'queued',
        f"Queued up to {limit:,} Steam chart fetches from {len(ranked):,} candidates",
        limit=limit,
        candidateCount=len(ranked),
        skippedUnfetchable=skipped_unfetchable,
    )
    for item in ranked:
        if refreshed >= limit or attempted >= max_attempts:
            break
        name = (item.get('name') or item.get('marketHashName') or '').strip()
        if not name:
            continue
        if name in cached_names and not force:
            continue
        if name in recent_failures and not force and not retry_failures:
            skipped_failures += 1
            continue
        attempted += 1
        report(
            'item',
            f"Steam chart {attempted}/{limit}: {name}",
            item=name,
            marketHashName=item.get('marketHashName') or name,
            attempted=attempted,
            limit=limit,
            refreshed=refreshed,
            skippedRecentFailures=skipped_failures,
        )
        hist = get_price_history(
            name,
            force=True,
            market_hash_name=(item.get('marketHashName') or name),
            rebuild_metrics=False,
        )
        if hist.get('points'):
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
            failure = {
                'name': name,
                'marketHashName': item.get('marketHashName') or name,
                'error': hist.get('error') or 'No Steam chart rows returned',
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
    save_steam_history_failures(failure_records)
    clear_steam_history_failures(successful_names)
    rebuilt_metrics = rebuild_metrics_for_names(successful_names) if successful_names else 0
    return {
        'attempted': attempted,
        'refreshed': refreshed,
        'rebuiltMetrics': rebuilt_metrics,
        'skippedRecentFailures': skipped_failures,
        'skippedUnfetchable': skipped_unfetchable,
        'errors': errors,
    }


def latest_portfolio_history_candidates(limit=120, portfolio_id=None):
    """Return portfolio items most likely to drive visible value curves."""
    limit = parse_int_param(limit, 120, minimum=0, maximum=1000)
    if limit <= 0:
        return []
    exact_meta, base_meta, _catalog_count = catalog_index()
    with db() as conn:
        portfolio = latest_portfolio(conn, portfolio_id)
        if not portfolio:
            return []
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
    with db() as conn:
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


def warm_steam_history_after_market_sync(limit=None, portfolio_id=None, force=False, retry_failures=False):
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

    Items with Steam chart rows move over time from cleaned raw Steam points.
    Items without chart rows contribute their current provider valuation as a
    marked backfill so cold-start group and portfolio curves stay near the
    current basket value without pretending missing Steam history moved.
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
        fallback_price = (
            _positive_float(item.get('latestPrice'))
            or _positive_float(item.get('marketUnitPrice'))
            or _positive_float(item.get('solidPrice'))
        )
        current_ts = int(item.get('latestAt') or item.get('updatedAt') or 0)
        if len(series) >= 2:
            if fallback_price is not None and current_ts and current_ts > int(series[-1].get('ts') or 0):
                series = list(series)
                series.append({
                    'ts': current_ts,
                    'price': fallback_price,
                    'volume': 0,
                    'source': 'current_provider_price',
                })
                latest_history_ts = max(latest_history_ts, current_ts)
                current_anchor_count += 1
            normalized_series[key] = series
        else:
            if fallback_price is not None:
                backfill_prices[key] = fallback_price

    items_with_history = len(normalized_series)
    if not normalized_series:
        return {
            'ok': True,
            'historyDays': history_days,
            'generatedAt': int(time.time()),
            'points': [],
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
                fallback_price = backfill_prices.get(key)
                if fallback_price is None:
                    continue
                total += fallback_price * item['chartQty']
                contributing += 1
                backfilled_items += 1
                continue
            price = None
            if ts < series[0]['ts']:
                price = series[0]['price']
                not_started_items += 1
                carried_forward_items += 1
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
        for item in items:
            hist = get_price_history(item.get('name'), force=force, market_hash_name=item.get('marketHashName'))
            if hist.get('points'):
                history_refreshed += 1

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
    refresh_catalog(False)
    with db() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT name, kind, item_type AS type, weapon, collection, rarity,
                      rarity_color AS rarityColor, image, market_hash_name AS marketHashName
               FROM item_catalog"""
        )]
    exact = {r['name']: r for r in rows}
    base = {}
    for r in rows:
        b = base_item_name(r['name'])
        if b not in base or r['name'] == b:
            base[b] = r
    return exact, base, len(rows)


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
        'marketHashName': meta.get('marketHashName') or row['name'],
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


def market_group_tags(item):
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
        case_alias = collection_case_alias(item.get('collection'))
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
    return {
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


def store_market_quote(conn, quote, source, recalculate=True):
    if not quote:
        return None
    provider_key = provider_key_from_source(source=source, provider=quote.get('provider'), quote=quote)
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
            return 'unchanged'
    save_price_observation(
        conn,
        quote,
        source=source,
        observed_at=quote.get('updated_at'),
        provider=provider_key,
    )
    upsert_market_item_price(conn, quote, provider=provider_key, source=source, recalculate=recalculate)
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
    errors = []
    unchanged = 0
    rebuilt_lookups = 0
    rebuilt_metrics = 0
    removed_stale = 0
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
            raw = fetch_json_request(req, timeout=120)
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
        source_payloads.append((source, raw))
        if progress:
            progress('provider_received', f"Received {len(raw):,} {CSGOTRADER_SOURCE_LABELS.get(source, source)} rows", source=source, rows=len(raw))
    timings['fetchProviderSnapshotsSeconds'] = round(time.perf_counter() - stage_start, 2)
    stage_start = time.perf_counter()
    with SQLITE_WRITE_LOCK:
        with db() as conn:
            for source, raw in source_payloads:
                provider_key = provider_key_from_source(source='csgotrader:' + source, provider=source)
                valid_names = set()
                before_source_count = count
                for name, raw_price in raw.items():
                    chosen = choose_csgotrader_price(raw_price, source)
                    if not chosen:
                        continue
                    quote = quote_from_market_price(
                        name,
                        chosen['price'],
                        upstream='csgotrader',
                        source=source,
                        updated_at=observed_at,
                        raw={**(chosen.get('raw') or {}), 'chosen_basis': chosen.get('basis')},
                    )
                    canonical_name = ensure_market_item(conn, quote.get('name')) or quote.get('name')
                    if canonical_name != quote.get('name'):
                        quote = {**quote, 'name': canonical_name}
                    stored_state = store_market_quote(conn, quote, 'csgotrader:' + source, recalculate=False)
                    valid_names.add(canonical_name)
                    if stored_state:
                        count += 1
                        if stored_state == 'inserted':
                            inserted += 1
                            changed_primary_names.add(canonical_name)
                            store_csgotrader_history_points(conn, name, raw_price, source, observed_at)
                        elif stored_state == 'updated':
                            updated += 1
                            changed_primary_names.add(canonical_name)
                            store_csgotrader_history_points(conn, name, raw_price, source, observed_at)
                        elif stored_state == 'unchanged':
                            unchanged += 1
                        by_source[source] = by_source.get(source, 0) + 1
                removed_stale += remove_missing_provider_current_prices(conn, provider_key, valid_names)
                if progress:
                    progress(
                        'provider_stored',
                        f"Stored {count - before_source_count:,} {CSGOTRADER_SOURCE_LABELS.get(source, source)} current-price rows",
                        source=source,
                        stored=count - before_source_count,
                        validNames=len(valid_names),
                    )
            if changed_primary_names:
                for changed_name in sorted(changed_primary_names):
                    recalculate_market_item_primary(conn, changed_name)
            timings['storeProviderRowsSeconds'] = round(time.perf_counter() - stage_start, 2)
            stage_start = time.perf_counter()
            material_changes = inserted + updated + removed_stale
            if material_changes:
                cleanup_market_universe(conn)
                conn.execute("DELETE FROM market_item_lookup")
                conn.execute("DELETE FROM item_market_metrics WHERE provider='market'")
                for window in VOLATILITY_DETAIL_WINDOWS:
                    rebuilt_metrics += rebuild_market_metrics(conn, window)
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
    ok = count > 0
    steam_history_warm = {'attempted': 0, 'refreshed': 0, 'errors': [], 'enabled': False}
    stage_start = time.perf_counter()
    if ok and warm_history:
        steam_history_warm = warm_steam_history_after_market_sync(
            limit=history_limit,
            portfolio_id=portfolio_id,
            force=False,
            retry_failures=retry_history_failures,
        )
    timings['warmSteamChartsSeconds'] = round(time.perf_counter() - stage_start, 2)
    stage_start = time.perf_counter()
    status = market_universe_status()
    timings['statusSeconds'] = round(time.perf_counter() - stage_start, 2)
    timings['totalSeconds'] = round(time.perf_counter() - started, 2)
    return {
        'ok': ok,
        'provider': 'csgotrader',
        'preferredSource': 'all',
        'sourcesPulled': sources,
        'itemsSeen': seen,
        'itemsStored': count,
        'newObservationRows': inserted,
        'updatedObservationRows': updated,
        'unchangedObservationRows': unchanged,
        'removedStaleCurrentRows': removed_stale,
        'rebuiltLookups': rebuilt_lookups,
        'rebuiltMetrics': rebuilt_metrics,
        'bySource': by_source,
        'steamHistoryWarm': steam_history_warm,
        'errors': errors,
        'timings': timings,
        'status': status,
        'error': None if ok else 'No prices were stored from the free CSGO Trader snapshots.',
    }


def market_universe_status():
    with db() as conn:
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
        obs_count = conn.execute("SELECT COUNT(*) FROM price_observations").fetchone()[0]
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
            WHERE provider IN ({})
            GROUP BY provider
            ORDER BY items DESC, rows DESC
            LIMIT 12
        """.format(provider_marks), CURRENT_PRICE_PROVIDERS)]
        source_map = {row['source']: row for row in source_rows}
        provider_coverage = []
        for provider in CURRENT_PRICE_PROVIDERS:
            row = source_map.get(provider, {})
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
            })
        price_history_rows = conn.execute(
            "SELECT COUNT(*) FROM price_history WHERE provider='steam' AND price IS NOT NULL"
        ).fetchone()[0]
        price_history_items = conn.execute(
            "SELECT COUNT(DISTINCT name) FROM price_history WHERE provider='steam' AND price IS NOT NULL"
        ).fetchone()[0]
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
        if metric_build_current:
            for row in metric_rows:
                row['currentRows'] = int(row.get('rows') or 0)
        else:
            for row in metric_rows:
                row['currentRows'] = 0
        current_metric_rows = sum(int(r.get('currentRows') or 0) for r in metric_rows)
        current_metric_items = max((int(r.get('items') or 0) for r in metric_rows), default=0)
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
            'steam_history',
            'Items without raw Steam chart history',
            max(0, market_universe_count - price_history_items),
            market_universe_count,
            'Pull Market Data warms priority portfolio and market chart histories. Remaining charts are filled on demand as groups/items are opened.',
        )
        add_gap(
            'metric_windows',
            'Items without current 7d/30d/90d metric rows',
            max(0, (observed_items * len(VOLATILITY_DETAIL_WINDOWS)) - current_metric_rows),
            observed_items * len(VOLATILITY_DETAIL_WINDOWS),
            'Metric rows rebuild automatically after market data is pulled or when a page requests a window.',
        )
    return {
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


def shared_pricing_status():
    with db() as conn:
        lanes = [dict(r) for r in conn.execute(
            """SELECT lane, COUNT(*) AS items, MAX(updated_at) AS latestAt
               FROM shared_item_prices
               GROUP BY lane
               ORDER BY lane"""
        )]
        return {
            'ok': True,
            'db': DB_FILE,
            'lanes': lanes,
            'items': conn.execute("SELECT COUNT(DISTINCT name) FROM shared_item_prices").fetchone()[0],
            'currentRows': conn.execute("SELECT COUNT(*) FROM shared_item_prices").fetchone()[0],
            'observationRows': conn.execute("SELECT COUNT(*) FROM shared_price_observations").fetchone()[0],
            'historyRows': conn.execute("SELECT COUNT(*) FROM shared_price_history").fetchone()[0],
            'sourceQuoteRows': conn.execute("SELECT COUNT(*) FROM shared_source_quotes").fetchone()[0],
            'latestAt': max((int(r.get('latestAt') or 0) for r in lanes), default=0),
        }


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


def run_data_accumulation_task(task_id, options):
    global ACTIVE_DATA_TASK_ID
    mode = (options.get('mode') or 'accumulate_missing').strip().lower()
    pull_mode = (options.get('pullMode') or 'gentle').strip().lower()
    portfolio_id = options.get('portfolio') or None
    force = bool(options.get('force'))
    retry_failures = bool(options.get('retryFailures', True))
    history_limit = parse_int_param(options.get('historyLimit'), 40 if pull_mode == 'gentle' else 120, minimum=0, maximum=500)
    cycles = parse_int_param(options.get('cycles'), 1 if mode != 'accumulate_missing' else 2, minimum=1, maximum=12)
    sleep_seconds = parse_int_param(options.get('sleepSeconds'), 3 if pull_mode == 'gentle' else 1, minimum=0, maximum=120)

    def progress(stage, message, **payload):
        data_task_event(task_id, stage, message, **payload)

    with DATA_TASK_LOCK:
        task = DATA_TASKS[task_id]
        task['status'] = 'running'
        task['startedAt'] = int(time.time())
        task['updatedAt'] = task['startedAt']
    try:
        progress('started', f"Started {mode.replace('_', ' ')} using {pull_mode} pull mode", mode=mode, pullMode=pull_mode, historyLimit=history_limit, cycles=cycles)
        result = None
        for cycle in range(1, cycles + 1):
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
                progress(
                    'provider_complete',
                    f"Provider sync checked {int(result.get('itemsStored') or 0):,} rows; new {int(result.get('newObservationRows') or 0):,}, updated {int(result.get('updatedObservationRows') or 0):,}",
                    itemsStored=result.get('itemsStored'),
                    newRows=result.get('newObservationRows'),
                    updatedRows=result.get('updatedObservationRows'),
                    unchangedRows=result.get('unchangedObservationRows'),
                    bySource=result.get('bySource') or {},
                )
            if mode in ('steam_history', 'accumulate_missing'):
                if portfolio_id:
                    portfolio_limit = max(1, history_limit // 2)
                    progress('portfolio_history', f"Warming up to {portfolio_limit:,} missing portfolio Steam charts", limit=portfolio_limit)
                    portfolio_result = refresh_steam_history_for_chart_items(
                        latest_portfolio_history_candidates(max(portfolio_limit * 8, 80), portfolio_id=portfolio_id),
                        limit=portfolio_limit,
                        force=force,
                        retry_failures=retry_failures,
                        progress=progress,
                        stage_prefix='portfolio_history',
                    )
                    progress(
                        'portfolio_history_complete',
                        f"Portfolio Steam charts cached {int(portfolio_result.get('refreshed') or 0):,}/{int(portfolio_result.get('attempted') or 0):,}",
                        **portfolio_result,
                    )
                    market_limit = max(0, history_limit - portfolio_limit)
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
                    progress(
                        'market_history_complete',
                        f"Market Steam charts cached {int(market_result.get('refreshed') or 0):,}/{int(market_result.get('attempted') or 0):,}",
                        **market_result,
                    )
            status = market_universe_status()
            progress(
                'coverage',
                f"Coverage now {status.get('coveragePct', 0)}% priced, {int((status.get('stores') or {}).get('steamHistoryItems') or 0):,} items with raw Steam charts",
                coveragePct=status.get('coveragePct'),
                observedItems=status.get('observedItems'),
                catalogItems=status.get('catalogItems'),
                steamHistoryItems=(status.get('stores') or {}).get('steamHistoryItems'),
                gaps=status.get('gaps') or [],
            )
            if cycle < cycles and sleep_seconds:
                time.sleep(sleep_seconds)
        with DATA_TASK_LOCK:
            task = DATA_TASKS[task_id]
            task['status'] = 'complete'
            task['finishedAt'] = int(time.time())
            task['updatedAt'] = task['finishedAt']
            task['result'] = _data_task_result_summary(result or {'ok': True})
        progress('complete', 'Data task complete')
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
            if ACTIVE_DATA_TASK_ID == task_id:
                ACTIVE_DATA_TASK_ID = None


def start_data_task(options):
    global ACTIVE_DATA_TASK_ID
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
    thread.start()
    snapshot = data_task_snapshot()
    snapshot.update({'ok': True, 'task': compact_data_task(task)})
    return snapshot


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
    rows = [dict(r) for r in conn.execute(
        """SELECT DISTINCT collection, item_type AS type, weapon, kind, rarity
           FROM market_item_lookup
           WHERE window_days=?""",
        (window_days,),
    )]
    collections = sorted({r.get('collection') for r in rows if r.get('collection')})
    types = sorted({
        r.get('type') or r.get('weapon') or r.get('kind')
        for r in rows
        if r.get('type') or r.get('weapon') or r.get('kind')
    })
    grades = sorted({r.get('rarity') for r in rows if r.get('rarity')})
    return {'collections': collections, 'types': types, 'grades': grades}


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
    rows = [dict(r) for r in conn.execute(
        """SELECT window_days, COUNT(*) AS n
           FROM item_market_metrics
           WHERE provider='market'
           GROUP BY window_days"""
    )]
    rows = [r for r in rows if int(r.get('n') or 0) > 0]
    if not rows:
        return requested, False, 0
    if requested == 0:
        chosen = max(rows, key=lambda r: int(r['window_days'] or 0))
    else:
        chosen = min(rows, key=lambda r: abs(int(r['window_days'] or 0) - requested))
    return int(chosen['window_days'] or 0), int(chosen['window_days'] or 0) == requested, int(chosen['n'] or 0)


def parse_float_param(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def market_item_lookup_is_current(conn, window_days):
    version = conn.execute("SELECT value FROM settings WHERE key='lookup_build_version'").fetchone()
    if not version or str(version['value']) != str(LOOKUP_BUILD_VERSION):
        return False
    metric_version = conn.execute("SELECT value FROM settings WHERE key='metric_build_version'").fetchone()
    if not metric_version or str(metric_version['value']) != str(METRIC_BUILD_VERSION):
        return False
    cache = conn.execute(
        "SELECT COUNT(*) AS n, MAX(metric_updated_at) AS updated_at FROM market_item_lookup WHERE window_days=?",
        (window_days,),
    ).fetchone()
    metrics = conn.execute(
        """SELECT COUNT(*) AS n, MAX(updated_at) AS updated_at
           FROM item_market_metrics
           WHERE provider='market' AND window_days=?""",
        (window_days,),
    ).fetchone()
    provider_marks = ','.join('?' for _ in CURRENT_PRICE_PROVIDERS)
    prices = conn.execute(
        f"SELECT MAX(updated_at) AS updated_at FROM market_item_prices WHERE provider IN ({provider_marks})",
        CURRENT_PRICE_PROVIDERS,
    ).fetchone()
    if not metrics or int(metrics['n'] or 0) == 0:
        return False
    price_updated_at = int(prices['updated_at'] or 0) if prices else 0
    source_updated_at = max(int(metrics['updated_at'] or 0), price_updated_at)
    return (
        int(cache['n'] or 0) == int(metrics['n'] or 0)
        and int(cache['updated_at'] or 0) >= source_updated_at
    )


def rebuild_market_item_lookup(conn, window_days):
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
    return len(payload)


def ensure_market_item_lookup(conn, window_days):
    if market_item_lookup_is_current(conn, window_days):
        return
    with SQLITE_WRITE_LOCK:
        with db() as write_conn:
            metrics = write_conn.execute(
                "SELECT COUNT(*) AS n FROM item_market_metrics WHERE provider='market' AND window_days=?",
                (window_days,),
            ).fetchone()
            rebuilt_metrics = False
            if not metrics or int(metrics['n'] or 0) == 0 or not metric_window_is_current(write_conn, window_days):
                rebuild_market_metrics(write_conn, window_days)
                rebuilt_metrics = True
            if market_item_lookup_is_current(write_conn, window_days):
                return
            if not rebuilt_metrics:
                rebuild_market_metrics(write_conn, window_days)
            rebuild_market_item_lookup(write_conn, window_days)
            write_conn.commit()


def market_item_lookup_status(conn, window_days):
    row = conn.execute(
        """SELECT COUNT(*) AS n,
                  SUM(CASE WHEN latest_price IS NOT NULL THEN 1 ELSE 0 END) AS priced,
                  SUM(CASE WHEN prior_price IS NOT NULL THEN 1 ELSE 0 END) AS prior_priced,
                  MAX(latest_at) AS latest_at
           FROM market_item_lookup
           WHERE window_days=?""",
        (window_days,),
    ).fetchone()
    sources = [dict(r) for r in conn.execute(
        """SELECT source, COUNT(*) AS items
           FROM market_item_lookup
           WHERE window_days=? AND source IS NOT NULL
           GROUP BY source
           ORDER BY items DESC
           LIMIT 12""",
        (window_days,),
    )]
    total = int(row['n'] or 0) if row else 0
    priced = int(row['priced'] or 0) if row else 0
    prior_priced = int(row['prior_priced'] or 0) if row else 0
    return {
        'catalogItems': total,
        'marketItems': total,
        'quotedItems': priced,
        'observedItems': priced,
        'observationRows': total,
        'priorItems': prior_priced,
        'priorCoveragePct': round((prior_priced / priced) * 100, 1) if priced else 0,
        'latestObservationAt': int(row['latest_at'] or 0) if row else 0,
        'coveragePct': round((priced / total) * 100, 1) if total else 0,
        'sources': sources,
        'dataset': {
            'id': f'market-item-lookup:{window_days}:{int(row["latest_at"] or 0) if row else 0}:{total}',
            'source': 'cached market item lookup',
            'asOf': int(row['latest_at'] or 0) if row else 0,
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
):
    limit = parse_int_param(limit, 500, minimum=25, maximum=1000)
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

    params = []
    with db() as conn:
        metric_window, exact_window, metric_count = resolve_market_metric_window(conn, history_days)
        ensure_market_item_lookup(conn, metric_window)
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
        total = conn.execute(f"SELECT COUNT(*) {base_sql}", params).fetchone()[0]
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
                {base_sql}
                ORDER BY {sort_clause}
                LIMIT ?""",
            params + [limit],
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

    return {
        'ok': True,
        'generatedAt': int(time.time()),
        'historyDays': parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS),
        'metricWindowDays': metric_window,
        'metricWindowExact': exact_window,
        'metricRows': metric_count,
        'limit': limit,
        'totalItems': total,
        'returnedItems': len(out),
        'items': out,
        'filters': options,
        'coverage': {
            **status,
            'movingItems': total,
            'returnedItems': len(out),
            'metricWindowDays': metric_window,
            'metricWindowExact': exact_window,
            'metricRows': metric_count,
        },
    }


def market_lookup_analysis_items(history_days=90, limit=100000):
    """Return market analysis rows from the materialized lookup table.

    The lookup is the release path for full-market reads: item search, market
    taxonomy, and group history all share this compact table so they do not
    rejoin catalog rows or recompute volatility for every request.
    """
    history_days = parse_int_param(history_days, 90, allowed=VOLATILITY_WINDOWS)
    limit = parse_int_param(limit, 100000, minimum=1, maximum=100000)
    with db() as conn:
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
    with db() as conn:
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
        'basis': ('steam_history_cleaned_interpolated_current_backfill' if has_backfill else 'steam_history_cleaned_interpolated') if out.get('points') else 'warming',
        'basisDetail': (
            'Cleaned/interpolated raw Steam chart history with current-price backfill for group items without cached chart rows. Dashed points identify carried/backfilled values.'
            if has_backfill else
            'Cleaned/interpolated raw Steam chart history. Carried-forward points are flagged per point.'
        ) if out.get('points') else 'Steam chart history is warming for this group.',
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
    try:
        return '${:,.2f}'.format(float(value or 0))
    except (TypeError, ValueError):
        return '$0.00'


def fmt_pct(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
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

    def _is_local(self):
        """Check if the request originates from localhost."""
        addr = self.client_address[0]
        return addr in LOCAL_ADDRS

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
        return get_session_user(self._cookies().get(SESSION_COOKIE))

    def require_auth(self, path, method):
        """Return True if the request may proceed; otherwise emit a denial."""
        if not AUTH_ENABLED:
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
        path = self.path.split('?')[0]

        if not self.require_auth(path, 'GET'):
            return

        if path == '/api/health':
            self.send_json({'ok': True, 'app': APP_NAME, 'version': APP_VERSION})

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
            snaps = get_snapshots()
            portfolios = get_portfolios()
            with db() as conn:
                catalog_count = conn.execute("SELECT COUNT(*) FROM item_catalog").fetchone()[0]
                price_count = conn.execute("SELECT COUNT(*) FROM market_item_prices").fetchone()[0]
                market_item_count = conn.execute("SELECT COUNT(*) FROM market_items").fetchone()[0]
                history_count = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
                observation_count = conn.execute("SELECT COUNT(*) FROM price_observations").fetchone()[0]
                shared_current_count = conn.execute("SELECT COUNT(*) FROM shared_item_prices").fetchone()[0]
                shared_history_count = conn.execute("SELECT COUNT(*) FROM shared_price_history").fetchone()[0]
                shared_observation_count = conn.execute("SELECT COUNT(*) FROM shared_price_observations").fetchone()[0]
                indexed_count = conn.execute("SELECT COUNT(*) FROM snapshot_items").fetchone()[0]
                portfolio_item_count = conn.execute("SELECT COUNT(*) FROM portfolio_items").fetchone()[0]
            self.send_json({
                'ok': True,
                'snaps': len(snaps),
                'portfolios': len(portfolios),
                'catalog': catalog_count,
                'marketItems': market_item_count,
                'prices': price_count,
                'priceHistoryRows': history_count,
                'priceObservationRows': observation_count,
                'sharedPriceRows': shared_current_count,
                'sharedPriceHistoryRows': shared_history_count,
                'sharedPriceObservationRows': shared_observation_count,
                'indexedRows': indexed_count,
                'portfolioItems': portfolio_item_count,
                'data_dir': DATA_DIR,
                'database': DB_FILE,
            })

        elif path == '/api/snaps':
            self.send_json(get_snapshots())

        elif path == '/api/portfolios':
            self.send_json(get_portfolios())

        elif path == '/api/settings':
            self.send_json(get_settings())

        elif path == '/api/access':
            self.send_json({'canDelete': self._is_local()})

        elif path == '/api/catalog':
            self.send_json(catalog_payload())

        elif path == '/api/prices':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            name = query.get('name', [''])[0]
            history_limit = query.get('history_limit', ['500'])[0]
            self.send_json(shared_item_pricing(name, history_limit=history_limit))

        elif path == '/api/history':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            name = query.get('name', [''])[0]
            market_hash_name = query.get('hash', [''])[0] or query.get('market_hash_name', [''])[0]
            force = query.get('force', ['0'])[0].lower() in ('1', 'true', 'yes')
            self.send_json(get_price_history(name, force=force, market_hash_name=market_hash_name or None))

        elif path == '/api/analysis':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            days = query.get('days', query.get('scale', query.get('history_days', query.get('history', ['90']))))[0]
            limit = query.get('limit', ['120'])[0]
            min_group = query.get('min_group', ['3'])[0]
            self.send_json(market_universe_analysis(history_days=days, limit=limit, min_group=min_group))

        elif path == '/api/market-universe':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            days = query.get('days', query.get('history_days', ['90']))[0]
            limit = query.get('limit', ['120'])[0]
            min_group = query.get('min_group', ['3'])[0]
            self.send_json(market_universe_analysis(history_days=days, limit=limit, min_group=min_group))

        elif path == '/api/market-items':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            ))

        elif path == '/api/taxonomy-analysis':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            self.send_json(market_universe_status())

        elif path == '/api/data-tasks':
            self.send_json(data_task_snapshot())

        elif path == '/api/inventory':
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
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

        # Delete is restricted to localhost
        if not self._is_local():
            self.send_json({'error': 'Delete is restricted to localhost'}, 403)
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
def market_refresh_is_due():
    if MARKET_REFRESH_INTERVAL_SECONDS <= 0:
        return False
    with db() as conn:
        latest = conn.execute(
            """SELECT MAX(updated_at) FROM market_item_prices
               WHERE provider IN ('steam_snapshot','csfloat','buff163','skinport','youpin')"""
        ).fetchone()[0] or 0
    return int(time.time()) - int(latest or 0) >= MARKET_REFRESH_INTERVAL_SECONDS


def background_market_refresh_loop():
    # The UI reads from SQLite; this task only appends provider observations.
    while True:
        try:
            time.sleep(60)
            if market_refresh_is_due():
                market_universe_bulk_sync('csgotrader', 'any')
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
    purge_expired_sessions()
    resumed_data_task = resume_persisted_data_task()
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
    print('  Note: Delete operations are restricted to localhost.')
    print('  Press Ctrl+C to stop.')
    print()

    server = http.server.ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Stopped.')




