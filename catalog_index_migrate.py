"""Explicit, recoverable creation of one selected small CS2 lookup index.

Default mode plans using an existing database opened read-only. Apply requires an
exact plan hash and a new, qualified external C: receipt. No DROP, ANALYZE,
VACUUM, checkpoint, journal-mode conversion or application service operation.
Requires Python 3.12+ SQLite NO_CKPT_ON_CLOSE controls for both modes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import tempfile
import time

TABLE = "market_item_lookup"
INDEX = "idx_market_lookup_change_order_v1"
CREATE_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_market_lookup_change_order_v1 "
    "ON market_item_lookup(window_days, (change_pct) IS NULL ASC, "
    "change_pct DESC, LOWER(name) ASC)"
)
VERIFY_SQL = (
    "EXPLAIN QUERY PLAN SELECT name,change_pct FROM market_item_lookup "
    "WHERE window_days=? ORDER BY (change_pct) IS NULL ASC, "
    "change_pct DESC, LOWER(name) ASC LIMIT 0"
)
MAX_ROWS = 250_000
MAX_SQL_CALLS = 64
MAX_INDEXES = 16
DB_FREE_FLOOR = 1 << 30
RECEIPT_FREE_FLOOR = 64 << 20
MAX_RECEIPT_BYTES = 64 << 10
VERSION = 1
SEARCH_INDEX = "idx_market_lookup_search_order_v1"
SEARCH_CREATE_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_market_lookup_search_order_v1 "
    "ON market_item_lookup(window_days, (change_pct) IS NULL ASC, "
    "change_pct DESC, LOWER(name) ASC, name COLLATE BINARY ASC, search_text)"
)
SEARCH_COUNT_SQL = (
    "SELECT COUNT(*) FROM market_item_lookup "
    "WHERE window_days=? AND search_text LIKE ?"
)
SEARCH_PAGE_SQL = (
    "SELECT rowid FROM market_item_lookup "
    "WHERE window_days=? AND search_text LIKE ? "
    "ORDER BY (change_pct) IS NULL ASC, change_pct DESC, LOWER(name) ASC, name COLLATE BINARY ASC LIMIT ?"
)
# Engineering admission limits for the optional wider index, not retention.
# The standalone process enforces SQLite's allocator and value/record limits;
# its external sorter can use a qualified C: temporary directory.
MAX_SQLITE_VALUE_BYTES = 4096
SQLITE_HEAP_BYTES = 256 << 20


def index_spec(index=INDEX):
    if index == INDEX:
        return {"name": INDEX, "ddl": CREATE_SQL, "version": VERSION, "search": False}
    if index == SEARCH_INDEX:
        return {"name": SEARCH_INDEX, "ddl": SEARCH_CREATE_SQL, "version": 2, "search": True}
    raise Refused("index must be one of the two exact supported index names")


def expected_index_keys(columns, spec):
    by_name = {row["name"].casefold(): row for row in columns}
    keys = [(by_name["window_days"]["cid"], "window_days", 0, "BINARY", 1),
            (-2, None, 0, "BINARY", 1),
            (by_name["change_pct"]["cid"], "change_pct", 1, "BINARY", 1),
            (-2, None, 0, "BINARY", 1)]
    if spec["search"]:
        keys.append((by_name["name"]["cid"], "name", 0, "BINARY", 1))
        keys.append((by_name["search_text"]["cid"], "search_text", 0, "BINARY", 1))
    keys.append((-1, None, 0, "BINARY", 0))
    return keys


def key_tuples(rows):
    return [(row["cid"], row["name"], row["desc"], row["coll"], row["key"]) for row in rows]


class Refused(RuntimeError):
    """A concrete preflight/verification condition prevented a write."""


class Limits:
    def __init__(self, seconds=3.0, vm_opcodes=5_000_000, max_rows=MAX_ROWS):
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not 0 < seconds <= 10:
            raise Refused("seconds must be positive and at most 10")
        if type(vm_opcodes) is not int or not 1000 <= vm_opcodes <= 20_000_000:
            raise Refused("vm_opcodes must be an integer from 1000 to 20000000")
        if type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
            raise Refused("max_rows can only lower the supported 250000-row domain")
        self.seconds, self.vm_opcodes, self.max_rows = float(seconds), vm_opcodes, max_rows

    def values(self):
        return {"seconds": self.seconds, "vm_opcodes": self.vm_opcodes,
                "max_rowid_span": self.max_rows, "max_sql_calls": MAX_SQL_CALLS}


class Budget:
    def __init__(self, limits):
        self.limits, self.started = limits, time.monotonic()
        self.sql_calls = self.opcodes = 0
        self.interrupted_reason = None

    def check(self):
        if time.monotonic() - self.started >= self.limits.seconds:
            raise Refused("cooperative wall-time budget exhausted")
        if self.opcodes >= self.limits.vm_opcodes:
            raise Refused("SQLite VM-opcode budget exhausted")

    def progress(self):
        self.opcodes += 100
        if self.opcodes >= self.limits.vm_opcodes:
            self.interrupted_reason = "SQLite VM-opcode budget exhausted"
            return 1
        if time.monotonic() - self.started >= self.limits.seconds:
            self.interrupted_reason = "cooperative wall-time budget exhausted"
            return 1
        return 0

    def query(self, conn, sql, params=(), max_results=130, check_after=True):
        self.check()
        self.sql_calls += 1
        if self.sql_calls > MAX_SQL_CALLS:
            raise Refused("fixed 64-SQL-call budget exhausted")
        rows = conn.execute(sql, params).fetchmany(max_results + 1)
        if len(rows) > max_results:
            raise Refused("bounded metadata result exceeded its fixed row limit")
        if check_after:
            self.check()
        return [dict(row) for row in rows]

    def report(self):
        return {"elapsed_seconds": time.monotonic() - self.started,
                "sql_calls": self.sql_calls, "vm_opcodes_approx": self.opcodes,
                "opcode_resolution": 100, "interrupt_reason": self.interrupted_reason,
                "time_guarantee": "cooperative SQLite progress and between-call checks; rollback/close/fsync may exceed deadline"}


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def normalized_index_sql(sql):
    # This accepts only case/whitespace differences and SQLite's removal of
    # IF NOT EXISTS. It does not normalize arbitrary equivalent SQL expressions.
    sql = re.sub(r"\bIF\s+NOT\s+EXISTS\b", "", sql or "", flags=re.I)
    return re.sub(r"\s+", "", sql.strip().rstrip(";")).casefold()


def file_state(path):
    try:
        value = path.stat()
        return {"exists": True, "bytes": value.st_size, "mtime_ns": value.st_mtime_ns,
                "device": value.st_dev, "inode": value.st_ino}
    except FileNotFoundError:
        return {"exists": False, "bytes": 0}


def disk_state(database, receipt):
    return {"database": file_state(database),
            "wal": file_state(Path(str(database) + "-wal")),
            "database_volume_free_bytes": shutil.disk_usage(database.parent).free,
            "receipt_volume_free_bytes": shutil.disk_usage(receipt.parent).free}


def search_temp_directory():
    temporary = Path(tempfile.gettempdir()).resolve(strict=True)
    if os.name != "nt" or temporary.drive.casefold() != "c:" or not temporary.is_dir():
        raise Refused("wider index requires an existing qualified C: temporary directory")
    return temporary


def storage_check(state):
    if state["database_volume_free_bytes"] < DB_FREE_FLOOR:
        raise Refused("database volume has less than the required 1 GiB free-space floor")
    if state["receipt_volume_free_bytes"] < RECEIPT_FREE_FLOOR:
        raise Refused("receipt volume has less than the required 64 MiB free-space floor")


def qualify(database, receipt):
    database, receipt = Path(database), Path(receipt)
    if not database.is_absolute() or not receipt.is_absolute():
        raise Refused("database and receipt paths must be absolute")
    database = database.resolve(strict=True)
    if not database.is_file():
        raise Refused("database must be an existing regular file")
    receipt_parent = receipt.parent.resolve(strict=True)
    receipt = receipt_parent / receipt.name
    if os.name != "nt" or receipt.drive.casefold() != "c:":
        raise Refused("durable receipt must be on a qualified absolute Windows C: path")
    if receipt.suffix.casefold() != ".json":
        raise Refused("receipt must be a new .json file")
    reserved = {"CON", "PRN", "AUX", "NUL"} | {f"{prefix}{i}" for prefix in ("COM", "LPT") for i in range(1, 10)}
    if any(char in receipt.name for char in '<>:"/\\|?*\0') or receipt.name.split(".")[0].upper() in reserved:
        raise Refused("receipt basename must be an ordinary Windows file name, without alternate streams or device names")
    if receipt == database or receipt.is_relative_to(database.parent):
        raise Refused("receipt must be external to the database directory tree")
    if receipt.exists():
        raise Refused("receipt already exists; inspect its recorded outcome before choosing any new action")
    return database, receipt


def connection(database, writable, budget, spec=None):
    if sys.version_info < (3, 12) or not hasattr(sqlite3.Connection, "setconfig"):
        raise Refused("Python 3.12+ SQLite setconfig/getconfig support is required")
    flag = getattr(sqlite3, "SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE", None)
    if flag is None:
        raise Refused("SQLite NO_CKPT_ON_CLOSE is unavailable")
    mode = "rw" if writable else "ro"
    conn = sqlite3.connect(database.as_uri() + "?mode=" + mode, uri=True,
                           timeout=0, isolation_level=None)
    try:
        if spec and spec["search"]:
            conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_SQLITE_VALUE_BYTES)
            if conn.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) != MAX_SQLITE_VALUE_BYTES:
                raise Refused("bounded SQLite value/record length could not be verified")
        conn.setconfig(flag, True)
        if conn.getconfig(flag) is not True:
            raise Refused("NO_CKPT_ON_CLOSE could not be verified")
        conn.row_factory = sqlite3.Row
        conn.set_progress_handler(budget.progress, 100)
        budget.query(conn, "PRAGMA wal_autocheckpoint=0")
        budget.query(conn, "PRAGMA cache_spill=OFF")
        budget.query(conn, "PRAGMA cache_size=-8192")
        budget.query(conn, "PRAGMA temp_store=" + ("FILE" if spec and spec["search"] else "MEMORY"))
        search_settings = {}
        if spec and spec["search"]:
            # Both pragmas affect this dedicated utility process, never import
            # or the application process. Unsupported controls fail closed.
            heap = budget.query(conn, "PRAGMA hard_heap_limit=" + str(SQLITE_HEAP_BYTES), (), 1)
            temporary = search_temp_directory()
            quoted = "'" + str(temporary).replace("'", "''") + "'"
            budget.query(conn, "PRAGMA temp_store_directory=" + quoted, (), 0)
            temp_setting = budget.query(conn, "PRAGMA temp_store_directory", (), 1)
            temp_store = budget.query(conn, "PRAGMA temp_store", (), 1)
            if (len(heap) != 1 or not 0 < heap[0].get("hard_heap_limit", 0) <= SQLITE_HEAP_BYTES
                    or len(temp_setting) != 1
                    or Path(temp_setting[0].get("temp_store_directory", "")).resolve() != temporary
                    or temp_store != [{"temp_store": 1}]):
                raise Refused("standalone heap/temp-file controls could not be verified")
            search_settings = {"sqlite_hard_heap_limit_bytes": heap[0]["hard_heap_limit"],
                               "sqlite_value_record_limit_bytes": MAX_SQLITE_VALUE_BYTES,
                               "temp_store": "FILE", "temp_directory": str(temporary)}
        if not writable:
            budget.query(conn, "PRAGMA query_only=ON")
        settings = {
            "busy_timeout": budget.query(conn, "PRAGMA busy_timeout")[0]["timeout"],
            "wal_autocheckpoint": budget.query(conn, "PRAGMA wal_autocheckpoint")[0]["wal_autocheckpoint"],
            "cache_spill": budget.query(conn, "PRAGMA cache_spill")[0]["cache_spill"],
            "journal_mode": budget.query(conn, "PRAGMA journal_mode")[0]["journal_mode"],
            "no_checkpoint_on_close": conn.getconfig(flag),
        }
        if settings != {"busy_timeout": 0, "wal_autocheckpoint": 0, "cache_spill": 0,
                        "journal_mode": "wal", "no_checkpoint_on_close": True}:
            raise Refused("required existing WAL/zero-wait/no-checkpoint connection settings were not verified: " + repr(settings))
        settings.update(search_settings)
        return conn, settings
    except BaseException:
        conn.set_progress_handler(None, 0)
        conn.close()
        raise


def metadata(conn, budget, limits, spec=None):
    spec = spec or index_spec()
    tables = budget.query(conn, "SELECT name,type,wr,strict FROM pragma_table_list "
                          "WHERE schema='main' AND name=?", (TABLE,), 1)
    if len(tables) != 1 or tables[0]["type"] != "table" or tables[0]["wr"] != 0:
        raise Refused("lookup must be an existing ordinary rowid table")
    definitions = budget.query(conn, "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                               "WHERE tbl_name=? AND type IN ('table','index') ORDER BY type,name LIMIT ?",
                               (TABLE, MAX_INDEXES + 2), MAX_INDEXES + 1)
    table_sql = [row["sql"] for row in definitions if row["type"] == "table"]
    if len(table_sql) != 1 or "VIRTUAL" in table_sql[0].upper():
        raise Refused("ordinary lookup table definition could not be established")
    columns = budget.query(conn, 'SELECT cid,name,type,"notnull" AS is_not_null,dflt_value,pk,hidden '
                           "FROM pragma_table_xinfo(?) ORDER BY cid LIMIT 129", (TABLE,), 128)
    by_name = {row["name"].casefold(): row for row in columns}
    required = {"window_days", "change_pct", "name"} | ({"search_text"} if spec["search"] else set())
    if not required.issubset(by_name):
        raise Refused("lookup is missing the exact indexed columns")
    if any(name in by_name for name in ("rowid", "_rowid_", "oid")):
        raise Refused("lookup shadows SQLite rowid aliases; span cannot be safely established")
    if any(by_name[name]["hidden"] != 0 for name in required):
        raise Refused("indexed lookup columns must be stored ordinary columns")
    indexes = budget.query(conn, "SELECT name,\"unique\",origin,partial FROM pragma_index_list(?) "
                           "ORDER BY name LIMIT ?", (TABLE, MAX_INDEXES + 1), MAX_INDEXES)
    signatures = []
    for entry in indexes:
        keys = budget.query(conn, 'SELECT seqno,cid,name,"desc",coll,"key" FROM pragma_index_xinfo(?) '
                            "ORDER BY seqno LIMIT 33", (entry["name"],), 32)
        signatures.append({**entry, "keys": keys})
    # Both boundaries use the integer primary-key B-tree. Never COUNT or scan.
    first = budget.query(conn, "SELECT rowid AS boundary FROM market_item_lookup ORDER BY rowid ASC LIMIT 1", (), 1)
    last = budget.query(conn, "SELECT rowid AS boundary FROM market_item_lookup ORDER BY rowid DESC LIMIT 1", (), 1)
    if bool(first) != bool(last):
        raise Refused("inconsistent rowid boundaries")
    low = first[0]["boundary"] if first else None
    high = last[0]["boundary"] if last else None
    if low is not None and (type(low) is not int or type(high) is not int or low < 1
                            or high > 9_007_199_254_740_991 or high < low):
        raise Refused("negative/zero/loose big-integer rowid domain is unsupported")
    span = high - low + 1 if low is not None else 0
    if span > limits.max_rows:
        raise Refused(f"conservative rowid-span upper bound {span} exceeds {limits.max_rows}")
    candidate = next((row for row in definitions if row["name"] == spec["name"]), None)
    # A conflicting index name on a different table must also fail, never let
    # CREATE IF NOT EXISTS silently skip it.
    global_candidate = budget.query(conn, "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=?", (spec["name"],), 1)
    if global_candidate and not candidate:
        raise Refused("candidate index name belongs to another schema object")
    if not candidate and len(indexes) >= MAX_INDEXES:
        raise Refused("adding the candidate would exceed the fixed metadata index-count limit")
    if candidate:
        if candidate["type"] != "index" or normalized_index_sql(candidate["sql"]) != normalized_index_sql(spec["ddl"]):
            raise Refused("existing candidate index definition does not match the exact approved DDL")
        signature = next((row for row in signatures if row["name"] == spec["name"]), None)
        expected_keys = expected_index_keys(columns, spec)
        actual_keys = key_tuples((signature or {}).get("keys", []))
        if (not signature or signature["unique"] != 0 or signature["origin"] != "c"
                or signature["partial"] != 0 or actual_keys != expected_keys):
            raise Refused("existing candidate index signature does not match the approved expression/order signature")
    schema_version = budget.query(conn, "PRAGMA schema_version")[0]["schema_version"]
    return {"table": tables[0], "definitions": definitions, "columns": columns,
            "index_signatures": signatures, "schema_version": schema_version,
            "rowid_lower": low, "rowid_upper": high, "row_count_upper_bound": span,
            "candidate_exists_exactly": bool(candidate)}


def explain(conn, budget, require_candidate, spec=None):
    spec = spec or index_spec()
    if spec["search"]:
        return explain_search(conn, budget, require_candidate, spec)
    rows = budget.query(conn, VERIFY_SQL, (90,), 16)
    descriptions = [row["detail"] for row in rows]
    if require_candidate and (not any("SEARCH " in value and INDEX in value for value in descriptions)
                              or any("TEMP" in value.upper() for value in descriptions)):
        raise Refused("default-query EXPLAIN does not SEARCH the candidate index without a temporary sort")
    return descriptions


def search_capacity(details, settings):
    """A worst-case admission allowance, with no wide lookup preflight scan."""
    keys = details["row_count_upper_bound"] * MAX_SQLITE_VALUE_BYTES
    return {"row_count_upper_bound": details["row_count_upper_bound"],
            "sqlite_value_record_limit_bytes": MAX_SQLITE_VALUE_BYTES,
            "encoded_key_bytes_upper_bound": keys,
            "sqlite_hard_heap_limit_bytes": settings["sqlite_hard_heap_limit_bytes"],
            "estimated_peak_file_bytes": 4 * keys + (8 << 20),
            "temp_directory": settings["temp_directory"],
            "method": "rowid-span times enforced record limit; fourfold disk allowance plus 8 MiB; no sampled or aggregate size proof"}


def search_storage_check(state, capacity):
    storage_check(state)
    if state["database_volume_free_bytes"] < DB_FREE_FLOOR + capacity["estimated_peak_file_bytes"]:
        raise Refused("database free space is below floor plus estimated wider-index build allowance")
    temporary_free = shutil.disk_usage(capacity["temp_directory"]).free
    if temporary_free < RECEIPT_FREE_FLOOR + capacity["estimated_peak_file_bytes"]:
        raise Refused("C: temporary volume is below floor plus wider-index sort allowance")
    return temporary_free


def explain_search(conn, budget, require_candidate, spec):
    result = {}
    table_root = None
    if require_candidate:
        table_root = budget.query(conn, "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name=?",
                                  (TABLE,), 1)[0]["rootpage"]
    for label, sql, params in (
        ("filtered_count", SEARCH_COUNT_SQL, (90, "%ak-47%")),
        ("filtered_rowid_page", SEARCH_PAGE_SQL, (90, "%ak-47%", 100)),
    ):
        eqp = budget.query(conn, "EXPLAIN QUERY PLAN " + sql, params, 16)
        descriptions = [row["detail"] for row in eqp]
        evidence = {"sql": sql, "parameters": list(params), "query_plan": descriptions}
        if require_candidate:
            if (not any("SEARCH " in value and spec["name"] in value for value in descriptions)
                    or any("TEMP" in value.upper() for value in descriptions)):
                raise Refused(label + " does not SEARCH the candidate without a temporary sort")
            # Expression indexes may omit the EQP word COVERING even when all
            # values come from the index. Verify the actual bounded VM program:
            # any table Column/Rowid fetch would defeat late materialization.
            vm = budget.query(conn, "EXPLAIN " + sql, params, 128)
            table_cursors = {row["p1"] for row in vm if row["opcode"] == "OpenRead"
                             and row["p2"] == table_root and row["p3"] == 0}
            table_reads = [row["opcode"] for row in vm if row["opcode"] in ("Column", "Rowid")
                           and row["p1"] in table_cursors]
            if table_reads:
                raise Refused(label + " fetches table payload instead of index-only keys")
            evidence.update(index_only_verified=True, vm_instructions=len(vm),
                            table_payload_read_opcodes=len(table_reads))
        result[label] = evidence
    return result


def make_plan(database, receipt, limits, state, details, settings, query_plan, spec=None, capacity=None):
    spec = spec or index_spec()
    identity = {key: state["database"][key] for key in ("device", "inode")}
    payload = {
        "utility_version": spec["version"], "database": str(database), "database_identity": identity,
        "receipt": str(receipt), "limits": limits.values(), "candidate_ddl": spec["ddl"],
        "lookup": details, "verified_connection_settings": settings,
        "storage_floors": {"database_bytes": DB_FREE_FLOOR, "receipt_bytes": RECEIPT_FREE_FLOOR},
    }
    if spec["search"]:
        payload.update(candidate_index=spec["name"], encoded_key_capacity=capacity,
                       verified_queries=query_plan)
    result = {"plan_hash": digest(payload), "bound_plan": payload, "observed_storage": state,
            "default_query_plan": query_plan,
            "limitations": [
                "Rowid span bounds entries, not encoded text bytes or physical WAL/I/O volume.",
                "Free-space floors are current observations, not a hard peak-space guarantee.",
                "Expected hash binds database identity/schema/index signatures/boundaries/limits/receipt, not immutable current row values.",
                "Checks are cooperative; blocking OS I/O, fsync, rollback and close can exceed elapsed budget.",
                "No checkpoint, journal conversion, index drop, raw-data change, ANALYZE or service operation.",
            ]}
    if spec["search"]:
        result["limitations"].extend([
            "Wider-index mode is standalone only: the SQLite hard heap cap and temporary-directory setting are process-wide; they are never set by import.",
            "The 256 MiB hard cap covers SQLite allocator memory, not total Python/OS resident memory. Metadata and receipt results retain their fixed limits.",
            "The enforced 4096-byte SQLite value/record limit can refuse an oversized key or source record; never automatically retry with a larger limit.",
            "C: temporary files are permitted. The fourfold worst-case encoded-key disk allowance is conservative admission planning, not a hard physical allocation cap.",
        ])
    return result


def durable_receipt(path, document, new=False):
    payload = canonical(document) + b"\n"
    if len(payload) > MAX_RECEIPT_BYTES:
        raise Refused("receipt exceeds the fixed 64 KiB metadata budget")
    # New receipt uses exclusive creation; updates use a same-directory durable
    # temporary file then atomic replacement. Failed update leaves prior receipt.
    temporary = path if new else path.with_name(path.name + f".{os.getpid()}.next")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if not new:
            os.replace(temporary, path)
    except BaseException:
        # Keep a partial new receipt or update file for explicit diagnosis;
        # never turn uncertain write outcomes into an automatic retry.
        raise


def run(database, receipt, *, apply=False, expected_plan_hash=None, limits=None, index=INDEX,
        standalone_process=False):
    limits = limits or Limits()
    budget = Budget(limits)
    report = {"status": "starting", "mode": "apply" if apply else "plan", "pid": os.getpid(),
              "started_utc": utc(), "limits": limits.values(), "ddl_attempted": False,
              "commit_attempted": False, "commit_confirmed": False, "receipt_written": False,
              "utility_exact_file_bytes_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "python_version": sys.version, "sqlite_version": sqlite3.sqlite_version}
    conn = None
    qualified = None
    receipt_document = None
    transaction_open = False
    try:
        spec = index_spec(index)
        if spec["search"] and standalone_process is not True:
            raise Refused("wider-index mode must run in a dedicated CLI process; allocator/temp controls are process-wide")
        database, receipt = qualify(database, receipt)
        qualified = (database, receipt)
        report.update(database=str(database), receipt=str(receipt))
        if apply and (not isinstance(expected_plan_hash, str)
                      or not re.fullmatch(r"[a-f0-9]{64}", expected_plan_hash)):
            raise Refused("apply requires an explicit lowercase SHA-256 expected plan hash")
        before = disk_state(database, receipt)
        storage_check(before)
        report["storage_before"] = before
        conn, settings = connection(database, apply, budget, spec)
        # Apply's structural/capacity checks occur with the write lock already
        # held; a competing writer gets no wait and cannot race the row bounds.
        budget.query(conn, "BEGIN IMMEDIATE" if apply else "BEGIN")
        transaction_open = True
        details = metadata(conn, budget, limits, spec)
        capacity = search_capacity(details, settings) if spec["search"] else None
        query_plan = explain(conn, budget, details["candidate_exists_exactly"], spec)
        current_state = disk_state(database, receipt)
        storage_check(current_state)
        if capacity:
            report["temporary_volume_free_bytes"] = search_storage_check(current_state, capacity)
        plan = make_plan(database, receipt, limits, current_state, details, settings, query_plan, spec, capacity)
        report["plan"] = plan
        if not apply:
            budget.query(conn, "ROLLBACK")
            transaction_open = False
            report["status"] = "planned"
            return report
        if expected_plan_hash != plan["plan_hash"]:
            raise Refused("expected plan hash mismatch; no DDL attempted")
        if details["candidate_exists_exactly"]:
            budget.query(conn, "ROLLBACK")
            transaction_open = False
            report["status"] = "already_present_verified"
            return report
        receipt_document = {
            "receipt_version": spec["version"], "status": "pending_before_ddl",
            "created_utc": utc(), "pid": os.getpid(), "plan_hash": plan["plan_hash"],
            "database": str(database), "database_identity": plan["bound_plan"]["database_identity"],
            "candidate_index": spec["name"], "candidate_ddl": spec["ddl"],
            "existing_indexes": [row["name"] for row in details["index_signatures"]],
            "row_count_upper_bound": details["row_count_upper_bound"],
            "limits": limits.values(), "storage_before": before,
            "recovery": "Inspect sqlite_schema and the exact candidate signature read-only. Logical source rollback may leave this additive index in place. Never drop historical indexes or raw data. A pending commit receipt has uncertain outcome until inspected.",
        }
        if spec["search"]:
            receipt_document["encoded_key_capacity"] = capacity
        durable_receipt(receipt, receipt_document, new=True)
        report["receipt_written"] = True
        budget.check()
        report["ddl_attempted"] = True
        budget.query(conn, spec["ddl"], (), 0)
        # Re-read exact candidate schema, then query-plan verification. The
        # approved DDL dictates its signature; future plans also inspect xinfo.
        candidate_sql = budget.query(conn, "SELECT sql FROM sqlite_schema WHERE type='index' AND name=? AND tbl_name=?",
                                     (spec["name"], TABLE), 1)
        if len(candidate_sql) != 1 or normalized_index_sql(candidate_sql[0]["sql"]) != normalized_index_sql(spec["ddl"]):
            raise Refused("created index DDL verification failed")
        if spec["search"]:
            created_keys = budget.query(conn, 'SELECT seqno,cid,name,"desc",coll,"key" FROM pragma_index_xinfo(?) '
                                        "ORDER BY seqno LIMIT 33", (spec["name"],), 32)
            if key_tuples(created_keys) != expected_index_keys(details["columns"], spec):
                raise Refused("created wider-index signature verification failed")
            report["post_ddl_index_signature"] = created_keys
        report["post_ddl_query_plan"] = explain(conn, budget, True, spec)
        before_commit = disk_state(database, receipt)
        storage_check(before_commit)
        if capacity:
            search_storage_check(before_commit, capacity)
        receipt_document.update(status="pending_commit", precommit_utc=utc(),
                                storage_before_commit=before_commit,
                                verified_query_plan=report["post_ddl_query_plan"])
        if spec["search"]:
            receipt_document["verified_index_signature"] = report["post_ddl_index_signature"]
        durable_receipt(receipt, receipt_document)
        budget.check()
        report["commit_attempted"] = True
        # Record success immediately after SQLite returns, even if commit
        # crossed the cooperative deadline. A post-call budget exception must
        # never mislabel a successful commit as a rolled-back transaction.
        budget.query(conn, "COMMIT", (), 0, check_after=False)
        transaction_open = False
        report["commit_confirmed"] = True
        report["status"] = "applied"
        receipt_document.update(status="committed", committed_utc=utc())
    except BaseException as exc:
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        report["status"] = ("commit_outcome_requires_inspection" if report["commit_attempted"]
                            and not report["commit_confirmed"] else "refused")
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            report["error"]["interrupted"] = True
    finally:
        if conn is not None:
            conn.set_progress_handler(None, 0)
            if conn.in_transaction:
                try:
                    conn.rollback()
                    report["rollback_confirmed"] = True
                    if not report["commit_confirmed"]:
                        report["status"] = "rolled_back"
                except BaseException as exc:
                    report["rollback_error"] = str(exc)
                    report["status"] = "outcome_requires_inspection"
            elif transaction_open and not report["commit_attempted"]:
                report["sqlite_transaction_already_ended"] = True
            try:
                conn.close()
                report["connection_closed"] = True
            except BaseException as exc:
                report["connection_closed"] = False
                report["close_error"] = str(exc)
                if not report["commit_confirmed"]:
                    report["status"] = "outcome_requires_inspection"
        else:
            report["connection_closed"] = True
        if qualified:
            try:
                after = disk_state(*qualified)
                report["storage_after"] = after
                if "storage_before" in report:
                    report["file_size_deltas"] = {
                        key: after[key]["bytes"] - report["storage_before"][key]["bytes"]
                        for key in ("database", "wal")
                    }
            except OSError as exc:
                report["post_storage_error"] = str(exc)
        report["budget"] = budget.report()
        report["ended_utc"] = utc()
        if receipt_document is not None and report["receipt_written"]:
            receipt_document.update(final_status=report["status"], ended_utc=report["ended_utc"],
                                    budget=report["budget"],
                                    storage_after=report.get("storage_after"),
                                    file_size_deltas=report.get("file_size_deltas"),
                                    error=report.get("error"),
                                    rollback_confirmed=report.get("rollback_confirmed", False),
                                    commit_confirmed=report["commit_confirmed"])
            if report.get("rollback_confirmed") and not report["commit_confirmed"]:
                receipt_document["status"] = "rolled_back"
            try:
                durable_receipt(qualified[1], receipt_document)
            except BaseException as exc:
                report["receipt_finalization_error"] = str(exc)
                report["status"] = ("committed_receipt_update_failed" if report["commit_confirmed"]
                                    else "receipt_update_failed_inspect_pending")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path,
                        help="new external absolute C: .json path; not created in default plan mode")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--vm-opcodes", type=int, default=5_000_000)
    parser.add_argument("--max-rows", type=int, default=MAX_ROWS)
    parser.add_argument("--index", choices=(INDEX, SEARCH_INDEX), default=INDEX,
                        help="exact index to plan/apply; existing change-order v1 remains the default")
    args = parser.parse_args()
    try:
        limits = Limits(args.seconds, args.vm_opcodes, args.max_rows)
        report = run(args.database, args.receipt, apply=args.apply,
                     expected_plan_hash=args.expected_plan_hash, limits=limits, index=args.index,
                     standalone_process=True)
    except (ValueError, Refused, OSError) as exc:
        report = {"status": "refused", "error": {"type": type(exc).__name__, "message": str(exc)},
                  "ddl_attempted": False, "commit_confirmed": False}
    print(json.dumps(report, indent=2))
    return 0 if report["status"] in ("planned", "applied", "already_present_verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
