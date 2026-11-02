"""Synthetic recovery proofs: no live databases, holdings, providers or network."""
import hashlib
from contextlib import contextmanager, closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import zlib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import research_recovery as recovery


TEXT = {'name', 'provider', 'market_hash_name', 'kind', 'item_type', 'weapon', 'collection',
        'rarity', 'source', 'basis', 'url', 'raw', 'price_source', 'groups_json', 'search_text',
        'price_basis', 'provider_prices_json'}
WINDOW = {'window_days', 'name', 'prior_price', 'change_pct', 'change_dollar', 'volatility_pct',
          'history_points', 'edge_case_count', 'groups_json', 'metric_updated_at'}


@contextmanager
def connection(path):
    conn = sqlite3.connect(path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def make_table(conn, table, fields, extra=''):
    declarations = ','.join('"' + key + '" ' + ('TEXT' if key in TEXT else 'REAL') for key in fields)
    conn.execute('CREATE TABLE "' + table + '"(' + declarations + extra + ')')


def insert(conn, table, fields, value):
    conn.execute('INSERT INTO ' + table + '(' + ','.join('"' + key + '"' for key in fields)
                 + ') VALUES(' + ','.join('?' for _ in fields) + ')', [value.get(key) for key in fields])


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cs2-recovery-synthetic-')
        self.root = Path(self.temp.name)
        self.source = self.root / 'personal-source.sqlite'
        self.target = self.root / 'offline-target.sqlite'
        self.derived = self.root / 'derived.sqlite'
        self.archive = self.root / 'recovery.zip'
        self.make_target_source()
        self.make_recovery_sources()

    def tearDown(self):
        self.temp.cleanup()

    def make_target_source(self):
        with connection(self.source) as c:
            for table, fields in recovery.EXPECTED.items():
                make_table(c, table, fields)
            c.execute('CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT)')
            c.execute('INSERT INTO settings VALUES(?,?)', ('private-setting', 'synthetic-sentinel'))
            c.execute('CREATE TABLE portfolio_items(name TEXT,qty INTEGER)')
            c.execute('INSERT INTO portfolio_items VALUES(?,?)', ('Synthetic holding', 2))
            c.execute('CREATE TABLE price_history(name TEXT,point_ts INTEGER,price REAL)')
            c.execute("CREATE VIRTUAL TABLE market_item_lookup_fts USING fts5(search_text,content='market_item_lookup',content_rowid='rowid')")

    def make_recovery_sources(self, missing_raw=False, missing_atom=False):
        hot = sqlite3.connect(':memory:')
        hot.execute('CREATE TABLE atoms(id INTEGER PRIMARY KEY,value TEXT NOT NULL)')
        atoms = [(1, 'Synthetic item'), (2, 'steam_snapshot'), (3, 'synthetic-provider-snapshot'),
                 (4, 'recorded_snapshot'), (5, 'reference'), (6, 'https://example.invalid/synthetic')]
        hot.executemany('INSERT INTO atoms VALUES(?,?)', atoms[:-1] if missing_atom else atoms)
        hot.execute('CREATE TABLE quotes(legacy_rowid INTEGER PRIMARY KEY,' + ','.join('"' + key + '"' for key in recovery.PRICE_COLUMNS) + ')')
        original_raw = json.dumps({'source': 'Synthetic archived evidence'}, separators=(',', ':'))
        self.original_raw = original_raw
        value = dict(name=1, provider=2, price=12.5, median=12, solid_price=12.5, volume=4,
                     source=3, basis=4, kind=5, url=6, raw=999 if missing_raw else 1,
                     observed_at=100, updated_at=200)
        insert(hot, 'quotes', recovery.PRICE_COLUMNS, value)
        cold = sqlite3.connect(':memory:')
        cold.execute('CREATE TABLE evidence(id INTEGER PRIMARY KEY,sha256 TEXT,compressed INTEGER,body BLOB)')
        cold.execute('INSERT INTO evidence VALUES(?,?,?,?)',
                     (1, hashlib.sha256(original_raw.encode()).hexdigest(), 1, zlib.compress(original_raw.encode())))
        with zipfile.ZipFile(self.archive, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr(recovery.HOT_MEMBER, hot.serialize())
            z.writestr(recovery.COLD_MEMBER, cold.serialize())
        hot.close()
        cold.close()
        if not self.derived.exists():
            with connection(self.derived) as c:
                make_table(c, 'item_market_metrics', recovery.METRIC_COLUMNS)
                insert(c, 'item_market_metrics', recovery.METRIC_COLUMNS,
                       dict(name='Synthetic item', provider='steam_history', window_days=90,
                            points=3, clean_points=3, first_ts=10, last_ts=100, updated_at=200,
                            raw='{"synthetic":true}'))
                common_fields = ('common_id', 'name') + tuple(key for key in recovery.LOOKUP_COLUMNS if key not in WINDOW)
                make_table(c, 'lookup_common', common_fields)
                insert(c, 'lookup_common', common_fields,
                       dict(common_id=1, name='Synthetic item', market_hash_name='Synthetic item',
                            source='steam_snapshot', latest_price=12.5, latest_at=100,
                            search_text='synthetic item', provider_prices_json='{}'))
                window_fields = ('common_id',) + tuple(key for key in recovery.LOOKUP_COLUMNS if key in WINDOW)
                make_table(c, 'lookup_windows', window_fields)
                insert(c, 'lookup_windows', window_fields,
                       dict(common_id=1, window_days=90, name='Synthetic item', prior_price=10,
                            change_pct=25, history_points=3, metric_updated_at=200, groups_json='[]'))

    def clone(self):
        recovery.clone_database(self.source, self.target)

    def test_plan_is_read_only_and_exposes_no_asset_rows(self):
        before = recovery.digest(self.source)
        plan = recovery.plan_recovery(self.source, self.derived, self.archive)
        self.assertEqual(plan['expectedRows'], dict.fromkeys(recovery.TABLES, 1))
        self.assertEqual(plan['asOf']['quotesObservedAt'], [100, 100])
        self.assertNotIn('Synthetic item', json.dumps(plan))
        self.assertEqual(before, recovery.digest(self.source))

    def test_apply_requires_helper_created_offline_clone(self):
        before = recovery.digest(self.source)
        with self.assertRaisesRegex(recovery.RecoveryError, 'clone first'):
            recovery.apply_recovery(self.source, self.derived, self.archive)
        self.assertEqual(before, recovery.digest(self.source))

    def test_complete_additive_recovery_preserves_personal_and_raw_history(self):
        with connection(self.source) as c:
            c.execute('INSERT INTO price_history VALUES(?,?,?)', ('Synthetic existing chart', 1, 2))
        source_hash = recovery.digest(self.source)
        self.clone()
        receipt = recovery.apply_recovery(self.target, self.derived, self.archive)
        self.assertEqual(receipt['status'], 'applied')
        self.assertEqual(receipt['rawSteamChartRowsImported'], 0)
        self.assertEqual(receipt['importedRows'], dict.fromkeys(recovery.TABLES, 1))
        self.assertTrue(receipt['ftsRebuilt'])
        with connection(self.target) as c:
            self.assertEqual(c.execute('SELECT * FROM settings').fetchall(), [('private-setting', 'synthetic-sentinel')])
            self.assertEqual(c.execute('SELECT * FROM portfolio_items').fetchall(), [('Synthetic holding', 2)])
            self.assertEqual(c.execute('SELECT * FROM price_history').fetchall(), [('Synthetic existing chart', 1, 2)])
            self.assertEqual(c.execute('SELECT provider,observed_at,updated_at,raw FROM market_item_prices').fetchone(),
                             ('steam_snapshot', 100, 200, self.original_raw))
            self.assertEqual(c.execute('SELECT first_ts,last_ts,updated_at FROM item_market_metrics').fetchone(), (10, 100, 200))
            self.assertEqual(c.execute("SELECT COUNT(*) FROM market_item_lookup_fts WHERE market_item_lookup_fts MATCH 'synthetic'").fetchone()[0], 1)
            self.assertEqual(recovery.recovery_status(c)['sourceFingerprint'], receipt['sourceFingerprint'])
        self.assertEqual(source_hash, recovery.digest(self.source))

    def test_raw_charts_are_not_fabricated_from_metric_aggregates(self):
        self.clone()
        recovery.apply_recovery(self.target, self.derived, self.archive)
        with connection(self.target) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM price_history').fetchone()[0], 0)

    def test_identical_recovery_is_idempotent(self):
        self.clone()
        first = recovery.apply_recovery(self.target, self.derived, self.archive)
        before = recovery.digest(self.target)
        second = recovery.apply_recovery(self.target, self.derived, self.archive)
        self.assertEqual(second['status'], 'already_applied')
        self.assertEqual(first['sourceFingerprint'], second['sourceFingerprint'])
        self.assertEqual(before, recovery.digest(self.target))

    def test_wal_source_is_consolidated_only_in_new_offline_clone(self):
        with connection(self.source) as c:
            self.assertEqual(c.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
        source_hash = recovery.digest(self.source)
        self.clone()
        with closing(recovery.read_db(self.target)) as c:
            self.assertEqual(c.execute('PRAGMA journal_mode').fetchone()[0], 'delete')
        recovery.validate_clone(self.target)
        recovery.apply_recovery(self.target, self.derived, self.archive)
        self.assertEqual(source_hash, recovery.digest(self.source))

    def test_changed_clone_is_refused(self):
        self.clone()
        with connection(self.target) as c:
            c.execute('INSERT INTO settings VALUES(?,?)', ('new-private-setting', 'changed'))
        with self.assertRaisesRegex(recovery.RecoveryError, 'identity changed'):
            recovery.apply_recovery(self.target, self.derived, self.archive)

    def test_existing_compatibility_rows_are_never_overwritten(self):
        with connection(self.source) as c:
            c.execute('INSERT INTO price_quotes(name,provider,price) VALUES(?,?,?)', ('Synthetic existing', 'private-provider', 1))
        self.clone()
        before = recovery.digest(self.target)
        with self.assertRaisesRegex(recovery.RecoveryError, 'not empty'):
            recovery.apply_recovery(self.target, self.derived, self.archive)
        self.assertEqual(before, recovery.digest(self.target))

    def test_missing_source_evidence_refuses_without_target_changes(self):
        self.make_recovery_sources(missing_raw=True)
        self.clone()
        before = recovery.digest(self.target)
        with self.assertRaisesRegex(recovery.RecoveryError, 'references'):
            recovery.apply_recovery(self.target, self.derived, self.archive)
        self.assertEqual(before, recovery.digest(self.target))

    def test_missing_source_atom_refuses_without_target_changes(self):
        self.make_recovery_sources(missing_atom=True)
        self.clone()
        with self.assertRaisesRegex(recovery.RecoveryError, 'references'):
            recovery.apply_recovery(self.target, self.derived, self.archive)

    def test_late_fts_contract_failure_rolls_back_all_research_rows(self):
        with connection(self.source) as c:
            c.execute('DROP TABLE market_item_lookup_fts')
            c.execute('CREATE VIRTUAL TABLE market_item_lookup_fts USING fts5(search_text)')
        self.clone()
        with self.assertRaisesRegex(recovery.RecoveryError, 'FTS contract'):
            recovery.apply_recovery(self.target, self.derived, self.archive)
        with connection(self.target) as c:
            for table in recovery.TABLES:
                self.assertEqual(c.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
            self.assertIsNone(recovery.recovery_status(c))

    def test_oversized_evidence_rolls_back_without_personal_changes(self):
        self.clone()
        with patch.object(recovery, 'MAX_PAYLOAD_BYTES', 8):
            with self.assertRaisesRegex(recovery.RecoveryError, 'payload bound'):
                recovery.apply_recovery(self.target, self.derived, self.archive)
        with connection(self.target) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM market_item_prices').fetchone()[0], 0)
            self.assertEqual(c.execute('SELECT value FROM settings').fetchone()[0], 'synthetic-sentinel')


if __name__ == '__main__':
    unittest.main()
