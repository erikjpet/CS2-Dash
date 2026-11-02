"""Portable restoration integration contracts using invented temporary data only.

No HTTP server, provider request, browser, credential or existing data file is used.
Run: python -m unittest discover -s tools -p test_restoration_runtime.py -v
"""
from contextlib import ExitStack, closing
import importlib.util
import gc
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock
import uuid
import urllib.error
import urllib.request
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class RestorationRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cs2-restoration-synthetic-')
        self.stack = ExitStack()
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'CS2DASH_DATA_DIR': self.temp.name,
            'CS2DASH_AUTH_DISABLE': '1',
            'CS2DASH_PUBLIC_READONLY': '0',
            'AUTO_MARKET_REFRESH': '0',
            'DATA_TASK_RESUME_ENABLED': '0',
            'CS2DASH_CATALOG_STORE_ENABLED': '0',
            'CS2DASH_CATALOG_STORE_WRITES': '0',
            # Invented deployment provenance avoids the local git subprocess
            # while keeping the no-provider-process guard active.
            'CS2DASH_SOURCE_REVISION': 'a' * 40,
        }))
        self.stack.enter_context(mock.patch('urllib.request.urlopen', side_effect=AssertionError('Network forbidden in synthetic test')))
        self.stack.enter_context(mock.patch('socket.create_connection', side_effect=AssertionError('Network forbidden in synthetic test')))
        self.stack.enter_context(mock.patch('subprocess.Popen', side_effect=AssertionError('Provider subprocess forbidden in synthetic test')))
        self.module_name = 'cs2_synthetic_server_' + uuid.uuid4().hex
        spec = importlib.util.spec_from_file_location(self.module_name, ROOT / 'server.py')
        self.app = importlib.util.module_from_spec(spec)
        sys.modules[self.module_name] = self.app
        spec.loader.exec_module(self.app)
        self.app.init_db()
        import restoration_runtime
        self.runtime = restoration_runtime
        self.quote_path = Path(self.temp.name) / 'synthetic-current.sqlite'
        self.original = {name: getattr(self.app, name) for name in (
            'best_market_quotes', 'get_price_quotes', 'get_price_history',
            'market_universe_item_slice', 'market_universe_analysis',
            'taxonomy_analysis', 'aggregate_group_history', 'group_item_history',
            'get_inventory_dataset', 'full_report',
        )}

    def tearDown(self):
        pipeline = getattr(self.app, 'PRICE_PIPELINE', None)
        if pipeline is not None:
            pipeline.stop()
        cache = getattr(self.app, 'CASH_PRICE_HOLDINGS_CACHE', None)
        if cache is not None:
            cache.close()
        self.app.close_idle_wal_anchor()
        sys.modules.pop(self.module_name, None)
        self.stack.close()
        # sqlite3's legacy connection context commits without closing. Collect
        # its now-unreachable cursor/connection cycles before Windows unlink.
        gc.collect()
        self.temp.cleanup()

    def install(self):
        return self.runtime.install(self.app, quote_path=self.quote_path, allow_network=False)

    def seed_portfolio(self):
        self.app.replace_portfolios([{
            'id': 'synthetic-portfolio', 'label': 'Synthetic portfolio',
            'importedAt': '2026-01-01T00:00:00Z',
            'data': {'items': [['Synthetic item 0001', 2, None, None, None, None,
                               'Synthetic category', 'Synthetic group']],
                     'totals': {'count': 1, 'curr': 0, 'past': 0, 'dollar': 0}},
        }])

    def test_empty_startup_initializes_only_new_quote_authority(self):
        self.assertEqual(self.app.get_portfolios(), [])
        pipeline = self.install()
        self.assertIs(pipeline, self.app.PRICE_PIPELINE)
        self.assertTrue(self.quote_path.is_file())
        self.assertEqual(self.app.get_portfolios(), [])
        self.assertIsNone(pipeline.portfolio_id)
        self.assertIsNone(pipeline.thread)
        self.assertFalse(pipeline.allow_network)
        with closing(sqlite3.connect(self.quote_path)) as conn:
            self.assertEqual(conn.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue({'quote', 'provider', 'cash_observed_close', 'cash_sales_reference'}.issubset(tables))

    def test_install_is_idempotent_and_preserves_existing_quote_control(self):
        pipeline = self.install()
        with pipeline.book.connect(True) as conn:
            conn.execute("INSERT INTO meta(key,value) VALUES('synthetic-marker','preserve-me')")
            conn.execute("UPDATE provider SET enabled=0,next_allowed=2000000000,last_error='Synthetic saved cooldown' WHERE id='steam'")
        again = self.install()
        self.assertIs(again, pipeline)
        with pipeline.book.connect() as conn:
            self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='synthetic-marker'").fetchone()[0], 'preserve-me')
            row = conn.execute("SELECT enabled,next_allowed,last_error FROM provider WHERE id='steam'").fetchone()
            self.assertEqual(tuple(row), (0, 2000000000, 'Synthetic saved cooldown'))

    def test_first_install_adopts_existing_ledger_without_resetting_cooldown(self):
        import price_features
        import price_pipeline
        import quote_book
        quote_book.initialize(self.quote_path, price_pipeline.PROVIDERS)
        book = quote_book.Book(self.quote_path)
        sales = next(p for p in price_pipeline.PROVIDERS if p['id'] == 'skinport_sales')
        price_features.initialize(book, sales)
        with book.connect(True) as conn:
            conn.execute("INSERT INTO meta(key,value) VALUES('synthetic-marker','pre-existing')")
            conn.execute("UPDATE provider SET enabled=0,next_allowed=2000000001,last_error='Synthetic prior pause' WHERE id='steam'")
        pipeline = self.install()
        with pipeline.book.connect() as conn:
            self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='synthetic-marker'").fetchone()[0], 'pre-existing')
            self.assertEqual(tuple(conn.execute("SELECT enabled,next_allowed,last_error FROM provider WHERE id='steam'").fetchone()),
                             (0, 2000000001, 'Synthetic prior pause'))

    def test_current_install_preserves_legacy_research_function_identity(self):
        self.install()
        for name, original in self.original.items():
            with self.subTest(function=name):
                self.assertIs(getattr(self.app, name), original)
        self.assertIn('/api/current-analysis', self.app.ANON_GET_PATHS)

    def test_legacy_routes_keep_their_original_scopes_and_parameter_shapes(self):
        self.install()
        cases = [
            ('/api/analysis?days=30', 'market_universe_analysis', 'history_days', '30'),
            ('/api/market-universe?days=7', 'market_universe_analysis', 'history_days', '7'),
            ('/api/taxonomy-analysis?scope=market&days=30', 'taxonomy_analysis', 'scope', 'market'),
            ('/api/group-history?scope=market&dimension=collection&group=Synthetic', 'aggregate_group_history', 'scope', 'market'),
            ('/api/group-item-history?scope=market&dimension=collection&group=Synthetic', 'group_item_history', 'scope', 'market'),
            ('/api/inventory?portfolio=synthetic-portfolio', 'get_inventory_dataset', 'portfolio_id', 'synthetic-portfolio'),
        ]
        for route, function, parameter, expected in cases:
            with self.subTest(route=route):
                handler = object.__new__(self.app.Handler)
                handler.path = route
                handler.client_address = ('127.0.0.1', 0)
                handler.require_auth = lambda *a: True
                handler.current_user = lambda: 'synthetic-user'
                handler.send_json = mock.Mock()
                marker = {'ok': True, 'synthetic-contract': function}
                with mock.patch.object(self.app, function, return_value=marker) as called:
                    handler._do_GET_with_capacity_response()
                self.assertEqual(called.call_args.kwargs[parameter], expected)
                handler.send_json.assert_called_once_with(marker)

    def test_portable_shared_reader_is_query_only(self):
        self.install()
        with closing(self.app.steam_ask_db(shared_read=True)) as conn:
            self.assertEqual(conn.execute('PRAGMA query_only').fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("INSERT INTO settings(key,value) VALUES('synthetic-read-probe','forbidden')")

    def test_optional_lean_identity_binding_is_preserved_without_changing_portfolios(self):
        self.seed_portfolio()
        before = self.app.get_portfolios()
        with self.app.db() as conn:
            conn.execute('CREATE TABLE lean_held_identity(name TEXT PRIMARY KEY,market_hash_name TEXT,identity_issue TEXT)')
            conn.execute('INSERT INTO lean_held_identity VALUES(?,?,?)',
                         ('Synthetic item 0001', 'Synthetic exact listing 0001', None))
        self.install()
        scope, rows, display = self.app.cash_held_rows('synthetic-portfolio')
        self.assertEqual(scope, 'portfolio:synthetic-portfolio')
        self.assertEqual(rows[0]['marketHashName'], 'Synthetic exact listing 0001')
        self.assertIsNone(rows[0]['identityIssue'])
        self.assertEqual(display, [['Synthetic item 0001', 2.0]])
        self.assertEqual(self.app.get_portfolios(), before)

    def prepare_shared_steam(self):
        pipeline = self.install()
        now = 1800000000
        pipeline.book.clock = lambda: now
        pipeline.clock = lambda: now
        pipeline.allow_network = True  # Only dummy opener/transport objects below.
        self.stack.enter_context(mock.patch('restoration_runtime.shutil.disk_usage',
                                            return_value=SimpleNamespace(free=20 * 1024 ** 3)))
        with pipeline.book.connect(True) as conn:
            conn.execute("UPDATE provider SET enabled=1,next_allowed=0,lease=NULL,lease_until=0,failures=0 WHERE id='steam'")
            subject = conn.execute('INSERT INTO subject(market_hash_name) VALUES(?)',
                                   ('Synthetic historical wallet item',)).lastrowid
            capture = conn.execute('''INSERT INTO capture(provider,received_us,body_sha,row_count,request_url,clock_semantics)
                VALUES('steam',?,?,1,?,'synthetic_old_wallet_capture')''',
                ((now - 900) * 1000000, 'a' * 64, 'https://steamcommunity.com/market/priceoverview/')).lastrowid
            conn.execute('''INSERT INTO quote(subject,provider,currency,kind,price_mills,quantity,state,observed_at,capture,active)
                VALUES(?,'steam','USD','wallet_ask',25000,1,'priced',?,?,1)''',
                (subject, now - 900, capture))
            conn.execute("UPDATE provider SET last_capture=?,last_received_us=?,last_status=200 WHERE id='steam'",
                         (capture, (now - 900) * 1000000))
        request = urllib.request.Request('https://steamcommunity.com/market/pricehistory/?appid=730&market_hash_name=Synthetic%20item')
        return pipeline, now, request

    @staticmethod
    def steam_receipts(book):
        with book.connect() as conn:
            return {
                'revision': book.revision(conn),
                'capture': tuple(conn.execute("SELECT last_capture,last_received_us FROM provider WHERE id='steam'").fetchone()),
                'captures': [tuple(r) for r in conn.execute('SELECT * FROM capture ORDER BY id')],
                'quotes': [tuple(r) for r in conn.execute('SELECT * FROM quote ORDER BY subject,provider,currency,kind')],
            }

    def test_shared_steam_history_success_does_not_freshen_wallet_quotes(self):
        pipeline, now, request = self.prepare_shared_steam()
        before = self.steam_receipts(pipeline.book)

        class DummyResponse(io.BytesIO):
            status = 200
            headers = {}

            def geturl(self):
                return request.full_url

        opener = mock.Mock()
        opener.open.return_value = DummyResponse(b'Synthetic history response')
        with self.app.steam_network_open(opener, request, 60) as response:
            self.assertEqual(response.read(), b'Synthetic history response')
        self.assertEqual(self.steam_receipts(pipeline.book), before)
        provider = next(p for p in pipeline.book.providers() if p['id'] == 'steam')
        self.assertIsNone(provider['lease'])
        self.assertEqual(provider['last_status'], 200)
        self.assertEqual(provider['failures'], 0)
        self.assertGreaterEqual(provider['next_allowed'], now + 60)
        opener.open.assert_called_once_with(request, timeout=20)
        with self.assertRaises(self.app.SteamGovernorDeferred):
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Second history request must remain paced')
        # Wallet asks share that same admission, so no second transport can run.
        with mock.patch('price_transport.fetch', side_effect=AssertionError('Paced ask must not fetch')):
            self.assertEqual(pipeline._fetch('steam', 'Synthetic item')['action'], 'deferred')
        self.assertEqual(opener.open.call_count, 1)

    def test_shared_steam_429_preserves_retry_after_and_previous_quotes(self):
        pipeline, now, request = self.prepare_shared_steam()
        before = self.steam_receipts(pipeline.book)
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.HTTPError(request.full_url, 429,
            'Synthetic rate limit', {'Retry-After': '600'}, None)
        with self.assertRaises(urllib.error.HTTPError):
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Synthetic 429 cannot yield a response')
        provider = next(p for p in pipeline.book.providers() if p['id'] == 'steam')
        self.assertIsNone(provider['lease'])
        self.assertEqual(provider['last_status'], 429)
        self.assertEqual(provider['failures'], 1)
        self.assertGreaterEqual(provider['next_allowed'], now + 600)
        self.assertEqual(self.steam_receipts(pipeline.book), before)
        with self.assertRaises(self.app.SteamGovernorDeferred) as deferred:
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Retry-After must block another request')
        self.assertGreaterEqual(deferred.exception.state['retryAt'], now + 600)
        self.assertEqual(opener.open.call_count, 1)

    def test_shared_steam_offline_and_operator_pause_deny_before_transport(self):
        pipeline, now, request = self.prepare_shared_steam()
        opener = mock.Mock()
        pipeline.allow_network = False
        with self.assertRaises(self.app.SteamGovernorDeferred) as offline:
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Offline history cannot enter transport')
        self.assertEqual(offline.exception.state['reason'], 'offline')
        pipeline.allow_network = True
        pipeline.book.enable('steam', False)
        with self.assertRaises(self.app.SteamGovernorDeferred) as paused:
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Paused history cannot enter transport')
        self.assertTrue(paused.exception.state['operatorPaused'])
        opener.open.assert_not_called()

    def seed_paged_market(self):
        names = ['Synthetic item %04d' % i for i in range(510)]
        with self.app.db() as conn:
            conn.executemany('''INSERT INTO item_catalog(name,market_hash_name,kind,item_type,collection,rarity,updated_at)
                VALUES(?,?,'skins','Synthetic type','Synthetic collection','Synthetic grade',1700000000)''',
                [(name, name) for name in names])
            conn.executemany('''INSERT INTO market_item_lookup(
                name,window_days,market_hash_name,search_text,latest_price,source,latest_at,
                kind,item_type,collection,rarity,groups_json,provider_prices_json)
                VALUES(?,90,?,?,?,?,?,'skins','Synthetic type','Synthetic collection','Synthetic grade','[]','{}')''',
                [(name, name, name.lower(), .5 if i == 509 else 10 + i,
                  'synthetic_alpha' if i == 509 else 'synthetic_zeta', 1700000000 + i)
                 for i, name in enumerate(names)])
            conn.execute('''INSERT INTO item_market_metrics(name,provider,window_days,updated_at,raw)
                VALUES(?,'market',90,1700000000,'{}')''', (names[0],))
        return names

    @staticmethod
    def assert_page_metadata(test, response, total, offset, returned, next_offset):
        test.assertEqual(response['totalItems'], total)
        test.assertEqual(response['offset'], offset)
        test.assertEqual(response['returnedItems'], returned)
        test.assertEqual(response['hasMore'], next_offset is not None)
        test.assertEqual(response['nextOffset'], next_offset)

    def test_research_price_sort_and_filter_precede_offset(self):
        self.install()
        names = self.seed_paged_market()
        first = self.app.market_universe_item_slice(history_days=90, limit=500,
            sort_key='latestPrice', sort_dir=1, projection='', offset=0)
        second = self.app.market_universe_item_slice(history_days=90, limit=500,
            sort_key='latestPrice', sort_dir=1, projection='', offset=500)
        expected = [names[509]] + names[:509]
        self.assertEqual([r['name'] for r in first['items']], expected[:500])
        self.assertEqual([r['name'] for r in second['items']], expected[500:])
        self.assert_page_metadata(self, first, 510, 0, 500, 500)
        self.assert_page_metadata(self, second, 510, 500, 10, None)
        filtered = self.app.market_universe_item_slice(history_days=90, limit=25,
            min_price=100, max_price=200, sort_key='latestPrice', sort_dir=1, projection='', offset=25)
        self.assertEqual([r['name'] for r in filtered['items']], names[115:140])
        self.assert_page_metadata(self, filtered, 101, 25, 25, 50)
        cheapest = self.app.market_universe_item_slice(history_days=90, limit=25,
            max_price=1, sort_key='latestPrice', sort_dir=1, projection='', offset=0)
        self.assertEqual([r['name'] for r in cheapest['items']], [names[509]])
        self.assert_page_metadata(self, cheapest, 1, 0, 1, None)

    def test_research_source_and_timestamp_sort_globally_before_offset(self):
        self.install()
        names = self.seed_paged_market()
        for key, direction, expected in (
            ('source', 1, [names[509]] + names[:509]),
            ('latestAt', -1, list(reversed(names))),
        ):
            with self.subTest(sort=key):
                result = self.app.market_universe_item_slice(history_days=90, limit=25,
                    sort_key=key, sort_dir=direction, projection='', offset=25)
                self.assertEqual([r['name'] for r in result['items']], expected[25:50])
                self.assert_page_metadata(self, result, 510, 25, 25, 50)

    @staticmethod
    def synthetic_market_prices(names):
        return {'items': [{
            'marketHashName': name,
            'canonicalPriceUsd': .5 if name.endswith('0509') else 10 + int(name.rsplit(' ', 1)[1]),
            'selectedProvider': 'synthetic_alpha' if name.endswith('0509') else 'synthetic_zeta',
            'capturedAt': 1700000000 + int(name.rsplit(' ', 1)[1]),
            'state': 'priced', 'freshnessState': 'fresh',
        } for name in names]}

    def test_current_cash_global_price_filter_and_second_page(self):
        pipeline = self.install()
        names = self.seed_paged_market()
        with mock.patch.object(pipeline.book, 'prices', side_effect=self.synthetic_market_prices) as prices:
            first = self.app.current_market_item_slice(limit=500, sort_key='latestPrice', sort_dir=1, offset=0)
            self.assertEqual(len(prices.call_args.args[0]), 510)
            second = self.app.current_market_item_slice(limit=500, sort_key='latestPrice', sort_dir=1, offset=500)
            cheapest = self.app.current_market_item_slice(limit=25, max_price=1,
                sort_key='latestPrice', sort_dir=1, offset=0)
        expected = [names[509]] + names[:509]
        self.assertEqual([r['name'] for r in first['items']], expected[:500])
        self.assertEqual([r['name'] for r in second['items']], expected[500:])
        self.assert_page_metadata(self, first, 510, 0, 500, 500)
        self.assert_page_metadata(self, second, 510, 500, 10, None)
        self.assertEqual([r['name'] for r in cheapest['items']], [names[509]])
        self.assert_page_metadata(self, cheapest, 1, 0, 1, None)

    def test_current_cash_source_and_timestamp_sort_before_offset(self):
        pipeline = self.install()
        names = self.seed_paged_market()
        with mock.patch.object(pipeline.book, 'prices', side_effect=self.synthetic_market_prices):
            for key, direction, expected in (
                ('source', 1, [names[509]] + names[:509]),
                ('latestAt', -1, list(reversed(names))),
            ):
                with self.subTest(sort=key):
                    first = self.app.current_market_item_slice(limit=25, sort_key=key, sort_dir=direction, offset=0)
                    second = self.app.current_market_item_slice(limit=25, sort_key=key, sort_dir=direction, offset=25)
                    self.assertEqual([r['name'] for r in first['items']], expected[:25])
                    self.assertEqual([r['name'] for r in second['items']], expected[25:50])
                    self.assert_page_metadata(self, first, 510, 0, 25, 25)
                    self.assert_page_metadata(self, second, 510, 25, 25, 50)

    def test_research_and_current_api_forward_offset_and_keep_page_metadata(self):
        pipeline = self.install()
        names = self.seed_paged_market()
        with mock.patch.object(pipeline.book, 'prices', side_effect=self.synthetic_market_prices):
            for path in ('/api/market-items', '/api/current-market-items'):
                with self.subTest(path=path):
                    handler = object.__new__(self.app.Handler)
                    handler.path = path + '?days=90&limit=25&sort=latestPrice&dir=1&offset=500'
                    handler.client_address = ('127.0.0.1', 0)
                    handler.require_auth = lambda *a: True
                    handler.current_user = lambda: 'synthetic-user'
                    handler.send_json = mock.Mock()
                    handler._do_GET_with_capacity_response()
                    handler.send_json.assert_called_once()
                    result = handler.send_json.call_args.args[0]
                    self.assertEqual([r['name'] for r in result['items']], names[499:509])
                    self.assert_page_metadata(self, result, 510, 500, 10, None)

    def test_report_formats_unavailable_values_without_turning_them_into_zero(self):
        for value in (None, float('nan'), float('inf'), float('-inf'), 'invalid synthetic value', {}):
            with self.subTest(value=repr(value)):
                self.assertEqual(self.app.fmt_money(value), 'Unavailable')
                self.assertEqual(self.app.fmt_pct(value), 'Unavailable')
        self.assertEqual(self.app.fmt_money(0), '$0.00')
        self.assertEqual(self.app.fmt_pct(0), '0.00%')

    def test_health_reports_code_build_identity_with_invented_revision(self):
        self.install()
        handler = object.__new__(self.app.Handler)
        handler.path = '/api/health'
        handler.client_address = ('127.0.0.1', 0)
        handler.require_auth = lambda *a: True
        handler.current_user = lambda: 'synthetic-user'
        handler.send_json = mock.Mock()
        handler._do_GET_with_capacity_response()
        payload = handler.send_json.call_args.args[0]
        self.assertTrue(payload['ok'])
        self.assertEqual(payload['version'], self.app.APP_VERSION)
        identity = payload['build']
        self.assertEqual(identity['version'], self.app.APP_VERSION)
        self.assertEqual(identity['sourceRevision'], 'a' * 40)
        self.assertEqual(identity['revisionOrigin'], 'environment')
        for key in ('runtimeSha256', 'frontendSha256', 'serverSha256'):
            self.assertRegex(identity[key], r'^[0-9a-f]{64}$')
        self.assertGreater(identity['runtimeFileCount'], 20)

    def test_recovery_provenance_preserves_original_clocks_and_reads_only(self):
        self.install()
        self.seed_paged_market()
        receipt = {
            'windows': [{'days': 90, 'rows': 510}],
            'asOf': {'metricsHistoryLastAt': [500, 1000]},
            'appliedAt': 2000,
            'sourceFingerprint': 'synthetic-provenance-fingerprint',
        }
        statements = []
        with mock.patch('research_recovery.recovery_status', return_value=receipt):
            with closing(self.app.catalog_read_db()) as conn:
                before_settings = [tuple(r) for r in conn.execute('SELECT * FROM settings ORDER BY key')]
                before_lookup = [tuple(r) for r in conn.execute('SELECT * FROM market_item_lookup ORDER BY window_days,name')]
                before_history = conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0]
                conn.set_trace_callback(statements.append)
                recovered = self.app.recovered_research_status(conn, 90)
                state = self.app.market_cache_state(conn, 90)
                status = self.app.market_item_lookup_status(conn, 90)
                self.assertEqual(recovered['sourceAsOf'], 1000)
                self.assertEqual(recovered['recoveredAt'], 2000)
                self.assertEqual(recovered['rawSteamChartRowsImported'], 0)
                self.assertFalse(recovered['rawSteamHistoryRestored'])
                self.assertFalse(recovered['currentQuotes'])
                self.assertEqual(state['cacheState'], 'recovered_historical')
                self.assertEqual(state['freshness']['currentness'], 'recovered_historical_snapshot')
                self.assertEqual(state['freshness']['sourceAsOf'], 1000)
                self.assertTrue(state['rebuildRequired'])
                self.assertEqual(status['cacheState'], 'recovered_historical')
                self.assertEqual(status['latestObservationAt'], 1000)
                self.assertEqual(status['dataset']['asOf'], 1000)
                self.assertEqual(status['catalogItems'], 510)
                self.assertIsNone(status['quotedItems'])
                self.assertIsNone(status['coveragePct'])
                self.assertEqual(status['recovery']['rawSteamChartRowsImported'], 0)
                self.assertEqual(conn.total_changes, 0)
                self.assertEqual([tuple(r) for r in conn.execute('SELECT * FROM settings ORDER BY key')], before_settings)
                self.assertEqual([tuple(r) for r in conn.execute('SELECT * FROM market_item_lookup ORDER BY window_days,name')], before_lookup)
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0], before_history)
        self.assertFalse(any(sql.lstrip().upper().startswith(('INSERT ', 'UPDATE ', 'DELETE ', 'REPLACE ', 'CREATE ', 'ALTER ', 'DROP ')) for sql in statements))

    def test_capabilities_distinguish_implemented_routes_from_missing_data_and_admission(self):
        pipeline = self.install()
        statements = []
        original_reader = self.app.steam_ask_db

        def traced_reader(*args, **kwargs):
            conn = original_reader(*args, **kwargs)
            conn.set_trace_callback(statements.append)
            return conn

        def saved_control():
            with pipeline.book.connect() as conn:
                return (pipeline.book.revision(conn),
                        tuple(conn.execute("SELECT * FROM provider WHERE id='steam'").fetchone()))

        with closing(original_reader()) as conn:
            original_settings = [tuple(r) for r in conn.execute('SELECT * FROM settings ORDER BY key')]
        before = saved_control()
        with mock.patch.object(self.app, 'steam_ask_db', side_effect=traced_reader), \
                mock.patch.object(pipeline.book, 'claim', side_effect=AssertionError('Capability reads cannot claim requests')) as claim:
            result = self.app.research_capabilities()
            self.assertTrue(result['researchRoutesImplemented'])
            self.assertFalse(result['rawSteamHistoryAvailable'])
            self.assertFalse(result['savedResearchMetricsAvailable'])
            self.assertFalse(result['savedResearchLookupAvailable'])
            self.assertFalse(result['historyAcquisition']['networkEnabled'])
            self.assertFalse(result['historyAcquisition']['admissionReady'])
            self.assertEqual(saved_control(), before)

            # This explicit fixture-only operator change is the only write;
            # subsequent capability reads must preserve it without reservations.
            pipeline.book.enable('steam', False)
            paused_control = saved_control()
            paused = self.app.research_capabilities()
            self.assertTrue(paused['historyAcquisition']['operatorPaused'])
            self.assertFalse(paused['historyAcquisition']['providerEnabled'])
            self.assertFalse(paused['historyAcquisition']['admissionReady'])
            self.assertEqual(saved_control(), paused_control)
            claim.assert_not_called()

            handler = object.__new__(self.app.Handler)
            handler.path = '/api/research-capabilities'
            handler.require_auth = mock.Mock(return_value=False)
            handler.send_json = mock.Mock()
            handler._do_GET_with_capacity_response()
            handler.require_auth.assert_called_once_with('/api/research-capabilities', 'GET')
            handler.send_json.assert_not_called()
            handler.require_auth.return_value = True
            handler._do_GET_with_capacity_response()
            self.assertTrue(handler.send_json.call_args.args[0]['historyAcquisition']['operatorPaused'])
        with closing(original_reader()) as conn:
            self.assertEqual([tuple(r) for r in conn.execute('SELECT * FROM settings ORDER BY key')], original_settings)
        self.assertFalse(any(sql.lstrip().upper().startswith(('INSERT ', 'UPDATE ', 'DELETE ', 'REPLACE ', 'CREATE ', 'ALTER ', 'DROP ')) for sql in statements))

    def run_synthetic_data_task_post(self, acquisition, options=None, acquisition_expected=True):
        class InlineThread:
            def __init__(self, target, args=(), kwargs=None, **ignored):
                self.target, self.args, self.kwargs = target, args, kwargs or {}
                self.alive = False

            def start(self):
                self.alive = True
                try:
                    self.target(*self.args, **self.kwargs)
                finally:
                    self.alive = False

            def is_alive(self):
                return self.alive

        handler = object.__new__(self.app.Handler)
        handler.path = '/api/data-tasks'
        handler.client_address = ('127.0.0.1', 0)
        handler.require_auth = mock.Mock(return_value=True)
        handler.read_body_json = lambda: options or {'mode': 'provider_snapshots', 'cycles': 1,
                                                    'historyLimit': 0, 'sleepSeconds': 0}
        handler.send_json = mock.Mock()
        with mock.patch.object(self.app.threading, 'Thread', InlineThread), \
                mock.patch.object(self.app, 'market_universe_bulk_sync', side_effect=acquisition) as acquired, \
                mock.patch.object(self.app, 'market_universe_status', return_value={
                    'coveragePct': 100, 'observedItems': 1, 'catalogItems': 1,
                    'stores': {'steamHistoryItems': 1}, 'gaps': []}):
            handler.do_POST()
        if acquisition_expected:
            acquired.assert_called_once()
        else:
            acquired.assert_not_called()
        handler.send_json.assert_called_once()
        response = handler.send_json.call_args.args[0]
        return self.app.DATA_TASKS[response['task']['id']]

    def test_explicit_history_finalizer_materializes_all_periods_and_gets_remain_readonly(self):
        self.install()
        name, now = 'Synthetic all-period research item', int(self.app.time.time())
        with self.app.db() as conn:
            conn.execute('''INSERT INTO item_catalog(name,market_hash_name,kind,item_type,collection,updated_at)
                VALUES(?,?,'skins','Synthetic type','Synthetic collection',?)''', (name, name, now))
            self.app.upsert_market_item_price(conn, {'name': name, 'provider': 'steam_snapshot',
                'price': 20, 'solidPrice': 20, 'updated_at': now, 'source': 'synthetic:dated'},
                provider='steam_snapshot', source='synthetic:dated')
            conn.executemany('''INSERT INTO price_history(name,provider,point_time,point_ts,price,volume,updated_at)
                VALUES(?,'steam',?,?,?,?,?)''', [(name, 'synthetic-day-'+str(day), now-day*86400,
                20-day/50, 1, now) for day in range(401)])
        self.app.rebuild_metrics_for_names([name])
        expected = self.app.VOLATILITY_WINDOWS
        with closing(self.app.catalog_read_db()) as conn:
            rows = conn.execute("SELECT window_days,prior_price FROM item_market_metrics WHERE name=? AND provider='market'", (name,)).fetchall()
            self.assertEqual({r['window_days'] for r in rows}, expected)
            self.assertEqual({r[0] for r in conn.execute('SELECT window_days FROM market_item_lookup WHERE name=?', (name,))}, expected)
            priors = {r['window_days']: r['prior_price'] for r in rows}
            self.assertGreater(priors[1], priors[180])
            self.assertGreater(priors[180], priors[365])
            self.assertGreater(priors[365], priors[0])
        with mock.patch.object(self.app, 'db', side_effect=AssertionError('GET cannot open a writer')), \
                mock.patch.object(self.app, 'rebuild_market_metric_windows', side_effect=AssertionError('GET cannot rebuild metrics')), \
                mock.patch.object(self.app, 'rebuild_market_item_lookup', side_effect=AssertionError('GET cannot rebuild lookup')):
            for window in sorted(expected):
                with self.subTest(window=window):
                    handler = object.__new__(self.app.Handler)
                    handler.path = '/api/market-items?days='+str(window)+'&limit=25'
                    handler.client_address = ('127.0.0.1', 0)
                    handler.require_auth = lambda *args: True
                    handler.current_user = lambda: 'synthetic-user'
                    handler.send_json = mock.Mock()
                    handler._do_GET_with_capacity_response()
                    response = handler.send_json.call_args.args[0]
                    self.assertEqual(response['metricWindowDays'], window)
                    self.assertTrue(response['metricWindowExact'])
                    self.assertEqual([row['name'] for row in response['items']], [name])

    def test_empty_explicit_build_preserves_recovered_metrics_and_lookup(self):
        self.install()
        self.seed_paged_market()
        tables = ('item_market_metrics', 'market_item_lookup', 'settings')
        def snapshot():
            with closing(self.app.catalog_read_db()) as conn:
                return {table: [tuple(row) for row in conn.execute('SELECT * FROM '+table+' ORDER BY rowid')] for table in tables}
        before = snapshot()
        with mock.patch('research_recovery.recovery_status', return_value={'windows': [{'days': 90, 'rows': 510}]}):
            with self.app.db() as conn:
                self.app.rebuild_market_metric_windows(conn, self.app.RESEARCH_METRIC_WINDOWS)
                for window in self.app.RESEARCH_METRIC_WINDOWS:
                    self.app.rebuild_market_item_lookup(conn, window)
                self.assertEqual(conn.total_changes, 0)
        self.assertEqual(snapshot(), before)

    def test_explicit_data_task_calls_real_metric_and_lookup_rebuilds(self):
        self.install()
        rebuilt = {}

        def acquire(**kwargs):
            self.assertEqual(kwargs['provider'], 'csgotrader')
            name = 'Synthetic rebuild item'
            now = int(self.app.time.time())
            with self.app.db() as conn:
                conn.execute('''INSERT INTO item_catalog(name,market_hash_name,kind,item_type,collection,updated_at)
                    VALUES(?,?,'skins','Synthetic type','Synthetic collection',?)''', (name, name, now))
                conn.execute('''INSERT INTO market_item_prices(name,provider,price,solid_price,source,observed_at,updated_at)
                    VALUES(?,'steam_snapshot',12,12,'synthetic_provider_capture',?,?)''', (name, now, now))
                conn.executemany('''INSERT INTO price_history(name,provider,point_time,point_ts,price,volume,updated_at)
                    VALUES(?,'steam',?,?,?,?,?)''',
                    [(name, 'synthetic-day-' + str(day), now - day * 86400,
                      12 - day / 100, 1, now) for day in range(91)])
                rebuilt['metrics'] = self.app.rebuild_market_metric_windows(conn, (7, 30, 90), as_of=now)
                rebuilt['lookups'] = {window: self.app.rebuild_market_item_lookup(conn, window)
                                      for window in (7, 30, 90)}
            return {'ok': True, 'itemsStored': 1, 'newObservationRows': 1,
                    'rebuiltMetrics': sum(rebuilt['metrics'].values()),
                    'rebuiltLookups': sum(rebuilt['lookups'].values())}

        task = self.run_synthetic_data_task_post(acquire)
        self.assertEqual(task['status'], 'complete', task)
        self.assertEqual(rebuilt['metrics'], {7: 1, 30: 1, 90: 1})
        self.assertEqual(rebuilt['lookups'], {7: 1, 30: 1, 90: 1})
        with closing(self.app.catalog_read_db()) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM item_market_metrics WHERE provider='market'").fetchone()[0], 3)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM market_item_lookup').fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM price_history WHERE provider='steam'").fetchone()[0], 91)

    def test_portfolio_history_job_keeps_full_limit_and_never_warms_market(self):
        self.install()
        candidates = [{'name': 'Synthetic selected '+str(i)} for i in range(80)]
        with mock.patch.object(self.app, 'latest_portfolio_history_candidates', return_value=candidates) as selected, \
                mock.patch.object(self.app, 'market_history_candidates', side_effect=AssertionError('Unrelated market work forbidden')), \
                mock.patch.object(self.app, 'refresh_steam_history_for_chart_items', return_value={'attempted':80,'refreshed':80,'failed':0,'errors':[]}) as refresh:
            task = self.run_synthetic_data_task_post(lambda **kwargs: self.fail('History job cannot pull providers'),
                options={'mode':'steam_history','historyScope':'portfolio','portfolio':'synthetic-selected',
                         'historyLimit':80,'cycles':1}, acquisition_expected=False)
        self.assertEqual(task['status'], 'complete', task)
        self.assertEqual(selected.call_args.kwargs['portfolio_id'], 'synthetic-selected')
        self.assertEqual(refresh.call_args.kwargs['limit'], 80)
        self.assertEqual(refresh.call_args.args[0], candidates)
        self.assertEqual(refresh.call_count, 1)

    def test_all_selected_history_is_batched_without_market_or_forced_refetch(self):
        self.install()
        candidates = [{'name':'Synthetic selected '+str(i)} for i in range(600)]
        calls = []
        def refresh(rows, **kwargs):
            calls.append((list(rows), kwargs))
            return {'attempted':len(rows),'refreshed':len(rows),'failed':0,'errors':[]}
        with mock.patch.object(self.app, 'latest_portfolio_history_candidates', return_value=candidates) as selected, \
                mock.patch.object(self.app, 'market_history_candidates', side_effect=AssertionError('Unrelated market work forbidden')), \
                mock.patch.object(self.app, 'refresh_steam_history_for_chart_items', side_effect=refresh):
            task = self.run_synthetic_data_task_post(lambda **kwargs: self.fail('History job cannot pull providers'),
                options={'mode':'steam_history','historyScope':'portfolio','historyAll':True,
                         'portfolio':'synthetic-selected','historyLimit':250,'cycles':1,'force':True}, acquisition_expected=False)
        self.assertEqual(task['status'], 'complete', task)
        self.assertTrue(selected.call_args.kwargs['all_items'])
        self.assertEqual([len(rows) for rows, kwargs in calls], [250,250,100])
        self.assertEqual([row for rows, kwargs in calls for row in rows], candidates)
        self.assertTrue(all(kwargs['force'] is False for rows, kwargs in calls))
        self.assertEqual({event.get('payload', {}).get('selectedItems') for event in task['events']
                          if event.get('stage') == 'portfolio_batch'}, {600})

    def test_oversized_selected_history_job_errors_instead_of_truncating(self):
        self.install()
        self.app.replace_portfolios([{'id':'synthetic-large','label':'Synthetic large',
            'importedAt':'2026-01-01T00:00:00Z',
            'data':{'items':[['Synthetic large '+str(i),1,None,None,None,None,'Synthetic','Synthetic'] for i in range(10001)]}}])
        with self.assertRaisesRegex(ValueError, 'no other portfolio'):
            self.app.latest_portfolio_history_candidates(80, portfolio_id='synthetic-missing')
        with mock.patch.object(self.app, 'refresh_steam_history_for_chart_items', side_effect=AssertionError('Truncated work forbidden')):
            task = self.run_synthetic_data_task_post(lambda **kwargs: self.fail('History job cannot pull providers'),
                options={'mode':'steam_history','historyScope':'portfolio','historyAll':True,
                         'portfolio':'synthetic-large','historyLimit':250,'cycles':1}, acquisition_expected=False)
        self.assertEqual(task['status'], 'error', task)
        self.assertIn('10,000-row', task.get('error',''))
        self.assertFalse(any(event.get('stage') == 'complete' for event in task['events']))
        with closing(self.app.catalog_read_db()) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM portfolio_items').fetchone()[0],10001)

    def test_explicit_data_task_provider_failure_cannot_be_complete(self):
        self.install()
        task = self.run_synthetic_data_task_post(lambda **kwargs: {
            'ok': False, 'itemsStored': 0, 'error': 'Synthetic provider unavailable'})
        self.assertEqual(task['status'], 'error', task)
        self.assertIn('Synthetic provider unavailable', task.get('error', ''))
        self.assertFalse(any(e.get('stage') == 'complete' for e in task.get('events', [])))

    def test_explicit_data_task_unpublished_derived_result_stays_pending(self):
        self.install()
        task = self.run_synthetic_data_task_post(lambda **kwargs: {
            'ok': True, 'itemsStored': 1,
            'status': {'commitState': 'source_committed_derived_pending',
                       'derivedPublication': {'state': 'source_committed_derived_pending', 'complete': False}}})
        self.assertEqual(task['status'], 'pending', task)
        self.assertEqual(task['stage'], 'derived_pending')
        self.assertEqual(task['result']['status']['commitState'], 'source_committed_derived_pending')
        self.assertFalse(any(e.get('stage') == 'complete' for e in task.get('events', [])))

    def test_history_canonical_redirect_is_saved_then_followed_only_after_pacing(self):
        pipeline, now, ignored = self.prepare_shared_steam()
        source = 'https://steamcommunity.com/market/listings/730/Synthetic%20item'
        canonical = 'https://steamcommunity.com/market/listings/730/123456/Synthetic%20item?l=english&currency=1'
        request = urllib.request.Request(source)
        before = self.steam_receipts(pipeline.book)
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.HTTPError(source, 302, 'Synthetic canonical migration', {'Location': canonical}, None)
        with self.assertRaises(self.app.SteamGovernorDeferred) as redirected:
            with self.app.steam_network_open(opener, request, 5):
                self.fail('A redirect must not be followed immediately')
        self.assertEqual(redirected.exception.state['reason'], 'canonical_history_redirect')
        self.assertTrue(redirected.exception.state['networkAttempted'])
        self.assertEqual(self.runtime.resolved_history_url(pipeline.book, source), canonical)
        self.assertEqual(self.steam_receipts(pipeline.book), before)
        with self.assertRaises(self.app.SteamGovernorDeferred):
            with self.app.steam_network_open(opener, request, 5):
                self.fail('Canonical request must respect original admission')
        self.assertEqual(opener.open.call_count, 1)

        due = redirected.exception.state['retryAt']
        pipeline.book.clock = lambda: due
        pipeline.clock = lambda: due

        class CanonicalResponse(io.BytesIO):
            status = 200
            headers = {}

            def geturl(self):
                return canonical

        opener.open.side_effect = None
        opener.open.return_value = CanonicalResponse(b'Synthetic canonical history')
        with self.app.steam_network_open(opener, request, 5) as response:
            self.assertEqual(response.read(), b'Synthetic canonical history')
        self.assertEqual(opener.open.call_count, 2)
        self.assertEqual(opener.open.call_args.args[0].full_url, canonical)
        self.assertEqual(self.steam_receipts(pipeline.book), before)

    def test_history_redirect_rejects_unapproved_destinations_and_chains(self):
        pipeline, now, ignored = self.prepare_shared_steam()
        source = 'https://steamcommunity.com/market/listings/730/Synthetic%20item'
        invalid = (
            'https://example.invalid/market/listings/730/Synthetic',
            'https://steamcommunity.com/login',
            source + '?sessionid=synthetic',
            source + '?api_key=synthetic',
            'https://synthetic:password@steamcommunity.com/market/listings/730/Synthetic',
            source + '#fragment', source,
        )
        for target in invalid:
            with self.subTest(target=target):
                with self.assertRaises(ValueError):
                    self.runtime.remember_history_redirect(pipeline.book, source, target)
        with pipeline.book.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM meta WHERE key LIKE 'steam_history_redirect:v1:%'").fetchone()[0], 0)
        canonical = source + '/canonical'
        self.runtime.remember_history_redirect(pipeline.book, source, canonical)
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.HTTPError(canonical, 302, 'Synthetic second hop', {'Location': canonical + '/again'}, None)
        with self.assertRaisesRegex(ValueError, 'one-hop'):
            with self.app.steam_network_open(opener, urllib.request.Request(source), 5):
                self.fail('A mapped redirect may not follow another hop')
        self.assertEqual(opener.open.call_count, 1)
        self.assertEqual(self.runtime.resolved_history_url(pipeline.book, source), canonical)
        with pipeline.book.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM meta WHERE key LIKE 'steam_history_redirect:v1:%'").fetchone()[0], 1)

    def test_saved_history_redirect_cannot_bypass_pause_or_backoff(self):
        pipeline, now, ignored = self.prepare_shared_steam()
        source = 'https://steamcommunity.com/market/listings/730/Synthetic%20item'
        canonical = source + '/canonical'
        self.runtime.remember_history_redirect(pipeline.book, source, canonical)
        opener = mock.Mock()
        pipeline.book.enable('steam', False)
        with self.assertRaises(self.app.SteamGovernorDeferred) as paused:
            with self.app.steam_network_open(opener, urllib.request.Request(source), 5):
                self.fail('Mapped URL cannot bypass operator pause')
        self.assertTrue(paused.exception.state['operatorPaused'])
        pipeline.book.enable('steam', True)
        with pipeline.book.connect(True) as conn:
            conn.execute("UPDATE provider SET next_allowed=?,last_error='Synthetic saved backoff' WHERE id='steam'", (now + 600,))
        before = self.steam_receipts(pipeline.book)
        with self.assertRaises(self.app.SteamGovernorDeferred) as paced:
            with self.app.steam_network_open(opener, urllib.request.Request(source), 5):
                self.fail('Mapped URL cannot bypass saved backoff')
        self.assertEqual(paced.exception.state['retryAt'], now + 600)
        opener.open.assert_not_called()
        self.assertEqual(self.steam_receipts(pipeline.book), before)
        self.assertEqual(self.runtime.resolved_history_url(pipeline.book, source), canonical)

    @staticmethod
    def synthetic_ssr_query(name, prices, appid=730, currency=1):
        return {'queryKey': ['market', 'pricehistory', appid, name],
                'state': {'data': {'ecurrency': currency, 'prices': prices}}}

    @staticmethod
    def modern_ssr_page(queries):
        root = {'renderContext': {'queryData': json.dumps({'queries': queries})}}
        return '<!doctype html><script id="valve-ssr-data" type="application/json">' + json.dumps(root) + '</script>'

    def test_modern_steam_ssr_selects_only_exact_listing_variant(self):
        name = 'Synthetic item | Finish (Field-Tested)'
        exact_prices = [['Jan 02 2026 01: +0', '3.25', '4'],
                        ['Jan 01 2026 01: +0', '1.25', '1,234']]
        queries = [
            self.synthetic_ssr_query('Synthetic item | Finish (Factory New)', [['Jan 01 2026 01: +0', 99, 1]]),
            self.synthetic_ssr_query('StatTrak Synthetic item | Finish (Field-Tested)', [['Jan 01 2026 01: +0', 77, 1]]),
            self.synthetic_ssr_query(name, exact_prices),
            self.synthetic_ssr_query(name.lower(), [['Jan 01 2026 01: +0', 55, 1]]),
        ]
        points = self.app.parse_steam_ssr_price_points(self.modern_ssr_page(queries), name)
        self.assertEqual([p['price'] for p in points], [1.25, 3.25])
        self.assertEqual([p['volume'] for p in points], [1234, 4])
        self.assertLess(points[0]['ts'], points[1]['ts'])
        self.assertEqual(points[0]['rawTime'], 'Jan 01 2026 01: +0')
        self.assertEqual(self.app.parse_steam_ssr_price_points(self.modern_ssr_page(queries), 'Synthetic missing item'), [])

    def test_modern_steam_ssr_rejects_wrong_app_or_currency(self):
        name = 'Synthetic exact item'
        prices = [['Jan 01 2026 01: +0', 5, 2]]
        for appid, currency in ((440, 1), (730, 3), (730, 0)):
            with self.subTest(appid=appid, currency=currency):
                page = self.modern_ssr_page([self.synthetic_ssr_query(name, prices, appid, currency)])
                self.assertEqual(self.app.parse_steam_ssr_price_points(page, name), [])

    def test_legacy_steam_ssr_window_marker_remains_supported(self):
        name = 'Synthetic legacy item'
        query = {'queryKey': ['market', 'pricehistory', 730, name],
                 'state': {'data': {'prices': [['Jan 01 2026 01: +0', '2.50', '2']]}}}
        context = {'queryData': json.dumps({'queries': [query]})}
        encoded_context = json.dumps(json.dumps(context))
        page = '<script>window.SSR.renderContext=JSON.parse(' + encoded_context + ');</script>'
        points = self.app.parse_steam_ssr_price_points(page, name)
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0]['price'], 2.5)
        self.assertEqual(points[0]['volume'], 2)
        self.assertGreater(points[0]['ts'], 0)
        self.assertEqual(self.app.parse_steam_ssr_price_points(page, name + ' different'), [])

    def prepare_waiting_history_task(self, on_wait=None, suffix=''):
        pipeline = self.install()
        # Simulate an enabled worker; get_price_history is replaced with an
        # invented saved-result callback, with all network guards still active.
        pipeline.allow_network = True
        clock = {'now': float(int(self.app.time.time()))}
        self.stack.enter_context(mock.patch.object(self.app, 'DATA_TASK_CLOCK', lambda: clock['now']))
        pipeline.clock = lambda: clock['now']
        pipeline.book.clock = lambda: clock['now']

        class AdvancingEvent:
            def __init__(self):
                self.waits, self.sets, self.flag = [], 0, False

            def clear(self):
                self.flag = False

            def set(self):
                self.sets += 1
                self.flag = True

            def is_set(self):
                return self.flag

            def wait(self, seconds):
                if not 0 < seconds <= 60 or len(self.waits) >= 20:
                    raise AssertionError('Worker must use positive bounded event waits, not a busy loop')
                self.waits.append(seconds)
                if on_wait:
                    on_wait()
                    return self.flag
                clock['now'] += seconds
                return False

        event = AdvancingEvent()
        self.stack.enter_context(mock.patch.object(self.app, 'DATA_TASK_WAKE', event))
        name = 'Synthetic continuing history item' + (' ' + suffix if suffix else '')
        with self.app.db() as conn:
            conn.execute('''INSERT INTO item_catalog(name,market_hash_name,kind,item_type,collection,updated_at)
                VALUES(?,?,'skins','Synthetic type','Synthetic collection',?)''', (name, name, int(clock['now'])))
            conn.execute('''INSERT INTO market_item_prices(name,provider,price,solid_price,source,observed_at,updated_at)
                VALUES(?,'steam_snapshot',12,12,'synthetic_provider_capture',?,?)''', (name, int(clock['now']), int(clock['now'])))
        self.stack.enter_context(mock.patch.object(self.app, 'market_history_candidates', return_value=[{
            'name': name, 'marketHashName': name, 'kind': 'skins', 'currentValue': 12}]))
        return pipeline, clock, event, name

    def save_synthetic_history_result(self, name, clock, kwargs):
        now = int(clock['now'])
        points = [{'time': 'synthetic-day-' + str(day), 'ts': now - day * 86400,
                   'price': 12 - day / 100, 'volume': 1} for day in range(91)]
        with self.app.db() as conn:
            conn.executemany('''INSERT INTO price_history(name,provider,point_time,point_ts,price,volume,updated_at)
                VALUES(?,'steam',?,?,?,?,?)''', [(name, p['time'], p['ts'], p['price'], p['volume'], now) for p in points])
        kwargs['_refresh_state'].setdefault('names', set()).add(name)
        return {'name': name, 'success': True, 'historySaved': True, 'points': points}

    def run_waiting_history_job(self, history):
        with mock.patch.object(self.app, 'get_price_history', side_effect=history):
            return self.run_synthetic_data_task_post(lambda **kwargs: self.fail('History-only job must not pull providers'),
                options={'mode': 'steam_history', 'cycles': 1, 'historyLimit': 1,
                         'sleepSeconds': 0, 'retryFailures': True, 'force': True}, acquisition_expected=False)

    def test_explicit_history_worker_resumes_same_item_after_canonical_deadline(self):
        pipeline, clock, event, name = self.prepare_waiting_history_task()
        calls, deadline = [], int(clock['now']) + 60

        def history(actual_name, **kwargs):
            calls.append((actual_name, clock['now']))
            if len(calls) == 1:
                with pipeline.book.connect(True) as conn:
                    conn.execute("UPDATE provider SET next_allowed=?,last_status=302 WHERE id='steam'", (deadline,))
                return {'governorDeferred': True, 'canonicalRedirect': True, 'networkAttempted': True,
                        'httpStatus': 302, 'operatorPaused': False, 'retryAt': deadline, 'points': []}
            self.assertEqual(actual_name, name)
            self.assertGreaterEqual(clock['now'], deadline)
            return self.save_synthetic_history_result(name, clock, kwargs)

        task = self.run_waiting_history_job(history)
        self.assertEqual(task['status'], 'complete', task)
        self.assertEqual([n for n, timestamp in calls], [name, name])
        self.assertEqual(event.waits, [60])
        self.assertTrue(any(e.get('stage') == 'waiting_provider' for e in task.get('events', [])))
        with closing(self.app.catalog_read_db()) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM price_history WHERE name=? AND provider='steam'", (name,)).fetchone()[0], 91)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM item_market_metrics WHERE name=? AND provider='market'", (name,)).fetchone()[0], len(self.app.RESEARCH_METRIC_WINDOWS))

    def test_explicit_history_worker_waits_entire_persisted_429_deadline(self):
        pipeline, clock, event, name = self.prepare_waiting_history_task()
        calls, deadline = [], int(clock['now']) + 125

        def history(actual_name, **kwargs):
            calls.append(clock['now'])
            if len(calls) == 1:
                with pipeline.book.connect(True) as conn:
                    conn.execute("UPDATE provider SET next_allowed=?,last_status=429,last_error='Synthetic provider wait' WHERE id='steam'", (deadline,))
                return {'rateLimited': True, 'httpStatus': 429, 'retryAfter': '125',
                        'retryAt': deadline, 'cooldownActive': False, 'networkAttempted': True, 'points': []}
            with pipeline.book.connect() as conn:
                saved_deadline = conn.execute("SELECT next_allowed FROM provider WHERE id='steam'").fetchone()[0]
            self.assertEqual(saved_deadline, deadline)
            self.assertGreaterEqual(clock['now'], saved_deadline)
            return self.save_synthetic_history_result(name, clock, kwargs)

        task = self.run_waiting_history_job(history)
        self.assertEqual(task['status'], 'complete', task)
        self.assertEqual(len(calls), 2)
        self.assertEqual(event.waits, [60, 60, 5])
        self.assertEqual(calls[1] - calls[0], 125)

    def test_explicit_history_pause_and_cancel_wake_without_retry(self):
        for action, expected in (('pause', 'paused'), ('cancel', 'cancelled')):
            with self.subTest(action=action):
                # Each subcase uses a fresh task and no completed history.
                def control():
                    task_id = self.app.ACTIVE_DATA_TASK_ID
                    handler = object.__new__(self.app.Handler)
                    handler.path = '/api/data-tasks'
                    handler.client_address = ('127.0.0.1', 0)
                    handler.require_auth = mock.Mock(return_value=True)
                    handler.read_body_json = lambda: {'action': action, 'taskId': task_id}
                    handler.send_json = mock.Mock()
                    handler.do_POST()
                    self.assertTrue(handler.send_json.call_args.args[0]['ok'])

                # Reuse the installed runtime with distinct invented metadata.
                pipeline, clock, event, name = self.prepare_waiting_history_task(control, suffix=action)
                calls = []

                def history(actual_name, **kwargs):
                    calls.append(actual_name)
                    return {'governorDeferred': True, 'networkAttempted': False,
                            'operatorPaused': False, 'retryAt': int(clock['now']) + 600, 'points': []}

                task = self.run_waiting_history_job(history)
                self.assertEqual(task['status'], expected, task)
                self.assertEqual(calls, [name])
                self.assertEqual(len(event.waits), 1)
                self.assertGreater(event.sets, 0)
                self.assertFalse(any(e.get('stage') == 'complete' for e in task.get('events', [])))


if __name__ == '__main__':
    unittest.main()
