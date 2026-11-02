"""Version-one behavioral acceptance using invented temporary databases only.

No live service, private data, browser, provider transport or external network
is used. Requests run through the actual HTTP handler over an in-memory socket.
Run from the repository root: python -m unittest discover -s tests -p test_v1_parity.py -v
"""
from contextlib import ExitStack, closing, redirect_stderr
from datetime import datetime, timezone
import gc
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class MemorySocket:
    """The real BaseHTTPRequestHandler protocol without a listening socket."""
    def __init__(self, request):
        self.input = io.BytesIO(request)
        self.output = bytearray()

    def makefile(self, mode, *args):
        if mode == 'rb':
            return self.input
        raise AssertionError('Unexpected buffered socket output')

    def sendall(self, value):
        self.output.extend(value)


class VersionOneParityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cs2-v1-parity-synthetic-')
        self.stack = ExitStack()
        self.pipeline = None
        self.app = None
        self.module_name = None
        self.addCleanup(self._cleanup)
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'CS2DASH_DATA_DIR': self.temp.name,
            'CS2DASH_AUTH_DISABLE': '1',
            'CS2DASH_PUBLIC_READONLY': '0',
            'CS2DASH_COOKIE_SECURE': '0',
            'AUTO_MARKET_REFRESH': '0',
            'DATA_TASK_RESUME_ENABLED': '0',
            'CS2DASH_CURRENT_NETWORK': '0',
            'CS2DASH_CATALOG_STORE_ENABLED': '0',
            'CS2DASH_CATALOG_STORE_WRITES': '0',
            'CS2DASH_SOURCE_REVISION': 'a' * 40,
        }))
        self.stack.enter_context(mock.patch('urllib.request.urlopen', side_effect=AssertionError('Provider network forbidden')))
        self.stack.enter_context(mock.patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('Provider network forbidden')))
        self.stack.enter_context(mock.patch('socket.create_connection', side_effect=AssertionError('Network forbidden')))
        self.stack.enter_context(mock.patch('subprocess.Popen', side_effect=AssertionError('Provider process forbidden')))
        self.module_name = 'cs2_v1_parity_' + uuid.uuid4().hex
        spec = importlib.util.spec_from_file_location(self.module_name, ROOT / 'server.py')
        self.app = importlib.util.module_from_spec(spec)
        sys.modules[self.module_name] = self.app
        spec.loader.exec_module(self.app)
        self.now = int(time.time())
        self.stack.enter_context(mock.patch.object(self.app.time, 'time', return_value=self.now))
        self.app.init_db()
        import restoration_runtime
        self.pipeline = restoration_runtime.install(
            self.app, quote_path=Path(self.temp.name) / 'synthetic-cash.sqlite', allow_network=False)

    def _cleanup(self):
        if self.pipeline is not None:
            self.pipeline.stop()
        if getattr(self.app, 'CASH_PRICE_HOLDINGS_CACHE', None) is not None:
            self.app.CASH_PRICE_HOLDINGS_CACHE.close()
        if self.app is not None:
            self.app.close_idle_wal_anchor()
        sys.modules.pop(self.module_name, None)
        self.stack.close()
        gc.collect()
        self.temp.cleanup()

    def request(self, path, method='GET', body=None, cookie=None):
        payload = b'' if body is None else json.dumps(body).encode('utf-8')
        headers = [f'{method} {path} HTTP/1.0', 'Host: synthetic.invalid', 'Connection: close']
        if body is not None:
            headers += ['Content-Type: application/json', 'Content-Length: ' + str(len(payload))]
        if cookie:
            headers.append('Cookie: ' + cookie)
        channel = MemorySocket(('\r\n'.join(headers) + '\r\n\r\n').encode() + payload)
        with redirect_stderr(io.StringIO()):
            self.app.Handler(channel, ('127.0.0.1', 12345), SimpleNamespace(server_name='synthetic.invalid', server_port=0))
        head, content = bytes(channel.output).split(b'\r\n\r\n', 1)
        lines = head.decode('latin1').split('\r\n')
        status = int(lines[0].split()[1])
        response_headers = {key.lower(): value.strip() for key, value in (line.split(':', 1) for line in lines[1:] if ':' in line)}
        value = json.loads(content) if 'application/json' in response_headers.get('content-type', '') else content.decode('utf-8')
        return status, response_headers, value

    def seed_research(self):
        self.names = ['Synthetic Alpha Rifle', 'Synthetic Beta Rifle', 'Synthetic Gamma Rifle']
        portfolio = {
            'id': 'synthetic-heavy', 'label': 'Synthetic heavy portfolio',
            'importedAt': datetime.fromtimestamp(self.now, timezone.utc).isoformat(),
            'data': {'items': [
                [self.names[0], 3, 999, 999999, 999999, 1, 'Synthetic category', 'Synthetic AB'],
                [self.names[1], 2, 999, 999999, 999999, 1, 'Synthetic category', 'Synthetic AB'],
            ]},
        }
        self.app.replace_portfolios([portfolio])
        with self.app.db() as conn:
            for index, name in enumerate(self.names):
                collection = 'Synthetic AB collection' if index < 2 else 'Synthetic C collection'
                conn.execute('''INSERT INTO item_catalog(name,kind,item_type,weapon,collection,rarity,market_hash_name,raw,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?)''', (name, 'skins', 'Rifles', 'Synthetic weapon', collection,
                    'Synthetic grade', name, '{}', self.now))
                current, initial = [(100.0, 50.0), (20.0, 40.0), (70.0, 35.0)][index]
                for provider, multiplier in [('steam_snapshot', 1), ('csfloat', .75), ('buff163', .7), ('skinport', .8), ('youpin', .72)]:
                    self.app.upsert_market_item_price(conn, {
                        'name': name, 'provider': provider, 'price': current * multiplier,
                        'solidPrice': current * multiplier, 'median': current * multiplier,
                        'volume': 100, 'updated_at': self.now,
                        'source': 'csgotrader:' + ('steam' if provider == 'steam_snapshot' else provider),
                    }, provider=provider, source='csgotrader:' + ('steam' if provider == 'steam_snapshot' else provider))
                for day in range(91):
                    timestamp = self.now - (90 - day) * 86400
                    amount = initial + (current - initial) * day / 90
                    stamp = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                    conn.execute('''INSERT INTO price_history(name,provider,point_time,point_ts,raw_time,price,volume,updated_at)
                        VALUES(?,'steam',?,?,?,?,?,?)''', (name, stamp, timestamp, stamp, amount, 100, self.now))
            self.app.rebuild_market_metric_windows(conn, windows=(7, 30, 90), names=self.names, as_of=self.now)
            for window in (7, 30, 90):
                self.app.rebuild_market_item_lookup(conn, window)
        return portfolio

    def seed_cash(self, prices=None):
        prices = dict(zip(self.names, (25, 10, 8))) if prices is None else prices
        with self.pipeline.book.connect(True) as conn:
            for provider in ('skinport_live', 'market_csgo_live'):
                capture = conn.execute('''INSERT INTO capture(provider,received_us,provider_time,body_sha,row_count,request_url,clock_semantics)
                    VALUES(?,?,?,?,?,?,?)''', (provider, self.now * 1000000, self.now, 'b' * 64, len(prices),
                    'https://synthetic.invalid/public-feed', 'provider_snapshot_time')).lastrowid
                conn.execute('UPDATE provider SET last_capture=?,last_received_us=?,last_status=200,enabled=1 WHERE id=?',
                             (capture, self.now * 1000000, provider))
                for name, amount in prices.items():
                    conn.execute('INSERT OR IGNORE INTO subject(market_hash_name) VALUES(?)', (name,))
                    subject = conn.execute('SELECT id FROM subject WHERE market_hash_name=?', (name,)).fetchone()[0]
                    conn.execute('''INSERT INTO quote(subject,provider,currency,kind,price_mills,quantity,state,observed_at,capture,active)
                        VALUES(?,?,'USD','cash_ask',?,10,'priced',?,?,1)''',
                        (subject, provider, amount * 1000, self.now, capture))
            conn.execute("UPDATE meta SET value='1' WHERE key='revision'")

    def test_empty_startup_and_null_history_are_honest(self):
        for path in ('/api/health', '/api/portfolios', '/api/inventory?days=90', '/api/market-items?days=90', '/api/market-universe?days=90'):
            with self.subTest(path=path):
                status, _, data = self.request(path)
                self.assertEqual(status, 200)
        status, _, history = self.request('/api/history?name=Synthetic%20uncached%20item')
        self.assertEqual(status, 200)
        self.assertFalse(history['success'])
        self.assertEqual(history['points'], [])
        self.assertFalse(history['refreshAttempted'])
        self.assertEqual(self.app.get_portfolios(), [])

    def test_import_deduplicates_identity_and_ignores_imported_prices(self):
        rows = [
            ['Synthetic repeated item', 2, 1, 999, 999, 999, 'Synthetic', 'Synthetic'],
            ['Synthetic repeated item', 2, 1, 999, 999, 999, 'Synthetic', 'Synthetic'],
            ['synthetic repeated item', 3, 1, 999, 999, 999, 'Synthetic', 'Synthetic'],
            ['Synthetic invalid item', -1, 1, 999, 999, 999, 'Synthetic', 'Synthetic'],
        ]
        status, _, _ = self.request('/api/portfolios', 'POST', [{'id': 'synthetic-import', 'label': 'Synthetic import', 'data': {'items': rows}}])
        self.assertEqual(status, 200)
        status, _, portfolios = self.request('/api/portfolios')
        self.assertEqual(status, 200)
        self.assertEqual(len(portfolios[0]['data']['items']), 1)
        row = portfolios[0]['data']['items'][0]
        self.assertEqual(row[1], 5)
        self.assertEqual(row[2:6], [None, None, None, None])

    def test_research_inventory_uses_provider_data_not_import_or_cash(self):
        self.seed_research(); self.seed_cash()
        status, _, inventory = self.request('/api/inventory?portfolio=synthetic-heavy&days=90')
        self.assertEqual(status, 200)
        by_name = {row['name']: row for row in inventory['items']}
        alpha, beta = (by_name[name] for name in self.names[:2])
        self.assertEqual(alpha['marketUnitPrice'], 100)
        self.assertEqual(alpha['marketCurr'], 300)
        self.assertEqual(beta['marketCurr'], 40)
        self.assertAlmostEqual(alpha['marketPct'], 100, places=2)
        self.assertAlmostEqual(beta['marketPct'], -50, places=2)
        self.assertTrue({'steam_snapshot', 'csfloat', 'buff163', 'skinport', 'youpin'} <= set(alpha['providerPrices']))
        _, _, cash = self.request('/api/canonical-cash-value?portfolio_id=synthetic-heavy')
        self.assertEqual(cash['totals']['coveredValueUsd'], 95)
        self.assertTrue(cash['coverage']['fullCoverage'])
        self.assertNotEqual(sum(row['marketCurr'] for row in inventory['items']), cash['totals']['coveredValueUsd'])

    def test_market_and_portfolio_scopes_have_distinct_mathematical_baskets(self):
        self.seed_research()
        portfolio_items = self.app.analysis_scope_items('portfolio', 90, 'synthetic-heavy')
        market_items = self.app.analysis_scope_items('market', 90, 'synthetic-heavy')
        self.assertEqual(len(portfolio_items), 2)
        self.assertEqual(len(market_items), 3)
        self.assertAlmostEqual(sum(row['currentValue'] for row in portfolio_items), 340, places=2)
        self.assertAlmostEqual(sum(row['priorValue'] for row in portfolio_items), 230, places=2)
        self.assertAlmostEqual(sum(row['currentValue'] for row in market_items), 190, places=2)
        self.assertAlmostEqual(sum(row['priorValue'] for row in market_items), 125, places=2)
        for scope in ('portfolio', 'market'):
            status, _, taxonomy = self.request('/api/taxonomy-analysis?' + urllib.parse.urlencode({
                'scope': scope, 'portfolio': 'synthetic-heavy', 'days': 90, 'min_group': 1, 'top_groups': 500}))
            self.assertEqual(status, 200)
            self.assertEqual(taxonomy['scope'], scope)
            self.assertTrue(taxonomy['dimensions'])

    def test_raw_history_and_quantity_weighted_curve_preserve_dated_evidence(self):
        self.seed_research()
        _, _, history = self.request('/api/history?' + urllib.parse.urlencode({'name': self.names[0]}))
        self.assertTrue(history['success'])
        self.assertEqual(len(history['points']), 91)
        self.assertEqual(history['points'][0]['price'], 50)
        self.assertEqual(history['points'][-1]['price'], 100)
        self.assertFalse(history['refreshAttempted'])
        _, _, curve = self.request('/api/portfolio-history?portfolio=synthetic-heavy&days=90&points=220')
        self.assertTrue(curve['points'])
        self.assertAlmostEqual(curve['points'][0]['value'], 230, places=1)
        self.assertAlmostEqual(curve['points'][-1]['value'], 340, places=1)

    def test_group_curves_and_item_lines_keep_scope_and_dated_prices(self):
        self.seed_research()
        for scope, first, last in [('portfolio', 230, 340), ('market', 90, 120)]:
            with self.subTest(scope=scope):
                query = urllib.parse.urlencode({'scope': scope, 'portfolio': 'synthetic-heavy',
                    'dimension': 'Collection', 'group': 'Synthetic AB collection', 'days': 90})
                status, _, group = self.request('/api/group-history?' + query)
                self.assertEqual(status, 200)
                self.assertEqual(group['scope'], scope)
                self.assertEqual(group['summary']['count'], 2)
                self.assertAlmostEqual(group['points'][0]['value'], first, places=1)
                self.assertAlmostEqual(group['points'][-1]['value'], last, places=1)
                self.assertEqual(group['coverage']['backfilledCurrentItems'], 0)
                status, _, detail = self.request('/api/group-item-history?' + query)
                self.assertEqual(status, 200)
                self.assertEqual(detail['scope'], scope)
                lines = {line['name']: line for line in detail['series']}
                self.assertEqual(set(lines), set(self.names[:2]))
                self.assertEqual(lines[self.names[0]]['points'][0]['price'], 50)
                self.assertEqual(lines[self.names[0]]['points'][-1]['price'], 100)

    def test_period_selection_uses_its_genuine_observation_window(self):
        self.seed_research()
        observed = {}
        for window in (7, 30, 90):
            status, _, dataset = self.request('/api/inventory?portfolio=synthetic-heavy&days=' + str(window))
            self.assertEqual(status, 200)
            alpha = next(row for row in dataset['items'] if row['name'] == self.names[0])
            self.assertEqual(dataset['historyDays'], window)
            self.assertEqual(alpha['volatility']['windowCoveragePct'], 100)
            observed[window] = alpha['marketPct']
        self.assertAlmostEqual(observed[7], 4.05, places=2)
        self.assertAlmostEqual(observed[30], 20, places=2)
        self.assertAlmostEqual(observed[90], 100, places=2)

    def test_missing_history_is_not_backfilled_from_current_quote(self):
        self.seed_research()
        with self.app.db() as conn:
            conn.execute('DELETE FROM price_history')
        _, _, curve = self.request('/api/portfolio-history?portfolio=synthetic-heavy&days=90')
        self.assertEqual(curve['points'], [])
        self.assertEqual(curve['historyState'], 'missing_raw_steam_history')
        self.assertFalse(curve['historyAvailable'])
        self.assertIn('Genuine cached Steam', curve['historyPrerequisite'])

    def test_both_market_bases_filter_and_rank_all_510_items_before_pagination(self):
        names = ['Synthetic Grid ' + str(index).zfill(4) for index in range(510)]
        prices = {name: (.5 if index == 509 else 10 + index) for index, name in enumerate(names)}
        with self.app.db() as conn:
            for name, amount in prices.items():
                conn.execute('''INSERT INTO item_catalog(name,kind,item_type,collection,rarity,market_hash_name,raw,updated_at)
                    VALUES(?,'skins','Synthetic type','Synthetic grid','Synthetic grade',?,'{}',?)''', (name, name, self.now))
                self.app.upsert_market_item_price(conn, {'name': name, 'price': amount, 'median': amount,
                    'updated_at': self.now, 'source': 'csgotrader:steam'}, provider='steam_snapshot', source='csgotrader:steam')
                self.app.store_item_market_metric(conn, name, 90, {
                    'points': 91, 'cleanPoints': 91, 'edgeCaseCount': 0, 'volatilityPct': 1,
                    'firstPrice': amount / 2, 'latestPrice': amount,
                    'firstTs': self.now - 90 * 86400, 'lastTs': self.now,
                    'metricState': 'real', 'windowCoveragePct': 100,
                    'metricBasis': 'synthetic_dated_fixture',
                }, current_price=amount, prior_price=amount / 2, price_source='csgotrader:steam')
            self.app.rebuild_market_item_lookup(conn, 90)
        self.seed_cash(prices)
        for route in ('/api/market-items', '/api/current-market-items'):
            with self.subTest(route=route):
                status, _, cheap = self.request(route + '?days=90&limit=25&max_price=1&sort=latestPrice&dir=1')
                self.assertEqual(status, 200)
                self.assertEqual([row['name'] for row in cheap['items']], [names[-1]])
                self.assertEqual(cheap['totalItems'], 1)
                status, _, bounded = self.request(route + '?days=90&limit=25&min_price=100&max_price=200&sort=latestPrice&dir=1&offset=25')
                self.assertEqual(status, 200)
                self.assertEqual(bounded['totalItems'], 101)
                self.assertEqual([row['name'] for row in bounded['items']], names[115:140])
                status, _, final_page = self.request(route + '?days=90&limit=500&sort=latestPrice&dir=1&offset=500')
                self.assertEqual(status, 200)
                self.assertEqual(final_page['totalItems'], 510)
                self.assertEqual([row['name'] for row in final_page['items']], names[499:509])
                self.assertFalse(final_page['hasMore'])

    def test_reports_have_real_appendix_and_distinct_current_semantics(self):
        self.seed_research(); self.seed_cash()
        status, _, research = self.request('/api/report')
        self.assertEqual(status, 200)
        self.assertEqual({row['name'] for row in research['items']}, set(self.names[:2]))
        self.assertEqual(research['coverage']['priceSource'], 'observed_market_only')
        status, _, markup = self.request('/report')
        self.assertEqual(status, 200)
        for name in self.names[:2]:
            self.assertIn(name, markup)
        self.assertIn('Appendix', markup)
        status, _, current = self.request('/api/current-analysis?portfolio=synthetic-heavy')
        self.assertEqual(status, 200)
        self.assertEqual(current['portfolioId'], 'synthetic-heavy')
        self.assertEqual(current['totals']['coveredValueUsd'], 95)
        self.assertEqual(current['basis'], 'qualified_cash_listing_estimate')
        self.assertNotIn('dimensions', current)

    def test_public_readonly_cannot_mutate_or_force_provider_requests(self):
        self.seed_research()
        self.app.AUTH_ENABLED = True
        self.app.PUBLIC_READONLY = True
        status, _, history = self.request('/api/history?' + urllib.parse.urlencode({'name': self.names[0], 'force': '1'}))
        self.assertEqual(status, 200)
        self.assertTrue(history['success'])
        self.assertFalse(history['refreshAttempted'])
        before = self.app.get_portfolios()
        self.assertEqual(self.request('/api/settings', 'POST', {'blacklist': ['synthetic']})[0], 401)
        self.assertEqual(self.request('/api/portfolios/synthetic-heavy', 'DELETE')[0], 401)
        self.assertEqual(self.app.get_portfolios(), before)

    def test_owner_login_settings_roundtrip_and_logout(self):
        self.app.AUTH_ENABLED = True
        self.app.PUBLIC_READONLY = False
        self.app.AUTH_USERNAME = 'synthetic-owner'
        self.app.AUTH_HASH = self.app.hash_password('synthetic-password-only')
        self.assertEqual(self.request('/api/settings')[0], 401)
        status, _, _ = self.request('/api/login', 'POST', {'username': 'synthetic-owner', 'password': 'wrong'})
        self.assertEqual(status, 401)
        status, headers, _ = self.request('/api/login', 'POST', {'username': 'synthetic-owner', 'password': 'synthetic-password-only'})
        self.assertEqual(status, 200)
        cookie = headers['set-cookie'].split(';', 1)[0]
        settings = {'blacklist': ['Synthetic excluded term'], 'synthetic-test-setting': 'retained'}
        self.assertEqual(self.request('/api/settings', 'POST', settings, cookie)[0], 200)
        self.assertEqual(self.request('/api/settings', cookie=cookie)[2], settings)
        self.assertEqual(self.request('/api/logout', 'POST', {}, cookie)[0], 200)
        self.assertEqual(self.request('/api/settings', cookie=cookie)[0], 401)


if __name__ == '__main__':
    unittest.main()
