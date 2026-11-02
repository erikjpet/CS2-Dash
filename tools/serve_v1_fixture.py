#!/usr/bin/env python3
"""Ephemeral real-handler browser fixture; every holding and amount is invented.

Run only in an isolated QA process with CPU, memory and lifetime limits.
Example (inside a capped owned job):
  python tools/serve_v1_fixture.py --port 8097 --ready /tmp/job/fixture-ready.json

Only 127.0.0.1 is bound. Lifetime is at most 180 seconds. Write the ready record's
stopFile to request early shutdown. The program then closes its own listener,
stops the offline pipeline/cache and removes its temporary databases. A closed
receipt is written next to the ready receipt. No existing service is attached,
stopped or reused, and no application/user data is read from disk.

Ordinary UI reads, notes/presets and invented portfolio/settings mutations use
the restored real Handler. Provider acquisition paths are explicitly rejected
by the fixture guard; their rejection is not a production capability result.
"""
from datetime import datetime, timezone
import hashlib
import http.server
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[1]


def fixture_test_class():
    path = ROOT / 'tests' / 'test_v1_parity.py'
    spec = importlib.util.spec_from_file_location('cs2_v1_synthetic_fixture_factory', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.VersionOneParityTests


def build_fixture():
    """Seed data only; does not bind a socket, start a worker or call a provider."""
    fixture = fixture_test_class()()
    isolation = mock.patch.dict(os.environ, {
        'STEAM_COOKIE': '',
        'CS2DASH_CSFLOAT_API_KEY': '',
        'CS2DASH_CSFLOAT_KEY_FILE': str(ROOT / 'tests' / 'synthetic-no-credential-file'),
        'CS2DASH_AUTH_PASSWORD': '',
        'CS2DASH_AUTH_PASSWORD_HASH': '',
    })
    isolation.start()
    try:
        fixture.setUp()
        # Point the configured credential metadata probe inside the newly made
        # private fixture directory, never at the current user's real key path.
        os.environ['CS2DASH_CSFLOAT_KEY_FILE'] = str(Path(fixture.temp.name) / 'nonexistent-synthetic-key')
        fixture.seed_research()
        app = fixture.app
        grid_names = ['Synthetic Grid ' + str(index).zfill(4) for index in range(507)]
        grid_prices = {name: (.5 if index == 506 else 10 + index)
                       for index, name in enumerate(grid_names)}
        with app.db() as conn:
            for index, (name, amount) in enumerate(grid_prices.items()):
                conn.execute('''INSERT INTO item_catalog(name,kind,item_type,weapon,collection,rarity,market_hash_name,raw,updated_at)
                    VALUES(?,'skins','Rifles','Synthetic grid weapon',?,?,?,'{}',?)''',
                    (name, 'Synthetic grid B' if index % 2 else 'Synthetic grid A',
                     'Covert' if index % 2 else 'Classified', name, fixture.now))
                app.upsert_market_item_price(conn, {
                    'name': name, 'price': amount, 'median': amount,
                    'updated_at': fixture.now, 'volume': 100,
                    'source': 'csgotrader:steam',
                }, provider='steam_snapshot', source='csgotrader:steam')
                # Four actual dated fixture observations, not a reconstructed
                # current-price history. First three main items have 91 daily
                # observations from the acceptance-test factory.
                for age in (90, 30, 7, 0):
                    timestamp = fixture.now - age * 86400
                    price = amount * (.5 + (90 - age) / 180)
                    stamp = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                    conn.execute('''INSERT INTO price_history(name,provider,point_time,point_ts,raw_time,price,volume,updated_at)
                        VALUES(?,'steam',?,?,?,?,?,?)''',
                        (name, stamp, timestamp, stamp, price, 100, fixture.now))
            app.rebuild_market_metric_windows(conn, windows=(7, 30, 90), names=grid_names, as_of=fixture.now)
            for window in (7, 30, 90):
                app.rebuild_market_item_lookup(conn, window)

        heavy = app.get_portfolios()[0]
        light = {
            'id': 'synthetic-light', 'label': 'Synthetic light portfolio',
            'importedAt': datetime.fromtimestamp(fixture.now - 86400, timezone.utc).isoformat(),
            'data': {'items': [
                [fixture.names[2], 7, None, None, None, None, 'Synthetic category', 'Synthetic C'],
                [grid_names[-1], 4, None, None, None, None, 'Synthetic category', 'Synthetic grid'],
            ]},
        }
        app.replace_portfolios([light, heavy])
        cash_prices = dict(zip(fixture.names, (25, 10, 8)))
        cash_prices.update(grid_prices)
        fixture.seed_cash(cash_prices)
        with fixture.pipeline.book.connect(True) as conn:
            medians = {fixture.names[0]: 30, fixture.names[1]: 8,
                       fixture.names[2]: 8, grid_names[-1]: .5}
            for name, median in medians.items():
                subject = conn.execute('SELECT id FROM subject WHERE market_hash_name=?', (name,)).fetchone()[0]
                for days in (1, 7, 30, 90):
                    conn.execute('INSERT INTO cash_sales_reference VALUES(?,?,?,?,?,?,?)',
                                 (subject, days, round(median * 1000), round(median * 900),
                                  round(median * 1100), 20, fixture.now * 1000000))
                for age, fraction in ((3, .92), (2, .96), (1, 1)):
                    bucket = fixture.now // 3600 - age
                    as_of = (bucket + 1) * 3600 - 1
                    conn.execute('INSERT INTO cash_observed_close VALUES(?,?,?,?,?,?,?)',
                                 (subject, 3600, bucket, round(cash_prices[name] * fraction * 1000),
                                  as_of, as_of, 'skinport_live'))
            conn.execute("UPDATE meta SET value='2' WHERE key='revision'")

        info = {
            'fixture': True, 'allDataInvented': True, 'providerNetworkEnabled': False,
            'ownerCanEdit': True, 'authenticationDisabledForSyntheticFixture': True,
            'catalogItems': 510, 'rawHistoryRows': 91 * 3 + 4 * 507,
            'initialPortfolioId': 'synthetic-heavy',
            'portfolios': [
                {'id': 'synthetic-light', 'label': 'Synthetic light portfolio',
                 'researchValue': 492, 'cashValue': 58},
                {'id': 'synthetic-heavy', 'label': 'Synthetic heavy portfolio',
                 'researchValue': 340, 'researchPrior90d': 230, 'cashValue': 95},
            ],
            'heavyHoldings': [{'name': fixture.names[0], 'qty': 3}, {'name': fixture.names[1], 'qty': 2}],
            'cheapItemBeyondAlphabetic500': grid_names[-1], 'cheapUnitPrice': .5,
            'searchTerm': 'Synthetic Alpha',
            'filterExample': {'collection': 'Synthetic AB collection', 'type': 'Rifles', 'grade': 'Synthetic grade'},
            'groupExample': {'dimension': 'Collection', 'group': 'Synthetic AB collection',
                             'portfolioFirst90d': 230, 'portfolioLast': 340,
                             'marketFirst90d': 90, 'marketLast': 120},
            'currentReviewExamples': {'buyWatch': fixture.names[0], 'sellReview': fixture.names[1]},
            'importedBenchmarkFieldsAreNull': True,
            'frontendSha256': hashlib.sha256((ROOT / 'index.html').read_bytes()).hexdigest(),
            'acquisitionGuard': 'Synthetic fixture blocks all provider/job acquisition; production behavior is not tested here.',
        }

        def cleanup():
            try:
                fixture.doCleanups()
            finally:
                isolation.stop()

        return SimpleNamespace(app=app, fixture=fixture, info=info, cleanup=cleanup)
    except BaseException:
        try:
            fixture.doCleanups()
        finally:
            isolation.stop()
        raise


def fixture_handler(app, info):
    blocked_get = {'/api/market-universe-sync', '/api/portfolio-sync'}
    blocked_post = {
        '/api/data-tasks', '/api/catalog/refresh', '/api/catalog-refresh', '/api/catalog-migrate',
        '/api/market-refresh', '/api/steam-current-ask', '/api/steam-ask-jobs',
        '/api/steam-request-control', '/api/cash-price-jobs', '/api/bulk-derived-control',
    }

    class SyntheticHandler(app.Handler):
        def do_GET(self):
            url = urllib.parse.urlsplit(self.path)
            if url.path == '/api/fixture-info':
                return self.send_json(info)
            query = urllib.parse.parse_qs(url.query)
            if url.path in blocked_get or any(
                    query.get(key, ['0'])[0].lower() in ('1', 'true', 'yes')
                    for key in ('force', 'refresh', 'refresh_history')):
                return self.send_json({'ok': False, 'state': 'synthetic_acquisition_disabled',
                    'fixture': True, 'error': info['acquisitionGuard']}, 503)
            return super().do_GET()

        def do_POST(self):
            if urllib.parse.urlsplit(self.path).path in blocked_post:
                return self.send_json({'ok': False, 'state': 'synthetic_acquisition_disabled',
                    'fixture': True, 'error': info['acquisitionGuard']}, 503)
            return super().do_POST()

    return SyntheticHandler


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8097)
    parser.add_argument('--ready', type=Path, required=True)
    parser.add_argument('--stop-file', type=Path)
    parser.add_argument('--lifetime', type=int, default=180)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535 or not 1 <= args.lifetime <= 180:
        parser.error('Port must be 1..65535; lifetime must be 1..180 seconds')
    ready = args.ready.resolve()
    stop_file = (args.stop_file or ready.with_name('stop.request')).resolve()
    if ready.exists() or stop_file.exists():
        raise SystemExit('Reconcile an existing ready/stop receipt before this fixture is started')
    stopped = threading.Event()
    for name in ('SIGINT', 'SIGTERM'):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *unused: stopped.set())
    bundle, server = None, None
    closed = {'fixture': True, 'pid': os.getpid(), 'ownedListenerClosed': False,
              'temporaryDataCleaned': False, 'providerNetworkEnabled': False}
    try:
        bundle = build_fixture()
        run_id = uuid.uuid4().hex
        bundle.info.update(runId=run_id, url='http://127.0.0.1:' + str(args.port))
        server = http.server.ThreadingHTTPServer(('127.0.0.1', args.port), fixture_handler(bundle.app, bundle.info))
        server.daemon_threads = False
        server.block_on_close = True
        server.timeout = .2
        deadline = time.monotonic() + args.lifetime
        ready.parent.mkdir(parents=True, exist_ok=True)
        receipt = {**bundle.info, 'pid': os.getpid(), 'lifetimeSeconds': args.lifetime,
                   'stopFile': str(stop_file), 'persistentProfile': False}
        with ready.open('x', encoding='utf-8') as stream:
            json.dump(receipt, stream, indent=2)
            stream.write('\n')
        print(json.dumps({'fixture': True, 'pid': os.getpid(), 'url': bundle.info['url'],
                          'ready': str(ready), 'lifetimeSeconds': args.lifetime}), flush=True)
        while time.monotonic() < deadline and not stopped.is_set() and not stop_file.exists():
            server.handle_request()
    finally:
        if server is not None:
            server.server_close()
            closed['ownedListenerClosed'] = True
        if bundle is not None:
            bundle.cleanup()
            closed['temporaryDataCleaned'] = True
        closed['closedAt'] = datetime.now(timezone.utc).isoformat()
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.with_suffix('.closed.json').write_text(json.dumps(closed, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(closed), flush=True)


if __name__ == '__main__':
    main()
