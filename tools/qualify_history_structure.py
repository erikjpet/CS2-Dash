"""One paced public history request; report structural metadata only.

Requires an EXISTING temporary qualification database and quote book. Never
creates a replacement admission ledger, saves HTML, or prints market values.
"""
import argparse
from contextlib import closing
import datetime
import gc
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.request
import uuid


def structure(page, market_hash, target):
    summaries, queries, query_shapes = [], [], []
    for attrs, content in re.findall(r'<script\b([^>]*)>(.*?)</script>', page, re.S | re.I):
        if 'application/json' not in attrs.lower():
            continue
        script_id = re.search(r'\bid=["\x27]([^"\x27]+)', attrs, re.I)
        summary = {'id': script_id.group(1)[:80] if script_id else None, 'length': len(content)}
        try:
            root = json.loads(content)
            summary['rootType'] = type(root).__name__
            summary['rootKeys'] = sorted(root)[:35] if isinstance(root, dict) else []
            stack, seen = [('root', root)], 0
            while stack and seen < 100000:
                path, value = stack.pop()
                seen += 1
                if isinstance(value, dict):
                    if 'queryKey' in value:
                        key = value.get('queryKey') or []
                        state = value.get('state') or {}
                        data = state.get('data')
                        if any(isinstance(item, str) and 'history' in item.lower() for item in key):
                            queries.append({
                                'path': path,
                                'keyShape': [item if item in ('market', 'pricehistory', 730, '730') else type(item).__name__ for item in key],
                                'keyMatchesName': market_hash in key,
                                'keyMatchesCanonical': target.rsplit('/', 1)[-1] in key,
                                'dataType': type(data).__name__,
                                'currencyEnum': data.get('ecurrency') if isinstance(data, dict) else None,
                                'dataKeys': sorted(data)[:25] if isinstance(data, dict) else [],
                                'arrayLengths': {name: len(items) for name, items in data.items() if isinstance(items, list)} if isinstance(data, dict) else {},
                            })
                    for key, item in value.items():
                        safe_key = key if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]{0,50}', str(key)) else '<dynamic-key>'
                        if key == 'queryData':
                            query_shapes.append({'path': path + '.queryData', 'type': type(item).__name__, 'keys': sorted(item)[:20] if isinstance(item, dict) else []})
                            if isinstance(item, str):
                                try:
                                    stack.append((path + '.queryData', json.loads(item)))
                                except ValueError:
                                    pass
                        if isinstance(item, (dict, list)):
                            stack.append((path + '.' + safe_key, item))
                elif isinstance(value, list):
                    stack.extend((path + '[]', item) for item in value[:10000] if isinstance(item, (dict, list)))
            summary['nodesInspected'] = seen
        except Exception as error:
            summary['parseErrorType'] = type(error).__name__
        summaries.append(summary)
    return {'jsonScripts': summaries, 'queryDataShapes': query_shapes[:10], 'historyQueries': queries[:12]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    data = Path(args.data_dir).resolve()
    if not (data / 'prices-current.sqlite').is_file() or not (data / 'cs2dash.sqlite').is_file():
        raise ValueError('An existing qualification database and quote ledger are required')
    for key in list(os.environ):
        if key.startswith('CS2DASH_') or key in ('STEAM_COOKIE', 'STEAM_LOGIN', 'STEAM_PASSWORD', 'CSFLOAT_API_KEY'):
            os.environ.pop(key, None)
    os.environ.update(CS2DASH_DATA_DIR=str(data), CS2DASH_AUTH_DISABLE='1', AUTO_MARKET_REFRESH='0', DATA_TASK_RESUME_ENABLED='0', STEAM_HISTORY_FETCH_TIMEOUT='12')
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location('cs2_public_contract_' + uuid.uuid4().hex, root / 'server.py')
    app = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = app
    spec.loader.exec_module(app)
    app.init_db()
    import restoration_runtime
    pipeline = restoration_runtime.install(app, quote_path=data / 'prices-current.sqlite', allow_network=True)
    name = 'AK-47 | Redline (Field-Tested)'
    target = restoration_runtime.resolved_history_url(pipeline.book, app.steam_market_url(name))
    original_parse, original_open = app.parse_steam_ssr_price_points, urllib.request.OpenerDirector.open
    network, contract = [], {}

    def inspect(page, market_hash):
        contract.update(structure(page, market_hash, target))
        return original_parse(page, market_hash)

    def single_open(opener, fullurl, data=None, timeout=20):
        request = fullurl if isinstance(fullurl, urllib.request.Request) else urllib.request.Request(fullurl)
        if network or request.full_url != target or request.get_method() != 'GET':
            raise RuntimeError('Only one expected canonical GET is permitted')
        if any(key.lower() in ('cookie', 'authorization') for key, value in request.header_items()):
            raise RuntimeError('Credential-bearing request prohibited')
        network.append({'method': 'GET', 'credentialsSent': False})
        response = original_open(opener, request, data=data, timeout=min(float(timeout), 20))
        network[0]['httpStatus'] = response.status
        return response

    before = next(provider for provider in pipeline.book.providers() if provider['id'] == 'steam')
    metadata = {'existingBook': True, 'now': int(time.time()), 'nextAllowedAtBefore': before['next_allowed'], 'bodyCapturedOrSaved': False, 'marketValuesReported': False}
    try:
        if before['next_allowed'] > int(time.time()):
            metadata.update(deferred=True, secondsRemaining=before['next_allowed'] - int(time.time()))
        else:
            urllib.request.OpenerDirector.open = single_open
            app.parse_steam_ssr_price_points = inspect
            result = app.get_price_history(name, force=True, market_hash_name=name, rebuild_metrics=False)
            dates = [point.get('ts') for point in result.get('points', []) if isinstance(point.get('ts'), int) and point['ts'] > 0]
            iso = lambda value: datetime.datetime.fromtimestamp(value, datetime.timezone.utc).isoformat() if value else None
            with closing(app.steam_ask_db()) as connection:
                saved_rows = connection.execute("SELECT COUNT(*) FROM price_history WHERE provider='steam' AND name=?", (name,)).fetchone()[0]
            after = next(provider for provider in pipeline.book.providers() if provider['id'] == 'steam')
            metadata.update(success=result.get('success'), pointCount=len(result.get('points', [])), savedRawPointCount=saved_rows,
                            oldestPointUTC=iso(min(dates)) if dates else None, latestPointUTC=iso(max(dates)) if dates else None,
                            historySaved=result.get('historySaved'), error=result.get('error'), requests=network, contract=contract,
                            leaseClosed=after['lease'] is None, walletCaptureCreated=after['last_received_us'] is not None,
                            backgroundCoordinatorStarted=pipeline.thread is not None)
    finally:
        app.parse_steam_ssr_price_points = original_parse
        urllib.request.OpenerDirector.open = original_open
        pipeline.stop()
        app.CASH_PRICE_HOLDINGS_CACHE.close()
        app.close_idle_wal_anchor()
        gc.collect()
    Path(args.output).write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(json.dumps(metadata, indent=2))


if __name__ == '__main__':
    main()
