#!/usr/bin/env python3
"""Validate cs2dash pricing-cache invariants.

This smoke test is intentionally local-first. By default it inspects the
current SQLite cache. With --cold-sync it uses a temporary empty data directory
and performs a CSGO Trader bulk pull, which verifies the cold-start path without
touching the real application database.
"""

import argparse
import os
import shutil
import sys
import tempfile


def configure_data_dir(args):
    if not args.cold_sync:
        return None
    tmp = tempfile.mkdtemp(prefix="cs2dash-pricing-")
    os.environ["CS2DASH_DATA_DIR"] = tmp
    os.environ["AUTO_MARKET_REFRESH"] = "0"
    return tmp


def fail(message):
    raise SystemExit("FAIL: " + message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cold-sync", action="store_true", help="use an empty temp DB and pull CSGO Trader snapshots")
    parser.add_argument("--keep-temp", action="store_true", help="keep the temp data directory after --cold-sync")
    args = parser.parse_args()

    temp_dir = configure_data_dir(args)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import server  # noqa: E402

    try:
        server.init_db()
        if args.cold_sync:
            result = server.market_universe_bulk_sync_csgotrader()
            if not result.get("ok"):
                fail("cold CSGO Trader sync did not store prices: " + str(result.get("error")))

        with server.db() as conn:
            for win in server.VOLATILITY_DETAIL_WINDOWS:
                if not server.metric_window_is_current(conn, win):
                    server.rebuild_market_metrics(conn, win)
            conn.commit()

            provider_count = conn.execute("SELECT COUNT(*) FROM market_item_prices WHERE price IS NOT NULL").fetchone()[0]
            if provider_count <= 0:
                fail("no current provider prices are stored")

            for win in server.VOLATILITY_DETAIL_WINDOWS:
                row = conn.execute(
                    """SELECT COUNT(*) AS rows,
                              SUM(CASE WHEN raw LIKE ? THEN 1 ELSE 0 END) AS current_rows,
                              SUM(CASE WHEN raw LIKE '%"metricBasis":"csgotrader_window_anchor"%' THEN 1 ELSE 0 END) AS anchors,
                              SUM(CASE WHEN raw LIKE '%"metricBasis":"provider_observations"%' THEN 1 ELSE 0 END) AS observations,
                              SUM(CASE WHEN raw LIKE '%"metricBasis":"steam_history"%' THEN 1 ELSE 0 END) AS steam
                       FROM item_market_metrics
                       WHERE provider='market' AND window_days=?""",
                    (f'%"metricBuildVersion":{server.METRIC_BUILD_VERSION}%', win),
                ).fetchone()
                rows = int(row["rows"] or 0)
                if rows <= 0:
                    fail(f"{win}d metrics are missing")
                if int(row["current_rows"] or 0) != rows:
                    fail(f"{win}d metrics contain stale build rows")
                if not any(int(row[k] or 0) for k in ("anchors", "observations", "steam")):
                    fail(f"{win}d metrics have no recognized basis")

        status = server.market_universe_status()
        if int(status.get("observedItems") or 0) <= 0:
            fail("market status does not report observed provider prices")

        print("pricing contract ok")
        print("data_dir:", server.DATA_DIR)
        print("observed_items:", status.get("observedItems"))
    finally:
        if temp_dir and not args.keep_temp:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
