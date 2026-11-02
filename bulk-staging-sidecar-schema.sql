-- Disposable PRIVATE C: SIDECAR ONLY. Never execute on the live database.
-- Derived projection tables/settings only: no history, source raw or holdings.
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS item_market_metrics(
 name TEXT NOT NULL,provider TEXT NOT NULL,window_days INTEGER NOT NULL,
 points INTEGER,clean_points INTEGER,edge_case_count INTEGER,volatility_pct REAL,
 range_pct REAL,trend_pct REAL,first_price REAL,latest_price REAL,first_ts INTEGER,
 last_ts INTEGER,current_price REAL,prior_price REAL,price_source TEXT,
 updated_at INTEGER NOT NULL,raw TEXT,PRIMARY KEY(name,provider,window_days));
CREATE INDEX IF NOT EXISTS idx_item_market_metrics_name ON item_market_metrics(name,provider,window_days);
CREATE INDEX IF NOT EXISTS idx_item_market_metrics_window ON item_market_metrics(provider,window_days,name);
CREATE TABLE IF NOT EXISTS market_item_lookup(
 window_days INTEGER NOT NULL,name TEXT NOT NULL,market_hash_name TEXT,kind TEXT,
 item_type TEXT,weapon TEXT,collection TEXT,rarity TEXT,latest_price REAL,
 prior_price REAL,change_pct REAL,change_dollar REAL,latest_at INTEGER,source TEXT,
 volatility_pct REAL,history_points INTEGER,edge_case_count INTEGER,groups_json TEXT,
 search_text TEXT,metric_updated_at INTEGER,steam_price REAL,steam_lowest_sell_order REAL,
 steam_highest_buy_order REAL,steam_spread_pct REAL,steam_stability_pct REAL,
 price_basis TEXT,confidence_pct REAL,provider_prices_json TEXT,
 PRIMARY KEY(window_days,name));
CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_change ON market_item_lookup(window_days,change_pct);
CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_price ON market_item_lookup(window_days,latest_price);
CREATE INDEX IF NOT EXISTS idx_market_item_lookup_window_name ON market_item_lookup(window_days,name);
CREATE INDEX IF NOT EXISTS idx_market_item_lookup_filters ON market_item_lookup(window_days,collection,item_type,rarity);
