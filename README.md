# cs2dash

Version **1.0.1** restores the published v1.0 research interface and data paths while retaining the later normalized current-cash pricing work. The verified historical baseline and complete control matrix are in [docs/V1-BASELINE.md](docs/V1-BASELINE.md) and [docs/V1-PARITY-MATRIX.md](docs/V1-PARITY-MATRIX.md).

The Items view has two explicit bases: **Provider benchmark research** and **Current cash listings**. Research uses cached provider snapshots and genuine dated history; cash values use qualified, fresh listing quotes. Their prices, period movements and coverage are not interchangeable. Global item filters and sorting run before the bounded result page, and Previous/Next controls reach subsequent pages.

The application uses Python 3.12+ and SQLite. Current public-price transports also require Node on PATH, or an absolute `CS2_CASH_NODE` executable path. No third-party Python packages are required. Existing inventory, settings and local note/preset storage keys are retained.

Both research and cash paths start with the existing `server.py` entry point. `CS2DASH_DATA_DIR` selects personal/research state; its separate `prices-current.sqlite` ledger is retained, or `CS2DASH_QUOTE_DB` can select an existing ledger. Startup never overwrites an existing quote ledger. Cash acquisition uses its saved provider pacing; original research refresh jobs remain explicit unless `CS2DASH_RESEARCH_AUTO_REFRESH=1` or `CS2DASH_RESUME_RESEARCH_TASKS=1` is deliberately configured.

`/api/health` reports version and code identity. `/api/research-capabilities` distinguishes implemented features from saved data, provider admission and acquisition prerequisites. Missing historical observations remain unavailable; current quotes are never used to manufacture a past curve. See [the offline recovery procedure](docs/RESEARCH-RECOVERY.md) for additive recovery from retained research archives without replacing personal data.

## Restoration validation

```sh
python -m unittest discover -s tests -p 'test_*.py' -v
python -m unittest discover -s tools -p 'test_*.py' -v
node --test tools/frontend_restore.test.mjs
```

These tests use invented temporary data. They exercise actual research/current API contracts, scope and period calculations, whole-catalog filtering/paging, authentication, null-safe import/export, shared Steam cooldowns, recovery safety and complete-script UI rendering. Browser acceptance and live provider checks are separate qualification stages; source tests alone do not establish a live deployment.

The original v1.0 usage and research contract follow below. References to Steam/provider benchmark values describe the research basis; current cash listings are shown separately.

Version 1.0 local-first dashboard for Counter-Strike 2 portfolio valuation, full-market analysis, provider price comparison, and collection-level trend research.

## What It Does

- Imports portfolio holdings as item identity, quantity, and metadata only.
- Ignores imported price columns because import pricing is not trusted.
- Stores market/catalog data locally in SQLite so refreshes and restarts keep pricing, history, and analysis data available.
- Uses cached CSGO Trader provider snapshots for current valuation: Steam, CSFloat, BUFF163, Skinport, and Youpin.
- Caches Steam market chart history for raw item charts and portfolio/group movement curves.
- Pull Market Data refreshes provider snapshots, rebuilds 7d/30d/90d metrics, and warms priority Steam histories; remaining Steam charts fill on demand.
- Separates portfolio analysis from full-market analysis. The Market page models one of every locally cached market item.
- Groups collections, item types, and events with sortable value, return, volatility, breadth, mover, and history views.
- Serves Items from a server-side materialized lookup table instead of pushing the full market universe to the browser.

## Quick Start

cs2dash requires a login. The convenience script handles first-run setup:

```bash
./start.sh
```

On first run it creates an ignored `cs2dash.env`, asks for the required login password, sets `CS2DASH_COOKIE_SECURE=0` for local `http://` testing, creates the data directory, and starts `server.py`. Open `http://localhost:8080` and sign in. The default username is `admin`; the server binds `127.0.0.1` by default.

To rebuild the local env file later:

```bash
./start.sh --setup
```

Manual startup still works if you prefer to provide environment variables yourself:

```bash
python server.py --hash-password
export CS2DASH_AUTH_PASSWORD_HASH='pbkdf2_sha256$...'
export CS2DASH_COOKIE_SECURE=0
python server.py 8080
```

For a **trusted LAN only**, you can skip login and listen on all interfaces:

```bash
export CS2DASH_AUTH_DISABLE=1
export CS2DASH_BIND=0.0.0.0
python server.py 8080
# then open http://<server-lan-ip>:8080
```

The convenience script honors the same environment variables and accepts an optional port:

```bash
./start.sh
./start.sh 9000
```

After code changes, restart the server and hard refresh the browser. To run cs2dash on the public internet over HTTPS, see [Deployment](#deployment).

## Requirements

- Python 3.12 or newer (standard library only - no third-party Python packages)
- Node on PATH (or `CS2_CASH_NODE`) for the current public cash-price transports
- Network access for catalog and market refreshes
- A login password via `CS2DASH_AUTH_PASSWORD_HASH` (or `CS2DASH_AUTH_PASSWORD` for dev), unless `CS2DASH_AUTH_DISABLE=1`
- Optional `STEAM_COOKIE` environment variable for reliable Steam chart-history requests that need an authenticated session

## Steam Chart History

Steam chart history is rate-limited and should be warmed gradually. `Pull Market Data` refreshes provider snapshots and warms a priority set of missing Steam charts. Already cached charts are skipped, and recent failures are retried on explicit data pulls without refetching cached rows.

Recommended runtime settings for the LAN server:

```bash
export STEAM_COOKIE='steamLoginSecure=...; sessionid=...'
export STEAM_MIN_INTERVAL=2.0
export STEAM_HISTORY_FETCH_TIMEOUT=10
python server.py 8080
```

Do not commit or log the cookie value. If `STEAM_COOKIE` is missing, cs2dash falls back to public Steam listing pages, which is less reliable for some items.

## Runtime Data

The app creates local runtime state under:

- `data/` for SQLite, saved portfolios, and settings
- `cache/` for refreshed external catalog/cache files
- `__pycache__/` for Python bytecode

These folders are intentionally ignored by Git and are not part of a release submission.

## Configuration

All configuration is via environment variables. The most common:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CS2DASH_AUTH_PASSWORD_HASH` | — | PBKDF2 login hash (generate with `--hash-password`). Required unless auth is disabled. |
| `CS2DASH_AUTH_PASSWORD` | — | Plaintext password hashed at startup (dev convenience; prefer the hash). |
| `CS2DASH_AUTH_USER` | `admin` | Login username. |
| `CS2DASH_AUTH_DISABLE` | `0` | Set to `1` to disable login (trusted LAN only). |
| `CS2DASH_BIND` | `127.0.0.1` | Listen address. Use `0.0.0.0` for direct LAN access. |
| `CS2DASH_SESSION_TTL` | `2592000` | Session lifetime in seconds (30 days). |
| `CS2DASH_COOKIE_SECURE` | `1` | Mark the session cookie HTTPS-only. Set `0` for plain-HTTP local testing. |
| `CS2DASH_DATA_DIR` | `./data` | Location of SQLite DB, portfolios, and settings. |
| `STEAM_COOKIE` | — | Steam session for reliable chart history. Never commit or log it. |
| `CSGOTRADER_PRICE_BASE` | `https://prices.csgotrader.app/latest` | Base URL for provider snapshot pricing. |
| `MARKET_SOURCE_PRIORITY` | `steam,csfloat,buff163,youpin,skinport` | Provider order for current price selection. |
| `AUTO_MARKET_REFRESH` | `1` | Enable periodic background provider snapshot refresh. `start.sh` defaults new local envs to `0` while importing/debugging. |
| `DATA_TASK_RESUME_ENABLED` | `1` | Resume interrupted background data fills on startup. `start.sh` defaults new local envs to `0` to avoid stale jobs locking local imports. |

## Project Structure

```text
cs2-dash/
|-- server.py      cs2dash server, SQLite schema, market cache, REST API
|-- index.html     Single-page dashboard UI
|-- start.sh       Convenience launch script
|-- README.md      Project and release notes
|-- deploy/        systemd unit, Caddyfile, env template, DEPLOY.md runbook
|-- tools/         Local validation utilities
`-- .gitignore     Runtime/generated file exclusions
```

## Main Data Flow

1. Portfolio imports are normalized and stored locally.
2. Public CS2 catalog metadata populates item identity, image, collection, rarity, kind, weapon, and event context.
3. CSGO Trader provider snapshots populate current provider prices.
4. The bulk market pull appends local provider observations, rebuilds metric rows, and warms priority Steam chart histories for portfolio and high-signal market items.
5. Steam history powers raw item charts and the moving portions of portfolio/group value curves; missing chart histories are marked as current-price backfill until warmed.
6. Metrics tables cache movement, volatility, current/prior values, and lookup rows for fast analysis.
7. `/api/taxonomy-analysis` returns comparable group data for `scope=portfolio` or `scope=market`.
8. `/api/group-history` returns chart-ready group value history. Portfolio groups are quantity-weighted; market groups are one-each baskets.

## Pricing Rules

- Current valuation comes from cached provider snapshot data, not imported prices.
- Provider prices never silently diverge from displayed valuation.
- Steam history is chart/reference data, not a competing current valuation provider.
- Portfolio and group Value Over Period charts use cleaned raw Steam history where cached and marked current-price backfill for items whose Steam chart has not been warmed yet.
- Raw Steam item charts preserve raw Steam points, including edge cases.
- Trend and group valuation charts use cleaned/interpolated data and are labeled as such.
- Carried-forward chart spans are visually dashed instead of presented as normal market movement.
- Bulk data pulls warm a configurable number of priority Steam histories; failures are cached with a cooldown so repeated pulls do not stall on the same rejected Steam chart.
- Manual bulk data pulls retry recent Steam failures while still skipping charts that are already cached.
- Low-volume isolated sales and suspicious one-off moves are guarded before they affect trend or volatility calculations.

## Key API Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /` | Dashboard UI |
| `GET /api/status` | Local storage and cache status |
| `GET /api/portfolios` | Saved portfolio imports |
| `POST /api/portfolios` | Replace saved portfolios |
| `GET /api/prices?names=[...]` | Cached provider prices for named items |
| `GET /api/history?name=...&hash=...` | Steam/local item history |
| `GET /api/market-items` | Server-side filtered market item slice |
| `GET /api/taxonomy-analysis?scope=portfolio` | Portfolio group analysis |
| `GET /api/taxonomy-analysis?scope=market` | Full-market one-each group analysis |
| `GET /api/group-history?scope=market` | Group value history |
| `GET /api/market-universe-sync?mode=bulk` | Refresh all CSGO Trader market snapshot sources, rebuild local metrics, and warm priority Steam histories |

## Release Validation

```bash
python -m py_compile server.py tools/validate_pricing_contract.py
```

```powershell
@'
const fs = require('fs');
const html = fs.readFileSync('index.html', 'utf8');
const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)].map(m => m[1]).join('\n');
new Function(scripts);
console.log('inline script syntax ok');
'@ | node -
```

Useful HTTP smoke checks after restarting:

```bash
curl "http://localhost:8080/api/status"
curl "http://localhost:8080/api/market-items?limit=25"
curl "http://localhost:8080/api/taxonomy-analysis?scope=market&days=90"
```

Pricing-cache contract checks:

```bash
python tools/validate_pricing_contract.py
python tools/validate_pricing_contract.py --cold-sync
```

The cold-sync check uses a temporary empty database, pulls CSGO Trader provider snapshots, rebuilds metrics, and deletes the temporary data when finished.

## Security

cs2dash includes single-user login. By default, protected pages and API calls require a session cookie; unauthenticated API requests get `401` and page loads redirect to `/login`. `CS2DASH_PUBLIC_READONLY=1` permits only the cached-data GET allowlist without signing in. Write operations still require authentication, and anonymous reads cannot force a provider refresh.

- Set a password before exposing the app: generate a hash with `python server.py --hash-password` and provide it via `CS2DASH_AUTH_PASSWORD_HASH` (username via `CS2DASH_AUTH_USER`, default `admin`).
- The server refuses to start if auth is enabled but no password is configured. For a trusted local network only, you may disable auth with `CS2DASH_AUTH_DISABLE=1`.
- The server binds `127.0.0.1` by default (`CS2DASH_BIND`). For direct LAN access set `CS2DASH_BIND=0.0.0.0`; for internet exposure keep it on loopback behind a reverse proxy.
- Session cookies are `HttpOnly; SameSite=Strict; Secure`. Set `CS2DASH_COOKIE_SECURE=0` only when testing over plain HTTP locally.
- Destructive delete operations additionally require the request to originate from localhost.

## Deployment

To run cs2dash on the public internet (HTTPS + login) from a home Linux server, see [`deploy/DEPLOY.md`](deploy/DEPLOY.md). It covers the systemd service ([`deploy/cs2dash.service`](deploy/cs2dash.service)), Caddy reverse proxy with automatic TLS ([`deploy/Caddyfile`](deploy/Caddyfile)), the secret env file ([`deploy/cs2dash.env.example`](deploy/cs2dash.env.example)), DuckDNS dynamic DNS, and router port forwarding.

If your ISP uses CGNAT or you'd rather not open router ports (and want to keep your home IP private), use the **Cloudflare Tunnel** path in [`deploy/DEPLOY-cloudflare-tunnel.md`](deploy/DEPLOY-cloudflare-tunnel.md) instead — it's outbound-only with no port forwarding.
