# CS2-04 real browser acceptance, invented fixture only

`v1_browser_acceptance.mjs` uses real Playwright Chromium against the restored HTTP handler served by `serve_v1_fixture.py`. It does not substitute a browser, DOM, Chart implementation or API responses. All portfolio/catalog/history/cash data in this fixture are invented.

This file prepares the run; **no browser or fixture listener was launched during preparation**. The QA run must execute inside an owned process job with explicit CPU/memory limits and a resource lease. The runner requires `CS2_V1_BROWSER_RELEASE=GRANTED`, verifies `/api/fixture-info` before launching Chromium, and refuses any base except `http://127.0.0.1:8097`. Use the capped, sandbox-enabled execution wrapper for that owned job. The runner defaults to a 150-second hard budget, configurable with `CS2_V1_BROWSER_BUDGET_MS` from 10,000 to 180,000 milliseconds. It uses one browser/context, enables Chromium sandboxing, blocks service workers and bounds individual operation timeouts. Budget time includes fixture identity verification and browser startup; leave the owned job enough time for cleanup.

Start the fixture in the same owned job with explicit receipts, for example:

```text
python tools/serve_v1_fixture.py --port 8097 --lifetime 180 --ready <fresh-ready-receipt> --stop-file <fresh-stop-receipt>
```

Set these variables through the owned job environment:

```text
CS2_V1_BROWSER_RELEASE=GRANTED
CS2_V1_FIXTURE_BASE=http://127.0.0.1:8097
CS2_V1_BROWSER_BUDGET_MS=150000
PLAYWRIGHT_MODULE=<installed Playwright module path, file URL or package>
CS2_V1_BROWSER_OUTPUT=<local synthetic-evidence output folder>
```

Then run `node tools/v1_browser_acceptance.mjs`. On Windows the existing `D:/Projects/site/node_modules/playwright/index.mjs` is the fallback when `PLAYWRIGHT_MODULE` is omitted. No package installation is performed.

The twelve macro cases cover:

1. Five tabs; 7/30/90 research periods and 7/90 curve periods; real chart creation; return modes; owner acquisition controls without starting a job.
2. Decision-note save and actual page reload.
3. Preset add/select/rename/delete using real controls and synthetic browser-local state.
4. Portfolio grouping lenses, sort controls, real group-history response and drilldown period.
5. Independent full-market grouping lenses/sorts/history/drilldown.
6. Both item bases in both portfolio/catalog scopes, expanded item details.
7. Whole-catalog price sort and bounds proving the cheapest item beyond the old alphabetical 500 is found, plus real Next/Previous pagination in both bases.
8. AND search chips, inline text and collection/type/grade filters.
9. Actual nullable TXT download and pasted reimport preview preserving identity/quantity.
10. Actual synthetic CSV upload, duplicate normalization preview and fixture-only save.
11. Saved snapshot View, History chart, real HTML report and deletion of the newly invented import.
12. 320/390/768 geometry, reachable horizontal table scrolling and synthetic-only screenshots.

Advanced controls are opened only after the previous request/key/render cycle settles; the runner does not force-click hidden inputs. Downloads, fixture mutations and screenshots are limited to invented data. External network requests are blocked except the page's exact Chart.js 4.4.1 CDN script GET. The runner never contacts the live dashboard. Fixture provider/history acquisition is deliberately disabled, so production acquisition and real data/history availability remain separate qualification work.

Screenshots capture the viewport rather than rendering every row of a 500-item table into a tall image. Geometry and horizontal-scroll assertions still exercise the actual table. This keeps evidence capture proportional to the owned memory/time budget. Fixture-import cleanup is attempted only after its save response succeeds, so an earlier save failure does not create a misleading later deletion failure.

`results.json` records each case separately as passed, failed or not run because the budget expired. A failing case does not silently count later work as complete. Any page exception, harness error, budget expiry or nonpassing case prevents an overall passed result. A dependency/CDN failure is reported as an incomplete chart check, not disguised by a fake chart library.

Preparation checks: `node --check tools/v1_browser_acceptance.mjs`. This is syntax validation only, not browser acceptance.
