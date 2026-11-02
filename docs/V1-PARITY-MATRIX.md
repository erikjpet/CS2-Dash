# CS2-04 — Version-one feature parity matrix

Updated 2026-11-02. **Tested `07d21ec` is activated at the existing LAN URL. All 20 cached-GET live checks passed. Authenticated CSFloat has fresh post-restart captures, and one normal public Steam-history job completed with 2,408 genuine points. Live item details, 390/768 mobile geometry and all five navigation tabs with populated groups passed. The initial grouping timeout was a 9-second harness limit; actual market rendering took about 12.2 seconds. The separately reproduced TAX-STALE correction is tested locally and awaits publication/activation. CS2-04 remains open for that follow-up and the explicit coverage limits below.**

This is the canonical delivery matrix for all **74** intake IDs. The original audited-live findings and line references are preserved in [the intake inventory](../tools/CS2-04-control-parity.md). “Audited live” below means that unchanged intake, not the isolated restoration. No initial missing/hidden finding is silently relabelled as a live repair.

## Baseline and stages

There is no GitHub Release or tag for v1. The original v1.0 commit is `de8a469184d4802c536a8937c7d945f021a26314`; the later published v1 fixes through `0e98899ef060742fcec8acd1ce22d03a728f18dc` are included. See [verified baseline](V1-BASELINE.md). The audited live HTML SHA256 was `d927bb422cab87008429378f5938d9a3c9da634ab9a4b38c657ec7ca020dff5e`; latest browser-qualified restored frontend SHA256 is `ed5a5e65c25268b97b321eded19121628b4efe415c2805a2318c512f6020a570`.

| Stage | Verified state |
| --- | --- |
| Source | Research scope/widgets/routes restored alongside current cash functionality in isolated CS2 worktree. All 10 metric windows are materialized by explicit refresh. Warm/Sync use the continuing tracked job with selected-portfolio scope. |
| Portable tests | **119** component checks: 47 current frontend Node cases including focused subcases, plus the unchanged 17 API/runtime and 55 runtime/recovery cases. **9** unchanged site checks: runtime profile 5 and sync 4. Syntax checks cover root Python/MJS, the browser runner and PowerShell launcher. |
| Browser | **UI01:** 7/12 passed on `7524344`. **UI02:** B1/B7/B9/B12 passed on `9ed207c`; B6 failed. **UI03:** B6 and affected B7 passed on `07d21ec`. Each run had zero page errors. This is combined coverage across three revisions; distinct evidence and limits are recorded below. See [acceptance runner](../tools/V1-BROWSER-ACCEPTANCE.md). |
| Normal startup/stop | **CS2STOP02 passed** on exact `9ed207cbc6710502d78427583e451a8ff90014c8`: public build identity matched all 43 runtime files; marker shutdown exited 0, port8098 closed, both invented-state databases passed quick checks, and no provider attempt occurred. |
| Published | **Confirmed to existing main branches:** latest tested CS2 source `07d21ec5f912ce70eb90a6505764df1d9025216e`; guarded site integration `b827906cd32d04c1c822e0eef581b47acd796cde`. Initial implementation was `e7894fe94e297aa442566724cf7acc318b29539d`; subsequent corrections remain traceable commits. Publication is separate from live activation. |
| Live | **Activated, HTTP- and browser-verified for the recorded flows.** Exact `07d21ec` started at 22:48:21 UTC on the existing `http://192.168.0.36:8089/` with unchanged bind/access policy. All 20 cached-GET checks passed; unique state and the complete current ledger were preserved. LIVE01 passed four item basis/scope details and 390/768 mobile geometry. NAV01 passed five tabs and populated visible groups in both scopes. |
| Full v1 parity | **Open.** Source/test coverage is not proof that every click, real dataset and upstream prerequisite works in the deployed service. |

Evidence keys: **F** = [47 frontend Node checks](../tools/frontend_restore.test.mjs), including focused subcases and synthetic DOM rendering (not a browser); **A** = [12 actual Handler/SQLite parity cases](../tests/test_v1_parity.py); **R** = [36 runtime tests](../tools/test_restoration_runtime.py), using synthetic acquisition/fake clocks where stated; **D** = [19 recovery tests](../tools/test_research_recovery.py). The other five application checks cover Node runtime selection and public build identity. Portable evidence, the actual UI01/UI02/UI03 ledgers below and future live verification are distinct stages.

## Data, acquisition and baseline limits

- The **pre-cutover** raw Steam history cache had zero rows. After activation, one normal live public Steam-history job saved **2,408 genuine points**. This verifies one selected item, not broad history coverage. Recovered provider snapshots/aggregate metrics retain their original **21 October** observation times; recovery does not synthesize raw charts or make dated research fresh.
- Actual normal `get_price_history` qualification in a separate temporary database saved **4,638** genuine public Steam observations spanning 2014-02-21 through 2026-11-02 17:00 UTC. A same-origin canonical listing migration and modern SSR JSON parser were repaired. This proves one public acquisition path, not live holdings/history coverage. No credentials, raw HTML or prices were exported in the qualification report.
- History and current wallet requests share persisted Steam admission, pause and Retry-After. Explicit history work continues in the same bounded task after a saved deadline; fake-clock redirect/429 and pause/cancel tests passed. No unconditional startup history sweep was enabled.
- Legacy identity/name/quantity rows match the current backup. Every old imported benchmark/percentage/return field is null in both sources. Those numeric values cannot be recovered from these files; null is unavailable, not zero.
- The published v1 `portfolio_analysis`/`full_report` already had empty signal/category/window/series placeholders. This restoration reconnects existing sections/appendix truthfully; it does not invent trading algorithms and call them original v1 functionality.
- Current cash quote ledger, provenance, provider control and pricing/freshness improvements remain separate from research benchmarks. Real personal data and private recovery artifacts remain local, outside Git and source/test bundles.

## Actual browser ledger: UI01

UI01 used exact application source `7524344fd9e1998c04d52525ec49968a2b3aecc8`,
real sandboxed Chromium, invented fixture data and a 12% CPU/2 GiB owned job.
It finished in 49.5 seconds with zero page errors. All eight observed owned PIDs
exited; port8097 closed and temporary fixture data was cleaned. Peak sampled job
memory was717,471,744 bytes. Existing live source, service and user data were not
changed. Screenshots and detailed receipts remain in the local QA run directory.

| Case | Actual evidence | Classification and next acceptance |
| --- | --- | --- |
| B1 | Tabs, real charts,7/30/90 research periods,7/90 curves and return modes worked; selecting the job strategy failed. | App: five job-field wrappers retained `hidden`; research action was labelled as cash refresh. Wrappers/label repaired while retaining owner guards. Repeat visible fields/label. |
| B2 | Decision note survived an actual reload. | Passed. |
| B3 | Preset add/select/rename/delete and stored cleanup worked. | Passed CRUD; filter semantics also have the separate cloud/VM replay evidence above. |
| B4 | Portfolio lenses, eight sorts, group curve and7-day item drill-down worked. | Passed these exercised flows; unexercised controls remain open. |
| B5 | Independent full-market lenses, eight sorts, group curve and7-day item drill-down worked. | Passed these exercised flows. |
| B6 | Header assertion failed before the four basis/scope combinations could finish. | Harness: CSS uppercased `innerText`. Read DOM text content; repeat actual headers/details without weakening basis checks. |
| B7 | Research global price ordering, bounds and pagination worked. Cash cheap-item ordering failed. | App: cached-quote sorting displaced new server-ranked cheap rows. Preserve global server order during hydration and refresh the page once on actual revision changes; repeat beyond-500 proof in both bases. |
| B8 | Two AND tags, collection/type/grade filters and inline narrowing worked. | Passed. |
| B9 | Actual null-safe TXT download worked; two-holding paste preview failed. | Inherited ten-item generic-import restriction. Native-v2 exports now accept one or more validated rows; generic heuristics retained. Repeat the actual two-holding round trip. |
| B10 | CSV upload, duplicate collapse/merge, preview and invented-portfolio save worked. | Passed; generic-import threshold remains unchanged by the native-v2 repair. |
| B11 | Saved snapshot View, History canvas visibility, report sections and deletion of the invented import worked. | Passed exercised controls. No numeric historical benchmark curve is claimed from null benchmark fields. |
| B12 |320/390px header and reachable table scrolling worked;768px header fit failed. | App: tablet wrapping gap. Targeted<=900px header/tabs wrapping repaired; repeat768 and affected320/390 geometry. |

Normal-entry-point stop qualification and fresh-data cutover were not run in
UI01. They remain separate finite phases; source repair is not a retroactive UI
pass. The focused rerun selects B1/B6/B7/B9/B12 and keeps UI01's seven passing
macro results. Per-row B1-B12 references below use this ledger; an earlier
"pending" note must not be read as a new pass for an unexercised subflow.

## Actual follow-up ledger: CS2STOP02 and UI02

Both phases used exact source `9ed207cbc6710502d78427583e451a8ff90014c8`,
frontend SHA256 `49654018908439ba83aa9b2d95e478a54acd8f52aac76a53df352b5e5ea41d45`
and runtime SHA256 `f3d704bfa840fad9ccefa42cce45fefc92eb4e560006dd2fef0bdbabe7394170`.
They used private invented state, disabled providers and the same 12% CPU/2 GiB
owned-job limits. Neither phase accessed the live service or changed user data.

CS2STOP02 exercised normal `server.py` startup and configured marker shutdown
in 1.672 seconds. The process exited 0, its listener closed, both database quick
checks passed, provider attempts were zero and no owned process remained.
This qualifies the normal stop mechanism in the invented-state setup; live
coordinator cadence, writer closure and cutover still require their own evidence.

UI02 selected only B1/B6/B7/B9/B12. It finished in 24.188 browser seconds
(25.953 controller seconds), with four passes, one assertion failure, no page
errors and no blocked requests. All eight observed owned processes exited,
port8097 closed, temporary data was cleaned and zero processes remained at
release, `2026-11-02T21:23:14.549017+00:00`. Peak job memory was822,194,176 bytes.

| Case | Actual UI02 evidence | Remaining limit |
| --- | --- | --- |
| B1 | Passed five tabs, research periods7/30/90/All(0), curve periods7/90, three return modes and visible job strategy/speed/count/retry controls. | No acquisition job was requested; remaining period options, live task progress and real holdings-history population are not implied. |
| B6 | The corrected header assertion ran; the research Full Market expansion then failed the `.steam-history-box` assertion. | Unresolved: renderer/dataflow versus pending-render readiness is being investigated. No missing-feature diagnosis or pass is asserted from this failure alone. Four basis/scope expansions and raw-chart interaction remain unqualified. |
| B7 | Passed both research and cash: globally cheapest item beyond the alphabetic first500, outgoing min/max bounds, offset500 and return to offset0. | This verifies the exercised price sort/bounds/page path, not every available sort key or live dataset. |
| B9 | Passed actual two-holding null-safe TXT download and pasted reimport preview; identity/quantity were preserved and unknown prices stayed unavailable. | No real holdings were exported or transferred; generic-import threshold remains separately covered. |
| B12 | Passed header bounds and reachable local table scrolling at320/390/768px; invented-data screenshots retained locally. | This is the exercised responsive geometry, not a complete accessibility or every-detail-layout audit. |

UI01 remains the immutable seven-pass/five-fail record on its earlier source.
Its seven passes plus UI02's four passes provide **11/12 macro-case evidence
across two revisions**. They do not establish a complete single-run pass, every
subflow in the74-row matrix, or deployed parity. Raw Steam history in the live
cache was empty at UI02; subsequent live acquisition is recorded below. Recovered
dated metrics are not raw history. Private receipts are retained locally under `cs2-04-qa/run-CS2STOP02`
and `cs2-04-qa/run-CS2UI02`, outside the source/test bundle.

### UI02 B6 diagnosis and prepared correction

The v1 research Full Market renderer contains the complete history panel. The
new positive renderer-to-history test also passes against exact `9ed207c`,
including the exact listing request and 90/7/All datasets in both research
scopes. UI02's failed assertion therefore does not prove a missing renderer.
Its readiness helper could return while a 250 ms filter reload was still queued;
that reload could then replace the just-expanded row.

Two reproducible application state defects were corrected: queued filter reads
now immediately invalidate the accepted key and mark results busy; moving to a
portfolio preset cancels the obsolete market timer. Busy cleanup belongs to the
request generation and cannot clear a newer pending read. Both negative tests
fail on `9ed207c` and pass with the correction; all 31 frontend checks pass.
The subsequent UI03 run exercised B6 and repeated the affected B7 filter/sort/
pagination path. Its actual results are recorded below; the UI02 failure remains
preserved in its original ledger.

## Actual final focused browser ledger: UI03

UI03 used exact source `07d21ec5f912ce70eb90a6505764df1d9025216e`, frontend
SHA256 `ed5a5e65c25268b97b321eded19121628b4efe415c2805a2318c512f6020a570`
and runtime SHA256 `ab01ae2c107846496556b7552296943f9ffafb0766789c219dbab035256892f1`.
It used invented fixture state, real sandboxed Chromium and Chart.js, provider
acquisition disabled, and the same 12% CPU/2 GiB owned-job limits.

| Case | Actual UI03 evidence | Remaining limit |
| --- | --- | --- |
| B6 | All four research/cash and portfolio/market detail combinations passed. Exact saved-history requests and real charts passed in both research scopes. Portfolio datasets were 91/8/91 points for 90d/7d/All; market datasets were 4/2/4. Both cash source panels appeared. | Fixture history is invented; this does not populate the live raw-history cache or prove live provider acquisition. |
| B7 | Both bases again found the globally cheapest item beyond the alphabetical first500; min/max requests, offset500 and return to offset0 passed after the pending-read correction. | Exercises price sorting/bounds/pagination, with separate portable checks for other keys; live data remains unqualified. |

Both selected cases passed with zero page errors and zero blocked requests.
Browser elapsed time was 19.083 seconds; the controller took 20.938 seconds,
used 19.765625 CPU seconds and sampled peak job memory of 717,594,624 bytes.
All eight observed owned processes exited. The fixture listener closed,
temporary data was cleaned and active processes were zero at release,
`2026-11-02T21:43:40.493461+00:00`. Local evidence is retained under
`cs2-04-qa/run-CS2UI03`; no live service or user data was accessed or changed.

Preserved UI01/UI02/UI03 evidence now covers **12/12 macro cases across three
source revisions**. It is not a single full-suite rerun or proof of every
subflow in the 74-row matrix. Backend startup/stop code is unchanged from the
passing CS2STOP02 run. The subsequent authorized fresh-state activation and live
HTTP/provider acceptance are recorded below.

## Actual activation and live acceptance

The original CS2 host stopped normally at **22:43:55 UTC**. One fresh working
clone was prepared directly from the closed original databases; no redundant
full-database backup was created. The original source and data remain retained.
The clone passed integrity checks, unique personal/settings/identity preservation
checks and complete current-ledger schema/content comparison. Additive research
recovery preserved its source observation times and all protected state.

Exact `07d21ec5f912ce70eb90a6505764df1d9025216e` started at **22:48:21 UTC**
on the same LAN URL and access policy. Both localhost and LAN served matching
frontend bytes and the expected 43-file build identity: runtime SHA256
`ab01ae2c107846496556b7552296943f9ffafb0766789c219dbab035256892f1`, frontend
SHA256 `ed5a5e65c25268b97b321eded19121628b4efe415c2805a2318c512f6020a570`.
Only CS2 was restarted. Existing credential paths were reused; no credentials
were copied or installed.

All **20 cached-GET acceptance checks passed** at 22:50:57 UTC. They covered
served identity on both hosts; SQL and API preservation of unique holdings,
settings and identity state; research inventory; current valuation agreement
with the canonical ledger; independent portfolio/market taxonomy; ascending
market price slices in both bases; recovered research availability; and both
HTML reports. The checked responses and private preservation proofs remain local.

Authenticated CSFloat was configured, qualified and successfully captured fresh
data after restart. A separate normal, non-forced Steam-history job selected one
item, followed a canonical redirect, waited for the saved shared admission
deadline, resumed the same job and completed: one item refreshed, zero failures,
and **2,408 genuine raw points** returned through the cached history endpoint.
The two attempts include the canonical redirect; this was one selected item.
The history request did not freshen the current wallet quote's capture time.

`STEAM_COOKIE` is empty, so this proves the public Steam acquisition path;
authenticated Steam history remains unqualified. Wider history coverage remains
open. Full 74-row acceptance is not implied by the HTTP/provider results. Sanitized receipts are retained locally
under `cs2-04-private/activation-07d21ec5`; private data is excluded from this document.

### Actual read-only live browser ledger: CS2LIVE01

CS2LIVE01 verified exact `07d21ec` identity, then exercised the existing service
through real sandboxed Chromium. It did not start or stop the service, write
user data, request provider acquisition, capture screenshots or download files.

| Step | Actual evidence | Status and limit |
| --- | --- | --- |
| LIVE-NAV-GROUPS | Waiting for an expandable grouping row timed out after 9.101 seconds. The exact failing tab was not recorded. | Open and unclassified: source/data versus harness readiness requires focused triage. No application defect or pass is inferred from this timeout alone. |
| LIVE-ITEM-BASES-SCOPES | All four research/cash and portfolio/market detail combinations expanded. Both research history panels truthfully showed unavailable history for the checked rows; both cash saved-source panels rendered. | Passed the exercised live details. These rows differ from the single item populated by the separate public-history qualification; unavailable row history is not a claim that the entire cache is empty. |
| LIVE-MOBILE-GEOMETRY | Header fit and reachable local table scrolling passed at 390 and 768px. | Passed the exercised geometry; a complete accessibility/every-detail-layout audit is not implied. |

There were zero page errors, blocked mutation attempts, downloads or screenshots.
The harness blocked 434 external asset requests under its same-origin policy;
this is not evidence of 434 application failures. Browser closure was confirmed.
The owned job released with zero processes after 32.5 controller seconds at
**22:58:30 UTC**; the persistent live CS2 service was left running. The grouping
timeout remained open until the separate focused NAV01 run below.
Local QA receipts: [technical results](../../cs2-04-qa/run-CS2LIVE01/technical-evidence/results.json)
and [owned-job release](../../cs2-04-qa/run-CS2LIVE01/job.json). They record no
personal values, item names or portfolio identities.

### Focused live navigation diagnosis: CS2NAV01

NAV01 used the same live `07d21ec` source and recorded both response timing and
rendered state. All five tabs passed. Full-market collection rows became
visible after 12.177 seconds; portfolio rows after 3.868 seconds. Both scopes
returned HTTP 200 with populated collection data, matching accepted cache keys,
visible expandable rows and no stale/generic analysis error. Read generation
remained zero. The original 9.101-second wait was too short for this live load;
this was not an empty dataset or a reproduced product failure.

Browser closure was confirmed with zero page errors, mutations, acquisition
reads, screenshots or downloads. The job released all owned processes after
22.39 controller seconds at **23:10:25 UTC**, leaving the service running.
Evidence: [navigation results](../../cs2-04-qa/run-CS2NAV01/technical-evidence/results.json)
and [release receipt](../../cs2-04-qa/run-CS2NAV01/job.json). Earlier item/mobile
passes remain preserved. TAX-STALE is a separate source-reproduced completion
race; it did not cause this timeout and is not yet applied to the live source.

## Feature matrix

### Focused cloud review corrections

These findings reopened the affected source claims. The exact regression
fixtures now pass; real-browser and fresh-clone execution remain separate stages.
No finding establishes that original user databases were modified.

| Stable follow-up | Finding | Owner and status | Required evidence |
| --- | --- | --- | --- |
| CS2-04/PRESET | Research portfolio preset controls update but table rebuild skips full search/metadata/price filters. | Fixed in `8406b468547ca03bf06eb05cbd97cc7443798c77`. | F actual select/apply/build/filter replay passes positive, negative, boundary, null-price and metadata cases, with inverted cash quotes, AND tags and exclusions. |
| CS2-04/SELECTION | A pending inventory request can prevent the newly selected snapshot from issuing its own request. | Fixed in `8406b468547ca03bf06eb05cbd97cc7443798c77`. | F delayed A-to-B-to-A tests use actual load/reset/build functions; stale success, error and cleanup cannot affect the new request owner. |
| CS2-04/ALL | Inherited Full Market query fallback changes All (0) into 90 days. | Fixed in `8406b468547ca03bf06eb05cbd97cc7443798c77`. | F query, cache and selector behavior preserves 0 versus ordinary 90; inherited bug is distinguished from restoration regressions. |
| CS2-04/CHART-FALLBACK | Missing-Chart fallback expects a canvas while restored callers also pass a 2D context. | Fixed in `8406b468547ca03bf06eb05cbd97cc7443798c77`. | F both fallback inputs target the actual canvas and note; normal Chart receives the original input. |
| CS2-04/RECOVERY-TRIGGERS | A research-table trigger can change protected clone settings while the receipt claims no personal changes. | Source fixed; protected-state proof required before commit. | D inherited pre-clone research/receipt triggers are refused; authorizer and independent post-write protected-state proof roll back unexpected writes. Normal FTS/idempotence pass. |
| CS2-04/RECOVERY-WAL | Derived data committed in WAL can change recovered metrics without changing the recorded main-file hash. | Source fixed; closed inputs and actual snapshot identity enforced. | D committed open-WAL fixture is refused without changing source/main/WAL/SHM/target; closed WAL-header input works, and derived queries use the recorded snapshot. |
| CS2-04/TAX-STALE | A matching history-job completion can invalidate a delayed taxonomy read; old builders then display a permanent stale error or overwrite a newer view. | Source fix tested and reviewed; activation pending. This is a separate confirmed race, not proven causality for LIVE01. | F actual completion-handler/cache/loader/builder replay proves one current visible-owner retry, changed scope/window/hidden-view suppression, bounded repeated invalidation, genuine HTTP failures and overlapping builder ownership. |

The cloud review also reported that 18 anonymous mutation/forced-read negatives
left its invented main and ledger table digests unchanged. That positive result
does not replace the focused corrections above.

Rows have the portable source coverage/limits listed and are included in the confirmed main publication above. Warm/Sync now use the continuing explicit job: Warm is bounded to 80 selected-portfolio items; Sync All batches only the selected portfolio, preserves cached history as its resume checkpoint, and reports an error rather than silently truncating scopes above 10,000. Browser evidence is limited to the exact UI01/UI02/UI03 flows above. The activation ledger adds only its listed live HTTP/provider evidence; other subflows and the live browser pass remain **pending**. A per-row historical "B pending" or "Live pending" note is qualified by these ledgers, never evidence of an unexercised interaction passing. "F source" is a source contract, not proof of each interaction. Root integration owns final live acceptance for every row.

| ID | Published v1 control / contract | Audited live intake | Isolated restoration | Owner | Portable evidence and remaining acceptance |
| --- | --- | --- | --- | --- | --- |
| NAV-01 | Overview, Market, Breakdown, Items, History tabs | Retained outer tabs; their analytical contents differ substantially | Five original tab regions and research widgets mounted; current additions retained. | Frontend | F shells and synthetic render; all five tabs activated in UI02 B1. Live pending. |
| NAV-02 | Portfolio selector loads saved snapshot | Retained `#snap-select`, current cash reset/fencing added | Saved selector and current holdings/revision fencing retained; selected scope feeds separate research/cash reads. | Frontend + Backend | F render and A scope contracts; B1 snapshot switching pending. |
| NAV-03 | Global period/volatility selector: 1d,3d,7d,30d,90d,180d,1y,All; 90d default | `#global-scale` remains but is hidden; replacement text says current cash/observed history | Original selector visible. Explicit refresh materializes all 10 supported windows, including genuine All-history. GET remains readonly; empty raw input preserves recovered dated metrics. | Frontend + Backend | Two final targeted R tests passed: 401-day synthetic history, distinct short/long/All priors, every selected window exact on real GET with write/rebuild paths forbidden, and empty-recovery preservation. Full option interaction/coverage B1 pending. |
| NAV-04 | Data button/menu, outside-click/Escape dismissal and positioning | Retained source | Data menu and original controls reconnected; cash refresh is separate. | Frontend | F source/shell coverage; actual dismissal/positioning B1/B12 pending. |
| NAV-05 | Report link opens `/report` in new tab; `rel=noopener` | Retained link; report content changed | Research report link restored; current report has its own route and semantics. | Backend + Frontend | A/R HTML/JSON reports and appendix; real popup B11 pending. |
| NAV-06 | Sign out; API401 redirects to `/login` | Retained; anonymous read-only banner/sign-in added and mutation controls hidden | Readonly banner, sign-in/sign-out and owner mutation guards retained. | Backend + Frontend | A owner auth/settings/logout and readonly mutation denial; actual UI auth pending. |
| NAV-07 | Login username/password/submit, busy/error state | Retained server-rendered login shell | Original login page/handler retained; no production credentials changed. | Backend | A real Handler login/logout with synthetic owner; browser login pending. |
| DAT-01 | Pull Market Data refreshes providers, metrics and priority charts | Changed to Refresh cash prices; historical bulk filling retired | Explicit provider/full-pull jobs call real metrics/lookup builders; current cash refresh remains separate. | Backend + Frontend | R actual builders with synthetic acquisition, failures not complete; B1 control and live acquisition pending. |
| DAT-02 | Refresh Status | Retained, current-source status replacement | Status reads retain current provider age/error/coverage and research capability context. | Backend + Frontend | R capability/read contracts; actual status UI B1 pending. |
| DAT-03 | Pull strategy: missing data, provider snapshots, Steam history, full pull + priority charts | Options remain in `.data-ops-panel`, but current source renderer hides panel | All original strategy options mounted and accepted by explicit task worker. | Backend + Frontend | F shell; R provider/history worker contracts. UI02 B1 selected provider snapshots and verified start caption; other strategies/job execution remain pending. |
| DAT-04 | Speed: gentle background / release catch-up | Old control remains in hidden data panel | Original gentle/catch-up options retained; shared durable Steam admission still controls requests. | Backend + Frontend | F source; R shared gate/429/deferred continuation; gentle option selected in UI02 B1. Catch-up/runtime provider behavior pending. |
| DAT-05 | Charts/cycle numeric0-500 step10 default40; cycles1-12 default2; retry cooldown checkbox checked | Old controls remain in hidden panel; start button relabeled cash refresh | Original bounded item/cycle controls and retry option retained; same task resumes at saved provider deadline. | Backend + Frontend | R redirect wait/resume, 125-second 429 fake-clock wait, pause/cancel; UI02 B1 filled charts0/cycles1 and toggled retry off/on. Other boundary UI behavior and live jobs pending. |
| DAT-06 | Active task strip, stage/progress/events, provider cards, coverage meters, latest pull | Current-source cards/capture ages and errors retained; legacy strip hidden, some original labels survive | Research task strip/events and current-source cards are separate; error/pending/partial are not called complete. | Backend + Frontend | R task outcome/progress contracts; actual progress rendering B1 pending. |
| DAT-07 | Background restart/resume/periodic refresh experience | Current quote coordinator, per-provider cadence, fresh/stale/missing states | Current coordinator preserved. Research automatic refresh/startup resume are explicit configuration opt-ins; saved tasks remain traceable. | Backend + Integration | R existing ledger/control preservation; CS2STOP02 normal startup/marker-stop passed. Original live writer closed normally during activation; authenticated CSFloat has successful post-restart captures. Extended cadence/polling behavior remains unqualified. |
| OV-01 | Headline Portfolio Value, Reference Value, Period Return and Total Items | Current cash covered subtotal and item count; prior/return cards removed; explicit partial coverage added | Separate research value/reference/return/item headline and canonical current cash headline; null priors retained. | Frontend + Backend | F headline separation/nulls; A mathematical scope/basis tests; B1 pending. |
| OV-02 | Allocation doughnut and category total-value bar charts | Missing; replaced by current cash allocation table | Research allocation doughnut/bar canvas and table restored beside current cash allocation. | Frontend | F mounted canvases/full synthetic Overview; real Chart.js B1 pending. |
| OV-03 | Allocation table sortable Group,Items,Value,Return,Avg,Vol7d,Vol30d,Vol90d,Up,Down | Current table has Category,Covered cash value,Covered share,Rows priced; no sort controls | Original research allocation columns/sort roles restored; current coverage table retained separately. | Frontend + Backend | F synthetic Overview and A research totals; actual all-header ordering B1 pending. |
| OV-04 | Portfolio Price Curve period:7d,30d,90d,180d,1y,All | Missing control/canvas; forward observed subset SVG added | Original portfolio curve period controls/chart path reconnected; raw dated history only, explicit missing coverage. | Frontend + Backend | A real quantity-weighted dated curves/no backfill; F readonly cached GET; B1 history rendering pending. |
| OV-05 | Warm Portfolio History + status/confidence/backfill indication | Mounted button/confidence panel removed; old functions remain disconnected | Warm-history control and evidence panel restored to explicit shared-admission job path. | Backend + Frontend | R same-job history continuation and TEMP public history qualification; B1 control/live population pending. |
| OV-06 | Action Brief from portfolio trend/risk/current data | Missing historical brief; current decision count summary replaces it | Original brief widget/model path restored; preserved current decision summary stays separate. | Frontend + Backend | F synthetic Overview; original v1 signal-model placeholders remain a baseline limit. B1 pending. |
| OV-07 | Opportunity Workflow five steps and local Decision note textarea | Entire card/textarea missing; save/hydrate functions and storage key remain | Workflow/Decision note textarea remounted using original storage key. | Frontend | F storage-key/source contracts; actual save/reload B2 pending. |
| OV-08 | Risk Controls: concentration, liquidity, source confidence/history coverage | Simplified largest-category/holding and missing-input narrative | Original risk widget/model path remounted; current coverage/concentration context retained. | Frontend + Backend | F render; unavailable liquidity/cost/fees not invented. Actual risk rendering B1 pending. |
| OV-09 | Sell,Buy/watch,Data-risk,Momentum,Mean-reversion ranking panels | Only current ask-versus-7d-sale Buy/watch and Sell review lists mounted | Original ranking widget roles mounted alongside current cash review lists; no new trading signal algorithm invented. | Frontend + Backend | F render; original v1 empty signal fields remain unavailable. Real evidence/UI B1 pending. |
| OV-10 | Highest-volatility holdings and 7/30/90 metrics | Missing mounted chart/panel; individual short hourly variability evidence exists | Original volatility widget and 7/30/90 research roles reconnected, separate from observed current variability. | Frontend + Backend | A genuine-window metrics; F synthetic render; real chart B1 pending. |
| OV-11 | Group return comparison: split top10/bottom10,Top20,Bottom20; keyword summary | Entire chart/options/keywords missing | Three original group-return chart modes and keyword shell restored. | Frontend | F mounted controls/render; actual mode changes/chart geometry B1 pending. |
| OV-12 | Significant Gainers,Declining Positions,Largest Movers by significance,Gainers/Losers by dollar return | Mounted lists removed; current ask-sale review is a different signal | Original gainers/decliners/movers panel roles restored; current ask/sale review remains separately labelled. | Frontend + Backend | F render; A genuine dated movements. Actual lists B1 pending. |
| OV-13 | No-item/import fallback and loading states | Retained/reworked metadata/cash/read errors; missing widget shells can mask loss | Research/current empty and loading/error shells coexist; unavailable prior values do not become zero. | Frontend + Backend | F null/render tests and A empty history/startup; real failed-request UI B1 pending. |
| OV-14 | None: canonical current cash refresh, current review reload, completed-sale details | New `#portfolio-cash-read`, Reload analysis, Refresh price evidence, reference details | Later current cash reload/refresh/reference/observed features retained. | Frontend + Backend | F endpoint/headline fencing; A separate canonical basis; actual current controls B1/B6 pending. |
| GRP-01 | Breakdown lenses Collections & Cases / Item Type / Tournament & Event | Three controls retained; current-only values/coverage rows | All three original portfolio grouping lenses and research tables/charts restored; cash grouping remains separate. | Frontend + Backend | F both-scope synthetic render; A group scopes; B4 pending. |
| GRP-02 | Market separate three grouping lenses | Entire Market lens controls missing; browse-current-items link added | Independent three-lens one-each market controls restored. | Frontend + Backend | A market basket distinct from holdings; F market render; B5 pending. |
| GRP-03 | Trend summary/title/subtitle and four scope KPIs | Breakdown changed to current covered estimate/coverage/full estimate/sale-unavailable; Market basket/prior coverage/return KPIs missing | Original research summaries/KPIs restored in both scopes with coverage and dated basis. | Frontend + Backend | A basket mathematics and F both-scope render; B4/B5 pending. |
| GRP-04 | Highest Volatility Items log chart + Biggest Group Movers weighted-return chart | Both charts removed for both scopes | Original volatility and weighted group-mover charts/hooks mounted for both scopes. | Frontend + Backend | F canvas/render and A dated metrics; real chart lifecycle B4/B5 pending. |
| GRP-05 | Real+estimated / Real-only volatility modes; include-thin checkbox; minimum price>=0; minimum coverage0-100 | Mounted toolbar removed for both scopes; stale shared state/helpers remain | Original volatility basis/thin/min-price/min-coverage toolbar restored in both scopes. | Frontend + Backend | F source/mounted controls; actual filtering combinations B4/B5 pending. |
| GRP-06 | Sort buttons Value,$Return,%Return,Vol7d,Vol30d,Vol90d,Items,A-Z | Removed for both scopes; current Breakdown fixed by covered value | Original group sort buttons reconnected for both scopes. | Frontend | F both-scope synthetic render; actual all-sort/null ordering B4/B5 pending. |
| GRP-07 | Sortable group table headers Name,Items,Priced,Value,Reference,$Return,%Return,Vol7,Vol30,Vol90,Up | Current table only Group,Covered cash,Covered share,Rows priced, no sort/expand | Original research group table columns and expansion restored beside cash summary. | Frontend + Backend | F tables/render and A group totals; actual header interaction B4/B5 pending. |
| GRP-08 | Expand row:current/reference/prior coverage/return/avg/vol/breadth; group Value Over Period; highest-volatility item lines with7/30/90/180/365/All selector; representative movers/provider links | Current group rows do not expand; old detail functions remain disconnected | Original lazy group detail/value curve/item-lines/period controls reconnected with separate scope. | Frontend + Backend | A genuine scoped group/item series; real expand and period interaction B4/B5 pending. |
| GRP-09 | Full-market one-each basket analytics and market taxonomy API | Current Market is selected-holdings opportunities + provider cards; compatibility route maps to portfolio | Original full-market research API restored; no selected-portfolio substitution. | Backend + Frontend | A distinct one-each basket and group contract; F market render; B5 pending. |
| GRP-10 | Provider and data freshness context | Current Market provider cards are active and richer; browse current items navigation retained | Current provider/freshness cards and browse-current navigation retained alongside research. | Frontend + Backend | F source/render; R saved status/admission; actual cards B5 pending. |
| IT-01 | Portfolio / Full Market modes | Retained | Portfolio/catalog scope works within explicit research and current-cash bases. | Frontend + Backend | F separate render/query paths; A distinct scopes; UI03 B6 passed all four basis/scope detail combinations. Live pending. |
| IT-02 | Inline search; Enter adds AND tag; Backspace removes last; chip x removes | Retained | Existing inline/AND tag search and keyboard/chip behavior retained. | Frontend + Backend | F query/render coverage; A global filter contract; actual key/chip interactions B8 pending. |
| IT-03 | Exclusion tags (OR hide), persisted shared settings; count/chip removals | Retained for owner; hidden in public read-only view | Owner exclusion settings retained; readonly visitors cannot mutate them. | Backend + Frontend | A owner settings roundtrip/readonly mutation denial; actual chips/exclusions B8 pending. |
| IT-04 | Advanced filters details, All Collections/All Types/All Grades + data-driven options | Retained; already collapsed in historical Market; each reload resets open-state | Data-driven advanced filters retained; response no longer forces disclosure closed. | Frontend + Backend | F explicit disclosure regression test; A filters; B8 pending. |
| IT-05 | Min$/Max$ input, Clear All (intentionally retains exclusions) | Retained controls; Market omits bounds from request and applies them only to loaded500 rows | True min/max sent to both market endpoints; filter before limit/offset, clear semantics retained. | Backend + Frontend | F outgoing query and A/R global 510-item bounds; UI02 B7 passed actual bounds/request/rows in both bases; UI01 B8 exercised filter clearing. Live pending. |
| IT-06 | Live Prices / Sync Steam in Portfolio; Pull Market Snapshot in Market | Changed to Refresh prices; Steam Sync hidden; current cash acquisition replaces legacy synchronization | Original research sync/pull controls restored and separate current refresh preserved. | Backend + Frontend | R original route contracts/shared gate; F control source; actual B6 acquisition controls pending. |
| IT-07 | Quick presets:Full Market,Knives,Gloves,Cases,Stickers,Covert,Under$5 | Same seven mounted presets, but movementScore unavailable and Under$5 suffers partial-slice bounds | Seven quick presets retained; Under-$5 now uses whole-match server bounds in either basis. | Frontend + Backend | F preset state and A global filter/sort; actual each preset B3/B7 pending. |
| IT-08 | Manage Presets:icon(*,$,^,!,#,K,G,C,S,R),name,Add Current Filters,Rename Selected,Delete Selected,inline x; system presets protected | Retained, localStorage key unchanged; manage hidden in readonly mode | Original preset storage/protection/manage controls retained. | Frontend | F original key and state contract; actual add/select/rename/delete/reload B3 pending. |
| IT-09 | Captured preset state includes mode,search text/tags,collection,type,grade,min/max,sort key/dir | Retained old schema; new held-review filter is not in old capture schema | Preset state includes explicit price basis while preserving existing fields/key compatibility. | Frontend | F basis capture/storage contract; actual old/new preset roundtrip B3 pending. |
| IT-10 | Portfolio sortable columns:Item,Grade,Type,Move%,Move$,Valuation Price,Steam,Steam Premium,CSFloat,BUFF163,Reference Price,Qty,Holding$,Vol7/30/90,Source,Updated | Only Item,Grade,Type,Cash price,Qty,Cash holding,Source,Freshness mounted | Original provider/movement/reference/volatility portfolio columns restored in research basis; cash columns retained. | Frontend + Backend | F separate renderers and A research inventory; UI03 B6 passed basis headers and both portfolio details. Every individual column/sort and live data are not implied. |
| IT-11 | Market sortable item columns include movement/provider/reference/volatility alongside identity/current price | Only Item,Grade,Type,Cash price,Source,Freshness mounted | Original market research comparison columns restored; current cash table separate. | Frontend + Backend | F separate renderers and A market basis; UI03 B6 passed both market detail bases; B7 price sort/bounds/paging passed. Other individual sorts/live pending. |
| IT-12 | Header ascending/descending sorts; server receives sort/bounds before500 limit | Price/source/time/holding requests remapped to name; client sorts alphabetic500 slice; unknown sort keys fall back to cash price | Both APIs globally sort requested price/source/time before offset+limit; frontend forwards true keys. | Backend + Frontend | R price/source/timestamp ordering and A 510-item case; F query; UI02 B7 passed real network price ordering/bounds in both bases. Every other sort/live dataset pending. |
| IT-13 | Portfolio300 / Market500 display caps and narrow-search messages | Caps retained; no next-page control found | Bounded Previous/Next 500-row pages expose later matching rows with hasMore/offset metadata. | Backend + Frontend | A/R second-page/global rank and F offsets; UI02 B7 passed actual offset500 and return0 in both bases. Live pending. |
| IT-14 | Row expansion shows identity,provider prices/divergence,reference/movement/volatility metrics and raw Steam chart; external Steam listing link | Current source offers,current quote basis/freshness,completed-sales references,observed closes,current review,Steam wallet ask; no raw chart | Research detail/provider/movement/raw-chart roles restored beside current offers/references/wallet details. | Frontend + Backend | A scoped raw history and F basis separation; UI03 B6 passed all four expansions and research charts. LIVE01 passed all four detail scopes and truthful unavailable-history states. A separate live public-history job and cached response passed. |
| IT-15 | Raw Steam chart selector7/30/90/180/365/All, partial/error/raw-edgecase warnings | Old select/function remains in source but `canonicalHistoryHtml()` returns empty string | Raw Steam chart/period selectors reconnected; modern canonical SSR parser handles exact app/name/USD and legacy format. | Backend + Frontend | R parser variants and TEMP 4,638-point acquisition; UI03 B6 passed default-90/7/All datasets in both scopes. A normal live public-history job added 2,408 genuine points and its cached read passed. Wider coverage, authenticated Steam and live browser selection remain pending. |
| IT-16 | None:current held-review filter All,Buy/watch,Sell/review,Current data needed,More evidence needed,Price reference available,No usable evidence | New active select; directional review filters apply to held scope, reference/no-evidence checks apply generally | Later held-review filter retained with scope-specific labels and availability rules. | Frontend + Backend | F complete script/render contracts; actual every review option B6/B8 pending. |
| IT-17 | None:Fetch current Steam ask; Queue fresh CSFloat ask | New owner-only per-item acquisition controls with pacing/pause/busy guards | Later explicit current Steam/CSFloat requests retained; history shares Steam origin gate without freshening wallet quotes. | Backend + Frontend | R shared gate/pause/429 tests; actual item acquisition/control pending. |
| IT-18 | Responsive item table and thumbnails, refresh status | Current table slimmer, lazy source details; synchronous full-holdings lookup remains expensive | Cash rendering indexes accepted holdings once per synchronous render; mutation fencing preserved, duplicate sort removed. | Frontend | F index/mutation regression test; actual latency/mobile B12 pending. |
| IO-01 | +Import / +Add Portfolio; modal close x, outside click; Paste Text / Upload File tabs | Retained; mutations hidden/guarded for readonly visitors | Original import modal/paste/upload controls retained with owner guards. | Frontend + Backend | A mutation denial; F mounted source; actual modal/focus/file transitions B10 pending. |
| IO-02 | Text parsing after10+ recognizable items; auto paste parse; CSV/TXT file chooser and drag/drop | Retained parser paths and .csv/.txt accept | Text/TXT path retained; CSV now supports LF/CRLF, quoted commas/escaped quotes and absent financial fields. | Frontend | F parser synthetic fixtures; actual upload/drop/error UI B9/B10 pending. |
| IO-03 | Import preview items/values/categories/source; label; nonempty/case-insensitive unique-name validation; Save Portfolio | Retained; identity/qty imported while price fields are null/untrusted | Preview/label/identity-quantity save path retained; imported prices remain untrusted. | Frontend + Backend | A real Handler import/persistence; F parser; actual preview/save B10 pending. |
| IO-04 | de8/e725 had no duplicate normalizer; f501+ collapses exact duplicates, merges same-name rows, rejects invalidqty, shows duplicate count | Latest duplicate normalizer retained | Latest f501+ deduplication/invalid-quantity fix retained rather than reverting root-v1 bug. | Frontend + Backend | F duplicate normalizer and A exact-identity import; actual duplicate preview B10 pending. |
| IO-05 | Server-saved portfolios, migration from older localStorage `cs_snaps`, cross-device polling and selector updates | Retained/reworked holdings-fingerprint/revision checks | Saved snapshots and legacy storage migration/fencing paths retained. | Frontend + Backend | A import/state and F key/fencing source; actual migration/cross-device polling remains unverified. |
| IO-06 | History: saved cards/date/items/import benchmark value/return, View,Export,Delete; deletion guarded and at least1 preserved by UI | Retained card controls; explicit imported-benchmark labels added; owner permissions changed | History cards/View/Export/Delete retained; imported benchmark labels remain explicit. | Frontend + Backend | A persistence/auth and F render; actual View/Delete of invented snapshot B11 pending. |
| IO-07 | History snapshot value trend with2+ snapshots, missing-data fallback | Retained benchmark canvas; Chart.js missing fallback improved | Snapshot benchmark chart retained; null financial fields remain unavailable, genuine zero retained. | Frontend + Backend | F null formatters/render; actual 2-snapshot Chart.js B11 pending; old real benchmark values are unrecoverable. |
| IO-08 | TXT export of selected snapshot, download/reimport format | Export wired; no historical CSV export found. Function assumes numeric pct and values; null-price import can challenge that assumption | TXT v2 null-safe N/A export round-trips identity/quantity, numeric/sign and Unicode legacy inputs. | Frontend | F nullable and numeric TXT roundtrips; UI02 B9 passed actual two-holding download/reimport preview with identity/quantity and unknown-value preservation. Live pending. No v1 CSV-export feature was found. |
| IO-09 | Local Decision note persists by `cs2dash_decision_note_v1` | Helpers/key preserved but textarea unmounted | Original Decision note textarea/key restored; no existing key rename/reset. | Frontend | F storage-key contract; actual save/reload B2 pending. |
| IO-10 | Report:status/coverage; strength/weakness/reversal up/down; category momentum;duplicate windows;full appendix | Current report offers cash buy/watch,sell review,all-held items and reference/missing-evidence columns; old sections gone | Original research report sections/appendix restored; current report stays distinct. Original empty signal/category/window fields remain baseline placeholders. | Backend + Frontend | A real appendix/separate semantics and R null formatting; actual report B11 pending. |
| IO-11 | `/api/report` JSON and `/report`/`report.html` HTML share report semantics | Current compatibility dispatcher maps all to selected current analysis | Research JSON/HTML routes restored; current JSON/HTML use explicit current routes. | Backend | A real Handler report contracts; actual popup B11 pending. |
| UX-01 | Responsive header stack,wrapped tabs,2columnstats,stacked grids,compact cards/presets at900/700/600/560 breakpoints | Largely retained CSS | Header/tabs/chips/headings wrap; original responsive shells retained. | Frontend | F CSS/source checks; UI02 B12 passed physical header geometry at320/390/768. Other layout subflows/live pending. |
| UX-02 | Portfolio allocation mobile row presentation / group detail responsiveness | Allocation changed to generic pulse table; current tables may exceed viewport | Wide allocation/group tables use reachable local horizontal scroll rather than page clipping. | Frontend | F overflow rule regression check; UI02 B12 passed reachable local table scrolling at320/390/768. Expanded group/detail fit and live pending. |
| UX-03 | Items fixed table,wrapping names,thumbnails,details | Retained wrapping; details/source/sales tables need independent fit | Items/source/details tables wrap and scroll locally. | Frontend | F CSS/render; UI02 B12 passed item-table scroll reachability; UI03 B6 passed desktop details in all four combinations. Expanded mobile-detail fit/live pending. |
| UX-04 | Local notes/presets editable with keyboard; header sorting/row expansion primarily click handlers | Existing accessibility debt, not proven new regression | Keyboard/ARIA support added for sortable headers, presets and expandable rows. | Frontend | Source inspected; actual keyboard/focus/sort state acceptance pending. Do not claim complete accessibility audit. |
| UX-05 | Empty/loading/error states and stale response prevention | New abort/timeouts/revision fences improve reads; may trigger repeated heavy rendering | Existing abort/timeouts/revision/mutation fences retained; render indexing avoids redundant full-holdings scans. | Frontend + Backend | F mutation/index and complete-script tests; actual asynchronous race/error UX not fully qualified. |
| UX-06 | Chart destroy/recreate and scope/window transitions | Legacy chart helpers remain; restored widgets must deliberately reconnect them | Restored chart hooks reconnect original canvases; actual missing history stays empty; Chart.js failure has fallback. | Frontend + Backend | F shells and A no-fabrication contracts; real chart teardown/CDN/window transitions B1/B4/B5/B11 pending. |
| UX-07 | Auto portfolio sync and current status polling | Retained; additional current-analysis15s polling | Existing auto-sync/current status polling retained; research/current reads use separate render paths. | Frontend + Backend | F wiring/fencing source; actual hidden-tab request counts/render-storm/latency remain unverified. |

## Completion and next execution

1. Completed: published tested upstream/site integration; preserved 12/12 fixture macro-case coverage across UI01/UI02/UI03; qualified normal startup/stop with unchanged backend code.
2. Completed: authorized CS2-only activation using one fresh working clone and additive research recovery, with original source/data retained. Unique state, the complete current ledger and all 20 cached-GET live checks passed. No redundant full-database backup was created.
3. Completed: successful post-restart authenticated CSFloat capture and one normal public Steam-history job, including persisted wait/resume and 2,408 genuine points. Wider coverage and authenticated Steam remain distinct qualifications.
4. Completed live browser scope: retain four item detail scopes and 390/768 geometry from LIVE01; NAV01 passed all five tabs and visible populated groups. Its timing diagnosis closes the earlier grouping timeout. Publish and activate the separately tested TAX-STALE correction when publication is permitted. Keep unexercised subflows and genuine prerequisites explicit; the entire 74-row matrix is not inferred complete from aggregate counts. Preserve the latest accepted writes before any future rollback.

See [cutover procedure](DEPLOYMENT-RESTORATION.md) and [additive recovery procedure](RESEARCH-RECOVERY.md). No financial trade, destructive reset or public exposure is part of this work.
