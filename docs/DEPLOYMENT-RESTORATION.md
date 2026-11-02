# CS2-04 restoration cutover

This is a deployment design and checklist, prepared 2026-11-02. No cutover,
process stop, provider request or data promotion was performed to write it.
The root integration worker owns execution and the final private receipts.

The application entry point after cutover is
`D:/Projects/site/vendor/CS2-Dash/server.py` on the existing port **8089**.
`state/cs2-runtime.json` selects the Python executable, this stable source root,
the newly prepared data directory and the exact upstream source commit. Preserve
the entire old vendor checkout, including `lean_host`, and its original data
intact in a private named rollback location.

## What must be resolved before stopping CS2

Publish the guarded site integration before the restored upstream main. Its
Docker recipe explicitly requires `restoration_runtime.py`, which the old v1
source lacks, so an intervening build fails before replacing a running image.
Record the exact upstream commit in the integration commit and review manifest.
The prior integration patches also fail forward and reverse checks against the
restored source, preventing an old sync recipe from building a partial package.
Push the upstream immediately after the guarded integration; verify both remote
main identities. Publication is separate from the explicit CS2-only cutover.
Do not run generic sync against the current dirty Windows vendor directory.

- [ ] Record the final tested and pushed **CS2-Dash main commit** and the site
  integration main commit. Save the expected build-identity object from the exact
  deployed root files (`build_identity.capture`); do not substitute a branch name
  or the old v1 baseline commit for the new release identity.
- [ ] Preserve the deployed-only source, current runtime profile, relevant CS2
  launcher hunks and private launch configuration outside Git. Retain the existing
  preservation backups; they are rollback evidence, not final promotion inputs.
- [ ] Verify the actual 8089 listener PID, executable, command/source path, data
  path and current quote-ledger path in the deployment host context.
  Match process creation identity as well as PID before acting. PID files
  `dev-cs2.pid` and `cs2dev.pid` are hints, not ownership proof.
- [ ] Identify the CS2-only refresher named by `dev-cs2-refresh.pid` and any
  confirmed CS2 supervisor that could restart it or the old host. Record its
  source/creation identity and how to prevent restart during cutover.
- [ ] Resolve a new, empty, private destination for the **final** data clone and
  the retained derived database/archive accepted by `research_recovery.py`.
  Record those resolved paths and hashes in a private receipt. An older
  `candidate02` or rehearsal clone must never replace current personal state.
- [ ] Confirm sufficient disk space for the two backups, research recovery and
  rollback retention. Acquisition enforces a 10 GiB free-space floor on both
  application and quote-ledger volumes. Validate the runtime profile's Python
  executable/hash and SQLite `deserialize` support; resolve the existing Node
  executable required by cash transports without invoking any provider.
- [ ] Establish the rollback rule for writes accepted after restart: retain and
  back up that latest state before changing data directories. The old lean
  host's `activation.pending` file does not gate restored `server.py`. A new
  maintenance gate is not required for this pre-user development deployment.

Keep exact process IDs, creation times and ownership observations in a local
deployment receipt. Recheck ownership immediately before action; a prior
observation is insufficient to identify a future process reusing a PID.

The root wrapper checks `DATA_DIR/lean-current.json` and delegates to
`lean_runtime.serve`; the live frontend/source is consequently `lean_host/app`.
That loader chooses `dataDirectory/prices-current.sqlite` when present, rather
than the older fallback cash path supplied by the wrapper.

Reconfirm effective live settings privately. Preserve the data directory,
listener address, authentication, public-readonly access and cookie policy,
Steam-cookie presence and provider configuration without printing private values
or broadening the audience. Runtime profiles containing only Python identity
remain compatible with the optional source/data/revision fields.

The actual dirty `D:/Projects/site/scripts/dev.ps1` differs from the prepared
site integration. Apply **only reviewed CS2 hunks**, including the new
`projects/cs2-dash/runtime-profile.ps1` helper. Do not overwrite unrelated local
launcher work. The prepared integration removes implicit `cs2-refresh.py`
startup, retains reporting/stop compatibility, consumes profile source/data/
revision and passes the current/research runtime options. Its project manifest
uses the upstream source with `patches: []`; old readonly/catalog source patches
must not be reapplied to restored code.

Do not use site-wide `dev.ps1 down/up`, `sync --all`, Docker Compose teardown or
generic process-name kills. They affect other active projects. The general
sync script also force-checks out vendor source and may reload the hub/nginx;
it is not a CS2-only cutover controller.

## Final writer boundary and fresh state

1. Finish source tests and browser acceptance before this window. Create a new
   staging Git checkout of the exact published main commit beside the canonical
   vendor checkout, for example `vendor/CS2-Dash.<task-staging>`. Verify its full
   commit and root runtime-file hashes before touching the old checkout. The
   upstream `.gitattributes` requires LF for code so Windows checkout hashes can
   match the tested commit. Do not copy private data/caches/credentials into the
   source stage or force-checkout the dirty running vendor directory.
2. Fence only the verified CS2 refresher/supervisor. Stop the refresher before
   stopping 8089 so it cannot enqueue a new Steam task when the new host appears.
   The legacy refresher immediately attempts a forced history task at startup.
3. Request the verified lean host's normal closed-unit stop using its own
   `lean_host/stop.request` mechanism, if that is the confirmed running entry
   point. Wait with a bounded deadline for its closed receipt, provider worker
   completion and process exit. Verify 8089 is unbound. If ownership, closure or
   the deadline is uncertain, stop the cutover and diagnose; do not kill a PID
   solely because it occurs in a saved file.
4. With **all CS2 writers now closed**, take a fresh read-only SQLite online
   backup of the actual main database and actual current quote ledger into a
   new private backup directory. Use `mode=ro` + `Connection.backup`, then
   `quick_check` on each destination. Do not copy a SQLite main file while
   omitting its WAL, change the live journal mode or checkpoint the live source.
   The online-backup API is appropriate even after the writer has stopped.
5. Copy existing companion `portfolios.json`, `settings.json`, `snaps.json` and
   any explicitly required runtime configuration after that same writer boundary.
   Retain all unknown runtime files in the backup; do not discard or activate
   sidecars by guessing. Record counts and canonical row digests privately for
   personal tables, saved identity resolutions, settings, auth/session state,
   catalog and real raw history. Record ledger revision, capture/provenance,
   leases, retry deadlines and provider activation unchanged.
6. Create the new isolated main database from that **final backup** with the
   recovery helper, then plan/apply additive research recovery:

   ```text
   python research_recovery.py clone --source <final-backup-main> --target <new-data>/cs2dash.sqlite
   python research_recovery.py plan --target <new-data>/cs2dash.sqlite --derived <retained-derived> --archive <retained-archive>
   python research_recovery.py apply --target <new-data>/cs2dash.sqlite --derived <retained-derived> --archive <retained-archive>
   ```

   Copy the freshly backed-up current quote ledger into the new data directory
   as `prices-current.sqlite` using a consistent SQLite backup, and copy the
   companion personal JSON files. Keep all originals untouched. Do not launch
   the application between clone and apply: the helper requires its unchanged
   offline clone marker and refuses an active WAL.
7. Require a successful recovery plan and receipt. The helper only fills empty
   `market_item_prices`, `price_quotes`, `item_market_metrics` and
   `market_item_lookup` tables and rebuilds their external-content FTS index.
   It preserves source observation times. If any target table is already
   populated or a source/marker check fails, **do not empty it or bypass the
   fence**; investigate a separate additive merge. No raw Steam history is
   synthesized, and no current ledger is modified by recovery.
8. Verify the recovered clone's quick check, four-table counts, FTS and receipt
   source hashes/date ranges. Compare all personal/settings/identity/catalog/
   raw-history invariants to the final backup, and the complete current ledger
   to its final backup. Existing current-feature schema must already be present;
   `restoration_runtime.install` deliberately refuses an existing incompatible
   ledger rather than migrating or replacing it implicitly.

## Activate only the CS2 service

After final data recovery/validation and old-process closure, resolve and verify
the three absolute checkout paths: current `vendor/CS2-Dash`, new staging
checkout, and a fresh private named old-checkout backup outside served source.
Verify their intended parent boundaries, distinct identities, destination
nonexistence and sufficient disk space. Prefer verified same-volume renames.
Move the **entire** old checkout to the rollback location, then move the verified
staging checkout into `D:/Projects/site/vendor/CS2-Dash`. Do not merge directories,
follow junctions, force-overwrite a destination or delete unknown old files. If
the second rename fails, restore the first rename and leave the data profile
unchanged. Record the completed paths and recheck canonical runtime hashes.
Keep the old checkout intact; never delete it automatically after acceptance.

Write the runtime profile atomically only after the final clone passes checks.
Preserve its existing interpreter identity fields and set the following:

```json
{
  "version": 1,
  "python": "<verified absolute Python executable>",
  "sourceDirectory": "D:/Projects/site/vendor/CS2-Dash",
  "dataDirectory": "<new final recovered data directory>",
  "sourceRevision": "<exact full 40-character upstream main commit>"
}
```

The profile contains no secrets. Resolve launch environment privately with the
site's existing dotenv parser semantics, preserving auth and bind settings.
Explicitly resolve `CS2DASH_QUOTE_DB` to the **new final ledger** or remove a
stale override so the data-directory default selects it; an old absolute quote
override would otherwise take precedence. Set `CS2DASH_SOURCE_REVISION` from the
verified profile. Leave research auto-refresh and startup task resume disabled
unless separately intended: `CS2DASH_RESEARCH_AUTO_REFRESH=0` and
`CS2DASH_RESUME_RESEARCH_TASKS=0`. Retain current-network policy, persisted cash
cadence, provider pauses/leases and cooldowns; do not reset them to force a quote.
Set `CS2DASH_STOP_FILE` to the site's private `state/dev-cs2-stop.request` path.
Remove a handled marker only before starting the verified replacement process.
Writing that marker requests normal server/task/coordinator shutdown; wait for
the owned process and any bounded source unit to close before backing up data.

Start just the verified Python executable with arguments
`-u D:/Projects/site/vendor/CS2-Dash/server.py 8089`, the site working directory
and that private environment. Use a hidden window and dedicated logs. Record
the newly returned PID and creation identity in `state/dev-cs2.pid`; do not reuse
a preexisting PID record or invoke the global launcher to restart other apps.
Keep the old launch descriptor/profile and source available for rollback.

## Acceptance and evidence

Use bounded, read-only checks at the existing loopback and LAN URLs. Store any
private responses locally; publish only non-sensitive aggregate evidence.
No refresh/force query, import/delete, job creation or trade is part of this
cutover smoke test.

| Check | Required evidence |
| --- | --- |
| Listener/audience | New owned PID is the sole 8089 listener; bind, LAN URL and existing access policy agree with preflight; unrelated services remain running. |
| Build | `/api/health` version, `build.sourceRevision`, runtime/frontend/server hashes and file count match the exact deployed root code. Served `/` bytes hash matches `frontendSha256`; a supplied revision alone is not byte proof. |
| Personal state | `/api/portfolios` selected identities/quantities and settings equal final-backup invariants; no synthetic fixture marker or test portfolio exists. |
| Current basis | `/api/current-analysis?portfolio=<existing-id>` and `/api/canonical-cash-value?portfolio_id=<existing-id>` agree on held scope, basis, coverage and quote revision. `/api/current-market-items` selects quotes from that same canonical ledger. Allow normal time-based staleness; never relabel a preserved old capture fresh. |
| Research basis | `/api/inventory?portfolio=<existing-id>`, `/api/market-items`, `/api/market-universe` and portfolio/market `/api/taxonomy-analysis` use recovered dated research, with one-each market scope separate from held quantities. Price bounds/sort precede pagination. |
| Provenance/history | `/api/research-capabilities` and recovered response date/coverage fields expose saved dated metrics and actual raw-history availability. Empty genuine raw history stays unavailable; archive apply time is not market observation time. |
| Reports/UI | Research `/api/report` + `/report` and cash `/api/current-report` + `/current-report` preserve separate semantics. All five tabs, period/basis controls, notes/presets, tables and local horizontal scroll remain usable. Browser checks use the bounded runtime lease. |
| Coordinator | Source-status reads preserve provider pauses, cooldowns and any unresolved lease. No revived legacy refresher, large research resume or unintended history sweep appears. |

`/api/health` currently serializes code identity under `build`; verify the final
handler contract if it changes before release. Missing raw history, unavailable
Steam session/admission or provider backoff must remain explicit blockers to
data acquisition, not a reason to fabricate charts or claim full v1 parity.

Record separate stages: **source tested**, **pushed**, **activated**, **live
verified**, and **full v1 parity / remaining data prerequisites**. Keep rollback
assets until the deployment is accepted. Remove only fully merged task branches;
retain private backups and runtime receipts outside Git.

## Rollback

Before the new listener starts, any failure leaves original source/data usable:
restore the saved profile/CS2 launcher hunks, close only owned candidate resources,
move a promoted new checkout to a separate checked destination and return the
whole saved old checkout to the canonical source path. Archive the lean host's
handled stop marker so it will not immediately exit, then restart the original
verified launch descriptor with original auth/bind settings.
Recheck old listener ownership, frontend identity, personal state and current cash
reads. Keep the legacy refresher fenced while diagnosing.

After the new listener starts, stop **only that new owned process** normally and
take a final backup of its latest personal state and current ledger before any
rollback. Do not point at the older pre-cutover directory if users or the cash
coordinator have since written newer state. A source-only rollback over that
latest state requires prior compatibility qualification of the preserved lean
host against additive recovered tables. Otherwise reconcile the latest personal
tables/settings/identity data and quote ledger into a new verified rollback clone;
this reconciliation is not supplied by `research_recovery.py`. Never solve a
rollback by silently discarding writes. If neither safe path is proven, hold the
service stopped and report the exact remaining prerequisite.

## Remaining execution prerequisites

The targeted process-description read succeeded in the approved host context.
There is no unresolved approval request. Ownership still needs its immediate
pre-stop recheck, and final source/archive identities and supervisor/refresher
closure need execution evidence.

The restored root `server.py` supports `CS2DASH_STOP_FILE` as well as console
`KeyboardInterrupt`, and closes explicit tasks, the server and coordinator in
`finally`. Qualify that configured marker on the owned candidate process before
rollback depends on it. A saved PID and `Stop-Process` alone do not prove graceful
closed-unit shutdown.

The final backup, recovery, profile update, restart, post-start write-safe rollback
qualification and live acceptance are all **pending execution**, not completed
by this document. Retained archive paths and any post-start personal-state merge
mechanism are explicit controller inputs, not guessed by this checklist.
