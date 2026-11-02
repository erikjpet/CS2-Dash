# Dated research recovery

`research_recovery.py` restores archived provider snapshots and aggregate research
metrics into a separately created offline clone. It does not recover raw Steam
charts from aggregates, refresh providers, modify inventories/settings, or write
the separate canonical cash ledger.

The helper uses only Python's standard library. Its SQLite binding must support
`Connection.deserialize` for the archived databases. The running application does
not need this helper to serve already recovered data.

## Procedure

Keep the original deployment and its data backup in place. Choose a new local,
private destination that is not served by an application. Do not upload data
files, clone markers or private receipts with source/test bundles.

```sh
python research_recovery.py clone --source /private/backup/cs2dash.sqlite --target /private/candidate/cs2dash.sqlite
python research_recovery.py plan --target /private/candidate/cs2dash.sqlite --derived /private/archive/closed-derived.sqlite --archive /private/archive/recovery.zip
python research_recovery.py apply --target /private/candidate/cs2dash.sqlite --derived /private/archive/closed-derived.sqlite --archive /private/archive/recovery.zip
```

`clone` opens the source read-only and uses SQLite's online backup API. Only the
new destination is consolidated to DELETE journal mode. The clone gets a path
and SHA-256 identity marker. `apply` requires that marker and an unchanged target,
refuses a WAL or preexisting research rows, and imports in one transaction. A
repeated import with the same receipt and counts is idempotent.

The supported archive contains `compact-price-store-candidate/current-hot.sqlite`
and `compact-price-store-candidate/cold-evidence.sqlite`. The derived database must
contain the original `item_market_metrics`, `lookup_common` and `lookup_windows`
schemas. Source file/member hashes and schema/reference checks prevent accidental
mixing of unrelated sources.

Only these empty compatibility tables are populated:

- `market_item_prices`: decoded provider snapshots, preserving provider labels,
  observation/update timestamps and original raw evidence.
- `price_quotes`: corresponding recorded provider quote rows.
- `item_market_metrics`: original dated aggregate metrics.
- `market_item_lookup`: denormalized original window projections. Its existing
  external-content FTS index is rebuilt inside the transaction.

No `price_history` rows are synthesized. No price observations are invented.
Existing personal, settings, catalog and raw-history tables remain untouched.

## Runtime provenance

`research_recovery.recovery_status(connection)` reads the most recent
`research_recovery_receipts` receipt. It contains source hashes, imported table
counts, lookup-window counts and original date ranges, not asset rows or prices.

Show archived research as dated/stale evidence until an authorized refresh
replaces it. `appliedAt` is the recovery operation time, **not** the source's
observation time. Use the `asOf` ranges for data age. Never erase recovered metrics
by rebuilding them from an empty raw-history table.

Historical acquisition remains separate: existing Steam session availability,
provider retry deadlines and a shared Steam admission gate must be respected.
Current cash prices retain their independent canonical quote/freshness rules.

## Validation

Run `python tools/test_research_recovery.py -v`. The synthetic suite verifies clone
identity, WAL-source preservation, atomic rollback, idempotence, FTS, provenance,
existing-data refusal and preservation of personal state. It uses no live service,
real holdings, credentials, external provider or network request.
