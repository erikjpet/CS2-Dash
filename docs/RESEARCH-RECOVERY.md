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

The personal-state source passed to `clone` can be a live WAL database: its online
backup includes committed WAL state without checkpointing or changing that source.
This differs from the `--derived` recovery input, which must be a closed,
consolidated database with **no `-wal`, `-shm` or `-journal` sidecar**. The helper
refuses those sidecars rather than hashing only the main file and silently reading
different committed data from its WAL. Prepare a separate consistent backup if
the retained derived source is still open; keep the original and its sidecars intact.

The supported archive contains `compact-price-store-candidate/current-hot.sqlite`
and `compact-price-store-candidate/cold-evidence.sqlite`. The derived database must
contain the original `item_market_metrics`, `lookup_common` and `lookup_windows`
schemas. Source file/member hashes and schema/reference checks prevent accidental
mixing of unrelated sources.

Derived queries run against a private SQLite backup snapshot, made through an
immutable read-only input connection. The helper verifies the original file's
identity before and after the snapshot and consolidates only the new snapshot.
The receipt binds both the original main-file SHA-256 and the SHA-256 of the
snapshot actually queried. Later changes to the original cannot change an
in-progress recovery's input. Archive identity is also rechecked after members
are read. Neither input is opened for writes or checkpointed.

Only these empty compatibility tables are populated:

- `market_item_prices`: decoded provider snapshots, preserving provider labels,
  observation/update timestamps and original raw evidence.
- `price_quotes`: corresponding recorded provider quote rows.
- `item_market_metrics`: original dated aggregate metrics.
- `market_item_lookup`: denormalized original window projections. Its existing
  external-content FTS index is rebuilt inside the transaction.

No `price_history` rows are synthesized. No price observations are invented.
Existing personal, settings, catalog and raw-history tables remain untouched.

Recovery targets must be ordinary v1 tables. SQL triggers attached to any recovery
write object, including the receipt table, are unsupported and cause refusal
before import. Standard v1 external-content FTS remains supported through its
explicit rebuild; it does not require SQL triggers. The transaction also permits
writes only to the four research tables, its receipt and verified FTS5 shadow
tables. A protected table/schema digest is compared after all writes and before
commit; any unexpected change rolls back the complete import.

## Runtime provenance

`research_recovery.recovery_status(connection)` reads the most recent
`research_recovery_receipts` receipt. It contains source hashes, imported table
counts, lookup-window counts and original date ranges, not asset rows or prices.
An unapplied plan leaves `personalTablesModified` unknown and
`personalPreservationVerified` false. Only a committed transaction with matching
protected-state proofs records `personalTablesModified: false` and
`personalPreservationVerified: true`, together with the verified table count and
aggregate state digest. A receipt alone is insufficient for an idempotent return:
the unchanged clone marker must also identify that completed recovery, with
matching expected and existing row counts.

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
existing-data refusal and preservation of personal state. Negative fixtures create
a settings-mutating trigger before cloning, exercise receipt triggers and both
transaction preservation fences, and keep a derived WAL writer open while its
main-file hash remains unchanged. The suite uses no live service, real holdings,
credentials, external provider or network request.
