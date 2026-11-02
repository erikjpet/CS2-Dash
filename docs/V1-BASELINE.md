# Verified v1.0 restoration baseline

Task: **CS2-04 — Restore CS2 version-one functionality**.

There is no published GitHub Release or remote tag identifying version one. Both the GitHub releases/tags APIs and `git ls-remote --tags` returned no entries. The complete remote history has five commits and one main branch; the repository is not shallow.

The original published commit is `de8a469184d4802c536a8937c7d945f021a26314` (1 July 2026, 01:50:41 UTC). Its README explicitly calls the application Version 1.0 and its server declares `APP_VERSION = '1.0'`. It already contains the portfolio, full-market, provider-comparison and historical research functionality.

The following v1.0 fixes are included rather than rolled back:

| Commit | Published change |
| --- | --- |
| `e7250892972242aa8b637c8fba9ac71dc8b32033` | First-run startup bootstrap |
| `f501973b66ba5dc00627564612603d57587858bf` | Duplicate-import normalization |
| `bae566ef1ca345ca59f0736a4baa124cc3f76fab` | Pricing startup and write-lock corrections |
| `0e98899ef060742fcec8acd1ce22d03a728f18dc` | LAN startup support; published main verified for this restoration |

The restoration contract is the original v1.0 feature set plus these subsequent v1.0 corrections. This identifies why the current published source belongs to v1.0; it does not invent a release tag or equate an arbitrary current main with an original release.

## Preservation and ownership

Before source reconciliation, the exact deployed source (79 files) and consistent online backups of both current databases were preserved locally outside Git. Both database backups passed SQLite quick checks. Inventory, settings, current quote provenance and historical records must not be replaced by demo data or reset. Private backup paths and data are never publication inputs.

The project repository is the source of truth. The site integration consumes the published upstream code without a second divergent implementation patch. Original research data and normalized cash quotes have distinct semantics and endpoints; a completed-sale median, a wallet ask and a current cash listing must not be silently mixed.

Implementation owners:

- Frontend and every mounted v1 control: frontend restoration worker.
- Original research/API contracts, normalized cash integration and shared Steam admission: backend worker.
- Additive recovery of dated research tables from retained archives: recovery worker.
- Independent synthetic API/HTTP parity acceptance: parity test worker.
- Preservation, transport portability, publication/integration and live acceptance: root integration worker.

## Completion stages

Every matrix row is tracked through source-ready, portable-tested, browser-tested where relevant, pushed, live-verified and full-v1-parity. A code change, mocked test or push alone is not delivery. Missing raw history, absent credentials, rate limits or unavailable upstream services must be recorded explicitly while independent repairs continue. No chart may fabricate prior history from today's quote.
