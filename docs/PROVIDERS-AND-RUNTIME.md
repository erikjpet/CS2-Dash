# Provider configuration and runtime portability

Verified 2026-11-02 for CS2-04. The restored application is running in the
existing Windows site environment. Moving hosting to Linux is a later task;
this restoration does not replace the application with a Windows implementation.

## Original committed version

The published v1 source through `0e98899` obtains Steam, CSFloat and other
benchmark prices from public CSGO Trader provider snapshots. Its authenticated
Steam Community Market history path reads `STEAM_COOKIE`, with a public listing
path when no session is configured. That committed source does not configure
a direct CSFloat API key or a Steam Web API key. A separate private collector
or service configuration outside this repository could account for the earlier
Linux setup's additional keys.

These are different capabilities: public benchmark snapshots, authenticated
CSFloat listing reads, Steam wallet asks and Steam price-history acquisition.
A successful public Steam request does not establish an authenticated session.

## Current operation

The active source is `07d21ec5f912ce70eb90a6505764df1d9025216e` at the existing
LAN address. After its restart, the existing authenticated CSFloat integration
continued returning HTTP 200 and fresh captures. It uses the existing local
credential store; restoration copied or installed no credentials. The public
Skinport, Market.CSGO and Waxpeer feeds also returned successful captures.

One normal selected-portfolio Steam-history job completed through the active
service. It followed a canonical listing redirect, waited for the persisted
shared admission deadline, resumed the same item and saved 2,408 genuine raw
points. A subsequent cached history GET succeeded. History acquisition did not
change the Steam wallet quote's capture time. This proves the exercised public
acquisition path for one item, not full holdings or full-market history coverage.

`STEAM_COOKIE` is currently empty in the site's configuration. Authenticated
Steam history remains unqualified; the current successful path is public.
If authenticated access is needed for a listing that the public path cannot
serve, the missing capability is an authorized Steam session in that existing
configuration interface. No secret should be pasted into reports or Git.

## Platform boundary

| Component | Current boundary | Later Linux configuration |
| --- | --- | --- |
| Application and collectors | Python/SQLite, browser JavaScript and Node transports retain their common implementation. | Reuse the same application and collector source. |
| CSFloat credentials | `CS2DASH_CSFLOAT_API_KEY` is checked first; otherwise the existing Windows DPAPI loader reads its configured/default file. That loader is unchanged from the deployed source before restoration. | Supply the same environment variable through the service's existing secret mechanism; Windows DPAPI files are not portable. |
| Node selection | `CS2_CASH_NODE`, then normal `node`/`nodejs` on PATH, then platform runtime locations. | Use an installed Node executable or an explicit absolute path. The existing Docker recipe sets `/usr/bin/node`. |
| Service launch and storage | Windows PowerShell launcher and non-secret runtime profile select Python, source, data, revision and stop-marker paths. | Use the existing Linux launcher/service arrangement with Linux paths and credentials. |
| Inactive derived/staging tools | Retained modules include Windows-specific locking and process/memory adapters. They are not activated by this restoration. | Qualify those adapters separately only if those optional tools are later activated. |

No actual Linux migration or speculative container rebuild was performed.
Runtime dependency and service configuration will still need normal validation
on the eventual Linux host, rather than a rewrite of the application.

See [source baseline](V1-BASELINE.md), [parity evidence](V1-PARITY-MATRIX.md),
[CSFloat credential adapter](../csfloat_read.py) and
[portable Node selection](../price_transport.py).
