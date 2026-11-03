# NBA advanced scraper: SOCKS failures and browser-compatible transport

## Current evidence

On November 2, 2026 the authorized original one-game CLI fetched and saved game `0022600001`, printed `Advanced stats updated: 1 (Failed: 0)` and `Result: SUCCESS`, and exited zero at CLI-local time 17:59:51. Independent SELECT-only readback verified canonical database/user `postgres`, 35 advanced player rows, all 22 positive-minute basic player/team pairs matched to advanced rows with non-NULL possessions, zero missing played pairs, and both team rows populated for possessions/offensive rating/pace. The one-game gate passes; this is not a whole-season or derived-feature readiness certificate.

No bulk catch-up, mirror, linker, rolling job, migration, deployment or OS-task change was launched. The one-game operation also added two missing player references through the existing scraper path.

## Why the old route failed

There were two different observations, not one proven permanent IP ban:

- Configured SOCKS proxy: connection closed before the stats response.
- Plain Python requests, explicitly direct with environment proxy discovery disabled: connection reset 10054 for the same game.
- Browser-compatible `curl_cffi` 0.14.0: direct HTTP 200, valid `boxScoreAdvanced` JSON, then successful nba_api parsing into 35 player rows and two team rows. The 35 include non-playing roster entries; do not confuse that with the played-player coverage count.

The controlled direct requests-versus-curl comparison supports a transport/client compatibility issue. It does not independently identify the exact NBA firewall rule. A longer timeout alone would not explain or repair the immediate resets we observed.

## Old and newer solutions checked

GBrain `decisions/infrastructure-decisions` already prescribed local direct advanced scraping rather than Railway or a default proxy. That rule remains relevant. We did not move stats.nba.com requests into Railway.

The upstream nba_api connection discussion includes older header-only workarounds and a May 13, 2026 report of browser-TLS transport with an NBA warmup. On this workstation the first direct browser-compatible request worked without warmup; we therefore did not add unnecessary warmup requests or an alternate data source.

Primary references:

- [nba_api connection discussion, issue 652](https://github.com/swar/nba_api/issues/652)
- [curl_cffi official project and browser impersonation documentation](https://github.com/lexiforest/curl_cffi)
- [nba_api HTTP session injection implementation](https://github.com/swar/nba_api/blob/v1.11.3/src/nba_api/library/http.py)

## What changed

- Local non-CDN CLI defaults to a Chrome-compatible curl session; the existing nba_api parser and stat transformations remain in use.
- Direct routing is now default. A configured proxy requires `--use-proxy`. `--no-proxy` remains accepted. No credential file was edited and no proxy URL/credential was exposed.
- Requests sessions disable environment discovery. Curl additionally sets an explicit empty `CURLOPT_PROXY` because curl_cffi 0.14 stores `trust_env` without applying it to libcurl. A same-game read-only probe with deliberately invalid ambient proxies still fetched/parsed 35 player and two team rows. An explicit empty NBA proxy argument prevents nba_api's own fallback proxy setting from undoing the direct-route choice.
- `--http-client requests` retains a diagnostic legacy transport. It is not the successful route in this case.
- The Windows runtime dependency pins `curl_cffi==0.14.0`. The local venv has it installed; existing nba_api remained 1.11.3. Railway's Linux requirements do not install this Windows-only extra, and `--cdn-only` never initializes the local stats transport.
- The existing advanced BAT explicitly chooses curl/direct by default, retains owner opt-in proxy configuration, and does not chain sync.
- Review found that curl network exceptions do not inherit requests exceptions. The curl session now normalizes connection/timeout failures into the existing requests error classes, preserving retries and direct-source CDN recovery. Explicit `--use-proxy` failures fail closed instead of switching to CDN. Small offline execution checks exercised both source stages and both routing decisions; endpoint parsing and SQL errors are not converted into network success.

## Commands

From the repo root in PowerShell, the user's original one-game command now selects curl/direct by default:

    .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_unified_scraper.py --season current --skip-team --skip-traditional --advanced-limit 1 --request-timeout 20 --max-retries 1

The smoke and independent readback gate have passed. The owner can launch the bounded catch-up explicitly:

    .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_unified_scraper.py --season current --skip-team --skip-traditional --advanced-limit 120 --request-timeout 30 --max-retries 2 --http-client curl --no-proxy

The limit is games selected in one invocation, not a claim that the season is complete. Verify the summary and source coverage before derived jobs. Current source remains uncommitted and undeployed.

## Verification scope

139 existing integration tests, Ruff, compilation and scoped whitespace checks passed after the final exception/routing correction. The final client also re-fetched and parsed the same official game read-only into 35 player and two team rows; no second game was written. No tests or fixtures were created or edited. Canonical readback receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-transport-probe/smoke-readback.json`. Other captures are in that directory.
