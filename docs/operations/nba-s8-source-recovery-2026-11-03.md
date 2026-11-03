# Slice 8 source-recovery evidence — November 3, 2026

## New primary-source finding

The official NBA static host still serves distinct dated injury-report PDF editions for April 12, 2026. This is a concrete recovery candidate beyond the current RapidAPI table; the earlier producer audit did not rule out external official sources.

Exactly two public PDFs were fetched with bounded 20-second requests and a 2 MB maximum response, without credentials, provider credits, DB queries/writes or source ingestion. Both returned HTTP 200 with `application/pdf`, resolved to the requested official host/path, passed PDF magic validation, and were hashed. Local document extraction confirmed the report date/edition header in each captured PDF. Full player-row parsing/mapping was not attempted; automatic table extraction can merge teams/players across rows and must not be treated as trustworthy row-level ingestion.

## Exact capture receipts

Morning edition:

- Requested/resolved: `https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-04-12_06_30AM.pdf`
- Header: `Injury Report: 04/12/26 06:30 AM`
- Bytes: 103482
- SHA-256: `7428d66011d348809242bb0f73bb9f5c8386254f53b1f8d0586519122c68f915`
- HTTP Last-Modified: `Sun, 12 Apr 2026 10:30:06 GMT`

Afternoon edition:

- Requested/resolved: `https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-04-12_04_00PM.pdf`
- Header: `Injury Report: 04/12/26 04:00 PM`
- Bytes: 103804
- SHA-256: `2c773253852d131a248711a919e55b7c6c88486ee2e95599a946d8267c575a41`
- HTTP Last-Modified: `Sun, 12 Apr 2026 20:00:05 GMT`

Capture root: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8-official-injury-probe`.
Raw HTTP metadata, actual resolved URLs, retrieval timestamps and filenames are in `capture_receipts.json`. Scratch may expire; the exact URLs/hashes and decisive scope are preserved here.

## What this establishes

- Two distinct publisher-hosted raw editions can be retrieved and preserved now.
- Their actual document headers can be checked independently of URL naming.
- Content-addressed capture and edition-aware parsing are feasible candidates for a source adapter.

## What it does not establish

- A complete edition sequence or complete historical date/game/team coverage.
- A guarantee that the publisher's displayed/report or HTTP-modification time equals historical public availability.
- Approved timezone/availability semantics, omitted-player meaning, complete roster membership or player/game ID mapping.
- Quote provenance, a non-overlapping cohort/split, a manifested model artifact, assembled historical feature readiness or profitable replay.
- Permission for bulk collection, source/schema repair, DB ingestion, API-credit use, training or a baseline launch.

## Next reviewable recovery slice

A bounded proposed source-reader scope would preserve official raw bytes and HTTP receipts; verify actual edition headers and game/team submission states; parse positioned rows rather than merged automatic tables; reconcile exact roster/player/game identities; and report missing/submission/omission ambiguity explicitly. Its source policy must be owner-approved before historical feature acceptance. This document is a proposal and bounded evidence receipt, not authorization or completed integration.
