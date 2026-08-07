# Helium 10 keyword evidence

- Availability: **AVAILABLE**
- Collection timestamp: 2026-08-07T17:51:11.639Z
- Marketplace: Amazon US
- Tools called:
  - `mcp__helium10__get_keywords_by_asin`: 6 calls
  - `mcp__helium10__analyze_keywords`: 1 call (99 unique phrases)

## ASIN coverage

- B00NLLUMOE: fetched 9460, normalized 9460, selected 25
- B00NQDGAP2: fetched 2746, normalized 2746, selected 25
- B00O35DAL4: fetched 1822, normalized 1822, selected 25
- B0822X1VP7: fetched 877, normalized 877, selected 25
- B016P42ARU: fetched 1945, normalized 1945, selected 25
- B0822X4TLW: fetched 639, normalized 639, selected 25

## Row totals

- Before normalization: 17489
- After lowercase/trim/whitespace normalization and exact ASIN+phrase deduplication: 17489
- Final candidate rows: 150

## Caveats

- Helium 10 search volume, keyword sales, ranks, CPR, IQ, competition, CPC, and trend are estimates rather than Mellanni internal sales truth.
- Current live MCP snapshots do not establish week-over-week rank movement.
- The shortlist is mechanically balanced at 25 strongest rows per ASIN, prioritized by estimated weekly keyword sales, search volume, organic rank, then IQ score.
- Missing source metrics are blank; no values were guessed.
- CSV cells beginning with formula-leading characters are apostrophe-neutralized.

