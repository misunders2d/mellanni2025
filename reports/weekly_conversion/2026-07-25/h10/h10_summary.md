# Helium 10 keyword summary — week ending 2026-07-25

## Availability

**AVAILABLE.** Current Helium 10 US data was returned for every requested call. H10 figures are planning estimates, not Mellanni internal sales truth.

## Tools called

- `list_tracked_keywords` — US, account-wide, limit 500, search-volume descending. Returned 500 rows (applied limit 500).
- `get_keywords_by_asin` — US/current period for each supplied ASIN. Returned:
  - B00NLLUMOE: 16,056 rows
  - B00NQDGAP2: 16,056 rows
  - B00O35DAL4: 16,025 rows
  - B016P42ARU: 16,056 rows
  - B0822X1VP7: 16,056 rows
- `analyze_keywords` — US for 9 supplied seed terms. Returned 9 rows.

## Output counts

- Normalized candidate CSV: 150 rows (maximum requested 150).
- Reverse-ASIN source rows: 80,249 total.
- Account-wide tracked rows: 500.
- Seed enrichments: 9.

## Important visibility and opportunity signals

High-value supplied ASIN visibility (selected ranked rows):

- B00NLLUMOE — `queen sheets`: organic #18, 188,972 volume, 1279 estimated weekly keyword sales.
- B00NQDGAP2 — `queen sheets`: organic #18, 188,972 volume, 1279 estimated weekly keyword sales.
- B00O35DAL4 — `queen sheets`: organic #18, 188,972 volume, 1279 estimated weekly keyword sales.
- B016P42ARU — `queen sheets`: organic #18, 188,972 volume, 1279 estimated weekly keyword sales.
- B0822X1VP7 — `queen sheets`: organic #18, 188,972 volume, 1279 estimated weekly keyword sales.
- B00NLLUMOE — `queen sheet set`: organic #15, 112,569 volume, 1173 estimated weekly keyword sales.
- B00NQDGAP2 — `queen sheet set`: organic #15, 112,569 volume, 1173 estimated weekly keyword sales.
- B00O35DAL4 — `queen sheet set`: organic #15, 112,569 volume, 1173 estimated weekly keyword sales.
- B016P42ARU — `queen sheet set`: organic #15, 112,569 volume, 1173 estimated weekly keyword sales.
- B0822X1VP7 — `queen sheet set`: organic #15, 112,569 volume, 1173 estimated weekly keyword sales.
- B00NLLUMOE — `king size sheets set`: organic #17, 130,261 volume, 2372 estimated weekly keyword sales.
- B00NQDGAP2 — `king size sheets set`: organic #17, 130,261 volume, 2372 estimated weekly keyword sales.

Seed-term enrichment:

- `queen sheets`: 188,972 search volume; 1279 estimated weekly keyword sales; CPR 252.31928000000002; IQ 9448.6; trend 73%.
- `twin xl sheets`: 156,894 search volume; 977 estimated weekly keyword sales; CPR 213.50394; IQ 26149; trend 91%.
- `king size sheets set`: 130,261 search volume; 2372 estimated weekly keyword sales; CPR 182.45319; IQ 13026.1; trend 44%.
- `full size sheets`: 84,575 search volume; 981 estimated weekly keyword sales; CPR 125.51677000000002; IQ 8457.5; trend 29%.
- `king sheets`: 69,719 search volume; 1612 estimated weekly keyword sales; CPR 109.44443000000001; IQ 3485.95; trend 72%.
- `sheets queen size bed set`: 59,512 search volume; 1540 estimated weekly keyword sales; CPR 95.69180000000001; IQ 5951.2; trend 42%.
- `bed sheets queen size`: 45,406 search volume; 1042 estimated weekly keyword sales; CPR 78.5844; IQ 2270.3; trend 43%.
- `deep pocket sheets`: 4,792 search volume; 100 estimated weekly keyword sales; CPR 31.86004; IQ 479.2; trend 63%.
- `microfiber sheets`: 2,873 search volume; 55 estimated weekly keyword sales; CPR 27.970129999999997; IQ 143.65; trend 17%.

Treat generic demand as a candidate, not immediate copy. Confirm relevance and listing/PPC coverage before execution; do not paste raw H10 phrases into backend search terms.

## Gaps and degradations

- The account-wide tracker call succeeded but is account-wide; it contains broader catalog keywords and is not limited to the five supplied ASINs.
- Full raw JSON for each reverse-ASIN call was not written: each returned about 16,000 rows (80,249 total), making five raw files disproportionately large. The normalized CSV preserves the selected decision candidates and exact source metrics.
- No tool errors or permission degradations were returned.
- No prior H10 export/current-vs-prior snapshot was supplied, so this artifact does not claim week-over-week rank movement beyond the tool's current trend fields.


