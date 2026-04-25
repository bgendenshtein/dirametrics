# CBS ETL Notes

Operational notes for `etl/fetch_cbs_price_indices.py` and
`etl/fetch_cbs_series.py`.

**Last updated:** 2026-04-25

## Table rename

The Supabase table was renamed `cbs_housing_prices` → `cbs_price_indices`.
The original name was too narrow: we want to ingest rent and headline CPI
into the same table alongside housing prices, since they share the same
schema (`series_id, series_name, date, value, is_provisional`) and the
same provisional-revision semantics.

The script was renamed to match: `fetch_cbs_housing_prices.py` →
`fetch_cbs_price_indices.py`.

## Series fetched

| codeId | Name (he)                              | Chapter / subject             | Notes |
|--------|----------------------------------------|-------------------------------|-------|
| 40010  | מדד מחירי דירות                         | chapter `aa` / subject 45     | National housing prices, monthly back to 1994. |
| 120460 | מדד שכר דירה                           | chapter `a` (CPI)             | Rent paid by tenants — see cross-check below. |
| 120010 | מדד המחירים לצרכן - כללי                | chapter `a` (CPI)             | Headline CPI, base 2024 average. |
| 60000  | ירושלים                                 | chapter `aa` / subject 166    | Housing prices — Jerusalem district, from 2017-10. |
| 60100  | צפון                                   | chapter `aa` / subject 166    | Housing prices — North district, from 2017-10. |
| 60200  | חיפה                                   | chapter `aa` / subject 166    | Housing prices — Haifa district, from 2017-10. |
| 60300  | מרכז                                   | chapter `aa` / subject 166    | Housing prices — Center district, from 2017-10. |
| 60400  | תל אביב                                | chapter `aa` / subject 166    | Housing prices — Tel Aviv district, from 2017-10. |
| 60500  | דרום                                   | chapter `aa` / subject 166    | Housing prices — South district, from 2017-10. |

## Cross-check: 120460 is rent (not imputed owner-occupier cost)

Between candidates 120440 and 120460:

- **`120460`** resolves to `שכר דירה פרטי, ציבורי ושכירות ארוכת טווח בפיקוח ממשלתי`
  ("Private rent + public rent + long-term rent under government
  supervision"). The name literally says שכר דירה (rent), and the
  three sub-populations it enumerates are all tenant-paid categories.
  This is the index we want.
- **`120440`** resolves to `מצרכים בקיוסקים ובחנויות נוחות` ("Goods at
  kiosks and convenience stores") — a consumer-goods index, unrelated
  to rent. It was a red herring from earlier research.

CBS also publishes a separate **imputed** housing-services index for
owner-occupiers (variants of "שירותי דיור בבעלות הדיירים"). That is
*not* what 120460 is, and we specifically want the tenant-paid number.

**Value cross-check:** 120460's YoY for 2026-03 came back at **+3.3%**,
consistent with the CBS Nov-2025 press release combining ~2.5% on lease
renewals and ~5.5% on new tenants (weighted average lands ~3.3%).

## By-district housing prices (subject 166)

Added **2026-04-25**. CBS chapter `aa` subject 166 = `מדד מחירי דירות
לפי מחוזות` ("Housing price index by district").

The catalog returns exactly 6 series — one per major Israeli statistical
district. No sub-districts and no Judea & Samaria entry; this is the
clean major-district set, no further filtering needed.

| codeId | District (he) | District (en) |
|--------|---------------|---------------|
| 60000  | ירושלים       | Jerusalem |
| 60100  | צפון          | North |
| 60200  | חיפה          | Haifa |
| 60300  | מרכז          | Center |
| 60400  | תל אביב       | Tel Aviv |
| 60500  | דרום          | South |

All 6 endpoints returned HTTP 200 with valid observations on
verification — no broken series in this set (unlike 70000, see below).
Payload schema is identical to 40010, so the existing fetch/parse code
in `fetch_cbs_price_indices.py` handles them without modification. Each
district lands as its own row in `cbs_price_indices` keyed by
`series_id`.

Same provisional-flag semantics apply: top 3 most recent observations
per series are flagged `is_provisional=true` (see Provisional section
below).

### UI consideration: short history

Coverage starts **2017-10**, much shorter than the national index 40010
(which goes back to 1994). When charting national vs. district series
on the same axis, the UI must render the pre-2017-10 district range as
**missing data**, not zero — otherwise the district lines will appear
to have crashed from zero into the 2017 base period.

## Known API issues

### New-housing series (70000) — HTTP 500

The new-housing index (`מדד מחירי דירות חדשות`, chapter `aa` subject 167,
codeId 70000) is correctly cataloged but the data endpoint returns
HTTP 500 `{"Message":"Error: Price Data"}` as of **2026-04-24**.

Retries with prefixed variants (`1170000`, `11070000`) return HTTP 200
but empty payloads, confirming the catalog ID is correct and the issue
is server-side. Not currently fetched. Re-check periodically; if CBS
fixes the endpoint, add an entry to `SERIES` with this codeId.

## Provisional flag semantics

CBS publishes the 3 most recent observations per series as provisional.
The script flags `is_provisional=true` on the top 3 *per series* (not
across the combined batch), which matters now that multiple series are
fetched in one run.

Upsert on `(series_id, date)` means re-running the job:
- overwrites a provisional value with the latest revision;
- promotes a previously-provisional row to `is_provisional=false` once a
  newer month pushes it out of that series's top-3 window.

---

# CBS Time-Series ETL Notes (`fetch_cbs_series.py`)

Added **2026-04-25**. Operational notes for the construction & real-estate
data pulled from the CBS time-series API and stored in `cbs_series`.

## Two CBS APIs — don't conflate them

CBS exposes two separate JSON APIs that look similar but are not the same
service:

| API | Host | Used for |
|-----|------|----------|
| Price-Index API | `api.cbs.gov.il` (singular) | `fetch_cbs_price_indices.py` — housing-price index, rent, CPI, by-district housing-price indices |
| Time-Series API | `apis.cbs.gov.il` (**plural**) | `fetch_cbs_series.py` — construction permits/starts/completions/active, sales, inventory |

The hostname is the only obvious tell; the URL paths and JSON envelopes
differ between them. Catalog discovery for the time-series API:

- Top-level subjects: `GET /series/catalog/level?id=1&format=json`
- Sub-topic drill-down: `GET /series/catalog/level?id=N&subject=M`
- Series under a path: `GET /series/catalog/path?id=A,B,C&Page=1&PageSize=100`
- Data for one series: `GET /series/data/list?id={seriesId}&format=json`
- Data for all series at a path: `GET /series/data/path?id=A,B,C&format=json`

The construction subject is `level1=44 (בינוי ונדל"ן)`. Sub-topics under
44 used by this ETL: 2=starts, 3=completions, 4=active, 10=transactions,
11=permits.

## User-Agent header is mandatory

The time-series API silently rejects requests without a User-Agent header.
The script sets `DiraMetrics/1.0 (+bgendenshtein@gmail.com)` on a session
that's reused for all calls. If you ever see "no Series object" or empty
responses despite a 200 status, suspect UA first.

The price-index API does not have this requirement, but setting a UA there
costs nothing — consider mirroring the convention if that script ever
breaks.

## Series selection — data=1 (original) only

Each leaf in the time-series catalog can have up to three statistical
variants:

- `data=1` נתונים מקוריים (original) — what we use
- `data=2` מנוכי עונתיות (seasonally adjusted)
- `data=3` נתוני מגמה (trend)

The series IDs in `TOPICS` are pinned to data=1. If CBS ever adds new
variants we'd need to re-verify; the script does not auto-filter by data
field, it relies on the curated ID list.

## District-code dual schemes (transactions topic)

`new_sales_total`, `new_sales_subsidized`, and `second_hand_sales` all live
under `[44,10,*]` but use **different geographic coding schemes**:

| Topic | National | Districts (codes 5070-5075 OR 7047-7052) |
|---|---|---|
| `new_sales_total`     | 5069 | **5070-5075** |
| `new_sales_subsidized`| 5069 | **5070-5075** (also 7047 exists for Jerusalem only — excluded, see below) |
| `second_hand_sales`         | 5069 | **7047-7052** |

The two schemes appear to represent **different historical district
boundaries** rather than just renaming. Sample comparison for
"Jerusalem subsidized" between codes 5070 (id 574341) and 7047 (id 574340)
gave wildly different value magnitudes (e.g., 2026-02 = 46 vs 3), so they
are *not* aliases. The 5070 series aligns with the 5070-coded national
total, so `new_sales_total` and `new_sales_subsidized` use 5070-based
codes consistently — this is what makes the `new_sales_free = total -
subsidized` derivation meaningful.

`second_hand_sales` is only available under 7047-7052. We persist all three
topics under the same English district names (`jerusalem`, `north`, etc.)
even though `second_hand_sales`'s underlying boundaries may differ subtly from
the other two. **Future investigators: if CBS changes coding schemes,
verify this alignment by re-running the catalog probe and comparing
overlapping periods between 5070-Jerusalem and 7047-Jerusalem.**

## Why 574341 (not 574340) for subsidized Jerusalem

`new_sales_subsidized` is the only topic where Jerusalem appears under
both coding schemes simultaneously: id=574341 (`name_id=5070, "ירושלים"`)
and id=574340 (`name_id=7047, "מחוז ירושלים"`). Comparison of recent
observations (2025-05 through 2026-02):

| Period | 574341 (5070) | 574340 (7047) |
|---|---|---|
| 2026-02 | 46 | 3 |
| 2026-01 | 38 | null |
| 2025-12 | 132 | 4 |
| 2025-06 | 174 | 6 |
| 2025-05 | 86 | 42 |

Both are data=1, both updated 2026-04-09 — but the values are not just
re-scaled, they're a different population. Using 574340 would also break
the `new_sales_free` derivation since `new_sales_total` for Jerusalem is
under code 5070. The script uses 574341.

## active national — derivation methodology

The catalog under `[44,4,3]` (active construction, by district) has 7
quarterly series (6 districts + judea_samaria) but **no quarterly
national-total series**. A yearly national exists (id=674320) but at the
wrong frequency.

Active national is derived in `derive_active_national()` as
`sum(jerusalem, north, haifa, center, tel_aviv, south)` per quarter. This
excludes the judea_samaria contribution (~0.5% of national activity at
recent levels) but keeps the geographic scope consistent with the rest of
this ETL, which excludes judea_samaria everywhere.

The derived row is written with:
- `series_id = 'DERIVED'`
- `is_derived = TRUE`
- `series_name = 'בנייה פעילה - סך כולל (חישוב: סכום ערכי 6 המחוזות)'`
- `is_provisional = TRUE` if any of the 6 sources for that quarter is
  provisional

If any of the 6 districts fails to fetch, the entire active-national
derivation is skipped (rather than producing a partial sum that would
under-state the real number). Per-quarter: a quarter is only included
when all 6 districts have a value for that quarter.

## new_sales_free — derivation methodology

`new_sales_free = new_sales_total - new_sales_subsidized` per (district,
period). Computed in `derive_new_sales_free()`. Provisional flag is the
OR of the two source flags.

Per-district independence: if either source is missing for a given
district, that district is skipped — other districts continue. The
derivation depends on the 5070-coded districts being aligned across the
two source topics (see "District-code dual schemes" above).

The derived row is written with:
- `series_id = 'DERIVED'`
- `is_derived = TRUE`
- `series_name = 'דירות חדשות שנמכרו בשוק החופשי (חישוב: סך הכל פחות סבסוד ממשלתי)'`

## Topic-to-path summary

For quick reference (mapping verified 2026-04-25):

| Topic | Path | Frequency | Geo coverage |
|---|---|---|---|
| `permits`              | 44,11,2 | monthly   | national + 6 districts |
| `starts`               | 44,2,3  | monthly   | national + 6 districts |
| `completions`          | 44,3,3  | monthly   | national + 6 districts |
| `active`               | 44,4,3  | quarterly | 6 districts + derived national |
| `new_sales_total`      | 44,10,2,1 | monthly | national + 6 districts |
| `new_sales_subsidized` | 44,10,2,2 | monthly | national + 6 districts |
| `new_sales_free`       | DERIVED   | monthly | national + 6 districts |
| `second_hand_sales`          | 44,10,7   | monthly | national + 6 districts |
| `new_inventory`        | 44,10,4   | monthly | national only |

Series IDs are listed in `TOPICS` in `etl/fetch_cbs_series.py`.
