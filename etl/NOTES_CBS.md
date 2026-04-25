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

## Transient-error retry (HTTP 5xx, dropped connections)

The CBS time-series API has been observed to return transient HTTP 500
errors mid-run from the GitHub Actions cloud workflow, while the same
script seconds earlier ran cleanly from a developer laptop. The pattern
is server-side intermittent flakiness (likely backend overload or
restart), not a configuration problem on our side.

`_http_get_with_retry()` wraps every `session.get()` in `_fetch_pages`
with retry-on-transient logic:

- **Retried statuses:** HTTP 500, 502, 503, 504 (`TRANSIENT_HTTP_STATUSES`)
- **Retried exceptions:** `requests.exceptions.ConnectionError`,
  `ChunkedEncodingError`, `Timeout` (covers `ReadTimeout` /
  `ConnectTimeout` via inheritance)
- **Backoff:** `HTTP_RETRY_BACKOFF_SEQUENCE = [2, 5, 15]` seconds, so
  4 total tries: initial + 3 retries
- **4xx and other non-listed 5xx are NOT retried** — those represent
  real bugs (bad params, bad series ID, etc.) and should fail loudly

Logging is structured so cloud-log skim distinguishes the two cases:
- *Transient & recovered* — INFO line `"Series N: succeeded after K
  retr(y/ies)"` after the retry that fixed it
- *Transient & persistent* — WARNING line `"Series N: HTTP 500
  persisted after 4 attempts"` (or analogous for connection errors),
  then the error propagates → caller surfaces it via the failures list
- *Clean run* — no retry-related output at all

This retry sits **below** the gap-recovery retry layer (`MAX_RETRIES`,
`_find_gaps`). HTTP retry handles a single failed request; gap recovery
handles a successful-but-incomplete paginated sweep. They compose:
worst-case for one series is `MAX_RETRIES × pages × (HTTP_RETRY_ATTEMPTS+1)`
HTTP calls, but with backoff this stays well under the workflow timeout
in practice.

### Why upsert is NOT retried

Supabase upsert errors (e.g., the `ON CONFLICT cannot affect row a
second time` error we hit earlier) indicate real problems in our row
collection — duplicate keys, schema mismatches, or constraint
violations. Retrying would silently mask those by waiting for the same
deterministic failure to "go away" — it won't. Upsert failures abort the
run loudly so the underlying bug surfaces.

## Lenient gap-recovery acceptance

The CBS API's pagination flakiness (see "Transient-error retry" above
and the `_find_gaps` machinery in `fetch_series`) sometimes leaves a
handful of observations stuck even after gap recovery — the targeted
`startperiod`/`endperiod` queries themselves can also be flaky. For a
series that already had a near-complete initial sweep, failing the whole
series for the sake of 1–4 missing observations is overkill: the
workflow would go red, alerts fire, and re-running the next day usually
recovers.

`GAP_RECOVERY_MIN_COVERAGE = 0.95` controls the lenient acceptance gate:

- **If initial-sweep coverage ≥ 95%** AND gap recovery still couldn't
  fill the residual: log a WARNING (`gap recovery exhausted; persisting
  partial data. initial=N/M (X.XX%), final=N/M (Y.YY%), residual gaps:
  ...`) and persist what we have. Series is **not** added to the
  failures list; the workflow exits zero on this account.
- **If initial-sweep coverage < 95%**: raise `IncompleteSeriesError`
  with the same diagnostic detail. The series is added to failures and
  the workflow exits non-zero. Sub-95% on the first sweep usually means
  something structural is wrong (auth, wrong series ID, deep API
  outage), so it deserves to fail loudly.

The gate is on the **initial** sweep deliberately, not on the final
post-retry coverage. This way, a series that started with deep coverage
problems still fails even if gap recovery happens to claw back 95% of
the data — that's a "we got lucky" situation we don't want to silently
accept. Only series that were near-complete from the first try and just
couldn't squeeze the last few obs through transient flakiness pass
through.

Tune `GAP_RECOVERY_MIN_COVERAGE` if needed: lower (e.g., 0.90) to
tolerate more aggressive flakiness; higher (e.g., 0.98) to insist on
near-perfection. Setting it to 1.0 disables the lenient path entirely
and reverts to the strict "any missing obs fails the series" behavior.

## Resilience parity between the two CBS scripts

Both `fetch_cbs_series.py` and `fetch_cbs_price_indices.py` ship the
same HTTP-level resilience layer:

- `_http_get_with_retry()` helper with identical signature and log format
- Same constants: `HTTP_RETRY_ATTEMPTS = 3`,
  `HTTP_RETRY_BACKOFF_SEQUENCE = [2, 5, 15]`,
  `TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}`,
  `TRANSIENT_EXCEPTIONS = (ConnectionError, ChunkedEncodingError, Timeout)`
- Same per-series pagination dedup safeguard (defense-in-depth)
- Both create a `requests.Session` with a User-Agent header

This is copy-and-adapt parity — extracting the shared helper into a
common module is a future refactor, deferred to keep this change
focused.

### What does NOT carry over: gap recovery / lenient acceptance

The price-index script does **not** implement targeted gap-recovery
(`startperiod`/`endperiod` re-querying), `GAP_RECOVERY_MIN_COVERAGE`,
or `IncompleteSeriesError`. Reasoning, based on direct probing
2026-04-25:

| Behavior | Time-series API (`apis.cbs.gov.il`) | Price-index API (`api.cbs.gov.il`) |
|---|---|---|
| Pagination determinism | Non-deterministic (one page sometimes returns the full series; cursor drifts when page 1 is short) | Deterministic across 3 successive runs |
| `paging.total_items` accuracy | Honest — matches what's available | Consistently ~10% higher than what's actually retrievable (e.g., 427 reported vs 385 retrievable for series 40010, 1994-01 → 2026-01 contiguous, no internal gaps; pre-1994 startperiod returns HTTP 500) |
| Need for gap recovery | YES — observed real interior gaps (e.g., 4 missing months for permits) that get filled by re-query | NO — pagination is reliable; the 42-obs shortfall is a counter quirk, not actual missing data |

If gap recovery were backported as-is, every price-index series would
read as 90% coverage on every run, fall below the 95% lenient threshold,
and be raised as `IncompleteSeriesError` — actively breaking the working
flow. So we keep this script's logic narrow: HTTP retry + dedup, no
gap-recovery layer.

### `startperiod` / `endperiod` accept both formats on the price-index API

For the record, even though we don't use them in the script:

- The time-series API (`apis.cbs.gov.il/series/`) docs specify
  `startperiod=MM-YYYY&endperiod=MM-YYYY` (month-first).
- The price-index API (`api.cbs.gov.il/index/`) accepts **both**
  `MM-YYYY` and `YYYY-MM` and returns identical results for either.

If we ever need targeted re-fetches on the price-index API (e.g., for
operational backfill scripts), use `MM-YYYY` for consistency with the
series API.

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
