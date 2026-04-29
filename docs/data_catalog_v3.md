# DiraMetrics Data Catalog

_Version 3 — updated April 25, 2026_

This document defines every data series that appears on the site, how it's sourced, how it's stored, and how it's displayed.

Version 3 reflects the completion of all primary ETL work. Changes from v2 are marked ⚠️.

---

## Implementation status

⚠️ **All primary data sources are now live.** Of the 15 planned series, 14 are flowing automatically. The one outstanding (CBS new housing price index, series 70000) is blocked by a server-side error on CBS's end.

| Series | Status | Supabase table | Rows |
|--------|--------|----------------|------|
| BoI base rate | ✅ Live | `boi_base_rate` | ~12,000 |
| BoI mortgage rates (indexed fixed + unindexed fixed) | ✅ Live | `boi_mortgage_rates` | ~350 |
| CBS housing price index (national) | ✅ Live | `cbs_price_indices` | ~385 |
| CBS housing price index (6 districts) | ✅ Live | `cbs_price_indices` | ~600 |
| CBS rent index | ✅ Live | `cbs_price_indices` | ~519 |
| CBS CPI | ✅ Live | `cbs_price_indices` | ~895 |
| CBS new housing price index | ⚠️ Blocked — CBS API 500 error | — | — |
| CBS building permits (national + 6 districts) | ✅ Live | `cbs_series` | ~2,604 |
| CBS construction starts | ✅ Live | `cbs_series` | ~2,604 |
| CBS construction completions | ✅ Live | `cbs_series` | ~2,604 |
| CBS active construction (quarterly) | ✅ Live | `cbs_series` | ~744 |
| CBS new apartments sold — total | ✅ Live | `cbs_series` | ~1,142 |
| CBS new apartments sold — subsidized | ✅ Live | `cbs_series` | ~580 |
| CBS new apartments sold — free market (derived) | ✅ Live | `cbs_series` | ~580 |
| CBS second-hand apartments sold | ✅ Live | `cbs_series` | ~1,142 |
| CBS new apartment inventory | ✅ Live | `cbs_series` | ~86 |

**Total: ~27,000 rows of authoritative real estate data, refreshed daily via GitHub Actions.**

---

## Dashboard structure

The dashboard consists of **5 tables**, a **KPI hero section**, and **shared filters**.

---

## Table 1: שיעורי ריבית (Interest Rates)

**Geography:** National only
**Source:** Bank of Israel (SDMX API)

| Series | Native frequency | Notes |
|--------|------------------|-------|
| BoI base rate | Daily | Benchmark set by Monetary Committee ~8x/year; values repeat between decisions |
| Unindexed fixed mortgage rate (ריבית משכנתא לא צמודה קבועה) | Monthly | BoI volume-weighted average |
| Indexed fixed mortgage rate (ריבית משכנתא צמודה קבועה) | Monthly | BoI volume-weighted average |

**Historical note (v2 → v3):** Originally we planned a "15+ year average" of mortgage rates. After investigation, BoI's SDMX API doesn't expose duration-bucketed series — only aggregate rates by indexation type and rate structure. We selected fixed-rate variants because most new Israeli mortgages are fixed.

---

## Table 2: בניה (Construction)

**Geography:** National + 6 districts (Jerusalem, North, Haifa, Center, Tel Aviv, South)
**Native frequency:** Monthly (active is quarterly), original values
**Source:** CBS (time-series API at `apis.cbs.gov.il/series`)

| Series | Type | Frequency | Notes |
|--------|------|-----------|-------|
| Permits (היתרי בניה) | Flow | Monthly | Issued permits per period |
| Starts (התחלות בניה) | Flow | Monthly | Construction starts per period |
| Completions (גמר בניה) | Flow | Monthly | Completions per period |
| Active construction (בבניה פעילה) | Stock | **Quarterly** | End-of-quarter inventory; national value derived from sum of districts |

**View modes:**
- **Mode A (Single indicator):** User picks one metric, plotted as line/bar over time
- **Mode B (Pipeline comparison):** 3 bars side-by-side per period showing permits + starts + completions. Active construction excluded (stock, not flow)

**Active national derivation:** CBS does not publish a quarterly national value for active construction. We compute it as sum of the 6 district values per quarter, marking `is_derived = TRUE`. Excludes Judea & Samaria (~0.5% of total).

---

## Table 3: מכירות (Sales)

**Native frequency:** Monthly, original values
**Source:** CBS (time-series API)

| Series | Geography | Type | Notes |
|--------|-----------|------|-------|
| New apartments sold — total | National + 6 districts | Flow | All new apartments sold |
| New apartments sold — subsidized | National + 6 districts | Flow | Government-subsidized sales |
| New apartments sold — free market (derived) | National + 6 districts | Flow | total − subsidized; `is_derived = TRUE` |
| Second-hand apartments sold | National + 6 districts | Flow | Resale market |
| New apartment inventory | National only | Stock | Unsold new apartment supply |

**Free-market derivation:** Computed at ETL time per (district, period): `total - subsidized`. Marked `is_derived = TRUE`. Self-documenting series_name in Hebrew.

**Inventory note:** CBS publishes new apartment inventory only nationally. District-level inventory is available in the press-release XLS files but not via the time-series API. Out of scope for v1.

---

## Table 4: מחירים כללי (General Prices)

**Geography:** National only
**Native frequency:** Monthly, original values
**Source:** CBS (price-index API at `api.cbs.gov.il/index`)

| Series | CBS ID | Notes |
|--------|--------|-------|
| Housing price index | 40010 | Primarily reflects second-hand apartments |
| ⚠️ New housing price index | 70000 | **BLOCKED:** CBS API returns HTTP 500 for this series |
| Rent index | 120460 | Tenant-paid rent; private + public + long-term controlled |
| CPI (Consumer Price Index) | 120010 | General inflation reference line |

**Provisionality rule:** The 3 most recent values per series are flagged as provisional in the database (`is_provisional = TRUE`). This reflects CBS's documented behavior for the housing price index. The same flagging is applied to rent and CPI for consistency, though those may not actually have provisional values — refinement pending.

**`total_items` discrepancy (v3 note):** CBS's price-index API reports 427 observations for series 40010 but only 385 are retrievable. The "missing" 42 are pre-1994 entries that error with HTTP 500 when queried. Treat the 385 as canonical; the 427 metadata count is incorrect.

---

## Table 5: מחירים לפי מחוזות (Prices by District)

**Geography:** 6 districts
**Native frequency:** Monthly, original values
**Source:** CBS (price-index API)

| District | CBS ID | Available from |
|----------|--------|----------------|
| Jerusalem | 60000 | October 2017 |
| North | 60100 | October 2017 |
| Haifa | 60200 | October 2017 |
| Center | 60300 | October 2017 |
| Tel Aviv | 60400 | October 2017 |
| South | 60500 | October 2017 |

**UI consideration:** District data starts October 2017, while the national index goes back to 1994. The UI should handle this gracefully — show the gap as missing data, not zero.

---

## KPI Hero Section

Displayed prominently at the top of the page:

| KPI | Calculation | Source |
|-----|-------------|--------|
| Current BoI base rate | Latest available value | BoI |
| Current mortgage rate | TBD — which of indexed/unindexed to headline | BoI |
| YoY housing price change | % change vs. 12 months ago | CBS (computed) |
| Affordability index | **TBD** — will incorporate monthly payment vs. income + equity requirement | Computed |

---

## Filters

Shared across tables where applicable:

| Filter | Options |
|--------|---------|
| Time range | Presets (1Y, 3Y, 5Y, Max) + custom range |
| Geography | National / District (where applicable) |
| Time frequency | Monthly / Quarterly / Semi-annual / Annual |
| View mode | Single / Pipeline (Table 2 only) |

---

## Aggregation logic (display frequency)

When the user picks a frequency coarser than the native data, data is aggregated on the fly per these rules:

| Data type | Aggregation method |
|-----------|-------------------|
| Interest rates | End-of-period value |
| Indices (housing, rent, CPI) | End-of-period value |
| Flow counts (permits, starts, completions, sales) | Sum across period |
| Stocks (inventory, active construction) | End-of-period value |

The user does not choose aggregation — it's selected automatically based on the nature of each series.

---

## Database architecture

⚠️ **Three Supabase tables in active use:**

### `boi_base_rate`

```
id, date (UNIQUE), rate, created_at
```

### `boi_mortgage_rates`

```
id, series_id, series_name, is_indexed, rate_type, date, rate, created_at
UNIQUE(series_id, date)
```

Currently contains: indexed fixed, unindexed fixed.

### `cbs_price_indices`

```
id, series_id, series_name, date, value, is_provisional, created_at
UNIQUE(series_id, date)
```

Currently contains: housing price index (national + 6 districts), rent index, CPI.

### `cbs_series`

```
id, series_id, series_name, topic, district, frequency, time_period, value,
is_provisional, is_derived, created_at
UNIQUE(topic, district, frequency, time_period)
```

Currently contains: permits, starts, completions, active, new_sales_total, new_sales_subsidized, new_sales_free, second_hand_sales, new_inventory across all available districts.

### Why two CBS tables instead of one?

- `cbs_price_indices` predates the `cbs_series` table and is structured around CBS's price-index API (different API, different series ID space, different schema needs)
- `cbs_series` is the more general-purpose design with topic + district + frequency dimensions
- Future refactor opportunity: consolidate into one table. Deferred — current split works.

---

## Data source APIs (technical reference)

| Source | API base | Notes |
|--------|----------|-------|
| Bank of Israel | `https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/` | SDMX standard |
| CBS price indices | `https://api.cbs.gov.il/index/` | REST, JSON; reports unreliable `total_items` |
| CBS time series | `https://apis.cbs.gov.il/series/` | REST, JSON; **note "apis" plural** — different host than price API |

⚠️ **Critical operational notes:**

- All CBS APIs require a `User-Agent` header. Without it, requests may be silently rejected.
- CBS time-series API has **non-deterministic pagination** — pages sometimes return the full series. The ETL handles this with per-series TimePeriod dedup.
- CBS time-series API uses **MM-YYYY** format for `startperiod`/`endperiod` filters (not the YYYY-MM format used in `obs.TimePeriod`).
- CBS price-index API uses YYYY-MM and is deterministic.
- CBS APIs occasionally return HTTP 500 for healthy queries (transient flakiness). The ETL retries with exponential backoff.

---

## Resilience layer (ETL operational)

⚠️ **Implemented in `fetch_cbs_series.py` (full layer) and `fetch_cbs_price_indices.py` (HTTP retry only):**

- **HTTP retry** on 500/502/503/504 and connection errors. 3 retries with backoff sequence [2s, 5s, 15s]. Distinguishes transient (recovered) from persistent (raised) failures in logs.
- **Pagination dedup** by TimePeriod within each series. Defends against CBS's non-deterministic pagination.
- **Gap detection** — after initial sweep, identifies missing observations and re-queries via `startperiod`/`endperiod`.
- **Lenient coverage acceptance** — if initial sweep collects ≥95% of `total_items`, accept partial data with a warning (handles both "couldn't fill" and "gap-fill request errored" scenarios).
- **Per-series failure isolation** — one failed series doesn't kill the run; failures are collected and surfaced at the end.
- **Pre-upsert deduplication** — final safety net before writing to Supabase.

The BoI ETLs do not yet have this layer (BoI's API has shown no flakiness; resilience parity is in TODO).

---

## What is NOT included (explicit exclusions)

- Seasonally adjusted data (we use original only)
- Transaction prices in shekels (only indices)
- Mortgage origination volumes
- Rent levels in shekels (only the index)
- Daily frequency display (monthly is the finest granularity shown)
- Mortgage rate sub-categories by loan duration (not available via BoI SDMX API)
- District-level apartment inventory (not available via CBS time-series API)
- Judea & Samaria district (excluded by project scope; would need user investigation)

These may be added in future iterations but are out of scope for v1.

---

## Open decisions deferred for later

- **Affordability index formula** — needs detailed thought; will include monthly payment-to-income ratio and equity requirement component
- **Mortgage rate KPI:** which to show (unindexed fixed, indexed fixed, or combined metric)?
- **New housing price index (series 70000):** API broken on CBS's side; revisit periodically
- **Resilience parity** for BoI ETLs (currently TODO)
- **CBS table consolidation** — `cbs_price_indices` and `cbs_series` could merge eventually

---

## Scope for v1 implementation

Based on the phased launch plan:

- All 5 tables fully populated ✅ (data is in place)
- KPI hero (excluding affordability index, which is deferred)
- All filters functional
- Hebrew only (English deferred)

**Phase 2 will add:** affordability index, English translation, the new housing price index (if CBS fixes the API), and any data series flagged during user testing.

---

## Change log

**v3 (April 25, 2026):**
- Marked all primary ETLs as live (4 of 4 main scripts)
- Added `cbs_series` table reference and full topic listing
- Added implementation status table with row counts
- Documented the time-series API discovery (apis.cbs.gov.il vs api.cbs.gov.il)
- Documented resilience layer (HTTP retry, pagination dedup, gap recovery, lenient coverage)
- Documented the `total_items` discrepancy on CBS price-index API
- Refined mortgage rate description: BoI volume-weighted, fixed only, no duration buckets
- Added 6 district-level housing price indices to Table 5

**v2 (April 24, 2026):**
- Corrected housing price index frequency: bi-monthly → monthly
- Revised mortgage rate approach
- Added implementation status table

**v1 (April 24, 2026):**
- Initial catalog after design session
