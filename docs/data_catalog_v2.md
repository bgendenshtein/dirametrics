# DiraMetrics Data Catalog

_Version 2 — updated April 24, 2026_

This document defines every data series that will appear on the site, how it's sourced, how it's displayed, and how it behaves under filters.

Version 2 reflects discoveries made during ETL implementation. Changes from v1 are marked ⚠️.

---

## Dashboard structure

The dashboard consists of **5 tables**, a **KPI hero section**, and **shared filters**.

---

## Implementation status

| Series | Status | Supabase table |
|--------|--------|----------------|
| BoI base rate | ✅ Live | `boi_base_rate` |
| BoI mortgage rates (indexed fixed + unindexed fixed) | ✅ Live | `boi_mortgage_rates` |
| CBS housing price index | ✅ Live | `cbs_price_indices` |
| CBS rent index | ✅ Live | `cbs_price_indices` |
| CBS CPI | ✅ Live | `cbs_price_indices` |
| CBS new housing price index | ⚠️ Blocked — CBS API 500 error | — |
| CBS housing price index by district | ⏭️ Not yet built | — |
| Construction: permits / starts / completions / active | ⏭️ Not yet built (XLSX path) | — |
| Sales: new (free) / new (subsidized) / second-hand / inventory | ⏭️ Not yet built (XLSX path) | — |

---

## Table 1: שיעורי ריבית (Interest Rates)

**Geography:** National only
**Source:** Bank of Israel (SDMX API)

| Series | Native frequency | Notes |
|--------|------------------|-------|
| BoI base rate | Daily | Benchmark set by Monetary Committee ~8x/year; repeats between decisions |
| ⚠️ Unindexed fixed mortgage rate (עבור משכנתא לא צמודה קבועה) | Monthly | BoI volume-weighted average across all new mortgages |
| ⚠️ Indexed fixed mortgage rate (עבור משכנתא צמודה קבועה) | Monthly | BoI volume-weighted average across all new mortgages |

**⚠️ Change from v1:** Original plan called for "average of 15+ year sub-categories." This was revised after discovering BoI's SDMX API doesn't expose duration-bucketed mortgage rates — those only exist as HTML on the BoI website. The BoI-published volume-weighted averages are the authoritative national figures used in press and by banks. Since most mortgages in Israel are 15+ years, the volume-weighted average effectively reflects that segment.

**Decisions deferred:**
- Which mortgage rate to use as headline KPI (indexed fixed vs unindexed fixed vs the spread between them)

---

## Table 2: בניה (Construction)

**Geography:** By district (6 districts + national)
**Native frequency:** Monthly, original values (not seasonally adjusted)
**Source:** CBS (XLSX download path — not yet implemented)

| Series | Type | Notes |
|--------|------|-------|
| Permits (היתרי בניה) | Flow | New permits issued per period |
| Starts (התחלות בניה) | Flow | Construction starts per period |
| Completions (סיומי בניה) | Flow | Completions per period |
| Active construction (בבניה פעילה) | Stock | End-of-period inventory |

**View modes:**
- **Mode A (Single indicator):** User picks one metric, plotted as line/bar over time
- **Mode B (Pipeline comparison):** 3 bars side-by-side per period showing permits + starts + completions. Active construction excluded (stock, not flow)

---

## Table 3: מכירות (Sales)

**Native frequency:** Monthly, original values
**Source:** CBS (XLSX download path — not yet implemented)

| Series | Geography | Type |
|--------|-----------|------|
| New apartments sold (free market) | By district | Flow |
| New apartments sold (government subsidized) | By district | Flow |
| Second-hand apartments sold | By district | Flow |
| New apartment inventory | National only | Stock |

---

## Table 4: מחירים כללי (General Prices)

**Geography:** National only
**Native frequency:** ⚠️ **Monthly** (not bi-monthly as originally stated), original values
**Source:** CBS (REST API)

| Series | CBS ID | Notes |
|--------|--------|-------|
| Housing price index | 40010 | Primarily reflects second-hand apartments |
| ⚠️ New housing price index | 70000 | **BLOCKED:** CBS API returns HTTP 500 for this series |
| Rent index | 120460 | Tenant-paid rent; private + public + long-term controlled |
| CPI (Consumer Price Index) | 120010 | General inflation reference line |

**⚠️ Change from v1 (bi-monthly → monthly):** The housing price index publishes monthly (one value per month) since 1994, with no gaps. CBS documentation describes the index's *calculation methodology* as based on a 2-month comparison window, which led to the initial misconception that the data frequency was bi-monthly. Actual data is monthly.

**⚠️ Provisionality rule:** The 3 most recent values per series are flagged as provisional in the database (`is_provisional = true`). This reflects CBS's documented behavior for the housing price index (latest 3 values may be revised). The same flagging is applied to rent and CPI for consistency, though those may not actually have provisional values — this is safe-side behavior and may be refined later.

---

## Table 5: מחירים לפי מחוזות (Prices by District)

**Geography:** By district
**Native frequency:** Monthly, original values
**Source:** CBS (not yet determined — REST API or XLSX)

| Series | Notes |
|--------|-------|
| Housing price index by district | Same methodology as Table 4, broken down by district |

---

## KPI Hero Section

Displayed prominently at the top of the page, above all tables:

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

## What is NOT included (explicit exclusions, for reference)

- Seasonally adjusted data (we use original only)
- Transaction prices in shekels (only indices)
- Transaction volumes (only counts of new vs second-hand sales)
- Mortgage origination volumes
- Rent levels in shekels (only the index)
- Daily frequency display (monthly is the finest granularity shown to the user)
- Mortgage rate sub-categories by loan duration (not available via BoI SDMX API)

These may be added in future iterations but are out of scope for v1.

---

## Open decisions deferred for later

- **Affordability index formula** — needs detailed thought; will include monthly payment-to-income ratio and equity requirement component
- **Mortgage rate KPI:** which to show (unindexed fixed, indexed fixed, or a combined metric)?
- **New housing price index (series 70000):** API is broken; to be revisited

---

## Data source APIs (technical reference)

| Source | API type | Base URL |
|--------|----------|----------|
| Bank of Israel | SDMX REST | `https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/` |
| CBS | REST (for price indices) | `https://api.cbs.gov.il/index/` |
| CBS | XLSX downloads (for construction, sales) | Various URLs per publication |

CBS construction and sales data are NOT available via the REST API. They require downloading XLSX files from CBS media-release URLs, which changes per publication. This is a known implementation challenge to be addressed in a future session.

---

## Database schema

Three Supabase tables currently in use:

### `boi_base_rate`
- id, date (UNIQUE), rate, created_at

### `boi_mortgage_rates`
- id, series_id, series_name, is_indexed, rate_type, date, rate, created_at
- UNIQUE(series_id, date)
- Currently contains: indexed fixed, unindexed fixed

### `cbs_price_indices`
- id, series_id, series_name, date, value, is_provisional, created_at
- UNIQUE(series_id, date)
- Currently contains: housing prices (40010), rent (120460), CPI (120010)

---

## Scope for v1 implementation

Based on the conversation about phased launch, v1 will include:

- All 5 tables fully populated
- KPI hero (excluding affordability index, which is deferred)
- All filters functional
- Hebrew only (English deferred)

Phase 2 will add affordability index, English translation, the new housing price index (if CBS fixes the API), and any data series flagged during user testing.

---

## Change log

**v2 (April 24, 2026):**
- Corrected housing price index frequency: bi-monthly → monthly
- Revised mortgage rate approach: volume-weighted averages (indexed fixed, unindexed fixed) instead of non-existent "15+ year average"
- Added implementation status table tracking what's built vs planned
- Added Supabase schema reference
- Added CBS series IDs as technical reference
- Flagged new housing price index (70000) as blocked
- Documented provisionality flagging behavior

**v1 (April 24, 2026):**
- Initial catalog after design session
