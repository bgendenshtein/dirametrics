# DiraMetrics Data Catalog v1

_Finalized: April 24, 2026_

This document defines every data series that will appear on the site, how it's sourced, how it's displayed, and how it behaves under filters.

---

## Dashboard structure

The dashboard consists of **5 tables**, a **KPI hero section**, and **shared filters**.

---

## Table 1: שיעורי ריבית (Interest Rates)

**Geography:** National only
**Native frequency:** Mixed (normalized to monthly for display)

| Series | Source | Native frequency | Notes |
|--------|--------|------------------|-------|
| Bank of Israel base rate | BoI | Daily | End-of-month value used for monthly display |
| Avg. unindexed mortgage rate | BoI | Monthly | Simple average of 15+ year sub-categories |
| Avg. indexed mortgage rate | BoI | Monthly | Simple average of 15+ year sub-categories |

---

## Table 2: בניה (Construction)

**Geography:** By district (6 districts + national)
**Native frequency:** Monthly, original values (not seasonally adjusted)
**Source:** CBS

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
**Source:** CBS

| Series | Geography | Type |
|--------|-----------|------|
| New apartments sold (free market) | By district | Flow |
| New apartments sold (government subsidized) | By district | Flow |
| Second-hand apartments sold | By district | Flow |
| New apartment inventory | National only | Stock |

---

## Table 4: מחירים כללי (General Prices)

**Geography:** National only
**Native frequency:** Monthly, original values
**Source:** CBS

| Series | Notes |
|--------|-------|
| Housing price index | Primarily reflects second-hand apartments |
| New housing price index | New apartments only |
| Rent index | For comparison with apartment prices |
| CPI (Consumer Price Index) | General inflation reference line |

---

## Table 5: מחירים לפי מחוזות (Prices by District)

**Geography:** By district
**Native frequency:** Monthly, original values
**Source:** CBS

| Series | Notes |
|--------|-------|
| Housing price index | Same index as Table 4, broken down by district |

---

## KPI Hero Section

Displayed prominently at the top of the page, above all tables:

| KPI | Calculation | Source |
|-----|-------------|--------|
| Current BoI base rate | Latest available value | BoI |
| Current avg. mortgage rate | Latest 15+ year avg. (unindexed, or user toggle?) | BoI |
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

These may be added in future iterations but are out of scope for v1.

---

## Open decisions deferred for later

- **Affordability index formula** — needs detailed thought; will include monthly payment-to-income ratio and equity requirement component
- **Mortgage rate KPI:** which to show (unindexed, indexed, or a combined metric)?

---

## Data source APIs (technical reference)

| Source | API type | Endpoint base |
|--------|----------|---------------|
| Bank of Israel | SDMX REST | `https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/` |
| CBS | Mixed (REST API + XLSX downloads) | `https://api.cbs.gov.il/` and various XLSX URLs |

CBS data availability via API varies by series. Some series (prices, rent, CPI) are via API; others (construction, sales) may require XLSX parsing. Exact mapping to be determined during ETL development.

---

## Scope for v1 implementation

Based on the conversation about phased launch, v1 will include:

- All 5 tables fully populated
- KPI hero (excluding affordability index, which is deferred)
- All filters functional
- Hebrew only (English deferred)

Phase 2 will add affordability index, English translation, and any data series flagged during user testing.
