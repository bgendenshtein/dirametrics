# DiraMetrics — Design Brief

_Project: residential real estate dashboard for Israel · Site: dirametrics.co.il_
_Brief drafted: April 25, 2026 · Stage: design phase opening_

---

## Project in one sentence

DiraMetrics is a Hebrew-language web dashboard that gives Israeli real estate professionals a clean, deeply historical view of the country's housing market — combining data from the Bank of Israel and the Central Bureau of Statistics into one place that the source agencies don't provide themselves.

## Audience (tight focus)

The primary user is the **Israeli real estate professional**: brokers, agents, mortgage advisors, investors, analysts at financial firms, and journalists covering the property market. They:

- Already know what "permits," "starts," "active construction," "indexed mortgage" mean. No glossary needed.
- Want to check "what's the latest" but also "how does this compare to 5/10/20 years ago."
- Need to ask comparison questions across data categories (e.g., "how does the BoI rate move with second-hand sales volume?").
- Will visit weekly or monthly, not daily. This is a reference tool, not a live feed.
- Use both desktop and mobile, but spend more meaningful time on desktop.
- Are time-pressured. The fewer clicks to insight, the better.
- Read Hebrew natively. Many also read English fluently but the product is Hebrew-only at v1.
- Form mental models of the market over time. The dashboard's job is to keep that mental model accurate and current.

We are not designing for: tourists, casual citizens curious about the market, first-time buyers, or international investors. Those audiences may use the site, but the design serves the professional.

## Aesthetic direction

**Reference: Stripe.com.** Specifically the *feeling* of Stripe — restrained, generous, modern, deliberate. We want the same impression of "this product knows exactly what it's doing." Not Stripe's specific colors or typography (we need our own), but Stripe's spirit:

- Generous whitespace
- Numbers and charts treated as design elements, not afterthoughts
- Sans-serif type at thoughtful sizes
- Subtle hierarchy through weight and spacing, not boxes and dividers
- Quiet confidence — no marketing language, no exclamation points, no "Get Started!" buttons

**Avoid:**
- Bloomberg-style density (too professional-trader, too aggressive)
- Yahoo Finance information overload
- Government-site utilitarian flatness (this is what we're improving on)
- Consumer-app brightness (DiraMetrics is for serious work)
- Onboarding modals, tour overlays, beginner accommodations
- Stock photos of buildings or families
- Hebrew typography that feels like it was an afterthought (a constant problem on Israeli sites — fix this)

## Core architectural decision: flexible chart slots, not category-locked charts

The dashboard's central interaction model is **two flexible chart slots**, side by side, where the user picks which data series to display in each.

This is intentionally different from the more obvious approach of "one chart per data category" (one chart for prices, one for construction, etc.). The reason: real estate professionals think in **relationships** — "did rates correlate with sales?" — which require putting different data types on the same chart. Locking each chart to one category would prevent the cross-category analysis that makes the dashboard valuable.

The categorization of data still exists — it lives in the **series picker**, not the page layout.

## Page architecture

**Single scrollable page, no tabs.**

Vertical structure (top to bottom):

1. **KPI hero strip** (small) — current readings: BoI rate, current mortgage rate, YoY housing change. Supporting context, not the main act. Compact horizontal strip across the top.

2. **Two flexible chart slots, side by side** — the dashboard's main interaction surface. Each chart has its own controls and series.

3. **About / methodology page** — separate URL, accessible via header link.

The two-chart layout is fixed. Charts are not added or removed by the user. We deliberately limit to two to give each chart room to breathe and to keep the page focused.

## Chart slots: how they work

Each chart slot is independently configurable.

### Series selection (per chart)

- User can put **1 to 5 series** on a chart
- Series are picked via a **categorized picker** organized by:
  - Interest rates (3 series)
  - Construction (4 series)
  - Sales (4 series)
  - General prices (3 series, will be 4 once CBS series 70000 is fixed)
  - Prices by district (6 series)
- Each picker entry shows series name in Hebrew and a brief unit/frequency hint
- Adding a series displays it on the chart immediately
- Removing happens via the legend (X next to series name)

### Smart visual defaults (per series)

When a series is added, the system picks a sensible default visual treatment:

- **Indices and rates** (housing price index, rent index, CPI, BoI rate, mortgage rates) → **line**
- **Counts and volumes** (permits, starts, completions, sales) → **bar**
- **Stocks** (active construction, inventory) → **area**

The user can override per-series via a small icon in the legend: line ↔ bar ↔ area. Most users will never need to.

### Multi-axis handling

When the user puts series with different units on the same chart (e.g., a price index + a percentage rate + a sales count), the chart **automatically uses multiple Y-axes**. The user doesn't think about this — the system makes it work.

Practical limit: up to 3 distinct unit types per chart (anything beyond gets visually busy).

### Chart-level controls

Below each chart, a compact controls bar with:

- **Time range** — presets (1Y / 3Y / 5Y / 10Y / Max) + custom range picker. Visible always.
- **Series picker** — opens the categorized picker. Visible always.
- **More** (expandable) — hidden by default; reveals:
  - **Frequency** — Monthly / Quarterly / Semi-annual / Annual
  - **Display mode** — Absolute / Indexed (base 100) / % change

The most-used controls (time range, series picker) are always visible. The deeper controls are one click away. Stripe-style restraint.

### Filter scope: per-chart with optional broadcast

Filters are **independent per chart by default**. Changing the time range on the left chart does NOT change the right chart.

**Why:** a user might want to see 20 years of prices on the left while looking at 5 years of construction starts on the right. Forcing global filters would prevent useful cross-comparisons.

However, when a user changes a filter, a contextual microinteraction appears: **"Apply to other chart? ✓"** — one click broadcasts the change. The microinteraction fades after a few seconds if not used.

This pattern preserves flexibility while enabling consistency on demand.

## Default state

When a user arrives, the two charts are pre-configured with sensible defaults:

**Left chart:**
- Housing price index (line)
- Unindexed fixed mortgage rate (line)
- New apartments sold — total (bar)

**Right chart:**
- Construction starts (bar)
- Construction completions (bar)
- New apartment inventory (area)

Both charts default to a 5-year time range, monthly frequency, absolute display mode.

The user can replace any series at any time via the picker.

## Above the fold (first 3 seconds)

When someone arrives, they should perceive three things in this order:

1. **Both charts are visible immediately** with their default series populated. Charts are the hero, not marketing copy.
2. **The visual obviously communicates "Israeli residential real estate data."** Through chart subjects, the page title, and Hebrew copy.
3. **The interface clearly suggests it's customizable.** The series picker affordance and the time-range controls beneath each chart should be visible enough that users perceive "I can adjust what I'm looking at." Without those controls dominating the screen.

The KPI hero strip across the top adds context for "what's happening right now" without requiring chart configuration.

## Mobile (responsive behavior)

On mobile, the two charts **stack vertically** — each chart full-width, scrolls down naturally. Controls stay below each chart.

The KPI hero strip wraps to two rows on narrow screens.

The series picker on mobile may use a sheet/drawer rather than the desktop dropdown.

**Mobile is secondary but must work.** A real estate broker checking a stat on the way to a meeting needs the site to function. We don't optimize FOR mobile, but we don't break it either.

## Available data series

A complete data catalog (`data_catalog_v3.md`) is provided separately. Summary:

| Category | Series | Geography | Frequency |
|----------|--------|-----------|-----------|
| Interest rates | BoI base rate | National | Daily (display monthly) |
| Interest rates | Indexed fixed mortgage rate | National | Monthly |
| Interest rates | Unindexed fixed mortgage rate | National | Monthly |
| Construction | Permits | Nat + 6 districts | Monthly |
| Construction | Starts | Nat + 6 districts | Monthly |
| Construction | Completions | Nat + 6 districts | Monthly |
| Construction | Active construction | Nat + 6 districts | Quarterly |
| Sales | New sales — total | Nat + 6 districts | Monthly |
| Sales | New sales — subsidized | Nat + 6 districts | Monthly |
| Sales | New sales — free market | Nat + 6 districts | Monthly |
| Sales | Second-hand sales | Nat + 6 districts | Monthly |
| Sales | New apartment inventory | National only | Monthly |
| General prices | Housing price index | National | Monthly |
| General prices | Rent index | National | Monthly |
| General prices | CPI | National | Monthly |
| Prices by district | Housing price index | 6 districts | Monthly |

For series with district-level data, the picker entry should let the user pick which geography (or default to national).

## Constraints

- **RTL by default.** Hebrew is the primary language. The entire layout flows right-to-left.
- **Hebrew typography matters.** Recommend Heebo or Rubik, both of which support Hebrew + Latin cleanly. Tabular figures essential for chart axes and KPI numbers.
- **Mobile must work.** Vertical stack, full-width charts.
- **Accessibility:** Israeli law requires WCAG 2.0 AA for public sites. Color choices, contrast ratios, and keyboard navigation all need to meet this bar.
- **Performance:** Charts should feel snappy when users add/remove series or change ranges.
- **Dark mode:** Optional but desirable. Real estate professionals often work late.

## What we want from this design phase

- A logo wordmark and small mark
- A color system (primary, neutral, accent, semantic + chart-series palette supporting up to 5 distinguishable colors)
- A type system (one Hebrew + Latin family)
- Component library:
  - KPI strip cards
  - Chart styles: line, bar, stacked area; with multi-axis support
  - Categorized series picker (the key custom component)
  - Filter chips and dropdowns
  - Time-range picker
  - "Apply to other chart" microinteraction
  - Interactive legend with per-series visual-type toggle
  - "More" expansion for hidden controls
- Page layout: home page (KPI hero + 2 charts), about/methodology page
- Mobile responsive behavior
- Empty state, loading state, error state ("data couldn't load right now")

We do not need: marketing pages, onboarding flows, in-app help, settings panels.

## What's NOT a brief decision

These are deliberately left to the design phase to determine:

- The specific color palette
- Logo direction (modern wordmark vs. abstract mark vs. illustrated)
- Whether to surface the (deferred) affordability index in the KPI hero
- Chart styling specifics (axis treatment, gridlines, animation behavior, hover tooltips)
- Exact treatment of the "apply to other chart" microinteraction (toast? inline pill? hover affordance?)
- Whether the controls bar should be sticky as the user scrolls within a long chart
- Exact treatment of the categorized series picker (sectioned dropdown? sheet? combo box with categories?)

These are creative decisions where the designer's judgment beats my prescription.

## Existing context

- The site is live at dirametrics.co.il with one placeholder chart. Currently embarrassingly thin — this is what we're replacing.
- All data is in Supabase, refreshed daily. Frontend is React + TypeScript + Vite + Tailwind + Recharts.
- ~27,000 rows of historical data are already loaded.
- Domain authority and competition: Bruchim Habaim and CBS's portal exist. Bruchim is clean but lacks history and downloads. CBS's portal is comprehensive but UX-poor. DiraMetrics differentiates on **historical depth, exportability, and cross-category analysis** (the flexible chart slots are the differentiator).

## Tone of the design

We're treating this as a serious professional product, not a portfolio piece. The design should feel like something an Israeli mortgage broker could open in front of a client and feel proud to be using. Not impressive in an "look at this fancy thing" way; impressive in a "this person uses real tools" way.
