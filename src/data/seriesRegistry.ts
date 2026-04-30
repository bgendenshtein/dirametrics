/**
 * seriesRegistry — single source of truth for the catalog of series the
 * user can pick from. Each entry carries:
 *   - display metadata (Hebrew name, category)
 *   - chart-engine metadata (family, default visual type, isStock,
 *     unit, precision, thousands)
 *   - a fetch function that pulls the series from Supabase given an
 *     optional district
 *
 * The picker reads the categories + entries to populate its lists; the
 * card's hydration hook (useSeriesList) calls fetch() per active spec.
 *
 * Why a registry (not per-series hooks): the user can compose any
 * subset of series into either chart card. A static registry decouples
 * the card from the specific series it shows — adding a new series
 * means appending one entry, not threading another hook through the
 * UI tree.
 */

import type { DisplayMode, Frequency } from '../components/chartLayout'
import { supabase } from '../lib/supabase'

export type District =
  | 'national'
  | 'jerusalem'
  | 'north'
  | 'haifa'
  | 'center'
  | 'tel-aviv'
  | 'south'

export const DISTRICTS: { id: District; name: string }[] = [
  { id: 'national',  name: 'לאומי' },
  { id: 'jerusalem', name: 'מחוז ירושלים' },
  { id: 'north',     name: 'מחוז הצפון' },
  { id: 'haifa',     name: 'מחוז חיפה' },
  { id: 'center',    name: 'מחוז המרכז' },
  { id: 'tel-aviv',  name: 'מחוז תל אביב' },
  { id: 'south',     name: 'מחוז הדרום' },
]

/** Map our React-side district id (kebab) to the database column value
 * (snake_case). Kept here so the rest of the code base sees only the
 * kebab form. */
const DISTRICT_DB_KEY: Record<District, string> = {
  national:   'national',
  jerusalem:  'jerusalem',
  north:      'north',
  haifa:      'haifa',
  center:     'center',
  'tel-aviv': 'tel_aviv',
  south:      'south',
}

export type CategoryId =
  | 'presets'
  | 'rates'
  | 'construction'
  | 'sales'
  | 'prices-general'
  | 'prices-by-district'

export interface Category {
  id: CategoryId
  name: string
  /** Whether the series under this category accept a district selector
   * in the picker. Construction + sales topics expose national + 6
   * districts; rates and price indices do not. (The by-district price
   * indices are individual entries — one per district — rather than a
   * single entry with a selector, since each district has its own
   * series_id in cbs_price_indices.) */
  hasDistrictSelector: boolean
}

export const CATEGORIES: Category[] = [
  // Presets render as the FIRST category — clicking a preset row
  // replaces the chart's series + filters in one action. Distinct
  // rendering path in SeriesPicker (see PRESETS below).
  { id: 'presets',            name: 'תצוגות מומלצות',     hasDistrictSelector: false },
  { id: 'rates',              name: 'ריביות',           hasDistrictSelector: false },
  { id: 'construction',       name: 'בנייה',            hasDistrictSelector: true  },
  { id: 'sales',              name: 'מכירות',           hasDistrictSelector: true  },
  { id: 'prices-general',     name: 'מחירים כלליים',     hasDistrictSelector: false },
  { id: 'prices-by-district', name: 'מחירים לפי מחוז',   hasDistrictSelector: false },
]

export interface SeriesPoint {
  date: Date
  value: number
}

/** Discriminated union: registry entries are either leaves (a single
 * series with its own fetch) or groups (a packaged set of leaves
 * that the picker adds together, optionally as a stacked bar). */
export type RegistryEntry = RegistryLeafEntry | RegistryGroupEntry

export interface RegistryLeafEntry {
  kind?: 'series'
  id: string
  name: string
  category: CategoryId
  family: 'idx' | 'pct' | 'count'
  defaultType: 'line' | 'bar' | 'area'
  isStock?: boolean
  unit?: string
  precision: number
  thousands?: boolean
  /** When true, render line/area as a stepAfter interpolation rather
   * than the default monotone curve. For step-function series whose
   * value is constant between observation dates and jumps at each
   * decision (e.g., the BoI base rate, set at irregular Monetary
   * Committee meetings — interpolating curves between decisions would
   * misrepresent the underlying mechanism). */
  step?: boolean
  /** Districts the series supports:
   *   undefined / 'national-only' — district picker disabled or
   *     forced to לאומי (e.g., new_inventory: CBS publishes national-
   *     only)
   *   'all' — לאומי + 6 districts available (mirrors cbs_series rows) */
  districts?: 'national-only' | 'all'
  /** Within-family axis subgroup. Two series in the same `family` and
   * same `group` always share an axis (overriding the median-magnitude
   * heuristic in splitByMedian). Two series in the same family with
   * DIFFERENT non-null groups always get separate axes — registry
   * author's explicit declaration that the magnitudes shouldn't be
   * cross-compared on a single scale.
   *
   * Examples in this codebase:
   *   'sales'        — total/free/subsidized/secondhand sales (~3K range)
   *   'construction' — permits/starts/completions/active/inventory
   *                    (~13–70K range)
   * Both are family='count' but should never share an axis: putting
   * them together flattens the smaller range against the baseline.
   *
   * Series without `group` fall back to the family-default bucket and
   * splitByMedian still runs as a safety net for ungrouped count
   * series with very different magnitudes. */
  group?: string
  fetch: (district: District) => Promise<SeriesPoint[]>
}

/** A group entry expands into multiple leaf specs when the user
 * picks it. Each member references an existing leaf entry (so we
 * don't duplicate metadata) and optionally carries a stackId — when
 * two or more members share a stackId, the chart engine stacks them
 * as a single bar column. */
export interface RegistryGroupEntry {
  kind: 'group'
  id: string
  name: string
  category: CategoryId
  /** District support: groups inherit from their members. If any
   * member is national-only, the whole group is treated as national-
   * only in the picker. Setting it explicitly here keeps the picker
   * code simple. */
  districts?: 'national-only' | 'all'
  members: Array<{
    /** id of an existing leaf entry */
    registryId: string
    /** When set, members sharing the same stackId render as one
     * stacked column. Bottom-up render order = array order. */
    stackId?: string
  }>
}

export function isGroupEntry(e: RegistryEntry): e is RegistryGroupEntry {
  return e.kind === 'group'
}

export function getLeafEntry(id: string): RegistryLeafEntry | undefined {
  const entry = SERIES_REGISTRY.find((e) => e.id === id)
  if (!entry || entry.kind === 'group') return undefined
  return entry
}

// ---- Source-specific fetch helpers --------------------------------------

interface CbsSeriesRow { time_period: string; value: number }
interface CbsPriceRow  { date: string; value: number }
interface BoiRateRow   { date: string; rate: number }

async function fetchCbsSeries(
  topic: string,
  district: District,
  frequency: 'monthly' | 'quarterly' = 'monthly',
): Promise<SeriesPoint[]> {
  const { data, error } = await supabase
    .from('cbs_series')
    .select('time_period, value')
    .eq('topic', topic)
    .eq('district', DISTRICT_DB_KEY[district])
    .eq('frequency', frequency)
    .order('time_period', { ascending: true })
    .limit(2000)
  if (error) throw new Error(error.message)
  return ((data ?? []) as CbsSeriesRow[]).map((r) => ({
    date: new Date(r.time_period + 'T00:00:00.000Z'),
    value: Number(r.value),
  }))
}

async function fetchPriceIndex(seriesId: number | string): Promise<SeriesPoint[]> {
  const { data, error } = await supabase
    .from('cbs_price_indices')
    .select('date, value')
    .eq('series_id', seriesId)
    .order('date', { ascending: true })
    .limit(2000)
  if (error) throw new Error(error.message)
  return ((data ?? []) as CbsPriceRow[]).map((r) => ({
    date: new Date(r.date + 'T00:00:00.000Z'),
    value: Number(r.value),
  }))
}

async function fetchBoiBaseRate(): Promise<SeriesPoint[]> {
  // boi_base_rate stores one row per month (date = last calendar day
  // of month, rate = end-of-month value). The ETL aggregates the
  // upstream daily SDMX feed down to ~390 rows back to 1994 so we
  // stay well under Supabase's 1000-row default fetch limit. The
  // step-function visualization (RegistryEntry.step = true) still
  // applies — month-to-month flat segments with jumps where a
  // Monetary Committee decision changed the rate.
  const { data, error } = await supabase
    .from('boi_base_rate')
    .select('date, rate')
    .order('date', { ascending: true })
    .limit(1000)
  if (error) throw new Error(error.message)
  return ((data ?? []) as BoiRateRow[]).map((r) => ({
    date: new Date(r.date + 'T00:00:00.000Z'),
    value: Number(r.rate),
  }))
}

async function fetchMortgageRate(isIndexed: boolean): Promise<SeriesPoint[]> {
  const { data, error } = await supabase
    .from('boi_mortgage_rates')
    .select('date, rate')
    .eq('rate_type', 'fixed')
    .eq('is_indexed', isIndexed)
    .order('date', { ascending: true })
    .limit(2000)
  if (error) throw new Error(error.message)
  return ((data ?? []) as BoiRateRow[]).map((r) => ({
    date: new Date(r.date + 'T00:00:00.000Z'),
    value: Number(r.rate),
  }))
}

// ---- Registry -----------------------------------------------------------

export const SERIES_REGISTRY: RegistryEntry[] = [
  // 1. ריביות
  {
    id: 'boi-base-rate',
    name: 'ריבית בנק ישראל',
    category: 'rates',
    family: 'pct',
    defaultType: 'line',
    step: true,
    unit: '%',
    precision: 2,
    fetch: () => fetchBoiBaseRate(),
  },
  {
    id: 'mortgage-fixed-unindexed',
    name: 'ריבית משכנתא קבועה לא צמודה',
    category: 'rates',
    family: 'pct',
    defaultType: 'line',
    unit: '%',
    precision: 2,
    fetch: () => fetchMortgageRate(false),
  },
  {
    id: 'mortgage-fixed-indexed',
    name: 'ריבית משכנתא קבועה צמודה',
    category: 'rates',
    family: 'pct',
    defaultType: 'line',
    unit: '%',
    precision: 2,
    fetch: () => fetchMortgageRate(true),
  },

  // 2. בנייה
  {
    id: 'permits',
    name: 'היתרי בנייה',
    category: 'construction',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'construction',
    fetch: (d) => fetchCbsSeries('permits', d),
  },
  {
    id: 'starts',
    name: 'התחלות בנייה',
    category: 'construction',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'construction',
    fetch: (d) => fetchCbsSeries('starts', d),
  },
  {
    id: 'completions',
    name: 'גמר בנייה',
    category: 'construction',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'construction',
    fetch: (d) => fetchCbsSeries('completions', d),
  },
  {
    // active is quarterly (CBS publishes end-of-quarter inventory of
    // units under construction). The fetcher uses frequency='quarterly'.
    id: 'active',
    name: 'בנייה פעילה',
    category: 'construction',
    family: 'count',
    defaultType: 'area',
    isStock: true,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'construction',
    fetch: (d) => fetchCbsSeries('active', d, 'quarterly'),
  },

  // 3. מכירות
  {
    id: 'new-sales-total',
    name: 'מכירות חדשות (סך הכל)',
    category: 'sales',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'sales',
    fetch: (d) => fetchCbsSeries('new_sales_total', d),
  },
  {
    id: 'new-sales-subsidized',
    // Renamed from 'מכירות מסובסדות' for naming consistency with
    // the other חדשות-prefixed components — emphasizes that this
    // is a NEW (not yad-sheni) sales subset.
    name: 'מכירות חדשות מסובסדות',
    category: 'sales',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'sales',
    fetch: (d) => fetchCbsSeries('new_sales_subsidized', d),
  },
  {
    id: 'new-sales-free',
    // Renamed from 'מכירות בשוק חופשי' — same rationale as
    // new-sales-subsidized above.
    name: 'מכירות חדשות בשוק חופשי',
    category: 'sales',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'sales',
    fetch: (d) => fetchCbsSeries('new_sales_free', d),
  },
  {
    id: 'second-hand-sales',
    name: 'מכירות יד שנייה',
    category: 'sales',
    family: 'count',
    defaultType: 'bar',
    isStock: false,
    precision: 0,
    thousands: true,
    districts: 'all',
    group: 'sales',
    fetch: (d) => fetchCbsSeries('second_hand_sales', d),
  },
  {
    id: 'new-inventory',
    name: 'מלאי דירות חדשות',
    category: 'sales',
    family: 'count',
    defaultType: 'area',
    isStock: true,
    precision: 0,
    thousands: true,
    // CBS publishes inventory national-only; the picker should disable
    // the district selector when this entry is highlighted.
    districts: 'national-only',
    // Despite living in the 'sales' picker category for discoverability,
    // inventory is a construction stock measure (~70K range) — share
    // its axis with permits/starts/completions/active rather than with
    // the per-period sales flows (~3K range).
    group: 'construction',
    fetch: () => fetchCbsSeries('new_inventory', 'national'),
  },

  // Stacked group: subsidized + free-market new sales render as one
  // stacked bar so the user can see the total volume + the breakdown
  // by buyer segment in a single visual. Picking this entry in the
  // picker adds BOTH leaf series at once with a shared stackId.
  {
    kind: 'group',
    id: 'new-sales-stacked',
    name: 'מכירות חדשות (מצטבר: מסובסדות + שוק חופשי)',
    category: 'sales',
    districts: 'all',
    members: [
      // Bottom of the stack: subsidized sales (typically the smaller
      // half — placed first so it renders at the column base).
      { registryId: 'new-sales-subsidized', stackId: 'new-sales-stack' },
      // Top of the stack: free-market sales.
      { registryId: 'new-sales-free',       stackId: 'new-sales-stack' },
    ],
  },

  // 4. מחירים כלליים
  {
    id: 'cbs-price-housing-national',
    name: 'מדד מחירי דיור (לאומי)',
    category: 'prices-general',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(40010),
  },
  {
    id: 'cbs-price-rent',
    name: 'מדד שכר דירה',
    category: 'prices-general',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(120460),
  },
  {
    id: 'cbs-price-cpi',
    name: 'מדד המחירים לצרכן (CPI)',
    category: 'prices-general',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(120010),
  },
  // Real (inflation-adjusted) housing & rent indices, derived in
  // fetch_cbs_price_indices.py by dividing each nominal month by
  // the CPI of that month and rebasing to the latest CPI month
  // ("today's purchasing power"). See docs/methodology.md →
  // "מדדים ריאליים". Stored under string series_ids — the
  // cbs_price_indices.series_id column is text and tolerates both.
  {
    id: 'cbs-price-housing-real',
    name: 'מדד מחירי דירות ריאלי',
    category: 'prices-general',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex('40010_real'),
  },
  {
    id: 'cbs-price-rent-real',
    name: 'מדד מחירי שכירות ריאלי',
    category: 'prices-general',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex('120460_real'),
  },

  // 5. מחירים לפי מחוז — one entry per district. Each is its own
  // series_id (60000–60500 in cbs_price_indices), available from
  // 2017-10. The catalog flags the gap vs the national index (1994);
  // the chart will show that as missing pre-2017 data.
  {
    id: 'cbs-price-housing-jerusalem',
    name: 'מחוז ירושלים',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60000),
  },
  {
    id: 'cbs-price-housing-north',
    name: 'מחוז הצפון',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60100),
  },
  {
    id: 'cbs-price-housing-haifa',
    name: 'מחוז חיפה',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60200),
  },
  {
    id: 'cbs-price-housing-center',
    name: 'מחוז המרכז',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60300),
  },
  {
    id: 'cbs-price-housing-tel-aviv',
    name: 'מחוז תל אביב',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60400),
  },
  {
    id: 'cbs-price-housing-south',
    name: 'מחוז הדרום',
    category: 'prices-by-district',
    family: 'idx',
    defaultType: 'line',
    precision: 1,
    fetch: () => fetchPriceIndex(60500),
  },
]

/** Spec the user picks: a registry entry plus (when applicable) which
 * district, plus an optional stackId that ties this spec to others
 * for stacked-bar rendering. The (registryId, district) pair is the
 * unique identity of an added series; stackId is presentation state
 * (a series can be re-added with a different stackId after removal). */
export interface SeriesSpec {
  registryId: string
  district: District
  /** When set on two or more specs in the same chart card, those
   * series render as a single stacked bar (each member contributes
   * one segment to the column). Bottom-up order = order in the
   * card's spec list. */
  stackId?: string
}

/** Display label for a chart series. National / undefined district
 * uses the bare registry name; non-national district appends the
 * district label so two chips for the same series in different
 * districts are distinguishable in the legend, the tooltip, and the
 * CSV header.
 *
 * Format: `${label} - ${district.name}`. DISTRICTS already encodes
 * the "מחוז " prefix in the name field (e.g. "מחוז ירושלים"), so the
 * concatenation reads naturally — "מכירות חדשות בשוק חופשי - מחוז ירושלים".
 *
 * The picker DOESN'T use this — it pairs the bare entry name with a
 * separate district selector, where the suffix would just be noise. */
export function displaySeriesName(
  entryName: string,
  district: District,
): string {
  if (district === 'national') return entryName
  const d = DISTRICTS.find((x) => x.id === district)
  if (!d) return entryName
  return `${entryName} - ${d.name}`
}

/** String key for spec — used as React key + dedup key in the
 * hydration cache. Excludes stackId so toggling a stacked entry
 * off and back on as standalone uses the same data fetch. */
export function specKey(spec: SeriesSpec): string {
  return `${spec.registryId}::${spec.district}`
}

/** Look up a leaf entry by id. Returns undefined for groups and for
 * unknown ids — callers fetching data should always be looking at
 * leaves (groups don't have data of their own). */
export function getRegistryEntry(id: string): RegistryLeafEntry | undefined {
  return getLeafEntry(id)
}

/** A chart preset: a complete view (series list + frequency + range
 * + optional display mode) the user can apply with one click in the
 * picker. Replaces the chart's existing series rather than adding to
 * them. The rangePreset literal is structurally compatible with
 * ChartCard's local Preset type so it threads into presetToRange
 * without conversion.
 *
 * Display mode left undefined → smart-default rules apply (the chart
 * picks 'indexed' when 2+ idx series, otherwise 'values'). Set
 * explicitly to override. */
export interface ChartPreset {
  id: string
  name: string
  series: SeriesSpec[]
  frequency: Frequency
  rangePreset: 'max' | '10y' | '5y' | '3y' | '1y'
  displayMode?: DisplayMode
}

/** The 6 curated views surfaced under the תצוגות מומלצות category in
 * the picker. Order = display order in the picker. Each preset's
 * series array is what the chart will be replaced with; member
 * stackIds opt those series into stacked-bar rendering exactly as
 * the existing 'group' registry entries do.
 *
 * Adding a preset: pick a stable id (used for analytics), give it
 * a Hebrew name, list the registry-id + district pairs, and set
 * frequency / rangePreset to whatever cadence + window the view
 * reads best at. */
export const PRESETS: ChartPreset[] = [
  {
    id: 'rates',
    name: 'ריביות',
    frequency: 'monthly',
    rangePreset: '3y',
    series: [
      { registryId: 'boi-base-rate',              district: 'national' },
      { registryId: 'mortgage-fixed-indexed',     district: 'national' },
      { registryId: 'mortgage-fixed-unindexed',   district: 'national' },
    ],
  },
  {
    id: 'sales',
    name: 'מכירות',
    frequency: 'quarterly',
    rangePreset: '5y',
    series: [
      // Stacked: subsidized + free render as one column.
      { registryId: 'new-sales-subsidized', district: 'national', stackId: 'preset-sales-stack' },
      { registryId: 'new-sales-free',       district: 'national', stackId: 'preset-sales-stack' },
      { registryId: 'second-hand-sales',    district: 'national' },
      { registryId: 'new-inventory',        district: 'national' },
    ],
  },
  {
    id: 'construction',
    name: 'בנייה',
    frequency: 'semiannual',
    rangePreset: '5y',
    series: [
      { registryId: 'permits',     district: 'national' },
      { registryId: 'starts',      district: 'national' },
      { registryId: 'completions', district: 'national' },
    ],
  },
  {
    id: 'sales-and-construction',
    name: 'מכירות ובנייה',
    frequency: 'semiannual',
    rangePreset: '5y',
    series: [
      { registryId: 'completions',      district: 'national' },
      { registryId: 'new-sales-total',  district: 'national' },
      { registryId: 'new-inventory',    district: 'national' },
    ],
  },
  {
    id: 'prices',
    name: 'מחירים',
    frequency: 'quarterly',
    rangePreset: '5y',
    series: [
      { registryId: 'cbs-price-housing-real', district: 'national' },
      { registryId: 'cbs-price-rent-real',    district: 'national' },
      // Sales-total alongside the price indices: provides a volume
      // counterpoint to the price trend in the same view.
      { registryId: 'new-sales-total',        district: 'national' },
    ],
  },
  {
    id: 'prices-by-district',
    name: 'מחירים לפי מחוז',
    frequency: 'quarterly',
    rangePreset: '5y',
    // 6 series — this preset is the reason SERIES_CAP went from 5 to 6.
    series: [
      { registryId: 'cbs-price-housing-jerusalem', district: 'national' },
      { registryId: 'cbs-price-housing-haifa',     district: 'national' },
      { registryId: 'cbs-price-housing-center',    district: 'national' },
      { registryId: 'cbs-price-housing-north',     district: 'national' },
      { registryId: 'cbs-price-housing-south',     district: 'national' },
      { registryId: 'cbs-price-housing-tel-aviv',  district: 'national' },
    ],
  },
]
