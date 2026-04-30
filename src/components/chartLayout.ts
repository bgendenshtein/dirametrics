/**
 * chartLayout — single source of truth for the per-family axis layout
 * that Chart.tsx renders AND that ChartCard.tsx uses to align the
 * brush with the plot's horizontal extent.
 *
 * Living outside Chart.tsx keeps the brush-alignment helper out of
 * the lazy Recharts chunk: ChartCard imports `getPlotOffsets` eagerly
 * (so the brush position is right on first render) without forcing
 * Recharts itself into the main bundle. `formatXAxisTick` ships from
 * here for the same reason — ChartCard's CSV export needs it before
 * the lazy chart bundle has loaded.
 */

import { formatHebrewMonthShortYear } from '../lib/dateRange'

export type SeriesFamily = 'idx' | 'pct' | 'count'

/** Display mode is a chart-level transformation:
 *   values              — native units, multi-axis (default)
 *   indexed             — every series rebased so its first visible
 *                         value = 100; single shared Y-axis
 *   percent-cumulative  — every point as % change vs first visible
 *                         value; single shared Y-axis (zero-centered)
 *   percent-period      — every point as % change vs the immediately
 *                         previous period (at the current frequency);
 *                         single shared Y-axis. Noisier than cumulative
 *                         since each point is independent of others
 *   log                 — native units (no rebase), Y-axes use log
 *                         scale. Multi-axis preserved; axes with non-
 *                         positive values fall back to linear (with
 *                         a console warning). */
export type DisplayMode =
  | 'values'
  | 'indexed'
  | 'percent-cumulative'
  | 'percent-period'
  | 'log'

/** Both percent modes share tick format + axis width (e.g. "+10%",
 * 44 px). They DIFFER on axis grouping — see isSingleAxisMode. */
export function isPercentMode(mode: DisplayMode | undefined): boolean {
  return mode === 'percent-cumulative' || mode === 'percent-period'
}

/** Modes that collapse all series onto ONE shared axis:
 *   indexed             — values rebased to 100, naturally converge
 *   percent-cumulative  — values rebased to 0%, naturally converge
 * NOT percent-period: each point is independent of others, so
 * series can have wildly different magnitude ranges (rates → ±2%,
 * sales → ±30%). Period mode uses range-based multi-axis instead
 * (see planAxes). */
export function isSingleAxisMode(mode: DisplayMode | undefined): boolean {
  return mode === 'indexed' || mode === 'percent-cumulative'
}

/** Modes with a dynamic Y-axis domain (no zero anchor). The axis
 * baseline is the data's natural reference (100 for indexed, 0% for
 * percents) — anchoring to zero would either waste space or clip
 * negatives. Bars/areas in these modes follow the same dynamic
 * treatment instead of their default zero-anchor. */
export function isDynamicDomainMode(mode: DisplayMode | undefined): boolean {
  return mode === 'indexed' || isPercentMode(mode)
}

/** Time-bucket sizes for the תדירות chip group. The hooks fetch raw
 * monthly data; non-monthly frequencies are computed client-side via
 * aggregateData (no extra Supabase queries). */
export type Frequency = 'monthly' | 'quarterly' | 'semiannual' | 'yearly'

/** Frequency-aware X-axis tick formatter:
 *   monthly    → "אפר׳ 26"   (Hebrew month abbrev + 2-digit year)
 *   quarterly  → "Q1 23"      (LTR — quarter number + 2-digit year)
 *   semiannual → "H1 23"      (LTR — half-year number + 2-digit year)
 *   yearly     → "2023"       (4-digit year only)
 *
 * Quarterly/semi-annual labels are LTR text starting with Q/H. They
 * render correctly inside the Recharts SVG (which treats axis tick
 * text as LTR by default) and against the RTL page direction. */
export function formatXAxisTick(t: number, frequency: Frequency): string {
  const d = new Date(t)
  const m = d.getUTCMonth()
  const y2 = String(d.getUTCFullYear()).slice(-2)
  if (frequency === 'yearly') return String(d.getUTCFullYear())
  if (frequency === 'semiannual') return `H${Math.floor(m / 6) + 1} ${y2}`
  if (frequency === 'quarterly') return `Q${Math.floor(m / 3) + 1} ${y2}`
  return formatHebrewMonthShortYear(d)
}

/** How to combine multiple monthly readings into one period:
 *   sum   — flows accumulate (sales/permits/starts/completions)
 *   last  — stocks and rates take the period's last reading (HPI,
 *           inventory, active construction, mortgage rate, BoI rate) */
export type Aggregation = 'last' | 'sum'

export function defaultAggregation(
  family: SeriesFamily,
  isStock?: boolean,
): Aggregation {
  if (family !== 'count') return 'last'
  return isStock ? 'last' : 'sum'
}

/** Period-start timestamp (UTC ms) for a date under a given frequency.
 * Used to bucket monthly observations into quarterly/semi-annual/annual
 * groups, and to anchor each aggregated point to its period start. */
function periodStartTimestamp(date: Date, frequency: Frequency): number {
  const y = date.getUTCFullYear()
  const m = date.getUTCMonth()
  if (frequency === 'yearly')     return Date.UTC(y, 0, 1)
  if (frequency === 'semiannual') return Date.UTC(y, Math.floor(m / 6) * 6, 1)
  if (frequency === 'quarterly')  return Date.UTC(y, Math.floor(m / 3) * 3, 1)
  return Date.UTC(y, m, 1)
}

/** Data point shape compatible with both the Chart and the brush. */
export interface AggregatablePoint {
  date: Date
  value: number
  isProvisional?: boolean
}

/** Transformable point: aggregateData's output that may also be
 * transformed by transformForMode. Adds optional fields surfaced in
 * the chart tooltip:
 *   originalValue  — pre-transform native value (indexed, both
 *                    percent modes set this)
 *   previousValue  — only set in percent-period mode; the prior
 *                    period's native value, used in the tooltip
 *                    "(=current, was previous)" rendering. */
export interface TransformablePoint extends AggregatablePoint {
  originalValue?: number
  previousValue?: number
}

/** Apply the indexed / percent transformation in-place across a
 * series's data. Each point's `value` is rebased; native units are
 * preserved on `originalValue`, and percent-period also stashes
 * `previousValue` for the tooltip's "was X" reference.
 *
 *   indexed             → value = (raw / first) × 100        (first = 100)
 *   percent-cumulative  → value = ((raw − first) / first) × 100  (first = 0%)
 *   percent-period      → value = ((raw − prior) / prior) × 100  (first = N/A)
 *
 * Pass-through for 'values' and 'log' modes (those don't transform
 * data, only axis behavior). Edge cases:
 *   - empty data: return unchanged
 *   - indexed/percent-cumulative with first === 0: console warn,
 *     return unchanged (division by zero undefined)
 *   - percent-period with prior === 0 at any step: that step's
 *     value is set to 0, originalValue + previousValue still set
 *     so the tooltip can disambiguate. */
export function transformForMode<P extends TransformablePoint>(
  data: P[],
  mode: DisplayMode,
): P[] {
  if (data.length === 0) return data

  if (mode === 'indexed' || mode === 'percent-cumulative') {
    const first = data[0].value
    if (first === 0) {
      console.warn(
        `Cannot apply '${mode}' mode: first visible value is zero, skipping transform`,
      )
      return data
    }
    return data.map(
      (p) =>
        ({
          ...p,
          value:
            mode === 'indexed'
              ? (p.value / first) * 100
              : ((p.value - first) / first) * 100,
          originalValue: p.value,
        }) as P,
    )
  }

  if (mode === 'percent-period') {
    return data.map((p, i) => {
      // First point in the visible range has no prior to compare to.
      // We emit value=0 + originalValue, and leave previousValue
      // undefined so the tooltip can render an em-dash placeholder
      // ("— (=N)") for that point.
      if (i === 0) {
        return {
          ...p,
          value: 0,
          originalValue: p.value,
          previousValue: undefined,
        } as P
      }
      const prior = data[i - 1].value
      if (prior === 0) {
        return {
          ...p,
          value: 0,
          originalValue: p.value,
          previousValue: prior,
        } as P
      }
      return {
        ...p,
        value: ((p.value - prior) / prior) * 100,
        originalValue: p.value,
        previousValue: prior,
      } as P
    })
  }

  // values, log: pass-through.
  return data
}

/** Aggregate monthly data points into the requested frequency. The
 * input MUST already be filtered to the visible range — boundary
 * periods are aggregated from whatever monthly observations they
 * happen to contain (so a quarter at the right edge of a 5Y window
 * may be partial; that's acceptable visual behavior).
 *
 * Each output point is anchored to its period-start date and inherits
 * the period's last observation's `isProvisional` flag — so the
 * provisional-tail logic in substep 7 will treat the most-recent
 * aggregated period as provisional whenever its terminal monthly
 * reading is. */
export function aggregateData<P extends AggregatablePoint>(
  data: P[],
  frequency: Frequency,
  method: Aggregation,
): P[] {
  if (frequency === 'monthly' || data.length === 0) return data

  const groups = new Map<number, P[]>()
  for (const p of data) {
    const key = periodStartTimestamp(p.date, frequency)
    const list = groups.get(key)
    if (list) list.push(p)
    else groups.set(key, [p])
  }

  const result: P[] = []
  for (const [ts, points] of groups) {
    const sorted = [...points].sort(
      (a, b) => a.date.getTime() - b.date.getTime(),
    )
    const last = sorted[sorted.length - 1]
    const value =
      method === 'sum'
        ? sorted.reduce((acc, p) => acc + p.value, 0)
        : last.value
    // Construct a new point preserving the input shape (so callers
    // that subclass AggregatablePoint don't lose extra fields beyond
    // what's mutated here).
    result.push({
      ...last,
      date: new Date(ts),
      value,
      isProvisional: last.isProvisional,
    } as P)
  }
  return result.sort((a, b) => a.date.getTime() - b.date.getTime())
}

/** Minimal series shape needed for axis planning. The real ChartSeries
 * (in Chart.tsx) extends this with name, color, etc.
 *
 * `data` requires `value: number` because the range-based grouping in
 * percent-period mode needs to read each point's value to compute the
 * series's range. ChartSeriesDataPoint is structurally compatible. */
export interface AxisLayoutSeries {
  id: string
  family: SeriesFamily
  data: Array<{ value: number }>
}

/** Per-family Y-axis width in CSS pixels. Tuned to fit the longest
 * tick label each family typically produces:
 *   idx    — 3–4-digit integers   (e.g., "440", "1,200") → 32 px
 *   pct    — 1 decimal + %         (e.g., "4.0%", "12.5%") → 36 px
 *   count  — K/M-suffix abbrev     (e.g., "5K", "17K", "1.2M") → 36 px
 *
 * Count tick labels use a K/M suffix rather than the full
 * comma-separated form (see formatTickKM in Chart.tsx) so the axis
 * doesn't reserve extra width for a "17,000"-style label. Tooltips
 * still show the full-precision number (e.g., "7,831") because there
 * the precision matters and horizontal space doesn't.
 */
export const AXIS_WIDTH_BY_FAMILY: Record<SeriesFamily, number> = {
  idx: 32,
  pct: 36,
  count: 36,
}

/** Recharts margins around the plot area. Tightened from prior
 * defaults to maximize horizontal plot space; top is slightly larger
 * so tooltip arrow doesn't clip when hovering at the top of the plot. */
export const RECHARTS_MARGIN = {
  top: 16,
  right: 8,
  left: 8,
  bottom: 8,
} as const

const AXIS_POSITIONS: ReadonlyArray<{
  id: string
  orientation: 'left' | 'right'
}> = [
  { id: 'axis-primary',   orientation: 'right' },
  { id: 'axis-secondary', orientation: 'left'  },
  { id: 'axis-tertiary',  orientation: 'left'  },
]

/** Median ratio above which series in the same unit family get
 * separate axes. At 3× a smaller-amplitude series visibly compresses
 * against a larger one — Bloomberg/Refinitiv use the same convention.
 * Was 5× originally; lowered after observing that count-family
 * inventory (~70K) vs starts (~13K), at 5.4×, still flattened the
 * starts bars near the axis baseline. Tune here if visual review
 * suggests otherwise. */
const WITHIN_FAMILY_SPLIT_THRESHOLD = 3.0

/** Median of an unsorted numeric array. Returns 0 for empty input
 * (the axis-planning callers treat zero-data series as no-signal and
 * group them with the smallest bucket). */
function median(values: number[]): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid]
}

/** Split a list of same-family series into sub-buckets by absolute
 * median magnitude. Sorts ascending, then opens a new bucket whenever
 * the next series's median exceeds WITHIN_FAMILY_SPLIT_THRESHOLD ×
 * the current bucket's smallest median. Series order within each
 * returned bucket follows ascending median; callers that care about
 * original input order should re-sort if needed. */
function splitByMedian<S extends AxisLayoutSeries>(series: S[]): S[][] {
  if (series.length <= 1) return [series]
  const withMedian = series.map((s) => ({
    s,
    median: median(s.data.map((p) => Math.abs(p.value)).filter(Number.isFinite)),
  }))
  withMedian.sort((a, b) => a.median - b.median)
  const buckets: { smallestMedian: number; series: S[] }[] = []
  for (const entry of withMedian) {
    const last = buckets[buckets.length - 1]
    // Math.max guards against zero-median series (would otherwise
    // force every subsequent series into its own bucket).
    if (
      !last ||
      entry.median >
        WITHIN_FAMILY_SPLIT_THRESHOLD * Math.max(last.smallestMedian, 0.001)
    ) {
      buckets.push({ smallestMedian: entry.median, series: [entry.s] })
    } else {
      last.series.push(entry.s)
    }
  }
  return buckets.map((b) => b.series)
}

export interface AxisAssignmentBase<S> {
  axisId: string
  orientation: 'left' | 'right'
  family: SeriesFamily
  series: S[]
}

/** Y-axis width for a given family + display mode. Modes that
 * collapse all series onto a single axis (indexed, both percent
 * variants) override the per-family widths because the axis is
 * no longer family-specific. */
export function axisWidthFor(family: SeriesFamily, mode?: DisplayMode): number {
  if (mode === 'indexed') return 36 // values around 100, max ~3 digits
  if (isPercentMode(mode)) return 44 // sign + digits + '%' (e.g., "+150%")
  return AXIS_WIDTH_BY_FAMILY[family]
}

/** Group series into axes based on the active display mode. Used by
 * Chart.tsx for rendering and by getPlotOffsets for brush alignment.
 *
 * Three grouping strategies:
 *
 *   isSingleAxisMode(mode) — all series collapse to one shared axis
 *     on the right. Used by indexed and percent-cumulative, where
 *     transformed values naturally converge to similar magnitudes.
 *     The `family` on the returned axis is a dummy ('idx'); Chart.tsx
 *     picks the tick formatter from mode directly in these modes.
 *
 *   mode === 'percent-period' — group by computed range. Each series's
 *     range = max(value) - min(value); series whose ranges are within
 *     5× of each other share an axis, larger ranges split off. This
 *     keeps small-amplitude series (e.g. rate QoQ ±2%) from being
 *     drowned by large-amplitude ones (e.g. sales QoQ ±30%).
 *
 *   else (values, log) — group by family (idx / pct / count). The
 *     original three-axis layout, with primary right + secondary +
 *     tertiary stacked left. */
export function planAxes<S extends AxisLayoutSeries>(
  series: S[],
  mode?: DisplayMode,
): {
  axes: AxisAssignmentBase<S>[]
  axisBySeries: Record<string, string>
} {
  if (series.length === 0) return { axes: [], axisBySeries: {} }

  if (isSingleAxisMode(mode)) {
    const axisId = 'axis-mode-shared'
    const axisBySeries: Record<string, string> = {}
    for (const s of series) axisBySeries[s.id] = axisId
    return {
      axes: [
        { axisId, orientation: 'right', family: 'idx', series },
      ],
      axisBySeries,
    }
  }

  if (mode === 'percent-period') {
    return planAxesByRange(series)
  }

  return planAxesByFamily(series)
}

/** Range-based grouping for percent-period mode. Walks series in
 * ascending range order, grouping each into the current bucket
 * unless its range exceeds 5× the bucket's smallest. Then orders
 * the resulting groups so the most-populated wins the primary
 * (right) axis, ties broken by which group contains the earliest
 * input series. */
function planAxesByRange<S extends AxisLayoutSeries>(
  series: S[],
): {
  axes: AxisAssignmentBase<S>[]
  axisBySeries: Record<string, string>
} {
  const ranges = series.map((s) => {
    if (s.data.length === 0) return 0
    let min = Infinity
    let max = -Infinity
    for (const p of s.data) {
      if (p.value < min) min = p.value
      if (p.value > max) max = p.value
    }
    return max - min
  })

  const sorted = series
    .map((s, i) => ({ s, range: ranges[i], originalIdx: i }))
    .sort((a, b) => a.range - b.range)

  // Group: walk sorted; new group whenever range > 5× the bucket's
  // smallest. Math.max guards against zero-range degenerate series
  // (would otherwise force every subsequent series into a new group).
  const buckets: { smallestRange: number; series: S[] }[] = []
  for (const entry of sorted) {
    const last = buckets[buckets.length - 1]
    if (!last || entry.range > 5 * Math.max(last.smallestRange, 0.001)) {
      buckets.push({ smallestRange: entry.range, series: [entry.s] })
    } else {
      last.series.push(entry.s)
    }
  }

  // Order buckets so largest gets primary right; ties → earliest
  // original-input position wins.
  buckets.sort((a, b) => {
    if (a.series.length !== b.series.length) return b.series.length - a.series.length
    const earliestA = Math.min(...a.series.map((s) => series.indexOf(s)))
    const earliestB = Math.min(...b.series.map((s) => series.indexOf(s)))
    return earliestA - earliestB
  })

  const axes: AxisAssignmentBase<S>[] = []
  const axisBySeries: Record<string, string> = {}
  for (let i = 0; i < buckets.length; i++) {
    const bucket = buckets[i]
    const pos = AXIS_POSITIONS[Math.min(i, AXIS_POSITIONS.length - 1)]
    const axisId = `axis-period-${i}`
    // family is a stand-in here — axis width and tick format are
    // mode-driven (44 px + percent format) regardless of family.
    axes.push({
      axisId,
      orientation: pos.orientation,
      family: bucket.series[0].family,
      series: bucket.series,
    })
    for (const s of bucket.series) axisBySeries[s.id] = axisId
  }
  return { axes, axisBySeries }
}

/** Family-based grouping for values + log modes. Each family bucket
 * is then further split by median magnitude (see splitByMedian + the
 * WITHIN_FAMILY_SPLIT_THRESHOLD constant) so e.g. count-family
 * inventory (~70K) and starts (~13K) get separate axes instead of
 * the smaller series compressing flat against the baseline. */
function planAxesByFamily<S extends AxisLayoutSeries>(
  series: S[],
): {
  axes: AxisAssignmentBase<S>[]
  axisBySeries: Record<string, string>
} {
  const byFamily = new Map<SeriesFamily, S[]>()
  for (const s of series) {
    const list = byFamily.get(s.family)
    if (list) list.push(s)
    else byFamily.set(s.family, [s])
  }

  type Group = { family: SeriesFamily; series: S[] }
  const groups: Group[] = []
  for (const [family, list] of byFamily) {
    for (const sub of splitByMedian(list)) groups.push({ family, series: sub })
  }

  // Order groups: largest series count wins primary (right) axis.
  // Ties → earliest original-input position. With no within-family
  // splits this collapses to the prior family-ordering behavior.
  groups.sort((a, b) => {
    if (a.series.length !== b.series.length) return b.series.length - a.series.length
    return series.indexOf(a.series[0]) - series.indexOf(b.series[0])
  })

  const axes: AxisAssignmentBase<S>[] = []
  const axisBySeries: Record<string, string> = {}
  for (let i = 0; i < groups.length; i++) {
    const group = groups[i]
    const pos = AXIS_POSITIONS[Math.min(i, AXIS_POSITIONS.length - 1)]
    // Suffix with the group index so axis IDs stay unique even when
    // the same family appears twice (post-split); pos.id alone is
    // unique per axis position so this composes cleanly.
    const axisId = `${pos.id}-${group.family}-${i}`
    axes.push({
      axisId,
      orientation: pos.orientation,
      family: group.family,
      series: group.series,
    })
    for (const s of group.series) axisBySeries[s.id] = axisId
  }
  return { axes, axisBySeries }
}

/** Total horizontal padding the chart's plot area takes up on each
 * side of its container — used by ChartCard to inset the brush so it
 * aligns with the plot's left and right edges. */
export function getPlotOffsets<S extends AxisLayoutSeries>(
  series: S[],
  mode?: DisplayMode,
): { left: number; right: number } {
  if (series.length === 0) return { left: 0, right: 0 }
  const { axes } = planAxes(series, mode)
  let left = RECHARTS_MARGIN.left
  let right = RECHARTS_MARGIN.right
  for (const a of axes) {
    const w = axisWidthFor(a.family, mode)
    if (a.orientation === 'right') right += w
    else left += w
  }
  return { left, right }
}
