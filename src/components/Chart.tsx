/**
 * Chart — Recharts line chart for one or more time-series, with
 * automatic Y-axis splitting when series come from different unit
 * families.
 *
 * Substep 3: multi-axis. Each ChartSeries declares a `family`:
 *   pct   — percentages (rates, YoY changes). Single shared axis.
 *   idx   — index series (HPI, CPI, rent). Single shared axis.
 *   count — raw counts/volumes (sales, permits, starts, …).
 *           In the future this family auto-splits when the median
 *           ratio between any two count series exceeds 5×; for now
 *           the count family is a single axis (we don't have multi-
 *           series-in-count cases yet — flagged with a comment so the
 *           split logic lands when needed).
 *
 * Axis layout (up to 3 axes, per design spec — TradingView-style
 * stacked-on-the-left for secondary + tertiary):
 *   primary    — right (RTL inline-start). Most series win, ties
 *                broken by which family's first series was added
 *                earliest.
 *   secondary  — left, attached to the plot's left edge.
 *   tertiary   — left, offset ~AXIS_WIDTH further outward. Recharts
 *                stacks multiple orientation="left" YAxis elements in
 *                declaration order, so tertiary lands beyond
 *                secondary without overlapping it.
 *
 * Each axis's tick labels are colored to match the bound series's
 * stroke when only one series is on that axis (subtle visual pairing);
 * neutral text-muted when multiple series share an axis.
 *
 * Beyond 3 families, additional series stack onto the tertiary slot —
 * acceptable edge case, never expected with our 8 series's families.
 */

import {
  Area,
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { TooltipContentProps } from 'recharts'

import { Fragment } from 'react'

import {
  RECHARTS_MARGIN,
  axisWidthFor,
  effectiveModeFor,
  formatXAxisTick,
  isDynamicDomainMode,
  isPercentMode,
  planAxes,
  type Aggregation,
  type DisplayMode,
  type Frequency,
  type SeriesFamily,
} from './chartLayout'

export type { Aggregation, DisplayMode, Frequency, SeriesFamily }

export type SeriesType = 'line' | 'bar' | 'area'

export interface ChartSeriesDataPoint {
  date: Date
  value: number
  isProvisional?: boolean
  /** Set by transformForMode (indexed/percent modes): the pre-transform
   * native-units value, surfaced in the tooltip alongside the
   * transformed value. Undefined in values/log modes. */
  originalValue?: number
  /** Set only in percent-period mode: the previous period's native
   * value, used in the tooltip "(=current, was previous)" rendering.
   * Undefined for the first point in the visible range and for all
   * other modes. */
  previousValue?: number
}

export interface ChartSeries {
  id: string
  name: string
  color: string
  data: ChartSeriesDataPoint[]
  /** Drives axis assignment; required so the layer above is explicit
   * about scale grouping. */
  family: SeriesFamily
  /** Visual treatment. Optional — defaults derived from family + isStock:
   *   pct, idx          → 'line'
   *   count + !isStock  → 'bar'   (flows: sales, starts, permits, completions)
   *   count + isStock   → 'area'  (stocks: inventory, active construction) */
  type?: SeriesType
  /** True for stock measures (inventory, active construction); false for
   * flows (sales, starts, completions). Influences default `type` and
   * default `aggregation`. Ignored for non-count families. */
  isStock?: boolean
  /** Optional override for the period-aggregation method. Defaults
   * derived from family + isStock via defaultAggregation:
   *   pct, idx          → 'last'
   *   count + !isStock  → 'sum'   (flows accumulate)
   *   count + isStock   → 'last'  (stocks: end-of-period level) */
  aggregation?: Aggregation
  /** Tooltip suffix (e.g., '%'). Empty by default. */
  unit?: string
  /** Tooltip decimals. Default 1. */
  precision?: number
  /** Insert thousands separators in tooltip values (axis ticks have
   * their own per-family formatting; see formatTickKM). */
  thousands?: boolean
  /** When true, render line/area as stepAfter (constant between
   * points, jump at each observation) instead of monotone curve.
   * Used for true step-function series like the BoI base rate. */
  step?: boolean
  /** When set on two or more bar series in the same chart, those
   * series render as a single stacked column (each contributes one
   * segment). Recharts handles the stacking math when the same
   * `stackId` prop is forwarded to multiple <Bar> children. Other
   * series types (line, area) ignore this. */
  stackId?: string
  /** Within-family axis subgroup. Forwarded to chartLayout's
   * AxisLayoutSeries so planAxesByFamily can split a single family
   * (e.g. count) onto multiple axes when sales and construction
   * series are in the same chart. See RegistryLeafEntry.group. */
  group?: string
}

/** Default visual type for a series, derived from family + stock flag.
 * The App layer can override by setting `type` explicitly on the series. */
function defaultTypeFor(family: SeriesFamily, isStock?: boolean): SeriesType {
  if (family !== 'count') return 'line'
  return isStock ? 'area' : 'bar'
}

export interface ChartProps {
  series: ChartSeries[]
  /** Reserved for future range-aware tick generation. */
  range: { start: Date; end: Date }
  /** Drives the X-axis tick label format. Series data should already
   * be aggregated upstream (in ChartCard) so this prop only formats
   * labels — the chart itself doesn't re-aggregate. */
  frequency: Frequency
  /** Drives Y-axis scale ('log' switches axes to log; everything else
   * uses 'auto'). Indexed/pct-change transformations are substep 6. */
  displayMode: DisplayMode
  height: number
}

type RechartsRow = { t: number } & Record<string, number>

/** Tick-label color: matches the bound series's stroke when only one
 * series is on the axis (subtle visual pairing); neutral text-muted
 * when multiple series share an axis. */
function tickColorFor(series: ChartSeries[]): string {
  return series.length === 1 ? series[0].color : 'var(--color-text-muted)'
}

interface AxisLayout {
  domain: [number | 'auto', number | 'auto']
  scale: 'auto' | 'log'
}

/** Compute the Y-axis domain + scale for one axis based on:
 *   1. The data's min/max across all series bound to this axis (and
 *      already filtered to the visible range upstream).
 *   2. Whether ANY series on the axis is bar/area — those types
 *      anchor their bottom at zero so the magnitude reads honestly.
 *      Pure-line axes use a tight dynamic domain with ~7.5% padding.
 *   3. displayMode === 'log' switches to log scale, but only if the
 *      data is strictly positive on this axis; otherwise we silently
 *      fall back to linear (per spec — log is undefined for ≤ 0). */
function computeAxisLayout(
  axisSeries: ChartSeries[],
  displayMode: DisplayMode,
): AxisLayout {
  let min = Infinity
  let max = -Infinity
  for (const s of axisSeries) {
    for (const p of s.data) {
      if (p.value < min) min = p.value
      if (p.value > max) max = p.value
    }
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return { domain: ['auto', 'auto'], scale: 'auto' }
  }

  // Log mode applies only when all values are strictly positive on
  // this axis. Multiplicative padding (×0.9 / ×1.1) widens the
  // visible window without distorting log spacing.
  if (displayMode === 'log' && min > 0) {
    return { domain: [min / 1.1, max * 1.1], scale: 'log' }
  }
  if (displayMode === 'log' && min <= 0) {
    console.warn(
      'Log mode requested but axis has non-positive values; falling back to linear',
    )
  }

  // Percent modes: symmetric around zero so the 0% gridline lands at
  // the same vertical position on every axis, letting the eye compare
  // sign and magnitude across series on different axes. Without this,
  // each axis picks its own independent [min, max] and the zero lines
  // drift apart vertically — an HPI line crossing zero would sit at a
  // different height than a sales line crossing zero.
  if (isPercentMode(displayMode)) {
    const maxAbs = Math.max(Math.abs(min), Math.abs(max))
    const padded = maxAbs * 1.05 || 1
    return { domain: [-padded, padded], scale: 'auto' }
  }

  // Indexed mode: dynamic around the 100 baseline (not zero — values
  // cluster near 100 and a symmetric-around-zero domain would crush
  // the visible variation). Bars/areas are forced dynamic here too,
  // since zero magnitude isn't the meaningful reference.
  if (isDynamicDomainMode(displayMode)) {
    if (min === max) {
      const pad = Math.abs(min) * 0.1 || 1
      return { domain: [min - pad, max + pad], scale: 'auto' }
    }
    const pad = (max - min) * 0.075
    return { domain: [min - pad, max + pad], scale: 'auto' }
  }

  const isZeroAnchored = axisSeries.some((s) => {
    const t = s.type ?? defaultTypeFor(s.family, s.isStock)
    return t === 'bar' || t === 'area'
  })

  // Values mode: bars/areas zero-anchored, lines dynamic.
  if (isZeroAnchored) {
    return { domain: [0, max * 1.05], scale: 'auto' }
  }
  if (min === max) {
    const pad = Math.abs(min) * 0.1 || 1
    return { domain: [min - pad, max + pad], scale: 'auto' }
  }
  const pad = (max - min) * 0.075
  return { domain: [min - pad, max + pad], scale: 'auto' }
}

/** Tick formatter for count-family axis labels: K (thousands) or M
 * (millions) suffix, with no decimal when the scaled value is whole
 * and 1 decimal otherwise. Examples:
 *   847    → "847"
 *   5,000  → "5K"
 *   5,500  → "5.5K"
 *   13,500 → "13.5K"
 *   17,000 → "17K"
 *   1,200,000 → "1.2M"
 *
 * Tooltip values use the full precision (e.g., "7,831"); this short
 * format is for axis ticks only, where horizontal space matters.
 */
function formatTickKM(v: number): string {
  const abs = Math.abs(v)
  if (abs < 1000) return v.toFixed(0)
  const sign = v < 0 ? '-' : ''
  const scaled = abs < 1_000_000 ? abs / 1000 : abs / 1_000_000
  const unit = abs < 1_000_000 ? 'K' : 'M'
  // Round to 1 decimal place, then drop the trailing ".0" when whole.
  const rounded = Math.round(scaled * 10) / 10
  const text = Number.isInteger(rounded) ? rounded.toFixed(0) : rounded.toFixed(1)
  return `${sign}${text}${unit}`
}

function tickFormatterForFamily(
  family: SeriesFamily,
  thousands: boolean,
): (v: number) => string {
  if (family === 'pct') return (v) => `${v.toFixed(1)}%`
  if (family === 'count') return formatTickKM
  if (thousands) {
    // Idx-family series that explicitly opt into thousands separators
    // keep the long form (no series currently do this — kept for
    // flexibility).
    return (v) =>
      Math.abs(v) >= 1000
        ? new Intl.NumberFormat('en-US').format(Math.round(v))
        : v.toFixed(0)
  }
  return (v) => v.toFixed(0)
}

const MINUS = '−' // U+2212 — used in tick + tooltip negatives

/** Tick formatter selection by display mode. In 'indexed' and the
 * two percent modes the family-based formatter doesn't apply (axis
 * holds mixed-family series with rebased values); the formatter is
 * chosen by mode instead. Both percent variants share the same tick
 * format — only the underlying values differ. */
function tickFormatterForMode(
  family: SeriesFamily,
  thousands: boolean,
  mode: DisplayMode,
): (v: number) => string {
  if (mode === 'indexed') return (v) => v.toFixed(0)
  if (isPercentMode(mode))
    return (v) => {
      if (v === 0) return '0%'
      const sign = v > 0 ? '+' : MINUS
      return `${sign}${Math.abs(v).toFixed(0)}%`
    }
  return tickFormatterForFamily(family, thousands)
}

/** Tooltip value formatter: shows the transformed value alongside
 * the original native-units value (and, for period mode, the prior
 * period's native value too).
 *
 *   values             → "599.6"
 *   indexed            → "120.5 (=599.6, +20.5%)"
 *   percent-cumulative → "+20.5% (=599.6)"
 *   percent-period     → "+0.5% (=599.6, was 596.6)"
 *                        (first point in range: "— (=599.6)")
 *   log                → same as values (log only changes axis scale) */
function formatTooltipForMode(
  point: ChartSeriesDataPoint,
  s: ChartSeries,
  mode: DisplayMode,
): string {
  if (mode === 'indexed' && point.originalValue != null) {
    const indexed = point.value.toFixed(1)
    const original = formatSeriesValue(point.originalValue, s)
    const pctChange = point.value - 100
    const sign = pctChange >= 0 ? '+' : MINUS
    const pct = Math.abs(pctChange).toFixed(1)
    return `${indexed} (=${original}, ${sign}${pct}%)`
  }
  if (mode === 'percent-cumulative' && point.originalValue != null) {
    const sign = point.value >= 0 ? '+' : MINUS
    const pct = Math.abs(point.value).toFixed(1)
    const original = formatSeriesValue(point.originalValue, s)
    return `${sign}${pct}% (=${original})`
  }
  if (mode === 'percent-period' && point.originalValue != null) {
    const current = formatSeriesValue(point.originalValue, s)
    if (point.previousValue == null) {
      // First visible point — no prior period to compare against.
      return `— (=${current})`
    }
    const sign = point.value >= 0 ? '+' : MINUS
    const pct = Math.abs(point.value).toFixed(1)
    const previous = formatSeriesValue(point.previousValue, s)
    return `${sign}${pct}% (=${current}, was ${previous})`
  }
  return formatSeriesValue(point.value, s)
}

/** Provisional-tail constants. At monthly frequency, the last
 * TAIL_LENGTH points of each series are treated as provisional:
 *   line / area  — split into main + tail entries sharing one
 *                  overlap point; the tail uses strokeDasharray so
 *                  the dashed segment continues from the solid line.
 *                  TAIL_SUFFIX marks the tail entry's dataKey.
 *   bar          — render uniformly with all other bars (no visual
 *                  marker). Two earlier visual variants — a stacked
 *                  second <Bar> and a per-bar `shape` callback —
 *                  both fought Recharts' bar-layout math (ghost
 *                  stacking; abnormally narrow bars). The dashed-
 *                  tail metaphor doesn't translate cleanly to filled
 *                  rectangles anyway.
 * Tooltip dates within any series's tail (including bar series) get a
 * "(זמני)" annotation, which is the sole indicator for bars. */
const TAIL_LENGTH = 3
const TAIL_SUFFIX = '__tail'

interface RenderSeries extends ChartSeries {
  /** True for the tail variant; false (or undefined) for the main
   * variant or for series that don't split. */
  isTail?: boolean
  /** Always the base series's id, even on tail variants. Used to
   * resolve axis assignment (axes are planned per-base-series, not
   * per-render-entry). */
  baseId: string
}

/** Split each series into main + tail render entries when applicable.
 * Tail rules:
 *   - Only at monthly frequency (per spec — at quarterly/etc., the
 *     "last 3" provisional concept doesn't translate).
 *   - Series needs at least TAIL_LENGTH + 1 points to split (so the
 *     main has at least one non-shared point).
 *   - Bar series are NEVER split — bars render uniformly. Their
 *     provisional status is conveyed by the tooltip annotation only;
 *     see the barProvisional fold into provisionalTimestamps in
 *     Chart() below.
 *   - For line/area, the split point appears in BOTH variants so the
 *     dashed tail visually continues from the solid main line. */
function expandSeries(
  series: ChartSeries[],
  frequency: Frequency,
): RenderSeries[] {
  const out: RenderSeries[] = []
  for (const s of series) {
    const seriesType = s.type ?? defaultTypeFor(s.family, s.isStock)
    const shouldSplit =
      seriesType !== 'bar' &&
      frequency === 'monthly' &&
      s.data.length > TAIL_LENGTH
    if (!shouldSplit) {
      out.push({ ...s, baseId: s.id })
      continue
    }
    const splitIdx = s.data.length - TAIL_LENGTH
    const main = s.data.slice(0, splitIdx + 1)
    const tail = s.data.slice(splitIdx)
    out.push({ ...s, data: main, baseId: s.id })
    out.push({
      ...s,
      id: s.id + TAIL_SUFFIX,
      data: tail,
      isTail: true,
      baseId: s.id,
    })
  }
  return out
}

function buildRows(entries: RenderSeries[]): RechartsRow[] {
  if (entries.length === 0) return []
  const allTs = new Set<number>()
  const seriesMaps = entries.map((s) => {
    const m = new Map<number, number>()
    for (const p of s.data) {
      const t = p.date.getTime()
      m.set(t, p.value)
      allTs.add(t)
    }
    return m
  })
  const sorted = [...allTs].sort((a, b) => a - b)
  const rows: RechartsRow[] = []
  for (const t of sorted) {
    const row: RechartsRow = { t }
    for (let i = 0; i < entries.length; i++) {
      const v = seriesMaps[i].get(t)
      if (v !== undefined) row[entries[i].id] = v
    }
    rows.push(row)
  }
  return rows
}

function formatSeriesValue(v: number, s: ChartSeries): string {
  const precision = s.precision ?? 1
  const formatted = s.thousands
    ? new Intl.NumberFormat('en-US', {
        minimumFractionDigits: precision,
        maximumFractionDigits: precision,
      }).format(v)
    : v.toFixed(precision)
  return s.unit ? `${formatted}${s.unit}` : formatted
}

interface MultiTooltipProps extends TooltipContentProps {
  seriesById: Record<string, ChartSeries>
  frequency: Frequency
  displayMode: DisplayMode
  /** Set of timestamps that fall inside ANY series's provisional
   * tail. Used to append "(זמני)" to the tooltip date — a per-row
   * marker, not per-series, since users hover on a single date. */
  provisionalTimestamps: Set<number>
}

function MultiTooltip({
  active,
  payload,
  seriesById,
  frequency,
  displayMode,
  provisionalTimestamps,
}: MultiTooltipProps) {
  if (!active || !payload || payload.length === 0) return null
  const row = payload[0]?.payload as RechartsRow | undefined
  if (!row) return null
  const dateLabel = formatXAxisTick(row.t, frequency)
  const isProvisional = provisionalTimestamps.has(row.t)
  // Dedup: the row may carry both a base id and `${id}__tail` at the
  // overlap timestamp. Render once per base id.
  const seenBaseIds = new Set<string>()

  // Pre-aggregate stack totals so the tooltip can append a "סה״כ"
  // row beneath each stack's constituent series. Walk the payload
  // once, grouping numeric values by stackId. Skip null values and
  // tail-suffix duplicates (the main key already counts).
  const stackTotals = new Map<string, { total: number; sample: ChartSeries }>()
  for (const p of payload) {
    if (p.value == null) continue
    const rawKey = String(p.dataKey)
    if (rawKey.endsWith(TAIL_SUFFIX)) continue
    const s = seriesById[rawKey]
    if (!s?.stackId) continue
    const prev = stackTotals.get(s.stackId)
    const num = Number(p.value)
    if (prev) prev.total += num
    else stackTotals.set(s.stackId, { total: num, sample: s })
  }
  // Track which stacks we've already rendered the total for, so the
  // total row appears once per stack right after the last
  // constituent series in payload order.
  const stackTotalRendered = new Set<string>()
  // Find the LAST payload index per stackId — the total row will
  // render after that index.
  const lastIndexByStack = new Map<string, number>()
  for (let i = 0; i < payload.length; i++) {
    const p = payload[i]
    const rawKey = String(p.dataKey)
    if (rawKey.endsWith(TAIL_SUFFIX)) continue
    const s = seriesById[rawKey]
    if (!s?.stackId) continue
    lastIndexByStack.set(s.stackId, i)
  }

  return (
    <div className="chart-tooltip" role="tooltip">
      <div className="chart-tooltip-date">
        <bdi dir="ltr">{dateLabel}</bdi>
        {isProvisional && (
          <span className="chart-tooltip-provisional"> (זמני)</span>
        )}
      </div>
      {payload.map((p, idx) => {
        const rawKey = String(p.dataKey)
        const baseId = rawKey.endsWith(TAIL_SUFFIX)
          ? rawKey.slice(0, -TAIL_SUFFIX.length)
          : rawKey
        if (seenBaseIds.has(baseId)) return null
        const s = seriesById[baseId]
        if (!s || p.value == null) return null
        seenBaseIds.add(baseId)
        const t = row.t
        const point = s.data.find((dp) => dp.date.getTime() === t)
        const formatted = point
          ? formatTooltipForMode(point, s, displayMode)
          : formatSeriesValue(Number(p.value), s)

        // After rendering this constituent, check if we've reached
        // the LAST member of its stack — if so, render a total row
        // immediately after.
        let totalRow: React.ReactNode = null
        if (s.stackId && !stackTotalRendered.has(s.stackId)) {
          const lastIdx = lastIndexByStack.get(s.stackId)
          if (lastIdx === idx) {
            const stack = stackTotals.get(s.stackId)
            if (stack && stack.total > 0) {
              stackTotalRendered.add(s.stackId)
              totalRow = (
                <div className="chart-tooltip-row chart-tooltip-row--total">
                  <span className="chart-tooltip-dot" aria-hidden="true" />
                  <span className="chart-tooltip-name">סה״כ</span>
                  <bdi dir="ltr" className="chart-tooltip-value tabular">
                    {formatSeriesValue(stack.total, stack.sample)}
                  </bdi>
                </div>
              )
            }
          }
        }

        return (
          <Fragment key={s.id}>
            <div className="chart-tooltip-row">
              <span
                className="chart-tooltip-dot"
                style={{ background: s.color }}
                aria-hidden="true"
              />
              <span className="chart-tooltip-name">{s.name}</span>
              <bdi dir="ltr" className="chart-tooltip-value tabular">
                {formatted}
              </bdi>
            </div>
            {totalRow}
          </Fragment>
        )
      })}
    </div>
  )
}

/** Watermark text rendered as an HTML overlay positioned absolutely
 * within the chart-engine container. Two reasons for going HTML
 * instead of an SVG <text>:
 *
 *   1. Recharts v3 deprecated <Customized> ("Customized component
 *      used to be necessary to render custom elements in Recharts
 *      2.x... no longer needed"); the prop-injection behavior is
 *      no longer guaranteed, and rendering a bare <text> as a
 *      direct child of ComposedChart isn't reliable across all the
 *      internal layout passes.
 *   2. The original SVG version used fill="var(--color-text-sub)"
 *      — a CSS custom property in an SVG attribute, which most
 *      browsers reject (var() needs a CSS context, not an
 *      attribute string), making the text invisible. HTML+CSS
 *      sidesteps that entirely.
 *
 * On screenshot capture: OS-level screenshot tools (Snipping
 * Tool, Cmd+Shift+4, etc.) capture the rendered viewport
 * including positioned HTML overlays, so the watermark still
 * lands in user screenshots. The only thing this loses is
 * SVG-export-via-`saveAs(svg)` — not a current feature, can be
 * reconsidered when the CSV/SVG download menu lands. */
/** Interior padding from the plot area's visual-left edge to the
 * watermark's start. Chosen so the text sits comfortably inside
 * the data region rather than hugging the axis labels. */
const WATERMARK_INTERIOR_INSET = 24

function ChartWatermark({ leftPx }: { leftPx: number }) {
  // Inline `left` overrides the CSS default; per-render value
  // because the plot inset shifts with axis count + display mode
  // (e.g., percent modes widen the axes).
  return (
    <span
      className="chart-watermark"
      style={{ left: leftPx }}
      aria-hidden="true"
      data-testid="chart-watermark"
    >
      DiraMetrics.co.il
    </span>
  )
}

export default function Chart({ series, frequency, displayMode, height }: ChartProps) {
  const entries = expandSeries(series, frequency)
  const rows = buildRows(entries)
  if (rows.length === 0 || series.length === 0) return null

  const seriesById: Record<string, ChartSeries> = {}
  for (const s of series) seriesById[s.id] = s

  // Provisional timestamps: union of every tail entry's data dates,
  // plus the last TAIL_LENGTH timestamps of each bar series at
  // monthly frequency (bars don't produce tail entries — they render
  // uniformly — but their tooltip should still annotate "(זמני)").
  // The tooltip uses this set to mark the date for any hovered point
  // that falls in at least one series's provisional region.
  const provisionalTimestamps = new Set<number>()
  for (const e of entries) {
    if (!e.isTail) continue
    for (const p of e.data) provisionalTimestamps.add(p.date.getTime())
  }
  if (frequency === 'monthly') {
    for (const s of series) {
      const t = s.type ?? defaultTypeFor(s.family, s.isStock)
      if (t !== 'bar') continue
      if (s.data.length <= TAIL_LENGTH) continue
      for (let i = s.data.length - TAIL_LENGTH; i < s.data.length; i++) {
        provisionalTimestamps.add(s.data[i].date.getTime())
      }
    }
  }

  const { axes, axisBySeries } = planAxes(series, displayMode)

  // Visual-left edge of the plot area in CSS pixels. Recharts insets
  // the plot by RECHARTS_MARGIN.left plus the cumulative width of
  // every left-oriented axis. The watermark is positioned a further
  // WATERMARK_INTERIOR_INSET inside this so it lands within the data
  // region (where lines/bars draw), not on top of axis labels.
  let plotLeftPx = RECHARTS_MARGIN.left
  for (const a of axes) {
    if (a.orientation === 'left') {
      plotLeftPx += axisWidthFor(a.family, displayMode)
    }
  }
  const watermarkLeftPx = plotLeftPx + WATERMARK_INTERIOR_INSET

  // Render order: bars (deepest) → areas → lines (topmost). Recharts
  // renders children in JSX order, with later children layered on top.
  // Sorting here ensures bars don't obscure lines crossing them.
  // Tail entries follow their corresponding main entry's type, so
  // sorting on type still produces a sensible bar/area/line stack.
  const RENDER_ORDER: Record<SeriesType, number> = { bar: 0, area: 1, line: 2 }
  const renderEntries = [...entries].sort((a, b) => {
    const ta = a.type ?? defaultTypeFor(a.family, a.isStock)
    const tb = b.type ?? defaultTypeFor(b.family, b.isStock)
    return RENDER_ORDER[ta] - RENDER_ORDER[tb]
  })

  // Screen-reader description: a single Hebrew sentence summarizing
  // what the chart contains. Visually-impaired users who can't see
  // the SVG get the series names + date range + frequency at minimum.
  // Per WCAG 1.1.1 (Non-text Content), an SVG that conveys
  // information needs a text alternative.
  const seriesNames = series.map((s) => s.name).join(', ')
  const earliestDate = rows[0]?.t ? new Date(rows[0].t) : null
  const latestDate = rows[rows.length - 1]?.t ? new Date(rows[rows.length - 1].t) : null
  const dateRange = earliestDate && latestDate
    ? `מ${formatXAxisTick(earliestDate.getTime(), frequency)} עד ${formatXAxisTick(latestDate.getTime(), frequency)}`
    : ''
  const chartAriaLabel = `תרשים סדרות זמן: ${seriesNames}. ${dateRange}.`

  return (
    <div
      className="chart-engine"
      style={{ height }}
      role="img"
      aria-label={chartAriaLabel}
    >
      {/* width="99%" instead of 100% is the well-known workaround for
       * Recharts' "width(-1) height(-1)" warning — at 100% it
       * sometimes measures the parent before flex layout has settled
       * on a concrete width and emits the warning. The 1% gap is
       * imperceptible visually and avoids the warning + helps the
       * plot-area measurement below land on real numbers from the
       * first frame. */}
      <ResponsiveContainer width="99%" height="100%">
        <ComposedChart data={rows} margin={RECHARTS_MARGIN}>
          <CartesianGrid
            stroke="var(--color-border-hairline)"
            strokeDasharray="0"
            vertical={false}
          />
          <XAxis
            dataKey="t"
            type="number"
            scale="time"
            domain={['dataMin', 'dataMax']}
            tickFormatter={(t: number) => formatXAxisTick(t, frequency)}
            tick={{ fill: 'var(--color-text-muted)', fontSize: 11.5 }}
            stroke="var(--color-border-hairline)"
            tickLine={false}
            axisLine={true}
            minTickGap={32}
          />
          {axes.map((a) => {
            const wantsThousands = a.series.some((s) => s.thousands)
            // Per-axis effective mode: indexed mode rebases only
            // idx-family series, so a count axis sitting beside an
            // idx axis sees `values` here and uses native-units
            // domain + tick formatting + width. percent and log
            // pass through unchanged.
            const axisMode = effectiveModeFor(a.family, displayMode)
            const { domain, scale } = computeAxisLayout(a.series, axisMode)
            // The mode-aware tick formatter overrides family-based
            // formatting (showing "120" for indexed, "+10%" for
            // percent); for axes whose effective mode is values, we
            // fall back to family-based formatting via the same helper.
            return (
              <YAxis
                key={a.axisId}
                yAxisId={a.axisId}
                orientation={a.orientation}
                domain={domain}
                scale={scale}
                allowDataOverflow={false}
                tick={{ fill: tickColorFor(a.series), fontSize: 11.5 }}
                tickLine={false}
                axisLine={false}
                width={axisWidthFor(a.family, axisMode)}
                tickFormatter={tickFormatterForMode(
                  a.family,
                  wantsThousands,
                  axisMode,
                )}
              />
            )
          })}
          <Tooltip
            cursor={{ stroke: 'var(--color-border-input)', strokeWidth: 1 }}
            content={(props: TooltipContentProps) => (
              <MultiTooltip
                {...props}
                seriesById={seriesById}
                frequency={frequency}
                displayMode={displayMode}
                provisionalTimestamps={provisionalTimestamps}
              />
            )}
            isAnimationActive={false}
          />
          {renderEntries.map((s) => {
            const type = s.type ?? defaultTypeFor(s.family, s.isStock)
            const yAxisId = axisBySeries[s.baseId]
            const isTail = !!s.isTail
            if (type === 'bar') {
              // All bars render uniformly. Provisional bars (last
              // TAIL_LENGTH at monthly frequency) are NOT visually
              // differentiated — both prior attempts (a stacked
              // second Bar; a per-bar `shape` callback) collided
              // with Recharts' bar-layout math. Provisional status
              // is conveyed via the tooltip "(זמני)" annotation.
              return (
                <Bar
                  key={s.id}
                  dataKey={s.id}
                  name={s.name}
                  yAxisId={yAxisId}
                  fill={s.color}
                  fillOpacity={0.55}
                  activeBar={{ fillOpacity: 0.75 }}
                  // stackId: Recharts groups Bar elements with the
                  // same stackId into a single stacked column.
                  // Undefined means the bar stands alone.
                  stackId={s.stackId}
                  isAnimationActive={false}
                />
              )
            }
            const interpolation = s.step ? 'stepAfter' : 'monotone'
            // connectNulls=true is correct here because each series is
            // continuous on its NATIVE cadence; nulls in the merged
            // `rows` array are just date-misalignment artifacts (e.g.
            // BoI base rate's irregular decision dates inject rows
            // where every other series has no observation). Without
            // this, adding a series whose dates don't coincide with
            // the others' month-firsts shatters their lines into
            // disconnected single-point segments.
            // Tail dasharray is ~stroke-width × 3 on, × 2 off — visible
            // as discrete segments at 1.5–2 px stroke without becoming
            // dotted noise.
            const tailDash = isTail ? '4 3' : undefined
            if (type === 'area') {
              return (
                <Area
                  key={s.id}
                  type={interpolation}
                  dataKey={s.id}
                  name={s.name}
                  yAxisId={yAxisId}
                  stroke={s.color}
                  strokeWidth={2}
                  strokeDasharray={tailDash}
                  fill={s.color}
                  fillOpacity={isTail ? 0.06 : 0.12}
                  dot={false}
                  connectNulls
                  isAnimationActive={false}
                />
              )
            }
            return (
              <Line
                key={s.id}
                type={interpolation}
                dataKey={s.id}
                name={s.name}
                yAxisId={yAxisId}
                stroke={s.color}
                strokeWidth={1.5}
                strokeDasharray={tailDash}
                dot={false}
                connectNulls
                isAnimationActive={false}
              />
            )
          })}
        </ComposedChart>
      </ResponsiveContainer>
      {/* Watermark sits as an absolutely-positioned HTML overlay
       * inside .chart-engine. Its `left` is computed per-render to
       * land inside the plot data region (past the left axes), so
       * a screenshot cropped to just the plot still captures it. */}
      <ChartWatermark leftPx={watermarkLeftPx} />
    </div>
  )
}
