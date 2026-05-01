/**
 * ChartCard — interactive chart slot. Two rendering modes:
 *
 *   demo mode  (no `initialSpecs` prop)
 *     Synthetic-brush + placeholder chart. Used by cards still
 *     awaiting real wiring.
 *
 *   real mode  (`initialSpecs` prop provided)
 *     Specs flow through useSeriesList → fetched + cached → assembled
 *     into ChartSeries → fed to the chart engine. The card owns the
 *     active spec list (initial seed + entries the user adds via the
 *     SeriesPicker), so the chart's contents become editable rather
 *     than being a fixed shape passed by the parent.
 *
 * Brush gets the longest series's full history. Chart receives each
 * series filtered to the current range and renders the lazy-loaded
 * Recharts engine. Loading, error, and empty states fall back to
 * ChartSkeleton or friendly Hebrew messages.
 */

import {
  Suspense,
  forwardRef,
  lazy,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from 'react'

import {
  displaySeriesName,
  getRegistryEntry,
  specKey,
  type ChartPreset,
  type SeriesSpec,
} from '../data/seriesRegistry'
import { useSeriesList } from '../hooks/useSeriesList'
import { track } from '../lib/analytics'
import {
  addMonths,
  formatHebrewDateRange,
  rangesEqual,
  type DateRange,
} from '../lib/dateRange'
import { seriesColorBySlot, useResolvedTheme } from '../styles/tokens'
import { ApplyPill } from './ApplyPill'
import { BrushOverview, type BrushDataPoint } from './BrushOverview'
import type { ChartSeries, SeriesType } from './Chart'
import {
  aggregateData,
  defaultAggregation,
  effectiveModeFor,
  formatXAxisTick,
  getPlotOffsets,
  transformForMode,
  type DisplayMode,
  type Frequency,
} from './chartLayout'
import { ChipGroup } from './ChipGroup'
import { SeriesPicker } from './SeriesPicker'

// Lazy import: keeps Recharts out of the initial bundle. Falls into
// Suspense fallback (ChartSkeleton) on first render of any real chart.
const Chart = lazy(() => import('./Chart'))

const CHART_HEIGHT = 400

/** Cycle order for the per-series type icon: line → bar → area → line.
 * Matches the user-spec sequence exactly. The icon button in each
 * legend chip clicks through this in order, persisting the override
 * in `typeOverrides`. */
const TYPE_CYCLE: SeriesType[] = ['line', 'bar', 'area']
function nextType(current: SeriesType): SeriesType {
  const i = TYPE_CYCLE.indexOf(current)
  return TYPE_CYCLE[(i + 1) % TYPE_CYCLE.length]
}

/** CSV-export support. Lives here (module-level) so the helpers don't
 * recreate on every render. The provisional-flag heuristic mirrors
 * Chart.tsx's tooltip behavior: at monthly frequency, the last
 * CSV_TAIL_LENGTH points of any series with more than that many
 * points are flagged "כן". Other frequencies don't surface a
 * provisional indicator on screen, so the column is uniformly "לא". */
const CSV_TAIL_LENGTH = 3

function csvEscape(field: string): string {
  // Quote when the field contains a separator, quote, or line break.
  // Embedded quotes are doubled per RFC 4180.
  if (/[",\r\n]/.test(field)) return `"${field.replace(/"/g, '""')}"`
  return field
}

/** Per-cell precision. Native modes (values, log) honor each series's
 * declared precision so counts stay whole and rates keep two decimals.
 * Transformed modes (indexed, percent-*) collapse to 1 decimal so the
 * percent/index column reads consistently regardless of source family.
 *
 * Uses effectiveModeFor so non-idx series in indexed mode (which
 * stay in native units — see chartLayout.effectiveModeFor) keep
 * their declared precision instead of being forced to 1 decimal. */
function csvValuePrecision(s: ChartSeries, mode: DisplayMode): number {
  const effective = effectiveModeFor(s.family, mode)
  if (effective === 'values' || effective === 'log') return s.precision ?? 1
  return 1
}

function buildChartCsv(
  series: ChartSeries[],
  frequency: Frequency,
  mode: DisplayMode,
): string {
  const provisional = new Set<number>()
  if (frequency === 'monthly') {
    for (const s of series) {
      if (s.data.length <= CSV_TAIL_LENGTH) continue
      for (let i = s.data.length - CSV_TAIL_LENGTH; i < s.data.length; i++) {
        provisional.add(s.data[i].date.getTime())
      }
    }
  }

  const tsSet = new Set<number>()
  for (const s of series) for (const p of s.data) tsSet.add(p.date.getTime())
  const ts = [...tsSet].sort((a, b) => a - b)

  const byTs = series.map((s) => {
    const m = new Map<number, number>()
    for (const p of s.data) m.set(p.date.getTime(), p.value)
    return m
  })

  const header = ['תאריך', ...series.map((s) => s.name), 'זמני']
  const lines: string[] = [header.map(csvEscape).join(',')]

  for (const t of ts) {
    const dateLabel = formatXAxisTick(t, frequency)
    const cells = byTs.map((m, i) => {
      const v = m.get(t)
      if (v == null) return ''
      return v.toFixed(csvValuePrecision(series[i], mode))
    })
    const flag = provisional.has(t) ? 'כן' : 'לא'
    lines.push([dateLabel, ...cells, flag].map(csvEscape).join(','))
  }

  // BOM (U+FEFF, the invisible character literal below) so Excel
  // detects UTF-8 and renders Hebrew correctly; CRLF line endings
  // per RFC 4180 + best Excel compatibility.
  return '﻿' + lines.join('\r\n') + '\r\n'
}

function triggerCsvDownload(filename: string, csv: string): void {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  // Anchor needs to be in the document for some browsers (Firefox)
  // to honor the click. Append/remove around the synthetic click; the
  // DOM is unchanged by the time control returns to the caller.
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  // Defer revoke so the browser has time to start the download before
  // the object URL is invalidated.
  setTimeout(() => URL.revokeObjectURL(url), 0)
}

function todayYmd(): string {
  const d = new Date()
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

type Preset = 'max' | '10y' | '5y' | '3y' | '1y'
// Frequency + DisplayMode imported from chartLayout (shared with
// Chart.tsx so axis scale logic and chip-group state stay in sync).

const PRESET_OPTIONS: { id: Preset; label: string }[] = [
  { id: 'max', label: 'Max' },
  { id: '10y', label: '10Y' },
  { id: '5y',  label: '5Y' },
  { id: '3y',  label: '3Y' },
  { id: '1y',  label: '1Y' },
]

const FREQUENCY_OPTIONS: { id: Frequency; label: string }[] = [
  { id: 'monthly',    label: 'חודשי' },
  { id: 'quarterly',  label: 'רבעוני' },
  { id: 'semiannual', label: 'חצי-שנתי' },
  { id: 'yearly',     label: 'שנתי' },
]

const DISPLAY_OPTIONS: { id: DisplayMode; label: string }[] = [
  { id: 'values',             label: 'ערכים' },
  { id: 'indexed',            label: 'מותאם 100' },
  { id: 'percent-cumulative', label: 'שינוי % מצטבר' },
  { id: 'percent-period',     label: 'שינוי % תקופתי' },
  { id: 'log',                label: 'לוגריתמי' },
]

/** Synthetic monthly series for cards still in demo mode. */
const SYNTHETIC_BRUSH_DATA: BrushDataPoint[] = (() => {
  const out: BrushDataPoint[] = []
  const totalMonths = 30 * 12
  const end = new Date(Date.UTC(2026, 3, 1))
  for (let i = totalMonths - 1; i >= 0; i--) {
    const d = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth() - i, 1))
    const t = (totalMonths - 1 - i) / 12
    const trend = 80 + t * 4
    const cycle = 22 * Math.sin(t * 0.6)
    const wobble = 8 * Math.sin(t * 3.1 + 1.7)
    out.push({ date: d, value: trend + cycle + wobble })
  }
  return out
})()

function deriveDataRange(brushData: BrushDataPoint[]): { start: Date; end: Date } {
  if (brushData.length === 0) {
    const now = new Date(Date.UTC(2026, 3, 1))
    return { start: addMonths(now, -360), end: now }
  }
  return {
    start: brushData[0].date,
    end: brushData[brushData.length - 1].date,
  }
}

/** Map a preset to its concrete date range, anchored to the data's
 * latest point. Years-back ranges clamp to dataStart for short series. */
function presetToRange(preset: Preset, dataStart: Date, dataEnd: Date): DateRange {
  if (preset === 'max') return { start: dataStart, end: dataEnd }
  const years = parseInt(preset, 10)
  const candidate = addMonths(dataEnd, -years * 12)
  return {
    start: candidate.getTime() < dataStart.getTime() ? dataStart : candidate,
    end: dataEnd,
  }
}

function activePresetFor(
  range: DateRange,
  dataStart: Date,
  dataEnd: Date,
): Preset | null {
  for (const opt of PRESET_OPTIONS) {
    if (rangesEqual(range, presetToRange(opt.id, dataStart, dataEnd))) return opt.id
  }
  return null
}

/** Three filter kinds participate in cross-card mirroring. Brush
 * range is intentionally excluded for now (per the design spec —
 * planned for a later iteration). */
export type FilterKind = 'frequency' | 'mode' | 'preset'

export type FilterChange =
  | { kind: 'frequency'; value: Frequency }
  | { kind: 'mode'; value: DisplayMode }
  | { kind: 'preset'; value: Preset }

/** Snapshot of the filter triplet, exchanged between paired cards
 * so each can decide whether the user's last change diverges from
 * the other's value (and so warrants showing the apply-pill). */
export interface FilterSnapshot {
  frequency: Frequency
  mode: DisplayMode
  preset: Preset | null
}

/** Imperative handle: parent calls `applyFilterChange` when the
 * other card's pill is clicked, mirroring that filter into this
 * card without firing onUserFilterChange (so card B doesn't itself
 * pop a pill that would offer to mirror back to card A). */
export interface ChartCardHandle {
  applyFilterChange: (change: FilterChange) => void
}

export interface ChartCardProps {
  slotId: 'left' | 'right'
  /** Optional accessible name for the chart article. When omitted,
   * the aria-label falls back to a slot-derived label. The title is
   * never rendered visually — the user-facing header now shows only
   * the live meta line ("X/5 סדרות · range") so it stays honest as
   * the user adds/removes series. */
  title?: string
  /** Names rendered as legend chips when the card is in demo mode.
   * Ignored in real mode (the legend reflects the active series). */
  defaultSeriesNames: string[]
  /** Real-data path. When undefined, the card runs in demo mode.
   * Each spec is hydrated via useSeriesList from the registry. The
   * card's active-spec list seeds from this prop and grows as the
   * user adds entries via the SeriesPicker. */
  initialSpecs?: SeriesSpec[]
  /** Latest filter snapshot of the OTHER card in the pair. Used to
   * suppress the apply-pill when the other card's value already
   * matches the new value (nothing to mirror). */
  otherSnapshot?: FilterSnapshot
  /** Fires on every user-driven filter change (chip click, preset
   * tap). Programmatic changes from applyFilterChange do NOT fire
   * this — that prevents mirror-back ping-pong. */
  onUserFilterChange?: (change: FilterChange) => void
  /** Fires when the user clicks the pill's ✓. Parent should mirror
   * the change to the other card via that card's imperative handle. */
  onMirrorRequest?: (change: FilterChange) => void
}

export const ChartCard = forwardRef<ChartCardHandle, ChartCardProps>(function ChartCard({
  slotId,
  title,
  defaultSeriesNames,
  initialSpecs,
  otherSnapshot,
  onUserFilterChange,
  onMirrorRequest,
}, ref) {
  const realMode = initialSpecs !== undefined
  const theme = useResolvedTheme()

  // Active spec list — owned here so the picker can append. Seeded
  // from initialSpecs once at mount; subsequent prop changes are
  // ignored on purpose (the user's added series shouldn't get
  // clobbered if the parent re-renders with a fresh array reference).
  const [specs, setSpecs] = useState<SeriesSpec[]>(initialSpecs ?? [])

  // Per-series visual type overrides. Keyed by specKey so adds/removes
  // don't churn unrelated entries. Absent key → use registry default
  // (entry.defaultType, which itself falls back to defaultTypeFor in
  // Chart.tsx). The legend's type-icon button clicks through TYPE_CYCLE
  // and writes to this map.
  const [typeOverrides, setTypeOverrides] = useState<Record<string, SeriesType>>({})

  // Per-series color slot. Each spec gets a monotonically-increasing
  // integer slot the first time it appears on the card; the slot
  // feeds into seriesColorBySlot to produce a stable hue. Slots are
  // never decremented or recycled — removing a series leaves its slot
  // entry intact, so removing+re-adding the same spec yields the same
  // color, and removing one series doesn't shift the colors of the
  // others (which a position-based scheme would do).
  //
  // Lazy useState initializer reads initialSpecs once at mount; the
  // ref starts at the seeded count so subsequent adds get fresh slots.
  const [colorSlots, setColorSlots] = useState<Record<string, number>>(() => {
    const m: Record<string, number> = {}
    if (initialSpecs) initialSpecs.forEach((s, i) => { m[specKey(s)] = i })
    return m
  })
  const nextSlotRef = useRef(initialSpecs?.length ?? 0)
  // Helper: idempotently assign a slot for a specKey if it isn't
  // already mapped. Used by handlePick (single-add) and below by
  // handleApplyPreset (preset bulk-add).
  const assignColorSlot = useCallback((k: string) => {
    setColorSlots((prev) => {
      if (k in prev) return prev
      const slot = nextSlotRef.current++
      return { ...prev, [k]: slot }
    })
  }, [])

  const hydrated = useSeriesList(realMode ? specs : [])

  // Loading: any spec still resolving. Error: surface the first one
  // (showing all is noisy and a single failure usually points to a
  // bigger upstream outage).
  const seriesLoading = hydrated.some((h) => h.loading)
  const seriesError = hydrated.find((h) => h.error)?.error ?? null

  // Assemble ChartSeries[] from hydrated specs + registry metadata.
  // Color comes from the spec's stable color slot (assigned in
  // assignColorSlot the first time the spec appeared on the card).
  // The slot is fed through seriesColorBySlot to produce an HSL
  // hue — golden-angle distribution so any number of series stay
  // visually distinct, and stable across remove/re-add of other
  // series. typeOverrides wins over the registry default when present.
  const series = useMemo<ChartSeries[] | undefined>(() => {
    if (!realMode) return undefined
    return hydrated.map((h) => {
      const entry = getRegistryEntry(h.spec.registryId)
      const id = specKey(h.spec)
      // Fallback to slot 0 only if the slot hasn't been assigned yet
      // (a transient state on the very first render before the
      // assignColorSlot effect runs). In practice every code path
      // that adds a spec also calls assignColorSlot synchronously.
      const slot = colorSlots[id] ?? 0
      return {
        id,
        // displaySeriesName appends the district label when district
        // != national, so two chips for the same series in different
        // districts are distinguishable. Falls back to the registry id
        // when the entry has been removed (defensive — shouldn't happen
        // in practice, but useSeriesList might briefly hold a stale id).
        name: displaySeriesName(entry?.name ?? h.spec.registryId, h.spec.district),
        color: seriesColorBySlot(slot, theme),
        data: h.data.map((p) => ({ date: p.date, value: p.value })),
        family: entry?.family ?? 'count',
        type: typeOverrides[id] ?? entry?.defaultType,
        isStock: entry?.isStock,
        unit: entry?.unit,
        precision: entry?.precision,
        thousands: entry?.thousands,
        step: entry?.step,
        // Propagate stackId from the spec so the chart engine can
        // group bar series into stacked columns. Set when the user
        // adds via a group registry entry that bundles members
        // sharing a stackId.
        stackId: h.spec.stackId,
        // Forward the registry's `group` so planAxesByFamily can
        // split same-family series onto separate axes when their
        // groups differ (sales vs construction is the canonical case).
        group: entry?.group,
        // Forward the registry's per-series aggregation override so
        // filteredSeries' aggregateData call uses 'average' for
        // unemployment / wage instead of the family default
        // ('last' for pct, 'sum' for count). Falls back to
        // defaultAggregation in the filteredSeries memo when
        // undefined.
        aggregation: entry?.aggregation,
      }
    })
  }, [realMode, hydrated, theme, typeOverrides, colorSlots])

  // Brush data:
  //   - line visualization → longest series's history (most rows)
  //   - bounds (start/end) → UNION of every series's extents
  // Padding the brushData with synthetic boundary points at the union
  // edges makes BrushOverview's start/end (which it reads from
  // data[0].date / data[last].date) cover all series, so adding a
  // longer-history series expands the brush extent even when a shorter
  // series is the one visualized. The padding points reuse the
  // visualized series's first/last values so the mini-line just
  // extends flat into the wider region — visually informative ("data
  // signal starts here") without distorting the y-axis range.
  const brushData = useMemo<BrushDataPoint[]>(() => {
    if (!realMode || series == null || series.length === 0) return SYNTHETIC_BRUSH_DATA
    const longest = series.reduce((a, b) =>
      a.data.length >= b.data.length ? a : b,
    )
    if (longest.data.length === 0) return SYNTHETIC_BRUSH_DATA
    let unionStart = longest.data[0].date.getTime()
    let unionEnd = longest.data[longest.data.length - 1].date.getTime()
    for (const s of series) {
      if (s.data.length === 0) continue
      const sStart = s.data[0].date.getTime()
      const sEnd = s.data[s.data.length - 1].date.getTime()
      if (sStart < unionStart) unionStart = sStart
      if (sEnd > unionEnd) unionEnd = sEnd
    }
    const out = longest.data.map((p) => ({ date: p.date, value: p.value }))
    if (unionStart < out[0].date.getTime()) {
      out.unshift({ date: new Date(unionStart), value: out[0].value })
    }
    if (unionEnd > out[out.length - 1].date.getTime()) {
      out.push({
        date: new Date(unionEnd),
        value: out[out.length - 1].value,
      })
    }
    return out
  }, [realMode, series])

  const dataExtent = useMemo(() => deriveDataRange(brushData), [brushData])
  const initialRange = useMemo(
    () => presetToRange('5y', dataExtent.start, dataExtent.end),
    [dataExtent],
  )

  const [range, setRange] = useState<DateRange>(initialRange)
  // Quarterly is the default initial frequency: monthly granularity is
  // noisy for several of the construction/sales series, and quarterly
  // averages give a less spiky first impression. Users still control
  // frequency directly via the chip group — this default doesn't auto-
  // switch on range change.
  const [frequency, setFrequency] = useState<Frequency>('quarterly')
  // Display mode storage: `null` means "no explicit user pick — use
  // the smart default below." Once the user clicks a mode chip (or
  // accepts a mirror pill), storedMode flips to a concrete value and
  // sticks until the chart drops to zero series, at which point we
  // reset it inside handleRemoveSpec. This implicit-override design
  // replaces a userOverrodeModeRef + sync-effect pair: the override
  // bit is "storedMode !== null" and the effective mode is computed
  // declaratively below — no setState-in-effect, no cascading render.
  const [storedMode, setStoredMode] = useState<DisplayMode | null>(null)
  // Effective display mode. When the chart contains 2+ index-family
  // series, "מותאם 100" (indexed) is the smarter default than raw
  // values — different index series live on different scales, so the
  // values view often crushes them. Mixed charts (1 index + others)
  // and pure non-index charts stay in 'values'. The user's explicit
  // pick (storedMode) overrides the smart default in either direction.
  const idxCount = useMemo(
    () =>
      series == null
        ? 0
        : series.reduce((n, s) => (s.family === 'idx' ? n + 1 : n), 0),
    [series],
  )
  const mode: DisplayMode = storedMode ?? (idxCount >= 2 ? 'indexed' : 'values')
  const [pickerOpen, setPickerOpen] = useState(false)
  const addBtnRef = useRef<HTMLButtonElement>(null)
  // Debounce timer for brush drag → track('range_change'). The brush
  // fires onRangeChange on every pixel during a drag; we want one
  // analytics event per drag gesture, not per frame.
  const brushTrackTimerRef = useRef<number | null>(null)
  // aria-live announcement for screen-reader users. Updated on the
  // user actions that change the chart's content (series add/remove)
  // or its display semantics (frequency / display mode). The region
  // itself is visually hidden but read aloud by AT.
  const [liveAnnouncement, setLiveAnnouncement] = useState('')

  // Snap to the 5Y preset ONCE — the first time real data is available
  // (longest series has > 0 rows, so brushData isn't synthetic). After
  // that the user's selection is sticky: adding a series with older
  // history expands the brush extent, but the highlighted window stays
  // where the user left it. Without this gate, `setRange` would fire
  // every time dataExtent changed (e.g., a series with a wider range
  // resolves), yanking the user out of their selected window.
  const initialSnapDoneRef = useRef(false)
  useEffect(() => {
    if (initialSnapDoneRef.current) return
    if (brushData === SYNTHETIC_BRUSH_DATA) return
    initialSnapDoneRef.current = true
    setRange(presetToRange('5y', dataExtent.start, dataExtent.end))
  }, [brushData, dataExtent])

  // Pending preset range — set by handleApplyPreset when the chart
  // currently has no real data (so the immediate range snap inside
  // the handler would compute against synthetic dataExtent and be
  // wrong). Once new data lands and dataExtent updates, this effect
  // re-snaps the range to the preset's intended window. Cleared on
  // success so it doesn't fire again on subsequent series changes.
  const pendingPresetRangeRef = useRef<ChartPreset['rangePreset'] | null>(null)
  useEffect(() => {
    if (pendingPresetRangeRef.current == null) return
    if (brushData === SYNTHETIC_BRUSH_DATA) return
    const r = pendingPresetRangeRef.current
    pendingPresetRangeRef.current = null
    setRange(presetToRange(r, dataExtent.start, dataExtent.end))
  }, [brushData, dataExtent])

  const activePreset = useMemo(
    () => activePresetFor(range, dataExtent.start, dataExtent.end),
    [range, dataExtent],
  )
  const seriesCountForMeta = realMode ? specs.length : defaultSeriesNames.length
  const meta = `${seriesCountForMeta} סדרות · ${formatHebrewDateRange(range)}`

  // Pill state: at most one pill visible at a time (per the design
  // spec "don't stack"). The id increments on each new pill so React
  // remounts <ApplyPill> for the latest change rather than reusing
  // the previous instance — this guarantees the new fade-in plays
  // cleanly even if a prior pill was mid-animation.
  const [pill, setPill] = useState<
    { id: number; change: FilterChange } | null
  >(null)
  const pillIdRef = useRef(0)

  // Show the pill for `change` IF the other card's value diverges.
  // Replaces any pill currently visible (the previous instance just
  // unmounts; no fade-out for the old one — keeps the timing simple
  // and matches the spec's "don't stack" guidance).
  const maybeShowPill = useCallback(
    (change: FilterChange) => {
      if (!otherSnapshot) return
      const otherValue = otherSnapshot[change.kind]
      if (otherValue === change.value) return
      pillIdRef.current += 1
      setPill({ id: pillIdRef.current, change })
    },
    [otherSnapshot],
  )

  const handlePreset = (next: Preset) => {
    setRange(presetToRange(next, dataExtent.start, dataExtent.end))
    onUserFilterChange?.({ kind: 'preset', value: next })
    maybeShowPill({ kind: 'preset', value: next })
    track('range_change', { method: 'preset', preset: next, slot: slotId })
    const label = PRESET_OPTIONS.find((p) => p.id === next)?.label ?? next
    setLiveAnnouncement(`הטווח שונה ל-${label}`)
  }

  const handleFrequency = (next: Frequency) => {
    setFrequency(next)
    onUserFilterChange?.({ kind: 'frequency', value: next })
    maybeShowPill({ kind: 'frequency', value: next })
    track('frequency_change', { value: next, slot: slotId })
    const label = FREQUENCY_OPTIONS.find((f) => f.id === next)?.label ?? next
    setLiveAnnouncement(`התדירות שונתה ל${label}`)
  }

  const handleMode = (next: DisplayMode) => {
    setStoredMode(next)
    onUserFilterChange?.({ kind: 'mode', value: next })
    maybeShowPill({ kind: 'mode', value: next })
    track('display_mode_change', { value: next, slot: slotId })
    const label = DISPLAY_OPTIONS.find((d) => d.id === next)?.label ?? next
    setLiveAnnouncement(`מצב התצוגה שונה ל${label}`)
  }

  // Programmatic apply triggered by the parent when the OTHER card's
  // pill is clicked. Updates this card's local state but does NOT
  // fire onUserFilterChange — that path is reserved for explicit
  // user actions (chip clicks, preset taps). Without this gate, card
  // B would itself raise a pill offering to mirror back to card A,
  // creating a ping-pong loop.
  useImperativeHandle(
    ref,
    () => ({
      applyFilterChange(change: FilterChange) {
        if (change.kind === 'frequency') setFrequency(change.value)
        else if (change.kind === 'mode') {
          // Mirroring a mode from the other card is itself a deliberate
          // user act (they clicked the apply pill). Set storedMode so
          // the smart default doesn't subsequently override what the
          // user just accepted.
          setStoredMode(change.value)
        }
        else if (change.kind === 'preset') {
          setRange(presetToRange(change.value, dataExtent.start, dataExtent.end))
        }
      },
    }),
    [dataExtent.start, dataExtent.end],
  )

  const handlePillApply = useCallback(() => {
    if (!pill) return
    onMirrorRequest?.(pill.change)
  }, [pill, onMirrorRequest])

  const handlePillDismiss = useCallback(() => {
    setPill(null)
  }, [])

  // Apply a complete preset — replaces specs, sets frequency, snaps
  // range, and (optionally) sets display mode. typeOverrides are
  // cleared so a preset starts from each member's registry default
  // rather than carrying over chip-cycled types from the previous
  // composition. The picker closes on its own (caller).
  const handleApplyPreset = useCallback((preset: ChartPreset) => {
    setSpecs(preset.series)
    setTypeOverrides({})
    // Assign color slots for any preset members the card hasn't seen
    // before. Existing slots are preserved (re-applying the same
    // preset, or applying a preset that overlaps with the current
    // composition, keeps colors stable).
    setColorSlots((prev) => {
      let changed = false
      const next = { ...prev }
      for (const s of preset.series) {
        const k = specKey(s)
        if (!(k in next)) {
          next[k] = nextSlotRef.current++
          changed = true
        }
      }
      return changed ? next : prev
    })
    setFrequency(preset.frequency)
    // Display mode: explicit override wins; otherwise null lets the
    // smart-default rules in `mode = storedMode ?? smartDefault`
    // take effect for the new composition.
    setStoredMode(preset.displayMode ?? null)
    // Range: snap immediately if real data is loaded (gives a
    // smooth visual handoff). Otherwise stash for the deferred
    // effect to apply once new data lands. Both paths use
    // presetToRange against the freshest dataExtent we have.
    if (brushData !== SYNTHETIC_BRUSH_DATA) {
      setRange(presetToRange(preset.rangePreset, dataExtent.start, dataExtent.end))
    }
    pendingPresetRangeRef.current = preset.rangePreset
    track('preset_applied', {
      preset_id: preset.id,
      slot: slotId,
      series_count: preset.series.length,
      frequency: preset.frequency,
      range_preset: preset.rangePreset,
    })
    setLiveAnnouncement(`התצוגה ${preset.name} הופעלה`)
  }, [brushData, dataExtent, slotId])

  // Picker handler — toggle a single spec. If the (registryId,
  // district) pair is already on the chart, this removes it (the
  // ✓ in the picker is now a toggle indicator, not a disabled
  // marker). If not present and the cap allows, adds it.
  const handlePick = (next: SeriesSpec) => {
    const k = specKey(next)
    const exists = specs.some((s) => specKey(s) === k)
    if (exists) {
      handleRemoveSpec(k)
      return
    }
    setSpecs((prev) => [...prev, next])
    assignColorSlot(k)
    track('series_add', {
      registry_id: next.registryId,
      district: next.district,
      slot: slotId,
      ...(next.stackId ? { stack_id: next.stackId } : {}),
    })
    const entry = getRegistryEntry(next.registryId)
    const name = entry?.name ?? next.registryId
    setLiveAnnouncement(`הסדרה ${name} נוספה לתרשים`)
  }

  // Legend X handler: remove the series with the given specKey id.
  // Also drop any type override for that key so a future re-add of
  // the same spec starts from the registry default rather than the
  // previously-cycled override.
  const handleRemoveSpec = (id: string) => {
    const removed = series?.find((s) => s.id === id)
    const willEmpty = specs.length === 1 && specs[0] && specKey(specs[0]) === id
    setSpecs((prev) => prev.filter((s) => specKey(s) !== id))
    setTypeOverrides((prev) => {
      if (!(id in prev)) return prev
      const next = { ...prev }
      delete next[id]
      return next
    })
    // Fresh start: when removing the last spec, clear the user's
    // stored display-mode pick so the smart default re-engages on
    // whatever they build next. Anchored to the only path that drains
    // the chart (legend X, including the picker's toggle-off branch
    // which routes through here).
    if (willEmpty) setStoredMode(null)
    track('series_remove', { spec_key: id, slot: slotId })
    if (removed) setLiveAnnouncement(`הסדרה ${removed.name} הוסרה מהתרשים`)
  }

  /** Resolve the visible type for a given spec key — override wins,
   * falling back to the registry default, then to the family-based
   * defaultTypeFor. Mirrors the chain in `series` assembly above so
   * the legend icon and the chart render stay in sync. */
  const currentTypeFor = (id: string): SeriesType => {
    if (typeOverrides[id]) return typeOverrides[id]
    const spec = specs.find((s) => specKey(s) === id)
    if (!spec) return 'line'
    const entry = getRegistryEntry(spec.registryId)
    if (entry?.defaultType) return entry.defaultType
    if (entry?.family === 'count') return entry.isStock ? 'area' : 'bar'
    return 'line'
  }

  // Legend type-icon handler: cycle line → bar → area → line for
  // the series with this id. Reads the CURRENT effective type
  // (override or registry default), advances one step, and writes
  // the result as an explicit override.
  const handleCycleType = (id: string) => {
    const current = currentTypeFor(id)
    setTypeOverrides((prev) => ({ ...prev, [id]: nextType(current) }))
  }

  const alreadyAdded = useMemo(
    () => new Set(specs.map(specKey)),
    [specs],
  )

  // Pipeline: filter to range → aggregate by frequency → transform
  // by display mode. The transformation happens here (not in Chart)
  // so the chart render stays stateless: it receives the data shape
  // it should plot, plus the mode prop only for axis/tick/tooltip
  // formatting. Indexed and percent modes mutate values; values and
  // log modes pass through.
  const filteredSeries = useMemo<ChartSeries[]>(() => {
    if (!realMode || series == null) return []
    return series.map((s) => {
      const inRange = s.data.filter(
        (p) =>
          p.date.getTime() >= range.start.getTime() &&
          p.date.getTime() <= range.end.getTime(),
      )
      const method = s.aggregation ?? defaultAggregation(s.family, s.isStock)
      const aggregated = aggregateData(inRange, frequency, method)
      // Per-series effective mode: in indexed mode, count/pct series
      // pass through unchanged ('values' branch of transformForMode)
      // so a sales bar series next to two index lines keeps its
      // 1500–5400 native range instead of getting rebased to 100.
      const seriesMode = effectiveModeFor(s.family, mode)
      const transformed = transformForMode(aggregated, seriesMode)
      return { ...s, data: transformed }
    })
  }, [realMode, series, range, frequency, mode])

  const allEmpty = filteredSeries.every((s) => s.data.length === 0)

  // CSV download — exports filteredSeries (already aggregated by
  // frequency and transformed by mode), with the current range, in
  // the same format the user sees on screen. Defined here so it
  // closes over the post-pipeline data; the button is disabled
  // whenever there's nothing meaningful to download.
  const handleDownload = useCallback(() => {
    if (filteredSeries.length === 0) return
    const csv = buildChartCsv(filteredSeries, frequency, mode)
    triggerCsvDownload(`dirametrics-data-${todayYmd()}.csv`, csv)
    track('csv_download', {
      slot: slotId,
      series_count: filteredSeries.length,
      frequency,
      display_mode: mode,
      date_range_start: range.start.toISOString().slice(0, 10),
      date_range_end: range.end.toISOString().slice(0, 10),
    })
    setLiveAnnouncement('הקובץ הורד')
  }, [filteredSeries, frequency, mode, range, slotId])

  // Brush alignment: pad the brush wrapper so its left/right edges
  // line up with the chart's plot area (inset by YAxis widths +
  // Recharts margins). For percent-period mode the axis layout
  // depends on each series's RANGE of transformed values, so we pass
  // filteredSeries (which holds the transformed data); for other
  // modes the layout depends only on family info, which is the same
  // pre- and post-transform, so we pass the original series.
  // Estimate-based offsets — fast, available immediately on render
  // before Recharts has actually rendered. Used as a fallback while
  // measurement settles.
  const estimatedPlotOffsets = useMemo(() => {
    if (!realMode || !series || series.length === 0) {
      return { left: 0, right: 0 }
    }
    const dataForLayout = mode === 'percent-period' ? filteredSeries : series
    return getPlotOffsets(dataForLayout, mode)
  }, [realMode, series, filteredSeries, mode])

  // Measured offsets — read from the actual rendered DOM after
  // Recharts has laid out. Recharts' axis widths can differ from
  // axisWidthFor's estimate (tick labels, internal padding, etc.),
  // so the only reliable way to align the brush with the plot area
  // is to measure where Recharts actually drew it.
  //
  // The ref attaches to the chart-card <article>; we query
  // .chart-engine + .recharts-cartesian-grid inside, then compute
  // grid.left - chartEngine.left for the offset. The chart-engine
  // and the brush-align div are sibling flex items in the article
  // so they share the same left edge — measuring the grid relative
  // to chart-engine yields exactly the inset the brush needs.
  const cardRef = useRef<HTMLElement>(null)
  const [measuredPlotOffsets, setMeasuredPlotOffsets] = useState<
    { left: number; right: number } | null
  >(null)

  useEffect(() => {
    const el = cardRef.current
    if (!el) return

    const measure = () => {
      const chartEngine = el.querySelector<HTMLElement>('.chart-engine')
      const grid = el.querySelector<SVGGElement>('.recharts-cartesian-grid')
      if (!chartEngine || !grid) return
      const engineRect = chartEngine.getBoundingClientRect()
      const gridRect = grid.getBoundingClientRect()
      // Skip during the initial -1×-1 phase — Recharts' first paint
      // before ResponsiveContainer measures the parent emits a
      // zero/negative-sized grid. Wait for a real layout.
      if (gridRect.width <= 0 || engineRect.width <= 0) return
      const left = Math.max(0, Math.round(gridRect.left - engineRect.left))
      const right = Math.max(0, Math.round(engineRect.right - gridRect.right))
      setMeasuredPlotOffsets((prev) => {
        // Avoid no-op state updates (which would trigger a re-render
        // → another ResizeObserver call → infinite loop in some
        // browsers).
        if (prev && prev.left === left && prev.right === right) return prev
        return { left, right }
      })
    }

    // Measure across the next two animation frames — Recharts
    // sometimes lays out in two passes (initial render at -1×-1,
    // then again after ResizeObserver). One frame is usually
    // enough; two is a safety margin.
    let raf2 = 0
    const raf1 = requestAnimationFrame(() => {
      measure()
      raf2 = requestAnimationFrame(measure)
    })

    // Re-measure on size change (window resize, theme toggle reflow,
    // sibling reflow when display mode changes axis widths).
    const ro = new ResizeObserver(() => {
      requestAnimationFrame(measure)
    })
    ro.observe(el)

    return () => {
      cancelAnimationFrame(raf1)
      cancelAnimationFrame(raf2)
      ro.disconnect()
    }
    // Re-run when chart contents change in ways that affect axis
    // layout (series add/remove, display mode that changes axis
    // count, frequency that affects axis labels).
  }, [series, mode, frequency])

  // Use the measured value once available; fall back to the estimate
  // for the first paint and any moments before a measurement lands.
  const plotOffsets = measuredPlotOffsets ?? estimatedPlotOffsets

  // Accessible name for the chart article. The visible header shows
  // only the meta line, but assistive tech still benefits from a
  // stable label. Use the caller-supplied title when present; fall
  // back to a generic slot-derived label otherwise.
  const ariaLabel = title ?? (slotId === 'left' ? 'תרשים שמאלי' : 'תרשים ימני')

  return (
    <article
      ref={cardRef}
      className="chart-card"
      data-slot={slotId}
      aria-label={ariaLabel}
    >
      {/* aria-live region for screen-reader announcements of state
       * changes — series add/remove, filter changes. Visually
       * hidden via .visually-hidden but read aloud politely. */}
      <div
        className="visually-hidden"
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        {liveAnnouncement}
      </div>

      {/* 1. Header — no fixed title (the title used to describe a
       * static composition; now that users add/remove series, the
       * meta line "X/5 סדרות · range" is the honest header). */}
      <header className="chart-card-header">
        <span className="chart-card-meta">{meta}</span>
        <button
          type="button"
          className="chart-icon-btn"
          aria-label="הורדת CSV"
          title="הורדת CSV"
          onClick={handleDownload}
          // Disabled whenever there's nothing meaningful to export:
          // demo mode, no specs, mid-load, error, or all series empty
          // in the current range.
          disabled={!realMode || specs.length === 0 || seriesLoading || !!seriesError || allEmpty}
        >
          <DownloadIcon />
        </button>
      </header>

      {/* 2. Legend — in real mode reflects the actual series colors so
       * legend dots match line strokes; in demo mode falls back to
       * positional palette colors via data-series-index. Series with
       * no data in the current range render muted with a parenthetical
       * note so the legend honestly signals what's actually visible. */}
      <div className="chart-card-legend" aria-label="סדרות">
        {realMode && series && series.length > 0 ? (
          series.map((s) => {
            const filtered = filteredSeries.find((fs) => fs.id === s.id)
            const noDataInRange =
              !seriesLoading && filtered != null && filtered.data.length === 0
            const seriesType = currentTypeFor(s.id)
            return (
              <span
                key={s.id}
                className={`chart-legend-chip${noDataInRange ? ' chart-legend-chip--muted' : ''}`}
              >
                <span
                  className="chart-legend-dot"
                  style={{ background: s.color }}
                  aria-hidden="true"
                />
                <span className="chart-legend-name">{s.name}</span>
                {/* Stack hint: tiny stacked-bars glyph appears on the
                 * chip of every series that participates in a stack
                 * group. Doubles as an a11y signal via the title
                 * attribute — screen readers pick that up. */}
                {s.stackId && (
                  <span
                    className="chart-legend-stack"
                    title="חלק מקבוצה מצטברת"
                    aria-label="חלק מקבוצה מצטברת"
                  >
                    <StackIcon />
                  </span>
                )}
                {noDataInRange && (
                  <span className="chart-legend-note">(אין נתונים בטווח)</span>
                )}
                <button
                  type="button"
                  className="chart-legend-type"
                  onClick={() => handleCycleType(s.id)}
                  aria-label={`שנה סוג תצוגה (${seriesTypeLabel(seriesType)})`}
                  title={`שנה סוג תצוגה (${seriesTypeLabel(seriesType)})`}
                >
                  <SeriesTypeIcon type={seriesType} />
                </button>
                <button
                  type="button"
                  className="chart-legend-remove"
                  onClick={() => handleRemoveSpec(s.id)}
                  aria-label={`הסר סדרה: ${s.name}`}
                  title={`הסר סדרה: ${s.name}`}
                >
                  <span aria-hidden="true">×</span>
                </button>
              </span>
            )
          })
        ) : defaultSeriesNames.length === 0 ? (
          <span className="chart-card-legend-empty">אין סדרות נבחרות</span>
        ) : (
          defaultSeriesNames.map((name, i) => (
            <span key={i} className="chart-legend-chip">
              <span
                className="chart-legend-dot"
                data-series-index={i}
                aria-hidden="true"
              />
              <span className="chart-legend-name">{name}</span>
            </span>
          ))
        )}
      </div>

      {/* 3. Chart area — branches on mode + load state. The "no series"
       * state takes priority over loading/error so removing the last
       * series via the legend X drops the user into a helpful empty
       * shell rather than the (briefly-flashing) loading skeleton. */}
      {!realMode ? (
        <PlaceholderChart />
      ) : specs.length === 0 ? (
        <ChartNoSeries
          height={CHART_HEIGHT}
          onActivate={() => setPickerOpen((v) => !v)}
          expanded={pickerOpen}
        />
      ) : seriesLoading ? (
        <ChartSkeleton height={CHART_HEIGHT} />
      ) : seriesError ? (
        <ChartError message={seriesError} height={CHART_HEIGHT} />
      ) : allEmpty ? (
        <ChartEmpty height={CHART_HEIGHT} />
      ) : (
        <Suspense fallback={<ChartSkeleton height={CHART_HEIGHT} />}>
          <Chart
            series={filteredSeries}
            range={range}
            frequency={frequency}
            displayMode={mode}
            height={CHART_HEIGHT}
          />
        </Suspense>
      )}

      {/* 4. Brush — wrapped to align with the chart's plot area.
       * Padding values come from `plotOffsets`, which prefers the
       * runtime-measured offsets (DOM read of .recharts-cartesian-
       * grid relative to .chart-engine) and falls back to the
       * estimated offsets during the first frame before measurement
       * lands. data-measured surfaces whether the value is from the
       * fallback (during loading) or measurement (post-paint) — keep
       * for now while we're confirming alignment in the wild. */}
      <div
        className="chart-brush-align"
        data-measured={measuredPlotOffsets ? 'true' : 'false'}
        style={{
          paddingLeft: plotOffsets.left,
          paddingRight: plotOffsets.right,
        }}
      >
        <BrushOverview
          data={brushData}
          range={range}
          onRangeChange={(next) => {
            setRange(next)
            // Debounced track: brush drags fire onRangeChange on every
            // pixel; wait 500ms after the last change so we record one
            // event per drag gesture, not hundreds.
            if (brushTrackTimerRef.current != null) {
              window.clearTimeout(brushTrackTimerRef.current)
            }
            brushTrackTimerRef.current = window.setTimeout(() => {
              track('range_change', {
                method: 'brush',
                start: next.start.toISOString().slice(0, 10),
                end: next.end.toISOString().slice(0, 10),
                slot: slotId,
              })
              brushTrackTimerRef.current = null
            }, 500)
          }}
        />
      </div>

      {/* 5. Range row — inset to match the brush + chart plot area
       * so the + הוסף סדרה button on the inline-start and the
       * preset chips on the inline-end line up vertically with the
       * brush handles + chart data edges above. Same plotOffsets
       * source as the brush. Frequency/display rows stay at full
       * card width for now — visual decision pending. */}
      <div
        className="chart-control-row chart-control-row--range"
        style={{
          paddingLeft: plotOffsets.left,
          paddingRight: plotOffsets.right,
        }}
      >
        <div className="series-picker-anchor">
          <button
            ref={addBtnRef}
            type="button"
            className="add-series-btn"
            data-picker-anchor="true"
            aria-haspopup="dialog"
            aria-expanded={pickerOpen}
            onClick={() => setPickerOpen((v) => !v)}
            disabled={!realMode}
          >
            + הוסף סדרה
          </button>
          <SeriesPicker
            open={pickerOpen}
            onClose={() => setPickerOpen(false)}
            onPick={handlePick}
            onApplyPreset={handleApplyPreset}
            alreadyAdded={alreadyAdded}
            atCap={false}
          />
        </div>
        <ChipGroup
          ariaLabel="טווח זמן"
          value={activePreset}
          onChange={handlePreset}
          options={PRESET_OPTIONS}
        />
      </div>

      {/* Range-row pill: a dedicated row beneath the range chips
       * (per design spec "below the chip group, aligned to the
       * active chip"). Aligned to the inline-end (where the chips
       * sit) so it visually associates with the changed control. */}
      {pill?.change.kind === 'preset' && (
        <div className="chart-pill-row chart-pill-row--range">
          <ApplyPill
            key={pill.id}
            onApply={handlePillApply}
            onDismiss={handlePillDismiss}
          />
        </div>
      )}

      {/* 6. Frequency row. Children layout under RTL flex with
       * justify-content: space-between:
       *
       *   [chart-control-group  →  flex-start  =  visual-RIGHT ]
       *   [apply-pill-slot      →  flex-end    =  visual-LEFT  ]
       *
       * Within the group (also RTL flex, default justify-content
       * flex-start packs both children at the start = right), we
       * put the label FIRST so it lands at the visual-far-right of
       * the row — Hebrew RTL natural reading order is label-then-
       * options ("תדירות חודשי | רבעוני | …"). Chips follow to
       * the visual-left of the label, separated by --space-4.
       *
       * Net visual: pill_slot — — — chip chip chip chip תדירות
       *             (visual-left)                       (visual-right)
       *
       * paddingLeft/Right match the range row above so the row's
       * content right edge lands on the same vertical line as the
       * +הוסף סדרה button's right edge — the label's right edge
       * sits flush with that line. */}
      <div
        className="chart-control-row"
        style={{
          paddingLeft: plotOffsets.left,
          paddingRight: plotOffsets.right,
        }}
      >
        <div className="chart-control-group">
          <span className="chart-control-label">תדירות</span>
          <ChipGroup
            ariaLabel="תדירות"
            value={frequency}
            onChange={handleFrequency}
            options={FREQUENCY_OPTIONS}
          />
        </div>
        <span className="apply-pill-slot">
          {pill?.change.kind === 'frequency' && (
            <ApplyPill
              key={pill.id}
              onApply={handlePillApply}
              onDismiss={handlePillDismiss}
            />
          )}
        </span>
      </div>

      {/* 7. Display row — same layout as frequency row: label on
       * visual-far-right, chips to its visual-left, pill on visual-
       * far-left. */}
      <div
        className="chart-control-row"
        style={{
          paddingLeft: plotOffsets.left,
          paddingRight: plotOffsets.right,
        }}
      >
        <div className="chart-control-group">
          <span className="chart-control-label">תצוגה</span>
          <ChipGroup
            ariaLabel="תצוגה"
            value={mode}
            onChange={handleMode}
            options={DISPLAY_OPTIONS}
          />
        </div>
        <span className="apply-pill-slot">
          {pill?.change.kind === 'mode' && (
            <ApplyPill
              key={pill.id}
              onApply={handlePillApply}
              onDismiss={handlePillDismiss}
            />
          )}
        </span>
      </div>
    </article>
  )
})

function PlaceholderChart() {
  return (
    <div className="chart-card-plot" role="img" aria-label="placeholder for chart">
      <span className="chart-card-plot-text">תרשים יוצג כאן (שלב 8)</span>
    </div>
  )
}

function ChartSkeleton({ height }: { height: number }) {
  return (
    <div
      className="chart-skeleton"
      style={{ height }}
      role="status"
      aria-live="polite"
      aria-label="טוען נתונים"
    />
  )
}

function ChartError({ message, height }: { message: string; height: number }) {
  return (
    <div className="chart-error" style={{ height }} role="alert">
      <div className="chart-error-icon" aria-hidden="true">⚠</div>
      <div className="chart-error-title">שגיאה בטעינת הנתונים</div>
      <div className="chart-error-detail">{message}</div>
    </div>
  )
}

function ChartEmpty({ height }: { height: number }) {
  return (
    <div className="chart-empty" style={{ height }} role="status">
      <span>אין נתונים בטווח הזמן הנבחר</span>
    </div>
  )
}

/** Empty-of-series state — shown when the user has removed all series.
 * Distinct from ChartEmpty (which is "all series have no data in the
 * current range"); here the chart has no series to plot at all.
 *
 * Rendered as a <button> so the entire card is clickable + keyboard-
 * activatable: tab focus lands on it, Enter/Space fires onClick. The
 * `data-picker-anchor` attribute opts the element in to the picker's
 * outside-click suppression, so clicking again toggles cleanly (without
 * outside-click closing first and the button reopening on the same
 * pointerup). aria-haspopup + aria-expanded mirror the add-series
 * button below the chart so screen readers describe the same affordance.
 */
function ChartNoSeries({
  height,
  onActivate,
  expanded,
}: {
  height: number
  onActivate: () => void
  expanded: boolean
}) {
  return (
    <button
      type="button"
      className="chart-no-series"
      style={{ height }}
      onClick={onActivate}
      data-picker-anchor="true"
      aria-haspopup="dialog"
      aria-expanded={expanded}
      aria-label="הוסף סדרה לתרשים"
    >
      <span className="chart-no-series-icon" aria-hidden="true">
        <PlusIcon />
      </span>
      <span className="chart-no-series-text">
        אין סדרות נתונים בתרשים. לחץ על + הוסף סדרה כדי להתחיל
      </span>
    </button>
  )
}

function PlusIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true">
      <path d="M10 4v12M4 10h12" />
    </svg>
  )
}

/** Hebrew label for a series type — used as accessible name + tooltip
 * on the legend type-icon button so screen readers and hover both
 * announce what cycling will produce next. */
function seriesTypeLabel(type: SeriesType): string {
  if (type === 'bar') return 'עמודות'
  if (type === 'area') return 'שטח'
  return 'קו'
}

/** Tiny inline SVG glyph showing the current series visual type.
 * 14×14 viewBox; sits inside the .chart-legend-type button. */
function SeriesTypeIcon({ type }: { type: SeriesType }) {
  if (type === 'bar') {
    return (
      <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
        {/* Three bars of varying heights — taller on the right reads
         * as growth, neutral with the legend's default semantic
         * color (currentColor). */}
        <rect x="2"  y="8"  width="2.4" height="4" rx="0.5" />
        <rect x="6"  y="5"  width="2.4" height="7" rx="0.5" />
        <rect x="10" y="3"  width="2.4" height="9" rx="0.5" />
      </svg>
    )
  }
  if (type === 'area') {
    return (
      <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.2" aria-hidden="true">
        {/* Filled polygon below a curving top stroke — the silhouette
         * of an area chart. */}
        <path d="M1 12 L1 8 L4 5 L7 7 L10 3 L13 6 L13 12 Z" fill="currentColor" fillOpacity="0.25" stroke="none" />
        <path d="M1 8 L4 5 L7 7 L10 3 L13 6" />
      </svg>
    )
  }
  // 'line'
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M1 10 L4 6 L7 8 L10 3 L13 5" />
    </svg>
  )
}

/** Tiny stacked-bars hint shown on legend chips of series that
 * belong to a stack group. 10×10, currentColor — inherits the
 * legend's muted text color so the hint reads as an annotation,
 * not a primary control. */
function StackIcon() {
  return (
    <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor" aria-hidden="true">
      <rect x="1" y="6" width="8" height="2" rx="0.5" />
      <rect x="1" y="3" width="8" height="2" rx="0.5" />
    </svg>
  )
}

function DownloadIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M7 1.5v8" />
      <path d="M3.5 6.5L7 10l3.5-3.5" />
      <path d="M2 12h10" />
    </svg>
  )
}
