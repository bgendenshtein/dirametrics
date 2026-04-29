/**
 * DashboardPage — main "/" route. KPI strip + chart strip.
 *
 * RTL flow order in the KPI strip: first DOM child renders rightmost.
 * Order matches the narrative arc the eye walks in Hebrew:
 *   starts → inventory → total sales → housing prices → mortgage rate.
 *
 * The ⓘ icon on each KPI card links to /about#<anchor> — the
 * AboutPage attaches matching ids to its H3 sections so the browser
 * scrolls the right section into view on click.
 */

import { useCallback, useRef, useState } from 'react'

import { ChartCard, type ChartCardHandle, type FilterChange, type FilterSnapshot } from '../components/ChartCard'
import { KpiCard } from '../components/KpiCard'
import type { SeriesSpec } from '../data/seriesRegistry'
import { useDataFreshness } from '../hooks/useDataFreshness'
import { useHousingPriceYoY } from '../hooks/useHousingPriceYoY'
import { useMortgageRate } from '../hooks/useMortgageRate'
import { useNewInventory } from '../hooks/useNewInventory'
import { useStartsTrailing12 } from '../hooks/useStartsTrailing12'
import { useTotalSalesYoY } from '../hooks/useTotalSalesYoY'

function StartsKpi() {
  const s = useStartsTrailing12()
  return (
    <KpiCard
      label="התחלות בנייה"
      sublabel="12 חודשים מול 12 קודמים"
      infoHref="/about#starts"
      delta={
        s.delta !== null && s.direction !== null
          ? { value: s.delta, unit: '%', direction: s.direction }
          : null
      }
      sparkValues={s.sparkValues}
      loading={s.loading}
      error={s.error}
    />
  )
}

function InventoryKpi() {
  const s = useNewInventory()
  return (
    <KpiCard
      label="מלאי דירות חדשות"
      sublabel="3 חודשים מול 3 קודמים"
      infoHref="/about#inventory"
      delta={
        s.delta !== null && s.direction !== null
          ? { value: s.delta, unit: '%', direction: s.direction }
          : null
      }
      sparkValues={s.sparkValues}
      loading={s.loading}
      error={s.error}
    />
  )
}

function TotalSalesKpi() {
  const s = useTotalSalesYoY()
  return (
    <KpiCard
      label="סך מכירות דירות"
      sublabel="3 חודשים מול שנה שעברה"
      infoHref="/about#sales"
      delta={
        s.delta !== null && s.direction !== null
          ? { value: s.delta, unit: '%', direction: s.direction }
          : null
      }
      sparkValues={s.sparkValues}
      loading={s.loading}
      error={s.error}
    />
  )
}

function HousingPriceKpi() {
  const s = useHousingPriceYoY()
  return (
    <KpiCard
      label="מדד מחירי הדיור"
      sublabel="שינוי שנתי"
      infoHref="/about#hpi"
      delta={
        s.delta !== null && s.direction !== null
          ? { value: s.delta, unit: '%', direction: s.direction }
          : null
      }
      sparkValues={s.sparkValues}
      loading={s.loading}
      error={s.error}
    />
  )
}

function MortgageRateKpi() {
  const s = useMortgageRate()
  return (
    <KpiCard
      label="ריבית משכנתא קבועה לא צמודה"
      sublabel="שינוי שנתי"
      infoHref="/about#mortgage"
      level={s.level !== null ? { value: s.level, unit: '%', precision: 2 } : null}
      delta={
        s.delta !== null && s.direction !== null
          ? { value: s.delta, unit: ' נק׳', precision: 2, direction: s.direction }
          : null
      }
      sparkValues={s.sparkValues}
      loading={s.loading}
      error={s.error}
    />
  )
}

/** Default series for the right card — prices/rates/sales narrative.
 * The previous wiring summed new + second-hand sales; the registry
 * exposes those separately, so this seed shows מכירות חדשות (סך הכל)
 * alone. The user can add מכירות יד שנייה via the picker. */
const RIGHT_CARD_SPECS: SeriesSpec[] = [
  { registryId: 'cbs-price-housing-national', district: 'national' },
  { registryId: 'mortgage-fixed-unindexed',    district: 'national' },
  { registryId: 'new-sales-total',             district: 'national' },
]

const LEFT_CARD_SPECS: SeriesSpec[] = [
  { registryId: 'starts',        district: 'national' },
  { registryId: 'completions',   district: 'national' },
  { registryId: 'new-inventory', district: 'national' },
]

function PageTitle() {
  const { label, loading } = useDataFreshness()
  // Subtitle reads cleanly even before the freshness label loads —
  // we drop the trailing sentence rather than show a placeholder.
  const updatedSentence = !loading && label ? ` עודכן ב${label}.` : ''
  return (
    <section className="page-title">
      <div className="page-title-inner">
        <h1>שוק הדיור הישראלי</h1>
        <p>
          תצוגה היסטורית של הריביות, הבנייה, המכירות והמחירים — מאוחדת מנתוני בנק ישראל
          והלמ״ס.{updatedSentence}
        </p>
      </div>
    </section>
  )
}

/** Default filter values that match each ChartCard's local-state
 * defaults. Snapshots start identical so the apply-pill is suppressed
 * until the user diverges one card from the other. The `preset`
 * value matches what ChartCard's initialSnapDoneRef effect will
 * settle into once real data arrives (5y). */
const INITIAL_SNAPSHOT: FilterSnapshot = {
  frequency: 'quarterly',
  mode: 'values',
  preset: '5y',
}

/** Apply a single FilterChange to a snapshot, returning a new copy.
 * Centralizes the kind→key mapping so the snapshot type stays the
 * single source of truth for "what's mirrored". */
function applyChangeToSnapshot(
  snapshot: FilterSnapshot,
  change: FilterChange,
): FilterSnapshot {
  if (change.kind === 'frequency') return { ...snapshot, frequency: change.value }
  if (change.kind === 'mode')      return { ...snapshot, mode: change.value }
  return { ...snapshot, preset: change.value }
}

type SlotId = 'left' | 'right'

export default function DashboardPage() {
  // Imperative handles for both cards — the parent calls
  // applyFilterChange on the OTHER card when the user clicks an
  // apply-pill on this card.
  const leftRef = useRef<ChartCardHandle>(null)
  const rightRef = useRef<ChartCardHandle>(null)

  // Per-card filter snapshots. Each card receives the OTHER card's
  // snapshot as a prop, used to decide whether the user's change
  // diverges and warrants showing a pill.
  const [snapshots, setSnapshots] = useState<Record<SlotId, FilterSnapshot>>({
    left: INITIAL_SNAPSHOT,
    right: INITIAL_SNAPSHOT,
  })

  const handleUserChange = useCallback(
    (slot: SlotId) => (change: FilterChange) => {
      setSnapshots((prev) => ({ ...prev, [slot]: applyChangeToSnapshot(prev[slot], change) }))
    },
    [],
  )

  const handleMirrorRequest = useCallback(
    (fromSlot: SlotId) => (change: FilterChange) => {
      const targetSlot: SlotId = fromSlot === 'left' ? 'right' : 'left'
      const targetRef = targetSlot === 'left' ? leftRef : rightRef
      targetRef.current?.applyFilterChange(change)
      setSnapshots((prev) => ({
        ...prev,
        [targetSlot]: applyChangeToSnapshot(prev[targetSlot], change),
      }))
    },
    [],
  )

  return (
    <>
      <PageTitle />

      <main id="main-content" tabIndex={-1} className="page">
        <section className="kpi-strip" aria-label="מדדי מפתח">
          {/* RTL flow: first child = rightmost. Order matches narrative. */}
          <StartsKpi />
          <InventoryKpi />
          <TotalSalesKpi />
          <HousingPriceKpi />
          <MortgageRateKpi />
        </section>

        <section className="chart-strip" aria-label="גרפים">
          {/* RTL flow: first child = rightmost. Right card holds
           * prices/rates/sales; left card holds construction
           * (starts, completions, inventory). */}
          <ChartCard
            ref={rightRef}
            slotId="right"
            defaultSeriesNames={[]}
            initialSpecs={RIGHT_CARD_SPECS}
            otherSnapshot={snapshots.left}
            onUserFilterChange={handleUserChange('right')}
            onMirrorRequest={handleMirrorRequest('right')}
          />
          <ChartCard
            ref={leftRef}
            slotId="left"
            defaultSeriesNames={[]}
            initialSpecs={LEFT_CARD_SPECS}
            otherSnapshot={snapshots.right}
            onUserFilterChange={handleUserChange('left')}
            onMirrorRequest={handleMirrorRequest('left')}
          />
        </section>
      </main>
    </>
  )
}
