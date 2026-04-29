/**
 * useTotalSalesYoY — total apartment sales = new (free + subsidized,
 * already aggregated into 'new_sales_total') + 'second_hand_sales',
 * national, monthly.
 *
 * Hero level: latest 3-month average of (new_total + second_hand) sums.
 * Delta: % change vs the same 3 months prior year.
 * Sparkline: monthly combined-sales series, last ~24 months.
 *
 * Query approach: a single Supabase request with `topic.in.(...)`,
 * grouped client-side by time_period and summed (only periods where
 * BOTH topics have a row are included — guards against half-counted
 * months while either side's ETL is mid-run).
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'
import type { KpiSparkPoint, KpiState } from './kpi'
import { directionFor, initialKpiState, isoMonthsAgo } from './kpi'

const SPARK_MONTHS = 24
/** 3 most-recent months + same 3 months prior year = ≥15 months span. Plus
 * sparkline depth. Round generously to absorb any edge gaps. */
const FETCH_MONTHS = SPARK_MONTHS + 18

interface RawRow {
  time_period: string
  topic: string
  value: number
  is_provisional: boolean | null
}

interface CombinedRow {
  date: string
  v: number
  isProvisional: boolean
}

/** Sum new_sales_total + second_hand_sales per month. Only months where
 * BOTH topics have a row are returned. Result is ascending by date. */
function combineSales(rows: RawRow[]): CombinedRow[] {
  const byDate = new Map<string, { total?: RawRow; secondhand?: RawRow }>()
  for (const r of rows) {
    const slot = byDate.get(r.time_period) ?? {}
    if (r.topic === 'new_sales_total') slot.total = r
    if (r.topic === 'second_hand_sales') slot.secondhand = r
    byDate.set(r.time_period, slot)
  }
  const out: CombinedRow[] = []
  for (const [date, slot] of byDate) {
    if (!slot.total || !slot.secondhand) continue
    out.push({
      date,
      v: slot.total.value + slot.secondhand.value,
      isProvisional:
        Boolean(slot.total.is_provisional) || Boolean(slot.secondhand.is_provisional),
    })
  }
  out.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0))
  return out
}

function avg(values: number[]): number {
  return values.reduce((s, v) => s + v, 0) / values.length
}

/** Shift 'YYYY-MM-DD' back by n full years. */
function shiftYearsBack(isoDate: string, n: number): string {
  const [y, m, d] = isoDate.split('-')
  return `${String(Number(y) - n).padStart(4, '0')}-${m}-${d}`
}

export function useTotalSalesYoY(): KpiState {
  const [state, setState] = useState<KpiState>(initialKpiState)

  useEffect(() => {
    let cancelled = false
    const since = isoMonthsAgo(FETCH_MONTHS)

    supabase
      .from('cbs_series')
      .select('time_period, topic, value, is_provisional')
      .in('topic', ['new_sales_total', 'second_hand_sales'])
      .eq('district', 'national')
      .eq('frequency', 'monthly')
      .gte('time_period', since)
      .order('time_period', { ascending: true })
      .limit(2000)
      .then(({ data, error }) => {
        if (cancelled) return
        if (error) {
          setState({ ...initialKpiState, loading: false, error: error.message })
          return
        }
        const rows = (data ?? []) as RawRow[]
        const combined = combineSales(rows)
        if (combined.length < 15) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב מכירות (חסר חודש בצד אחד)',
          })
          return
        }
        // 3 most-recent combined months
        const recentSlice = combined.slice(-3)
        const recentAvg = avg(recentSlice.map((r) => r.v))
        const recentDates = recentSlice.map((r) => r.date)
        // Same 3 months prior year
        const priorDates = recentDates.map((d) => shiftYearsBack(d, 1))
        const priorRows = combined.filter((r) => priorDates.includes(r.date))
        if (priorRows.length < 3) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין נתון תואם לפני שנה',
          })
          return
        }
        const priorAvg = avg(priorRows.map((r) => r.v))
        const delta = priorAvg === 0 ? 0 : (recentAvg / priorAvg - 1) * 100
        const sparkValues: KpiSparkPoint[] = combined
          .slice(-SPARK_MONTHS)
          .map((r) => ({ date: r.date, v: r.v }))
        setState({
          loading: false,
          error: null,
          level: recentAvg,
          delta,
          direction: directionFor(delta),
          sparkValues,
          asOf: recentSlice[recentSlice.length - 1].date,
          isProvisional: recentSlice.some((r) => r.isProvisional),
        })
      })

    return () => {
      cancelled = true
    }
  }, [])

  return state
}
