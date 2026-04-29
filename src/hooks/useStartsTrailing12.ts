/**
 * useStartsTrailing12 — construction starts (national, monthly).
 *
 * Computes a rolling 12-month sum at each month-end. The hero level is
 * the latest 12-month sum; the delta is the % change vs the 12-month
 * sum from the prior period (i.e., months 1..12 vs months 13..24
 * counting back). Sparkline is the rolling-12 series.
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'
import type { KpiSparkPoint, KpiState } from './kpi'
import { directionFor, initialKpiState, isoMonthsAgo } from './kpi'

const SPARK_MONTHS = 24
/** 24 rolling-12 points need 24 + 11 = 35 monthly rows. Plus 12 for the
 * prior-window comparison anchor that's used only at the latest point.
 * Round up generously. */
const FETCH_MONTHS = SPARK_MONTHS + 24

interface RawRow {
  time_period: string
  value: number
  is_provisional: boolean | null
}

/** Rolling 12-month sums. `rows` ordered ascending by month; output is
 * ordered ascending too, with each entry's `v` = sum of the trailing
 * 12 months ending at (and including) that entry's month. The first
 * 11 input rows can't be summed and are skipped. */
function rolling12(rows: RawRow[]): KpiSparkPoint[] {
  const out: KpiSparkPoint[] = []
  for (let i = 11; i < rows.length; i++) {
    let sum = 0
    for (let j = i - 11; j <= i; j++) sum += rows[j].value
    out.push({ date: rows[i].time_period, v: sum })
  }
  return out
}

export function useStartsTrailing12(): KpiState {
  const [state, setState] = useState<KpiState>(initialKpiState)

  useEffect(() => {
    let cancelled = false
    const since = isoMonthsAgo(FETCH_MONTHS)

    supabase
      .from('cbs_series')
      .select('time_period, value, is_provisional')
      .eq('topic', 'starts')
      .eq('district', 'national')
      .eq('frequency', 'monthly')
      .gte('time_period', since)
      .order('time_period', { ascending: true })
      .limit(500)
      .then(({ data, error }) => {
        if (cancelled) return
        if (error) {
          setState({ ...initialKpiState, loading: false, error: error.message })
          return
        }
        const rows = (data ?? []) as RawRow[]
        if (rows.length < 24) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב שינוי 12 חודשים',
          })
          return
        }
        const series = rolling12(rows)
        if (series.length < 13) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב שינוי 12 חודשים',
          })
          return
        }
        const latest = series[series.length - 1]
        const prior = series[series.length - 13] // 12 months earlier rolling-12 sum
        const delta = prior.v === 0 ? 0 : (latest.v / prior.v - 1) * 100
        const sparkValues = series.slice(-SPARK_MONTHS)
        const latestRaw = rows[rows.length - 1]
        setState({
          loading: false,
          error: null,
          level: latest.v,
          delta,
          direction: directionFor(delta),
          sparkValues,
          asOf: latest.date,
          isProvisional: Boolean(latestRaw?.is_provisional),
        })
      })

    return () => {
      cancelled = true
    }
  }, [])

  return state
}
