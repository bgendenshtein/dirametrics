/**
 * useMortgageRate — fetches BoI fixed unindexed mortgage rate
 * (rate_type='fixed', is_indexed=false) and returns:
 *   level: latest rate (e.g., 4.74)
 *   delta: YoY change in PERCENTAGE POINTS (e.g., -0.18 pp)
 *   sparkValues: the rate series itself, last ~24 months
 *
 * The delta unit is "נק'" (n'kudot, percentage points), NOT %, because
 * the metric is itself a percentage and a relative-% change would be
 * ambiguous (4.92% → 4.74% is -0.18 pp, but -3.66% relative).
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'
import type { KpiState } from './kpi'
import { directionFor, initialKpiState, isoMonthsAgo } from './kpi'

const SPARK_MONTHS = 24
const FETCH_MONTHS = SPARK_MONTHS + 1

interface RawRow {
  date: string
  rate: number
}

export function useMortgageRate(): KpiState {
  const [state, setState] = useState<KpiState>(initialKpiState)

  useEffect(() => {
    let cancelled = false
    const since = isoMonthsAgo(FETCH_MONTHS)

    supabase
      .from('boi_mortgage_rates')
      .select('date, rate')
      .eq('rate_type', 'fixed')
      .eq('is_indexed', false)
      .gte('date', since)
      .order('date', { ascending: true })
      .limit(500)
      .then(({ data, error }) => {
        if (cancelled) return
        if (error) {
          setState({ ...initialKpiState, loading: false, error: error.message })
          return
        }
        const rows = (data ?? []) as RawRow[]
        if (rows.length < 13) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב שינוי שנתי',
          })
          return
        }
        const latest = rows[rows.length - 1]
        // 12 months prior — match by year-month rather than blind index
        const [latestYear, latestMonth] = latest.date.slice(0, 7).split('-')
        const priorKey = `${Number(latestYear) - 1}-${latestMonth}`
        const prior = rows.find((r) => r.date.slice(0, 7) === priorKey)
        if (!prior) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'נתון לפני שנה לא זמין',
          })
          return
        }
        const delta = latest.rate - prior.rate // percentage points
        const sparkValues = rows
          .slice(-SPARK_MONTHS)
          .map((r) => ({ date: r.date, v: r.rate }))
        setState({
          loading: false,
          error: null,
          level: latest.rate,
          delta,
          direction: directionFor(delta, 0.005),
          sparkValues,
          asOf: latest.date,
          isProvisional: false, // BoI rate isn't flagged provisional
        })
      })

    return () => {
      cancelled = true
    }
  }, [])

  return state
}
