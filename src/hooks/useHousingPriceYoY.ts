/**
 * useHousingPriceYoY — fetches CBS housing-price index (series 40010)
 * and returns the year-over-year percent change as a delta plus a
 * sparkline of YoY % over the last ~24 months.
 *
 * Delta-as-hero KPI: there is no level field, only a delta. The hero on
 * the card is the YoY %, with the sparkline showing the YoY trajectory
 * (so the rightmost sparkline point matches the hero number).
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'
import type { Direction, KpiSparkPoint, KpiState } from './kpi'
import { directionFor, initialKpiState, isoMonthsAgo } from './kpi'

const SERIES_ID = 40010
const SPARK_MONTHS = 24
const FETCH_MONTHS = SPARK_MONTHS + 14 // 24 YoY points need ≥36 monthly rows

interface RawRow {
  date: string
  value: number
  is_provisional: boolean | null
}

function computeYoY(rows: RawRow[]): KpiSparkPoint[] {
  if (rows.length < 13) return []
  const byMonth = new Map<string, RawRow>()
  for (const r of rows) byMonth.set(r.date.slice(0, 7), r)

  const out: KpiSparkPoint[] = []
  for (const r of rows) {
    const ym = r.date.slice(0, 7)
    const [y, m] = ym.split('-').map(Number)
    const priorYm = `${String(y - 1).padStart(4, '0')}-${String(m).padStart(2, '0')}`
    const prior = byMonth.get(priorYm)
    if (!prior || prior.value === 0) continue
    out.push({ date: r.date, v: (r.value / prior.value - 1) * 100 })
  }
  return out
}

export function useHousingPriceYoY(): KpiState {
  const [state, setState] = useState<KpiState>(initialKpiState)

  useEffect(() => {
    let cancelled = false
    const since = isoMonthsAgo(FETCH_MONTHS)

    supabase
      .from('cbs_price_indices')
      .select('date, value, is_provisional')
      .eq('series_id', SERIES_ID)
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
        const yoy = computeYoY(rows)
        if (yoy.length === 0) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב שינוי שנתי',
          })
          return
        }
        const sparkValues = yoy.slice(-SPARK_MONTHS)
        const latest = yoy[yoy.length - 1]
        const latestRaw = rows[rows.length - 1]
        const direction: Direction = directionFor(latest.v)
        setState({
          loading: false,
          error: null,
          level: null, // delta-as-hero: no level
          delta: latest.v,
          direction,
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
