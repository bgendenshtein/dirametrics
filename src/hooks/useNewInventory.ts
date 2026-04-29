/**
 * useNewInventory — new-apartment unsold inventory (national).
 *
 * Per the data catalog and our ETL notes, this topic is national-only
 * (no district breakdown is available via the CBS time-series API).
 *
 * Hero level: latest 3-month average inventory.
 * Delta: % change vs the previous 3-month average (months 4..6 prior).
 * Sparkline: monthly inventory level series, last ~24 months.
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'
import type { KpiState } from './kpi'
import { directionFor, initialKpiState, isoMonthsAgo } from './kpi'

const SPARK_MONTHS = 24
const FETCH_MONTHS = SPARK_MONTHS + 6 // need at least 6 months for the comparison

interface RawRow {
  time_period: string
  value: number
  is_provisional: boolean | null
}

function avg(values: number[]): number {
  return values.reduce((s, v) => s + v, 0) / values.length
}

export function useNewInventory(): KpiState {
  const [state, setState] = useState<KpiState>(initialKpiState)

  useEffect(() => {
    let cancelled = false
    const since = isoMonthsAgo(FETCH_MONTHS)

    supabase
      .from('cbs_series')
      .select('time_period, value, is_provisional')
      .eq('topic', 'new_inventory')
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
        if (rows.length < 6) {
          setState({
            ...initialKpiState,
            loading: false,
            error: 'אין מספיק נתונים לחישוב מלאי',
          })
          return
        }
        // Use the last 6 months for the two windows
        const last6 = rows.slice(-6)
        const recent = avg(last6.slice(3, 6).map((r) => r.value))
        const prior = avg(last6.slice(0, 3).map((r) => r.value))
        const delta = prior === 0 ? 0 : (recent / prior - 1) * 100
        const sparkValues = rows
          .slice(-SPARK_MONTHS)
          .map((r) => ({ date: r.time_period, v: r.value }))
        const latestRaw = rows[rows.length - 1]
        setState({
          loading: false,
          error: null,
          level: recent,
          delta,
          direction: directionFor(delta),
          sparkValues,
          asOf: latestRaw.time_period,
          isProvisional: Boolean(latestRaw.is_provisional),
        })
      })

    return () => {
      cancelled = true
    }
  }, [])

  return state
}
