/**
 * useDataFreshness — returns the most-recent created_at timestamp
 * across all four data tables, formatted as a Hebrew month + 4-digit
 * year (e.g., "אפר׳ 2026"). Used in the page subtitle to indicate
 * "last updated".
 *
 * Implementation: parallel REST calls (one per table) for the latest
 * created_at; client picks the maximum. This is intentionally robust
 * to a single failed table — we want a freshness signal even if one
 * ETL hasn't run yet today.
 */

import { useEffect, useState } from 'react'

import { supabase } from '../lib/supabase'

const TABLES = [
  'boi_base_rate',
  'boi_mortgage_rates',
  'cbs_price_indices',
  'cbs_series',
] as const

/** Hebrew month abbreviations, January=0..December=11.
 * Geresh (U+05F3) used per Hebrew typography convention. */
const HEBREW_MONTHS_ABBR = [
  'ינו׳', 'פבר׳', 'מרץ', 'אפר׳', 'מאי', 'יוני',
  'יולי', 'אוג׳', 'ספט׳', 'אוק׳', 'נוב׳', 'דצמ׳',
] as const

export function formatHebrewMonthYear(iso: string): string {
  const d = new Date(iso)
  if (isNaN(d.getTime())) return ''
  return `${HEBREW_MONTHS_ABBR[d.getUTCMonth()]} ${d.getUTCFullYear()}`
}

export interface DataFreshnessState {
  loading: boolean
  /** Pre-formatted Hebrew month + year, e.g., "אפר׳ 2026". '' until loaded. */
  label: string
  /** Raw ISO timestamp for callers that want a different format. */
  latestCreatedAt: string | null
}

export function useDataFreshness(): DataFreshnessState {
  const [state, setState] = useState<DataFreshnessState>({
    loading: true,
    label: '',
    latestCreatedAt: null,
  })

  useEffect(() => {
    let cancelled = false
    Promise.all(
      TABLES.map((t) =>
        supabase
          .from(t)
          .select('created_at')
          .order('created_at', { ascending: false })
          .limit(1),
      ),
    ).then((results) => {
      if (cancelled) return
      const dates: string[] = []
      for (const { data, error } of results) {
        if (error || !data || data.length === 0) continue
        const ts = (data[0] as { created_at?: string | null }).created_at
        if (typeof ts === 'string') dates.push(ts)
      }
      if (dates.length === 0) {
        setState({ loading: false, label: '', latestCreatedAt: null })
        return
      }
      dates.sort()
      const latest = dates[dates.length - 1]
      setState({
        loading: false,
        label: formatHebrewMonthYear(latest),
        latestCreatedAt: latest,
      })
    })
    return () => {
      cancelled = true
    }
  }, [])

  return state
}
