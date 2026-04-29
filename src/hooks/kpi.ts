/**
 * Shared types/helpers for KPI data hooks.
 *
 * Each hook returns a uniform `KpiState`; the corresponding KpiCard
 * adapter in App.tsx maps it onto KpiCardProps with its own labels and
 * unit choices. This keeps the hooks data-domain only — they don't
 * decide how things should be displayed, just what the numbers are.
 */

export type Direction = 'up' | 'down' | 'flat'

export interface KpiSparkPoint {
  date: string // 'YYYY-MM-01' or 'YYYY-MM' for quarterly
  v: number
}

export interface KpiState {
  loading: boolean
  error: string | null
  /** Current level (e.g., a rate, a 12-month total, a 3-mo avg). Null for delta-only KPIs. */
  level: number | null
  /** Change vs reference period. The unit is the hook's responsibility to know
   * but the KpiCard adapter in App.tsx labels it. */
  delta: number | null
  direction: Direction | null
  /** Oldest-first; length up to ~24 months. */
  sparkValues: KpiSparkPoint[]
  /** ISO date of the most-recent observation. */
  asOf: string | null
  /** True if the most-recent observation is flagged provisional in the source data. */
  isProvisional: boolean
}

export const initialKpiState: KpiState = {
  loading: true,
  error: null,
  level: null,
  delta: null,
  direction: null,
  sparkValues: [],
  asOf: null,
  isProvisional: false,
}

/** Direction from a delta value, with a small dead-zone for "flat". */
export function directionFor(value: number, deadZone = 0.05): Direction {
  if (value > deadZone) return 'up'
  if (value < -deadZone) return 'down'
  return 'flat'
}

/** Subtract n months from today, return 'YYYY-MM-DD'. */
export function isoMonthsAgo(months: number): string {
  const d = new Date()
  d.setUTCDate(1)
  d.setUTCMonth(d.getUTCMonth() - months)
  return d.toISOString().slice(0, 10)
}
