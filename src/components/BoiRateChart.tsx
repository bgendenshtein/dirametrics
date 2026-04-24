import { useEffect, useState } from 'react'
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { TooltipContentProps } from 'recharts'
import { supabase } from '../lib/supabase'

type Row = { date: string; rate: number }

const YEARS_BACK = 5

function isoDaysAgo(years: number): string {
  const d = new Date()
  d.setFullYear(d.getFullYear() - years)
  return d.toISOString().slice(0, 10)
}

function yearTicks(rows: Row[]): string[] {
  const seen = new Set<string>()
  const ticks: string[] = []
  for (const r of rows) {
    const year = r.date.slice(0, 4)
    if (!seen.has(year)) {
      seen.add(year)
      ticks.push(r.date)
    }
  }
  return ticks
}

function formatDateLong(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleDateString('he-IL', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

function CustomTooltip({ active, payload }: TooltipContentProps) {
  if (!active || !payload || payload.length === 0) return null
  const row = payload[0].payload as Row
  return (
    <div
      style={{
        background: '#fff',
        border: '1px solid #ccc',
        padding: '6px 10px',
        fontSize: 13,
        direction: 'ltr',
      }}
    >
      <div>{formatDateLong(row.date)}</div>
      <div style={{ fontWeight: 600 }}>{row.rate.toFixed(2)}%</div>
    </div>
  )
}

export function BoiRateChart() {
  const [rows, setRows] = useState<Row[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const since = isoDaysAgo(YEARS_BACK)

    supabase
      .from('boi_base_rate')
      .select('date, rate')
      .gte('date', since)
      .order('date', { ascending: true })
      .limit(10000)
      .then(({ data, error }) => {
        if (cancelled) return
        if (error) {
          setError(error.message)
          return
        }
        setRows((data ?? []) as Row[])
      })

    return () => {
      cancelled = true
    }
  }, [])

  return (
    <section style={{ width: '100%', maxWidth: 1000, margin: '0 auto' }}>
      <h2 style={{ margin: '0 0 4px', textAlign: 'center' }}>ריבית בנק ישראל</h2>
      <p style={{ margin: '0 0 16px', textAlign: 'center', fontSize: 13, color: '#666' }}>
        מקור: בנק ישראל
      </p>

      {error && (
        <div style={{ color: '#b00020', textAlign: 'center', padding: 24 }}>
          Error loading data: {error}
        </div>
      )}

      {!error && rows === null && (
        <div style={{ textAlign: 'center', padding: 24 }}>Loading data...</div>
      )}

      {!error && rows !== null && rows.length === 0 && (
        <div style={{ textAlign: 'center', padding: 24 }}>No data available.</div>
      )}

      {!error && rows && rows.length > 0 && (
        <div style={{ width: '100%', height: 400 }}>
          <ResponsiveContainer>
            <LineChart data={rows} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
              <XAxis
                dataKey="date"
                ticks={yearTicks(rows)}
                tickFormatter={(v: string) => v.slice(0, 4)}
                minTickGap={20}
              />
              <YAxis
                tickFormatter={(v: number) => `${v.toFixed(2)}%`}
                width={60}
                domain={['auto', 'auto']}
              />
              <Tooltip content={CustomTooltip} />
              <Line
                type="stepAfter"
                dataKey="rate"
                stroke="#aa3bff"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  )
}
