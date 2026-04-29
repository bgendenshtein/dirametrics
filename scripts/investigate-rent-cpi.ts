/**
 * One-off investigation: rent index (120460) and CPI (120010).
 *
 * Pulls the actual rows from cbs_price_indices and prints:
 *   - row count, MIN/MAX dates per series
 *   - sample timeline (one row per January)
 *   - month-over-month changes that exceed a "suspicious" threshold
 *   - cross-check anchor points at known dates (Jan 2020, Jan 2024)
 *
 * Run with: npx tsx scripts/investigate-rent-cpi.ts
 *
 * Reads VITE_SUPABASE_URL + VITE_SUPABASE_ANON_KEY from .env at the
 * project root (the same anon-key path the frontend uses; the tables
 * are public-read so anon is sufficient).
 */

import { config } from 'dotenv'
import { createClient } from '@supabase/supabase-js'

config()

const url = process.env.VITE_SUPABASE_URL
const key = process.env.VITE_SUPABASE_ANON_KEY
if (!url || !key) {
  console.error('Missing VITE_SUPABASE_URL / VITE_SUPABASE_ANON_KEY in .env')
  process.exit(1)
}

const supabase = createClient(url, key)

interface Row {
  series_id: number
  series_name: string | null
  date: string
  value: number
  is_provisional: boolean | null
}

const SERIES = [
  { id: 40010,  label: 'Housing prices (40010, anchor)' },
  { id: 120460, label: 'Rent (120460)' },
  { id: 120010, label: 'CPI (120010)' },
]

const SUSPICIOUS_MOM = 3.0 // percent — flag MoM moves bigger than this

async function fetchSeries(seriesId: number): Promise<Row[]> {
  const { data, error } = await supabase
    .from('cbs_price_indices')
    .select('series_id, series_name, date, value, is_provisional')
    .eq('series_id', seriesId)
    .order('date', { ascending: true })
    .limit(2000)
  if (error) throw new Error(`series ${seriesId}: ${error.message}`)
  return (data ?? []) as Row[]
}

function fmt(n: number, dp = 2): string {
  return n.toFixed(dp)
}

function describeShape(rows: Row[]) {
  if (rows.length === 0) {
    console.log('  (no rows)')
    return
  }
  console.log(`  rows: ${rows.length}`)
  console.log(`  date range: ${rows[0].date} .. ${rows[rows.length - 1].date}`)
  console.log(`  series_name: ${rows[0].series_name ?? '(null)'}`)

  // Distinct values count — if a series's `value` only takes 2-3 distinct
  // values, that's a strong indicator something's wrong.
  const distinctValues = new Set(rows.map((r) => r.value)).size
  console.log(`  distinct values: ${distinctValues}`)

  // Sample one row per January (so the printout shows a coarse timeline).
  console.log('  yearly snapshot (Jan of each year, value):')
  for (const r of rows) {
    if (r.date.endsWith('-01-01')) {
      console.log(
        `    ${r.date}  ${fmt(r.value)}${r.is_provisional ? ' (provisional)' : ''}`,
      )
    }
  }

  // Recent 6 months (regardless of month).
  console.log('  most recent 6 rows:')
  for (const r of rows.slice(-6)) {
    console.log(
      `    ${r.date}  ${fmt(r.value)}${r.is_provisional ? ' (provisional)' : ''}`,
    )
  }

  // Month-over-month percent changes; flag big jumps.
  console.log(`  MoM moves > ${SUSPICIOUS_MOM}% (absolute):`)
  let flagged = 0
  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1].value
    const curr = rows[i].value
    if (prev === 0) continue
    const pct = ((curr - prev) / prev) * 100
    if (Math.abs(pct) >= SUSPICIOUS_MOM) {
      console.log(
        `    ${rows[i - 1].date} → ${rows[i].date}: ${fmt(prev)} → ${fmt(curr)}` +
          `  (${pct >= 0 ? '+' : ''}${fmt(pct)}%)`,
      )
      flagged++
    }
  }
  if (flagged === 0) console.log('    (none)')
  else console.log(`    total: ${flagged}`)

  // Anchor dates the user mentioned: Jan 2020 + Jan 2024.
  const anchors = ['2020-01-01', '2024-01-01']
  console.log('  anchor values:')
  for (const a of anchors) {
    const row = rows.find((r) => r.date === a)
    console.log(`    ${a}: ${row ? fmt(row.value) : '(missing)'}`)
  }
}

async function main() {
  for (const s of SERIES) {
    console.log('===', s.label)
    try {
      const rows = await fetchSeries(s.id)
      describeShape(rows)
    } catch (e) {
      console.log('  ERROR:', (e as Error).message)
    }
    console.log('')
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
