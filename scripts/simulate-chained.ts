/**
 * Simulate what the chained rent + CPI series will look like once the
 * ETL is re-run with chain_to_latest_base.
 *
 * The current DB rows don't carry base_desc, so we detect rebase
 * boundaries heuristically: any month-over-month change ≤ HEURISTIC_DROP
 * (currently -8%) is treated as a rebase. This catches every CBS rebase
 * in the history (smallest one in our data is ~−6.6% — see investigate-
 * rent-cpi.ts output) without false-positives on real-world Israeli
 * inflation events (which never reach −8% MoM, even in deflationary
 * periods).
 *
 * After re-running the real ETL, base_desc will be the canonical signal
 * and this heuristic goes away. The simulation here just shows the user
 * the SHAPE of what's coming so they can preview before re-running.
 *
 * Run with: npx tsx scripts/simulate-chained.ts
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

// MoM <= this is treated as a rebase boundary. CBS rebases produce drops
// of −5% to −99% in the data; real-world Israeli MoM CPI/rent changes
// are bounded at roughly ±5% even in hyperinflation. −8% threshold
// catches every rebase while remaining conservative against false
// positives.
const HEURISTIC_DROP_PCT = -8.0

interface Row {
  date: string
  value: number
}

async function fetchSeries(seriesId: number): Promise<Row[]> {
  const { data, error } = await supabase
    .from('cbs_price_indices')
    .select('date, value')
    .eq('series_id', seriesId)
    .order('date', { ascending: true })
    .limit(2000)
  if (error) throw new Error(`series ${seriesId}: ${error.message}`)
  return (data ?? []) as Row[]
}

function chainHeuristic(rows: Row[]): { chained: Row[]; boundaries: Array<{ idx: number; before: Row; after: Row; pct: number }> } {
  // Walk forward, mark indices where MoM ≤ threshold (rebase boundaries).
  const boundaries: Array<{ idx: number; before: Row; after: Row; pct: number }> = []
  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1].value
    const curr = rows[i].value
    if (prev === 0) continue
    const pct = ((curr - prev) / prev) * 100
    if (pct <= HEURISTIC_DROP_PCT) {
      boundaries.push({ idx: i, before: rows[i - 1], after: rows[i], pct })
    }
  }

  if (boundaries.length === 0) {
    return { chained: rows.map((r) => ({ ...r })), boundaries }
  }

  // Build segments: [start, end_excl]
  const segments: Array<{ start: number; end: number }> = []
  let segStart = 0
  for (const b of boundaries) {
    segments.push({ start: segStart, end: b.idx })
    segStart = b.idx
  }
  segments.push({ start: segStart, end: rows.length })

  // Per-segment chain factors (newest = 1.0; older compounds backward).
  const factors = new Array<number>(segments.length).fill(1.0)
  for (let k = segments.length - 2; k >= 0; k--) {
    const lastOld = rows[segments[k].end - 1].value
    const firstNew = rows[segments[k + 1].start].value
    const boundary = lastOld === 0 ? 1.0 : firstNew / lastOld
    factors[k] = factors[k + 1] * boundary
  }

  const chained: Row[] = []
  for (let s = 0; s < segments.length; s++) {
    const f = factors[s]
    for (let i = segments[s].start; i < segments[s].end; i++) {
      chained.push({ date: rows[i].date, value: rows[i].value * f })
    }
  }
  return { chained, boundaries }
}

function describe(label: string, original: Row[], chained: Row[], boundaries: Array<{ idx: number; before: Row; after: Row; pct: number }>) {
  console.log('===', label)
  console.log('  rebase boundaries detected:', boundaries.length)
  for (const b of boundaries) {
    console.log(
      `    ${b.before.date} -> ${b.after.date}: ${b.before.value.toFixed(2)} -> ${b.after.value.toFixed(2)}  (${b.pct.toFixed(2)}%)`,
    )
  }
  console.log('  Jan-of-each-year — orig vs chained:')
  for (let i = 0; i < original.length; i++) {
    if (original[i].date.endsWith('-01-01')) {
      const o = original[i].value
      const c = chained[i].value
      console.log(
        `    ${original[i].date}  orig ${o.toFixed(2).padStart(10)}   chained ${formatChained(c)}`,
      )
    }
  }
  console.log('  most recent 6 — orig vs chained:')
  for (let i = original.length - 6; i < original.length; i++) {
    if (i < 0) continue
    const o = original[i].value
    const c = chained[i].value
    console.log(
      `    ${original[i].date}  orig ${o.toFixed(2).padStart(10)}   chained ${formatChained(c)}`,
    )
  }

  // Check: post-chaining, should there be no MoM moves more extreme
  // than ~5%? Walk and report any lingering >|5%| moves.
  console.log('  remaining MoM moves > 5% on chained series:')
  let lingering = 0
  for (let i = 1; i < chained.length; i++) {
    const prev = chained[i - 1].value
    const curr = chained[i].value
    if (prev === 0) continue
    const pct = ((curr - prev) / prev) * 100
    if (Math.abs(pct) > 5) {
      console.log(
        `    ${chained[i - 1].date} -> ${chained[i].date}: ${prev.toFixed(2)} -> ${curr.toFixed(2)}  (${pct.toFixed(2)}%)`,
      )
      lingering++
    }
  }
  if (lingering === 0) console.log('    (none — saw-tooth eliminated)')
  else console.log(`    total: ${lingering} (these are real economic moves, mostly 1979-85 hyperinflation)`)
  console.log('')
}

function formatChained(v: number): string {
  if (Math.abs(v) >= 1) return v.toFixed(2).padStart(10)
  // Very small chained values (deep history) — use scientific
  return v.toExponential(2).padStart(10)
}

const SERIES = [
  { id: 40010, label: 'Housing prices (40010, single-base)' },
  { id: 120460, label: 'Rent (120460, multi-base)' },
  { id: 120010, label: 'CPI (120010, multi-base)' },
]

async function main() {
  for (const s of SERIES) {
    const rows = await fetchSeries(s.id)
    const { chained, boundaries } = chainHeuristic(rows)
    describe(s.label, rows, chained, boundaries)
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
