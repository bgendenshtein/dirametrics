/**
 * Sparkline — tiny inline line chart, no axes, no chrome.
 *
 * Bespoke SVG implementation (~50 lines + path-building helper) so the
 * KPI strip doesn't pull Recharts (~620 KB minified) into the main
 * bundle. Visually parity-matches Recharts' `<Line type="monotone">`
 * via Fritsch-Carlson monotone cubic interpolation — the same
 * algorithm d3-shape's curveMonotoneX uses, which Recharts wraps.
 *
 * Renders width-responsive via ResizeObserver. Values are y-normalized
 * to fit the SVG height with `padding` of breathing room top/bottom
 * so the stroke doesn't clip at peaks/valleys.
 *
 * Provisional tail: the last TAIL_LENGTH points render as a dashed
 * continuation of the solid main line. This mirrors the chart-engine
 * convention (Chart.tsx) — CBS marks the 3 most recent monthly readings
 * provisional, and the dashed treatment communicates that visually.
 * The path is split into two `<path>` elements that share the split
 * point so the dashed tail visually picks up where the solid line
 * ends, without a gap.
 */

import { useEffect, useRef, useState } from 'react'

const TAIL_LENGTH = 3

export interface SparklinePoint {
  date: string
  v: number
}

export interface SparklineProps {
  values: SparklinePoint[]
  stroke: string
  strokeWidth?: number
  height?: number
  /** When set, renders an accessible chart with this label; otherwise
   * the SVG is decorative-only. */
  ariaLabel?: string
}

export function Sparkline({
  values,
  stroke,
  strokeWidth = 1.5,
  height = 52,
  ariaLabel,
}: SparklineProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(0)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    setWidth(el.getBoundingClientRect().width)
    const obs = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width ?? 0
      setWidth(w)
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [])

  const tooFew = values.length < 2 || width === 0
  const paths = tooFew ? null : buildMonotonePaths(values, width, height, strokeWidth)

  return (
    <div ref={containerRef} className="kpi-spark" style={{ height }}>
      {paths && (
        <svg
          width={width}
          height={height}
          role={ariaLabel ? 'img' : 'presentation'}
          aria-label={ariaLabel}
          aria-hidden={ariaLabel ? undefined : true}
        >
          {paths.solid && (
            <path
              d={paths.solid}
              fill="none"
              stroke={stroke}
              strokeWidth={strokeWidth}
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          )}
          {paths.tail && (
            <path
              d={paths.tail}
              fill="none"
              stroke={stroke}
              strokeWidth={strokeWidth}
              strokeLinecap="round"
              strokeLinejoin="round"
              /* Dash pattern matches Chart.tsx's tail dash for visual
               * consistency between the KPI sparklines and the main
               * chart lines. */
              strokeDasharray="4 3"
            />
          )}
        </svg>
      )}
    </div>
  )
}

/** Monotone cubic spline via Fritsch-Carlson with weighted-harmonic
 * tangent estimation. Matches d3-shape's curveMonotoneX (which Recharts
 * uses for type="monotone"), so swapping this in for the existing
 * Recharts sparkline is visually a no-op for monthly KPI data.
 *
 * Returns two subpaths: `solid` covers points [0..splitIdx] (i.e.,
 * everything except the last TAIL_LENGTH-1 segments), and `tail`
 * covers [splitIdx..n-1] (the provisional tail). The two share the
 * split point so the dashed tail visually continues from the solid
 * line. When the input is too short to split (n ≤ TAIL_LENGTH), the
 * full series renders as solid and tail returns empty. */
function buildMonotonePaths(
  values: SparklinePoint[],
  width: number,
  height: number,
  pad: number,
): { solid: string; tail: string } {
  const n = values.length

  let yMin = Infinity
  let yMax = -Infinity
  for (const p of values) {
    if (p.v < yMin) yMin = p.v
    if (p.v > yMax) yMax = p.v
  }
  const yRange = yMax - yMin || 1
  const yPad = pad
  const usableH = Math.max(0, height - 2 * yPad)

  // Coordinates over the FULL array (uniform x spacing). Tail and
  // solid subpaths both index into these — so the tail's geometry
  // remains anchored to the full timeline rather than re-stretching
  // to fill the SVG's horizontal extent.
  const xs = new Array<number>(n)
  const ys = new Array<number>(n)
  for (let i = 0; i < n; i++) {
    xs[i] = (i / (n - 1)) * width
    ys[i] = height - yPad - ((values[i].v - yMin) / yRange) * usableH
  }

  const slopes = new Array<number>(n - 1)
  const dxs = new Array<number>(n - 1)
  for (let i = 0; i < n - 1; i++) {
    dxs[i] = xs[i + 1] - xs[i]
    slopes[i] = (ys[i + 1] - ys[i]) / (dxs[i] || 1)
  }

  const tangents = new Array<number>(n)
  tangents[0] = slopes[0]
  tangents[n - 1] = slopes[n - 2]
  for (let i = 1; i < n - 1; i++) {
    const sPrev = slopes[i - 1]
    const sNext = slopes[i]
    if (sPrev * sNext <= 0) {
      tangents[i] = 0
    } else {
      const dxPrev = dxs[i - 1]
      const dxNext = dxs[i]
      const w1 = 2 * dxNext + dxPrev
      const w2 = dxNext + 2 * dxPrev
      tangents[i] = (w1 + w2) / (w1 / sPrev + w2 / sNext)
    }
  }

  // Build a path by segment range [from, to]: emits M at xs[from],
  // then C-segments through xs[to]. Empty range → empty string.
  const buildSegment = (from: number, to: number): string => {
    if (to <= from) return ''
    let d = `M${xs[from].toFixed(1)},${ys[from].toFixed(1)}`
    for (let i = from; i < to; i++) {
      const dx = dxs[i]
      const cp1x = xs[i] + dx / 3
      const cp1y = ys[i] + (tangents[i] * dx) / 3
      const cp2x = xs[i + 1] - dx / 3
      const cp2y = ys[i + 1] - (tangents[i + 1] * dx) / 3
      d +=
        ` C${cp1x.toFixed(1)},${cp1y.toFixed(1)}` +
        ` ${cp2x.toFixed(1)},${cp2y.toFixed(1)}` +
        ` ${xs[i + 1].toFixed(1)},${ys[i + 1].toFixed(1)}`
    }
    return d
  }

  // Need at least TAIL_LENGTH + 1 points for a meaningful split (so
  // the solid segment has at least one non-shared point). Otherwise
  // render the whole thing solid; visually softer to err on the side
  // of "no tail" than to dash the entire sparkline.
  if (n <= TAIL_LENGTH) {
    return { solid: buildSegment(0, n - 1), tail: '' }
  }
  const splitIdx = n - TAIL_LENGTH
  return {
    solid: buildSegment(0, splitIdx),
    tail: buildSegment(splitIdx, n - 1),
  }
}
