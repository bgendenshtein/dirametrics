/**
 * BrushOverview — FRED-style range selector. Mini line chart of the
 * full data history with a draggable selection rectangle on top.
 *
 * Three drag modes:
 *   left handle   — resize from the past edge (changes start date)
 *   right handle  — resize from the recent edge (changes end date)
 *   selection body — pan (preserves duration, shifts in time)
 * Plus: click outside selection in the brush jumps the selection so
 * its center moves to the click point (preserving duration).
 *
 * Data assumptions:
 *   - Monthly cadence, contiguous (no gaps). Real data later will need
 *     gap handling; synthetic step-7 data is contiguous.
 *   - Time flows left-to-right inside the SVG regardless of the
 *     surrounding RTL layout. Finance time-series convention.
 *
 * Snap behavior: every pointer move quantizes to the nearest month via
 * Math.round on (dxPx / width × totalMonths). At ~700 px wide and 360
 * months of synthetic data, that's ~2 px per month — visually smooth
 * without sub-month flicker.
 */

import { useEffect, useRef, useState } from 'react'

import { addMonths, monthsBetween, type DateRange } from '../lib/dateRange'

export interface BrushDataPoint {
  date: Date
  value: number
}

export interface BrushOverviewProps {
  data: BrushDataPoint[]
  range: DateRange
  onRangeChange: (next: DateRange) => void
  /** Smallest allowed selection width, in months. Default 6. */
  minRangeMonths?: number
  /** SVG height in pixels. Default 44. */
  height?: number
}

type DragMode = 'left' | 'right' | 'body'

interface DragState {
  mode: DragMode
  startPxX: number
  startRange: DateRange
}

const HANDLE_WIDTH = 6
/** Touch-target width for the handle hit area. The visible handle
 * stays 6 px wide for the desktop look; an invisible wider rect
 * sits behind it (centered on the same x) so touch users get a
 * 24 px-wide hit zone rather than 6 px. WCAG 2.5.5 wants 44×44 —
 * we don't go that wide because it would extend visually beyond
 * the brush body and conflict with the click-to-jump track on
 * either side. 24 px is the pragmatic compromise. */
const HANDLE_TOUCH_WIDTH = 24

export function BrushOverview({
  data,
  range,
  onRangeChange,
  minRangeMonths = 6,
  height = 44,
}: BrushOverviewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)
  const [width, setWidth] = useState(0)
  const [drag, setDrag] = useState<DragState | null>(null)

  // Track container width via ResizeObserver so SVG math stays accurate
  // when the chart card resizes (window resize, theme toggle reflow, etc.).
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

  if (data.length < 2) {
    return <div ref={containerRef} className="brush" style={{ height }} />
  }

  const dataStart = data[0].date
  const dataEnd = data[data.length - 1].date
  const totalMonths = monthsBetween(dataStart, dataEnd) || 1

  const dateToX = (d: Date): number => {
    if (width <= 0) return 0
    const m = monthsBetween(dataStart, d)
    return Math.max(0, Math.min(width, (m / totalMonths) * width))
  }

  const pxToDate = (px: number): Date => {
    const fraction = Math.max(0, Math.min(1, px / Math.max(1, width)))
    const months = Math.round(fraction * totalMonths)
    return addMonths(dataStart, months)
  }

  const getPxX = (e: React.PointerEvent): number => {
    const rect = svgRef.current?.getBoundingClientRect()
    if (!rect) return 0
    return e.clientX - rect.left
  }

  // Build the line path. Y-domain is the full data extent with 4px
  // padding so the line never touches the brush's top/bottom edges.
  let pathD = ''
  if (width > 0) {
    let yMin = Infinity
    let yMax = -Infinity
    for (const p of data) {
      if (p.value < yMin) yMin = p.value
      if (p.value > yMax) yMax = p.value
    }
    const yRange = yMax - yMin || 1
    const yPad = 4
    const usable = Math.max(0, height - 2 * yPad)
    const segs: string[] = []
    for (let i = 0; i < data.length; i++) {
      const x = (i / (data.length - 1)) * width
      const y = height - yPad - ((data[i].value - yMin) / yRange) * usable
      segs.push(`${x.toFixed(1)},${y.toFixed(1)}`)
    }
    pathD = `M${segs.join(' L')}`
  }

  const selStart = dateToX(range.start)
  const selEnd = dateToX(range.end)

  const startDrag = (mode: DragMode) => (e: React.PointerEvent) => {
    e.stopPropagation()
    svgRef.current?.setPointerCapture(e.pointerId)
    setDrag({ mode, startPxX: getPxX(e), startRange: range })
  }

  const handlePointerMove = (e: React.PointerEvent) => {
    if (!drag) return
    const px = getPxX(e)
    const dxPx = px - drag.startPxX
    const dxMonths = Math.round((dxPx / Math.max(1, width)) * totalMonths)

    let next: DateRange
    if (drag.mode === 'left') {
      let newStart = addMonths(drag.startRange.start, dxMonths)
      if (newStart.getTime() < dataStart.getTime()) newStart = dataStart
      const maxStart = addMonths(drag.startRange.end, -minRangeMonths)
      if (newStart.getTime() > maxStart.getTime()) newStart = maxStart
      next = { start: newStart, end: drag.startRange.end }
    } else if (drag.mode === 'right') {
      let newEnd = addMonths(drag.startRange.end, dxMonths)
      if (newEnd.getTime() > dataEnd.getTime()) newEnd = dataEnd
      const minEnd = addMonths(drag.startRange.start, minRangeMonths)
      if (newEnd.getTime() < minEnd.getTime()) newEnd = minEnd
      next = { start: drag.startRange.start, end: newEnd }
    } else {
      let newStart = addMonths(drag.startRange.start, dxMonths)
      let newEnd = addMonths(drag.startRange.end, dxMonths)
      const duration = monthsBetween(drag.startRange.start, drag.startRange.end)
      if (newStart.getTime() < dataStart.getTime()) {
        newStart = dataStart
        newEnd = addMonths(newStart, duration)
      }
      if (newEnd.getTime() > dataEnd.getTime()) {
        newEnd = dataEnd
        newStart = addMonths(newEnd, -duration)
      }
      next = { start: newStart, end: newEnd }
    }
    onRangeChange(next)
  }

  const handlePointerUp = (e: React.PointerEvent) => {
    if (drag) {
      svgRef.current?.releasePointerCapture(e.pointerId)
      setDrag(null)
    }
  }

  const handleTrackPointerDown = (e: React.PointerEvent) => {
    if (drag) return
    const px = getPxX(e)
    if (px >= selStart && px <= selEnd) return // click inside selection — ignore
    const clickDate = pxToDate(px)
    const duration = monthsBetween(range.start, range.end)
    const half = Math.floor(duration / 2)
    let newStart = addMonths(clickDate, -half)
    let newEnd = addMonths(newStart, duration)
    if (newStart.getTime() < dataStart.getTime()) {
      newStart = dataStart
      newEnd = addMonths(newStart, duration)
    }
    if (newEnd.getTime() > dataEnd.getTime()) {
      newEnd = dataEnd
      newStart = addMonths(newEnd, -duration)
    }
    onRangeChange({ start: newStart, end: newEnd })
  }

  // Keyboard handlers per WCAG 2.1.1: each handle exposes role="slider"
  // and responds to arrow keys (1 month per press), Page Up/Down (12
  // months), Home/End (clamp to data extent). Bounds match the
  // pointer-drag clamps above so keyboard + mouse stay consistent.
  // Note: Hebrew RTL doesn't change the slider semantic — Right
  // arrow always moves the value forward in time (toward dataEnd),
  // matching how the visual chart flows left-to-right.
  const stepHandle = (handle: 'left' | 'right', months: number) => {
    if (handle === 'left') {
      let newStart = addMonths(range.start, months)
      if (newStart.getTime() < dataStart.getTime()) newStart = dataStart
      const maxStart = addMonths(range.end, -minRangeMonths)
      if (newStart.getTime() > maxStart.getTime()) newStart = maxStart
      onRangeChange({ start: newStart, end: range.end })
    } else {
      let newEnd = addMonths(range.end, months)
      if (newEnd.getTime() > dataEnd.getTime()) newEnd = dataEnd
      const minEnd = addMonths(range.start, minRangeMonths)
      if (newEnd.getTime() < minEnd.getTime()) newEnd = minEnd
      onRangeChange({ start: range.start, end: newEnd })
    }
  }

  const handleKeyDown = (handle: 'left' | 'right') => (e: React.KeyboardEvent) => {
    let consumed = true
    if (e.key === 'ArrowRight' || e.key === 'ArrowUp') stepHandle(handle, 1)
    else if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') stepHandle(handle, -1)
    else if (e.key === 'PageUp') stepHandle(handle, 12)
    else if (e.key === 'PageDown') stepHandle(handle, -12)
    else if (e.key === 'Home') {
      if (handle === 'left') {
        onRangeChange({ start: dataStart, end: range.end })
      } else {
        const minEnd = addMonths(range.start, minRangeMonths)
        onRangeChange({ start: range.start, end: minEnd })
      }
    } else if (e.key === 'End') {
      if (handle === 'left') {
        const maxStart = addMonths(range.end, -minRangeMonths)
        onRangeChange({ start: maxStart, end: range.end })
      } else {
        onRangeChange({ start: range.start, end: dataEnd })
      }
    } else {
      consumed = false
    }
    if (consumed) e.preventDefault()
  }

  // ARIA value for each handle expressed as months-since-dataStart.
  // Screen readers read this as a position; combined with the
  // valuetext we provide a human-readable date alongside.
  const totalMonthsAria = totalMonths
  const leftValue = monthsBetween(dataStart, range.start)
  const rightValue = monthsBetween(dataStart, range.end)
  const formatHandleDate = (d: Date): string =>
    `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`

  const dragMode = drag?.mode ?? null

  return (
    <div
      ref={containerRef}
      className="brush"
      data-dragging={dragMode ?? undefined}
      style={{ height }}
    >
      <svg
        ref={svgRef}
        width={width || undefined}
        height={height}
        viewBox={`0 0 ${Math.max(1, width)} ${height}`}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
        role="img"
        aria-label="בורר טווח: תרשים סקירה"
      >
        {/* Track / hit area for click-to-jump. */}
        <rect
          x={0}
          y={0}
          width={width || 0}
          height={height}
          className="brush-track"
          onPointerDown={handleTrackPointerDown}
        />

        {/* Mini line chart of full data extent. */}
        <path d={pathD} className="brush-line" pointerEvents="none" />

        {/* Selection body — drag to pan. */}
        <rect
          x={selStart}
          y={0}
          width={Math.max(0, selEnd - selStart)}
          height={height}
          className="brush-selection-body"
          onPointerDown={startDrag('body')}
        />

        {/* Selection outline — purely decorative. */}
        <rect
          x={selStart}
          y={0.5}
          width={Math.max(0, selEnd - selStart)}
          height={Math.max(0, height - 1)}
          className="brush-selection-outline"
          pointerEvents="none"
        />

        {/* Left handle — controls the start (oldest) edge of the
         * selection. Two-rect pattern: an invisible wider rect
         * (HANDLE_TOUCH_WIDTH) carries pointer + keyboard handlers
         * and ARIA, providing a comfortable touch target; the
         * visible decorative rect (HANDLE_WIDTH) sits on top with
         * pointer-events disabled so it doesn't intercept events.
         * Same approach for the right handle below. */}
        <rect
          x={selStart - HANDLE_TOUCH_WIDTH / 2}
          y={0}
          width={HANDLE_TOUCH_WIDTH}
          height={height}
          className="brush-handle-touch"
          data-handle="left"
          role="slider"
          tabIndex={0}
          aria-label="תאריך התחלה של הטווח"
          aria-valuemin={0}
          aria-valuemax={totalMonthsAria}
          aria-valuenow={leftValue}
          aria-valuetext={formatHandleDate(range.start)}
          onPointerDown={startDrag('left')}
          onKeyDown={handleKeyDown('left')}
        />
        <rect
          x={selStart - HANDLE_WIDTH / 2}
          y={0}
          width={HANDLE_WIDTH}
          height={height}
          className="brush-handle"
          aria-hidden="true"
          pointerEvents="none"
        />

        {/* Right handle — same two-rect pattern as left. */}
        <rect
          x={selEnd - HANDLE_TOUCH_WIDTH / 2}
          y={0}
          width={HANDLE_TOUCH_WIDTH}
          height={height}
          className="brush-handle-touch"
          data-handle="right"
          role="slider"
          tabIndex={0}
          aria-label="תאריך סיום של הטווח"
          aria-valuemin={0}
          aria-valuemax={totalMonthsAria}
          aria-valuenow={rightValue}
          aria-valuetext={formatHandleDate(range.end)}
          onPointerDown={startDrag('right')}
          onKeyDown={handleKeyDown('right')}
        />
        <rect
          x={selEnd - HANDLE_WIDTH / 2}
          y={0}
          width={HANDLE_WIDTH}
          height={height}
          className="brush-handle"
          aria-hidden="true"
          pointerEvents="none"
        />
      </svg>
    </div>
  )
}
