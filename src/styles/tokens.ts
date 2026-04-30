/**
 * tokens.ts — JS-time mirror of the design tokens declared as CSS custom
 * properties in src/index.css.
 *
 * src/index.css is the source of truth for static styling (anything that
 * goes into a stylesheet). This file exists for the small set of cases
 * where JS needs hex values directly — primarily Recharts series colors
 * and any chart-engine logic that can't be expressed via CSS.
 *
 * Drift between this file and src/index.css is a bug. If you change a
 * color or constant in one place, change it in the other.
 */

import { useEffect, useState } from 'react'

import type { Theme } from '../hooks/useTheme'

export const brand = {
  navy900: '#0b1e3a',
  navy800: '#13294b',
  navy700: '#1e3a66',
} as const

export const accent = {
  blue700: '#1d4ed8',
  blue500: '#2563eb',
  blue300: '#93c5fd',
} as const

/** Series color generator. Each chart series is assigned a stable
 * non-negative integer slot; that slot maps to a hue via the golden
 * angle (137.508°), which spaces successive hues maximally on the
 * color wheel for any N. Saturation is fixed at 70%; lightness flips
 * by theme so colors stay readable on both surfaces (50% in light,
 * 60% in dark — the same brightness window the previous fixed
 * palette landed in).
 *
 * The starting hue is 220° (brand-blue territory), so the first
 * series picked feels on-brand; subsequent series fan out through
 * the spectrum without ever clustering. Slots are monotonic in
 * ChartCard so removing a series doesn't shift other series'
 * colors. */
const HUE_START = 220
const GOLDEN_ANGLE = 137.508

export function seriesColorBySlot(slot: number, theme: Theme): string {
  const hue = ((HUE_START + slot * GOLDEN_ANGLE) % 360 + 360) % 360
  const saturation = 70
  const lightness = theme === 'dark' ? 60 : 50
  return `hsl(${hue.toFixed(2)}, ${saturation}%, ${lightness}%)`
}

export const semantic = {
  light: { up: '#047857', down: '#b91c1c' },
  dark:  { up: '#34d399', down: '#fb7185' },
} as const

export const motion = {
  fastMs:   120,
  normalMs: 220,
  longMs:   4000,
  ease:     'cubic-bezier(0.2, 0, 0, 1)',
} as const

/** Convenience: pick the right semantic hex (up/down) for the active theme. */
export function semanticColor(kind: 'up' | 'down', theme: Theme): string {
  return semantic[theme][kind]
}

/**
 * Hook variant of seriesColor / semanticColor — re-renders when the
 * document's data-theme attribute changes. Use this in chart components
 * that pass hex strings to Recharts.
 */
export function useResolvedTheme(): Theme {
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof document === 'undefined') return 'light'
    return (document.documentElement.dataset.theme as Theme) ?? 'light'
  })

  useEffect(() => {
    const obs = new MutationObserver(() => {
      const next = (document.documentElement.dataset.theme as Theme) ?? 'light'
      setTheme(next)
    })
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })
    return () => obs.disconnect()
  }, [])

  return theme
}
