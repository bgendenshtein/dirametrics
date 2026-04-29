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

export const seriesPalette = {
  // Light values stepped one Tailwind shade lighter than the original
  // tokens-spec (blue/red/green from -600 → -500) for chart-series
  // restraint. Dark values unchanged — they were already tuned for
  // visibility on dark surfaces. Keep these in sync with the CSS
  // custom properties in src/index.css.
  light: {
    blue:   '#3b82f6',
    red:    '#ef4444',
    green:  '#10b981',
    amber:  '#d97706',
    violet: '#7c3aed',
  },
  dark: {
    blue:   '#60a5fa',
    red:    '#f87171',
    green:  '#34d399',
    amber:  '#fbbf24',
    violet: '#a78bfa',
  },
} as const

export type SeriesColorName = keyof typeof seriesPalette.light

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

/** Convenience: pick the right series hex for the active theme. */
export function seriesColor(name: SeriesColorName, theme: Theme): string {
  return seriesPalette[theme][name]
}

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
