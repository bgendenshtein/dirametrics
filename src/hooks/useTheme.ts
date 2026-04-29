/**
 * useTheme — applies a 'light' | 'dark' theme by setting `data-theme`
 * on <html>, persists the choice to localStorage, and falls back to the
 * OS preference on first visit.
 *
 * CSS in src/index.css reacts to `[data-theme="dark"]` and switches all
 * color custom properties. JS components that need hex values (chart
 * series colors) should use useResolvedTheme() from styles/tokens.ts so
 * they re-render on attribute change.
 */

import { useCallback, useEffect, useState } from 'react'

export type Theme = 'light' | 'dark'

const STORAGE_KEY = 'dirametrics-theme'

function readInitialTheme(): Theme {
  if (typeof window === 'undefined') return 'light'
  const stored = window.localStorage.getItem(STORAGE_KEY)
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export interface UseThemeResult {
  theme: Theme
  setTheme: (next: Theme) => void
  toggleTheme: () => void
}

export function useTheme(): UseThemeResult {
  const [theme, setThemeState] = useState<Theme>(readInitialTheme)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try {
      window.localStorage.setItem(STORAGE_KEY, theme)
    } catch {
      // Ignore quota / private-mode failures — UI still works for the session.
    }
  }, [theme])

  const setTheme = useCallback((next: Theme) => setThemeState(next), [])
  const toggleTheme = useCallback(
    () => setThemeState((prev) => (prev === 'light' ? 'dark' : 'light')),
    [],
  )

  return { theme, setTheme, toggleTheme }
}
