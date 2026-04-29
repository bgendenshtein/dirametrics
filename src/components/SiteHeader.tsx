/**
 * SiteHeader — navy nav bar shared by every route. Wordmark on the
 * inline-start, two routed nav links + theme toggle on the inline-end.
 *
 * Active-link styling: NavLink's `aria-current="page"` is what we hook
 * for the bright-white-on-current visual, falling back to the dimmed
 * white-with-low-opacity for the inactive route.
 */

import { NavLink } from 'react-router-dom'

import { Wordmark } from './Wordmark'
import { useTheme } from '../hooks/useTheme'

export function SiteHeader() {
  const { theme, toggleTheme } = useTheme()
  const toggleLabel = theme === 'dark' ? 'החלף למצב יום' : 'החלף למצב לילה'

  return (
    <header className="hero">
      <div className="hero-row">
        <NavLink to="/" className="hero-wordmark-link" aria-label="DiraMetrics — דף הבית">
          <Wordmark color="#ffffff" size={18} />
        </NavLink>

        <nav className="hero-nav" aria-label="ניווט ראשי">
          <NavLink
            to="/"
            end
            className={({ isActive }) =>
              `hero-nav-link${isActive ? ' is-active' : ''}`
            }
          >
            נתונים
          </NavLink>
          <NavLink
            to="/about"
            className={({ isActive }) =>
              `hero-nav-link${isActive ? ' is-active' : ''}`
            }
          >
            אודות ומתודולוגיה
          </NavLink>
          <button
            type="button"
            onClick={toggleTheme}
            className="theme-toggle"
            aria-label={toggleLabel}
            aria-pressed={theme === 'dark'}
            title={toggleLabel}
          >
            {theme === 'dark' ? '☀' : '☾'}
          </button>
        </nav>
      </div>
    </header>
  )
}
