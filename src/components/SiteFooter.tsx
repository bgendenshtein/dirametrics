/**
 * SiteFooter — closing element of every route. Two rows on desktop:
 *
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │ [sources + provisional legend]   [nav links]                │
 *   │                                                             │
 *   │                  © DiraMetrics 2026                         │
 *   └─────────────────────────────────────────────────────────────┘
 *
 * On narrow viewports the rows stack vertically so the source
 * attribution and the link group don't crowd each other.
 *
 * All four nav links resolve to real routes. /about renders the
 * methodology page; /terms, /privacy, /accessibility each render
 * their corresponding markdown source from docs/ via the shared
 * MarkdownPage renderer.
 */

import { Link } from 'react-router-dom'

import { track } from '../lib/analytics'
import { openConsentSettings } from '../lib/consentEvents'

interface FooterLink {
  to: string
  label: string
}

const FOOTER_LINKS: FooterLink[] = [
  { to: '/about',          label: 'אודות ומתודולוגיה' },
  { to: '/terms',          label: 'תנאי שימוש'        },
  { to: '/privacy',        label: 'מדיניות פרטיות'     },
  { to: '/accessibility',  label: 'הצהרת נגישות'       },
]

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="site-footer-inner">
        <div className="site-footer-row site-footer-row--top">
          <div className="site-footer-meta">
            <span>
              מקורות: בנק ישראל (SDMX), הלשכה המרכזית לסטטיסטיקה. מתעדכן יומית.
            </span>
            <span className="site-footer-provisional">
              <span className="site-footer-provisional-line" aria-hidden="true" />
              ערכים זמניים — שלוש הקריאות האחרונות
            </span>
          </div>
          <nav className="site-footer-nav" aria-label="קישורי תחתית">
            {FOOTER_LINKS.map((link) => (
              <span key={link.to} className="site-footer-nav-item">
                <Link
                  to={link.to}
                  onClick={() =>
                    track('footer_link_click', { to: link.to, label: link.label })
                  }
                >
                  {link.label}
                </Link>
                <span className="site-footer-sep" aria-hidden="true">
                  ·
                </span>
              </span>
            ))}
            {/* "ניהול cookies" — re-opens the consent settings modal
             * so users can revisit their analytics-tracking decision
             * after dismissing the initial banner. */}
            <span className="site-footer-nav-item">
              <button
                type="button"
                className="site-footer-cookies-btn"
                onClick={() => {
                  track('footer_link_click', { to: 'cookies', label: 'ניהול cookies' })
                  openConsentSettings()
                }}
              >
                ניהול cookies
              </button>
            </span>
          </nav>
        </div>
        <div className="site-footer-row site-footer-row--copy">
          <span className="site-footer-copy">© DiraMetrics 2026</span>
        </div>
      </div>
    </footer>
  )
}
