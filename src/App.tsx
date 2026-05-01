/**
 * App — routed layout shell.
 *
 *   <SiteHeader>     navy bar with wordmark + nav links + theme toggle
 *   <Routes>
 *     "/"      → DashboardPage   (KPI strip + chart strip)
 *     "/about" → AboutPage       (methodology content)
 *   </Routes>
 *   <SiteFooter>     placeholder legal links + sources line
 *
 * BrowserRouter lives in main.tsx so the entry point owns history
 * and StrictMode boundaries. App stays a plain layout component.
 */

import { Analytics } from '@vercel/analytics/react'
import { lazy, Suspense, useEffect } from 'react'
import { Route, Routes, useLocation } from 'react-router-dom'

import { ConsentBanner } from './components/ConsentBanner'
import { SiteFooter } from './components/SiteFooter'
import { SiteHeader } from './components/SiteHeader'
import { trackPageView } from './lib/analytics'
import DashboardPage from './pages/DashboardPage'

// AboutPage pulls in react-markdown + the methodology source file;
// keeping it lazy means visitors who never click "אודות ומתודולוגיה"
// don't pay for the markdown renderer in their initial bundle.
const AboutPage = lazy(() => import('./pages/AboutPage'))

// Legal/accessibility pages each lazy-load their own markdown source.
// The shared MarkdownPage renderer is folded into each chunk by Vite,
// since none of these are visited by most users.
const PrivacyPage = lazy(() => import('./pages/PrivacyPage'))
const TermsPage = lazy(() => import('./pages/TermsPage'))
const AccessibilityPage = lazy(() => import('./pages/AccessibilityPage'))

/** Page-view tracker. Fires on every route change; trackPageView is
 * itself gated on consent (no-op if analytics is declined or not yet
 * decided), so this is safe to mount unconditionally. */
function RouteTracker() {
  const location = useLocation()
  useEffect(() => {
    trackPageView(location.pathname + location.search, document.title)
  }, [location.pathname, location.search])
  return null
}

export default function App() {
  return (
    <>
      <RouteTracker />
      {/* Skip-to-main link per WCAG 2.4.1 (Bypass Blocks). Visible
       * only when keyboard-focused; lets users jump past the navy
       * header + nav directly to the page content. The #main-content
       * anchor lands on the first <main> element rendered by each
       * route. */}
      <a href="#main-content" className="skip-to-main">
        דלג לתוכן הראשי
      </a>
      <SiteHeader />
      <Suspense fallback={<div className="route-fallback" aria-hidden="true" />}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route path="/terms" element={<TermsPage />} />
          <Route path="/privacy" element={<PrivacyPage />} />
          <Route path="/accessibility" element={<AccessibilityPage />} />
        </Routes>
      </Suspense>
      <SiteFooter />
      <ConsentBanner />
      {/* Vercel Web Analytics — cookieless visit counting that runs
       * for ALL visitors, not just ones who accept the consent
       * banner. Counts page views + basic device/locale info; no
       * tracking cookies, no cross-site identifiers. Sits OUTSIDE
       * the consent gate intentionally: it's how we measure raw
       * visit volume without relying on GA4 acceptance rates. The
       * Analytics component injects its script lazily on mount and
       * pings Vercel's edge endpoint, which is auto-wired for
       * projects hosted on Vercel — no measurement ID needed. */}
      <Analytics />
    </>
  )
}
