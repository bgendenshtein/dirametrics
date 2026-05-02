/**
 * analytics — GA4 conditional loader + track() facade.
 *
 * Behavior:
 *   - At module load: subscribe to consent changes. When consent
 *     transitions to analytics=true, inject the GA4 script tag.
 *     When it transitions to analytics=false (declined), DO NOT
 *     load. (We don't tear down a loaded GA4 if the user later
 *     declines via the settings panel — they'd need to reload to
 *     clear gtag's in-memory queue. Documented as a known
 *     limitation; revisit if it matters.)
 *   - track(name, params): no-op when consent.analytics !== true
 *     OR when GA4 isn't loaded yet. When both conditions hold,
 *     forwards to gtag('event', name, params).
 *   - On consent-accept the loader fires a one-shot page_view for
 *     the current URL ("seed event") so the freshly-loaded GA4
 *     session has its first hit. Without this, a visitor who
 *     accepts on the landing page and stays there never produces
 *     a page_view (RouteTracker fires only on route *changes* and
 *     send_page_view is disabled in config to avoid double-counting).
 */

import { addConsentListener, getConsent, type ConsentState } from './consent'

const MEASUREMENT_ID = 'G-NX5Z6NJBMW'

declare global {
  interface Window {
    dataLayer?: unknown[]
    gtag?: (...args: unknown[]) => void
  }
}

let scriptInjected = false

/** Inject the GA4 gtag.js script and bootstrap the dataLayer. Idempotent. */
function loadGoogleAnalytics(): void {
  if (scriptInjected) return
  if (typeof window === 'undefined' || typeof document === 'undefined') return
  scriptInjected = true

  // Bootstrap the dataLayer + gtag stub before the script loads so
  // calls made between injection and script-ready get queued.
  //
  // The stub MUST use `arguments` (an Arguments object) — NOT a rest
  // parameter array. gtag.js's queue processor distinguishes gtag
  // commands from GTM event objects by entry shape: Arguments-like
  // entries are processed as commands (config/event/js), Array entries
  // are treated as GTM event objects expecting an `event` property and
  // are silently ignored when that property is absent. Using
  // `(...args) => dataLayer.push(args)` looks equivalent but pushes a
  // real Array — gtag.js loads, recognizes nothing in the queue, and
  // never fires a /collect request. This took an hours-long debug
  // session to find; do not "modernize" this function.
  window.dataLayer = window.dataLayer ?? []
  window.gtag = function gtag() {
    // eslint-disable-next-line prefer-rest-params
    window.dataLayer!.push(arguments)
  }
  window.gtag('js', new Date())
  window.gtag('config', MEASUREMENT_ID, {
    // SPA: we'll fire page_view manually on route change rather than
    // relying on the auto pageview, since the URL changes without a
    // full page load.
    send_page_view: false,
    // anonymize_ip is implicit in GA4 (Google strips the last octet
    // automatically). No need to set it explicitly, but documenting
    // here so a future reader doesn't try to add it.
  })

  // Seed the session: fire an immediate page_view for the page that
  // was already loaded when consent was granted. RouteTracker fires
  // page_view on route *changes*, but a fresh visitor who accepts
  // cookies on the landing page never triggers a route change — so
  // without this seed call GA4 sees the user but never gets their
  // first page_view, and the session shows zero hits in Realtime.
  // Mirrors the manual page_view shape we send from trackPageView.
  window.gtag('event', 'page_view', {
    page_path: window.location.pathname + window.location.search,
    page_title: document.title,
    page_location: window.location.href,
  })

  const s = document.createElement('script')
  s.async = true
  s.src = `https://www.googletagmanager.com/gtag/js?id=${MEASUREMENT_ID}`
  document.head.appendChild(s)
}

// Module-init: load immediately if consent already says yes (returning
// visitor); subscribe so consent flips to yes mid-session also load.
const initial = getConsent()
if (initial.status === 'decided' && initial.record.analytics) {
  loadGoogleAnalytics()
}
addConsentListener((state: ConsentState) => {
  if (state.status === 'decided' && state.record.analytics) {
    loadGoogleAnalytics()
  }
})

/** Fire a GA4 event, gated on consent. Safe to call from anywhere —
 * pre-consent calls no-op silently rather than buffering. */
export function track(eventName: string, params?: Record<string, unknown>): void {
  const state = getConsent()
  if (state.status !== 'decided' || !state.record.analytics) return
  if (typeof window === 'undefined' || !window.gtag) return
  window.gtag('event', eventName, params)
}

/** Manual page-view event for client-side route changes. Should be
 * called from a top-level component on every route change. */
export function trackPageView(path: string, title?: string): void {
  track('page_view', {
    page_path: path,
    page_title: title,
    page_location: typeof window !== 'undefined' ? window.location.href : undefined,
  })
}
