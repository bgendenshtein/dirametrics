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
 *
 * Placeholder measurement ID 'G-NX5Z6NJBMW' — replace with the
 * real ID before launch. The loader functions normally with the
 * placeholder; GA4 just rejects events to a non-existent property
 * (visible in DevTools Network as 4xx; doesn't break the app).
 */

import { addConsentListener, getConsent } from './consent'

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
  window.dataLayer = window.dataLayer ?? []
  window.gtag = function gtag(...args: unknown[]) {
    window.dataLayer!.push(args)
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
addConsentListener((state) => {
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
