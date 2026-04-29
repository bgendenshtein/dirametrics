/**
 * ConsentBanner — bottom-fixed banner shown to first-time visitors
 * who haven't yet decided about analytics consent.
 *
 * Three actions:
 *   - "מקבל" (Accept) → analytics enabled, banner dismisses
 *   - "דוחה" (Decline) → analytics disabled, banner dismisses
 *   - "הגדרות" (Settings) → opens ConsentSettings modal
 *
 * The banner manages its own slide-in animation (200ms after mount),
 * but the dismissal is handled by the consent state — once a
 * decision is recorded, useConsent reports status='decided' and the
 * banner unmounts (parent stops rendering it). The `phase` state
 * here only handles the entrance, not exit; the transition out is
 * a quick fade via the .is-hiding class applied just before
 * unmount via the SETTINGS_OPENED_EVENT side-channel for the
 * footer's "ניהול cookies" link.
 *
 * The footer's "ניהול cookies" link dispatches a window event that
 * this component (when no banner is showing) ignores — but that
 * the App's mounted ConsentSettings dialog listens for to open.
 * That keeps the settings dialog mountable from anywhere without
 * needing a Context provider.
 */

import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'

import { setConsent, useConsent } from '../lib/consent'
import { openConsentSettings } from '../lib/consentEvents'
import { ConsentSettings } from './ConsentSettings'

export function ConsentBanner() {
  const consent = useConsent()
  const [mounted, setMounted] = useState(false)

  // Mount → wait one frame → set 'mounted' so the CSS transition
  // from initial off-screen state to visible plays. Without the
  // delay the browser commits the final state immediately and
  // the animation doesn't run.
  useEffect(() => {
    if (consent.status !== 'pending') return
    const t = window.setTimeout(() => setMounted(true), 16)
    return () => window.clearTimeout(t)
  }, [consent.status])

  // ConsentSettings is mounted unconditionally so that the footer's
  // "ניהול cookies" link can open it even after the banner is gone.
  // It manages its own open/closed state via the openConsentSettings()
  // event channel.
  if (consent.status !== 'pending') {
    return <ConsentSettings />
  }

  return (
    <>
      <div
        className={`consent-banner${mounted ? ' is-mounted' : ''}`}
        role="dialog"
        aria-live="polite"
        aria-label="הסכמה לקובצי Cookie"
      >
        <div className="consent-banner-inner">
          <div className="consent-banner-text">
            <h3 className="consent-banner-title">השימוש שלך באתר</h3>
            <p className="consent-banner-body">
              האתר משתמש בקובצי Cookie לצרכי ניתוח שימוש בלבד. הנתונים נאספים באמצעות
              Google Analytics ואינם משמשים לזיהוי אישי או לפרסום ממוקד. ניתן לקרוא
              עוד ב<Link to="/privacy">מדיניות הפרטיות</Link>.
            </p>
          </div>
          <div className="consent-banner-actions">
            <button
              type="button"
              className="consent-btn consent-btn--primary"
              onClick={() => setConsent(true)}
            >
              מקבל
            </button>
            <button
              type="button"
              className="consent-btn consent-btn--secondary"
              onClick={() => setConsent(false)}
            >
              דוחה
            </button>
            <button
              type="button"
              className="consent-btn consent-btn--text"
              onClick={openConsentSettings}
            >
              הגדרות
            </button>
          </div>
        </div>
      </div>
      <ConsentSettings />
    </>
  )
}
