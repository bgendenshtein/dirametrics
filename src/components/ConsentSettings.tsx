/**
 * ConsentSettings — modal dialog with toggle controls for analytics
 * consent. Two ways to open:
 *   1. From the consent banner's "הגדרות" button (first visit).
 *   2. From the footer's "ניהול cookies" link (subsequent visits).
 *
 * Open/close is coordinated via a custom window event so multiple
 * callers can trigger the dialog without a shared Context.
 */

import { useEffect, useRef, useState } from 'react'

import { useFocusTrap } from '../hooks/useFocusTrap'
import { getConsent, setConsent } from '../lib/consent'
import { onConsentSettingsOpen } from '../lib/consentEvents'

export function ConsentSettings() {
  const [open, setOpen] = useState(false)
  const dialogRef = useRef<HTMLDivElement>(null)
  // Local toggle state — initialized from the persisted consent record
  // when the dialog opens, then overwritten on save. The user can
  // experiment with toggles without committing until they hit "שמור
  // הגדרות".
  const [analyticsToggle, setAnalyticsToggle] = useState(false)
  useFocusTrap(dialogRef, open)

  useEffect(() => {
    return onConsentSettingsOpen(() => {
      const c = getConsent()
      setAnalyticsToggle(
        c.status === 'decided' ? c.record.analytics : false,
      )
      setOpen(true)
    })
  }, [])

  // Esc to close — common modal-dialog convention; keeps the dialog
  // dismissible without forcing a click on a specific control.
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open])

  if (!open) return null

  const handleSave = () => {
    setConsent(analyticsToggle)
    setOpen(false)
  }

  return (
    <div
      className="consent-modal-backdrop"
      onClick={() => setOpen(false)}
      role="presentation"
    >
      <div
        ref={dialogRef}
        className="consent-modal"
        role="dialog"
        aria-modal="true"
        aria-label="הגדרות פרטיות"
        onClick={(e) => e.stopPropagation()}
        tabIndex={-1}
      >
        <header className="consent-modal-header">
          <h2 className="consent-modal-title">הגדרות פרטיות</h2>
          <button
            type="button"
            className="consent-modal-close"
            onClick={() => setOpen(false)}
            aria-label="סגור"
          >
            <span aria-hidden="true">×</span>
          </button>
        </header>

        <div className="consent-modal-body">
          <label className="consent-toggle-row">
            <input
              type="checkbox"
              className="consent-toggle"
              checked={analyticsToggle}
              onChange={(e) => setAnalyticsToggle(e.target.checked)}
            />
            <span className="consent-toggle-label">
              <span className="consent-toggle-title">ניתוח שימוש (Google Analytics)</span>
              <span className="consent-toggle-desc">
                איסוף נתונים אנונימיים על השימוש באתר לצורך שיפור המוצר. אינו משמש לפרסום
                או לזיהוי אישי.
              </span>
            </span>
          </label>

          <div className="consent-toggle-row consent-toggle-row--info">
            <span className="consent-toggle-info-icon" aria-hidden="true">ⓘ</span>
            <span className="consent-toggle-label">
              <span className="consent-toggle-title">שמירת העדפות תצוגה</span>
              <span className="consent-toggle-desc">
                העדפות כמו מצב יום/לילה נשמרות באופן מקומי בדפדפן שלך בלבד, ואינן
                כרוכות באיסוף נתונים. הגדרה זו אינה ניתנת לכיבוי.
              </span>
            </span>
          </div>
        </div>

        <footer className="consent-modal-footer">
          <button
            type="button"
            className="consent-btn consent-btn--primary"
            onClick={handleSave}
          >
            שמור הגדרות
          </button>
          <button
            type="button"
            className="consent-btn consent-btn--text"
            onClick={() => setOpen(false)}
          >
            ביטול
          </button>
        </footer>
      </div>
    </div>
  )
}
