/**
 * consent — user's analytics-consent decision, persisted to
 * localStorage. Single source of truth for whether GA4 should load
 * and whether track() calls should fire.
 *
 * Two consumers:
 *   - useConsent() → { state, accept, decline, save, openSettings, ...}
 *     for UI components that need to read or change the decision.
 *   - getConsent() / addListener() — the analytics module subscribes
 *     to changes so GA4 can be loaded as soon as the user accepts
 *     (and prevented from loading when they decline).
 *
 * Versioning: consent.version = '1.0'. Bumping the version (e.g.
 * if we add a new tracking purpose) invalidates older decisions
 * and re-shows the banner — the version stamp is checked on read.
 */

import { useEffect, useState } from 'react'

const STORAGE_KEY = 'dirametrics-consent'
const CURRENT_VERSION = '1.0'

export interface ConsentRecord {
  /** Whether the user has accepted analytics tracking. */
  analytics: boolean
  /** Schema version of this record. Bump when the consent surface
   * changes meaningfully (new purposes, new vendors). On read, an
   * older version is treated as "no decision" and the banner shows. */
  version: string
  /** ISO timestamp of when the user made the decision. Useful for
   * debugging + future "your consent was X months ago, please
   * re-confirm" flows. */
  date: string
}

export type ConsentState =
  | { status: 'pending' }
  | { status: 'decided'; record: ConsentRecord }

/** In-memory mirror of the persisted record. Read once on module
 * load; updated by setConsent and the storage event listener. Kept
 * outside React state so the analytics module can read it
 * synchronously without a hook. */
let current: ConsentState = readFromStorage()
const listeners = new Set<(state: ConsentState) => void>()

function readFromStorage(): ConsentState {
  if (typeof window === 'undefined') return { status: 'pending' }
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return { status: 'pending' }
    const parsed = JSON.parse(raw) as Partial<ConsentRecord>
    if (
      typeof parsed.analytics !== 'boolean' ||
      parsed.version !== CURRENT_VERSION ||
      typeof parsed.date !== 'string'
    ) {
      return { status: 'pending' }
    }
    return {
      status: 'decided',
      record: {
        analytics: parsed.analytics,
        version: parsed.version,
        date: parsed.date,
      },
    }
  } catch {
    return { status: 'pending' }
  }
}

function notify() {
  for (const listener of listeners) listener(current)
}

/** Synchronous read for non-React consumers (e.g. analytics module). */
export function getConsent(): ConsentState {
  return current
}

/** Subscribe to consent changes. Returns unsubscribe function. */
export function addConsentListener(fn: (state: ConsentState) => void): () => void {
  listeners.add(fn)
  return () => {
    listeners.delete(fn)
  }
}

/** Persist + broadcast a new decision. */
export function setConsent(analytics: boolean): void {
  const record: ConsentRecord = {
    analytics,
    version: CURRENT_VERSION,
    date: new Date().toISOString(),
  }
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(record))
  } catch {
    // localStorage can fail (Safari private mode, quota). Falls back
    // to in-memory only — the user's decision sticks for this tab
    // but the banner will reappear on reload. Acceptable failure.
  }
  current = { status: 'decided', record }
  notify()
}

/** React hook for consent state. Subscribes to module-level changes
 * so cross-component updates (e.g. footer "ניהול cookies" → settings
 * → save) propagate to all banner/footer/analytics consumers. */
export function useConsent(): ConsentState {
  const [state, setState] = useState<ConsentState>(current)
  useEffect(() => {
    const unsubscribe = addConsentListener((next) => setState(next))
    return unsubscribe
  }, [])
  return state
}
