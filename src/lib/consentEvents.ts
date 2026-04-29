/**
 * Side-channel for opening the ConsentSettings modal from anywhere
 * without a Context provider. Lives in its own module so the
 * component file only exports the component (Vite fast-refresh
 * requires single-export-kind per file).
 */

const OPEN_EVENT = 'dirametrics:open-consent-settings'

/** Trigger the settings dialog. Listeners (currently the
 * ConsentSettings component) handle the open transition. */
export function openConsentSettings() {
  window.dispatchEvent(new CustomEvent(OPEN_EVENT))
}

/** Subscribe to open requests. Returns unsubscribe function. */
export function onConsentSettingsOpen(handler: () => void): () => void {
  window.addEventListener(OPEN_EVENT, handler)
  return () => window.removeEventListener(OPEN_EVENT, handler)
}
