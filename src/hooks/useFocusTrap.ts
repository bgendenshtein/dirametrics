/**
 * useFocusTrap — focus management for modal dialogs.
 *
 * On `active === true`:
 *   1. Saves the currently-focused element (the modal trigger).
 *   2. Moves focus into the dialog: first focusable, or the dialog
 *      element itself if no focusable child exists.
 *   3. Traps Tab/Shift+Tab inside the dialog by detecting boundary
 *      crossings and cycling back to the opposite end.
 *
 * On `active` flipping back to false:
 *   - Restores focus to the element that was focused when the modal
 *     opened (so the user lands back on the button they clicked,
 *     not at the document start).
 *
 * The trap implementation queries focusable elements on every Tab
 * press rather than caching them, since the dialog's contents can
 * change while open (e.g., toggle states, new buttons appearing).
 *
 * Doesn't do anything cross-cutting like blocking outside clicks or
 * Esc — those are concerns of the modal component itself. This hook
 * is purely focus-management.
 */

import { useEffect, useRef, type RefObject } from 'react'

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function getFocusableElements(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR))
    .filter((el) => !el.hasAttribute('hidden') && el.offsetParent !== null)
}

export function useFocusTrap(
  containerRef: RefObject<HTMLElement | null>,
  active: boolean,
): void {
  const lastFocusedRef = useRef<HTMLElement | null>(null)

  useEffect(() => {
    if (!active) return
    const container = containerRef.current
    if (!container) return

    // Stash the trigger so we can restore focus to it on close.
    // Cast: document.activeElement is typed as Element | null, but
    // any focusable element is also an HTMLElement in practice.
    lastFocusedRef.current = document.activeElement as HTMLElement | null

    // Focus the first focusable element. Using rAF so the browser
    // has committed the dialog's render before we move focus —
    // querying immediately can race the React render.
    const focusFirst = () => {
      const focusables = getFocusableElements(container)
      if (focusables.length > 0) {
        focusables[0].focus()
      } else {
        // Fallback: focus the container itself so screen readers
        // announce the dialog. Container needs tabIndex=-1 in markup.
        container.focus()
      }
    }
    const rafId = requestAnimationFrame(focusFirst)

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Tab') return
      const focusables = getFocusableElements(container)
      if (focusables.length === 0) {
        e.preventDefault()
        return
      }
      const first = focusables[0]
      const last = focusables[focusables.length - 1]
      const activeEl = document.activeElement as HTMLElement | null
      // Shift+Tab on the first focusable → wrap to the last
      if (e.shiftKey && activeEl === first) {
        e.preventDefault()
        last.focus()
        return
      }
      // Tab on the last focusable → wrap to the first
      if (!e.shiftKey && activeEl === last) {
        e.preventDefault()
        first.focus()
        return
      }
      // If focus has somehow escaped the container (rare; can happen
      // if a focused element was removed from the DOM mid-interaction),
      // pull it back to the first focusable.
      if (activeEl && !container.contains(activeEl)) {
        e.preventDefault()
        first.focus()
      }
    }

    document.addEventListener('keydown', handleKeyDown, true)

    return () => {
      cancelAnimationFrame(rafId)
      document.removeEventListener('keydown', handleKeyDown, true)
      // Restore focus to the trigger, but only if it's still in the
      // DOM and focusable. If the user navigated away or unmounted
      // the trigger somehow, we silently skip — better than throwing
      // focus at a stale ref.
      const last = lastFocusedRef.current
      if (last && document.contains(last) && typeof last.focus === 'function') {
        last.focus()
      }
      lastFocusedRef.current = null
    }
  }, [active, containerRef])
}
