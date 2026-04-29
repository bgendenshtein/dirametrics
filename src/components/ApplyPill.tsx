/**
 * ApplyPill — small accent-blue pill that offers to mirror a recent
 * filter change to the other chart card.
 *
 * Lifecycle phases (`phase` state):
 *   in       — first render frame; CSS transition target unset so the
 *              mounted element starts at opacity 0 / translateY(-2px).
 *   visible  — settles into resting state via CSS transition.
 *   flash    — user clicked ✓; brief brightened/scaled state to confirm
 *              the action before fading out.
 *   out      — fading away (CSS transition back to opacity 0). Triggered
 *              by ✓ click (after flash), × click, or 4-second timeout.
 *   gone     — element returns null; parent should remove the pill from
 *              its render tree on the onDismiss callback.
 *
 * The component owns its own timers so the parent only has to provide
 * "what changed" and the two side-effect callbacks (apply / dismiss).
 * Replacing one pill with another is handled at the parent level by
 * keying the JSX <ApplyPill key={pillId} /> — each new pill is a fresh
 * instance with its own phase progression.
 */

import { useEffect, useState } from 'react'

type Phase = 'in' | 'visible' | 'flash' | 'out' | 'gone'

const ENTER_FRAME_MS = 16     // one rAF tick — lets the DOM commit the
                              // initial 'in' state before transitioning
                              // to 'visible'
const FLASH_MS = 220          // brief confirmation pulse
const FADE_MS = 220           // matches --duration-normal; CSS transition

export interface ApplyPillProps {
  /** Pill copy (excluding the action buttons). The default reads
   * "החל גם על הגרף השני?". Pass a custom string to disambiguate
   * across chart cards if needed in the future. */
  label?: string
  /** Fires when the user clicks ✓. Parent should mirror the change to
   * the other card; the pill then enters its flash → out animation. */
  onApply: () => void
  /** Fires once the pill has finished its out-animation (whether
   * triggered by ✓, ×, or auto-dismiss). Parent should clear the
   * pill from its state at this point. */
  onDismiss: () => void
  /** Auto-dismiss timeout in milliseconds. Default 4000 per spec. */
  autoDismissMs?: number
}

export function ApplyPill({
  label = 'החל גם על הגרף השני?',
  onApply,
  onDismiss,
  autoDismissMs = 4000,
}: ApplyPillProps) {
  const [phase, setPhase] = useState<Phase>('in')

  // Auto-dismiss timer — only runs while the pill is in 'visible' state.
  // If the user clicks ✓ or × before the timer fires, the phase moves
  // away from 'visible' and the cleanup cancels the timeout.
  useEffect(() => {
    if (phase !== 'visible') return
    const t = window.setTimeout(() => setPhase('out'), autoDismissMs)
    return () => window.clearTimeout(t)
  }, [phase, autoDismissMs])

  // Phase progression for the non-resting states. Each branch returns
  // a cleanup that cancels its own timer, so phase transitions are
  // resilient to interruptions (e.g., user clicks ✓ during the
  // initial 'in' frame).
  useEffect(() => {
    if (phase === 'in') {
      const t = window.setTimeout(() => setPhase('visible'), ENTER_FRAME_MS)
      return () => window.clearTimeout(t)
    }
    if (phase === 'flash') {
      const t = window.setTimeout(() => setPhase('out'), FLASH_MS)
      return () => window.clearTimeout(t)
    }
    if (phase === 'out') {
      const t = window.setTimeout(() => setPhase('gone'), FADE_MS)
      return () => window.clearTimeout(t)
    }
    return undefined
  }, [phase])

  // 'gone' is the terminal state; signal the parent so it can drop the
  // <ApplyPill> from its tree. The effect runs once when phase
  // becomes 'gone'.
  useEffect(() => {
    if (phase === 'gone') onDismiss()
  }, [phase, onDismiss])

  if (phase === 'gone') return null

  const handleApply = () => {
    if (phase === 'out' || phase === 'flash') return
    onApply()
    setPhase('flash')
  }

  const handleDismiss = () => {
    if (phase === 'out') return
    setPhase('out')
  }

  return (
    <span className="apply-pill" data-phase={phase} role="status">
      <span className="apply-pill-text">{label}</span>
      <button
        type="button"
        className="apply-pill-btn apply-pill-btn--apply"
        onClick={handleApply}
        aria-label="החל גם על הגרף השני"
      >
        <span aria-hidden="true">✓</span>
      </button>
      <button
        type="button"
        className="apply-pill-btn apply-pill-btn--dismiss"
        onClick={handleDismiss}
        aria-label="התעלם"
      >
        <span aria-hidden="true">×</span>
      </button>
    </span>
  )
}
