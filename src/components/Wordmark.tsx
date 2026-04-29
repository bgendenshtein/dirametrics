/**
 * Wordmark — DiraMetrics brand mark, four-part composition matching
 * design/dashboard.jsx Logo():
 *
 *   ┌─────────────────────────┬────┬─────────────────┐
 *   │  דירה {מטריקס}          │ │  │ Dira {Metrics}  │
 *   └─────────────────────────┴────┴─────────────────┘
 *      bold + light/55%       hairline   semi+regular/55%
 *
 * The Hebrew "דירה" is bold (700); "מטריקס" tail is regular (400) at
 * 55% opacity. A 1px hairline divider (18% opacity) separates the
 * Hebrew and Latin halves. The Latin "Dira" is semibold (600), 78%
 * size of the Hebrew; the "Metrics" tail is regular at 55% opacity.
 *
 * Color is set via `color` prop (default = currentColor) — in the
 * navy header it's white. Opacity ramps inside the wordmark are
 * deliberate: sub-elements decrease via opacity rather than separate
 * color tokens, so the wordmark looks correct on any background that
 * contrasts with `color` enough.
 */

export interface WordmarkProps {
  /** Base color for the wordmark. Defaults to inherited text color. */
  color?: string
  /** Pixel size for the Hebrew bold portion. Latin auto-derives at 0.78×. */
  size?: number
}

export function Wordmark({ color = 'currentColor', size = 18 }: WordmarkProps) {
  return (
    <span
      className="wordmark"
      style={{ color, fontSize: size }}
      aria-label="דירהמטריקס"
    >
      <span className="wordmark-he" aria-hidden="true">
        דירה
        <span className="wordmark-he-tail">מטריקס</span>
      </span>
      <span className="wordmark-divider" aria-hidden="true" />
      <span className="wordmark-en" aria-hidden="true">
        Dira<span className="wordmark-en-tail">Metrics</span>
      </span>
    </span>
  )
}
