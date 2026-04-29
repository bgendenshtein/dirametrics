/**
 * ChipGroup — segmented control for time range, frequency, display mode.
 *
 * Visual: rounded container (surface-subtle bg, hairline border) with
 * inline button "chips". Active chip gets a brighter background + ring;
 * inactive chips are muted with hover-to-strong text transition.
 *
 * A11y: role="group" + aria-pressed on each button. Each chip is in the
 * Tab order; Space/Enter activates. This is the "toggle button group"
 * ARIA pattern. The strict radiogroup pattern (single tab stop, arrow
 * keys to move within) requires custom keyboard handling — deferred
 * until we wire the controls to actual data.
 */

export interface ChipOption<T extends string> {
  id: T
  label: string
}

export interface ChipGroupProps<T extends string> {
  /** Active option id, or null when no chip should be highlighted
   * (e.g., the user manually adjusted a brush selection that doesn't
   * match any preset). */
  value: T | null
  onChange: (next: T) => void
  options: ChipOption<T>[]
  /** Localized label for screen readers, e.g., "טווח זמן". */
  ariaLabel: string
}

export function ChipGroup<T extends string>({
  value,
  onChange,
  options,
  ariaLabel,
}: ChipGroupProps<T>) {
  return (
    <div role="group" aria-label={ariaLabel} className="chip-group">
      {options.map((opt) => {
        const active = value !== null && value === opt.id
        return (
          <button
            key={opt.id}
            type="button"
            aria-pressed={active}
            className={`chip${active ? ' is-active' : ''}`}
            onClick={() => onChange(opt.id)}
          >
            {opt.label}
          </button>
        )
      })}
    </div>
  )
}
