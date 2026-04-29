/**
 * SeriesPicker — two-pane combobox for adding series to a chart card.
 *
 * Layout (RTL):
 *   [ search input across the top                                ]
 *   [ categories pane (start) | series pane (end)                ]
 *
 * The right pane lists CATEGORIES (5 of them); hovering one (with
 * 100ms debounce to avoid flicker as the cursor passes through)
 * filters the left pane to that category's entries from
 * SERIES_REGISTRY. Click also works for explicit selection.
 * Categories that opt into districts (`hasDistrictSelector`) get a
 * small district picker above the series list — affects which
 * underlying row is fetched when the user picks an entry.
 *
 * Touch fallback: pointerEnter only fires for mouse-type pointers,
 * so on touch devices (phone/tablet) the picker falls back to
 * click-to-select naturally — no platform detection needed.
 *
 * Disabled-state rules:
 *   - Entry already added to this card → disabled with ✓
 *   - Card is at the 5-series cap → all entries disabled
 *   - Entry is national-only (e.g. new_inventory) and the district
 *     picker isn't on national → disabled with "(לאומי בלבד)" note
 *
 * Close interactions: Esc + click outside the panel. Wrap in a
 * relatively-positioned anchor in the parent so the absolute
 * positioning lands beneath the trigger button.
 */

import { useEffect, useRef, useState } from 'react'

import {
  CATEGORIES,
  DISTRICTS,
  SERIES_REGISTRY,
  isGroupEntry,
  specKey,
  type CategoryId,
  type District,
  type RegistryEntry,
  type SeriesSpec,
} from '../data/seriesRegistry'
import { useFocusTrap } from '../hooks/useFocusTrap'

interface SeriesPickerProps {
  open: boolean
  onClose: () => void
  /** Toggle handler. ChartCard's handlePick is implemented as a
   * toggle (already-added → remove; not-added → add), so a single
   * onPick call per spec covers both directions. Group entries
   * fan out into multiple onPick calls within the same click. */
  onPick: (spec: SeriesSpec) => void
  /** Set of specKey strings already on the chart. */
  alreadyAdded: Set<string>
  /** True when the chart is at the series cap (5). Disables
   * not-yet-added entries; already-added entries stay clickable
   * for toggle-off. */
  atCap: boolean
}

/** Coverage of an entry's specs against the chart's already-added
 * set. For leaf entries it's binary (none / all). For groups it
 * adds a "partial" middle state — some members are added, others
 * aren't — which the click handler resolves by adding the missing
 * members rather than removing what's there.
 *
 *   none    → click adds (all specs)
 *   partial → click adds the missing members
 *   all     → click removes (all specs)
 */
type EntryCoverage = 'none' | 'partial' | 'all'

interface EntryState {
  coverage: EntryCoverage
  /** Specs the click handler should toggle. For coverage='all'
   * these are the existing specs (clicking removes them). For
   * 'none' / 'partial' these are the specs to add. */
  specsToToggle: SeriesSpec[]
  /** True if the entry's data isn't available at the picker's
   * currently-selected district (e.g., new_inventory at a
   * district other than national). Click is disabled. */
  districtMismatch: boolean
}

/** Plan the click action for a leaf or group entry against the
 * picker's current district + alreadyAdded set. */
function computeEntryState(
  entry: RegistryEntry,
  district: District,
  alreadyAdded: Set<string>,
  atCap: boolean,
): EntryState {
  if (isGroupEntry(entry)) {
    // Group: every member's coverage is "added" iff its (registryId,
    // effective-district) is in alreadyAdded. Effective district
    // honors per-member national-only overrides via getLeafEntry —
    // but groups currently don't have such heterogeneity in our
    // registry, so per-group `districts` controls the whole bundle.
    const districtMismatch =
      entry.districts === 'national-only' && district !== 'national'
    const effectiveDistrict: District =
      entry.districts === 'national-only' ? 'national' : district
    const memberSpecs: SeriesSpec[] = entry.members.map((m) => ({
      registryId: m.registryId,
      district: effectiveDistrict,
      stackId: m.stackId,
    }))
    const addedMembers = memberSpecs.filter((s) => alreadyAdded.has(specKey(s)))
    const coverage: EntryCoverage =
      addedMembers.length === 0
        ? 'none'
        : addedMembers.length === memberSpecs.length
          ? 'all'
          : 'partial'

    if (coverage === 'all') {
      // Toggle-off: send all member specs; ChartCard's toggle
      // handlePick removes each.
      return { coverage, specsToToggle: memberSpecs, districtMismatch }
    }
    // Add (or top up): only the missing members. atCap blocks
    // adding new ones — mirror that here so we don't queue picks
    // that ChartCard would silently reject.
    const missing = memberSpecs.filter((s) => !alreadyAdded.has(specKey(s)))
    if (atCap && coverage === 'none') {
      return { coverage, specsToToggle: [], districtMismatch }
    }
    return { coverage, specsToToggle: missing, districtMismatch }
  }

  // Leaf entry — binary coverage.
  const districtMismatch =
    entry.districts === 'national-only' && district !== 'national'
  const effectiveDistrict: District =
    entry.districts === 'national-only' ? 'national' : district
  const spec: SeriesSpec = {
    registryId: entry.id,
    district: effectiveDistrict,
  }
  const isAdded = alreadyAdded.has(specKey(spec))
  return {
    coverage: isAdded ? 'all' : 'none',
    specsToToggle: [spec],
    districtMismatch,
  }
}

export function SeriesPicker({
  open,
  onClose,
  onPick,
  alreadyAdded,
  atCap,
}: SeriesPickerProps) {
  const [activeCategoryId, setActiveCategoryId] = useState<CategoryId>('rates')
  const [district, setDistrict] = useState<District>('national')
  const [search, setSearch] = useState('')
  const panelRef = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)
  // Trap Tab + restore focus to the trigger button on close so
  // keyboard users don't get dropped at the document start. The
  // existing rAF-then-focus(searchRef) below already lands first
  // focus on the search input; useFocusTrap takes over from there.
  useFocusTrap(panelRef, open)
  // Hover-debounce timer: when the cursor enters a category, we
  // schedule the switch 100ms out so a fast pass-through across
  // multiple categories doesn't flicker the right pane. Cleared on
  // pointerLeave or when a different category is entered.
  const hoverTimerRef = useRef<number | null>(null)

  const cancelHover = () => {
    if (hoverTimerRef.current != null) {
      window.clearTimeout(hoverTimerRef.current)
      hoverTimerRef.current = null
    }
  }

  // Cleanup any pending hover-switch when the picker closes/unmounts.
  // Without this a hover scheduled just before close would still fire
  // 100ms later and update state on the (closed) component.
  useEffect(() => {
    return () => cancelHover()
  }, [])

  // Reset state on (re)open so the next open starts fresh — avoids the
  // user seeing stale search text or a forgotten category selection
  // from a previous interaction. Focus the search so typing immediately
  // filters without an extra click.
  useEffect(() => {
    if (open) {
      setSearch('')
      setActiveCategoryId('rates')
      setDistrict('national')
      // Defer focus until the panel mounts — autoFocus on the input
      // would also work, but explicit focus here is symmetric with the
      // outside-click/Escape handlers and survives panel remounts.
      requestAnimationFrame(() => searchRef.current?.focus())
    }
  }, [open])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    const onMouse = (e: MouseEvent) => {
      const target = e.target as Node | null
      if (!target) return
      if (panelRef.current?.contains(target)) return
      // Clicks on the trigger button itself reach this handler before
      // the button's onClick toggles open. The button has data-picker-
      // anchor — ignore those so toggle-off behavior comes from the
      // button's own handler, not from outside-click closing first.
      const el = target as Element
      if (el.closest && el.closest('[data-picker-anchor]')) return
      onClose()
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('mousedown', onMouse)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('mousedown', onMouse)
    }
  }, [open, onClose])

  if (!open) return null

  const activeCategory = CATEGORIES.find((c) => c.id === activeCategoryId)
  const matches = (name: string) =>
    search.trim() === '' ? true : name.includes(search.trim())

  const entries = SERIES_REGISTRY.filter(
    (e) => e.category === activeCategoryId && matches(e.name),
  )

  return (
    <div
      className="series-picker"
      ref={panelRef}
      role="dialog"
      aria-label="הוסף סדרה"
    >
      <div className="series-picker-search-wrap">
        <input
          ref={searchRef}
          className="series-picker-search"
          type="search"
          placeholder="חיפוש סדרה..."
          aria-label="חיפוש סדרה"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      <div className="series-picker-panes">
        <ul
          className="series-picker-categories"
          role="tablist"
          onPointerLeave={cancelHover}
        >
          {CATEGORIES.map((c) => (
            <li key={c.id}>
              <button
                type="button"
                role="tab"
                aria-selected={c.id === activeCategoryId}
                className={`series-picker-cat${
                  c.id === activeCategoryId ? ' is-active' : ''
                }`}
                onClick={() => {
                  // Click-as-explicit-select: fire immediately and
                  // cancel any pending hover-switch so a click during
                  // the hover-debounce window doesn't get overridden.
                  cancelHover()
                  setActiveCategoryId(c.id)
                }}
                onPointerEnter={(e) => {
                  // pointerEnter only fires for mouse-type pointers;
                  // touch devices fall through to onClick naturally.
                  if (e.pointerType !== 'mouse') return
                  if (c.id === activeCategoryId) return
                  cancelHover()
                  hoverTimerRef.current = window.setTimeout(() => {
                    setActiveCategoryId(c.id)
                    hoverTimerRef.current = null
                  }, 100)
                }}
              >
                {c.name}
              </button>
            </li>
          ))}
        </ul>

        <div className="series-picker-list">
          {activeCategory?.hasDistrictSelector && (
            <div className="series-picker-district">
              <label className="series-picker-district-label" htmlFor="picker-district">
                מחוז
              </label>
              <select
                id="picker-district"
                className="series-picker-district-select"
                value={district}
                onChange={(e) => setDistrict(e.target.value as District)}
              >
                {DISTRICTS.map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.name}
                  </option>
                ))}
              </select>
            </div>
          )}

          {entries.length === 0 ? (
            <div className="series-picker-empty">אין סדרות תואמות</div>
          ) : (
            <ul className="series-picker-entries">
              {entries.map((e) => {
                const state = computeEntryState(
                  e,
                  district,
                  alreadyAdded,
                  atCap,
                )
                // Disabled state applies only when the user CAN'T act:
                // at-cap on a not-yet-added entry, or district mismatch
                // on a national-only entry. Already-added entries stay
                // clickable so the same click toggles the series off.
                const disabled =
                  state.districtMismatch ||
                  (state.coverage !== 'all' && atCap)
                const note = state.districtMismatch
                  ? '(לאומי בלבד)'
                  : state.coverage === 'all'
                    ? '' // already added — ✓ alone is the indicator
                    : atCap
                      ? '(הגעת למקסימום)'
                      : ''
                return (
                  <li key={e.id}>
                    <button
                      type="button"
                      className={
                        'series-picker-entry' +
                        (disabled ? ' is-disabled' : '') +
                        (state.coverage === 'all' ? ' is-added' : '') +
                        (state.coverage === 'partial' ? ' is-partial' : '')
                      }
                      disabled={disabled}
                      onClick={() => {
                        // Issue all picks first, then close. Group
                        // entries fan out into multiple picks; React
                        // batches the resulting setSpecs callbacks.
                        for (const spec of state.specsToToggle) {
                          onPick(spec)
                        }
                        onClose()
                      }}
                    >
                      <span className="series-picker-entry-name">{e.name}</span>
                      {state.coverage === 'all' && (
                        <span
                          className="series-picker-entry-check"
                          aria-hidden="true"
                        >
                          ✓
                        </span>
                      )}
                      {note && (
                        <span className="series-picker-entry-note">{note}</span>
                      )}
                    </button>
                  </li>
                )
              })}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}
