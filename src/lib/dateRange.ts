/**
 * dateRange — month-grained date helpers + Hebrew formatting for the
 * brush + chart-card range UI. Everything operates in UTC to avoid
 * timezone shifts (data is monthly, so a one-day TZ offset would push
 * "March 1" to "Feb 28" silently).
 */

export interface DateRange {
  start: Date
  end: Date
}

const HEBREW_MONTHS_ABBR = [
  'ינו׳', 'פבר׳', 'מרץ',  'אפר׳', 'מאי',  'יוני',
  'יולי', 'אוג׳', 'ספט׳', 'אוק׳', 'נוב׳', 'דצמ׳',
] as const

/** First day of the given date's month, in UTC. */
export function startOfMonth(d: Date): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1))
}

/** Add `months` calendar months (negative allowed). Result is start of
 * that month, UTC. */
export function addMonths(d: Date, months: number): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + months, 1))
}

/** Whole-month difference: positive when b is after a. */
export function monthsBetween(a: Date, b: Date): number {
  return (
    (b.getUTCFullYear() - a.getUTCFullYear()) * 12 +
    (b.getUTCMonth() - a.getUTCMonth())
  )
}

export function rangesEqual(a: DateRange, b: DateRange): boolean {
  return a.start.getTime() === b.start.getTime() && a.end.getTime() === b.end.getTime()
}

/** Compact "מאי 21" / "אפר׳ 26" style — Hebrew month abbreviation
 * (with geresh where applicable) plus 2-digit year. */
export function formatHebrewMonthShortYear(d: Date): string {
  return `${HEBREW_MONTHS_ABBR[d.getUTCMonth()]} ${String(d.getUTCFullYear()).slice(-2)}`
}

/** Range formatter for the chart-card meta line, e.g.
 * "מאי 21 - אפר׳ 26". */
export function formatHebrewDateRange(range: DateRange): string {
  return `${formatHebrewMonthShortYear(range.start)} - ${formatHebrewMonthShortYear(range.end)}`
}
