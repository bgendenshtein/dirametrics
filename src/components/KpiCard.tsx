/**
 * KpiCard — hero metric card for the dashboard top strip.
 *
 * Layout (per design/dashboard.jsx — KpiCard, Direction D):
 *
 *   ┌────────────────────────────────────────────────────────────┐
 *   │ label                                                    ⓘ │
 *   │ sublabel (2-line clamp, muted)                             │
 *   │                                                            │
 *   │ {hero}              {sparkline ~70×26}                     │
 *   └────────────────────────────────────────────────────────────┘
 *
 *   Card is column flex with justify-content: space-between, so the
 *   header pins to the top and the {numbers, sparkline} row pins to
 *   the bottom. The bottom row is itself a row flex with the numbers
 *   at the inline-start (right in RTL) and the sparkline at the
 *   inline-end (left in RTL) — sparkline NEVER below the number.
 *
 * Two display modes:
 *
 *   delta-as-hero (only `delta`):
 *     Big sign+number+unit+arrow as the hero, semantic up/down color.
 *
 *   level-as-hero (`level` AND `delta`):
 *     Big neutral level number with a smaller-muted unit suffix, then
 *     a small signed delta inline on the same baseline (gap 8) in
 *     semantic color. Used for mortgage rate, where the user wants to
 *     see the current rate AND its change.
 *
 * Numeric rendering rules apply in both modes:
 *   - U+2212 (−) for negatives, never hyphen-minus
 *   - tabular-nums for column alignment
 *   - sign/number/unit/arrow group is one dir="ltr" inline so the sign
 *     reads on the LEFT visually inside the RTL parent
 */

import { Link } from 'react-router-dom'

import { Sparkline as SparklineSvg } from './Sparkline'
import { semanticColor, useResolvedTheme } from '../styles/tokens'

export type Direction = 'up' | 'down' | 'flat'

export interface KpiLevel {
  value: number
  /** Unit suffix (e.g., '%'). Rendered smaller + muted next to the level number. */
  unit?: string
  /** Decimal places. Default 1 for percent, 0 for counts. */
  precision?: number
  /** Insert thousands separators (for unitless counts). */
  thousands?: boolean
}

export interface KpiDelta {
  value: number
  /** Unit suffix (e.g., '%' or " נק׳"). Rendered same color as the delta. */
  unit?: string
  /** Decimal places. Default 1. */
  precision?: number
  direction: Direction
}

export interface KpiCardProps {
  label: string
  sublabel: string
  /** Optional. When present, level-as-hero mode (mortgage rate). */
  level?: KpiLevel | null
  /** Required for ready state. Drives sparkline color in both modes. */
  delta: KpiDelta | null
  sparkValues: Array<{ date: string; v: number }> | null
  /** Anchor target on the methodology page (placeholder until /about lands). */
  infoHref?: string
  loading?: boolean
  error?: string | null
}

const MINUS = '−' // U+2212

function arrowFor(direction: Direction): string {
  if (direction === 'up') return '▲'
  if (direction === 'down') return '▼'
  return '→'
}

function formatNumber(value: number, precision: number, thousands: boolean): string {
  if (thousands) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: precision,
      maximumFractionDigits: precision,
    }).format(value)
  }
  return value.toFixed(precision)
}

function defaultPrecision(unit: string | undefined, thousands: boolean | undefined): number {
  if (thousands) return 0
  if (unit === '%') return 1
  return 1
}

interface SparklineProps {
  values: Array<{ date: string; v: number }>
  direction: Direction
}

function Sparkline({ values, direction }: SparklineProps) {
  const theme = useResolvedTheme()
  const stroke =
    direction === 'flat'
      ? 'currentColor'
      : semanticColor(direction === 'up' ? 'up' : 'down', theme)
  // Bespoke SVG sparkline (matches the visual output of Recharts'
  // type="monotone" via Fritsch-Carlson cubic). Removing Recharts from
  // the KPI strip path drops it from the main bundle entirely; it now
  // only ships in the lazy-loaded Chart chunk.
  return <SparklineSvg values={values} stroke={stroke} height={52} />
}

interface SignedNumberProps {
  value: number
  unit: string
  precision: number
  direction: Direction
  /** 'hero' = big (delta-as-hero); 'inline' = small (secondary delta beside level). */
  size: 'hero' | 'inline'
}

function SignedNumber({ value, unit, precision, direction, size }: SignedNumberProps) {
  const sign = value > 0 ? '+' : value < 0 ? MINUS : ''
  const abs = formatNumber(Math.abs(value), precision, false)
  const arrow = arrowFor(direction)
  const className = size === 'hero' ? 'kpi-hero tabular' : 'kpi-delta-inline tabular'
  // Bidi + accessibility notes:
  //
  //   <span dir="ltr"> + CSS unicode-bidi:isolate (set on .kpi-hero
  //     and .kpi-delta-inline): the dir attribute sets visual flow;
  //     isolate prevents the surrounding RTL card from leaking into
  //     the indicator's bidi resolution. Using <span> instead of
  //     <bdi> avoids ARIA's prohibited-naming rule for the generic
  //     role.
  //
  //   {abs}{unit} in ONE text span: when the unit contains Hebrew
  //     (e.g. " נק׳" for percentage points), splitting across span
  //     boundaries in a flex container causes each item's bidi to
  //     resolve in its own context and the Hebrew can drift to the
  //     wrong side of the digits. One contiguous text node = one
  //     bidi resolution.
  //
  //   No aria-label on the wrapper: per ARIA 1.2 the generic role
  //     (default for <span>) prohibits aria-label. Lighthouse flags
  //     it as a violation. Instead, the value span carries the
  //     readable accessible name; sign + arrow stay aria-hidden as
  //     decorative glyphs. Tradeoff: SR users hear only the
  //     magnitude ("0.18 נק׳"), losing the +/− direction. If user
  //     reports flag this as confusing we'll switch to
  //     aria-labelledby pointing at a visually-hidden helper that
  //     spells out the direction in Hebrew.
  return (
    <span
      dir="ltr"
      className={className}
      data-direction={direction}
    >
      <span className="kpi-num-sign" aria-hidden="true">{sign}</span>
      <span className="kpi-num-value">{abs}{unit}</span>
      <span className="kpi-num-arrow" aria-hidden="true">{arrow}</span>
    </span>
  )
}

interface LevelNumberProps {
  level: KpiLevel
}

function LevelNumber({ level }: LevelNumberProps) {
  const precision = level.precision ?? defaultPrecision(level.unit, level.thousands)
  const formatted = formatNumber(level.value, precision, level.thousands ?? false)
  // <span dir="ltr"> with CSS-applied unicode-bidi: isolate (set on
  // .kpi-hero) — same isolation behavior as <bdi> but without the
  // ARIA-prohibited element. The level number is digits-only ASCII
  // so bidi shouldn't reorder anything, but isolating the wrap means
  // a future numeric format with locale-specific marks (₪, etc.)
  // won't leak into the surrounding RTL card.
  return (
    <span dir="ltr" className="kpi-hero tabular" data-direction="neutral">
      <span className="kpi-num-value">{formatted}</span>
      {level.unit && <span className="kpi-level-unit">{level.unit}</span>}
    </span>
  )
}

export function KpiCard(props: KpiCardProps) {
  const {
    label,
    sublabel,
    level = null,
    delta,
    sparkValues,
    infoHref = '#about',
    loading = false,
    error = null,
  } = props

  const ready = !loading && !error && delta !== null

  let hero: React.ReactNode
  if (!ready) {
    hero = (
      <span dir="ltr" className="kpi-hero tabular" data-direction="placeholder">
        {error ? '—' : '   '}
      </span>
    )
  } else if (level && delta) {
    // level-as-hero: level + signed delta INLINE on the same baseline row
    hero = (
      <>
        <LevelNumber level={level} />
        <SignedNumber
          value={delta.value}
          unit={delta.unit ?? ''}
          precision={delta.precision ?? defaultPrecision(delta.unit, false)}
          direction={delta.direction}
          size="inline"
        />
      </>
    )
  } else if (delta) {
    // delta-as-hero: signed delta IS the hero
    hero = (
      <SignedNumber
        value={delta.value}
        unit={delta.unit ?? ''}
        precision={delta.precision ?? defaultPrecision(delta.unit, false)}
        direction={delta.direction}
        size="hero"
      />
    )
  }

  const sparkDirection = delta?.direction ?? 'flat'

  return (
    <article
      className="kpi-card"
      data-loading={loading || undefined}
      data-error={error ? 'true' : undefined}
      aria-busy={loading || undefined}
    >
      <div className="kpi-card-top">
        <div className="kpi-card-header">
          <span className="kpi-label">{label}</span>
          <Link
            to={infoHref}
            className="kpi-info"
            aria-label={`מתודולוגיה: ${label}`}
            title={`מתודולוגיה: ${label}`}
          >
            <span aria-hidden="true">ⓘ</span>
          </Link>
        </div>
        <span className="kpi-sublabel" title={sublabel}>
          {sublabel}
        </span>
      </div>

      <div className="kpi-card-bottom">
        <div className="kpi-numbers">{hero}</div>
        {ready && sparkValues && sparkValues.length > 1 ? (
          <Sparkline values={sparkValues} direction={sparkDirection} />
        ) : (
          <div className="kpi-spark kpi-spark--placeholder" aria-hidden="true" />
        )}
      </div>

      {error && (
        <span role="alert" className="kpi-error">
          {error}
        </span>
      )}
    </article>
  )
}
