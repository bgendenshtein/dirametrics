/**
 * AboutPage — methodology + about content rendered from
 * docs/methodology.md.
 *
 * Source pipeline:
 *   1. Vite's `?raw` import inlines the markdown file as a string at
 *      build time, so there's no runtime fetch and no missing-file
 *      branch to handle.
 *   2. The leading `<div dir="rtl">` wrapper from the source file is
 *      stripped — the entire document already flows RTL via the body
 *      CSS, so the wrapper would just be a no-op div in the output.
 *   3. The `{dynamic_date}` placeholder near the end is replaced with
 *      the Hebrew month-year label from useDataFreshness (the same
 *      hook the dashboard subtitle uses) so "last updated" stays
 *      truthful without manual edits to the markdown.
 *   4. react-markdown renders to HTML. The `components` override on
 *      `h3` adds `id="<anchor>"` for the five KPI sections so the
 *      KPI cards' ⓘ icons (which point at /about#starts etc.) scroll
 *      to the right place.
 *
 * Scroll behavior: the page uses native fragment-based scrolling
 * (the browser handles #starts links automatically once the
 * markdown is rendered and the IDs are present). On first mount
 * with a fragment in the URL, we manually scroll the matching
 * element into view because the DOM isn't ready when the browser
 * does its initial fragment lookup.
 */

import { useEffect } from 'react'
import ReactMarkdown from 'react-markdown'
import { useLocation } from 'react-router-dom'

import methodologyMd from '../../docs/methodology.md?raw'
import { useDataFreshness } from '../hooks/useDataFreshness'

/** Hebrew section heading → fragment anchor id. The keys are matched
 * by `headingTextToId` against the H3 text content; the first key
 * the heading text starts with wins (handles the trailing ranges
 * like "התחלות בנייה - 12 חודשים מול 12 קודמים"). */
const H3_ANCHORS: ReadonlyArray<readonly [string, string]> = [
  ['התחלות בנייה',                'starts'],
  ['מלאי דירות חדשות',            'inventory'],
  ['סך מכירות דירות',             'sales'],
  ['מדד מחירי הדיור',             'hpi'],
  ['ריבית משכנתא קבועה לא צמודה', 'mortgage'],
]

function headingTextToId(text: string): string | undefined {
  const trimmed = text.trim()
  for (const [prefix, id] of H3_ANCHORS) {
    if (trimmed.startsWith(prefix)) return id
  }
  return undefined
}

/** Coerce ReactMarkdown's children prop (string | string[] | ReactNode[])
 * into a flat plain-text string. We only need the text content for the
 * heading-to-id match, so anything that isn't a primitive (rare in H3
 * source) is safely ignored. */
function extractText(children: React.ReactNode): string {
  if (children == null) return ''
  if (typeof children === 'string') return children
  if (typeof children === 'number') return String(children)
  if (Array.isArray(children)) return children.map(extractText).join('')
  return ''
}

/** Strip the leading `<div dir="rtl">` wrapper and trailing `</div>`
 * from the source file. The page body is already RTL via global CSS,
 * so the wrapper would render as an extra inert div. Using a regex
 * (rather than rehype-raw + a real HTML parser) is fine because the
 * wrapper is a known fixture at known positions in the source file. */
function stripRtlWrapper(md: string): string {
  return md
    .replace(/^\s*<div dir="rtl">\s*\n/, '')
    .replace(/\n\s*<\/div>\s*$/, '')
}

export default function AboutPage() {
  const { label } = useDataFreshness()
  const { hash } = useLocation()

  // Scroll the targeted heading into view when the URL has a fragment.
  // Runs once on mount + whenever the hash changes so navigating away
  // and back to /about#mortgage still scrolls. The setTimeout gives
  // ReactMarkdown one tick to render the heading IDs before we look
  // them up — without it the element doesn't exist yet on the very
  // first render.
  useEffect(() => {
    if (!hash) {
      // Fragment-less /about navigation: scroll to top so the user
      // doesn't land mid-document if they were previously at #mortgage.
      window.scrollTo({ top: 0 })
      return
    }
    const id = hash.replace(/^#/, '')
    const t = setTimeout(() => {
      const el = document.getElementById(id)
      if (el) el.scrollIntoView({ behavior: 'auto', block: 'start' })
    }, 0)
    return () => clearTimeout(t)
  }, [hash])

  // Replace the {dynamic_date} placeholder. Falling back to "—" while
  // the freshness query resolves keeps the page from showing a literal
  // "{dynamic_date}" to the user during the brief loading window.
  const content = stripRtlWrapper(methodologyMd).replace(
    '{dynamic_date}',
    label || '—',
  )

  return (
    <main id="main-content" tabIndex={-1} className="about-page">
      <article className="about-content" lang="he">
        <ReactMarkdown
          components={{
            h3: ({ children, ...rest }) => {
              const text = extractText(children)
              const id = headingTextToId(text)
              return (
                <h3 id={id} {...rest}>
                  {children}
                </h3>
              )
            },
          }}
        >
          {content}
        </ReactMarkdown>
      </article>
    </main>
  )
}
