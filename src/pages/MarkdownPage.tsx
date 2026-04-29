/**
 * MarkdownPage — generic renderer for static markdown content
 * (legal pages, accessibility statement, etc.).
 *
 * Reuses the .about-page typography container so legal pages
 * share the same visual frame as the methodology page: 800px
 * max-width, 15px / 1.7 lh body, scoped heading sizes, RTL list
 * indents, etc. Setting it up as a shared component means future
 * legal additions (e.g. a cookie-policy page) plug in without
 * re-styling.
 *
 * Sets document.title on mount, restores on unmount. Scrolls to
 * top when the route mounts so users landing from a footer link
 * deep in the previous page start at the top of the new content
 * (default browser scroll restore would otherwise pin their
 * scroll position from the previous route).
 */

import { useEffect } from 'react'
import ReactMarkdown from 'react-markdown'

interface MarkdownPageProps {
  /** Page-specific title; suffixed with "- DiraMetrics" before
   * being written to document.title. Also used as the page H1
   * fallback (the markdown source's own H1 takes precedence). */
  title: string
  /** Raw markdown source. Pass via Vite's `?raw` import suffix —
   * see PrivacyPage / TermsPage / AccessibilityPage for the
   * concrete pattern. */
  source: string
}

/** Strip the `<div dir="rtl">` wrapper that all the docs/*.md
 * sources start with. The page is already RTL via the global body
 * style, so the wrapper renders as an inert div in the output.
 * Same mechanic as AboutPage. */
function stripRtlWrapper(md: string): string {
  return md
    .replace(/^\s*<div dir="rtl">\s*\n/, '')
    .replace(/\n\s*<\/div>\s*$/, '')
}

export default function MarkdownPage({ title, source }: MarkdownPageProps) {
  useEffect(() => {
    const previous = document.title
    document.title = `${title} - DiraMetrics`
    window.scrollTo({ top: 0 })
    return () => {
      document.title = previous
    }
  }, [title])

  return (
    <main id="main-content" tabIndex={-1} className="about-page">
      <article className="about-content" lang="he">
        <ReactMarkdown>{stripRtlWrapper(source)}</ReactMarkdown>
      </article>
    </main>
  )
}
