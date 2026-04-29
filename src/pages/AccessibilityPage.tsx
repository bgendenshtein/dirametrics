import accessibilityMd from '../../docs/accessibility_statement.md?raw'
import MarkdownPage from './MarkdownPage'

export default function AccessibilityPage() {
  return <MarkdownPage title="הצהרת נגישות" source={accessibilityMd} />
}
