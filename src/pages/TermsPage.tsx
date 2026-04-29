import termsMd from '../../docs/terms_of_use.md?raw'
import MarkdownPage from './MarkdownPage'

export default function TermsPage() {
  return <MarkdownPage title="תנאי שימוש" source={termsMd} />
}
