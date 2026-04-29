import privacyMd from '../../docs/privacy_policy.md?raw'
import MarkdownPage from './MarkdownPage'

export default function PrivacyPage() {
  return <MarkdownPage title="מדיניות פרטיות" source={privacyMd} />
}
