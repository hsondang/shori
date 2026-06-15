import { Button } from '@shori/design-system'

export function Variants() {
  return (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', padding: 16 }}>
      <Button variant="primary">Save pipeline</Button>
      <Button variant="secondary">Cancel</Button>
      <Button variant="ghost">Settings</Button>
      <Button variant="danger">Delete project</Button>
    </div>
  )
}

export function Sizes() {
  return (
    <div style={{ display: 'flex', gap: 8, alignItems: 'center', padding: 16 }}>
      <Button size="sm" variant="primary">Small</Button>
      <Button size="md" variant="primary">Medium</Button>
      <Button size="lg" variant="primary">Large</Button>
    </div>
  )
}

export function States() {
  return (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', padding: 16 }}>
      <Button variant="primary" disabled>Disabled primary</Button>
      <Button variant="secondary" disabled>Disabled secondary</Button>
      <Button variant="danger" disabled>Disabled danger</Button>
    </div>
  )
}

export function FullWidth() {
  return (
    <div style={{ width: 300, padding: 16 }}>
      <Button variant="primary" fullWidth>Execute pipeline</Button>
    </div>
  )
}
