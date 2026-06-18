import { Switch } from '@shori/design-system'

export function States() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 16 }}>
      <Switch checked={false} onChange={() => {}} label="Edit mode" />
      <Switch checked={true} onChange={() => {}} label="Edit mode" />
      <Switch checked={true} onChange={() => {}} label="CSV preprocessing" size="sm" />
      <Switch checked={false} onChange={() => {}} label="Disabled" disabled />
    </div>
  )
}

export function Sizes() {
  return (
    <div style={{ display: 'flex', gap: 16, alignItems: 'center', padding: 16 }}>
      <Switch checked={true} onChange={() => {}} label="Small" size="sm" />
      <Switch checked={true} onChange={() => {}} label="Medium" size="md" />
    </div>
  )
}
