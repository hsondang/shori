import { ToolbarChip } from '@shori/design-system'

export function AllAccents() {
  return (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', padding: 16 }}>
      <ToolbarChip accent="csv" label="CSV Source" />
      <ToolbarChip accent="excel" label="Excel Source" />
      <ToolbarChip accent="db" label="Database" />
      <ToolbarChip accent="transform" label="Transform" />
      <ToolbarChip accent="export" label="Export" />
    </div>
  )
}

export function Draggable() {
  return (
    <div style={{ padding: 16 }}>
      <p style={{ fontSize: 12, color: '#6b7280', marginBottom: 8 }}>Drag chips onto the canvas to add nodes</p>
      <div style={{ display: 'flex', gap: 8 }}>
        <ToolbarChip accent="csv" label="CSV Source" draggable />
        <ToolbarChip accent="transform" label="Transform" draggable />
      </div>
    </div>
  )
}
