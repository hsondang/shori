import { Toolbar, ToolbarChip, ToolbarSeparator } from '@shori/design-system'

export function AllChips() {
  return (
    <div style={{ padding: 16 }}>
      <Toolbar>
        <ToolbarChip accent="csv" label="CSV Source" draggable />
        <ToolbarChip accent="excel" label="Excel Source" draggable />
        <ToolbarChip accent="db" label="Database" draggable />
        <ToolbarSeparator />
        <ToolbarChip accent="transform" label="Transform" draggable />
        <ToolbarSeparator />
        <ToolbarChip accent="export" label="Export" draggable />
      </Toolbar>
    </div>
  )
}

export function FullToolbar() {
  return (
    <div style={{ padding: 16, width: 600 }}>
      <div style={{ marginBottom: 8, fontSize: 11, fontWeight: 600, color: '#6b7280', letterSpacing: '0.05em', textTransform: 'uppercase' }}>Add node</div>
      <Toolbar>
        <ToolbarChip accent="csv" label="CSV Source" draggable />
        <ToolbarChip accent="excel" label="Excel Source" draggable />
        <ToolbarChip accent="db" label="PostgreSQL" draggable />
        <ToolbarChip accent="db" label="Oracle" draggable />
        <ToolbarSeparator />
        <ToolbarChip accent="transform" label="SQL Transform" draggable />
        <ToolbarSeparator />
        <ToolbarChip accent="export" label="Export CSV" draggable />
      </Toolbar>
    </div>
  )
}
