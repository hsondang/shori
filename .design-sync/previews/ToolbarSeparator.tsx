import { Toolbar, ToolbarChip, ToolbarSeparator } from '@shori/design-system'

export function InToolbar() {
  return (
    <div style={{ padding: 16 }}>
      <Toolbar>
        <ToolbarChip accent="csv" label="CSV Source" />
        <ToolbarChip accent="excel" label="Excel Source" />
        <ToolbarSeparator />
        <ToolbarChip accent="transform" label="Transform" />
        <ToolbarSeparator />
        <ToolbarChip accent="export" label="Export" />
      </Toolbar>
    </div>
  )
}
