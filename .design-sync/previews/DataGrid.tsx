import { DataGrid } from '@shori/design-system'

const COLUMNS = [
  { name: 'region', type: 'VARCHAR' },
  { name: 'revenue', type: 'DOUBLE' },
  { name: 'orders', type: 'INTEGER' },
  { name: 'avg_order_value', type: 'DOUBLE' },
  { name: 'period', type: 'DATE' },
]

const ROWS = [
  ['West', 182450.00, 1243, 146.79, '2024-Q4'],
  ['East', 241830.50, 1891, 127.88, '2024-Q4'],
  ['North', 98200.00, 732, 134.15, '2024-Q4'],
  ['South', 154320.75, 1102, 140.04, '2024-Q4'],
  ['Central', 67890.25, 489, 138.84, '2024-Q4'],
]

export function WithData() {
  return (
    <div style={{ height: 280, padding: 16 }}>
      <DataGrid columns={COLUMNS} rows={ROWS} />
    </div>
  )
}

export function LoadingState() {
  return (
    <div style={{ height: 200, padding: 16 }}>
      <DataGrid columns={COLUMNS} rows={[]} loading />
    </div>
  )
}

export function EmptyState() {
  return (
    <div style={{ height: 160, padding: 16 }}>
      <DataGrid columns={COLUMNS} rows={[]} emptyMessage="No rows returned" />
    </div>
  )
}

export function WithNulls() {
  return (
    <div style={{ height: 200, padding: 16 }}>
      <DataGrid
        columns={[{ name: 'id', type: 'INTEGER' }, { name: 'name', type: 'VARCHAR' }, { name: 'score', type: 'DOUBLE' }]}
        rows={[[1, 'Alice', 98.5], [2, null, null], [3, 'Charlie', 87.0]]}
      />
    </div>
  )
}
