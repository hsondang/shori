import { NodeCard } from '@shori/design-system'

export function CsvSource() {
  return (
    <div style={{ width: 260, padding: 16 }}>
      <NodeCard
        kind="csv"
        icon="📄"
        title="Sales Data"
        tableName="sales_data"
        subtitle="sales_2024.csv"
        result={{ status: 'success', executionTimeMs: 320, rowCount: 15420, columnCount: 7 }}
        actions={[{ label: 'Preview data' }]}
      />
    </div>
  )
}

export function Transform() {
  return (
    <div style={{ width: 260, padding: 16 }}>
      <NodeCard
        kind="transform"
        icon="⚙️"
        title="Filter & Group"
        tableName="aggregated"
        subtitle={<span style={{ fontStyle: 'italic' }}>SELECT region, SUM(revenue)…</span>}
        result={{ status: 'running', elapsedLabel: '2s' }}
        actions={[{ label: 'Running…' }]}
        selected
      />
    </div>
  )
}

export function Database() {
  return (
    <div style={{ width: 260, padding: 16 }}>
      <NodeCard
        kind="db"
        accent="postgres"
        icon="🐘"
        title="Production DB"
        tableName="orders"
        subtitle="prod.db.company.com"
        result={{ status: 'success', cached: true, rowCount: 891230, columnCount: 12 }}
        actions={[
          { label: 'Preview' },
          { label: 'Materialize' },
          { label: 'View table', tone: 'muted' },
        ]}
      />
    </div>
  )
}

export function ErrorState() {
  return (
    <div style={{ width: 260, padding: 16 }}>
      <NodeCard
        kind="csv"
        icon="📄"
        title="Bad CSV"
        tableName="bad_csv"
        subtitle="corrupt-file.csv"
        result={{ status: 'error' }}
        onViewError={() => {}}
        actions={[{ label: 'Preview data' }]}
      />
    </div>
  )
}

export function ExcelAndExport() {
  return (
    <div style={{ display: 'flex', gap: 16, padding: 16, flexWrap: 'wrap' }}>
      <div style={{ width: 240 }}>
        <NodeCard
          kind="excel"
          icon="▦"
          title="Revenue Report"
          tableName="revenue"
          subtitle="Q4-report.xlsx · Sheet: Revenue"
          result={{ status: 'idle' }}
          actions={[]}
        />
      </div>
      <div style={{ width: 240 }}>
        <NodeCard
          kind="export"
          icon="📥"
          title="Export Results"
          subtitle="Source: aggregated"
          result={{ status: 'success', executionTimeMs: 55 }}
          actions={[{ label: 'Download CSV' }]}
        />
      </div>
    </div>
  )
}
